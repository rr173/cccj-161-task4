// 投递模块领域引擎：所有状态变更都以“只增账本记录 + SHA-256 哈希链”落地，
// 进程启动时逐条重放重建投影。任何业务代码都不允许原地修改队列/游标/信封，
// 因此通知信封不可变、游标不会被重复推进、重启与首次执行走完全相同的 apply 路径。
//
// 队列每订阅方独立、按 envelopeSeq（= 事件发生顺序）严格排序；
// 不同订阅方各自一条链表（subscribers[].queueSeq[]），互不影响。
// 游标只在“队首确认”时向后推进到连续确认边界，重复确认走幂等分支，永不二次推进。
import { EventStore } from '../src/store.js';
import { canonicalJSON } from '../src/util.js';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import {
  generateSigningKeyPair,
  signPayload,
  verifyPayload,
  hmacHex,
  newSubscriberSecret,
} from './crypto.js';

export const NOTIFY_TYPES = Object.freeze({
  PUBLISHED: 'dr.event.published',
  CANCELLED: 'dr.event.cancelled',
  STARTED: 'dr.event.started',
  ENDED: 'dr.event.ended',
  REPORT_CORRECTED: 'dr.event.report.corrected',
  DISPUTE_ACCEPTED: 'dr.dispute.accepted',
  DISPUTE_SUPPLEMENTED: 'dr.dispute.supplemented',
  DISPUTE_RESOLVED: 'dr.dispute.resolved',
});

export const ITEM_STATUS = Object.freeze({
  PENDING: 'PENDING',
  INFLIGHT: 'INFLIGHT',
  DEAD: 'DEAD',
  ACKED: 'ACKED',
});

export const SUBSCRIPTION_STATUS = Object.freeze({
  PENDING: 'PENDING',
  ACTIVE: 'ACTIVE',
  REJECTED: 'REJECTED',
  EXPIRED: 'EXPIRED',
});

export class DomainError extends Error {
  constructor(code, message, status = 400, details = {}) {
    super(message);
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

// 信封体的规范化序列化：键名排序，保证签名可被任意实现独立复算。
export function canonicalEnvelope(env) {
  return canonicalJSON(env);
}

export class NotifierEngine {
  constructor(store, config = {}) {
    this.store = store;
    this.maxAttempts = config.maxAttempts ?? 4;

    // ---- 投影（全部由账本记录重建，不单独持久化）----
    this.keys = new Map(); // keyId -> {keyId, alg, publicKeyPem, privateKeyPem, rotatedAt, active, retired}
    this.keyOrder = [];
    this.activeKeyId = null;
    this.subscribers = new Map(); // subId -> subscriber 对象（含 queueSeq 链表）
    this.envelopes = new Map(); // envelopeSeq -> envelope（不可变）
    this.itemsByIdem = new Map(); // `${subId}|${envelopeSeq}` -> queue item
    this.acksByKey = new Map(); // idempotencyKey -> ack 记录 payload（重复确认检测）
    this.sourcesSeen = new Map(); // sourceRef -> envelopeSeq（事件接入幂等）
    this.envelopeSeq = 0;
  }

  get writable() {
    return this.store.writable;
  }

  // ---- 启动：重放账本，首轮自动生成签名密钥（以 KEY_ROTATED 记录落盘）----
  async load() {
    const verify = await this.store.load();
    for (const rec of this.store.all()) this.#apply(rec, { replaying: true });
    if (this.store.writable && this.keyOrder.length === 0) {
      this.rotateKey('初始签名密钥（首次引导）');
    }
    // 重启恢复：上次进程残留的 INFLIGHT 租约没有归属，全部置回 PENDING。
    // 游标只由 DELIVERY_ACKED 推进，恢复不动游标；重复重启时第二趟无 INFLIGHT，天然幂等。
    this.recoverInflightLeases('STARTUP');
    return this.store.verify();
  }

  activeKey() {
    return this.keys.get(this.activeKeyId) || null;
  }

  // ================= 订阅管理 =================
  createSubscriber(input = {}) {
    const subId = this.#requireStr(input.subscriberId, 'subscriberId', 1, 64);
    if (this.subscribers.has(subId)) {
      throw new DomainError('SUBSCRIBER_EXISTS', `订阅方 ${subId} 已存在`, 409);
    }
    const callbackUrl = this.#requireStr(input.callbackUrl, 'callbackUrl', 4, 500);
    new URL(callbackUrl); // 校验为绝对 URL，非法时抛错 -> 400
    const secret = input.secret ? String(input.secret) : newSubscriberSecret();
    const topics = normalizeTopics(input.topics);
    const payload = {
      subscriberId: subId,
      displayName: input.displayName || subId,
      callbackUrl,
      secret,
      topics,
      status: SUBSCRIPTION_STATUS.PENDING,
      createdAt: nowIso(),
      challenge: randomChallenge(),
      cursor: 0,
      lastDeliveryAt: null,
    };
    this.#append('SUBSCRIBER_REGISTERED', payload);
    return { ...this.subscribers.get(subId), secret }; // 明文 secret 仅此一次随响应返回
  }

  // 订阅校验结果（WebSub 风格：对 callbackUrl 发 GET 带 hub.challenge / 签名）
  validationResolved(subId, accepted, reason, httpStatus) {
    const sub = this.#requireSubscriber(subId);
    if (sub.status !== SUBSCRIPTION_STATUS.PENDING) {
      return { subscriberId: subId, status: sub.status, alreadyResolved: true };
    }
    const payload = {
      subscriberId: subId,
      accepted: !!accepted,
      reason: reason || (accepted ? 'challenge 回显一致' : '校验失败'),
      httpStatus: httpStatus ?? null,
      at: nowIso(),
    };
    this.#append('SUBSCRIPTION_VALIDATED', payload);
    return { subscriberId: subId, status: this.subscribers.get(subId).status };
  }

  // ================= 密钥轮换 =================
  rotateKey(reason = '人工轮换') {
    const { publicKeyPem, privateKeyPem } = generateSigningKeyPair();
    const keyId = `key-${this.store.seq + 1}-${randomBytes6()}`;
    this.#append('KEY_ROTATED', {
      keyId,
      alg: 'Ed25519',
      publicKeyPem,
      privateKeyPem, // 生产环境私钥应由 KMS/HSM 托管；演示系统随链留档
      reason: String(reason).slice(0, 200),
      rotatedAt: nowIso(),
    });
    return this.keyView(this.activeKeyId);
  }

  keyView(keyId) {
    const k = this.keys.get(keyId);
    if (!k) return null;
    return { keyId: k.keyId, alg: k.alg, publicKeyPem: k.publicKeyPem, active: k.active, rotatedAt: k.rotatedAt };
  }

  // ================= 事件接入 → 不可变信封 → 每订阅方入队 =================
  // sourceRef 为稳定去重键（编排服务 hashchain seq，或 API 显式给出），
  // 同一 sourceRef 重复接入永远返回同一个信封，不重复入队。
  ingestEvent(input = {}) {
    const type = this.#requireStr(input.type, 'type');
    if (!Object.values(NOTIFY_TYPES).includes(type)) {
      throw new DomainError('UNKNOWN_NOTIFY_TYPE', `未知通知类型：${type}`, 400, {
        allowed: Object.values(NOTIFY_TYPES),
      });
    }
    const eventId = this.#requireStr(input.eventId, 'eventId', 1, 120);

    let sourceRef = input.sourceRef ? String(input.sourceRef) : null;
    if (!sourceRef) {
      const v = input.sourceSeq != null ? Number(input.sourceSeq) : null;
      sourceRef = v ? `${type}:${eventId}:${v}` : `${type}:${eventId}:${this.store.seq + 1}`;
    }
    const dup = this.sourcesSeen.get(sourceRef);
    if (dup != null) {
      return { envelopeSeq: dup, deduplicated: true, envelope: this.envelopes.get(dup) };
    }

    const detail = input.detail && typeof input.detail === 'object' ? input.detail : {};
    if (input.version != null) detail.version = Number(input.version);
    if (input.reason != null) detail.reason = input.reason;

    const envSeq = this.envelopeSeq + 1;
    const key = this.activeKey();
    if (!key) throw new DomainError('NO_SIGNING_KEY', '签名密钥尚未初始化', 503);
    const envelope = {
      envelopeSeq: envSeq,
      notificationId: `ntf_${envSeq}_${randomBytes6()}`,
      type,
      eventId,
      sourceRef,
      occurredAt: input.occurredAt || nowIso(),
      detail,
      keyId: key.keyId,
    };
    // 信封一旦生成即不可变：签名只覆盖信封体；签名值与账本哈希共同锚定证据。
    const signature = signPayload(key.privateKeyPem, canonicalEnvelope(envelope));
    this.#append('DR_NOTIFICATION', { envelope, signature });

    // 按订阅方分别持久入队；只投递给订阅生效（ACTIVE）且主题匹配的订阅方。
    const enqueued = [];
    for (const sub of this.subscribers.values()) {
      if (sub.status !== SUBSCRIPTION_STATUS.ACTIVE) continue;
      if (!topicMatches(sub.topics, type, detail)) continue;
      const idempotencyKey = this.idempotencyKey(sub.subscriberId, envSeq);
      this.#append('QUEUE_ENQUEUED', {
        subscriberId: sub.subscriberId,
        envelopeSeq: envSeq,
        idempotencyKey,
        at: nowIso(),
      });
      enqueued.push({ subscriberId: sub.subscriberId, idempotencyKey });
    }
    return { envelopeSeq: envSeq, deduplicated: false, envelope, signature, enqueued };
  }

  // 稳定幂等键：同一订阅方 + 同一信封永远相同——超时重发、重启后再发都复用它。
  idempotencyKey(subscriberId, envelopeSeq) {
    return `ack:${subscriberId}:${envelopeSeq}`;
  }

  // ================= 投递状态机（由 dispatcher 调用）=================
  // 认领该订阅方队首可投递项：游标之后的【第一个未确认项】就是队首——
  // 它若 DEAD 则阻塞后续（返回 null，绝不跳号投递，保证恢复后顺序可验证）；
  // 过期 INFLIGHT 租约可被同一订阅方再次认领（超时后的“再次请求”）。
  claimHead(subscriberId, nowTs = Date.now()) {
    const sub = this.#requireSubscriber(subscriberId);
    const headSeq = sub.queueSeq.find((s) => {
      const it = this.itemsByIdem.get(this.idempotencyKey(subscriberId, s));
      return it && it.status !== ITEM_STATUS.ACKED;
    });
    if (headSeq == null) return null;
    const item = this.itemsByIdem.get(this.idempotencyKey(subscriberId, headSeq));
    if (item.status === ITEM_STATUS.DEAD) return null; // 死信阻塞，等待人工重新入队
    if (item.status === ITEM_STATUS.INFLIGHT) {
      if (item.leaseUntil > nowTs) return null; // 租约未到期，不重复并发投递
      this.#append('DELIVERY_RECLAIMED', {
        subscriberId, envelopeSeq: headSeq, idempotencyKey: item.idempotencyKey,
        attempt: item.attempts + 1, at: nowIso(nowTs), reason: 'LEASE_TIMEOUT',
      });
    }
    return this.itemsByIdem.get(this.idempotencyKey(subscriberId, headSeq));
  }

  // dispatcher 用稳定 idempotencyKey 建立租约（仅队首 PENDING/过期INFLIGHT 可建）。
  lease(subscriberId, envelopeSeq, leaseMs, nowTs = Date.now()) {
    const sub = this.#requireSubscriber(subscriberId);
    const item = this.#requireItem(subscriberId, envelopeSeq);
    const headSeq = sub.queueSeq.find((s) => {
      const it = this.itemsByIdem.get(this.idempotencyKey(subscriberId, s));
      return it && it.status !== ITEM_STATUS.ACKED;
    });
    if (headSeq !== envelopeSeq) {
      throw new DomainError('NOT_HEAD', '只能为队首未确认项建立租约（必须按事件顺序投递）', 409);
    }
    if (item.status === ITEM_STATUS.DEAD) {
      throw new DomainError('ITEM_DEAD', '该项在死信队列，需人工重新入队后才能投递', 409);
    }
    if (item.status === ITEM_STATUS.INFLIGHT && item.leaseUntil > nowTs) {
      throw new DomainError('LEASE_HELD', '租约仍有效，禁止重复并发投递', 409);
    }
    this.#append('DELIVERY_LEASED', {
      subscriberId, envelopeSeq, idempotencyKey: item.idempotencyKey,
      attempt: item.attempts + 1, leasedAt: nowIso(nowTs), leaseUntil: nowIso(nowTs + leaseMs),
    });
    return this.itemsByIdem.get(item.idempotencyKey);
  }

  // 投递请求已送达对端（对端返回 2xx；不等于确认，确认走确认 API）。
  recordSent(subscriberId, envelopeSeq, httpStatus, nowTs = Date.now()) {
    const item = this.#requireItem(subscriberId, envelopeSeq);
    this.#append('DELIVERY_SENT', {
      subscriberId, envelopeSeq, idempotencyKey: item.idempotencyKey,
      attempt: item.attempts, httpStatus, at: nowIso(nowTs),
    });
    return this.itemsByIdem.get(item.idempotencyKey);
  }

  // 投递请求失败（连接错误/非 2xx/超时）：累计 attempt；超过上限转死信（阻塞后续，保序）。
  recordFailure(subscriberId, envelopeSeq, reason, httpStatus = null, nowTs = Date.now()) {
    const item = this.#requireItem(subscriberId, envelopeSeq);
    const attempts = item.attempts;
    // 投递尝试（含首次）达到上限即转死信：DEAD 停在队首阻塞后续，等待人工重新入队。
    const dead = attempts >= this.maxAttempts;
    this.#append(dead ? 'DELIVERY_DEADLETTERED' : 'DELIVERY_FAILED', {
      subscriberId, envelopeSeq, idempotencyKey: item.idempotencyKey,
      attempt: attempts, reason: String(reason).slice(0, 300), httpStatus,
      at: nowIso(nowTs), dead, maxAttempts: this.maxAttempts,
    });
    return this.itemsByIdem.get(item.idempotencyKey);
  }

  // ================= 确认 API（幂等） =================
  confirm(input = {}) {
    const subscriberId = this.#requireStr(input.subscriberId, 'subscriberId');
    const sub = this.#requireSubscriber(subscriberId);
    const key = this.#requireStr(input.idempotencyKey, 'idempotencyKey');

    // 订阅方身份：HMAC-SHA256(secret, canonicalJSON 回执体)
    const { signature, ...body } = input;
    if (!signature) throw new DomainError('MISSING_SIGNATURE', '缺少 X-Subscriber-Signature', 401);
    const expected = hmacHex(sub.secret, canonicalJSON({ ...body }));
    if (!safeEqualHex(expected, signature)) {
      throw new DomainError('BAD_SUBSCRIBER_SIGNATURE', '订阅方回执签名不匹配', 401);
    }

    const dup = this.acksByKey.get(key);
    if (dup) {
      // 重复确认：返回与首次完全一致的结果，幂等且绝不重复推进游标。
      return { duplicate: true, idempotencyKey: key, cursor: this.subscribers.get(subscriberId).cursor, envelopeSeq: dup.envelopeSeq };
    }
    const item = this.itemsByIdem.get(key);
    if (!item || item.subscriberId !== subscriberId) {
      throw new DomainError('UNKNOWN_IDEMPOTENCY_KEY', '幂等键不属于该订阅方或不存在', 404);
    }
    if (item.status === ITEM_STATUS.DEAD) {
      throw new DomainError('ITEM_DEAD', '该项已进入死信队列，确认被拒绝（先人工重新入队）', 409);
    }
    if (item.status === ITEM_STATUS.PENDING) {
      throw new DomainError('NOT_DELIVERED', '订阅方尚未收到投递（PENDING），确认被拒绝', 409);
    }

    const prevCursor = sub.cursor;
    // 游标推进规则：只接受“队首”确认；提前到达的确认（INFLIGHT 的非队首项不可能发生，
    // 因为按序投递）——这里仍严格要求 envelopeSeq === cursor+1，任何跳号都不推进。
    if (item.envelopeSeq !== prevCursor + 1) {
      throw new DomainError('OUT_OF_ORDER_ACK', '确认乱序：必须先确认队首（游标下一条）', 409);
    }
    this.#append('DELIVERY_ACKED', {
      subscriberId, envelopeSeq: item.envelopeSeq, idempotencyKey: key,
      at: nowIso(), note: input.note ? String(input.note).slice(0, 200) : null,
    });
    return { duplicate: false, idempotencyKey: key, cursor: this.subscribers.get(subscriberId).cursor, envelopeSeq: item.envelopeSeq };
  }

  // ================= 死信人工重新入队 =================
  requeueDead(subscriberId, envelopeSeq, reason = '人工重新入队') {
    const item = this.#requireItem(subscriberId, envelopeSeq);
    if (item.status !== ITEM_STATUS.DEAD) {
      throw new DomainError('NOT_DEAD', '只有死信项可以重新入队', 409);
    }
    this.#append('DLQ_REQUEUED', {
      subscriberId, envelopeSeq, idempotencyKey: item.idempotencyKey,
      reason: String(reason).slice(0, 200), at: nowIso(),
    });
    return this.itemsByIdem.get(item.idempotencyKey);
  }

  // ================= 重启恢复 =================
  recoverInflightLeases(reason = 'MANUAL') {
    const recovered = [];
    for (const sub of this.subscribers.values()) {
      for (const seq of sub.queueSeq) {
        const item = this.itemsByIdem.get(this.idempotencyKey(sub.subscriberId, seq));
        if (item?.status === ITEM_STATUS.INFLIGHT) recovered.push({ subscriberId: sub.subscriberId, envelopeSeq: seq });
      }
    }
    if (!recovered.length) {
      return { recovered: [] };
    }
    this.#append('INFLIGHT_RECOVERED', { reason, at: nowIso(), items: recovered });
    return { recovered };
  }

  // ================= 证据：信封独立验签 =================
  verifyEnvelope(envelopeSeq) {
    const created = this.store.all().find(
      (r) => r.type === 'DR_NOTIFICATION' && r.payload.envelope?.envelopeSeq === envelopeSeq,
    );
    if (!created) throw new DomainError('ENVELOPE_NOT_FOUND', `信封 ${envelopeSeq} 不存在`, 404);
    const { envelope, signature } = created.payload;
    const key = this.keys.get(envelope.keyId);
    const canonical = canonicalEnvelope(envelope);
    const signatureValid = key ? verifyPayload(key.publicKeyPem, canonical, signature) : false;
    // 账本自校验：该记录仍在哈希链上（被改写会断链/哈希不符）。
    const chain = this.store.verify();
    return {
      envelopeSeq,
      notificationId: envelope.notificationId,
      type: envelope.type,
      keyId: envelope.keyId,
      keyRotated: !!key,
      keyActive: key ? key.active : false, // 轮换后旧信封此处为 false，仍须验证通过
      canonical,
      signature,
      signatureValid,
      ledgerSeq: created.seq,
      ledgerHash: created.hash,
      chainOk: chain.ok,
      immutabilityIntact: signatureValid && chain.ok,
    };
  }

  evidence(subscriberId, envelopeSeq) {
    const sub = this.#requireSubscriber(subscriberId);
    const item = this.#requireItem(subscriberId, envelopeSeq);
    const created = this.#notificationRecord(envelopeSeq);
    const trace = this.store.all()
      .filter((r) => {
        const p = r.payload;
        if (r.type === 'DR_NOTIFICATION') return p.envelope?.envelopeSeq === envelopeSeq;
        return p.subscriberId === subscriberId && p.envelopeSeq === envelopeSeq;
      })
      .map((r) => ({ seq: r.seq, ts: r.ts, type: r.type, payload: r.payload, hash: r.hash }));
    const verification = this.verifyEnvelope(envelopeSeq);
    return {
      subscriber: { subscriberId, status: sub.status, cursor: sub.cursor },
      item: itemView(item),
      envelope: created.payload.envelope,
      signature: created.payload.signature,
      verification,
      trace,
    };
  }

  #notificationRecord(envelopeSeq) {
    const rec = this.store.all().find(
      (r) => r.type === 'DR_NOTIFICATION' && r.payload.envelope?.envelopeSeq === envelopeSeq,
    );
    if (!rec) throw new DomainError('ENVELOPE_NOT_FOUND', `信封 ${envelopeSeq} 不存在`, 404);
    return rec;
  }

  // ================= 视图 =================
  stateView() {
    const subs = [...this.subscribers.values()].map((s) => this.subscriberView(s.subscriberId));
    return {
      mode: this.store.mode,
      writable: this.store.writable,
      recovery: this.store.recovery,
      chain: this.store.verify(),
      activeKeyId: this.activeKeyId,
      keys: this.keyOrder.map((id) => this.keyView(id)),
      envelopeSeq: this.envelopeSeq,
      subscribers: subs,
      deadLetterCount: subs.reduce((n, s) => n + s.queue.filter((q) => q.status === 'DEAD').length, 0),
      inflightCount: subs.reduce((n, s) => n + s.queue.filter((q) => q.status === 'INFLIGHT').length, 0),
    };
  }

  subscriberView(subscriberId) {
    const s = this.#requireSubscriber(subscriberId);
    const queue = s.queueSeq.map((seq) => itemView(this.itemsByIdem.get(this.idempotencyKey(subscriberId, seq))));
    return {
      subscriberId: s.subscriberId,
      displayName: s.displayName,
      callbackUrl: s.callbackUrl,
      topics: s.topics,
      status: s.status,
      challenge: s.challenge,
      createdAt: s.createdAt,
      cursor: s.cursor,
      lastDeliveryAt: s.lastDeliveryAt,
      queue,
    };
  }

  envelopeView(envelopeSeq) {
    const rec = this.#notificationRecord(envelopeSeq);
    return { ...rec.payload, ledgerSeq: rec.seq, ledgerHash: rec.hash };
  }

  // ================= 内部：追加 + apply（线上与重放共用） =================
  #append(type, payload) {
    if (!this.store.writable) {
      throw new DomainError('LOG_QUARANTINED', '通知账本已隔离（只读），拒绝写入', 503);
    }
    const rec = this.store.append(type, payload);
    this.#apply(rec, { replaying: false });
    return rec;
  }

  #apply(rec) {
    const p = rec.payload;
    switch (rec.type) {
      case 'KEY_ROTATED': {
        if (this.activeKeyId) this.keys.get(this.activeKeyId).active = false;
        this.keys.set(p.keyId, {
          keyId: p.keyId, alg: p.alg, publicKeyPem: p.publicKeyPem,
          privateKeyPem: p.privateKeyPem, rotatedAt: p.rotatedAt, active: true, retired: false,
        });
        this.keyOrder.push(p.keyId);
        this.activeKeyId = p.keyId;
        break;
      }
      case 'SUBSCRIBER_REGISTERED': {
        this.subscribers.set(p.subscriberId, {
          subscriberId: p.subscriberId,
          displayName: p.displayName,
          callbackUrl: p.callbackUrl,
          secret: p.secret,
          topics: p.topics,
          status: p.status,
          challenge: p.challenge,
          createdAt: p.createdAt,
          cursor: 0,
          queueSeq: [],
          lastDeliveryAt: null,
        });
        break;
      }
      case 'SUBSCRIPTION_VALIDATED': {
        const sub = this.subscribers.get(p.subscriberId);
        if (sub && sub.status === SUBSCRIPTION_STATUS.PENDING) {
          sub.status = p.accepted ? SUBSCRIPTION_STATUS.ACTIVE : SUBSCRIPTION_STATUS.REJECTED;
        }
        break;
      }
      case 'DR_NOTIFICATION': {
        const env = p.envelope;
        this.envelopes.set(env.envelopeSeq, env);
        this.sourcesSeen.set(env.sourceRef, env.envelopeSeq);
        this.envelopeSeq = Math.max(this.envelopeSeq, env.envelopeSeq);
        break;
      }
      case 'QUEUE_ENQUEUED': {
        const sub = this.subscribers.get(p.subscriberId);
        const item = {
          subscriberId: p.subscriberId,
          envelopeSeq: p.envelopeSeq,
          idempotencyKey: p.idempotencyKey,
          status: ITEM_STATUS.PENDING,
          attempts: 0,
          enqueuedAt: p.at,
          leasedAt: null,
          leaseUntil: null,
          sentAt: null,
          ackedAt: null,
          deadAt: null,
          requeuedAt: null,
          lastError: null,
          history: [{ type: 'ENQUEUED', at: p.at }],
        };
        this.itemsByIdem.set(p.idempotencyKey, item);
        sub.queueSeq.push(p.envelopeSeq);
        break;
      }
      case 'DELIVERY_LEASED': {
        const it = this.itemsByIdem.get(p.idempotencyKey);
        it.status = ITEM_STATUS.INFLIGHT;
        it.attempts = p.attempt;
        it.leasedAt = p.leasedAt;
        it.leaseUntil = Date.parse(p.leaseUntil);
        it.history.push({ type: 'LEASED', attempt: p.attempt, at: p.leasedAt });
        break;
      }
      case 'DELIVERY_RECLAIMED': {
        const it = this.itemsByIdem.get(p.idempotencyKey);
        it.history.push({ type: 'RECLAIMED', attempt: p.attempt, at: p.at, reason: p.reason });
        break;
      }
      case 'DELIVERY_SENT': {
        const it = this.itemsByIdem.get(p.idempotencyKey);
        // 防御任何顺序下的“迟到送达覆盖确认”：已确认（含重复投递送达）一律不回退状态。
        if (it.status === ITEM_STATUS.ACKED) {
          it.history.push({ type: 'SENT_IGNORED_AFTER_ACK', attempt: p.attempt, at: p.at });
          break;
        }
        it.status = ITEM_STATUS.INFLIGHT; // 已送达但未确认，仍占用租约
        it.sentAt = p.at;
        it.lastHttpStatus = p.httpStatus;
        const sub = this.subscribers.get(it.subscriberId);
        if (sub) sub.lastDeliveryAt = p.at;
        it.history.push({ type: 'SENT', attempt: p.attempt, httpStatus: p.httpStatus, at: p.at });
        break;
      }
      case 'DELIVERY_FAILED': {
        const it = this.itemsByIdem.get(p.idempotencyKey);
        it.status = ITEM_STATUS.PENDING; // 等待下一轮（仍按队首保序）
        it.leaseUntil = null;
        it.lastError = p.reason;
        it.lastHttpStatus = p.httpStatus;
        it.history.push({ type: 'FAILED', attempt: p.attempt, reason: p.reason, httpStatus: p.httpStatus, at: p.at });
        break;
      }
      case 'DELIVERY_DEADLETTERED': {
        const it = this.itemsByIdem.get(p.idempotencyKey);
        it.status = ITEM_STATUS.DEAD;
        it.leaseUntil = null;
        it.deadAt = p.at;
        it.lastError = p.reason;
        it.lastHttpStatus = p.httpStatus;
        it.history.push({ type: 'DEADLETTERED', attempt: p.attempt, reason: p.reason, at: p.at });
        break;
      }
      case 'DELIVERY_ACKED': {
        const it = this.itemsByIdem.get(p.idempotencyKey);
        it.status = ITEM_STATUS.ACKED;
        it.ackedAt = p.at;
        it.leaseUntil = null;
        it.history.push({ type: 'ACKED', at: p.at });
        this.acksByKey.set(p.idempotencyKey, p);
        const sub = this.subscribers.get(p.subscriberId);
        // 单调游标：仅当确认的正是“下一条”时 +1；重复确认在 confirm() 已被幂等拦截，
        // 这里再用 envelopeSeq 断言一次，任何路径都不可能重复推进或回退。
        if (it.envelopeSeq === sub.cursor + 1) sub.cursor = it.envelopeSeq;
        break;
      }
      case 'DLQ_REQUEUED': {
        const it = this.itemsByIdem.get(p.idempotencyKey);
        it.status = ITEM_STATUS.PENDING;
        it.attempts = 0;
        it.deadAt = null;
        it.leaseUntil = null;
        it.lastError = null;
        it.requeuedAt = p.at;
        it.history.push({ type: 'REQUEUED', reason: p.reason, at: p.at });
        break;
      }
      case 'INFLIGHT_RECOVERED': {
        for (const x of p.items) {
          const it = this.itemsByIdem.get(this.idempotencyKey(x.subscriberId, x.envelopeSeq));
          if (it && it.status === ITEM_STATUS.INFLIGHT) {
            it.status = ITEM_STATUS.PENDING;
            it.leaseUntil = null;
            it.history.push({ type: 'LEASE_RESET_ON_RESTART', at: p.at, reason: p.reason });
          }
        }
        break;
      }
      default:
        // 未知记录类型（未来版本写入）不致命，但不投影。
        break;
    }
  }

  #requireSubscriber(subscriberId) {
    const sub = this.subscribers.get(subscriberId);
    if (!sub) throw new DomainError('SUBSCRIBER_NOT_FOUND', `订阅方 ${subscriberId} 不存在`, 404);
    return sub;
  }

  #requireItem(subscriberId, envelopeSeq) {
    const key = this.idempotencyKey(subscriberId, Number(envelopeSeq));
    const item = this.itemsByIdem.get(key);
    if (!item) throw new DomainError('ITEM_NOT_FOUND', '投递项不存在', 404);
    return item;
  }

  #requireStr(v, field, min = 1, max = 5000) {
    if (typeof v !== 'string' || v.trim().length < min) {
      throw new DomainError('BAD_REQUEST', `字段 ${field} 缺失或非法`);
    }
    if (v.length > max) throw new DomainError('BAD_REQUEST', `字段 ${field} 过长（>${max}）`);
    return v.trim();
  }
}

// ================= 纯函数辅助 =================
function itemView(item) {
  return {
    subscriberId: item.subscriberId,
    envelopeSeq: item.envelopeSeq,
    idempotencyKey: item.idempotencyKey,
    status: item.status,
    attempts: item.attempts,
    enqueuedAt: item.enqueuedAt,
    leasedAt: item.leasedAt,
    leaseUntil: item.leaseUntil ? new Date(item.leaseUntil).toISOString() : null,
    sentAt: item.sentAt,
    ackedAt: item.ackedAt,
    deadAt: item.deadAt,
    requeuedAt: item.requeuedAt,
    lastError: item.lastError,
    lastHttpStatus: item.lastHttpStatus ?? null,
  };
}

function normalizeTopics(topics) {
  if (!topics) return ['*'];
  const arr = Array.isArray(topics) ? topics.map(String) : String(topics).split(',').map((s) => s.trim());
  const clean = arr.filter(Boolean);
  if (!clean.length) return ['*'];
  return clean;
}

// 主题匹配：'*' 全量；'dr.event.*' 前缀通配；否则精确匹配。
function topicMatches(subscribed, type, detail) {
  void detail;
  return subscribed.some((t) => {
    if (t === '*' || t === 'dr.event.*') return true;
    if (t.endsWith('.*')) return type.startsWith(t.slice(0, -1));
    return t === type;
  });
}

function nowIso(ts = Date.now()) {
  return new Date(ts).toISOString();
}

function randomChallenge() {
  return `ch_${randomBytes(16).toString('hex')}`;
}

function randomBytes6() {
  return randomBytes(6).toString('hex');
}

function safeEqualHex(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string' || a.length !== b.length) return false;
  try {
    return timingSafeEqual(Buffer.from(a), Buffer.from(b));
  } catch {
    return false;
  }
}
