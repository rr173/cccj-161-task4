// 内置“模拟订阅端”：独立的接收方视角，用来演示投递链路各种故障。
// 它有自己的持久账本（data/mock-received.log），重启后：
//  - 收信台账（按 idempotencyKey 去重）仍在 → 可演示重复投递不重复入账；
//  - 故障模式与预算仍在。
//
// 故障模式（POST /mock/subscribers/:id/mode 切换）：
//  OK            正常：验签 → 入账 → 立刻回调确认 API
//  FAIL_NEXT:n   接下来 n 次投递返回 503（订阅端暂时失败，聚合商按序重试，幂等键不变）
//  LOSE_ACK      投递 200 并入账，但【不发确认】→ 租约超时后聚合商用同一幂等键再投 →
//                订阅端按 idempotencyKey 去重，随后补发一次确认
//  DUPLICATE_ACK 正常入账，但向确认 API 连续 POST 两次 → 第二次必须幂等，游标不重复推进
//  REJECT_VALIDATION 订阅校验时不回显 challenge → 订阅被拒，收不到任何通知
import { EventStore } from '../src/store.js';
import { canonicalJSON } from '../src/util.js';
import { verifyPayload, hmacHex } from './crypto.js';

export const MOCK_MODES = Object.freeze(['OK', 'FAIL_NEXT', 'LOSE_ACK', 'DUPLICATE_ACK', 'REJECT_VALIDATION']);

export class MockSubscriber {
  constructor(file, { confirmBaseUrl, timeoutMs = 4000, logger = console } = {}) {
    this.store = new EventStore(file); // MOCK_RECEIVED / MOCK_MODE
    this.confirmBaseUrl = confirmBaseUrl; // 聚合商确认 API 基址（真实 HTTP 回环）
    this.timeoutMs = timeoutMs;
    this.log = logger;
    this.received = new Map(); // idempotencyKey -> 收信记录（payload 与账本记录共享，绝不可原地改写）
    this.seenCount = new Map(); // idempotencyKey -> 看到投递的总次数（内存易变计数，绝不能写进只增账本）
    this.modes = new Map(); // subscriberId -> { mode, budget }
    this.logs = []; // 订阅端视角的操作日志（内存，仅演示）
  }

  async load() {
    await this.store.load();
    for (const r of this.store.all()) {
      if (r.type === 'MOCK_RECEIVED') {
        this.received.set(r.payload.idempotencyKey, r.payload);
        this.seenCount.set(r.payload.idempotencyKey, r.payload.attemptsSeen ?? 1);
      } else if (r.type === 'MOCK_MODE') {
        this.modes.set(r.payload.subscriberId, { mode: r.payload.mode, budget: r.payload.budget });
      }
    }
  }

  modeOf(subscriberId) {
    return this.modes.get(subscriberId) || { mode: 'OK', budget: 0 };
  }

  // 切换故障模式；FAIL_NEXT 需要 budget（接下来失败多少次）
  setMode(subscriberId, mode, budget = 0) {
    if (!MOCK_MODES.includes(mode)) {
      const e = new Error(`未知模式：${mode}`);
      e.status = 400;
      throw e;
    }
    const b = mode === 'FAIL_NEXT' ? Math.max(1, Number(budget) || 1) : 0;
    this.store.append('MOCK_MODE', { subscriberId, mode, budget: b, at: new Date().toISOString() });
    this.modes.set(subscriberId, { mode, budget: b });
    this.#pushLog(subscriberId, 'MODE_SET', { mode, budget: b });
    return { subscriberId, mode, budget: b };
  }

  // ---- 订阅校验端点（GET，带 hub.challenge）----
  // REJECT_VALIDATION 模式拒绝；其他模式回显 challenge（聚合商还会校验签名，此处不放宽）。
  handleValidate(url, headers) {
    const challenge = url.searchParams.get('hub.challenge');
    // 路径形如 /mock/subscribers/:id/callback
    const m = url.pathname.match(/\/mock\/subscribers\/([^/]+)\/callback/);
    const subscriberId = m ? m[1] : url.pathname.split('/').pop();
    const { mode } = this.modeOf(subscriberId);
    void headers; // 订阅端可自行验证 X-Aggregator-Signature；模拟端以 challenge 回显为准
    if (mode === 'REJECT_VALIDATION') {
      this.#pushLog(subscriberId, 'VALIDATE_REJECTED', { challenge });
      return { status: 500, body: 'validation rejected by subscriber' };
    }
    this.#pushLog(subscriberId, 'VALIDATE_ACKED', { challenge });
    return { status: 200, body: challenge || '' };
  }

  // ---- 通知投递端点（POST 签名信封）----
  async handleCallback(subscriberId, body, headers, engine) {
    const idempotencyKey = headers['x-idempotency-key'];
    const envelopeSeq = Number(headers['x-envelope-seq']);
    const signature = headers['x-aggregator-signature'];
    const keyId = headers['x-key-id'];

    if (!idempotencyKey || !signature || !keyId) {
      return { status: 400, json: { error: '缺少投递头（idempotencyKey/signature/keyId）' } };
    }

    // 1) 验签：用 keyId 取公钥——轮换期间“旧在途通知”用旧公钥仍必须通过。
    const key = engine.keys.get(keyId);
    const canonical = canonicalJSON(body);
    const sigOk = key ? verifyPayload(key.publicKeyPem, canonical, signature) : false;
    if (!sigOk) {
      this.#pushLog(subscriberId, 'SIGNATURE_REJECTED', { envelopeSeq, keyId });
      return { status: 401, json: { error: '签名校验失败', keyId, envelopeSeq } };
    }

    const { mode, budget } = this.modeOf(subscriberId);

    // 2) 订阅端暂时失败：消耗预算，返回非 2xx，聚合商会用同一幂等键重试。
    if (mode === 'FAIL_NEXT' && budget > 0) {
      this.#persistMode(subscriberId, 'FAIL_NEXT', budget - 1);
      this.#pushLog(subscriberId, 'DELIBERATE_FAIL', { envelopeSeq, remaining: budget - 1 });
      return { status: 503, json: { error: '订阅端暂时不可用（模拟 FAIL_NEXT）', retry: true, remaining: budget - 1 } };
    }

    // 3) 幂等去重：同一幂等键重复投递（超时重发/重启后再发）只入账一次，但仍返回 200。
    const already = this.received.get(idempotencyKey);
    const duplicate = !!already;
    if (!already) {
      const rec = {
        idempotencyKey, envelopeSeq, keyId, notificationId: body.notificationId,
        type: body.type, eventId: body.eventId, receivedAt: new Date().toISOString(),
      };
      this.store.append('MOCK_RECEIVED', rec);
      this.received.set(idempotencyKey, rec);
      this.seenCount.set(idempotencyKey, 1);
    } else {
      // 重复投递只累加【内存】计数——账本记录是只增证据，永远不原地修改。
      this.seenCount.set(idempotencyKey, (this.seenCount.get(idempotencyKey) || 1) + 1);
      this.#pushLog(subscriberId, 'DUPLICATE_DELIVERY', { envelopeSeq, attemptsSeen: this.seenCount.get(idempotencyKey) });
    }
    this.#pushLog(subscriberId, 'RECEIVED', { envelopeSeq, duplicate, type: body.type, keyId });

    // 4) 按模式处理确认。
    // 注意：此处【只决定待发送的确认动作】，真正发送在响应投递端点之后（见 server.js
    // respond-then-ack 流程），避免“确认先于 DELIVERY_SENT 落账、状态被覆盖”的竞态。
    let ackPlan = { kind: 'SEND' };
    if (mode === 'LOSE_ACK') {
      this.#pushLog(subscriberId, 'ACK_LOST', { envelopeSeq, note: '确认被故意丢弃，等待超时重投后补发' });
      ackPlan = { kind: 'LOST' };
    } else if (mode === 'DUPLICATE_ACK') {
      ackPlan = { kind: 'TWICE' };
    }
    return {
      status: 200,
      json: { received: true, duplicate },
      // 由 server 在投递响应 flush 之后执行（异步、不阻塞响应）
      afterRespond: () => this.#executeAckPlan(engine, subscriberId, idempotencyKey, envelopeSeq, ackPlan, duplicate),
    };
  }

  async #executeAckPlan(engine, subscriberId, idempotencyKey, envelopeSeq, plan, duplicate) {
    if (plan.kind === 'LOST') return;
    if (plan.kind === 'TWICE') {
      const r1 = await this.#sendConfirmation(engine, subscriberId, idempotencyKey, envelopeSeq);
      const r2 = await this.#sendConfirmation(engine, subscriberId, idempotencyKey, envelopeSeq);
      this.#pushLog(subscriberId, 'DUPLICATE_ACK_SENT', { envelopeSeq, first: r1, second: r2 });
      return { sentTwice: true, first: r1, second: r2 };
    }
    const r = await this.#sendConfirmation(engine, subscriberId, idempotencyKey, envelopeSeq);
    return { sent: true, duplicate, result: r };
  }

  // 手工补发确认（演示“确认丢失”后，超时重投让订阅端再次看到同一信封，再补发确认）。
  async resendAck(engine, subscriberId, envelopeSeq) {
    const idempotencyKey = engine.idempotencyKey(subscriberId, envelopeSeq);
    if (!this.received.has(idempotencyKey)) {
      const e = new Error('订阅端台账中没有该信封，无法补发确认');
      e.status = 404;
      throw e;
    }
    const r = await this.#sendConfirmation(engine, subscriberId, idempotencyKey, envelopeSeq);
    this.#pushLog(subscriberId, 'ACK_RESENT', { envelopeSeq, result: r });
    return { envelopeSeq, idempotencyKey, result: r };
  }

  inbox(subscriberId) {
    const prefix = `ack:${subscriberId}:`;
    return [...this.received.values()]
      .filter((r) => r.idempotencyKey.startsWith(prefix))
      .map((r) => ({ ...r, attemptsSeen: this.seenCount.get(r.idempotencyKey) ?? r.attemptsSeen ?? 1 }))
      .sort((a, b) => a.envelopeSeq - b.envelopeSeq);
  }

  subscriberLogs(subscriberId) {
    return this.logs.filter((l) => l.subscriberId === subscriberId);
  }

  // ---- 真实 HTTP 回调聚合商确认 API（HMAC 签名回执）----
  async #sendConfirmation(engine, subscriberId, idempotencyKey, envelopeSeq) {
    const sub = engine.subscribers.get(subscriberId);
    if (!sub) return { ok: false, error: '订阅方不存在（聚合商侧）' };
    const ackBody = { subscriberId, idempotencyKey };
    const signature = hmacHex(sub.secret, canonicalJSON({ ...ackBody }));
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error('timeout')), this.timeoutMs);
    try {
      const res = await fetch(`${this.confirmBaseUrl}/api/confirmations`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          'X-Subscriber-Signature': signature,
        },
        body: canonicalJSON({ ...ackBody, signature }),
        signal: controller.signal,
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        this.#pushLog(subscriberId, 'CONFIRM_REJECTED', { envelopeSeq, httpStatus: res.status, body: json });
        return { ok: false, httpStatus: res.status, body: json };
      }
      this.#pushLog(subscriberId, 'CONFIRM_SENT', { envelopeSeq, httpStatus: res.status, duplicate: json.duplicate, cursor: json.cursor });
      return { ok: true, httpStatus: res.status, ...json };
    } catch (err) {
      const reason = err?.name === 'AbortError' ? '确认请求超时' : `确认请求失败：${err?.message || err}`;
      this.#pushLog(subscriberId, 'CONFIRM_ERROR', { envelopeSeq, reason });
      return { ok: false, error: reason };
    } finally {
      clearTimeout(timer);
    }
  }

  #persistMode(subscriberId, mode, budget) {
    this.store.append('MOCK_MODE', { subscriberId, mode, budget, at: new Date().toISOString() });
    this.modes.set(subscriberId, { mode, budget });
  }

  #pushLog(subscriberId, kind, detail) {
    this.logs.push({ at: new Date().toISOString(), subscriberId, kind, detail });
    if (this.logs.length > 500) this.logs.shift();
  }
}
