// 领域内核：命令 -> 校验 -> 追加不可变记录 -> 同步更新投影。
// Node 单线程 + 全程同步，命令严格按到达次序串行裁决，无需加锁即可保证一致。
import { Clock } from './clock.js';
import { computeBaseline } from './baseline.js';
import { buildReport } from './adjudication.js';
import { seed, BASE_NOW, meterProfile, mulberry32, strSeed, dayValuesForKey } from './seed.js';
import { EventStore, LogQuarantineError } from './store.js';
import {
  INTERVAL_MS,
  INTERVALS_PER_DAY,
  dayKey,
  floorInterval,
  intervalIndex,
  intervalStarts,
  iso,
  round3,
  uid,
  sha256Hex,
  canonicalJSON,
} from './util.js';

export class DomainError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.code = code;
    this.details = details;
  }
}

const RECEIPT_STATUSES = ['ACCEPTED', 'DECLINED', 'EXECUTING', 'RECOVERED'];
const EVENT_TERMINAL = new Set(['RECALLED', 'CLOSED']);

// 履约异议处置：自裁定封存（报告签发）时刻起的受理时限。
// 时限锚定在只增记录里（deadlineMs 随 OBJECTION_FILED 落盘），重启后不变化。
export const OBJECTION_WINDOW_MS = 24 * 60 * 60 * 1000;
export const OBJECTION_STATUS = Object.freeze({ PENDING: 'PENDING', APPROVED: 'APPROVED', REJECTED: 'REJECTED' });
const REVIEW_DECISIONS = ['APPROVED', 'REJECTED'];
const HASH64_RE = /^[0-9a-f]{64}$/;

// 事件窗口内真实出力（仿真）：基线画像减去执行设备的削减量
function simIntervalActual(meterIdx, ts, totalShedKw) {
  const d = new Date(ts);
  const dow = d.getUTCDay();
  const h = d.getUTCHours() + d.getUTCMinutes() / 60;
  const rnd = mulberry32(strSeed(`live-${meterIdx}-${ts}`));
  const raw = meterProfile(meterIdx, dow, h, rnd);
  return Math.max(0, round3(raw - totalShedKw));
}

export class Kernel {
  constructor(store, { boot = true } = {}) {
    this.store = store;
    this.clock = new Clock(BASE_NOW);
    if (boot) this.bootstrap();
  }

  static freshMemory(now = BASE_NOW) {
    const store = new EventStore(null);
    seed(store, now);
    return new Kernel(store);
  }

  bootstrap() {
    // 空库 -> 写入种子（日志隔离时绝不写入，直接按已验证通过的前缀重放）
    if (this.store.all().length === 0 && this.store.writable) {
      seed(this.store, BASE_NOW);
    }
    this.replay();
  }

  reset() {
    this.store.truncate();
    seed(this.store, BASE_NOW);
    this.clock = new Clock(BASE_NOW);
    this.replay();
  }

  replay() {
    this.s = {
      aggregators: new Map(),
      meters: new Map(),
      devices: new Map(),
      meterIndex: new Map(), // meterId -> idx（仿真画像用）
      days: new Map(), // meterId -> Map(dayKey -> arr96)
      eventDays: new Map(), // meterId -> Set(dayKey)
      arrivals: [], // {meterId,ts,value,arrivedAt,source}
      events: new Map(),
      participants: new Map(), // eventId -> Map(deviceId -> snapshot)
      claims: new Map(), // deviceId -> claim
      buffers: new Map(), // deviceId -> [buffered receipt]
      inbox: new Set(), // deviceId:receiptId
      baselines: new Map(), // eventId -> [versions]
      reports: new Map(), // eventId -> [reports]
      objections: new Map(), // objectionId -> 异议聚合（只增投影）
      objectionIds: [], // 受理顺序
      objectionsByEvent: new Map(), // eventId -> Set(objectionId)
      objectionRequests: new Map(), // 客户请求号 -> 原请求摘要（受理/补充的幂等与 409）
      sheds: new Map(), // deviceId -> Map(ts -> kw)
      offline: new Set(),
      genCursor: floorInterval(BASE_NOW),
    };
    this.clock = new Clock(BASE_NOW);
    for (const rec of this.store.all()) this.apply(rec);
  }

  now() {
    return this.clock.now();
  }

  // ---------- 投影 ----------

  apply(rec) {
    const s = this.s;
    const p = rec.payload;
    switch (rec.type) {
      case 'SEED_CLOCK':
        this.clock.set(p.now);
        s.genCursor = floorInterval(p.now);
        break;
      case 'SEED_AGGREGATOR':
        s.aggregators.set(p.id, { ...p });
        break;
      case 'SEED_METER':
        s.meters.set(p.id, { ...p });
        s.meterIndex.set(p.id, s.meterIndex.size);
        s.days.set(p.id, new Map());
        s.eventDays.set(p.id, new Set());
        break;
      case 'SEED_DEVICE':
        s.devices.set(p.id, { ...p });
        s.sheds.set(p.id, new Map());
        break;
      case 'SEED_DAY_READINGS':
        this.#putDay(p.meterId, p.day, p.values, rec.ts);
        break;
      case 'DAY_READINGS_LOADED':
        this.#putDay(p.meterId, p.day, p.values, rec.ts);
        break;
      case 'SIM_TICK':
        this.clock.set(p.to);
        break;
      case 'SIM_METER_OFFLINE':
        if (p.offline) s.offline.add(p.meterId);
        else s.offline.delete(p.meterId);
        break;
      case 'EVENT_PUBLISHED':
        s.events.set(p.id, { ...p, status: 'SCHEDULED' });
        s.participants.set(p.id, new Map());
        s.baselines.set(p.id, []);
        s.reports.set(p.id, []);
        s.eventDays.get(p.meterId)?.add(dayKey(p.startMs));
        break;
      case 'BASELINE_FROZEN':
      case 'BASELINE_CORRECTED':
        s.baselines.get(p.eventId)?.push({ ...p });
        break;
      case 'EVENT_ACTIVATED': {
        const ev = s.events.get(p.eventId);
        ev.status = 'ACTIVE';
        ev.activatedAt = p.at;
        break;
      }
      case 'EVENT_CANCELLED': {
        const ev = s.events.get(p.eventId);
        ev.status = 'RECALLED';
        ev.recalledAt = p.at;
        ev.cancelReason = p.reason;
        break;
      }
      case 'EVENT_CLOSED': {
        const ev = s.events.get(p.eventId);
        ev.status = 'CLOSED';
        ev.closedAt = p.at;
        break;
      }
      case 'DEVICE_CLAIMED': {
        s.claims.set(p.deviceId, {
          eventId: p.eventId,
          aggregatorId: p.aggregatorId,
          status: null,
          requestedKw: 0,
          appliedSeq: 0,
          lastReceiptId: null,
          lastAckAt: null,
          claimedAt: p.at,
        });
        const part = s.participants.get(p.eventId);
        if (!part.has(p.deviceId)) {
          part.set(p.deviceId, {
            deviceId: p.deviceId,
            aggregatorId: p.aggregatorId,
            status: 'ENROLLED',
            requestedKw: 0,
            appliedSeq: 0,
            lastAckPhase: null,
            lastAckAt: null,
          });
        }
        break;
      }
      case 'DEVICE_RELEASED': {
        const claim = s.claims.get(p.deviceId);
        if (claim && claim.eventId === p.eventId) s.claims.delete(p.deviceId);
        break;
      }
      case 'RECEIPT_BUFFERED':
        if (!s.buffers.has(p.deviceId)) s.buffers.set(p.deviceId, []);
        s.buffers.get(p.deviceId).push({ ...p, bufferedRecordSeq: rec.seq });
        break;
      case 'RECEIPT_DROPPED': {
        const claim = s.claims.get(p.deviceId);
        if (claim && claim.eventId === p.eventId) claim.appliedSeq = Math.max(claim.appliedSeq, p.clientSeq);
        break;
      }
      case 'DEVICE_RECEIPT': {
        const { eventId, deviceId, status, requestedKw, clientSeq, arrivalTs, sentAt } = p;
        const claim = s.claims.get(deviceId);
        if (claim && claim.eventId === eventId) {
          claim.status = status;
          if (typeof requestedKw === 'number') claim.requestedKw = requestedKw;
          claim.appliedSeq = Math.max(claim.appliedSeq, clientSeq || claim.appliedSeq + 1);
          claim.lastReceiptId = p.receiptId;
          claim.lastAckAt = arrivalTs;
        }
        const part = s.participants.get(eventId)?.get(deviceId);
        if (part) {
          part.status = status;
          part.lastAckPhase = status;
          part.lastAckAt = arrivalTs;
          part.sentAt = sentAt || null;
          if (typeof requestedKw === 'number') part.requestedKw = requestedKw;
          part.appliedSeq = Math.max(part.appliedSeq, clientSeq || part.appliedSeq + 1);
        }
        s.inbox.add(`${deviceId}:${p.receiptId}`);
        break;
      }
      case 'READINGS_BATCH': {
        for (const it of p.items) {
          s.arrivals.push({ meterId: p.meterId, ts: it.ts, value: it.value, arrivedAt: p.arrivedAt, source: p.source });
          const dayMap = s.days.get(p.meterId);
          const dk = dayKey(it.ts);
          let arr = dayMap.get(dk);
          if (!arr) {
            arr = new Array(INTERVALS_PER_DAY).fill(null);
            dayMap.set(dk, arr);
          }
          arr[intervalIndex(it.ts)] = it.value;
        }
        for (const sh of p.sheds || []) {
          s.sheds.get(sh.deviceId)?.set(sh.ts, sh.kw);
          s.genCursor = Math.max(s.genCursor, sh.ts + INTERVAL_MS);
        }
        if ((p.items || []).length) {
          const maxTs = Math.max(...p.items.map((i) => i.ts));
          s.genCursor = Math.max(s.genCursor, maxTs + INTERVAL_MS);
        }
        break;
      }
      case 'REPORT_ISSUED':
        s.reports.get(p.report.eventId)?.push(p.report);
        break;
      case 'OBJECTION_FILED': {
        s.objections.set(p.id, {
          id: p.id,
          eventId: p.eventId,
          reportVersion: p.reportVersion,
          reportId: p.reportId,
          reportHash: p.reportHash, // 被质疑标的内容指纹（封存后永不改写）
          aggregatorId: p.aggregatorId,
          reason: p.reason,
          status: 'PENDING',
          filedAt: p.at,
          deadlineMs: p.deadlineMs,
          materials: [],
          review: null,
        });
        s.objectionIds.push(p.id);
        if (!s.objectionsByEvent.has(p.eventId)) s.objectionsByEvent.set(p.eventId, new Set());
        s.objectionsByEvent.get(p.eventId).add(p.id);
        s.objectionRequests.set(p.requestId, {
          kind: 'FILE',
          bodyHash: p.bodyHash,
          objectionId: p.id,
          at: p.at,
        });
        break;
      }
      case 'OBJECTION_MATERIAL_APPENDED': {
        const obj = s.objections.get(p.objectionId);
        obj.materials.push({
          seq: p.seq,
          summary: p.summary,
          content: p.content,
          attachedAt: p.at,
          requestId: p.requestId,
        });
        s.objectionRequests.set(p.requestId, {
          kind: 'MATERIAL',
          bodyHash: p.bodyHash,
          objectionId: p.objectionId,
          materialSeq: p.seq,
          at: p.at,
        });
        break;
      }
      case 'OBJECTION_REVIEWED': {
        const obj = s.objections.get(p.objectionId);
        obj.status = p.decision;
        obj.review = {
          decision: p.decision,
          rationale: p.rationale,
          reviewerId: p.reviewerId,
          reviewedAt: p.at,
        };
        break;
      }
      default:
        break;
    }
  }

  #putDay(meterId, key, values, _arrivedAt) {
    const s = this.s;
    let day = s.days.get(meterId).get(key);
    if (!day) {
      day = new Array(INTERVALS_PER_DAY).fill(null);
      s.days.get(meterId).set(key, day);
    }
    values.forEach((v, i) => {
      if (typeof v === 'number') day[i] = v;
    });
  }

  #readingsByDay(meterId) {
    const out = {};
    for (const [k, arr] of this.s.days.get(meterId)) out[k] = arr;
    return out;
  }

  // ---------- 命令 ----------

  dispatch(cmd) {
    // 恢复契约：日志隔离期间所有业务写入（含 RESET）一律拒绝，返回 503。
    // 只读接口（stateView/eventView/report 查询/链校验）不受影响。
    if (!this.store.writable) throw new LogQuarantineError(this.store.recovery);
    const now = this.now();
    switch (cmd.type) {
      case 'PUBLISH_EVENT':
        return this.publishEvent(cmd, now);
      case 'CANCEL_EVENT':
        return this.cancelEvent(cmd, now);
      case 'CLAIM_DEVICE':
        return this.claimDevice(cmd, now);
      case 'DEVICE_RECEIPT':
        return this.deviceReceipt(cmd, now);
      case 'INGEST_READINGS':
        return this.ingestReadings(cmd.items || [], { source: cmd.source || 'API', now });
      case 'LOAD_MISSING_DAY':
        return this.loadMissingDay(cmd, now);
      case 'BACKFILL_ACTUALS':
        return this.backfillActuals(cmd, now);
      case 'SET_METER_OFFLINE':
        return this.setMeterOffline(cmd, now);
      case 'FILE_OBJECTION':
        return this.fileObjection(cmd, now);
      case 'APPEND_OBJECTION_MATERIAL':
        return this.appendObjectionMaterial(cmd, now);
      case 'REVIEW_OBJECTION':
        return this.reviewObjection(cmd, now);
      case 'TICK':
        return this.tick(cmd.minutes ?? 15, now);
      case 'RESET':
        this.reset();
        return { reset: true, now: this.now() };
      default:
        throw new DomainError('UNKNOWN_COMMAND', `未知命令 ${cmd.type}`);
    }
  }

  requireMeter(meterId) {
    const m = this.s.meters.get(meterId);
    if (!m) throw new DomainError('NOT_FOUND', `计量点 ${meterId} 不存在`, { meterId });
    return m;
  }
  requireEvent(id) {
    const ev = this.s.events.get(id);
    if (!ev) throw new DomainError('NOT_FOUND', `事件 ${id} 不存在`, { eventId: id });
    return ev;
  }

  publishEvent(cmd, now) {
    const meterId = cmd.meterId;
    this.requireMeter(meterId);
    const alg = cmd.algorithm || 'MID_8_OF_10';
    if (!['LAST_10_AVG', 'MID_8_OF_10', 'LAST_5_AVG'].includes(alg)) {
      throw new DomainError('BAD_REQUEST', `未知基线算法 ${alg}`);
    }
    const targetKw = Number(cmd.targetKw);
    if (!Number.isFinite(targetKw) || targetKw <= 0) {
      throw new DomainError('BAD_REQUEST', 'targetKw 必须为正数');
    }
    let startMs = Number(cmd.startMs);
    let endMs = Number(cmd.endMs);
    if (!Number.isFinite(startMs)) startMs = floorInterval(now) + 2 * INTERVAL_MS;
    if (!Number.isFinite(endMs)) endMs = startMs + 4 * 4 * INTERVAL_MS; // 默认 4 小时
    if (startMs % INTERVAL_MS !== 0 || endMs % INTERVAL_MS !== 0) {
      throw new DomainError('BAD_REQUEST', '时窗端点必须对齐 15 分钟区间');
    }
    if (endMs <= startMs) throw new DomainError('BAD_REQUEST', '结束时间必须晚于开始时间');
    if (startMs <= now) throw new DomainError('BAD_REQUEST', '事件开始时间必须在未来（基线在开始时冻结）');

    // 同一计量点、时窗重叠且仍有效（未撤销/关闭）-> 拒绝
    for (const ev of this.s.events.values()) {
      if (ev.meterId !== meterId || EVENT_TERMINAL.has(ev.status)) continue;
      if (startMs < ev.endMs && ev.startMs < endMs) {
        throw new DomainError('OVERLAPPING_WINDOW', `与有效事件 ${ev.id} 时窗重叠`, {
          conflictEventId: ev.id,
          status: ev.status,
        });
      }
    }

    const id = cmd.eventId || uid('EV');
    const payload = {
      id,
      meterId,
      meterName: this.s.meters.get(meterId).name,
      aggregatorId: cmd.aggregatorId || 'OPS',
      algorithm: alg,
      targetKw: round3(targetKw),
      startMs,
      endMs,
      windowStartIdx: intervalIndex(startMs),
      windowCount: (endMs - startMs) / INTERVAL_MS,
      createdAt: now,
      createdBy: cmd.createdBy || '运营方',
    };
    this.store.append('EVENT_PUBLISHED', payload, now);
    this.apply(this.store.all().at(-1));
    return { event: this.eventView(id) };
  }

  cancelEvent(cmd, now) {
    const ev = this.requireEvent(cmd.eventId);
    if (EVENT_TERMINAL.has(ev.status)) {
      throw new DomainError('EVENT_TERMINAL', `事件已处于终态 ${ev.status}`, { status: ev.status });
    }
    const released = [];
    this.store.append('EVENT_CANCELLED', { eventId: ev.id, at: now, reason: cmd.reason || '运营方撤销' }, now);
    this.apply(this.store.all().at(-1));

    // 并发要点：撤销立即释放设备归属；已执行设备事后回报的 RECOVERED 仍会被接受为迟到终态
    for (const [deviceId, claim] of [...this.s.claims.entries()]) {
      if (claim.eventId !== ev.id) continue;
      this.store.append('DEVICE_RELEASED', { eventId: ev.id, deviceId, reason: 'EVENT_RECALLED', at: now }, now);
      this.apply(this.store.all().at(-1));
      released.push(deviceId);
    }

    let report = null;
    if (ev.startMs <= now) {
      // 时窗已开始：确保基线已冻结（取消与开始并发时），出具撤销版原始裁决
      if (!this.s.baselines.get(ev.id).length) {
        try {
          this.freezeBaseline(ev, now);
        } catch (e) {
          // 数据不足无法冻结时，仍完成撤销
        }
      }
      // 事件已激活（EVENT_ACTIVATED 在冻结时写入）会把状态覆盖回 ACTIVE，重新置为撤销
      this.store.append('EVENT_CANCELLED', { eventId: ev.id, at: now, reason: 'CANCEL_AFTER_START' }, now);
      this.apply(this.store.all().at(-1));
      if (this.s.baselines.get(ev.id).length) {
        report = this.issueReport(ev, { kind: 'ORIGINAL', reason: 'EVENT_RECALLED', cutoffTs: now });
      }
    }
    return { eventId: ev.id, status: 'RECALLED', released, report };
  }

  claimDevice(cmd, now) {
    const ev = this.requireEvent(cmd.eventId);
    const dev = this.s.devices.get(cmd.deviceId);
    if (!dev) throw new DomainError('NOT_FOUND', `设备 ${cmd.deviceId} 不存在`);
    if (dev.meterId !== ev.meterId) {
      throw new DomainError('METER_MISMATCH', '设备不属于该事件的计量点', {
        deviceMeter: dev.meterId, eventMeter: ev.meterId,
      });
    }
    if (!this.s.aggregators.has(cmd.aggregatorId)) {
      throw new DomainError('NOT_FOUND', `聚合商 ${cmd.aggregatorId} 不存在`);
    }
    if (EVENT_TERMINAL.has(ev.status)) {
      throw new DomainError('EVENT_TERMINAL', `事件 ${ev.status}，不可再声明设备`, { status: ev.status });
    }
    const existing = this.s.claims.get(cmd.deviceId);
    if (existing) {
      if (existing.eventId === ev.id) {
        if (existing.aggregatorId === cmd.aggregatorId) {
          return { idempotent: true, claim: existing, event: this.eventView(ev.id) };
        }
        throw new DomainError('DEVICE_ALREADY_CLAIMED', '该设备已由另一聚合商在此事件声明', {
          ownerAggregatorId: existing.aggregatorId,
        });
      }
      const other = this.s.events.get(existing.eventId);
      if (other && !EVENT_TERMINAL.has(other.status)) {
        throw new DomainError('DEVICE_BUSY', '设备已归属另一有效事件（一台设备同一时刻只能属于一个有效事件）', {
          currentEventId: existing.eventId,
          currentStatus: other.status,
          ownerAggregatorId: existing.aggregatorId,
        });
      }
    }
    const payload = { eventId: ev.id, deviceId: cmd.deviceId, aggregatorId: cmd.aggregatorId, at: now };
    this.store.append('DEVICE_CLAIMED', payload, now);
    this.apply(this.store.all().at(-1));
    return { claim: payload, event: this.eventView(ev.id) };
  }

  // 回执状态机：能否在当前事件状态/当前设备阶段下应用
  #checkPhase(status, ev, claimOrPart) {
    if (ev.status === 'RECALLED') {
      if (status === 'RECOVERED') return; // 撤销后迟到的恢复回报：只形成记录
      throw new DomainError('EVENT_RECALLED', '事件已撤销；仅接受迟到的 RECOVERED 回报');
    }
    if (ev.status === 'CLOSED') {
      if (status === 'RECOVERED') return;
      throw new DomainError('EVENT_TERMINAL', '事件已结束；仅接受迟到的 RECOVERED 回报');
    }
    const cur = claimOrPart?.status;
    switch (status) {
      case 'ACCEPTED':
        if (cur === 'EXECUTING') throw new DomainError('PHASE_CONFLICT', '设备已在执行，不能回退到 ACCEPTED');
        return;
      case 'DECLINED':
        if (cur === 'EXECUTING') throw new DomainError('PHASE_CONFLICT', '执行中请用 RECOVERED 结束，而非 DECLINED');
        if (cur === 'DECLINED') throw new DomainError('PHASE_CONFLICT', '设备已拒绝');
        return;
      case 'EXECUTING':
        if (cur === 'DECLINED') throw new DomainError('PHASE_CONFLICT', '设备已拒绝，不能进入执行');
        if (cur === 'RECOVERED') throw new DomainError('PHASE_CONFLICT', '设备已恢复，不能重新执行');
        return;
      case 'RECOVERED':
        if (cur !== 'EXECUTING') throw new DomainError('PHASE_CONFLICT', '只有执行中的设备可以回报 RECOVERED');
        return;
      default:
        throw new DomainError('BAD_REQUEST', `未知回执状态 ${status}`);
    }
  }

  deviceReceipt(cmd, now) {
    const ev = this.requireEvent(cmd.eventId);
    const dev = this.s.devices.get(cmd.deviceId);
    if (!dev) throw new DomainError('NOT_FOUND', `设备 ${cmd.deviceId} 不存在`);
    if (!RECEIPT_STATUSES.includes(cmd.status)) {
      throw new DomainError('BAD_REQUEST', `回执状态必须是 ${RECEIPT_STATUSES.join('/')}`);
    }
    const receiptId = cmd.receiptId || uid('RCP');
    const dedupKey = `${cmd.deviceId}:${receiptId}`;
    if (this.s.inbox.has(dedupKey)) {
      const claim = this.s.claims.get(cmd.deviceId);
      return { duplicate: true, receiptId, watermark: claim?.appliedSeq ?? null, status: claim?.status ?? null };
    }

    const claim = this.s.claims.get(cmd.deviceId);
    const participant = this.s.participants.get(ev.id)?.get(cmd.deviceId);
    const attached = claim && claim.eventId === ev.id;
    const lateTerminal = !attached && (ev.status === 'RECALLED' || ev.status === 'CLOSED') &&
      cmd.status === 'RECOVERED' && participant;

    if (!attached && !lateTerminal) {
      // 事件终态后，曾参与的设备迟到非 RECOVERED 回报：给出明确的阶段错误
      if (participant && ev.status === 'RECALLED') {
        throw new DomainError('EVENT_RECALLED', '事件已撤销，设备归属已释放；仅接受迟到的 RECOVERED 回报');
      }
      if (participant && ev.status === 'CLOSED') {
        throw new DomainError('EVENT_TERMINAL', '事件已结束，设备归属已释放；仅接受迟到的 RECOVERED 回报');
      }
      throw new DomainError('NOT_CLAIMED', '设备未（或不再）归属此事件，回执被拒绝', {
        eventStatus: ev.status,
      });
    }

    // 乱序水位（显式 clientSeq）
    let buffered = false;
    const seq = Number.isFinite(Number(cmd.clientSeq)) ? Number(cmd.clientSeq) : null;
    if (seq !== null) {
      const applied = (attached ? claim.appliedSeq : participant.appliedSeq) || 0;
      if (seq <= applied) {
        throw new DomainError('STALE_SEQ', `回执序号 ${seq} 已落后于水位 ${applied}（乱序迟到）`, { watermark: applied });
      }
      if (seq > applied + 1) {
        buffered = true;
        const payload = {
          eventId: ev.id, deviceId: cmd.deviceId, aggregatorId: cmd.aggregatorId || claim?.aggregatorId || participant.aggregatorId,
          receiptId, clientSeq: seq, status: cmd.status, requestedKw: cmd.requestedKw ?? null,
          sentAt: cmd.sentAt || now,
        };
        this.store.append('RECEIPT_BUFFERED', payload, now);
        this.apply(this.store.all().at(-1));
        return { buffered: true, receiptId, expectSeq: applied + 1, watermark: applied };
      }
    }

    // 立即应用（缓冲排水也复用此路径）
    const applied = this.#applyReceipt({ ...cmd, receiptId, clientSeq: seq }, ev, attached ? claim : participant, now, lateTerminal);
    const drained = this.#drainBuffer(ev, now);
    return {
      accepted: true,
      receiptId,
      status: cmd.status,
      watermark: applied.watermark,
      drained: drained.length,
      drainedReceipts: drained,
    };
  }

  #applyReceipt(cmd, ev, claimOrPart, now, lateTerminal) {
    this.#checkPhase(cmd.status, ev, claimOrPart);
    const payload = {
      eventId: ev.id,
      deviceId: cmd.deviceId,
      aggregatorId: cmd.aggregatorId || claimOrPart.aggregatorId,
      receiptId: cmd.receiptId,
      clientSeq: cmd.clientSeq ?? null,
      status: cmd.status,
      requestedKw: cmd.requestedKw ?? null,
      sentAt: cmd.sentAt || now,
      arrivalTs: now,
      lateAfterTerminal: !!lateTerminal,
    };
    this.store.append('DEVICE_RECEIPT', payload, now);
    this.apply(this.store.all().at(-1));
    const claim = this.s.claims.get(cmd.deviceId);
    const part = this.s.participants.get(ev.id)?.get(cmd.deviceId);
    return { watermark: Math.max(claim?.appliedSeq ?? 0, part?.appliedSeq ?? 0) };
  }

  #drainBuffer(ev, now) {
    const drained = [];
    // 每个设备独立排水（不同设备的水位互不影响）；只处理本事件的缓冲
    for (const [deviceId, buf] of [...this.s.buffers.entries()]) {
      if (!buf.length) continue;
      const own = buf.filter((b) => b.eventId === ev.id);
      if (!own.length) continue;
      let progressed = true;
      while (progressed) {
        progressed = false;
        const claim = this.s.claims.get(deviceId);
        const part = this.s.participants.get(ev.id)?.get(deviceId);
        const attached = claim && claim.eventId === ev.id;
        const terminalLate = !attached && (ev.status === 'RECALLED' || ev.status === 'CLOSED');
        const holder = attached ? claim : terminalLate ? part : null;
        if (!holder) break;
        const wm = attached ? claim.appliedSeq : part.appliedSeq;
        const expected = wm + 1;
        const idx = own.findIndex((b) => b.clientSeq === expected);
        if (idx === -1) break;
        const [b] = own.splice(idx, 1);
        buf.splice(buf.indexOf(b), 1);
        try {
          this.#applyReceipt(
            { ...b, clientSeq: b.clientSeq, sentAt: b.sentAt },
            ev, holder, now, !attached && b.status === 'RECOVERED'
          );
          drained.push({ receiptId: b.receiptId, clientSeq: b.clientSeq, status: b.status });
        } catch (e) {
          // 缺口补齐后，占位的乱序回执可能在新阶段下非法：丢弃但消耗序号，保持水位明确
          this.store.append('RECEIPT_DROPPED', {
            eventId: ev.id, deviceId, clientSeq: b.clientSeq, receiptId: b.receiptId, reason: e.code,
          }, now);
          this.apply(this.store.all().at(-1));
          drained.push({ receiptId: b.receiptId, clientSeq: b.clientSeq, dropped: true, reason: e.code });
        }
        progressed = true;
      }
    }
    return drained;
  }

  // ---------- 冻结 / 关闭 ----------

  freezeDue(now) {
    const frozen = [];
    for (const ev of [...this.s.events.values()].sort((a, b) => a.startMs - b.startMs)) {
      if (ev.status !== 'SCHEDULED' || ev.startMs > now) continue;
      this.freezeBaseline(ev, now);
      frozen.push(ev.id);
    }
    return frozen;
  }

  freezeBaseline(ev, now) {
    const res = computeBaseline({
      algorithm: ev.algorithm,
      eventTs: ev.startMs,
      readingsByDay: this.#readingsByDay(ev.meterId),
      eventDays: this.s.eventDays.get(ev.meterId),
      eventDayKey: dayKey(ev.startMs),
    });
    const windowKw = res.perInterval.slice(ev.windowStartIdx, ev.windowStartIdx + ev.windowCount);
    const payload = {
      eventId: ev.id,
      version: 1,
      frozenAt: now,
      algorithm: res.algorithm,
      days: res.days,
      perInterval: res.perInterval,
      windowKw,
    };
    this.store.append('BASELINE_FROZEN', payload, now);
    this.apply(this.store.all().at(-1));
    this.store.append('EVENT_ACTIVATED', { eventId: ev.id, at: now }, now);
    this.apply(this.store.all().at(-1));
    return payload;
  }

  closeDue(now) {
    const closed = [];
    for (const ev of [...this.s.events.values()].sort((a, b) => a.endMs - b.endMs)) {
      if (ev.status !== 'ACTIVE' || ev.endMs > now) continue;
      this.closeEvent(ev, now);
      closed.push(ev.id);
    }
    return closed;
  }

  closeEvent(ev, now) {
    this.store.append('EVENT_CLOSED', { eventId: ev.id, at: now }, now);
    this.apply(this.store.all().at(-1));
    for (const [deviceId, claim] of [...this.s.claims.entries()]) {
      if (claim.eventId !== ev.id) continue;
      this.store.append('DEVICE_RELEASED', { eventId: ev.id, deviceId, reason: 'EVENT_CLOSED', at: now }, now);
      this.apply(this.store.all().at(-1));
      this.s.buffers.delete(deviceId); // 终态后水位固定，未补齐的乱序缓冲不再保留
    }
    this.issueReport(ev, { kind: 'ORIGINAL', reason: 'WINDOW_ENDED', cutoffTs: now });
  }

  // ---------- 读数 / 订正 ----------

  ingestReadings(items, { source = 'API', now = this.now() } = {}) {
    const affectedMeters = new Set();
    const windowTouched = new Set(); // eventId
    const batches = new Map();

    for (const it of items) {
      const meter = this.s.meters.get(it.meterId);
      if (!meter) throw new DomainError('NOT_FOUND', `计量点 ${it.meterId} 不存在`);
      const ts = floorInterval(Number(it.ts));
      const value = round3(Number(it.value));
      if (!Number.isFinite(value) || value < 0) throw new DomainError('BAD_REQUEST', '读数非法');
      const dk = dayKey(ts);
      const idx = intervalIndex(ts);
      const existing = this.s.days.get(it.meterId).get(dk)?.[idx];
      if (typeof existing === 'number') {
        if (Math.abs(existing - value) < 1e-6) continue;
        throw new DomainError('CONFLICTING_READING', '该区间已有不可变读数；修正只能形成订正版', {
          meterId: it.meterId, ts, existing, attempted: value,
        });
      }
      if (!batches.has(it.meterId)) batches.set(it.meterId, []);
      batches.get(it.meterId).push({ ts, value });
      affectedMeters.add(it.meterId);
    }

    for (const [meterId, list] of batches) {
      list.sort((a, b) => a.ts - b.ts);
      const payload = { meterId, items: list, arrivedAt: now, source, sheds: [] };
      this.store.append('READINGS_BATCH', payload, now);
      this.apply(this.store.all().at(-1));
      for (const it of list) {
        for (const ev of this.s.events.values()) {
          if (ev.meterId === meterId && it.ts >= ev.startMs && it.ts < ev.endMs && ev.activatedAt) {
            windowTouched.add(ev.id);
          }
        }
      }
    }

    return this.#afterReadings([...affectedMeters], windowTouched, source, now);
  }

  // 读数到达后：冻结过的基线可能需要订正；已结束的事件可能需要订正报告
  #afterReadings(affectedMeters, windowTouched, source, now) {
    const baselineCorrected = [];
    const reportsIssued = [];

    for (const ev of this.s.events.values()) {
      if (!affectedMeters.includes(ev.meterId)) continue;
      if (!this.s.baselines.get(ev.id)?.length) continue; // 未冻结：开始时自然纳入
      const res = computeBaseline({
        algorithm: ev.algorithm,
        eventTs: ev.startMs,
        readingsByDay: this.#readingsByDay(ev.meterId),
        eventDays: this.s.eventDays.get(ev.meterId),
        eventDayKey: dayKey(ev.startMs),
      });
      const windowKw = res.perInterval.slice(ev.windowStartIdx, ev.windowStartIdx + ev.windowCount);
      const latest = this.s.baselines.get(ev.id).at(-1);
      const changed = windowKw.some((v, i) => v === null || latest.windowKw[i] === null || Math.abs(v - latest.windowKw[i]) > 0.01);
      if (changed) {
        const version = latest.version + 1;
        const payload = {
          eventId: ev.id, version, reason: 'LATE_HISTORICAL', at: now,
          algorithm: res.algorithm, days: res.days, perInterval: res.perInterval, windowKw,
        };
        this.store.append('BASELINE_CORRECTED', payload, now);
        this.apply(this.store.all().at(-1));
        baselineCorrected.push({ eventId: ev.id, version });
      }
    }

    // 已裁决（已有报告）的事件：用最新基线 + 当前水位复算，有差异才出新订正版
    for (const ev of this.s.events.values()) {
      if (ev.status === 'RECALLED') continue;
      const reports = this.s.reports.get(ev.id);
      if (!reports?.length) continue;
      if (!affectedMeters.includes(ev.meterId) && !windowTouched.has(ev.id)) continue;
      const candidate = this.buildReportObject(ev, {
        version: reports.at(-1).version + 1,
        kind: 'CORRECTED',
        reason: baselineCorrected.some((b) => b.eventId === ev.id) ? 'LATE_HISTORICAL' : 'LATE_ACTUAL',
        cutoffTs: now,
      });
      if (candidate.hash !== reports.at(-1).hash) {
        this.store.append('REPORT_ISSUED', { report: candidate }, now);
        this.apply(this.store.all().at(-1));
        reportsIssued.push(candidate.reportId);
      }
    }

    return { ingested: true, baselineCorrected, reportsIssued };
  }

  loadMissingDay(cmd, now) {
    this.requireMeter(cmd.meterId);
    const meterIdx = this.s.meterIndex.get(cmd.meterId);
    const offset = Number(cmd.dayOffset);
    if (!Number.isInteger(offset) || offset <= 0) throw new DomainError('BAD_REQUEST', 'dayOffset 必须为正整数');
    const key = new Date(BASE_NOW - offset * 86400000).toISOString().slice(0, 10);
    if (this.s.days.get(cmd.meterId).has(key)) {
      throw new DomainError('DAY_PRESENT', `计量点 ${cmd.meterId} 的 ${key} 数据已存在（不可重复写入）`);
    }
    const { values } = dayValuesForKey(meterIdx, key);
    this.store.append('DAY_READINGS_LOADED', { meterId: cmd.meterId, day: key, values, dayOffset: offset }, now);
    this.apply(this.store.all().at(-1));
    return this.#afterReadings([cmd.meterId], new Set(), 'LATE_HISTORICAL', now);
  }

  // 事件结束后补传窗口内缺失的实际读数（通信恢复后的电表补传）
  backfillActuals(cmd, now) {
    const ev = this.requireEvent(cmd.eventId);
    const meterIdx = this.s.meterIndex.get(ev.meterId);
    const items = [];
    for (const ts of intervalStarts(ev.startMs, ev.endMs)) {
      const dk = dayKey(ts);
      const idx = intervalIndex(ts);
      if (typeof this.s.days.get(ev.meterId).get(dk)?.[idx] === 'number') continue;
      // 补传按“无削减”的真实负荷画像（设备未响应或表计中断期间保守取值）
      const value = simIntervalActual(meterIdx, ts, 0);
      items.push({ meterId: ev.meterId, ts, value });
    }
    if (!items.length) return { backfilled: 0, reportsIssued: [] };
    return this.ingestReadings(items, { source: 'LATE_BACKFILL', now });
  }

  setMeterOffline(cmd, now) {
    this.requireMeter(cmd.meterId);
    const offline = !!cmd.offline;
    this.store.append('SIM_METER_OFFLINE', { meterId: cmd.meterId, offline }, now);
    this.apply(this.store.all().at(-1));
    return { meterId: cmd.meterId, offline };
  }

  // ---------- 履约异议处置 ----------
  //
  // 标的是一份【已封存裁定】（REPORT_ISSUED 落链后即为封存），异议只锚定其
  // 编号（eventId + reportVersion / reportId）与内容指纹（report.hash）。
  // 迟到计量数据产生新裁定时是另一个标的：旧异议不转移、新标的可单独受理；
  // 异议受理不允许回写被质疑的报告（v1 字节级保持不变）。

  #resolveTargetReport(eventId, reportVersion) {
    this.requireEvent(eventId);
    const reports = this.s.reports.get(eventId);
    if (!reports?.length) throw new DomainError('REPORT_NOT_SEALED', '该事件尚无已封存裁定，不能提起异议');
    const v = Number(reportVersion) || reports.at(-1).version;
    const report = reports.find((r) => r.version === v);
    if (!report) throw new DomainError('NOT_FOUND', `裁定 v${v} 不存在`, { eventId, reportVersion: v });
    return report;
  }

  #objectionTargetKey(eventId, reportVersion) {
    return `${eventId}:v${reportVersion}`;
  }

  // 客户请求号幂等：同号且同正文 -> 返回原结果（idempotent:true）；同号不同正文 -> 409。
  #idempotentRequest(requestId, body) {
    if (!requestId) return null;
    const seen = this.s.objectionRequests.get(requestId);
    if (!seen) return null;
    const bodyHash = sha256Hex(canonicalJSON(body));
    if (seen.bodyHash !== bodyHash) {
      throw new DomainError('IDEMPOTENCY_CONFLICT',
        '客户请求号重复但请求正文不同（409）：请使用新的请求号，或原样重发', {
          requestId, originalKind: seen.kind, originalObjectionId: seen.objectionId,
        });
    }
    return seen;
  }

  fileObjection(cmd, now) {
    const requestId = cmd.requestId ? String(cmd.requestId) : null;
    if (!requestId) throw new DomainError('BAD_REQUEST', '缺少客户请求号 requestId（重复请求去重所必需）');
    const reason = typeof cmd.reason === 'string' ? cmd.reason.trim() : '';
    if (!reason) throw new DomainError('BAD_REQUEST', '异议理由不能为空');
    const aggregatorId = String(cmd.aggregatorId || '');
    if (!this.s.aggregators.has(aggregatorId)) {
      throw new DomainError('NOT_FOUND', `代理商 ${aggregatorId} 不存在`, { aggregatorId });
    }
    // 幂等必须先于存在性/业务校验：同号重放即使原标的已不可提，也要原样返回原结果。
    const body = {
      eventId: String(cmd.eventId || ''),
      reportVersion: Number(cmd.reportVersion) || null,
      reportHash: cmd.reportHash ? String(cmd.reportHash) : null,
      aggregatorId,
      reason,
    };
    const seen = this.#idempotentRequest(requestId, body);
    if (seen) {
      return { idempotent: true, requestId, objection: this.objectionView(seen.objectionId) };
    }

    const report = this.#resolveTargetReport(cmd.eventId, cmd.reportVersion);
    const submittedHash = cmd.reportHash === undefined ? report.hash : String(cmd.reportHash);
    if (cmd.reportHash !== undefined && !HASH64_RE.test(submittedHash)) {
      throw new DomainError('BAD_REQUEST', '内容指纹必须是 64 位十六进制 sha256');
    }
    if (submittedHash !== report.hash) {
      throw new DomainError('FINGERPRINT_MISMATCH',
        `提交的内容指纹与封存裁定 ${report.reportId} 不一致`, {
          eventId: report.eventId, reportVersion: report.version,
          expectedHash: report.hash, submittedHash,
        });
    }
    // 时限：必须在截止时刻前（含截止时刻）。
    const deadlineMs = report.issuedAt + OBJECTION_WINDOW_MS;
    if (now > deadlineMs) {
      throw new DomainError('OBJECTION_DEADLINE_PASSED', '异议受理时限已过', {
        target: this.#objectionTargetKey(report.eventId, report.version),
        deadlineMs, now,
      });
    }
    // 同一代理商对同一标的只允许一个待处理实例。
    for (const obj of this.s.objections.values()) {
      const sameTarget = obj.eventId === report.eventId && obj.reportVersion === report.version;
      if (sameTarget && obj.aggregatorId === aggregatorId && obj.status === 'PENDING') {
        throw new DomainError('DUPLICATE_PENDING_OBJECTION',
          '该代理商对此标的已有待处理异议；审核出结论前不得重复提起', {
            existingObjectionId: obj.id, target: this.#objectionTargetKey(report.eventId, report.version),
          });
      }
    }

    const id = cmd.objectionId || uid('OBJ');
    const payload = {
      id,
      requestId,
      eventId: report.eventId,
      reportVersion: report.version,
      reportId: report.reportId,
      reportHash: report.hash,
      verdict: report.verdict,
      aggregatorId,
      reason,
      bodyHash: sha256Hex(canonicalJSON(body)),
      at: now,
      deadlineMs,
    };
    this.store.append('OBJECTION_FILED', payload, now);
    this.apply(this.store.all().at(-1));
    return { filed: true, idempotent: false, requestId, objection: this.objectionView(id) };
  }

  appendObjectionMaterial(cmd, now) {
    const requestId = cmd.requestId ? String(cmd.requestId) : null;
    if (!requestId) throw new DomainError('BAD_REQUEST', '缺少客户请求号 requestId（重复请求去重所必需）');
    const obj = this.requireObjection(cmd.objectionId);
    if (obj.aggregatorId !== cmd.aggregatorId) {
      throw new DomainError('NOT_OBJECTION_OWNER', '只有提起该异议的代理商可以补充材料', {
        objectionId: obj.id, ownerAggregatorId: obj.aggregatorId,
      });
    }
    const summary = typeof cmd.summary === 'string' ? cmd.summary.trim() : '';
    const content = typeof cmd.content === 'string' ? cmd.content.trim() : '';
    if (!summary && !content) throw new DomainError('BAD_REQUEST', '补充材料必须包含摘要或正文');

    const body = {
      objectionId: obj.id,
      aggregatorId: String(cmd.aggregatorId || ''),
      summary,
      content,
    };
    const seen = this.#idempotentRequest(requestId, body);
    if (seen) {
      return { idempotent: true, requestId, objection: this.objectionView(obj.id), materialSeq: seen.materialSeq };
    }
    // 补充材料只能在待处理期间顺序追加：审核一经通过/驳回即封卷。
    if (obj.status !== 'PENDING') {
      throw new DomainError('OBJECTION_ALREADY_DECIDED',
        `异议已${obj.status === 'APPROVED' ? '通过' : '驳回'}，不能再补充材料`, { status: obj.status });
    }

    const seq = obj.materials.length + 1;
    const payload = {
      objectionId: obj.id,
      eventId: obj.eventId,
      reportVersion: obj.reportVersion,
      requestId,
      seq,
      summary: summary.slice(0, 200),
      content: content.slice(0, 20000),
      bodyHash: sha256Hex(canonicalJSON(body)),
      at: now,
    };
    this.store.append('OBJECTION_MATERIAL_APPENDED', payload, now);
    this.apply(this.store.all().at(-1));
    return { appended: true, idempotent: false, requestId, materialSeq: seq, objection: this.objectionView(obj.id) };
  }

  reviewObjection(cmd, now) {
    const obj = this.requireObjection(cmd.objectionId);
    const decision = String(cmd.decision || '').toUpperCase();
    if (!REVIEW_DECISIONS.includes(decision)) {
      throw new DomainError('BAD_REQUEST', `审核结论必须是 ${REVIEW_DECISIONS.join('/')}`);
    }
    // 审核一经通过或驳回便不可再次裁定（终态机，禁止任何二次裁决/改写）。
    if (obj.status !== 'PENDING') {
      throw new DomainError('OBJECTION_ALREADY_DECIDED',
        `异议已有结论 ${obj.status}，不可再次裁定`, { status: obj.status, existingReview: obj.review });
    }
    const rationale = typeof cmd.rationale === 'string' ? cmd.rationale.trim() : '';
    if (!rationale) throw new DomainError('BAD_REQUEST', '审核意见不能为空');

    const payload = {
      objectionId: obj.id,
      eventId: obj.eventId,
      reportVersion: obj.reportVersion,
      target: this.#objectionTargetKey(obj.eventId, obj.reportVersion),
      decision,
      rationale: rationale.slice(0, 2000),
      reviewerId: cmd.reviewerId ? String(cmd.reviewerId).slice(0, 64) : 'OPS',
      at: now,
    };
    this.store.append('OBJECTION_REVIEWED', payload, now);
    this.apply(this.store.all().at(-1));
    // 结论只作用于异议卷宗：被质疑的原始裁定内容绝不修改。
    return { decided: true, objection: this.objectionView(obj.id) };
  }

  requireObjection(id) {
    const obj = this.s.objections.get(id);
    if (!obj) throw new DomainError('NOT_FOUND', `异议 ${id} 不存在`, { objectionId: id });
    return obj;
  }

  // ---------- 仿真推进 ----------

  tick(minutes, prevNow) {
    const from = this.now();
    const to = from + Math.round(minutes) * 60000;
    this.store.append('SIM_TICK', { from, to, minutes }, to);
    this.apply(this.store.all().at(-1));

    const frozen = this.freezeDue(to);
    const generated = this.#generateSimReadings(to);
    const closed = this.closeDue(to);

    return { now: to, iso: iso(to), frozen, closed, generatedIntervals: generated };
  }

  #generateSimReadings(now) {
    const s = this.s;
    let count = 0;
    while (s.genCursor + INTERVAL_MS <= now) {
      const ts = s.genCursor;
      const arrivalTs = ts + INTERVAL_MS; // 区间结束后才抄回
      // 找出此刻正在执行的设备（按计量点汇总削减）
      const shedByMeter = new Map();
      for (const [deviceId, claim] of s.claims.entries()) {
        if (claim.status !== 'EXECUTING') continue;
        const ev = s.events.get(claim.eventId);
        if (!ev || ev.status !== 'ACTIVE') continue;
        if (ts < ev.startMs || ts >= ev.endMs) continue;
        const dev = s.devices.get(deviceId);
        const noise = 0.97 + 0.06 * (mulberry32(strSeed(`eff-${deviceId}-${ts}`))());
        const shed = round3(Math.min(claim.requestedKw, dev.maxKw) * dev.eff * noise);
        if (!shedByMeter.has(ev.meterId)) shedByMeter.set(ev.meterId, { sheds: [], total: 0 });
        shedByMeter.get(ev.meterId).sheds.push({ deviceId, ts, kw: shed });
        shedByMeter.get(ev.meterId).total += shed;
      }
      for (const [meterId, meter] of s.meters.entries()) {
        if (s.offline.has(meterId)) continue; // 通信中断：无读数 -> 事件结束时形成 DATA_GAP
        const meterIdx = s.meterIndex.get(meterId);
        const totalShed = shedByMeter.get(meterId)?.total || 0;
        const value = simIntervalActual(meterIdx, ts, totalShed);
        const payload = {
          meterId,
          items: [{ ts, value }],
          arrivedAt: arrivalTs,
          source: 'SIM',
          sheds: shedByMeter.get(meterId)?.sheds || [],
        };
        this.store.append('READINGS_BATCH', payload, now);
        this.apply(this.store.all().at(-1));
      }
      s.genCursor = ts + INTERVAL_MS;
      count++;
    }
    return count;
  }

  // ---------- 报告 ----------

  buildReportObject(ev, { version, kind, reason, cutoffTs }) {
    const intervals = intervalStarts(ev.startMs, ev.endMs);
    const baselineVersion = kind === 'ORIGINAL' ? 1 : this.s.baselines.get(ev.id).at(-1).version;
    const baseline = this.s.baselines.get(ev.id).find((b) => b.version === baselineVersion);
    const arrivals = new Map();
    for (const a of this.s.arrivals) {
      if (a.meterId !== ev.meterId) continue;
      if (a.ts < ev.startMs || a.ts >= ev.endMs) continue;
      if (a.arrivedAt > cutoffTs) continue;
      arrivals.set(a.ts, a.value); // 同一区间只读不可变，首次到达即定值
    }
    const parts = [...(this.s.participants.get(ev.id)?.values() || [])];
    const devices = parts.map((part) => ({
      id: part.deviceId,
      status: part.status,
      requestedKw: part.requestedKw || 0,
      lastAckPhase: part.lastAckPhase,
      shedByTs: Object.fromEntries(this.s.sheds.get(part.deviceId)),
    }));
    return buildReport(ev, {
      version,
      kind,
      reason,
      issuedAt: this.now(),
      intervalTs: intervals,
      baselineKw: baseline.windowKw,
      meterActualKw: Object.fromEntries(arrivals),
      devices,
      baselineVersion,
      cutoffTs,
      eventStatus: ev.status,
    });
  }

  issueReport(ev, { kind, reason, cutoffTs }) {
    const reports = this.s.reports.get(ev.id);
    const version = kind === 'ORIGINAL' ? 1 : reports.at(-1).version + 1;
    const report = this.buildReportObject(ev, { version, kind, reason, cutoffTs });
    this.store.append('REPORT_ISSUED', { report }, this.now());
    this.apply(this.store.all().at(-1));
    return report;
  }

  // ---------- 异议视图 ----------

  objectionView(id) {
    const obj = this.s.objections.get(id);
    if (!obj) return null;
    const now = this.now();
    const report = this.s.reports.get(obj.eventId)?.find((r) => r.version === obj.reportVersion) || null;
    return {
      id: obj.id,
      eventId: obj.eventId,
      reportVersion: obj.reportVersion,
      reportId: obj.reportId,
      target: this.#objectionTargetKey(obj.eventId, obj.reportVersion),
      reportHash: obj.reportHash,
      reportVerdict: obj.verdict || report?.verdict || null,
      // 指纹仍与当前链上同版裁定一致：异议不修改原始内容；新版本是另一个标的。
      targetIntact: report ? report.hash === obj.reportHash : false,
      aggregatorId: obj.aggregatorId,
      reason: obj.reason,
      status: obj.status,
      filedAt: obj.filedAt,
      filedIso: iso(obj.filedAt),
      deadlineMs: obj.deadlineMs,
      deadlineIso: iso(obj.deadlineMs),
      overdue: now > obj.deadlineMs,
      materials: obj.materials.map((m) => ({ ...m, attachedIso: iso(m.attachedAt) })),
      materialCount: obj.materials.length,
      review: obj.review ? { ...obj.review, reviewedIso: iso(obj.review.reviewedAt) } : null,
    };
  }

  objectionListView() {
    return this.s.objectionIds.map((id) => this.objectionView(id));
  }

  // ---------- 视图 ----------

  eventView(id) {
    const ev = this.s.events.get(id);
    if (!ev) return null;
    const baselines = this.s.baselines.get(id) || [];
    const reports = this.s.reports.get(id) || [];
    const claims = [];
    for (const [deviceId, claim] of this.s.claims.entries()) {
      if (claim.eventId === id) claims.push({ deviceId, ...claim });
    }
    const parts = [...(this.s.participants.get(id)?.values() || [])];
    const buffered = [];
    for (const [deviceId, buf] of this.s.buffers.entries()) {
      for (const b of buf) if (b.eventId === id) buffered.push({ deviceId, ...b });
    }
    return {
      ...ev,
      startIso: iso(ev.startMs),
      endIso: iso(ev.endMs),
      baselineVersions: baselines.map((b) => ({
        version: b.version, frozenAt: b.frozenAt || b.at, reason: b.reason || 'FREEZE', days: b.days,
      })),
      participants: parts,
      claims,
      bufferedReceipts: buffered,
      reports: reports.map((r) => ({
        reportId: r.reportId, version: r.version, kind: r.kind, verdict: r.verdict,
        reductionKwh: r.reductionKwh, targetKwh: r.targetKwh, deliveryRatio: r.deliveryRatio,
        gapCount: r.gapCount, baselineVersion: r.baselineVersion, issuedAt: r.issuedAt, hash: r.hash,
        objectionDeadlineMs: r.issuedAt + OBJECTION_WINDOW_MS,
      })),
      objections: [...(this.s.objectionsByEvent.get(id) || [])].map((oid) => {
        const o = this.objectionView(oid);
        return {
          id: o.id, reportVersion: o.reportVersion, target: o.target, aggregatorId: o.aggregatorId,
          status: o.status, filedAt: o.filedAt, deadlineMs: o.deadlineMs, overdue: o.overdue,
          materialCount: o.materialCount, targetIntact: o.targetIntact,
          review: o.review ? { decision: o.review.decision, reviewedAt: o.review.reviewedAt } : null,
        };
      }),
    };
  }

  stateView() {
    const chain = this.store.verify();
    return {
      now: this.now(),
      nowIso: iso(this.now()),
      mode: chain.mode,
      readOnly: !chain.writable,
      recovery: chain.recovery,
      aggregators: [...this.s.aggregators.values()],
      meters: [...this.s.meters.values()].map((m) => ({ ...m, offline: this.s.offline.has(m.id) })),
      devices: [...this.s.devices.values()].map((d) => {
        const claim = this.s.claims.get(d.id);
        return {
          ...d,
          claim: claim ? {
            eventId: claim.eventId,
            aggregatorId: claim.aggregatorId,
            status: claim.status,
            requestedKw: claim.requestedKw,
            watermark: claim.appliedSeq,
            lastAckAt: claim.lastAckAt,
          } : null,
          buffered: (this.s.buffers.get(d.id) || []).length,
        };
      }),
      events: [...this.s.events.values()].map((e) => this.eventView(e.id)),
      objections: this.objectionListView(),
      objectionWindowMs: OBJECTION_WINDOW_MS,
      chain: this.store.verify(),
    };
  }
}
