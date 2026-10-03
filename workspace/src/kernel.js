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
  canonicalJSON,
  dayKey,
  floorInterval,
  intervalIndex,
  intervalStarts,
  iso,
  round3,
  sha256Hex,
  uid,
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

// 履约异议：裁定（报告）封存后 48 个仿真小时内可提异议；时限随 DISPUTE_FILED 记录落盘，
// 重启重放后不变。审核结论 UPHELD（通过）/ REJECTED（驳回）为终态。
export const DISPUTE_WINDOW_MS = 48 * 3600 * 1000;
export const DISPUTE_OUTCOMES = ['UPHELD', 'REJECTED'];

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
      sheds: new Map(), // deviceId -> Map(ts -> kw)
      offline: new Set(),
      disputes: new Map(), // disputeId -> 异议工单投影（待处理状态/时限/材料次序全部由记录重建）
      requests: new Map(), // requestId -> {op, bodyHash, disputeId, materialSeq?}（客户请求号幂等台账）
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
      case 'DISPUTE_FILED':
        s.disputes.set(p.disputeId, {
          disputeId: p.disputeId,
          aggregatorId: p.aggregatorId,
          eventId: p.eventId,
          reportId: p.reportId,
          reportVersion: p.reportVersion,
          reportHash: p.reportHash,
          reason: p.reason,
          status: 'PENDING',
          filedAt: p.filedAt,
          deadline: p.deadline,
          materials: [],
          resolution: null,
          history: [{ type: 'FILED', at: p.filedAt, recordSeq: rec.seq }],
        });
        s.requests.set(p.requestId, { op: rec.type, bodyHash: p.bodyHash, disputeId: p.disputeId });
        break;
      case 'DISPUTE_SUPPLEMENTED': {
        const d = s.disputes.get(p.disputeId);
        if (d) {
          // 材料只增且序号单调：重放顺序即材料次序，重启后不变
          d.materials.push({ seq: p.materialSeq, kind: p.material.kind, note: p.material.note, ref: p.material.ref ?? null, at: p.at });
          d.history.push({ type: 'SUPPLEMENTED', at: p.at, materialSeq: p.materialSeq, recordSeq: rec.seq });
        }
        s.requests.set(p.requestId, { op: rec.type, bodyHash: p.bodyHash, disputeId: p.disputeId, materialSeq: p.materialSeq });
        break;
      }
      case 'DISPUTE_RESOLVED': {
        const d = s.disputes.get(p.disputeId);
        if (d) {
          d.status = p.outcome; // UPHELD / REJECTED，终态
          d.resolution = { outcome: p.outcome, comment: p.comment ?? null, resolvedBy: p.resolvedBy, resolvedAt: p.resolvedAt };
          d.history.push({ type: 'RESOLVED', at: p.resolvedAt, outcome: p.outcome, recordSeq: rec.seq });
        }
        s.requests.set(p.requestId, { op: rec.type, bodyHash: p.bodyHash, disputeId: p.disputeId });
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
      case 'FILE_DISPUTE':
        return this.fileDispute(cmd, now);
      case 'SUPPLEMENT_DISPUTE':
        return this.supplementDispute(cmd, now);
      case 'RESOLVE_DISPUTE':
        return this.resolveDispute(cmd, now);
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

  // 客户请求号幂等闸：同号同文 -> 返回已受理结果（不重复追加记录）；
  // 同号不同文 -> 409。台账由链上记录（requestId+bodyHash）重放重建，重启后语义不变。
  #requestGate(cmd) {
    const requestId = cmd.requestId;
    if (typeof requestId !== 'string' || !requestId.trim()) {
      throw new DomainError('BAD_REQUEST', '客户请求号 requestId 必填（作为幂等键）');
    }
    const bodyHash = sha256Hex(canonicalJSON(cmd));
    const seen = this.s.requests.get(requestId);
    if (seen && seen.bodyHash !== bodyHash) {
      throw new DomainError('REQUEST_ID_CONFLICT', '客户请求号已被不同正文使用（幂等冲突）', { requestId });
    }
    return { bodyHash, seen: seen || null };
  }

  #requireDispute(disputeId) {
    const d = this.s.disputes.get(disputeId);
    if (!d) throw new DomainError('DISPUTE_NOT_FOUND', `异议 ${disputeId} 不存在`, { disputeId });
    return d;
  }

  // 提起异议：代理商在截止时刻前，以“已封存裁定的编号 + 内容指纹”为标的提交。
  fileDispute(cmd, now) {
    const gate = this.#requestGate(cmd);
    if (gate.seen) {
      return { dispute: this.disputeView(gate.seen.disputeId), idempotent: true };
    }
    const aggregatorId = String(cmd.aggregatorId || '');
    if (!this.s.aggregators.has(aggregatorId)) {
      throw new DomainError('NOT_FOUND', `聚合商 ${aggregatorId} 不存在`);
    }
    // 标的必须是一份已封存裁定（已签发的报告版本）
    const report = [...this.s.reports.values()].flat().find((r) => r.reportId === cmd.reportId);
    if (!report) {
      throw new DomainError('REPORT_NOT_FOUND', `裁定 ${cmd.reportId} 不存在（尚未封存）`, { reportId: cmd.reportId });
    }
    if (cmd.reportHash !== report.hash) {
      throw new DomainError('REPORT_HASH_MISMATCH', '标的指纹与封存裁定的内容指纹不符', {
        reportId: report.reportId,
        expected: report.hash,
        provided: cmd.reportHash ?? null,
      });
    }
    const deadline = report.issuedAt + DISPUTE_WINDOW_MS;
    if (now > deadline) {
      throw new DomainError('DISPUTE_DEADLINE_PASSED', '已超过该裁定的异议截止时刻', {
        reportId: report.reportId, deadline, deadlineIso: iso(deadline),
      });
    }
    // 同一代理商对同一标的只允许一个待处理实例
    for (const d of this.s.disputes.values()) {
      if (d.status === 'PENDING' && d.aggregatorId === aggregatorId && d.reportId === report.reportId) {
        throw new DomainError('DISPUTE_PENDING_EXISTS', '同一代理商对同一标的只允许一个待处理异议', {
          pendingDisputeId: d.disputeId,
        });
      }
    }
    const reason = String(cmd.reason || '').trim();
    if (!reason) throw new DomainError('BAD_REQUEST', '异议事由 reason 必填');

    const disputeId = uid('DSP');
    const payload = {
      disputeId,
      requestId: cmd.requestId,
      bodyHash: gate.bodyHash,
      aggregatorId,
      eventId: report.eventId,
      reportId: report.reportId,
      reportVersion: report.version,
      reportHash: report.hash,
      reason,
      filedAt: now,
      deadline,
    };
    this.store.append('DISPUTE_FILED', payload, now);
    this.apply(this.store.all().at(-1));
    return { dispute: this.disputeView(disputeId), idempotent: false };
  }

  // 补充材料：只能向待处理异议顺序追加（materialSeq 单调递增，不允许插序/改写）。
  supplementDispute(cmd, now) {
    const gate = this.#requestGate(cmd);
    if (gate.seen) {
      const view = this.disputeView(gate.seen.disputeId);
      return {
        dispute: view,
        material: view.materials.find((m) => m.seq === gate.seen.materialSeq) ?? null,
        idempotent: true,
      };
    }
    const d = this.#requireDispute(cmd.disputeId);
    if (d.status !== 'PENDING') {
      throw new DomainError('DISPUTE_TERMINAL', '异议已有审核结论（终态），不可再补充材料', { status: d.status });
    }
    if (String(cmd.aggregatorId || '') !== d.aggregatorId) {
      throw new DomainError('DISPUTE_OWNER_MISMATCH', '只有提起该异议的代理商可以补充材料', { owner: d.aggregatorId });
    }
    const expected = d.materials.length + 1;
    if (cmd.materialSeq != null && Number(cmd.materialSeq) !== expected) {
      throw new DomainError('MATERIAL_SEQ_CONFLICT', `补充材料只能顺序追加：期望序号 ${expected}`, {
        expected, provided: Number(cmd.materialSeq),
      });
    }
    const note = String(cmd.note || '').trim();
    if (!note) throw new DomainError('BAD_REQUEST', '材料说明 note 必填');

    const payload = {
      disputeId: d.disputeId,
      requestId: cmd.requestId,
      bodyHash: gate.bodyHash,
      aggregatorId: d.aggregatorId,
      eventId: d.eventId,
      reportId: d.reportId,
      materialSeq: expected,
      material: { kind: String(cmd.kind || 'EVIDENCE'), note, ref: cmd.ref ? String(cmd.ref) : null },
      at: now,
    };
    this.store.append('DISPUTE_SUPPLEMENTED', payload, now);
    this.apply(this.store.all().at(-1));
    return { dispute: this.disputeView(d.disputeId), material: this.s.disputes.get(d.disputeId).materials.at(-1), idempotent: false };
  }

  // 审核：一经通过（UPHELD）或驳回（REJECTED）即为终态，不可再次裁定；
  // 审核只形成结论记录，绝不修改被质疑的原始裁定内容。
  resolveDispute(cmd, now) {
    const gate = this.#requestGate(cmd);
    if (gate.seen) {
      return { dispute: this.disputeView(gate.seen.disputeId), idempotent: true };
    }
    const d = this.#requireDispute(cmd.disputeId);
    if (d.status !== 'PENDING') {
      throw new DomainError('DISPUTE_TERMINAL', '审核一经通过或驳回便不可再次裁定', {
        status: d.status, resolution: d.resolution,
      });
    }
    const outcome = String(cmd.outcome || '');
    if (!DISPUTE_OUTCOMES.includes(outcome)) {
      throw new DomainError('BAD_REQUEST', `outcome 必须是 ${DISPUTE_OUTCOMES.join('（通过）/ ')}（驳回）`);
    }
    const payload = {
      disputeId: d.disputeId,
      requestId: cmd.requestId,
      bodyHash: gate.bodyHash,
      aggregatorId: d.aggregatorId,
      eventId: d.eventId,
      reportId: d.reportId,
      outcome,
      comment: cmd.comment ? String(cmd.comment) : null,
      resolvedBy: cmd.resolvedBy ? String(cmd.resolvedBy) : '运营方',
      resolvedAt: now,
    };
    this.store.append('DISPUTE_RESOLVED', payload, now);
    this.apply(this.store.all().at(-1));
    return { dispute: this.disputeView(d.disputeId), idempotent: false };
  }

  disputeView(id) {
    const d = this.s.disputes.get(id);
    if (!d) return null;
    return {
      disputeId: d.disputeId,
      aggregatorId: d.aggregatorId,
      eventId: d.eventId,
      reportId: d.reportId,
      reportVersion: d.reportVersion,
      reportHash: d.reportHash,
      reason: d.reason,
      status: d.status,
      filedAt: d.filedAt,
      filedAtIso: iso(d.filedAt),
      deadline: d.deadline,
      deadlineIso: iso(d.deadline),
      materials: d.materials.map((m) => ({ ...m })),
      resolution: d.resolution ? { ...d.resolution } : null,
      history: d.history.map((h) => ({ ...h })),
    };
  }

  listDisputes() {
    return [...this.s.disputes.values()].map((d) => this.disputeView(d.disputeId));
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
      })),
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
      disputes: this.listDisputes(),
      chain: this.store.verify(),
    };
  }
}
