// 投递调度器：
//  - 每个订阅方一条严格串行的投递循环——同一订阅方永远“前一封租约结束/确认”后才发下一封，
//    因此同一订阅方严格按 envelopeSeq（事件顺序）投递；
//  - 所有订阅方的投递循环并发执行——一个订阅方慢/失败/死信，绝不阻塞其他订阅方；
//  - 租约（LEASE_MS）内的项不会被重复投递；超时后用【同一个稳定幂等键】再次请求，
//    订阅方可安全去重；
//  - 失败指数退避（基于上一次失败时间）；连续失败达 maxAttempts 转死信，阻塞本队列后续，
//    等待 DLQ_REQUEUED 人工重新入队。
import { canonicalEnvelope } from './engine.js';
import { signPayload } from './crypto.js';

export class Dispatcher {
  // engineRef 可以是引擎本身，也可以是 { engine } 持有者（重启重放后引用自动跟随）。
  constructor(engineRef, transport, {
    pollMs = 100,
    leaseMs = 15_000,
    baseBackoffMs = 300,
    maxBackoffMs = 5_000,
    logger = console,
  } = {}) {
    this.engineRef = engineRef;
    this.transport = transport;
    this.pollMs = pollMs;
    this.leaseMs = leaseMs;
    this.baseBackoffMs = baseBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
    this.log = logger;
    this.running = false;
    this._timer = null;
    this._busy = new Set(); // 正在投递的订阅方（串行保证）
  }

  get engine() {
    return this.engineRef?.engine ? this.engineRef.engine : this.engineRef;
  }

  // 重启重放后替换为新引擎（持有者场景写入 ctx，裸引擎场景直接替换引用）。
  replaceEngine(engine) {
    if (this.engineRef && typeof this.engineRef === 'object' && 'engine' in this.engineRef) {
      this.engineRef.engine = engine;
    } else {
      this.engineRef = engine;
    }
  }

  start() {
    if (this.running) return;
    this.running = true;
    const tick = () => {
      if (!this.running) return;
      this.tick().catch((e) => this.log?.error?.('dispatcher tick error:', e));
      this._timer = setTimeout(tick, this.pollMs);
    };
    this._timer = setTimeout(tick, 10);
  }

  stop() {
    this.running = false;
    clearTimeout(this._timer);
  }

  async tick() {
    const subs = [...this.engine.subscribers.values()]
      .filter((s) => s.status === 'ACTIVE');
    await Promise.all(subs.map((s) => this.#pumpSubscriber(s.subscriberId)));
  }

  async #pumpSubscriber(subscriberId) {
    if (this._busy.has(subscriberId)) return; // 该订阅方上一封仍在途，严格串行
    const item = this.engine.claimHead(subscriberId);
    if (!item) return;

    // 失败退避：失败后等待 base * 2^(attempt-1)，避免风暴；重启恢复的 PENDING 立即重试。
    if (item.status === 'PENDING' && item.lastError && item.attempts > 0 && !item.requeuedAt) {
      const lastFail = item.history.filter((h) => h.type === 'FAILED').at(-1);
      if (lastFail) {
        const wait = Math.min(this.maxBackoffMs, this.baseBackoffMs * 2 ** (item.attempts - 1));
        if (Date.now() - Date.parse(lastFail.at) < wait) return;
      }
    }
    this._busy.add(subscriberId);
    try {
      await this.#deliverOne(subscriberId, item.envelopeSeq);
    } finally {
      this._busy.delete(subscriberId);
    }
  }

  async #deliverOne(subscriberId, envelopeSeq) {
    // claimHead 已对过期租约追加了 RECLAIMED；这里建立/续立租约。
    const leased = this.engine.lease(subscriberId, envelopeSeq, this.leaseMs);
    const envRec = this.#notificationRecord(envelopeSeq);
    const envelope = envRec.payload.envelope;
    const key = this.engine.keys.get(envelope.keyId); // 信封永远用“签发时”的那把私钥
    const signature = envRec.payload.signature || signPayload(key.privateKeyPem, canonicalEnvelope(envelope));
    const sub = this.engine.subscribers.get(subscriberId);
    try {
      const { httpStatus } = await this.transport.deliver({
        callbackUrl: sub.callbackUrl,
        envelope,
        signature,
        idempotencyKey: leased.idempotencyKey,
        attempt: leased.attempts,
      });
      this.engine.recordSent(subscriberId, envelopeSeq, httpStatus);
    } catch (err) {
      // 投递失败：累计尝试；达到上限转死信。租约仍在账本上留痕（INFLIGHT→PENDING/DEAD）。
      this.engine.recordFailure(subscriberId, envelopeSeq, err.message, err.httpStatus ?? null);
      const after = this.engine.itemsByIdem.get(this.engine.idempotencyKey(subscriberId, envelopeSeq));
      if (after.status === 'DEAD') {
        this.log?.warn?.(`[dispatcher] ${subscriberId} 信封 #${envelopeSeq} 连续 ${after.attempts} 次失败，进入死信队列（后续按序阻塞，等待人工重新入队）`);
      }
    }
  }

  #notificationRecord(envelopeSeq) {
    const rec = this.engine.store.all().find(
      (r) => r.type === 'DR_NOTIFICATION' && r.payload.envelope?.envelopeSeq === envelopeSeq,
    );
    if (!rec) throw new Error(`信封 #${envelopeSeq} 的账本记录缺失`);
    return rec;
  }
}
