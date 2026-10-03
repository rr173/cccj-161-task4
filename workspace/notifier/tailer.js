// 编排服务（../src，data/events.log）→ 通知模块 的只读后向桥接器。
//  - 顺序读编排日志新增行，把 5 类裁决映射为通知事件；
//  - sourceRef = `orch:${seq}`，由引擎幂等去重——尾随位置丢失、重启、与手工 API 并发都安全；
//  - 桥接器绝不写编排日志，只读、失败重试、可随时关闭（NOTIFIER_TAIL_ORCH=0）。
import { openSync, readSync, statSync, closeSync } from 'node:fs';

const MAP = {
  EVENT_PUBLISHED: 'dr.event.published',
  EVENT_CANCELLED: 'dr.event.cancelled',
  EVENT_ACTIVATED: 'dr.event.started',
  EVENT_CLOSED: 'dr.event.ended',
};

export class OrchestratorTailer {
  // engineRef 可为引擎本身或 { engine } 持有者（重启重放后自动跟随当前引擎）。
  constructor(engineRef, orchestratorLog, { pollMs = 250, logger = console } = {}) {
    this.engineRef = engineRef;
    this.file = orchestratorLog;
    this.pollMs = pollMs;
    this.log = logger;
    this.offset = 0;
    this._buf = '';
    this._timer = null;
    this._stopped = false;
  }

  get engine() {
    return this.engineRef?.engine ? this.engineRef.engine : this.engineRef;
  }

  start() {
    const loop = () => {
      if (this._stopped) return;
      this.pump().catch((e) => this.log?.error?.('[tailer] pump 失败：', e));
      this._timer = setTimeout(loop, this.pollMs);
    };
    this._timer = setTimeout(loop, 50);
  }

  stop() {
    this._stopped = true;
    clearTimeout(this._timer);
  }

  async pump() {
    let size;
    try {
      size = statSync(this.file).size;
    } catch {
      return; // 编排日志尚未生成
    }
    if (size < this.offset) {
      // 日志被截断重置（编排方 reset）：从头重读，sourceRef 幂等会忽略已接入记录
      this.offset = 0;
      this._buf = '';
    }
    if (size === this.offset) return;

    const fd = openSync(this.file, 'r');
    try {
      const len = size - this.offset;
      const buf = Buffer.allocUnsafe(len);
      readSync(fd, buf, 0, len, this.offset);
      this.offset = size;
      this._buf += buf.toString('utf8');
    } finally {
      closeSync(fd);
    }

    const lines = this._buf.split('\n');
    this._buf = lines.pop() || ''; // 最后一段可能是残尾，等下次拼接
    for (const line of lines) {
      const t = line.trim();
      if (!t) continue;
      let rec;
      try { rec = JSON.parse(t); } catch { continue; }
      this.#emit(rec);
    }
  }

  #emit(rec) {
    const mapped = this.#mapRecord(rec);
    if (!mapped) return;
    this.engine.ingestEvent({
      type: mapped.type,
      eventId: mapped.eventId,
      sourceRef: `orch:${rec.seq}`,
      occurredAt: rec.ts,
      detail: mapped.detail,
    });
  }

  #mapRecord(rec) {
    const p = rec.payload;
    if (MAP[rec.type]) {
      // 编排方对“开始后撤销”会补一条 reason=CANCEL_AFTER_START 的内部一致性记录
      // （状态在 激活→撤销 间归位），对订阅方是同一次撤销，不重复通知。
      if (rec.type === 'EVENT_CANCELLED' && p.reason === 'CANCEL_AFTER_START') return null;
      const eventId = p.eventId || p.id;
      if (!eventId) return null;
      const detail = { orchestratorSeq: rec.seq };
      if (p.reason) detail.reason = p.reason;
      if (p.meterId) detail.meterId = p.meterId;
      if (p.targetKw != null) detail.targetKw = p.targetKw;
      if (p.startMs != null) detail.startMs = p.startMs;
      if (p.endMs != null) detail.endMs = p.endMs;
      return { type: MAP[rec.type], eventId, detail };
    }
    // 报告订正：只有 v>1 的 REPORT_ISSUED（kind=CORRECTED）才通知；
    // v1 原始裁决随 EVENT_CLOSED 已隐含，避免重复打扰订阅方。
    if (rec.type === 'REPORT_ISSUED' && p.report?.version > 1) {
      const r = p.report;
      return {
        type: 'dr.event.report.corrected',
        eventId: r.eventId,
        detail: {
          orchestratorSeq: rec.seq,
          reportId: r.reportId,
          version: r.version,
          kind: r.kind,
          reason: r.reason,
          hash: r.hash,
        },
      };
    }
    return null;
  }
}
