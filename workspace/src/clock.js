// 仿真时钟：默认冻结在固定的“现在”，仅由 /api/sim/tick 推进，
// 这样裁决、迟到数据、订正版本都是可重复演示的。
export class Clock {
  constructor(startTs) {
    this.nowTs = startTs;
  }
  now() {
    return this.nowTs;
  }
  advance(ms) {
    this.nowTs += ms;
    return this.nowTs;
  }
  set(ts) {
    this.nowTs = ts;
    return this.nowTs;
  }
}
