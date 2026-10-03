// 确定性种子数据：2 个聚合商、3 个计量点、5 台设备、21 天 15 分钟历史读数。
// 所有种子数据同样作为不可变记录写入哈希链。
import { INTERVALS_PER_DAY } from './util.js';

export const BASE_NOW = Date.UTC(2026, 8, 15, 1, 30); // 2026-09-15 01:30 UTC（事件发布时刻，周二）

function strSeed(s) {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function mulberry32(a) {
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// 计量点日内负荷曲线（按小时，kW），商业/工业/混合三种画像
function meterProfile(meterIdx, dow, h, rnd) {
  const weekend = dow === 0 || dow === 6;
  if (meterIdx === 0) {
    // 商业：午后到晚间高峰
    const peak = 26 * Math.exp(-((h - 15) ** 2) / 18) + 14 * Math.exp(-((h - 20) ** 2) / 6);
    const base = weekend ? 78 : 92;
    const factor = weekend ? 0.82 : 1;
    return (base + peak) * factor + (rnd() - 0.5) * 3.2;
  }
  if (meterIdx === 1) {
    // 工业：白天平稳高负荷
    const day = h >= 7 && h <= 21 ? 62 : 18;
    const base = weekend ? 95 : 120;
    return base + day * (weekend ? 0.35 : 1) + (rnd() - 0.5) * 2.6;
  }
  // 混合：早峰 + 晚峰
  const peak = 22 * Math.exp(-((h - 9) ** 2) / 8) + 26 * Math.exp(-((h - 19) ** 2) / 8);
  const base = weekend ? 70 : 84;
  return base + peak * (weekend ? 0.7 : 1) + (rnd() - 0.5) * 3;
}

// 确定性的某日负荷画像（补传/迟到数据与历史画像完全一致）
export function dayValuesForKey(meterIdx, key) {
  const [y, m, d] = key.split('-').map(Number);
  const dayDate = new Date(Date.UTC(y, m - 1, d));
  const dow = dayDate.getUTCDay();
  const rnd = mulberry32(strSeed(`mp${meterIdx}-${key}`));
  const vals = new Array(INTERVALS_PER_DAY);
  for (let i = 0; i < INTERVALS_PER_DAY; i++) {
    vals[i] = Math.round(meterProfile(meterIdx, dow, i / 4, rnd) * 100) / 100;
  }
  return { key, values: vals };
}

export function dayValues(meterIdx, dayOffset) {
  const key = new Date(BASE_NOW - dayOffset * 86400000).toISOString().slice(0, 10);
  return dayValuesForKey(meterIdx, key);
}

export function seed(store, now = BASE_NOW) {
  store.append('SEED_CLOCK', { now }, now);

  const aggregators = [
    { id: 'AGG-A', name: '北星能源聚合' },
    { id: 'AGG-B', name: '南网负荷聚合' },
  ];
  aggregators.forEach((a) => store.append('SEED_AGGREGATOR', a, now));

  const meters = [
    { id: 'MP-1', name: '浦东#12 商业表', kind: '商业', timezone: 'UTC' },
    { id: 'MP-2', name: '松江#07 工业表', kind: '工业', timezone: 'UTC' },
    { id: 'MP-3', name: '嘉定#21 混合表', kind: '混合', timezone: 'UTC' },
  ];
  meters.forEach((m) => store.append('SEED_METER', m, now));

  // meterId, 设备，额定可削减 kW，执行有效率（真实响应相对指令的比例）
  const devices = [
    { id: 'DEV-1', meterId: 'MP-1', name: '中央空调机组 A', maxKw: 50, eff: 0.92 },
    { id: 'DEV-2', meterId: 'MP-1', name: '冷库压缩机 B', maxKw: 30, eff: 0.88 },
    { id: 'DEV-3', meterId: 'MP-2', name: '产线可调负荷 C', maxKw: 40, eff: 0.85 },
    { id: 'DEV-4', meterId: 'MP-3', name: '蓄冰空调 D', maxKw: 60, eff: 0.9 },
    { id: 'DEV-5', meterId: 'MP-3', name: '照明分区 E', maxKw: 25, eff: 0.7 },
  ];
  devices.forEach((d) => store.append('SEED_DEVICE', d, now));

  // 21 个历史日；故意制造缺口：
  // - MP-1 缺 D-4（演示迟到历史数据 -> 基线订正版 v2）
  // - MP-2 缺 D-2
  const MISSING = { 'MP-1': [4], 'MP-2': [2], 'MP-3': [] };
  meters.forEach((m, meterIdx) => {
    for (let off = 1; off <= 21; off++) {
      if (MISSING[m.id].includes(off)) continue;
      const { key, values } = dayValues(meterIdx, off);
      store.append('SEED_DAY_READINGS', { meterId: m.id, day: key, values }, now);
    }
  });

  return { aggregators, meters, devices };
}

export { meterProfile, mulberry32, strSeed };
