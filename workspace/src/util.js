// 时间 / 区间 / 数值 / 哈希 / 规范化 JSON 等基础工具（UTC，15 分钟区间）。
import { createHash } from 'node:crypto';

export const INTERVAL_MS = 15 * 60 * 1000;
export const INTERVALS_PER_DAY = 96;

export function pad2(n) {
  return String(n).padStart(2, '0');
}

// 形如 2026-09-12 的 UTC 日键
export function dayKey(ts) {
  const d = new Date(ts);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

export function dateOfDayKey(key) {
  const [y, m, d] = key.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d));
}

export function iso(ts) {
  return new Date(ts).toISOString();
}

// 一天内第几个 15 分钟区间（0..95）
export function intervalIndex(ts) {
  const d = new Date(ts);
  return Math.floor(
    (d.getUTCHours() * 60 + d.getUTCMinutes()) / 15
  );
}

export function isWeekendKey(key) {
  const dow = dateOfDayKey(key).getUTCDay();
  return dow === 0 || dow === 6;
}

// 向最近的区间起点取整
export function floorInterval(ts) {
  return Math.floor(ts / INTERVAL_MS) * INTERVAL_MS;
}

// [start,end) 的区间起点时间戳列表（要求端点对齐 15 分钟）
export function intervalStarts(start, end) {
  const out = [];
  for (let t = start; t < end; t += INTERVAL_MS) out.push(t);
  return out;
}

export function intervalKey(ts) {
  return String(floorInterval(ts));
}

export function round3(n) {
  return Math.round(n * 1000) / 1000;
}

export function mean(nums) {
  if (!nums.length) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

export function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

export function uid(prefix) {
  const s = createHash('sha1').update(`${prefix}-${Date.now()}-${Math.random()}`).digest('hex');
  return `${prefix}_${s.slice(0, 10)}`;
}

// 键按字典序稳定排序的规范化 JSON（用于哈希）
export function canonicalJSON(value) {
  return JSON.stringify(sortValue(value));
}

function sortValue(v) {
  if (Array.isArray(v)) return v.map(sortValue);
  if (v && typeof v === 'object') {
    const out = {};
    for (const k of Object.keys(v).sort()) out[k] = sortValue(v[k]);
    return out;
  }
  return v;
}

export function sha256Hex(s) {
  return createHash('sha256').update(s).digest('hex');
}
