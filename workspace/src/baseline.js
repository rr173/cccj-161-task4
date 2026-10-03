// 基线算法。
// 候选日：事件日之前、该计量点非“事件日”、数据完整度 >= 80% 的同类日（工作日/周末）；
// 同类日不足时回退到任意合格日。
import { INTERVALS_PER_DAY, dayKey, isWeekendKey, mean, round3 } from './util.js';

export const BASELINE_ALGORITHMS = ['LAST_10_AVG', 'MID_8_OF_10', 'LAST_5_AVG'];

// readingsByDay: { dayKey: { idx: kW 或 null } }（idx 0..95）
// 返回 { startInterval, days:[dayKey], perInterval:[kW...96], coverage }
export function computeBaseline({
  algorithm,
  eventTs,
  readingsByDay,
  eventDays, // Set<dayKey>
  eventDayKey = null, // 事件所在日（也排除）
}) {
  if (!BASELINE_ALGORITHMS.includes(algorithm)) {
    throw Object.assign(new Error(`未知基线算法 ${algorithm}`), { code: 'BAD_REQUEST' });
  }
  const targetCount = algorithm === 'LAST_5_AVG' ? 5 : 10;
  const sameClass = isWeekendKey(eventDayKey || dayKey(eventTs));

  const eligible = [];
  Object.keys(readingsByDay || {})
    .sort()
    .reverse()
    .forEach((key) => {
      if (key >= (eventDayKey || dayKey(eventTs))) return; // 只取事件日之前
      if (eventDays && eventDays.has(key)) return;
      const day = readingsByDay[key] || {};
      let present = 0;
      for (let i = 0; i < INTERVALS_PER_DAY; i++) {
        const v = day[i];
        if (typeof v === 'number' && Number.isFinite(v)) present++;
      }
      if (present >= Math.ceil(INTERVALS_PER_DAY * 0.8)) {
        eligible.push({ key, coverage: present / INTERVALS_PER_DAY });
      }
    });

  const sameClassDays = eligible.filter((e) => isWeekendKey(e.key) === sameClass);
  const pool = (sameClassDays.length >= Math.min(3, targetCount) ? sameClassDays : eligible)
    .slice(0, targetCount);

  let chosen;
  if (algorithm === 'MID_8_OF_10' && pool.length >= 8) {
    // 取最近 10 个合格日中，日均负荷居中的 8 天（去掉最高/最低各 1 天）
    const ten = pool.slice(0, 10);
    const withAvg = ten.map((e) => {
      const vals = [];
      for (let i = 0; i < INTERVALS_PER_DAY; i++) {
        const v = readingsByDay[e.key][i];
        if (typeof v === 'number') vals.push(v);
      }
      return { key: e.key, avg: mean(vals) };
    });
    withAvg.sort((a, b) => a.avg - b.avg);
    chosen = withAvg.slice(1, Math.max(1, withAvg.length - 1)).map((x) => x.key);
  } else {
    chosen = pool.map((e) => e.key);
  }

  const perInterval = [];
  for (let i = 0; i < INTERVALS_PER_DAY; i++) {
    const vals = chosen
      .map((k) => readingsByDay[k][i])
      .filter((v) => typeof v === 'number' && Number.isFinite(v));
    perInterval.push(vals.length ? round3(mean(vals)) : null);
  }

  return {
    algorithm,
    days: chosen,
    perInterval,
    coverage: chosen.length,
    computedFromDays: chosen.length,
  };
}

// 将 96 点基线裁剪到事件时窗（按区间索引）
export function sliceWindowBaseline(perInterval, startIdx, endIdxExclusive) {
  const out = [];
  for (let i = startIdx; i < endIdxExclusive; i++) {
    out.push(perInterval[i] ?? null);
  }
  return out;
}
