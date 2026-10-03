// 裁决与削减报告（纯函数）。
// 报告对象本身被哈希并锚定到事件链；verifyReport 可用任意“读取水位”重新复算，
// 从而证明：原始 v1 按事件结束水位固化，迟到数据只能产生 v2 订正版。
import { round3, sha256Hex, canonicalJSON } from './util.js';

export const QH_KWH = 0.25; // 15 分钟 = 0.25 小时

function kwToKwh(kw) {
  return round3(kw * QH_KWH);
}

// inputs:
// {
//   version, kind: 'ORIGINAL'|'CORRECTED', issuedAt,
//   intervalTs: [ts...], baselineKw: [kw...],
//   meterActualKw: { [ts]: kw } (只含截止水位之前的读数),
//   devices: [{ id, status, requestedKw, lastAckPhase, shedByTs: {ts: kw} }],
//   baselineVersion, cutoffTs, reason }
export function buildReport(event, inputs) {
  const intervals = inputs.intervalTs.map((ts, i) => {
    const base = inputs.baselineKw[i];
    const actual = inputs.meterActualKw[ts] ?? null;
    const hasGap = actual === null;
    const reductionKw = !hasGap && base !== null ? round3(Math.max(0, base - actual)) : null;
    return {
      ts,
      baselineKw: base,
      actualKw: actual,
      reductionKw,
      reductionKwh: reductionKw === null ? null : kwToKwh(reductionKw),
      gap: hasGap,
    };
  });

  const valid = intervals.filter((x) => !x.gap && x.reductionKw !== null);
  const measuredIntervals = valid.length;
  const totalIntervals = intervals.length;
  const reductionKwh = round3(valid.reduce((a, x) => a + x.reductionKwh, 0));
  const avgReductionKw = measuredIntervals
    ? round3(valid.reduce((a, x) => a + x.reductionKw, 0) / measuredIntervals)
    : 0;

  const targetKw = event.targetKw;
  const targetKwh = kwToKwh(targetKw * totalIntervals); // targetKw * 小时数
  const deliveryRatio = targetKwh > 0 ? round3(reductionKwh / targetKwh) : 0;

  // 设备行：回执状态 + 设备实测分摊
  const deviceRows = inputs.devices.map((d) => {
    const shedKwh = round3(
      inputs.intervalTs.reduce((acc, ts) => acc + (d.shedByTs[ts] || 0) * QH_KWH, 0)
    );
    const requestedKwh = d.status === 'EXECUTING'
      ? kwToKwh((d.requestedKw || 0) * totalIntervals)
      : 0;
    let compliance = 'NOT_PARTICIPATING';
    if (d.status === 'EXECUTING') {
      const r = requestedKwh > 0 ? shedKwh / requestedKwh : 0;
      compliance = r >= 0.85 ? 'PERFORMED' : r >= 0.5 ? 'UNDERPERFORMED' : 'FAILED';
    } else if (d.status === 'RECOVERED') {
      compliance = 'PERFORMED';
    } else if (d.status === 'DECLINED' || d.status === 'RECALLED') {
      compliance = 'OPT_OUT';
    }
    return {
      deviceId: d.id,
      status: d.status,
      lastAckPhase: d.lastAckPhase || null,
      requestedKw: d.requestedKw || 0,
      measuredShedKwh: shedKwh,
      requestedShedKwh: requestedKwh,
      compliance,
    };
  });

  const gapCount = intervals.filter((x) => x.gap).length;
  let verdict = 'MISSED';
  const executed = deviceRows.some((d) => d.status === 'EXECUTING' || d.status === 'RECOVERED');
  if (inputs.eventStatus === 'RECALLED') {
    verdict = 'RECALLED';
  } else if (!executed) {
    verdict = 'NO_PARTICIPATION';
  } else if (gapCount >= Math.ceil(totalIntervals * 0.4)) {
    verdict = 'INCONCLUSIVE';
  } else if (deliveryRatio >= 1) {
    verdict = 'MET';
  } else if (deliveryRatio >= 0.5) {
    verdict = 'PARTIAL';
  }

  const report = {
    reportId: `${event.id}:v${inputs.version}`,
    eventId: event.id,
    meterId: event.meterId,
    version: inputs.version,
    kind: inputs.kind,
    reason: inputs.reason || null,
    issuedAt: inputs.issuedAt,
    baselineVersion: inputs.baselineVersion,
    cutoffTs: inputs.cutoffTs,
    windowStart: event.startMs,
    windowEnd: event.endMs,
    targetKw,
    targetKwh,
    measuredIntervals,
    totalIntervals,
    gapCount,
    avgReductionKw,
    reductionKwh,
    deliveryRatio,
    verdict,
    intervals,
    devices: deviceRows,
  };
  // 哈希只覆盖“裁决内容”，不覆盖签发时刻与指纹字段本身；
  // 这样“无新证据则不产生订正版”，且任何一方都能独立重算指纹。
  const { issuedAt, hash, bodyHash, ...body } = report;
  void issuedAt; void hash; void bodyHash;
  report.bodyHash = sha256Hex(canonicalJSON(body));
  report.hash = report.bodyHash; // 报告内容指纹（API 与链上锚定同一值）
  return report;
}

// v2 与 v1 的差异摘要
export function diffReports(v1, v2) {
  const changed = [];
  for (const k of ['verdict', 'reductionKwh', 'avgReductionKw', 'deliveryRatio', 'gapCount', 'baselineVersion']) {
    if (JSON.stringify(v1[k]) !== JSON.stringify(v2[k])) {
      changed.push({ field: k, from: v1[k], to: v2[k] });
    }
  }
  const intervalsChanged = v1.intervals
    .map((x, i) => {
      const y = v2.intervals[i];
      if (x.actualKw !== y.actualKw || x.baselineKw !== y.baselineKw) {
        return { ts: x.ts, from: { actual: x.actualKw, baseline: x.baselineKw }, to: { actual: y.actualKw, baseline: y.baselineKw } };
      }
      return null;
    })
    .filter(Boolean);
  return { changed, intervalsChanged };
}
