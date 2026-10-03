// 履约异议处置工作流测试：
// 受理（截止时刻/标的指纹）、客户请求号幂等、同一标的唯一待处理、材料顺序追加、
// 审核终态、原裁定不可改、新裁定不转移旧异议、重启恢复、投递服务三类消息。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import fsp from 'node:fs/promises';
import { Kernel, DISPUTE_WINDOW_MS } from '../src/kernel.js';
import { BASE_NOW } from '../src/seed.js';
import { EventStore } from '../src/store.js';
import { NotifierEngine, NOTIFY_TYPES } from '../notifier/engine.js';
import { OrchestratorTailer } from '../notifier/tailer.js';

const I = 15 * 60 * 1000;
const START = Date.UTC(2026, 8, 16, 2, 0, 0);
const END = START + 16 * I;

function newKernel() {
  return Kernel.freshMemory(BASE_NOW);
}

function publish(k, { meterId = 'MP-1', targetKw = 40, start = START, end = END, algorithm = 'MID_8_OF_10' } = {}) {
  return k.dispatch({ type: 'PUBLISH_EVENT', meterId, targetKw, startMs: start, endMs: end, algorithm }).event.id;
}

function runEvent(k, id, { device = 'DEV-1', kw = 40, agg = 'AGG-A' } = {}) {
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: device, aggregatorId: agg });
  k.dispatch({ type: 'TICK', minutes: (k.s.events.get(id).startMs - k.now()) / 60000 });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: device, status: 'ACCEPTED', requestedKw: kw });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: device, status: 'EXECUTING', requestedKw: kw });
  k.dispatch({ type: 'TICK', minutes: (k.s.events.get(id).endMs - k.now()) / 60000 });
}

// 跑出一个已封存 v1 裁定
function sealedReport(k, opts = {}) {
  const eventId = publish(k, opts);
  runEvent(k, eventId, opts);
  return { eventId, report: k.s.reports.get(eventId)[0] };
}

function file(k, { requestId = 'REQ-1', aggregatorId = 'AGG-A', reportId, reportHash, reason = '计量数据存疑' } = {}) {
  return k.dispatch({ type: 'FILE_DISPUTE', requestId, aggregatorId, reportId, reportHash, reason });
}

function supplement(k, disputeId, { requestId, aggregatorId = 'AGG-A', note = '补充证据', materialSeq } = {}) {
  const cmd = { type: 'SUPPLEMENT_DISPUTE', disputeId, requestId, aggregatorId, note };
  if (materialSeq != null) cmd.materialSeq = materialSeq;
  return k.dispatch(cmd);
}

function resolve(k, disputeId, { requestId, outcome = 'UPHELD', comment = null } = {}) {
  return k.dispatch({ type: 'RESOLVE_DISPUTE', disputeId, requestId, outcome, comment });
}

function throwsCode(fn, code) {
  try {
    fn();
  } catch (e) {
    assert.equal(e.code, code, `期望错误码 ${code}，实际 ${e.code}：${e.message}`);
    return;
  }
  assert.fail(`期望抛出 ${code}，但没有抛错`);
}

test('受理：截止前对已封存裁定提起异议，标的=编号+指纹，时限=签发后48小时', () => {
  const k = newKernel();
  const { eventId, report } = sealedReport(k);
  const out = file(k, { requestId: 'REQ-A1', reportId: report.reportId, reportHash: report.hash });
  assert.equal(out.idempotent, false);
  const d = out.dispute;
  assert.equal(d.status, 'PENDING');
  assert.equal(d.eventId, eventId);
  assert.equal(d.reportId, report.reportId);
  assert.equal(d.reportVersion, 1);
  assert.equal(d.reportHash, report.hash, '标的锚定内容指纹');
  assert.equal(d.deadline, report.issuedAt + DISPUTE_WINDOW_MS, '截止时刻 = 裁定签发后 48 仿真小时');
  assert.equal(d.materials.length, 0);
  assert.deepEqual(d.history.map((h) => h.type), ['FILED']);
});

test('受理校验：未封存裁定/指纹不符/未知聚合商/缺请求号均被拒', () => {
  const k = newKernel();
  const id = publish(k); // 事件未结束，尚无裁定
  throwsCode(() => file(k, { requestId: 'REQ-B1', reportId: `${id}:v1`, reportHash: 'x'.repeat(64) }), 'REPORT_NOT_FOUND');

  runEvent(k, id); // 事件结束，v1 裁定封存
  const report = k.s.reports.get(id)[0];
  throwsCode(
    () => file(k, { requestId: 'REQ-B2', reportId: report.reportId, reportHash: '0'.repeat(64) }),
    'REPORT_HASH_MISMATCH'
  );
  throwsCode(
    () => file(k, { requestId: 'REQ-B3', aggregatorId: 'AGG-Z', reportId: report.reportId, reportHash: report.hash }),
    'NOT_FOUND'
  );
  throwsCode(
    () => k.dispatch({ type: 'FILE_DISPUTE', aggregatorId: 'AGG-A', reportId: report.reportId, reportHash: report.hash, reason: 'x' }),
    'BAD_REQUEST'
  );
});

test('客户请求号：同号同文返回原结果（不新增记录），同号不同文返回 409', () => {
  const k = newKernel();
  const { report } = sealedReport(k);
  const first = file(k, { requestId: 'REQ-C1', reportId: report.reportId, reportHash: report.hash });
  const filedCount = k.store.all().filter((r) => r.type === 'DISPUTE_FILED').length;

  const again = file(k, { requestId: 'REQ-C1', reportId: report.reportId, reportHash: report.hash });
  assert.equal(again.idempotent, true);
  assert.equal(again.dispute.disputeId, first.dispute.disputeId, '返回原受理结果');
  assert.equal(
    k.store.all().filter((r) => r.type === 'DISPUTE_FILED').length,
    filedCount,
    '幂等重放不新增记录'
  );

  throwsCode(
    () => file(k, { requestId: 'REQ-C1', reportId: report.reportId, reportHash: report.hash, reason: '不同的正文' }),
    'REQUEST_ID_CONFLICT'
  );
});

test('同一代理商对同一标的只允许一个待处理实例；裁定后可再提新实例', () => {
  const k = newKernel();
  const { report } = sealedReport(k);
  const d1 = file(k, { requestId: 'REQ-D1', reportId: report.reportId, reportHash: report.hash }).dispute;

  throwsCode(
    () => file(k, { requestId: 'REQ-D2', reportId: report.reportId, reportHash: report.hash, reason: '再次异议' }),
    'DISPUTE_PENDING_EXISTS'
  );
  // 不同代理商可对同一标的提起
  const d2 = file(k, { requestId: 'REQ-D3', aggregatorId: 'AGG-B', reportId: report.reportId, reportHash: report.hash }).dispute;
  assert.equal(d2.status, 'PENDING');

  // d1 审核终结后，同一代理商可对同一标的再提新实例（前一实例已非待处理）
  resolve(k, d1.disputeId, { requestId: 'REQ-D4', outcome: 'REJECTED' });
  const d3 = file(k, { requestId: 'REQ-D5', reportId: report.reportId, reportHash: report.hash, reason: '新证据再议' }).dispute;
  assert.notEqual(d3.disputeId, d1.disputeId);
  assert.equal(d3.status, 'PENDING');
});

test('截止时刻后提起异议被拒', () => {
  const k = newKernel();
  const { report } = sealedReport(k);
  k.dispatch({ type: 'TICK', minutes: DISPUTE_WINDOW_MS / 60000 + 15 });
  throwsCode(
    () => file(k, { requestId: 'REQ-E1', reportId: report.reportId, reportHash: report.hash }),
    'DISPUTE_DEADLINE_PASSED'
  );
});

test('补充材料只能顺序追加：显式序号校验、非提起方拒绝、请求号幂等', () => {
  const k = newKernel();
  const { report } = sealedReport(k);
  const d = file(k, { requestId: 'REQ-F1', reportId: report.reportId, reportHash: report.hash }).dispute;

  const m1 = supplement(k, d.disputeId, { requestId: 'REQ-F2', note: '材料一', materialSeq: 1 });
  assert.equal(m1.material.seq, 1);
  // 跳序被拒
  throwsCode(
    () => supplement(k, d.disputeId, { requestId: 'REQ-F3', note: '材料X', materialSeq: 3 }),
    'MATERIAL_SEQ_CONFLICT'
  );
  // 非提起方被拒
  throwsCode(
    () => supplement(k, d.disputeId, { requestId: 'REQ-F4', aggregatorId: 'AGG-B', note: '越权补充' }),
    'DISPUTE_OWNER_MISMATCH'
  );
  const m2 = supplement(k, d.disputeId, { requestId: 'REQ-F5', note: '材料二' });
  assert.equal(m2.material.seq, 2);
  const m3 = supplement(k, d.disputeId, { requestId: 'REQ-F6', note: '材料三' });
  assert.equal(m3.material.seq, 3);
  assert.deepEqual(m3.dispute.materials.map((m) => [m.seq, m.note]), [[1, '材料一'], [2, '材料二'], [3, '材料三']]);

  // 同号同文幂等重放：返回原结果，不重复追加
  const replay = supplement(k, d.disputeId, { requestId: 'REQ-F6', note: '材料三' });
  assert.equal(replay.idempotent, true);
  assert.equal(replay.material.seq, 3);
  assert.equal(k.disputeView(d.disputeId).materials.length, 3);
  // 同号不同文 -> 409
  throwsCode(
    () => supplement(k, d.disputeId, { requestId: 'REQ-F6', note: '改动后的材料三' }),
    'REQUEST_ID_CONFLICT'
  );
});

test('审核终态：通过/驳回后不可再裁定、不可再补充；审核请求号幂等', () => {
  const k = newKernel();
  const { report } = sealedReport(k);
  const d = file(k, { requestId: 'REQ-G1', reportId: report.reportId, reportHash: report.hash }).dispute;
  supplement(k, d.disputeId, { requestId: 'REQ-G2', note: '证据' });

  const done = resolve(k, d.disputeId, { requestId: 'REQ-G3', outcome: 'UPHELD', comment: '异议成立' });
  assert.equal(done.dispute.status, 'UPHELD');
  assert.equal(done.dispute.resolution.outcome, 'UPHELD');
  assert.equal(done.dispute.resolution.comment, '异议成立');

  // 同号同文 -> 幂等返回原结论（不是错误）
  const replay = resolve(k, d.disputeId, { requestId: 'REQ-G3', outcome: 'UPHELD', comment: '异议成立' });
  assert.equal(replay.idempotent, true);
  assert.equal(replay.dispute.status, 'UPHELD');
  // 新请求号再次裁定 -> 终态拒绝
  throwsCode(() => resolve(k, d.disputeId, { requestId: 'REQ-G4', outcome: 'REJECTED' }), 'DISPUTE_TERMINAL');
  // 终态后补充材料被拒
  throwsCode(() => supplement(k, d.disputeId, { requestId: 'REQ-G5', note: '终态后补充' }), 'DISPUTE_TERMINAL');

  // 驳回路径同样终态
  const d2 = file(k, { requestId: 'REQ-G6', aggregatorId: 'AGG-B', reportId: report.reportId, reportHash: report.hash }).dispute;
  resolve(k, d2.disputeId, { requestId: 'REQ-G7', outcome: 'REJECTED' });
  assert.equal(k.disputeView(d2.disputeId).status, 'REJECTED');
  throwsCode(() => resolve(k, d2.disputeId, { requestId: 'REQ-G8', outcome: 'UPHELD' }), 'DISPUTE_TERMINAL');
  // 非法结论
  const d3 = file(k, { requestId: 'REQ-G9', reportId: report.reportId, reportHash: report.hash, reason: '再议' }).dispute;
  throwsCode(() => resolve(k, d3.disputeId, { requestId: 'REQ-G10', outcome: 'MAYBE' }), 'BAD_REQUEST');
});

test('被质疑的原始裁定内容不可修改（异议全生命周期指纹恒定）', () => {
  const k = newKernel();
  const { eventId, report } = sealedReport(k);
  const before = JSON.stringify(k.s.reports.get(eventId)[0]);

  const d = file(k, { requestId: 'REQ-H1', reportId: report.reportId, reportHash: report.hash }).dispute;
  supplement(k, d.disputeId, { requestId: 'REQ-H2', note: '证据一' });
  supplement(k, d.disputeId, { requestId: 'REQ-H3', note: '证据二' });
  resolve(k, d.disputeId, { requestId: 'REQ-H4', outcome: 'UPHELD', comment: '裁定维持，记录异议成立' });

  assert.equal(JSON.stringify(k.s.reports.get(eventId)[0]), before, 'v1 裁定字节级不变');
  assert.equal(k.s.reports.get(eventId).length, 1, '异议流程不产生报告新版本');
  assert.equal(k.store.verify().ok, true);
});

test('后补计量数据生成新裁定：旧异议锚定原标的不转移，新标的可单独受理', () => {
  const k = newKernel();
  const { eventId, report } = sealedReport(k);
  const d1 = file(k, { requestId: 'REQ-I1', reportId: report.reportId, reportHash: report.hash }).dispute;
  supplement(k, d1.disputeId, { requestId: 'REQ-I2', note: '针对 v1 的证据' });

  // 迟到历史数据 -> 基线 v2 + 裁定 v2
  const out = k.dispatch({ type: 'LOAD_MISSING_DAY', meterId: 'MP-1', dayOffset: 4 });
  assert.equal(out.reportsIssued.length, 1);
  const v2 = k.s.reports.get(eventId)[1];
  assert.equal(v2.version, 2);

  // 旧异议仍指向原标的（编号+指纹不变），材料保留
  const view = k.disputeView(d1.disputeId);
  assert.equal(view.reportId, report.reportId);
  assert.equal(view.reportHash, report.hash);
  assert.equal(view.reportVersion, 1);
  assert.equal(view.materials.length, 1);
  assert.equal(view.status, 'PENDING');

  // 新标的（v2）可单独受理，两个待处理并存
  const d2 = file(k, { requestId: 'REQ-I3', reportId: v2.reportId, reportHash: v2.hash, reason: '对订正版仍有异议' }).dispute;
  assert.equal(d2.reportVersion, 2);
  assert.equal(k.listDisputes().filter((x) => x.status === 'PENDING').length, 2);
  // v1 裁定依旧字节级不变
  assert.equal(k.s.reports.get(eventId)[0].hash, report.hash);
});

test('重启恢复：待处理状态、时限与材料次序不变，请求号幂等台账保留', async () => {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'dr-dsp-'));
  const logFile = path.join(dir, 'events.log');
  try {
    const st1 = new EventStore(logFile);
    const k1 = new Kernel(st1);
    const { report } = sealedReport(k1);
    const d = file(k1, { requestId: 'REQ-R1', reportId: report.reportId, reportHash: report.hash }).dispute;
    supplement(k1, d.disputeId, { requestId: 'REQ-R2', note: '材料一' });
    supplement(k1, d.disputeId, { requestId: 'REQ-R3', note: '材料二' });
    const viewBefore = k1.disputeView(d.disputeId);

    const st2 = new EventStore(logFile);
    await st2.load();
    const k2 = new Kernel(st2, { boot: false });
    k2.replay();
    const viewAfter = k2.disputeView(d.disputeId);
    assert.deepEqual(viewAfter, viewBefore, '重放后工单投影完全一致');
    assert.equal(viewAfter.status, 'PENDING');
    assert.equal(viewAfter.deadline, report.issuedAt + DISPUTE_WINDOW_MS, '时限不变');
    assert.deepEqual(viewAfter.materials.map((m) => m.seq), [1, 2], '材料次序不变');
    assert.deepEqual(viewAfter.materials.map((m) => m.note), ['材料一', '材料二']);

    // 请求号幂等台账随重放恢复：同号同文仍返回原结果，不新增记录
    const recCount = st2.all().length;
    const again = file(k2, { requestId: 'REQ-R1', reportId: report.reportId, reportHash: report.hash });
    assert.equal(again.idempotent, true);
    assert.equal(again.dispute.disputeId, d.disputeId);
    assert.equal(st2.all().length, recCount);

    // 重启后可继续顺序补充与审核
    const sup = supplement(k2, d.disputeId, { requestId: 'REQ-R4', note: '材料三' });
    assert.equal(sup.material.seq, 3);
    const done = resolve(k2, d.disputeId, { requestId: 'REQ-R5', outcome: 'REJECTED', comment: '证据不足' });
    assert.equal(done.dispute.status, 'REJECTED');
    assert.equal(st2.verify().ok, true);
  } finally {
    await fsp.rm(dir, { recursive: true, force: true });
  }
});

test('投递服务：受理/补充/结论分别生成可验签且去重的消息', async () => {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'dr-dsp-ntf-'));
  const orchFile = path.join(dir, 'events.log');
  try {
    const st = new EventStore(orchFile);
    const k = new Kernel(st);
    const { eventId, report } = sealedReport(k);
    const d = file(k, { requestId: 'REQ-N1', reportId: report.reportId, reportHash: report.hash }).dispute;
    supplement(k, d.disputeId, { requestId: 'REQ-N2', note: '补充证据' });
    resolve(k, d.disputeId, { requestId: 'REQ-N3', outcome: 'UPHELD' });

    const eng = new NotifierEngine(new EventStore(null), {});
    await eng.load();
    const tailer = new OrchestratorTailer(eng, orchFile, { pollMs: 60_000 });
    await tailer.pump();

    const disputeEnvs = [...eng.envelopes.values()].filter((e) => e.type.startsWith('dr.dispute.'));
    const byType = new Map(disputeEnvs.map((e) => [e.type, e]));
    assert.equal(disputeEnvs.length, 3, '受理/补充/结论各生成一条消息');
    for (const t of [NOTIFY_TYPES.DISPUTE_ACCEPTED, NOTIFY_TYPES.DISPUTE_SUPPLEMENTED, NOTIFY_TYPES.DISPUTE_RESOLVED]) {
      assert.ok(byType.has(t), `缺少 ${t}`);
    }
    // 内容对应工单与标的；sourceRef 唯一（orch:seq）
    assert.equal(byType.get(NOTIFY_TYPES.DISPUTE_ACCEPTED).detail.disputeId, d.disputeId);
    assert.equal(byType.get(NOTIFY_TYPES.DISPUTE_ACCEPTED).detail.reportHash, report.hash);
    assert.equal(byType.get(NOTIFY_TYPES.DISPUTE_SUPPLEMENTED).detail.materialSeq, 1);
    assert.equal(byType.get(NOTIFY_TYPES.DISPUTE_RESOLVED).detail.outcome, 'UPHELD');
    assert.equal(byType.get(NOTIFY_TYPES.DISPUTE_ACCEPTED).eventId, eventId);
    assert.equal(new Set(disputeEnvs.map((e) => e.sourceRef)).size, 3);

    // 每条消息均可独立验签
    for (const e of disputeEnvs) {
      const v = eng.verifyEnvelope(e.envelopeSeq);
      assert.equal(v.signatureValid, true);
      assert.equal(v.immutabilityIntact, true);
    }

    // 去重：再次 pump 不产生新信封；同 sourceRef 重复接入幂等
    const before = eng.envelopeSeq;
    await tailer.pump();
    assert.equal(eng.envelopeSeq, before);
    const dup = eng.ingestEvent({
      type: NOTIFY_TYPES.DISPUTE_ACCEPTED,
      eventId,
      sourceRef: byType.get(NOTIFY_TYPES.DISPUTE_ACCEPTED).sourceRef,
    });
    assert.equal(dup.deduplicated, true);
    assert.equal(eng.envelopeSeq, before);
  } finally {
    await fsp.rm(dir, { recursive: true, force: true });
  }
});
