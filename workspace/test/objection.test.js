// 履约异议处置工作流领域契约（node:test）：
// 标的为已封存裁定（编号 + 内容指纹）、截止时刻、请求号幂等/409、
// 同一代理商同一标的唯一待处理、材料顺序追加、审核终态、
// 迟到数据新裁定不转移旧异议、重启恢复（状态/时限/材料次序不变）。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Kernel } from '../src/kernel.js';
import { EventStore } from '../src/store.js';
import { BASE_NOW } from '../src/seed.js';

const I = 15 * 60 * 1000;
const START = Date.UTC(2026, 8, 16, 2, 0, 0);
const END = START + 16 * I;

function newKernel() {
  return Kernel.freshMemory(BASE_NOW);
}

function publish(k, { meterId = 'MP-1', targetKw = 40, start = START, end = END } = {}) {
  return k.dispatch({ type: 'PUBLISH_EVENT', meterId, targetKw, startMs: start, endMs: end, algorithm: 'MID_8_OF_10' }).event.id;
}

// 结束事件并返回 v1 已封存报告
function sealedEvent(k, { id, device = 'DEV-1', kw = 40, agg = 'AGG-A' } = {}) {
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: device, aggregatorId: agg });
  k.dispatch({ type: 'TICK', minutes: (k.s.events.get(id).startMs - k.now()) / 60000 });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: device, status: 'ACCEPTED', requestedKw: kw });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: device, status: 'EXECUTING', requestedKw: kw });
  k.dispatch({ type: 'TICK', minutes: (k.s.events.get(id).endMs - k.now()) / 60000 });
  return k.s.reports.get(id)[0];
}

function fileObj(k, { id, version = 1, hash, agg = 'AGG-A', req = 'REQ-1', reason = '基线偏高' } = {}) {
  const cmd = {
    type: 'FILE_OBJECTION',
    requestId: req, eventId: id, reportVersion: version,
    aggregatorId: agg, reason,
  };
  // 仅在调用方未显式提供指纹时才取链上指纹（未封存场景留给内核报 REPORT_NOT_SEALED）
  if (hash !== undefined || k.s.reports.get(id)?.length) {
    const report = k.s.reports.get(id).find((r) => r.version === version);
    cmd.reportHash = hash ?? report?.hash;
  }
  return k.dispatch(cmd);
}

test('只能对已封存裁定提起异议：未结束事件 -> REPORT_NOT_SEALED', () => {
  const k = newKernel();
  const id = publish(k);
  assert.throws(
    () => fileObj(k, { id }),
    (e) => e.code === 'REPORT_NOT_SEALED'
  );
});

test('受理：以编号与内容指纹为标的，截止时刻 = 封存后 24h', () => {
  const k = newKernel();
  const id = publish(k);
  const r1 = sealedEvent(k, { id });
  const out = fileObj(k, { id });
  assert.equal(out.filed, true);
  const o = out.objection;
  assert.equal(o.target, `${id}:v1`);
  assert.equal(o.reportId, r1.reportId);
  assert.equal(o.reportHash, r1.hash);
  assert.equal(o.status, 'PENDING');
  assert.equal(o.deadlineMs, r1.issuedAt + 24 * 3600000);
  assert.equal(o.overdue, false);
  assert.equal(o.targetIntact, true);
  assert.equal(o.materials.length, 0);
});

test('内容指纹与封存裁定不一致 -> FINGERPRINT_MISMATCH（不得质疑被篡改内容）', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  assert.throws(
    () => fileObj(k, { id, hash: 'a'.repeat(64), req: 'REQ-X' }),
    (e) => e.code === 'FINGERPRINT_MISMATCH'
  );
  // 未产生任何异议（校验失败不追加记录）
  assert.equal(k.objectionListView().length, 0);
});

test('超过截止时刻 -> OBJECTION_DEADLINE_PASSED；恰好截止时刻仍可受理', () => {
  const k = newKernel();
  const id = publish(k);
  // 第二事件排在第一事件窗口之后，稍后一并跨越
  const start2 = START + 20 * I;
  const id2 = publish(k, { meterId: 'MP-2', targetKw: 20, start: start2, end: start2 + 8 * I });
  sealedEvent(k, { id });
  sealedEvent(k, { id: id2, device: 'DEV-3', agg: 'AGG-B', kw: 20 });
  const r2IssuedAt = k.s.reports.get(id2)[0].issuedAt;

  // 推进到事件 2 的恰好截止时刻：此时事件 1 早已过截止、事件 2 正好到点
  const mins = (r2IssuedAt + 24 * 3600000 - k.now()) / 60000;
  assert.ok(mins > 0);
  k.dispatch({ type: 'TICK', minutes: mins });
  assert.throws(
    () => fileObj(k, { id, req: 'REQ-LATE' }),
    (e) => e.code === 'OBJECTION_DEADLINE_PASSED'
  );
  const out = fileObj(k, { id: id2, agg: 'AGG-B', req: 'REQ-EDGE' });
  assert.equal(out.objection.status, 'PENDING');
  assert.equal(out.objection.overdue, false);
  // 再推进一个区间即超时
  k.dispatch({ type: 'TICK', minutes: 15 });
  assert.throws(
    () => fileObj(k, { id: id2, agg: 'AGG-B', req: 'REQ-LATE2' }),
    (e) => e.code === 'OBJECTION_DEADLINE_PASSED'
  );
});

test('请求号重复：同正文返回原结果（幂等），正文不同返回 409', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  const first = fileObj(k, { id, req: 'REQ-DUP', reason: '同一份正文' });
  const again = fileObj(k, { id, req: 'REQ-DUP', reason: '同一份正文' });
  assert.equal(again.idempotent, true);
  assert.equal(again.objection.id, first.objection.id);
  assert.equal(k.objectionListView().length, 1, '幂等重放不产生第二个实例');

  // 同号不同正文（理由变化）-> 409
  assert.throws(
    () => fileObj(k, { id, req: 'REQ-DUP', reason: '不一样的正文！' }),
    (e) => e.code === 'IDEMPOTENCY_CONFLICT'
  );
  // 同号但指向另一标的也属正文不同 -> 409
  const id2 = publish(k, { meterId: 'MP-2', targetKw: 15, start: START + 40 * I, end: START + 48 * I });
  sealedEvent(k, { id: id2, device: 'DEV-3', agg: 'AGG-B', kw: 15 });
  assert.throws(
    () => fileObj(k, { id: id2, agg: 'AGG-B', req: 'REQ-DUP' }),
    (e) => e.code === 'IDEMPOTENCY_CONFLICT'
  );
});

test('同一代理商对同一标的只允许一个待处理实例（结案后可再次提起）', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  const a = fileObj(k, { id, agg: 'AGG-A', req: 'REQ-A1' });
  assert.throws(
    () => fileObj(k, { id, agg: 'AGG-A', req: 'REQ-A2' }),
    (e) => e.code === 'DUPLICATE_PENDING_OBJECTION'
  );
  // 不同代理商对同一标的可以各自提起
  const b = fileObj(k, { id, agg: 'AGG-B', req: 'REQ-B1' });
  assert.equal(b.filed, true);
  // 驳回 A 后，A 可以再次提起（同一标的第二实例）
  k.dispatch({ type: 'REVIEW_OBJECTION', objectionId: a.objection.id, decision: 'REJECTED', rationale: '证据不足' });
  const a2 = fileObj(k, { id, agg: 'AGG-A', req: 'REQ-A3', reason: '新证据' });
  assert.equal(a2.filed, true);
  assert.notEqual(a2.objection.id, a.objection.id);
});

test('补充材料只能顺序追加；重复请求幂等；不同正文 409；非所有者被拒', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  const f = fileObj(k, { id, req: 'REQ-F' });
  const oid = f.objection.id;

  const m1 = k.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-M1', objectionId: oid, aggregatorId: 'AGG-A', summary: '证书1' });
  assert.equal(m1.materialSeq, 1);
  const m2 = k.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-M2', objectionId: oid, aggregatorId: 'AGG-A', summary: '证书2' });
  assert.equal(m2.materialSeq, 2);
  // 次序：重放视图严格 1,2
  assert.deepEqual(k.objectionView(oid).materials.map((x) => x.seq), [1, 2]);
  // 同请求号同正文 -> 原结果
  const m1again = k.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-M1', objectionId: oid, aggregatorId: 'AGG-A', summary: '证书1' });
  assert.equal(m1again.idempotent, true);
  assert.equal(m1again.materialSeq, 1);
  assert.equal(k.objectionView(oid).materialCount, 2, '幂等不追加材料');
  // 同请求号不同正文 -> 409
  assert.throws(
    () => k.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-M1', objectionId: oid, aggregatorId: 'AGG-A', summary: '篡改正文' }),
    (e) => e.code === 'IDEMPOTENCY_CONFLICT'
  );
  // 非提起方代理商不能补充
  assert.throws(
    () => k.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-MX', objectionId: oid, aggregatorId: 'AGG-B', summary: '他方材料' }),
    (e) => e.code === 'NOT_OBJECTION_OWNER'
  );
});

test('审核通过/驳回即终态：不可再次裁定，也不能再补充材料', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  const oid = fileObj(k, { id, req: 'REQ-R' }).objection.id;
  k.dispatch({ type: 'REVIEW_OBJECTION', objectionId: oid, decision: 'APPROVED', rationale: '基线确实受缺失日影响' });

  assert.throws(
    () => k.dispatch({ type: 'REVIEW_OBJECTION', objectionId: oid, decision: 'REJECTED', rationale: '改判' }),
    (e) => e.code === 'OBJECTION_ALREADY_DECIDED'
  );
  assert.throws(
    () => k.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-M', objectionId: oid, aggregatorId: 'AGG-A', summary: '结案后补料' }),
    (e) => e.code === 'OBJECTION_ALREADY_DECIDED'
  );
  const o = k.objectionView(oid);
  assert.equal(o.status, 'APPROVED');
  assert.equal(o.review.decision, 'APPROVED');
});

test('异议绝不修改被质疑的原始内容（v1 指纹字节级不变）', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  const v1Before = k.s.reports.get(id)[0];
  const hashBefore = v1Before.hash;
  const frozenV1 = JSON.stringify(v1Before);
  const oid = fileObj(k, { id, req: 'REQ-I' }).objection.id;
  k.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-IM', objectionId: oid, aggregatorId: 'AGG-A', summary: '补料' });
  k.dispatch({ type: 'REVIEW_OBJECTION', objectionId: oid, decision: 'APPROVED', rationale: '成立' });
  const v1After = k.s.reports.get(id)[0];
  assert.equal(v1After.hash, hashBefore);
  assert.equal(JSON.stringify(v1After), frozenV1);
});

test('后补计量数据生成新裁定：旧异议继续指向原标的不转移，新标的可单独受理', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  const v1 = k.s.reports.get(id)[0];
  const old = fileObj(k, { id, version: 1, req: 'REQ-OLD' }).objection;
  assert.equal(old.target, `${id}:v1`);

  // 迟到历史数据 -> 新裁定 v2
  const corr = k.dispatch({ type: 'LOAD_MISSING_DAY', meterId: 'MP-1', dayOffset: 4 });
  assert.ok(corr.reportsIssued.includes(`${id}:v2`));

  // 旧异议仍指向 v1：target 不变，指纹与 v1 一致（v1 字节未变），显示已有新版本
  const oldAgain = k.objectionView(old.id);
  assert.equal(oldAgain.target, `${id}:v1`);
  assert.equal(oldAgain.reportHash, v1.hash);
  assert.equal(oldAgain.targetIntact, true);

  // 旧异议待处理期间，同一代理商对 v1 不能重复，但对新标的 v2 可单独受理
  const onV2 = fileObj(k, { id, version: 2, req: 'REQ-NEW' });
  assert.equal(onV2.filed, true);
  assert.equal(onV2.objection.target, `${id}:v2`);
  assert.notEqual(onV2.objection.id, old.id);
});

test('审核旧异议不影响对新标的的独立异议', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  const old = fileObj(k, { id, version: 1, req: 'REQ-O' }).objection;
  k.dispatch({ type: 'LOAD_MISSING_DAY', meterId: 'MP-1', dayOffset: 4 });
  const neu = fileObj(k, { id, version: 2, req: 'REQ-N' }).objection;
  k.dispatch({ type: 'REVIEW_OBJECTION', objectionId: old.id, decision: 'REJECTED', rationale: 'v1 按当时水位无误' });
  assert.equal(k.objectionView(old.id).status, 'REJECTED');
  assert.equal(k.objectionView(neu.id).status, 'PENDING', '新标的异议独立存活、可继续审核');
  k.dispatch({ type: 'REVIEW_OBJECTION', objectionId: neu.id, decision: 'APPROVED', rationale: 'v2 采信迟到证据' });
  assert.equal(k.objectionView(neu.id).status, 'APPROVED');
});

test('重启恢复：待处理状态、时限与材料次序完全不变', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'dr-obj-'));
  try {
    const file = join(dir, 'events.log');
    const st1 = new EventStore(file);
    const k1 = new Kernel(st1);
    const id = publish(k1);
    sealedEvent(k1, { id });
    const f1 = fileObj(k1, { id, req: 'REQ-PERSIST-1' });
    const oid = f1.objection.id;
    const deadline = f1.objection.deadlineMs;
    const f2 = fileObj(k1, { id, agg: 'AGG-B', req: 'REQ-PERSIST-2' });
    k1.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-PM-1', objectionId: oid, aggregatorId: 'AGG-A', summary: '第一份', content: 'AAA' });
    k1.dispatch({ type: 'APPEND_OBJECTION_MATERIAL', requestId: 'REQ-PM-2', objectionId: oid, aggregatorId: 'AGG-A', summary: '第二份', content: 'BBB' });
    k1.dispatch({ type: 'TICK', minutes: 60 }); // 在截止内推进 1h，状态仍待处理
    const tip = st1.tipHash;

    // 重启：新 store/内核重放
    const st2 = new EventStore(file);
    const k2 = new Kernel(st2, { boot: false });
    await st2.load();
    k2.replay();
    assert.equal(st2.verify().ok, true);
    assert.equal(st2.tipHash, tip);
    const o = k2.objectionView(oid);
    assert.equal(o.status, 'PENDING');
    assert.equal(o.deadlineMs, deadline, '时限不变');
    assert.deepEqual(o.materials.map((m) => m.seq), [1, 2], '材料次序不变');
    assert.deepEqual(o.materials.map((m) => m.summary), ['第一份', '第二份']);
    const o2 = k2.objectionView(f2.objection.id);
    assert.equal(o2.status, 'PENDING');
    // 重启后请求号索引同样恢复：同号同正文仍幂等、不同正文仍 409
    const again = fileObj(k2, { id, req: 'REQ-PERSIST-1' });
    assert.equal(again.idempotent, true);
    assert.equal(again.objection.id, oid);
    assert.throws(
      () => fileObj(k2, { id, req: 'REQ-PERSIST-1', reason: '重启后改正文' }),
      (e) => e.code === 'IDEMPOTENCY_CONFLICT'
    );
    // 重启后可正常审核
    k2.dispatch({ type: 'REVIEW_OBJECTION', objectionId: oid, decision: 'APPROVED', rationale: '重启后审核' });
    assert.equal(k2.objectionView(oid).status, 'APPROVED');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('事件视图/状态视图暴露异议列表', () => {
  const k = newKernel();
  const id = publish(k);
  sealedEvent(k, { id });
  fileObj(k, { id, req: 'REQ-V' });
  const ev = k.eventView(id);
  assert.equal(ev.objections.length, 1);
  assert.equal(ev.objections[0].target, `${id}:v1`);
  assert.equal(ev.reports[0].objectionDeadlineMs, ev.reports[0].issuedAt + 24 * 3600000);
  const state = k.stateView();
  assert.equal(state.objections.length, 1);
  assert.equal(state.objectionWindowMs, 24 * 3600000);
});
