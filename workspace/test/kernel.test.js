import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Kernel } from '../src/kernel.js';
import { BASE_NOW } from '../src/seed.js';
import { EventStore } from '../src/store.js';
import { sha256Hex, canonicalJSON } from '../src/util.js';

const I = 15 * 60 * 1000;
// 演示用事件窗口：发布日次日凌晨
const START = Date.UTC(2026, 8, 16, 2, 0, 0);
const END = START + 16 * I;

function newKernel() {
  return Kernel.freshMemory(BASE_NOW);
}

function publish(k, { meterId = 'MP-1', targetKw = 40, start = START, end = END, algorithm = 'MID_8_OF_10' } = {}) {
  return k.dispatch({ type: 'PUBLISH_EVENT', meterId, targetKw, startMs: start, endMs: end, algorithm }).event.id;
}

function runEvent(k, id, { tickEnd = true, device = 'DEV-1', kw = 40, agg = 'AGG-A' } = {}) {
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: device, aggregatorId: agg });
  k.dispatch({ type: 'TICK', minutes: (k.s.events.get(id).startMs - k.now()) / 60000 });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: device, status: 'ACCEPTED', requestedKw: kw });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: device, status: 'EXECUTING', requestedKw: kw });
  if (tickEnd) k.dispatch({ type: 'TICK', minutes: (k.s.events.get(id).endMs - k.now()) / 60000 });
}

test('发布事件并在开始时冻结基线（候选日全部来自事件日之前）', () => {
  const k = newKernel();
  const id = publish(k);
  let ev = k.eventView(id);
  assert.equal(ev.status, 'SCHEDULED');
  assert.equal(ev.baselineVersions.length, 0, '冻结前没有基线');

  k.dispatch({ type: 'TICK', minutes: (START - k.now()) / 60000 });
  ev = k.eventView(id);
  assert.equal(ev.status, 'ACTIVE');
  assert.equal(ev.baselineVersions.length, 1);
  const days = ev.baselineVersions[0].days;
  assert.ok(days.length >= 5);
  assert.ok(days.every((d) => d < '2026-09-16'), '基线只用事件前的日子');
});

test('基线冻结后不可改写：迟到历史日只产生 v2', () => {
  const k = newKernel();
  const id = publish(k);
  runEvent(k, id);
  const v1 = k.s.reports.get(id)[0];
  const frozenV1 = JSON.stringify(k.s.baselines.get(id)[0]);

  const out = k.dispatch({ type: 'LOAD_MISSING_DAY', meterId: 'MP-1', dayOffset: 4 });
  assert.ok(out.baselineCorrected.length >= 1);
  const bv = out.baselineCorrected[0];
  assert.equal(bv.version, 2);

  // v1 基线对象字节级不变
  assert.equal(JSON.stringify(k.s.baselines.get(id)[0]), frozenV1);
  // v1 报告对象不变，v2 为 CORRECTED 且锚定基线 v2
  const reports = k.s.reports.get(id);
  assert.equal(reports[0], v1);
  assert.equal(reports[0].kind, 'ORIGINAL');
  assert.equal(reports[1].kind, 'CORRECTED');
  assert.equal(reports[1].baselineVersion, 2);
  assert.equal(reports[0].baselineVersion, 1);
});

test('同一设备不能归属两个有效事件；首个事件关闭后可被重新声明', () => {
  const k = newKernel();
  const e1 = publish(k, { meterId: 'MP-1' });
  const start2 = START + 20 * I;
  const e2 = publish(k, { meterId: 'MP-1', start: start2, end: start2 + 8 * I });

  k.dispatch({ type: 'CLAIM_DEVICE', eventId: e1, deviceId: 'DEV-1', aggregatorId: 'AGG-A' });
  assert.throws(
    () => k.dispatch({ type: 'CLAIM_DEVICE', eventId: e2, deviceId: 'DEV-1', aggregatorId: 'AGG-A' }),
    (e) => e.code === 'DEVICE_BUSY'
  );
  // 同事件换聚合商重复声明 -> DEVICE_ALREADY_CLAIMED
  assert.throws(
    () => k.dispatch({ type: 'CLAIM_DEVICE', eventId: e1, deviceId: 'DEV-1', aggregatorId: 'AGG-B' }),
    (e) => e.code === 'DEVICE_ALREADY_CLAIMED'
  );
  // 幂等
  const again = k.dispatch({ type: 'CLAIM_DEVICE', eventId: e1, deviceId: 'DEV-1', aggregatorId: 'AGG-A' });
  assert.equal(again.idempotent, true);

  runEvent(k, e1);
  // e1 结束释放后，可声明到 e2
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: e2, deviceId: 'DEV-1', aggregatorId: 'AGG-B' });
  assert.equal(k.s.claims.get('DEV-1').eventId, e2);
});

test('撤销与执行并发：先撤销则执行被拒；RECOVERED 作为迟到终态仍被记录', () => {
  const k = newKernel();
  const id = publish(k);
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: 'DEV-1', aggregatorId: 'AGG-A' });
  k.dispatch({ type: 'TICK', minutes: (START - k.now()) / 60000 + 30 }); // 进入时窗 30 分钟
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'EXECUTING', requestedKw: 40 });

  const cancel = k.dispatch({ type: 'CANCEL_EVENT', eventId: id, reason: '紧急' });
  assert.deepEqual(cancel.released, ['DEV-1']);
  assert.equal(k.s.claims.get('DEV-1'), undefined);

  assert.throws(
    () => k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'EXECUTING', requestedKw: 40 }),
    (e) => e.code === 'EVENT_RECALLED'
  );
  // 迟到 RECOVERED 不改变归属、不改写裁决，但必须留下明确水位
  const late = k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'RECOVERED' });
  assert.equal(late.accepted, true);
  assert.equal(k.s.claims.get('DEV-1'), undefined, '撤销后设备保持空闲');
  assert.equal(k.s.participants.get(id).get('DEV-1').status, 'RECOVERED');
});

test('回执乱序：高序号先到被缓冲，缺口补齐后自动排水，低序号被拒', () => {
  const k = newKernel();
  const id = publish(k);
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: 'DEV-1', aggregatorId: 'AGG-A' });
  k.dispatch({ type: 'TICK', minutes: (START - k.now()) / 60000 });

  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'ACCEPTED', requestedKw: 40, clientSeq: 1 });
  const jump = k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'EXECUTING', requestedKw: 40, clientSeq: 3 });
  assert.equal(jump.buffered, true);
  assert.equal(jump.expectSeq, 2);
  assert.equal(k.s.participants.get(id).get('DEV-1').status, 'ACCEPTED', '缓冲不改变已裁决状态');

  // seq 1 重放 -> STALE_SEQ
  assert.throws(
    () => k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'ACCEPTED', clientSeq: 1 }),
    (e) => e.code === 'STALE_SEQ'
  );

  const fill = k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'EXECUTING', requestedKw: 40, clientSeq: 2 });
  assert.equal(fill.drained, 1);
  assert.equal(k.s.participants.get(id).get('DEV-1').status, 'EXECUTING');
  assert.equal(k.s.participants.get(id).get('DEV-1').appliedSeq, 3, '水位明确推进到 3');
});

test('状态非法迁移被拒（执行后不能回退、未执行不能 RECOVERED）', () => {
  const k = newKernel();
  const id = publish(k);
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: 'DEV-1', aggregatorId: 'AGG-A' });
  k.dispatch({ type: 'TICK', minutes: (START - k.now()) / 60000 });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'EXECUTING', requestedKw: 40 });
  assert.throws(
    () => k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'ACCEPTED' }),
    (e) => e.code === 'PHASE_CONFLICT'
  );
  const k2 = newKernel();
  const id2 = publish(k2);
  k2.dispatch({ type: 'CLAIM_DEVICE', eventId: id2, deviceId: 'DEV-2', aggregatorId: 'AGG-B' });
  k2.dispatch({ type: 'TICK', minutes: (START - k2.now()) / 60000 });
  assert.throws(
    () => k2.dispatch({ type: 'DEVICE_RECEIPT', eventId: id2, deviceId: 'DEV-2', status: 'RECOVERED' }),
    (e) => e.code === 'PHASE_CONFLICT'
  );
});

test('同计量点有效事件时窗重叠被拒绝', () => {
  const k = newKernel();
  publish(k, { start: START, end: END });
  assert.throws(
    () => publish(k, { start: START + 2 * I, end: START + 10 * I }),
    (e) => e.code === 'OVERLAPPING_WINDOW'
  );
  // 不同计量点不冲突
  publish(k, { meterId: 'MP-2', targetKw: 20, start: START + 2 * I, end: START + 10 * I });
});

test('设备不属于事件计量点时声明被拒', () => {
  const k = newKernel();
  const id = publish(k, { meterId: 'MP-1' });
  assert.throws(
    () => k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: 'DEV-3', aggregatorId: 'AGG-A' }),
    (e) => e.code === 'METER_MISMATCH'
  );
});

test('事件结束出具 v1 报告：执行设备产生正削减，目标与裁决量化', () => {
  const k = newKernel();
  const id = publish(k, { targetKw: 40 });
  runEvent(k, id, { device: 'DEV-1', kw: 40 });
  const ev = k.eventView(id);
  assert.equal(ev.status, 'CLOSED');
  const r = k.s.reports.get(id)[0];
  assert.equal(r.kind, 'ORIGINAL');
  assert.ok(r.reductionKwh > 0, '存在削减');
  assert.equal(r.gapCount, 0);
  assert.ok(['MET', 'PARTIAL'].includes(r.verdict));
  const devRow = r.devices.find((d) => d.deviceId === 'DEV-1');
  assert.equal(devRow.compliance, 'PERFORMED');
});

test('通信中断造成 DATA_GAP，补传后只新增订正版（v1 保留）', () => {
  const k = newKernel();
  const id = publish(k, { meterId: 'MP-2', targetKw: 20, end: START + 8 * I });
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: 'DEV-3', aggregatorId: 'AGG-B' });
  k.dispatch({ type: 'SET_METER_OFFLINE', meterId: 'MP-2', offline: true });
  k.dispatch({ type: 'TICK', minutes: (START - k.now()) / 60000 });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-3', status: 'EXECUTING', requestedKw: 20 });
  k.dispatch({ type: 'TICK', minutes: 120 }); // 8 个区间无读数
  k.dispatch({ type: 'SET_METER_OFFLINE', meterId: 'MP-2', offline: false });
  k.dispatch({ type: 'TICK', minutes: (START + 8 * I - k.now()) / 60000 });

  const v1 = k.s.reports.get(id)[0];
  assert.equal(v1.gapCount, 8);
  assert.equal(v1.verdict, 'INCONCLUSIVE');

  const bf = k.dispatch({ type: 'BACKFILL_ACTUALS', eventId: id });
  const reports = k.s.reports.get(id);
  assert.equal(reports[0], v1);
  assert.equal(reports.length, 2);
  assert.equal(reports[1].kind, 'CORRECTED');
  assert.equal(reports[1].gapCount, 0);
  assert.ok(bf.reportsIssued.length === 1);
});

test('重复写入同一区间读数被拒绝（不可变）', () => {
  const k = newKernel();
  const id = publish(k, { meterId: 'MP-2', targetKw: 20 });
  runEvent(k, id, { device: 'DEV-3', kw: 20 });
  // 事件窗口第一个区间已有 SIM 读数
  assert.throws(
    () => k.dispatch({
      type: 'INGEST_READINGS',
      items: [{ meterId: 'MP-2', ts: START, value: 999 }],
    }),
    (e) => e.code === 'CONFLICTING_READING'
  );
});

test('哈希链：重放投影一致；篡改任一历史记录即校验失败', () => {
  const k = newKernel();
  const id = publish(k);
  runEvent(k, id);
  const v = k.store.verify();
  assert.equal(v.ok, true);

  // 用全新内核重放同一内存记录（通过伪造文件做不到内存；直接校验重放幂等）
  const tipBefore = k.store.tipHash;
  k.replay();
  assert.equal(k.store.tipHash, tipBefore);
  assert.equal(k.eventView(id).status, 'CLOSED');

  // 篡改一条历史记录
  const recs = k.store.all();
  const target = recs.find((r) => r.type === 'EVENT_PUBLISHED');
  const original = JSON.stringify(target.payload);
  target.payload.targetKw = 9999;
  const bad = k.store.verify();
  assert.equal(bad.ok, false);
  assert.equal(bad.brokenAt, target.seq);
  target.payload = JSON.parse(original);
  assert.equal(k.store.verify().ok, true);
});

test('报告内容指纹可独立重算（证明链上锚定的是裁决内容）', () => {
  const k = newKernel();
  const id = publish(k);
  runEvent(k, id);
  k.dispatch({ type: 'LOAD_MISSING_DAY', meterId: 'MP-1', dayOffset: 4 });
  for (const r of k.s.reports.get(id)) {
    const { hash, bodyHash, issuedAt, ...body } = r;
    const recomputed = sha256Hex(canonicalJSON(body));
    assert.equal(recomputed, hash);
    assert.equal(recomputed, bodyHash);
  }
});

test('重启恢复：文件日志重新加载后状态与哈希链一致', async () => {
  const os = await import('node:os');
  const path = await import('node:path');
  const fsp = await import('node:fs/promises');
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'dr-'));
  const file = path.join(dir, 'events.log');

  const st1 = new EventStore(file);
  const k1 = new Kernel(st1);
  const id = publish(k1);
  runEvent(k1, id);
  const tip = st1.tipHash;
  const reportCount = k1.s.reports.get(id).length;

  const st2 = new EventStore(file);
  await st2.load();
  const k2 = new Kernel(st2, { boot: false });
  k2.replay();
  assert.equal(st2.verify().ok, true);
  assert.equal(st2.tipHash, tip);
  assert.equal(k2.eventView(id).status, 'CLOSED');
  assert.equal(k2.s.reports.get(id).length, reportCount);

  await fsp.rm(dir, { recursive: true, force: true });
});
