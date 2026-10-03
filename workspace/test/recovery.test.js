// 恢复契约测试：正常重启 / 断电残尾 / 尾部哈希错 / 中间篡改 / 隔离读写边界 / 恢复后再重启与连续追加。
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Kernel } from '../src/kernel.js';
import { BASE_NOW } from '../src/seed.js';
import { EventStore, LogQuarantineError } from '../src/store.js';

const I = 15 * 60 * 1000;
const START = Date.UTC(2026, 8, 16, 2, 0, 0);
const END = START + 16 * I;
const SERVER = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'server.js');

let tmpSeq = 0;
async function tmpLog() {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), `dr-rec-${tmpSeq++}-`));
  after(async () => fsp.rm(dir, { recursive: true, force: true }));
  return { dir, file: path.join(dir, 'events.log') };
}

async function buildLog(file) {
  const st = new EventStore(file);
  const k = new Kernel(st);
  const id = k.dispatch({
    type: 'PUBLISH_EVENT', meterId: 'MP-1', targetKw: 40,
    startMs: START, endMs: END, algorithm: 'MID_8_OF_10',
  }).event.id;
  k.dispatch({ type: 'CLAIM_DEVICE', eventId: id, deviceId: 'DEV-1', aggregatorId: 'AGG-A' });
  k.dispatch({ type: 'TICK', minutes: (START - k.now()) / 60000 });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'ACCEPTED', requestedKw: 40 });
  k.dispatch({ type: 'DEVICE_RECEIPT', eventId: id, deviceId: 'DEV-1', status: 'EXECUTING', requestedKw: 40 });
  k.dispatch({ type: 'TICK', minutes: (END - k.now()) / 60000 });
  return { store: st, kernel: k, eventId: id };
}

function readLines(file) {
  return fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
}

async function reload(file) {
  const st = new EventStore(file);
  const v = await st.load();
  const k = new Kernel(st, { boot: false });
  k.replay();
  return { store: st, kernel: k, verify: v };
}

test('正常重启：完整日志重新加载，链校验通过、可继续追加', async () => {
  const { file } = await tmpLog();
  const { store: s1, eventId } = await buildLog(file);
  const tip = s1.tipHash;

  const { store: s2, kernel: k2, verify } = await reload(file);
  assert.equal(verify.ok, true);
  assert.equal(verify.mode, 'OK');
  assert.equal(verify.writable, true);
  assert.equal(s2.tipHash, tip);
  assert.equal(k2.eventView(eventId).status, 'CLOSED');

  // 重启后可继续写入，且新记录延续原链
  const rec = s2.append('SIM_TICK', { from: 1, to: 2, minutes: 1 }, 2);
  assert.equal(rec.seq, s2.all().length);
  assert.equal(rec.prevHash, tip);
  assert.equal(s2.verify().ok, true);
});

test('尾部不完整记录（无换行残尾）：归档审计副本、截断、从最后完好记录续链', async () => {
  const { file, dir } = await tmpLog();
  const { store: s1 } = await buildLog(file);
  const goodCount = s1.all().length;

  // 模拟断电：只写入了半条 JSON（含多字节 UTF-8 被切断），且没有结尾换行
  const partial = JSON.stringify({ seq: goodCount + 1, ts: BASE_NOW, type: 'READINGS_BATCH', payload: { note: '残尾☕☕' } });
  fs.appendFileSync(file, partial.slice(0, 60));

  const { store: s2, verify } = await reload(file);
  assert.equal(verify.ok, true, '残尾恢复后链必须完好');
  assert.equal(verify.writable, true);
  assert.equal(verify.recovery.kind, 'torn-tail-recovered');
  assert.equal(s2.all().length, goodCount, '内存只含验证通过的记录');
  assert.equal(s2.seq, goodCount);

  // 主日志已截断，末尾恰好是最后一条完好记录
  const onDisk = readLines(file);
  assert.equal(onDisk.length, goodCount);
  assert.equal(onDisk.at(-1).hash, s1.all().at(-1).hash);

  // 审计副本：残尾字节 + meta（含 sha256）
  const qdir = path.join(dir, 'quarantine');
  const archived = fs.readdirSync(qdir).filter((f) => f.endsWith('.log'));
  const metas = fs.readdirSync(qdir).filter((f) => f.endsWith('.meta.json'));
  assert.equal(archived.length, 1);
  assert.equal(metas.length, 1);
  const archiveBytes = fs.readFileSync(path.join(qdir, archived[0]));
  assert.equal(archiveBytes.toString('utf8'), partial.slice(0, 60), '残尾必须逐字节原样保留');
  const meta = JSON.parse(fs.readFileSync(path.join(qdir, metas[0]), 'utf8'));
  assert.equal(meta.kind, 'torn-tail');
  assert.equal(meta.tailBytes, Buffer.byteLength(partial.slice(0, 60)));
  assert.equal(meta.lastGoodSeq, goodCount);
  assert.equal(meta.tailSha256, createHash('sha256').update(archiveBytes).digest('hex'));

  // 继续追加：序号与哈希链都从最后完好记录延续
  const rec = s2.append('SIM_TICK', { from: 1, to: 2, minutes: 1 }, 2);
  assert.equal(rec.seq, goodCount + 1);
  assert.equal(rec.prevHash, s1.tipHash);
  assert.equal(s2.verify().ok, true);
});

test('尾部残尾（完整换行之后另起的半截 JSON）也按残尾处理', async () => {
  const { file } = await tmpLog();
  const { store: s1 } = await buildLog(file);
  const goodCount = s1.all().length;
  const partial = JSON.stringify({ seq: goodCount + 1, type: 'READINGS_BATCH', payload: { items: [{ ts: 1, value: 2 }] } });
  assert.ok(partial.length > 50);
  fs.appendFileSync(file, partial.slice(0, 50)); // 无换行残尾
  const { store: s2, verify } = await reload(file);
  assert.equal(verify.ok, true);
  assert.equal(verify.recovery.kind, 'torn-tail-recovered');
  assert.equal(s2.all().length, goodCount);
});

test('尾部完整但哈希错误：进入只读隔离，首个损坏位置为尾部 seq，拒绝追加', async () => {
  const { file } = await tmpLog();
  const { store: s1 } = await buildLog(file);
  const goodCount = s1.all().length;

  // 在尾部追加一条结构完整的记录，再改写其内容使 hash 对不上
  const tailRec = s1.append('SIM_TICK', { from: 1, to: 2, minutes: 1 }, 2);
  const raw = fs.readFileSync(file, 'utf8').trimEnd().split('\n');
  const tampered = JSON.parse(raw.at(-1));
  tampered.payload.minutes = 999;
  raw[raw.length - 1] = JSON.stringify(tampered);
  fs.writeFileSync(file, raw.join('\n') + '\n');

  const { store: s2, verify } = await reload(file);
  assert.equal(verify.ok, false);
  assert.equal(verify.mode, 'QUARANTINED');
  assert.equal(verify.writable, false);
  assert.equal(verify.brokenAt, tailRec.seq);
  assert.match(verify.reason, /哈希不符/);
  assert.equal(s2.all().length, goodCount, '损坏记录不进入内存');

  const fileBefore = fs.readFileSync(file);
  assert.throws(() => s2.append('SIM_TICK', {}, 3), LogQuarantineError);
  assert.deepEqual(fs.readFileSync(file), fileBefore, '隔离期间日志文件不得被追加或改动');
});

test('中间记录被篡改：定位首个损坏位置，其后完整记录也不重放，服务仍可只读', async () => {
  const { file } = await tmpLog();
  const { store: s1, eventId } = await buildLog(file);
  const raw = fs.readFileSync(file, 'utf8').trimEnd().split('\n');
  const idx = raw.findIndex((l) => JSON.parse(l).type === 'EVENT_PUBLISHED');
  const target = JSON.parse(raw[idx]);
  const brokenSeq = target.seq;
  target.payload.targetKw = 9999;
  raw[idx] = JSON.stringify(target);
  fs.writeFileSync(file, raw.join('\n') + '\n');

  const { store: s2, kernel: k2, verify } = await reload(file);
  assert.equal(verify.mode, 'QUARANTINED');
  assert.equal(verify.ok, false);
  assert.equal(verify.brokenAt, brokenSeq, '必须报告首个损坏位置');
  assert.equal(verify.lineNo, brokenSeq);
  assert.match(verify.reason, /哈希不符/);
  assert.equal(s2.all().length, brokenSeq - 1, '损坏位置起停止重放');
  assert.equal(s2.tipHash, s1.all()[brokenSeq - 2].hash);

  // 读接口可用（隔离前缀的投影），写接口全部拒绝
  assert.ok(k2.stateView().readOnly === true);
  assert.equal(k2.stateView().mode, 'QUARANTINED');
  assert.equal(k2.s.events.get(eventId), undefined, '被篡改的发布记录不参与投影');
  for (const cmd of [
    { type: 'TICK', minutes: 15 },
    { type: 'PUBLISH_EVENT', meterId: 'MP-1', targetKw: 1, startMs: START + 40 * I, endMs: START + 44 * I },
    { type: 'RESET' },
  ]) {
    assert.throws(() => k2.dispatch(cmd), (e) => e.code === 'LOG_QUARANTINED' && e.status === 503);
  }
});

test('中间记录无法解析为 JSON：首个坏行即隔离（不当作残尾）', async () => {
  const { file } = await tmpLog();
  await buildLog(file);
  const raw = fs.readFileSync(file, 'utf8').split('\n').filter(Boolean);
  raw.splice(3, 0, '{这不是JSON');
  fs.writeFileSync(file, raw.join('\n') + '\n');

  const { store: s2, verify } = await reload(file);
  assert.equal(verify.mode, 'QUARANTINED');
  assert.equal(verify.brokenAt, 4);
  assert.equal(verify.lineNo, 4);
  assert.match(verify.reason, /无法解析/);
  assert.equal(s2.all().length, 3);
});

test('序号不连续：进入隔离并报告首个异常序号位置', async () => {
  const { file } = await tmpLog();
  await buildLog(file);
  const raw = fs.readFileSync(file, 'utf8').trimEnd().split('\n');
  const rec = JSON.parse(raw[5]);
  rec.seq = rec.seq + 1;
  rec.hash = EventStore.hashOf(rec);
  raw[5] = JSON.stringify(rec);
  fs.writeFileSync(file, raw.join('\n') + '\n');

  const { verify } = await reload(file);
  assert.equal(verify.mode, 'QUARANTINED');
  assert.equal(verify.brokenAt, 6);
  assert.match(verify.reason, /序号异常/);
});

test('prevHash 断链：进入隔离并报告首个断裂位置', async () => {
  const { file } = await tmpLog();
  await buildLog(file);
  const raw = fs.readFileSync(file, 'utf8').trimEnd().split('\n');
  const rec = JSON.parse(raw[5]);
  rec.prevHash = 'f'.repeat(64);
  rec.hash = EventStore.hashOf(rec);
  raw[5] = JSON.stringify(rec);
  fs.writeFileSync(file, raw.join('\n') + '\n');

  const { verify } = await reload(file);
  assert.equal(verify.mode, 'QUARANTINED');
  assert.equal(verify.brokenAt, 6);
  assert.match(verify.reason, /prevHash/);
});

test('残尾之后仍有内容（多条坏行）：不允许残尾恢复，直接隔离', async () => {
  const { file } = await tmpLog();
  await buildLog(file);
  fs.appendFileSync(file, '{半截记录\n');
  fs.appendFileSync(file, '{另一条半截\n');
  const { store: s2, verify } = await reload(file);
  assert.equal(verify.mode, 'QUARANTINED');
  assert.match(verify.reason, /无法解析|残尾之后/);
  assert.equal(verify.recovery.kind, 'corrupt');
  assert.ok(!fs.existsSync(path.join(path.dirname(file), 'quarantine')), '隔离不得产生残尾归档');
  assert.ok(s2.append === s2.append);
  assert.throws(() => s2.append('SIM_TICK', {}, 1), LogQuarantineError);
});

test('末行自带换行却无法解析：物理行完整，按损坏隔离而非残尾恢复', async () => {
  const { file } = await tmpLog();
  await buildLog(file);
  fs.appendFileSync(file, '{这行有换行结尾但不是JSON\n');
  const { store: s2, verify } = await reload(file);
  assert.equal(verify.mode, 'QUARANTINED');
  assert.match(verify.reason, /物理行完整/);
  assert.throws(() => s2.append('SIM_TICK', {}, 1), LogQuarantineError);
  assert.ok(!fs.existsSync(path.join(path.dirname(file), 'quarantine')));
});

test('恢复后再次重启：残尾只处理一次，二次启动为正常重启并可连续追加', async () => {
  const { file } = await tmpLog();
  const { store: s1 } = await buildLog(file);
  const goodCount = s1.all().length;
  const partial = JSON.stringify({ seq: goodCount + 1, type: 'X', payload: { x: 1 } });
  fs.appendFileSync(file, partial.slice(0, 33));

  const r1 = await reload(file);
  assert.equal(r1.verify.recovery.kind, 'torn-tail-recovered');
  // 第一次恢复后由服务追加两条业务记录
  r1.store.append('SIM_TICK', { from: 1, to: 2, minutes: 1 }, 2);
  r1.store.append('SIM_TICK', { from: 2, to: 3, minutes: 1 }, 3);

  // 再次重启：没有新残尾、没有隔离，链完整
  const r2 = await reload(file);
  assert.equal(r2.verify.ok, true);
  assert.equal(r2.verify.writable, true);
  assert.equal(r2.verify.recovery, null);
  assert.equal(r2.store.all().length, goodCount + 2);
  assert.equal(r2.store.seq, goodCount + 2);

  // 连续追加若干条，链始终闭合
  let tip = r2.store.tipHash;
  for (let i = 0; i < 5; i++) {
    const rec = r2.store.append('SIM_TICK', { from: i, to: i + 1, minutes: 1 }, i + 4);
    assert.equal(rec.prevHash, tip);
    tip = rec.hash;
  }
  assert.equal(r2.store.verify().ok, true);
  assert.equal(r2.store.all().length, goodCount + 7);

  // 第三次重启仍然完好
  const r3 = await reload(file);
  assert.equal(r3.verify.ok, true);
  assert.equal(r3.store.tipHash, tip);
});

test('HTTP 边界：隔离时 GET 只读接口 200 且含首个损坏位置，所有业务写入 503', async () => {
  const { file, dir } = await tmpLog();
  await buildLog(file);
  // 制造中间篡改
  const raw = fs.readFileSync(file, 'utf8').trimEnd().split('\n');
  const idx = raw.findIndex((l) => JSON.parse(l).type === 'EVENT_PUBLISHED');
  const target = JSON.parse(raw[idx]);
  const brokenSeq = target.seq;
  target.payload.targetKw = 7777;
  raw[idx] = JSON.stringify(target);
  fs.writeFileSync(file, raw.join('\n') + '\n');

  const proc = spawn(process.execPath, [SERVER], {
    env: { ...process.env, DR_LOG: file, PORT: '0' },
  });
  let port = null;
  proc.stdout.on('data', (d) => {
    const m = String(d).match(/http:\/\/localhost:(\d+)/);
    if (m) port = Number(m[1]);
  });
  proc.stderr.on('data', () => {});
  try {
    for (let i = 0; i < 50 && port === null; i++) await sleep(100);
    assert.ok(port, '服务应在隔离模式下照常启动');
    const base = `http://localhost:${port}`;

    const stateRes = await fetch(base + '/api/state');
    assert.equal(stateRes.status, 200, '状态接口只读可用');
    const state = await stateRes.json();
    assert.equal(state.mode, 'QUARANTINED');
    assert.equal(state.readOnly, true);
    assert.equal(state.chain.brokenAt, brokenSeq);
    assert.match(state.chain.reason, /哈希不符/);

    const chainRes = await fetch(base + '/api/chain/verify');
    assert.equal(chainRes.status, 200);
    const chain = await chainRes.json();
    assert.equal(chain.ok, false);
    assert.equal(chain.brokenAt, brokenSeq);

    const logRes = await fetch(base + '/api/log?limit=5');
    assert.equal(logRes.status, 200, '裁决日志只读可用');

    // 静态页可打开
    const page = await fetch(base + '/');
    assert.equal(page.status, 200);

    // 各类业务写入一律 503，且响应带恢复信息
    for (const [p, body] of [
      ['/api/events', { meterId: 'MP-1', targetKw: 1, startInMinutes: 30, durationIntervals: 4 }],
      ['/api/sim/tick', { minutes: 15 }],
      ['/api/readings', { items: [] }],
      ['/api/claims', { eventId: 'X', deviceId: 'DEV-1', aggregatorId: 'AGG-A' }],
      ['/api/receipts', { eventId: 'X', deviceId: 'DEV-1', status: 'ACCEPTED' }],
      ['/api/sim/reset', {}],
    ]) {
      const res = await fetch(base + p, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      assert.equal(res.status, 503, `${p} 应返回 503`);
      const data = await res.json();
      assert.equal(data.code, 'LOG_QUARANTINED');
      assert.equal(data.recovery.brokenAt, brokenSeq);
    }

    // 文件未被服务改动
    assert.equal(fs.readFileSync(file, 'utf8'), raw.join('\n') + '\n');
    assert.ok(!fs.existsSync(path.join(dir, 'quarantine')));
  } finally {
    proc.kill('SIGKILL');
  }
});

test('HTTP 边界：残尾恢复后服务正常启动，写入 2xx，重启后连续追加', async () => {
  const { file } = await tmpLog();
  const { store: s1, eventId } = await buildLog(file);
  const goodCount = s1.all().length;
  const partial = JSON.stringify({ seq: goodCount + 1, type: 'X', payload: {} });
  fs.appendFileSync(file, partial.slice(0, 25));

  const proc = spawn(process.execPath, [SERVER], { env: { ...process.env, DR_LOG: file, PORT: '0' } });
  let port = null;
  proc.stdout.on('data', (d) => {
    const m = String(d).match(/http:\/\/localhost:(\d+)/);
    if (m) port = Number(m[1]);
  });
  let stderr = '';
  proc.stderr.on('data', (d) => { stderr += String(d); });
  try {
    for (let i = 0; i < 50 && port === null; i++) await sleep(100);
    assert.ok(port, '残尾恢复后服务应启动');
    assert.match(stderr, /断电残尾/);
    const base = `http://localhost:${port}`;
    const state = await (await fetch(base + '/api/state')).json();
    assert.equal(state.chain.ok, true);
    assert.equal(state.chain.recovery.kind, 'torn-tail-recovered');
    assert.equal(state.chain.length, goodCount);

    // 业务写入正常（推进时间），返回 2xx
    const tick = await fetch(base + '/api/sim/tick', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ minutes: 15 }),
    });
    assert.equal(tick.status, 200);
    void eventId;
  } finally {
    proc.kill('SIGKILL');
  }

  // 重启：恢复为常态；tick 会顺带追加 1 条 SIM_TICK 与 3 条 SIM 抄表记录
  const r = await reload(file);
  assert.equal(r.verify.ok, true);
  assert.equal(r.verify.recovery, null);
  assert.ok(r.store.all().length > goodCount);
  const firstNew = r.store.all()[goodCount];
  assert.equal(firstNew.type, 'SIM_TICK');
  assert.equal(firstNew.prevHash, s1.tipHash);
  assert.equal(r.store.verify().ok, true);
});
