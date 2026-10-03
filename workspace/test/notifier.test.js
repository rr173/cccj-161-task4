// 通知投递模块领域契约测试（node:test）：
// 不可变信封/哈希链、按订阅方保序、稳定幂等键、重复确认不重复推进游标、
// 密钥轮换旧信封可验、死信阻塞/重入队、磁盘重启恢复、篡改检出。
import { test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventStore } from '../src/store.js';
import { NotifierEngine, NOTIFY_TYPES, ITEM_STATUS } from '../notifier/engine.js';
import { hmacHex } from '../notifier/crypto.js';
import { canonicalJSON } from '../src/util.js';

function freshEngine(file, maxAttempts = 4) {
  const store = new EventStore(file);
  const eng = new NotifierEngine(store, { maxAttempts });
  return { store, eng, load: () => eng.load() };
}

async function ack(eng, subId, secret, envelopeSeq) {
  const key = eng.idempotencyKey(subId, envelopeSeq);
  const body = { subscriberId: subId, idempotencyKey: key };
  const signature = hmacHex(secret, canonicalJSON({ ...body }));
  return eng.confirm({ ...body, signature });
}

async function driveToAck(eng, subId, secret, seq) {
  eng.claimHead(subId);
  eng.lease(subId, seq, 5000);
  eng.recordSent(subId, seq, 200);
  return ack(eng, subId, secret, seq);
}

async function rejectsCode(fn, code) {
  try {
    await fn();
  } catch (err) {
    assert.equal(err.code, code, `期望错误码 ${code}，实际 ${err.code}：${err.message}`);
    return;
  }
  assert.fail(`期望抛出错误码 ${code}，但没有抛错`);
}

let dir;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), 'ntf-')); });
test.afterEach(() => rmSync(dir, { recursive: true, force: true }));

test('全部通知类型都能生成不可变信封并按主题入队', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'));
  await load();
  const c1 = eng.createSubscriber({ subscriberId: 'all', callbackUrl: 'http://127.0.0.1:9/x/callback' });
  eng.validationResolved('all', true, 'ok', 200);
  const c2 = eng.createSubscriber({ subscriberId: 'only-pub', callbackUrl: 'http://127.0.0.1:9/y/callback', topics: ['dr.event.published'] });
  eng.validationResolved('only-pub', true, 'ok', 200);
  // 待校验的订阅方不收通知
  eng.createSubscriber({ subscriberId: 'pending', callbackUrl: 'http://127.0.0.1:9/z/callback' });

  let seq = 0;
  for (const type of Object.values(NOTIFY_TYPES)) {
    const r = eng.ingestEvent({ type, eventId: 'EV', sourceSeq: ++seq });
    assert.equal(r.deduplicated, false);
    assert.equal(r.envelopeSeq, seq);
  }
  assert.equal(eng.subscriberView('all').queue.length, Object.keys(NOTIFY_TYPES).length);
  const onlyPub = eng.subscriberView('only-pub').queue;
  assert.equal(onlyPub.length, 1);
  assert.equal(onlyPub[0].envelopeSeq, 1);
  assert.equal(eng.subscriberView('pending').queue.length, 0);

  // 信封不可变：账本记录里的信封与签名证据锚定
  const rec = eng.store.all().find((r) => r.type === 'DR_NOTIFICATION' && r.payload.envelope.envelopeSeq === 1);
  const v = eng.verifyEnvelope(1);
  assert.equal(v.signatureValid, true);
  assert.equal(v.chainOk, true);
  assert.equal(v.ledgerHash, rec.hash);
});

test('sourceRef 幂等：重复接入不生成新信封、不重复入队', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'));
  await load();
  eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  eng.validationResolved('s', true, 'ok', 200);
  const first = eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceRef: 'orch:42' });
  const again = eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceRef: 'orch:42' });
  assert.equal(again.deduplicated, true);
  assert.equal(again.envelopeSeq, first.envelopeSeq);
  assert.equal(eng.subscriberView('s').queue.length, 1);
});

test('同一订阅方严格按 envelopeSeq 投递，队首未确认不发下一封', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'));
  await load();
  const c = eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  eng.validationResolved('s', true, 'ok', 200);
  for (let i = 1; i <= 3; i++) eng.ingestEvent({ type: NOTIFY_TYPES.STARTED, eventId: `E${i}`, sourceSeq: i });

  let head = eng.claimHead('s');
  assert.equal(head.envelopeSeq, 1);
  eng.lease('s', 1, 5000);
  eng.recordSent('s', 1, 200);
  // #1 在租约内：既不能并发投递 #1，也不能越过 #1 投递 #2
  assert.equal(eng.claimHead('s'), null);
  await rejectsCode(() => eng.lease('s', 2, 5000), 'NOT_HEAD');
  await rejectsCode(() => eng.lease('s', 1, 5000), 'LEASE_HELD');

  await ack(eng, 's', c.secret, 1);
  head = eng.claimHead('s');
  assert.equal(head.envelopeSeq, 2);
});

test('确认必须按序：未投递（PENDING）拒绝；重放乱序 ACK 也不推进游标', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'));
  await load();
  const c = eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  eng.validationResolved('s', true, 'ok', 200);
  eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceSeq: 1 });
  eng.ingestEvent({ type: NOTIFY_TYPES.ENDED, eventId: 'E', sourceSeq: 2 });

  // #2 未投递（PENDING）→ 拒绝，游标不动
  await rejectsCode(() => ack(eng, 's', c.secret, 2), 'NOT_DELIVERED');
  assert.equal(eng.subscriberView('s').cursor, 0);

  // 正常确认 #1 后游标为 1；直接向账本追加“跳过 #2 确认 #3”式的乱序 ACK 记录，
  // apply 的单调游标防护必须忽略它（任何重放/故障路径都不能重复或跳号推进游标）。
  eng.ingestEvent({ type: NOTIFY_TYPES.CANCELLED, eventId: 'E', sourceSeq: 3 });
  await driveToAck(eng, 's', c.secret, 1);
  assert.equal(eng.subscriberView('s').cursor, 1);
  // #2 仍是 PENDING，手工构造一条针对 #3 的 ACK 记录（模拟损坏/越序写入）
  const key3 = eng.idempotencyKey('s', 3);
  const rec = eng.store.append('DELIVERY_ACKED', { subscriberId: 's', envelopeSeq: 3, idempotencyKey: key3, at: new Date().toISOString(), note: 'tamper' });
  eng.acksByKey.set(key3, rec.payload);
  assert.equal(eng.subscriberView('s').cursor, 1, '乱序 ACK 不得推进游标');
});

test('重复确认幂等且绝不重复推进游标', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'));
  await load();
  const c = eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  eng.validationResolved('s', true, 'ok', 200);
  eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceSeq: 1 });
  const first = await driveToAck(eng, 's', c.secret, 1);
  assert.equal(first.cursor, 1);
  assert.equal(first.duplicate, false);

  // 连续重复确认：始终返回相同结果，游标保持 1
  for (let i = 0; i < 3; i++) {
    const dup = await ack(eng, 's', c.secret, 1);
    assert.equal(dup.duplicate, true);
    assert.equal(dup.cursor, 1);
  }
  assert.equal(eng.subscriberView('s').cursor, 1);
  // 账本上 DELIVERY_ACKED 只有一条（游标只推进一次）
  const acks = eng.store.all().filter((r) => r.type === 'DELIVERY_ACKED');
  assert.equal(acks.length, 1);
});

test('错误/缺失订阅方签名被拒', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'));
  await load();
  eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  eng.validationResolved('s', true, 'ok', 200);
  eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceSeq: 1 });
  eng.lease('s', 1, 5000);
  eng.recordSent('s', 1, 200);
  const key = eng.idempotencyKey('s', 1);
  await rejectsCode(() => eng.confirm({ subscriberId: 's', idempotencyKey: key }), 'MISSING_SIGNATURE');
  await rejectsCode(
    () => eng.confirm({ subscriberId: 's', idempotencyKey: key, signature: 'deadbeef' }),
    'BAD_SUBSCRIBER_SIGNATURE',
  );
  assert.equal(eng.subscriberView('s').cursor, 0);
});

test('超时后再次请求复用同一稳定幂等键', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'));
  await load();
  const c = eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  void c;
  eng.validationResolved('s', true, 'ok', 200);
  eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceSeq: 1 });

  const t0 = Date.now();
  eng.lease('s', 1, 1000, t0);
  assert.equal(eng.claimHead('s', t0 + 500), null); // 租约内
  const reclaimed = eng.claimHead('s', t0 + 1001); // 超时可再次认领
  assert.equal(reclaimed.envelopeSeq, 1);
  assert.equal(reclaimed.idempotencyKey, 'ack:s:1'); // 幂等键稳定不变
  eng.lease('s', 1, 1000, t0 + 1001);
  const item = eng.itemsByIdem.get('ack:s:1');
  assert.equal(item.attempts, 2);
  const reclaims = eng.store.all().filter((r) => r.type === 'DELIVERY_RECLAIMED');
  assert.equal(reclaims.length, 1);
});

test('失败达上限进死信、阻塞后续；人工重入队后可继续保序投递', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'), 2);
  await load();
  const c = eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  eng.validationResolved('s', true, 'ok', 200);
  eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceSeq: 1 });
  eng.ingestEvent({ type: NOTIFY_TYPES.STARTED, eventId: 'E', sourceSeq: 2 });

  // 两次投递失败 → #1 死信
  for (const attempt of [1, 2]) {
    eng.claimHead('s');
    eng.lease('s', 1, 5000);
    const it = eng.recordFailure('s', 1, `fail ${attempt}`, 503);
    if (attempt === 1) assert.equal(it.status, ITEM_STATUS.PENDING);
    else assert.equal(it.status, ITEM_STATUS.DEAD);
  }
  // 死信阻塞：队首为 DEAD，#2 不可投递
  assert.equal(eng.claimHead('s'), null);
  await rejectsCode(() => driveToAck(eng, 's', c.secret, 2), 'NOT_HEAD');
  // 对死信项确认被拒
  await rejectsCode(() => ack(eng, 's', c.secret, 1), 'ITEM_DEAD');

  // 人工重新入队：attempts 归零，位置不变
  eng.requeueDead('s', 1, '人工');
  await driveToAck(eng, 's', c.secret, 1);
  assert.equal(eng.subscriberView('s').cursor, 1);
  await driveToAck(eng, 's', c.secret, 2);
  assert.equal(eng.subscriberView('s').cursor, 2);
});

test('密钥轮换：新信封用新密钥；旧信封永远可用旧公钥复验', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'));
  await load();
  eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  eng.validationResolved('s', true, 'ok', 200);
  eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceSeq: 1 });
  const oldKeyId = eng.activeKeyId;

  eng.rotateKey('test');
  assert.notEqual(eng.activeKeyId, oldKeyId);
  eng.ingestEvent({ type: NOTIFY_TYPES.CANCELLED, eventId: 'E', sourceSeq: 2 });

  const v1 = eng.verifyEnvelope(1);
  const v2 = eng.verifyEnvelope(2);
  assert.equal(v1.signatureValid, true); // 旧在途/历史通知
  assert.equal(v1.keyActive, false);
  assert.equal(v1.keyId, oldKeyId);
  assert.equal(v2.signatureValid, true);
  assert.equal(v2.keyActive, true);
  assert.equal(v2.keyId, eng.activeKeyId);
  // 旧密钥仍在密钥列表（不删除）
  assert.ok(eng.keyOrder.includes(oldKeyId));
});

test('磁盘重启：重放账本恢复队列/游标，残留 INFLIGHT 复位且不重复推进游标', async () => {
  const file = join(dir, 'persist.log');
  const boot1 = freshEngine(file, 4);
  await boot1.load();
  const c = boot1.eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  boot1.eng.validationResolved('s', true, 'ok', 200);
  boot1.eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceSeq: 1 });
  boot1.eng.ingestEvent({ type: NOTIFY_TYPES.STARTED, eventId: 'E', sourceSeq: 2 });
  await driveToAck(boot1.eng, 's', c.secret, 1);
  // #2 已建租约并送达但未确认（模拟进程崩溃在确认之前）
  boot1.eng.lease('s', 2, 5000);
  boot1.eng.recordSent('s', 2, 200);
  assert.equal(boot1.eng.subscriberView('s').cursor, 1);
  const inflightRecoveredCountBefore = boot1.store.all().filter((r) => r.type === 'INFLIGHT_RECOVERED').length;

  // 重启：新 store/engine 从磁盘重放
  const boot2 = freshEngine(file, 4);
  await boot2.load();
  const view = boot2.eng.subscriberView('s');
  assert.equal(view.cursor, 1); // 游标未回退、未重复推进
  assert.equal(view.queue.length, 2);
  const item2 = view.queue.find((q) => q.envelopeSeq === 2);
  assert.equal(item2.status, ITEM_STATUS.PENDING); // 残留 INFLIGHT 复位为待发
  assert.equal(item2.attempts, 1); // 崩溃前的尝试次数保留
  const recovered = boot2.store.all().filter((r) => r.type === 'INFLIGHT_RECOVERED');
  assert.equal(recovered.length, inflightRecoveredCountBefore + 1);

  // 恢复后继续投递 #2 并确认，游标推进到 2
  await driveToAck(boot2.eng, 's', c.secret, 2);
  assert.equal(boot2.eng.subscriberView('s').cursor, 2);

  // 再重启一次：幂等恢复（第二次无 INFLIGHT，不再追加恢复记录），游标仍是 2
  const boot3 = freshEngine(file, 4);
  await boot3.load();
  assert.equal(boot3.eng.subscriberView('s').cursor, 2);
  const recovered2 = boot3.store.all().filter((r) => r.type === 'INFLIGHT_RECOVERED').length;
  assert.equal(recovered2, recovered.length);
});

test('信封内容被篡改：哈希链校验失败（QUARANTINED 只读）', async () => {
  const file = join(dir, 'tamper.log');
  const boot = freshEngine(file);
  await boot.load();
  boot.eng.createSubscriber({ subscriberId: 's', callbackUrl: 'http://127.0.0.1:9/cb' });
  boot.eng.validationResolved('s', true, 'ok', 200);
  boot.eng.ingestEvent({ type: NOTIFY_TYPES.PUBLISHED, eventId: 'E', sourceSeq: 1 });

  // 直接篡改磁盘上 DR_NOTIFICATION 信封的 eventId
  const lines = readFileSync(file, 'utf8').trim().split('\n');
  const idx = lines.findIndex((l) => l.includes('DR_NOTIFICATION'));
  const rec = JSON.parse(lines[idx]);
  rec.payload.envelope.eventId = 'EV-FORGED';
  lines[idx] = JSON.stringify(rec);
  writeFileSync(file, lines.join('\n') + '\n');

  const reboot = freshEngine(file);
  await reboot.load();
  const v = reboot.store.verify();
  assert.equal(v.ok, false);
  assert.equal(v.writable, false);
  assert.equal(reboot.store.mode, 'QUARANTINED');
  await rejectsCode(
    () => reboot.eng.ingestEvent({ type: NOTIFY_TYPES.ENDED, eventId: 'E', sourceSeq: 2 }),
    'LOG_QUARANTINED',
  );
});

test('不同订阅方互不影响：一方死信阻塞，另一方持续推进', async () => {
  const { eng, load } = freshEngine(join(dir, 'n.log'), 2);
  await load();
  const a = eng.createSubscriber({ subscriberId: 'a', callbackUrl: 'http://127.0.0.1:9/a' });
  const b = eng.createSubscriber({ subscriberId: 'b', callbackUrl: 'http://127.0.0.1:9/b' });
  eng.validationResolved('a', true, 'ok', 200);
  eng.validationResolved('b', true, 'ok', 200);
  void a; void b;

  for (let i = 1; i <= 3; i++) {
    eng.ingestEvent({ type: NOTIFY_TYPES.STARTED, eventId: `E${i}`, sourceSeq: i });
  }
  // a 的 #1 失败两次进死信
  for (let i = 0; i < 2; i++) { eng.claimHead('a'); eng.lease('a', 1, 5000); eng.recordFailure('a', 1, 'boom', 500); }
  assert.equal(eng.claimHead('a'), null); // a 被死信阻塞
  // b 完全不受影响，3 封顺序确认
  for (let seq = 1; seq <= 3; seq++) await driveToAck(eng, 'b', b.secret, seq);
  assert.equal(eng.subscriberView('b').cursor, 3);
  assert.equal(eng.subscriberView('a').cursor, 0);
});
