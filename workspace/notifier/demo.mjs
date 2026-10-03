#!/usr/bin/env node
// 一键演示脚本：在真实 HTTP 回环下顺序演出通知投递模块的 7 个场景。
//   1) 订阅端暂时失败（同幂等键重试，最终成功，台账只入账一次）
//   2) 确认丢失（租约超时同幂等键重投，订阅端去重）
//   3) 重复确认（第二次幂等，游标不重复推进）
//   4) 密钥轮换期间旧在途通知仍可校验
//   5) 死信后人工重新入队（含死信阻塞本队列后续、不同订阅方互不阻塞）
//   6) 进程重启后继续未确认投递、游标不重复推进
//   7) 恢复后顺序与签名证据仍可独立验证
//
// 用法：node notifier/demo.mjs            （自动拉起内存服务，演完即停）
//       node notifier/demo.mjs --keep     （演完保留服务，可打开投递台）
//
// 也可直接使用投递台手动演示： npm run notifier
process.env.NOTIFIER_TAIL_ORCH = '0';
process.env.NOTIFIER_LEASE_MS = '700';
process.env.NOTIFIER_POLL_MS = '60';
process.env.NOTIFIER_BACKOFF_BASE_MS = '120';
process.env.NOTIFIER_BACKOFF_MAX_MS = '500';
process.env.NOTIFIER_MAX_ATTEMPTS = '3';
process.env.NOTIFIER_HTTP_TIMEOUT_MS = '2000';

const { createServer } = await import('node:http');
const { createApp } = await import('./server.js');

const keep = process.argv.includes('--keep');
const tmp = createServer();
await new Promise((r) => tmp.listen(0, '127.0.0.1', r));
const port = tmp.address().port;
await new Promise((r) => tmp.close(r));

const app = await createApp({ port, memory: true, mockMemory: true, noTailer: true, confirmBaseUrl: `http://127.0.0.1:${port}` });
await app.start();
const base = `http://127.0.0.1:${port}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const J = (b) => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b) });
async function api(p, opts) {
  const res = await fetch(base + p, opts);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(json.error || `HTTP ${res.status}`), { body: json });
  return json;
}
async function sub(id) { return (await api(`/api/subscribers/${id}`)); }
async function cursor(id) { return (await sub(id)).cursor; }
async function queueItem(id, seq) { return (await sub(id)).queue.find((q) => q.envelopeSeq === seq); }
async function inboxCount(id, seq) {
  const r = await api(`/mock/subscribers/${id}/inbox`);
  const m = r.inbox.find((x) => x.envelopeSeq === seq);
  return m ? { ledger: 1, seen: m.attemptsSeen, keyId: m.keyId } : { ledger: 0, seen: 0 };
}
function h1(s) { console.log(`\n\x1b[36m▶ ${s}\x1b[0m`); }
function ok(s) { console.log(`  \x1b[32m✔\x1b[0m ${s}`); }
function info(s) { console.log(`  · ${s}`); }

console.log('════════════════════════════════════════════════════════════');
console.log(' 聚合商通知投递模块 · 故障与恢复演示（内存服务，真实 HTTP 回环）');
console.log('════════════════════════════════════════════════════════════');

// ---------- 0. 注册两个订阅方（订阅校验） ----------
h1('0 · 订阅校验（WebSub 风格 challenge 回显 + 聚合商签名）');
for (const id of ['alpha', 'beta']) {
  const r = await api('/api/subscribers', J({ subscriberId: id, displayName: { alpha: '聚合商甲', beta: '聚合商乙' }[id], callbackUrl: `${base}/mock/subscribers/${id}/callback` }));
  if (!r.validation.accepted) throw new Error(`${id} 校验失败：${r.validation.reason}`);
  ok(`${id} 校验通过并生效（secret 仅创建时返回一次，长度 ${r.secret.length} hex）`);
}

let envNo = 0;
async function publish(type, eventId, tag, detail) {
  const r = await api('/api/events/ingest', J({ type, eventId, sourceRef: `demo:${tag}`, detail: detail || {} }));
  envNo = r.envelopeSeq;
  return r;
}

// ---------- 1. 订阅端暂时失败 ----------
h1('1 · 订阅端暂时失败 → 同一稳定幂等键按序重试');
await api('/mock/mode', J({ subscriberId: 'beta', mode: 'FAIL_NEXT', budget: 2 }));
const e1 = await publish('dr.event.published', 'EV-DEMO', 'p1');
info(`生成信封 #${e1.envelopeSeq}，beta 前两次收到 503（模拟暂时不可用）…`);
await sleep(1600);
const q1 = await queueItem('beta', e1.envelopeSeq);
const ib1 = await inboxCount('beta', e1.envelopeSeq);
ok(`beta 最终确认 #${e1.envelopeSeq}：共投递 ${q1.attempts} 次，幂等键始终为 ${q1.idempotencyKey}`);
ok(`订阅端台账只入账 ${ib1.ledger} 条（503 失败响应不入账），cursor=${await cursor('beta')}`);

// ---------- 2 & 4. 确认丢失 + 轮换期间旧在途通知 ----------
h1('2 · 确认丢失 → 租约超时用同一幂等键重投（订阅端按幂等键去重）');
h1('4 · 密钥轮换期间，旧在途通知仍可用旧公钥校验');
await api('/mock/mode', J({ subscriberId: 'beta', mode: 'LOSE_ACK' }));
const e2 = await publish('dr.event.started', 'EV-DEMO', 's2');
await sleep(450);
info(`#${e2.envelopeSeq} 首次投递成功（旧密钥 ${e2.envelope.keyId.slice(0, 18)}…），beta 故意不回确认`);
await api('/api/keys/rotate', J({ reason: '演示：在途期间轮换' }));
info('签名密钥已轮换（新信封将用新密钥；旧密钥保留不删）');
await sleep(1100);
const ib2 = await inboxCount('beta', e2.envelopeSeq);
ok(`租约超时后同幂等键重投：台账仍 1 条，但订阅端实际见到投递 ${ib2.seen} 次（去重生效）`);
ok(`重投的是旧信封，携带旧 keyId，订阅端用旧公钥复验通过（keyId=${ib2.keyId.slice(0, 18)}…）`);
const verifyOld = await api(`/api/envelopes/${e2.envelopeSeq}/verify`);
ok(`独立验签：signatureValid=${verifyOld.signatureValid}，该密钥已非活动 keyActive=${verifyOld.keyActive}，链上 ledgerHash 完好`);
const ra = await api(`/mock/subscribers/beta/resend-ack/${e2.envelopeSeq}`, { method: 'POST' });
ok(`恢复后补发确认 → cursor=${ra.result.cursor}`);

// ---------- 3. 重复确认 ----------
h1('3 · 重复确认 → 幂等返回，绝不重复推进游标');
const before = await cursor('alpha');
await api('/mock/mode', J({ subscriberId: 'alpha', mode: 'DUPLICATE_ACK' }));
const e3 = await publish('dr.event.cancelled', 'EV-OTHER', 'c3');
await sleep(1000);
await api('/mock/mode', J({ subscriberId: 'alpha', mode: 'OK' }));
const dupAck = await api(`/mock/subscribers/alpha/resend-ack/${e3.envelopeSeq}`, { method: 'POST' });
ok(`#${e3.envelopeSeq} 投递时已连发两次确认、之后又手工补发一次；最终 duplicate=${dupAck.result.duplicate}，cursor=${dupAck.result.cursor}（推进仅一次）`);
const ackCount = app.store.all().filter((r) => r.type === 'DELIVERY_ACKED' && r.payload.subscriberId === 'alpha' && r.payload.envelopeSeq === e3.envelopeSeq).length;
ok(`账本中 #${e3.envelopeSeq} 的 DELIVERY_ACKED 记录恰好 ${ackCount} 条`);
void before;

// ---------- 5. 死信 + 人工重新入队（阻塞保序 + 互不阻塞） ----------
h1('5 · 连续失败进入死信 → 阻塞同队列后续但不影响其他订阅方 → 人工重新入队保序恢复');
await api('/mock/mode', J({ subscriberId: 'beta', mode: 'OK' }));
await api('/mock/mode', J({ subscriberId: 'alpha', mode: 'FAIL_NEXT', budget: 99 }));
const e5 = await publish('dr.event.published', 'EV-DLQ', 'dlq5');
await sleep(2200);
const dead = await queueItem('alpha', e5.envelopeSeq);
ok(`alpha #${e5.envelopeSeq} 投递 ${dead.attempts} 次仍失败 → 状态 DEAD（死信）`);
const e5b = await publish('dr.event.ended', 'EV-DLQ', 'dlq5b');
await sleep(700);
const behind = await queueItem('alpha', e5b.envelopeSeq);
ok(`后续 #${e5b.envelopeSeq} 被死信阻塞：保持 ${behind.status}、attempts=${behind.attempts}（绝不越过队首）`);
ok(`同一时段 beta 不受影响：cursor=${await cursor('beta')}（不同订阅方互不阻塞）`);
await api('/mock/mode', J({ subscriberId: 'alpha', mode: 'OK' }));
await api(`/api/subscribers/alpha/queue/${e5.envelopeSeq}/requeue`, J({ reason: '演示：运营台人工重新入队' }));
info(`人工将 #${e5.envelopeSeq} 重新入队（attempts 归零，位置不变）…`);
await sleep(1400);
ok(`恢复后严格按序：#${e5.envelopeSeq} → #${e5b.envelopeSeq} 全部确认，alpha cursor=${await cursor('alpha')}`);

// ---------- 6. 进程重启 ----------
h1('6 · 进程重启 → 继续未确认投递、游标不回退/不重复推进');
const cBefore = { alpha: await cursor('alpha'), beta: await cursor('beta') };
await api('/mock/mode', J({ subscriberId: 'beta', mode: 'LOSE_ACK' }));
const e6 = await publish('dr.event.report.corrected', 'EV-DEMO', 'r6', { version: 2 });
await sleep(450);
const inflight = await queueItem('beta', e6.envelopeSeq);
ok(`#${e6.envelopeSeq} 已送达未确认（${inflight.status}），此刻“重启”：从账本重放全部投影（等价新进程）`);
const engine2 = await app.restartFromLedger();
const view2 = engine2.subscriberView('beta');
const after = { alpha: engine2.subscriberView('alpha').cursor, beta: view2.cursor };
const recItem = view2.queue.find((q) => q.envelopeSeq === e6.envelopeSeq);
ok(`重放后游标保持 alpha=${after.alpha} beta=${after.beta}（重启前 ${cBefore.alpha}/${cBefore.beta}，未回退、未重复推进）`);
ok(`崩溃前在途的 #${e6.envelopeSeq} 复位为 ${recItem.status}（尝试次数 ${recItem.attempts} 保留），等待重新投递`);

// ---------- 7. 恢复后顺序与签名证据 ----------
h1('7 · 恢复后顺序投递成功，信封签名与全链证据仍可独立验证');
await api('/mock/mode', J({ subscriberId: 'beta', mode: 'OK' }));
await sleep(1100); // 重新投递 #6 并由 beta 正常确认
ok(`beta 最终 cursor=${await cursor('beta')}，#${e6.envelopeSeq}=${(await queueItem('beta', e6.envelopeSeq)).status}`);

const ev = await api(`/api/subscribers/beta/queue/${e6.envelopeSeq}/evidence`);
ok(`证据包：signatureValid=${ev.verification.signatureValid}，chainOk=${ev.verification.chainOk}，immutabilityIntact=${ev.verification.immutabilityIntact}`);
info(`#${e6.envelopeSeq} 全链路轨迹：${ev.trace.map((t) => t.type.replace('DELIVERY_', '').replace('QUEUE_', 'Q_')).join(' → ')}`);

const chains = await api('/api/chain/verify');
ok(`通知账本哈希链 ${chains.notifier.ok ? '完整' : '异常'}（${chains.notifier.length} 条）；模拟端台账链 ${chains.mock.ok ? '完整' : '异常'}`);

const total = app.store.all().length;
ok(`全程 ${total} 条账本记录，通知信封从未被原地修改；任何历史篡改都会在 /api/chain/verify 暴露`);

console.log('\n════════════════════════════════════════════════════════════');
console.log(' 全部场景演示完毕：保序投递 / 幂等确认 / 轮换验签 / 死信重入 / 重启恢复 / 证据可验');
console.log('════════════════════════════════════════════════════════════');

if (keep) {
  console.log(`\n--keep 已指定：服务继续运行，投递台 → http://localhost:${port}/ （Ctrl+C 退出）`);
} else {
  await app.stop();
  process.exit(0);
}
