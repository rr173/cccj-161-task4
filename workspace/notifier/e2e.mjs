// 真实 HTTP 端到端冒烟：拉起服务（内存账本、短租约/快轮询），跑通
// 注册校验 → 5 类事件顺序投递确认 → 暂时失败 → 死信重入队 → 轮换旧信封验签。
// 注意：ESM 静态 import 会被提升，环境变量必须在【动态导入 server.js 之前】设置才有效。
process.env.NOTIFIER_TAIL_ORCH = '0';
process.env.NOTIFIER_LEASE_MS = '600';
process.env.NOTIFIER_POLL_MS = '60';
process.env.NOTIFIER_BACKOFF_BASE_MS = '120';
process.env.NOTIFIER_BACKOFF_MAX_MS = '600';
process.env.NOTIFIER_MAX_ATTEMPTS = '3';
process.env.NOTIFIER_HTTP_TIMEOUT_MS = '2000';

const { createServer } = await import('node:http');
const { createApp } = await import('./server.js');

const tmpServer = createServer();
await new Promise((r) => tmpServer.listen(0, '127.0.0.1', r));
const port = tmpServer.address().port;
await new Promise((r) => tmpServer.close(r));

const app = await createApp({
  port,
  memory: true,
  mockMemory: true,
  noTailer: true,
  confirmBaseUrl: `http://127.0.0.1:${port}`,
});
await app.start();
const base = `http://127.0.0.1:${port}`;
const call = async (p, opts) => {
  const res = await fetch(base + p, opts);
  const json = await res.json().catch(() => ({}));
  return { status: res.status, json };
};
const J = (body) => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let pass = 0, fail = 0;
const check = (name, cond, extra) => {
  if (cond) { pass++; console.log('  ✔', name); }
  else { fail++; console.log('  ✘', name, extra ?? ''); }
};

try {
  // 1. 注册两个模拟订阅方（真实订阅校验回环）
  const a = await call('/api/subscribers', J({ subscriberId: 'alpha', displayName: '甲', callbackUrl: `${base}/mock/subscribers/alpha/callback` }));
  check('alpha 注册+校验通过', a.status === 201 && a.json.validation.accepted, JSON.stringify(a.json));
  const b = await call('/api/subscribers', J({ subscriberId: 'beta', displayName: '乙', callbackUrl: `${base}/mock/subscribers/beta/callback` }));
  check('beta 注册+校验通过', b.status === 201 && b.json.validation.accepted, JSON.stringify(b.json));

  // 1b. 拒绝校验：模拟端先置 REJECT_VALIDATION，注册时 challenge 回显校验必须失败 → REJECTED
  await call('/mock/mode', J({ subscriberId: 'gamma', mode: 'REJECT_VALIDATION' }));
  const g = await call('/api/subscribers', J({ subscriberId: 'gamma', callbackUrl: `${base}/mock/subscribers/gamma/callback` }));
  check('拒绝校验的订阅方不生效（REJECTED）', g.json.subscriber.status === 'REJECTED' && g.json.validation.accepted === false, g.json.validation?.reason);

  // 2. 发布 3 个事件
  await call('/api/events/ingest', J({ type: 'dr.event.published', eventId: 'EV-1', sourceRef: 'e2e:p' }));
  await call('/api/events/ingest', J({ type: 'dr.event.started', eventId: 'EV-1', sourceRef: 'e2e:s' }));
  await call('/api/events/ingest', J({ type: 'dr.event.report.corrected', eventId: 'EV-1', sourceRef: 'e2e:r', detail: { version: 2 } }));

  await sleep(1200);
  let va = await call('/api/subscribers/alpha').then((r) => r.json);
  let vb = await call('/api/subscribers/beta').then((r) => r.json);
  check('alpha 3 封全部确认且 cursor=3', va.cursor === 3 && va.queue.every((q) => q.status === 'ACKED'), JSON.stringify(va.queue));
  check('beta 3 封全部确认且 cursor=3', vb.cursor === 3 && vb.queue.every((q) => q.status === 'ACKED'));
  const gv = await call('/api/subscribers/gamma').then((r) => r.json);
  check('REJECTED 订阅方收不到任何通知（队列空）', gv.queue.length === 0, JSON.stringify(gv.queue));

  // 3. 验签 + 链
  const v = await call('/api/envelopes/1/verify').then((r) => r.json);
  check('信封#1 签名有效且在链上', v.signatureValid && v.chainOk, JSON.stringify(v));

  // 4. 暂时失败：beta 接下来失败 2 次（503，订阅端未入账），随后同幂等键重试成功
  await call('/mock/mode', J({ subscriberId: 'beta', mode: 'FAIL_NEXT', budget: 2 }));
  await call('/api/events/ingest', J({ type: 'dr.event.ended', eventId: 'EV-1', sourceRef: 'e2e:e' }));
  await sleep(1500);
  vb = await call('/api/subscribers/beta').then((r) => r.json);
  const item4 = vb.queue.find((q) => q.envelopeSeq === 4);
  check('beta #4 失败 2 次后用同一幂等键重试成功（attempts>=3，cursor=4）', vb.cursor === 4 && item4.attempts >= 3, `cursor=${vb.cursor} attempts=${item4.attempts}`);
  const inbox = await call('/mock/subscribers/beta/inbox').then((r) => r.json);
  const in4all = inbox.inbox.filter((m) => m.envelopeSeq === 4);
  check('订阅端台账 #4 恰好一条（失败响应不入账，成功入账一次）', in4all.length === 1 && in4all[0].attemptsSeen === 1, JSON.stringify(in4all));

  // 5. 不同订阅方互不阻塞：beta 折腾期间 alpha 不受影响
  va = await call('/api/subscribers/alpha').then((r) => r.json);
  check('alpha 与 beta 互不阻塞（alpha cursor=4）', va.cursor === 4, `alpha cursor=${va.cursor}`);

  // 6. 重复确认幂等
  const rr = await call(`/mock/subscribers/alpha/resend-ack/1`, { method: 'POST' });
  check('补发历史确认返回 duplicate=true 且游标不变', rr.json.result?.duplicate === true && rr.json.result?.cursor === 4, JSON.stringify(rr.json));

  // 7. 确认丢失 + 密钥轮换期间旧在途通知仍可校验：
  //    beta 收到 #5 但丢确认 → 租约超时后用同一幂等键重投；期间轮换密钥，
  //    重投的旧信封带旧 keyId，订阅端用旧公钥仍验签通过并入账去重。
  await call('/mock/mode', J({ subscriberId: 'beta', mode: 'LOSE_ACK' }));
  await call('/api/events/ingest', J({ type: 'dr.event.cancelled', eventId: 'EV-9', sourceRef: 'e2e:c' }));
  await sleep(500); // 首次投递（旧密钥签名，beta 入账但不确认）
  const keysBefore = await call('/api/keys').then((r) => r.json);
  await call('/api/keys/rotate', J({ reason: 'e2e 轮换（在途期间）' }));
  const vOld = await call('/api/envelopes/5/verify').then((r) => r.json);
  check('轮换后旧信封 #5 仍用旧公钥验签通过（keyActive=false）', vOld.signatureValid && vOld.keyActive === false);
  await sleep(1300); // 租约超时 → 同幂等键重投，mock 用信封上的旧 keyId 取旧公钥复验
  let in5 = (await call('/mock/subscribers/beta/inbox').then((r) => r.json)).inbox.find((m) => m.envelopeSeq === 5);
  check('确认丢失触发重投：#5 台账仍只一条，但见到投递 ≥2 次（稳定幂等键去重）', in5 && in5.attemptsSeen >= 2, JSON.stringify(in5));
  check('重投的旧在途通知通过旧公钥校验（台账 keyId 是旧密钥）', in5 && in5.keyId === vOld.keyId, `${in5?.keyId} vs ${vOld.keyId}`);
  vb = await call('/api/subscribers/beta').then((r) => r.json);
  check('未确认前 beta 游标停在 4（#5 已送达未确认）', vb.cursor === 4, `cursor=${vb.cursor}`);
  const ra = await call(`/mock/subscribers/beta/resend-ack/5`, { method: 'POST' });
  check('补发确认后 beta 游标推进到 5', ra.json.result?.ok && ra.json.result?.cursor === 5, JSON.stringify(ra.json));
  const keysAfter = await call('/api/keys').then((r) => r.json);
  check('历史密钥被保留（数量 +1）', keysAfter.keys.length === keysBefore.keys.length + 1);

  // 8. 死信：alpha 持续失败，#6 达 maxAttempts 进死信，#7 必须在队首被阻塞（不跳号）
  const maxAttempts = app.engine.maxAttempts;
  await call('/mock/mode', J({ subscriberId: 'beta', mode: 'OK' })); // beta 恢复正常
  await call('/mock/mode', J({ subscriberId: 'alpha', mode: 'FAIL_NEXT', budget: 99 }));
  await call('/api/events/ingest', J({ type: 'dr.event.published', eventId: 'EV-DLQ', sourceRef: 'e2e:dlq' }));
  await sleep(2200);
  va = await call('/api/subscribers/alpha').then((r) => r.json);
  const dlqItem = va.queue.find((q) => q.envelopeSeq === 6);
  check(`#6 连续 ${maxAttempts} 次失败进入死信`, dlqItem.status === 'DEAD' && dlqItem.attempts >= maxAttempts, JSON.stringify(dlqItem));

  // #7 在 #6 死信后入队，必须停在 PENDING，绝不越过死信投递（同一订阅方保序）
  await call('/api/events/ingest', J({ type: 'dr.event.ended', eventId: 'EV-DLQ', sourceRef: 'e2e:dlq-end' }));
  await sleep(900);
  va = await call('/api/subscribers/alpha').then((r) => r.json);
  const behind = va.queue.find((q) => q.envelopeSeq === 7);
  check('死信 #6 阻塞同队列后续 #7（#7 保持 PENDING、未尝试）', behind.status === 'PENDING' && behind.attempts === 0, JSON.stringify(behind));
  // beta 不受 alpha 死信影响（不同订阅方互不阻塞）
  vb = await call('/api/subscribers/beta').then((r) => r.json);
  check('alpha 死信不阻塞 beta（beta cursor 已到 7）', vb.cursor === 7, `beta cursor=${vb.cursor}`);

  // 恢复 alpha → 人工重新入队 #6 → 严格按 6→7 顺序投递确认
  await call('/mock/mode', J({ subscriberId: 'alpha', mode: 'OK' }));
  const rq = await call('/api/subscribers/alpha/queue/6/requeue', J({ reason: 'e2e 人工重入' }));
  check('死信重新入队成功', rq.status === 200);
  await sleep(1500);
  va = await call('/api/subscribers/alpha').then((r) => r.json);
  check('重入队后 #6→#7 依次确认、cursor 推进到 7（恢复后顺序与签名证据仍可验证）',
    va.cursor === 7 && va.queue.filter((q) => q.envelopeSeq >= 6).every((q) => q.status === 'ACKED'),
    `cursor=${va.cursor}`);
  const ev = await call('/api/subscribers/alpha/queue/6/evidence').then((r) => r.json);
  check('#6 证据包：签名有效且轨迹含 DEAD→REQUEUED→ACKED',
    ev.verification?.signatureValid &&
    ev.trace.some((t) => t.type === 'DELIVERY_DEADLETTERED') &&
    ev.trace.some((t) => t.type === 'DLQ_REQUEUED') &&
    ev.trace.some((t) => t.type === 'DELIVERY_ACKED'),
    ev.trace?.map((t) => t.type).join(','));

  // 9. 重启恢复：新建引擎重放同一账本，INFLIGHT 复位、游标不重复推进/不回退
  await app.stop();
  const { NotifierEngine } = await import('./engine.js');
  const engine2 = new NotifierEngine(app.store, { maxAttempts });
  await engine2.load();
  const sa = engine2.subscriberView('alpha');
  const sb = engine2.subscriberView('beta');
  check('重启后游标不回退/不重复推进（alpha=7 beta=7）', sa.cursor === 7 && sb.cursor === 7, `${sa.cursor}/${sb.cursor}`);
  check('重启后无残留 INFLIGHT（未确认投递待重新认领）', [...sa.queue, ...sb.queue].every((q) => q.status !== 'INFLIGHT'));
  check('重启后账本记录重放一致', engine2.store.verify().ok);

  console.log(`\nE2E 结果：${pass} 通过 / ${fail} 失败`);
  process.exit(fail ? 1 : 0);
} catch (e) {
  console.error('E2E 异常：', e);
  process.exit(1);
}
