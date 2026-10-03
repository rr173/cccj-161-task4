// 订阅校验（WebSub 风格）：聚合商向 callbackUrl 发 GET 请求，带 hub.challenge、
// keyId 与 X-Aggregator-Signature；订阅方必须原样回显 challenge 且通过签名校验才算生效。
// 校验结果通过 engine.validationResolved 落账，未通过的订阅方不会收到任何通知。
import { canonicalEnvelope } from './engine.js';

export async function validateSubscription(engine, subscriberId, { timeoutMs = 4000 } = {}) {
  const sub = engine.subscribers.get(subscriberId);
  if (!sub) return { subscriberId, accepted: false, reason: '订阅方不存在', httpStatus: null };

  const u = new URL(sub.callbackUrl);
  u.searchParams.set('hub.mode', 'subscribe');
  u.searchParams.set('hub.challenge', sub.challenge);
  u.searchParams.set('hub.topic', 'dr.event');

  const challengePayload = { challenge: sub.challenge, subscriberId, topic: 'dr.event' };
  const canonical = canonicalEnvelope(challengePayload);
  const { signPayload } = await import('./crypto.js');
  const signature = signPayload(engine.activeKey().privateKeyPem, canonical);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);
  try {
    const res = await fetch(u, {
      method: 'GET',
      headers: {
        'X-Key-Id': engine.activeKeyId,
        'X-Aggregator-Signature': signature,
        'X-Aggregator-Challenge': sub.challenge,
      },
      signal: controller.signal,
    });
    const text = (await res.text().catch(() => '')).trim();
    const accepted = res.ok && text === sub.challenge;
    const reason = accepted
      ? 'challenge 回显一致'
      : !res.ok
        ? `校验端点返回 HTTP ${res.status}`
        : `challenge 回显不匹配（期望 ${sub.challenge.slice(0, 16)}…，实际 ${text.slice(0, 40)}）`;
    engine.validationResolved(subscriberId, accepted, reason, res.status);
    return { subscriberId, accepted, reason, httpStatus: res.status };
  } catch (err) {
    const reason = err?.name === 'AbortError' ? `校验超时（${timeoutMs}ms）` : `校验请求失败：${err?.message || err}`;
    engine.validationResolved(subscriberId, false, reason, null);
    return { subscriberId, accepted: false, reason, httpStatus: null };
  } finally {
    clearTimeout(timer);
  }
}
