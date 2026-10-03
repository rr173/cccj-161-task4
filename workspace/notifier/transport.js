// HTTP 投递传输：POST 签名信封到订阅方 callbackUrl。
// 使用 Node 20 内置全局 fetch，无第三方依赖；支持连接/读取超时。
import { canonicalJSON } from '../src/util.js';

export class DeliveryError extends Error {
  constructor(message, httpStatus = null) {
    super(message);
    this.code = 'DELIVERY_FAILED';
    this.httpStatus = httpStatus;
  }
}

export class HttpTransport {
  constructor({ timeoutMs = 4000 } = {}) {
    this.timeoutMs = timeoutMs;
  }

  // 投递一次。成功（HTTP 2xx）返回 { httpStatus }；非 2xx 或网络/超时抛 DeliveryError。
  // 注意：2xx 只代表订阅方“收到”，真正推进游标的是随后带稳定幂等键的确认 API。
  async deliver({ callbackUrl, envelope, signature, idempotencyKey, attempt }) {
    const body = canonicalJSON(envelope); // 与签名时完全一致的字节
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error('timeout')), this.timeoutMs);
    let res;
    try {
      res = await fetch(callbackUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          'X-Notification-Id': envelope.notificationId,
          'X-Envelope-Seq': String(envelope.envelopeSeq),
          'X-Idempotency-Key': idempotencyKey,
          'X-Key-Id': envelope.keyId,
          'X-Aggregator-Signature': signature,
          'X-Delivery-Attempt': String(attempt),
        },
        body,
        signal: controller.signal,
      });
    } catch (err) {
      const reason = err?.name === 'AbortError' ? `投递超时（${this.timeoutMs}ms）` : `网络错误：${err?.message || err}`;
      throw new DeliveryError(reason);
    } finally {
      clearTimeout(timer);
    }
    if (!res.ok) {
      const text = await safeText(res);
      throw new DeliveryError(`订阅方返回 HTTP ${res.status}：${text.slice(0, 200)}`, res.status);
    }
    await res.body?.cancel?.().catch(() => {});
    return { httpStatus: res.status };
  }
}

async function safeText(res) {
  try {
    return await res.text();
  } catch {
    return '';
  }
}
