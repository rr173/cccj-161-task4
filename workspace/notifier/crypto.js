// 通知信封的非对称签名（Ed25519）与订阅方回执 HMAC（SHA-256）。
//
// 签名对象：投递时以 HTTP header 传输的 canonical JSON（见 canonicalPayload），
// 任意语言/实现都可按“键名字典序 + 无多余空白”的同样规则独立复算。
// - 聚合商私钥签名：X-Aggregator-Signature（Ed25519，hex），header 带 X-Key-Id；
// - 订阅方回执密钥：X-Subscriber-Signature = HMAC-SHA256(secret, canonicalJSON 回执体)（hex）。
//
// 旧在途通知的可验证性：密钥轮换只追加 KEY_ROTATED，旧 key 永不删除，
// 信封上的 keyId 永远指向“签发时那把钥匙”，因此任何历史信封都能按 keyId 取旧公钥复验。
import {
  generateKeyPairSync,
  sign,
  verify,
  createHmac,
  randomBytes,
} from 'node:crypto';

// 生成 Ed25519 密钥对，返回 PEM 字符串（私钥用于签名，公钥随 KEY_ROTATED 分发）。
export function generateSigningKeyPair() {
  const { publicKey, privateKey } = generateKeyPairSync('ed25519');
  return {
    publicKeyPem: publicKey.export({ type: 'spki', format: 'pem' }),
    privateKeyPem: privateKey.export({ type: 'pkcs8', format: 'pem' }),
  };
}

// 对规范化载荷签名，返回 hex。Ed25519 是 PureEdDSA：hash 必须为 null（内部自处理）。
export function signPayload(privateKeyPem, canonical) {
  return sign(null, Buffer.from(canonical, 'utf8'), privateKeyPem).toString('hex');
}

// 用 PEM 公钥验证 hex 签名；任何异常都视为验证失败。
export function verifyPayload(publicKeyPem, canonical, signatureHex) {
  try {
    return verify(null, Buffer.from(canonical, 'utf8'), publicKeyPem, Buffer.from(signatureHex, 'hex'));
  } catch {
    return false;
  }
}

// 订阅方回执的 HMAC：HMAC-SHA256(secret, canonicalJSON(body))。
export function hmacHex(secret, canonical) {
  return createHmac('sha256', secret).update(canonical).digest('hex');
}

export function newSubscriberSecret() {
  return randomBytes(32).toString('hex');
}
