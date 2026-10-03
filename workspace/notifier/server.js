// 聚合商通知投递服务（独立进程，零依赖 Node ≥20）。
//   通知 API + 确认 API + 订阅校验 + 内置模拟订阅端 + 投递台静态页面。
// 默认端口 8091（PORT 可覆盖）；编排服务在 8080，两者通过 tailer 只读桥接。
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, normalize, extname } from 'node:path';
import { EventStore } from '../src/store.js';
import { NotifierEngine, NOTIFY_TYPES } from './engine.js';
import { HttpTransport } from './transport.js';
import { Dispatcher } from './dispatcher.js';
import { MockSubscriber } from './mock-subscriber.js';
import { validateSubscription } from './subscription.js';
import { OrchestratorTailer } from './tailer.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = join(__dirname, 'public');
const NOTIFIER_LOG = process.env.NOTIFIER_LOG || join(__dirname, '..', 'data', 'notifier.log');
const MOCK_LOG = process.env.NOTIFIER_MOCK_LOG || join(__dirname, '..', 'data', 'mock-received.log');
const ORCH_LOG = process.env.DR_LOG || join(__dirname, '..', 'data', 'events.log');
const PORT = Number(process.env.NOTIFIER_PORT || 8091);
const HOST = process.env.NOTIFIER_HOST || '127.0.0.1';
const TAIL_ORCH = process.env.NOTIFIER_TAIL_ORCH !== '0'; // 默认尾随编排服务日志

const LEASE_MS = Number(process.env.NOTIFIER_LEASE_MS || 15_000);
const POLL_MS = Number(process.env.NOTIFIER_POLL_MS || 100);
const MAX_ATTEMPTS = Number(process.env.NOTIFIER_MAX_ATTEMPTS || 4);

export async function createApp(deps = {}) {
  const store = deps.store ?? new EventStore(deps.memory ? null : NOTIFIER_LOG);
  const initialEngine = deps.engine ?? new NotifierEngine(store, { maxAttempts: MAX_ATTEMPTS });
  await initialEngine.load();

  // 可变持有者：路由 / dispatcher / tailer / mock 回调都通过 ctx.engine 访问“当前”引擎。
  // 进程重启恢复（从账本重放出新引擎）时只替换 ctx.engine，所有引用自动跟随，杜绝多引擎分裂。
  const ctx = { engine: initialEngine };

  const transport = deps.transport ?? new HttpTransport({ timeoutMs: Number(process.env.NOTIFIER_HTTP_TIMEOUT_MS || 4000) });
  const dispatcher = deps.dispatcher ?? new Dispatcher(ctx, transport, {
    pollMs: Number(POLL_MS),
    leaseMs: Number(LEASE_MS),
    baseBackoffMs: Number(process.env.NOTIFIER_BACKOFF_BASE_MS || 300),
    maxBackoffMs: Number(process.env.NOTIFIER_BACKOFF_MAX_MS || 5000),
  });

  // 模拟订阅端回调聚合商确认 API 的基址（默认回环到自身）。
  const confirmBaseUrl = deps.confirmBaseUrl ?? `http://${HOST === '0.0.0.0' ? '127.0.0.1' : HOST}:${deps.port ?? PORT}`;
  const mock = deps.mock ?? new MockSubscriber(deps.mockMemory ? null : MOCK_LOG, { confirmBaseUrl });
  await mock.load();

  const tailer = (!deps.noTailer && TAIL_ORCH)
    ? (deps.tailer ?? new OrchestratorTailer(ctx, ORCH_LOG))
    : null;

  const MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
  };

  function send(res, status, body, headers = {}) {
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', ...headers });
    res.end(typeof body === 'string' ? body : JSON.stringify(body));
  }

  function readBody(req) {
    if (req.method === 'GET' || req.method === 'HEAD') return Promise.resolve({});
    return new Promise((resolve, reject) => {
      let data = '';
      req.on('data', (c) => {
        data += c;
        if (data.length > 2_000_000) reject(Object.assign(new Error('body too large'), { status: 413 }));
      });
      req.on('end', () => {
        if (!data.trim()) return resolve({});
        try { resolve(JSON.parse(data)); }
        catch { reject(Object.assign(new Error('非法 JSON'), { status: 400 })); }
      });
      req.on('error', reject);
    });
  }

  async function serveStatic(req, res) {
    let path = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    if (path === '/') path = '/index.html';
    const file = normalize(join(PUBLIC_DIR, path));
    if (!file.startsWith(PUBLIC_DIR)) return send(res, 403, { error: 'forbidden' });
    try {
      const buf = await readFile(file);
      res.writeHead(200, { 'Content-Type': MIME[extname(file)] || 'application/octet-stream' });
      res.end(buf);
    } catch {
      send(res, 404, { error: 'not found' });
    }
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const path = url.pathname;
    const engine = ctx.engine; // 每次请求解析当前引擎（重启重放后自动跟随）
    try {
      if (req.method === 'GET' && !path.startsWith('/api') && !path.startsWith('/mock')) {
        return await serveStatic(req, res);
      }

      // ---------- 模拟订阅端接收端点（订阅方视角，先于 JSON body 读取）----------
      // WebSub 约定：对回调 URL 本身发 GET（带 hub.challenge）做订阅校验，POST 投递通知。
      const mockCallback = path.match(/^\/mock\/subscribers\/([^/]+)\/callback$/);
      if (mockCallback) {
        const id = decodeURIComponent(mockCallback[1]);
        if (req.method === 'GET') {
          const r = mock.handleValidate(url, headerMap(req));
          res.writeHead(r.status, { 'Content-Type': 'text/plain; charset=utf-8' });
          return res.end(r.body);
        }
        if (req.method === 'POST') {
          const body = await readBody(req);
          const r = await mock.handleCallback(id, body, headerMap(req), engine);
          send(res, r.status, r.json);
          // 响应已交给对端后，再由模拟端异步回调确认 API（确认/送达顺序的真实建模）
          if (r.afterRespond) setImmediate(() => r.afterRespond().catch(() => {}));
          return;
        }
      }

      const body = await readBody(req);

      // ---------- 状态 / 证据 ----------
      if (path === '/api/state' && req.method === 'GET') return send(res, 200, engine.stateView());
      if (path === '/api/chain/verify' && req.method === 'GET') {
        return send(res, 200, { notifier: engine.store.verify(), mock: mock.store.verify() });
      }
      if (path === '/api/log' && req.method === 'GET') {
        const type = url.searchParams.get('type');
        const limit = Number(url.searchParams.get('limit') || 300);
        let recs = engine.store.all();
        if (type) recs = recs.filter((r) => r.type === type);
        return send(res, 200, { count: recs.length, records: recs.slice(-limit) });
      }
      const envMatch = path.match(/^\/api\/envelopes\/(\d+)$/);
      if (envMatch && req.method === 'GET') return send(res, 200, engine.envelopeView(Number(envMatch[1])));
      const envVerify = path.match(/^\/api\/envelopes\/(\d+)\/verify$/);
      if (envVerify && req.method === 'GET') return send(res, 200, engine.verifyEnvelope(Number(envVerify[1])));
      const evidenceMatch = path.match(/^\/api\/subscribers\/([^/]+)\/queue\/(\d+)\/evidence$/);
      if (evidenceMatch && req.method === 'GET') {
        return send(res, 200, engine.evidence(decodeURIComponent(evidenceMatch[1]), Number(evidenceMatch[2])));
      }

      // ---------- 订阅方 / 订阅校验 ----------
      if (path === '/api/subscribers' && req.method === 'GET') {
        return send(res, 200, [...engine.subscribers.keys()].map((id) => engine.subscriberView(id)));
      }
      if (path === '/api/subscribers' && req.method === 'POST') {
        const created = engine.createSubscriber(body);
        // 订阅落账后立即发起校验（callbackUrl 可指向本服务内置模拟端）。
        const result = await validateSubscription(engine, created.subscriberId);
        return send(res, 201, { subscriber: engine.subscriberView(created.subscriberId), secret: created.secret, validation: result });
      }
      const subView = path.match(/^\/api\/subscribers\/([^/]+)$/);
      if (subView && req.method === 'GET') return send(res, 200, engine.subscriberView(decodeURIComponent(subView[1])));
      const subRevalidate = path.match(/^\/api\/subscribers\/([^/]+)\/validate$/);
      if (subRevalidate && req.method === 'POST') {
        const result = await validateSubscription(engine, decodeURIComponent(subRevalidate[1]));
        return send(res, 200, result);
      }

      // ---------- 密钥 ----------
      if (path === '/api/keys' && req.method === 'GET') {
        return send(res, 200, { activeKeyId: engine.activeKeyId, keys: engine.keyOrder.map((id) => engine.keyView(id)) });
      }
      if (path === '/api/keys/rotate' && req.method === 'POST') {
        return send(res, 201, engine.rotateKey(body.reason || 'API 轮换'));
      }

      // ---------- 事件接入 ----------
      if (path === '/api/notify-types' && req.method === 'GET') {
        return send(res, 200, { types: Object.values(NOTIFY_TYPES) });
      }
      if (path === '/api/events/ingest' && req.method === 'POST') {
        return send(res, 201, engine.ingestEvent(body));
      }

      // ---------- 确认 API（订阅方回执，HMAC 签名在 header）----------
      if (path === '/api/confirmations' && req.method === 'POST') {
        const signature = req.headers['x-subscriber-signature'];
        return send(res, 200, engine.confirm({ ...body, signature }));
      }

      // ---------- 死信人工重新入队 ----------
      const requeue = path.match(/^\/api\/subscribers\/([^/]+)\/queue\/(\d+)\/requeue$/);
      if (requeue && req.method === 'POST') {
        engine.requeueDead(decodeURIComponent(requeue[1]), Number(requeue[2]), body.reason || '人工重新入队');
        return send(res, 200, { requeued: true, subscriber: engine.subscriberView(decodeURIComponent(requeue[1])) });
      }

      // ---------- 模拟订阅端控制面 ----------
      if (path === '/mock/mode' && req.method === 'POST') {
        const r = mock.setMode(body.subscriberId, body.mode, body.budget);
        return send(res, 200, r);
      }
      const mockInbox = path.match(/^\/mock\/subscribers\/([^/]+)\/inbox$/);
      if (mockInbox && req.method === 'GET') {
        return send(res, 200, { inbox: mock.inbox(decodeURIComponent(mockInbox[1])) });
      }
      const mockLogs = path.match(/^\/mock\/subscribers\/([^/]+)\/logs$/);
      if (mockLogs && req.method === 'GET') {
        return send(res, 200, { logs: mock.subscriberLogs(decodeURIComponent(mockLogs[1])).slice(-100) });
      }
      const mockResend = path.match(/^\/mock\/subscribers\/([^/]+)\/resend-ack\/(\d+)$/);
      if (mockResend && req.method === 'POST') {
        const r = await mock.resendAck(engine, decodeURIComponent(mockResend[1]), Number(mockResend[2]));
        return send(res, 200, r);
      }

      return send(res, 404, { error: 'unknown route', path });
    } catch (err) {
      const status = err.status || (err.code === 'LOG_QUARANTINED' ? 503 : 500);
      if (status === 500) console.error(err);
      send(res, status, {
        error: err.message || 'internal error',
        code: err.code || 'INTERNAL',
        details: err.details || {},
        ...(err.code === 'LOG_QUARANTINED' ? { recovery: engine.store.recovery } : {}),
      }, status === 503 ? { 'Retry-After': '300', 'Cache-Control': 'no-store' } : {});
    }
  });

  // 从账本重放出新引擎并热替换（等价于进程重启：所有内存投影从零重建、租约复位、游标不动）。
  async function restartFromLedger() {
    dispatcher.stop();
    const replayed = new NotifierEngine(store, { maxAttempts: MAX_ATTEMPTS });
    await replayed.load(); // 重放期间自动把残留 INFLIGHT 复位为 PENDING（INFLIGHT_RECOVERED）
    ctx.engine = replayed;
    dispatcher.start();
    return replayed;
  }

  return {
    server, ctx, get engine() { return ctx.engine; }, store, mock, dispatcher, tailer, restartFromLedger,
    start() {
      return new Promise((resolve) => {
        server.listen(deps.port ?? PORT, HOST, () => {
          dispatcher.start();
          tailer?.start();
          resolve(server.address().port);
        });
      });
    },
    async stop() {
      dispatcher.stop();
      tailer?.stop();
      await new Promise((r) => server.close(r));
    },
  };
}

function headerMap(req) {
  const out = {};
  for (const [k, v] of Object.entries(req.headers)) out[k] = Array.isArray(v) ? v.join(',') : v;
  return out;
}

// 直接运行时启动。
if (import.meta.url === `file://${process.argv[1]}`) {
  const app = await createApp();
  const actualPort = await app.start();
  console.log(`聚合商通知投递服务已启动: http://localhost:${actualPort}/`);
  console.log(`投递台:               http://localhost:${actualPort}/`);
  console.log(`通知账本: ${NOTIFIER_LOG}`);
  console.log(`模拟订阅端台账: ${MOCK_LOG}`);
  console.log(`编排日志桥接: ${TAIL_ORCH ? `开启（只读 ${ORCH_LOG}）` : '关闭（NOTIFIER_TAIL_ORCH=0）'}`);
  console.log(`投递租约: ${LEASE_MS}ms，轮询: ${POLL_MS}ms，最大尝试: ${MAX_ATTEMPTS}`);
}
