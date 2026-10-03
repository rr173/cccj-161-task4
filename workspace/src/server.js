// 零依赖 HTTP 服务：事件 API + 静态页面。
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, extname, normalize } from 'node:path';
import { Kernel } from './kernel.js';
import { EventStore } from './store.js';
import { sha256Hex, canonicalJSON } from './util.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = join(__dirname, '..', 'public');
const LOG_FILE = process.env.DR_LOG || join(__dirname, '..', 'data', 'events.log');

const store = new EventStore(process.env.DR_MEMORY ? null : LOG_FILE);
// 先从磁盘重放，再引导内核：空库才写入种子，避免重启后重复种子破坏哈希链。
// 恢复契约：加载完成即确定模式——链损坏进入只读隔离（服务照常启动提供只读接口），
// 末尾单条残尾则归档审计副本后截断并续链。
await store.load();
const recoveryStatus = store.verify();
const kernel = new Kernel(store);
if (!recoveryStatus.writable) {
  console.error('⛔ 事件日志已进入只读隔离：');
  console.error(`   首个损坏位置 seq=${recoveryStatus.brokenAt}（第 ${recoveryStatus.lineNo ?? '?'} 行，字节偏移 ${recoveryStatus.byteOffset ?? '?'}）`);
  console.error(`   原因：${recoveryStatus.reason}`);
  console.error('   所有业务写入返回 503；日志文件原样保留，请人工核验后再恢复。');
} else if (recoveryStatus.recovery?.kind === 'torn-tail-recovered') {
  console.warn('⚠️  检测到断电残尾（文件末尾单条不完整记录）：');
  console.warn(`   残尾 ${recoveryStatus.recovery.tailBytes} 字节已归档：${recoveryStatus.recovery.archivePath}`);
  console.warn(`   主日志已截断到字节 ${recoveryStatus.recovery.byteOffset}，从 seq ${recoveryStatus.brokenAt - 1} 续链。`);
}

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

async function readBody(req) {
  if (req.method === 'GET' || req.method === 'HEAD') return {};
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => {
      data += c;
      if (data.length > 2_000_000) reject(Object.assign(new Error('body too large'), { status: 413 }));
    });
    req.on('end', () => {
      if (!data.trim()) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch {
        reject(Object.assign(new Error('非法 JSON'), { status: 400 }));
      }
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

// 重新独立计算报告指纹（校验链上哈希确实等于报告内容指纹）
function verifyReport(report) {
  const { hash, bodyHash, issuedAt, ...body } = report;
  void issuedAt;
  const recomputed = sha256Hex(canonicalJSON(body));
  return {
    reportId: report.reportId,
    version: report.version,
    kind: report.kind,
    storedHash: hash,
    recomputedHash: recomputed,
    bodyHashMatches: recomputed === (bodyHash || hash),
    anchorIntact: recomputed === hash,
  };
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const path = url.pathname;
  try {
    if (req.method === 'GET' && !path.startsWith('/api')) return serveStatic(req, res);

    const body = await readBody(req);
    const merged = { ...body, ...Object.fromEntries(url.searchParams.entries()) };

    // ---- API ----
    if (path === '/api/state' && req.method === 'GET') {
      return send(res, 200, kernel.stateView());
    }
    if (path === '/api/events' && req.method === 'POST') {
      const cmd = {
        type: 'PUBLISH_EVENT',
        meterId: body.meterId,
        targetKw: Number(body.targetKw),
        algorithm: body.algorithm,
        aggregatorId: body.aggregatorId,
      };
      if (body.startMs) cmd.startMs = Number(body.startMs);
      if (body.endMs) cmd.endMs = Number(body.endMs);
      if (body.startInMinutes != null) {
        const I = 15 * 60000;
        const start = Math.ceil((kernel.now() + Number(body.startInMinutes) * 60000) / I) * I;
        cmd.startMs = start;
        cmd.endMs = start + Number(body.durationIntervals || 16) * I;
      }
      return send(res, 201, kernel.dispatch(cmd));
    }
    if (path === '/api/events/cancel' && req.method === 'POST') {
      return send(res, 200, kernel.dispatch({ type: 'CANCEL_EVENT', eventId: body.eventId, reason: body.reason }));
    }
    if (path === '/api/claims' && req.method === 'POST') {
      return send(res, 201, kernel.dispatch({
        type: 'CLAIM_DEVICE',
        eventId: body.eventId,
        deviceId: body.deviceId,
        aggregatorId: body.aggregatorId,
      }));
    }
    if (path === '/api/receipts' && req.method === 'POST') {
      const cmd = {
        type: 'DEVICE_RECEIPT',
        eventId: body.eventId,
        deviceId: body.deviceId,
        status: body.status,
        aggregatorId: body.aggregatorId,
        receiptId: body.receiptId,
        sentAt: body.sentAt,
      };
      if (body.requestedKw != null) cmd.requestedKw = Number(body.requestedKw);
      if (body.clientSeq != null) cmd.clientSeq = Number(body.clientSeq);
      const out = kernel.dispatch(cmd);
      return send(res, out.buffered ? 202 : 200, out);
    }
    if (path === '/api/readings' && req.method === 'POST') {
      return send(res, 200, kernel.dispatch({
        type: 'INGEST_READINGS',
        items: body.items,
        source: body.source || 'API',
      }));
    }
    if (path === '/api/late-history' && req.method === 'POST') {
      return send(res, 200, kernel.dispatch({
        type: 'LOAD_MISSING_DAY',
        meterId: body.meterId,
        dayOffset: Number(body.dayOffset),
      }));
    }
    if (path === '/api/backfill' && req.method === 'POST') {
      return send(res, 200, kernel.dispatch({ type: 'BACKFILL_ACTUALS', eventId: body.eventId }));
    }
    if (path === '/api/meters/offline' && req.method === 'POST') {
      return send(res, 200, kernel.dispatch({
        type: 'SET_METER_OFFLINE',
        meterId: body.meterId,
        offline: !!body.offline,
      }));
    }
    if (path === '/api/sim/tick' && req.method === 'POST') {
      return send(res, 200, kernel.dispatch({ type: 'TICK', minutes: Number(body.minutes ?? 15) }));
    }
    if (path === '/api/sim/reset' && req.method === 'POST') {
      return send(res, 200, kernel.dispatch({ type: 'RESET' }));
    }

    // 报告详情 / 校验
    const reportMatch = path.match(/^\/api\/reports\/([^/]+)\/versions\/(\d+)$/);
    if (reportMatch && req.method === 'GET') {
      const evId = decodeURIComponent(reportMatch[1]);
      const v = Number(reportMatch[2]);
      const report = kernel.s.reports.get(evId)?.find((r) => r.version === v);
      if (!report) return send(res, 404, { error: '报告不存在' });
      return send(res, 200, report);
    }
    const verifyMatch = path.match(/^\/api\/reports\/([^/]+)\/versions\/(\d+)\/verify$/);
    if (verifyMatch && req.method === 'GET') {
      const evId = decodeURIComponent(verifyMatch[1]);
      const v = Number(verifyMatch[2]);
      const report = kernel.s.reports.get(evId)?.find((r) => r.version === v);
      if (!report) return send(res, 404, { error: '报告不存在' });
      return send(res, 200, verifyReport(report));
    }
    if (path === '/api/chain/verify' && req.method === 'GET') {
      return send(res, 200, {
        ...kernel.store.verify(),
        reportChecks: [...kernel.s.reports.values()].flat().map(verifyReport),
      });
    }
    if (path === '/api/log' && req.method === 'GET') {
      const type = url.searchParams.get('type');
      const limit = Number(url.searchParams.get('limit') || 200);
      let recs = kernel.store.all();
      if (type) recs = recs.filter((r) => r.type === type);
      recs = recs.slice(-limit);
      return send(res, 200, { count: recs.length, records: recs });
    }

    return send(res, 404, { error: 'unknown route', path });
  } catch (err) {
    const statusMap = {
      BAD_REQUEST: 400, NOT_FOUND: 404, CONFLICTING_READING: 409, OVERLAPPING_WINDOW: 409,
      DEVICE_BUSY: 409, DEVICE_ALREADY_CLAIMED: 409, METER_MISMATCH: 409,
      EVENT_TERMINAL: 409, EVENT_RECALLED: 409, NOT_CLAIMED: 409,
      PHASE_CONFLICT: 409, STALE_SEQ: 409, DAY_PRESENT: 409,
      LOG_QUARANTINED: 503,
      UNKNOWN_COMMAND: 400,
    };
    const status = err.status || statusMap[err.code] || 500;
    if (status === 500) console.error(err);
    const headers = status === 503 ? { 'Retry-After': '300', 'Cache-Control': 'no-store' } : {};
    send(res, status, {
      ...(err.code === 'LOG_QUARANTINED' ? err.recovery : {}),
      error: err.message,
      code: err.code || 'INTERNAL',
      details: err.details || {},
      ...(err.code === 'LOG_QUARANTINED' ? { recovery: err.recovery || kernel.store.recovery } : {}),
    }, headers);
  }
});

const PORT = Number(process.env.PORT || 8080);
server.listen(PORT, () => {
  const actual = server.address().port;
  console.log(`需求响应编排服务已启动: http://localhost:${actual}`);
  console.log(`事件日志: ${process.env.DR_MEMORY ? '(内存，不持久化)' : LOG_FILE}`);
});

export { server, kernel };
