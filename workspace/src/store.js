// 只增事件日志 + SHA-256 哈希链。
// 每条记录形如：
// { seq, ts, type, payload, prevHash, hash }
// hash = sha256(canonical({seq,ts,type,payload,prevHash}))
// 任何对历史记录的篡改都会破坏链；append 之后永不更新、永不删除。
//
// 恢复契约（重启加载时）：
//  - 逐行校验，第一条出现【序号异常 / prevHash 断链 / 内容哈希不符 / 结构性损坏】
//    的“完整记录”即为首个损坏位置：进入只读隔离（QUARANTINED），文件原样保留，
//    只从最后一条验证通过的记录恢复内存状态，之后拒绝一切追加。
//  - 仅当文件末尾有且仅有一条无法解析为 JSON 的物理行（断电/崩溃造成的残尾）时，
//    才允许恢复：残尾原样另存为可审计副本（附 sha256/字节长度/时间戳），截断主日志，
//    服务从最后一条完好记录继续，新记录延续原哈希链。
//  - 残尾之后若还有任何内容（多于一条坏行、坏行后还有记录等），一律按损坏隔离。
import {
  existsSync,
  mkdirSync,
  appendFileSync,
  writeFileSync,
  truncateSync,
  readFileSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import { createHash } from 'node:crypto';
import { canonicalJSON, sha256Hex } from './util.js';

export const ZERO_HASH = '0'.repeat(64);
const HASH_RE = /^[0-9a-f]{64}$/;

// 日志被隔离（只读）：任何追加都会抛出此错，由上层映射为 HTTP 503。
export class LogQuarantineError extends Error {
  constructor(recovery) {
    super(`事件日志已隔离（只读）：${recovery?.reason ?? '未知原因'}`);
    this.code = 'LOG_QUARANTINED';
    this.status = 503;
    this.recovery = recovery;
  }
}

export class EventStore {
  constructor(file = null) {
    this.file = file;
    this.records = [];
    this.seq = 0;
    this.tipHash = ZERO_HASH;
    // 'OK'：链完整可写；'QUARANTINED'：检测到损坏，只读隔离
    this.mode = 'OK';
    // 恢复元信息：隔离时为 {kind:'corrupt',...}；残尾恢复后为 {kind:'torn-tail-recovered',...}
    this.recovery = null;
    if (file && dirname(file) && dirname(file) !== '.') {
      mkdirSync(dirname(file), { recursive: true });
    }
  }

  static hashOf(rec) {
    const { hash, ...rest } = rec;
    void hash;
    return sha256Hex(canonicalJSON(rest));
  }

  // 结构性检查：记录是否具备链校验所需的字段与类型。
  static #structuralProblem(rec, expectedSeq) {
    if (rec === null || typeof rec !== 'object' || Array.isArray(rec)) {
      return '记录不是合法的 JSON 对象';
    }
    if (rec.seq !== expectedSeq) {
      return `序号异常：期望 ${expectedSeq}，实际 ${JSON.stringify(rec.seq)}`;
    }
    if (typeof rec.type !== 'string' || !rec.type) {
      return '记录缺少合法 type';
    }
    if (!('payload' in rec) || rec.payload === null || typeof rec.payload !== 'object') {
      return '记录缺少合法 payload';
    }
    if (!HASH_RE.test(rec.prevHash)) {
      return '记录缺少合法 prevHash';
    }
    if (!HASH_RE.test(rec.hash)) {
      return '记录缺少合法 hash';
    }
    return null;
  }

  // 校验一条记录相对于“前一条已验证记录”是否完好；返回异常原因（null 表示通过）。
  static #recordProblem(rec, expectedSeq, prevHash) {
    const structural = EventStore.#structuralProblem(rec, expectedSeq);
    if (structural) return structural;
    if (rec.prevHash !== prevHash) return 'prevHash 不衔接（链断裂）';
    if (EventStore.hashOf(rec) !== rec.hash) return '记录内容与哈希不符（记录被改写）';
    return null;
  }

  append(type, payload, ts) {
    if (this.mode === 'QUARANTINED') throw new LogQuarantineError(this.recovery);
    const rec = {
      seq: this.seq + 1,
      ts: ts ?? null,
      type,
      payload: payload ?? {},
      prevHash: this.tipHash,
    };
    rec.hash = EventStore.hashOf(rec);
    this.records.push(rec);
    this.seq = rec.seq;
    this.tipHash = rec.hash;
    if (this.file) {
      appendFileSync(this.file, JSON.stringify(rec) + '\n');
    }
    return rec;
  }

  all() {
    return this.records;
  }

  get writable() {
    return this.mode !== 'QUARANTINED';
  }

  // 截断式重置：清空内存与磁盘（演示复位用）；同时解除隔离状态。
  truncate() {
    this.records = [];
    this.seq = 0;
    this.tipHash = ZERO_HASH;
    this.mode = 'OK';
    this.recovery = null;
    if (this.file) writeFileSync(this.file, '');
  }

  // 校验整条哈希链；返回 { mode, ok, writable, length, tipHash, brokenAt, reason, ... }
  verify() {
    const base = { mode: this.mode, writable: this.writable, recovery: this.recovery };
    if (this.mode === 'QUARANTINED' && this.recovery) {
      return { ...base, ok: false, length: this.records.length, tipHash: this.tipHash, ...this.recovery };
    }
    let prev = ZERO_HASH;
    for (let i = 0; i < this.records.length; i++) {
      const r = this.records[i];
      const problem = EventStore.#recordProblem(r, i + 1, prev);
      if (problem) {
        return { ...base, ok: false, length: this.records.length, tipHash: this.tipHash, brokenAt: r.seq, reason: problem };
      }
      prev = r.hash;
    }
    return { ...base, ok: true, length: this.records.length, tipHash: this.tipHash };
  }

  // 从磁盘恢复。返回 this.verify()。
  // 内存库或文件不存在：直接成功。
  async load() {
    if (!this.file || !existsSync(this.file)) return this.verify();
    const buf = readFileSync(this.file);
    if (!buf.length) return this.verify();

    // 字节级切分物理行（\n，兼容 \r\n），并定位最后一行是否以换行结束，
    // 保证残尾按原始字节（含多字节 UTF-8 被截断的情况）归档与截断。
    const lines = [];
    let start = 0;
    for (let i = 0; i < buf.length; i++) {
      if (buf[i] === 0x0a) {
        lines.push({ start, end: i, crlf: i > start && buf[i - 1] === 0x0d, complete: true });
        start = i + 1;
      }
    }
    if (start < buf.length) {
      lines.push({ start, end: buf.length, crlf: false, complete: false });
    }

    let prev = ZERO_HASH;
    let torn = null; // 唯一的候选残尾物理行
    for (let li = 0; li < lines.length; li++) {
      const ln = lines[li];
      const raw = buf.subarray(ln.start, ln.end);
      const text = raw.toString('utf8').replace(/\r$/, '').trim();
      if (!text) continue;

      let rec;
      try {
        rec = JSON.parse(text);
      } catch {
        // 不可解析的物理行：只有“文件末尾仅此一条、其后无字节、且没有终止换行”
        // 才是断电残尾（写了一半、连换行都没刷盘）。自带换行的坏物理行按损坏隔离。
        const isLastLine = li === lines.length - 1;
        const bytesAfterLine = buf.length - ln.end - (ln.complete ? 1 : 0);
        if (isLastLine && bytesAfterLine === 0 && !ln.complete) {
          torn = { line: ln, raw, text };
          break;
        }
        this.#quarantine({
          kind: 'corrupt',
          brokenAt: this.records.length + 1,
          lineNo: li + 1,
          byteOffset: ln.start,
          reason: ln.complete
            ? '损坏的日志行：物理行完整（含换行）但无法解析为 JSON'
            : '文件中部存在无法解析的日志行（残尾之后仍有内容）',
          rawPreview: text.slice(0, 200),
        });
        return this.verify();
      }

      const expectedSeq = this.records.length + 1;
      const problem = EventStore.#recordProblem(rec, expectedSeq, prev);
      if (problem) {
        this.#quarantine({
          kind: 'corrupt',
          brokenAt: expectedSeq,
          foundSeq: rec?.seq,
          lineNo: li + 1,
          byteOffset: ln.start,
          reason: problem,
          rawPreview: text.slice(0, 200),
        });
        return this.verify();
      }

      this.records.push(rec);
      this.seq = rec.seq;
      this.tipHash = rec.hash;
      prev = rec.hash;
    }

    if (torn) this.#recoverTornTail(buf, torn.raw, torn.line);
    return this.verify();
  }

  // 断电残尾恢复：原样归档残尾字节（可审计），截断主日志，链从最后一条完好记录延续。
  #recoverTornTail(buf, rawTail, line) {
    const dir = this.file ? dirname(this.file) : '.';
    const quarantineDir = join(dir, 'quarantine');
    mkdirSync(quarantineDir, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const archiveBase = `torn-tail-${stamp}-${process.pid}`;
    const archivePath = join(quarantineDir, `${archiveBase}.log`);
    const metaPath = join(quarantineDir, `${archiveBase}.meta.json`);
    const tailSha256 = createHash('sha256').update(rawTail).digest('hex');
    let truncatedAt = line.start; // 残尾起始字节偏移 == 截断后文件长度

    // 防御：若残尾紧邻的前一字节不是换行（异常交错写入），补一个换行，
    // 避免截断后下一条 append 与完好记录拼成同一物理行。
    if (truncatedAt > 0 && buf[truncatedAt - 1] !== 0x0a) {
      appendFileSync(this.file, '\n');
      truncatedAt += 1;
    }

    writeFileSync(archivePath, rawTail);
    writeFileSync(metaPath, JSON.stringify({
      kind: 'torn-tail',
      recoveredAt: new Date().toISOString(),
      sourceLog: this.file,
      lineByteOffset: line.start,
      truncatedAt,
      tailBytes: rawTail.length,
      tailSha256,
      tailPreview: rawTail.toString('utf8').slice(0, 500),
      lastGoodSeq: this.seq,
      lastGoodHash: this.tipHash,
    }, null, 2) + '\n');
    truncateSync(this.file, truncatedAt);

    this.recovery = {
      kind: 'torn-tail-recovered',
      reason: '检测到文件末尾单条不完整记录（断电残尾），已归档并截断，从最后一条完好记录恢复',
      brokenAt: this.seq + 1,
      byteOffset: line.start,
      tailBytes: rawTail.length,
      tailSha256,
      archivePath,
      archiveMetaPath: metaPath,
      recoveredAt: new Date().toISOString(),
    };
    // 模式保持 OK：链在最后一条完好记录处闭合，可以继续追加。
  }

  #quarantine(info) {
    this.mode = 'QUARANTINED';
    this.recovery = info;
    // 故意不修改日志文件：保留现场证据。
  }
}
