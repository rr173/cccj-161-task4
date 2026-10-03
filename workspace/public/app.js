/* 运营台前端：原生 JS，轮询 /api/state，所有写操作走事件 API。 */
let STATE = null;
let selectedEvent = null;
let timer = null;

const $ = (id) => document.getElementById(id);

async function api(path, body, method = 'POST') {
  const res = await fetch(path, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.error || `HTTP ${res.status}`);
    err.code = data.code;
    err.details = data.details;
    throw err;
  }
  return data;
}

function toast(msg, kind = '') {
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.innerHTML = msg;
  $('toasts').appendChild(el);
  setTimeout(() => el.remove(), 5200);
}
const errToast = (e) => toast(`<b>${e.code || '错误'}</b>：${e.message}`, 'err');

const fmtTs = (ms) => new Date(ms).toISOString().replace('T', ' ').slice(0, 16) + ' UTC';

async function refresh() {
  try {
    STATE = await api('/api/state', null, 'GET');
    render();
  } catch (e) { /* 忽略轮询抖动 */ }
}

function render() {
  $('clock').textContent = '仿真时间 ' + STATE.nowIso.replace('T', ' ').slice(0, 16) + ' UTC';
  renderMeters();
  renderEvents();
  renderDevices();
  renderDetail();
  renderReports();
  renderChain();
  if (!$('evMeter').dataset.filled) {
    $('evMeter').innerHTML = STATE.meters.map((m) => `<option value="${m.id}">${m.id} · ${m.name}</option>`).join('');
    $('evMeter').dataset.filled = '1';
  }
}

function renderChain() {
  const c = STATE.chain;
  const quarantined = c.mode === 'QUARANTINED' || c.ok === false;
  const recovered = c.recovery?.kind === 'torn-tail-recovered';
  $('chainBadge').innerHTML = quarantined
    ? `<span class="chain-bad">⛔ 只读隔离 @seq ${c.brokenAt ?? '?'}</span>`
    : recovered
      ? `<span style="color:var(--warn)">⚠️ 残尾已恢复 (${c.length} 条)</span>`
      : `<span class="chain-ok">🔗 哈希链完整 (${c.length} 条)</span>`;
  if (quarantined) {
    $('chainInfo').innerHTML =
      `<b style="color:var(--bad)">只读隔离 QUARANTINED</b><br>
       首个损坏位置：<b>seq ${c.brokenAt ?? '?'}</b>（第 ${c.lineNo ?? '?'} 行，字节偏移 ${c.byteOffset ?? '?'}）<br>
       原因：${c.reason}<br>
       已验证记录：${c.length} 条，tip ${c.tipHash.slice(0, 16)}…<br>
       <span style="color:var(--muted)">所有业务写入返回 503，日志不再追加；文件原样保留待人工核验。</span>`;
  } else if (recovered) {
    const r = c.recovery;
    $('chainInfo').innerHTML =
      `<b style="color:var(--warn)">断电残尾已恢复</b><br>
       残尾 ${r.tailBytes} 字节（起始偏移 ${r.byteOffset}）已归档<br>
       归档副本：<span class="mono" style="font-size:10.5px">${r.archivePath}</span><br>
       残尾 sha256：<span class="mono" style="font-size:10.5px">${r.tailSha256.slice(0, 32)}…</span><br>
       已从最后一条完好记录（seq ${c.length}）延续哈希链。`;
  } else {
    $('chainInfo').innerHTML = `长度 ${c.length}<br>tip ${c.tipHash.slice(0, 20)}…`;
  }
  renderRecoveryBanner(quarantined, recovered);
}

function renderRecoveryBanner(quarantined, recovered) {
  const el = $('recoveryBanner');
  const r = STATE.chain.recovery;
  if (quarantined) {
    el.hidden = false;
    el.className = 'quarantine-banner';
    el.innerHTML =
      `⛔ <b>日志只读隔离</b>：持久日志在 <b>seq ${STATE.chain.brokenAt ?? '?'}</b>（第 ${r?.lineNo ?? '?'} 行）` +
      `检测到首个损坏 —— ${STATE.chain.reason}。` +
      `当前为隔离前最后一致状态（${STATE.chain.length} 条记录）；所有业务写入均被拒绝（HTTP 503），日志不再追加。`;
  } else if (recovered) {
    el.hidden = false;
    el.className = 'recovered-banner';
    el.innerHTML =
      `⚠️ <b>断电残尾恢复</b>：文件末尾单条不完整记录（${r.tailBytes} 字节）已留存审计副本并从主日志截断，` +
      `服务已从最后一条验证通过的记录（seq ${STATE.chain.length}）恢复并延续原哈希链。` +
      `审计副本：<span class="mono">${r.archivePath}</span>`;
  } else {
    el.hidden = true;
    el.textContent = '';
  }
}

function renderMeters() {
  $('meters').innerHTML = STATE.meters.map((m) => `
    <div class="row" style="justify-content:space-between;margin:6px 0">
      <div><b>${m.id}</b> <span class="muted">${m.name}</span>
        ${m.offline ? '<span class="offline">● 通信中断</span>' : '<span class="pill">在线</span>'}</div>
      <div class="btns">
        <button class="${m.offline ? 'good' : 'bad'}" onclick="toggleMeter('${m.id}', ${!m.offline})">
          ${m.offline ? '恢复通信' : '中断通信'}</button>
      </div>
    </div>`).join('');
}

function statusTag(s) {
  return s ? `<span class="tag ${s}">${s}</span>` : '<span class="muted">未回报</span>';
}

function renderEvents() {
  const evs = [...STATE.events].sort((a, b) => b.createdAt - a.createdAt);
  if (!evs.length) { $('events').innerHTML = '<div class="muted">尚无事件。</div>'; return; }
  $('events').innerHTML = evs.map((ev) => {
    const exec = ev.participants.filter((p) => ['EXECUTING', 'RECOVERED'].includes(p.status)).length;
    return `
    <div class="card ${ev.status.toLowerCase()} ${selectedEvent === ev.id ? 'selected' : ''}" onclick="selectEvent('${ev.id}')" style="cursor:pointer">
      <div class="row" style="justify-content:space-between">
        <b>${ev.id}</b> ${statusTag(ev.status)}
      </div>
      <div class="muted mono" style="margin:4px 0">
        ${ev.meterId} · ${ev.algorithm}<br>
        时窗 ${fmtTs(ev.startMs)} → ${fmtTs(ev.endMs)}<br>
        目标 ${ev.targetKw} kW · 参与 ${ev.participants.length} 台 · 执行 ${exec} 台
      </div>
      <div class="muted">基线版本 ${ev.baselineVersions.length} · 报告 ${ev.reports.length} · 缓冲回执 ${ev.bufferedReceipts.length}</div>
      ${ev.baselineVersions[0] ? `<div class="muted">冻结基线依据 ${ev.baselineVersions[0].days.length} 个历史日</div>` : '<div class="muted">基线待开始时冻结</div>'}
      <div class="btns" style="margin-top:7px" onclick="event.stopPropagation()">
        ${['SCHEDULED', 'ACTIVE'].includes(ev.status) ? `<button class="bad" onclick="cancelEvent('${ev.id}')">撤销指令</button>` : ''}
        <button class="ghost" onclick="selectEvent('${ev.id}')">查看 / 报告</button>
      </div>
    </div>`;
  }).join('');
}

function renderDevices() {
  const sel = selectedEvent;
  $('devices').innerHTML = STATE.devices.map((d) => {
    const inSel = sel && STATE.events.find((e) => e.id === sel)?.participants.some((p) => p.deviceId === d.id);
    const claim = d.claim;
    const claimTxt = claim
      ? `<div>${claim.aggregatorId} → ${claim.eventId.slice(-6)}</div>`
      : '<span class="muted">空闲</span>';
    let state;
    if (inSel) {
      const part = STATE.events.find((e) => e.id === sel).participants.find((p) => p.deviceId === d.id);
      state = `${statusTag(part.status)} <span class="muted mono">wm ${part.appliedSeq}</span>` +
        (d.buffered ? ` <span class="tag PARTIAL">缓冲${d.buffered}</span>` : '');
    } else if (claim) {
      state = `${statusTag(claim.status)} <span class="muted mono">wm ${claim.watermark} (其他事件)</span>`;
    } else {
      state = '<span class="muted">—</span>';
    }
    const ops = sel && inSel
      ? participantOps(d, STATE.events.find((e) => e.id === sel))
      : `<button class="primary" ${sel ? '' : 'disabled'} onclick="claim('${d.id}')">声明</button>`;
    return `<tr class="${inSel ? 'selected' : ''}">
      <td><b>${d.id}</b><div class="muted">${d.name}</div></td>
      <td class="muted">${d.meterId}<br>≤${d.maxKw}kW</td>
      <td>${claimTxt}<select id="agg-${d.id}" style="width:90px;margin-top:2px">
        ${STATE.aggregators.map((a) => `<option value="${a.id}">${a.id}</option>`).join('')}
      </select></td>
      <td>${state}</td>
      <td class="btns">${ops}</td>
    </tr>`;
  }).join('');
}

function participantOps(d, ev) {
  const part = ev.participants.find((p) => p.deviceId === d.id);
  const st = part.status;
  const kw = part.requestedKw || Math.round(d.maxKw * 0.8);
  const req = `<input id="kw-${d.id}" type="number" value="${kw}" style="width:64px;display:inline-block">`;
  let b = '';
  if (['ENROLLED', 'ACCEPTED'].includes(st) || st === null) {
    b += `<button class="ghost" onclick="receipt('${d.id}','ACCEPTED')">接受</button>`;
  }
  if (['ENROLLED', 'ACCEPTED'].includes(st)) {
    b += `<button class="good" onclick="receipt('${d.id}','EXECUTING')">执行 ${req}</button>`;
    b += `<button class="bad" onclick="receipt('${d.id}','DECLINED')">拒绝</button>`;
  }
  if (st === 'EXECUTING') {
    b += `<button class="warn" onclick="receipt('${d.id}','RECOVERED')">已恢复</button>`;
  }
  if (['RECOVERED', 'DECLINED'].includes(st)) {
    b += `<span class="tag ${st === 'RECOVERED' ? 'RECOVERED' : 'DECLINED'}">终态</span>`;
  }
  // 乱序演示：发送 seq 跳跃再补发（只有事件仍有效时有意义）
  if (['SCHEDULED', 'ACTIVE'].includes(ev.status) && st !== 'DECLINED' && st !== 'RECOVERED') {
    b += `<button class="ghost" title="模拟乱序：先发 seq+2，再发下一个回执自动补齐排水"
        onclick="receiptReorder('${d.id}','EXECUTING')">↕ 乱序执行</button>`;
  }
  return b;
}

function renderDetail() {
  const ev = selectedEvent ? STATE.events.find((e) => e.id === selectedEvent) : null;
  if (!ev) { $('detail').innerHTML = '在中间选择一个事件。'; $('detail').className = 'muted'; return; }
  $('detail').className = '';
  const baseline = ev.baselineVersions;
  $('detail').innerHTML = `
    <div class="row" style="justify-content:space-between">
      <b>${ev.id}</b> ${statusTag(ev.status)}
    </div>
    <dl class="kv">
      <dt>计量点</dt><dd>${ev.meterId} · ${ev.meterName}</dd>
      <dt>基线算法</dt><dd>${ev.algorithm}</dd>
      <dt>目标削减</dt><dd>${ev.targetKw} kW</dd>
      <dt>时窗</dt><dd>${fmtTs(ev.startMs)} → ${fmtTs(ev.endMs)}</dd>
      <dt>发布于</dt><dd>${fmtTs(ev.createdAt)}</dd>
    </dl>
    <h3>基线（开始时冻结）</h3>
    ${baseline.length ? baseline.map((b) => `
      <div class="report ${b.version === 1 ? 'v1' : ''}">
        <b>v${b.version}</b> <span class="muted">${b.reason} · ${fmtTs(b.frozenAt)}</span><br>
        <span class="muted">依据 ${b.days.length} 日：${b.days.join(', ')}</span>
      </div>`).join('') : '<div class="muted">事件尚未开始，基线未冻结。</div>'}
    <h3>参与设备水位（watermark）</h3>
    <table><thead><tr><th>设备</th><th>聚合商</th><th>阶段</th><th>wm</th></tr></thead><tbody>
      ${ev.participants.map((p) => `<tr><td>${p.deviceId}</td><td>${p.aggregatorId}</td>
        <td>${statusTag(p.status)}</td><td class="mono">${p.appliedSeq}</td></tr>`).join('')}
    </tbody></table>
    ${ev.bufferedReceipts.length ? `<h3>乱序缓冲（等待缺口补齐）</h3>
      ${ev.bufferedReceipts.map((b) => `<div class="muted mono">${b.deviceId} seq=${b.clientSeq} ${b.status}（等待 seq ${b.clientSeq - 1}）</div>`).join('')}` : ''}
  `;
}

function verdictBadge(v) { return `<span class="tag ${v}">${v}</span>`; }

function renderReports() {
  const ev = selectedEvent ? STATE.events.find((e) => e.id === selectedEvent) : null;
  if (!ev) { $('reports').className = 'muted'; $('reports').textContent = '事件结束后生成报告。'; return; }
  if (!ev.reports.length) {
    $('reports').className = 'muted';
    $('reports').textContent = ['CLOSED', 'RECALLED'].includes(ev.status) ? '（无报告）' : '事件结束后在此生成 v1 原始裁决。';
    return;
  }
  $('reports').className = '';
  $('reports').innerHTML = ev.reports.map((r) => `
    <div class="report ${r.version === 1 ? 'v1' : ''}">
      <div class="row" style="justify-content:space-between">
        <b>${r.kind === 'ORIGINAL' ? 'v1 原始裁决' : `v${r.version} 订正版`}</b>
        ${verdictBadge(r.verdict)}
      </div>
      <dl class="kv" style="margin-top:5px">
        <dt>削减电量</dt><dd><b>${r.reductionKwh}</b> / ${r.targetKwh} kWh</dd>
        <dt>交付率</dt><dd>${(r.deliveryRatio * 100).toFixed(1)}%
          <div class="bar"><i style="width:${Math.min(100, r.deliveryRatio * 100)}%"></i></div></dd>
        <dt>基线版本</dt><dd>v${r.baselineVersion}</dd>
        <dt>缺失区间</dt><dd>${r.gapCount}/${r.totalIntervals}</dd>
        <dt>指纹</dt><dd class="mono" style="font-size:10.5px">${r.hash.slice(0, 16)}…</dd>
      </dl>
      <div class="btns" style="margin-top:5px">
        <button class="ghost" onclick="openReport('${ev.id}',${r.version})">完整报告</button>
        <button class="ghost" onclick="verifyReport('${ev.id}',${r.version})">校验指纹</button>
      </div>
    </div>`).join('');
}

// ---------- 操作 ----------

function selectEvent(id) { selectedEvent = id; render(); }

async function publishEvent() {
  try {
    const r = await api('/api/events', {
      meterId: $('evMeter').value,
      algorithm: $('evAlg').value,
      targetKw: Number($('evTarget').value),
      startInMinutes: Number($('evIn').value),
      durationIntervals: Number($('evCount').value),
    });
    selectedEvent = r.event.id;
    toast(`已发布事件 <b>${r.event.id}</b>，时窗 ${r.event.startIso.slice(11, 16)} 开始`, 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

async function cancelEvent(id) {
  if (!confirm('确认撤销该指令？已声明设备将立即释放，执行中的设备事后 RECOVERED 仍会记录为迟到终态。')) return;
  try {
    const r = await api('/api/events/cancel', { eventId: id, reason: '运营台人工撤销' });
    toast(`事件已撤销，释放设备 ${r.released.join(', ') || '（无）'}${r.report ? '，已出具撤销版报告' : ''}`, 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

async function claim(deviceId) {
  if (!selectedEvent) return toast('请先在中间选择一个事件', 'err');
  const aggregatorId = $(`agg-${deviceId}`).value;
  try {
    await api('/api/claims', { eventId: selectedEvent, deviceId, aggregatorId });
    toast(`${aggregatorId} 声明 ${deviceId} 成功`, 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

function requestedKwFor(deviceId) {
  const el = $(`kw-${deviceId}`);
  return el ? Number(el.value) : undefined;
}

async function receipt(deviceId, status) {
  try {
    const payload = { eventId: selectedEvent, deviceId, status, aggregatorId: currentAgg(deviceId) };
    const kw = requestedKwFor(deviceId);
    if (status === 'EXECUTING' && kw) payload.requestedKw = kw;
    const r = await api('/api/receipts', payload);
    if (r.duplicate) toast(`重复回执被幂等忽略，水位 wm ${r.watermark}`, '');
    else if (r.drained) toast(`${status} 已应用，乱序缓冲自动补齐排水 ${r.drained} 条，水位 wm ${r.watermark}`, 'ok');
    else toast(`${status} 已应用，水位 wm ${r.watermark ?? ''}`, 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

// 乱序：用显式 clientSeq 先发“下下个”，再发“下一个”，观察缓冲与自动排水
async function receiptReorder(deviceId, status) {
  const agg = currentAgg(deviceId);
  const ev = STATE.events.find((e) => e.id === selectedEvent);
  const part = ev.participants.find((p) => p.deviceId === deviceId);
  const next = part.appliedSeq + 1;
  try {
    const r1 = await api('/api/receipts', { eventId: selectedEvent, deviceId, status, aggregatorId: agg, clientSeq: next + 1, requestedKw: requestedKwFor(deviceId) });
    toast(`乱序 seq=${next + 1}：${r1.buffered ? '已缓冲，等待 seq ' + r1.expectSeq : '直接应用'}`, '');
    const r2 = await api('/api/receipts', { eventId: selectedEvent, deviceId, status: 'ACCEPTED', aggregatorId: agg, clientSeq: next });
    toast(`缺口 seq=${next} 到达：应用并排水 ${r2.drained} 条`, 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

function currentAgg(deviceId) {
  return $(`agg-${deviceId}`)?.value || 'AGG-A';
}

async function toggleMeter(meterId, offline) {
  try {
    await api('/api/meters/offline', { meterId, offline });
    toast(`${meterId} 通信${offline ? '中断（该区间将成为数据缺口）' : '恢复'}`, offline ? 'err' : 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

async function lateHistory() {
  try {
    const r = await api('/api/late-history', { meterId: 'MP-1', dayOffset: 4 });
    toast(`迟到历史数据已送达：基线订正 ${r.baselineCorrected.map((b) => b.eventId.slice(-6) + '@v' + b.version).join(', ') || '（窗口基线无变化）'}；` +
      `订正报告 ${r.reportsIssued.length} 份（v1 永不改写）`, 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

async function backfillSelected() {
  if (!selectedEvent) return toast('请先选择已结束事件', 'err');
  try {
    const r = await api('/api/backfill', { eventId: selectedEvent });
    toast(`补传完成，回填 ${r.backfilled ?? '?'} 个区间，新订正版 ${r.reportsIssued.length} 份`, 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

async function tick(minutes) {
  try { const r = await api('/api/sim/tick', { minutes });
    const notes = [];
    if (r.frozen.length) notes.push('冻结基线 ' + r.frozen.length);
    if (r.closed.length) notes.push('结束事件 ' + r.closed.length);
    toast(`时间推进到 ${r.iso.slice(11, 16)} ${notes.length ? '（' + notes.join('，') + '）' : ''}`, 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

async function tickToClose() {
  const ev = STATE.events.find((e) => e.id === selectedEvent);
  if (!ev) return toast('请先选择事件', 'err');
  const mins = Math.max(15, Math.ceil((ev.endMs - STATE.now) / 60000 / 15) * 15);
  tick(mins);
}

async function resetAll() {
  if (!confirm('重置全部演示数据（清空事件链并重建种子）？')) return;
  try {
    await api('/api/sim/reset', {});
    selectedEvent = null;
    toast('演示数据已重置', 'ok');
    refresh();
  } catch (e) { errToast(e); }
}

async function openReport(evId, v) {
  const r = await api(`/api/reports/${encodeURIComponent(evId)}/versions/${v}`, null, 'GET');
  const w = window.open('', '_blank');
  w.document.write('<pre style="font:12px ui-monospace;white-space:pre-wrap;background:#0e1420;color:#cfe1f8;padding:20px">' +
    JSON.stringify(r, null, 2).replace(/[<>&]/g, (c) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c])) + '</pre>');
}

async function verifyReport(evId, v) {
  const r = await api(`/api/reports/${encodeURIComponent(evId)}/versions/${v}/verify`, null, 'GET');
  toast(`报告 v${v} 指纹${r.bodyHashMatches ? '一致 ✔' : '不一致 �'}<br>
    <span class="mono" style="font-size:10px">${r.recomputedHash.slice(0, 24)}…</span>`,
    r.bodyHashMatches ? 'ok' : 'err');
}

async function verifyChain() {
  const r = await api('/api/chain/verify', null, 'GET');
  $('verifyOut').hidden = false;
  $('verifyOut').textContent = JSON.stringify({
    ok: r.ok, mode: r.mode, writable: r.writable,
    length: r.length, brokenAt: r.brokenAt, reason: r.reason,
    byteOffset: r.byteOffset, lineNo: r.lineNo,
    recovery: r.recovery,
    tipHash: r.tipHash,
    reports: r.reportChecks.map((x) => ({ report: x.reportId, intact: x.anchorIntact })),
  }, null, 2);
}

async function loadLog() {
  const r = await api('/api/log?limit=60', null, 'GET');
  $('miniLog').textContent = r.records.slice().reverse().map((x) =>
    `#${String(x.seq).padStart(3, '0')} ${new Date(x.ts || 0).toISOString().slice(11, 16)} ${x.type.padEnd(20)} ${JSON.stringify(x.payload).slice(0, 90)}`
  ).join('\n');
}

refresh();
timer = setInterval(refresh, 2500);
