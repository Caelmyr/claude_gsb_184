/* 作业确定性回放 Replay & Diff */
Components.init('replay');
const C = Components;

const VERDICT_LABEL = {
  RUNNING: '回放运行中 Running',
  MATCH: '一致 Consistent',
  ORDER_ONLY: '仅顺序差异 Order only',
  VALUE_DIFF: '数值差异 Value diff',
  COUNT_DIFF: '数量差异 Count diff',
  MIXED_DIFF: '数值+数量差异 Mixed diff',
  REPLAY_FAILED: '回放失败 Failed',
};
const VERDICT_CLS = {
  MATCH: 'good', ORDER_ONLY: 'warn', VALUE_DIFF: 'bad',
  COUNT_DIFF: 'bad', MIXED_DIFF: 'bad', REPLAY_FAILED: 'bad', RUNNING: 'aqua',
};

let jobs = [];
let currentJob = '';
let currentReplayId = null;
let poller = null;

function jobById(id) { return jobs.find(j => j.job_id === id); }

function kvTable(rows) {
  return C.table(
    [
      { key: 'k', label: '项 Item', render: r => C.esc(r.k) },
      { key: 'v', label: '值 Value', render: r => `<span class="mono">${C.esc(r.v)}</span>` },
    ],
    rows,
  );
}

function verdictBadge(status) {
  const cls = VERDICT_CLS[status] || 'muted';
  return `<span class="badge ${cls}">${C.esc(VERDICT_LABEL[status] || status)}</span>`;
}

// ---------------------------------------------------------------------------
async function loadJobsAndPick() {
  const d = await API.get('/api/jobs');
  jobs = d.jobs || [];
  const host = document.getElementById('job-picker');
  let html = '<select class="job-select"><option value="">选择已完成作业 Select a finished job…</option>';
  jobs.forEach(j => {
    const can = j.status === 'SUCCEEDED' && !j.replay_of;
    html += `<option value="${j.job_id}"${can ? '' : ' disabled'}>` +
            `${C.esc(j.name)} — ${j.status}${j.replay_of ? ' (replay)' : ''}</option>`;
  });
  html += '</select>';
  host.innerHTML = html;
  const sel = host.querySelector('select');
  sel.addEventListener('change', () => selectJob(sel.value));
  const target = new URLSearchParams(location.search).get('job');
  if (target && jobs.some(j => j.job_id === target)) {
    sel.value = target;
    selectJob(target);
  } else {
    const first = jobs.find(j => j.status === 'SUCCEEDED' && !j.replay_of);
    if (first) { sel.value = first.job_id; selectJob(first.job_id); }
  }
}

async function selectJob(jobId) {
  currentJob = jobId;
  currentReplayId = null;
  if (poller) { poller.stop(); poller = null; }
  resetPanels();
  const btn = document.getElementById('btn-replay');
  btn.disabled = !jobId;
  if (!jobId) return;

  // Auto-select the most recent replay of this job, if any.
  try {
    const all = await API.get('/api/replays');
    const existing = (all.replays || []).find(r => r.original_job_id === jobId);
    if (existing) {
      currentReplayId = existing.replay_id;
      if (existing.status === 'RUNNING') startPolling(existing.replay_id);
      else renderManifest(existing);
    } else {
      renderPlaceholder(jobId);
    }
  } catch (e) { /* ignore */ }
}

function resetPanels() {
  ['banner', 'spec', 'fingerprint', 'partitions', 'value-diffs', 'count-diffs'].forEach(id => {
    document.getElementById(id).innerHTML = '';
  });
  document.getElementById('stats').innerHTML = '';
}

function renderPlaceholder(jobId) {
  const j = jobById(jobId);
  document.getElementById('banner').innerHTML =
    `<div class="card"><div class="empty">作业「${C.esc(j ? j.name : jobId)}」尚无回放记录。点击右上角按钮用相同输入与参数重跑一次。No replay yet — click “Replay this job”.</div></div>`;
  renderSpecFromJob(j);
}

function renderSpecFromJob(j) {
  if (!j) return;
  document.getElementById('spec').innerHTML = kvTable([
    { k: '原作业 Original job', v: j.job_id },
    { k: 'Mapper', v: j.mapper },
    { k: 'Reducer', v: j.reducer },
    { k: 'Map 任务数 Map tasks', v: j.num_map_tasks },
    { k: 'Reduce 任务数 Reduce tasks', v: j.num_reduce_tasks },
    { k: '输入行数 Input rows', v: C.fmtNum(j.input_rows) },
    { k: '参数 Params', v: JSON.stringify(j.params || {}) },
  ]);
}

// ---------------------------------------------------------------------------
async function startReplay() {
  if (!currentJob) return;
  const btn = document.getElementById('btn-replay');
  btn.disabled = true;
  try {
    const m = await API.post('/api/jobs/' + currentJob + '/replay', {});
    currentReplayId = m.replay_id;
    renderManifest(m);
    startPolling(m.replay_id);
  } catch (e) {
    document.getElementById('banner').innerHTML =
      `<div class="card"><div class="empty">无法启动回放：${C.esc(e.message)}</div></div>`;
  } finally {
    btn.disabled = false;
  }
}

function startPolling(replayId) {
  if (poller) poller.stop();
  poller = C.poll(async () => {
    const m = await API.get('/api/replays/' + replayId);
    renderManifest(m);
    if (['MATCH', 'ORDER_ONLY', 'VALUE_DIFF', 'COUNT_DIFF', 'MIXED_DIFF', 'REPLAY_FAILED'].includes(m.status)) {
      poller.stop();
      loadHistory();
    }
  }, 2000);
  poller.start();
}

// ---------------------------------------------------------------------------
function renderManifest(m) {
  const running = m.status === 'RUNNING';
  const r = m.report || {};
  const s = r.summary || {};

  document.getElementById('banner').innerHTML = `
    <div class="card">
      <div class="flex between wrap">
        <div>
          <div style="font-size:16px;font-weight:650;margin-bottom:6px">
            回放结论 Verdict: ${verdictBadge(m.status)}
          </div>
          <div class="small muted">
            原作业 <span class="mono">${C.esc(m.original_job_id)}</span> →
            回放作业 <span class="mono">${C.esc(m.replay_job_id)}</span>
            ${running ? ' · 正在相同条件下重跑… running…' : ''}
          </div>
          ${m.error ? `<div class="small" style="color:var(--critical)">${C.esc(m.error)}</div>` : ''}
          ${!running && m.status === 'MATCH'
            ? '<div class="small" style="color:var(--good);margin-top:4px">✔ 两次运行记录完全一致（含顺序）。All records identical.</div>' : ''}
          ${!running && m.status === 'ORDER_ONLY'
            ? '<div class="small" style="color:var(--serious);margin-top:4px">⚠ 记录与数值完全相同，仅呈现顺序不同（已按 key 对齐，非计算不一致）。Values identical; only order differs.</div>' : ''}
        </div>
        <a class="btn small" href="monitor.html">查看回放作业进度 View replay job →</a>
      </div>
    </div>`;

  if (r.summary) {
    document.getElementById('stats').innerHTML = [
      { label: '原记录 Original', value: C.fmtNum(s.original_records) },
      { label: '回放记录 Replay', value: C.fmtNum(s.replay_records) },
      { label: '数值差异 Value diffs', value: s.value_diff_records, cls: s.value_diff_records ? 'bad' : 'good' },
      { label: '回放缺失 Missing', value: s.missing_in_replay, cls: s.missing_in_replay ? 'bad' : 'good' },
      { label: '回放多出 Extra', value: s.extra_in_replay, cls: s.extra_in_replay ? 'bad' : 'good' },
    ].map(t => `<div class="stat"><div class="label">${t.label}</div><div class="value ${t.cls || ''}">${t.value}</div></div>`).join('');
  } else {
    document.getElementById('stats').innerHTML = '';
  }

  // Conditions captured for this replay
  const spec = m.spec || {};
  document.getElementById('spec').innerHTML = kvTable([
    { k: 'Mapper', v: spec.mapper || '-' },
    { k: 'Reducer', v: spec.reducer || '-' },
    { k: 'Map / Reduce 任务', v: `${spec.num_map_tasks} / ${spec.num_reduce_tasks}` },
    { k: '输入行数 Input rows', v: C.fmtNum(spec.input_rows) },
    { k: '参数 Params', v: JSON.stringify(spec.params || {}) },
    { k: '开始 Started', v: new Date(m.started_ms).toLocaleString() },
  ]);

  const fp = m.input_fingerprint || {};
  document.getElementById('fingerprint').innerHTML = kvTable([
    { k: '输入分片数 Shard count', v: C.fmtNum(fp.shard_count) },
    { k: '输入记录总数 Total records', v: C.fmtNum(fp.total_records) },
    { k: '指纹 SHA-256', v: fp.sha256 ? fp.sha256.slice(0, 24) + '…' : '-' },
  ]) + '<div class="small muted" style="margin-top:6px">输入分片从原作业逐片复制，而非按种子重新生成——两次运行消费的是同一份切分。Shards are copied verbatim from the original, never regenerated.</div>';

  renderPartitions(r);
  renderValueDiffs(r);
  renderCountDiffs(r);
  loadHistory();
}

function renderPartitions(r) {
  const diffs = r.partition_count_diffs;
  const host = document.getElementById('partitions');
  if (!diffs) { host.innerHTML = C.empty('等待回放完成… Pending replay completion'); return; }
  if (!diffs.length) {
    host.innerHTML = '<div class="empty" style="color:var(--good)">✔ 每个 Reduce 分区的记录数两次运行均相同。All partition counts match.</div>';
    return;
  }
  host.innerHTML = C.table([
    { key: 'partition', label: '分区 Partition', render: x => `<span class="mono">${C.esc(x.partition)}</span>` },
    { key: 'original', label: '原记录数 Original', num: true, render: x => C.fmtNum(x.original) },
    { key: 'replay', label: '回放记录数 Replay', num: true, render: x => C.fmtNum(x.replay) },
    { key: 'delta', label: '差值 Δ', num: true, render: x => (x.replay - x.original) },
  ], diffs);
}

function renderValueDiffs(r) {
  const host = document.getElementById('value-diffs');
  const diffs = r.value_diffs || [];
  if (!r.summary) { host.innerHTML = C.empty('等待回放完成… Pending'); return; }
  if (!diffs.length) {
    host.innerHTML = '<div class="empty" style="color:var(--good)">✔ 无数值差异记录。No value differences.</div>';
    return;
  }
  const rows = [];
  diffs.forEach(d => {
    (d.fields || []).forEach(f => {
      rows.push({
        key: d.key,
        pos: `${d.original_index} → ${d.replay_index}`,
        field: f.field === null ? '(whole record)' : f.field,
        o: JSON.stringify(f.original),
        n: JSON.stringify(f.replay),
        delta: f.delta != null ? f.delta : '',
      });
    });
  });
  host.innerHTML = C.table([
    { key: 'key', label: 'Key', render: x => `<b>${C.esc(x.key)}</b>` },
    { key: 'pos', label: '位置 orig→replay', render: x => `<span class="mono small">${C.esc(x.pos)}</span>` },
    { key: 'field', label: '字段 Field', render: x => C.esc(x.field) },
    { key: 'o', label: '原值 Original', render: x => C.esc(x.o) },
    { key: 'n', label: '回放值 Replay', render: x => C.esc(x.n) },
    { key: 'delta', label: '差值 Δ', num: true, render: x => x.delta },
  ], rows) + (r.truncated ? '<div class="small muted">仅显示前 200 条 Showing first 200 only.</div>' : '');
}

function renderCountDiffs(r) {
  const host = document.getElementById('count-diffs');
  if (!r.summary) { host.innerHTML = C.empty('等待回放完成… Pending'); return; }
  const missing = (r.missing || []).map(x => ({ kind: '缺失 missing', cls: 'bad', key: x.key,
      pos: 'orig #' + x.original_index, rec: JSON.stringify(x.record) }));
  const extra = (r.extra || []).map(x => ({ kind: '多出 extra', cls: 'warn', key: x.key,
      pos: 'replay #' + x.replay_index, rec: JSON.stringify(x.record) }));
  const rows = missing.concat(extra);
  if (!rows.length) {
    host.innerHTML = '<div class="empty" style="color:var(--good)">✔ 记录集合完全相同（无缺失/多出）。Same key set.</div>';
    return;
  }
  host.innerHTML = C.table([
    { key: 'kind', label: '类型 Kind', render: x => `<span class="badge ${x.cls}">${C.esc(x.kind)}</span>` },
    { key: 'key', label: 'Key', render: x => `<b>${C.esc(x.key)}</b>` },
    { key: 'pos', label: '位置 Position', render: x => `<span class="mono small">${C.esc(x.pos)}</span>` },
    { key: 'rec', label: '记录 Record', render: x => `<span class="small">${C.esc(x.rec)}</span>` },
  ], rows) + (r.truncated ? '<div class="small muted">仅显示前 200 条 Showing first 200 only.</div>' : '');
}

async function loadHistory() {
  const host = document.getElementById('history');
  const d = await API.get('/api/replays');
  const rows = (d.replays || []).slice(0, 20);
  if (!rows.length) { host.innerHTML = C.empty('暂无回放 No replays yet'); return; }
  host.innerHTML = C.table([
    { key: 'started_ms', label: '开始 Started', render: x => C.fmtTime(x.started_ms) },
    { key: 'original_name', label: '原作业 Original', render: x => C.esc(x.original_name) },
    { key: 'status', label: '结论 Verdict', render: x => verdictBadge(x.status) },
    { key: 'replay_job_id', label: '回放作业 Replay job', render: x => `<span class="mono small">${C.esc(x.replay_job_id)}</span>` },
    { key: 'open', label: '', render: (x, i) => `<button class="btn small" data-idx="${i}">查看 View</button>` },
  ], rows, { onClick: true });
  host.querySelectorAll('button[data-idx]').forEach(btn => {
    btn.addEventListener('click', () => {
      const r = rows[Number(btn.dataset.idx)];
      currentReplayId = r.replay_id;
      const sel = document.querySelector('#job-picker select');
      if (sel) sel.value = r.original_job_id;
      currentJob = r.original_job_id;
      renderManifest(r);
    });
  });
}

document.getElementById('btn-replay').addEventListener('click', startReplay);
loadJobsAndPick();
