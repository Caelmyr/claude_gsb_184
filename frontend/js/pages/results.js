/* 结果导出与确定性回放 Results and deterministic replay */
Components.init('results');
const C = Components;

let currentJob = '';
let activeReplayId = '';

function diffTypeLabel(type) {
  return {
    numeric_difference: '数值差异 Numeric',
    value_difference: '数值/内容差异 Value',
    type_mismatch: '类型差异 Type',
    order_difference: '顺序差异 Order',
    count_difference: '数量差异 Count',
    partition_difference: '分区差异 Partition',
    extra_field: '新增字段 Extra field',
    missing_field: '缺失字段 Missing field',
  }[type] || type;
}

function replayHeadline(status) {
  return {
    MATCH: '一致 Consistent — 回放结果与原作业在允许的数值容差内完全一致',
    ORDER_DIFF: '仅顺序/分区差异 Order-only — 记录集合一致，但输出位置不同',
    NUMERIC_DIFF: '数值不一致 Numeric difference',
    COUNT_DIFF: '记录数量不一致 Count difference',
    MISMATCH: '不一致 Mismatch',
    FAILED: '回放失败 Replay failed',
    RUNNING: '回放运行中 Running — 正在用相同输入分片和参数重跑',
  }[status] || status;
}

function renderDiffs(report) {
  const diffs = report.differences || [];
  if (!diffs.length) return C.empty('未发现差异 No differences');
  const rows = diffs.slice(0, 200);
  return C.table([
    { key: 'type', label: '类型 Type', render: r => `<span class="badge ${r.type.includes('numeric') ? 'warn' : 'bad'}">${C.esc(diffTypeLabel(r.type))}</span>` },
    { key: 'key', label: '记录 Key', render: r => `<b>${C.esc(r.key)}</b>` },
    { key: 'path', label: '字段 Path', render: r => C.esc(r.path || r.direction || '-') },
    { key: 'source', label: '原结果位置/值', render: r => C.esc(`${r.source_partition ?? '-'}:${r.source_position ?? '-'} ${r.source_value ?? ''}`) },
    { key: 'replay', label: '回放位置/值', render: r => C.esc(`${r.replay_partition ?? '-'}:${r.replay_position ?? '-'} ${r.replay_value ?? ''}`) },
    { key: 'delta', label: '差值 Delta', num: true, render: r => r.abs_delta == null ? '-' : Number(r.abs_delta).toPrecision(6) },
  ], rows);
}

function renderReplayPanel(replays) {
  const panel = document.getElementById('replay-panel');
  const summary = document.getElementById('replay-summary');
  const diffHost = document.getElementById('replay-diffs');
  if (!replays || !replays.length) {
    panel.style.display = 'none';
    activeReplayId = '';
    return;
  }

  const latest = replays[0];
  if (!activeReplayId && ['RUNNING', 'MAP', 'SHUFFLE', 'REDUCE', 'PENDING', 'ASSIGNED'].includes(latest.replay_status || latest.status)) {
    activeReplayId = latest.job_id;
  }
  const report = latest.report || {};
  const status = latest.replay_status || latest.status;
  const running = ['RUNNING', 'MAP', 'SHUFFLE', 'REDUCE', 'PENDING', 'ASSIGNED'].includes(status);
  if (!running) activeReplayId = '';

  panel.style.display = '';
  const cls = report.consistent ? 'good' : (report.logical_consistent ? 'warn' : (running ? 'aqua' : 'bad'));
  summary.innerHTML = `
    <div class="flex between">
      <div><span class="badge ${cls}">${C.esc(replayHeadline(status))}</span></div>
      <div class="muted small mono">replay: ${C.esc(latest.job_id)}</div>
    </div>
    <div class="stat-tiles mt">
      <div class="stat"><div class="label">原记录数 Source</div><div class="value">${C.fmtNum(report.source_total)}</div></div>
      <div class="stat"><div class="label">回放记录数 Replay</div><div class="value">${C.fmtNum(report.replay_total)}</div></div>
      <div class="stat"><div class="label">差异总数 Differences</div><div class="value">${C.fmtNum(report.difference_count)}</div></div>
      <div class="stat"><div class="label">数值容差 Tolerance</div><div class="value">${C.esc(report.numeric_tolerance)}</div></div>
    </div>`;
  diffHost.innerHTML = running ? C.empty('回放完成后自动生成对比报告 Comparison runs automatically after completion.') : renderDiffs(report);
}

async function startReplay() {
  const btn = document.getElementById('start-replay');
  btn.disabled = true;
  btn.textContent = '提交中 Starting…';
  try {
    const replay = await API.post('/api/jobs/' + currentJob + '/replays', { numeric_tolerance: 1e-9 });
    activeReplayId = replay.job_id;
    C.toast('回放已启动 Replay started: ' + replay.job_id);
  } catch (e) {
    C.toast(e.message || '启动回放失败', 'bad');
  } finally {
    btn.disabled = false;
    btn.textContent = '回放并对比 Replay & Compare';
  }
  render();
}

async function render() {
  if (!currentJob) return;
  let d;
  let replays;
  try {
    [d, replays] = await Promise.all([
      API.get('/api/jobs/' + currentJob + '/results?limit=100'),
      API.get('/api/jobs/' + currentJob + '/replays'),
    ]);
  } catch (e) { return; }
  replays = replays.replays || [];

  document.getElementById('dl-json').href = '/api/jobs/' + currentJob + '/results/download?format=json';
  document.getElementById('dl-csv').href = '/api/jobs/' + currentJob + '/results/download?format=csv';
  const btn = document.getElementById('start-replay');
  btn.disabled = d.status !== 'SUCCEEDED';
  btn.title = d.status === 'SUCCEEDED' ? '' : '仅已完成作业可回放 Only succeeded jobs can be replayed';

  document.getElementById('stats').innerHTML = [
    { label: '结果记录 Total records', value: C.fmtNum(d.total) },
    { label: '分区数 Partitions', value: d.partitions.length },
    { label: '作业状态 Status', value: d.status },
    { label: '回放次数 Replays', value: replays.length },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value">${C.esc(s.value)}</div></div>`).join('');

  document.getElementById('partitions').innerHTML = d.partitions.length
    ? C.table([
        { key: 'partition_name', label: '分区 Partition', render: r => `<span class="mono">${C.esc(r.partition_name)}</span>` },
        { key: 'count', label: '记录数 Count', render: r => C.fmtNum(r.count), num: true },
        { key: 'task_id', label: 'Reduce 任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
      ], d.partitions)
    : C.empty('暂无结果 No results — 作业可能尚未完成');

  const records = d.records || [];
  document.getElementById('preview').innerHTML = records.length
    ? C.table([
        { key: 'key', label: 'Key', render: r => `<b>${C.esc(r.key)}</b>` },
        { key: 'value', label: 'Value', render: r => C.valueCell(r) },
      ], records)
    : C.empty('暂无结果 No results');

  renderReplayPanel(replays);
}

document.getElementById('start-replay').addEventListener('click', startReplay);
C.jobPicker('job-picker', (id) => { currentJob = id; activeReplayId = ''; render(); });
C.poll(render, 3000).start();
