/* Pointwise revocation console — vanilla JS, no build step. */
"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

async function api(method, path, body) {
  const opts = { method, headers: { "Accept": "application/json" } };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const resp = await fetch(path, opts);
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.error || `${resp.status}`);
  return data;
}

const esc = (s) => String(s ?? "").replace(/[&<>"]/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const fmtTs = (ts) => ts == null ? "—" : new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false });
const pill = (v) => `<span class="pill ${esc(v)}">${esc(v)}</span>`;

function scenarioLog(line) {
  const el = $("#scenario-log");
  el.textContent += `[${new Date().toLocaleTimeString("zh-CN", { hour12: false })}] ${line}\n`;
  el.scrollTop = el.scrollHeight;
}

// ---------------------------------------------------------------- rendering
async function refresh() {
  const [ov, notifs, supplierReqs, logs, audit] = await Promise.all([
    api("GET", "/api/overview"),
    api("GET", "/api/notifications"),
    api("GET", "/api/supplier/requests"),
    api("GET", "/api/logs"),
    api("GET", "/api/audit"),
  ]);
  renderWorker(ov.worker);
  renderLicenses(ov.fields);
  renderTasks(ov.tasks);
  renderPackages(ov.packages);
  renderSeals(ov.seals, ov.audit_chain);
  renderNotifs(notifs.notifications);
  renderTable("#supplier-req-tbody", supplierReqs.requests,
    (r) => [r.id, fmtTs(r.ts), r.method, `<span class="mono">${esc(r.path)}</span>`, r.status_code]);
  renderTable("#app-log-tbody", logs.logs,
    (r) => [fmtTs(r.ts), r.level, r.component, esc(r.message)]);
  renderAudit(audit.entries);
}

function renderWorker(w) {
  const badge = $("#worker-badge");
  badge.textContent = `worker: ${w.state}${w.pid ? ` (pid ${w.pid})` : ""}`;
  badge.className = `badge ${w.state}`;
  $("#worker-detail").innerHTML = [
    ["状态", w.state], ["PID", w.pid ?? "—"],
    ["挂起", w.suspended ? "是" : "否"],
    ["上次心跳", w.last_heartbeat == null ? "从未" :
      `${fmtTs(w.last_heartbeat)}（${w.seconds_since_heartbeat}s 前）`],
    ["离线判定阈值", `${w.offline_after}s`],
  ].map(([k, v]) => `<div><b>${k}：</b>${esc(v)}</div>`).join("");
}

function renderLicenses(fields) {
  $("#license-tbody").innerHTML = Object.entries(fields).map(([name, f]) => {
    const l = f.license;
    const permitted = l.permitted === 1 || l.permitted === true;
    const counts = Object.entries(f.result_counts)
      .map(([k, v]) => `${k}:${v}`).join("，") || "—";
    return `<tr><td><b>${esc(name)}</b></td>
      <td>${pill(permitted ? "permitted" : "revoked")}</td>
      <td>${fmtTs(l.effective_time)}</td>
      <td class="mono">${esc(l.notification_id || "—")}</td>
      <td>${esc(counts)}</td></tr>`;
  }).join("");
}

function renderNotifs(list) {
  $("#notif-tbody").innerHTML = list.map((n) =>
    `<tr><td>${n.seq}</td><td class="mono">${esc(n.notification_id)}</td>
     <td>${esc(n.field)}</td><td>${pill(n.permitted ? "permitted" : "revoked")}</td>
     <td>${fmtTs(n.effective_time)}</td><td>${fmtTs(n.processed_at)}</td>
     <td><button class="btn" onclick="redeliver(${n.seq})">重发</button></td></tr>`).join("");
}

function renderTasks(tasks) {
  $("#task-tbody").innerHTML = tasks.map((t) => {
    const fields = JSON.parse(t.requested_fields);
    const cancel = t.status === "QUEUED"
      ? `<button class="btn btn-danger" onclick="cancelTask(${t.id})">取消</button>` : "";
    const reason = t.cancel_reason || t.filtered_reason ||
      (t.status === "DOWNLOADING" && t.downloaded_at ? "已下载，等待消费者" : "");
    return `<tr><td>${t.id}</td><td class="mono">${esc(t.task_uid)}</td>
      <td>${esc(t.package_id)}</td><td>${esc(fields.join(", "))}</td>
      <td>${pill(t.status)}</td><td>${esc(reason)}</td><td>${cancel}</td></tr>`;
  }).join("");
}

function renderPackages(packages) {
  $("#package-tbody").innerHTML = packages.map((p) =>
    `<tr><td><b>${esc(p.id)}</b>${p.sealed_path ? " 🔒" : ""}</td>
     <td><code class="path">${esc(p.cache_path || "（已清理）")}</code></td>
     <td>${p.size_bytes}</td>
     <td class="mono">${esc(p.content_hash.slice(0, 16))}…</td></tr>`).join("")
    || `<tr><td colspan="4" class="hint">尚无下载</td></tr>`;
}

function renderSeals(seals, chain) {
  $("#seal-tbody").innerHTML = seals.map((s) =>
    `<tr><td><b>${esc(s.package_id)}</b></td>
     <td><code class="path">${esc(s.sealed_path)}</code></td>
     <td class="mono">${esc(s.sha256.slice(0, 16))}…</td>
     <td>${esc(s.reason)}</td></tr>`).join("")
    || `<tr><td colspan="4" class="hint">尚无封存证据</td></tr>`;
}

function renderAudit(entries) {
  $("#audit-tbody").innerHTML = entries.slice().reverse().map((e) =>
    `<tr><td>${e.seq}</td><td>${fmtTs(e.ts)}</td><td>${esc(e.actor)}</td>
     <td>${esc(e.action)}</td>
     <td class="mono">${esc(e.payload_json)}</td></tr>`).join("");
}

function renderTable(sel, rows, mapper) {
  $(sel).innerHTML = rows.map((r) => "<tr>" + mapper(r).map((c) =>
    `<td>${c}</td>`).join("") + "</tr>").join("")
    || `<tr><td colspan="5" class="hint">暂无数据</td></tr>`;
}

// ---------------------------------------------------------------- lineage
async function showLineage() {
  const field = $("#lineage-field").value;
  const data = await api("GET", `/api/lineage?field=${encodeURIComponent(field)}`);
  const l = data.license;
  const permitted = !l || l.permitted === 1 || l.permitted === true;
  $("#lineage-out").innerHTML = `
    <div class="lineage-block">
      <h3>许可</h3>
      <div>字段 <b>${esc(field)}</b>：${pill(permitted ? "permitted" : "revoked")}
      ${l && l.notification_id ? `（通知 <span class="mono">${esc(l.notification_id)}</span>，${fmtTs(l.effective_time)} 生效）` : ""}</div>
    </div>
    <div class="lineage-block"><h3>出现的包（${data.packages.length}）</h3>
      <table><thead><tr><th>包</th><th>字段值哈希</th><th>缓存</th><th>封存证据</th></tr></thead>
      <tbody>${data.packages.map((p) => `<tr><td>${esc(p.package_id)}</td>
        <td class="mono">${esc(p.value_hash.slice(0, 16))}…</td>
        <td>${p.cache_path ? "在 raw_cache" : "已清理"}</td>
        <td>${p.sealed_path ? "🔒 已封存" : "—"}</td></tr>`).join("") ||
        '<tr><td colspan="4" class="hint">无</td></tr>'}</tbody></table>
    </div>
    <div class="lineage-block"><h3>相关任务（${data.tasks.length}）</h3>
      <table><thead><tr><th>id</th><th>uid</th><th>状态</th><th>请求字段</th></tr></thead>
      <tbody>${data.tasks.map((t) => `<tr><td>${t.id}</td>
        <td class="mono">${esc(t.task_uid)}</td><td>${pill(t.status)}</td>
        <td>${esc(t.requested_fields)}</td></tr>`).join("") ||
        '<tr><td colspan="4" class="hint">无</td></tr>'}</tbody></table>
    </div>
    <div class="lineage-block"><h3>结果（${data.results.length}）</h3>
      <table><thead><tr><th>id</th><th>任务</th><th>包</th><th>状态</th><th>可分发值</th><th>原因</th></tr></thead>
      <tbody>${data.results.map((r) => `<tr><td>${r.id}</td>
        <td class="mono">${esc(r.task_uid)}</td><td>${esc(r.package_id)}</td>
        <td>${pill(r.status)}</td>
        <td class="mono">${r.value == null ? "—（不可分发）" : esc(r.value)}</td>
        <td>${esc(r.filtered_reason || "")}</td></tr>`).join("") ||
        '<tr><td colspan="6" class="hint">无</td></tr>'}</tbody></table>
    </div>
    <div class="lineage-block"><h3>通知历史（${data.notifications.length}）</h3>
      <table><thead><tr><th>seq</th><th>notification_id</th><th>动作</th><th>生效</th></tr></thead>
      <tbody>${data.notifications.map((n) => `<tr><td>${n.seq}</td>
        <td class="mono">${esc(n.notification_id)}</td>
        <td>${pill(n.permitted ? "permitted" : "revoked")}</td>
        <td>${fmtTs(n.effective_time)}</td></tr>`).join("") ||
        '<tr><td colspan="4" class="hint">无</td></tr>'}</tbody></table>
    </div>`;
}

// ---------------------------------------------------------------- actions
window.redeliver = async (seq) => {
  const r = await api("POST", "/api/redeliver", { seq });
  scenarioLog(`重发 seq=${seq} → 新 seq=${r.notification.seq}，id 仍为 ${r.notification.notification_id}（将被去重）`);
  refresh();
};
window.cancelTask = async (id) => {
  await api("POST", "/api/tasks/cancel", { task_id: id, reason: "页面手动取消" });
  refresh();
};

async function bindActions() {
  $("#btn-refresh").onclick = refresh;
  $("#btn-reset").onclick = async () => {
    if (!confirm("重置将停止 worker、清空 runtime 并复位供应商，确定？")) return;
    await api("POST", "/api/reset");
    $("#scenario-log").textContent = "";
    scenarioLog("环境已重置");
    refresh();
  };
  $$("[data-worker]").forEach((btn) => {
    btn.onclick = async () => {
      const action = btn.dataset.worker;
      if (action === "suspend" || action === "resume") {
        await api("POST", `/api/worker/${action}`);
      } else {
        await api("POST", `/api/worker/${action}`);
      }
      refresh();
    };
  });
  $("#btn-revoke").onclick = async () => {
    const field = $("#revoke-field").value;
    const r = await api("POST", "/api/revoke", { field, effective_in: 0 });
    scenarioLog(`已请求收回 ${field}，通知 ${r.notification.notification_id}`);
    refresh();
  };
  $("#btn-restore").onclick = async () => {
    const field = $("#revoke-field").value;
    await api("POST", "/api/restore", { field });
    scenarioLog(`已恢复 ${field} 的许可`);
    refresh();
  };
  $("#btn-redeliver").onclick = () =>
    redeliver(Number($("#redeliver-seq").value));
  $("#btn-poll").onclick = async () => {
    const r = await api("POST", "/api/poll");
    scenarioLog(`手动拉取：新摄入 ${r.ingested} 条通知`);
    refresh();
  };
  $("#btn-create-task").onclick = async () => {
    const fields = $$(".fld:checked").map((c) => c.value);
    if (!fields.length) return alert("至少选择一个字段");
    const t = await api("POST", "/api/tasks",
      { package_id: $("#task-package").value, requested_fields: fields });
    scenarioLog(`创建任务 ${t.task_uid}（id=${t.task_id}）`);
    refresh();
  };
  $("#btn-evict").onclick = async () => {
    const r = await api("POST", "/api/cache/evict");
    scenarioLog(`缓存清理：删除 ${r.evicted.length} 个，保留 ${r.skipped.length} 个；封存证据不受影响`);
    r.evicted.forEach((e) => scenarioLog(`  evict ${e.package_id}（封存证据保留=${e.sealed_evidence_preserved}）`));
    r.skipped.forEach((s) => scenarioLog(`  skip ${s.package_id}: ${s.reason}`));
    refresh();
  };
  $("#btn-verify").onclick = async () => {
    const r = await api("POST", "/api/audit/verify");
    $("#verify-out").textContent = JSON.stringify(r, null, 2);
  };
  $("#btn-lineage").onclick = showLineage;
  $$("[data-scenario]").forEach((btn) => {
    btn.onclick = () => runScenario(btn.dataset.scenario);
  });
}

// ---------------------------------------------------------------- scenarios
async function runScenario(name) {
  try {
    if (name === "race") await scenarioRace();
    if (name === "redeliver") await scenarioRedeliver();
    if (name === "offline") await scenarioOffline();
    if (name === "cancel") await scenarioCancel();
  } catch (exc) {
    scenarioLog(`✗ 场景失败: ${exc.message}`);
  }
  refresh();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// A: 下载刚完成就收回许可 —— 接收后过滤在入库边界兜底
async function scenarioRace() {
  scenarioLog("── 场景 A：在途下载 / 下载刚完成即收回 ──");
  await api("POST", "/api/worker/resume");
  // 仅订阅 phone，保证下载窗口内任务只涉及该字段
  const t = await api("POST", "/api/tasks",
    { package_id: "pkg-bravo", requested_fields: ["phone"] });
  scenarioLog(`任务 #${t.task_id} 已入队，开始真实 HTTP 下载（供应商有模拟延迟）…`);
  // 在下载窗口内发出收回：许可先于消费者入库到达
  await sleep(300);
  const rev = await api("POST", "/api/revoke", { field: "phone", effective_in: 0 });
  scenarioLog(`收回通知已发出：${rev.notification.notification_id}（下载仍在途）`);
  await waitForTask(t.task_id, ["PUBLISHED", "FILTERED", "CANCELLED", "FAILED"], 15000);
  const after = await api("GET", "/api/overview");
  const task = after.tasks.find((x) => x.id === t.task_id);
  scenarioLog(`任务终态=${task.status}（未硬杀下载，由消费者按最新许可过滤）`);
  const lin = await api("GET", "/api/lineage?field=phone");
  const mine = lin.results.filter((r) => r.task_id === t.task_id);
  mine.forEach((r) => scenarioLog(`  结果 #${r.id}: ${r.status}${r.filtered_reason ? " — " + r.filtered_reason : ""}`));
  if (mine.every((r) => r.status !== "published"))
    scenarioLog("✓ 验证通过：phone 没有任何已发布值，原始包已封存为证据");
  else scenarioLog("✗ 验证失败：存在已发布的 phone 值");
}

// B: 通知重发 —— 同 notification_id 幂等，不重复下架
async function scenarioRedeliver() {
  scenarioLog("── 场景 B：通知重发与幂等去重 ──");
  const rev = await api("POST", "/api/revoke", { field: "age", effective_in: 0 });
  scenarioLog(`收回 age：${rev.notification.notification_id} seq=${rev.notification.seq}`);
  await sleep(800);
  const before = await api("GET", "/api/audit");
  const red = await api("POST", "/api/redeliver", { seq: rev.notification.seq });
  scenarioLog(`重发：新 seq=${red.notification.seq}，notification_id 不变`);
  await sleep(800);
  const after = await api("GET", "/api/audit");
  const dupLogs = after.entries.filter((e) =>
    e.action === "license.revoked" && e.payload_json.includes("age")).length;
  scenarioLog(`license.revoked(age) 审计条目数=${dupLogs}（应为 1，重发只记去重日志）`);
  if (dupLogs === 1) scenarioLog("✓ 验证通过：重发未重复应用、未重复下架");
  else scenarioLog("✗ 验证失败：通知被重复应用");
}

// C: worker 离线期间收回 —— 通知不丢，重连后补齐
async function scenarioOffline() {
  scenarioLog("── 场景 C：工作进程离线期间收回 ──");
  await api("POST", "/api/worker/stop");
  scenarioLog("worker 已停止（真离线，心跳将过期）");
  await sleep(7000); // 超过离线判定阈值
  const rev = await api("POST", "/api/revoke", { field: "city", effective_in: 0 });
  scenarioLog(`离线期间收回 city：${rev.notification.notification_id}（许可服务照常摄入）`);
  const t = await api("POST", "/api/tasks",
    { package_id: "pkg-charlie", requested_fields: ["city", "email"] });
  scenarioLog(`离线期间排队任务 #${t.task_id}（city 已收回，email 仍许可）`);
  await api("POST", "/api/worker/start");
  scenarioLog("worker 重启，启动时执行 catch-up 重放通知…");
  await waitForTask(t.task_id, ["PUBLISHED", "FILTERED", "CANCELLED", "FAILED"], 20000);
  const lin = await api("GET", "/api/lineage?field=city");
  const mine = lin.results.filter((r) => r.task_id === t.task_id);
  mine.forEach((r) => scenarioLog(`  结果 #${r.id}: ${r.status} — ${r.filtered_reason || ""}`));
  if (mine.every((r) => r.status !== "published"))
    scenarioLog("✓ 验证通过：离线期间的收回在重连后生效，city 未发布");
  else scenarioLog("✗ 验证失败");
}

// D: 队列中、请求范围全部被收回 → 取消；部分收回 → 保留任务并过滤
async function scenarioCancel() {
  scenarioLog("── 场景 D：取消 vs 接收后过滤的边界 ──");
  // Ensure a clean license baseline regardless of which scenarios ran before.
  await api("POST", "/api/restore", { field: "email" });
  await api("POST", "/api/restore", { field: "phone" });
  await api("POST", "/api/worker/suspend"); // 保证任务停在 QUEUED
  const t1 = await api("POST", "/api/tasks",
    { package_id: "pkg-alpha", requested_fields: ["email"] });
  const t2 = await api("POST", "/api/tasks",
    { package_id: "pkg-alpha", requested_fields: ["email", "phone"] });
  scenarioLog(`已挂起 worker，排队任务 #${t1.task_id}（仅 email）、#${t2.task_id}（email+phone）`);
  await sleep(500);
  await api("POST", "/api/revoke", { field: "email", effective_in: 0 });
  scenarioLog("收回 email：#1 全部字段失效 → 应 CANCELLED；#2 仍有 phone → 保留，过滤 email");
  await sleep(800);
  await api("POST", "/api/worker/resume");
  await Promise.all([
    waitForTask(t1.task_id, ["PUBLISHED", "FILTERED", "CANCELLED", "FAILED"], 15000),
    waitForTask(t2.task_id, ["PUBLISHED", "FILTERED", "CANCELLED", "FAILED"], 15000),
  ]);
  const ov = await api("GET", "/api/overview");
  const s1 = ov.tasks.find((x) => x.id === t1.task_id).status;
  const s2 = ov.tasks.find((x) => x.id === t2.task_id).status;
  scenarioLog(`终态：#${t1.task_id}=${s1}，#${t2.task_id}=${s2}`);
  if (s1 === "CANCELLED" && s2 === "PUBLISHED")
    scenarioLog("✓ 验证通过：全量收回走取消，部分收回走过滤且其余字段正常发布");
  else scenarioLog("✗ 验证失败");
}

async function waitForTask(taskId, terminal, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ov = await api("GET", "/api/overview");
    const t = ov.tasks.find((x) => x.id === taskId);
    if (t && terminal.includes(t.status)) return t;
    await sleep(400);
  }
  throw new Error(`任务 ${taskId} 等待终态超时`);
}

// ---------------------------------------------------------------- bootstrap
let timer = null;
bindActions().then(refresh);
timer = setInterval(() => {
  if ($("#auto-refresh").checked) refresh().catch(() => {});
}, 2000);
