export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (request.method === "OPTIONS") return handleOptions(request);
      if (path === "/favicon.ico") return new Response(null, { status: 204, headers: corsHeaders(request) });
      if (path === "/auth/login" && request.method === "POST") return handleLogin(request, env);
      if (path === "/sync" && request.method === "GET") return handleSyncGet(request, env);
      if (path === "/sync" && request.method === "PUT") return handleSyncPut(request, env);
      if (path === "/db/tasks" && request.method === "GET") return handleDbTasks(request, env);
      if (path === "/auth/logout") return json({ ok: true }, { headers: clearCookieHeaders(request) }, request);
      return env.ASSETS.fetch(request);
    } catch (err) {
      return json({ ok: false, error: err?.message || "server error" }, { status: 500 }, request);
    }
  }
};

const PRIMARY_STATE_ID = "vladuka6@gmail.com";
const MIRROR_STATE_IDS = ["main"];

function corsHeaders(request) {
  const origin = request?.headers?.get("Origin") || "";
  const allowed = new Set([
    "https://bps05.fun",
    "https://bps05.vladuka6.workers.dev",
  ]);
  const headers = {
    "Access-Control-Allow-Methods": "GET, POST, PUT, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Cache-Control, Pragma",
    "Access-Control-Allow-Credentials": "true",
    "Vary": "Origin",
  };
  if (allowed.has(origin)) headers["Access-Control-Allow-Origin"] = origin;
  return headers;
}

function handleOptions(request) {
  return new Response(null, { status: 204, headers: corsHeaders(request) });
}

function json(data, init = {}, request = null) {
  const headers = new Headers(init.headers || {});
  headers.set("Content-Type", "application/json; charset=utf-8");
  headers.set("Cache-Control", "no-store");
  const cors = corsHeaders(request);
  Object.entries(cors).forEach(([key, value]) => headers.set(key, value));
  return new Response(JSON.stringify(data), { ...init, headers });
}

function cookieHeaders(request, userId) {
  return {
    ...corsHeaders(request),
    "Set-Cookie": `bps05_user=${encodeURIComponent(userId)}; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=2592000`,
    "Cache-Control": "no-store"
  };
}

function clearCookieHeaders(request) {
  return {
    ...corsHeaders(request),
    "Set-Cookie": "bps05_user=; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=0",
    "Cache-Control": "no-store"
  };
}

function rowToUser(row) {
  if (!row) return null;
  return {
    id: row.id,
    login: row.login,
    name: row.name,
    role: row.role,
    departmentId: row.department_id || null,
    active: !!row.active,
    readOnly: !!row.read_only,
  };
}

async function handleLogin(request, env) {
  const body = await request.json().catch(() => ({}));
  const login = String(body.login || "").trim();
  const pass = String(body.pass || "");
  if (!login || !pass) return json({ ok: false }, { status: 400 }, request);

  const row = await env.DB.prepare(
    "SELECT id, login, name, role, department_id, active, read_only FROM users WHERE login = ? AND pass = ? AND active = 1 LIMIT 1"
  ).bind(login, pass).first();

  if (!row) return json({ ok: false }, { status: 401 }, request);
  return json({ ok: true, user: rowToUser(row) }, { headers: cookieHeaders(request, row.id) }, request);
}

async function loadState(env) {
  let row = await env.DB.prepare("SELECT state_json FROM app_state WHERE id = ? LIMIT 1").bind(PRIMARY_STATE_ID).first();
  if (!row) row = await env.DB.prepare("SELECT state_json FROM app_state WHERE id = 'main' LIMIT 1").first();
  if (!row) return null;
  return JSON.parse(row.state_json);
}

async function handleSyncGet(request, env) {
  const state = await loadState(env);
  return json({ state }, {}, request);
}

async function handleSyncPut(request, env) {
  const body = await request.json().catch(() => ({}));
  const state = body?.state;
  if (!state || typeof state !== "object") return json({ ok: false, error: "bad state" }, { status: 400 }, request);

  const stateJson = JSON.stringify(state);
  const updatedAt = state?.sync?.updatedAt || new Date().toISOString();
  await env.DB.prepare(
    "INSERT INTO app_state(id, state_json, updated_at) VALUES(?, ?, ?) ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json, updated_at=excluded.updated_at"
  ).bind(PRIMARY_STATE_ID, stateJson, updatedAt).run();

  for (const id of MIRROR_STATE_IDS) {
    await env.DB.prepare(
      "INSERT INTO app_state(id, state_json, updated_at) VALUES(?, ?, ?) ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json, updated_at=excluded.updated_at"
    ).bind(id, stateJson, updatedAt).run();
  }

  await syncTasksTable(env, state);
  return json({ ok: true }, {}, request);
}

function toDbTask(t) {
  return {
    id: String(t.id || ""),
    type: String(t.type || "internal"),
    title: String(t.title || ""),
    description: t.description || "",
    department_id: t.departmentId || null,
    responsible_user_id: t.responsibleUserId || null,
    created_by: t.createdBy || null,
    priority: t.priority || null,
    status: t.status || "в_процесі",
    start_date: t.startDate || null,
    due_date: t.dueDate || null,
    next_control_date: t.nextControlDate || null,
    control_always: t.controlAlways ? 1 : 0,
    created_at: t.createdAt || new Date().toISOString(),
    updated_at: t.updatedAt || new Date().toISOString(),
    complexity: t.complexity || null,
    closed_at: t.closedAt || null,
    report_plan_id: t.reportPlanId || null,
    report_month: t.reportMonth || null,
    audience: t.audience || null,
    ann_order: Number.isFinite(Number(t.annOrder)) ? Number(t.annOrder) : null,
    meeting_repeat_count: Number.isFinite(Number(t.meetingRepeatCount)) ? Number(t.meetingRepeatCount) : 0,
    meeting_last_date: t.meetingLastDate || null,
    meeting_next_date: t.meetingNextDate || null,
    meeting_skip_date: t.meetingSkipDate || null,
  };
}

async function syncTasksTable(env, state) {
  const tasks = Array.isArray(state.tasks) ? state.tasks.filter(t => t && t.id) : [];
  const deleted = Array.isArray(state.deletedTaskIds) ? state.deletedTaskIds.map(String) : [];

  for (const id of deleted) {
    await env.DB.prepare("DELETE FROM tasks WHERE id = ?").bind(id).run();
  }

  const sql = `INSERT INTO tasks(
    id,type,title,description,department_id,responsible_user_id,created_by,priority,status,start_date,due_date,next_control_date,control_always,created_at,updated_at,complexity,closed_at,report_plan_id,report_month,audience,ann_order,meeting_repeat_count,meeting_last_date,meeting_next_date,meeting_skip_date
  ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  ON CONFLICT(id) DO UPDATE SET
    type=excluded.type,title=excluded.title,description=excluded.description,department_id=excluded.department_id,responsible_user_id=excluded.responsible_user_id,created_by=excluded.created_by,priority=excluded.priority,status=excluded.status,start_date=excluded.start_date,due_date=excluded.due_date,next_control_date=excluded.next_control_date,control_always=excluded.control_always,created_at=excluded.created_at,updated_at=excluded.updated_at,complexity=excluded.complexity,closed_at=excluded.closed_at,report_plan_id=excluded.report_plan_id,report_month=excluded.report_month,audience=excluded.audience,ann_order=excluded.ann_order,meeting_repeat_count=excluded.meeting_repeat_count,meeting_last_date=excluded.meeting_last_date,meeting_next_date=excluded.meeting_next_date,meeting_skip_date=excluded.meeting_skip_date`;

  for (const t of tasks) {
    const d = toDbTask(t);
    await env.DB.prepare(sql).bind(
      d.id,d.type,d.title,d.description,d.department_id,d.responsible_user_id,d.created_by,d.priority,d.status,d.start_date,d.due_date,d.next_control_date,d.control_always,d.created_at,d.updated_at,d.complexity,d.closed_at,d.report_plan_id,d.report_month,d.audience,d.ann_order,d.meeting_repeat_count,d.meeting_last_date,d.meeting_next_date,d.meeting_skip_date
    ).run();
  }
}

async function handleDbTasks(request, env) {
  const { results } = await env.DB.prepare(
    "SELECT * FROM tasks ORDER BY COALESCE(updated_at, created_at) DESC LIMIT 2000"
  ).all();
  return json({ items: results || [], tasks: results || [] }, {}, request);
}
