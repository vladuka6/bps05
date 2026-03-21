/* ===========================

   LOCAL STORAGE

=========================== */

const LS_KEY = "planner_prototype_ua_v2_full";

const THEME_KEY = "planner_theme_pref";

const SYNC_URL = "/sync";

const AUTH_LOGIN_URL = "/auth/login";

const SYNC_POLL_MS = 30000;

const SYNC_DEBOUNCE_MS = 2500;

const DEVICE_ID_KEY = "planner_device_id";

let _syncTimer = null;

let _syncInFlight = false;

let _lastPullAt = null;

let _lastPushAt = null;

let _overdueTimer = null;

let _syncReady = false;

let _syncPending = false;

let _syncInitDone = !SYNC_URL;

let DB_TASKS_CACHE = null;

let DB_TASKS_LOADING = false;

let DB_TASKS_ERROR = null;

const memoryStorage = {};

function safeGet(key){

  try{

    return localStorage.getItem(key);

  } catch{

    return Object.prototype.hasOwnProperty.call(memoryStorage, key) ? memoryStorage[key] : null;

  }

}

function safeSet(key, value){

  try{

    localStorage.setItem(key, value);

  } catch{

    memoryStorage[key] = String(value);

  }

}

function safeRemove(key){

  try{

    localStorage.removeItem(key);

  } catch{

    delete memoryStorage[key];

  }

}

let _deviceId = null;

function getDeviceId(){

  if(_deviceId) return _deviceId;

  let id = safeGet(DEVICE_ID_KEY);

  if(!id){

    id = `dev_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;

    safeSet(DEVICE_ID_KEY, id);

  }

  _deviceId = id;

  return id;

}



function kyivNow(){

  const d = new Date();

  const parts = new Intl.DateTimeFormat('uk-UA', {

    timeZone: 'Europe/Kyiv',

    year:'numeric', month:'2-digit', day:'2-digit',

    hour:'2-digit', minute:'2-digit', second:'2-digit',

    hour12:false

  }).formatToParts(d).reduce((acc,p)=>{acc[p.type]=p.value; return acc;},{});

  const iso = `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`;

  return new Date(iso);

}

function kyivDateStr(dateObj=kyivNow()){

  const y = dateObj.getFullYear();

  const m = String(dateObj.getMonth()+1).padStart(2,'0');

  const d = String(dateObj.getDate()).padStart(2,'0');

  return `${y}-${m}-${d}`;

}

function isWeekend(dateObj=kyivNow()){

  const day = dateObj.getDay(); // 0 Sun .. 6 Sat

  return day === 0 || day === 6;

}

function minutesSinceMidnight(dateObj=kyivNow()){

  return dateObj.getHours()*60 + dateObj.getMinutes();

}

const REPORT_DEADLINE_MIN = 17*60 + 30;



function uid(prefix="id"){

  return `${prefix}_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;

}

function taskDisplayFingerprint(task){

  if(!task || typeof task !== "object") return "";

  const norm = (value)=>String(value || "").trim().toLowerCase();

  return [

    norm(task.type),

    norm(task.category),

    norm(task.title),

    norm(task.description),

    norm(task.departmentId),

    norm(task.responsibleUserId),

    norm(task.createdBy),

    norm(task.status),

    norm(task.priority),

    norm(task.complexity),

    norm(task.startDate),

    norm(task.dueDate),

    norm(task.nextControlDate),

    norm(task.reportPlanId),

    norm(task.reportMonth),

    norm(task.audience),

    String(!!task.controlAlways),

  ].join("|");

}

function dedupeTasksForDisplay(tasks){

  if(!Array.isArray(tasks) || !tasks.length) return Array.isArray(tasks) ? tasks.slice() : [];



  const byFingerprint = new Map();



  for(const task of tasks){

    if(!task || typeof task !== "object"){

      continue;

    }



    const fingerprint = taskDisplayFingerprint(task);

    const prev = byFingerprint.get(fingerprint);



    if(!prev){

      byFingerprint.set(fingerprint, task);

      continue;

    }



    const prevStamp = String(prev.updatedAt || prev.createdAt || "");

    const nextStamp = String(task.updatedAt || task.createdAt || "");

    if(nextStamp >= prevStamp){

      byFingerprint.set(fingerprint, task);

    }

  }



  return Array.from(byFingerprint.values());

}

function migrateState(st){

  if(!st || typeof st !== "object") return null;



  const deletedTaskIds = Array.isArray(st.deletedTaskIds)

    ? Array.from(new Set(st.deletedTaskIds.map(String).filter(Boolean)))

    : [];

  const deletedTaskIdSet = new Set(deletedTaskIds);



  const rawTasks = Array.isArray(st.tasks) ? st.tasks : [];

  const tasks = dedupeTasksForDisplay(rawTasks.map(t=>{

    if(!t || typeof t !== "object") return t;

    const task = {...t};

    if(!task.category && (task.audience === "staff" || task.audience === "meeting")){

      task.category = "announcement";

    }

    task.controlAlways = !!task.controlAlways;

    if(task.dueDate){

      task.nextControlDate = null;

      task.controlAlways = false;

    } else if(task.controlAlways){

      task.nextControlDate = null;

    }

    if(task.category === "announcement"){

      task.complexity = null;

    } else if(!task.complexity){

      const inferred = priorityToComplexity(task.priority);

      task.complexity = inferred || "середня";

    }

    return task;

  }));



  const next = {

    version: st.version ?? 0,

    session: st.session ?? { userId: null },

    departments: Array.isArray(st.departments) ? st.departments : [],

    users: Array.isArray(st.users) ? st.users : [],

    delegations: Array.isArray(st.delegations) ? st.delegations : [],

    tasks: tasks.filter(t=>t && !deletedTaskIdSet.has(String(t.id || ""))),

    deletedTaskIds,

    taskUpdates: Array.isArray(st.taskUpdates) ? st.taskUpdates : [],

    dailyReports: Array.isArray(st.dailyReports) ? st.dailyReports : [],

    deptSummaries: Array.isArray(st.deptSummaries) ? st.deptSummaries : [],

    weeklyTasks: Array.isArray(st.weeklyTasks) ? st.weeklyTasks : [],

    recurringTemplates: Array.isArray(st.recurringTemplates) ? st.recurringTemplates : [],

    reportPlans: Array.isArray(st.reportPlans) ? st.reportPlans : [],

    sync: (st.sync && typeof st.sync === "object") ? st.sync : null,

  };

  if(Array.isArray(next.reportPlans)){

    next.reportPlans = next.reportPlans.map(p=>{

      if(!p || typeof p !== "object") return p;

      const days = Array.isArray(p.daysOfMonth) ? p.daysOfMonth : [];

      const legacy = Number(p.dayOfMonth);

      const daysOfMonth = days.length ? days : (Number.isFinite(legacy) ? [legacy] : []);

      const weekDays = Array.isArray(p.weekDays) ? p.weekDays : [];

      return {...p, daysOfMonth, weekDays};

    });

  }

  if(Array.isArray(next.users)){

    const hasViewer = next.users.some(u=>u && u.login==="viewer");

    if(!hasViewer){

      next.users.push({id:"u_viewer", login:"viewer", pass:"view", name:"Перегляд", role:"boss", departmentId:null, active:true, readOnly:true});

    }

  }





  if(next.version < 4){

    next.version = 4;

  }

  if(next.version < 5){

    next.version = 5;

  }

  if(Array.isArray(next.departments) && next.departments.length){

    const deptMap = {

      "Відділ №1":"Відділ БАС",

      "Відділ №2":"Відділ НРК",

      "Відділ №3":"Відділ МБеС",

      "Відділ №4":"Відділ БС",

      "Відділ №5":"Відділ ІОЗ",

      "Відділ №6":"Відділ КПЗБС",

      "Відділ №7":"Відділ РТБС",

      "Відділ 1":"Відділ БАС",

      "Відділ 2":"Відділ НРК",

      "Відділ 3":"Відділ МБеС",

      "Відділ 4":"Відділ БС",

      "Відділ 5":"Відділ ІОЗ",

      "Відділ 6":"Відділ КПЗБС",

      "Відділ 7":"Відділ РТБС",

      "Відділ № 1":"Відділ БАС",

      "Відділ № 2":"Відділ НРК",

      "Відділ № 3":"Відділ МБеС",

      "Відділ № 4":"Відділ БС",

      "Відділ № 5":"Відділ ІОЗ",

      "Відділ № 6":"Відділ КПЗБС",

      "Відділ № 7":"Відділ РТБС",

    };

    next.departments = next.departments.map(d=>{

      if(!d || typeof d !== "object") return d;

      const mapped = d.name ? deptMap[d.name] : null;

      const base = mapped ? {...d, name: mapped} : {...d};

      if(typeof base.note !== "string") base.note = "";

      return base;

    });

  }



  return next;

}

function loadState(){

  const raw = safeGet(LS_KEY);

  if(!raw) return null;

  try{

    const parsed = JSON.parse(raw);

    return migrateState(parsed);

  } catch{

    return null;

  }

}

function ensureSyncMeta(st){

  if(!st.sync || typeof st.sync !== "object") st.sync = {};

  if(!st.sync.deviceId) st.sync.deviceId = getDeviceId();

  if(typeof st.sync.revision !== "number") st.sync.revision = 0;

}

function stateForSync(st){

  return {...st, session: {userId: null}};

}

function markStateChanged(st){

  ensureSyncMeta(st);

  st.sync.updatedAt = nowIsoKyiv();

  st.sync.revision += 1;

}

function saveState(st, opts={}){

  DB_TASKS_CACHE = null;

  DB_TASKS_ERROR = null;

  if(!opts.skipSyncStamp){

    markStateChanged(st);

    queueSync();

  }

  safeSet(LS_KEY, JSON.stringify(st));

}

function nowIsoKyiv(){

  const d = kyivNow();

  const y = d.getFullYear();

  const m = String(d.getMonth()+1).padStart(2,'0');

  const da = String(d.getDate()).padStart(2,'0');

  const hh = String(d.getHours()).padStart(2,'0');

  const mm = String(d.getMinutes()).padStart(2,'0');

  const ss = String(d.getSeconds()).padStart(2,'0');

  return `${y}-${m}-${da} ${hh}:${mm}:${ss}`;

}

function addDays(dateStr, days){

  const [y,m,d] = dateStr.split('-').map(Number);

  const dt = new Date(y, m-1, d);

  dt.setDate(dt.getDate()+days);

  const yy = dt.getFullYear();

  const mm = String(dt.getMonth()+1).padStart(2,'0');

  const dd = String(dt.getDate()).padStart(2,'0');

  return `${yy}-${mm}-${dd}`;

}

function startOfWeek(dateStr){

  const [y,m,d] = dateStr.split("-").map(Number);

  const dt = new Date(y, m-1, d);

  const day = dt.getDay(); // 0 Sun .. 6 Sat

  const diff = (day + 6) % 7; // Monday start

  dt.setDate(dt.getDate() - diff);

  const yy = dt.getFullYear();

  const mm = String(dt.getMonth()+1).padStart(2,'0');

  const dd = String(dt.getDate()).padStart(2,'0');

  return `${yy}-${mm}-${dd}`;

}

function weekRangeFor(dateStr, offsetWeeks=0){

  const start = startOfWeek(dateStr);

  const from = addDays(start, -7*offsetWeeks);

  const to = addDays(from, 6);

  return {from, to};

}

function weeksInMonth(dateStr){

  const {from, to} = monthRangeFor(dateStr);

  let cursor = startOfWeek(from);

  const out = [];

  while(cursor <= to){

    out.push(cursor);

    cursor = addDays(cursor, 7);

  }

  return out;

}

function resolveWeeklyAnchorDate(today){

  const mode = UI.weeklyPeriodMode || "current";

  if(mode === "prev") return addDays(today, -7);

  if(mode === "next") return addDays(today, 7);

  if(mode === "custom") return UI.weeklyAnchorDate || today;

  if(mode === "month"){

    const monthStr = UI.weeklyMonth || today.slice(0,7);

    const weeks = weeksInMonth(`${monthStr}-01`);

    const idx = Math.max(1, Math.min(UI.weeklyWeekIndex || 1, weeks.length));

    UI.weeklyWeekIndex = idx;

    UI.weeklyMonth = monthStr;

    return weeks[idx - 1] || today;

  }

  return today;

}

function getWeeklySelectedRange(){

  const today = kyivDateStr();

  const anchor = resolveWeeklyAnchorDate(today);

  UI.weeklyAnchorDate = anchor;

  return weekRangeFor(anchor, 0);

}

function setWeeklyPeriodFromSelect(){

  const sel = document.getElementById("weeklyPeriod");

  const mode = sel?.value || "current";

  UI.weeklyPeriodMode = mode;

  if(mode === "current") UI.weeklyAnchorDate = kyivDateStr();

  if(mode === "prev") UI.weeklyAnchorDate = addDays(kyivDateStr(), -7);

  if(mode === "next") UI.weeklyAnchorDate = addDays(kyivDateStr(), 7);

  if(mode === "custom"){

    const v = document.getElementById("weeklyDate")?.value || kyivDateStr();

    UI.weeklyAnchorDate = v;

  }

  if(mode === "month"){

    const m = document.getElementById("weeklyMonth")?.value || kyivDateStr().slice(0,7);

    const w = Number(document.getElementById("weeklyWeekIdx")?.value || 1);

    UI.weeklyMonth = m;

    UI.weeklyWeekIndex = w;

    const weeks = weeksInMonth(`${m}-01`);

    UI.weeklyAnchorDate = weeks[Math.max(0, Math.min(w, weeks.length) - 1)] || kyivDateStr();

  }

  render();

}

function setWeeklyAnchorDateFromInput(){

  const v = document.getElementById("weeklyDate")?.value || kyivDateStr();

  UI.weeklyPeriodMode = "custom";

  UI.weeklyAnchorDate = v;

  render();

}

function setWeeklyMonthFromInput(){

  const m = document.getElementById("weeklyMonth")?.value || kyivDateStr().slice(0,7);

  UI.weeklyPeriodMode = "month";

  UI.weeklyMonth = m;

  const weeks = weeksInMonth(`${m}-01`);

  const idx = Math.max(1, Math.min(UI.weeklyWeekIndex || 1, weeks.length));

  UI.weeklyWeekIndex = idx;

  UI.weeklyAnchorDate = weeks[idx - 1] || kyivDateStr();

  render();

}

function setWeeklyWeekIndexFromSelect(){

  const w = Number(document.getElementById("weeklyWeekIdx")?.value || 1);

  UI.weeklyPeriodMode = "month";

  UI.weeklyWeekIndex = w;

  const m = UI.weeklyMonth || kyivDateStr().slice(0,7);

  const weeks = weeksInMonth(`${m}-01`);

  UI.weeklyAnchorDate = weeks[Math.max(0, Math.min(w, weeks.length) - 1)] || kyivDateStr();

  render();

}

function recurringMatchesToday(tpl, today){

  if(!tpl || !tpl.schedule) return false;

  if(tpl.lastGenerated === today) return false;

  if(tpl.schedule.type === "weekly"){

    const day = new Date(today + "T12:00:00").getDay();

    return Array.isArray(tpl.schedule.days) && tpl.schedule.days.includes(day);

  }

  if(tpl.schedule.type === "monthly"){

    const day = Number(today.slice(8,10));

    return Array.isArray(tpl.schedule.dates) && tpl.schedule.dates.includes(day);

  }

  return false;

}

function runRecurringTemplates(){

  if(!STATE.recurringTemplates) STATE.recurringTemplates = [];

  const today = kyivDateStr();

  STATE.recurringTemplates.forEach(tpl=>{

    if(!recurringMatchesToday(tpl, today)) return;

    tpl.lastGenerated = today;

    const dueDate = tpl.noDue ? null : today;

    const controlAlways = tpl.noDue ? !!tpl.controlAlways : false;

    const nextControlDate = (tpl.noDue && !controlAlways) ? (tpl.nextControlDate || null) : null;

    createTask({

      id: genTaskCode((tpl.type==="managerial") ? "T" : (tpl.type==="internal" ? "I" : "P")),

      type: tpl.type,

      title: tpl.title,

      description: tpl.description || "",

      departmentId: tpl.departmentId || null,

      responsibleUserId: tpl.responsibleUserId || "u_boss",

      complexity: tpl.complexity || "середня",

      status: "в_процесі",

      startDate: today,

      dueDate,

      nextControlDate,

      controlAlways,

      createdBy: tpl.createdBy || "u_boss",

      createdAt: nowIsoKyiv(),

      updatedAt: nowIsoKyiv(),

    }, tpl.createdBy || "u_boss");

  });

}

function daysInMonth(monthStr){

  const [y,m] = (monthStr || "").split("-").map(Number);

  if(!y || !m) return 30;

  return new Date(y, m, 0).getDate();

}

function reportPlanDateForMonth(dayOfMonth, monthStr){

  const day = Math.max(1, Math.min(Number(dayOfMonth) || 1, daysInMonth(monthStr)));

  return `${monthStr}-${String(day).padStart(2,'0')}`;

}

function reportPlanScheduleDates(plan, monthStr){

  const dates = new Set();

  const days = Array.isArray(plan?.daysOfMonth) ? plan.daysOfMonth : (Number.isFinite(Number(plan?.dayOfMonth)) ? [Number(plan.dayOfMonth)] : []);

  days.forEach(d=>{

    const date = reportPlanDateForMonth(d, monthStr);

    if(date) dates.add(date);

  });

  const weekDays = Array.isArray(plan?.weekDays) ? plan.weekDays : [];

  if(weekDays.length){

    const [y,m] = monthStr.split("-").map(Number);

    const total = daysInMonth(monthStr);

    for(let d=1; d<=total; d++){

      const dow = new Date(y, m-1, d).getDay();

      if(weekDays.includes(dow)){

        dates.add(`${monthStr}-${String(d).padStart(2,'0')}`);

      }

    }

  }

  return Array.from(dates).sort();

}

function reportPlanTaskDate(t){

  if(!t) return null;

  if(t.reportPlanDate) return t.reportPlanDate;

  if(t.dueDate) return splitDateTime(t.dueDate).date || toDateOnly(t.dueDate);

  if(t.startDate) return t.startDate;

  return null;

}

function reportPlanTaskMatches(t, planId, monthStr, deptId, date){

  if(!t) return false;

  if(t.reportPlanId !== planId) return false;

  if(t.reportMonth !== monthStr) return false;

  if(t.departmentId !== deptId) return false;

  const tDate = reportPlanTaskDate(t);

  return tDate === date;

}

function getReportPlanOccurrences(monthStr){

  const plans = (STATE.reportPlans || []);

  const tasks = STATE.tasks.filter(t=>t.reportPlanId && t.reportMonth === monthStr);

  const taskMap = new Map();

  tasks.forEach(t=>{

    const date = reportPlanTaskDate(t);

    if(!date) return;

    const key = `${t.reportPlanId}__${t.departmentId || ""}__${date}`;

    if(!taskMap.has(key)) taskMap.set(key, t);

  });

  const list = [];

  plans.forEach(plan=>{

    const deptIds = Array.isArray(plan.deptIds) ? plan.deptIds : [];

    const dates = reportPlanScheduleDates(plan, monthStr);

    dates.forEach(date=>{

      deptIds.forEach(deptId=>{

        const key = `${plan.id}__${deptId}__${date}`;

        const task = taskMap.get(key) || null;

        const closeDate = task ? getCloseDateForTask(task) : null;

        const missing = !task && reportingMissingLabel(monthStr, date)==="Не створено";

        list.push({date, plan, deptId, task, closeDate, missing});

      });

    });

  });

  return list;

}

function runReportPlans(){

  if(!STATE.reportPlans) STATE.reportPlans = [];

  const today = kyivDateStr();

  const monthStr = today.slice(0,7);



  STATE.reportPlans.forEach(plan=>{

    const scheduleDates = reportPlanScheduleDates(plan, monthStr);

    if(!scheduleDates.length) return;

    const deptIds = Array.isArray(plan.deptIds) ? plan.deptIds : [];

    scheduleDates.forEach(scheduledDate=>{

      if(today < scheduledDate) return;

      deptIds.forEach(deptId=>{

        if(!deptId) return;

        const exists = STATE.tasks.some(t=>reportPlanTaskMatches(t, plan.id, monthStr, deptId, scheduledDate));

        if(exists) return;

        const headId = effectiveDeptHeadUserId(deptId);

        const respId = headId || getDeptResponsibleOptions(deptId)[0]?.id || "u_boss";

        createTask({

          id: genTaskCode("I"),

          type: "internal",

          title: plan.title,

          description: plan.description || "",

          departmentId: deptId,

          responsibleUserId: respId,

          complexity: plan.complexity || "середня",

          status: "в_процесі",

          startDate: scheduledDate,

          dueDate: scheduledDate,

          nextControlDate: null,

          controlAlways: false,

          createdBy: plan.createdBy || "u_boss",

          createdAt: nowIsoKyiv(),

          updatedAt: nowIsoKyiv(),

          reportPlanId: plan.id,

          reportMonth: monthStr,

          reportPlanDate: scheduledDate,

        }, plan.createdBy || "u_boss");

      });

    });

  });

}

function monthRangeFor(dateStr){

  const [y,m] = dateStr.split("-").map(Number);

  const from = `${y}-${String(m).padStart(2,'0')}-01`;

  const dt = new Date(y, m, 0);

  const to = `${dt.getFullYear()}-${String(dt.getMonth()+1).padStart(2,'0')}-${String(dt.getDate()).padStart(2,'0')}`;

  return {from, to};

}

function monthsBetween(from, to){

  if(!from || !to) return [];

  const [fy, fm] = from.split("-").map(Number);

  const [ty, tm] = to.split("-").map(Number);

  if(!fy || !fm || !ty || !tm) return [];

  const out = [];

  let y = fy;

  let m = fm;

  while (y < ty || (y === ty && m <= tm)){

    out.push(`${y}-${String(m).padStart(2,'0')}`);

    m += 1;

    if(m > 12){

      m = 1;

      y += 1;

    }

  }

  return out;

}



function seed(){

  const today = kyivDateStr();

  const st = {

    version: 5,

    session: { userId: null },

    departments: [

      {id:"d1", name:"Відділ БАС", note:""},

      {id:"d2", name:"Відділ НРК", note:""},

      {id:"d3", name:"Відділ МБеС", note:""},

      {id:"d4", name:"Відділ БС", note:""},

      {id:"d5", name:"Відділ ІОЗ", note:""},

      {id:"d6", name:"Відділ КПЗБС", note:""},

      {id:"d7", name:"Відділ РТБС", note:""},

    ],

    users: [

      {id:"u_boss", login:"boss", pass:"1234", name:"Керівник", role:"boss", departmentId:null, active:true},

      {id:"u_viewer", login:"viewer", pass:"view", name:"Перегляд", role:"boss", departmentId:null, active:true, readOnly:true},

      {id:"u_h2", login:"head2", pass:"1234", name:"Начальник Відділу №2", role:"dept_head", departmentId:"d2", active:true},

      {id:"u_h5", login:"head5", pass:"1234", name:"Начальник Відділу №5", role:"dept_head", departmentId:"d5", active:true},

      {id:"u_e21", login:"e21", pass:"1234", name:"Виконавець 2-1", role:"executor", departmentId:"d2", active:true},

      {id:"u_e22", login:"e22", pass:"1234", name:"Виконавець 2-2", role:"executor", departmentId:"d2", active:true},

      {id:"u_e51", login:"e51", pass:"1234", name:"Виконавець 5-1", role:"executor", departmentId:"d5", active:true},

      {id:"u_e41", login:"e41", pass:"1234", name:"Виконавець 4-1", role:"executor", departmentId:"d4", active:true},

    ],

    delegations: [],

    tasks: [],

    deletedTaskIds: [],

    taskUpdates: [],

    dailyReports: [],

    deptSummaries: [],

    weeklyTasks: [],

    recurringTemplates: [],

    reportPlans: [],

  };

  saveState(st);

  return st;

}



let STATE = loadState() || seed();

const dedupedBootTasks = dedupeTasksForDisplay(STATE.tasks);

if(dedupedBootTasks.length !== STATE.tasks.length){

  STATE = {...STATE, tasks: dedupedBootTasks};

  saveState(STATE, {skipSyncStamp:true});

}



/* ===========================

   GETTERS / HELPERS

=========================== */

function getUserById(id){ return STATE.users.find(u=>u.id===id) || null; }

function upsertStateUser(user){

  if(!user || !user.id) return null;

  const clean = {...user};

  delete clean.pass;

  const idx = STATE.users.findIndex(u=>u.id===clean.id);

  if(idx >= 0){

    STATE.users[idx] = {...STATE.users[idx], ...clean};

    return STATE.users[idx];

  }

  STATE.users.push(clean);

  return STATE.users[STATE.users.length - 1];

}

async function authenticateUser(login, pass){

  const res = await fetch(AUTH_LOGIN_URL, {

    method: "POST",

    headers: {"Content-Type":"application/json"},

    credentials: "include",

    body: JSON.stringify({login, pass})

  });

  if(!res.ok) return null;

  const data = await res.json().catch(()=>null);

  return data?.user || null;

}

function getDeptById(id){ return STATE.departments.find(d=>d.id===id) || null; }

function currentSessionUser(){ return STATE.session.userId ? getUserById(STATE.session.userId) : null; }



function htmlesc(s){

  return (s ?? "").toString()

    .replaceAll("&","&amp;").replaceAll("<","&lt;")

    .replaceAll(">","&gt;").replaceAll('"',"&quot;")

    .replaceAll("'","&#039;");

}

function richText(s){

  const safe = htmlesc(s ?? "");

  if(!safe) return "";

  let out = safe;

  out = out.replace(/__(.+?)__/g, "<u>$1</u>");

  out = out.replace(/~~(.+?)~~/g, "<s>$1</s>");

  out = out.replace(/\*\*(.+?)\*\*/g, "<b>$1</b>");

  out = out.replace(/(^|[^*])\*([^*]+)\*(?!\*)/g, "$1<i>$2</i>");

  return out;

}

function formatToolbar(textareaId, variant=""){

  const cls = variant === "inline" ? "format-chips inline" : "format-chips";

  return `

    <div class="${cls}" aria-label="Форматування тексту">

      <button class="format-chip" data-action="applyTextFormat" data-arg1="${textareaId}" data-arg2="bold" title="Жирний (**текст**)"><span class="format-ico">B</span></button>

      <button class="format-chip" data-action="applyTextFormat" data-arg1="${textareaId}" data-arg2="italic" title="Курсив (*текст*)"><span class="format-ico"><i>I</i></span></button>

      <button class="format-chip" data-action="applyTextFormat" data-arg1="${textareaId}" data-arg2="underline" title="Підкреслення (__текст__)"><span class="format-ico"><u>U</u></span></button>

      <button class="format-chip" data-action="applyTextFormat" data-arg1="${textareaId}" data-arg2="strike" title="Перекреслення (~~текст~~)"><span class="format-ico"><s>S</s></span></button>

    </div>

  `;

}

function applyTextFormat(textareaId, type){

  const el = document.getElementById(textareaId);

  if(!el) return;

  const wrap = (type==="bold") ? "**" : (type==="italic" ? "*" : (type==="strike" ? "~~" : "__"));

  const start = el.selectionStart ?? 0;

  const end = el.selectionEnd ?? 0;

  const val = el.value || "";

  if(start === end){

    el.value = val.slice(0, start) + wrap + wrap + val.slice(end);

    const caret = start + wrap.length;

    el.focus();

    el.setSelectionRange(caret, caret);

  } else {

    const selected = val.slice(start, end);

    el.value = val.slice(0, start) + wrap + selected + wrap + val.slice(end);

    el.focus();

    el.setSelectionRange(start + wrap.length, end + wrap.length);

  }

  el.dispatchEvent(new Event("input", {bubbles:true}));

}

function fmtDate(d){

  if(!d) return "—";

  const [y,m,da] = d.split("T")[0].split("-");

  return `${da}.${m}.${y}`;

}

function fmtDateShort(d){

  if(!d) return "—";

  const parts = fmtDate(d).split(".");

  return `${parts[0]}.${parts[1]}`;

}

function splitDateTime(v){

  if(!v) return {date:"", time:""};

  const [date, time] = v.split("T");

  return {date, time: time ? time.slice(0,5) : ""};

}

function joinDateTime(date, time){

  if(!date) return null;

  if(!time) return date;

  return `${date}T${time}`;

}

function dueDisplay(due){

  if(!due) return "—";

  const {date, time} = splitDateTime(due);

  if(!time) return fmtDateShort(date);

  const today = kyivDateStr();

  if(date === today) return time;

  return `${time} ${fmtDateShort(date)}`;

}

function dueTitle(due){

  if(!due) return "Без дедлайну";

  const {date, time} = splitDateTime(due);

  return time ? `${fmtDate(date)} ${time}` : fmtDate(date);

}

function dueSortKey(due){

  if(!due) return "9999-99-99T99:99";

  const {date, time} = splitDateTime(due);

  return `${date}T${time || "00:00"}`;

}

function splitDateTimeLoose(v){

  if(!v) return {date:"", time:""};

  const str = String(v).trim();

  if(str.includes("T")){

    const [date, time] = str.split("T");

    return {date, time: time ? time.slice(0,5) : ""};

  }

  if(str.includes(" ")){

    const [date, time] = str.split(" ");

    return {date, time: time ? time.slice(0,5) : ""};

  }

  if(/^\d{4}-\d{2}-\d{2}$/.test(str)) return {date: str, time:""};

  return {date:"", time:""};

}

function closeDisplay(dt){

  const {date, time} = splitDateTimeLoose(dt);

  if(!date) return "—";

  const t = time ? time.replace(":",".") : "";

  return t ? `${fmtDateShort(date)} ${t}` : fmtDateShort(date);

}

function closeTitle(dt){

  const {date, time} = splitDateTimeLoose(dt);

  if(!date) return "—";

  return time ? `${fmtDate(date)} ${time}` : `${fmtDate(date)}`;

}

function deptShortLabel(dept){

  if(!dept?.name) return "Особ.";

  if(dept.name.startsWith("Відділ ")){

    return dept.name.replace(/^Відділ\s+/,"").trim();

  }

  const m = dept.name.match(/№\s*\d+/);

  return m ? m[0].replace(/\s+/g,"") : dept.name;

}

function deptBadgeHtml(dept){

  const name = dept?.name || "Відділ";

  const short = deptShortLabel(dept);

  return `<span class="dept-badge" title="${htmlesc(name)}">${htmlesc(short)}</span>`;

}

function getDeptResponsibleOptions(deptId){

  return STATE.users.filter(x=>x.active && x.departmentId===deptId && (x.role==="executor" || x.role==="dept_head"));

}

function canEditTask(u, t){

  if(!u || !t) return false;

  if(u.role==="boss") return !u.readOnly;

  const {isDeptHeadLike} = asDeptRole(u);

  if(!isDeptHeadLike) return false;

  if(t.type==="personal" || t.type==="managerial") return false;

  return t.departmentId === u.departmentId;

}

function canDeleteTask(u, t){

  if(!u || !t) return false;

  if(u.role==="boss") return !u.readOnly;

  if(isAnnouncement(t)) return false;

  return canEditTask(u, t);

}

function shorten(s, max=70){

  s = (s || "").trim();

  if(!s) return "—";

  return s.length>max ? s.slice(0,max-1)+"…" : s;

}



/* ===========================

   DELEGATIONS (в.о.)

=========================== */

function recomputeDelegationStatuses(){

  const today = kyivDateStr();

  STATE.delegations = STATE.delegations.map(d=>{

    if(d.status==="скасовано") return d;

    if(d.startDate > today) return {...d, status:"заплановано"};

    if(d.untilCancel) return {...d, status:"активне"};

    if(d.endDate && today <= d.endDate) return {...d, status:"активне"};

    return {...d, status:"завершено"};

  });

}

function activeDelegationForDept(deptId, dateStr=kyivDateStr()){

  const list = STATE.delegations.filter(x=>x.departmentId===deptId);

  const today = dateStr;

  return list.find(x=>{

    if(x.status==="скасовано" || x.status==="завершено") return false;

    const starts = x.startDate <= today;

    if(!starts) return false;

    if(x.untilCancel) return true;

    return today <= x.endDate;

  }) || null;

}

function effectiveDeptHeadUserId(deptId){

  const del = activeDelegationForDept(deptId);

  if(del) return del.actingHeadUserId;

  const head = STATE.users.find(u=>u.role==="dept_head" && u.departmentId===deptId && u.active);

  return head ? head.id : null;

}

function isActingHead(userId){

  const today = kyivDateStr();

  return STATE.delegations.some(d=>{

    if(d.status==="скасовано" || d.status==="завершено") return false;

    if(d.actingHeadUserId !== userId) return false;

    if(d.startDate > today) return false;

    if(d.untilCancel) return true;

    return today <= d.endDate;

  });

}

function actingBannerForUser(u){

  if(!u || u.role==="boss") return null;

  const today = kyivDateStr();

  const del = STATE.delegations.find(d=>{

    if(d.status==="скасовано" || d.status==="завершено") return false;

    if(d.actingHeadUserId !== u.id) return false;

    if(d.startDate > today) return false;

    if(d.untilCancel) return true;

    return today <= d.endDate;

  });

  if(!del) return null;

  const dept = getDeptById(del.departmentId);

  const until = del.untilCancel ? "до скасування" : `до ${del.endDate}`;

  return `🟦 Ви в.о. начальника ${dept?.name ?? "відділу"} ${until}`;

}



/* ===========================

   PERMISSIONS

=========================== */

function isReadOnly(u){

  return !!u && !!u.readOnly;

}

function canWrite(u){

  return !!u && !u.readOnly;

}

function roleSubtitle(u){

  if(!u) return "";

  if(u.readOnly) return "Перегляд";

  if(u.role==="boss") return "Керівник";

  return getDeptById(u.departmentId)?.name ?? "Відділ";

}

function roleLabel(u){

  if(!u) return "";

  if(u.readOnly) return "Перегляд";

  if(u.role==="boss") return "Керівник";

  const {isDeptHeadLike} = asDeptRole(u);

  return isDeptHeadLike ? "Начальник відділу / в.о." : "Виконавець";

}

function canAccessDept(u, deptId){

  if(!u) return false;

  if(u.role==="boss") return true;

  return u.departmentId === deptId;

}

function asDeptRole(u){

  if(!u || u.role==="boss") return {scopeDeptId:null, isDeptHeadLike:false};

  const deptId = u.departmentId;

  const eff = effectiveDeptHeadUserId(deptId);

  return {scopeDeptId:deptId, isDeptHeadLike: (eff === u.id)};

}



/* ===========================

   TASK LOGIC

=========================== */

function statusLabel(s){

  const map = {

    "на_контролі":"На контролі",

    "в_процесі":"В процесі",

    "очікування":"Очікування",

    "блокер":"Блокер",

    "очікує_підтвердження":"Очікує підтвердження",

    "закрито":"Закрито",

    "скасовано":"Скасовано",

  };

  return map[s] || s;

}

function statusIcon(s){

  const map = {

    "на_контролі":"🧭",

    "в_процесі":"🔄",

    "очікування":"⏳",

    "блокер":"⛔",

    "очікує_підтвердження":"🟣",

    "закрито":"✅",

    "скасовано":"✖️",

  };

  return map[s] || "•";

}

function statusBadgeClass(s){

  if(s==="закрито") return "b-ok";

  if(s==="блокер" || s==="очікування") return "b-warn";

  if(s==="очікує_підтвердження") return "b-violet";

  if(s==="в_процесі" || s==="на_контролі") return "b-blue";

  return "";

}

const COMPLEXITY_KEYS = ["легка","середня","складна"];

const COMPLEXITY_LABELS = {

  "легка":"Легка",

  "середня":"Середня",

  "складна":"Складна"

};

function priorityToComplexity(p){

  const map = {

    "терміново":"складна",

    "високий":"складна",

    "звичайний":"середня",

    "низький":"легка",

  };

  return map[p] || null;

}

function complexityLabel(c){

  if(!c) return "—";

  return COMPLEXITY_LABELS[c] || (c[0].toUpperCase() + c.slice(1));

}

function complexityIcon(c){

  const map = {

    "легка":"Л",

    "середня":"Ср",

    "складна":"Ск",

  };

  return map[c] || "•";

}

function taskComplexity(t){

  if(!t || isAnnouncement(t)) return null;

  return t.complexity || priorityToComplexity(t.priority) || "середня";

}

function controlMeta(task){

  if(task.dueDate){

    return {label:"", title:"", exportValue:""};

  }

  if(task.controlAlways){

    return {label:"постійно", title:"Контроль: постійно", exportValue:"постійно"};

  }

  if(task.nextControlDate){

    return {

      label: fmtDateShort(task.nextControlDate),

      title: `Контроль ${fmtDate(task.nextControlDate)}`,

      exportValue: task.nextControlDate

    };

  }

  return {label:"", title:"", exportValue:""};

}

function controlSortKey(task){

  if(task.dueDate) return "9999-99-99";

  if(task.controlAlways) return "0000-00-00";

  return task.nextControlDate || "9999-99-99";

}

function controlHint(task){

  if(task.controlAlways) return "Контроль: постійно.";

  if(task.nextControlDate) return `Контроль на ${fmtDate(task.nextControlDate)}.`;

  return "";

}

function lastBlockerUpdate(task){

  const updates = STATE.taskUpdates

    .filter(u=>u.taskId===task.id && (u.status==="блокер" || u.status==="очікування"))

    .sort((a,b)=>b.at.localeCompare(a.at));

  if(!updates.length) return null;

  const withReason = updates.find(u=>isBlockerReasonNote(u.note));

  return withReason || updates[0];

}

function getCloseUpdate(task){

  if(!task) return null;

  const upd = STATE.taskUpdates

    .filter(u=>u.taskId===task.id && u.status==="закрито")

    .sort((a,b)=>b.at.localeCompare(a.at))[0];

  if(upd) return upd;

  if(task.status==="закрито") return {at: task.updatedAt || "", note: ""};

  return null;

}

function getCloseDateForTask(task){

  if(!task) return null;

  const upd = STATE.taskUpdates

    .filter(u=>u.taskId===task.id && u.status==="закрито")

    .sort((a,b)=>b.at.localeCompare(a.at))[0];

  if(upd) return toDateOnly(upd.at);

  if(task.status==="закрито") return toDateOnly(task.updatedAt);

  return null;

}

function isClosedLate(task, closeDate){

  if(!task?.dueDate || !closeDate) return false;

  const {date} = splitDateTime(task.dueDate);

  if(!date) return false;

  return closeDate > date;

}

function normalizeCloseNote(note){

  if(!note) return "";

  return String(note)

    .replace(/^Статус\s*→\s*[^:]+:\s*/i, "")

    .replace(/^Розблоковано\s*→\s*[^:]+:\s*/i, "")

    .replace(/^Закрито:\s*/i, "")

    .trim();

}

function normalizeBlockerNote(note){

  if(!note) return "";

  return String(note)

    .replace(/^Статус\s*→\s*(Блокер|Очікування)\s*:\s*/i, "")

    .replace(/^(Блокер|Очікування)\s*:\s*/i, "")

    .trim();

}

function isBlockerReasonNote(note){

  if(!note) return false;

  const n = String(note).trim().toLowerCase();

  return n.startsWith("блокер:")

    || n.startsWith("очікування:")

    || n.startsWith("статус → блокер:")

    || n.startsWith("статус -> блокер:")

    || n.startsWith("статус → очікування:")

    || n.startsWith("статус -> очікування:");

}

function isStatusChangeNote(note){

  if(!note) return false;

  const n = String(note).trim().toLowerCase();

  return n.startsWith("статус")

    || n.startsWith("блокер:")

    || n.startsWith("очікування:")

    || n.startsWith("розблоковано");

}

function isDeadlineChangeNote(note){

  if(!note) return false;

  const n = String(note).trim().toLowerCase();

  return (n.startsWith("змінено:") && n.includes("дедлайн"))

    || (n.includes("дедлайн") && n.includes("→"));

}

function isOverdue(task){

  if(!task?.dueDate) return false;

  if(task.status === "закрито" || task.status === "скасовано") return false;

  const today = kyivDateStr();

  const {date, time} = splitDateTime(task.dueDate);

  if(!date) return false;

  if(date < today) return true;

  if(date > today) return false;

  if(!time) return false;

  const now = kyivNow();

  const nowMin = now.getHours()*60 + now.getMinutes();

  const [hh, mm] = time.split(":").map(Number);

  const dueMin = (hh || 0)*60 + (mm || 0);

  return nowMin >= dueMin;

}

function isDueToday(task){

  if(!task?.dueDate) return false;

  const {date} = splitDateTime(task.dueDate);

  if(!date) return false;

  return date === kyivDateStr();

}

function needsControl(task){

  const today = kyivDateStr();

  if(task.controlAlways) return true;

  if(!task.nextControlDate) return false;

  if(task.status === "закрито" || task.status === "скасовано") return false;

  return task.nextControlDate <= today;

}

function staleTask(task, days=7){

  const updates = STATE.taskUpdates.filter(u=>u.taskId===task.id).sort((a,b)=>a.at.localeCompare(b.at));

  const lastAt = updates.length ? updates[updates.length-1].at : task.updatedAt;

  const lastDate = lastAt.slice(0,10);

  const today = kyivDateStr();

  const diff = dateDiffDays(lastDate, today);

  return diff > days && task.status !== "закрито" && task.status !== "скасовано";

}

function dateDiffDays(a,b){

  const [ay,am,ad] = a.split("-").map(Number);

  const [by,bm,bd] = b.split("-").map(Number);

  const da = new Date(ay,am-1,ad);

  const db = new Date(by,bm-1,bd);

  return Math.round((db-da)/(1000*60*60*24));

}

function getVisibleTasksForUser(u){

  if(!u) return [];

  if(u.role==="boss"){

    if(u.readOnly) return STATE.tasks.filter(t=>!isAnnouncement(t) && t.type!=="personal");

    return STATE.tasks.filter(t=>!isAnnouncement(t));

  }

  return STATE.tasks.filter(t=>!isAnnouncement(t) && t.departmentId===u.departmentId);

}

function normalizeDbTask(row){

  if(!row || typeof row !== "object") return null;

  const annOrder = row.ann_order == null ? null : Number(row.ann_order);

  const meetingRepeatCount = row.meeting_repeat_count == null ? 0 : Number(row.meeting_repeat_count);

  const category = row.audience ? "announcement" : null;

  return {

    id: row.id,

    type: row.type || "managerial",

    title: row.title || "",

    description: row.description || "",

    departmentId: row.department_id || null,

    responsibleUserId: row.responsible_user_id || null,

    createdBy: row.created_by || null,

    priority: row.priority || null,

    status: row.status || null,

    startDate: row.start_date || null,

    dueDate: row.due_date || null,

    nextControlDate: row.next_control_date || null,

    controlAlways: !!row.control_always,

    complexity: row.complexity || null,

    closedAt: row.closed_at || null,

    reportPlanId: row.report_plan_id || null,

    reportMonth: row.report_month || null,

    audience: row.audience || null,

    annOrder: Number.isFinite(annOrder) ? annOrder : null,

    meetingRepeatCount: Number.isFinite(meetingRepeatCount) ? meetingRepeatCount : 0,

    meetingLastDate: row.meeting_last_date || null,

    meetingNextDate: row.meeting_next_date || null,

    meetingSkipDate: row.meeting_skip_date || null,

    createdAt: row.created_at || null,

    updatedAt: row.updated_at || null,

    category

  };

}

async function ensureDbTasksCache(force=false){

  if(DB_TASKS_LOADING) return;

  if(!force && Array.isArray(DB_TASKS_CACHE)) return;

  DB_TASKS_LOADING = true;

  try{

    const res = await fetch("/db/tasks", { credentials: "include" });

    if(!res.ok) throw new Error(`HTTP ${res.status}`);

    const data = await res.json();

    const items = Array.isArray(data?.items) ? data.items : [];

    DB_TASKS_CACHE = items.map(normalizeDbTask).filter(Boolean);

    DB_TASKS_ERROR = null;

  } catch(err){

    DB_TASKS_ERROR = err;

  } finally {

    DB_TASKS_LOADING = false;

    if(UI.tab===ROUTES.TASKS) render();

  }

}

function getTaskSourceForView(){

  const deletedIds = new Set(Array.isArray(STATE.deletedTaskIds) ? STATE.deletedTaskIds.map(String) : []);

  if(!Array.isArray(DB_TASKS_CACHE)){

    return Array.isArray(STATE.tasks)

      ? STATE.tasks.filter(t=>t && !deletedIds.has(String(t.id || "")))

      : [];

  }



  const stateTasks = Array.isArray(STATE.tasks)

    ? STATE.tasks.filter(t=>t && !deletedIds.has(String(t.id || "")))

    : [];

  const stateById = new Map(stateTasks.map(t=>[t.id, t]));

  const merged = [];

  const seen = new Set();



  for(const dbTask of DB_TASKS_CACHE){

    if(deletedIds.has(String(dbTask?.id || ""))) continue;

    if(!dbTask || !stateById.has(dbTask.id) || seen.has(dbTask.id)) continue;

    const stateTask = stateById.get(dbTask.id);

    merged.push({

      ...dbTask,

      ...stateTask,

      category: stateTask.category || dbTask.category || null,

      audience: stateTask.audience || dbTask.audience || null,

      annOrder: stateTask.annOrder ?? dbTask.annOrder ?? null,

    });

    seen.add(dbTask.id);

  }



  for(const task of stateTasks){

    if(seen.has(task.id)) continue;

    merged.push(task);

    seen.add(task.id);

  }



  return merged;

}

function getVisibleTasksForView(u){

  const source = getTaskSourceForView();

  if(!u) return [];

  if(u.role==="boss"){

    if(u.readOnly) return source.filter(t=>!isAnnouncement(t) && t.type!=="personal");

    return source.filter(t=>!isAnnouncement(t));

  }

  return source.filter(t=>!isAnnouncement(t) && t.departmentId===u.departmentId);

}

function updateTask(taskId, patch, authorId, note){

  const idx = STATE.tasks.findIndex(t=>t.id===taskId);

  if(idx < 0) return;

  STATE.tasks[idx] = {...STATE.tasks[idx], ...patch, updatedAt: nowIsoKyiv()};

  if(Array.isArray(DB_TASKS_CACHE)){

    const dbIdx = DB_TASKS_CACHE.findIndex(t=>t.id===taskId);

    if(dbIdx >= 0){

      DB_TASKS_CACHE[dbIdx] = {

        ...DB_TASKS_CACHE[dbIdx],

        ...STATE.tasks[idx],

        category: STATE.tasks[idx].category || DB_TASKS_CACHE[dbIdx].category || null,

        audience: STATE.tasks[idx].audience || DB_TASKS_CACHE[dbIdx].audience || null,

        annOrder: STATE.tasks[idx].annOrder ?? DB_TASKS_CACHE[dbIdx].annOrder ?? null,

      };

    }

  }

  STATE.taskUpdates.push({

    id: uid("upd"),

    taskId,

    authorUserId: authorId,

    at: nowIsoKyiv(),

    note: note || "",

    status: patch.status || STATE.tasks[idx].status

  });

  saveState(STATE);

}

function createTask(task, authorId){

  if(Array.isArray(STATE.deletedTaskIds)){

    STATE.deletedTaskIds = STATE.deletedTaskIds.filter(id=>String(id)!==String(task.id));

  }

  STATE.tasks.push(task);

  if(Array.isArray(DB_TASKS_CACHE)){

    DB_TASKS_CACHE = [task, ...DB_TASKS_CACHE.filter(x=>x.id!==task.id)];

  }

  const note = task?.category === "announcement" ? "\u0421\u0442\u0432\u043e\u0440\u0435\u043d\u043e \u043e\u0433\u043e\u043b\u043e\u0448\u0435\u043d\u043d\u044f" : "\u0421\u0442\u0432\u043e\u0440\u0435\u043d\u043e \u0437\u0430\u0434\u0430\u0447\u0443";

  STATE.taskUpdates.push({

    id: uid("upd"),

    taskId: task.id,

    authorUserId: authorId,

    at: nowIsoKyiv(),

    note,

    status: task.status

  });

  saveState(STATE);

}

function genTaskCode(prefix){

  const year = kyivDateStr().slice(0,4);

  const nums = STATE.tasks

    .filter(t=>t.id.startsWith(prefix+"-"+year))

    .map(t=>Number(t.id.split("-").pop()))

    .filter(n=>Number.isFinite(n));

  const next = (nums.length ? Math.max(...nums) : 0) + 1;

  return `${prefix}-${year}-${String(next).padStart(4,'0')}`;

}

function normalizeSheetName(name){

  return (name || "Sheet")

    .replace(/[\\\/\?\*\[\]\:]/g, " ")

    .trim()

    .slice(0,31) || "Sheet";

}

function xmlEsc(s){

  return (s ?? "").toString()

    .replaceAll("&","&amp;")

    .replaceAll("<","&lt;")

    .replaceAll(">","&gt;")

    .replaceAll('"',"&quot;")

    .replaceAll("'","&apos;");

}

function toDateOnly(v){

  if(!v) return null;

  const str = String(v);

  if(/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;

  const m = str.match(/^(\d{4}-\d{2}-\d{2})/);

  return m ? m[1] : null;

}

function inRange(dateStr, from, to){

  if(!dateStr) return false;

  return dateStr >= from && dateStr <= to;

}

function taskInPeriod(task, from, to){

  const check = [

    toDateOnly(task.createdAt),

    toDateOnly(task.updatedAt),

    toDateOnly(task.startDate),

  ].filter(Boolean);

  return check.some(d=>inRange(d, from, to));

}

function taskTypeLabel(type){

  if(type==="managerial") return "Управлінська";

  if(type==="internal") return "Внутрішня";

  if(type==="personal") return "Моя задача";

  return type;

}

function isAnnouncement(t){

  return !!t && (t.category === "announcement" || t.audience === "staff" || t.audience === "meeting");

}

function announcementAudienceLabel(a){

  if(a === "meeting") return "Нарада";

  return "Особовий склад";

}

function isMeetingHiddenToday(task){

  if(!task || task.audience !== "meeting") return false;

  return task.meetingSkipDate === kyivDateStr();

}

function meetingAnnouncementMeta(task){

  if(!task || task.audience !== "meeting") return "";

  const parts = [];

  const count = Number(task.meetingRepeatCount || 0);

  if(count > 0) parts.push(`Озвучено: ${count}`);

  if(task.meetingLastDate) parts.push(`Останнє: ${fmtDateShort(task.meetingLastDate)}`);

  if(task.meetingNextDate) parts.push(`Наступне: ${fmtDateShort(task.meetingNextDate)}`);

  return parts.join(" • ");

}

function canSeeAnnouncement(u, t){

  if(!u || !isAnnouncement(t)) return false;

  if(u.role === "boss") return !(u.readOnly && t.audience === "staff");

  if(t.audience === "staff") return true;

  if(t.audience === "meeting"){

    const {isDeptHeadLike} = asDeptRole(u);

    return !!isDeptHeadLike;

  }

  return false;

}

function getVisibleAnnouncementsForUser(u){

  if(!u) return [];

  return getTaskSourceForView().filter(isAnnouncement).filter(t=>canSeeAnnouncement(u, t));

}

function taskExportRows(tasks){

  return tasks.map(t=>{

    const dept = t.departmentId ? getDeptById(t.departmentId)?.name : "Особисто";

    const resp = getUserById(t.responsibleUserId)?.name || "";

    const creator = getUserById(t.createdBy)?.name || t.createdBy || "";

    const ctrl = controlMeta(t);

    const cx = taskComplexity(t);

    const cxLabel = cx ? complexityLabel(cx) : "";

    return [

      t.id,

      t.title,

      taskTypeLabel(t.type),

      statusLabel(t.status),

      dept || "",

      resp,

      cxLabel,

      t.startDate || "",

      t.dueDate || "",

      ctrl.exportValue || "",

      toDateOnly(t.updatedAt) || "",

      creator,

    ];

  });

}

function sortedTasksForExport(tasks){

  return tasks.slice().sort((a,b)=>{

    const bucket = (t)=>{

      if(t.dueDate) return 0;

      if(["блокер","очікування"].includes(t.status)) return 1;

      if(t.nextControlDate) return 2;

      if(t.controlAlways) return 3;

      return 4;

    };

    const dateKey = (t)=>{

      if(t.dueDate) return dueSortKey(t.dueDate);

      if(t.nextControlDate) return t.nextControlDate;

      if(t.controlAlways) return "0000-00-00";

      return "9999-99-99";

    };

    const ba = bucket(a);

    const bb = bucket(b);

    if(ba!==bb) return ba - bb;

    const dka = dateKey(a);

    const dkb = dateKey(b);

    if(dka!==dkb) return dka.localeCompare(dkb);

    return (a.title || "").localeCompare(b.title || "");

  });

}

function lastUpdateByTask(tasks){

  const ids = new Set(tasks.map(t=>t.id));

  const map = {};

  STATE.taskUpdates.forEach(u=>{

    if(!ids.has(u.taskId)) return;

    if(!map[u.taskId] || (map[u.taskId].at || "") < (u.at || "")){

      map[u.taskId] = u;

    }

  });

  return map;

}

function taskExportRowsFull(tasks){

  const sorted = sortedTasksForExport(tasks);

  const lastMap = lastUpdateByTask(sorted);

  return sorted.map((t, idx)=>{

    const resp = getUserById(t.responsibleUserId)?.name || "";

    const creator = getUserById(t.createdBy)?.name || t.createdBy || "";

    const ctrl = controlMeta(t);

    const cx = taskComplexity(t);

    const cxLabel = cx ? complexityLabel(cx) : "";

    const last = lastMap[t.id];

    const lastAuthor = last ? (getUserById(last.authorUserId)?.name || last.authorUserId || "") : "";

    const lastText = last

      ? `${toDateOnly(last.at) || ""} ${lastAuthor}: ${shorten(last.note || statusLabel(last.status) || "", 80)}`

      : "";

    const updates = STATE.taskUpdates

      .filter(u=>u.taskId===t.id)

      .sort((a,b)=>(a.at || "").localeCompare(b.at || ""))

      .map(u=>{

        const au = getUserById(u.authorUserId)?.name || u.authorUserId || "";

        const note = u.note || statusLabel(u.status) || "";

        const d = toDateOnly(u.at) || "";

        return `${d} ${au}: ${note}`;

      }).join(" | ");

    const updatesShort = shorten(updates, 200);

    return [

      `${idx+1}.`,

      t.id,

      t.title,

      taskTypeLabel(t.type),

      statusLabel(t.status),

      t.startDate || "",

      t.dueDate || "",

      ctrl.exportValue || "",

      cxLabel,

      resp,

      toDateOnly(t.updatedAt) || "",

      updatesShort || lastText,

      creator,

    ];

  });

}

function autoCols(data, min=8, max=40){

  if(!data.length) return [];

  return data[0].map((_, i)=>{

    let w = min;

    data.forEach(row=>{

      const v = row[i];

      const len = (v === null || v === undefined) ? 0 : String(v).length;

      if(len > w) w = len;

    });

    return {wch: Math.min(max, Math.max(min, w + 2))};

  });

}

function buildWorksheetXmlRaw(name, rows){

  const rowsXml = rows.map(r=>`<Row>${r.map(v=>`<Cell><Data ss:Type="String">${xmlEsc(v)}</Data></Cell>`).join("")}</Row>`).join("");

  return `<Worksheet ss:Name="${xmlEsc(normalizeSheetName(name))}"><Table>${rowsXml}</Table></Worksheet>`;

}

function buildAnalyticsRows(){

  const today = kyivDateStr();

  const days = Array.from({length:7}, (_,i)=>addDays(today, -(6-i)));

  const closeDateForTask = (task)=>{

    const updates = STATE.taskUpdates

      .filter(u=>u.taskId===task.id && u.status==="закрито")

      .sort((a,b)=>b.at.localeCompare(a.at));

    if(updates[0]) return toDateOnly(updates[0].at);

    if(task.status==="закрито") return toDateOnly(task.updatedAt);

    return null;

  };

  const weekClosed = days.map(d=>{

    const count = STATE.tasks.filter(t=>closeDateForTask(t)===d).length;

    return {date:d, count};

  });

  const closedDurations = STATE.tasks

    .map(t=>{

      const closeDate = closeDateForTask(t);

      const startDate = toDateOnly(t.createdAt) || t.startDate;

      if(!closeDate || !startDate) return null;

      const daysToClose = dateDiffDays(startDate, closeDate);

      if(daysToClose < 0) return null;

      return {task:t, daysToClose};

    })

    .filter(Boolean);

  const avgClose = closedDurations.length

    ? (closedDurations.reduce((s,x)=>s+x.daysToClose, 0) / closedDurations.length).toFixed(1)

    : "—";

  const topProblems = STATE.tasks

    .filter(t=>t.status!=="закрито" && t.status!=="скасовано")

    .map(t=>{

      const blockerUpdates = STATE.taskUpdates.filter(u=>

        u.taskId===t.id

        && (u.status==="блокер" || u.status==="очікування")

        && isBlockerReasonNote(u.note)

      );

      return {task:t, count:blockerUpdates.length, last:blockerUpdates.sort((a,b)=>b.at.localeCompare(a.at))[0]};

    })

    .filter(x=>x.count>0)

    .sort((a,b)=>b.count-a.count)

    .slice(0,5);

  const deptLoad = STATE.departments.map(d=>{

    const deptTasks = STATE.tasks.filter(t=>t.departmentId===d.id);

    const active = deptTasks.filter(t=>t.status!=="закрито" && t.status!=="скасовано").length;

    const blockers = deptTasks.filter(t=>t.status==="блокер" || t.status==="очікування").length;

    const overdue = deptTasks.filter(t=>isOverdue(t)).length;

    return {dept:d, active, blockers, overdue};

  });

  const activeDeptTasks = STATE.tasks.filter(t=>t.departmentId && t.status!=="закрито" && t.status!=="скасовано");

  const recentClosed = STATE.tasks.filter(t=>t.departmentId && t.status==="закрито" && closeDateForTask(t) && closeDateForTask(t) >= days[0] && closeDateForTask(t) <= days[days.length-1]);

  const complexityKeys = COMPLEXITY_KEYS;

  const complexityCounts = complexityKeys.map(k=>({

    key:k,

    label: complexityLabel(k),

    count: activeDeptTasks.filter(t=>taskComplexity(t)===k).length

  }));

  const complexityOther = activeDeptTasks.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

  if(complexityOther>0){

    complexityCounts.push({key:"other", label:"Без складності", count: complexityOther});

  }

  const complexityClosed = complexityKeys.map(k=>({

    key:k,

    label: complexityLabel(k),

    count: recentClosed.filter(t=>taskComplexity(t)===k).length

  }));

  const complexityClosedOther = recentClosed.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

  if(complexityClosedOther>0){

    complexityClosed.push({key:"other", label:"Без складності", count: complexityClosedOther});

  }

  const complexityBreakdown = (list)=>{

    const rows = complexityKeys.map(k=>({

      key:k,

      label: complexityLabel(k),

      count: list.filter(t=>taskComplexity(t)===k).length

    }));

    const other = list.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

    if(other>0){

      rows.push({key:"other", label:"Без складності", count: other});

    }

    return {rows, total: list.length};

  };

  const activeDeadline = activeDeptTasks.filter(t=>!!t.dueDate);

  const activeCtrlDate = activeDeptTasks.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);

  const activeCtrlAlways = activeDeptTasks.filter(t=>!t.dueDate && !!t.controlAlways);

  const closedDeadline = recentClosed.filter(t=>!!t.dueDate);

  const closedCtrlDate = recentClosed.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);

  const closedCtrlAlways = recentClosed.filter(t=>!t.dueDate && !!t.controlAlways);

  const cxActiveDeadline = complexityBreakdown(activeDeadline);

  const cxActiveCtrlDate = complexityBreakdown(activeCtrlDate);

  const cxActiveCtrlAlways = complexityBreakdown(activeCtrlAlways);

  const cxClosedDeadline = complexityBreakdown(closedDeadline);

  const cxClosedCtrlDate = complexityBreakdown(closedCtrlDate);

  const cxClosedCtrlAlways = complexityBreakdown(closedCtrlAlways);



  const rows = [];

  rows.push([`АНАЛІТИКА (останні 7 днів)`]);

  rows.push([]);

  rows.push(["Графік закриття задач"]);

  rows.push(["Дата","Кількість"]);

  weekClosed.forEach(x=>rows.push([fmtDate(x.date), x.count]));

  rows.push([]);

  rows.push(["Середній час закриття (днів)", avgClose]);

  rows.push([]);

  rows.push(["Топ проблем"]);

  rows.push(["Задача","Відділ","К-сть блокерів","Останнє"]);

  topProblems.forEach(x=>{

    const dept = x.task.departmentId ? getDeptById(x.task.departmentId)?.name : "Особисто";

    const note = x.last?.note ? shorten(normalizeBlockerNote(x.last.note), 80) : "";

    rows.push([x.task.title, dept || "", x.count, note]);

  });

  rows.push([]);

  rows.push(["Навантаження по відділах"]);

  rows.push(["Відділ","Активні","Блокери","Прострочені"]);

  deptLoad.forEach(x=>rows.push([x.dept.name, x.active, x.blockers, x.overdue]));

  rows.push([]);

  rows.push(["Складність активних", activeDeptTasks.length]);

  rows.push(["Складність","К-сть"]);

  complexityCounts.forEach(x=>rows.push([x.label, x.count]));

  rows.push([]);

  rows.push(["Активні з дедлайном — складність", cxActiveDeadline.total]);

  rows.push(["Складність","К-сть"]);

  cxActiveDeadline.rows.forEach(x=>rows.push([x.label, x.count]));

  rows.push([]);

  rows.push(["Активні з датою контролю — складність", cxActiveCtrlDate.total]);

  rows.push(["Складність","К-сть"]);

  cxActiveCtrlDate.rows.forEach(x=>rows.push([x.label, x.count]));

  rows.push([]);

  rows.push(["Активні на постійному контролі — складність", cxActiveCtrlAlways.total]);

  rows.push(["Складність","К-сть"]);

  cxActiveCtrlAlways.rows.forEach(x=>rows.push([x.label, x.count]));

  rows.push([]);

  rows.push(["Складність закритих (7 днів)", recentClosed.length]);

  rows.push(["Складність","К-сть"]);

  complexityClosed.forEach(x=>rows.push([x.label, x.count]));

  rows.push([]);

  rows.push(["Закриті з дедлайном — складність (7 днів)", cxClosedDeadline.total]);

  rows.push(["Складність","К-сть"]);

  cxClosedDeadline.rows.forEach(x=>rows.push([x.label, x.count]));

  rows.push([]);

  rows.push(["Закриті з датою контролю — складність (7 днів)", cxClosedCtrlDate.total]);

  rows.push(["Складність","К-сть"]);

  cxClosedCtrlDate.rows.forEach(x=>rows.push([x.label, x.count]));

  rows.push([]);

  rows.push(["Закриті на постійному контролі — складність (7 днів)", cxClosedCtrlAlways.total]);

  rows.push(["Складність","К-сть"]);

  cxClosedCtrlAlways.rows.forEach(x=>rows.push([x.label, x.count]));

  return rows;

}

function buildAnalyticsTableRows(){

  const today = kyivDateStr();

  const days = Array.from({length:7}, (_,i)=>addDays(today, -(6-i)));

  const closeDateForTask = (task)=>{

    const updates = STATE.taskUpdates

      .filter(u=>u.taskId===task.id && u.status==="закрито")

      .sort((a,b)=>b.at.localeCompare(a.at));

    if(updates[0]) return toDateOnly(updates[0].at);

    if(task.status==="закрито") return toDateOnly(task.updatedAt);

    return null;

  };

  const weekClosed = days.map(d=>{

    const count = STATE.tasks.filter(t=>closeDateForTask(t)===d).length;

    return {date:d, count};

  });

  const closedDurations = STATE.tasks

    .map(t=>{

      const closeDate = closeDateForTask(t);

      const startDate = toDateOnly(t.createdAt) || t.startDate;

      if(!closeDate || !startDate) return null;

      const daysToClose = dateDiffDays(startDate, closeDate);

      if(daysToClose < 0) return null;

      return {task:t, daysToClose};

    })

    .filter(Boolean);

  const avgClose = closedDurations.length

    ? (closedDurations.reduce((s,x)=>s+x.daysToClose, 0) / closedDurations.length).toFixed(1)

    : "—";



  const topProblems = STATE.tasks

    .filter(t=>t.status!=="закрито" && t.status!=="скасовано")

    .map(t=>{

      const blockerUpdates = STATE.taskUpdates.filter(u=>

        u.taskId===t.id

        && (u.status==="блокер" || u.status==="очікування")

        && isBlockerReasonNote(u.note)

      );

      return {task:t, count:blockerUpdates.length};

    })

    .filter(x=>x.count>0)

    .sort((a,b)=>b.count-a.count)

    .slice(0,5);



  const deptLoad = STATE.departments.map(d=>{

    const deptTasks = STATE.tasks.filter(t=>t.departmentId===d.id);

    const active = deptTasks.filter(t=>t.status!=="закрито" && t.status!=="скасовано").length;

    const blockers = deptTasks.filter(t=>t.status==="блокер" || t.status==="очікування").length;

    const overdue = deptTasks.filter(t=>isOverdue(t)).length;

    return {dept:d, active, blockers, overdue};

  });



  const activeDeptTasks = STATE.tasks.filter(t=>t.departmentId && t.status!=="закрито" && t.status!=="скасовано");

  const recentClosed = STATE.tasks.filter(t=>t.departmentId && t.status==="закрито" && closeDateForTask(t) && closeDateForTask(t) >= days[0] && closeDateForTask(t) <= days[days.length-1]);

  const complexityKeys = COMPLEXITY_KEYS;

  const complexityCounts = complexityKeys.map(k=>({

    key:k,

    label: complexityLabel(k),

    count: activeDeptTasks.filter(t=>taskComplexity(t)===k).length

  }));

  const complexityOther = activeDeptTasks.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

  if(complexityOther>0){

    complexityCounts.push({key:"other", label:"Без складності", count: complexityOther});

  }

  const complexityClosed = complexityKeys.map(k=>({

    key:k,

    label: complexityLabel(k),

    count: recentClosed.filter(t=>taskComplexity(t)===k).length

  }));

  const complexityClosedOther = recentClosed.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

  if(complexityClosedOther>0){

    complexityClosed.push({key:"other", label:"Без складності", count: complexityClosedOther});

  }

  const complexityBreakdown = (list)=>{

    const rows = complexityKeys.map(k=>({

      key:k,

      label: complexityLabel(k),

      count: list.filter(t=>taskComplexity(t)===k).length

    }));

    const other = list.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

    if(other>0){

      rows.push({key:"other", label:"Без складності", count: other});

    }

    return rows;

  };

  const activeDeadline = activeDeptTasks.filter(t=>!!t.dueDate);

  const activeCtrlDate = activeDeptTasks.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);

  const activeCtrlAlways = activeDeptTasks.filter(t=>!t.dueDate && !!t.controlAlways);

  const closedDeadline = recentClosed.filter(t=>!!t.dueDate);

  const closedCtrlDate = recentClosed.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);

  const closedCtrlAlways = recentClosed.filter(t=>!t.dueDate && !!t.controlAlways);



  const rows = [["Група","Сегмент","Показник","Значення"]];

  weekClosed.forEach(x=>rows.push(["Закриття","Дата", fmtDate(x.date), x.count]));

  rows.push(["Середній час закриття","Усереднено","Днів", avgClose]);

  topProblems.forEach(x=>rows.push(["Топ проблем","Задача", x.task.title, x.count]));

  deptLoad.forEach(x=>{

    rows.push(["Відділи", x.dept.name, "Активні", x.active]);

    rows.push(["Відділи", x.dept.name, "Блокери", x.blockers]);

    rows.push(["Відділи", x.dept.name, "Прострочені", x.overdue]);

  });

  complexityCounts.forEach(x=>rows.push(["Складність (активні)","Всі", x.label, x.count]));

  complexityClosed.forEach(x=>rows.push(["Складність (закриті 7 днів)","Всі", x.label, x.count]));

  complexityBreakdown(activeDeadline).forEach(x=>rows.push(["Активні з дедлайном","Складність", x.label, x.count]));

  complexityBreakdown(activeCtrlDate).forEach(x=>rows.push(["Активні з датою контролю","Складність", x.label, x.count]));

  complexityBreakdown(activeCtrlAlways).forEach(x=>rows.push(["Активні на постійному контролі","Складність", x.label, x.count]));

  complexityBreakdown(closedDeadline).forEach(x=>rows.push(["Закриті з дедлайном (7 днів)","Складність", x.label, x.count]));

  complexityBreakdown(closedCtrlDate).forEach(x=>rows.push(["Закриті з датою контролю (7 днів)","Складність", x.label, x.count]));

  complexityBreakdown(closedCtrlAlways).forEach(x=>rows.push(["Закриті на постійному контролі (7 днів)","Складність", x.label, x.count]));

  return rows;

}

function applyTimesFont(ws){

  if(!ws || !ws["!ref"]) return;

  const range = XLSX.utils.decode_range(ws["!ref"]);

  for(let R = range.s.r; R <= range.e.r; R++){

    for(let C = range.s.c; C <= range.e.c; C++){

      const cellRef = XLSX.utils.encode_cell({r:R,c:C});

      const cell = ws[cellRef];

      if(!cell) continue;

      cell.s = cell.s || {};

      cell.s.font = {name:"Times New Roman", sz:11, bold: R===0};

    }

  }

}

function buildWorksheetXml(name, header, rows){

  const headerXml = `<Row>${header.map(h=>`<Cell ss:StyleID="header"><Data ss:Type="String">${xmlEsc(h)}</Data></Cell>`).join("")}</Row>`;

  const rowsXml = rows.map(r=>`<Row>${r.map(v=>`<Cell><Data ss:Type="String">${xmlEsc(v)}</Data></Cell>`).join("")}</Row>`).join("");

  return `<Worksheet ss:Name="${xmlEsc(normalizeSheetName(name))}"><Table>${headerXml}${rowsXml}</Table></Worksheet>`;

}

function buildTasksWorkbookXml(sheets){

  return `<?xml version="1.0" encoding="UTF-8"?>

<?mso-application progid="Excel.Sheet"?>

<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"

 xmlns:o="urn:schemas-microsoft-com:office:office"

 xmlns:x="urn:schemas-microsoft-com:office:excel"

 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"

 xmlns:html="http://www.w3.org/TR/REC-html40">

 <Styles>

  <Style ss:ID="Default" ss:Name="Normal">

   <Alignment ss:Vertical="Bottom"/>

   <Borders/>

   <Font ss:FontName="Times New Roman" ss:Size="11"/>

   <Interior/>

   <NumberFormat/>

   <Protection/>

  </Style>

  <Style ss:ID="header">

   <Font ss:Bold="1" ss:FontName="Times New Roman"/>

   <Interior ss:Color="#DCE6F1" ss:Pattern="Solid"/>

  </Style>

 </Styles>

 ${sheets.join("\n")}

</Workbook>`;

}

function downloadExcelXml(filename, xml){

  const blob = new Blob(["\uFEFF"+xml], {type:"application/vnd.ms-excel;charset=utf-8;"});

  const href = URL.createObjectURL(blob);

  const a = document.createElement("a");

  a.href = href;

  a.download = filename;

  document.body.appendChild(a);

  a.click();

  a.remove();

  URL.revokeObjectURL(href);

}

function openTasksExportDialog(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Експорт доступний тільки керівнику.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const today = kyivDateStr();

  showSheet("Експорт задач у Excel", `

    <div class="hint">Буде сформовано книгу Excel: окремі вкладки по відділах і <b>Особисті</b>, вкладки <b>Аналітика (візуально)</b> + <b>Аналітика (таблично)</b> та вкладки <b>Звітність YYYY-MM</b> для кожного місяця у періоді. Колонка <b>Оновлення</b> містить усю історію (обрізано).</div>

    <div class="row2">

      <div class="field">

        <label>Від дати</label>

        <input id="expFrom" type="date" value="${addDays(today, -30)}" />

      </div>

      <div class="field">

        <label>До дати</label>

        <input id="expTo" type="date" value="${today}" />

      </div>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="exportTasksExcelNow">⬇️ Завантажити Excel</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function reportingMissingLabel(monthStr, scheduledDate){

  const today = kyivDateStr();

  const currentMonth = today.slice(0,7);

  const isPastMonth = monthStr < currentMonth;

  const shouldExist = isPastMonth || ((monthStr === currentMonth) && (today >= scheduledDate));

  return shouldExist ? "Не створено" : "Заплановано";

}

function reportingExportRows(monthStr){

  const plans = (STATE.reportPlans || []).slice().sort((a,b)=>{

    const da = Array.isArray(a.daysOfMonth) && a.daysOfMonth.length ? Math.min(...a.daysOfMonth) : (Number(a.dayOfMonth) || 0);

    const db = Array.isArray(b.daysOfMonth) && b.daysOfMonth.length ? Math.min(...b.daysOfMonth) : (Number(b.dayOfMonth) || 0);

    if(da !== db) return da - db;

    return (a.title || "").localeCompare(b.title || "");

  });

  const rows = [];

  plans.forEach(plan=>{

    const deptIds = Array.isArray(plan.deptIds) ? plan.deptIds : [];

    const scheduleDates = reportPlanScheduleDates(plan, monthStr);

    const planTasks = STATE.tasks.filter(t=>t.reportPlanId===plan.id && t.reportMonth===monthStr);

    const taskMap = new Map();

    planTasks.forEach(t=>{

      const d = reportPlanTaskDate(t);

      if(!d) return;

      const key = `${t.departmentId || ""}__${d}`;

      if(!taskMap.has(key)) taskMap.set(key, t);

    });

    const weekLabels = ["Нд","Пн","Вт","Ср","Чт","Пт","Сб"];

    const weekDayList = Array.isArray(plan.weekDays) ? plan.weekDays : [];

    const weekLabelText = weekDayList.length ? weekDayList.map(x=>weekLabels[x] || "").filter(Boolean).join(", ") : "";

    deptIds.forEach(deptId=>{

      const dept = getDeptById(deptId);

      scheduleDates.forEach(date=>{

        const task = taskMap.get(`${deptId}__${date}`) || null;

        const closeDate = task ? getCloseDateForTask(task) : null;

        const status = task ? statusLabel(task.status) : reportingMissingLabel(monthStr, date);

        const closedInMonth = closeDate && closeDate.startsWith(monthStr) ? "так" : "ні";

        rows.push([

          monthStr,

          plan.title || "",

          plan.description || "",

          Array.isArray(plan.daysOfMonth) && plan.daysOfMonth.length ? plan.daysOfMonth.join(",") : (Number(plan.dayOfMonth) || ""),

          weekLabelText,

          date,

          dept?.name || "",

          status,

          closeDate ? fmtDate(closeDate) : "",

          closedInMonth,

          task?.id || ""

        ]);

      });

    });

  });

  return rows;

}

function exportTasksExcelNow(){

  const from = document.getElementById("expFrom")?.value;

  const to = document.getElementById("expTo")?.value;

  if(!from || !to || from > to){

    showSheet("Помилка", `<div class="hint">Перевір період: дата “Від” має бути не пізніше “До”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const visible = getVisibleTasksForUser(currentSessionUser()).filter(t=>taskInPeriod(t, from, to));

  const header = ["№","Код","Назва","Тип","Статус","Старт","Дедлайн","Контроль","Складність","Відповідальний","Оновлено","Оновлення","Створив"];

  const reportingHeader = ["Місяць","Захід","Опис","Дні місяця","Дні тижня","Планова дата","Відділ","Статус","Дата закриття","Закрито в місяці","ID задачі"];

  const reportMonths = monthsBetween(from, to);

  const groups = [

    ...STATE.departments.map(d=>({name: d.name, id: d.id})),

    {name: "Особисті", id: "personal"}

  ];

  const announcementsAll = getVisibleAnnouncementsForUser(currentSessionUser()).filter(t=>taskInPeriod(t, from, to));

  const staffAnnouncements = announcementsAll.filter(t=>t.audience !== "meeting");

  const meetingAnnouncements = announcementsAll.filter(t=>t.audience === "meeting");

  const canUseXlsx = typeof XLSX !== "undefined" && XLSX.utils && XLSX.writeFile;

  if(canUseXlsx){

    const wb = XLSX.utils.book_new();

    const addSheet = (name, headerRow, rows)=>{

      const data = [headerRow, ...rows];

      const ws = XLSX.utils.aoa_to_sheet(data);

      ws["!cols"] = autoCols(data);

      applyTimesFont(ws);

      XLSX.utils.book_append_sheet(wb, ws, normalizeSheetName(name));

    };

    const addSheetRaw = (name, data)=>{

      const ws = XLSX.utils.aoa_to_sheet(data);

      ws["!cols"] = autoCols(data);

      applyTimesFont(ws);

      XLSX.utils.book_append_sheet(wb, ws, normalizeSheetName(name));

    };



    groups.forEach(g=>{

      const deptTasks = (g.id==="personal")

        ? visible.filter(t=>!t.departmentId)

        : visible.filter(t=>t.departmentId===g.id);

      if(!deptTasks.length) return;

      addSheet(g.name, header, taskExportRowsFull(deptTasks));

    });

    addSheet("Оголошення (особовий склад)", header, taskExportRowsFull(staffAnnouncements));

    addSheet("Оголошення (керівництво)", header, taskExportRowsFull(meetingAnnouncements));

    addSheetRaw("Аналітика (візуально)", buildAnalyticsRows());

    addSheetRaw("Аналітика (таблично)", buildAnalyticsTableRows());

    reportMonths.forEach(m=>{

      const rows = reportingExportRows(m);

      addSheet(`Звітність ${m}`, reportingHeader, rows);

    });



    XLSX.writeFile(wb, `tasks_${from}_${to}.xlsx`, {cellStyles:true});

    hideSheet();

    return;

  }



  const sheets = [];

  groups.forEach(g=>{

    const deptTasks = (g.id==="personal")

      ? visible.filter(t=>!t.departmentId)

      : visible.filter(t=>t.departmentId===g.id);

    if(!deptTasks.length) return;

    sheets.push(buildWorksheetXml(g.name, header, taskExportRowsFull(deptTasks)));

  });

  sheets.push(buildWorksheetXml("Оголошення (особовий склад)", header, taskExportRowsFull(staffAnnouncements)));

  sheets.push(buildWorksheetXml("Оголошення (керівництво)", header, taskExportRowsFull(meetingAnnouncements)));

  sheets.push(buildWorksheetXmlRaw("Аналітика (візуально)", buildAnalyticsRows()));

  sheets.push(buildWorksheetXmlRaw("Аналітика (таблично)", buildAnalyticsTableRows()));

  reportMonths.forEach(m=>{

    const rows = reportingExportRows(m);

    sheets.push(buildWorksheetXml(`Звітність ${m}`, reportingHeader, rows));

  });

  const xml = buildTasksWorkbookXml(sheets);

  downloadExcelXml(`tasks_${from}_${to}.xml`, xml);

  hideSheet();

}



/* ===========================

   REPORTS + SUMMARIES

=========================== */

function getVisibleReportsForUser(u){

  if(!u) return [];

  if(u.role==="boss") return STATE.dailyReports.slice();

  return STATE.dailyReports.filter(r=>r.departmentId===u.departmentId);

}

function submitDailyReport({userId, doneText, progressText, blockedText}){

  const u = getUserById(userId);

  const date = kyivDateStr();

  const now = kyivNow();

  const late = (!isWeekend(now)) && minutesSinceMidnight(now) > REPORT_DEADLINE_MIN;



  const existing = STATE.dailyReports.find(r=>r.userId===userId && r.reportDate===date);

  const payload = {

    id: existing?.id || uid("rep"),

    reportDate: date,

    userId,

    departmentId: u.departmentId,

    doneText, progressText, blockedText,

    submittedAt: nowIsoKyiv(),

    isLate: late

  };

  if(existing){

    const idx = STATE.dailyReports.findIndex(r=>r.id===existing.id);

    STATE.dailyReports[idx] = payload;

  } else {

    STATE.dailyReports.push(payload);

  }

  saveState(STATE);

}

function submitDeptSummary({departmentId, authorUserId, text}){

  const date = kyivDateStr();

  const existing = STATE.deptSummaries.find(s=>s.departmentId===departmentId && s.summaryDate===date);

  const payload = {

    id: existing?.id || uid("sum"),

    summaryDate: date,

    departmentId,

    authorUserId,

    text: (text || "").trim(),

    submittedAt: nowIsoKyiv()

  };

  if(existing){

    const idx = STATE.deptSummaries.findIndex(s=>s.id===existing.id);

    STATE.deptSummaries[idx] = payload;

  } else {

    STATE.deptSummaries.push(payload);

  }

  saveState(STATE);

}



/* ===========================

   MODAL

=========================== */

const root = document.getElementById("root");

const modal = document.getElementById("modal");

const sheetTitle = document.getElementById("sheetTitle");

const sheetBody = document.getElementById("sheetBody");

const sheetEl = document.getElementById("sheet");

let _sheetStack = [];

let _sheetStackOn = false;

document.getElementById("sheetClose").addEventListener("click", ()=>hideSheet());

modal.addEventListener("click", (e)=>{ if(e.target === modal) hideSheet(); });



function showSheet(title, html, opts={}){

  const useStack = !!opts.stack || _sheetStackOn;

  if(useStack && modal.classList.contains("show")){

    _sheetStack.push({

      title: sheetTitle.textContent,

      html: sheetBody.innerHTML,

      scrollTop: (sheetEl ? sheetEl.scrollTop : sheetBody.scrollTop) || 0

    });

    _sheetStackOn = true;

  }

  sheetTitle.textContent = title;

  sheetBody.innerHTML = html;

  modal.classList.add("show");

}

function hideSheet(){

  if(_sheetStackOn && _sheetStack.length){

    const prev = _sheetStack.pop();

    sheetTitle.textContent = prev.title;

    sheetBody.innerHTML = prev.html;

    if(sheetEl){

      sheetEl.scrollTop = prev.scrollTop || 0;

    } else {

      sheetBody.scrollTop = prev.scrollTop || 0;

    }

    modal.classList.add("show");

    if(_sheetStack.length === 0) _sheetStackOn = false;

    return;

  }

  _sheetStackOn = false;

  _sheetStack = [];

  modal.classList.remove("show");

  sheetBody.innerHTML = "";

}

let toastTimer = null;

function ensureToastContainer(){

  let el = document.getElementById("toastContainer");

  if(el) return el;

  el = document.createElement("div");

  el.id = "toastContainer";

  el.className = "toast-container";

  document.body.appendChild(el);

  return el;

}

function showToast(message, type="info"){

  const box = ensureToastContainer();

  box.innerHTML = `<div class="toast toast-${type}">${htmlesc(message)}</div>`;

  box.classList.add("show");

  if(toastTimer) clearTimeout(toastTimer);

  toastTimer = setTimeout(()=>{

    box.classList.remove("show");

    box.innerHTML = "";

  }, 1700);

}



/* ===========================

   ROUTING / UI

=========================== */

const ROUTES = {

  LOGIN: "login",

  CONTROL: "control",

  REPORTS: "reports",

  TASKS: "tasks",

  ANALYTICS: "analytics",

  REPORTING: "reporting",

  PLAN: "plan",

  WEEKLY: "weekly",

  PROFILE: "profile",

};

const READ_ONLY_ALLOWED_TABS = new Set([

  ROUTES.TASKS,

  ROUTES.WEEKLY,

  ROUTES.ANALYTICS,

  ROUTES.REPORTING,

  ROUTES.PLAN,

]);

function getVisibleTabsForUser(u, tabs){

  const items = Array.isArray(tabs) ? tabs : [];

  if(!u || !u.readOnly) return items;

  return items.filter(t=>t && READ_ONLY_ALLOWED_TABS.has(t.key));

}

function enforceReadOnlyNavigation(u){

  if(!u || !u.readOnly) return;

  if(UI.route === ROUTES.PROFILE) UI.route = ROUTES.TASKS;

  if(UI.tab === ROUTES.CONTROL || UI.tab === ROUTES.REPORTS || !READ_ONLY_ALLOWED_TABS.has(UI.tab)) {

    UI.tab = ROUTES.TASKS;

  }

}

function loadTheme(){

  return safeGet(THEME_KEY) === "dark" ? "dark" : "light";

}

function applyTheme(theme){

  const dark = theme === "dark";

  document.body.classList.toggle("theme-dark", dark);

  document.body.classList.toggle("theme-light", !dark);

}

let UI = {

  route: ROUTES.LOGIN,

  tab: ROUTES.TASKS,

  taskFilter: "активні",

  taskDeptFilter: "all",

  taskSearch: "",

  taskDensity: "comfortable",

  taskPersonalFilter: "all",

  taskAnnAudienceFilter: "all",

  taskIndexMap: {},

  deptOpen: {},

  analyticsShowDetails: false,

  reportFilter: "сьогодні",

  reportsControlDate: null, // NEW

  reportingMonth: null,

  planMonth: null,

  planMode: "reporting",

  weeklyPeriodMode: "current",

  weeklyAnchorDate: null,

  weeklyMonth: null,

  weeklyWeekIndex: 1,

  theme: loadTheme(),

};

function toggleTheme(){

  UI.theme = UI.theme === "dark" ? "light" : "dark";

  safeSet(THEME_KEY, UI.theme);

  applyTheme(UI.theme);

  render();

}

function toggleAnalyticsDetails(){

  UI.analyticsShowDetails = !UI.analyticsShowDetails;

  render();

}

function toggleTaskDensity(){

  UI.taskDensity = UI.taskDensity === "compact" ? "comfortable" : "compact";

  render();

}



function ensureLoggedIn(){

  const u = currentSessionUser();

  if(!u){

    UI.route = ROUTES.LOGIN;

    return false;

  }

  return true;

}

function logout(){

  STATE.session.userId = null;

  saveState(STATE);

  UI.route = ROUTES.LOGIN;

  render();

}

function setTab(tab){

  UI.tab = tab;

  render();

}

function goProfile(){

  const u = currentSessionUser();

  if(u && u.readOnly){

    logout();

    return;

  }

  UI.route = ROUTES.PROFILE;

  render();

}

function openTasksAnalytics(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  const deptId = UI.taskDeptFilter;

  if(deptId && deptId!=="all" && deptId!=="personal"){

    openDeptAnalytics(deptId, "week");

    return;

  }

  setTab(ROUTES.ANALYTICS);

}

function openSyncLogin(){

  if(!SYNC_URL) return;

  let origin = "";

  try{

    origin = new URL(SYNC_URL, window.location.href).origin;

  } catch{

    origin = window.location.origin;

  }

  const redirect = encodeURIComponent(window.location.href);

  window.location.href = `${origin}/cdn-cgi/access/login?redirect_url=${redirect}`;

}



function appShell({title, subtitle, bodyHtml, showFab, fabAction, tabs}){

  const u = currentSessionUser();

  const visibleTabs = getVisibleTabsForUser(u, tabs);

  const banner = actingBannerForUser(u);

  const date = kyivDateStr();

  const weekend = isWeekend(kyivNow());

  const deadlineInfo = weekend ? "Вихідний" : "Дедлайн звіту 17:30";

  const themeIcon = UI.theme === "dark" ? "☀️" : "🌙";

  const themeTitle = UI.theme === "dark" ? "Світла тема" : "Темна тема";

  const profileTitle = (u && u.readOnly) ? "Змінити користувача" : "Профіль";

  const profileIcon = (u && u.readOnly) ? "🚪" : "👤";

  const syncTitle = _syncReady ? "Дані завантажено" : (_syncInitDone ? "Дані не завантажено" : "Завантаження даних...");

  const syncLabel = _syncReady ? "Синхрон." : "Офлайн";

  const syncDot = SYNC_URL ? `<span class="sync-dot ${_syncReady ? "ok" : "err"}" title="${syncTitle}"></span>` : ``;

  const syncNeedsLogin = !!(SYNC_URL && _syncInitDone && !_syncReady);

  const syncBanner = syncNeedsLogin ? `

    <div class="banner sync-banner">

      <div>Синхронізація недоступна — потрібен вхід</div>

      <button class="btn ghost btn-mini" data-action="openSyncLogin">Увійти</button>

    </div>

  ` : "";

  const compactTasks = !!(

    u &&

    UI.tab===ROUTES.TASKS &&

    (

      UI.taskDensity === "compact" ||

      (u.role==="boss" && UI.taskDeptFilter && !["all","personal"].includes(UI.taskDeptFilter))

    )

  );

  const scopeAll = !!(u && u.role==="boss" && UI.tab===ROUTES.TASKS && UI.taskDeptFilter==="all");

  const scopeDept = !!(u && UI.tab===ROUTES.TASKS && (u.role!=="boss" || (u.role==="boss" && UI.taskDeptFilter && !["all","personal"].includes(UI.taskDeptFilter))));



  root.innerHTML = `

    <div class="app">

      <div class="topbar">

        <div class="topbar-inner">

          <div class="brand">

            <div class="logo">П</div>

            <div class="titleblock">

              <div class="h">${htmlesc(title)}</div>

              <div class="topbar-meta">

                <span class="header-pill header-role-pill">${htmlesc(subtitle)}</span>

                <span class="header-pill header-date-pill mono ${weekend ? "is-weekend" : ""}">${date}</span>

                <span class="header-pill header-deadline-pill ${weekend ? "is-weekend" : ""}">${deadlineInfo}</span>

              </div>

            </div>

          </div>

          <div class="top-tabs">

            ${renderTabs(visibleTabs)}

          </div>

          <div class="top-actions">

            <div class="header-sync" title="${syncTitle}">

              ${syncDot}

              <span class="header-sync-label">${syncLabel}</span>

            </div>

            <button class="iconbtn" data-action="openHelp" title="Довідка">❓</button>

            <button class="iconbtn" data-action="toggleTheme" title="${themeTitle}">${themeIcon}</button>

            <button class="iconbtn" data-action="goProfile" title="${profileTitle}">${profileIcon}</button>

          </div>

        </div>

      </div>



      ${banner ? `<div class="banner"><div>${htmlesc(banner)}</div><div class="mono">${date}</div></div>` : ``}

      ${syncBanner}



      <div class="content">${bodyHtml}</div>



      ${showFab ? `<button class="fab" id="fab">＋</button>` : ``}



      <div class="nav">

        <div class="nav-inner">

          ${renderTabs(visibleTabs)}

        </div>

      </div>

    </div>

  `;



  document.body.classList.toggle("role-boss", !!(u && u.role==="boss"));

  document.body.classList.toggle("compact-tasks", compactTasks);

  document.body.classList.toggle("scope-all", scopeAll);

  document.body.classList.toggle("scope-dept", scopeDept);

  document.body.classList.toggle("personal-announcements", (UI.tab===ROUTES.TASKS && UI.taskPersonalFilter==="announcements"));

  document.body.classList.toggle("analytics-details", (UI.tab===ROUTES.ANALYTICS && !!UI.analyticsShowDetails));

  if(showFab){

    document.getElementById("fab").addEventListener("click", fabAction);

  }

}

function renderTabs(tabs){

  const u = currentSessionUser();

  const cls = (u && u.role==="boss") ? "tabs" : "tabs three";

  return `

    <div class="${cls}">

      ${tabs.map(t=>{

        const active = (UI.tab===t.key) ? "active" : "";

        return `

        <div class="tab ${active}" data-action="setTab" data-arg1="${t.key}" aria-label="${htmlesc(t.label)}">

            <div class="ico">${t.ico}</div>

            <div class="label">${htmlesc(t.label)}</div>

          </div>

        `;

      }).join("")}

    </div>

  `;

}



/* ===========================

   LOGIN VIEW

=========================== */

function viewLogin(){

  const themeIcon = UI.theme === "dark" ? "\u2600\ufe0f" : "\ud83c\udf19";

  const themeTitle = UI.theme === "dark" ? "\u0421\u0432\u0456\u0442\u043b\u0430 \u0442\u0435\u043c\u0430" : "\u0422\u0435\u043c\u043d\u0430 \u0442\u0435\u043c\u0430";

  const syncLoading = !!SYNC_URL && !_syncInitDone;

  const syncTitle = _syncReady ? "\u0414\u0430\u043d\u0456 \u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043e" : (_syncInitDone ? "\u0414\u0430\u043d\u0456 \u043d\u0435 \u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043e" : "\u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043d\u044f \u0434\u0430\u043d\u0438\u0445...");

  const syncDot = SYNC_URL ? `<span class="sync-dot ${_syncReady ? "ok" : "err"}" title="${syncTitle}"></span>` : ``;

  document.body.classList.remove("role-boss");

  const html = `

    <div class="app">

      <div class="topbar">

        <div class="topbar-inner">

          <div class="brand">

            <div class="logo">\u041f</div>

            <div class="titleblock">

              <div class="h">\u041f\u043b\u0430\u043d\u0443\u0432\u0430\u043b\u044c\u043d\u0438\u043a</div>

              <div class="s">Secure login \u2022 Cloud sync \u2022 \u0423\u043a\u0440\u0430\u0457\u043d\u0441\u044c\u043a\u0430</div>

            </div>

          </div>

          <div class="top-actions">

            <div class="pill mono">${kyivDateStr()}</div>

            ${syncDot}

            <button class="iconbtn" data-action="openHelp" title="\u0414\u043e\u0432\u0456\u0434\u043a\u0430">?</button>

            <button class="iconbtn" data-action="toggleTheme" title="${themeTitle}">${themeIcon}</button>

          </div>

        </div>

      </div>



      <div class="content">

        <div class="card">

          <div class="card-h">

            <div class="t">\u0412\u0445\u0456\u0434</div>

            <span class="badge b-blue">Server auth</span>

          </div>

          <div class="card-b">

            <div class="field">

              <label>\u041b\u043e\u0433\u0456\u043d</label>

              <input id="login" placeholder="\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043b\u043e\u0433\u0456\u043d" autocomplete="username" ${syncLoading ? "disabled" : ""} />

            </div>

            <div class="field">

              <label>\u041f\u0430\u0440\u043e\u043b\u044c</label>

              <input id="pass" placeholder="\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043f\u0430\u0440\u043e\u043b\u044c" type="password" autocomplete="current-password" ${syncLoading ? "disabled" : ""} />

            </div>



            <div class="actions" style="margin-top:14px;">

              <button class="btn primary" id="btnLogin" ${syncLoading ? "disabled" : ""}>\u0423\u0412\u0406\u0419\u0422\u0418</button>

              <button class="btn ghost" id="btnReset">\u0421\u043a\u0438\u043d\u0443\u0442\u0438 \u043b\u043e\u043a\u0430\u043b\u044c\u043d\u0456 \u0434\u0430\u043d\u0456</button>

            </div>



            ${syncLoading ? `<div class="hint">\u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043d\u044f \u0434\u0430\u043d\u0438\u0445 \u0437 \u0445\u043c\u0430\u0440\u0438...</div>` : ``}

            <div class="hint">

              \u041e\u0431\u043b\u0456\u043a\u043e\u0432\u0456 \u0434\u0430\u043d\u0456 \u043f\u0435\u0440\u0435\u0432\u0456\u0440\u044f\u044e\u0442\u044c\u0441\u044f \u043d\u0430 \u0441\u0435\u0440\u0432\u0435\u0440\u0456. \u041f\u0430\u0440\u043e\u043b\u0456 \u0431\u0456\u043b\u044c\u0448\u0435 \u043d\u0435 \u0437\u0431\u0435\u0440\u0456\u0433\u0430\u044e\u0442\u044c\u0441\u044f \u0443 \u0444\u0440\u043e\u043d\u0442\u0435\u043d\u0434\u0456.

            </div>

          </div>

        </div>

      </div>

    </div>

  `;

  root.innerHTML = html;



  document.getElementById("btnLogin").addEventListener("click", async ()=>{

    const login = document.getElementById("login").value.trim();

    const pass = document.getElementById("pass").value.trim();

    const btn = document.getElementById("btnLogin");



    if(!login || !pass){

      showSheet("\u041f\u043e\u043c\u0438\u043b\u043a\u0430 \u0432\u0445\u043e\u0434\u0443", `<div class="hint">\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043b\u043e\u0433\u0456\u043d \u0456 \u043f\u0430\u0440\u043e\u043b\u044c.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

      return;

    }



    btn.disabled = true;

    try{

      const user = await authenticateUser(login, pass);

      if(!user || !user.id || !user.active){

        showSheet("\u041f\u043e\u043c\u0438\u043b\u043a\u0430 \u0432\u0445\u043e\u0434\u0443", `<div class="hint">\u041d\u0435\u0432\u0456\u0440\u043d\u0438\u0439 \u043b\u043e\u0433\u0456\u043d \u0430\u0431\u043e \u043f\u0430\u0440\u043e\u043b\u044c.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

        return;

      }



      upsertStateUser(user);

      recomputeDelegationStatuses();

      STATE.session.userId = user.id;

      saveState(STATE);

      UI.tab = ROUTES.TASKS;

      render();

      pullSync();

    } catch{

      showSheet("\u041f\u043e\u043c\u0438\u043b\u043a\u0430 \u0432\u0445\u043e\u0434\u0443", `<div class="hint">\u041d\u0435 \u0432\u0434\u0430\u043b\u043e\u0441\u044f \u043f\u0435\u0440\u0435\u0432\u0456\u0440\u0438\u0442\u0438 \u043e\u0431\u043b\u0456\u043a\u043e\u0432\u0456 \u0434\u0430\u043d\u0456 \u043d\u0430 \u0441\u0435\u0440\u0432\u0435\u0440\u0456.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    } finally {

      btn.disabled = false;

    }

  });



  document.getElementById("btnReset").addEventListener("click", ()=>{

    safeRemove(LS_KEY);

    STATE = seed();

    render();

    pullSync();

    showSheet("\u0413\u043e\u0442\u043e\u0432\u043e", `<div class="hint">\u041b\u043e\u043a\u0430\u043b\u044c\u043d\u0456 \u0434\u0430\u043d\u0456 \u0441\u043a\u0438\u043d\u0443\u0442\u043e. \u041e\u0431\u043b\u0456\u043a\u043e\u0432\u0456 \u0437\u0430\u043f\u0438\u0441\u0438 \u0442\u0430 \u0434\u0430\u043d\u0456 \u0431\u0443\u0434\u0435 \u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043e \u0437 \u0441\u0435\u0440\u0432\u0435\u0440\u0430.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

  });

}



/* ===========================

   CONTROL VIEW

=========================== */

function viewControl(){

  if(!ensureLoggedIn()) return viewLogin();

  recomputeDelegationStatuses();



  const u = currentSessionUser();

  UI.tab = ROUTES.CONTROL;



  const today = kyivDateStr();

  const weekend = isWeekend(kyivNow());



  // Блок "Не здали до 17:30" прибрано з контролю.



  const tasksVis = getVisibleTasksForUser(u);

  const requestClose = tasksVis.filter(t=>t.type==="managerial" && t.status==="очікує_підтвердження");

  const overdue = tasksVis.filter(t=>isOverdue(t));

  const notBlocked = (t)=>!["блокер","очікування"].includes(t.status);

  const isDeptTask = (t)=>!!t.departmentId;

  const controlTasksDate = tasksVis.filter(t=>t.nextControlDate && !t.controlAlways && notBlocked(t) && isDeptTask(t));

  const controlTasksAlways = tasksVis.filter(t=>t.controlAlways && !t.nextControlDate && notBlocked(t) && isDeptTask(t));

  const deadlineTasks = tasksVis.filter(t=>t.dueDate && isDeptTask(t) && t.status!=="закрито" && t.status!=="скасовано");

  const controlByDeptDate = (u.role==="boss")

    ? STATE.departments.map(d=>{

        const list = controlTasksDate.filter(t=>t.departmentId===d.id);

        return {dept:d, count:list.length};

      }).filter(x=>x.count>0)

    : [];

  const controlByDeptAlways = (u.role==="boss")

    ? STATE.departments.map(d=>{

        const list = controlTasksAlways.filter(t=>t.departmentId===d.id);

        return {dept:d, count:list.length};

      }).filter(x=>x.count>0)

    : [];

  const controlByDeptDeadline = (u.role==="boss")

    ? STATE.departments.map(d=>{

        const list = deadlineTasks.filter(t=>t.departmentId===d.id);

        return {dept:d, count:list.length};

      }).filter(x=>x.count>0)

    : [];

  const blockers = tasksVis.filter(t=>["блокер","очікування"].includes(t.status));

  const stale = tasksVis.filter(t=>staleTask(t, 7));

  const activeDelegations = (u.role==="boss") ? STATE.delegations.filter(d=>d.status==="активне") : [];



  const {isDeptHeadLike} = asDeptRole(u);



  let summaryBadge = "";

  if(u.role!=="boss"){

    const sum = STATE.deptSummaries.find(s=>s.departmentId===u.departmentId && s.summaryDate===today);

    summaryBadge = sum ? `<span class="badge b-ok">✅ Підсумок подано</span>` : `<span class="badge b-warn">🟡 Підсумку немає</span>`;

  }



  const overview = `

    <div class="desk-overview">

      <div class="ov-item ov-warn" data-action="openTaskList" data-arg1="блокери">

        <div class="k">⛔ Блокери / очікування</div>

        <div class="v mono">${blockers.length}</div>

      </div>

      <div class="ov-item ov-danger" data-action="openTaskList" data-arg1="прострочені">

        <div class="k">🟠 Прострочені</div>

        <div class="v mono">${overdue.length}</div>

      </div>

      <div class="ov-item ov-violet" data-action="openTaskList" data-arg1="очікує_підтвердження">

        <div class="k">🟣 Очікує підтвердження</div>

        <div class="v mono">${requestClose.length}</div>

      </div>

    </div>

  `;



  const body = `

    ${overview}

    <div class="card">

      <div class="card-h">

        <div class="t">Контроль</div>

        <span class="badge control-day-badge ${weekend ? "b-warn":"b-blue"}">${weekend ? "Вихідний" : "Будній"}</span>

      </div>

      <div class="card-b">

        <div class="statlist">

          ${u.role==="boss" ? `

          <div class="stat" data-action="openControlByDept">

            <div class="l">

              <div class="k">🎯 <span class="qa-full">На контролі по відділах</span><span class="qa-short">По відділам</span></div>

              <div class="d control-lines">

                ${

                  (!controlByDeptDate.length && !controlByDeptAlways.length && !controlByDeptDeadline.length)

                    ? `<span>Немає задач на контролі</span>`

                    : ``

                }

              </div>

            </div>

            <div class="r control-counts">

              <span class="pill mono">⏱ ${deadlineTasks.length}</span>

              <span class="pill mono">🗓 ${controlTasksDate.length}</span>

              <span class="pill mono">🎯 ${controlTasksAlways.length}</span>

            </div>

          </div>

          ` : ``}



          <div class="stat" data-action="openTaskList" data-arg1="очікує_підтвердження">

            <div class="l">

              <div class="k">🟣 Очікує підтвердження</div>

              <div class="d">Управлінські задачі</div>

            </div>

            <div class="r"><span class="mono">${requestClose.length}</span> ›</div>

          </div>



          <div class="stat" data-action="openTaskList" data-arg1="прострочені">

            <div class="l">

              <div class="k">🟠 Прострочені задачі</div>

              <div class="d">Є дедлайн і він минув</div>

            </div>

            <div class="r"><span class="mono">${overdue.length}</span> ›</div>

          </div>



          <div class="stat" data-action="openTaskList" data-arg1="блокери">

            <div class="l">

              <div class="k">🟡 Блокери / очікування</div>

              <div class="d">Потребує уваги</div>

            </div>

            <div class="r"><span class="mono">${blockers.length}</span> ›</div>

          </div>



          <div class="stat" data-action="openTaskList" data-arg1="без_оновлень">

            <div class="l">

              <div class="k">⏳ Без оновлення &gt; 7 днів</div>

              <div class="d">Довгі задачі “висять”</div>

            </div>

            <div class="r"><span class="mono">${stale.length}</span> ›</div>

          </div>



          ${u.role==="boss" ? `

          <div class="stat" data-action="openDelegations">

            <div class="l">

              <div class="k">🧩 Активні заміщення (в.о.)</div>

              <div class="d">Хто зараз виконує обов’язки</div>

            </div>

            <div class="r"><span class="mono">${activeDelegations.length}</span> ›</div>

          </div>

          ` : `

          <div class="stat" data-action="openDeptPeople">

            <div class="l">

              <div class="k">👥 Люди / звітність сьогодні</div>

              <div class="d">Хто здав / не здав</div>

            </div>

            <div class="r">›</div>

          </div>

          `}

        </div>

      </div>

    </div>



    <div class="card">

      <div class="card-h">

        <div class="t">Швидкі дії</div>

      </div>

      <div class="card-b">

        <div class="actions control-actions">

          ${u.role==="boss" ? `<button class="btn primary" data-action="openDelegations">🧩 Заміщення (в.о.)</button>` : ``}

          <button class="btn ghost" data-action="openAbout">ℹ️ Про прототип</button>

          <button class="btn danger" data-action="logout">🚪 Вийти</button>

        </div>



        ${u.role!=="boss" ? `` : ``}

      </div>

    </div>

  `;



  const tabs = (u.role==="boss")

    ? [

      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

      {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

      {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

      {key:ROUTES.PLAN, label:"План", ico:"📅"},

    ]

    : [

      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

    ];



  const subtitle = roleSubtitle(u);

  const fabAction = ()=>{

    if(u.role==="boss"){

      showSheet("Додати", `

        <div class="actions">

          <button class="btn primary" data-action="hideThen" data-next="openCreateTask" data-arg1="personal">➕ Моя задача</button>

          <button class="btn ghost" data-action="hideThen" data-next="openCreateTask" data-arg1="managerial">➕ Управлінська</button>

        </div>

        <div class="sep"></div>

        <button class="btn ghost" data-action="hideSheet">Закрити</button>

      `);

    } else {

      openCreateTask('internal');

    }

  };



  appShell({

    title: "Контроль",

    subtitle,

    bodyHtml: body,

    showFab: !u.readOnly,

    fabAction,

    tabs

  });

}



/* ===========================

   DEPT PEOPLE (Начальник/в.о.)

=========================== */

function openDeptPeople(){

  const u = currentSessionUser();

  const {isDeptHeadLike} = asDeptRole(u);

  if(u.role==="boss"){

    showSheet("Люди/штат", `<div class="hint">Цей екран потрібен саме для начальника відділу (або в.о.). У керівника є “👥 Люди” у вкладці “Звіти”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(!isDeptHeadLike){

    showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може переглядати “Люди/штат”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const deptId = u.departmentId;

  const dept = getDeptById(deptId);

  const today = kyivDateStr();

  const weekend = isWeekend(kyivNow());



  const people = STATE.users

    .filter(x=>x.active && x.departmentId===deptId)

    .sort((a,b)=>a.role.localeCompare(b.role));



  const repsToday = STATE.dailyReports.filter(r=>r.reportDate===today && r.departmentId===deptId);



  const rows = people.map(p=>{

    const expected = (p.role==="executor") ? (!weekend) : false;

    const rep = repsToday.find(r=>r.userId===p.id) || null;



    let badge = `<span class="badge">—</span>`;

    if(expected){

      if(!rep) badge = `<span class="badge b-danger">🔴 НЕ ЗДАВ</span>`;

      else badge = rep.isLate ? `<span class="badge b-warn">🟡 ПІЗНО</span>` : `<span class="badge b-ok">✅ ВЧАСНО</span>`;

    } else {

      if(rep) badge = rep.isLate ? `<span class="badge b-warn">🟡 (є звіт, пізно)</span>` : `<span class="badge b-ok">✅ (є звіт)</span>`;

      else badge = `<span class="badge">не очікується</span>`;

    }



    const roleLabel = (p.role==="dept_head") ? "начальник" : (p.role==="executor" ? "виконавець" : p.role);

    const acting = isActingHead(p.id) ? " (в.о.)" : "";



    return `

      <div class="item" style="cursor:default;">

        <div class="row">

          <div>

            <div class="name">${htmlesc(p.name)}${acting}</div>

            <div class="sub">

              <span class="pill">${roleLabel}</span>

              ${badge}

              ${rep ? `<span class="pill mono">${htmlesc(rep.submittedAt.slice(11,16))}</span>` : ``}

            </div>

            ${rep ? `<div class="hint" style="margin-top:8px;"><b>Коротко:</b> ${htmlesc(shorten(rep.doneText, 90))}</div>` : ``}

          </div>

        </div>

      </div>

    `;

  }).join("");



  showSheet(`Люди/штат — ${dept?.name ?? ""}`, `

    <div class="hint">

      Показано статус звітності за <span class="mono">${fmtDate(today)}</span>.

      ${weekend ? "Сьогодні вихідний — звіти не обов’язкові." : "Очікуємо звіт лише від виконавців."}

    </div>

    <div class="sep"></div>

    ${rows || `<div class="hint">Немає людей у відділі.</div>`}

    <div class="sep"></div>

    <div class="actions">

      <button class="btn ghost" data-action="hideSheet">Закрити</button>

    </div>

  `);

}



/* ===========================

   DEPT PEOPLE (КЕРІВНИК) — NEW: по даті

=========================== */

function openDeptPeopleBoss(deptId, dateStr){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Цей екран доступний тільки керівнику.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const dept = getDeptById(deptId);

  const day = dateStr || kyivDateStr();

  const weekend = isWeekend(new Date(day + "T12:00:00"));



  const people = STATE.users

    .filter(x=>x.active && x.departmentId===deptId)

    .sort((a,b)=>a.role.localeCompare(b.role));



  const reps = STATE.dailyReports.filter(r=>r.reportDate===day && r.departmentId===deptId);



  const rows = people.map(p=>{

    const expected = (p.role==="executor") ? (!weekend) : false;

    const rep = reps.find(r=>r.userId===p.id) || null;



    let badge = `<span class="badge">—</span>`;

    if(expected){

      if(!rep) badge = `<span class="badge b-danger">🔴 НЕ ЗДАВ</span>`;

      else badge = rep.isLate ? `<span class="badge b-warn">🟡 ПІЗНО</span>` : `<span class="badge b-ok">✅ ВЧАСНО</span>`;

    } else {

      if(rep) badge = rep.isLate ? `<span class="badge b-warn">🟡 (є звіт, пізно)</span>` : `<span class="badge b-ok">✅ (є звіт)</span>`;

      else badge = `<span class="badge">не очікується</span>`;

    }



    const roleLabel = (p.role==="dept_head") ? "начальник" : (p.role==="executor" ? "виконавець" : p.role);

    const acting = isActingHead(p.id) ? " (в.о.)" : "";



    return `

      <div class="item" style="cursor:default;">

        <div class="row">

          <div>

            <div class="name">${htmlesc(p.name)}${acting}</div>

            <div class="sub">

              <span class="pill">${roleLabel}</span>

              ${badge}

              ${rep ? `<span class="pill mono">${htmlesc(rep.submittedAt.slice(11,16))}</span>` : ``}

            </div>

            ${rep ? `<div class="hint" style="margin-top:8px;"><b>Коротко:</b> ${htmlesc(shorten(rep.doneText, 90))}</div>` : ``}

          </div>

        </div>

      </div>

    `;

  }).join("");



  showSheet(`Люди — ${deptShortLabel(dept)}`, `

    <div class="hint">

      Дата: <span class="mono">${fmtDate(day)}</span>.

      ${weekend ? "Вихідний — звіти не обов’язкові." : "Очікуємо звіт від виконавців."}

    </div>

    <div class="sep"></div>

    ${rows || `<div class="hint">Немає людей у відділі.</div>`}

    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}



function openDeptAnalytics(deptId, periodKey="week"){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  const dept = getDeptById(deptId);

  if(!dept) return;



  const today = kyivDateStr();

  const ranges = {

    week: {...weekRangeFor(today, 0), label: "Цей тиждень"},

    prev_week: {...weekRangeFor(today, 1), label: "Попередній тиждень"},

    month: {...monthRangeFor(today), label: "Цей місяць"},

  };

  const range = ranges[periodKey] || ranges.week;



  const tasks = STATE.tasks.filter(t=>t.departmentId===deptId);

  const taskIds = new Set(tasks.map(t=>t.id));

  const allUpdatesByTask = {};

  STATE.taskUpdates

    .filter(u=>taskIds.has(u.taskId))

    .sort((a,b)=>b.at.localeCompare(a.at))

    .forEach(u=>{

      if(!allUpdatesByTask[u.taskId]) allUpdatesByTask[u.taskId] = [];

      allUpdatesByTask[u.taskId].push(u);

    });

  const updatesInPeriod = STATE.taskUpdates

    .filter(u=>taskIds.has(u.taskId))

    .filter(u=>{

      const d = toDateOnly(u.at);

      return d && inRange(d, range.from, range.to);

    })

    .sort((a,b)=>b.at.localeCompare(a.at));

  const updatesByTask = {};

  updatesInPeriod.forEach(u=>{

    if(!updatesByTask[u.taskId]) updatesByTask[u.taskId] = [];

    updatesByTask[u.taskId].push(u);

  });

  const activeNow = tasks.filter(t=>t.status!=="закрито" && t.status!=="скасовано");

  const closedInPeriod = tasks.filter(t=>{

    const closeDate = getCloseDateForTask(t);

    return closeDate && inRange(closeDate, range.from, range.to);

  });

  const closedSorted = closedInPeriod

    .slice()

    .sort((a,b)=>(getCloseDateForTask(b) || "").localeCompare(getCloseDateForTask(a) || ""));



  const closedWithDue = closedInPeriod.filter(t=>!!t.dueDate);

  const late = closedWithDue.filter(t=>isClosedLate(t, getCloseDateForTask(t)));

  const onTime = closedWithDue.length - late.length;

  const noDue = closedInPeriod.length - closedWithDue.length;



  const pct = (n, d)=> d ? Math.round((n/d)*100) : 0;

  const onTimePct = pct(onTime, closedWithDue.length);

  const latePct = pct(late.length, closedWithDue.length);



  const updateStats = tasks.map(t=>{

    const list = updatesByTask[t.id] || [];

    const allList = allUpdatesByTask[t.id] || [];

    const last = list[0] || null;

    const statusChanges = list.filter(u=>isStatusChangeNote(u.note)).length;

    const editChanges = list.filter(u=>String(u.note||"").trim().toLowerCase().startsWith("змінено:")).length;

    const blockerReasons = list.filter(u=>isBlockerReasonNote(u.note)).length;

    const deadlineChanges = list.filter(u=>isDeadlineChangeNote(u.note)).length;

    return {

      task:t,

      total:list.length,

      totalAll: allList.length,

      statusChanges,

      editChanges,

      blockerReasons,

      deadlineChanges,

      last,

      lastAll: allList[0] || null,

    };

  });

  const totalUpdates = updatesInPeriod.length;

  const totalStatusChanges = updateStats.reduce((s,x)=>s+x.statusChanges,0);

  const totalEdits = updateStats.reduce((s,x)=>s+x.editChanges,0);

  const totalBlockerReasons = updateStats.reduce((s,x)=>s+x.blockerReasons,0);



  const activeOverdue = activeNow.filter(t=>isOverdue(t)).length;

  const activeBlockers = activeNow.filter(t=>["блокер","очікування"].includes(t.status)).length;

  const staleActive = activeNow.filter(t=>staleTask(t,7)).length;



  const topChanged = updateStats

    .filter(x=>x.total>0)

    .sort((a,b)=>b.total-a.total)

    .slice(0,8);



  const listHtml = closedSorted.length

    ? `<ul class="report-list">` + closedSorted.map(t=>{

        const closeDate = getCloseDateForTask(t);

        const dueDate = t.dueDate ? splitDateTime(t.dueDate).date : "";

        const lateFlag = dueDate && closeDate && closeDate > dueDate;

        const stats = updateStats.find(x=>x.task.id===t.id);

        const lastNote = stats?.last?.note ? shorten(normalizeCloseNote(stats.last.note), 80) : "";

        const descShort = t.description ? shorten(t.description, 80) : "";

        return `

          <li>

            <div class="report-line">

              <span class="report-strong">${htmlesc(t.title)}</span>

              ${lateFlag ? `<span class="badge b-danger">прострочено</span>` : (dueDate ? `<span class="badge b-ok">в строк</span>` : ``)}

            </div>

            <div class="report-meta">Закрито: <span class="mono">${fmtDate(closeDate)}</span>${dueDate ? ` • Дедлайн: <span class="mono">${fmtDate(dueDate)}</span>` : " • Без дедлайну"}</div>

            ${descShort ? `<div class="report-meta">Опис: ${htmlesc(descShort)}</div>` : ``}

            <div class="report-meta">Оновлень за період: <b>${stats?.total || 0}</b> • статусів: <b>${stats?.statusChanges || 0}</b>${lastNote ? ` • останнє: ${htmlesc(lastNote)}` : ""}</div>

          </li>

        `;

      }).join("") + `</ul>`

    : `<div class="hint">Немає виконаних задач за період.</div>`;



  const changesHtml = topChanged.length

    ? `<ul class="report-list">` + topChanged.map(x=>{

        const lastNote = x.last?.note ? shorten(x.last.note, 80) : "";

        return `

          <li>

            <div class="report-line">

              <span class="report-strong">${htmlesc(x.task.title)}</span>

            </div>

            <div class="report-meta">Оновлень: <b>${x.total}</b> • статусів: <b>${x.statusChanges}</b> • змін: <b>${x.editChanges}</b>${lastNote ? ` • останнє: ${htmlesc(lastNote)}` : ""}</div>

          </li>

        `;

      }).join("") + `</ul>`

    : `<div class="hint">Немає змін за період.</div>`;



  const recentChanges = updatesInPeriod.length

    ? `<ul class="report-list">` + updatesInPeriod.slice(0,12).map(u=>{

        const task = tasks.find(t=>t.id===u.taskId);

        const who = getUserById(u.authorUserId)?.name || "—";

        return `

          <li>

            <div class="report-line">

              <span class="mono">${htmlesc(u.at)}</span>

              <span class="report-strong">${htmlesc(task?.title || u.taskId)}</span>

              <span class="report-meta">(${htmlesc(who)})</span>

            </div>

            <div class="report-meta">${htmlesc(shorten(u.note || "", 120))}</div>

          </li>

        `;

      }).join("") + `</ul>`

    : `<div class="hint">Немає оновлень за період.</div>`;



  const allTasksSorted = tasks.slice().sort((a,b)=>{

    const aClosed = (a.status==="закрито" || a.status==="скасовано") ? 1 : 0;

    const bClosed = (b.status==="закрито" || b.status==="скасовано") ? 1 : 0;

    if(aClosed !== bClosed) return aClosed - bClosed;

    const ad = dueSortKey(a.dueDate || "");

    const bd = dueSortKey(b.dueDate || "");

    return ad.localeCompare(bd);

  });

  const allTasksHtml = allTasksSorted.length

    ? `<ul class="report-list">` + allTasksSorted.map(t=>{

        const stats = updateStats.find(x=>x.task.id===t.id);

        const closeDate = getCloseDateForTask(t);

        const dueDate = t.dueDate ? splitDateTime(t.dueDate).date : "";

        const lateFlag = dueDate && closeDate && closeDate > dueDate;

        const overdueNow = isOverdue(t);

        const flags = [];

        if(stats?.deadlineChanges) flags.push(`дедлайн змінено ${stats.deadlineChanges}р`);

        if(lateFlag) flags.push("прострочено");

        if(overdueNow && t.status!=="закрито" && t.status!=="скасовано") flags.push("прострочено зараз");

        const lastNote = stats?.last?.note ? shorten(stats.last.note, 80) : "";

        const lastAllNote = (!lastNote && stats?.lastAll?.note) ? shorten(stats.lastAll.note, 80) : "";

        const desc = t.description ? shorten(t.description, 80) : "";

        return `

          <li>

            <div class="report-line">

              <span class="report-strong">${htmlesc(t.title)}</span>

              <span class="badge ${t.status==="закрито"?"b-ok":(t.status==="скасовано"?"":"b-blue")}">${htmlesc(statusLabel(t.status))}</span>

              ${closeDate ? `<span class="mono">${fmtDate(closeDate)}</span>` : ``}

            </div>

            <div class="report-meta">Дедлайн: <b>${dueDate ? fmtDate(dueDate) : "—"}</b>${flags.length ? ` • ${flags.join(" • ")}` : ""}</div>

            ${desc ? `<div class="report-meta">Опис: ${htmlesc(desc)}</div>` : ``}

            <div class="report-meta">Оновлень за період: <b>${stats?.total || 0}</b> • всього: <b>${stats?.totalAll || 0}</b>${lastNote ? ` • останнє: ${htmlesc(lastNote)}` : (lastAllNote ? ` • останнє: ${htmlesc(lastAllNote)}` : "")}</div>

          </li>

        `;

      }).join("") + `</ul>`

    : `<div class="hint">Немає задач у відділі.</div>`;



  const conclusion = (()=> {

    if(closedInPeriod.length === 0) return "Висновок: за період немає закритих задач — потрібна увага до завершення.";

    if(closedWithDue.length && latePct >= 30) return "Висновок: високий відсоток прострочень — потрібен контроль дедлайнів.";

    if(totalBlockerReasons > 0) return "Висновок: є повторювані блокери — перевірити причини і зняти ризики.";

    return "Висновок: відділ працює стабільно, критичних сигналів не виявлено.";

  })();



  const periodChips = `

    <div class="chips task-chips" style="margin-top:8px;">

      <div class="chip ${periodKey==="week"?"active":""}" data-action="openDeptAnalytics" data-arg1="${deptId}" data-arg2="week">Цей тиждень</div>

      <div class="chip ${periodKey==="prev_week"?"active":""}" data-action="openDeptAnalytics" data-arg1="${deptId}" data-arg2="prev_week">Попер. тиждень</div>

      <div class="chip ${periodKey==="month"?"active":""}" data-action="openDeptAnalytics" data-arg1="${deptId}" data-arg2="month">Цей місяць</div>

    </div>

  `;



  showSheet(`Звіт відділу — ${htmlesc(dept.name)}`, `

    <div class="item report-card" style="cursor:default;">

      <div class="row">

        <div class="name">${htmlesc(range.label)}</div>

        <span class="pill mono">${fmtDate(range.from)} — ${fmtDate(range.to)}</span>

      </div>

      ${periodChips}

    </div>



    <div class="report-section">

      <div class="report-title">Коротко</div>

      <div class="report-grid">

        <div class="report-tile">

          <div class="k">Активні зараз</div>

          <div class="v">${activeNow.length}</div>

          <div class="s">в роботі</div>

        </div>

        <div class="report-tile">

          <div class="k">Закрито за період</div>

          <div class="v">${closedInPeriod.length}</div>

          <div class="s">${htmlesc(range.label.toLowerCase())}</div>

        </div>

        <div class="report-tile">

          <div class="k">В строк</div>

          <div class="v">${onTime}</div>

          <div class="s">${closedWithDue.length ? `${onTimePct}%` : "—"}</div>

        </div>

        <div class="report-tile">

          <div class="k">Прострочено</div>

          <div class="v">${late.length}</div>

          <div class="s">${closedWithDue.length ? `${latePct}%` : "—"}</div>

        </div>

        <div class="report-tile">

          <div class="k">Прострочені зараз</div>

          <div class="v">${activeOverdue}</div>

          <div class="s">активні</div>

        </div>

        <div class="report-tile">

          <div class="k">Блокери зараз</div>

          <div class="v">${activeBlockers}</div>

          <div class="s">активні</div>

        </div>

        <div class="report-tile">

          <div class="k">Без оновлень 7 днів</div>

          <div class="v">${staleActive}</div>

          <div class="s">активні</div>

        </div>

        <div class="report-tile">

          <div class="k">Усього задач</div>

          <div class="v">${tasks.length}</div>

          <div class="s">у відділі</div>

        </div>

      </div>

    </div>



    <div class="report-section">

      <div class="report-title">Виконані за період</div>

      ${listHtml}

    </div>



    <div class="report-section">

      <div class="report-title">Оновлення / активність</div>

      <div class="report-meta">Оновлень за період: <b>${totalUpdates}</b> • зміни статусу: <b>${totalStatusChanges}</b> • редагування: <b>${totalEdits}</b> • причини блокера/очікування: <b>${totalBlockerReasons}</b></div>

      <details class="report-details" ${topChanged.length ? "open" : ""}>

        <summary>Найактивніші задачі</summary>

        ${changesHtml}

      </details>

      <details class="report-details">

        <summary>Останні зміни</summary>

        ${recentChanges}

      </details>

    </div>



    <div class="report-section">

      <details class="report-details">

        <summary>Усі задачі відділу (детально)</summary>

        ${allTasksHtml}

      </details>

    </div>



    <div class="report-section">

      <div class="report-title">Висновок</div>

      <div class="report-meta">${conclusion}</div>

    </div>



    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}



function openAllDeptReport(periodKey="week"){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;



  const today = kyivDateStr();

  const ranges = {

    week: {...weekRangeFor(today, 0), label: "Цей тиждень"},

    prev_week: {...weekRangeFor(today, 1), label: "Попередній тиждень"},

    month: {...monthRangeFor(today), label: "Цей місяць"},

  };

  const range = ranges[periodKey] || ranges.week;



  const allTasks = u.readOnly ? getVisibleTasksForUser(u) : STATE.tasks.slice();

  const announcementsAll = u.readOnly ? getVisibleAnnouncementsForUser(u) : STATE.tasks.filter(isAnnouncement);

  const personalAll = allTasks.filter(t=>t.type==="personal" && !isAnnouncement(t));

  const personalAnnouncements = announcementsAll;



  const activeNow = (list)=>list.filter(t=>t.status!=="закрито" && t.status!=="скасовано");

  const closedInPeriod = (list)=>list.filter(t=>{

    const closeDate = getCloseDateForTask(t);

    return closeDate && inRange(closeDate, range.from, range.to);

  });

  const countLate = (list)=>{

    const closed = closedInPeriod(list).filter(t=>!!t.dueDate);

    return closed.filter(t=>isClosedLate(t, getCloseDateForTask(t))).length;

  };



  const globalActive = activeNow(allTasks);

  const globalClosed = closedInPeriod(allTasks);

  const globalClosedWithDue = globalClosed.filter(t=>!!t.dueDate);

  const globalLate = globalClosedWithDue.filter(t=>isClosedLate(t, getCloseDateForTask(t)));

  const globalOnTime = globalClosedWithDue.length - globalLate.length;

  const globalOverdue = globalActive.filter(t=>isOverdue(t)).length;

  const globalBlockers = globalActive.filter(t=>["блокер","очікування"].includes(t.status)).length;

  const globalStale = globalActive.filter(t=>staleTask(t,7)).length;



  const pct = (n, d)=> d ? Math.round((n/d)*100) : 0;



  const deptRows = STATE.departments.map(d=>{

    const list = allTasks.filter(t=>t.departmentId===d.id);

    const active = activeNow(list);

    const closed = closedInPeriod(list);

    const closedWithDue = closed.filter(t=>!!t.dueDate);

    const late = closedWithDue.filter(t=>isClosedLate(t, getCloseDateForTask(t))).length;

    const onTime = closedWithDue.length - late;

    const overdue = active.filter(t=>isOverdue(t)).length;

    const blockers = active.filter(t=>["блокер","очікування"].includes(t.status)).length;

    return {dept:d, active: active.length, blockers, overdue, closed: closed.length, onTime, late, closedWithDue: closedWithDue.length};

  });



  const personalActive = activeNow(personalAll);

  const personalClosed = closedInPeriod(personalAll);

  const personalOverdue = personalActive.filter(t=>isOverdue(t)).length;

  const personalBlockers = personalActive.filter(t=>["блокер","очікування"].includes(t.status)).length;



  const annActive = activeNow(personalAnnouncements);

  const annClosed = closedInPeriod(personalAnnouncements);



  const periodChips = `

    <div class="chips task-chips" style="margin-top:8px;">

      <div class="chip ${periodKey==="week"?"active":""}" data-action="openAllDeptReport" data-arg1="week">Цей тиждень</div>

      <div class="chip ${periodKey==="prev_week"?"active":""}" data-action="openAllDeptReport" data-arg1="prev_week">Попер. тиждень</div>

      <div class="chip ${periodKey==="month"?"active":""}" data-action="openAllDeptReport" data-arg1="month">Цей місяць</div>

    </div>

  `;



  const deptListHtml = deptRows.length

    ? `<ul class="report-list">` + deptRows.map(r=>`

        <li>

          <div class="report-line">

            <span class="report-strong">${htmlesc(r.dept.name)}</span>

            <span class="badge b-blue">Активні ${r.active}</span>

            <span class="badge b-warn">Блокери ${r.blockers}</span>

            <span class="badge b-danger">Прострочені ${r.overdue}</span>

            <span class="badge b-ok">Закрито ${r.closed}</span>

          </div>

          <div class="report-meta">В строк: <b>${r.onTime}</b>${r.closedWithDue ? ` (${pct(r.onTime, r.closedWithDue)}%)` : ""} • Прострочено при закритті: <b>${r.late}</b>${r.closedWithDue ? ` (${pct(r.late, r.closedWithDue)}%)` : ""}</div>

        </li>

      `).join("") + `</ul>`

    : `<div class="hint">Немає даних по відділах.</div>`;



  const personalBlock = !u.readOnly ? `

    <div class="report-section">

      <div class="report-title">Мої особисті задачі</div>

      <div class="report-line">

        <span class="badge b-blue">Активні ${personalActive.length}</span>

        <span class="badge b-warn">Блокери ${personalBlockers}</span>

        <span class="badge b-danger">Прострочені ${personalOverdue}</span>

        <span class="badge b-ok">Закрито ${personalClosed.length}</span>

      </div>

      <div class="report-meta">Оголошення: активні <b>${annActive.length}</b> • закриті за період <b>${annClosed.length}</b></div>

    </div>

  ` : "";



  showSheet("Звіт по всім відділам", `

    <div class="item report-card" style="cursor:default;">

      <div class="row">

        <div class="name">${htmlesc(range.label)}</div>

        <span class="pill mono">${fmtDate(range.from)} — ${fmtDate(range.to)}</span>

      </div>

      ${periodChips}

    </div>



    <div class="report-section">

      <div class="report-title">Загальна аналітика</div>

      <div class="report-grid">

        <div class="report-tile clickable" data-action="openReportStatusTasks" data-arg1="active" data-arg2="${periodKey}">

          <div class="k">Активні зараз</div>

          <div class="v">${globalActive.length}</div>

          <div class="s">усі відділи</div>

        </div>

        <div class="report-tile clickable" data-action="openReportStatusTasks" data-arg1="closed" data-arg2="${periodKey}">

          <div class="k">Закрито за період</div>

          <div class="v">${globalClosed.length}</div>

          <div class="s">${htmlesc(range.label.toLowerCase())}</div>

        </div>

        <div class="report-tile clickable" data-action="openReportStatusTasks" data-arg1="on_time" data-arg2="${periodKey}">

          <div class="k">В строк</div>

          <div class="v">${globalOnTime}</div>

          <div class="s">${globalClosedWithDue.length ? `${pct(globalOnTime, globalClosedWithDue.length)}%` : "—"}</div>

        </div>

        <div class="report-tile clickable" data-action="openReportStatusTasks" data-arg1="closed_late" data-arg2="${periodKey}">

          <div class="k">Прострочено при закритті</div>

          <div class="v">${globalLate.length}</div>

          <div class="s">${globalClosedWithDue.length ? `${pct(globalLate.length, globalClosedWithDue.length)}%` : "—"}</div>

        </div>

        <div class="report-tile clickable" data-action="openReportStatusTasks" data-arg1="overdue" data-arg2="${periodKey}">

          <div class="k">Прострочені зараз</div>

          <div class="v">${globalOverdue}</div>

          <div class="s">активні</div>

        </div>

        <div class="report-tile clickable" data-action="openReportStatusTasks" data-arg1="blockers" data-arg2="${periodKey}">

          <div class="k">Блокери зараз</div>

          <div class="v">${globalBlockers}</div>

          <div class="s">активні</div>

        </div>

        <div class="report-tile clickable" data-action="openReportStatusTasks" data-arg1="stale" data-arg2="${periodKey}">

          <div class="k">Без оновлень 7 днів</div>

          <div class="v">${globalStale}</div>

          <div class="s">активні</div>

        </div>

        <div class="report-tile">

          <div class="k">Усього задач</div>

          <div class="v">${allTasks.length}</div>

          <div class="s">в системі</div>

        </div>

      </div>

    </div>



    <div class="report-section">

      <div class="report-title">Відділи</div>

      ${deptListHtml}

    </div>



    ${personalBlock}



    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}



function openReportStatusTasks(filterKey, periodKey="week"){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;



  const today = kyivDateStr();

  const ranges = {

    week: {...weekRangeFor(today, 0), label: "Цей тиждень"},

    prev_week: {...weekRangeFor(today, 1), label: "Попередній тиждень"},

    month: {...monthRangeFor(today), label: "Цей місяць"},

  };

  const range = ranges[periodKey] || ranges.week;

  const allTasks = u.readOnly ? getVisibleTasksForUser(u) : STATE.tasks.slice();

  const activeNow = (list)=>list.filter(t=>t.status!=="закрито" && t.status!=="скасовано");

  const closedInPeriod = (list)=>list.filter(t=>{

    const closeDate = getCloseDateForTask(t);

    return closeDate && inRange(closeDate, range.from, range.to);

  });



  let title = "";

  let list = [];

  if(filterKey==="active"){

    title = "Активні зараз";

    list = activeNow(allTasks);

  } else if(filterKey==="overdue"){

    title = "Прострочені зараз";

    list = activeNow(allTasks).filter(t=>isOverdue(t));

  } else if(filterKey==="blockers"){

    title = "Блокери зараз";

    list = activeNow(allTasks).filter(t=>["блокер","очікування"].includes(t.status));

  } else if(filterKey==="stale"){

    title = "Без оновлень 7 днів";

    list = activeNow(allTasks).filter(t=>staleTask(t,7));

  } else if(filterKey==="closed"){

    title = `Закрито за період (${range.label.toLowerCase()})`;

    list = closedInPeriod(allTasks);

  } else if(filterKey==="on_time"){

    title = `В строк (${range.label.toLowerCase()})`;

    list = closedInPeriod(allTasks).filter(t=>!!t.dueDate && !isClosedLate(t, getCloseDateForTask(t)));

  } else if(filterKey==="closed_late"){

    title = `Прострочено при закритті (${range.label.toLowerCase()})`;

    list = closedInPeriod(allTasks).filter(t=>!!t.dueDate && isClosedLate(t, getCloseDateForTask(t)));

  } else {

    return;

  }



  const sorted = list.slice().sort((a,b)=> (b.updatedAt || "").localeCompare(a.updatedAt || ""));

  const rows = sorted.length ? `

    <ul class="report-list">

      ${sorted.map(t=>{

        const dept = t.departmentId ? (getDeptById(t.departmentId)?.name || "Відділ") : (isAnnouncement(t) ? "Оголошення" : "Особисто");

        const due = t.dueDate ? fmtDate(t.dueDate) : "—";

        const closeDate = getCloseDateForTask(t);

        const closeInfo = closeDate ? ` • Закрито: <b>${fmtDate(closeDate)}</b>` : "";

        return `

          <li data-action="openTask" data-arg1="${t.id}">

            <div class="report-line">

              <span class="report-strong">${htmlesc(t.title || t.id)}</span>

              <span class="badge b-blue">${htmlesc(statusLabel(t.status))}</span>

            </div>

            <div class="report-meta">Відділ: <b>${htmlesc(dept)}</b> • Дедлайн: <b>${due}</b>${closeInfo}</div>

          </li>

        `;

      }).join("")}

    </ul>

  ` : `<div class="hint">Немає задач для цього статусу.</div>`;



  showSheet(title, `

    <div class="hint">Період: <span class="mono">${fmtDate(range.from)} — ${fmtDate(range.to)}</span></div>

    <div class="sep"></div>

    ${rows}

    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}



/* ===========================

   DEPT SUMMARY FORM

=========================== */

function openDeptNote(deptId){

  const u = currentSessionUser();

  if(!u) return;

  const dept = getDeptById(deptId);

  if(!dept) return;

  const {isDeptHeadLike} = asDeptRole(u);

  const canEdit = !u.readOnly && (u.role==="boss" || (isDeptHeadLike && u.departmentId===deptId));

  if(!canEdit){

    showSheet("Немає прав", `<div class="hint">Тільки керівник або начальник відділу (в.о.) може редагувати примітку.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  showSheet(`Примітка — ${htmlesc(dept.name)}`, `

    <div class="field">

      <label>Примітка</label>

      <textarea id="deptNoteText" placeholder="Коротка примітка по відділу…">${htmlesc(dept.note || "")}</textarea>

      ${formatToolbar("deptNoteText", "inline")}

    </div>

    <div class="hint">Без ліміту символів.</div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="saveDeptNoteNow" data-arg1="${deptId}">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function saveDeptNoteNow(deptId){

  const u = currentSessionUser();

  if(!u) return;

  const dept = getDeptById(deptId);

  if(!dept) return;

  const {isDeptHeadLike} = asDeptRole(u);

  const canEdit = !u.readOnly && (u.role==="boss" || (isDeptHeadLike && u.departmentId===deptId));

  if(!canEdit){

    showSheet("Немає прав", `<div class="hint">Тільки керівник або начальник відділу (в.о.) може редагувати примітку.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const text = document.getElementById("deptNoteText")?.value.trim() || "";

  dept.note = text;

  saveState(STATE);

  hideSheet();

  render();

}



function openDeptSummaryForm(){

  const u = currentSessionUser();

  const {isDeptHeadLike} = asDeptRole(u);

  if(u.role==="boss"){

    showSheet("Підсумок відділу", `<div class="hint">Підсумок подає начальник відділу (або в.о.).</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(!isDeptHeadLike){

    showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може подати підсумок.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const today = kyivDateStr();

  const existing = STATE.deptSummaries.find(s=>s.departmentId===u.departmentId && s.summaryDate===today);



  showSheet("Підсумок відділу", `

    <div class="hint">

      3–5 речень. Ключове: виконано/ризики/що потребує рішення.

    </div>

    <div class="field">

      <label>Текст підсумку</label>

      <textarea id="sumText" maxlength="600" placeholder="Приклад: За день виконано … / В процесі … / Ризик: … / Потрібно рішення: …">${htmlesc(existing?.text || "")}</textarea>

    </div>

    <div class="hint">Ліміт: 600 символів. Дата: <span class="mono">${fmtDate(today)}</span>.</div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="submitDeptSummaryNow">Надіслати</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}



function submitDeptSummaryNow(){

  const u = currentSessionUser();

  const text = document.getElementById("sumText").value.trim();

  if(text.length < 10){

    showSheet("Помилка", `<div class="hint">Напиши хоча б кілька речень (мінімум 10 символів).</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  submitDeptSummary({departmentId:u.departmentId, authorUserId:u.id, text});

  hideSheet();

  render();

}



/* ===========================

   REPORT FORM

=========================== */

function openReportForm(){

  const u = currentSessionUser();

  const now = kyivNow();

  const date = kyivDateStr(now);

  const weekend = isWeekend(now);

  const mins = minutesSinceMidnight(now);

  const late = (!weekend) && mins > REPORT_DEADLINE_MIN;



  const existing = STATE.dailyReports.find(r=>r.userId===u.id && r.reportDate===date);



  showSheet("Щоденний звіт", `

    <div class="hint">

      Звіт за <span class="mono">${fmtDate(date)}</span>.

      ${weekend ? "Сьогодні вихідний — подання не обов’язкове." : (late ? "<b>Увага:</b> після 17:30 буде “ПІЗНО”." : "Подай до 17:30, щоб було вчасно.")}

    </div>

    <div class="actions" style="margin-top:10px;">

      <button class="btn ghost" data-action="applyReportTemplate">🧩 Шаблон</button>

      <button class="btn ghost" data-action="autoFillReport">⚡ Авто з задач</button>

    </div>



    <div class="field">

      <label>Що виконано</label>

      <textarea id="rDone" placeholder="Коротко, по пунктах. Можеш вставляти коди задач: T-2026-0004">${htmlesc(existing?.doneText || "")}</textarea>

    </div>

    <div class="field">

      <label>Що в процесі</label>

      <textarea id="rProg" placeholder="Що робиться зараз, що залишилось.">${htmlesc(existing?.progressText || "")}</textarea>

    </div>

    <div class="field">

      <label>Проблеми / блокери</label>

      <textarea id="rBlock" placeholder="Що заважає, кого/чого чекаємо.">${htmlesc(existing?.blockedText || "")}</textarea>

    </div>



    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="submitReportNow">Надіслати</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}



function submitReportNow(){

  const u = currentSessionUser();

  const doneText = document.getElementById("rDone").value.trim();

  const progressText = document.getElementById("rProg").value.trim();

  const blockedText = document.getElementById("rBlock").value.trim();

  submitDailyReport({userId:u.id, doneText, progressText, blockedText});

  hideSheet();

  render();

}



/* ===========================

   REPORTING PLANS

=========================== */

function setReportingMonthFromInput(){

  const input = document.getElementById("reportMonthInput");

  if(!input || !input.value) return;

  UI.reportingMonth = input.value;

  render();

  updateReportPlanDaysGrid(UI.reportingMonth);

}

function setPlanMonthFromInput(){

  const input = document.getElementById("planMonthInput");

  if(!input || !input.value) return;

  UI.planMonth = input.value;

  render();

}

function setPlanMode(mode){

  UI.planMode = mode === "tasks" ? "tasks" : "reporting";

  render();

}

function updateReportPlanDaysGrid(monthStr){

  if(!modal.classList.contains("show")) return;

  const grid = document.getElementById("rpDaysGrid");

  if(!grid) return;

  const selected = [...document.querySelectorAll('input[name="rpDay"]:checked')]

    .map(x=>Number(x.value))

    .filter(Number.isFinite);

  const extraStored = (grid.dataset.extraDays || "")

    .split(",")

    .map(x=>Number(x))

    .filter(Number.isFinite);

  const allSelected = [...new Set([...selected, ...extraStored])];

  const maxDays = daysInMonth(monthStr);

  const visibleSelected = allSelected.filter(d=>d>=1 && d<=maxDays);

  const extra = allSelected.filter(d=>d>maxDays);

  grid.dataset.extraDays = extra.join(",");

  const monthDays = Array.from({length:maxDays}, (_,i)=>i+1);

  grid.innerHTML = monthDays.map(d=>`

    <label class="rec-toggle">

      <input type="checkbox" name="rpDay" value="${d}" ${visibleSelected.includes(d) ? "checked" : ""} />

      <span class="rec-label">${d}</span>

    </label>

  `).join("");

}

function renderReportPlanDeptChecks(selectedIds){

  const selected = new Set(Array.isArray(selectedIds) ? selectedIds : []);

  return `

    <div class="dept-toggle-grid">

      ${STATE.departments.map(d=>`

        <label class="dept-toggle">

          <span class="dept-name">${htmlesc(d.name)}</span>

          <span class="switch">

            <input type="checkbox" name="rpDept" value="${d.id}" ${selected.has(d.id) ? "checked" : ""} />

            <span class="slider"></span>

          </span>

        </label>

      `).join("")}

    </div>

  `;

}

function reportPlanFormHtml(plan=null, monthStr=null){

  const title = plan?.title || "";

  const desc = plan?.description || "";

  const daySet = new Set(Array.isArray(plan?.daysOfMonth) ? plan.daysOfMonth : (Number.isFinite(Number(plan?.dayOfMonth)) ? [Number(plan.dayOfMonth)] : []));

  const weekSet = new Set(Array.isArray(plan?.weekDays) ? plan.weekDays : []);

  const deptChecks = renderReportPlanDeptChecks(plan?.deptIds || []);

  const month = monthStr || UI.reportingMonth || kyivDateStr().slice(0,7);

  const maxDays = daysInMonth(month);

  const monthDays = Array.from({length:maxDays}, (_,i)=>i+1);

  const weekDays = [

    {v:1, label:"Пн"},

    {v:2, label:"Вт"},

    {v:3, label:"Ср"},

    {v:4, label:"Чт"},

    {v:5, label:"Пт"},

    {v:6, label:"Сб"},

    {v:0, label:"Нд"},

  ];

  return `

    <div class="field">

      <label>Назва заходу</label>

      <input id="rpTitle" value="${htmlesc(title)}" placeholder="Наприклад: Щомісячна нарада" />

    </div>

    <div class="field">

      <div class="label-row">

        <label>Опис (опційно)</label>

        ${formatToolbar("rpDesc", "inline")}

      </div>

      <textarea id="rpDesc" placeholder="Деталі / очікуваний результат">${htmlesc(desc)}</textarea>

    </div>

    <div class="field">

      <label>Дати місяця</label>

      <div id="rpDaysGrid" class="rec-toggle-grid monthday-grid">

        ${monthDays.map(d=>`

          <label class="rec-toggle">

            <input type="checkbox" name="rpDay" value="${d}" ${daySet.has(d) ? "checked" : ""} />

            <span class="rec-label">${d}</span>

          </label>

        `).join("")}

      </div>

      <div class="hint">Можна обрати кілька. Якщо в місяці менше днів — ставимо на останній день.</div>

    </div>

    <div class="field">

      <label>Дні тижня (за потреби)</label>

      <div class="rec-toggle-grid">

        ${weekDays.map(d=>`

          <label class="rec-toggle">

            <input type="checkbox" name="rpWeekday" value="${d.v}" ${weekSet.has(d.v) ? "checked" : ""} />

            <span class="rec-label">${d.label}</span>

          </label>

        `).join("")}

      </div>

    </div>

    <div class="field">

      <label>Відділи</label>

      ${deptChecks}

    </div>

  `;

}

function openReportPlanCreate(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  showSheet("Новий захід (щомісяця)", `

    ${reportPlanFormHtml(null, UI.reportingMonth || kyivDateStr().slice(0,7))}

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="saveReportPlanNow">Створити</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function openReportPlanEdit(planId){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  const plan = STATE.reportPlans?.find(p=>p.id===planId);

  if(!plan) return;

  showSheet("Редагувати захід", `

    ${reportPlanFormHtml(plan, UI.reportingMonth || kyivDateStr().slice(0,7))}

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="saveReportPlanNow" data-arg1="${plan.id}">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function saveReportPlanNow(planId){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  const title = document.getElementById("rpTitle")?.value.trim();

  if(!title){

    showSheet("Помилка", `<div class="hint">Вкажи назву заходу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const daysOfMonth = [...document.querySelectorAll('input[name="rpDay"]:checked')]

    .map(x=>Number(x.value))

    .filter(n=>Number.isFinite(n) && n>=1 && n<=31);

  const grid = document.getElementById("rpDaysGrid");

  const extraDays = (grid?.dataset?.extraDays || "")

    .split(",")

    .map(x=>Number(x))

    .filter(n=>Number.isFinite(n) && n>=1 && n<=31);

  const weekDays = [...document.querySelectorAll('input[name="rpWeekday"]:checked')]

    .map(x=>Number(x.value))

    .filter(n=>Number.isFinite(n) && n>=0 && n<=6);

  if(!daysOfMonth.length && !weekDays.length){

    showSheet("Помилка", `<div class="hint">Обери хоча б одну дату або день тижня.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const deptIds = [...document.querySelectorAll('input[name="rpDept"]:checked')]

    .map(x=>x.value)

    .filter(Boolean);

  if(!deptIds.length){

    showSheet("Помилка", `<div class="hint">Обери хоча б один відділ.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const desc = document.getElementById("rpDesc")?.value || "";

  if(!STATE.reportPlans) STATE.reportPlans = [];



  if(planId){

    const idx = STATE.reportPlans.findIndex(p=>p.id===planId);

    if(idx < 0) return;

    const prev = STATE.reportPlans[idx];

    STATE.reportPlans[idx] = {

      ...prev,

      title,

      description: desc,

      daysOfMonth: [...new Set([...daysOfMonth, ...extraDays])],

      weekDays: [...new Set(weekDays)],

      deptIds: [...new Set(deptIds)],

      updatedAt: nowIsoKyiv(),

      updatedBy: u.id,

    };

  } else {

    STATE.reportPlans.push({

      id: uid("rp"),

      title,

      description: desc,

      daysOfMonth: [...new Set([...daysOfMonth, ...extraDays])],

      weekDays: [...new Set(weekDays)],

      deptIds: [...new Set(deptIds)],

      createdBy: u.id,

      createdAt: nowIsoKyiv(),

      updatedAt: nowIsoKyiv(),

    });

  }



  saveState(STATE);

  hideSheet();

  runReportPlans();

  render();

  showToast(planId ? "Зміни збережено" : "Захід додано", "ok");

}

function confirmDeleteReportPlan(planId){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  const plan = STATE.reportPlans?.find(p=>p.id===planId);

  if(!plan) return;

  showSheet("Видалити захід?", `

    <div class="hint">“${htmlesc(plan.title)}” буде видалено з плану. Уже створені задачі залишаться в історії.</div>

    <div class="sep"></div>

    <div class="actions">

      <button class="btn danger" data-action="deleteReportPlanNow" data-arg1="${plan.id}">Видалити</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function deleteReportPlanNow(planId){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  if(!STATE.reportPlans) STATE.reportPlans = [];

  STATE.reportPlans = STATE.reportPlans.filter(p=>p.id!==planId);

  saveState(STATE);

  hideSheet();

  render();

  showToast("Захід видалено", "ok");

}



/* ===========================

   REPORTS VIEW (UPDATED with Control Block for Boss)

=========================== */

function setReportFilter(k){ UI.reportFilter = k; render(); }



function setReportsControlDate(v){

  if(!v) return;

  UI.reportsControlDate = v;

  render();

}

function setReportsControlDateFromInput(){

  const input = document.getElementById("ctrlDateInput");

  if(!input) return;

  setReportsControlDate(input.value);

}



function viewReports(){

  if(!ensureLoggedIn()) return viewLogin();

  recomputeDelegationStatuses();



  const u = currentSessionUser();

  UI.tab = ROUTES.REPORTS;



  const filter = UI.reportFilter;

  const today = kyivDateStr();

  let start = today;

  if(filter==="тиждень") start = addDays(today, -6);

  if(filter==="місяць") start = addDays(today, -29);



  if(UI.reportsControlDate == null) UI.reportsControlDate = today;

  const ctrlDate = UI.reportsControlDate;



  const reports = getVisibleReportsForUser(u)

    .filter(r=>r.reportDate >= start && r.reportDate <= today)

    .sort((a,b)=> (b.reportDate + b.submittedAt).localeCompare(a.reportDate + a.submittedAt));



  const sums = (u.role==="boss" ? STATE.deptSummaries : STATE.deptSummaries.filter(s=>s.departmentId===u.departmentId))

    .filter(s=>s.summaryDate >= start && s.summaryDate <= today)

    .sort((a,b)=> (b.summaryDate + b.submittedAt).localeCompare(a.summaryDate + a.submittedAt));



  const chips = `

    <div class="chips">

      <div class="chip ${filter==="сьогодні"?"active":""}" data-action="setReportFilter" data-arg1="сьогодні">Сьогодні</div>

      <div class="chip ${filter==="тиждень"?"active":""}" data-action="setReportFilter" data-arg1="тиждень">Тиждень</div>

      <div class="chip ${filter==="місяць"?"active":""}" data-action="setReportFilter" data-arg1="місяць">Місяць</div>

    </div>

  `;



  function computeDeptControl(dateStr){

    const weekend = isWeekend(new Date(dateStr + "T12:00:00"));

    const reps = STATE.dailyReports.filter(r=>r.reportDate===dateStr);



    return STATE.departments.map(d=>{

      const executors = STATE.users.filter(x=>x.active && x.role==="executor" && x.departmentId===d.id);

      const deptReps = reps.filter(r=>r.departmentId===d.id);



      const missing = weekend ? [] : executors.filter(ex=>!deptReps.some(r=>r.userId===ex.id));

      const late = weekend ? [] : deptReps.filter(r=>r.isLate && executors.some(ex=>ex.id===r.userId));



      return {

        deptId: d.id,

        deptName: d.name,

        weekend,

        expected: executors.length,

        missingCount: missing.length,

        lateCount: late.length

      };

    });

  }



  const deptControl = (u.role==="boss") ? computeDeptControl(ctrlDate) : [];



  const controlBlock = (u.role==="boss") ? `

    <div class="item" style="cursor:default;">

      <div class="row">

        <div class="name">🧭 Контроль подання звітів</div>

        <span class="badge b-blue">Керівник</span>

      </div>

      <div class="hint" style="margin-top:10px;">

        Обери дату — і одразу видно “хто не здав” по відділах.

      </div>



      <div class="field" style="margin-top:12px;">

        <label>Дата</label>

        <input type="date" id="ctrlDateInput" value="${ctrlDate}" data-change="setReportsControlDateFromInput" />

      </div>

    </div>



    <div class="list">

      ${deptControl.map(x=>{

        const missBadge = x.weekend ? `<span class="badge">Вихідний</span>` :

          (x.missingCount ? `<span class="badge b-danger">🔴 ${x.missingCount} не здали</span>` : `<span class="badge b-ok">✅ всі здали</span>`);

        const lateBadge = (!x.weekend && x.lateCount) ? `<span class="badge b-warn">🟡 ${x.lateCount} пізно</span>` : ``;



        return `

          <div class="item" style="cursor:default;">

            <div class="row">

              <div>

            <div class="name">${deptBadgeHtml(getDeptById(x.deptId))}</div>

                <div class="sub">

                  ${missBadge} ${lateBadge}

                  <span class="pill">Виконавців: <span class="mono">${x.expected}</span></span>

                </div>

              </div>

              <button class="btn ghost" data-action="openDeptPeopleBoss" data-arg1="${x.deptId}" data-arg2="${ctrlDate}">👥 Люди</button>

            </div>

          </div>

        `;

      }).join("")}

    </div>



    <div class="sep"></div>

  ` : ``;



  const listReports = reports.length ? reports.map(r=>{

    const usr = getUserById(r.userId);

    const dept = getDeptById(r.departmentId);

    const badge = r.isLate ? `<span class="badge b-warn">🟡 ПІЗНО</span>` : `<span class="badge b-ok">✅ ВЧАСНО</span>`;

    return `

      <div class="item" data-action="openReport" data-arg1="${r.id}">

        <div class="row">

          <div>

            <div class="name">${deptBadgeHtml(dept)} — ${htmlesc(usr?.name ?? "")}</div>

            <div class="sub">

              ${badge}

              <span class="pill mono">${fmtDate(r.reportDate)}</span>

              <span class="pill mono">${htmlesc(r.submittedAt.slice(11,16))}</span>

            </div>

          </div>

          <div class="pill">›</div>

        </div>

        <div class="hint" style="margin-top:10px;">

          <b>Виконано:</b> ${htmlesc(shorten(r.doneText))}<br/>

          <b>Блокери:</b> ${htmlesc(shorten(r.blockedText))}

        </div>

      </div>

    `;

  }).join("") : `<div class="hint">Немає звітів за обраний період. Спробуй інший фільтр або перевір, чи подані звіти.</div>`;



  const listSums = sums.length ? sums.map(s=>{

    const dept = getDeptById(s.departmentId);

    const au = getUserById(s.authorUserId);

    return `

      <div class="item" data-action="openDeptSummary" data-arg1="${s.id}">

        <div class="row">

          <div>

            <div class="name">🧾 Підсумок — ${deptBadgeHtml(dept)}</div>

            <div class="sub">

              <span class="badge b-violet">Підсумок відділу</span>

              <span class="pill mono">${fmtDate(s.summaryDate)}</span>

              <span class="pill">${htmlesc(au?.name ?? "")}${isActingHead(au?.id) ? " (в.о.)" : ""}</span>

              <span class="pill mono">${htmlesc(s.submittedAt.slice(11,16))}</span>

            </div>

          </div>

          <div class="pill">›</div>

        </div>

        <div class="hint" style="margin-top:10px;">${htmlesc(shorten(s.text, 140))}</div>

      </div>

    `;

  }).join("") : `<div class="hint">Немає підсумків за обраний період. Спробуй інший фільтр або період.</div>`;



  const body = `

    <div class="card">

      <div class="card-h">

        <div class="t">Звіти</div>

        <span class="badge b-blue">${u.role==="boss" ? "Всі відділи" : "Мій відділ"}</span>

      </div>

      <div class="card-b">

        ${chips}

        <div class="sep"></div>



        ${controlBlock}



        <div class="item" style="cursor:default;">

          <div class="row"><div class="name">🧾 Підсумки відділів</div><span class="badge b-violet mono">${sums.length}</span></div>

          <div class="hint">Короткий підсумок (3–5 речень) від начальника/в.о.</div>

        </div>

        <div class="list">${listSums}</div>



        <div class="sep"></div>

        <div class="item" style="cursor:default;">

          <div class="row"><div class="name">📝 Щоденні звіти</div><span class="badge b-blue mono">${reports.length}</span></div>

          <div class="hint">Звіти виконавців (вчасно/пізно).</div>

        </div>

        <div class="list">${listReports}</div>

      </div>

    </div>

  `;



  const tabs = (u.role==="boss")

    ? [

      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

      {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

      {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

      {key:ROUTES.PLAN, label:"План", ico:"📅"},

    ]

    : [

      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

    ];



  const subtitle = roleSubtitle(u);

  appShell({title:"Звіти", subtitle, bodyHtml: body, showFab:false, fabAction:null, tabs});

}



function openReport(reportId){

  const u = currentSessionUser();

  const r = STATE.dailyReports.find(x=>x.id===reportId);

  if(!r) return;



  if(u.role!=="boss" && !canAccessDept(u, r.departmentId)){

    showSheet("Немає доступу", `<div class="hint">Цей звіт належить іншому відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const usr = getUserById(r.userId);

  const dept = getDeptById(r.departmentId);



  showSheet("Звіт", `

    <div class="item" style="cursor:default;">

      <div class="row">

        <div>

          <div class="name">${htmlesc(dept?.name ?? "")} — ${htmlesc(usr?.name ?? "")}</div>

          <div class="sub">

            <span class="pill mono">${fmtDate(r.reportDate)}</span>

            <span class="pill mono">${htmlesc(r.submittedAt)}</span>

            ${r.isLate ? `<span class="badge b-warn">🟡 ПІЗНО</span>` : `<span class="badge b-ok">✅ ВЧАСНО</span>`}

          </div>

        </div>

      </div>

    </div>



    <div class="sep"></div>



    <div class="item" style="cursor:default;">

      <div class="name">✔ Виконано</div>

      <div class="hint" style="margin-top:10px; white-space:pre-wrap;">${htmlesc(r.doneText || "—")}</div>

    </div>



    <div class="item" style="cursor:default;">

      <div class="name">⏳ В процесі</div>

      <div class="hint" style="margin-top:10px; white-space:pre-wrap;">${htmlesc(r.progressText || "—")}</div>

    </div>



    <div class="item" style="cursor:default;">

      <div class="name">⛔ Блокери</div>

      <div class="hint" style="margin-top:10px; white-space:pre-wrap;">${htmlesc(r.blockedText || "—")}</div>

    </div>



    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}



function openDeptSummary(summaryId){

  const u = currentSessionUser();

  const s = STATE.deptSummaries.find(x=>x.id===summaryId);

  if(!s) return;



  if(u.role!=="boss" && s.departmentId !== u.departmentId){

    showSheet("Немає доступу", `<div class="hint">Цей підсумок належить іншому відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const dept = getDeptById(s.departmentId);

  const au = getUserById(s.authorUserId);



  showSheet("Підсумок відділу", `

    <div class="item" style="cursor:default;">

      <div class="row">

        <div>

          <div class="name">${htmlesc(dept?.name ?? "")}</div>

          <div class="sub">

            <span class="badge b-violet">Підсумок</span>

            <span class="pill mono">${fmtDate(s.summaryDate)}</span>

            <span class="pill">${htmlesc(au?.name ?? "")}${isActingHead(au?.id) ? " (в.о.)" : ""}</span>

            <span class="pill mono">${htmlesc(s.submittedAt)}</span>

          </div>

        </div>

      </div>

    </div>



    <div class="sep"></div>



    <div class="item" style="cursor:default;">

      <div class="name">Текст</div>

      <div class="hint" style="margin-top:10px; white-space:pre-wrap;">${htmlesc(s.text || "—")}</div>

    </div>



    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}



/* ===========================

   REPORTING (MONTHLY PLANS)

=========================== */

function viewReporting(){

  if(!ensureLoggedIn()) return viewLogin();

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    UI.tab = ROUTES.CONTROL;

    return viewControl();

  }



  UI.tab = ROUTES.REPORTING;



  const today = kyivDateStr();

  const monthStr = UI.reportingMonth || today.slice(0,7);

  UI.reportingMonth = monthStr;

  const currentMonth = today.slice(0,7);



  const plans = (STATE.reportPlans || []).slice().sort((a,b)=>{

    const da = Array.isArray(a.daysOfMonth) && a.daysOfMonth.length ? Math.min(...a.daysOfMonth) : (Number(a.dayOfMonth) || 0);

    const db = Array.isArray(b.daysOfMonth) && b.daysOfMonth.length ? Math.min(...b.daysOfMonth) : (Number(b.dayOfMonth) || 0);

    if(da !== db) return da - db;

    return (a.title || "").localeCompare(b.title || "");

  });



  const tasksForMonth = STATE.tasks.filter(t=>t.reportPlanId && t.reportMonth === monthStr);

  const doneInMonth = tasksForMonth.filter(t=>{

    if(t.status !== "закрито") return false;

    const closeDate = getCloseDateForTask(t);

    return closeDate && closeDate.startsWith(monthStr);

  }).length;



  const totalPlanned = plans.reduce((s,p)=>{

    const deptCount = Array.isArray(p.deptIds) ? p.deptIds.length : 0;

    const datesCount = reportPlanScheduleDates(p, monthStr).length;

    return s + (deptCount * (datesCount || 0));

  }, 0);



  const planList = plans.length ? plans.map(plan=>{

    const deptIds = Array.isArray(plan.deptIds) ? plan.deptIds : [];

    const scheduleDates = reportPlanScheduleDates(plan, monthStr);

  const planTasks = tasksForMonth.filter(t=>t.reportPlanId===plan.id);

  const taskMap = new Map();

  planTasks.forEach(t=>{

    const d = reportPlanTaskDate(t);

    if(!d) return;

    const key = `${t.departmentId || ""}__${d}`;

    if(!taskMap.has(key)) taskMap.set(key, t);

  });

    const closedInMonth = planTasks.filter(t=>{

      if(t.status !== "закрито") return false;

      const closeDate = getCloseDateForTask(t);

      return closeDate && closeDate.startsWith(monthStr);

    }).length;



    const deptRows = deptIds.flatMap(deptId=>{

      const dept = getDeptById(deptId);

      return scheduleDates.map(date=>{

        const task = taskMap.get(`${deptId}__${date}`) || null;

        const closeDate = task ? getCloseDateForTask(task) : null;

        const closedInMonth = !!(closeDate && closeDate.startsWith(monthStr));

        const statusBadge = task

          ? `<span class="badge ${statusBadgeClass(task.status)}">${statusIcon(task.status)} ${htmlesc(statusLabel(task.status))}</span>`

          : `<span class="badge">Заплановано</span>`;

        const missingLabel = reportingMissingLabel(monthStr, date);

        const statusBadgeFinal = task ? statusBadge : `<span class="badge">${missingLabel}</span>`;

        const closePill = (task && task.status==="закрито" && closeDate)

          ? `<span class="pill mono">${fmtDate(closeDate)}${closedInMonth ? "" : " (поза місяцем)"}</span>`

          : ``;

        const openAttr = task ? `data-action="openTask" data-arg1="${task.id}"` : "";

        const cursor = task ? "" : "cursor:default;";



        return `

          <div class="item" ${openAttr} style="${cursor}">

            <div class="row">

              <div>

                <div class="name">${deptBadgeHtml(dept)} ${htmlesc(dept?.name ?? "Відділ")}</div>

                <div class="sub">

                  ${statusBadgeFinal}

                  <span class="pill">План: <span class="mono">${fmtDate(date)}</span></span>

                  ${closePill}

                </div>

              </div>

              ${task ? `<div class="pill">›</div>` : ``}

            </div>

          </div>

        `;

      });

    }).join("");



    const desc = plan.description ? `<div class="hint rich-text" style="margin-top:10px;">${richText(plan.description)}</div>` : "";

    const actions = u.readOnly ? "" : `

      <div style="display:flex; gap:6px; flex-wrap:wrap;">

        <button class="btn ghost" data-action="openReportPlanEdit" data-arg1="${plan.id}">Редагувати</button>

        <button class="btn ghost" data-action="confirmDeleteReportPlan" data-arg1="${plan.id}">Видалити</button>

      </div>

    `;



    const monthDayList = Array.isArray(plan.daysOfMonth) ? plan.daysOfMonth : [];

    const weekDayList = Array.isArray(plan.weekDays) ? plan.weekDays : [];

    const weekLabels = ["Нд","Пн","Вт","Ср","Чт","Пт","Сб"];

    const weekLabelText = weekDayList.length ? weekDayList.map(x=>weekLabels[x] || "").filter(Boolean).join(", ") : "—";

    const dayLabelText = monthDayList.length ? monthDayList.join(", ") : "—";

    const schedulePills = scheduleDates.length

      ? scheduleDates.map(d=>`<span class="pill mono">${fmtDate(d)}</span>`).join("")

      : `<span class="pill">—</span>`;



    return `

      <div class="item" style="cursor:default;">

        <div class="row">

          <div>

            <div class="name">${htmlesc(plan.title || "Без назви")}</div>

            <div class="sub">

              <span class="pill">Дні місяця: <span class="mono">${htmlesc(dayLabelText)}</span></span>

              <span class="pill">Дні тижня: <span class="mono">${htmlesc(weekLabelText)}</span></span>

              ${schedulePills}

              <span class="pill">Відділів: <span class="mono">${deptIds.length}</span></span>

              <span class="pill">Створено: <span class="mono">${planTasks.length}</span></span>

              <span class="pill">Закрито в місяці: <span class="mono">${closedInMonth}</span></span>

            </div>

          </div>

          ${actions}

        </div>

        ${desc}

        ${deptIds.length ? `<div class="list" style="margin-top:10px;">${deptRows}</div>` : `<div class="hint">Відділи не вибрані.</div>`}

      </div>

    `;

  }).join("") : `<div class="hint">Немає планових заходів. Додай перший через “＋”.</div>`;



  const body = `

    <div class="card">

      <div class="card-h">

        <div class="t">Звітність</div>

        <span class="badge b-blue mono">${plans.length}</span>

      </div>

      <div class="card-b">

        <div class="field">

          <label>Звітний місяць</label>

          <input type="month" id="reportMonthInput" value="${monthStr}" data-change="setReportingMonthFromInput" />

        </div>



        <div class="item" style="cursor:default;">

          <div class="row">

            <div class="name">Підсумок за місяць</div>

            <span class="badge b-blue mono">${totalPlanned}</span>

          </div>

          <div class="sub">

            <span class="pill">Створено задач: <span class="mono">${tasksForMonth.length}</span></span>

            <span class="pill">Закрито в місяці: <span class="mono">${doneInMonth}</span></span>

          </div>

        </div>



        <div class="list">

          ${planList}

        </div>

      </div>

    </div>

  `;



  const tabs = [

    {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

    {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

    {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

    {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

    {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

    {key:ROUTES.PLAN, label:"План", ico:"📅"},

  ];



  const subtitle = roleSubtitle(u);

  const fabAction = ()=>openReportPlanCreate();

  appShell({title:"Звітність", subtitle, bodyHtml: body, showFab: !u.readOnly, fabAction, tabs});

}



/* ===========================

   PLAN (CALENDAR)

=========================== */

function calendarCellsForMonth(monthStr){

  const [y,m] = monthStr.split("-").map(Number);

  const first = new Date(y, m-1, 1);

  const startDow = (first.getDay() + 6) % 7; // Monday start

  const total = daysInMonth(monthStr);

  const cells = [];

  const totalCells = 42;

  for(let i=0; i<totalCells; i++){

    const dayNum = i - startDow + 1;

    if(dayNum < 1 || dayNum > total){

      cells.push({inMonth:false, day:null, date:null});

    } else {

      const date = `${monthStr}-${String(dayNum).padStart(2,'0')}`;

      cells.push({inMonth:true, day:dayNum, date});

    }

  }

  return cells;

}

function openPlanDay(dateStr, mode){

  if(mode === "tasks") return openPlanDayTasks(dateStr);

  return openPlanDayReporting(dateStr);

}

function openPlanCreateTask(dateStr, deptId=null){

  if(!dateStr) return;

  openCreateTask("internal", deptId || null, dateStr);

}

function openPlanCreateTaskFromPicker(dateStr){

  const sel = document.getElementById("planAddDept");

  const deptId = sel?.value || "";

  if(!deptId){

    showSheet("Помилка", `<div class="hint">Оберіть відділ.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  openPlanCreateTask(dateStr, deptId);

}

function openTaskFromPlanDay(taskId){

  openTask(taskId, {stack:true});

}

function openPlanDayReporting(dateStr){

  const monthStr = dateStr.slice(0,7);

  const occ = getReportPlanOccurrences(monthStr).filter(o=>o.date===dateStr);

  if(!occ.length){

    showSheet("План на дату", `<div class="hint">Немає заходів на <span class="mono">${fmtDate(dateStr)}</span>.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const list = occ.map(o=>{

    const dept = getDeptById(o.deptId);

    const statusBadge = o.task

      ? `<span class="badge ${statusBadgeClass(o.task.status)}">${statusIcon(o.task.status)} ${htmlesc(statusLabel(o.task.status))}</span>`

      : `<span class="badge">${o.missing ? "Не створено" : "Заплановано"}</span>`;

    const closePill = (o.task && o.task.status==="закрито" && o.closeDate)

      ? `<span class="pill mono">${fmtDate(o.closeDate)}</span>`

      : ``;

    const openAttr = o.task ? `data-action="openTaskFromPlanDay" data-arg1="${o.task.id}"` : "";

    const cursor = o.task ? "" : "cursor:default;";

    return `

      <div class="item" ${openAttr} style="${cursor}">

        <div class="row">

          <div>

            <div class="name">${htmlesc(o.plan.title || "Захід")} — ${htmlesc(dept?.name ?? "Відділ")}</div>

            <div class="sub">

              ${statusBadge}

              ${closePill}

            </div>

          </div>

          ${o.task ? `<div class="pill">›</div>` : ``}

        </div>

      </div>

    `;

  }).join("");

  showSheet(`План на ${fmtDate(dateStr)}`, `

    <div class="list">${list}</div>

    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}

function openPlanDayTasks(dateStr){

  const u = currentSessionUser();

  const tasks = getVisibleTasksForUser(u)

    .filter(t=>!isAnnouncement(t))

    .filter(t=>t.status!=="закрито" && t.status!=="скасовано")

    .filter(t=>{

      if(!t.dueDate) return false;

      const {date} = splitDateTime(t.dueDate);

      if(!date) return false;

      return date === dateStr;

    });

  const {isDeptHeadLike} = asDeptRole(u);

  const canCreate = !!u && !u.readOnly && (u.role==="boss" || isDeptHeadLike);

  const deptOptions = canCreate

    ? (u.role==="boss" ? STATE.departments : STATE.departments.filter(d=>d.id===u.departmentId))

    : [];

  const addBlock = canCreate ? `

    <div class="plan-add">

      <div class="row2" style="align-items:flex-end;">

        <div class="field">

          <label>Додати задачу для відділу</label>

          <select id="planAddDept">

            ${deptOptions.map(d=>`<option value="${d.id}">${htmlesc(d.name)}</option>`).join("")}

          </select>

        </div>

        <div class="field">

          <label>&nbsp;</label>

          <button class="btn primary" data-action="openPlanCreateTaskFromPicker" data-arg1="${dateStr}">＋ Додати</button>

        </div>

      </div>

    </div>

  ` : "";

  if(!tasks.length){

    showSheet("План задач на дату", `

      ${addBlock}

      <div class="hint">Немає активних задач на <span class="mono">${fmtDate(dateStr)}</span>.</div>

      <div class="sep"></div>

      <button class="btn primary" data-action="hideSheet">OK</button>

    `);

    return;

  }

  const groups = new Map();

  tasks.forEach(t=>{

    const key = t.departmentId || "personal";

    if(!groups.has(key)) groups.set(key, []);

    groups.get(key).push(t);

  });

  const deptOrder = STATE.departments.map(d=>d.id);

  const orderIndex = (key)=>{

    if(key === "personal") return 1e9;

    const idx = deptOrder.indexOf(key);

    return idx === -1 ? 1e8 : idx;

  };

  const keys = [...groups.keys()].sort((a,b)=>orderIndex(a)-orderIndex(b));

  const list = keys.map(key=>{

    const listTasks = groups.get(key) || [];

    listTasks.sort((a,b)=>{

      const ta = splitDateTime(a.dueDate).time || "";

      const tb = splitDateTime(b.dueDate).time || "";

      if(ta !== tb) return ta.localeCompare(tb);

      return (a.title || "").localeCompare(b.title || "");

    });

    const dept = (key === "personal") ? null : getDeptById(key);

    const deptName = (key === "personal") ? "Особисто" : (dept?.name || "Відділ");

    const addBtn = (canCreate && key !== "personal") ? `<button class="btn ghost btn-mini" data-action="openPlanCreateTask" data-arg1="${dateStr}" data-arg2="${key}">＋ Додати</button>` : ``;

    const items = listTasks.map((t, idx)=>{

      const statusBadge = `<span class="badge ${statusBadgeClass(t.status)}">${statusIcon(t.status)} ${htmlesc(statusLabel(t.status))}</span>`;

      const time = splitDateTime(t.dueDate).time || "";

      const timePill = time ? `<span class="pill mono">${htmlesc(time)}</span>` : ``;

      return `

        <div class="item" data-action="openTaskFromPlanDay" data-arg1="${t.id}">

          <div class="row">

            <div>

              <div class="name"><span class="plan-item-index">${idx + 1}.</span> ${htmlesc(t.title || t.id)}</div>

              <div class="sub">

                ${statusBadge}

                ${timePill}

                ${t.reportPlanId ? `<span class="pill">Звітність</span>` : ``}

              </div>

            </div>

            <div class="pill">›</div>

          </div>

        </div>

      `;

    }).join("");

    return `

      <div class="plan-dept-group">

        <div class="plan-dept-head">

          <div class="name">${htmlesc(deptName)}</div>

          ${addBtn}

        </div>

        <div class="list plan-dept-list">${items}</div>

      </div>

    `;

  }).join("");

  showSheet(`План задач на ${fmtDate(dateStr)}`, `

    ${addBlock}

    ${list}

    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}

function viewPlan(){

  if(!ensureLoggedIn()) return viewLogin();

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    UI.tab = ROUTES.CONTROL;

    return viewControl();

  }

  UI.tab = ROUTES.PLAN;



  const today = kyivDateStr();

  const monthStr = UI.planMonth || UI.reportingMonth || today.slice(0,7);

  UI.planMonth = monthStr;

  const mode = UI.planMode || "reporting";



  const weekLabels = ["Пн","Вт","Ср","Чт","Пт","Сб","Нд"];

  const cells = calendarCellsForMonth(monthStr);

  const monthStats = (()=>{

    if(mode === "tasks"){

      const map = new Map();

      const tasks = getVisibleTasksForUser(u)

        .filter(t=>!isAnnouncement(t))

        .filter(t=>t.status!=="закрито" && t.status!=="скасовано")

        .filter(t=>!!t.dueDate);

      tasks.forEach(t=>{

        const {date} = splitDateTime(t.dueDate);

        if(!date || !date.startsWith(monthStr)) return;

        const s = map.get(date) || {total:0, overdue:0};

        s.total += 1;

        if(isOverdue(t)) s.overdue += 1;

        map.set(date, s);

      });

      return {map};

    }

    const occ = getReportPlanOccurrences(monthStr);

    const map = new Map();

    occ.forEach(o=>{

      const s = map.get(o.date) || {total:0, done:0, missing:0, active:0};

      s.total += 1;

      if(o.task){

        if(o.task.status==="закрито" && o.closeDate && o.closeDate.startsWith(monthStr)) s.done += 1;

        else s.active += 1;

      } else if(o.missing){

        s.missing += 1;

      }

      map.set(o.date, s);

    });

    return {map};

  })();



  const grid = `

    <div class="cal-grid">

      ${weekLabels.map(w=>`<div class="cal-head">${w}</div>`).join("")}

      ${cells.map(c=>{

        if(!c.inMonth){

          return `<div class="cal-cell is-out"></div>`;

        }

        const stats = monthStats.map.get(c.date);

        const isToday = c.date === today;

        const badges = (()=> {

          if(!stats || !stats.total) return "";

          if(mode === "tasks"){

            return `

              <div class="cal-badges">

                <span class="pill">Дедл. <span class="mono">${stats.total}</span></span>

                ${stats.overdue ? `<span class="badge b-warn">🟠 ${stats.overdue}</span>` : ``}

              </div>

            `;

          }

          return `

            <div class="cal-badges">

              <span class="pill">План <span class="mono">${stats.total}</span></span>

              ${stats.done ? `<span class="badge b-ok">✅ ${stats.done}</span>` : ``}

              ${stats.missing ? `<span class="badge b-danger">⚠️ ${stats.missing}</span>` : ``}

            </div>

          `;

        })();

        return `

          <div class="cal-cell ${isToday ? "is-today" : ""}" data-action="openPlanDay" data-arg1="${c.date}" data-arg2="${mode}">

            <div class="cal-day">${c.day}</div>

            ${badges}

          </div>

        `;

      }).join("")}

    </div>

  `;



  const body = `

    <div class="card">

      <div class="card-h">

        <div class="t">План</div>

        <span class="badge b-blue mono">${htmlesc(monthStr)}</span>

      </div>

      <div class="card-b">

        <div class="row2">

          <div class="field">

            <label>Місяць</label>

            <input type="month" id="planMonthInput" value="${monthStr}" data-change="setPlanMonthFromInput" />

          </div>

          <div class="field">

            <label>Режим</label>

            <div class="chips">

              <div class="chip ${mode==="reporting"?"active":""}" data-action="setPlanMode" data-arg1="reporting">Звітність</div>

              <div class="chip ${mode==="tasks"?"active":""}" data-action="setPlanMode" data-arg1="tasks">Усі задачі</div>

            </div>

          </div>

        </div>

        ${grid}

      </div>

    </div>

  `;



  const tabs = [

    {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

    {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

    {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

    {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

    {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

    {key:ROUTES.PLAN, label:"План", ico:"📅"},

  ];

  const subtitle = roleSubtitle(u);

  appShell({title:"План", subtitle, bodyHtml: body, showFab:false, fabAction:null, tabs});

}



/* ===========================

   WEEKLY TASKS

=========================== */

function weeklyTasksForRange(range){

  if(!STATE.weeklyTasks) STATE.weeklyTasks = [];

  return STATE.weeklyTasks.filter(t=>t.weekStart===range.from)

    .slice()

    .sort((a,b)=>{

      const ao = Number.isFinite(a.order) ? a.order : 1e9;

      const bo = Number.isFinite(b.order) ? b.order : 1e9;

      if(ao !== bo) return ao - bo;

      return (b.updatedAt || b.createdAt || "").localeCompare(a.updatedAt || a.createdAt || "");

    });

}

function weeklyTaskRows(list){

  return list.map((t, idx)=>([

    String(idx+1),

    t.title || "",

    t.description || "",

    getUserById(t.createdBy)?.name || "",

    t.updatedAt || t.createdAt || "",

    t.weekStart || "",

    t.weekEnd || "",

  ]));

}

function setWeeklyTaskClosed(taskId, closed, closeAtOverride=null){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  if(u.readOnly){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування вимкнено.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const t = STATE.weeklyTasks?.find(x=>x.id===taskId);

  if(!t) return;

  if(closed){

    t.status = "закрито";

    t.closedAt = closeAtOverride || nowIsoKyiv();

    t.closedBy = u.id;

  } else {

    t.status = null;

    t.closedAt = null;

    t.closedBy = null;

  }

  t.updatedAt = nowIsoKyiv();

  saveState(STATE);

  hideSheet();

  render();

  showToast(closed ? "Закрито" : "Відкрито", "ok");

}

function closeWeeklyTaskNow(taskId){ openWeeklyClosePicker(taskId); }

function reopenWeeklyTaskNow(taskId){ setWeeklyTaskClosed(taskId, false); }

function openWeeklyClosePicker(taskId){

  const u = currentSessionUser();

  const t = STATE.weeklyTasks?.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.readOnly){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування вимкнено.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const now = t.closedAt || nowIsoKyiv();

  const parts = splitDateTimeLoose(now);

  showSheet("Закрити задачу", `

    <div class="hint">Обери дату та час закриття.</div>

    <div class="row2">

      <div class="field">

        <label>Дата</label>

        <input id="wCloseDate" type="date" value="${htmlesc(parts.date)}" />

      </div>

      <div class="field">

        <label>Час</label>

        <input id="wCloseTime" type="time" value="${htmlesc(parts.time)}" />

      </div>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn ok" data-action="applyWeeklyClose" data-arg1="${t.id}">✅ Закрити</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function applyWeeklyClose(taskId){

  const date = document.getElementById("wCloseDate")?.value || null;

  const time = document.getElementById("wCloseTime")?.value || "";

  if(!date){

    showSheet("Помилка", `<div class="hint">Вкажи дату закриття.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const closeAt = joinDateTime(date, time);

  setWeeklyTaskClosed(taskId, true, closeAt);

}

function openWeeklyTaskCreate(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Тижневі задачі може створювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(u.readOnly){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування вимкнено.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const range = getWeeklySelectedRange();

  showSheet("Нова задача за тиждень", `

    <div class="hint">Період: <span class="mono">${fmtDate(range.from)} — ${fmtDate(range.to)}</span></div>

    <div class="field">

      <label>Задача</label>

      <input id="wTitle" placeholder="Наприклад: що виконано цього тижня" />

    </div>

    <div class="field">

      <label>Опис (опційно)</label>

      <textarea id="wDesc" placeholder="Деталі / результат / примітки"></textarea>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="createWeeklyTaskNow">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function createWeeklyTaskNow(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Тижневі задачі може створювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(u.readOnly){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування вимкнено.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const title = (document.getElementById("wTitle")?.value || "").trim();

  if(!title){

    showSheet("Помилка", `<div class="hint">Вкажи назву задачі.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const desc = (document.getElementById("wDesc")?.value || "").trim();

  const today = kyivDateStr();

  const range = getWeeklySelectedRange();

  const existing = weeklyTasksForRange(range);

  const maxOrder = existing.reduce((m, t)=> Number.isFinite(t.order) ? Math.max(m, t.order) : m, 0);

  if(!STATE.weeklyTasks) STATE.weeklyTasks = [];

  STATE.weeklyTasks.push({

    id: uid("w"),

    title,

    description: desc,

    weekStart: range.from,

    weekEnd: range.to,

    order: maxOrder + 1,

    createdBy: u.id,

    createdAt: nowIsoKyiv(),

    updatedAt: nowIsoKyiv(),

  });

  saveState(STATE);

  hideSheet();

  render();

  showToast("Збережено", "ok");

}

function openWeeklyTaskEdit(taskId){

  const u = currentSessionUser();

  const t = STATE.weeklyTasks?.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.readOnly){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування вимкнено.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const isClosed = (t.status === "закрито");

  showSheet("Редагувати задачу", `

    <div class="hint">Період: <span class="mono">${fmtDate(t.weekStart)} — ${fmtDate(t.weekEnd)}</span></div>

    <div class="field">

      <label>Задача</label>

      <input id="wTitle" value="${htmlesc(t.title)}" />

    </div>

    <div class="field">

      <label>Опис (опційно)</label>

      <textarea id="wDesc">${htmlesc(t.description || "")}</textarea>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="saveWeeklyTaskEdits" data-arg1="${t.id}">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

      ${isClosed

        ? `<button class="btn ghost" data-action="reopenWeeklyTaskNow" data-arg1="${t.id}">↩ Відкрити</button>`

        : `<button class="btn ok" data-action="closeWeeklyTaskNow" data-arg1="${t.id}">✅ Закрити</button>`

      }

      <button class="btn danger" data-action="confirmDeleteWeeklyTask" data-arg1="${t.id}">Видалити</button>

    </div>

  `);

}

function saveWeeklyTaskEdits(taskId){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  if(u.readOnly){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування вимкнено.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const t = STATE.weeklyTasks?.find(x=>x.id===taskId);

  if(!t) return;

  const title = (document.getElementById("wTitle")?.value || "").trim();

  if(!title){

    showSheet("Помилка", `<div class="hint">Вкажи назву задачі.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const desc = (document.getElementById("wDesc")?.value || "").trim();

  t.title = title;

  t.description = desc;

  t.updatedAt = nowIsoKyiv();

  saveState(STATE);

  hideSheet();

  render();

  showToast("Зміни збережено", "ok");

}

function confirmDeleteWeeklyTask(taskId){

  showSheet("Видалити задачу", `

    <div class="hint">Видалити цю задачу за тиждень?</div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn danger" data-action="deleteWeeklyTaskNow" data-arg1="${taskId}">Видалити</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function deleteWeeklyTaskNow(taskId){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  if(u.readOnly){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування вимкнено.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(!STATE.weeklyTasks) STATE.weeklyTasks = [];

  STATE.weeklyTasks = STATE.weeklyTasks.filter(x=>x.id!==taskId);

  saveState(STATE);

  hideSheet();

  render();

  showToast("Видалено", "ok");

}

function applyWeeklyOrder(weekStart, orderedIds){

  if(!STATE.weeklyTasks) STATE.weeklyTasks = [];

  const orderMap = new Map(orderedIds.map((id, idx)=>[id, idx + 1]));

  let changed = false;

  STATE.weeklyTasks.forEach(t=>{

    if(t.weekStart !== weekStart) return;

    if(!orderMap.has(t.id)) return;

    const next = orderMap.get(t.id);

    if(t.order !== next){

      t.order = next;

      changed = true;

    }

  });

  if(changed){

    saveState(STATE);

    render();

  }

}

function applyAnnouncementOrder(orderedIds){

  const orderMap = new Map(orderedIds.map((id, idx)=>[id, idx + 1]));

  let changed = false;

  STATE.tasks.forEach(t=>{

    if(!isAnnouncement(t)) return;

    if(!orderMap.has(t.id)) return;

    const next = orderMap.get(t.id);

    if(t.annOrder !== next){

      t.annOrder = next;

      changed = true;

    }

  });

  if(changed){

    saveState(STATE);

    render();

  }

}

function applyDeptOrder(deptKey, orderedIds){

  const orderMap = new Map(orderedIds.map((id, idx)=>[id, idx + 1]));

  let changed = false;

  STATE.tasks.forEach(t=>{

    if(isAnnouncement(t)) return;

    const key = t.departmentId || "personal";

    if(key !== deptKey) return;

    if(!orderMap.has(t.id)) return;

    const next = orderMap.get(t.id);

    if(t.deptOrder !== next){

      t.deptOrder = next;

      changed = true;

    }

  });

  if(changed){

    saveState(STATE);

    render();

  }

}

function getWeeklyDragAfterElement(container, y){

  const items = [...container.querySelectorAll(".weekly-item:not(.dragging)")];

  let closest = {offset: Number.NEGATIVE_INFINITY, element: null};

  items.forEach(child=>{

    const box = child.getBoundingClientRect();

    const offset = y - box.top - (box.height / 2);

    if(offset < 0 && offset > closest.offset){

      closest = {offset, element: child};

    }

  });

  return closest.element;

}

function getAnnouncementDragAfterElement(container, y){

  const items = [...container.querySelectorAll(".task-item.announcement-item:not(.dragging)")];

  let closest = {offset: Number.NEGATIVE_INFINITY, element: null};

  items.forEach(child=>{

    const box = child.getBoundingClientRect();

    const offset = y - box.top - (box.height / 2);

    if(offset < 0 && offset > closest.offset){

      closest = {offset, element: child};

    }

  });

  return closest.element;

}

function getTaskDragAfterElement(container, y){

  const items = [...container.querySelectorAll(":scope > .task-item:not(.announcement-item):not(.dragging)")];

  let closest = {offset: Number.NEGATIVE_INFINITY, element: null};

  items.forEach(child=>{

    const box = child.getBoundingClientRect();

    const offset = y - box.top - (box.height / 2);

    if(offset < 0 && offset > closest.offset){

      closest = {offset, element: child};

    }

  });

  return closest.element;

}

function exportWeeklyTasksExcelNow(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Експорт доступний тільки керівнику.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const range = getWeeklySelectedRange();

  const prev = weekRangeFor(range.from, 1);

  const curTasks = weeklyTasksForRange(range);

  const prevTasks = weeklyTasksForRange(prev);

  const header = ["#", "Задача", "Опис", "Автор", "Оновлено", "Початок", "Кінець"];

  const rowsCur = weeklyTaskRows(curTasks);

  const rowsPrev = weeklyTaskRows(prevTasks);

  const sheetCur = `Обраний ${range.from}`;

  const sheetPrev = `Попередній ${prev.from}`;

  if(window.XLSX){

    const wb = XLSX.utils.book_new();

    const ws1 = XLSX.utils.aoa_to_sheet([header, ...rowsCur]);

    const ws2 = XLSX.utils.aoa_to_sheet([header, ...rowsPrev]);

    applyTimesFont(ws1);

    applyTimesFont(ws2);

    XLSX.utils.book_append_sheet(wb, ws1, sheetCur);

    XLSX.utils.book_append_sheet(wb, ws2, sheetPrev);

    XLSX.writeFile(wb, `weekly_${range.from}_${range.to}.xlsx`, {cellStyles:true});

    hideSheet();

    return;

  }

  const sheets = [];

  sheets.push(buildWorksheetXml(sheetCur, header, rowsCur));

  sheets.push(buildWorksheetXml(sheetPrev, header, rowsPrev));

  const xml = buildTasksWorkbookXml(sheets);

  downloadExcelXml(`weekly_${range.from}_${range.to}.xml`, xml);

}

function viewWeeklyTasks(){

  if(!ensureLoggedIn()) return viewLogin();

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    UI.tab = ROUTES.CONTROL;

    return viewControl();

  }

  UI.tab = ROUTES.WEEKLY;

  const today = kyivDateStr();

  const anchor = resolveWeeklyAnchorDate(today);

  UI.weeklyAnchorDate = anchor;

  const range = weekRangeFor(anchor, 0);

  const prev = weekRangeFor(range.from, 1);

  const curTasks = weeklyTasksForRange(range);

  const prevTasks = weeklyTasksForRange(prev);

  const diff = curTasks.length - prevTasks.length;

  const diffLabel = (diff > 0 ? `+${diff}` : String(diff));

  const periodMode = UI.weeklyPeriodMode || "current";

  const monthStr = UI.weeklyMonth || anchor.slice(0,7);

  const monthWeeks = weeksInMonth(`${monthStr}-01`);

  const weekIdx = Math.max(1, Math.min(UI.weeklyWeekIndex || 1, monthWeeks.length || 1));

  UI.weeklyWeekIndex = weekIdx;

  UI.weeklyMonth = monthStr;

  const weekOptions = monthWeeks.map((start, idx)=>{

    const end = addDays(start, 6);

    const label = `${idx + 1} (${fmtDateShort(start)} — ${fmtDateShort(end)})`;

    return `<option value="${idx + 1}" ${idx + 1 === weekIdx ? "selected" : ""}>${label}</option>`;

  }).join("");

  const periodControls = `

    <div class="weekly-controls">

      <div class="field">

        <label>Період</label>

        <select id="weeklyPeriod" data-change="setWeeklyPeriodFromSelect">

          <option value="current" ${periodMode==="current"?"selected":""}>Цей тиждень</option>

          <option value="prev" ${periodMode==="prev"?"selected":""}>Попередній тиждень</option>

          <option value="next" ${periodMode==="next"?"selected":""}>Наступний тиждень</option>

          <option value="custom" ${periodMode==="custom"?"selected":""}>Обрати дату</option>

          <option value="month" ${periodMode==="month"?"selected":""}>Тиждень місяця</option>

        </select>

      </div>

      ${periodMode==="custom" ? `

        <div class="field">

          <label>Дата тижня</label>

          <input id="weeklyDate" type="date" value="${anchor}" data-change="setWeeklyAnchorDateFromInput" />

        </div>

      ` : ``}

      ${periodMode==="month" ? `

        <div class="field">

          <label>Місяць</label>

          <input id="weeklyMonth" type="month" value="${monthStr}" data-change="setWeeklyMonthFromInput" />

        </div>

        <div class="field">

          <label>Тиждень</label>

          <select id="weeklyWeekIdx" data-change="setWeeklyWeekIndexFromSelect">

            ${weekOptions}

          </select>

        </div>

      ` : ``}

    </div>

  `;

  const renderList = (list, emptyText, editable)=>{

    if(!list.length) return `<div class="hint">${emptyText}</div>`;

    return list.map((t, idx)=>{

      const desc = (t.description || "").trim();

      const isClosed = (t.status === "закрито");

      const closeAt = isClosed ? (t.closedAt || t.updatedAt || "") : "";

      const closeShort = isClosed ? closeDisplay(closeAt) : "";

      const closeHint = isClosed ? closeTitle(closeAt) : "";

      const closeMeta = (isClosed && closeShort)

        ? `<span class="pill mono" title="Закрито ${htmlesc(closeHint)}">✅ ${htmlesc(closeShort)}</span>`

        : "";

      const canDrag = editable && !u.readOnly && !isClosed;

      const dragAttrs = canDrag ? `draggable="true"` : "";

      const baseCursor = u.readOnly ? "cursor:default;" : (canDrag ? "cursor:grab;" : "cursor:pointer;");

      const openAttrs = u.readOnly ? "" : `data-action="openWeeklyTaskEdit" data-arg1="${t.id}"`;

      return `

        <div class="item weekly-item ${isClosed ? "is-completed" : ""}" data-weekly-id="${t.id}" ${dragAttrs} style="${baseCursor}">

          <div class="row" ${openAttrs}>

            <div>

              <div class="name"><span class="mono">${idx + 1}.</span> ${htmlesc(t.title)}</div>

              ${desc ? `<div class="hint" style="margin-top:8px;">${htmlesc(desc)}</div>` : ``}

            </div>

            ${closeMeta ? `<div class="weekly-meta">${closeMeta}</div>` : ``}

          </div>

        </div>

      `;

    }).join("");

  };

  const headerActions = `

    <div style="display:flex;gap:8px;">

      <button class="btn ghost" data-action="exportWeeklyTasksExcelNow">⬇️ Excel</button>

      ${u.readOnly ? `` : `<button class="btn primary" data-action="openWeeklyTaskCreate">➕ Додати</button>`}

    </div>

  `;

  const body = `

    <div class="card task-card">

      <div class="card-h">

        <div class="t">Задачі за тиждень</div>

        ${headerActions}

      </div>

      <div class="card-b">

        ${periodControls}

        <div class="hint">

          Обраний тиждень: <b>${curTasks.length}</b> • Попередній: <b>${prevTasks.length}</b> • Різниця: <b>${diffLabel}</b><br/>

          Період: <span class="mono">${fmtDate(range.from)} — ${fmtDate(range.to)}</span>

        </div>

        <div class="sep"></div>

        <div class="section-title">Обраний тиждень</div>

        <div class="list weekly-list" data-weekly-list="current">${renderList(curTasks, "Немає задач за цей тиждень.", true)}</div>

        <div class="sep"></div>

        <div class="section-title">Попередній тиждень</div>

        <div class="list weekly-list" data-weekly-list="prev">${renderList(prevTasks, "Немає задач за попередній тиждень.", false)}</div>

      </div>

    </div>

  `;

  const tabs = [

    {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

    {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

    {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

    {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

    {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

    {key:ROUTES.PLAN, label:"План", ico:"📅"},

  ];

  const subtitle = roleSubtitle(u);

  appShell({title:"Тиждень", subtitle, bodyHtml: body, showFab:false, fabAction:null, tabs});



  const listEl = document.querySelector('[data-weekly-list="current"]');

  if(listEl && !u.readOnly){

    let dragging = null;

    listEl.querySelectorAll(".weekly-item").forEach(el=>{

      el.addEventListener("dragstart", (e)=>{

        dragging = el;

        el.classList.add("dragging");

        e.dataTransfer.effectAllowed = "move";

        e.dataTransfer.setData("text/plain", el.getAttribute("data-weekly-id") || "");

      });

      el.addEventListener("dragend", ()=>{

        if(dragging) dragging.classList.remove("dragging");

        dragging = null;

      });

    });

    listEl.addEventListener("dragover", (e)=>{

      e.preventDefault();

      const afterEl = getWeeklyDragAfterElement(listEl, e.clientY);

      if(!dragging) return;

      if(afterEl == null){

        listEl.appendChild(dragging);

      } else {

        listEl.insertBefore(dragging, afterEl);

      }

    });

    listEl.addEventListener("drop", (e)=>{

      e.preventDefault();

      const ids = [...listEl.querySelectorAll(".weekly-item")].map(el=>el.getAttribute("data-weekly-id")).filter(Boolean);

      applyWeeklyOrder(range.from, ids);

    });

  }

}



/* ===========================

   TASKS VIEW + ACTIONS

=========================== */

function setTaskFilter(k){ UI.taskFilter = k; render(); }

function setTaskDeptFilter(k){

  UI.taskDeptFilter = k;

  if(k !== "personal"){

    UI.taskPersonalFilter = "all";

    UI.taskAnnAudienceFilter = "all";

  }

  render();

}

function setTaskPersonalFilter(k){ UI.taskPersonalFilter = k; render(); }

function openMyTasks(){

  UI.taskDeptFilter = "personal";

  UI.taskPersonalFilter = "tasks";

  UI.taskAnnAudienceFilter = "all";

  render();

}

function openAllTasks(){

  UI.taskDeptFilter = "all";

  UI.taskPersonalFilter = "all";

  UI.taskAnnAudienceFilter = "all";

  render();

}

function openAnnouncementsAudience(aud){

  UI.taskDeptFilter = "personal";

  UI.taskPersonalFilter = "announcements";

  UI.taskAnnAudienceFilter = aud;

  render();

}

function toggleTaskScope(){

  const next = (UI.taskDeptFilter === "personal") ? "all" : "personal";

  UI.taskDeptFilter = next;

  if(next === "personal"){

    UI.taskFilter = "активні";

    UI.taskPersonalFilter = "all";

  }

  render();

}

function setTaskSearchFromInput(){

  const input = document.getElementById("taskSearchInput");

  UI.taskSearch = (input?.value || "").trim().toLowerCase();

  render();

}

function clearTaskSearch(){

  UI.taskSearch = "";

  render();

}

function confirmDeleteTask(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(!canDeleteTask(u, t)){

    showSheet("Немає прав", `<div class="hint">Ви не маєте прав видаляти цю задачу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const isAnn = isAnnouncement(t);

  showSheet(isAnn ? "Видалити оголошення" : "Видалити задачу", `

    <div class="hint">Видалити "${htmlesc(t.title)}"? Це також прибере всі оновлення по задачі.</div>

    <div class="actions">

      <button class="btn danger" data-action="deleteTaskNow" data-arg1="${t.id}">🗑 Видалити</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function deleteTaskNow(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(!canDeleteTask(u, t)){

    showSheet("\u041d\u0435\u043c\u0430\u0454 \u043f\u0440\u0430\u0432", `<div class="hint">\u0412\u0438 \u043d\u0435 \u043c\u0430\u0454\u0442\u0435 \u043f\u0440\u0430\u0432 \u0432\u0438\u0434\u0430\u043b\u044f\u0442\u0438 \u0446\u044e \u0437\u0430\u0434\u0430\u0447\u0443.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const targetFingerprint = taskDisplayFingerprint(t);

  const duplicateIds = new Set(

    STATE.tasks

      .filter(x=>taskDisplayFingerprint(x) === targetFingerprint)

      .map(x=>x.id)

  );



  if(!Array.isArray(STATE.deletedTaskIds)) STATE.deletedTaskIds = [];

  const deletedSet = new Set(STATE.deletedTaskIds.map(String));

  duplicateIds.forEach(id=>deletedSet.add(String(id)));

  STATE.deletedTaskIds = Array.from(deletedSet);



  STATE.tasks = STATE.tasks.filter(x=>!duplicateIds.has(x.id));

  STATE.taskUpdates = STATE.taskUpdates.filter(x=>!duplicateIds.has(x.taskId));

  if(Array.isArray(DB_TASKS_CACHE)){

    DB_TASKS_CACHE = DB_TASKS_CACHE.filter(x=>!duplicateIds.has(x.id));

  }

  saveState(STATE);

  hideSheet();

  render();

  showToast(duplicateIds.size > 1 ? `\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e ${duplicateIds.size} \u0434\u0443\u0431\u043b\u0456\u043a\u0430\u0442\u0438` : "\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e", "ok");

}



function viewTasks(){

  if(!ensureLoggedIn()) return viewLogin();

  recomputeDelegationStatuses();

  const u = currentSessionUser();

  const {isDeptHeadLike} = asDeptRole(u);

  UI.tab = ROUTES.TASKS;

  ensureDbTasksCache();



  let tasks = getVisibleTasksForView(u);

  const filter = UI.taskFilter;

  const deptFilter = UI.taskDeptFilter || "all";

  const taskSearch = UI.taskSearch || "";

  const personalFilter = UI.taskPersonalFilter || "all";

  const annAudience = UI.taskAnnAudienceFilter || "all";

  const showAnnouncementsScope = (u.role!=="boss") || (u.role==="boss" && deptFilter==="personal");

  const isPersonalScope = (u.role==="boss" && deptFilter==="personal");

  const effectivePersonalFilter = showAnnouncementsScope ? personalFilter : "tasks";

  let announcements = showAnnouncementsScope ? getVisibleAnnouncementsForUser(u) : [];

  const deptLabel = (t)=> t.departmentId ? (getDeptById(t.departmentId)?.name || "Відділ") : "Особисто";

  const matchesSearch = (t)=>{

    if(!taskSearch) return true;

    const dept = t.departmentId ? getDeptById(t.departmentId)?.name : "Особисто";

    const resp = getUserById(t.responsibleUserId)?.name || "";

    const aud = isAnnouncement(t) ? announcementAudienceLabel(t.audience) : "";

    const hay = `${t.title} ${t.id} ${dept} ${resp} ${aud}`.toLowerCase();

    return hay.includes(taskSearch);

  };

  const highlightMatch = (text)=>{

    const raw = String(text ?? "");

    if(!taskSearch) return htmlesc(raw);

    const needle = taskSearch;

    const lower = raw.toLowerCase();

    if(!needle || !lower.includes(needle)) return htmlesc(raw);

    let out = "";

    let i = 0;

    while(true){

      const idx = lower.indexOf(needle, i);

      if(idx === -1){

        out += htmlesc(raw.slice(i));

        break;

      }

      out += htmlesc(raw.slice(i, idx));

      out += `<mark class="search-hit">${htmlesc(raw.slice(idx, idx + needle.length))}</mark>`;

      i = idx + needle.length;

    }

    return out;

  };



  if(u.role === "boss"){

    if(deptFilter === "personal"){

      tasks = tasks.filter(t=>t.type==="personal");

    } else if(deptFilter !== "all"){

      tasks = tasks.filter(t=>t.departmentId === deptFilter);

    }

  }



  const isDeptScope = (u.role!=="boss") || (u.role==="boss" && deptFilter!=="all" && deptFilter!=="personal");

  const taskSort = (a,b)=>{

    const bucket = (t)=>{

      if(t.dueDate) return 0;

      if(["блокер","очікування"].includes(t.status)) return 1;

      if(t.nextControlDate) return 2;

      if(t.controlAlways) return 3;

      return 4;

    };

    const dateKey = (t)=>{

      if(t.dueDate) return dueSortKey(t.dueDate);

      if(t.nextControlDate) return t.nextControlDate;

      if(t.controlAlways) return "0000-00-00";

      return "9999-99-99";

    };

    if(u.role==="boss" && deptFilter==="all"){

      const deptName = (t)=> t.departmentId ? (getDeptById(t.departmentId)?.name || "Відділ") : "Особисто";

      const deptKey = (t)=> `${t.departmentId ? "0" : "1"}_${deptName(t)}`;

      const dk = deptKey(a).localeCompare(deptKey(b));

      if(dk!==0) return dk;

      const ao = Number.isFinite(a.deptOrder) ? a.deptOrder : null;

      const bo = Number.isFinite(b.deptOrder) ? b.deptOrder : null;

      if(ao!==null && bo!==null && ao!==bo) return ao - bo;

      const ba = bucket(a);

      const bb = bucket(b);

      if(ba!==bb) return ba - bb;

      const dka = dateKey(a);

      const dkb = dateKey(b);

      if(dka!==dkb) return dka.localeCompare(dkb);

      return (a.title || "").localeCompare(b.title || "");

    }

    if(isDeptScope){

      const ao = Number.isFinite(a.deptOrder) ? a.deptOrder : null;

      const bo = Number.isFinite(b.deptOrder) ? b.deptOrder : null;

      if(ao!==null && bo!==null && ao!==bo) return ao - bo;

      const ba = bucket(a);

      const bb = bucket(b);

      if(ba!==bb) return ba - bb;

      const dka = dateKey(a);

      const dkb = dateKey(b);

      if(dka!==dkb) return dka.localeCompare(dkb);

      return (a.title || "").localeCompare(b.title || "");

    }



    const ar = (a.status==="очікує_підтвердження") ? 0 : 1;

    const br = (b.status==="очікує_підтвердження") ? 0 : 1;

    if(ar!==br) return ar-br;

    const ao = isOverdue(a) ? 0 : 1;

    const bo = isOverdue(b) ? 0 : 1;

    if(ao!==bo) return ao-bo;

    const anc = controlSortKey(a);

    const bnc = controlSortKey(b);

    if(anc!==bnc) return anc.localeCompare(bnc);

    const ad = dueSortKey(a.dueDate);

    const bd = dueSortKey(b.dueDate);

    return ad.localeCompare(bd);

  };



  const filterFn = (t)=>{

    if(filter==="активні") return t.status!=="закрито" && t.status!=="скасовано";

    if(filter==="прострочені") return isOverdue(t);

    if(filter==="очікує_підтвердження") return t.type==="managerial" && t.status==="очікує_підтвердження";

    if(filter==="блокери") return ["блокер","очікування"].includes(t.status);

    if(filter==="без_оновлень") return staleTask(t,7);

    if(filter==="закриті") return t.status==="закрито";

    return true;

  };



  const filtered = tasks.filter(filterFn).filter(matchesSearch).sort(taskSort);

  const announcementsMatched = announcements.filter(matchesSearch);

  const sortAnnouncements = (list)=>{

    if(!list.length) return [];

    const allHaveOrder = list.every(t=>Number.isFinite(t.annOrder));

    const sorted = list.slice().sort((a,b)=>{

      if(allHaveOrder){

        const ao = a.annOrder;

        const bo = b.annOrder;

        if(ao !== bo) return ao - bo;

      }

      return (b.updatedAt || "").localeCompare(a.updatedAt || "");

    });

    return sorted;

  };

  const announcementsFiltered = sortAnnouncements(announcementsMatched.filter(filterFn));

  const announcementsActive = sortAnnouncements(announcementsMatched

    .filter(t=>t.status!=="закрито" && t.status!=="скасовано"));

  const announcementsClosed = sortAnnouncements(announcementsMatched

    .filter(t=>t.status==="закрито"));



  const chips = `

    <div class="task-menu-row task-menu-row-primary">

      <div class="chips task-chips status-chips modern-status-chips">

        <div class="chip ${filter==="активні"?"active":""}" data-action="setTaskFilter" data-arg1="активні"><span class="chip-ico">📌</span><span class="chip-text">Активні</span></div>

        <div class="chip ${filter==="очікує_підтвердження"?"active":""}" data-action="setTaskFilter" data-arg1="очікує_підтвердження"><span class="chip-ico">🟣</span><span class="chip-text">Очікує</span></div>

        <div class="chip ${filter==="прострочені"?"active":""}" data-action="setTaskFilter" data-arg1="прострочені"><span class="chip-ico">🟠</span><span class="chip-text">Прострочені</span></div>

        <div class="chip ${filter==="блокери"?"active":""}" data-action="setTaskFilter" data-arg1="блокери"><span class="chip-ico">⛔</span><span class="chip-text">Блокери</span></div>

        <div class="chip ${filter==="без_оновлень"?"active":""}" data-action="setTaskFilter" data-arg1="без_оновлень"><span class="chip-ico">⏳</span><span class="chip-text">Без оновлень</span></div>

        <div class="chip ${filter==="закриті"?"active":""}" data-action="setTaskFilter" data-arg1="закриті"><span class="chip-ico">✅</span><span class="chip-text">Закриті</span></div>

      </div>

      ${u.role==="boss" ? `<button class="btn ghost btn-analytics-short" data-action="openTasksAnalytics" title="Аналітика по вибраному контексту">📊 Аналітика</button>` : ``}

    </div>

  `;

  const statusChips = isPersonalScope ? "" : chips;

  const personalChips = showAnnouncementsScope ? `

    <div class="task-menu-row task-menu-row-secondary">

      <div class="chips task-chips personal-chips modern-personal-chips">

        <div class="chip ${personalFilter==="all"?"active":""}" data-action="setTaskPersonalFilter" data-arg1="all">Все</div>

        <div class="chip ${personalFilter==="tasks"?"active":""}" data-action="setTaskPersonalFilter" data-arg1="tasks">Задачі</div>

        <div class="chip ${personalFilter==="announcements"?"active":""}" data-action="setTaskPersonalFilter" data-arg1="announcements">Оголошення</div>

      </div>

    </div>

  ` : ``;

  const deptChips = (u.role==="boss") ? `

    <div class="task-menu-row task-menu-row-depts">

      <div class="chips dept-chips dept-segments modern-dept-segments">

        ${STATE.departments.map(d=>{

          const active = deptFilter===d.id ? "active" : "";

          return `

            <div class="chip ${active}" data-action="setTaskDeptFilter" data-arg1="${d.id}" title="${htmlesc(d.name)}">

              <span class="dept-label">${htmlesc(deptShortLabel(d))}</span>

            </div>

          `;

        }).join("")}

        <div class="chip ${(deptFilter==="personal" && personalFilter==="tasks") ? "active" : ""}" data-action="openMyTasks">Мої</div>

        <div class="chip ${(deptFilter==="personal" && personalFilter==="announcements") ? "active" : ""}" data-action="openAnnouncementsAudience" data-arg1="all">Оголошення</div>

      </div>

    </div>

  ` : ``;

  const searchUi = isPersonalScope ? "" : `

    <div class="field search-inline">

      <label>Пошук задач / оголошень</label>

      <div class="row" style="gap:8px;">

        <input id="taskSearchInput" type="text" value="${htmlesc(UI.taskSearch)}" placeholder="" data-change="setTaskSearchFromInput" />

        ${UI.taskSearch ? `<button class="btn ghost" data-action="clearTaskSearch">Скинути</button>` : ``}

      </div>

    </div>

  `;

  const searchBlock = searchUi ? `<div class="task-search">${searchUi}</div>` : ``;

  const showTasks = effectivePersonalFilter!=="announcements";

  const showAnns = showAnnouncementsScope && effectivePersonalFilter!=="tasks";

  const annDisplay = (filter==="активні") ? announcementsActive : announcementsFiltered;

  const shownCount = (showTasks ? filtered.length : 0) + (showAnns ? annDisplay.length : 0);

  const totalCount = (showTasks ? tasks.length : 0) + (showAnns ? announcements.length : 0);

  const activeCount = (showTasks ? tasks.filter(t=>t.status!=="закрито" && t.status!=="скасовано").length : 0)

    + (showAnns ? announcements.filter(t=>t.status!=="закрито" && t.status!=="скасовано").length : 0);

  const closedCount = (showTasks ? tasks.filter(t=>t.status==="закрито" || t.status==="скасовано").length : 0)

    + (showAnns ? announcements.filter(t=>t.status==="закрито" || t.status==="скасовано").length : 0);

  const filterLabelMap = {

    "активні": "Активні",

    "очікує_підтвердження": "Очікує підтвердження",

    "прострочені": "Прострочені",

    "блокери": "Блокери",

    "без_оновлень": "Без оновлень",

    "закриті": "Закриті"

  };

  const currentFilterLabel = filterLabelMap[filter] || "Активні";

  const currentScopeLabel = (()=> {

    if(deptFilter==="personal"){

      if(effectivePersonalFilter==="tasks") return "Мої задачі";

      if(effectivePersonalFilter==="announcements") return "Оголошення";

      return "Мої / оголошення";

    }

    if(deptFilter && deptFilter!=="all"){

      const dept = getDeptById(deptFilter);

      return dept ? dept.name : deptFilter;

    }

    return u.role==="boss" ? "Усі відділи" : "Мій відділ";

  })();

  const currentCountLabel = (filter==="активні")

    ? `${shownCount} із ${activeCount} активних`

    : `${shownCount} із ${totalCount} записів`;

  const deptNoteActionBtn = (()=> {

    const deptId = (u.role === "boss") ? deptFilter : u.departmentId;

    if(!deptId || deptId==="all" || deptId==="personal") return "";

    const dept = getDeptById(deptId);

    if(!dept) return "";

    const noteRaw = (dept.note || "").trim();

    const {isDeptHeadLike} = asDeptRole(u);

    const canEditNote = !u.readOnly && (u.role==="boss" || (isDeptHeadLike && u.departmentId===deptId));

    if(!noteRaw && !canEditNote) return "";

    const tip = noteRaw

      ? `<span class="dept-note-inline" title="${htmlesc(noteRaw)}"><span class="dept-note-label">Примітка:</span><span class="dept-note-text rich-text">${richText(noteRaw)}</span></span>`

      : "";

    const editBtn = canEditNote

      ? `<button type="button" class="btn ghost btn-mini dept-note-btn" data-action="openDeptNote" data-arg1="${dept.id}" title="Примітка">✏️</button>`

      : "";

    return `

      <span class="dept-note-tip">

        ${editBtn}

        ${tip}

      </span>

    `;

  })();

  const taskSourceLabel = u.role==="boss"

    ? (Array.isArray(DB_TASKS_CACHE)

        ? "D1"

        : (DB_TASKS_LOADING ? "D1 (завантаження...)" : "local state"))

    : "";

  const taskSourceHint = u.role==="boss"

    ? `<div class="subhint">джерело задач: <span class="mono">${taskSourceLabel}</span>${DB_TASKS_ERROR ? `, fallback після помилки` : ``}</div>`

    : ``;

  const searchHint = (filter==="активні")

    ? `<div class="hint task-count-hint">Показано: <span class="mono">${shownCount}</span> із <span class="mono">${activeCount}</span> активних ${deptNoteActionBtn || ""}${taskSourceHint}</div>`

    : `<div class="hint task-count-hint">Показано: <span class="mono">${shownCount}</span> із <span class="mono">${totalCount}</span> (всього) ${deptNoteActionBtn || ""}<div class="subhint">активні <span class="mono">${activeCount}</span>, закриті <span class="mono">${closedCount}</span></div>${taskSourceHint}</div>`;

  const taskHeadSummary = `

    <div class="task-head-summary">

      <span class="task-head-pill accent">Режим: ${htmlesc(currentFilterLabel)}</span>

      <span class="task-head-pill">Контекст: ${htmlesc(currentScopeLabel)}</span>

      <span class="task-head-pill strong">Показано: <span class="mono">${htmlesc(currentCountLabel)}</span></span>

      ${u.role==="boss" ? `<span class="task-head-pill subtle">Джерело: <span class="mono">${htmlesc(taskSourceLabel)}</span></span>` : ``}

    </div>

  `;

  const announcementBtn = (u.role==="boss" && !u.readOnly && showAnnouncementsScope)

    ? `<button class="btn ghost" data-action="openCreateAnnouncement">📣 Оголошення</button>`

    : ``;

  const densityBtn = `<button class="btn ghost btn-density ${UI.taskDensity==="compact" ? "active" : ""}" data-action="toggleTaskDensity" title="Щільність списку задач">${UI.taskDensity==="compact" ? "▥ Щільно" : "☰ Зручно"}</button>`;

  const buildDeptIndexMap = (list)=>{

    const map = {};

    const counts = {};

    list.forEach(t=>{

      const key = t.departmentId || "personal";

      counts[key] = (counts[key] || 0) + 1;

      map[t.id] = counts[key];

    });

    return map;

  };

  UI.taskIndexMap = buildDeptIndexMap(filtered);



  const showDoneToggle = (filter === "активні");

  const completed = showDoneToggle

    ? tasks.filter(t=>t.status==="закрито").filter(matchesSearch).sort(taskSort)

    : [];

  const completedByDept = {};

  if(showDoneToggle && completed.length){

    completed.forEach(t=>{

      const key = deptLabel(t);

      if(!completedByDept[key]) completedByDept[key] = [];

      completedByDept[key].push(t);

    });

  }



  const isScopeAll = (u.role==="boss" && deptFilter==="all");

  const renderTaskItem = (t, idx)=>{

    const titleTypeClass = (t.type==="managerial")

      ? "task-title-type-managerial"

      : (t.type==="internal")

        ? "task-title-type-internal"

        : "task-title-type-personal";



    const numbering = `${idx + 1}.`;

    const deptName = t.departmentId ? (getDeptById(t.departmentId)?.name || "Відділ") : "Особисто";

    const respName = getUserById(t.responsibleUserId)?.name || "—";

    const titleHtml = highlightMatch(t.title || "");

    const isAnn = isAnnouncement(t);

    const canDragAnn = isAnn && u.role==="boss" && !u.readOnly;

    const canDragTask = !isAnn && canEditTask(u, t) && !u.readOnly && t.status!=="закрито" && t.status!=="скасовано";

    const annDragAttrs = canDragAnn ? `draggable="true" data-ann-draggable="1"` : "";

    const taskDragAttrs = canDragTask ? `draggable="true" data-task-draggable="1"` : "";

    const annDragClass = canDragAnn ? "ann-draggable" : "";

    const taskDragClass = canDragTask ? "task-draggable" : "";

    const annLabel = isAnn ? announcementAudienceLabel(t.audience) : "";

    const searchMeta = taskSearch

      ? `<div class="task-search-meta">ID: <span class="mono">${highlightMatch(t.id)}</span> • ${highlightMatch(deptName)} • ${highlightMatch(respName)}${isAnn ? ` • ${highlightMatch(annLabel)}` : ""}</div>`

      : "";

    const meetingMeta = (isAnn && t.audience==="meeting") ? meetingAnnouncementMeta(t) : "";

    const meetingHtml = meetingMeta ? `<div class="ann-meta">🗣 ${htmlesc(meetingMeta)}</div>` : "";



    const dueShort = t.dueDate ? dueDisplay(t.dueDate) : "—";

    const statusChip = {cls: statusBadgeClass(t.status), label: statusLabel(t.status), icon: statusIcon(t.status)};

    const cx = taskComplexity(t);

    const cxLabel = cx ? complexityLabel(cx) : "—";

    const cxIcon = complexityIcon(cx);

    const cxHard = (cx === "складна");

    const ctrl = controlMeta(t);

    const dueHot = !!t.dueDate && cxHard;

    const isBlocked = (t.status==="блокер" || t.status==="очікування");

    const blocker = isBlocked ? lastBlockerUpdate(t) : null;

    const blockerNoteRaw = blocker?.note ? normalizeBlockerNote(blocker.note) : "";

    const blockerNote = blockerNoteRaw ? htmlesc(blockerNoteRaw).slice(0,120) : "";

    const isLate = isOverdue(t);

    const isDueTodayTask = isDueToday(t) && !isLate;

    const isDone = t.status==="закрито";

    const hideStatus = isAnn || isDone || (t.status==="в_процесі" && !t.dueDate && (t.controlAlways || t.nextControlDate));

    const descRaw = (t.description || "");

    const hasDesc = descRaw.trim().length > 0;

    const descLabel = isAnn ? "Текст" : "Опис";

    const descStartsWithBreak = /^\s*\r?\n/.test(descRaw);

    const descPrefix = descStartsWithBreak ? `${descLabel}:<br/>` : `${descLabel}: `;

    const descHtml = (!isAnn && hasDesc) ? `<div class="task-desc rich-text">${descPrefix}${richText(descRaw)}</div>` : "";

    const annDesc = (isAnn && t.audience==="meeting" && hasDesc) ? `<div class="task-desc rich-text">Опис:${descStartsWithBreak ? "<br/>" : " "}${richText(descRaw)}</div>` : "";

    const contextHtml = isAnn

      ? `<div class="task-context"><span class="task-context-pill">${htmlesc(annLabel || "Оголошення")}</span><span class="task-context-code mono">${htmlesc(t.id || "")}</span></div>`

      : `<div class="task-context"><span class="task-context-pill">${htmlesc(deptName)}</span>${respName !== "—" ? `<span class="task-context-pill subtle">${htmlesc(respName)}</span>` : ``}<span class="task-context-code mono">${htmlesc(t.id || "")}</span></div>`;

    const closeUpd = isDone ? getCloseUpdate(t) : null;

    const closeAt = isDone ? (closeUpd?.at || t.updatedAt || "") : "";

    const closeShort = isDone ? closeDisplay(closeAt) : "";

    const closeHint = isDone ? closeTitle(closeAt) : "";

    const closeNote = isDone ? normalizeCloseNote(closeUpd?.note || "") : "";

    const resultHtml = (!isAnn && isDone) ? `<div class="task-result">Результат:${closeNote ? htmlesc(closeNote) : "—"}</div>` : "";



    const ctrlClass = t.controlAlways ? "ctrl-always" : (t.nextControlDate ? "ctrl-date" : "");

    const canDelete = canDeleteTask(u, t);

    const deleteBtn = canDelete

      ? `<button class="task-del-btn" type="button" data-action="confirmDeleteTask" data-arg1="${t.id}" title="Видалити">🗑</button>`

      : "";

    return `

      <div class="item task-item ${isAnn ? "announcement-item" : ""} ${annDragClass} ${taskDragClass} ${isBlocked ? "is-blocker" : ""} ${t.dueDate ? "has-due" : "no-due"} ${ctrlClass} ${isDueTodayTask ? "due-today" : ""} ${isLate ? "is-overdue" : ""} ${isDone ? "is-completed" : ""}" data-type="${t.type}" data-task-id="${t.id}" ${annDragAttrs} ${taskDragAttrs}>

        <div class="row" data-action="openTask" data-arg1="${t.id}">

          <div class="task-main">

            <div class="task-line">

              <div class="task-title">

                ${contextHtml}

                <div class="name ${titleTypeClass}"><span class="task-num mono">${numbering}</span> ${titleHtml}</div>

                ${descHtml}${annDesc}

                ${resultHtml}

                ${searchMeta}

                ${meetingHtml}

                ${blockerNote ? `<div class="task-note">⛔ ${blockerNote}</div>` : ``}

              </div>

              <div class="task-meta">

                ${!hideStatus ? `<span class="task-token token-status token-action ${statusChip.cls} compact-hide" data-action="openQuickActions" data-arg1="${t.id}" title="Статус"><span class="token-ico">${statusChip.icon}</span><span class="token-text">${htmlesc(statusChip.label)}</span></span>` : ``}

                ${

                  isDone

                    ? `<span class="task-token token-due token-closed" title="${htmlesc(closeHint)}"><span class="token-ico">✅</span><span class="token-text">${htmlesc(closeShort || "—")}</span></span>`

                    : (t.dueDate

                      ? `<span class="task-token token-due ${dueHot ? "due-hot" : ""}" title="Дедлайн ${dueTitle(t.dueDate)}"><span class="token-ico">⏱</span><span class="token-text">${dueShort}</span></span>`

                    : (ctrl.label

                      ? `<span class="task-token token-due" title="${ctrl.title}"><span class="token-ico">${ctrl.label==="постійно" ? "🎯" : "🗓"}</span><span class="token-text">${htmlesc(ctrl.label)}</span></span>`

                      : ``)

                    )

                }

                ${isAnn ? `` : `<span class="task-token token-complexity ${cxHard ? "complexity-hard" : ""} compact-hide" title="Складність"><span class="token-ico">${cxIcon}</span><span class="token-text">${htmlesc(cxLabel)}</span></span>`}

                ${deleteBtn}

              </div>

            </div>

          </div>

        </div>

      </div>

    `;

  };



  const renderDoneToggle = (items, startIdx=0)=>{

    if(!items || !items.length) return "";

    const rows = items.map((t,i)=>renderTaskItem(t, startIdx + i)).join("");

    return `

      <details class="done-toggle">

        <summary>ВИКОНАНІ ЗАДАЧІ <span class="mono">${items.length}</span></summary>

        <div class="done-list">${rows}</div>

      </details>

    `;

  };



  const renderGroupedList = (items)=>{

    let current = null;

    let currentKey = null;

    let groupItems = [];

    let counts = null;

    let groupHtml = [];

    let idx = 0;

    const openAttrFor = (key)=>{

      const pref = UI.deptOpen ? UI.deptOpen[key] : undefined;

      if(pref === true) return " open";

      if(pref === false) return "";

      return taskSearch ? " open" : "";

    };

    const countBucket = (t)=>{

      if(t.dueDate) return "due";

      if(["блокер","очікування"].includes(t.status)) return "blocker";

      if(t.nextControlDate) return "controlDate";

      if(t.controlAlways) return "controlAlways";

      return "other";

    };

    const countBadge = (icon, label, count, cls)=>`

      <span class="dept-count ${cls} ${count ? "" : "zero"}" title="${label}">

        ${icon} <span class="mono">${count}</span>

      </span>

    `;

    const flush = ()=>{

      if(current === null) return;

      const doneItems = showDoneToggle ? (completedByDept[current] || []) : [];

      const doneBlock = doneItems.length ? renderDoneToggle(doneItems, groupItems.length) : "";

      const noteBody = "";

      const countsHtml = `

        <span class="dept-counts">

          ${countBadge("⏱", "Дедлайн", counts.due, "count-due")}

          ${countBadge("⛔", "Блокер", counts.blocker, "count-blocker")}

          ${countBadge("🗓", "Контроль з датою", counts.controlDate, "count-ctrl")}

          ${countBadge("🎯", "Контроль постійно", counts.controlAlways, "count-always")}

        </span>

      `;

      const openAttr = openAttrFor(currentKey || "");

      groupHtml.push(`

        <details class="dept-group dept-disclosure"${openAttr} data-dept-key="${htmlesc(currentKey || "")}">

          <summary class="dept-title">

            <span class="dept-title-text">${highlightMatch(current)}</span>

            ${countsHtml}

          </summary>

          <div class="dept-list">${groupItems.join("")}${doneBlock}${noteBody}</div>

        </details>

      `);

    };

    items.forEach(t=>{

      const label = deptLabel(t);

      const key = t.departmentId || "personal";

      if(label !== current){

        flush();

        current = label;

        currentKey = key;

        groupItems = [];

        counts = {due:0, blocker:0, controlDate:0, controlAlways:0};

        idx = 0;

      }

      const bucketKey = countBucket(t);

      if(bucketKey in counts) counts[bucketKey] += 1;

      groupItems.push(renderTaskItem(t, idx));

      idx += 1;

    });

    flush();

    return groupHtml.join("");

  };



  const emptyHint = (() => {

    if(filter==="блокери") return `<div class="hint">Немає блокерів. Якщо є перешкода — постав статус “Блокер”.</div>`;

    if(filter==="прострочені") return `<div class="hint">Немає прострочених задач.</div>`;

    if(filter==="очікує_підтвердження") return `<div class="hint">Немає задач на підтвердження.</div>`;

    if(filter==="без_оновлень") return `<div class="hint">Немає задач без оновлень &gt; 7 днів.</div>`;

    if(filter==="закриті") return `<div class="hint">Немає закритих задач за цим фільтром.</div>`;

    return `<div class="hint">Немає задач за цим фільтром.</div>`;

  })();

  let tasksList = "";

  if(filtered.length){

    if(isScopeAll){

      tasksList = renderGroupedList(filtered);

    } else {

      const doneBlock = showDoneToggle ? renderDoneToggle(completed, filtered.length) : "";

      tasksList = filtered.map(renderTaskItem).join("") + doneBlock;

    }

  } else if(showDoneToggle && completed.length){

    const doneBlock = renderDoneToggle(completed, 0);

    tasksList = `<div class="hint">Немає активних задач.</div>${doneBlock}`;

  } else {

    tasksList = emptyHint;

  }



  const canSeeMeetingAnnouncements = (u.role==="boss") || isDeptHeadLike;

  let staffAnnouncements = annDisplay.filter(t=>t.audience !== "meeting");

  let meetingAnnouncements = annDisplay.filter(t=>t.audience === "meeting" && !isMeetingHiddenToday(t));

  let meetingHiddenAnnouncements = annDisplay.filter(t=>t.audience === "meeting" && isMeetingHiddenToday(t));

  let staffClosedAnnouncements = showDoneToggle ? announcementsClosed.filter(t=>t.audience !== "meeting") : [];

  let meetingClosedAnnouncements = showDoneToggle ? announcementsClosed.filter(t=>t.audience === "meeting") : [];

  if(annAudience === "staff"){

    meetingAnnouncements = [];

    meetingHiddenAnnouncements = [];

    meetingClosedAnnouncements = [];

  }

  if(annAudience === "meeting"){

    staffAnnouncements = [];

    staffClosedAnnouncements = [];

  }

  const renderAnnouncementDone = (list)=>(

    showDoneToggle && list.length

      ? `

        <details class="done-toggle ann-done-toggle">

          <summary>Оголошення доведені <span class="mono">${list.length}</span></summary>

          <div class="done-list">${list.map(renderTaskItem).join("")}</div>

        </details>

      `

      : ``

  );

  const renderAnnouncementSection = (title, list, closedList, extraHtml="", listAttr="")=>`

    <details class="announcement-section" open>

      <summary class="announcement-title">

        ${title}

        <span class="ann-count mono">${list.length}</span>

      </summary>

      <div class="announcement-list"${listAttr}>

        ${list.length ? list.map(renderTaskItem).join("") : `<div class="hint">Немає оголошень.</div>`}

        ${renderAnnouncementDone(closedList)}

        ${extraHtml}

      </div>

    </details>

  `;

  const hiddenMeetingBlock = meetingHiddenAnnouncements.length

    ? `

      <details class="announcement-section announcement-hidden">

        <summary class="announcement-title">

          Приховані сьогодні

          <span class="ann-count mono">${meetingHiddenAnnouncements.length}</span>

        </summary>

        <div class="announcement-list">

          ${meetingHiddenAnnouncements.map(renderTaskItem).join("")}

        </div>

      </details>

    `

    : ``;

  const announcementsBlock = showAnnouncementsScope ? `

    <div class="announcement-block">

      ${renderAnnouncementSection("Оголошення для особового складу", staffAnnouncements, staffClosedAnnouncements, "", ' data-ann-list="staff"')}

      ${canSeeMeetingAnnouncements ? renderAnnouncementSection("Оголошення для наради", meetingAnnouncements, meetingClosedAnnouncements, hiddenMeetingBlock, ' data-ann-list="meeting"') : ``}

    </div>

  ` : "";



  const listParts = [];

  if(showAnnouncementsScope && effectivePersonalFilter!=="tasks"){

    listParts.push(announcementsBlock);

  }

  if(showAnnouncementsScope && effectivePersonalFilter==="all" && announcementsBlock){

    listParts.push(`<div class="section-title">Задачі</div>`);

  }

  if(effectivePersonalFilter!=="announcements"){

    listParts.push(tasksList);

  }

  const list = listParts.join("");



  const body = `

    <div class="card">

      <div class="card-h task-head">

        <div class="card-h-row">

          <div class="t">Задачі</div>

          <div class="card-actions">

          ${u.role==="boss" ? `<button class="btn ghost" data-action="openTasksExportDialog">⬇️ Excel</button>` : ``}

          ${densityBtn}

          ${announcementBtn}

          ${u.role==="boss" ? `` : `<span class="badge b-blue">Мій відділ</span>`}

          </div>

        </div>

        ${taskHeadSummary}

        ${(statusChips || personalChips || searchBlock) ? `

          <div class="card-h-row task-head-tools">

            <div class="task-filters">

              ${statusChips}

              ${personalChips}

            </div>

            ${searchBlock}

          </div>

        ` : ``}

      </div>

      <div class="card-b">

        <div class="task-toolbar-sticky">

          ${deptChips}

        </div>

        <div class="list">${list}</div>

      </div>

    </div>

  `;



  const tabs = (u.role==="boss")

    ? [

      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

      {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

      {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

      {key:ROUTES.PLAN, label:"План", ico:"📅"},

    ]

    : [

      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

    ];



  const subtitle = roleSubtitle(u);

  const fabAction = ()=>{

    if(u.role==="boss"){

      if(showAnnouncementsScope && effectivePersonalFilter==="announcements"){

        openCreateAnnouncement();

        return;

      }

      showSheet("\u0414\u043e\u0434\u0430\u0442\u0438", `

        <div class="actions">

          <button class="btn primary" data-action="hideThen" data-next="openCreateTask" data-arg1="personal">+ \u041c\u043e\u044f \u0437\u0430\u0434\u0430\u0447\u0430</button>

          <button class="btn ghost" data-action="hideThen" data-next="openCreateTask" data-arg1="managerial">+ \u0423\u043f\u0440\u0430\u0432\u043b\u0456\u043d\u0441\u044c\u043a\u0430</button>

        </div>

        <div class="sep"></div>

        <button class="btn ghost" data-action="hideSheet">\u0417\u0430\u043a\u0440\u0438\u0442\u0438</button>

      `);

    } else {

      openCreateTask('internal');

    }

  };



  appShell({title:"Задачі", subtitle, bodyHtml: body, showFab:!u.readOnly, fabAction, tabs});



  if(showAnnouncementsScope && u.role==="boss" && !u.readOnly){

    document.querySelectorAll('.announcement-list[data-ann-list]').forEach((listEl)=>{

      let dragging = null;

      listEl.querySelectorAll('.task-item.announcement-item[draggable="true"]').forEach(el=>{

        el.addEventListener("dragstart", (e)=>{

          dragging = el;

          el.classList.add("dragging");

          e.dataTransfer.effectAllowed = "move";

          e.dataTransfer.setData("text/plain", el.getAttribute("data-task-id") || "");

        });

        el.addEventListener("dragend", ()=>{

          if(dragging) dragging.classList.remove("dragging");

          dragging = null;

        });

      });

      listEl.addEventListener("dragover", (e)=>{

        if(!dragging) return;

        e.preventDefault();

        const afterEl = getAnnouncementDragAfterElement(listEl, e.clientY);

        if(afterEl == null){

          listEl.appendChild(dragging);

        } else {

          listEl.insertBefore(dragging, afterEl);

        }

      });

      listEl.addEventListener("drop", (e)=>{

        if(!dragging) return;

        e.preventDefault();

        const ids = [...listEl.querySelectorAll(":scope > .task-item.announcement-item")]

          .map(el=>el.getAttribute("data-task-id"))

          .filter(Boolean);

        applyAnnouncementOrder(ids);

      });

    });

  }



  const setupTaskDrag = (listEl, deptKey)=>{

    const itemSelector = ':scope > .task-item:not(.announcement-item):not(.is-completed)';

    const items = [...listEl.querySelectorAll(itemSelector)];

    if(!items.length) return;

    let dragging = null;

    items.filter(el=>el.getAttribute("draggable")==="true").forEach(el=>{

      el.addEventListener("dragstart", (e)=>{

        dragging = el;

        el.classList.add("dragging");

        e.dataTransfer.effectAllowed = "move";

        e.dataTransfer.setData("text/plain", el.getAttribute("data-task-id") || "");

      });

      el.addEventListener("dragend", ()=>{

        if(dragging) dragging.classList.remove("dragging");

        dragging = null;

      });

    });

    listEl.addEventListener("dragover", (e)=>{

      if(!dragging) return;

      e.preventDefault();

      const afterEl = getTaskDragAfterElement(listEl, e.clientY);

      if(afterEl == null){

        listEl.appendChild(dragging);

      } else {

        listEl.insertBefore(dragging, afterEl);

      }

    });

    listEl.addEventListener("drop", (e)=>{

      if(!dragging) return;

      e.preventDefault();

      const ids = [...listEl.querySelectorAll(itemSelector)]

        .map(el=>el.getAttribute("data-task-id"))

        .filter(Boolean);

      applyDeptOrder(deptKey, ids);

    });

  };



  if(!u.readOnly){

    const groupLists = document.querySelectorAll(".dept-group .dept-list");

    if(groupLists.length){

      groupLists.forEach(listEl=>{

        const deptKey = listEl.closest(".dept-group")?.getAttribute("data-dept-key") || "personal";

        setupTaskDrag(listEl, deptKey);

      });

    } else {

      const listEl = document.querySelector(".list");

      if(listEl){

        const first = listEl.querySelector('.task-item[data-task-id]:not(.announcement-item)');

        if(first){

          const t = STATE.tasks.find(x=>x.id===first.getAttribute("data-task-id"));

          const deptKey = t?.departmentId || "personal";

          setupTaskDrag(listEl, deptKey);

        }

      }

    }

  }



  document.querySelectorAll(".dept-group.dept-disclosure").forEach((el)=>{

    el.addEventListener("toggle", ()=>{

      const key = el.getAttribute("data-dept-key") || "";

      if(!key) return;

      if(!UI.deptOpen) UI.deptOpen = {};

      UI.deptOpen[key] = el.open;

    });

  });



  const fab = document.getElementById("fab");

  if(fab){

    document.querySelectorAll(".dept-disclosure > summary").forEach((el)=>{

      el.addEventListener("dblclick", (e)=>{

        if(e.button !== 0) return;

        e.preventDefault();

        e.stopPropagation();

        fab.click();

      });

    });

    document.querySelectorAll(".dept-chips .chip").forEach((el)=>{

      el.addEventListener("dblclick", (e)=>{

        if(e.button !== 0) return;

        if(el.dataset.action === "setTaskDeptFilter") return;

        fab.click();

      });

    });

  }

}



function quickActionsForTask(u, t){

  const {isDeptHeadLike} = asDeptRole(u);

  const isBoss = (u.role==="boss" && !u.readOnly);

  const canUpdate = isBoss || isDeptHeadLike;

  const canDelete = canDeleteTask(u, t);

  if(!canUpdate || t.status==="закрито"){

    return canDelete

      ? `<div class="actions"><button class="btn danger" data-action="confirmDeleteTask" data-arg1="${t.id}">🗑 Видалити</button></div>`

      : "";

  }



  const isAnn = isAnnouncement(t);

  if(isAnn){

    if(!isBoss) return "";

    const btns = [];

    if(t.audience==="meeting"){

      btns.push(`<button class="btn ok" data-action="markMeetingAnnounced" data-arg1="${t.id}">🗣 Озвучено сьогодні</button>`);

      btns.push(`<button class="btn ghost" data-action="openMeetingRepeat" data-arg1="${t.id}">🔁 Повторити</button>`);

      const hiddenToday = isMeetingHiddenToday(t);

      btns.push(`<button class="btn ghost" data-action="toggleMeetingHideToday" data-arg1="${t.id}">${hiddenToday ? "👁 Повернути сьогодні" : "🙈 Сховати сьогодні"}</button>`);

    }

    btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Виконано</button>`);

    if(canEditTask(u, t)){

      btns.push(`<button class="btn ghost" data-action="openEditTask" data-arg1="${t.id}">✏️ Редагувати</button>`);

    }

    if(canDelete) btns.push(`<button class="btn danger" data-action="confirmDeleteTask" data-arg1="${t.id}">🗑 Видалити</button>`);

    return `<div class="actions">${btns.join("")}</div>`;

  }



  const btns = [];

  const isBlocked = (t.status==="блокер" || t.status==="очікування");

  const blockerBtn = isBlocked

    ? `<button class="btn warn" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="в_процесі">🔓 Розблок</button>`

    : `<button class="btn warn" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="блокер">⛔ Блокер</button>`;



  if(isBoss){

    if(t.type==="managerial" && t.status==="очікує_підтвердження"){

      btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Підтвердити</button>`);

    } else {

      btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Закрити</button>`);

    }

    btns.push(blockerBtn);

  } else {

    if(t.type==="internal"){

      btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Закрити</button>`);

    }

    if(t.type==="managerial"){

      btns.push(`<button class="btn violet" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="очікує_підтвердження">🟣 Запит закриття</button>`);

    }

    btns.push(blockerBtn);

  }



  if(canEditTask(u, t)){

    btns.push(`<button class="btn ghost" data-action="openEditTask" data-arg1="${t.id}">✏️ Редагувати</button>`);

  }

  if(canDelete) btns.push(`<button class="btn danger" data-action="confirmDeleteTask" data-arg1="${t.id}">🗑 Видалити</button>`);

  return `<div class="actions">${btns.join("")}</div>`;

}



function openStatusReasonModal(taskId, status){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!t) return;

  if(u.readOnly && t.type==="personal" && !isAnnouncement(t)){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування недоступне.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const isBlocking = status === "блокер";

  const isClosing = status === "закрито";

  const title = isBlocking

    ? "Блокер: вкажи причину"

    : (isClosing ? "Закриття: результат" : "Розблокування: причина");

  const label = isBlocking

    ? "Причина блокера"

    : (isClosing ? "Результат / причина закриття" : "Причина розблокування");

  const hint = isBlocking

    ? "Опиши, що заважає або кого/чого очікуємо."

    : (isClosing ? "Коротко: що зроблено або який результат." : "Що змінилося і чому можна рухатись далі.");

  const placeholder = isBlocking

    ? "Наприклад: немає доступу / чекаємо підтвердження / бракує ресурсу."

    : (isClosing ? "Наприклад: виконано повністю / передано результат / підтверджено." : "Наприклад: отримали доступ / підтвердили рішення / ресурс з’явився.");



  showSheet(title, `

    <div class="hint">${hint}</div>

    <div class="field">

      <label>${label}</label>

      <textarea id="statusReason" placeholder="${placeholder}"></textarea>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="submitStatusReason" data-arg1="${t.id}" data-arg2="${status}">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}



function openEditAnnouncement(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Оголошення може редагувати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  showSheet("Редагувати оголошення", `

    <div class="hint">Оголошення: <b>${htmlesc(t.id)}</b></div>

    <div class="field">

      <label>Аудиторія</label>

      <select id="aAudience">

        <option value="staff" ${t.audience==="staff" ? "selected" : ""}>Особовий склад</option>

        <option value="meeting" ${t.audience==="meeting" ? "selected" : ""}>Нарада (керівництво)</option>

      </select>

    </div>

    <div class="field">

      <label>Заголовок</label>

      <input id="aTitle" value="${htmlesc(t.title)}" />

    </div>

    <div class="field">

      <label>Опис (для наради, опційно)</label>

      <textarea id="aDesc">${htmlesc(t.description || "")}</textarea>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="saveAnnouncementEdits" data-arg1="${t.id}">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}



function markMeetingAnnounced(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.role!=="boss" || !isAnnouncement(t) || t.audience!=="meeting") return;

  const today = kyivDateStr();

  const count = Number(t.meetingRepeatCount || 0) + 1;

  updateTask(taskId, {meetingRepeatCount: count, meetingLastDate: today}, u.id, `Озвучено: ${fmtDate(today)}`);

  hideSheet();

  render();

  showToast("Озвучено", "ok");

}

function openMeetingRepeat(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.role!=="boss" || !isAnnouncement(t) || t.audience!=="meeting") return;

  hideSheet();

  const meta = meetingAnnouncementMeta(t);

  showSheet("Повторити оголошення", `

    <div class="hint">Оголошення: <b>${htmlesc(t.id)}</b></div>

    ${meta ? `<div class="hint" style="margin-top:6px;">🗣 ${htmlesc(meta)}</div>` : ``}

    <div class="field" style="margin-top:10px;">

      <label>Наступне озвучення</label>

      <input id="annRepeatDate" type="date" value="${htmlesc(t.meetingNextDate || "")}" />

    </div>

    <div class="actions" style="margin-top:10px;">

      <button class="btn ghost btn-mini" data-action="setMeetingRepeatTomorrow">Завтра</button>

      <label class="hint" style="margin-left:6px;"><input id="annRepeatClear" type="checkbox" /> Прибрати дату</label>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="applyMeetingRepeat" data-arg1="${t.id}">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}

function toggleMeetingHideToday(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.role!=="boss" || !isAnnouncement(t) || t.audience!=="meeting") return;

  const today = kyivDateStr();

  const hide = !isMeetingHiddenToday(t);

  const note = hide ? `Приховано сьогодні: ${fmtDate(today)}` : "Приховано сьогодні: скасовано";

  updateTask(taskId, {meetingSkipDate: hide ? today : null}, u.id, note);

  hideSheet();

  render();

  showToast(hide ? "Приховано до завтра" : "Повернуто в список", "ok");

}

function setMeetingRepeatTomorrow(){

  const input = document.getElementById("annRepeatDate");

  if(!input) return;

  input.value = addDays(kyivDateStr(), 1);

  const clear = document.getElementById("annRepeatClear");

  if(clear) clear.checked = false;

}

function applyMeetingRepeat(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.role!=="boss" || !isAnnouncement(t) || t.audience!=="meeting") return;

  const clear = !!document.getElementById("annRepeatClear")?.checked;

  const nextDate = clear ? null : (document.getElementById("annRepeatDate")?.value || null);

  const note = nextDate ? `Наступне озвучення: ${fmtDate(nextDate)}` : "Наступне озвучення: прибрано";

  updateTask(taskId, {meetingNextDate: nextDate || null}, u.id, note);

  hideSheet();

  render();

  showToast(nextDate ? "Дату збережено" : "Дату прибрано", "ok");

}



function appendReportText(id, text){

  if(!text) return;

  const el = document.getElementById(id);

  if(!el) return;

  const cur = el.value.trim();

  el.value = cur ? `${cur}\n${text}` : text;

}

function fillReportIfEmpty(id, text){

  if(!text) return;

  const el = document.getElementById(id);

  if(!el) return;

  if(!el.value.trim()) el.value = text;

}

function buildReportTemplate(){

  return {

    done: "• [задача/подія] — результат",

    progress: "• [задача] — поточний стан / що залишилось",

    blocked: "• [задача] — причина / кого чекаємо",

  };

}

function applyReportTemplate(){

  const t = buildReportTemplate();

  fillReportIfEmpty("rDone", t.done);

  fillReportIfEmpty("rProg", t.progress);

  fillReportIfEmpty("rBlock", t.blocked);

  showToast("Шаблон вставлено в порожні поля.", "info");

}

function buildAutoReport(){

  const u = currentSessionUser();

  if(!u) return {done:"", progress:"", blocked:"", empty:true};

  const today = kyivDateStr();

  const updates = STATE.taskUpdates

    .filter(x=>x.authorUserId===u.id && toDateOnly(x.at)===today)

    .sort((a,b)=>b.at.localeCompare(a.at));



  if(!updates.length) return {done:"", progress:"", blocked:"", empty:true};



  const latestByTask = {};

  updates.forEach(upd=>{

    if(!latestByTask[upd.taskId]) latestByTask[upd.taskId] = upd;

  });



  const done = [];

  const progress = [];

  const blocked = [];



  Object.values(latestByTask).forEach(upd=>{

    const task = STATE.tasks.find(t=>t.id===upd.taskId);

    const label = task ? `${task.id} — ${task.title}` : `Задача ${upd.taskId}`;

    const noteText = upd.note

      ? ((upd.status==="блокер" || upd.status==="очікування") ? normalizeBlockerNote(upd.note) : upd.note)

      : "";

    const line = `• ${label}${noteText ? `: ${noteText}` : ""}`;

    if(upd.status==="закрито"){

      done.push(line);

    } else if(upd.status==="блокер" || upd.status==="очікування"){

      blocked.push(line);

    } else {

      progress.push(line);

    }

  });



  return {

    done: done.join("\n"),

    progress: progress.join("\n"),

    blocked: blocked.join("\n"),

    empty: false

  };

}

function autoFillReport(){

  const data = buildAutoReport();

  if(data.empty){

    showToast("Сьогодні немає оновлень задач для автозаповнення.", "info");

    return;

  }

  appendReportText("rDone", data.done);

  appendReportText("rProg", data.progress);

  appendReportText("rBlock", data.blocked);

  showToast("Звіт автозаповнено з оновлень задач.", "ok");

}



function submitStatusReason(taskId, status){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;



  const reason = (document.getElementById("statusReason")?.value || "").trim();

  if(reason.length < 3){

    showToast("Вкажи причину (мін. 3 символи).", "warn");

    return;

  }



  const {isDeptHeadLike} = asDeptRole(u);

  const isBoss = (u.role==="boss" && !u.readOnly);

  if(isAnnouncement(t) && !isBoss){

    showSheet("Немає прав", `<div class="hint">Оголошення може змінювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(!(isBoss || isDeptHeadLike)){

    showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може змінювати статуси.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(!isBoss && t.departmentId !== u.departmentId){

    showSheet("Немає прав", `<div class="hint">Ви не маєте доступу до іншого відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(!isBoss && t.type==="managerial" && status==="закрито"){

    showSheet("Обмеження", `<div class="hint">Управлінську задачу закриває тільки керівник. Використайте “Запит закриття”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const wasBlocked = (t.status==="блокер" || t.status==="очікування");

  const isBlocking = (status === "блокер");

  const stillBlocked = (status==="блокер" || status==="очікування");

  let note = `Статус → ${statusLabel(status)}: ${reason}`;

  if(isBlocking){

    note = `Блокер: ${reason}`;

  } else if(status==="закрито"){

    note = reason;

  } else if(wasBlocked && !stillBlocked){

    note = `Розблоковано → ${statusLabel(status)}: ${reason}`;

  }



  updateTask(taskId, {status}, u.id, note);

  hideSheet();

  render();

  showToast(`Статус оновлено: ${statusLabel(status)}`, "ok");

}



function setTaskStatus(taskId, status, bypassConfirm=false){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!t) return;



  const {isDeptHeadLike} = asDeptRole(u);

  if(isAnnouncement(t) && u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Оголошення може змінювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(!(u.role==="boss" || isDeptHeadLike)){

    showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може змінювати статуси.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(u.role!=="boss" && t.departmentId !== u.departmentId){

    showSheet("Немає прав", `<div class="hint">Ви не маєте доступу до іншого відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(u.role!=="boss" && t.type==="managerial" && status==="закрито"){

    showSheet("Обмеження", `<div class="hint">Управлінську задачу закриває тільки керівник. Використайте “Запит закриття”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(status==="закрито"){

    return openStatusReasonModal(taskId, status);

  }



  const isBlocking = (status === "блокер");

  const isUnblocking = (t.status==="блокер" || t.status==="очікування") && !(status==="блокер" || status==="очікування");

  if(isBlocking || isUnblocking){

    return openStatusReasonModal(taskId, status);

  }



  updateTask(taskId, {status}, u.id, `Статус → ${statusLabel(status)}`);

  render();

  showToast(`Статус оновлено: ${statusLabel(status)}`, "ok");

}



function confirmTaskClose(taskId){

  setTaskStatus(taskId, "закрито", true);

}



function setControlDate(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!t) return;



  const {isDeptHeadLike} = asDeptRole(u);

  if(!(u.role==="boss" || isDeptHeadLike)){

    showSheet("Немає прав", `<div class="hint">Тільки керівник або начальник відділу (в.о.) може змінювати контрольну дату.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(t.dueDate){

    showSheet("Контроль недоступний", `<div class="hint">Контроль недоступний.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const isAlways = !!t.controlAlways;

  showSheet("Контрольна дата", `

    <div class="hint">Контроль — коли потрібно повернутися до задачі (або постійний контроль).</div>

    <div class="field">

      <label>Контроль</label>

      <input id="ctrlDate" type="date" value="${isAlways ? "" : (t.nextControlDate ?? kyivDateStr())}" ${isAlways ? "disabled" : ""} />

    </div>

    <div class="field">

      <label><input id="ctrlAlways" type="checkbox" data-change="toggleCtrlAlways" ${isAlways ? "checked" : ""} /> Постійний контроль (без дати)</label>

    </div>

    <div class="field">

      <label><input id="ctrlClear" type="checkbox" /> Прибрати контрольну дату</label>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="applyControlDate" data-arg1="${t.id}">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}



function applyControlDate(taskId){

  const u = currentSessionUser();

  const clear = document.getElementById("ctrlClear")?.checked;

  const always = document.getElementById("ctrlAlways")?.checked;

  const d = (document.getElementById("ctrlDate")?.value || null);



  let nextControlDate = null;

  let controlAlways = false;

  let note = "Контроль → без контролю";

  let toast = "Контроль прибрано";



  if(!clear){

    if(always){

      controlAlways = true;

      note = "Контроль → постійно";

      toast = "Контроль: постійно";

    } else if(d){

      nextControlDate = d;

      note = `Контроль → ${fmtDate(d)}`;

      toast = `Контроль: ${fmtDate(d)}`;

    }

  }



  updateTask(taskId, {nextControlDate, controlAlways}, u.id, note);

  hideSheet();

  render();

  showToast(toast, "info");

}



function openTask(taskId, opts={}){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!t) return;



  if(u.role!=="boss" && t.departmentId && !canAccessDept(u, t.departmentId)){

    showSheet("Немає доступу", `<div class="hint">Ця задача належить іншому відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(t.type==="personal" && u.role!=="boss" && !isAnnouncement(t)){

    showSheet("Немає доступу", `<div class="hint">Особисті задачі бачить тільки керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(isAnnouncement(t) && !canSeeAnnouncement(u, t)){

    showSheet("Немає доступу", `<div class="hint">Це оголошення не призначене для вашої ролі.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const dept = t.departmentId ? getDeptById(t.departmentId) : null;

  const resp = getUserById(t.responsibleUserId);

  const upd = STATE.taskUpdates.filter(x=>x.taskId===t.id).sort((a,b)=>b.at.localeCompare(a.at)).slice(0,8);

  const ctrl = controlMeta(t);

  const cx = taskComplexity(t);

  const cxLabel = cx ? complexityLabel(cx) : "—";

  const cxIcon = complexityIcon(cx);

  const cxHard = (cx === "складна");

  const dueHot = !!t.dueDate && cxHard;

  const isDone = t.status==="закрито";

  const isAnn = isAnnouncement(t);

  const annLabel = isAnn ? announcementAudienceLabel(t.audience) : "";

  const descLabel = isAnn ? "Текст" : "Опис";

  const meetingMeta = (isAnn && t.audience==="meeting") ? meetingAnnouncementMeta(t) : "";

  const statusChip = {cls: statusBadgeClass(t.status), label: statusLabel(t.status), icon: statusIcon(t.status)};

  const hideStatus = isAnn || isDone || (t.status==="в_процесі" && !t.dueDate && (t.controlAlways || t.nextControlDate));

  const closeUpd = isDone ? getCloseUpdate(t) : null;

  const closeAt = isDone ? (closeUpd?.at || t.updatedAt || "") : "";

  const closeShort = isDone ? closeDisplay(closeAt) : "";

  const closeHint = isDone ? closeTitle(closeAt) : "";

  const closeNote = isDone ? normalizeCloseNote(closeUpd?.note || "") : "";

  const titleTypeClass = (t.type==="managerial")

    ? "task-title-type-managerial"

    : (t.type==="internal")

      ? "task-title-type-internal"

      : "task-title-type-personal";

  const titleClass = isDueToday(t) ? "task-title-due-today" : (t.dueDate ? "task-title-due" : titleTypeClass);

  let deptNum = UI.taskIndexMap?.[t.id];

  if(!deptNum){

    const key = t.departmentId || "personal";

    const list = getVisibleTasksForUser(u).filter(x=>(x.departmentId || "personal")===key);

    const bucket = (x)=>{

      if(x.dueDate) return 0;

      if(["блокер","очікування"].includes(x.status)) return 1;

      if(x.nextControlDate) return 2;

      if(x.controlAlways) return 3;

      return 4;

    };

    const dateKey = (x)=>{

      if(x.dueDate) return dueSortKey(x.dueDate);

      if(x.nextControlDate) return x.nextControlDate;

      if(x.controlAlways) return "0000-00-00";

      return "9999-99-99";

    };

    list.sort((a,b)=>{

      const ba = bucket(a);

      const bb = bucket(b);

      if(ba!==bb) return ba - bb;

      const dka = dateKey(a);

      const dkb = dateKey(b);

      if(dka!==dkb) return dka.localeCompare(dkb);

      return (a.title || "").localeCompare(b.title || "");

    });

    const idx = list.findIndex(x=>x.id===t.id);

    deptNum = idx>=0 ? idx+1 : null;

  }



  showSheet(isAnn ? "Оголошення" : "Картка задачі", `

    <div class="item task-sheet-compact" style="cursor:default;">

      <div class="task-line">

        <div class="task-title">

          <div class="name ${titleClass}">${deptNum ? `<span class="task-num mono">${deptNum}.</span>` : ""} ${htmlesc(t.title)}</div>

        </div>

        <div class="task-meta">

          ${!hideStatus ? `<span class="task-token token-status ${statusChip.cls} compact-hide" title="Статус"><span class="token-ico">${statusChip.icon}</span><span class="token-text">${htmlesc(statusChip.label)}</span></span>` : ``}

          ${

            isDone

              ? `<span class="task-token token-due token-closed" title="${htmlesc(closeHint)}"><span class="token-ico">✅</span><span class="token-text">${htmlesc(closeShort || "—")}</span></span>`

              : (t.dueDate

                ? `<span class="task-token token-due ${dueHot ? "due-hot" : ""}" title="Дедлайн ${dueTitle(t.dueDate)}"><span class="token-ico">⏱</span><span class="token-text">${t.dueDate ? dueDisplay(t.dueDate) : "—"}</span></span>`

              : (ctrl.label

                ? `<span class="task-token token-due" title="${ctrl.title}"><span class="token-ico">${ctrl.label==="постійно" ? "🎯" : "🗓"}</span><span class="token-text">${htmlesc(ctrl.label)}</span></span>`

                : ``)

              )

          }

          ${isAnn ? `` : `<span class="task-token token-complexity ${cxHard ? "complexity-hard" : ""} compact-hide"><span class="token-ico">${cxIcon}</span><span class="token-text">${htmlesc(cxLabel)}</span></span>`}

        </div>

      </div>



      ${meetingMeta ? `<div class="hint ann-meta">🗣 ${htmlesc(meetingMeta)}</div>` : ``}

      ${(isAnn && t.audience==="meeting" && t.description) ? `<div class="hint rich-text"><b>Опис:</b> ${richText(t.description)}</div>` : ``}

      ${isAnn ? `` : `<div class="hint rich-text"><b>${descLabel}:</b> ${t.description ? richText(t.description) : "—"}</div>`}

      ${(!isAnn && isDone) ? `<div class="hint"><b>Результат:</b>${closeNote ? htmlesc(closeNote) : "—"}</div>` : ``}



      <details class="task-disclosure" ${upd.length ? "" : "open"}>

        <summary>Оновлення (${upd.length})</summary>

        <div class="hint">

          ${upd.length ? upd.map(x=>{

            const au = getUserById(x.authorUserId);

            const who = au ? `${au.name}${isActingHead(au.id) ? " (в.о.)" : ""}` : "—";

            return `• <span class="mono">${htmlesc(x.at)}</span> — <b>${htmlesc(who)}</b>: ${htmlesc(x.note || "")}`;

          }).join("<br/>") : "Немає оновлень."}

        </div>

      </details>

    </div>

    <div class="sep"></div>

    ${

      isDone

        ? `<button class="btn primary" data-action="hideSheet">OK</button>`

        : (quickActionsForTask(u, t) || `<button class="btn primary" data-action="hideSheet">Закрити</button>`)

    }

  `, opts);

}



function openQuickActions(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.readOnly){

    showSheet("Немає доступу", `<div class="hint">Переглядовий режим — редагування вимкнено.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(isAnnouncement(t) && u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Оголошення може змінювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(u.role!=="boss"){

    const {isDeptHeadLike} = asDeptRole(u);

    if(!isDeptHeadLike){

      showSheet("Немає прав", `<div class="hint">Тільки начальник відділу (або в.о.) може змінювати статуси.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

      return;

    }

    if(t.departmentId && t.departmentId !== u.departmentId){

      showSheet("Немає доступу", `<div class="hint">Ви не маєте доступу до іншого відділу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

      return;

    }

  }



  const actions = quickActionsForTask(u, t);

  if(!actions) return openTask(taskId);

  showSheet("Швидкі дії", `

    <div class="hint">${htmlesc(t.title || t.id)}</div>

    <div class="sep"></div>

    ${actions}

    <div class="sep"></div>

    <button class="btn ghost" data-action="hideSheet">Закрити</button>

  `);

}



function openEditTask(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(isAnnouncement(t)) return openEditAnnouncement(taskId);



  if(!canEditTask(u, t)){

    showSheet("Немає прав", `<div class="hint">Ви не маєте прав редагувати цю задачу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const isBoss = (u.role==="boss" && !u.readOnly);

  const isPersonal = (t.type==="personal");

  const today = kyivDateStr();



  const deptOptions = isPersonal

    ? []

    : (isBoss ? STATE.departments : STATE.departments.filter(d=>d.id===u.departmentId));



  createTaskUserOptions = (deptId)=>{

    if(isPersonal) return [STATE.users.find(x=>x.id==="u_boss")].filter(Boolean);

    return getDeptResponsibleOptions(deptId);

  };



  const deptId = t.departmentId || (deptOptions[0]?.id ?? "");

  const noDue = !t.dueDate;

  const dueParts = splitDateTime(t.dueDate);



  showSheet("Редагувати задачу", `

    <div class="hint">

      Редагування задачі: <b>${htmlesc(t.id)}</b>

    </div>



    <div class="field">

      <label>Назва</label>

      <input id="tTitle" value="${htmlesc(t.title)}" />

    </div>



    <div class="field">

      <div class="label-row">

        <label>Опис (опційно)</label>

        ${formatToolbar("tDesc", "inline")}

      </div>

      <textarea id="tDesc" class="task-desc-input">${htmlesc(t.description || "")}</textarea>

    </div>



    ${!isPersonal ? `

      <div class="row2">

        <div class="field">

          <label>Відділ</label>

          <select id="tDept" data-change="refreshRespOptions" ${isBoss ? "" : "disabled"}>

            ${deptOptions.map(d=>`<option value="${d.id}" ${d.id===deptId ? "selected" : ""}>${htmlesc(d.name)}</option>`).join("")}

          </select>

        </div>



        <div class="field">

          <label>Відповідальний</label>

          <select id="tResp"></select>

        </div>

      </div>

    ` : `

      <div class="field">

        <label>Відповідальний</label>

        <input value="Керівник (ви)" disabled />

      </div>

    `}



    <div class="row2">

      <div class="field">

        <label>Складність</label>

        <select id="tCx">

          <option value="легка" ${(taskComplexity(t)==="легка") ? "selected" : ""}>Легка</option>

          <option value="середня" ${(taskComplexity(t)==="середня") ? "selected" : ""}>Середня</option>

          <option value="складна" ${(taskComplexity(t)==="складна") ? "selected" : ""}>Складна</option>

        </select>

      </div>



      <div class="field">

        <label>Дедлайн</label>

        <div class="row" style="display:flex;gap:8px;">

          <input id="tDue" type="date" value="${dueParts.date}" />

          <input id="tDueTime" type="time" value="${dueParts.time}" />

        </div>

      </div>

    </div>



    <div class="row3">

      <div class="field">

        <div class="toggle-row">

          <span class="toggle-label">Без дедлайну</span>

          <label class="switch">

            <input id="noDue" type="checkbox" data-change="toggleNoDue" ${noDue ? "checked" : ""} />

            <span class="slider"></span>

          </label>

        </div>

      </div>



      <div id="ctrlBlock" class="ctrl-inline">

        <div class="field">

          <input id="tCtrl" type="date" value="${t.nextControlDate ?? ""}" />

        </div>

        <div class="field">

          <div class="toggle-row">

            <span class="toggle-label">Постійний контроль</span>

            <label class="switch">

              <input id="tCtrlAlways" type="checkbox" data-change="toggleCtrlAlways" ${t.controlAlways ? "checked" : ""} />

              <span class="slider"></span>

            </label>

          </div>

        </div>

      </div>

    </div>



    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="saveTaskEdits" data-arg1="${t.id}">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);



  toggleNoDue();

  if(!isPersonal) refreshRespOptions();

  const respSel = document.getElementById("tResp");

  if(respSel && t.responsibleUserId){

    respSel.value = t.responsibleUserId;

  }

}



function saveTaskEdits(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;



  if(!canEditTask(u, t)){

    showSheet("Немає прав", `<div class="hint">Ви не маєте прав редагувати цю задачу.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const title = document.getElementById("tTitle").value.trim();

  if(!title){

    showSheet("Помилка", `<div class="hint">Вкажи назву задачі.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const desc = document.getElementById("tDesc").value || "";

  const cx = document.getElementById("tCx").value;

  const noDue = document.getElementById("noDue").checked;

  const dueDateVal = document.getElementById("tDue").value || null;

  const dueTimeVal = document.getElementById("tDueTime")?.value || "";

  const due = noDue ? null : joinDateTime(dueDateVal, dueTimeVal);

  const ctrlAlways = noDue ? !!document.getElementById("tCtrlAlways")?.checked : false;

  const ctrl = (noDue && !ctrlAlways) ? (document.getElementById("tCtrl").value || null) : null;



  if(!noDue && !dueDateVal){

    showSheet("Помилка", `<div class="hint">Вкажи дедлайн або вибери “Без дедлайну”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const isBoss = (u.role==="boss" && !u.readOnly);

  let departmentId = t.departmentId;

  let responsibleUserId = t.responsibleUserId;



  if(t.type!=="personal"){

    const deptSel = document.getElementById("tDept");

    if(isBoss && deptSel) departmentId = deptSel.value;

    if(!departmentId) departmentId = t.departmentId;

    const respSel = document.getElementById("tResp");

    if(respSel) responsibleUserId = respSel.value;



    const allowed = getDeptResponsibleOptions(departmentId).map(x=>x.id);

    if(!allowed.includes(responsibleUserId)){

      responsibleUserId = allowed[0] || responsibleUserId;

    }

  }



  const patch = {

    title,

    description: desc,

    complexity: cx,

    dueDate: due,

    nextControlDate: ctrl,

    controlAlways: ctrlAlways,

    departmentId,

    responsibleUserId,

  };



  const oldDept = t.departmentId ? (getDeptById(t.departmentId)?.name || "—") : "Особисто";

  const newDept = departmentId ? (getDeptById(departmentId)?.name || "—") : "Особисто";

  const oldResp = t.responsibleUserId ? (getUserById(t.responsibleUserId)?.name || "—") : "—";

  const newResp = responsibleUserId ? (getUserById(responsibleUserId)?.name || "—") : "—";

  const ctrlLabel = (d, always)=> always ? "постійно" : (d ? fmtDate(d) : "—");

  const changes = [];

  if(title !== t.title) changes.push(`Назва: "${shorten(t.title)}" → "${shorten(title)}"`);

  if(desc !== (t.description || "")) changes.push(`Опис: "${shorten(t.description || "")}" → "${shorten(desc)}"`);

  const prevCx = taskComplexity(t) || "середня";

  if(cx !== prevCx) changes.push(`Складність: ${complexityLabel(prevCx)} → ${complexityLabel(cx)}`);

  if(due !== t.dueDate) changes.push(`Дедлайн: ${t.dueDate ? dueTitle(t.dueDate) : "—"} → ${due ? dueTitle(due) : "—"}`);

  if(ctrlLabel(t.nextControlDate, t.controlAlways) !== ctrlLabel(ctrl, ctrlAlways)) changes.push(`Контроль: ${ctrlLabel(t.nextControlDate, t.controlAlways)} → ${ctrlLabel(ctrl, ctrlAlways)}`);

  if(departmentId !== t.departmentId) changes.push(`Відділ: ${oldDept} → ${newDept}`);

  if(responsibleUserId !== t.responsibleUserId) changes.push(`Відповідальний: ${oldResp} → ${newResp}`);

  const note = changes.length ? `Змінено: ${changes.join("; ")}` : "Редагування без змін";



  updateTask(taskId, patch, u.id, note);

  hideSheet();

  render();

  showToast("Зміни збережено", "ok");

}



function saveAnnouncementEdits(taskId){

  const u = currentSessionUser();

  const t = STATE.tasks.find(x=>x.id===taskId);

  if(!u || !t) return;

  if(u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Оголошення може редагувати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const audience = document.getElementById("aAudience")?.value || "staff";

  const title = (document.getElementById("aTitle")?.value || "").trim();

  const desc = (document.getElementById("aDesc")?.value || "").trim();

  if(!title){

    showSheet("Помилка", `<div class="hint">Вкажи заголовок оголошення.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const changes = [];

  if(title !== t.title) changes.push(`Назва: "${shorten(t.title)}" → "${shorten(title)}"`);

  if(audience !== (t.audience || "staff")) changes.push(`Аудиторія: ${announcementAudienceLabel(t.audience)} → ${announcementAudienceLabel(audience)}`);

  const nextDesc = (audience === "meeting") ? desc : "";

  if(nextDesc !== (t.description || "")) changes.push(`Опис: "${shorten(t.description || "")}" → "${shorten(nextDesc)}"`);

  const note = changes.length ? `Оголошення: ${changes.join("; ")}` : "Оголошення без змін";



  const audienceChanged = audience !== (t.audience || "staff");

  let annOrderPatch = {};

  if(audienceChanged){

    const ordered = STATE.tasks.filter(x=>isAnnouncement(x) && (x.audience || "staff")===audience && Number.isFinite(x.annOrder));

    if(ordered.length){

      const nextOrder = Math.max(...ordered.map(x=>x.annOrder)) + 1;

      annOrderPatch = {annOrder: nextOrder};

    } else {

      annOrderPatch = {annOrder: null};

    }

  }



  updateTask(taskId, {title, audience, description: nextDesc, complexity: null, ...annOrderPatch}, u.id, note);

  hideSheet();

  render();

  showToast("Оголошення оновлено", "ok");

}



/* ===========================

   CREATE TASK

=========================== */

let createTaskUserOptions = null;



function toggleNoDue(){

  const noDueEl = document.getElementById("noDue");

  const due = document.getElementById("tDue");

  const dueTime = document.getElementById("tDueTime");

  const ctrl = document.getElementById("tCtrl");

  const ctrlAlways = document.getElementById("tCtrlAlways");

  const ctrlBlock = document.getElementById("ctrlBlock");

  if(!noDueEl || !due) return;



  const no = noDueEl.checked;

  due.disabled = no;

  if(dueTime) dueTime.disabled = no;

  if(no){

    due.value = "";

    if(dueTime) dueTime.value = "";

    if(ctrl){

      if(!ctrl.value && !(ctrlAlways && ctrlAlways.checked)){

        ctrl.value = addDays(kyivDateStr(), 1);

      }

      ctrl.disabled = !!(ctrlAlways && ctrlAlways.checked);

    }

    if(ctrlAlways) ctrlAlways.disabled = false;

    if(ctrlBlock) ctrlBlock.classList.remove("disabled");

  } else {

    if(!due.value){

      due.value = addDays(kyivDateStr(), 3);

    }

    if(ctrl){

      ctrl.value = "";

      ctrl.disabled = true;

    }

    if(ctrlAlways){

      ctrlAlways.checked = false;

      ctrlAlways.disabled = true;

    }

    if(ctrlBlock) ctrlBlock.classList.add("disabled");

  }

}

function toggleCtrlAlways(){

  const always = document.getElementById("tCtrlAlways") || document.getElementById("ctrlAlways");

  const ctrl = document.getElementById("tCtrl") || document.getElementById("ctrlDate");

  if(!always || !ctrl) return;



  if(always.checked){

    ctrl.value = "";

    ctrl.disabled = true;

    return;

  }



  ctrl.disabled = false;

  if(!ctrl.value){

    ctrl.value = (ctrl.id === "tCtrl") ? addDays(kyivDateStr(), 1) : kyivDateStr();

  }

}

let _deptAllSync = false;

function refreshRespOptions(){

  const multiToggles = [...document.querySelectorAll('input[name="tDeptMulti"]')];

  const allToggle = document.querySelector('input[name="tDeptAll"]');

  if(allToggle && allToggle.checked){

    if(_deptAllSync){

      // if "All" was just toggled on, select every dept

      multiToggles.forEach(t=>{ t.checked = true; });

    } else if(multiToggles.some(t=>!t.checked)){

      // user turned off a dept while "All" was on

      allToggle.checked = false;

    }

  }

  _deptAllSync = false;



  const respSel = document.getElementById("tResp");

  if(!respSel || typeof createTaskUserOptions !== "function") return;



  if(multiToggles.length){

    const selected = multiToggles.filter(x=>x.checked).map(x=>x.value);

    if(selected.length === 1){

      respSel.disabled = false;

      const opts = createTaskUserOptions(selected[0]);

      respSel.innerHTML = opts.map(x=>`<option value="${x.id}">${htmlesc(x.name)}</option>`).join("");

    } else {

      respSel.disabled = true;

      respSel.innerHTML = `<option value="">Керівник відділу</option>`;

    }

    return;

  }



  const multiSel = document.getElementById("tDeptMulti");

  if(multiSel){

    const selected = [...multiSel.selectedOptions].map(o=>o.value);

    if(selected.length === 1){

      respSel.disabled = false;

      const opts = createTaskUserOptions(selected[0]);

      respSel.innerHTML = opts.map(x=>`<option value="${x.id}">${htmlesc(x.name)}</option>`).join("");

    } else {

      respSel.disabled = true;

      respSel.innerHTML = `<option value="">Керівник відділу</option>`;

    }

    return;

  }



  const deptSel = document.getElementById("tDept");

  if(!deptSel) return;

  const opts = createTaskUserOptions(deptSel.value);

  respSel.disabled = false;

  respSel.innerHTML = opts.map(x=>`<option value="${x.id}">${htmlesc(x.name)}</option>`).join("");

}



function toggleDeptAll(){

  const allToggle = document.querySelector('input[name="tDeptAll"]');

  const multiToggles = [...document.querySelectorAll('input[name="tDeptMulti"]')];

  if(!allToggle || !multiToggles.length) return;

  if(allToggle.checked){

    _deptAllSync = true;

    multiToggles.forEach(t=>{ t.checked = true; });

  }

  refreshRespOptions();

}



function openCreateTask(kind, preselectDeptId=null, preselectDueDate=null){

  const u = currentSessionUser();

  const {isDeptHeadLike} = asDeptRole(u);



  if(kind==="managerial" && u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Управлінські задачі створює тільки керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(kind==="personal" && u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Особисті задачі створює та бачить тільки керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(kind==="internal" && !(u.role==="boss" || isDeptHeadLike)){

    showSheet("Немає прав", `<div class="hint">Внутрішні задачі може створювати начальник відділу (або в.о.).</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const today = kyivDateStr();

  const isPersonal = (kind==="personal");

  const isManagerial = (kind==="managerial");



  const deptOptions = isPersonal

    ? []

    : (u.role==="boss" ? STATE.departments : STATE.departments.filter(d=>d.id===u.departmentId));



  const userOptions = (deptId)=>{

    if(isPersonal) return [STATE.users.find(x=>x.id==="u_boss")].filter(Boolean);

    const list = STATE.users.filter(x=>x.active && x.departmentId===deptId && (x.role==="executor" || x.role==="dept_head"));

    return list;

  };

  createTaskUserOptions = userOptions;



  const metaBlock = `

    <div class="task-meta-right">

      <div class="row2">

        <div class="field">

          <label>Складність</label>

          <select id="tCx">

            <option value="легка">Легка</option>

            <option value="середня" selected>Середня</option>

            <option value="складна">Складна</option>

          </select>

        </div>



        <div class="field">

          <label>Дедлайн</label>

          <div class="row" style="display:flex;gap:8px;">

            <input id="tDue" type="date" value="${addDays(today, 3)}" />

            <input id="tDueTime" type="time" value="" />

          </div>

        </div>

      </div>



      <div class="row3">

        <div class="field">

          <div class="toggle-row">

            <span class="toggle-label">Без дедлайну</span>

            <label class="switch">

              <input id="noDue" type="checkbox" data-change="toggleNoDue" />

              <span class="slider"></span>

            </label>

          </div>

        </div>



        <div id="ctrlBlock" class="ctrl-inline">

          <div class="field">

            <input id="tCtrl" type="date" value="${addDays(today, 1)}" />

          </div>

          <div class="field">

            <div class="toggle-row">

              <span class="toggle-label">Постійний контроль</span>

              <label class="switch">

                <input id="tCtrlAlways" type="checkbox" data-change="toggleCtrlAlways" />

                <span class="slider"></span>

              </label>

            </div>

          </div>

        </div>

      </div>

    </div>

  `;



  const deptBlock = !isPersonal ? (

    isManagerial ? `

      <div class="task-meta-grid">

        <div class="task-meta-left">

          <div class="field">

          <label>Відділи</label>

          <div class="dept-toggle-grid">

            ${deptOptions.map(d=>`

              <label class="dept-toggle">

                <span class="dept-name">${htmlesc(d.name)}</span>

                <span class="switch">

                    <input type="checkbox" name="tDeptMulti" value="${d.id}" data-change="refreshRespOptions" />

                    <span class="slider"></span>

                  </span>

                </label>

              `).join("")}

            <label class="dept-toggle dept-toggle-all">

              <span class="dept-name">Всі</span>

              <span class="switch">

                <input type="checkbox" name="tDeptAll" data-change="toggleDeptAll" />

                <span class="slider"></span>

              </span>

            </label>

          </div>

          </div>

        </div>

        ${metaBlock}

      </div>

    ` : `

      <div class="task-meta-grid">

        <div class="task-meta-left">

          <div class="row2">

            <div class="field">

              <label>Відділ</label>

              <select id="tDept" data-change="refreshRespOptions">

                ${deptOptions.map(d=>`<option value="${d.id}">${htmlesc(d.name)}</option>`).join("")}

              </select>

            </div>



            <div class="field">

              <label>Відповідальний</label>

              <select id="tResp"></select>

            </div>

          </div>

        </div>

        ${metaBlock}

      </div>

    `

  ) : metaBlock;



  const recurringBlock = (u.role==="boss" && !u.readOnly) ? (()=>{

    const days = [

      {v:1, label:"Пн"},

      {v:2, label:"Вт"},

      {v:3, label:"Ср"},

      {v:4, label:"Чт"},

      {v:5, label:"Пт"},

      {v:6, label:"Сб"},

      {v:0, label:"Нд"},

    ];

    return `

      <details class="recurring-block">

        <summary>Повторення</summary>

        <div class="field">

          <div class="toggle-row">

            <span class="toggle-label">Повторювана задача</span>

            <label class="switch">

              <input id="recEnabled" type="checkbox" data-change="toggleRecurrenceEnabled" />

              <span class="slider"></span>

            </label>

          </div>

        </div>

        <div id="recBody" class="rec-body disabled">

          <div class="rec-type-row">

            <label class="rec-type-pill">

              <input type="radio" name="recType" value="weekly" checked data-change="toggleRecurrenceType" />

              <span>Щотижня</span>

            </label>

            <label class="rec-type-pill">

              <input type="radio" name="recType" value="monthly" data-change="toggleRecurrenceType" />

              <span>Щомісяця</span>

            </label>

          </div>

          <div id="recWeekly" class="rec-toggle-grid">

            ${days.map(d=>`

              <label class="rec-toggle">

                <input type="checkbox" name="recDay" value="${d.v}" />

                <span class="rec-label">${d.label}</span>

              </label>

            `).join("")}

          </div>

          <div id="recMonthly" class="field" style="display:none;">

            <label>Дати місяця (через кому)</label>

            <input id="recDates" placeholder="5, 15" />

          </div>

        </div>

      </details>

    `;

  })() : "";



  showSheet(

    kind==="managerial" ? "Нова управлінська задача" :

    kind==="internal" ? "Нова внутрішня задача" :

    "Нова моя задача",

    `

    <div class="field">

      <label>Назва</label>

      <input id="tTitle" placeholder="Коротко: що зробити" />

    </div>



    <div class="field">

      <div class="label-row">

        <label>Опис (опційно)</label>

        ${formatToolbar("tDesc", "inline")}

      </div>

      <textarea id="tDesc" class="task-desc-input" placeholder="Деталі / очікуваний результат"></textarea>

    </div>



    ${recurringBlock}



    ${deptBlock}



    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="createTaskNow" data-arg1="${kind}">Створити</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);



  if(preselectDueDate){

    const dueInput = document.getElementById("tDue");

    if(dueInput) dueInput.value = preselectDueDate;

  }

  toggleNoDue();

  toggleRecurrenceEnabled();

  toggleRecurrenceType();

  if(!isPersonal){

    if(isManagerial){

      const multiToggles = [...document.querySelectorAll('input[name="tDeptMulti"]')];

      if(multiToggles.length){

        if(preselectDeptId){

          multiToggles.forEach(t=>{ t.checked = (t.value === preselectDeptId); });

        }

        if(!multiToggles.some(x=>x.checked)){

          multiToggles[0].checked = true;

        }

      }

    } else {

      const deptSel = document.getElementById("tDept");

      if(deptSel && preselectDeptId){

        deptSel.value = preselectDeptId;

      }

    }

    refreshRespOptions();

  }

}

function toggleRecurrenceEnabled(){

  const enabled = document.getElementById("recEnabled")?.checked;

  const block = document.getElementById("recBody");

  if(block) block.classList.toggle("disabled", !enabled);

}

function toggleRecurrenceType(){

  const type = document.querySelector('input[name="recType"]:checked')?.value || "weekly";

  const weekly = document.getElementById("recWeekly");

  const monthly = document.getElementById("recMonthly");

  if(weekly) weekly.style.display = (type === "weekly") ? "block" : "none";

  if(monthly) monthly.style.display = (type === "monthly") ? "block" : "none";

  document.querySelectorAll(".rec-type-pill").forEach(el=>{

    const val = el.querySelector('input')?.value;

    el.classList.toggle("active", val === type);

  });

}



function createTaskNow(kind){

  const u = currentSessionUser();

  const title = document.getElementById("tTitle").value.trim();

  if(!title){

    showSheet("Помилка", `<div class="hint">Вкажи назву задачі.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const desc = document.getElementById("tDesc").value || "";

  const cx = document.getElementById("tCx").value;

  const noDue = document.getElementById("noDue").checked;

  const ctrlAlways = noDue ? !!document.getElementById("tCtrlAlways")?.checked : false;

  const dueDateVal = document.getElementById("tDue").value || null;

  const dueTimeVal = document.getElementById("tDueTime")?.value || "";

  const due = noDue ? null : joinDateTime(dueDateVal, dueTimeVal);

  const ctrl = (noDue && !ctrlAlways) ? (document.getElementById("tCtrl").value || null) : null;

  const recEnabled = !!document.getElementById("recEnabled")?.checked;



  if(!recEnabled && !noDue && !dueDateVal){

    showSheet("Помилка", `<div class="hint">Вкажи дедлайн або вибери “Без дедлайну”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const today = kyivDateStr();

  const status = "в_процесі";



  const type = kind;

  const idPrefix = (kind==="managerial") ? "T" : (kind==="internal" ? "I" : "P");

  const id = genTaskCode(idPrefix);



  let departmentId = null;

  let responsibleUserId = "u_boss";

  const pickResponsibleForDept = (deptId)=>{

    const headId = effectiveDeptHeadUserId(deptId);

    if(headId) return headId;

    const opts = getDeptResponsibleOptions(deptId);

    return opts[0]?.id || "u_boss";

  };

  let schedule = null;

  if(recEnabled){

    const recType = document.querySelector('input[name="recType"]:checked')?.value || "weekly";

    if(recType === "weekly"){

      const days = [...document.querySelectorAll('input[name="recDay"]:checked')]

        .map(x=>Number(x.value))

        .filter(n=>Number.isFinite(n));

      if(!days.length){

        showSheet("Помилка", `<div class="hint">Обери дні тижня.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

        return;

      }

      schedule = {type:"weekly", days: [...new Set(days)]};

    } else {

      const raw = (document.getElementById("recDates")?.value || "");

      const dates = raw.split(/[\s,;]+/).map(x=>Number(x)).filter(n=>n>=1 && n<=31);

      const unique = [...new Set(dates)];

      if(!unique.length){

        showSheet("Помилка", `<div class="hint">Вкажи дати місяця (наприклад: 5, 15).</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

        return;

      }

      schedule = {type:"monthly", dates: unique};

    }

  }



  if(kind==="personal"){

    departmentId = null;

    responsibleUserId = "u_boss";

  } else if(kind==="managerial"){

    const multiToggles = [...document.querySelectorAll('input[name="tDeptMulti"]')];

    const selected = multiToggles.filter(x=>x.checked).map(x=>x.value);

    if(!selected.length){

      showSheet("Помилка", `<div class="hint">Обери хоча б один відділ.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

      return;

    }

    if(selected.length === 1){

      departmentId = selected[0];

      responsibleUserId = pickResponsibleForDept(selected[0]);

    } else {

      if(recEnabled){

        if(!STATE.recurringTemplates) STATE.recurringTemplates = [];

        selected.forEach((deptIdSel)=>{

          const tpl = {

            id: uid("rt"),

            type,

            title,

            description: desc,

            departmentId: deptIdSel,

            responsibleUserId: pickResponsibleForDept(deptIdSel),

            complexity: cx,

            noDue,

            controlAlways: noDue ? !!ctrlAlways : false,

            nextControlDate: (noDue && !ctrlAlways) ? ctrl : null,

            schedule,

            createdBy: u.id,

            createdAt: nowIsoKyiv(),

            lastGenerated: null,

          };

          STATE.recurringTemplates.push(tpl);

          if(recurringMatchesToday(tpl, today)){

            tpl.lastGenerated = today;

            createTask({

              id: genTaskCode(idPrefix),

              type,

              title,

              description: desc,

              departmentId: deptIdSel,

              responsibleUserId: pickResponsibleForDept(deptIdSel),

              complexity: cx,

              status,

              startDate: today,

              dueDate: noDue ? null : today,

              nextControlDate: (noDue && !ctrlAlways) ? ctrl : null,

              controlAlways: noDue ? !!ctrlAlways : false,

              createdBy: u.id,

              createdAt: nowIsoKyiv(),

              updatedAt: nowIsoKyiv()

            }, u.id);

          }

        });

        saveState(STATE);

        hideSheet();

        UI.tab = ROUTES.TASKS;

        UI.taskFilter = "активні";

        render();

        showToast("Шаблони створено", "ok");

        return;

      }

      selected.forEach((deptIdSel)=>{

        const taskId = genTaskCode(idPrefix);

        createTask({

          id: taskId,

          type,

          title,

          description: desc,

          departmentId: deptIdSel,

          responsibleUserId: pickResponsibleForDept(deptIdSel),

          complexity: cx,

          status,

          startDate: today,

          dueDate: due,

          nextControlDate: ctrl,

          controlAlways: ctrlAlways,

          createdBy: u.id,

          createdAt: nowIsoKyiv(),

          updatedAt: nowIsoKyiv()

        }, u.id);

      });



      hideSheet();

      UI.tab = ROUTES.TASKS;

      UI.taskFilter = "активні";

      render();

      return;

    }

  } else {

    departmentId = document.getElementById("tDept").value;

    responsibleUserId = document.getElementById("tResp").value;

  }



  if(recEnabled){

    if(!STATE.recurringTemplates) STATE.recurringTemplates = [];

    const tpl = {

      id: uid("rt"),

      type,

      title,

      description: desc,

      departmentId,

      responsibleUserId,

      complexity: cx,

      noDue,

      controlAlways: noDue ? !!ctrlAlways : false,

      nextControlDate: (noDue && !ctrlAlways) ? ctrl : null,

      schedule,

      createdBy: u.id,

      createdAt: nowIsoKyiv(),

      lastGenerated: null,

    };

    STATE.recurringTemplates.push(tpl);

    if(recurringMatchesToday(tpl, today)){

      tpl.lastGenerated = today;

      createTask({

        id,

        type,

        title,

        description: desc,

        departmentId,

        responsibleUserId,

        complexity: cx,

        status,

        startDate: today,

        dueDate: noDue ? null : today,

        nextControlDate: (noDue && !ctrlAlways) ? ctrl : null,

        controlAlways: noDue ? !!ctrlAlways : false,

        createdBy: u.id,

        createdAt: nowIsoKyiv(),

        updatedAt: nowIsoKyiv()

      }, u.id);

    } else {

      saveState(STATE);

    }

    hideSheet();

    UI.tab = ROUTES.TASKS;

    UI.taskFilter = "активні";

    render();

    showToast("Шаблон створено", "ok");

    return;

  }



  createTask({

    id,

    type,

    title,

    description: desc,

    departmentId,

    responsibleUserId,

    complexity: cx,

    status,

    startDate: today,

    dueDate: due,

    nextControlDate: ctrl,

    controlAlways: ctrlAlways,

    createdBy: u.id,

    createdAt: nowIsoKyiv(),

    updatedAt: nowIsoKyiv()

  }, u.id);



  hideSheet();

  UI.tab = ROUTES.TASKS;

  UI.taskFilter = "активні";

  render();

}



function openCreateAnnouncement(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Оголошення може створювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  showSheet("Нове оголошення", `

    <div class="field">

      <label>Аудиторія</label>

      <select id="aAudience">

        <option value="staff">Особовий склад</option>

        <option value="meeting">Нарада (керівництво)</option>

      </select>

    </div>

    <div class="field">

      <label>Заголовок</label>

      <input id="aTitle" />

    </div>

    <div class="field">

      <label>Опис (для наради, опційно)</label>

      <textarea id="aDesc" placeholder="Коротко: що потрібно озвучити"></textarea>

    </div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="createAnnouncementNow">Зберегти</button>

      <button class="btn ghost" data-action="hideSheet">Скасувати</button>

    </div>

  `);

}



function createAnnouncementNow(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Оголошення може створювати лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const audience = document.getElementById("aAudience")?.value || "staff";

  const title = (document.getElementById("aTitle")?.value || "").trim();

  const desc = (document.getElementById("aDesc")?.value || "").trim();

  if(!title){

    showSheet("Помилка", `<div class="hint">Вкажи заголовок оголошення.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  const finalDesc = (audience === "meeting") ? desc : "";

  const ordered = STATE.tasks.filter(t=>isAnnouncement(t) && (t.audience || "staff")===audience && Number.isFinite(t.annOrder));

  const annOrder = ordered.length ? (Math.max(...ordered.map(t=>t.annOrder)) + 1) : null;

  const annOrderPatch = Number.isFinite(annOrder) ? {annOrder} : {};



  const today = kyivDateStr();

  const id = genTaskCode("A");

  createTask({

    id,

    type: "personal",

    title,

    description: finalDesc,

    departmentId: null,

    responsibleUserId: u.id,

    complexity: null,

    status: "в_процесі",

    startDate: today,

    dueDate: null,

    nextControlDate: null,

    controlAlways: false,

    createdBy: u.id,

    createdAt: nowIsoKyiv(),

    updatedAt: nowIsoKyiv(),

    category: "announcement",

    audience,

    ...annOrderPatch

  }, u.id);



  hideSheet();

  UI.tab = ROUTES.TASKS;

  if(u.role==="boss") UI.taskDeptFilter = "personal";

  UI.taskPersonalFilter = "announcements";

  UI.taskFilter = "активні";

  render();

}



/* ===========================

   MISSING LIST

=========================== */

function groupBy(arr, fn){

  return arr.reduce((acc,x)=>{

    const k = fn(x) ?? "unknown";

    acc[k] = acc[k] || [];

    acc[k].push(x);

    return acc;

  },{});

}

function openMissing(){

  const u = currentSessionUser();

  const today = kyivDateStr();

  const weekend = isWeekend(kyivNow());



  const executors = (u.role==="boss")

    ? STATE.users.filter(x=>x.active && x.role==="executor")

    : STATE.users.filter(x=>x.active && x.role==="executor" && x.departmentId===u.departmentId);



  const reportsToday = STATE.dailyReports.filter(r=>r.reportDate===today);

  const missing = weekend ? [] : executors.filter(x=>!reportsToday.some(r=>r.userId===x.id));



  const grouped = groupBy(missing, x=>x.departmentId);

  const html = `

    <div class="hint">${weekend ? "Сьогодні вихідний — контроль звітів не обов’язковий." : "Список виконавців без звіту."}</div>

    <div class="sep"></div>

    ${

      Object.keys(grouped).length

      ? Object.entries(grouped).map(([deptId, users])=>{

          const dept = getDeptById(deptId);

          return `

            <div class="item" style="cursor:default;">

              <div class="row"><div class="name">${htmlesc(dept?.name ?? "")}</div><span class="badge b-danger mono">${users.length}</span></div>

              <div class="hint" style="margin-top:10px;">${users.map(x=>`• ${htmlesc(x.name)}`).join("<br/>")}</div>

            </div>

          `;

        }).join("")

      : `<div class="hint">Немає “не здали”.</div>`

    }

    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `;

  showSheet("Не здали", html);

}



/* ===========================

   DELEGATIONS (boss only)

=========================== */

function createDelegation({departmentId, actingHeadUserId, startDate, endDate, untilCancel}){

  const primary = STATE.users.find(u=>u.role==="dept_head" && u.departmentId===departmentId);

  if(!primary) throw new Error("Немає начальника відділу в довіднику.");



  STATE.delegations = STATE.delegations.map(d=>{

    if(d.departmentId!==departmentId) return d;

    if(d.status==="скасовано" || d.status==="завершено") return d;

    return {...d, status:"завершено", endedAt: nowIsoKyiv()};

  });



  STATE.delegations.push({

    id: uid("del"),

    departmentId,

    primaryHeadUserId: primary.id,

    actingHeadUserId,

    startDate,

    endDate: untilCancel ? null : endDate,

    untilCancel: !!untilCancel,

    status: "заплановано",

    createdAt: nowIsoKyiv(),

    createdBy: "u_boss",

  });



  recomputeDelegationStatuses();

  saveState(STATE);

}

function cancelDelegation(delegationId){

  const idx = STATE.delegations.findIndex(d=>d.id===delegationId);

  if(idx<0) return;

  STATE.delegations[idx] = {...STATE.delegations[idx], status:"скасовано", cancelledAt: nowIsoKyiv()};

  saveState(STATE);

}



function openDelegations(){

  const u = currentSessionUser();

  if(u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Заміщення призначає тільки керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  recomputeDelegationStatuses();



  const active = STATE.delegations.filter(d=>d.status==="активне");

  const scheduled = STATE.delegations.filter(d=>d.status==="заплановано");



  const renderDel = (d)=>{

    const dept = getDeptById(d.departmentId);

    const prim = getUserById(d.primaryHeadUserId);

    const act = getUserById(d.actingHeadUserId);

    const period = d.untilCancel ? `з ${fmtDate(d.startDate)} • до скасування` : `з ${fmtDate(d.startDate)} по ${fmtDate(d.endDate)}`;

    const stCls = d.status==="активне" ? "b-blue" : "b-warn";

    const stLbl = d.status==="активне" ? "АКТИВНЕ" : "ЗАПЛАНОВАНО";



    return `

      <div class="item" style="cursor:default;">

        <div class="row">

          <div>

            <div class="name">${deptBadgeHtml(dept)}</div>

            <div class="sub">

              <span class="badge ${stCls}">${stLbl}</span>

              <span class="pill">Нач.: ${htmlesc(prim?.name ?? "")}</span>

              <span class="pill">В.о.: ${htmlesc(act?.name ?? "")}</span>

            </div>

            <div class="hint" style="margin-top:10px;">Період: <span class="mono">${htmlesc(period)}</span></div>

          </div>

        </div>

        <div class="actions">

          <button class="btn danger" data-action="cancelDelegationUi" data-arg1="${d.id}">Скасувати</button>

        </div>

      </div>

    `;

  };



  showSheet("Заміщення (в.о.)", `

    <div class="hint">Тут ти призначаєш в.о. начальника відділу. Відділ має лише одного активного керівника на дату.</div>

    <div class="sep"></div>



    <div class="item" style="cursor:default;">

      <div class="row"><div class="name">Активні</div><span class="badge b-blue mono">${active.length}</span></div>

    </div>

    ${active.map(renderDel).join("") || `<div class="hint">Немає активних.</div>`}



    <div class="sep"></div>



    <div class="item" style="cursor:default;">

      <div class="row"><div class="name">Заплановані</div><span class="badge b-warn mono">${scheduled.length}</span></div>

    </div>

    ${scheduled.map(renderDel).join("") || `<div class="hint">Немає запланованих.</div>`}



    <div class="sep"></div>

    <button class="btn primary" data-action="openDelegationCreate">➕ Додати заміщення</button>

    <button class="btn ghost" data-action="hideSheet">Закрити</button>

  `);

}



function cancelDelegationUi(id){

  const d = STATE.delegations.find(x=>x.id===id);

  if(!d){

    cancelDelegation(id);

    hideSheet();

    openDelegations();

    return;

  }

  const dept = getDeptById(d.departmentId)?.name || "Відділ";

  const acting = getUserById(d.actingUserId)?.name || "—";

  showSheet("Скасувати заміщення", `

    <div class="hint">Скасувати заміщення у <b>${htmlesc(dept)}</b> (в.о.: <b>${htmlesc(acting)}</b>)?</div>

    <div class="actions" style="margin-top:14px;">

      <button class="btn danger" data-action="confirmCancelDelegation" data-arg1="${d.id}">Скасувати</button>

      <button class="btn ghost" data-action="hideSheet">Назад</button>

    </div>

  `);

}



function openControlByDept(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Цей екран доступний тільки керівнику.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }



  const tasks = getVisibleTasksForUser(u).filter(t=>!!t.departmentId);

  const notBlocked = (t)=>!["блокер","очікування"].includes(t.status);

  const tasksDate = tasks.filter(t=>t.nextControlDate && !t.controlAlways && notBlocked(t));

  const tasksAlways = tasks.filter(t=>t.controlAlways && !t.nextControlDate && notBlocked(t));

  const tasksDeadline = tasks.filter(t=>t.dueDate && t.status!=="закрито" && t.status!=="скасовано");



  const byDept = STATE.departments.map(d=>{

    const deadline = tasksDeadline

      .filter(t=>t.departmentId===d.id)

      .sort((a,b)=>dueSortKey(a.dueDate).localeCompare(dueSortKey(b.dueDate)));

    const ctrlDate = tasksDate

      .filter(t=>t.departmentId===d.id)

      .sort((a,b)=>(a.nextControlDate || "9999-99-99").localeCompare(b.nextControlDate || "9999-99-99"));

    const ctrlAlways = tasksAlways.filter(t=>t.departmentId===d.id);

    const total = deadline.length + ctrlDate.length + ctrlAlways.length;

    return {dept:d, deadline, ctrlDate, ctrlAlways, total};

  }).filter(x=>x.total>0);



  const renderRows = (list, suffixFn)=> list.map(t=>{

    const suffix = suffixFn ? suffixFn(t) : "";

    const rowCls = isOverdue(t) ? "control-task overdue-line" : "control-task";

    return `<span class="${rowCls}">• <b>${htmlesc(t.title)}</b>${htmlesc(suffix)}</span>`;

  }).join("<br/>");



  const renderSection = (title, icon, list, suffixFn)=>`

    <div class="control-dept-section">

      <div class="control-dept-section-h">${icon} ${title} <span class="mono">${list.length}</span></div>

      <div class="control-dept-section-b">

        ${list.length ? renderRows(list, suffixFn) : `<span class="hint">Немає</span>`}

      </div>

    </div>

  `;



  const html = `

    <div class="control-dept-grid">

      ${byDept.map(x=>`

        <div class="control-dept-card">

          <div class="control-dept-head">

            <div class="control-dept-name">${deptBadgeHtml(x.dept)} <span class="mono">${x.total}</span></div>

            <div class="control-dept-counts">

              <span class="pill mono">⏱ ${x.deadline.length}</span>

              <span class="pill mono">🗓 ${x.ctrlDate.length}</span>

              <span class="pill mono">🎯 ${x.ctrlAlways.length}</span>

            </div>

          </div>

          <div class="control-dept-body">

            ${renderSection("Дедлайн", "⏱", x.deadline, (t)=> t.dueDate ? ` — ${dueTitle(t.dueDate)}` : "")}

            ${renderSection("Контроль з датою", "🗓", x.ctrlDate, (t)=> t.nextControlDate ? ` — ${fmtDate(t.nextControlDate)}` : "")}

            ${renderSection("Постійний контроль", "🎯", x.ctrlAlways)}

          </div>

        </div>

      `).join("")}

      ${byDept.length ? "" : `<div class="hint">Немає задач.</div>`}

    </div>

  `;



  showSheet("Контроль по відділах", `

    ${html}

    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Закрити</button>

  `);

}

function confirmCancelDelegation(id){

  cancelDelegation(id);

  hideSheet();

  openDelegations();

}



function refreshDelPeople(){

  const dept = document.getElementById("dDept");

  const actingSelect = document.getElementById("dAct");

  if(!dept || !actingSelect) return;



  const deptId = dept.value;

  const primary = STATE.users.find(u=>u.role==="dept_head" && u.departmentId===deptId);

  const candidates = STATE.users.filter(u=>u.active && u.departmentId===deptId && u.id !== primary?.id);

  actingSelect.innerHTML = candidates.map(c=>`<option value="${c.id}">${htmlesc(c.name)} (${c.role})</option>`).join("");

}



function openDelegationCreate(){

  const deptOptions = STATE.departments;

  const today = kyivDateStr();



  showSheet("Нове заміщення", `

    <div class="hint">Призначення <b>в.о.</b> (лише керівник).</div>



    <div class="field">

      <label>Відділ</label>

      <select id="dDept" data-change="refreshDelPeople">

        ${deptOptions.map(d=>`<option value="${d.id}">${htmlesc(d.name)}</option>`).join("")}

      </select>

    </div>



    <div class="field">

      <label>В.о. начальника</label>

      <select id="dAct"></select>

    </div>



    <div class="row2">

      <div class="field">

        <label>Початок</label>

        <input id="dStart" type="date" value="${today}" />

      </div>

      <div class="field">

        <label>Кінець</label>

        <input id="dEnd" type="date" value="${addDays(today, 7)}" />

      </div>

    </div>



    <div class="field">

      <label><input id="dUntil" type="checkbox" /> До скасування</label>

    </div>



    <div class="actions" style="margin-top:14px;">

      <button class="btn primary" data-action="createDelegationNow">Зберегти</button>

      <button class="btn ghost" data-action="openDelegations">Назад</button>

    </div>

  `);

  refreshDelPeople();

}



function createDelegationNow(){

  try{

    const deptId = document.getElementById("dDept").value;

    const act = document.getElementById("dAct").value;

    const start = document.getElementById("dStart").value;

    const end = document.getElementById("dEnd").value;

    const until = document.getElementById("dUntil").checked;

    if(!deptId || !act || !start || (!until && !end)){

      showSheet("Помилка", `<div class="hint">Заповни обов’язкові поля.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

      return;

    }

    createDelegation({departmentId:deptId, actingHeadUserId:act, startDate:start, endDate:end, untilCancel:until});

    hideSheet();

    openDelegations();

    render();

  } catch(err){

    showSheet("Помилка", `<div class="hint">${htmlesc(err.message || "Не вдалося створити заміщення.")}</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

  }

}



/* ===========================

   PROFILE VIEW

=========================== */

function viewProfile(){

  if(!ensureLoggedIn()) return viewLogin();

  recomputeDelegationStatuses();



  const u = currentSessionUser();

  UI.route = ROUTES.PROFILE;



  const dept = u.departmentId ? getDeptById(u.departmentId) : null;

  const {isDeptHeadLike} = asDeptRole(u);

  const roleText = roleLabel(u);

  const delBanner = actingBannerForUser(u);



  const body = `

    <div class="card">

      <div class="card-h">

        <div class="t">Профіль</div>

        <span class="badge b-blue">${htmlesc(roleText)}</span>

      </div>

      <div class="card-b">

        <div class="item" style="cursor:default;">

          <div class="row">

            <div>

              <div class="name">${htmlesc(u.name)}</div>

              <div class="sub">

                <span class="pill mono">${htmlesc(u.login)}</span>

                ${dept ? `<span class="pill">${htmlesc(dept.name)}</span>` : `<span class="pill">Всі відділи</span>`}

              </div>

            </div>

          </div>

          ${delBanner ? `<div class="hint" style="margin-top:10px;">${htmlesc(delBanner)}</div>` : ``}

        </div>



        <div class="actions">

          ${u.role==="boss" ? `<button class="btn primary" data-action="openDelegations">🧩 Заміщення (в.о.)</button>` : ``}

          ${u.role==="boss" ? `<button class="btn ghost" data-action="openDbTasksPreview">🗄 D1 задачі</button>` : ``}

          ${u.role==="boss" ? `<button class="btn ghost" data-action="exportBackupNow">💾 Експорт backup</button>` : ``}

          <button class="btn ghost" data-action="openAbout">ℹ️ Про прототип</button>

          <button class="btn danger" data-action="logout">🚪 Вийти</button>

        </div>



        <div class="hint">

          Для імітації начальника: вийди → зайди як <span class="mono">head2</span> або <span class="mono">head5</span>.

        </div>

      </div>

    </div>

  `;

  const tabs = (u.role==="boss")

    ? [

      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

      {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

      {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

      {key:ROUTES.PLAN, label:"План", ico:"📅"},

    ]

    : [

      {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

    ];



  const subtitle = roleSubtitle(u);

  appShell({title:"Профіль", subtitle, bodyHtml: body, showFab:false, fabAction:null, tabs});

}



async function openDbTasksPreview(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  try{

    const res = await fetch("/db/tasks", { credentials: "include" });

    if(!res.ok){

      throw new Error(`HTTP ${res.status}`);

    }

    const data = await res.json();

    const items = Array.isArray(data?.items) ? data.items : [];

    const top = items.slice(0, 12);

    const rows = top.length ? top.map((t, idx)=>`

      <tr>

        <td class="mono">${idx + 1}</td>

        <td class="mono">${htmlesc(t.id || "")}</td>

        <td>${htmlesc(t.title || "")}</td>

        <td>${htmlesc(t.status || "")}</td>

        <td>${htmlesc(t.department_id || "")}</td>

      </tr>

    `).join("") : `<tr><td colspan="5" class="hint">У D1 задач поки немає.</td></tr>`;

    showSheet("D1 задачі", `

      <div class="hint">У таблиці <span class="mono">tasks</span>: <b>${items.length}</b> записів. У локальному стані: <b>${STATE.tasks.length}</b>.</div>

      <div class="sep"></div>

      <div style="overflow:auto; max-height:55vh;">

        <table class="table">

          <thead>

            <tr>

              <th>#</th>

              <th>ID</th>

              <th>Назва</th>

              <th>Статус</th>

              <th>Відділ</th>

            </tr>

          </thead>

          <tbody>${rows}</tbody>

        </table>

      </div>

      <div class="sep"></div>

      <button class="btn primary" data-action="hideSheet">Закрити</button>

    `);

  } catch(err){

    showSheet("Помилка D1", `

      <div class="hint">Не вдалося прочитати <span class="mono">/db/tasks</span>.<br/>${htmlesc(err?.message || "Невідома помилка")}</div>

      <div class="sep"></div>

      <button class="btn primary" data-action="hideSheet">Закрити</button>

    `);

  }

}

function exportBackupNow(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  try{

    const payload = stateForSync(STATE);

    const stamp = nowIsoKyiv().replaceAll(":","-").replace(" ", "_");

    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json;charset=utf-8" });

    const link = document.createElement("a");

    link.href = URL.createObjectURL(blob);

    link.download = `bps05-backup-${stamp}.json`;

    document.body.appendChild(link);

    link.click();

    setTimeout(()=>{

      URL.revokeObjectURL(link.href);

      link.remove();

    }, 0);

    showToast("Backup експортовано", "ok");

  } catch(err){

    showSheet("Помилка backup", `

      <div class="hint">Не вдалося створити backup.<br/>${htmlesc(err?.message || "Невідома помилка")}</div>

      <div class="sep"></div>

      <button class="btn primary" data-action="hideSheet">Закрити</button>

    `);

  }

}



function openAbout(){

  showSheet("Про прототип", `

    <div class="hint">

      Це <b>mobile-first</b> прототип без сервера.<br/>

      Дані зберігаються в <b>localStorage</b> (тільки для тестування).<br/><br/>

      Реалізовано:

      <ul>

        <li>Логін/пароль</li>

        <li>Ролі: керівник / начальник відділу / виконавець</li>

        <li>Заміщення (в.о.) — призначає лише керівник</li>

        <li>Задачі: управлінські + внутрішні + <b>мої (керівника)</b></li>

        <li>Задачі <b>без дедлайну</b> + контрольна дата</li>

        <li>Щоденні звіти (ПІЗНО після 17:30)</li>

        <li>Підсумок відділу (3–5 речень)</li>

        <li>Люди/штат у начальника: хто здав/не здав</li>

        <li>Керівник у “Звітах”: “Хто не здав” по відділах + “👥 Люди”</li>

      </ul>

    </div>

    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">OK</button>

  `);

}



/* ===========================

   ANALYTICS VIEW (demo)

=========================== */

function viewAnalytics(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){ UI.tab = ROUTES.CONTROL; return viewControl(); }

  UI.tab = ROUTES.ANALYTICS;

  const tasksAll = u.readOnly ? getVisibleTasksForUser(u) : STATE.tasks.slice();



  const today = kyivDateStr();

  const days = Array.from({length:7}, (_,i)=>addDays(today, -(6-i)));

  const closeDateForTask = (task)=>{

    const updates = STATE.taskUpdates

      .filter(u=>u.taskId===task.id && u.status==="закрито")

      .sort((a,b)=>b.at.localeCompare(a.at));

    if(updates[0]) return toDateOnly(updates[0].at);

    if(task.status==="закрито") return toDateOnly(task.updatedAt);

    return null;

  };

  const weekClosed = days.map(d=>{

    const count = tasksAll.filter(t=>closeDateForTask(t)===d).length;

    return {date:d, count};

  });

  const maxClosed = Math.max(1, ...weekClosed.map(x=>x.count));



  const closedDurations = tasksAll

    .map(t=>{

      const closeDate = closeDateForTask(t);

      const startDate = toDateOnly(t.createdAt) || t.startDate;

      if(!closeDate || !startDate) return null;

      const daysToClose = dateDiffDays(startDate, closeDate);

      if(daysToClose < 0) return null;

      return {task:t, daysToClose};

    })

    .filter(Boolean);

  const avgClose = closedDurations.length

    ? (closedDurations.reduce((s,x)=>s+x.daysToClose, 0) / closedDurations.length).toFixed(1)

    : "—";



  const topProblems = tasksAll

    .filter(t=>t.status!=="закрито" && t.status!=="скасовано")

    .map(t=>{

      const blockerUpdates = STATE.taskUpdates.filter(u=>

        u.taskId===t.id

        && (u.status==="блокер" || u.status==="очікування")

        && isBlockerReasonNote(u.note)

      );

      return {task:t, count:blockerUpdates.length, last:blockerUpdates.sort((a,b)=>b.at.localeCompare(a.at))[0]};

    })

    .filter(x=>x.count>0)

    .sort((a,b)=>b.count-a.count)

    .slice(0,5);



  const deptLoad = STATE.departments.map(d=>{

    const deptTasks = tasksAll.filter(t=>t.departmentId===d.id);

    const active = deptTasks.filter(t=>t.status!=="закрито" && t.status!=="скасовано").length;

    const blockers = deptTasks.filter(t=>t.status==="блокер" || t.status==="очікування").length;

    const overdue = deptTasks.filter(t=>isOverdue(t)).length;

    return {dept:d, active, blockers, overdue};

  });

  const maxActive = Math.max(1, ...deptLoad.map(x=>x.active));



  const activeDeptTasks = tasksAll.filter(t=>t.departmentId && t.status!=="закрито" && t.status!=="скасовано");

  const recentClosed = tasksAll.filter(t=>t.departmentId && t.status==="закрито" && closeDateForTask(t) && closeDateForTask(t) >= days[0] && closeDateForTask(t) <= days[days.length-1]);

  const complexityKeys = COMPLEXITY_KEYS;

  const complexityCounts = complexityKeys.map(k=>({

    key:k,

    label: complexityLabel(k),

    count: activeDeptTasks.filter(t=>taskComplexity(t)===k).length

  }));

  const complexityOther = activeDeptTasks.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

  if(complexityOther>0){

    complexityCounts.push({key:"other", label:"Без складності", count: complexityOther});

  }

  const maxComplexity = Math.max(1, ...complexityCounts.map(x=>x.count));

  const complexityClosed = complexityKeys.map(k=>({

    key:k,

    label: complexityLabel(k),

    count: recentClosed.filter(t=>taskComplexity(t)===k).length

  }));

  const complexityClosedOther = recentClosed.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

  if(complexityClosedOther>0){

    complexityClosed.push({key:"other", label:"Без складності", count: complexityClosedOther});

  }

  const maxComplexityClosed = Math.max(1, ...complexityClosed.map(x=>x.count));



  const complexityBreakdown = (list)=>{

    const rows = complexityKeys.map(k=>({

      key:k,

      label: complexityLabel(k),

      count: list.filter(t=>taskComplexity(t)===k).length

    }));

    const other = list.filter(t=>!complexityKeys.includes(taskComplexity(t))).length;

    if(other>0){

      rows.push({key:"other", label:"Без складності", count: other});

    }

    const max = Math.max(1, ...rows.map(x=>x.count));

    return {rows, max, total: list.length};

  };



  const activeDeadline = activeDeptTasks.filter(t=>!!t.dueDate);

  const activeCtrlDate = activeDeptTasks.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);

  const activeCtrlAlways = activeDeptTasks.filter(t=>!t.dueDate && !!t.controlAlways);

  const closedDeadline = recentClosed.filter(t=>!!t.dueDate);

  const closedCtrlDate = recentClosed.filter(t=>!t.dueDate && !!t.nextControlDate && !t.controlAlways);

  const closedCtrlAlways = recentClosed.filter(t=>!t.dueDate && !!t.controlAlways);



  const cxActiveDeadline = complexityBreakdown(activeDeadline);

  const cxActiveCtrlDate = complexityBreakdown(activeCtrlDate);

  const cxActiveCtrlAlways = complexityBreakdown(activeCtrlAlways);

  const cxClosedDeadline = complexityBreakdown(closedDeadline);

  const cxClosedCtrlDate = complexityBreakdown(closedCtrlDate);

  const cxClosedCtrlAlways = complexityBreakdown(closedCtrlAlways);



  const closedTotal = weekClosed.reduce((s,x)=>s+x.count,0);

  const topProblemsShort = topProblems.length

    ? topProblems.slice(0,3).map(x=>{

        const dept = x.task.departmentId ? getDeptById(x.task.departmentId)?.name : "Особисто";

        const note = x.last?.note ? normalizeBlockerNote(x.last.note) : "";

        return `• ${htmlesc(x.task.title)} (${htmlesc(dept || "")}) • ${x.count}${note ? ` — ${htmlesc(shorten(note, 50))}` : ""}`;

      }).join("<br/>")

    : "Немає активних блокерів.";

  const deptLoadTop = deptLoad.slice().sort((a,b)=>b.active-a.active).slice(0,3)

    .map(x=>`• ${htmlesc(x.dept.name)} — ${x.active} (⛔ ${x.blockers}, 🟠 ${x.overdue})`).join("<br/>");

  const complexitySummary = complexityCounts.map(x=>`${htmlesc(x.label)}: ${x.count}`).join(" • ");

  const blockersTotal = deptLoad.reduce((s,x)=>s+x.blockers,0);

  const overdueTotal = deptLoad.reduce((s,x)=>s+x.overdue,0);



  const weekRangeLabel = `${fmtDate(days[0])} — ${fmtDate(days[days.length-1])}`;

  const weekSpark = weekClosed.map(x=>`

    <div class="spark-item">

      <div class="spark-bar" style="height:${Math.max(6, Math.round((x.count/maxClosed)*100))}%"></div>

      <div class="spark-label mono">${fmtDate(x.date).slice(5)}</div>

    </div>

  `).join("");

  const deptLoadTopRows = deptLoad.slice().sort((a,b)=>b.active-a.active).slice(0,3).map(x=>`

    <div class="mini-row">

      <div class="mini-name">${htmlesc(x.dept.name)}</div>

      <div class="mini-val mono">${x.active}</div>

      <div class="mini-sub">⛔ ${x.blockers} • 🟠 ${x.overdue}</div>

    </div>

  `).join("") || `<div class="mini-empty">Немає даних.</div>`;

  const topProblemsRows = topProblems.length

    ? topProblems.slice(0,3).map(x=>{

        const dept = x.task.departmentId ? getDeptById(x.task.departmentId)?.name : "Особисто";

        const note = x.last?.note ? normalizeBlockerNote(x.last.note) : "";

        return `

          <div class="mini-row">

            <div class="mini-name">${htmlesc(x.task.title)}</div>

            <div class="mini-val mono">${x.count}</div>

            <div class="mini-sub">${htmlesc(dept || "")}${note ? ` • ${htmlesc(shorten(note, 44))}` : ""}</div>

          </div>

        `;

      }).join("")

    : `<div class="mini-empty">Немає активних блокерів.</div>`;

  const complexityTags = complexityCounts.length

    ? complexityCounts.map(x=>`<span class="ana-tag">${htmlesc(x.label)} <b>${x.count}</b></span>`).join("")

    : `<span class="ana-tag">Немає активних</span>`;



  const tiles = `

    <div class="analytics-tiles">

      <div class="analytics-hero">

        <div class="hero-card">

          <div class="hero-eyebrow">Аналітика • 7 днів</div>

          <div class="hero-title">Пульс виконання</div>

          <div class="hero-sub">Період: <span class="mono">${weekRangeLabel}</span></div>

          <div class="hero-metrics">

            <div class="hero-metric">

              <div class="label">Закрито</div>

              <div class="value mono">${closedTotal}</div>

            </div>

            <div class="hero-metric">

              <div class="label">Середній час</div>

              <div class="value mono">${avgClose} дн.</div>

            </div>

            <div class="hero-metric">

              <div class="label">Активні</div>

              <div class="value mono">${activeDeptTasks.length}</div>

            </div>

          </div>

          <div class="hero-spark">${weekSpark}</div>

        </div>

        <div class="hero-stack">

          <div class="signal-card">

            <div class="signal-title">Блокери / прострочені</div>

            <div class="signal-value mono">⛔ ${blockersTotal}</div>

            <div class="signal-sub">🟠 ${overdueTotal} активні</div>

          </div>

          <div class="signal-card">

            <div class="signal-title">Контроль і дедлайни</div>

            <div class="signal-value mono">⏱ ${activeDeadline.length}</div>

            <div class="signal-sub">🗓 ${activeCtrlDate.length} • 🎯 ${activeCtrlAlways.length}</div>

          </div>

        </div>

      </div>



      <div class="analytics-mosaic">

        <div class="metric-card wide">

          <div class="metric-head">

            <div class="metric-title">Топ проблем (коротко)</div>

            <div class="metric-pill">${topProblems.length}</div>

          </div>

          <div class="metric-body">${topProblemsRows}</div>

        </div>



        <div class="metric-card">

          <div class="metric-head">

            <div class="metric-title">Навантаження (топ‑3)</div>

            <div class="metric-pill">${deptLoad.reduce((s,x)=>s+x.active,0)}</div>

          </div>

          <div class="metric-body">${deptLoadTopRows}</div>

        </div>



        <div class="metric-card">

          <div class="metric-head">

            <div class="metric-title">Складність активних</div>

            <div class="metric-pill">${activeDeptTasks.length}</div>

          </div>

          <div class="metric-tags">${complexityTags}</div>

          <div class="metric-foot">${complexitySummary || "Немає активних."}</div>

        </div>



        <div class="metric-card">

          <div class="metric-head">

            <div class="metric-title">Динаміка закриття</div>

            <div class="metric-pill">${weekClosed.reduce((s,x)=>s+x.count,0)}</div>

          </div>

          <div class="metric-body">

            <div class="analytics-bars">

              ${weekClosed.map(x=>`

                <div class="analytics-bar-row">

                  <div class="analytics-label mono">${fmtDate(x.date).slice(0,5)}</div>

                  <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/maxClosed)*100)}%"></div></div>

                  <div class="analytics-value mono">${x.count}</div>

                </div>

              `).join("")}

            </div>

          </div>

        </div>

      </div>

    </div>

  `;



  const body = `

    <div class="card">

      <div class="card-h">

        <div class="t">Аналітика</div>

        <div class="card-actions">

          <span class="badge b-blue">Останні 7 днів</span>

          <button class="btn ghost btn-mini analytics-toggle" data-action="toggleAnalyticsDetails">

            ${UI.analyticsShowDetails ? "Сховати деталі" : "Показати деталі"}

          </button>

        </div>

      </div>

      <div class="card-b">

        ${tiles}

        <div class="analytics-grid">

        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Графік закриття задач</div><span class="badge b-ok mono">${weekClosed.reduce((s,x)=>s+x.count,0)}</span></div>

          <div class="hint">Скільки задач закрито по днях.</div>

          <div class="analytics-bars">

            ${weekClosed.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label mono">${fmtDate(x.date).slice(0,5)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/maxClosed)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Середній час закриття</div><span class="badge b-violet mono">${avgClose}</span></div>

          <div class="hint">Середня тривалість від створення до закриття (у днях).</div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Топ проблем</div><span class="badge b-warn mono">${topProblems.length}</span></div>

          <div class="hint">

            ${

              topProblems.length

                ? topProblems.map(x=>{

                    const dept = x.task.departmentId ? getDeptById(x.task.departmentId)?.name : "Особисто";

                    const note = x.last?.note ? ` — ${htmlesc(normalizeBlockerNote(x.last.note)).slice(0,80)}` : "";

                    return `• <b>${htmlesc(x.task.title)}</b> (${htmlesc(dept || "")}) • ${x.count}${note}`;

                  }).join("<br/>")

                : "Немає активних блокерів."

            }

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Навантаження по відділах</div><span class="badge b-blue mono">${deptLoad.reduce((s,x)=>s+x.active,0)}</span></div>

          <div class="analytics-bars">

            ${deptLoad.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.dept.name)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.active/maxActive)*100)}%"></div></div>

                <div class="analytics-value mono">${x.active}</div>

              </div>

              <div class="hint" style="margin-top:2px;">⛔ ${x.blockers} • 🟠 ${x.overdue}</div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Складність задач</div><span class="badge b-blue mono">${activeDeptTasks.length}</span></div>

          <div class="hint">Активні задачі по складності.</div>

          <div class="analytics-bars">

            ${complexityCounts.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.label)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/maxComplexity)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Активні з дедлайном — складність</div><span class="badge b-blue mono">${cxActiveDeadline.total}</span></div>

          <div class="hint">Активні задачі з дедлайном.</div>

          <div class="analytics-bars">

            ${cxActiveDeadline.rows.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.label)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/cxActiveDeadline.max)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Активні з датою контролю — складність</div><span class="badge b-blue mono">${cxActiveCtrlDate.total}</span></div>

          <div class="hint">Активні задачі з контрольними датами.</div>

          <div class="analytics-bars">

            ${cxActiveCtrlDate.rows.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.label)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/cxActiveCtrlDate.max)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Активні на постійному контролі — складність</div><span class="badge b-blue mono">${cxActiveCtrlAlways.total}</span></div>

          <div class="hint">Активні задачі на постійному контролі.</div>

          <div class="analytics-bars">

            ${cxActiveCtrlAlways.rows.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.label)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/cxActiveCtrlAlways.max)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Складність закритих (7 днів)</div><span class="badge b-ok mono">${recentClosed.length}</span></div>

          <div class="hint">Закриті задачі за останні 7 днів.</div>

          <div class="analytics-bars">

            ${complexityClosed.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.label)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/maxComplexityClosed)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Закриті з дедлайном — складність (7 днів)</div><span class="badge b-ok mono">${cxClosedDeadline.total}</span></div>

          <div class="hint">Закриті задачі з дедлайном за останні 7 днів.</div>

          <div class="analytics-bars">

            ${cxClosedDeadline.rows.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.label)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/cxClosedDeadline.max)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Закриті з датою контролю — складність (7 днів)</div><span class="badge b-ok mono">${cxClosedCtrlDate.total}</span></div>

          <div class="hint">Закриті задачі з контрольними датами за останні 7 днів.</div>

          <div class="analytics-bars">

            ${cxClosedCtrlDate.rows.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.label)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/cxClosedCtrlDate.max)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>



        <div class="item analytics-block" style="cursor:default;">

          <div class="row"><div class="name">Закриті на постійному контролі — складність (7 днів)</div><span class="badge b-ok mono">${cxClosedCtrlAlways.total}</span></div>

          <div class="hint">Закриті задачі на постійному контролі за останні 7 днів.</div>

          <div class="analytics-bars">

            ${cxClosedCtrlAlways.rows.map(x=>`

              <div class="analytics-bar-row">

                <div class="analytics-label">${htmlesc(x.label)}</div>

                <div class="analytics-bar-wrap"><div class="analytics-bar" style="width:${Math.round((x.count/cxClosedCtrlAlways.max)*100)}%"></div></div>

                <div class="analytics-value mono">${x.count}</div>

              </div>

            `).join("")}

          </div>

        </div>

        </div>

      </div>

    </div>

  `;



  const tabs = [

    {key:ROUTES.CONTROL, label:"Контроль", ico:"🧭"},

    {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

    {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

    {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

    {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

    {key:ROUTES.PLAN, label:"План", ico:"📅"},

  ];



  appShell({title:"Аналітика", subtitle:"Керівник", bodyHtml: body, showFab:false, fabAction:null, tabs});

}



/* ===========================

   TASK LIST SHORTCUT

=========================== */

function openTaskList(filterKey){

  UI.tab = ROUTES.TASKS;

  UI.taskFilter = filterKey;

  if(UI.deptOpen){

    UI.deptOpen = {};

    STATE.departments.forEach(d=>{ UI.deptOpen[d.id] = true; });

    UI.deptOpen.personal = true;

  }

  render();

}

function openHelp(){

  showSheet("Довідка користувача", `

    <div class="item" style="cursor:default;">

      <div class="name">Що це за програма</div>

      <div class="hint">Планувальник задач, щоденних звітів і контролю виконання для керівника, начальників відділів і виконавців.</div>

    </div>



    <div class="item" style="cursor:default;">

      <div class="name">Основні екрани і логіка</div>

      <div class="hint">

        🧭 Контроль: що критично сьогодні (не здали звіт, блокери, задачі на підтвердження).<br/>

        📝 Звіти: щоденні звіти виконавців та підсумки відділів.<br/>

        📋 Задачі: постановка, виконання, фільтри по статусах і відділах.<br/>

        📈 Аналітика: динаміка закриття, топ проблем, середній час закриття, навантаження відділів.<br/>

        📑 Звітність: план щомісячних заходів по відділах і контроль виконання.<br/>

        📅 План: календар звітності або дедлайнів задач за місяць.

      </div>

    </div>



    <div class="item" style="cursor:default;">

      <div class="name">Роль: Керівник (boss) — приклад дня</div>

      <div class="hint">

        1) Відкрий “Контроль” і перевір плитку “Очікує підтвердження”.<br/>

        2) Перейди в “Задачі” → фільтр “Очікує підтвердження”.<br/>

        3) Відкрий картку задачі, перевір оновлення, натисни “Підтвердити” або “Повернути”.<br/>

        4) У “Аналітиці” оціни, де ростуть блокери і який відділ перевантажений.

      </div>

    </div>



    <div class="item" style="cursor:default;">

      <div class="name">Роль: Начальник відділу (head) — приклад дня</div>

      <div class="hint">

        1) Отримай управлінську задачу від керівника.<br/>

        2) Розбий роботу на внутрішні задачі для виконавців.<br/>

        3) В кінці дня перевір “Люди/штат” (хто здав/не здав).<br/>

        4) Подай підсумок відділу (3–5 речень) у вкладці “Контроль”.

      </div>

    </div>



    <div class="item" style="cursor:default;">

      <div class="name">Роль: Виконавець — приклад дня</div>

      <div class="hint">

        1) Відкрий свої задачі, онови статус (в процесі/блокер).<br/>

        2) Якщо є проблема — зафіксуй блокер у задачі та у щоденному звіті.<br/>

        3) Подай щоденний звіт до 17:30, щоб не було позначки “ПІЗНО”.

      </div>

    </div>



    <div class="item" style="cursor:default;">

      <div class="name">Аналітика: як читати</div>

      <div class="hint">

        Графік закриття: якщо падає 2–3 дні поспіль — відділ “застряг”.<br/>

        Топ проблем: задачі з найчастішими причинами блокера/очікування.<br/>

        Середній час закриття: орієнтир швидкості виконання.<br/>

        Навантаження відділів: порівняння активних задач + блокерів/прострочених.

      </div>

    </div>



    <div class="item" style="cursor:default;">

      <div class="name">Експорт у Excel</div>

      <div class="hint">

        У “Задачах” натисни ⬇️ Excel, обери період і завантаж файл.<br/>

        Книга містить вкладку “Загальні” і окремі вкладки по відділах.

      </div>

    </div>



    <div class="sep"></div>

    <button class="btn primary" data-action="hideSheet">Зрозуміло</button>

  `);

}



const ACTIONS = {

  applyControlDate,

  cancelDelegationUi,

  confirmCancelDelegation,

  confirmTaskClose,

  createDelegationNow,

  createAnnouncementNow,

  createTaskNow,

  goProfile,

  openSyncLogin,

  hideSheet,

  logout,

  openDbTasksPreview,

  exportBackupNow,

  openAbout,

  openHelp,

  applyTextFormat,

  openCreateTask,

  openCreateAnnouncement,

  openDelegationCreate,

  openDelegations,

  openDeptPeople,

  openDeptPeopleBoss,

  openDeptAnalytics,

  openTasksAnalytics,

  openAllDeptReport,

  openDeptNote,

  openDeptSummary,

  openDeptSummaryForm,

  openControlByDept,

  openMissing,

  openReport,

  openReportForm,

  openReportPlanCreate,

  openReportPlanEdit,

  saveReportPlanNow,

  confirmDeleteReportPlan,

  deleteReportPlanNow,

  openPlanDay,

  openPlanCreateTask,

  openPlanCreateTaskFromPicker,

  openTaskFromPlanDay,

  openReportStatusTasks,

  openQuickActions,

  openMyTasks,

  openAllTasks,

  openAnnouncementsAudience,

  openTasksExportDialog,

  openTask,

  openEditTask,

  openMeetingRepeat,

  openTaskList,

  markMeetingAnnounced,

  toggleMeetingHideToday,

  setMeetingRepeatTomorrow,

  applyMeetingRepeat,

  toggleTaskScope,

  clearTaskSearch,

  confirmDeleteTask,

  deleteTaskNow,

  setControlDate,

  applyReportTemplate,

  autoFillReport,

  saveTaskEdits,

  saveAnnouncementEdits,

  setTaskDeptFilter,

  setTaskPersonalFilter,

  setReportFilter,

  setPlanMode,

  setTab,

  setTaskFilter,

  setTaskStatus,

  submitDeptSummaryNow,

  saveDeptNoteNow,

  submitReportNow,

  submitStatusReason,

  exportTasksExcelNow,

  exportWeeklyTasksExcelNow,

  openWeeklyTaskCreate,

  openWeeklyTaskEdit,

  createWeeklyTaskNow,

  saveWeeklyTaskEdits,

  closeWeeklyTaskNow,

  reopenWeeklyTaskNow,

  applyWeeklyClose,

  confirmDeleteWeeklyTask,

  deleteWeeklyTaskNow,

  toggleTaskDensity,

  toggleTheme,

  toggleAnalyticsDetails,

};

const CHANGE_ACTIONS = {

  refreshDelPeople,

  refreshRespOptions,

  setTaskSearchFromInput,

  setReportsControlDateFromInput,

  setReportingMonthFromInput,

  setPlanMonthFromInput,

  setWeeklyPeriodFromSelect,

  setWeeklyAnchorDateFromInput,

  setWeeklyMonthFromInput,

  setWeeklyWeekIndexFromSelect,

  toggleNoDue,

  toggleRecurrenceEnabled,

  toggleRecurrenceType,

  toggleCtrlAlways,

  toggleDeptAll,

};



const READONLY_BLOCKED_ACTIONS = new Set([

  "applyControlDate",

  "applyMeetingRepeat",

  "applyReportTemplate",

  "autoFillReport",

  "cancelDelegationUi",

  "confirmCancelDelegation",

  "confirmDeleteTask",

  "confirmTaskClose",

  "createAnnouncementNow",

  "createDelegationNow",

  "createTaskNow",

  "deleteTaskNow",

  "markMeetingAnnounced",

  "openCreateAnnouncement",

  "openCreateTask",

  "openPlanCreateTask",

  "openPlanCreateTaskFromPicker",

  "openDelegationCreate",

  "openDeptNote",

  "openDeptSummaryForm",

  "openEditTask",

  "openMeetingRepeat",

  "openReportForm",

  "openReportPlanCreate",

  "openReportPlanEdit",

  "saveReportPlanNow",

  "confirmDeleteReportPlan",

  "deleteReportPlanNow",

  "saveAnnouncementEdits",

  "saveDeptNoteNow",

  "saveTaskEdits",

  "setControlDate",

  "setMeetingRepeatTomorrow",

  "setTaskStatus",

  "submitDeptSummaryNow",

  "submitReportNow",

  "submitStatusReason",

  "toggleMeetingHideToday",

  "openWeeklyTaskCreate",

  "openWeeklyTaskEdit",

  "createWeeklyTaskNow",

  "saveWeeklyTaskEdits",

  "closeWeeklyTaskNow",

  "reopenWeeklyTaskNow",

  "applyWeeklyClose",

  "confirmDeleteWeeklyTask",

  "deleteWeeklyTaskNow",

]);



function runMappedAction(name, arg1, arg2){

  const action = ACTIONS[name];

  if(typeof action !== "function") return;

  if(isReadOnly(currentSessionUser()) && READONLY_BLOCKED_ACTIONS.has(name)){

    showToast("Режим перегляду: зміни заборонені.", "warn");

    return;

  }

  if(typeof arg2 !== "undefined") return action(arg1, arg2);

  if(typeof arg1 !== "undefined") return action(arg1);

  return action();

}

function runMappedChange(name){

  const action = CHANGE_ACTIONS[name];

  if(typeof action === "function") action();

}



/* ===========================

   AUTO SYNC

=========================== */

function stateStamp(st){

  return (st && st.sync && st.sync.updatedAt) ? st.sync.updatedAt : "";

}

function queueSync(){

  if(!SYNC_URL) return;

  if(!_syncReady){

    _syncPending = true;

    return;

  }

  _syncPending = false;

  if(_syncTimer) clearTimeout(_syncTimer);

  _syncTimer = setTimeout(pushSync, SYNC_DEBOUNCE_MS);

}

async function pushSync(){

  if(!SYNC_URL || _syncInFlight || !_syncReady) return;

  _syncInFlight = true;

  try{

    ensureSyncMeta(STATE);

    const payload = { state: stateForSync(STATE) };

    const res = await fetch(SYNC_URL, {

      method: "PUT",

      headers: {"Content-Type":"application/json"},

      credentials: "include",

      body: JSON.stringify(payload),

    });

    if(res.ok){

      _lastPushAt = nowIsoKyiv();

      await ensureDbTasksCache(true);

    }

  } catch{}

  _syncInFlight = false;

}

async function pullSync(){

  if(!SYNC_URL || _syncInFlight) return;

  _syncInFlight = true;

  const wasInitDone = _syncInitDone;

  try{

    const res = await fetch(SYNC_URL, { credentials: "include" });

    if(!res.ok){

      _syncInFlight = false;

      _syncInitDone = true;

      if(!wasInitDone) render();

      return;

    }

    const data = await res.json();

    if(!data || !("state" in data)){

      _syncInFlight = false;

      _syncInitDone = true;

      if(!wasInitDone) render();

      return;

    }

    if(data.state === null){

      const isFirstSync = !_syncReady;

      _syncReady = true;

      _syncInitDone = true;

      if(isFirstSync){

        queueSync();

        if(!wasInitDone) render();

      }

      _lastPullAt = nowIsoKyiv();

      _syncInFlight = false;

      return;

    }

    const localUserId = STATE?.session?.userId || null;

    const remote = migrateState(data.state) || data.state;

    remote.session = {userId: localUserId};

    const isFirstSync = !_syncReady;

    _syncReady = true;

    _syncInitDone = true;

    const localStamp = stateStamp(STATE);

    const remoteStamp = stateStamp(remote);

    if(isFirstSync){

      if(remoteStamp && (!localStamp || remoteStamp > localStamp)){

        STATE = remote;

        saveState(STATE, {skipSyncStamp:true});

        _syncPending = false;

      } else {

        queueSync();

      }

      render();

      await ensureDbTasksCache(true);

      _lastPullAt = nowIsoKyiv();

      _syncInFlight = false;

      return;

    }

    if(remoteStamp && (!localStamp || remoteStamp > localStamp)){

      STATE = remote;

      saveState(STATE, {skipSyncStamp:true});

      _syncPending = false;

      render();

      await ensureDbTasksCache(true);

    }

    _lastPullAt = nowIsoKyiv();

    if(_syncPending) queueSync();

  } catch{}

  _syncInFlight = false;

  if(!_syncInitDone){

    _syncInitDone = true;

    if(!wasInitDone) render();

  }

}

function initAutoSync(){

  if(!SYNC_URL) return;

  pullSync();

  setInterval(pullSync, SYNC_POLL_MS);

  document.addEventListener("visibilitychange", ()=>{

    if(!document.hidden) pullSync();

  });

  window.addEventListener("online", pullSync);

}



function refreshOverdueClasses(){

  const items = document.querySelectorAll(".task-item[data-task-id]");

  if(!items.length) return;

  items.forEach(el=>{

    const id = el.dataset.taskId;

    if(!id) return;

    const t = STATE.tasks.find(x=>x.id===id);

    if(!t) return;

    el.classList.toggle("is-overdue", isOverdue(t));

  });

}

function initOverdueTicker(){

  refreshOverdueClasses();

  if(_overdueTimer) clearInterval(_overdueTimer);

  _overdueTimer = setInterval(refreshOverdueClasses, 30000);

  document.addEventListener("visibilitychange", ()=>{

    if(!document.hidden) refreshOverdueClasses();

  });

}



document.addEventListener("click", (e)=>{

  const deptChip = e.target.closest('[data-action="setTaskDeptFilter"]');

  if(deptChip){

    const deptId = deptChip.dataset.arg1 || "";

    const u = currentSessionUser();

    if(u && u.role==="boss" && UI.taskDeptFilter==="all" && deptId && deptId!=="all" && deptId!=="personal"){

      e.preventDefault();

      e.stopPropagation();

      e.stopImmediatePropagation();

      setTaskDeptFilter(deptId);

      openCreateTask("managerial", deptId);

      return;

    }

  }



  const deptNote = e.target.closest(".dept-note-inline");

  if(deptNote){

    e.preventDefault();

    e.stopPropagation();

    deptNote.classList.toggle("is-expanded");

    return;

  }



  const el = e.target.closest("[data-action]");

  if(!el) return;

  e.preventDefault();



  if(el.dataset.action === "hideThen"){

    hideSheet();

    return runMappedAction(el.dataset.next, el.dataset.arg1, el.dataset.arg2);

  }

  runMappedAction(el.dataset.action, el.dataset.arg1, el.dataset.arg2);

});

document.addEventListener("change", (e)=>{

  const el = e.target.closest("[data-change]");

  if(!el) return;

  runMappedChange(el.dataset.change);

});



/* ===========================

   RENDER

=========================== */

function render(){

  const user = currentSessionUser();

  if(!user){

    UI.route = ROUTES.LOGIN;

    return viewLogin();

  }

  enforceReadOnlyNavigation(user);

  runRecurringTemplates();

  runReportPlans();



  if(UI.route === ROUTES.PROFILE) return viewProfile();



  if(UI.tab === ROUTES.CONTROL) return viewControl();

  if(UI.tab === ROUTES.REPORTS) return viewReports();

  if(UI.tab === ROUTES.TASKS) return viewTasks();

  if(UI.tab === ROUTES.WEEKLY) return viewWeeklyTasks();

  if(UI.tab === ROUTES.ANALYTICS) return viewAnalytics();

  if(UI.tab === ROUTES.REPORTING) return viewReporting();

  if(UI.tab === ROUTES.PLAN) return viewPlan();



  return viewControl();

}



/* ===========================

   START

=========================== */

applyTheme(UI.theme);

render();

initAutoSync();

initOverdueTicker();









































