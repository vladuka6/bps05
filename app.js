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

const EVAL_TOAST_DATE_KEY = "planner_eval_toast_date";

let _syncTimer = null;

let _syncInFlight = false;

let _lastPullAt = null;

let _lastPushAt = null;

let _overdueTimer = null;

let _syncReady = false;

let _syncPending = false;

let _syncInitDone = !SYNC_URL;

let _lastLocalPersistOk = true;

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

    return true;

  } catch{

    memoryStorage[key] = String(value);

    return false;

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

function normalizeReferenceNotes(source){

  const data = (source && typeof source === "object") ? source : {};
  const byDeptRaw = (data.byDept && typeof data.byDept === "object") ? data.byDept : {};

  const normalizeSections = (value)=>{

    if(typeof value === "string"){
      return {
        orders: "",
        contacts: "",
        staff: "",
        other: value,
      };
    }

    const item = (value && typeof value === "object") ? value : {};

    return {
      orders: typeof item.orders === "string" ? item.orders : "",
      contacts: typeof item.contacts === "string" ? item.contacts : "",
      staff: typeof item.staff === "string" ? item.staff : "",
      other: typeof item.other === "string" ? item.other : "",
    };

  };

  const sectionsToText = (sections)=>[
    sections.orders ? `Накази / нормативка: ${sections.orders}` : "",
    sections.contacts ? `Контакти / канали: ${sections.contacts}` : "",
    sections.staff ? `Штатні пропозиції / структура: ${sections.staff}` : "",
    sections.other ? `Інше: ${sections.other}` : "",
  ].filter(Boolean).join("\n");

  const normalizeEntry = (value, index=0)=>{

    if(typeof value === "string"){
      return {
        id: uid("ref"),
        deptId: "",
        title: "",
        text: value.trim(),
        tableType: "none",
        createdAt: nowIsoKyiv(),
        updatedAt: nowIsoKyiv(),
      };
    }

    const item = (value && typeof value === "object") ? value : {};
    const tableVersions = Array.isArray(item.tableVersions)
      ? item.tableVersions.map((version, versionIndex)=>{
          const versionItem = (version && typeof version === "object") ? version : {};
          let rows = [];

          if(Array.isArray(versionItem.rows)){
            rows = cloneStoredTableRows(versionItem.rows);
          } else if(typeof versionItem.text === "string" && versionItem.text.trim()){
            const versionBlock = findStoredTableBlock(versionItem.text) || {rows: parseStoredTableRows(versionItem.text)};
            rows = cloneStoredTableRows(versionBlock.rows);
          }

          return {
            id: typeof versionItem.id === "string" && versionItem.id ? versionItem.id : uid(`ref_ver_${index}_${versionIndex}`),
            createdAt: typeof versionItem.createdAt === "string" && versionItem.createdAt ? versionItem.createdAt : nowIsoKyiv(),
            rows,
          };
        }).filter(item=>item.rows.length)
      : [];

    tableVersions.sort((a,b)=> String(b.createdAt || "").localeCompare(String(a.createdAt || "")));

    return {
      id: typeof item.id === "string" && item.id ? item.id : uid(`ref_${index}`),
      deptId: typeof item.deptId === "string" ? item.deptId : "",
      title: typeof item.title === "string" ? item.title.trim() : "",
      text: typeof item.text === "string" ? item.text.trim() : "",
      tableType: normalizeReferenceTableType(item.tableType),
      createdAt: typeof item.createdAt === "string" && item.createdAt ? item.createdAt : nowIsoKyiv(),
      updatedAt: typeof item.updatedAt === "string" && item.updatedAt ? item.updatedAt : nowIsoKyiv(),
      tableVersions,
    };

  };


  const normalizeAttachment = (value, index=0)=>{

    const item = (value && typeof value === "object") ? value : {};

    return {
      id: typeof item.id === "string" && item.id ? item.id : uid(`ref_file_${index}`),
      entryId: typeof item.entryId === "string" ? item.entryId : "",
      deptId: typeof item.deptId === "string" ? item.deptId : "",
      title: typeof item.title === "string" ? item.title.trim() : "",
      url: typeof item.url === "string" ? item.url.trim() : "",
      note: typeof item.note === "string" ? item.note.trim() : "",
      createdAt: typeof item.createdAt === "string" && item.createdAt ? item.createdAt : nowIsoKyiv(),
      updatedAt: typeof item.updatedAt === "string" && item.updatedAt ? item.updatedAt : nowIsoKyiv(),
    };

  };

  const byDept = {};

  Object.keys(byDeptRaw).forEach(key=>{
    byDept[key] = normalizeSections(byDeptRaw[key]);
  });

  let entries = Array.isArray(data.entries)
    ? data.entries.map((item, index)=>normalizeEntry(item, index)).filter(item=>item.text)
    : [];

  if(!entries.length){

    const legacyEntries = [];
    const general = normalizeSections(data.general);
    const generalText = sectionsToText(general);

    if(generalText){
      legacyEntries.push({
        id: "ref_general_legacy",
        deptId: "",
        title: "Загальне",
        text: generalText,
        tableType: "none",
        tableVersions: [],
        createdAt: nowIsoKyiv(),
        updatedAt: nowIsoKyiv(),
      });
    }

    Object.keys(byDept).forEach((deptId, index)=>{
      const deptText = sectionsToText(byDept[deptId]);
      if(!deptText) return;
      legacyEntries.push({
        id: `ref_${deptId}_legacy_${index}`,
        deptId,
        title: "",
        text: deptText,
        tableType: "none",
        tableVersions: [],
        createdAt: nowIsoKyiv(),
        updatedAt: nowIsoKyiv(),
      });
    });

    entries = legacyEntries;

  }

  const attachments = Array.isArray(data.attachments)
    ? data.attachments.map((item, index)=>normalizeAttachment(item, index)).filter(item=>item.url)
    : [];

  entries.sort((a,b)=> String(b.updatedAt || b.createdAt || "").localeCompare(String(a.updatedAt || a.createdAt || "")));
  attachments.sort((a,b)=> String(b.updatedAt || b.createdAt || "").localeCompare(String(a.updatedAt || a.createdAt || "")));

  return {
    general: normalizeSections(data.general),
    byDept,
    entries,
    attachments,
  };

}

const REFERENCE_TABLE_TYPE_OPTIONS = [
  {id:"none", label:"Без аналітики"},
  {id:"staffing", label:"Укомплектованість"},
  {id:"planfact", label:"План / факт"},
  {id:"compare", label:"Порівняння"},
  {id:"delta_bpla", label:"Delta / БпЛА"},
  {id:"delta_nrk", label:"Delta / НРК"},
];

function normalizeReferenceTableType(value){

  const raw = String(value || "").trim().toLowerCase();

  if(raw === "staffing") return "staffing";
  if(raw === "planfact") return "planfact";
  if(raw === "compare") return "compare";
  if(raw === "delta_bpla") return "delta_bpla";
  if(raw === "delta_nrk") return "delta_nrk";
  return "none";

}

function getLatestReferenceTableVersion(entry){

  const versions = Array.isArray(entry?.tableVersions) ? entry.tableVersions : [];

  return versions.length ? versions[0] : null;

}

function getReferenceTableTypeLabel(value){

  const typeId = normalizeReferenceTableType(value);
  return REFERENCE_TABLE_TYPE_OPTIONS.find(x=>x.id===typeId)?.label || "Без аналітики";

}

const DEFAULT_EVALUATION_START_DATE = "2026-03-23";

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

    taskEvaluations: Array.isArray(st.taskEvaluations) ? st.taskEvaluations : [],

    dailyReports: Array.isArray(st.dailyReports) ? st.dailyReports : [],

    deptSummaries: Array.isArray(st.deptSummaries) ? st.deptSummaries : [],

    weeklyTasks: Array.isArray(st.weeklyTasks) ? st.weeklyTasks : [],

    recurringTemplates: Array.isArray(st.recurringTemplates) ? st.recurringTemplates : [],

    reportPlans: Array.isArray(st.reportPlans) ? st.reportPlans : [],

    referenceNotes: normalizeReferenceNotes(st.referenceNotes),

    evaluationStartDate: typeof st.evaluationStartDate === "string" && st.evaluationStartDate
      ? st.evaluationStartDate
      : DEFAULT_EVALUATION_START_DATE,

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
  if(next.version < 6){

    next.version = 6;

  }

  if(next.version < 9){

    if(!next.evaluationStartDate || next.evaluationStartDate < DEFAULT_EVALUATION_START_DATE){

      next.evaluationStartDate = DEFAULT_EVALUATION_START_DATE;

    }

    next.version = 9;

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

  const persisted = safeSet(LS_KEY, JSON.stringify(st));

  _lastLocalPersistOk = persisted;

  if(!persisted && !opts.silentLocalPersistWarning){

    console.warn("localStorage save failed; state kept only in memory until sync completes");

    if(_syncReady){

      pushSync();

    }

  }

}

function ensureCriticalStateSaved(warnText="Зміни поки що збережені тимчасово. Дочекайся синхронізації перед оновленням сторінки."){

  if(!_lastLocalPersistOk){

    showToast(warnText, "warn");

  }

  if(_syncReady){

    pushSync();

  }

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

function reportPlanOccurrenceState(monthStr, scheduledDate, deptIds, taskMap){

  const ids = Array.isArray(deptIds) ? deptIds.filter(Boolean) : [];

  if(!ids.length){

    return {kind:"empty", label:"Відділи не вибрані", closedCount:0, createdCount:0, total:0};

  }

  const tasks = ids.map(deptId=>taskMap.get(`${deptId}__${scheduledDate}`) || null);
  const total = tasks.length;
  const createdCount = tasks.filter(Boolean).length;
  const closedCount = tasks.filter(t=>t?.status === "закрито").length;

  if(closedCount === total){

    return {kind:"done", label:"Закрито", closedCount, createdCount, total};

  }

  if(closedCount > 0){

    return {kind:"partial", label:`Частково виконано ${closedCount}/${total}`, closedCount, createdCount, total};

  }

  if(createdCount > 0){

    const hasBlocker = tasks.some(t=>t && (t.status === "блокер" || t.status === "очікування"));
    const hasPending = tasks.some(t=>t?.status === "очікує_підтвердження");

    if(hasBlocker){

      return {kind:"blocked", label:"Є блокери", closedCount, createdCount, total};

    }

    if(hasPending){

      return {kind:"pending", label:"Очікує підтвердження", closedCount, createdCount, total};

    }

    return {kind:"progress", label:"В роботі", closedCount, createdCount, total};

  }

  const missingLabel = reportingMissingLabel(monthStr, scheduledDate);

  return {
    kind: missingLabel === "Не створено" ? "missing" : "planned",
    label: missingLabel,
    closedCount,
    createdCount,
    total
  };

}

function reportPlanOccurrenceBadgeHtml(state){

  if(!state) return `<span class="badge">—</span>`;

  if(state.kind === "done") return `<span class="badge b-ok">✅ ${htmlesc(state.label)}</span>`;
  if(state.kind === "partial") return `<span class="badge b-warn">◐ ${htmlesc(state.label)}</span>`;
  if(state.kind === "blocked") return `<span class="badge b-warn">⛔ ${htmlesc(state.label)}</span>`;
  if(state.kind === "pending") return `<span class="badge b-violet">🕒 ${htmlesc(state.label)}</span>`;
  if(state.kind === "progress") return `<span class="badge b-blue">🔄 ${htmlesc(state.label)}</span>`;
  if(state.kind === "missing") return `<span class="badge b-warn">⚠️ ${htmlesc(state.label)}</span>`;
  if(state.kind === "planned") return `<span class="badge">🗓 ${htmlesc(state.label)}</span>`;

  return `<span class="badge">${htmlesc(state.label || "—")}</span>`;

}

function runReportPlans(){

  if(!STATE.reportPlans) STATE.reportPlans = [];

  const today = kyivDateStr();
  const monthCandidates = Array.from(new Set([today.slice(0,7), addDays(today, 1).slice(0,7)]));



  STATE.reportPlans.forEach(plan=>{

    monthCandidates.forEach(monthStr=>{

      const scheduleDates = reportPlanScheduleDates(plan, monthStr);

      if(!scheduleDates.length) return;

      const deptIds = Array.isArray(plan.deptIds) ? plan.deptIds : [];

      scheduleDates.forEach(scheduledDate=>{

        const triggerDate = addDays(scheduledDate, -1);

        if(today < triggerDate) return;

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

            startDate: triggerDate,

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

    version: 9,

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

    taskEvaluations: [],

    dailyReports: [],

    deptSummaries: [],

    weeklyTasks: [],

    recurringTemplates: [],

    reportPlans: [],

    referenceNotes: {
      general: {orders:"", contacts:"", staff:"", other:""},
      byDept: {},
      entries: [],
      attachments: [],
    },

    evaluationStartDate: DEFAULT_EVALUATION_START_DATE,

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

function applyInlineRichText(safeText){

  let out = safeText || "";

  out = out.replace(/__(.+?)__/g, "<u>$1</u>");

  out = out.replace(/~~(.+?)~~/g, "<s>$1</s>");

  out = out.replace(/\*\*(.+?)\*\*/g, "<b>$1</b>");

  out = out.replace(/(^|[^*])\*([^*]+)\*(?!\*)/g, "$1<i>$2</i>");

  return out;

}

function splitMarkdownTableRow(line){

  return String(line || "")

    .trim()

    .replace(/^\|/, "")

    .replace(/\|$/, "")

    .split("|")

    .map(cell=>cell.trim());

}

const TABLE_LINEBREAK_TOKEN = "[[LB]]";

function sanitizePastedTableCell(value){

  let next = String(value ?? "")

    .replace(/\u00A0/g, " ")

    .replace(/[\u200B-\u200D\u2060\uFEFF]/g, "")

    .replace(/\r/g, " ")

    .replace(/\t/g, " ")

    .replace(/\s*\n\s*/g, "\n")

    .replace(/[ \f\v]+/g, " ")

    .trim();

  if(/^"[\s\S]*"$/.test(next)){

    next = next.slice(1, -1).replace(/""/g, '"').trim();

  }

  return next;

}

function mergeBrokenClipboardRows(rows){

  const safeRows = Array.isArray(rows)

    ? rows
        .map(row=>Array.isArray(row) ? row.map(cell=>sanitizePastedTableCell(cell)) : [])
        .filter(row=>row.some(cell=>String(cell || "").trim()))

    : [];

  if(!safeRows.length) return [];

  const expectedWidth = Math.max(2, ...safeRows.map(row=>row.length));

  const out = [];

  let pending = null;

  const appendContinuationToRow = (targetRow, continuationRow)=>{

    if(!Array.isArray(targetRow) || !Array.isArray(continuationRow) || !continuationRow.length) return targetRow;

    let attachIdx = -1;

    for(let i = targetRow.length - 1; i >= 0; i -= 1){

      if(String(targetRow[i] || "").trim()){

        attachIdx = i;

        break;

      }

    }

    if(attachIdx < 0) attachIdx = Math.max(0, targetRow.length - 1);

    const firstPart = String(continuationRow[0] || "").trim();

    if(firstPart){

      targetRow[attachIdx] = sanitizePastedTableCell(
        [targetRow[attachIdx], firstPart]
          .filter(Boolean)
          .join("\n")
      );

    }

    if(continuationRow.length > 1){

      let cursor = attachIdx + 1;

      for(const cell of continuationRow.slice(1)){

        if(cursor >= targetRow.length) break;

        const clean = String(cell || "").trim();

        if(clean){

          targetRow[cursor] = sanitizePastedTableCell(
            [targetRow[cursor], clean]
              .filter(Boolean)
              .join("\n")
          );

        }

        cursor += 1;

      }

    }

    return targetRow;

  };

  const pushPending = ()=>{

    if(!pending) return;

    const next = pending.slice(0, expectedWidth);

    while(next.length < expectedWidth) next.push("");

    out.push(next);

    pending = null;

  };

  safeRows.forEach(row=>{

    if(!pending && out.length > 1 && row.length < expectedWidth){

      out[out.length - 1] = appendContinuationToRow(out[out.length - 1], row.slice());

      return;

    }

    if(!pending){

      pending = row.slice();

      if(pending.length >= expectedWidth) pushPending();

      return;

    }

    if(pending.length < expectedWidth){

      const continuation = String(row[0] || "").trim();

      if(continuation){

        pending[pending.length - 1] = sanitizePastedTableCell(
          [pending[pending.length - 1], continuation]
            .filter(Boolean)
            .join("\n")
        );

      }

      if(row.length > 1){

        pending.push(...row.slice(1));

      }

      if(pending.length >= expectedWidth) pushPending();

      return;

    }

    pushPending();

    pending = row.slice();

    if(pending.length >= expectedWidth) pushPending();

  });

  pushPending();

  return out.map(row=>row.map(cell=>sanitizePastedTableCell(cell)));

}

function parseStoredTableRows(content){

  const rows = String(content || "")

    .split(/\r?\n/)

    .map(line=>splitMarkdownTableRow(line))

    .filter(row=>row.some(cell=>String(cell || "").trim()));

  if(!rows.length) return [["Колонка 1","Колонка 2"],["",""]];

  const width = Math.max(2, ...rows.map(row=>row.length));

  return rows.map(row=>{

    const next = row
      .slice(0, width)
      .map(cell=>String(cell || "").replaceAll(TABLE_LINEBREAK_TOKEN, "\n"));

    while(next.length < width) next.push("");

    return next;

  });

}

function parseClipboardTableText(text){

  const raw = String(text || "")

    .replace(/\u00A0/g, " ")

    .replace(/[\u200B-\u200D\u2060\uFEFF]/g, "")

    .replace(/\r/g, "")

    .trim();

  if(!raw) return [];

  const lines = raw

    .split("\n")

    .map(line=>line.replace(/[ \t]+$/g, ""))

    .filter(line=>line.trim().length);

  if(!lines.length) return [];

  let rows = lines.map(line=>line.split("\t").map(cell=>sanitizePastedTableCell(cell)));

  const maxCols = Math.max(0, ...rows.map(row=>row.length));

  if(maxCols < 2 && raw.includes("|")){

    rows = lines

      .map(splitMarkdownTableRow)

      .map(row=>row.map(cell=>sanitizePastedTableCell(cell)))

      .filter(row=>row.some(cell=>String(cell || "").trim()));

  }

  rows = mergeBrokenClipboardRows(rows);

  const width = Math.max(2, ...rows.map(row=>row.length));

  return rows.map(row=>{

    const next = row.slice(0, width);

    while(next.length < width) next.push("");

    return next;

  });

}

function findStoredTableBlockByMarker(text, marker="TABLE"){

  const safeMarker = String(marker || "TABLE").replace(/[^\w]/g, "");

  const re = new RegExp(`\\[\\[${safeMarker}\\]\\]\\r?\\n([\\s\\S]*?)\\r?\\n\\[\\[\\/${safeMarker}\\]\\]`);

  const match = re.exec(String(text || ""));

  if(!match) return null;

  return {
    start: match.index,
    end: match.index + match[0].length,
    raw: match[0],
    rows: parseStoredTableRows(match[1] || "")
  };

}

function findStoredTableBlock(text){

  return findStoredTableBlockByMarker(text, "TABLE");

}

function findPreviousStoredTableBlock(text){

  return findStoredTableBlockByMarker(text, "TABLE_PREV");

}

function hasStoredTable(text){

  return /\[\[TABLE\]\]/.test(String(text || ""));

}

function cloneStoredTableRows(rows){

  return Array.isArray(rows)
    ? rows.map(row=>(Array.isArray(row) ? row : []).map(cell=>String(cell ?? "")))
    : [];

}

function areStoredTableRowsEqual(a, b){

  return serializeStoredTable(cloneStoredTableRows(a)) === serializeStoredTable(cloneStoredTableRows(b));

}

function serializeStoredTable(rows, marker="TABLE"){

  const normalized = (rows || []).map(row=>(row || []).map(cell=>
    String(cell || "")
      .replace(/\|/g, "/")
      .replace(/\r/g, "")
      .replace(/\n/g, TABLE_LINEBREAK_TOKEN)
      .trim()
  ));

  const safeMarker = String(marker || "TABLE").replace(/[^\w]/g, "");

  return `[[${safeMarker}]]\n${normalized.map(row=>`| ${row.join(" | ")} |`).join("\n")}\n[[/${safeMarker}]]`;

}

function isMarkdownTableBlock(lines){

  if(!Array.isArray(lines) || lines.length < 2) return false;

  if(!lines[0].includes("|") || !lines[1].includes("|")) return false;

  const header = splitMarkdownTableRow(lines[0]);

  const divider = splitMarkdownTableRow(lines[1]);

  if(header.length < 2 || divider.length !== header.length) return false;

  if(!divider.every(cell=>/^:?-{3,}:?$/.test(cell))) return false;

  return true;

}

function renderMarkdownTableBlock(lines){

  const rows = lines.map(splitMarkdownTableRow);

  const header = rows[0] || [];

  const body = rows.slice(2).filter(row=>row.some(cell=>String(cell || "").trim()));

  const renderCellHtml = (cell)=> applyInlineRichText(String(cell || "")).replaceAll(TABLE_LINEBREAK_TOKEN, "<br/>").replace(/\n/g, "<br/>");

  const headHtml = `<tr>${header.map(cell=>`<th>${renderCellHtml(cell)}</th>`).join("")}</tr>`;

  const bodyHtml = body.length

    ? body.map(row=>`<tr>${row.map(cell=>`<td>${renderCellHtml(cell)}</td>`).join("")}</tr>`).join("")

    : `<tr>${header.map(()=>`<td>—</td>`).join("")}</tr>`;

  return `<div class="rt-table-wrap"><table class="rt-table"><thead>${headHtml}</thead><tbody>${bodyHtml}</tbody></table></div>`;

}

function renderStoredTableBlock(content){

  return renderMarkdownTableBlock(

    parseStoredTableRows(content).map((row, idx)=>{

      const line = `| ${row.join(" | ")} |`;

      if(idx === 0){

        return [line, `| ${row.map(()=> "---").join(" | ")} |`];

      }

      return line;

    }).flat()

  );

}

function normalizeComparedTableRows(rows, width, height){

  const safeRows = Array.isArray(rows) ? rows : [];

  return Array.from({length:height}, (_, r)=>{

    const src = Array.isArray(safeRows[r]) ? safeRows[r] : [];

    return Array.from({length:width}, (_, c)=> String(src[c] ?? "").trim());

  });

}

function tableDiffMeta(currentRows, previousRows){

  const currentWidth = Math.max(2, ...(Array.isArray(currentRows) ? currentRows.map(row=>Array.isArray(row) ? row.length : 0) : [0]));

  const previousWidth = Math.max(2, ...(Array.isArray(previousRows) ? previousRows.map(row=>Array.isArray(row) ? row.length : 0) : [0]));

  const width = Math.max(currentWidth, previousWidth);

  const height = Math.max(Array.isArray(currentRows) ? currentRows.length : 0, Array.isArray(previousRows) ? previousRows.length : 0, 2);

  const current = normalizeComparedTableRows(currentRows, width, height);

  const previous = normalizeComparedTableRows(previousRows, width, height);

  let changedCount = 0;

  for(let r=0; r<height; r+=1){

    for(let c=0; c<width; c+=1){

      if((current[r]?.[c] || "") !== (previous[r]?.[c] || "")) changedCount += 1;

    }

  }

  return {
    width,
    height,
    current,
    previous,
    changedCount,
    structureChanged: currentWidth !== previousWidth || (Array.isArray(currentRows) ? currentRows.length : 0) !== (Array.isArray(previousRows) ? previousRows.length : 0)
  };

}

function renderTableDiffBlock(currentRows, previousRows){

  const meta = tableDiffMeta(currentRows, previousRows);

  const headerCells = meta.current[0].map((cell, idx)=>{

    const prev = meta.previous[0]?.[idx] || "";

    const changed = cell !== prev;

    const cls = changed ? ` class="is-diff"` : "";

    const title = changed && prev ? ` title="Було: ${htmlesc(prev)}"` : "";

    return `<th${cls}${title}>${applyInlineRichText(String(cell || prev || "—")).replaceAll(TABLE_LINEBREAK_TOKEN, "<br/>").replace(/\n/g, "<br/>")}</th>`;

  }).join("");

  const bodyHtml = Array.from({length:Math.max(1, meta.height - 1)}, (_, rowIndex)=>{

    const r = rowIndex + 1;

    const cells = meta.current[r].map((cell, colIndex)=>{

      const prev = meta.previous[r]?.[colIndex] || "";

      const changed = cell !== prev;

      const cls = changed ? ` class="is-diff"` : "";

      const title = changed ? ` title="Було: ${htmlesc(prev || "—")}"` : "";

      return `<td${cls}${title}>${applyInlineRichText(String(cell || "—")).replaceAll(TABLE_LINEBREAK_TOKEN, "<br/>").replace(/\n/g, "<br/>")}</td>`;

    }).join("");

    return `<tr>${cells}</tr>`;

  }).join("");

  return `

    <div class="task-table-diff-meta">Змінено клітинок: <span class="mono">${meta.changedCount}</span></div>

    <div class="rt-table-wrap"><table class="rt-table rt-table-diff"><thead><tr>${headerCells}</tr></thead><tbody>${bodyHtml}</tbody></table></div>

  `;

}

function richText(s){

  const safe = htmlesc(String(s ?? "").replace(/\[\[TABLE_PREV\]\]\r?\n[\s\S]*?\r?\n\[\[\/TABLE_PREV\]\]/g, ""));

  if(!safe) return "";

  const chunks = [];

  let cursor = 0;

  safe.replace(/\[\[TABLE\]\]\r?\n([\s\S]*?)\r?\n\[\[\/TABLE\]\]/g, (full, tableContent, offset)=>{

    chunks.push({type:"text", value:safe.slice(cursor, offset)});

    chunks.push({type:"table", value:tableContent});

    cursor = offset + full.length;

    return full;

  });

  chunks.push({type:"text", value:safe.slice(cursor)});

  const renderTextChunk = (text)=> text.split(/\r?\n\r?\n+/).map(block=>{

    const lines = block.split(/\r?\n/);

    if(isMarkdownTableBlock(lines)) return renderMarkdownTableBlock(lines);

    return applyInlineRichText(block);

  }).join("\n\n");

  return chunks.map(chunk=>{

    if(chunk.type === "table") return renderStoredTableBlock(chunk.value);

    return renderTextChunk(chunk.value);

  }).join("");

}

function formatToolbar(textareaId, variant="", opts={}){

  const cls = variant === "inline" ? "format-chips inline" : "format-chips";

  const tableBtn = opts?.table

    ? `<button class="format-chip" data-action="insertTextTable" data-arg1="${textareaId}" title="Вставити таблицю"><span class="format-ico">▦</span></button>`

    : "";

  const pasteTableBtn = opts?.table

    ? `<button class="format-chip" data-action="pasteTextTableFromClipboard" data-arg1="${textareaId}" title="Вставити з Excel / Word"><span class="format-ico">📋</span></button>`

    : "";

  return `

    <div class="${cls}" aria-label="Форматування тексту">

      <button class="format-chip" data-action="applyTextFormat" data-arg1="${textareaId}" data-arg2="bold" title="Жирний (**текст**)"><span class="format-ico">B</span></button>

      <button class="format-chip" data-action="applyTextFormat" data-arg1="${textareaId}" data-arg2="italic" title="Курсив (*текст*)"><span class="format-ico"><i>I</i></span></button>

      <button class="format-chip" data-action="applyTextFormat" data-arg1="${textareaId}" data-arg2="underline" title="Підкреслення (__текст__)"><span class="format-ico"><u>U</u></span></button>

      <button class="format-chip" data-action="applyTextFormat" data-arg1="${textareaId}" data-arg2="strike" title="Перекреслення (~~текст~~)"><span class="format-ico"><s>S</s></span></button>

      ${tableBtn}

      ${pasteTableBtn}

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

function defaultTextTableRows(){

  return [
    ["Колонка 1","Колонка 2"],
    ["",""],
    ["",""]
  ];

}

function readTextTableEditorRows(textareaId){

  const wrap = document.querySelector(`.text-table-editor[data-for="${textareaId}"]`);

  if(!wrap) return defaultTextTableRows();

  const rows = Number(wrap.dataset.rows || 0);

  const cols = Number(wrap.dataset.cols || 0);

  if(!rows || !cols) return defaultTextTableRows();

  const data = [];

  for(let r=0; r<rows; r+=1){

    const row = [];

    for(let c=0; c<cols; c+=1){

      row.push(document.getElementById(`${textareaId}_tbl_${r}_${c}`)?.value || "");

    }

    data.push(row);

  }

  return data;

}

function getTextTableActiveCell(textareaId, rows){

  const wrap = document.querySelector(`.text-table-editor[data-for="${textareaId}"]`);

  const rowCount = rows?.length || 0;

  const colCount = rows?.[0]?.length || 0;

  if(!wrap || !rowCount || !colCount) return {row:0, col:0};

  const row = Math.max(0, Math.min(rowCount - 1, Number(wrap.dataset.activeRow || 0)));

  const col = Math.max(0, Math.min(colCount - 1, Number(wrap.dataset.activeCol || 0)));

  return {row, col};

}

function setTextTableActiveCell(textareaId, row, col){

  const wrap = document.querySelector(`.text-table-editor[data-for="${textareaId}"]`);

  if(!wrap) return;

  wrap.dataset.activeRow = String(Math.max(0, Number(row || 0)));

  wrap.dataset.activeCol = String(Math.max(0, Number(col || 0)));

}

function hideTextTableContextMenu(textareaId){

  const menu = document.querySelector(`.text-table-context-menu[data-for="${textareaId}"]`);

  if(!menu) return;

  menu.hidden = true;

}

function positionTextTableContextMenu(textareaId, row, col){

  const wrap = document.querySelector(`.text-table-editor[data-for="${textareaId}"]`);

  const menu = wrap?.querySelector(`.text-table-context-menu[data-for="${textareaId}"]`);

  const cell = document.getElementById(`${textareaId}_tbl_${row}_${col}`);

  if(!wrap || !menu || !cell){
    hideTextTableContextMenu(textareaId);
    return;
  }

  const wrapRect = wrap.getBoundingClientRect();
  const cellRect = cell.getBoundingClientRect();
  const menuWidth = menu.offsetWidth || 156;

  const left = Math.max(
    8,
    Math.min(
      (wrap.clientWidth || wrapRect.width) - menuWidth - 8,
      (cellRect.left - wrapRect.left) + ((cellRect.width - menuWidth) / 2)
    )
  );

  const top = Math.max(8, (cellRect.top - wrapRect.top) - 42);

  menu.style.left = `${Math.round(left)}px`;
  menu.style.top = `${Math.round(top)}px`;
  menu.hidden = false;

}

function showTextTableContextMenu(textareaId, row, col){

  setTextTableActiveCell(textareaId, row, col);

  requestAnimationFrame(()=>{
    positionTextTableContextMenu(textareaId, row, col);
  });

}

function buildTextTableEditorHtml(textareaId, rows){

  const safeRows = (rows && rows.length) ? rows : defaultTextTableRows();

  const rowCount = safeRows.length;

  const colCount = Math.max(2, ...safeRows.map(row=>row.length));

  const normalized = safeRows.map(row=>{

    const next = row.slice(0, colCount);

    while(next.length < colCount) next.push("");

    return next;

  });

  const grid = normalized.map((row, r)=>`

    <div class="text-table-row">

      ${row.map((cell, c)=>`<textarea id="${textareaId}_tbl_${r}_${c}" class="text-table-cell ${r===0 ? "is-head" : ""}" placeholder="${r===0 ? `Колонка ${c + 1}` : "Значення"}">${htmlesc(String(cell || "").replaceAll(TABLE_LINEBREAK_TOKEN, "\n"))}</textarea>`).join("")}

    </div>

  `).join("");

  return `

    <div class="text-table-editor" data-for="${textareaId}" data-rows="${rowCount}" data-cols="${colCount}">

      <div class="text-table-editor-head">

        <div class="hint">Таблиця для опису задачі. Перший рядок — заголовки. Вставка й видалення працюють від активної клітинки.</div>

        <div class="text-table-editor-actions">

          <button type="button" class="btn ghost btn-mini" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="insertRowAbove">+ Рядок ↑</button>

          <button type="button" class="btn ghost btn-mini" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="insertRowBelow">+ Рядок ↓</button>

          <button type="button" class="btn ghost btn-mini" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="insertColLeft">+ Колонка ←</button>

          <button type="button" class="btn ghost btn-mini" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="insertColRight">+ Колонка →</button>

          <button type="button" class="btn ghost btn-mini" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="removeActiveRow">- Рядок</button>

          <button type="button" class="btn ghost btn-mini" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="removeActiveCol">- Колонка</button>

        </div>

      </div>

      <div class="text-table-context-menu" data-for="${textareaId}" hidden>
        <button type="button" class="text-table-context-btn" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="insertRowAbove" title="Вставити рядок вище">↑Р</button>
        <button type="button" class="text-table-context-btn" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="insertRowBelow" title="Вставити рядок нижче">↓Р</button>
        <button type="button" class="text-table-context-btn" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="insertColLeft" title="Вставити колонку ліворуч">←К</button>
        <button type="button" class="text-table-context-btn" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="insertColRight" title="Вставити колонку праворуч">→К</button>
        <button type="button" class="text-table-context-btn danger" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="removeActiveRow" title="Видалити поточний рядок">−Р</button>
        <button type="button" class="text-table-context-btn danger" data-action="mutateTextTableEditor" data-arg1="${textareaId}" data-arg2="removeActiveCol" title="Видалити поточну колонку">−К</button>
      </div>

      <div class="text-table-grid">${grid}</div>

      <div class="actions" style="margin-top:12px;">

        <button type="button" class="btn primary btn-mini" data-action="applyTextTableEditor" data-arg1="${textareaId}">Оновити таблицю</button>

        <button type="button" class="btn danger btn-mini" data-action="deleteTextTableFromTextarea" data-arg1="${textareaId}">Видалити таблицю</button>

      </div>

    </div>

  `;

}

function writeTextTableToTextarea(textareaId, rows){

  const el = document.getElementById(textareaId);

  if(!el) return;

  const serialized = serializeStoredTable(rows);

  const currentRaw = String(el.dataset.tableRaw || "");

  const previousRaw = String(el.dataset.tablePrevRaw || "");

  const currentBlock = currentRaw ? findStoredTableBlock(currentRaw) : null;

  const previousSerialized = previousRaw || (
    (currentBlock && currentBlock.raw !== serialized)

      ? serializeStoredTable(currentBlock.rows, "TABLE_PREV")

      : ""
  );

  el.dataset.tableRaw = serialized;

  if(previousSerialized){

    el.dataset.tablePrevRaw = previousSerialized;

  } else {

    delete el.dataset.tablePrevRaw;

  }

}

function initDescriptionTableState(textareaId, fullText=""){

  const el = document.getElementById(textareaId);

  if(!el) return;

  const raw = String(fullText || "");

  const currentBlock = findStoredTableBlock(raw);

  const previousBlock = findPreviousStoredTableBlock(raw);

  el.value = stripStoredTables(raw);

  if(currentBlock?.raw){

    el.dataset.tableRaw = currentBlock.raw;

  } else {

    delete el.dataset.tableRaw;

  }

  if(previousBlock?.raw){

    el.dataset.tablePrevRaw = previousBlock.raw;

  } else {

    delete el.dataset.tablePrevRaw;

  }

}

function buildDescriptionValueFromEditor(textareaId){

  const el = document.getElementById(textareaId);

  if(!el) return "";

  const textOnly = stripStoredTables(el.value || "");

  const currentRaw = String(el.dataset.tableRaw || "").trim();

  const previousRaw = String(el.dataset.tablePrevRaw || "").trim();

  return [textOnly, currentRaw, previousRaw].filter(Boolean).join("\n\n").trim();

}

function bindTextTableEditorLiveSync(textareaId){

  const wrap = document.querySelector(`.text-table-editor[data-for="${textareaId}"]`);

  if(!wrap) return;

  const menu = wrap.querySelector(`.text-table-context-menu[data-for="${textareaId}"]`);

  if(menu){
    menu.addEventListener("mousedown", e=>e.preventDefault());
  }

  wrap.addEventListener("focusout", ()=>{
    setTimeout(()=>{
      const active = document.activeElement;
      if(active && wrap.contains(active)) return;
      hideTextTableContextMenu(textareaId);
    }, 0);
  });

  wrap.querySelectorAll(".text-table-cell").forEach(input=>{

    const m = input.id.match(/_tbl_(\d+)_(\d+)$/);

    const row = m ? Number(m[1]) : 0;

    const col = m ? Number(m[2]) : 0;

    const markActive = ()=>{
      showTextTableContextMenu(textareaId, row, col);
    };

    input.addEventListener("focus", markActive);

    input.addEventListener("click", markActive);

    input.addEventListener("input", ()=>{

      markActive();

      writeTextTableToTextarea(textareaId, readTextTableEditorRows(textareaId));
      positionTextTableContextMenu(textareaId, row, col);

    });

  });

}

function renderTextTableEditor(textareaId, rows, focusPos=null){

  const el = document.getElementById(textareaId);

  if(!el) return;

  const summary = document.querySelector(`.text-table-import-summary[data-for="${textareaId}"]`);

  if(summary) summary.remove();

  let wrap = document.querySelector(`.text-table-editor[data-for="${textareaId}"]`);

  if(!wrap){

    el.insertAdjacentHTML("afterend", buildTextTableEditorHtml(textareaId, rows));

    bindTextTableEditorLiveSync(textareaId);

    const nextFocus = focusPos || {row:0, col:0};
    setTextTableActiveCell(textareaId, nextFocus.row, nextFocus.col);
    document.getElementById(`${textareaId}_tbl_${nextFocus.row}_${nextFocus.col}`)?.focus();

    return;

  }

  wrap.outerHTML = buildTextTableEditorHtml(textareaId, rows);

  bindTextTableEditorLiveSync(textareaId);

  const nextFocus = focusPos || {row:0, col:0};
  setTextTableActiveCell(textareaId, nextFocus.row, nextFocus.col);
  document.getElementById(`${textareaId}_tbl_${nextFocus.row}_${nextFocus.col}`)?.focus();

}

function insertTextTable(textareaId){

  const el = document.getElementById(textareaId);

  if(!el) return;

  const existing = findStoredTableBlock(el.dataset.tableRaw || "");
  const rows = existing?.rows || defaultTextTableRows();

  if(textareaId === "referenceEntryText"){
    const tableType = normalizeReferenceTableType(document.getElementById("referenceEntryTableType")?.value || "none");
    renderReferenceEntryTableWorkspace(textareaId, tableType, rows);
    return;
  }

  renderTextTableEditor(textareaId, rows);

}

function mutateTextTableEditor(textareaId, action){

  let rows = readTextTableEditorRows(textareaId);
  const active = getTextTableActiveCell(textareaId, rows);
  let nextFocus = {...active};
  const emptyRow = ()=>new Array(rows[0]?.length || 2).fill("");

  if(action==="addRow"){

    rows.push(new Array(rows[0]?.length || 2).fill(""));

    nextFocus = {row: rows.length - 1, col: 0};

  } else if(action==="removeRow"){

    if(rows.length > 2){
      rows = rows.slice(0, -1);
      nextFocus = {row: Math.max(0, Math.min(active.row, rows.length - 1)), col: Math.min(active.col, (rows[0]?.length || 1) - 1)};
    }

  } else if(action==="addCol"){

    rows = rows.map(row=>[...row, ""]);

    nextFocus = {row: active.row, col: rows[0].length - 1};

  } else if(action==="removeCol"){

    if((rows[0]?.length || 0) > 2){

      rows = rows.map(row=>row.slice(0, -1));

      nextFocus = {row: active.row, col: Math.max(0, Math.min(active.col, rows[0].length - 1))};

    }

  } else if(action==="insertRowAbove"){

    rows.splice(active.row, 0, emptyRow());

    nextFocus = {row: active.row, col: active.col};

  } else if(action==="insertRowBelow"){

    rows.splice(active.row + 1, 0, emptyRow());

    nextFocus = {row: active.row + 1, col: active.col};

  } else if(action==="insertColLeft"){

    rows = rows.map((row, index)=>{
      const next = row.slice();
      next.splice(active.col, 0, "");
      return next;
    });

    nextFocus = {row: active.row, col: active.col};

  } else if(action==="insertColRight"){

    rows = rows.map(row=>{
      const next = row.slice();
      next.splice(active.col + 1, 0, "");
      return next;
    });

    nextFocus = {row: active.row, col: active.col + 1};

  } else if(action==="removeActiveRow"){

    if(rows.length > 2){
      rows.splice(active.row, 1);
      nextFocus = {row: Math.max(0, Math.min(active.row, rows.length - 1)), col: Math.min(active.col, (rows[0]?.length || 1) - 1)};
    }

  } else if(action==="removeActiveCol"){

    if((rows[0]?.length || 0) > 2){
      rows = rows.map(row=>{
        const next = row.slice();
        next.splice(active.col, 1);
        return next;
      });
      nextFocus = {row: active.row, col: Math.max(0, Math.min(active.col, rows[0].length - 1))};
    }

  }

  renderTextTableEditor(textareaId, rows, nextFocus);

}

function applyTextTableEditor(textareaId){

  const el = document.getElementById(textareaId);

  if(!el) return;

  const rows = readTextTableEditorRows(textareaId).map(row=>row.map(cell=>String(cell || "").trim()));

  writeTextTableToTextarea(textareaId, rows);

  const active = getTextTableActiveCell(textareaId, rows);

  renderTextTableEditor(textareaId, rows, active);

}

function deleteTextTableFromTextarea(textareaId){

  const el = document.getElementById(textareaId);

  if(!el) return;

  el.value = stripStoredTables(el.value || "");

  delete el.dataset.tableRaw;

  delete el.dataset.tablePrevRaw;

  closeTextTableEditor(textareaId);

}

function closeTextTableEditor(textareaId){

  const wrap = document.querySelector(`.text-table-editor[data-for="${textareaId}"]`);

  if(wrap) wrap.remove();

  const summary = document.querySelector(`.text-table-import-summary[data-for="${textareaId}"]`);

  if(summary) summary.remove();

}

function extractStoredTables(text){

  const items = [];

  String(text || "").replace(/\[\[TABLE\]\]\r?\n([\s\S]*?)\r?\n\[\[\/TABLE\]\]/g, (_, content)=>{

    items.push({content: content || ""});

    return _;

  });

  return items;

}

function stripStoredTables(text){

  return String(text || "")

    .replace(/\[\[TABLE_PREV\]\]\r?\n[\s\S]*?\r?\n\[\[\/TABLE_PREV\]\]/g, "")

    .replace(/\[\[TABLE\]\]\r?\n[\s\S]*?\r?\n\[\[\/TABLE\]\]/g, "")

    .replace(/\n{3,}/g, "\n\n")

    .trim();

}

function renderTaskDescWithTableToggle(text, label, opts={}){

  const raw = String(text || "");

  const tables = extractStoredTables(raw);

  const currentTable = findStoredTableBlock(raw);

  const previousTable = findPreviousStoredTableBlock(raw);
  const analyticsType = normalizeReferenceTableType(opts.analyticsType || opts.tableType || "");
  let analyticsModalKey = "";

  if(currentTable?.rows?.length){

    if(analyticsType === "staffing"){
      analyticsModalKey = registerStaffingAnalyticsModalSet(
        currentTable.rows,
        opts.analyticsTitle || label || "Таблиця",
        Array.isArray(opts.dynamicVersions) ? opts.dynamicVersions : [],
        opts.updatedAt || ""
      );
      } else if(analyticsType === "compare"){
        analyticsModalKey = registerRenderedTableModal(
          `Аналітика: ${opts.analyticsTitle || label || "Таблиця"}`,
          buildComparisonAnalyticsModalHtml(currentTable.rows, opts.analyticsTitle || label || "Таблиця")
        );
      } else if(analyticsType === "delta_bpla"){
        analyticsModalKey = registerRenderedTableModal(
          `${opts.analyticsTitle || label || "Delta / БпЛА"}`,
          ""
        );
        UI.renderedTableModals[analyticsModalKey].deltaRows = currentTable.rows;
        UI.renderedTableModals[analyticsModalKey].deltaTitle = opts.analyticsTitle || label || "Delta / БпЛА";
        UI.renderedTableModals[analyticsModalKey].deltaFilters = {unit:"", taskType:"", asset:""};
        UI.renderedTableModals[analyticsModalKey].deltaAnalyticsKind = "delta_bpla";
        UI.renderedTableModals[analyticsModalKey].bodyHtml = buildDeltaBplaAnalyticsModalHtml(
          currentTable.rows,
          opts.analyticsTitle || label || "Delta / БпЛА",
          {modalKey: analyticsModalKey, filters: UI.renderedTableModals[analyticsModalKey].deltaFilters}
        );
      } else if(analyticsType === "delta_nrk"){
        analyticsModalKey = registerRenderedTableModal(
          `${opts.analyticsTitle || label || "Delta / НРК"}`,
          ""
        );
        UI.renderedTableModals[analyticsModalKey].deltaRows = currentTable.rows;
        UI.renderedTableModals[analyticsModalKey].deltaTitle = opts.analyticsTitle || label || "Delta / НРК";
        UI.renderedTableModals[analyticsModalKey].deltaFilters = {unit:"", taskType:"", asset:""};
        UI.renderedTableModals[analyticsModalKey].deltaAnalyticsKind = "delta_nrk";
        UI.renderedTableModals[analyticsModalKey].bodyHtml = buildDeltaNrkAnalyticsModalHtml(
          currentTable.rows,
          opts.analyticsTitle || label || "Delta / НРК",
        {modalKey: analyticsModalKey, filters: UI.renderedTableModals[analyticsModalKey].deltaFilters}
      );
    }

  }

  const diffMeta = (currentTable && previousTable) ? tableDiffMeta(currentTable.rows, previousTable.rows) : null;

  const textOnly = stripStoredTables(raw);

  const parts = [];

  if(textOnly){

    const startsWithBreak = /^\s*\r?\n/.test(textOnly);

    const prefix = startsWithBreak ? `${label}:<br/>` : `${label}: `;

    parts.push(`<div class="${opts.className || "task-desc rich-text"}">${prefix}${richText(textOnly)}</div>`);

  } else if(!tables.length && opts.showEmpty){

    parts.push(`<div class="${opts.className || "task-desc rich-text"}">${label}: —</div>`);

  }

  if(tables.length){

    const updatedShort = opts.updatedAt ? compactTimeFirst(opts.updatedAt) : "";
    const currentModalKey = registerRenderedTableModal(
      tables.length > 1 ? `${label || "Дані"} (${tables.length})` : (label || "Дані"),
      `<div class="rich-text">${tables.map(item=>renderStoredTableBlock(item.content)).join("")}</div>`
    );
    parts.push(`

      <div class="task-table-toggle task-table-toggle-actions">
        <button class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${currentModalKey}">${tables.length > 1 ? `Показати дані (${tables.length})` : "Показати дані"}</button>
        ${analyticsModalKey ? `<button class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${analyticsModalKey}">Аналітика</button>` : ``}
        ${updatedShort ? `<span class="task-table-stamp mono">${htmlesc(updatedShort)}</span>` : ``}
      </div>

    `);

    if(diffMeta && diffMeta.changedCount){
      const diffModalKey = registerRenderedTableModal(
        "Показати зміни",
        `<div class="rich-text">${renderTableDiffBlock(currentTable.rows, previousTable.rows)}</div>`
      );

      parts.push(`

        <div class="task-table-toggle task-table-toggle-actions task-table-diff-toggle">
          <button class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${diffModalKey}">Показати зміни</button>
          ${analyticsModalKey ? `<button class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${analyticsModalKey}">Аналітика</button>` : ``}
          <span class="task-table-diff-badge mono">${htmlesc(String(diffMeta.changedCount))}</span>
        </div>

      `);

      }

  }

  return parts.join("");

}

function registerRenderedTableModal(title, bodyHtml){

  if(!UI.renderedTableModals || typeof UI.renderedTableModals !== "object"){
    UI.renderedTableModals = {};
  }

  const key = `tbl_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;

  UI.renderedTableModals[key] = {
    title: String(title || "Дані"),
    bodyHtml: String(bodyHtml || "")
  };

  return key;

}

function getStaffingVersionShortDate(value, fallback=""){

  const compact = String(compactTimeFirst(value || "") || "").trim();
  if(!compact) return fallback;

  const parts = compact.split(/\s+/);
  return parts[1] || parts[0] || fallback;

}

function buildStaffingTrendPoints(currentRows, versions=[], currentAt=""){

  const currentAnalytics = buildStaffingAnalytics(currentRows);
  if(!currentAnalytics) return [];

  const normalizedVersions = Array.isArray(versions)
    ? versions
      .filter(item=>item && Array.isArray(item.rows) && item.rows.length)
      .slice(0, 7)
      .reverse()
    : [];

  const points = normalizedVersions.map((version, index)=>{
    const analytics = buildStaffingAnalytics(version.rows);
    if(!analytics) return null;
    return {
      id: String(version.id || `ver_${index}`),
      label: getStaffingVersionShortDate(version.createdAt, `V${index + 1}`),
      fact: analytics.totalFact,
      completion: analytics.completion,
      createdAt: version.createdAt || "",
      isCurrent: false,
    };
  }).filter(Boolean);

  points.push({
    id: "current",
    label: getStaffingVersionShortDate(currentAt, "Зараз"),
    fact: currentAnalytics.totalFact,
    completion: currentAnalytics.completion,
    createdAt: currentAt || "",
    isCurrent: true,
  });

  return points;

}

function buildSparklineSvg(values, color="#5a84ea"){

  const nums = Array.isArray(values)
    ? values.map(value=>Number(value)).filter(Number.isFinite)
    : [];

  if(nums.length < 2){
    return `<div class="staffing-trend-empty"></div>`;
  }

  const width = 220;
  const height = 56;
  const padX = 8;
  const padY = 8;
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const range = Math.max(max - min, 1);
  const stepX = (width - padX * 2) / Math.max(nums.length - 1, 1);

  const points = nums.map((value, index)=>{
    const x = padX + stepX * index;
    const y = height - padY - (((value - min) / range) * (height - padY * 2));
    return {x, y, value};
  });

  const polyline = points.map(point=>`${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const last = points[points.length - 1];
  const first = points[0];
  const area = `${polyline} ${last.x.toFixed(1)},${(height - padY).toFixed(1)} ${first.x.toFixed(1)},${(height - padY).toFixed(1)}`;

  return `
    <svg class="staffing-trend-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
      <polyline class="staffing-trend-area" points="${area}"></polyline>
      <polyline class="staffing-trend-line" points="${polyline}" style="--trend-color:${color};"></polyline>
      ${points.map((point, index)=>`<circle class="staffing-trend-dot ${index===points.length-1 ? "is-last" : ""}" cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="${index===points.length-1 ? "3.5" : "2.4"}" style="--trend-color:${color};"></circle>`).join("")}
    </svg>
  `;

}

function abbreviateStaffingUnitLabel(value){

  const text = String(value || "").trim();
  if(!text) return "—";
  if(text.length <= 12) return text;

  const words = text.split(/\s+/).filter(Boolean);
  if(words.length >= 2){
    const first = words[0];
    const rest = words.slice(1).map(word=>word[0]).join("");
    const compact = `${first} ${rest}`.trim();
    if(compact.length <= 12) return compact;
  }

  return `${text.slice(0, 10)}…`;

}

function buildStaffingBarChartSvg(seriesList, labels, opts={}){

  const series = Array.isArray(seriesList) ? seriesList.filter(item=>Array.isArray(item?.values) && item.values.some(Number.isFinite)) : [];
  const unitLabels = Array.isArray(labels) ? labels : [];
  if(!series.length || unitLabels.length < 1) return "";

  const width = Math.max(560, unitLabels.length * (series.length > 1 ? 84 : 62));
  const height = 228;
  const padTop = 16;
  const padRight = 18;
  const padBottom = 50;
  const padLeft = 22;
  const chartHeight = height - padTop - padBottom;
  const chartWidth = width - padLeft - padRight;
  const groupWidth = chartWidth / Math.max(unitLabels.length, 1);
  const innerGap = series.length > 1 ? 4 : 0;
  const usableGroupWidth = Math.max(groupWidth - 10, 18);
  const barWidth = Math.max(Math.min((usableGroupWidth - innerGap * Math.max(series.length - 1, 0)) / Math.max(series.length, 1), 18), 8);
  const allValues = series.flatMap(item=>item.values.map(value=>Number(value)).filter(Number.isFinite));
  if(!allValues.length) return "";

  const max = Math.max(...allValues, Number(opts.maxValue || 0), 1);
  const min = Number(opts.minValue || 0);
  const range = Math.max(max - min, 1);
  const gridValues = Array.from({length:4}, (_, idx)=>min + ((range / 3) * idx));

  const yGrid = gridValues.map(value=>{
    const y = height - padBottom - (((value - min) / range) * chartHeight);
    return `
      <line class="staffing-barchart-gridline" x1="${padLeft}" y1="${y.toFixed(2)}" x2="${(width - padRight).toFixed(2)}" y2="${y.toFixed(2)}"></line>
      <text class="staffing-barchart-ylabel" x="${(padLeft - 6).toFixed(2)}" y="${(y + 4).toFixed(2)}" text-anchor="end">${htmlesc(fmtNum(value))}</text>
    `;
  }).join("");

  const bars = unitLabels.map((label, groupIndex)=>{
    const groupX = padLeft + groupWidth * groupIndex + Math.max((groupWidth - usableGroupWidth) / 2, 0);
    const xLabelX = padLeft + groupWidth * groupIndex + (groupWidth / 2);
    const seriesBars = series.map((serie, serieIndex)=>{
      const raw = Number(serie.values[groupIndex]);
      const value = Number.isFinite(raw) ? raw : 0;
      const barHeight = ((value - min) / range) * chartHeight;
      const safeHeight = Math.max(barHeight, 0);
      const x = groupX + (barWidth + innerGap) * serieIndex;
      const y = height - padBottom - safeHeight;
      return `
        <rect
          class="staffing-barchart-bar is-animated-barchart"
          x="${x.toFixed(2)}"
          y="${y.toFixed(2)}"
          width="${barWidth.toFixed(2)}"
          height="${safeHeight.toFixed(2)}"
          rx="4"
          ry="4"
          style="--bar-color:${serie.color}; --bar-delay:${(groupIndex * 35) + (serieIndex * 80)}ms;"
        ></rect>
      `;
    }).join("");

    return `
      <g class="staffing-barchart-group">
        ${seriesBars}
        <text class="staffing-barchart-xlabel" x="${xLabelX.toFixed(2)}" y="${(height - 18).toFixed(2)}" text-anchor="middle">${htmlesc(abbreviateStaffingUnitLabel(label))}</text>
      </g>
    `;
  }).join("");

  return `
    <div class="staffing-barchart-scroll">
      <svg class="staffing-barchart-svg" viewBox="0 0 ${width} ${height}" style="min-width:${width}px" preserveAspectRatio="none" aria-hidden="true">
        ${yGrid}
        ${bars}
      </svg>
    </div>
  `;

}

function buildStaffingUnitChartsHtml(items){

  const rows = Array.isArray(items) ? items.filter(item=>Number(item?.plan || 0) > 0) : [];
  if(rows.length < 2) return "";

  const labels = rows.map(item=>item.name);
  const quantityChart = buildStaffingBarChartSvg([
    {label:"План", color:"#6d8ff5", values: rows.map(item=>Number(item.plan || 0))},
    {label:"Факт", color:"#44b678", values: rows.map(item=>Number(item.fact || 0))},
    {label:"Нестача", color:"#ffb347", values: rows.map(item=>Number(item.shortage || 0))},
  ], labels);

  const percentChart = buildStaffingBarChartSvg([
    {label:"% укомплектованості", color:"#7a6cf3", values: rows.map(item=>Number(item.percent || 0))},
  ], labels, {maxValue:100});

  const legend = (items)=>items.map(item=>`
    <div class="staffing-linechart-legend-item">
      <span class="staffing-linechart-legend-dot" style="background:${item.color}"></span>
      <span>${htmlesc(item.label)}</span>
    </div>
  `).join("");

  return `
    <div class="control-grid staffing-linecharts-grid">
      <div class="item analytics-block staffing-linechart-card">
        <div class="row"><div class="name">План / Факт / Нестача по підрозділах</div></div>
        <div class="staffing-linechart-legend">
          ${legend([
            {label:"План", color:"#6d8ff5"},
            {label:"Факт", color:"#44b678"},
            {label:"Нестача", color:"#ffb347"},
          ])}
        </div>
        <div class="hint staffing-linechart-note">По осі X — підрозділи, по осі Y — кількість.</div>
        ${quantityChart}
      </div>
      <div class="item analytics-block staffing-linechart-card">
        <div class="row"><div class="name">% укомплектованості по підрозділах</div></div>
        <div class="staffing-linechart-legend">
          ${legend([{label:"% укомплектованості", color:"#7a6cf3"}])}
        </div>
        <div class="hint staffing-linechart-note">Шкала до 100%, щоб одразу бачити провали і лідерів.</div>
        ${percentChart}
      </div>
    </div>
  `;

}

function buildStaffingTrendBlockHtml(points){

  const items = Array.isArray(points) ? points : [];
  if(items.length < 2) return "";

  const factValues = items.map(item=>item.fact);
  const completionValues = items.map(item=>item.completion);
  const labels = items.map(item=>`<span class="staffing-trend-label ${item.isCurrent ? "is-current" : ""}" title="${htmlesc(compactTimeFirst(item.createdAt || "") || item.label)}">${htmlesc(item.label)}</span>`).join("");

  return `
    <div class="control-grid staffing-trend-grid">
      <div class="item analytics-block staffing-trend-card">
        <div class="row"><div class="name">Тренд по Списку</div></div>
        <div class="staffing-trend-value mono">${fmtNum(factValues[factValues.length - 1])}</div>
        ${buildSparklineSvg(factValues, "#4e7ef1")}
        <div class="staffing-trend-labels">${labels}</div>
      </div>
      <div class="item analytics-block staffing-trend-card">
        <div class="row"><div class="name">Тренд по % укомплектованості</div></div>
        <div class="staffing-trend-value mono">${fmtNum(completionValues[completionValues.length - 1])}%</div>
        ${buildSparklineSvg(completionValues, "#49b277")}
        <div class="staffing-trend-labels">${labels}</div>
      </div>
    </div>
  `;

}

function buildStaffingAutoSummaryHtml(currentAnalytics, previousAnalytics=null){

  if(!currentAnalytics) return "";

  const topShortageItem = (currentAnalytics.topShortage || [])[0] || null;
  const bestFilledItem = (currentAnalytics.bestFilled || [])[0] || null;
  const topShortageGroup = topShortageItem
    ? (currentAnalytics.items || []).filter(item=>Number(item.shortage || 0) === Number(topShortageItem.shortage || 0))
    : [];
  const bestFilledGroup = bestFilledItem
    ? (currentAnalytics.items || []).filter(item=>Number(item.percent || 0) === Number(bestFilledItem.percent || 0))
    : [];
  const criticalZero = (currentAnalytics.items || []).filter(item=>Number(item.percent || 0) <= 0 && Number(item.plan || 0) > 0).length;

  const formatCompactUnitGroup = (items, limit=2)=>{
    const list = Array.isArray(items) ? items.map(item=>String(item.name || "").trim()).filter(Boolean) : [];
    if(!list.length) return "";
    const head = list.slice(0, limit).join(", ");
    return list.length > limit ? `${head} +${list.length - limit}` : head;
  };

  let dynamicsText = "Суттєвих змін щодо попередньої версії поки не зафіксовано.";
  if(previousAnalytics){
    const factDelta = currentAnalytics.totalFact - previousAnalytics.totalFact;
    const shortageDelta = currentAnalytics.totalShortage - previousAnalytics.totalShortage;

    if(factDelta > 0 && shortageDelta < 0){
      dynamicsText = `Список зріс на ${fmtNum(factDelta)}, а нестача зменшилась на ${fmtNum(Math.abs(shortageDelta))}.`;
    } else if(factDelta > 0){
      dynamicsText = `Список зріс на ${fmtNum(factDelta)} відносно попередньої версії.`;
    } else if(factDelta < 0){
      dynamicsText = `Список зменшився на ${fmtNum(Math.abs(factDelta))} відносно попередньої версії.`;
    } else if(shortageDelta < 0){
      dynamicsText = `Нестача зменшилась на ${fmtNum(Math.abs(shortageDelta))}.`;
    } else if(shortageDelta > 0){
      dynamicsText = `Нестача зросла на ${fmtNum(shortageDelta)}.`;
    }
  }

  const cards = [
    {
      label: "Загальний стан",
      value: `${fmtNum(currentAnalytics.completion)}%`,
      text: `Некомплект становить ${fmtNum(Math.max(0, 100 - Number(currentAnalytics.completion || 0)))}%.`,
    },
    {
      label: "Найбільша проблема",
      value: topShortageItem
        ? (topShortageGroup.length > 1
          ? `${fmtNum(topShortageItem.shortage)} у ${fmtNum(topShortageGroup.length)} підрозділів`
          : htmlesc(topShortageItem.name))
        : "—",
      text: topShortageItem
        ? (topShortageGroup.length > 1
          ? `Найбільша нестача повторюється у: ${htmlesc(formatCompactUnitGroup(topShortageGroup))}.`
          : `Найбільший некомплект: ${fmtNum(topShortageItem.shortage)}.`)
        : "Некомплекту в таблиці не виявлено.",
    },
    {
      label: "Найкращий показник",
      value: bestFilledItem
        ? (bestFilledGroup.length > 1
          ? `${fmtNum(bestFilledItem.percent)}% у ${fmtNum(bestFilledGroup.length)} підрозділів`
          : htmlesc(bestFilledItem.name))
        : "—",
      text: bestFilledItem
        ? (bestFilledGroup.length > 1
          ? `Однаковий найкращий рівень мають: ${htmlesc(formatCompactUnitGroup(bestFilledGroup))}.`
          : `Укомплектованість ${fmtNum(bestFilledItem.percent)}% при факті ${fmtNum(bestFilledItem.fact)}.`)
        : "Даних для рейтингу поки недостатньо.",
    },
    {
      label: "Динаміка / ризик",
      value: criticalZero ? `${fmtNum(criticalZero)}` : "0",
      text: previousAnalytics
        ? dynamicsText
        : (criticalZero ? `У критичній зоні ${fmtNum(criticalZero)} підрозділів із 0% укомплектованості.` : "Критичних 0% підрозділів наразі не зафіксовано."),
    },
  ];

  return `
    <div class="staffing-summary-grid">
      ${cards.map(card=>`
        <div class="staffing-summary-card">
          <div class="staffing-summary-k">${card.label}</div>
          <div class="staffing-summary-v">${card.value}</div>
          <div class="staffing-summary-s">${card.text}</div>
        </div>
      `).join("")}
    </div>
  `;

}

function registerStaffingAnalyticsModalSet(currentRows, title, versions=[], currentAt=""){

  const modalTitle = `Аналітика: ${title || "Таблиця"}`;
  const normalizedVersions = Array.isArray(versions)
    ? versions
      .filter(item=>item && Array.isArray(item.rows) && item.rows.length)
      .map(item=>({
        id: String(item.id || uid("ref_ver")),
        createdAt: typeof item.createdAt === "string" ? item.createdAt : "",
        rows: cloneStoredTableRows(item.rows),
      }))
    : [];

  const primaryKey = registerRenderedTableModal(modalTitle, "");

  if(!normalizedVersions.length){
    UI.renderedTableModals[primaryKey].bodyHtml = buildStaffingAnalyticsModalHtml(currentRows, title, {
      compareOptions:[],
      selectedVersionId:"",
      trendPoints: buildStaffingTrendPoints(currentRows, [], currentAt),
    });
    return primaryKey;
  }

  const versionDefs = normalizedVersions.map(version=>({
    ...version,
    key: registerRenderedTableModal(modalTitle, ""),
  }));

  const compareOptions = versionDefs.map((item, index)=>({
    id: item.id,
    key: item.key,
    primaryLabel: index === 0 ? "Попередня" : `${index + 1} версії тому`,
    secondaryLabel: compactTimeFirst(item.createdAt || ""),
  }));
  const trendPoints = buildStaffingTrendPoints(currentRows, normalizedVersions, currentAt);

  UI.renderedTableModals[primaryKey].bodyHtml = buildStaffingAnalyticsModalHtml(currentRows, title, {
    compareOptions,
    selectedVersionId: versionDefs[0]?.id || "",
    compareRows: versionDefs[0]?.rows || [],
    compareAt: versionDefs[0]?.createdAt || "",
    trendPoints,
  });

  versionDefs.forEach(item=>{
    UI.renderedTableModals[item.key].bodyHtml = buildStaffingAnalyticsModalHtml(currentRows, title, {
      compareOptions,
      selectedVersionId: item.id,
      compareRows: item.rows,
      compareAt: item.createdAt,
      trendPoints,
    });
  });

  return primaryKey;

}

function parseAnalyticsNumber(value){

  const raw = String(value ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, "")
    .replace(/,/g, ".");

  const match = raw.match(/-?\d+(?:\.\d+)?/);
  if(!match) return null;

  const num = Number(match[0]);
  return Number.isFinite(num) ? num : null;

}

function normalizeAnalyticsHeader(value){

  return String(value || "")
    .toLowerCase()
    .replace(/\u00A0/g, " ")
    .replace(/[^\p{L}\p{N}%]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();

}

function isAnalyticsSummaryLabel(value){

  const label = normalizeAnalyticsHeader(value);
  return /^(всього|усього|разом|итого|всего|підсумок|підсумки|загалом)$/.test(label);

}

function fmtNum(value){

  const num = Number(value);
  if(!Number.isFinite(num)) return "0";

  const isInteger = Math.abs(num % 1) < 0.000001;

  return num.toLocaleString("uk-UA", {
    minimumFractionDigits: 0,
    maximumFractionDigits: isInteger ? 0 : 2,
  });

}

function fmtCompactMoneyUa(value){

  const num = Number(value);
  if(!Number.isFinite(num)) return "0";

  const abs = Math.abs(num);

  if(abs >= 1000000){
    return `${(num / 1000000).toLocaleString("uk-UA", {minimumFractionDigits: 0, maximumFractionDigits: 2})} млн`;
  }

  if(abs >= 100000){
    return `${Math.round(num / 1000).toLocaleString("uk-UA")} тис.`;
  }

  if(abs >= 10000){
    return `${(num / 1000).toLocaleString("uk-UA", {minimumFractionDigits: 0, maximumFractionDigits: 1})} тис.`;
  }

  return fmtNum(num);

}

function detectStaffingColumns(headerRow){

  const headers = (headerRow || []).map(normalizeAnalyticsHeader);
  const result = {dept:-1, plan:-1, fact:-1, shortage:-1, percent:-1, total:-1, modelIndexes:[]};

  headers.forEach((header, idx)=>{

    if(result.dept < 0 && /(орган дпсу|орган|підрозділ|відділ|загін|рота|баталь|взвод|екіпаж|підгруп|назва|unit)/.test(header)){
      result.dept = idx;
      return;
    }

    if(result.shortage < 0 && /(некомплект|нестач|браку|дефіцит|відсут)/.test(header)){
      result.shortage = idx;
      return;
    }

    if(result.percent < 0 && /(%|відсот|укомплектованіст)/.test(header)){
      result.percent = idx;
      return;
    }

    if(result.plan < 0 && /(штат|план|потреб|належ|потрібно)/.test(header)){
      result.plan = idx;
      return;
    }

    if(result.fact < 0 && /(список|факт|наяв|фактич|є |є$|у наявності|укомплектовано|наявність)/.test(header)){
      result.fact = idx;
      return;
    }

    if(result.total < 0 && /(всього|усього|итого|разом|сумарно|всего)/.test(header)){
      result.total = idx;
    }

  });

  if(result.dept < 0) result.dept = 0;

  headers.forEach((header, idx)=>{
    if(idx===result.dept || idx===result.plan || idx===result.fact || idx===result.shortage || idx===result.percent || idx===result.total) return;
    if(!header) return;
    if(/^(номер|№|no|n)$/.test(header)) return;
    result.modelIndexes.push(idx);
  });

  return result;

}

function buildStaffingAnalytics(rows){

  const grid = Array.isArray(rows) ? rows : [];
  if(grid.length < 2) return null;

  const columns = detectStaffingColumns(grid[0]);
  const modelTotals = new Map();
  const items = grid.slice(1).map((row, index)=>{
    const name = String(row?.[columns.dept] || "").trim() || `Рядок ${index + 1}`;
    if(isAnalyticsSummaryLabel(name)) return null;

    let plan = columns.plan >= 0 ? parseAnalyticsNumber(row?.[columns.plan]) : null;
    let fact = columns.fact >= 0 ? parseAnalyticsNumber(row?.[columns.fact]) : null;
    let shortage = columns.shortage >= 0 ? parseAnalyticsNumber(row?.[columns.shortage]) : null;
    let percent = columns.percent >= 0 ? parseAnalyticsNumber(row?.[columns.percent]) : null;
    let total = columns.total >= 0 ? parseAnalyticsNumber(row?.[columns.total]) : null;

    const modelBreakdown = columns.modelIndexes.map(colIdx=>{
      const label = String(grid[0]?.[colIdx] || "").trim();
      const value = parseAnalyticsNumber(row?.[colIdx]);
      return {
        label,
        value: Number.isFinite(value) ? Number(value) : 0,
      };
    }).filter(item=>item.label);

    modelBreakdown.forEach(item=>{
      const prev = modelTotals.get(item.label) || 0;
      modelTotals.set(item.label, prev + item.value);
    });

    if(shortage == null && plan != null && fact != null) shortage = Math.max(plan - fact, 0);
    if(fact == null && plan != null && shortage != null) fact = Math.max(plan - shortage, 0);
    if(plan == null && fact != null && shortage != null) plan = fact + shortage;
    if(fact == null && total != null) fact = total;
    if(total == null && fact != null) total = fact;
    if(percent == null && plan && fact != null) percent = Math.round((fact / plan) * 100);

    const hasData = [plan, fact, shortage, percent, total].some(v=>Number.isFinite(v)) || modelBreakdown.some(item=>item.value > 0);
    if(!hasData) return null;

    return {
      name,
      plan: Number.isFinite(plan) ? Number(plan) : 0,
      fact: Number.isFinite(fact) ? Number(fact) : 0,
      total: Number.isFinite(total) ? Number(total) : 0,
      shortage: Number.isFinite(shortage) ? Math.max(Number(shortage), 0) : 0,
      percent: Number.isFinite(percent) ? Number(percent) : 0,
      modelBreakdown,
    };
  }).filter(Boolean);

  if(!items.length) return null;

  const totalPlan = items.reduce((sum, item)=>sum + item.plan, 0);
  const totalFact = items.reduce((sum, item)=>sum + item.fact, 0);
  const totalAssets = items.reduce((sum, item)=>sum + item.total, 0);
  const totalShortage = items.reduce((sum, item)=>sum + item.shortage, 0);
  const completion = totalPlan > 0 ? Math.round((totalFact / totalPlan) * 100) : 0;
  const topShortage = items.slice().sort((a,b)=>b.shortage-a.shortage).slice(0, 8);
  const bestFilled = items.slice().sort((a,b)=>(b.percent || 0)-(a.percent || 0)).slice(0, 5);
  const topModels = Array.from(modelTotals.entries())
    .map(([name, value])=>({name, value:Number(value || 0)}))
    .filter(item=>item.value > 0)
    .sort((a,b)=>b.value-a.value)
    .slice(0, 10);
  const donut = buildEvalSlices([
    {label:"Укомплектовано", value: totalFact},
    {label:"Нестача", value: totalShortage},
  ], ["#5f8ef5", "#ffcc66"]);
  const shortageDonut = buildEvalSlices(
    topShortage.filter(item=>item.shortage > 0).slice(0, 5).map(item=>({label:item.name, value:item.shortage})),
    ["#ff9f43", "#ff6b8b", "#5f8ef5", "#6fbf73", "#b783ff"]
  );

  return {
    columns,
    items,
    totalPlan,
    totalFact,
    totalAssets,
    totalShortage,
    completion,
    topShortage,
    bestFilled,
    topModels,
    donut,
    shortageDonut,
  };

}

function buildStaffingAnalyticsModalHtml(rows, title="", opts={}){

  const analytics = buildStaffingAnalytics(rows);

  if(!analytics){
    return `
      <div class="hint">Не вдалося розпізнати таблицю укомплектованості. Очікуються колонки на кшталт: підрозділ, план/штат, факт/наявні, некомплект.</div>
    `;
  }

  const {items, totalPlan, totalFact, totalAssets, totalShortage, completion, topShortage, bestFilled, topModels, donut, shortageDonut} = analytics;
  const shortagePercent = Math.max(0, 100 - Number(completion || 0));

  const summaryGrid = `
      <div class="report-grid staffing-analytics-kpis">
        <div class="report-tile"><div class="k">План</div><div class="v mono">${fmtNum(totalPlan)}</div><div class="s">&nbsp;</div></div>
        <div class="report-tile"><div class="k">Факт</div><div class="v mono">${fmtNum(totalFact)}</div><div class="s">${fmtNum(completion)}%</div></div>
        <div class="report-tile"><div class="k">Нестача</div><div class="v mono">${fmtNum(totalShortage)}</div><div class="s">${fmtNum(shortagePercent)}%</div></div>
        <div class="report-tile"><div class="k">Всього засобів</div><div class="v mono">${fmtNum(totalAssets)}</div><div class="s">&nbsp;</div></div>
      </div>
  `;

  const totalDonutCard = `
    <div class="item analytics-block eval-donut-card">
      <div class="row"><div class="name">Загальна укомплектованість</div></div>
      <div class="eval-donut-wrap">
        <div class="eval-donut is-animated-donut" data-donut-gradient="${htmlesc(donut.gradient)}" style="background:conic-gradient(#dfe6f6 0 360deg);"></div>
        <div>
          ${donut.legendRows.map(row=>`<div class="eval-legend-item"><span class="eval-legend-dot" style="background:${row.color}"></span><span>${htmlesc(row.label)}</span><b class="mono">${fmtNum(row.value)}</b><span class="mono">${row.percent}%</span></div>`).join("")}
        </div>
      </div>
    </div>
  `;

  const shortageDonutCard = `
    <div class="item analytics-block eval-donut-card">
      <div class="row"><div class="name">Найбільший некомплект</div></div>
      <div class="eval-donut-wrap">
        <div class="eval-donut is-animated-donut" data-donut-gradient="${htmlesc(shortageDonut.gradient)}" style="background:conic-gradient(#dfe6f6 0 360deg);"></div>
        <div>
          ${shortageDonut.legendRows.length
            ? shortageDonut.legendRows.map(row=>`<div class="eval-legend-item"><span class="eval-legend-dot" style="background:${row.color}"></span><span>${htmlesc(row.label)}</span><b class="mono">${fmtNum(row.value)}</b><span class="mono">${row.percent}%</span></div>`).join("")
            : `<div class="hint">Некомплект не виявлено.</div>`
          }
        </div>
      </div>
    </div>
  `;

  const unitsBlock = renderStaffingUnitsCombinedBlock("Підрозділи", items, "shortage");

  const modelsList = `
      <div class="item analytics-block staffing-analytics-list">
        <div class="row"><div class="name">Найпоширеніші моделі / позиції</div></div>
        <div class="staffing-model-grid">
          ${topModels.length
            ? topModels.map(item=>`
                <div class="staffing-model-card">
                  <div class="staffing-model-name">${htmlesc(item.name)}</div>
                  <div class="staffing-model-value mono">${fmtNum(item.value)}</div>
                </div>
              `).join("")
            : `<div class="hint">По моделях поки немає заповнених даних.</div>`
          }
        </div>
      </div>
    `;

  const compareOptions = Array.isArray(opts.compareOptions) ? opts.compareOptions : [];
  const selectedVersionId = String(opts.selectedVersionId || "");
  const compareRows = Array.isArray(opts.compareRows) ? opts.compareRows : [];
  const compareAt = String(opts.compareAt || "");
  const trendPoints = Array.isArray(opts.trendPoints) ? opts.trendPoints : [];
  const previousAnalytics = compareRows.length ? buildStaffingAnalytics(compareRows) : null;
  const compareSelector = compareOptions.length ? `
    <div class="item analytics-block staffing-dynamics-picker">
      <div class="row"><div class="name">Динаміка</div></div>
      <div class="hint staffing-dynamics-picker-note">Порівняй поточну таблицю з однією з попередніх версій.</div>
      <div class="comparison-switcher-buttons staffing-dynamics-buttons">
        ${compareOptions.map(item=>`
          <button
            type="button"
            class="comparison-switcher-btn ${item.id===selectedVersionId ? "is-active" : ""}"
            data-action="openRenderedTableModal"
            data-arg1="${item.key}"
          >
            <span class="staffing-dynamics-btn-main">${htmlesc(item.primaryLabel || "Версія")}</span>
            <span class="staffing-dynamics-btn-sub mono">${htmlesc(item.secondaryLabel || "")}</span>
          </button>
        `).join("")}
      </div>
    </div>
  ` : "";

  const dynamicsBlock = compareRows.length
    ? buildStaffingDynamicsModalHtml(rows, compareRows, title, compareAt)
    : "";

  return `
    <div class="staffing-analytics-modal">
      ${summaryGrid}
      ${buildStaffingAutoSummaryHtml({items, totalPlan, totalFact, totalAssets, totalShortage, completion, topShortage, bestFilled}, previousAnalytics)}
      ${buildStaffingUnitChartsHtml(items)}
      ${buildStaffingTrendBlockHtml(trendPoints)}
      <div class="eval-donut-grid">
        ${totalDonutCard}
        ${shortageDonutCard}
      </div>
      ${unitsBlock}
      ${modelsList}
      ${compareSelector}
      ${dynamicsBlock}
    </div>
  `;

}

function renderStaffingDynamicsDelta(delta, opts={}){

  const num = Number(delta);
  if(!Number.isFinite(num) || Math.abs(num) < 0.000001){
    return `<span class="badge mono">0</span>`;
  }

  const invert = !!opts.invert;
  const isBetter = invert ? num < 0 : num > 0;
  const cls = isBetter ? "badge b-ok mono" : "badge b-warn mono";
  const prefix = num > 0 ? "+" : "−";
  const suffix = opts.suffix ? ` ${opts.suffix}` : "";

  return `<span class="${cls}">${prefix}${fmtNum(Math.abs(num))}${suffix}</span>`;

}

function renderStaffingUnitList(items, sortKey="shortage", panelId=""){

  const list = Array.isArray(items) ? items.slice() : [];
  const key = String(sortKey || "shortage");
  const visibleLimit = 6;

  list.sort((a,b)=>{
    if(key === "plan") return (b.plan - a.plan) || String(a.name).localeCompare(String(b.name), "uk");
    if(key === "fact") return (b.fact - a.fact) || String(a.name).localeCompare(String(b.name), "uk");
    if(key === "percent") return (b.percent - a.percent) || (a.shortage - b.shortage) || String(a.name).localeCompare(String(b.name), "uk");
    return (b.shortage - a.shortage) || (a.percent - b.percent) || String(a.name).localeCompare(String(b.name), "uk");
  });

  return `
    <div class="staffing-unit-list-wrap ${list.length <= visibleLimit ? "is-short" : ""}" data-staffing-list="${panelId}">
      <ul class="report-list staffing-unit-list">
        ${list.length
        ? list.map((item, index)=>`
            <li>
              <div class="staffing-unit-card" data-staffing-rank="${index}" data-staffing-plan="${fmtNum(item.plan)}" data-staffing-plan-value="${Number(item.plan || 0)}">
                <div class="staffing-unit-card-head">
                  <span class="report-strong staffing-unit-name">${htmlesc(item.name)}</span>
                </div>
                <div class="staffing-unit-metrics">
                  <div class="staffing-unit-metric">
                    <div class="staffing-unit-metric-k">План</div>
                    <div class="staffing-unit-metric-v mono">${fmtNum(item.plan)}</div>
                  </div>
                  <div class="staffing-unit-metric is-blue">
                    <div class="staffing-unit-metric-k">Факт</div>
                    <div class="staffing-unit-metric-v mono">${fmtNum(item.fact)}</div>
                  </div>
                  <div class="staffing-unit-metric is-warn">
                    <div class="staffing-unit-metric-k">Нестача</div>
                    <div class="staffing-unit-metric-v mono">${fmtNum(item.shortage)}</div>
                  </div>
                  <div class="staffing-unit-metric">
                    <div class="staffing-unit-metric-k">%</div>
                    <div class="staffing-unit-metric-v mono">${fmtNum(item.percent)}%</div>
                  </div>
                </div>
                <div class="staffing-unit-progress-view">
                  <div class="staffing-unit-progress-meta">
                    <span><b>План</b> ${fmtNum(item.plan)}</span>
                    <span><b>Факт</b> ${fmtNum(item.fact)}</span>
                    <span><b>Нестача</b> ${fmtNum(item.shortage)}</span>
                    <span><b>%</b> ${fmtNum(item.percent)}%</span>
                  </div>
                  <div class="staffing-unit-progress-track">
                    <span class="staffing-unit-progress-segment is-fact" style="width:${Math.max(0, Math.min(100, Number(item.percent || 0)))}%"></span>
                    <span class="staffing-unit-progress-segment is-gap" style="width:${Math.max(0, Math.min(100, 100 - Number(item.percent || 0)))}%"></span>
                  </div>
                </div>
              </div>
            </li>
          `).join("")
        : `<li><div class="hint">Дані по підрозділах поки відсутні.</div></li>`
        }
      </ul>
      ${list.length > visibleLimit ? `
        <div class="staffing-unit-list-actions">
          <button
            type="button"
            class="btn ghost btn-mini staffing-unit-toggle"
            data-action="toggleStaffingUnitsExpand"
            data-arg1="${panelId}"
          >Показати всі (${list.length})</button>
        </div>
      ` : ``}
    </div>
  `;

}

function buildStaffingUnitsSearchIndex(items){

  const data = Array.isArray(items) ? items : [];
  return data.map(item=>({
    ...item,
    searchText: String(item.name || "").toLowerCase(),
  }));

}

function renderStaffingUnitsCombinedBlock(title, items, defaultKey="shortage"){

  const buttons = [
    {key:"shortage", label:"Нестача"},
    {key:"percent", label:"%"},
    {key:"fact", label:"Факт"},
    {key:"plan", label:"План"},
  ];

  const groupId = `stf_sort_${Math.random().toString(36).slice(2, 8)}`;
  const searchId = `stf_search_${Math.random().toString(36).slice(2, 8)}`;
  const indexedItems = buildStaffingUnitsSearchIndex(items);

  return `
    <div class="item analytics-block comparison-switch-block staffing-units-block">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="comparison-switcher-buttons staffing-scope-buttons" data-staffing-scope-group="${groupId}">
        <button
          type="button"
          class="comparison-switcher-btn is-active"
          data-action="setStaffingUnitsScope"
          data-arg1="${groupId}"
          data-arg2="plan"
        >Тільки з планом</button>
        <button
          type="button"
          class="comparison-switcher-btn"
          data-action="setStaffingUnitsScope"
          data-arg1="${groupId}"
          data-arg2="all"
        >Усі</button>
      </div>
      <div class="field staffing-units-search">
        <input
          id="${searchId}"
          type="search"
          class="staffing-units-search-input"
          placeholder="Пошук загону / органу..."
          data-action="filterStaffingUnitsBlock"
          data-arg1="${groupId}"
        />
      </div>
      <div class="comparison-switcher-buttons staffing-view-buttons" data-staffing-view-group="${groupId}">
        <button
          type="button"
          class="comparison-switcher-btn is-active"
          data-action="setStaffingUnitsViewMode"
          data-arg1="${groupId}"
          data-arg2="cards"
        >Картки</button>
        <button
          type="button"
          class="comparison-switcher-btn"
          data-action="setStaffingUnitsViewMode"
          data-arg1="${groupId}"
          data-arg2="progress"
        >Прогрес</button>
      </div>
      <div class="comparison-switcher" data-topswitch-group="${groupId}" data-staffing-scope="plan" data-staffing-view-mode="cards">
        <div class="comparison-switcher-buttons staffing-sort-buttons">
          ${buttons.map(btn=>`
            <button
              type="button"
              class="comparison-switcher-btn ${btn.key===defaultKey ? "is-active" : ""}"
              data-action="switchComparisonTopPanel"
              data-arg1="${groupId}"
              data-arg2="${btn.key}"
            >${htmlesc(btn.label)}</button>
          `).join("")}
        </div>
        <div class="comparison-switch-panels">
          ${buttons.map(btn=>`
            <div class="comparison-switch-panel ${btn.key===defaultKey ? "is-active" : ""}" data-topswitch-panel="${groupId}:${btn.key}">
              <div class="staffing-units-panel-body" data-staffing-sort="${btn.key}" data-staffing-filter-group="${groupId}" data-staffing-panel="${groupId}:${btn.key}">
                ${renderStaffingUnitList(indexedItems, btn.key, `${groupId}:${btn.key}`)}
              </div>
            </div>
          `).join("")}
        </div>
      </div>
    </div>
  `;

}

function buildStaffingDynamicsModalHtml(currentRows, previousRows, title="", previousAt=""){

  const current = buildStaffingAnalytics(currentRows);
  const previous = buildStaffingAnalytics(previousRows);

  if(!current || !previous){
    return `
      <div class="hint">Для динаміки потрібні поточна і попередня версії таблиці укомплектованості.</div>
    `;
  }

  const currentByName = new Map(current.items.map(item=>[item.name, item]));
  const previousByName = new Map(previous.items.map(item=>[item.name, item]));
  const unitNames = Array.from(new Set([...currentByName.keys(), ...previousByName.keys()]));

  const unitChanges = unitNames.map(name=>{
    const prev = previousByName.get(name) || {plan:0, fact:0, shortage:0, percent:0};
    const curr = currentByName.get(name) || {plan:0, fact:0, shortage:0, percent:0};
    return {
      name,
      prev,
      curr,
      deltaFact: curr.fact - prev.fact,
      deltaShortage: curr.shortage - prev.shortage,
      deltaPercent: curr.percent - prev.percent,
    };
  });

  const topImproved = unitChanges
    .filter(item=>item.deltaFact > 0 || item.deltaShortage < 0 || item.deltaPercent > 0)
    .sort((a,b)=>
      (b.deltaFact - a.deltaFact) ||
      (a.deltaShortage - b.deltaShortage) ||
      (b.deltaPercent - a.deltaPercent)
    )
    .slice(0, 6);

  const topDeclined = unitChanges
    .filter(item=>item.deltaFact < 0 || item.deltaShortage > 0 || item.deltaPercent < 0)
    .sort((a,b)=>
      (a.deltaFact - b.deltaFact) ||
      (b.deltaShortage - a.deltaShortage) ||
      (a.deltaPercent - b.deltaPercent)
    )
    .slice(0, 6);

  const buildModelTotals = analytics=>{
    const totals = new Map();
    analytics.items.forEach(item=>{
      (item.modelBreakdown || []).forEach(model=>{
        const prev = totals.get(model.label) || 0;
        totals.set(model.label, prev + Number(model.value || 0));
      });
    });
    return totals;
  };

  const currentModelTotals = buildModelTotals(current);
  const previousModelTotals = buildModelTotals(previous);
  const modelNames = Array.from(new Set([...currentModelTotals.keys(), ...previousModelTotals.keys()]));
  const modelChanges = modelNames.map(name=>{
    const prev = Number(previousModelTotals.get(name) || 0);
    const curr = Number(currentModelTotals.get(name) || 0);
    return {
      name,
      prev,
      curr,
      delta: curr - prev,
    };
  }).filter(item=>item.prev || item.curr);

  const topAddedModels = modelChanges
    .filter(item=>item.delta > 0)
    .sort((a,b)=>b.delta - a.delta)
    .slice(0, 8);

  const topReducedModels = modelChanges
    .filter(item=>item.delta < 0)
    .sort((a,b)=>a.delta - b.delta)
    .slice(0, 8);

  const summaryGrid = `
    <div class="report-grid staffing-analytics-kpis staffing-dynamics-kpis">
      <div class="report-tile staffing-dynamics-tile">
        <div class="k">Штат</div>
        <div class="v mono">${fmtNum(current.totalPlan)}</div>
        <div class="s">Було ${fmtNum(previous.totalPlan)}</div>
        <div class="staffing-dynamics-delta">${renderStaffingDynamicsDelta(current.totalPlan - previous.totalPlan)}</div>
      </div>
      <div class="report-tile staffing-dynamics-tile">
        <div class="k">Список</div>
        <div class="v mono">${fmtNum(current.totalFact)}</div>
        <div class="s">Було ${fmtNum(previous.totalFact)}</div>
        <div class="staffing-dynamics-delta">${renderStaffingDynamicsDelta(current.totalFact - previous.totalFact)}</div>
      </div>
      <div class="report-tile staffing-dynamics-tile">
        <div class="k">Нестача</div>
        <div class="v mono">${fmtNum(current.totalShortage)}</div>
        <div class="s">Було ${fmtNum(previous.totalShortage)}</div>
        <div class="staffing-dynamics-delta">${renderStaffingDynamicsDelta(current.totalShortage - previous.totalShortage, {invert:true})}</div>
      </div>
      <div class="report-tile staffing-dynamics-tile">
        <div class="k">% укомплектованості</div>
        <div class="v mono">${fmtNum(current.completion)}%</div>
        <div class="s">Було ${fmtNum(previous.completion)}%</div>
        <div class="staffing-dynamics-delta">${renderStaffingDynamicsDelta(current.completion - previous.completion, {suffix:"%"})}</div>
      </div>
      <div class="report-tile staffing-dynamics-tile">
        <div class="k">Всього засобів</div>
        <div class="v mono">${fmtNum(current.totalAssets)}</div>
        <div class="s">Було ${fmtNum(previous.totalAssets)}</div>
        <div class="staffing-dynamics-delta">${renderStaffingDynamicsDelta(current.totalAssets - previous.totalAssets)}</div>
      </div>
    </div>
  `;

  const improvedList = `
    <div class="item analytics-block staffing-analytics-list">
      <div class="row"><div class="name">Де стало краще</div></div>
      <ul class="report-list">
        ${topImproved.length
          ? topImproved.map(item=>`
              <li>
                <div class="report-line">
                  <span class="report-strong">${htmlesc(item.name)}</span>
                  ${renderStaffingDynamicsDelta(item.deltaFact)}
                  ${renderStaffingDynamicsDelta(item.deltaShortage, {invert:true})}
                </div>
                <div class="report-meta">Список: ${fmtNum(item.prev.fact)} → ${fmtNum(item.curr.fact)} · Нестача: ${fmtNum(item.prev.shortage)} → ${fmtNum(item.curr.shortage)} · %: ${fmtNum(item.prev.percent)} → ${fmtNum(item.curr.percent)}</div>
              </li>
            `).join("")
          : `<li><div class="hint">Суттєвих покращень між версіями не видно.</div></li>`
        }
      </ul>
    </div>
  `;

  const declinedList = `
    <div class="item analytics-block staffing-analytics-list">
      <div class="row"><div class="name">Де стало гірше</div></div>
      <ul class="report-list">
        ${topDeclined.length
          ? topDeclined.map(item=>`
              <li>
                <div class="report-line">
                  <span class="report-strong">${htmlesc(item.name)}</span>
                  ${renderStaffingDynamicsDelta(item.deltaFact)}
                  ${renderStaffingDynamicsDelta(item.deltaShortage, {invert:true})}
                </div>
                <div class="report-meta">Список: ${fmtNum(item.prev.fact)} → ${fmtNum(item.curr.fact)} · Нестача: ${fmtNum(item.prev.shortage)} → ${fmtNum(item.curr.shortage)} · %: ${fmtNum(item.prev.percent)} → ${fmtNum(item.curr.percent)}</div>
              </li>
            `).join("")
          : `<li><div class="hint">Погіршень між версіями не зафіксовано.</div></li>`
        }
      </ul>
    </div>
  `;

  const modelLists = `
    <div class="control-grid staffing-analytics-sections staffing-dynamics-models">
      <div class="item analytics-block staffing-analytics-list">
        <div class="row"><div class="name">По моделях додалось</div></div>
        <div class="staffing-model-grid">
          ${topAddedModels.length
            ? topAddedModels.map(item=>`
                <div class="staffing-model-card staffing-model-card-delta is-up">
                  <div>
                    <div class="staffing-model-name">${htmlesc(item.name)}</div>
                    <div class="report-meta">Було ${fmtNum(item.prev)} → стало ${fmtNum(item.curr)}</div>
                  </div>
                  <div class="staffing-model-value mono">+${fmtNum(item.delta)}</div>
                </div>
              `).join("")
            : `<div class="hint">Нових приростів по моделях не видно.</div>`
          }
        </div>
      </div>
      <div class="item analytics-block staffing-analytics-list">
        <div class="row"><div class="name">По моделях зменшилось</div></div>
        <div class="staffing-model-grid">
          ${topReducedModels.length
            ? topReducedModels.map(item=>`
                <div class="staffing-model-card staffing-model-card-delta is-down">
                  <div>
                    <div class="staffing-model-name">${htmlesc(item.name)}</div>
                    <div class="report-meta">Було ${fmtNum(item.prev)} → стало ${fmtNum(item.curr)}</div>
                  </div>
                  <div class="staffing-model-value mono">−${fmtNum(Math.abs(item.delta))}</div>
                </div>
              `).join("")
            : `<div class="hint">Зменшень по моделях не зафіксовано.</div>`
          }
        </div>
      </div>
    </div>
  `;

  return `
    <div class="staffing-analytics-modal staffing-dynamics-modal">
      <div class="hint staffing-dynamics-note">${previousAt ? `Порівняння з версією від ${htmlesc(compactTimeFirst(previousAt))}.` : "Порівняння з попередньою версією."}</div>
      ${summaryGrid}
      <div class="control-grid staffing-analytics-sections">
        ${improvedList}
        ${declinedList}
      </div>
      ${modelLists}
    </div>
  `;

}

const COMPARISON_SUBTYPE_KEYWORDS = {
  fpv: /(fpv|камікадзе|коптер|оптоволокон|ов\b)/,
  interceptor: /(перехоп|інтерцеп|intercept|зенітн|шахед)/,
  logistics: /(логіст|логст|транспорт|вантаж)/,
  fixedWing: /(літак|літаков|крил|fixed wing|катапульт|парашут|vtol)/,
  multirotor: /(мультиротор|\bмр\b|бомбер|гексакоп|квадрокоп|октокоп|скид)/,
  recon: /(розвід|спостереж)/,
  strike: /(удар|бомбер|камікадзе)/,
};

const COMPARISON_HEADER_ALIASES = {
  vendor: [/виробник/, /компан/, /постачальник/, /бренд/],
  name: [/найменування бпак/, /найменування/, /назва виробу/, /назва бпак/, /назва моделі/, /назва/, /модель/, /виріб/],
  systemPrice: [/орієнтовна вартість бпак/, /орієнтовна ціна за бпак/, /орієнтовна ціна бпак/, /орієнтовна ціна/, /вартість бпак/, /ціна за бпак/, /ціна бпак/, /вартість комплексу/, /ціна комплексу/],
  unitPrice: [/орієнтовна вартість бпла/, /орієнтовна ціна за бпла/, /орієнтовна ціна бпла/, /вартість бпла/, /ціна за бпла/, /ціна бпла/],
  quantity: [/кількість бпла в бпак/, /кількість бпла/, /кількість у комплексі/, /кількість апаратів/],
  payload: [/корисне навантаження/, /вага корисного навантаження/, /макс корисне навантаження/, /навантаження кг/, /вантаж/],
  distance: [/макс дальність передачі даних/, /макс дальність польоту з корисним навантаженням/, /макс дальність польоту/, /дальність польоту/, /макс дальність/, /дальність км/],
  radius: [/тактичний радіус/, /радіус дії/, /робочий радіус/],
  wind: [/допустима швидкість вітру/, /швидкість вітру/, /вітер/],
  speed: [/максимальна швидкість з робочим навантаженням/, /швидкість максимальна/, /максимальна швидкість/, /макс швидкість/, /крейсерська швидкість/, /швидкість/],
  flightTime: [/макс час польоту з робочим навантаженням/, /макс час польоту/, /час польоту/, /тривалість польоту/],
  height: [/макс висота польоту/, /висота польоту/, /стеля/],
  deployTime: [/час розгортання/, /розгортання згортання/, /розгортан/],
  cameraType: [/тип камери/, /камера/, /тепловіз/, /нічна/, /денна/],
  codified: [/кодифікація/, /кодифік/],
};

function detectComparisonSubtype(title="", headerRow=[]){

  const source = normalizeAnalyticsHeader(`${title || ""} ${(headerRow || []).join(" ")}`);

  if(COMPARISON_SUBTYPE_KEYWORDS.logistics.test(source)) return "logistics_multirotor";
  if(COMPARISON_SUBTYPE_KEYWORDS.interceptor.test(source)) return "interceptor";
  if(COMPARISON_SUBTYPE_KEYWORDS.fpv.test(source)) return "fpv";
  if(COMPARISON_SUBTYPE_KEYWORDS.fixedWing.test(source) && COMPARISON_SUBTYPE_KEYWORDS.recon.test(source)) return "fixed_wing_recon";
  if(COMPARISON_SUBTYPE_KEYWORDS.fixedWing.test(source) && COMPARISON_SUBTYPE_KEYWORDS.strike.test(source)) return "fixed_wing_strike";
  if(COMPARISON_SUBTYPE_KEYWORDS.fixedWing.test(source)) return "fixed_wing";
  if(COMPARISON_SUBTYPE_KEYWORDS.multirotor.test(source) && COMPARISON_SUBTYPE_KEYWORDS.recon.test(source)) return "multirotor_recon";
  if(COMPARISON_SUBTYPE_KEYWORDS.multirotor.test(source) && COMPARISON_SUBTYPE_KEYWORDS.strike.test(source)) return "multirotor_strike";
  if(COMPARISON_SUBTYPE_KEYWORDS.multirotor.test(source)) return "multirotor";

  return "generic";

}

function comparisonHeaderMatches(header, aliasKey){

  return (COMPARISON_HEADER_ALIASES[aliasKey] || []).some(pattern=>pattern.test(header));

}

function detectComparisonHeaderUnits(headerRow, columns){

  const headers = Array.isArray(headerRow) ? headerRow : [];
  const pick = (key)=>(columns?.[key] >= 0 ? normalizeAnalyticsHeader(headers[columns[key]]) : "");
  const heightHeader = pick("height");
  const flightTimeHeader = pick("flightTime");
  const distanceHeader = pick("distance");
  const radiusHeader = pick("radius");

  return {
    price: "грн",
    payload: "кг",
    speed: "км/год",
    flightTime: /год/.test(flightTimeHeader) ? "год" : "хв",
    height: /км/.test(heightHeader) ? "км" : "м",
    distance: /м\b/.test(distanceHeader) && !/км/.test(distanceHeader) ? "м" : "км",
    radius: /м\b/.test(radiusHeader) && !/км/.test(radiusHeader) ? "м" : "км",
    wind: "м/с",
    deployTime: "хв",
  };

}

function detectComparisonColumns(headerRow, title=""){

  const headers = (headerRow || []).map(normalizeAnalyticsHeader);
  const result = {
    name:-1,
    model:-1,
    vendor:-1,
    systemPrice:-1,
    unitPrice:-1,
    quantity:-1,
    payload:-1,
    speed:-1,
    radius:-1,
    distance:-1,
    flightTime:-1,
    height:-1,
    wind:-1,
    deployTime:-1,
    cameraType:-1,
    codified:-1,
    subtype: detectComparisonSubtype(title, headerRow),
    units: null,
  };

  headers.forEach((header, idx)=>{

    const hasWind = /вітр|вiтр/.test(header);
    const hasDeploy = /розгортан/.test(header);
    const hasFlightTime = /((час|тривалість).*(польот|польоту))|((польот|польоту).*(час|тривалість))/.test(header);
    const hasFlightHeight = /((висот|стеля).*(польот|польоту))|((польот|польоту).*(висот|стеля))/.test(header);
    const hasFlightDistance = /((дальн|радіус).*(польот|польоту))|((польот|польоту).*(дальн|радіус))/.test(header);

    if(result.vendor < 0 && comparisonHeaderMatches(header, "vendor")){
      result.vendor = idx;
      return;
    }

    if(result.name < 0 && comparisonHeaderMatches(header, "name")){
      result.name = idx;
      return;
    }

    if(result.model < 0 && comparisonHeaderMatches(header, "name")){
      result.model = idx;
      return;
    }

    if(result.systemPrice < 0 && comparisonHeaderMatches(header, "systemPrice") && !comparisonHeaderMatches(header, "unitPrice")){
      result.systemPrice = idx;
      return;
    }

    if(result.unitPrice < 0 && comparisonHeaderMatches(header, "unitPrice")){
      result.unitPrice = idx;
      return;
    }

    if(result.quantity < 0 && comparisonHeaderMatches(header, "quantity")){
      result.quantity = idx;
      return;
    }

    if(result.payload < 0 && comparisonHeaderMatches(header, "payload")){
      result.payload = idx;
      return;
    }

    if(result.distance < 0 && (hasFlightDistance || comparisonHeaderMatches(header, "distance"))){
      result.distance = idx;
      return;
    }

    if(result.radius < 0 && comparisonHeaderMatches(header, "radius")){
      result.radius = idx;
      return;
    }

    if(result.wind < 0 && (comparisonHeaderMatches(header, "wind") || (hasWind && /швидкіст/.test(header)))){
      result.wind = idx;
      return;
    }

    if(result.speed < 0 && !hasWind && comparisonHeaderMatches(header, "speed")){
      result.speed = idx;
      return;
    }

    if(result.flightTime < 0 && !hasDeploy && (hasFlightTime || comparisonHeaderMatches(header, "flightTime"))){
      result.flightTime = idx;
      return;
    }

    if(result.height < 0 && (hasFlightHeight || comparisonHeaderMatches(header, "height"))){
      result.height = idx;
      return;
    }

    if(result.deployTime < 0 && (hasDeploy || comparisonHeaderMatches(header, "deployTime"))){
      result.deployTime = idx;
      return;
    }

    if(result.cameraType < 0 && comparisonHeaderMatches(header, "cameraType")){
      result.cameraType = idx;
      return;
    }

    if(result.codified < 0 && comparisonHeaderMatches(header, "codified")){
      result.codified = idx;
    }

  });

  if(result.name < 0) result.name = result.model >= 0 ? result.model : 0;
  if(result.model < 0) result.model = result.name;
  result.units = detectComparisonHeaderUnits(headerRow, result);

  return result;

}

function pickTopComparisonRows(items, key, limit=5){

  return items
    .filter(item=>Number.isFinite(item?.[key]))
    .slice()
    .sort((a,b)=>(b[key] || 0) - (a[key] || 0))
    .slice(0, limit);

}

function buildComparisonMetricStats(items, keys){

  const stats = {};

  (keys || []).forEach(key=>{
    const values = items.map(item=>Number(item?.[key])).filter(Number.isFinite);
    if(!values.length){
      stats[key] = null;
      return;
    }
    stats[key] = {
      min: Math.min(...values),
      max: Math.max(...values),
    };
  });

  return stats;

}

function normalizeComparisonMetric(value, stat, inverse=false){

  const num = Number(value);
  if(!Number.isFinite(num) || !stat) return null;

  if(stat.max === stat.min) return 1;

  const raw = (num - stat.min) / (stat.max - stat.min);
  return inverse ? (1 - raw) : raw;

}

function calculateComparisonProfileScore(item, metricStats, weights){

  let weightedSum = 0;
  let totalWeight = 0;

  (weights || []).forEach(metric=>{
    if(metric.kind === "flag"){
      weightedSum += (item?.[metric.key] ? 1 : 0) * metric.weight;
      totalWeight += metric.weight;
      return;
    }

    const score = normalizeComparisonMetric(item?.[metric.key], metricStats?.[metric.key], !!metric.inverse);
    if(score == null) return;
    weightedSum += score * metric.weight;
    totalWeight += metric.weight;
  });

  return totalWeight > 0 ? Math.round((weightedSum / totalWeight) * 100) : 0;

}

const COMPARISON_PROFILE_CONFIG = {
  universal: {
    label: "Універсальний профіль",
    shortLabel: "Універсальний",
    tone: "blue",
    weights: [
      {key:"systemPrice", weight:0.24, inverse:true},
      {key:"payload", weight:0.16, inverse:false},
      {key:"distance", weight:0.15, inverse:false},
      {key:"flightTime", weight:0.11, inverse:false},
      {key:"speed", weight:0.1, inverse:false},
      {key:"radius", weight:0.08, inverse:false},
      {key:"wind", weight:0.06, inverse:false},
      {key:"deployTime", weight:0.05, inverse:true},
      {key:"height", weight:0.03, inverse:false},
      {kind:"flag", key:"thermal", weight:0.01},
      {kind:"flag", key:"codified", weight:0.01},
    ],
  },
  strike_multirotor: {
    label: "Ударний профіль",
    shortLabel: "Ударний",
    tone: "warn",
    weights: [
      {key:"payload", weight:0.24, inverse:false},
      {key:"distance", weight:0.18, inverse:false},
      {key:"speed", weight:0.12, inverse:false},
      {key:"radius", weight:0.12, inverse:false},
      {key:"flightTime", weight:0.08, inverse:false},
      {key:"wind", weight:0.08, inverse:false},
      {key:"height", weight:0.04, inverse:false},
      {key:"deployTime", weight:0.06, inverse:true},
      {key:"systemPrice", weight:0.04, inverse:true},
      {kind:"flag", key:"codified", weight:0.02},
      {kind:"flag", key:"thermal", weight:0.02},
    ],
  },
  recon_fixed_wing: {
    label: "Розвідник літакового типу",
    shortLabel: "Розвідка",
    tone: "blue",
    weights: [
      {key:"flightTime", weight:0.24, inverse:false},
      {key:"distance", weight:0.2, inverse:false},
      {key:"radius", weight:0.18, inverse:false},
      {key:"speed", weight:0.08, inverse:false},
      {key:"wind", weight:0.08, inverse:false},
      {key:"height", weight:0.06, inverse:false},
      {key:"deployTime", weight:0.05, inverse:true},
      {key:"systemPrice", weight:0.05, inverse:true},
      {kind:"flag", key:"thermal", weight:0.04},
      {kind:"flag", key:"codified", weight:0.02},
    ],
  },
  interceptor: {
    label: "Профіль перехоплення",
    shortLabel: "Перехоплення",
    tone: "warn",
    weights: [
      {key:"speed", weight:0.24, inverse:false},
      {key:"distance", weight:0.18, inverse:false},
      {key:"radius", weight:0.16, inverse:false},
      {key:"flightTime", weight:0.1, inverse:false},
      {key:"height", weight:0.1, inverse:false},
      {key:"wind", weight:0.08, inverse:false},
      {key:"deployTime", weight:0.06, inverse:true},
      {key:"systemPrice", weight:0.04, inverse:true},
      {kind:"flag", key:"codified", weight:0.02},
      {kind:"flag", key:"thermal", weight:0.02},
    ],
  },
  logistics: {
    label: "Логістичний профіль",
    shortLabel: "Логістика",
    tone: "ok",
    weights: [
      {key:"payload", weight:0.28, inverse:false},
      {key:"distance", weight:0.16, inverse:false},
      {key:"flightTime", weight:0.14, inverse:false},
      {key:"radius", weight:0.1, inverse:false},
      {key:"wind", weight:0.08, inverse:false},
      {key:"deployTime", weight:0.08, inverse:true},
      {key:"systemPrice", weight:0.12, inverse:true},
      {kind:"flag", key:"codified", weight:0.02},
      {kind:"flag", key:"thermal", weight:0.02},
    ],
  },
  value: {
    label: "Ціна / можливості",
    shortLabel: "Ціна / можливості",
    tone: "ok",
    weights: [
      {key:"payload", weight:0.16, inverse:false},
      {key:"distance", weight:0.16, inverse:false},
      {key:"speed", weight:0.08, inverse:false},
      {key:"flightTime", weight:0.12, inverse:false},
      {key:"radius", weight:0.08, inverse:false},
      {key:"wind", weight:0.06, inverse:false},
      {key:"systemPrice", weight:0.24, inverse:true},
      {key:"deployTime", weight:0.06, inverse:true},
      {kind:"flag", key:"codified", weight:0.02},
      {kind:"flag", key:"thermal", weight:0.02},
    ],
  },
};

const COMPARISON_SCENARIO_PROFILES = {
  default: ["universal", "value"],
  strike_multirotor: ["strike_multirotor", "value", "universal"],
  recon_fixed_wing: ["recon_fixed_wing", "value", "universal"],
  interceptor: ["interceptor", "value", "universal"],
  logistics: ["logistics", "value", "universal"],
};

function detectComparisonScenario(title="", items=[]){

  const source = `${title || ""} ${(items || []).map(item=>item?.name || "").join(" ")}`.toLowerCase();

  if(/перехоп|шахед|інтерцеп|intercept/.test(source)) return "interceptor";
  if(/розвід.*(літак|літаков|крил|fixed wing|fw\b)|((літак|літаков|крил|fixed wing|fw\b).*(розвід))/.test(source)) return "recon_fixed_wing";
  if(/логіст|транспорт|вантаж/.test(source)) return "logistics";
  if(/удар|бомбер|мультиротор|multirotor|\bмр\b|fpv/.test(source)) return "strike_multirotor";

  return "default";

}

function buildComparisonAnalytics(rows, title=""){

  const grid = Array.isArray(rows) ? rows : [];
  if(grid.length < 2) return null;

  const columns = detectComparisonColumns(grid[0], title);
  const comparisonUnits = columns.units || {
    price: "грн",
    payload: "кг",
    speed: "км/год",
    flightTime: "хв",
    height: "м",
    distance: "км",
    radius: "км",
    wind: "м/с",
    deployTime: "хв",
  };

  const items = grid.slice(1).map((row, index)=>{
    const model = String(row?.[columns.model] || row?.[columns.name] || "").trim();
    const vendor = columns.vendor >= 0 ? String(row?.[columns.vendor] || "").trim() : "";
    const name = model || vendor || `Позиція ${index + 1}`;

    if(isAnalyticsSummaryLabel(name)) return null;

    const item = {
      name,
      vendor,
      model,
      systemPrice: columns.systemPrice >= 0 ? parseAnalyticsNumber(row?.[columns.systemPrice]) : null,
      unitPrice: columns.unitPrice >= 0 ? parseAnalyticsNumber(row?.[columns.unitPrice]) : null,
      quantity: columns.quantity >= 0 ? parseAnalyticsNumber(row?.[columns.quantity]) : null,
      payload: columns.payload >= 0 ? parseAnalyticsNumber(row?.[columns.payload]) : null,
      speed: columns.speed >= 0 ? parseAnalyticsNumber(row?.[columns.speed]) : null,
      radius: columns.radius >= 0 ? parseAnalyticsNumber(row?.[columns.radius]) : null,
      distance: columns.distance >= 0 ? parseAnalyticsNumber(row?.[columns.distance]) : null,
      flightTime: columns.flightTime >= 0 ? parseAnalyticsNumber(row?.[columns.flightTime]) : null,
      height: columns.height >= 0 ? parseAnalyticsNumber(row?.[columns.height]) : null,
      wind: columns.wind >= 0 ? parseAnalyticsNumber(row?.[columns.wind]) : null,
      deployTime: columns.deployTime >= 0 ? parseAnalyticsNumber(row?.[columns.deployTime]) : null,
      cameraType: columns.cameraType >= 0 ? String(row?.[columns.cameraType] || "").trim() : "",
      codifiedRaw: columns.codified >= 0 ? String(row?.[columns.codified] || "").trim().toLowerCase() : "",
      subtype: columns.subtype,
      units: comparisonUnits,
    };

    const hasData = [
      item.systemPrice, item.unitPrice, item.quantity, item.payload, item.speed, item.radius,
      item.distance, item.flightTime, item.height, item.wind, item.deployTime
    ].some(v=>Number.isFinite(v)) || !!item.cameraType || !!item.vendor || !!item.model;

    if(!hasData) return null;

    item.codified = /^(так|є|yes|true|1)$/i.test(item.codifiedRaw);
    item.thermal = /тепловіз|thermal|ir/i.test(item.cameraType);

    return item;
  }).filter(Boolean);

  if(!items.length) return null;

  const avgSystemPriceRows = items.filter(item=>Number.isFinite(item.systemPrice));
  const avgSystemPrice = avgSystemPriceRows.length
    ? avgSystemPriceRows.reduce((sum, item)=>sum + item.systemPrice, 0) / avgSystemPriceRows.length
    : 0;

  const maxDistance = pickTopComparisonRows(items, "distance", 1)[0] || null;
  const maxPayload = pickTopComparisonRows(items, "payload", 1)[0] || null;
  const maxSpeed = pickTopComparisonRows(items, "speed", 1)[0] || null;
  const maxRadius = pickTopComparisonRows(items, "radius", 1)[0] || null;
  const maxHeight = pickTopComparisonRows(items, "height", 1)[0] || null;
  const maxWind = pickTopComparisonRows(items, "wind", 1)[0] || null;
  const topDistance = pickTopComparisonRows(items, "distance", items.length);
  const topPayload = pickTopComparisonRows(items, "payload", items.length);
  const topSpeed = pickTopComparisonRows(items, "speed", items.length);
  const topFlightTime = pickTopComparisonRows(items, "flightTime", items.length);
  const topRadius = pickTopComparisonRows(items, "radius", items.length);
  const topHeight = pickTopComparisonRows(items, "height", items.length);
  const topWind = pickTopComparisonRows(items, "wind", items.length);
  const cheapestSystems = items
    .filter(item=>Number.isFinite(item.systemPrice))
    .slice()
    .sort((a,b)=>(a.systemPrice || 0) - (b.systemPrice || 0));
  const fastestDeploy = items
    .filter(item=>Number.isFinite(item.deployTime))
    .slice()
    .sort((a,b)=>(a.deployTime || 0) - (b.deployTime || 0));
  const codifiedCount = items.filter(item=>item.codified).length;
  const thermalCount = items.filter(item=>item.thermal).length;
  const vendors = Array.from(new Set(items.map(item=>item.vendor).filter(Boolean)));
  const metricStats = buildComparisonMetricStats(items, ["payload", "distance", "speed", "flightTime", "radius", "height", "wind", "systemPrice", "deployTime"]);
  const scenario = detectComparisonScenario(title, items);
  const scenarioProfiles = COMPARISON_SCENARIO_PROFILES[scenario] || COMPARISON_SCENARIO_PROFILES.default;
  const profileScores = {};
  const profileRankings = {};

  Object.entries(COMPARISON_PROFILE_CONFIG).forEach(([profileId, profile])=>{
    const scoreKey = `${profileId}Score`;
    items.forEach(item=>{
      item[scoreKey] = calculateComparisonProfileScore(item, metricStats, profile.weights);
    });
    profileScores[profileId] = scoreKey;
    profileRankings[profileId] = items
      .slice()
      .sort((a,b)=>(b[scoreKey] || 0) - (a[scoreKey] || 0));
  });

  const overallTop = profileRankings.universal || [];
  const bestOverall = overallTop[0] || null;
  const featuredProfiles = scenarioProfiles.map(profileId=>({
    id: profileId,
    config: COMPARISON_PROFILE_CONFIG[profileId],
    scoreKey: profileScores[profileId],
    top: profileRankings[profileId] || [],
    best: (profileRankings[profileId] || [])[0] || null,
  })).filter(item=>item?.config);
  const priceSlices = buildComparisonRangeSliceRows(items, "systemPrice", " грн");
  const payloadSlices = buildComparisonRangeSliceRows(items, "payload", " кг");
  const distanceSlices = buildComparisonRangeSliceRows(items, "distance", " км");
  const flightTimeSlices = buildComparisonRangeSliceRows(items, "flightTime", " хв");
  const cameraSlices = [
    {label:"З тепловізором", value: thermalCount},
    {label:"Без тепловізора", value: Math.max(0, items.length - thermalCount)},
  ].filter(item=>item.value > 0);

  return {
    items,
    subtype: columns.subtype,
    units: comparisonUnits,
    scenario,
    scenarioProfiles,
    featuredProfiles,
    avgSystemPrice,
    maxDistance,
    maxPayload,
    maxSpeed,
    maxRadius,
    maxHeight,
    maxWind,
    topDistance,
    topPayload,
    topSpeed,
    topFlightTime,
    topRadius,
    topHeight,
    topWind,
    cheapestSystems,
    fastestDeploy,
    overallTop,
    bestOverall,
    codifiedCount,
    thermalCount,
    vendorCount: vendors.length,
    priceSlices,
    payloadSlices,
    distanceSlices,
    flightTimeSlices,
    cameraSlices,
  };

}

function renderComparisonTopList(title, rows, metricKey, metricLabel, unit=""){

  return `
    <div class="item analytics-block comparison-compact-section">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="comparison-compact-grid">
        ${rows.length
          ? rows.map((item, index)=>{
              const detailKey = registerRenderedTableModal(`Модель: ${item.name}`, buildComparisonItemDetailHtml(item));
              return `
              <button type="button" class="comparison-compact-card comparison-card-btn" data-action="openRenderedTableModal" data-arg1="${detailKey}">
                <div class="comparison-compact-rank mono">${index + 1}</div>
                <div class="comparison-compact-main">
                  <div class="comparison-compact-title">${htmlesc(item.name)}</div>
                  <div class="comparison-compact-meta">${metricLabel}: ${fmtNum(item[metricKey])}${unit}${item.vendor ? ` · ${htmlesc(item.vendor)}` : ""}</div>
                </div>
                <div class="badge b-blue mono">${fmtNum(item[metricKey])}${unit}</div>
              </button>
            `;
            }).join("")
          : `<div class="hint">Даних для цього рейтингу поки немає.</div>`
        }
      </div>
    </div>
  `;

}

function renderComparisonTopListAsc(title, rows, metricKey, metricLabel, unit=""){

  const formatValue = (item)=>{
    if(metricKey === "systemPrice") return fmtCompactMoneyUa(item?.[metricKey]);
    return `${fmtNum(item?.[metricKey])}${unit}`;
  };

  return `
    <div class="item analytics-block comparison-compact-section">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="comparison-compact-grid">
        ${rows.length
          ? rows.map((item, index)=>{
              const detailKey = registerRenderedTableModal(`Модель: ${item.name}`, buildComparisonItemDetailHtml(item));
              return `
              <button type="button" class="comparison-compact-card comparison-card-btn" data-action="openRenderedTableModal" data-arg1="${detailKey}">
                <div class="comparison-compact-rank mono">${index + 1}</div>
                <div class="comparison-compact-main">
                  <div class="comparison-compact-title">${htmlesc(item.name)}</div>
                  <div class="comparison-compact-meta">${metricLabel}: ${formatValue(item)}${item.vendor ? ` · ${htmlesc(item.vendor)}` : ""}</div>
                </div>
                <div class="badge b-ok mono">${formatValue(item)}</div>
              </button>
            `;
            }).join("")
          : `<div class="hint">Даних для цього рейтингу поки немає.</div>`
        }
      </div>
    </div>
  `;

}

function buildComparisonOverallRankingHelpHtml(){

  const universal = COMPARISON_PROFILE_CONFIG.universal;
  const weightRows = (universal?.weights || []).map(item=>{
    const labels = {
      systemPrice: "Ціна комплексу",
      payload: "Навантаження",
      distance: "Дальність",
      flightTime: "Час польоту",
      speed: "Швидкість",
      radius: "Радіус",
      wind: "Стійкість до вітру",
      deployTime: "Час розгортання",
      height: "Висота",
      thermal: "Тепловізор",
      codified: "Кодифікація",
    };

    const label = labels[item.key] || item.key;
    const percent = Math.round(Number(item.weight || 0) * 100);
    const isPrimary = item.key === "systemPrice";
    return `
      <div class="comparison-weight-row ${isPrimary ? "is-primary" : ""}">
        <div class="comparison-weight-head">
          <div class="comparison-weight-label">${htmlesc(label)}</div>
          <div class="comparison-weight-value mono">${percent}%</div>
        </div>
        <div class="comparison-weight-bar">
          <div class="comparison-weight-fill" style="width:${Math.max(4, Math.min(percent, 100))}%"></div>
        </div>
        <div class="comparison-weight-note">${item.inverse ? "Менше = краще" : "Більше = краще"}</div>
      </div>
    `;
  }).join("");

  return `
    <div class="comparison-help-modal">
      <div class="comparison-help-block">
        <div class="comparison-help-title">Як рахується “Загальний” рейтинг</div>
        <div class="comparison-help-text">
          Це інтегральний рейтинг для швидкого вибору моделі без переплати.
        </div>
      </div>
      <div class="comparison-help-block comparison-help-primary">
        <div class="comparison-help-title">Головний акцент</div>
        <div class="comparison-help-text">
          У “Загальному” рейтингу <b>ціна комплексу — пріоритет №1</b>. Якщо дві моделі близькі за можливостями, вище буде та, що дає схожий результат за менші гроші.
        </div>
      </div>
      <div class="comparison-help-block">
        <div class="comparison-help-title">Ваги критеріїв</div>
        <div class="comparison-weight-grid">${weightRows}</div>
      </div>
      <div class="comparison-help-block">
        <div class="comparison-help-title">Приклад</div>
        <div class="comparison-help-text">
          Якщо модель <b>A</b> коштує <b>1,2 млн</b>, а модель <b>B</b> — <b>2,4 млн</b>, і при цьому різниця по дальності / навантаженню / часу польоту невелика, то вище у “Загальному” рейтингу буде <b>A</b>. Тобто рейтинг спеціально зсунений у бік <b>не переплачувати</b> за близькі можливості.
        </div>
      </div>
      <div class="comparison-help-block">
        <div class="comparison-help-title">Що важливо</div>
        <div class="comparison-help-text">
          Якщо одна модель істотно сильніша за ключовими характеристиками, вона все одно може піднятись вище, навіть якщо дорожча. Тобто це не “найдешевше будь-якою ціною”, а <b>розумний баланс із пріоритетом ціни</b>.
        </div>
      </div>
    </div>
  `;

}

function renderComparisonDonutCard(title, rows, metricKey, unit="", emptyText="Поки немає даних.", colors=null){

  const slices = buildEvalSlices(
    (rows || []).slice(0, 5).map(item=>({label:item.name, value:item?.[metricKey] || 0})),
    colors || ["#5f8ef5", "#6fbf73", "#ff9f43", "#b783ff", "#ff6b8b"]
  );

  return `
    <div class="item analytics-block eval-donut-card comparison-donut-card">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="eval-donut-wrap">
        <div class="eval-donut is-animated-donut" data-donut-gradient="${htmlesc(slices.gradient)}" style="background:conic-gradient(#dfe6f6 0 360deg);"></div>
        <div>
          ${slices.legendRows.length
            ? slices.legendRows.map(row=>`<div class="eval-legend-item"><span class="eval-legend-dot" style="background:${row.color}"></span><span>${htmlesc(row.label)}</span><b class="mono">${fmtNum(row.value)}${unit}</b></div>`).join("")
            : `<div class="hint">${htmlesc(emptyText)}</div>`
          }
        </div>
      </div>
    </div>
  `;

}

function renderComparisonSliceDonutCard(title, slices, emptyText="Поки немає даних.", colors=null){

  const donut = buildEvalSlices(
    slices || [],
    colors || ["#5f8ef5", "#6fbf73", "#ff9f43", "#b783ff", "#ff6b8b"]
  );

  return `
    <div class="item analytics-block eval-donut-card comparison-donut-card">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="eval-donut-wrap">
        <div class="eval-donut is-animated-donut" data-donut-gradient="${htmlesc(donut.gradient)}" style="background:conic-gradient(#dfe6f6 0 360deg);"></div>
        <div>
          ${donut.legendRows.length
            ? donut.legendRows.map(row=>`<div class="eval-legend-item"><span class="eval-legend-dot" style="background:${row.color}"></span><span>${htmlesc(row.label)}</span><b class="mono">${fmtNum(row.value)}</b></div>`).join("")
            : `<div class="hint">${htmlesc(emptyText)}</div>`
          }
        </div>
      </div>
    </div>
  `;

}

function buildComparisonRangeSliceRows(items, key, unit=""){

  const rows = (items || []).filter(item=>Number.isFinite(item?.[key]));
  if(!rows.length) return [];

  const values = rows.map(item=>Number(item[key]));
  const min = Math.min(...values);
  const max = Math.max(...values);

  if(min === max){
    return [{label:`Усі: ${fmtNum(min)}${unit}`, value: rows.length}];
  }

  const step = (max - min) / 3;
  const limit1 = min + step;
  const limit2 = min + step * 2;

  const bins = [
    {label:`до ${fmtNum(limit1)}${unit}`, value:0},
    {label:`${fmtNum(limit1)}–${fmtNum(limit2)}${unit}`, value:0},
    {label:`від ${fmtNum(limit2)}${unit}`, value:0},
  ];

  rows.forEach(item=>{
    const value = Number(item[key]);
    if(value < limit1){
      bins[0].value += 1;
    } else if(value < limit2){
      bins[1].value += 1;
    } else {
      bins[2].value += 1;
    }
  });

  return bins.filter(item=>item.value > 0);

}

function renderComparisonCompactCards(title, rows, metricKey, unit="", tone="blue"){

  const safeTone = ["blue","ok","warn"].includes(tone) ? tone : "blue";

  return `
    <div class="item analytics-block comparison-compact-section">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="comparison-compact-grid">
        ${rows.length
          ? rows.slice(0, 4).map((item, index)=>{
              const detailKey = registerRenderedTableModal(`Модель: ${item.name}`, buildComparisonItemDetailHtml(item));
              return `
              <button type="button" class="comparison-compact-card comparison-card-btn" data-action="openRenderedTableModal" data-arg1="${detailKey}">
                <div class="comparison-compact-rank mono">${index + 1}</div>
                <div class="comparison-compact-main">
                  <div class="comparison-compact-title">${htmlesc(item.name)}</div>
                  <div class="comparison-compact-meta">${item.vendor ? `Виробник: ${htmlesc(item.vendor)}` : "Без виробника"}</div>
                </div>
                <div class="badge b-${safeTone} mono">${fmtNum(item[metricKey])}${unit}</div>
              </button>
            `;
            }).join("")
          : `<div class="hint">Даних поки немає.</div>`
        }
      </div>
    </div>
  `;

}

function renderComparisonLeaderCards(title, cards){

  return `
    <div class="item analytics-block comparison-compact-section">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="comparison-leader-grid">
        ${cards.filter(card=>card && card.item).map(card=>{
          const item = card.item;
          const detailKey = registerRenderedTableModal(`Модель: ${item.name}`, buildComparisonItemDetailHtml(item));
          const value = Number.isFinite(card.value) ? fmtNum(card.value) : "—";
          const unit = card.unit || "";
          const tone = ["blue","ok","warn"].includes(card.tone) ? card.tone : "blue";
          const meta = card.meta || `${card.metricLabel}: ${value}${unit}${item.vendor ? ` · ${htmlesc(item.vendor)}` : ""}`;
          return `
            <button type="button" class="comparison-leader-card comparison-card-btn" data-action="openRenderedTableModal" data-arg1="${detailKey}">
              <div class="comparison-leader-head">
                <div class="comparison-leader-label">${htmlesc(card.label)}</div>
                <div class="badge b-${tone} mono">${value}${unit}</div>
              </div>
              <div class="comparison-leader-title">${htmlesc(item.name)}</div>
              <div class="comparison-leader-meta">${meta}</div>
            </button>
          `;
        }).join("") || `<div class="hint">Даних для цього блоку поки немає.</div>`}
      </div>
    </div>
  `;

}

function buildComparisonAutoSummaryHtml(analytics){

  if(!analytics) return "";

  const {
    items=[],
    scenario="default",
    cheapestSystems=[],
    bestOverall=null,
    maxDistance=null,
    maxPayload=null,
    maxSpeed=null,
    topFlightTime=[],
    thermalCount=0,
    vendorCount=0,
  } = analytics;

  const priceItems = items.filter(item=>Number.isFinite(item.systemPrice));
  const minPrice = priceItems.length ? Math.min(...priceItems.map(item=>item.systemPrice)) : null;
  const maxPrice = priceItems.length ? Math.max(...priceItems.map(item=>item.systemPrice)) : null;
  const cheapest = cheapestSystems[0] || null;
  const bestFlightTime = topFlightTime[0] || null;

  let focusLeader = maxPayload;
  let focusLabel = "Лідер по навантаженню";
  let focusText = maxPayload
    ? `Корисне навантаження ${fmtNum(maxPayload.payload)} ${analytics?.units?.payload || "кг"}.`
    : "Ключового лідера за цим профілем поки не видно.";

  if(scenario === "recon_fixed_wing"){
    focusLeader = bestFlightTime || maxDistance;
    focusLabel = bestFlightTime ? "Лідер по тривалості польоту" : "Лідер по дальності";
    focusText = bestFlightTime
      ? `Час польоту ${fmtNum(bestFlightTime.flightTime)} ${analytics?.units?.flightTime || "хв"}.`
      : (maxDistance ? `Дальність ${fmtNum(maxDistance.distance)} ${analytics?.units?.distance || "км"}.` : "Ключового лідера поки не видно.");
  } else if(scenario === "interceptor"){
    focusLeader = maxSpeed || maxDistance;
    focusLabel = maxSpeed ? "Лідер по швидкості" : "Лідер по дальності";
    focusText = maxSpeed
      ? `Максимальна швидкість ${fmtNum(maxSpeed.speed)} ${analytics?.units?.speed || "км/год"}.`
      : (maxDistance ? `Дальність ${fmtNum(maxDistance.distance)} ${analytics?.units?.distance || "км"}.` : "Ключового лідера поки не видно.");
  } else if(scenario === "logistics"){
    focusLeader = maxPayload || bestFlightTime;
    focusLabel = maxPayload ? "Лідер по вантажу" : "Лідер по часу польоту";
    focusText = maxPayload
      ? `Корисне навантаження ${fmtNum(maxPayload.payload)} ${analytics?.units?.payload || "кг"}.`
      : (bestFlightTime ? `Час польоту ${fmtNum(bestFlightTime.flightTime)} ${analytics?.units?.flightTime || "хв"}.` : "Ключового лідера поки не видно.");
  } else if(scenario === "strike_multirotor"){
    focusLeader = maxPayload || maxDistance;
    focusLabel = maxPayload ? "Лідер по навантаженню" : "Лідер по дальності";
    focusText = maxPayload
      ? `Корисне навантаження ${fmtNum(maxPayload.payload)} ${analytics?.units?.payload || "кг"}.`
      : (maxDistance ? `Дальність ${fmtNum(maxDistance.distance)} ${analytics?.units?.distance || "км"}.` : "Ключового лідера поки не видно.");
  }

  const coverageText = thermalCount <= 0
    ? `Тепловізійних камер у вибірці не зафіксовано. Виробників: ${fmtNum(vendorCount)}.`
    : (thermalCount === items.length
      ? `Усі ${fmtNum(items.length)} позицій мають тепловізійну камеру.`
      : `Тепловізійна камера є у ${fmtNum(thermalCount)} з ${fmtNum(items.length)} позицій.`);

  const cards = [
    {
      label: "Найдоступніший варіант",
      value: cheapest ? htmlesc(cheapest.name) : "—",
      text: cheapest && Number.isFinite(cheapest.systemPrice)
        ? `Вартість комплексу: ${fmtCompactMoneyUa(cheapest.systemPrice)}.`
        : "По ціні даних поки недостатньо.",
    },
    {
      label: focusLabel,
      value: focusLeader ? htmlesc(focusLeader.name) : "—",
      text: focusText,
    },
    {
      label: "Збалансований вибір",
      value: bestOverall ? htmlesc(bestOverall.name) : "—",
      text: bestOverall && Number.isFinite(bestOverall.systemPrice)
        ? `Без переплати: ${fmtCompactMoneyUa(bestOverall.systemPrice)} за сильний загальний профіль.`
        : "Загальний рейтинг поки не зібрав достатньо даних.",
    },
    {
      label: "Зріз вибірки",
      value: (minPrice != null && maxPrice != null) ? `${fmtCompactMoneyUa(minPrice)} – ${fmtCompactMoneyUa(maxPrice)}` : fmtNum(items.length),
      text: coverageText,
    },
  ];

  return `
    <div class="staffing-summary-grid comparison-summary-grid">
      ${cards.map(card=>`
        <div class="staffing-summary-card comparison-summary-card">
          <div class="staffing-summary-k">${card.label}</div>
          <div class="staffing-summary-v">${card.value}</div>
          <div class="staffing-summary-s">${card.text}</div>
        </div>
      `).join("")}
    </div>
  `;

}

function buildComparisonItemDetailHtml(item){

  const units = item?.units || {
    price: "грн",
    payload: "кг",
    speed: "км/год",
    flightTime: "хв",
    height: "м",
    distance: "км",
    radius: "км",
    wind: "м/с",
    deployTime: "хв",
  };

  const classifyAccent = (key, value)=>{
    const num = Number(value);

    if(key === "cameraType"){
      return /тепловіз|thermal|ir/i.test(String(value || "")) ? "strong" : "neutral";
    }
    if(key === "codified"){
      return value ? "strong" : "neutral";
    }
    if(!Number.isFinite(num)) return "neutral";

    const thresholds = {
      systemPrice: {strongMax: 1200000, weakMin: 3000000, inverse:true},
      quantity: {strongMin: 3, weakMax: 1},
      payload: {strongMin: 15, weakMax: 5},
      distance: {strongMin: 25, weakMax: 10},
      flightTime: {strongMin: 25, weakMax: 10},
      speed: {strongMin: 70, weakMax: 40},
      radius: {strongMin: 15, weakMax: 8},
      height: {strongMin: 1000, weakMax: 400},
      wind: {strongMin: 10, weakMax: 6},
      deployTime: {strongMax: 8, weakMin: 20, inverse:true},
    };

    const t = thresholds[key];
    if(!t) return "neutral";

    if(t.inverse){
      if(Number.isFinite(t.strongMax) && num <= t.strongMax) return "strong";
      if(Number.isFinite(t.weakMin) && num >= t.weakMin) return "weak";
      return "neutral";
    }

    if(Number.isFinite(t.strongMin) && num >= t.strongMin) return "strong";
    if(Number.isFinite(t.weakMax) && num <= t.weakMax) return "weak";
    return "neutral";
  };

  const groups = [
    {
      title: "Економіка",
      rows: [
        {label:"Виробник", value:item.vendor || "—", accent:"neutral"},
        {label:"Вартість БпАК", value:Number.isFinite(item.systemPrice) ? fmtCompactMoneyUa(item.systemPrice) : "—", accent:classifyAccent("systemPrice", item.systemPrice)},
        {label:"Вартість БпЛА", value:Number.isFinite(item.unitPrice) ? fmtCompactMoneyUa(item.unitPrice) : "—", accent:"neutral"},
        {label:"Кількість у комплексі", value:Number.isFinite(item.quantity) ? fmtNum(item.quantity) : "—", accent:classifyAccent("quantity", item.quantity)},
      ],
    },
    {
      title: "Льотні характеристики",
      rows: [
        {label:"Дальність", value:Number.isFinite(item.distance) ? `${fmtNum(item.distance)} ${units.distance}` : "—", accent:classifyAccent("distance", item.distance)},
        {label:"Час польоту", value:Number.isFinite(item.flightTime) ? `${fmtNum(item.flightTime)} ${units.flightTime}` : "—", accent:classifyAccent("flightTime", item.flightTime)},
        {label:"Швидкість", value:Number.isFinite(item.speed) ? `${fmtNum(item.speed)} ${units.speed}` : "—", accent:classifyAccent("speed", item.speed)},
        {label:"Радіус", value:Number.isFinite(item.radius) ? `${fmtNum(item.radius)} ${units.radius}` : "—", accent:classifyAccent("radius", item.radius)},
        {label:"Висота", value:Number.isFinite(item.height) ? `${fmtNum(item.height)} ${units.height}` : "—", accent:classifyAccent("height", item.height)},
        {label:"Стійкість до вітру", value:Number.isFinite(item.wind) ? `${fmtNum(item.wind)} ${units.wind}` : "—", accent:classifyAccent("wind", item.wind)},
      ],
    },
    {
      title: "Навантаження та розгортання",
      rows: [
        {label:"Корисне навантаження", value:Number.isFinite(item.payload) ? `${fmtNum(item.payload)} ${units.payload}` : "—", accent:classifyAccent("payload", item.payload)},
        {label:"Час розгортання", value:Number.isFinite(item.deployTime) ? `${fmtNum(item.deployTime)} ${units.deployTime}` : "—", accent:classifyAccent("deployTime", item.deployTime)},
      ],
    },
    {
      title: "Оснащення",
      rows: [
        {label:"Камера", value:item.cameraType || "—", accent:classifyAccent("cameraType", item.cameraType)},
        {label:"Кодифікація", value:item.codified ? "Так" : "Ні", accent:classifyAccent("codified", item.codified)},
      ],
    },
  ].filter(group=>group.rows.some(row=>String(row.value || "").trim() && String(row.value || "").trim() !== "—"));

  const profileRows = [
    {label:"Універсальний", key:"universalScore"},
    {label:"Ударний", key:"strike_multirotorScore"},
    {label:"Розвідка", key:"recon_fixed_wingScore"},
    {label:"Перехоплення", key:"interceptorScore"},
    {label:"Логістика", key:"logisticsScore"},
    {label:"Ціна / можливості", key:"valueScore"},
  ]
    .filter(row=>Number.isFinite(item?.[row.key]))
    .map(row=>({...row, score:Number(item[row.key])}))
    .sort((a,b)=>b.score - a.score);

  const flattenedRows = groups.flatMap(group=>group.rows);
  const strengthLabels = flattenedRows
    .filter(row=>row.accent === "strong")
    .map(row=>row.label)
    .slice(0, 4);
  const weaknessLabels = flattenedRows
    .filter(row=>row.accent === "weak")
    .map(row=>row.label)
    .slice(0, 4);
  const primaryProfile = profileRows[0] || null;
  const secondaryProfiles = profileRows.slice(1, 3);
  const recommendationBlock = `
    <div class="comparison-insight-grid">
      <div class="comparison-insight-card">
        <div class="comparison-insight-label">Основний сценарій</div>
        <div class="comparison-insight-value">${htmlesc(primaryProfile?.label || "Потрібно дивитись вручну")}</div>
      </div>
      <div class="comparison-insight-card">
        <div class="comparison-insight-label">Також підходить</div>
        <div class="comparison-insight-tags">
          ${secondaryProfiles.length
            ? secondaryProfiles.map(row=>`<span class="comparison-insight-tag">${htmlesc(row.label)}</span>`).join("")
            : `<span class="comparison-insight-muted">Немає вираженого другого сценарію</span>`
          }
        </div>
      </div>
      <div class="comparison-insight-card">
        <div class="comparison-insight-label">Сильні сторони</div>
        <div class="comparison-insight-tags">
          ${strengthLabels.length
            ? strengthLabels.map(label=>`<span class="comparison-insight-tag is-strong">${htmlesc(label)}</span>`).join("")
            : `<span class="comparison-insight-muted">Явно виражених немає</span>`
          }
        </div>
      </div>
      <div class="comparison-insight-card">
        <div class="comparison-insight-label">Слабкі сторони</div>
        <div class="comparison-insight-tags">
          ${weaknessLabels.length
            ? weaknessLabels.map(label=>`<span class="comparison-insight-tag is-weak">${htmlesc(label)}</span>`).join("")
            : `<span class="comparison-insight-muted">Критичних слабких сторін не видно</span>`
          }
        </div>
      </div>
    </div>
  `;

  return `
    <div class="comparison-detail-modal">
      <div class="comparison-detail-head">
        <div class="comparison-detail-title">${htmlesc(item.name || "Модель")}</div>
        ${item.vendor ? `<div class="comparison-detail-sub">${htmlesc(item.vendor)}</div>` : ``}
      </div>
      ${recommendationBlock}
      <div class="comparison-detail-groups">
        ${groups.map(group=>`
          <div class="comparison-detail-section">
              <div class="comparison-detail-section-title">${htmlesc(group.title)}</div>
              <div class="comparison-detail-grid">
                ${group.rows.map(row=>`
                <div class="comparison-detail-row ${row.accent === "strong" ? "is-strong" : (row.accent === "weak" ? "is-weak" : "")}">
                  <div class="comparison-detail-row-top">
                    <div class="comparison-detail-label">${htmlesc(row.label)}</div>
                    ${row.accent === "strong" ? `<span class="comparison-detail-flag is-strong">сильна</span>` : (row.accent === "weak" ? `<span class="comparison-detail-flag is-weak">слабка</span>` : ``)}
                  </div>
                  <div class="comparison-detail-value">${htmlesc(String(row.value || "—"))}</div>
                </div>
              `).join("")}
            </div>
          </div>
        `).join("")}
      </div>
    </div>
  `;

}

function renderComparisonSwitchTopBlock(title, itemsByKey, units={}, defaultKey="price"){

  const buttons = [
    {key:"overall", label:"Загальний"},
    {key:"price", label:"Ціна"},
    {key:"distance", label:"Дальність"},
    {key:"payload", label:"Навантаження"},
    {key:"speed", label:"Швидкість"},
    {key:"flightTime", label:"Час"},
    {key:"radius", label:"Радіус"},
    {key:"height", label:"Висота"},
    {key:"wind", label:"Вітер"},
  ].filter(item=>itemsByKey?.[item.key]);

  if(!buttons.length) return "";

  const groupId = `cmp_top_${Math.random().toString(36).slice(2, 8)}`;
  const helpKey = registerRenderedTableModal("Довідка: загальний рейтинг", buildComparisonOverallRankingHelpHtml());

  const panels = {
    overall: renderComparisonTopList("Рейтинг за загальним критерієм", itemsByKey.overall || [], "universalScore", "Загальний рейтинг", ""),
    price: renderComparisonTopListAsc("Рейтинг за ціною", itemsByKey.price || [], "systemPrice", "Ціна", ` ${units.price || "грн"}`),
    distance: renderComparisonTopList("Рейтинг за дальністю", itemsByKey.distance || [], "distance", "Дальність", ` ${units.distance || "км"}`),
    payload: renderComparisonTopList("Рейтинг за навантаженням", itemsByKey.payload || [], "payload", "Навантаження", ` ${units.payload || "кг"}`),
    speed: renderComparisonTopList("Рейтинг за швидкістю", itemsByKey.speed || [], "speed", "Швидкість", ` ${units.speed || "км/год"}`),
    flightTime: renderComparisonTopList("Рейтинг за часом польоту", itemsByKey.flightTime || [], "flightTime", "Час польоту", ` ${units.flightTime || "хв"}`),
    radius: renderComparisonTopList("Рейтинг за радіусом", itemsByKey.radius || [], "radius", "Радіус", ` ${units.radius || "км"}`),
    height: renderComparisonTopList("Рейтинг за висотою", itemsByKey.height || [], "height", "Висота", ` ${units.height || "м"}`),
    wind: renderComparisonTopList("Рейтинг за стійкістю до вітру", itemsByKey.wind || [], "wind", "Вітер", ` ${units.wind || "м/с"}`),
  };

  return `
    <div class="item analytics-block comparison-switch-block">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="comparison-switcher" data-topswitch-group="${groupId}">
        <div class="comparison-switcher-head">
          <div class="comparison-switcher-buttons">
          ${buttons.map(btn=>`
            <button
              type="button"
              class="comparison-switcher-btn ${btn.key===defaultKey ? "is-active" : ""}"
              data-action="switchComparisonTopPanel"
              data-arg1="${groupId}"
              data-arg2="${btn.key}"
            >${htmlesc(btn.label)}</button>
          `).join("")}
          </div>
          <button type="button" class="btn ghost btn-mini comparison-switcher-help" data-action="openRenderedTableModal" data-arg1="${helpKey}">Довідка</button>
        </div>
        <div class="comparison-switch-panels">
          ${buttons.map(btn=>`
            <div class="comparison-switch-panel ${btn.key===defaultKey ? "is-active" : ""}" data-topswitch-panel="${groupId}:${btn.key}">
              ${panels[btn.key] || ""}
            </div>
          `).join("")}
        </div>
      </div>
    </div>
  `;

}

function buildComparisonAnalyticsModalHtml(rows, title=""){

  const analytics = buildComparisonAnalytics(rows, title);

  if(!analytics){
    return `
      <div class="hint">Не вдалося розпізнати таблицю порівняння. Очікуються колонки на кшталт: найменування, виробник, ціна, дальність, навантаження, швидкість.</div>
    `;
  }

    const {
      items,
      subtype,
      units,
      avgSystemPrice,
    maxDistance,
    maxPayload,
    maxSpeed,
    maxRadius,
    maxHeight,
    maxWind,
    topDistance,
    topPayload,
    topSpeed,
    topFlightTime,
    topRadius,
    topHeight,
    topWind,
      cheapestSystems,
      fastestDeploy,
      overallTop,
      bestOverall,
      featuredProfiles,
      codifiedCount,
      thermalCount,
      vendorCount,
      priceSlices,
      payloadSlices,
      distanceSlices,
      flightTimeSlices,
      cameraSlices,
    } = analytics;

    const primaryProfile = featuredProfiles[0] || null;
    const secondaryProfile = featuredProfiles[1] || null;
    const summaryTile = (label, value, sub, item=null)=>{
      if(item){
        const detailKey = registerRenderedTableModal(`Модель: ${item.name}`, buildComparisonItemDetailHtml(item));
        return `<button type="button" class="report-tile clickable comparison-summary-btn" data-action="openRenderedTableModal" data-arg1="${detailKey}"><div class="k">${htmlesc(label)}</div><div class="v mono">${htmlesc(String(value))}</div><div class="s">${htmlesc(sub || "—")}</div></button>`;
      }
      return `<div class="report-tile"><div class="k">${htmlesc(label)}</div><div class="v mono">${htmlesc(String(value))}</div><div class="s">${htmlesc(sub || "—")}</div></div>`;
    };

    const summaryGrid = `
      <div class="report-grid staffing-analytics-kpis">
        ${summaryTile("Сер. ціна БпАК", fmtCompactMoneyUa(avgSystemPrice), "грн")}
        ${summaryTile("Макс. дальність", maxDistance ? fmtNum(maxDistance.distance) : "0", maxDistance ? maxDistance.name : "—", maxDistance)}
        ${summaryTile("Макс. навантаження", maxPayload ? fmtNum(maxPayload.payload) : "0", maxPayload ? maxPayload.name : "—", maxPayload)}
        ${summaryTile("Макс. швидкість", maxSpeed ? fmtNum(maxSpeed.speed) : "0", maxSpeed ? maxSpeed.name : "—", maxSpeed)}
        ${summaryTile("Макс. радіус", maxRadius ? fmtNum(maxRadius.radius) : "0", maxRadius ? maxRadius.name : "—", maxRadius)}
        ${summaryTile("Макс. висота", maxHeight ? fmtNum(maxHeight.height) : "0", maxHeight ? maxHeight.name : "—", maxHeight)}
        ${summaryTile("Стійкість до вітру", maxWind ? fmtNum(maxWind.wind) : "0", maxWind ? maxWind.name : "—", maxWind)}
        ${summaryTile(primaryProfile ? primaryProfile.config.shortLabel : "Профіль 1", primaryProfile?.best ? fmtNum(primaryProfile.best[primaryProfile.scoreKey]) : "0", primaryProfile?.best ? primaryProfile.best.name : "—", primaryProfile?.best || null)}
        ${summaryTile(secondaryProfile ? secondaryProfile.config.shortLabel : "Профіль 2", secondaryProfile?.best ? fmtNum(secondaryProfile.best[secondaryProfile.scoreKey]) : "0", secondaryProfile?.best ? secondaryProfile.best.name : "—", secondaryProfile?.best || null)}
        ${summaryTile("Кодифіковано", fmtNum(codifiedCount), `із ${fmtNum(items.length)}`)}
        ${summaryTile("З тепловізором", fmtNum(thermalCount), "позицій")}
        ${summaryTile("Виробників", fmtNum(vendorCount), "у таблиці")}
      </div>
    `;

    const technicalLeaders = renderComparisonLeaderCards("Лідери за технічними критеріями", [
    maxDistance ? {label:"Дальність", item:maxDistance, value:maxDistance.distance, metricLabel:"Дальність", unit:` ${units?.distance || "км"}`} : null,
    maxPayload ? {label:"Навантаження", item:maxPayload, value:maxPayload.payload, metricLabel:"Навантаження", unit:` ${units?.payload || "кг"}`} : null,
    maxSpeed ? {label:"Швидкість", item:maxSpeed, value:maxSpeed.speed, metricLabel:"Швидкість", unit:` ${units?.speed || "км/год"}`} : null,
    topFlightTime[0] ? {label:"Час польоту", item:topFlightTime[0], value:topFlightTime[0].flightTime, metricLabel:"Час польоту", unit:` ${units?.flightTime || "хв"}`} : null,
    maxRadius ? {label:"Радіус", item:maxRadius, value:maxRadius.radius, metricLabel:"Радіус", unit:` ${units?.radius || "км"}`} : null,
    maxHeight ? {label:"Висота", item:maxHeight, value:maxHeight.height, metricLabel:"Висота", unit:` ${units?.height || "м"}`} : null,
    maxWind ? {label:"Стійкість до вітру", item:maxWind, value:maxWind.wind, metricLabel:"Вітер", unit:` ${units?.wind || "м/с"}`} : null,
    ]);
  const switchTopBlock = renderComparisonSwitchTopBlock("Рейтинг по критерію", {
    overall: overallTop,
    price: cheapestSystems,
    distance: topDistance,
    payload: topPayload,
    speed: topSpeed,
    flightTime: topFlightTime,
    radius: topRadius,
    height: topHeight,
    wind: topWind,
  }, units, "overall");

    return `
      <div class="staffing-analytics-modal comparison-analytics-modal">
        ${summaryGrid}
        ${buildComparisonAutoSummaryHtml(analytics)}
        ${technicalLeaders}
        ${switchTopBlock}
      </div>
    `;

  }

function detectDeltaNrkColumns(headerRow){

  const headers = (headerRow || []).map(normalizeAnalyticsHeader);
  const result = {
    reportUuid:-1,
    unit:-1,
    reporter:-1,
    taskType:-1,
    result:-1,
    evacuatedCategory:-1,
    evacuatedQty:-1,
    resultAt:-1,
    startAt:-1,
    endAt:-1,
    duration:-1,
    circumstances:-1,
    cargo:-1,
    cargoWeight:-1,
    assetStatus:-1,
    lossCircumstances:-1,
    asset:-1,
    totalPoints:-1,
    primaryLink:-1,
    reserveLink:-1,
  };

  headers.forEach((header, idx)=>{
    if(result.reportUuid < 0 && /(uuid|ідентифікатор звіту|uuid звіту|uuid запису)/.test(header)){
      result.reportUuid = idx;
      return;
    }
    if(result.unit < 0 && /(підрозділ|загін|орган)/.test(header)){
      result.unit = idx;
      return;
    }
    if(result.reporter < 0 && /(доповідач|докладач|reporter)/.test(header)){
      result.reporter = idx;
      return;
    }
    if(result.taskType < 0 && /(тип задачі|тип завдання)/.test(header)){
      result.taskType = idx;
      return;
    }
    if(result.result < 0 && /(^результат$|статус місії|результат місії)/.test(header)){
      result.result = idx;
      return;
    }
    if(result.evacuatedCategory < 0 && /(евакуйовано)/.test(header)){
      result.evacuatedCategory = idx;
      return;
    }
    if(result.evacuatedQty < 0 && /(кількість евакуйованих)/.test(header)){
      result.evacuatedQty = idx;
      return;
    }
    if(result.circumstances < 0 && /(^обставини$|обставини місії|деталі місії)/.test(header)){
      result.circumstances = idx;
      return;
    }
    if(result.resultAt < 0 && /(дата і час результату|час результату|дата результату)/.test(header)){
      result.resultAt = idx;
      return;
    }
    if(result.startAt < 0 && /(дата і час початку місії|час початку місії|початок місії)/.test(header)){
      result.startAt = idx;
      return;
    }
    if(result.endAt < 0 && /(дата і час завершення місії|час завершення місії|завершення місії)/.test(header)){
      result.endAt = idx;
      return;
    }
    if(result.duration < 0 && /(тривалість місії|час місії|тривалість)/.test(header)){
      result.duration = idx;
      return;
    }
    if(result.cargo < 0 && /(^вантаж$|тип вантажу)/.test(header)){
      result.cargo = idx;
      return;
    }
    if(result.cargoWeight < 0 && /(вага вантажу|вага вантажу кг)/.test(header)){
      result.cargoWeight = idx;
      return;
    }
    if(result.assetStatus < 0 && /(статус засобу)/.test(header)){
      result.assetStatus = idx;
      return;
    }
    if(result.lossCircumstances < 0 && /(обставини втрати засобу|втрати засобу|причина втрати засобу)/.test(header)){
      result.lossCircumstances = idx;
      return;
    }
    if(result.asset < 0 && /(^засіб$|платформа|модель засобу)/.test(header)){
      result.asset = idx;
      return;
    }
    if(result.totalPoints < 0 && /(всього нараховано балів|нараховано балів|балів)/.test(header)){
      result.totalPoints = idx;
      return;
    }
    if(result.primaryLink < 0 && /(зв язок основний|зв’язок основний|основний звязок|основний зв язок)/.test(header)){
      result.primaryLink = idx;
      return;
    }
    if(result.reserveLink < 0 && /(зв язок резервний|зв’язок резервний|резервний звязок|резервний зв язок)/.test(header)){
      result.reserveLink = idx;
    }
  });

  const matchedCore = [
    result.unit,
    result.taskType,
    result.result,
    result.cargo,
    result.cargoWeight,
    result.assetStatus,
    result.asset,
  ].filter(idx=>idx >= 0).length;

  const width = headers.length;
  if(matchedCore < 4){
    // Delta NRK export without UUID columns
    if(width >= 28){
      if(result.unit < 0) result.unit = 1;
      if(result.taskType < 0) result.taskType = 4;
      if(result.resultAt < 0) result.resultAt = 6;
      if(result.startAt < 0) result.startAt = 8;
      if(result.endAt < 0) result.endAt = 9;
      if(result.duration < 0) result.duration = 10;
      if(result.result < 0) result.result = 12;
      if(result.circumstances < 0) result.circumstances = 13;
      if(result.cargo < 0) result.cargo = 14;
      if(result.cargoWeight < 0) result.cargoWeight = 15;
      if(result.evacuatedCategory < 0) result.evacuatedCategory = 16;
      if(result.evacuatedQty < 0) result.evacuatedQty = 17;
      if(result.assetStatus < 0) result.assetStatus = 21;
      if(result.lossCircumstances < 0) result.lossCircumstances = 22;
      if(result.asset < 0) result.asset = 23;
      if(result.primaryLink < 0) result.primaryLink = 24;
      if(result.reserveLink < 0) result.reserveLink = 25;
    }
    // Delta NRK export with UUID/reporter columns in front
    if(width >= 30 && /uuid/.test(headers[0] || "")){
      if(result.reportUuid < 0) result.reportUuid = 0;
      if(result.unit < 0) result.unit = 2;
      if(result.reporter < 0) result.reporter = 4;
      if(result.taskType < 0) result.taskType = 6;
      if(result.resultAt < 0) result.resultAt = 8;
      if(result.startAt < 0) result.startAt = 10;
      if(result.endAt < 0) result.endAt = 11;
      if(result.duration < 0) result.duration = 12;
      if(result.result < 0) result.result = 14;
      if(result.circumstances < 0) result.circumstances = 15;
      if(result.cargo < 0) result.cargo = 16;
      if(result.cargoWeight < 0) result.cargoWeight = 17;
      if(result.evacuatedCategory < 0) result.evacuatedCategory = 18;
      if(result.evacuatedQty < 0) result.evacuatedQty = 19;
      if(result.totalPoints < 0) result.totalPoints = 22;
      if(result.assetStatus < 0) result.assetStatus = 23;
      if(result.lossCircumstances < 0) result.lossCircumstances = 24;
      if(result.asset < 0) result.asset = 25;
      if(result.primaryLink < 0) result.primaryLink = 26;
      if(result.reserveLink < 0) result.reserveLink = 27;
    }
  }

  return result;

}

function buildDeltaNrkTopList(title, rows, emptyText){

  const items = Array.isArray(rows) ? rows : [];

  return `
    <div class="item analytics-block delta-nrk-list">
      <div class="row"><div class="name">${htmlesc(title)}</div></div>
      <div class="comparison-compact-grid">
        ${items.length
          ? items.map((item, index)=>`
              <div class="comparison-compact-card delta-nrk-card">
                <div class="comparison-compact-rank mono">${index + 1}</div>
                  <div class="comparison-compact-main">
                    <div class="comparison-compact-title">${htmlesc(item.label)}</div>
                    <div class="comparison-compact-meta">${htmlesc(item.meta || "")}</div>
                  </div>
                  ${renderDeltaMetricBadge(item, title)}
                </div>
              `).join("")
            : `<div class="hint">${htmlesc(emptyText || "Даних поки немає.")}</div>`
          }
      </div>
    </div>
  `;

}

function normalizeDeltaPlatformKey(value){

  const visuallyNormalized = String(value || "")
    .replace(/\u00A0/g, " ")
    .replace(/[AaАа]/g, "a")
    .replace(/[BbВв]/g, "b")
    .replace(/[CcСс]/g, "c")
    .replace(/[EeЕеЁё]/g, "e")
    .replace(/[HhНн]/g, "h")
    .replace(/[IiІіЇї]/g, "i")
    .replace(/[KkКк]/g, "k")
    .replace(/[MmМм]/g, "m")
    .replace(/[OoОо]/g, "o")
    .replace(/[PpРр]/g, "p")
    .replace(/[TtТт]/g, "t")
    .replace(/[XxХх]/g, "x")
    .replace(/[YyУу]/g, "y");

  return normalizeAnalyticsHeader(visuallyNormalized)
    .replace(/\bнрк\b/g, " ")
    .replace(/\bплатформа\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();

}

function inferDeltaMetricLabel(contextTitle="", rawValueText=""){

  const title = String(contextTitle || "").toLowerCase();
  const valueText = String(rawValueText || "").trim();
  if(!/^[\d\s.,]+$/.test(valueText)) return "";
  if(/втрат/.test(title)) return "втр.";
  if(/ціл(і|ей)?/.test(title) && !/по цілях/.test(title)) return "міс.";
  if(/по місіях|типи задач|типи цілей|статус цілей|боєприпаси|підрозділи|платформи|керування|зв’язок|зв'язок|евакуація|надійність/.test(title)) return "міс.";
  return "";

}

function renderDeltaMetricBadge(item, contextTitle=""){

  const valueText = String(item?.valueText || fmtNum(item?.value || 0));
  const valueLabel = String(item?.valueLabel || inferDeltaMetricLabel(contextTitle, valueText) || "").trim();
  const badgeClass = item?.tone || "b-blue";
  const displayText = valueLabel ? `${valueText} ${valueLabel}` : valueText;
  return `<div class="badge ${badgeClass} mono">${htmlesc(displayText)}</div>`;

}

function getDeltaMissionGroupKey(item){

  const reportUuid = String(item?.reportUuid || "").trim();
  if(reportUuid) return `uuid:${normalizeAnalyticsHeader(reportUuid) || reportUuid}`;

  const unit = normalizeAnalyticsHeader(item?.unit || "");
  const taskType = normalizeAnalyticsHeader(item?.taskType || "");
  const resultAt = normalizeAnalyticsHeader(item?.resultAt || item?.effectiveDateRaw || item?.endAt || item?.startAt || "");
  const asset = normalizeDeltaPlatformKey(item?.asset || "");

  if(unit && taskType && resultAt && asset){
    return `mission:${unit}|${taskType}|${resultAt}|${asset}`;
  }

  return `row:${item?.id || Math.random().toString(36).slice(2)}`;

}

function aggregateDeltaMissionItems(items){

  const map = new Map();

  (Array.isArray(items) ? items : []).forEach(item=>{
    const key = getDeltaMissionGroupKey(item);
    if(!map.has(key)){
      map.set(key, {
        key,
        rows: [],
        totalWeight: 0,
        maxRecordWeight: 0,
      });
    }
    const bucket = map.get(key);
    bucket.rows.push(item);
    bucket.totalWeight += Number(item?.cargoWeight) || 0;
    bucket.maxRecordWeight = Math.max(bucket.maxRecordWeight, Number(item?.cargoWeight) || 0);
    bucket.totalPoints = (bucket.totalPoints || 0) + (Number(item?.totalPoints) || 0);
  });

  const chooseByPriority = (rows, getter)=>{
    const values = rows
      .map(row=>String(getter(row) || "").trim())
      .filter(Boolean);
    if(!values.length) return "";
    const counts = new Map();
    values.forEach(value=>counts.set(value, (counts.get(value) || 0) + 1));
    return Array.from(counts.entries())
      .sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || values[0] || "";
  };

    return Array.from(map.values()).map((bucket, index)=>{
      const rows = bucket.rows;
      const durationCandidates = rows
        .map(row=>row?.providedDurationMinutes)
        .filter(value=>Number.isFinite(value) && value >= 0);
      const missionDurationMinutes = durationCandidates.length ? Math.max(...durationCandidates) : null;
      const representativeRow = rows
        .slice()
        .sort((a,b)=>(Number(b?.providedDurationMinutes) || 0) - (Number(a?.providedDurationMinutes) || 0))[0] || rows[0];
    const resultKind = rows.some(row=>getDeltaMissionResultKind(row?.result, row?.taskType) === "not_delivered")
      ? "not_delivered"
      : rows.some(row=>getDeltaMissionResultKind(row?.result, row?.taskType) === "evacuated")
        ? "evacuated"
        : rows.some(row=>getDeltaMissionResultKind(row?.result, row?.taskType) === "delivered")
          ? "delivered"
          : (rows.some(row=>getDeltaMissionResultKind(row?.result, row?.taskType) === "evac_task") ? "evac_task" : "");
    const reliabilityKind = rows.some(row=>getDeltaReliabilityKind(row?.assetStatus) === "loss")
      ? "loss"
      : rows.some(row=>getDeltaReliabilityKind(row?.assetStatus) === "damaged")
        ? "damaged"
        : (rows.some(row=>getDeltaReliabilityKind(row?.assetStatus) === "returned") ? "returned" : "");

      return {
        id: `mission_${index + 1}`,
        key: bucket.key,
        rows,
        rowCount: rows.length,
      reportUuid: String(representativeRow?.reportUuid || "").trim(),
      unit: chooseByPriority(rows, row=>row?.unit) || String(representativeRow?.unit || "").trim(),
      reporter: chooseByPriority(rows, row=>row?.reporter) || String(representativeRow?.reporter || "").trim(),
      taskType: chooseByPriority(rows, row=>row?.taskType) || String(representativeRow?.taskType || "").trim(),
      result: chooseByPriority(rows, row=>row?.result) || String(representativeRow?.result || "").trim(),
        asset: chooseByPriority(rows, row=>row?.asset) || String(representativeRow?.asset || "").trim(),
        assetStatus: chooseByPriority(rows, row=>row?.assetStatus) || String(representativeRow?.assetStatus || "").trim(),
        primaryLink: chooseByPriority(rows, row=>row?.primaryLink) || String(representativeRow?.primaryLink || "").trim(),
        reserveLink: chooseByPriority(rows, row=>row?.reserveLink) || String(representativeRow?.reserveLink || "").trim(),
        circumstances: rows.map(row=>String(row?.circumstances || "").trim()).filter(Boolean).join(" | "),
        lossCircumstances: rows.map(row=>String(row?.lossCircumstances || "").trim()).filter(Boolean).join(" | "),
      cargoWeight: bucket.totalWeight,
      totalPoints: Number(bucket.totalPoints) || 0,
      maxRecordWeight: bucket.maxRecordWeight,
      missionDurationMinutes,
      invalidTimeline: rows.some(row=>row?.invalidTimeline),
      resultAt: String(representativeRow?.resultAt || "").trim(),
      resultAtTs: representativeRow?.resultAtTs ?? null,
      startAt: String(representativeRow?.startAt || "").trim(),
      startAtTs: representativeRow?.startAtTs ?? null,
      endAt: String(representativeRow?.endAt || "").trim(),
      endAtTs: representativeRow?.endAtTs ?? null,
      effectiveDateRaw: String(representativeRow?.effectiveDateRaw || "").trim(),
      dayNightKind: representativeRow?.dayNightKind || "",
      dayMinutes: Number(representativeRow?.dayMinutes) || 0,
      nightMinutes: Number(representativeRow?.nightMinutes) || 0,
      evacuatedCategory: chooseByPriority(rows, row=>row?.evacuatedCategory) || String(representativeRow?.evacuatedCategory || "").trim(),
      evacuatedQty: rows.reduce((sum, row)=>sum + (Number(row?.evacuatedQty) || 0), 0),
      resultKind,
      reliabilityKind,
    };
  });

}

function summarizeNormalizedLabelCounts(items, getter, normalizeFn=normalizeAnalyticsHeader){

  const map = new Map();

  (Array.isArray(items) ? items : []).forEach(item=>{
    const rawLabel = String(getter(item) || "").trim();
    if(!rawLabel) return;
    const key = String(normalizeFn(rawLabel) || "").trim();
    if(!key) return;
    if(!map.has(key)){
      map.set(key, {label: rawLabel, value: 0, variants: new Map()});
    }
    const bucket = map.get(key);
    bucket.value += 1;
    bucket.variants.set(rawLabel, (bucket.variants.get(rawLabel) || 0) + 1);
  });

  return Array.from(map.values()).map(bucket=>{
    const display = Array.from(bucket.variants.entries())
      .sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || bucket.label;
    return {label: display, value: bucket.value};
  }).sort((a,b)=>b.value-a.value || String(a.label).localeCompare(String(b.label), "uk"));

}

function getDeltaReliabilityKind(value){

  const raw = String(value || "").trim();
  if(/втрата/i.test(raw)) return "loss";
  if(/пошкоджен/i.test(raw)) return "damaged";
  if(/повернення/i.test(raw)) return "returned";
  return "";

}

function getDeltaMissionResultKind(resultValue, taskTypeValue=""){

  const result = String(resultValue || "").trim();
  const taskType = String(taskTypeValue || "").trim();

  if(/не доставлено/i.test(result)) return "not_delivered";
  if(/евакуй/i.test(result)) return "evacuated";
  if(/доставлено/i.test(result)) return "delivered";
  if(/евакуац/i.test(taskType)) return "evac_task";
  return "";

}

function getDeltaEvacCargoKind(cargoValue){

  const cargo = normalizeAnalyticsHeader(cargoValue);
  if(!cargo) return "";
  if(/поранен/.test(cargo)) return "300";
  if(/загибл/.test(cargo)) return "200";
  return "other";

}

function normalizeDeltaCargoTag(value){

  const normalized = normalizeAnalyticsHeader(value);
  if(!normalized) return "";

  if(/(^бк$|боєприпас|боеприпас)/.test(normalized)) return "БК";
  if(/(їжа|вода|харч)/.test(normalized)) return "Їжа / вода";
  if(/палив/.test(normalized)) return "Паливо";
  if(/(засоби і обладнання бпла|обладнання бпла|бпла)/.test(normalized)) return "Засоби і обладнання БПЛА";
  if(/(засоби і обладнання|обладнання)/.test(normalized)) return "Засоби і обладнання";
  if(/реб/.test(normalized)) return "РЕБ";
  if(/інше/.test(normalized)) return "Інше";

  return String(value || "").trim();

}

function extractDeltaCargoTags(value){

  const raw = String(value || "")
    .replace(/\u00A0/g, " ")
    .replace(/[;|/]+/g, ",")
    .trim();
  if(!raw) return [];

  const order = [
    "БК",
    "Їжа / вода",
    "Паливо",
    "Засоби і обладнання БПЛА",
    "Засоби і обладнання",
    "РЕБ",
    "Інше",
  ];
  const orderIndex = new Map(order.map((item, index)=>[item, index]));

  const tags = raw
    .split(",")
    .map(item=>normalizeDeltaCargoTag(item))
    .filter(Boolean);

  const unique = Array.from(new Set(tags));
  return unique.sort((a,b)=>{
    const ai = orderIndex.has(a) ? orderIndex.get(a) : 999;
    const bi = orderIndex.has(b) ? orderIndex.get(b) : 999;
    if(ai !== bi) return ai - bi;
    return String(a).localeCompare(String(b), "uk");
  });

}

function splitDeltaMultiValue(value){

  return String(value || "")
    .replace(/\u00A0/g, " ")
    .split(/[;,\n]+/g)
    .map(item=>String(item || "").trim())
    .filter(Boolean);

}

function detectDeltaBplaColumns(headerRow){

  const headers = (headerRow || []).map(normalizeAnalyticsHeader);
  const result = {
    reportUuid: -1,
    unit: -1,
    taskType: -1,
    resultAt: -1,
    startAt: -1,
    endAt: -1,
    duration: -1,
    targetType: -1,
    targetDescription: -1,
    targetQty: -1,
    targetStatus: -1,
    ammoType: -1,
    ammo: -1,
    ammoQty: -1,
    cargo: -1,
    cargoQty: -1,
    cargoStatus: -1,
    asset: -1,
    assetStatus: -1,
    lossCircumstances: -1,
    controlType: -1,
    freqs: -1,
  };

  headers.forEach((header, idx)=>{
    if(result.reportUuid < 0 && /(uuid звіту|uuid звиту|^uuid$)/.test(header)){ result.reportUuid = idx; return; }
    if(result.unit < 0 && /(підрозділ|орган|загін)/.test(header)){ result.unit = idx; return; }
    if(result.taskType < 0 && /(тип задачі|тип задачи)/.test(header)){ result.taskType = idx; return; }
    if(result.resultAt < 0 && /(дата і час результату|дата та час результату)/.test(header)){ result.resultAt = idx; return; }
    if(result.startAt < 0 && /(дата і час початку місії|дата та час початку місії)/.test(header)){ result.startAt = idx; return; }
    if(result.endAt < 0 && /(дата і час завершення місії|дата та час завершення місії)/.test(header)){ result.endAt = idx; return; }
    if(result.duration < 0 && /(тривалість місії)/.test(header)){ result.duration = idx; return; }
    if(result.targetType < 0 && /(тип цілі|тип цiлi)/.test(header)){ result.targetType = idx; return; }
    if(result.targetDescription < 0 && /(опис цілі|опис цiлi)/.test(header)){ result.targetDescription = idx; return; }
    if(result.targetQty < 0 && /(кількість цілей|кiлькiсть цiлей)/.test(header)){ result.targetQty = idx; return; }
    if(result.targetStatus < 0 && /(статус цілі|статус цiлi)/.test(header)){ result.targetStatus = idx; return; }
    if(result.ammoType < 0 && /(тип використаного бк|тип використаного bk|тип боєприпасу)/.test(header)){ result.ammoType = idx; return; }
    if(result.ammo < 0 && /(^боєприпас$|^боеприпас$)/.test(header)){ result.ammo = idx; return; }
    if(result.ammoQty < 0 && /(кількість бк|кiлькiсть бк)/.test(header)){ result.ammoQty = idx; return; }
    if(result.cargo < 0 && /(^вантаж$|вантаж\/кн)/.test(header)){ result.cargo = idx; return; }
    if(result.cargoQty < 0 && /(кількість вантаж|кiлькiсть вантаж)/.test(header)){ result.cargoQty = idx; return; }
    if(result.cargoStatus < 0 && /(статус вантаж|статус вантаж\/кн)/.test(header)){ result.cargoStatus = idx; return; }
    if(result.asset < 0 && /(^засіб$|^засiб$|платформа)/.test(header)){ result.asset = idx; return; }
    if(result.assetStatus < 0 && /(статус засобу|статус засiбу)/.test(header)){ result.assetStatus = idx; return; }
    if(result.lossCircumstances < 0 && /(обставини втрати засобу|обставини втрати засiбу)/.test(header)){ result.lossCircumstances = idx; return; }
    if(result.controlType < 0 && /(тип керування)/.test(header)){ result.controlType = idx; return; }
    if(result.freqs < 0 && /(дані по частотам|данi по частотам|частот)/.test(header)){ result.freqs = idx; }
  });

  const width = headers.length;
  if(width >= 42 && /uuid/.test(headers[0] || "")){
    if(result.reportUuid < 0) result.reportUuid = 0;
    if(result.unit < 0) result.unit = 3;
    if(result.taskType < 0) result.taskType = 7;
    if(result.resultAt < 0) result.resultAt = 9;
    if(result.startAt < 0) result.startAt = 11;
    if(result.endAt < 0) result.endAt = 12;
    if(result.duration < 0) result.duration = 13;
    if(result.targetType < 0) result.targetType = 18;
    if(result.targetDescription < 0) result.targetDescription = 19;
    if(result.targetQty < 0) result.targetQty = 20;
    if(result.targetStatus < 0) result.targetStatus = 21;
    if(result.ammoType < 0) result.ammoType = 23;
    if(result.ammo < 0) result.ammo = 24;
    if(result.ammoQty < 0) result.ammoQty = 25;
    if(result.cargo < 0) result.cargo = 27;
    if(result.cargoQty < 0) result.cargoQty = 28;
    if(result.cargoStatus < 0) result.cargoStatus = 29;
    if(result.asset < 0) result.asset = 34;
    if(result.assetStatus < 0) result.assetStatus = 35;
    if(result.lossCircumstances < 0) result.lossCircumstances = 36;
    if(result.controlType < 0) result.controlType = 38;
    if(result.freqs < 0) result.freqs = 39;
  }

  return result;

}

function aggregateDeltaBplaMissionItems(items){

  const map = new Map();

  (Array.isArray(items) ? items : []).forEach(item=>{
    const key = getDeltaMissionGroupKey(item);
    if(!map.has(key)){
      map.set(key, {key, rows: []});
    }
    map.get(key).rows.push(item);
  });

  const chooseByPriority = (rows, getter)=>{
    const values = rows.map(row=>String(getter(row) || "").trim()).filter(Boolean);
    if(!values.length) return "";
    const counts = new Map();
    values.forEach(value=>counts.set(value, (counts.get(value) || 0) + 1));
    return Array.from(counts.entries())
      .sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || values[0] || "";
  };

  const collectUniqueTags = (rows, getter)=>{
    const map = new Map();
    rows.forEach(row=>{
      splitDeltaMultiValue(getter(row)).forEach(tag=>{
        const key = normalizeAnalyticsHeader(tag) || tag;
        if(!map.has(key)) map.set(key, tag);
      });
    });
    return Array.from(map.values());
  };

  return Array.from(map.values()).map((bucket, index)=>{
    const rows = bucket.rows;
    const durationCandidates = rows
      .map(row=>row?.providedDurationMinutes)
      .filter(value=>Number.isFinite(value) && value >= 0);
    const missionDurationMinutes = durationCandidates.length ? Math.max(...durationCandidates) : null;
    const representativeRow = rows
      .slice()
      .sort((a,b)=>(Number(b?.providedDurationMinutes) || 0) - (Number(a?.providedDurationMinutes) || 0))[0] || rows[0];
    const targetTypes = collectUniqueTags(rows, row=>row?.targetType);
    const targetStatuses = collectUniqueTags(rows, row=>row?.targetStatus);
    const ammoTypes = collectUniqueTags(rows, row=>row?.ammoType);
    const ammoNames = collectUniqueTags(rows, row=>row?.ammo);
    const controlTypes = collectUniqueTags(rows, row=>row?.controlType);
    const cargoStatuses = collectUniqueTags(rows, row=>row?.cargoStatus);
    const targetCount = rows.reduce((sum, row)=>sum + (Number(row?.targetQty) || 0), 0);
    const ammoQty = rows.reduce((sum, row)=>sum + (Number(row?.ammoQty) || 0), 0);
    const cargoQty = rows.reduce((sum, row)=>sum + (Number(row?.cargoQty) || 0), 0);
    const reliabilityKind = rows.some(row=>getDeltaReliabilityKind(row?.assetStatus) === "loss")
      ? "loss"
      : (rows.some(row=>getDeltaReliabilityKind(row?.assetStatus) === "returned") ? "returned" : "");

    return {
      id: `bpla_mission_${index + 1}`,
      key: bucket.key,
      rows,
      rowCount: rows.length,
      reportUuid: String(representativeRow?.reportUuid || "").trim(),
      unit: chooseByPriority(rows, row=>row?.unit) || String(representativeRow?.unit || "").trim(),
      taskType: chooseByPriority(rows, row=>row?.taskType) || String(representativeRow?.taskType || "").trim(),
      asset: chooseByPriority(rows, row=>row?.asset) || String(representativeRow?.asset || "").trim(),
      assetStatus: chooseByPriority(rows, row=>row?.assetStatus) || String(representativeRow?.assetStatus || "").trim(),
      controlType: chooseByPriority(rows, row=>row?.controlType) || String(representativeRow?.controlType || "").trim(),
      freqs: chooseByPriority(rows, row=>row?.freqs) || String(representativeRow?.freqs || "").trim(),
      targetTypes,
      targetStatuses,
      ammoTypes,
      ammoNames,
      cargoStatuses,
      targetCount,
      ammoQty,
      cargoQty,
      lossCircumstances: rows.map(row=>String(row?.lossCircumstances || "").trim()).filter(Boolean).join(" | "),
      targetDescription: rows.map(row=>String(row?.targetDescription || "").trim()).filter(Boolean).join(" | "),
      missionDurationMinutes,
      invalidTimeline: rows.some(row=>row?.invalidTimeline),
      resultAt: String(representativeRow?.resultAt || "").trim(),
      resultAtTs: representativeRow?.resultAtTs ?? null,
      startAt: String(representativeRow?.startAt || "").trim(),
      startAtTs: representativeRow?.startAtTs ?? null,
      endAt: String(representativeRow?.endAt || "").trim(),
      endAtTs: representativeRow?.endAtTs ?? null,
      effectiveDateRaw: String(representativeRow?.effectiveDateRaw || "").trim(),
      dayNightKind: representativeRow?.dayNightKind || "",
      dayMinutes: Number(representativeRow?.dayMinutes) || 0,
      nightMinutes: Number(representativeRow?.nightMinutes) || 0,
      reliabilityKind,
    };
  });

}

function buildDeltaNrkInsightModalHtml(sections){

  const blocks = Array.isArray(sections) ? sections.filter(Boolean) : [];

  return `
    <div class="delta-nrk-insight-modal">
      ${blocks.map(section=>`
        <div class="item analytics-block delta-nrk-list">
          <div class="row">
            <div class="name">${htmlesc(section.title || "Аналітика")}</div>
            ${section.summary ? `<div class="hint">${htmlesc(section.summary)}</div>` : ""}
          </div>
          ${section.clickHint ? `<div class="hint delta-nrk-click-hint">${htmlesc(section.clickHint)}</div>` : ""}
          <div class="comparison-compact-grid">
            ${(section.rows || []).length
              ? section.rows.map((item, index)=>{
                    const cardInner = `
                      <div class="comparison-compact-rank mono">${index + 1}</div>
                      <div class="comparison-compact-main">
                        <div class="comparison-compact-title">${htmlesc(item.label)}</div>
                        <div class="comparison-compact-meta">${htmlesc(item.meta || "")}${item.modalKey ? ` · Відкрити місії` : ""}</div>
                      </div>
                      ${renderDeltaMetricBadge(item, section.title || "Аналітика")}
                    `;
                  return item.modalKey
                    ? `<button type="button" class="comparison-compact-card comparison-card-btn delta-nrk-card" data-action="openRenderedTableModal" data-arg1="${item.modalKey}">${cardInner}</button>`
                    : `<div class="comparison-compact-card delta-nrk-card">${cardInner}</div>`;
                }).join("")
              : `<div class="hint">${htmlesc(section.emptyText || "Даних поки немає.")}</div>`
            }
          </div>
        </div>
      `).join("")}
    </div>
  `;

}

function attrEsc(s){

  return htmlesc(s);

}

function buildDeltaNrkAutoSummaryHtml(analytics){

  if(!analytics) return "";

  const deliveredPercent = analytics.deliverySuccessRate || 0;
  const logisticsMissionCount = Math.max(0, Number(analytics.missionCount || 0) - Number(analytics.evacuationCount || 0));
  const evacuationSuccessRate = analytics.evacuationCount ? Math.round((Number(analytics.evacuatedCount || 0) / Math.max(1, Number(analytics.evacuationCount || 0))) * 1000) / 10 : 0;
  const cards = [
      {
        label:"Місій",
        value: fmtNum(analytics.missionCount),
        text: `${fmtNum(analytics.recordCount)} записів у зрізі`,
      },
      {
        label:"Платформа",
        value: analytics.topAsset?.label || "—",
        text: analytics.topAsset ? `${fmtNum(analytics.topAsset.value)} місій` : "Немає даних",
      },
      {
        label:"Логістика",
        value: fmtNum(analytics.successCount),
        text: logisticsMissionCount
          ? `${fmtNum(logisticsMissionCount)} місій · ${fmtNum(deliveredPercent)}% успішно · Не дост. ${fmtNum(analytics.notDeliveredCount)}`
          : "Логістичних місій немає",
        tone: analytics.notDeliveredCount > 0 ? "warn" : "",
      },
      ...(analytics.evacuationCount > 0 ? [{
        label:"Евакуація",
        value: fmtNum(analytics.evacuationCount),
        text: `${fmtNum(evacuationSuccessRate)}% успішно · 300: ${fmtNum(analytics.evacuation300Count)} · 200: ${fmtNum(analytics.evacuation200Count)}`,
        tone: "ok",
      }] : []),
      {
        label:"Вага",
        value: analytics.totalWeight > 0 ? `${fmtNum(analytics.totalWeight)} кг` : "—",
        text: analytics.avgWeight > 0 ? `Сер. ${fmtNum(analytics.avgWeight)} кг на місію` : "Немає даних",
      },
      {
        label:"Макс. вантаж",
        value: analytics.maxCargoAsset?.maxRecordWeight > 0 ? `${fmtNum(analytics.maxCargoAsset.maxRecordWeight)} кг` : "—",
        text: analytics.maxCargoAsset
          ? `${analytics.maxCargoAsset.label}`
          : "Немає даних",
      },
      {
        label:"Надійність",
        value: `${fmtNum(analytics.reliabilityRate)}%`,
        text: `Повернення ${fmtNum(analytics.returnedCount)} · Пошк. ${fmtNum(analytics.damagedCount)} · Втрати ${fmtNum(analytics.lossCount)}`,
        tone: (analytics.lossCount > 0 || analytics.damagedCount > 0) ? "danger" : "ok",
      },
    ];

  return `
    <div class="delta-nrk-summary-row">
      ${cards.map(card=>`
        <div class="delta-nrk-summary-card ${card.tone ? `is-${card.tone}` : ""}">
          <div class="delta-nrk-summary-k">${card.label}</div>
          <div class="delta-nrk-summary-v">${card.value}</div>
          <div class="delta-nrk-summary-s">${card.text}</div>
        </div>
      `).join("")}
    </div>
  `;

}

function buildDeltaNrkPrimaryKpisHtml(analytics){

  if(!analytics) return "";

  const deliveredPercent = analytics.deliverySuccessRate || 0;
  const cards = [
    {label:"Місій", value: fmtNum(analytics.missionCount), text:`${fmtNum(analytics.recordCount)} записів`},
    {label:"Доставлено", value: fmtNum(analytics.deliveredCount), text:`${fmtNum(deliveredPercent)}% успішно`},
    {label:"Не доставлено", value: fmtNum(analytics.notDeliveredCount), text:"Проблемні місії"},
    {label:"Евакуаційні", value: fmtNum(analytics.evacuationCount), text:"Окремий тип задач"},
    {label:"Загальна вага", value: `${fmtNum(analytics.totalWeight)} кг`, text:"Усі перевезення"},
    {label:"Сер. вага", value: `${fmtNum(analytics.avgWeight)} кг`, text:"На одну місію з вагою"},
    {label:"Втрати", value: fmtNum(analytics.lossCount), text:`${fmtNum(analytics.reliabilityRate)}% надійність`},
  ];

  return `
    <div class="delta-nrk-primary-kpis">
      ${cards.map(card=>`
        <div class="delta-nrk-primary-card">
          <div class="delta-nrk-primary-k">${card.label}</div>
          <div class="delta-nrk-primary-v mono">${card.value}</div>
          <div class="delta-nrk-primary-s">${card.text}</div>
        </div>
      `).join("")}
    </div>
  `;

}

function parseDeltaMonthKey(value=""){

  const raw = String(value || "").trim();
  if(!raw) return "";

  let match = raw.match(/(\d{1,2})[./-](\d{1,2})[./-](\d{2})(?:[,\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if(match){
    const year = Number(match[3]);
    const fullYear = year >= 70 ? (1900 + year) : (2000 + year);
    return `${fullYear}-${String(match[2]).padStart(2, "0")}`;
  }

  match = raw.match(/(\d{4})[./-](\d{1,2})[./-](\d{1,2})/);
  if(match){
    return `${match[1]}-${String(match[2]).padStart(2, "0")}`;
  }

  match = raw.match(/(\d{1,2})[./-](\d{1,2})[./-](\d{4})/);
  if(match){
    return `${match[3]}-${String(match[2]).padStart(2, "0")}`;
  }

  match = raw.match(/(\d{4})[./-](\d{1,2})/);
  if(match){
    return `${match[1]}-${String(match[2]).padStart(2, "0")}`;
  }

  return "";

}

function formatDeltaMonthLabel(key=""){

  const raw = String(key || "").trim();
  if(!raw) return "Без дати";

  const match = raw.match(/^(\d{4})-(\d{2})$/);
  if(!match) return raw;

  const monthNames = ["січ", "лют", "бер", "квіт", "трав", "черв", "лип", "серп", "вер", "жовт", "лист", "груд"];
  const monthIndex = Math.max(1, Math.min(12, Number(match[2] || 1))) - 1;
  return `${monthNames[monthIndex]} ${match[1]}`;

}

function parseDeltaDateTimeValue(value){

  if(value instanceof Date){
    const ts = value.getTime();
    return Number.isFinite(ts) ? ts : null;
  }

  const raw = String(value || "").replace(/\u00A0/g, " ").trim();
  if(!raw) return null;

  let match = raw.match(/(\d{1,2})[./-](\d{1,2})[./-](\d{2})(?:[,\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if(match){
    const year = Number(match[3]);
    const fullYear = year >= 70 ? (1900 + year) : (2000 + year);
    const dt = new Date(
      fullYear,
      Number(match[2]) - 1,
      Number(match[1]),
      Number(match[4] || 0),
      Number(match[5] || 0),
      Number(match[6] || 0)
    );
    const ts = dt.getTime();
    return Number.isFinite(ts) ? ts : null;
  }

  match = raw.match(/(\d{1,2})[./-](\d{1,2})[./-](\d{4})(?:[,\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if(match){
    const dt = new Date(
      Number(match[3]),
      Number(match[2]) - 1,
      Number(match[1]),
      Number(match[4] || 0),
      Number(match[5] || 0),
      Number(match[6] || 0)
    );
    const ts = dt.getTime();
    return Number.isFinite(ts) ? ts : null;
  }

  match = raw.match(/(\d{4})[./-](\d{1,2})[./-](\d{1,2})(?:[,\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if(match){
    const dt = new Date(
      Number(match[1]),
      Number(match[2]) - 1,
      Number(match[3]),
      Number(match[4] || 0),
      Number(match[5] || 0),
      Number(match[6] || 0)
    );
    const ts = dt.getTime();
    return Number.isFinite(ts) ? ts : null;
  }

  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : null;

}

function parseDeltaDurationMinutes(value){

  const raw = String(value || "").replace(/\u00A0/g, " ").trim();
  if(!raw) return null;

  let match = raw.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if(match){
    const hours = Number(match[1] || 0);
    const mins = Number(match[2] || 0);
    const secs = Number(match[3] || 0);
    return Math.round((hours * 3600 + mins * 60 + secs) / 60);
  }

  const hoursMatch = raw.match(/(\d+(?:[.,]\d+)?)\s*год/iu);
  const minsMatch = raw.match(/(\d+(?:[.,]\d+)?)\s*(хв|min|мин)/iu);
  if(hoursMatch || minsMatch){
    const hours = Number(String(hoursMatch?.[1] || "0").replace(",", "."));
    const mins = Number(String(minsMatch?.[1] || "0").replace(",", "."));
    const total = (Number.isFinite(hours) ? hours * 60 : 0) + (Number.isFinite(mins) ? mins : 0);
    return total > 0 ? Math.round(total) : null;
  }

  if(/^\d+(?:[.,]\d+)?$/.test(raw)){
    const num = Number(String(raw).replace(",", "."));
    return Number.isFinite(num) ? Number(num) : null;
  }

  return null;

}

function formatDurationMinutes(minutes){

  const total = Math.round(Number(minutes) || 0);
  if(total <= 0) return "—";
  if(total < 60) return `${fmtNum(total)} хв`;
  const hours = Math.floor(total / 60);
  const mins = total % 60;
  return mins ? `${fmtNum(hours)} год ${fmtNum(mins)} хв` : `${fmtNum(hours)} год`;

}

function getDeltaDayNightKindForTimestamp(ts){

  if(!Number.isFinite(ts)) return "";
  const dt = new Date(ts);
  const hour = dt.getHours();
  return (hour >= 6 && hour < 22) ? "day" : "night";

}

function splitDeltaDayNight(startTs, endTs, fallbackTs){

  if(Number.isFinite(startTs) && Number.isFinite(endTs) && endTs >= startTs){
    let dayMinutes = 0;
    let nightMinutes = 0;
    let cursor = startTs;

    while(cursor < endTs){
      const current = new Date(cursor);
      const dayStart = new Date(current.getFullYear(), current.getMonth(), current.getDate(), 6, 0, 0, 0).getTime();
      const dayEnd = new Date(current.getFullYear(), current.getMonth(), current.getDate(), 22, 0, 0, 0).getTime();
      const nextMidnight = new Date(current.getFullYear(), current.getMonth(), current.getDate() + 1, 0, 0, 0, 0).getTime();

      let segmentEnd = endTs;
      if(cursor < dayStart){
        segmentEnd = Math.min(endTs, dayStart);
        nightMinutes += Math.max(0, Math.round((segmentEnd - cursor) / 60000));
      } else if(cursor < dayEnd){
        segmentEnd = Math.min(endTs, dayEnd);
        dayMinutes += Math.max(0, Math.round((segmentEnd - cursor) / 60000));
      } else {
        segmentEnd = Math.min(endTs, nextMidnight);
        nightMinutes += Math.max(0, Math.round((segmentEnd - cursor) / 60000));
      }

      if(segmentEnd <= cursor) break;
      cursor = segmentEnd;
    }

    const kind = dayMinutes > nightMinutes ? "day" : "night";
    return {kind, dayMinutes, nightMinutes, totalMinutes: dayMinutes + nightMinutes};
  }

  const refTs = Number.isFinite(startTs) ? startTs : (Number.isFinite(fallbackTs) ? fallbackTs : (Number.isFinite(endTs) ? endTs : null));
  if(!Number.isFinite(refTs)) return {kind:"", dayMinutes:0, nightMinutes:0, totalMinutes:0};

  const kind = getDeltaDayNightKindForTimestamp(refTs);
  return {
    kind,
    dayMinutes: kind === "day" ? 1 : 0,
    nightMinutes: kind === "night" ? 1 : 0,
    totalMinutes: 1,
  };

}

function buildDeltaNrkDayNightHtml(analytics){

  const block = analytics?.dayNight;
  if(!block?.units?.length && !block?.assets?.length) return "";

  const renderSection = (sectionTitle, items, modalTitle)=>{
    if(!items?.length) return "";
    const modalRows = items.map(item=>({
      label: item.label,
      valueText: fmtNum(item.total),
      meta: `День ${fmtNum(item.dayCount)} · Ніч ${fmtNum(item.nightCount)} · Нічні ${fmtNum(item.nightPercent)}%`,
      tone: item.nightPercent >= 50 ? "b-violet" : "b-blue",
    }));
    const detailKey = registerRenderedTableModal(
      modalTitle,
      buildDeltaNrkInsightModalHtml([
        {
          title: sectionTitle,
          summary: `День: ${fmtNum(block.dayCount)} · Ніч: ${fmtNum(block.nightCount)} · Нічні місії: ${fmtNum(block.nightPercent)}%`,
          rows: modalRows,
          emptyText: "По часу місій даних поки немає.",
        }
      ])
    );

    return `
      <div class="delta-nrk-daynight-section">
        <div class="row">
          <div class="name">${htmlesc(sectionTitle)}</div>
          <button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${detailKey}">Детальніше</button>
        </div>
        <div class="delta-nrk-daynight-list">
          ${items.slice(0, 8).map(item=>`
            <div class="delta-nrk-daynight-card">
              <div class="delta-nrk-daynight-head">
                <div class="delta-nrk-daynight-name">${htmlesc(item.label)}</div>
                <div class="delta-nrk-daynight-total mono">${fmtNum(item.total)}</div>
              </div>
              <div class="delta-nrk-daynight-meta">День ${fmtNum(item.dayCount)} · Ніч ${fmtNum(item.nightCount)} · Нічні ${fmtNum(item.nightPercent)}%</div>
              <div class="delta-nrk-daynight-bar">
                <div class="delta-nrk-daynight-day" style="width:${Math.max(0, Math.min(100, item.dayPercent))}%;"></div>
                <div class="delta-nrk-daynight-night" style="width:${Math.max(0, Math.min(100, item.nightPercent))}%;"></div>
              </div>
            </div>
          `).join("")}
        </div>
      </div>
    `;
  };

  const overallDonut = renderComparisonSliceDonutCard(
    "День / ніч",
    [
      {label:"Денні", value: block.dayCount},
      {label:"Нічні", value: block.nightCount},
    ],
    "Не вдалося визначити час місій.",
    ["#4f88ff", "#1f2d4a"]
  );

  return `
    <div class="item analytics-block delta-nrk-daynight">
      <div class="row"><div class="name">День / ніч · по місіях</div></div>
      <div class="delta-nrk-daynight-grid">
        <div class="delta-nrk-daynight-donut">${overallDonut}</div>
        <div class="delta-nrk-daynight-sections">
          ${renderSection("День / ніч по підрозділах", block.units, `${analytics.title || "Delta / НРК"} · День / ніч · Підрозділи`)}
          ${renderSection("День / ніч по платформах", block.assets, `${analytics.title || "Delta / НРК"} · День / ніч · Платформи`)}
        </div>
      </div>
    </div>
  `;

}

function buildDeltaNrkTimeQualityHtml(analytics){

  const q = analytics?.timeQuality;
  if(!q) return "";

  const durationBaseCount = Number(q.durationBaseCount || q.durationFilledCount || 0);
  const fillItems = [
    {label:"Початок місії", value:q.startFilledCount, percent:q.startFilledPercent, baseCount:q.totalCount},
    {label:"Завершення місії", value:q.endFilledCount, percent:q.endFilledPercent, baseCount:q.totalCount},
    {label:"Час результату", value:q.resultFilledCount, percent:q.resultFilledPercent, baseCount:q.totalCount},
    {label:"Тривалість місії", value:q.durationFilledCount, percent:q.durationFilledPercent, baseCount:q.totalCount},
    {label:"Зв'язок Основний", value:q.primaryLinkWithDurationCount, percent:q.primaryLinkWithDurationPercent, baseCount:durationBaseCount, baseLabel:"місій із тривалістю"},
    {label:"Зв'язок Резервний", value:q.reserveLinkWithDurationCount, percent:q.reserveLinkWithDurationPercent, baseCount:durationBaseCount, baseLabel:"місій із тривалістю"},
    {label:"Повний часовий ланцюжок", value:q.fullTimelineCount, percent:q.fullTimelinePercent, baseCount:q.totalCount},
  ];

  const qualityDonut = renderComparisonSliceDonutCard(
    "Якість заповнення часу",
    [
      {label:"Повний ланцюжок", value: q.fullTimelineCount},
      {label:"Частково заповнено", value: Math.max(0, q.anyTimeCount - q.fullTimelineCount)},
      {label:"Без часу", value: Math.max(0, q.totalCount - q.anyTimeCount)},
    ],
    "По часових полях даних поки немає.",
    ["#4f88ff", "#ffb547", "#d5deef"]
  );

  const durationDonut = renderComparisonSliceDonutCard(
    "Тривалість місій",
    [
      {label:"Валідно пораховано", value: q.validDurationCount},
      {label:"Некоректні часові пари", value: q.invalidTimelineCount},
      {label:"Без тривалості", value: Math.max(0, (q.missionTotalCount || 0) - q.validDurationCount - q.invalidTimelineCount)},
    ],
    "Тривалість місій поки не визначена.",
    ["#6bc46d", "#ff7b87", "#d5deef"]
  );

  return `
    <div class="item analytics-block delta-nrk-time-quality">
      <div class="row">
        <div class="name">Якість заповнення даних</div>
        <div class="hint">Заповнення і тривалість рахуються по унікальних місіях.</div>
      </div>
      <div class="report-grid staffing-analytics-kpis delta-nrk-kpis delta-nrk-time-kpis">
        <div class="report-tile"><div class="k">Початок</div><div class="v mono">${fmtNum(q.startFilledPercent)}%</div><div class="s">${fmtNum(q.startFilledCount)} із ${fmtNum(q.totalCount)}</div></div>
        <div class="report-tile"><div class="k">Завершення</div><div class="v mono">${fmtNum(q.endFilledPercent)}%</div><div class="s">${fmtNum(q.endFilledCount)} із ${fmtNum(q.totalCount)}</div></div>
        <div class="report-tile"><div class="k">Повний ланцюжок</div><div class="v mono">${fmtNum(q.fullTimelinePercent)}%</div><div class="s">${fmtNum(q.fullTimelineCount)} місій</div></div>
        <div class="report-tile"><div class="k">Сер. час місії</div><div class="v mono">${htmlesc(formatDurationMinutes(q.avgDurationMinutes))}</div><div class="s">${fmtNum(q.validDurationCount)} місій із тривалістю</div></div>
      </div>
      <div class="eval-donut-grid">
        ${qualityDonut}
        ${durationDonut}
      </div>
      <div class="delta-nrk-fill-grid">
        ${fillItems.map(item=>`
          <div class="delta-nrk-fill-card">
            <div class="delta-nrk-fill-top">
              <div class="delta-nrk-fill-label">${htmlesc(item.label)}</div>
              <div class="delta-nrk-fill-value mono">${fmtNum(item.percent)}%</div>
            </div>
            <div class="delta-nrk-fill-bar"><div class="delta-nrk-fill-bar-in" style="width:${Math.max(0, Math.min(100, item.percent))}%;"></div></div>
            <div class="delta-nrk-fill-meta">${fmtNum(item.value)} із ${fmtNum(item.baseCount || q.totalCount)} ${htmlesc(item.baseLabel || "місій")}</div>
          </div>
        `).join("")}
      </div>
      <div class="delta-nrk-time-summary">
        <span class="delta-nrk-filter-chip">Медіана: ${htmlesc(formatDurationMinutes(q.medianDurationMinutes))}</span>
        <span class="delta-nrk-filter-chip">Макс: ${htmlesc(formatDurationMinutes(q.maxDurationMinutes))}</span>
        <span class="delta-nrk-filter-chip">Некоректні пари часу: ${fmtNum(q.invalidTimelineCount)}</span>
      </div>
    </div>
  `;

}

function buildDeltaNrkMonthlyAnalyticsHtml(analytics){

  if(!analytics?.months?.length) return "";

  const maxMissionCount = Math.max(1, ...analytics.months.map(item=>Number(item.missionCount) || 0));
  const formatDelta = (value, suffix="")=>{
    const num = Number(value) || 0;
    if(Math.abs(num) < 0.000001) return `без змін${suffix}`;
    return `${num > 0 ? "+" : ""}${fmtNum(num)}${suffix}`;
  };
  const getDeltaTone = value=>{
    const num = Number(value) || 0;
    if(num > 0) return "is-up";
    if(num < 0) return "is-down";
    return "is-flat";
  };

  return `
    <div class="item analytics-block delta-monthly-block">
      <div class="row delta-monthly-block-head">
        <div class="name">Аналітика по місяцях</div>
      </div>
      <div class="delta-monthly-grid delta-monthly-rich-grid">
        ${analytics.months.map((item, index)=>{
          const prev = index > 0 ? analytics.months[index - 1] : null;
          const missionDelta = prev ? ((Number(item.missionCount) || 0) - (Number(prev.missionCount) || 0)) : null;
          const weightDelta = prev ? ((Number(item.totalWeight) || 0) - (Number(prev.totalWeight) || 0)) : null;
          const successDelta = prev ? ((Number(item.successRate) || 0) - (Number(prev.successRate) || 0)) : null;
          const lossesDelta = prev ? ((Number(item.lossCount) || 0) - (Number(prev.lossCount) || 0)) : null;
          return `
            <div class="delta-monthly-card tone-blue delta-monthly-rich-card">
              <div class="delta-monthly-head">
                <div class="delta-monthly-month">${htmlesc(item.label)}</div>
                <div class="delta-monthly-value mono">Всього місій ${fmtNum(item.missionCount)}</div>
              </div>
              <div class="delta-monthly-statline">
                <span class="delta-monthly-statlabel">Доставлено</span> <strong>${fmtNum(item.deliveredCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Не доставлено</span> <strong>${fmtNum(item.notDeliveredCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Евакуація</span> <strong>${fmtNum(item.evacuationCount)}</strong>
              </div>
              <div class="delta-monthly-statline">
                <span class="delta-monthly-statlabel">Вага</span> <strong>${fmtNum(item.totalWeight)} кг</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Сер. вага</span> <strong>${fmtNum(item.avgWeight)} кг</strong>
              </div>
              <div class="delta-monthly-statline">
                <span class="delta-monthly-statlabel">Втрати</span> <strong>${fmtNum(item.lossCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Пошкоджено</span> <strong>${fmtNum(item.damagedCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Успішність</span> <strong>${fmtNum(item.successRate)}%</strong>
              </div>
              <div class="delta-monthly-delta-grid">
                <div class="delta-monthly-delta-chip ${prev ? getDeltaTone(missionDelta) : "is-flat"}">Місії: ${prev ? formatDelta(missionDelta) : "—"}</div>
                <div class="delta-monthly-delta-chip ${prev ? getDeltaTone(weightDelta) : "is-flat"}">Вага: ${prev ? formatDelta(weightDelta, " кг") : "—"}</div>
                <div class="delta-monthly-delta-chip ${prev ? getDeltaTone(successDelta) : "is-flat"}">Успішність: ${prev ? formatDelta(successDelta, " п.п.") : "—"}</div>
                <div class="delta-monthly-delta-chip ${prev ? getDeltaTone(-lossesDelta) : "is-flat"}">Втрати: ${prev ? formatDelta(lossesDelta) : "—"}</div>
              </div>
            </div>
          `;
        }).join("")}
      </div>
    </div>
  `;

}

function buildDeltaNrkExecutiveReportText(analytics){

  if(!analytics) return "";

  const lines = [];
  const scopeParts = [];
  if(analytics.filters?.unit) scopeParts.push(`підрозділ: ${analytics.filters.unit}`);
  if(analytics.filters?.taskType) scopeParts.push(`тип задачі: ${analytics.filters.taskType}`);
  if(analytics.filters?.asset) scopeParts.push(`платформа: ${analytics.filters.asset}`);

  const lastMonth = Array.isArray(analytics.months) && analytics.months.length ? analytics.months[analytics.months.length - 1] : null;
  const prevMonth = Array.isArray(analytics.months) && analytics.months.length > 1 ? analytics.months[analytics.months.length - 2] : null;
  const monthMissionDelta = lastMonth && prevMonth ? (Number(lastMonth.missionCount || 0) - Number(prevMonth.missionCount || 0)) : 0;
  const monthWeightDelta = lastMonth && prevMonth ? (Number(lastMonth.totalWeight || 0) - Number(prevMonth.totalWeight || 0)) : 0;
  const monthSuccessDelta = lastMonth && prevMonth ? (Number(lastMonth.successRate || 0) - Number(prevMonth.successRate || 0)) : 0;

  const reporterMissingCount = analytics.missions.filter(item=>!String(item.reporter || "").trim()).length;
  const noPrimaryLinkCount = analytics.missions.filter(item=>!String(item.primaryLink || "").trim()).length;
  const noReserveLinkCount = analytics.missions.filter(item=>!String(item.reserveLink || "").trim()).length;
  const noAnyLinkCount = analytics.missions.filter(item=>!String(item.primaryLink || "").trim() && !String(item.reserveLink || "").trim()).length;
  const noWeightCount = analytics.missions.filter(item=>(Number(item.cargoWeight) || 0) <= 0).length;
  const noResultCount = analytics.missions.filter(item=>!String(item.result || "").trim()).length;
  const topAsset = analytics.topAsset?.label ? `${analytics.topAsset.label} (${fmtNum(analytics.topAsset.value)} місій)` : "—";
  const topUnit = analytics.unitsByMissions?.[0]?.label ? `${analytics.unitsByMissions[0].label} (${fmtNum(analytics.unitsByMissions[0].value)} місій)` : "—";
  const topPointsUnit = analytics.pointsByUnits?.[0]?.label ? `${analytics.pointsByUnits[0].label} (${fmtNum(analytics.pointsByUnits[0].value)} бал.)` : "—";
  const topEfficiencyUnit = analytics.pointsByUnits?.[0]?.label ? `${analytics.pointsByUnits.slice().sort((a,b)=>b.avgPoints-a.avgPoints || b.value-a.value)[0]?.label || "—"} (${fmtNum(analytics.pointsByUnits.slice().sort((a,b)=>b.avgPoints-a.avgPoints || b.value-a.value)[0]?.avgPoints || 0)} бал./місію)` : "—";
  const unitMissionMap = new Map((analytics.unitsByMissions || []).map(item=>[String(item.label || "").trim(), Number(item.value) || 0]));
  const unitWeightMap = new Map((analytics.unitsByWeight || []).map(item=>[String(item.label || "").trim(), Number(item.value) || 0]));
  const unitsWithIssues = Array.from(new Set(
    analytics.missions
      .filter(item=>item.reliabilityKind === "loss" || item.reliabilityKind === "damaged" || item.resultKind === "not_delivered")
      .map(item=>String(item.unit || "").trim())
      .filter(Boolean)
  ))
    .map(label=>{
      const missions = analytics.missions.filter(item=>String(item.unit || "").trim() === label);
      const losses = missions.filter(item=>item.reliabilityKind === "loss").length;
      const damaged = missions.filter(item=>item.reliabilityKind === "damaged").length;
      const notDelivered = missions.filter(item=>item.resultKind === "not_delivered").length;
      return {label, missions: missions.length, losses, damaged, notDelivered};
    })
    .sort((a,b)=>(b.losses + b.damaged + b.notDelivered) - (a.losses + a.damaged + a.notDelivered) || b.missions - a.missions || String(a.label).localeCompare(String(b.label), "uk"));
  const topProblemUnit = unitsWithIssues[0] || null;
  const assetMissionMap = new Map();
  analytics.missions.forEach(item=>{
    const label = String(item.asset || "").trim();
    if(!label) return;
    if(!assetMissionMap.has(label)){
      assetMissionMap.set(label, {label, missions:0, losses:0, damaged:0, notDelivered:0});
    }
    const bucket = assetMissionMap.get(label);
    bucket.missions += 1;
    if(item.reliabilityKind === "loss") bucket.losses += 1;
    if(item.reliabilityKind === "damaged") bucket.damaged += 1;
    if(item.resultKind === "not_delivered") bucket.notDelivered += 1;
  });
  const assetRisks = Array.from(assetMissionMap.values())
    .sort((a,b)=>(b.losses + b.damaged + b.notDelivered) - (a.losses + a.damaged + a.notDelivered) || b.missions - a.missions || String(a.label).localeCompare(String(b.label), "uk"));
  const topRiskAsset = assetRisks[0] || null;
  const topWeightUnitEntry = Array.from(unitWeightMap.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0] || null;
  const topWeightUnit = topWeightUnitEntry ? `${topWeightUnitEntry[0]} (${fmtNum(topWeightUnitEntry[1])} кг)` : "—";
  const topPointsAsset = analytics.pointsByAssets?.[0]?.label ? `${analytics.pointsByAssets[0].label} (${fmtNum(analytics.pointsByAssets[0].value)} бал.)` : "—";
  const topEfficiencyAsset = analytics.pointsByAssets?.length
    ? `${analytics.pointsByAssets.slice().sort((a,b)=>b.avgPoints-a.avgPoints || b.value-a.value)[0]?.label || "—"} (${fmtNum(analytics.pointsByAssets.slice().sort((a,b)=>b.avgPoints-a.avgPoints || b.value-a.value)[0]?.avgPoints || 0)} бал./місію)`
    : "—";

  lines.push(`Короткий звіт Delta / НРК${scopeParts.length ? ` (${scopeParts.join("; ")})` : ""}`);
  lines.push("");
  lines.push("1. Загальна картина");
  lines.push(`- Унікальних місій: ${fmtNum(analytics.missionCount)}; технічних записів: ${fmtNum(analytics.recordCount)}.`);
  lines.push(`- Логістичних місій: ${fmtNum(Math.max(0, analytics.missionCount - analytics.evacuationCount))}; евакуаційних: ${fmtNum(analytics.evacuationCount)}.`);
  lines.push(`- Доставлено: ${fmtNum(analytics.deliveredCount)}; не доставлено: ${fmtNum(analytics.notDeliveredCount)}; успішність логістики: ${fmtNum(analytics.deliverySuccessRate)}%.`);
  lines.push(`- Загальна вага: ${fmtNum(analytics.totalWeight)} кг; середня вага на місію: ${fmtNum(analytics.avgWeight)} кг.`);
  lines.push(`- Надійність: ${fmtNum(analytics.reliabilityRate)}% (повернення ${fmtNum(analytics.returnedCount)}, пошкодження ${fmtNum(analytics.damagedCount)}, втрати ${fmtNum(analytics.lossCount)}).`);
  lines.push(`- Основна платформа: ${topAsset}; найбільш активний підрозділ: ${topUnit}.`);
  if(analytics.pointsTotal > 0){
    lines.push(`- Усього нараховано балів: ${fmtNum(analytics.pointsTotal)}; лідер за балами: ${topPointsUnit}; найвищий середній бал на місію: ${topEfficiencyUnit}.`);
  }
  lines.push(`- Найбільше перевезень за масою виконав підрозділ: ${topWeightUnit}.`);

  lines.push("");
  lines.push("2. Позитивні моменти");
  if(lastMonth && prevMonth){
    lines.push(`- Останній місяць у зрізі: ${lastMonth.label}. Динаміка до ${prevMonth.label}: місії ${monthMissionDelta > 0 ? "+" : ""}${fmtNum(monthMissionDelta)}, вага ${monthWeightDelta > 0 ? "+" : ""}${fmtNum(monthWeightDelta)} кг, успішність ${monthSuccessDelta > 0 ? "+" : ""}${fmtNum(monthSuccessDelta)} п.п..`);
  } else if(lastMonth){
    lines.push(`- Поточний місяць у зрізі: ${lastMonth.label}; місій ${fmtNum(lastMonth.missionCount)}, вага ${fmtNum(lastMonth.totalWeight)} кг, успішність ${fmtNum(lastMonth.successRate)}%.`);
  }
  if(analytics.notDeliveredCount === 0){
    lines.push("- У поточному зрізі відсутні місії зі статусом «Не доставлено».");
  } else if(analytics.notDeliveredCount <= 3){
    lines.push(`- Кількість недоставлених місій низька: ${fmtNum(analytics.notDeliveredCount)} випадки.`);
  }
  if(analytics.lossCount === 0 && analytics.damagedCount === 0){
    lines.push("- Втрат і пошкоджень засобів у поточному зрізі не зафіксовано.");
  }
  if(analytics.evacuationCount > 0){
    lines.push(`- Евакуаційний напрямок закрито на ${fmtNum(analytics.evacuationCount)} місій: 300 — ${fmtNum(analytics.evacuation300Count)}, 200 — ${fmtNum(analytics.evacuation200Count)}.`);
  }
  if(analytics.pointsTotal > 0){
    lines.push(`- Серед платформ найвищу результативність за балами на місію має ${topEfficiencyAsset}.`);
  }

  lines.push("");
  lines.push("3. Проблематика");
  if(analytics.notDeliveredCount > 0){
    lines.push(`- Не доставлено ${fmtNum(analytics.notDeliveredCount)} місій; ці випадки потребують окремого розбору по причинах та платформах.`);
  }
  if(analytics.lossCount > 0 || analytics.damagedCount > 0){
    lines.push(`- Зафіксовано втрати/пошкодження: втрати ${fmtNum(analytics.lossCount)}, пошкодження ${fmtNum(analytics.damagedCount)}.`);
  }
  if(noAnyLinkCount > 0){
    lines.push(`- У ${fmtNum(noAnyLinkCount)} місій не вказано жодного каналу зв’язку.`);
  }
  if(noWeightCount > 0){
    lines.push(`- У ${fmtNum(noWeightCount)} місій відсутня вага вантажу.`);
  }
  if(noResultCount > 0){
    lines.push(`- У ${fmtNum(noResultCount)} місій не заповнено результат.`);
  }
  if(topProblemUnit){
    lines.push(`- Найбільш проблемний підрозділ у поточному зрізі: ${topProblemUnit.label} (не доставлено ${fmtNum(topProblemUnit.notDelivered)}, пошкодження ${fmtNum(topProblemUnit.damaged)}, втрати ${fmtNum(topProblemUnit.losses)}).`);
  }
  if(topRiskAsset){
    lines.push(`- Платформа з найбільшим ризиковим навантаженням: ${topRiskAsset.label} (не доставлено ${fmtNum(topRiskAsset.notDelivered)}, пошкодження ${fmtNum(topRiskAsset.damaged)}, втрати ${fmtNum(topRiskAsset.losses)} при ${fmtNum(topRiskAsset.missions)} місіях).`);
  }

  lines.push("");
  lines.push("4. На що звернути увагу");
  lines.push(`- Якість даних: початок місії заповнено у ${fmtNum(analytics.timeQuality.startFilledPercent)}% місій, завершення — у ${fmtNum(analytics.timeQuality.endFilledPercent)}%, тривалість — у ${fmtNum(analytics.timeQuality.durationFilledPercent)}%.`);
  lines.push(`- Доповідач не вказаний у ${fmtNum(reporterMissingCount)} місій.`);
  lines.push(`- Основний зв’язок не вказано у ${fmtNum(noPrimaryLinkCount)} місій, резервний — у ${fmtNum(noReserveLinkCount)} місій.`);
  if(analytics.timeQuality.invalidTimelineCount > 0){
    lines.push(`- Є ${fmtNum(analytics.timeQuality.invalidTimelineCount)} місій з некоректною часовою парою початок/завершення.`);
  }
  if(topPointsUnit !== "—"){
    lines.push(`- Для роботи з ефективністю варто окремо відстежувати лідера за сумою балів (${topPointsUnit}) і лідера за середнім балом на місію (${topEfficiencyUnit}).`);
  }

  lines.push("");
  lines.push("5. Рекомендовані дії");
  if(analytics.notDeliveredCount > 0){
    lines.push("- Підрозділам розібрати всі місії «Не доставлено» з прив’язкою до платформи, обставин та причин.");
  }
  if(analytics.lossCount > 0 || analytics.damagedCount > 0){
    lines.push("- Окремо розглянути місії зі втратою або пошкодженням засобів та перевірити повторювані причини.");
  }
  if(reporterMissingCount > 0){
    lines.push("- Забезпечити обов’язкове заповнення поля «Доповідач» по всіх місіях.");
  }
  if(noPrimaryLinkCount > 0 || noReserveLinkCount > 0){
    lines.push("- Посилити дисципліну заповнення полів «Зв’язок Основний» і «Зв’язок Резервний», особливо по місіях з тривалістю.");
  }
  if(noWeightCount > 0){
    lines.push("- Для логістичних місій не залишати порожнім поле «Вага вантажу», щоб не втрачалась точність аналітики.");
  }
  if(analytics.pointsTotal > 0){
    lines.push("- Використовувати зріз «Бали на 1 місію» для виявлення підрозділів і платформ з найкращою результативністю.");
  }
  lines.push("- Для розсилки підрозділам робити акцент окремо на: найактивнішому підрозділі, підрозділі з проблемами, платформі-лідері та платформі з ризиками.");

  return lines.join("\n");

}

function buildDeltaNrkExecutiveReportHtml(analytics, modalKey=""){

  const reportText = buildDeltaNrkExecutiveReportText(analytics);
  const textareaId = `deltaNrkExecutiveReport_${modalKey || uid("delta_nrk_report")}`;
  return `
    <details class="item analytics-block delta-nrk-collapsible-section">
      <summary class="delta-nrk-collapsible-summary">
        <span class="name">Короткий звіт / висновки</span>
        <span class="hint">Готовий текст для копіювання</span>
      </summary>
      <div class="delta-nrk-collapsible-body">
        <div class="delta-report-copybar">
          <button type="button" class="btn ghost btn-mini" data-action="copyTextFromElement" data-arg1="${textareaId}">Копіювати текст</button>
        </div>
        <textarea id="${textareaId}" class="delta-report-textarea" readonly>${htmlesc(reportText)}</textarea>
      </div>
    </details>
  `;

}

function buildDeltaNrkAnalytics(rows, title="", filters={}){

  const grid = Array.isArray(rows) ? rows : [];
  if(grid.length < 2) return null;

  const columns = detectDeltaNrkColumns(grid[0]);
  const detectedFormat = columns.resultAt === 6 && columns.result === 12 ? "Delta NRK 28 колонок" : (columns.resultAt === 8 && columns.result === 14 ? "Delta NRK 30 колонок" : "Delta NRK / alias-map");
  const allItems = grid.slice(1).map((row, index)=>{
    const reportUuid = columns.reportUuid >= 0 ? String(row?.[columns.reportUuid] || "").trim() : "";
    const asset = columns.asset >= 0 ? String(row?.[columns.asset] || "").trim() : "";
    const unit = columns.unit >= 0 ? String(row?.[columns.unit] || "").trim() : "";
    const reporter = columns.reporter >= 0 ? String(row?.[columns.reporter] || "").trim() : "";
    const result = columns.result >= 0 ? String(row?.[columns.result] || "").trim() : "";
    const circumstances = columns.circumstances >= 0 ? String(row?.[columns.circumstances] || "").trim() : "";
    const taskType = columns.taskType >= 0 ? String(row?.[columns.taskType] || "").trim() : "";
    const startAt = columns.startAt >= 0 ? row?.[columns.startAt] : "";
    const endAt = columns.endAt >= 0 ? row?.[columns.endAt] : "";
    const durationRaw = columns.duration >= 0 ? row?.[columns.duration] : "";
    const cargo = columns.cargo >= 0 ? String(row?.[columns.cargo] || "").trim() : "";
    const assetStatus = columns.assetStatus >= 0 ? String(row?.[columns.assetStatus] || "").trim() : "";
    const lossCircumstances = columns.lossCircumstances >= 0 ? String(row?.[columns.lossCircumstances] || "").trim() : "";
    const evacuatedCategory = columns.evacuatedCategory >= 0 ? String(row?.[columns.evacuatedCategory] || "").trim() : "";
    const evacuatedQty = columns.evacuatedQty >= 0 ? parseAnalyticsNumber(row?.[columns.evacuatedQty]) : null;
    const primaryLink = columns.primaryLink >= 0 ? String(row?.[columns.primaryLink] || "").trim() : "";
    const reserveLink = columns.reserveLink >= 0 ? String(row?.[columns.reserveLink] || "").trim() : "";
    const resultAt = columns.resultAt >= 0 ? String(row?.[columns.resultAt] || "").trim() : "";
    const cargoWeight = columns.cargoWeight >= 0 ? parseAnalyticsNumber(row?.[columns.cargoWeight]) : null;
    const totalPoints = columns.totalPoints >= 0 ? parseAnalyticsNumber(row?.[columns.totalPoints]) : null;

    const hasData = [asset, unit, reporter, result, circumstances, taskType, cargo, assetStatus, lossCircumstances, evacuatedCategory, primaryLink, reserveLink, resultAt, String(startAt || "").trim(), String(endAt || "").trim(), String(durationRaw || "").trim()]
      .some(Boolean) || Number.isFinite(cargoWeight) || Number.isFinite(totalPoints);
    if(!hasData) return null;

    const resultAtTs = parseDeltaDateTimeValue(resultAt);
    const startAtTs = parseDeltaDateTimeValue(startAt);
    const endAtTs = parseDeltaDateTimeValue(endAt);
    const providedDurationMinutes = parseDeltaDurationMinutes(durationRaw);
    const computedDurationMinutes = (Number.isFinite(startAtTs) && Number.isFinite(endAtTs) && endAtTs >= startAtTs)
      ? Math.round((endAtTs - startAtTs) / 60000)
      : null;
    const invalidTimeline = Number.isFinite(startAtTs) && Number.isFinite(endAtTs) && endAtTs < startAtTs;
    const missionDurationMinutes = Number.isFinite(computedDurationMinutes) && computedDurationMinutes >= 0
      ? computedDurationMinutes
      : (Number.isFinite(providedDurationMinutes) && providedDurationMinutes >= 0 ? providedDurationMinutes : null);
    const dayNight = splitDeltaDayNight(startAtTs, endAtTs, resultAtTs);
    const effectiveDateRaw = resultAt || endAt || startAt || "";

    return {
      id: index + 1,
      reportUuid,
      asset,
      unit,
      reporter,
      result,
      circumstances,
      taskType,
      cargo,
      evacuatedCategory,
      evacuatedQty: Number.isFinite(evacuatedQty) ? Number(evacuatedQty) : 0,
      assetStatus,
      lossCircumstances,
      primaryLink,
      reserveLink,
      resultAt,
      resultAtTs,
      startAt: String(startAt || "").trim(),
      startAtTs,
      endAt: String(endAt || "").trim(),
      endAtTs,
      durationRaw: String(durationRaw || "").trim(),
      providedDurationMinutes,
      computedDurationMinutes,
      missionDurationMinutes,
      invalidTimeline,
      dayNightKind: dayNight.kind,
      dayMinutes: dayNight.dayMinutes,
      nightMinutes: dayNight.nightMinutes,
      effectiveDateRaw,
      cargoWeight: Number.isFinite(cargoWeight) ? Number(cargoWeight) : 0,
      totalPoints: Number.isFinite(totalPoints) ? Number(totalPoints) : 0,
    };
  }).filter(Boolean);

  if(!allItems.length) return null;

  const unitFilter = String(filters.unit || "").trim();
  const taskTypeFilter = String(filters.taskType || "").trim();
  const assetFilter = String(filters.asset || "").trim();
  const unitOptions = Array.from(new Set(allItems.map(item=>String(item.unit || "").trim()).filter(Boolean))).sort((a,b)=>a.localeCompare(b, "uk"));
  const taskTypeOptions = Array.from(new Set(allItems.map(item=>String(item.taskType || "").trim()).filter(Boolean))).sort((a,b)=>a.localeCompare(b, "uk"));
  const assetOptions = summarizeNormalizedLabelCounts(allItems, item=>String(item.asset || "").trim(), normalizeDeltaPlatformKey).map(item=>item.label);
  const items = allItems.filter(item=>{
    if(unitFilter && item.unit !== unitFilter) return false;
    if(taskTypeFilter && item.taskType !== taskTypeFilter) return false;
    if(assetFilter && normalizeDeltaPlatformKey(item.asset) !== normalizeDeltaPlatformKey(assetFilter)) return false;
    return true;
  });

  if(!items.length){
    return {
      title,
      detectedFormat,
      sourceRows: Math.max(0, grid.length - 1),
      parsedRows: 0,
      recordCount: 0,
      items: [],
      missions: [],
      missionCount: 0,
      deliveredCount: 0,
      notDeliveredCount: 0,
      evacuationCount: 0,
      evacuatedCount: 0,
      successCount: 0,
      successRate: 0,
      deliverySuccessRate: 0,
      lossCount: 0,
      damagedCount: 0,
      returnedCount: 0,
      reliabilityRate: 0,
      totalWeight: 0,
      avgWeight: 0,
      maxWeight: 0,
      maxRecordWeight: 0,
      taskTypes: [],
      assets: [],
        cargoes: [],
        primaryLinks: [],
        reserveLinks: [],
        reporters: [],
        unitsByMissions: [],
        unitsByWeight: [],
        pointsTotal: 0,
        pointsByUnits: [],
        pointsByAssets: [],
        dayNight: {
        dayCount: 0,
        nightCount: 0,
        total: 0,
        dayPercent: 0,
        nightPercent: 0,
        units: [],
        assets: [],
      },
      months: [],
      timeQuality: {
        totalCount: 0,
        missionTotalCount: 0,
        startFilledCount: 0,
        endFilledCount: 0,
        resultFilledCount: 0,
        durationFilledCount: 0,
        durationBaseCount: 0,
        primaryLinkWithDurationCount: 0,
        reserveLinkWithDurationCount: 0,
        fullTimelineCount: 0,
        anyTimeCount: 0,
        validDurationCount: 0,
        invalidTimelineCount: 0,
        startFilledPercent: 0,
        endFilledPercent: 0,
        resultFilledPercent: 0,
        durationFilledPercent: 0,
        primaryLinkWithDurationPercent: 0,
        reserveLinkWithDurationPercent: 0,
        fullTimelinePercent: 0,
        avgDurationMinutes: 0,
        medianDurationMinutes: 0,
        maxDurationMinutes: 0,
      },
      topTaskType: null,
      topAsset: null,
      assetStats: [],
      maxCargoAsset: null,
      maxRecordWeight: 0,
      evacuationItems: [],
      evacuation200Count: 0,
      evacuation300Count: 0,
      evacuationOtherCount: 0,
      unitOptions,
      taskTypeOptions,
      assetOptions,
      filters: {unit: unitFilter, taskType: taskTypeFilter, asset: assetFilter},
      totalParsedRows: allItems.length,
    };
  }

      const countBy = (source, getter)=> {
    const map = new Map();
    (Array.isArray(source) ? source : []).forEach(item=>{
      const key = String(getter(item) || "").trim();
      if(!key) return;
      map.set(key, (map.get(key) || 0) + 1);
    });
    return Array.from(map.entries()).map(([label, value])=>({label, value})).sort((a,b)=>b.value-a.value || String(a.label).localeCompare(String(b.label), "uk"));
  };

  const missions = aggregateDeltaMissionItems(items);
  const recordCount = items.length;
  const missionCount = missions.length;
  const hasMissionUuid = items.some(item=>!!String(item.reportUuid || "").trim());
  const deliveredCount = missions.filter(item=>item.resultKind === "delivered").length;
  const notDeliveredCount = missions.filter(item=>item.resultKind === "not_delivered").length;
  const evacuationCount = missions.filter(item=>/евакуац/i.test(String(item.taskType || ""))).length;
  const evacuatedCount = missions.filter(item=>item.resultKind === "evacuated").length;
  const successCount = deliveredCount;
  const lossCount = missions.filter(item=>item.reliabilityKind === "loss").length;
  const damagedCount = missions.filter(item=>item.reliabilityKind === "damaged").length;
  const returnedCount = missions.filter(item=>item.reliabilityKind === "returned").length;
  const totalWeight = missions.reduce((sum, item)=>sum + (Number(item.cargoWeight) || 0), 0);
  const weightCount = missions.filter(item=>Number.isFinite(item.cargoWeight) && item.cargoWeight > 0).length;
  const avgWeight = weightCount ? (totalWeight / weightCount) : 0;
  const maxWeight = missions.reduce((max, item)=>Math.max(max, Number(item.cargoWeight) || 0), 0);
  const maxRecordWeight = items.reduce((max, item)=>Math.max(max, Number(item.cargoWeight) || 0), 0);
  const logisticsCount = missions.filter(item=>!/евакуац/i.test(String(item.taskType || ""))).length;
  const reliabilityRate = missionCount ? ((returnedCount / missionCount) * 100) : 0;
  const successRate = logisticsCount ? ((deliveredCount / logisticsCount) * 100) : 0;

  const taskTypes = countBy(missions, item=>item.taskType);
  const assets = summarizeNormalizedLabelCounts(missions, item=>item.asset, normalizeDeltaPlatformKey);
  const assetStatsMap = new Map();
  missions.forEach(item=>{
    const rawLabel = String(item.asset || "").trim();
    if(!rawLabel) return;
    const key = normalizeDeltaPlatformKey(rawLabel) || rawLabel;
    if(!assetStatsMap.has(key)){
      assetStatsMap.set(key, {
        label: rawLabel,
        total: 0,
        totalWeight: 0,
        maxWeight: 0,
        maxRecordWeight: 0,
        weightCount: 0,
        variants: new Map(),
      });
    }
    const bucket = assetStatsMap.get(key);
    bucket.total += 1;
    bucket.variants.set(rawLabel, (bucket.variants.get(rawLabel) || 0) + 1);
    const weight = Number(item.cargoWeight) || 0;
    bucket.totalWeight += weight;
    if(weight > 0){
      bucket.weightCount += 1;
      bucket.maxWeight = Math.max(bucket.maxWeight, weight);
    }
    bucket.maxRecordWeight = Math.max(bucket.maxRecordWeight, Number(item.maxRecordWeight) || 0);
  });
  const assetStats = Array.from(assetStatsMap.values()).map(bucket=>{
    const displayLabel = Array.from(bucket.variants.entries())
      .sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || bucket.label;
    return {
      label: displayLabel,
      total: bucket.total,
      totalWeight: bucket.totalWeight,
      avgWeight: bucket.weightCount ? (bucket.totalWeight / bucket.weightCount) : 0,
      maxWeight: bucket.maxWeight,
      maxRecordWeight: bucket.maxRecordWeight,
    };
  }).sort((a,b)=>b.total-a.total || b.maxRecordWeight-a.maxRecordWeight || b.maxWeight-a.maxWeight || String(a.label).localeCompare(String(b.label), "uk"));
  const maxCargoAsset = assetStats
    .slice()
    .sort((a,b)=>b.maxRecordWeight-a.maxRecordWeight || b.maxWeight-a.maxWeight || b.avgWeight-a.avgWeight || b.total-a.total || String(a.label).localeCompare(String(b.label), "uk"))[0] || null;
  const primaryLinks = countBy(missions, item=>item.primaryLink);
  const reserveLinks = countBy(missions, item=>item.reserveLink);
  const reporters = summarizeNormalizedLabelCounts(missions, item=>item.reporter);
  const unitsByMissions = countBy(missions, item=>item.unit);
  const unitsByWeightMap = new Map();
  missions.forEach(item=>{
    const key = String(item.unit || "").trim();
    if(!key) return;
    unitsByWeightMap.set(key, (unitsByWeightMap.get(key) || 0) + (Number(item.cargoWeight) || 0));
  });
  const unitsByWeight = Array.from(unitsByWeightMap.entries()).map(([label, value])=>({label, value})).sort((a,b)=>b.value-a.value || String(a.label).localeCompare(String(b.label), "uk"));
  const unitPlatformMap = new Map();
  missions.forEach(item=>{
    const unitRaw = String(item.unit || "").trim() || "Без підрозділу";
    const unitKey = normalizeAnalyticsHeader(unitRaw) || unitRaw;
    const assetRaw = String(item.asset || "").trim() || "Без платформи";
    const assetKey = normalizeDeltaPlatformKey(assetRaw) || assetRaw;
    if(!unitPlatformMap.has(unitKey)){
      unitPlatformMap.set(unitKey, {
        label: unitRaw,
        total: 0,
        variants: new Map(),
        assets: new Map(),
      });
    }
    const unitBucket = unitPlatformMap.get(unitKey);
    unitBucket.total += 1;
    unitBucket.variants.set(unitRaw, (unitBucket.variants.get(unitRaw) || 0) + 1);
    if(!unitBucket.assets.has(assetKey)){
      unitBucket.assets.set(assetKey, {
        label: assetRaw,
        value: 0,
        totalWeight: 0,
        maxWeight: 0,
        variants: new Map(),
      });
    }
    const assetBucket = unitBucket.assets.get(assetKey);
    assetBucket.value += 1;
    const missionWeight = Number(item.cargoWeight) || 0;
    assetBucket.totalWeight += missionWeight;
    assetBucket.maxWeight = Math.max(assetBucket.maxWeight, missionWeight);
    assetBucket.variants.set(assetRaw, (assetBucket.variants.get(assetRaw) || 0) + 1);
  });
  const unitPlatformStats = Array.from(unitPlatformMap.values()).map(bucket=>{
    const label = Array.from(bucket.variants.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || bucket.label;
    const platforms = Array.from(bucket.assets.values()).map(assetBucket=>({
      label: Array.from(assetBucket.variants.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || assetBucket.label,
      value: assetBucket.value,
      totalWeight: assetBucket.totalWeight,
      avgWeight: assetBucket.value ? (assetBucket.totalWeight / assetBucket.value) : 0,
      maxWeight: assetBucket.maxWeight,
    })).sort((a,b)=>b.value-a.value || b.totalWeight-a.totalWeight || String(a.label).localeCompare(String(b.label), "uk"));
    return {
      label,
      total: bucket.total,
      platformCount: platforms.length,
      totalWeight: platforms.reduce((sum, platform)=>sum + (Number(platform.totalWeight) || 0), 0),
      platforms,
    };
  }).sort((a,b)=>b.total-a.total || b.platformCount-a.platformCount || String(a.label).localeCompare(String(b.label), "uk"));

  const pointsTotal = missions.reduce((sum, item)=>sum + (Number(item.totalPoints) || 0), 0);
  const pointsByUnitMap = new Map();
  const pointsByAssetMap = new Map();
  missions.forEach(item=>{
    const missionPoints = Number(item.totalPoints) || 0;
    if(missionPoints <= 0) return;

    const unitLabel = String(item.unit || "").trim();
    if(unitLabel){
      if(!pointsByUnitMap.has(unitLabel)){
        pointsByUnitMap.set(unitLabel, {label: unitLabel, value: 0, missionCount: 0});
      }
      const unitBucket = pointsByUnitMap.get(unitLabel);
      unitBucket.value += missionPoints;
      unitBucket.missionCount += 1;
    }

    const assetRaw = String(item.asset || "").trim();
    if(assetRaw){
      const assetKey = normalizeDeltaPlatformKey(assetRaw) || assetRaw;
      if(!pointsByAssetMap.has(assetKey)){
        pointsByAssetMap.set(assetKey, {label: assetRaw, value: 0, missionCount: 0, variants: new Map()});
      }
      const assetBucket = pointsByAssetMap.get(assetKey);
      assetBucket.value += missionPoints;
      assetBucket.missionCount += 1;
      assetBucket.variants.set(assetRaw, (assetBucket.variants.get(assetRaw) || 0) + 1);
    }
  });
  const pointsByUnits = Array.from(pointsByUnitMap.values())
    .map(item=>({
      ...item,
      avgPoints: item.missionCount ? item.value / item.missionCount : 0,
    }))
    .sort((a,b)=>b.value-a.value || b.missionCount-a.missionCount || String(a.label).localeCompare(String(b.label), "uk"));
  const pointsByAssets = Array.from(pointsByAssetMap.values())
    .map(item=>{
      const displayLabel = Array.from(item.variants.entries())
        .sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || item.label;
      return {
        label: displayLabel,
        value: item.value,
        missionCount: item.missionCount,
        avgPoints: item.missionCount ? item.value / item.missionCount : 0,
      };
    })
    .sort((a,b)=>b.value-a.value || b.missionCount-a.missionCount || String(a.label).localeCompare(String(b.label), "uk"));

  const dayNightByUnitMap = new Map();
  const dayNightByAssetMap = new Map();
  let dayCount = 0;
  let nightCount = 0;
  missions.forEach(item=>{
    const kind = item.dayNightKind;
    if(kind !== "day" && kind !== "night") return;
    const unitKey = String(item.unit || "Без підрозділу").trim();
    if(!dayNightByUnitMap.has(unitKey)){
      dayNightByUnitMap.set(unitKey, {label:unitKey, total:0, dayCount:0, nightCount:0});
    }
    const unitBucket = dayNightByUnitMap.get(unitKey);
    unitBucket.total += 1;
    if(kind === "day"){
      unitBucket.dayCount += 1;
      dayCount += 1;
    } else {
      unitBucket.nightCount += 1;
      nightCount += 1;
    }

    const assetRaw = String(item.asset || "Без платформи").trim();
    const assetKey = normalizeDeltaPlatformKey(assetRaw) || assetRaw;
    if(!dayNightByAssetMap.has(assetKey)){
      dayNightByAssetMap.set(assetKey, {label:assetRaw, total:0, dayCount:0, nightCount:0, variants:new Map()});
    }
    const assetBucket = dayNightByAssetMap.get(assetKey);
    assetBucket.total += 1;
    assetBucket.variants.set(assetRaw, (assetBucket.variants.get(assetRaw) || 0) + 1);
    if(kind === "day"){
      assetBucket.dayCount += 1;
    } else {
      assetBucket.nightCount += 1;
    }
  });
  const dayNightUnits = Array.from(dayNightByUnitMap.values()).map(item=>({
    ...item,
    dayPercent: item.total ? Math.round((item.dayCount / item.total) * 100) : 0,
    nightPercent: item.total ? Math.round((item.nightCount / item.total) * 100) : 0,
  })).sort((a,b)=>b.nightCount-a.nightCount || b.total-a.total || String(a.label).localeCompare(String(b.label), "uk"));
  const dayNightAssets = Array.from(dayNightByAssetMap.values()).map(item=>{
    const displayLabel = Array.from(item.variants.entries())
      .sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || item.label;
    return {
      label: displayLabel,
      total: item.total,
      dayCount: item.dayCount,
      nightCount: item.nightCount,
      dayPercent: item.total ? Math.round((item.dayCount / item.total) * 100) : 0,
      nightPercent: item.total ? Math.round((item.nightCount / item.total) * 100) : 0,
    };
  }).sort((a,b)=>b.nightCount-a.nightCount || b.total-a.total || String(a.label).localeCompare(String(b.label), "uk"));
  const dayNight = {
    dayCount,
    nightCount,
    total: dayCount + nightCount,
    dayPercent: (dayCount + nightCount) ? Math.round((dayCount / (dayCount + nightCount)) * 100) : 0,
    nightPercent: (dayCount + nightCount) ? Math.round((nightCount / (dayCount + nightCount)) * 100) : 0,
    units: dayNightUnits,
    assets: dayNightAssets,
  };

  const cargoCategoryMap = new Map();
  const cargoComboMap = new Map();
  missions.forEach(item=>{
    const tags = Array.from(new Set(
      (item.rows || []).flatMap(row=>extractDeltaCargoTags(row.cargo))
    ));
    item.cargoTags = tags;
    item.cargoComboLabel = tags.length ? tags.join(" + ") : "Не визначено";
    if(tags.length){
      tags.forEach(tag=>{
        cargoCategoryMap.set(tag, (cargoCategoryMap.get(tag) || 0) + 1);
      });
    } else {
      cargoCategoryMap.set("Не визначено", (cargoCategoryMap.get("Не визначено") || 0) + 1);
    }
    cargoComboMap.set(item.cargoComboLabel, (cargoComboMap.get(item.cargoComboLabel) || 0) + 1);
  });
  const cargoes = Array.from(cargoCategoryMap.entries()).map(([label, value])=>({label, value})).sort((a,b)=>b.value-a.value || String(a.label).localeCompare(String(b.label), "uk"));
  const cargoCombos = Array.from(cargoComboMap.entries()).map(([label, value])=>({label, value})).sort((a,b)=>b.value-a.value || String(a.label).localeCompare(String(b.label), "uk"));
  const evacuationItems = missions.filter(item=>/евакуац/i.test(String(item.taskType || "")));
  const evacuation200Count = evacuationItems.reduce((sum, item)=>sum + (getDeltaEvacCargoKind(item.evacuatedCategory) === "200" ? (Number(item.evacuatedQty) || 1) : 0), 0);
  const evacuation300Count = evacuationItems.reduce((sum, item)=>sum + (getDeltaEvacCargoKind(item.evacuatedCategory) === "300" ? (Number(item.evacuatedQty) || 1) : 0), 0);
  const evacuationOtherCount = evacuationItems.filter(item=>getDeltaEvacCargoKind(item.evacuatedCategory) === "other").length;

  const startFilledCount = missions.filter(item=>(item.rows || []).some(row=>!!String(row?.startAt || "").trim())).length;
  const endFilledCount = missions.filter(item=>(item.rows || []).some(row=>!!String(row?.endAt || "").trim())).length;
  const resultFilledCount = missions.filter(item=>(item.rows || []).some(row=>!!String(row?.resultAt || "").trim())).length;
  const durationFilledCount = missions.filter(item=>(item.rows || []).some(row=>!!String(row?.durationRaw || "").trim())).length;
  const primaryLinkWithDurationCount = missions.filter(item=>{
    const rows = item.rows || [];
    const hasDuration = rows.some(row=>!!String(row?.durationRaw || "").trim());
    return hasDuration && rows.some(row=>!!String(row?.primaryLink || "").trim());
  }).length;
  const reserveLinkWithDurationCount = missions.filter(item=>{
    const rows = item.rows || [];
    const hasDuration = rows.some(row=>!!String(row?.durationRaw || "").trim());
    return hasDuration && rows.some(row=>!!String(row?.reserveLink || "").trim());
  }).length;
  const fullTimelineCount = missions.filter(item=>{
    const rows = item.rows || [];
    const hasStart = rows.some(row=>!!String(row?.startAt || "").trim());
    const hasEnd = rows.some(row=>!!String(row?.endAt || "").trim());
    const hasResult = rows.some(row=>!!String(row?.resultAt || "").trim());
    return hasStart && hasEnd && hasResult;
  }).length;
  const anyTimeCount = missions.filter(item=>(item.rows || []).some(row=>row?.startAt || row?.endAt || row?.resultAt || row?.durationRaw)).length;
  const invalidTimelineCount = missions.filter(item=>item.invalidTimeline).length;
  const durationValues = missions
    .map(item=>item.missionDurationMinutes)
    .filter(value=>Number.isFinite(value) && value >= 0);
  const validDurationCount = durationValues.length;
  const durationSorted = durationValues.slice().sort((a,b)=>a-b);
  const avgDurationMinutes = validDurationCount ? durationValues.reduce((sum, value)=>sum + value, 0) / validDurationCount : 0;
  const medianDurationMinutes = !validDurationCount ? 0 : (validDurationCount % 2
    ? durationSorted[(validDurationCount - 1) / 2]
    : ((durationSorted[(validDurationCount / 2) - 1] + durationSorted[validDurationCount / 2]) / 2));
  const maxDurationMinutes = validDurationCount ? durationSorted[durationSorted.length - 1] : 0;
  const percentOf = (value)=> missionCount ? Math.round((value / missionCount) * 100) : 0;
  const timeQuality = {
    totalCount: missionCount,
    missionTotalCount: missionCount,
    startFilledCount,
    endFilledCount,
    resultFilledCount,
    durationFilledCount,
    durationBaseCount: durationFilledCount,
    primaryLinkWithDurationCount,
    reserveLinkWithDurationCount,
    fullTimelineCount,
    anyTimeCount,
    validDurationCount,
    invalidTimelineCount,
    startFilledPercent: percentOf(startFilledCount),
    endFilledPercent: percentOf(endFilledCount),
    resultFilledPercent: percentOf(resultFilledCount),
    durationFilledPercent: percentOf(durationFilledCount),
    primaryLinkWithDurationPercent: durationFilledCount ? Math.round((primaryLinkWithDurationCount / durationFilledCount) * 100) : 0,
    reserveLinkWithDurationPercent: durationFilledCount ? Math.round((reserveLinkWithDurationCount / durationFilledCount) * 100) : 0,
    fullTimelinePercent: percentOf(fullTimelineCount),
    avgDurationMinutes,
    medianDurationMinutes,
    maxDurationMinutes,
  };

  const monthMap = new Map();
  missions.forEach(item=>{
    const monthKey = parseDeltaMonthKey(item.resultAt || item.endAt || item.startAt || item.effectiveDateRaw) || "no-date";
    const existing = monthMap.get(monthKey) || {
      key: monthKey,
      label: monthKey === "no-date" ? "Без дати" : formatDeltaMonthLabel(monthKey),
      missionCount: 0,
      deliveredCount: 0,
      notDeliveredCount: 0,
      evacuationCount: 0,
      evacuatedCount: 0,
      lossCount: 0,
      damagedCount: 0,
      returnedCount: 0,
      totalWeight: 0,
      maxWeight: 0,
      weightCount: 0,
    };
    existing.missionCount += 1;
    const resultKind = item.resultKind || getDeltaMissionResultKind(item.result, item.taskType);
    if(resultKind === "delivered") existing.deliveredCount += 1;
    if(resultKind === "not_delivered") existing.notDeliveredCount += 1;
    if(/евакуац/i.test(String(item.taskType || ""))) existing.evacuationCount += 1;
    if(resultKind === "evacuated") existing.evacuatedCount = (existing.evacuatedCount || 0) + 1;
    if(item.reliabilityKind === "loss") existing.lossCount += 1;
    if(item.reliabilityKind === "damaged") existing.damagedCount += 1;
    if(item.reliabilityKind === "returned") existing.returnedCount += 1;
    const weight = Number(item.cargoWeight) || 0;
    existing.totalWeight += weight;
    if(weight > 0){
      existing.weightCount += 1;
      existing.maxWeight = Math.max(existing.maxWeight, weight);
    }
    monthMap.set(monthKey, existing);
  });
  const months = Array.from(monthMap.values())
    .map(item=>({
      ...item,
      avgWeight: item.weightCount ? (item.totalWeight / item.weightCount) : 0,
      successCount: item.deliveredCount,
      successRate: item.missionCount ? Math.round((item.deliveredCount / Math.max(1, item.missionCount - item.evacuationCount)) * 100) : 0,
      returnRate: item.missionCount ? Math.round((item.returnedCount / item.missionCount) * 100) : 0,
    }))
    .sort((a,b)=>{
      if(a.key === "no-date") return 1;
      if(b.key === "no-date") return -1;
      return String(a.key).localeCompare(String(b.key), "uk");
    });

  return {
    title,
    detectedFormat,
    sourceRows: Math.max(0, grid.length - 1),
    parsedRows: missionCount,
    recordCount,
    items,
    missions,
    missionCount,
    deliveredCount,
    notDeliveredCount,
    evacuationCount,
    evacuatedCount,
    successCount,
    successRate,
    deliverySuccessRate: successRate,
    lossCount,
    damagedCount,
    returnedCount,
    reliabilityRate,
    totalWeight,
    avgWeight,
    maxWeight,
    maxRecordWeight,
    taskTypes,
    assets,
    cargoes,
    cargoCombos,
    primaryLinks,
    reserveLinks,
    reporters,
    unitsByMissions,
    unitsByWeight,
    unitPlatformStats,
    pointsTotal,
    pointsByUnits,
    pointsByAssets,
    dayNight,
    months,
    timeQuality,
    topTaskType: taskTypes[0] || null,
    topAsset: assets[0] || null,
    assetStats,
    maxCargoAsset,
    evacuationItems,
    evacuation200Count,
    evacuation300Count,
    evacuationOtherCount,
    unitOptions,
    taskTypeOptions,
    assetOptions,
    filters: {unit: unitFilter, taskType: taskTypeFilter, asset: assetFilter},
    totalParsedRows: allItems.length,
    missionGroupingLabel: hasMissionUuid
      ? "UUID Звіту"
      : "Підрозділ + Тип задачі + Дата результату + Платформа",
  };

}

function buildDeltaNrkAnalyticsModalHtml(rows, title="", opts={}){

  const modalKey = String(opts.modalKey || "");
  const currentFilters = opts.filters && typeof opts.filters === "object" ? opts.filters : {};
  const analytics = buildDeltaNrkAnalytics(rows, title, currentFilters);
  if(!analytics){
    return `<div class="hint">Не вдалося розпізнати Delta / НРК. Очікуються колонки на кшталт: Підрозділ, Тип задачі, Результат, Вантаж, Вага вантажу, Статус засобу, Засіб.</div>`;
  }

  const filtersBlock = `
    <div class="item analytics-block delta-nrk-filters">
      <div class="delta-nrk-filter-grid">
        <label class="delta-nrk-filter-field">
          <span>Підрозділ</span>
          <select id="deltaUnitFilter_${modalKey}">
            <option value="">Усі підрозділи</option>
            ${analytics.unitOptions.map(item=>`<option value="${attrEsc(item)}" ${item === analytics.filters.unit ? "selected" : ""}>${htmlesc(item)}</option>`).join("")}
          </select>
        </label>
        <label class="delta-nrk-filter-field">
          <span>Тип задачі</span>
          <select id="deltaTaskTypeFilter_${modalKey}">
            <option value="">Усі типи задач</option>
            ${analytics.taskTypeOptions.map(item=>`<option value="${attrEsc(item)}" ${item === analytics.filters.taskType ? "selected" : ""}>${htmlesc(item)}</option>`).join("")}
          </select>
        </label>
        <label class="delta-nrk-filter-field">
          <span>Платформа</span>
          <select id="deltaAssetFilter_${modalKey}">
            <option value="">Усі платформи</option>
            ${analytics.assetOptions.map(item=>`<option value="${attrEsc(item)}" ${item === analytics.filters.asset ? "selected" : ""}>${htmlesc(item)}</option>`).join("")}
          </select>
        </label>
        <div class="delta-nrk-filter-actions">
          <button class="btn primary btn-mini" data-action="applyDeltaNrkAnalyticsFilters" data-arg1="${modalKey}">Застосувати</button>
          <button class="btn ghost btn-mini" data-action="resetDeltaNrkAnalyticsFilters" data-arg1="${modalKey}">Скинути</button>
        </div>
      </div>
      <div class="delta-nrk-filter-summary">
        <span class="delta-nrk-filter-chip">${analytics.filters.unit ? `Підрозділ: ${htmlesc(analytics.filters.unit)}` : "Усі підрозділи"}</span>
        <span class="delta-nrk-filter-chip">${analytics.filters.taskType ? `Тип задачі: ${htmlesc(analytics.filters.taskType)}` : "Усі типи задач"}</span>
        <span class="delta-nrk-filter-chip">${analytics.filters.asset ? `Платформа: ${htmlesc(analytics.filters.asset)}` : "Усі платформи"}</span>
        <span class="delta-nrk-filter-chip mono">Місій у зрізі: ${fmtNum(analytics.missionCount)}</span>
        <span class="delta-nrk-filter-chip mono">Записів у зрізі: ${fmtNum(analytics.recordCount)}</span>
        ${analytics.totalParsedRows !== analytics.recordCount ? `<span class="delta-nrk-filter-chip mono">Із загалу записів: ${fmtNum(analytics.totalParsedRows)}</span>` : ""}
      </div>
    </div>
  `;
  const wrapDeltaNrkCollapsible = (titleText, bodyHtml, startOpen=false)=>{
    if(!bodyHtml) return "";
    return `
      <details class="item analytics-block delta-nrk-collapsible-section" ${startOpen ? "open" : ""}>
        <summary class="delta-nrk-collapsible-summary">
          <span class="name">${htmlesc(titleText)}</span>
          <span class="hint">Натисни, щоб ${startOpen ? "згорнути" : "розгорнути"} блок</span>
        </summary>
        <div class="delta-nrk-collapsible-body">
          ${bodyHtml}
        </div>
      </details>
    `;
  };

  const diagnosticsBlock = `
    <details class="item analytics-block delta-nrk-diagnostics delta-nrk-collapsible">
      <summary class="delta-nrk-collapsible-summary">
        <span class="name">Перевірка імпорту</span>
        <span class="hint">Службова діагностика імпорту</span>
      </summary>
      <div class="delta-nrk-diagnostics-grid">
        <div class="delta-nrk-diagnostics-item">
          <div class="delta-nrk-diagnostics-k">Формат</div>
          <div class="delta-nrk-diagnostics-v">${htmlesc(analytics.detectedFormat || "—")}</div>
        </div>
        <div class="delta-nrk-diagnostics-item">
          <div class="delta-nrk-diagnostics-k">Рядків у таблиці</div>
          <div class="delta-nrk-diagnostics-v mono">${fmtNum(analytics.sourceRows)}</div>
        </div>
        <div class="delta-nrk-diagnostics-item">
          <div class="delta-nrk-diagnostics-k">Розпізнано записів</div>
          <div class="delta-nrk-diagnostics-v mono">${fmtNum(analytics.recordCount)}</div>
        </div>
        <div class="delta-nrk-diagnostics-item">
          <div class="delta-nrk-diagnostics-k">Унікальних місій</div>
          <div class="delta-nrk-diagnostics-v mono">${fmtNum(analytics.missionCount)}</div>
        </div>
        <div class="delta-nrk-diagnostics-item">
          <div class="delta-nrk-diagnostics-k">Групування місій</div>
          <div class="delta-nrk-diagnostics-v">${htmlesc(analytics.missionGroupingLabel || "—")}</div>
        </div>
      </div>
    </details>
  `;

  const countingLogicBlock = `
    <details class="item analytics-block delta-nrk-diagnostics delta-nrk-collapsible">
      <summary class="delta-nrk-collapsible-summary">
        <span class="name">Логіка підрахунку</span>
        <span class="hint">Пояснення, як формується аналітика</span>
      </summary>
      <div class="delta-nrk-filter-summary">
        <span class="delta-nrk-filter-chip">Основна аналітика і час рахуються по унікальних місіях (UUID або fallback-групування).</span>
        <span class="delta-nrk-filter-chip">По записах лишається тільки технічна перевірка імпорту.</span>
      </div>
    </details>
  `;

  if(!analytics.items.length){
    return `
      <div class="staffing-analytics-modal comparison-analytics-modal delta-nrk-analytics-modal">
        ${filtersBlock}
        ${diagnosticsBlock}
        ${countingLogicBlock}
        ${buildDeltaNrkAutoSummaryHtml(analytics)}
        <div class="item analytics-block">
          <div class="hint">За поточними фільтрами місій не знайдено. Спробуй інший підрозділ або платформу.</div>
        </div>
      </div>
    `;
  }

  const platformsRows = (analytics.assetStats || []).map(item=>({
    label: item.label,
    valueText: fmtNum(item.total),
    meta: `${fmtNum(analytics.missionCount ? Math.round((item.total / analytics.missionCount) * 100) : 0)}% місій · сер. ${fmtNum(item.avgWeight)} кг · макс. рядок ${fmtNum(item.maxRecordWeight)} кг`,
    tone: "b-blue",
  }));
  const cargoRows = analytics.cargoes.map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: `${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій, де була вказана ця категорія`,
    tone: "b-ok",
  }));
  const cargoComboRows = (analytics.cargoCombos || []).map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: `${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій, де була така комбінація`,
    tone: "b-blue",
  }));
  const evacuationRows = [
    {
      label: "Евакуйовано",
      value: analytics.evacuatedCount,
      valueText: fmtNum(analytics.evacuatedCount),
      meta: `${fmtNum(analytics.evacuationCount ? Math.round((analytics.evacuatedCount / analytics.evacuationCount) * 100) : 0)}% евакуаційних місій`,
      tone: "b-ok",
    },
    {
      label: "300 (поранені)",
      value: analytics.evacuation300Count,
      valueText: fmtNum(analytics.evacuation300Count),
      meta: "Евакуйовані поранені",
      tone: "b-blue",
    },
    {
      label: "200 (загиблі)",
      value: analytics.evacuation200Count,
      valueText: fmtNum(analytics.evacuation200Count),
      meta: "Евакуйовані загиблі",
      tone: "b-danger",
    },
  ].filter(item=>item.value > 0 || item.label === "Евакуйовано");
  const mapMissionDetailRow = (item, tone="b-blue")=>({
    label: item.unit || "Без підрозділу",
    valueText: item.resultAt || item.endAt || item.startAt || "Без дати",
    meta: [
      item.asset ? `Платформа: ${item.asset}` : "",
      item.reporter ? `Доповідач: ${item.reporter}` : "",
      item.taskType ? `Тип задачі: ${item.taskType}` : "",
      item.result ? `Результат: ${item.result}` : "",
      item.primaryLink ? `Осн. зв'язок: ${item.primaryLink}` : "",
      item.reserveLink ? `Рез. зв'язок: ${item.reserveLink}` : "",
      Number(item.cargoWeight) > 0 ? `Вага: ${fmtNum(item.cargoWeight)} кг` : "",
      Number(item.totalPoints) > 0 ? `Бали: ${fmtNum(item.totalPoints)}` : "",
      item.circumstances ? `Обставини: ${item.circumstances}` : "",
    ].filter(Boolean).join(" · "),
    tone,
  });
  const reliabilityGroups = [
    {
      label: "Повернення",
      tone: "b-ok",
      items: analytics.missions.filter(item=>item.reliabilityKind === "returned"),
      meta: "успішне завершення місії",
    },
    {
      label: "Повернення з пошкодженням",
      tone: "b-warn",
      items: analytics.missions.filter(item=>item.reliabilityKind === "damaged"),
      meta: "пошкоджені засоби",
    },
    {
      label: "Втрата",
      tone: "b-danger",
      items: analytics.missions.filter(item=>item.reliabilityKind === "loss"),
      meta: "втрачені засоби",
    },
  ];
  const reliabilityRows = reliabilityGroups.map(group=>{
    const detailRows = group.items.map(item=>({
      label: item.unit || "Без підрозділу",
      valueText: item.resultAt || item.endAt || item.startAt || "Без дати",
      meta: [
        item.asset ? `Платформа: ${item.asset}` : "",
        item.taskType ? `Тип задачі: ${item.taskType}` : "",
        item.result ? `Результат: ${item.result}` : "",
        item.circumstances ? `Обставини: ${item.circumstances}` : "",
        item.lossCircumstances ? `Втрати/пошкодження: ${item.lossCircumstances}` : "",
        item.cargoComboLabel || item.cargo ? `Вантаж: ${item.cargoComboLabel || item.cargo}` : "",
        Number(item.cargoWeight) > 0 ? `Вага: ${fmtNum(item.cargoWeight)} кг` : "",
        item.missionDurationMinutes ? `Час: ${formatDurationMinutes(item.missionDurationMinutes)}` : "",
      ].filter(Boolean).join(" · "),
      tone: group.tone,
    }));
    const modalKey = registerRenderedTableModal(
      `${analytics.title || "Delta / НРК"} · ${group.label}`,
      buildDeltaNrkInsightModalHtml([
        {
          title: `${group.label} · місії`,
        summary: `${fmtNum(group.items.length)} місій у цьому статусі · ${fmtNum(analytics.missionCount ? Math.round((group.items.length / analytics.missionCount) * 100) : 0)}% від усіх`,
          rows: detailRows,
          emptyText: "Місій у цьому статусі поки немає.",
        }
      ])
    );
    return {
      label: group.label,
      valueText: fmtNum(group.items.length),
      meta: `${fmtNum(analytics.missionCount ? Math.round((group.items.length / analytics.missionCount) * 100) : 0)}% місій · ${group.meta}`,
      tone: group.tone,
      modalKey,
    };
  });
  const unitMissionRows = analytics.unitsByMissions.map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: `${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій`,
    tone: "b-blue",
  }));
  const unitWeightRows = analytics.unitsByWeight.map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: `${fmtNum(analytics.totalWeight ? Math.round((item.value / analytics.totalWeight) * 100) : 0)}% від загальної маси`,
    tone: "b-ok",
  }));
  const unitPlatformRows = (analytics.unitPlatformStats || []).map(item=>({
    label: item.label,
    valueText: fmtNum(item.platformCount),
    valueLabel: "плф.",
    meta: (item.platforms || []).slice(0, 3).map(platform=>`${platform.label} ${fmtNum(platform.value)} міс. · ${fmtNum(platform.totalWeight)} кг`).join(" · ") || "Платформи не вказані",
    tone: "b-violet",
  }));
  const primaryLinkRows = analytics.primaryLinks.map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: "основний канал",
    tone: "b-blue",
  }));
  const reserveLinkRows = analytics.reserveLinks.map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: "резервний канал",
    tone: "b-ok",
  }));
  const reporterRows = (analytics.reporters || []).map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: `${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій`,
    tone: "b-blue",
  }));
  const pointsUnitRows = (analytics.pointsByUnits || []).map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    valueLabel: "бал.",
    meta: `${fmtNum(item.missionCount)} місій · сер. ${fmtNum(item.avgPoints)} бал./місію`,
    tone: "b-violet",
  }));
  const pointsAssetRows = (analytics.pointsByAssets || []).map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    valueLabel: "бал.",
    meta: `${fmtNum(item.missionCount)} місій · сер. ${fmtNum(item.avgPoints)} бал./місію`,
    tone: "b-blue",
  }));
  const pointEfficiencyUnitRows = (analytics.pointsByUnits || []).map(item=>({
    label: item.label,
    valueText: fmtNum(item.avgPoints),
    valueLabel: "бал./міс.",
    meta: `${fmtNum(item.value)} бал. · ${fmtNum(item.missionCount)} місій`,
    tone: "b-violet",
  })).sort((a,b)=>(parseAnalyticsNumber(b.valueText) || 0) - (parseAnalyticsNumber(a.valueText) || 0) || String(a.label).localeCompare(String(b.label), "uk"));
  const pointEfficiencyAssetRows = (analytics.pointsByAssets || []).map(item=>({
    label: item.label,
    valueText: fmtNum(item.avgPoints),
    valueLabel: "бал./міс.",
    meta: `${fmtNum(item.value)} бал. · ${fmtNum(item.missionCount)} місій`,
    tone: "b-blue",
  })).sort((a,b)=>(parseAnalyticsNumber(b.valueText) || 0) - (parseAnalyticsNumber(a.valueText) || 0) || String(a.label).localeCompare(String(b.label), "uk"));

  const platformsModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Платформи`,
      buildDeltaNrkInsightModalHtml([
      {
        title: "Платформи · по місіях",
        summary: `Усього платформ: ${fmtNum(platformsRows.length)} · місій: ${fmtNum(analytics.missionCount)}`,
        rows: platformsRows,
        emptyText: "По платформах даних поки немає.",
      }
    ])
  );
  const cargoModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Вантажі`,
      buildDeltaNrkInsightModalHtml([
      {
        title: "Категорії вантажу · по місіях",
        summary: `Загальна вага: ${fmtNum(analytics.totalWeight)} кг · середня: ${fmtNum(analytics.avgWeight)} кг · макс. місія: ${fmtNum(analytics.maxWeight)} кг · макс. рядок: ${fmtNum(analytics.maxRecordWeight)} кг`,
        rows: cargoRows,
        emptyText: "По вантажах даних поки немає.",
      },
      {
        title: "Найчастіші комбінації · по місіях",
        summary: `Унікальних комбінацій: ${fmtNum(cargoComboRows.length)}`,
        rows: cargoComboRows,
        emptyText: "Комбінацій вантажу поки немає.",
      }
    ])
  );
  const evacuationItems = analytics.evacuationItems || [];
  const mapEvacDetailRow = (item)=>({
    label: item.unit || "Без підрозділу",
    valueText: item.resultAt || item.endAt || item.startAt || "Без дати",
    meta: [
      item.asset ? `Платформа: ${item.asset}` : "",
      item.result ? `Результат: ${item.result}` : "",
      item.evacuatedCategory ? `Евакуйовано: ${item.evacuatedCategory}` : "",
      item.evacuatedQty > 0 ? `Кількість: ${fmtNum(item.evacuatedQty)}` : "",
      item.cargo ? `Вантаж: ${item.cargo}` : "",
      item.circumstances ? `Обставини: ${item.circumstances}` : "",
      item.missionDurationMinutes ? `Час: ${formatDurationMinutes(item.missionDurationMinutes)}` : "",
    ].filter(Boolean).join(" · "),
    tone: getDeltaEvacCargoKind(item.evacuatedCategory) === "200" ? "b-danger" : "b-blue",
  });
  const evacuationDetailRows = evacuationItems.map(mapEvacDetailRow);
  const evacuation300Rows = evacuationItems.filter(item=>getDeltaEvacCargoKind(item.evacuatedCategory) === "300").map(mapEvacDetailRow);
  const evacuation200Rows = evacuationItems.filter(item=>getDeltaEvacCargoKind(item.evacuatedCategory) === "200").map(mapEvacDetailRow);
  const evacuationModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Евакуація`,
      buildDeltaNrkInsightModalHtml([
      {
        title: "Евакуація · по місіях",
        summary: `Усього евакуаційних: ${fmtNum(analytics.evacuationCount)} · Евакуйовано: ${fmtNum(analytics.evacuatedCount)}`,
        rows: evacuationRows,
        emptyText: "Евакуаційних місій поки немає.",
      },
      {
        title: "300 (поранені)",
        summary: `${fmtNum(analytics.evacuation300Count)} евакуйованих поранених`,
        rows: evacuation300Rows,
        emptyText: "Місій категорії 300 поки немає.",
      },
      {
        title: "200 (загиблі)",
        summary: `${fmtNum(analytics.evacuation200Count)} евакуйованих загиблих`,
        rows: evacuation200Rows,
        emptyText: "Місій категорії 200 поки немає.",
      }
    ])
  );
  const unitsModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Підрозділи`,
      buildDeltaNrkInsightModalHtml([
      {
        title: "За кількістю місій",
        summary: `Усього підрозділів: ${fmtNum(unitMissionRows.length)} · місій: ${fmtNum(analytics.missionCount)}`,
        rows: unitMissionRows,
        emptyText: "По підрозділах даних поки немає.",
      },
      {
        title: "За масою перевезень",
        summary: `Загальна вага: ${fmtNum(analytics.totalWeight)} кг`,
        rows: unitWeightRows,
        emptyText: "По масі перевезень даних поки немає.",
      }
    ])
  );
  const unitPlatformsModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Платформи в підрозділах`,
    buildDeltaNrkInsightModalHtml([
      {
        title: "Платформи в підрозділах · по місіях",
        summary: `Підрозділів: ${fmtNum(unitPlatformRows.length)} · місій: ${fmtNum(analytics.missionCount)}`,
        rows: (analytics.unitPlatformStats || []).map(item=>({
          label: item.label,
          valueText: fmtNum(item.platformCount),
          valueLabel: "плф.",
          meta: (item.platforms || []).length
            ? item.platforms.map(platform=>`${platform.label} ${fmtNum(platform.value)} міс. · ${fmtNum(platform.totalWeight)} кг · сер. ${fmtNum(platform.avgWeight)} кг`).join(" · ")
            : "Платформи не вказані",
          tone: "b-violet",
        })),
        emptyText: "По зв’язку підрозділ-платформа даних поки немає.",
      },
      {
        title: "Усього за підрозділами",
        summary: (() => {
          const totalLinks = (analytics.unitPlatformStats || []).reduce((sum, item)=>sum + (Number(item.platformCount) || 0), 0);
          return `Всього за підрозділами: ${fmtNum(totalLinks)}`;
        })(),
        rows: (() => {
          const map = new Map();
          (analytics.unitPlatformStats || []).forEach(unitItem=>{
            (unitItem.platforms || []).forEach(platform=>{
              const key = normalizeDeltaPlatformKey(platform.label) || platform.label;
              if(!map.has(key)){
                map.set(key, {
                  label: platform.label,
                  unitCount: 0,
                  variants: new Map(),
                });
              }
              const bucket = map.get(key);
              bucket.unitCount += 1;
              bucket.variants.set(platform.label, (bucket.variants.get(platform.label) || 0) + 1);
            });
          });
          return Array.from(map.values())
            .map(bucket=>({
              label: Array.from(bucket.variants.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || bucket.label,
              valueText: fmtNum(bucket.unitCount),
              meta: "",
              tone: "b-blue",
            }))
            .sort((a,b)=>(parseAnalyticsNumber(b.valueText) || 0) - (parseAnalyticsNumber(a.valueText) || 0) || String(a.label).localeCompare(String(b.label), "uk"));
        })(),
        emptyText: "По видах платформ даних поки немає.",
      }
    ])
  );
  const linksModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Зв’язок`,
    buildDeltaNrkInsightModalHtml([
      {
        title: "Основний канал",
        summary: `Унікальних каналів: ${fmtNum(primaryLinkRows.length)}`,
        rows: primaryLinkRows,
        emptyText: "По основному зв’язку даних поки немає.",
      },
      {
        title: "Резервний канал",
        summary: `Унікальних каналів: ${fmtNum(reserveLinkRows.length)}`,
        rows: reserveLinkRows,
        emptyText: "По резервному зв’язку даних поки немає.",
      }
    ])
  );
  const reportersModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Доповідачі`,
    buildDeltaNrkInsightModalHtml([
      {
        title: "Доповідачі · по місіях",
        summary: `Унікальних доповідачів: ${fmtNum(reporterRows.length)} · місій: ${fmtNum(analytics.missionCount)}`,
        rows: reporterRows,
        emptyText: "По доповідачах даних поки немає.",
      }
    ])
  );
  const pointsModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Нараховані бали`,
    buildDeltaNrkInsightModalHtml([
      {
        title: "Бали · по підрозділах",
        summary: `Усього балів: ${fmtNum(analytics.pointsTotal)} · підрозділів: ${fmtNum(pointsUnitRows.length)}`,
        rows: pointsUnitRows,
        emptyText: "По підрозділах балів поки немає.",
      },
      {
        title: "Бали · по платформах",
        summary: `Усього балів: ${fmtNum(analytics.pointsTotal)} · платформ: ${fmtNum(pointsAssetRows.length)}`,
        rows: pointsAssetRows,
        emptyText: "По платформах балів поки немає.",
      }
    ])
  );
  const pointsEfficiencyModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / НРК"} · Бали на 1 місію`,
    buildDeltaNrkInsightModalHtml([
      {
        title: "Бали на 1 місію · по підрозділах",
        summary: `Усього балів: ${fmtNum(analytics.pointsTotal)} · підрозділів: ${fmtNum(pointEfficiencyUnitRows.length)}`,
        rows: pointEfficiencyUnitRows,
        emptyText: "По підрозділах балів поки немає.",
      },
      {
        title: "Бали на 1 місію · по платформах",
        summary: `Усього балів: ${fmtNum(analytics.pointsTotal)} · платформ: ${fmtNum(pointEfficiencyAssetRows.length)}`,
        rows: pointEfficiencyAssetRows,
        emptyText: "По платформах балів поки немає.",
      }
    ])
  );
  const anomalyGroups = [
    {
      label: "Без результату",
      tone: "b-warn",
      items: analytics.missions.filter(item=>!String(item.result || "").trim()),
      meta: "не заповнено поле результату",
    },
    {
      label: "Без ваги",
      tone: "b-blue",
      items: analytics.missions.filter(item=>(Number(item.cargoWeight) || 0) <= 0),
      meta: "немає ваги місії",
    },
    {
      label: "Без доповідача",
      tone: "b-violet",
      items: analytics.missions.filter(item=>!String(item.reporter || "").trim()),
      meta: "не вказано доповідача",
    },
    {
      label: "Без основного зв'язку",
      tone: "b-warn",
      items: analytics.missions.filter(item=>!String(item.primaryLink || "").trim()),
      meta: "не вказано основний зв’язок",
    },
    {
      label: "Без резервного зв'язку",
      tone: "b-blue",
      items: analytics.missions.filter(item=>!String(item.reserveLink || "").trim()),
      meta: "не вказано резервний зв’язок",
    },
    {
      label: "Без жодного зв'язку",
      tone: "b-danger",
      items: analytics.missions.filter(item=>!String(item.primaryLink || "").trim() && !String(item.reserveLink || "").trim()),
      meta: "не вказано жоден канал зв’язку",
    },
  ];
  const anomalyRows = anomalyGroups.map(group=>{
    const modalKey = registerRenderedTableModal(
      `${analytics.title || "Delta / НРК"} · ${group.label}`,
      buildDeltaNrkInsightModalHtml([
        {
          title: `${group.label} · місії`,
          summary: `${fmtNum(group.items.length)} місій · ${fmtNum(analytics.missionCount ? Math.round((group.items.length / analytics.missionCount) * 100) : 0)}% від усіх`,
          rows: group.items.map(item=>mapMissionDetailRow(item, group.tone)),
          emptyText: "У цьому зрізі аномалій поки немає.",
        }
      ])
    );
    return {
      label: group.label,
      valueText: fmtNum(group.items.length),
      meta: `${fmtNum(analytics.missionCount ? Math.round((group.items.length / analytics.missionCount) * 100) : 0)}% місій · ${group.meta}`,
      tone: group.tone,
      modalKey,
    };
  }).filter(item=>(parseAnalyticsNumber(item.valueText) || 0) > 0);

  const platformsBlock = buildDeltaNrkTopList(
    "Платформи · по місіях",
    platformsRows.slice(0, 8),
    "По платформах даних поки немає."
  ).replace(
    '<div class="row"><div class="name">Платформи · по місіях</div></div>',
    `<div class="row"><div class="name">Платформи</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${platformsModalKey}">Детальніше</button></div>`
  );

  const cargoBlock = buildDeltaNrkTopList(
    "Вантажі · по місіях",
    cargoRows.slice(0, 8),
    "По вантажах даних поки немає."
  ).replace(
    '<div class="row"><div class="name">Вантажі · по місіях</div></div>',
    `<div class="row"><div class="name">Вантажі</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${cargoModalKey}">Детальніше</button></div>`
  );

  const evacuationBlock = analytics.evacuationCount > 0 ? buildDeltaNrkTopList(
    "Евакуація · по місіях",
    evacuationRows,
    "Евакуаційних місій поки немає."
  ).replace(
    '<div class="row"><div class="name">Евакуація</div></div>',
    `<div class="row"><div class="name">Евакуація</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${evacuationModalKey}">Детальніше</button></div>`
  ) : "";

  const reliabilityBlock = `
    <div class="item analytics-block delta-nrk-list">
      <div class="row"><div class="name">Надійність · по місіях</div></div>
      <div class="comparison-compact-grid">
        ${reliabilityRows.length
          ? reliabilityRows.map((item, index)=>`
              <div class="comparison-compact-card delta-nrk-card delta-nrk-reliability-card">
                <div class="comparison-compact-rank mono">${index + 1}</div>
                <div class="comparison-compact-main">
                  <div class="comparison-compact-title">${htmlesc(item.label)}</div>
                  <div class="comparison-compact-meta">${htmlesc(item.meta || "")}</div>
                </div>
                <div class="delta-nrk-reliability-actions">
                  <div class="badge ${item.tone || "b-blue"} mono">${htmlesc(String(item.valueText || ""))}</div>
                  <button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${item.modalKey}">Місії</button>
                </div>
              </div>
            `).join("")
          : `<div class="hint">Даних по статусу засобу поки немає.</div>`
        }
      </div>
    </div>
  `;

  const unitsBlock = buildDeltaNrkTopList(
    "Підрозділи · по місіях",
    unitMissionRows.slice(0, 8),
    "По підрозділах даних поки немає."
  ).replace(
    '<div class="row"><div class="name">Підрозділи · по місіях</div></div>',
    `<div class="row"><div class="name">Підрозділи</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${unitsModalKey}">Детальніше</button></div>`
  );
  const unitPlatformsBlock = buildDeltaNrkTopList(
    "Платформи в підрозділах · по місіях",
    unitPlatformRows.slice(0, 8),
    "По зв’язку підрозділ-платформа даних поки немає."
  ).replace(
    '<div class="row"><div class="name">Платформи в підрозділах · по місіях</div></div>',
    `<div class="row"><div class="name">Платформи в підрозділах</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${unitPlatformsModalKey}">Детальніше</button></div>`
  );

  const linksBlock = buildDeltaNrkTopList(
    "Зв’язок · по місіях",
    [
      ...primaryLinkRows.slice(0, 4),
      ...reserveLinkRows.slice(0, 4),
    ],
    "По зв’язку даних поки немає."
  ).replace(
    '<div class="row"><div class="name">Зв’язок · по місіях</div></div>',
    `<div class="row"><div class="name">Зв’язок</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${linksModalKey}">Детальніше</button></div>`
  );
  const reportersBlock = buildDeltaNrkTopList(
    "Доповідачі · по місіях",
    reporterRows.slice(0, 8),
    "По доповідачах даних поки немає."
  ).replace(
    '<div class="row"><div class="name">Доповідачі · по місіях</div></div>',
    `<div class="row"><div class="name">Доповідачі · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${reportersModalKey}">Детальніше</button></div>`
  );
  const pointsBlock = buildDeltaNrkTopList(
    "Нараховані бали · по місіях",
    pointsUnitRows.slice(0, 8),
    "По балах даних поки немає."
  ).replace(
    '<div class="row"><div class="name">Нараховані бали · по місіях</div></div>',
    `<div class="row"><div class="name">Нараховані бали · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${pointsModalKey}">Детальніше</button></div>`
  );
  const pointsEfficiencyBlock = buildDeltaNrkTopList(
    "Бали на 1 місію · по місіях",
    pointEfficiencyUnitRows.slice(0, 8),
    "По ефективності балів даних поки немає."
  ).replace(
    '<div class="row"><div class="name">Бали на 1 місію · по місіях</div></div>',
    `<div class="row"><div class="name">Бали на 1 місію · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${pointsEfficiencyModalKey}">Детальніше</button></div>`
  );
  const anomaliesBlock = `
    <div class="item analytics-block delta-nrk-list">
      <div class="row"><div class="name">Аномалії · по місіях</div></div>
      <div class="comparison-compact-grid">
        ${anomalyRows.length
          ? anomalyRows.map((item, index)=>`
              <div class="comparison-compact-card delta-nrk-card delta-nrk-reliability-card">
                <div class="comparison-compact-rank mono">${index + 1}</div>
                <div class="comparison-compact-main">
                  <div class="comparison-compact-title">${htmlesc(item.label)}</div>
                  <div class="comparison-compact-meta">${htmlesc(item.meta || "")}</div>
                </div>
                <div class="delta-nrk-reliability-actions">
                  ${renderDeltaMetricBadge(item, "Аномалії · по місіях")}
                  <button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${item.modalKey}">Місії</button>
                </div>
              </div>
            `).join("")
          : `<div class="hint">Критичних пропусків у поточному зрізі не знайдено.</div>`
        }
      </div>
    </div>
  `;

  return `
    <div class="staffing-analytics-modal comparison-analytics-modal delta-nrk-analytics-modal">
      ${filtersBlock}
      ${buildDeltaNrkAutoSummaryHtml(analytics)}
      ${buildDeltaNrkMonthlyAnalyticsHtml(analytics)}
      <div class="control-grid">
        ${wrapDeltaNrkCollapsible("Платформи", platformsBlock)}
        ${wrapDeltaNrkCollapsible("Вантажі", cargoBlock)}
      </div>
      ${evacuationBlock
        ? `<div class="control-grid">${wrapDeltaNrkCollapsible("Евакуація", evacuationBlock)}${wrapDeltaNrkCollapsible("Надійність", reliabilityBlock)}</div><div class="control-grid">${wrapDeltaNrkCollapsible("Підрозділи", unitsBlock)}${wrapDeltaNrkCollapsible("Платформи в підрозділах", unitPlatformsBlock)}</div><div class="control-grid">${wrapDeltaNrkCollapsible("Зв’язок", linksBlock)}${wrapDeltaNrkCollapsible("Доповідачі", reportersBlock)}</div><div class="control-grid">${wrapDeltaNrkCollapsible("Нараховані бали", pointsBlock)}${wrapDeltaNrkCollapsible("Бали на 1 місію", pointsEfficiencyBlock)}</div><div class="control-grid">${wrapDeltaNrkCollapsible("Аномалії", anomaliesBlock)}</div>`
        : `<div class="control-grid">${wrapDeltaNrkCollapsible("Надійність", reliabilityBlock)}${wrapDeltaNrkCollapsible("Підрозділи", unitsBlock)}</div><div class="control-grid">${wrapDeltaNrkCollapsible("Платформи в підрозділах", unitPlatformsBlock)}${wrapDeltaNrkCollapsible("Зв’язок", linksBlock)}</div><div class="control-grid">${wrapDeltaNrkCollapsible("Доповідачі", reportersBlock)}${wrapDeltaNrkCollapsible("Нараховані бали", pointsBlock)}</div><div class="control-grid">${wrapDeltaNrkCollapsible("Бали на 1 місію", pointsEfficiencyBlock)}${wrapDeltaNrkCollapsible("Аномалії", anomaliesBlock)}</div>`
      }
      ${wrapDeltaNrkCollapsible("День / ніч", buildDeltaNrkDayNightHtml(analytics))}
      ${buildDeltaNrkExecutiveReportHtml(analytics, modalKey)}
      ${wrapDeltaNrkCollapsible("Якість заповнення даних", buildDeltaNrkTimeQualityHtml(analytics))}
      ${diagnosticsBlock}
      ${countingLogicBlock}
    </div>
  `;

}

function openRenderedTableModal(key){

  const item = UI.renderedTableModals?.[key];

  if(!item) return;

  showSheet(item.title, `
    <div class="table-modal-body" data-rendered-modal-key="${attrEsc(key)}">
      ${item.bodyHtml}
    </div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn primary" data-action="hideSheet">Закрити</button>
    </div>
  `, {stack:true, sheetClass:"rendered-analytics-sheet"});

  requestAnimationFrame(()=>{
    animateRenderedDonuts(document.querySelector('.sheet'));
  });

}

function buildDeltaBplaAutoSummaryHtml(analytics){

  if(!analytics) return "";

  const percentOfMissions = value=>analytics.missionCount ? fmtNum(Math.round((Number(value || 0) / analytics.missionCount) * 100)) : "0";
  const cards = [
    {label:"Місій", value: fmtNum(analytics.missionCount), text: `${fmtNum(analytics.recordCount)} записів у зрізі`},
    {label:"Розвідка", value: fmtNum(analytics.reconMissionCount), text: `${percentOfMissions(analytics.reconMissionCount)}% місій`},
    {label:"Ураження", value: fmtNum(analytics.strikeMissionCount), text: `${percentOfMissions(analytics.strikeMissionCount)}% місій · цілі ${fmtNum(analytics.totalTargets)}`},
    {label:"Доставка", value: fmtNum(analytics.deliveryMissionCount), text: `${percentOfMissions(analytics.deliveryMissionCount)}% місій · доставлено ${fmtNum(analytics.deliveredCargoCount)}`},
    {label:"Цілі", value: fmtNum(analytics.totalTargets), text: `${fmtNum(analytics.targetMissionCount)} місій з цілями`},
    {label:"Платформа", value: analytics.topAsset?.label || "—", text: analytics.topAsset ? `${fmtNum(analytics.topAsset.value)} місій` : "Немає даних"},
    {label:"Сер. час", value: htmlesc(formatDurationMinutes(analytics.timeQuality?.avgDurationMinutes || 0)), text: `${fmtNum(analytics.timeQuality?.validDurationCount || 0)} місій із тривалістю`},
    {label:"Надійність", value: `${fmtNum(analytics.reliabilityRate)}%`, text: `Повернення ${fmtNum(analytics.returnedCount)} · Втрати ${fmtNum(analytics.lossCount)}`, tone: analytics.lossCount > 0 ? "danger" : "ok"},
  ];

  return `
    <div class="delta-nrk-summary-row">
      ${cards.map(card=>`
        <div class="delta-nrk-summary-card ${card.tone ? `is-${card.tone}` : ""}">
          <div class="delta-nrk-summary-k">${card.label}</div>
          <div class="delta-nrk-summary-v">${card.value}</div>
          <div class="delta-nrk-summary-s">${card.text}</div>
        </div>
      `).join("")}
    </div>
  `;

}

function buildDeltaBplaMonthlyAnalyticsHtml(analytics){

  if(!analytics?.months?.length) return "";

  const formatDelta = (value, suffix="")=>{
    const num = Number(value || 0);
    if(!Number.isFinite(num) || num === 0) return `0${suffix}`;
    return `${num > 0 ? "+" : ""}${fmtNum(num)}${suffix}`;
  };
  const getDeltaTone = value=>{
    const num = Number(value || 0);
    if(num > 0) return "is-up";
    if(num < 0) return "is-down";
    return "is-flat";
  };

  return `
    <div class="item analytics-block delta-monthly-block">
      <div class="row delta-monthly-block-head">
        <div class="name">Аналітика по місяцях</div>
      </div>
      <div class="delta-monthly-grid delta-monthly-rich-grid">
        ${analytics.months.map((item, index)=>{
          const prev = index > 0 ? analytics.months[index - 1] : null;
          const reliabilityRate = item.missionCount ? Math.round((Number(item.returnedCount || 0) / item.missionCount) * 100) : 0;
          const missionDelta = prev ? ((Number(item.missionCount) || 0) - (Number(prev.missionCount) || 0)) : null;
          const targetDelta = prev ? ((Number(item.targetCount) || 0) - (Number(prev.targetCount) || 0)) : null;
          const strikeDelta = prev ? ((Number(item.strikeCount) || 0) - (Number(prev.strikeCount) || 0)) : null;
          const lossesDelta = prev ? ((Number(item.lossCount) || 0) - (Number(prev.lossCount) || 0)) : null;
          return `
            <div class="delta-monthly-card tone-blue delta-monthly-rich-card">
              <div class="delta-monthly-head">
                <div class="delta-monthly-month">${htmlesc(item.label)}</div>
                <div class="delta-monthly-value mono">Всього місій ${fmtNum(item.missionCount)}</div>
              </div>
              <div class="delta-monthly-statline">
                <span class="delta-monthly-statlabel">Розвідка</span> <strong>${fmtNum(item.reconCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Ураження</span> <strong>${fmtNum(item.strikeCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Доставка</span> <strong>${fmtNum(item.deliveryCount)}</strong>
              </div>
              <div class="delta-monthly-statline">
                <span class="delta-monthly-statlabel">Цілі</span> <strong>${fmtNum(item.targetCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Місій з цілями</span> <strong>${fmtNum(item.targetMissionCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Мінування</span> <strong>${fmtNum(item.miningCount)}</strong>
              </div>
              <div class="delta-monthly-statline">
                <span class="delta-monthly-statlabel">Втрати</span> <strong>${fmtNum(item.lossCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Повернення</span> <strong>${fmtNum(item.returnedCount)}</strong>
                <span class="delta-monthly-sep">·</span>
                <span class="delta-monthly-statlabel">Надійність</span> <strong>${fmtNum(reliabilityRate)}%</strong>
              </div>
              <div class="delta-monthly-delta-grid">
                <div class="delta-monthly-delta-chip ${prev ? getDeltaTone(missionDelta) : "is-flat"}">Місії: ${prev ? formatDelta(missionDelta) : "—"}</div>
                <div class="delta-monthly-delta-chip ${prev ? getDeltaTone(targetDelta) : "is-flat"}">Цілі: ${prev ? formatDelta(targetDelta) : "—"}</div>
                <div class="delta-monthly-delta-chip ${prev ? getDeltaTone(strikeDelta) : "is-flat"}">Ураження: ${prev ? formatDelta(strikeDelta) : "—"}</div>
                <div class="delta-monthly-delta-chip ${prev ? getDeltaTone(-lossesDelta) : "is-flat"}">Втрати: ${prev ? formatDelta(lossesDelta) : "—"}</div>
              </div>
            </div>
          `;
        }).join("")}
      </div>
    </div>
  `;

}

function buildDeltaBplaExecutiveReportText(analytics){

  if(!analytics) return "";

  const lines = [];
  const scopeParts = [];
  if(analytics.filters?.unit) scopeParts.push(`підрозділ: ${analytics.filters.unit}`);
  if(analytics.filters?.taskType) scopeParts.push(`тип задачі: ${analytics.filters.taskType}`);
  if(analytics.filters?.asset) scopeParts.push(`платформа: ${analytics.filters.asset}`);

  const lastMonth = Array.isArray(analytics.months) && analytics.months.length ? analytics.months[analytics.months.length - 1] : null;
  const prevMonth = Array.isArray(analytics.months) && analytics.months.length > 1 ? analytics.months[analytics.months.length - 2] : null;
  const monthMissionDelta = lastMonth && prevMonth ? (Number(lastMonth.missionCount || 0) - Number(prevMonth.missionCount || 0)) : 0;
  const monthTargetDelta = lastMonth && prevMonth ? (Number(lastMonth.targetCount || 0) - Number(prevMonth.targetCount || 0)) : 0;
  const monthStrikeDelta = lastMonth && prevMonth ? (Number(lastMonth.strikeCount || 0) - Number(prevMonth.strikeCount || 0)) : 0;

  const topAsset = analytics.topAsset?.label ? `${analytics.topAsset.label} (${fmtNum(analytics.topAsset.value)} місій)` : "—";
  const topTask = analytics.topTaskType?.label ? `${analytics.topTaskType.label} (${fmtNum(analytics.topTaskType.value)} місій)` : "—";
  const topUnit = analytics.unitsByMissions?.[0]?.label ? `${analytics.unitsByMissions[0].label} (${fmtNum(analytics.unitsByMissions[0].value)} місій)` : "—";
  const topTarget = analytics.targetTypes?.[0]?.label ? `${analytics.targetTypes[0].label} (${fmtNum(analytics.targetTypes[0].value)} місій)` : "—";
  const topAmmo = analytics.topAmmoType?.label ? `${analytics.topAmmoType.label} (${fmtNum(analytics.topAmmoType.value)} місій)` : "—";
  const topStatus = analytics.topTargetStatus?.label ? `${analytics.topTargetStatus.label} (${fmtNum(analytics.topTargetStatus.value)} місій)` : "—";
  const topRiskAsset = analytics.assetStats?.slice().sort((a,b)=>(b.lossCount - a.lossCount) || (b.total - a.total) || String(a.label).localeCompare(String(b.label), "uk"))[0] || null;
  const missingDuration = Math.max(0, analytics.missionCount - Number(analytics.timeQuality?.durationFilledCount || 0));
  const missingTargetStatus = analytics.missions.filter(item=>!(item.targetStatuses || []).length && ((Number(item.targetCount) || 0) > 0 || (item.targetTypes || []).length)).length;
  const missingControl = analytics.missions.filter(item=>!String(item.controlType || "").trim()).length;

  lines.push(`Короткий звіт Delta / БпЛА${scopeParts.length ? ` (${scopeParts.join("; ")})` : ""}`);
  lines.push("");
  lines.push("1. Загальна картина");
  lines.push(`- Унікальних місій: ${fmtNum(analytics.missionCount)}; технічних записів: ${fmtNum(analytics.recordCount)}.`);
  lines.push(`- Розвідка: ${fmtNum(analytics.reconMissionCount)}; ураження: ${fmtNum(analytics.strikeMissionCount)}; доставка: ${fmtNum(analytics.deliveryMissionCount)}; мінування: ${fmtNum(analytics.miningMissionCount)}.`);
  lines.push(`- Усього цілей: ${fmtNum(analytics.totalTargets)}; місій із цілями: ${fmtNum(analytics.targetMissionCount)}; усього БК: ${fmtNum(analytics.totalAmmoQty)}.`);
  lines.push(`- Надійність: ${fmtNum(analytics.reliabilityRate)}% (повернення ${fmtNum(analytics.returnedCount)}, втрати ${fmtNum(analytics.lossCount)}).`);
  lines.push(`- Основна платформа: ${topAsset}; найактивніший підрозділ: ${topUnit}; основний тип задачі: ${topTask}.`);
  lines.push(`- Найпоширеніша ціль: ${topTarget}; основний статус цілей: ${topStatus}; найуживаніший БК: ${topAmmo}.`);

  lines.push("");
  lines.push("2. Позитивні моменти");
  if(lastMonth && prevMonth){
    lines.push(`- Останній місяць у зрізі: ${lastMonth.label}. Динаміка до ${prevMonth.label}: місії ${monthMissionDelta > 0 ? "+" : ""}${fmtNum(monthMissionDelta)}, цілі ${monthTargetDelta > 0 ? "+" : ""}${fmtNum(monthTargetDelta)}, ураження ${monthStrikeDelta > 0 ? "+" : ""}${fmtNum(monthStrikeDelta)}.`);
  } else if(lastMonth){
    lines.push(`- Поточний місяць у зрізі: ${lastMonth.label}; місій ${fmtNum(lastMonth.missionCount)}, цілей ${fmtNum(lastMonth.targetCount)}, ураження ${fmtNum(lastMonth.strikeCount)}.`);
  }
  if(analytics.lossCount === 0){
    lines.push("- У поточному зрізі втрат засобів не зафіксовано.");
  }
  if(analytics.deliveryMissionCount > 0 && analytics.deliveredCargoCount > 0){
    lines.push(`- У логістичних задачах зафіксовано ${fmtNum(analytics.deliveredCargoCount)} місій із доставленим вантажем.`);
  }

  lines.push("");
  lines.push("3. Проблематика");
  if(analytics.lossCount > 0){
    lines.push(`- Зафіксовано ${fmtNum(analytics.lossCount)} втрат засобів; потрібен окремий розбір причин і умов.`);
  }
  if(topRiskAsset && topRiskAsset.lossCount > 0){
    lines.push(`- Платформа з найбільшим ризиком: ${topRiskAsset.label} (втрати ${fmtNum(topRiskAsset.lossCount)} при ${fmtNum(topRiskAsset.total)} місіях).`);
  }
  if(missingTargetStatus > 0){
    lines.push(`- У ${fmtNum(missingTargetStatus)} місій із цілями не вказано статус цілі.`);
  }
  if(missingDuration > 0){
    lines.push(`- У ${fmtNum(missingDuration)} місій не заповнено тривалість.`);
  }
  if(missingControl > 0){
    lines.push(`- У ${fmtNum(missingControl)} місій не вказано тип керування.`);
  }

  lines.push("");
  lines.push("4. На що звернути увагу");
  lines.push(`- Початок місії заповнено у ${fmtNum(analytics.timeQuality.startFilledPercent)}% місій, завершення — у ${fmtNum(analytics.timeQuality.endFilledPercent)}%, тривалість — у ${fmtNum(analytics.timeQuality.durationFilledPercent)}%.`);
  lines.push(`- Найбільший акцент по цілях зараз на: ${topTarget}.`);
  lines.push(`- Для контролю ефективності по цілях варто окремо дивитись зв’язку: ціль → платформа → БК → статус.`);
  if(analytics.dayNight?.total){
    lines.push(`- Нічних місій у зрізі: ${fmtNum(analytics.dayNight.nightPercent)}%, денних — ${fmtNum(analytics.dayNight.dayPercent)}%.`);
  }

  lines.push("");
  lines.push("5. Рекомендовані дії");
  if(analytics.lossCount > 0){
    lines.push("- Окремо розібрати всі місії зі втратою засобу: платформа, тип задачі, тип цілі, статус цілі та обставини втрати.");
  }
  if(missingTargetStatus > 0){
    lines.push("- Посилити дисципліну заповнення статусу цілі по місіях, де ціль була вказана.");
  }
  if(missingControl > 0){
    lines.push("- Не залишати порожнім поле «Тип керування», щоб не втрачалась технічна аналітика.");
  }
  if(missingDuration > 0){
    lines.push("- Для місій БпЛА заповнювати тривалість, щоб середній час і динаміка по місяцях були точними.");
  }
  lines.push("- Для підрозділів у розсилці окремо підсвічувати: основну платформу, найпоширенішу ціль, головний тип задачі та проблемні втрати.");

  return lines.join("\n");

}

function buildDeltaBplaExecutiveReportHtml(analytics, modalKey=""){

  const reportText = buildDeltaBplaExecutiveReportText(analytics);
  const textareaId = `deltaBplaExecutiveReport_${modalKey || uid("delta_bpla_report")}`;
  return `
    <details class="item analytics-block delta-nrk-collapsible-section">
      <summary class="row delta-nrk-collapsible-summary"><div class="name">Короткий звіт / висновки</div></summary>
      <div class="delta-nrk-collapsible-body">
        <div class="item analytics-block">
          <div class="delta-report-copybar">
            <button type="button" class="btn ghost btn-mini" data-action="copyTextFromElement" data-arg1="${textareaId}">Копіювати текст</button>
          </div>
          <textarea id="${textareaId}" class="delta-report-textarea mono" readonly>${htmlesc(reportText)}</textarea>
        </div>
      </div>
    </details>
  `;

}

function buildDeltaBplaAnalytics(rows, title="", filters={}){

  const grid = Array.isArray(rows) ? rows : [];
  if(grid.length < 2) return null;

  const columns = detectDeltaBplaColumns(grid[0]);
  const detectedFormat = columns.reportUuid === 0 && columns.asset === 34 ? "Delta БпЛА 42 колонки" : "Delta БпЛА / alias-map";
  const allItems = grid.slice(1).map((row, index)=>{
    const reportUuid = columns.reportUuid >= 0 ? String(row?.[columns.reportUuid] || "").trim() : "";
    const unit = columns.unit >= 0 ? String(row?.[columns.unit] || "").trim() : "";
    const taskType = columns.taskType >= 0 ? String(row?.[columns.taskType] || "").trim() : "";
    const resultAt = columns.resultAt >= 0 ? String(row?.[columns.resultAt] || "").trim() : "";
    const startAt = columns.startAt >= 0 ? row?.[columns.startAt] : "";
    const endAt = columns.endAt >= 0 ? row?.[columns.endAt] : "";
    const durationRaw = columns.duration >= 0 ? row?.[columns.duration] : "";
    const targetType = columns.targetType >= 0 ? String(row?.[columns.targetType] || "").trim() : "";
    const targetDescription = columns.targetDescription >= 0 ? String(row?.[columns.targetDescription] || "").trim() : "";
    const targetQty = columns.targetQty >= 0 ? parseAnalyticsNumber(row?.[columns.targetQty]) : null;
    const targetStatus = columns.targetStatus >= 0 ? String(row?.[columns.targetStatus] || "").trim() : "";
    const ammoType = columns.ammoType >= 0 ? String(row?.[columns.ammoType] || "").trim() : "";
    const ammo = columns.ammo >= 0 ? String(row?.[columns.ammo] || "").trim() : "";
    const ammoQty = columns.ammoQty >= 0 ? parseAnalyticsNumber(row?.[columns.ammoQty]) : null;
    const cargo = columns.cargo >= 0 ? String(row?.[columns.cargo] || "").trim() : "";
    const cargoQty = columns.cargoQty >= 0 ? parseAnalyticsNumber(row?.[columns.cargoQty]) : null;
    const cargoStatus = columns.cargoStatus >= 0 ? String(row?.[columns.cargoStatus] || "").trim() : "";
    const asset = columns.asset >= 0 ? String(row?.[columns.asset] || "").trim() : "";
    const assetStatus = columns.assetStatus >= 0 ? String(row?.[columns.assetStatus] || "").trim() : "";
    const lossCircumstances = columns.lossCircumstances >= 0 ? String(row?.[columns.lossCircumstances] || "").trim() : "";
    const controlType = columns.controlType >= 0 ? String(row?.[columns.controlType] || "").trim() : "";
    const freqs = columns.freqs >= 0 ? String(row?.[columns.freqs] || "").trim() : "";
    const hasData = [reportUuid, unit, taskType, resultAt, String(startAt || "").trim(), String(endAt || "").trim(), String(durationRaw || "").trim(), targetType, targetDescription, targetStatus, ammoType, ammo, cargo, cargoStatus, asset, assetStatus, lossCircumstances, controlType, freqs].some(Boolean)
      || Number.isFinite(targetQty) || Number.isFinite(ammoQty) || Number.isFinite(cargoQty);
    if(!hasData) return null;

    const resultAtTs = parseDeltaDateTimeValue(resultAt);
    const startAtTs = parseDeltaDateTimeValue(startAt);
    const endAtTs = parseDeltaDateTimeValue(endAt);
    const providedDurationMinutes = parseDeltaDurationMinutes(durationRaw);
    const invalidTimeline = Number.isFinite(startAtTs) && Number.isFinite(endAtTs) && endAtTs < startAtTs;
    const dayNight = splitDeltaDayNight(startAtTs, endAtTs, resultAtTs);
    return {
      id: index + 1,
      reportUuid,
      unit,
      taskType,
      resultAt,
      resultAtTs,
      startAt: String(startAt || "").trim(),
      startAtTs,
      endAt: String(endAt || "").trim(),
      endAtTs,
      durationRaw: String(durationRaw || "").trim(),
      providedDurationMinutes,
      invalidTimeline,
      dayNightKind: dayNight.kind,
      dayMinutes: dayNight.dayMinutes,
      nightMinutes: dayNight.nightMinutes,
      effectiveDateRaw: resultAt || endAt || startAt || "",
      targetType,
      targetDescription,
      targetQty: Number.isFinite(targetQty) ? Number(targetQty) : 0,
      targetStatus,
      ammoType,
      ammo,
      ammoQty: Number.isFinite(ammoQty) ? Number(ammoQty) : 0,
      cargo,
      cargoQty: Number.isFinite(cargoQty) ? Number(cargoQty) : 0,
      cargoStatus,
      asset,
      assetStatus,
      lossCircumstances,
      controlType,
      freqs,
    };
  }).filter(Boolean);

  if(!allItems.length) return null;

  const unitFilter = String(filters.unit || "").trim();
  const taskTypeFilter = String(filters.taskType || "").trim();
  const assetFilter = String(filters.asset || "").trim();
  const unitOptions = Array.from(new Set(allItems.map(item=>item.unit).filter(Boolean))).sort((a,b)=>a.localeCompare(b, "uk"));
  const taskTypeOptions = Array.from(new Set(allItems.map(item=>item.taskType).filter(Boolean))).sort((a,b)=>a.localeCompare(b, "uk"));
  const assetOptions = summarizeNormalizedLabelCounts(allItems, item=>item.asset, normalizeDeltaPlatformKey).map(item=>item.label);
  const items = allItems.filter(item=>{
    if(unitFilter && item.unit !== unitFilter) return false;
    if(taskTypeFilter && item.taskType !== taskTypeFilter) return false;
    if(assetFilter && normalizeDeltaPlatformKey(item.asset) !== normalizeDeltaPlatformKey(assetFilter)) return false;
    return true;
  });

  const missions = aggregateDeltaBplaMissionItems(items);
  const recordCount = items.length;
  const missionCount = missions.length;
  const hasMissionUuid = items.some(item=>!!String(item.reportUuid || "").trim());

  const countBy = (source, getter)=>{
    const map = new Map();
    (Array.isArray(source) ? source : []).forEach(item=>{
      const key = String(getter(item) || "").trim();
      if(!key) return;
      map.set(key, (map.get(key) || 0) + 1);
    });
    return Array.from(map.entries()).map(([label, value])=>({label, value})).sort((a,b)=>b.value-a.value || String(a.label).localeCompare(String(b.label), "uk"));
  };
  const countMissionTags = (source, getter)=>{
    const map = new Map();
    (Array.isArray(source) ? source : []).forEach(item=>{
      const variants = new Map();
      (getter(item) || []).forEach(tag=>{
        const raw = String(tag || "").trim();
        if(!raw) return;
        const key = normalizeAnalyticsHeader(raw) || raw;
        if(!variants.has(key)) variants.set(key, raw);
      });
      variants.forEach((raw, key)=>{
        if(!map.has(key)) map.set(key, {label: raw, value: 0, variants: new Map()});
        const bucket = map.get(key);
        bucket.value += 1;
        bucket.variants.set(raw, (bucket.variants.get(raw) || 0) + 1);
      });
    });
    return Array.from(map.values()).map(bucket=>({
      label: Array.from(bucket.variants.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || bucket.label,
      value: bucket.value,
    })).sort((a,b)=>b.value-a.value || String(a.label).localeCompare(String(b.label), "uk"));
  };

  const taskTypes = countBy(missions, item=>item.taskType);
  const assets = summarizeNormalizedLabelCounts(missions, item=>item.asset, normalizeDeltaPlatformKey);
  const unitsByMissions = countBy(missions, item=>item.unit);
  const controlTypes = summarizeNormalizedLabelCounts(missions, item=>item.controlType);
  const targetTypes = countMissionTags(missions, item=>item.targetTypes);
  const targetStatuses = countMissionTags(missions, item=>item.targetStatuses);
  const ammoTypes = countMissionTags(missions, item=>item.ammoTypes.length ? item.ammoTypes : item.ammoNames);
  const cargoStatuses = countMissionTags(missions, item=>item.cargoStatuses);
  const totalTargets = missions.reduce((sum, item)=>sum + (Number(item.targetCount) || 0), 0);
  const targetMissionCount = missions.filter(item=>item.targetTypes.length || item.targetStatuses.length || item.targetCount > 0).length;
  const totalAmmoQty = missions.reduce((sum, item)=>sum + (Number(item.ammoQty) || 0), 0);
  const deliveryMissionCount = missions.filter(item=>/достав/i.test(String(item.taskType || ""))).length;
  const deliveredCargoCount = missions.filter(item=>(item.cargoStatuses || []).some(tag=>/доставлено/i.test(tag))).length;
  const reconMissionCount = missions.filter(item=>/розвід|цілевказ/i.test(String(item.taskType || ""))).length;
  const strikeMissionCount = missions.filter(item=>/уражен/i.test(String(item.taskType || ""))).length;
  const miningMissionCount = missions.filter(item=>/мінуван|загороджен/i.test(String(item.taskType || ""))).length;
  const returnedCount = missions.filter(item=>item.reliabilityKind === "returned").length;
  const lossCount = missions.filter(item=>item.reliabilityKind === "loss").length;
  const reliabilityRate = missionCount ? ((returnedCount / missionCount) * 100) : 0;

  const assetStatsMap = new Map();
  missions.forEach(item=>{
    const rawLabel = String(item.asset || "").trim();
    if(!rawLabel) return;
    const key = normalizeDeltaPlatformKey(rawLabel) || rawLabel;
    if(!assetStatsMap.has(key)) assetStatsMap.set(key, {label: rawLabel, total:0, targetCount:0, lossCount:0, returnedCount:0, variants:new Map()});
    const bucket = assetStatsMap.get(key);
    bucket.total += 1;
    bucket.targetCount += Number(item.targetCount) || 0;
    if(item.reliabilityKind === "loss") bucket.lossCount += 1;
    if(item.reliabilityKind === "returned") bucket.returnedCount += 1;
    bucket.variants.set(rawLabel, (bucket.variants.get(rawLabel) || 0) + 1);
  });
  const assetStats = Array.from(assetStatsMap.values()).map(bucket=>({
    label: Array.from(bucket.variants.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || bucket.label,
    total: bucket.total,
    targetCount: bucket.targetCount,
    lossCount: bucket.lossCount,
    returnedCount: bucket.returnedCount,
    reliabilityRate: bucket.total ? ((bucket.returnedCount / bucket.total) * 100) : 0,
  })).sort((a,b)=>b.total-a.total || b.targetCount-a.targetCount || String(a.label).localeCompare(String(b.label), "uk"));

  const unitTaskStatsMap = new Map();
  missions.forEach(item=>{
    const rawLabel = String(item.unit || "Без підрозділу").trim() || "Без підрозділу";
    if(!unitTaskStatsMap.has(rawLabel)){
      unitTaskStatsMap.set(rawLabel, {
        label: rawLabel,
        total: 0,
        reconCount: 0,
        strikeCount: 0,
        deliveryCount: 0,
        miningCount: 0,
        targetCount: 0,
        lossCount: 0,
      });
    }
    const bucket = unitTaskStatsMap.get(rawLabel);
    bucket.total += 1;
    if(/розвід|цілевказ/i.test(String(item.taskType || ""))) bucket.reconCount += 1;
    if(/уражен/i.test(String(item.taskType || ""))) bucket.strikeCount += 1;
    if(/достав/i.test(String(item.taskType || ""))) bucket.deliveryCount += 1;
    if(/мінуван|загороджен/i.test(String(item.taskType || ""))) bucket.miningCount += 1;
    bucket.targetCount += Number(item.targetCount) || 0;
    if(item.reliabilityKind === "loss") bucket.lossCount += 1;
  });
  const unitTaskStats = Array.from(unitTaskStatsMap.values()).map(bucket=>({
    ...bucket,
    reconPercent: bucket.total ? Math.round((bucket.reconCount / bucket.total) * 100) : 0,
    strikePercent: bucket.total ? Math.round((bucket.strikeCount / bucket.total) * 100) : 0,
  })).sort((a,b)=>((b.reconCount + b.strikeCount) - (a.reconCount + a.strikeCount)) || b.total-a.total || String(a.label).localeCompare(String(b.label), "uk"));

  const dayNightByUnitMap = new Map();
  const dayNightByAssetMap = new Map();
  let dayCount = 0;
  let nightCount = 0;
  missions.forEach(item=>{
    const kind = item.dayNightKind;
    if(kind !== "day" && kind !== "night") return;
    const unitKey = String(item.unit || "Без підрозділу").trim();
    if(!dayNightByUnitMap.has(unitKey)) dayNightByUnitMap.set(unitKey, {label: unitKey, total:0, dayCount:0, nightCount:0});
    const unitBucket = dayNightByUnitMap.get(unitKey);
    unitBucket.total += 1;
    if(kind === "day"){ unitBucket.dayCount += 1; dayCount += 1; } else { unitBucket.nightCount += 1; nightCount += 1; }
    const assetRaw = String(item.asset || "Без платформи").trim();
    const assetKey = normalizeDeltaPlatformKey(assetRaw) || assetRaw;
    if(!dayNightByAssetMap.has(assetKey)) dayNightByAssetMap.set(assetKey, {label:assetRaw, total:0, dayCount:0, nightCount:0, variants:new Map()});
    const assetBucket = dayNightByAssetMap.get(assetKey);
    assetBucket.total += 1;
    assetBucket.variants.set(assetRaw, (assetBucket.variants.get(assetRaw) || 0) + 1);
    if(kind === "day") assetBucket.dayCount += 1; else assetBucket.nightCount += 1;
  });
  const dayNight = {
    dayCount,
    nightCount,
    total: dayCount + nightCount,
    dayPercent: (dayCount + nightCount) ? Math.round((dayCount / (dayCount + nightCount)) * 100) : 0,
    nightPercent: (dayCount + nightCount) ? Math.round((nightCount / (dayCount + nightCount)) * 100) : 0,
    units: Array.from(dayNightByUnitMap.values()).map(item=>({...item, dayPercent:item.total ? Math.round((item.dayCount / item.total) * 100) : 0, nightPercent:item.total ? Math.round((item.nightCount / item.total) * 100) : 0})).sort((a,b)=>b.nightCount-a.nightCount || b.total-a.total || String(a.label).localeCompare(String(b.label), "uk")),
    assets: Array.from(dayNightByAssetMap.values()).map(item=>({label:Array.from(item.variants.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || item.label, total:item.total, dayCount:item.dayCount, nightCount:item.nightCount, dayPercent:item.total ? Math.round((item.dayCount / item.total) * 100) : 0, nightPercent:item.total ? Math.round((item.nightCount / item.total) * 100) : 0})).sort((a,b)=>b.nightCount-a.nightCount || b.total-a.total || String(a.label).localeCompare(String(b.label), "uk")),
  };

  const startFilledCount = missions.filter(item=>(item.rows || []).some(row=>!!String(row?.startAt || "").trim())).length;
  const endFilledCount = missions.filter(item=>(item.rows || []).some(row=>!!String(row?.endAt || "").trim())).length;
  const resultFilledCount = missions.filter(item=>(item.rows || []).some(row=>!!String(row?.resultAt || "").trim())).length;
  const durationFilledCount = missions.filter(item=>(item.rows || []).some(row=>!!String(row?.durationRaw || "").trim())).length;
  const fullTimelineCount = missions.filter(item=>{
    const missionRows = item.rows || [];
    return missionRows.some(row=>!!String(row?.startAt || "").trim()) && missionRows.some(row=>!!String(row?.endAt || "").trim()) && missionRows.some(row=>!!String(row?.resultAt || "").trim());
  }).length;
  const anyTimeCount = missions.filter(item=>(item.rows || []).some(row=>row?.startAt || row?.endAt || row?.resultAt || row?.durationRaw)).length;
  const invalidTimelineCount = missions.filter(item=>item.invalidTimeline).length;
  const durationValues = missions.map(item=>item.missionDurationMinutes).filter(value=>Number.isFinite(value) && value >= 0);
  const validDurationCount = durationValues.length;
  const durationSorted = durationValues.slice().sort((a,b)=>a-b);
  const avgDurationMinutes = validDurationCount ? durationValues.reduce((sum, value)=>sum + value, 0) / validDurationCount : 0;
  const medianDurationMinutes = !validDurationCount ? 0 : (validDurationCount % 2 ? durationSorted[(validDurationCount - 1) / 2] : ((durationSorted[(validDurationCount / 2) - 1] + durationSorted[validDurationCount / 2]) / 2));
  const maxDurationMinutes = validDurationCount ? durationSorted[durationSorted.length - 1] : 0;
  const percentOf = value=> missionCount ? Math.round((value / missionCount) * 100) : 0;
  const timeQuality = {totalCount: missionCount, missionTotalCount: missionCount, startFilledCount, endFilledCount, resultFilledCount, durationFilledCount, fullTimelineCount, anyTimeCount, validDurationCount, invalidTimelineCount, startFilledPercent: percentOf(startFilledCount), endFilledPercent: percentOf(endFilledCount), resultFilledPercent: percentOf(resultFilledCount), durationFilledPercent: percentOf(durationFilledCount), fullTimelinePercent: percentOf(fullTimelineCount), avgDurationMinutes, medianDurationMinutes, maxDurationMinutes};

  const monthMap = new Map();
  missions.forEach(item=>{
    const monthKey = parseDeltaMonthKey(item.resultAt || item.endAt || item.startAt || item.effectiveDateRaw) || "no-date";
    const existing = monthMap.get(monthKey) || {key:monthKey, label:monthKey === "no-date" ? "Без дати" : formatDeltaMonthLabel(monthKey), missionCount:0, reconCount:0, strikeCount:0, deliveryCount:0, miningCount:0, targetCount:0, targetMissionCount:0, deliveredCargoCount:0, lossCount:0, returnedCount:0};
    existing.missionCount += 1;
    if(/розвід|цілевказ/i.test(String(item.taskType || ""))) existing.reconCount += 1;
    if(/уражен/i.test(String(item.taskType || ""))) existing.strikeCount += 1;
    if(/достав/i.test(String(item.taskType || ""))) existing.deliveryCount += 1;
    if(/мінуван|загороджен/i.test(String(item.taskType || ""))) existing.miningCount += 1;
    existing.targetCount += Number(item.targetCount) || 0;
    if((Number(item.targetCount) || 0) > 0 || (item.targetTypes || []).length || (item.targetStatuses || []).length) existing.targetMissionCount += 1;
    if((item.cargoStatuses || []).some(tag=>/доставлено/i.test(tag))) existing.deliveredCargoCount += 1;
    if(item.reliabilityKind === "loss") existing.lossCount += 1;
    if(item.reliabilityKind === "returned") existing.returnedCount += 1;
    monthMap.set(monthKey, existing);
  });
  const months = Array.from(monthMap.values()).sort((a,b)=>{ if(a.key === "no-date") return 1; if(b.key === "no-date") return -1; return String(a.key).localeCompare(String(b.key), "uk"); });

  return {
    title,
    detectedFormat,
    sourceRows: Math.max(0, grid.length - 1),
    parsedRows: missionCount,
    recordCount,
    items,
    missions,
    missionCount,
    taskTypes,
    topTaskType: taskTypes[0] || null,
    assets,
    topAsset: assets[0] || null,
    assetStats,
    unitTaskStats,
    unitsByMissions,
    controlTypes,
    targetTypes,
    targetStatuses,
    topTargetStatus: targetStatuses[0] || null,
    ammoTypes,
    topAmmoType: ammoTypes[0] || null,
    cargoStatuses,
    totalTargets,
    targetMissionCount,
    totalAmmoQty,
    deliveryMissionCount,
    deliveredCargoCount,
    reconMissionCount,
    strikeMissionCount,
    miningMissionCount,
    returnedCount,
    lossCount,
    reliabilityRate,
    dayNight,
    months,
    timeQuality,
    unitOptions,
    taskTypeOptions,
    assetOptions,
    filters: {unit: unitFilter, taskType: taskTypeFilter, asset: assetFilter},
    totalParsedRows: allItems.length,
    missionGroupingLabel: hasMissionUuid ? "UUID Звіту" : "Підрозділ + Тип задачі + Дата результату + Платформа",
  };

}

function buildDeltaBplaAnalyticsModalHtml(rows, title="", opts={}){

  const modalKey = String(opts.modalKey || "");
  const currentFilters = opts.filters && typeof opts.filters === "object" ? opts.filters : {};
  const analytics = buildDeltaBplaAnalytics(rows, title, currentFilters);
  if(!analytics){
    return `<div class="hint">Не вдалося розпізнати Delta / БпЛА. Очікуються колонки на кшталт: Підрозділ, Тип задачі, Тип цілі, Статус цілі, Боєприпас, Засіб.</div>`;
  }
  const wrapDeltaBplaCollapsible = (titleText, bodyHtml, startOpen=false)=>`
    <details class="item analytics-block delta-nrk-collapsible-section" ${startOpen ? "open" : ""}>
      <summary class="row delta-nrk-collapsible-summary"><div class="name">${htmlesc(titleText)}</div></summary>
      <div class="delta-nrk-collapsible-body">${bodyHtml}</div>
    </details>
  `;

  const filtersBlock = `
    <div class="item analytics-block delta-nrk-filters">
      <div class="delta-nrk-filter-grid">
        <label class="delta-nrk-filter-field"><span>Підрозділ</span><select id="deltaUnitFilter_${modalKey}"><option value="">Усі підрозділи</option>${analytics.unitOptions.map(item=>`<option value="${attrEsc(item)}" ${item === analytics.filters.unit ? "selected" : ""}>${htmlesc(item)}</option>`).join("")}</select></label>
        <label class="delta-nrk-filter-field"><span>Тип задачі</span><select id="deltaTaskTypeFilter_${modalKey}"><option value="">Усі типи задач</option>${analytics.taskTypeOptions.map(item=>`<option value="${attrEsc(item)}" ${item === analytics.filters.taskType ? "selected" : ""}>${htmlesc(item)}</option>`).join("")}</select></label>
        <label class="delta-nrk-filter-field"><span>Платформа</span><select id="deltaAssetFilter_${modalKey}"><option value="">Усі платформи</option>${analytics.assetOptions.map(item=>`<option value="${attrEsc(item)}" ${item === analytics.filters.asset ? "selected" : ""}>${htmlesc(item)}</option>`).join("")}</select></label>
        <div class="delta-nrk-filter-actions"><button class="btn primary btn-mini" data-action="applyDeltaNrkAnalyticsFilters" data-arg1="${modalKey}">Застосувати</button><button class="btn ghost btn-mini" data-action="resetDeltaNrkAnalyticsFilters" data-arg1="${modalKey}">Скинути</button></div>
      </div>
      <div class="delta-nrk-filter-summary">
        <span class="delta-nrk-filter-chip">${analytics.filters.unit ? `Підрозділ: ${htmlesc(analytics.filters.unit)}` : "Усі підрозділи"}</span>
        <span class="delta-nrk-filter-chip">${analytics.filters.taskType ? `Тип задачі: ${htmlesc(analytics.filters.taskType)}` : "Усі типи задач"}</span>
        <span class="delta-nrk-filter-chip">${analytics.filters.asset ? `Платформа: ${htmlesc(analytics.filters.asset)}` : "Усі платформи"}</span>
        <span class="delta-nrk-filter-chip mono">Місій у зрізі: ${fmtNum(analytics.missionCount)}</span>
        <span class="delta-nrk-filter-chip mono">Записів у зрізі: ${fmtNum(analytics.recordCount)}</span>
      </div>
    </div>
  `;

  const diagnosticsBlock = `
    <div class="item analytics-block delta-nrk-diagnostics">
      <div class="row"><div class="name">Перевірка імпорту</div></div>
      <div class="delta-nrk-diagnostics-grid">
        <div class="delta-nrk-diagnostics-item"><div class="delta-nrk-diagnostics-k">Формат</div><div class="delta-nrk-diagnostics-v">${htmlesc(analytics.detectedFormat || "—")}</div></div>
        <div class="delta-nrk-diagnostics-item"><div class="delta-nrk-diagnostics-k">Рядків у таблиці</div><div class="delta-nrk-diagnostics-v mono">${fmtNum(analytics.sourceRows)}</div></div>
        <div class="delta-nrk-diagnostics-item"><div class="delta-nrk-diagnostics-k">Розпізнано записів</div><div class="delta-nrk-diagnostics-v mono">${fmtNum(analytics.recordCount)}</div></div>
        <div class="delta-nrk-diagnostics-item"><div class="delta-nrk-diagnostics-k">Унікальних місій</div><div class="delta-nrk-diagnostics-v mono">${fmtNum(analytics.missionCount)}</div></div>
        <div class="delta-nrk-diagnostics-item"><div class="delta-nrk-diagnostics-k">Групування місій</div><div class="delta-nrk-diagnostics-v">${htmlesc(analytics.missionGroupingLabel || "—")}</div></div>
      </div>
    </div>
  `;
  const countingLogicBlock = `
    <div class="item analytics-block delta-nrk-diagnostics">
      <div class="row"><div class="name">Логіка підрахунку</div></div>
      <div class="delta-nrk-filter-summary">
        <span class="delta-nrk-filter-chip">Основна аналітика і час рахуються по унікальних місіях.</span>
        <span class="delta-nrk-filter-chip">По записах лишається тільки технічна перевірка імпорту.</span>
      </div>
    </div>
  `;

  const platformsRows = analytics.assetStats.map(item=>({label:item.label, valueText:fmtNum(item.total), meta:`${fmtNum(analytics.missionCount ? Math.round((item.total / analytics.missionCount) * 100) : 0)}% місій · цілі ${fmtNum(item.targetCount)} · втрати ${fmtNum(item.lossCount)}`, tone:"b-blue"}));
  const unitTaskRows = analytics.unitTaskStats.map(item=>({label:item.label, valueText:`${fmtNum(item.reconCount)} / ${fmtNum(item.strikeCount)}`, meta:`Розвідка / Ураження · Доставка ${fmtNum(item.deliveryCount)} · ${fmtNum(item.total)} місій`, tone:"b-violet"}));
  const assetLossRows = analytics.assetStats.filter(item=>item.lossCount > 0).map(item=>({label:item.label, valueText:fmtNum(item.lossCount), meta:`Повернення ${fmtNum(item.returnedCount)} · Надійність ${fmtNum(item.reliabilityRate)}% · ${fmtNum(item.total)} місій`, tone:"b-danger"}));
  const lossMissions = analytics.missions.filter(item=>item.reliabilityKind === "loss");
  const lossCauseRows = summarizeNormalizedLabelCounts(lossMissions, item=>String(item.lossCircumstances || "").trim() || "Не вказано").map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: `${fmtNum(lossMissions.length ? Math.round((item.value / lossMissions.length) * 100) : 0)}% втрат`,
    tone: "b-danger",
  }));
  const lossTaskMap = new Map();
  lossMissions.forEach(item=>{
    const label = String(item.taskType || "").trim() || "Не визначено";
    lossTaskMap.set(label, (lossTaskMap.get(label) || 0) + 1);
  });
  const lossTaskRows = Array.from(lossTaskMap.entries())
    .map(([label, value])=>({
      label,
      valueText: fmtNum(value),
      meta: `${fmtNum(lossMissions.length ? Math.round((value / lossMissions.length) * 100) : 0)}% втрат`,
      tone: "b-violet",
    }))
    .sort((a,b)=>Number(String(b.valueText || "0").replace(/\s+/g, "")) - Number(String(a.valueText || "0").replace(/\s+/g, "")) || String(a.label).localeCompare(String(b.label), "uk"));
  const lossDayNightRows = [
    {label:"День", value: lossMissions.filter(item=>item.dayNightKind === "day").length, tone:"b-blue"},
    {label:"Ніч", value: lossMissions.filter(item=>item.dayNightKind === "night").length, tone:"b-danger"},
    {label:"Без часу", value: lossMissions.filter(item=>!item.dayNightKind).length, tone:"b-violet"},
  ].filter(item=>item.value > 0).map(item=>({
    label: item.label,
    valueText: fmtNum(item.value),
    meta: `${fmtNum(lossMissions.length ? Math.round((item.value / lossMissions.length) * 100) : 0)}% втрат`,
    tone: item.tone,
  }));
  const lossTargetMap = new Map();
  lossMissions.forEach(item=>{
    const variants = new Map();
    (item.targetTypes || []).forEach(tag=>{
      const raw = String(tag || "").trim();
      if(!raw) return;
      const key = normalizeAnalyticsHeader(raw) || raw;
      if(!variants.has(key)) variants.set(key, raw);
    });
    variants.forEach((raw, key)=>{
      if(!lossTargetMap.has(key)) lossTargetMap.set(key, {label: raw, value: 0, variants: new Map()});
      const bucket = lossTargetMap.get(key);
      bucket.value += 1;
      bucket.variants.set(raw, (bucket.variants.get(raw) || 0) + 1);
    });
  });
  const lossTargetRows = Array.from(lossTargetMap.values())
    .map(bucket=>({
      label: Array.from(bucket.variants.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || bucket.label,
      valueText: fmtNum(bucket.value),
      meta: `${fmtNum(lossMissions.length ? Math.round((bucket.value / lossMissions.length) * 100) : 0)}% втрат із цією ціллю`,
      tone: "b-blue",
    }))
    .sort((a,b)=>Number(String(b.valueText || "0").replace(/\s+/g, "")) - Number(String(a.valueText || "0").replace(/\s+/g, "")) || String(a.label).localeCompare(String(b.label), "uk"));
  const taskTypeRows = analytics.taskTypes.map(item=>({label:item.label, valueText:fmtNum(item.value), meta:`${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій`, tone:"b-violet"}));
  const targetTypeRows = analytics.targetTypes.map(item=>({label:item.label, valueText:fmtNum(item.value), meta:`${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій, де була ця ціль`, tone:"b-blue"}));
  const targetStatusRows = analytics.targetStatuses.map(item=>({label:item.label, valueText:fmtNum(item.value), meta:`${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій, де був цей статус`, tone:"b-ok"}));
  const targetSummaryMap = new Map();
  const addAssocCount = (map, value)=>{
    const key = String(value || "").trim();
    if(!key) return;
    map.set(key, (map.get(key) || 0) + 1);
  };
  const topAssocLabel = map=>{
    if(!(map instanceof Map) || !map.size) return "";
    return Array.from(map.entries()).sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))[0]?.[0] || "";
  };
  analytics.missions.forEach(item=>{
    const missionTargetTypes = item.targetTypes?.length ? item.targetTypes : ((Number(item.targetCount) || 0) > 0 ? ["Не визначено"] : []);
    missionTargetTypes.forEach(targetLabel=>{
      const label = String(targetLabel || "").trim();
      if(!label) return;
      if(!targetSummaryMap.has(label)){
        targetSummaryMap.set(label, {
          label,
          missionCount: 0,
          totalTargets: 0,
          statuses: new Map(),
          units: new Map(),
          assets: new Map(),
          ammoTypes: new Map(),
          ammoNames: new Map(),
          taskTypes: new Map(),
        });
      }
      const bucket = targetSummaryMap.get(label);
      bucket.missionCount += 1;
      bucket.totalTargets += Number(item.targetCount) || 0;
      (item.targetStatuses || []).forEach(status=>addAssocCount(bucket.statuses, status));
      addAssocCount(bucket.units, item.unit);
      addAssocCount(bucket.assets, item.asset);
      (item.ammoTypes || []).forEach(ammoType=>addAssocCount(bucket.ammoTypes, ammoType));
      (item.ammoNames || []).forEach(ammoName=>addAssocCount(bucket.ammoNames, ammoName));
      addAssocCount(bucket.taskTypes, item.taskType);
    });
  });
  const targetOverviewRows = Array.from(targetSummaryMap.values()).map(bucket=>{
    const statusParts = Array.from(bucket.statuses.entries())
      .sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))
      .slice(0, 3)
      .map(([label, value])=>`${label} ${fmtNum(value)}`);
    return {
      label: bucket.label,
      valueText: fmtNum(bucket.missionCount),
      meta: [
        `${fmtNum(analytics.missionCount ? Math.round((bucket.missionCount / analytics.missionCount) * 100) : 0)}% місій`,
        bucket.totalTargets > 0 ? `цілей ${fmtNum(bucket.totalTargets)}` : "",
        statusParts.length ? `статуси ${statusParts.join(" · ")}` : "",
        topAssocLabel(bucket.assets) ? `платф. ${topAssocLabel(bucket.assets)}` : "",
        topAssocLabel(bucket.ammoTypes) ? `БК ${topAssocLabel(bucket.ammoTypes)}` : (topAssocLabel(bucket.ammoNames) ? `боєприпас ${topAssocLabel(bucket.ammoNames)}` : ""),
      ].filter(Boolean).join(" · "),
      tone: "b-blue",
    };
  }).sort((a,b)=>Number(String(b.valueText || "0").replace(/\s+/g, "")) - Number(String(a.valueText || "0").replace(/\s+/g, "")) || String(a.label).localeCompare(String(b.label), "uk"));
  const targetPlatformRows = Array.from(targetSummaryMap.values()).map(bucket=>({
    label: bucket.label,
    valueText: topAssocLabel(bucket.assets) || "—",
    meta: [
      `${fmtNum(bucket.missionCount)} місій із цією ціллю`,
      topAssocLabel(bucket.statuses) ? `статус ${topAssocLabel(bucket.statuses)}` : "",
      bucket.totalTargets > 0 ? `цілей ${fmtNum(bucket.totalTargets)}` : "",
    ].filter(Boolean).join(" · "),
    tone: "b-blue",
  })).filter(item=>item.valueText !== "—")
    .sort((a,b)=>String(a.label).localeCompare(String(b.label), "uk"));
  const targetUnitRows = Array.from(targetSummaryMap.values()).map(bucket=>({
    label: bucket.label,
    valueText: topAssocLabel(bucket.units) || "—",
    meta: [
      `${fmtNum(bucket.missionCount)} місій із цією ціллю`,
      topAssocLabel(bucket.assets) ? `платф. ${topAssocLabel(bucket.assets)}` : "",
      topAssocLabel(bucket.statuses) ? `статус ${topAssocLabel(bucket.statuses)}` : "",
    ].filter(Boolean).join(" · "),
    tone: "b-green",
  })).filter(item=>item.valueText !== "—")
    .sort((a,b)=>String(a.label).localeCompare(String(b.label), "uk"));
  const targetTaskRows = Array.from(targetSummaryMap.values()).map(bucket=>({
    label: bucket.label,
    valueText: topAssocLabel(bucket.taskTypes) || "—",
    meta: [
      `${fmtNum(bucket.missionCount)} місій із цією ціллю`,
      topAssocLabel(bucket.assets) ? `платф. ${topAssocLabel(bucket.assets)}` : "",
      topAssocLabel(bucket.statuses) ? `статус ${topAssocLabel(bucket.statuses)}` : "",
    ].filter(Boolean).join(" · "),
    tone: "b-violet",
  })).filter(item=>item.valueText !== "—")
    .sort((a,b)=>String(a.label).localeCompare(String(b.label), "uk"));
  const targetStatusMatrixRows = Array.from(targetSummaryMap.values()).map(bucket=>{
    const statusParts = Array.from(bucket.statuses.entries())
      .sort((a,b)=>b[1]-a[1] || String(a[0]).localeCompare(String(b[0]), "uk"))
      .slice(0, 3)
      .map(([label, value])=>`${label} ${fmtNum(value)}`);
    return {
      label: bucket.label,
      valueText: statusParts[0] || "—",
      meta: [
        statusParts.slice(1).join(" · "),
        `${fmtNum(bucket.missionCount)} місій із цією ціллю`,
        bucket.totalTargets > 0 ? `цілей ${fmtNum(bucket.totalTargets)}` : "",
      ].filter(Boolean).join(" · "),
      tone: "b-ok",
    };
  }).filter(item=>item.valueText !== "—")
    .sort((a,b)=>String(a.label).localeCompare(String(b.label), "uk"));
  const targetAmmoRows = Array.from(targetSummaryMap.values()).map(bucket=>({
    label: bucket.label,
    valueText: topAssocLabel(bucket.ammoTypes) || topAssocLabel(bucket.ammoNames) || "—",
    meta: [
      `${fmtNum(bucket.missionCount)} місій із цією ціллю`,
      topAssocLabel(bucket.assets) ? `платф. ${topAssocLabel(bucket.assets)}` : "",
      topAssocLabel(bucket.statuses) ? `статус ${topAssocLabel(bucket.statuses)}` : "",
    ].filter(Boolean).join(" · "),
    tone: "b-violet",
  })).filter(item=>item.valueText !== "—")
    .sort((a,b)=>String(a.label).localeCompare(String(b.label), "uk"));
  const ammoRows = analytics.ammoTypes.map(item=>({label:item.label, valueText:fmtNum(item.value), meta:`${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій, де був цей тип БК`, tone:"b-blue"}));
  const deliveryRows = analytics.cargoStatuses.map(item=>({label:item.label, valueText:fmtNum(item.value), meta:`${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій, де був цей статус вантажу`, tone:"b-ok"}));
  const unitsRows = analytics.unitsByMissions.map(item=>({label:item.label, valueText:fmtNum(item.value), meta:`${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій`, tone:"b-blue"}));
  const controlRows = analytics.controlTypes.map(item=>({label:item.label, valueText:fmtNum(item.value), meta:`${fmtNum(analytics.missionCount ? Math.round((item.value / analytics.missionCount) * 100) : 0)}% місій`, tone:"b-violet"}));

  const reliabilityGroups = [
    {label:"Повернення", tone:"b-ok", items: analytics.missions.filter(item=>item.reliabilityKind === "returned"), meta:"успішне повернення засобу"},
    {label:"Втрата", tone:"b-danger", items: analytics.missions.filter(item=>item.reliabilityKind === "loss"), meta:"втрата засобу"},
  ];
  const reliabilityRows = reliabilityGroups.map(group=>{
    const detailRows = group.items.map(item=>({label:item.unit || "Без підрозділу", valueText:item.resultAt || item.endAt || item.startAt || "Без дати", meta:[item.asset ? `Платформа: ${item.asset}` : "", item.taskType ? `Тип задачі: ${item.taskType}` : "", item.targetTypes?.length ? `Ціль: ${item.targetTypes.join(", ")}` : "", item.targetStatuses?.length ? `Статус: ${item.targetStatuses.join(", ")}` : "", item.ammoTypes?.length ? `БК: ${item.ammoTypes.join(", ")}` : "", item.lossCircumstances ? `Втрати: ${item.lossCircumstances}` : "", item.missionDurationMinutes ? `Час: ${formatDurationMinutes(item.missionDurationMinutes)}` : ""].filter(Boolean).join(" · "), tone:group.tone}));
    const modalDetailKey = registerRenderedTableModal(`${analytics.title || "Delta / БпЛА"} · ${group.label}`, buildDeltaNrkInsightModalHtml([{title:`${group.label} · місії`, summary:`${fmtNum(group.items.length)} місій у цьому статусі`, rows:detailRows, emptyText:"Місій у цьому статусі поки немає."}]));
    return {label:group.label, valueText:fmtNum(group.items.length), meta:`${fmtNum(analytics.missionCount ? Math.round((group.items.length / analytics.missionCount) * 100) : 0)}% місій · ${group.meta}`, tone:group.tone, modalKey:modalDetailKey};
  });

  const detailModal = (modalTitle, sectionTitle, summary, rowsData, emptyText)=>registerRenderedTableModal(modalTitle, buildDeltaNrkInsightModalHtml([{title: sectionTitle, summary, rows: rowsData, emptyText}]));
  const platformsModalKey = detailModal(`${analytics.title || "Delta / БпЛА"} · Платформи`, "Платформи · по місіях", `Усього платформ: ${fmtNum(platformsRows.length)} · місій: ${fmtNum(analytics.missionCount)}`, platformsRows, "По платформах даних поки немає.");
  const unitTaskModalKey = detailModal(`${analytics.title || "Delta / БпЛА"} · Розвідка / Ураження`, "Розвідка / Ураження · по підрозділах", `Формат значення: Розвідка / Ураження · підрозділів: ${fmtNum(unitTaskRows.length)}`, unitTaskRows, "По підрозділах даних поки немає.");
  const lossMissionRows = lossMissions.map(item=>({
    label: item.unit || "Без підрозділу",
    valueText: item.resultAt || item.endAt || item.startAt || "Без дати",
    meta: [
      item.asset ? `Платформа: ${item.asset}` : "",
      item.taskType ? `Тип задачі: ${item.taskType}` : "",
      item.targetTypes?.length ? `Ціль: ${item.targetTypes.join(", ")}` : "",
      item.targetStatuses?.length ? `Статус цілі: ${item.targetStatuses.join(", ")}` : "",
      item.lossCircumstances ? `Причина: ${item.lossCircumstances}` : "",
      item.dayNightKind === "day" ? "День" : (item.dayNightKind === "night" ? "Ніч" : ""),
      item.missionDurationMinutes ? `Час: ${formatDurationMinutes(item.missionDurationMinutes)}` : "",
    ].filter(Boolean).join(" · "),
    tone: "b-danger",
  }));
  const assetLossModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / БпЛА"} · Втрати`,
    buildDeltaNrkInsightModalHtml([
      {
        title: "Втрати · по платформах",
        summary: `Усього втрат: ${fmtNum(lossMissions.length)} · платформ із втратами: ${fmtNum(assetLossRows.length)}`,
        rows: assetLossRows,
        emptyText: "Втрат по платформах поки немає.",
      },
      {
        title: "Причини втрат",
        summary: `Унікальних причин: ${fmtNum(lossCauseRows.length)}`,
        rows: lossCauseRows,
        emptyText: "Причини втрат поки не вказані.",
      },
      {
        title: "Втрати · по типах задач",
        summary: `Розподіл втрат за типами задач`,
        rows: lossTaskRows,
        emptyText: "По типах задач втрат поки немає.",
      },
      {
        title: "Втрати · день / ніч",
        summary: `Коли найчастіше втрачаються засоби`,
        rows: lossDayNightRows,
        emptyText: "По часу втрат даних поки немає.",
      },
      {
        title: "Втрати · по типах цілей",
        summary: `Які цілі частіше зустрічаються у втрачених місіях`,
        rows: lossTargetRows,
        emptyText: "По типах цілей втрат поки немає.",
      },
      {
        title: "Місії зі втратою",
        summary: `${fmtNum(lossMissions.length)} місій зі втратою засобу`,
        rows: lossMissionRows,
        emptyText: "Місій зі втратою поки немає.",
      }
    ])
  );
  const taskTypesModalKey = detailModal(`${analytics.title || "Delta / БпЛА"} · Типи задач`, "Типи задач · по місіях", `Унікальних типів: ${fmtNum(taskTypeRows.length)}`, taskTypeRows, "По типах задач даних поки немає.");
  const targetTypesModalKey = registerRenderedTableModal(
    `${analytics.title || "Delta / БпЛА"} · Цілі`,
    buildDeltaNrkInsightModalHtml([
      {
        title: "Типи цілей · по місіях",
        summary: `Унікальних типів цілей: ${fmtNum(targetTypeRows.length)} · місій з цілями: ${fmtNum(analytics.targetMissionCount)}`,
        rows: targetOverviewRows.length ? targetOverviewRows : targetTypeRows,
        emptyText: "По типах цілей даних поки немає.",
      },
      {
        title: "Цілі × Платформи",
        summary: `Яка платформа найчастіше працює по кожній цілі`,
        rows: targetPlatformRows,
        emptyText: "По зв’язку цілей з платформами даних поки немає.",
      },
      {
        title: "Цілі × Підрозділи",
        summary: `Який підрозділ найчастіше працює по кожній цілі`,
        rows: targetUnitRows,
        emptyText: "По зв’язку цілей з підрозділами даних поки немає.",
      },
      {
        title: "Цілі × Тип задачі",
        summary: `У якому типі задачі найчастіше зустрічається кожна ціль`,
        rows: targetTaskRows,
        emptyText: "По зв’язку цілей з типами задач даних поки немає.",
      },
      {
        title: "Цілі × Статуси",
        summary: `Які статуси найчастіше мають різні типи цілей`,
        rows: targetStatusMatrixRows,
        emptyText: "По зв’язку цілей зі статусами даних поки немає.",
      },
      {
        title: "Цілі × БК",
        summary: `Який БК або боєприпас найчастіше використовується по кожній цілі`,
        rows: targetAmmoRows,
        emptyText: "По зв’язку цілей з БК даних поки немає.",
      },
      {
        title: "Статус цілей · по місіях",
        summary: `Унікальних статусів: ${fmtNum(targetStatusRows.length)}`,
        rows: targetStatusRows,
        emptyText: "По статусу цілей даних поки немає.",
      },
      {
        title: "Місії з цілями",
        summary: `${fmtNum(analytics.targetMissionCount)} місій, де вказані цілі`,
        rows: analytics.missions
          .filter(item=>item.targetTypes?.length || item.targetStatuses?.length || (Number(item.targetCount) || 0) > 0)
          .map(item=>({
            label: item.unit || "Без підрозділу",
            valueText: item.resultAt || item.endAt || item.startAt || "Без дати",
            meta: [
              item.taskType ? `Тип задачі: ${item.taskType}` : "",
              item.asset ? `Платформа: ${item.asset}` : "",
              item.targetTypes?.length ? `Ціль: ${item.targetTypes.join(", ")}` : "",
              item.targetStatuses?.length ? `Статус: ${item.targetStatuses.join(", ")}` : "",
              Number(item.targetCount) ? `Кількість цілей: ${fmtNum(item.targetCount)}` : "",
              item.ammoTypes?.length ? `БК: ${item.ammoTypes.join(", ")}` : "",
              item.ammoNames?.length ? `Боєприпас: ${item.ammoNames.join(", ")}` : "",
              item.targetDescription ? `Опис: ${item.targetDescription}` : "",
            ].filter(Boolean).join(" · "),
            tone: "b-blue",
          })),
        emptyText: "Місій із цілями поки немає.",
      }
    ])
  );
  const targetStatusModalKey = detailModal(`${analytics.title || "Delta / БпЛА"} · Статус цілей`, "Статус цілей · по місіях", `Унікальних статусів: ${fmtNum(targetStatusRows.length)}`, targetStatusRows, "По статусу цілей даних поки немає.");
  const ammoModalKey = detailModal(`${analytics.title || "Delta / БпЛА"} · Боєприпаси`, "Типи БК · по місіях", `Унікальних типів БК: ${fmtNum(ammoRows.length)} · усього БК: ${fmtNum(analytics.totalAmmoQty)}`, ammoRows, "По боєприпасах даних поки немає.");
  const deliveryModalKey = detailModal(`${analytics.title || "Delta / БпЛА"} · Статус вантажу`, "Статус вантажу · по місіях", `Логістичних місій: ${fmtNum(analytics.deliveryMissionCount)} · доставлено: ${fmtNum(analytics.deliveredCargoCount)}`, deliveryRows, "По статусу вантажу даних поки немає.");
  const unitsModalKey = detailModal(`${analytics.title || "Delta / БпЛА"} · Підрозділи`, "Підрозділи · по місіях", `Усього підрозділів: ${fmtNum(unitsRows.length)}`, unitsRows, "По підрозділах даних поки немає.");
  const controlModalKey = detailModal(`${analytics.title || "Delta / БпЛА"} · Керування`, "Тип керування · по місіях", `Унікальних типів: ${fmtNum(controlRows.length)}`, controlRows, "По типу керування даних поки немає.");

  const platformsBlock = buildDeltaNrkTopList("Платформи · по місіях", platformsRows.slice(0, 8), "По платформах даних поки немає.").replace('<div class="row"><div class="name">Платформи · по місіях</div></div>', `<div class="row"><div class="name">Платформи · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${platformsModalKey}">Детальніше</button></div>`);
  const unitTaskBlock = buildDeltaNrkTopList("Розвідка / Ураження · по підрозділах", unitTaskRows.slice(0, 8), "По підрозділах даних поки немає.").replace('<div class="row"><div class="name">Розвідка / Ураження · по підрозділах</div></div>', `<div class="row"><div class="name">Розвідка / Ураження · по підрозділах</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${unitTaskModalKey}">Детальніше</button></div>`);
  const assetLossBlock = buildDeltaNrkTopList("Втрати · причини та умови", lossCauseRows.slice(0, 8), "Втрат по причинах поки немає.").replace('<div class="row"><div class="name">Втрати · причини та умови</div></div>', `<div class="row"><div class="name">Втрати · причини та умови</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${assetLossModalKey}">Детальніше</button></div>`);
  const taskTypesBlock = buildDeltaNrkTopList("Типи задач · по місіях", taskTypeRows.slice(0, 8), "По типах задач даних поки немає.").replace('<div class="row"><div class="name">Типи задач · по місіях</div></div>', `<div class="row"><div class="name">Типи задач · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${taskTypesModalKey}">Детальніше</button></div>`);
  const targetTypesBlock = buildDeltaNrkTopList("Цілі · повна аналітика", targetOverviewRows.slice(0, 8), "По цілях даних поки немає.").replace('<div class="row"><div class="name">Цілі · повна аналітика</div></div>', `<div class="row"><div class="name">Цілі · повна аналітика</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${targetTypesModalKey}">Детальніше</button></div>`);
  const targetStatusBlock = buildDeltaNrkTopList("Статус цілей · по місіях", targetStatusRows.slice(0, 8), "По статусу цілей даних поки немає.").replace('<div class="row"><div class="name">Статус цілей · по місіях</div></div>', `<div class="row"><div class="name">Статус цілей · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${targetStatusModalKey}">Детальніше</button></div>`);
  const ammoBlock = buildDeltaNrkTopList("Боєприпаси · по місіях", ammoRows.slice(0, 8), "По боєприпасах даних поки немає.").replace('<div class="row"><div class="name">Боєприпаси · по місіях</div></div>', `<div class="row"><div class="name">Боєприпаси · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${ammoModalKey}">Детальніше</button></div>`);
  const deliveryBlock = deliveryRows.length ? buildDeltaNrkTopList("Статус вантажу · по місіях", deliveryRows.slice(0, 8), "По статусу вантажу даних поки немає.").replace('<div class="row"><div class="name">Статус вантажу · по місіях</div></div>', `<div class="row"><div class="name">Статус вантажу · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${deliveryModalKey}">Детальніше</button></div>`) : "";
  const unitsBlock = buildDeltaNrkTopList("Підрозділи · по місіях", unitsRows.slice(0, 8), "По підрозділах даних поки немає.").replace('<div class="row"><div class="name">Підрозділи · по місіях</div></div>', `<div class="row"><div class="name">Підрозділи · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${unitsModalKey}">Детальніше</button></div>`);
  const controlBlock = buildDeltaNrkTopList("Керування · по місіях", controlRows.slice(0, 8), "По типу керування даних поки немає.").replace('<div class="row"><div class="name">Керування · по місіях</div></div>', `<div class="row"><div class="name">Керування · по місіях</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${controlModalKey}">Детальніше</button></div>`);
  const reliabilityBlock = `<div class="item analytics-block delta-nrk-list"><div class="row"><div class="name">Надійність · по місіях</div></div><div class="comparison-compact-grid">${reliabilityRows.length ? reliabilityRows.map((item, index)=>`<div class="comparison-compact-card delta-nrk-card delta-nrk-reliability-card"><div class="comparison-compact-rank mono">${index + 1}</div><div class="comparison-compact-main"><div class="comparison-compact-title">${htmlesc(item.label)}</div><div class="comparison-compact-meta">${htmlesc(item.meta || "")}</div></div><div class="delta-nrk-reliability-actions"><div class="badge ${item.tone || "b-blue"} mono">${htmlesc(String(item.valueText || ""))}</div><button type="button" class="btn ghost btn-mini" data-action="openRenderedTableModal" data-arg1="${item.modalKey}">Місії</button></div></div>`).join("") : `<div class="hint">Даних по статусу засобу поки немає.</div>`}</div></div>`;

  return `
    <div class="staffing-analytics-modal comparison-analytics-modal delta-nrk-analytics-modal">
      ${filtersBlock}
      ${buildDeltaBplaAutoSummaryHtml(analytics)}
      ${buildDeltaBplaMonthlyAnalyticsHtml(analytics)}
      <div class="control-grid">
        ${wrapDeltaBplaCollapsible("Платформи", platformsBlock)}
        ${wrapDeltaBplaCollapsible("Розвідка / Ураження по підрозділах", unitTaskBlock)}
      </div>
      <div class="control-grid">
        ${wrapDeltaBplaCollapsible("Типи задач", taskTypesBlock)}
        ${wrapDeltaBplaCollapsible("Підрозділи", unitsBlock)}
      </div>
      <div class="control-grid">
        ${wrapDeltaBplaCollapsible("Цілі", targetTypesBlock)}
        ${wrapDeltaBplaCollapsible("Статус цілей", targetStatusBlock)}
      </div>
      <div class="control-grid">
        ${wrapDeltaBplaCollapsible("Втрати", assetLossBlock)}
        ${wrapDeltaBplaCollapsible("Надійність", reliabilityBlock)}
      </div>
      <div class="control-grid">
        ${wrapDeltaBplaCollapsible("Боєприпаси", ammoBlock)}
        ${wrapDeltaBplaCollapsible("Статус вантажу", deliveryBlock || `<div class="item analytics-block"><div class="hint">По статусу вантажу даних поки немає.</div></div>`)}
      </div>
      ${wrapDeltaBplaCollapsible("День / ніч", buildDeltaNrkDayNightHtml(analytics))}
      ${wrapDeltaBplaCollapsible("Керування", controlBlock)}
      ${buildDeltaBplaExecutiveReportHtml(analytics, modalKey)}
      ${wrapDeltaBplaCollapsible("Якість заповнення даних", buildDeltaNrkTimeQualityHtml(analytics))}
      ${wrapDeltaBplaCollapsible("Перевірка імпорту", diagnosticsBlock)}
      ${wrapDeltaBplaCollapsible("Логіка підрахунку", countingLogicBlock)}
    </div>
  `;

}

function sanitizeExportFilename(name){

  return String(name || "analytics")
    .trim()
    .replace(/[\\/:*?"<>|]+/g, " ")
    .replace(/\s+/g, " ")
    .trim() || "analytics";

}

function copyComputedStylesDeep(source, target){

  if(!(source instanceof Element) || !(target instanceof Element)) return;

  const computed = getComputedStyle(source);
  for(let i = 0; i < computed.length; i += 1){
    const prop = computed[i];
    try{
      target.style.setProperty(prop, computed.getPropertyValue(prop), computed.getPropertyPriority(prop));
    }catch(_){}
  }

  const sourceChildren = Array.from(source.children || []);
  const targetChildren = Array.from(target.children || []);
  for(let i = 0; i < sourceChildren.length; i += 1){
    if(targetChildren[i]) copyComputedStylesDeep(sourceChildren[i], targetChildren[i]);
  }

}

async function exportCurrentRenderedModalPng(){

  const bodySource = document.querySelector(".table-modal-body");
  if(!bodySource){
    showToast("Немає відкритої аналітики для експорту.", "warn");
    return;
  }

  try{
    const host = document.createElement("div");
    host.style.position = "fixed";
    host.style.left = "-100000px";
    host.style.top = "0";
    host.style.pointerEvents = "none";
    host.style.opacity = "0";
    host.style.zIndex = "-1";
    host.style.background = "#f4f7fc";
    host.style.padding = "0";

    const frame = document.createElement("div");
    frame.style.width = `${Math.ceil((sheetEl?.getBoundingClientRect()?.width || bodySource.getBoundingClientRect().width || 960))}px`;
    frame.style.maxWidth = "none";
    frame.style.background = getComputedStyle(sheetEl || document.body).backgroundColor || "#ffffff";
    frame.style.borderRadius = "24px";
    frame.style.boxShadow = "0 18px 50px rgba(24,39,75,.14)";
    frame.style.overflow = "visible";
    frame.style.padding = "0";

    const titleWrap = document.createElement("div");
    titleWrap.style.padding = "20px 22px 14px";
    titleWrap.style.borderBottom = "1px solid rgba(150,170,205,.18)";
    titleWrap.style.background = getComputedStyle(modal || document.body).backgroundColor || "#ffffff";

    const titleNode = document.createElement("div");
    titleNode.textContent = String(sheetTitle?.textContent || "Аналітика");
    titleNode.style.fontSize = "18px";
    titleNode.style.fontWeight = "900";
    titleNode.style.lineHeight = "1.25";
    titleNode.style.color = getComputedStyle(sheetTitle || document.body).color || "#1f2d4a";

    const bodyClone = bodySource.cloneNode(true);
    copyComputedStylesDeep(bodySource, bodyClone);
    bodyClone.style.maxHeight = "none";
    bodyClone.style.height = "auto";
    bodyClone.style.overflow = "visible";
    bodyClone.style.padding = bodyClone.style.padding || "18px 20px 22px";

    titleWrap.appendChild(titleNode);
    frame.appendChild(titleWrap);
    frame.appendChild(bodyClone);
    host.appendChild(frame);
    document.body.appendChild(host);

    await new Promise(resolve=>requestAnimationFrame(resolve));

    const width = Math.ceil(frame.scrollWidth);
    const height = Math.ceil(frame.scrollHeight);
    const serialized = new XMLSerializer().serializeToString(frame);
    const svg = `
      <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
        <foreignObject width="100%" height="100%">
          <div xmlns="http://www.w3.org/1999/xhtml">${serialized}</div>
        </foreignObject>
      </svg>
    `;
    const blob = new Blob([svg], {type:"image/svg+xml;charset=utf-8"});
    const url = URL.createObjectURL(blob);
    const img = new Image();

    await new Promise((resolve, reject)=>{
      img.onload = ()=>resolve();
      img.onerror = reject;
      img.src = url;
    });

    const scale = Math.min(window.devicePixelRatio || 1, 2);
    const canvas = document.createElement("canvas");
    canvas.width = Math.ceil(width * scale);
    canvas.height = Math.ceil(height * scale);
    const ctx = canvas.getContext("2d");
    ctx.scale(scale, scale);
    ctx.drawImage(img, 0, 0, width, height);
    URL.revokeObjectURL(url);
    host.remove();

    const link = document.createElement("a");
    link.href = canvas.toDataURL("image/png");
    link.download = `${sanitizeExportFilename(sheetTitle?.textContent || "analytics")}.png`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    showToast("PNG збережено.", "ok");
  } catch(err){
    console.error(err);
    showToast("PNG не вдався. Спробуй PDF / Друк.", "warn");
  }

}

function printCurrentRenderedModal(){

  const bodySource = document.querySelector(".table-modal-body");
  if(!bodySource){
    showToast("Немає відкритої аналітики для друку.", "warn");
    return;
  }

  const printWindow = window.open("", "_blank", "noopener,noreferrer,width=1200,height=900");
  if(!printWindow){
    showToast("Браузер заблокував вікно друку.", "warn");
    return;
  }

  const title = String(sheetTitle?.textContent || "Аналітика");
  const shell = document.createElement("div");
  shell.className = "print-shell";
  shell.style.maxWidth = "1200px";
  shell.style.margin = "0 auto";
  shell.style.background = "#ffffff";
  shell.style.borderRadius = "24px";
  shell.style.boxShadow = "0 18px 50px rgba(24,39,75,.14)";
  shell.style.overflow = "hidden";

  const head = document.createElement("div");
  head.className = "print-head";
  head.style.padding = "20px 22px 14px";
  head.style.borderBottom = "1px solid rgba(150,170,205,.18)";
  head.style.background = "#ffffff";

  const titleNode = document.createElement("div");
  titleNode.className = "print-title";
  titleNode.textContent = title;
  titleNode.style.fontSize = "20px";
  titleNode.style.fontWeight = "900";
  titleNode.style.lineHeight = "1.25";
  titleNode.style.color = "#1f2d4a";
  head.appendChild(titleNode);

  const clone = bodySource.cloneNode(true);
  copyComputedStylesDeep(bodySource, clone);
  clone.style.maxHeight = "none";
  clone.style.height = "auto";
  clone.style.overflow = "visible";
  clone.style.padding = clone.style.padding || "18px 20px 22px";
  clone.querySelectorAll("*").forEach(node=>{
    if(node instanceof HTMLElement){
      node.style.maxHeight = node.style.maxHeight === "none" ? "none" : node.style.maxHeight;
      if(node.classList.contains("comparison-switch-panel") && !node.classList.contains("is-active")){
        node.style.display = "none";
      }
    }
  });

  shell.appendChild(head);
  shell.appendChild(clone);
  const html = `
    <!doctype html>
    <html lang="uk">
      <head>
        <meta charset="utf-8" />
        <title>${htmlesc(title)}</title>
        <style>
          body{
            margin:0;
            padding:24px;
            background:#f4f7fc;
            color:#1f2d4a;
            font-family: "Segoe UI", Arial, sans-serif;
          }
          @media print{
            body{
              padding:0;
              background:#fff;
            }
            .print-shell{
              max-width:none;
              box-shadow:none;
              border-radius:0;
            }
          }
        </style>
      </head>
      <body>
        ${shell.outerHTML}
      </body>
    </html>
  `;

  printWindow.document.open();
  printWindow.document.write(html);
  printWindow.document.close();
  printWindow.focus();
  printWindow.onload = ()=>{
    setTimeout(()=>{
      try{
        printWindow.print();
      }catch(err){
        console.error(err);
      }
    }, 250);
  };

}

function buildStandaloneRenderedModalHtml(){

  const bodySource = document.querySelector(".table-modal-body");
  if(!bodySource) return "";

  const title = String(sheetTitle?.textContent || "Аналітика");
  const shell = document.createElement("div");
  shell.className = "print-shell";
  shell.style.maxWidth = "none";
  shell.style.width = "max-content";
  shell.style.minWidth = "100%";
  shell.style.margin = "0";
  shell.style.background = "#ffffff";
  shell.style.borderRadius = "24px";
  shell.style.boxShadow = "0 18px 50px rgba(24,39,75,.14)";
  shell.style.overflow = "visible";

  const head = document.createElement("div");
  head.className = "print-head";
  head.style.padding = "20px 22px 14px";
  head.style.borderBottom = "1px solid rgba(150,170,205,.18)";
  head.style.background = "#ffffff";

  const titleNode = document.createElement("div");
  titleNode.className = "print-title";
  titleNode.textContent = title;
  titleNode.style.fontSize = "20px";
  titleNode.style.fontWeight = "900";
  titleNode.style.lineHeight = "1.25";
  titleNode.style.color = "#1f2d4a";
  head.appendChild(titleNode);

  const clone = bodySource.cloneNode(true);
  copyComputedStylesDeep(bodySource, clone);
  clone.style.maxHeight = "none";
  clone.style.height = "auto";
  clone.style.overflow = "visible";
  clone.style.padding = clone.style.padding || "18px 20px 22px";
  clone.querySelectorAll("*").forEach(node=>{
    if(node instanceof HTMLElement){
      if(node.classList.contains("comparison-switch-panel") && !node.classList.contains("is-active")){
        node.style.display = "none";
      }
    }
  });

  shell.appendChild(head);
  shell.appendChild(clone);

  return `
    <!doctype html>
    <html lang="uk">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>${htmlesc(title)}</title>
        <style>
          body{
            margin:0;
            padding:18px;
            background:#f4f7fc;
            color:#1f2d4a;
            font-family:"Segoe UI", Arial, sans-serif;
          }
          .standalone-wrap{
            width:100%;
            overflow:auto;
            padding-bottom:12px;
          }
          .print-shell{
            max-width:none !important;
            width:max-content !important;
            min-width:100%;
            overflow:visible !important;
          }
          .print-shell .table-modal-body{
            max-height:none !important;
            height:auto !important;
            overflow:visible !important;
          }
          @media print{
            body{
              padding:0;
              background:#fff;
            }
            .standalone-wrap{
              overflow:visible;
            }
            .print-shell{
              max-width:none !important;
              width:auto !important;
              min-width:0 !important;
              box-shadow:none !important;
              border-radius:0 !important;
            }
          }
        </style>
      </head>
      <body>
        <div class="standalone-wrap">${shell.outerHTML}</div>
      </body>
    </html>
  `;

}

function openCurrentRenderedModalStandalone(){

  const html = buildStandaloneRenderedModalHtml();
  if(!html){
    showToast("Немає відкритої аналітики.", "warn");
    return;
  }

  try{
    const blob = new Blob([html], {type:"text/html;charset=utf-8"});
    const url = URL.createObjectURL(blob);
    const win = window.open(url, "_blank", "noopener,noreferrer");
    if(!win){
      showToast("Браузер заблокував нову вкладку.", "warn");
      URL.revokeObjectURL(url);
      return;
    }
    setTimeout(()=>URL.revokeObjectURL(url), 60000);
  }catch(err){
    console.error(err);
    showToast("Не вдалося відкрити повний звіт.", "warn");
  }

}

function rerenderDeltaNrkAnalyticsModal(key, filters={}){

  const item = UI.renderedTableModals?.[key];
  if(!item?.deltaRows?.length) return;

  const nextFilters = {
    unit: String(filters.unit || "").trim(),
    taskType: String(filters.taskType || "").trim(),
    asset: String(filters.asset || "").trim(),
  };

  item.deltaFilters = nextFilters;
  const analyticsKind = String(item.deltaAnalyticsKind || "delta_nrk");
  item.bodyHtml = analyticsKind === "delta_bpla"
    ? buildDeltaBplaAnalyticsModalHtml(
        item.deltaRows,
        item.deltaTitle || item.title || "Delta / БпЛА",
        {modalKey: key, filters: nextFilters}
      )
    : buildDeltaNrkAnalyticsModalHtml(
        item.deltaRows,
        item.deltaTitle || item.title || "Delta / НРК",
        {modalKey: key, filters: nextFilters}
      );

  const body = document.querySelector(`.table-modal-body[data-rendered-modal-key="${key}"]`);
  if(body){
    body.innerHTML = item.bodyHtml;
    requestAnimationFrame(()=>{
      animateRenderedDonuts(document.querySelector(".sheet"));
    });
  }

}

function applyDeltaNrkAnalyticsFilters(key){

  if(!key) return;

  const unit = String(document.getElementById(`deltaUnitFilter_${key}`)?.value || "").trim();
  const taskType = String(document.getElementById(`deltaTaskTypeFilter_${key}`)?.value || "").trim();
  const asset = String(document.getElementById(`deltaAssetFilter_${key}`)?.value || "").trim();
  rerenderDeltaNrkAnalyticsModal(key, {unit, taskType, asset});

}

function resetDeltaNrkAnalyticsFilters(key){

  if(!key) return;
  rerenderDeltaNrkAnalyticsModal(key, {unit:"", taskType:"", asset:""});

}

async function copyTextFromElement(id){

  const el = document.getElementById(String(id || ""));
  if(!el){
    showToast("Не знайдено текст для копіювання.", "warn");
    return;
  }
  const text = "value" in el ? String(el.value || "") : String(el.textContent || "");
  if(!text.trim()){
    showToast("Немає тексту для копіювання.", "warn");
    return;
  }

  try{
    if(navigator?.clipboard?.writeText){
      await navigator.clipboard.writeText(text);
    } else {
      if(typeof el.select === "function"){
        el.focus();
        el.select();
      }
      document.execCommand("copy");
    }
    showToast("Текст скопійовано.", "ok");
  } catch(err){
    console.warn("copy text failed", err);
    try{
      if(typeof el.select === "function"){
        el.focus();
        el.select();
        document.execCommand("copy");
        showToast("Текст скопійовано.", "ok");
        return;
      }
    } catch(_err){
      console.warn("copy text fallback failed", _err);
    }
    showToast("Не вдалося скопіювати текст.", "warn");
  }

}

function switchComparisonTopPanel(groupId, key){

  if(!groupId || !key) return;

  const scope = [...document.querySelectorAll("[data-topswitch-group]")].find(el=>el.getAttribute("data-topswitch-group") === groupId);
  if(!scope) return;

  scope.querySelectorAll(".comparison-switcher-btn").forEach(btn=>{
    btn.classList.toggle("is-active", btn.dataset.arg2 === key);
  });

  scope.querySelectorAll("[data-topswitch-panel]").forEach(panel=>{
    panel.classList.toggle("is-active", panel.getAttribute("data-topswitch-panel") === `${groupId}:${key}`);
  });

}

function filterStaffingUnitsBlock(groupId){

  if(!groupId) return;

  const switcher = [...document.querySelectorAll("[data-topswitch-group]")].find(el=>el.getAttribute("data-topswitch-group") === groupId);
  if(!switcher) return;

  const input = switcher.parentElement?.querySelector(`.staffing-units-search-input[data-arg1="${groupId}"]`);
  const query = String(input?.value || "").trim().toLowerCase();
  const scopeMode = String(switcher.getAttribute("data-staffing-scope") || "plan");

  const panels = switcher.querySelectorAll("[data-staffing-filter-group]");
  panels.forEach(panel=>{
    const wrap = panel.querySelector(".staffing-unit-list-wrap");
    const items = [...panel.querySelectorAll(".staffing-unit-list li")];
    let visibleCount = 0;

    items.forEach(item=>{
      const name = String(item.querySelector(".report-strong")?.textContent || "").trim().toLowerCase();
      const planValue = Number(item.querySelector(".staffing-unit-card")?.getAttribute("data-staffing-plan-value") || 0);
      const matchesSearch = !query || name.includes(query);
      const matchesScope = scopeMode === "all" ? true : planValue > 0;
      const show = matchesSearch && matchesScope;
      item.style.display = show ? "" : "none";
      if(show) visibleCount += 1;
    });

    if(wrap){
      wrap.classList.toggle("is-filtering", !!query);
      const toggleBtn = wrap.querySelector(".staffing-unit-toggle");
      if(toggleBtn){
        toggleBtn.textContent = wrap.classList.contains("is-expanded")
          ? "Згорнути"
          : `Показати всі (${visibleCount || items.length})`;
      }
    }

    let hint = panel.querySelector(".staffing-units-empty");
    if(!visibleCount){
      if(!hint){
        hint = document.createElement("div");
        hint.className = "hint staffing-units-empty";
        panel.appendChild(hint);
      }
      hint.textContent = "Нічого не знайдено за цим запитом.";
    } else if(hint){
      hint.remove();
    }
  });

}

function setStaffingUnitsScope(groupId, scope="plan"){

  if(!groupId) return;

  const switcher = [...document.querySelectorAll("[data-topswitch-group]")].find(el=>el.getAttribute("data-topswitch-group") === groupId);
  if(!switcher) return;

  const nextScope = scope === "all" ? "all" : "plan";
  switcher.setAttribute("data-staffing-scope", nextScope);

  const scopeWrap = switcher.parentElement?.querySelector(`[data-staffing-scope-group="${groupId}"]`);
  if(scopeWrap){
    scopeWrap.querySelectorAll(".comparison-switcher-btn").forEach(btn=>{
      btn.classList.toggle("is-active", btn.dataset.arg2 === nextScope);
    });
  }

  filterStaffingUnitsBlock(groupId);

}

function setStaffingUnitsViewMode(groupId, mode="cards"){

  if(!groupId) return;

  const switcher = [...document.querySelectorAll("[data-topswitch-group]")].find(el=>el.getAttribute("data-topswitch-group") === groupId);
  if(!switcher) return;

  const nextMode = mode === "progress" ? "progress" : "cards";
  switcher.setAttribute("data-staffing-view-mode", nextMode);

  const modeWrap = switcher.parentElement?.querySelector(`[data-staffing-view-group="${groupId}"]`);
  if(modeWrap){
    modeWrap.querySelectorAll(".comparison-switcher-btn").forEach(btn=>{
      btn.classList.toggle("is-active", btn.dataset.arg2 === nextMode);
    });
  }

}

function toggleStaffingUnitsExpand(panelId){

  if(!panelId) return;

  const wrap = document.querySelector(`.staffing-unit-list-wrap[data-staffing-list="${panelId}"]`);
  if(!wrap) return;

  wrap.classList.toggle("is-expanded");

  const btn = wrap.querySelector(".staffing-unit-toggle");
  if(btn){
    const total = wrap.querySelectorAll(".staffing-unit-list li").length;
    btn.textContent = wrap.classList.contains("is-expanded")
      ? "Згорнути"
      : `Показати всі (${total})`;
  }

}

function animateRenderedDonuts(scope=document){

  const donuts = [...(scope || document).querySelectorAll('.is-animated-donut[data-donut-gradient]')];
  if(!donuts.length) return;

  const duration = 820;
  const easeOutCubic = (t)=>1 - Math.pow(1 - t, 3);

  donuts.forEach((el, idx)=>{
    const gradient = el.getAttribute('data-donut-gradient') || 'conic-gradient(#dfe6f6 0 360deg)';
    const delay = idx * 80;

    el.classList.remove('donut-ready');
    el.style.background = gradient;
    el.style.setProperty('--donut-progress', '0turn');

    const start = performance.now() + delay;

    const tick = (now)=>{
      if(now < start){
        requestAnimationFrame(tick);
        return;
      }

      const progress = Math.min(1, (now - start) / duration);
      const eased = easeOutCubic(progress);
      el.style.setProperty('--donut-progress', `${eased}turn`);

      if(progress < 1){
        requestAnimationFrame(tick);
      } else {
        el.classList.add('donut-ready');
      }
    };

    requestAnimationFrame(tick);
  });

}

async function pasteTextTableFromClipboard(textareaId){

  if(!(navigator?.clipboard?.readText)){

    showToast("Буфер обміну недоступний у цьому браузері.", "warn");

    return;

  }

  try{

    const text = await navigator.clipboard.readText();

    const rows = parseClipboardTableText(text);

    if(!rows.length){

      showToast("У буфері немає табличних даних.", "warn");

      return;

    }

    writeTextTableToTextarea(textareaId, rows);

    if(textareaId === "referenceEntryText") {
      const tableType = normalizeReferenceTableType(document.getElementById("referenceEntryTableType")?.value || "none");
      renderReferenceEntryTableWorkspace(textareaId, tableType, rows);
    } else {
      renderTextTableEditor(textareaId, rows);
    }

    showToast("Таблицю вставлено з буфера", "ok");

  } catch(err){

    console.warn("clipboard read failed", err);

    showToast("Не вдалося прочитати буфер. Скопіюй таблицю ще раз і повтори.", "warn");

  }

}

function canUseWorkbookImport(){

  return typeof FileReader !== "undefined" && typeof XLSX !== "undefined" && typeof XLSX.read === "function" && XLSX.utils;

}

function getCurrentTextareaTableRows(textareaId){

  const el = document.getElementById(textareaId);

  if(!el) return [];

  if(document.querySelector(`.text-table-editor[data-for="${textareaId}"]`)){
    return readTextTableEditorRows(textareaId);
  }

  const block = findStoredTableBlock(el.dataset.tableRaw || "");

  return cloneStoredTableRows(block?.rows || []);

}

function shouldRenderCompactTableSummary(tableType, rows){

  const safeRows = Array.isArray(rows) ? rows : [];
  const rowCount = safeRows.length;
  const colCount = rowCount ? Math.max(0, ...safeRows.map(row=>Array.isArray(row) ? row.length : 0)) : 0;

  if(!rowCount || !colCount) return false;

  if(tableType === "delta_nrk" || tableType === "delta_bpla") return rowCount > 40 || (rowCount * colCount) > 480;

  return false;

}

function buildTextTableImportSummaryHtml(textareaId, rows, opts={}){

  const safeRows = Array.isArray(rows) ? rows : [];
  const header = safeRows[0] || [];
  const colCount = header.length || Math.max(0, ...safeRows.map(row=>Array.isArray(row) ? row.length : 0));
  const rowCount = Math.max(0, safeRows.length - 1);
  const headerLabels = header.map(x=>String(x || "").trim()).filter(Boolean);
  const visibleHeaders = headerLabels.slice(0, 6);
  const moreHeaders = Math.max(0, headerLabels.length - visibleHeaders.length);
  const typeLabel = getReferenceTableTypeLabel(opts.tableType || "none");

  return `
    <div class="text-table-import-summary" data-for="${textareaId}">
      <div class="text-table-import-summary-head">
        <div>
          <div class="text-table-import-summary-title">Таблицю імпортовано</div>
          <div class="hint">${htmlesc(typeLabel)} · ${rowCount} рядків · ${colCount} колонок</div>
        </div>
        <div class="text-table-import-summary-badges">
          <span class="ref-table-type-pill">Імпорт .xlsx</span>
        </div>
      </div>
      <div class="hint">Для великого звіту редагування по клітинках приховано. Якщо треба оновити дані — імпортуй файл ще раз.</div>
      ${visibleHeaders.length ? `
        <div class="text-table-import-summary-columns">
          ${visibleHeaders.map(item=>`<span class="text-table-import-col">${htmlesc(item)}</span>`).join("")}
          ${moreHeaders ? `<span class="text-table-import-col more">+${moreHeaders}</span>` : ""}
        </div>
      ` : ""}
      <div class="actions" style="margin-top:10px;">
        <button type="button" class="btn danger btn-mini" data-action="deleteTextTableFromTextarea" data-arg1="${textareaId}">Видалити таблицю</button>
      </div>
    </div>
  `;

}

function renderReferenceEntryTableWorkspace(textareaId, tableType, rows){

  const el = document.getElementById(textareaId);

  if(!el) return;

  closeTextTableEditor(textareaId);

  const safeRows = Array.isArray(rows) ? rows : [];

  if(!safeRows.length) return;

  if(shouldRenderCompactTableSummary(tableType, safeRows)){
    el.insertAdjacentHTML("afterend", buildTextTableImportSummaryHtml(textareaId, safeRows, {tableType}));
    return;
  }

  renderTextTableEditor(textareaId, safeRows);

}

function normalizeImportedWorksheetRows(rows){

  const safeRows = Array.isArray(rows) ? rows : [];
  const normalized = safeRows.map(row=>(Array.isArray(row) ? row : []).map(cell=>String(cell ?? "").replace(/\r?\n+/g, " ").trim()));
  const nonEmptyRows = normalized.filter(row=>row.some(cell=>String(cell || "").trim()));

  if(!nonEmptyRows.length) return [];

  let maxCols = 0;

  nonEmptyRows.forEach(row=>{
    for(let i = row.length - 1; i >= 0; i--){
      if(String(row[i] || "").trim()){
        maxCols = Math.max(maxCols, i + 1);
        break;
      }
    }
  });

  return nonEmptyRows.map(row=>{
    const next = row.slice(0, maxCols);
    while(next.length < maxCols) next.push("");
    return next;
  });

}

function pickWorkbookSheetName(workbook, preferredName=""){

  const names = Array.isArray(workbook?.SheetNames) ? workbook.SheetNames : [];
  if(!names.length) return "";

  const normalize = value=>String(value || "").trim().toLowerCase().replace(/\s+/g, " ");
  const preferred = normalize(preferredName);

  if(preferred){
    const exact = names.find(name=>normalize(name) === preferred);
    if(exact) return exact;
    const partial = names.find(name=>normalize(name).includes(preferred));
    if(partial) return partial;
  }

  const firstWithData = names.find(name=>workbook.Sheets?.[name]?.["!ref"]);

  return firstWithData || names[0] || "";

}

function getReferenceWorkbookImportConfig(tableType){

  const type = normalizeReferenceTableType(tableType);

  if(type === "delta_bpla"){
    return {
      enabled: true,
      type,
      title: "Delta / БпЛА",
      preferredSheet: "БпЛА",
      hint: "Для Delta / БпЛА можна завантажити весь Excel-файл напряму — без копіювання шматків таблиці."
    };
  }

  if(type === "delta_nrk"){
    return {
      enabled: true,
      type,
      title: "Delta / НРК",
      preferredSheet: "НРК",
      hint: "Для Delta / НРК можна завантажити весь Excel-файл напряму — без копіювання шматків таблиці."
    };
  }

  if(type === "compare"){
    return {
      enabled: true,
      type,
      title: "Порівняння",
      preferredSheet: document.getElementById("referenceEntryTitle")?.value || "",
      hint: "Для порівняльних таблиць можна обрати потрібну вкладку з Excel-файлу — це надійніше, ніж вставка через буфер."
    };
  }

  return {enabled:false, type, title:"", preferredSheet:"", hint:""};

}

function getWorkbookSheetPreviewRows(workbook, sheetName){

  const sheet = sheetName ? workbook?.Sheets?.[sheetName] : null;

  if(!sheet) return [];

  return normalizeImportedWorksheetRows(
    XLSX.utils.sheet_to_json(sheet, {header:1, defval:"", raw:false, blankrows:false})
  );

}

function buildReferenceWorkbookSheetPickerHtml(fileName, sheets, preferredSheet=""){

  const normalize = value=>String(value || "").trim().toLowerCase().replace(/\s+/g, " ");
  const preferred = normalize(preferredSheet);

  return `
    <div class="field">
      <div class="hint">Файл: <span class="mono">${htmlesc(fileName || "Excel")}</span></div>
      <div class="hint" style="margin-top:4px;">Оберемо одну вкладку і імпортуємо її в поточний запис.</div>
    </div>
    <div class="comparison-compact-grid">
      ${sheets.map(item=>{
        const isPreferred = preferred && normalize(item.name) === preferred;
        return `
          <button
            type="button"
            class="comparison-compact-card comparison-card-btn"
            data-action="openReferenceWorkbookSheetPreview"
            data-arg1="${attrEsc(item.name)}"
          >
            <div class="comparison-compact-main">
              <div class="comparison-compact-title">${htmlesc(item.name)}${isPreferred ? ` <span class="pill">Рекомендовано</span>` : ``}</div>
              <div class="comparison-compact-meta">${item.rowCount} рядків · ${item.colCount} колонок</div>
            </div>
          </button>
        `;
      }).join("")}
    </div>
    <div class="actions" style="margin-top:12px;">
      <button type="button" class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>
  `;

}

function buildReferenceWorkbookSheetPreviewHtml(fileName, sheetName, rows, typeLabel="Таблиця"){

  const safeRows = Array.isArray(rows) ? rows : [];
  const header = safeRows[0] || [];
  const body = safeRows.slice(1, 6);
  const rowCount = Math.max(0, safeRows.length - 1);
  const colCount = header.length || Math.max(0, ...safeRows.map(row=>Array.isArray(row) ? row.length : 0));
  const previewRows = [header, ...body];
  const currentRows = getCurrentTextareaTableRows("referenceEntryText");
  const hasExistingTable = currentRows.length > 1;
  const currentRowCount = hasExistingTable ? Math.max(0, currentRows.length - 1) : 0;
  const rowDiff = hasExistingTable ? (rowCount - currentRowCount) : 0;
  const rowDiffLabel = !hasExistingTable
    ? ""
    : rowDiff === 0
      ? "Стільки ж рядків, як і зараз."
      : rowDiff > 0
        ? `На ${fmtNum(rowDiff)} рядк. більше, ніж у поточній таблиці.`
        : `На ${fmtNum(Math.abs(rowDiff))} рядк. менше, ніж у поточній таблиці.`;

  const previewTable = previewRows.length
    ? renderMarkdownTableBlock(
        previewRows.map((row, idx)=>{
          const line = `| ${row.map(cell=>String(cell ?? "").replace(/\|/g, "/")).join(" | ")} |`;
          if(idx === 0){
            return [line, `| ${row.map(()=> "---").join(" | ")} |`];
          }
          return line;
        }).flat()
      )
    : `<div class="hint">Немає даних для попереднього перегляду.</div>`;

  return `
    <div class="field">
      <div class="text-table-import-summary-head">
        <div>
          <div class="text-table-import-summary-title">${htmlesc(sheetName)}</div>
          <div class="hint">${htmlesc(typeLabel)} · ${rowCount} рядків · ${colCount} колонок</div>
        </div>
        <div class="text-table-import-summary-badges">
          <span class="ref-table-type-pill">${htmlesc(fileName || "Excel")}</span>
        </div>
      </div>
      <div class="hint" style="margin-top:8px;">Показуємо перші ${Math.max(0, body.length)} рядків для перевірки перед імпортом.</div>
      ${hasExistingTable ? `<div class="hint" style="margin-top:4px;color:var(--danger,#c85a5a);">Поточна таблиця в записі буде замінена цією вкладкою.</div>` : ``}
      ${rowDiffLabel ? `<div class="hint" style="margin-top:4px;">${htmlesc(rowDiffLabel)}</div>` : ``}
    </div>
    <div class="task-desc rich-text">${previewTable}</div>
    <div class="actions" style="margin-top:12px;">
      <button type="button" class="btn primary" data-action="importReferenceWorkbookSheet" data-arg1="${attrEsc(sheetName)}">${hasExistingTable ? "Замінити поточну таблицю" : "Імпортувати вкладку"}</button>
      <button type="button" class="btn ghost" data-action="hideSheet">Назад</button>
    </div>
  `;

}

function captureReferenceEntryFormState(){

  const textarea = document.getElementById("referenceEntryText");
  const titleInput = document.getElementById("referenceEntryTitle");
  const deptSelect = document.getElementById("referenceEntryDept");
  const typeSelect = document.getElementById("referenceEntryTableType");

  return {
    sheetTitle: sheetTitle?.textContent || "Редагувати запис",
    sheetHtml: sheetBody?.innerHTML || "",
    titleValue: titleInput?.value || "",
    deptValue: deptSelect?.value || "",
    tableTypeValue: typeSelect?.value || "",
    textValue: textarea?.value || "",
    tableRaw: textarea?.dataset?.tableRaw || "",
    tablePrevRaw: textarea?.dataset?.tablePrevRaw || ""
  };

}

function restoreReferenceEntryFormState(snapshot){

  if(!snapshot?.sheetHtml) return false;

  sheetTitle.textContent = snapshot.sheetTitle || "Редагувати запис";
  sheetBody.innerHTML = snapshot.sheetHtml;
  modal.classList.add("show");

  const titleInput = document.getElementById("referenceEntryTitle");
  const deptSelect = document.getElementById("referenceEntryDept");
  const typeSelect = document.getElementById("referenceEntryTableType");
  const textarea = document.getElementById("referenceEntryText");

  if(titleInput) titleInput.value = snapshot.titleValue || "";
  if(deptSelect) deptSelect.value = snapshot.deptValue || "";
  if(typeSelect) typeSelect.value = snapshot.tableTypeValue || "";

  if(textarea){
    textarea.value = snapshot.textValue || "";
    if(snapshot.tableRaw){
      textarea.dataset.tableRaw = snapshot.tableRaw;
    } else {
      delete textarea.dataset.tableRaw;
    }
    if(snapshot.tablePrevRaw){
      textarea.dataset.tablePrevRaw = snapshot.tablePrevRaw;
    } else {
      delete textarea.dataset.tablePrevRaw;
    }
  }

  document.getElementById("referenceEntryTableType")?.addEventListener("change", syncReferenceEntryImportUi);
  syncReferenceEntryImportUi();

  return !!textarea;

}

function openReferenceWorkbookSheetPreview(sheetName=""){

  const pending = UI.pendingReferenceWorkbook || null;

  if(!pending?.workbook){
    showToast("Немає підготовленого Excel-імпорту.", "warn");
    return;
  }

  const resolvedSheetName = sheetName || pickWorkbookSheetName(pending.workbook, pending.preferredSheet);
  const rows = getWorkbookSheetPreviewRows(pending.workbook, resolvedSheetName);

  if(rows.length < 2){
    showToast("У вибраній вкладці не знайдено повної таблиці для імпорту.", "warn");
    return;
  }

  const typeLabel = pending.type === "compare"
    ? "Порівняння"
    : (pending.type === "delta_bpla" ? "Delta / БпЛА" : "Delta / НРК");

  showSheet(
    "Перевірка вкладки",
    buildReferenceWorkbookSheetPreviewHtml(pending.fileName, resolvedSheetName, rows, typeLabel),
    {stack:true}
  );

}

function applyReferenceWorkbookImport(sheetName="", opts={}){

  const pending = UI.pendingReferenceWorkbook || null;

  if(!pending?.workbook){
    showToast("Немає підготовленого Excel-імпорту.", "warn");
    return;
  }

  const resolvedSheetName = sheetName || pickWorkbookSheetName(pending.workbook, pending.preferredSheet);
  const importedRows = getWorkbookSheetPreviewRows(pending.workbook, resolvedSheetName);

  if(importedRows.length < 2){
    showToast("У вибраній вкладці не знайдено повної таблиці для імпорту.", "warn");
    return;
  }

  let textarea = document.getElementById("referenceEntryText");

  if(!textarea && pending.formState){
    restoreReferenceEntryFormState(pending.formState);
    textarea = document.getElementById("referenceEntryText");
  }

  if(!textarea){
    showToast("Не вдалося повернути форму запису для імпорту.", "warn");
    return;
  }

  if(!String(textarea.value || "").trim()){
      const typeLabel = pending.type === "delta_nrk"
        ? "Delta / НРК"
        : (pending.type === "delta_bpla" ? "Delta / БпЛА" : "Порівняння");
    textarea.value = `Імпорт із ${typeLabel} · ${pending.fileName}${resolvedSheetName ? ` · ${resolvedSheetName}` : ""}`;
  }

  writeTextTableToTextarea("referenceEntryText", importedRows);
  renderReferenceEntryTableWorkspace("referenceEntryText", pending.type, importedRows);

  if(!opts.keepModal){
    UI.pendingReferenceWorkbook = null;
    _sheetStackOn = false;
    _sheetStack = [];
    modal.classList.remove("show");
    sheetBody.innerHTML = "";
  }

  showToast(`Таблицю оновлено: ${Math.max(0, importedRows.length - 1)} рядків з аркуша ${resolvedSheetName}`, "ok");

}

function importReferenceWorkbookSheet(sheetName=""){

  applyReferenceWorkbookImport(sheetName);

}

function importReferenceWorkbook(){

  const tableType = normalizeReferenceTableType(document.getElementById("referenceEntryTableType")?.value || "none");
  const config = getReferenceWorkbookImportConfig(tableType);

  if(!config.enabled){
    showToast("Імпорт .xlsx зараз доступний для типів Порівняння, Delta / БпЛА та Delta / НРК.", "warn");
    return;
  }

  if(!canUseWorkbookImport()){
    showToast("Імпорт .xlsx зараз недоступний у цьому браузері.", "warn");
    return;
  }

  const input = document.createElement("input");
  input.type = "file";
  input.accept = ".xlsx,.xls,.xlsm";

  input.addEventListener("change", ()=>{
    const file = input.files?.[0];
    if(!file) return;

    const reader = new FileReader();

    reader.onload = ()=>{
      try{
        const workbook = XLSX.read(reader.result, {type:"array"});
        if(config.type === "delta_nrk" || config.type === "delta_bpla"){
          UI.pendingReferenceWorkbook = {
            type: config.type,
            fileName: file.name,
            workbook,
            preferredSheet: config.preferredSheet
          };
          applyReferenceWorkbookImport(pickWorkbookSheetName(workbook, config.preferredSheet), {keepModal:true});
          UI.pendingReferenceWorkbook = null;
          return;
        }

        const sheetOptions = (Array.isArray(workbook?.SheetNames) ? workbook.SheetNames : [])
          .map(name=>{
            const rows = getWorkbookSheetPreviewRows(workbook, name);
            const header = rows[0] || [];
            return {
              name,
              rowCount: Math.max(0, rows.length - 1),
              colCount: header.length || Math.max(0, ...rows.map(row=>Array.isArray(row) ? row.length : 0))
            };
          })
          .filter(item=>item.rowCount > 0 && item.colCount > 0);

        if(!sheetOptions.length){
          showToast("У файлі не знайдено повних вкладок для імпорту.", "warn");
          return;
        }

        UI.pendingReferenceWorkbook = {
          type: config.type,
          fileName: file.name,
          workbook,
          preferredSheet: config.preferredSheet,
          formState: captureReferenceEntryFormState()
        };

        const resolvedPreferredSheet = pickWorkbookSheetName(workbook, config.preferredSheet);

        showSheet(
          "Оберіть вкладку Excel",
          buildReferenceWorkbookSheetPickerHtml(file.name, sheetOptions, resolvedPreferredSheet),
          {stack:true}
        );
      } catch(err){
        console.warn("reference workbook import failed", err);
        showToast("Не вдалося прочитати Excel-файл. Спробуй ще раз або перевір його формат.", "warn");
      }
    };

    reader.onerror = ()=>{
      showToast("Не вдалося прочитати вибраний файл.", "warn");
    };

    reader.readAsArrayBuffer(file);
  }, {once:true});

  input.click();

}

function importReferenceDeltaWorkbook(){

  importReferenceWorkbook();

}

function syncReferenceEntryImportUi(){

  const row = document.getElementById("referenceDeltaImportRow");
  const type = normalizeReferenceTableType(document.getElementById("referenceEntryTableType")?.value || "none");
  const config = getReferenceWorkbookImportConfig(type);
  const hint = document.getElementById("referenceWorkbookImportHint");

  if(row) row.hidden = !config.enabled;
  if(hint) hint.innerHTML = config.hint || "";

  const rows = getCurrentTextareaTableRows("referenceEntryText");

  if(rows.length){
    renderReferenceEntryTableWorkspace("referenceEntryText", type, rows);
  }

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

function compactTimeFirst(dt){

  const {date, time} = splitDateTimeLoose(dt);

  if(!date) return "";

  const d = fmtDateShort(date);

  if(!time) return d;

  return `${time.replace(":", ".")} ${d}`;

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

    "на_перевірці":"На перевірці",

    "перевірено":"Перевірено",

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

    "на_перевірці":"🔎",

    "перевірено":"👁",

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

  if(s==="очікує_підтвердження" || s==="на_перевірці") return "b-violet";

  if(s==="в_процесі" || s==="на_контролі" || s==="перевірено") return "b-blue";

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

const TASK_EVAL_CRITERIA = [

  {key:"labor", label:"Трудомісткість"},

  {key:"importance", label:"Важливість"},

  {key:"urgency", label:"Терміновість"},

  {key:"result", label:"Результат"},

];

const TASK_EVAL_HINTS = {

  labor: "1 — швидка проста дія; 3 — нормальний робочий обсяг; 5 — багато координації, матеріалів або тривала робота.",

  importance: "1 — локальне питання; 3 — важливо для відділу; 5 — суттєво для керівництва, кількох відділів або ключового процесу.",

  urgency: "1 — був запас часу; 3 — треба було вкластися в строк; 5 — дуже терміново, нарада / перевірка / сьогодні на сьогодні.",

  result: "1 — мінімальний або проміжний ефект; 3 — повний очікуваний результат; 5 — сильний результат із відчутним впливом."

};

const TASK_EVAL_PRESETS = [

  {
    key:"letter",
    label:"Листи",
    scores:{labor:2, importance:3, urgency:3, result:3},
    hint:"Для листів, відповідей, направлень.",
    ranges:{labor:"1–2", importance:"2–4", urgency:"2–4", result:"2–3"}
  },

  {
    key:"service_docs",
    label:"Службові документи",
    scores:{labor:2, importance:4, urgency:3, result:3},
    hint:"Для службових записок, розпоряджень, наказів, рапортів.",
    ranges:{labor:"2–3", importance:"3–4", urgency:"2–4", result:"2–4"}
  },

  {
    key:"analysis",
    label:"Довідки та таблиці",
    scores:{labor:4, importance:4, urgency:3, result:4},
    hint:"Для довідок, порівняльних таблиць, зведень, звітів, списків.",
    ranges:{labor:"3–5", importance:"2–4", urgency:"2–3", result:"2–4"}
  },

  {
    key:"external",
    label:"Зовнішня взаємодія",
    scores:{labor:4, importance:5, urgency:3, result:4},
    hint:"Для виробників, компаній, НГУ, інших структур, робочої координації.",
    ranges:{labor:"2–4", importance:"3–5", urgency:"2–4", result:"3–5"}
  },

  {
    key:"event",
    label:"Зустрічі та участь",
    scores:{labor:4, importance:4, urgency:3, result:4},
    hint:"Для зустрічей, демонстрацій, участі в заходах і виїздів.",
    ranges:{labor:"2–4", importance:"3–4", urgency:"2–4", result:"3–4"}
  },

  {
    key:"training",
    label:"Навчання",
    scores:{labor:4, importance:4, urgency:3, result:4},
    hint:"Для організації навчання, проходження навчання, оповіщення по навчанню.",
    ranges:{labor:"3–4", importance:"3–4", urgency:"2–3", result:"3–5"}
  },

  {
    key:"permits",
    label:"Допуски та перепустки",
    scores:{labor:3, importance:4, urgency:3, result:4},
    hint:"Для допусків, перепусток, посвідчень, недопусків.",
    ranges:{labor:"2–3", importance:"3–5", urgency:"2–4", result:"3–4"}
  },

  {
    key:"support",
    label:"Доступи та налаштування",
    scores:{labor:2, importance:3, urgency:2, result:3},
    hint:"Для доступів, токенів, АРМ, робочого місця, технічного налаштування.",
    ranges:{labor:"2–3", importance:"2–4", urgency:"2–4", result:"2–4"}
  },

  {
    key:"docs_decl",
    label:"Оформлення та декларації",
    scores:{labor:3, importance:4, urgency:3, result:3},
    hint:"Для пакетів документів, форм, декларацій, службового оформлення.",
    ranges:{labor:"2–4", importance:"3–4", urgency:"2–4", result:"2–4"}
  },

  {
    key:"assets",
    label:"Майно та господарські питання",
    scores:{labor:3, importance:3, urgency:2, result:3},
    hint:"Для майна, авто, приміщень, харчування, господарських питань.",
    ranges:{labor:"2–3", importance:"2–4", urgency:"1–3", result:"2–3"}
  },

  {
    key:"followup",
    label:"Уточнення та контроль",
    scores:{labor:1, importance:2, urgency:3, result:2},
    hint:"Для уточнень, коротких доручень, контролю, перевірки статусу.",
    ranges:{labor:"1–2", importance:"1–3", urgency:"2–4", result:"1–2"}
  },

  {
    key:"other",
    label:"Інше / разові",
    scores:{labor:3, importance:3, urgency:3, result:3},
    hint:"Для нетипових задач, які не лягають у стандартні категорії.",
    ranges:{labor:"оцінюй вручну", importance:"оцінюй вручну", urgency:"оцінюй вручну", result:"оцінюй вручну"}
  }

];

function guessTaskEvaluationPreset(task){

  const title = String(task?.title || "").toLowerCase();

  if(!title) return null;

  if(/лист|відповід|направлен|звернен/.test(title)) return "letter";

  if(/службов|записк|розпоряджен|наказ|рапорт/.test(title)) return "service_docs";

  if(/таблиц|довідк|слайд|аналіт|порівнял|зведен|звіт|список/.test(title)) return "analysis";

  if(/взаємод|виробник|компан|нгу|нацпол|окоіз|диндур|квантум|брейв|дснс/.test(title)) return "external";

  if(/зустріч|участ|демонстрац|захід|виїзд|відеоконференц/.test(title)) return "event";

  if(/навчан|оператор|ппм|неон/.test(title)) return "training";

  if(/допуск|перепуст|посвідчен|недопуск/.test(title)) return "permits";

  if(/декларац|пакет документ|форма для мвс|оформлен|договір|договор|подання документ/.test(title)) return "docs_decl";

  if(/арм|робочого місця|токен|доступ|налаштуван|дельт|mission/.test(title)) return "support";

  if(/майн|авто|машин|приміщ|їдальн|харчуван|столів|облік|дефект|господар/.test(title)) return "assets";

  if(/уточн|контрол|чекаємо|оповіст|перевірити|дізнат|розібратись|розібратися/.test(title)) return "followup";

  return null;

}

function taskEvalPresetRangeText(value){
  return value ? String(value) : "—";
}

function renderTaskEvaluationPresetGuide(preset){
  if(!preset){
    return `
      <div class="item" style="cursor:default; margin-top:10px;">
        <div class="name">Підказка по типу задачі</div>
        <div class="hint">Обери тип задачі вище — і система підкаже робочі діапазони оцінки. Це лише орієнтир, не жорстке правило.</div>
      </div>
    `;
  }

  const ranges = preset.ranges || {};
  return `
    <div class="item" style="cursor:default; margin-top:10px;">
      <div class="name">Підказка по типу: ${htmlesc(preset.label)}</div>
      <div class="hint">${htmlesc(preset.hint || "")}</div>
      <div class="hint" style="margin-top:8px;">
        <b>Трудомісткість</b>: ${htmlesc(taskEvalPresetRangeText(ranges.labor))} •
        <b>Важливість</b>: ${htmlesc(taskEvalPresetRangeText(ranges.importance))} •
        <b>Терміновість</b>: ${htmlesc(taskEvalPresetRangeText(ranges.urgency))} •
        <b>Результат</b>: ${htmlesc(taskEvalPresetRangeText(ranges.result))}
      </div>
    </div>
  `;
}

function getTaskEvaluationPreset(task, evaluation=null){

  const presetKey = evaluation?.presetKey || guessTaskEvaluationPreset(task);

  return TASK_EVAL_PRESETS.find(x=>x.key===presetKey) || null;

}

function getTaskEvaluation(taskId){

  if(!taskId || !Array.isArray(STATE.taskEvaluations)) return null;

  return STATE.taskEvaluations.find(x=>x && x.taskId===taskId) || null;

}

function evalTotalScore(evaluation){

  if(!evaluation) return 0;

  return TASK_EVAL_CRITERIA.reduce((sum, item)=>sum + Number(evaluation[item.key] || 0), 0);

}

function isTaskAwaitingEvaluation(task){

  if(!task || task.status!=="закрито" || isAnnouncement(task) || getTaskEvaluation(task.id)) return false;

  const closeDate = getCloseDateForTask(task) || "";
  const startDate = String(STATE.evaluationStartDate || kyivDateStr());

  return !!closeDate && closeDate >= startDate;

}

function evaluationStatusLabel(task){

  if(!task || task.status!=="закрито") return "Не закрито";

  if(getTaskEvaluation(task.id)) return "Оцінено";

  return isTaskAwaitingEvaluation(task) ? "Не оцінено" : "Поза періодом оцінювання";

}

function analyticsEvalPeriodRange(periodKey){

  const today = kyivDateStr();

  if(periodKey === "week"){

    return {from: addDays(today, -6), to: today, label:"Останні 7 днів"};

  }

  if(periodKey === "quarter"){

    return {from: addDays(today, -89), to: today, label:"Останні 90 днів"};

  }

  if(periodKey === "all"){

    return {from:null, to:null, label:"За весь час"};

  }

  const monthStart = `${today.slice(0,7)}-01`;

  return {from: monthStart, to: today, label:"Поточний місяць"};

}

function buildEvalSlices(rows, colors){

  const safeRows = (rows || []).filter(x=>Number(x?.value || 0) > 0);

  if(!safeRows.length){

    return {gradient:"conic-gradient(#dfe6f6 0 360deg)", legendRows:[]};

  }

  const total = safeRows.reduce((sum, item)=>sum + Number(item.value || 0), 0) || 1;
  let offset = 0;
  const gradientParts = [];
  const legendRows = [];

  safeRows.forEach((item, index)=>{

    const value = Number(item.value || 0);
    const sweep = (value / total) * 360;
    const color = (colors && colors[index % colors.length]) || "#5f8ef5";
    gradientParts.push(`${color} ${offset}deg ${offset + sweep}deg`);
    legendRows.push({...item, color, percent: Math.round((value / total) * 100)});
    offset += sweep;

  });

  return {gradient:`conic-gradient(${gradientParts.join(", ")})`, legendRows};

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

  const savedTask = STATE.tasks[idx];

  if(savedTask?.type==="personal"){

    ensureCriticalStateSaved("Моя задача поки що збережена тимчасово. Дочекайся синхронізації перед оновленням сторінки.");

  } else if(hasStoredTable(savedTask?.description || patch?.description || "")){

    ensureCriticalStateSaved("Таблиця поки що збережена тимчасово. Дочекайся синхронізації перед оновленням сторінки.");

  }

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

  if(task?.type==="personal"){

    ensureCriticalStateSaved("Моя задача поки що збережена тимчасово. Дочекайся синхронізації перед оновленням сторінки.");

  } else if(hasStoredTable(task?.description || "")){

    ensureCriticalStateSaved("Таблиця поки що збережена тимчасово. Дочекайся синхронізації перед оновленням сторінки.");

  }

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
  const triggerDate = addDays(scheduledDate, -1);
  const shouldExist = today >= triggerDate;

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
  const nextSheetClass = String(opts.sheetClass || "").trim();

  if(useStack && modal.classList.contains("show")){

    _sheetStack.push({

      title: sheetTitle.textContent,

      html: sheetBody.innerHTML,

      sheetClass: sheetEl ? sheetEl.className : "sheet",

      scrollTop: (sheetEl ? sheetEl.scrollTop : sheetBody.scrollTop) || 0

    });

    _sheetStackOn = true;

  }

  sheetTitle.textContent = title;

  sheetBody.innerHTML = html;

  if(sheetEl){
    sheetEl.className = `sheet${nextSheetClass ? ` ${nextSheetClass}` : ""}`;
  }

  modal.classList.add("show");

}

function hideSheet(){

  if(_sheetStackOn && _sheetStack.length){

    const prev = _sheetStack.pop();

    sheetTitle.textContent = prev.title;

    sheetBody.innerHTML = prev.html;

    if(sheetEl){
      sheetEl.className = prev.sheetClass || "sheet";
    }

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

  if(sheetEl){
    sheetEl.className = "sheet";
  }

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

function remindPendingEvaluations(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss") return;

  const pendingCount = STATE.tasks.filter(t=>isTaskAwaitingEvaluation(t)).length;

  if(!pendingCount) return;

  const today = kyivDateStr();
  const lastShown = safeGet(EVAL_TOAST_DATE_KEY);

  if(lastShown === today) return;

  safeSet(EVAL_TOAST_DATE_KEY, today);
  showToast(`Є ${pendingCount} закритих задач без оцінки. Перевір вкладку “Аналітика”.`, "info");

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

  ROUTES.CONTROL,

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

  renderedTableModals: {},

  deptOpen: {},

  analyticsShowDetails: false,

  analyticsEvalPeriod: "month",

  analyticsEvalDeptFilter: "all",

  analyticsEvalUserFilter: "all",

  analyticsEvalStatusFilter: "pending",

  analyticsEvalTypeFilter: "all",

  analyticsEvalPresetFilter: "all",

  refSearch: "",

  refDeptFilter: "all",

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

function referenceNotePreview(text, fallback="Немає запису. Натисни, щоб додати коротку довідку."){

  const value = String(text || "").trim();

  if(!value) return fallback;

  return value.length > 180 ? `${value.slice(0, 180)}…` : value;

}

const REFERENCE_SECTION_FIELDS = [

  {key:"orders", label:"Накази / нормативка", placeholder:"Які накази, положення, нормативні документи або правила тут важливо пам’ятати."},

  {key:"contacts", label:"Контакти / канали", placeholder:"Ключові телефони, установи, кому писати або дзвонити по цьому напрямку."},

  {key:"staff", label:"Штатні пропозиції / структура", placeholder:"Короткі нотатки про штат, потреби, пропозиції, посади, навантаження."},

  {key:"other", label:"Інше", placeholder:"Будь-які інші примітки, які треба тримати під рукою."},

];

function referenceSectionsToText(sections){

  const data = normalizeReferenceNotes({general: sections}).general;

  return REFERENCE_SECTION_FIELDS
    .map(field=>{
      const value = String(data[field.key] || "").trim();
      return value ? `${field.label}: ${value}` : "";
    })
    .filter(Boolean)
    .join("\n");

}

function referenceSectionsFilledCount(sections){

  const data = normalizeReferenceNotes({general: sections}).general;

  return REFERENCE_SECTION_FIELDS.filter(field=>String(data[field.key] || "").trim()).length;

}

function buildReferenceEditorFields(prefix, sections){

  const data = normalizeReferenceNotes({general: sections}).general;

  return REFERENCE_SECTION_FIELDS.map(field=>`
    <div class="field">
      <label>${field.label}</label>
      <textarea id="${prefix}_${field.key}" class="task-desc-input" placeholder="${field.placeholder}">${htmlesc(data[field.key] || "")}</textarea>
    </div>
  `).join("");

}

function readReferenceEditorFields(prefix){

  const next = {};

  REFERENCE_SECTION_FIELDS.forEach(field=>{
    next[field.key] = (document.getElementById(`${prefix}_${field.key}`)?.value || "").trim();
  });

  return next;

}

function setReferenceSearchFromInput(){

  UI.refSearch = (document.getElementById("referenceSearch")?.value || "").trim().toLowerCase();

  render();

}

function openReferenceGeneral(){

  const u = currentSessionUser();
  const notes = normalizeReferenceNotes(STATE.referenceNotes);
  const readOnly = !!u?.readOnly;

  showSheet("Цікаве — загальне", `

    <div class="hint">Тут можна тримати під рукою загальну довідкову інформацію: ключові накази, контакти, нотатки, правила роботи, короткі нагадування.</div>
    <div class="sep"></div>
    ${buildReferenceEditorFields("referenceGeneral", notes.general).replaceAll("<textarea ", `<textarea ${readOnly ? "readonly" : ""} `)}
    <div class="actions" style="margin-top:14px;">
      ${readOnly ? "" : `<button class="btn primary" data-action="saveReferenceGeneralNow">Зберегти</button>`}
      <button class="btn ghost" data-action="hideSheet">${readOnly ? "Закрити" : "Скасувати"}</button>
    </div>

  `);

}

function openReferenceDept(deptId){

  const u = currentSessionUser();
  const dept = getDeptById(deptId);
  if(!dept) return;

  const notes = normalizeReferenceNotes(STATE.referenceNotes);
  const text = notes.byDept?.[deptId] || "";
  const readOnly = !!u?.readOnly;

  showSheet(`Цікаве — ${dept.name}`, `

    <div class="hint">Сюди зручно записувати саме довідкову інформацію по відділу: накази, штатні пропозиції, особливості, контакти, примітки по напрямку.</div>
    <div class="sep"></div>
    ${buildReferenceEditorFields("referenceDept", text).replaceAll("<textarea ", `<textarea ${readOnly ? "readonly" : ""} `)}
    <div class="actions" style="margin-top:14px;">
      ${readOnly ? "" : `<button class="btn primary" data-action="saveReferenceDeptNow" data-arg1="${dept.id}">Зберегти</button>`}
      <button class="btn ghost" data-action="hideSheet">${readOnly ? "Закрити" : "Скасувати"}</button>
    </div>

  `);

}

function saveReferenceGeneralNow(){

  const text = readReferenceEditorFields("referenceGeneral");

  STATE.referenceNotes = normalizeReferenceNotes(STATE.referenceNotes);
  STATE.referenceNotes.general = text;

  saveState(STATE);
  ensureCriticalStateSaved("Запис у Цікавому поки що збережений тимчасово. Дочекайся синхронізації перед оновленням сторінки.");
  hideSheet();
  showToast("Загальну довідку збережено", "ok");
  render();

}

function saveReferenceDeptNow(deptId){

  const dept = getDeptById(deptId);
  if(!dept) return;

  const text = readReferenceEditorFields("referenceDept");

  STATE.referenceNotes = normalizeReferenceNotes(STATE.referenceNotes);
  STATE.referenceNotes.byDept[deptId] = text;

  saveState(STATE);
  ensureCriticalStateSaved("Запис у Цікавому поки що збережений тимчасово. Дочекайся синхронізації перед оновленням сторінки.");
  hideSheet();
  showToast(`Збережено: ${dept.name}`, "ok");
  render();

}

function getReferenceEntryDeptLabel(deptId){

  return deptId ? (getDeptById(deptId)?.name || "Відділ") : "Загальне";

}


function getReferenceTextPreview(text=""){

  return String(text || "").replace(/\s+/g, " ").trim();

}

function escapeRegExp(text=""){

  return String(text || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

}

function highlightReferenceText(text="", query=""){

  const source = String(text || "");
  const needle = String(query || "").trim();
  if(!needle) return htmlesc(source);

  const parts = source.split(new RegExp(`(${escapeRegExp(needle)})`, "ig"));
  return parts.map((part, idx)=> idx % 2 ? `<mark class="ref-hit">${htmlesc(part)}</mark>` : htmlesc(part)).join("");

}

function getReferenceAttachmentIcon(item){

  const src = `${item?.title || ""} ${item?.url || ""}`.toLowerCase();
  if(/\.(png|jpe?g|webp|gif|svg)(\?|#|$)/.test(src)) return "🖼";
  if(/\.(pdf)(\?|#|$)/.test(src)) return "📄";
  if(/\.(doc|docx|odt|rtf)(\?|#|$)/.test(src)) return "📝";
  if(/\.(xls|xlsx|csv)(\?|#|$)/.test(src)) return "📊";
  if(/\.(ppt|pptx)(\?|#|$)/.test(src)) return "📽";
  if(/\.(zip|rar|7z)(\?|#|$)/.test(src)) return "🗜";
  return "📎";

}

function openReferenceLink(entryId="", attachmentId=""){

  const u = currentSessionUser();
  const readOnly = !!u?.readOnly;
  const notes = normalizeReferenceNotes(STATE.referenceNotes);
  const parentEntry = (notes.entries || []).find(x=>x && x.id===entryId) || null;
  const attachment = attachmentId ? (notes.attachments || []).find(x=>x && x.id===attachmentId) || null : null;

  if(!parentEntry) {
    showToast("Спочатку відкрий існуючий запис", "warn");
    return;
  }

  showSheet(readOnly ? "Перегляд вкладення" : (attachment ? "Редагувати вкладення" : "Нове вкладення"), `

    <div class="hint">Запис: <b>${htmlesc(parentEntry.title || "Без назви")}</b></div>
    <div class="sep"></div>
    <div class="field">
      <label>Назва вкладення</label>
      <input id="referenceLinkTitle" type="text" value="${htmlesc(attachment?.title || "")}" placeholder="Наприклад: Наказ №..., PDF, таблиця" ${readOnly ? "readonly" : ""} />
    </div>
    <div class="field">
      <label>Посилання</label>
      <input id="referenceLinkUrl" type="url" value="${htmlesc(attachment?.url || "")}" placeholder="https://..." ${readOnly ? "readonly" : ""} />
    </div>
    <div class="field">
      <label>Короткий опис</label>
      <textarea id="referenceLinkNote" class="task-desc-input" placeholder="Що це за файл і для чого він потрібен" ${readOnly ? "readonly" : ""}>${htmlesc(attachment?.note || "")}</textarea>
    </div>
    <div class="actions" style="margin-top:14px;">
      ${readOnly ? "" : `<button class="btn primary" data-action="saveReferenceLinkNow" data-arg1="${entryId}" data-arg2="${attachment?.id || ""}">Зберегти</button>`}
      ${(!readOnly && attachment) ? `<button class="btn danger" data-action="deleteReferenceLinkNow" data-arg1="${entryId}" data-arg2="${attachment.id}">Видалити</button>` : ""}
      <button class="btn ghost" data-action="hideSheet">${readOnly ? "Закрити" : "Скасувати"}</button>
    </div>

  `);

}


function saveReferenceLinkNow(entryId="", attachmentId=""){

  const title = (document.getElementById("referenceLinkTitle")?.value || "").trim();
  const url = (document.getElementById("referenceLinkUrl")?.value || "").trim();
  const note = (document.getElementById("referenceLinkNote")?.value || "").trim();

  if(!entryId){
    showToast("Не знайдено запис для вкладення", "warn");
    return;
  }

  if(!title){
    showToast("Додай назву вкладення", "warn");
    return;
  }

  if(!url){
    showToast("Додай посилання на файл", "warn");
    return;
  }

  STATE.referenceNotes = normalizeReferenceNotes(STATE.referenceNotes);
  const parentEntry = (STATE.referenceNotes.entries || []).find(x=>x && x.id===entryId) || null;
  if(!parentEntry){
    showToast("Не знайдено запис для вкладення", "warn");
    return;
  }
  const items = Array.isArray(STATE.referenceNotes.attachments) ? STATE.referenceNotes.attachments.slice() : [];
  const now = nowIsoKyiv();
  const existingIdx = attachmentId ? items.findIndex(x=>x && x.id===attachmentId) : -1;

  if(existingIdx >= 0){
    items[existingIdx] = {
      ...items[existingIdx],
      entryId,
      deptId: parentEntry.deptId || "",
      title,
      url,
      note,
      updatedAt: now,
    };
  } else {
    items.unshift({
      id: uid("ref_file"),
      entryId,
      deptId: parentEntry.deptId || "",
      title,
      url,
      note,
      createdAt: now,
      updatedAt: now,
    });
  }

  STATE.referenceNotes.attachments = items;
  saveState(STATE);
  ensureCriticalStateSaved("Вкладення поки що збережене тимчасово. Дочекайся синхронізації перед оновленням сторінки.");
  hideSheet();
  showToast("Вкладення збережено", "ok");
  render();

}


function deleteReferenceLinkNow(entryId="", attachmentId=""){

  if(!attachmentId) return;

  STATE.referenceNotes = normalizeReferenceNotes(STATE.referenceNotes);
  STATE.referenceNotes.attachments = (STATE.referenceNotes.attachments || []).filter(x=>x && x.id!==attachmentId);
  saveState(STATE);
  ensureCriticalStateSaved("Вкладення поки що збережене тимчасово. Дочекайся синхронізації перед оновленням сторінки.");
  hideSheet();
  showToast("Вкладення видалено", "ok");
  render();

}


function setReferenceDeptFilterFromInput(){

  UI.refDeptFilter = document.getElementById("referenceDeptFilter")?.value || "all";

  render();

}

function setReferenceDeptFilter(filter="all"){

  UI.refDeptFilter = filter || "all";

  render();

}

function toggleReferenceEntry(entryId=""){

  if(!entryId) return;
  if(!UI.refExpandedEntries || typeof UI.refExpandedEntries !== "object") UI.refExpandedEntries = {};
  UI.refExpandedEntries[entryId] = !UI.refExpandedEntries[entryId];
  render();

}

function openReferenceEntry(entryId=""){

  const u = currentSessionUser();
  const readOnly = !!u?.readOnly;
  const notes = normalizeReferenceNotes(STATE.referenceNotes);
  const entry = (notes.entries || []).find(x=>x && x.id===entryId) || null;
  const deptOptions = [`<option value="">Загальне</option>`, ...STATE.departments.map(dept=>`<option value="${dept.id}" ${(entry?.deptId || "")===dept.id ? "selected" : ""}>${htmlesc(dept.name)}</option>`)].join("");
  const tableType = normalizeReferenceTableType(entry?.tableType);
  const tableTypeOptions = REFERENCE_TABLE_TYPE_OPTIONS.map(item=>`<option value="${item.id}" ${tableType===item.id ? "selected" : ""}>${htmlesc(item.label)}</option>`).join("");

  showSheet(readOnly ? "Перегляд запису" : (entry ? "Редагувати запис" : "Новий запис"), `

    <div class="field">
      <label>Відділ</label>
      <select id="referenceEntryDept" ${readOnly ? "disabled" : ""}>
        ${deptOptions}
      </select>
    </div>
    <div class="field">
      <label>Назва запису</label>
      <input id="referenceEntryTitle" type="text" value="${htmlesc(entry?.title || "")}" placeholder="Коротка назва запису" ${readOnly ? "readonly" : ""} />
    </div>
    <div class="field">
      <label>Тип таблиці</label>
      <select id="referenceEntryTableType" ${readOnly ? "disabled" : ""}>
        ${tableTypeOptions}
      </select>
      <div class="hint">Поки що це підготовка під аналітику для таблиць у записі.</div>
      ${readOnly ? "" : `
        <div class="reference-import-row" id="referenceDeltaImportRow" hidden>
          <button type="button" class="btn ghost btn-mini" data-action="importReferenceWorkbook">Імпорт .xlsx</button>
          <div class="hint" id="referenceWorkbookImportHint">Для <span class="mono">Delta / НРК</span> можна завантажити весь Excel-файл напряму — без копіювання шматків таблиці.</div>
        </div>
      `}
    </div>
    ${readOnly ? `
      <div class="field">
        <label>Текст</label>
        <div class="task-desc-input ref-entry-preview">${renderTaskDescWithTableToggle(entry?.text || "", "Текст", {showEmpty:true, updatedAt:entry?.updatedAt, className:"ref-entry-preview rich-text", analyticsType: tableType, analyticsTitle: entry?.title || "Запис", dynamicVersions: entry?.tableVersions || []})}</div>
      </div>
    ` : `
      <div class="field">
        <label>Текст</label>
        ${formatToolbar("referenceEntryText", "inline", {table:true})}
        <textarea id="referenceEntryText" class="task-desc-input" placeholder="Запиши коротко те, що хочеш тримати під рукою." ${readOnly ? "readonly" : ""}>${htmlesc(stripStoredTables(entry?.text || ""))}</textarea>
      </div>
    `}
    ${entry?.updatedAt ? `<div class="hint">Оновлено: <span class="mono">${fmtDate(toDateOnly(entry.updatedAt) || "")}</span></div>` : ""}
    <div class="actions" style="margin-top:14px;">
      ${readOnly ? "" : `<button class="btn primary" data-action="saveReferenceEntryNow" data-arg1="${entry?.id || ""}">Зберегти</button>`}
      ${(!readOnly && entry) ? `<button class="btn danger" data-action="deleteReferenceEntryNow" data-arg1="${entry.id}">Видалити</button>` : ""}
      <button class="btn ghost" data-action="hideSheet">${readOnly ? "Закрити" : "Скасувати"}</button>
    </div>

  `);

  if(!readOnly){

    initDescriptionTableState("referenceEntryText", entry?.text || "");

    const existingTable = findStoredTableBlock(entry?.text || "");

    if(existingTable) renderReferenceEntryTableWorkspace("referenceEntryText", tableType, existingTable.rows);

    document.getElementById("referenceEntryTableType")?.addEventListener("change", syncReferenceEntryImportUi);
    syncReferenceEntryImportUi();

  }

}

function saveReferenceEntryNow(entryId=""){

  const deptId = document.getElementById("referenceEntryDept")?.value || "";
  const title = (document.getElementById("referenceEntryTitle")?.value || "").trim();
  const tableType = normalizeReferenceTableType(document.getElementById("referenceEntryTableType")?.value || "none");
  if(document.querySelector('.text-table-editor[data-for="referenceEntryText"]')){
    writeTextTableToTextarea("referenceEntryText", readTextTableEditorRows("referenceEntryText"));
  }
  const text = buildDescriptionValueFromEditor("referenceEntryText").trim();

  if(!text){
    showToast("Додай текст запису", "warn");
    return;
  }

  STATE.referenceNotes = normalizeReferenceNotes(STATE.referenceNotes);
  const entries = Array.isArray(STATE.referenceNotes.entries) ? STATE.referenceNotes.entries.slice() : [];
  const now = nowIsoKyiv();
  const existingIdx = entryId ? entries.findIndex(x=>x && x.id===entryId) : -1;
  let tableVersionCreated = false;

  if(existingIdx >= 0){
    const existingEntry = entries[existingIdx] || {};
    const existingCurrentTable = findStoredTableBlock(existingEntry.text || "");
    const nextCurrentTable = findStoredTableBlock(text || "");
    let nextTableVersions = Array.isArray(existingEntry.tableVersions) ? existingEntry.tableVersions.slice() : [];

    if(
      tableType === "staffing" &&
      existingCurrentTable?.rows?.length &&
      nextCurrentTable?.rows?.length &&
      !areStoredTableRowsEqual(existingCurrentTable.rows, nextCurrentTable.rows)
    ){
      const existingSerialized = serializeStoredTable(existingCurrentTable.rows);
      const alreadySaved = nextTableVersions.some(version=>areStoredTableRowsEqual(version.rows, existingCurrentTable.rows));

      if(!alreadySaved && existingSerialized){
        nextTableVersions.unshift({
          id: uid("ref_ver"),
          createdAt: existingEntry.updatedAt || existingEntry.createdAt || now,
          rows: cloneStoredTableRows(existingCurrentTable.rows),
        });
        tableVersionCreated = true;
      }
    }

    if(nextTableVersions.length > 24){
      nextTableVersions = nextTableVersions.slice(0, 24);
    }

    entries[existingIdx] = {
      ...existingEntry,
      deptId,
      title,
      text,
      tableType,
      tableVersions: nextTableVersions,
      updatedAt: now,
    };
  } else {
    entries.unshift({
      id: uid("ref"),
      deptId,
      title,
      text,
      tableType,
      tableVersions: [],
      createdAt: now,
      updatedAt: now,
    });
  }

  STATE.referenceNotes.entries = entries;
  saveState(STATE);
  ensureCriticalStateSaved("Запис у Цікавому поки що збережений тимчасово. Дочекайся синхронізації перед оновленням сторінки.");
  hideSheet();
  showToast(tableVersionCreated ? "Запис збережено · створено нову версію таблиці" : "Запис збережено", "ok");
  render();

}

function deleteReferenceEntryNow(entryId=""){

  if(!entryId) return;

  STATE.referenceNotes = normalizeReferenceNotes(STATE.referenceNotes);
  STATE.referenceNotes.entries = (STATE.referenceNotes.entries || []).filter(x=>x && x.id!==entryId);
  saveState(STATE);
  ensureCriticalStateSaved("Запис у Цікавому поки що збережений тимчасово. Дочекайся синхронізації перед оновленням сторінки.");
  hideSheet();
  showToast("Запис видалено", "ok");
  render();

}

function viewControl(){

  if(!ensureLoggedIn()) return viewLogin();

  const u = currentSessionUser();
  const notes = normalizeReferenceNotes(STATE.referenceNotes);
  const refSearch = String(UI.refSearch || "").trim().toLowerCase();
  const deptFilter = UI.refDeptFilter || "all";

  UI.tab = ROUTES.CONTROL;

  const filterButtons = [
    {key:"all", label:"Усі"},
    {key:"general", label:"Загальні"},
    ...STATE.departments.map(dept=>({key:dept.id, label:dept.name.replace(/^Відділ\s+/,"").trim()}))
  ].map(item=>`
    <button
      type="button"
      class="btn btn-mini ref-filter-btn ${deptFilter===item.key ? "primary" : "ghost"}"
      data-action="setReferenceDeptFilter"
      data-arg1="${item.key}"
    >${htmlesc(item.label)}</button>
  `).join("");

  const entries = (notes.entries || []).filter(entry=>{
    if(!entry || !entry.text) return false;
    if(!refSearch){
      if(deptFilter === "general" && entry.deptId) return false;
      if(deptFilter !== "all" && deptFilter !== "general" && entry.deptId !== deptFilter) return false;
      return true;
    }
    const haystack = `${entry.title || ""} ${stripStoredTables(entry.text || "")} ${getReferenceEntryDeptLabel(entry.deptId)} ${getReferenceTableTypeLabel(entry.tableType)}`.toLowerCase();
    return haystack.includes(refSearch);
  });

  const entryCards = entries.map((entry, idx)=>{
    const title = entry.title || `Запис ${idx + 1}`;
    const numberedTitle = `${idx + 1}. ${title}`;
    const plainText = stripStoredTables(entry.text || "");
    const currentTable = findStoredTableBlock(entry.text || "");
    const previousTable = findPreviousStoredTableBlock(entry.text || "");
    const tableOnlyText = [currentTable?.raw || "", previousTable?.raw || ""].filter(Boolean).join("\n\n");
    const bodyHtml = refSearch ? highlightReferenceText(plainText, refSearch) : richText(plainText);
    const tableHtml = tableOnlyText
      ? renderTaskDescWithTableToggle(tableOnlyText, "Дані", {updatedAt:entry.updatedAt, showEmpty:false, className:"ref-note-body rich-text", analyticsType: entry.tableType, analyticsTitle: title, dynamicVersions: entry.tableVersions || []})
      : "";
      const tableTypeBadge = currentTable
        ? `<span class="ref-tabletype-badge"><span class="ref-tabletype-dot"></span><span>${htmlesc(getReferenceTableTypeLabel(entry.tableType))}</span></span>`
        : "";
    const isExpanded = !!refSearch || !!UI.refExpandedEntries?.[entry.id];
    const entryAttachments = (notes.attachments || []).filter(item=>item && item.url && item.entryId===entry.id);
    const attachmentList = entryAttachments.map((item, fileIdx)=>`
      <div class="ref-attachment-chip-wrap">
        <a class="ref-attachment-chip" href="${htmlesc(item.url)}" target="_blank" rel="noopener noreferrer" title="${htmlesc(item.title || `Вкладення ${fileIdx + 1}`)}">
          <span class="ref-attachment-ico">${getReferenceAttachmentIcon(item)}</span>
          <span class="ref-attachment-name">${htmlesc(item.title || `Вкладення ${fileIdx + 1}`)}</span>
        </a>
        ${u.readOnly ? "" : `<button class="btn btn-mini ghost ref-attachment-edit" data-action="openReferenceLink" data-arg1="${entry.id}" data-arg2="${item.id}" title="Редагувати вкладення">✎</button>`}
      </div>
    `).join("");
    return `
      <div class="ref-note">
        <div class="ref-note-titlebar">
          <button class="ref-note-toggle ${isExpanded ? "is-open" : ""}" data-action="toggleReferenceEntry" data-arg1="${entry.id}" title="${isExpanded ? "Згорнути опис" : "Розгорнути опис"}" aria-label="${isExpanded ? "Згорнути опис" : "Розгорнути опис"}">
            <span class="ref-note-caret"></span>
          </button>
          <button class="ref-note-link" data-action="openReferenceEntry" data-arg1="${entry.id}">${refSearch ? highlightReferenceText(numberedTitle, refSearch) : htmlesc(numberedTitle)}</button>
          ${tableTypeBadge}
        </div>
        ${isExpanded ? `
          <div class="ref-note-body rich-text">${bodyHtml || (!tableHtml ? "Без тексту" : "")}</div>
          ${tableHtml}
          <div class="ref-attachments-block">
            <div class="ref-attachments-head">
              <span>Вкладення</span>
              ${u.readOnly ? "" : `<button class="btn btn-mini ghost" data-action="openReferenceLink" data-arg1="${entry.id}">+ Додати</button>`}
            </div>
            <div class="ref-attachments-list">
              ${attachmentList || `<div class="hint">Для цього запису вкладень поки немає.</div>`}
            </div>
          </div>
        ` : ""}
      </div>
    `;
  }).join("");

  const body = `

    <div class="section-title ref-section-title">Довідка керівника</div>

    <div class="card">
      <div class="card-h">
        <div class="t">Список записів</div>
        <div class="actions">
          <span class="badge b-blue">${entries.length}</span>
          ${u.readOnly ? "" : `<button class="btn primary btn-mini" data-action="openReferenceEntry">+ Новий запис</button>`}
        </div>
      </div>
      <div class="card-b">
        <div class="ref-controls">
          <div class="ref-filterbar">${filterButtons}</div>
          <div class="field ref-search-inline">
            <input id="referenceSearch" type="search" placeholder="Пошук: наказ, НРК, контакт, штат..." value="${htmlesc(UI.refSearch || "")}" data-change="setReferenceSearchFromInput" />
          </div>
        </div>
        <div class="ref-list">
          ${entryCards || `<div class="hint">Поки немає записів для цього фільтра. Додай нотатку і прив’яжи її до відділу або лиши як загальну.</div>`}
        </div>
      </div>
    </div>

  `;

  const tabs = (u.role==="boss")
    ? [
      {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
      {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},
      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},
      {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},
      {key:ROUTES.PLAN, label:"План", ico:"📅"},
    ]
    : [
      {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},
      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},
    ];

  appShell({
    title: "Цікаве",
    subtitle: roleSubtitle(u),
    bodyHtml: body,
    showFab: false,
    fabAction: null,
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

  if(t.type==="personal"){

    ensureCriticalStateSaved("Моя задача поки що збережена тимчасово. Дочекайся синхронізації перед оновленням сторінки.");

  }

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

      {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

      {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

      {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

      {key:ROUTES.PLAN, label:"План", ico:"📅"},

    ]

    : [

      {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

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

  const pctSafe = (value, total)=> total ? Math.round((Number(value || 0) / total) * 100) : 0;



  const plans = (STATE.reportPlans || []).slice().sort((a,b)=>{

    const da = Array.isArray(a.daysOfMonth) && a.daysOfMonth.length ? Math.min(...a.daysOfMonth) : (Number(a.dayOfMonth) || 0);

    const db = Array.isArray(b.daysOfMonth) && b.daysOfMonth.length ? Math.min(...b.daysOfMonth) : (Number(b.dayOfMonth) || 0);

    if(da !== db) return da - db;

    return (a.title || "").localeCompare(b.title || "");

  });

  const activePlanIds = new Set(plans.map(p=>p.id));



  const tasksForMonth = STATE.tasks.filter(t=>t.reportPlanId && activePlanIds.has(t.reportPlanId) && t.reportMonth === monthStr);

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

  const missingPlanned = Math.max(totalPlanned - tasksForMonth.length, 0);

  const createdPct = totalPlanned ? pctSafe(tasksForMonth.length, totalPlanned) : 0;

  const donePct = totalPlanned ? pctSafe(doneInMonth, totalPlanned) : 0;



  const planList = plans.length ? plans.map((plan, planIndex)=>{

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

      <div class="reporting-plan-actions">

        <button class="btn ghost reporting-icon-btn" data-action="openReportPlanEdit" data-arg1="${plan.id}" title="Редагувати">
          <span class="reporting-btn-ico">✏️</span>
          <span class="reporting-btn-text">Редагувати</span>
        </button>

        <button class="btn ghost reporting-icon-btn" data-action="confirmDeleteReportPlan" data-arg1="${plan.id}" title="Видалити">
          <span class="reporting-btn-ico">🗑️</span>
          <span class="reporting-btn-text">Видалити</span>
        </button>

      </div>

    `;



    const monthDayList = Array.isArray(plan.daysOfMonth) ? plan.daysOfMonth : [];

    const weekDayList = Array.isArray(plan.weekDays) ? plan.weekDays : [];

    const weekLabels = ["Нд","Пн","Вт","Ср","Чт","Пт","Сб"];

    const weekLabelText = weekDayList.length ? weekDayList.map(x=>weekLabels[x] || "").filter(Boolean).join(", ") : "—";

    const dayLabelText = monthDayList.length ? monthDayList.join(", ") : "—";

    const occurrenceTotal = deptIds.length * scheduleDates.length;

    const createdLabel = occurrenceTotal ? `${planTasks.length}/${occurrenceTotal}` : `${planTasks.length}`;

    const closedLabel = occurrenceTotal ? `${closedInMonth}/${occurrenceTotal}` : `${closedInMonth}`;

    const deptSummaryHtml = deptIds.length

      ? deptIds.map(deptId=>{

          const dept = getDeptById(deptId);
          const deptTasks = scheduleDates.map(date=>taskMap.get(`${deptId}__${date}`) || null);
          const deptDone = deptTasks.length > 0 && deptTasks.every(task=>task && task.status === "закрито");
          const cls = deptDone ? "reporting-strike" : "";

          return `<span class="reporting-inline-text ${cls}">${htmlesc(deptShortLabel(dept) || dept?.name || "Відділ")}</span>`;

        }).join(`<span class="reporting-inline-sep">,</span> `)

      : `<span class="reporting-inline-text">Відділи не вибрані</span>`;

    const schedulePreviewHtml = scheduleDates.length
      ? scheduleDates.map(date=>{

          const tasksForDate = deptIds.map(deptId=>taskMap.get(`${deptId}__${date}`) || null);
          const dateDone = tasksForDate.length > 0 && tasksForDate.every(task=>task && task.status === "закрито");
          const cls = dateDone ? "reporting-strike" : "";

          return `<span class="reporting-inline-text mono ${cls}">${fmtDate(date)}</span>`;

        }).join(`<span class="reporting-inline-sep">,</span> `)

      : `<span class="reporting-inline-text mono">—</span>`;

    return `

      <div class="item reporting-plan-card reporting-plan-compact" style="cursor:default;">

        <div class="row reporting-plan-head">

          <div class="reporting-plan-title">

            <div class="name">${planIndex + 1}. ${htmlesc(plan.title || "Без назви")}</div>

          </div>

          ${actions}

        </div>

        <div class="reporting-plan-inline reporting-plan-inline-compact">
          <div class="reporting-plan-inline-row">
            <span class="reporting-inline-label">Відділи</span>
            <div class="reporting-inline-values">${deptSummaryHtml}</div>
          </div>
          <div class="reporting-plan-inline-row">
            <span class="reporting-inline-label">Графік</span>
            <div class="reporting-inline-values">${schedulePreviewHtml}</div>
          </div>
        </div>

        ${desc ? `<div class="reporting-plan-desc">${desc}</div>` : ``}

        <details class="report-details reporting-plan-toggle">
          <summary>Розклад і статуси</summary>
          <div class="reporting-plan-mini">
            ${dayLabelText !== "—" ? `Дні місяця: <span class="mono">${htmlesc(dayLabelText)}</span>` : ``}
            ${(dayLabelText !== "—" && weekLabelText !== "—") ? `<span class="reporting-mini-sep">•</span>` : ``}
            ${weekLabelText !== "—" ? `Дні тижня: <span class="mono">${htmlesc(weekLabelText)}</span>` : ``}
          </div>
          ${deptIds.length ? `<div class="list reporting-plan-list">${deptRows}</div>` : `<div class="hint">Відділи не вибрані.</div>`}
        </details>

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

        <div class="reporting-toolbar">

          <div class="field reporting-month-field">

            <label>Звітний місяць</label>

            <input type="month" id="reportMonthInput" value="${monthStr}" data-change="setReportingMonthFromInput" />

          </div>

          <div class="report-grid reporting-summary-grid">

            <div class="report-tile">

              <div class="k">План</div>

              <div class="v">${totalPlanned}</div>

              <div class="s">подій у ${htmlesc(monthStr)}</div>

            </div>

            <div class="report-tile">

              <div class="k">Створено</div>

              <div class="v">${tasksForMonth.length}</div>

              <div class="s">${totalPlanned ? `${createdPct}% від плану` : "без плану"}</div>

            </div>

            <div class="report-tile">

              <div class="k">Закрито</div>

              <div class="v">${doneInMonth}</div>

              <div class="s">${missingPlanned ? `ще ${missingPlanned} не створено` : (totalPlanned ? `${donePct}% від плану` : "—")}</div>

            </div>

          </div>

        </div>



        <div class="list">

          ${planList}

        </div>

      </div>

    </div>

  `;



  const tabs = [

    {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

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

                <span class="pill cal-pill cal-pill-total"><span class="cal-pill-label">Дедл.</span><span class="mono">${stats.total}</span></span>

                ${stats.overdue ? `<span class="badge b-warn cal-badge"><span class="cal-badge-ico">🟠</span><span class="mono">${stats.overdue}</span></span>` : ``}

              </div>

            `;

          }

          return `

            <div class="cal-badges">

              <span class="pill cal-pill cal-pill-total"><span class="cal-pill-label">План</span><span class="mono">${stats.total}</span></span>

              ${stats.done ? `<span class="badge b-ok cal-badge"><span class="cal-badge-ico">✅</span><span class="mono">${stats.done}</span></span>` : ``}

              ${stats.missing ? `<span class="badge b-danger cal-badge"><span class="cal-badge-ico">⚠️</span><span class="mono">${stats.missing}</span></span>` : ``}

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

              <div class="chip plan-mode-chip ${mode==="reporting"?"active":""}" data-action="setPlanMode" data-arg1="reporting">
                <span class="plan-mode-ico">📑</span>
                <span class="plan-mode-text">Звітність</span>
              </div>

              <div class="chip plan-mode-chip ${mode==="tasks"?"active":""}" data-action="setPlanMode" data-arg1="tasks">
                <span class="plan-mode-ico">📋</span>
                <span class="plan-mode-text">Усі задачі</span>
              </div>

            </div>

          </div>

        </div>

        ${grid}

      </div>

    </div>

  `;



  const tabs = [

    {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

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

    {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

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

  if(Array.isArray(STATE.taskEvaluations)){

    STATE.taskEvaluations = STATE.taskEvaluations.filter(x=>!duplicateIds.has(x.taskId));

  }

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

      tasks = tasks.filter(t=>t.type==="personal" || t.status==="на_перевірці");

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

    const isOnReview = t.status==="на_перевірці";

    const isVerified = t.status==="перевірено";

    const reviewMark = isOnReview

      ? `<span class="task-context-pill review" title="На перевірці">🔎 Перевірка</span>`

      : ``;

    const verifiedMark = isVerified

      ? `<span class="task-context-pill subtle" title="Перевірено">👁</span>`

      : ``;

    const hideStatus = isAnn || isDone || (t.status==="в_процесі" && !t.dueDate && (t.controlAlways || t.nextControlDate));

    const descRaw = (t.description || "");

    const hasDesc = descRaw.trim().length > 0;

    const descStartsWithBreak = /^\s*(?:\r?\n)/.test(descRaw);

    const descLabel = isAnn ? "Текст" : "Опис";

    const descHtml = (!isAnn && hasDesc) ? renderTaskDescWithTableToggle(descRaw, descLabel, {updatedAt: t.updatedAt || t.createdAt || ""}) : "";

    const annDesc = (isAnn && t.audience==="meeting" && hasDesc) ? `<div class="task-desc rich-text">Опис:${descStartsWithBreak ? "<br/>" : " "}${richText(descRaw)}</div>` : "";

    const contextHtml = isAnn

      ? `<div class="task-context"><span class="task-context-pill">${htmlesc(annLabel || "Оголошення")}</span><span class="task-context-code mono">${htmlesc(t.id || "")}</span></div>`

      : `<div class="task-context"><span class="task-context-pill">${htmlesc(deptName)}</span>${reviewMark}${respName !== "—" ? `<span class="task-context-pill subtle">${htmlesc(respName)}</span>` : ``}${verifiedMark}<span class="task-context-code mono">${htmlesc(t.id || "")}</span></div>`;

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

  const renderTaskSection = (title, items, emptyText, startIdx=0, doneItems=[])=>{

    const bodyHtml = items.length

      ? items.map((t,i)=>renderTaskItem(t, startIdx + i)).join("")

      : `<div class="hint">${emptyText}</div>`;

    const doneBlock = (showDoneToggle && doneItems.length)

      ? renderDoneToggle(doneItems, startIdx + items.length)

      : "";

    return `

      <div class="task-subsection">

        <div class="section-title">${title} <span class="mono">${items.length}</span></div>

        ${bodyHtml}

        ${doneBlock}

      </div>

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

    } else if(isPersonalScope && effectivePersonalFilter==="tasks"){

      const reviewItems = filtered.filter(t=>t.status==="на_перевірці");

      const personalItems = filtered.filter(t=>t.type==="personal" && t.status!=="на_перевірці");

      const doneItems = showDoneToggle

        ? completed.filter(t=>t.type==="personal")

        : [];

      const sections = [];

      sections.push(renderTaskSection("На перевірці", reviewItems, "Немає задач на перевірці."));

      sections.push(renderTaskSection("Особисті", personalItems, "Немає особистих задач.", 0, doneItems));

      tasksList = sections.join("");

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

      {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

      {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

      {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

      {key:ROUTES.PLAN, label:"План", ico:"📅"},

    ]

    : [

      {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

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

  const isOnReview = (t.status==="на_перевірці");

  const blockerBtn = isBlocked

    ? `<button class="btn warn" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="в_процесі">🔓 Розблок</button>`

    : `<button class="btn warn" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="блокер">⛔ Блокер</button>`;



  if(isBoss){

    if(isOnReview){

      btns.push(`<button class="btn violet" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="перевірено">↩ Повернути</button>`);

    } else if(t.type==="managerial" && t.status==="очікує_підтвердження"){

      btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Підтвердити</button>`);

    } else {

      btns.push(`<button class="btn ok" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="закрито">✅ Закрити</button>`);

    }

    if(!isOnReview){

      btns.push(`<button class="btn violet" data-action="setTaskStatus" data-arg1="${t.id}" data-arg2="на_перевірці">🔎 На перевірку</button>`);

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

  const isReviewing = status === "на_перевірці";

  const isVerified = status === "перевірено";

  const title = isBlocking

    ? "Блокер: вкажи причину"

    : (isClosing

      ? "Закриття: результат"

      : (isReviewing

        ? "На перевірку: короткий коментар"

        : (isVerified ? "Перевірено: короткий висновок" : "Розблокування: причина")));

  const label = isBlocking

    ? "Причина блокера"

    : (isClosing

      ? "Результат / причина закриття"

      : (isReviewing

        ? "Що саме треба перевірити"

        : (isVerified ? "Що перевірено / який висновок" : "Причина розблокування")));

  const hint = isBlocking

    ? "Опиши, що заважає або кого/чого очікуємо."

    : (isClosing

      ? "Коротко: що зроблено або який результат."

      : (isReviewing

        ? "Коротко вкажи, що саме треба подивитись або перевірити."

        : (isVerified ? "Коротко зафіксуй результат перевірки." : "Що змінилося і чому можна рухатись далі.")));

  const placeholder = isBlocking

    ? "Наприклад: немає доступу / чекаємо підтвердження / бракує ресурсу."

    : (isClosing

      ? "Наприклад: виконано повністю / передано результат / підтверджено."

      : (isReviewing

        ? "Наприклад: перевірити комплектність / звірити дані / переглянути документ."

        : (isVerified ? "Наприклад: перевірено, можна продовжувати / зауважень немає." : "Наприклад: отримали доступ / підтвердили рішення / ресурс з’явився.")));



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

  if((status==="на_перевірці" || status==="перевірено") && !isBoss){

    showSheet("Немає прав", `<div class="hint">Перевірку поки що запускає і повертає лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

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

  } else if(status==="на_перевірці"){

    note = `На перевірку: ${reason}`;

  } else if(status==="перевірено"){

    note = `Перевірено: ${reason}`;

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

  if((status==="на_перевірці" || status==="перевірено") && u.role!=="boss"){

    showSheet("Немає прав", `<div class="hint">Перевірку поки що запускає і повертає лише керівник.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(u.role!=="boss" && t.type==="managerial" && status==="закрито"){

    showSheet("Обмеження", `<div class="hint">Управлінську задачу закриває тільки керівник. Використайте “Запит закриття”.</div><div class="sep"></div><button class="btn primary" data-action="hideSheet">OK</button>`);

    return;

  }

  if(status==="закрито" || status==="на_перевірці" || status==="перевірено"){

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

      ${isAnn ? `` : renderTaskDescWithTableToggle(t.description || "", descLabel, {className:"hint rich-text", showEmpty:true, updatedAt: t.updatedAt || t.createdAt || ""})}

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



  const metaBlock = `

    <div class="task-meta-right">

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

    </div>

  `;

  const deptBlock = !isPersonal ? (t.type==="managerial" && isBoss ? `

    <div class="task-meta-grid">

      <div class="task-meta-left">

        <div class="field">

          <label>Відділи</label>

          <div class="dept-toggle-grid">

            ${deptOptions.map(d=>`

              <label class="dept-toggle">

                <span class="dept-name">${htmlesc(d.name)}</span>

                <span class="switch">

                  <input type="checkbox" name="tDeptMulti" value="${d.id}" data-change="selectSingleDeptToggleFromInput" ${d.id===deptId ? "checked" : ""} />

                  <span class="slider"></span>

                </span>

              </label>

            `).join("")}

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

            <select id="tDept" data-change="refreshRespOptions" ${isBoss ? "" : "disabled"}>

              ${deptOptions.map(d=>`<option value="${d.id}" ${d.id===deptId ? "selected" : ""}>${htmlesc(d.name)}</option>`).join("")}

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

  `) : metaBlock;

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

        ${formatToolbar("tDesc", "inline", {table:true})}

      </div>

      <textarea id="tDesc" class="task-desc-input" placeholder="Деталі / очікуваний результат">${htmlesc(stripStoredTables(t.description || ""))}</textarea>

    </div>

    ${deptBlock}

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

  initDescriptionTableState("tDesc", t.description || "");

  const existingTable = findStoredTableBlock(t.description || "");

  if(existingTable) renderTextTableEditor("tDesc", existingTable.rows);

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



  if(document.querySelector('.text-table-editor[data-for="tDesc"]')){
    writeTextTableToTextarea("tDesc", readTextTableEditorRows("tDesc"));
  }

  const desc = buildDescriptionValueFromEditor("tDesc");

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

    const multiToggles = [...document.querySelectorAll('input[name="tDeptMulti"]')];
    const deptSel = document.getElementById("tDept");

    if(isBoss && multiToggles.length){
      departmentId = multiToggles.find(x=>x.checked)?.value || departmentId;
    } else if(isBoss && deptSel) departmentId = deptSel.value;

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

function selectSingleDeptToggleFromInput(){

  const toggles = [...document.querySelectorAll('input[name="tDeptMulti"]')];
  if(!toggles.length) return;

  const active = document.activeElement;
  const changed = toggles.find(x=>x===active) || toggles.find(x=>x.checked);
  if(!changed) return;

  if(changed.checked){
    toggles.forEach(x=>{ if(x !== changed) x.checked = false; });
  } else if(!toggles.some(x=>x.checked)){
    changed.checked = true;
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

        ${formatToolbar("tDesc", "inline", {table:true})}

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

  initDescriptionTableState("tDesc", "");

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

  if(document.querySelector('.text-table-editor[data-for="tDesc"]')){
    writeTextTableToTextarea("tDesc", readTextTableEditorRows("tDesc"));
  }

  const desc = buildDescriptionValueFromEditor("tDesc");

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

  const existingTable = findStoredTableBlock(t.description || "");

  if(existingTable) renderTextTableEditor("tDesc", existingTable.rows);

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

      {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

      {key:ROUTES.TASKS, label:"Задачі", ico:"📋"},

      {key:ROUTES.WEEKLY, label:"Тиждень", ico:"🗓"},

      {key:ROUTES.ANALYTICS, label:"Аналітика", ico:"📈"},

      {key:ROUTES.REPORTING, label:"Звітність", ico:"📑"},

      {key:ROUTES.PLAN, label:"План", ico:"📅"},

    ]

    : [

      {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},

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

   TASK EVALUATION

=========================== */

function setAnalyticsEvalPeriod(period){

  UI.analyticsEvalPeriod = ["week","month","quarter","all"].includes(period) ? period : "month";

  render();

}

function setAnalyticsEvalDeptFilterFromInput(){

  const el = document.getElementById("analyticsDeptFilter");

  UI.analyticsEvalDeptFilter = el?.value || "all";

  render();

}

function setAnalyticsEvalUserFilterFromInput(){

  const el = document.getElementById("analyticsUserFilter");

  UI.analyticsEvalUserFilter = el?.value || "all";

  render();

}

function setAnalyticsEvalStatusFilterFromInput(){

  const el = document.getElementById("analyticsStatusFilter");

  UI.analyticsEvalStatusFilter = el?.value || "pending";

  render();

}

function setAnalyticsEvalTypeFilterFromInput(){

  const el = document.getElementById("analyticsTypeFilter");

  UI.analyticsEvalTypeFilter = el?.value || "all";

  render();

}

function setAnalyticsEvalPresetFilterFromInput(){

  const el = document.getElementById("analyticsPresetFilter");

  UI.analyticsEvalPresetFilter = el?.value || "all";

  render();

}

function setEvaluationStartDateFromInput(){

  const value = (document.getElementById("evaluationStartDate")?.value || "").trim();

  STATE.evaluationStartDate = value || kyivDateStr();

  saveState(STATE);
  render();

}

function applyTaskEvaluationPresetFromInput(){

  const select = document.getElementById("eval_preset");
  const presetKey = select?.value || "";
  const preset = TASK_EVAL_PRESETS.find(x=>x.key===presetKey);

  if(!preset){
    showToast("Оберіть тип задачі.", "warn");
    return;
  }

  TASK_EVAL_CRITERIA.forEach(item=>{
    const el = document.getElementById(`eval_${item.key}`);
    if(!el) return;
    el.value = String(Number(preset.scores[item.key] || 3));
  });

  showToast(`Підставлено тип задачі: ${preset.label}`, "ok");

}

function openTaskEvaluation(taskId){

  const u = currentSessionUser();
  const task = STATE.tasks.find(t=>t.id===taskId);

  if(!u || u.role!=="boss" || !task) return;

  const evaluation = getTaskEvaluation(taskId) || {};
  const dept = task.departmentId ? getDeptById(task.departmentId)?.name || "Відділ" : "Особисто";
  const closeDate = getCloseDateForTask(task);
  const guessedPresetKey = evaluation.presetKey || guessTaskEvaluationPreset(task);
  const guessedPreset = TASK_EVAL_PRESETS.find(x=>x.key===guessedPresetKey) || null;
  const initialScores = Object.keys(evaluation).length ? evaluation : (guessedPreset?.scores || {});
  const presetOptions = TASK_EVAL_PRESETS.map(item=>`<option value="${item.key}" ${item.key===guessedPresetKey ? "selected" : ""}>${htmlesc(item.label)}</option>`).join("");

  const fields = TASK_EVAL_CRITERIA.map(item=>`

    <div class="field">
      <label>${item.label}</label>
      <select id="eval_${item.key}">
        ${[1,2,3,4,5].map(score=>`<option value="${score}" ${Number(initialScores[item.key] || 0)===score ? "selected" : ""}>${score}</option>`).join("")}
      </select>
      <div class="hint" style="margin-top:6px;">${TASK_EVAL_HINTS[item.key] || ""}</div>
    </div>

  `).join("");

  showSheet(`Оцінка задачі`, `

    <div class="hint">
      <b>${htmlesc(task.title || "Без назви")}</b><br/>
      ${htmlesc(dept)}${closeDate ? ` • закрито ${fmtDate(closeDate)}` : ""}
    </div>
    <div class="sep"></div>
    <div class="field">
      <label>Тип задачі</label>
      <div class="row2">
        <select id="eval_preset">
          <option value="">Оберіть тип задачі…</option>
          ${presetOptions}
        </select>
        <button class="btn ghost" data-action="applyTaskEvaluationPresetFromInput">Підставити</button>
      </div>
      <div class="hint" style="margin-top:6px;">
        ${guessedPreset ? `Рекомендовано за назвою задачі: <b>${htmlesc(guessedPreset.label)}</b>.<br/>` : ""}
        Тип задачі лише підставляє стартові бали й дає діапазон-підказку. Після цього ти можеш спокійно скоригувати оцінку вручну.
      </div>
    </div>
    ${renderTaskEvaluationPresetGuide(guessedPreset)}
    <div class="sep"></div>
    <div class="eval-form-grid">
      ${fields}
    </div>
    <div class="field" style="margin-top:10px;">
      <label>Коментар (опційно)</label>
      <textarea id="eval_note" placeholder="Коротка примітка до оцінки">${htmlesc(evaluation.note || "")}</textarea>
    </div>
    <div class="item" style="cursor:default; margin-top:10px;">
      <div class="name">Міні-шпаргалка 1 / 3 / 5</div>
      <div class="hint">
        <b>1</b> — локально, швидко, без значного впливу.<br/>
        <b>3</b> — нормальний робочий рівень, повноцінна службова задача.<br/>
        <b>5</b> — дійсно вагома задача: велика, термінова, важлива або з сильним результатом.
      </div>
    </div>
    <div class="actions" style="margin-top:14px;">
      <button class="btn ghost" data-action="openTaskEvaluationHelp">Довідка</button>
      <button class="btn primary" data-action="saveTaskEvaluationNow" data-arg1="${task.id}">Зберегти оцінку</button>
      <button class="btn ghost" data-action="hideSheet">Скасувати</button>
    </div>

  `);

}

function openTaskEvaluationHelp(){

  showSheet("Довідка: як оцінювати задачі", `

    <div class="item" style="cursor:default;">
      <div class="name">Для чого ця оцінка</div>
      <div class="hint">
        Оцінка потрібна не для “враження”, а для більш чесної аналітики по відділах і задачах.
        Ми оцінюємо <b>вже закриту</b> задачу спокійно, після виконання, коли видно реальний обсяг роботи, важливість і результат.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Головний принцип</div>
      <div class="hint">
        Не оцінюй задачу “по симпатії” або “бо довго обговорювали на нараді”.
        Оцінюй лише по факту: скільки було роботи, наскільки вона була важлива, наскільки горіла по часу і який дала результат.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">1. Трудомісткість</div>
      <div class="hint">
        Це про <b>реальний обсяг зусиль</b>, а не про важливість.<br/><br/>
        <b>1 бал</b> — коротка проста дія: уточнення, дзвінок, пересилання, короткий документ, швидке погодження.<br/>
        <b>3 бали</b> — помірний обсяг: треба було зібрати дані, узгодити, підготувати матеріал, зробити кілька кроків.<br/>
        <b>5 балів</b> — значний обсяг: багато координації, кілька виконавців, складна підготовка, тривала робота або великий пакет матеріалів.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">2. Важливість</div>
      <div class="hint">
        Це про <b>значення задачі для підрозділу або керівництва</b>.<br/><br/>
        <b>1 бал</b> — локальне, рутинне, без суттєвого впливу.<br/>
        <b>3 бали</b> — важливо для роботи відділу, впливає на процес або строки, але без критичних наслідків.<br/>
        <b>5 балів</b> — стратегічно важливо, впливає на кілька відділів, керівництво, перевірки, випробування, безпеку або репутацію.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">3. Терміновість</div>
      <div class="hint">
        Це про <b>тиск по часу</b>.<br/><br/>
        <b>1 бал</b> — спокійний режим, був нормальний запас часу.<br/>
        <b>3 бали</b> — задачу треба було зробити в строк, без великого запасу, але без авралу.<br/>
        <b>5 балів</b> — дуже терміново: сьогодні на сьогодні, жорсткий дедлайн, нарада, перевірка, негайне рішення.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">4. Результат</div>
      <div class="hint">
        Це про <b>користь і завершеність результату</b>.<br/><br/>
        <b>1 бал</b> — мінімальний результат: часткове закриття питання, базова відповідь, проміжний крок.<br/>
        <b>3 бали</b> — повний нормальний результат: задачу закрито, очікуваний результат отримано.<br/>
        <b>5 балів</b> — сильний результат: повністю закрито проблему, є відчутний ефект, економія часу, зняття ризику або якісне управлінське рішення.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Як оцінювати без помилки</div>
      <div class="hint">
        1. Спочатку подумай, скільки реально було роботи — це <b>трудомісткість</b>.<br/>
        2. Потім окремо подумай, наскільки ця задача була важлива — це <b>важливість</b>.<br/>
        3. Окремо оцінюй, чи був часовий тиск — це <b>терміновість</b>.<br/>
        4. І лише потім оцінюй, який фактичний ефект дала задача — це <b>результат</b>.<br/><br/>
        Якщо сумніваєшся між двома балами — став нижчий, якщо немає явних підстав для вищого.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Приклад 1</div>
      <div class="hint">
        <b>Коротке погодження листа або уточнення інформації</b><br/>
        Трудомісткість: 1<br/>
        Важливість: 2<br/>
        Терміновість: 2<br/>
        Результат: 2<br/><br/>
        Логіка: зроблено швидко, задача не дуже велика, але мала практичну користь.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Приклад 2</div>
      <div class="hint">
        <b>Підготовка порівняльних таблиць по кількох напрямках для наради</b><br/>
        Трудомісткість: 4<br/>
        Важливість: 4<br/>
        Терміновість: 4<br/>
        Результат: 4<br/><br/>
        Логіка: багато підготовки, важливо для прийняття рішення, дедлайн жорсткий, результат повноцінний.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Приклад 3</div>
      <div class="hint">
        <b>Організація або супровід випробувань / перевірки з кількома відділами</b><br/>
        Трудомісткість: 5<br/>
        Важливість: 5<br/>
        Терміновість: 4 або 5<br/>
        Результат: 4 або 5<br/><br/>
        Логіка: висока координація, значимість для керівництва, серйозний вплив на роботу і результат.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Коротка пам’ятка для керівника</div>
      <div class="hint">
        <b>1–2</b> — невелика або локальна задача.<br/>
        <b>3</b> — нормальний робочий рівень.<br/>
        <b>4</b> — сильна, важлива або трудомістка задача.<br/>
        <b>5</b> — реально вагома задача з великим впливом, складністю або терміновістю.
      </div>
    </div>

    <div class="item" style="cursor:default;">
      <div class="name">Типові задачі і який тип обирати</div>
      <div class="hint">
        <b>Лист / уточнення / записка</b> — лист на нацпол, лист на виробників, уточнення по наявності, службова записка.<br/>
        <b>Таблиця / довідка / слайди</b> — порівняльні таблиці, довідки, слайди, аналітичні матеріали.<br/>
        <b>Зовнішня взаємодія / зустріч</b> — виробники, компанії, НГУ, зовнішні органи, координаційні зустрічі.<br/>
        <b>Допуск / декларація / оформлення</b> — допуски, перепустки, декларації, оформлення документів.<br/>
        <b>Організація участі / навчання / заходу</b> — навчання, демонстрації, участь у заходах, супровід випробувань.<br/>
        <b>АРМ / доступ / токен / налаштування</b> — робоче місце, АРМ, доступи, токени, технічні налаштування.<br/>
        <b>Майно / закупівлі / договір / контроль</b> — майно, закупівлі, договори, контроль підписання, забезпечення.
      </div>
    </div>

    <div class="sep"></div>
    <div class="actions">
      <button class="btn primary" data-action="hideSheet">Зрозуміло</button>
    </div>

  `, { stack:true });

}

function saveTaskEvaluationNow(taskId){

  const u = currentSessionUser();
  const task = STATE.tasks.find(t=>t.id===taskId);

  if(!u || u.role!=="boss" || !task || task.status!=="закрито"){

    showToast("Оцінювати можна лише закриті задачі.", "warn");

    return;

  }

  const payload = {
    id: `eval_${taskId}`,
    taskId,
    evaluatedAt: nowIsoKyiv(),
    evaluatedBy: u.id,
    note: (document.getElementById("eval_note")?.value || "").trim(),
  };
  const selectedPresetKey = document.getElementById("eval_preset")?.value || "";
  payload.presetKey = TASK_EVAL_PRESETS.some(x=>x.key===selectedPresetKey)
    ? selectedPresetKey
    : (guessTaskEvaluationPreset(task) || null);

  let hasError = false;

  TASK_EVAL_CRITERIA.forEach(item=>{

    const value = Number(document.getElementById(`eval_${item.key}`)?.value || 0);

    if(value < 1 || value > 5) hasError = true;

    payload[item.key] = value;

  });

  if(hasError){

    showToast("Постав оцінки від 1 до 5 по кожному критерію.", "warn");

    return;

  }

  payload.total = evalTotalScore(payload);

  if(!Array.isArray(STATE.taskEvaluations)) STATE.taskEvaluations = [];

  const existingIdx = STATE.taskEvaluations.findIndex(x=>x && x.taskId===taskId);

  if(existingIdx >= 0){

    STATE.taskEvaluations[existingIdx] = {...STATE.taskEvaluations[existingIdx], ...payload};

  } else {

    STATE.taskEvaluations.push(payload);

  }

  saveState(STATE);
  hideSheet();
  showToast("Оцінку збережено", "ok");
  render();

}



/* ===========================

   ANALYTICS VIEW (demo)

=========================== */

function viewAnalytics(){

  const u = currentSessionUser();

  if(!u || u.role!=="boss"){
    UI.tab = ROUTES.CONTROL;
    return viewControl();
  }

  UI.tab = ROUTES.ANALYTICS;

  const tasksAll = STATE.tasks.filter(t=>!isAnnouncement(t));
  const period = analyticsEvalPeriodRange(UI.analyticsEvalPeriod);
  const evaluationStartDate = String(STATE.evaluationStartDate || kyivDateStr());

  const closedTasks = tasksAll.filter(t=>{
    if(t.status!=="закрито") return false;
    const closeDate = getCloseDateForTask(t);
    if(!closeDate) return false;
    if(period.from && closeDate < period.from) return false;
    if(period.to && closeDate > period.to) return false;
    return true;
  });

  const deptOptions = STATE.departments.filter(d=>closedTasks.some(t=>t.departmentId===d.id));
  const userIds = Array.from(new Set(closedTasks.map(t=>t.responsibleUserId).filter(Boolean)));
  const userOptions = userIds.map(getUserById).filter(Boolean);
  const presetOptions = TASK_EVAL_PRESETS.filter(item=>closedTasks.some(t=>getTaskEvaluationPreset(t, getTaskEvaluation(t))?.key === item.key));

  const filteredClosed = closedTasks.filter(t=>{
    if(UI.analyticsEvalDeptFilter !== "all" && (t.departmentId || "") !== UI.analyticsEvalDeptFilter) return false;
    if(UI.analyticsEvalUserFilter !== "all" && (t.responsibleUserId || "") !== UI.analyticsEvalUserFilter) return false;
    if(UI.analyticsEvalTypeFilter !== "all" && (t.type || "") !== UI.analyticsEvalTypeFilter) return false;

    const evaluation = getTaskEvaluation(t.id);
    const preset = getTaskEvaluationPreset(t, evaluation);
    const closeDate = getCloseDateForTask(t) || "";
    const needsEvaluationNow = !evaluation && !!closeDate && closeDate >= evaluationStartDate;

    if(UI.analyticsEvalPresetFilter !== "all" && (preset?.key || "") !== UI.analyticsEvalPresetFilter) return false;

    if(UI.analyticsEvalStatusFilter === "pending" && !needsEvaluationNow) return false;
    if(UI.analyticsEvalStatusFilter === "evaluated" && !evaluation) return false;

    return true;
  }).map(task=>( {
    task,
    evaluation: getTaskEvaluation(task.id),
    closeDate: getCloseDateForTask(task),
    preset: getTaskEvaluationPreset(task, getTaskEvaluation(task.id)),
  }));

  const evaluatedRows = filteredClosed.filter(x=>x.evaluation);
  const pendingRows = filteredClosed.filter(x=>!x.evaluation && x.closeDate && x.closeDate >= evaluationStartDate);
  const totalScore = evaluatedRows.reduce((sum, row)=>sum + evalTotalScore(row.evaluation), 0);
  const avgScore = evaluatedRows.length ? (totalScore / evaluatedRows.length).toFixed(1) : "—";

  const criteriaRows = TASK_EVAL_CRITERIA.map(item=>{
    const avg = evaluatedRows.length
      ? (evaluatedRows.reduce((sum, row)=>sum + Number(row.evaluation[item.key] || 0), 0) / evaluatedRows.length)
      : 0;
    return {...item, avg, value: Math.round(avg * 20)};
  });

  const deptScoreRows = STATE.departments.map(dept=>{
    const rows = evaluatedRows.filter(x=>x.task.departmentId===dept.id);
    const score = rows.reduce((sum, row)=>sum + evalTotalScore(row.evaluation), 0);
    const avg = rows.length ? (score / rows.length) : 0;
    return {dept, count: rows.length, score, avg};
  }).filter(x=>x.count > 0).sort((a,b)=>b.score-a.score);

  const pendingDeptRows = STATE.departments.map(dept=>{
    const rows = pendingRows.filter(x=>x.task.departmentId===dept.id);
    return {dept, count: rows.length};
  }).filter(x=>x.count > 0).sort((a,b)=>b.count-a.count);

  const userScoreRows = userOptions.map(user=>{
    const rows = evaluatedRows.filter(x=>(x.task.responsibleUserId || "")===user.id);
    const score = rows.reduce((sum, row)=>sum + evalTotalScore(row.evaluation), 0);
    const avg = rows.length ? (score / rows.length) : 0;
    return {user, count: rows.length, score, avg};
  }).filter(x=>x.count > 0).sort((a,b)=>b.score-a.score);

  const presetScoreRows = TASK_EVAL_PRESETS.map(preset=>{
    const rows = evaluatedRows.filter(x=>(x.preset?.key || "")===preset.key);
    const score = rows.reduce((sum, row)=>sum + evalTotalScore(row.evaluation), 0);
    const avg = rows.length ? (score / rows.length) : 0;
    return {preset, count: rows.length, score, avg};
  }).filter(x=>x.count > 0).sort((a,b)=>b.score-a.score);

  const recentEvaluatedRows = evaluatedRows.slice().sort((a,b)=>(b.evaluation.evaluatedAt || "").localeCompare(a.evaluation.evaluatedAt || "")).slice(0,8);

  const statusDonut = buildEvalSlices([
    {label:"Оцінено", value:evaluatedRows.length},
    {label:"Не оцінено", value:pendingRows.length},
  ], ["#5f8ef5", "#ffcc66"]);

  const deptDonut = buildEvalSlices(
    deptScoreRows.slice(0,5).map(row=>({label: deptShortLabel(row.dept), value: row.score})),
    ["#5f8ef5", "#6fbf73", "#ff9f43", "#b783ff", "#ff6b8b"]
  );

  const maxDeptScore = Math.max(1, ...deptScoreRows.map(x=>x.score), 1);
  const maxCriteria = Math.max(1, ...criteriaRows.map(x=>x.value), 1);
  const maxUserScore = Math.max(1, ...userScoreRows.map(x=>x.score), 1);
  const maxPresetScore = Math.max(1, ...presetScoreRows.map(x=>x.score), 1);
  const topDept = deptScoreRows[0] || null;
  const topPendingDept = pendingDeptRows[0] || null;

  const filterBar = `
    <div class="eval-toolbar">
      <div class="eval-period-chips">
        <button class="chip ${UI.analyticsEvalPeriod==="week" ? "active" : ""}" data-action="setAnalyticsEvalPeriod" data-arg1="week">7 днів</button>
        <button class="chip ${UI.analyticsEvalPeriod==="month" ? "active" : ""}" data-action="setAnalyticsEvalPeriod" data-arg1="month">Місяць</button>
        <button class="chip ${UI.analyticsEvalPeriod==="quarter" ? "active" : ""}" data-action="setAnalyticsEvalPeriod" data-arg1="quarter">90 днів</button>
        <button class="chip ${UI.analyticsEvalPeriod==="all" ? "active" : ""}" data-action="setAnalyticsEvalPeriod" data-arg1="all">Усе</button>
      </div>
      <div class="eval-filter-grid">
        <div class="field eval-filter-start">
          <label>Старт оцінювання</label>
          <input type="date" id="evaluationStartDate" value="${htmlesc(evaluationStartDate)}" data-change="setEvaluationStartDateFromInput" />
        </div>
        <div class="field">
          <label>Відділ</label>
          <select id="analyticsDeptFilter" data-change="setAnalyticsEvalDeptFilterFromInput">
            <option value="all">Усі відділи</option>
            ${deptOptions.map(d=>`<option value="${d.id}" ${UI.analyticsEvalDeptFilter===d.id ? "selected" : ""}>${htmlesc(d.name)}</option>`).join("")}
          </select>
        </div>
        <div class="field eval-filter-user">
          <label>Виконавець</label>
          <select id="analyticsUserFilter" data-change="setAnalyticsEvalUserFilterFromInput">
            <option value="all">Усі виконавці</option>
            ${userOptions.map(user=>`<option value="${user.id}" ${UI.analyticsEvalUserFilter===user.id ? "selected" : ""}>${htmlesc(user.name)}</option>`).join("")}
          </select>
        </div>
        <div class="field">
          <label>Статус оцінки</label>
          <select id="analyticsStatusFilter" data-change="setAnalyticsEvalStatusFilterFromInput">
            <option value="pending" ${UI.analyticsEvalStatusFilter==="pending" ? "selected" : ""}>Не оцінені</option>
            <option value="evaluated" ${UI.analyticsEvalStatusFilter==="evaluated" ? "selected" : ""}>Оцінені</option>
            <option value="all" ${UI.analyticsEvalStatusFilter==="all" ? "selected" : ""}>Усі</option>
          </select>
        </div>
        <div class="field">
          <label>Тип задачі</label>
          <select id="analyticsTypeFilter" data-change="setAnalyticsEvalTypeFilterFromInput">
            <option value="all">Усі типи</option>
            ${["managerial","internal","personal"].map(type=>`<option value="${type}" ${UI.analyticsEvalTypeFilter===type ? "selected" : ""}>${htmlesc(taskTypeLabel(type))}</option>`).join("")}
          </select>
        </div>
        <div class="field eval-filter-preset">
          <label>Тип задачі</label>
          <select id="analyticsPresetFilter" data-change="setAnalyticsEvalPresetFilterFromInput">
            <option value="all">Усі типи</option>
            ${presetOptions.map(item=>`<option value="${item.key}" ${UI.analyticsEvalPresetFilter===item.key ? "selected" : ""}>${htmlesc(item.label)}</option>`).join("")}
          </select>
        </div>
      </div>
    </div>
  `;

  const kpis = `
    <div class="report-grid eval-kpi-grid">
      <div class="report-tile"><div class="k">Закрито задач</div><div class="v">${filteredClosed.length}</div><div class="s">${htmlesc(period.label)}</div></div>
      <div class="report-tile"><div class="k">Оцінено</div><div class="v">${evaluatedRows.length}</div><div class="s">${filteredClosed.length ? `${Math.round((evaluatedRows.length/filteredClosed.length)*100)}%` : "—"}</div></div>
      <div class="report-tile"><div class="k">Не оцінено</div><div class="v">${pendingRows.length}</div><div class="s">чекають оцінки</div></div>
      <div class="report-tile"><div class="k">Середній бал</div><div class="v">${avgScore}</div><div class="s">із 20 можливих</div></div>
      <div class="report-tile"><div class="k">Сума балів</div><div class="v">${totalScore}</div><div class="s">по оцінених задачах</div></div>
    </div>
  `;

  const hero = `
    <div class="eval-hero">
      <div class="eval-hero-card">
        <div class="eval-hero-eyebrow">Оцінювання після закриття</div>
        <div class="eval-hero-title">
          <span class="eval-hero-title-full">Керівницький дашборд якості виконання</span>
          <span class="eval-hero-title-short">Дашборд</span>
        </div>
        <div class="eval-hero-sub">
          Період: <span class="mono">${htmlesc(period.label)}</span>
          • Старт оцінювання: <span class="mono">${fmtDate(evaluationStartDate)}</span>
          ${topDept ? ` • Лідер: <b>${htmlesc(topDept.dept.name)}</b>` : ``}
        </div>
        <div class="eval-hero-metrics">
          <div class="eval-hero-metric">
            <div class="k">Закрито</div>
            <div class="v mono">${filteredClosed.length}</div>
          </div>
          <div class="eval-hero-metric">
            <div class="k">Середній бал</div>
            <div class="v mono">${avgScore}</div>
          </div>
          <div class="eval-hero-metric">
            <div class="k">Не оцінено</div>
            <div class="v mono">${pendingRows.length}</div>
          </div>
        </div>
      </div>
      <div class="eval-hero-side">
        <div class="eval-signal-card">
          <div class="k">Лідер відділів</div>
          <div class="v">${topDept ? htmlesc(topDept.dept.name) : "—"}</div>
          <div class="s">${topDept ? `${topDept.score} балів • ${topDept.count} задач` : "Поки немає оцінених задач"}</div>
        </div>
        <div class="eval-signal-card warn">
          <div class="k">Потребують уваги</div>
          <div class="v">${topPendingDept ? htmlesc(topPendingDept.dept.name) : "—"}</div>
          <div class="s">${topPendingDept ? `${topPendingDept.count} неоцінених задач` : "Усі закриті задачі оцінені"}</div>
        </div>
      </div>
    </div>
  `;

  const statusDonutCard = `
    <div class="eval-donut-grid eval-donut-grid-single">
      <div class="item analytics-block eval-donut-card eval-donut-status" style="cursor:default;">
        <div class="row"><div class="name">Статус оцінювання</div><span class="badge b-blue mono">${filteredClosed.length}</span></div>
        <div class="eval-donut-wrap">
          <div class="eval-donut" style="background:${statusDonut.gradient};"></div>
          <div class="eval-legend">
            ${statusDonut.legendRows.map(row=>`<div class="eval-legend-item"><span class="eval-legend-dot" style="background:${row.color}"></span><span>${htmlesc(row.label)}</span><b class="mono">${row.value}</b><span class="mono">${row.percent}%</span></div>`).join("")}
          </div>
        </div>
      </div>
    </div>
  `;

  const deptDonutCard = `
    <div class="item analytics-block eval-donut-card eval-donut-depts" style="cursor:default;">
      <div class="row"><div class="name">Внесок відділів</div><span class="badge b-violet mono">${deptScoreRows.length}</span></div>
      <div class="eval-donut-wrap">
        <div class="eval-donut" style="background:${deptDonut.gradient};"></div>
        <div class="eval-legend">
          ${deptDonut.legendRows.length ? deptDonut.legendRows.map(row=>`<div class="eval-legend-item"><span class="eval-legend-dot" style="background:${row.color}"></span><span>${htmlesc(row.label)}</span><b class="mono">${row.value}</b><span class="mono">${row.percent}%</span></div>`).join("") : `<div class="hint">Поки немає оцінених задач.</div>`}
        </div>
      </div>
    </div>
  `;

  const criteriaBars = `
    <div class="item analytics-block" style="cursor:default;">
      <div class="row"><div class="name">Середній бал по критеріях</div><span class="badge b-ok mono">${evaluatedRows.length}</span></div>
      <div class="hint">Кожен критерій оцінюється від 1 до 5 після закриття задачі.</div>
      <div class="eval-bars">
        ${criteriaRows.map(row=>`
          <div class="eval-bar-row">
            <div class="eval-label">${htmlesc(row.label)}</div>
            <div class="eval-bar-wrap"><div class="eval-bar" style="width:${Math.round((row.value / maxCriteria) * 100)}%"></div></div>
            <div class="eval-value mono">${row.avg ? row.avg.toFixed(1) : "0.0"}</div>
          </div>
        `).join("")}
      </div>
    </div>
  `;

  const deptBars = `
    <div class="item analytics-block" style="cursor:default;">
      <div class="row"><div class="name">Бали по відділах</div><span class="badge b-blue mono">${deptScoreRows.length}</span></div>
      <div class="eval-bars">
        ${deptScoreRows.length ? deptScoreRows.map(row=>`
          <div class="eval-bar-row">
            <div class="eval-label">${htmlesc(row.dept.name)}</div>
            <div class="eval-bar-wrap"><div class="eval-bar alt" style="width:${Math.round((row.score / maxDeptScore) * 100)}%"></div></div>
            <div class="eval-value mono">${row.score}</div>
          </div>
        `).join("") : `<div class="hint">Ще немає оцінених задач.</div>`}
      </div>
    </div>
  `;

  const deptRanking = `
    <div class="item analytics-block eval-ranking-card" style="cursor:default;">
      <div class="row"><div class="name">Загальний рейтинг відділів</div><span class="badge b-blue mono">${deptScoreRows.length}</span></div>
      <div class="eval-ranking-list">
        ${deptScoreRows.length ? deptScoreRows.map((row, index)=>`
          <div class="eval-ranking-item ${index < 3 ? "top" : ""}">
            <div class="eval-ranking-pos">${index + 1}</div>
            <div class="eval-ranking-main">
              <div class="eval-ranking-name">${htmlesc(row.dept.name)}</div>
              <div class="eval-ranking-sub">${row.count} задач • середній ${row.avg.toFixed(1)}</div>
            </div>
            <div class="eval-ranking-score mono">${row.score}</div>
          </div>
        `).join("") : `<div class="hint">Ще немає оцінених задач.</div>`}
      </div>
    </div>
  `;

  const presetBars = `
    <div class="item analytics-block" style="cursor:default;">
      <div class="row"><div class="name">Які типи задач дали найбільше балів</div><span class="badge b-violet mono">${presetScoreRows.length}</span></div>
      <div class="eval-bars">
        ${presetScoreRows.length ? presetScoreRows.map(row=>`
          <div class="eval-bar-row">
            <div class="eval-label">${htmlesc(row.preset.label)}</div>
            <div class="eval-bar-wrap"><div class="eval-bar alt" style="width:${Math.max(8, Math.round((row.score / maxPresetScore) * 100))}%"></div></div>
            <div class="eval-value mono">${row.score}</div>
          </div>
          <div class="hint" style="margin:-6px 0 8px;">${row.count} задач • середній ${row.avg.toFixed(1)}</div>
        `).join("") : `<div class="hint">Ще немає оцінених задач по типах.</div>`}
      </div>
    </div>
  `;

  const pendingList = `
    <div class="item analytics-block" style="cursor:default;">
      <div class="row"><div class="name">Потребують оцінювання</div><span class="badge b-warn mono">${pendingRows.length}</span></div>
      <div class="eval-task-list">
        ${pendingRows.length ? pendingRows.slice(0,12).map(row=>{
          const dept = row.task.departmentId ? getDeptById(row.task.departmentId) : null;
          const user = row.task.responsibleUserId ? getUserById(row.task.responsibleUserId) : null;
          return `
            <div class="eval-task-item">
              <div class="eval-task-main">
                <div class="eval-task-title">${htmlesc(row.task.title || "Без назви")}</div>
                <div class="eval-task-meta">
                  ${dept ? deptBadgeHtml(dept) : `<span class="pill">Особисто</span>`}
                  <span class="pill">${htmlesc(user?.name || "Без виконавця")}</span>
                  <span class="pill mono">${row.closeDate ? fmtDate(row.closeDate) : "—"}</span>
                  ${row.preset ? `<span class="pill">Тип: ${htmlesc(row.preset.label)}</span>` : ``}
                </div>
              </div>
              <div class="eval-task-actions">
                <button class="btn ghost btn-mini" data-action="openTask" data-arg1="${row.task.id}">Відкрити</button>
                <button class="btn primary btn-mini" data-action="openTaskEvaluation" data-arg1="${row.task.id}">Оцінити</button>
              </div>
            </div>
          `;
        }).join("") : `<div class="hint">Немає закритих задач, які очікують оцінки після ${fmtDate(evaluationStartDate)}.</div>`}
      </div>
    </div>
  `;

  const recentList = `
    <div class="item analytics-block" style="cursor:default;">
      <div class="row"><div class="name">Останні оцінені задачі</div><span class="badge b-ok mono">${recentEvaluatedRows.length}</span></div>
      <div class="eval-task-list">
        ${recentEvaluatedRows.length ? recentEvaluatedRows.map(row=>{
          const dept = row.task.departmentId ? getDeptById(row.task.departmentId) : null;
          const score = evalTotalScore(row.evaluation);
          return `
            <div class="eval-task-item compact">
              <div class="eval-task-main">
                <div class="eval-task-title">${htmlesc(row.task.title || "Без назви")}</div>
                <div class="eval-task-meta">
                  ${dept ? deptBadgeHtml(dept) : `<span class="pill">Особисто</span>`}
                  <span class="pill mono">${fmtDate(toDateOnly(row.evaluation.evaluatedAt) || row.closeDate || "")}</span>
                  ${row.preset ? `<span class="pill">Тип: ${htmlesc(row.preset.label)}</span>` : ``}
                  <span class="badge b-ok mono">${score}/20</span>
                </div>
              </div>
              <div class="eval-task-actions">
                <button class="btn ghost btn-mini" data-action="openTaskEvaluation" data-arg1="${row.task.id}">Редагувати</button>
              </div>
            </div>
          `;
        }).join("") : `<div class="hint">Ще немає оцінених задач у вибраному зрізі.</div>`}
      </div>
    </div>
  `;

  const peopleBlock = `
    <div class="item analytics-block" style="cursor:default;">
      <div class="row"><div class="name">Бали по виконавцях</div><span class="badge b-violet mono">${userScoreRows.length}</span></div>
      <div class="eval-bars">
        ${userScoreRows.length ? userScoreRows.slice(0,10).map(row=>`
          <div class="eval-bar-row">
            <div class="eval-label">${htmlesc(row.user.name)}</div>
            <div class="eval-bar-wrap"><div class="eval-bar" style="width:${Math.round((row.score / maxUserScore) * 100)}%"></div></div>
            <div class="eval-value mono">${row.score}</div>
          </div>
        `).join("") : `<div class="hint">Недостатньо даних.</div>`}
      </div>
    </div>
  `;

  const body = `
    <div class="card analytics-card">
      <div class="card-h">
        <div class="t">Аналітика оцінювання</div>
        <div class="card-actions">
          <span class="badge b-blue">${htmlesc(period.label)}</span>
          <button class="btn ghost btn-mini analytics-toggle" data-action="toggleAnalyticsDetails">
            ${UI.analyticsShowDetails ? "Сховати деталі" : "Показати деталі"}
          </button>
        </div>
      </div>
      <div class="card-b">
        ${hero}
        ${filterBar}
        ${kpis}
        ${statusDonutCard}
        <div class="analytics-grid">
          ${criteriaBars}
          ${deptRanking}
          ${presetBars}
          ${deptBars}
          ${deptDonutCard}
          ${pendingList}
          ${recentList}
          ${UI.analyticsShowDetails ? peopleBlock : ``}
        </div>
      </div>
    </div>
  `;

  const tabs = [
    {key:ROUTES.CONTROL, label:"Цікаве", ico:"📚"},
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

        📚 Цікаве: довідкові записи по відділах і загальна інформація під рукою.<br/>

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

        1) Відкрий “Задачі” або “Аналітику” і перевір, що потребує уваги.<br/>

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

        4) За потреби тримай важливі службові примітки у вкладці “Цікаве”.

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
  exportCurrentRenderedModalPng,
  openCurrentRenderedModalStandalone,
  printCurrentRenderedModal,
  switchComparisonTopPanel,
  applyDeltaNrkAnalyticsFilters,
  resetDeltaNrkAnalyticsFilters,
  copyTextFromElement,
  filterStaffingUnitsBlock,
  setStaffingUnitsScope,
  setStaffingUnitsViewMode,
  toggleStaffingUnitsExpand,

  logout,

  openDbTasksPreview,

  exportBackupNow,

  openAbout,

  openHelp,

  applyTextFormat,

  insertTextTable,

  pasteTextTableFromClipboard,

  importReferenceWorkbook,

  importReferenceDeltaWorkbook,

  openReferenceWorkbookSheetPreview,

  importReferenceWorkbookSheet,

  mutateTextTableEditor,

  applyTextTableEditor,

  deleteTextTableFromTextarea,

  closeTextTableEditor,

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

  openTaskEvaluation,

  applyTaskEvaluationPresetFromInput,

  openTaskEvaluationHelp,

  saveTaskEvaluationNow,

  openRenderedTableModal,

  openReferenceGeneral,

  openReferenceDept,

  openReferenceEntry,

  openReferenceLink,

  saveReferenceGeneralNow,

  saveReferenceDeptNow,

  saveReferenceEntryNow,

  saveReferenceLinkNow,

  deleteReferenceEntryNow,

  deleteReferenceLinkNow,

  setReferenceDeptFilter,

  toggleReferenceEntry,

  setAnalyticsEvalPeriod,

  toggleTaskDensity,

  toggleTheme,

  toggleAnalyticsDetails,

};

const CHANGE_ACTIONS = {

  refreshDelPeople,

  refreshRespOptions,

  selectSingleDeptToggleFromInput,

  setTaskSearchFromInput,

  setReportsControlDateFromInput,

  setReportingMonthFromInput,

  setPlanMonthFromInput,

  setAnalyticsEvalDeptFilterFromInput,

  setAnalyticsEvalUserFilterFromInput,

  setAnalyticsEvalStatusFilterFromInput,

  setAnalyticsEvalTypeFilterFromInput,

  setAnalyticsEvalPresetFilterFromInput,

  setEvaluationStartDateFromInput,

  setReferenceSearchFromInput,

  setReferenceDeptFilterFromInput,

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

  "openTaskEvaluation",

  "openMeetingRepeat",

  "openReportForm",

  "openReportPlanCreate",

  "openReportPlanEdit",

  "saveReportPlanNow",

  "confirmDeleteReportPlan",

  "deleteReportPlanNow",

  "saveAnnouncementEdits",

  "saveReferenceDeptNow",

  "saveReferenceGeneralNow",

  "saveReferenceEntryNow",

  "deleteReferenceEntryNow",

  "saveTaskEvaluationNow",

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

  const tableToggleSummary = e.target.closest(".task-table-toggle summary");

  if(tableToggleSummary){

    e.stopPropagation();

    return;

  }

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

  UI.renderedTableModals = {};

  if(!user){

    UI.route = ROUTES.LOGIN;

    return viewLogin();

  }

  enforceReadOnlyNavigation(user);

  runRecurringTemplates();

  runReportPlans();

  remindPendingEvaluations();



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

















































