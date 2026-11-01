// The playground page: one live paros universe in a worker, driven by the
// operator's buttons. Each click sends one step (or a few) to the universe
// and prints the `parosctl` line it runs; a fault prints the simulator's
// line. The page draws only what the universe sends back: the topology the
// operator reads through the admin API, and the events the machines log.

import { Stage, Universe, chip, drawTimeline, el, legend, roles } from "./stage.js";

const $ = (id) => document.getElementById(id);

// Eight machines over three zones: init forms cell-1 over three, two wait
// for `cell add-machine`, and three found cell-2.
const MACHINES = 8;
const FOUNDERS = [0, 1, 2];
const TENANT_NAMES = ["acme", "globex", "initech", "umbrella", "hooli", "stark", "wayne", "tyrell"];
const JOURNAL_NAMES = ["orders", "events", "audit", "payments", "invoices", "sessions", "clicks", "feed"];
// The simulated ms a tick runs, per real ms, by speed name.
const SPEEDS = { "0.05×": 0.05, "0.2×": 0.2, "1×": 1, "5×": 5 };
const TICK_MS = 100;

const addr = (m) => `machine-${m}:4500`;

// ---- the universe ----

let seed = Number(new URLSearchParams(location.search).get("seed")) || 1;
let u = null;
let speed = 0.2;
let paused = false;
let last = { sim: 0, wall: performance.now() };
let shownT = 0;
// What the operator asked for, in click order, for the log: each entry is
// one step with its parosctl lines, its index once the universe took it,
// and its result once it ended.
let entries = [];
let waiting = [];
// What the page believes exists, from the steps asked for: undone when a
// step fails.
let plan = null;
// Each journal's acceptors in the last topology, by its key: a change is a
// move the cell made on its own (a heal off a down machine, a drain).
let members = new Map();

function freshPlan() {
  return { init: false, cell2: false, tenants: new Map(), journals: new Map(), added: [], draining: new Set(), streams: new Set() };
}

const stage = new Stage({
  stage: $("stage"), wires: $("wires"), cells: $("cells"), client: $("client"), rail: $("rail"),
}, () => { syncJournalPick(); });

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
worker.onmessage = ({ data }) => {
  if (data.type === "frame") {
    const frame = data.frame;
    u.add(frame);
    last = { sim: frame.now_ms, wall: performance.now() };
    for (const step of frame.steps) settle(step);
    if (frame.topology.length) { noteMoves(); refreshPicks(); renderRoles(); $("legend").replaceChildren(...legend(u)); }
    if (frame.error) setStatus("stopped", "gap", frame.error);
  } else if (data.type === "acted") {
    const entry = waiting.shift();
    if (entry) { entry.index = data.index; entry.at = u.now; }
  } else if (data.type === "error") {
    const entry = waiting.shift();
    if (entry) { entry.done = { ok: false, detail: data.message }; undo(entry); renderLog(); }
    else setStatus("stopped", "gap", data.message);
  }
};

function start() {
  u = new Universe({ seed, machines: MACHINES });
  stage.load(u);
  plan = freshPlan();
  members = new Map();
  entries = [];
  waiting = [];
  last = { sim: 0, wall: performance.now() };
  shownT = 0;
  worker.postMessage({ type: "start", config: { seed, machines: MACHINES, run_ms: 300000 } });
  worker.postMessage({ type: "speed", value: speed });
  worker.postMessage({ type: "pause", value: paused });
  $("seed").textContent = String(seed);
  setStatus(paused ? "paused" : "running", paused ? "" : "done");
  renderLog();
  renderRoles();
  refreshPicks();
}

function setStatus(text, kind, why) {
  $("status").replaceChildren(chip(text, kind));
  $("status").title = why || "";
}

// ---- steps and the log ----

// Send `ops`, each with its lines, as one click.
function send(steps) {
  for (const step of steps) {
    const entry = { op: step.op, lines: step.lines, fault: Boolean(step.fault), index: null, done: null, apply: step.apply };
    step.apply?.(true);
    entries.push(entry);
    waiting.push(entry);
    worker.postMessage({ type: "act", op: step.op });
  }
  renderLog();
}

function settle(step) {
  const entry = entries.find((e) => e.index === step.index);
  if (!entry) return;
  entry.done = step;
  if (!step.ok) undo(entry);
  renderLog();
}

function undo(entry) {
  entry.apply?.(false);
  refreshPicks();
}

// Log each journal whose acceptors changed since the last topology: the
// cell coordinator moved them, with no click (a heal re-places the roles of
// a machine down for 4.5 s of sim time; a drain moves them off).
function noteMoves() {
  const topo = u.topo;
  if (!topo) return;
  const gone = new Set([...(topo.down ?? []), ...(topo.isolated ?? [])]);
  for (const tenant of topo.tenants ?? []) {
    for (const j of tenant.journals) {
      const now = j.members ?? [];
      const before = members.get(j.key);
      members.set(j.key, now);
      if (!before || !before.length || !now.length) continue;
      const left = before.filter((m) => !now.includes(m));
      const came = now.filter((m) => !before.includes(m));
      if (!left.length && !came.length) continue;
      const why = left.some((m) => gone.has(m)) ? "heal" : "move";
      const from = left.length ? left.map((m) => `m${m}`).join(", ") : "–";
      const to = came.length ? came.map((m) => `m${m}`).join(", ") : "–";
      note(`${why}: ${tenant.name}/${j.name} acceptor moved from ${from} to ${to}`);
    }
  }
}

function note(text) {
  entries.push({ op: null, lines: [], note: text, done: null });
  renderLog();
}

function renderLog() {
  const log = $("log");
  const rows = [];
  for (const e of entries.slice(-40)) {
    if (e.note) { rows.push(el("div", { className: "line note", textContent: `# ${e.note}` })); continue; }
    const block = el("div", { className: `entry${e.done ? (e.done.ok ? " ok" : " failed") : " queued"}` });
    for (const line of e.lines) block.append(el("div", { className: `line${e.fault ? " struck" : ""}`, textContent: line }));
    const state = e.done
      ? `${e.done.ok ? "ok" : "failed"} · ${fmt(e.done.start_ms ?? u.now)}${e.done.end_ms !== undefined ? `–${fmt(e.done.end_ms)}` : ""} · ${e.done.detail}`
      : e.index === null ? "sending…" : "queued: it starts when the steps before it end";
    block.append(el("div", { className: "result", textContent: `→ ${state}` }));
    rows.push(block);
  }
  log.replaceChildren(...rows);
  log.scrollTop = log.scrollHeight;
}

const fmt = (ms) => `${(ms / 1000).toFixed(1)} s`;

// ---- what to act on ----

function cellOf() { return u.cellOf(); }

function inCell(m) {
  return cellOf()[m] !== null || (plan.init && FOUNDERS.includes(m)) || plan.added.includes(m)
    || (plan.cell2 && plan.cell2.includes?.(m));
}

function idleMachines() {
  const out = [];
  for (let m = 0; m < MACHINES; m++) if (!inCell(m)) out.push(m);
  return out;
}

function down() { return new Set(u.topo?.down ?? []); }
function cut() { return new Set(u.topo?.isolated ?? []); }

// The machine to strike: the one picked, else the one that leads the most
// journals and is up.
function target(filter, fallback = "leader") {
  const picked = $("machine-pick").value;
  if (picked !== "auto") return Number(picked);
  const ok = [];
  for (let m = 0; m < MACHINES; m++) if (filter(m)) ok.push(m);
  if (!ok.length) return null;
  if (fallback === "leader") {
    const leads = new Map();
    for (const tenant of u.topo?.tenants ?? []) for (const j of tenant.journals) {
      if (j.leader !== null && ok.includes(j.leader)) leads.set(j.leader, (leads.get(j.leader) || 0) + 1);
    }
    const busiest = [...leads].sort((x, y) => y[1] - x[1] || x[0] - y[0])[0];
    if (busiest) return busiest[0];
  }
  return fallback === "last" ? ok[ok.length - 1] : ok[0];
}

function freeName(names, taken, prefix) {
  const free = names.find((n) => !taken.includes(n));
  if (free) return free;
  for (let i = 2; ; i++) if (!taken.includes(`${prefix}-${i}`)) return `${prefix}-${i}`;
}

// The journal picked, else the newest.
function journal() {
  const picked = $("journal-pick").value;
  if (picked && plan.journals.has(picked)) return picked;
  const all = [...plan.journals.keys()];
  return all[all.length - 1] ?? null;
}

function modeFlags(mode) {
  const parts = Object.fromEntries(mode.split(",").map((p) => p.split("=")));
  const flags = [];
  if (parts.machines !== undefined) flags.push(`--survive machines=${parts.machines}`);
  if (parts.partitions !== undefined) flags.push(`--partitions ${parts.partitions}`);
  if (parts.replicas !== undefined && parts.replicas !== "0") flags.push(`--replicas ${parts.replicas}`);
  return flags.join(" ");
}

// ---- the buttons ----

function needCell() {
  if (plan.init) return true;
  note("Init cell-1 first.");
  return false;
}

function tenantStep(name, cell) {
  return {
    op: { kind: "create_tenant", tenant: name, cell },
    lines: [`$ parosctl tenant create ${name} --cell ${cell}`],
    apply: (on) => on ? plan.tenants.set(name, cell) : plan.tenants.delete(name),
  };
}

function journalSteps(how) {
  const steps = [];
  let tenant = [...plan.tenants.keys()].at(-1);
  if (!tenant) {
    tenant = freeName(TENANT_NAMES, [], "tenant");
    steps.push(tenantStep(tenant, "cell-1"));
  }
  const taken = [...plan.journals.keys()].filter((k) => k.startsWith(`${tenant}/`)).map((k) => k.split("/")[1]);
  const mode = $("mode").value;
  const multi = $("writers").value === "multi";
  for (let i = 0; i < how; i++) {
    const name = freeName(JOURNAL_NAMES, taken, "journal");
    taken.push(name);
    const key = `${tenant}/${name}`;
    steps.push({
      op: { kind: "create_journal", tenant, journal: name, mode, multi },
      lines: [`$ parosctl journal create ${tenant} ${name} ${modeFlags(mode)}${multi ? " --writers multi" : ""}`],
      apply: (on) => on ? plan.journals.set(key, { multi }) : plan.journals.delete(key),
    });
  }
  return steps;
}

function journalOp(kind, extra = {}) {
  const key = journal();
  if (!key) { note("Create a journal first."); return null; }
  const [tenant, name] = key.split("/");
  return { key, tenant, name, op: { kind, tenant, journal: name, ...extra } };
}

const ACTIONS = {
  // Bootstrap
  init: () => {
    if (plan.init) return note("cell-1 exists already: Reset to start again.");
    send([{
      op: { kind: "init", machines: FOUNDERS.length },
      lines: [`$ parosctl init --members ${FOUNDERS.map(addr).join(",")}`],
      apply: (on) => { plan.init = on; },
    }]);
  },
  "add-machine": () => {
    if (!needCell()) return;
    // Keep three idle machines for cell-2 while it does not exist.
    const idle = idleMachines();
    const spare = plan.cell2 ? idle : idle.slice(0, Math.max(0, idle.length - 3));
    const m = spare[0] ?? (plan.cell2 ? null : idle[0]);
    if (m === undefined || m === null) return note("No idle machine is left.");
    send([{
      op: { kind: "add_machine", machine: m },
      lines: [`$ parosctl cell add-machine ${addr(m)}`],
      apply: (on) => { if (on) plan.added.push(m); else plan.added = plan.added.filter((x) => x !== m); },
    }]);
  },
  "add-cell": () => {
    if (!needCell()) return;
    if (plan.cell2) return note("cell-2 exists already.");
    const members = idleMachines().slice(-3);
    if (!members.length) return note("No idle machine is left to found cell-2.");
    const list = members.map(addr).join(",");
    send([{
      op: { kind: "add_cell", machines: members },
      lines: [`$ parosctl --servers ${addr(members[0])} init --idle --members ${list}`,
        `$ parosctl universe add-cell --members ${list} --name cell-2`],
      apply: (on) => { plan.cell2 = on ? members : false; refreshPicks(); },
    }]);
  },
  // Resources
  tenant: () => {
    if (!needCell()) return;
    const cell = $("cell-pick").value;
    if (cell === "cell-2" && !plan.cell2) return note("Add cell-2 first.");
    send([tenantStep(freeName(TENANT_NAMES, [...plan.tenants.keys()], "tenant"), cell)]);
  },
  journal: () => { if (needCell()) send(journalSteps(1)); },
  journals: () => { if (needCell()) send(journalSteps(3)); },
  configure: () => {
    const j = journalOp("configure");
    if (!j) return;
    const mode = `${$("mode").value},replicas=${$("replicas").value}`;
    j.op.mode = mode;
    send([{ op: j.op, lines: [`$ parosctl configure --tenant ${j.tenant} --journal ${j.name} ${modeFlags(mode)}`] }]);
  },
  // Workloads
  traffic: () => {
    if (!needCell()) return;
    const steps = plan.journals.size ? [] : journalSteps(2);
    steps.push({ op: { kind: "traffic", count: 10 },
      lines: ["$ parosctl write <each journal> r1 … r10   # every journal at the same time"] });
    send(steps);
  },
  stream: () => {
    const j = journalOp("stream");
    if (!j) return;
    const on = !plan.streams.has(j.key);
    j.op.on = on;
    send([{ op: j.op,
      lines: [on ? `$ while true; do parosctl write ${j.key} record; sleep 0.2; done &` : `$ kill %${j.key}`],
      apply: (yes) => { if (yes === on) plan.streams.add(j.key); else plan.streams.delete(j.key); refreshStreamLabel(); } }]);
  },
  write: () => {
    const j = journalOp("write", { count: 5 });
    if (j) send([{ op: j.op, lines: [`$ parosctl write ${j.key} r1 r2 r3 r4 r5`] }]);
  },
  read: () => {
    const j = journalOp("read", { count: 5 });
    if (j) send([{ op: j.op, lines: [`$ parosctl read ${j.key} --from <tail − 5> --limit 5`] }]);
  },
  "set-leader": () => {
    const j = journalOp("set_leader");
    if (!j) return;
    if (plan.journals.get(j.key)?.multi) return note(`${j.key} is a multi-writer journal: it has no leader to replace.`);
    send([{ op: j.op, lines: [`$ parosctl set-leader ${j.key} --new <fresh uuid>`, `$ parosctl write ${j.key} stale --leader <old uuid> --no-claim`] }]);
  },
  resolve: () => {
    const tenant = journal()?.split("/")[0] ?? [...plan.tenants.keys()].at(-1);
    if (!tenant) return note("Create a tenant first.");
    send([{ op: { kind: "resolve", tenant }, lines: [`$ parosctl resolve ${tenant}`] }]);
  },
  // Machines
  maintenance: () => {
    const m = target((x) => cellOf()[x] === "cell-1" && !FOUNDERS.includes(x), "last") ?? target((x) => cellOf()[x] === "cell-1", "last");
    if (m === null) return note("No machine of cell-1 to put in maintenance.");
    send([{ op: { kind: "maintenance", machine: m, for_ms: 30000 }, lines: [`$ parosctl cell maintenance machine-${m} --for 30s`] }]);
  },
  drain: () => {
    const m = target((x) => cellOf()[x] === "cell-1" && !FOUNDERS.includes(x), "last");
    if (m === null) return note("Add a machine to cell-1 first: drain one that is not a founder.");
    send([{ op: { kind: "drain", machine: m }, lines: [`$ parosctl cell drain-machine machine-${m}`],
      apply: (on) => on ? plan.draining.add(m) : plan.draining.delete(m) }]);
  },
  remove: () => {
    const picked = $("machine-pick").value;
    const m = picked !== "auto" ? Number(picked) : [...plan.draining][0];
    if (m === undefined) return note("Drain a machine first.");
    send([{ op: { kind: "remove", machine: m }, lines: [`$ parosctl cell remove-machine machine-${m}`] }]);
  },
  // Faults
  kill: () => {
    const m = target((x) => cellOf()[x] !== null && !down().has(x));
    if (m === null) return note("No machine is up.");
    send([{ op: { kind: "crash", machine: m }, fault: true, lines: [`# sim: crash machine-${m} (its unsynced writes are lost)`] }]);
  },
  restart: () => {
    const m = target((x) => down().has(x), "first");
    if (m === null) return note("No machine is down.");
    send([{ op: { kind: "restart", machine: m }, fault: true, lines: [`# sim: restart machine-${m} on its disk`] }]);
  },
  isolate: () => {
    const m = target((x) => cellOf()[x] !== null && !cut().has(x) && !down().has(x));
    if (m === null) return note("No machine to cut off.");
    send([{ op: { kind: "isolate", machine: m }, fault: true, lines: [`# sim: cut machine-${m} from the network`] }]);
  },
  heal: () => {
    const m = target((x) => cut().has(x), "first");
    if (m === null) return note("No machine is cut off.");
    send([{ op: { kind: "heal", machine: m }, fault: true, lines: [`# sim: join machine-${m} to the network again`] }]);
  },
};

for (const button of document.querySelectorAll("[data-act]")) {
  button.onclick = () => ACTIONS[button.dataset.act]();
}

// ---- the pickers ----

function refreshPicks() {
  if (!u) return;
  const mp = $("machine-pick");
  const keep = mp.value;
  const cells = cellOf();
  mp.replaceChildren(el("option", { value: "auto", textContent: "auto" }),
    ...Array.from({ length: MACHINES }, (_, m) => el("option", { value: String(m), textContent: `m${m}${cells[m] ? ` · ${cells[m]}` : " · idle"}` })));
  mp.value = [...mp.options].some((o) => o.value === keep) ? keep : "auto";
  const cp = $("cell-pick");
  cp.options[1].disabled = !plan.cell2;
  syncJournalPick();
  refreshStreamLabel();
}

function syncJournalPick() {
  const jp = $("journal-pick");
  const keys = [...plan.journals.keys()];
  const followed = stage.selected && u.journals.get(stage.selected);
  const want = followed && !followed.tenant.internal ? `${followed.tenant.name}/${followed.name}` : jp.value;
  jp.replaceChildren(...(keys.length ? keys : ["(none yet)"]).map((k) => el("option", { value: keys.length ? k : "", textContent: k })));
  if (keys.includes(want)) jp.value = want; else if (keys.length) jp.value = keys[keys.length - 1];
  refreshStreamLabel();
}

function refreshStreamLabel() {
  const key = journal();
  $("stream-btn").textContent = key && plan.streams.has(key) ? "Stop the writer" : "Start a writer";
}

$("journal-pick").onchange = () => {
  const key = $("journal-pick").value;
  for (const j of u.journals.values()) if (`${j.tenant.name}/${j.name}` === key) stage.select(j.key);
  refreshStreamLabel();
};

function renderRoles() {
  $("roles").replaceChildren(roles(u));
  const cells = u.topo?.cells?.length ?? 0;
  const up = cellOf().filter((c, m) => c !== null && !down().has(m)).length;
  $("cells-count").textContent = String(cells);
  $("machines-up").textContent = `${up} of ${MACHINES}`;
  $("journals-count").textContent = String(plan.journals.size);
}

// ---- the clock ----

$("play").onclick = () => {
  paused = !paused;
  worker.postMessage({ type: "pause", value: paused });
  $("play").textContent = paused ? "▶ Play" : "❚❚ Pause";
  setStatus(paused ? "paused" : "running", paused ? "" : "done");
};
$("speed").onchange = () => {
  speed = SPEEDS[$("speed").value];
  worker.postMessage({ type: "speed", value: speed });
};
$("reset").onclick = () => {
  seed = Number($("seed-input").value) || seed;
  start();
};
$("seed-input").value = String(seed);
for (const name of Object.keys(SPEEDS)) $("speed").append(el("option", { value: name, textContent: name, selected: name === "0.2×" }));

function frame() {
  if (u) {
    const lag = TICK_MS * speed;
    const now = performance.now();
    const t = paused ? shownT : Math.min(last.sim, Math.max(shownT, last.sim - lag + (now - last.wall) * speed));
    shownT = t;
    stage.cards();
    stage.draw(t, Math.max(8, 300 * speed), $("show-beats").checked);
    const { label } = stage.rail(t);
    $("followed").textContent = label ?? "";
    $("time").textContent = fmt(t);
    const marks = entries.filter((e) => e.done && e.op).map((e) => ({
      t: e.done.start_ms, fault: e.fault, label: e.op.kind.replace("create_", "").replace("_", " "),
    }));
    drawTimeline($("timeline"), u, t, marks);
  }
  requestAnimationFrame(frame);
}

start();
requestAnimationFrame(frame);
