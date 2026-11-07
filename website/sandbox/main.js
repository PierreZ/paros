// The playground page: one live paros universe in a worker, driven by the
// operator's buttons. Each click sends one step (or a few) to the universe
// and prints the `parosctl` line it runs; a fault prints the simulator's
// line. The page draws only what the universe sends back: the topology the
// operator reads through the admin API, and the events the machines log.

import { mountBlocks } from "./blocks.js";
import { ELECTION_SPAN_MS, milestones, nextCampaign } from "./election.js";
import { Stage, Universe, chip, drawTimeline, el, legend, roles } from "./stage.js";

const $ = (id) => document.getElementById(id);

// Nine machines over three zones: init forms cell-1 over the first ones, the
// others wait for `cell add-machine` or found cell-2. Machines 3 and 5 run with
// PAROS_ROLES=stateless (#822 (stateless playground roles)): they take the
// proxy leaders, the batchers and the frontends, and never found a cell.
const MACHINES = 9;
const STATELESS = [3, 5];
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
let plan = freshPlan();
// Each journal's acceptors in the last topology, by its key: a change is a
// move the cell made on its own (a heal off a down machine, a drain).
let members = new Map();

function freshPlan() {
  return { init: 0, cell2: false, tenants: new Map(), journals: new Map(), added: [], draining: new Set(), streams: new Set() };
}

const stage = new Stage({
  stage: $("stage"), wires: $("wires"), cells: $("cells"), client: $("client"), rail: $("rail"),
}, () => { refreshPicks(); });

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
worker.onmessage = ({ data }) => {
  if (data.type === "frame") {
    const frame = data.frame;
    u.add(frame);
    last = { sim: frame.now_ms, wall: performance.now() };
    for (const step of frame.steps) settle(step);
    if (frame.choices) choices = frame.choices;
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
  armed = null;
  stepping = null;
  heldWorker = false;
  showStepper(false);
  u = new Universe({ seed, machines: MACHINES, stateless: STATELESS });
  stage.load(u);
  plan = freshPlan();
  choices = null;
  members = new Map();
  entries = [];
  waiting = [];
  last = { sim: 0, wall: performance.now() };
  shownT = 0;
  worker.postMessage({ type: "start", config: { seed, machines: MACHINES, run_ms: 300000, stateless: STATELESS } });
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
  refreshPicks();
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

// ---- what is valid now ----

// The newest choices the universe sent (`Frame.choices`, read from its last
// topology), or `null` before the first one.
let choices = null;

const all = () => Array.from({ length: MACHINES }, (_, m) => m);
const sortNum = (list) => [...new Set(list)].sort((x, y) => x - y);
const stateless = (m) => STATELESS.includes(m);
const machineText = (m) => `machine-${m}${stateless(m) ? " · stateless" : ""}`;

// The machines the steps asked for put in a cell, before a topology shows them.
function claimed() {
  const out = new Set(plan.added);
  for (let m = 0; m < (plan.init || 0); m++) out.add(m);
  for (const m of plan.cell2 || []) out.add(m);
  return out;
}

function cells() {
  const out = plan.init ? ["cell-1"] : [];
  if (plan.cell2) out.push("cell-2");
  for (const c of choices?.cells ?? []) if (!out.includes(c)) out.push(c);
  return out;
}

function idle() {
  const mine = claimed();
  return (choices?.idle ?? all()).filter((m) => !mine.has(m));
}

function running() { return choices ? choices.running : sortNum([...claimed()]); }
function downs() { return choices?.down ?? []; }
function cutOff() { return choices?.cut ?? []; }
function connected() { return choices ? choices.connected : running(); }
function busiest(among) {
  const b = choices?.busiest;
  return among.includes(b) ? b : among[0];
}

// `list` as options, `first` first (the value a click with no change uses).
function machineOptions(list, first = list[0]) {
  const rest = list.filter((m) => m !== first);
  return (first === undefined ? rest : [first, ...rest]).map((m) => [m, machineText(m)]);
}

// The tenants, newest first.
function tenants() {
  const out = [...plan.tenants.keys()].reverse();
  for (const t of choices?.tenants ?? []) if (!out.includes(t)) out.push(t);
  return out;
}

// The journals, the followed one first, then the newest.
function journals() {
  const out = [...plan.journals.keys()].reverse();
  for (const j of choices?.journals ?? []) if (!out.includes(j)) out.push(j);
  const followed = $("journal-pick").value;
  return out.includes(followed) ? [followed, ...out.filter((j) => j !== followed)] : out;
}

const as = (list) => list.map((x) => [x, x]);

function freeName(names, taken, prefix) {
  const free = names.find((n) => !taken.includes(n));
  if (free) return free;
  for (let i = 2; ; i++) if (!taken.includes(`${prefix}-${i}`)) return `${prefix}-${i}`;
}

function modeFlags(mode) {
  const parts = Object.fromEntries(mode.split(",").map((p) => p.split("=")));
  const flags = [];
  if (parts.machines !== undefined) flags.push(`--survive machines=${parts.machines}`);
  if (parts.partitions !== undefined) flags.push(`--partitions ${parts.partitions}`);
  if (parts.layout !== undefined) flags.push(`--layout ${parts.layout}`);
  for (const count of ["replicas", "proxies", "batchers"]) {
    if (parts[count] !== undefined) flags.push(`--${count} ${parts[count]}`);
  }
  return flags.join(" ");
}

// `machines=F[,partitions=P],layout=L`.
function modeOf(v) {
  return `machines=${v.survive}${v.partitions !== "1" ? `,partitions=${v.partitions}` : ""},layout=${v.layout}`;
}

// ---- the steps a block sends ----

function tenantStep(name, cell) {
  return {
    op: { kind: "create_tenant", tenant: name, cell },
    lines: [`$ parosctl tenant create ${name} --cell ${cell}`],
    apply: (on) => on ? plan.tenants.set(name, cell) : plan.tenants.delete(name),
  };
}

function journalSteps(how, tenantPicked, mode, multi) {
  const steps = [];
  let tenant = tenantPicked;
  if (!tenant) {
    tenant = freeName(TENANT_NAMES, tenants(), "tenant");
    steps.push(tenantStep(tenant, "cell-1"));
  }
  const taken = journals().filter((k) => k.startsWith(`${tenant}/`)).map((k) => k.split("/")[1]);
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

function split(key) {
  const at = key.indexOf("/");
  return [key.slice(0, at), key.slice(at + 1)];
}

// The `--servers` flag of a command for `cell`: cell-1 is the default.
function serversOf(cell) {
  if (cell !== "cell-2" || !plan.cell2) return "";
  return `--servers ${addr(plan.cell2[0])} `;
}

const needInit = () => plan.init ? null : "Init cell-1 first.";

// ---- the blocks ----

// The fields the blocks share.
const machineField = (label, list, empty, first) => ({
  key: "machine", kind: "select", label, empty, options: () => machineOptions(list(), first?.(list())),
});
const journalField = (label = "journal", list = journals) => ({
  key: "journal", kind: "select", label, options: () => as(list()), empty: "Create a journal first.",
});
const survive = { key: "survive", kind: "select", label: "machines it survives", options: () => [["1", "1"], ["0", "0"], ["2", "2"]] };
const partitions = { key: "partitions", kind: "select", label: "partitions", options: () => [["1", "1"], ["2", "2"]] };
const layout = { key: "layout", kind: "select", label: "layout", options: () => as(["colocated", "compartmentalized"]) };
const count012 = (key, label, first) => ({ key, kind: "select", label, options: () => as([first, ...["0", "1", "2"].filter((n) => n !== first)]) });

// The most machines `init` can list: the first machines, up to the first
// stateless one (a stateless machine is never an initial member).
const MAX_INITIAL = Math.min(3, STATELESS.length ? Math.min(...STATELESS) : MACHINES);

const BLOCKS = [
  { group: "bootstrap", label: "Bootstrap", blocks: [
    { id: "init", title: "Form cell-1 over the first machines, and the universe on it",
      parts: ["init cell-1 over", { key: "count", kind: "number", label: "machines", min: 1, max: MAX_INITIAL, value: MAX_INITIAL }, "machines"],
      why: () => plan.init ? "cell-1 exists already: Reset to start again." : null,
      run: ({ count }) => {
        const members = all().slice(0, count);
        send([{
          op: { kind: "init", machines: count },
          lines: [`$ parosctl init --members ${members.map(addr).join(",")}`],
          apply: (on) => { plan.init = on ? count : 0; },
        }]);
      } },
    { id: "add-machine", title: "Admit an idle machine into a cell",
      parts: ["add", machineField("idle machine", idle, "No idle machine is left."), "to", { key: "cell", kind: "select", label: "cell", options: () => as(cells()), empty: "Init cell-1 first." }],
      why: needInit,
      run: ({ machine, cell }) => {
        const m = Number(machine);
        send([{
          op: { kind: "add_machine", machine: m, cell },
          lines: [`$ parosctl ${serversOf(cell)}cell add-machine ${addr(m)}`],
          apply: (on) => { if (on) plan.added.push(m); else plan.added = plan.added.filter((x) => x !== m); },
        }]);
      } },
    { id: "add-cell", title: "Form an idle cell over the last idle machines that store, then join it to the universe",
      parts: ["add cell-2 over", { key: "count", kind: "number", label: "machines", min: 1, max: 3, value: 3 }, "machines"],
      why: () => needInit() ?? (plan.cell2 ? "cell-2 exists already." : null),
      run: ({ count }) => {
        const members = idle().filter((m) => !stateless(m)).slice(-count);
        if (members.length < count) return note(`Only ${members.length} idle machines that store are left to found cell-2.`);
        const list = members.map(addr).join(",");
        send([{
          op: { kind: "add_cell", machines: members },
          lines: [`$ parosctl --servers ${addr(members[0])} init --idle --members ${list}`,
            `$ parosctl universe add-cell --members ${list} --name cell-2`],
          apply: (on) => { plan.cell2 = on ? members : false; },
        }]);
      } },
  ] },
  { group: "resources", label: "Resources", blocks: [
    { id: "tenant", title: "Create a tenant in a cell",
      parts: ["create tenant", { key: "name", kind: "text", label: "tenant name", auto: () => freeName(TENANT_NAMES, tenants(), "tenant") },
        "in", { key: "cell", kind: "select", label: "cell", options: () => as(cells()), empty: "Init cell-1 first." }],
      why: needInit,
      run: ({ name, cell }) => {
        if (tenants().includes(name)) return note(`Tenant ${name} exists already.`);
        send([tenantStep(name, cell)]);
      } },
    { id: "journal", title: "Create journals in a tenant, with their durability, writers and layout",
      parts: ["create", { key: "count", kind: "number", label: "journals", min: 1, max: 8, value: 1 }, "journals in",
        { key: "tenant", kind: "select", label: "tenant", options: () => { const t = tenants(); return t.length ? as(t) : [["", "a new tenant"]]; } },
        "survive", survive, "machines,", partitions, "partitions,",
        { key: "writers", kind: "select", label: "writers", options: () => as(["single", "multi"]) }, "writers,", layout],
      why: needInit,
      run: (v) => send(journalSteps(v.count, v.tenant, modeOf(v), v.writers === "multi")) },
    { id: "configure", title: "Record a new durability, layout and role counts for a journal, and the frontend count of its tenant",
      parts: ["configure", journalField(), "survive", survive, "machines,", partitions, "partitions,", layout, "with",
        count012("replicas", "replicas", "1"), "replicas,", count012("proxies", "proxy leaders", "1"), "proxies,",
        count012("batchers", "batchers", "0"), "batchers,", count012("frontends", "frontends", "1"), "frontends"],
      run: (v) => {
        const [tenant, name] = split(v.journal);
        // A colocated journal runs every job on its acceptors: the counts
        // apply to a compartmentalized one only.
        const mode = v.layout === "compartmentalized"
          ? `${modeOf(v)},replicas=${v.replicas},proxies=${v.proxies},batchers=${v.batchers}`
          : modeOf(v);
        const frontends = Number(v.frontends);
        send([
          { op: { kind: "configure", tenant, journal: name, mode },
            lines: [`$ parosctl configure --tenant ${tenant} --journal ${name} ${modeFlags(mode)}`] },
          { op: { kind: "frontends", tenant, count: frontends },
            lines: [`$ parosctl configure --tenant ${tenant} --frontend-count ${frontends}`] },
        ]);
      } },
  ] },
  { group: "workloads", label: "Workloads", blocks: [
    { id: "write", title: "Write records to one journal, or to every journal at the same time",
      parts: ["write", { key: "count", kind: "number", label: "records", min: 1, max: 100, value: 10 }, "records to",
        { key: "journal", kind: "select", label: "journal", options: () => [["*", "every journal"], ...as(journals())] }],
      why: needInit,
      run: ({ count, journal }) => {
        const records = count === 1 ? "r1" : `r1 … r${count}`;
        if (journal === "*") {
          const steps = journals().length ? [] : journalSteps(2, tenants()[0], "machines=1,layout=colocated", false);
          steps.push({ op: { kind: "traffic", count },
            lines: [`$ parosctl write <each journal> ${records}   # every journal at the same time`] });
          return send(steps);
        }
        const [tenant, name] = split(journal);
        send([{ op: { kind: "write", tenant, journal: name, count }, lines: [`$ parosctl write ${journal} ${records}`] }]);
      } },
    { id: "stream", title: "A writer appends to the journal at this rate, between the other steps",
      parts: ["start writer on", journalField(), "at", { key: "rate", kind: "number", label: "records a second", min: 1, max: 20, value: 5 }, "per second"],
      run: ({ journal, rate }) => {
        const [tenant, name] = split(journal);
        send([{ op: { kind: "stream", tenant, journal: name, on: true, rate },
          lines: [`$ while true; do parosctl write ${journal} record; sleep ${(1 / rate).toFixed(2)}; done &`],
          apply: (yes) => { if (yes) plan.streams.add(journal); else plan.streams.delete(journal); } }]);
      } },
    { id: "stop-stream", title: "Stop the writer of a journal",
      parts: ["stop writer on", { key: "journal", kind: "select", label: "journal", options: () => as(journals().filter((j) => plan.streams.has(j))), empty: "No writer runs." }],
      run: ({ journal }) => {
        const [tenant, name] = split(journal);
        send([{ op: { kind: "stream", tenant, journal: name, on: false, rate: 5 }, lines: [`$ kill %${journal}`],
          apply: (yes) => { if (yes) plan.streams.delete(journal); else plan.streams.add(journal); } }]);
      } },
    { id: "read", title: "Read the last records of a journal",
      parts: ["read last", { key: "count", kind: "number", label: "records", min: 1, max: 100, value: 5 }, "records of", journalField()],
      run: ({ count, journal }) => {
        const [tenant, name] = split(journal);
        send([{ op: { kind: "read", tenant, journal: name, count }, lines: [`$ parosctl read ${journal} --from <tail − ${count}> --limit ${count}`] }]);
      } },
    { id: "set-leader", title: "A new writer takes the journal; the old writer's next write is refused",
      parts: ["set new leader of", journalField("single-writer journal", () => journals().filter((j) => !plan.journals.get(j)?.multi))],
      run: ({ journal }) => {
        const [tenant, name] = split(journal);
        send([{ op: { kind: "set_leader", tenant, journal: name },
          lines: [`$ parosctl set-leader ${journal} --new <fresh uuid>`, `$ parosctl write ${journal} stale --leader <old uuid> --no-claim`] }]);
      } },
    { id: "resolve", title: "Ask where the universe sends the tenant's clients",
      parts: ["resolve", { key: "tenant", kind: "select", label: "tenant", options: () => as(tenants()), empty: "Create a tenant first." }],
      run: ({ tenant }) => send([{ op: { kind: "resolve", tenant }, lines: [`$ parosctl resolve ${tenant}`] }]) },
  ] },
  { group: "machines", label: "Machines", blocks: [
    { id: "maintenance", title: "The cell does not re-place the machine's roles for this time",
      parts: ["maintenance", machineField("machine in a cell", running, "No machine of a cell runs.", (l) => l.at(-1)),
        "for", { key: "seconds", kind: "number", label: "seconds", min: 1, max: 300, value: 30 }, "s"],
      run: ({ machine, seconds }) => send([{ op: { kind: "maintenance", machine: Number(machine), for_ms: seconds * 1000 },
        lines: [`$ parosctl cell maintenance machine-${machine} --for ${seconds}s`] }]) },
    { id: "drain", title: "The cell moves every role off the machine: each journal by a reconfiguration, a matchmaker set by a handover",
      parts: ["drain", machineField("machine in a cell", running, "No machine of a cell runs.", (l) => l.at(-1))],
      run: ({ machine }) => {
        const m = Number(machine);
        send([{ op: { kind: "drain", machine: m }, lines: [`$ parosctl cell drain-machine machine-${m}`],
          apply: (on) => on ? plan.draining.add(m) : plan.draining.delete(m) }]);
      } },
    { id: "remove", title: "Retire a drained machine",
      parts: ["remove", machineField("drained machine", () => sortNum([...plan.draining]), "Drain a machine first.")],
      run: ({ machine }) => {
        const m = Number(machine);
        send([{ op: { kind: "remove", machine: m }, lines: [`$ parosctl cell remove-machine machine-${m}`],
          apply: (on) => {
            if (on) { plan.draining.delete(m); plan.added = plan.added.filter((x) => x !== m); } else plan.draining.add(m);
          } }]);
      } },
  ] },
  { group: "faults", label: "Faults", blocks: [
    { id: "kill", title: "Crash a machine: its unsynced writes are lost. busiest is the machine that leads the most journals",
      parts: ["kill", { key: "machine", kind: "select", label: "machine", empty: "No machine of a cell runs.",
        options: () => { const up = running(); return up.length ? [["busiest", `busiest (machine-${busiest(up)})`], ...machineOptions(up)] : []; } }],
      run: ({ machine }) => {
        const m = machine === "busiest" ? busiest(running()) : Number(machine);
        send([{ op: { kind: "crash", machine: m }, fault: true, lines: [`# sim: crash machine-${m} (its unsynced writes are lost)`] }]);
      } },
    { id: "restart", title: "Start a crashed machine again on its disk",
      parts: ["restart", machineField("crashed machine", downs, "No machine is down.")],
      run: ({ machine }) => send([{ op: { kind: "restart", machine: Number(machine) }, fault: true, lines: [`# sim: restart machine-${machine} on its disk`] }]) },
    { id: "isolate", title: "Drop every message to or from a machine",
      parts: ["cut off", machineField("machine on the network", connected, "No machine to cut off.", busiest)],
      run: ({ machine }) => send([{ op: { kind: "isolate", machine: Number(machine) }, fault: true, lines: [`# sim: cut machine-${machine} from the network`] }]) },
    { id: "heal", title: "Join a cut machine to the network again",
      parts: ["heal", machineField("cut machine", cutOff, "No machine is cut off.")],
      run: ({ machine }) => send([{ op: { kind: "heal", machine: Number(machine) }, fault: true, lines: [`# sim: join machine-${machine} to the network again`] }]) },
  ] },
];

const blocks = mountBlocks($("blocks"), BLOCKS, { narrow: matchMedia("(max-width: 640px)").matches });

function refreshPicks() {
  if (!u) return;
  syncJournalPick();
  blocks.refresh();
}

// The journal the panels follow: a view control, not an action input.
function syncJournalPick() {
  const jp = $("journal-pick");
  const keys = [...plan.journals.keys()];
  for (const j of choices?.journals ?? []) if (!keys.includes(j)) keys.push(j);
  const followed = stage.selected && u.journals.get(stage.selected);
  const want = followed && !followed.tenant.internal ? `${followed.tenant.name}/${followed.name}` : jp.value;
  const shown = keys.length ? keys : [""];
  const same = shown.length === jp.options.length && shown.every((k, i) => jp.options[i].value === k);
  if (!same) jp.replaceChildren(...shown.map((k) => el("option", { value: k, textContent: k || "(none yet)" })));
  if (keys.includes(want)) jp.value = want; else if (keys.length) jp.value = keys[keys.length - 1];
}

$("journal-pick").onchange = () => {
  const key = $("journal-pick").value;
  for (const j of u.journals.values()) if (`${j.tenant.name}/${j.name}` === key) stage.select(j.key);
  blocks.refresh();
};

function renderRoles() {
  $("roles").replaceChildren(roles(u));
  const cells = u.topo?.cells?.length ?? 0;
  const down = new Set(u.topo?.down ?? []);
  const up = u.cellOf().filter((c, m) => c !== null && !down.has(m)).length;
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

// ---- the election stepper ----
//
// "Next election" waits for the next campaign of the followed journal, then
// holds the page's clock on it: each step moves the page to the next
// milestone (matchmaking, Phase 1, elected, Phase 2, chosen). The universe
// runs on for ELECTION_SPAN_MS of simulated time, so the page holds every
// milestone, then its clock stops until Resume.
let armed = null;
let stepping = null;
let heldWorker = false;

function showStepper(on) { $("stepper").hidden = !on; }

$("step-arm").onclick = () => {
  const key = stage.followed();
  if (!key) { note("the stepper follows a journal: create one first"); return; }
  stepping = null;
  armed = { key, from: shownT };
  showStepper(true);
  $("step-caption").replaceChildren(el("b", { textContent: "Waiting for an election" }),
    `of ${u.journals.get(key)?.label ?? "the journal"}. Kill its leader, or cut it off, to start one now. `
    + "Start a writer on it first to see Phase 2 too.");
  $("step-count").textContent = "";
};

$("step-next").onclick = () => { if (stepping) stepping.i = Math.min(stepping.i + 1, stepping.steps.length - 1); };
$("step-back").onclick = () => { if (stepping) stepping.i = Math.max(stepping.i - 1, 0); };
$("step-done").onclick = () => {
  armed = null;
  stepping = null;
  showStepper(false);
  if (heldWorker && !paused) worker.postMessage({ type: "pause", value: false });
  heldWorker = false;
};

// The page's time while stepping, or null when it does not step.
function stepTime(travel) {
  if (armed) {
    const start = nextCampaign(u.msgs, u.campaigns, armed.key, armed.from);
    if (start !== null && start <= last.sim) { stepping = { key: armed.key, start, i: 0, steps: [] }; armed = null; }
  }
  if (!stepping) return null;
  const end = stepping.start + ELECTION_SPAN_MS;
  if (!heldWorker && last.sim >= end) { worker.postMessage({ type: "pause", value: true }); heldWorker = true; }
  const place = u.place(stepping.key);
  const majority = place ? Math.floor(place.members.length / 2) + 1 : 2;
  stepping.steps = milestones(u.msgs, u.campaigns, stepping.key, stepping.start, majority);
  const steps = stepping.steps;
  if (!steps.length) return stepping.start;
  stepping.i = Math.min(stepping.i, steps.length - 1);
  const step = steps[stepping.i];
  $("step-caption").replaceChildren(el("b", { textContent: step.title }), step.text);
  $("step-count").textContent = `${stepping.i + 1} / ${steps.length}${last.sim < end ? " …" : ""}`;
  // A little after the milestone: its messages are in flight.
  return step.t + travel * 0.6;
}

function frame() {
  if (u) {
    const lag = TICK_MS * speed;
    const now = performance.now();
    const held = stepTime(Math.max(8, 300 * speed));
    const t = held !== null ? held : paused ? shownT : Math.min(last.sim, Math.max(shownT, last.sim - lag + (now - last.wall) * speed));
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
