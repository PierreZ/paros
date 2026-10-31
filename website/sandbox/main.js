// The sandbox page: edit a universe and its action log, run it in a worker,
// then watch it play in the cluster view. The page draws only what the run
// returns; the run is the shipped paros code on moonpool's simulator.

import { BEATS, Stage, drawTimeline, el, explain, index, legend, lowerBound, role } from "./stage.js";

const $ = (id) => document.getElementById(id);

const init = (machines) => ({ at_ms: 0, op: { kind: "init", machines } });

const PRESETS = {
  "Three acts: cell, tenant, writes": {
    config: { seed: 1, machines: 7, run_ms: 12000 },
    actions: [
      init(7),
      { at_ms: 3000, op: { kind: "create_tenant", tenant: "acme" } },
      { at_ms: 4000, op: { kind: "create_journal", tenant: "acme", journal: "orders", mode: "machines=0,partitions=3" } },
      { at_ms: 5000, op: { kind: "create_journal", tenant: "acme", journal: "audit", mode: "machines=1" } },
      { at_ms: 7000, op: { kind: "write", tenant: "acme", journal: "orders", count: 12 } },
      { at_ms: 8000, op: { kind: "write", tenant: "acme", journal: "audit", count: 4 } },
      { at_ms: 9000, op: { kind: "read", tenant: "acme", journal: "orders", count: 12 } },
    ],
  },
  "Crash the leader, then restart it": {
    config: { seed: 1, machines: 5, run_ms: 30000 },
    actions: [
      init(5),
      { at_ms: 3000, op: { kind: "create_tenant", tenant: "acme" } },
      { at_ms: 4000, op: { kind: "create_journal", tenant: "acme", journal: "orders", mode: "machines=1" } },
      { at_ms: 6000, op: { kind: "write", tenant: "acme", journal: "orders", count: 4 } },
      { at_ms: 9000, op: { kind: "crash", machine: 0 } },
      { at_ms: 10000, op: { kind: "write", tenant: "acme", journal: "orders", count: 4 } },
      { at_ms: 20000, op: { kind: "restart", machine: 0 } },
      { at_ms: 24000, op: { kind: "read", tenant: "acme", journal: "orders", count: 8 } },
    ],
  },
  "Cut a machine off, then heal it": {
    config: { seed: 2, machines: 5, run_ms: 30000 },
    actions: [
      init(5),
      { at_ms: 3000, op: { kind: "create_tenant", tenant: "acme" } },
      { at_ms: 4000, op: { kind: "create_journal", tenant: "acme", journal: "orders", mode: "machines=1" } },
      { at_ms: 6000, op: { kind: "isolate", machine: 1 } },
      { at_ms: 7000, op: { kind: "write", tenant: "acme", journal: "orders", count: 6 } },
      { at_ms: 15000, op: { kind: "heal", machine: 1 } },
      { at_ms: 20000, op: { kind: "read", tenant: "acme", journal: "orders", count: 6 } },
    ],
  },
  "Two tenants": {
    config: { seed: 1, machines: 6, run_ms: 12000 },
    actions: [
      init(6),
      { at_ms: 3000, op: { kind: "create_tenant", tenant: "acme" } },
      { at_ms: 3500, op: { kind: "create_journal", tenant: "acme", journal: "orders", mode: "machines=1" } },
      { at_ms: 4000, op: { kind: "create_tenant", tenant: "globex" } },
      { at_ms: 4500, op: { kind: "create_journal", tenant: "globex", journal: "events", mode: "machines=2" } },
      { at_ms: 7000, op: { kind: "write", tenant: "acme", journal: "orders", count: 5 } },
      { at_ms: 8000, op: { kind: "write", tenant: "globex", journal: "events", count: 5 } },
    ],
  },
};

const CAPTIONS = [
  "The cell runs itself. paros keeps its own records in journals: the universe directory, the machine registry and the election journal. Grey dots are that internal traffic.",
  "A tenant arrives. Its control journal and its journals land on machines, one zone at a time. Each journal is its own Paxos instance and elects its own leader.",
  "Writes flow. A client outside the cell sends each write to the journal's leader. The leader asks a Phase-2 quorum to accept it; then the record is chosen.",
];

// Replay mode: a run opened from a file or pasted is stepped, not edited.
let replaying = false;
let state = loadFromHash() || structuredClone(PRESETS["Three acts: cell, tenant, writes"]);
let run = null;
let ix = null;
let t = 0;
let playing = false;
let stepped = null;
let lastFrame = 0;
let lastLog = 0;

const stage = new Stage({
  stage: $("stage"), wires: $("wires"), cell: $("cell"), client: $("client"), rail: $("rail"),
}, () => render(true));

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
worker.onmessage = ({ data }) => {
  run = JSON.parse(data.json);
  $("status").textContent = run.error
    ? `The run stopped: ${run.error}`
    : `Ran ${state.config.run_ms / 1000} simulated seconds in ${data.wallMs} ms of your time.`;
  setBusy(false);
  renderActions();
  if (!run.topology) return;
  ix = index(run);
  stage.load(ix);
  $("legend").replaceChildren(...legend(ix));
  renderActs();
  t = 0;
  stepped = null;
  playing = true;
  render(true);
};

function setBusy(busy) {
  for (const id of ["add", "undo", "clear", "load-preset", "quick-tenant", "quick-journals", "quick-traffic", "quick-machine"]) {
    $(id).disabled = busy || replaying;
  }
  if (busy) $("status").textContent = "Running…";
}

function start() {
  saveToHash();
  setBusy(true);
  playing = false;
  worker.postMessage(state);
}

// ---- the URL is the replay ----

function saveToHash() {
  const bytes = new TextEncoder().encode(JSON.stringify(state));
  let text = "";
  bytes.forEach((b) => (text += String.fromCharCode(b)));
  history.replaceState(null, "", `#${replaying ? "replay:" : ""}` + btoa(text).replaceAll("+", "-").replaceAll("/", "_"));
}

function loadFromHash() {
  if (location.hash.length < 2) return null;
  try {
    // `#replay:<run>` opens the run in replay mode.
    let hash = location.hash.slice(1);
    if (hash.startsWith("replay:")) { replaying = true; hash = hash.slice(7); }
    const text = atob(hash.replaceAll("-", "+").replaceAll("_", "/"));
    const bytes = Uint8Array.from(text, (c) => c.charCodeAt(0));
    const loaded = JSON.parse(new TextDecoder().decode(bytes));
    if (!loaded.config || !Array.isArray(loaded.actions)) return null;
    // The log always starts with init at 0 ms, and holds no other init.
    const first = loaded.actions.find((a) => a.op.kind === "init");
    const machines = Number(first?.op.machines) || loaded.config.machines || 3;
    loaded.actions = [init(machines), ...loaded.actions.filter((a) => a.op.kind !== "init")];
    return loaded;
  } catch {
    return null;
  }
}

// ---- the editor ----

function describe(op) {
  switch (op.kind) {
    case "init": return `init: cell init (cell-1 over m0–m${op.machines - 1}), then universe init`;
    case "create_tenant": return `create tenant ${op.tenant}`;
    case "create_journal": return `create journal ${op.tenant}/${op.journal} (${op.mode || "machines=1"})`;
    case "write": return `write ${op.count} records to ${op.tenant}/${op.journal}`;
    case "read": return `read ${op.count} records of ${op.tenant}/${op.journal}`;
    case "traffic": return `traffic: write ${op.count} records to every journal at the same time`;
    case "add_machine": return `add m${op.machine} to cell-1 (cell add-machine)`;
    case "crash": return `crash m${op.machine}`;
    case "restart": return `restart m${op.machine}`;
    case "isolate": return `cut m${op.machine} from the network`;
    case "heal": return `join m${op.machine} to the network again`;
    default: return op.kind;
  }
}

function renderConfig() {
  $("seed").value = state.config.seed;
  $("machines").value = state.config.machines;
  $("run").value = state.config.run_ms / 1000;
}

function renderActions() {
  const results = new Map((run?.steps || []).map((s) => [s.index, s]));
  $("actions").replaceChildren(
    ...state.actions.map((action, index) => {
      const item = document.createElement("li");
      const result = results.get(index);
      if (index === 0) {
        const count = el("input", { type: "number", min: 1, max: state.config.machines,
          value: action.op.machines, title: "How many machines init lists" });
        count.onchange = () => {
          action.op.machines = clamp(Number(count.value), 1, state.config.machines);
          start();
        };
        item.append("0 ms: init over ", count, " machines: cell init, then universe init");
      } else {
        item.textContent = `${action.at_ms} ms: ${describe(action.op)}`;
      }
      if (result) {
        const span = el("span", { className: result.ok ? "ok" : "bad",
          textContent: ` → ${result.detail} (${result.start_ms}–${result.end_ms} ms)` });
        item.append(span);
      }
      if (index > 0 && !replaying) {
        const remove = el("button", { textContent: "×", title: "Remove this action" });
        remove.onclick = () => { state.actions.splice(index, 1); start(); };
        item.append(remove);
      }
      return item;
    }),
  );
  const last = state.actions.at(-1);
  $("at").value = Math.min(last.at_ms + 1000, state.config.run_ms);
  renderPicks();
}

// The tenants and journals the log creates: what writes and reads can name.
function created() {
  const tenants = [], journals = [];
  for (const a of state.actions) {
    if (a.op.kind === "create_tenant") tenants.push(a.op.tenant);
    if (a.op.kind === "create_journal") journals.push(`${a.op.tenant}/${a.op.journal}`);
  }
  return { tenants, journals };
}

function renderPicks() {
  const { tenants, journals } = created();
  const fill = (select, items) => {
    const old = select.value;
    select.replaceChildren(...items.map((x) => new Option(x, x)));
    if (items.includes(old)) select.value = old;
  };
  fill($("tenant-pick"), tenants);
  fill($("journal-pick"), journals);
  fill($("machine-pick"), Array.from({ length: state.config.machines }, (_, m) => String(m)));
  renderOpFields();
}

function renderOpFields() {
  const kind = $("op").value;
  $("tenant-name-field").hidden = kind !== "create_tenant";
  $("tenant-pick-field").hidden = kind !== "create_journal";
  $("journal-name-field").hidden = kind !== "create_journal";
  $("mode-field").hidden = kind !== "create_journal";
  $("journal-pick-field").hidden = !["write", "read"].includes(kind);
  $("count-field").hidden = !["write", "read", "traffic"].includes(kind);
  $("machine-pick-field").hidden = !FAULTS.has(kind) && kind !== "add_machine";
}

const FAULTS = new Set(["crash", "restart", "isolate", "heal"]);

function readConfig() {
  state.config = {
    seed: Math.max(0, Math.floor(Number($("seed").value) || 0)),
    machines: clamp(Number($("machines").value), 1, 7),
    run_ms: clamp(Math.round(Number($("run").value) * 1000), 1000, 300000),
  };
  state.actions = state.actions.filter((a) => a.at_ms <= state.config.run_ms);
  const first = state.actions[0].op;
  first.machines = Math.min(first.machines, state.config.machines);
  renderConfig();
  start();
}

function clamp(value, low, high) {
  return Math.min(high, Math.max(low, Number.isFinite(value) ? Math.round(value) : low));
}

function addAction() {
  const kind = $("op").value;
  const text = (id) => $(id).value.trim();
  let op;
  if (kind === "create_tenant") {
    if (!text("tenant-name")) { $("status").textContent = "Name the tenant."; return; }
    op = { kind, tenant: text("tenant-name") };
  } else if (kind === "create_journal") {
    if (!text("tenant-pick")) { $("status").textContent = "Create a tenant first."; return; }
    if (!text("journal-name")) { $("status").textContent = "Name the journal."; return; }
    op = { kind, tenant: text("tenant-pick"), journal: text("journal-name"), mode: $("mode").value };
  } else if (kind === "traffic") {
    if (!created().journals.length) { $("status").textContent = "Create a journal first."; return; }
    op = { kind, count: clamp(Number($("count").value), 1, 100) };
  } else if (FAULTS.has(kind) || kind === "add_machine") {
    op = { kind, machine: clamp(Number($("machine-pick").value), 0, state.config.machines - 1) };
  } else {
    const pick = text("journal-pick");
    if (!pick) { $("status").textContent = "Create a journal first."; return; }
    const [tenant, journal] = pick.split("/");
    op = { kind, tenant, journal, count: clamp(Number($("count").value), 1, 100) };
  }
  const at_ms = clamp(Number($("at").value), 1, state.config.run_ms);
  state.actions.push({ at_ms, op });
  state.actions.sort((x, y) => x.at_ms - y.at_ms);
  start();
}

// ---- the one-click steps ----
// Each button adds steps after the last one, as the form would, so the run
// stays a pure function of its seed, its configuration and its log.

const TENANT_NAMES = ["acme", "globex", "initech"];
const JOURNAL_NAMES = ["orders", "audit", "events", "payments", "invoices", "metrics"];

// The first name of `names` not in `taken`, then `<prefix>-2`, `<prefix>-3`, …
function freeName(names, taken, prefix) {
  const free = names.find((n) => !taken.includes(n));
  if (free) return free;
  for (let i = 2; ; i++) if (!taken.includes(`${prefix}-${i}`)) return `${prefix}-${i}`;
}

// Add `ops` one second apart after the last step, then run the log again.
function quick(ops) {
  let at = state.actions.at(-1).at_ms;
  for (const op of ops) {
    at += 1000;
    if (at > state.config.run_ms) {
      $("status").textContent = "The run ends before these steps: make the run longer.";
      return;
    }
    state.actions.push({ at_ms: at, op });
  }
  start();
}

function newTenant() {
  return { kind: "create_tenant", tenant: freeName(TENANT_NAMES, created().tenants, "tenant") };
}

// Three journals in the last tenant; a tenant first when there is none.
function newJournals(how = 3) {
  const ops = [];
  let tenant = created().tenants.at(-1);
  if (!tenant) { const op = newTenant(); ops.push(op); tenant = op.tenant; }
  const taken = created().journals.filter((j) => j.startsWith(`${tenant}/`)).map((j) => j.split("/")[1]);
  for (let i = 0; i < how; i++) {
    const journal = freeName(JOURNAL_NAMES, taken, "journal");
    taken.push(journal);
    ops.push({ kind: "create_journal", tenant, journal, mode: $("mode").value });
  }
  return ops;
}

// The next machine outside cell-1: neither in init nor added by the log.
function idleMachine() {
  const inCell = new Set(Array.from({ length: state.actions[0].op.machines }, (_, m) => m));
  for (const a of state.actions) if (a.op.kind === "add_machine") inCell.add(a.op.machine);
  for (let m = 0; m < state.config.machines; m++) if (!inCell.has(m)) return m;
  return null;
}

$("quick-tenant").onclick = () => quick([newTenant()]);
$("quick-journals").onclick = () => quick(newJournals());
$("quick-traffic").onclick = () => {
  const ops = created().journals.length ? [] : newJournals(2);
  ops.push({ kind: "traffic", count: clamp(Number($("count").value), 1, 100) });
  quick(ops);
};
$("quick-machine").onclick = () => {
  let machine = idleMachine();
  if (machine === null) {
    if (state.config.machines >= 7) {
      $("status").textContent = "Every machine is in cell-1: a universe holds 7 machines at most.";
      return;
    }
    // One more machine in the universe: it starts idle. The simulated
    // draws change with it, so the run before this step can change too.
    machine = state.config.machines;
    state.config.machines += 1;
    renderConfig();
  }
  quick([{ kind: "add_machine", machine }]);
};

// ---- playback ----

function speed() { return Number($("speed").value); }

// A dot crosses the stage in about half a second of your time.
function travel() { return Math.min(150, Math.max(3, 500 * speed())); }

function actAt(time) {
  let current = 0;
  ix.acts.forEach((a, i) => { if (a.start !== null && time >= a.start) current = i; });
  return current;
}

function renderActs() {
  $("acts").replaceChildren(...ix.acts.map((act, i) => {
    const b = el("button", { className: "act", textContent: `${"①②③"[i]} ${act.title}`, disabled: act.start === null });
    b.onclick = () => { t = act.start; stepped = null; playing = true; render(true); };
    return b;
  }), el("span", { className: "time", id: "act-time" }));
}

function caption() {
  if (stepped !== null) return explain(ix, ix.msgs[stepped], t);
  const act = actAt(t);
  let now = "";
  for (const s of run.steps) {
    if (s.start_ms <= t) {
      const a = state.actions[s.index];
      const flavour = a.op.kind === "init" ? " (single-decree Paxos chooses the cell plan)" : "";
      now = s.end_ms <= t
        ? `Step ${s.index}, ${describe(a.op)}${flavour}: ${s.detail}.`
        : `Step ${s.index} is running: ${describe(a.op)}${flavour}.`;
    }
  }
  return `${CAPTIONS[act]} ${now}`;
}

function render(full) {
  if (!ix) return;
  stage.cards(t);
  stage.draw(t, travel(), $("show-beats").checked, stepped);
  stage.rail(t);
  $("caption").textContent = caption();
  const act = actAt(t);
  document.querySelectorAll(".act").forEach((b, i) => b.classList.toggle("on", i === act));
  $("time").textContent = `t = ${(t / 1000).toFixed(3)} s of ${(ix.end / 1000).toFixed(1)} s`;
  $("play").textContent = playing ? "⏸" : "▶";
  drawTimeline($("timeline"), ix, t);
  if (full || performance.now() - lastLog > 400) {
    renderLog(t);
    lastLog = performance.now();
  }
}

function frame(now) {
  const dt = lastFrame ? now - lastFrame : 0;
  lastFrame = now;
  if (playing && ix) {
    t = Math.min(ix.end, t + dt * speed());
    if (t >= ix.end) playing = false;
    render(false);
  }
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);

// One message at a time: the selected journal's, heartbeats left out.
function stepMessage(direction) {
  if (!ix) return;
  playing = false;
  const keep = (m) => !BEATS.has(m.kind) && (!stage.selected || m.key === stage.selected);
  let i = stepped ?? lowerBound(ix.msgs, t, (m) => m.t) - (direction > 0 ? 1 : 0);
  do { i += direction; } while (i >= 0 && i < ix.msgs.length && !keep(ix.msgs[i]));
  if (i < 0 || i >= ix.msgs.length) return;
  stepped = i;
  t = ix.msgs[i].t + travel() * 0.6;
  render(true);
}

function scrub(event) {
  if (!ix) return;
  const r = $("timeline").getBoundingClientRect();
  t = Math.max(0, Math.min(ix.end, ((event.clientX - r.left) / r.width) * ix.end));
  stepped = null;
  render(true);
}

function renderLog(time) {
  const showMsgs = $("show-msgs").checked;
  const lines = [];
  for (let i = run.events.length - 1; i >= 0 && lines.length < 150; i--) {
    const e = run.events[i];
    if (e.time_ms > time || (!showMsgs && e.name === "msg_sent")) continue;
    const who = e.machine !== null ? `m${e.machine}` : e.name === "operator_step" || e.name.startsWith("client") ? "op" : "sim";
    const fields = e.fields.map(([k, v]) => `${k}=${/^\d{10,}$/.test(v) ? BigInt(v).toString(16).padStart(16, "0").slice(0, 6) : v}`).join(" ");
    const flavour = role(ix, e, e.time_ms);
    lines.push(`${String(e.time_ms).padStart(6)} ms  ${who.padEnd(4)} ${flavour ? `[${flavour}] ` : ""}${e.name}  ${fields}`);
  }
  $("log").textContent = lines.join("\n");
}

// One step of the log at a time: jump to where the step starts, paused.
function stepAction(direction) {
  if (!ix || !run.steps.length) return;
  playing = false;
  stepped = null;
  const starts = run.steps.map((s) => s.start_ms);
  let i = direction > 0 ? starts.findIndex((x) => x > t + 1) : starts.findLastIndex((x) => x < t - 1);
  if (i < 0) i = direction > 0 ? starts.length - 1 : 0;
  t = starts[i];
  renderReplaying(i);
  render(true);
}

// ---- replay ----

function renderReplaying(at) {
  $("replaying").hidden = !replaying;
  $("edit-run").hidden = !replaying;
  for (const id of ["add", "undo", "clear", "load-preset", "seed", "machines", "run"]) $(id).disabled = replaying;
  $("quick").hidden = replaying;
  if (!replaying) return;
  const steps = state.actions.length;
  const where = at === undefined ? "" : ` Step ${at} of ${steps - 1}: ${describe(state.actions[at].op)}.`;
  $("replaying").textContent =
    `Replaying seed ${state.config.seed} over ${state.config.machines} machines, ${steps} steps. Use “◀ step” and “step ▶” to go through the log.${where}`;
}

// A saved or pasted run: `{config, actions}`, or a whole run view.
function openRun(text) {
  let loaded;
  try { loaded = JSON.parse(text); } catch { $("status").textContent = "That is not JSON."; return; }
  if (!loaded || !loaded.config || !Array.isArray(loaded.actions)) {
    $("status").textContent = "A run holds a config and a list of actions.";
    return;
  }
  state = { config: loaded.config, actions: loaded.actions };
  replaying = true;
  renderConfig();
  renderReplaying();
  start();
}

function saveRun() {
  const blob = new Blob([JSON.stringify(state, null, 2)], { type: "application/json" });
  const a = el("a", { href: URL.createObjectURL(blob), download: `paros-run-seed-${state.config.seed}.json` });
  document.body.append(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

// ---- wiring ----

$("preset").replaceChildren(...Object.keys(PRESETS).map((name) => new Option(name, name)));
$("load-preset").onclick = () => {
  state = structuredClone(PRESETS[$("preset").value]);
  renderConfig();
  start();
};
for (const name of ["seed", "machines", "run"]) $(name).onchange = readConfig;
$("add").onclick = addAction;
$("prev-step").onclick = () => stepAction(-1);
$("next-step").onclick = () => stepAction(1);
$("copy-link").onclick = async () => {
  saveToHash();
  try { await navigator.clipboard.writeText(location.href); $("status").textContent = "The link is copied."; }
  catch { $("status").textContent = "Copy the address bar: it holds the run."; }
};
$("save-run").onclick = saveRun;
$("open-run").onchange = async (e) => { const file = e.target.files[0]; if (file) openRun(await file.text()); e.target.value = ""; };
$("replay-paste").onclick = () => openRun($("paste-run").value);
$("edit-run").onclick = () => { replaying = false; renderReplaying(); renderActions(); };
$("undo").onclick = () => { if (state.actions.length > 1) state.actions.pop(); start(); };
$("clear").onclick = () => { state.actions = [state.actions[0]]; start(); };
$("op").onchange = renderOpFields;
$("play").onclick = () => { if (ix && t >= ix.end) t = 0; playing = !playing; stepped = null; render(true); };
$("prev-msg").onclick = () => stepMessage(-1);
$("next-msg").onclick = () => stepMessage(1);
$("show-msgs").onchange = () => renderLog(t);
$("show-beats").onchange = () => render(false);
$("speed").onchange = () => render(false);
let dragging = false;
$("timeline").onpointerdown = (e) => { dragging = true; playing = false; $("timeline").setPointerCapture(e.pointerId); scrub(e); };
$("timeline").onpointermove = (e) => { if (dragging) scrub(e); };
$("timeline").onpointerup = () => { dragging = false; };
document.addEventListener("keydown", (e) => {
  if (e.target.closest("input, select, textarea")) return;
  if (e.key === " ") { e.preventDefault(); $("play").click(); }
  if (e.key === "[") stepMessage(-1);
  if (e.key === "]") stepMessage(1);
  if (e.key === ",") stepAction(-1);
  if (e.key === ".") stepAction(1);
});
window.addEventListener("resize", () => { if (ix) { stage.layout(); render(false); } });
window.addEventListener("paros-theme", () => { if (ix) render(true); });

renderConfig();
renderActions();
renderReplaying();
start();
