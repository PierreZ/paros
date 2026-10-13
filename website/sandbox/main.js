// The sandbox page: edit a universe and its action log, run it in a worker,
// then step through where its tenants and journals live. The page draws only
// what the run returns; the run is the shipped paros code on moonpool's
// simulator.

const $ = (id) => document.getElementById(id);

const INIT = { at_ms: 0, op: { kind: "init" } };

const PRESETS = {
  "Two tenants, three journals": {
    config: { seed: 1, machines: 5, run_ms: 20000 },
    actions: [
      INIT,
      { at_ms: 1000, op: { kind: "write", tenant: "acme", journal: "orders", count: 5 } },
      { at_ms: 2000, op: { kind: "write", tenant: "acme", journal: "payments", count: 3 } },
      { at_ms: 3000, op: { kind: "write", tenant: "globex", journal: "events", count: 4 } },
      { at_ms: 4000, op: { kind: "read", tenant: "acme", journal: "orders", count: 5 } },
    ],
  },
  "One journal": {
    config: { seed: 1, machines: 3, run_ms: 10000 },
    actions: [
      INIT,
      { at_ms: 1000, op: { kind: "write", tenant: "acme", journal: "orders", count: 3 } },
      { at_ms: 2000, op: { kind: "read", tenant: "acme", journal: "orders", count: 3 } },
    ],
  },
};

let state = loadFromHash() || structuredClone(PRESETS["Two tenants, three journals"]);
let run = null;

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
worker.onmessage = ({ data }) => {
  run = JSON.parse(data.json);
  const step = $("step");
  step.max = Math.max(0, run.topology.length - 1);
  step.value = step.max;
  $("status").textContent = run.error
    ? `The run stopped: ${run.error}`
    : `Ran ${state.config.run_ms / 1000} simulated seconds in ${data.wallMs} ms of your time.`;
  setBusy(false);
  renderActions();
  renderStep();
};

function setBusy(busy) {
  for (const id of ["add", "undo", "clear", "load-preset"]) $(id).disabled = busy;
  if (busy) $("status").textContent = "Running…";
}

function start() {
  saveToHash();
  setBusy(true);
  worker.postMessage(state);
}

// ---- the URL is the replay ----

function saveToHash() {
  const bytes = new TextEncoder().encode(JSON.stringify(state));
  let text = "";
  bytes.forEach((b) => (text += String.fromCharCode(b)));
  history.replaceState(null, "", "#" + btoa(text).replaceAll("+", "-").replaceAll("/", "_"));
}

function loadFromHash() {
  if (location.hash.length < 2) return null;
  try {
    const text = atob(location.hash.slice(1).replaceAll("-", "+").replaceAll("_", "/"));
    const bytes = Uint8Array.from(text, (c) => c.charCodeAt(0));
    const loaded = JSON.parse(new TextDecoder().decode(bytes));
    if (!loaded.config || !Array.isArray(loaded.actions)) return null;
    // The log always starts with init at 0 ms, and holds no other init.
    loaded.actions = [INIT, ...loaded.actions.filter((a) => a.op.kind !== "init")];
    return loaded;
  } catch {
    return null;
  }
}

// ---- the editor ----

function describe(op) {
  switch (op.kind) {
    case "init": return "init: cell init (cell-1 over every machine), then universe init";
    case "write": return `write ${op.count} records to ${op.tenant}/${op.journal}`;
    case "read": return `read ${op.count} records of ${op.tenant}/${op.journal}`;
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
      item.textContent = `${action.at_ms} ms: ${describe(action.op)}`;
      if (result) {
        const span = document.createElement("span");
        span.className = result.ok ? "ok" : "bad";
        span.textContent = ` → ${result.detail} (${result.start_ms}–${result.end_ms} ms)`;
        item.append(span);
      }
      if (index > 0) {
        const remove = document.createElement("button");
        remove.textContent = "×";
        remove.title = "Remove this action";
        remove.onclick = () => { state.actions.splice(index, 1); start(); };
        item.append(remove);
      }
      return item;
    }),
  );
  const last = state.actions.at(-1);
  $("at").value = Math.min(last.at_ms + 1000, state.config.run_ms);
  const tenants = new Set(), journals = new Set();
  for (const a of state.actions) {
    if (a.op.tenant) tenants.add(a.op.tenant);
    if (a.op.journal) journals.add(a.op.journal);
  }
  $("tenants").replaceChildren(...[...tenants].map((t) => new Option(t)));
  $("journals").replaceChildren(...[...journals].map((j) => new Option(j)));
}

function readConfig() {
  state.config = {
    seed: Math.max(0, Math.floor(Number($("seed").value) || 0)),
    machines: clamp(Number($("machines").value), 1, 7),
    run_ms: clamp(Math.round(Number($("run").value) * 1000), 1000, 300000),
  };
  state.actions = state.actions.filter((a) => a.at_ms <= state.config.run_ms);
  renderConfig();
  start();
}

function clamp(value, low, high) {
  return Math.min(high, Math.max(low, Number.isFinite(value) ? Math.round(value) : low));
}

function addAction() {
  const name = (id) => $(id).value.trim();
  if (!name("tenant") || !name("journal")) {
    $("status").textContent = "Name a tenant and a journal.";
    return;
  }
  const op = {
    kind: $("op").value,
    tenant: name("tenant"),
    journal: name("journal"),
    count: clamp(Number($("count").value), 1, 100),
  };
  const at_ms = clamp(Number($("at").value), 1, state.config.run_ms);
  state.actions.push({ at_ms, op });
  state.actions.sort((x, y) => x.at_ms - y.at_ms);
  start();
}

// ---- what the run shows ----

function renderStep() {
  if (!run) return;
  const topology = run.topology[Number($("step").value)];
  if (!topology) {
    $("after").textContent = "Nothing to show: the run read no topology.";
    $("spread").replaceChildren();
    renderLog(0);
    return;
  }
  const action = state.actions[topology.step];
  $("after").textContent =
    `After step ${topology.step}, ${action ? describe(action.op) : ""}, read at ${topology.time_ms} ms.`;
  renderSpread(topology);
  renderLog(topology.time_ms);
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  Object.assign(node, attrs);
  node.append(...children);
  return node;
}

function mark(kind, text, title) {
  return el("span", { className: `mark ${kind}`, textContent: text, title });
}

function renderSpread(topology) {
  const machines = topology.machines;
  const load = machines.map(() => 0);
  const table = $("spread");
  table.replaceChildren(el("tr", {},
    el("th", { className: "name", textContent: "Tenant / journal" }),
    ...machines.map((node, i) => el("th", {}, `m${i}`, el("small", { textContent: node ?? "idle" }))),
    el("th", { textContent: "Chosen" })));
  for (const tenant of topology.tenants) {
    const head = el("td", { colSpan: machines.length + 2 }, tenant.name,
      el("span", { textContent: `${tenant.groups} · ${tenant.cell}` }));
    table.append(el("tr", { className: "tenant" }, head));
    for (const journal of tenant.journals) {
      const row = el("tr", {}, el("td", { className: "name", textContent: journal.name }));
      machines.forEach((_, m) => {
        const cell = el("td");
        if (journal.leader === m) cell.append(mark("leader", "L", "leader and acceptor"));
        else if (journal.members.includes(m)) cell.append(mark("member", "A", "acceptor"));
        else if (journal.matchmakers.includes(m)) cell.append(mark("mm", "M", "matchmaker only"));
        if (journal.members.includes(m)) load[m] += 1;
        row.append(cell);
      });
      row.append(el("td", { textContent: journal.chosen ?? "–" }));
      table.append(row);
    }
  }
  table.append(el("tr", { className: "load" },
    el("td", { className: "name", textContent: "acceptors per machine" }),
    ...load.map((n) => el("td", { textContent: String(n) })),
    el("td")));
}

function short(node) {
  return BigInt(node).toString(16).padStart(16, "0").slice(0, 6);
}

function renderLog(t) {
  const showMsgs = $("show-msgs").checked;
  const lines = [];
  for (let i = run.events.length - 1; i >= 0 && lines.length < 200; i--) {
    const e = run.events[i];
    if (e.time_ms > t || (!showMsgs && e.name === "msg_sent")) continue;
    const who = e.machine !== null ? `m${e.machine}` : e.name === "operator_step" ? "op" : "sim";
    const fields = e.fields.map(([k, v]) => `${k}=${/^\d{10,}$/.test(v) ? short(v) : v}`).join(" ");
    lines.push(`${String(e.time_ms).padStart(6)} ms  ${who.padEnd(4)} ${e.name}  ${fields}`);
  }
  $("log").textContent = lines.join("\n");
}

function legend() {
  $("legend").replaceChildren(
    mark("leader", "L"), "leads the journal (and accepts) ",
    mark("member", "A"), "acceptor ",
    mark("mm", "M"), "in the tenant's matchmaker set only. Chosen: the highest position its leader knows chosen.");
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
$("undo").onclick = () => { if (state.actions.length > 1) state.actions.pop(); start(); };
$("clear").onclick = () => { state.actions = [INIT]; start(); };
$("step").oninput = renderStep;
$("prev").onclick = () => { $("step").stepDown(); renderStep(); };
$("next").onclick = () => { $("step").stepUp(); renderStep(); };
$("show-msgs").onchange = renderStep;

legend();
renderConfig();
renderActions();
start();
