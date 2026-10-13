// The sandbox page: edit a universe and its action log, run it in a worker,
// then scrub through what the machines did. The page draws only what the run
// returns; the run is the shipped paros code on moonpool's simulator.

const $ = (id) => document.getElementById(id);

const PRESETS = {
  "Form a cell and write": {
    config: { seed: 1, machines: 3, founders: 3, run_ms: 10000 },
    actions: [
      { at_ms: 0, op: { kind: "init" } },
      { at_ms: 1000, op: { kind: "write", value: "a" } },
      { at_ms: 2000, op: { kind: "write", value: "b" } },
    ],
  },
  "Crash a leader, add a machine": {
    config: { seed: 1, machines: 4, founders: 3, run_ms: 15000 },
    actions: [
      { at_ms: 0, op: { kind: "init" } },
      { at_ms: 1000, op: { kind: "write", value: "a" } },
      { at_ms: 5000, op: { kind: "crash", machine: 0 } },
      { at_ms: 6000, op: { kind: "write", value: "b" } },
      { at_ms: 7000, op: { kind: "admit", machine: 3 } },
      { at_ms: 11000, op: { kind: "restart", machine: 0 } },
    ],
  },
  "Cut one machine off": {
    config: { seed: 7, machines: 3, founders: 3, run_ms: 15000 },
    actions: [
      { at_ms: 0, op: { kind: "init" } },
      { at_ms: 1000, op: { kind: "write", value: "a" } },
      { at_ms: 4000, op: { kind: "partition", a: 0, b: 1 } },
      { at_ms: 4000, op: { kind: "partition", a: 0, b: 2 } },
      { at_ms: 5000, op: { kind: "write", value: "b" } },
      { at_ms: 9000, op: { kind: "heal", a: 0, b: 1 } },
      { at_ms: 9000, op: { kind: "heal", a: 0, b: 2 } },
    ],
  },
};

const KIND_COLOURS = {
  prepare: "#c97a2b", promise: "#b8860b", accept: "#2f5fb3", accepted: "#3b8bd9",
  commit: "#3b6e47", nack: "#9b3434", heartbeat: "#8a8f99",
};

let state = loadFromHash() || structuredClone(PRESETS["Crash a leader, add a machine"]);
let run = null;
let model = null;
let playing = null;

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
worker.onmessage = ({ data }) => {
  run = JSON.parse(data.json);
  model = buildModel(run);
  const time = $("time");
  time.max = state.config.run_ms;
  if (Number(time.value) > state.config.run_ms) time.value = state.config.run_ms;
  $("status").textContent = run.error
    ? `The run stopped: ${run.error}`
    : `Ran ${state.config.run_ms / 1000} simulated seconds in ${data.wallMs} ms of your time.`;
  setBusy(false);
  renderActions();
  renderTime();
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
    return loaded.config && Array.isArray(loaded.actions) ? loaded : null;
  } catch {
    return null;
  }
}

// ---- the editor ----

function describe(op) {
  switch (op.kind) {
    case "init": return "init: form the cell over the founding members";
    case "write": return `write "${op.value}"`;
    case "admit": return `add machine-${op.machine} to the cell`;
    case "crash": return `crash machine-${op.machine}`;
    case "restart": return `restart machine-${op.machine}`;
    case "partition": return `cut machine-${op.a} from machine-${op.b}`;
    case "heal": return `heal machine-${op.a} and machine-${op.b}`;
    default: return op.kind;
  }
}

function renderConfig() {
  $("seed").value = state.config.seed;
  $("machines").value = state.config.machines;
  $("founders").value = state.config.founders;
  $("run").value = state.config.run_ms / 1000;
  for (const id of ["a", "b"]) {
    const select = $(id);
    const old = select.value;
    select.replaceChildren(
      ...Array.from({ length: state.config.machines }, (_, i) => new Option(`machine-${i}`, i)),
    );
    if (old !== "" && Number(old) < state.config.machines) select.value = old;
  }
  if ($("b").value === $("a").value && state.config.machines > 1) $("b").value = 1;
  renderOpFields();
}

function renderOpFields() {
  const kind = $("op").value;
  $("value-field").hidden = kind !== "write";
  $("a-field").hidden = ["init", "write"].includes(kind);
  $("b-field").hidden = !["partition", "heal"].includes(kind);
}

function renderActions() {
  const results = new Map((run?.steps || []).map((s) => [s.index, s]));
  const list = $("actions");
  list.replaceChildren(
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
      const remove = document.createElement("button");
      remove.textContent = "×";
      remove.title = "Remove this action";
      remove.onclick = () => { state.actions.splice(index, 1); start(); };
      item.append(remove);
      return item;
    }),
  );
  const last = state.actions.at(-1);
  $("at").value = last ? Math.min(last.at_ms + 1000, state.config.run_ms) : 0;
}

function readConfig() {
  const machines = clamp(Number($("machines").value), 1, 7);
  state.config = {
    seed: Math.max(0, Math.floor(Number($("seed").value) || 0)),
    machines,
    founders: clamp(Number($("founders").value), 1, machines),
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
  const kind = $("op").value;
  const at_ms = clamp(Number($("at").value), 0, state.config.run_ms);
  const a = Number($("a").value);
  const b = Number($("b").value);
  let op;
  if (kind === "write") op = { kind, value: $("value").value || "x" };
  else if (kind === "partition" || kind === "heal") {
    if (a === b) { $("status").textContent = "Pick two different machines."; return; }
    op = { kind, a, b };
  } else if (kind === "init") op = { kind };
  else op = { kind, machine: a };
  state.actions.push({ at_ms, op });
  state.actions.sort((x, y) => x.at_ms - y.at_ms);
  start();
}

// ---- what the run shows ----

function buildModel(run) {
  const machines = run.config.machines;
  const nodeToMachine = new Map();
  for (const e of run.events) {
    const node = field(e, "node");
    if (node && e.machine !== null) nodeToMachine.set(node, e.machine);
  }
  const ipToMachine = (ip) => {
    const m = /^10\.0\.1\.(\d+)$/.exec(ip || "");
    return m ? Number(m[1]) - 1 : null;
  };
  const messages = [];
  const timeline = Array.from({ length: machines }, () => []);
  for (const e of run.events) {
    if (e.name === "msg_sent") {
      const to = nodeToMachine.get((field(e, "to") || "").replace(/^node:/, ""));
      if (to !== undefined && e.machine !== null && to !== e.machine) {
        messages.push({ t: e.time_ms, from: e.machine, to, kind: field(e, "kind") });
      }
      continue;
    }
    let m = e.machine;
    if (e.name === "sim_fault") m = ipToMachine(field(e, "ip"));
    if (m !== null && m !== undefined && m < machines) timeline[m].push(e);
  }
  return { nodeToMachine, messages, timeline };
}

function field(event, key) {
  const pair = event.fields.find(([k]) => k === key);
  return pair ? pair[1] : undefined;
}

function machineAt(index, t) {
  const s = { phase: "idle", up: true, leads: 0, chosen: 0, node: null, round: null, last: null };
  for (const node of model.nodeToMachine.keys()) {
    if (model.nodeToMachine.get(node) === index) s.node = node;
  }
  for (const e of model.timeline[index]) {
    if (e.time_ms > t) break;
    s.last = e;
    switch (e.name) {
      case "machine_serving": s.phase = "formed"; break;
      case "machine_admitted":
      case "machine_serving_admitted_journals": s.phase = "admitted"; break;
      case "leader_elected": s.round = field(e, "round"); s.leads += 1; break;
      case "leadership_resigned":
      case "leader_quorum_lost": s.leads = Math.max(0, s.leads - 1); break;
      case "value_chosen": s.chosen += 1; break;
      case "sim_fault": {
        const kind = field(e, "kind");
        if (kind === "process_force_kill") { s.up = false; s.leads = 0; }
        if (kind === "process_restart") s.up = true;
        break;
      }
    }
  }
  return s;
}

function partitionsAt(t) {
  const cut = new Set();
  for (const a of state.actions) {
    if (a.at_ms > t) break;
    const key = [a.op.a, a.op.b].sort().join("-");
    if (a.op.kind === "partition") cut.add(key);
    if (a.op.kind === "heal") cut.delete(key);
  }
  return cut;
}

const SVG = "http://www.w3.org/2000/svg";
function svg(tag, attrs, text) {
  const el = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  if (text !== undefined) el.textContent = text;
  return el;
}

function colourOf(s) {
  const css = getComputedStyle(document.documentElement);
  if (!s.up) return css.getPropertyValue("--bad");
  if (s.phase === "formed") return css.getPropertyValue("--ok");
  if (s.phase === "admitted") return css.getPropertyValue("--admitted");
  return css.getPropertyValue("--idle");
}

function renderTime() {
  if (!model) return;
  const t = Number($("time").value);
  $("clock").textContent = `${t} ms`;
  const n = run.config.machines;
  const centre = { x: 320, y: 210 };
  const radius = n === 1 ? 0 : 150;
  const at = (i) => ({
    x: centre.x + radius * Math.cos((2 * Math.PI * i) / n - Math.PI / 2),
    y: centre.y + radius * Math.sin((2 * Math.PI * i) / n - Math.PI / 2),
  });
  const stage = $("stage");
  stage.replaceChildren();
  for (const key of partitionsAt(t)) {
    const [a, b] = key.split("-").map(Number);
    const p = at(a), q = at(b);
    stage.append(svg("line", { x1: p.x, y1: p.y, x2: q.x, y2: q.y, stroke: "#c84646",
      "stroke-dasharray": "6 5", "stroke-width": 2 }));
  }
  const window = 120;
  for (const m of model.messages) {
    if (m.t > t || m.t < t - window) continue;
    const p = at(m.from), q = at(m.to);
    const age = (t - m.t) / window;
    const x = p.x + (q.x - p.x) * Math.min(1, 0.15 + age);
    const y = p.y + (q.y - p.y) * Math.min(1, 0.15 + age);
    stage.append(svg("line", { x1: p.x, y1: p.y, x2: x, y2: y,
      stroke: KIND_COLOURS[m.kind] || "#8a8f99", "stroke-width": 2, opacity: 1 - age * 0.8 }));
  }
  const rows = [];
  for (let i = 0; i < n; i++) {
    const s = machineAt(i, t);
    const p = at(i);
    stage.append(svg("circle", { cx: p.x, cy: p.y, r: 30, fill: colourOf(s),
      stroke: s.leads > 0 && s.up ? "#e0b030" : "none", "stroke-width": 5 }));
    stage.append(svg("text", { x: p.x, y: p.y + 4, "text-anchor": "middle", fill: "#fff",
      style: "fill:#fff;font-weight:600" }, `m${i}`));
    stage.append(svg("text", { x: p.x, y: p.y + 48, "text-anchor": "middle" },
      !s.up ? "down" : s.leads > 0 ? `leads ${s.leads}` : s.phase));
    rows.push([`machine-${i}`, s.node ? short(s.node) : "–", s.up ? s.phase : "down",
      s.round ?? "–", String(s.leads), String(s.chosen)]);
  }
  renderTable(rows);
  renderLog(t);
}

function short(node) {
  return BigInt(node).toString(16).padStart(16, "0").slice(0, 6);
}

function renderTable(rows) {
  const head = ["Machine", "Node id", "State", "Last ballot round", "Journals it leads", "Values it applied"];
  const table = $("nodes");
  table.replaceChildren();
  const tr = document.createElement("tr");
  head.forEach((h) => { const th = document.createElement("th"); th.textContent = h; tr.append(th); });
  table.append(tr);
  for (const row of rows) {
    const r = document.createElement("tr");
    row.forEach((cell) => { const td = document.createElement("td"); td.textContent = cell; r.append(td); });
    table.append(r);
  }
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
  const items = [["--idle", "idle"], ["--ok", "founding member"], ["--admitted", "admitted"],
    ["--bad", "down"]];
  $("legend").replaceChildren(...items.map(([v, text]) => {
    const span = document.createElement("span");
    const dot = document.createElement("i");
    dot.style.background = `var(${v})`;
    span.append(dot, text);
    return span;
  }), Object.assign(document.createElement("span"), { textContent: "gold ring: leads a journal" }));
}

// ---- wiring ----

$("preset").replaceChildren(...Object.keys(PRESETS).map((name) => new Option(name, name)));
$("load-preset").onclick = () => {
  state = structuredClone(PRESETS[$("preset").value]);
  renderConfig();
  start();
};
for (const name of ["seed", "machines", "founders", "run"]) $(name).onchange = readConfig;
$("op").onchange = renderOpFields;
$("add").onclick = addAction;
$("undo").onclick = () => { state.actions.pop(); start(); };
$("clear").onclick = () => { state.actions = []; start(); };
$("time").oninput = renderTime;
$("show-msgs").onchange = renderTime;
$("play").onclick = () => {
  if (playing) { clearInterval(playing); playing = null; $("play").textContent = "Play"; return; }
  $("play").textContent = "Pause";
  playing = setInterval(() => {
    const time = $("time");
    const next = Number(time.value) + 20;
    if (next > Number(time.max)) { clearInterval(playing); playing = null; $("play").textContent = "Play"; return; }
    time.value = next;
    renderTime();
  }, 40);
};

legend();
renderConfig();
renderActions();
start();
