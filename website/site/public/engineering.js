// The live parts of the Engineering pages (#812 (website redo 3/4)).
//
// Both widgets run the playground engine (paros + moonpool-sim compiled to
// wasm, staged at /playground/app/ by scripts/build-wasm.sh) in a worker,
// and draw what a run returns. No protocol logic here: every fact comes from
// the run's events and from the topology the admin API reported.
//
//   [data-widget="determinism"]  run a seed twice and compare the runs
//   [data-widget="xray"]         the dogfooding tower: each layer's traffic
const WORKER = new URL("playground/app/worker.js", import.meta.url);

// ---- the engine: one worker, one request at a time ----

let worker = null;
const queue = [];
let busy = false;

function engine() {
  if (!worker) {
    worker = new Worker(WORKER, { type: "module" });
    worker.onmessage = ({ data }) => {
      const job = queue.shift();
      busy = false;
      let run;
      try { run = JSON.parse(data.json); } catch (e) { run = { error: String(e) }; }
      job.resolve(run);
      pump();
    };
    worker.onerror = (e) => {
      e.preventDefault();
      while (queue.length) queue.shift().resolve({ error: "the playground engine did not load" });
      busy = false;
    };
  }
  return worker;
}

function pump() {
  if (busy || !queue.length) return;
  busy = true;
  engine().postMessage(queue[0].request);
}

/** Run `{config, actions}` and resolve the run (a RunView). */
function simulate(request) {
  return new Promise((resolve) => {
    queue.push({ request, resolve });
    pump();
  });
}

// ---- small helpers ----

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

function field(event, key) {
  for (const [k, v] of event.fields) if (k === key) return v;
  return undefined;
}

function chip(text, kind) {
  return el("span", "chip-status" + (kind ? " " + kind : ""), text);
}

function strip(items) {
  const s = el("div", "strip");
  for (const [k, v] of items) {
    const d = el("div");
    d.append(el("span", "k", k));
    const value = el("span", "v");
    if (v instanceof Node) value.append(v); else value.textContent = v;
    d.append(value);
    s.append(d);
  }
  return s;
}

function panel(num, title, meta) {
  const p = el("section", "panel");
  const head = el("div", "panel-head label");
  if (num) head.append(el("span", "num", num));
  head.append(el("span", "title", title));
  if (meta != null) {
    const m = el("span", "meta");
    if (meta instanceof Node) m.append(meta); else m.textContent = meta;
    head.append(m);
  }
  const body = el("div", "panel-body");
  p.append(head, body);
  return { panel: p, head, body };
}

function button(text, primary) {
  const b = el("button", primary ? "btn primary" : "btn", text);
  b.type = "button";
  return b;
}

/** A 64-bit FNV-1a of a string, as 16 hex digits. */
function fingerprint(text) {
  let h = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  const mask = 0xffffffffffffffffn;
  for (let i = 0; i < text.length; i++) {
    h ^= BigInt(text.charCodeAt(i));
    h = (h * prime) & mask;
  }
  return h.toString(16).padStart(16, "0");
}

/** Bins of a series of times over [0, end): how many fall in each. */
function bins(times, end, count) {
  const out = new Array(count).fill(0);
  for (const t of times) out[Math.min(count - 1, Math.floor((t / end) * count))]++;
  return out;
}

/** A flat bar histogram as SVG: one bar per bin, scaled to `max`. */
function histogram(values, max, cls) {
  const NS = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(NS, "svg");
  const w = values.length;
  svg.setAttribute("viewBox", `0 0 ${w} 20`);
  svg.setAttribute("preserveAspectRatio", "none");
  svg.setAttribute("class", "hist " + (cls || ""));
  svg.setAttribute("aria-hidden", "true");
  const base = document.createElementNS(NS, "line");
  base.setAttribute("x1", "0"); base.setAttribute("x2", String(w));
  base.setAttribute("y1", "19.5"); base.setAttribute("y2", "19.5");
  base.setAttribute("class", "hist-base");
  svg.append(base);
  values.forEach((v, i) => {
    if (!v) return;
    const h = Math.max(1.5, (Math.sqrt(v) / Math.sqrt(max || 1)) * 19);
    const r = document.createElementNS(NS, "rect");
    r.setAttribute("x", String(i + 0.1));
    r.setAttribute("width", "0.8");
    r.setAttribute("y", String(19 - h));
    r.setAttribute("height", String(h));
    svg.append(r);
  });
  return svg;
}

function seconds(ms) {
  return (ms / 1000).toFixed(ms % 1000 ? 1 : 0) + " s";
}

/** The operator's steps as numbered ticks on a time axis. */
function actionMarks(r) {
  const marks = el("div", "eng-marks label");
  r.actions.forEach((a, i) => {
    if (i === 0) return;
    const m = el("span", "eng-mark", String(i));
    m.title = a.op.kind.replace("_", " ");
    m.style.left = (a.at_ms / r.config.run_ms) * 100 + "%";
    marks.append(m);
  });
  return marks;
}

/** The legend of the numbered ticks: what each step of the log does. */
function actionLegend(r) {
  const ol = el("ol", "eng-legend label");
  r.actions.forEach((a, i) => {
    const what = a.op.kind.replace("_", " ") + (a.op.machine != null ? " machine " + a.op.machine : a.op.journal ? " " + a.op.tenant + "/" + a.op.journal : a.op.tenant ? " " + a.op.tenant : "");
    const li = el("li", null, `${i} · ${(a.at_ms / 1000).toFixed(0)} s · ${i === 0 ? "init" : what}`);
    ol.append(li);
  });
  return ol;
}

// ---- the scenario both widgets run ----

const init = (n) => ({ at_ms: 0, op: { kind: "init", machines: n } });
const BASE = [
  init(5),
  { at_ms: 3000, op: { kind: "create_tenant", tenant: "acme" } },
  { at_ms: 4000, op: { kind: "create_journal", tenant: "acme", journal: "orders", mode: "machines=1" } },
  { at_ms: 6000, op: { kind: "write", tenant: "acme", journal: "orders", count: 6 } },
];

// ---- determinism: one seed, two runs ----

const DETERMINISM = {
  machines: 5,
  run_ms: 16000,
  actions: [
    ...BASE,
    { at_ms: 7000, op: { kind: "crash", machine: 1 } },
    { at_ms: 8000, op: { kind: "write", tenant: "acme", journal: "orders", count: 4 } },
    { at_ms: 11000, op: { kind: "restart", machine: 1 } },
    { at_ms: 14000, op: { kind: "read", tenant: "acme", journal: "orders", count: 10 } },
  ],
};

function determinism(mount) {
  const ui = panel("01", "One seed, two runs", chip("READY"));
  const status = ui.head.querySelector(".meta");
  const seedA = el("input"); seedA.type = "number"; seedA.min = "0"; seedA.value = "7";
  const seedB = el("input"); seedB.type = "number"; seedB.min = "0"; seedB.value = "7";
  for (const [i, label] of [[seedA, "seed of run A"], [seedB, "seed of run B"]]) i.setAttribute("aria-label", label);
  const run = button("▶ Run A and B", true);
  const same = button("Same seed");
  const next = button("B = seed + 1");
  const form = el("div", "eng-controls");
  const la = el("label", "label", "Run A "); la.append(seedA);
  const lb = el("label", "label", "Run B "); lb.append(seedB);
  form.append(la, lb, run, same, next);
  const rows = el("div", "eng-runs");
  const verdict = el("p", "eng-verdict");
  verdict.textContent = "Press run. Each run is a full simulation: five machines, a crash and a restart.";
  ui.body.append(form, rows, verdict);
  mount.replaceChildren(ui.panel);

  same.onclick = () => { seedB.value = seedA.value; };
  next.onclick = () => { seedB.value = String(Number(seedA.value) + 1); };

  function config(seed) {
    return { config: { seed: Number(seed) || 0, machines: DETERMINISM.machines, run_ms: DETERMINISM.run_ms }, actions: DETERMINISM.actions };
  }

  function row(name, seed, r, max) {
    const box = el("div", "eng-run");
    const msgs = r.events.filter((e) => e.name === "msg_sent").map((e) => e.time_ms);
    const fp = fingerprint(JSON.stringify(r.events));
    box.append(strip([[name + " · seed", String(seed)], ["Events", String(r.events.length)], ["Fingerprint", fp]]));
    const h = histogram(bins(msgs, r.config.run_ms, 80), max);
    const marks = actionMarks(r);
    const lane = el("div", "eng-lane");
    lane.append(h, marks);
    box.append(lane);
    return { box, fp, events: r.events };
  }

  run.onclick = async () => {
    run.disabled = true;
    status.replaceChildren(chip("RUNNING", "accent"));
    verdict.textContent = "Running two simulations in your browser…";
    const [a, b] = [seedA.value, seedB.value];
    const ra = await simulate(config(a));
    const rb = await simulate(config(b));
    run.disabled = false;
    if (ra.error || rb.error) {
      status.replaceChildren(chip("ERROR", "gap"));
      verdict.textContent = "The run stopped: " + (ra.error || rb.error);
      return;
    }
    const all = [ra, rb].map((r) => bins(r.events.filter((e) => e.name === "msg_sent").map((e) => e.time_ms), r.config.run_ms, 80));
    const max = Math.max(...all.flat());
    const A = row("A", a, ra, max);
    const B = row("B", b, rb, max);
    rows.replaceChildren(A.box, B.box, actionLegend(ra));
    if (A.fp === B.fp) {
      status.replaceChildren(chip("SAME RUN", "done"));
      verdict.textContent = a === b
        ? "Same seed, same run: every message, every crash and every choice happened at the same simulated time. This is what makes a failing seed a bug report you can replay."
        : "Two seeds gave the same events. That is rare; try other seeds.";
    } else {
      status.replaceChildren(chip("DIFFERENT", "shared"));
      let i = 0;
      while (i < A.events.length && i < B.events.length && JSON.stringify(A.events[i]) === JSON.stringify(B.events[i])) i++;
      const ea = A.events[i], eb = B.events[i];
      const say = (e) => (e ? `${e.name} at ${e.time_ms} ms` : "the end of the run");
      verdict.textContent = a === b
        ? `The same seed gave two runs. That is a determinism bug: the first difference is event ${i}.`
        : `Another seed, another run. The runs agree on the first ${i} events, then A has ${say(ea)} and B has ${say(eb)}. Both runs still pass every check.`;
    }
  };
  run.click();
}

// ---- the x-ray of the dogfooding page ----

const XRAY_SCENARIOS = {
  "Form a cell, add a tenant, write": {
    machines: 5, run_ms: 12000,
    actions: [
      ...BASE,
      { at_ms: 8000, op: { kind: "read", tenant: "acme", journal: "orders", count: 6 } },
    ],
  },
  "Crash a machine, then restart it": {
    machines: 5, run_ms: 16000,
    actions: [
      ...BASE,
      { at_ms: 7000, op: { kind: "crash", machine: 0 } },
      { at_ms: 8000, op: { kind: "write", tenant: "acme", journal: "orders", count: 4 } },
      { at_ms: 12000, op: { kind: "restart", machine: 0 } },
    ],
  },
  "Two tenants": {
    machines: 5, run_ms: 12000,
    actions: [
      ...BASE,
      { at_ms: 7000, op: { kind: "create_tenant", tenant: "globex" } },
      { at_ms: 8000, op: { kind: "create_journal", tenant: "globex", journal: "events", mode: "machines=1" } },
      { at_ms: 10000, op: { kind: "write", tenant: "globex", journal: "events", count: 6 } },
    ],
  },
};

const LAYER_EVENTS = new Set(["machine_formatted", "cell_init_adopts_a_plan", "cell_initialized"]);
const HEARTBEATS = new Set(["heartbeat", "heartbeat_ack", "pre_read", "pre_read_ack"]);

/** Which layer of the tower an event belongs to: 1 to 5, or 0. */
function classify(run) {
  const journals = new Map(); // "tenant:journal" -> { layer, name }
  for (const top of run.topology) {
    for (const t of top.tenants) {
      t.journals.forEach((j, i) => {
        const layer = t.kind === "cell" ? 2 : t.kind === "universe" ? 3 : i === 0 ? 4 : 5;
        journals.set(j.key, { layer, name: `${t.name}/${j.name}` });
      });
    }
  }
  return (event) => {
    if (LAYER_EVENTS.has(event.name)) return { layer: 1, journal: "cell init" };
    let key = null;
    if (event.name === "msg_sent" || event.name === "client_call") {
      key = `${field(event, "tenant")}:${field(event, "journal")}`;
    } else if (event.name === "journal_started") {
      key = (field(event, "journal") || "").replace("/", ":");
    }
    const j = key && journals.get(key);
    return j ? { layer: j.layer, journal: j.name } : null;
  };
}

function xray(mount) {
  const tower = mount.querySelector(".tower");
  const layers = [...mount.querySelectorAll("[data-layer]")];
  const controls = el("div", "eng-controls");
  const pick = el("select");
  pick.setAttribute("aria-label", "Scenario");
  for (const name of Object.keys(XRAY_SCENARIOS)) pick.append(new Option(name, name));
  const seed = el("input"); seed.type = "number"; seed.min = "0"; seed.value = "1";
  seed.setAttribute("aria-label", "Seed");
  const run = button("▶ Run", true);
  const explode = button("Pull the layers apart");
  explode.setAttribute("aria-pressed", "false");
  const beats = el("input"); beats.type = "checkbox";
  const lbeats = el("label", "label"); lbeats.append(beats, " heartbeats and lease reads");
  const ls = el("label", "label", "Seed "); ls.append(seed);
  controls.append(pick, ls, run, explode, lbeats);

  const top = el("div", "eng-top");
  const detail = el("div", "eng-detail");
  const axis = el("div", "eng-axis");
  mount.prepend(top);
  tower.before(controls);
  tower.after(axis);
  axis.after(detail);

  let current = null; // { run, per-layer data }
  let focus = 0;

  explode.onclick = () => {
    const on = !tower.classList.contains("exploded");
    tower.classList.toggle("exploded", on);
    explode.setAttribute("aria-pressed", String(on));
  };

  for (const layer of layers) {
    const n = Number(layer.dataset.layer);
    const head = layer.querySelector(".panel-head");
    const toggle = button("x-ray");
    toggle.classList.add("xray-btn");
    toggle.setAttribute("aria-pressed", "false");
    toggle.setAttribute("aria-label", "Show only the traffic of layer " + n);
    head.append(toggle);
    toggle.onclick = () => { focus = focus === n ? 0 : n; draw(); };
  }
  beats.onchange = () => draw();

  function draw() {
    for (const layer of layers) {
      const n = Number(layer.dataset.layer);
      layer.classList.toggle("dim", focus !== 0 && focus !== n);
      layer.classList.toggle("focus", focus === n);
      layer.querySelector(".xray-btn").setAttribute("aria-pressed", String(focus === n));
    }
    if (!current) return;
    const { run, events } = current;
    const end = run.config.run_ms;
    const shown = events.filter((e) => beats.checked || !HEARTBEATS.has(e.kind));
    const per = new Map();
    for (const e of shown) {
      if (!per.has(e.layer)) per.set(e.layer, []);
      per.get(e.layer).push(e);
    }
    const all = layers.map((l) => bins((per.get(Number(l.dataset.layer)) || []).map((e) => e.time), end, 60));
    const max = Math.max(1, ...all.flat());
    layers.forEach((layer, i) => {
      const n = Number(layer.dataset.layer);
      const list = per.get(n) || [];
      const slot = layer.querySelector(".layer-traffic");
      const names = [...new Set(list.map((e) => e.journal))];
      const info = el("div", "layer-info label");
      info.append(el("span", null, list.length ? `${list.length} events` : "no traffic in this run"));
      for (const name of names) info.append(el("code", null, name));
      slot.replaceChildren(histogram(all[i], max, focus === n ? "on" : ""), info);
    });
    // The detail of the focused layer: its message kinds and its first events.
    detail.replaceChildren();
    if (!focus) {
      detail.append(el("p", "aside", "Press x-ray on a layer to show only its traffic."));
      return;
    }
    const list = per.get(focus) || [];
    const kinds = new Map();
    for (const e of list) kinds.set(e.kind, (kinds.get(e.kind) || 0) + 1);
    const p = panel("", "x-ray · layer " + String(focus).padStart(2, "0"), `${list.length} events`);
    const meters = el("dl", "meters");
    const top = Math.max(1, ...kinds.values());
    for (const [k, v] of [...kinds].sort((a, b) => b[1] - a[1])) {
      meters.append(el("dt", null, k));
      const dd = el("dd");
      const bar = el("span", "meter");
      bar.style.setProperty("--fill", (v / top) * 100 + "%");
      dd.append(bar, el("span", "tnum", String(v)));
      meters.append(dd);
    }
    const log = el("ol", "eng-log");
    for (const e of list.filter((e) => !HEARTBEATS.has(e.kind)).slice(0, 12)) {
      const li = el("li");
      li.append(el("span", "t tnum", e.time + " ms"), el("span", null, `${e.journal} · ${e.kind}${e.machine != null ? " · machine " + e.machine : ""}`));
      log.append(li);
    }
    p.body.append(meters, list.length ? log : el("p", "aside", "No traffic."));
    detail.append(p.panel);
  }

  async function go() {
    run.disabled = true;
    const sc = XRAY_SCENARIOS[pick.value];
    top.replaceChildren(strip([["Seed", seed.value], ["Machines", String(sc.machines)], ["Simulated time", seconds(sc.run_ms)], ["Status", chip("RUNNING", "accent")]]));
    const r = await simulate({ config: { seed: Number(seed.value) || 0, machines: sc.machines, run_ms: sc.run_ms }, actions: sc.actions });
    run.disabled = false;
    if (r.error) {
      top.replaceChildren(strip([["Seed", seed.value], ["Status", chip("ERROR", "gap")]]));
      detail.replaceChildren(el("p", "aside", "The run stopped: " + r.error));
      return;
    }
    const of = classify(r);
    const events = [];
    for (const e of r.events) {
      const c = of(e);
      if (!c) continue;
      events.push({
        time: e.time_ms,
        layer: c.layer,
        journal: c.journal,
        machine: e.machine,
        kind: e.name === "msg_sent" ? field(e, "kind") : e.name === "client_call" ? "client " + field(e, "kind") : e.name.replace(/_/g, " "),
      });
    }
    current = { run: r, events };
    const lane = el("div", "eng-lane eng-timeline");
    lane.append(el("span", "label", "Operator steps over simulated time"), actionMarks(r));
    axis.replaceChildren(lane, actionLegend(r));
    const ok = r.steps.every((s) => s.ok);
    top.replaceChildren(strip([["Seed", seed.value], ["Machines", String(sc.machines)], ["Simulated time", seconds(sc.run_ms)], ["Status", chip(ok ? "DONE" : "STEP FAILED", ok ? "done" : "gap")]]));
    draw();
  }
  run.onclick = go;
  pick.onchange = go;
  draw();
  go();
}

for (const mount of document.querySelectorAll('[data-widget="determinism"]')) determinism(mount);
for (const mount of document.querySelectorAll('[data-widget="xray"]')) xray(mount);
