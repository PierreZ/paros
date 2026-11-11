// The live parts of the Engineering pages (#812 (website redo 3/4)).
//
// The widgets run the playground engine (paros + moonpool-sim compiled to
// wasm, staged at /playground/app/ by scripts/build-wasm.sh) in a worker
// (engineering-worker.js), and draw what a run returns. No protocol logic here: every fact comes from
// the run's events and from the topology the admin API reported.
//
//   [data-widget="determinism"]  run a seed twice and compare the runs, with
//                                the faults each seed drew
//   [data-widget="xray"]         the dogfooding tower: each layer's traffic
//   [data-widget="hints"]        seeds until a crash hint strikes, and the
//                                recovery (same-code)
//   [data-widget="fsync"]        a failed writeback: quarantine, re-open
//   [data-widget="syncs"]        syncs per write, Ordered and Batched stores
//   [data-widget="swarm"]        what each seed draws (swarm testing)
//   [data-widget="ready"]        the Ready inspector, on paros-core (sans-IO)
//   [data-widget="quorums"]      the quorum picker, on paros-core (grid quorums)
//
// The last two run paros-core alone (the `paxos` bundle at /paxos/wasm/),
// the others the playground engine.
const WORKER = new URL("engineering-worker.js", import.meta.url);
const PAXOS = new URL("paxos/wasm/paros_paxos.js", import.meta.url);

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
  const faults = el("input"); faults.type = "checkbox"; faults.checked = true;
  const lf = el("label", "label"); lf.append(faults, " faults drawn by the seed");
  form.append(la, lb, run, same, next, lf);
  const rows = el("div", "eng-runs");
  const verdict = el("p", "eng-verdict");
  verdict.textContent = "Press run. Each run is a full simulation: five machines, a crash and a restart, and the faults that the seed draws.";
  ui.body.append(form, rows, verdict);
  mount.replaceChildren(ui.panel);

  same.onclick = () => { seedB.value = seedA.value; };
  next.onclick = () => { seedB.value = String(Number(seedA.value) + 1); };

  function config(seed) {
    const chaos = faults.checked ? ALL_FAULTS : {};
    return { config: { seed: Number(seed) || 0, machines: DETERMINISM.machines, run_ms: DETERMINISM.run_ms, chaos }, actions: DETERMINISM.actions };
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
    box.append(lane, faultStrip(r));
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

// ---- the faults a seed draws (#951 (live Engineering pages)) ----

/** Every fault family the playground engine can let a seed draw. */
const ALL_FAULTS = { network: true, storage: true, crashes: true };

/** moonpool kills a process at its own hint (moonpool-sim's hint sink). */
const HINT_STRUCK = "hint: rebooting the process at its own hint";

/** The family of a simulator fault event, or null for one the page skips. */
function family(event) {
  if (event.name === HINT_STRUCK) return "crash";
  if (event.name !== "sim_fault") return null;
  const kind = field(event, "kind") || "";
  if (kind === "process_force_kill") return "crash";
  if (/^storage_(read|write|sync)_fault$|^storage_disk_failure$/.test(kind)) return "storage";
  if (/partition_created$|^interface_clogged$|^random_close$|^black_hole$|^bit_flip$/.test(kind)) return "network";
  return null;
}

const FAMILIES = [["network", "network"], ["storage", "storage"], ["crash", "crashes"]];

/** Machine `i` is 10.0.1.{i+1} (playground/machine.rs). */
function machineOfIp(ip) {
  const m = /^10\.0\.1\.(\d+)$/.exec(ip || "");
  return m ? Number(m[1]) - 1 : null;
}

/** "tenant:journal" ids to "tenant/journal" names, from the run's topology. */
function journalNames(run) {
  const names = new Map();
  for (const top of run.topology || []) {
    for (const t of top.tenants) for (const j of t.journals) names.set(j.key, `${t.name}/${j.name}`);
  }
  return names;
}

/** The name of a journal an event names, as `tenant/journal` or `tenant:journal`. */
function journalOf(event, names) {
  const j = field(event, "journal") || "";
  const key = j.includes("/") ? j.replace("/", ":") : `${field(event, "tenant")}:${j}`;
  return names.get(key) || "a journal";
}

/** The faults a run drew, on a time axis, and their count per family. */
function faultStrip(run) {
  const box = el("div", "eng-faults");
  const lane = el("div", "eng-fault-lane");
  lane.setAttribute("aria-hidden", "true");
  const counts = { network: 0, storage: 0, crash: 0 };
  let hinted = 0;
  for (const e of run.events) {
    const f = family(e);
    if (!f) continue;
    if (e.name === HINT_STRUCK) { hinted++; continue; }
    counts[f]++;
    const m = el("span", "f f-" + f);
    m.style.left = Math.min(100, (e.time_ms / run.config.run_ms) * 100) + "%";
    lane.append(m);
  }
  const legend = el("p", "eng-fault-legend label");
  for (const [f, word] of FAMILIES) {
    const k = el("span", "k k-" + f, `${word} ${counts[f]}`);
    legend.append(k);
  }
  if (hinted) legend.append(el("span", null, `(${hinted} at a hint)`));
  box.append(lane, legend);
  return box;
}

/** Run seeds from `first` until `found(run)` answers, at most `max` seeds. */
async function seek(first, max, request, found, progress) {
  for (let i = 0; i < max; i++) {
    const seed = first + i;
    const r = await simulate(request(seed));
    if (r.error) return { seed, run: r, error: r.error };
    const hit = found(r);
    progress(seed, r, hit);
    if (hit) return { seed, run: r, hit };
  }
  return null;
}

/** One line of an event log: a time and what happened. */
function logLine(time, text, kind) {
  const li = el("li", kind ? "ev-" + kind : null);
  li.append(el("span", "t tnum", (time / 1000).toFixed(2) + " s"), el("span", null, text));
  return li;
}

function stepsLine(run) {
  const ok = run.steps.filter((s) => s.ok).length;
  return `${ok} of ${run.actions.length} operator steps ended well`;
}

// ---- same-code: a crash at a hint of the shipped code, and the recovery ----

const HINT_LABELS = [
  "batch durable, not sent",
  "promise durable, entries staged",
  "batch staged, not synced",
  "accepts sent, own vote not durable",
  "registration staged, not synced",
];

const HINT_RUN = {
  machines: 5,
  run_ms: 16000,
  actions: [
    ...BASE,
    { at_ms: 10000, op: { kind: "write", tenant: "acme", journal: "orders", count: 6 } },
    { at_ms: 14000, op: { kind: "read", tenant: "acme", journal: "orders", count: 12 } },
  ],
};

function hints(mount) {
  const ui = panel("01", "Strike a hint", chip("READY"));
  const status = ui.head.querySelector(".meta");
  const pick = el("select");
  pick.setAttribute("aria-label", "Hint label");
  pick.append(new Option("any hint", ""));
  for (const l of HINT_LABELS) pick.append(new Option(l, l));
  const seed = el("input"); seed.type = "number"; seed.min = "0"; seed.value = "1";
  seed.setAttribute("aria-label", "First seed");
  const go = button("▶ Run seeds until it strikes", true);
  const controls = el("div", "eng-controls");
  const ls = el("label", "label", "From seed "); ls.append(seed);
  controls.append(pick, ls, go);
  const top = el("div", "eng-top");
  const log = el("ol", "eng-log");
  const verdict = el("p", "eng-verdict", "Pick a label, then run. Each seed is a full simulation of five machines with crashes on.");
  ui.body.append(controls, top, log, verdict);
  mount.replaceChildren(ui.panel);

  go.onclick = async () => {
    go.disabled = true;
    status.replaceChildren(chip("RUNNING", "accent"));
    log.replaceChildren();
    top.replaceChildren();
    const want = pick.value;
    const strikes = (r) => r.events.filter((e) => e.name === HINT_STRUCK && (!want || field(e, "label") === want));
    const found = await seek(Number(seed.value) || 0, 12,
      (s) => ({ config: { seed: s, machines: HINT_RUN.machines, run_ms: HINT_RUN.run_ms, chaos: { crashes: true } }, actions: HINT_RUN.actions }),
      (r) => strikes(r)[0],
      (s, r, hit) => {
        const labels = [...new Set(r.events.filter((e) => e.name === HINT_STRUCK).map((e) => field(e, "label")))];
        log.append(logLine(r.config.run_ms, `seed ${s}: ${labels.length ? "struck " + labels.join(", ") : "no hint struck"}${hit ? "" : ""}`));
        verdict.textContent = `Seed ${s} ran.`;
      });
    go.disabled = false;
    if (!found || found.error) {
      status.replaceChildren(chip(found ? "ERROR" : "NOT FOUND", "gap"));
      verdict.textContent = found ? "The run stopped: " + found.error
        : "No seed of these twelve struck that hint. A rare point needs more seeds: CI runs thousands. Try the next seeds.";
      if (!found) seed.value = String((Number(seed.value) || 0) + 12);
      return;
    }
    const r = found.run;
    const strike = found.hit;
    const ip = field(strike, "ip");
    const m = machineOfIp(ip);
    const t0 = strike.time_ms;
    top.replaceChildren(strip([["Seed", String(found.seed)], ["Hint", field(strike, "label")], ["Machine", "machine-" + m], ["At", seconds(t0)]]));
    // The crash and what follows on the struck machine, then the elections
    // and the steps after it.
    const lines = [];
    lines.push([t0, `machine-${m} reaches hint!("${field(strike, "label")}"): moonpool kills it there`, "fault"]);
    // This one crash of the struck machine: its lost writes, its restart,
    // and the first time it serves again. A later crash is another story.
    let restarted = null, lost = false, served = false;
    for (const e of r.events) {
      if (e.time_ms < t0) continue;
      if (e.name === "sim_fault" && field(e, "ip") === ip) {
        const kind = field(e, "kind");
        if (kind === "storage_crash" && !lost && restarted == null) { lost = true; lines.push([e.time_ms, `machine-${m} loses every write it did not sync`, "fault"]); }
        if (kind === "process_restart" && restarted == null) { restarted = e.time_ms; lines.push([e.time_ms, `machine-${m} starts again on its disk`]); }
      } else if (restarted != null && !served && e.machine === m && (e.name === "machine_serving" || e.name === "machine_serving_admitted_journals")) {
        served = true;
        lines.push([e.time_ms, `machine-${m} serves its journals again`, "ok"]);
      } else if (e.name === "leader_elected" && e.time_ms < t0 + 6000) {
        lines.push([e.time_ms, `machine-${e.machine} is elected leader of ${journalOf(e, journalNames(r))}`]);
      }
    }
    for (const st of r.steps) if (st.end_ms >= t0) lines.push([st.end_ms, `operator step ${st.index}: ${st.ok ? "" : "refused: "}${st.detail}`, st.ok ? "ok" : "fault"]);
    lines.sort((a, b) => a[0] - b[0]);
    log.replaceChildren(...lines.slice(0, 16).map(([t, text, k]) => logLine(t, text, k)));
    status.replaceChildren(chip("STRUCK", "done"));
    verdict.textContent = `The same run_machine as parosd crashed at a line of the shipped code, started again from its disk, and the cell went on: ${stepsLine(r)}. Run seed ${found.seed} again and the crash happens at the same simulated time.`;
    seed.value = String(found.seed + 1);
  };
}

// ---- fsync: a failed writeback, quarantine, re-open ----

const FSYNC_RUN = {
  machines: 5,
  run_ms: 16000,
  actions: [
    ...BASE,
    { at_ms: 9000, op: { kind: "write", tenant: "acme", journal: "orders", count: 10 } },
    { at_ms: 14000, op: { kind: "read", tenant: "acme", journal: "orders", count: 16 } },
  ],
};

function fsync(mount) {
  const ui = panel("01", "A failed writeback", chip("READY"));
  const status = ui.head.querySelector(".meta");
  const seed = el("input"); seed.type = "number"; seed.min = "0"; seed.value = "1";
  seed.setAttribute("aria-label", "First seed");
  const go = button("▶ Run seeds until a writeback fails", true);
  const controls = el("div", "eng-controls");
  const ls = el("label", "label", "From seed "); ls.append(seed);
  controls.append(ls, go);
  const top = el("div", "eng-top");
  const log = el("ol", "eng-log");
  const verdict = el("p", "eng-verdict", "Each seed runs five machines on a simulated disk whose background writeback can fail, and only that.");
  ui.body.append(controls, top, log, verdict);
  mount.replaceChildren(ui.panel);

  go.onclick = async () => {
    go.disabled = true;
    status.replaceChildren(chip("RUNNING", "accent"));
    log.replaceChildren();
    top.replaceChildren();
    const found = await seek(Number(seed.value) || 0, 16,
      (s) => ({ config: { seed: s, machines: FSYNC_RUN.machines, run_ms: FSYNC_RUN.run_ms, chaos: { writeback: true } }, actions: FSYNC_RUN.actions }),
      (r) => r.events.find((e) => e.name === "journal_quarantined"),
      (s, r) => {
        const failed = r.events.filter((e) => e.name === "sim_fault" && field(e, "write_kind") === "writeback").length;
        log.append(logLine(r.config.run_ms, `seed ${s}: ${failed} failed writebacks, ${r.events.filter((e) => e.name === "journal_quarantined").length} journals quarantined`));
      });
    go.disabled = false;
    if (!found || found.error) {
      status.replaceChildren(chip(found ? "ERROR" : "NOT FOUND", "gap"));
      verdict.textContent = found ? "The run stopped: " + found.error : "No failed writeback reached a sync in these seeds. Try the next seeds.";
      if (!found) seed.value = String((Number(seed.value) || 0) + 16);
      return;
    }
    const r = found.run;
    const q = found.hit;
    const m = q.machine;
    const names = journalNames(r);
    const journal = journalOf(q, names);
    const node = field(q, "node");
    const lines = [];
    const fault = r.events.filter((e) => e.name === "sim_fault" && field(e, "write_kind") === "writeback" && machineOfIp(field(e, "ip")) === m && e.time_ms <= q.time_ms).pop();
    if (fault) lines.push([fault.time_ms, `1-2 · machine-${m}: the background writeback fails. The pages are marked clean, the bytes are not on the device.`, "fault"]);
    lines.push([q.time_ms, `2-3 · the next sync of ${journal} reports the error. The vote does not leave, and the journal is quarantined. The other journals of machine-${m} go on.`, "fault"]);
    const reopen = r.events.find((e) => e.name === "journal_reopened" && e.machine === m && e.time_ms > q.time_ms && field(e, "journal") === field(q, "journal"));
    let after = 0;
    if (reopen) {
      lines.push([reopen.time_ms, `4-5 · ${journal} re-opens on machine-${m}: the open drops the cached pages and reads what the device holds.`]);
      for (const e of r.events) {
        if (e.machine !== m || e.time_ms <= reopen.time_ms) continue;
        if (e.name === "faulty_entry_reported") lines.push([e.time_ms, `5 · the open reports ${field(e, "slots")} slots with no clean copy`, "fault"]);
        if (e.name === "journal_ambiguous_batch") lines.push([e.time_ms, `5 · the open reports an ambiguous batch of ${field(e, "entries")} entries`, "fault"]);
        if (e.name === "journal_committed" && field(e, "node") === node && `${field(e, "tenant")}/${field(e, "journal")}` === field(q, "journal")) after++;
      }
      if (after) {
        const first = r.events.find((e) => e.name === "journal_committed" && e.machine === m && e.time_ms > reopen.time_ms && field(e, "node") === node && `${field(e, "tenant")}/${field(e, "journal")}` === field(q, "journal"));
        lines.push([first.time_ms, `6 · ${journal} on machine-${m} votes again: ${after} durable commits after the re-open`, "ok"]);
      }
    }
    lines.sort((a, b) => a[0] - b[0]);
    log.replaceChildren(...lines.map(([t, text, k]) => logLine(t, text, k)));
    top.replaceChildren(strip([["Seed", String(found.seed)], ["Journal", journal], ["Machine", "machine-" + m], ["Out of service", reopen ? seconds(reopen.time_ms - q.time_ms) : "to the end"]]));
    status.replaceChildren(chip(reopen ? "RECOVERED" : "QUARANTINED", reopen ? "done" : "shared"));
    verdict.textContent = `The numbers match the steps of the table above. ${stepsLine(r)}: no client saw the fault.`;
    seed.value = String(found.seed + 1);
  };
}

// ---- durable-writes: syncs per write, Ordered and Batched ----

const SYNC_WRITES = 20;
const SYNC_RUN = {
  machines: 5,
  run_ms: 14000,
  actions: [
    init(5),
    { at_ms: 3000, op: { kind: "create_tenant", tenant: "acme" } },
    { at_ms: 4000, op: { kind: "create_journal", tenant: "acme", journal: "orders", mode: "machines=1" } },
    { at_ms: 6000, op: { kind: "write", tenant: "acme", journal: "orders", count: SYNC_WRITES } },
  ],
};

function syncs(mount) {
  const ui = panel("01", "Syncs per write", chip("READY"));
  const status = ui.head.querySelector(".meta");
  const seed = el("input"); seed.type = "number"; seed.min = "0"; seed.value = "1";
  seed.setAttribute("aria-label", "Seed");
  const go = button("▶ Run Ordered and Batched", true);
  const controls = el("div", "eng-controls");
  const ls = el("label", "label", "Seed "); ls.append(seed);
  controls.append(ls, go);
  const out = el("div", "eng-runs");
  const verdict = el("p", "eng-verdict", `Each run writes ${SYNC_WRITES} records to acme/orders, a journal on three acceptors, and counts the syncs of its store commits.`);
  ui.body.append(controls, out, verdict);
  mount.replaceChildren(ui.panel);

  function count(r) {
    const names = journalNames(r);
    const per = new Map();
    let commits = 0, total = 0;
    for (const e of r.events) {
      if (e.name !== "journal_committed") continue;
      if (names.get(`${field(e, "tenant")}:${field(e, "journal")}`) !== "acme/orders") continue;
      const n = Number(field(e, "syncs")) || 0;
      commits++; total += n;
      per.set(e.machine, (per.get(e.machine) || 0) + n);
    }
    return { commits, total, per };
  }

  go.onclick = async () => {
    go.disabled = true;
    status.replaceChildren(chip("RUNNING", "accent"));
    const s = Number(seed.value) || 0;
    const runs = [];
    for (const batched of [false, true]) {
      runs.push([batched, await simulate({ config: { seed: s, machines: SYNC_RUN.machines, run_ms: SYNC_RUN.run_ms, batched }, actions: SYNC_RUN.actions })]);
    }
    go.disabled = false;
    const bad = runs.find(([, r]) => r.error);
    if (bad) { status.replaceChildren(chip("ERROR", "gap")); verdict.textContent = "The run stopped: " + bad[1].error; return; }
    const counted = runs.map(([b, r]) => [b, r, count(r)]);
    const top = Math.max(...counted.map(([, , c]) => c.total), 1);
    out.replaceChildren();
    for (const [batched, r, c] of counted) {
      const box = el("div", "eng-run");
      box.append(strip([["Store", batched ? "Batched" : "Ordered"], ["Commits", String(c.commits)], ["Syncs", String(c.total)], ["Syncs per write, all acceptors", (c.total / SYNC_WRITES).toFixed(1)]]));
      const meters = el("dl", "meters");
      for (const [m, n] of [...c.per].sort((a, b) => a[0] - b[0])) {
        meters.append(el("dt", null, "machine-" + m));
        const dd = el("dd");
        const bar = el("span", "meter");
        bar.style.setProperty("--fill", (n / top) * 100 + "%");
        dd.append(bar, el("span", "tnum", `${n} syncs`));
        meters.append(dd);
      }
      box.append(meters, el("p", "aside", stepsLine(r)));
      out.append(box);
    }
    status.replaceChildren(chip("DONE", "done"));
    const [o, b] = counted.map(([, , c]) => c.total);
    verdict.textContent = `Same seed, same writes. Ordered syncs ${o} times, Batched ${b}: one sync per commit instead of two. A count, not a time: the disk and the clock are simulated. Each write still waits for a majority of durable votes in both modes.`;
  };
}

// ---- swarm: what each seed draws ----

const SWARM_RUN = {
  machines: 5,
  run_ms: 14000,
  actions: [
    ...BASE,
    { at_ms: 10000, op: { kind: "read", tenant: "acme", journal: "orders", count: 6 } },
  ],
};

function swarm(mount) {
  const ui = panel("01", "Six seeds, six worlds", chip("READY"));
  const status = ui.head.querySelector(".meta");
  const seed = el("input"); seed.type = "number"; seed.min = "0"; seed.value = "1";
  seed.setAttribute("aria-label", "First seed");
  const go = button("▶ Run six seeds", true);
  const controls = el("div", "eng-controls");
  const ls = el("label", "label", "From seed "); ls.append(seed);
  controls.append(ls, go);
  const table = el("div", "eng-swarm");
  const verdict = el("p", "eng-verdict", "Every fault family is on. Each seed turns on its own subset and draws its own rates.");
  ui.body.append(controls, table, verdict);
  mount.replaceChildren(ui.panel);

  go.onclick = async () => {
    go.disabled = true;
    status.replaceChildren(chip("RUNNING", "accent"));
    table.replaceChildren();
    const first = Number(seed.value) || 0;
    for (let s = first; s < first + 6; s++) {
      const r = await simulate({ config: { seed: s, machines: SWARM_RUN.machines, run_ms: SWARM_RUN.run_ms, chaos: ALL_FAULTS }, actions: SWARM_RUN.actions });
      const row = el("div", "eng-run");
      if (r.error) { row.append(el("p", "aside", `seed ${s}: the run stopped: ${r.error}`)); table.append(row); continue; }
      const kinds = new Map();
      for (const e of r.events) {
        if (!family(e) || e.name === HINT_STRUCK) continue;
        const k = field(e, "kind") === "storage_write_fault" ? "write fault: " + field(e, "write_kind") : field(e, "kind").replace(/_/g, " ");
        kinds.set(k, (kinds.get(k) || 0) + 1);
      }
      const hinted = [...new Set(r.events.filter((e) => e.name === HINT_STRUCK).map((e) => field(e, "label")))];
      row.append(strip([["Seed", String(s)], ["Steps", `${r.steps.filter((x) => x.ok).length} of ${r.actions.length} ok`]]), faultStrip(r));
      const what = el("p", "label eng-kinds");
      what.textContent = [...kinds].map(([k, n]) => `${k} ×${n}`).join(" · ") || "no fault";
      row.append(what);
      if (hinted.length) row.append(el("p", "label eng-kinds", "crash hints: " + hinted.join(" · ")));
      table.append(row);
    }
    go.disabled = false;
    status.replaceChildren(chip("DONE", "done"));
    verdict.textContent = "No two seeds draw the same world. A seed with no network faults lets the disk faults pile up; a seed with only cut links never sees a corrupt read. Over thousands of seeds, every mix comes up, and every run is judged by the same checks.";
    seed.value = String(first + 6);
  };
}

// ---- the paros-core engine: the paxos bundle ----

let paxosEngine = null;
function paxos() {
  if (!paxosEngine) {
    paxosEngine = import(PAXOS.href).then(async (m) => { await m.default(); return m; });
  }
  return paxosEngine;
}

function needsWasm(mount, error) {
  console.warn("paros-core in wasm did not load", error);
  mount.replaceChildren(el("p", "aside", "This demo needs paros-core compiled to wasm (scripts/build-wasm.sh)."));
}

// ---- sans-io: the Ready inspector ----

function ready(mount) {
  paxos().then((engine) => readyLab(mount, engine), (e) => needsWasm(mount, e));
}

function readyLab(mount, engine) {
  let lab = new engine.ReadyLab();
  let view = JSON.parse(lab.view());
  let n = 0;

  const nodesUi = panel("01", "Three ColocatedNodes", "");
  const reset = button("Reset");
  nodesUi.head.querySelector(".meta").append(reset);
  const wireUi = panel("02", "The wire", null);
  const readyUi = panel("03", "What the last input gave the driver", null);
  mount.replaceChildren(nodesUi.panel, wireUi.panel, readyUi.panel);

  function act(input) {
    const answer = JSON.parse(lab.act(JSON.stringify(input)));
    if (!answer.error || answer.nodes) view = answer;
    draw(answer.error && !answer.nodes ? answer.error : null);
  }

  reset.onclick = () => { lab.free(); lab = new engine.ReadyLab(); view = JSON.parse(lab.view()); draw(); };

  function draw(problem) {
    // 01: the nodes and their inputs.
    const cards = el("div", "lab-nodes");
    for (const node of view.nodes) {
      const card = el("div", "lab-node");
      card.append(el("div", "label", `node ${node.id} · ${node.role}`));
      card.append(el("div", "lab-state tnum", `promise ${node.promise} · ${node.records} on disk · ${node.chosen} chosen`));
      const row = el("div", "lab-buttons");
      const t = button("timeout"); t.onclick = () => act({ kind: "timeout", node: node.id });
      const k = button("tick"); k.onclick = () => act({ kind: "tick", node: node.id });
      const w = button("write"); w.onclick = () => act({ kind: "write", node: node.id, text: "r" + (n++) });
      t.title = "tick() with the election clock run out";
      w.title = "propose() a client record at this node";
      row.append(t, k, w);
      card.append(row);
      cards.append(card);
    }
    nodesUi.body.replaceChildren(cards);
    // 02: the wire.
    const list = el("ol", "eng-log lab-wire");
    for (const m of view.wire) {
      const li = el("li");
      const d = button("deliver"); d.onclick = () => act({ kind: "deliver", id: m.id });
      const x = button("lose"); x.onclick = () => act({ kind: "drop", id: m.id });
      li.append(el("span", "t tnum", `${m.from} → ${m.to}`), el("span", "lab-msg", m.summary), el("span", "lab-buttons", null));
      li.lastChild.append(d, x);
      list.append(li);
    }
    const all = button("deliver all", true);
    all.onclick = () => {
      // The page only repeats the reader's own input: deliver the oldest
      // message, until the wire is empty or a bound is reached.
      for (let i = 0; i < 60 && view.wire.length; i++) act({ kind: "deliver", id: view.wire[0].id });
    };
    wireUi.body.replaceChildren(view.wire.length ? list : el("p", "aside", "No message on the wire."), view.wire.length ? all : el("span"));
    // 03: the last input and its Ready batches.
    const out = [];
    if (problem || view.error) out.push(el("p", "callout", problem || view.error));
    const last = view.last;
    if (!last) {
      out.push(el("p", "aside", "Start: press timeout on node 0. Its election clock runs out, and it campaigns."));
    } else {
      out.push(strip([["Input", last.input], ["Answer", last.answer || "—"], ["Ready batches", String(last.readies.length)]]));
      if (!last.readies.length) out.push(el("p", "aside", "Nothing to do: the input changed no durable state and sent nothing."));
      last.readies.forEach((r, i) => {
        const box = el("div", "lab-ready");
        box.append(el("div", "label", `ready() #${i + 1}`));
        const b = el("ol", "lab-buckets");
        const bucket = (num, name, items, extra) => {
          const li = el("li");
          const head = el("div", "lab-bucket label");
          head.append(el("span", "num", num), el("span", null, name));
          if (extra) head.append(extra);
          li.append(head);
          const ul = el("ul", "lab-items");
          if (!items.length) ul.append(el("li", "aside", "empty"));
          for (const it of items) ul.append(el("li", null, it));
          li.append(ul);
          b.append(li);
        };
        bucket("1", "writes(): persist", r.writes.map((w) => `${w.kind}: ${w.detail}`), r.sync ? chip("SYNC", "accent") : null);
        bucket("2", "messages(): send, after the sync", r.messages.map((m) => `${m.kind} → node ${m.to}`));
        bucket("3", "committed(): answer the client", r.committed);
        bucket("4", "read_states(): serve the reads", r.read_states);
        const adv = el("li");
        const head = el("div", "lab-bucket label");
        head.append(el("span", "num", "5"), el("span", null, "advance()"));
        adv.append(head);
        b.append(adv);
        box.append(b);
        out.push(box);
      });
    }
    readyUi.body.replaceChildren(...out);
  }
  draw();
}

// ---- grid-quorums: the quorum picker ----

function quorums(mount) {
  paxos().then((engine) => picker(mount, engine), (e) => needsWasm(mount, e));
}

function picker(mount, engine) {
  const ui = panel("01", "Stop some acceptors", null);
  const fSel = el("select"); fSel.setAttribute("aria-label", "Failures per partition");
  for (const f of [1, 2]) fSel.append(new Option(`F = ${f} (${2 * f + 1} per partition)`, String(f)));
  const pSel = el("select"); pSel.setAttribute("aria-label", "Partitions");
  for (const p of [1, 2, 3, 4]) pSel.append(new Option(`${p} partition${p > 1 ? "s" : ""}`, String(p)));
  pSel.value = "3";
  const controls = el("div", "eng-controls");
  controls.append(fSel, pSel);
  const grid = el("div", "qgrid");
  const result = el("div", "eng-top");
  ui.body.append(controls, grid, result);
  mount.replaceChildren(ui.panel);
  let down = new Set();

  fSel.onchange = pSel.onchange = () => { down = new Set(); draw(); };

  function draw() {
    const v = JSON.parse(engine.quorum_reach(JSON.stringify({ f: Number(fSel.value), partitions: Number(pSel.value), down: [...down] })));
    if (v.error) { result.replaceChildren(el("p", "callout", v.error)); return; }
    grid.style.setProperty("--cols", String(v.cols));
    grid.replaceChildren();
    for (let c = 0; c < v.cols; c++) {
      const h = el("div", "qhead label", `partition ${c}`);
      h.classList.toggle("lost", !v.phase2[c]);
      h.style.gridColumn = String(c + 1);
      h.style.gridRow = "1";
      grid.append(h);
    }
    v.cells.forEach(([row, col], i) => {
      const b = button(down.has(i) ? `acceptor ${i} · down` : `acceptor ${i}`);
      b.classList.add("qcell");
      b.setAttribute("aria-pressed", String(down.has(i)));
      b.style.gridColumn = String(col + 1);
      b.style.gridRow = String(row + 2);
      b.onclick = () => { if (down.has(i)) down.delete(i); else down.add(i); draw(); };
      grid.append(b);
    });
    const writes = v.phase2.filter(Boolean).length;
    result.replaceChildren(
      strip([
        ["Election and tail read", chip(v.phase1 ? "CAN FINISH" : "BLOCKED", v.phase1 ? "done" : "gap")],
        ["Partitions that take writes", chip(`${writes} OF ${v.cols}`, writes === v.cols ? "done" : writes ? "shared" : "gap")],
      ]),
      el("p", "eng-verdict", `A write needs ${v.write_quorum} acceptors of its own partition. An election needs ${v.election_per_partition} acceptors of every partition. ${
        v.phase1 ? (writes === v.cols ? "Every quorum still holds." : "The slots of a lost partition wait; the other partitions take writes, and an election can still finish and reconfigure.")
          : writes ? "No election can finish: one partition lost its majority. The leader in place still writes to the partitions that hold one." : "Nothing can finish."}`),
    );
  }
  draw();
}

for (const mount of document.querySelectorAll('[data-widget="determinism"]')) determinism(mount);
for (const mount of document.querySelectorAll('[data-widget="xray"]')) xray(mount);
for (const mount of document.querySelectorAll('[data-widget="hints"]')) hints(mount);
for (const mount of document.querySelectorAll('[data-widget="fsync"]')) fsync(mount);
for (const mount of document.querySelectorAll('[data-widget="syncs"]')) syncs(mount);
for (const mount of document.querySelectorAll('[data-widget="swarm"]')) swarm(mount);
for (const mount of document.querySelectorAll('[data-widget="ready"]')) ready(mount);
for (const mount of document.querySelectorAll('[data-widget="quorums"]')) quorums(mount);
