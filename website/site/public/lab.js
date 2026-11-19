// "Your turn": the exercises of the Paxos course, on paros-consensus compiled
// to wasm (`PaxosLab` in crates/support/paros-wasm/src/paxos/lab.rs).
//
// The reader is the network: each message on the wire has Deliver and Drop.
// An exercise can add a few more moves (a proposer opens a ballot, an
// acceptor loses its disk). The core decides the rest, and the engine says
// when the goal holds. No protocol logic here: the page draws the view and
// sends the moves.
//
//   <div data-lab="id id id">   one tab per exercise, in order
//
// A solved exercise is kept in the browser (public/course.js) and fires
// "paxos:solved" on the document.
const PAXOS = new URL("paxos/wasm/paros_paxos.js", import.meta.url);
const SVG = "http://www.w3.org/2000/svg";
const ROWS = ["proposer", "node", "acceptor", "matchmaker"];

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}
function svg(tag, attrs, text) {
  const e = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) e.setAttribute(k, v);
  if (text != null) e.textContent = text;
  return e;
}
function button(text, cls) {
  const b = el("button", "btn" + (cls ? " " + cls : ""), text);
  b.type = "button";
  return b;
}
function short(lane) {
  const m = /^(\w)\w*\D*(\d+)$/.exec(lane.label.replace(/­/g, ""));
  return m ? m[1] + m[2] : lane.id;
}
function nameOf(view, id) {
  const lane = view.lanes.find((l) => l.id === id);
  return lane ? lane.label.replace(/­/g, "") : id;
}

function mountLab(mount, engine) {
  const ids = mount.dataset.lab.split(/\s+/).filter(Boolean);
  const course = window.paxosCourse || null;
  let lab = null;
  let view = null;
  let current = ids[0];
  const fromHash = /^#lab=([\w-]+)$/.exec(window.location.hash);
  if (fromHash && ids.includes(fromHash[1])) current = fromHash[1];

  const panel = el("section", "panel lab");
  panel.id = "your-turn-lab";
  const head = el("div", "panel-head label");
  head.append(el("span", "title", "Your turn"));
  const live = el("span", "chip-status accent", "live");
  live.title = "paros-consensus runs in this page";
  head.append(live);
  const meta = el("span", "meta tnum");
  head.append(meta);
  const tabs = el("div", "lab-tabs");
  const task = el("p", "lab-task");
  const ruleOff = el("div", "callout lab-rule-off");
  ruleOff.dataset.tag = "one rule off";
  const stage = el("div", "lab-stage");
  const chosen = el("p", "lab-chosen");
  const done = el("div", "callout lab-done");
  done.dataset.tag = "goal reached";
  const wireHead = el("p", "lab-sub label", "On the wire: you are the network");
  const wire = el("ul", "lab-wire");
  const extrasHead = el("p", "lab-sub label", "Other moves");
  const extras = el("div", "lab-extras");
  const said = el("p", "lab-said");
  said.setAttribute("aria-live", "polite");
  const controls = el("div", "lab-controls");
  const undo = button("Undo");
  const reset = button("Start again");
  controls.append(undo, reset);
  panel.append(head, tabs, task, ruleOff, stage, chosen, done, wireHead, wire, extrasHead, extras, said, controls);
  mount.replaceChildren(panel);

  function solvedBefore(id) {
    return !!(course && course.load().solved && course.load().solved[id]);
  }

  function drawTabs() {
    tabs.replaceChildren();
    ids.forEach((id, i) => {
      const b = el("button", "lab-tab" + (id === current ? " now" : ""));
      b.type = "button";
      b.append(el("span", "lab-tab-n tnum", solvedBefore(id) ? "✓" : String(i + 1)));
      b.append(document.createTextNode(id === current && view ? view.title : (titles[id] || id)));
      b.addEventListener("click", () => start(id));
      tabs.append(b);
    });
  }

  const titles = {};

  function drawStage() {
    const rows = {};
    view.lanes.forEach((lane, i) => {
      const r = ROWS.indexOf(lane.role);
      (rows[r] = rows[r] || []).push({ lane, i });
    });
    const keys = Object.keys(rows).map(Number).sort((a, b) => a - b);
    const w = Math.max(320, Math.max(...keys.map((k) => rows[k].length)) * 96);
    const rowH = 104;
    const s = svg("svg", { viewBox: `0 0 ${w} ${keys.length * rowH + 4}`, class: "lab-svg", role: "img" });
    s.style.maxWidth = Math.min(w * 1.2, 560) + "px";
    const at = {};
    keys.forEach((k, r) => {
      rows[k].forEach((entry, c) => {
        at[entry.lane.id] = { x: ((c + 0.5) / rows[k].length) * w, y: r * rowH + 26 };
      });
    });
    // One faint line per message on the wire, so the reader sees who waits.
    for (const m of view.wire) {
      const a = at[m.from], b = at[m.to];
      if (!a || !b) continue;
      s.append(svg("line", { x1: a.x, y1: a.y, x2: b.x, y2: b.y, class: "lab-path" }));
    }
    view.lanes.forEach((lane, i) => {
      const p = at[lane.id];
      const up = view.alive[i] !== false;
      const g = svg("g", { class: "lab-node " + lane.role + (up ? "" : " down") });
      g.append(svg("circle", { cx: p.x, cy: p.y, r: 17 }));
      g.append(svg("text", { x: p.x, y: p.y + 4, class: "lab-node-id" }, short(lane)));
      const lines = up ? (view.state[i] || []) : ["down"];
      lines.slice(0, 2).forEach((line, k) => {
        g.append(svg("text", { x: p.x, y: p.y + 32 + k * 12, class: "lab-node-line" }, line));
      });
      g.append(svg("title", {}, lane.label.replace(/­/g, "")));
      s.append(g);
    });
    stage.replaceChildren(s);
  }

  function draw() {
    titles[view.id] = view.title;
    meta.textContent = view.moves + (view.moves === 1 ? " move" : " moves");
    drawTabs();
    task.textContent = view.task;
    ruleOff.hidden = !view.rule_off;
    ruleOff.replaceChildren(el("p", null, view.rule_off || ""));
    drawStage();

    chosen.replaceChildren();
    if (view.chosen.length > 1) chosen.append(el("span", "chip-status gap", "two values chosen"));
    else if (view.chosen.length) chosen.append(el("span", "chip-status done", "chosen"));
    else chosen.append(el("span", "chip-status", "not chosen"));
    if (view.chosen.length) chosen.append(document.createTextNode(" " + view.chosen.join(", ")));

    done.hidden = !view.solved;
    done.replaceChildren(el("p", null, view.done || ""));
    if (view.solved) {
      const k = ids.indexOf(view.id);
      if (k + 1 < ids.length) {
        const next = button("Next exercise", "primary");
        next.addEventListener("click", () => start(ids[k + 1]));
        done.append(next);
      }
    }

    wire.replaceChildren();
    if (!view.wire.length) wire.append(el("li", "lab-empty", "Nothing is on the wire."));
    for (const m of view.wire) {
      const li = el("li", "lab-msg");
      li.style.setProperty("--phase", `var(--phase-${m.phase || "other"})`);
      const what = el("span", "lab-msg-what");
      what.append(el("b", "lab-msg-label", m.label));
      what.append(el("span", "lab-msg-route", nameOf(view, m.from) + " → " + nameOf(view, m.to)));
      what.title = m.summary;
      const go = button("Deliver", "primary");
      go.addEventListener("click", () => play({ kind: "deliver", id: m.id }));
      const lose = button("Drop");
      lose.addEventListener("click", () => play({ kind: "drop", id: m.id }));
      li.append(what, go, lose);
      wire.append(li);
    }

    extras.replaceChildren();
    extrasHead.hidden = !view.extras.length;
    view.extras.forEach((label, index) => {
      const b = button(label);
      b.addEventListener("click", () => play({ kind: "extra", index }));
      extras.append(b);
    });

    said.replaceChildren();
    if (view.refused) said.append(el("span", "chip-status gap", "refused"), document.createTextNode(" " + view.refused));
    else if (view.said.length) said.textContent = view.said.join(" ");
    undo.disabled = view.moves === 0;
  }

  function show(json) {
    const next = JSON.parse(json);
    if (next.error) { said.textContent = next.error; return; }
    const was = view && view.id === next.id && view.solved;
    view = next;
    if (view.solved && !was) {
      if (course && course.solved) course.solved(view.id);
      try { document.dispatchEvent(new CustomEvent("paxos:solved", { detail: { id: view.id } })); } catch (e) { /* old browser */ }
    }
    draw();
  }

  function play(move) {
    show(lab.play(JSON.stringify(move)));
  }

  function start(id) {
    current = id;
    if (lab && lab.free) lab.free();
    lab = new engine.PaxosLab(id);
    view = null;
    show(lab.view());
    if (window.history && window.history.replaceState) window.history.replaceState(null, "", "#lab=" + id);
  }

  undo.addEventListener("click", () => play({ kind: "undo" }));
  reset.addEventListener("click", () => play({ kind: "reset" }));
  // The titles of the other tabs: read once from a fresh lab of each.
  for (const id of ids) {
    try { const l = new engine.PaxosLab(id); titles[id] = JSON.parse(l.view()).title; if (l.free) l.free(); } catch (e) { titles[id] = id; }
  }
  start(current);
}

for (const mount of document.querySelectorAll("[data-lab]")) {
  import(PAXOS.href)
    .then(async (m) => { await m.default(); return m; })
    .then((engine) => mountLab(mount, engine), (error) => {
      console.warn("paros-consensus in wasm did not load", error);
      mount.replaceChildren(el("p", "aside", "The exercises need paros-consensus compiled to wasm (scripts/build-wasm.sh)."));
    });
}
