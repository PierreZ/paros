// "Your turn": the exercises of the Paxos course, on paros-consensus compiled
// to wasm (`PaxosLab` in crates/support/paros-wasm/src/paxos/lab.rs).
//
// The reader is the network: each message on the wire has Deliver and Drop,
// and a delivered or dropped message crosses the stage (public/stage.js,
// loaded before this module).
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

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}
function button(text, cls) {
  const b = el("button", "btn" + (cls ? " " + cls : ""), text);
  b.type = "button";
  return b;
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
  const all = button("Deliver all");
  all.title = "Deliver every message, and the ones they send, until the wire is empty";
  const undo = button("Undo");
  const reset = button("Start again");
  controls.append(all, undo, reset);
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

  // The parties and the wire, on the stage the story uses
  // (public/stage.js): a grid lane at its cell, the log strip under each
  // node of a log, and each message as an arrow on its path.
  const scene = window.paxosStage.create(stage);
  let staged = null;  // the exercise the stage is laid out for
  const reduced = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function sceneOf(v) {
    return {
      nodes: v.lanes.map((_, i) => ({ alive: v.alive[i] !== false, holes: (v.holes && v.holes[i]) || [] })),
      state: v.state,
      logs: v.logs || [],
      wire: v.wire,
    };
  }

  // Draw the stage for `view`. When the last move moved one message
  // (`moved`, with the view before it), the message crosses first.
  function drawStage(before, moved) {
    if (staged !== view.id) {
      const cols = Math.max(0, ...view.lanes.map((l) => (l.cell ? l.cell.column + 1 : 0)));
      scene.layout(view.lanes, { grid: cols ? { cols } : null, log: (view.logs || []).length > 0 });
      staged = view.id;
      before = null;
    }
    const mine = scene.begin();
    if (before && moved && !reduced) scene.step(sceneOf(before), sceneOf(view), moved, mine);
    else scene.draw(sceneOf(view));
  }

  function draw(before, moved) {
    titles[view.id] = view.title;
    meta.textContent = view.moves + (view.moves === 1 ? " move" : " moves");
    drawTabs();
    task.textContent = view.task;
    ruleOff.hidden = !view.rule_off;
    ruleOff.replaceChildren(el("p", null, view.rule_off || ""));
    drawStage(before, moved);

    chosen.replaceChildren();
    if (view.two_values) chosen.append(el("span", "chip-status gap", "two values chosen"));
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
      // What the message is, for a script that plays the network (the
      // browser smoke test, website/site/tests/).
      li.dataset.kind = m.kind;
      li.dataset.from = m.from;
      li.dataset.to = m.to;
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
    all.disabled = !view.wire.length;
  }

  function show(json, moved) {
    const next = JSON.parse(json);
    if (next.error) { said.textContent = next.error; return; }
    const was = view && view.id === next.id && view.solved;
    const before = view && view.id === next.id ? view : null;
    view = next;
    if (view.solved && !was) {
      if (course && course.solved) course.solved(view.id);
      try { document.dispatchEvent(new CustomEvent("paxos:solved", { detail: { id: view.id } })); } catch (e) { /* old browser */ }
    }
    draw(before, next.refused ? null : moved);
  }

  function play(move) {
    // A delivered or dropped message crosses the stage before the new state
    // shows.
    const one = move.kind === "deliver" || move.kind === "drop";
    show(lab.play(JSON.stringify(move)), one ? { message: move.id, lost: move.kind === "drop" } : null);
  }

  function start(id) {
    current = id;
    if (lab && lab.free) lab.free();
    lab = new engine.PaxosLab(id);
    view = null;
    show(lab.view());
    if (window.history && window.history.replaceState) window.history.replaceState(null, "", "#lab=" + id);
  }

  all.addEventListener("click", () => play({ kind: "deliver_all" }));
  undo.addEventListener("click", () => play({ kind: "undo" }));
  // A link to "#lab=<id>" (the end of a lesson's story) opens that tab.
  window.addEventListener("hashchange", () => {
    const to = /^#lab=([\w-]+)$/.exec(window.location.hash);
    if (!to || !ids.includes(to[1])) return;
    if (to[1] !== current) start(to[1]);
    panel.scrollIntoView({ block: "start" });
  });
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
