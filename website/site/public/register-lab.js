// "Your turn" on the Matchmakers and Segments pages: the journal's matchmaker
// register and its segment records, on paros-journal compiled to wasm
// (`RegisterLab` and `RecordLab` in crates/support/paros-wasm/src/journal/).
//
// The engine lists the moves open now, each with a key; the page draws the
// view and sends the key of the button the reader clicks. Every reply, every
// guard and every "done" comes from paros-journal. No register logic here.
//
//   <div data-register-lab="id id id">   two controllers, three matchmakers
//   <div data-record-lab="id id id">     the records of the segments
//
// A solved exercise is kept in the browser (public/course.js) and fires
// "paxos:solved" on the document.
const JOURNAL = new URL("journal/wasm/paros_journal_lab.js", import.meta.url);

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
function chip(text, kind) {
  return el("span", "chip-status" + (kind ? " " + kind : ""), text);
}
function strip(items) {
  const s = el("div", "strip");
  for (const [k, v] of items) {
    const d = el("div");
    d.append(el("span", "k", k), el("span", "v", v));
    s.append(d);
  }
  return s;
}

// The buttons of one group of moves.
function moves(view, group, play) {
  const box = el("div", "lab-extras");
  for (const a of view.actions.filter((a) => a.group === group)) {
    const b = button(a.label);
    b.dataset.key = a.key;
    b.addEventListener("click", () => play({ kind: "act", key: a.key }));
    box.append(b);
  }
  return box;
}

// The register: the matchmakers, then each controller with its moves.
function drawRegister(view, into, play) {
  const w = view.world;
  into.append(el("p", "lab-sub label", "Matchmakers: what each one holds"));
  const members = el("div", "lab-nodes rlab-members");
  for (const m of w.members) {
    const card = el("div", "lab-node rlab-card" + (m.down ? " down" : ""));
    card.dataset.member = m.name;
    const name = el("p", "label");
    name.append(document.createTextNode(m.name + " "), chip(m.down ? "down" : "up", m.down ? "gap" : ""));
    card.append(name);
    card.append(el("p", "lab-state", `read ${m.read} · write ${m.written}`));
    card.append(el("p", "lab-state", m.value.term));
    for (const s of m.value.segments) card.append(el("p", "lab-state", s));
    members.append(card);
  }
  into.append(members, moves(view, "matchmakers", play));
  for (const c of w.callers) {
    into.append(el("p", "lab-sub label", "Controller " + c.name));
    const card = el("div", "rlab-caller");
    card.dataset.caller = c.name;
    card.append(el("p", "lab-state", (c.op ? c.op + " · " : "") + c.phase + " · " + c.believes));
    into.append(card, moves(view, "controller " + c.name, play));
  }
}

// The records: the journal's moves, then a strip of the segments.
function drawRecords(view, into, play) {
  const w = view.world;
  into.append(strip([["Controller term", String(w.term)], ["first_seq", String(w.first_seq)]]));
  into.append(moves(view, "journal", play));
  into.append(el("p", "lab-sub label", "Segment records"));
  const list = el("ol", "jlab-segments");
  for (const r of w.records) {
    const li = el("li", "jlab-segment" + (r.state === "in force" ? "" : " sealed"));
    li.dataset.segment = String(r.n);
    const name = el("p", "jlab-segment-name label");
    name.append(document.createTextNode(`Segment ${r.n} `), chip(r.state, r.state === "in force" ? "accent" : ""));
    li.append(name);
    if (r.slots) li.append(el("p", "jlab-detail", r.slots));
    if (r.boundary) li.append(el("p", "jlab-detail", "boundary: " + r.boundary));
    li.append(moves(view, "segment " + r.n, play));
    list.append(li);
  }
  into.append(list);
}

function mountLab(mount, ids, Engine, drawWorld, kind) {
  const course = window.paxosCourse || null;
  const titles = {};
  let lab = null;
  let view = null;
  let current = ids[0];
  const fromHash = /^#lab=([\w-]+)$/.exec(window.location.hash);
  if (fromHash && ids.includes(fromHash[1])) current = fromHash[1];

  const panel = el("section", "panel lab jlab rlab");
  panel.id = "your-turn-" + kind;
  const head = el("div", "panel-head label");
  head.append(el("span", "title", "Your turn"));
  const live = chip("live", "accent");
  live.title = "paros-journal runs in this page";
  head.append(live);
  const meta = el("span", "meta tnum");
  head.append(meta);
  const tabs = el("div", "lab-tabs");
  const task = el("p", "lab-task");
  const done = el("div", "callout lab-done");
  done.dataset.tag = "goal reached";
  const world = el("div", "rlab-world");
  const answerHead = el("p", "lab-sub label", "paros-journal answers");
  const answer = el("div", "jlab-answer");
  answer.setAttribute("aria-live", "polite");
  const history = el("ol", "jlab-history");
  const controls = el("div", "lab-controls");
  const undo = button("Undo");
  const reset = button("Start again");
  controls.append(undo, reset);
  panel.append(head, tabs, task, done, world, answerHead, answer, history, controls);
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
      b.append(document.createTextNode(titles[id] || id));
      b.addEventListener("click", () => start(id));
      tabs.append(b);
    });
  }

  function drawEvent(e, into) {
    const sent = el("p", "jlab-sent");
    sent.append(el("code", null, e.sent));
    if (e.answer) sent.append(document.createTextNode(" "), chip(e.answer, e.tone || ""));
    if (e.lost) sent.append(document.createTextNode(" "), chip(e.answer === "dropped" ? "request lost" : "reply lost"));
    into.append(sent);
    if (e.detail) into.append(el("p", "jlab-detail", e.detail));
  }

  function draw() {
    titles[view.id] = view.title;
    meta.textContent = view.moves + (view.moves === 1 ? " move" : " moves");
    drawTabs();
    task.textContent = view.task;

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

    world.replaceChildren();
    drawWorld(view, world, play);

    answer.replaceChildren();
    history.replaceChildren();
    const events = view.world.history.slice(view.setup).reverse();
    if (view.refused) {
      const p = el("p", "rlab-refused");
      p.append(chip("refused", "gap"), document.createTextNode(" " + view.refused));
      answer.append(p);
    } else if (!events.length) {
      answer.append(el("p", "lab-empty", "Make a move. Each answer comes from paros-journal."));
    }
    const [last, ...before] = events;
    if (last) {
      const box = el("div", "jlab-last-event");
      drawEvent(last, box);
      answer.append(box);
    }
    for (const e of before.slice(0, 8)) {
      const li = el("li");
      drawEvent(e, li);
      history.append(li);
    }
    history.hidden = !before.length;
    undo.disabled = view.moves === 0;
  }

  function show(json) {
    const next = JSON.parse(json);
    if (next.error) { answer.replaceChildren(el("p", null, next.error)); return; }
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
    lab = new Engine(id);
    view = null;
    show(lab.view());
    if (window.history && window.history.replaceState) window.history.replaceState(null, "", "#lab=" + id);
  }

  undo.addEventListener("click", () => play({ kind: "undo" }));
  reset.addEventListener("click", () => play({ kind: "reset" }));
  window.addEventListener("hashchange", () => {
    const to = /^#lab=([\w-]+)$/.exec(window.location.hash);
    if (!to || !ids.includes(to[1])) return;
    if (to[1] !== current) start(to[1]);
    panel.scrollIntoView({ block: "start" });
  });
  // The titles of the other tabs: read once from a fresh lab of each.
  for (const id of ids) {
    try { const l = new Engine(id); titles[id] = JSON.parse(l.view()).title; if (l.free) l.free(); } catch (e) { titles[id] = id; }
  }
  start(current);
}

const MOUNTS = [
  ["registerLab", "[data-register-lab]", "RegisterLab", drawRegister, "register"],
  ["recordLab", "[data-record-lab]", "RecordLab", drawRecords, "records"],
];
for (const [data, selector, engineName, drawWorld, kind] of MOUNTS) {
  for (const mount of document.querySelectorAll(selector)) {
    const ids = mount.dataset[data].split(/\s+/).filter(Boolean);
    import(JOURNAL.href)
      .then(async (m) => { await m.default(); return m; })
      .then((engine) => mountLab(mount, ids, engine[engineName], drawWorld, kind), (error) => {
        console.warn("paros-journal in wasm did not load", error);
        mount.replaceChildren(el("p", "aside", "The exercises need paros-journal compiled to wasm (scripts/build-wasm.sh)."));
      });
  }
}
