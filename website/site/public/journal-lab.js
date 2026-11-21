// "Your turn" on the Journal pages: the exercises of the journal fold, on
// paros-journal compiled to wasm (`JournalLab` in
// crates/support/paros-wasm/src/journal/lab.rs).
//
// The reader makes the calls of three writers. paros-journal answers each one
// (JournalState::apply, resolve), and the engine says when the goal holds. No
// journal logic here: the page draws the view and sends the moves.
//
//   <div data-journal-lab="id id id">   one tab per exercise, in order
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
// The kind of chip an answer gets: green when the write is in, red when it
// is not, plain when the writer must act again.
const DONE = ["Written", "Duplicate", "written", "LeaderSet", "Trimmed"];
const GAP = ["Taken", "Fenced", "not written", "WrongMode", "LeaderRefused", "Empty"];
function answerChip(e) {
  return chip(e.answer, DONE.includes(e.answer) ? "done" : GAP.includes(e.answer) ? "gap" : "");
}

function mountLab(mount, engine) {
  const ids = mount.dataset.journalLab.split(/\s+/).filter(Boolean);
  const course = window.paxosCourse || null;
  const titles = {};
  let lab = null;
  let view = null;
  let current = ids[0];
  const fromHash = /^#lab=([\w-]+)$/.exec(window.location.hash);
  if (fromHash && ids.includes(fromHash[1])) current = fromHash[1];

  const panel = el("section", "panel lab jlab");
  panel.id = "your-turn-lab";
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
  const journal = el("div", "jlab-journal");
  const segHead = el("p", "lab-sub label", "Segments: the records each one took");
  const segments = el("ol", "jlab-segments");
  const writersHead = el("p", "lab-sub label", "Writers: what each one believes");
  const writers = el("div", "lab-nodes jlab-writers");
  const callsHead = el("p", "lab-sub label", "Your calls");
  const calls = el("div", "lab-extras");
  const answerHead = el("p", "lab-sub label", "paros-journal answers");
  const answer = el("div", "jlab-answer");
  answer.setAttribute("aria-live", "polite");
  const history = el("ol", "jlab-history");
  const controls = el("div", "lab-controls");
  const undo = button("Undo");
  const reset = button("Start again");
  controls.append(undo, reset);
  panel.append(head, tabs, task, done, journal, segHead, segments, writersHead, writers,
    callsHead, calls, answerHead, answer, history, controls);
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
    sent.append(el("code", null, e.sent), document.createTextNode(" "), answerChip(e));
    if (e.lost) sent.append(document.createTextNode(" "), chip("answer lost"));
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

    const j = view.world.journal;
    journal.replaceChildren(strip([["Leader", j.leader], ["next_seq", String(j.next_seq)], ["first_seq", String(j.first_seq)]]));

    segments.replaceChildren();
    for (const s of view.world.segments) {
      const li = el("li", "jlab-segment" + (s.sealed ? " sealed" : ""));
      const name = el("p", "jlab-segment-name label");
      name.append(document.createTextNode(`Segment ${s.n} · from ${s.start} `), chip(s.sealed ? "sealed" : "in force", s.sealed ? "" : "accent"));
      const cells = el("ol", "jlab-records");
      if (!s.records.length) cells.append(el("li", "jlab-none", "no record"));
      for (const r of s.records) {
        const cell = el("li", "jlab-record" + (r.trimmed ? " trimmed" : ""));
        cell.append(el("span", "jlab-seq tnum", String(r.seq)), el("span", "jlab-text", r.text));
        if (r.trimmed) cell.title = "below first_seq: truncated";
        cells.append(cell);
      }
      li.append(name, cells);
      segments.append(li);
    }

    writers.replaceChildren();
    for (const w of view.world.writers) {
      const card = el("div", "lab-node jlab-writer");
      card.dataset.writer = w.name;
      card.append(el("p", "label", "Writer " + w.name));
      const b = w.believes;
      card.append(el("p", "lab-state", `leader ${b.leader} · next_seq ${b.next_seq} · first_seq ${b.first_seq}`));
      card.append(el("p", "lab-state jlab-last", w.last ? "last: " + w.last : "no write yet"));
      writers.append(card);
    }

    calls.replaceChildren();
    view.calls.forEach((c, index) => {
      const b = button(c.label);
      b.dataset.call = c.key;
      b.addEventListener("click", () => play({ kind: "call", index }));
      calls.append(b);
    });

    answer.replaceChildren();
    history.replaceChildren();
    if (view.refused) {
      answer.append(el("p", null, ""));
      answer.firstChild.append(chip("refused", "gap"), document.createTextNode(" " + view.refused));
    } else if (!view.history.length) {
      answer.append(el("p", "lab-empty", "Make a call. Each answer comes from the fold of paros-journal."));
    }
    const [last, ...before] = view.history;
    if (last) {
      const box = el("div", "jlab-last-event");
      box.dataset.answer = last.answer;
      drawEvent(last, box);
      if (last.fin || last.then) {
        const row = el("dl", "jlab-row");
        if (last.fin) row.append(el("dt", "label", "Final?"), el("dd", null, last.fin));
        row.append(el("dt", "label", "What the writer does"), el("dd", null, last.lost ? "Nothing: the writer did not hear this answer." : last.then));
        box.append(row);
      }
      answer.append(box);
    }
    for (const e of before) {
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
    lab = new engine.JournalLab(id);
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
    try { const l = new engine.JournalLab(id); titles[id] = JSON.parse(l.view()).title; if (l.free) l.free(); } catch (e) { titles[id] = id; }
  }
  start(current);
}

for (const mount of document.querySelectorAll("[data-journal-lab]")) {
  import(JOURNAL.href)
    .then(async (m) => { await m.default(); return m; })
    .then((engine) => mountLab(mount, engine), (error) => {
      console.warn("paros-journal in wasm did not load", error);
      mount.replaceChildren(el("p", "aside", "The exercises need paros-journal compiled to wasm (scripts/build-wasm.sh)."));
    });
}
