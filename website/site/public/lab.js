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
// A guided exercise (its view has `parts`) adds: an intro that plays the
// setup one sentence at a time, the goal as a checklist, a Hint in three
// levels (the idea, the plan, an outline on the button to press), a named
// dead end with Undo, and "Show me how", which plays the exercise's guide on
// the live core with one sentence per step. A demo never counts as solved:
// "Now you" starts again. The narrator of the intro and of the demo never
// starts by itself; it has Play or Pause, Next, the speed, and Skip.
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
  // The goal as a checklist, the hint and the dead end.
  const parts = el("ul", "lab-parts");
  const help = el("div", "lab-help");
  const hintBtn = button("Hint", "lab-hint-btn");
  const showBtn = button("Show me how", "lab-show-btn");
  const hintBox = el("div", "lab-hint");
  hintBox.setAttribute("aria-live", "polite");
  help.append(hintBtn, showBtn, hintBox);
  const stuck = el("div", "callout lab-stuck");
  stuck.dataset.tag = "dead end";
  // The narrator of the intro and of "Show me how".
  const narr = el("div", "lab-narrator");
  const narrSays = el("p", "lab-narrator-says");
  narrSays.setAttribute("aria-live", "polite");
  const narrBar = el("div", "lab-narrator-bar");
  const narrAt = el("span", "label tnum lab-narrator-at");
  const narrPlay = button("Play", "primary lab-narrator-play");
  const narrNext = button("Next");
  const narrSpeed = button("1×", "lab-narrator-speed");
  narrSpeed.title = "Speed";
  const narrSkip = button("Skip", "lab-narrator-skip");
  narrBar.append(narrAt, narrPlay, narrNext, narrSpeed, narrSkip);
  narr.append(narrSays, narrBar);
  const said = el("p", "lab-said");
  said.setAttribute("aria-live", "polite");
  const controls = el("div", "lab-controls");
  const all = button("Deliver all");
  all.title = "Deliver every message, and the ones they send, until the wire is empty";
  const undo = button("Undo");
  const reset = button("Start again");
  controls.append(all, undo, reset);
  panel.append(head, tabs, task, ruleOff, parts, narr, stage, chosen, stuck, done, help, wireHead, wire, extrasHead, extras, said, controls);
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
      nodes: v.nodes,
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
    if (before && moved && !reduced) return scene.step(sceneOf(before), sceneOf(view), moved, mine);
    scene.draw(sceneOf(view));
    return Promise.resolve(true);
  }

  function draw(before, moved) {
    titles[view.id] = view.title;
    meta.textContent = view.moves + (view.moves === 1 ? " move" : " moves");
    drawTabs();
    task.textContent = view.task;
    ruleOff.hidden = !view.rule_off;
    ruleOff.replaceChildren(el("p", null, view.rule_off || ""));
    const moving = drawStage(before, moved);
    drawGuide();

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
      li.dataset.id = String(m.id);
      li.style.setProperty("--phase", `var(--phase-${m.phase || "other"})`);
      const what = el("span", "lab-msg-what");
      what.append(el("b", "lab-msg-label", m.label));
      what.append(el("span", "lab-msg-route", nameOf(view, m.from) + " → " + nameOf(view, m.to)));
      what.title = m.summary;
      const go = button("Deliver", "primary lab-deliver");
      go.addEventListener("click", () => play({ kind: "deliver", id: m.id }));
      const lose = button("Drop", "lab-drop");
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
    // While the narrator runs, the reader's moves wait: the demo plays its
    // guide alone, and the intro is the setup.
    const busy = narrating !== null;
    for (const b of panel.querySelectorAll(".lab-wire .btn, .lab-extras .btn")) b.disabled = busy;
    if (busy) { all.disabled = true; undo.disabled = true; }
    outline();
    return moving;
  }

  // The checklist, the hint and the dead end of a guided exercise.
  let hintLevel = 0;   // 0: no hint shown; 1 the idea; 2 the plan; 3 the move
  let hintPart = -1;   // the line the hint is about
  function drawGuide() {
    const guided = view.parts && view.parts.length > 0;
    parts.hidden = !guided;
    parts.replaceChildren();
    for (const part of view.parts || []) {
      const li = el("li", part.holds ? "holds" : "");
      li.append(el("span", "lab-part-mark", part.holds ? "✓" : "○"), document.createTextNode(" " + part.label));
      parts.append(li);
    }
    const hint = view.hint || null;
    if (!hint || hint.part !== hintPart) { hintLevel = 0; hintPart = hint ? hint.part : -1; }
    help.hidden = !guided || view.solved || narrating !== null;
    hintBtn.hidden = !hint;
    hintBtn.disabled = hintLevel >= 3;
    hintBtn.textContent = hintLevel === 0 ? "Hint" : hintLevel < 3 ? "More hint" : "Hint";
    showBtn.hidden = !(view.guide > 0) || view.demo;
    hintBox.replaceChildren();
    if (hint && hintLevel >= 1) hintBox.append(el("p", null, hint.idea));
    if (hint && hintLevel >= 2) hintBox.append(el("p", null, hint.plan));
    if (hint && hintLevel >= 3) hintBox.append(el("p", "lab-hint-move", hint.next ? "Press the outlined button." : "No button does this step alone: use the plan."));
    stuck.hidden = !view.stuck;
    stuck.replaceChildren();
    if (view.stuck) {
      stuck.append(el("p", null, view.stuck.says));
      const back = button("Undo", "primary");
      back.addEventListener("click", () => play({ kind: "undo" }));
      stuck.append(back);
    }
  }
  hintBtn.addEventListener("click", () => { hintLevel = Math.min(3, hintLevel + 1); drawGuide(); outline(); });

  // The button of `move` on the page, if one is there.
  function buttonOf(move) {
    if (!move) return null;
    if (move.kind === "deliver" || move.kind === "drop") {
      const row = wire.querySelector(`li[data-id="${move.id}"]`);
      return row ? row.querySelector(move.kind === "deliver" ? ".lab-deliver" : ".lab-drop") : null;
    }
    if (move.kind === "extra") return extras.children[move.index] || null;
    if (move.kind === "deliver_all") return all;
    if (move.kind === "undo") return undo;
    return null;
  }
  let shown = null;    // the move the outline marks
  function outline() {
    for (const b of panel.querySelectorAll(".lab-next")) b.classList.remove("lab-next");
    const move = shown || (hintLevel >= 3 && view.hint ? view.hint.next : null);
    const b = buttonOf(move);
    if (b) b.classList.add("lab-next");
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
    closeNarrator();
    if (lab && lab.free) lab.free();
    lab = new engine.PaxosLab(id);
    view = null;
    hintLevel = 0;
    const first = JSON.parse(lab.view());
    if (first.intro > 0) {
      // The intro: the stage starts before the setup, and each step plays
      // its moves with its sentence.
      const steps = [];
      for (let k = 0; k < first.intro; k++) steps.push(JSON.parse(lab.intro_step(k)));
      narrate("intro", JSON.parse(lab.intro_start()), (k) => steps[k] || null, steps.length);
    } else show(lab.view());
    if (window.history && window.history.replaceState) window.history.replaceState(null, "", "#lab=" + id);
  }

  // The narrator. `fetch(k)` answers step k ({ says, frames }) or null;
  // each frame is one move: the message it moved and the view after it.
  // Play goes on by itself at the pace of reading; it never starts alone.
  let narrating = null;   // { what: "intro" | "demo", at, n, fetch, playing }
  let narrSpeedV = 1;
  const SPEEDS = [1, 1.5, 0.75];
  function readMs(text) {
    const n = String(text || "").split(/\s+/).filter(Boolean).length;
    return Math.min(9000, Math.max(2200, 330 * n)) / narrSpeedV;
  }
  function narrate(what, startView, fetch, n) {
    stopNarrator();
    narrating = { what, at: 0, n, fetch, playing: false, run: 0 };
    // The narrator's outline is the only one: no hint outline under it.
    hintLevel = 0;
    panel.classList.add("narrating");
    panel.classList.toggle("demo", what === "demo");
    narr.hidden = false;
    narrPlay.hidden = reduced;
    narrSpeed.hidden = reduced;
    // With reduced motion, Next is the main button.
    narrNext.classList.toggle("primary", reduced);
    narrSkip.textContent = what === "intro" ? "Skip" : "Stop";
    scene.speed(narrSpeedV);
    view = startView;
    draw(null, null);
    sayStep();
  }
  function sayStep() {
    const nr = narrating;
    const step = nr.at < nr.n ? nr.fetch(nr.at) : null;
    narrAt.textContent = nr.what === "intro" ? `Intro ${Math.min(nr.at + 1, nr.n)}/${nr.n}` : `Step ${Math.min(nr.at + 1, nr.n)}/${nr.n}`;
    if (!step) { finish(); return; }
    narrSays.textContent = step.says;
    narrPlay.textContent = nr.playing ? "Pause" : "Play";
    narrNext.textContent = "Next";
    // While the reader reads why, the button of the step's first move has
    // the outline.
    shown = step.frames.length ? moveOf(step.frames[0]) : null;
    outline();
  }
  // The move of a frame, as the button that plays it.
  function moveOf(frame) {
    if (frame.message != null) return { kind: frame.lost ? "drop" : "deliver", id: frame.message };
    if (frame.extra != null) return { kind: "extra", index: frame.extra };
    return null;
  }
  // Play the moves of the current step, then wait the time to read, then go
  // on when playing.
  async function playStep() {
    const nr = narrating;
    if (!nr || nr.busy) return;
    const step = nr.fetch(nr.at);
    if (!step) { finish(); return; }
    nr.busy = true;
    narrNext.disabled = true;
    const run = ++nr.run;
    for (const frame of step.frames) {
      if (narrating !== nr || nr.run !== run) return;
      const before = view;
      // Before a move, its button gets the outline.
      shown = moveOf(frame);
      outline();
      if (shown && !reduced) await scene.wait(450, scene.begin());
      if (narrating !== nr || nr.run !== run) return;
      shown = null;
      view = frame.view;
      await draw(before, frame.message != null ? { message: frame.message, lost: frame.lost } : null);
      if (!reduced && narrating === nr && nr.run === run) await scene.wait(250, scene.begin());
      if (narrating !== nr || nr.run !== run) return;
    }
    nr.busy = false;
    narrNext.disabled = false;
    nr.at += 1;
    if (nr.at >= nr.n) { finish(); return; }
    sayStep();
    if (nr.playing) {
      nr.timer = setTimeout(() => { if (narrating === nr && nr.playing) playStep(); }, readMs(narrSays.textContent));
    }
  }
  function finish() {
    const nr = narrating;
    if (!nr) return;
    clearTimeout(nr.timer);
    nr.playing = false;
    narrPlay.hidden = true;
    narrSpeed.hidden = true;
    narrNext.hidden = true;
    narrAt.textContent = "";
    if (nr.what === "intro") {
      endNarrator();
      return;
    }
    // The last sentence stays: the reader may not have read it yet.
    narrAt.textContent = "The demo does not count";
    narrSkip.textContent = "Now you";
    narrSkip.classList.add("primary");
    nr.done = true;
  }
  function stopNarrator() {
    if (!narrating) return;
    clearTimeout(narrating.timer);
    narrating.run++;
    narrating = null;
    shown = null;
    scene.cancel();
  }
  function closeNarrator() {
    stopNarrator();
    // The reader's own moves play at the stage's own pace.
    scene.speed(1);
    panel.classList.remove("narrating", "demo");
    narr.hidden = true;
    narrPlay.hidden = false; narrSpeed.hidden = false; narrNext.hidden = false;
    narrNext.disabled = false;
    narrSkip.classList.remove("primary");
  }
  // Leave the narrator: the page shows the lab as the engine has it now.
  function endNarrator() {
    closeNarrator();
    view = null;
    show(lab.view());
  }
  narr.hidden = true;
  narrPlay.addEventListener("click", () => {
    const nr = narrating;
    if (!nr) return;
    nr.playing = !nr.playing;
    narrPlay.textContent = nr.playing ? "Pause" : "Play";
    clearTimeout(nr.timer);
    if (nr.playing && !nr.busy) playStep();
  });
  narrNext.addEventListener("click", () => {
    const nr = narrating;
    if (!nr) return;
    nr.playing = false;
    clearTimeout(nr.timer);
    narrPlay.textContent = "Play";
    playStep();
  });
  narrSpeed.addEventListener("click", () => {
    narrSpeedV = SPEEDS[(SPEEDS.indexOf(narrSpeedV) + 1) % SPEEDS.length];
    narrSpeed.textContent = (narrSpeedV === 0.75 ? "0.75" : String(narrSpeedV)) + "×";
    if (narrating) scene.speed(narrSpeedV);
  });
  narrSkip.addEventListener("click", () => {
    const nr = narrating;
    if (!nr) return;
    // Skip the intro: the lab is already past it. Stop or Now you after a
    // demo: start the exercise again, with no intro.
    if (nr.what === "intro") { endNarrator(); return; }
    stopNarrator();
    lab.play(JSON.stringify({ kind: "reset" }));
    endNarrator();
  });
  // A hidden page pauses the narrator.
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden || !narrating) return;
    narrating.playing = false;
    clearTimeout(narrating.timer);
    narrPlay.textContent = "Play";
  });
  // "Show me how": the exercise from its start, then the guide, step by
  // step. The press is a press to play.
  showBtn.addEventListener("click", () => {
    lab.play(JSON.stringify({ kind: "reset" }));
    const fresh = JSON.parse(lab.view());
    const steps = [];
    narrate("demo", fresh, (k) => {
      if (steps[k] === undefined) {
        const s = JSON.parse(lab.guide_step(k));
        steps[k] = s;
      }
      return steps[k];
    }, fresh.guide);
    // The press is a press to play; with reduced motion, Next plays each
    // step after its sentence.
    if (!reduced) { narrating.playing = true; narrPlay.textContent = "Pause"; playStep(); }
  });

  all.addEventListener("click", () => play({ kind: "deliver_all" }));
  undo.addEventListener("click", () => play({ kind: "undo" }));
  // A link to "#lab=<id>" (the end of a lesson's story) opens that tab.
  window.addEventListener("hashchange", () => {
    const to = /^#lab=([\w-]+)$/.exec(window.location.hash);
    if (!to || !ids.includes(to[1])) return;
    if (to[1] !== current) start(to[1]);
    panel.scrollIntoView({ block: "start" });
  });
  reset.addEventListener("click", () => {
    if (narrating) { closeNarrator(); view = null; }
    play({ kind: "reset" });
  });
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
