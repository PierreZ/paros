// The story mode of the Paxos pages (#1147 (story-mode renderer)).
//
// A run with beats (src/traces/<page>.json, written by
// `cargo test -p paros-wasm --test paxos_traces`) is told one screen at a
// time: one written sentence, the nodes drawn in a fixed layout, and the
// messages of the beat moving from sender to receiver. A broadcast leaves as
// one and arrives one by one. A lost message stops half way and fades. A
// node that is down goes dark. A disk write shows on the node before its
// reply leaves, because the order is the point.
//
// The page opens in story mode when its trace has a run with beats. The
// link "Show each message" opens the step-through (public/steps.js) at the
// same point, and the step-through has a link back. The left and right
// arrow keys move between screens, and the address #run=<id>&beat=<n> names
// a screen. With reduced motion, a screen appears complete.
//
// A beat can ask a question before it plays. The screen then shows the
// state before the beat, the question and its answers. The reader answers
// (or presses "Show me"), the beat plays, and the screen says which answer
// is right and why. A wrong answer costs nothing. The answers live in this
// page only; window.paxosStory.answers() lists them, and each answer fires
// the event "paxos:answer" on the document.
//
// No protocol logic here: every sentence, state and message comes from the
// trace. This script runs before steps.js.
(function () {
  var data = document.getElementById("trace");
  var mount = document.querySelector("[data-steps]");
  if (!data || !mount) return;
  var trace = JSON.parse(data.textContent);
  var courseData = document.getElementById("course");
  var course = courseData ? JSON.parse(courseData.textContent) : { lessons: [] };
  var progress = window.paxosCourse || null;
  var stories = trace.scenarios.filter(function (run) { return run.beats && run.beats.length; });
  if (!stories.length) return;

  var SVG = "http://www.w3.org/2000/svg";
  var MOVE = 650;      // ms for a message to cross
  var EMERGE = 260;    // ms for new messages to leave their sender
  var REST = 0.42;     // where a message waits on its path, from 0 to 1
  // A message: a short shaft and a head that points along +x, about the size
  // of a node label so that the direction reads on a phone.
  var ARROW = "M-9 -1.6 H0 V-5.5 L9 0 L0 5.5 V1.6 H-9 Z";
  var reduced = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  // Rows of the drawing, top to bottom, by the role of a lane.
  var ROWS = ["proposer", "batcher", "matchmaker", "proxy", "node", "acceptor", "replica"];

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  function svg(tag, attrs, text) {
    var e = document.createElementNS(SVG, tag);
    Object.keys(attrs || {}).forEach(function (k) { e.setAttribute(k, attrs[k]); });
    if (text != null) e.textContent = text;
    return e;
  }
  function plain(label) { return label.replace(/­/g, ""); }
  function short(lane) {
    var m = /^(\w)\w*\D*(\d+)$/.exec(plain(lane.label));
    if (lane.role === "batcher") return "b";
    return m ? m[1] + m[2] : lane.id;
  }

  // ---- the frame ----

  var panel = el("section", "panel story");
  var head = el("div", "panel-head label");
  head.appendChild(el("span", "title", "Story"));
  var chip = el("span", "chip-status", "recorded");
  chip.title = "recorded from paros-consensus at build time";
  head.appendChild(chip);
  var counter = el("span", "meta tnum");
  head.appendChild(counter);
  panel.appendChild(head);

  var runName = el("div", "story-run label");
  panel.appendChild(runName);
  var stage = el("div", "story-stage");
  panel.appendChild(stage);
  var says = el("p", "story-says");
  says.setAttribute("aria-live", "polite");
  panel.appendChild(says);
  var quiz = el("div", "story-quiz");
  quiz.hidden = true;
  panel.appendChild(quiz);
  var chosen = el("p", "story-chosen");
  panel.appendChild(chosen);
  var choices = el("div", "story-choices");
  panel.appendChild(choices);

  var controls = el("div", "story-controls");
  var backBtn = el("button", "btn", "Back");
  var nextBtn = el("button", "btn primary", "Continue");
  [backBtn, nextBtn].forEach(function (b) { b.type = "button"; controls.appendChild(b); });
  panel.appendChild(controls);

  var others = el("details", "story-others");
  if (stories.length > 1) {
    others.appendChild(el("summary", "label story-others-title", "All stories"));
    stories.forEach(function (run) {
      var b = el("button", "steps-chapter", run.label);
      b.type = "button";
      b.dataset.run = run.id;
      b.addEventListener("click", function () { open(run, 0, true); });
      others.appendChild(b);
    });
    panel.appendChild(others);
  }

  var foot = el("p", "aside story-foot");
  var toSteps = el("a", null, "Show each message");
  toSteps.href = "#";
  toSteps.addEventListener("click", function (e) {
    e.preventDefault();
    if (!window.paxosSteps) return;
    var beat = run.beats[at];
    panel.hidden = true;
    window.paxosSteps.show(run.id, beat.to);
  });
  foot.appendChild(toSteps);
  foot.appendChild(document.createTextNode(": the same run, one message per step, with what each message carries."));
  panel.appendChild(foot);

  mount.parentNode.insertBefore(panel, mount);
  // The step-through draws into the mount, hidden until the reader asks.
  mount.hidden = true;

  // ---- the drawing ----

  var run = null;
  var at = 0;          // the beat on screen
  var lanes = [];      // { lane, i, x, y }
  var byId = {};
  var box = { w: 360, h: 200 };
  var drawing = null;  // the svg
  var nodesLayer, wireLayer, moving = null;
  var token = 0;       // cancels an animation in progress

  function runLanes() { return run.lanes || trace.lanes; }

  function layout() {
    var used = {};
    run.steps.forEach(function (step) {
      if (step.from) used[step.from] = true;
      if (step.to) used[step.to] = true;
      (step.wire || []).forEach(function (f) { used[f.from] = true; used[f.to] = true; });
    });
    var all = runLanes();
    if (!Object.keys(used).length) all.forEach(function (l) { used[l.id] = true; });
    var rows = {};
    lanes = [];
    byId = {};
    all.forEach(function (lane, i) {
      if (!used[lane.id]) return;
      var r = ROWS.indexOf(lane.role);
      var key = (r < 0 ? ROWS.length : r) * 10 + (lane.cell ? lane.cell.row : 0);
      (rows[key] = rows[key] || []).push({ lane: lane, i: i });
    });
    var keys = Object.keys(rows).map(Number).sort(function (a, b) { return a - b; });
    var widest = 1;
    var grid = run.lanes ? run.grid : trace.grid;
    keys.forEach(function (k) {
      widest = Math.max(widest, grid && rows[k][0].lane.cell ? grid.cols : rows[k].length);
    });
    var log = run.steps.some(function (s) { return s.logs && s.logs.length; });
    var rowH = log ? 150 : 124;
    box.w = Math.max(360, widest * 84);
    box.h = keys.length * rowH + 16;
    keys.forEach(function (k, r) {
      var list = rows[k];
      list.forEach(function (entry, c) {
        var n = grid && entry.lane.cell ? grid.cols : list.length;
        var col = grid && entry.lane.cell ? entry.lane.cell.column : c;
        entry.x = ((col + 0.5) / n) * box.w;
        entry.w = box.w / n;
        entry.y = r * rowH + 34;
        lanes.push(entry);
        byId[entry.lane.id] = entry;
      });
    });
    stage.textContent = "";
    drawing = svg("svg", { viewBox: "0 0 " + box.w + " " + box.h, class: "story-svg", role: "img" });
    drawing.style.maxWidth = Math.min(box.w * 1.25, 600) + "px";
    wireLayer = svg("g", { class: "story-paths" });
    drawing.appendChild(wireLayer);
    nodesLayer = svg("g", { class: "story-nodes" });
    drawing.appendChild(nodesLayer);
    stage.appendChild(drawing);
  }

  function stateAt(n) {
    // The state after `n` steps of the run.
    if (n === 0) return { nodes: run.start_nodes || [], logs: run.start_logs || [], wire: run.start_wire || [], chosen: [], state: run.start };
    var s = run.steps[n - 1];
    return { nodes: s.nodes || [], logs: s.logs || [], wire: s.wire || [], chosen: s.chosen || [], state: s.state, broken: s.broken };
  }

  function drawNodes(st, writes) {
    nodesLayer.textContent = "";
    lanes.forEach(function (entry) {
      var node = st.nodes[entry.i] || { alive: true };
      var lines = (st.state && st.state[entry.i]) || [];
      var g = svg("g", { class: "story-node " + entry.lane.role + (node.alive ? "" : " down") + (node.role === "leader" ? " leader" : "") });
      g.appendChild(svg("circle", { cx: entry.x, cy: entry.y, r: 17 }));
      g.appendChild(svg("text", { x: entry.x, y: entry.y + 4, class: "story-node-id" }, short(entry.lane)));
      var y = entry.y + 32;
      // A state line keeps to the node's own column: a long one (a
      // matchmaker's registry) is cut, and the node's title holds it whole.
      var room = Math.max(8, Math.floor((entry.w - 8) / 6.2));
      var clip = function (text) { return text.length > room ? text.slice(0, room - 1) + "…" : text; };
      var first = node.alive ? (lines[0] || "") : "down";
      g.appendChild(svg("text", { x: entry.x, y: y, class: "story-node-line" }, clip(first)));
      if (node.alive && lines[1]) g.appendChild(svg("text", { x: entry.x, y: y + 12, class: "story-node-line" }, clip(lines.slice(1).join(" "))));
      // The log: one box per slot, a dashed box for a hole.
      var cells = (st.logs && st.logs[entry.i]) || [];
      var holes = node.holes || [];
      if (node.alive && (cells.length || holes.length)) {
        var slots = cells.map(function (c) { return { slot: c.slot, cls: c.state + (c.control ? " control" : ""), v: c.value }; })
          .concat(holes.map(function (h) { return { slot: h, cls: "hole", v: "" }; }))
          .sort(function (a, b) { return a.slot - b.slot; }).slice(-5);
        var w = 13, x0 = entry.x - (slots.length * w) / 2;
        slots.forEach(function (s, k) {
          var cell = svg("g", { class: "story-slot " + s.cls });
          cell.appendChild(svg("rect", { x: x0 + k * w, y: y + 18, width: w - 2, height: 12 }));
          cell.appendChild(svg("text", { x: x0 + k * w + (w - 2) / 2, y: y + 27 }, s.v ? s.v.charAt(0) : ""));
          cell.appendChild(svg("title", {}, "slot " + s.slot + (s.v ? ": " + s.v : ": hole") + " (" + s.cls.split(" ")[0] + ")"));
          g.appendChild(cell);
        });
      }
      // What this step wrote on the disk, under the node, before it answers.
      var wrote = writes && writes[entry.i];
      if (wrote && wrote.length) {
        var fit = Math.max(6, Math.floor((entry.w - 6) / 5.5));
        var dy = cells.length || holes.length ? 50 : 26;
        wrote.slice(0, 2).forEach(function (w, k) {
          var text = (k === 0 ? "disk " : "") + w;
          if (text.length > fit) text = text.slice(0, fit - 1) + "…";
          var tw = text.length * 5.5 + 6;
          var tag = svg("g", { class: "story-disk" });
          tag.appendChild(svg("rect", { x: entry.x - tw / 2, y: y + dy - 10 + k * 14, width: tw, height: 13 }));
          tag.appendChild(svg("text", { x: entry.x, y: y + dy - 0.5 + k * 14 }, text));
          tag.appendChild(svg("title", {}, "written to disk: " + wrote.join(", ")));
          g.appendChild(tag);
        });
      }
      g.appendChild(svg("title", {}, plain(entry.lane.label) + (node.alive && lines.length ? ": " + lines.join(" ") : "")));
      nodesLayer.appendChild(g);
    });
  }

  // The path of a flight: a gentle arc from its sender to its receiver, kept
  // off the two circles. The arc bends to the right of the direction of
  // travel, so a request and its reply on the same pair of nodes run on two
  // paths, and an arc that would cross a third node bends wide around it.
  function curve(flight) {
    var a = byId[flight.from], b = byId[flight.to];
    if (!a || !b) return null;
    var dx = b.x - a.x, dy = b.y - a.y, len = Math.sqrt(dx * dx + dy * dy) || 1;
    var ux = dx / len, uy = dy / len, pad = 20;
    var bend = 10;
    lanes.forEach(function (n) {
      if (n === a || n === b) return;
      var along = (n.x - a.x) * ux + (n.y - a.y) * uy;
      var across = Math.abs((n.x - a.x) * uy - (n.y - a.y) * ux);
      if (along > 0 && along < len && across < 26) bend = 64;
    });
    var p0 = { x: a.x + ux * pad, y: a.y + uy * pad };
    var p2 = { x: b.x - ux * pad, y: b.y - uy * pad };
    var c = { x: (p0.x + p2.x) / 2 - uy * bend, y: (p0.y + p2.y) / 2 + ux * bend };
    return { p0: p0, c: c, p2: p2 };
  }

  // A point on the path of a flight, from its sender (0) to its receiver (1),
  // with the heading of the path there, in degrees.
  function point(flight, t) {
    var q = curve(flight);
    if (!q) return null;
    var s = 1 - t;
    var x = s * s * q.p0.x + 2 * s * t * q.c.x + t * t * q.p2.x;
    var y = s * s * q.p0.y + 2 * s * t * q.c.y + t * t * q.p2.y;
    var tx = s * (q.c.x - q.p0.x) + t * (q.p2.x - q.c.x);
    var ty = s * (q.c.y - q.p0.y) + t * (q.p2.y - q.c.y);
    return { x: x, y: y, angle: Math.atan2(ty, tx) * 180 / Math.PI };
  }

  // Put a message on its path at `t`: the arrow turns with the path, and
  // its label stays upright.
  function place(g, flight, t) {
    var p = point(flight, t);
    if (!p) return;
    g.setAttribute("transform", "translate(" + p.x + " " + p.y + ")");
    g.firstChild.setAttribute("transform", "rotate(" + p.angle + ")");
  }

  // A message in flight is an arrow that points along its path, at its
  // receiver.
  function dot(flight, t, cls) {
    if (!curve(flight)) return null;
    var g = svg("g", { class: "story-msg " + (cls || "") });
    g.style.color = "var(--phase-" + (flight.phase || "other") + ")";
    var head = svg("g", { class: "story-msg-arrow" });
    head.appendChild(svg("path", { d: ARROW }));
    g.appendChild(head);
    place(g, flight, t);
    g.appendChild(svg("title", {}, flight.label));
    return g;
  }

  function drawWire(wire, skip) {
    wireLayer.textContent = "";
    wire.forEach(function (flight) {
      var q = curve(flight);
      if (!q) return;
      wireLayer.appendChild(svg("path", { d: "M" + q.p0.x + " " + q.p0.y + " Q" + q.c.x + " " + q.c.y + " " + q.p2.x + " " + q.p2.y, class: "story-path" }));
    });
    wire.forEach(function (flight) {
      if (skip && skip[flight.id]) return;
      var d = dot(flight, REST);
      if (d) wireLayer.appendChild(d);
    });
  }

  function tween(ms, fn, mine) {
    return new Promise(function (resolve) {
      var t0 = null;
      function frame(now) {
        if (mine !== token) return resolve(false);
        if (t0 === null) t0 = now;
        var t = Math.min(1, (now - t0) / ms);
        fn(t);
        if (t < 1) requestAnimationFrame(frame); else resolve(true);
      }
      requestAnimationFrame(frame);
    });
  }

  function label(g, flight) {
    var t = svg("text", { x: 0, y: -13, class: "story-msg-label" }, flight.label.split(" · ")[0]);
    g.appendChild(t);
  }

  // Play step `n` (1-based): its message crosses, the receiver writes, then
  // the messages it sent leave the sender.
  function playStep(n, mine) {
    var before = stateAt(n - 1), after = stateAt(n), step = run.steps[n - 1];
    var flight = null;
    // A copy step puts a second copy on the wire: nothing crosses.
    if (step.kind !== "copy") before.wire.forEach(function (f) { if (f.id === step.message) flight = f; });
    var skip = {};
    if (flight) skip[flight.id] = true;
    drawNodes(before);
    drawWire(before.wire, skip);
    var chain = Promise.resolve(true);
    if (flight && byId[flight.from] && byId[flight.to]) {
      var g = dot(flight, REST, "now" + (step.kind === "drop" ? " lost" : ""));
      label(g, flight);
      wireLayer.appendChild(g);
      var end = step.kind === "drop" ? 0.6 : 1;
      chain = tween(MOVE, function (t) {
        var e = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
        place(g, flight, REST + (end - REST) * e);
        if (step.kind === "drop" && t > 0.7) g.style.opacity = String(1 - (t - 0.7) / 0.3);
      }, mine);
    }
    return chain.then(function (ok) {
      if (!ok) return false;
      // The receiver writes, then answers.
      var writes = (after.nodes || []).map(function (node) { return node.writes; });
      drawNodes(after, writes);
      var old = {};
      before.wire.forEach(function (f) { old[f.id] = true; });
      var fresh = after.wire.filter(function (f) { return !old[f.id]; });
      var hold = {};
      fresh.forEach(function (f) { hold[f.id] = true; });
      drawWire(after.wire, hold);
      if (!fresh.length) return tween(writes.some(function (w) { return w && w.length; }) ? 420 : 120, function () {}, mine);
      var wait = writes.some(function (w) { return w && w.length; }) ? 380 : 0;
      return tween(wait, function () {}, mine).then(function (ok2) {
        if (!ok2) return false;
        var dots = fresh.map(function (f) {
          var d = dot(f, 0, "fresh");
          if (d) wireLayer.appendChild(d);
          return { f: f, d: d };
        });
        return tween(EMERGE, function (t) {
          dots.forEach(function (x) {
            if (!x.d) return;
            place(x.d, x.f, REST * t);
          });
        }, mine);
      });
    });
  }

  // The end of a screen: the state after step `n`, with every disk write of
  // the screen's steps from step `from` on.
  function still(n, from) {
    var st = stateAt(n);
    var writes = [];
    for (var k = from == null ? n : from; k < n; k++) {
      (run.steps[k].nodes || []).forEach(function (node, i) {
        (node.writes || []).forEach(function (w) {
          writes[i] = writes[i] || [];
          if (writes[i].indexOf(w) < 0) writes[i].push(w);
        });
      });
    }
    drawNodes(st, writes);
    drawWire(st.wire);
  }

  // ---- questions ----

  // "run:beat" -> the index of the answer, -1 for "Show me". A question the
  // reader answered on an earlier visit is not asked again.
  var asked = progress ? progress.answersOf(trace.page) : {};
  var waiting = false;  // the screen shows a question that has no answer yet

  function key() { return run.id + ":" + at; }

  function answer(i) {
    var q = run.beats[at].question;
    asked[key()] = i;
    var detail = { page: trace.page, run: run.id, beat: at, answer: i, right: i === q.right };
    try { document.dispatchEvent(new CustomEvent("paxos:answer", { detail: detail })); } catch (e) { /* old browser */ }
    go(at, true);
  }

  // The question of the beat on screen: its answers while it waits, then
  // the reader's answer, the right one and why.
  function drawQuiz(beat) {
    var q = beat.question;
    quiz.textContent = "";
    quiz.hidden = !q;
    if (!q) return;
    if (waiting) {
      quiz.appendChild(el("p", "story-quiz-head label", "Before it plays: what happens?"));
      var list = el("div", "story-answers");
      q.answers.forEach(function (text, i) {
        var b = el("button", "story-answer", text);
        b.type = "button";
        b.insertBefore(el("span", "story-answer-key tnum", String(i + 1)), b.firstChild);
        b.addEventListener("click", function () { answer(i); });
        list.appendChild(b);
      });
      quiz.appendChild(list);
      return;
    }
    var mine = asked[key()];
    var head = el("p", "story-quiz-head label");
    if (mine === undefined || mine < 0) {
      head.appendChild(el("span", "chip-status", "answer"));
    } else if (mine === q.right) {
      head.appendChild(el("span", "chip-status done", "right"));
    } else {
      head.appendChild(el("span", "chip-status gap", "not this time"));
    }
    head.appendChild(document.createTextNode(" " + q.asks));
    quiz.appendChild(head);
    var list = el("ul", "story-answers done");
    q.answers.forEach(function (text, i) {
      var cls = i === q.right ? "right" : (i === mine ? "wrong" : "");
      var li = el("li", "story-answer " + cls);
      li.appendChild(el("span", "story-answer-key tnum", i === q.right ? "✓" : (i === mine ? "✗" : String(i + 1))));
      li.appendChild(document.createTextNode(text));
      list.appendChild(li);
    });
    quiz.appendChild(list);
    quiz.appendChild(el("p", "story-why", q.why));
  }

  // ---- the course ----

  // The lesson of run `id`, and its place in it.
  function lessonOf(id) {
    var found = null;
    course.lessons.forEach(function (lesson) {
      lesson.runs.forEach(function (r, k) { if (r.id === id) found = { lesson: lesson, k: k }; });
    });
    return found;
  }

  // At the end of a run: the next run of its lesson, or the next lesson.
  function nextInCourse(list) {
    var place = lessonOf(run.id);
    if (!place) return;
    var box = el("div", "story-next");
    var after = place.lesson.runs[place.k + 1];
    if (after) {
      var b = el("button", "btn primary story-next-btn", "Next in this lesson: " + after.label);
      b.type = "button";
      b.addEventListener("click", function () {
        stories.forEach(function (s) { if (s.id === after.id) open(s, 0, true); });
      });
      box.appendChild(b);
    } else {
      box.appendChild(el("p", "story-next-done", "Lesson " + place.lesson.n + " is done." + (place.lesson.labs.length ? " Now it is your turn." : "")));
      if (place.lesson.labs.length && document.getElementById("your-turn")) {
        var turn = el("a", "btn primary story-next-btn", "Your turn: " + place.lesson.labs.length + " exercises");
        turn.href = "#lab=" + place.lesson.labs[0];
        box.appendChild(turn);
      } else if (place.lesson.next) {
        var a = el("a", "btn primary story-next-btn", "Next lesson: " + place.lesson.next.title);
        a.href = place.lesson.next.href;
        box.appendChild(a);
      }
      var m = el("a", "story-next-map", "The course map");
      m.href = course.map;
      box.appendChild(m);
    }
    list.insertBefore(box, list.firstChild);
  }

  // ---- screens ----

  function words() {
    var beat = run.beats[at];
    counter.textContent = "screen " + (at + 1) + " / " + run.beats.length;
    var place = lessonOf(run.id);
    runName.textContent = place
      ? "Lesson " + place.lesson.n + " · " + place.lesson.title + " · " + (place.k + 1) + " of " + place.lesson.runs.length + ": " + run.label
      : run.label;
    // A question hides the sentence of its beat: the sentence gives the answer.
    says.textContent = waiting ? beat.question.asks : beat.says;
    says.classList.toggle("asks", waiting);
    drawQuiz(beat);
    var st = stateAt(waiting ? beat.from : beat.to);
    chosen.textContent = "";
    if (st.broken) {
      chosen.className = "story-chosen broken";
      chosen.appendChild(el("span", "chip-status gap", "two values chosen"));
      chosen.appendChild(document.createTextNode(" " + st.chosen.join(", ")));
    } else if (st.chosen.length) {
      chosen.className = "story-chosen yes";
      chosen.appendChild(el("span", "chip-status done", "chosen"));
      chosen.appendChild(document.createTextNode(" " + st.chosen.join(", ")));
    } else {
      chosen.className = "story-chosen";
      chosen.appendChild(el("span", "chip-status", "not chosen"));
    }
    var last = at === run.beats.length - 1;
    if (last && !waiting) {
      // The run's outcome says what is chosen, in a sentence.
      chosen.textContent = "";
      chosen.appendChild(chosen.className.indexOf("broken") >= 0
        ? el("span", "chip-status gap", "two values chosen")
        : el("span", "chip-status" + (st.chosen.length ? " done" : ""), st.chosen.length ? "chosen" : "not chosen"));
      chosen.appendChild(el("span", "story-end", " " + run.outcome));
    }
    choices.textContent = "";
    beat.choices = beat.choices || [];
    beat.choices.forEach(function (choice) {
      var target = null;
      trace.scenarios.forEach(function (s) { if (s.id === choice.run) target = s; });
      if (!target) return;
      var b = el("button", "story-choice", choice.label);
      b.type = "button";
      b.addEventListener("click", function () { open(target, choice.beat, true); });
      choices.appendChild(b);
    });
    if (last && !waiting) {
      nextInCourse(choices);
      if (progress) progress.seen(trace.page, run.id);
    }
    if (progress && touched) progress.at(trace.page, run.id, at);
    choices.hidden = waiting || !choices.childNodes.length;
    backBtn.disabled = at === 0;
    nextBtn.disabled = last && !waiting;
    nextBtn.textContent = waiting ? "Show me" : "Continue";
    others.querySelectorAll("button").forEach(function (b) {
      b.classList.toggle("now", b.dataset.run === run.id);
    });
    if (window.history && window.history.replaceState && touched) {
      window.history.replaceState(null, "", "#run=" + run.id + "&beat=" + at);
    }
  }

  var touched = false;

  // Show beat `n`: play its steps when `animate`, else draw its end.
  function go(n, animate) {
    n = Math.max(0, Math.min(run.beats.length - 1, n));
    at = n;
    var mine = ++token;
    var beat = run.beats[at];
    waiting = !!beat.question && !(key() in asked);
    words();
    if (waiting) {
      still(beat.from, beat.from);
      return;
    }
    if (!animate || reduced || beat.from === beat.to) {
      still(beat.to, beat.from);
      return;
    }
    nextBtn.textContent = "Skip";
    var k = beat.from;
    (function next() {
      if (mine !== token) return;
      if (k >= beat.to) { still(beat.to, beat.from); nextBtn.textContent = "Continue"; return; }
      k += 1;
      playStep(k, mine).then(function (ok) { if (ok) next(); });
    })();
  }

  function open(target, beat, animate) {
    touched = true;
    run = target;
    layout();
    go(beat, animate);
  }

  backBtn.addEventListener("click", function () { touched = true; nextBtn.textContent = "Continue"; go(at - 1, false); });
  nextBtn.addEventListener("click", function () {
    touched = true;
    if (waiting) { answer(-1); return; }
    if (nextBtn.textContent === "Skip") { token++; nextBtn.textContent = "Continue"; still(run.beats[at].to, run.beats[at].from); return; }
    go(at + 1, true);
  });
  panel.addEventListener("keydown", function (e) {
    if (e.altKey || e.ctrlKey || e.metaKey) return;
    var digit = /^[1-4]$/.test(e.key) ? parseInt(e.key, 10) - 1 : -1;
    if (waiting && digit >= 0 && digit < run.beats[at].question.answers.length) { answer(digit); e.preventDefault(); return; }
    if (e.key === "ArrowRight") { nextBtn.click(); e.preventDefault(); }
    else if (e.key === "ArrowLeft") { if (!backBtn.disabled) backBtn.click(); e.preventDefault(); }
  });
  panel.tabIndex = -1;

  // The step-through calls back here: open the beat that holds step `n`.
  window.paxosStory = {
    answers: function () {
      return Object.keys(asked).map(function (k) {
        var p = k.split(":");
        return { run: p[0], beat: parseInt(p[1], 10), answer: asked[k] };
      });
    },
    show: function (id, n) {
      var target = null;
      stories.forEach(function (s) { if (s.id === id) target = s; });
      if (!target) target = stories[0];
      var k = 0;
      target.beats.forEach(function (beat, i) { if (beat.from < n || (n === 0 && i === 0)) k = i; });
      mount.hidden = true;
      var steps = document.querySelector(".panel.steps");
      if (steps) steps.hidden = true;
      panel.hidden = false;
      open(target, k, false);
      panel.scrollIntoView({ block: "nearest" });
    },
  };

  var m = /^#run=([\w-]+)&beat=(\d+)$/.exec(window.location.hash);
  var start = stories[0], beat0 = 0;
  if (m) stories.forEach(function (s) { if (s.id === m[1]) { start = s; beat0 = parseInt(m[2], 10); } });
  run = start;
  layout();
  go(beat0, false);
  touched = !!m;
  // An address of the step-through opens the step-through.
  if (/^#run=[\w-]+&step=\d+$/.test(window.location.hash)) {
    panel.hidden = true;
    mount.hidden = false;
  }
})();
