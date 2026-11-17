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
// No protocol logic here: every sentence, state and message comes from the
// trace. This script runs before steps.js.
(function () {
  var data = document.getElementById("trace");
  var mount = document.querySelector("[data-steps]");
  if (!data || !mount) return;
  var trace = JSON.parse(data.textContent);
  var stories = trace.scenarios.filter(function (run) { return run.beats && run.beats.length; });
  if (!stories.length) return;

  var SVG = "http://www.w3.org/2000/svg";
  var MOVE = 650;      // ms for a message to cross
  var EMERGE = 260;    // ms for new messages to leave their sender
  var REST = 0.42;     // where a message waits on its path, from 0 to 1
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
  chip.title = "recorded from paros-core at build time";
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
      var first = node.alive ? (lines[0] || "") : "down";
      g.appendChild(svg("text", { x: entry.x, y: y, class: "story-node-line" }, first));
      if (node.alive && lines[1]) g.appendChild(svg("text", { x: entry.x, y: y + 12, class: "story-node-line" }, lines.slice(1).join(" ")));
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
      g.appendChild(svg("title", {}, plain(entry.lane.label)));
      nodesLayer.appendChild(g);
    });
  }

  // A point on the path of a flight, from its sender (0) to its receiver (1),
  // kept off the two circles.
  function point(flight, t) {
    var a = byId[flight.from], b = byId[flight.to];
    if (!a || !b) return null;
    var dx = b.x - a.x, dy = b.y - a.y, len = Math.sqrt(dx * dx + dy * dy) || 1;
    var ux = dx / len, uy = dy / len, pad = 20;
    var from = { x: a.x + ux * pad, y: a.y + uy * pad };
    var span = Math.max(0, len - 2 * pad);
    return { x: from.x + ux * span * t, y: from.y + uy * span * t };
  }

  function dot(flight, t, cls) {
    var p = point(flight, t);
    if (!p) return null;
    var g = svg("g", { class: "story-msg " + (cls || "") });
    g.style.color = "var(--phase-" + (flight.phase || "other") + ")";
    g.appendChild(svg("circle", { cx: 0, cy: 0, r: 5 }));
    g.setAttribute("transform", "translate(" + p.x + " " + p.y + ")");
    g.appendChild(svg("title", {}, flight.label));
    return g;
  }

  function drawWire(wire, skip) {
    wireLayer.textContent = "";
    wire.forEach(function (flight) {
      var a = point(flight, 0), b = point(flight, 1);
      if (!a) return;
      wireLayer.appendChild(svg("line", { x1: a.x, y1: a.y, x2: b.x, y2: b.y, class: "story-path" }));
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
    var t = svg("text", { x: 0, y: -9, class: "story-msg-label" }, flight.label.split(" · ")[0]);
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
        var p = point(flight, REST + (end - REST) * e);
        g.setAttribute("transform", "translate(" + p.x + " " + p.y + ")");
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
            var p = point(x.f, REST * t);
            x.d.setAttribute("transform", "translate(" + p.x + " " + p.y + ")");
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

  // ---- screens ----

  function words() {
    var beat = run.beats[at];
    counter.textContent = "screen " + (at + 1) + " / " + run.beats.length;
    runName.textContent = run.label;
    says.textContent = beat.says;
    var st = stateAt(beat.to);
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
    if (last) {
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
    choices.hidden = !beat.choices.length;
    backBtn.disabled = at === 0;
    nextBtn.disabled = last;
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
    words();
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
    if (nextBtn.textContent === "Skip") { token++; nextBtn.textContent = "Continue"; still(run.beats[at].to, run.beats[at].from); return; }
    go(at + 1, true);
  });
  panel.addEventListener("keydown", function (e) {
    if (e.altKey || e.ctrlKey || e.metaKey) return;
    if (e.key === "ArrowRight") { nextBtn.click(); e.preventDefault(); }
    else if (e.key === "ArrowLeft") { if (!backBtn.disabled) backBtn.click(); e.preventDefault(); }
  });
  panel.tabIndex = -1;

  // The step-through calls back here: open the beat that holds step `n`.
  window.paxosStory = {
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
