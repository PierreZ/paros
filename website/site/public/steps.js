// The step-through diagram of the Paxos pages (#811 (Paxos flavor pages)).
//
// It first draws the recorded trace (src/traces/<page>.json, written by
// `cargo test -p paros-wasm --test paxos_traces` from the real paros-core),
// then loads paros-core compiled to wasm and draws the same runs computed
// live in the browser. The page holds the trace in
// <script type="application/json" id="trace"> and a <div data-steps> where
// the diagram goes; the script's data-wasm names the wasm bundle.
//
// What a reader sees, top to bottom:
//   1. the stage: which phase this step is in, and what that phase does;
//   2. one card per lane that this run uses: what that party knows now
//      (its promise, its vote), with the lines that changed in the accent;
//   3. the messages of the current stage, one row each, the current one
//      marked; each earlier stage folds into one row, so nothing scrolls away;
//   4. one short sentence on the step, and what is chosen so far;
//   5. the controls, the "what if" runs, and the engine's full notes.
// No protocol logic here: every word, state line and stage comes from the
// trace.
(function () {
  var data = document.getElementById("trace");
  var first = document.querySelector("[data-steps]");
  if (!data || !first) return;
  var wasm = data.dataset.wasm;
  var current = draw(JSON.parse(data.textContent), first, false, null);

  // Then run the same scripts live: paros-core compiled to wasm (the
  // `paxos` feature of paros-wasm) records the trace again in the browser,
  // and the diagram switches to it at the same step.
  if (wasm) {
    import(wasm)
      .then(function (engine) {
        return engine.default().then(function () {
          return JSON.parse(engine.paxos_trace(current.page));
        });
      })
      .then(function (live) {
        current = draw(live, current.panel, true, current.position());
      })
      .catch(function (err) {
        console.warn("paros-core in wasm did not load; the diagram shows the recorded run", err);
      });
  }

  function draw(trace, mount, live, keep) {
    var base = trace.scenarios[0];
    var run = base;
    if (keep) {
      trace.scenarios.forEach(function (scenario) { if (scenario.id === keep.run) run = scenario; });
    }
    var at = 0; // how many steps are shown
    var timer = null;
    var PACE = 1400;
    var lanes = [];  // the lanes this run uses: { lane, i } with i the index in trace.lanes
    var col = {};    // lane id -> column in `lanes`

    function el(tag, cls, text) {
      var e = document.createElement(tag);
      if (cls) e.className = cls;
      if (text != null) e.textContent = text;
      return e;
    }

    // Notes are plain text with `code` and *emphasis*; nothing else.
    function rich(text) {
      var out = document.createDocumentFragment();
      text.split(/(`[^`]+`|\*[^*]+\*)/).forEach(function (part) {
        if (/^`[^`]+`$/.test(part)) out.appendChild(el("code", null, part.slice(1, -1)));
        else if (/^\*[^*]+\*$/.test(part)) out.appendChild(el("em", null, part.slice(1, -1)));
        else if (part) out.appendChild(document.createTextNode(part));
      });
      return out;
    }

    // ---- the frame ----

    var panel = el("section", "panel steps");
    var head = el("div", "panel-head label");
    head.appendChild(el("span", "title", "Step through"));
    var source = el("span", "chip-status" + (live ? " done" : ""), live ? "live" : "recorded");
    source.title = live ? "paros-core, compiled to wasm, runs this page's scripts now" : "recorded from paros-core at build time";
    head.appendChild(source);
    var count = el("span", "meta tnum");
    head.appendChild(count);
    panel.appendChild(head);

    var stageBox = el("div", "steps-stage");
    var stageTitle = el("div", "steps-stage-title label");
    var stageAbout = el("p", "steps-stage-about");
    stageBox.appendChild(stageTitle);
    stageBox.appendChild(stageAbout);
    panel.appendChild(stageBox);

    var cards = el("div", "steps-cards");
    cards.setAttribute("aria-label", "What each party knows now");
    panel.appendChild(cards);

    var body = el("div", "steps-body");
    body.setAttribute("tabindex", "0");
    body.setAttribute("aria-label", "Messages so far");
    var inner = el("div", "steps-inner");
    var lines = el("div", "steps-lines");
    inner.appendChild(lines);
    var rows = el("ol", "steps-rows");
    inner.appendChild(rows);
    body.appendChild(inner);
    panel.appendChild(body);

    var note = el("div", "steps-note");
    note.setAttribute("aria-live", "polite");
    var says = el("p", "steps-says");
    var chosen = el("p", "steps-chosen");
    note.appendChild(says);
    note.appendChild(chosen);
    panel.appendChild(note);

    var controls = el("div", "steps-controls");
    function button(label, cls, onClick) {
      var b = el("button", "btn" + (cls ? " " + cls : ""), label);
      b.type = "button";
      b.addEventListener("click", onClick);
      controls.appendChild(b);
      return b;
    }
    var restartBtn = button("Restart", "", function () { stop(); show(0); });
    var backBtn = button("Back", "", function () { stop(); show(at - 1); });
    var playBtn = button("Play", "primary", function () { timer ? stop() : play(); });
    var stepBtn = button("Step", "", function () { stop(); show(at + 1); });
    panel.appendChild(controls);

    var whatif = el("div", "steps-whatif");
    whatif.appendChild(el("span", "label steps-whatif-title", "What if"));
    var runBtns = trace.scenarios.map(function (scenario) {
      var b = el("button", "btn", scenario.label);
      b.type = "button";
      b.addEventListener("click", function () { choose(scenario); });
      whatif.appendChild(b);
      return b;
    });
    var forkNote = el("p", "aside", "A bar on the left marks the steps where this run is different from the first one.");
    whatif.appendChild(forkNote);
    if (trace.scenarios.length > 1) panel.appendChild(whatif);

    var details = el("details", "steps-details");
    details.appendChild(el("summary", "label", "What paros-core says about this step"));
    var notes = el("div", "steps-notes");
    details.appendChild(notes);
    panel.appendChild(details);

    mount.replaceWith(panel);

    // ---- the lanes of a run: only the parties that send or get a message ----

    function layout() {
      var used = {};
      run.steps.forEach(function (step) {
        if (step.from) used[step.from] = true;
        if (step.to) used[step.to] = true;
      });
      lanes = [];
      col = {};
      trace.lanes.forEach(function (lane, i) {
        if (!used[lane.id]) return;
        col[lane.id] = lanes.length;
        lanes.push({ lane: lane, i: i });
      });
      var template = "repeat(" + lanes.length + ", minmax(0, 1fr))";
      cards.style.gridTemplateColumns = template;
      cards.classList.toggle("many", lanes.length > 4);
      cards.textContent = "";
      lanes.forEach(function (entry) {
        var card = el("div", "steps-card " + entry.lane.role);
        card.appendChild(el("div", "steps-card-name label", entry.lane.label));
        card.appendChild(el("ul", "steps-card-lines"));
        cards.appendChild(card);
      });
      lines.textContent = "";
      lanes.forEach(function (_, c) {
        var line = el("div", "steps-line");
        line.style.left = ((c + 0.5) / lanes.length) * 100 + "%";
        lines.appendChild(line);
      });
    }

    // ---- one row ----

    function row(step, n) {
      var li = el("li", "steps-row " + step.kind);
      if (step.kind === "event") {
        li.appendChild(el("span", "steps-event label", step.label));
        li.setAttribute("aria-label", "step " + n + ": " + step.label);
        return li;
      }
      var a = col[step.from];
      var b = col[step.to];
      var lo = Math.min(a, b);
      var hi = Math.max(a, b);
      var arrow = el("span", "steps-arrow " + (a <= b ? "right" : "left"));
      arrow.style.left = ((lo + 0.5) / lanes.length) * 100 + "%";
      arrow.style.width = (Math.max(hi - lo, 0.4) / lanes.length) * 100 + "%";
      arrow.style.color = "var(--phase-" + (step.phase || "other") + ")";
      if (step.kind === "drop") arrow.appendChild(el("span", "steps-lost", "✕"));
      li.appendChild(arrow);
      var label = el("span", "steps-msg tnum", step.label + (step.kind === "drop" ? " (lost)" : ""));
      label.dataset.mid = String((lo + hi + 1) / 2 / lanes.length);
      li.appendChild(label);
      li.setAttribute("aria-label", "step " + n + ": " + lanes[a].lane.label + " to " + lanes[b].lane.label + ", " + step.label + (step.kind === "drop" ? ", lost" : ""));
      return li;
    }

    // A folded stage: one row with its name and how many steps it had.
    function folded(stage, from, to) {
      var li = el("li", "steps-row folded");
      var text = "✓ " + stage.title + " · " + (to - from) + (to - from === 1 ? " step" : " steps");
      var b = el("button", "steps-fold label", text);
      b.type = "button";
      b.title = "Show these steps";
      b.addEventListener("click", function () { open[from] = !open[from]; render(); });
      li.appendChild(b);
      return li;
    }

    // A label sits over its arrow but never leaves the diagram.
    function place(label) {
      var width = rows.clientWidth;
      var w = label.offsetWidth;
      var mid = parseFloat(label.dataset.mid) * width;
      label.style.left = Math.max(0, Math.min(width - w, mid - w / 2)) + "px";
    }

    // The groups of the steps shown: consecutive steps with one stage title.
    function groups(n) {
      var out = [];
      for (var i = 0; i < n; i++) {
        var t = run.steps[i].stage ? run.steps[i].stage.title : "";
        if (!out.length || out[out.length - 1].title !== t) out.push({ title: t, stage: run.steps[i].stage, from: i, to: i + 1 });
        else out[out.length - 1].to = i + 1;
      }
      return out;
    }

    var open = {}; // folded groups the reader opened, by first step

    function render() {
      rows.textContent = "";
      var all = groups(at);
      // Keep the last few folded stages; merge the older ones into one row.
      var KEEP = 3;
      var older = all.length - 1 - KEEP;
      if (older > 1) {
        var merged = el("li", "steps-row folded");
        var steps = all[older - 1].to;
        merged.appendChild(el("span", "steps-fold label", "✓ " + older + " earlier stages · " + steps + " steps"));
        rows.appendChild(merged);
        all = all.slice(older);
      }
      all.forEach(function (g, k) {
        var last = k === all.length - 1;
        if (!last && !open[g.from]) {
          rows.appendChild(folded(g.stage || { title: "" }, g.from, g.to));
          return;
        }
        if (!last) {
          var fold = folded(g.stage || { title: "" }, g.from, g.to);
          fold.classList.add("opened");
          rows.appendChild(fold);
        }
        for (var i = g.from; i < g.to; i++) {
          var li = row(run.steps[i], i + 1);
          li.classList.toggle("now", i === at - 1);
          li.classList.toggle("other", run !== base && i >= run.fork);
          rows.appendChild(li);
          var label = li.querySelector(".steps-msg");
          if (label) place(label);
        }
      });
      var now = rows.querySelector(".now");
      if (now) body.scrollTop = now.offsetTop + now.offsetHeight - body.clientHeight + 12;
      else body.scrollTop = 0;
    }

    // ---- state ----

    function show(n) {
      n = Math.max(0, Math.min(run.steps.length, n));
      at = n;
      render();

      var step = run.steps[n - 1];
      var state = step ? step.state : run.start;
      var before = n > 1 ? run.steps[n - 2].state : run.start;
      count.textContent = "step " + n + " / " + run.steps.length;

      // The stage.
      var stage = step ? step.stage : run.steps[0] && run.steps[0].stage;
      stageTitle.textContent = step ? (stage ? stage.title : "") : "Before the first step";
      stageAbout.textContent = step && stage ? stage.about : "Press Play or Step. Each step is one message that paros-core sends, or one event.";

      // The cards.
      lanes.forEach(function (entry, c) {
        var card = cards.children[c];
        var now = state[entry.i] || [];
        var was = before[entry.i] || [];
        var list = card.querySelector(".steps-card-lines");
        list.textContent = "";
        now.forEach(function (line, k) {
          var li = el("li", null, line);
          if (n > 0 && was[k] !== line) li.className = "changed";
          list.appendChild(li);
        });
        var involved = step && (step.from === entry.lane.id || step.to === entry.lane.id);
        card.classList.toggle("on", !!involved);
        card.classList.toggle("down", now[0] === "down");
      });

      // The sentence and the outcome.
      says.textContent = step ? step.says : "Nothing has happened yet.";
      chosen.textContent = "";
      var values = step ? step.chosen : [];
      var newly = step && values.length > (n > 1 ? run.steps[n - 2].chosen.length : 0);
      chosen.className = "steps-chosen" + (values.length ? " yes" : "") + (newly ? " new" : "");
      if (values.length) {
        chosen.appendChild(el("span", "chip-status done", "chosen"));
        chosen.appendChild(document.createTextNode(" " + values.join(", ")));
      } else {
        chosen.appendChild(el("span", "chip-status", "not chosen"));
        chosen.appendChild(document.createTextNode(" No value is chosen yet."));
      }
      if (n === run.steps.length) {
        var end = el("span", "steps-end", " End of the run: " + run.outcome);
        chosen.appendChild(end);
      }

      // The engine's own notes.
      notes.textContent = "";
      (step ? step.notes : []).forEach(function (text) {
        var p = el("p");
        p.appendChild(rich(text));
        notes.appendChild(p);
      });
      details.hidden = !step || !step.notes.length;

      restartBtn.disabled = n === 0;
      backBtn.disabled = n === 0;
      stepBtn.disabled = n === run.steps.length;
      if (n === run.steps.length) stop();
      forkNote.hidden = run === base;
      runBtns.forEach(function (b, i) { b.setAttribute("aria-pressed", String(trace.scenarios[i] === run)); });
    }

    function play() {
      if (at === run.steps.length) show(0);
      playBtn.textContent = "Pause";
      timer = setInterval(function () { show(at + 1); }, PACE);
      show(at + 1);
    }

    function stop() {
      if (timer) clearInterval(timer);
      timer = null;
      playBtn.textContent = "Play";
    }

    // A "what if" run shares its first steps with the base run: keep the
    // reader's place up to the fork, then go on from there.
    function choose(scenario) {
      stop();
      var keepAt = Math.min(at, scenario.fork, run === base ? Infinity : run.fork);
      run = scenario;
      open = {};
      layout();
      show(keepAt);
    }

    window.addEventListener("resize", function () {
      rows.querySelectorAll(".steps-msg").forEach(place);
    });
    layout();
    show(keep ? keep.at : 0);
    return {
      panel: panel,
      page: trace.page,
      position: function () { stop(); return { run: run.id, at: at }; },
    };
  }
})();
