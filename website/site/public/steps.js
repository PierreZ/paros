// The step-through diagram of the Paxos pages (#811 (Paxos flavor pages)).
//
// It first draws the recorded trace (src/traces/<page>.json, written by
// `cargo test -p paros-wasm --test paxos_traces` from the real paros-core),
// then loads paros-core compiled to wasm and draws the same runs computed
// live in the browser. A trace is a sequence diagram: one lane per role,
// one row per message. The page
// holds the trace in <script type="application/json" id="trace"> and a
// <div data-steps> where the diagram goes; the script's data-wasm names the
// wasm bundle. No protocol logic here: every
// message, note and lane state comes from the trace.
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
  var lanes = trace.lanes;
  var index = {};
  lanes.forEach(function (lane, i) { index[lane.id] = i; });

  var run = trace.scenarios[0];
  if (keep) {
    trace.scenarios.forEach(function (scenario) { if (scenario.id === keep.run) run = scenario; });
  }
  var at = 0; // how many steps are shown
  var timer = null;
  var PACE = 900;

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  // Narration is plain text with `code` and *emphasis*; nothing else.
  function rich(text) {
    var out = document.createDocumentFragment();
    text.split(/(`[^`]+`|\*[^*]+\*)/).forEach(function (part) {
      if (/^`[^`]+`$/.test(part)) out.appendChild(el("code", null, part.slice(1, -1)));
      else if (/^\*[^*]+\*$/.test(part)) out.appendChild(el("em", null, part.slice(1, -1)));
      else if (part) out.appendChild(document.createTextNode(part));
    });
    return out;
  }

  // ---- the frame: a panel, the lanes, the rows, the note, the controls ----

  var panel = el("section", "panel steps");
  var head = el("div", "panel-head label");
  head.appendChild(el("span", "title", "Step through"));
  var source = el("span", "chip-status" + (live ? " done" : ""), live ? "live" : "recorded");
  source.title = live ? "paros-core, compiled to wasm, runs this page's scripts now" : "recorded from paros-core at build time";
  head.appendChild(source);
  var count = el("span", "meta tnum");
  head.appendChild(count);
  panel.appendChild(head);

  var cols = "repeat(" + lanes.length + ", minmax(0, 1fr))";
  var header = el("div", "steps-lanes label");
  header.style.gridTemplateColumns = cols;
  var laneHeads = lanes.map(function (lane) {
    var h = el("div", "steps-lane " + lane.role, lane.label);
    header.appendChild(h);
    return h;
  });
  panel.appendChild(header);

  var body = el("div", "steps-body");
  body.setAttribute("tabindex", "0");
  body.setAttribute("aria-label", "Messages so far");
  var inner = el("div", "steps-inner");
  lanes.forEach(function (_, i) {
    var line = el("div", "steps-line");
    line.style.left = ((i + 0.5) / lanes.length) * 100 + "%";
    inner.appendChild(line);
  });
  var rows = el("ol", "steps-rows");
  inner.appendChild(rows);
  body.appendChild(inner);
  panel.appendChild(body);

  var note = el("div", "steps-note");
  note.setAttribute("aria-live", "polite");
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
  var runBtns = trace.scenarios.map(function (scenario) {
    var b = el("button", "btn", scenario.label);
    b.type = "button";
    b.addEventListener("click", function () { choose(scenario); });
    whatif.appendChild(b);
    return b;
  });
  var forkNote = el("p", "aside", "A bar on the left marks the steps where this run differs from the first one.");
  whatif.appendChild(forkNote);
  if (trace.scenarios.length > 1) panel.appendChild(whatif);

  var state = el("dl", "kv steps-state");
  panel.appendChild(state);

  mount.replaceWith(panel);

  // ---- one row ----

  function row(step, n) {
    var li = el("li", "steps-row " + step.kind);
    li.style.gridTemplateColumns = cols;
    if (step.kind === "event") {
      li.appendChild(el("span", "steps-event label", step.label));
      return li;
    }
    var a = index[step.from];
    var b = index[step.to];
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
    li.setAttribute("aria-label", "step " + n + ": " + lanes[a].label + " to " + lanes[b].label + ", " + step.label + (step.kind === "drop" ? ", lost" : ""));
    return li;
  }

  // A label sits over its arrow but never leaves the diagram.
  function place(label) {
    var width = rows.clientWidth;
    var w = label.offsetWidth;
    var mid = parseFloat(label.dataset.mid) * width;
    label.style.left = Math.max(0, Math.min(width - w, mid - w / 2)) + "px";
  }

  // ---- state ----

  function show(n) {
    n = Math.max(0, Math.min(run.steps.length, n));
    while (rows.children.length > n) rows.removeChild(rows.lastChild);
    while (rows.children.length < n) {
      var li = row(run.steps[rows.children.length], rows.children.length + 1);
      rows.appendChild(li);
      var label = li.querySelector(".steps-msg");
      if (label) place(label);
    }
    Array.prototype.forEach.call(rows.children, function (li, i) {
      li.classList.toggle("now", i === n - 1);
      li.classList.toggle("other", i >= run.fork && run !== trace.scenarios[0]);
    });
    at = n;
    body.scrollTop = body.scrollHeight;

    var step = run.steps[n - 1];
    count.textContent = "step " + n + " / " + run.steps.length;
    note.textContent = "";
    if (!step) {
      note.appendChild(el("p", "steps-hint", live
        ? "Press Play or Step. paros-core, compiled to wasm, runs these scripts in this page. Each step is one message it sent, delivered or lost."
        : "Press Play or Step. Each step is one message that the real paros-core sent, delivered or lost."));
    } else {
      step.notes.forEach(function (text) {
        var p = el("p");
        p.appendChild(rich(text));
        note.appendChild(p);
      });
      if (!step.notes.length) note.appendChild(el("p", "steps-hint", step.label));
    }
    if (n === run.steps.length) {
      var end = el("p", "steps-end");
      end.appendChild(el("span", "chip-status done", "end"));
      end.appendChild(document.createTextNode(" " + run.outcome));
      note.appendChild(end);
    }

    var before = n > 1 ? run.steps[n - 2].state : null;
    var now = step ? step.state : null;
    state.textContent = "";
    lanes.forEach(function (lane, i) {
      var dd = el("dd", null, now ? now[i] || "·" : "·");
      var changed = now && before && now[i] !== before[i];
      var involved = step && (step.from === lane.id || step.to === lane.id);
      if (changed) dd.className = "changed";
      state.appendChild(el("dt", null, lane.label));
      state.appendChild(dd);
      laneHeads[i].classList.toggle("on", !!involved);
      laneHeads[i].classList.toggle("down", now ? now[i] === "down" : false);
    });

    restartBtn.disabled = n === 0;
    backBtn.disabled = n === 0;
    stepBtn.disabled = n === run.steps.length;
    if (n === run.steps.length) stop();
    forkNote.hidden = run === trace.scenarios[0];
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
    var keep = Math.min(at, scenario.fork, run === trace.scenarios[0] ? Infinity : run.fork);
    run = scenario;
    rows.textContent = "";
    show(keep);
  }

  window.addEventListener("resize", function () {
    rows.querySelectorAll(".steps-msg").forEach(place);
  });
  show(keep ? keep.at : 0);
  return {
    panel: panel,
    page: trace.page,
    position: function () { stop(); return { run: run.id, at: at }; },
  };
  }
})();
