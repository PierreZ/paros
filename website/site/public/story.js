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
// A "find the bug" question (`after` in the trace) waits for its beat: the
// beat plays first, then the screen asks what went wrong in it. An answer
// marks the right one and says why, with no second play.
//
// The tour (public/tour.js) plays runs in watch mode: no question, no choice,
// no link to the step-through, no lesson, and nothing in the address or the
// progress of the lessons. The page /paxos/tour/ is in watch mode from the
// start (`data-tour` on its trace); a flavor page enters it for "Watch this
// flavor". A beat can name a run to see next (`more`) and the lanes that
// enter at it (`enter`): a lane is drawn from the beat it enters on.
//
// The drawing is public/stage.js, which the exercises share. No protocol
// logic here: every sentence, state and message comes from the trace. This
// script runs after stage.js and before steps.js.
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

  // Watch mode (see above): set by tour.js through paxosStory.tour().
  var watching = data.dataset.tour === "on";

  var reduced = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  // ---- the frame ----

  var panel = el("section", "panel story");
  var head = el("div", "panel-head label");
  head.appendChild(el("span", "title", "Story"));
  var chip = el("span", "chip-status", "recorded");
  chip.title = "recorded from " + (data.dataset.source || "paros-consensus") + " at build time";
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

  // The player: a progress line with one mark per screen, then Back, Play or
  // Pause, Continue and the speed.
  var line = el("div", "story-line");
  line.setAttribute("role", "group");
  line.setAttribute("aria-label", "Screens of this story");
  panel.appendChild(line);
  var controls = el("div", "story-controls");
  var backBtn = el("button", "btn", "Back");
  var playBtn = el("button", "btn story-play", "Play");
  var nextBtn = el("button", "btn primary", "Continue");
  var speedBtn = el("button", "btn story-speed tnum", "1×");
  speedBtn.title = "Speed of the player";
  [backBtn, playBtn, nextBtn, speedBtn].forEach(function (b) { b.type = "button"; controls.appendChild(b); });
  panel.appendChild(controls);

  var others = el("details", "story-others");
  if (stories.length > 1) {
    others.appendChild(el("summary", "label story-others-title", "All stories"));
    stories.forEach(function (run) {
      var b = el("button", "steps-chapter", run.label);
      b.type = "button";
      b.dataset.run = run.id;
      b.addEventListener("click", function () { stop(); open(run, 0, true); });
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
    stop();
    var beat = run.beats[at];
    panel.hidden = true;
    window.paxosSteps.show(run.id, beat.to);
  });
  foot.appendChild(toSteps);
  foot.appendChild(document.createTextNode(": the same run, one message per step, with what each message carries."));
  panel.appendChild(foot);
  foot.hidden = watching;
  others.hidden = watching;
  panel.classList.toggle("watching", watching);

  mount.parentNode.insertBefore(panel, mount);
  // The step-through draws into the mount, hidden until the reader asks.
  mount.hidden = true;

  // ---- the drawing (public/stage.js) ----

  var run = null;
  var at = 0;          // the beat on screen
  var view = window.paxosStage.create(stage);

  function runLanes() { return run.lanes || trace.lanes; }

  function layout() {
    var used = {};
    run.steps.forEach(function (step) {
      if (step.from) used[step.from] = true;
      if (step.to) used[step.to] = true;
      (step.wire || []).forEach(function (f) { used[f.from] = true; used[f.to] = true; });
    });
    view.layout(runLanes(), {
      used: used,
      grid: run.lanes ? run.grid : trace.grid,
      log: run.steps.some(function (s) { return s.logs && s.logs.length; }),
    });
  }

  function stateAt(n) {
    // The state after `n` steps of the run.
    if (n === 0) return { nodes: run.start_nodes || [], logs: run.start_logs || [], wire: run.start_wire || [], chosen: [], state: run.start };
    var s = run.steps[n - 1];
    return { nodes: s.nodes || [], logs: s.logs || [], wire: s.wire || [], chosen: s.chosen || [], state: s.state, broken: s.broken };
  }

  // Play step `n` (1-based): its message crosses, the receiver writes, then
  // the messages it sent leave the sender. A copy step puts a second copy on
  // the wire: nothing crosses.
  function playStep(n, mine) {
    var step = run.steps[n - 1];
    var after = stateAt(n);
    return view.step(stateAt(n - 1), after, {
      message: step.kind === "copy" ? null : step.message,
      lost: step.kind === "drop",
      writes: (after.nodes || []).map(function (node) { return node.writes; }),
    }, mine);
  }

  // The end of a screen: the state after step `n`, with every disk write of
  // the screen's steps from step `from` on.
  function still(n, from) {
    var writes = [];
    for (var k = from == null ? n : from; k < n; k++) {
      (run.steps[k].nodes || []).forEach(function (node, i) {
        (node.writes || []).forEach(function (w) {
          writes[i] = writes[i] || [];
          if (writes[i].indexOf(w) < 0) writes[i].push(w);
        });
      });
    }
    view.draw(stateAt(n), writes);
  }

  // ---- questions ----

  // "run:beat" -> the index of the answer, -1 for "Show me". A question the
  // reader answered on an earlier visit is not asked again.
  var asked = progress ? progress.answersOf(trace.page) : {};
  var waiting = false;  // the screen shows a question that has no answer yet

  function key() { return run.id + ":" + at; }

  // The question of the beat on screen is asked before it plays.
  function before() {
    var q = questionOf(run.beats[at]);
    return !!q && !q.after;
  }

  // The question of a beat: none in watch mode.
  function questionOf(beat) { return watching ? null : beat.question; }

  // The beat on screen has played to its end: a "find the bug" question with
  // no answer is asked now.
  function landed() {
    var q = questionOf(run.beats[at]);
    if (q && q.after && !(key() in asked)) {
      waiting = true;
      words();
    }
    rest();
  }

  function answer(i) {
    stop();
    var q = run.beats[at].question;
    asked[key()] = i;
    var detail = { page: trace.page, run: run.id, beat: at, answer: i, right: i === q.right };
    try { document.dispatchEvent(new CustomEvent("paxos:answer", { detail: detail })); } catch (e) { /* old browser */ }
    go(at, !q.after);
  }

  // The question of the beat on screen: its answers while it waits, then
  // the reader's answer, the right one and why.
  function drawQuiz(beat) {
    var q = questionOf(beat);
    quiz.textContent = "";
    quiz.hidden = !q;
    if (!q) return;
    if (waiting) {
      quiz.appendChild(q.after
        ? el("p", "story-quiz-head label", "Find the bug: " + q.asks)
        : el("p", "story-quiz-head label", "Before it plays: what happens?"));
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
    var place = watching ? null : lessonOf(run.id);
    if (!place) return;
    var box = el("div", "story-next");
    var after = place.lesson.runs[place.k + 1];
    if (after) {
      var b = el("button", "btn primary story-next-btn", "Next in this lesson: " + after.label);
      b.type = "button";
      b.addEventListener("click", function () {
        stop();
        stories.forEach(function (s) { if (s.id === after.id) open(s, 0, true); });
      });
      box.appendChild(b);
    } else {
      box.appendChild(el("p", "story-next-done", "Lesson " + place.lesson.n + " is done." + (place.lesson.labs.length ? " Now it is your turn." : "")));
      if (place.lesson.labs.length && document.getElementById("your-turn")) {
        var turn = el("a", "btn primary story-next-btn", "Your turn: " + place.lesson.labs.length + (place.lesson.labs.length === 1 ? " exercise" : " exercises"));
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
    var place = watching ? null : lessonOf(run.id);
    runName.textContent = place
      ? "Lesson " + place.lesson.n + " · " + place.lesson.title + " · " + (place.k + 1) + " of " + place.lesson.runs.length + ": " + run.label
      : run.label;
    // A question hides the sentence of its beat: the sentence gives the answer.
    var ahead = waiting && before();
    says.textContent = ahead ? beat.question.asks : beat.says;
    says.classList.toggle("asks", ahead);
    drawQuiz(beat);
    var st = stateAt(ahead ? beat.from : beat.to);
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
    if (beat.more) drawMore(beat.more);
    (watching ? [] : beat.choices || []).forEach(function (choice) {
      var target = null;
      trace.scenarios.forEach(function (s) { if (s.id === choice.run) target = s; });
      if (!target) return;
      var b = el("button", "story-choice", choice.label);
      b.type = "button";
      b.addEventListener("click", function () { stop(); open(target, choice.beat, true); });
      choices.appendChild(b);
    });
    if (last && !waiting) {
      nextInCourse(choices);
      if (progress && !watching) progress.seen(trace.page, run.id);
    }
    if (progress && touched && !watching) progress.at(trace.page, run.id, at);
    choices.hidden = waiting || !choices.childNodes.length;
    backBtn.disabled = at === 0;
    // In watch mode, Continue on the last screen goes where the tour says.
    nextBtn.disabled = last && !waiting && !(watching && onEnd);
    nextBtn.textContent = waiting ? (before() ? "Show me" : "Tell me") : "Continue";
    others.querySelectorAll("button").forEach(function (b) {
      b.classList.toggle("now", b.dataset.run === run.id);
    });
    if (window.history && window.history.replaceState && touched && !watching) {
      window.history.replaceState(null, "", "#run=" + run.id + "&beat=" + at);
    }
  }

  // A link to a run that tells more: in place when this page has it, else
  // on the page of the run (`href` of a run of the tour page).
  function drawMore(id) {
    var target = null;
    stories.forEach(function (s) { if (s.id === id) target = s; });
    var a = el("a", "story-choice story-more", "More: " + (target ? target.label : id));
    a.href = (run.href || "") + "#run=" + id + "&beat=0";
    if (target && !run.href) {
      a.addEventListener("click", function (e) {
        e.preventDefault();
        stop();
        if (watching && leave) leave();
        open(target, 0, true);
      });
    }
    choices.appendChild(a);
  }

  // The lanes that enter after beat `n`: placed, not drawn yet.
  function later(n) {
    var out = {};
    run.beats.forEach(function (beat, k) {
      if (k > n) (beat.enter || []).forEach(function (lane) { out[lane] = true; });
    });
    return out;
  }

  var touched = false;

  // ---- the player ----
  //
  // Play goes on by itself: after the messages of a screen stop, it waits
  // the time to read the sentence, then plays the next screen. It stops at
  // a question, at the end of a run, when the reader presses any other
  // control (the speed button only changes the speed), and when the page
  // is hidden. It never starts by itself, and
  // with reduced motion there is no Play: each screen appears complete.

  var SPEEDS = [1, 1.5, 0.75];
  var playing = false;
  var speed = 1;
  var timer = null;
  var onEnd = null;    // what Play does at the end of a run (the tour sets it)
  var leave = null;    // what leaving watch mode does (the tour sets it)
  var kept = null;     // the lesson screen on view when watch mode began

  // The time to read a sentence: 0.33 s per word, from 2.2 s to 9 s.
  function readMs(text) {
    var n = (text || "").split(/\s+/).filter(Boolean).length;
    return Math.min(9000, Math.max(2200, 330 * n)) / speed;
  }

  function clear() { if (timer) { clearTimeout(timer); timer = null; } }

  function setPlaying(on) {
    playing = on;
    clear();
    playBtn.textContent = on ? "Pause" : "Play";
    panel.classList.toggle("playing", on);
  }

  function stop() { if (playing) setPlaying(false); }

  // The screen on view is complete: when playing, the next one follows.
  function rest() {
    // A screen whose messages still move rests when they stop.
    if (!playing || nextBtn.textContent === "Skip") return;
    clear();
    if (waiting) { setPlaying(false); return; }
    // The last screen of a run: Play stops at once, unless a hook goes on.
    if (at === run.beats.length - 1 && !onEnd) { setPlaying(false); return; }
    var mine = run.id + ":" + at;
    timer = setTimeout(function () {
      timer = null;
      if (!playing || run.id + ":" + at !== mine || waiting) return;
      if (at < run.beats.length - 1) { go(at + 1, true); return; }
      if (onEnd && onEnd(run, "auto")) return;
      setPlaying(false);
    }, readMs(run.beats[at].says));
  }

  function play() {
    if (reduced) return;
    setPlaying(true);
    touched = true;
    if (waiting) { setPlaying(false); return; }
    // At the end of a run, Play starts it again.
    if (at === run.beats.length - 1) {
      if (onEnd && onEnd(run, "play")) return;
      go(0, true);
      return;
    }
    if (nextBtn.textContent === "Skip") return; // the screen plays: rest() follows
    go(at + 1, true);
  }

  // The marks are built once for each run, so a mark that has the focus
  // keeps it when the screen changes; each go() only moves the classes.
  var lineOf = null;
  var lineMode = null;
  function drawLine() {
    if (lineOf !== run || lineMode !== watching) {
      lineMode = watching;
      lineOf = run;
      line.textContent = "";
      run.beats.forEach(function (beat, k) {
        var b = el("button", "story-mark" + (questionOf(beat) ? " ask" : ""));
        b.type = "button";
        b.setAttribute("aria-label", "Screen " + (k + 1));
        b.addEventListener("click", function () { stop(); touched = true; go(k, false); });
        line.appendChild(b);
      });
    }
    Array.prototype.forEach.call(line.children, function (b, k) {
      b.classList.toggle("seen", k < at);
      b.classList.toggle("now", k === at);
      if (k === at) b.setAttribute("aria-current", "step"); else b.removeAttribute("aria-current");
    });
  }

  // Show beat `n`: play its steps when `animate`, else draw its end.
  function go(n, animate) {
    n = Math.max(0, Math.min(run.beats.length - 1, n));
    at = n;
    var mine = view.begin();
    var beat = run.beats[at];
    waiting = before() && !(key() in asked);
    view.hide(later(at));
    words();
    drawLine();
    if (waiting) {
      still(beat.from, beat.from);
      stop();
      return;
    }
    if (!animate || reduced || beat.from === beat.to) {
      still(beat.to, beat.from);
      landed();
      return;
    }
    nextBtn.textContent = "Skip";
    var k = beat.from;
    (function next() {
      if (!view.live(mine)) return;
      if (k >= beat.to) { still(beat.to, beat.from); nextBtn.textContent = "Continue"; landed(); return; }
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

  backBtn.addEventListener("click", function () { stop(); touched = true; nextBtn.textContent = "Continue"; go(at - 1, false); });
  playBtn.addEventListener("click", function () { if (playing) setPlaying(false); else play(); });
  speedBtn.addEventListener("click", function () {
    speed = SPEEDS[(SPEEDS.indexOf(speed) + 1) % SPEEDS.length];
    speedBtn.textContent = speed + "×";
    view.speed(speed);
    rest();
  });
  playBtn.hidden = speedBtn.hidden = reduced;
  document.addEventListener("visibilitychange", function () { if (document.hidden) stop(); });
  nextBtn.addEventListener("click", function () {
    stop();
    touched = true;
    if (waiting) { answer(-1); return; }
    if (watching && onEnd && at === run.beats.length - 1 && nextBtn.textContent !== "Skip") { onEnd(run, "next"); return; }
    if (nextBtn.textContent === "Skip") { view.cancel(); nextBtn.textContent = "Continue"; still(run.beats[at].to, run.beats[at].from); landed(); return; }
    go(at + 1, true);
  });
  panel.addEventListener("keydown", function (e) {
    if (e.altKey || e.ctrlKey || e.metaKey) return;
    var digit = /^[1-4]$/.test(e.key) ? parseInt(e.key, 10) - 1 : -1;
    if (waiting && digit >= 0 && digit < run.beats[at].question.answers.length) { answer(digit); e.preventDefault(); return; }
    if (e.key === " " && !reduced && e.target === panel) { playBtn.click(); e.preventDefault(); return; }
    if (e.key === "ArrowRight") { nextBtn.click(); e.preventDefault(); }
    else if (e.key === "ArrowLeft") { if (!backBtn.disabled) backBtn.click(); e.preventDefault(); }
  });
  panel.tabIndex = -1;

  // The step-through calls back here: open the beat that holds step `n`.
  window.paxosStory = {
    // The player, for the smoke test: play, pause, its state and its speed.
    play: play,
    pause: stop,
    playing: function () { return playing; },
    speed: function (v) { speed = v; speedBtn.textContent = v + "×"; view.speed(v); },
    // What the end of a run does: f(run, how) returns true when it took
    // over (the tour goes on to the next run). `how` is "auto" when the
    // player reached the end, "play" for Play on the last screen and "next"
    // for Continue on it in watch mode. null clears it.
    onEnd: function (f) { onEnd = f || null; },
    // The panel, so the tour can put its screens next to it.
    panel: panel,
    // Watch mode on (`opts.leave` runs when a link leaves it) or off (null).
    // Leaving it opens again the screen of the lesson that was on view, so
    // the address and the lesson progress keep the reader's own place.
    tour: function (opts) {
      stop();
      var was = watching;
      watching = !!opts;
      leave = opts ? opts.leave || null : null;
      foot.hidden = others.hidden = watching;
      panel.classList.toggle("watching", watching);
      if (watching && !was) kept = { run: run, at: at, touched: touched };
      asked = progress && !watching ? progress.answersOf(trace.page) : asked;
      if (!watching && was && kept) {
        run = kept.run;
        touched = kept.touched;
        layout();
        go(kept.at, false);
        kept = null;
        return;
      }
      go(at, false);
    },
    watching: function () { return watching; },
    // Open run `id` at its first screen; Play goes on when `play`.
    start: function (id, play) {
      var target = null;
      stories.forEach(function (s) { if (s.id === id) target = s; });
      if (!target) return false;
      stop();
      if (play && !reduced) setPlaying(true);
      // Nothing moves before the reader presses Play.
      open(target, 0, !!play && !reduced);
      return true;
    },
    answers: function () {
      return Object.keys(asked).map(function (k) {
        var p = k.split(":");
        return { run: p[0], beat: parseInt(p[1], 10), answer: asked[k] };
      });
    },
    show: function (id, n) {
      stop();
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
