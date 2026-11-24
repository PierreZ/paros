// The tour of the Paxos flavors (src/tour.ts): chapters of recorded runs,
// played back to back by the story player (public/story.js) in watch mode.
//
// The page /paxos/tour/ plays every chapter: a title screen with one large
// Play and the chapter menu, then for each chapter its bridge (the problem
// that the flavor solves), its runs, and its end (Next chapter, Your turn,
// Go deeper). A flavor page plays its own chapters in place, from the button
// "Watch this flavor" at the top of its story.
//
// When the player reaches the end of a chapter, Next chapter follows after
// 5 s, with a countdown that the reader can stop. Nothing plays by itself
// before the reader presses Play, and with reduced motion nothing plays by
// itself at all. The address says only which chapter is open
// (#chapter=<id>), never that it plays. The progress stays in this browser
// (public/course.js).
//
// No protocol here: the chapters, the bridges and the runs come from the
// page (`#tour`, written at build time). This script runs after story.js.
(function () {
  var data = document.getElementById("tour");
  var story = window.paxosStory;
  if (!data || !story) return;
  var tour = JSON.parse(data.textContent);
  var chapters = tour.chapters;
  if (!chapters.length) return;
  var onTourPage = !!tour.all;   // every chapter of the course, on /paxos/tour/
  var progress = window.paxosCourse || null;
  var reduced = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var COUNTDOWN = 5;

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  function minutes(n) { return "about " + n + " min"; }
  function readMs(text) {
    var n = (text || "").split(/\s+/).filter(Boolean).length;
    return Math.min(9000, Math.max(2200, 330 * n));
  }

  // The screen of the tour: the title, a bridge or the end of a chapter, in
  // place of the story panel.
  var screen = el("section", "panel tour-screen");
  screen.hidden = true;
  screen.setAttribute("aria-live", "polite");
  story.panel.parentNode.insertBefore(screen, story.panel);

  // The bar above the story while a chapter plays: where the reader is.
  var bar = el("div", "tour-bar label");
  var where = el("span", "tour-where");
  bar.appendChild(where);
  var menuBtn = el("button", "linkish tour-menu-btn", onTourPage ? "Chapters" : "Stop watching");
  menuBtn.type = "button";
  bar.appendChild(menuBtn);
  bar.hidden = true;
  var head = story.panel.querySelector(".panel-head");
  story.panel.insertBefore(bar, head ? head.nextSibling : story.panel.firstChild);

  var current = -1;    // the chapter on view
  var runAt = 0;       // the run of the chapter on view
  var timer = null;
  var playing = false; // the reader pressed Play, and nothing stopped it

  function clear() { if (timer) { clearInterval(timer); clearTimeout(timer); timer = null; } }
  function address(c) {
    if (!onTourPage || !window.history || !window.history.replaceState) return;
    window.history.replaceState(null, "", c >= 0 ? "#chapter=" + chapters[c].id : window.location.pathname);
  }

  function showScreen(build) {
    clear();
    story.pause();
    screen.textContent = "";
    build(screen);
    screen.hidden = false;
    story.panel.hidden = true;
    bar.hidden = true;
    // The keys go on: Space presses the screen's main button, or the first
    // button or link of a screen with no main button (the end of the tour).
    var main = screen.querySelector(".primary") || screen.querySelector("button, a[href]");
    if (main) main.focus({ preventScroll: true });
  }

  function button(cls, text, fn) {
    var b = el("button", cls, text);
    b.type = "button";
    b.addEventListener("click", fn);
    return b;
  }
  function link(cls, text, href) {
    var a = el("a", cls, text);
    a.href = href;
    return a;
  }

  // The chapter menu: each chapter with its time and a mark when watched.
  function menu(box) {
    var done = progress ? progress.load().tour.done : {};
    var list = el("ol", "tour-chapters");
    chapters.forEach(function (ch, k) {
      var li = el("li", done[ch.id] ? "done" : "");
      var b = button("tour-chapter", "", function () { playing = false; begin(k, true); });
      b.appendChild(el("span", "tour-chapter-n label tnum", String(ch.n).padStart(2, "0")));
      b.appendChild(el("span", "tour-chapter-title", ch.title));
      b.appendChild(el("span", "tour-chapter-min label tnum", done[ch.id] ? "watched" : minutes(ch.minutes)));
      li.appendChild(b);
      list.appendChild(li);
    });
    box.appendChild(list);
  }

  // The title screen of the tour page.
  function title() {
    current = -1;
    address(-1);
    showScreen(function (box) {
      var total = chapters.reduce(function (s, ch) { return s + ch.minutes; }, 0);
      box.appendChild(el("p", "tour-kicker label", chapters.length + " chapters · " + minutes(total)));
      box.appendChild(el("p", "tour-title", "The Paxos flavors, one run at a time"));
      var go = el("div", "tour-actions");
      go.appendChild(button("btn primary tour-play", reduced ? "Start" : "▶ Play", function () { playing = !reduced; begin(0, false); }));
      var at = progress ? progress.load().tour.at : null;
      var k = at && at.chapter ? chapters.findIndex(function (ch) { return ch.id === at.chapter; }) : -1;
      if (k > 0 || (k === 0 && at.run > 0)) {
        go.appendChild(button("btn tour-resume", "Continue where you stopped: chapter " + chapters[k].n, function () {
          playing = !reduced;
          begin(k, false, Math.min(at.run, chapters[k].runs.length - 1));
        }));
      }
      box.appendChild(go);
      menu(box);
    });
  }

  // Chapter `k`: its bridge first (none for the first chapter, nor when the
  // reader starts at its first run on purpose), then its runs.
  function begin(k, fromMenu, run) {
    current = k;
    address(k);
    var ch = chapters[k];
    if (ch.bridge && !run) {
      showScreen(function (box) {
        box.appendChild(el("p", "tour-kicker label", "Chapter " + ch.n + " · " + ch.title));
        box.appendChild(el("p", "tour-bridge", ch.bridge));
        var go = el("div", "tour-actions");
        go.appendChild(button("btn primary tour-next", playing ? "Continue" : (reduced ? "Start" : "▶ Play"), function () {
          if (!playing) playing = !reduced;
          runs(k, 0);
        }));
        box.appendChild(go);
      });
      if (playing) {
        var mine = k;
        timer = setTimeout(function () { timer = null; if (current === mine && playing) runs(k, 0); }, readMs(ch.bridge));
      }
      return;
    }
    if (fromMenu) playing = !reduced;
    runs(k, run || 0);
  }

  // Play run `r` of chapter `k` in the story panel.
  function runs(k, r) {
    clear();
    current = k;
    runAt = r;
    var ch = chapters[k];
    screen.hidden = true;
    story.panel.hidden = false;
    bar.hidden = false;
    where.textContent = "Chapter " + ch.n + "/" + (onTourPage ? chapters.length : tour.of) + " · " + ch.title +
      (ch.runs.length > 1 ? " · run " + (r + 1) + "/" + ch.runs.length : "");
    if (progress) progress.tourAt(ch.id, r);
    story.start(ch.runs[r], playing);
    // The keys go on: Space plays or pauses the story.
    story.panel.focus({ preventScroll: true });
  }

  // The end of a run: the next run of the chapter, or the end of the chapter.
  function ended(run, how) {
    if (current < 0) return false;
    var ch = chapters[current];
    // The player reached the end: it played, whoever pressed Play.
    if (how === "auto") playing = !reduced;
    if (how === "play" || how === "next") playing = how === "play" && !reduced;
    if (runAt < ch.runs.length - 1) { runs(current, runAt + 1); return true; }
    if (progress) {
      progress.tourDone(ch.id);
      // The next place to continue from: the next chapter of the whole
      // tour (a flavor page plays only some), or none after the last.
      progress.tourAt(ch.next || null, 0);
    }
    chapterEnd(how === "auto");
    return true;
  }

  // The end of chapter `current`: Next chapter (after a countdown when the
  // player reached it), Your turn, Go deeper.
  function chapterEnd(auto) {
    var k = current, ch = chapters[k];
    var next = k + 1 < chapters.length ? chapters[k + 1] : null;
    showScreen(function (box) {
      box.appendChild(el("p", "tour-kicker label", "Chapter " + ch.n + " · " + ch.title));
      box.appendChild(el("p", "tour-done", next || !onTourPage ? "This chapter is done." : "The tour is done."));
      var go = el("div", "tour-actions");
      var nextBtn = null;
      if (next) {
        nextBtn = button("btn primary tour-next", "Next chapter: " + next.title, function () { clear(); playing = !reduced; begin(k + 1, false); });
        go.appendChild(nextBtn);
      } else if (!onTourPage && tour.all_href) {
        go.appendChild(link("btn primary tour-next", "The whole tour", tour.all_href));
      }
      if (ch.turn) {
        var turn = link("btn tour-turn", "Your turn", ch.turn);
        if (!onTourPage) turn.addEventListener("click", stopWatching);
        go.appendChild(turn);
      }
      var deeper = link("btn tour-deeper", "Go deeper", ch.deeper);
      // On its own page, the lesson opens in place.
      if (!onTourPage) {
        deeper.addEventListener("click", function (e) {
          e.preventDefault();
          stopWatching();
          story.start(ch.lesson_run, false);
          story.panel.scrollIntoView({ block: "nearest" });
        });
      }
      go.appendChild(deeper);
      box.appendChild(go);
      var count = el("p", "tour-count label tnum");
      box.appendChild(count);
      if (nextBtn && auto && playing && !reduced) {
        var left = COUNTDOWN;
        var stopBtn = button("linkish tour-stop", "Stop", function () { clear(); playing = false; count.textContent = ""; });
        var tick = function () {
          count.textContent = "Next chapter in " + left + " s ";
          count.appendChild(stopBtn);
        };
        tick();
        timer = setInterval(function () {
          left -= 1;
          if (left > 0) { tick(); return; }
          clear();
          begin(k + 1, false);
        }, 1000);
      }
      if (onTourPage) menu(box);
    });
  }

  // The links that leave a chapter for a lesson run leave watch mode on a
  // flavor page.
  function stopWatching() {
    clear();
    playing = false;
    current = -1;
    screen.hidden = true;
    bar.hidden = true;
    story.panel.hidden = false;
    story.onEnd(null);
    story.tour(null);
    if (watchBtn) watchBtn.hidden = false;
  }

  menuBtn.addEventListener("click", function () {
    if (onTourPage) { playing = false; title(); } else stopWatching();
  });
  document.addEventListener("visibilitychange", function () {
    if (!document.hidden) return;
    clear();
    playing = false;
    // A countdown that stopped says nothing more.
    var count = screen.querySelector(".tour-count");
    if (count) count.textContent = "";
  });

  var watchBtn = null;
  if (onTourPage) {
    story.onEnd(ended);
    var m = /^#chapter=([\w-]+)$/.exec(window.location.hash);
    var k = m ? chapters.findIndex(function (ch) { return ch.id === m[1]; }) : -1;
    if (k >= 0) begin(k, false); else title();
  } else {
    // A flavor page: one button plays its chapters in place.
    var total = chapters.reduce(function (s, ch) { return s + ch.minutes; }, 0);
    watchBtn = button("btn tour-watch", "▶ Watch this flavor (" + total + " min)", function () {
      watchBtn.hidden = true;
      story.tour({ leave: stopWatching });
      story.onEnd(ended);
      playing = !reduced;
      runs(0, 0);
    });
    story.panel.insertBefore(watchBtn, bar.nextSibling);
  }

  window.paxosTour = {
    chapter: function () { return current; },
    run: function () { return runAt; },
  };
})();
