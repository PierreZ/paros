// The progress of the Paxos course (src/course.ts), kept in this browser
// only: the runs the reader saw to their end, the answers the reader gave,
// the exercises the reader solved, the last screen the reader opened, and
// where the reader is in the tour (public/tour.js). Nothing leaves the browser. When the
// storage is blocked or empty, the course works the same and remembers
// nothing.
(function () {
  var KEY = "paros-course-v1";

  function load() {
    var st = null;
    try { st = JSON.parse(window.localStorage.getItem(KEY) || "null"); } catch (e) { st = null; }
    if (!st || typeof st !== "object") st = {};
    return { seen: st.seen || {}, answers: st.answers || {}, solved: st.solved || {}, last: st.last || null,
      tour: { at: (st.tour && st.tour.at) || null, done: (st.tour && st.tour.done) || {} } };
  }
  function save(st) {
    try { window.localStorage.setItem(KEY, JSON.stringify(st)); } catch (e) { /* storage blocked */ }
  }
  function change(fn) { var st = load(); fn(st); save(st); }

  window.paxosCourse = {
    load: load,
    // The reader saw run `run` of page `page` to its last screen.
    seen: function (page, run) { change(function (st) { st.seen[page + "/" + run] = true; }); },
    // The reader opened this screen.
    at: function (page, run, beat) { change(function (st) { st.last = { page: page, run: run, beat: beat }; }); },
    // The answer of the reader to the question of a beat, -1 for "Show me".
    answered: function (page, run, beat, answer) {
      change(function (st) { st.answers[page + "/" + run + ":" + beat] = answer; });
    },
    // The tour: the reader is at run `run` (its index) of chapter `chapter`.
    tourAt: function (chapter, run) { change(function (st) { st.tour.at = { chapter: chapter, run: run }; }); },
    // The tour: the reader watched chapter `chapter` to its end.
    tourDone: function (chapter) { change(function (st) { st.tour.done[chapter] = true; }); },
    // The reader reached the goal of exercise `id`.
    solved: function (id) { change(function (st) { st.solved[id] = true; }); },
    // The answers already given on one page: "run:beat" -> answer.
    answersOf: function (page) {
      var out = {}, all = load().answers;
      Object.keys(all).forEach(function (k) {
        if (k.indexOf(page + "/") === 0) out[k.slice(page.length + 1)] = all[k];
      });
      return out;
    },
    forget: function () { try { window.localStorage.removeItem(KEY); } catch (e) { /* storage blocked */ } },
  };

  document.addEventListener("paxos:answer", function (e) {
    var d = e.detail || {};
    window.paxosCourse.answered(d.page, d.run, d.beat, d.answer);
  });
})();
