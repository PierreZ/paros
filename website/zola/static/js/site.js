// The book's small behaviours (#804 (flat engineering redesign)): code block
// headers and copy, shell prompts, callout tags, table boxes, mermaid in the
// site's colours, the table of contents marker and search. The theme switch and
// the phone menu are in website/shared/theme.js.
(function () {
  "use strict";

  var LANG_NAMES = { shellscript: "sh", sh: "sh", bash: "sh", rust: "rust", toml: "toml", text: "text" };

  // ---- code blocks: a header row with the language and a copy button ----
  function decorateCode() {
    document.querySelectorAll(".prose pre.giallo, .prose pre:not(.mermaid):not(.giallo)").forEach(function (pre) {
      if (pre.closest(".codeblock")) return;
      var code = pre.querySelector("code");
      var lang = (code && code.getAttribute("data-lang")) || "text";
      var box = document.createElement("div");
      box.className = "codeblock";
      var head = document.createElement("div");
      head.className = "code-head label";
      var name = document.createElement("span");
      name.textContent = LANG_NAMES[lang] || lang;
      var copy = document.createElement("button");
      copy.type = "button";
      copy.textContent = "Copy";
      copy.addEventListener("click", function () {
        var text = (code || pre).innerText;
        navigator.clipboard.writeText(text.replace(/\n$/, "")).then(
          function () {
            copy.textContent = "Copied";
            setTimeout(function () { copy.textContent = "Copy"; }, 1200);
          },
          function () { copy.textContent = "Failed"; }
        );
      });
      head.appendChild(name);
      head.appendChild(copy);
      pre.parentNode.insertBefore(box, pre);
      box.appendChild(head);
      box.appendChild(pre);

      // Shell: a muted `$` before each command line (not before a comment or
      // a continued line). The prompt is CSS, so copy leaves it out.
      if (lang === "shellscript" || lang === "sh" || lang === "bash") {
        var continued = false;
        pre.querySelectorAll(".giallo-l").forEach(function (line) {
          var t = line.textContent;
          var trimmed = t.trim();
          if (!continued && trimmed && trimmed[0] !== "#" && t[0] !== " ") line.classList.add("cmd");
          continued = /\\\s*$/.test(t);
        });
      }
    });
  }

  // ---- callouts: a blockquote opened by a bold "Play it." is tagged PLAY ----
  function tagCallouts() {
    document.querySelectorAll(".prose blockquote").forEach(function (q) {
      var first = q.querySelector("p > strong:first-child");
      if (!first) return;
      var t = first.textContent.trim().replace(/[.:]$/, "");
      var tags = { "Play it": "PLAY", Note: "NOTE", Warning: "WARN", Safety: "SAFETY", "Watch it": "WATCH" };
      if (tags[t]) q.setAttribute("data-tag", tags[t]);
    });
  }

  // ---- tables scroll inside their own box ----
  function boxTables() {
    document.querySelectorAll(".prose table").forEach(function (t) {
      if (t.parentNode.classList.contains("table-wrap")) return;
      var w = document.createElement("div");
      w.className = "table-wrap";
      t.parentNode.insertBefore(w, t);
      w.appendChild(t);
    });
  }

  // ---- mermaid: square nodes, mono labels, thin lines, double-line subgraphs ----
  var diagrams = [];

  function tokens() {
    var cs = getComputedStyle(document.documentElement);
    function v(n) { return cs.getPropertyValue(n).trim(); }
    return {
      darkMode: window.parosTheme && window.parosTheme.isDark(),
      background: v("--bg"),
      fontFamily: v("--font-mono"),
      fontSize: "13px",
      primaryColor: v("--panel"),
      primaryTextColor: v("--fg"),
      primaryBorderColor: v("--fg"),
      secondaryColor: v("--bg"),
      tertiaryColor: v("--bg"),
      lineColor: v("--muted"),
      textColor: v("--fg"),
      mainBkg: v("--panel"),
      nodeBorder: v("--fg"),
      clusterBkg: v("--bg"),
      clusterBorder: v("--fg"),
      titleColor: v("--fg"),
      edgeLabelBackground: v("--bg"),
      actorBkg: v("--panel"),
      actorBorder: v("--fg"),
      actorTextColor: v("--fg"),
      actorLineColor: v("--rule-strong"),
      signalColor: v("--fg"),
      signalTextColor: v("--fg"),
      labelBoxBkgColor: v("--panel"),
      labelBoxBorderColor: v("--fg"),
      labelTextColor: v("--fg"),
      loopTextColor: v("--fg"),
      noteBkgColor: v("--bg"),
      noteBorderColor: v("--rule-strong"),
      noteTextColor: v("--fg"),
      activationBkgColor: v("--panel"),
      activationBorderColor: v("--fg"),
      sequenceNumberColor: v("--bg"),
    };
  }

  function doubleClusters(root) {
    root.querySelectorAll("g.cluster > rect").forEach(function (r) {
      if (r.nextSibling && r.nextSibling.classList && r.nextSibling.classList.contains("inner")) return;
      var x = +r.getAttribute("x"), y = +r.getAttribute("y");
      var w = +r.getAttribute("width"), h = +r.getAttribute("height");
      if (!w || !h) return;
      var inner = r.cloneNode(false);
      inner.setAttribute("class", "inner");
      inner.setAttribute("x", x + 3);
      inner.setAttribute("y", y + 3);
      inner.setAttribute("width", Math.max(0, w - 6));
      inner.setAttribute("height", Math.max(0, h - 6));
      inner.style.fill = "none";
      r.parentNode.insertBefore(inner, r.nextSibling);
    });
  }

  function renderMermaid() {
    if (!diagrams.length || typeof mermaid === "undefined") return;
    mermaid.initialize({
      startOnLoad: false,
      theme: "base",
      themeVariables: tokens(),
      flowchart: { curve: "linear", htmlLabels: true },
      sequence: { mirrorActors: false },
    });
    diagrams.forEach(function (d) {
      d.el.removeAttribute("data-processed");
      d.el.textContent = d.src;
    });
    mermaid.run({ nodes: diagrams.map(function (d) { return d.el; }) }).then(function () {
      diagrams.forEach(function (d) { doubleClusters(d.el); });
    });
  }

  function loadMermaid() {
    var els = document.querySelectorAll("pre.mermaid");
    if (!els.length) return;
    els.forEach(function (el) { diagrams.push({ el: el, src: el.textContent }); });
    var s = document.createElement("script");
    s.src = window.PAROS.mermaid;
    s.onload = renderMermaid;
    document.head.appendChild(s);
    window.addEventListener("paros-theme", renderMermaid);
  }

  // ---- table of contents: mark the heading in view ----
  function watchToc() {
    var links = document.querySelectorAll(".toc a");
    if (!links.length || !("IntersectionObserver" in window)) return;
    var byId = {};
    links.forEach(function (a) { byId[decodeURIComponent(a.hash.slice(1))] = a; });
    var obs = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (!e.isIntersecting) return;
        links.forEach(function (a) { a.classList.remove("active"); });
        var a = byId[e.target.id];
        if (a) a.classList.add("active");
      });
    }, { rootMargin: "-60px 0px -70% 0px" });
    Object.keys(byId).forEach(function (id) {
      var h = document.getElementById(id);
      if (h) obs.observe(h);
    });
  }

  // ---- search over Zola's index: title hits first, then body hits ----
  var docs = null;

  function loadIndex(done) {
    if (docs) return done();
    var s = document.createElement("script");
    s.src = window.PAROS.search;
    s.onload = function () {
      var store = window.searchIndex && window.searchIndex.documentStore && window.searchIndex.documentStore.docs;
      docs = store ? Object.keys(store).map(function (k) { return store[k]; }) : [];
      done();
    };
    document.body.appendChild(s);
  }

  function esc(t) {
    return t.replace(/[&<>"]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]; });
  }

  function search(q) {
    var words = q.toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) return [];
    var hits = [];
    docs.forEach(function (d) {
      var title = (d.title || "").toLowerCase();
      var body = (d.body || "").toLowerCase();
      var score = 0;
      for (var i = 0; i < words.length; i++) {
        var w = words[i];
        var inTitle = title.indexOf(w) >= 0;
        var at = body.indexOf(w);
        if (!inTitle && at < 0) return;
        if (inTitle) score += 10;
        if (at >= 0) score += 1 + Math.min(5, body.split(w).length - 2);
      }
      hits.push({ d: d, score: score });
    });
    hits.sort(function (a, b) { return b.score - a.score; });
    return hits.slice(0, 12);
  }

  function snippet(body, w) {
    var at = body.toLowerCase().indexOf(w);
    if (at < 0) return esc(body.slice(0, 120));
    var start = Math.max(0, at - 50);
    var text = (start ? "…" : "") + body.slice(start, at + 90) + "…";
    var re = new RegExp("(" + w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + ")", "ig");
    return esc(text).replace(re, "<mark>$1</mark>");
  }

  function setupSearch() {
    var dlg = document.getElementById("search");
    var input = document.getElementById("search-input");
    var list = document.getElementById("search-results");
    if (!dlg) return;
    var sel = -1;

    function open() {
      if (!dlg.open) dlg.showModal();
      input.select();
      loadIndex(run);
    }

    function run() {
      if (!docs) return;
      var q = input.value.trim();
      var first = q.toLowerCase().split(/\s+/)[0] || "";
      sel = -1;
      list.innerHTML = search(q).map(function (h) {
        var path = h.d.id.replace(/^https?:\/\/[^/]+/, "").replace(/^\/paros/, "");
        return '<li><a href="' + esc(h.d.id) + '">' + esc(h.d.title || path) +
          '<span class="where">' + esc(path) + '</span><span class="snip">' + snippet(h.d.body || "", first) + "</span></a></li>";
      }).join("");
    }

    function move(by) {
      var items = list.querySelectorAll("a");
      if (!items.length) return;
      sel = (sel + by + items.length) % items.length;
      items.forEach(function (a, i) { a.classList.toggle("sel", i === sel); });
      items[sel].scrollIntoView({ block: "nearest" });
    }

    input.addEventListener("input", run);
    input.addEventListener("keydown", function (e) {
      if (e.key === "ArrowDown") { e.preventDefault(); move(1); }
      else if (e.key === "ArrowUp") { e.preventDefault(); move(-1); }
      else if (e.key === "Enter") {
        var a = list.querySelectorAll("a")[sel < 0 ? 0 : sel];
        if (a) { e.preventDefault(); window.location.href = a.href; }
      }
    });
    dlg.addEventListener("click", function (e) { if (e.target === dlg) dlg.close(); });
    document.querySelectorAll("[data-search-open]").forEach(function (b) { b.addEventListener("click", open); });
    document.addEventListener("keydown", function (e) {
      var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement && document.activeElement.tagName);
      if ((e.key === "/" && !typing) || ((e.ctrlKey || e.metaKey) && e.key === "k")) {
        e.preventDefault();
        open();
      }
    });
  }

  decorateCode();
  tagCallouts();
  boxTables();
  loadMermaid();
  watchToc();
  setupSearch();
})();
