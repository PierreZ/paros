// The stage of the Paxos pages: one drawing of the parties and the wire that
// the story (public/story.js) and the exercises (public/lab.js) share, so
// both keep one style.
//
// The parties sit in a fixed layout: one row per role, top to bottom, and a
// lane of an acceptor grid at its own cell. Each node shows its state lines,
// the strip of its log (a box per slot, a dotted box for a hole), and the
// disk writes of the last step before its reply leaves. A message on the
// wire is an arrow that waits on a gentle arc from its sender to its
// receiver. `step` plays one move: the message crosses (or stops half way
// and fades when it is lost), the receiver writes, then the messages it sent
// leave their sender.
//
// No protocol logic here: the page hands over the state to draw, as the
// trace or the engine gives it.
//
//   var stage = window.paxosStage.create(container);
//   stage.layout(lanes, { grid, log, used });
//   stage.draw(state, writes);          // state: { nodes, state, logs, wire }
//   var mine = stage.begin();           // cancels a move in progress
//   stage.step(before, after, { message, lost, writes }, mine).then(...)
(function () {
  var SVG = "http://www.w3.org/2000/svg";
  var MOVE = 650;      // ms for a message to cross
  var EMERGE = 260;    // ms for new messages to leave their sender
  var REST = 0.42;     // where a message waits on its path, from 0 to 1
  // A message: a short shaft and a head that points along +x, about the size
  // of a node label so that the direction reads on a phone.
  var ARROW = "M-9 -1.6 H0 V-5.5 L9 0 L0 5.5 V1.6 H-9 Z";
  // Rows of the drawing, top to bottom, by the role of a lane. The client and
  // the controller of a Journal page sit above the proposers.
  var ROWS = ["client", "controller", "proposer", "batcher", "matchmaker", "proxy", "node", "acceptor", "replica"];

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

  function create(container) {
    var lanes = [];      // { lane, i, x, y, w }
    var byId = {};
    var box = { w: 360, h: 200 };
    var nodesLayer = null, wireLayer = null;
    var token = 0;

    // Place the lanes. `used` (lane id -> true) keeps only some lanes; with
    // no `used`, every lane is drawn. `log` leaves room for the log strip.
    function layout(all, opts) {
      opts = opts || {};
      var used = opts.used && Object.keys(opts.used).length ? opts.used : null;
      var grid = opts.grid || null;
      var rows = {};
      lanes = [];
      byId = {};
      all.forEach(function (lane, i) {
        if (used && !used[lane.id]) return;
        var r = ROWS.indexOf(lane.role);
        var key = (r < 0 ? ROWS.length : r) * 10 + (lane.cell ? lane.cell.row : 0);
        (rows[key] = rows[key] || []).push({ lane: lane, i: i });
      });
      var keys = Object.keys(rows).map(Number).sort(function (a, b) { return a - b; });
      var widest = 1;
      keys.forEach(function (k) {
        widest = Math.max(widest, grid && rows[k][0].lane.cell ? grid.cols : rows[k].length);
      });
      var rowH = opts.log ? 150 : 124;
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
      container.textContent = "";
      var drawing = svg("svg", { viewBox: "0 0 " + box.w + " " + box.h, class: "stage-svg", role: "img" });
      drawing.style.maxWidth = Math.min(box.w * 1.25, 600) + "px";
      wireLayer = svg("g", { class: "stage-paths" });
      drawing.appendChild(wireLayer);
      nodesLayer = svg("g", { class: "stage-nodes" });
      drawing.appendChild(nodesLayer);
      container.appendChild(drawing);
    }

    // The nodes of state `st`: { nodes: [{ alive, role, holes }], state:
    // [[line]], logs: [[{ slot, value, state, control }]] }, by lane index.
    // `writes[i]` is what lane i wrote on its disk.
    function drawNodes(st, writes) {
      nodesLayer.textContent = "";
      lanes.forEach(function (entry) {
        var node = (st.nodes && st.nodes[entry.i]) || { alive: true };
        var lines = (st.state && st.state[entry.i]) || [];
        var g = svg("g", { class: "stage-node " + entry.lane.role + (node.alive ? "" : " down") + (node.role === "leader" ? " leader" : "") });
        g.dataset.lane = entry.lane.id;
        g.appendChild(svg("circle", { cx: entry.x, cy: entry.y, r: 17 }));
        g.appendChild(svg("text", { x: entry.x, y: entry.y + 4, class: "stage-node-id" }, short(entry.lane)));
        var y = entry.y + 32;
        // A state line keeps to the node's own column: a long one (a
        // matchmaker's registry) is cut, and the node's title holds it whole.
        var room = Math.max(8, Math.floor((entry.w - 8) / 6.2));
        var clip = function (text) { return text.length > room ? text.slice(0, room - 1) + "…" : text; };
        var first = node.alive ? (lines[0] || "") : "down";
        g.appendChild(svg("text", { x: entry.x, y: y, class: "stage-node-line" }, clip(first)));
        if (node.alive && lines[1]) g.appendChild(svg("text", { x: entry.x, y: y + 12, class: "stage-node-line" }, clip(lines.slice(1).join(" "))));
        // The log: one box per slot, a dotted box for a hole.
        var cells = (st.logs && st.logs[entry.i]) || [];
        var holes = node.holes || [];
        if (node.alive && (cells.length || holes.length)) {
          var slots = cells.map(function (c) { return { slot: c.slot, cls: c.state + (c.control ? " control" : ""), v: c.value }; })
            .concat(holes.map(function (h) { return { slot: h, cls: "hole", v: "" }; }))
            .sort(function (a, b) { return a.slot - b.slot; }).slice(-5);
          var w = 13, x0 = entry.x - (slots.length * w) / 2;
          slots.forEach(function (s, k) {
            var cell = svg("g", { class: "stage-slot " + s.cls });
            cell.dataset.slot = String(s.slot);
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
          wrote.slice(0, 2).forEach(function (text0, k) {
            var text = (k === 0 ? "disk " : "") + text0;
            if (text.length > fit) text = text.slice(0, fit - 1) + "…";
            var tw = text.length * 5.5 + 6;
            var tag = svg("g", { class: "stage-disk" });
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

    // The path of a flight: a gentle arc from its sender to its receiver,
    // kept off the two circles. The arc bends to the right of the direction
    // of travel, so a request and its reply on the same pair of nodes run on
    // two paths, and an arc that would cross a third node bends wide around
    // it.
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

    // A point on the path of a flight, from its sender (0) to its receiver
    // (1), with the heading of the path there, in degrees.
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
      var g = svg("g", { class: "stage-msg " + (cls || "") });
      g.style.color = "var(--phase-" + (flight.phase || "other") + ")";
      var head = svg("g", { class: "stage-msg-arrow" });
      head.appendChild(svg("path", { d: ARROW }));
      g.appendChild(head);
      place(g, flight, t);
      g.appendChild(svg("title", {}, flight.label));
      return g;
    }

    function drawWire(wire, skip) {
      wireLayer.textContent = "";
      (wire || []).forEach(function (flight) {
        var q = curve(flight);
        if (!q) return;
        wireLayer.appendChild(svg("path", { d: "M" + q.p0.x + " " + q.p0.y + " Q" + q.c.x + " " + q.c.y + " " + q.p2.x + " " + q.p2.y, class: "stage-path" }));
      });
      (wire || []).forEach(function (flight) {
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
          var t = ms > 0 ? Math.min(1, (now - t0) / ms) : 1;
          fn(t);
          if (t < 1) requestAnimationFrame(frame); else resolve(true);
        }
        requestAnimationFrame(frame);
      });
    }

    function label(g, flight) {
      g.appendChild(svg("text", { x: 0, y: -13, class: "stage-msg-label" }, flight.label.split(" · ")[0]));
    }

    // Play one move from state `before` to state `after`: the message
    // `move.message` (an id on the wire of `before`) crosses, or stops half
    // way and fades when `move.lost`; the receiver shows `move.writes`; then
    // the messages that are new in `after` leave their sender. Resolves to
    // false when a later `begin` cancelled it.
    function step(before, after, move, mine) {
      move = move || {};
      var flight = null;
      (before.wire || []).forEach(function (f) { if (f.id === move.message) flight = f; });
      var skip = {};
      if (flight) skip[flight.id] = true;
      drawNodes(before);
      drawWire(before.wire, skip);
      var chain = Promise.resolve(true);
      if (flight && byId[flight.from] && byId[flight.to]) {
        var g = dot(flight, REST, "now" + (move.lost ? " lost" : ""));
        label(g, flight);
        wireLayer.appendChild(g);
        var end = move.lost ? 0.6 : 1;
        chain = tween(MOVE, function (t) {
          var e = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
          place(g, flight, REST + (end - REST) * e);
          if (move.lost && t > 0.7) g.style.opacity = String(1 - (t - 0.7) / 0.3);
        }, mine);
      }
      return chain.then(function (ok) {
        if (!ok || mine !== token) return false;
        // The receiver writes, then answers.
        var writes = move.writes || [];
        var wrote = writes.some(function (w) { return w && w.length; });
        drawNodes(after, writes);
        var old = {};
        (before.wire || []).forEach(function (f) { old[f.id] = true; });
        var fresh = (after.wire || []).filter(function (f) { return !old[f.id]; });
        var hold = {};
        fresh.forEach(function (f) { hold[f.id] = true; });
        drawWire(after.wire, hold);
        if (!fresh.length) return tween(wrote ? 420 : 120, function () {}, mine);
        return tween(wrote ? 380 : 0, function () {}, mine).then(function (ok2) {
          if (!ok2) return false;
          var dots = fresh.map(function (f) {
            var d = dot(f, 0, "fresh");
            if (d) wireLayer.appendChild(d);
            return { f: f, d: d };
          });
          return tween(EMERGE, function (t) {
            dots.forEach(function (x) { if (x.d) place(x.d, x.f, REST * t); });
          }, mine);
        });
      });
    }

    return {
      layout: layout,
      draw: function (st, writes) { drawNodes(st, writes); drawWire(st.wire); },
      // Start a new move: any move in progress stops at its next frame.
      begin: function () { return ++token; },
      cancel: function () { token++; },
      // Whether move `mine` is still the latest.
      live: function (mine) { return mine === token; },
      wait: function (ms, mine) { return tween(ms, function () {}, mine); },
      step: step,
    };
  }

  window.paxosStage = { create: create, short: short, plain: plain, ROWS: ROWS };
})();
