// The live journal of /journal/live/: the whole journal v1 on six hosts, on
// paros-journal and paros-journal-io compiled to wasm (`LiveJournal` in
// crates/support/paros-wasm/src/journal/live.rs).
//
// The engine runs every role on the real library code: the register on three
// matchmakers, six acceptors on a grid of two columns, three replicas for each
// segment, one leader and the pure client. The page only asks it to run ticks
// and to play the reader's moves (append, read, roll, crash or restart a
// host), and draws the view it answers. No journal logic here.
//
//   <div data-live-journal></div>
const JOURNAL = new URL("journal/wasm/paros_journal_lab.js", import.meta.url);

// A frame every FRAME ms runs `speed` ticks of the engine.
const FRAME = 100;
const SPEEDS = [["slow", 1], ["normal", 4], ["fast", 16]];

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
function chip(text, kind) {
  return el("span", "chip-status" + (kind ? " " + kind : ""), text);
}
function strip(items) {
  const s = el("div", "strip");
  for (const [k, v] of items) {
    const d = el("div");
    d.append(el("span", "k", k), el("span", "v", v));
    s.append(d);
  }
  return s;
}

function mount(into, Engine) {
  let live = new Engine();
  let view = JSON.parse(live.view());
  let speed = SPEEDS[1][1];
  let timer = null;

  const panel = el("section", "panel lab jlab rlab llab");
  panel.id = "live-journal";
  const head = el("div", "panel-head label");
  head.append(el("span", "title", "The journal, live"));
  const badge = chip("live", "accent");
  badge.title = "paros-journal and paros-journal-io run in this page";
  head.append(badge);
  const meta = el("span", "meta tnum");
  head.append(meta);

  const controls = el("div", "lab-controls llab-controls");
  const play = button("Pause", "primary");
  play.dataset.live = "play";
  const step = button("Step");
  step.dataset.live = "step";
  const speedBox = el("div", "llab-speed");
  const speedButtons = SPEEDS.map(([name, ticks]) => {
    const b = el("button", "lab-tab" + (ticks === speed ? " now" : ""), name);
    b.type = "button";
    b.setAttribute("aria-pressed", String(ticks === speed));
    b.addEventListener("click", () => {
      speed = ticks;
      for (const o of speedButtons) {
        const on = o === b;
        o.classList.toggle("now", on);
        o.setAttribute("aria-pressed", String(on));
      }
    });
    speedBox.append(b);
    return b;
  });
  const restart = button("Start again");
  restart.dataset.live = "reset";
  controls.append(play, step, speedBox, restart);

  const moves = el("div", "lab-extras llab-moves");
  const append = button("Append a record");
  append.dataset.live = "append";
  const read = button("Read the journal");
  read.dataset.live = "read";
  const roll = button("Roll to a new segment");
  roll.dataset.live = "roll";
  moves.append(append, read, roll);

  const summary = el("div", "llab-summary");
  const hostsHead = el("p", "lab-sub label", "Hosts: each one a driver of paros-journal-io");
  const hosts = el("div", "lab-nodes llab-hosts");
  const mmHead = el("p", "lab-sub label", "Matchmakers: the register of segment records");
  const matchmakers = el("div", "lab-nodes rlab-members llab-mm");
  const segHead = el("p", "lab-sub label", "Segments of the register's done value");
  const segments = el("ol", "jlab-segments llab-segments");
  const readHead = el("p", "lab-sub label", "The last whole read");
  const records = el("div", "llab-read");
  const eventsHead = el("p", "lab-sub label", "What happened, newest first");
  const events = el("ol", "jlab-history llab-events");
  events.setAttribute("aria-live", "polite");
  panel.append(head, controls, moves, summary, hostsHead, hosts, mmHead, matchmakers, segHead, segments,
    readHead, records, eventsHead, events);
  into.replaceChildren(panel);

  // A part is drawn again only when its part of the view changed, so a
  // button under the pointer or the focus stays while nothing changes.
  const drawn = {};
  function changed(part, value) {
    const json = JSON.stringify(value);
    if (drawn[part] === json) return false;
    drawn[part] = json;
    return true;
  }

  function stopped(error) {
    setPlaying(false);
    for (const b of panel.querySelectorAll("button")) b.disabled = true;
    events.replaceChildren(el("li", null, "The engine stopped: " + error));
  }

  // One call into the engine. An abort of the wasm stops the timer.
  function engine(call) {
    let json;
    try {
      json = call();
    } catch (error) {
      console.warn("the live journal stopped", error);
      stopped(String(error));
      return;
    }
    const next = JSON.parse(json);
    if (next.error) {
      events.replaceChildren(el("li", null, next.error));
      return;
    }
    view = next;
    draw();
  }

  function act(move) {
    engine(() => live.act(JSON.stringify(move)));
  }

  function drawHost(h) {
    const card = el("div", "lab-node rlab-card llab-host" + (h.up ? "" : " down"));
    card.dataset.host = String(h.id);
    const name = el("p", "label llab-host-name");
    name.append(document.createTextNode("host " + h.id + " "), chip(h.up ? "up" : "down", h.up ? "" : "gap"));
    card.append(name);
    for (const r of h.roles) {
      const line = el("p", "lab-state llab-role llab-" + r.kind);
      line.append(el("b", null, r.name), document.createTextNode(" " + (r.detail ?? "(down: its disk stays)")));
      card.append(line);
    }
    const toggle = button(h.up ? "Crash" : "Restart");
    toggle.dataset.live = (h.up ? "crash:" : "restart:") + h.id;
    toggle.addEventListener("click", () => act({ kind: h.up ? "crash" : "restart", host: h.id }));
    card.append(toggle);
    return card;
  }

  function draw() {
    meta.textContent = view.ticks + (view.ticks === 1 ? " tick" : " ticks");
    summary.replaceChildren(strip([
      ["writer A", view.writer],
      ["writes done", String(view.written)],
      ["register", view.register ? "a majority holds it" : "no majority"],
    ]));
    append.disabled = view.busy || view.reading;
    read.disabled = view.busy || view.reading;

    if (changed("hosts", view.hosts)) hosts.replaceChildren(...view.hosts.map(drawHost));

    if (changed("matchmakers", view.matchmakers)) matchmakers.replaceChildren(...view.matchmakers.map((m) => {
      const card = el("div", "lab-node rlab-card" + (m.up ? "" : " down"));
      card.dataset.matchmaker = String(m.id);
      const name = el("p", "label");
      name.append(document.createTextNode("m" + m.id + " "), chip(m.up ? "up" : "down", m.up ? "" : "gap"));
      card.append(name);
      card.append(el("p", "lab-state", `write ${m.op} · controller term ${m.term}`));
      for (const s of m.segments) card.append(el("p", "lab-state", s));
      return card;
    }));

    if (changed("segments", view.segments)) segments.replaceChildren(...view.segments.map((s) => {
      const inForce = s.state === "in force";
      const li = el("li", "jlab-segment" + (inForce ? "" : " sealed"));
      li.dataset.segment = String(s.id);
      const name = el("p", "jlab-segment-name label");
      name.append(document.createTextNode(`Segment ${s.id} `), chip(s.state, inForce ? "accent" : ""));
      li.append(name);
      const slots = s.start == null ? "not in force yet"
        : s.end == null ? `slots from ${s.start}` : `slots ${s.start} to ${s.end - 1}`;
      li.append(el("p", "jlab-detail", slots));
      li.append(el("p", "jlab-detail", "replicas: " + s.replicas.join(", ")));
      return li;
    }));

    if (!changed("read", [view.reading, view.read])) {
      // The last read is drawn already.
    } else if (view.reading) {
      records.replaceChildren(el("p", "lab-empty", "The read runs: the client reads each segment from its replicas."));
    } else if (view.read) {
      const list = el("ol", "llab-records");
      for (const r of view.read) list.append(el("li", "lab-msg", r));
      records.replaceChildren(list);
      if (!view.read.length) records.replaceChildren(el("p", "lab-empty", "The journal holds no record yet."));
    } else {
      records.replaceChildren(el("p", "lab-empty", "No read yet. Press “Read the journal”."));
    }

    if (changed("events", view.events)) events.replaceChildren(...view.events.map((e) => el("li", null, e)));
  }

  function run() {
    engine(() => live.step(speed));
  }
  function setPlaying(on) {
    if (on && !timer) timer = setInterval(run, FRAME);
    if (!on && timer) { clearInterval(timer); timer = null; }
    play.textContent = on ? "Pause" : "Play";
    step.disabled = on;
  }

  play.addEventListener("click", () => setPlaying(!timer));
  step.addEventListener("click", () => engine(() => live.step(1)));
  restart.addEventListener("click", () => {
    if (live.free) live.free();
    live = new Engine();
    for (const part of Object.keys(drawn)) delete drawn[part];
    engine(() => live.view());
  });
  append.addEventListener("click", () => act({ kind: "append" }));
  read.addEventListener("click", () => act({ kind: "read" }));
  roll.addEventListener("click", () => act({ kind: "roll" }));
  // A page in the background runs no ticks.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) setPlaying(false);
  });

  draw();
  const still = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  setPlaying(!still);
}

for (const into of document.querySelectorAll("[data-live-journal]")) {
  import(JOURNAL.href)
    .then(async (m) => { await m.default(); return m; })
    .then((engine) => mount(into, engine.LiveJournal), (error) => {
      console.warn("paros-journal in wasm did not load", error);
      into.replaceChildren(el("p", "aside", "The live journal needs paros-journal compiled to wasm (scripts/build-wasm.sh)."));
    });
}
