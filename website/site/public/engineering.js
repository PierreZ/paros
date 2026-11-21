// The live parts of the Engineering pages and of the Consensus driver page (#812 (website redo 3/4), #1349).
//
// The widgets run paros-consensus compiled to wasm (the `paxos` bundle of
// paros-wasm, staged at /paxos/wasm/ by scripts/build-wasm.sh) and draw what
// it returns. No protocol logic here: every fact comes from the core.
//
//   [data-widget="ready"]        the Ready inspector (consensus/driver)
//   [data-widget="quorums"]      the quorum picker (grid quorums)
const PAXOS = new URL("paxos/wasm/paros_paxos.js", import.meta.url);

// ---- small helpers ----

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

function chip(text, kind) {
  return el("span", "chip-status" + (kind ? " " + kind : ""), text);
}

function strip(items) {
  const s = el("div", "strip");
  for (const [k, v] of items) {
    const d = el("div");
    d.append(el("span", "k", k));
    const value = el("span", "v");
    if (v instanceof Node) value.append(v); else value.textContent = v;
    d.append(value);
    s.append(d);
  }
  return s;
}

function panel(num, title, meta) {
  const p = el("section", "panel");
  const head = el("div", "panel-head label");
  if (num) head.append(el("span", "num", num));
  head.append(el("span", "title", title));
  if (meta != null) {
    const m = el("span", "meta");
    if (meta instanceof Node) m.append(meta); else m.textContent = meta;
    head.append(m);
  }
  const body = el("div", "panel-body");
  p.append(head, body);
  return { panel: p, head, body };
}

function button(text, primary) {
  const b = el("button", primary ? "btn primary" : "btn", text);
  b.type = "button";
  return b;
}

// ---- the paros-consensus engine: the paxos bundle ----

let paxosEngine = null;
function paxos() {
  if (!paxosEngine) {
    paxosEngine = import(PAXOS.href).then(async (m) => { await m.default(); return m; });
  }
  return paxosEngine;
}

function needsWasm(mount, error) {
  console.warn("paros-consensus in wasm did not load", error);
  mount.replaceChildren(el("p", "aside", "This demo needs paros-consensus compiled to wasm (scripts/build-wasm.sh)."));
}

// ---- sans-io: the Ready inspector ----

function ready(mount) {
  paxos().then((engine) => readyLab(mount, engine), (e) => needsWasm(mount, e));
}

function readyLab(mount, engine) {
  let lab = new engine.ReadyLab();
  let view = JSON.parse(lab.view());
  let n = 0;

  const nodesUi = panel("01", "Three ColocatedNodes", "");
  const reset = button("Reset");
  nodesUi.head.querySelector(".meta").append(reset);
  const wireUi = panel("02", "The wire", null);
  const readyUi = panel("03", "What the last input gave the driver", null);
  mount.replaceChildren(nodesUi.panel, wireUi.panel, readyUi.panel);

  function act(input) {
    const answer = JSON.parse(lab.act(JSON.stringify(input)));
    if (!answer.error || answer.nodes) view = answer;
    draw(answer.error && !answer.nodes ? answer.error : null);
  }

  reset.onclick = () => { lab.free(); lab = new engine.ReadyLab(); view = JSON.parse(lab.view()); draw(); };

  function draw(problem) {
    // 01: the nodes and their inputs.
    const cards = el("div", "lab-nodes");
    for (const node of view.nodes) {
      const card = el("div", "lab-node");
      card.append(el("div", "label", `node ${node.id} · ${node.role}`));
      card.append(el("div", "lab-state tnum", `promise ${node.promise} · ${node.records} on disk · ${node.chosen} chosen`));
      const row = el("div", "lab-buttons");
      const t = button("timeout"); t.onclick = () => act({ kind: "timeout", node: node.id });
      const k = button("tick"); k.onclick = () => act({ kind: "tick", node: node.id });
      const w = button("write"); w.onclick = () => act({ kind: "write", node: node.id, text: "r" + (n++) });
      t.title = "tick() with the election clock run out";
      w.title = "propose() a client record at this node";
      row.append(t, k, w);
      card.append(row);
      cards.append(card);
    }
    nodesUi.body.replaceChildren(cards);
    // 02: the wire.
    const list = el("ol", "eng-log lab-wire");
    for (const m of view.wire) {
      const li = el("li");
      const d = button("deliver"); d.onclick = () => act({ kind: "deliver", id: m.id });
      const x = button("lose"); x.onclick = () => act({ kind: "drop", id: m.id });
      li.append(el("span", "t tnum", `${m.from} → ${m.to}`), el("span", "lab-msg", m.summary), el("span", "lab-buttons", null));
      li.lastChild.append(d, x);
      list.append(li);
    }
    const all = button("deliver all", true);
    all.onclick = () => {
      // The page only repeats the reader's own input: deliver the oldest
      // message, until the wire is empty or a bound is reached.
      for (let i = 0; i < 60 && view.wire.length; i++) act({ kind: "deliver", id: view.wire[0].id });
    };
    wireUi.body.replaceChildren(view.wire.length ? list : el("p", "aside", "No message on the wire."), view.wire.length ? all : el("span"));
    // 03: the last input and its Ready batches.
    const out = [];
    if (problem || view.error) out.push(el("p", "callout", problem || view.error));
    const last = view.last;
    if (!last) {
      out.push(el("p", "aside", "Start: press timeout on node 0. Its election clock runs out, and it campaigns."));
    } else {
      out.push(strip([["Input", last.input], ["Answer", last.answer || "—"], ["Ready batches", String(last.readies.length)]]));
      if (!last.readies.length) out.push(el("p", "aside", "Nothing to do: the input changed no durable state and sent nothing."));
      last.readies.forEach((r, i) => {
        const box = el("div", "lab-ready");
        box.append(el("div", "label", `ready() #${i + 1}`));
        const b = el("ol", "lab-buckets");
        const bucket = (num, name, items, extra) => {
          const li = el("li");
          const head = el("div", "lab-bucket label");
          head.append(el("span", "num", num), el("span", null, name));
          if (extra) head.append(extra);
          li.append(head);
          const ul = el("ul", "lab-items");
          if (!items.length) ul.append(el("li", "aside", "empty"));
          for (const it of items) ul.append(el("li", null, it));
          li.append(ul);
          b.append(li);
        };
        bucket("1", "writes(): persist", r.writes.map((w) => `${w.kind}: ${w.detail}`), r.sync ? chip("SYNC", "accent") : null);
        bucket("2", "messages(): send, after the sync", r.messages.map((m) => `${m.kind} → node ${m.to}`));
        bucket("3", "committed(): answer the client", r.committed);
        bucket("4", "read_states(): serve the reads", r.read_states);
        const adv = el("li");
        const head = el("div", "lab-bucket label");
        head.append(el("span", "num", "5"), el("span", null, "advance()"));
        adv.append(head);
        b.append(adv);
        box.append(b);
        out.push(box);
      });
    }
    readyUi.body.replaceChildren(...out);
  }
  draw();
}

// ---- grid-quorums: the quorum picker ----

function quorums(mount) {
  paxos().then((engine) => picker(mount, engine), (e) => needsWasm(mount, e));
}

function picker(mount, engine) {
  const ui = panel("01", "Stop some acceptors", null);
  const fSel = el("select"); fSel.setAttribute("aria-label", "Failures per partition");
  for (const f of [1, 2]) fSel.append(new Option(`F = ${f} (${2 * f + 1} per partition)`, String(f)));
  const pSel = el("select"); pSel.setAttribute("aria-label", "Partitions");
  for (const p of [1, 2, 3, 4]) pSel.append(new Option(`${p} partition${p > 1 ? "s" : ""}`, String(p)));
  pSel.value = "3";
  const controls = el("div", "eng-controls");
  controls.append(fSel, pSel);
  const grid = el("div", "qgrid");
  const result = el("div", "eng-top");
  ui.body.append(controls, grid, result);
  mount.replaceChildren(ui.panel);
  let down = new Set();

  fSel.onchange = pSel.onchange = () => { down = new Set(); draw(); };

  function draw() {
    const v = JSON.parse(engine.quorum_reach(JSON.stringify({ f: Number(fSel.value), partitions: Number(pSel.value), down: [...down] })));
    if (v.error) { result.replaceChildren(el("p", "callout", v.error)); return; }
    grid.style.setProperty("--cols", String(v.cols));
    grid.replaceChildren();
    for (let c = 0; c < v.cols; c++) {
      const h = el("div", "qhead label", `partition ${c}`);
      h.classList.toggle("lost", !v.phase2[c]);
      h.style.gridColumn = String(c + 1);
      h.style.gridRow = "1";
      grid.append(h);
    }
    v.cells.forEach(([row, col], i) => {
      const b = button(down.has(i) ? `acceptor ${i} · down` : `acceptor ${i}`);
      b.classList.add("qcell");
      b.setAttribute("aria-pressed", String(down.has(i)));
      b.style.gridColumn = String(col + 1);
      b.style.gridRow = String(row + 2);
      b.onclick = () => { if (down.has(i)) down.delete(i); else down.add(i); draw(); };
      grid.append(b);
    });
    const writes = v.phase2.filter(Boolean).length;
    result.replaceChildren(
      strip([
        ["Election and tail read", chip(v.phase1 ? "CAN FINISH" : "BLOCKED", v.phase1 ? "done" : "gap")],
        ["Partitions that take writes", chip(`${writes} OF ${v.cols}`, writes === v.cols ? "done" : writes ? "shared" : "gap")],
      ]),
      el("p", "eng-verdict", `A write needs ${v.write_quorum} acceptors of its own partition. An election needs ${v.election_per_partition} acceptors of every partition. ${
        v.phase1 ? (writes === v.cols ? "Every quorum still holds." : "The slots of a lost partition wait; the other partitions take writes, and an election can still finish and reconfigure.")
          : writes ? "No election can finish: one partition lost its majority. The leader in place still writes to the partitions that hold one." : "Nothing can finish."}`),
    );
  }
  draw();
}

for (const mount of document.querySelectorAll('[data-widget="ready"]')) ready(mount);
for (const mount of document.querySelectorAll('[data-widget="quorums"]')) quorums(mount);
