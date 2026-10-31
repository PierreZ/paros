// The live view of a universe: its cells, their zones and machines, the
// journals each machine hosts, and every message and client call as a dot in
// flight. The page feeds it the frames the worker sends (`Universe.add`);
// the stage draws the universe at a simulated time `t` a little behind the
// newest frame, so the dots of each frame play out smoothly.

// Tenant colours are tokens (website/shared/tokens.css): CSS takes the var()
// as is, and the canvas resolves it with paint(), again after a theme switch.
const SERVED = [0, 1, 2, 3, 4, 5].map((i) => `var(--tenant-${i})`);
const INTERNAL = "var(--tenant-internal)";
const NO_TENANT = "var(--open)";

let painted = new Map();
window.addEventListener("paros-theme", () => { painted = new Map(); });

// A colour for the canvas: a var(--token) becomes its value on the page.
export function paint(color) {
  if (!color.startsWith("var(")) return color;
  let v = painted.get(color);
  if (v === undefined) {
    v = getComputedStyle(document.documentElement).getPropertyValue(color.slice(4, -1)).trim();
    painted.set(color, v);
  }
  return v;
}

export const BEATS = new Set(["heartbeat", "heartbeat_ack"]);
const READS = new Set(["pre_read", "pre_read_ack"]);
const PHASE1 = new Set(["prepare", "promise", "catchup_request", "catchup_response", "nack"]);
const PHASE2 = new Set(["accept", "accepted"]);

// How much simulated time the universe keeps: the stage, the rail and the
// timeline look back no further.
export const WINDOW_MS = 30_000;
// The timeline's bins.
const BIN_MS = 250;

function field(e, k) {
  for (const [key, v] of e.fields) if (key === k) return v;
  return undefined;
}

export function lowerBound(arr, t, get = (x) => x) {
  let lo = 0, hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (get(arr[mid]) < t) lo = mid + 1; else hi = mid;
  }
  return lo;
}

// ---- the universe as the page knows it ----

export class Universe {
  constructor(config) {
    this.config = config;
    this.now = 0;
    this.topo = null;
    this.msgs = [];
    this.calls = [];
    this.byToken = new Map();
    this.nodeToMachine = new Map();
    this.tenants = new Map();
    this.journals = new Map();
    this.served = 0;
    // Messages per bin, by tenant name: a Map of bin index to counts.
    this.bins = new Map();
    // The operator's steps, by their index in the log.
    this.steps = new Map();
  }

  // Take one frame from the worker.
  add(frame) {
    this.now = Math.max(this.now, frame.now_ms);
    for (const topo of frame.topology) this.learn(topo);
    for (const e of frame.events) this.event(e);
    for (const s of frame.steps) this.steps.set(s.index, s);
    this.trim();
  }

  learn(topo) {
    if (this.topo && topo.time_ms < this.topo.time_ms) return;
    this.topo = topo;
    topo.nodes.forEach((n, i) => { if (n) this.nodeToMachine.set(n, i); });
    for (const tenant of topo.tenants) {
      if (!this.tenants.has(tenant.name)) {
        const internal = tenant.kind !== "users";
        this.tenants.set(tenant.name, {
          name: tenant.name, internal,
          label: internal ? `${tenant.kind}@${tenant.cell}` : tenant.name,
          color: internal ? INTERNAL : SERVED[this.served++ % SERVED.length],
        });
      }
      const t = this.tenants.get(tenant.name);
      for (const j of tenant.journals) {
        if (!this.journals.has(j.key)) {
          this.journals.set(j.key, { key: j.key, name: j.name, tenant: t, label: `${t.label}/${j.name}` });
        }
      }
    }
  }

  event(e) {
    if (e.name === "msg_sent") {
      const to = this.nodeToMachine.get((field(e, "to") || "").replace("node:", ""));
      if (e.machine === null || to === undefined || to === e.machine) return;
      const key = `${field(e, "tenant")}:${field(e, "journal")}`;
      const slot = field(e, "slot");
      this.msgs.push({
        t: e.time_ms, from: e.machine, to, kind: field(e, "kind"), key,
        slot: slot === undefined ? null : Number(slot),
        round: field(e, "bround") ?? null,
      });
      const tenant = this.journals.get(key)?.tenant.name ?? "?";
      const bin = Math.floor(e.time_ms / BIN_MS);
      if (!this.bins.has(bin)) this.bins.set(bin, new Map());
      const counts = this.bins.get(bin);
      counts.set(tenant, (counts.get(tenant) || 0) + 1);
    } else if (e.name === "client_call") {
      const call = {
        t: e.time_ms, kind: field(e, "kind"),
        key: `${field(e, "tenant")}:${field(e, "journal")}`, answer: null, outcome: null,
      };
      this.byToken.set(field(e, "token"), call);
      this.calls.push(call);
    } else if (e.name === "client_answer") {
      const token = field(e, "token");
      const call = this.byToken.get(token);
      if (call) { call.answer = e.time_ms; call.outcome = field(e, "outcome"); this.byToken.delete(token); }
    }
  }

  // Forget what is older than the window.
  trim() {
    const floor = this.now - WINDOW_MS;
    const m = lowerBound(this.msgs, floor, (x) => x.t);
    if (m > 2048) this.msgs = this.msgs.slice(m);
    const c = lowerBound(this.calls, floor, (x) => x.t);
    if (c > 256) this.calls = this.calls.slice(c);
    const oldest = Math.floor(floor / BIN_MS);
    for (const bin of this.bins.keys()) if (bin < oldest) this.bins.delete(bin);
    if (this.byToken.size > 4096) this.byToken.clear();
  }

  place(key) {
    if (!this.topo) return null;
    for (const tenant of this.topo.tenants) for (const j of tenant.journals) if (j.key === key) return j;
    return null;
  }

  // The served journals, in the order they appeared, as `tenant/journal`.
  served_journals() {
    return [...this.journals.values()].filter((j) => !j.tenant.internal && j.name !== "control");
  }

  // The cell each machine is in, by index: a cell name or null (idle).
  cellOf() {
    const cells = new Array(this.config.machines).fill(null);
    for (const cell of this.topo?.cells ?? []) for (const m of cell.machines) cells[m] = cell.name;
    return cells;
  }
}

/// The latest Accept of `key` sent by `t`, the machines it addressed, and
/// the ones that answered Accepted by `t`.
function roundAt(u, key, t, place) {
  let last = -1;
  for (let i = lowerBound(u.msgs, t + 1e-9, (m) => m.t) - 1; i >= 0 && t - u.msgs[i].t < 5000; i--) {
    const m = u.msgs[i];
    if (m.key === key && m.kind === "accept") { last = i; break; }
  }
  if (last < 0) return null;
  const first = u.msgs[last];
  const slot = first.slot;
  const addressed = new Set();
  const accepted = new Set();
  for (let i = lowerBound(u.msgs, first.t - 50, (m) => m.t); i < u.msgs.length && u.msgs[i].t <= t; i++) {
    const m = u.msgs[i];
    if (m.key !== key || m.slot !== slot) continue;
    if (m.kind === "accept") addressed.add(m.to);
    if (m.kind === "accepted") accepted.add(m.from);
  }
  // The leader accepts its own proposal without a message, when it is an
  // acceptor the slot goes to.
  const leader = first.from;
  const position = place ? place.members.indexOf(leader) : -1;
  const grid = place?.quorum.system === "grid";
  if (position >= 0 && (!grid || position % place.quorum.cols === slot % place.quorum.cols)) {
    addressed.add(leader);
    accepted.add(leader);
  }
  return { slot, addressed, accepted, leader };
}

// ---- the stage ----

export class Stage {
  constructor(els, onSelect) {
    this.els = els;
    this.onSelect = onSelect;
    this.u = null;
    this.drawn = null;
    this.selected = null;
    this.positions = [];
    this.chips = new Map();
    this.ctx = els.wires.getContext("2d");
    new ResizeObserver(() => this.layout()).observe(els.stage);
  }

  load(u) {
    this.u = u;
    this.drawn = null;
    this.selected = null;
  }

  select(key) {
    this.selected = key;
    this.drawn = null;
    this.onSelect?.(key);
  }

  // The selected journal, or the newest served one.
  followed() {
    const u = this.u;
    if (this.selected && u.place(this.selected)) return this.selected;
    const served = u.served_journals().filter((j) => u.place(j.key));
    return served.length ? served[served.length - 1].key : null;
  }

  // The cells and their machine cards; drawn again only when the topology
  // in force or the selection changes.
  cards() {
    const u = this.u;
    const topo = u.topo;
    const stamp = `${topo?.time_ms}|${this.followed()}`;
    if (stamp === this.drawn) return;
    this.drawn = stamp;
    const cellOf = u.cellOf();
    const boxes = [];
    const cells = topo?.cells?.length ? topo.cells.map((c) => c.name) : [];
    for (const name of cells) {
      const members = [];
      for (let m = 0; m < u.config.machines; m++) if (cellOf[m] === name) members.push(m);
      boxes.push(this.box(name, members, topo, false));
    }
    const idle = [];
    for (let m = 0; m < u.config.machines; m++) if (cellOf[m] === null) idle.push(m);
    if (idle.length) boxes.push(this.box(cells.length ? "idle machines" : "idle machines: init forms cell-1", idle, topo, true));
    this.els.cells.replaceChildren(...boxes);
    this.layout();
  }

  box(name, members, topo, idle) {
    const zones = new Map();
    for (const m of members) {
      const zone = topo ? topo.zones[m] : `zone-${m % 3}`;
      if (!zones.has(zone)) zones.set(zone, []);
      zones.get(zone).push(m);
    }
    const box = el("div", { className: idle ? "cell idle-pool" : "cell" },
      el("div", { className: "cell-name", textContent: name }));
    const row = el("div", { className: "zones" });
    for (const [zone, ms] of [...zones].sort()) {
      const column = el("div", { className: "zone" }, el("div", { className: "zone-name", textContent: zone }));
      for (const m of ms) column.append(this.card(m, topo, idle));
      row.append(column);
    }
    box.append(row);
    return box;
  }

  card(m, topo, idle) {
    const u = this.u;
    const node = topo ? topo.machines[m] : null;
    const down = topo?.down?.includes(m);
    const cut = topo?.isolated?.includes(m);
    const card = el("div", { className: `machine${idle ? " idle" : ""}${down ? " down" : ""}${cut ? " cut" : ""}` });
    card.dataset.machine = m;
    card.append(el("div", { className: "machine-head" }, el("b", { textContent: `m${m}` }),
      el("small", { textContent: node ?? "idle" })));
    if (down || cut) {
      card.append(el("div", { className: "fault", textContent: down ? "crashed" : "cut off",
        title: down ? "Crashed: its process is down until a restart." : "Cut off: the network drops every message to or from it." }));
    }
    if (!topo) return card;
    const followed = this.followed();
    const sets = el("div", { className: "sets" });
    for (const tenant of topo.tenants) {
      const t = u.tenants.get(tenant.name);
      for (const j of tenant.journals) {
        if (!j.members.includes(m)) continue;
        const info = u.journals.get(j.key);
        const lead = j.leader === m;
        const chip = el("button", {
          className: `chip${lead ? " lead" : ""}${j.key === followed ? " selected" : ""}${t.internal ? " internal" : ""}`,
          title: `${info.label}: ${lead ? "leader and acceptor" : "acceptor"}. Click to follow it.`,
        }, el("span", { className: "swatch" }), info.label, el("i", { textContent: lead ? "L" : "A" }));
        chip.style.setProperty("--tenant", t.color);
        chip.dataset.at = `${j.key}@${m}`;
        chip.onclick = () => this.select(j.key);
        card.append(chip);
      }
      const control = tenant.journals.find((j) => j.name === "control");
      if (control && control.matchmakers.includes(m)) {
        const badge = el("span", { className: "mm", title: `in ${t.label}'s matchmaker set`, textContent: "◆" });
        badge.style.color = t.color;
        sets.append(badge);
      }
    }
    if (sets.childElementCount) card.append(el("div", { className: "sets-row" }, el("small", { textContent: "matchmaker " }), sets));
    return card;
  }

  layout() {
    const { stage, wires, client } = this.els;
    const box = stage.getBoundingClientRect();
    const ratio = window.devicePixelRatio || 1;
    wires.width = Math.round(box.width * ratio);
    wires.height = Math.round(box.height * ratio);
    wires.style.width = `${box.width}px`;
    wires.style.height = `${box.height}px`;
    this.ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    this.positions = [];
    for (const card of stage.querySelectorAll(".machine")) {
      const r = card.getBoundingClientRect();
      this.positions[Number(card.dataset.machine)] = { x: r.left - box.left + r.width / 2, y: r.top - box.top + 14 };
    }
    // A journal's message leaves and lands at its own chip on each machine.
    this.chips = new Map();
    for (const chip of stage.querySelectorAll(".chip")) {
      const r = chip.getBoundingClientRect();
      this.chips.set(chip.dataset.at, { x: r.left - box.left + r.width / 2, y: r.top - box.top + r.height / 2 });
    }
    const c = client.getBoundingClientRect();
    this.clientPos = { x: c.left - box.left + c.width / 2, y: c.top - box.top + c.height / 2 };
    this.bounds = box;
  }

  // Dots in flight: every message sent in the last `travel` simulated ms.
  draw(t, travel, showBeats) {
    const u = this.u;
    const ctx = this.ctx;
    if (!this.bounds) return;
    ctx.clearRect(0, 0, this.bounds.width, this.bounds.height);
    const followed = this.followed();
    const lo = lowerBound(u.msgs, t - travel, (m) => m.t);
    const hi = lowerBound(u.msgs, t + 1e-9, (m) => m.t);
    let budget = 400;
    const busy = hi - lo > budget;
    for (let i = hi - 1; i >= lo && budget > 0; i--) {
      const m = u.msgs[i];
      if (BEATS.has(m.kind) && (!showBeats || busy)) continue;
      const a = this.chips.get(`${m.key}@${m.from}`) ?? this.positions[m.from];
      const b = this.chips.get(`${m.key}@${m.to}`) ?? this.positions[m.to];
      if (!a || !b) continue;
      const j = u.journals.get(m.key);
      dot(ctx, a, b, (t - m.t) / travel, m.kind, j ? j.tenant.color : NO_TENANT, !followed || m.key === followed);
      budget--;
    }
    // Client calls: from outside the cells to the journal's leader, and back.
    let calling = false;
    const from = lowerBound(u.calls, t - 5000, (c) => c.t);
    for (let i = from; i < u.calls.length; i++) {
      const call = u.calls[i];
      if (call.t > t) break;
      const place = u.place(call.key);
      const target = place && place.leader !== null
        ? this.chips.get(`${call.key}@${place.leader}`) ?? this.positions[place.leader] : null;
      if (call.answer === null || t < call.answer + travel) calling = true;
      if (!target) continue;
      const color = u.journals.get(call.key)?.tenant.color ?? NO_TENANT;
      if (t - call.t < travel) dot(ctx, this.clientPos, target, (t - call.t) / travel, "client", color, true);
      if (call.answer !== null && call.answer <= t && t - call.answer < travel) {
        dot(ctx, target, this.clientPos, (t - call.answer) / travel, "client", color, true);
      }
    }
    this.els.client.classList.toggle("busy", calling);
  }

  // The followed journal's quorum and acceptors.
  rail(t) {
    const u = this.u;
    const rail = this.els.rail;
    const key = this.followed();
    const info = key && u.journals.get(key);
    const place = info ? u.place(key) : null;
    if (!info || !place) {
      rail.replaceChildren(el("p", { className: "muted", textContent:
        "Create a journal, or click one on a machine: its quorum shape and its acceptors show here." }));
      return { label: null };
    }
    const q = place.quorum;
    const n = place.members.length;
    const round = roundAt(u, key, t, place);
    const head = el("h3", {}, el("span", { className: "swatch" }), info.label);
    head.style.setProperty("--tenant", info.tenant.color);
    const parts = [head];
    const leader = place.leader !== null ? `leader m${place.leader}` : "no leader";
    const status = el("p", { className: "chips" },
      chip(place.leader !== null ? "led" : "electing", place.leader !== null ? "done" : "gap"),
      chip(q.system, "accent"),
      ...(place.matchmakers.length ? [chip("matchmakers", "shared")] : []));
    parts.push(status);
    if (q.system === "grid") {
      parts.push(el("p", { textContent: `grid ${q.rows}×${q.cols} · ${n} acceptors · ${leader}` }));
      const grid = el("div", { className: "grid" });
      grid.style.gridTemplateColumns = `auto repeat(${q.cols}, 1fr)`;
      grid.append(el("span"));
      for (let c = 0; c < q.cols; c++) {
        grid.append(el("span", { className: `axis${round && round.slot % q.cols === c ? " lit" : ""}`, textContent: `col ${c}` }));
      }
      for (let r = 0; r < q.rows; r++) {
        grid.append(el("span", { className: "axis", textContent: `row ${r}` }));
        for (let c = 0; c < q.cols; c++) grid.append(this.cell(place.members[r * q.cols + c], place, round));
      }
      parts.push(grid);
      parts.push(el("p", { className: "muted", textContent:
        `A full row is a Phase-1 quorum (election, read). A full column is a Phase-2 quorum: slot s goes to column s mod ${q.cols}.` }));
    } else {
      const need = Math.floor(n / 2) + 1;
      parts.push(el("p", { textContent: `majority · any ${need} of ${n} decide · ${leader}` }));
      const row = el("div", { className: "majority" });
      for (const m of place.members) row.append(this.cell(m, place, round));
      parts.push(row);
      parts.push(el("p", { className: "muted", textContent:
        `Every write goes to all ${n} acceptors. The first ${need} answers choose it.` }));
    }
    if (round) {
      const got = [...round.accepted].filter((m) => round.addressed.has(m)).length;
      parts.push(el("p", { className: "round", textContent:
        `Slot ${round.slot}: Accept sent to ${[...round.addressed].map((m) => `m${m}`).join(", ")}; ${got} accepted.` }));
    }
    if (place.acceptors?.length) parts.push(acceptorTable(place));
    rail.replaceChildren(...parts);
    return { label: info.label };
  }

  cell(m, place, round) {
    const c = el("span", { className: "acceptor", textContent: m === undefined ? "?" : `m${m}` });
    if (m === place.leader) c.classList.add("lead");
    if (round && round.addressed.has(m)) c.classList.add("asked");
    if (round && round.accepted.has(m)) c.classList.add("yes");
    return c;
  }
}

export function chip(text, kind = "") {
  return el("span", { className: `chip-status${kind ? ` ${kind}` : ""}`, textContent: text });
}

/// Each machine's acceptor state for the followed journal, as its `Inspect`
/// answered at the last look: a read only.
function acceptorTable(place) {
  const ballot = (b) => (b ? `${b.round}.${b.node}` : "–");
  const num = (v) => (v === null || v === undefined ? "–" : String(v));
  const table = el("table", { className: "acceptors" });
  table.append(el("tr", {}, ...["", "promised", "accepted", "chosen", "floor"].map((h) => el("th", { textContent: h }))));
  for (const a of place.acceptors) {
    const last = a.tail.length ? a.tail[a.tail.length - 1] : null;
    const accepted = a.accepted ? `${a.accepted}${last ? ` (s${last[0]})` : ""}` : "0";
    table.append(el("tr", { className: a.member ? "" : "out",
      title: a.member
        ? `m${a.machine}: promised ballot ${ballot(a.promised)}, holds ${a.accepted} accepted records.`
        : `m${a.machine} serves this journal but is not an acceptor of the configuration in force.` },
      el("td", { textContent: `m${a.machine}${a.machine === place.leader ? " L" : ""}` }),
      el("td", { textContent: ballot(a.promised) }),
      el("td", { textContent: accepted }),
      el("td", { textContent: num(a.chosen) }),
      el("td", { textContent: String(a.floor) })));
  }
  return el("div", { className: "table-wrap" }, el("p", { className: "muted", textContent:
    "Each machine's Inspect: the ballot it promised (round.node), the records it accepted, the highest slot it knows chosen, and its floor." }), table);
}

/// The machines and the roles each holds, from the newest topology.
export function roles(u) {
  const topo = u.topo;
  const cellOf = u.cellOf();
  const rows = [];
  const capacity = 32;
  for (let m = 0; m < u.config.machines; m++) {
    let acceptor = 0, leads = 0, matchmaker = 0;
    for (const tenant of topo?.tenants ?? []) {
      for (const j of tenant.journals) {
        if (j.members.includes(m)) acceptor++;
        if (j.leader === m) leads++;
        if (j.name === "control" && j.matchmakers.includes(m)) matchmaker++;
      }
    }
    const down = topo?.down?.includes(m);
    const cut = topo?.isolated?.includes(m);
    const state = down ? chip("crashed", "gap") : cut ? chip("cut off", "gap")
      : cellOf[m] ? chip("up", "done") : chip("idle");
    // The site's meter (site.css): its fill is the slots held.
    const meter = el("span", { className: "meter slots", title: `${acceptor} acceptor slots of ${capacity}`,
      style: `--fill:${Math.min(100, (acceptor / capacity) * 100)}%` });
    rows.push(el("tr", {},
      el("td", { textContent: `m${m}` }),
      el("td", { textContent: cellOf[m] ?? "–" }),
      el("td", { textContent: topo ? topo.zones[m] : `zone-${m % 3}` }),
      el("td", {}, state),
      el("td", { textContent: acceptor ? `acceptor ×${acceptor}` : "–" }),
      el("td", { textContent: leads ? `leader ×${leads}` : "–" }),
      el("td", { textContent: matchmaker ? `matchmaker ×${matchmaker}` : "–" }),
      el("td", {}, meter)));
  }
  const table = el("table", { className: "roles" });
  table.append(el("tr", {}, ...["machine", "cell", "zone", "state", "acceptor", "leader", "matchmaker", "slots"]
    .map((h) => el("th", { textContent: h }))));
  table.append(...rows);
  return table;
}

// ---- drawing ----

function dot(ctx, a, b, f, kind, color, focus) {
  color = paint(color);
  f = Math.min(1, Math.max(0, f));
  const dx = b.x - a.x, dy = b.y - a.y;
  const len = Math.hypot(dx, dy) || 1;
  // Requests and replies travel side by side, not on top of each other.
  const off = 4;
  const x = a.x + dx * f - (dy / len) * off;
  const y = a.y + dy * f + (dx / len) * off;
  ctx.globalAlpha = focus ? 1 : 0.3;
  if (BEATS.has(kind)) {
    ctx.globalAlpha = focus ? 0.6 : 0.25;
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(x, y, 2.5, 0, Math.PI * 2); ctx.fill();
  } else if (PHASE1.has(kind)) {
    ctx.strokeStyle = color; ctx.lineWidth = 2;
    ctx.beginPath(); ctx.moveTo(x, y - 6); ctx.lineTo(x + 6, y); ctx.lineTo(x, y + 6); ctx.lineTo(x - 6, y); ctx.closePath(); ctx.stroke();
  } else if (PHASE2.has(kind)) {
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(x, y, 5, 0, Math.PI * 2); ctx.fill();
  } else if (kind === "commit") {
    ctx.fillStyle = color; ctx.fillRect(x - 4, y - 4, 8, 8);
  } else if (READS.has(kind)) {
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.moveTo(x, y - 5); ctx.lineTo(x + 5, y + 4); ctx.lineTo(x - 5, y + 4); ctx.closePath(); ctx.fill();
  } else if (kind === "client") {
    ctx.fillStyle = color; ctx.strokeStyle = paint("var(--fg)"); ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.arc(x, y, 7, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
  } else {
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(x, y, 3, 0, Math.PI * 2); ctx.fill();
  }
  ctx.globalAlpha = 1;
}

/// The legend: one swatch per tenant, one glyph per message family.
export function legend(u) {
  const items = [];
  const seen = new Set();
  for (const t of u.tenants.values()) {
    const label = t.internal ? "internal" : t.label;
    if (seen.has(label)) continue;
    seen.add(label);
    const s = el("span", { className: "key" }, el("span", { className: "swatch" }), label);
    s.style.setProperty("--tenant", t.color);
    items.push(s);
  }
  items.push(el("span", { className: "key", textContent: "◇ Phase 1 · ● Phase 2 · ■ chosen · ▲ read · ◯ client call" }));
  return items;
}

/// The timeline: the last `WINDOW_MS` of simulated time, messages per bin
/// stacked by tenant, and the operator's steps as marks.
export function drawTimeline(canvas, u, t, marks) {
  const ratio = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  if (!w || !h) return;
  if (canvas.width !== Math.round(w * ratio) || canvas.height !== Math.round(h * ratio)) {
    canvas.width = Math.round(w * ratio); canvas.height = Math.round(h * ratio);
  }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, w, h);
  const start = t - WINDOW_MS;
  const x = (ms) => ((ms - start) / WINDOW_MS) * w;
  const order = [...u.tenants.values()].sort((a, b) => Number(b.internal) - Number(a.internal));
  let max = 1;
  for (const counts of u.bins.values()) {
    let s = 0;
    for (const v of counts.values()) s += v;
    max = Math.max(max, s);
  }
  const bw = Math.max(1, (BIN_MS / WINDOW_MS) * w - 1);
  const base = h - 14;
  for (const [bin, counts] of u.bins) {
    const bx = x(bin * BIN_MS);
    if (bx < -bw || bx > w) continue;
    let y = base;
    for (const tenant of order) {
      const v = counts.get(tenant.name);
      if (!v) continue;
      const bh = (v / max) * (base - 4);
      ctx.fillStyle = paint(tenant.color);
      ctx.fillRect(bx, y - bh, bw, bh);
      y -= bh;
    }
  }
  ctx.fillStyle = paint("var(--rule)");
  ctx.fillRect(0, base, w, 1);
  ctx.font = `10px ${paint("var(--font-mono)")}`;
  // A label that would overlap the one before it is left out.
  let free = -Infinity;
  for (const mark of [...marks].sort((a, b) => a.t - b.t)) {
    const mx = x(mark.t);
    if (mx < 0 || mx > w) continue;
    ctx.fillStyle = paint(mark.fault ? "var(--gap)" : "var(--accent)");
    ctx.fillRect(mx, 0, 1, h);
    if (mx < free) continue;
    ctx.fillText(mark.label, mx + 3, h - 3);
    free = mx + 6 + ctx.measureText(mark.label).width;
  }
}

export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "style") node.setAttribute("style", v); else node[k] = v;
  }
  node.append(...children);
  return node;
}
