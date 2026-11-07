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

import { MATCH_EVENTS, MATCH_KINDS, Matchmakers } from "./matchmakers.js";

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

// The roles a stateless machine takes (#822 (stateless playground roles)),
// as the registry names them, and as people read them.
const BOOKED = { proxy_leader: "proxy leader", batcher: "batcher", unbatcher: "unbatcher",
  frontend: "frontend", resolver: "resolver", replica: "replica" };

// Whether machine `m` runs with PAROS_ROLES=stateless: no durable store.
export function isStateless(u, m) {
  const cls = u.topo?.classes?.[m];
  return cls ? cls === "stateless" : (u.config.stateless ?? []).includes(m);
}

// The bookings of machine `m` other than acceptors and matchmakers, as
// "proxy leader ×1" labels, from the cell's admin view.
function booked(u, m) {
  const counts = u.topo?.bookings?.[m] ?? {};
  return Object.entries(BOOKED).filter(([role]) => counts[role]).map(([role, name]) => `${name} ×${counts[role]}`);
}

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
    // What the matchmakers store and do, from their events.
    this.matchmakers = new Matchmakers();
    // Each tenant, by its decimal id.
    this.tenantIds = new Map();
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
      const id = tenant.journals[0]?.key.split(":")[0];
      if (id !== undefined) { t.id = id; this.tenantIds.set(id, t); }
      for (const j of tenant.journals) {
        if (!this.journals.has(j.key)) {
          this.journals.set(j.key, { key: j.key, name: j.name, tenant: t, label: `${t.label}/${j.name}` });
        }
      }
    }
  }

  event(e) {
    if (MATCH_EVENTS.has(e.name)) {
      const sent = this.matchmakers.add(e);
      if (!sent) return;
      const from = this.nodeToMachine.get(sent.from);
      const to = this.nodeToMachine.get(sent.to);
      if (from === undefined || to === undefined || from === to) return;
      this.msgs.push({ t: e.time_ms, from, to, kind: sent.kind, key: sent.key, slot: null, round: null });
    } else if (e.name === "msg_sent") {
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
    this.badges = new Map();
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
    // A stateless machine holds no journal: it is drawn apart from the
    // zones, in its own column.
    const stateless = members.filter((m) => isStateless(this.u, m));
    for (const m of members.filter((x) => !stateless.includes(x))) {
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
    if (stateless.length) {
      const column = el("div", { className: "zone stateless" }, el("div", { className: "zone-name", textContent: "stateless",
        title: "PAROS_ROLES=stateless: no durable store. Proxy leaders, batchers and frontends go here." }));
      for (const m of stateless) column.append(this.card(m, topo, idle));
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
    const stateless = isStateless(u, m);
    const card = el("div", { className: `machine${idle ? " idle" : ""}${stateless ? " stateless" : ""}${down ? " down" : ""}${cut ? " cut" : ""}` });
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
      // One matchmaker set per tenant (#190): the control journal names it.
      const control = tenant.journals.find((j) => j.name === "control");
      if (control && control.matchmakers.includes(m)) {
        const selected = followed && followed.split(":")[0] === control.key.split(":")[0];
        const badge = el("button", {
          className: `mmchip${selected ? " selected" : ""}${t.internal ? " internal" : ""}`,
          title: `${t.label}: matchmaker. It stores the configuration of each journal of the tenant, by ballot. Click to follow the tenant's control journal.`,
        }, el("span", { className: "hex", textContent: "⬡" }), t.label);
        badge.style.setProperty("--tenant", t.color);
        badge.dataset.mm = `${control.key.split(":")[0]}@${m}`;
        badge.onclick = () => this.select(control.key);
        sets.append(badge);
      }
    }
    if (sets.childElementCount) card.append(el("div", { className: "sets-row" }, sets));
    const jobs = booked(u, m);
    if (jobs.length) card.append(el("div", { className: "booked-row", textContent: jobs.join(" · ") }));
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
    // A matchmaker's message leaves and lands at its badge for the tenant.
    this.badges = new Map();
    for (const badge of stage.querySelectorAll(".mmchip")) {
      const r = badge.getBoundingClientRect();
      this.badges.set(badge.dataset.mm, { x: r.left - box.left + r.width / 2, y: r.top - box.top + r.height / 2 });
    }
    const c = client.getBoundingClientRect();
    this.clientPos = { x: c.left - box.left + c.width / 2, y: c.top - box.top + c.height / 2 };
    this.bounds = box;
  }

  // Where a message of `m` leaves or lands on machine `machine`: a
  // matchmaker's end is its badge for the tenant, a node's end the
  // journal's chip, else the machine.
  endpoint(m, machine, matchmaker) {
    if (matchmaker) {
      const badge = this.badges.get(`${m.key.split(":")[0]}@${machine}`);
      if (badge) return badge;
    }
    return this.chips.get(`${m.key}@${machine}`) ?? this.positions[machine];
  }

  // Dots in flight: every message sent in the last `travel` simulated ms.
  draw(t, travel, showBeats) {
    const u = this.u;
    const ctx = this.ctx;
    if (!this.bounds) return;
    // A matchmaker badge lights while its matchmaker works for the tenant.
    for (const badge of this.els.stage.querySelectorAll(".mmchip")) {
      const [tenant, m] = badge.dataset.mm.split("@");
      const node = u.topo?.nodes?.[Number(m)];
      badge.classList.toggle("busy", Boolean(node) && u.matchmakers.active(tenant, node, t, Math.max(400, travel * 2)));
    }
    ctx.clearRect(0, 0, this.bounds.width, this.bounds.height);
    const followed = this.followed();
    const lo = lowerBound(u.msgs, t - travel, (m) => m.t);
    const hi = lowerBound(u.msgs, t + 1e-9, (m) => m.t);
    let budget = 400;
    const busy = hi - lo > budget;
    for (let i = hi - 1; i >= lo && budget > 0; i--) {
      const m = u.msgs[i];
      if (BEATS.has(m.kind) && (!showBeats || busy)) continue;
      const a = this.endpoint(m, m.from, m.kind === "match_reply" || m.kind === "gc_ack" || m.kind === "handover_reply");
      const b = this.endpoint(m, m.to, m.kind === "match" || m.kind === "gc" || m.kind === "handover");
      if (!a || !b) continue;
      const tenant = u.journals.get(m.key)?.tenant ?? u.tenantIds.get(m.key.split(":")[0]);
      const focus = !followed || m.key === followed || (MATCH_KINDS.has(m.kind) && m.key.split(":")[0] === followed.split(":")[0]);
      dot(ctx, a, b, (t - m.t) / travel, m.kind, tenant ? tenant.color : NO_TENANT, focus);
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
    if (place.matchmakers.length) parts.push(matchmakerSection(u, key, place));
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

/// A ballot as people read it: its round and the machine that owns it.
function ballotText(u, b) {
  if (!b) return "–";
  const m = u.nodeToMachine.get(b.node);
  return `${b.round}·${m === undefined ? b.node.slice(0, 6) : `m${m}`}`;
}

function machines(u, nodes) {
  return nodes.map((n) => { const m = u.nodeToMachine.get(n); return m === undefined ? "?" : `m${m}`; });
}

/// The followed journal's matchmakers (Matchmaker Paxos): the tenant's set,
/// each one's floor, the configurations they store by ballot, and what the
/// last leader did with them. Read from the events the machines log.
function matchmakerSection(u, key, place) {
  const set = place.matchmakers.map((m) => u.topo.nodes[m]).filter(Boolean);
  const view = u.matchmakers.view(key, set);
  const tenantId = key.split(":")[0];
  // The set's generation, once a matchmaker reported one: a handover
  // (#125) gives the tenant a new set of a later generation.
  const generation = u.matchmakers.generations.get(tenantId);
  const parts = [el("h4", { className: "mm-head" }, el("span", { className: "hex", textContent: "⬡" }),
    generation === undefined ? "Matchmakers" : `Matchmakers · generation ${generation}`)];
  parts.push(el("p", { className: "muted", textContent:
    "The tenant's matchmakers store each configuration of the journal under its ballot. "
    + "A new leader registers its configuration on a majority of them and gets the older ones back. "
    + "Then it raises their floor (GC): a configuration under the floor is dropped." }));
  const row = el("div", { className: "majority" });
  for (const m of place.matchmakers) {
    const floor = view?.floors.get(u.topo.nodes[m]);
    const cell = el("span", { className: "acceptor matchmaker", title: `m${m}: matchmaker of the tenant. Floor: ${ballotText(u, floor)}.` },
      el("b", { textContent: `m${m}` }), el("small", { textContent: ` floor ${ballotText(u, floor)}` }));
    if (u.topo.down?.includes(m) || u.topo.isolated?.includes(m)) cell.classList.add("off");
    row.append(cell);
  }
  parts.push(row);
  if (!view || !view.rows.length) {
    parts.push(el("p", { className: "muted", textContent: "No registration seen yet: a leader registers at its election." }));
    return el("div", { className: "mm-section" }, ...parts);
  }
  const phase = view.phase;
  if (phase) {
    const leader = machines(u, [phase.leader])[0];
    const what = phase.reconfiguration ? "a new configuration (reconfiguration)" : "its configuration";
    const text = phase.done
      ? `Ballot ${ballotText(u, phase.ballot)}: leader ${leader} registered ${what} on ${phase.registeredBy} of ${set.length} matchmakers`
        + ` and got ${phase.prior} older configuration${phase.prior === 1 ? "" : "s"} back.`
        + (phase.floor ? ` The floor is now ${ballotText(u, phase.floor)}.` : " GC follows.")
      : `Ballot ${ballotText(u, phase.ballot)}: leader ${leader} asks the matchmakers to register ${what}.`;
    parts.push(el("p", { className: "round", textContent: text }));
  }
  const table = el("table", { className: "acceptors configs" });
  table.append(el("tr", {}, ...["ballot", "acceptors", "held by", ""].map((h) => el("th", { textContent: h }))));
  for (const c of view.rows) {
    const stored = el("td");
    for (const m of machines(u, c.kept)) stored.append(el("span", { className: "kept", textContent: `${m} ` }));
    for (const m of machines(u, c.dropped)) stored.append(el("s", { className: "dropped", title: "dropped by GC", textContent: `${m} ` }));
    table.append(el("tr", { className: c.state === "dropped" ? "gone" : "",
      title: c.state === "dropped" ? "Every matchmaker dropped this configuration: its ballot is under their floor." : "" },
      el("td", { textContent: ballotText(u, c.ballot),
        title: c.reconfiguration ? "Registered by a reconfiguration: a new set of acceptors." : "Registered at an election." },
        el("small", { textContent: c.reconfiguration ? " reconfig" : "" })),
      el("td", { textContent: machines(u, c.acceptors).join(" ") }),
      stored,
      el("td", { className: `state ${c.state}`, textContent: c.state === "newest" ? "in force" : c.state })));
  }
  parts.push(el("div", { className: "table-wrap" }, table));
  if (view.log.length) {
    const log = el("ol", { className: "mm-log" });
    for (const line of view.log) log.append(el("li", { textContent: logLine(u, line) }));
    parts.push(log);
  }
  return el("div", { className: "mm-section" }, ...parts);
}

function logLine(u, { t, text }) {
  const at = `${(t / 1000).toFixed(1)} s`;
  switch (text.kind) {
    case "started":
      return `${at} · ${machines(u, [text.leader])[0]} opens matchmaking at ballot ${ballotText(u, text.ballot)}${text.reconfiguration ? " (reconfiguration)" : ""}`;
    case "completed":
      return `${at} · a majority (${text.registeredBy}) registered ballot ${ballotText(u, text.ballot)}; ${text.prior} older returned`;
    case "gc":
      return `${at} · ${machines(u, [text.matchmaker])[0]} raises its floor to ${ballotText(u, text.floor)}${text.dropped ? `, drops ${text.dropped}` : ""}`;
    default:
      return at;
  }
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
    // A machine has no capacity in role slots (#837 (remove role slots)):
    // the column counts the roles it hosts: its acceptors and matchmakers,
    // and the other roles the registry books on it.
    const counts = topo?.bookings?.[m] ?? {};
    const hosted = acceptor + matchmaker + Object.keys(BOOKED).reduce((n, role) => n + (counts[role] ?? 0), 0);
    const jobs = booked(u, m);
    rows.push(el("tr", {},
      el("td", { textContent: `m${m}` }),
      el("td", { textContent: cellOf[m] ?? "–" }),
      el("td", { textContent: topo ? topo.zones[m] : `zone-${m % 3}` }),
      el("td", { textContent: isStateless(u, m) ? "stateless" : "all" }),
      el("td", {}, state),
      el("td", { textContent: acceptor ? `acceptor ×${acceptor}` : "–" }),
      el("td", { textContent: leads ? `leader ×${leads}` : "–" }),
      el("td", { textContent: matchmaker ? `matchmaker ×${matchmaker}` : "–" }),
      el("td", { textContent: jobs.length ? jobs.join(", ") : "–" }),
      el("td", { textContent: hosted ? String(hosted) : "–" })));
  }
  const table = el("table", { className: "roles" });
  table.append(el("tr", {}, ...["machine", "cell", "zone", "class", "state", "acceptor", "leader", "matchmaker", "booked", "roles"]
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
  } else if (kind === "match" || kind === "match_reply" || kind === "handover" || kind === "handover_reply") {
    // A hexagon: a matchmaker's message. Hollow on the way to it, full on
    // the way back.
    ctx.strokeStyle = color; ctx.fillStyle = color; ctx.lineWidth = 2;
    ctx.beginPath();
    for (let k = 0; k < 6; k++) {
      const a = Math.PI / 6 + (k * Math.PI) / 3;
      const px = x + 6 * Math.cos(a), py = y + 6 * Math.sin(a);
      if (k) ctx.lineTo(px, py); else ctx.moveTo(px, py);
    }
    ctx.closePath();
    if (kind.endsWith("reply")) ctx.fill(); else ctx.stroke();
  } else if (kind === "gc" || kind === "gc_ack") {
    // A cross: GC, the request to raise the floor and its answer.
    ctx.strokeStyle = color; ctx.lineWidth = kind === "gc" ? 2 : 3;
    ctx.beginPath(); ctx.moveTo(x - 5, y - 5); ctx.lineTo(x + 5, y + 5); ctx.moveTo(x + 5, y - 5); ctx.lineTo(x - 5, y + 5); ctx.stroke();
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
  items.push(el("span", { className: "key", textContent: "◇ Phase 1 · ● Phase 2 · ■ chosen · ▲ read · ⬡ matchmaker · ✕ GC · ◯ client call" }));
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
