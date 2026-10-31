// The cluster view: one cell, its zones and machines, the journals each
// machine hosts, and every message and client call as a dot in flight. The
// stage is a pure function of the simulated time `t`: scrubbing, playing
// and stepping all set `t` and draw again. Everything drawn comes from the
// run: the topology after each step and the events the machines logged.

// Tenant colours are tokens (website/shared/tokens.css): CSS takes the var()
// as is, and the canvas resolves it with paint(), again after a theme switch.
const SERVED = [0, 1, 2, 3, 4, 5].map((i) => `var(--tenant-${i})`);
const INTERNAL = ["var(--tenant-internal)", "var(--open)", "var(--muted)"];
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
const BEATS = new Set(["heartbeat", "heartbeat_ack"]);
const READS = new Set(["pre_read", "pre_read_ack"]);
const PHASE1 = new Set(["prepare", "promise", "catchup_request", "catchup_response", "nack"]);
const PHASE2 = new Set(["accept", "accepted"]);

// ---- the run, indexed once ----

function field(e, k) {
  for (const [key, v] of e.fields) if (key === k) return v;
  return undefined;
}

function lowerBound(arr, t, get = (x) => x) {
  let lo = 0, hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (get(arr[mid]) < t) lo = mid + 1; else hi = mid;
  }
  return lo;
}

export function index(run) {
  const topos = run.topology;
  const machines = run.config.machines;
  const nodeToMachine = new Map();
  for (const topo of topos) {
    topo.nodes.forEach((n, i) => { if (n) nodeToMachine.set(n, i); });
  }
  // Journals and tenants, in the order they first appear.
  const journals = new Map();
  const tenants = new Map();
  let served = 0, internal = 0;
  for (const topo of topos) {
    for (const tenant of topo.tenants) {
      if (!tenants.has(tenant.name)) {
        const isInternal = tenant.kind !== "users";
        const label = isInternal ? tenant.kind : tenant.name;
        tenants.set(tenant.name, {
          name: tenant.name, label, internal: isInternal,
          color: isInternal ? INTERNAL[internal++ % INTERNAL.length] : SERVED[served++ % SERVED.length],
        });
      }
      const t = tenants.get(tenant.name);
      for (const j of tenant.journals) {
        if (!journals.has(j.key)) journals.set(j.key, { key: j.key, name: j.name, tenant: t, label: `${t.label}/${j.name}` });
      }
    }
  }
  const msgs = [];
  const calls = [];
  const byToken = new Map();
  for (const e of run.events) {
    if (e.name === "msg_sent") {
      const to = nodeToMachine.get((field(e, "to") || "").replace("node:", ""));
      if (e.machine === null || to === undefined || to === e.machine) continue;
      const key = `${field(e, "tenant")}:${field(e, "journal")}`;
      const slot = field(e, "slot");
      msgs.push({
        t: e.time_ms, from: e.machine, to, kind: field(e, "kind"), key,
        slot: slot === undefined ? null : Number(slot),
        round: field(e, "bround") ?? null,
      });
    } else if (e.name === "client_call") {
      const call = {
        t: e.time_ms, kind: field(e, "kind"),
        key: `${field(e, "tenant")}:${field(e, "journal")}`, answer: null, outcome: null,
      };
      byToken.set(field(e, "token"), call);
      calls.push(call);
    } else if (e.name === "client_answer") {
      const call = byToken.get(field(e, "token"));
      if (call) { call.answer = e.time_ms; call.outcome = field(e, "outcome"); }
    }
  }
  // Per journal: the Phase-2 and commit messages, for the quorum rail.
  const perKey = new Map();
  msgs.forEach((m, i) => {
    if (!perKey.has(m.key)) perKey.set(m.key, { accept: [], accepted: [], commit: [], beat: [] });
    const p = perKey.get(m.key);
    if (m.kind === "accept") p.accept.push(i);
    else if (m.kind === "accepted") p.accepted.push(i);
    else if (m.kind === "commit") p.commit.push(i);
    else if (m.kind === "heartbeat") p.beat.push(i);
  });
  const lastEvent = run.events.length ? run.events[run.events.length - 1].time_ms : 0;
  const end = Math.max(run.config.run_ms, lastEvent);
  // The three acts, from the action log.
  const stepStart = (kind) => {
    const i = run.actions.findIndex((a) => a.op.kind === kind);
    const s = run.steps.find((x) => x.index === i);
    return i < 0 ? null : (s ? s.start_ms : run.actions[i].at_ms);
  };
  const acts = [
    { title: "The cell runs itself", start: 0 },
    { title: "A tenant arrives", start: stepStart("create_tenant") },
    { title: "Writes flow", start: [stepStart("write"), stepStart("read")].filter((x) => x !== null).sort((a, b) => a - b)[0] ?? null },
  ];
  // Messages per 100 ms, by tenant, for the timeline.
  const bin = 100;
  const bins = Math.ceil(end / bin) + 1;
  const hist = new Map();
  for (const m of msgs) {
    const j = journals.get(m.key);
    const name = j ? j.tenant.name : "?";
    if (!hist.has(name)) hist.set(name, new Uint32Array(bins));
    hist.get(name)[Math.floor(m.t / bin)] += 1;
  }
  return { run, topos, machines, journals, tenants, msgs, calls, perKey, end, acts, hist, bin };
}

// ---- what is true at time t ----

export function topoAt(ix, t) {
  let found = -1;
  ix.topos.forEach((topo, i) => { if (topo.time_ms <= t) found = i; });
  return found;
}

function placeOf(topo, key) {
  for (const tenant of topo.tenants) for (const j of tenant.journals) if (j.key === key) return j;
  return null;
}

/// The latest Accept of `key` sent by `t`, the machines it addressed, and
/// the ones that answered Accepted by `t`.
function roundAt(ix, key, t, place, pinned) {
  const p = ix.perKey.get(key);
  if (!p) return null;
  let a = lowerBound(p.accept, t + 1e-9, (i) => ix.msgs[i].t) - 1;
  // A message stepped to pins the round to its slot.
  if (pinned && pinned.key === key && pinned.slot !== null) {
    while (a >= 0 && ix.msgs[p.accept[a]].slot !== pinned.slot) a--;
  }
  if (a < 0) return null;
  const first = ix.msgs[p.accept[a]];
  const slot = first.slot;
  const addressed = new Set();
  for (let k = a; k >= 0 && ix.msgs[p.accept[k]].slot === slot; k--) addressed.add(ix.msgs[p.accept[k]].to);
  const accepted = new Set();
  const from = lowerBound(p.accepted, first.t - 50, (i) => ix.msgs[i].t);
  for (let k = from; k < p.accepted.length && ix.msgs[p.accepted[k]].t <= t; k++) {
    const m = ix.msgs[p.accepted[k]];
    if (m.slot === slot) accepted.add(m.from);
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

function chosenAt(ix, key, t) {
  const p = ix.perKey.get(key);
  if (!p) return null;
  let best = null;
  const hi = lowerBound(p.commit, t + 1e-9, (i) => ix.msgs[i].t);
  for (let k = Math.max(0, hi - 64); k < hi; k++) {
    const s = ix.msgs[p.commit[k]].slot;
    if (s !== null && (best === null || s > best)) best = s;
  }
  return best;
}

// ---- the stage ----

export class Stage {
  constructor(els, onSelect) {
    this.els = els;
    this.onSelect = onSelect;
    this.ix = null;
    this.topoIndex = null;
    this.selected = null;
    this.positions = [];
    this.ctx = els.wires.getContext("2d");
    new ResizeObserver(() => this.layout()).observe(els.stage);
  }

  load(ix) {
    this.ix = ix;
    this.topoIndex = null;
    if (!this.selected || !ix.journals.has(this.selected)) {
      const served = [...ix.journals.values()].filter((j) => !j.tenant.internal && j.name !== "control");
      const any = served[0] || [...ix.journals.values()].find((j) => !j.tenant.internal) || [...ix.journals.values()][0];
      this.selected = any ? any.key : null;
    }
  }

  select(key) {
    this.selected = key;
    this.topoIndex = null;
    this.onSelect?.(key);
  }

  // The machine cards, grouped by zone; drawn again only when the topology
  // in force or the selection changes.
  cards(t) {
    const ix = this.ix;
    const ti = topoAt(ix, t);
    if (ti === this.topoIndex) return;
    const previous = this.topoIndex === null || ti < this.topoIndex ? null : ix.topos[this.topoIndex];
    this.topoIndex = ti;
    const topo = ti >= 0 ? ix.topos[ti] : null;
    const zones = new Map();
    for (let m = 0; m < ix.machines; m++) {
      const zone = topo ? topo.zones[m] : `zone-${m % 3}`;
      if (!zones.has(zone)) zones.set(zone, []);
      zones.get(zone).push(m);
    }
    const had = new Set();
    if (previous) for (const tenant of previous.tenants) for (const j of tenant.journals) for (const m of j.members) had.add(`${j.key}@${m}`);
    const cell = this.els.cell;
    cell.replaceChildren(el("div", { className: "cell-name", textContent: topo ? "cell-1" : "forming the cell…" }));
    const row = el("div", { className: "zones" });
    for (const [zone, members] of zones) {
      const column = el("div", { className: "zone" }, el("div", { className: "zone-name", textContent: zone }));
      for (const m of members) column.append(this.card(m, topo, had, previous !== null));
      row.append(column);
    }
    cell.append(row);
    this.layout();
  }

  card(m, topo, had, animate) {
    const ix = this.ix;
    const node = topo ? topo.machines[m] : null;
    const down = topo?.down?.includes(m);
    const cut = topo?.isolated?.includes(m);
    const card = el("div", { className: `machine${node ? "" : " idle"}${down ? " down" : ""}${cut ? " cut" : ""}` });
    card.dataset.machine = m;
    card.append(el("div", { className: "machine-head" }, el("b", { textContent: `m${m}` }),
      el("small", { textContent: node ?? (topo ? "idle" : "") })));
    if (down || cut) {
      card.append(el("div", { className: "fault", textContent: down ? "crashed" : "cut off",
        title: down ? "Crashed: its process is down until a restart." : "Cut off: the network drops every message to or from it." }));
    }
    if (!topo) return card;
    const sets = el("div", { className: "sets" });
    for (const tenant of topo.tenants) {
      const t = ix.tenants.get(tenant.name);
      for (const j of tenant.journals) {
        if (!j.members.includes(m)) continue;
        const info = ix.journals.get(j.key);
        const lead = j.leader === m;
        const chip = el("button", {
          className: `chip${lead ? " lead" : ""}${j.key === this.selected ? " selected" : ""}${t.internal ? " internal" : ""}${animate && !had.has(`${j.key}@${m}`) ? " new" : ""}`,
          title: `${info.label}: ${lead ? "leader and acceptor" : "acceptor"}. Click to follow it.`,
        }, el("span", { className: "swatch" }), `${info.label}`, el("i", { textContent: lead ? "L" : "A" }));
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
    if (sets.childElementCount) card.append(el("div", { className: "sets-row" }, el("small", { textContent: "matchmaker for " }), sets));
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
    this.box = box;
  }

  // Dots in flight: every message sent in the last `travel` simulated ms.
  draw(t, travel, showBeats, stepped) {
    const ix = this.ix;
    const ctx = this.ctx;
    ctx.clearRect(0, 0, this.box.width, this.box.height);
    this.pinned = stepped === null ? null : ix.msgs[stepped];
    const lo = lowerBound(ix.msgs, t - travel, (m) => m.t);
    const hi = lowerBound(ix.msgs, t + 1e-9, (m) => m.t);
    let budget = 400;
    const busy = hi - lo > budget;
    for (let i = hi - 1; i >= lo && budget > 0; i--) {
      const m = ix.msgs[i];
      if (BEATS.has(m.kind) && (!showBeats || busy)) continue;
      const a = this.chips.get(`${m.key}@${m.from}`) ?? this.positions[m.from];
      const b = this.chips.get(`${m.key}@${m.to}`) ?? this.positions[m.to];
      if (!a || !b) continue;
      const j = ix.journals.get(m.key);
      const focus = !this.selected || m.key === this.selected;
      dot(ctx, a, b, (t - m.t) / travel, m.kind, j ? j.tenant.color : NO_TENANT, focus);
      budget--;
    }
    // Client calls: from outside the cell to the journal's leader, and back.
    const ti = topoAt(ix, t);
    const topo = ti >= 0 ? ix.topos[ti] : null;
    let calling = false;
    for (const call of ix.calls) {
      if (call.t > t) break;
      const place = topo && placeOf(topo, call.key);
      const target = place && place.leader !== null
        ? this.chips.get(`${call.key}@${place.leader}`) ?? this.positions[place.leader] : null;
      if (call.answer === null || t < call.answer + travel) calling = true;
      if (!target) continue;
      const color = ix.journals.get(call.key)?.tenant.color ?? NO_TENANT;
      if (t - call.t < travel) dot(ctx, this.clientPos, target, (t - call.t) / travel, "client", color, true);
      if (call.answer !== null && call.answer <= t && t - call.answer < travel) {
        dot(ctx, target, this.clientPos, (t - call.answer) / travel, "client", color, true);
      }
    }
    // The message stepped to: drawn large, on its way, with its name.
    if (this.pinned) {
      const m = this.pinned;
      const a = this.chips.get(`${m.key}@${m.from}`) ?? this.positions[m.from];
      const b = this.chips.get(`${m.key}@${m.to}`) ?? this.positions[m.to];
      if (a && b) {
        const color = ix.journals.get(m.key)?.tenant.color ?? NO_TENANT;
        ctx.strokeStyle = paint(color); ctx.globalAlpha = 0.5; ctx.lineWidth = 1.5; ctx.setLineDash([4, 4]);
        ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke(); ctx.setLineDash([]);
        ctx.globalAlpha = 1;
        const x = a.x + (b.x - a.x) * 0.6, y = a.y + (b.y - a.y) * 0.6;
        ctx.fillStyle = paint(color); ctx.beginPath(); ctx.arc(x, y, 9, 0, Math.PI * 2); ctx.fill();
        ctx.font = `600 12px ${paint("var(--font-mono)")}`;
        ctx.fillStyle = getComputedStyle(document.body).color;
        ctx.fillText(`${m.kind}${m.slot !== null ? ` s${m.slot}` : ""}`, x + 12, y - 8);
      }
    }
    const firstCall = ix.calls[0];
    this.els.client.hidden = !(firstCall && t >= firstCall.t);
    this.els.client.classList.toggle("busy", calling);
  }

  // The selected journal's quorum and log.
  rail(t) {
    const ix = this.ix;
    const rail = this.els.rail;
    const ti = topoAt(ix, t);
    const topo = ti >= 0 ? ix.topos[ti] : null;
    // The followed journal, or the newest one while it does not exist yet.
    let key = this.selected;
    if (topo && !placeOf(topo, key)) {
      const all = topo.tenants.flatMap((tenant) => tenant.journals);
      key = all.length ? all[all.length - 1].key : null;
    }
    const info = key && ix.journals.get(key);
    const place = info && topo ? placeOf(topo, key) : null;
    if (!info || !place) {
      rail.replaceChildren(el("h3", { textContent: "Click a journal" }),
        el("p", { className: "muted", textContent: "Its quorum shape and its log show here." }));
      return;
    }
    const q = place.quorum;
    const n = place.members.length;
    const round = roundAt(ix, key, t, place, this.pinned);
    const chosen = chosenAt(ix, key, t);
    const head = el("h3", {}, el("span", { className: "swatch" }), info.label);
    head.style.setProperty("--tenant", info.tenant.color);
    const parts = [head];
    const leader = place.leader !== null ? `leader m${place.leader}` : "no leader yet";
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
        for (let c = 0; c < q.cols; c++) {
          const m = place.members[r * q.cols + c];
          grid.append(this.cell(m, place, round));
        }
      }
      parts.push(grid);
      parts.push(el("p", { className: "muted", textContent:
        "A full row is a Phase-1 quorum (election, read). A full column is a Phase-2 quorum: slot s goes to column s mod " + q.cols + "." }));
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
    const log = el("div", { className: "logbar" });
    const count = chosen === null ? 0 : chosen + 1;
    const shown = Math.min(count, 48);
    for (let s = count - shown; s < count; s++) log.append(el("span", { title: `slot ${s} chosen` }));
    parts.push(el("p", { className: "muted", textContent: `Log: ${count} slot${count === 1 ? "" : "s"} chosen` }), log);
    rail.replaceChildren(...parts);
    rail.style.setProperty("--tenant", info.tenant.color);
  }

  cell(m, place, round) {
    const c = el("span", { className: "acceptor", textContent: m === undefined ? "?" : `m${m}` });
    if (m === place.leader) c.classList.add("lead");
    if (round && round.addressed.has(m)) c.classList.add("asked");
    if (round && round.accepted.has(m)) c.classList.add("yes");
    return c;
  }
}

/// Each machine's acceptor state for the followed journal, as its `Inspect`
/// answered after the last step: a read only.
function acceptorTable(place) {
  const ballot = (b) => (b ? `${b.round}.${b.node}` : "–");
  const num = (v) => (v === null || v === undefined ? "–" : String(v));
  const table = el("table", { className: "acceptors" });
  table.append(el("tr", {}, ...["", "promised", "accepted", "chosen", "floor"].map((h) => el("th", { textContent: h }))));
  for (const a of place.acceptors) {
    const last = a.tail.length ? a.tail[a.tail.length - 1] : null;
    const accepted = a.accepted ? `${a.accepted}${last ? ` (s${last[0]} @${last[1].round})` : ""}` : "0";
    const row = el("tr", { className: a.member ? "" : "out",
      title: a.member
        ? `m${a.machine}: promised ballot ${ballot(a.promised)}, holds ${a.accepted} accepted records, vote watermark ${num(a.watermark)}. Last accepted: ${a.tail.map(([s, b]) => `s${s}@${b.round}`).join(" ") || "none"}.`
        : `m${a.machine} serves this journal but is not an acceptor of the configuration in force.` },
      el("td", { textContent: `m${a.machine}${a.machine === place.leader ? " L" : ""}` }),
      el("td", { textContent: ballot(a.promised) }),
      el("td", { textContent: accepted }),
      el("td", { textContent: num(a.chosen) }),
      el("td", { textContent: String(a.floor) }));
    table.append(row);
  }
  const wrap = el("div", {}, el("p", { className: "muted", textContent:
    "Acceptor state after the last step, from each machine's Inspect: the ballot it promised (round.node), the records it accepted (the last slot @ its round), the highest slot it knows chosen, and its floor." }), table);
  return wrap;
}

/// The Paxos flavour a message or an event belongs to.
export function role(ix, m, t) {
  if (m.name) {
    if (m.name.startsWith("cell_init") || m.name === "cell_initialized") return "single-decree Paxos";
    if (m.name.startsWith("matchmaking")) return "Matchmaker Paxos";
    if (m.name === "msg_sent" || m.name === "leader_elected" || m.name === "value_chosen") return "Multi-Paxos";
    return null;
  }
  const ti = topoAt(ix, t);
  const place = ti >= 0 ? placeOf(ix.topos[ti], m.key) : null;
  if (PHASE1.has(m.kind) && m.kind !== "catchup_request" && m.kind !== "catchup_response" && place?.matchmakers.length) {
    return "Matchmaker Paxos, Phase 1";
  }
  return "Multi-Paxos";
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
    ctx.fillStyle = color; ctx.strokeStyle = getComputedStyle(document.body).color; ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.arc(x, y, 7, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
  } else {
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(x, y, 3, 0, Math.PI * 2); ctx.fill();
  }
  ctx.globalAlpha = 1;
}

/// The legend: one swatch per tenant, one glyph per message family.
export function legend(ix) {
  const items = [];
  for (const t of ix.tenants.values()) {
    const s = el("span", { className: "key" }, el("span", { className: "swatch" }), t.internal ? `${t.label} (internal)` : t.label);
    s.style.setProperty("--tenant", t.color);
    items.push(s);
  }
  items.push(el("span", { className: "key", textContent: "◇ Phase 1 · ● Phase 2 · ■ chosen · ▲ read · · heartbeat · ◯ client call" }));
  return items;
}

/// The timeline: messages per 100 ms stacked by tenant, the acts, the steps.
export function drawTimeline(canvas, ix, t) {
  const ratio = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  if (canvas.width !== Math.round(w * ratio)) { canvas.width = Math.round(w * ratio); canvas.height = Math.round(h * ratio); }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, w, h);
  const x = (ms) => (ms / ix.end) * w;
  const styles = getComputedStyle(document.body);
  // Acts as bands.
  ix.acts.forEach((act, i) => {
    if (act.start === null) return;
    const next = ix.acts.slice(i + 1).find((a) => a.start !== null);
    if (!(i % 2)) return;
    ctx.fillStyle = paint("var(--panel)");
    ctx.fillRect(x(act.start), 0, x(next ? next.start : ix.end) - x(act.start), h);
  });
  // Histogram.
  const order = [...ix.tenants.values()].sort((a, b) => Number(b.internal) - Number(a.internal));
  const bins = ix.hist.values().next().value?.length ?? 0;
  let max = 1;
  for (let b = 0; b < bins; b++) {
    let s = 0;
    for (const arr of ix.hist.values()) s += arr[b];
    max = Math.max(max, s);
  }
  const bw = Math.max(1, w / bins);
  for (let b = 0; b < bins; b++) {
    let y = h - 12;
    for (const tenant of order) {
      const arr = ix.hist.get(tenant.name);
      if (!arr || !arr[b]) continue;
      const bh = (arr[b] / max) * (h - 22);
      ctx.fillStyle = paint(tenant.color);
      ctx.fillRect(x(b * ix.bin), y - bh, bw, bh);
      y -= bh;
    }
  }
  // Steps.
  ctx.fillStyle = styles.getPropertyValue("--muted");
  ctx.font = `10px ${paint("var(--font-mono)")}`;
  for (const s of ix.run.steps) {
    ctx.fillRect(x(s.start_ms), h - 12, 1, 12);
    const a = ix.run.actions[s.index];
    if (a) ctx.fillText(a.op.kind.replace("create_", ""), x(s.start_ms) + 2, h - 2);
  }
  // Now.
  ctx.fillStyle = styles.getPropertyValue("--accent");
  ctx.fillRect(x(t) - 1, 0, 2, h);
}

/// Why a message is sent, in one sentence a newcomer can read.
export function explain(ix, m, t) {
  const j = ix.journals.get(m.key);
  const name = j ? j.label : "a journal";
  const ti = topoAt(ix, t);
  const place = ti >= 0 ? placeOf(ix.topos[ti], m.key) : null;
  const q = place?.quorum;
  const n = place?.members.length ?? 0;
  const slot = m.slot;
  const flavour = role(ix, m, t);
  const head = `[${flavour}] ${m.kind} from m${m.from} to m${m.to}, ${name}${slot !== null ? `, slot ${slot}` : ""}${m.round ? `, ballot ${m.round}` : ""}. `;
  const why = {
    prepare: flavour.startsWith("Matchmaker")
      ? `Phase 1: the candidate registered ballot ${m.round} with a matchmaker quorum first. Now it asks the acceptors of every configuration still in force to promise it and to tell what they accepted.`
      : `Phase 1: a candidate asks the acceptors to promise ballot ${m.round} and to tell what they accepted.`,
    promise: `Phase 1: m${m.from} promises ballot ${m.round}. With a Phase-1 quorum of promises, the candidate leads.`,
    accept: q?.system === "grid"
      ? `Phase 2: the leader asks column ${slot % q.cols} (slot ${slot} mod ${q.cols}) to accept the record. Only that column decides it.`
      : `Phase 2: the leader asks every acceptor to accept the record. Any ${Math.floor(n / 2) + 1} of ${n} decide.`,
    accepted: `Phase 2: m${m.from} accepted slot ${slot}. The leader counts it toward a Phase-2 quorum.`,
    commit: `Slot ${slot} is chosen. The leader tells m${m.to}, so it can apply the record.`,
    heartbeat: "The leader's heartbeat: it is still in charge, so nobody campaigns.",
    heartbeat_ack: "An acceptor answers the heartbeat: the leader still has a quorum.",
    pre_read: "A read asks a Phase-1 quorum (a grid row) how far the journal is chosen.",
    pre_read_ack: `m${m.from} answers the read with what it accepted.`,
    catchup_request: `m${m.from} asks for chosen slots it does not hold yet.`,
    catchup_response: `m${m.from} sends chosen slots that m${m.to} missed.`,
    nack: `m${m.from} refuses: it promised a higher ballot.`,
  }[m.kind] ?? "";
  return head + why;
}

export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  Object.assign(node, attrs);
  node.append(...children);
  return node;
}

export { lowerBound, BEATS };
