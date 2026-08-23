// modes/multi.js — the Multi-Paxos renderer.
//
// Teaches leader election + replicated log: a stable leader runs Phase 1 once per
// ballot, then streams Phase 2 (Accept) per log slot. Stage = node columns × slot
// cells; same colour across nodes = agreement; a green edge marks the committed
// prefix. Election messages are prepare-blue, the steady Phase-2 stream is
// accept-amber — a visible teaching cue for the two regimes, backed by chips.

import { C, PHASE, valueColor } from '../tokens.js';
import { el, nodeDisc, particle, arcPath, arcPoint, ease, badgeLeader, badgeDrop } from '../svg.js';
import { nodesAt, leaderAt, committedAt, logAt, logMax, stateAt } from '../adapter.js';

function buildFrames(run) {
  const events = [];
  for (const l of run.leaders) events.push({ t: l.time_ms, type: 'election', node: l.node, round: l.round });
  // first chosen instant per slot
  const bySlot = new Map();
  for (const c of run.chosen) {
    if (!bySlot.has(c.slot) || c.time_ms < bySlot.get(c.slot).t) bySlot.set(c.slot, { t: c.time_ms, vhash: c.vhash });
  }
  for (const [slot, v] of bySlot) events.push({ t: v.t, type: 'slot', slot, vhash: v.vhash });
  events.sort((a, b) => a.t - b.t || (a.type === 'election' ? -1 : 1));

  // which ballot each accept-slot belongs to, to flag piggybacked Phase-2 slots
  const firstAcceptSlotOfBallot = new Map();
  for (const p of run.protocol) if (p.kind === 'accept') {
    const k = `${p.bround}:${p.bnode}`;
    firstAcceptSlotOfBallot.set(k, Math.min(firstAcceptSlotOfBallot.has(k) ? firstAcceptSlotOfBallot.get(k) : Infinity, p.slot));
  }

  const frames = events.map((e) => {
    if (e.type === 'election') {
      const legs = run.protocol.filter((p) => (p.kind === 'prepare' || p.kind === 'promise') && p.bnode === e.node && p.bround === e.round);
      return { phase: 'election', timeMs: e.t, node: e.node, round: e.round, legs };
    }
    const legs = run.protocol.filter((p) => (p.kind === 'accept' || p.kind === 'accepted') && p.slot === e.slot);
    // is this slot the first accept of its ballot (Phase 1 just happened) or piggybacked?
    let piggyback = false;
    const acc = legs.find((p) => p.kind === 'accept');
    if (acc) piggyback = firstAcceptSlotOfBallot.get(`${acc.bround}:${acc.bnode}`) !== e.slot;
    return { phase: 'commit', timeMs: e.t, slot: e.slot, vhash: e.vhash, legs, piggyback };
  });
  frames.forEach((f, i) => { f.index = i; f.__snap = nodesAt(run, f.timeMs); });
  return frames.length ? frames : [{ index: 0, phase: 'idle', timeMs: 0, legs: [], __snap: nodesAt(run, 0) }];
}

function layout(dims, lmax) {
  const { w, h, mobile } = dims;
  const cols = [w * 0.22, w * 0.5, w * 0.78];
  const headerY = h * (mobile ? 0.10 : 0.13);
  const logTop = h * (mobile ? 0.24 : 0.30);
  const cellH = Math.max(11, Math.min(26, (h - logTop - h * 0.05) / (lmax + 1)));
  const cellW = Math.min(w * 0.22, 150);
  return { cols, headerY, logTop, cellH, cellW, r: mobile ? 16 : 20 };
}

export const multi = {
  id: 'multi',
  label: 'multi-paxos',
  transport: 'step',
  seeds: [
    { value: 0, caption: 'a stable leader' },
    { value: 7, caption: 'a leader failover' },
  ],
  seedCaption(seed) {
    const s = this.seeds.find((x) => x.value === Number(seed));
    return s ? s.caption : 'lesson';
  },
  stageSize(mobile) {
    return mobile ? { w: 620, h: 560, scrollW: 620 } : { w: 1000, h: 500 };
  },

  frames: buildFrames,

  render(frame, ctx) {
    const g = ctx.stage;
    const run = ctx.run;
    const lmax = logMax(run);
    const geo = layout(ctx.dims, lmax);
    const snap = frame.__snap;
    const lead = leaderAt(run, frame.timeMs);
    const log = logAt(run, frame.timeMs);
    const committed = committedAt(run, frame.timeMs);
    const activeColor = frame.phase === 'election' ? PHASE.prepare : PHASE.accept;

    // slot gutter
    for (let s = 0; s <= lmax; s++) {
      g.appendChild(el('text', { x: geo.cols[0] - geo.cellW / 2 - 10, y: geo.logTop + s * geo.cellH + geo.cellH * 0.7, 'text-anchor': 'end', class: 'mono', 'font-size': 10, fill: C.faint }, `slot ${s}`));
    }

    // columns
    geo.cols.forEach((cx, i) => {
      const isLeader = lead && lead.node === i;
      // log cells
      for (let s = 0; s <= lmax; s++) {
        const y = geo.logTop + s * geo.cellH;
        const x = cx - geo.cellW / 2;
        const vh = log[i].get(s);
        const filled = vh !== undefined;
        g.appendChild(el('rect', {
          x, y: y + 1, width: geo.cellW, height: geo.cellH - 3, rx: 3,
          fill: filled ? valueColor(vh) : 'none',
          'fill-opacity': filled ? 0.92 : 0,
          stroke: filled ? C.stage : C.softline, 'stroke-width': 1,
        }));
        if (s <= committed[i]) g.appendChild(el('rect', { x: x - 3, y: y + 1, width: 3, height: geo.cellH - 3, fill: PHASE.chosen }));
        if (frame.phase === 'commit' && frame.slot === s) g.appendChild(el('rect', { x: x - 1, y: y, width: geo.cellW + 2, height: geo.cellH - 1, rx: 3, fill: 'none', stroke: activeColor, 'stroke-width': 1.5, opacity: 0.8 }));
      }
      // header node
      const acting = frame.phase === 'election' ? (i === frame.node) : (lead && i === lead.node);
      const disc = nodeDisc(cx, geo.headerY, geo.r, `N${i}`, {
        ring: isLeader ? PHASE.leader : (acting ? activeColor : C.neutralRing),
        active: isLeader || acting, glowColor: isLeader ? PHASE.leader : activeColor,
      });
      clickable(disc, i, ctx);
      g.appendChild(disc);
      if (isLeader) {
        g.appendChild(badgeLeader(cx + geo.r - 2, geo.headerY - geo.r + 2));
        g.appendChild(el('text', { x: cx, y: geo.headerY - geo.r - 8, 'text-anchor': 'middle', class: 'mono', 'font-size': 10, fill: PHASE.leader }, `LEADER · r${lead.round}`));
      }
      g.appendChild(el('text', { x: cx, y: geo.headerY + geo.r + 14, 'text-anchor': 'middle', class: 'mono', 'font-size': 10, fill: C.muted }, `promised (${snap[i].promised.r},${snap[i].promised.n})`));
      if (ctx.selection === i) g.appendChild(el('circle', { cx, cy: geo.headerY, r: geo.r + 5, fill: 'none', stroke: C.text, 'stroke-width': 1, 'stroke-dasharray': '2 3', opacity: 0.7 }));
    });

    // particles between headers
    const pg = el('g', {});
    g.appendChild(pg);
    const headers = geo.cols.map((cx) => ({ x: cx, y: geo.headerY }));
    const legs = frame.legs.filter((l) => l.from < 3 && l.to < 3).map((l) => ({ leg: l, a: headers[l.from], b: headers[l.to] }));
    // static links
    for (const { a, b } of legs) g.appendChild(el('path', { d: arcPath(a, b), fill: 'none', stroke: activeColor, 'stroke-width': 1.2, opacity: 0.28 }));
    return {
      tick(t) {
        while (pg.firstChild) pg.removeChild(pg.firstChild);
        for (const { leg, a, b } of legs) {
          const dropped = leg.outcome === 'dropped';
          const reach = dropped ? 0.6 : 1;
          const p = arcPoint(a, b, reach * ease(t));
          if (dropped && t > 0.55) { pg.appendChild(badgeDrop(p.x, p.y, PHASE.nack)); continue; }
          pg.appendChild(particle(p.x, p.y, PHASE[leg.kind] || activeColor));
        }
      },
    };
  },

  narrate(frame) {
    if (frame.phase === 'election') {
      return { phase: 'leader', color: PHASE.leader, title: 'Leader election',
        lines: [`N${frame.node} times out and runs Phase 1 for ballot r${frame.round} — a Prepare across the cluster.`,
          'A majority promises and N' + frame.node + ' becomes leader. Phase 1 happens once per ballot.'] };
    }
    if (frame.phase === 'commit') {
      return { phase: 'accept', color: PHASE.accept, title: `Slot ${frame.slot} chosen`,
        lines: [`The leader runs Phase 2 (Accept) for slot ${frame.slot}; a majority accepts and the slot is chosen.`,
          frame.piggyback
            ? 'No new election — this slot piggybacks on the leader’s existing ballot (Phase 2 only). The committed prefix grows by one.'
            : 'This is the first slot under the new ballot; every later slot reuses it. The committed prefix grows by one.'] };
    }
    return { phase: 'idle', color: C.neutralRing, title: 'Multi-Paxos', lines: ['Awaiting a leader…'] };
  },

  digest(run) {
    const slots = new Set(run.chosen.map((c) => c.slot));
    const high = run.chosen.length ? Math.max(...run.chosen.map((c) => c.slot)) : -1;
    const leaders = [...run.leaders].sort((a, b) => a.time_ms - b.time_ms);
    const failovers = Math.max(0, leaders.length - 1);
    const trail = leaders.map((l) => `N${l.node}@r${l.round}`).join(' → ');
    const ph1 = new Set(run.protocol.filter((p) => p.kind === 'prepare').map((p) => `${p.bround}:${p.bnode}`)).size;
    const byBallot = new Map();
    for (const p of run.protocol) if (p.kind === 'accept') {
      const k = `${p.bround}:${p.bnode}`;
      if (!byBallot.has(k)) byBallot.set(k, new Set());
      byBallot.get(k).add(p.slot);
    }
    let piggyback = 0;
    for (const s of byBallot.values()) piggyback += Math.max(0, s.size - 1);
    const drops = run.protocol.filter((p) => p.outcome === 'dropped').length;
    const chips = [
      { label: 'log committed', value: `${slots.size} slots (high ${high})` },
      { label: 'leader failovers', value: String(failovers) },
      { label: 'phase 1 elections', value: String(ph1) },
      { label: 'phase 2 piggybacked', value: `${piggyback} slots` },
      { label: 'network drops', value: String(drops) },
    ];
    if (trail) chips.splice(2, 0, { label: 'ballot trail', value: trail });
    return chips;
  },

  progress(run, frame) {
    const lead = leaderAt(run, frame.timeMs);
    const committed = committedAt(run, frame.timeMs);
    const high = logMax(run);
    return { type: 'committed', leader: lead ? lead.node : null, round: lead ? lead.round : null, committed: Math.max(-1, ...committed), high };
  },

  inspect(frame, selection) {
    if (selection === null || selection === undefined || !frame.__snap) return [];
    const s = frame.__snap[selection];
    const rows = [];
    rows.title = `${s.isLeader ? 'leader · ' : 'follower · '}N${selection}`;
    rows.push({ group: 'role', k: 'role', v: s.isLeader ? `leader (r${s.leaderRound})` : 'follower' });
    rows.push({ group: 'durable', k: 'promised ballot', v: `(${s.promised.r},${s.promised.n})` });
    rows.push({ group: 'durable', k: 'committed index', v: s.committed >= 0 ? String(s.committed) : '— none —' });
    rows.push({ group: 'log', k: 'note', v: 'per-slot acceptor state not in contract' });
    return rows;
  },
};

function clickable(node, id, ctx) {
  node.style.cursor = 'pointer';
  node.setAttribute('tabindex', '0');
  node.setAttribute('role', 'button');
  node.addEventListener('click', () => ctx.onSelect(id));
  node.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); ctx.onSelect(id); } });
  return node;
}
