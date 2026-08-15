// modes/multi.js — the Multi-Paxos renderer.
//
// Teaches leader election + replicated log: a stable leader runs Phase 1 once per
// ballot, then streams Phase 2 (Accept) per log slot. Stage = node columns × slot
// cells; same colour across nodes = agreement; a green edge marks the committed
// prefix. Election messages are prepare-blue, the steady Phase-2 stream is
// accept-amber — a visible teaching cue for the two regimes, backed by chips.
//
// It also teaches **linearizable reads**, because a read is an index into that
// same log: a read lane on the right shares the log's rows, and a read marker
// waits at its captured index — the commit barrier — until a heartbeat-ack quorum
// confirms the leader and the applied prefix covers the index. A round whose
// leader is deposed first is abandoned, and the read completes under the next
// one. Read violet is used for nothing else on the stage.

import { C, PHASE, valueColor } from '../tokens.js';
import { el, nodeDisc, particle, arcPath, arcPoint, ease, badgeLeader, badgeDrop, badgeRead } from '../svg.js';
import {
  nodesAt, leaderAt, committedAt, logAt, logMax, stateAt,
  idxNum, isAbandoned, crossedLeaders, readStateAt, liveRoundAt, readWaitMs, readWaitsAtBarrier,
} from '../adapter.js';

// Event ordering inside one millisecond: an election opens a step, then the slot
// it enables, then the read that observes it. Keeps a same-ms trio readable.
const TYPE_ORDER = { election: 0, slot: 1, read_wait: 2, read_end: 3 };

function buildFrames(run) {
  const events = [];
  for (const l of run.leaders) events.push({ t: l.time_ms, type: 'election', node: l.node, round: l.round });
  // first chosen instant per slot
  const bySlot = new Map();
  for (const c of run.chosen) {
    if (!bySlot.has(c.slot) || c.time_ms < bySlot.get(c.slot).t) bySlot.set(c.slot, { t: c.time_ms, vhash: c.vhash });
  }
  for (const [slot, v] of bySlot) events.push({ t: v.t, type: 'slot', slot, vhash: v.vhash });
  // Reads: every read gets a closing step (served, or failed). A read that had to
  // *work* for its answer — a round abandoned under a leader change, a redirect,
  // more than one node asked — also gets an opening step at its first capture, so
  // the reader can see it parked at the barrier before it completes.
  for (const r of run.reads || []) {
    const first = r.rounds[0];
    if (first && (crossedLeaders(r) || readWaitsAtBarrier(run, r))) {
      events.push({ t: first.captured_ms, type: 'read_wait', read: r });
    }
    const end = r.served_ms !== null && r.served_ms !== undefined
      ? r.served_ms
      : Math.max(r.issued_ms, ...r.redirects.map((x) => x.time_ms), ...r.rounds.map((x) => x.captured_ms));
    events.push({ t: end, type: 'read_end', read: r });
  }
  events.sort((a, b) => a.t - b.t || TYPE_ORDER[a.type] - TYPE_ORDER[b.type]);

  // which ballot each accept-slot belongs to, to flag piggybacked Phase-2 slots
  const firstAcceptSlotOfBallot = new Map();
  for (const p of run.protocol) if (p.kind === 'accept') {
    const k = `${p.bround}:${p.bnode}`;
    firstAcceptSlotOfBallot.set(k, Math.min(firstAcceptSlotOfBallot.has(k) ? firstAcceptSlotOfBallot.get(k) : Infinity, p.slot));
  }

  const frames = events.map((e) => {
    if (e.type === 'read_wait' || e.type === 'read_end') {
      // A read step draws no protocol legs of its own: the messages it depends on
      // (the heartbeat round) are not in the protocol stream, and the point of the
      // step is the *wait*, not a packet in flight.
      const served = e.read.served_ms !== null && e.read.served_ms !== undefined;
      return {
        phase: 'read',
        kind: e.type === 'read_wait' ? 'wait' : (served ? 'serve' : 'fail'),
        timeMs: e.t, read: e.read, legs: [],
      };
    }
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
  // The three node columns sit left of a dedicated read lane on the right: reads
  // are indexed by *slot*, so the lane shares the log's rows and a read marker
  // lines up with the slot it observes.
  const cols = [w * 0.19, w * 0.43, w * 0.67];
  const laneX = w * 0.88;
  const headerY = h * (mobile ? 0.10 : 0.13);
  const logTop = h * (mobile ? 0.24 : 0.30);
  const cellH = Math.max(11, Math.min(26, (h - logTop - h * 0.05) / (lmax + 1)));
  const cellW = Math.min(w * 0.20, 140);
  return { cols, laneX, headerY, logTop, cellH, cellW, r: mobile ? 16 : 20 };
}

// The y of a read index inside the log: the centre of that slot's row. The empty
// applied prefix (index null → -1) sits just above slot 0.
function rowY(geo, idx) {
  return geo.logTop + (idxNum(idx) + 0.5) * geo.cellH;
}

// The read lane: the barrier the focus read waits at, its marker, and a quiet
// trail of the reads already answered (each parked at the watermark it observed,
// so the trail climbs the log exactly as the committed prefix grows).
function drawReadLane(g, geo, run, frame, T) {
  const reads = run.reads || [];
  if (!reads.length) return;
  const focus = frame.phase === 'read' ? frame.read : null;
  const lead = leaderAt(run, T);
  const committed = committedAt(run, T);

  g.appendChild(el('text', {
    x: geo.laneX, y: geo.headerY - geo.r - 8, 'text-anchor': 'middle', class: 'mono',
    'font-size': 10, fill: PHASE.read,
  }, 'READS'));
  // the lane's spine, so the markers read as one column
  g.appendChild(el('line', {
    x1: geo.laneX, y1: geo.logTop, x2: geo.laneX, y2: geo.logTop + (logMax(run) + 1) * geo.cellH,
    stroke: PHASE.read, 'stroke-width': 1, opacity: 0.16,
  }));

  // trail: every read already answered, at the watermark it observed
  for (const r of reads) {
    if (r === focus || readStateAt(r, T) !== 'served') continue;
    g.appendChild(el('circle', {
      cx: geo.laneX, cy: rowY(geo, r.read_index), r: 2.4, fill: PHASE.read, opacity: 0.34,
    }));
  }

  if (!focus) return;
  const round = liveRoundAt(focus, T);
  // The frame knows the read's terminal outcome; only ask the time-based
  // derivation when it does not (a round can confirm long after the client's
  // deadline expired, which would otherwise read as "still waiting").
  const state = frame.kind === 'fail' ? 'failed' : readStateAt(focus, T);
  const idx = round ? round.index : focus.read_index;
  const y = rowY(geo, idx);
  const waiting = state === 'waiting' || state === 'asking';
  const barrierColor = state === 'failed' ? PHASE.nack : waiting ? PHASE.read : PHASE.chosen;

  // The commit barrier: the read may not be answered from anything above this
  // line until the applied prefix has crossed it.
  const bx0 = geo.cols[0] - geo.cellW / 2 - 4;
  g.appendChild(el('line', {
    x1: bx0, y1: y, x2: geo.laneX + 14, y2: y,
    stroke: barrierColor, 'stroke-width': 1.2,
    'stroke-dasharray': state === 'served' ? '1 0' : '4 4', opacity: waiting ? 0.85 : 0.55,
  }));
  g.appendChild(el('text', {
    x: geo.laneX - 14, y: y - 6, 'text-anchor': 'end', class: 'mono', 'font-size': 10,
    fill: barrierColor,
  }, idxNum(idx) < 0 ? 'read index · empty prefix' : `read index ${idxNum(idx)}`));

  // The marker itself, and — while it waits — the gap it is waiting on: the
  // leader's applied prefix has not yet reached the captured index.
  const owner = round ? round.node : (lead ? lead.node : null);
  if (waiting && owner !== null && idxNum(idx) > committed[owner]) {
    const from = rowY(geo, committed[owner]);
    g.appendChild(el('rect', {
      x: geo.cols[owner] - geo.cellW / 2, y: Math.min(from, y), width: geo.cellW,
      height: Math.abs(y - from), fill: PHASE.read, opacity: 0.12,
    }));
    g.appendChild(el('text', {
      x: geo.cols[owner], y: (from + y) / 2 + 3, 'text-anchor': 'middle', class: 'mono',
      'font-size': 9, fill: PHASE.read,
    }, `applied ${committed[owner]} < ${idxNum(idx)}`));
  }
  // Where the answer landed. A served read reports the watermark at *serve*
  // time, which can be past the index it pinned: the barrier is a floor on
  // freshness, not a ceiling. When the two differ, the marker sits at what the
  // client observed and a connector runs back down to the barrier.
  const markerIdx = state === 'served' ? focus.read_index : idx;
  const my = rowY(geo, markerIdx);
  if (my !== y) {
    g.appendChild(el('line', {
      x1: geo.laneX, y1: y, x2: geo.laneX, y2: my,
      stroke: PHASE.chosen, 'stroke-width': 1.4, opacity: 0.5,
    }));
  }
  g.appendChild(badgeRead(geo.laneX, my, state === 'failed' ? PHASE.nack : PHASE.read, {
    filled: state === 'served', r: 6,
  }));
  const tag = state === 'served' ? `R${focus.seq} ✓ ${idxNum(focus.read_index) < 0 ? '—' : idxNum(focus.read_index)}`
    : state === 'failed' ? `R${focus.seq} ✗` : `R${focus.seq} waiting`;
  g.appendChild(el('text', {
    x: geo.laneX, y: my + 20, 'text-anchor': 'middle', class: 'mono', 'font-size': 10,
    fill: state === 'failed' ? PHASE.nack : PHASE.read,
  }, tag));
  // the node holding (or that held) the round, so the wait has an owner on screen
  if (round) {
    g.appendChild(el('line', {
      x1: geo.cols[round.node], y1: geo.headerY + geo.r + 4, x2: geo.laneX, y2: my - 10,
      stroke: isAbandoned(round) ? PHASE.nack : PHASE.read, 'stroke-width': 1,
      'stroke-dasharray': '3 3', opacity: 0.45,
    }));
  }
}

export const multi = {
  id: 'multi',
  label: 'multi-paxos',
  transport: 'step',
  seeds: [
    { value: 0, caption: 'a stable leader' },
    { value: 7, caption: 'a leader failover' },
    { value: 19, caption: 'a read waits out a leader change' },
  ],
  seedCaption(seed) {
    const s = this.seeds.find((x) => x.value === Number(seed));
    return s ? s.caption : 'lesson';
  },
  stageSize(mobile) {
    return mobile ? { w: 700, h: 560, scrollW: 700 } : { w: 1000, h: 500 };
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
    const activeColor = frame.phase === 'election' ? PHASE.prepare
      : frame.phase === 'read' ? PHASE.read : PHASE.accept;

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

    // the read lane, drawn over the log so the barrier line crosses it
    drawReadLane(g, geo, run, frame, frame.timeMs);

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
    if (frame.phase === 'read') return narrateRead(frame);
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
    const hops = leaders.map((l) => `N${l.node}@r${l.round}`);
    const trail = hops.length > 6
      ? `${hops.slice(0, 2).join(' → ')} → … → ${hops.slice(-2).join(' → ')} (${hops.length} in all)`
      : hops.join(' → ');
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

    const reads = run.reads || [];
    if (reads.length) {
      const served = reads.filter((r) => r.served_ms !== null && r.served_ms !== undefined);
      const waits = served.map(readWaitMs).filter((w) => w !== null);
      const crossed = reads.filter(crossedLeaders).length;
      chips.push({ label: 'reads served', value: `${served.length} / ${reads.length}` });
      chips.push({
        label: 'longest read wait',
        value: waits.length ? `${Math.max(...waits)} ms` : '—',
      });
      chips.push({ label: 'reads that changed hands', value: String(crossed) });
    }
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
    if (frame.phase === 'read' && frame.read) {
      const mine = frame.read.rounds.filter((q) => q.node === selection);
      rows.push({ group: 'read', k: `read ${frame.read.seq} rounds here`, v: String(mine.length) });
      for (const q of mine) {
        rows.push({
          group: 'read',
          k: `ctx ${q.ctx} · index ${idxNum(q.index) < 0 ? '—' : idxNum(q.index)}`,
          v: isAbandoned(q) ? 'abandoned (deposed)' : `confirmed +${q.confirmed_ms - q.captured_ms} ms`,
        });
      }
    }
    rows.push({ group: 'log', k: 'note', v: 'per-slot acceptor state not in contract' });
    return rows;
  },
};

// The read narration — the whole lesson of the page in three lines: an index is
// pinned, a quorum round proves the pinner still leads, and only then, once the
// applied prefix covers the index, is the read answered.
function narrateRead(frame) {
  const r = frame.read;
  const first = r.rounds[0];
  const idx = (i) => (idxNum(i) < 0 ? 'the empty prefix' : `slot ${idxNum(i)}`);
  if (frame.kind === 'wait') {
    return {
      phase: 'read', color: PHASE.read, title: `Read ${r.seq} pins its index`,
      lines: [
        `N${first.node} captured read index ${idx(first.index)} and parked the reply — it may not answer from local state yet.`,
        'Two things must land first: a heartbeat-ack quorum at its ballot (proving it still leads, no log write) and an applied prefix that covers the index.',
      ],
    };
  }
  if (frame.kind === 'fail') {
    return {
      phase: 'read', color: PHASE.nack, title: `Read ${r.seq} timed out`,
      lines: [
        `No node confirmed a read index inside the client's deadline (${r.redirects.length} redirect${r.redirects.length === 1 ? '' : 's'}).`,
        'A read that cannot prove freshness is refused, never answered from a stale belief — an unanswered read constrains nothing.',
      ],
    };
  }
  const wait = readWaitMs(r);
  const abandoned = r.rounds.filter(isAbandoned);
  const l2 = abandoned.length
    ? `N${abandoned[0].node}'s round was abandoned — it lost the ballot before its quorum landed, so the reply was released and the read completed under the next leader.`
    : r.attempts > 1
      ? `The client asked ${r.attempts} nodes: a follower cannot serve a read, it can only name the leader.`
      : 'The heartbeat quorum came back and the applied prefix already covered the index, so the answer was one round trip, no log write.';
  return {
    phase: 'read', color: PHASE.chosen, title: `Read ${r.seq} served at ${idx(r.read_index)}`,
    lines: [
      `Answered ${wait === null ? '' : `${wait} ms after its index was pinned`} — every write acked before this read began is inside the prefix it observed.`,
      l2,
    ],
  };
}

function clickable(node, id, ctx) {
  node.style.cursor = 'pointer';
  node.setAttribute('tabindex', '0');
  node.setAttribute('role', 'button');
  node.addEventListener('click', () => ctx.onSelect(id));
  node.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); ctx.onSelect(id); } });
  return node;
}
