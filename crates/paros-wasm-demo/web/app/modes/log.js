// modes/log.js — the catch-up, compaction & snapshot renderer (#31).
//
// Teaches how the log stays bounded and how a laggard converges: per-node log
// columns where a *hole* (a decided slot this node missed) fills via commit-
// replay catch-up; a decided Truncate raises the cluster-wide floor and the
// prefix collapses into a snapshot block; a node that fell below the floor is
// rescued by an opaque application snapshot plus the log tail. Frames step
// through the catch-up / compaction / install events of one seeded run.

import { C, PHASE, valueColor } from '../tokens.js';
import { el, badgeSnapshot, particle, arcPath, arcPoint, ease } from '../svg.js';
import { clusterOf, leaderAt, committedAt, logAt, logMax, floorAt, holesAt, nodesAt } from '../adapter.js';

// Cap the stepped catch-up frames: keep the largest replays, chronological.
const MAX_CATCHUP_FRAMES = 10;

function buildFrames(run) {
  const events = [];
  for (const c of run.compactions || []) events.push({ t: c.time_ms, type: 'compact', node: c.node, first: c.first });
  for (const s of run.snapshots || []) events.push({ t: s.time_ms, type: 'snapshot', node: s.node, chosen_index: s.chosen_index, first: s.first });
  let replays = (run.catchups || []).filter((c) => c.kind === 'response' && c.count > 0);
  if (replays.length > MAX_CATCHUP_FRAMES) {
    replays = [...replays].sort((a, b) => b.count - a.count).slice(0, MAX_CATCHUP_FRAMES)
      .sort((a, b) => a.time_ms - b.time_ms);
  }
  for (const c of replays) events.push({ t: c.time_ms, type: 'catchup', from: c.from, to: c.to, first: c.first, last: c.last, count: c.count });
  events.sort((a, b) => a.t - b.t);

  // The intro frame sits just before the first repair/compaction event, so
  // the opening view shows the log the run has built up by then (an intro at
  // t=0 is an empty stage — nothing has been chosen yet).
  const introT = events.length ? Math.max(0, events[0].t - 1) : run.sim_duration_ms;
  const frames = [{ type: 'intro', timeMs: introT }];
  for (const e of events) frames.push({ ...e, timeMs: e.t });
  frames.push({ type: 'converged', timeMs: run.sim_duration_ms });
  frames.forEach((f, i) => { f.index = i; f.__snap = nodesAt(run, f.timeMs); });
  return frames;
}

function layout(run, dims, lmax) {
  const { w, h, mobile } = dims;
  const n = clusterOf(run);
  const cols = Array.from({ length: n }, (_, i) => w * (i + 1) / (n + 1));
  const headerY = h * (mobile ? 0.09 : 0.11);
  const logTop = h * (mobile ? 0.2 : 0.24);
  const cellH = Math.max(5, Math.min(22, (h - logTop - h * 0.06) / (lmax + 1)));
  const cellW = Math.min(w * 0.8 / n - 14, 130);
  return { cols, headerY, logTop, cellH, cellW, r: mobile ? 14 : 18 };
}

// Node i's snapshot-install high-water at T (-1 = never installed one).
function installedAt(run, T, i) {
  let hi = -1;
  for (const s of run.snapshots || []) if (s.node === i && s.time_ms <= T) hi = Math.max(hi, s.chosen_index);
  return hi;
}

export const log = {
  id: 'log',
  label: 'catch-up & snapshots',
  transport: 'step',
  seeds: [
    { value: 0, caption: 'holes filled by commit replay' },
    { value: 2, caption: 'a laggard saved by a snapshot' },
  ],
  seedCaption(seed) {
    const s = this.seeds.find((x) => x.value === Number(seed));
    return s ? s.caption : 'lesson';
  },
  stageSize(mobile) {
    return mobile ? { w: 640, h: 620, scrollW: 640 } : { w: 1000, h: 560 };
  },

  frames: buildFrames,

  render(frame, ctx) {
    const g = ctx.stage;
    const run = ctx.run;
    const T = frame.timeMs;
    const lmax = logMax(run);
    const geo = layout(run, ctx.dims, lmax);
    const lead = leaderAt(run, T);
    const logm = logAt(run, T);
    const committed = committedAt(run, T);
    const n = clusterOf(run);

    // slot gutter
    const step = lmax > 24 ? 4 : (lmax > 12 ? 2 : 1);
    for (let s = 0; s <= lmax; s += step) {
      g.appendChild(el('text', { x: geo.cols[0] - geo.cellW / 2 - 8, y: geo.logTop + s * geo.cellH + geo.cellH * 0.75, 'text-anchor': 'end', class: 'mono', 'font-size': 9, fill: C.faint }, `${s}`));
    }

    geo.cols.forEach((cx, i) => {
      const isLeader = lead && lead.node === i;
      const floor = floorAt(run, T, i);
      const installed = installedAt(run, T, i);
      const holes = new Set(holesAt(run, T, i));
      const x = cx - geo.cellW / 2;

      // the compacted prefix collapses into one block (snapshot vs truncated)
      if (floor > 0) {
        const y0 = geo.logTop, y1 = geo.logTop + floor * geo.cellH;
        const isSnap = installed >= 0;
        g.appendChild(el('rect', {
          x, y: y0 + 1, width: geo.cellW, height: y1 - y0 - 3, rx: 4,
          fill: C.inset, stroke: isSnap ? PHASE.promise : C.softline, 'stroke-width': 1.2,
          'stroke-dasharray': isSnap ? null : '4 3',
        }));
        if (y1 - y0 > 22) {
          if (isSnap) g.appendChild(badgeSnapshot(cx - (geo.cellW > 70 ? 32 : 0), (y0 + y1) / 2, PHASE.promise));
          g.appendChild(el('text', { x: cx + (isSnap && geo.cellW > 70 ? 6 : 0), y: (y0 + y1) / 2 + 3, 'text-anchor': 'middle', class: 'mono', 'font-size': 9, fill: isSnap ? PHASE.promise : C.muted }, isSnap ? 'snapshot' : 'truncated'));
        }
        // the truncation-point marker
        g.appendChild(el('line', { x1: x - 4, y1, x2: x + geo.cellW + 4, y2: y1, stroke: PHASE.accept, 'stroke-width': 1.4 }));
        g.appendChild(el('text', { x: x + geo.cellW + 6, y: y1 + 3, class: 'mono', 'font-size': 8, fill: PHASE.accept }, `floor ${floor}`));
      }

      // retained slots: chosen fills, holes as dashed red outlines
      for (let s = floor; s <= lmax; s++) {
        const y = geo.logTop + s * geo.cellH;
        const vh = logm[i].get(s);
        const filled = vh !== undefined || s <= installed;
        if (filled) {
          g.appendChild(el('rect', {
            x, y: y + 1, width: geo.cellW, height: geo.cellH - 2, rx: 2,
            fill: vh !== undefined ? valueColor(vh) : PHASE.promise,
            'fill-opacity': vh !== undefined ? 0.92 : 0.35,
            stroke: C.stage, 'stroke-width': 1,
          }));
        } else if (holes.has(s)) {
          g.appendChild(el('rect', {
            x, y: y + 1, width: geo.cellW, height: geo.cellH - 2, rx: 2,
            fill: 'none', stroke: PHASE.nack, 'stroke-width': 1, 'stroke-dasharray': '3 2', opacity: 0.75,
          }));
        }
        if (s <= committed[i]) g.appendChild(el('rect', { x: x - 3, y: y + 1, width: 3, height: geo.cellH - 2, fill: PHASE.chosen }));
      }

      // catch-up fill highlight on the receiving laggard
      if (frame.type === 'catchup' && frame.to === i && frame.first !== undefined) {
        const y0 = geo.logTop + frame.first * geo.cellH;
        const y1 = geo.logTop + (frame.last + 1) * geo.cellH;
        g.appendChild(el('rect', { x: x - 2, y: y0, width: geo.cellW + 4, height: y1 - y0, rx: 3, fill: 'none', stroke: PHASE.chosen, 'stroke-width': 1.6, opacity: 0.9 }));
      }
      if (frame.type === 'snapshot' && frame.node === i) {
        const y1 = geo.logTop + Math.max(1, frame.first) * geo.cellH;
        g.appendChild(el('rect', { x: x - 2, y: geo.logTop - 1, width: geo.cellW + 4, height: y1 - geo.logTop + 1, rx: 4, fill: 'none', stroke: PHASE.promise, 'stroke-width': 1.8, opacity: 0.95 }));
      }
      if (frame.type === 'compact' && frame.node === i) {
        const y = geo.logTop + frame.first * geo.cellH;
        g.appendChild(el('line', { x1: x - 6, y1: y, x2: x + geo.cellW + 6, y2: y, stroke: PHASE.accept, 'stroke-width': 2.4 }));
      }

      // header node
      const active = (frame.type === 'catchup' && (frame.from === i || frame.to === i))
        || ((frame.type === 'snapshot' || frame.type === 'compact') && frame.node === i);
      const color = frame.type === 'compact' ? PHASE.accept : (frame.type === 'snapshot' ? PHASE.promise : PHASE.chosen);
      const disc = nodeDiscClickable(cx, geo.headerY, geo.r, `N${i}`, isLeader, active, color, i, ctx);
      g.appendChild(disc);
      g.appendChild(el('text', { x: cx, y: geo.headerY + geo.r + 13, 'text-anchor': 'middle', class: 'mono', 'font-size': 9, fill: C.muted }, `applied ${committed[i] >= 0 ? committed[i] : '—'}`));
      if (ctx.selection === i) g.appendChild(el('circle', { cx, cy: geo.headerY, r: geo.r + 5, fill: 'none', stroke: C.text, 'stroke-width': 1, 'stroke-dasharray': '2 3', opacity: 0.7 }));
    });

    // the moving leg: catch-up replay or snapshot install
    const pg = el('g', {});
    g.appendChild(pg);
    let legA = null, legB = null, legColor = PHASE.chosen;
    if (frame.type === 'catchup' && frame.from < n && frame.to < n) {
      legA = { x: geo.cols[frame.from], y: geo.headerY };
      legB = { x: geo.cols[frame.to], y: geo.headerY };
    } else if (frame.type === 'snapshot' && lead && lead.node < n && frame.node < n && lead.node !== frame.node) {
      legA = { x: geo.cols[lead.node], y: geo.headerY };
      legB = { x: geo.cols[frame.node], y: geo.headerY };
      legColor = PHASE.promise;
    }
    if (legA) g.appendChild(el('path', { d: arcPath(legA, legB), fill: 'none', stroke: legColor, 'stroke-width': 1.2, opacity: 0.28 }));
    return {
      tick(t) {
        while (pg.firstChild) pg.removeChild(pg.firstChild);
        if (!legA) return;
        const p = arcPoint(legA, legB, ease(t));
        pg.appendChild(particle(p.x, p.y, legColor));
      },
    };
  },

  narrate(frame) {
    if (frame.type === 'catchup') {
      const span = frame.last > frame.first ? `slots ${frame.first}–${frame.last}` : `slot ${frame.first}`;
      return { phase: 'chosen', color: PHASE.chosen, title: 'Commit-replay catch-up',
        lines: [`N${frame.to} was missing decided slots — a hole a missed Accept + Commit leaves. It asked a peer for everything from its prefix up.`,
          `N${frame.from} replays ${span} (${frame.count} decided ${frame.count === 1 ? 'entry' : 'entries'}, already chosen, so learning them directly is safe). The hole fills; no re-vote needed.`] };
    }
    if (frame.type === 'compact') {
      return { phase: 'accept', color: PHASE.accept, title: `Log truncated at slot ${frame.first}`,
        lines: [`A Truncate control command was *decided by consensus* like any other slot, and N${frame.node} now applies it: everything below slot ${frame.first} is durably dropped.`,
          'One cluster-wide floor, forwarded by normal replication — no node prunes on its own. The log stays bounded.'] };
    }
    if (frame.type === 'snapshot') {
      return { phase: 'promise', color: PHASE.promise, title: 'Snapshot install',
        lines: [`N${frame.node} came back below the cluster's truncation floor: the slots it needs were deleted everywhere, so catch-up cannot replay them.`,
          `A peer ships its opaque application snapshot at slot ${frame.chosen_index}. N${frame.node} jumps its prefix there, keeps its durable promise, and the log tail arrives via ordinary catch-up.`] };
    }
    if (frame.type === 'converged') {
      return { phase: 'chosen', color: PHASE.chosen, title: 'Converged',
        lines: ['Every live node holds the same bounded log: one shared floor, snapshot blocks below it, no holes above it.',
          'Step back through the frames to see each hole fill and the floor rise.'] };
    }
    return { phase: 'idle', color: C.neutralRing, title: 'Catch-up, compaction & snapshots',
      lines: ['Each column is one node\'s log. Watch holes (dashed red) fill via commit replay, the floor rise as Truncate commands apply, and a laggard recover through a snapshot.',
        'Step forward — each frame is one recorded repair or compaction event from this seed.'] };
  },

  digest(run) {
    const replayed = (run.catchups || []).filter((c) => c.kind === 'response').reduce((a, c) => a + c.count, 0);
    const floors = new Set((run.compactions || []).map((c) => c.first));
    const maxFloor = Math.max(0, ...(run.compactions || []).map((c) => c.first), ...(run.snapshots || []).map((s) => s.first));
    const holesLeft = Array.from({ length: clusterOf(run) }, (_, i) => holesAt(run, run.sim_duration_ms, i).length).reduce((a, b) => a + b, 0);
    return [
      { label: 'entries replayed by catch-up', value: String(replayed) },
      { label: 'truncations applied', value: `${(run.compactions || []).length} (${floors.size} floor${floors.size === 1 ? '' : 's'})` },
      { label: 'final floor', value: String(maxFloor) },
      { label: 'snapshot installs', value: String((run.snapshots || []).length) },
      { label: 'holes at end of run', value: String(holesLeft) },
      { label: 'log committed', value: `${new Set(run.chosen.map((c) => c.slot)).size} slots` },
    ];
  },

  progress(run, frame) {
    const lead = leaderAt(run, frame.timeMs);
    const committed = committedAt(run, frame.timeMs);
    return { type: 'committed', leader: lead ? lead.node : null, round: lead ? lead.round : null, committed: Math.max(-1, ...committed), high: logMax(run) };
  },

  inspect(frame, selection) {
    if (selection === null || selection === undefined || !frame.__snap) return [];
    const s = frame.__snap[selection];
    const rows = [];
    rows.title = `node · N${selection}`;
    rows.push({ group: 'log', k: 'applied index', v: s.committed >= 0 ? String(s.committed) : '— none —' });
    rows.push({ group: 'log', k: 'role', v: s.isLeader ? `leader (r${s.leaderRound})` : 'follower' });
    return rows;
  },
};

function nodeDiscClickable(cx, cy, r, label, isLeader, active, color, id, ctx) {
  const g = el('g', {});
  if (active) g.appendChild(el('circle', { cx, cy, r: r + 7, fill: color, opacity: 0.14, filter: 'url(#paros-blur)' }));
  g.appendChild(el('circle', { cx, cy, r, fill: C.nodeFill, stroke: isLeader ? PHASE.leader : (active ? color : C.neutralRing), 'stroke-width': 2 }));
  g.appendChild(el('text', { x: cx, y: cy, 'text-anchor': 'middle', 'dominant-baseline': 'central', class: 'mono', 'font-size': r * 0.66, 'font-weight': 700, fill: C.text }, label));
  if (isLeader) g.appendChild(el('text', { x: cx, y: cy - r - 8, 'text-anchor': 'middle', class: 'mono', 'font-size': 9, fill: PHASE.leader }, 'LEADER'));
  g.style.cursor = 'pointer';
  g.setAttribute('tabindex', '0');
  g.setAttribute('role', 'button');
  g.addEventListener('click', () => ctx.onSelect(id));
  g.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); ctx.onSelect(id); } });
  return g;
}
