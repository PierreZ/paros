// modes/read.js — the linearizable-read renderer (#43).
//
// The deliberately lightweight twin of the multi-Paxos scene: the same node
// columns and log cells, plus a client rail on the left. One frame per client
// read: the read reaches the leader, the leader captures a *read index* and
// confirms leadership with a heartbeat-ack quorum round, and the answer leaves
// only once the applied prefix covers the read index — the commit barrier a
// read must wait at, so it can never observe a stale or rolled-back value.

import { C, PHASE, valueColor } from '../tokens.js';
import { el, particle, arcPath, arcPoint, ease, badgeDrop } from '../svg.js';
import { clusterOf, leaderAt, committedAt, logAt, logMax, nodesAt, readTimeline } from '../adapter.js';

function buildFrames(run) {
  const reads = readTimeline(run);
  // Open on the state just before the first read goes out, not the empty
  // t=0 stage.
  const introT = reads.length ? Math.max(0, reads[0].issued_ms - 1) : run.sim_duration_ms;
  const frames = [{ type: 'intro', timeMs: introT }];
  for (const r of reads) frames.push({ ...r, type: 'read', timeMs: r.done_ms });
  frames.forEach((f, i) => {
    f.index = i;
    f.__snap = nodesAt(run, f.timeMs);
    // For a read frame: was the barrier already satisfied when the read was
    // issued? If the leader's applied prefix at issue time sat below the read
    // index, the read genuinely waited.
    if (f.type === 'read' && f.read_index !== null && f.read_index !== undefined) {
      const lead = leaderAt(run, f.done_ms);
      const appliedAtIssue = lead ? committedAt(run, f.issued_ms)[lead.node] : -1;
      f.waitedAtBarrier = appliedAtIssue < f.read_index;
    }
  });
  return frames;
}

function layout(run, dims, lmax) {
  const { w, h, mobile } = dims;
  const n = clusterOf(run);
  const left = w * 0.14;
  const cols = Array.from({ length: n }, (_, i) => w * 0.3 + (w * 0.62) * (n === 1 ? 0.5 : i / Math.max(1, n - 1)));
  const headerY = h * (mobile ? 0.11 : 0.14);
  const logTop = h * (mobile ? 0.26 : 0.32);
  const cellH = Math.max(6, Math.min(24, (h - logTop - h * 0.05) / (lmax + 1)));
  const cellW = Math.min(w * 0.6 / n - 12, 120);
  return { left, cols, headerY, logTop, cellH, cellW, r: mobile ? 14 : 18 };
}

export const read = {
  id: 'read',
  label: 'reads',
  transport: 'step',
  seeds: [
    { value: 0, caption: 'reads at the commit barrier' },
    { value: 183, caption: 'a read across a leader change' },
  ],
  seedCaption(seed) {
    const s = this.seeds.find((x) => x.value === Number(seed));
    return s ? s.caption : 'lesson';
  },
  stageSize(mobile) {
    return mobile ? { w: 640, h: 560, scrollW: 640 } : { w: 1000, h: 520 };
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

    // client rail
    const clientY = geo.headerY;
    g.appendChild(el('circle', { cx: geo.left, cy: clientY, r: geo.r - 3, fill: C.nodeFill, stroke: PHASE.propose, 'stroke-width': 2 }));
    g.appendChild(el('text', { x: geo.left, y: clientY, 'text-anchor': 'middle', 'dominant-baseline': 'central', class: 'mono', 'font-size': 10, 'font-weight': 700, fill: C.text }, 'C'));
    g.appendChild(el('text', { x: geo.left, y: clientY + geo.r + 12, 'text-anchor': 'middle', class: 'mono', 'font-size': 9, fill: C.muted }, 'client'));

    // node columns + log cells
    geo.cols.forEach((cx, i) => {
      const isLeader = lead && lead.node === i;
      const x = cx - geo.cellW / 2;
      for (let s = 0; s <= lmax; s++) {
        const y = geo.logTop + s * geo.cellH;
        const vh = logm[i].get(s);
        if (vh !== undefined) {
          g.appendChild(el('rect', { x, y: y + 1, width: geo.cellW, height: geo.cellH - 2, rx: 2, fill: valueColor(vh), 'fill-opacity': 0.92, stroke: C.stage, 'stroke-width': 1 }));
        }
        if (s <= committed[i]) g.appendChild(el('rect', { x: x - 3, y: y + 1, width: 3, height: geo.cellH - 2, fill: PHASE.chosen }));
      }
      // the commit barrier on the leader column: the read index line
      if (frame.type === 'read' && isLeader && frame.read_index !== null && frame.read_index !== undefined) {
        const y = geo.logTop + (frame.read_index + 1) * geo.cellH;
        g.appendChild(el('line', { x1: x - 8, y1: y, x2: x + geo.cellW + 8, y2: y, stroke: PHASE.accept, 'stroke-width': 2, 'stroke-dasharray': '5 3' }));
        g.appendChild(el('text', { x: x + geo.cellW + 10, y: y + 3, class: 'mono', 'font-size': 9, fill: PHASE.accept }, `read index ${frame.read_index}`));
      }
      const g2 = el('g', {});
      if (isLeader) g2.appendChild(el('circle', { cx, cy: geo.headerY, r: geo.r + 7, fill: PHASE.leader, opacity: 0.12, filter: 'url(#paros-blur)' }));
      g2.appendChild(el('circle', { cx, cy: geo.headerY, r: geo.r, fill: C.nodeFill, stroke: isLeader ? PHASE.leader : C.neutralRing, 'stroke-width': 2 }));
      g2.appendChild(el('text', { x: cx, y: geo.headerY, 'text-anchor': 'middle', 'dominant-baseline': 'central', class: 'mono', 'font-size': geo.r * 0.66, 'font-weight': 700, fill: C.text }, `N${i}`));
      if (isLeader) g2.appendChild(el('text', { x: cx, y: geo.headerY - geo.r - 8, 'text-anchor': 'middle', class: 'mono', 'font-size': 9, fill: PHASE.leader }, `LEADER · r${lead.round}`));
      g2.style.cursor = 'pointer';
      g2.setAttribute('tabindex', '0');
      g2.setAttribute('role', 'button');
      g2.addEventListener('click', () => ctx.onSelect(i));
      g2.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); ctx.onSelect(i); } });
      g.appendChild(g2);
      if (ctx.selection === i) g.appendChild(el('circle', { cx, cy: geo.headerY, r: geo.r + 5, fill: 'none', stroke: C.text, 'stroke-width': 1, 'stroke-dasharray': '2 3', opacity: 0.7 }));
    });

    // the read's legs: client → leader, leader ⇄ quorum (confirm), leader → client
    const pg = el('g', {});
    g.appendChild(pg);
    if (frame.type !== 'read' || !lead || lead.node >= n) return {};
    const cpt = { x: geo.left, y: clientY };
    const lpt = { x: geo.cols[lead.node], y: geo.headerY };
    const peers = geo.cols.map((cx, i) => ({ x: cx, y: geo.headerY, i })).filter((p) => p.i !== lead.node);
    g.appendChild(el('path', { d: arcPath(cpt, lpt), fill: 'none', stroke: PHASE.propose, 'stroke-width': 1.2, opacity: 0.28 }));
    for (const p of peers) g.appendChild(el('path', { d: arcPath(lpt, p), fill: 'none', stroke: PHASE.promise, 'stroke-width': 1, opacity: 0.2 }));
    const dropped = frame.outcome === 'dropped';
    return {
      tick(t) {
        while (pg.firstChild) pg.removeChild(pg.firstChild);
        // three acts on one 0→1 clock: request (0–.3), confirm round (.3–.7), reply (.7–1)
        if (t < 0.3) {
          const p = arcPoint(cpt, lpt, ease(t / 0.3));
          pg.appendChild(particle(p.x, p.y, PHASE.propose));
        } else if (t < 0.7) {
          const u = (t - 0.3) / 0.4;
          for (const pp of peers) {
            const p = u < 0.5 ? arcPoint(lpt, pp, ease(u * 2)) : arcPoint(pp, lpt, ease((u - 0.5) * 2));
            pg.appendChild(particle(p.x, p.y, PHASE.promise));
          }
          if (!peers.length) {
            const p = arcPoint(cpt, lpt, 1);
            pg.appendChild(particle(p.x, p.y, PHASE.promise));
          }
        } else if (dropped) {
          pg.appendChild(badgeDrop(lpt.x, lpt.y - 26, PHASE.nack));
        } else {
          const p = arcPoint(lpt, cpt, ease((t - 0.7) / 0.3));
          pg.appendChild(particle(p.x, p.y, PHASE.chosen));
        }
      },
    };
  },

  narrate(frame) {
    if (frame.type === 'read') {
      if (frame.outcome === 'dropped') {
        return { phase: 'nack', color: PHASE.nack, title: `Read #${frame.seq} timed out`,
          lines: ['The confirmation round never completed inside the client\'s deadline — a leader change or partition swallowed it.',
            `The client saw nothing rather than something stale: after ${frame.waited_ms}ms it gave up. Refusing to answer is the safe failure mode for a read.`] };
      }
      const idx = frame.read_index === null || frame.read_index === undefined ? 'an empty prefix' : `read index ${frame.read_index}`;
      const l2 = frame.waitedAtBarrier
        ? `The applied prefix had not reached the read index yet, so the answer *waited at the commit barrier* — ${frame.waited_ms}ms in total (${frame.attempts} attempt${frame.attempts === 1 ? '' : 's'}). Only once applied ≥ read index was it served.`
        : `The prefix already covered the read index, so after the quorum round the answer left immediately (${frame.waited_ms}ms, ${frame.attempts} attempt${frame.attempts === 1 ? '' : 's'}).`;
      return { phase: 'promise', color: PHASE.promise, title: `Read #${frame.seq} — ${idx}`,
        lines: ['The leader does not answer from local state: it captures its applied watermark as the read index, then proves it is *still* leader with one heartbeat-ack quorum round.', l2] };
    }
    return { phase: 'idle', color: C.neutralRing, title: 'Why reads are not free',
      lines: ['"I am the leader" is a belief about the past. A deposed leader answering from local state serves a value that may already be overwritten.',
        'Step through each read: capture the read index → confirm leadership with a quorum → wait until applied ≥ read index → answer.'] };
  },

  digest(run) {
    const reads = readTimeline(run);
    const served = reads.filter((r) => r.outcome === 'delivered');
    const failed = reads.length - served.length;
    const maxWait = Math.max(0, ...served.map((r) => r.waited_ms));
    const redirects = served.filter((r) => r.attempts > 1).length;
    return [
      { label: 'reads served', value: String(served.length) },
      { label: 'reads timed out', value: String(failed) },
      { label: 'longest read wait', value: `${maxWait}ms` },
      { label: 'reads redirected first', value: String(redirects) },
      { label: 'confirm rounds run', value: String((run.read_confirms || []).length) },
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
    rows.push({ group: 'role', k: 'role', v: s.isLeader ? `leader (r${s.leaderRound})` : 'follower' });
    rows.push({ group: 'log', k: 'applied index', v: s.committed >= 0 ? String(s.committed) : '— none —' });
    return rows;
  },
};
