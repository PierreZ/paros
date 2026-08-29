// modes/disk.js — the fail-stop disk-fault renderer (#32, still timeline).
//
// Stage 6 teaches: a disk fault that needs no CTRL machinery (a write EIO, a
// failed fsync) degrades cleanly to a *crash* — the node refuses to run on a
// disk it cannot trust — and then recovers through the ordinary Stage-4 restart
// path, while the cluster stays available with up to f nodes down.

import { C, PHASE } from '../tokens.js';
import { el, badgeCrash, badgeReboot, badgeDisk } from '../svg.js';
import { clusterOf, nodesAt, downWindowsAll, downCountSegments, faultToleranceOf } from '../adapter.js';
import { timelineGeo, drawHeader, drawAxis, drawLaneBase, drawLegend, tagStacker } from './timeline.js';

export const disk = {
  id: 'disk',
  label: 'disk faults',
  transport: 'still',
  seeds: [
    { value: 3, caption: 'EIO + failed fsyncs, cleanly' },
    { value: 121, caption: 'fsyncgate: failed but durable' },
  ],
  seedCaption(seed) {
    const s = this.seeds.find((x) => x.value === Number(seed));
    return s ? s.caption : 'lesson';
  },
  stageSize(mobile) {
    return mobile ? { w: 760, h: 520, scrollW: 760 } : { w: 1000, h: 500 };
  },

  frames(run) {
    return [{ index: 0, type: 'timeline', timeMs: run.sim_duration_ms, legs: [], __snap: nodesAt(run, run.sim_duration_ms) }];
  },

  render(frame, ctx) {
    const g = ctx.stage;
    const run = ctx.run;
    const n = clusterOf(run);
    const f = faultToleranceOf(n);
    const geo = timelineGeo(run, ctx.dims, { padB: 76 });
    const segs = downCountSegments(run, n);
    const worst = Math.max(0, ...segs.map((s) => s.down));
    const stayedAvailable = worst <= f;
    drawHeader(g, `seed ${run.seed} — fail-stop disk faults (n=${n}, f=${f})`, {
      ok: stayedAvailable,
      // Losing the quorum is a liveness *pause*, never a safety loss — the
      // oracle badges below stay green either way — so the over-f case reads
      // amber, not failure-red.
      color: stayedAvailable ? undefined : PHASE.accept,
      text: stayedAvailable
        ? `availability held: at most ${worst} node${worst === 1 ? '' : 's'} down at once (≤ f=${f})`
        : `${worst} nodes down at once (> f=${f}): service paused, safety held, and it resumed on restart`,
    });
    drawLegend(g, 12, 68, [
      { mark: (x, y) => badgeDisk(x, y, PHASE.accept), label: 'disk fault injected' },
      { mark: (x, y) => badgeCrash(x, y), label: 'crash (the only reaction)' },
      { mark: (x, y) => badgeReboot(x, y), label: 'restart & recover' },
    ]);
    drawAxis(g, geo);

    for (let i = 0; i < n; i++) {
      const faults = (run.disk_faults || []).filter((d) => d.node === i);
      drawLaneBase(g, geo, run, i, `${faults.length} disk fault${faults.length === 1 ? '' : 's'}`);
      const y = geo.laneY(i);
      const stack = tagStacker();
      for (const d of faults) {
        const x = geo.xOf(d.time_ms);
        g.appendChild(badgeDisk(x, y - 1, PHASE.accept));
        const tag = `${d.kind === 'write_eio' ? 'EIO' : 'fsync✗'} ${d.record}`;
        const row = stack(x, tag);
        g.appendChild(el('text', { x, y: y - 24 - row * 11, 'text-anchor': 'middle', 'font-size': 9, fill: PHASE.accept }, tag));
      }
      for (const c of (run.storage_crashes || []).filter((c) => c.node === i)) {
        g.appendChild(badgeCrash(geo.xOf(c.time_ms), y + 1));
      }
      for (const c of run.crashes.filter((c) => c.node === i)) {
        g.appendChild(badgeCrash(geo.xOf(c.time_ms), y + 1));
      }
      for (const r of run.restarts.filter((r) => r.node === i)) {
        g.appendChild(badgeReboot(geo.xOf(r.time_ms), y + 2));
      }
    }

    // availability strip: down-count over time vs f
    const stripY = geo.axisY + 26, stripH = 12;
    g.appendChild(el('text', { x: 12, y: stripY + 10, class: 'mono', 'font-size': 9, fill: C.muted }, 'available?'));
    for (const s of segs) {
      const x0 = geo.xOf(s.start), x1 = Math.max(geo.xOf(s.end), x0 + 1);
      const okSeg = s.down <= f;
      g.appendChild(el('rect', { x: x0, y: stripY, width: x1 - x0, height: stripH, fill: okSeg ? PHASE.chosen : PHASE.nack, 'fill-opacity': s.down === 0 ? 0.18 : 0.45 }));
    }
    g.appendChild(el('text', { x: geo.w - geo.padR, y: stripY + 10, 'text-anchor': 'end', class: 'mono', 'font-size': 9, fill: C.faint }, `green = a quorum is up (≤ ${f} down)`));
    return {};
  },

  narrate() {
    return { phase: 'accept', color: PHASE.accept, title: 'When the disk dies',
      lines: ['A write EIO or a failed fsync is ambiguous — the bytes may or may not be on the medium — so the node does the only safe thing: it treats the fault as a crash.',
        'No special machinery: it restarts through the ordinary recovery path, rebuilds from what *is* durable, and the cluster keeps serving as long as at most f nodes are down.'] };
  },

  digest(run) {
    const faults = run.disk_faults || [];
    const eio = faults.filter((d) => d.kind === 'write_eio').length;
    const fsync = faults.filter((d) => d.kind === 'fsync_failed').length;
    // fsyncgate: only the *fsync* verdicts are the interesting ambiguity — a
    // "failed" flush whose batch secretly reached the medium anyway.
    const persisted = faults.filter((d) => d.kind === 'fsync_failed' && d.persisted).length;
    const n = clusterOf(run);
    let downMs = 0;
    for (let i = 0; i < n; i++) for (const w of downWindowsAll(run, i)) downMs += w.end - w.start;
    return [
      { label: 'disk faults injected', value: String(faults.length) },
      { label: 'write EIO', value: String(eio) },
      { label: 'fsync failures', value: `${fsync}${fsync ? ` (${persisted} secretly durable)` : ''}` },
      { label: 'storage crash decisions', value: String((run.storage_crashes || []).length) },
      { label: 'restarts', value: String(run.restarts.length) },
      { label: 'node-downtime total', value: `${downMs}ms` },
      { label: 'client acks', value: `${run.delivered} / ${run.requests}` },
    ];
  },

  progress() { return { type: 'none' }; },

  inspect(frame, selection) {
    if (selection === null || selection === undefined || !frame.__snap) return [];
    const s = frame.__snap[selection];
    const rows = [];
    rows.title = `node · N${selection}`;
    rows.push({ group: 'durable', k: 'promised ballot', v: `(${s.promised.r},${s.promised.n})` });
    rows.push({ group: 'durable', k: 'applied index', v: s.committed >= 0 ? String(s.committed) : '— none —' });
    return rows;
  },
};
