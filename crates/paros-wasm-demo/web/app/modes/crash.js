// modes/crash.js — the crash & recovery renderer (v1: a still timeline).
//
// Retells one seed as three node lanes on a shared time axis: a node crashes at
// the persist/send seam (⚡); its *volatile* state (leadership, in-flight msgs)
// clears while its *durable* state (promised ballot + fsync'd log) survives; it
// reboots (↻) and rejoins without breaking a promise. Nothing animates — the
// reader studies it at their own pace. This proves the renderer interface already
// covers a time-axis, durable-vs-volatile, recovery-badge mode: it is registered
// exactly like single/multi, with `transport: 'still'` so the shell hides the
// step controls. It is the shape the eventual animated crash mode will grow into.

import { C, PHASE } from '../tokens.js';
import { el, badgeCrash, badgeReboot } from '../svg.js';
import { downWindows, SEAM_TAG, promisesIntact, nodesAt, CLUSTER } from '../adapter.js';

export const crash = {
  id: 'crash',
  label: 'crash & recovery',
  transport: 'still',
  seeds: [
    { value: 99, caption: 'a seam crash' },
    { value: 7, caption: 'a failover under crash' },
  ],
  seedCaption(seed) {
    const s = this.seeds.find((x) => x.value === Number(seed));
    return s ? s.caption : 'lesson';
  },
  stageSize(mobile) {
    return mobile ? { w: 720, h: 460, scrollW: 720 } : { w: 1000, h: 440 };
  },

  // A still timeline is a single frame carrying the whole run.
  frames(run) {
    return [{ index: 0, phase: 'timeline', timeMs: run.sim_duration_ms, legs: [], __snap: nodesAt(run, run.sim_duration_ms) }];
  },

  render(frame, ctx) {
    const g = ctx.stage;
    const run = ctx.run;
    const { w, h } = ctx.dims;
    const padL = 110, padR = 30, padT = 96, padB = 44;
    const plotW = w - padL - padR;
    const dur = Math.max(run.sim_duration_ms, 1);
    const xOf = (t) => padL + plotW * Math.max(0, Math.min(1, t / dur));
    const laneH = (h - padT - padB) / CLUSTER;
    const laneY = (i) => padT + laneH * (i + 0.5);
    const ok = promisesIntact(run);

    // header (starts at the lane-label gutter, x=12, so the narrow mobile view
    // shows it before any swipe; the oracle verdict is colour + words, no glyph)
    g.appendChild(el('text', { x: 12, y: 30, class: 'mono', 'font-size': 13, 'font-weight': 700, fill: C.text }, `seed ${run.seed} — crash & recovery timeline`));
    g.appendChild(el('text', { x: 12, y: 50, 'font-size': 12, 'font-weight': 700, fill: ok ? PHASE.chosen : PHASE.nack }, ok ? 'recovery oracle: no promise lowered across any restart' : 'recovery oracle VIOLATED: a promise went backwards'));
    legend(g, 12, 68);

    // time axis + gridlines
    const axisY = h - padB;
    g.appendChild(el('line', { x1: padL, y1: axisY, x2: w - padR, y2: axisY, stroke: C.hairline, 'stroke-width': 1 }));
    for (let k = 0; k <= 4; k++) {
      const t = dur * k / 4, x = xOf(t);
      g.appendChild(el('line', { x1: x, y1: padT - 6, x2: x, y2: axisY, stroke: C.softline, 'stroke-width': 1 }));
      g.appendChild(el('text', { x, y: axisY + 16, 'text-anchor': 'middle', class: 'mono', 'font-size': 10, fill: C.faint }, `${Math.round(t)}ms`));
    }

    for (let i = 0; i < CLUSTER; i++) {
      const y = laneY(i);
      const wins = downWindows(run, i);

      // durable band: one continuous green line — never breaks
      g.appendChild(el('line', { x1: padL, y1: y + 16, x2: w - padR, y2: y + 16, stroke: '#238636', 'stroke-width': 3 }));
      for (const c of run.chosen) if (c.node === i) {
        const x = xOf(c.time_ms);
        g.appendChild(el('line', { x1: x, y1: y + 16, x2: x, y2: y + 10, stroke: PHASE.chosen, 'stroke-width': 2 }));
      }

      // volatile baseline + red down windows
      g.appendChild(el('line', { x1: padL, y1: y, x2: w - padR, y2: y, stroke: '#3d444d', 'stroke-width': 2 }));
      for (const win of wins) {
        const x0 = xOf(win.start), x1 = Math.max(xOf(win.end), x0 + 6);
        g.appendChild(el('rect', { x: x0, y: y - 13, width: x1 - x0, height: 26, fill: PHASE.nack, 'fill-opacity': 0.14, stroke: PHASE.nack, 'stroke-opacity': 0.5, 'stroke-width': 1, 'stroke-dasharray': '3 3' }));
        if (x1 - x0 > 26) g.appendChild(el('text', { x: (x0 + x1) / 2, y: y - 16, 'text-anchor': 'middle', 'font-size': 9, fill: PHASE.nack }, 'down'));
      }

      // leader spans (gold), clipped to exclude down windows
      const leaders = [...run.leaders].sort((a, b) => a.time_ms - b.time_ms);
      for (let li = 0; li < leaders.length; li++) {
        if (leaders[li].node !== i) continue;
        const start = leaders[li].time_ms;
        const end = li + 1 < leaders.length ? leaders[li + 1].time_ms : run.sim_duration_ms;
        let segStart = start;
        for (const win of [...wins, { start: end, end }]) {
          if (win.start > segStart) {
            const xs = xOf(segStart), xe = xOf(Math.min(win.start, end));
            if (xe > xs) g.appendChild(el('line', { x1: xs, y1: y - 20, x2: xe, y2: y - 20, stroke: PHASE.leader, 'stroke-width': 4 }));
          }
          segStart = Math.max(segStart, win.end);
          if (segStart >= end) break;
        }
      }

      // crash (seam line + tag) and restart. Tags of crashes that land close in
      // time would overprint each other, so each tag takes the first of three
      // stacked rows whose previous tag it does not overlap.
      const crashes = run.crashes.filter((c) => c.node === i).sort((a, b) => a.time_ms - b.time_ms);
      const tagRowEnd = [-Infinity, -Infinity, -Infinity];
      for (const c of crashes) {
        const x = xOf(c.time_ms);
        g.appendChild(el('line', { x1: x, y1: y - 20, x2: x, y2: y + 16, stroke: PHASE.nack, 'stroke-opacity': 0.5, 'stroke-width': 1, 'stroke-dasharray': '2 2' }));
        g.appendChild(badgeCrash(x, y + 1));
        const tag = SEAM_TAG[c.seam] || c.seam;
        const half = tag.length * 5.4 / 2; // ~9px type
        let row = tagRowEnd.findIndex((end) => x - half > end);
        if (row < 0) row = tagRowEnd.indexOf(Math.min(...tagRowEnd));
        tagRowEnd[row] = x + half;
        g.appendChild(el('text', { x, y: y - 24 - row * 11, 'text-anchor': 'middle', 'font-size': 9, fill: '#f0b6b3' }, tag));
      }
      for (const r of run.restarts) if (r.node === i) {
        g.appendChild(badgeReboot(xOf(r.time_ms), y + 2));
      }

      // lane label + durable summary
      const promised = [...run.node_states].filter((s) => s.node === i).sort((a, b) => a.time_ms - b.time_ms).pop();
      const slots = new Set(run.chosen.filter((c) => c.node === i).map((c) => c.slot)).size;
      g.appendChild(el('text', { x: 12, y: y - 2, class: 'mono', 'font-size': 12, 'font-weight': 700, fill: C.text }, `N${i}`));
      g.appendChild(el('text', { x: 12, y: y + 13, class: 'mono', 'font-size': 9, fill: PHASE.chosen }, promised ? `durable (${promised.pround},${promised.pbnode})` : 'durable —'));
      g.appendChild(el('text', { x: 12, y: y + 25, class: 'mono', 'font-size': 9, fill: C.muted }, `${slots} slots`));
    }
    return {};
  },

  narrate(frame, run) {
    return { phase: 'idle', color: PHASE.leader, title: 'Crash & recovery',
      lines: ['A node dies at the persist/send seam. Its volatile state clears; its durable state survives.',
        'It reboots, rebuilds from the durable promised ballot + fsync’d log, and rejoins without breaking a promise.'] };
  },

  digest(run) {
    const bs = run.crashes.filter((c) => c.seam === 'before_sync').length;
    const as = run.crashes.length - bs;
    const slots = new Set(run.chosen.map((c) => c.slot));
    const syncd = run.syncs.filter((s) => s.sync).length;
    const relaxed = run.syncs.length - syncd;
    return [
      { label: 'seam crashes', value: String(run.crashes.length) },
      { label: 'after fsync', value: `${as} (durable, send lost)` },
      { label: 'before fsync', value: `${bs} (batch lost)` },
      { label: 'restarts', value: String(run.restarts.length) },
      { label: 'durable read-backs', value: String(run.recovered.length) },
      { label: 'batches fsync’d', value: relaxed ? `${syncd} (+${relaxed} relaxed)` : String(syncd) },
      { label: 'log committed', value: `${slots.size} slots` },
      { label: 'promises', value: promisesIntact(run) ? 'intact ✓' : 'BROKEN ✗' },
    ];
  },

  progress() { return { type: 'none' }; },

  inspect(frame, selection) {
    if (selection === null || selection === undefined || !frame.__snap) return [];
    const s = frame.__snap[selection];
    const rows = [];
    rows.title = `node · N${selection}`;
    rows.push({ group: 'durable', k: 'promised ballot', v: `(${s.promised.r},${s.promised.n})` });
    rows.push({ group: 'durable', k: 'committed index', v: s.committed >= 0 ? String(s.committed) : '— none —' });
    return rows;
  },
};

// The legend row, built from the same vector marks the timeline uses — never font
// glyphs (svg.js doctrine: a machine without an emoji font must not show tofu).
function legend(g, x, y) {
  const label = (lx, text) => {
    g.appendChild(el('text', { x: lx, y: y + 4, 'font-size': 11, fill: C.muted }, text));
    return lx + text.length * 6 + 26; // ~6px/char at 11px + gap to the next mark
  };
  g.appendChild(badgeCrash(x + 4, y));
  x = label(x + 12, 'crash at seam');
  g.appendChild(badgeReboot(x + 5, y));
  x = label(x + 14, 'restart');
  g.appendChild(el('rect', { x, y: y - 5, width: 16, height: 10, fill: PHASE.nack, 'fill-opacity': 0.14, stroke: PHASE.nack, 'stroke-opacity': 0.5, 'stroke-width': 1, 'stroke-dasharray': '3 3' }));
  x = label(x + 22, 'down (volatile lost)');
  g.appendChild(el('line', { x1: x, y1: y, x2: x + 16, y2: y, stroke: '#238636', 'stroke-width': 3 }));
  x = label(x + 22, 'durable (survives)');
  g.appendChild(el('line', { x1: x + 2, y1: y - 5, x2: x + 2, y2: y + 5, stroke: PHASE.chosen, 'stroke-width': 2 }));
  label(x + 10, 'chosen');
}
