// modes/ctrl.js — the protocol-aware recovery renderer (#34, still timeline).
//
// Stage 8 flips detect ⇒ crash into *recover-or-wait* (CTRL): a corrupted
// committed entry is re-fetched from a peer that still holds a correct copy;
// when no correct copy exists anywhere the node WAITS rather than fabricate or
// truncate history; and recovery never deletes promised/accepted ballot state
// — the adversarial promise-corruption case stays safe.

import { C, PHASE } from '../tokens.js';
import { el, badgeCrash, badgeReboot, badgeRot, badgeHeal, badgeWait, badgeSnapshot } from '../svg.js';
import { clusterOf, nodesAt, oneValuePerSlot, promisesIntact } from '../adapter.js';
import { timelineGeo, drawHeader, drawAxis, drawLaneBase, drawLegend, tagStacker } from './timeline.js';

export const ctrl = {
  id: 'ctrl',
  label: 'ctrl recovery',
  transport: 'still',
  seeds: [
    { value: 4, caption: 'healed from peers; one waits' },
    { value: 3, caption: 'a promise-copy rot survived' },
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
    const geo = timelineGeo(run, ctx.dims);
    const repairs = run.repairs || [];
    const healed = repairs.filter((r) => r.kind === 'healed').length;
    const waited = repairs.filter((r) => r.kind === 'parked').length;
    const notLost = oneValuePerSlot(run) && promisesIntact(run);
    drawHeader(g, `seed ${run.seed} — recover-or-wait (CTRL)`, {
      ok: notLost,
      text: notLost
        ? `committed data never lost: ${healed} record${healed === 1 ? '' : 's'} healed from peers, ${waited} node-boot${waited === 1 ? '' : 's'} chose to wait, no promise reneged`
        : 'CTRL guarantee VIOLATED — history was fabricated or a promise reneged',
    });
    drawLegend(g, 12, 68, [
      { mark: (x, y) => badgeRot(x, y), label: 'committed record rots' },
      { mark: (x, y) => badgeHeal(x, y), label: 'healed from a peer copy' },
      { mark: (x, y) => badgeWait(x, y), label: 'waits (no correct copy)' },
      { mark: (x, y) => badgeSnapshot(x, y), label: 'snapshot reset' },
      { mark: (x, y) => badgeCrash(x, y), label: 'crash' },
      { mark: (x, y) => badgeReboot(x, y), label: 'restart' },
    ]);
    drawAxis(g, geo);

    for (let i = 0; i < n; i++) {
      const rots = (run.corruptions || []).filter((c) => c.node === i);
      const mine = repairs.filter((r) => r.node === i);
      const lane = mine.filter((r) => r.kind === 'healed').length;
      drawLaneBase(g, geo, run, i, `${rots.length} rotted · ${lane} healed`);
      const y = geo.laneY(i);
      const stack = tagStacker();
      for (const c of rots) {
        const x = geo.xOf(c.time_ms);
        g.appendChild(badgeRot(x, y - 1));
        const isPromise = c.record.startsWith('promise');
        const tag = `${c.record}`;
        const row = stack(x, tag);
        g.appendChild(el('text', { x, y: y - 24 - row * 11, 'text-anchor': 'middle', 'font-size': 9, fill: isPromise ? PHASE.leader : '#f0b6b3' }, tag));
      }
      for (const r of mine) {
        const x = geo.xOf(r.time_ms);
        if (r.kind === 'healed') g.appendChild(badgeHeal(x, y + 11));
        else if (r.kind === 'parked') g.appendChild(badgeWait(x, y + 11, PHASE.accept));
        else if (r.kind === 'reset') g.appendChild(badgeSnapshot(x, y + 11));
      }
      for (const c of (run.storage_crashes || []).filter((c) => c.node === i)) {
        g.appendChild(badgeCrash(geo.xOf(c.time_ms), y + 1));
      }
      for (const r of run.restarts.filter((r) => r.node === i)) {
        g.appendChild(badgeReboot(geo.xOf(r.time_ms), y + 2));
      }
    }

    // cluster-wide unrecoverable facts (world ground truth), if any
    const unrec = repairs.filter((r) => r.kind === 'unrecoverable');
    if (unrec.length) {
      const y = geo.padT - 14;
      for (const u of unrec) {
        const x = geo.xOf(u.time_ms);
        g.appendChild(badgeWait(x, y, PHASE.nack));
        g.appendChild(el('text', { x: x + 10, y: y + 4, 'font-size': 9, fill: PHASE.nack }, `slot ${u.slot}: no readable copy anywhere — wait`));
      }
    }
    return {};
  },

  narrate() {
    return { phase: 'chosen', color: PHASE.chosen, title: 'Protocol-aware recovery (CTRL)',
      lines: ['A corrupted committed entry is not the end: Phase 1 doubles as the recovery query, and a peer holding a correct copy re-ships it — the record heals in place.',
        'When *no* correct copy exists the node waits rather than fabricate or truncate; and recovery never deletes promise/accepted ballot state, so even a rotted promise copy cannot make a node renege.'] };
  },

  digest(run) {
    const repairs = run.repairs || [];
    const rots = run.corruptions || [];
    const promiseRots = rots.filter((c) => c.record.startsWith('promise')).length;
    const notLost = oneValuePerSlot(run) && promisesIntact(run);
    return [
      { label: 'committed data', value: notLost ? 'never lost ✓' : 'LOST ✗' },
      { label: 'records rotted', value: String(rots.length) },
      { label: 'healed from peers', value: String(repairs.filter((r) => r.kind === 'healed').length) },
      { label: 'chose to wait', value: String(repairs.filter((r) => r.kind === 'parked').length) },
      { label: 'snapshot resets', value: String(repairs.filter((r) => r.kind === 'reset').length) },
      { label: 'promise copies rotted', value: `${promiseRots} (${promisesIntact(run) ? 'none reneged ✓' : 'RENEGED ✗'})` },
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
