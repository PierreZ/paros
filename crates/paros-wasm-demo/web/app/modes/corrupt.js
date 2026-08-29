// modes/corrupt.js — the corruption-detection renderer (#33, still timeline).
//
// Stage 7 teaches the CLStore rule: silent disk corruption (bit flip, lost
// write, misdirected write, torn tail, …) becomes a *detected* checksum
// mismatch on read, and the node crashes rather than let one corrupted byte
// reach protocol logic — the counter of silent bad reads must stay zero.

import { C, PHASE } from '../tokens.js';
import { el, badgeCrash, badgeReboot, badgeRot, badgeDetect } from '../svg.js';
import { clusterOf, nodesAt, oneValuePerSlot, promisesIntact } from '../adapter.js';
import { timelineGeo, drawHeader, drawAxis, drawLaneBase, drawLegend, tagStacker } from './timeline.js';

// Short human tags for the injected corruption families.
const KIND_TAG = {
  BitFlip: 'bit flip', LostWrite: 'lost write', Misdirected: 'misdirected',
  ReadEio: 'read EIO', TornTail: 'torn tail', PromiseCopy: 'promise copy', Metadata: 'fs metadata',
};

export const corrupt = {
  id: 'corrupt',
  label: 'corruption',
  transport: 'still',
  seeds: [
    { value: 1, caption: 'bit flips caught by checksums' },
    { value: 4, caption: 'several families at once' },
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
    const injected = (run.corruptions || []).length;
    const safe = oneValuePerSlot(run) && promisesIntact(run);
    drawHeader(g, `seed ${run.seed} — silent corruption vs checksums`, {
      ok: safe,
      text: safe
        ? `${injected} corruption${injected === 1 ? '' : 's'} injected — zero silent bad reads reached protocol logic`
        : 'safety oracle VIOLATED — corrupted bytes changed protocol state',
    });
    drawLegend(g, 12, 68, [
      { mark: (x, y) => badgeRot(x, y), label: 'corruption injected (silent)' },
      { mark: (x, y) => badgeDetect(x, y), label: 'checksum mismatch detected' },
      { mark: (x, y) => badgeCrash(x, y), label: 'detect ⇒ crash' },
      { mark: (x, y) => badgeReboot(x, y), label: 'restart' },
    ]);
    drawAxis(g, geo);

    for (let i = 0; i < n; i++) {
      const rots = (run.corruptions || []).filter((c) => c.node === i);
      const dets = (run.detections || []).filter((d) => d.node === i);
      drawLaneBase(g, geo, run, i, `${rots.length} injected · ${dets.length} flagged`);
      const y = geo.laneY(i);
      const stack = tagStacker();
      for (const c of rots) {
        const x = geo.xOf(c.time_ms);
        g.appendChild(badgeRot(x, y - 1));
        const tag = `${KIND_TAG[c.kind] || c.kind} ${c.record}`;
        const row = stack(x, tag);
        g.appendChild(el('text', { x, y: y - 24 - row * 11, 'text-anchor': 'middle', 'font-size': 9, fill: '#f0b6b3' }, tag));
      }
      for (const d of dets) {
        g.appendChild(badgeDetect(geo.xOf(d.time_ms), y + 10));
      }
      for (const c of (run.storage_crashes || []).filter((c) => c.node === i)) {
        g.appendChild(badgeCrash(geo.xOf(c.time_ms), y + 1));
      }
      for (const r of run.restarts.filter((r) => r.node === i)) {
        g.appendChild(badgeReboot(geo.xOf(r.time_ms), y + 2));
      }
    }
    return {};
  },

  narrate() {
    return { phase: 'nack', color: PHASE.nack, title: 'Detecting silent corruption',
      lines: ['The disk does not announce that it lied: a bit flip, a lost write, a misdirected write all return "success" and rot quietly. Every record therefore carries a checksum, verified on every read.',
        'A mismatch — including the zeros a lost write leaves — is never repaired-by-guess and never truncated away: the node crashes before one bad byte reaches protocol logic. Zero silent bad reads is the whole contract.'] };
  },

  digest(run) {
    const rots = run.corruptions || [];
    const byKind = new Map();
    for (const c of rots) byKind.set(c.kind, (byKind.get(c.kind) || 0) + 1);
    const families = [...byKind.entries()].map(([k, v]) => `${KIND_TAG[k] || k}×${v}`).join(' · ') || '—';
    const safe = oneValuePerSlot(run) && promisesIntact(run);
    return [
      { label: 'silent bad reads', value: safe ? '0 ✓' : 'VIOLATION ✗' },
      { label: 'corruptions injected', value: String(rots.length) },
      { label: 'families', value: families },
      { label: 'mismatches flagged on read', value: String((run.detections || []).length) },
      { label: 'detect ⇒ crash decisions', value: String((run.storage_crashes || []).length) },
      { label: 'restarts', value: String(run.restarts.length) },
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
