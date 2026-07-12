// modes/single.js — the single-decree renderer.
//
// Teaches how a 3-node cluster agrees on ONE value: a proposer runs Phase 1
// (Prepare → Promise) to claim a ballot, then Phase 2 (Accept → Accepted) to get
// a value chosen by a majority. Each of N0/N1/N2 can propose *and* accept; the
// acting proposer is drawn on the left, the three acceptors on the right, and one
// glowing phase-coloured particle set travels the active leg.

import { C, PHASE, PHASE_LABEL, valueColor } from '../tokens.js';
import { el, nodeDisc, particle, arcPath, arcPoint, ease, badgeDrop, badgeChosen } from '../svg.js';
import { nodesAt, stateAt, chosenAt, cmpBallot } from '../adapter.js';

// kind → phase-track index (-1 = before the track, e.g. the client propose intro).
const TRACK = { propose: -1, prepare: 0, promise: 1, accept: 2, accepted: 3, chosen: 4, commit: 4, nack: 0 };

// Group the slot-0 protocol stream into teaching steps: a contiguous run of the
// same (kind, ballot) is one fan-out (a Prepare broadcast, a set of Promises, …).
function buildFrames(run) {
  const legs = run.protocol
    .filter((p) => p.slot === 0 && p.kind !== 'heartbeat' && p.kind !== 'commit')
    .slice()
    .sort((a, b) => a.depart_ms - b.depart_ms || a.from - b.from);

  const frames = [];
  // intro: the client asks the first proposer to drive a round
  const first = legs[0];
  frames.push({
    index: 0, phase: 'propose', trackIndex: -1, timeMs: 0, ballot: null, legs: [],
    propose: first ? first.bnode : 0,
  });

  let cur = null;
  for (const p of legs) {
    const key = `${p.kind}:${p.bround}:${p.bnode}`;
    if (!cur || cur.key !== key) {
      cur = { key, kind: p.kind, ballot: { r: p.bround, n: p.bnode }, legs: [] };
      frames.push({ phase: p.kind, trackIndex: TRACK[p.kind] ?? 0, ballot: cur.ballot, legs: cur.legs, timeMs: 0 });
      cur._frame = frames[frames.length - 1];
    }
    cur.legs.push(p);
    cur._frame.timeMs = Math.max(cur._frame.timeMs, p.arrive_ms);
  }

  // final chosen step from the first slot-0 chosen event
  const chosen = run.chosen.filter((c) => c.slot === 0).sort((a, b) => a.time_ms - b.time_ms)[0];
  if (chosen) {
    const b = lastBallot(frames);
    frames.push({ phase: 'chosen', trackIndex: 4, ballot: b, legs: [], timeMs: chosen.time_ms, vhash: chosen.vhash });
  }
  frames.forEach((f, i) => { f.index = i; });
  return frames;
}

function lastBallot(frames) {
  for (let i = frames.length - 1; i >= 0; i--) if (frames[i].ballot) return frames[i].ballot;
  return { r: 1, n: 0 };
}

// ---- geometry: proposer left, three acceptors right ------------------------
function layout(dims) {
  const { w, h, mobile } = dims;
  if (mobile) {
    // proposer on top, acceptor row at the bottom: each eyebrow sits just above
    // its own group (a shared mid-height row belongs to neither).
    return {
      left: { x: w * 0.5, y: h * 0.16 },
      right: [
        { x: w * 0.22, y: h * 0.66 },
        { x: w * 0.5, y: h * 0.66 },
        { x: w * 0.78, y: h * 0.66 },
      ],
      client: { x: w * 0.12, y: h * 0.16 },
      r: 18,
      eyeProposer: { x: w * 0.5, y: h * 0.16 - 52 },
      eyeAcceptors: { x: w * 0.5, y: h * 0.66 - 62 },
    };
  }
  return {
    left: { x: w * 0.24, y: h * 0.5 },
    right: [
      { x: w * 0.76, y: h * 0.22 },
      { x: w * 0.76, y: h * 0.5 },
      { x: w * 0.76, y: h * 0.78 },
    ],
    client: { x: w * 0.1, y: h * 0.84 },
    r: 21,
    eyeProposer: { x: w * 0.24, y: 22 },
    eyeAcceptors: { x: w * 0.76, y: 22 },
  };
}

// Resolve a leg's screen endpoints. The proposer end (node == ballot proposer) is
// the left node; the acceptor end is the right column. Particle travels from→to.
function legEnds(leg, geo) {
  const prop = leg.bnode;
  if (leg.from === prop) return { a: geo.left, b: geo.right[leg.to], acc: leg.to };
  return { a: geo.right[leg.from], b: geo.left, acc: leg.from };
}

export const single = {
  id: 'single',
  label: 'single-decree',
  transport: 'step',
  seeds: [
    { value: 0, caption: 'the happy path' },
    { value: 19, caption: 'a duel resolves' },
    { value: 42, caption: 'watch livelock' },
  ],
  seedCaption(seed) {
    const s = this.seeds.find((x) => x.value === Number(seed));
    return s ? s.caption : 'lesson';
  },
  stageSize(mobile) {
    return mobile ? { w: 560, h: 520 } : { w: 1000, h: 360 };
  },

  frames: buildFrames,

  render(frame, ctx) {
    const g = ctx.stage;
    const geo = layout(ctx.dims);
    const snap = nodesAt(ctx.run, frame.timeMs);
    const acting = new Set(frame.legs.map((l) => l.from).concat(frame.legs.map((l) => l.to)));
    const propId = frame.ballot ? frame.ballot.n : frame.propose ?? 0;
    const color = PHASE[frame.phase] || C.neutralRing;
    const fs = ctx.dims.mobile ? 14 : 10; // in-stage label size (see eyebrow note)

    // eyebrows
    g.appendChild(eyebrow(geo.eyeProposer.x, geo.eyeProposer.y, 'PROPOSER', ctx.dims.mobile));
    g.appendChild(eyebrow(geo.eyeAcceptors.x, geo.eyeAcceptors.y, 'ACCEPTORS', ctx.dims.mobile));

    // faint client hint
    g.appendChild(nodeDisc(geo.client.x, geo.client.y, geo.r * 0.7, 'C', { ring: C.dim }));
    g.appendChild(el('text', { x: geo.client.x, y: geo.client.y + geo.r + 6, 'text-anchor': 'middle', 'font-size': fs, fill: C.faint }, 'client'));

    // static links for this frame's legs
    const linksG = el('g', {});
    for (const leg of frame.legs) {
      const { a, b } = legEnds(leg, geo);
      linksG.appendChild(el('path', { d: arcPath(a, b), fill: 'none', stroke: color, 'stroke-width': 1.2, opacity: 0.32 }));
    }
    g.appendChild(linksG);

    // proposer (left)
    const propActs = frame.phase === 'prepare' || frame.phase === 'accept' || frame.phase === 'propose';
    g.appendChild(clickable(nodeDisc(geo.left.x, geo.left.y, geo.r, `N${propId}`, {
      ring: propActs ? color : C.neutralRing, active: propActs, glowColor: color,
    }), propId, ctx));
    if (frame.ballot) {
      g.appendChild(el('text', { x: geo.left.x, y: geo.left.y + geo.r + 15, 'text-anchor': 'middle', class: 'mono', 'font-size': ctx.dims.mobile ? 14 : 11, fill: C.muted }, `ballot (${frame.ballot.r},${frame.ballot.n})`));
    }

    // acceptors (right)
    geo.right.forEach((pos, i) => {
      const s = snap[i];
      const on = acting.has(i) && (frame.phase === 'promise' || frame.phase === 'accepted' || frame.phase === 'nack');
      const isChosen = s.chosen;
      const ring = isChosen ? PHASE.chosen : (on ? color : C.neutralRing);
      g.appendChild(clickable(nodeDisc(pos.x, pos.y, geo.r, `N${i}`, {
        ring, active: on || isChosen, glowColor: isChosen ? PHASE.chosen : color,
      }), i, ctx));
      // accepted-value swatch above
      if (s.accepted.has) {
        g.appendChild(el('rect', { x: pos.x - 5, y: pos.y - geo.r - 12, width: 10, height: 10, rx: 2, fill: valueColor(s.accepted.vhash), stroke: C.stage, 'stroke-width': 1 }));
      }
      if (isChosen) g.appendChild(badgeChosen(pos.x + geo.r - 2, pos.y - geo.r + 2));
      // promised ballot label
      g.appendChild(el('text', { x: pos.x, y: pos.y + geo.r + 15, 'text-anchor': 'middle', class: 'mono', 'font-size': fs, fill: C.muted }, `promised (${s.promised.r},${s.promised.n})`));
      // selection highlight ring
      if (ctx.selection === i) g.appendChild(el('circle', { cx: pos.x, cy: pos.y, r: geo.r + 5, fill: 'none', stroke: C.text, 'stroke-width': 1, 'stroke-dasharray': '2 3', opacity: 0.7 }));
    });

    // faint quorum hint near the acceptors
    g.appendChild(el('text', { x: geo.right[2].x, y: geo.right[2].y + geo.r + (ctx.dims.mobile ? 34 : 30), 'text-anchor': 'middle', class: 'mono', 'font-size': fs, fill: C.faint }, 'quorum 2 / 3'));

    // particle layer
    const pg = el('g', {});
    g.appendChild(pg);
    const legs = frame.legs.map((leg) => ({ leg, ...legEnds(leg, geo) }));
    return {
      tick(t) {
        while (pg.firstChild) pg.removeChild(pg.firstChild);
        for (const { leg, a, b } of legs) {
          const dropped = leg.outcome === 'dropped';
          const reach = dropped ? 0.6 : 1;
          const p = arcPoint(a, b, reach * ease(t));
          if (dropped && t > 0.55) { pg.appendChild(badgeDrop(p.x, p.y, PHASE.nack)); continue; }
          pg.appendChild(particle(p.x, p.y, PHASE[leg.kind] || color));
        }
      },
    };
  },

  narrate(frame) {
    const b = frame.ballot || { r: 1, n: 0 };
    const P = PHASE;
    switch (frame.phase) {
      case 'propose':
        return { phase: 'propose', color: P.propose, title: 'Propose',
          lines: [`A client asks the cluster to agree on one value; node N${frame.propose} will drive the round.`,
            'Agreement needs a majority — 2 of 3 — at every step.'] };
      case 'prepare':
        return { phase: 'prepare', title: 'Prepare',
          lines: [`N${b.n} asks every acceptor for permission to propose, tagging its request with ballot (${b.r},${b.n}).`,
            'Phase 1 claims the ballot; a majority must promise before any value is proposed.'] };
      case 'promise': {
        const piggy = frame._piggyback;
        return { phase: 'promise', title: 'Promise',
          lines: [`Acceptors promise ballot (${b.r},${b.n}) and report any value they already accepted — that is a quorum.`,
            piggy
              ? 'An acceptor already holds a value, so the proposer must re-propose that one — the value-selection rule that keeps Paxos safe.'
              : 'No acceptor holds a value yet, so the proposer is free to choose its own.'] };
      }
      case 'accept':
        return { phase: 'accept', title: 'Accept',
          lines: [`N${b.n} now runs Phase 2: it asks the same majority to accept the value under ballot (${b.r},${b.n}).`,
            'The value it proposes obeys the promise rule from Phase 1.'] };
      case 'accepted':
        return { phase: 'accepted', title: 'Accepted',
          lines: [`A majority accepts ballot (${b.r},${b.n}). That is the second quorum of the round.`,
            'Once a majority has accepted, the value is locked in — no other value can win this slot.'] };
      case 'nack':
        return { phase: 'nack', title: 'Nack',
          lines: [`An acceptor has already promised a higher ballot, so it rejects (${b.r},${b.n}).`,
            'The stale proposer steps down and retries with a higher ballot — this back-and-forth is a duel.'] };
      case 'chosen':
        return { phase: 'chosen', title: 'Chosen',
          lines: [`A majority accepted ballot (${b.r},${b.n}); the value is chosen.`,
            'No two acceptors can ever choose differently — every learner converges on this one value.'] };
      default:
        return { phase: 'idle', title: PHASE_LABEL[frame.phase] || '', lines: [] };
    }
  },

  digest(run) {
    const st = stateAt(run, run.sim_duration_ms);
    let mr = 0, mn = 0;
    for (const s of st) if (cmpBallot(s.pr, s.pn, mr, mn) > 0) { mr = s.pr; mn = s.pn; }
    const promisedK = st.filter((s) => s.pr === mr && s.pn === mn).length;
    const acceptedK = st.filter((s) => s.acc).length;
    const choosers = new Set(run.chosen.filter((c) => c.slot === 0).map((c) => c.node)).size;
    const nacks = run.protocol.filter((p) => p.kind === 'nack').length;
    const firstChosenT = run.chosen.length ? Math.min(...run.chosen.map((c) => c.time_ms)) : Infinity;
    const ballotsBefore = new Set(run.protocol.filter((p) => p.kind === 'prepare' && p.depart_ms < firstChosenT).map((p) => `${p.bround}:${p.bnode}`)).size;
    const dueling = nacks > 0 || ballotsBefore >= 2;
    const resolved = run.chosen.some((c) => c.slot === 0);
    const drops = run.protocol.filter((p) => p.outcome === 'dropped' && p.slot === 0).length;
    return [
      { label: 'ballot', value: mr > 0 ? `(${mr},${mn})` : '—' },
      { label: 'promised quorum', value: `${promisedK}/3` },
      { label: 'accepted quorum', value: `${acceptedK}/3` },
      { label: 'chosen by', value: `${choosers}/3` },
      { label: 'dueling proposers', value: dueling ? (resolved ? 'yes → resolved' : 'yes → livelock') : 'no' },
      { label: 'network drops', value: String(drops) },
    ];
  },

  progress(run, frame) {
    return { type: 'track', activeIndex: frame.trackIndex };
  },

  inspect(run_or_frame, selection) {
    // shell calls inspect(frame, selection); we need the run, held on the frame's snapshot.
    const frame = run_or_frame;
    if (selection === null || selection === undefined) return [];
    const snap = frame.__snap;
    if (!snap) return [];
    const s = snap[selection];
    const rows = [];
    rows.title = `${s.isProposer ? 'proposer · ' : 'acceptor · '}N${selection}`;
    rows.push({ group: 'role', k: 'role', v: s.isProposer ? 'proposer + acceptor' : 'acceptor' });
    rows.push({ group: 'durable', k: 'promised ballot', v: `(${s.promised.r},${s.promised.n})` });
    if (s.accepted.has) rows.push({ group: 'volatile', k: 'accepted value', v: `#${shortHash(s.accepted.vhash)}`, swatch: valueColor(s.accepted.vhash) });
    else rows.push({ group: 'volatile', k: 'accepted value', v: '— none —' });
    rows.push({ group: 'volatile', k: 'has chosen', v: s.chosen ? 'yes' : 'no' });
    return rows;
  },
};

function shortHash(vh) { return (Number(vh) >>> 0).toString(16).slice(0, 6); }

// snapshot the promise-value-selection flag + node states onto each frame so
// narrate()/inspect() (which only receive the frame) can read them.
const _origFrames = single.frames;
single.frames = function (run) {
  const frames = _origFrames(run);
  for (const f of frames) {
    f.__snap = nodesAt(run, f.timeMs);
    if (f.phase === 'promise' && f.ballot) {
      // detect the value-selection (piggyback) case: some acceptor already held a
      // value when this ballot's Prepare went out.
      const pre = stateAt(run, f.legs.length ? Math.min(...f.legs.map((l) => l.depart_ms)) : f.timeMs);
      f._piggyback = pre.some((s) => s.acc);
    }
  }
  return frames;
};

function eyebrow(x, y, text, mobile) {
  // the mobile viewBox renders at ~0.6 scale, so bump the type to stay legible
  return el('text', { x, y, 'text-anchor': 'middle', 'font-size': mobile ? 14 : 10, 'letter-spacing': '1.6', fill: C.muted, 'font-weight': 700 }, text);
}
function clickable(node, id, ctx) {
  node.style.cursor = 'pointer';
  node.setAttribute('tabindex', '0');
  node.setAttribute('role', 'button');
  node.addEventListener('click', () => ctx.onSelect(id));
  node.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); ctx.onSelect(id); } });
  return node;
}
