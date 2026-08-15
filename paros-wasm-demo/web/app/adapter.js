// adapter.js — the boundary between the RunResult JSON and the renderers.
//
// `runSeed(seed)` (wasm) returns a flat RunResult (see README "Contract map").
// Everything the UI shows is *derived* from that payload here, so a future
// additive contract change touches this one file. All helpers are pure functions
// of `run` (order-independent folds over the event arrays — they never assume a
// stream is pre-sorted), which is what keeps a given step deterministic.
//
// The Frame type (the unit the transport indexes into) is produced per-mode by
// each renderer's `frames(run)`, but the shared snapshot/derivation helpers it
// leans on live here.

export const CLUSTER = 3; // run_seed fixes a 3-node cluster (CLUSTER_SIZE)

// Ballot order is lexicographic on (round, node).
export const cmpBallot = (ar, an, br, bn) => (ar !== br ? ar - br : an - bn);

// ---- per-node durable/volatile state as of simulated time T ----------------
// Latest node_states entry at or before T for each node → its promised ballot and
// its accepted value (slot 0 only, per the contract). Order-independent.
export function stateAt(run, T, n = CLUSTER) {
  const st = Array.from({ length: n }, () => ({ pr: 0, pn: 0, acc: false, vh: 0, t: -1 }));
  for (const s of run.node_states) {
    if (s.time_ms <= T && s.node < st.length && s.time_ms >= st[s.node].t) {
      st[s.node] = { pr: s.pround, pn: s.pbnode, acc: s.has_accepted, vh: s.vhash, t: s.time_ms };
    }
  }
  return st;
}

// Nodes that have learned a chosen value (slot 0) by T, plus the latest value hash.
export function chosenAt(run, T, slot = 0) {
  const set = new Set();
  let vh = null, vt = -1;
  for (const c of run.chosen) {
    if (c.time_ms > T || c.slot !== slot) continue;
    set.add(c.node);
    if (c.time_ms >= vt) { vt = c.time_ms; vh = c.vhash; }
  }
  return { set, vh };
}

// ---- multi-decree derived views --------------------------------------------
// The cluster leader as of T: the most recent leadership takeover at or before T.
export function leaderAt(run, T) {
  let best = null;
  for (const l of run.leaders) {
    if (l.time_ms <= T && (!best || l.time_ms >= best.time_ms)) best = l;
  }
  return best; // {time_ms, node, round} | null
}

// Per-node highest applied (committed) slot at or before T; -1 = nothing applied.
export function committedAt(run, T, n = CLUSTER) {
  const c = Array.from({ length: n }, () => -1);
  for (const a of run.applied) {
    if (a.time_ms <= T && a.node < c.length) c[a.node] = Math.max(c[a.node], a.slot);
  }
  return c;
}

// Per-node map slot -> chosen value hash, as learned by time T.
export function logAt(run, T, n = CLUSTER) {
  const m = Array.from({ length: n }, () => new Map());
  for (const ch of run.chosen) {
    if (ch.time_ms <= T && ch.node < m.length) m[ch.node].set(ch.slot, ch.vhash);
  }
  return m;
}

// Highest slot any node touched (multi-decree log height).
export function logMax(run) {
  let m = 0;
  for (const c of run.chosen) m = Math.max(m, c.slot);
  for (const p of run.protocol) m = Math.max(m, p.slot);
  return m;
}

// ---- linearizable-read derivations -----------------------------------------
// A read's lifecycle is issued → captured (a leader pins a read index) → confirmed
// (heartbeat-ack quorum + applied ≥ index) → served. Everything below is a pure
// fold over `run.reads` (see ReadShot in paros-sim/src/oracle.rs). `index` and
// `read_index` are `null` for the empty applied prefix, which orders below slot 0 —
// hence `idxNum` rather than a bare `??`.

// A read index as a comparable number: null (empty prefix) sorts below slot 0.
export const idxNum = (i) => (i === null || i === undefined ? -1 : i);

// A round that never confirmed: its leader was deposed (or its acks were lost)
// before the barrier cleared, so the client had to ask someone else.
export const isAbandoned = (round) => round.confirmed_ms === null || round.confirmed_ms === undefined;

// Did this read have to outlive a leader? (an abandoned round, a redirect after
// it was already parked, or more than one node asked)
export function crossedLeaders(read) {
  return read.rounds.some(isAbandoned)
    || read.rounds.length > 1
    || read.redirects.some((r) => r.kind === 'stepped_down' || r.kind === 'timeout');
}

// The read's state as of simulated time T:
//   'idle'    — not issued yet
//   'asking'  — issued, no round captured yet (or bounced off a follower)
//   'waiting' — a round captured its index; the barrier has not cleared
//   'served'  — the client has its answer
//   'failed'  — the client's deadline expired without one
export function readStateAt(read, T) {
  if (T < read.issued_ms) return 'idle';
  const served = read.served_ms !== null && read.served_ms !== undefined;
  if (served && T >= read.served_ms) return 'served';
  if (!served) {
    // Never served: once its last event is behind us, the client's deadline won.
    // Checked before the waiting case — an abandoned round would otherwise leave
    // the read "waiting" forever.
    const last = Math.max(read.issued_ms, ...read.redirects.map((r) => r.time_ms),
      ...read.rounds.map((r) => r.captured_ms));
    if (T >= last && read.rounds.every(isAbandoned)) return 'failed';
  }
  const live = read.rounds.find((r) => r.captured_ms <= T && (isAbandoned(r) || r.confirmed_ms > T));
  return live ? 'waiting' : 'asking';
}

// The round that is (or was last) in flight at T — the one holding the barrier.
export function liveRoundAt(read, T) {
  let best = null;
  for (const r of read.rounds) {
    if (r.captured_ms <= T && (!best || r.captured_ms >= best.captured_ms)) best = r;
  }
  return best;
}

// Did the read have to wait at the *commit barrier* proper — i.e. did any round
// pin an index the capturing node had not applied yet? That is the fresh-leader
// fence: a leader may hold a perfectly good quorum and still owe the reader the
// slots its election recovered but has not re-decided.
export function readWaitsAtBarrier(run, read) {
  return read.rounds.some((q) => idxNum(q.index) > committedAt(run, q.captured_ms)[q.node]);
}

// How long the read waited between pinning its index and answering, in ms.
export function readWaitMs(read) {
  if (!read.rounds.length || read.served_ms === null || read.served_ms === undefined) return null;
  return read.served_ms - read.rounds[0].captured_ms;
}

// Linearizability, condition C2, re-derived client-side for the badge: the client
// is sequential, so the watermarks it observes never move backwards.
export function readWatermarksMonotone(run) {
  let prev = -1;
  for (const r of [...(run.reads || [])].sort((a, b) => a.seq - b.seq)) {
    if (r.served_ms === null || r.served_ms === undefined) continue;
    const wm = idxNum(r.read_index);
    if (wm < prev) return false;
    prev = wm;
  }
  return true;
}

// Condition C1's visible half: no read is ever answered before its own barrier
// cleared (a served read has a confirmed round at or before its serve time).
export function readsServedAfterConfirmation(run) {
  return (run.reads || []).every((r) => {
    if (r.served_ms === null || r.served_ms === undefined) return true;
    return r.rounds.some((q) => !isAbandoned(q) && q.confirmed_ms <= r.served_ms);
  });
}

// ---- crash / recovery derivations ------------------------------------------
// The crash→restart "down" windows for node i: a crash opens one, its next restart
// closes it (an unclosed one runs to the end).
export function downWindows(run, i) {
  const evs = [];
  for (const c of run.crashes) if (c.node === i) evs.push({ t: c.time_ms, k: 'c', seam: c.seam });
  for (const r of run.restarts) if (r.node === i) evs.push({ t: r.time_ms, k: 'r' });
  evs.sort((a, b) => a.t - b.t);
  const wins = [];
  let open = null;
  for (const e of evs) {
    if (e.k === 'c' && open === null) open = { start: e.t, seam: e.seam };
    else if (e.k === 'r' && open !== null) { wins.push({ start: open.start, end: e.t, seam: open.seam }); open = null; }
  }
  if (open !== null) wins.push({ start: open.start, end: run.sim_duration_ms, seam: open.seam });
  return wins;
}

export const SEAM_TAG = { before_sync: 'pre-fsync', after_sync_before_send: 'fsync ok · send lost' };

// ---- oracle badge (green by construction; surfaced to be legible) ----------
// The sim panics on any safety/recovery violation, so a returned run is always
// safe. We re-derive the guarantees client-side purely to *show* them.

// Recovery oracle: a node's promised ballot never decreases, incl. across restart.
export function promisesIntact(run) {
  const last = new Map();
  for (const s of [...run.node_states].sort((a, b) => a.time_ms - b.time_ms)) {
    const p = last.get(s.node);
    if (p && cmpBallot(s.pround, s.pbnode, p.r, p.n) < 0) return false;
    last.set(s.node, { r: s.pround, n: s.pbnode });
  }
  return true;
}

// Safety oracle: at most one value hash chosen per slot.
export function oneValuePerSlot(run) {
  const byslot = new Map();
  for (const c of run.chosen) {
    if (byslot.has(c.slot) && byslot.get(c.slot) !== c.vhash) return false;
    byslot.set(c.slot, c.vhash);
  }
  return true;
}

// No-gaps oracle: the set of chosen slots is a contiguous prefix 0..high.
export function noGaps(run) {
  const slots = new Set(run.chosen.map((c) => c.slot));
  if (slots.size === 0) return true;
  const high = Math.max(...slots);
  for (let s = 0; s <= high; s++) if (!slots.has(s)) return false;
  return true;
}

// The oracle badge descriptor the shell renders: a list of {label, ok}. All green
// on any run the sim returned.
export function oracleBadge(run, mode) {
  const b = [{ label: 'safety · one value per slot', ok: oneValuePerSlot(run) }];
  if (mode === 'multi' || mode === 'crash') b.push({ label: 'no gaps in the log', ok: noGaps(run) });
  if (mode === 'multi' && (run.reads || []).length) {
    b.push({ label: 'linearizable · watermarks never regress', ok: readWatermarksMonotone(run) });
    b.push({ label: 'no read served before its barrier', ok: readsServedAfterConfirmation(run) });
  }
  if (mode === 'crash') b.push({ label: 'recovery · no promise lowered', ok: promisesIntact(run) });
  return b;
}

// ---- node snapshot (drives node rings, swatches, and the inspector) --------
// A uniform per-node view at simulated time T, blending durable promised ballot,
// volatile accepted value, chosen status, and inferred role.
export function nodesAt(run, T, n = CLUSTER) {
  const st = stateAt(run, T, n);
  const ch = chosenAt(run, T);
  const lead = leaderAt(run, T);
  const committed = committedAt(run, T, n);
  // top ballot across the cluster → the current proposer/leader for single-decree
  let mr = 0, mn = 0;
  for (const s of st) if (cmpBallot(s.pr, s.pn, mr, mn) > 0) { mr = s.pr; mn = s.pn; }
  return st.map((s, i) => ({
    id: i,
    promised: { r: s.pr, n: s.pn },
    accepted: s.acc ? { has: true, ballot: { r: 0, n: 0 }, vhash: s.vh } : { has: false },
    chosen: ch.set.has(i),
    committed: committed[i],
    isLeader: lead ? lead.node === i : false,
    leaderRound: lead && lead.node === i ? lead.round : null,
    // single-decree: the top-ballot's node is the acting proposer
    isProposer: mr > 0 && i === mn,
    topBallot: { r: mr, n: mn },
  }));
}
