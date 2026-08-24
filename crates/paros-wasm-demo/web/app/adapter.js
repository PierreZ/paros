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
