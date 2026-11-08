// The election stepper: the milestones of one leader election of one
// journal, in the order they happened, as the page knows them from the
// messages and events the machines log. A read only: stepping moves the
// page's clock back and forth over what it already holds; it changes
// nothing in the universe.
//
// Pure: no DOM, so `node --test website/sandbox/*.test.mjs` checks it.

// How long after its start the page looks for an election's milestones, in
// simulated ms: an election that takes longer shows what it reached.
export const ELECTION_SPAN_MS = 2000;

const m = (machine) => (machine === undefined || machine === null ? "?" : `m${machine}`);

// The campaign of `key` that starts first after `from`: a matchmaking phase
// (a journal with matchmakers), or else the first Prepare of a round higher
// than any Prepare before `from`. `campaigns` is the page's list of
// `{ t, key, kind, ... }` (kind `started`, `completed` or `elected`).
// Answers the start time, or null.
export function nextCampaign(msgs, campaigns, key, from) {
  let best = null;
  for (const c of campaigns) {
    if (c.key === key && c.kind === "started" && c.t > from) { best = c.t; break; }
  }
  let before = -1;
  for (const msg of msgs) {
    if (msg.key !== key || msg.kind !== "prepare") continue;
    const round = Number(msg.round ?? 0);
    if (msg.t <= from) { before = Math.max(before, round); continue; }
    if (round > before) { if (best === null || msg.t < best) best = msg.t; break; }
  }
  return best;
}

// The milestones of the election of `key` that starts at `start`: each
// `{ t, title, text }`, in time order. `majority` is the size of a Phase-1
// quorum of the configuration in force, for the promise count.
export function milestones(msgs, campaigns, key, start, majority) {
  const end = start + ELECTION_SPAN_MS;
  const out = [];
  const mine = campaigns.filter((c) => c.key === key && c.t >= start && c.t <= end);
  const began = mine.find((c) => c.kind === "started");
  if (began) {
    out.push({ t: began.t, title: "Matchmaking",
      text: `${m(began.leader)} wants to lead at ballot ${began.round}. It first registers its configuration`
        + `${began.reconfiguration ? " (a new one: a reconfiguration)" : ""} at the matchmakers.` });
  }
  const done = mine.find((c) => c.kind === "completed" && (!began || c.round === began.round));
  if (done) {
    out.push({ t: done.t, title: "Matchmakers answered",
      text: `A majority of matchmakers (${done.registeredBy}) stored it and sent back ${done.prior} older `
        + `configuration${done.prior === 1 ? "" : "s"}. Phase 1 must reach a quorum of each of them.` });
  }
  // Phase 1: the first Prepare after the matchmaking (or the start), and
  // the promise that completes a quorum.
  const after = done ? done.t : start;
  let prepare = null;
  const promised = new Set();
  let quorum = null;
  let accept = null;
  let commit = null;
  for (const msg of msgs) {
    if (msg.key !== key || msg.t < after || msg.t > end) continue;
    if (!prepare && msg.kind === "prepare") prepare = msg;
    if (prepare && !quorum && msg.kind === "promise" && msg.round === prepare.round) {
      promised.add(msg.from);
      // The candidate promises its own ballot without a message.
      if (promised.size + 1 >= majority) quorum = msg;
    }
    if (prepare && !accept && msg.kind === "accept" && msg.t >= prepare.t) accept = msg;
    if (accept && !commit && msg.kind === "commit" && msg.t >= accept.t) commit = msg;
  }
  if (prepare) {
    out.push({ t: prepare.t, title: "Phase 1: Prepare",
      text: `${m(prepare.from)} sends Prepare at ballot ${prepare.round} to the acceptors: "promise to ignore lower ballots".` });
  }
  if (quorum) {
    out.push({ t: quorum.t, title: "Phase 1: promise quorum",
      text: `${m(quorum.from)}'s promise completes a quorum. Each promise reports the values its acceptor accepted.` });
  }
  const elected = mine.find((c) => c.kind === "elected" && (!prepare || c.t >= prepare.t));
  if (elected) {
    out.push({ t: elected.t, title: "Elected",
      text: `${m(elected.leader)} leads at ballot ${elected.round}. It re-proposes what the promises reported and fills the holes with no-ops.` });
  }
  if (accept) {
    out.push({ t: accept.t, title: "Phase 2: Accept",
      text: `${m(accept.from)} sends Accept for slot ${accept.slot ?? "?"} to the acceptors of its configuration only.` });
  }
  if (commit) {
    out.push({ t: commit.t, title: "Chosen",
      text: `A quorum accepted slot ${commit.slot ?? "?"}: it is chosen, and the leader tells the others (commit).` });
  }
  out.sort((a, b) => a.t - b.t);
  return out;
}
