// The chapter of Matchmaker Paxos (`src/tour.ts`): its own tour run, "Change
// the acceptors" (`tour()` in paros-wasm's
// `src/paxos/trace/pages/matchmaker/tour.rs`). One world: the matchmakers
// before Phase 1, a new set of acceptors as a new ballot, the cut of the log
// into segments with a Roll, the crash of the leader, a Phase 1 over the old
// set and the new one, and the retirement of the acceptor that left.

import type { Chapter } from "../tour";

export const MATCHMAKER: Chapter = {
  lesson: "matchmakers",
  runs: ["tour"],
  bridge: "matchmaker",
};
