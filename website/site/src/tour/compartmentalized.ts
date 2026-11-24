// The chapter of Compartmentalized Paxos (`src/tour.ts`): the tour run of
// the page, "Split the leader" (`tour()` in paros-wasm's
// `src/paxos/trace/compart.rs`). One world: the leader first does all the
// work, then the replicas, the proxy leaders and the batcher enter one by one
// and take work off it, a proxy leader crashes, and a replica reads with no
// leader.

import type { Chapter } from "../tour";

export const COMPARTMENTALIZED: Chapter = {
  lesson: "compartments",
  runs: ["tour"],
  bridge: "compartmentalized",
};
