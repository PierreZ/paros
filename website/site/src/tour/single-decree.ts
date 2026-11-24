// The chapter of single-decree Paxos (`src/tour.ts`). Day one: the runs of
// the lesson "One value". The flavor's own tour run takes their place.

import type { Chapter } from "../tour";

export const SINGLE_DECREE: Chapter = {
  lesson: "one-value",
  runs: ["base", "second-proposer", "acceptor-down"],
};
