// The chapter of Matchmaker Paxos (`src/tour.ts`). Day one: the runs of the
// lesson "Change the acceptors". Its own tour run waits for the segment
// design.

import type { Chapter } from "../tour";

export const MATCHMAKER: Chapter = {
  lesson: "matchmakers",
  runs: ["base", "replace"],
  bridge: "matchmaker",
};
