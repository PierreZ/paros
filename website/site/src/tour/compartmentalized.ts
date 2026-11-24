// The chapter of Compartmentalized Paxos (`src/tour.ts`). Day one: the runs
// of the lesson "Split the leader". The flavor's own tour run takes their
// place.

import type { Chapter } from "../tour";

export const COMPARTMENTALIZED: Chapter = {
  lesson: "compartments",
  runs: ["colocated", "base", "proxy-crash"],
  bridge: "compartmentalized",
};
