// The two chapters of Multi-Paxos (`src/tour.ts`). Day one: the runs of the
// lessons "Many values: the log and its leader" and "Reads and truncation".
// The flavor's own tour runs take their place.

import type { Chapter } from "../tour";

export const MULTI_PAXOS_LOG: Chapter = {
  lesson: "the-log",
  runs: ["base", "gap"],
  bridge: "multi-paxos",
};

export const MULTI_PAXOS_READS: Chapter = {
  lesson: "reads",
  runs: ["read", "read-waits"],
};
