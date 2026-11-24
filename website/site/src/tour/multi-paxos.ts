// The two chapters of Multi-Paxos (`src/tour.ts`). Day one: the runs of the
// lessons "Many values: the log and its leader" and "Reads and truncation".
// The flavor's own tour runs take their place.

import type { Chapter } from "../tour";

export const MULTI_PAXOS_LOG: Chapter = {
  lesson: "the-log",
  runs: ["base", "gap"],
  bridge: "A journal needs many values in order, and two round trips per record is slow.",
};

export const MULTI_PAXOS_READS: Chapter = {
  lesson: "reads",
  runs: ["read", "read-waits"],
  bridge: "A journal is also read, and a read must see each value that is chosen.",
};
