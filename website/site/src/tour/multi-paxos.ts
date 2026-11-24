// The two chapters of Multi-Paxos (`src/tour.ts`): the flavor's own tour
// runs, one world each (`trace/pages/multi_paxos/tour.rs` in paros-wasm).

import type { Chapter } from "../tour";

export const MULTI_PAXOS_LOG: Chapter = {
  lesson: "the-log",
  runs: ["tour-log"],
  bridge: "multi-paxos",
};

export const MULTI_PAXOS_READS: Chapter = {
  lesson: "reads",
  runs: ["tour-reads"],
  problem: "Every read through the leader is slow, and the log grows without end.",
};
