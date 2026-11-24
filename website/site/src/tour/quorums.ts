// The two chapters of the quorums page (`src/tour.ts`). Each plays the
// flavor's own tour run, one world with no restart (`tour_majority` and
// `tour_grids` in crates/support/paros-wasm/src/paxos/trace/pages/quorums.rs).

import type { Chapter } from "../tour";

export const QUORUMS_MAJORITY: Chapter = {
  lesson: "majority",
  runs: ["tour-majority"],
};

export const QUORUMS_GRIDS: Chapter = {
  lesson: "grids",
  runs: ["tour-grids"],
  bridge: "quorums",
};
