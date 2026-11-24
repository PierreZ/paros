// The chapter of single-decree Paxos (`src/tour.ts`): its own tour run, the
// whole flavor in one world with no restart (`tour()` in
// crates/support/paros-wasm/src/paxos/trace/pages/single_decree.rs).

import type { Chapter } from "../tour";

export const SINGLE_DECREE: Chapter = {
  lesson: "one-value",
  runs: ["tour"],
};
