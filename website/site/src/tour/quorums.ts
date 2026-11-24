// The two chapters of the quorums page (`src/tour.ts`). Day one: the runs of
// the lessons "Who must answer" and "Grids". The flavor's own tour runs take
// their place.

import type { Chapter } from "../tour";

export const QUORUMS_MAJORITY: Chapter = {
  lesson: "majority",
  runs: ["majority"],
};

export const QUORUMS_GRIDS: Chapter = {
  lesson: "grids",
  runs: ["base", "column-down"],
  bridge: "quorums",
};
