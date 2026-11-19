// The Paxos course: the lessons in order, each with what the reader can do
// at its end and the recorded runs that teach it. A lesson is a group of runs
// of one page, never a page of its own.
//
// `src/pages/paxos/index.astro` draws the map, `src/layouts/Doc.astro` gives
// each page its lessons, and `public/story.js` offers the next run of the
// lesson at the end of each run. `public/course.js` keeps the progress in the
// reader's browser only.
//
// `check()` runs at build time: every run a lesson names exists, and every
// run of a page with a story is in exactly one lesson. A new run needs a
// place here, or the build fails.

export interface Lesson {
  /** A stable id, used in the address and in the progress. */
  id: string;
  title: string;
  /** What the reader can do at the end of the lesson. */
  goal: string;
  /** The trace of the page: `src/traces/<page>.json`, `paxos/<page>/`. */
  page: string;
  /** The runs of the page, in the order of the lesson. */
  runs: string[];
  /** The exercises at the end of the lesson ("your turn"), by their id in
   * `crates/support/paros-wasm/src/paxos/lab.rs`. The page that holds the
   * lesson draws them under `## Your turn`. */
  labs?: string[];
}

export const LESSONS: Lesson[] = [
  {
    id: "one-value",
    title: "One value",
    goal: "say why a later proposer must take the value that it finds, and why an acceptor writes to disk before it answers.",
    page: "single-decree",
    runs: ["base", "acceptor-down", "second-proposer", "lost-message", "acceptor-restart", "repeated-answers", "accept-is-a-promise", "fenced-out", "duel", "break-the-rule"],
    labs: ["vote-not-chosen", "two-values", "duel"],
  },
  {
    id: "majority",
    title: "Who must answer",
    goal: "say which two sets of acceptors must share an acceptor, and count how many acceptors can fail.",
    page: "quorums",
    runs: ["majority", "break-the-rule"],
  },
  {
    id: "the-log",
    title: "Many values: the log and its leader",
    goal: "follow one election and many writes, and say how a new leader recovers the log of a dead one.",
    page: "multi-paxos",
    runs: ["base", "leader-crash", "gap", "lost-commit", "lagging", "seam-before-sync", "seam-after-sync", "stale-nack", "stale-promise", "recovery-crash", "handoff", "handoff-lost"],
  },
  {
    id: "reads",
    title: "Reads and truncation",
    goal: "say why a read asks a quorum and not the leader, and what a node does when the log that it needs is gone.",
    page: "multi-paxos",
    runs: ["read", "read-waits", "truncate", "trimmed"],
  },
  {
    id: "grids",
    title: "Grids",
    goal: "pick a write quorum and an election quorum on a grid, and say what each shape costs.",
    page: "quorums",
    runs: ["base", "election", "column-down", "stuck-column", "two-shapes", "partitions"],
  },
  {
    id: "compartments",
    title: "Split the leader",
    goal: "name each role of a compartmentalized journal, and say what breaks when it stops.",
    page: "compartmentalized",
    runs: ["colocated", "base", "batcher", "read", "proxy-crash", "lost-vote", "proxy-nack"],
  },
  {
    id: "matchmakers",
    title: "Change the acceptors",
    goal: "follow one reconfiguration, and say why the old configurations can be forgotten.",
    page: "matchmaker",
    runs: ["base", "leader-crash", "replace", "retire", "break-the-rule"],
  },
];

interface TraceLike { scenarios: { id: string; label: string; beats?: unknown[] }[] }

/** The lessons of one page, in course order, with their number from 1. */
export function lessonsOf(page: string): (Lesson & { n: number })[] {
  return LESSONS.map((l, i) => ({ ...l, n: i + 1 })).filter((l) => l.page === page);
}

/** Fail the build when a lesson and the traces disagree. */
export function check(traces: Record<string, TraceLike>): void {
  const placed = new Map<string, string>();
  for (const lesson of LESSONS) {
    const trace = traces[lesson.page];
    if (!trace) throw new Error(`course: lesson ${lesson.id} names no trace ${lesson.page}`);
    for (const run of lesson.runs) {
      if (!trace.scenarios.some((s) => s.id === run)) throw new Error(`course: lesson ${lesson.id} names no run ${lesson.page}/${run}`);
      const key = `${lesson.page}/${run}`;
      if (placed.has(key)) throw new Error(`course: ${key} is in lessons ${placed.get(key)} and ${lesson.id}`);
      placed.set(key, lesson.id);
    }
  }
  for (const [page, trace] of Object.entries(traces)) {
    for (const s of trace.scenarios) {
      if (s.beats?.length && !placed.has(`${page}/${s.id}`)) throw new Error(`course: the run ${page}/${s.id} is in no lesson: add it to src/course.ts`);
    }
  }
}
