// The tour of the Paxos flavors (tour plan §3.2): seven chapters, in course
// order, on five pages. Each chapter plays its runs back to back with the
// player of the story. Before a chapter, one screen (the bridge) can say the
// problem that its flavor solves: the problem line of `src/flavors.ts`, the
// same text as the map of /paxos/, never a text of its own. A chapter that
// no problem line fits has no bridge (today: "Who must answer" and "Reads").
//
// A chapter names its runs in `src/tour/<page>.ts`. Today each chapter is
// made from the recorded runs of its lesson (the base run and the first
// fault). A flavor's own tour run (one world, no restart; `Recorder::tour` in
// paros-wasm) takes their place, one chapter at a time.
//
// `/paxos/tour/` plays every chapter (`src/pages/paxos/tour.astro`, `tourTrace`), and each
// flavor page plays its own chapters in place ("Watch this flavor").
// `check()` runs at build time.

import { LESSONS, pathOf, type Lesson } from "./course";
import { FLAVORS } from "./flavors";
import { SINGLE_DECREE } from "./tour/single-decree";
import { QUORUMS_MAJORITY, QUORUMS_GRIDS } from "./tour/quorums";
import { MULTI_PAXOS_LOG, MULTI_PAXOS_READS } from "./tour/multi-paxos";
import { COMPARTMENTALIZED } from "./tour/compartmentalized";
import { MATCHMAKER } from "./tour/matchmaker";

export interface Chapter {
  /** The lesson of `src/course.ts` that the chapter shows: its title, its
   * page, its exercises ("Your turn") and its runs ("Go deeper"). */
  lesson: string;
  /** The runs of the page that the chapter plays, in order. */
  runs: string[];
  /** The bridge before the chapter: the page of the flavor whose problem
   * line (`src/flavors.ts`) the screen shows. The first chapter has none. */
  bridge?: string;
}

/** The chapters, in course order. */
export const CHAPTERS: Chapter[] = [
  SINGLE_DECREE,
  QUORUMS_MAJORITY,
  MULTI_PAXOS_LOG,
  MULTI_PAXOS_READS,
  QUORUMS_GRIDS,
  COMPARTMENTALIZED,
  MATCHMAKER,
];

interface Beat { says: string; from: number; to: number; question?: unknown; choices?: unknown[] }
interface Run { id: string; label: string; beats?: Beat[]; steps: unknown[]; lanes?: unknown; grid?: unknown; tour?: { chapter: string; title: string }; href?: string }
interface Trace { page: string; lanes: unknown; grid?: unknown; scenarios: Run[] }

/** The lesson of a chapter. */
export function lessonOf(chapter: Chapter): Lesson {
  const lesson = LESSONS.find((l) => l.id === chapter.lesson);
  if (!lesson) throw new Error(`tour: chapter ${chapter.lesson} names no lesson`);
  return lesson;
}

/** The time a run takes on the player at speed 1, in seconds: the reading
 * time of each sentence (public/story.js: 0.33 s per word, from 2.2 s to
 * 9 s) and about 0.7 s for each message that moves. */
export function seconds(run: Run): number {
  let s = 0;
  for (const beat of run.beats ?? []) {
    const words = beat.says.split(/\s+/).filter(Boolean).length;
    s += Math.min(9, Math.max(2.2, 0.33 * words)) + 0.7 * (beat.to - beat.from);
  }
  return s;
}

/** The text of a chapter's bridge: the problem line of its flavor. */
export function bridgeOf(chapter: Chapter): string | undefined {
  if (!chapter.bridge) return undefined;
  const flavor = FLAVORS.find((f) => f.page === chapter.bridge);
  if (!flavor) throw new Error(`tour: chapter ${chapter.lesson} names no flavor ${chapter.bridge} for its bridge`);
  return flavor.problem;
}

/** Fail the build when the chapters and the traces disagree: a chapter names
 * a lesson or a run that does not exist, a run with no beats, two chapters
 * name one lesson, the chapters are not in course order, a tour run in no
 * chapter, in two or in a lesson, a tour run whose mark names another
 * chapter, a bridge that is not the problem line of the chapter's own
 * flavor, or a flavor page with no chapter. */
export function check(traces: Record<string, Trace>): void {
  const placed = new Map<string, string>();
  const inLessons = new Set(LESSONS.flatMap((l) => l.runs.map((r) => `${l.page}/${r}`)));
  let last = -1;
  const lessons = new Set<string>();
  for (const chapter of CHAPTERS) {
    const lesson = lessonOf(chapter);
    if (lessons.has(lesson.id)) throw new Error(`tour: two chapters name the lesson ${lesson.id}`);
    lessons.add(lesson.id);
    const order = LESSONS.indexOf(lesson);
    if (order < last) throw new Error(`tour: chapter ${lesson.id} is out of course order`);
    last = order;
    if (chapter.bridge && chapter.bridge !== lesson.page) {
      throw new Error(`tour: the bridge of chapter ${lesson.id} names the flavor ${chapter.bridge}, not its own ${lesson.page}`);
    }
    bridgeOf(chapter);
    const trace = traces[lesson.page];
    if (!trace) throw new Error(`tour: chapter ${chapter.lesson} names no trace ${lesson.page}`);
    if (!chapter.runs.length) throw new Error(`tour: chapter ${chapter.lesson} plays no run`);
    if (new Set(chapter.runs).size !== chapter.runs.length) {
      throw new Error(`tour: chapter ${chapter.lesson} names one run twice`);
    }
    for (const id of chapter.runs) {
      const run = trace.scenarios.find((s) => s.id === id);
      if (!run) throw new Error(`tour: chapter ${chapter.lesson} names no run ${lesson.page}/${id}`);
      if (!run.beats?.length) throw new Error(`tour: ${lesson.page}/${id} has no story`);
      if (run.tour && run.tour.chapter !== chapter.lesson) {
        throw new Error(`tour: ${lesson.page}/${id} is marked for chapter ${run.tour.chapter}, not ${chapter.lesson}`);
      }
      const key = `${lesson.page}/${id}`;
      if (run.tour && placed.has(key)) throw new Error(`tour: the tour run ${key} is in two chapters`);
      if (run.tour && inLessons.has(key)) throw new Error(`tour: the tour run ${key} is also in a lesson`);
      placed.set(key, chapter.lesson);
    }
    if (chapter === CHAPTERS[0] && chapter.bridge) throw new Error(`tour: the first chapter has a bridge`);
  }
  for (const trace of Object.values(traces)) {
    for (const run of trace.scenarios) {
      if (run.tour && !placed.has(`${trace.page}/${run.id}`)) {
        throw new Error(`tour: the tour run ${trace.page}/${run.id} is in no chapter`);
      }
    }
  }
  const pages = new Set(CHAPTERS.map((c) => lessonOf(c).page));
  for (const lesson of LESSONS) {
    if (lesson.page.startsWith("journal-")) continue;
    if (!pages.has(lesson.page)) throw new Error(`tour: the flavor page ${lesson.page} has no chapter`);
  }
}

/** One chapter as public/tour.js reads it. */
export interface PlayedChapter {
  id: string;
  n: number;
  title: string;
  page: string;
  bridge?: string;
  /** Where "Go deeper" and "Your turn" lead. */
  deeper: string;
  /** The first run of the lesson, which "Go deeper" opens in place on the
   * chapter's own page. */
  lesson_run: string;
  turn?: string;
  /** About how long the chapter plays, in minutes. */
  minutes: number;
  /** The ids of its runs in the trace of the page that plays it. */
  runs: string[];
  /** The id of the next chapter of the whole tour, where the place in the
   * tour moves at the end of this one; none after the last chapter. */
  next?: string;
}

/** The id of a run of chapter `chapter` in the trace of the tour page: the
 * runs of five traces share one list there, and two can have one id. */
export function tourId(chapter: string, run: string): string {
  return `${chapter}--${run}`;
}

/** The chapters for public/tour.js. `url` makes a path of the site absolute.
 * `only` keeps the chapters of one page, whose runs keep their own ids;
 * without it, the run ids are those of `tourTrace`. */
export function played(traces: Record<string, Trace>, url: (path: string) => string, only?: string): PlayedChapter[] {
  return CHAPTERS.map((chapter, k) => ({ chapter, k })).filter(({ chapter }) => !only || lessonOf(chapter).page === only).map(({ chapter, k }) => {
    const lesson = lessonOf(chapter);
    const trace = traces[lesson.page];
    const runs = chapter.runs.map((id) => trace.scenarios.find((s) => s.id === id)!);
    const page = url(pathOf(lesson));
    return {
      id: lesson.id,
      n: k + 1,
      title: lesson.title,
      page: lesson.page,
      bridge: bridgeOf(chapter),
      deeper: `${page}#run=${lesson.runs[0]}&beat=0`,
      lesson_run: lesson.runs[0],
      turn: lesson.labs?.length ? `${page}#lab=${lesson.labs[0]}` : undefined,
      minutes: Math.max(1, Math.round(runs.reduce((s, r) => s + seconds(r), 0) / 60)),
      runs: chapter.runs.map((id) => (only ? id : tourId(lesson.id, id))),
      next: k + 1 < CHAPTERS.length ? lessonOf(CHAPTERS[k + 1]).id : undefined,
    };
  });
}

/** The trace of the tour page: the runs of every chapter, each copied out of
 * its trace with its own lanes and grid and the page it comes from (`href`,
 * where a `more` link leads), under the id of `tourId`. */
export function tourTrace(traces: Record<string, Trace>, url: (path: string) => string): Trace {
  const scenarios = CHAPTERS.flatMap((chapter) => {
    const lesson = lessonOf(chapter);
    const trace = traces[lesson.page];
    return chapter.runs.map((id) => {
      const run = trace.scenarios.find((s) => s.id === id)!;
      return { ...run, id: tourId(lesson.id, id), lanes: run.lanes ?? trace.lanes, grid: run.lanes ? run.grid : trace.grid, href: url(pathOf(lesson)) };
    });
  });
  return { page: "tour", lanes: scenarios[0].lanes, scenarios };
}
