// The local progress store.
//
// Everything here is a convenience: a private-mode browser, a cleared site, a
// storage quota — every read and every write is wrapped, and the app runs the
// same with an empty store. Nothing here is protocol state; the engine keeps
// all of that.

import type { AutomationFlag } from './types';

/** The key the whole store lives under. */
export const STORAGE_KEY = 'paros-play/progress/v1';

/** What the player has done with one level. */
export interface LevelProgress {
  /** Whether its goal has been reached at least once. */
  passed: boolean;
  /** The fewest mistakes any passing attempt cost. */
  mistakes: number;
}

/** The whole store. */
export interface Progress {
  levels: Record<string, LevelProgress>;
  /** Automation flags the passed levels have unlocked. */
  unlocked: AutomationFlag[];
}

/** The minimum `Storage` surface this module needs. */
export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

function emptyProgress(): Progress {
  return { levels: {}, unlocked: [] };
}

function backing(storage?: StorageLike): StorageLike | null {
  if (storage) return storage;
  try {
    return globalThis.localStorage ?? null;
  } catch {
    // Site data blocked: the accessor itself throws in some browsers.
    return null;
  }
}

function sanitise(raw: unknown): Progress {
  const progress = emptyProgress();
  if (typeof raw !== 'object' || raw === null) return progress;
  const { levels, unlocked } = raw as { levels?: unknown; unlocked?: unknown };
  if (typeof levels === 'object' && levels !== null) {
    for (const [id, value] of Object.entries(levels as Record<string, unknown>)) {
      if (typeof value !== 'object' || value === null) continue;
      const { passed, mistakes } = value as { passed?: unknown; mistakes?: unknown };
      progress.levels[id] = {
        passed: passed === true,
        mistakes: typeof mistakes === 'number' && Number.isFinite(mistakes) ? mistakes : 0,
      };
    }
  }
  if (Array.isArray(unlocked)) {
    progress.unlocked = unlocked.filter((flag): flag is AutomationFlag => typeof flag === 'string');
  }
  return progress;
}

/** Read the store. Never throws; an unreadable store is an empty one. */
export function load(storage?: StorageLike): Progress {
  const store = backing(storage);
  if (!store) return emptyProgress();
  try {
    const raw = store.getItem(STORAGE_KEY);
    if (raw === null) return emptyProgress();
    return sanitise(JSON.parse(raw));
  } catch {
    return emptyProgress();
  }
}

/** Write the store. Never throws; an unwritable store is silently dropped. */
export function save(progress: Progress, storage?: StorageLike): void {
  const store = backing(storage);
  if (!store) return;
  try {
    store.setItem(STORAGE_KEY, JSON.stringify(progress));
  } catch {
    // A full or read-only store costs the player their bookmarks, not their game.
  }
}

/**
 * Record that a level's goal was reached, with the automation it unlocks.
 *
 * A second pass with fewer mistakes improves the record; a worse one does not
 * spoil it.
 */
export function recordPass(
  id: string,
  mistakes: number,
  unlocks: readonly AutomationFlag[],
  storage?: StorageLike,
): Progress {
  const progress = load(storage);
  const previous = progress.levels[id];
  progress.levels[id] = {
    passed: true,
    mistakes: previous?.passed ? Math.min(previous.mistakes, mistakes) : mistakes,
  };
  for (const flag of unlocks) {
    if (!progress.unlocked.includes(flag)) progress.unlocked.push(flag);
  }
  save(progress, storage);
  return progress;
}

/** Record an attempt that has not (yet) reached its goal. */
export function recordAttempt(id: string, mistakes: number, storage?: StorageLike): Progress {
  const progress = load(storage);
  const previous = progress.levels[id];
  if (previous?.passed) {
    return progress;
  }
  progress.levels[id] = { passed: false, mistakes };
  save(progress, storage);
  return progress;
}

/** Whether a level has been passed. */
export function isPassed(id: string, storage?: StorageLike): boolean {
  return load(storage).levels[id]?.passed === true;
}

/** Forget everything. */
export function clear(storage?: StorageLike): Progress {
  const progress = emptyProgress();
  save(progress, storage);
  return progress;
}
