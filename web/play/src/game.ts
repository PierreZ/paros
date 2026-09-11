// The typed wrapper over the wasm engine.
//
// The wasm surface speaks JSON strings: every call returns a `GameView` or an
// `ErrorView`. Everything below turns that into typed values, keeps the last
// refusal for the UI to show, and is the only place in the app that touches
// `WasmGame`.

import type { Action, ErrorView, GameView, LevelSummary } from './types';

/** The subset of `WasmGame` this app uses. */
export interface Engine {
  act(actionJson: string): string;
  undo(): string;
  reset(): string;
  view(): string;
}

/** Either a rendered frame or the refusal that replaced it. */
export type Outcome = { ok: true; view: GameView } | { ok: false; error: ErrorView };

/** Whether a decoded reply is the engine's refusal shape. */
export function isErrorView(value: unknown): value is ErrorView {
  if (typeof value !== 'object' || value === null) return false;
  const candidate = value as { code?: unknown; error?: unknown };
  return typeof candidate.code === 'string' && typeof candidate.error === 'string';
}

/** Decode one reply from the engine. */
export function decode(json: string): Outcome {
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch (cause) {
    return {
      ok: false,
      error: { code: 'bad_json', error: `the engine returned something unreadable: ${String(cause)}` },
    };
  }
  if (isErrorView(parsed)) return { ok: false, error: parsed };
  if (typeof parsed !== 'object' || parsed === null || !('level' in parsed)) {
    return { ok: false, error: { code: 'bad_view', error: 'the engine returned a frame with no level' } };
  }
  return { ok: true, view: parsed as GameView };
}

/** Decode the level list `WasmGame.levels()` hands back. */
export function decodeLevels(json: string): LevelSummary[] {
  const parsed: unknown = JSON.parse(json);
  if (!Array.isArray(parsed)) return [];
  return parsed as LevelSummary[];
}

/**
 * One level in progress.
 *
 * `view` is always the last frame the engine produced: a refused action leaves
 * it untouched and lands in `lastError` instead, which is exactly how the panel
 * renders a refusal without losing the board.
 */
export class Game {
  readonly #engine: Engine;
  #view: GameView;
  #lastError: ErrorView | null = null;

  constructor(engine: Engine) {
    this.#engine = engine;
    const outcome = decode(engine.view());
    if (!outcome.ok) throw new Error(outcome.error.error);
    this.#view = outcome.view;
  }

  /** The last frame. */
  get view(): GameView {
    return this.#view;
  }

  /** The last refusal, or `null` if the last move landed. */
  get lastError(): ErrorView | null {
    return this.#lastError;
  }

  /** Forget the last refusal (the player acknowledged it). */
  clearError(): void {
    this.#lastError = null;
  }

  /** Play one move. Returns false when the engine refused it. */
  act(action: Action): boolean {
    return this.#absorb(this.#engine.act(JSON.stringify(action)));
  }

  /** Undo the last move. */
  undo(): boolean {
    return this.#absorb(this.#engine.undo());
  }

  /** Start the level again. */
  reset(): boolean {
    return this.#absorb(this.#engine.reset());
  }

  #absorb(json: string): boolean {
    const outcome = decode(json);
    if (outcome.ok) {
      this.#view = outcome.view;
      this.#lastError = null;
      return true;
    }
    this.#lastError = outcome.error;
    return false;
  }
}
