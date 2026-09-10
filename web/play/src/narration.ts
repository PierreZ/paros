// Reading the narration.
//
// The engine derives a line from every transition it makes, in Paxos words
// with this transition's own numbers. Two places hold them, and they mean
// different things:
//
// - `GameView.narration` is what the **last action** did. It is cleared and
//   rebuilt on every move, so it is exactly the caption under the stage.
// - `ActionView.narration` keeps each action's lines in the log, so the panel
//   can show the whole stream without the frontend accumulating anything.
//
// Both are read defensively: an engine built before the field landed simply
// narrates nothing, and the app renders the same.

import type { GameView, NarrationView } from './types';

function clean(raw: unknown): NarrationView[] {
  if (!Array.isArray(raw)) return [];
  const out: NarrationView[] = [];
  for (const entry of raw) {
    if (typeof entry !== 'object' || entry === null) continue;
    const { kind, text } = entry as { kind?: unknown; text?: unknown };
    if (typeof text !== 'string') continue;
    out.push({ kind: (typeof kind === 'string' ? kind : 'info') as NarrationView['kind'], text });
  }
  return out;
}

/** What the last action did, oldest line first. */
export function narration(view: GameView): readonly NarrationView[] {
  return clean((view as { narration?: unknown }).narration);
}

/** Every line the whole game has produced, oldest first. */
export function narrationStream(view: GameView): readonly NarrationView[] {
  const log = (view as { log?: unknown }).log;
  if (!Array.isArray(log)) return [];
  return log.flatMap((entry) => clean((entry as { narration?: unknown }).narration));
}

/** The last `count` lines of the last action. */
export function latestNarration(view: GameView, count: number): readonly NarrationView[] {
  const all = narration(view);
  return count >= all.length ? all : all.slice(all.length - count);
}
