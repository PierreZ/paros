// The caption under the stage: what the last move did, in Paxos words.
//
// `GameView.narration` is exactly that — the engine clears and rebuilds it on
// every action — so the caption is a straight render of it: the newest line
// emphasised, a `violation` red, the older lines of the same move behind it.
// The whole stream lives in the panel's log.

import type { GameView } from '../types';
import { latestNarration } from '../narration';
import { h } from '../render/dom';

const SHOWN = 4;

/** The caption block. */
export function renderCaption(view: GameView): HTMLElement {
  const lines = latestNarration(view, SHOWN);
  if (lines.length === 0) {
    const fallback =
      view.goal.status === 'open' ? view.goal.detail : goalHeadline(view);
    return h('div', { class: 'caption' }, h('p', { class: 'caption-line latest' }, fallback));
  }
  return h(
    'div',
    { class: 'caption' },
    ...lines.map((entry, index) =>
      h(
        'p',
        {
          class: `caption-line kind-${entry.kind}${index === lines.length - 1 ? ' latest' : ''}`,
        },
        entry.text,
      ),
    ),
  );
}

function goalHeadline(view: GameView): string {
  return view.goal.status === 'reached'
    ? `Goal reached. ${view.goal.detail}`
    : view.goal.detail;
}
