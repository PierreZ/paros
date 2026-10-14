// The prompt card: the moment the player *is* the node.
//
// A wrong answer never enters the core — the engine judges it against a clone
// of the real role — so the card stays open, the explanation appears, and the
// world does not move. That is the whole interaction: question, the state the
// decision rests on, one button per answer.

import type { Action, GameView } from '../types';
import { h } from '../render/dom';

type Dispatch = (action: Action) => void;

/** The open prompt, or nothing. */
export function renderPrompt(view: GameView, dispatch: Dispatch): HTMLElement | null {
  const prompt = view.prompt;
  if (!prompt) return null;

  const choices = prompt.choices.map((choice) => {
    const button = h('button', { class: 'choice', type: 'button' }, choice.label);
    button.addEventListener('click', () => {
      dispatch({ kind: 'answer', prompt: prompt.id, choice: choice.id });
    });
    return button;
  });

  return h(
    'section',
    { class: 'prompt-card', role: 'group', 'aria-label': 'the question this node has to answer' },
    h('h2', {}, `You are node ${prompt.node}`),
    h('p', { class: 'prompt-question' }, prompt.question),
    prompt.state_summary.length > 0
      ? h(
          'ul',
          { class: 'prompt-state' },
          ...prompt.state_summary.map((line) => h('li', {}, line)),
        )
      : null,
    h('div', { class: 'prompt-choices' }, ...choices),
    prompt.feedback ? h('div', { class: 'prompt-feedback' }, prompt.feedback) : null,
  );
}
