// The wire list: every in-flight message as a row with buttons.
//
// The stage's dots are the fast way to play; this table is the way to play
// with a keyboard, with a screen reader, or by reading rather than aiming —
// and it is the only place the whole summary of a message is legible.

import type { Action, ActionKind, GameView, MessageView } from '../types';
import { h } from '../render/dom';
import { phaseClass } from '../render/stage';

type Dispatch = (action: Action) => void;

function button(
  label: string,
  title: string,
  allowed: boolean,
  onClick: () => void,
): HTMLButtonElement | null {
  if (!allowed) return null;
  const element = h('button', { class: 'wire-button', type: 'button', title }, label);
  element.addEventListener('click', onClick);
  return element;
}

function row(
  message: MessageView,
  allowed: readonly ActionKind[],
  dispatch: Dispatch,
): HTMLTableRowElement {
  return h(
    'tr',
    { class: 'wire-row', 'data-msg': message.id },
    h(
      'td',
      { class: 'wire-route' },
      h('span', { class: `wire-swatch ${phaseClass(message.phase)}` }),
      `${message.from} → ${message.to}`,
    ),
    h('td', { class: 'wire-summary', title: message.summary }, message.summary),
    h(
      'td',
      { class: 'wire-actions' },
      button('Deliver', 'Hand this message to its addressee', allowed.includes('deliver'), () =>
        dispatch({ kind: 'deliver', id: message.id }),
      ),
      button('Drop', 'Lose it on the wire', allowed.includes('drop'), () =>
        dispatch({ kind: 'drop', id: message.id }),
      ),
      button('Dup', 'Deliver it twice', allowed.includes('duplicate'), () =>
        dispatch({ kind: 'duplicate', id: message.id }),
      ),
    ),
  );
}

/** The wire list. */
export function renderWire(view: GameView, dispatch: Dispatch): HTMLElement {
  const wire = view.world.wire;
  const allowed = view.level.allowed_actions;
  if (wire.length === 0) {
    return h(
      'section',
      { class: 'wire-list' },
      h('h2', {}, 'On the wire'),
      h('p', { class: 'empty' }, 'Nothing is in flight. Make a move to put a message on the wire.'),
    );
  }
  return h(
    'section',
    { class: 'wire-list' },
    h('h2', {}, `On the wire (${wire.length})`),
    h(
      'table',
      { class: 'wire-table' },
      h(
        'tbody',
        {},
        ...wire.map((message) => row(message, allowed, dispatch)),
      ),
    ),
  );
}
