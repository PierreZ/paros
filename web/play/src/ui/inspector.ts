// The node inspector: the whole of one node, on a screen that cannot draw it.
//
// A wide stage draws every node's accepted log beside its disc. A narrow stage
// has no room for six columns of slot boxes, so it folds each log into a
// summary and puts the column here: the player taps a node, and the card under
// the stage shows the log, the floor, the hole and every label the picture
// clipped.
//
// The card reads the same fields the stage reads. It works nothing out: a slot
// is chosen because the engine says so, and the meta lines are the stage's own
// `nodeMeta`.

import type { NodeView } from '../types';
import { h } from '../render/dom';
import { nodeMeta, roleLabel, slotClass, slotLabel } from '../render/stage';
import type { Cell } from '../render/layout';

/** What the card needs beside the node itself. */
export interface InspectorDeps {
  node: NodeView;
  cell: Cell | null;
  wiped: boolean;
  matchmade: boolean;
  close: () => void;
}

/**
 * The accepted log, as a list of rows.
 *
 * The stage's slot box and this row carry the same classes, so a chosen slot
 * is the same green in both.
 */
function logRows(node: NodeView): HTMLElement {
  const slots = Array.isArray(node.accepted) ? node.accepted : [];
  if (slots.length === 0) {
    return h('p', { class: 'empty' }, 'This node accepted no slot yet.');
  }
  return h(
    'ul',
    { class: 'inspector-log' },
    ...slots.map((slot) =>
      h(
        'li',
        { class: `inspector-slot ${slotClass(slot)}` },
        h('span', { class: 'inspector-slot-name' }, slotLabel(slot)),
        h('span', { class: 'inspector-slot-ballot' }, `ballot ${slot.ballot ?? 'none'}`),
        h(
          'span',
          { class: 'inspector-slot-state' },
          slot.applied ? 'applied' : slot.chosen ? 'chosen' : 'open',
        ),
      ),
    ),
  );
}

/** The card. */
export function renderInspector(deps: InspectorDeps): HTMLElement {
  const { node } = deps;
  const close = h(
    'button',
    { class: 'control-button', type: 'button', 'aria-label': 'Close the node' },
    'Close',
  );
  close.addEventListener('click', deps.close);

  const floor = typeof node.floor === 'number' && node.floor > 0 ? node.floor : null;
  const meta = nodeMeta(node, deps.cell, deps.wiped, deps.matchmade);

  return h(
    'section',
    { class: 'inspector', role: 'group', 'aria-label': `node ${node.id}` },
    h(
      'header',
      { class: 'inspector-head' },
      h('h2', {}, `Node ${node.id} · ${roleLabel(node, deps.wiped)}`),
      close,
    ),
    floor === null
      ? null
      : h('p', { class: 'small' }, `The floor is ${floor}. Every slot before it is deleted here.`),
    node.chosen_gap
      ? h(
          'p',
          { class: 'inspector-hole' },
          `Slot ${node.chosen_gap.hole} is undecided, and slot ${node.chosen_gap.highest} is chosen.`,
        )
      : null,
    logRows(node),
    meta.length === 0
      ? null
      : h('ul', { class: 'inspector-meta' }, ...meta.map((line) => h('li', {}, line))),
  );
}
