// Which nodes have lost their disk.
//
// A crash costs a node its memory and keeps its disk, so the stage draws a
// crashed node from that disk. A **wipe** takes the disk, and what goes with
// it is the promise — the one thing a node may not take back. Such a node does
// not come back, so the stage must not draw it as an ordinary crash.
//
// The engine is asked first: a `NodeView.wiped` field, when the contract gains
// one, is the answer. Until then the fact is read out of two things the engine
// already reports, and never out of protocol reasoning:
//
//  - the action log says the player played a `wipe`, and the label names the
//    node;
//  - that node's disk reads empty.
//
// A level that offers no wipe therefore never draws one, and an undone wipe
// leaves the log with the world.

import type { GameView, NodeView } from '../types';

/** The zero ballot: what an empty disk reports as its promise. */
const NO_PROMISE = '0.0';

/**
 * Whether this node's disk reads empty.
 *
 * Every field is read defensively, and an empty disk is the whole set: no
 * promise above the zero ballot, no accepted record, nothing applied, and no
 * chosen prefix.
 */
export function diskIsEmpty(node: NodeView): boolean {
  const accepted = Array.isArray(node.accepted) ? node.accepted : [];
  const applied = Array.isArray(node.applied) ? node.applied : [];
  const promised = node.promised;
  return (
    accepted.length === 0 &&
    applied.length === 0 &&
    (promised === null || promised === undefined || promised === NO_PROMISE) &&
    (node.chosen_index === null || node.chosen_index === undefined)
  );
}

/** The node ids the log's `wipe` entries name. */
function wipedByLog(view: GameView): Set<number> {
  const named = new Set<number>();
  const log = Array.isArray(view.log) ? view.log : [];
  for (const entry of log) {
    if (entry.kind !== 'wipe') continue;
    const match = /node (\d+)/.exec(typeof entry.label === 'string' ? entry.label : '');
    if (match?.[1] !== undefined) named.add(Number(match[1]));
  }
  return named;
}

/**
 * Every node the player has erased.
 *
 * A node the engine reports as wiped itself is taken as it comes. Otherwise a
 * node counts only when the log names it **and** its disk reads empty, so a
 * node that is merely crashed is never drawn as a lost disk.
 */
export function wipedNodes(view: GameView): Set<number> {
  const nodes = Array.isArray(view.world?.nodes) ? view.world.nodes : [];
  const reported = new Set<number>();
  for (const node of nodes) {
    if ((node as { wiped?: unknown }).wiped === true) reported.add(node.id);
  }
  if (reported.size > 0) return reported;

  const named = wipedByLog(view);
  if (named.size === 0) return reported;
  const erased = new Set<number>();
  for (const node of nodes) {
    if (named.has(node.id) && diskIsEmpty(node)) erased.add(node.id);
  }
  return erased;
}
