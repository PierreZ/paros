// Which nodes have lost their disk.
//
// A crash costs a node its memory and keeps its disk, so the stage draws a
// crashed node from that disk. A **wipe** takes the disk, and what goes with
// it is the promise — the one thing a node may not take back. Such a node does
// not come back, so the stage must not draw it as an ordinary crash. The
// engine reports the fact itself (`NodeView.wiped`), and the stage reads it
// from nowhere else.

import type { GameView } from '../types';

/** Every node the player has erased, as the engine reports it. */
export function wipedNodes(view: GameView): Set<number> {
  const nodes = Array.isArray(view.world?.nodes) ? view.world.nodes : [];
  const erased = new Set<number>();
  for (const node of nodes) {
    if (node.wiped === true) erased.add(node.id);
  }
  return erased;
}
