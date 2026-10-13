// The acceptor grid: which grid the world runs, and where each node sits in it.
//
// A grid is not a count. A row is a Phase-1 quorum, a column is a Phase-2
// quorum, and each slot's Accept goes to one column. The stage therefore lays
// a grid deployment out as a grid, and every fact it draws comes from the
// engine: `NodeView.quorum` says the shape, `NodeView.grid_cell` says the
// cell, and `MessageView.column` says which column an Accept was addressed to.
//
// Every field is read defensively: an engine that sends no `quorum` at all
// renders the circle the game had before grids existed.

import type { MessageView, NodeView, WorldView } from '../types';
import type { Cell, GridShape } from './layout';

/** How many columns the stage gives a colour of their own. */
export const COLUMN_COLOURS = 6;

function positive(raw: unknown): number | null {
  return typeof raw === 'number' && Number.isFinite(raw) && raw >= 1 ? Math.floor(raw) : null;
}

/**
 * The grid this world runs, or `null` for every deployment that runs none.
 *
 * The shape is read from the nodes' own `quorum`, and the first node that
 * reports a grid decides it. Act IV part one puts one quorum system in force
 * per level; a later act that reconfigures between two systems reports the
 * shape per node, and the stage follows whichever node answers first.
 */
export function gridOf(world: Pick<WorldView, 'nodes'>): GridShape | null {
  const nodes = Array.isArray(world.nodes) ? world.nodes : [];
  for (const node of nodes) {
    const quorum = node.quorum;
    if (!quorum || quorum.kind !== 'grid') continue;
    const rows = positive(quorum.rows);
    const cols = positive(quorum.cols);
    if (rows === null || cols === null) continue;
    return { rows, cols };
  }
  return null;
}

/**
 * Where each node sits in `shape`.
 *
 * `NodeView.grid_cell` is the engine's own answer and is used whenever it is
 * there. A node the engine gives no cell — one outside the configuration, or
 * an engine that does not send the field — falls back to its place in the node
 * list, which is how the configuration itself lays a grid out.
 */
export function gridCells(
  world: Pick<WorldView, 'nodes'>,
  shape: GridShape,
): Map<number, Cell> {
  const cells = new Map<number, Cell>();
  const nodes = Array.isArray(world.nodes) ? world.nodes : [];
  nodes.forEach((node: NodeView, index: number) => {
    const cell = node.grid_cell;
    if (
      cell &&
      typeof cell.row === 'number' &&
      Number.isFinite(cell.row) &&
      typeof cell.column === 'number' &&
      Number.isFinite(cell.column)
    ) {
      cells.set(node.id, { row: cell.row, column: cell.column });
      return;
    }
    const cols = Math.max(1, shape.cols);
    cells.set(node.id, { row: Math.floor(index / cols), column: index % cols });
  });
  return cells;
}

/**
 * The CSS class that carries a column's colour.
 *
 * A message with no column — every message of a deployment that runs no grid,
 * and every message of a grid that is not an `Accept` — gets no class.
 */
export function columnClass(column: number | null | undefined): string {
  if (typeof column !== 'number' || !Number.isFinite(column) || column < 0) return '';
  return `column-${Math.floor(column) % COLUMN_COLOURS}`;
}

/** The badge the stage prints under a node that sits in a grid. */
export function cellBadge(cell: Cell | null | undefined): string | null {
  if (!cell) return null;
  return `row ${cell.row} · col ${cell.column}`;
}

/** The column an in-flight message names, or `null`. */
export function columnOf(message: Pick<MessageView, 'column'>): number | null {
  const column = message.column;
  return typeof column === 'number' && Number.isFinite(column) ? column : null;
}
