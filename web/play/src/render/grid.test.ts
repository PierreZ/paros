import { describe, expect, it } from 'vitest';

import { cellBadge, columnClass, columnOf, gridCells, gridOf } from './grid';
import { gridPoint } from './layout';
import type { MessageView, NodeView, QuorumSystemView, WorldView } from '../types';

/**
 * One node, with every field the contract has today.
 *
 * The literal is cast rather than typed: the engine adds fields to `NodeView`
 * as later acts land, and a fixture that has to grow with each of them tests
 * nothing extra. What the app reads is checked where the app reads it.
 */
function node(id: number, over: Partial<NodeView> = {}): NodeView {
  return {
    id,
    flavour: 'colocated',
    alive: true,
    role: null,
    attempt: null,
    ballot: null,
    leader: null,
    promised: null,
    accepted: [],
    chosen_index: null,
    first_unchosen: null,
    next_slot: null,
    chosen_gap: null,
    floor: 0,
    election: null,
    open_rounds: [],
    pending_accepts: false,
    read_rounds: [],
    recovery_remaining: 0,
    acceptors: [0, 1, 2, 3, 4, 5],
    quorum_system: 'majority',
    quorum: { kind: 'majority', q1: null, q2: null, rows: null, cols: null },
    grid_cell: null,
    applied: [],
    armed_seam: null,
    ...over,
  } as unknown as NodeView;
}

const GRID: QuorumSystemView = { kind: 'grid', q1: null, q2: null, rows: 2, cols: 3 };

function world(nodes: NodeView[]): Pick<WorldView, 'nodes'> {
  return { nodes };
}

describe('which grid the world runs', () => {
  it('reads the shape the engine reports, never the quorum name', () => {
    const nodes = [node(0, { quorum: GRID, quorum_system: 'grid of 2 by 3' }), node(1)];
    expect(gridOf(world(nodes))).toEqual({ rows: 2, cols: 3 });
  });

  it('runs no grid for a majority or a flexible split', () => {
    expect(gridOf(world([node(0)]))).toBeNull();
    const flexible: QuorumSystemView = { kind: 'flexible', q1: 3, q2: 2, rows: null, cols: null };
    expect(gridOf(world([node(0, { quorum: flexible })]))).toBeNull();
  });

  it('runs no grid when the engine sends no shape with the kind', () => {
    const half = { kind: 'grid', q1: null, q2: null, rows: 2, cols: null } as QuorumSystemView;
    expect(gridOf(world([node(0, { quorum: half })]))).toBeNull();
    const none = world([{ ...node(0), quorum: undefined } as unknown as NodeView]);
    expect(gridOf(none)).toBeNull();
  });
});

describe('where each node sits in the grid', () => {
  it("takes the engine's own cell", () => {
    const nodes = [
      node(0, { quorum: GRID, grid_cell: { row: 0, column: 0 } }),
      node(3, { quorum: GRID, grid_cell: { row: 1, column: 0 } }),
    ];
    const cells = gridCells(world(nodes), { rows: 2, cols: 3 });
    expect(cells.get(0)).toEqual({ row: 0, column: 0 });
    expect(cells.get(3)).toEqual({ row: 1, column: 0 });
  });

  it('falls back to the place in the list, which is how a grid is laid out', () => {
    const nodes = [0, 1, 2, 3, 4, 5].map((id) => node(id, { quorum: GRID }));
    const cells = gridCells(world(nodes), { rows: 2, cols: 3 });
    expect(cells.get(0)).toEqual({ row: 0, column: 0 });
    expect(cells.get(2)).toEqual({ row: 0, column: 2 });
    expect(cells.get(3)).toEqual({ row: 1, column: 0 });
    expect(cells.get(5)).toEqual({ row: 1, column: 2 });
  });
});

describe('the geometry of a grid', () => {
  const shape = { rows: 2, cols: 3 };
  const centre = { x: 600, y: 320 };
  const gap = { x: 180, y: 176 };

  it('centres the whole grid on the centre point', () => {
    const left = gridPoint({ row: 0, column: 0 }, shape, centre, gap);
    const right = gridPoint({ row: 0, column: 2 }, shape, centre, gap);
    expect((left.x + right.x) / 2).toBeCloseTo(centre.x);
    const top = gridPoint({ row: 0, column: 1 }, shape, centre, gap);
    const bottom = gridPoint({ row: 1, column: 1 }, shape, centre, gap);
    expect((top.y + bottom.y) / 2).toBeCloseTo(centre.y);
    expect(top.x).toBeCloseTo(centre.x);
  });

  it('spaces neighbours by exactly one gap', () => {
    const a = gridPoint({ row: 0, column: 0 }, shape, centre, gap);
    const b = gridPoint({ row: 0, column: 1 }, shape, centre, gap);
    const c = gridPoint({ row: 1, column: 0 }, shape, centre, gap);
    expect(b.x - a.x).toBeCloseTo(gap.x);
    expect(c.y - a.y).toBeCloseTo(gap.y);
  });

  it('gives one row and one column exactly one cell in common', () => {
    const row = [0, 1, 2].map((column) => gridPoint({ row: 1, column }, shape, centre, gap));
    const column = [0, 1].map((row) => gridPoint({ row, column: 2 }, shape, centre, gap));
    const shared = row.filter((a) => column.some((b) => a.x === b.x && a.y === b.y));
    expect(shared).toHaveLength(1);
  });

  it('clamps a cell outside the grid instead of drawing off the stage', () => {
    const far = gridPoint({ row: 9, column: 9 }, shape, centre, gap);
    const last = gridPoint({ row: 1, column: 2 }, shape, centre, gap);
    expect(far).toEqual(last);
    const before = gridPoint({ row: -3, column: -3 }, shape, centre, gap);
    expect(before).toEqual(gridPoint({ row: 0, column: 0 }, shape, centre, gap));
  });

  it('puts a one-by-one grid on the centre', () => {
    expect(gridPoint({ row: 0, column: 0 }, { rows: 1, cols: 1 }, centre, gap)).toEqual(centre);
    expect(gridPoint({ row: 0, column: 0 }, { rows: 0, cols: 0 }, centre, gap)).toEqual(centre);
  });
});

describe('the column a message carries', () => {
  it("reads the engine's field, and gives a class only when there is one", () => {
    const accept = { column: 2 } as MessageView;
    expect(columnOf(accept)).toBe(2);
    expect(columnClass(columnOf(accept))).toBe('column-2');
    expect(columnOf({ column: null } as MessageView)).toBeNull();
    expect(columnClass(null)).toBe('');
    expect(columnClass(undefined)).toBe('');
  });

  it('wraps a column beyond the palette instead of losing its colour', () => {
    expect(columnClass(6)).toBe('column-0');
    expect(columnClass(7)).toBe('column-1');
    expect(columnClass(-1)).toBe('');
  });
});

describe('the badge under a node', () => {
  it('names the row and the column', () => {
    expect(cellBadge({ row: 1, column: 2 })).toBe('row 1 · col 2');
    expect(cellBadge(null)).toBeNull();
    expect(cellBadge(undefined)).toBeNull();
  });
});
