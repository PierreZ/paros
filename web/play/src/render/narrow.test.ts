import { describe, expect, it } from 'vitest';

import {
  NARROW_BREAKPOINT,
  NARROW_LINE,
  NARROW_LABEL_HALF,
  NARROW_LABEL_MIN,
  NARROW_NODE_RADIUS,
  clipLabel,
  narrowBoard,
  narrowChars,
  narrowLayout,
  ringRadius,
  type NarrowNode,
} from './narrow';

/** A phone at 375 CSS pixels: the width the layout is judged at. */
const PHONE = 375;

/** The slack a bound that is met exactly needs, after the floating-point maths. */
const EPSILON = 0.001;

function ring(count: number, lines = 4): NarrowNode[] {
  return Array.from({ length: count }, (_, id) => ({ id, lines }));
}

function grid(rows: number, cols: number, lines = 4): NarrowNode[] {
  const nodes: NarrowNode[] = [];
  for (let index = 0; index < rows * cols; index += 1) {
    nodes.push({ id: index, cell: { row: Math.floor(index / cols), column: index % cols }, lines });
  }
  return nodes;
}

describe('the board', () => {
  it('never claims more width than the container gives it', () => {
    for (const width of [320, 360, 375, 414, 599]) {
      expect(narrowBoard(width)).toBeLessThan(width);
    }
  });

  it('holds a floor and a ceiling, so a hidden or a huge container still draws', () => {
    expect(narrowBoard(0)).toBe(260);
    expect(narrowBoard(Number.NaN)).toBe(260);
    expect(narrowBoard(4000)).toBe(NARROW_BREAKPOINT);
  });
});

describe('a label line', () => {
  it('is clipped to the room the picture leaves, and says that it was', () => {
    expect(clipLabel('promised 1.0', 62)).toBe('promised 1.0');
    const cut = clipLabel('matchmakers m0,m1,m2 · gen 3', NARROW_LABEL_MIN);
    expect(cut.endsWith('…')).toBe(true);
    expect(cut).toHaveLength(narrowChars(NARROW_LABEL_MIN));
  });

  it('keeps at least six characters, whatever the room is', () => {
    expect(narrowChars(0)).toBe(6);
  });
});

describe('the ring', () => {
  it('grows with the node count, so three nodes are not all empty middle', () => {
    expect(ringRadius(PHONE, 3)).toBeLessThan(ringRadius(PHONE, 6));
  });

  it('is nothing for one node and for none', () => {
    expect(ringRadius(PHONE, 1)).toBe(0);
    expect(ringRadius(PHONE, 0)).toBe(0);
  });
});

describe('narrowLayout, on a ring', () => {
  it('draws one unit per pixel, so 11-unit text is 11-pixel text', () => {
    const stage = narrowLayout(PHONE, ring(3));
    // The browser scales the viewBox up to the container, never down.
    expect(stage.width).toBeLessThanOrEqual(PHONE);
    expect(PHONE / stage.width).toBeGreaterThanOrEqual(1);
  });

  it('keeps every node, and every label under it, inside the board', () => {
    for (const count of [1, 2, 3, 4, 5, 6]) {
      const stage = narrowLayout(PHONE, ring(count));
      for (const point of stage.points.values()) {
        expect(point.x - stage.nodeRadius).toBeGreaterThanOrEqual(0);
        expect(point.x + stage.nodeRadius).toBeLessThanOrEqual(stage.width);
        expect(point.x - stage.labelHalf).toBeGreaterThanOrEqual(-1);
        expect(point.x + stage.labelHalf).toBeLessThanOrEqual(stage.width + 1);
        expect(point.y + stage.nodeRadius).toBeLessThanOrEqual(stage.height);
      }
    }
  });

  it('never prints one node label over another', () => {
    for (const width of [320, 375, 414, 560]) {
      for (const count of [2, 3, 4, 5, 6]) {
        const stage = narrowLayout(width, ring(count));
        const points = [...stage.points.values()];
        const tall = 2 * stage.nodeRadius + 14 + 4 * NARROW_LINE;
        for (let i = 0; i < points.length; i += 1) {
          for (let j = i + 1; j < points.length; j += 1) {
            const a = points[i]!;
            const b = points[j]!;
            const apart =
              Math.abs(a.x - b.x) >= 2 * stage.labelHalf - EPSILON ||
              Math.abs(a.y - b.y) >= tall - EPSILON;
            expect(apart).toBe(true);
          }
        }
      }
    }
  });

  it('never lets two discs touch', () => {
    for (const count of [2, 3, 4, 5, 6]) {
      const stage = narrowLayout(PHONE, ring(count));
      const points = [...stage.points.values()];
      for (let i = 0; i < points.length; i += 1) {
        for (let j = i + 1; j < points.length; j += 1) {
          const a = points[i]!;
          const b = points[j]!;
          expect(Math.hypot(a.x - b.x, a.y - b.y)).toBeGreaterThan(2 * stage.nodeRadius);
        }
      }
    }
  });

  it('puts a lone node in the middle and draws nothing for none', () => {
    const one = narrowLayout(PHONE, ring(1));
    expect(one.points.get(0)).toEqual(one.centre);
    const none = narrowLayout(PHONE, []);
    expect(none.points.size).toBe(0);
    expect(none.height).toBeGreaterThan(0);
    expect(none.labelHalf).toBe(NARROW_LABEL_HALF);
  });
});

describe('narrowLayout, on a grid', () => {
  it('fits the columns into the width, whatever the shape', () => {
    for (const [rows, cols] of [
      [2, 2],
      [2, 3],
      [3, 2],
    ] as const) {
      const stage = narrowLayout(PHONE, grid(rows, cols), { grid: { rows, cols } });
      expect(stage.gap).not.toBeNull();
      for (const point of stage.points.values()) {
        expect(point.x - stage.nodeRadius).toBeGreaterThanOrEqual(0);
        expect(point.x + stage.nodeRadius).toBeLessThanOrEqual(stage.width);
      }
    }
  });

  it('stacks the rows much closer than the wide stage does', () => {
    const stage = narrowLayout(PHONE, grid(2, 3), { grid: { rows: 2, cols: 3 } });
    // The wide grid puts 176 units between two rows, because a row there holds
    // a six-slot log column.
    expect(stage.gap?.y).toBeLessThan(176);
  });

  it('reads the cell the engine gave, never the node order', () => {
    const nodes: NarrowNode[] = [
      { id: 7, cell: { row: 1, column: 1 }, lines: 2 },
      { id: 8, cell: { row: 0, column: 0 }, lines: 2 },
    ];
    const stage = narrowLayout(PHONE, nodes, { grid: { rows: 2, cols: 2 } });
    expect(stage.points.get(8)!.y).toBeLessThan(stage.points.get(7)!.y);
    expect(stage.points.get(8)!.x).toBeLessThan(stage.points.get(7)!.x);
  });

  it('never prints one node label over another', () => {
    for (const [rows, cols] of [
      [2, 2],
      [2, 3],
      [3, 2],
    ] as const) {
      const stage = narrowLayout(PHONE, grid(rows, cols), { grid: { rows, cols } });
      const points = [...stage.points.values()];
      const tall = 2 * stage.nodeRadius + 14 + 4 * NARROW_LINE;
      for (let i = 0; i < points.length; i += 1) {
        for (let j = i + 1; j < points.length; j += 1) {
          const a = points[i]!;
          const b = points[j]!;
          const apart =
            Math.abs(a.x - b.x) >= 2 * stage.labelHalf - EPSILON ||
            Math.abs(a.y - b.y) >= tall - EPSILON;
          expect(apart).toBe(true);
        }
      }
    }
  });
});

describe('the matchmaker band', () => {
  it('sits under the cluster, and never beside it', () => {
    const stage = narrowLayout(PHONE, ring(3), { matchmakers: 3 });
    expect(stage.bandTitle).not.toBeNull();
    expect(stage.bandTitle!).toBeGreaterThanOrEqual(stage.clusterBottom);
    for (const square of stage.band) {
      expect(square.y).toBeGreaterThan(stage.clusterBottom);
      expect(square.x).toBeGreaterThan(0);
      expect(square.x).toBeLessThan(stage.width);
    }
  });

  it('spreads the squares evenly and keeps them on the board', () => {
    for (const count of [1, 2, 3, 4]) {
      const stage = narrowLayout(PHONE, ring(3), { matchmakers: count });
      expect(stage.band).toHaveLength(count);
      for (const square of stage.band) {
        expect(square.x - stage.matchmakerSize / 2).toBeGreaterThanOrEqual(0);
        expect(square.x + stage.matchmakerSize / 2).toBeLessThanOrEqual(stage.width);
        expect(square.y + stage.matchmakerSize / 2).toBeLessThan(stage.height);
      }
    }
  });

  it('gives a square more room for a line than a node gets', () => {
    // A registry row is the longest line the stage prints, and a square owns
    // half the band, not half a cluster.
    const stage = narrowLayout(PHONE, ring(4), { matchmakers: 2 });
    expect(stage.bandLabelHalf).toBeGreaterThan(stage.labelHalf);
    expect(narrowChars(stage.bandLabelHalf)).toBeGreaterThanOrEqual(
      '2.0 → 0,1,2 · change'.length,
    );
  });

  it('keeps a line inside a square\'s share of the band, however many there are', () => {
    for (const count of [1, 2, 3, 4]) {
      const stage = narrowLayout(PHONE, ring(3), { matchmakers: count });
      expect(2 * stage.bandLabelHalf).toBeLessThanOrEqual(stage.width / count);
      expect(stage.bandLabelHalf).toBeGreaterThanOrEqual(NARROW_LABEL_MIN);
    }
  });

  it('makes the stage taller, and draws no band where none is named', () => {
    const plain = narrowLayout(PHONE, ring(3));
    const matchmade = narrowLayout(PHONE, ring(3), { matchmakers: 2 });
    expect(plain.band).toHaveLength(0);
    expect(plain.bandTitle).toBeNull();
    expect(plain.bandLabelHalf).toBe(NARROW_LABEL_HALF);
    expect(matchmade.height).toBeGreaterThan(plain.height);
  });
});

describe('the node disc', () => {
  it('is smaller than the wide stage draws, because the text is not', () => {
    expect(NARROW_NODE_RADIUS).toBeLessThan(32);
    expect(narrowLayout(PHONE, ring(6)).nodeRadius).toBe(NARROW_NODE_RADIUS);
  });
});
