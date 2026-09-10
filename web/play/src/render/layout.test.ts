import { describe, expect, it } from 'vitest';

import { circleLayout, dotPositions, groupByLink, spreadAlongLink, trim } from './layout';

describe('circleLayout', () => {
  it('starts at the top and goes clockwise', () => {
    const [top, right, bottom, left] = circleLayout(4, { x: 0, y: 0 }, 100);
    expect(top?.x).toBeCloseTo(0);
    expect(top?.y).toBeCloseTo(-100);
    expect(right?.x).toBeCloseTo(100);
    expect(bottom?.y).toBeCloseTo(100);
    expect(left?.x).toBeCloseTo(-100);
  });

  it('puts a lone node in the middle and nothing nowhere', () => {
    expect(circleLayout(1, { x: 5, y: 6 }, 100)).toEqual([{ x: 5, y: 6 }]);
    expect(circleLayout(0, { x: 5, y: 6 }, 100)).toEqual([]);
  });
});

describe('trim', () => {
  it('pulls both ends in by the gap', () => {
    const { from, to } = trim({ x: 0, y: 0 }, { x: 100, y: 0 }, 10);
    expect(from.x).toBeCloseTo(10);
    expect(to.x).toBeCloseTo(90);
  });

  it('leaves a segment shorter than two gaps alone', () => {
    const ends = trim({ x: 0, y: 0 }, { x: 5, y: 0 }, 10);
    expect(ends.from).toEqual({ x: 0, y: 0 });
    expect(ends.to).toEqual({ x: 5, y: 0 });
  });
});

describe('spreadAlongLink', () => {
  it('centres one dot', () => {
    expect(spreadAlongLink(1)).toEqual([0.5]);
    expect(spreadAlongLink(0)).toEqual([]);
  });

  it('centres a run of dots on the middle of the link', () => {
    const three = spreadAlongLink(3);
    expect(three).toHaveLength(3);
    expect((three[0] as number) + (three[2] as number)).toBeCloseTo(1);
    expect(three[1]).toBeCloseTo(0.5);
  });

  it('never lets two dots share a place', () => {
    for (const count of [2, 3, 5, 9, 20]) {
      const spread = spreadAlongLink(count);
      const sorted = [...spread].sort((a, b) => a - b);
      expect(spread).toEqual(sorted);
      for (let i = 1; i < spread.length; i += 1) {
        expect((spread[i] as number) - (spread[i - 1] as number)).toBeGreaterThan(0.005);
      }
    }
  });

  it('keeps every dot clear of the node discs, however many there are', () => {
    for (const count of [2, 4, 12, 40]) {
      for (const t of spreadAlongLink(count)) {
        expect(t).toBeGreaterThanOrEqual(0.12);
        expect(t).toBeLessThanOrEqual(0.88);
      }
    }
  });
});

describe('dotPositions', () => {
  it('places dots along the link, offset to one side of the travel', () => {
    const points = dotPositions({ x: 0, y: 0 }, { x: 100, y: 0 }, 2, 8);
    expect(points).toHaveLength(2);
    for (const point of points) {
      // Travelling +x, "right of travel" is +y.
      expect(point.y).toBeCloseTo(8);
    }
    expect(points[0]?.x).toBeLessThan(points[1]?.x as number);
  });

  it('separates the two directions of the same pair', () => {
    const there = dotPositions({ x: 0, y: 0 }, { x: 100, y: 0 }, 1, 8);
    const back = dotPositions({ x: 100, y: 0 }, { x: 0, y: 0 }, 1, 8);
    expect(there[0]?.y).toBeCloseTo(8);
    expect(back[0]?.y).toBeCloseTo(-8);
  });

  it('does not divide by zero on a self-link', () => {
    const points = dotPositions({ x: 4, y: 4 }, { x: 4, y: 4 }, 1, 8);
    expect(Number.isFinite(points[0]?.x)).toBe(true);
    expect(Number.isFinite(points[0]?.y)).toBe(true);
  });
});

describe('groupByLink', () => {
  it('buckets by the ordered pair', () => {
    const links = groupByLink([
      { from: 1, to: 2 },
      { from: 2, to: 1 },
      { from: 1, to: 2 },
    ]);
    expect(links.get('1->2')).toHaveLength(2);
    expect(links.get('2->1')).toHaveLength(1);
  });
});
