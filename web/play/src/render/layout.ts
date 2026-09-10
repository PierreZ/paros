// Stage geometry, as pure functions.
//
// Nothing here reads the DOM or the view: it takes counts and points and
// returns points, which is why the message spreading — the one piece with a
// rule worth getting wrong — is unit-tested.

/** A point on the stage, in SVG user units. */
export interface Point {
  readonly x: number;
  readonly y: number;
}

/**
 * `count` nodes evenly spaced on a circle, starting at the top and going
 * clockwise.
 */
export function circleLayout(count: number, centre: Point, radius: number): Point[] {
  if (count <= 0) return [];
  if (count === 1) return [centre];
  const points: Point[] = [];
  for (let i = 0; i < count; i += 1) {
    const angle = -Math.PI / 2 + (2 * Math.PI * i) / count;
    points.push({
      x: centre.x + radius * Math.cos(angle),
      y: centre.y + radius * Math.sin(angle),
    });
  }
  return points;
}

/** Shorten a segment at both ends so it starts and stops outside the node discs. */
export function trim(from: Point, to: Point, gap: number): { from: Point; to: Point } {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const length = Math.hypot(dx, dy);
  if (length <= gap * 2) return { from, to };
  const ux = dx / length;
  const uy = dy / length;
  return {
    from: { x: from.x + ux * gap, y: from.y + uy * gap },
    to: { x: to.x - ux * gap, y: to.y - uy * gap },
  };
}

/**
 * Where `count` dots sit along a link, as fractions of its length.
 *
 * They are centred on the middle of the link and spaced so they never overlap,
 * and the spread is clamped to stay inside `[0.12, 0.88]` however many messages
 * pile up — a dot that slid onto a node disc would be unclickable.
 */
export function spreadAlongLink(count: number, spacing = 0.11): number[] {
  if (count <= 0) return [];
  if (count === 1) return [0.5];
  const lowest = 0.12;
  const highest = 0.88;
  const used = Math.min(spacing, (highest - lowest) / (count - 1));
  const start = 0.5 - (used * (count - 1)) / 2;
  return Array.from({ length: count }, (_, i) => start + used * i);
}

/**
 * The dots in flight on one link.
 *
 * Each is pushed `lateral` units to the right of the direction of travel, so
 * the two directions of a pair of nodes never draw on top of each other.
 */
export function dotPositions(from: Point, to: Point, count: number, lateral = 8): Point[] {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const length = Math.hypot(dx, dy) || 1;
  const nx = -dy / length;
  const ny = dx / length;
  return spreadAlongLink(count).map((t) => ({
    x: from.x + dx * t + nx * lateral,
    y: from.y + dy * t + ny * lateral,
  }));
}

/** Group messages by the ordered pair they travel between. */
export function groupByLink<T extends { from: number; to: number }>(
  messages: readonly T[],
): Map<string, T[]> {
  const links = new Map<string, T[]>();
  for (const message of messages) {
    const key = `${message.from}->${message.to}`;
    const bucket = links.get(key);
    if (bucket) bucket.push(message);
    else links.set(key, [message]);
  }
  return links;
}
