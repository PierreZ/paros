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

/** The shape of an acceptor grid: how many rows, and how many columns. */
export interface GridShape {
  readonly rows: number;
  readonly cols: number;
}

/** Where one acceptor sits in a grid. */
export interface Cell {
  readonly row: number;
  readonly column: number;
}

/** How far apart two neighbour cells sit, in SVG user units. */
export interface Gap {
  readonly x: number;
  readonly y: number;
}

/**
 * Where one cell of a `rows × cols` grid sits, with the whole grid centred on
 * `centre`.
 *
 * A grid deployment is not a ring: a row is a Phase-1 quorum and a column is a
 * Phase-2 quorum, so the player must see the rows and the columns. A cell
 * outside the grid is clamped into it, because a bogus cell must still draw
 * somewhere the player can click.
 */
export function gridPoint(cell: Cell, shape: GridShape, centre: Point, gap: Gap): Point {
  const cols = Math.max(1, Math.floor(shape.cols));
  const rows = Math.max(1, Math.floor(shape.rows));
  const column = Math.min(Math.max(Math.floor(cell.column), 0), cols - 1);
  const row = Math.min(Math.max(Math.floor(cell.row), 0), rows - 1);
  return {
    x: centre.x + (column - (cols - 1) / 2) * gap.x,
    y: centre.y + (row - (rows - 1) / 2) * gap.y,
  };
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

/**
 * The prefix that names a message endpoint's tier.
 *
 * Node ids and matchmaker ids are different identity spaces, so matchmaker 0
 * and node 0 are two endpoints and must never share a link. A message with no
 * party at all is a node message, which is what every message was before the
 * matchmakers arrived.
 */
function tier(party: unknown): string {
  return party === 'matchmaker' ? 'm' : '';
}

/**
 * Group messages by the ordered pair they travel between.
 *
 * The pair is a tier and an id at each end, never an id alone.
 */
export function groupByLink<
  T extends { from: number; to: number; from_party?: unknown; to_party?: unknown },
>(messages: readonly T[]): Map<string, T[]> {
  const links = new Map<string, T[]>();
  for (const message of messages) {
    const key = `${tier(message.from_party)}${message.from}->${tier(message.to_party)}${message.to}`;
    const bucket = links.get(key);
    if (bucket) bucket.push(message);
    else links.set(key, [message]);
  }
  return links;
}
