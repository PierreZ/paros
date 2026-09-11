// The narrow stage: the geometry a phone gets.
//
// The wide stage draws a 960-unit picture and lets the browser shrink it into
// the screen. On a phone that makes a node label four pixels tall and a
// message dot seven across, so the player can read nothing and hit nothing.
// The narrow stage scales the *geometry* instead of the picture: the viewBox
// is the container's own width, so one SVG unit is one CSS pixel, a font size
// of 11 units is 11 pixels on the glass, and the node disc shrinks to make the
// room that the text keeps.
//
// One rule decides the rest. A node prints its labels centred under its disc,
// so two nodes that sit at the same height must stand `2 × labelHalf` apart.
// The layout therefore works `labelHalf` out from the picture it just laid
// out — the room at the edge of the board, and the smallest gap between two
// nodes whose labels share a height — and the stage clips every line to it.
//
// Everything here is pure — counts and a width in, points out — so those
// rules are unit-tested.

import { gridPoint, type Cell, type Gap, type GridShape, type Point } from './layout';

/** The container width below which the stage uses this geometry. */
export const NARROW_BREAKPOINT = 600;

/**
 * The height of one line of label text.
 *
 * A little more than the 11-pixel text it holds: a label block of five lines
 * is read at arm's length, and lines that touch are read as one.
 */
export const NARROW_LINE = 13;

/** The node disc's radius on a narrow stage. */
export const NARROW_NODE_RADIUS = 21;

/** The radius of a message dot. */
export const NARROW_DOT_RADIUS = 7;

/**
 * The radius of a dot's transparent hit circle.
 *
 * A finger needs a target of 24 pixels. This circle is 28 across.
 */
export const NARROW_HIT_RADIUS = 14;

/** The side of a matchmaker's square on a narrow stage. */
export const NARROW_MATCHMAKER_SIZE = 34;

/**
 * How many label lines the stage prints under one node.
 *
 * The inspector holds everything past this. Six lines under a 21-unit disc is
 * already most of a row's height.
 */
export const NARROW_META_LINES = 6;

/** The widest a label block gets, as a half-width. */
export const NARROW_LABEL_HALF = 62;

/**
 * The narrowest a label block gets.
 *
 * A ring is laid out to leave at least this much, and a line is clipped to
 * whatever the picture leaves. Twelve characters still name a ballot.
 */
export const NARROW_LABEL_MIN = 34;

/**
 * How wide one character of label text is.
 *
 * The narrow stage sets every label to 11 pixels (`styles.css`), and one unit
 * is one pixel, so this is the average advance of the body face at that size.
 */
const CHAR_WIDTH = 5.8;

/** The smallest board the geometry is computed for. */
const MIN_WIDTH = 260;

/** The margin a grid keeps at the two sides of the board. */
const MARGIN = 8;

/** The band at the top that the chosen banner uses. */
const TOP = 44;

/** The band above a grid that the column labels use. */
const GRID_AXIS = 20;

/** The sine of 60 degrees: the widest a ring gets, as a fraction of its radius. */
const SIN60 = Math.sin(Math.PI / 3);

/** The smallest ring the stage draws. */
const MIN_RING = 58;

/** How much a ring grows for each node on it. */
const RING_STEP = 14;

/** The space between the disc's bottom and the first label line. */
const LABEL_TOP = 14;

/** The space the stage keeps under the lowest label. */
const BOTTOM = 14;

/** How far the matchmaker band's title sits under the cluster. */
const BAND_TITLE = 22;

/** One node the layout must place. */
export interface NarrowNode {
  /** The node's id: the key the points are returned under. */
  readonly id: number;
  /** Where it sits in the grid, where the deployment runs one. */
  readonly cell?: Cell | null;
  /** How many label lines it prints. The tallest node sets the row height. */
  readonly lines?: number;
}

/** What the layout must make room for beside the nodes. */
export interface NarrowOptions {
  /** The acceptor grid, or nothing for a ring. */
  readonly grid?: GridShape | null;
  /** How many matchmakers the band below the cluster holds. */
  readonly matchmakers?: number;
  /** How many label lines one matchmaker prints. */
  readonly matchmakerLines?: number;
}

/** The whole narrow geometry. */
export interface NarrowStage {
  /** The viewBox width. One unit is one pixel. */
  readonly width: number;
  /** The viewBox height. */
  readonly height: number;
  /** The node disc's radius. */
  readonly nodeRadius: number;
  /** The middle of the cluster: what "outward" is measured from. */
  readonly centre: Point;
  /** Where each node sits, by node id. */
  readonly points: Map<number, Point>;
  /** How far apart two neighbour cells sit, or `null` for a ring. */
  readonly gap: Gap | null;
  /** Half the width one label line may use, under a node. */
  readonly labelHalf: number;
  /** Half the width one label line may use, under a matchmaker's square. */
  readonly bandLabelHalf: number;
  /** The lowest the cluster reaches, labels included. */
  readonly clusterBottom: number;
  /** Where each matchmaker's square sits, left to right, below the cluster. */
  readonly band: readonly Point[];
  /** The side of a matchmaker's square. */
  readonly matchmakerSize: number;
  /** Where the band's title prints, or `null` where there is no band. */
  readonly bandTitle: number | null;
}

/** How far the label block reaches below the disc's centre. */
function labelHeight(lines: number): number {
  return LABEL_TOP + Math.max(1, lines) * NARROW_LINE;
}

/** How many characters fit in a label block of this half-width. */
export function narrowChars(labelHalf: number): number {
  return Math.max(6, Math.floor((labelHalf * 2) / CHAR_WIDTH));
}

/**
 * One label line, clipped to the room the picture leaves it.
 *
 * A line that is cut keeps an ellipsis, and the whole line stays in the node's
 * inspector. Nothing the player must read is only here.
 */
export function clipLabel(line: string, labelHalf: number): string {
  const room = narrowChars(labelHalf);
  return line.length <= room ? line : `${line.slice(0, room - 1)}…`;
}

/** The board the narrow stage draws on: the container's width, within bounds. */
export function narrowBoard(width: number): number {
  const usable = Number.isFinite(width) ? Math.floor(width) - 2 : MIN_WIDTH;
  return Math.max(MIN_WIDTH, Math.min(usable, NARROW_BREAKPOINT));
}

/**
 * The ring a narrow board holds.
 *
 * The ring grows with the node count, because three nodes on a large circle
 * are all empty middle. It stops where a node on the side of the ring would
 * leave less than `NARROW_LABEL_MIN` between itself and the edge of the board.
 */
export function ringRadius(board: number, count: number): number {
  if (count <= 1) return 0;
  const wanted = MIN_RING + RING_STEP * count;
  const fits = (board / 2 - NARROW_LABEL_MIN) / SIN60;
  return Math.max(MIN_RING, Math.min(wanted, fits));
}

/**
 * Half the width a label may use, given where the nodes ended up.
 *
 * Two things bound it: the edge of the board, and any other node whose label
 * block shares a height with this one. The second is the bound a ring of six
 * hits first.
 */
function labelRoom(points: readonly Point[], board: number, radius: number, block: number): number {
  let half = NARROW_LABEL_HALF;
  const tall = 2 * radius + block;
  for (const point of points) {
    half = Math.min(half, point.x, board - point.x);
  }
  for (let i = 0; i < points.length; i += 1) {
    for (let j = i + 1; j < points.length; j += 1) {
      const a = points[i] as Point;
      const b = points[j] as Point;
      if (Math.abs(a.y - b.y) >= tall) continue;
      half = Math.min(half, Math.abs(a.x - b.x) / 2);
    }
  }
  return Math.max(NARROW_LABEL_MIN, half);
}

/**
 * Where every part of the narrow stage sits.
 *
 * The viewBox is two units narrower than the container, so the browser scales
 * the picture *up* by a hair rather than down. A scale below one would take a
 * font size of 11 units under 11 pixels, which is the whole thing this layout
 * exists to prevent.
 */
export function narrowLayout(
  width: number,
  nodes: readonly NarrowNode[],
  options: NarrowOptions = {},
): NarrowStage {
  const board = narrowBoard(width);
  const radius = NARROW_NODE_RADIUS;
  const lines = Math.max(1, ...nodes.map((node) => node.lines ?? 1));
  const block = labelHeight(lines);
  const shape = options.grid ?? null;
  const top = TOP + (shape ? GRID_AXIS : 0);

  const points = new Map<number, Point>();
  let centre: Point;
  let gap: Gap | null = null;

  if (shape) {
    const cols = Math.max(1, Math.floor(shape.cols));
    const rows = Math.max(1, Math.floor(shape.rows));
    // A row holds one disc and its labels, and nothing else: the log column
    // the wide stage draws beside a node is folded into those labels here, so
    // the rows stack much closer than they do on a wide screen.
    gap = { x: (board - 2 * MARGIN) / cols, y: radius * 2 + block + 8 };
    centre = { x: board / 2, y: top + radius + ((rows - 1) * gap.y) / 2 };
    for (const node of nodes) {
      points.set(node.id, gridPoint(node.cell ?? { row: 0, column: 0 }, shape, centre, gap));
    }
  } else {
    const ring = ringRadius(board, nodes.length);
    centre = { x: board / 2, y: top + ring + radius };
    nodes.forEach((node, index) => {
      if (nodes.length === 1) {
        points.set(node.id, centre);
        return;
      }
      const angle = -Math.PI / 2 + (2 * Math.PI * index) / nodes.length;
      points.set(node.id, {
        x: centre.x + ring * Math.cos(angle),
        y: centre.y + ring * Math.sin(angle),
      });
    });
  }

  const placed = [...points.values()];
  const labelHalf = placed.length === 0 ? NARROW_LABEL_HALF : labelRoom(placed, board, radius, block);
  const lowest = placed.length === 0 ? centre.y : Math.max(...placed.map((point) => point.y));
  const clusterBottom = lowest + radius + block;

  const count = Math.max(0, Math.floor(options.matchmakers ?? 0));
  const band: Point[] = [];
  let bandTitle: number | null = null;
  let bandLabelHalf = NARROW_LABEL_HALF;
  let height = clusterBottom + BOTTOM;
  if (count > 0) {
    // The band goes under the cluster, not beside it. A band beside the
    // acceptors takes a fifth of a phone's width, and then neither tier is
    // readable.
    bandTitle = clusterBottom + BAND_TITLE;
    const slot = board / count;
    // A square owns its share of the band, which is wider than a node's share
    // of the cluster: a registry row is the longest line the stage prints.
    bandLabelHalf = Math.max(NARROW_LABEL_MIN, slot / 2 - 4);
    const y = bandTitle + 14 + NARROW_MATCHMAKER_SIZE / 2;
    for (let index = 0; index < count; index += 1) {
      band.push({ x: slot * (index + 0.5), y });
    }
    height = y + NARROW_MATCHMAKER_SIZE / 2 + labelHeight(options.matchmakerLines ?? 3) + BOTTOM;
  }

  return {
    width: board,
    height: Math.round(height),
    nodeRadius: radius,
    centre,
    points,
    gap,
    labelHalf,
    bandLabelHalf,
    clusterBottom,
    band,
    matchmakerSize: NARROW_MATCHMAKER_SIZE,
    bandTitle,
  };
}
