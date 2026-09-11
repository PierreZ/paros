// The stage: one hand-drawn SVG, derived from the view on every change.
//
// There is no animation state and no scene graph to keep in step — the whole
// picture is a pure function of `GameView` and the width it is drawn into.
// Interaction rides on data attributes (`data-msg`, `data-node`) and is
// handled by delegation in `main.ts`, so a re-render never has to re-bind
// anything.
//
// Two geometries, one picture. The wide stage draws 960 units and lets the
// browser fit them to the screen; the narrow stage (`render/narrow.ts`) draws
// the container's own width, so a label keeps its size and the disc gives up
// its room. Everything below the geometry — the links, the dots, the roles,
// the labels — is written once and reads the geometry it was handed.

import type { GameView, MatchmakerView, MessageView, NodeView, SlotView, WorldView } from '../types';
import { svg } from './dom';
import { wipedNodes } from './disk';
import { cellBadge, columnClass, columnOf, gridCells, gridOf } from './grid';
import {
  circleLayout,
  dotPositions,
  gridPoint,
  groupByLink,
  trim,
  type Cell,
  type Gap,
  type GridShape,
  type Point,
} from './layout';
import {
  BAND_WIDTH,
  MATCHMAKER_SIZE,
  endpointOf,
  matchmakerClass,
  matchmakerLabel,
  matchmakerPositions,
  phaseWords,
  registrationLine,
  registryLines,
} from './matchmaker';
import {
  NARROW_DOT_RADIUS,
  NARROW_HIT_RADIUS,
  NARROW_LABEL_HALF,
  NARROW_LINE,
  NARROW_META_LINES,
  clipLabel,
  narrowLayout,
  type NarrowNode,
} from './narrow';

/** The stage without a matchmaker band. */
const WIDTH = 960;

/** The stage without the taller node badges a matchmaker deployment prints. */
const HEIGHT = 620;

/** The stage with a matchmaker deployment's band and badges. */
const TALL = 680;

const NODE_RADIUS = 32;
const SLOT_WIDTH = 92;
const SLOT_HEIGHT = 19;
const MAX_SLOTS = 6;

/**
 * How far apart two neighbour cells of a grid sit.
 *
 * The horizontal gap holds a log column (`SLOT_WIDTH`) and two node radii, so
 * a node's log never lands on the node beside it. The vertical gap holds six
 * slot boxes and the meta lines below a node.
 */
const GRID_GAP = { x: 180, y: 176 };

/**
 * How far left of a node the row label sits.
 *
 * A node in the left column draws its log on its left, so the label must clear
 * the whole log column. A label the log covers names nothing.
 */
const ROW_LABEL_OFFSET = NODE_RADIUS + 10 + SLOT_WIDTH + 16;

/** The radius of a message dot, and of the transparent circle around it. */
const DOT_RADIUS = 7;
const HIT_RADIUS = 13;

/**
 * How the stage is drawn.
 *
 * `narrow` is the phone geometry, and `width` is the container's own width in
 * CSS pixels — the number `main.ts` reads through a `ResizeObserver`. The wide
 * stage ignores the width: it draws 960 units and the browser fits them.
 */
export interface Viewport {
  readonly narrow: boolean;
  readonly width: number;
}

/** The wide stage, which is what a caller that says nothing wants. */
export const WIDE: Viewport = { narrow: false, width: WIDTH };

/** Where everything sits, whichever geometry drew it. */
interface Geometry {
  readonly narrow: boolean;
  readonly width: number;
  readonly height: number;
  readonly centre: Point;
  readonly nodeRadius: number;
  readonly labelHalf: number;
  readonly bandLabelHalf: number;
  readonly gridGap: Gap;
  readonly at: Map<number, Point>;
  readonly bandAt: Map<number, Point>;
  readonly band: { readonly size: number; readonly title: number } | null;
  readonly shape: GridShape | null;
  readonly cells: Map<number, Cell> | null;
}

/**
 * Whether a message answers one.
 *
 * The engine reports this fact (`MessageView.reply`); the frontend must not
 * read it out of the variant's name. An engine that does not send the field
 * yet gives every message the request shape.
 */
export function isReply(message: Pick<MessageView, 'reply'>): boolean {
  return message.reply === true;
}

/** The CSS class carrying a message's phase colour. */
export function phaseClass(phase: string): string {
  const known = [
    'prepare',
    'promise',
    'accept',
    'accepted',
    'nack',
    'commit',
    'heartbeat',
    'catchup',
    'snapshot',
    'read',
    'handoff',
    'match',
    'gc',
    'reconfigure',
  ];
  return `phase-${known.includes(phase) ? phase : 'other'}`;
}

/**
 * The name of a control command, as a slot box prints it.
 *
 * The engine sends the discriminant in lower case (`noop`, `truncate`,
 * `snap`); the label is the command's own name. A slot with no control command
 * holds an opaque client value and has no name to print.
 */
export function controlLabel(control: string | null | undefined): string | null {
  if (typeof control !== 'string' || control === '') return null;
  const known: Record<string, string> = {
    noop: 'Noop',
    truncate: 'Truncate',
    snap: 'Snap',
  };
  return known[control.toLowerCase()] ?? control;
}

/** A short label for one slot box. */
export function slotLabel(slot: SlotView): string {
  const control = controlLabel(slot.control);
  const value = control ?? slot.value;
  if (value === '') return `${slot.slot}: ·`;
  return `${slot.slot}: ${value.length > 10 ? `${value.slice(0, 9)}…` : value}`;
}

/** The CSS classes one slot box carries. */
export function slotClass(slot: SlotView): string {
  const parts = ['slot'];
  if (slot.applied) parts.push('applied');
  else if (slot.chosen) parts.push('chosen');
  else parts.push('open');
  if (controlLabel(slot.control)) parts.push('control');
  return parts.join(' ');
}

/**
 * What the stage prints under a node.
 *
 * A log node has a role in the current ballot. A single-decree proposer has no
 * role, but it has an attempt, and that is what the player watches. A node
 * with neither prints what it is.
 */
export function roleLabel(node: NodeView, wiped = false): string {
  // A retired node answered the evidence and shut down for good. It is not a
  // crashed node, because it does not come back.
  if (node.retired === true) return 'retired';
  if (wiped) return 'wiped';
  if (!node.alive) return 'crashed';
  if (node.role) return node.role;
  const attempts: Record<string, string> = {
    idle: 'idle',
    phase1: 'phase 1',
    phase2: 'phase 2',
    preempted: 'preempted',
    won: 'won',
  };
  if (node.attempt) return attempts[node.attempt] ?? node.attempt;
  return node.flavour;
}

function nodeStateClass(node: NodeView, wiped = false): string {
  const parts = ['node'];
  if (node.retired === true) parts.push('retired');
  if (wiped) parts.push('wiped');
  if (!node.alive) parts.push('crashed');
  if (node.role === 'leader' || node.attempt === 'won') parts.push('leader');
  if (node.role === 'candidate' || node.attempt === 'phase1' || node.attempt === 'phase2') {
    parts.push('candidate');
  }
  return parts.join(' ');
}

function slotTitle(slot: SlotView): string {
  const control = controlLabel(slot.control);
  const what = control ? `the control command ${slot.value}` : `the value ${slot.value || '(empty)'}`;
  return `slot ${slot.slot} holds ${what}, at ballot ${slot.ballot ?? 'none'}${
    slot.chosen ? ', chosen' : ''
  }${slot.applied ? ', applied' : ''}`;
}

/**
 * The compaction floor: the first slot this node still keeps.
 *
 * A floor of zero drops nothing, so the stage draws no line for it.
 */
function floorOf(node: Pick<NodeView, 'floor'>): number | null {
  const floor = node.floor;
  return typeof floor === 'number' && floor > 0 ? floor : null;
}

function logColumn(node: NodeView, at: Point, centre: Point): SVGGElement {
  const toTheRight = at.x >= centre.x;
  const x = toTheRight ? at.x + NODE_RADIUS + 10 : at.x - NODE_RADIUS - 10 - SLOT_WIDTH;
  const shown = node.accepted.slice(0, MAX_SLOTS);
  const rows: (SVGGElement | SVGTextElement | SVGLineElement)[] = [];
  const floor = floorOf(node);
  const total = shown.length + (node.chosen_gap ? 1 : 0) + (floor === null ? 0 : 1);
  const top = at.y - ((total * (SLOT_HEIGHT + 3)) / 2 - 2);
  let row = 0;

  // The floor sits above the slots the node still keeps: everything before it
  // is deleted here, and only a snapshot can put it back.
  if (floor !== null) {
    const y = top + SLOT_HEIGHT - 4;
    rows.push(
      svg('line', { class: 'floor-line', x1: x, y1: y, x2: x + SLOT_WIDTH, y2: y }),
      svg('text', { class: 'floor-label', x: x + 2, y: y - 4 }, `floor ${floor}`),
    );
    row += 1;
  }

  shown.forEach((slot) => {
    const y = top + row * (SLOT_HEIGHT + 3);
    row += 1;
    rows.push(
      svg(
        'g',
        { class: slotClass(slot) },
        svg('rect', { x, y, width: SLOT_WIDTH, height: SLOT_HEIGHT, rx: 3 }),
        svg('text', { x: x + 5, y: y + 13, class: 'slot-label' }, slotLabel(slot)),
        svg('title', {}, slotTitle(slot)),
      ),
    );
  });

  if (node.chosen_gap) {
    const y = top + row * (SLOT_HEIGHT + 3);
    row += 1;
    rows.push(
      svg(
        'g',
        { class: 'slot hole' },
        svg('rect', { x, y, width: SLOT_WIDTH, height: SLOT_HEIGHT, rx: 3 }),
        svg(
          'text',
          { x: x + 5, y: y + 13, class: 'slot-label' },
          `hole at ${node.chosen_gap.hole}`,
        ),
        svg(
          'title',
          {},
          `slot ${node.chosen_gap.hole} is undecided, and slot ${node.chosen_gap.highest} is already chosen`,
        ),
      ),
    );
  }

  if (node.accepted.length > MAX_SLOTS) {
    const y = top + row * (SLOT_HEIGHT + 3) + 11;
    rows.push(
      svg(
        'text',
        { x: x + 5, y, class: 'slot-more' },
        `+${node.accepted.length - MAX_SLOTS} more`,
      ),
    );
  }

  return svg('g', { class: 'log-column' }, ...rows);
}

function electionRing(node: NodeView, nodeRadius: number): SVGElement | null {
  const election = node.election;
  if (!election) return null;
  if (election.held) {
    return svg('circle', {
      class: 'election held',
      r: nodeRadius + 7,
      cx: 0,
      cy: 0,
    });
  }
  if (election.timeout <= 0) return null;
  // No `elapsed` in the view contract yet: the ring shows the timeout's size,
  // one dash per tick, rather than pretending to know how far it has run.
  const radius = nodeRadius + 7;
  const circumference = 2 * Math.PI * radius;
  const dash = Math.max(2, circumference / (election.timeout * 2));
  return svg('circle', {
    class: 'election armed',
    r: radius,
    cx: 0,
    cy: 0,
    'stroke-dasharray': `${dash.toFixed(2)} ${dash.toFixed(2)}`,
  });
}

/**
 * What the stage prints under a node, the top line first.
 *
 * `matchmade` says the deployment names matchmakers. On a plain deployment the
 * acceptor set is fixed for life and its badge would be noise; where a
 * reconfiguration is possible the set in force and the ballot it is bound to
 * are the two facts the player works with, so they come first.
 *
 * Every line is a field the engine reports. The frontend derives no
 * configuration, no floor and no generation of its own.
 */
export function nodeMeta(
  node: NodeView,
  cell: Cell | null,
  wiped: boolean,
  matchmade: boolean,
): string[] {
  const lines: string[] = [];
  const badge = cellBadge(cell);
  if (badge) lines.push(badge);
  // A retired node answered the evidence and shut down. Its old state says
  // nothing about the cluster now.
  if (node.retired === true) return [...lines, 'it does not come back'];
  if (wiped) return [...lines, 'the disk is empty'];

  if (matchmade) {
    const set = Array.isArray(node.acceptors) ? node.acceptors : [];
    const since = typeof node.acceptors_since === 'string' ? node.acceptors_since : null;
    lines.push(`acceptors ${set.join(',') || '—'}${since === null ? '' : ` · since ${since}`}`);
    const registry = node.matchmakers;
    if (registry) {
      const members = Array.isArray(registry.members) ? registry.members : [];
      lines.push(`matchmakers ${members.map((id) => `m${id}`).join(',')} · gen ${registry.generation}`);
    }
  }
  if (node.promised) lines.push(`promised ${node.promised}`);
  if (node.ballot) lines.push(`ballot ${node.ballot}`);
  if (node.chosen_index !== null) lines.push(`chosen ≤ ${node.chosen_index}`);
  if (node.leader !== null) lines.push(`leader ${node.leader}`);

  const matchmaking = node.matchmaking;
  if (matchmaking) {
    const kind = matchmaking.kind === 'reconfiguration' ? 'change' : 'belief';
    lines.push(`matchmaking ${matchmaking.ballot} · ${kind}`);
    lines.push(`${matchmaking.remaining} to answer`);
  }
  const gc = node.gc;
  if (gc) {
    lines.push(`gc floor ${gc.effective_watermark}`);
    const retirable = Array.isArray(gc.retirable) ? gc.retirable : [];
    if (retirable.length > 0) lines.push(`it frees ${retirable.join(',')}`);
  }
  if (node.handover) lines.push(`handover · ${node.handover}`);
  return lines;
}

/**
 * The accepted log, folded to the lines a phone has room for.
 *
 * The wide stage draws the whole column of slot boxes beside the node. A
 * narrow stage prints this summary instead — how much is decided, how far the
 * log goes, and the hole if there is one — and the player opens the node to
 * read the column itself. Every number is counted from the slots the engine
 * sent; nothing here decides what a slot is.
 */
export function logSummary(
  node: Pick<NodeView, 'accepted' | 'chosen_gap' | 'floor'>,
): string[] {
  const accepted = Array.isArray(node.accepted) ? node.accepted : [];
  const floor = floorOf(node);
  const lines: string[] = [];
  if (accepted.length === 0) {
    lines.push(floor === null ? 'no slot yet' : `floor ${floor} · no slot`);
  } else {
    const chosen = accepted.filter((slot) => slot.chosen || slot.applied).length;
    const applied = accepted.filter((slot) => slot.applied).length;
    const top = accepted.reduce((high, slot) => Math.max(high, slot.slot), 0);
    lines.push(`${chosen} chosen · ${applied} applied`);
    lines.push(floor === null ? `top slot ${top}` : `top slot ${top} · floor ${floor}`);
  }
  if (node.chosen_gap) lines.push(`hole at ${node.chosen_gap.hole}`);
  return lines;
}

/** One line of a narrow node's label block, and what it says. */
export interface NarrowLabel {
  readonly text: string;
  readonly kind: 'badge' | 'role' | 'summary' | 'meta';
}

/**
 * What a narrow stage prints under a node.
 *
 * Everything goes below the disc, the role included: a disc of 21 units is
 * narrower than the word `acceptor`, and a role printed inside it runs over
 * both edges. The wide list of facts is too long and too wide for a phone, so
 * this one keeps what the player plays with — what the log holds, what the
 * node promised, how far it applied — and the node's inspector holds the rest.
 * A line is short on purpose: the layout says how wide a label may be, and a
 * line the picture cannot hold is clipped there.
 */
export function narrowMeta(
  node: NodeView,
  cell: Cell | null,
  wiped: boolean,
  matchmade: boolean,
): NarrowLabel[] {
  const lines: NarrowLabel[] = [];
  const badge = cellBadge(cell);
  if (badge) lines.push({ text: badge, kind: 'badge' });
  lines.push({ text: roleLabel(node, wiped), kind: 'role' });
  if (node.retired === true) return [...lines, { text: 'it stays down', kind: 'meta' }];
  if (wiped) return [...lines, { text: 'the disk is empty', kind: 'meta' }];
  for (const text of logSummary(node)) lines.push({ text, kind: 'summary' });
  const meta: string[] = [];
  if (node.promised) meta.push(`promised ${node.promised}`);
  if (node.chosen_index !== null) meta.push(`chosen ≤ ${node.chosen_index}`);
  if (matchmade) {
    const set = Array.isArray(node.acceptors) ? node.acceptors : [];
    meta.push(`acceptors ${set.join(',') || '—'}`);
  }
  if (node.matchmaking) meta.push(`${node.matchmaking.remaining} must answer`);
  if (node.gc) meta.push(`gc floor ${node.gc.effective_watermark}`);
  if (node.handover) meta.push(`handover · ${node.handover}`);
  for (const text of meta) lines.push({ text, kind: 'meta' });
  return lines.slice(0, NARROW_META_LINES);
}

/** The sentence a node's tooltip carries. */
function nodeTitle(node: NodeView, wiped: boolean): string {
  if (node.retired === true) {
    return `node ${node.id} is retired. It showed the garbage-collection floor, it shut down, and it does not come back.`;
  }
  if (wiped) {
    return `node ${node.id} lost its disk. It must not start again: a promise cannot come back.`;
  }
  const promise = node.promised ? `, and it promised ${node.promised}` : '';
  const matchmaking = node.matchmaking
    ? ` It registers ballot ${node.matchmaking.ballot} with the matchmakers, and ${node.matchmaking.remaining} of them must still answer.`
    : '';
  return `node ${node.id} is ${roleLabel(node)}${promise}.${matchmaking}`;
}

function nodeGroup(
  node: NodeView,
  at: Point,
  cell: Cell | null,
  wiped: boolean,
  matchmade: boolean,
  geo: Geometry,
): SVGGElement {
  const narrow = geo.narrow;
  const badge = cellBadge(cell);
  const under: NarrowLabel[] = narrow
    ? narrowMeta(node, cell, wiped, matchmade).map((label) => ({
        text: clipLabel(label.text, geo.labelHalf),
        kind: label.kind,
      }))
    : nodeMeta(node, cell, wiped, matchmade).map((text, index) => ({
        text,
        kind: index === 0 && badge ? 'badge' : 'meta',
      }));
  const ring = wiped || node.retired === true ? null : electionRing(node, geo.nodeRadius);
  const line = narrow ? NARROW_LINE : 12;
  const labels = under.map((label, index) =>
    svg(
      'text',
      {
        class: nodeLabelClass(label.kind),
        x: 0,
        y: geo.nodeRadius + (narrow ? 13 : 16) + index * line,
      },
      label.text,
    ),
  );
  const title = nodeTitle(node, wiped);

  return svg(
    'g',
    {
      class: `${nodeStateClass(node, wiped)}${narrow ? ' tappable' : ''}`,
      transform: `translate(${at.x.toFixed(1)}, ${at.y.toFixed(1)})`,
      'data-node': node.id,
      tabindex: narrow ? 0 : null,
      role: narrow ? 'button' : null,
      'aria-label': narrow ? `${title} Open node ${node.id} to read its log.` : null,
    },
    ring,
    svg('circle', { class: 'node-disc', r: geo.nodeRadius, cx: 0, cy: 0 }),
    svg('text', { class: 'node-id', x: 0, y: narrow ? 6 : -2 }, String(node.id)),
    narrow ? null : svg('text', { class: 'node-role', x: 0, y: 14 }, roleLabel(node, wiped)),
    ...labels,
    svg('title', {}, title),
  );
}

/**
 * The class one label line carries.
 *
 * The grid badge, the role and the folded log summary are each marked: they
 * are what the player reads first, and the rest is detail.
 */
function nodeLabelClass(kind: NarrowLabel['kind']): string {
  if (kind === 'badge') return 'node-meta cell-badge';
  if (kind === 'role') return 'node-meta node-role';
  if (kind === 'summary') return 'node-meta log-summary';
  return 'node-meta';
}

/**
 * How a message's endpoint is named in a sentence.
 *
 * The engine says which tier the number belongs to. Node 0 and matchmaker 0
 * are two different processes, and a reader must be told which one moved.
 */
export function partyName(id: number, party: string | null | undefined): string {
  return party === 'matchmaker' ? `matchmaker ${id}` : `node ${id}`;
}

function messageDot(message: MessageView, at: Point, geo: Geometry): SVGGElement {
  // The column an Accept was addressed to colours the dot's edge: a grid
  // decides a slot by one whole column, so the player must see which one a
  // message belongs to. The engine names it (`MessageView.column`); the
  // frontend never works it out from the slot.
  const column = columnOf(message);
  const columnClasses = column === null ? '' : ` column ${columnClass(column)}`;
  const where = column === null ? '' : `, column ${column}`;
  const from = partyName(message.from, message.from_party);
  const to = partyName(message.to, message.to_party);
  return svg(
    'g',
    {
      class: `wire-dot ${phaseClass(message.phase)} ${isReply(message) ? 'reply' : 'request'}${columnClasses}`,
      'data-msg': message.id,
      transform: `translate(${at.x.toFixed(1)}, ${at.y.toFixed(1)})`,
      tabindex: 0,
      role: 'button',
      'aria-label': `${message.summary}, from ${from} to ${to}${where}. Click to deliver it.`,
    },
    svg('circle', {
      class: 'dot-hit',
      r: geo.narrow ? NARROW_HIT_RADIUS : HIT_RADIUS,
      cx: 0,
      cy: 0,
    }),
    svg('circle', {
      class: 'dot',
      r: geo.narrow ? NARROW_DOT_RADIUS : DOT_RADIUS,
      cx: 0,
      cy: 0,
    }),
    svg('title', {}, `${message.summary} (${from} → ${to})${where}. Click to deliver it.`),
  );
}

/** The row and column labels that name a grid's quorums. */
function gridLabels(shape: GridShape, geo: Geometry): SVGGElement {
  const marks: SVGElement[] = [];
  const gap = geo.gridGap;
  for (let row = 0; row < shape.rows; row += 1) {
    const at = gridPoint({ row, column: 0 }, shape, geo.centre, gap);
    // A narrow node draws no log column, so the row label goes at the edge of
    // the board and reads from there. A wide one must clear the whole column.
    marks.push(
      svg(
        'text',
        {
          class: `grid-axis grid-row${geo.narrow ? ' narrow' : ''}`,
          x: geo.narrow ? 3 : at.x - ROW_LABEL_OFFSET,
          y: geo.narrow ? at.y - geo.nodeRadius - 4 : at.y + 4,
        },
        `row ${row}`,
      ),
      svg(
        'title',
        {},
        `row ${row} is a Phase-1 quorum: a whole row answers a Prepare.`,
      ),
    );
  }
  for (let column = 0; column < shape.cols; column += 1) {
    const at = gridPoint({ row: 0, column }, shape, geo.centre, gap);
    marks.push(
      svg(
        'text',
        {
          class: `grid-axis grid-col ${columnClass(column)}`,
          x: at.x,
          y: geo.narrow ? at.y - geo.nodeRadius - 10 : at.y - gap.y * 0.5,
        },
        `col ${column}`,
      ),
    );
  }
  return svg('g', { class: 'grid-axes' }, ...marks);
}

/**
 * One matchmaker: a rounded square, its generation and phase, its registry and
 * its floor.
 *
 * The square is a different shape from a node's disc on purpose. A matchmaker
 * keeps a map from a ballot to an acceptor set; it holds no log and it votes
 * on no slot, so it must not read as one more acceptor.
 */
function matchmakerGroup(matchmaker: MatchmakerView, at: Point, geo: Geometry): SVGGElement {
  const size = geo.band?.size ?? MATCHMAKER_SIZE;
  const half = size / 2;
  const phase = phaseWords(matchmaker.phase);
  const successor = matchmaker.successor;
  const under: string[] = [`gen ${matchmaker.generation} · ${matchmaker.alive === false ? 'down' : phase}`];
  under.push(`floor ${matchmaker.gc_watermark}`);
  if (successor) {
    under.push(`next gen ${successor.generation}: ${successor.members.map((id) => `m${id}`).join(',')}`);
  }

  // A narrow band stacks its lines under the square, because there is no room
  // beside it. The registry folds to the newest row and a count of the rest;
  // the panel's own matchmaker controls hold the whole registry.
  const registry = registryLines(matchmaker);
  const rows = geo.narrow
    ? narrowRegistry(matchmaker).map((line, index) =>
        svg(
          'text',
          {
            class: 'registry-line narrow',
            x: 0,
            y: half + 14 + (under.length + index) * NARROW_LINE,
          },
          clipLabel(line, geo.bandLabelHalf),
        ),
      )
    : registry.map((line, index) =>
        svg('text', { class: 'registry-line', x: half + 10, y: -8 + index * 12 }, line),
      );
  const title =
    matchmaker.alive === false
      ? `matchmaker ${matchmaker.id} is down. Its registry stays on its disk.`
      : matchmaker.phase === 'stopped'
        ? `matchmaker ${matchmaker.id} is frozen for generation ${matchmaker.generation}. It registers nothing more, and it points late candidates at the successor.`
        : `matchmaker ${matchmaker.id} serves generation ${matchmaker.generation}. It holds ${matchmaker.registrations.length} registration(s), and it forgets every ballot below ${matchmaker.gc_watermark}.`;

  return svg(
    'g',
    {
      class: matchmakerClass(matchmaker),
      transform: `translate(${at.x.toFixed(1)}, ${at.y.toFixed(1)})`,
      'data-matchmaker': matchmaker.id,
    },
    svg('rect', {
      class: 'matchmaker-box',
      x: -half,
      y: -half,
      width: size,
      height: size,
      rx: geo.narrow ? 7 : 9,
    }),
    svg('text', { class: 'matchmaker-id', x: 0, y: 5 }, matchmakerLabel(matchmaker)),
    ...under.map((line, index) =>
      svg(
        'text',
        { class: 'matchmaker-meta', x: 0, y: half + 14 + index * (geo.narrow ? NARROW_LINE : 12) },
        geo.narrow ? clipLabel(line, geo.bandLabelHalf) : line,
      ),
    ),
    ...rows,
    svg('title', {}, title),
  );
}

/**
 * The registry, as a narrow square lists it: the newest row, then a count.
 *
 * A phone has room for one row. The row the player works with is the newest
 * one, which is the one the engine sends last.
 */
export function narrowRegistry(
  matchmaker: Pick<MatchmakerView, 'registrations'>,
): string[] {
  const rows = Array.isArray(matchmaker.registrations) ? matchmaker.registrations : [];
  const newest = rows[rows.length - 1];
  if (!newest) return ['no registration'];
  const lines = [registrationLine(newest)];
  if (rows.length > 1) lines.push(`+${rows.length - 1} more`);
  return lines;
}

/**
 * The whole matchmaker band, and the rule that separates it from the cluster.
 *
 * A wide stage puts the band in a gutter on the right, behind a rule. A narrow
 * stage puts it in a row *under* the acceptors, behind the same rule drawn
 * across the board: a band beside the cluster on a phone takes a fifth of the
 * width, and then neither tier is readable.
 */
function matchmakerBand(
  matchmakers: readonly MatchmakerView[],
  centre: Point,
  geo: Geometry,
): SVGGElement | null {
  if (matchmakers.length === 0) return null;
  if (geo.narrow) {
    const title = geo.band?.title ?? 0;
    return svg(
      'g',
      { class: 'matchmaker-band narrow' },
      svg('line', { class: 'band-rule', x1: 0, y1: title - 14, x2: geo.width, y2: title - 14 }),
      svg('text', { class: 'gutter-title', x: 6, y: title }, 'matchmakers'),
      ...matchmakers.map((matchmaker, index) =>
        matchmakerGroup(matchmaker, geo.bandAt.get(matchmaker.id) ?? { x: index, y: title }, geo),
      ),
    );
  }
  const positions = matchmakerPositions(matchmakers.length, centre);
  return svg(
    'g',
    { class: 'matchmaker-band' },
    svg('line', {
      class: 'band-rule',
      x1: centre.x - MATCHMAKER_SIZE,
      y1: 44,
      x2: centre.x - MATCHMAKER_SIZE,
      y2: centre.y * 2 - 30,
    }),
    svg('text', { class: 'gutter-title', x: centre.x - MATCHMAKER_SIZE + 12, y: 30 }, 'matchmakers'),
    ...matchmakers.map((matchmaker, index) =>
      matchmakerGroup(matchmaker, positions[index] ?? centre, geo),
    ),
  );
}

function clientColumn(world: WorldView): SVGGElement | null {
  if (world.clients.length === 0) return null;
  const rows: SVGElement[] = [svg('text', { class: 'gutter-title', x: 14, y: 30 }, 'clients')];
  let y = 52;
  for (const client of world.clients) {
    rows.push(svg('text', { class: 'client-id', x: 14, y }, `client ${client.id}`));
    y += 16;
    for (const proposal of client.proposals.slice(-4)) {
      const state = proposal.acked
        ? `acked at slot ${proposal.slot ?? '?'}`
        : proposal.slot !== null
          ? `slot ${proposal.slot}`
          : 'in flight';
      rows.push(
        svg('text', { class: 'client-line', x: 20, y }, `#${proposal.seq} ${proposal.value} — ${state}`),
      );
      y += 14;
    }
    for (const read of client.reads.slice(-3)) {
      rows.push(
        svg(
          'text',
          { class: 'client-line', x: 20, y },
          `read ${read.ctx} — ${read.served ? `slot ${read.index ?? '?'}` : 'waiting'}`,
        ),
      );
      y += 14;
    }
    y += 8;
  }
  return svg('g', { class: 'client-gutter' }, ...rows);
}

function chosenBanner(world: WorldView, width: number): SVGGElement | null {
  if (!world.chosen) return null;
  const control = controlLabel(world.chosen.control);
  // The banner is as wide as it needs to be and never wider than the board.
  const box = Math.min(400, width - 20);
  return svg(
    'g',
    { class: 'chosen-banner' },
    svg('rect', { x: width / 2 - box / 2, y: 12, width: box, height: 34, rx: 6 }),
    svg(
      'text',
      { x: width / 2, y: 34 },
      `chosen: ${control ?? world.chosen.value} at ballot ${world.chosen.ballot}`,
    ),
  );
}

/**
 * Where every part of this world sits, for the viewport it is drawn into.
 *
 * The two geometries answer the same questions, so everything the stage draws
 * below this point is written once. A grid deployment is laid out as a grid —
 * a row is a Phase-1 quorum and a column is a Phase-2 quorum, and neither is
 * legible on a ring — and every other quorum system keeps the circle.
 */
function geometryOf(view: GameView, viewport: Viewport): Geometry {
  const world = view.world;
  const matchmakers = Array.isArray(world.matchmakers) ? world.matchmakers : [];
  const shape = gridOf(world);
  const cells = shape ? gridCells(world, shape) : null;
  const wiped = wipedNodes(view);
  const matchmade = matchmakers.length > 0;

  if (viewport.narrow) {
    const nodes: NarrowNode[] = world.nodes.map((node) => ({
      id: node.id,
      cell: cells?.get(node.id) ?? null,
      lines: narrowMeta(node, cells?.get(node.id) ?? null, wiped.has(node.id), matchmade).length,
    }));
    const matchmakerLines = matchmakers.reduce(
      (most, matchmaker) => Math.max(most, 2 + (matchmaker.successor ? 1 : 0) + narrowRegistry(matchmaker).length),
      3,
    );
    const stage = narrowLayout(viewport.width, nodes, {
      grid: shape,
      matchmakers: matchmakers.length,
      matchmakerLines,
    });
    const bandAt = new Map<number, Point>();
    matchmakers.forEach((matchmaker, index) => {
      const point = stage.band[index];
      if (point) bandAt.set(matchmaker.id, point);
    });
    return {
      narrow: true,
      width: stage.width,
      height: stage.height,
      centre: stage.centre,
      nodeRadius: stage.nodeRadius,
      labelHalf: stage.labelHalf,
      bandLabelHalf: stage.bandLabelHalf,
      gridGap: stage.gap ?? GRID_GAP,
      at: stage.points,
      bandAt,
      band: { size: stage.matchmakerSize, title: stage.bandTitle ?? 0 },
      shape,
      cells,
    };
  }

  // A deployment that names matchmakers gets a band of its own on the right
  // and a taller stage for the badges the tier adds. Every other position is
  // exactly where it was, so the acceptor ring never moves under the player.
  const hasClients = world.clients.length > 0;
  const width = WIDTH + (matchmade ? BAND_WIDTH : 0);
  const height = matchmade ? TALL : HEIGHT;
  // A grid sits further right than a ring: it spends its left margin on the
  // row labels and on the leftmost logs.
  const centre: Point = shape
    ? { x: hasClients ? 600 : 520, y: 320 }
    : { x: hasClients ? 560 : 480, y: 320 };
  const at = new Map<number, Point>();
  if (shape && cells) {
    for (const node of world.nodes) {
      at.set(node.id, gridPoint(cells.get(node.id) ?? { row: 0, column: 0 }, shape, centre, GRID_GAP));
    }
  } else {
    const radius = world.nodes.length > 4 ? 210 : 185;
    const positions = circleLayout(world.nodes.length, centre, radius);
    world.nodes.forEach((node, index) => {
      at.set(node.id, positions[index] ?? centre);
    });
  }
  const bandCentre: Point = { x: WIDTH + MATCHMAKER_SIZE + 20, y: height / 2 };
  const bandAt = new Map<number, Point>();
  matchmakerPositions(matchmakers.length, bandCentre).forEach((point, index) => {
    const matchmaker = matchmakers[index];
    if (matchmaker) bandAt.set(matchmaker.id, point);
  });

  return {
    narrow: false,
    width,
    height,
    centre,
    nodeRadius: NODE_RADIUS,
    labelHalf: NARROW_LABEL_HALF,
    bandLabelHalf: NARROW_LABEL_HALF,
    gridGap: GRID_GAP,
    at,
    bandAt,
    band: null,
    shape,
    cells,
  };
}

/**
 * Draw the whole stage.
 *
 * `viewport` says which geometry the picture gets. A caller that says nothing
 * gets the wide one, which is what every test and the desktop board want.
 */
export function renderStage(view: GameView, viewport: Viewport = WIDE): SVGSVGElement {
  const world = view.world;
  const matchmakers = Array.isArray(world.matchmakers) ? world.matchmakers : [];
  const geo = geometryOf(view, viewport);
  const wiped = wipedNodes(view);
  const matchmade = matchmakers.length > 0;
  const bandCentre: Point = { x: WIDTH + MATCHMAKER_SIZE + 20, y: geo.height / 2 };

  const links: SVGElement[] = [];
  const dots: SVGElement[] = [];
  for (const [, messages] of groupByLink(world.wire)) {
    const first = messages[0];
    if (!first) continue;
    // The tier an endpoint belongs to is the engine's answer: matchmaker 0 and
    // node 0 are two processes, and only `from_party`/`to_party` tell them
    // apart.
    const from = endpointOf(first.from, first.from_party, geo.at, geo.bandAt);
    const to = endpointOf(first.to, first.to_party, geo.at, geo.bandAt);
    if (!from || !to) continue;
    // A narrow node keeps its labels under its disc, so a link is pulled in
    // further there: a dot that lands on a label hides a word.
    const ends = trim(from, to, geo.nodeRadius + (geo.narrow ? 12 : 4));
    links.push(
      svg('line', {
        class: 'link',
        x1: ends.from.x,
        y1: ends.from.y,
        x2: ends.to.x,
        y2: ends.to.y,
      }),
    );
    dotPositions(ends.from, ends.to, messages.length, geo.narrow ? 6 : 8).forEach((point, index) => {
      const message = messages[index];
      if (message) dots.push(messageDot(message, point, geo));
    });
  }

  const nodes = world.nodes.map((node) => {
    const point = geo.at.get(node.id) ?? geo.centre;
    return svg(
      'g',
      {},
      // A retired node draws no log: it shut down for good, and its log says
      // nothing about the cluster now. A narrow node draws none either: its
      // log is folded into the labels, and the inspector holds the column.
      node.retired === true || geo.narrow ? null : logColumn(node, point, geo.centre),
      nodeGroup(node, point, geo.cells?.get(node.id) ?? null, wiped.has(node.id), matchmade, geo),
    );
  });

  return svg(
    'svg',
    {
      class: `stage${geo.narrow ? ' narrow' : ''}`,
      viewBox: `0 0 ${geo.width} ${geo.height}`,
      preserveAspectRatio: 'xMidYMid meet',
      role: 'img',
      'aria-label': matchmade
        ? 'the cluster, the matchmakers, the logs and the messages in flight'
        : 'the cluster, the logs and the messages in flight',
    },
    chosenBanner(world, geo.width),
    // The client gutter is a column of prose beside the ring. A phone has no
    // room for it, and the panel's client history says the same thing.
    geo.narrow ? null : clientColumn(world),
    geo.shape ? gridLabels(geo.shape, geo) : null,
    matchmakerBand(matchmakers, bandCentre, geo),
    svg('g', { class: 'links' }, ...links),
    svg('g', { class: 'nodes' }, ...nodes),
    svg('g', { class: 'wire' }, ...dots),
  );
}
