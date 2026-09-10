// The stage: one hand-drawn SVG, derived from the view on every change.
//
// There is no animation state and no scene graph to keep in step — the whole
// picture is a pure function of `GameView`. Interaction rides on data
// attributes (`data-msg`, `data-node`) and is handled by delegation in
// `main.ts`, so a re-render never has to re-bind anything.

import type { GameView, MessageView, NodeView, SlotView, WorldView } from '../types';
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
  type GridShape,
  type Point,
} from './layout';

const WIDTH = 960;
const HEIGHT = 620;
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
function floorOf(node: NodeView): number | null {
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

function electionRing(node: NodeView): SVGElement | null {
  const election = node.election;
  if (!election) return null;
  if (election.held) {
    return svg('circle', {
      class: 'election held',
      r: NODE_RADIUS + 7,
      cx: 0,
      cy: 0,
    });
  }
  if (election.timeout <= 0) return null;
  // No `elapsed` in the view contract yet: the ring shows the timeout's size,
  // one dash per tick, rather than pretending to know how far it has run.
  const radius = NODE_RADIUS + 7;
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

function nodeGroup(node: NodeView, at: Point, cell: Cell | null, wiped: boolean): SVGGElement {
  const under: string[] = [];
  const badge = cellBadge(cell);
  if (badge) under.push(badge);
  if (wiped) {
    under.push('the disk is empty');
  } else {
    if (node.promised) under.push(`promised ${node.promised}`);
    if (node.ballot) under.push(`ballot ${node.ballot}`);
    if (node.chosen_index !== null) under.push(`chosen ≤ ${node.chosen_index}`);
    if (node.leader !== null) under.push(`leader ${node.leader}`);
  }

  const ring = wiped ? null : electionRing(node);
  const labels = under.map((line, index) =>
    svg(
      'text',
      {
        class: index === 0 && badge ? 'node-meta cell-badge' : 'node-meta',
        x: 0,
        y: NODE_RADIUS + 16 + index * 12,
      },
      line,
    ),
  );
  const title = wiped
    ? `node ${node.id} lost its disk. It must not start again: a promise cannot come back.`
    : `node ${node.id} is ${roleLabel(node)}${node.promised ? `, and it promised ${node.promised}` : ''}`;

  return svg(
    'g',
    {
      class: nodeStateClass(node, wiped),
      transform: `translate(${at.x.toFixed(1)}, ${at.y.toFixed(1)})`,
      'data-node': node.id,
    },
    ring,
    svg('circle', { class: 'node-disc', r: NODE_RADIUS, cx: 0, cy: 0 }),
    svg('text', { class: 'node-id', x: 0, y: -2 }, String(node.id)),
    svg('text', { class: 'node-role', x: 0, y: 14 }, roleLabel(node, wiped)),
    ...labels,
    svg('title', {}, title),
  );
}

function messageDot(message: MessageView, at: Point): SVGGElement {
  // The column an Accept was addressed to colours the dot's edge: a grid
  // decides a slot by one whole column, so the player must see which one a
  // message belongs to. The engine names it (`MessageView.column`); the
  // frontend never works it out from the slot.
  const column = columnOf(message);
  const columnClasses = column === null ? '' : ` column ${columnClass(column)}`;
  const where = column === null ? '' : `, column ${column}`;
  return svg(
    'g',
    {
      class: `wire-dot ${phaseClass(message.phase)} ${isReply(message) ? 'reply' : 'request'}${columnClasses}`,
      'data-msg': message.id,
      transform: `translate(${at.x.toFixed(1)}, ${at.y.toFixed(1)})`,
      tabindex: 0,
      role: 'button',
      'aria-label': `${message.summary}, from node ${message.from} to node ${message.to}${where}. Click to deliver it.`,
    },
    svg('circle', { class: 'dot-hit', r: 13, cx: 0, cy: 0 }),
    svg('circle', { class: 'dot', r: 7, cx: 0, cy: 0 }),
    svg(
      'title',
      {},
      `${message.summary} (${message.from} → ${message.to})${where}. Click to deliver it.`,
    ),
  );
}

/** The row and column labels that name a grid's quorums. */
function gridLabels(shape: GridShape, centre: Point): SVGGElement {
  const marks: SVGElement[] = [];
  for (let row = 0; row < shape.rows; row += 1) {
    const at = gridPoint({ row, column: 0 }, shape, centre, GRID_GAP);
    marks.push(
      svg(
        'text',
        { class: 'grid-axis grid-row', x: at.x - ROW_LABEL_OFFSET, y: at.y + 4 },
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
    const at = gridPoint({ row: 0, column }, shape, centre, GRID_GAP);
    marks.push(
      svg(
        'text',
        { class: `grid-axis grid-col ${columnClass(column)}`, x: at.x, y: at.y - GRID_GAP.y * 0.5 },
        `col ${column}`,
      ),
    );
  }
  return svg('g', { class: 'grid-axes' }, ...marks);
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

function chosenBanner(world: WorldView): SVGGElement | null {
  if (!world.chosen) return null;
  const control = controlLabel(world.chosen.control);
  return svg(
    'g',
    { class: 'chosen-banner' },
    svg('rect', { x: WIDTH / 2 - 200, y: 12, width: 400, height: 34, rx: 6 }),
    svg(
      'text',
      { x: WIDTH / 2, y: 34 },
      `chosen: ${control ?? world.chosen.value} at ballot ${world.chosen.ballot}`,
    ),
  );
}

/** Draw the whole stage. */
export function renderStage(view: GameView): SVGSVGElement {
  const world = view.world;
  const hasClients = world.clients.length > 0;
  // A grid deployment is laid out as a grid: a row is a Phase-1 quorum and a
  // column is a Phase-2 quorum, and neither is legible on a ring. Every other
  // quorum system keeps the circle. A grid sits further right than a ring: it
  // spends its left margin on the row labels and on the leftmost logs.
  const shape = gridOf(world);
  const centre: Point = shape
    ? { x: hasClients ? 600 : 520, y: 320 }
    : { x: hasClients ? 560 : 480, y: 320 };
  const cells = shape ? gridCells(world, shape) : null;
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
  const wiped = wipedNodes(view);

  const links: SVGElement[] = [];
  const dots: SVGElement[] = [];
  for (const [, messages] of groupByLink(world.wire)) {
    const first = messages[0];
    if (!first) continue;
    const from = at.get(first.from);
    const to = at.get(first.to);
    if (!from || !to) continue;
    const ends = trim(from, to, NODE_RADIUS + 4);
    links.push(
      svg('line', {
        class: 'link',
        x1: ends.from.x,
        y1: ends.from.y,
        x2: ends.to.x,
        y2: ends.to.y,
      }),
    );
    dotPositions(ends.from, ends.to, messages.length).forEach((point, index) => {
      const message = messages[index];
      if (message) dots.push(messageDot(message, point));
    });
  }

  const nodes = world.nodes.map((node) => {
    const point = at.get(node.id) ?? centre;
    return svg(
      'g',
      {},
      logColumn(node, point, centre),
      nodeGroup(node, point, cells?.get(node.id) ?? null, wiped.has(node.id)),
    );
  });

  return svg(
    'svg',
    {
      class: 'stage',
      viewBox: `0 0 ${WIDTH} ${HEIGHT}`,
      preserveAspectRatio: 'xMidYMid meet',
      role: 'img',
      'aria-label': 'the cluster, the logs and the messages in flight',
    },
    chosenBanner(world),
    clientColumn(world),
    shape ? gridLabels(shape, centre) : null,
    svg('g', { class: 'links' }, ...links),
    svg('g', { class: 'nodes' }, ...nodes),
    svg('g', { class: 'wire' }, ...dots),
  );
}
