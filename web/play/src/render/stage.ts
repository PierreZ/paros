// The stage: one hand-drawn SVG, derived from the view on every change.
//
// There is no animation state and no scene graph to keep in step — the whole
// picture is a pure function of `GameView`. Interaction rides on data
// attributes (`data-msg`, `data-node`) and is handled by delegation in
// `main.ts`, so a re-render never has to re-bind anything.

import type { GameView, MessageView, NodeView, SlotView, WorldView } from '../types';
import { svg } from './dom';
import { circleLayout, dotPositions, groupByLink, trim, type Point } from './layout';

const WIDTH = 960;
const HEIGHT = 620;
const NODE_RADIUS = 32;
const SLOT_WIDTH = 92;
const SLOT_HEIGHT = 19;
const MAX_SLOTS = 6;

/** Replies are drawn hollow; requests solid. */
export function isReply(kind: string): boolean {
  return /(Ack|Promise|Accepted|Nack|Reply|Response|Learned)$/.test(kind);
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
 * A command's text, as the player should read it.
 *
 * The engine renders a user command with Rust's `Debug`, so `alpha` arrives as
 * `"alpha"`. Strip the quoting for display only — the raw string stays in the
 * view, and in the `title` a hover shows.
 */
export function displayValue(value: string): string {
  return value.length >= 2 && value.startsWith('"') && value.endsWith('"')
    ? value.slice(1, -1)
    : value;
}

/** A short label for one slot box. */
function slotLabel(slot: SlotView): string {
  const value = displayValue(slot.value) || '·';
  return `${slot.slot}: ${value.length > 10 ? `${value.slice(0, 9)}…` : value}`;
}

function slotClass(slot: SlotView): string {
  if (slot.applied) return 'slot applied';
  if (slot.chosen) return 'slot chosen';
  return 'slot open';
}

function nodeStateClass(node: NodeView): string {
  const parts = ['node'];
  if (!node.alive) parts.push('crashed');
  if (node.role === 'leader' || node.role === 'won') parts.push('leader');
  if (node.role === 'candidate' || node.role === 'phase 1') parts.push('candidate');
  return parts.join(' ');
}

function logColumn(node: NodeView, at: Point, centre: Point): SVGGElement {
  const toTheRight = at.x >= centre.x;
  const x = toTheRight ? at.x + NODE_RADIUS + 10 : at.x - NODE_RADIUS - 10 - SLOT_WIDTH;
  const shown = node.accepted.slice(0, MAX_SLOTS);
  const rows: (SVGGElement | SVGTextElement)[] = [];
  const total = shown.length + (node.chosen_gap ? 1 : 0);
  const top = at.y - ((total * (SLOT_HEIGHT + 3)) / 2 - 2);

  shown.forEach((slot, index) => {
    const y = top + index * (SLOT_HEIGHT + 3);
    const box = svg(
      'g',
      { class: slotClass(slot) },
      svg('rect', { x, y, width: SLOT_WIDTH, height: SLOT_HEIGHT, rx: 3 }),
      svg('text', { x: x + 5, y: y + 13, class: 'slot-label' }, slotLabel(slot)),
      svg(
        'title',
        {},
        `slot ${slot.slot} — value ${displayValue(slot.value) || '(empty)'}, ballot ${slot.ballot ?? 'none'}${
          slot.chosen ? ', chosen' : ''
        }${slot.applied ? ', applied' : ''}`,
      ),
    );
    rows.push(box);
  });

  if (node.chosen_gap) {
    const y = top + shown.length * (SLOT_HEIGHT + 3);
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
          `slot ${node.chosen_gap.hole} is undecided while ${node.chosen_gap.highest} is already chosen`,
        ),
      ),
    );
  }

  if (node.accepted.length > MAX_SLOTS) {
    const y = top + total * (SLOT_HEIGHT + 3) + 11;
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

function nodeGroup(node: NodeView, at: Point): SVGGElement {
  const under: string[] = [];
  if (node.promised) under.push(`promised ${node.promised}`);
  if (node.ballot) under.push(`ballot ${node.ballot}`);
  if (node.chosen_index !== null) under.push(`chosen ≤ ${node.chosen_index}`);
  if (node.leader !== null) under.push(`leader ${node.leader}`);

  const ring = electionRing(node);
  const labels = under.map((line, index) =>
    svg('text', { class: 'node-meta', x: 0, y: NODE_RADIUS + 16 + index * 12 }, line),
  );

  return svg(
    'g',
    {
      class: nodeStateClass(node),
      transform: `translate(${at.x.toFixed(1)}, ${at.y.toFixed(1)})`,
      'data-node': node.id,
    },
    ring,
    svg('circle', { class: 'node-disc', r: NODE_RADIUS, cx: 0, cy: 0 }),
    svg('text', { class: 'node-id', x: 0, y: -2 }, String(node.id)),
    svg('text', { class: 'node-role', x: 0, y: 14 }, node.role ?? node.flavour),
    ...labels,
    svg(
      'title',
      {},
      `node ${node.id} — ${node.alive ? node.role ?? node.flavour : 'crashed'}${
        node.promised ? `, promised ${node.promised}` : ''
      }`,
    ),
  );
}

function messageDot(message: MessageView, at: Point): SVGGElement {
  const reply = isReply(message.kind);
  return svg(
    'g',
    {
      class: `wire-dot ${phaseClass(message.phase)} ${reply ? 'reply' : 'request'}`,
      'data-msg': message.id,
      transform: `translate(${at.x.toFixed(1)}, ${at.y.toFixed(1)})`,
      tabindex: 0,
      role: 'button',
      'aria-label': `${message.summary} — from ${message.from} to ${message.to}. Click to deliver.`,
    },
    svg('circle', { class: 'dot-hit', r: 13, cx: 0, cy: 0 }),
    svg('circle', { class: 'dot', r: 7, cx: 0, cy: 0 }),
    svg('title', {}, `${message.summary} (${message.from} → ${message.to}) — click to deliver`),
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
        ? 'applied'
        : proposal.slot !== null
          ? `slot ${proposal.slot}`
          : 'in flight';
      rows.push(
        svg('text', { class: 'client-line', x: 20, y }, `#${proposal.seq} ${displayValue(proposal.value)} — ${state}`),
      );
      y += 14;
    }
    for (const read of client.reads.slice(-3)) {
      rows.push(
        svg(
          'text',
          { class: 'client-line', x: 20, y },
          `read ${read.ctx} — ${read.served ? `served at ${read.index ?? '?'}` : 'waiting'}`,
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
  return svg(
    'g',
    { class: 'chosen-banner' },
    svg('rect', { x: WIDTH / 2 - 200, y: 12, width: 400, height: 34, rx: 6 }),
    svg(
      'text',
      { x: WIDTH / 2, y: 34 },
      `chosen: ${displayValue(world.chosen.value)} at ballot ${world.chosen.ballot}`,
    ),
  );
}

/** Draw the whole stage. */
export function renderStage(view: GameView): SVGSVGElement {
  const world = view.world;
  const hasClients = world.clients.length > 0;
  const centre: Point = { x: hasClients ? 560 : 480, y: 320 };
  const radius = world.nodes.length > 4 ? 210 : 185;
  const positions = circleLayout(world.nodes.length, centre, radius);
  const at = new Map<number, Point>();
  world.nodes.forEach((node, index) => {
    at.set(node.id, positions[index] ?? centre);
  });

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
    return svg('g', {}, logColumn(node, point, centre), nodeGroup(node, point));
  });

  return svg(
    'svg',
    {
      class: 'stage',
      viewBox: `0 0 ${WIDTH} ${HEIGHT}`,
      preserveAspectRatio: 'xMidYMid meet',
      role: 'img',
      'aria-label': 'the cluster, its logs and the messages in flight',
    },
    chosenBanner(world),
    clientColumn(world),
    svg('g', { class: 'links' }, ...links),
    svg('g', { class: 'nodes' }, ...nodes),
    svg('g', { class: 'wire' }, ...dots),
  );
}
