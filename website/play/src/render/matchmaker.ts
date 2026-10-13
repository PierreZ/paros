// The matchmaker tier: where it is drawn, and what each square says.
//
// A matchmaker is not an acceptor. It keeps one durable map from a ballot to
// an acceptor set, it holds no log, and it votes on no slot. So the stage
// draws it as a different shape — a rounded square — in a band of its own
// beside the acceptor ring, and it never lands in the ring.
//
// The two identity spaces are separate: matchmaker 0 and node 0 are two
// different processes. The engine says which tier an endpoint belongs to
// (`MessageView.from_party` / `MessageView.to_party`), and the frontend reads
// that field. It never works the tier out from the message's name.
//
// Every function here is pure, so the geometry and the labels are unit-tested.

import type { MatchmakerView, PartyView, RegistrationView } from '../types';
import type { Point } from './layout';

/** The side of one matchmaker's square, in SVG user units. */
export const MATCHMAKER_SIZE = 44;

/** How far apart two neighbour squares sit. */
export const MATCHMAKER_GAP = 96;

/**
 * How much width the band takes.
 *
 * The square is on the left of the band and the registry list is beside it, so
 * the band must hold both. The stage grows by this much when the deployment
 * names matchmakers, which keeps every other position exactly where it was.
 */
export const BAND_WIDTH = 250;

/** How many registrations one square lists before it counts the rest. */
export const MAX_REGISTRATIONS = 3;

/**
 * Where `count` squares sit, stacked down the band and centred on `centre`.
 *
 * A band with one matchmaker puts it on the centre line. A band with none
 * draws nothing.
 */
export function matchmakerPositions(
  count: number,
  centre: Point,
  gap: number = MATCHMAKER_GAP,
): Point[] {
  if (count <= 0) return [];
  const top = centre.y - (gap * (count - 1)) / 2;
  return Array.from({ length: count }, (_, index) => ({ x: centre.x, y: top + gap * index }));
}

/**
 * What a matchmaker's phase is called for the player.
 *
 * The words are the protocol's own: a matchmaker that is `stopped` is frozen
 * for its generation and registers nothing more.
 */
export function phaseWords(phase: string | null | undefined): string {
  const known: Record<string, string> = {
    fresh: 'fresh',
    inactive: 'inactive',
    active: 'active',
    stopped: 'frozen',
  };
  if (typeof phase !== 'string') return 'unknown';
  return known[phase] ?? phase;
}

/**
 * The CSS classes one matchmaker square carries.
 *
 * A crashed matchmaker is greyed and a frozen one has a dashed outline, so the
 * two states are told apart at a glance. A crashed matchmaker keeps its
 * registry, exactly as a crashed node keeps its disk.
 */
export function matchmakerClass(matchmaker: MatchmakerView): string {
  const parts = ['matchmaker'];
  if (matchmaker.alive === false) parts.push('crashed');
  const phase = typeof matchmaker.phase === 'string' ? matchmaker.phase : 'unknown';
  parts.push(`mm-${phase}`);
  if (phase === 'stopped') parts.push('frozen');
  return parts.join(' ');
}

/** The name the stage prints inside a matchmaker's square. */
export function matchmakerLabel(matchmaker: Pick<MatchmakerView, 'id'>): string {
  return `m${matchmaker.id}`;
}

/**
 * One registry row: which ballot registered which acceptors, and why.
 *
 * A `belief` is the set a candidate thought was in force. A `reconfiguration`
 * is a set an operator asked for, and the highest one a quorum holds is the
 * set in force.
 */
export function registrationLine(registration: RegistrationView): string {
  const members = Array.isArray(registration.members) ? registration.members : [];
  const kind = registration.kind === 'reconfiguration' ? 'change' : 'belief';
  return `${registration.ballot} → ${members.join(',') || '—'} · ${kind}`;
}

/**
 * The registry, as the band lists it: the newest rows first, then a count of
 * the rest.
 *
 * The engine sends the rows in ballot order, and the newest is the one the
 * player is working with.
 */
export function registryLines(
  matchmaker: Pick<MatchmakerView, 'registrations'>,
  max: number = MAX_REGISTRATIONS,
): string[] {
  const rows = Array.isArray(matchmaker.registrations) ? matchmaker.registrations : [];
  if (rows.length === 0) return ['no registration'];
  const newest = rows.slice(-max).reverse();
  const lines = newest.map(registrationLine);
  if (rows.length > max) lines.push(`+${rows.length - max} more`);
  return lines;
}

/**
 * Where one end of a message sits.
 *
 * The tier decides which map answers. A message whose endpoint the stage does
 * not draw — a matchmaker of a world that lists none — has no point, and the
 * caller then draws no link at all.
 */
export function endpointOf(
  id: number,
  party: PartyView | null | undefined,
  nodes: ReadonlyMap<number, Point>,
  matchmakers: ReadonlyMap<number, Point>,
): Point | null {
  const map = party === 'matchmaker' ? matchmakers : nodes;
  return map.get(id) ?? null;
}

/** How a message's endpoint is named in a list: `m0` for a matchmaker, `0` for a node. */
export function endpointName(id: number, party: PartyView | null | undefined): string {
  return party === 'matchmaker' ? `m${id}` : String(id);
}
