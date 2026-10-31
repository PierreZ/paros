// The compartments: proxy leaders and replicas, drawn in the side band.
//
// A proxy leader and a replica are processes beside the acceptors, and each
// has its own identity space: proxy 0 is not node 0, and replica 10 is not an
// acceptor. The stage draws them as squares in the same band as the
// matchmakers, never in the acceptor ring, because neither one votes.
//
// Every function here is pure, so the labels and the keys are unit-tested.

import type { PartyView, ProxyView, ReplicaView, WorldView } from '../types';

/** One square of the band that is not a matchmaker. */
export interface Helper {
  /** The band key: `p0` for proxy 0, `r10` for replica 10. */
  readonly key: string;
  /** Which tier. */
  readonly party: 'proxy' | 'replica';
  /** The id inside that tier. */
  readonly id: number;
  /** The short name inside the square. */
  readonly label: string;
  /** The lines under the square. */
  readonly lines: string[];
  /** False while the process is down. */
  readonly alive: boolean;
  /** The tooltip. */
  readonly title: string;
}

/** The band key of a non-node endpoint, or `null` for a node. */
export function bandKey(id: number, party: PartyView | string | null | undefined): string | null {
  if (party === 'matchmaker') return `m${id}`;
  if (party === 'proxy') return `p${id}`;
  if (party === 'replica') return `r${id}`;
  return null;
}

/** The lines a proxy square prints. */
export function proxyLines(proxy: ProxyView): string[] {
  if (proxy.alive === false) return ['down'];
  const open = Array.isArray(proxy.open_rounds) ? proxy.open_rounds : [];
  const lines = [`decided ${proxy.decided}`];
  lines.push(
    open.length === 0
      ? 'no open round'
      : `open: ${open.map((round) => `slot ${round.slot} for ${round.leader}`).join(', ')}`,
  );
  return lines;
}

/** The lines a replica square prints. */
export function replicaLines(replica: ReplicaView): string[] {
  const learned = Array.isArray(replica.learned) ? replica.learned : [];
  const lines = [
    replica.chosen_index === null ? 'learned nothing' : `learned to slot ${replica.chosen_index}`,
  ];
  if (replica.chosen_gap) {
    lines.push(`hole at ${replica.chosen_gap.hole}, chosen to ${replica.chosen_gap.highest}`);
  } else if (learned.length > 0) {
    lines.push(learned.slice(-3).map((slot) => slot.value).join(', '));
  }
  if (replica.caught_up > 0) lines.push(`caught up ${replica.caught_up}`);
  return lines;
}

/** Every proxy and replica of the world, in the order the band draws them. */
export function helpersOf(world: Pick<WorldView, 'proxies' | 'replicas'>): Helper[] {
  const proxies = Array.isArray(world.proxies) ? world.proxies : [];
  const replicas = Array.isArray(world.replicas) ? world.replicas : [];
  return [
    ...proxies.map(
      (proxy): Helper => ({
        key: `p${proxy.id}`,
        party: 'proxy',
        id: proxy.id,
        label: `p${proxy.id}`,
        lines: proxyLines(proxy),
        alive: proxy.alive !== false,
        title:
          proxy.alive === false
            ? `proxy ${proxy.id} is down. It kept nothing durable, so it lost nothing that was decided.`
            : `proxy ${proxy.id} works for ballot ${proxy.ballot ?? '—'}. It sends a delegated slot's Accept to the column, counts the votes and announces the Commit. It votes on nothing.`,
      }),
    ),
    ...replicas.map(
      (replica): Helper => ({
        key: `r${replica.id}`,
        party: 'replica',
        id: replica.id,
        label: `r${replica.id}`,
        lines: replicaLines(replica),
        alive: true,
        title: `replica ${replica.id} learns the chosen log and never votes.`,
      }),
    ),
  ];
}

/** The band's title for the squares it holds. */
export function bandTitle(matchmakers: number, helpers: readonly Helper[]): string {
  if (matchmakers > 0) return 'matchmakers';
  const proxies = helpers.some((helper) => helper.party === 'proxy');
  const replicas = helpers.some((helper) => helper.party === 'replica');
  if (proxies && replicas) return 'proxies · replicas';
  return proxies ? 'proxy leaders' : 'replicas';
}
