// The client history: what each client asked, and what it was told.
//
// Act III's last levels are judged on this list. A read must observe every
// write that was acknowledged before it started, and a watermark must not go
// backwards — so the player must be able to read the history the game judges,
// operation by operation, in the order each client asked.
//
// Every field is read defensively: a client the engine sends with no reads, or
// an engine that adds a field tomorrow, renders the same.

import type { ClientView, GameView, ProposalView, ReadView } from '../types';
import { h } from '../render/dom';

/** What the game knows about one client operation. */
export type OpStatus = 'pending' | 'ambiguous' | 'proposed' | 'acked' | 'read';

/** One row of the history. */
export interface OpRow {
  /** The client that asked. */
  client: number;
  /** Whether it is a write. */
  write: boolean;
  /** `write 2` or `read 5`. */
  name: string;
  /** The command the client wrote, or an empty string for a read. */
  value: string;
  /** The node the client asked. */
  node: number;
  /** The slot a write landed at, or the watermark a read observed. */
  slot: number | null;
  /** What the game can say about it. */
  status: OpStatus;
  /** One sentence for the player. */
  detail: string;
}

function array<T>(raw: unknown): T[] {
  return Array.isArray(raw) ? (raw as T[]) : [];
}

function slotOf(raw: unknown): number | null {
  return typeof raw === 'number' && Number.isFinite(raw) ? raw : null;
}

function writeRow(client: ClientView, proposal: ProposalView, alive: boolean): OpRow {
  const slot = slotOf(proposal.slot);
  const value = typeof proposal.value === 'string' ? proposal.value : '';
  const base = {
    client: client.id,
    write: true,
    name: `write ${proposal.seq}`,
    value,
    node: proposal.node,
    slot,
  };
  if (proposal.acked === true) {
    return {
      ...base,
      status: 'acked',
      detail: `Node ${proposal.node} applied it at slot ${slot ?? '?'} and acknowledged it.`,
    };
  }
  if (!alive) {
    return {
      ...base,
      status: 'ambiguous',
      detail:
        slot === null
          ? `Node ${proposal.node} is down. The write can still commit, so the result is ambiguous.`
          : `Node ${proposal.node} is down, and it holds the write at slot ${slot}. The write can still commit, so the result is ambiguous.`,
    };
  }
  if (slot !== null) {
    return {
      ...base,
      status: 'proposed',
      detail: `Node ${proposal.node} holds it at slot ${slot}. The client has no acknowledgement yet.`,
    };
  }
  return {
    ...base,
    status: 'pending',
    detail: `The client waits for node ${proposal.node} to give it a slot.`,
  };
}

function readRow(client: ClientView, read: ReadView, alive: boolean): OpRow {
  const index = slotOf(read.index);
  const base = {
    client: client.id,
    write: false,
    name: `read ${read.ctx}`,
    value: '',
    node: read.node,
    slot: index,
  };
  if (read.served === true) {
    return {
      ...base,
      status: 'read',
      detail: `Node ${read.node} served it at the watermark ${index === null ? 'unknown' : `slot ${index}`}.`,
    };
  }
  if (!alive) {
    return {
      ...base,
      status: 'ambiguous',
      detail: `Node ${read.node} is down. The read has no answer, so the result is ambiguous.`,
    };
  }
  return {
    ...base,
    status: 'pending',
    detail: `Node ${read.node} must prove that it is the leader now. Until it does, the read waits.`,
  };
}

/**
 * Every client operation this world has, writes first, in the order the client
 * asked.
 */
export function clientOps(view: GameView): OpRow[] {
  const world = view.world;
  const alive = new Map<number, boolean>();
  for (const node of array<{ id: number; alive: boolean }>(world.nodes)) {
    alive.set(node.id, node.alive !== false);
  }
  const rows: OpRow[] = [];
  for (const client of array<ClientView>(world.clients)) {
    for (const proposal of array<ProposalView>(client.proposals)) {
      rows.push(writeRow(client, proposal, alive.get(proposal.node) !== false));
    }
    for (const read of array<ReadView>(client.reads)) {
      rows.push(readRow(client, read, alive.get(read.node) !== false));
    }
  }
  return rows;
}

const STATUS_WORDS: Record<OpStatus, string> = {
  pending: 'pending',
  ambiguous: 'ambiguous',
  proposed: 'at a slot',
  acked: 'acknowledged',
  read: 'served',
};

/** The client history panel, or nothing when the level has no clients. */
export function renderHistory(view: GameView): HTMLElement | null {
  const clients = array<ClientView>(view.world.clients);
  if (clients.length === 0) return null;
  const rows = clientOps(view);
  return h(
    'section',
    { class: 'history' },
    h('h2', {}, 'The client history'),
    h(
      'p',
      { class: 'small' },
      'The game judges this list. A read must observe every write that the cluster acknowledged before the read started.',
    ),
    rows.length === 0
      ? h('p', { class: 'empty' }, 'The clients did nothing yet. Ask a node to write a command.')
      : h(
          'ol',
          { class: 'history-list' },
          ...rows.map((row) =>
            h(
              'li',
              { class: `history-op status-${row.status}` },
              h('span', { class: 'history-client' }, `client ${row.client}`),
              h('span', { class: 'history-name' }, row.value ? `${row.name} ${row.value}` : row.name),
              h('span', { class: 'history-status' }, STATUS_WORDS[row.status]),
              h('span', { class: 'history-detail' }, row.detail),
            ),
          ),
        ),
  );
}
