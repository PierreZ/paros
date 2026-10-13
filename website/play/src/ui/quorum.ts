// The quorum system, in words.
//
// Act IV takes the majority apart, so the panel must say which quorums are in
// force. The sentences come from `NodeView.quorum` — a structure, not a
// sentence — and the frontend never parses `NodeView.quorum_system` and never
// works a quorum size out for itself. A majority sends no numbers, so the
// panel says "a majority" and prints no count of its own.

import type { NodeView, QuorumSystemView } from '../types';

/** What the panel prints about the quorums in force. */
export interface QuorumPanel {
  /** Which family this is, in one short sentence. */
  headline: string;
  /** What answers Phase 1. */
  phaseOne: string;
  /** What votes in Phase 2. */
  phaseTwo: string;
  /** One more sentence the family earns, or `null`. */
  note: string | null;
}

function count(raw: unknown): number | null {
  return typeof raw === 'number' && Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : null;
}

/**
 * The quorum system the world runs, read from the first node that reports one.
 *
 * A world whose nodes send no quorum at all reports `null`, and the panel then
 * prints nothing rather than a guess.
 */
export function quorumOf(nodes: readonly NodeView[]): QuorumSystemView | null {
  for (const node of nodes) {
    const quorum = node.quorum;
    if (quorum && typeof quorum.kind === 'string') return quorum;
  }
  return null;
}

/** How many acceptors the configuration in force holds. */
export function memberCount(nodes: readonly NodeView[]): number {
  for (const node of nodes) {
    if (Array.isArray(node.acceptors) && node.acceptors.length > 0) return node.acceptors.length;
  }
  return nodes.length;
}

/** The quorum system, in sentences the player can read. */
export function quorumPanel(
  quorum: QuorumSystemView | null | undefined,
  members: number,
): QuorumPanel | null {
  if (!quorum) return null;
  const acceptors = `${members} acceptor${members === 1 ? '' : 's'}`;
  if (quorum.kind === 'grid') {
    const rows = count(quorum.rows);
    const cols = count(quorum.cols);
    if (rows === null || cols === null) return null;
    return {
      headline: `A grid of ${rows} rows and ${cols} columns.`,
      phaseOne: 'Phase 1 quorum: any full row.',
      phaseTwo: 'Phase 2 quorum: any full column.',
      note: `Each slot goes to one column. The column is the slot number modulo ${cols}.`,
    };
  }
  if (quorum.kind === 'flexible') {
    const q1 = count(quorum.q1);
    const q2 = count(quorum.q2);
    if (q1 === null || q2 === null) return null;
    return {
      headline: `A flexible split over ${acceptors}.`,
      phaseOne: `Phase 1: ${q1} of ${members}.`,
      phaseTwo: `Phase 2: ${q2} of ${members}.`,
      note: 'The two phases must meet. Two Phase-1 quorums must not.',
    };
  }
  return {
    headline: `A majority of ${acceptors}.`,
    phaseOne: 'Phase 1 quorum: a majority.',
    phaseTwo: 'Phase 2 quorum: a majority.',
    note: null,
  };
}

/**
 * How many acceptors one phase needs, when the engine says so.
 *
 * A majority and a grid answer `null`: a majority sends no number, and a
 * grid's quorum is a shape and not a count.
 */
export function phaseSize(
  quorum: QuorumSystemView | null | undefined,
  phase: 'one' | 'two',
): number | null {
  if (!quorum || quorum.kind !== 'flexible') return null;
  return count(phase === 'one' ? quorum.q1 : quorum.q2);
}
