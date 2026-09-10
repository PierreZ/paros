// Reading a ballot the engine printed.
//
// A ballot is always the string `round.node` — the one encoding the contract
// fixes, and the same one the panel prints. The player must pass one back
// exactly once: a `Retire` carries the garbage-collection floor as evidence,
// and the operator reads that floor from the leader's own report.
//
// This is a read of a documented text format, not protocol reasoning. The
// frontend derives no ballot of its own and compares no two ballots.

import type { BallotSpec } from './types';

/**
 * The `round.node` string, as the two numbers an action carries.
 *
 * Anything the engine did not print in that shape answers `null`, and the
 * caller then sends no evidence at all.
 */
export function parseBallot(text: string | null | undefined): BallotSpec | null {
  if (typeof text !== 'string') return null;
  const match = /^(\d+)\.(\d+)$/.exec(text.trim());
  if (!match?.[1] || !match[2]) return null;
  const round = Number(match[1]);
  const node = Number(match[2]);
  if (!Number.isFinite(round) || !Number.isFinite(node)) return null;
  return { round, node };
}
