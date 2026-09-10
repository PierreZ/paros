// The refusal: what the engine said when it did not play a move.
//
// A refused move leaves the board alone, so the player needs to read why next
// to the buttons that made it. The engine's own sentence comes first, because
// it carries this move's numbers and the reason — `handoff_refused` names the
// rule that closed the second hop, `amnesia` names the promise that is gone.
// The second line is what the player does next, and it is written here.

import type { ActionErrorCode, ErrorView } from '../types';
import { h } from '../render/dom';

/** What the player does after each refusal the game has a next step for. */
const ADVICE: Partial<Record<ActionErrorCode, string>> = {
  handoff_refused:
    'Only the node that won the ballot may give it away. To move the leadership again, run an election.',
  amnesia:
    'The disk of this node is empty, and a promise cannot come back. The node must stay out, and the cluster must change its acceptor set instead.',
  bad_column:
    'Give a column of this grid. The column of a slot is the slot number modulo the number of columns.',
  not_leader: 'Ask the leader. Only the leader puts a command in a slot.',
  no_matchmakers:
    'This cluster names no matchmakers. There is no place to record a second acceptor set, so the set it started with is the set it keeps.',
  handover_busy:
    'A matchmaker handover is already open at this node. Wait for it, or send its open step again.',
  no_handover: 'No matchmaker handover is open at this node. Ask for one first.',
  unknown_party: 'This process is not in this world.',
  node_crashed: 'Start the node again first.',
  node_alive: 'Stop the node first.',
  prompt_open: 'Answer the open question first.',
  pinned_off: 'This level teaches this decision. You must make it yourself.',
  not_unlocked: 'Pass the level that teaches this decision first.',
  not_allowed: 'This level does not offer this move.',
  bad_reach: 'Select at least one acceptor.',
  unknown_message: 'This message is not on the wire any more.',
  nothing_to_undo: 'You did not play a move yet.',
};

/**
 * The advice one refusal earns, or `null`.
 *
 * A code the game does not know yet gets none: the engine's own sentence is
 * always shown, and it is the part that carries the reason.
 */
export function refusalAdvice(code: string): string | null {
  return ADVICE[code as ActionErrorCode] ?? null;
}

/** The refusal block, or nothing when the last move landed. */
export function renderRefusal(error: ErrorView | null): HTMLElement | null {
  if (!error) return null;
  const advice = refusalAdvice(error.code);
  return h(
    'section',
    { class: 'refusal', role: 'status' },
    h('h2', {}, 'The game refuses this move'),
    h('p', { class: 'refusal-message' }, error.error),
    advice ? h('p', { class: 'refusal-advice' }, advice) : null,
    h('p', { class: 'refusal-code' }, h('code', {}, error.code)),
  );
}
