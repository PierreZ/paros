import { describe, expect, it } from 'vitest';

import { diskIsEmpty, wipedNodes } from './disk';
import { roleLabel } from './stage';
import type { ActionView, GameView, NodeView, SlotView } from '../types';

function slot(over: Partial<SlotView> = {}): SlotView {
  return { slot: 0, ballot: '1.0', value: 'alpha', control: null, chosen: true, applied: true, ...over };
}

/**
 * One node, with every field the contract has today.
 *
 * The literal is cast rather than typed: the engine adds fields to `NodeView`
 * as later acts land, and a fixture that has to grow with each of them tests
 * nothing extra. What the app reads is checked where the app reads it.
 */
function node(id: number, over: Partial<NodeView> = {}): NodeView {
  return {
    id,
    flavour: 'colocated',
    alive: true,
    role: 'follower',
    attempt: null,
    ballot: '1.0',
    leader: 0,
    promised: '1.0',
    accepted: [slot()],
    chosen_index: 0,
    first_unchosen: 1,
    next_slot: 1,
    chosen_gap: null,
    floor: 0,
    election: null,
    open_rounds: [],
    pending_accepts: false,
    read_rounds: [],
    recovery_remaining: 0,
    acceptors: [0, 1, 2],
    quorum_system: 'majority',
    quorum: { kind: 'majority', q1: null, q2: null, rows: null, cols: null },
    grid_cell: null,
    applied: [slot()],
    armed_seam: null,
    ...over,
  } as unknown as NodeView;
}

/** A node whose disk was erased: nothing on it, and it is not running. */
function erased(id: number): NodeView {
  return node(id, {
    alive: false,
    role: null,
    ballot: null,
    leader: null,
    promised: '0.0',
    accepted: [],
    applied: [],
    chosen_index: null,
  });
}

function entry(kind: string, label: string): ActionView {
  return { index: 0, kind, label, narration: [] } as unknown as ActionView;
}

function view(nodes: NodeView[], log: ActionView[] = []): GameView {
  return { world: { nodes }, log } as unknown as GameView;
}

describe('an empty disk', () => {
  it('holds no promise, no record and nothing applied', () => {
    expect(diskIsEmpty(erased(2))).toBe(true);
    expect(diskIsEmpty(node(2))).toBe(false);
    expect(diskIsEmpty(node(2, { accepted: [], applied: [], chosen_index: null }))).toBe(false);
  });
});

describe('which nodes the player erased', () => {
  it('draws none until a wipe is played', () => {
    expect(wipedNodes(view([node(0), erased(2)]))).toEqual(new Set());
  });

  it('names the node the log names, when that disk reads empty', () => {
    const log = [entry('wipe', "wipe node 2's disk")];
    expect(wipedNodes(view([node(0), node(1), erased(2)], log))).toEqual(new Set([2]));
  });

  it('never draws a crash as a lost disk', () => {
    // Node 1 is merely down: its promise and its records survived.
    const log = [entry('wipe', "wipe node 2's disk")];
    const down = node(1, { alive: false, role: null });
    expect(wipedNodes(view([node(0), down, erased(2)], log))).toEqual(new Set([2]));
  });

  it('takes the engine at its word when it reports the fact itself', () => {
    const reported = { ...node(2), wiped: true } as unknown as NodeView;
    // No log entry at all, and the node still reports a promise: the engine
    // said so, and the engine decides.
    expect(wipedNodes(view([node(0), reported]))).toEqual(new Set([2]));
  });

  it('does not explode on a view with no log and no nodes', () => {
    expect(wipedNodes({ world: {} } as unknown as GameView)).toEqual(new Set());
    expect(wipedNodes({} as unknown as GameView)).toEqual(new Set());
  });
});

describe('what the stage prints under an erased node', () => {
  it('says wiped, not crashed', () => {
    expect(roleLabel(erased(2), true)).toBe('wiped');
    expect(roleLabel(erased(2), false)).toBe('crashed');
    expect(roleLabel(node(0), false)).toBe('follower');
  });
});
