import { describe, expect, it } from 'vitest';

import { wipedNodes } from './disk';
import { roleLabel } from './stage';
import type { GameView, NodeView, SlotView } from '../types';

function slot(over: Partial<SlotView> = {}): SlotView {
  return { slot: 0, ballot: '1.0', value: 'alpha', control: null, chosen: true, applied: true, ...over };
}

/**
 * One node, with the fields the stage reads.
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
    retired: false,
    wiped: false,
    ...over,
  } as unknown as NodeView;
}

/** A node the engine reports as wiped: nothing on its disk, and it is not running. */
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
    wiped: true,
  });
}

function view(nodes: NodeView[]): GameView {
  return { world: { nodes } } as unknown as GameView;
}

describe('which nodes the player erased', () => {
  it('draws none until the engine reports a wipe', () => {
    expect(wipedNodes(view([node(0), node(1)]))).toEqual(new Set());
  });

  it('names the nodes the engine reports as wiped', () => {
    expect(wipedNodes(view([node(0), node(1), erased(2)]))).toEqual(new Set([2]));
  });

  it('never draws a crash as a lost disk', () => {
    // Node 1 is merely down: its promise and its records survived.
    const down = node(1, { alive: false, role: null });
    expect(wipedNodes(view([node(0), down, erased(2)]))).toEqual(new Set([2]));
  });

  it('does not explode on a view with no nodes', () => {
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
