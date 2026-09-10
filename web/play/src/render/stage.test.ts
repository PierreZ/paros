import { describe, expect, it } from 'vitest';

import { controlLabel, isReply, phaseClass, roleLabel, slotClass, slotLabel } from './stage';
import type { MessageView, NodeView, SlotView } from '../types';

function slot(over: Partial<SlotView> = {}): SlotView {
  return {
    slot: 3,
    ballot: '1.0',
    value: 'alpha',
    control: null,
    chosen: false,
    applied: false,
    ...over,
  };
}

function node(over: Partial<NodeView> = {}): NodeView {
  return {
    id: 0,
    flavour: 'colocated',
    alive: true,
    role: null,
    attempt: null,
    ballot: null,
    leader: null,
    promised: null,
    accepted: [],
    chosen_index: null,
    first_unchosen: null,
    next_slot: null,
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
    applied: [],
    armed_seam: null,
    ...over,
  };
}

describe('a message is a request or a reply', () => {
  it("reads the engine's own answer, never the variant name", () => {
    const promise = { kind: 'Promise', reply: true } as unknown as MessageView;
    const prepare = { kind: 'Prepare', reply: false } as unknown as MessageView;
    // `Ack` in the name proves nothing: the field decides.
    const odd = { kind: 'HeartbeatAck', reply: false } as unknown as MessageView;
    expect(isReply(promise)).toBe(true);
    expect(isReply(prepare)).toBe(false);
    expect(isReply(odd)).toBe(false);
  });

  it('treats an engine that sends no field as asking', () => {
    expect(isReply({ kind: 'Promise' } as unknown as MessageView)).toBe(false);
  });
});

describe('phases', () => {
  it('names the known families and falls back to other', () => {
    expect(phaseClass('snapshot')).toBe('phase-snapshot');
    expect(phaseClass('prepare')).toBe('phase-prepare');
    expect(phaseClass('nonsense')).toBe('phase-other');
  });
});

describe('a slot box', () => {
  it('prints a client value as the client wrote it', () => {
    expect(slotLabel(slot({ value: 'alpha' }))).toBe('3: alpha');
    expect(slotClass(slot())).toBe('slot open');
  });

  it('names a control command instead of its text', () => {
    expect(slotLabel(slot({ value: 'Truncate up to 2', control: 'truncate' }))).toBe('3: Truncate');
    expect(slotLabel(slot({ value: 'Noop', control: 'noop' }))).toBe('3: Noop');
    expect(slotLabel(slot({ value: 'Snap at 4', control: 'snap' }))).toBe('3: Snap');
  });

  it('marks a control command, so the box does not look like a client value', () => {
    expect(slotClass(slot({ control: 'truncate', chosen: true }))).toBe('slot chosen control');
    expect(slotClass(slot({ chosen: true, applied: true }))).toBe('slot applied');
  });

  it('truncates a long value and keeps a short one', () => {
    expect(slotLabel(slot({ value: 'a-very-long-command' }))).toBe('3: a-very-lo…');
    expect(slotLabel(slot({ value: '' }))).toBe('3: ·');
  });

  it('leaves an unknown control kind alone', () => {
    expect(controlLabel('reconfigure')).toBe('reconfigure');
    expect(controlLabel(null)).toBeNull();
    expect(controlLabel(undefined)).toBeNull();
  });
});

describe('what the stage prints under a node', () => {
  it('prints the role of a log node', () => {
    expect(roleLabel(node({ role: 'leader' }))).toBe('leader');
    expect(roleLabel(node({ role: 'follower' }))).toBe('follower');
  });

  it('prints the attempt of a single-decree proposer, which holds no role', () => {
    expect(roleLabel(node({ flavour: 'proposer', attempt: 'phase1' }))).toBe('phase 1');
    expect(roleLabel(node({ flavour: 'proposer', attempt: 'preempted' }))).toBe('preempted');
    expect(roleLabel(node({ flavour: 'proposer', attempt: 'won' }))).toBe('won');
  });

  it('prints what a node is when it holds neither', () => {
    expect(roleLabel(node({ flavour: 'acceptor' }))).toBe('acceptor');
  });

  it('says a crashed node is crashed', () => {
    expect(roleLabel(node({ role: 'leader', alive: false }))).toBe('crashed');
  });
});
