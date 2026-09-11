import { describe, expect, it } from 'vitest';

import {
  controlLabel,
  isReply,
  logSummary,
  narrowMeta,
  nodeMeta,
  partyName,
  phaseClass,
  roleLabel,
  slotClass,
  slotLabel,
} from './stage';
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
    acceptors_since: null,
    matchmakers: null,
    matchmaking: null,
    gc: null,
    handover: null,
    retired: false,
    wiped: false,
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

  it('says a retired node is retired, because it does not come back', () => {
    expect(roleLabel(node({ role: 'follower', alive: false, retired: true }))).toBe('retired');
  });
});

describe('the badges under a node', () => {
  it('prints no acceptor set where the set is fixed for life', () => {
    const lines = nodeMeta(node({ promised: '1.0' }), null, false, false);
    expect(lines).toEqual(['promised 1.0']);
  });

  it('prints the set in force and the ballot it is bound to, where one can change', () => {
    const lines = nodeMeta(
      node({ acceptors: [0, 1, 2, 3], acceptors_since: '2.0' }),
      null,
      false,
      true,
    );
    expect(lines[0]).toBe('acceptors 0,1,2,3 · since 2.0');
  });

  it('names the matchmaker set and its generation', () => {
    const lines = nodeMeta(
      node({ matchmakers: { generation: 1, members: [0, 1, 3] } }),
      null,
      false,
      true,
    );
    expect(lines).toContain('matchmakers m0,m1,m3 · gen 1');
  });

  it('prints an open matchmaking phase, its kind and how many must still answer', () => {
    const lines = nodeMeta(
      node({
        matchmaking: { ballot: '2.1', config: [0, 1, 2], kind: 'reconfiguration', remaining: 1 },
      }),
      null,
      false,
      true,
    );
    expect(lines).toContain('matchmaking 2.1 · change');
    expect(lines).toContain('1 to answer');
  });

  it('prints the floor in force and the acceptors it released', () => {
    const lines = nodeMeta(
      node({ gc: { effective_watermark: '2.0', retirable: [3] } }),
      null,
      false,
      true,
    );
    expect(lines).toContain('gc floor 2.0');
    expect(lines).toContain('it frees 3');
  });

  it('prints no released list when the floor released nobody', () => {
    const lines = nodeMeta(
      node({ gc: { effective_watermark: '2.0', retirable: [] } }),
      null,
      false,
      true,
    );
    expect(lines).toContain('gc floor 2.0');
    expect(lines.some((line) => line.startsWith('it frees'))).toBe(false);
  });

  it('names the step of a matchmaker handover this node drives', () => {
    expect(nodeMeta(node({ handover: 'stopping' }), null, false, true)).toContain(
      'handover · stopping',
    );
  });

  it('says nothing else about a retired node, whose old state proves nothing', () => {
    expect(
      nodeMeta(node({ retired: true, promised: '3.0', chosen_index: 4 }), null, false, true),
    ).toEqual(['it does not come back']);
  });

  it('says nothing else about a wiped node either', () => {
    expect(nodeMeta(node({ promised: '3.0' }), null, true, true)).toEqual(['the disk is empty']);
  });
});

describe('naming a message endpoint', () => {
  it('says which tier the number belongs to', () => {
    expect(partyName(0, 'matchmaker')).toBe('matchmaker 0');
    expect(partyName(0, 'node')).toBe('node 0');
    expect(partyName(0, undefined)).toBe('node 0');
  });
});

describe('the folded log a narrow stage prints', () => {
  it('says nothing was accepted when nothing was', () => {
    expect(logSummary(node())).toEqual(['no slot yet']);
    expect(logSummary(node({ floor: 3 }))).toEqual(['floor 3 · no slot']);
  });

  it('counts what is chosen and what is applied, and names the top slot', () => {
    const lines = logSummary(
      node({
        accepted: [
          slot({ slot: 0, chosen: true, applied: true }),
          slot({ slot: 1, chosen: true }),
          slot({ slot: 2 }),
        ],
      }),
    );
    expect(lines[0]).toBe('2 chosen · 1 applied');
    expect(lines[1]).toBe('top slot 2');
  });

  it('names the floor beside the top slot, and the hole after both', () => {
    const lines = logSummary(
      node({
        floor: 2,
        accepted: [slot({ slot: 3, chosen: true })],
        chosen_gap: { hole: 4, highest: 6 },
      }),
    );
    expect(lines[1]).toBe('top slot 3 · floor 2');
    expect(lines[2]).toBe('hole at 4');
  });
});

describe('what a narrow stage prints under a node', () => {
  it('puts the role in the label block, because the disc is too small for it', () => {
    const lines = narrowMeta(node({ role: 'leader', promised: '1.0' }), null, false, false);
    expect(lines[0]).toEqual({ text: 'leader', kind: 'role' });
    expect(lines.some((line) => line.kind === 'summary')).toBe(true);
    expect(lines.map((line) => line.text)).toContain('promised 1.0');
  });

  it('keeps the grid badge first, where a grid is in force', () => {
    const lines = narrowMeta(node(), { row: 1, column: 2 }, false, false);
    expect(lines[0]).toEqual({ text: 'row 1 · col 2', kind: 'badge' });
    expect(lines[1]?.kind).toBe('role');
  });

  it('says nothing else about a retired node, or about a wiped one', () => {
    expect(
      narrowMeta(node({ retired: true, promised: '3.0', chosen_index: 4 }), null, false, true).map(
        (line) => line.text,
      ),
    ).toEqual(['retired', 'it stays down']);
    expect(narrowMeta(node({ promised: '3.0' }), null, true, true).map((line) => line.text)).toEqual([
      'wiped',
      'the disk is empty',
    ]);
  });

  it('never prints more lines than the layout made room for', () => {
    const lines = narrowMeta(
      node({
        role: 'leader',
        promised: '3.0',
        chosen_index: 4,
        acceptors: [0, 1, 2],
        matchmaking: { ballot: '3.0', config: [0, 1, 2], kind: 'belief', remaining: 1 },
        gc: { effective_watermark: '2.0', retirable: [3] },
        handover: 'stopping',
      }),
      { row: 0, column: 1 },
      false,
      true,
    );
    expect(lines.length).toBeLessThanOrEqual(6);
  });
});
