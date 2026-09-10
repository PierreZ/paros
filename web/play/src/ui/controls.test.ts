import { describe, expect, it } from 'vitest';

import { handoffTargets, misrouteTargets, nodeControlsFor, proposeColumns } from './controls';
import { memberCount, phaseSize, quorumOf, quorumPanel } from './quorum';
import { refusalAdvice } from './refusal';
import type {
  ActionKind,
  GameView,
  MessageView,
  NodeView,
  QuorumSystemView,
} from '../types';

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
  } as unknown as NodeView;
}

function view(allowed: ActionKind[], nodes: NodeView[]): GameView {
  return {
    level: { allowed_actions: allowed },
    world: { nodes, clients: [], wire: [] },
  } as unknown as GameView;
}

const GRID: QuorumSystemView = { kind: 'grid', q1: null, q2: null, rows: 2, cols: 3 };
const FLEXIBLE: QuorumSystemView = { kind: 'flexible', q1: 3, q2: 2, rows: null, cols: null };

describe('which controls a node row offers', () => {
  it('offers only what the level lists', () => {
    const plain = view(['tick', 'crash'], [node(0)]);
    expect(nodeControlsFor(plain, node(0))).toEqual(['tick', 'crash']);
    expect(nodeControlsFor(view([], [node(0)]), node(0))).toEqual([]);
  });

  it("offers Act IV's own verbs when the level lists them", () => {
    const level = view(['quorum_read', 'relinquish', 'corrupt', 'wipe'], [node(0)]);
    expect(nodeControlsFor(level, node(0))).toEqual([
      'quorum_read',
      'relinquish',
      'corrupt',
      'wipe',
    ]);
  });

  it('does not tick, elect, read or hand off a node that is down', () => {
    const level = view(
      ['tick', 'start_election', 'quorum_read', 'relinquish', 'crash', 'restart'],
      [node(0)],
    );
    expect(nodeControlsFor(level, node(0, { alive: false }))).toEqual(['restart']);
  });

  it('does not restart a node that runs', () => {
    const level = view(['restart', 'crash'], [node(0)]);
    expect(nodeControlsFor(level, node(0))).toEqual(['crash']);
    expect(nodeControlsFor(level, node(0, { alive: false }))).toEqual(['restart']);
  });

  it('damages and erases a disk whether the node runs or not', () => {
    // The damage shows up at the next boot, which is what the level is about.
    const level = view(['corrupt', 'wipe'], [node(0)]);
    expect(nodeControlsFor(level, node(0))).toEqual(['corrupt', 'wipe']);
    expect(nodeControlsFor(level, node(0, { alive: false }))).toEqual(['corrupt', 'wipe']);
  });
});

describe('the peer a leader hands its authority to', () => {
  it('offers every other node, and lets the engine refuse', () => {
    const nodes = [node(0), node(1), node(2)];
    expect(handoffTargets(view(['relinquish'], nodes), nodes[0] as NodeView)).toEqual([1, 2]);
    expect(handoffTargets(view(['relinquish'], nodes), nodes[2] as NodeView)).toEqual([0, 1]);
  });
});

describe('misrouting a copy of a message', () => {
  const nodes = [node(0), node(1), node(2)];
  const message = { id: 1, from: 0, to: 1 } as MessageView;

  it('offers every node except the addressee', () => {
    expect(misrouteTargets(view(['duplicate'], nodes), message)).toEqual([0, 2]);
  });

  it('offers nothing when the level does not duplicate', () => {
    expect(misrouteTargets(view(['deliver'], nodes), message)).toEqual([]);
  });
});

describe('the column a write may name', () => {
  it('offers one option per column of the grid', () => {
    expect(proposeColumns(view(['propose'], [node(0, { quorum: GRID })]))).toEqual([0, 1, 2]);
  });

  it('offers none where the deployment runs no grid', () => {
    expect(proposeColumns(view(['propose'], [node(0)]))).toBeNull();
    expect(proposeColumns(view(['propose'], [node(0, { quorum: FLEXIBLE })]))).toBeNull();
  });
});

describe('the quorums, in words', () => {
  it('says a grid is two shapes and not two counts', () => {
    const panel = quorumPanel(GRID, 6);
    expect(panel?.headline).toBe('A grid of 2 rows and 3 columns.');
    expect(panel?.phaseOne).toBe('Phase 1 quorum: any full row.');
    expect(panel?.phaseTwo).toBe('Phase 2 quorum: any full column.');
    expect(panel?.note).toContain('modulo 3');
  });

  it('prints a flexible split as the two sizes the engine sent', () => {
    const panel = quorumPanel(FLEXIBLE, 4);
    expect(panel?.phaseOne).toBe('Phase 1: 3 of 4.');
    expect(panel?.phaseTwo).toBe('Phase 2: 2 of 4.');
  });

  it('works no majority out for itself, because the engine sends no number', () => {
    const panel = quorumPanel(
      { kind: 'majority', q1: null, q2: null, rows: null, cols: null },
      3,
    );
    expect(panel?.headline).toBe('A majority of 3 acceptors.');
    expect(panel?.phaseOne).toBe('Phase 1 quorum: a majority.');
    expect(panel?.note).toBeNull();
  });

  it('prints nothing when the engine sends no quorum at all', () => {
    expect(quorumPanel(null, 3)).toBeNull();
    expect(quorumPanel(undefined, 3)).toBeNull();
    const halfGrid = { kind: 'grid', q1: null, q2: null, rows: 2, cols: null } as QuorumSystemView;
    expect(quorumPanel(halfGrid, 6)).toBeNull();
  });

  it('gives a phase size only where the engine states one', () => {
    expect(phaseSize(FLEXIBLE, 'one')).toBe(3);
    expect(phaseSize(FLEXIBLE, 'two')).toBe(2);
    expect(phaseSize(GRID, 'one')).toBeNull();
    expect(phaseSize(null, 'two')).toBeNull();
  });

  it('reads the system and the membership off the nodes', () => {
    const nodes = [node(0, { quorum: FLEXIBLE, acceptors: [0, 1, 2, 3] }), node(1)];
    expect(quorumOf(nodes)).toBe(FLEXIBLE);
    expect(memberCount(nodes)).toBe(4);
    expect(memberCount([])).toBe(0);
  });
});

describe('what a refusal tells the player to do', () => {
  it('adds a next step to the refusals that have one', () => {
    expect(refusalAdvice('handoff_refused')).toContain('run an election');
    expect(refusalAdvice('amnesia')).toContain('promise cannot come back');
    expect(refusalAdvice('bad_column')).toContain('modulo');
  });

  it('adds nothing to a code it does not know', () => {
    expect(refusalAdvice('a_code_from_tomorrow')).toBeNull();
  });
});
