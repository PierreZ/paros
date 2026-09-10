import { describe, expect, it } from 'vitest';

import { handoffTargets, misrouteTargets, nodeControlsFor, proposeColumns } from './controls';
import {
  acceptorPool,
  acceptorsInForce,
  handoverMembers,
  matchmakerControlsFor,
  matchmakerPool,
  matchmakersInForce,
  newMatchmakerState,
  quorumSpecOf,
  reconfigureMembers,
  retireEvidence,
  retireTargets,
} from './matchmakers';
import { memberCount, phaseSize, quorumOf, quorumPanel } from './quorum';
import { refusalAdvice } from './refusal';
import type {
  ActionKind,
  GameView,
  MatchmakerView,
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

function matchmaker(id: number, over: Partial<MatchmakerView> = {}): MatchmakerView {
  return {
    id,
    alive: true,
    generation: 0,
    phase: 'active',
    gc_watermark: '0.0',
    registrations: [],
    successor: null,
    ...over,
  };
}

function view(
  allowed: ActionKind[],
  nodes: NodeView[],
  matchmakers: MatchmakerView[] = [],
): GameView {
  return {
    level: { allowed_actions: allowed },
    world: { nodes, clients: [], wire: [], matchmakers },
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

describe("Act IV's matchmaker plane", () => {
  const MATCHMADE: ActionKind[] = [
    'reconfigure',
    'retire',
    'reconfigure_matchmakers',
    'crash_matchmaker',
    'restart_matchmaker',
  ];

  describe('which resend a node row offers', () => {
    it('offers each of the three only where the level lists it', () => {
      const level = view(['resend_matchmaking', 'resend_gc', 'resend_reconfigurer'], [node(0)]);
      expect(nodeControlsFor(level, node(0))).toEqual([
        'resend_matchmaking',
        'resend_gc',
        'resend_reconfigurer',
      ]);
      expect(nodeControlsFor(view(['resend_gc'], [node(0)]), node(0))).toEqual(['resend_gc']);
    });

    it('does not ask a node that is down to send anything again', () => {
      const level = view(['resend_matchmaking', 'resend_gc', 'resend_reconfigurer'], [node(0)]);
      expect(nodeControlsFor(level, node(0, { alive: false }))).toEqual([]);
    });
  });

  describe('which controls a matchmaker row offers', () => {
    it('crashes one that runs and restarts one that does not', () => {
      const level = view(MATCHMADE, [node(0)], [matchmaker(0)]);
      expect(matchmakerControlsFor(level, matchmaker(0))).toEqual(['crash_matchmaker']);
      expect(matchmakerControlsFor(level, matchmaker(0, { alive: false }))).toEqual([
        'restart_matchmaker',
      ]);
    });

    it('offers nothing where the level lists neither', () => {
      expect(matchmakerControlsFor(view([], [node(0)], [matchmaker(0)]), matchmaker(0))).toEqual([]);
    });
  });

  describe('the set the reconfigure picker starts from', () => {
    it('reads the set in force off the leader, and never counts one', () => {
      const nodes = [
        node(0, { acceptors: [0, 1, 2, 3] }),
        node(1, { role: 'leader', acceptors: [0, 1, 2] }),
      ];
      const level = view(MATCHMADE, nodes);
      expect(acceptorsInForce(level)).toEqual([0, 1, 2]);
      expect(reconfigureMembers(level, newMatchmakerState())).toEqual([0, 1, 2]);
    });

    it('keeps what the player ticked once the player ticks something', () => {
      const level = view(MATCHMADE, [node(0, { role: 'leader' })]);
      const state = { ...newMatchmakerState(), reconfigureMembers: [0, 3] };
      expect(reconfigureMembers(level, state)).toEqual([0, 3]);
    });

    it('offers every node that has not retired', () => {
      const nodes = [node(0), node(1), node(2, { retired: true })];
      expect(acceptorPool(view(MATCHMADE, nodes))).toEqual([0, 1]);
      expect(retireTargets(view(MATCHMADE, nodes))).toEqual([0, 1]);
    });
  });

  describe('the quorum system a new set runs', () => {
    it('sends no numbers for a majority', () => {
      expect(quorumSpecOf(newMatchmakerState())).toBeNull();
    });

    it('sends the two sizes of a split, and the two of a grid', () => {
      const split = {
        ...newMatchmakerState(),
        reconfigureQuorum: 'flexible' as const,
        quorumSizes: { q1: '3', q2: '2', rows: '', cols: '' },
      };
      expect(quorumSpecOf(split)).toEqual({ kind: 'flexible', q1: 3, q2: 2 });
      const grid = {
        ...newMatchmakerState(),
        reconfigureQuorum: 'grid' as const,
        quorumSizes: { q1: '', q2: '', rows: '2', cols: '3' },
      };
      expect(quorumSpecOf(grid)).toEqual({ kind: 'grid', rows: 2, cols: 3 });
    });

    it('sends nothing for a shape the player did not finish', () => {
      const half = {
        ...newMatchmakerState(),
        reconfigureQuorum: 'grid' as const,
        quorumSizes: { q1: '', q2: '', rows: '2', cols: '' },
      };
      expect(quorumSpecOf(half)).toBeNull();
    });
  });

  describe('the evidence a Retire carries', () => {
    it("reads the floor off the leader's own report", () => {
      const nodes = [node(0, { gc: { effective_watermark: '2.0', retirable: [3] } }), node(1)];
      expect(retireEvidence(view(MATCHMADE, nodes), 0)).toEqual({ round: 2, node: 0 });
    });

    it('carries none where the node reports no floor, so the refusal can be played', () => {
      const nodes = [node(0), node(1)];
      expect(retireEvidence(view(MATCHMADE, nodes), 0)).toBeNull();
      expect(retireEvidence(view(MATCHMADE, nodes), 9)).toBeNull();
    });
  });

  describe('the matchmaker picker', () => {
    it('offers every matchmaker the deployment names, spares included', () => {
      const level = view(MATCHMADE, [node(0)], [matchmaker(0), matchmaker(3, { phase: 'inactive' })]);
      expect(matchmakerPool(level)).toEqual([0, 3]);
    });

    it('starts from the set a node believes authoritative', () => {
      const nodes = [node(0, { matchmakers: { generation: 1, members: [0, 1, 3] } })];
      const level = view(MATCHMADE, nodes, [matchmaker(0), matchmaker(1), matchmaker(3)]);
      expect(matchmakersInForce(level)).toEqual([0, 1, 3]);
      expect(handoverMembers(level, newMatchmakerState())).toEqual([0, 1, 3]);
    });

    it('falls back to the matchmakers that serve, where no node names a set', () => {
      const level = view(
        MATCHMADE,
        [node(0)],
        [matchmaker(0), matchmaker(1), matchmaker(3, { phase: 'inactive' })],
      );
      expect(matchmakersInForce(level)).toEqual([0, 1]);
    });
  });

  describe('what the new refusals tell the player to do', () => {
    it('names the next step for each', () => {
      expect(refusalAdvice('no_matchmakers')).toContain('names no matchmakers');
      expect(refusalAdvice('handover_busy')).toContain('already open');
      expect(refusalAdvice('no_handover')).toContain('Ask for one first');
    });
  });
});
