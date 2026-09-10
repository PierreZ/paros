import { describe, expect, it } from 'vitest';

import { parseBallot } from '../ballot';
import { groupByLink } from './layout';
import {
  MATCHMAKER_GAP,
  endpointName,
  endpointOf,
  matchmakerClass,
  matchmakerLabel,
  matchmakerPositions,
  phaseWords,
  registrationLine,
  registryLines,
} from './matchmaker';
import type { MatchmakerView, RegistrationView } from '../types';

function matchmaker(over: Partial<MatchmakerView> = {}): MatchmakerView {
  return {
    id: 0,
    alive: true,
    generation: 0,
    phase: 'active',
    gc_watermark: '0.0',
    registrations: [],
    successor: null,
    ...over,
  };
}

function registration(over: Partial<RegistrationView> = {}): RegistrationView {
  return { ballot: '1.0', members: [0, 1, 2], kind: 'belief', ...over };
}

describe('where the band puts its squares', () => {
  it('stacks them down the band and centres the stack', () => {
    const points = matchmakerPositions(3, { x: 1000, y: 300 }, 100);
    expect(points).toEqual([
      { x: 1000, y: 200 },
      { x: 1000, y: 300 },
      { x: 1000, y: 400 },
    ]);
  });

  it('puts a lone matchmaker on the centre line, and draws none for none', () => {
    expect(matchmakerPositions(1, { x: 5, y: 6 })).toEqual([{ x: 5, y: 6 }]);
    expect(matchmakerPositions(0, { x: 5, y: 6 })).toEqual([]);
  });

  it('spaces two neighbours by the gap', () => {
    const [first, second] = matchmakerPositions(2, { x: 0, y: 0 });
    expect((second?.y ?? 0) - (first?.y ?? 0)).toBe(MATCHMAKER_GAP);
  });
});

describe('how a square is drawn', () => {
  it('marks a frozen matchmaker, which registers nothing more', () => {
    const classes = matchmakerClass(matchmaker({ phase: 'stopped' }));
    expect(classes).toContain('frozen');
    expect(classes).toContain('mm-stopped');
  });

  it('greys a matchmaker that is down, and keeps it apart from a frozen one', () => {
    expect(matchmakerClass(matchmaker({ alive: false }))).toContain('crashed');
    expect(matchmakerClass(matchmaker({ phase: 'stopped' }))).not.toContain('crashed');
  });

  it('names the phase in words, and leaves a phase from tomorrow alone', () => {
    expect(phaseWords('stopped')).toBe('frozen');
    expect(phaseWords('active')).toBe('active');
    expect(phaseWords('inactive')).toBe('inactive');
    expect(phaseWords('something-new')).toBe('something-new');
    expect(phaseWords(null)).toBe('unknown');
  });

  it('labels a matchmaker in its own identity space', () => {
    expect(matchmakerLabel({ id: 2 })).toBe('m2');
  });
});

describe('the registry a square lists', () => {
  it('says which ballot registered which acceptors, and why', () => {
    expect(registrationLine(registration())).toBe('1.0 → 0,1,2 · belief');
    expect(registrationLine(registration({ ballot: '2.0', kind: 'reconfiguration' }))).toBe(
      '2.0 → 0,1,2 · change',
    );
  });

  it('lists the newest rows first and counts the rest', () => {
    const rows = [
      registration({ ballot: '1.0' }),
      registration({ ballot: '2.0' }),
      registration({ ballot: '3.0' }),
      registration({ ballot: '4.0' }),
    ];
    const lines = registryLines(matchmaker({ registrations: rows }), 2);
    expect(lines[0]).toContain('4.0');
    expect(lines[1]).toContain('3.0');
    expect(lines[2]).toBe('+2 more');
  });

  it('says so when a matchmaker holds nothing', () => {
    expect(registryLines(matchmaker())).toEqual(['no registration']);
  });
});

describe('which tier an endpoint belongs to', () => {
  const nodes = new Map([[0, { x: 1, y: 1 }]]);
  const matchmakers = new Map([[0, { x: 9, y: 9 }]]);

  it("reads the engine's own answer, so node 0 and matchmaker 0 are two places", () => {
    expect(endpointOf(0, 'node', nodes, matchmakers)).toEqual({ x: 1, y: 1 });
    expect(endpointOf(0, 'matchmaker', nodes, matchmakers)).toEqual({ x: 9, y: 9 });
  });

  it('treats an endpoint with no party as a node, which is what every message was', () => {
    expect(endpointOf(0, null, nodes, matchmakers)).toEqual({ x: 1, y: 1 });
  });

  it('gives no point for an endpoint the stage does not draw', () => {
    expect(endpointOf(7, 'matchmaker', nodes, matchmakers)).toBeNull();
  });

  it('names an endpoint in its own space', () => {
    expect(endpointName(1, 'matchmaker')).toBe('m1');
    expect(endpointName(1, 'node')).toBe('1');
    expect(endpointName(1, undefined)).toBe('1');
  });
});

describe('the links messages travel on', () => {
  it('never puts node 0 and matchmaker 0 on one link', () => {
    const links = groupByLink([
      { from: 1, to: 0, from_party: 'node', to_party: 'node' },
      { from: 1, to: 0, from_party: 'node', to_party: 'matchmaker' },
    ]);
    expect(links.size).toBe(2);
    expect(links.get('1->0')).toHaveLength(1);
    expect(links.get('1->m0')).toHaveLength(1);
  });
});

describe('the ballot a Retire carries as evidence', () => {
  it('reads the round and the node out of the printed form', () => {
    expect(parseBallot('2.0')).toEqual({ round: 2, node: 0 });
    expect(parseBallot(' 12.3 ')).toEqual({ round: 12, node: 3 });
  });

  it('reads nothing out of anything else, so the request carries no evidence', () => {
    expect(parseBallot(null)).toBeNull();
    expect(parseBallot(undefined)).toBeNull();
    expect(parseBallot('')).toBeNull();
    expect(parseBallot('none')).toBeNull();
    expect(parseBallot('2')).toBeNull();
  });
});
