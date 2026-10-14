import { describe, expect, it } from 'vitest';

import { hashFor, parseHash, sameRoute } from './route';

describe('hash routing', () => {
  it('reads the level map from an empty hash', () => {
    expect(parseHash('')).toEqual({ kind: 'map' });
    expect(parseHash('#')).toEqual({ kind: 'map' });
    expect(parseHash('#/')).toEqual({ kind: 'map' });
  });

  it('reads a level id', () => {
    expect(parseHash('#act1/choose-a-value')).toEqual({
      kind: 'level',
      id: 'act1/choose-a-value',
    });
  });

  it('tolerates a leading or trailing slash', () => {
    expect(parseHash('#/act2/elect-a-leader/')).toEqual({
      kind: 'level',
      id: 'act2/elect-a-leader',
    });
  });

  it('decodes an escaped hash', () => {
    expect(parseHash('#act1%2Fthe-duel')).toEqual({ kind: 'level', id: 'act1/the-duel' });
  });

  it('survives a malformed escape', () => {
    expect(parseHash('#act1/%E0%A4%A')).toEqual({ kind: 'level', id: 'act1/%E0%A4%A' });
  });

  it('round-trips through hashFor', () => {
    const route = parseHash('#act1/adopt-the-value');
    expect(parseHash(hashFor(route))).toEqual(route);
    expect(hashFor({ kind: 'map' })).toBe('#');
  });

  it('compares routes', () => {
    expect(sameRoute({ kind: 'map' }, { kind: 'map' })).toBe(true);
    expect(sameRoute({ kind: 'map' }, { kind: 'level', id: 'a' })).toBe(false);
    expect(sameRoute({ kind: 'level', id: 'a' }, { kind: 'level', id: 'a' })).toBe(true);
    expect(sameRoute({ kind: 'level', id: 'a' }, { kind: 'level', id: 'b' })).toBe(false);
  });
});
