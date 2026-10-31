import { describe, expect, it } from 'vitest';

import { decodeLog, encodeLog, parseOptions, runLink } from './replay';
import type { Action } from './types';

const log: Action[] = [
  { kind: 'start_election', node: 1 },
  { kind: 'deliver', id: 3 },
  { kind: 'duplicate', id: 4, to: 2 },
];

describe('replay links', () => {
  it('round-trips an action log', () => {
    expect(decodeLog(encodeLog(log))).toEqual(log);
  });

  it('writes only URL-safe characters', () => {
    expect(encodeLog(log)).toMatch(/^[A-Za-z0-9_-]+$/);
  });

  it('refuses a value that is not an action log', () => {
    expect(decodeLog('not base64 !')).toBeNull();
    expect(decodeLog(encodeLog([] as Action[]).concat('x'))).toBeNull();
    expect(decodeLog(btoa('{"kind":"deliver"}'))).toBeNull();
    expect(decodeLog(btoa('[1,2]'))).toBeNull();
  });

  it('reads the page options', () => {
    expect(parseOptions('')).toEqual({ embed: false, log: null, theme: null });
    const options = parseOptions(`?embed&theme=dark&log=${encodeLog(log)}`);
    expect(options.embed).toBe(true);
    expect(options.theme).toBe('dark');
    expect(options.log).toEqual(log);
    expect(parseOptions('?theme=blue').theme).toBeNull();
  });

  it('builds a link that replays the run', () => {
    expect(runLink('/paros/play/', 'act1/the-duel', [])).toBe('/paros/play/#act1/the-duel');
    const link = runLink('/paros/play/', 'act1/the-duel', log);
    const [path, hash] = link.split('#');
    expect(hash).toBe('act1/the-duel');
    expect(parseOptions(path?.slice(path.indexOf('?')) ?? '').log).toEqual(log);
  });
});
