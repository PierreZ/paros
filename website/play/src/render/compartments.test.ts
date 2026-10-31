import { describe, expect, it } from 'vitest';

import { bandKey, bandTitle, helpersOf, proxyLines, replicaLines } from './compartments';
import { groupByLink } from './layout';
import { endpointName } from './matchmaker';
import type { ProxyView, ReplicaView } from '../types';

const proxy = (id: number, alive = true): ProxyView => ({
  id,
  alive,
  ballot: alive ? '1.1' : null,
  open_rounds: alive ? [{ slot: 3, leader: 1 }] : [],
  decided: 2,
  refanned: 0,
});

const replica = (id: number): ReplicaView => ({
  id,
  chosen_index: 1,
  chosen_gap: { hole: 2, highest: 3 },
  learned: [],
  caught_up: 0,
});

describe('the compartments band', () => {
  it('keys every tier apart, so proxy 0 is never node 0', () => {
    expect(bandKey(0, 'proxy')).toBe('p0');
    expect(bandKey(10, 'replica')).toBe('r10');
    expect(bandKey(0, 'matchmaker')).toBe('m0');
    expect(bandKey(0, 'node')).toBeNull();
    expect(endpointName(0, 'proxy')).toBe('p0');
    expect(endpointName(10, 'replica')).toBe('r10');
    const links = groupByLink([
      { from: 0, to: 0, from_party: 'node', to_party: 'proxy' },
      { from: 0, to: 0, from_party: 'node', to_party: 'node' },
    ]);
    expect(links.size).toBe(2);
  });

  it('lists proxies first, then replicas, and names the band for what it holds', () => {
    const helpers = helpersOf({ proxies: [proxy(0), proxy(1, false)], replicas: [replica(10)] });
    expect(helpers.map((helper) => helper.key)).toEqual(['p0', 'p1', 'r10']);
    expect(helpers[1]?.alive).toBe(false);
    expect(bandTitle(0, helpers)).toBe('proxies · replicas');
    expect(bandTitle(0, helpers.slice(0, 2))).toBe('proxy leaders');
    expect(bandTitle(2, helpers)).toBe('matchmakers');
  });

  it('prints what a proxy holds and where a replica waits', () => {
    expect(proxyLines(proxy(0))).toEqual(['decided 2', 'open: slot 3 for 1']);
    expect(proxyLines(proxy(1, false))).toEqual(['down']);
    expect(replicaLines(replica(10))).toEqual(['learned to slot 1', 'hole at 2, chosen to 3']);
  });
});
