import { describe, expect, it } from 'vitest';

import { clientOps } from './history';
import type { GameView } from '../types';

interface Shape {
  nodes?: unknown[];
  clients?: unknown[];
}

function view(world: Shape): GameView {
  return { world: { nodes: [], clients: [], ...world } } as unknown as GameView;
}

const UP = [{ id: 0, alive: true }];
const DOWN = [{ id: 0, alive: false }];

describe('the client history', () => {
  it('reads a write that waits for a slot', () => {
    const rows = clientOps(
      view({
        nodes: UP,
        clients: [
          {
            id: 7,
            proposals: [{ seq: 1, value: 'alpha', node: 0, slot: null, acked: false }],
            reads: [],
          },
        ],
      }),
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]?.status).toBe('pending');
    expect(rows[0]?.name).toBe('write 1');
    expect(rows[0]?.client).toBe(7);
  });

  it('reads a write that holds a slot but has no acknowledgement', () => {
    const rows = clientOps(
      view({
        nodes: UP,
        clients: [
          {
            id: 7,
            proposals: [{ seq: 2, value: 'bravo', node: 0, slot: 1, acked: false }],
            reads: [],
          },
        ],
      }),
    );
    expect(rows[0]?.status).toBe('proposed');
    expect(rows[0]?.slot).toBe(1);
  });

  it('reads an acknowledged write and the slot it landed at', () => {
    const rows = clientOps(
      view({
        nodes: UP,
        clients: [
          {
            id: 7,
            proposals: [{ seq: 2, value: 'bravo', node: 0, slot: 1, acked: true }],
            reads: [],
          },
        ],
      }),
    );
    expect(rows[0]?.status).toBe('acked');
    expect(rows[0]?.detail).toContain('slot 1');
  });

  it('calls a write ambiguous when the node that holds its slot is down', () => {
    const rows = clientOps(
      view({
        nodes: DOWN,
        clients: [
          {
            id: 7,
            proposals: [{ seq: 1, value: 'alpha', node: 0, slot: 0, acked: false }],
            reads: [],
          },
        ],
      }),
    );
    expect(rows[0]?.status).toBe('ambiguous');
    expect(rows[0]?.detail).toContain('slot 0');
  });

  it('calls an operation ambiguous when the node it asked is down', () => {
    const rows = clientOps(
      view({
        nodes: DOWN,
        clients: [
          {
            id: 7,
            proposals: [{ seq: 1, value: 'alpha', node: 0, slot: null, acked: false }],
            reads: [{ ctx: 4, node: 0, index: null, served: false }],
          },
        ],
      }),
    );
    expect(rows.map((row) => row.status)).toEqual(['ambiguous', 'ambiguous']);
  });

  it('reads a served read and its watermark', () => {
    const rows = clientOps(
      view({
        nodes: UP,
        clients: [
          { id: 8, proposals: [], reads: [{ ctx: 5, node: 0, index: 2, served: true }] },
        ],
      }),
    );
    expect(rows[0]?.status).toBe('read');
    expect(rows[0]?.slot).toBe(2);
    expect(rows[0]?.write).toBe(false);
  });

  it('keeps every client, in the order the engine sends them', () => {
    const rows = clientOps(
      view({
        nodes: UP,
        clients: [
          {
            id: 7,
            proposals: [{ seq: 1, value: 'alpha', node: 0, slot: 0, acked: true }],
            reads: [],
          },
          { id: 8, proposals: [], reads: [{ ctx: 9, node: 0, index: 0, served: true }] },
        ],
      }),
    );
    expect(rows.map((row) => row.client)).toEqual([7, 8]);
  });

  it('survives a world the engine sends with no clients at all', () => {
    expect(clientOps(view({}))).toEqual([]);
    expect(clientOps({ world: {} } as unknown as GameView)).toEqual([]);
  });
});
