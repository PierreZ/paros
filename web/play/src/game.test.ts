import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import { Game, decode, decodeLevels, isErrorView, type Engine } from './game';
import { latestNarration, narration, narrationStream } from './narration';
import fixture from './fixtures/game-view.json';
import { clientOps } from './ui/history';
import type { Action, GameView, MessageView } from './types';

const FIXTURE = JSON.stringify(fixture);

describe('decoding what the engine hands back', () => {
  it('reads a real GameView', () => {
    const outcome = decode(FIXTURE);
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    const view: GameView = outcome.view;
    expect(view.level.id).toBe('act1/choose-a-value');
    expect(view.world.flavour).toBe('decree');
    expect(view.world.nodes.length).toBeGreaterThan(0);
    expect(view.world.wire.length).toBeGreaterThan(0);
    expect(view.goal.status).toBe('open');
    // Numbers are numbers and ballots are `round.node` strings.
    expect(typeof view.world.wire[0]?.id).toBe('number');
    for (const message of view.world.wire) {
      if (message.ballot !== null) expect(message.ballot).toMatch(/^\d+\.\d+$/);
      expect(typeof message.reply).toBe('boolean');
    }
    // The decree world carries its reach sets, and a bare role holds an
    // attempt instead of a role.
    expect(view.world.reach?.one).toEqual([1, 2, 3]);
    expect(view.world.reach?.two).toEqual([1, 2, 3]);
    const proposer = view.world.nodes.find((entry) => entry.flavour === 'proposer');
    expect(proposer?.role).toBeNull();
    expect(proposer?.attempt).toBe('phase1');
    // The field guide is a bare book filename the frontend prefixes.
    expect(view.level.field_guide).not.toContain('/');
    expect(Array.isArray(view.level.unlocks)).toBe(true);
  });

  it('round-trips a view through JSON unchanged', () => {
    const outcome = decode(FIXTURE);
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(JSON.parse(JSON.stringify(outcome.view))).toEqual(fixture);
  });

  it('reads the last action\'s narration, and the whole stream from the log', () => {
    const outcome = decode(FIXTURE);
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    const lines = narration(outcome.view);
    expect(lines.length).toBeGreaterThan(0);
    expect(lines[0]?.text).toMatch(/Prepare/);
    // `GameView.narration` is the last move only; the log keeps every move's.
    expect(narrationStream(outcome.view).length).toBeGreaterThanOrEqual(lines.length);
    expect(latestNarration(outcome.view, 1)).toEqual([lines[lines.length - 1]]);
  });

  it('narrates nothing when the engine sends nothing', () => {
    const { narration: _dropped, ...withoutNarration } = fixture as Record<string, unknown>;
    const outcome = decode(JSON.stringify({ ...withoutNarration, log: [] }));
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(narration(outcome.view)).toEqual([]);
    expect(narrationStream(outcome.view)).toEqual([]);
  });

  it('drops narration entries that are not narration', () => {
    const outcome = decode(
      JSON.stringify({
        ...fixture,
        narration: [
          { kind: 'chosen', text: 'slot 0 is chosen' },
          { kind: 'bogus-shape' },
          { text: 'no kind at all' },
          'not even an object',
        ],
      }),
    );
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(narration(outcome.view)).toEqual([
      { kind: 'chosen', text: 'slot 0 is chosen' },
      { kind: 'info', text: 'no kind at all' },
    ]);
  });

  it('tells an ErrorView from a view', () => {
    const outcome = decode('{"code":"not_allowed","error":"this level does not offer tick"}');
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.error.code).toBe('not_allowed');
    expect(isErrorView(outcome.error)).toBe(true);
  });

  it('does not explode on junk', () => {
    expect(decode('{oops').ok).toBe(false);
    expect(decode('{"world":{}}').ok).toBe(false);
    expect(decodeLevels('null')).toEqual([]);
  });
});

describe('the Game wrapper', () => {
  function stubEngine(): Engine & { played: Action[] } {
    const played: Action[] = [];
    return {
      played,
      act(actionJson: string): string {
        const action = JSON.parse(actionJson) as Action;
        played.push(action);
        if (action.kind === 'tick') {
          return '{"code":"not_allowed","error":"this level does not offer tick"}';
        }
        return FIXTURE;
      },
      undo: () => FIXTURE,
      reset: () => FIXTURE,
      view: () => FIXTURE,
    };
  }

  it('keeps the last frame when a move is refused', () => {
    const engine = stubEngine();
    const game = new Game(engine);
    expect(game.act({ kind: 'deliver', id: 1 })).toBe(true);
    expect(game.lastError).toBeNull();

    expect(game.act({ kind: 'tick', node: 1 })).toBe(false);
    expect(game.lastError?.code).toBe('not_allowed');
    expect(game.view.level.id).toBe('act1/choose-a-value');

    expect(game.act({ kind: 'deliver', id: 2 })).toBe(true);
    expect(game.lastError).toBeNull();
    expect(engine.played).toHaveLength(3);
  });
});

// ---- the real engine, if the wasm has been built ----------------------------

const WASM = fileURLToPath(new URL('./wasm/paros_play_bg.wasm', import.meta.url));
const built = existsSync(WASM);

describe.skipIf(!built)('the real engine (wasm)', () => {
  async function engine(levelId: string): Promise<Game> {
    const module = await import('./wasm/paros_play.js');
    module.initSync({ module: readFileSync(WASM) });
    return new Game(new module.WasmGame(levelId));
  }

  it('lists the levels it knows', async () => {
    const module = await import('./wasm/paros_play.js');
    module.initSync({ module: readFileSync(WASM) });
    const levels = decodeLevels(module.WasmGame.levels());
    expect(levels.length).toBeGreaterThan(0);
    expect(levels[0]?.id).toBe('act1/choose-a-value');
    expect(levels.every((level) => level.act >= 1)).toBe(true);
  });

  it('plays act1/choose-a-value to its goal', async () => {
    const game = await engine('act1/choose-a-value');
    expect(game.view.goal.status).toBe('open');
    expect(game.view.world.wire).toHaveLength(0);

    const reference: Action[] = [
      { kind: 'open_ballot', proposer: 5, value: 'alpha' },
      { kind: 'deliver', id: 1 },
      { kind: 'deliver', id: 2 },
      { kind: 'drop', id: 3 },
      { kind: 'deliver', id: 4 },
      { kind: 'deliver', id: 5 },
      { kind: 'deliver', id: 6 },
      { kind: 'deliver', id: 7 },
      { kind: 'drop', id: 8 },
      { kind: 'deliver', id: 9 },
      { kind: 'deliver', id: 10 },
    ];
    for (const action of reference) {
      expect(game.act(action), `${action.kind} was refused: ${game.lastError?.error}`).toBe(true);
    }

    expect(game.view.goal.status).toBe('reached');
    // A value is plain text in the view: no Rust quoting, and no control
    // command, because a client wrote it.
    expect(game.view.world.chosen?.value).toBe('alpha');
    expect(game.view.world.chosen?.control).toBeNull();
    expect(game.view.mistakes).toBe(0);
  });

  it('refuses a move the level does not offer, and undo rewinds exactly', async () => {
    const game = await engine('act1/choose-a-value');
    expect(game.act({ kind: 'tick', node: 1 })).toBe(false);
    expect(game.lastError?.code).toBe('not_allowed');

    expect(game.act({ kind: 'open_ballot', proposer: 5, value: 'alpha' })).toBe(true);
    const opened = JSON.stringify(game.view.world);
    expect(game.act({ kind: 'deliver', id: 1 })).toBe(true);
    expect(JSON.stringify(game.view.world)).not.toBe(opened);

    game.undo();
    expect(JSON.stringify(game.view.world)).toBe(opened);

    game.reset();
    expect(game.view.log).toHaveLength(0);
    expect(game.view.world.wire).toHaveLength(0);
  });

  /**
   * Deliver every message the filter keeps, lowest id first, until nothing is
   * left — the frontend's copy of the engine's own `settle`. A level that
   * answers every role itself raises no prompt, so this loop needs none.
   */
  function settle(game: Game, keep: (message: MessageView) => boolean = () => true): void {
    for (let step = 0; step < 512; step += 1) {
      expect(game.view.prompt, 'this level answers every role itself').toBeNull();
      const ids = game.view.world.wire
        .filter(keep)
        .map((message) => message.id)
        .sort((a, b) => a - b);
      const next = ids[0];
      if (next === undefined) return;
      expect(game.act({ kind: 'deliver', id: next }), game.lastError?.error).toBe(true);
    }
    throw new Error('the world did not settle');
  }

  it('plays act3/truncate-by-consensus to its goal', async () => {
    const game = await engine('act3/truncate-by-consensus');
    expect(game.view.level.allowed_actions).toContain('compact');
    expect(game.view.world.clients).toHaveLength(1);
    const client = game.view.world.clients[0]?.id ?? 0;

    expect(game.act({ kind: 'start_election', node: 0 })).toBe(true);
    settle(game);
    expect(game.act({ kind: 'propose', node: 0, client, value: 'alpha', column: null })).toBe(true);
    expect(game.act({ kind: 'propose', node: 0, client, value: 'bravo', column: null })).toBe(true);
    settle(game);

    // No quorum holds a decided snapshot point yet, so the first request is
    // refused — and the refusal seeds the point the retry needs.
    expect(game.act({ kind: 'compact', node: 0, up_to: 8 })).toBe(true);
    settle(game);
    expect(game.act({ kind: 'compact', node: 0, up_to: 8 })).toBe(true);
    settle(game);

    expect(game.view.goal.status, game.view.goal.detail).toBe('reached');

    // One cluster-wide floor, and every node computed it by applying the same
    // decided command.
    const floors = new Set(game.view.world.nodes.map((node) => node.floor));
    expect(floors.size).toBe(1);
    expect([...floors][0]).toBeGreaterThan(0);

    // The control commands the act is about reach the view as control
    // commands, not as opaque client values.
    const controls = new Set(
      game.view.world.nodes.flatMap((node) =>
        node.accepted.map((slot) => slot.control).filter((control) => control !== null),
      ),
    );
    expect(controls).toContain('snap');
    expect([...controls].every((control) => ['noop', 'truncate', 'snap'].includes(control))).toBe(
      true,
    );
    expect(game.view.log.some((entry) => entry.kind === 'compact')).toBe(true);
    const said = game.view.log.flatMap((entry) => entry.narration.map((line) => line.kind));
    expect(said).toContain('truncate');

    // The client's writes are acknowledged, which is what the history panel
    // shows the player.
    const rows = clientOps(game.view);
    expect(rows).toHaveLength(2);
    expect(rows.every((row) => row.status === 'acked')).toBe(true);
  });
});
