import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import { Game, decode, decodeLevels, isErrorView, type Engine } from './game';
import { latestNarration, narration, narrationStream } from './narration';
import fixture from './fixtures/game-view.json';
import type { Action, GameView } from './types';

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
    }
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
    // The engine renders a command with Rust's `Debug`, quotes and all.
    expect(game.view.world.chosen?.value).toBe('"alpha"');
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
});
