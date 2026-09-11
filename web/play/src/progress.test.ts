import { describe, expect, it } from 'vitest';

import {
  STORAGE_KEY,
  clear,
  foldBriefing,
  isBriefingFolded,
  isPassed,
  load,
  recordAttempt,
  recordPass,
  save,
  type StorageLike,
} from './progress';

class FakeStorage implements StorageLike {
  readonly items = new Map<string, string>();
  throwOnWrite = false;

  getItem(key: string): string | null {
    return this.items.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    if (this.throwOnWrite) throw new Error('quota');
    this.items.set(key, value);
  }
}

describe('the progress store', () => {
  it('starts empty', () => {
    const store = new FakeStorage();
    expect(load(store)).toEqual({ levels: {}, unlocked: [], folded: [] });
  });

  it('records a pass with the automation it unlocks', () => {
    const store = new FakeStorage();
    recordPass('act1/be-the-acceptor', 2, ['acceptor_replies'], store);
    const progress = load(store);
    expect(progress.levels['act1/be-the-acceptor']).toEqual({ passed: true, mistakes: 2 });
    expect(progress.unlocked).toEqual(['acceptor_replies']);
    expect(isPassed('act1/be-the-acceptor', store)).toBe(true);
  });

  it('keeps the best pass and never unlocks twice', () => {
    const store = new FakeStorage();
    recordPass('act1/the-duel', 3, ['proposer_p2c'], store);
    recordPass('act1/the-duel', 1, ['proposer_p2c'], store);
    recordPass('act1/the-duel', 7, ['proposer_p2c'], store);
    const progress = load(store);
    expect(progress.levels['act1/the-duel']?.mistakes).toBe(1);
    expect(progress.unlocked).toEqual(['proposer_p2c']);
  });

  it('does not let a later attempt un-pass a level', () => {
    const store = new FakeStorage();
    recordPass('act1/choose-a-value', 0, [], store);
    recordAttempt('act1/choose-a-value', 5, store);
    expect(isPassed('act1/choose-a-value', store)).toBe(true);
    expect(load(store).levels['act1/choose-a-value']?.mistakes).toBe(0);
  });

  it('records an attempt that has not reached its goal', () => {
    const store = new FakeStorage();
    recordAttempt('act1/quorum-intersection', 2, store);
    expect(load(store).levels['act1/quorum-intersection']).toEqual({
      passed: false,
      mistakes: 2,
    });
  });

  it('survives junk in the store', () => {
    const store = new FakeStorage();
    store.items.set(STORAGE_KEY, '{not json');
    expect(load(store)).toEqual({ levels: {}, unlocked: [], folded: [] });

    store.items.set(STORAGE_KEY, JSON.stringify({ levels: { a: 3 }, unlocked: 'nope' }));
    expect(load(store)).toEqual({ levels: {}, unlocked: [], folded: [] });

    store.items.set(
      STORAGE_KEY,
      JSON.stringify({ levels: { a: { passed: 'yes', mistakes: 'many' } } }),
    );
    expect(load(store).levels['a']).toEqual({ passed: false, mistakes: 0 });
  });

  it('swallows a write that throws', () => {
    const store = new FakeStorage();
    store.throwOnWrite = true;
    expect(() => save({ levels: {}, unlocked: [], folded: [] }, store)).not.toThrow();
    expect(() => recordPass('act1/choose-a-value', 0, [], store)).not.toThrow();
  });

  it('remembers which briefings the player folded away', () => {
    const store = new FakeStorage();
    expect(isBriefingFolded('act1/choose-a-value', load(store))).toBe(false);
    foldBriefing('act1/choose-a-value', true, store);
    expect(isBriefingFolded('act1/choose-a-value', load(store))).toBe(true);
    // Folding the same level two times records it one time.
    foldBriefing('act1/choose-a-value', true, store);
    expect(load(store).folded).toEqual(['act1/choose-a-value']);
    foldBriefing('act1/choose-a-value', false, store);
    expect(load(store).folded).toEqual([]);
  });

  it('clears', () => {
    const store = new FakeStorage();
    recordPass('act1/choose-a-value', 0, [], store);
    expect(clear(store)).toEqual({ levels: {}, unlocked: [], folded: [] });
    expect(load(store)).toEqual({ levels: {}, unlocked: [], folded: [] });
  });
});
