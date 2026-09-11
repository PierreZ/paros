// The level map: every act, every level, and what the player has done with it.

import type { LevelSummary } from '../types';
import type { Progress } from '../progress';
import { h } from '../render/dom';

const ACT_TITLES: Record<number, string> = {
  1: 'Act I — a single decree',
  2: 'Act II — a replicated log',
  3: 'Act III — truncation, snapshots and reads',
  4: 'Act IV — everything the book never wrote',
};

/** The whole map page. */
export function renderLevelMap(levels: readonly LevelSummary[], progress: Progress): HTMLElement {
  const acts = new Map<number, LevelSummary[]>();
  for (const level of levels) {
    const bucket = acts.get(level.act);
    if (bucket) bucket.push(level);
    else acts.set(level.act, [level]);
  }

  const sections = [...acts.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([act, entries]) =>
      h(
        'section',
        { class: 'act' },
        h('h2', {}, ACT_TITLES[act] ?? `Act ${act}`),
        h(
          'ol',
          { class: 'level-list' },
          ...entries.map((level) => {
            const record = progress.levels[level.id];
            const status = record?.passed
              ? `passed${record.mistakes > 0 ? ` — ${record.mistakes} mistake${record.mistakes === 1 ? '' : 's'}` : ' — no mistake'}`
              : record
                ? 'attempted'
                : 'not played';
            return h(
              'li',
              { class: `level-entry${record?.passed ? ' passed' : ''}` },
              h(
                'a',
                { class: 'level-link', href: `#${level.id}` },
                h('span', { class: 'level-title' }, level.title),
                h('span', { class: 'level-id' }, level.id),
                h('span', { class: 'level-status' }, status),
              ),
            );
          }),
        ),
      ),
    );

  return h(
    'main',
    { class: 'level-map' },
    h('h1', {}, 'paros play'),
    h(
      'p',
      { class: 'lede' },
      'You operate Paxos by hand. You are the network and the clock. A message moves only when ' +
        'you deliver it. When a node must make a decision, you make it, and the real state ' +
        'machine marks your answer.',
    ),
    ...sections,
    h(
      'p',
      { class: 'footnote' },
      'This browser keeps your progress. ',
      h('a', { href: '../index.html' }, 'The field guide'),
      ' gives the reference pages for the mechanisms in the levels.',
    ),
  );
}
