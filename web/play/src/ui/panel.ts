// The side panel: the briefing, the goal, the prompt, the log, the toggles.
//
// The education rule from the plan decides the order: the briefing and the
// prompt come first and are written in Paxos words, and the `paros-core`
// symbols the level names are a small footnote at the bottom, beside the link
// into the field guide.

import type { Action, AutomationFlag, AutomationFlagView, GameView, LevelSummary } from '../types';
import type { Progress } from '../progress';
import { narrationStream } from '../narration';
import { h } from '../render/dom';
import { markdown } from './markdown';
import { renderPrompt } from './prompt';

type Dispatch = (action: Action) => void;

export interface PanelDeps {
  view: GameView;
  levels: readonly LevelSummary[];
  progress: Progress;
  error: { code: string; error: string } | null;
  dispatch: Dispatch;
  undo: () => void;
  reset: () => void;
}

function goalBlock(view: GameView): HTMLElement {
  const goal = view.goal;
  const heading =
    goal.status === 'reached'
      ? 'Goal reached'
      : goal.status === 'failed'
        ? 'Goal failed'
        : 'Goal';
  return h(
    'section',
    { class: `goal goal-${goal.status}` },
    h('h2', {}, heading),
    h('p', {}, goal.detail),
    view.mistakes > 0
      ? h(
          'p',
          { class: 'mistakes' },
          `You gave ${view.mistakes} wrong answer${view.mistakes === 1 ? '' : 's'}. The world did not move for them.`,
        )
      : null,
  );
}

/**
 * What passing this level gives the player.
 *
 * The engine names the flags (`LevelView.unlocks`); their labels come from the
 * automation view, which is where a flag's name for the player lives. A level
 * that unlocks nothing renders nothing.
 */
function unlocksBlock(view: GameView): HTMLElement | null {
  const unlocks = Array.isArray(view.level.unlocks) ? view.level.unlocks : [];
  if (unlocks.length === 0) return null;
  const labels = new Map<AutomationFlag, string>();
  for (const flag of view.automation.flags) labels.set(flag.flag, flag.label);
  const names = unlocks.map((flag) => labels.get(flag) ?? String(flag).replace(/_/g, ' '));
  return h(
    'section',
    { class: 'unlocks' },
    h('h2', {}, 'The reward'),
    h('p', {}, `Passing this level unlocks: ${names.join(', ')}.`),
    h(
      'p',
      { class: 'small' },
      'The engine then takes this decision for you in the levels that come after this one.',
    ),
  );
}

function automationBlock(view: GameView, dispatch: Dispatch): HTMLElement | null {
  const flags = view.automation.flags.filter((flag) => flag.unlocked || flag.on);
  if (flags.length === 0) return null;
  return h(
    'section',
    { class: 'automation' },
    h('h2', {}, 'Automation'),
    h(
      'p',
      { class: 'small' },
      'The engine can play a role that you know. A level that teaches a decision keeps that ' +
        'decision manual.',
    ),
    ...flags.map((flag) => toggle(flag, dispatch)),
  );
}

function toggle(flag: AutomationFlagView, dispatch: Dispatch): HTMLElement {
  const disabled = flag.pinned_off || !flag.unlocked;
  const box = h('input', {
    type: 'checkbox',
    class: 'auto-box',
    id: `auto-${flag.flag}`,
    checked: flag.on,
    disabled,
  });
  box.addEventListener('change', () => {
    dispatch({ kind: 'set_automation', flag: flag.flag, on: box.checked });
  });
  return h(
    'label',
    {
      class: `auto-toggle${disabled ? ' disabled' : ''}`,
      for: `auto-${flag.flag}`,
      title: flag.pinned_off
        ? 'This level teaches this decision. You must make it.'
        : flag.unlocked
          ? 'The engine makes this decision for you.'
          : 'You must pass the level that teaches this decision first.',
    },
    box,
    h('span', {}, flag.label),
  );
}

function logBlock(deps: PanelDeps): HTMLElement {
  const { view } = deps;
  const undo = h('button', { class: 'control-button', type: 'button', disabled: view.log.length === 0 }, 'Undo');
  undo.addEventListener('click', deps.undo);
  const reset = h('button', { class: 'control-button', type: 'button' }, 'Reset');
  reset.addEventListener('click', deps.reset);

  const entries = view.log.slice(-12);
  return h(
    'section',
    { class: 'log' },
    h('h2', {}, 'What you have played'),
    h('div', { class: 'log-buttons' }, undo, reset),
    entries.length === 0
      ? h('p', { class: 'empty' }, 'You did not play a move yet.')
      : h(
          'ol',
          { class: 'log-list', start: view.log.length - entries.length + 1 },
          ...entries.map((entry) => h('li', { class: `log-entry kind-${entry.kind}` }, entry.label)),
        ),
  );
}

function narrationBlock(view: GameView): HTMLElement | null {
  const stream = narrationStream(view);
  if (stream.length === 0) return null;
  return h(
    'section',
    { class: 'narration' },
    h('h2', {}, 'What happened'),
    h(
      'ol',
      { class: 'narration-list' },
      ...stream
        .slice(-20)
        .map((entry) => h('li', { class: `narration-entry kind-${entry.kind}` }, entry.text)),
    ),
  );
}

function navBlock(deps: PanelDeps): HTMLElement {
  const { view, levels } = deps;
  const index = levels.findIndex((level) => level.id === view.level.id);
  const previous = index > 0 ? levels[index - 1] : undefined;
  const next = index >= 0 && index + 1 < levels.length ? levels[index + 1] : undefined;
  return h(
    'nav',
    { class: 'level-nav' },
    previous
      ? h('a', { class: 'nav-link', href: `#${previous.id}` }, `← ${previous.title}`)
      : h('span', { class: 'nav-link disabled' }, '← start'),
    h('a', { class: 'nav-link', href: '#' }, 'All levels'),
    next
      ? h('a', { class: 'nav-link', href: `#${next.id}` }, `${next.title} →`)
      : h('span', { class: 'nav-link disabled' }, 'end →'),
  );
}

function footnote(view: GameView): HTMLElement {
  const level = view.level;
  return h(
    'section',
    { class: 'footnote-block' },
    h(
      'details',
      {},
      h('summary', {}, 'In the code'),
      h(
        'p',
        { class: 'small' },
        'You do not need this to play. It shows where the rules that you used are in the code.',
      ),
      level.symbols.length > 0
        ? h(
            'ul',
            { class: 'symbol-list' },
            ...level.symbols.map((symbol) => h('li', {}, h('code', {}, symbol))),
          )
        : null,
      h('p', {}, h('a', { href: `../${level.field_guide}` }, 'The field guide page for this level')),
    ),
  );
}

/** The whole panel. */
export function renderPanel(deps: PanelDeps): HTMLElement {
  const { view } = deps;
  const record = deps.progress.levels[view.level.id];
  return h(
    'aside',
    { class: 'panel' },
    h(
      'header',
      { class: 'level-header' },
      h('p', { class: 'act-label' }, `Act ${view.level.act}${record?.passed ? ' · passed' : ''}`),
      h('h1', {}, view.level.title),
    ),
    navBlock(deps),
    deps.error
      ? h('div', { class: 'error-banner', role: 'status' }, deps.error.error)
      : null,
    renderPrompt(view, deps.dispatch),
    goalBlock(view),
    h(
      'section',
      { class: 'briefing' },
      h('h2', {}, 'The briefing'),
      markdown(view.level.briefing),
    ),
    unlocksBlock(view),
    view.level.hint ? h('section', { class: 'hint' }, h('h2', {}, 'A hint'), h('p', {}, view.level.hint)) : null,
    automationBlock(view, deps.dispatch),
    logBlock(deps),
    narrationBlock(view),
    footnote(view),
  );
}
