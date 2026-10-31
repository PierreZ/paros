// The proxy leaders' controls: crash one, restart one.
//
// A proxy is an operator's process like a matchmaker, and its ids are their
// own space, so it gets its own verbs. Which of them this level offers is
// `LevelView.allowed_actions`; nothing here reads a level id.

import type { Action, ActionKind, GameView, ProxyView } from '../types';
import { h } from '../render/dom';

type Dispatch = (action: Action) => void;

function allowed(view: GameView, kind: ActionKind): boolean {
  const actions = Array.isArray(view.level?.allowed_actions) ? view.level.allowed_actions : [];
  return actions.includes(kind);
}

/** Which of the two proxy controls this level offers for this proxy. */
export function proxyControlsFor(view: GameView, proxy: ProxyView): ActionKind[] {
  const controls: ActionKind[] = [];
  if (allowed(view, 'crash_proxy') && proxy.alive !== false) controls.push('crash_proxy');
  if (allowed(view, 'restart_proxy') && proxy.alive === false) controls.push('restart_proxy');
  return controls;
}

function button(label: string, title: string, onClick: () => void, className = 'control-button') {
  const element = h('button', { class: className, type: 'button', title }, label);
  element.addEventListener('click', onClick);
  return element;
}

/** The proxy rows, or nothing on a level without proxies. */
export function renderProxyControls(view: GameView, dispatch: Dispatch): HTMLElement | null {
  const proxies = Array.isArray(view.world?.proxies) ? view.world.proxies : [];
  const rows: HTMLElement[] = [];
  for (const proxy of proxies) {
    const offered = proxyControlsFor(view, proxy);
    if (offered.length === 0) continue;
    const open = Array.isArray(proxy.open_rounds) ? proxy.open_rounds.length : 0;
    rows.push(
      h(
        'div',
        { class: `control-row node-row${proxy.alive === false ? ' crashed' : ''}` },
        h('span', { class: 'control-label' }, `proxy ${proxy.id}`),
        h('span', { class: 'control-hint' }, `${open} open round${open === 1 ? '' : 's'}`),
        offered.includes('crash_proxy')
          ? button(
              'Crash',
              'Stop this proxy. It keeps nothing durable, so its open rounds are gone.',
              () => dispatch({ kind: 'crash_proxy', proxy: proxy.id }),
              'control-button danger',
            )
          : null,
        offered.includes('restart_proxy')
          ? button('Restart', 'Start this proxy again, empty.', () =>
              dispatch({ kind: 'restart_proxy', proxy: proxy.id }),
            )
          : null,
      ),
    );
  }
  if (rows.length === 0) return null;
  return h('section', { class: 'control-block' }, h('h3', {}, 'The proxy leaders'), ...rows);
}
