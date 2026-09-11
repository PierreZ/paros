// Boot the engine, route the URL, render, and turn clicks into actions.
//
// The render loop is deliberately dumb: every change re-renders the whole
// screen from the view the engine just returned. There is no diffing and no
// component state — the two things a frame needs to keep (what the player has
// typed into a box, and where the focus was) are carried explicitly.

import './styles.css';
import init, { WasmGame } from './wasm/paros_play.js';
import { Game, decodeLevels } from './game';
import { parseHash, type Route } from './route';
import { h, replace } from './render/dom';
import { wipedNodes } from './render/disk';
import { gridCells, gridOf } from './render/grid';
import { NARROW_BREAKPOINT } from './render/narrow';
import { renderStage, type Viewport } from './render/stage';
import { renderCaption } from './ui/caption';
import { renderInspector } from './ui/inspector';
import {
  misrouteTargets,
  newControlState,
  renderControls,
  type ControlState,
} from './ui/controls';
import { renderHistory } from './ui/history';
import { renderLevelMap } from './ui/levelmap';
import { renderPanel } from './ui/panel';
import { renderRefusal } from './ui/refusal';
import { renderWire } from './ui/wire';
import { foldBriefing, load, recordAttempt, recordPass, type Progress } from './progress';
import type { Action, ActionKind, GameView, LevelSummary } from './types';

const app = document.querySelector<HTMLDivElement>('#app');
if (!app) throw new Error('the page has no #app to render into');

let levels: LevelSummary[] = [];
let progress: Progress = load();
let game: Game | null = null;
let controls: ControlState = newControlState();
let route: Route = parseHash(window.location.hash);

// ---- the viewport -----------------------------------------------------------
//
// The stage is drawn for the width it is given, not for the screen. A phone
// gets the narrow geometry — one SVG unit per pixel, a small disc and full-size
// text — and everything wider keeps the 960-unit picture it always had. The
// width comes from the container through a `ResizeObserver`, so a rotated
// phone, a split screen and a resized window all redraw.

let viewport: Viewport = { narrow: false, width: 960 };

/** The node whose log the player opened, on a narrow stage. */
let inspecting: number | null = null;

function measure(): Viewport {
  const width = app?.clientWidth || window.innerWidth || 960;
  return { narrow: width < NARROW_BREAKPOINT, width };
}

function reducedMotion(): boolean {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

// ---- progress ---------------------------------------------------------------

function recordProgress(view: GameView): void {
  const summary = levels.find((level) => level.id === view.level.id);
  if (view.goal.status === 'reached') {
    progress = recordPass(view.level.id, view.mistakes, summary?.unlocks ?? []);
  } else if (view.log.length > 0) {
    progress = recordAttempt(view.level.id, view.mistakes);
  }
}

// ---- the frame --------------------------------------------------------------

interface FocusMemory {
  key: string;
  start: number | null;
  end: number | null;
}

function rememberFocus(): FocusMemory | null {
  const active = document.activeElement;
  if (!(active instanceof HTMLElement)) return null;
  const key = active.dataset['focusKey'];
  if (!key) return null;
  const input = active instanceof HTMLInputElement ? active : null;
  return { key, start: input?.selectionStart ?? null, end: input?.selectionEnd ?? null };
}

function restoreFocus(memory: FocusMemory | null): void {
  if (!memory) return;
  const target = document.querySelector<HTMLElement>(`[data-focus-key="${memory.key}"]`);
  if (!target) return;
  target.focus();
  if (target instanceof HTMLInputElement && memory.start !== null && memory.end !== null) {
    try {
      target.setSelectionRange(memory.start, memory.end);
    } catch {
      // Not every input type carries a selection.
    }
  }
}

function dispatch(action: Action): void {
  if (!game) return;
  game.act(action);
  render();
}

/**
 * The card that holds one node's whole log.
 *
 * It is drawn under the stage, and only on a narrow one: a wide stage draws
 * every log column beside its node already.
 */
function inspector(view: GameView): HTMLElement | null {
  if (!viewport.narrow || inspecting === null) return null;
  const node = view.world.nodes.find((entry) => entry.id === inspecting);
  if (!node) {
    inspecting = null;
    return null;
  }
  const shape = gridOf(view.world);
  const cells = shape ? gridCells(view.world, shape) : null;
  return renderInspector({
    node,
    cell: cells?.get(node.id) ?? null,
    wiped: wipedNodes(view).has(node.id),
    matchmade: (view.world.matchmakers ?? []).length > 0,
    close: () => {
      inspecting = null;
      render();
    },
  });
}

function render(): void {
  if (!app) return;
  const memory = rememberFocus();
  closeMenu();

  if (route.kind === 'map' || !game) {
    replace(app, renderLevelMap(levels, progress));
    app.classList.remove('playing');
    restoreFocus(memory);
    return;
  }

  const view = game.view;
  recordProgress(view);
  app.classList.add('playing');
  viewport = measure();
  const stage = h(
    'main',
    { class: 'board' },
    h('div', { class: 'stage-host' }, renderStage(view, viewport)),
    inspector(view),
    renderCaption(view),
    // A refused move leaves the board alone, so the reason belongs beside the
    // controls that made it — and above them, because the controls of a
    // six-node level are longer than the screen.
    renderRefusal(game.lastError),
    renderControls(view, controls, dispatch, render),
    renderHistory(view),
    renderWire(view, dispatch),
  );
  replace(
    app,
    stage,
    renderPanel({
      view,
      levels,
      progress,
      dispatch,
      undo: () => {
        game?.undo();
        render();
      },
      reset: () => {
        game?.reset();
        controls = newControlState();
        render();
      },
      narrow: viewport.narrow,
      fold: (folded: boolean) => {
        progress = foldBriefing(view.level.id, folded);
      },
    }),
  );
  restoreFocus(memory);
}

// ---- the stage's own interactions ------------------------------------------

let menu: HTMLElement | null = null;

function closeMenu(): void {
  menu?.remove();
  menu = null;
}

/**
 * The message a pointer is over.
 *
 * `selector` matters: the wire list's rows carry `data-msg` too, so a click on
 * a row's own Deliver button would otherwise be *two* deliveries — the
 * button's, and this delegation's. Clicks look only at the stage's dots
 * (`g[data-msg]`); the context menu is happy to open over either.
 */
function messageIdFrom(target: EventTarget | null, selector = '[data-msg]'): number | null {
  if (!(target instanceof Element)) return null;
  const host = target.closest(selector);
  if (!(host instanceof Element)) return null;
  const raw = host.getAttribute('data-msg');
  if (raw === null) return null;
  const id = Number(raw);
  return Number.isFinite(id) ? id : null;
}

/**
 * Where a message's addressee is drawn.
 *
 * The tier is the engine's answer: a matchmaker and a node with the same
 * number are two processes, and they are two different squares on the stage.
 */
function partyCentre(id: number, party: string | null | undefined): { x: number; y: number } | null {
  const attribute = party === 'matchmaker' ? 'data-matchmaker' : 'data-node';
  const group = document.querySelector(`[${attribute}="${id}"]`);
  const transform = group?.getAttribute('transform');
  const match = transform?.match(/translate\(([-\d.]+),\s*([-\d.]+)\)/);
  if (!match?.[1] || !match[2]) return null;
  return { x: Number(match[1]), y: Number(match[2]) };
}

/** Deliver, with the one bit of motion the game has. */
function deliver(id: number, dot: Element | null): void {
  if (!game || reducedMotion() || !(dot instanceof SVGGElement)) {
    dispatch({ kind: 'deliver', id });
    return;
  }
  const message = game.view.world.wire.find((entry) => entry.id === id);
  const target = message ? partyCentre(message.to, message.to_party) : null;
  if (!target) {
    dispatch({ kind: 'deliver', id });
    return;
  }
  dot.classList.add('flying');
  dot.setAttribute('transform', `translate(${target.x.toFixed(1)}, ${target.y.toFixed(1)})`);
  window.setTimeout(() => dispatch({ kind: 'deliver', id }), 170);
}

function openMenu(id: number, x: number, y: number): void {
  closeMenu();
  if (!game) return;
  const view = game.view;
  const message = view.world.wire.find((entry) => entry.id === id);
  if (!message) return;
  const offers = (kind: ActionKind): boolean => view.level.allowed_actions.includes(kind);
  const item = (label: string, action: Action, title?: string): HTMLButtonElement => {
    const button = h(
      'button',
      { class: 'menu-item', type: 'button', title: title ?? null },
      label,
    );
    button.addEventListener('click', () => {
      closeMenu();
      dispatch(action);
    });
    return button;
  };
  const items: (HTMLElement | null)[] = [
    offers('deliver') ? item('Deliver', { kind: 'deliver', id }) : null,
    offers('drop') ? item('Drop', { kind: 'drop', id }) : null,
    // `to: null` keeps the copy's addressee; a named node misroutes the copy,
    // which is a thing networks do and which every rule in the protocol is
    // written to survive.
    offers('duplicate') ? item('Duplicate', { kind: 'duplicate', id, to: null }) : null,
    ...misrouteTargets(view, message).map((to) =>
      item(
        `Duplicate to node ${to}`,
        { kind: 'duplicate', id, to },
        'Send a copy to a node that this message was not addressed to.',
      ),
    ),
  ];
  const kept = items.filter((entry): entry is HTMLElement => entry !== null);
  if (kept.length === 0) return;
  const element = h('div', { class: 'wire-menu', style: `left:${x}px; top:${y}px` }, ...kept);
  document.body.append(element);
  menu = element;
}

/**
 * The node a tap landed on, on a narrow stage.
 *
 * A wide stage draws every log beside its node, so a tap there opens nothing.
 */
function nodeIdFrom(target: EventTarget | null): number | null {
  if (!viewport.narrow || !(target instanceof Element)) return null;
  const host = target.closest('g[data-node]');
  const raw = host?.getAttribute('data-node');
  if (raw === undefined || raw === null) return null;
  const id = Number(raw);
  return Number.isFinite(id) ? id : null;
}

/** Open one node's log, or close it if it is the one already open. */
function inspect(id: number): void {
  inspecting = inspecting === id ? null : id;
  render();
}

document.addEventListener('click', (event) => {
  if (menu && !(event.target instanceof Node && menu.contains(event.target))) closeMenu();
  const node = nodeIdFrom(event.target);
  if (node !== null) {
    inspect(node);
    return;
  }
  const id = messageIdFrom(event.target, 'g[data-msg]');
  if (id === null) return;
  const dot = event.target instanceof Element ? event.target.closest('g[data-msg]') : null;
  deliver(id, dot);
});

document.addEventListener('contextmenu', (event) => {
  const id = messageIdFrom(event.target);
  if (id === null) return;
  event.preventDefault();
  openMenu(id, event.clientX, event.clientY);
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    closeMenu();
    if (inspecting !== null) {
      inspecting = null;
      render();
    }
  }
  if (event.key !== 'Enter' && event.key !== ' ') return;
  const node = nodeIdFrom(document.activeElement);
  if (node !== null) {
    event.preventDefault();
    inspect(node);
    return;
  }
  const id = messageIdFrom(document.activeElement, 'g[data-msg]');
  if (id === null) return;
  event.preventDefault();
  dispatch({ kind: 'deliver', id });
});

// The stage is drawn for the container, so the container is what is watched.
// A width that crosses the narrow breakpoint, or that moves at all inside it,
// redraws: the viewBox is the width, and a stale viewBox is a stale scale.
const sizes = new ResizeObserver(() => {
  const next = measure();
  if (next.narrow === viewport.narrow && Math.abs(next.width - viewport.width) < 4) return;
  viewport = next;
  render();
});
sizes.observe(app);

// ---- routing ----------------------------------------------------------------

function openRoute(next: Route): void {
  route = next;
  controls = newControlState();
  inspecting = null;
  progress = load();
  if (next.kind === 'map') {
    game = null;
    render();
    return;
  }
  try {
    game = new Game(new WasmGame(next.id));
  } catch (cause) {
    game = null;
    if (app) {
      replace(
        app,
        h(
          'main',
          { class: 'level-map' },
          h('h1', {}, 'No such level'),
          h('p', {}, `The engine does not know ${next.id}: ${String(cause)}`),
          h('p', {}, h('a', { href: '#' }, 'Back to the level map')),
        ),
      );
    }
    return;
  }
  render();
}

window.addEventListener('hashchange', () => {
  openRoute(parseHash(window.location.hash));
});

// ---- boot -------------------------------------------------------------------

async function boot(): Promise<void> {
  await init({ module_or_path: new URL('./wasm/paros_play_bg.wasm', import.meta.url) });
  levels = decodeLevels(WasmGame.levels());
  openRoute(parseHash(window.location.hash));
}

boot().catch((cause: unknown) => {
  if (app) {
    replace(
      app,
      h(
        'main',
        { class: 'level-map' },
        h('h1', {}, 'The engine did not start'),
        h('p', {}, String(cause)),
      ),
    );
  }
});
