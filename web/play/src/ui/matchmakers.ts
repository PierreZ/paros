// The matchmaker plane's controls: the acceptor set, the floor, the tier.
//
// Four moves live here, and they are the operator's, not the client's. The
// operator changes the acceptor set, retires an acceptor the floor released,
// replaces a matchmaker, and pushes a stalled phase along again. Which of them
// this level offers is `LevelView.allowed_actions`, and nothing here reads a
// level id.
//
// Two rules the whole file follows. Every set the player composes is sent as
// data — a list of members and, where the level asks for one, a quorum system
// — and the engine decides whether it is legal. And the one ballot the player
// passes back, the garbage-collection floor a `Retire` carries as evidence, is
// read from the leader's own report and never derived here.

import type {
  Action,
  ActionKind,
  BallotSpec,
  GameView,
  MatchmakerView,
  NodeView,
  QuorumSpec,
} from '../types';
import { parseBallot } from '../ballot';
import { h } from '../render/dom';

type Dispatch = (action: Action) => void;

/** Which quorum system the member picker composes. */
export type QuorumKind = 'majority' | 'flexible' | 'grid';

/** The input the matchmaker-plane controls keep between frames. */
export interface MatchmakerControlState {
  /** The leader a reconfiguration is asked of, or `null` for the leader. */
  reconfigureNode: number | null;
  /** The acceptors the new set names, or `null` for the set in force. */
  reconfigureMembers: number[] | null;
  /** The quorum system the new set runs. */
  reconfigureQuorum: QuorumKind;
  /** The two numbers a flexible split or a grid needs, as the player typed them. */
  quorumSizes: { q1: string; q2: string; rows: string; cols: string };
  /** The leader whose report the operator reads the floor from, or `null`. */
  retireNode: number | null;
  /** The acceptor asked to retire, or `null` for the first one offered. */
  retireTarget: number | null;
  /** Whether the request carries the floor as evidence. */
  retireWithEvidence: boolean;
  /** The node that drives a matchmaker handover, or `null` for the leader. */
  handoverNode: number | null;
  /** The matchmakers the successor names, or `null` for the set in force. */
  handoverMembers: number[] | null;
}

/** A fresh matchmaker-plane state. */
export function newMatchmakerState(): MatchmakerControlState {
  return {
    reconfigureNode: null,
    reconfigureMembers: null,
    reconfigureQuorum: 'majority',
    quorumSizes: { q1: '', q2: '', rows: '', cols: '' },
    retireNode: null,
    retireTarget: null,
    retireWithEvidence: true,
    handoverNode: null,
    handoverMembers: null,
  };
}

function allowed(view: GameView, kind: ActionKind): boolean {
  return view.level.allowed_actions.includes(kind);
}

// ---- what the pickers are filled with ---------------------------------------

/** Every node the deployment can ever name as an acceptor. */
export function acceptorPool(view: GameView): number[] {
  const nodes = Array.isArray(view.world?.nodes) ? view.world.nodes : [];
  return nodes.filter((node) => node.retired !== true).map((node) => node.id);
}

/**
 * The acceptor set in force, as the picker starts.
 *
 * It is read off a node, never counted here: a configuration belongs to a
 * ballot, and the engine is the one that says which one is in force.
 */
export function acceptorsInForce(view: GameView): number[] {
  const nodes = Array.isArray(view.world?.nodes) ? view.world.nodes : [];
  const leader = nodes.find((node) => node.role === 'leader');
  const source = leader ?? nodes.find((node) => Array.isArray(node.acceptors));
  return Array.isArray(source?.acceptors) ? [...source.acceptors] : [];
}

/** The members the reconfigure picker has ticked. */
export function reconfigureMembers(view: GameView, state: MatchmakerControlState): number[] {
  return state.reconfigureMembers ?? acceptorsInForce(view);
}

/** Every matchmaker the deployment can ever name, spares included. */
export function matchmakerPool(view: GameView): number[] {
  const matchmakers = Array.isArray(view.world?.matchmakers) ? view.world.matchmakers : [];
  return matchmakers.map((matchmaker) => matchmaker.id);
}

/**
 * The matchmaker set a node believes authoritative, as the picker starts.
 *
 * A world whose nodes name no set at all falls back to the matchmakers that
 * serve the generation in force.
 */
export function matchmakersInForce(view: GameView): number[] {
  const nodes = Array.isArray(view.world?.nodes) ? view.world.nodes : [];
  for (const node of nodes) {
    const set = node.matchmakers;
    if (set && Array.isArray(set.members) && set.members.length > 0) return [...set.members];
  }
  const matchmakers = Array.isArray(view.world?.matchmakers) ? view.world.matchmakers : [];
  return matchmakers.filter((matchmaker) => matchmaker.phase === 'active').map((one) => one.id);
}

/** The members the matchmaker picker has ticked. */
export function handoverMembers(view: GameView, state: MatchmakerControlState): number[] {
  return state.handoverMembers ?? matchmakersInForce(view);
}

/**
 * The quorum system a `Reconfigure` sends, or `null` for a majority.
 *
 * A majority sends no numbers at all, which is what every deployment runs
 * unless it says otherwise. A split or a grid the player has not filled in
 * sends nothing either: an incomplete shape is not a configuration.
 */
export function quorumSpecOf(state: MatchmakerControlState): QuorumSpec | null {
  const size = (raw: string): number | null => {
    const value = Number.parseInt(raw, 10);
    return Number.isFinite(value) && value >= 1 ? value : null;
  };
  if (state.reconfigureQuorum === 'flexible') {
    const q1 = size(state.quorumSizes.q1);
    const q2 = size(state.quorumSizes.q2);
    return q1 === null || q2 === null ? null : { kind: 'flexible', q1, q2 };
  }
  if (state.reconfigureQuorum === 'grid') {
    const rows = size(state.quorumSizes.rows);
    const cols = size(state.quorumSizes.cols);
    return rows === null || cols === null ? null : { kind: 'grid', rows, cols };
  }
  return null;
}

/** Every node a `Retire` may name, in id order. */
export function retireTargets(view: GameView): number[] {
  const nodes = Array.isArray(view.world?.nodes) ? view.world.nodes : [];
  return nodes.filter((node) => node.retired !== true).map((node) => node.id);
}

/**
 * The garbage-collection floor a `Retire` carries, read off the leader's own
 * report.
 *
 * A leader with no effective floor reports none, and the request then carries
 * no evidence — which the engine refuses with `not_collected`. That refusal is
 * the point of the level, so the control must be able to send it.
 */
export function retireEvidence(view: GameView, node: number): BallotSpec | null {
  const nodes = Array.isArray(view.world?.nodes) ? view.world.nodes : [];
  const source = nodes.find((one) => one.id === node);
  return parseBallot(source?.gc?.effective_watermark);
}

/** Which of the two matchmaker controls this level offers for this matchmaker. */
export function matchmakerControlsFor(
  view: GameView,
  matchmaker: MatchmakerView,
): ActionKind[] {
  const controls: ActionKind[] = [];
  if (allowed(view, 'crash_matchmaker') && matchmaker.alive !== false) {
    controls.push('crash_matchmaker');
  }
  if (allowed(view, 'restart_matchmaker') && matchmaker.alive === false) {
    controls.push('restart_matchmaker');
  }
  return controls;
}

// ---- the widgets ------------------------------------------------------------

function button(label: string, title: string, onClick: () => void, className = 'control-button') {
  const element = h('button', { class: className, type: 'button', title }, label);
  element.addEventListener('click', onClick);
  return element;
}

function tinyNumber(key: string, value: string, label: string, onInput: (next: string) => void) {
  const input = h('input', {
    class: 'text-input tiny',
    type: 'text',
    value,
    placeholder: label,
    title: label,
    'data-focus-key': key,
  });
  input.addEventListener('input', () => onInput(input.value));
  return input;
}

function nodeSelect(
  key: string,
  nodes: readonly NodeView[],
  target: number,
  onChange: (id: number) => void,
): HTMLSelectElement {
  const select = h('select', { class: 'node-select', 'data-focus-key': key });
  for (const node of nodes) {
    select.append(
      h(
        'option',
        { value: node.id, selected: node.id === target },
        `node ${node.id}${node.role === 'leader' ? ' (leader)' : ''}${node.alive ? '' : ' (down)'}`,
      ),
    );
  }
  select.addEventListener('change', () => onChange(Number(select.value)));
  return select;
}

/** The node a request goes to first: the leader, or anything alive. */
function defaultTarget(view: GameView, chosen: number | null): number {
  if (chosen !== null) return chosen;
  const nodes = view.world.nodes;
  const leader = nodes.find((node) => node.role === 'leader');
  return leader?.id ?? nodes.find((node) => node.alive)?.id ?? 0;
}

/** A row of checkboxes over a pool, and the set they compose. */
function memberPicker(
  key: string,
  pool: readonly number[],
  ticked: readonly number[],
  prefix: string,
  onChange: (members: number[]) => void,
): HTMLElement {
  const set = new Set(ticked);
  const boxes = pool.map((id) => {
    const box = h('input', {
      type: 'checkbox',
      class: 'reach-box',
      id: `${key}-${id}`,
      checked: set.has(id),
      'data-focus-key': `${key}-${id}`,
    });
    box.addEventListener('change', () => {
      const next = new Set(set);
      if (box.checked) next.add(id);
      else next.delete(id);
      onChange([...next].sort((a, b) => a - b));
    });
    return h('label', { class: 'reach-label', for: `${key}-${id}` }, box, `${prefix}${id}`);
  });
  return h('span', { class: 'reach-boxes' }, ...boxes);
}

/**
 * The quorum system the new set runs.
 *
 * A configuration and its quorum system are one thing, so the picker asks for
 * both together. A majority needs no numbers; a split needs `q1` and `q2`, and
 * a grid needs its rows and its columns.
 */
function quorumPicker(state: MatchmakerControlState, rerender: () => void): HTMLElement {
  const select = h('select', {
    class: 'node-select',
    'data-focus-key': 'reconfigure-quorum',
    title: 'The quorum system the new acceptor set runs.',
  });
  const kinds: { kind: QuorumKind; label: string }[] = [
    { kind: 'majority', label: 'a majority' },
    { kind: 'flexible', label: 'a flexible split' },
    { kind: 'grid', label: 'a grid' },
  ];
  for (const { kind, label } of kinds) {
    select.append(h('option', { value: kind, selected: state.reconfigureQuorum === kind }, label));
  }
  select.addEventListener('change', () => {
    state.reconfigureQuorum = select.value as QuorumKind;
    rerender();
  });
  const sizes =
    state.reconfigureQuorum === 'flexible'
      ? [
          tinyNumber('reconfigure-q1', state.quorumSizes.q1, 'q1', (next) => {
            state.quorumSizes.q1 = next;
          }),
          tinyNumber('reconfigure-q2', state.quorumSizes.q2, 'q2', (next) => {
            state.quorumSizes.q2 = next;
          }),
        ]
      : state.reconfigureQuorum === 'grid'
        ? [
            tinyNumber('reconfigure-rows', state.quorumSizes.rows, 'rows', (next) => {
              state.quorumSizes.rows = next;
            }),
            tinyNumber('reconfigure-cols', state.quorumSizes.cols, 'cols', (next) => {
              state.quorumSizes.cols = next;
            }),
          ]
        : [];
  return h('span', { class: 'quorum-picker' }, select, ...sizes);
}

function reconfigureRow(
  view: GameView,
  state: MatchmakerControlState,
  dispatch: Dispatch,
  rerender: () => void,
): HTMLElement | null {
  if (!allowed(view, 'reconfigure')) return null;
  const target = defaultTarget(view, state.reconfigureNode);
  const select = nodeSelect('reconfigure-node', view.world.nodes, target, (id) => {
    state.reconfigureNode = id;
    rerender();
  });
  const members = reconfigureMembers(view, state);
  const picker = memberPicker('reconfigure-member', acceptorPool(view), members, '', (next) => {
    state.reconfigureMembers = next;
    rerender();
  });
  return h(
    'div',
    { class: 'control-row' },
    h('span', { class: 'control-label' }, 'the operator asks'),
    select,
    h('span', { class: 'control-hint' }, 'to run with'),
    picker,
    quorumPicker(state, rerender),
    button(
      'Reconfigure',
      'The leader moves to a fresh ballot, registers this set with the matchmakers, and covers the old set in Phase 1.',
      () => {
        dispatch({
          kind: 'reconfigure',
          node: target,
          members,
          quorum: quorumSpecOf(state),
        });
      },
    ),
    h('span', { class: 'control-hint' }, `${members.length} acceptor${members.length === 1 ? '' : 's'}`),
  );
}

function retireRow(
  view: GameView,
  state: MatchmakerControlState,
  dispatch: Dispatch,
  rerender: () => void,
): HTMLElement | null {
  if (!allowed(view, 'retire')) return null;
  const leader = defaultTarget(view, state.retireNode);
  const targets = retireTargets(view);
  const chosen = state.retireTarget ?? targets[targets.length - 1] ?? 0;
  const from = nodeSelect('retire-node', view.world.nodes, leader, (id) => {
    state.retireNode = id;
    rerender();
  });
  const who = h('select', { class: 'node-select', 'data-focus-key': 'retire-target' });
  for (const id of targets) {
    who.append(h('option', { value: id, selected: id === chosen }, `node ${id}`));
  }
  who.addEventListener('change', () => {
    state.retireTarget = Number(who.value);
    rerender();
  });
  const evidence = retireEvidence(view, leader);
  const box = h('input', {
    type: 'checkbox',
    class: 'reach-box',
    id: 'retire-evidence',
    checked: state.retireWithEvidence,
    'data-focus-key': 'retire-evidence',
  });
  box.addEventListener('change', () => {
    state.retireWithEvidence = box.checked;
    rerender();
  });
  return h(
    'div',
    { class: 'control-row' },
    h('span', { class: 'control-label' }, 'the operator reads the floor from'),
    from,
    h('span', { class: 'control-hint' }, 'and retires'),
    who,
    h(
      'label',
      {
        class: 'reach-label',
        for: 'retire-evidence',
        title:
          'The floor is the evidence. Send the request with no floor and the node refuses it: an installed successor set is not a collected predecessor.',
      },
      box,
      'with the floor',
    ),
    h(
      'span',
      { class: 'control-hint' },
      evidence === null
        ? 'this node reports no floor yet'
        : `floor ${evidence.round}.${evidence.node}`,
    ),
    button(
      'Retire',
      'Ask this acceptor to shut down for good. The request must carry the floor a matchmaker quorum wrote down.',
      () => {
        dispatch({
          kind: 'retire',
          node: leader,
          target: chosen,
          gc_watermark: state.retireWithEvidence ? evidence : null,
        });
      },
      'control-button danger',
    ),
  );
}

function handoverRow(
  view: GameView,
  state: MatchmakerControlState,
  dispatch: Dispatch,
  rerender: () => void,
): HTMLElement | null {
  if (!allowed(view, 'reconfigure_matchmakers')) return null;
  const pool = matchmakerPool(view);
  if (pool.length === 0) return null;
  const driver = defaultTarget(view, state.handoverNode);
  const select = nodeSelect('handover-node', view.world.nodes, driver, (id) => {
    state.handoverNode = id;
    rerender();
  });
  const members = handoverMembers(view, state);
  const picker = memberPicker('handover-member', pool, members, 'm', (next) => {
    state.handoverMembers = next;
    rerender();
  });
  return h(
    'div',
    { class: 'control-row' },
    h('span', { class: 'control-label' }, 'the operator asks'),
    select,
    h('span', { class: 'control-hint' }, 'for the next generation'),
    picker,
    button(
      'Replace the matchmakers',
      'Stop the generation in force, reconstruct it, bootstrap the successor, decide it by one decree, and publish it.',
      () => {
        dispatch({ kind: 'reconfigure_matchmakers', node: driver, members });
      },
    ),
  );
}

function matchmakerRows(view: GameView, dispatch: Dispatch): HTMLElement[] {
  const matchmakers = Array.isArray(view.world?.matchmakers) ? view.world.matchmakers : [];
  const rows: HTMLElement[] = [];
  for (const matchmaker of matchmakers) {
    const offered = matchmakerControlsFor(view, matchmaker);
    if (offered.length === 0) continue;
    rows.push(
      h(
        'div',
        { class: `control-row node-row${matchmaker.alive === false ? ' crashed' : ''}` },
        h('span', { class: 'control-label' }, `matchmaker ${matchmaker.id}`),
        h(
          'span',
          { class: 'control-hint' },
          `generation ${matchmaker.generation}, ${matchmaker.registrations.length} registration${
            matchmaker.registrations.length === 1 ? '' : 's'
          }`,
        ),
        offered.includes('crash_matchmaker')
          ? button(
              'Crash',
              'Stop this matchmaker. Its registry stays on its disk.',
              () => dispatch({ kind: 'crash_matchmaker', matchmaker: matchmaker.id }),
              'control-button danger',
            )
          : null,
        offered.includes('restart_matchmaker')
          ? button('Restart', 'Start this matchmaker again from its registry.', () =>
              dispatch({ kind: 'restart_matchmaker', matchmaker: matchmaker.id }),
            )
          : null,
      ),
    );
  }
  return rows;
}

/**
 * The whole matchmaker plane, or nothing.
 *
 * A level that offers none of these moves renders no block at all, which is
 * what every level before Act IV part two does.
 */
export function renderMatchmakerControls(
  view: GameView,
  state: MatchmakerControlState,
  dispatch: Dispatch,
  rerender: () => void,
): HTMLElement | null {
  const rows = [
    reconfigureRow(view, state, dispatch, rerender),
    retireRow(view, state, dispatch, rerender),
    handoverRow(view, state, dispatch, rerender),
    ...matchmakerRows(view, dispatch),
  ].filter((row): row is HTMLElement => row !== null);
  if (rows.length === 0) return null;
  return h('section', { class: 'control-block' }, h('h3', {}, 'The matchmaker plane'), ...rows);
}
