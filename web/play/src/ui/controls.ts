// The controls: everything the player can do that is not a message on the wire.
//
// Which controls exist is derived from `LevelView.allowed_actions` and the
// world's flavour — never from the level id. A level that stops offering
// `crash` stops rendering a Crash button, and a level the engine adds
// tomorrow gets its controls for free.

import type { Action, ActionKind, GameView, NodeView, Phase, Seam } from '../types';
import { h } from '../render/dom';

type Dispatch = (action: Action) => void;

/** The bits of input the panel keeps between frames. */
export interface ControlState {
  /** The value each proposer's next ballot carries. */
  ballotValues: Map<number, string>;
  /** The command the client's next proposal carries. */
  proposeValue: string;
  /** The node the client asks, or `null` for "whoever leads". */
  proposeNode: number | null;
  /** The acceptors each phase reaches, in the quorum-intersection level. */
  reach: Map<Phase, Set<number>>;
  /** The election timeout box, per node. */
  timeouts: Map<number, string>;
}

/** A fresh control state. */
export function newControlState(): ControlState {
  return {
    ballotValues: new Map(),
    proposeValue: 'x=1',
    proposeNode: null,
    reach: new Map(),
    timeouts: new Map(),
  };
}

function textInput(
  key: string,
  value: string,
  placeholder: string,
  onInput: (value: string) => void,
): HTMLInputElement {
  const input = h('input', {
    class: 'text-input',
    type: 'text',
    value,
    placeholder,
    'data-focus-key': key,
  });
  input.addEventListener('input', () => onInput(input.value));
  return input;
}

function action(
  label: string,
  title: string,
  onClick: () => void,
  className = 'control-button',
): HTMLButtonElement {
  const button = h('button', { class: className, type: 'button', title }, label);
  button.addEventListener('click', onClick);
  return button;
}

function allowed(view: GameView, kind: ActionKind): boolean {
  return view.level.allowed_actions.includes(kind);
}

// ---- the single-decree world ------------------------------------------------

function ballotForms(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement | null {
  if (!allowed(view, 'open_ballot')) return null;
  const proposers = view.world.nodes.filter((node) => node.flavour === 'proposer');
  if (proposers.length === 0) return null;
  return h(
    'section',
    { class: 'control-block' },
    h('h3', {}, 'Open a ballot'),
    ...proposers.map((proposer) => {
      const value = state.ballotValues.get(proposer.id) ?? 'x=1';
      const input = textInput(`ballot-${proposer.id}`, value, 'the value to propose', (next) => {
        state.ballotValues.set(proposer.id, next);
      });
      const submit = (): void => {
        dispatch({ kind: 'open_ballot', proposer: proposer.id, value: input.value.trim() || 'x=1' });
      };
      input.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') submit();
      });
      return h(
        'div',
        { class: 'control-row' },
        h('span', { class: 'control-label' }, `proposer ${proposer.id}`),
        input,
        action('Open', `Run Phase 1 at a fresh ballot from proposer ${proposer.id}`, submit),
      );
    }),
  );
}

function reachPicker(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement | null {
  if (!allowed(view, 'set_reach')) return null;
  const acceptors = view.world.nodes.filter((node) => node.flavour === 'acceptor');
  const phases: { phase: Phase; label: string; hint: string }[] = [
    { phase: 'one', label: 'Phase 1 reaches', hint: 'which acceptors a Prepare is sent to' },
    { phase: 'two', label: 'Phase 2 reaches', hint: 'which acceptors an Accept is sent to' },
  ];
  return h(
    'section',
    { class: 'control-block' },
    h('h3', {}, 'Reach'),
    ...phases.map(({ phase, label, hint }) => {
      let chosen = state.reach.get(phase);
      if (!chosen) {
        chosen = new Set(acceptors.map((node) => node.id));
        state.reach.set(phase, chosen);
      }
      const set = chosen;
      return h(
        'div',
        { class: 'control-row reach-row' },
        h('span', { class: 'control-label', title: hint }, label),
        ...acceptors.map((acceptor) => {
          const box = h('input', {
            type: 'checkbox',
            class: 'reach-box',
            id: `reach-${phase}-${acceptor.id}`,
            checked: set.has(acceptor.id),
          });
          box.addEventListener('change', () => {
            if (box.checked) set.add(acceptor.id);
            else set.delete(acceptor.id);
            dispatch({ kind: 'set_reach', phase, nodes: [...set].sort((a, b) => a - b) });
          });
          return h(
            'label',
            { class: 'reach-label', for: `reach-${phase}-${acceptor.id}` },
            box,
            String(acceptor.id),
          );
        }),
      );
    }),
  );
}

// ---- the replicated-log world -----------------------------------------------

function proposeForm(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement | null {
  if (!allowed(view, 'propose')) return null;
  const clients = view.world.clients;
  const client = clients[0]?.id ?? 0;
  const leader = view.world.nodes.find((node) => node.role === 'leader');
  const target =
    state.proposeNode ?? leader?.id ?? view.world.nodes.find((node) => node.alive)?.id ?? 0;

  const select = h('select', { class: 'node-select', 'data-focus-key': 'propose-node' });
  for (const node of view.world.nodes) {
    const option = h(
      'option',
      { value: node.id, selected: node.id === target },
      `node ${node.id}${node.role === 'leader' ? ' (leader)' : ''}`,
    );
    select.append(option);
  }
  select.addEventListener('change', () => {
    state.proposeNode = Number(select.value);
  });

  const input = textInput('propose-value', state.proposeValue, 'the command', (next) => {
    state.proposeValue = next;
  });
  const submit = (): void => {
    dispatch({
      kind: 'propose',
      node: Number(select.value),
      client,
      value: input.value.trim() || 'x=1',
    });
  };
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') submit();
  });

  return h(
    'section',
    { class: 'control-block' },
    h('h3', {}, 'The client'),
    h(
      'div',
      { class: 'control-row' },
      h('span', { class: 'control-label' }, `client ${client} asks`),
      select,
      input,
      action('Propose', 'Ask this node to get the command chosen', submit),
    ),
  );
}

function seamButtons(node: NodeView, dispatch: Dispatch): HTMLElement[] {
  const seams: { seam: Seam; label: string; title: string }[] = [
    {
      seam: 'before_sync',
      label: 'Crash before sync',
      title: 'Cut the next batch before its writes are durable: nothing persists, nothing is sent',
    },
    {
      seam: 'after_sync_before_send',
      label: 'Crash after sync',
      title: 'Cut the next batch after its writes are durable but before its messages go out',
    },
  ];
  return seams.map(({ seam, label, title }) =>
    action(label, title, () => dispatch({ kind: 'crash_at', node: node.id, seam }), 'control-button danger'),
  );
}

function nodeControls(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement | null {
  const kinds: ActionKind[] = [
    'tick',
    'start_election',
    'crash',
    'crash_at',
    'restart',
    'resend_pending',
    'step_down',
    'read_index',
    'set_election_timeout',
  ];
  if (!kinds.some((kind) => allowed(view, kind)) && !allowed(view, 'tick_all')) return null;

  const nodes = view.world.nodes.filter((node) => node.flavour !== 'proposer');
  return h(
    'section',
    { class: 'control-block' },
    h('h3', {}, 'The nodes'),
    allowed(view, 'tick_all')
      ? h(
          'div',
          { class: 'control-row' },
          action('Tick every node', 'Advance every node’s clock by one tick', () =>
            dispatch({ kind: 'tick_all' }),
          ),
        )
      : null,
    ...nodes.map((node) => {
      const buttons: (HTMLElement | null)[] = [
        allowed(view, 'tick') && node.alive
          ? action('Tick', 'Advance this node’s clock by one tick', () =>
              dispatch({ kind: 'tick', node: node.id }),
            )
          : null,
        allowed(view, 'start_election') && node.alive
          ? action('Elect', 'Campaign at a fresh, higher ballot', () =>
              dispatch({ kind: 'start_election', node: node.id }),
            )
          : null,
        allowed(view, 'read_index') && node.alive
          ? action('Read', 'A client read: prove leadership now, then serve', () =>
              dispatch({ kind: 'read_index', node: node.id }),
            )
          : null,
        allowed(view, 'resend_pending') && node.alive
          ? action('Resend', 'Re-send every Accept still waiting for its quorum', () =>
              dispatch({ kind: 'resend_pending', node: node.id }),
            )
          : null,
        allowed(view, 'step_down') && node.alive
          ? action('Step down', 'Resign leadership', () =>
              dispatch({ kind: 'step_down', node: node.id }),
            )
          : null,
        allowed(view, 'crash') && node.alive
          ? action(
              'Crash',
              'Lose the running node; its disk survives',
              () => dispatch({ kind: 'crash', node: node.id }),
              'control-button danger',
            )
          : null,
        allowed(view, 'restart') && !node.alive
          ? action('Restart', 'Boot it again from its disk', () =>
              dispatch({ kind: 'restart', node: node.id }),
            )
          : null,
        ...(allowed(view, 'crash_at') && node.alive ? seamButtons(node, dispatch) : []),
      ];
      const timeoutBox = allowed(view, 'set_election_timeout')
        ? (() => {
            const value = state.timeouts.get(node.id) ?? String(node.election?.timeout ?? 0);
            const input = textInput(`timeout-${node.id}`, value, 'ticks', (next) => {
              state.timeouts.set(node.id, next);
            });
            input.classList.add('tiny');
            return h(
              'span',
              { class: 'timeout-box' },
              input,
              action('Set timeout', 'How many ticks before this node campaigns', () => {
                const ticks = Number.parseInt(input.value, 10);
                if (Number.isFinite(ticks) && ticks >= 0) {
                  dispatch({ kind: 'set_election_timeout', node: node.id, ticks });
                }
              }),
            );
          })()
        : null;
      return h(
        'div',
        { class: `control-row node-row${node.alive ? '' : ' crashed'}` },
        h('span', { class: 'control-label' }, `node ${node.id}`),
        ...buttons.filter((element): element is HTMLElement => element !== null),
        timeoutBox,
      );
    }),
  );
}

/** Every control this level offers. */
export function renderControls(
  view: GameView,
  state: ControlState,
  dispatch: Dispatch,
): HTMLElement {
  const blocks = [
    ballotForms(view, state, dispatch),
    reachPicker(view, state, dispatch),
    proposeForm(view, state, dispatch),
    nodeControls(view, state, dispatch),
  ].filter((block): block is HTMLElement => block !== null);
  return h('section', { class: 'controls' }, ...blocks);
}
