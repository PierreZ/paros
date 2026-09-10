// The controls: everything the player can do that is not a message on the wire.
//
// Which controls exist is derived from `LevelView.allowed_actions` and the
// world's flavour — never from the level id. A level that stops offering
// `crash` stops rendering a Crash button, and a level the engine adds
// tomorrow gets its controls for free.

import type { Action, ActionKind, ClientView, GameView, NodeView, Phase, Seam } from '../types';
import { h } from '../render/dom';

type Dispatch = (action: Action) => void;

/** The bits of input the panel keeps between frames. */
export interface ControlState {
  /** The value each proposer's next ballot carries. */
  ballotValues: Map<number, string>;
  /** The command the client's next write carries. */
  proposeValue: string;
  /** The client that writes, or `null` for the first client. */
  proposeClient: number | null;
  /** The node the client asks, or `null` for the leader. */
  proposeNode: number | null;
  /** The client that reads, or `null` for the first client. */
  readClient: number | null;
  /** The node the read goes to, or `null` for the leader. */
  readNode: number | null;
  /** The node the compaction request goes to, or `null` for the leader. */
  compactNode: number | null;
  /** The last slot the client permits dropping, or `null` for the default. */
  compactUpTo: string | null;
  /** The node a retry goes to, or `null` for the leader. */
  retryNode: number | null;
  /** The election timeout box, per node. */
  timeouts: Map<number, string>;
}

/** A fresh control state. */
export function newControlState(): ControlState {
  return {
    ballotValues: new Map(),
    proposeValue: 'x=1',
    proposeClient: null,
    proposeNode: null,
    readClient: null,
    readNode: null,
    compactNode: null,
    compactUpTo: null,
    retryNode: null,
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

/** The node a client request goes to first: the leader, or anything alive. */
function defaultTarget(view: GameView, chosen: number | null): number {
  if (chosen !== null) return chosen;
  const leader = view.world.nodes.find((node) => node.role === 'leader');
  return leader?.id ?? view.world.nodes.find((node) => node.alive)?.id ?? 0;
}

/** A select of every node, with the leader marked. */
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

/** A select of every client. One client needs no select. */
function clientSelect(
  key: string,
  clients: readonly ClientView[],
  target: number,
  onChange: (id: number) => void,
): HTMLSelectElement | null {
  if (clients.length < 2) return null;
  const select = h('select', { class: 'client-select', 'data-focus-key': key });
  for (const client of clients) {
    select.append(
      h('option', { value: client.id, selected: client.id === target }, `client ${client.id}`),
    );
  }
  select.addEventListener('change', () => onChange(Number(select.value)));
  return select;
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
        action('Open', `Run Phase 1 at a new ballot from proposer ${proposer.id}`, submit),
      );
    }),
  );
}

/**
 * The reach picker.
 *
 * The boxes are drawn from `WorldView.reach` — the sets that are in force in
 * the engine — so an undo or a reset moves them back with the world. The
 * frontend keeps no copy.
 */
function reachPicker(view: GameView, dispatch: Dispatch): HTMLElement | null {
  if (!allowed(view, 'set_reach')) return null;
  const acceptors = view.world.nodes.filter((node) => node.flavour === 'acceptor');
  const all = acceptors.map((node) => node.id);
  const reach = view.world.reach;
  const phases: { phase: Phase; label: string; hint: string; nodes: number[] }[] = [
    {
      phase: 'one',
      label: 'Phase 1 reaches',
      hint: 'The acceptors that receive a Prepare.',
      nodes: reach?.one ?? all,
    },
    {
      phase: 'two',
      label: 'Phase 2 reaches',
      hint: 'The acceptors that receive an Accept.',
      nodes: reach?.two ?? all,
    },
  ];
  return h(
    'section',
    { class: 'control-block' },
    h('h3', {}, 'Reach'),
    ...phases.map(({ phase, label, hint, nodes }) => {
      const set = new Set(nodes);
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
            const next = new Set(set);
            if (box.checked) next.add(acceptor.id);
            else next.delete(acceptor.id);
            dispatch({ kind: 'set_reach', phase, nodes: [...next].sort((a, b) => a - b) });
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

function proposeRow(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement | null {
  if (!allowed(view, 'propose')) return null;
  const clients = view.world.clients;
  const client = state.proposeClient ?? clients[0]?.id ?? 0;
  const target = defaultTarget(view, state.proposeNode);
  const select = nodeSelect('propose-node', view.world.nodes, target, (id) => {
    state.proposeNode = id;
  });
  const who = clientSelect('propose-client', clients, client, (id) => {
    state.proposeClient = id;
  });
  const input = textInput('propose-value', state.proposeValue, 'the command', (next) => {
    state.proposeValue = next;
  });
  const submit = (): void => {
    dispatch({
      kind: 'propose',
      node: Number(select.value),
      client: who ? Number(who.value) : client,
      value: input.value.trim() || 'x=1',
      // The engine derives the column of a grid deployment; a level that
      // makes the player pick one has its own control.
      column: null,
    });
  };
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') submit();
  });
  return h(
    'div',
    { class: 'control-row' },
    h('span', { class: 'control-label' }, who ? 'a write from' : `client ${client} writes`),
    who,
    who ? h('span', { class: 'control-hint' }, 'to') : null,
    select,
    input,
    action('Write', 'The client asks this node to get the command chosen.', submit),
  );
}

function readRow(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement | null {
  if (!allowed(view, 'read_index')) return null;
  const clients = view.world.clients;
  const client = state.readClient ?? clients[0]?.id ?? 0;
  const target = defaultTarget(view, state.readNode);
  const select = nodeSelect('read-node', view.world.nodes, target, (id) => {
    state.readNode = id;
  });
  const who = clientSelect('read-client', clients, client, (id) => {
    state.readClient = id;
  });
  return h(
    'div',
    { class: 'control-row' },
    h('span', { class: 'control-label' }, who ? 'a read from' : `client ${client} reads`),
    who,
    who ? h('span', { class: 'control-hint' }, 'to') : null,
    select,
    action(
      'Read',
      'The client asks this node for a read. The node must prove that it is the leader now.',
      () => {
        dispatch({
          kind: 'read_index',
          node: Number(select.value),
          client: who ? Number(who.value) : (clients[0]?.id ?? null),
        });
      },
    ),
  );
}

function compactRow(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement | null {
  if (!allowed(view, 'compact')) return null;
  const target = defaultTarget(view, state.compactNode);
  const select = nodeSelect('compact-node', view.world.nodes, target, (id) => {
    state.compactNode = id;
  });
  const highest = view.world.nodes.find((node) => node.id === target)?.chosen_index ?? 0;
  const input = textInput(
    'compact-up-to',
    state.compactUpTo ?? String(highest),
    'the last slot',
    (next) => {
      state.compactUpTo = next;
    },
  );
  input.classList.add('tiny');
  const submit = (): void => {
    const upTo = Number.parseInt(input.value, 10);
    if (!Number.isFinite(upTo) || upTo < 0) return;
    dispatch({ kind: 'compact', node: Number(select.value), up_to: upTo });
  };
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') submit();
  });
  return h(
    'div',
    { class: 'control-row' },
    h('span', { class: 'control-label' }, 'the client compacts'),
    select,
    h('span', { class: 'control-hint' }, 'up to slot'),
    input,
    action(
      'Compact',
      'The client asks the leader to drop the log prefix. The leader must hold a snapshot point first.',
      submit,
    ),
  );
}

function retryRows(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement[] {
  if (!allowed(view, 'retry')) return [];
  const target = defaultTarget(view, state.retryNode);
  const rows: HTMLElement[] = [];
  for (const client of view.world.clients) {
    for (const proposal of client.proposals) {
      const where =
        proposal.acked === true
          ? `acknowledged at slot ${proposal.slot ?? '?'}`
          : proposal.slot !== null
            ? `at slot ${proposal.slot}`
            : 'not at a slot yet';
      const select = nodeSelect(`retry-node-${client.id}-${proposal.seq}`, view.world.nodes, target, (id) => {
        state.retryNode = id;
      });
      rows.push(
        h(
          'div',
          { class: 'control-row retry-row' },
          h(
            'span',
            { class: 'control-label' },
            `client ${client.id} · write ${proposal.seq} ${proposal.value}`,
          ),
          h('span', { class: 'control-hint' }, where),
          select,
          action(
            'Retry',
            'The client asks again for the same write. The node must answer from its two dedup tables.',
            () => {
              dispatch({
                kind: 'retry',
                node: Number(select.value),
                client: client.id,
                seq: proposal.seq,
              });
            },
          ),
        ),
      );
    }
  }
  return rows;
}

function clientControls(view: GameView, state: ControlState, dispatch: Dispatch): HTMLElement | null {
  const rows = [
    proposeRow(view, state, dispatch),
    readRow(view, state, dispatch),
    compactRow(view, state, dispatch),
    ...retryRows(view, state, dispatch),
  ].filter((row): row is HTMLElement => row !== null);
  if (rows.length === 0) return null;
  return h('section', { class: 'control-block' }, h('h3', {}, 'The clients'), ...rows);
}

function seamButtons(node: NodeView, dispatch: Dispatch): HTMLElement[] {
  const seams: { seam: Seam; label: string; title: string }[] = [
    {
      seam: 'before_sync',
      label: 'Crash before sync',
      title:
        'Cut the next batch before its writes are durable. Nothing persists, and nothing goes out.',
    },
    {
      seam: 'after_sync_before_send',
      label: 'Crash after sync',
      title: 'Cut the next batch after its writes are durable, but before its messages go out.',
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
          action('Tick every node', 'Move the clock of every node forward one tick.', () =>
            dispatch({ kind: 'tick_all' }),
          ),
        )
      : null,
    ...nodes.map((node) => {
      const buttons: (HTMLElement | null)[] = [
        allowed(view, 'tick') && node.alive
          ? action('Tick', 'Move the clock of this node forward one tick.', () =>
              dispatch({ kind: 'tick', node: node.id }),
            )
          : null,
        allowed(view, 'start_election') && node.alive
          ? action('Elect', 'Campaign at a new, higher ballot.', () =>
              dispatch({ kind: 'start_election', node: node.id }),
            )
          : null,
        allowed(view, 'resend_pending') && node.alive
          ? action('Resend', 'Send every Accept that still waits for its quorum again.', () =>
              dispatch({ kind: 'resend_pending', node: node.id }),
            )
          : null,
        allowed(view, 'step_down') && node.alive
          ? action('Step down', 'The node gives up the leadership.', () =>
              dispatch({ kind: 'step_down', node: node.id }),
            )
          : null,
        allowed(view, 'crash') && node.alive
          ? action(
              'Crash',
              'Stop the node. Its disk stays.',
              () => dispatch({ kind: 'crash', node: node.id }),
              'control-button danger',
            )
          : null,
        allowed(view, 'restart') && !node.alive
          ? action('Restart', 'Start the node again from its disk.', () =>
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
              action('Set timeout', 'The number of ticks before this node campaigns.', () => {
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
    reachPicker(view, dispatch),
    clientControls(view, state, dispatch),
    nodeControls(view, state, dispatch),
  ].filter((block): block is HTMLElement => block !== null);
  return h('section', { class: 'controls' }, ...blocks);
}
