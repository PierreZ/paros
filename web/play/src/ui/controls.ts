// The controls: everything the player can do that is not a message on the wire.
//
// Which controls exist is derived from `LevelView.allowed_actions` and the
// world's flavour — never from the level id. A level that stops offering
// `crash` stops rendering a Crash button, and a level the engine adds
// tomorrow gets its controls for free.

import type {
  Action,
  ActionKind,
  ClientView,
  GameView,
  MessageView,
  NodeView,
  Phase,
  Seam,
} from '../types';
import { h } from '../render/dom';
import { gridOf } from '../render/grid';
import { memberCount, phaseSize, quorumOf } from './quorum';

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
  /**
   * The column the next write's Accept goes to, as the player typed it. An
   * empty box lets the level ask, which is what a grid level does.
   */
  proposeColumn: string;
  /** The client that reads without a leader, or `null` for the first client. */
  quorumReadClient: number | null;
  /** The peer each leader offers its authority to. */
  handoffTargets: Map<number, number>;
  /** The slot each node's next corruption damages. */
  corruptSlots: Map<number, string>;
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
    proposeColumn: '',
    quorumReadClient: null,
    handoffTargets: new Map(),
    corruptSlots: new Map(),
  };
}

// ---- what this level offers -------------------------------------------------

/** One per-node control. */
export type NodeControl =
  | 'tick'
  | 'start_election'
  | 'resend_pending'
  | 'step_down'
  | 'quorum_read'
  | 'relinquish'
  | 'corrupt'
  | 'wipe'
  | 'crash'
  | 'crash_at'
  | 'restart';

/**
 * Which controls this level offers for this node.
 *
 * Two rules, and no third: the level must list the action, and the node must
 * be in a state where the action means something. A crashed node is not
 * ticked and a running node is not restarted. Everything else — whether this
 * node leads, whether it holds a record for a slot — is the engine's answer,
 * and the refusal it sends is what the player reads.
 */
export function nodeControlsFor(view: GameView, node: NodeView): NodeControl[] {
  const offered = (kind: ActionKind): boolean => allowed(view, kind);
  const live: NodeControl[] = [
    'tick',
    'start_election',
    'resend_pending',
    'step_down',
    'quorum_read',
    'relinquish',
    'crash',
    'crash_at',
  ];
  const controls: NodeControl[] = [];
  for (const control of live) {
    if (offered(control) && node.alive) controls.push(control);
  }
  // A disk is damaged and erased whether the node runs or not: the damage
  // shows up at the next boot, which is what these two levels are about.
  if (offered('corrupt')) controls.push('corrupt');
  if (offered('wipe')) controls.push('wipe');
  if (offered('restart') && !node.alive) controls.push('restart');
  return controls;
}

/**
 * The peers a leader may be asked to hand its authority to.
 *
 * Every other node is offered. Whether the hand-off is legal is the engine's
 * answer: it refuses with `handoff_refused` and names the rule.
 */
export function handoffTargets(view: GameView, node: NodeView): number[] {
  return view.world.nodes.filter((peer) => peer.id !== node.id).map((peer) => peer.id);
}

/**
 * The nodes a copy of `message` may be misrouted to.
 *
 * A misrouted message is a thing networks do, and every rule in the protocol
 * is written to survive one, so every node except the addressee is offered.
 */
export function misrouteTargets(view: GameView, message: MessageView): number[] {
  if (!allowed(view, 'duplicate')) return [];
  return view.world.nodes.filter((node) => node.id !== message.to).map((node) => node.id);
}

/**
 * The columns a write may be addressed to, or `null` when the deployment runs
 * no grid.
 *
 * The shape comes from the engine's own quorum view. An empty choice lets the
 * configuration derive the column, which is what every deployment that is not
 * a grid does — and what a grid level asks the player about.
 */
export function proposeColumns(view: GameView): number[] | null {
  const shape = gridOf(view.world);
  if (!shape) return null;
  return Array.from({ length: shape.cols }, (_, column) => column);
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
  // The sizes the two phases need come from the engine's quorum view. A
  // majority sends no numbers, so the picker prints none.
  const quorum = quorumOf(acceptors);
  const members = memberCount(acceptors);
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
      const need = phaseSize(quorum, phase);
      const size =
        need === null
          ? null
          : h(
              'span',
              { class: 'control-hint' },
              `${set.size} of ${members} · this phase needs ${need}`,
            );
      const boxes = acceptors.map((acceptor) => {
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
      });
      return h(
        'div',
        { class: 'control-row reach-row' },
        h('span', { class: 'control-label', title: hint }, label),
        h('span', { class: 'reach-boxes' }, ...boxes),
        size,
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
  const columns = proposeColumns(view);
  const column = columns === null ? null : columnSelect(state, columns);
  const submit = (): void => {
    const picked = column === null || column.value === '' ? null : Number(column.value);
    dispatch({
      kind: 'propose',
      node: Number(select.value),
      client: who ? Number(who.value) : client,
      value: input.value.trim() || 'x=1',
      // An empty box lets the configuration derive the column, which is what
      // every deployment that is not a grid does — and what makes a grid level
      // ask the player.
      column: picked,
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
    column ? h('span', { class: 'control-hint' }, 'column') : null,
    column,
    action('Write', 'The client asks this node to get the command chosen.', submit),
  );
}

/**
 * Which column of the grid the next write's Accept goes to.
 *
 * The empty option is the default: it sends no column, and the configuration
 * derives the one the rule gives. A level that teaches the rule asks the
 * player instead, through the prompt card.
 */
function columnSelect(state: ControlState, columns: readonly number[]): HTMLSelectElement {
  const select = h('select', {
    class: 'node-select column-select',
    'data-focus-key': 'propose-column',
    title: 'The column that votes for this slot. Leave it empty and the level asks you.',
  });
  select.append(
    h('option', { value: '', selected: state.proposeColumn === '' }, 'let the level ask'),
  );
  for (const column of columns) {
    select.append(
      h(
        'option',
        { value: column, selected: state.proposeColumn === String(column) },
        `column ${column}`,
      ),
    );
  }
  select.addEventListener('change', () => {
    state.proposeColumn = select.value;
  });
  return select;
}

function quorumReadRow(view: GameView, state: ControlState): HTMLElement | null {
  if (!allowed(view, 'quorum_read')) return null;
  const clients = view.world.clients;
  if (clients.length < 2) return null;
  const client = state.quorumReadClient ?? clients[0]?.id ?? 0;
  const who = clientSelect('quorum-read-client', clients, client, (id) => {
    state.quorumReadClient = id;
  });
  if (!who) return null;
  return h(
    'div',
    { class: 'control-row' },
    h('span', { class: 'control-label' }, 'a leaderless read comes from'),
    who,
    h(
      'span',
      { class: 'control-hint' },
      'Ask a node for a quorum read with the button in its own row.',
    ),
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
    quorumReadRow(view, state),
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

/** The select that names the peer a leader offers its authority to. */
function handoffBox(
  view: GameView,
  node: NodeView,
  state: ControlState,
  dispatch: Dispatch,
): HTMLElement | null {
  const targets = handoffTargets(view, node);
  if (targets.length === 0) return null;
  const chosen = state.handoffTargets.get(node.id) ?? targets[0] ?? 0;
  const select = h('select', {
    class: 'node-select',
    'data-focus-key': `handoff-${node.id}`,
  });
  for (const id of targets) {
    select.append(h('option', { value: id, selected: id === chosen }, `node ${id}`));
  }
  select.addEventListener('change', () => {
    state.handoffTargets.set(node.id, Number(select.value));
  });
  return h(
    'span',
    { class: 'handoff-box' },
    action(
      'Hand off to',
      'The leader gives its authority to this peer, under the same ballot and with no Phase 1.',
      () => dispatch({ kind: 'relinquish', node: node.id, to: Number(select.value) }),
    ),
    select,
  );
}

/** The box that names the slot whose record rots on this node's disk. */
function corruptBox(node: NodeView, state: ControlState, dispatch: Dispatch): HTMLElement {
  const value = state.corruptSlots.get(node.id) ?? '0';
  const input = textInput(`corrupt-${node.id}`, value, 'slot', (next) => {
    state.corruptSlots.set(node.id, next);
  });
  input.classList.add('tiny');
  const submit = (): void => {
    const slot = Number.parseInt(input.value, 10);
    if (!Number.isFinite(slot) || slot < 0) return;
    dispatch({ kind: 'corrupt', node: node.id, slot });
  };
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') submit();
  });
  return h(
    'span',
    { class: 'corrupt-box' },
    action(
      'Corrupt slot',
      'The value of this record is lost. The slot and the ballot beside it survive.',
      submit,
      'control-button danger',
    ),
    input,
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
    'quorum_read',
    'relinquish',
    'corrupt',
    'wipe',
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
      const offered = new Set(nodeControlsFor(view, node));
      const readClient =
        state.quorumReadClient ?? view.world.clients[0]?.id ?? null;
      const buttons: (HTMLElement | null)[] = [
        offered.has('tick')
          ? action('Tick', 'Move the clock of this node forward one tick.', () =>
              dispatch({ kind: 'tick', node: node.id }),
            )
          : null,
        offered.has('start_election')
          ? action('Elect', 'Campaign at a new, higher ballot.', () =>
              dispatch({ kind: 'start_election', node: node.id }),
            )
          : null,
        offered.has('resend_pending')
          ? action('Resend', 'Send every Accept that still waits for its quorum again.', () =>
              dispatch({ kind: 'resend_pending', node: node.id }),
            )
          : null,
        offered.has('step_down')
          ? action('Step down', 'The node gives up the leadership.', () =>
              dispatch({ kind: 'step_down', node: node.id }),
            )
          : null,
        offered.has('quorum_read')
          ? action(
              'Quorum read',
              'The client asks this node for a read. The node asks a row, and no leader is involved.',
              () => dispatch({ kind: 'quorum_read', node: node.id, client: readClient }),
            )
          : null,
        offered.has('relinquish') ? handoffBox(view, node, state, dispatch) : null,
        offered.has('crash')
          ? action(
              'Crash',
              'Stop the node. Its disk stays.',
              () => dispatch({ kind: 'crash', node: node.id }),
              'control-button danger',
            )
          : null,
        offered.has('restart')
          ? action('Restart', 'Start the node again from its disk.', () =>
              dispatch({ kind: 'restart', node: node.id }),
            )
          : null,
        offered.has('corrupt') ? corruptBox(node, state, dispatch) : null,
        offered.has('wipe')
          ? action(
              'Wipe',
              'Erase the whole disk. The promise goes with it, and it cannot come back.',
              () => dispatch({ kind: 'wipe', node: node.id }),
              'control-button danger',
            )
          : null,
        ...(offered.has('crash_at') ? seamButtons(node, dispatch) : []),
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
