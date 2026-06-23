//! The [`RawNode`] handle: the sans-IO state machine and the `step`/`tick`/
//! `ready`/`advance` contract.

use crate::message::Message;
use crate::ready::Ready;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{Slot, Value};

/// The pure, synchronous, single-threaded Multi-Paxos state machine.
///
/// No I/O, no clock, no randomness. Inputs arrive via [`RawNode::step`] (peer
/// messages and tick-injected self-events) and [`RawNode::tick`] (logical time).
/// Output is drained via [`RawNode::ready`] and acknowledged via
/// [`Ready::advance`]. This is the sans-IO object; a driver (an async runtime or
/// a deterministic simulator) wraps it and performs all side effects in the
/// order the [`Ready`] documents.
///
/// Stage 0 pins this contract in the type system with **zero protocol logic** —
/// `step`/`tick` are no-op stubs and the output buckets stay empty.
pub struct RawNode {
    /// This node's static identity and membership.
    config: Config,
    /// The must-be-durable state. Mutated by `step`/`tick`; surfaced for
    /// persistence via [`Ready::hard_state`].
    hard_state: HardState,

    // ---- pending output buckets: filled by the protocol logic, drained by
    // ---- `ready`, cleared by `advance`. All empty in Stage 0.
    /// `Some` when `hard_state` changed and must be persisted this batch.
    pending_hard_state: Option<HardState>,
    /// Outbound messages buffered for this batch.
    pending_messages: Vec<Message>,
    /// Newly chosen entries to apply this batch.
    pending_committed: Vec<(Slot, Value)>,

    /// Logical clock, advanced by [`RawNode::tick`]. Stage 0: a bare counter
    /// with no thresholds.
    tick_count: u64,
}

impl RawNode {
    /// Construct from a read-only [`Storage`] by reading durable state back in.
    /// Bootstrap and restart share this path. Stage 0 simply adopts
    /// `initial_state()` and performs no recovery logic.
    pub fn new<S: Storage>(storage: &S) -> Self {
        let (hard_state, config) = storage.initial_state();
        Self {
            config,
            hard_state,
            pending_hard_state: None,
            pending_messages: Vec::new(),
            pending_committed: Vec::new(),
            tick_count: 0,
        }
    }

    /// The single input entry point: every stimulus is a [`Message`]. Stage 0 is
    /// a no-op — protocol routing lands in a later stage.
    pub fn step(&mut self, _msg: Message) {}

    /// Advance logical time by one tick. Later stages synthesize
    /// `CheckLeader`/`Heartbeat` self-events here and route them through `step`;
    /// the core reads no clock, so the caller decides what a tick is worth.
    /// Stage 0 just counts.
    pub fn tick(&mut self) {
        self.tick_count += 1;
    }

    /// Borrow the node to drain one batch of work. The returned [`Ready`] holds
    /// the unique `&mut` borrow, so a second `ready()` before [`Ready::advance`]
    /// is a **compile error**. See [`Ready`] for the persist-before-send ordering
    /// the caller must honor.
    pub fn ready(&mut self) -> Ready<'_> {
        Ready::new(self)
    }

    /// This node's configuration (identity + membership).
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The current durable state. (The pending, to-be-persisted view is exposed
    /// through [`Ready::hard_state`].)
    #[must_use]
    pub fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    /// The number of ticks observed so far.
    #[must_use]
    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }

    // ---- crate-internal accessors used by `Ready` (not public API) ----

    pub(crate) fn pending_hard_state(&self) -> Option<&HardState> {
        self.pending_hard_state.as_ref()
    }

    pub(crate) fn pending_messages(&self) -> &[Message] {
        &self.pending_messages
    }

    pub(crate) fn pending_committed(&self) -> &[(Slot, Value)] {
        &self.pending_committed
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_hard_state = None;
        self.pending_messages.clear();
        self.pending_committed.clear();
    }
}
