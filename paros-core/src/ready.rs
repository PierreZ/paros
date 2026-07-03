//! The [`Ready`] borrow guard: one batch of work, and the compile-time gate that
//! enforces "one batch in flight".

use crate::message::Message;
use crate::node::RawNode;
use crate::types::{Entry, NodeId, Slot};
use crate::write::{self, MustSync, WriteOp};

/// A single batch of work the caller must process, and a **compile-time gate**
/// enforcing one batch in flight.
///
/// `Ready` holds the unique mutable borrow of its [`RawNode`]. Because that
/// borrow is alive for the lifetime of the `Ready`, the borrow checker makes a
/// second `node.ready()` a **compile error** until this guard is consumed by
/// [`Ready::advance`]. (Contrast etcd-raft, which only *panics at runtime* on a
/// second `Ready()` without `Advance()`.)
///
/// # Durability ordering — process the buckets in this order
///
/// 1. **Persist** [`Ready::writes`] to stable storage, applying each
///    [`WriteOp`] in order; fsync the batch first if [`Ready::must_sync`] is
///    [`MustSync::Sync`].
/// 2. **Send** [`Ready::messages`] to peers — *only after* step 1 is durable. A
///    `Promise`/`Accepted` published before its durable write is on disk is a
///    safety violation (a crash could un-promise / un-accept).
/// 3. **Apply** [`Ready::committed`] to the application state machine — these are
///    already chosen *and* durable.
/// 4. Call [`Ready::advance`] to release the gate and unlock the next batch.
///
/// # Async drivers
///
/// The accessors borrow node-owned buffers, so a guard must not be held across
/// an `.await`. An async driver should copy the buckets out
/// (`writes().to_vec()`, `must_sync()`, `messages().to_vec()`,
/// `committed().to_vec()`), `advance()`, and *then* await its I/O.
#[must_use = "a Ready must be processed and then advanced; dropping it silently skips a batch"]
pub struct Ready<'a> {
    node: &'a mut RawNode,
}

impl<'a> Ready<'a> {
    /// Wrap a uniquely-borrowed node. Crate-internal: only [`RawNode::ready`]
    /// constructs a `Ready`.
    pub(crate) fn new(node: &'a mut RawNode) -> Self {
        Self { node }
    }

    /// The semantic durable write deltas to persist **first** (step 1), in apply
    /// order. Empty when nothing durable changed this batch.
    #[must_use]
    pub fn writes(&self) -> &[WriteOp] {
        self.node.pending_writes()
    }

    /// Whether this batch must be fsync'd before its [`Ready::messages`] are sent.
    /// [`MustSync::Sync`] when any write raises a promise or appends an accept;
    /// [`MustSync::Relaxed`] when the batch only advances the chosen index.
    #[must_use]
    pub fn must_sync(&self) -> MustSync {
        write::classify(self.node.pending_writes())
    }

    /// Outbound messages to send **after** [`Ready::hard_state`] is durable
    /// (step 2). Each entry is `(destination, message)`: the core decides where
    /// every message goes (`Promise`/`Accepted`/`Nack` reply to the proposer;
    /// `Prepare`/`Accept`/`Commit` fan out to peers), so the driver only maps the
    /// `NodeId` to an address and sends — it makes no routing decision.
    #[must_use]
    pub fn messages(&self) -> &[(NodeId, Message)] {
        self.node.pending_messages()
    }

    /// Newly chosen `(slot, entry)` pairs to apply **after** they are durable
    /// (step 3), surfaced in contiguous slot order (no gaps).
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Entry)] {
        self.node.pending_committed()
    }

    /// Acknowledge the batch: clears the pending buckets and releases the unique
    /// borrow, so the next [`RawNode::ready`] is allowed. Consumes `self` — the
    /// guard cannot be reused.
    pub fn advance(self) {
        self.node.clear_pending();
        // Stage 0: no follow-up self-messages to replay on advance yet.
    }
}
