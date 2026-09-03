//! The [`Ready`] borrow guard: one batch of work, and the compile-time gate that
//! enforces "one batch in flight".

use crate::matchmaker::{GcRequest, MatchRequest};
use crate::membership::MatchmakerId;
use crate::message::Message;
use crate::node::{RawNode, ReadState};
use crate::types::{Ballot, Command, ConfigId, NodeId, Slot};
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
/// 4. **Answer** [`Ready::read_states`] — *after* step 3, so the applied state a
///    read serves covers the confirmed read index this same batch carried.
/// 5. Call [`Ready::advance`] to release the gate and unlock the next batch.
///
/// # Async drivers
///
/// The accessors borrow node-owned buffers, so a guard must not be held across
/// an `.await`. An async driver should copy the buckets out
/// (`writes().to_vec()`, `must_sync()`, `messages().to_vec()`,
/// `committed().to_vec()`), `advance()`, await its I/O, then call
/// [`RawNode::advance_recovery`] to release the next bounded continuation.
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

    /// Newly chosen `(slot, command)` pairs to apply **after** they are durable
    /// (step 3), surfaced in contiguous slot order (no gaps). Each is a
    /// [`Command::User`] client entry to hand the application, or a
    /// [`Command::Control`] the driver acts on (a `Truncate` records the durable
    /// floor).
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Command)] {
        self.node.pending_committed()
    }

    /// Snapshot offers to serve this batch: `(to, chosen_index, ballot, config_id)`.
    /// The core
    /// decided a peer needs a snapshot (it asked for a prefix below this node's
    /// compaction floor) but holds no application state, so the **driver** must
    /// read the opaque snapshot bytes from storage, build a
    /// [`Message::InstallSnapshot`] at `chosen_index`/`ballot`, and send it to
    /// `to`. Serve these only **after** applying [`Ready::committed`] (step 3)
    /// and making that application state durable (the application fsync, plus
    /// any truncate flush ordered behind it): the snapshot bytes are read from
    /// storage at serve time, so an offer served alongside step 2's messages
    /// could carry bytes that do not yet cover the advertised `chosen_index`
    /// boundary.
    #[must_use]
    pub fn snapshot_offers(&self) -> &[(NodeId, Slot, Ballot, ConfigId)] {
        self.node.pending_snapshot_offers()
    }

    /// Read-index rounds confirmed this batch: each [`ReadState`] certifies that
    /// this node was still leader after the read at `ctx` began (a heartbeat-ack
    /// quorum proved it) and that the applied prefix covers `index`. Answer them
    /// **after** applying [`Ready::committed`] (step 4) — the same batch may
    /// carry the very entries that satisfied the read's index. Consume-once:
    /// cleared on [`Ready::advance`], like every bucket.
    #[must_use]
    pub fn read_states(&self) -> &[ReadState] {
        self.node.pending_read_states()
    }

    /// Newly started leader-recovery rounds, how many are fresh gap fills, and
    /// the suffix slots remaining after this batch. Pure observability.
    #[must_use]
    pub fn recovery_batch(&self) -> Option<(usize, usize, usize)> {
        self.node.pending_recovery_batch()
    }

    /// Matchmaking requests to send this batch, one per addressed matchmaker
    /// (the leader-side half of the matchmaker contract, #120). Sent **after**
    /// step 1 like every message — the candidate's promise raise travels in
    /// the same batch — over the matchmaker RPC service, never the peer wire;
    /// the answers come back through [`RawNode::on_match_reply`]. Always empty
    /// on plain Multi-Paxos.
    #[must_use]
    pub fn match_requests(&self) -> &[(MatchmakerId, MatchRequest)] {
        self.node.pending_match_requests()
    }

    /// Garbage-collection requests to send this batch (#123), one per
    /// addressed matchmaker, over the matchmaker RPC service; the acks come
    /// back through [`RawNode::on_gc_ack`]. Always empty on plain Multi-Paxos.
    #[must_use]
    pub fn gc_requests(&self) -> &[(MatchmakerId, GcRequest)] {
        self.node.pending_gc_requests()
    }

    /// Acknowledge the batch: clears the pending buckets and releases the unique
    /// borrow, so the next [`RawNode::ready`] is allowed. Consumes `self` — the
    /// guard cannot be reused.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.node.config().id.0)))]
    pub fn advance(self) {
        self.node.clear_pending();
    }
}
