//! The [`Ready`] borrow guard: one batch of work, and the compile-time gate that
//! enforces "one batch in flight".

use crate::contract::message::{Audience, Message};
use crate::contract::write::{AcceptorWrite, WriteOp};
use crate::journal::{Command, JournalIdentifier};
use crate::matchmaker::{GcRequest, MatchRequest};
use crate::membership::MatchmakerId;
use crate::node::ColocatedNode;
use crate::quorum_read::ReadState;
use crate::types::{Ballot, NodeId, Slot};

/// A single batch of work the caller must process, and a **compile-time gate**
/// enforcing one batch in flight.
///
/// `Ready` holds the unique mutable borrow of its [`ColocatedNode`]. Because that
/// borrow is alive for the lifetime of the `Ready`, the borrow checker makes a
/// second `node.ready()` a **compile error** until this guard is consumed by
/// [`Ready::advance`]. (Contrast etcd-raft, which only *panics at runtime* on a
/// second `Ready()` without `Advance()`.)
///
/// # Durability ordering — process the buckets in this order
///
/// 1. **Persist** [`Ready::writes`] to stable storage, applying each
///    [`WriteOp`] in order; fsync the batch first if any op
///    [`needs_sync`](WriteOp::needs_sync) (the [`crate::contract::MustSync`] contract).
/// 2. **Send** [`Ready::messages`] to peers — *only after* step 1 is durable. A
///    `Promise`/`Accepted` published before its durable write is on disk is a
///    safety violation (a crash could un-promise / un-accept).
/// 3. **Learn** [`Ready::committed`] — the slots the contiguous chosen prefix
///    just walked over, already chosen *and* durable: ack the clients waiting
///    on them. paros runs no application (#186); a journal client folds what
///    it reads.
/// 4. **Answer** [`Ready::read_states`] — *after* step 3, so the prefix a
///    read serves covers the read index this same batch carried.
/// 5. Call [`Ready::advance`] to release the gate and unlock the next batch.
///
/// # Async drivers
///
/// The accessors borrow node-owned buffers, so a guard must not be held across
/// an `.await`. An async driver copies the buckets out
/// (`writes().to_vec()`, `messages().to_vec()`,
/// `committed().to_vec()`), calls [`Ready::release`] in place of `advance()`,
/// awaits its flush, gives the [`Flight`] back with
/// [`ColocatedNode::durable`], then calls
/// [`ColocatedNode::advance_recovery`] to release the next bounded continuation.
///
/// **One batch in flight.** Between the end of a batch and the end of its
/// flush, the driver must not give the node an input (`step`, `tick`,
/// `propose*` or any other mutating call) and must not send a message of a
/// later batch. The core assumes that the batch of an earlier input is
/// durable when a later input arrives: an acceptor emits no write for a
/// `Prepare` at the ballot it already promised, because the batch that raised
/// the promise carries it. A `Promise` sent before that batch is durable can
/// be lost in a crash after a candidate counted it. The core holds this rule
/// for a driver that releases its batches: every mutating entry point of a
/// node with a batch in flight panics, and so does `ready()`. A driver that
/// flushes before it calls `advance()` holds the rule by its order alone.
#[must_use = "a Ready must be processed and then advanced; a dropped Ready keeps its batch, which the next ready() returns again"]
pub struct Ready<'a> {
    node: &'a mut ColocatedNode,
}

impl<'a> Ready<'a> {
    /// Wrap a uniquely-borrowed node. Crate-internal: only [`ColocatedNode::ready`]
    /// constructs a `Ready`.
    pub(crate) fn new(node: &'a mut ColocatedNode) -> Self {
        Self { node }
    }

    /// The semantic durable write deltas to persist **first** (step 1), in apply
    /// order. Empty when nothing durable changed this batch.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn writes(&self) -> &[WriteOp] {
        let writes = self.node.pending_writes();
        // The write half of the pairs the boot scan re-asserts: no record is
        // persisted above the promise, and every durable scalar the batch
        // carries is monotone and at or below the value memory now holds.
        let promised = self.node.acceptor().promised();
        assert!(
            writes.iter().all(|op| match op {
                WriteOp::Acceptor(
                    AcceptorWrite::SetPromise(ballot)
                    | AcceptorWrite::AppendAccepted { ballot, .. },
                ) => *ballot <= promised,
                _ => true,
            }),
            "a batch persists no promise or record above the promise in memory"
        );
        let raises: Vec<Ballot> = writes
            .iter()
            .filter_map(|op| match op {
                WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot)) => Some(*ballot),
                _ => None,
            })
            .collect();
        assert!(
            raises.windows(2).all(|w| w[0] < w[1]),
            "a batch only ever raises the promise"
        );
        let chosen: Vec<Slot> = writes
            .iter()
            .filter_map(|op| match op {
                WriteOp::SetChosenIndex(slot) => Some(*slot),
                _ => None,
            })
            .collect();
        assert!(
            chosen.windows(2).all(|w| w[0] < w[1]),
            "a batch only ever advances the chosen index"
        );
        assert!(
            chosen
                .last()
                .is_none_or(|ci| Some(*ci) <= self.node.replica().chosen_index()),
            "a batch persists no chosen index past the one in memory"
        );
        let floors: Vec<Slot> = writes
            .iter()
            .filter_map(|op| match op {
                WriteOp::Truncate { first, .. } => Some(*first),
                WriteOp::TrimmedTo { point, .. } => Some(*point),
                _ => None,
            })
            .collect();
        assert!(
            floors.windows(2).all(|w| w[0] < w[1]),
            "a batch only ever raises the floor"
        );
        assert!(
            floors
                .last()
                .is_none_or(|floor| *floor == self.node.acceptor().first_slot()),
            "a batch's last floor is the floor in memory"
        );
        writes
    }

    /// Outbound messages to send **after** [`Ready::writes`] are durable
    /// (step 2). Each entry is `(audience, message)`: the core decides *who*
    /// every message is for in protocol terms (`Promise`/`Accepted`/`Nack`
    /// answer the address the request named; an `Accept` is for the acceptors
    /// of a configuration; a `Commit` or a beat is for the learners), and the
    /// driver's deployment map turns an [`Audience`] into node ids
    /// ([`Audience::resolve`]) and those into addresses. It still makes no
    /// routing *decision* — and a fan-out costs one entry, not one clone of
    /// the bytes per addressee.
    #[must_use]
    pub fn messages(&self) -> &[(Audience, Message)] {
        self.node.pending_messages()
    }

    /// The `(slot, ballot, command, outcome)` entries the contiguous chosen
    /// prefix just walked over, **after** they are durable (step 3), in slot
    /// order (no gaps). `ballot` is the ballot of this node's record of the
    /// slot when the walk applied it; `outcome` is the journal state
    /// machine's verdict. Both ride with the slot, so a slot a `Truncate`
    /// compacts later in the same walk still carries them. A #94 duplicate
    /// surfaces as the `Noop` the walk executed it as. The driver acks the
    /// clients waiting on these slots and reports them; there is no
    /// application to hand them to (#186).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Ballot, Command, crate::journal::Outcome)] {
        let committed = self.node.pending_committed();
        // The walk moves forward: each slot once, in slot order.
        assert!(
            committed.windows(2).all(|w| w[0].0 < w[1].0),
            "the walk surfaces each slot once, in order"
        );
        committed
    }

    /// Leaderless quorum reads served this batch: each [`ReadState`] says that
    /// a Phase-1 quorum of the read basis answered the read at `ctx` with vote
    /// watermarks whose maximum is `index`, and that this node's chosen prefix
    /// covers `index`. Answer them **after** applying [`Ready::committed`]
    /// (step 4) — the same batch may carry the very entries that satisfied the
    /// read's index. Consume-once: cleared on [`Ready::advance`], like every
    /// bucket.
    #[must_use]
    pub fn read_states(&self) -> &[ReadState] {
        self.node.pending_read_states()
    }

    /// Newly started leader-recovery rounds, how many are fresh gap fills, and
    /// the suffix slots remaining after this batch. Also the pacing gate: while
    /// a batch is pending the core starts no further recovery page;
    /// [`Ready::advance`] clears it and [`ColocatedNode::advance_recovery`]
    /// schedules the next.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn recovery_batch(&self) -> Option<(usize, usize, usize)> {
        self.node.pending_recovery_batch()
    }

    /// Matchmaking requests to send this batch, one per addressed matchmaker
    /// (the leader-side half of the matchmaker contract, #120). Sent **after**
    /// step 1 like every message — the candidate's promise raise travels in
    /// the same batch — over the matchmaker RPC service, never the peer wire;
    /// the answers come back through [`ColocatedNode::on_match_reply`]. Always empty
    /// on plain Multi-Paxos.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn match_requests(&self) -> &[(MatchmakerId, MatchRequest)] {
        self.node.pending_match_requests()
    }

    /// Garbage-collection requests to send this batch (#123), one per
    /// addressed matchmaker, over the matchmaker RPC service; the acks come
    /// back through [`ColocatedNode::on_gc_ack`]. Always empty on plain Multi-Paxos.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn gc_requests(&self) -> &[(MatchmakerId, GcRequest)] {
        self.node.pending_gc_requests()
    }

    /// Whether the batch holds no work: nothing to persist, send, learn or
    /// answer. A driver loop drains a node until its batch is empty:
    ///
    /// ```
    /// use paros_consensus::contract::MemStorage;
    /// use paros_consensus::journal::JournalIdentifier;
    /// use paros_consensus::{ColocatedNode, Config, Limits, NodeId};
    ///
    /// let config = Config::new(NodeId(1), JournalIdentifier::UNSET).with_peers([NodeId(1)]);
    /// let mut disk = MemStorage::new(config);
    /// let mut node = ColocatedNode::new(&disk, Limits::DEFAULT);
    /// node.campaign();
    /// loop {
    ///     let ready = node.ready();
    ///     if ready.is_empty() {
    ///         ready.advance();
    ///         break;
    ///     }
    ///     disk.apply(ready.writes());
    ///     // A node alone has no peer: a real driver sends `ready.messages()` here.
    ///     ready.advance();
    ///     node.advance_recovery();
    /// }
    /// assert!(node.is_leader());
    /// ```
    ///
    /// # Panics
    ///
    /// If an assertion of a bucket fails: a programmer error, never an
    /// operating condition.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.writes().is_empty()
            && self.messages().is_empty()
            && self.committed().is_empty()
            && self.read_states().is_empty()
            && self.recovery_batch().is_none()
            && self.match_requests().is_empty()
            && self.gc_requests().is_empty()
    }

    /// Acknowledge the batch: clears the pending buckets and releases the unique
    /// borrow, so the next [`ColocatedNode::ready`] is allowed. Consumes `self` — the
    /// guard cannot be reused. Call it once the batch is durable and sent; a
    /// driver that flushes after it lets go of the buckets calls
    /// [`Ready::release`] instead.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.node.config().id.0)))]
    pub fn advance(self) {
        self.node.clear_pending();
    }

    /// Let go of the buckets before their flush is done: clears them like
    /// [`Ready::advance`], and puts the batch **in flight**. The node takes
    /// no input until the driver gives the returned [`Flight`] back with
    /// [`ColocatedNode::durable`], once every write of the batch is durable.
    /// The core then holds the rule of one batch in flight (the section
    /// "Async drivers" above) and does not leave it to the driver's order.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.node.config().id.0)))]
    pub fn release(self) -> Flight {
        let flight = self.node.begin_flight();
        assert!(self.node.in_flight(), "a released batch is in flight");
        flight
    }
}

/// A batch in flight ([`Ready::release`]): the token a driver gives back to
/// [`ColocatedNode::durable`] once every write of the batch is durable. It
/// names its node, its journal and its batch, so a token given back twice,
/// to another node or to another journal of the same node panics.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a Flight must be given back to ColocatedNode::durable once its batch is durable; the node takes no input until then"]
pub struct Flight {
    pub(crate) node: NodeId,
    pub(crate) journal: JournalIdentifier,
    pub(crate) batch: u64,
}
