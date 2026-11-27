//! The replica's [`ReplicaReady`] batch.

use super::ReplicaNode;
use crate::contract::message::{Audience, Message};
use crate::contract::write::WriteOp;
use crate::journal::Command;
use crate::quorum_read::ReadState;
use crate::types::{Ballot, Slot};

/// One batch of a replica's work, and the compile-time gate that enforces
/// one batch in flight — the replica's [`crate::Ready`]. Process it in the
/// node's order: persist [`ReplicaReady::writes`], send
/// [`ReplicaReady::messages`], report [`ReplicaReady::committed`], then
/// [`ReplicaReady::advance`].
#[must_use = "a ReplicaReady must be processed and then advanced; a dropped one keeps its batch, which the next ready() returns again"]
pub struct ReplicaReady<'a> {
    pub(super) node: &'a mut ReplicaNode,
}

impl ReplicaReady<'_> {
    /// The durable writes to persist first, in order: [`WriteOp::Learned`],
    /// [`WriteOp::SetChosenIndex`], [`WriteOp::Truncate`],
    /// [`WriteOp::TrimmedTo`] — never an acceptor op.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn writes(&self) -> &[WriteOp] {
        let writes = &self.node.pending_writes;
        assert!(
            !writes.iter().any(|w| matches!(w, WriteOp::Acceptor(_))),
            "a replica persists no acceptor op"
        );
        writes
    }

    /// Outbound messages: catch-up requests, quorum-read `PreRead`s and
    /// the `SegmentHeld` reports of a sealed replica, each to one node.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn messages(&self) -> &[(Audience, Message)] {
        let me = self.node.config.id;
        let messages = &self.node.pending_messages;
        // A replica speaks only in its own name, and each message goes to
        // one party: a pooled node, or one peer of its segment (R8). It
        // never fans out and never addresses itself.
        for (audience, message) in messages {
            assert!(
                match message {
                    Message::CatchUpRequest { from, .. }
                    | Message::SegmentHeld { from, .. }
                    | Message::CatchUpResponse { from, .. } => *from == me,
                    Message::PreRead { reply_to, .. } => *reply_to == me,
                    _ => false,
                },
                "a replica sends only its own catch-up requests, pages, pre-reads and held reports"
            );
            assert!(
                match audience {
                    Audience::Node(to) => *to != me && self.node.in_pool(*to),
                    Audience::ReplicasOf { replicas, .. } => {
                        replicas.members().len() == 1
                            && replicas
                                .members()
                                .iter()
                                .all(|to| *to != me && self.node.is_segment_peer(*to))
                    }
                    _ => false,
                },
                "a replica's message goes to one pooled node or one peer of its segment"
            );
            assert!(
                !matches!(message, Message::CatchUpResponse { .. })
                    || matches!(audience, Audience::ReplicasOf { .. }),
                "a replica pages only a peer of its segment"
            );
        }
        messages
    }

    /// Quorum reads this replica can now answer from its own state: the
    /// row confirmed the index and this replica applied at or past it. The
    /// node's [`crate::Ready::read_states`] contract — answer each one once
    /// the batch's `committed` is applied.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn read_states(&self) -> &[ReadState] {
        let reads = &self.node.pending_read_states;
        assert!(
            reads.iter().all(|r| self.node.replica.covers(r.index)),
            "a surfaced read is covered by this replica's fold"
        );
        reads
    }

    /// Quorum reads this sealed replica answers "sealed" (R6 of
    /// `docs/architecture/04-7-segments.md`): the row confirmed an index
    /// past the end of its segment. The driver answers each one with the
    /// records of the segment from the read's position
    /// ([`ReplicaNode::read_log`](super::ReplicaNode::read_log), which ends
    /// with [`LogRead::Sealed`](crate::replica::LogRead::Sealed)), and the
    /// reader asks a replica of the next segment for the rest.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn sealed_reads(&self) -> &[ReadState] {
        let reads = &self.node.pending_sealed_reads;
        assert!(
            reads.is_empty() || self.node.replica.is_sealed(),
            "only a sealed replica answers a read sealed"
        );
        assert!(
            reads.iter().all(|r| !self.node.replica.covers(r.index)),
            "a read answered sealed asks past the end of the segment"
        );
        reads
    }

    /// The `(slot, ballot, command, outcome)` entries the prefix just
    /// walked over, in contiguous slot order, after the writes are durable —
    /// the node's [`crate::Ready::committed`] contract. `ballot` is the one
    /// this replica's learned record holds.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Ballot, Command, crate::journal::Outcome)] {
        let committed = self.node.replica.committed();
        assert!(
            committed.windows(2).all(|w| w[1].0.0 == w[0].0.0 + 1),
            "the committed entries are in contiguous slot order"
        );
        committed
    }

    /// Whether the batch holds no work: nothing to persist, send, answer or
    /// report. The replica's [`crate::Ready::is_empty`].
    ///
    /// # Panics
    ///
    /// If an assertion of a bucket fails: a programmer error, never an
    /// operating condition.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.writes().is_empty()
            && self.messages().is_empty()
            && self.read_states().is_empty()
            && self.sealed_reads().is_empty()
            && self.committed().is_empty()
    }

    /// Acknowledge the batch: clears every bucket and releases the borrow.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(replica = self.node.config.id.0)))]
    pub fn advance(self) {
        self.node.clear_pending();
    }
}
