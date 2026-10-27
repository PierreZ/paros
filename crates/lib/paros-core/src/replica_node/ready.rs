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
#[must_use = "a ReplicaReady must be processed and then advanced; dropping it silently skips a batch"]
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

    /// Outbound messages: catch-up requests and quorum-read `PreRead`s,
    /// each to one node.
    #[must_use]
    pub fn messages(&self) -> &[(Audience, Message)] {
        &self.node.pending_messages
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

    /// The `(slot, ballot, command, outcome)` entries the prefix just
    /// walked over, in contiguous slot order, after the writes are durable —
    /// the node's [`crate::Ready::committed`] contract. `ballot` is the one
    /// this replica's learned record holds.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Ballot, Command, crate::journal::Outcome)] {
        self.node.replica.committed()
    }

    /// Acknowledge the batch: clears every bucket and releases the borrow.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(replica = self.node.config.id.0)))]
    pub fn advance(self) {
        self.node.clear_pending();
    }
}
