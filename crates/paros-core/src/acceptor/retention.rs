//! Log retention — the two ops that move the acceptor's compaction floor.
//!
//! A module, not a role. The floor belongs to the acceptor's windows (the
//! records and the faulty set both raise it), and the rule pinned by
//! `every_acceptor_mutation_emits_its_own_fsynced_write` is that the role
//! that moves durable state emits the write that makes it durable: a
//! separate retention *type* would either reach into the acceptor's windows
//! or leave the floor and its write in two hands, which is the ordering bug
//! the rule exists to prevent. So the ops stay methods of [`Acceptor`]; what
//! this file separates is the concern — truncation decided by consensus and
//! the fold an installed snapshot performs — from the voting state machine
//! beside it. The two callers differ only in the durable op they emit
//! beside the same private `drop_prefix`.

use super::Acceptor;
use crate::types::{Ballot, SessionEntry, Slot, Value};
use crate::write::WriteOp;

impl<V: Clone + PartialEq> Acceptor<V> {
    /// Drop every record and faulty entry below `first`, raise the floor to
    /// it, and emit the durable [`WriteOp::Truncate`] carrying `sealed` (the
    /// at-most-once ledger records whose slots the drop removes). A decided
    /// truncation: the caller has already established that the prefix is
    /// chosen and applied.
    ///
    /// # Panics
    ///
    /// If `first` is below the floor held.
    pub fn truncate(&mut self, first: Slot, sealed: Vec<SessionEntry>, writes: &mut Vec<WriteOp>) {
        self.drop_prefix(first);
        writes.push(WriteOp::Truncate { first, sealed });
    }

    /// Fold the prefix an installed snapshot covers: drop every record and
    /// faulty entry at or below `chosen_index` (their decided effects live in
    /// the opaque bytes now), raise the floor one past it, and emit the
    /// durable [`WriteOp::InstallSnapshot`]. Returns the new floor.
    ///
    /// The caller adopts the snapshot's ballot through [`Self::set_promise`]
    /// *before* this call (the promise never regresses) and owns everything
    /// outside the acceptor — the replica's prefix jump, the proposer's
    /// blocked work. What the acceptor owns is the floor and the write.
    ///
    /// # Panics
    ///
    /// If the resulting floor is below the floor held, or `chosen_index` is
    /// the numeric ceiling (the caller's wire guard refuses one).
    pub fn install(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        snapshot: Value,
        sessions: Vec<SessionEntry>,
        writes: &mut Vec<WriteOp>,
    ) -> Slot {
        assert!(
            chosen_index.0 < u64::MAX,
            "a snapshot boundary has a floor one past it"
        );
        let first = Slot(chosen_index.0 + 1);
        self.drop_prefix(first);
        writes.push(WriteOp::InstallSnapshot {
            chosen_index,
            ballot,
            snapshot,
            sessions,
        });
        first
    }

    /// Drop every record and faulty entry below `first` and raise the floor
    /// to it. The floor never moves backward. The two callers differ only in
    /// the durable op they emit beside it.
    fn drop_prefix(&mut self, first: Slot) {
        assert!(
            first >= self.first_slot(),
            "the compaction floor never moves backward"
        );
        let watermark_before = self.vote_watermark();
        self.records.raise_floor(first);
        self.faulty.raise_floor(first);
        self.assert_invariants();
        // Postcondition: the slots a truncation drops were voted, and the
        // floor now stands in for them — the watermark never regresses.
        assert!(
            self.vote_watermark() >= watermark_before,
            "the vote watermark never decreases across a truncation"
        );
    }
}
