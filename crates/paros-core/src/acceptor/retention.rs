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
//! the jump below a peer's trim point — from the voting state machine
//! beside it. The two callers differ only in the durable op they emit
//! beside the same private `drop_prefix`.

use super::Acceptor;
use crate::journal_state::JournalState;
use crate::types::Slot;
use crate::write::WriteOp;

impl<V: Clone + PartialEq> Acceptor<V> {
    /// Drop every record and faulty entry below `first`, raise the floor to
    /// it, and emit the durable [`WriteOp::Truncate`] carrying `sealed` (the
    /// journal state the dropped slots folded to, #204). A decided
    /// truncation: the caller has already established that the prefix is
    /// chosen and applied.
    ///
    /// # Panics
    ///
    /// If `first` is below the floor held.
    pub fn truncate(&mut self, first: Slot, sealed: JournalState, writes: &mut Vec<WriteOp>) {
        self.drop_prefix(first);
        writes.push(WriteOp::Truncate { first, sealed });
    }

    /// Jump below the trim point (#186): drop every record and faulty
    /// entry below `point` (everything there is chosen, and the log is
    /// trimmed cluster-wide), raise the floor to it, and emit the durable
    /// [`WriteOp::TrimmedTo`]. The promise does not move. The caller owns
    /// everything outside the acceptor — the replica's prefix jump, the
    /// proposer's blocked work.
    ///
    /// # Panics
    ///
    /// If `point` is below the floor held.
    pub fn trim_to(&mut self, point: Slot, state: JournalState, writes: &mut Vec<WriteOp>) {
        self.drop_prefix(point);
        writes.push(WriteOp::TrimmedTo { point, state });
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
