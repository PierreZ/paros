//! The matchmaker's **durable write batch**: the ops a driver must persist
//! and fsync, and the borrow-guarded [`MatchmakerReady`] that hands them out
//! before the replies they cover may leave.

use std::collections::BTreeMap;

use super::{MatchReply, Matchmaker, MatchmakerHardState, ReconfigureReply, Registration};
use crate::types::Ballot;

/// A single semantic durable write the driver must apply to stable storage
/// and **fsync before** the batch's replies leave — every matchmaker write is
/// safety-critical, so there is no relaxed class here.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MatchmakerWriteOp {
    /// Register `config` under `ballot`. Append-only: `ballot` is strictly
    /// above every ballot the registry holds.
    Register {
        /// The ballot registered.
        ballot: Ballot,
        /// The record registered under it.
        registration: Registration,
    },
    /// Raise the durable GC watermark to `watermark` and drop every
    /// registration below it. Monotone: never below the current watermark.
    SetGcWatermark(Ballot),
    /// Persist the durable scalars whole (the generation state and the
    /// decree record). The watermark inside equals the durable one.
    SetScalars(MatchmakerHardState),
    /// Replace the registry whole — the activation of a successor generation:
    /// every record dropped, these written, and the scalars (whose watermark
    /// is the reconstructed one) persisted in the same batch.
    InstallRegistry {
        /// The scalars after activation.
        scalars: MatchmakerHardState,
        /// The reconstructed registry.
        registrations: BTreeMap<Ballot, Registration>,
    },
}

/// One batch of matchmaker work, and the compile-time gate enforcing one batch
/// in flight — the matchmaker's [`crate::Ready`].
///
/// # Durability ordering — process the buckets in this order
///
/// 1. **Persist** [`MatchmakerReady::writes`] to stable storage, in order, and
///    fsync them. Every write here is safety-critical.
/// 2. **Send** [`MatchmakerReady::replies`] and
///    [`MatchmakerReady::reconfigure_replies`] — *only after* step 1 is
///    durable. A `Registered` reply published before its registration is on
///    disk is the matchmaker's version of an un-promise: a crash then forgets
///    a configuration the proposer already believes every later leader will
///    be told about. The same holds for a `Stopped` that left before the
///    freeze was durable, and for a decree promise or vote.
/// 3. Call [`MatchmakerReady::advance`] to release the gate.
#[must_use = "a MatchmakerReady must be processed and then advanced; dropping it silently skips a batch"]
pub struct MatchmakerReady<'a> {
    pub(super) matchmaker: &'a mut Matchmaker,
}

impl MatchmakerReady<'_> {
    /// The durable writes to persist and fsync **first** (step 1), in order.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn writes(&self) -> &[MatchmakerWriteOp] {
        let writes = &self.matchmaker.pending_writes;
        // The durable watermark only rises, so every floor a batch stages is
        // at or below the one the matchmaker now holds — the batch can never
        // persist a floor memory has not reached.
        let watermark = self.matchmaker.hard_state.gc_watermark;
        assert!(
            writes.iter().all(|op| match op {
                MatchmakerWriteOp::Register { .. } => true,
                MatchmakerWriteOp::SetGcWatermark(raised) => *raised <= watermark,
                MatchmakerWriteOp::SetScalars(scalars)
                | MatchmakerWriteOp::InstallRegistry { scalars, .. } => {
                    scalars.gc_watermark <= watermark
                }
            }),
            "a matchmaker batch never stages a floor above the one it holds"
        );
        writes
    }

    /// The matchmaking replies to send **after** the writes are durable
    /// (step 2).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn replies(&self) -> &[MatchReply] {
        let replies = &self.matchmaker.pending_replies;
        assert!(
            replies
                .iter()
                .all(|r| r.matchmaker == self.matchmaker.config.id),
            "a matchmaker answers in its own name"
        );
        replies
    }

    /// The reconfiguration replies to send **after** the writes are durable
    /// (step 2).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn reconfigure_replies(&self) -> &[ReconfigureReply] {
        let replies = &self.matchmaker.pending_reconfigure_replies;
        assert!(
            replies
                .iter()
                .all(|r| r.matchmaker() == self.matchmaker.config.id),
            "a matchmaker answers a handover in its own name"
        );
        replies
    }

    /// Acknowledge the batch: clears the pending buckets and releases the
    /// unique borrow. Consumes `self` — the guard cannot be reused.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(matchmaker = self.matchmaker.config.id.0)))]
    pub fn advance(self) {
        self.matchmaker.pending_writes.clear();
        self.matchmaker.pending_replies.clear();
        self.matchmaker.pending_reconfigure_replies.clear();
        assert!(
            self.matchmaker.pending_writes.is_empty(),
            "an advanced batch has no write left"
        );
        assert!(
            self.matchmaker.pending_replies.is_empty(),
            "an advanced batch has no reply left"
        );
    }
}
