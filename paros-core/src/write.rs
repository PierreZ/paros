//! Durable write deltas ([`WriteOp`]) and their durability classification
//! ([`MustSync`]) — the semantic persistence contract a [`crate::Ready`] batch
//! surfaces.
//!
//! Instead of cloning the whole [`crate::HardState`] on every mutation, the core
//! emits the *minimal* per-mutation deltas: raise the promised ballot, append (or
//! overwrite) a per-slot accepted entry, or advance the chosen index. This mirrors
//! etcd-raft's `HardState`-vs-`entries` split — the two small scalars persist
//! whole, the log persists per record — and is what lets later stages truncate,
//! checksum, and recover per entry without a blob rewrite.

use crate::types::{Ballot, Entry, Slot};

/// A single semantic durable write the driver must apply to stable storage,
/// **in order**, before sending the batch's messages.
///
/// The three variants map one-to-one onto the durable state:
/// [`SetPromise`](WriteOp::SetPromise) raises the promised ballot (Phase 1),
/// [`AppendAccepted`](WriteOp::AppendAccepted) records the `(ballot, entry)` a
/// slot has accepted (Phase 2, an upsert-by-slot — a chosen value overwrites any
/// stale lower-ballot accept), and [`SetChosenIndex`](WriteOp::SetChosenIndex)
/// advances the contiguous commit index.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WriteOp {
    /// Persist a raised promised ballot (Phase 1). Monotonically non-decreasing.
    SetPromise(Ballot),
    /// Persist the `(ballot, entry)` accepted for `slot` (Phase 2). An
    /// upsert-by-slot: overwriting a stale lower-ballot accept for a now-chosen
    /// slot is load-bearing for restart safety (see [`crate::RawNode`]).
    AppendAccepted {
        /// The slot this accept is for.
        slot: Slot,
        /// The ballot the entry was accepted under.
        ballot: Ballot,
        /// The accepted entry.
        entry: Entry,
    },
    /// Advance the durable chosen index (commit index) to `slot`.
    SetChosenIndex(Slot),
}

/// Whether a [`crate::Ready`] batch must be flushed to stable storage (fsync'd)
/// **before** its messages are sent.
///
/// A promise-raise or an accepted-append is a safety-critical durable write: a
/// crash that loses it lets a node renege on a promise or vote, so it requires an
/// fsync. A batch that only advances the chosen index carries no new promise or
/// vote — the chosen value is already durable from the accept that preceded it —
/// so it may use a relaxed (non-fsync) write and be safely re-derived on restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MustSync {
    /// The batch raises a promise or appends an accept: fsync before sending.
    Sync,
    /// The batch only advances the chosen index: a relaxed write is safe.
    Relaxed,
}

impl WriteOp {
    /// Whether this op requires an fsync (a promise-raise or accepted-append).
    #[must_use]
    pub fn needs_sync(&self) -> bool {
        matches!(
            self,
            WriteOp::SetPromise(_) | WriteOp::AppendAccepted { .. }
        )
    }
}

/// The [`MustSync`] classification of a whole batch: [`MustSync::Sync`] if any op
/// needs an fsync, else [`MustSync::Relaxed`].
#[must_use]
pub fn classify(writes: &[WriteOp]) -> MustSync {
    if writes.iter().any(WriteOp::needs_sync) {
        MustSync::Sync
    } else {
        MustSync::Relaxed
    }
}
