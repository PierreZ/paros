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

use crate::types::{Ballot, Command, SessionEntry, Slot, Value};

/// A single semantic durable write the driver must apply to stable storage,
/// **in order**, before sending the batch's messages.
///
/// The variants map one-to-one onto the durable state:
/// [`SetPromise`](WriteOp::SetPromise) raises the promised ballot (Phase 1),
/// [`AppendAccepted`](WriteOp::AppendAccepted) records the `(ballot, entry)` a
/// slot has accepted (Phase 2, an upsert-by-slot — a chosen value overwrites any
/// stale lower-ballot accept), [`SetChosenIndex`](WriteOp::SetChosenIndex)
/// advances the contiguous commit index, and [`Truncate`](WriteOp::Truncate)
/// drops the compacted log prefix.
///
/// # Which role emits which op
///
/// The classification is role-shaped, and stays that way: **every op that
/// [`needs_sync`](WriteOp::needs_sync) is emitted by
/// [`Acceptor`](crate::acceptor::Acceptor)** — the promise, the accepted
/// record, the truncation and the snapshot install are all mutations of the
/// acceptor's own durable state, and each is emitted by the method that makes
/// it, never pushed beside the call by the wiring.
/// [`Replica`](crate::replica::Replica) emits exactly one op, the relaxed
/// [`SetChosenIndex`](WriteOp::SetChosenIndex), from its apply walk;
/// [`Proposer`](crate::proposer::Proposer) emits none at all — it holds no
/// durable state. That is the answer to "is persist-before-send an acceptor
/// property": it is, and a second deployment that reuses `Acceptor` gets the
/// whole durable surface with the role.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WriteOp {
    /// Persist a raised promised ballot (Phase 1). Monotonically non-decreasing.
    SetPromise(Ballot),
    /// Persist the `(ballot, command)` accepted for `slot` (Phase 2). An
    /// upsert-by-slot: overwriting a stale lower-ballot accept for a now-chosen
    /// slot is load-bearing for restart safety (see [`crate::RawNode`]).
    AppendAccepted {
        /// The slot this accept is for.
        slot: Slot,
        /// The ballot the command was accepted under.
        ballot: Ballot,
        /// The accepted command (an opaque client entry or a control command).
        command: Command,
    },
    /// Advance the durable chosen index (commit index) to `slot`.
    SetChosenIndex(Slot),
    /// Truncate the log below `first`, discarding the compacted prefix, and
    /// record `first` as the durable compaction floor. Application-driven (see
    /// [`crate::RawNode::compact`]); `first` always sits within the chosen
    /// prefix, so nothing undecided is dropped.
    Truncate {
        /// The first slot still retained. Everything below it is dropped.
        first: Slot,
        /// The at-most-once ledger records whose slots this truncation drops,
        /// **sealed** durably in the same flush: the ledger is rebuilt from the
        /// retained log on boot, so without sealing, a restart after truncation
        /// would forget these `(client, seq) -> slot` facts and a later mandatory
        /// P2c re-proposal of the same identity would apply for real on the
        /// restarted node while every other node suppresses it — state
        /// divergence, strictly worse than the double-apply (#94).
        sealed: Vec<SessionEntry>,
    },
    /// Install an opaque application snapshot: record `chosen_index` as the
    /// durable commit index, `chosen_index + 1` as the durable compaction floor,
    /// `ballot` as (at least) the durable promise, and persist the opaque
    /// `snapshot` bytes (so a restart boots from them and the node can serve them
    /// onward). Produced only by [`crate::Message::InstallSnapshot`]; the bytes are
    /// never interpreted by the core.
    InstallSnapshot {
        /// The commit index the snapshot brings the node up to.
        chosen_index: Slot,
        /// The ballot adopted with the snapshot (the promise takes its max).
        ballot: Ballot,
        /// Opaque application snapshot bytes at `chosen_index`.
        snapshot: Value,
        /// The serving peer's at-most-once session ledger, persisted as sealed
        /// records beside the opaque bytes: the folded prefix's log records will
        /// never be walked here, so this is the only carrier of its
        /// `(client, seq) -> slot` facts (see [`WriteOp::Truncate::sealed`]).
        sessions: Vec<SessionEntry>,
    },
}

/// Whether a [`crate::Ready`] batch must be flushed to stable storage (fsync'd)
/// **before** its messages are sent.
///
/// A promise-raise or an accepted-append is a safety-critical durable write: a
/// crash that loses it lets a node renege on a promise or vote, so it requires an
/// fsync. A batch that only advances the chosen index carries no new promise or
/// vote — the chosen value is already durable from the accept that preceded it —
/// so it may use a relaxed (non-fsync) write and be safely re-derived on restart.
///
/// A truncate is also fsync'd: it must land in the same flush as (and after) any
/// chosen-index advance in the batch, else a crash could leave a durable floor
/// above the durable chosen index (an unfillable hole below the node's own floor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MustSync {
    /// The batch raises a promise or appends an accept: fsync before sending.
    Sync,
    /// The batch only advances the chosen index: a relaxed write is safe.
    Relaxed,
}

impl WriteOp {
    /// Whether this op requires an fsync (a promise-raise, accepted-append, or
    /// truncate).
    #[must_use]
    pub fn needs_sync(&self) -> bool {
        matches!(
            self,
            WriteOp::SetPromise(_)
                | WriteOp::AppendAccepted { .. }
                | WriteOp::Truncate { .. }
                | WriteOp::InstallSnapshot { .. }
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

#[cfg(test)]
mod tests {
    use super::{MustSync, WriteOp, classify};
    use crate::types::{Ballot, ClientId, ClientSeq, Command, Entry, NodeId, Slot, Value};

    fn ballot() -> Ballot {
        Ballot {
            round: 1,
            node: NodeId(0),
        }
    }

    fn append(slot: u64) -> WriteOp {
        WriteOp::AppendAccepted {
            slot: Slot(slot),
            ballot: ballot(),
            command: Command::User(Entry {
                client: ClientId(1),
                seq: ClientSeq(1),
                value: Value(vec![7]),
            }),
        }
    }

    #[test]
    fn promise_and_accept_need_fsync_chosen_index_does_not() {
        assert!(WriteOp::SetPromise(ballot()).needs_sync());
        assert!(append(0).needs_sync());
        assert!(!WriteOp::SetChosenIndex(Slot(0)).needs_sync());
        assert!(
            WriteOp::Truncate {
                first: Slot(1),
                sealed: vec![]
            }
            .needs_sync()
        );
    }

    #[test]
    fn a_batch_is_sync_iff_it_raises_a_promise_or_appends_an_accept() {
        // Promise-raise or accepted-append ⇒ fsync required.
        assert_eq!(classify(&[WriteOp::SetPromise(ballot())]), MustSync::Sync);
        assert_eq!(classify(&[append(0)]), MustSync::Sync);
        // Even mixed with a chosen-index advance, the batch is Sync.
        assert_eq!(
            classify(&[append(0), WriteOp::SetChosenIndex(Slot(0))]),
            MustSync::Sync
        );
        // A chosen-index-only advance may use a relaxed write.
        assert_eq!(
            classify(&[WriteOp::SetChosenIndex(Slot(0))]),
            MustSync::Relaxed
        );
        // A truncate is fsync'd, on its own or mixed with a chosen-index advance.
        assert_eq!(
            classify(&[WriteOp::Truncate {
                first: Slot(1),
                sealed: vec![]
            }]),
            MustSync::Sync
        );
        assert_eq!(
            classify(&[
                WriteOp::SetChosenIndex(Slot(3)),
                WriteOp::Truncate {
                    first: Slot(1),
                    sealed: vec![]
                }
            ]),
            MustSync::Sync
        );
        // An empty batch persists nothing; relaxed is the safe default.
        assert_eq!(classify(&[]), MustSync::Relaxed);
    }
}
