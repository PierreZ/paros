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

use crate::types::{Ballot, Command, SessionEntry, Slot};

/// The two durable writes an [`Acceptor`](crate::acceptor::Acceptor) makes,
/// over whatever value that deployment's log carries.
///
/// They are the acceptor role's whole durable surface — a promise raise
/// (Phase 1) and an accepted record (Phase 2, an upsert-by-slot: a chosen
/// value overwrites any stale lower-ballot accept) — named once here so a
/// second deployment over another value type reuses the *ops* along with the
/// role, and its own batch type only has to say where they sit.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AcceptorWrite<V> {
    /// Persist a raised promised ballot (Phase 1). Monotonically
    /// non-decreasing.
    SetPromise(Ballot),
    /// Persist the `(ballot, value)` accepted for `slot` (Phase 2).
    AppendAccepted {
        /// The slot this accept is for.
        slot: Slot,
        /// The ballot the value was accepted under.
        ballot: Ballot,
        /// The accepted value (opaque to the acceptor).
        value: V,
    },
}

/// A single semantic durable write the driver must apply to stable storage,
/// **in order**, before sending the batch's messages.
///
/// The variants map one-to-one onto the durable state:
/// [`Acceptor`](WriteOp::Acceptor) carries the acceptor role's own two ops
/// over this deployment's value ([`AcceptorWrite`]),
/// [`SetChosenIndex`](WriteOp::SetChosenIndex)
/// advances the contiguous commit index, and [`Truncate`](WriteOp::Truncate)
/// drops the compacted log prefix.
///
/// # Which role emits which op
///
/// The classification is role-shaped, and stays that way: **every op that
/// [`needs_sync`](WriteOp::needs_sync) is emitted by
/// [`Acceptor`](crate::acceptor::Acceptor)** — the promise, the accepted
/// record, the truncation and the trim-point jump are all mutations of the
/// acceptor's own durable state, and each is emitted by the method that makes
/// it, never pushed beside the call by the wiring.
/// [`Replica`](crate::replica::Replica) emits exactly one op, the relaxed
/// [`SetChosenIndex`](WriteOp::SetChosenIndex), from its apply walk;
/// [`Proposer`](crate::proposer::Proposer) emits none at all — it holds no
/// durable state. That is the answer to "is persist-before-send an acceptor
/// property": it is, and a second deployment that reuses `Acceptor` gets the
/// whole durable surface with the role.
///
/// The one deployment without an `Acceptor` that still keeps a log, the
/// [`ReplicaNode`](crate::ReplicaNode) (#144), emits
/// [`Learned`](WriteOp::Learned) in place of the accepted record, and the
/// floor-moving ops its prefix needs ([`Truncate`](WriteOp::Truncate),
/// [`TrimmedTo`](WriteOp::TrimmedTo)) — never an
/// [`Acceptor`](WriteOp::Acceptor) op. Nothing it sends is predicated on
/// its writes (it sends only catch-up requests), so its fsync buys only
/// that the boot read-back finds every record below the chosen index.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WriteOp {
    /// A write of the node's acceptor role: the raised promise, or the
    /// `(ballot, command)` accepted for a slot. Overwriting a stale
    /// lower-ballot accept for a now-chosen slot is load-bearing for restart
    /// safety (see [`crate::ColocatedNode`]).
    Acceptor(AcceptorWrite<Command>),
    /// Persist the `(ballot, command)` **chosen** at `slot` on a node that
    /// is not an acceptor — a [`crate::ReplicaNode`] (#144). The same durable
    /// record shape as [`AcceptorWrite::AppendAccepted`] (a boot scan reads
    /// both back through [`crate::Storage::accepted`]), and never a vote: a
    /// replica answers no `Prepare` and sits in no configuration, so nothing
    /// ever counts this record toward a quorum. A distinct op so a driver and
    /// an audit can tell a learned record from an accepted one — an audit
    /// that folds durable accepts into a quorum oracle must not fold this.
    Learned {
        /// The chosen slot.
        slot: Slot,
        /// The ballot the command was chosen at.
        ballot: Ballot,
        /// The chosen command.
        command: Command,
    },
    /// Advance the durable chosen index (commit index) to `slot`.
    SetChosenIndex(Slot),
    /// Truncate the log below `first`, discarding the compacted prefix, and
    /// record `first` as the durable compaction floor. Decided by the log (a
    /// `Control::Truncate` the walk reached, see
    /// [`crate::ColocatedNode::compact`]); `first` always sits within the chosen
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
    /// Jump below the trim point (#186, [`crate::Message::TrimmedTo`]):
    /// record `point` as the durable compaction floor and at least
    /// `point - 1` as the durable chosen index, drop every record below
    /// `point`, and seal `sessions`. No bytes and no ballot: the promise does
    /// not move.
    TrimmedTo {
        /// The trim point: the first slot still retained.
        point: Slot,
        /// The serving peer's at-most-once session ledger for the slots below
        /// `point`, persisted as sealed records: their log records will never
        /// be walked here, so this is the only carrier of their
        /// `(client, seq) -> slot` facts (see [`WriteOp::Truncate::sealed`]).
        sessions: Vec<SessionEntry>,
    },
}

impl From<AcceptorWrite<Command>> for WriteOp {
    fn from(write: AcceptorWrite<Command>) -> Self {
        WriteOp::Acceptor(write)
    }
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
    /// Whether this op requires an fsync (a promise-raise, an accepted or
    /// learned record, a truncate, or a trim-point jump).
    #[must_use]
    pub fn needs_sync(&self) -> bool {
        matches!(
            self,
            WriteOp::Acceptor(_)
                | WriteOp::Learned { .. }
                | WriteOp::Truncate { .. }
                | WriteOp::TrimmedTo { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{AcceptorWrite, WriteOp};
    use crate::types::{Ballot, ClientId, ClientSeq, Command, Entry, NodeId, Slot, Value};

    fn ballot() -> Ballot {
        Ballot {
            round: 1,
            node: NodeId(0),
        }
    }

    fn append(slot: u64) -> WriteOp {
        WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
            slot: Slot(slot),
            ballot: ballot(),
            value: Command::User(Entry {
                client: ClientId(1),
                seq: ClientSeq(1),
                value: Value(vec![7]),
            }),
        })
    }

    fn promise() -> WriteOp {
        WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot()))
    }

    #[test]
    fn promise_and_accept_need_fsync_chosen_index_does_not() {
        assert!(promise().needs_sync());
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
}
