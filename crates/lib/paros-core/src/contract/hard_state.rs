//! The durable scalars of Multi-Paxos ([`HardState`]).

use crate::types::{Ballot, Slot};

/// The small, persisted-whole durable scalars of Multi-Paxos: the state that has
/// to hit stable storage **before any message predicated on it is sent**.
///
/// The per-slot accepted log is *not* here — it is persisted separately, one
/// record at a time, through the semantic write ops a [`crate::Ready`] surfaces
/// ([`crate::WriteOp`]). This mirrors etcd-raft's `HardState`-vs-`entries` shape:
/// these scalars are tiny and rewritten whole, while the log grows and is
/// appended per record (so a mutation no longer clones the whole log).
///
/// # Durability contract
///
/// An acceptor must persist a raised `max_promised_ballot` before replying
/// [`crate::Message::Promise`], and persist a new accepted entry (a
/// [`crate::AcceptorWrite::AppendAccepted`]) before replying
/// [`crate::Message::Accepted`]. Sending either reply before the corresponding
/// write is durable violates Paxos safety: a crash could "un-promise" or
/// "un-accept", letting two different values be chosen for one slot. The
/// [`crate::Ready`] handshake enforces *persist writes → then send messages*.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct HardState {
    /// Highest ballot this node has promised (Phase 1). Monotonically
    /// non-decreasing across the node's lifetime.
    pub max_promised_ballot: Ballot,
    /// Highest contiguous chosen slot (the commit index), or `None` when nothing
    /// is chosen yet. When `Some(s)`, every slot `<=` s is chosen and safe to
    /// apply. `Option` (rather than a `Slot(0)` sentinel) keeps genesis
    /// unambiguous: `None` is "nothing applied", `Some(Slot(0))` is "slot 0
    /// applied".
    pub chosen_index: Option<Slot>,
}
