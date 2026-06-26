//! Durable state ([`HardState`]) and static node configuration ([`Config`]).

use std::collections::BTreeMap;

use crate::types::{Ballot, Entry, NodeId, Slot};

/// The must-be-durable triple of Multi-Paxos: the *exact* state that has to hit
/// stable storage **before any message predicated on it is sent**.
///
/// # Durability contract
///
/// An acceptor must persist a raised `max_promised_ballot` before replying
/// [`crate::Message::Promise`], and persist a new `accepted` entry before
/// replying [`crate::Message::Accepted`]. Sending either reply before the
/// corresponding field is durable violates Paxos safety: a crash could
/// "un-promise" or "un-accept", letting two different values be chosen for one
/// slot. The [`crate::Ready`] handshake enforces *persist `HardState` → then
/// send `messages`*.
///
/// Iteration order matters for determinism, so `accepted` is a [`BTreeMap`] —
/// never a `HashMap`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    /// Highest ballot this node has promised (Phase 1). Monotonically
    /// non-decreasing across the node's lifetime.
    pub max_promised_ballot: Ballot,
    /// Per-slot accepted proposal: the `(ballot, entry)` this node has accepted
    /// for each slot (Phase 2). The [`Entry`] carries the client `(id, seq)` so
    /// dedup state survives restart (rebuilt from this map).
    pub accepted: BTreeMap<Slot, (Ballot, Entry)>,
    /// Highest contiguous chosen slot (the commit index), or `None` when nothing
    /// is chosen yet. When `Some(s)`, every slot `<=` s is chosen and safe to
    /// apply. `Option` (rather than a `Slot(0)` sentinel) keeps genesis
    /// unambiguous: `None` is "nothing applied", `Some(Slot(0))` is "slot 0
    /// applied".
    pub chosen_index: Option<Slot>,
}

/// Static, immutable-for-this-instance configuration: who *I* am and who my
/// peers are.
///
/// Cluster membership is fixed at construction in Stage 0 — no reconfiguration
/// or joint consensus yet (that arrives with the Matchmaker milestone).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// This node's identity.
    pub id: NodeId,
    /// The full cluster membership, *including* `id`. A sorted, deduplicated
    /// `Vec` keeps iteration deterministic without a map.
    pub peers: Vec<NodeId>,
}
