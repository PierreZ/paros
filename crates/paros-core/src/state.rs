//! Durable state ([`HardState`]) and static node configuration ([`Config`]).

use crate::types::{Ballot, ConfigId, NodeId, Slot};

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
/// [`crate::WriteOp::AppendAccepted`]) before replying
/// [`crate::Message::Accepted`]. Sending either reply before the corresponding
/// write is durable violates Paxos safety: a crash could "un-promise" or
/// "un-accept", letting two different values be chosen for one slot. The
/// [`crate::Ready`] handshake enforces *persist writes → then send messages*.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct HardState {
    /// Durable identity of the cluster configuration this node belongs to.
    #[cfg_attr(feature = "serde", serde(default))]
    pub config_id: ConfigId,
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

/// The quorum system a configuration uses: which sets of acceptors count as a
/// quorum for Phase 1 (election) and Phase 2 (decide).
///
/// Carried as a *value* in [`Config`] from the start (even though there is only
/// ever one variant today) so that Matchmaker reconfiguration (Stage 9) is a
/// *data* change — a different quorum system per round — rather than a rewrite of
/// the election/decide logic. Paxos safety rests on every Phase-1 quorum
/// intersecting every Phase-2 quorum; a simple majority satisfies that trivially.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum QuorumSystem {
    /// A simple majority of the membership: any `⌊n/2⌋ + 1` acceptors. Every two
    /// majorities intersect, so Phase-1 and Phase-2 quorums always share an
    /// acceptor.
    #[default]
    Majority,
}

impl QuorumSystem {
    /// The number of acceptors that form a quorum over a membership of `members`.
    #[must_use]
    pub fn quorum_size(self, members: usize) -> usize {
        match self {
            QuorumSystem::Majority => members / 2 + 1,
        }
    }
}

/// Static, immutable-for-this-instance configuration: who *I* am, who my peers
/// are, and the quorum system in force.
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
    /// The quorum system election and decide consult. A value, so config-per-round
    /// reconfiguration is later a data change, not a logic change.
    pub quorum_system: QuorumSystem,
}
