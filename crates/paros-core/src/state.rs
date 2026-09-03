//! Durable state ([`HardState`]) and static node configuration ([`Config`]).

use crate::matchmaker::MatchmakerId;
pub use crate::membership::QuorumSystem;
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

/// Static, immutable-for-this-instance configuration: who *I* am, the
/// **bootstrap** acceptor configuration, the pool of nodes that may ever be an
/// acceptor, and the matchmaker set (empty for plain Multi-Paxos).
///
/// Two deployments live in this one struct, told apart by `matchmakers`:
///
/// - **Plain Multi-Paxos** (`matchmakers` empty — the default, and permanent:
///   see AGENTS.md, *Plain Multi-Paxos is first-class*): `peers` is the fixed
///   membership for the node's whole life, `nodes` is `peers` (or empty, which
///   means the same), and no matchmaking phase, matchmaker message, or extra
///   round trip ever exists.
/// - **Matchmaker Paxos** (`matchmakers` non-empty): `peers` is only the
///   configuration in force *before any ballot was registered*; every ballot
///   binds its own acceptor configuration through the matchmakers, drawn from
///   `nodes`, and the node tracks the configuration of the highest ballot it
///   has seen (`RawNode::acceptors`). A node may be in `nodes` without being
///   in `peers` — a spare waiting to be added — and may be in `peers` and
///   later removed; either way it stays addressable, answers Phase 1 for the
///   ballots it took part in, and learns the chosen log as a replica.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// This node's identity.
    pub id: NodeId,
    /// The bootstrap acceptor configuration: the full membership before any
    /// reconfiguration. Sorted and deduplicated (a `Vec` keeps iteration
    /// deterministic without a map). On a plain deployment it *includes*
    /// `id`; on a matchmaker deployment a spare's `id` may sit outside it.
    pub peers: Vec<NodeId>,
    /// The quorum system election and decide consult. A value, so config-per-round
    /// reconfiguration is a data change, not a logic change.
    pub quorum_system: QuorumSystem,
    /// Every node that may ever be an acceptor — the addressable pool a
    /// reconfiguration draws from. Sorted and deduplicated, a superset of
    /// `peers` that includes `id`. Empty means "exactly `peers`", the plain
    /// deployment's shape.
    pub nodes: Vec<NodeId>,
    /// The **bootstrap** matchmaker set (generation 0). **Empty is plain
    /// Multi-Paxos**: no matchmaking phase runs and no reconfiguration is
    /// honored. Non-empty turns every campaign into matchmaking followed by a
    /// cross-configuration Phase 1. A matchmaker-set reconfiguration (#125)
    /// moves the node's *volatile* belief (`RawNode::matchmaker_set`) to a
    /// later generation; this stays the set a fresh incarnation asks first.
    pub matchmakers: Vec<MatchmakerId>,
    /// Every matchmaker that may ever be in a matchmaker set — the pool a
    /// matchmaker-set reconfiguration draws from, a superset of
    /// `matchmakers`. Empty means "exactly `matchmakers`".
    pub matchmaker_pool: Vec<MatchmakerId>,
}

impl Config {
    /// The addressable pool: `nodes`, or `peers` when `nodes` is empty.
    #[must_use]
    pub fn pool(&self) -> &[NodeId] {
        if self.nodes.is_empty() {
            &self.peers
        } else {
            &self.nodes
        }
    }

    /// Whether this deployment names matchmakers (the opt-in that enables
    /// matchmaking and reconfiguration).
    #[must_use]
    pub fn has_matchmakers(&self) -> bool {
        !self.matchmakers.is_empty()
    }

    /// The matchmaker pool: `matchmaker_pool`, or `matchmakers` when empty.
    #[must_use]
    pub fn matchmaker_pool(&self) -> &[MatchmakerId] {
        if self.matchmaker_pool.is_empty() {
            &self.matchmakers
        } else {
            &self.matchmaker_pool
        }
    }
}
