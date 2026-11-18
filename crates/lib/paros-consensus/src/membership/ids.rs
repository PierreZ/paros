use super::quorum_system::{rank_is_in, slot_rank};
#[cfg(doc)]
use crate::types::NodeId;
use crate::types::{FNV_OFFSET, Fingerprint, Slot, fnv1a};

/// Stable identity of a matchmaker within the matchmaker pool. A distinct
/// namespace from [`NodeId`]: a matchmaker is not an acceptor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerId(pub u64);

/// The most proxy leaders, or replicas, one configuration may name (#754
/// (counts off the format marker)): the cap an acceptor checks a proxy's
/// rank against, since it may not know the count of the configuration the
/// proxy serves yet.
pub const MAX_ROLE_COUNT: usize = 64;

/// Stable identity of a **proxy leader** within the deployment's proxy set
/// (#142, Compartmentalized Paxos §3.1). A distinct namespace from
/// [`NodeId`], exactly as [`MatchmakerId`] is and for the same reason: a
/// proxy is not an acceptor. It is never in `Config::nodes`, never drawn by a
/// reconfiguration, never counted by retirement, GC, the pool guards or a
/// dead-node budget; an operator scales proxies by starting processes and
/// changing the count, never by changing the acceptor set.
///
/// The core holds only the proxy **count** of the configuration bound to
/// the ballot (`AcceptorConfig::proxy_count`, #754) and
/// derives a slot's proxy as `ProxyId(slot % proxy_count)`
/// ([`ProxyId::of`]): a pure function of the slot, so a handoff successor
/// re-delegating an inherited round and a restarted leader's re-send name
/// the same proxy without carrying it. The driver's deployment map resolves
/// the id to a process, as it resolves `Learners` from the pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProxyId(pub u64);

impl ProxyId {
    /// The proxy `slot`'s Phase 2 is delegated to under a deployment of
    /// `proxy_count` proxies: `slot % proxy_count`. `None` when the count is
    /// zero — the plain deployment, whose Phase 2 stays colocated.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn of(slot: Slot, proxy_count: usize) -> Option<Self> {
        let proxy = slot_rank(slot, proxy_count).map(Self);
        assert!(
            proxy.is_some() == (proxy_count > 0),
            "a slot has a proxy iff proxies exist"
        );
        assert!(
            proxy.is_none_or(|p| p.is_in(proxy_count)),
            "a slot's proxy is deployed"
        );
        proxy
    }

    /// Whether this id names a proxy of a deployment of `proxy_count`.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_in(self, proxy_count: usize) -> bool {
        let deployed = rank_is_in(self.0, proxy_count);
        if deployed {
            assert!(
                proxy_count > 0,
                "a deployed proxy belongs to a proxy deployment"
            );
        }
        deployed
    }
}

/// A **replica** of the deployment (#144, Compartmentalized Paxos §3.3):
/// a rank among the [`crate::ReplicaNode`]s that learn and apply the chosen
/// log without voting. Its own namespace, like [`ProxyId`]: a replica is
/// never an acceptor, never in a configuration and never counted by a
/// quorum. The id names **who owns a slot's reply**, nothing else — on the
/// wire a replica speaks as the [`NodeId`] its deployment
/// map gave it, because the learner messages it sends and receives
/// (`CatchUpRequest`, `CatchUpResponse`, `TrimmedTo`) address nodes.
///
/// The core holds only the count of the configuration bound to the ballot
/// (`AcceptorConfig::replica_count`, #754) and derives a
/// slot's owner as `ReplicaId(slot % replica_count)` ([`ReplicaId::of`]):
/// a pure function of the slot, so every replica, every node and a future
/// client library agree on the owner without carrying it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReplicaId(pub u64);

impl ReplicaId {
    /// The replica that owns `slot`'s reply under a deployment of
    /// `replica_count` replicas: `slot % replica_count`. `None` when the
    /// count is zero — the plain deployment, where the node asked replies.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn of(slot: Slot, replica_count: usize) -> Option<Self> {
        let replica = slot_rank(slot, replica_count).map(Self);
        assert!(
            replica.is_some() == (replica_count > 0),
            "a slot has a replica iff replicas exist"
        );
        assert!(
            replica.is_none_or(|r| r.is_in(replica_count)),
            "a slot's replica is deployed"
        );
        replica
    }

    /// Whether this id names a replica of a deployment of `replica_count`.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_in(self, replica_count: usize) -> bool {
        let deployed = rank_is_in(self.0, replica_count);
        if deployed {
            assert!(
                replica_count > 0,
                "a deployed replica belongs to a replica tier"
            );
        }
        deployed
    }
}

impl Fingerprint for Vec<MatchmakerId> {
    /// The identity a matchmaker set carries through Phase 2: an FNV-1a fold
    /// over the members, in their sorted order. The value a decree chooses is
    /// small and always normalized, so its identity is its content.
    fn fingerprint(&self) -> u64 {
        self.iter().fold(FNV_OFFSET, |hash, member| {
            fnv1a(hash, &member.0.to_le_bytes())
        })
    }
}
