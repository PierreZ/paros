//! The per-seed **deployment / role map**: which process of the topology plays
//! which role.
//!
//! Membership is never "every process in the topology". Moonpool decides how
//! many processes a seed has, per **process group** — one group per role,
//! each with its own per-seed count draw and its own IP range (moonpool #197):
//! the [`ACCEPTOR_GROUP`] holds the paros nodes (`NodeId(rank)` among them, in
//! IP order) and the [`MATCHMAKER_GROUP`] holds the matchmakers
//! (`MatchmakerId(rank)`, a process that is *not* an acceptor), and the
//! [`PROXY_GROUP`] holds the proxy leaders (`ProxyId(rank)`, #142 — neither
//! an acceptor nor a replica). The acceptor list is the pool every node
//! derives its `Config` from and every client proposes to; the matchmaker
//! list is what a campaigning leader registers with; the proxy list is what
//! a settled leader delegates Phase 2 to, and its length is the `Config`'s
//! `proxy_count`. The [`REPLICA_GROUP`] holds the replica tier (#144): a
//! learner that applies the chosen log and never votes, `ReplicaId(rank)`
//! for the reply it owns and [`replica_node_id`] on the wire — an id outside
//! the pool, since a replica is never a member of anything.
//!
//! The map is a pure function of the seed's topology, so every process and
//! every workload derives the *same* map without coordination, a recipe
//! replays it exactly, and an attrition restart never re-rolls a node's role.
//!
//! **The default is the plain Multi-Paxos deployment** (AGENTS.md, *Plain
//! Multi-Paxos is first-class*): a seed whose matchmaker group drew zero
//! members deploys no matchmakers, and every campaign goes straight to
//! `Prepare`; a seed whose proxy group drew zero members delegates nothing
//! and runs every Phase 2 colocated. The main campaign draws both counts per
//! seed ([`crate::MATCHMAKER_POOL_RANGE`], [`crate::PROXY_POOL_RANGE`]); the
//! scripted corpus registers the [`ACCEPTOR_GROUP`] and neither other group,
//! which reads here **exactly** like a main-campaign seed whose other groups
//! drew zero members — byte-identical, one code path, no corpus special
//! case.

use std::net::IpAddr;

use moonpool_sim::{WorkloadTopology, assert_always};
use paros::{MatchmakerId, NodeId, ProxyId, ReplicaId};

/// The process group of the paros nodes (`NodeProcess::name`).
pub(crate) const ACCEPTOR_GROUP: &str = "paros-node";
/// The process group of the matchmakers (`MatchmakerProcess::name`).
pub(crate) const MATCHMAKER_GROUP: &str = "paros-matchmaker";
/// The process group of the proxy leaders (`ProxyProcess::name`, #142).
pub(crate) const PROXY_GROUP: &str = "paros-proxy";
/// The process group of the replicas (`ReplicaProcess::name`, #144).
pub(crate) const REPLICA_GROUP: &str = "paros-replica";
/// The process group of the joiners (`JoinerProcess::name`, #189): nodes
/// outside the genesis pool that join it at runtime through the registry.
pub(crate) const JOINER_GROUP: &str = "paros-joiner";

/// Where the joiners' `NodeId`s start: above any genesis pool the campaign
/// draws and below the replicas', so a joiner's identity collides with
/// neither and reads apart in a trace.
const JOINER_ID_BASE: u64 = 100;

/// The `NodeId` the joiner of rank `rank` joins the pool as.
pub(crate) fn joiner_node_id(rank: usize) -> NodeId {
    NodeId(JOINER_ID_BASE + rank as u64)
}

/// Where the replicas' `NodeId`s start: far above any pool the campaign
/// draws (`PROCESS_POOL_RANGE` tops out at six), so a replica's wire
/// identity can never collide with an acceptor's and reads apart in a trace.
const REPLICA_ID_BASE: u64 = 1000;

/// The `NodeId` the replica of rank `rank` speaks as on the wire: outside the
/// node pool by construction (a replica is in no configuration).
pub(crate) fn replica_node_id(rank: ReplicaId) -> NodeId {
    NodeId(REPLICA_ID_BASE + rank.0)
}

/// One process's role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    /// A paros node, ranked among the acceptors.
    Acceptor(NodeId),
    /// A matchmaker, ranked among the matchmakers — not an acceptor.
    Matchmaker(MatchmakerId),
    /// A proxy leader, ranked among the proxies — neither an acceptor nor a
    /// replica (#142).
    Proxy(ProxyId),
    /// A replica, ranked among the replicas — a learner that is not an
    /// acceptor (#144).
    Replica(ReplicaId),
    /// A joiner (#189), speaking as [`joiner_node_id`] of its rank: outside
    /// the genesis pool until the registry admits it.
    Joiner(NodeId),
}

/// The seed's deployment: sorted acceptor IPs (`NodeId(i)` ↔ `acceptors[i]`),
/// sorted matchmaker IPs (`MatchmakerId(i)` ↔ `matchmakers[i]`), sorted
/// proxy IPs (`ProxyId(i)` ↔ `proxies[i]`) and sorted replica IPs
/// (`ReplicaId(i)` ↔ `replicas[i]`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Deployment {
    acceptors: Vec<String>,
    matchmakers: Vec<String>,
    proxies: Vec<String>,
    replicas: Vec<String>,
    joiners: Vec<String>,
}

impl Deployment {
    /// Build the map from four IP lists (any order, duplicates allowed).
    fn from_groups(
        mut acceptors: Vec<String>,
        mut matchmakers: Vec<String>,
        mut proxies: Vec<String>,
        mut replicas: Vec<String>,
    ) -> Self {
        sort_ips(&mut acceptors);
        sort_ips(&mut matchmakers);
        sort_ips(&mut proxies);
        sort_ips(&mut replicas);
        Self {
            acceptors,
            matchmakers,
            proxies,
            replicas,
            joiners: Vec::new(),
        }
    }

    /// The same map with `joiners` (#189), sorted.
    fn with_joiners(mut self, mut joiners: Vec<String>) -> Self {
        sort_ips(&mut joiners);
        self.joiners = joiners;
        self
    }

    /// The role of `ip`, or `None` for an IP outside the pool (a workload).
    pub(crate) fn role_of(&self, ip: &str) -> Option<Role> {
        if let Some(rank) = self.acceptors.iter().position(|a| a == ip) {
            return Some(Role::Acceptor(NodeId(rank as u64)));
        }
        if let Some(rank) = self.matchmakers.iter().position(|m| m == ip) {
            return Some(Role::Matchmaker(MatchmakerId(rank as u64)));
        }
        if let Some(rank) = self.proxies.iter().position(|p| p == ip) {
            return Some(Role::Proxy(ProxyId(rank as u64)));
        }
        if let Some(rank) = self.replicas.iter().position(|r| r == ip) {
            return Some(Role::Replica(ReplicaId(rank as u64)));
        }
        self.joiners
            .iter()
            .position(|j| j == ip)
            .map(|rank| Role::Joiner(joiner_node_id(rank)))
    }

    /// The joiners (#189), in rank order: each joins as
    /// [`joiner_node_id`] of its rank.
    pub(crate) fn joiners(&self) -> &[String] {
        &self.joiners
    }

    /// The acceptor pool, in `NodeId` order.
    pub(crate) fn acceptors(&self) -> &[String] {
        &self.acceptors
    }

    /// The matchmaker set, in `MatchmakerId` order (empty on a plain seed).
    pub(crate) fn matchmakers(&self) -> &[String] {
        &self.matchmakers
    }

    /// The proxy leaders, in `ProxyId` order (empty on a seed without
    /// proxies, whose every Phase 2 is colocated).
    pub(crate) fn proxies(&self) -> &[String] {
        &self.proxies
    }

    /// The replicas, in `ReplicaId` order (empty on a seed without
    /// replicas, whose learner traffic reaches the pool alone); each speaks
    /// as [`replica_node_id`] on the wire.
    pub(crate) fn replicas(&self) -> &[String] {
        &self.replicas
    }

    /// How many replicas the seed deploys: every node's `replica_count`.
    pub(crate) fn replica_count(&self) -> usize {
        self.replicas.len()
    }
}

fn sort_ips(ips: &mut Vec<String>) {
    ips.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
    ips.dedup();
}

/// The seed's deployment, read off the topology's process groups. Every
/// builder registers its nodes as the [`ACCEPTOR_GROUP`] — a process is named
/// by [`NodeProcess`](crate::process::NodeProcess)'s
/// [`Process::name`](moonpool_sim::Process::name), corpus and main campaign
/// alike — so a topology with no matchmaker group is simply a deployment whose
/// matchmaker list is empty: the plain one.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn deployment(topology: &WorkloadTopology) -> Deployment {
    let acceptors = topology.ips_in_group(ACCEPTOR_GROUP);
    let matchmakers = topology.ips_in_group(MATCHMAKER_GROUP);
    let proxies = topology.ips_in_group(PROXY_GROUP);
    let replicas = topology.ips_in_group(REPLICA_GROUP);
    let joiners = topology.ips_in_group(JOINER_GROUP);
    let map =
        Deployment::from_groups(acceptors, matchmakers, proxies, replicas).with_joiners(joiners);
    assert_always!(
        !map.acceptors.is_empty(),
        "a deployment names at least one acceptor",
        { "matchmakers" => map.matchmakers.len(), "proxies" => map.proxies.len() }
    );
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(n: usize) -> Vec<String> {
        (1..=n).map(|i| format!("10.0.1.{i}")).collect()
    }

    /// The mechanism, not a seed: acceptors rank in IP order whatever order
    /// the group listed them in, and matchmakers rank among themselves.
    #[test]
    fn a_deployment_ranks_each_group_in_ip_order() {
        let mut shuffled = pool(4);
        shuffled.reverse();
        let map = Deployment::from_groups(
            shuffled,
            vec!["10.0.2.2".to_string(), "10.0.2.1".to_string()],
            vec!["10.0.3.2".to_string(), "10.0.3.1".to_string()],
            vec!["10.0.4.2".to_string(), "10.0.4.1".to_string()],
        );
        assert_eq!(map.acceptors(), pool(4).as_slice());
        assert_eq!(map.matchmakers().len(), 2);
        assert_eq!(map.proxies().len(), 2);
        assert_eq!(map.role_of("10.0.1.3"), Some(Role::Acceptor(NodeId(2))));
        assert_eq!(
            map.role_of("10.0.2.2"),
            Some(Role::Matchmaker(MatchmakerId(1)))
        );
        assert_eq!(map.role_of("10.0.3.1"), Some(Role::Proxy(ProxyId(0))));
        assert_eq!(map.role_of("10.0.4.2"), Some(Role::Replica(ReplicaId(1))));
        assert_eq!(replica_node_id(ReplicaId(0)), NodeId(1000));
        assert_eq!(map.role_of("10.9.9.9"), None);
    }

    /// A seed whose matchmaker and proxy groups drew nothing is the plain
    /// deployment.
    #[test]
    fn an_empty_matchmaker_group_is_the_plain_deployment() {
        let map = Deployment::from_groups(pool(3), Vec::new(), Vec::new(), Vec::new());
        assert!(map.matchmakers().is_empty());
        assert!(map.proxies().is_empty());
        assert_eq!(map.replica_count(), 0);
        assert_eq!(map.acceptors().len(), 3);
    }
}
