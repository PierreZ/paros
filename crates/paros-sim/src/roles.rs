//! The per-seed **deployment / role map**: which process of the topology plays
//! which role.
//!
//! Membership is never "every process in the topology". Moonpool decides how
//! many processes a seed has, per **process group** — one group per role,
//! each with its own per-seed count draw and its own IP range (moonpool #197):
//! the [`ACCEPTOR_GROUP`] holds the paros nodes (`NodeId(rank)` among them, in
//! IP order) and the [`MATCHMAKER_GROUP`] holds the matchmakers
//! (`MatchmakerId(rank)`, a process that is *not* an acceptor). The acceptor
//! list is the pool every node derives its `Config` from and every client
//! proposes to; the matchmaker list is what a campaigning leader registers
//! with.
//!
//! The map is a pure function of the seed's topology, so every process and
//! every workload derives the *same* map without coordination, a recipe
//! replays it exactly, and an attrition restart never re-rolls a node's role.
//!
//! **The default is the plain Multi-Paxos deployment** (AGENTS.md, *Plain
//! Multi-Paxos is first-class*): a seed whose matchmaker group drew zero
//! members deploys no matchmakers, and every campaign goes straight to
//! `Prepare`. The main campaign draws the matchmaker count per seed
//! ([`crate::MATCHMAKER_POOL_RANGE`]); the scripted corpus registers the
//! [`ACCEPTOR_GROUP`] and no matchmaker group, which reads here **exactly**
//! like a main-campaign seed whose matchmaker group drew zero members —
//! byte-identical, one code path, no corpus special case.

use std::net::IpAddr;

use moonpool_sim::{WorkloadTopology, assert_always};
use paros::{MatchmakerId, NodeId};

/// The process group of the paros nodes (`NodeProcess::name`).
pub(crate) const ACCEPTOR_GROUP: &str = "paros-node";
/// The process group of the matchmakers (`MatchmakerProcess::name`).
pub(crate) const MATCHMAKER_GROUP: &str = "paros-matchmaker";

/// One process's role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    /// A paros node, ranked among the acceptors.
    Acceptor(NodeId),
    /// A matchmaker, ranked among the matchmakers — not an acceptor.
    Matchmaker(MatchmakerId),
}

/// The seed's deployment: sorted acceptor IPs (`NodeId(i)` ↔ `acceptors[i]`)
/// and sorted matchmaker IPs (`MatchmakerId(i)` ↔ `matchmakers[i]`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Deployment {
    acceptors: Vec<String>,
    matchmakers: Vec<String>,
}

impl Deployment {
    /// Build the map from two IP lists (any order, duplicates allowed).
    fn from_groups(mut acceptors: Vec<String>, mut matchmakers: Vec<String>) -> Self {
        sort_ips(&mut acceptors);
        sort_ips(&mut matchmakers);
        Self {
            acceptors,
            matchmakers,
        }
    }

    /// The role of `ip`, or `None` for an IP outside the pool (a workload).
    pub(crate) fn role_of(&self, ip: &str) -> Option<Role> {
        if let Some(rank) = self.acceptors.iter().position(|a| a == ip) {
            return Some(Role::Acceptor(NodeId(rank as u64)));
        }
        self.matchmakers
            .iter()
            .position(|m| m == ip)
            .map(|rank| Role::Matchmaker(MatchmakerId(rank as u64)))
    }

    /// The acceptor pool, in `NodeId` order.
    pub(crate) fn acceptors(&self) -> &[String] {
        &self.acceptors
    }

    /// The matchmaker set, in `MatchmakerId` order (empty on a plain seed).
    pub(crate) fn matchmakers(&self) -> &[String] {
        &self.matchmakers
    }
}

fn sort_ips(ips: &mut Vec<String>) {
    ips.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
    ips.dedup();
}

/// The seed's deployment, read off the topology's process groups. Every
/// builder registers its nodes as the [`ACCEPTOR_GROUP`] — a process is named
/// by [`crate::process::NodeProcess::name`], corpus and main campaign alike —
/// so a topology with no matchmaker group is simply a deployment whose
/// matchmaker list is empty: the plain one.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn deployment(topology: &WorkloadTopology) -> Deployment {
    let acceptors = topology.ips_in_group(ACCEPTOR_GROUP);
    let matchmakers = topology.ips_in_group(MATCHMAKER_GROUP);
    let map = Deployment::from_groups(acceptors, matchmakers);
    assert_always!(
        !map.acceptors.is_empty(),
        "a deployment names at least one acceptor",
        { "matchmakers" => map.matchmakers.len() }
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
        );
        assert_eq!(map.acceptors(), pool(4).as_slice());
        assert_eq!(map.matchmakers().len(), 2);
        assert_eq!(map.role_of("10.0.1.3"), Some(Role::Acceptor(NodeId(2))));
        assert_eq!(
            map.role_of("10.0.2.2"),
            Some(Role::Matchmaker(MatchmakerId(1)))
        );
        assert_eq!(map.role_of("10.9.9.9"), None);
    }

    /// A seed whose matchmaker group drew nothing is the plain deployment.
    #[test]
    fn an_empty_matchmaker_group_is_the_plain_deployment() {
        let map = Deployment::from_groups(pool(3), Vec::new());
        assert!(map.matchmakers().is_empty());
        assert_eq!(map.acceptors().len(), 3);
    }
}
