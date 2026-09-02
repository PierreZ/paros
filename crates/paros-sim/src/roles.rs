//! The per-seed **deployment / role map**: which process of the topology pool
//! plays which role.
//!
//! Membership is never "every process in the topology". Moonpool decides how
//! many processes a seed has (the pool, `crate::PROCESS_POOL_RANGE`); this map
//! decides what each one *is*: an **acceptor** (a paros node, `NodeId(rank)`
//! among the acceptors) or a **matchmaker** (`MatchmakerId(rank)` among the
//! matchmakers, a process that is *not* an acceptor). The acceptor list is the
//! cluster membership every node derives its `Config` from and every client
//! proposes to; the matchmaker list is what the matchmaking client asks.
//!
//! The map is factory-created per seed — drawn once by whichever process or
//! workload boots first, published on the `StateHandle` beside the storage
//! world and the shape registry, and handed back unchanged to every later
//! caller and every restart — so a recipe replays it exactly and an attrition
//! restart never re-rolls a node's role.
//!
//! **The default is the plain Multi-Paxos deployment** (AGENTS.md, *Plain
//! Multi-Paxos is first-class*): no matchmakers, every process an acceptor.
//! Whether a seed deploys matchmakers is a `buggify_knob!` draw (prong 2) — an
//! activated seed draws a fixed matchmaker set of `2f + 1` for `f ∈ {0, 1}`,
//! carved out of the pool. The knob's floor is structural: at least
//! [`MIN_ACCEPTORS`] acceptors must remain (three is the smallest cluster that
//! tolerates a failure), so a pool too small for the drawn set shrinks the set
//! (three matchmakers → one → none) rather than the cluster. The scripted
//! corpus never draws: its three nodes are three acceptors, as before.

use std::net::IpAddr;
use std::sync::Arc;

use moonpool_sim::{StateHandle, assert_always, assert_reachable, buggify_knob};
use paros::{MatchmakerId, NodeId};

/// Well-known [`StateHandle`] key of the per-iteration map.
const ROLES_KEY: &str = "paros-deployment";

/// The smallest acceptor cluster a deployment may leave: three is the smallest
/// cluster that tolerates a failure, and the floor of the matchmaker knob.
pub(crate) const MIN_ACCEPTORS: usize = 3;

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
    /// The plain deployment over `pool`: every process an acceptor.
    fn plain(mut pool: Vec<String>) -> Self {
        sort_ips(&mut pool);
        Self {
            acceptors: pool,
            matchmakers: Vec::new(),
        }
    }

    /// Draw a perturbing seed's deployment over `pool`.
    fn draw(mut pool: Vec<String>) -> Self {
        sort_ips(&mut pool);
        // The knob: off (plain Multi-Paxos), `f = 0` (one matchmaker), or
        // `f = 1` (three). One location, one draw per seed.
        let wanted = match buggify_knob!(0_u64, 1_u64..3_u64) {
            0 => 0,
            1 => 1,
            _ => 3,
        };
        // The floor: the drawn set shrinks before the acceptor cluster does.
        let room = pool.len().saturating_sub(MIN_ACCEPTORS);
        let matchmakers = [wanted, 1, 0]
            .into_iter()
            .find(|m| *m <= wanted && *m <= room)
            .unwrap_or(0);
        if matchmakers > 0 {
            // BUGGIFY pairing: a seed genuinely deploys a matchmaker set, in
            // each of the two shapes.
            if matchmakers == 1 {
                assert_reachable!("a run deploys a single matchmaker (f = 0)");
            } else {
                assert_reachable!("a run deploys three matchmakers (f = 1)");
            }
        }
        let matchmakers = pool.split_off(pool.len() - matchmakers);
        Self {
            acceptors: pool,
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

    /// The acceptor cluster, in `NodeId` order.
    pub(crate) fn acceptors(&self) -> &[String] {
        &self.acceptors
    }

    /// The matchmaker set, in `MatchmakerId` order (empty on a plain seed).
    pub(crate) fn matchmakers(&self) -> &[String] {
        &self.matchmakers
    }

    /// Whether this seed deploys matchmakers at all.
    pub(crate) fn has_matchmakers(&self) -> bool {
        !self.matchmakers.is_empty()
    }
}

fn sort_ips(ips: &mut Vec<String>) {
    ips.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
    ips.dedup();
}

/// The seed's deployment over `pool` (every process IP of the topology, in any
/// order, duplicates allowed): drawn by the first caller, reused by every
/// later one. `perturb` selects the drawn (main-campaign) map over the plain
/// one; it is a property of the campaign, so every caller passes the same
/// value and every caller sees the same map.
#[tracing::instrument(level = "debug", skip_all, fields(pool = pool.len(), perturb))]
pub(crate) fn deployment(state: &StateHandle, pool: &[String], perturb: bool) -> Arc<Deployment> {
    if let Some(existing) = state.get::<Arc<Deployment>>(ROLES_KEY) {
        let mut sorted = pool.to_vec();
        sort_ips(&mut sorted);
        let consistent = existing.acceptors.len() + existing.matchmakers.len() == sorted.len();
        assert_always!(
            consistent,
            "every process and workload derives the same deployment pool",
            { "pool" => sorted.len(), "mapped" => existing.acceptors.len() + existing.matchmakers.len() }
        );
        return existing;
    }
    let map = Arc::new(if perturb {
        Deployment::draw(pool.to_vec())
    } else {
        Deployment::plain(pool.to_vec())
    });
    assert_always!(
        map.acceptors.len() >= MIN_ACCEPTORS.min(pool.len()),
        "a deployment keeps the acceptor floor",
        { "acceptors" => map.acceptors.len(), "matchmakers" => map.matchmakers.len() }
    );
    tracing::info!(
        acceptors = map.acceptors.len() as u64,
        matchmakers = map.matchmakers.len() as u64,
        "deployment_drawn"
    );
    state.publish(ROLES_KEY, map.clone());
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(n: usize) -> Vec<String> {
        (1..=n).map(|i| format!("10.0.1.{i}")).collect()
    }

    /// The mechanism, not a seed: a plain map is every process an acceptor in
    /// IP order, and a second caller gets the same map back.
    #[test]
    fn a_plain_deployment_ranks_every_process_as_an_acceptor() {
        let state = StateHandle::new();
        let mut shuffled = pool(4);
        shuffled.reverse();
        let map = deployment(&state, &shuffled, false);
        assert_eq!(map.acceptors(), pool(4).as_slice());
        assert!(!map.has_matchmakers());
        assert_eq!(map.role_of("10.0.1.3"), Some(Role::Acceptor(NodeId(2))));
        assert_eq!(map.role_of("10.9.9.9"), None);
        let again = deployment(&state, &pool(4), false);
        assert_eq!(*again, *map);
    }

    /// A drawn map ranks matchmakers among themselves and keeps the acceptor
    /// floor whatever the knob says (outside a simulation the knob is inert,
    /// so this pins the plain outcome of the drawn path too).
    #[test]
    fn a_drawn_deployment_keeps_the_acceptor_floor() {
        let state = StateHandle::new();
        let map = deployment(&state, &pool(3), true);
        assert!(map.acceptors().len() >= MIN_ACCEPTORS);
        assert_eq!(map.acceptors().len() + map.matchmakers().len(), 3);
        for (rank, ip) in map.matchmakers().iter().enumerate() {
            assert_eq!(
                map.role_of(ip),
                Some(Role::Matchmaker(MatchmakerId(rank as u64)))
            );
        }
    }
}
