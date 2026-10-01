//! The deployment as the operator states it: the address book of every
//! role and the protocol configuration each process derives from it.
//!
//! Every process of a deployment is started with the **same** topology and
//! derives the same configuration from it, exactly as the simulation's
//! deployment/role map does (`paros_sim::roles`): the acceptor pool, the
//! bootstrap configuration (the whole pool unless the operator names a
//! subset, which only a deployment with matchmakers may), the matchmakers,
//! the proxy leaders (`ProxyId(i)`, the `i`-th named) and the replicas
//! (`ReplicaId(i)` is the `i`-th named, each with its own `NodeId` outside
//! the pool). The quorum system is the majority; a flexible or grid system
//! is protocol data a later flag adds.
//!
//! The first journal listed is the deployment's: the only one the matchmaker
//! plane, the proxy leaders and the replica tier serve (#188). Every other
//! journal is plain Multi-Paxos over the whole pool — the sim's rule.
//!
//! Nothing here is durable yet: a node reads its configuration from these
//! flags on every boot, and #207 records it at format so an edited one is
//! refused.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use paros::{
    AcceptorConfig, Config, JournalId, MatchmakerConfig, MatchmakerId, NodeId, ProxyConfig,
    ProxyId, QuorumSystem,
};

/// One `ID=HOST:PORT` entry of an address book.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The identity.
    pub id: u64,
    /// The socket address, normalised (`parse_addr`'s form).
    pub addr: String,
}

impl FromStr for Entry {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id, addr) = s
            .split_once('=')
            .ok_or_else(|| format!("`{s}`: expected ID=HOST:PORT"))?;
        let id = id
            .trim()
            .parse::<u64>()
            .map_err(|e| format!("`{s}`: bad id: {e}"))?;
        let addr = addr.trim().parse::<SocketAddr>().map_err(|e| {
            format!("`{s}`: bad address (an IP and a port; hostnames are #209): {e}")
        })?;
        Ok(Self {
            id,
            addr: addr.to_string(),
        })
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.id, self.addr)
    }
}

/// The whole deployment (see the module doc).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Topology {
    /// The acceptor pool: every node that may ever be an acceptor.
    pub nodes: Vec<Entry>,
    /// The bootstrap configuration, when it is not the whole pool.
    pub bootstrap: Vec<u64>,
    /// The matchmakers (empty: plain Multi-Paxos).
    pub matchmakers: Vec<Entry>,
    /// The proxy leaders, in `ProxyId` order (empty: Phase 2 colocated).
    pub proxies: Vec<Entry>,
    /// The replicas, in `ReplicaId` order (empty: no replica tier).
    pub replicas: Vec<Entry>,
    /// The journals every node serves; the first is the deployment's.
    pub journals: Vec<JournalId>,
}

fn ids(entries: &[Entry]) -> Vec<u64> {
    entries.iter().map(|entry| entry.id).collect()
}

fn unique(what: &str, ids: &[u64]) -> Result<(), String> {
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    match sorted.windows(2).find(|pair| pair[0] == pair[1]) {
        Some(pair) => Err(format!("{what}: id {} named twice", pair[0])),
        None => Ok(()),
    }
}

impl Topology {
    /// Check what no driver would: identities unique within and across the
    /// node and replica books, a bootstrap inside the pool, a subset
    /// bootstrap only with matchmakers, journal ids in range.
    ///
    /// # Errors
    ///
    /// A description of the first inconsistency.
    pub fn validate(&self) -> Result<(), String> {
        if self.nodes.is_empty() {
            return Err("at least one node (--node ID=ADDR)".into());
        }
        unique("--node", &ids(&self.nodes))?;
        unique("--matchmaker", &ids(&self.matchmakers))?;
        unique("--proxy", &ids(&self.proxies))?;
        let mut wire = ids(&self.nodes);
        wire.extend(ids(&self.replicas));
        unique("--node and --replica (one NodeId space)", &wire)?;
        unique(
            "--journal",
            &self.journals.iter().map(|j| j.0).collect::<Vec<_>>(),
        )?;
        if self.journals.is_empty() {
            return Err("at least one journal".into());
        }
        if let Some(bad) = self.journals.iter().find(|j| j.0 < JournalId::FIRST_USER.0) {
            return Err(format!(
                "journal {} is reserved (user journals start at {})",
                bad.0,
                JournalId::FIRST_USER.0
            ));
        }
        unique("--bootstrap", &self.bootstrap)?;
        let pool = ids(&self.nodes);
        if let Some(stray) = self.bootstrap.iter().find(|id| !pool.contains(id)) {
            return Err(format!("--bootstrap names {stray}, not a --node"));
        }
        if !self.bootstrap.is_empty()
            && self.bootstrap.len() != pool.len()
            && self.matchmakers.is_empty()
        {
            return Err(
                "a bootstrap smaller than the pool needs matchmakers: plain Multi-Paxos never reconfigures"
                    .into(),
            );
        }
        Ok(())
    }

    fn pool(&self) -> Vec<NodeId> {
        let mut pool: Vec<NodeId> = self.nodes.iter().map(|n| NodeId(n.id)).collect();
        pool.sort_unstable();
        pool
    }

    fn bootstrap_members(&self) -> Vec<NodeId> {
        if self.bootstrap.is_empty() {
            return self.pool();
        }
        let mut members: Vec<NodeId> = self.bootstrap.iter().copied().map(NodeId).collect();
        members.sort_unstable();
        members
    }

    /// The deployment journal's bootstrap acceptor configuration.
    #[must_use]
    pub fn bootstrap_config(&self) -> AcceptorConfig {
        AcceptorConfig::new(self.bootstrap_members(), QuorumSystem::Majority)
    }

    /// The deployment journal (the first listed).
    #[must_use]
    pub fn deployment_journal(&self) -> JournalId {
        self.journals[0]
    }

    /// The configuration `id` runs the deployment journal under — a node of
    /// the pool or a replica.
    #[must_use]
    pub fn deployment_config(&self, id: NodeId) -> Config {
        let matchmakers: Vec<MatchmakerId> = self
            .matchmakers
            .iter()
            .map(|m| MatchmakerId(m.id))
            .collect();
        Config {
            id,
            peers: self.bootstrap_members(),
            quorum_system: QuorumSystem::Majority,
            nodes: self.pool(),
            matchmaker_pool: matchmakers.clone(),
            matchmakers,
            proxy_count: self.proxies.len(),
            replica_count: self.replicas.len(),
            journal: self.deployment_journal(),
        }
    }

    /// Every journal's configuration on node `id`: the deployment's, then
    /// each other journal as plain Multi-Paxos over the whole pool.
    #[must_use]
    pub fn node_configs(&self, id: NodeId) -> Vec<Config> {
        let deployment = self.deployment_config(id);
        let mut configs = vec![deployment.clone()];
        configs.extend(self.journals[1..].iter().map(|&journal| Config {
            journal,
            peers: self.pool(),
            quorum_system: QuorumSystem::Majority,
            matchmakers: Vec::new(),
            matchmaker_pool: Vec::new(),
            proxy_count: 0,
            replica_count: 0,
            ..deployment.clone()
        }));
        configs
    }

    /// Matchmaker `id`'s configuration.
    #[must_use]
    pub fn matchmaker_config(&self, id: MatchmakerId) -> MatchmakerConfig {
        MatchmakerConfig {
            id,
            bootstrap: self
                .matchmakers
                .iter()
                .map(|m| MatchmakerId(m.id))
                .collect(),
        }
    }

    /// Proxy leader `id`'s configuration.
    #[must_use]
    pub fn proxy_config(&self, id: ProxyId) -> ProxyConfig {
        ProxyConfig {
            id,
            acceptors: self.bootstrap_config(),
            journal: self.deployment_journal(),
        }
    }

    /// The node address book, as every driver takes it.
    #[must_use]
    pub fn node_book(&self) -> Vec<(NodeId, String)> {
        self.nodes
            .iter()
            .map(|n| (NodeId(n.id), n.addr.clone()))
            .collect()
    }

    /// The matchmaker address book.
    #[must_use]
    pub fn matchmaker_book(&self) -> Vec<(MatchmakerId, String)> {
        self.matchmakers
            .iter()
            .map(|m| (MatchmakerId(m.id), m.addr.clone()))
            .collect()
    }

    /// The proxy leaders' address book: `ProxyId(i)` is the `i`-th named.
    #[must_use]
    pub fn proxy_book(&self) -> Vec<(ProxyId, String)> {
        (0..)
            .zip(&self.proxies)
            .map(|(rank, p)| (ProxyId(rank), p.addr.clone()))
            .collect()
    }

    /// The replica tier's address book, in `ReplicaId` order.
    #[must_use]
    pub fn replica_book(&self) -> Vec<(NodeId, String)> {
        self.replicas
            .iter()
            .map(|r| (NodeId(r.id), r.addr.clone()))
            .collect()
    }

    /// `ProxyId` of the proxy named `id` in the book, if it is named.
    #[must_use]
    pub fn proxy_rank(&self, id: u64) -> Option<ProxyId> {
        (0..)
            .zip(&self.proxies)
            .find(|(_, p)| p.id == id)
            .map(|(rank, _)| ProxyId(rank))
    }

    /// Where `id` listens, from the book it is named in.
    #[must_use]
    pub fn address_of(book: &[Entry], id: u64) -> Option<String> {
        book.iter().find(|e| e.id == id).map(|e| e.addr.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(s: &str) -> Entry {
        s.parse().expect("valid entry")
    }

    fn three() -> Topology {
        Topology {
            nodes: vec![
                entry("0=127.0.0.1:4500"),
                entry("1=127.0.0.1:4501"),
                entry("2=127.0.0.1:4502"),
            ],
            journals: vec![JournalId::FIRST_USER],
            ..Topology::default()
        }
    }

    #[test]
    fn an_entry_is_an_id_and_a_socket_address() {
        assert_eq!(
            entry("7= 127.0.0.1:9 "),
            Entry {
                id: 7,
                addr: "127.0.0.1:9".into()
            }
        );
        assert!("7".parse::<Entry>().is_err());
        assert!("x=127.0.0.1:9".parse::<Entry>().is_err());
        assert!("7=localhost:9".parse::<Entry>().is_err());
    }

    #[test]
    fn a_plain_deployment_bootstraps_the_whole_pool() {
        let topology = three();
        topology.validate().expect("valid");
        let config = topology.deployment_config(NodeId(1));
        assert_eq!(config.peers, vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert!(!config.has_matchmakers());
        assert_eq!(config.journal, JournalId::FIRST_USER);
    }

    #[test]
    fn every_other_journal_is_plain_over_the_pool() {
        let mut topology = three();
        topology.matchmakers = vec![entry("0=127.0.0.1:4600")];
        topology.bootstrap = vec![0, 1];
        topology.journals.push(JournalId(129));
        topology.validate().expect("valid");
        let configs = topology.node_configs(NodeId(2));
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].peers, vec![NodeId(0), NodeId(1)]);
        assert!(configs[0].has_matchmakers());
        assert_eq!(configs[1].journal, JournalId(129));
        assert_eq!(configs[1].peers, vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert!(!configs[1].has_matchmakers());
    }

    #[test]
    fn inconsistent_topologies_are_refused() {
        let mut subset = three();
        subset.bootstrap = vec![0, 1];
        assert!(subset.validate().is_err(), "a subset without matchmakers");

        let mut clash = three();
        clash.replicas = vec![entry("1=127.0.0.1:4700")];
        assert!(clash.validate().is_err(), "a replica reusing a node id");

        let mut reserved = three();
        reserved.journals = vec![JournalId(1)];
        assert!(reserved.validate().is_err(), "a system journal id");

        let mut stray = three();
        stray.matchmakers = vec![entry("0=127.0.0.1:4600")];
        stray.bootstrap = vec![9];
        assert!(stray.validate().is_err(), "a bootstrap outside the pool");
    }
}
