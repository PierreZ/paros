//! The deployment every role is started with: the address books of the four
//! roles, the journals the pool serves, and the core [`Config`] each role
//! derives from them.
//!
//! The deployment is **configuration data the operator hands every process
//! alike**, and each process derives the same thing from it: a node's
//! bootstrap membership is the whole acceptor pool under a majority, its
//! matchmaker set the whole matchmaker book, its proxy and replica counts
//! the length of those books. The derived [`Config`] is a safety input — it
//! is recorded in every store at `format` and an edited one is refused at
//! the next boot (#207) — so it is derived, never typed in twice.
//!
//! An address is `HOST:PORT`, a literal or a name (#209). The names are
//! resolved once, at startup ([`Deployment::resolve`]), and the drivers
//! only ever see socket addresses; the derived [`Config`] carries ids, never
//! addresses, so a peer that comes back at another address is the same
//! member.

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use clap::Args;
use paros::{
    AcceptorConfig, Config, JournalId, MatchmakerConfig, MatchmakerId, NodeId, ProxyConfig,
    ProxyId, QuorumSystem,
};

/// One entry of an address book: `ID=HOST:PORT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The role's numeric identity.
    pub id: u64,
    /// Where it listens: `HOST:PORT` as given, then the socket address it
    /// resolved to ([`Deployment::resolve`]).
    pub addr: String,
}

impl FromStr for Entry {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id, addr) = s
            .split_once('=')
            .ok_or_else(|| format!("expected ID=HOST:PORT, got {s:?}"))?;
        let id = id
            .trim()
            .parse()
            .map_err(|e| format!("bad id in {s:?}: {e}"))?;
        let addr = addr.trim();
        crate::resolve::check_shape(addr).map_err(|e| format!("bad address in {s:?}: {e}"))?;
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

/// The deployment: every role's address book and the journals served.
#[derive(Args, Clone, Debug)]
pub struct Deployment {
    /// An acceptor node, `ID=HOST:PORT`; repeat for the whole pool. The
    /// pool is the bootstrap membership, under a majority.
    #[arg(long = "node", required = true, value_name = "ID=ADDR")]
    pub nodes: Vec<Entry>,
    /// A matchmaker, `ID=HOST:PORT`; repeat for the whole set. None is the
    /// plain Multi-Paxos deployment.
    #[arg(long = "matchmaker", value_name = "ID=ADDR")]
    pub matchmakers: Vec<Entry>,
    /// A proxy leader, `ID=HOST:PORT` with ids `0..n`. None is the plain
    /// deployment.
    #[arg(long = "proxy", value_name = "ID=ADDR")]
    pub proxies: Vec<Entry>,
    /// A replica, `ID=HOST:PORT` with its wire node id (outside the pool's
    /// ids). None is the plain deployment.
    #[arg(long = "replica", value_name = "ID=ADDR")]
    pub replicas: Vec<Entry>,
    /// A journal the pool serves (`>= 128`); repeat for several. The first
    /// is the deployment's: the matchmakers, proxies and replicas serve it,
    /// and every other one is plain Multi-Paxos over the whole pool.
    #[arg(long = "journal", value_name = "ID", default_value = "128")]
    pub journals: Vec<u64>,
}

impl Deployment {
    /// Check what no derivation below can: ids unique and in range, the
    /// proxies numbered `0..n`, the replicas outside the pool, the journals
    /// user journals.
    ///
    /// # Errors
    ///
    /// A description of the first inconsistency.
    pub fn validate(&self) -> Result<(), String> {
        unique("node", &self.nodes)?;
        unique("matchmaker", &self.matchmakers)?;
        unique("proxy", &self.proxies)?;
        unique("replica", &self.replicas)?;
        let mut proxies: Vec<u64> = self.proxies.iter().map(|e| e.id).collect();
        proxies.sort_unstable();
        if proxies.iter().copied().ne(0..self.proxies.len() as u64) {
            return Err("proxy ids must be 0..n".into());
        }
        if let Some(replica) = self
            .replicas
            .iter()
            .find(|r| self.nodes.iter().any(|n| n.id == r.id))
        {
            return Err(format!("replica id {} is also a node id", replica.id));
        }
        if self.journals.is_empty() {
            return Err("at least one journal".into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for &journal in &self.journals {
            if !JournalId(journal).is_user() {
                return Err(format!(
                    "journal {journal} is reserved: user journals start at {}",
                    JournalId::FIRST_USER.0
                ));
            }
            if !seen.insert(journal) {
                return Err(format!("journal {journal} listed twice"));
            }
        }
        Ok(())
    }

    /// Resolve every address of the deployment to a socket address, once,
    /// at startup. A name that does not resolve yet (a Compose service
    /// whose container is still starting) is asked again every
    /// `retry` until `patience` runs out.
    ///
    /// # Errors
    ///
    /// The first name that never resolved within `patience`.
    pub fn resolve(&mut self, patience: Duration, retry: Duration) -> Result<(), String> {
        let deadline = Instant::now() + patience;
        for entry in self
            .nodes
            .iter_mut()
            .chain(&mut self.matchmakers)
            .chain(&mut self.proxies)
            .chain(&mut self.replicas)
        {
            let addr = loop {
                match crate::resolve::resolve(&entry.addr) {
                    Ok(addr) => break addr,
                    Err(error) if Instant::now() >= deadline => return Err(error),
                    Err(error) => {
                        tracing::info!(%error, "parosd_resolve_retry");
                        std::thread::sleep(retry);
                    }
                }
            };
            let resolved = addr.to_string();
            if resolved != entry.addr {
                tracing::info!(id = entry.id, host = %entry.addr, %addr, "parosd_resolved");
            }
            entry.addr = resolved;
        }
        Ok(())
    }

    /// The acceptor pool, in id order.
    #[must_use]
    pub fn pool(&self) -> Vec<NodeId> {
        let mut pool: Vec<NodeId> = self.nodes.iter().map(|e| NodeId(e.id)).collect();
        pool.sort_unstable();
        pool
    }

    /// The bootstrap matchmaker set, in id order (empty: plain).
    #[must_use]
    pub fn matchmaker_set(&self) -> Vec<MatchmakerId> {
        let mut set: Vec<MatchmakerId> = self
            .matchmakers
            .iter()
            .map(|e| MatchmakerId(e.id))
            .collect();
        set.sort_unstable();
        set
    }

    /// The deployment's journal: the first listed.
    #[must_use]
    pub fn first_journal(&self) -> JournalId {
        JournalId(self.journals[0])
    }

    /// The bootstrap acceptor configuration: the whole pool, a majority.
    #[must_use]
    pub fn bootstrap(&self) -> AcceptorConfig {
        let pool = self.pool();
        AcceptorConfig::new(pool, QuorumSystem::Majority)
    }

    /// Node `id`'s configuration of `journal`: the deployment's on the first
    /// journal, plain Multi-Paxos over the whole pool on every other one —
    /// the driver's rule that only a node's first journal names
    /// matchmakers, proxies or replicas.
    #[must_use]
    pub fn node_config(&self, id: NodeId, journal: JournalId) -> Config {
        let pool = self.pool();
        let plain = Config {
            journal,
            id,
            peers: pool.clone(),
            quorum_system: QuorumSystem::Majority,
            nodes: pool,
            ..Config::default()
        };
        if journal != self.first_journal() {
            return plain;
        }
        let matchmakers = self.matchmaker_set();
        Config {
            matchmaker_pool: matchmakers.clone(),
            matchmakers,
            proxy_count: self.proxies.len(),
            replica_count: self.replicas.len(),
            ..plain
        }
    }

    /// Replica `id`'s configuration: the bootstrap membership it learns
    /// from, outside the pool, on the deployment's journal. It names the
    /// matchmakers too: on a matchmaker deployment a replica follows the
    /// configuration the beats carry, and a quorum read it serves is bound
    /// to the configuration the acceptors it asked are in.
    #[must_use]
    pub fn replica_config(&self, id: NodeId) -> Config {
        let pool = self.pool();
        let matchmakers = self.matchmaker_set();
        Config {
            journal: self.first_journal(),
            id,
            peers: pool.clone(),
            quorum_system: QuorumSystem::Majority,
            nodes: pool,
            matchmaker_pool: matchmakers.clone(),
            matchmakers,
            replica_count: self.replicas.len(),
            ..Config::default()
        }
    }

    /// Matchmaker `id`'s configuration.
    #[must_use]
    pub fn matchmaker_config(&self, id: MatchmakerId) -> MatchmakerConfig {
        MatchmakerConfig {
            id,
            bootstrap: self.matchmaker_set(),
        }
    }

    /// Proxy `id`'s configuration.
    #[must_use]
    pub fn proxy_config(&self, id: ProxyId) -> ProxyConfig {
        ProxyConfig {
            id,
            acceptors: self.bootstrap(),
            journal: self.first_journal(),
        }
    }

    /// The acceptor address book.
    #[must_use]
    pub fn node_book(&self) -> Vec<(NodeId, String)> {
        book(&self.nodes, NodeId)
    }

    /// The matchmaker address book.
    #[must_use]
    pub fn matchmaker_book(&self) -> Vec<(MatchmakerId, String)> {
        book(&self.matchmakers, MatchmakerId)
    }

    /// The proxy address book.
    #[must_use]
    pub fn proxy_book(&self) -> Vec<(ProxyId, String)> {
        book(&self.proxies, ProxyId)
    }

    /// The replica address book, under their wire node ids.
    #[must_use]
    pub fn replica_book(&self) -> Vec<(NodeId, String)> {
        book(&self.replicas, NodeId)
    }

    /// Where `id` listens in `entries`.
    ///
    /// # Errors
    ///
    /// `id` is not in the book.
    pub fn addr_of(entries: &[Entry], role: &str, id: u64) -> Result<String, String> {
        entries
            .iter()
            .find(|e| e.id == id)
            .map(|e| e.addr.clone())
            .ok_or_else(|| format!("{role} {id} is not in the deployment's {role} list"))
    }
}

fn unique(role: &str, entries: &[Entry]) -> Result<(), String> {
    let mut ids: Vec<u64> = entries.iter().map(|e| e.id).collect();
    ids.sort_unstable();
    if let Some([twice, ..]) = ids.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(format!("{role} id {twice} listed twice"));
    }
    Ok(())
}

fn book<Id: Ord>(entries: &[Entry], id: impl Fn(u64) -> Id) -> Vec<(Id, String)> {
    let mut book: Vec<(Id, String)> = entries.iter().map(|e| (id(e.id), e.addr.clone())).collect();
    book.sort_by(|a, b| a.0.cmp(&b.0));
    book
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deployment() -> Deployment {
        Deployment {
            nodes: vec![
                "1=127.0.0.1:2".parse().expect("valid entry"),
                "0=127.0.0.1:1".parse().expect("valid entry"),
            ],
            matchmakers: vec!["0=127.0.0.1:3".parse().expect("valid entry")],
            proxies: Vec::new(),
            replicas: vec!["1000=127.0.0.1:4".parse().expect("valid entry")],
            journals: vec![128, 129],
        }
    }

    #[test]
    fn only_the_first_journal_names_the_planes() {
        let d = deployment();
        d.validate().expect("valid entry");
        let first = d.node_config(NodeId(0), JournalId(128));
        assert_eq!(first.peers, vec![NodeId(0), NodeId(1)]);
        assert_eq!(first.matchmakers, vec![MatchmakerId(0)]);
        assert_eq!(first.replica_count, 1);
        let second = d.node_config(NodeId(0), JournalId(129));
        assert!(second.matchmakers.is_empty());
        assert_eq!(second.replica_count, 0);
        assert_eq!(second.journal, JournalId(129));
        let replica = d.replica_config(NodeId(1000));
        assert_eq!(replica.matchmakers, first.matchmakers);
        assert!(!replica.peers.contains(&NodeId(1000)));
    }

    #[test]
    fn inconsistent_deployments_are_refused() {
        let mut d = deployment();
        d.replicas = vec!["1=127.0.0.1:4".parse().expect("valid entry")];
        assert!(d.validate().is_err(), "a replica id inside the pool");
        let mut d = deployment();
        d.journals = vec![2];
        assert!(d.validate().is_err(), "a system journal id");
        let mut d = deployment();
        d.proxies = vec!["1=127.0.0.1:5".parse().expect("valid entry")];
        assert!(d.validate().is_err(), "proxy ids not 0..n");
        assert!("nope".parse::<Entry>().is_err());
    }
}
