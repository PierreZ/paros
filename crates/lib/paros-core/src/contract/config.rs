//! Static node configuration ([`Config`]).

use crate::journal::JournalIdentifier;
use crate::journal::state::WriterMode;
use crate::membership::{AcceptorConfig, FailureDomain, MatchmakerId, QuorumSystem};
use crate::types::NodeId;

/// Static, immutable-for-this-instance configuration: who *I* am, the
/// **bootstrap** acceptor configuration, the pool of nodes that may ever be an
/// acceptor, and the matchmaker set (empty for plain Multi-Paxos).
///
/// Two deployments live in this one struct, told apart by `matchmakers`
/// (and, orthogonally, the configuration's proxy count says whether Phase 2
/// is delegated):
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
///   has seen (`ColocatedNode::acceptors`). A node may be in `nodes` without being
///   in `peers` — a spare waiting to be added — and may be in `peers` and
///   later removed; either way it stays addressable, answers Phase 1 for the
///   ballots it took part in, and learns the chosen log as a replica.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    /// moves the node's *volatile* belief (`ColocatedNode::matchmaker_set`) to a
    /// later generation; this stays the set a fresh incarnation asks first.
    pub matchmakers: Vec<MatchmakerId>,
    /// Every matchmaker that may ever be in a matchmaker set — the pool a
    /// matchmaker-set reconfiguration draws from, a superset of
    /// `matchmakers`. Empty means "exactly `matchmakers`".
    pub matchmaker_pool: Vec<MatchmakerId>,
    /// How many **proxy leaders** the bootstrap configuration names (#142,
    /// Compartmentalized Paxos §3.1): `ProxyId(0..proxy_count)`. **Zero is
    /// the plain deployment**: every Phase 2 stays colocated on the leader,
    /// the `None` arm like an empty `matchmakers`. The count in force is the
    /// one of the configuration bound to the ballot
    /// ([`AcceptorConfig::proxy_count`], #754 (counts off the format
    /// marker)): a reconfiguration changes it, as a round change. It is
    /// **not recorded in the format marker** ([`Config::recorded`]), so a
    /// restart may name another bootstrap count; acceptors read counts only
    /// for wire checks, so safety never depends on them. Which process
    /// answers to a `ProxyId` is the driver's deployment map.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub proxy_count: usize,
    /// How many **replicas** the bootstrap configuration names (#144,
    /// Compartmentalized Paxos §3.3): `ReplicaId(0..replica_count)`, each a
    /// [`crate::ReplicaNode`] that learns and applies but never votes.
    /// **Zero is the plain deployment**, the `None` arm like a zero
    /// `proxy_count`. As `proxy_count`: the count in force is the
    /// configuration's ([`AcceptorConfig::reply_owner`] names a slot's reply
    /// owner), and the format marker does not record it (#754).
    #[cfg_attr(feature = "serde", serde(skip))]
    pub replica_count: usize,
    /// The journal this node serves, named with its tenant (#184, #235):
    /// carried for assertions and tracing, never read by a protocol decision
    /// — the driver routes a message to its journal before the core sees it,
    /// and a client call naming any other journal is refused at the wire.
    /// A deployment draws one for every journal it serves (no id is fixed
    /// and none has a default, `docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8); a node alone
    /// that routes nothing is given [`JournalIdentifier::UNSET`].
    pub journal: JournalIdentifier,
    /// Who may write the journal (#241, `docs/architecture/02-data-plane.md` §2): fixed
    /// when the journal is created and recorded with the rest of this
    /// configuration in the format marker, so a restart never changes it.
    /// [`WriterMode::Single`] is the default: a fresh configuration is the
    /// single-writer journal. The default does not make an older marker
    /// decode: a store formatted before this field is not migrated.
    #[cfg_attr(feature = "serde", serde(default))]
    pub writer_mode: WriterMode,
    /// The failure domain of each of `peers`, in the same order (#215
    /// (zones)): the labels of the bootstrap configuration
    /// ([`Config::bootstrap`]). **Empty is the plain arm**, an unlabeled
    /// configuration. Recorded in the format marker with the rest, so a
    /// restart never changes a label.
    #[cfg_attr(feature = "serde", serde(default))]
    pub peer_domains: Vec<FailureDomain>,
    /// The cell that owns the journal (#215 (zones), `docs/architecture/03-7-universe.md`
    /// §3.7): the bootstrap configuration's `cell`. `0` is none, the plain
    /// arm.
    #[cfg_attr(feature = "serde", serde(default))]
    pub cell_id: u64,
}

impl Config {
    /// Node `id` serving the journal `journal`, alone: no peers, a
    /// majority, nothing opt-in. There is no `Config::default`: a node and
    /// its identifier are always named (no id has a default,
    /// `docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8); a sans-IO caller that routes nothing
    /// names [`JournalIdentifier::UNSET`] outright.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn new(id: NodeId, journal: JournalIdentifier) -> Self {
        let config = Self {
            id,
            peers: Vec::new(),
            quorum_system: QuorumSystem::default(),
            nodes: Vec::new(),
            matchmakers: Vec::new(),
            matchmaker_pool: Vec::new(),
            proxy_count: 0,
            replica_count: 0,
            journal,
            writer_mode: WriterMode::Single,
            peer_domains: Vec::new(),
            cell_id: 0,
        };
        // A fresh configuration is the plain deployment: every opt-in is off.
        assert!(
            !config.has_matchmakers(),
            "a fresh configuration names no matchmaker"
        );
        assert!(!config.has_proxies(), "a fresh configuration runs no proxy");
        assert!(
            !config.has_replicas(),
            "a fresh configuration runs no replica tier"
        );
        config
    }

    /// The bootstrap acceptor configuration: `peers` under `quorum_system`,
    /// each labeled with its failure domain when `peer_domains` names them,
    /// owned by `cell_id` (#215 (zones)).
    ///
    /// # Panics
    ///
    /// If `peer_domains` is neither empty nor one label per peer, or as
    /// [`AcceptorConfig::new`].
    #[must_use]
    pub fn bootstrap(&self) -> AcceptorConfig {
        if self.peer_domains.is_empty() {
            return AcceptorConfig::new(self.peers.clone(), self.quorum_system)
                .with_cell(self.cell_id)
                .with_counts(self.proxy_count, self.replica_count);
        }
        assert_eq!(
            self.peer_domains.len(),
            self.peers.len(),
            "a configuration labels every bootstrap member or none"
        );
        let config = AcceptorConfig::zoned(
            self.peers
                .iter()
                .copied()
                .zip(self.peer_domains.iter().cloned())
                .collect(),
            self.quorum_system,
        );
        assert!(config.is_zoned(), "a labeled bootstrap stays labeled");
        config
            .with_cell(self.cell_id)
            .with_counts(self.proxy_count, self.replica_count)
    }

    /// This configuration as the format marker records it (#754 (counts off
    /// the format marker)): the counts cleared, every other field unchanged.
    /// A store is formatted with it, and a boot compares the marker with the
    /// operator's configuration through it, so a count change never refuses
    /// a restart.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn recorded(&self) -> Self {
        let recorded = Self {
            proxy_count: 0,
            replica_count: 0,
            ..self.clone()
        };
        assert!(
            !recorded.has_proxies() && !recorded.has_replicas(),
            "the format marker records no count"
        );
        recorded
    }

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

    /// Whether the bootstrap configuration runs proxy leaders (the opt-in
    /// that delegates Phase 2, #142).
    #[must_use]
    pub fn has_proxies(&self) -> bool {
        self.proxy_count > 0
    }

    /// Whether the bootstrap configuration runs a replica tier (#144).
    #[must_use]
    pub fn has_replicas(&self) -> bool {
        self.replica_count > 0
    }

    /// The matchmaker pool: `matchmaker_pool`, or `matchmakers` when empty.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn matchmaker_pool(&self) -> &[MatchmakerId] {
        let pool = if self.matchmaker_pool.is_empty() {
            &self.matchmakers
        } else {
            &self.matchmaker_pool
        };
        if self.has_matchmakers() {
            assert!(
                !pool.is_empty(),
                "a matchmaker deployment has a matchmaker pool"
            );
        }
        pool
    }
}
