//! Durable state ([`HardState`]) and static node configuration ([`Config`]),
//! with the proof obligations a configuration carries ([`Config::check`]).

use std::collections::BTreeSet;
use std::fmt;

use crate::membership::{MatchmakerId, MatchmakerSet, QuorumSystem, Quorums};
use crate::types::{Ballot, NodeId, Slot};

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
/// [`crate::AcceptorWrite::AppendAccepted`]) before replying
/// [`crate::Message::Accepted`]. Sending either reply before the corresponding
/// write is durable violates Paxos safety: a crash could "un-promise" or
/// "un-accept", letting two different values be chosen for one slot. The
/// [`crate::Ready`] handshake enforces *persist writes → then send messages*.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct HardState {
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
/// Two deployments live in this one struct, told apart by `matchmakers`
/// (and, orthogonally, `proxy_count` says whether Phase 2 is delegated):
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
    /// moves the node's *volatile* belief (`ColocatedNode::matchmaker_set`) to a
    /// later generation; this stays the set a fresh incarnation asks first.
    pub matchmakers: Vec<MatchmakerId>,
    /// Every matchmaker that may ever be in a matchmaker set — the pool a
    /// matchmaker-set reconfiguration draws from, a superset of
    /// `matchmakers`. Empty means "exactly `matchmakers`".
    pub matchmaker_pool: Vec<MatchmakerId>,
    /// How many **proxy leaders** the deployment runs (#142, Compartmentalized
    /// Paxos §3.1): `ProxyId(0..proxy_count)`. **Zero is the plain
    /// deployment** — every Phase 2 stays colocated on the leader and the
    /// wire carries exactly today's messages; the `None` arm, like an empty
    /// `matchmakers`. With a count, a leader delegates the Phase 2 of every
    /// slot it allocates on a settled leadership to `ProxyId(slot %
    /// proxy_count)` ([`crate::ProxyId::of`]), and only the count is
    /// protocol data: which process answers to a `ProxyId` is the driver's
    /// deployment map, so a dead proxy is replaced without editing this.
    pub proxy_count: usize,
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

    /// Whether this deployment runs proxy leaders (the opt-in that delegates
    /// Phase 2, #142).
    #[must_use]
    pub fn has_proxies(&self) -> bool {
        self.proxy_count > 0
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

    /// The proof obligations the deployment carries. Safety-side fields
    /// (the quorum system, the matchmaker set) each state a law; work-side
    /// fields (`proxy_count`) contribute nothing to a decision and prove
    /// nothing.
    ///
    /// `Config` *is* the flavor of Paxos a node runs — `quorum_system`,
    /// `matchmakers`, `proxy_count` — and this is where the flavor is
    /// judged, once, before [`crate::ColocatedNode::new`] builds the
    /// bootstrap configuration from it. Every obligation is over the
    /// **acceptor membership only** (`peers.len()`, never `nodes.len()`: the
    /// pool holds spares, and a future replica tier's processes, that vote
    /// on nothing), in this order:
    ///
    /// 1. every Phase-1 quorum meets every Phase-2 quorum
    ///    ([`ConfigError::QuorumsDoNotIntersect`]);
    /// 2. the quorum system can be run over the membership at all
    ///    ([`ConfigError::QuorumSystemNotAdmitted`]);
    /// 3. every read row meets every write column
    ///    ([`ConfigError::ReadRowMissesWriteColumn`]);
    /// 4. a named matchmaker set admits the matchmaker quorum system
    ///    ([`ConfigError::MatchmakerSetNotAdmitted`]).
    ///
    /// The first three are [`Config::check_quorums`] over `quorum_system`,
    /// so a reader's own [`Quorums`] can be judged by the same rule. A
    /// **work-side count proves nothing**: `proxy_count` has no line here —
    /// a proxy votes on nothing and adopts no ballot, and a count of zero is
    /// the colocated deployment — and a `replica_count` or a `batcher_count`
    /// landing beside it will have none either; adding such a field to this
    /// struct changes nothing in this method.
    ///
    /// The plain deployment — a fixed membership under the majority, no
    /// matchmakers, no proxies — passes trivially, and the shape checks
    /// (sorted memberships, this node in its pool) are the boot's own
    /// asserts, not obligations of the flavor.
    ///
    /// # Errors
    ///
    /// The first obligation the deployment fails, in the order above.
    pub fn check(&self) -> Result<(), ConfigError> {
        Self::check_quorums(&self.quorum_system, self.peers.len())?;
        if self.has_matchmakers() && !MatchmakerSet::admits(&self.matchmakers) {
            return Err(ConfigError::MatchmakerSetNotAdmitted);
        }
        Ok(())
    }

    /// The three quorum obligations of [`Config::check`] over any
    /// [`Quorums`] and an acceptor membership of `acceptors`: the law
    /// ([`Quorums::cross_intersects`]), the well-formedness arm
    /// ([`Quorums::admits`]), and Paxos Quorum Reads' obligation that every
    /// read row meets every write column (Compartmentalized Paxos §3.4).
    /// Public and generic so a reader's own implementation of the trait is
    /// judged by exactly the rule the wire's [`QuorumSystem`] is.
    ///
    /// The read rows are enumerated through [`Quorums::row_of`] over the
    /// tokens `0..acceptors` (a row system has at most one row per member,
    /// so every row is reached) and the write columns through
    /// [`Quorums::column_count`]; a system that names neither has one row
    /// and one column, the whole membership, which meet whenever the
    /// membership is non-empty. The obligation is over the membership's
    /// *size* alone — every predicate here is asked over the indices
    /// `0..acceptors`, since no addressing rule depends on what an identity
    /// is.
    ///
    /// # Errors
    ///
    /// The first obligation the system fails, in the order above.
    pub fn check_quorums<Q: Quorums + ?Sized>(
        system: &Q,
        acceptors: usize,
    ) -> Result<(), ConfigError> {
        if !system.cross_intersects(acceptors) {
            return Err(ConfigError::QuorumsDoNotIntersect { acceptors });
        }
        if !system.admits(acceptors) {
            return Err(ConfigError::QuorumSystemNotAdmitted { acceptors });
        }
        let members: Vec<usize> = (0..acceptors).collect();
        let rows: BTreeSet<Option<usize>> = (0..acceptors)
            .map(|ctx| system.row_of(u64::try_from(ctx).unwrap_or(u64::MAX)))
            .collect();
        let columns: Vec<Option<usize>> = match system.column_count() {
            Some(cols) => (0..cols).map(Some).collect(),
            None => vec![None],
        };
        for row in rows {
            let read_row = system.phase1_addressees(&members, row);
            for &column in &columns {
                let write_column = system.phase2_addressees(&members, column);
                if !read_row.iter().any(|m| write_column.contains(m)) {
                    return Err(ConfigError::ReadRowMissesWriteColumn { row, column });
                }
            }
        }
        Ok(())
    }
}

/// Why a [`Config`] fails [`Config::check`]: one variant per proof
/// obligation, each the law in one line and the paper it comes from. Every
/// `acceptors` is the acceptor membership's size (`Config::peers`), the only
/// count a quorum law is ever over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfigError {
    /// Some Phase-1 quorum misses some Phase-2 quorum — `Q1 ∩ Q2 = ∅` — so a
    /// later ballot could be elected without hearing what an earlier one
    /// chose. The one law of Paxos quorums (Flexible Paxos §3: `∀ Q1, ∀ Q2 :
    /// Q1 ∩ Q2 ≠ ∅`; [`Quorums::cross_intersects`]).
    QuorumsDoNotIntersect {
        /// The acceptor membership the system was judged over.
        acceptors: usize,
    },
    /// The quorum system cannot be run over the membership at all: a phase's
    /// quorum is empty or larger than the membership (Flexible Paxos §4, the
    /// simple quorums' bounds `1 <= q1, q2 <= n`), or a grid does not tile it
    /// (Compartmentalized Paxos §3.2, `rows × cols = n`); the well-formedness
    /// arm [`Quorums::admits`] states, and [`crate::AcceptorConfig::new`]
    /// asserts.
    QuorumSystemNotAdmitted {
        /// The acceptor membership the system was judged over.
        acceptors: usize,
    },
    /// A read row misses a write column, so a quorum read addressed to that
    /// row could answer without a vote watermark from the column a slot was
    /// decided on (Paxos Quorum Reads, Compartmentalized Paxos §3.4: a read
    /// quorum meets every write quorum). Holds by construction for every
    /// [`QuorumSystem`] today — a majority or a flexible split names no row
    /// and no column, so both are the whole membership, and a grid's rows
    /// and columns meet by geometry — and becomes a real check the moment
    /// [`Quorums`] has a third implementor.
    ReadRowMissesWriteColumn {
        /// The read row ([`Quorums::row_of`]), `None` the whole membership.
        row: Option<usize>,
        /// The write column ([`Quorums::column_of`]), `None` the whole
        /// membership.
        column: Option<usize>,
    },
    /// The named matchmaker set does not admit the matchmaker quorum system,
    /// a majority ([`MatchmakerSet::admits`], the rule [`MatchmakerSet::new`]
    /// asserts; Matchmaker Paxos §3, matchmaker quorums intersect). A plain
    /// deployment names no matchmakers and carries no such obligation.
    MatchmakerSetNotAdmitted,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::QuorumsDoNotIntersect { acceptors } => write!(
                f,
                "some Phase-1 quorum misses some Phase-2 quorum over {acceptors} acceptors"
            ),
            ConfigError::QuorumSystemNotAdmitted { acceptors } => write!(
                f,
                "the quorum system cannot be run over {acceptors} acceptors"
            ),
            ConfigError::ReadRowMissesWriteColumn { row, column } => {
                write!(f, "read row {row:?} misses write column {column:?}")
            }
            ConfigError::MatchmakerSetNotAdmitted => {
                write!(f, "the matchmaker set does not admit a majority quorum")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{Config, ConfigError};
    use crate::membership::{MatchmakerId, QuorumSystem, Quorums};
    use crate::types::NodeId;

    fn plain(peers: impl IntoIterator<Item = u64>) -> Config {
        Config {
            id: NodeId(1),
            peers: peers.into_iter().map(NodeId).collect(),
            ..Config::default()
        }
    }

    /// The plain deployment passes trivially: a fixed membership under the
    /// majority, no matchmakers, no proxies — and a proxy count, a work-side
    /// field, changes nothing.
    #[test]
    fn the_plain_deployment_passes_trivially() {
        assert_eq!(plain(1..=3).check(), Ok(()));
        assert_eq!(plain(1..=1).check(), Ok(()));
        let mut proxied = plain(1..=3);
        proxied.proxy_count = 2;
        assert_eq!(proxied.check(), Ok(()));
    }

    /// Every system the wire carries passes over a membership it admits,
    /// matchmakers or not; the obligations are over the acceptors, so a
    /// pool of spares beyond `peers` changes nothing.
    #[test]
    fn every_wire_system_passes_over_a_membership_it_admits() {
        let mut flexible = plain(1..=4);
        flexible.quorum_system = QuorumSystem::Flexible { q1: 3, q2: 2 };
        assert_eq!(flexible.check(), Ok(()));
        let mut grid = plain(1..=6);
        grid.quorum_system = QuorumSystem::Grid { rows: 2, cols: 3 };
        grid.nodes = (1..=8).map(NodeId).collect();
        grid.matchmakers = (1..=3).map(MatchmakerId).collect();
        assert_eq!(grid.check(), Ok(()));
    }

    /// The law first, then the bounds: two halves fail the intersection,
    /// a quorum larger than the membership fails the arm, a grid that does
    /// not tile fails the arm too (its geometry always intersects).
    #[test]
    fn the_first_failed_obligation_is_reported() {
        let mut halves = plain(1..=4);
        halves.quorum_system = QuorumSystem::Flexible { q1: 2, q2: 2 };
        assert_eq!(
            halves.check(),
            Err(ConfigError::QuorumsDoNotIntersect { acceptors: 4 })
        );
        let mut oversized = plain(1..=4);
        oversized.quorum_system = QuorumSystem::Flexible { q1: 5, q2: 1 };
        assert_eq!(
            oversized.check(),
            Err(ConfigError::QuorumSystemNotAdmitted { acceptors: 4 })
        );
        let mut untiled = plain(1..=5);
        untiled.quorum_system = QuorumSystem::Grid { rows: 2, cols: 3 };
        assert_eq!(
            untiled.check(),
            Err(ConfigError::QuorumSystemNotAdmitted { acceptors: 5 })
        );
        // The count is the acceptors', never the pool's.
        untiled.nodes = (1..=6).map(NodeId).collect();
        assert_eq!(
            untiled.check(),
            Err(ConfigError::QuorumSystemNotAdmitted { acceptors: 5 })
        );
    }

    /// A [`Quorums`] whose predicates are majorities — so the law holds,
    /// `3 + 3 > 4` — but whose *addressing* splits four members into two
    /// halves both ways: a read addressed to half `0` never hears from the
    /// column half `1` was decided on. The one obligation the wire's systems
    /// satisfy by construction, caught on a third implementor.
    struct MisroutedHalves;

    impl Quorums for MisroutedHalves {
        fn phase1_quorum_size(&self, members: usize) -> usize {
            members / 2 + 1
        }
        fn phase2_quorum_size(&self, members: usize) -> usize {
            members / 2 + 1
        }
        fn admits(&self, members: usize) -> bool {
            members == 4
        }
        fn is_phase1_quorum_in<I: Ord>(
            &self,
            members: &[I],
            voters: &BTreeSet<I>,
            _row: Option<usize>,
        ) -> bool {
            QuorumSystem::is_majority(members, voters)
        }
        fn is_phase2_quorum_in<I: Ord>(
            &self,
            members: &[I],
            voters: &BTreeSet<I>,
            _column: Option<usize>,
        ) -> bool {
            QuorumSystem::is_majority(members, voters)
        }
        fn phase1_addressees<I: Copy>(&self, members: &[I], row: Option<usize>) -> Vec<I> {
            row.map_or_else(|| members.to_vec(), |r| members[r * 2..r * 2 + 2].to_vec())
        }
        fn phase2_addressees<I: Copy>(&self, members: &[I], column: Option<usize>) -> Vec<I> {
            column.map_or_else(|| members.to_vec(), |c| members[c * 2..c * 2 + 2].to_vec())
        }
        fn row_of(&self, ctx: u64) -> Option<usize> {
            Some(usize::try_from(ctx % 2).unwrap_or(0))
        }
        fn column_of(&self, slot: crate::types::Slot) -> Option<usize> {
            Some(usize::try_from(slot.0 % 2).unwrap_or(0))
        }
        fn column_count(&self) -> Option<usize> {
            Some(2)
        }
    }

    #[test]
    fn a_read_row_that_misses_a_write_column_is_refused() {
        assert!(MisroutedHalves.cross_intersects(4));
        assert_eq!(
            Config::check_quorums(&MisroutedHalves, 4),
            Err(ConfigError::ReadRowMissesWriteColumn {
                row: Some(0),
                column: Some(1),
            })
        );
        // The same rule on the wire's grid: rows and columns meet by
        // geometry.
        assert_eq!(
            Config::check_quorums(&QuorumSystem::Grid { rows: 2, cols: 2 }, 4),
            Ok(())
        );
    }
}
