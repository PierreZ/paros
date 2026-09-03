//! **Membership**: the data every quorum question is asked over — an
//! acceptor configuration, a matchmaker set, and the quorum system that says
//! which subsets of a membership count.
//!
//! This is the boundary the rest of the core reasons through and never
//! around: the proposer, the read rounds, `CheckQuorum` and the GC fence all
//! ask [`AcceptorConfig::has_quorum`], which asks [`QuorumSystem::is_quorum`];
//! no tally compares a count against a threshold on its own. Today the one
//! quorum system is a majority. Flexible quorums, grids and the
//! compartmentalized deployments are new variants of [`QuorumSystem`] and
//! new data in a configuration — never a rewrite of the tallies.
//!
//! Matchmaker quorums are deliberately **not** parameterized
//! ([`MatchmakerSet::quorum_size`] is a majority by construction): the
//! generation handover's safety argument is made under the majority model
//! alone.

use std::collections::BTreeSet;

use crate::types::NodeId;

/// The quorum system a configuration uses: which sets of acceptors count as a
/// quorum for Phase 1 (election) and Phase 2 (decide).
///
/// Carried as a *value* in [`crate::Config`] from the start (even though there is only
/// ever one variant today) so that Matchmaker reconfiguration (Stage 9) is a
/// *data* change — a different quorum system per round — rather than a rewrite of
/// the election/decide logic. Paxos safety rests on every Phase-1 quorum
/// intersecting every Phase-2 quorum; a simple majority satisfies that trivially.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

    /// Whether `voters` form a quorum over the sorted membership `members`.
    /// The **one** predicate every tally asks — Phase-1 completion, a
    /// Phase-2 decision, a read-index confirmation, `CheckQuorum`, the GC
    /// fence — so a quorum system that is not a cardinality (a grid, a
    /// flexible split) answers here with set membership and no tally ever
    /// compares a count against a threshold on its own. A voter outside
    /// `members` never counts.
    #[must_use]
    pub fn is_quorum(self, members: &[NodeId], voters: &BTreeSet<NodeId>) -> bool {
        match self {
            QuorumSystem::Majority => {
                let counted = voters
                    .iter()
                    .filter(|v| members.binary_search(v).is_ok())
                    .count();
                counted >= self.quorum_size(members.len())
            }
        }
    }
}

/// An acceptor configuration as registered with a matchmaker: a membership
/// plus the quorum system in force over it — [`crate::Config`] minus the
/// per-node `id`. The core never interprets it beyond storing and reporting it;
/// the leader-side matchmaking phase is what runs Phase 1 against it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AcceptorConfig {
    /// The full membership, sorted and deduplicated (a [`Vec`] keeps iteration
    /// deterministic without a map).
    pub members: Vec<NodeId>,
    /// The quorum system election and decide consult over `members`.
    pub quorum_system: QuorumSystem,
}

impl AcceptorConfig {
    /// A configuration over `members` (sorted and deduplicated here) under
    /// `quorum_system`.
    ///
    /// # Panics
    ///
    /// If `members` is empty: a configuration with no acceptor can never form
    /// a quorum, so registering one is a programmer error.
    #[must_use]
    pub fn new(mut members: Vec<NodeId>, quorum_system: QuorumSystem) -> Self {
        members.sort_unstable();
        members.dedup();
        assert!(
            !members.is_empty(),
            "an acceptor configuration names at least one acceptor"
        );
        Self {
            members,
            quorum_system,
        }
    }

    /// The number of acceptors that form a quorum over this configuration.
    ///
    /// # Panics
    ///
    /// If the quorum system cannot self-intersect over this membership: Paxos
    /// safety rests on any two quorums of one configuration sharing an
    /// acceptor (for the majority system, `2q > n`), and a configuration that
    /// breaks it must fail loudly rather than let two values be chosen for
    /// one slot.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        let n = self.members.len();
        let q = self.quorum_system.quorum_size(n);
        assert!(q >= 1, "a quorum requires at least one acceptor");
        assert!(2 * q > n, "any two quorums must intersect");
        q
    }

    /// Whether this configuration can be run at all: at least one acceptor,
    /// and a quorum system whose quorums all intersect (`2q > n`). The
    /// operating-condition twin of [`AcceptorConfig::quorum_size`]'s hard
    /// asserts: a boundary that takes a configuration from outside
    /// (`RawNode::reconfigure`) refuses a malformed one here instead of
    /// letting a later quorum tally panic on it.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        let n = self.members.len();
        if n == 0 {
            return false;
        }
        let q = self.quorum_system.quorum_size(n);
        q >= 1 && 2 * q > n
    }

    /// Whether `voters` hold a quorum of this configuration under its quorum
    /// system — the only way a tally over this configuration is ever
    /// judged ([`QuorumSystem::is_quorum`]); a voter outside the membership
    /// never counts.
    ///
    /// # Panics
    ///
    /// If the configuration is not well formed (see
    /// [`AcceptorConfig::quorum_size`]): a tally over a configuration whose
    /// quorums do not intersect is meaningless and must fail loudly.
    #[must_use]
    pub fn has_quorum(&self, voters: &BTreeSet<NodeId>) -> bool {
        assert!(
            self.is_well_formed(),
            "a quorum tally runs over a well-formed configuration"
        );
        self.quorum_system.is_quorum(&self.members, voters)
    }

    /// Whether `node` is a member of this configuration.
    #[must_use]
    pub fn contains(&self, node: NodeId) -> bool {
        self.members.binary_search(&node).is_ok()
    }
}

/// Stable identity of a matchmaker within the matchmaker pool. A distinct
/// namespace from [`NodeId`]: a matchmaker is not an acceptor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerId(pub u64);

/// A matchmaker-set **generation** (#125): which matchmaker set is
/// authoritative. Distinct from a Paxos ballot (consensus leadership, and the
/// acceptor configuration bound to it) and from [`crate::ConfigId`] (the
/// durable cluster-configuration tag). Generation 0 is the bootstrap set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerGeneration(pub u64);

impl MatchmakerGeneration {
    /// The next generation.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// A matchmaker set bound to its generation: the value the successor decree
/// chooses, and what every matchmaking message is fenced by.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerSet {
    /// The generation this set is authoritative for.
    pub generation: MatchmakerGeneration,
    /// The members, sorted and deduplicated.
    pub members: Vec<MatchmakerId>,
}

impl MatchmakerSet {
    /// A set of `members` (sorted and deduplicated here) for `generation`.
    #[must_use]
    pub fn new(generation: MatchmakerGeneration, mut members: Vec<MatchmakerId>) -> Self {
        members.sort_unstable();
        members.dedup();
        Self {
            generation,
            members,
        }
    }

    /// The matchmaker quorum over this set: a majority, so any two
    /// registration quorums (and any two stop / decree quorums) intersect.
    ///
    /// **Majority quorums only.** Matchmaker Paxos generalizes matchmaker
    /// quorums to arbitrary quorum systems; paros deliberately does not. Every
    /// matchmaker-side quorum — registration, GC ack, freeze, the successor
    /// decree over `M_g` ([`crate::DecreeProposer`] derives the same majority
    /// from the acceptor set it is handed) and publication — is this rule,
    /// and the generation handover's safety argument (quorum intersection
    /// between the freeze quorum and every completed registration, Appendix
    /// B, and between the decree's two phases) is made only under it. A
    /// flexible matchmaker quorum system would have to replace this method
    /// *and* the decree kernel together, never one without the other.
    ///
    /// # Panics
    ///
    /// If the majority does not self-intersect over the membership (a
    /// programmer error: the arithmetic guarantees it).
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        assert!(
            self.is_well_formed(),
            "a matchmaker quorum is drawn over a well-formed set"
        );
        let quorum = self.members.len() / 2 + 1;
        // Postcondition: self-intersecting over the membership.
        assert!(
            quorum * 2 > self.members.len(),
            "a matchmaker quorum is a majority"
        );
        quorum
    }

    /// Whether `id` is a member.
    #[must_use]
    pub fn contains(&self, id: MatchmakerId) -> bool {
        self.members.binary_search(&id).is_ok()
    }

    /// Whether this set can serve as a matchmaker configuration at all: it
    /// names at least one matchmaker and admits the quorum system every
    /// matchmaker-side quorum is drawn from (majority: any two quorums
    /// intersect, `2q > n`). **A chosen `MatchmakerSet` must itself admit
    /// the required quorum system** — the protocol boundaries that take a
    /// set from outside refuse one that does not ([`MatchmakerReconfigurer::start`]
    /// refuses the target, a matchmaker refuses a `Bootstrap` or `Chosen`
    /// naming it), and the boundaries that *produce* one assert it (a
    /// `finish` proposes the members that answered the freeze — at least a
    /// quorum of the old set, never fewer). Under the majority system every
    /// non-empty set qualifies; the check is the explicit invariant a
    /// flexible matchmaker quorum system would have to satisfy too.
    ///
    /// [`MatchmakerReconfigurer::start`]: crate::MatchmakerReconfigurer::start
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        let n = self.members.len();
        if n == 0 {
            return false;
        }
        let q = n / 2 + 1;
        2 * q > n && self.members.windows(2).all(|w| w[0] < w[1])
    }
}
