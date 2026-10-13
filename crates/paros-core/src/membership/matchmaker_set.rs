use std::collections::BTreeSet;

#[cfg(doc)]
use super::AcceptorConfig;
use super::quorum_system::{counted, majority_of};
use super::{MatchmakerId, QuorumSystem};

/// A matchmaker-set **generation** (#125): which matchmaker set is
/// authoritative. Distinct from a Paxos ballot (consensus leadership, and the
/// acceptor configuration bound to it). Generation 0 is the bootstrap set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchmakerGeneration(pub u64);

impl MatchmakerGeneration {
    /// The next generation.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn next(self) -> Self {
        let next = Self(self.0.saturating_add(1));
        assert!(next >= self, "a generation never moves back");
        if self.0 < u64::MAX {
            assert!(next > self, "a successor generation is strictly newer");
        }
        next
    }
}

/// A matchmaker set bound to its generation: the value the successor decree
/// chooses, and what every matchmaking message is fenced by.
///
/// **The membership is private and [`MatchmakerSet::new`] is the only way to
/// build one**, deserialisation included (see `SerdeMatchmakerSet`), for the
/// reason [`AcceptorConfig`] gives: [`MatchmakerSet::contains`] and every
/// quorum tally binary-search the membership, so an unsorted, duplicated or
/// empty vector would not fail, it would make a tally *silently miscount*.
/// Only `new` normalizes and asserts [`MatchmakerSet::is_well_formed`], so
/// only `new` may construct — and a deployment with no matchmakers holds no
/// `MatchmakerSet` at all (`ColocatedNode::matchmaker_set` is `None` there)
/// rather than an empty one.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(from = "SerdeMatchmakerSet"))]
pub struct MatchmakerSet {
    /// The generation this set is authoritative for.
    pub generation: MatchmakerGeneration,
    /// The members, sorted and deduplicated.
    members: Vec<MatchmakerId>,
}

/// The wire shape [`MatchmakerSet`] deserialises through, so a serialized set
/// is normalized and checked by [`MatchmakerSet::new`] exactly like a
/// constructed one.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct SerdeMatchmakerSet {
    generation: MatchmakerGeneration,
    members: Vec<MatchmakerId>,
}

#[cfg(feature = "serde")]
impl From<SerdeMatchmakerSet> for MatchmakerSet {
    fn from(wire: SerdeMatchmakerSet) -> Self {
        Self::new(wire.generation, wire.members)
    }
}

impl MatchmakerSet {
    /// A set of `members` (sorted and deduplicated here) for `generation`.
    ///
    /// # Panics
    ///
    /// If `members` is empty: a set with no matchmaker can never form a
    /// quorum, so a generation naming one is a programmer error. Also if the
    /// normalized set is not well formed ([`MatchmakerSet::is_well_formed`])
    /// — asserted here, once, at the only construction site, never per
    /// tally.
    #[must_use]
    pub fn new(generation: MatchmakerGeneration, mut members: Vec<MatchmakerId>) -> Self {
        members.sort_unstable();
        members.dedup();
        assert!(
            !members.is_empty(),
            "a matchmaker set names at least one matchmaker"
        );
        let set = Self {
            generation,
            members,
        };
        assert!(
            set.is_well_formed(),
            "a matchmaker set admits the matchmaker quorum system"
        );
        assert!(
            set.quorum_size() <= set.members.len(),
            "a matchmaker quorum fits the set"
        );
        set
    }

    /// The members, sorted and deduplicated.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn members(&self) -> &[MatchmakerId] {
        assert!(
            !self.members.is_empty(),
            "a matchmaker set names a matchmaker"
        );
        &self.members
    }

    /// The size of a matchmaker quorum over this set: a majority. Kept for
    /// the one thing a predicate cannot answer — how many more acks a
    /// pending tally still waits for (`remaining:`). Whether a tally *holds*
    /// is always [`MatchmakerSet::has_quorum`].
    ///
    /// **Majority quorums only.** Matchmaker Paxos generalizes matchmaker
    /// quorums to arbitrary quorum systems; paros deliberately does not. Every
    /// matchmaker-side quorum — registration, GC ack, freeze, the successor
    /// decree over `M_g` (whose `Decree` builds the same majority
    /// from the set it replaces) and publication — is this rule,
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
        let quorum = majority_of(self.members.len());
        // Postcondition: self-intersecting over the membership.
        assert!(
            quorum * 2 > self.members.len(),
            "a matchmaker quorum is a majority"
        );
        quorum
    }

    /// Whether `voters` hold a matchmaker quorum of this set — the only way
    /// a matchmaker-side tally is ever judged. Routes to
    /// [`QuorumSystem::is_majority`], the one quorum model paros supports for
    /// matchmakers (see [`MatchmakerSet::quorum_size`]); a voter outside the
    /// set never counts.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn has_quorum(&self, voters: &BTreeSet<MatchmakerId>) -> bool {
        let quorum = QuorumSystem::is_majority(&self.members, voters);
        if quorum {
            assert!(
                counted(&self.members, voters) >= self.quorum_size(),
                "a matchmaker quorum is a majority of members"
            );
        }
        quorum
    }

    /// How many more answers a tally holding `voters` still waits for — the
    /// one thing a predicate cannot report. Every tally over a voter set asks
    /// here; the reconfigurer's stop phase, which keys its acks by matchmaker,
    /// spells the same subtraction itself. Zero once [`Self::has_quorum`]
    /// holds over voters drawn from this set.
    ///
    /// # Panics
    ///
    /// If the set is not well formed ([`Self::quorum_size`]).
    #[must_use]
    pub fn remaining(&self, voters: &BTreeSet<MatchmakerId>) -> usize {
        let remaining = self.quorum_size().saturating_sub(voters.len());
        if self.has_quorum(voters) {
            assert!(remaining == 0, "a held quorum waits for nothing more");
        }
        assert!(
            remaining <= self.quorum_size(),
            "a tally never owes more than a quorum"
        );
        remaining
    }

    /// The members not among `voters`, in id order — whom a tally's re-send
    /// addresses.
    pub fn unanswered<'a>(
        &'a self,
        voters: &'a BTreeSet<MatchmakerId>,
    ) -> impl Iterator<Item = MatchmakerId> + 'a {
        self.members
            .iter()
            .copied()
            .filter(|mm| !voters.contains(mm))
    }

    /// Whether `id` is a member.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn contains(&self, id: MatchmakerId) -> bool {
        let member = self.members.binary_search(&id).is_ok();
        assert!(
            self.members.windows(2).all(|w| w[0] < w[1]),
            "a matchmaker set is sorted and deduplicated"
        );
        member
    }

    /// Whether this set can serve as a matchmaker configuration at all: it
    /// names at least one matchmaker, sorted and deduplicated, and admits the
    /// quorum system every matchmaker-side quorum is drawn from (majority:
    /// any two quorums intersect, `2q > n`). **A chosen `MatchmakerSet` must
    /// itself admit the required quorum system**, and since
    /// [`MatchmakerSet::new`] is the only constructor and asserts this, every
    /// set that exists — the one a `start` targets, the one a `Bootstrap` or
    /// `Chosen` carries, the one a `finish` proposes from the members that
    /// answered the freeze — does. Under the majority system every non-empty
    /// set qualifies; the check is the explicit invariant a flexible
    /// matchmaker quorum system would have to satisfy too.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        let n = self.members.len();
        if n == 0 {
            return false;
        }
        QuorumSystem::Majority.cross_intersects(n) && self.members.windows(2).all(|w| w[0] < w[1])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};

    /// Matchmaker quorums are majorities only, whatever the acceptor side
    /// runs: `MatchmakerSet` holds no quorum system at all.
    #[test]
    fn matchmaker_quorums_stay_majorities() {
        let set = MatchmakerSet::new(MatchmakerGeneration(0), (1..=4).map(MatchmakerId).collect());
        assert_eq!(set.quorum_size(), 3);
        let two: BTreeSet<MatchmakerId> = [1, 2].into_iter().map(MatchmakerId).collect();
        let three: BTreeSet<MatchmakerId> = [1, 2, 3].into_iter().map(MatchmakerId).collect();
        assert!(!set.has_quorum(&two));
        assert!(set.has_quorum(&three));
    }
}
