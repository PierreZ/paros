use std::collections::BTreeSet;

#[cfg(doc)]
use super::AcceptorConfig;
use super::quorum_system::{counted, majority_of};
use super::{MatchmakerId, QuorumSystem, ShapeError};

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
        assert!(
            self.0 == u64::MAX || next > self,
            "a successor generation is strictly newer"
        );
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
#[cfg_attr(feature = "serde", serde(try_from = "SerdeMatchmakerSet"))]
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
impl TryFrom<SerdeMatchmakerSet> for MatchmakerSet {
    type Error = ShapeError;

    /// A set off the wire is an input: an empty one is refused with a
    /// decode error, never a panic.
    fn try_from(wire: SerdeMatchmakerSet) -> Result<Self, ShapeError> {
        Self::try_new(wire.generation, wire.members)
    }
}

impl MatchmakerSet {
    /// A set of `members` (sorted and deduplicated here) for `generation`:
    /// [`MatchmakerSet::try_new`] for a value the program builds itself.
    ///
    /// # Panics
    ///
    /// If [`MatchmakerSet::try_new`] refuses it: a set with no matchmaker.
    /// The panic text is the [`ShapeError`]'s rule.
    #[must_use]
    pub fn new(generation: MatchmakerGeneration, members: Vec<MatchmakerId>) -> Self {
        Self::try_new(generation, members).unwrap_or_else(|e| panic!("{e}"))
    }

    /// A set of `members` (sorted and deduplicated here) for `generation`,
    /// or the rule it breaks. The form for an input: a set from the wire or
    /// from an operator.
    ///
    /// # Errors
    ///
    /// [`ShapeError::NoMember`] if `members` is empty: a set with no
    /// matchmaker can never form a quorum.
    ///
    /// # Panics
    ///
    /// If the normalized set is not well formed
    /// ([`MatchmakerSet::is_well_formed`]): asserted here, once, at the only
    /// construction site, never per tally.
    pub fn try_new(
        generation: MatchmakerGeneration,
        mut members: Vec<MatchmakerId>,
    ) -> Result<Self, ShapeError> {
        members.sort_unstable();
        members.dedup();
        if members.is_empty() {
            return Err(ShapeError::NoMember);
        }
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
        Ok(set)
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
    /// B, and between the decree's two phases) is made only under it. Another
    /// matchmaker quorum system would have to replace this method *and* the
    /// decree kernel together, never one without the other.
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

    /// Whether `voters` hold **every** member of this set: what a
    /// retirement needs (#1002 (retire fence gaps), decided on 2026-11-12),
    /// because a registration may still reach a matchmaker outside any
    /// quorum that answered. A voter outside the set never counts.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error.
    #[must_use]
    pub fn has_all(&self, voters: &BTreeSet<MatchmakerId>) -> bool {
        let all = self.members.iter().all(|mm| voters.contains(mm));
        if all {
            assert!(
                self.has_quorum(voters),
                "every member is a matchmaker quorum"
            );
        }
        all
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
    /// set qualifies; the check is the explicit invariant another matchmaker
    /// quorum system would have to satisfy too.
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

    /// A set read from the wire goes through the constructor. Its members
    /// are sorted and deduplicated, and its generation stays.
    #[cfg(feature = "serde")]
    #[test]
    fn a_wire_set_is_normalized_like_a_built_one() {
        let wire = super::SerdeMatchmakerSet {
            generation: MatchmakerGeneration(4),
            members: [3, 1, 3, 2].into_iter().map(MatchmakerId).collect(),
        };
        let set = MatchmakerSet::try_from(wire).expect("a non-empty wire set decodes");
        assert_eq!(set.generation, MatchmakerGeneration(4));
        assert_eq!(set.members(), [1, 2, 3].map(MatchmakerId).as_slice());
    }

    /// Only a non-empty, sorted and deduplicated membership is well formed.
    /// The constructor normalizes, so the bad shapes are built by hand here.
    #[test]
    fn only_a_sorted_deduplicated_nonempty_set_is_well_formed() {
        let raw = |members: &[u64]| MatchmakerSet {
            generation: MatchmakerGeneration(0),
            members: members.iter().copied().map(MatchmakerId).collect(),
        };
        assert!(raw(&[1]).is_well_formed());
        assert!(raw(&[1, 2, 3]).is_well_formed());
        assert!(!raw(&[]).is_well_formed(), "empty");
        assert!(!raw(&[2, 1]).is_well_formed(), "unsorted");
        assert!(!raw(&[1, 1]).is_well_formed(), "duplicated");
    }

    /// A generation's successor is one above it, and the last generation
    /// saturates.
    #[test]
    fn a_generation_moves_up_by_one() {
        assert_eq!(MatchmakerGeneration(0).next(), MatchmakerGeneration(1));
        assert_eq!(
            MatchmakerGeneration(u64::MAX).next(),
            MatchmakerGeneration(u64::MAX)
        );
    }

    /// A set with no matchmaker can never form a quorum. The wire path
    /// refuses it with a decode error, never a panic.
    #[cfg(feature = "serde")]
    #[test]
    fn an_empty_wire_set_is_refused() {
        let refused = MatchmakerSet::try_from(super::SerdeMatchmakerSet {
            generation: MatchmakerGeneration(0),
            members: Vec::new(),
        });
        assert_eq!(refused, Err(super::ShapeError::NoMember));
        let kept = MatchmakerSet::try_from(super::SerdeMatchmakerSet {
            generation: MatchmakerGeneration(2),
            members: vec![MatchmakerId(3), MatchmakerId(1), MatchmakerId(3)],
        })
        .expect("a non-empty set is kept");
        assert_eq!(kept.members(), [MatchmakerId(1), MatchmakerId(3)]);
        assert_eq!(kept.generation, MatchmakerGeneration(2));
    }

    /// The fallible constructor refuses an empty set and builds the same set
    /// as `new` otherwise.
    #[test]
    fn try_new_refuses_only_an_empty_set() {
        assert_eq!(
            MatchmakerSet::try_new(MatchmakerGeneration(0), Vec::new()),
            Err(super::ShapeError::NoMember)
        );
        let members = vec![MatchmakerId(2), MatchmakerId(1)];
        assert_eq!(
            MatchmakerSet::try_new(MatchmakerGeneration(1), members.clone()),
            Ok(MatchmakerSet::new(MatchmakerGeneration(1), members))
        );
    }
}
