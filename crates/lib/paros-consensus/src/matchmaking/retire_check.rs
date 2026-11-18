//! The [`RetireCheck`]: a node asks every matchmaker whether it may retire.
//! The [`RejoinCheck`]: a node that comes back asks every matchmaker to drop
//! its retirement record.

use std::collections::BTreeSet;

use crate::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::types::Ballot;

/// A **retirement check** (#1002 (retire fence gaps), decided on
/// 2026-11-12): the tally of a node outside the configuration in force
/// that asks every matchmaker of the set whether a registration at or above
/// the GC floor still names it.
///
/// Each matchmaker that finds none records the node as retired, durably,
/// before it answers, and refuses every later registration naming it
/// ([`MatchRefusal::Retired`](crate::matchmaker::MatchRefusal::Retired)).
/// The node retires only once **every** matchmaker agreed. A quorum is not
/// enough: a slow campaign's registration may reach a matchmaker outside
/// the quorum that answered, after the answers, and every later matchmaker
/// quorum would then hold a configuration whose Phase 1 needs this node.
/// The TLA+ model of PR #1021 found that trace; with every matchmaker
/// agreeing, no matchmaker can take such a registration any more.
///
/// The cost: a retirement waits until every matchmaker of the set answers
/// (a lost matchmaker is first replaced by a handover). A node that retired
/// is named again only after it came back and rejoined ([`RejoinCheck`]).
///
/// Agreements never add up across matchmaker generations: a check that
/// meets a later generation restarts with no agreement
/// ([`RetireCheck::agree`]), so every matchmaker of one set agreed.
#[derive(Clone, Debug)]
pub struct RetireCheck {
    /// The GC floor the node retires on.
    watermark: Ballot,
    /// The node's incarnation the check asks for.
    incarnation: u64,
    /// The matchmakers that agreed: no registration at or above the floor
    /// names the node, and it is recorded as retired there.
    agreements: Agreements,
}

impl RetireCheck {
    /// Open a check on the probe tagged `tag`, for the floor `watermark`, in
    /// the node's incarnation `incarnation`, under matchmaker generation
    /// `generation`.
    #[must_use]
    pub fn new(
        tag: Ballot,
        watermark: Ballot,
        incarnation: u64,
        generation: MatchmakerGeneration,
    ) -> Self {
        Self {
            watermark,
            incarnation,
            agreements: Agreements::new(tag, generation),
        }
    }

    /// The matchmaker generation the agreements came from.
    #[must_use]
    pub fn generation(&self) -> MatchmakerGeneration {
        self.agreements.generation
    }

    /// The tag of the probe the check rides on.
    #[must_use]
    pub fn tag(&self) -> Ballot {
        self.agreements.tag
    }

    /// The GC floor the node retires on.
    #[must_use]
    pub fn watermark(&self) -> Ballot {
        self.watermark
    }

    /// The node's incarnation the check asks for.
    #[must_use]
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Fold `matchmaker`'s answer, given under matchmaker generation
    /// `generation`: whether it counted. A later generation restarts the
    /// tally; an earlier one is ignored.
    pub fn agree(&mut self, matchmaker: MatchmakerId, generation: MatchmakerGeneration) -> bool {
        self.agreements.agree(matchmaker, generation)
    }

    /// Whether every matchmaker of `matchmakers` answered.
    #[must_use]
    pub fn all_agreed(&self, matchmakers: &MatchmakerSet) -> bool {
        self.agreements.all_agreed(matchmakers)
    }

    /// The matchmakers of `matchmakers` that have not answered yet.
    #[must_use]
    pub fn unanswered(&self, matchmakers: &MatchmakerSet) -> Vec<MatchmakerId> {
        self.agreements.unanswered(matchmakers)
    }
}

/// A **rejoin** (#1037 (rejoin)): the tally of a node that comes back to the
/// journal with its store after it retired, a later booking having started
/// the journal on it again. It asks every matchmaker of the set to drop its
/// retirement record of every earlier incarnation
/// ([`MatchPurpose::Rejoin`](crate::matchmaker::MatchPurpose::Rejoin)), so
/// a reconfiguration that names the node registers again. Until every
/// matchmaker answered, the node asks again; it opens no retirement check.
#[derive(Clone, Debug)]
pub struct RejoinCheck {
    /// The node's new incarnation.
    incarnation: u64,
    /// The matchmakers that took the rejoin.
    agreements: Agreements,
}

impl RejoinCheck {
    /// Open a rejoin on the probe tagged `tag`, at `incarnation`, under
    /// matchmaker generation `generation`.
    ///
    /// # Panics
    ///
    /// If `incarnation` is zero: a rejoin follows a retirement.
    #[must_use]
    pub fn new(tag: Ballot, incarnation: u64, generation: MatchmakerGeneration) -> Self {
        assert!(incarnation > 0, "a rejoin follows a retirement");
        Self {
            incarnation,
            agreements: Agreements::new(tag, generation),
        }
    }

    /// The tag of the probe the rejoin rides on.
    #[must_use]
    pub fn tag(&self) -> Ballot {
        self.agreements.tag
    }

    /// The matchmaker generation the answers came from.
    #[must_use]
    pub fn generation(&self) -> MatchmakerGeneration {
        self.agreements.generation
    }

    /// The node's new incarnation.
    #[must_use]
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Fold `matchmaker`'s answer, given under matchmaker generation
    /// `generation`: whether it counted. A later generation restarts the
    /// tally; an earlier one is ignored.
    pub fn agree(&mut self, matchmaker: MatchmakerId, generation: MatchmakerGeneration) -> bool {
        self.agreements.agree(matchmaker, generation)
    }

    /// Whether every matchmaker of `matchmakers` answered.
    #[must_use]
    pub fn all_agreed(&self, matchmakers: &MatchmakerSet) -> bool {
        self.agreements.all_agreed(matchmakers)
    }

    /// The matchmakers of `matchmakers` that have not answered yet.
    #[must_use]
    pub fn unanswered(&self, matchmakers: &MatchmakerSet) -> Vec<MatchmakerId> {
        self.agreements.unanswered(matchmakers)
    }
}

/// The answers of every matchmaker of one generation to the requests of
/// one probe tag: what a [`RetireCheck`] and a [`RejoinCheck`] tally.
#[derive(Clone, Debug)]
struct Agreements {
    /// The tag of the probe the requests ride on.
    tag: Ballot,
    /// The matchmaker generation every agreement in `agreed` came from.
    generation: MatchmakerGeneration,
    /// The matchmakers that agreed.
    agreed: BTreeSet<MatchmakerId>,
}

impl Agreements {
    fn new(tag: Ballot, generation: MatchmakerGeneration) -> Self {
        Self {
            tag,
            generation,
            agreed: BTreeSet::new(),
        }
    }

    /// Fold `matchmaker`'s agreement, given under matchmaker generation
    /// `generation`. Returns whether it counted: a second one from the same
    /// matchmaker is ignored (wire input). An agreement from a later
    /// generation restarts the check: the earlier generation's agreements
    /// are dropped. One from an earlier generation is ignored.
    ///
    /// # Panics
    ///
    /// If the agreement is not held afterwards (a programmer error).
    pub fn agree(&mut self, matchmaker: MatchmakerId, generation: MatchmakerGeneration) -> bool {
        if generation < self.generation {
            return false;
        }
        if generation > self.generation {
            self.generation = generation;
            self.agreed.clear();
        }
        assert!(
            self.generation == generation,
            "every agreement comes from one generation"
        );
        let fresh = self.agreed.insert(matchmaker);
        assert!(self.agreed.contains(&matchmaker), "an agreement is held");
        fresh
    }

    /// Whether every matchmaker of `matchmakers` agreed — asked at the
    /// membership boundary, never as a count.
    #[must_use]
    pub fn all_agreed(&self, matchmakers: &MatchmakerSet) -> bool {
        matchmakers.has_all(&self.agreed)
    }

    /// The matchmakers of `matchmakers` that have not agreed yet: whom a
    /// re-ask addresses.
    ///
    /// # Panics
    ///
    /// If an agreed matchmaker is listed (a programmer error).
    #[must_use]
    pub fn unanswered(&self, matchmakers: &MatchmakerSet) -> Vec<MatchmakerId> {
        let unanswered: Vec<MatchmakerId> = matchmakers.unanswered(&self.agreed).collect();
        assert!(
            unanswered.iter().all(|m| !self.agreed.contains(m)),
            "an agreed matchmaker is never asked again"
        );
        unanswered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(generation: u64, ids: &[u64]) -> MatchmakerSet {
        MatchmakerSet::new(
            g(generation),
            ids.iter().map(|&i| MatchmakerId(i)).collect(),
        )
    }

    fn g(generation: u64) -> MatchmakerGeneration {
        MatchmakerGeneration(generation)
    }

    #[test]
    fn agreements_do_not_add_up_across_generations() {
        let tag = Ballot {
            round: 4,
            node: crate::types::NodeId(1),
        };
        let mut check = RetireCheck::new(tag, Ballot::zero(), 0, g(1));
        assert!(check.agree(MatchmakerId(1), g(1)));
        assert!(check.agree(MatchmakerId(2), g(1)));
        // Generation 2 keeps matchmakers 1 and 2 and adds 3. Matchmaker 3
        // agrees under it: the check restarts, so 1 and 2 must agree again.
        assert!(check.agree(MatchmakerId(3), g(2)));
        assert_eq!(check.generation(), g(2));
        assert!(!check.all_agreed(&set(2, &[1, 2, 3])));
        assert_eq!(
            check.unanswered(&set(2, &[1, 2, 3])),
            vec![MatchmakerId(1), MatchmakerId(2)]
        );
        // A late agreement from generation 1 is ignored.
        assert!(!check.agree(MatchmakerId(1), g(1)));
        assert!(!check.all_agreed(&set(2, &[1, 2, 3])));
        assert!(check.agree(MatchmakerId(1), g(2)));
        assert!(check.agree(MatchmakerId(2), g(2)));
        assert!(check.all_agreed(&set(2, &[1, 2, 3])));
    }

    #[test]
    fn a_repeated_agreement_counts_once() {
        let tag = Ballot {
            round: 1,
            node: crate::types::NodeId(1),
        };
        let mut check = RetireCheck::new(tag, Ballot::zero(), 0, g(0));
        assert!(check.agree(MatchmakerId(1), g(0)));
        assert!(!check.agree(MatchmakerId(1), g(0)));
        assert!(!check.all_agreed(&set(0, &[1, 2])));
    }
}
