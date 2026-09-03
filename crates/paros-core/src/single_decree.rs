//! A **single-decree Paxos kernel**: one value chosen once, over an acceptor
//! set the caller names. Generic over the acceptor identity `A` and the value
//! `V`, sans-IO like everything else here — the caller carries the messages.
//!
//! # Why a kernel beside [`crate::RawNode`], not extracted from it
//!
//! [`crate::RawNode`] runs single-decree Paxos per log slot, but its Phase 1
//! is inseparable from what makes it Multi-Paxos: one `Prepare` per ballot
//! over a whole log suffix, paged `Promise`s, the CTRL tri-state
//! (`have`/`none`/`faulty`), the cross-configuration completion predicate, the
//! no-op gap fill, the read fence, and the bounded leader recovery. Pulling a
//! `SingleDecree` out of that would mean re-deriving the proven core around a
//! kernel it never had, for the benefit of one consumer — the matchmaker-set
//! reconfiguration of #125, which needs exactly the classic protocol and
//! nothing of the above. The deliberate decision (recorded in
//! `docs/analysis/consensus/matchmaker-gc-and-generations.md`) is therefore a
//! **separate, tiny kernel**: the two roles below are the whole of "Paxos
//! made simple" §2.2, with the value-selection rule (adopt the highest-ballot
//! vote, else propose your own) in one place, unit-tested against the
//! dueling-proposer case. The matchmaker embeds the acceptor half in its
//! durable scalars; the reconfigurer drives the proposer half.
//!
//! What is *not* here: leadership, retries, timeouts, and message transport —
//! the caller decides when to re-send and when to retry at a higher ballot
//! (a [`DecreeProposer::on_nack`] tells it what to open above).

use std::collections::BTreeSet;

use crate::types::Ballot;

/// The P2c value-selection fold, shared by every Phase-1 tally in the crate:
/// [`DecreeProposer::on_promise`] over one decree, and the log proposer's
/// per-slot merge over a promise page. Keep the highest-ballot report, ignore
/// a lower one, and **assert** that two reports at one ballot agree — one
/// ballot has exactly one proposer (P2b), so a disagreement is a protocol
/// violation, not a tie to break. Silently keeping the first of two would let
/// two proposers with different arrival orders select different values at one
/// ballot, which is the one thing the rule exists to prevent.
///
/// The caller supplies its own assertion message: the two tallies name
/// different coordinates (a slot's command, a decree's value) and both
/// messages are load-bearing.
///
/// # Panics
///
/// If two reports at one ballot disagree.
pub(crate) fn select_highest<V: PartialEq>(
    best: &mut Option<(Ballot, V)>,
    report: (Ballot, V),
    disagreement: &'static str,
) {
    let (ballot, value) = report;
    match best {
        Some((held, _)) if ballot < *held => {}
        Some((held, recorded)) if ballot == *held => {
            assert!(*recorded == value, "{disagreement}");
        }
        _ => *best = Some((ballot, value)),
    }
}

/// The acceptor half: the durable promise and the durable vote of one
/// acceptor for one decree. Both scalars are persisted whole by the caller
/// **before** the reply that reports them leaves (persist-before-reply, the
/// acceptor rule of [`crate::HardState`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DecreeAcceptor<V> {
    /// The highest ballot promised. Monotone.
    pub promised: Ballot,
    /// The highest-ballot value accepted, if any.
    pub vote: Option<(Ballot, V)>,
}

impl<V: Clone + PartialEq> DecreeAcceptor<V> {
    /// Phase 1b: promise `ballot` if it is at or above the promise held (an
    /// equal ballot is the same proposer asking again — answered
    /// idempotently), returning the vote to report; otherwise the promise
    /// that refuses it.
    ///
    /// # Errors
    ///
    /// The held promise, when it dominates `ballot` (a `Nack`).
    pub fn prepare(&mut self, ballot: Ballot) -> Result<Option<(Ballot, V)>, Ballot> {
        if ballot < self.promised {
            return Err(self.promised);
        }
        self.promised = ballot;
        Ok(self.vote.clone())
    }

    /// Phase 2b: accept `value` at `ballot` if the promise allows, raising
    /// the promise to it (an accept is a promise too); otherwise the promise
    /// that refuses it.
    ///
    /// # Errors
    ///
    /// The held promise, when it dominates `ballot` (a `Nack`).
    ///
    /// # Panics
    ///
    /// If a second value arrives at an already-voted ballot: one ballot has
    /// one proposer, so that is a programmer error, never an operating
    /// condition.
    pub fn accept(&mut self, ballot: Ballot, value: V) -> Result<(), Ballot> {
        if ballot < self.promised {
            return Err(self.promised);
        }
        if let Some((voted, v)) = &self.vote
            && *voted == ballot
        {
            assert!(
                *v == value,
                "one ballot carries one value: a re-accept at the voted ballot is the same value"
            );
        }
        self.promised = ballot;
        self.vote = Some((ballot, value));
        Ok(())
    }
}

/// Where a [`DecreeProposer`] stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecreePhase<V> {
    /// Collecting Phase-1 promises.
    Phase1,
    /// A Phase-1 quorum holds: proposing `value` (the highest reported vote,
    /// or the proposer's own when nothing was voted) and collecting accepts.
    Phase2(V),
    /// A Phase-2 quorum holds: `value` is chosen.
    Chosen(V),
    /// A higher promise refused this ballot; the caller retries strictly
    /// above `promised`.
    Preempted(Ballot),
}

/// The proposer half: one ballot, one named acceptor set, the own proposal,
/// and the tallies. Every counting set is keyed by acceptor identity, so a
/// duplicated reply never inflates a quorum, and a reply from an identity
/// outside the acceptor set never counts at all.
///
/// **Quorum model.** The decree's Phase-1 and Phase-2 quorums are both a
/// **majority of the named acceptors**, derived here and nowhere else, so
/// any two quorums intersect — the whole of the P2c argument. This kernel
/// does not take a quorum *size*: a caller cannot instantiate a decree whose
/// quorums fail to intersect, or one whose quorum exceeds its acceptors.
/// Matchmaker Paxos generalizes matchmaker quorums to arbitrary systems;
/// paros deliberately supports **majority matchmaker quorums only**
/// (`MatchmakerSet::quorum_size` is the same rule), and the handover is
/// safe exactly under that model.
#[derive(Clone, Debug)]
pub struct DecreeProposer<A, V> {
    ballot: Ballot,
    acceptors: BTreeSet<A>,
    quorum: usize,
    proposal: V,
    promised_by: BTreeSet<A>,
    best: Option<(Ballot, V)>,
    accepted_by: BTreeSet<A>,
    phase: DecreePhase<V>,
}

impl<A: Ord + Copy, V: Clone + PartialEq> DecreeProposer<A, V> {
    /// Open a proposal of `proposal` at `ballot` over `acceptors`, with a
    /// majority of them as the quorum.
    ///
    /// # Panics
    ///
    /// If `acceptors` is empty: a decree with no acceptor is a programmer
    /// error.
    #[must_use]
    pub fn new(ballot: Ballot, acceptors: impl IntoIterator<Item = A>, proposal: V) -> Self {
        let acceptors: BTreeSet<A> = acceptors.into_iter().collect();
        assert!(
            !acceptors.is_empty(),
            "a decree needs at least one acceptor"
        );
        let quorum = acceptors.len() / 2 + 1;
        // Postcondition: the quorum self-intersects over the acceptor set.
        assert!(
            quorum * 2 > acceptors.len(),
            "a decree quorum is a majority"
        );
        assert!(
            quorum <= acceptors.len(),
            "a decree quorum fits its acceptors"
        );
        Self {
            ballot,
            acceptors,
            quorum,
            proposal,
            promised_by: BTreeSet::new(),
            best: None,
            accepted_by: BTreeSet::new(),
            phase: DecreePhase::Phase1,
        }
    }

    /// The ballot this proposal runs at.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// Where the proposal stands.
    #[must_use]
    pub fn phase(&self) -> &DecreePhase<V> {
        &self.phase
    }

    /// The value proposed if Phase 2 has opened (or is chosen).
    #[must_use]
    pub fn value(&self) -> Option<&V> {
        match &self.phase {
            DecreePhase::Phase2(v) | DecreePhase::Chosen(v) => Some(v),
            DecreePhase::Phase1 | DecreePhase::Preempted(_) => None,
        }
    }

    /// Whether the proposer adopted a prior vote instead of its own proposal
    /// (the P2c rule fired) — observability for the caller's audit.
    #[must_use]
    pub fn adopted_prior_vote(&self) -> bool {
        self.value().is_some_and(|v| *v != self.proposal)
    }

    /// The acceptor set this decree runs over.
    #[must_use]
    pub fn acceptors(&self) -> &BTreeSet<A> {
        &self.acceptors
    }

    /// The quorum size: a majority of the acceptors.
    #[must_use]
    pub fn quorum(&self) -> usize {
        self.quorum
    }

    /// Acceptors that have not promised yet (Phase 1) or not accepted yet
    /// (Phase 2) — what a re-send targets.
    pub fn unanswered(&self) -> impl Iterator<Item = A> + '_ {
        self.acceptors
            .iter()
            .copied()
            .filter(move |a| match self.phase {
                DecreePhase::Phase1 => !self.promised_by.contains(a),
                DecreePhase::Phase2(_) => !self.accepted_by.contains(a),
                DecreePhase::Chosen(_) | DecreePhase::Preempted(_) => false,
            })
    }

    /// Fold one Phase-1b promise. Returns the value to propose when this
    /// promise completes the quorum (P2c: the highest-ballot vote reported,
    /// else the own proposal); `None` otherwise, including duplicates and
    /// promises arriving outside Phase 1.
    ///
    /// # Panics
    ///
    /// If two acceptors report different values at one ballot
    /// ([`select_highest`]): one ballot has one proposer, so that is a
    /// protocol violation, never an operating condition.
    pub fn on_promise(&mut self, from: A, vote: Option<(Ballot, V)>) -> Option<V> {
        if self.phase != DecreePhase::Phase1
            || !self.acceptors.contains(&from)
            || !self.promised_by.insert(from)
        {
            return None;
        }
        if let Some(report) = vote {
            select_highest(
                &mut self.best,
                report,
                "two Phase-1 votes at one decree ballot agree on the value",
            );
        }
        if self.promised_by.len() < self.quorum {
            return None;
        }
        let value = self
            .best
            .as_ref()
            .map_or_else(|| self.proposal.clone(), |(_, v)| v.clone());
        self.phase = DecreePhase::Phase2(value.clone());
        Some(value)
    }

    /// Fold one Phase-2b accept. Returns the chosen value when this accept
    /// completes the quorum; `None` otherwise.
    pub fn on_accepted(&mut self, from: A) -> Option<V> {
        let DecreePhase::Phase2(value) = &self.phase else {
            return None;
        };
        let value = value.clone();
        if !self.acceptors.contains(&from)
            || !self.accepted_by.insert(from)
            || self.accepted_by.len() < self.quorum
        {
            return None;
        }
        self.phase = DecreePhase::Chosen(value.clone());
        Some(value)
    }

    /// A refusal: some acceptor promised `promised` above this ballot. The
    /// proposal is preempted; the caller opens a fresh one strictly above.
    /// Ignored once the value is chosen (a late refusal changes nothing).
    pub fn on_nack(&mut self, promised: Ballot) {
        if matches!(self.phase, DecreePhase::Chosen(_)) || promised <= self.ballot {
            return;
        }
        self.phase = DecreePhase::Preempted(promised);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeId;

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    #[test]
    fn a_lone_proposal_is_chosen_by_a_quorum() {
        let mut p: DecreeProposer<u8, &str> = DecreeProposer::new(ballot(1, 0), [0, 1, 2], "mine");
        assert_eq!(p.quorum(), 2);
        assert_eq!(p.on_promise(0, None), None);
        assert_eq!(p.on_promise(0, None), None, "a duplicate never counts");
        assert_eq!(p.on_promise(7, None), None, "a stranger never counts");
        assert_eq!(p.on_promise(1, None), Some("mine"));
        assert_eq!(p.phase(), &DecreePhase::Phase2("mine"));
        assert!(!p.adopted_prior_vote());
        assert_eq!(p.on_accepted(0), None);
        assert_eq!(p.on_accepted(0), None, "a duplicate never counts");
        assert_eq!(p.on_accepted(2), Some("mine"));
        assert_eq!(p.phase(), &DecreePhase::Chosen("mine"));
        assert_eq!(p.unanswered().count(), 0);
    }

    /// P2c: the dueling proposers. R2's Phase 1 finds R1's vote and must
    /// propose R1's value, never its own.
    #[test]
    fn a_later_proposal_adopts_the_highest_prior_vote() {
        let mut acceptors: Vec<DecreeAcceptor<&str>> = vec![DecreeAcceptor::default(); 3];
        // R1 at ballot 1 reaches acceptor 0 in Phase 2 before dying.
        for a in &mut acceptors {
            assert_eq!(a.prepare(ballot(1, 1)), Ok(None));
        }
        assert_eq!(acceptors[0].accept(ballot(1, 1), "r1"), Ok(()));
        // R2 at ballot 2 prepares 0 and 1.
        let mut p: DecreeProposer<usize, &str> = DecreeProposer::new(ballot(2, 2), [0, 1, 2], "r2");
        let v0 = acceptors[0].prepare(ballot(2, 2)).expect("promise");
        let v1 = acceptors[1].prepare(ballot(2, 2)).expect("promise");
        assert_eq!(p.on_promise(1, v1), None);
        assert_eq!(p.on_promise(0, v0), Some("r1"), "the prior vote wins");
        assert!(p.adopted_prior_vote());
        // R1's lower ballot is refused everywhere R2 reached.
        assert_eq!(acceptors[1].accept(ballot(1, 1), "r1"), Err(ballot(2, 2)));
        for (i, acceptor) in acceptors.iter_mut().enumerate().take(2) {
            assert_eq!(acceptor.accept(ballot(2, 2), "r1"), Ok(()));
            p.on_accepted(i);
        }
        assert_eq!(p.phase(), &DecreePhase::Chosen("r1"));
    }

    #[test]
    fn a_nack_preempts_and_names_the_promise_to_beat() {
        let mut a: DecreeAcceptor<u8> = DecreeAcceptor::default();
        assert_eq!(a.prepare(ballot(5, 1)), Ok(None));
        assert_eq!(a.prepare(ballot(3, 2)), Err(ballot(5, 1)));
        assert_eq!(a.prepare(ballot(5, 1)), Ok(None), "re-asking is idempotent");
        let mut p: DecreeProposer<u8, u8> = DecreeProposer::new(ballot(3, 2), [0], 9);
        p.on_nack(ballot(5, 1));
        assert_eq!(p.phase(), &DecreePhase::Preempted(ballot(5, 1)));
        assert_eq!(p.on_promise(0, None), None, "a preempted proposal is dead");
        assert_eq!(p.unanswered().count(), 0);
    }

    /// Review finding P8: the proposer half of "one ballot, one value". The
    /// acceptor's twin is `two_values_at_one_ballot_are_a_programmer_error`;
    /// this is the half where a silent pick would be consequential — two
    /// proposers with different arrival orders would select different values
    /// and two successor sets could be chosen for one generation.
    #[test]
    #[should_panic(expected = "two Phase-1 votes at one decree ballot agree on the value")]
    fn two_votes_at_one_ballot_are_a_programmer_error() {
        let mut p: DecreeProposer<u8, &str> = DecreeProposer::new(ballot(2, 0), [0, 1, 2], "mine");
        assert_eq!(p.on_promise(0, Some((ballot(1, 0), "voted"))), None);
        let _ = p.on_promise(1, Some((ballot(1, 0), "other")));
    }

    #[test]
    #[should_panic(expected = "one ballot carries one value")]
    fn two_values_at_one_ballot_are_a_programmer_error() {
        let mut a: DecreeAcceptor<u8> = DecreeAcceptor::default();
        a.accept(ballot(1, 1), 1).expect("first");
        let _ = a.accept(ballot(1, 1), 2);
    }
}
