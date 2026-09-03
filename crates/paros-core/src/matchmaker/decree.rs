//! The successor **decree**: single-decree Paxos wired out of the shared
//! Paxos roles, over the matchmakers of `M_g` as its acceptors.
//!
//! There is no separate kernel here. A decree is exactly what
//! [`Proposer`](crate::proposer::Proposer) already runs — a Phase 1 that
//! adopts the highest-ballot vote reported (P2c) and a Phase 2 that chooses
//! it — over a log of **one slot**, with no paging (a decree's whole log fits
//! in one `Promise`), no tri-state (a lost decree vote is not repaired in
//! place; the generation is replaced instead), no gap fill and no recovery.
//! The matchmaker's half is the same [`Acceptor`](crate::acceptor::Acceptor)
//! over the same one slot (`super::generation`).
//!
//! What stays here rather than moving into the role is the one place the two
//! deployments genuinely differ: a `Nack`. The log side *discards* the
//! refusing acceptor's promise and lets the leadership fall to a fresh
//! election; a decree keeps the refusal, because its retry must open strictly
//! above the promise that refused it and the reconfigurer owns that round
//! floor.
//!
//! **Quorum model.** Both phases take a majority of the named matchmakers,
//! built here from the set being replaced and asked through the same
//! membership boundary as every other tally in the core. Matchmaker Paxos
//! generalizes matchmaker quorums to arbitrary systems; paros deliberately
//! supports **majority matchmaker quorums only**
//! ([`MatchmakerSet::has_quorum`] is the same rule), and the handover is safe
//! exactly under that model.

use std::collections::BTreeMap;

use super::{MatchmakerId, MatchmakerSet};
use crate::membership::{AcceptorConfig, QuorumSystem};
use crate::proposer::{Campaign, PromiseFold as PageFold, Proposer, Round};
use crate::types::{Ballot, Fingerprint, Slot};

/// The one slot a decree runs over: a matchmaker set is a single value,
/// chosen once per generation.
const DECREE_SLOT: Slot = Slot(0);

/// What one Phase-1b promise did to a [`Decree`]. A fold that *counted* is
/// progress even when the quorum is still short, which is exactly what a
/// caller's stall clock must be able to tell from a duplicate (review finding
/// P4: counted-but-short promises reported as "nothing happened" let the
/// driver abandon a decree that was progressing, while duplicates reported as
/// progress kept resetting its clock).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PromiseFold {
    /// Not folded: no matching phase, a sender already counted, or one
    /// outside the acceptor set.
    Ignored,
    /// Counted; `remaining` more promises before the Phase-1 quorum holds.
    Counted { remaining: usize },
    /// The quorum holds: propose this value (P2c — the highest-ballot vote
    /// reported, else the proposer's own).
    Quorum(Vec<MatchmakerId>),
}

/// What one Phase-2b accept did to a [`Decree`]. The twin of
/// [`PromiseFold`], counted the same way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum AcceptFold {
    /// Not folded: not in Phase 2, a sender already counted, or one outside
    /// the acceptor set.
    Ignored,
    /// Counted; `remaining` more accepts before the value is chosen.
    Counted { remaining: usize },
    /// The Phase-2 quorum holds: this value is chosen.
    Chosen(Vec<MatchmakerId>),
}

/// One proposal of one successor set, at one ballot, over one generation's
/// matchmakers.
///
/// Opaque from outside the crate: it appears in
/// [`ReconfigurerPhase::Deciding`](super::ReconfigurerPhase) so a driver can
/// see *that* a decree is running, and everything it does is the
/// reconfigurer's to drive.
#[derive(Clone, Debug)]
pub struct Decree {
    ballot: Ballot,
    /// The acceptors: the matchmakers of the generation being replaced, under
    /// the majority system.
    acceptors: AcceptorConfig<MatchmakerId>,
    /// What this reconfigurer wants chosen, proposed only when Phase 1 finds
    /// no earlier vote.
    proposal: Vec<MatchmakerId>,
    proposer: Proposer<MatchmakerId, Vec<MatchmakerId>>,
    /// The promise that refused this ballot, once one has.
    preempted: Option<Ballot>,
}

impl Decree {
    /// Open Phase 1 of `proposal` at `ballot` over the members of `old`.
    ///
    /// # Panics
    ///
    /// If `old` names no matchmaker (a decree with no acceptor is a
    /// programmer error; the reconfigurer refuses a malformed set at
    /// `start`).
    pub(super) fn new(ballot: Ballot, old: &MatchmakerSet, proposal: Vec<MatchmakerId>) -> Self {
        let acceptors = AcceptorConfig::new(old.members.clone(), QuorumSystem::Majority);
        let mut proposer = Proposer::new();
        // A one-slot log, from slot zero, over one configuration: the decree
        // has no prior configuration to cover but the acceptors themselves,
        // and the proposer is a *node*, never one of these matchmakers, so it
        // holds no acceptor identity and casts no vote of its own.
        proposer.open_phase1(
            Campaign {
                me: None,
                ballot,
                config: acceptors.clone(),
                prior: vec![acceptors.clone()],
                from_slot: DECREE_SLOT,
            },
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        Self {
            ballot,
            acceptors,
            proposal,
            proposer,
            preempted: None,
        }
    }

    /// The ballot this proposal runs at.
    pub(super) fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The value proposed once Phase 2 has opened.
    pub(super) fn value(&self) -> Option<&Vec<MatchmakerId>> {
        self.proposer.rounds().get(&DECREE_SLOT).map(Round::command)
    }

    /// Whether Phase 1 adopted a prior vote instead of this reconfigurer's
    /// own proposal (the P2c rule fired) — observability for the caller's
    /// audit.
    pub(super) fn adopted_prior_vote(&self) -> bool {
        self.value().is_some_and(|v| *v != self.proposal)
    }

    /// The promise that refused this ballot, once one has: the caller reopens
    /// strictly above it.
    pub(super) fn preempted(&self) -> Option<Ballot> {
        self.preempted
    }

    /// The matchmakers that have not answered the phase in flight — what a
    /// re-send targets. Empty once the decree is preempted (the caller
    /// reopens it before it re-sends).
    pub(super) fn unanswered(&self) -> Vec<MatchmakerId> {
        if self.preempted.is_some() {
            return Vec::new();
        }
        match self.proposer.rounds().get(&DECREE_SLOT) {
            None => self
                .proposer
                .election()
                .map(|e| e.unpromised(None))
                .unwrap_or_default(),
            Some(round) => self
                .acceptors
                .members()
                .iter()
                .copied()
                .filter(|m| !round.accepted_by().contains(m))
                .collect(),
        }
    }

    /// Fold one Phase-1b promise, opening Phase 2 with the selected value
    /// when it completes the quorum.
    ///
    /// # Panics
    ///
    /// If two matchmakers report different values at one ballot: one ballot
    /// has one proposer, so that is a protocol violation, never an operating
    /// condition.
    pub(super) fn on_promise(
        &mut self,
        from: MatchmakerId,
        vote: Option<(Ballot, Vec<MatchmakerId>)>,
    ) -> PromiseFold {
        if !self.acceptors.contains(from) || self.preempted.is_some() {
            return PromiseFold::Ignored;
        }
        let accepted = vote
            .map(|vote| BTreeMap::from([(DECREE_SLOT, vote)]))
            .unwrap_or_default();
        // A decree's whole log is one slot, so its promise is never paged:
        // one terminal page carries the vote or reports none.
        if self.proposer.fold_promise(
            from,
            self.ballot,
            DECREE_SLOT,
            accepted,
            BTreeMap::new(),
            None,
        ) != PageFold::Answered
        {
            return PromiseFold::Ignored;
        }
        if !self.proposer.phase1_won(self.ballot) {
            return PromiseFold::Counted {
                remaining: self.remaining(self.promised()),
            };
        }
        // Nothing is ever chosen behind a decree, so no slot is excluded and
        // no slot can be blocked: `close_phase1` opens no repair probe here.
        let outcome = self.proposer.close_phase1(|_| false);
        let value = outcome
            .recovered
            .get(&DECREE_SLOT)
            .map_or_else(|| self.proposal.clone(), |(_, v)| v.clone());
        self.proposer
            .open_round(DECREE_SLOT, self.ballot, value.clone(), None);
        PromiseFold::Quorum(value)
    }

    /// Fold one Phase-2b accept, reporting the chosen value when it completes
    /// the quorum.
    pub(super) fn on_accepted(&mut self, from: MatchmakerId) -> AcceptFold {
        if !self.acceptors.contains(from) || self.preempted.is_some() {
            return AcceptFold::Ignored;
        }
        let Some(round) = self.proposer.rounds().get(&DECREE_SLOT) else {
            return AcceptFold::Ignored;
        };
        // A duplicate is *not* progress, and the distinction is the caller's
        // stall clock (review finding P4). The log deployment credits a
        // repeated `Accepted` deliberately — it is leader contact for its
        // `CheckQuorum` window — so the round tally counts it either way and
        // this is the one place that has to tell them apart.
        if round.accepted_by().contains(&from) {
            return AcceptFold::Ignored;
        }
        let vhash = round.command().fingerprint();
        if !self
            .proposer
            .fold_accepted(from, self.ballot, DECREE_SLOT, vhash)
        {
            return AcceptFold::Ignored;
        }
        if let Some((_, value)) = self.proposer.decided(DECREE_SLOT, &self.acceptors) {
            return AcceptFold::Chosen(value);
        }
        let accepted = self
            .proposer
            .rounds()
            .get(&DECREE_SLOT)
            .map_or(0, |round| round.accepted_by().len());
        AcceptFold::Counted {
            remaining: self.remaining(accepted),
        }
    }

    /// A refusal: some matchmaker promised `promised` above this ballot. The
    /// proposal is preempted and the caller reopens strictly above it.
    pub(super) fn on_nack(&mut self, promised: Ballot) {
        if promised <= self.ballot {
            return;
        }
        self.preempted = Some(promised);
    }

    /// How many matchmakers have promised.
    fn promised(&self) -> usize {
        self.proposer.election().map_or(0, |e| e.promised().len())
    }

    /// How many more answers a quorum still waits for — the one thing a
    /// quorum *predicate* cannot report.
    fn remaining(&self, held: usize) -> usize {
        self.acceptors
            .quorum_system()
            .quorum_size(self.acceptors.members().len())
            .saturating_sub(held)
    }
}

#[cfg(test)]
mod tests {
    use super::{AcceptFold, Decree, PromiseFold};
    use crate::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
    use crate::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
    use crate::types::{Ballot, NodeId, Slot};
    use crate::write::AcceptorWrite;
    use std::collections::BTreeMap;

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn ids(ids: &[u64]) -> Vec<MatchmakerId> {
        ids.iter().copied().map(MatchmakerId).collect()
    }

    fn generation(members: &[u64]) -> MatchmakerSet {
        MatchmakerSet::new(MatchmakerGeneration(0), ids(members))
    }

    /// A matchmaker's acceptor half: the shared role over the decree's one
    /// slot, as `Matchmaker::decree_acceptor` builds it.
    struct Voter(Acceptor<Vec<MatchmakerId>>);

    impl Voter {
        fn new() -> Self {
            Self(Acceptor::new(
                Ballot::default(),
                BTreeMap::new(),
                Slot(0),
                BTreeMap::new(),
            ))
        }

        fn prepare(&mut self, b: Ballot) -> Result<Option<(Ballot, Vec<MatchmakerId>)>, Ballot> {
            let mut writes: Vec<AcceptorWrite<Vec<MatchmakerId>>> = Vec::new();
            match self.0.prepare(b, Slot(0), &mut writes) {
                PrepareOutcome::Promised { .. } => Ok(self.0.record(Slot(0)).cloned()),
                PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Err(self.0.promised()),
            }
        }

        fn accept(&mut self, b: Ballot, value: Vec<MatchmakerId>) -> Result<(), Ballot> {
            let mut writes: Vec<AcceptorWrite<Vec<MatchmakerId>>> = Vec::new();
            match self.0.admit(b, Slot(0)) {
                AcceptOutcome::Admitted => {
                    self.0.set_promise(b, &mut writes);
                    self.0.record_accepted(Slot(0), b, value, &mut writes);
                    Ok(())
                }
                AcceptOutcome::Refused | AcceptOutcome::BelowFloor => Err(self.0.promised()),
            }
        }
    }

    /// Review finding P4, both directions: a fold that counts toward a quorum
    /// still short reports the progress it made and how much is missing,
    /// while a duplicate or a stranger reports `Ignored`. The caller's stall
    /// clock is driven by exactly that distinction.
    #[test]
    fn a_lone_proposal_is_chosen_by_a_quorum() {
        let old = generation(&[0, 1, 2]);
        let mut d = Decree::new(ballot(1, 0), &old, ids(&[3, 4, 5]));
        assert_eq!(
            d.on_promise(MatchmakerId(0), None),
            PromiseFold::Counted { remaining: 1 }
        );
        assert_eq!(
            d.on_promise(MatchmakerId(0), None),
            PromiseFold::Ignored,
            "a duplicate never counts"
        );
        assert_eq!(
            d.on_promise(MatchmakerId(7), None),
            PromiseFold::Ignored,
            "a stranger never counts"
        );
        assert_eq!(
            d.on_promise(MatchmakerId(1), None),
            PromiseFold::Quorum(ids(&[3, 4, 5]))
        );
        assert!(!d.adopted_prior_vote());
        assert_eq!(
            d.on_accepted(MatchmakerId(0)),
            AcceptFold::Counted { remaining: 1 }
        );
        assert_eq!(
            d.on_accepted(MatchmakerId(0)),
            AcceptFold::Ignored,
            "a duplicate never counts"
        );
        assert_eq!(
            d.on_accepted(MatchmakerId(7)),
            AcceptFold::Ignored,
            "a stranger never counts"
        );
        assert_eq!(
            d.on_accepted(MatchmakerId(2)),
            AcceptFold::Chosen(ids(&[3, 4, 5]))
        );
    }

    /// P2c: the dueling proposers. R2's Phase 1 finds R1's vote and must
    /// propose R1's value, never its own.
    #[test]
    fn a_later_proposal_adopts_the_highest_prior_vote() {
        let old = generation(&[0, 1, 2]);
        let mut voters = [Voter::new(), Voter::new(), Voter::new()];
        // R1 at ballot 1 reaches matchmaker 0 in Phase 2 before dying.
        for v in &mut voters {
            assert_eq!(v.prepare(ballot(1, 1)), Ok(None));
        }
        assert_eq!(voters[0].accept(ballot(1, 1), ids(&[9])), Ok(()));
        // R2 at ballot 2 prepares 0 and 1.
        let mut d = Decree::new(ballot(2, 2), &old, ids(&[8]));
        let v0 = voters[0].prepare(ballot(2, 2)).expect("promise");
        let v1 = voters[1].prepare(ballot(2, 2)).expect("promise");
        assert_eq!(
            d.on_promise(MatchmakerId(1), v1),
            PromiseFold::Counted { remaining: 1 }
        );
        assert_eq!(
            d.on_promise(MatchmakerId(0), v0),
            PromiseFold::Quorum(ids(&[9])),
            "the prior vote wins"
        );
        assert!(d.adopted_prior_vote());
        // R1's lower ballot is refused everywhere R2 reached.
        assert_eq!(voters[1].accept(ballot(1, 1), ids(&[9])), Err(ballot(2, 2)));
    }

    #[test]
    fn a_nack_preempts_and_names_the_promise_to_beat() {
        let mut v = Voter::new();
        assert_eq!(v.prepare(ballot(5, 1)), Ok(None));
        assert_eq!(v.prepare(ballot(3, 2)), Err(ballot(5, 1)));
        assert_eq!(v.prepare(ballot(5, 1)), Ok(None), "re-asking is idempotent");
        let old = generation(&[0]);
        let mut d = Decree::new(ballot(3, 2), &old, ids(&[1]));
        d.on_nack(ballot(5, 1));
        assert_eq!(d.preempted(), Some(ballot(5, 1)));
        assert_eq!(
            d.on_promise(MatchmakerId(0), None),
            PromiseFold::Ignored,
            "a preempted proposal is dead"
        );
        assert!(d.unanswered().is_empty());
    }

    /// Review finding P8: the proposer half of "one ballot, one value". The
    /// acceptor's twin is
    /// `acceptor::tests::two_values_at_one_ballot_are_a_programmer_error`;
    /// this is the half where a silent pick would be consequential — two
    /// proposers with different arrival orders would select different values
    /// and two successor sets could be chosen for one generation.
    #[test]
    #[should_panic(expected = "two Phase-1 reports of one (slot, ballot) agree on the command")]
    fn two_votes_at_one_ballot_are_a_programmer_error() {
        let old = generation(&[0, 1, 2]);
        let mut d = Decree::new(ballot(2, 0), &old, ids(&[8]));
        assert_eq!(
            d.on_promise(MatchmakerId(0), Some((ballot(1, 0), ids(&[1])))),
            PromiseFold::Counted { remaining: 1 }
        );
        let _ = d.on_promise(MatchmakerId(1), Some((ballot(1, 0), ids(&[2]))));
    }
}
