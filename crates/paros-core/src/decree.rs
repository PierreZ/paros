//! A **single-decree Paxos** wired out of the shared Paxos roles: one value,
//! chosen once, over any acceptor configuration (`docs/architecture.md` §1,
//! "paros eats its own food": one-shot decisions use this flavor, never a
//! hand-rolled agreement).
//!
//! There is no separate kernel here. A decree is exactly what
//! [`Proposer`] already runs — a Phase 1 that
//! adopts the highest-ballot vote reported (P2c) and a Phase 2 that chooses
//! it — over a log of **one slot**, with no paging (a decree's whole log fits
//! in one `Promise`), no tri-state (a lost decree vote is not repaired in
//! place), no gap fill and no recovery. Its acceptors run the same
//! [`Acceptor`](crate::acceptor::Acceptor) over the same one slot.
//!
//! Two decrees run on it:
//!
//! - **The matchmaker handover** (`matchmaker/reconfigurer.rs`): the
//!   successor set, over the matchmakers of `M_g` under a majority — paros
//!   supports **majority matchmaker quorums only**
//!   ([`MatchmakerSet::has_quorum`](crate::MatchmakerSet::has_quorum) is the
//!   same rule), and the handover is safe exactly under that model.
//! - **`cell init`** (#277, `paros::machine`): the cell plan, over the
//!   listed founding members, every one of them in both quorums.
//!
//! The caller names the acceptors and their quorum system; every quorum
//! question still crosses `membership.rs`.
//!
//! What stays here rather than moving into the role is the one place a
//! decree differs from a log: a `Nack`. The log side *discards* the refusing
//! acceptor's promise and lets the leadership fall to a fresh election; a
//! decree keeps the refusal, because its retry must open strictly above the
//! promise that refused it and the caller owns that round floor.

use std::collections::{BTreeMap, BTreeSet};

use crate::membership::AcceptorConfig;
use crate::proposer::{Campaign, PromiseFold, Proposer, Round};
use crate::types::{Ballot, Fingerprint, Slot};

/// The one slot a decree runs over: its value is chosen once.
pub const DECREE_SLOT: Slot = Slot(0);

/// What one Phase-1b promise did to a [`Decree`]. A fold that *counted* is
/// progress even when the quorum is still short, which is exactly what a
/// caller's stall clock must be able to tell from a duplicate (review finding
/// P4: counted-but-short promises reported as "nothing happened" let the
/// driver abandon a decree that was progressing, while duplicates reported as
/// progress kept resetting its clock).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecreePromise<V> {
    /// Not folded: no matching phase, a sender already counted, or one
    /// outside the acceptor set.
    Ignored,
    /// Counted; `remaining` more promises before the Phase-1 quorum holds.
    Counted { remaining: usize },
    /// The quorum holds: propose this value (P2c — the highest-ballot vote
    /// reported, else the proposer's own).
    Quorum(V),
}

/// What one Phase-2b accept did to a [`Decree`]. The twin of
/// [`DecreePromise`], counted the same way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptFold<V> {
    /// Not folded: not in Phase 2, a sender already counted, or one outside
    /// the acceptor set.
    Ignored,
    /// Counted; `remaining` more accepts before the value is chosen.
    Counted { remaining: usize },
    /// The Phase-2 quorum holds: this value is chosen.
    Chosen(V),
}

/// One proposal of one value, at one ballot, over one acceptor
/// configuration.
///
/// The matchmaker handover shows it in
/// [`ReconfigurerPhase::Deciding`](crate::ReconfigurerPhase) so a driver can
/// see *where* the decree stands — its ballot, the value in flight, whether
/// P2c adopted a prior vote, the promise that preempted it — and everything
/// it *does* is its caller's to drive.
#[derive(Clone, Debug)]
pub struct Decree<Id, V> {
    ballot: Ballot,
    /// The acceptors, under the quorum system the caller chose.
    acceptors: AcceptorConfig<Id>,
    /// What the caller wants chosen, proposed only when Phase 1 finds no
    /// earlier vote.
    proposal: V,
    proposer: Proposer<Id, V>,
    /// The promise that refused this ballot, once one has.
    preempted: Option<Ballot>,
}

impl<Id: Copy + Ord, V: Clone + PartialEq + Fingerprint> Decree<Id, V> {
    /// Open Phase 1 of `proposal` at `ballot` over `acceptors`.
    ///
    /// # Panics
    ///
    /// If `acceptors` names no member (a decree with no acceptor is a
    /// programmer error; [`AcceptorConfig::new`] never builds one).
    pub fn new(ballot: Ballot, acceptors: AcceptorConfig<Id>, proposal: V) -> Self {
        assert!(!acceptors.members().is_empty(), "a decree has an acceptor");
        let mut proposer = Proposer::new();
        // A one-slot log, from slot zero, over one configuration: the decree
        // has no prior configuration to cover but the acceptors themselves,
        // and the proposer casts no vote of its own here (an acceptor that is
        // also the proposer answers like any other, through the caller).
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
        assert!(
            proposer.election().is_some_and(|e| e.ballot() == ballot),
            "a decree opens Phase 1 at its own ballot"
        );
        Self {
            ballot,
            acceptors,
            proposal,
            proposer,
            preempted: None,
        }
    }

    /// The acceptors this decree runs over.
    #[must_use]
    pub fn acceptors(&self) -> &AcceptorConfig<Id> {
        &self.acceptors
    }

    /// The ballot this proposal runs at.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The value proposed once Phase 2 has opened: the reconfigurer's own
    /// proposal, or the prior vote P2c made it adopt.
    #[must_use]
    pub fn value(&self) -> Option<&V> {
        self.proposer.rounds().get(&DECREE_SLOT).map(Round::command)
    }

    /// Whether Phase 1 adopted a prior vote instead of the caller's own
    /// proposal (the P2c rule fired) — observability for the caller's
    /// audit.
    #[must_use]
    pub fn adopted_prior_vote(&self) -> bool {
        self.value().is_some_and(|v| *v != self.proposal)
    }

    /// The promise that refused this ballot, once one has: the caller reopens
    /// strictly above it.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn preempted(&self) -> Option<Ballot> {
        // Only a promise strictly above this ballot preempts it.
        if let Some(promised) = self.preempted {
            assert!(
                promised > self.ballot,
                "a preempting promise lies above the ballot"
            );
        }
        self.preempted
    }

    /// The acceptors that have not answered the phase in flight — what a
    /// re-send targets. Empty once the decree is preempted (the caller
    /// reopens it before it re-sends).
    ///
    /// # Panics
    ///
    /// If the re-send would address a node outside the acceptors: a
    /// programmer error, never an operating condition.
    #[must_use]
    pub fn unanswered(&self) -> Vec<Id> {
        if self.preempted.is_some() {
            return Vec::new();
        }
        let unanswered = self.unanswered_live();
        assert!(
            unanswered.iter().all(|m| self.acceptors.contains(*m)),
            "a decree re-send addresses only its acceptors"
        );
        unanswered
    }

    /// [`Decree::unanswered`] for a decree no promise has preempted.
    fn unanswered_live(&self) -> Vec<Id> {
        match self.proposer.rounds().get(&DECREE_SLOT) {
            None => self
                .proposer
                .election()
                .map(|e| e.unpromised(None))
                .unwrap_or_default(),
            Some(round) => {
                // A decree is never delegated: its rounds are always the
                // proposer's own.
                let accepted_by = round.accepted_by().expect("a decree round is colocated");
                self.acceptors
                    .members()
                    .iter()
                    .copied()
                    .filter(|m| !accepted_by.contains(m))
                    .collect()
            }
        }
    }

    /// Fold one Phase-1b promise, opening Phase 2 with the selected value
    /// when it completes the quorum.
    ///
    /// # Panics
    ///
    /// If two acceptors report different values at one ballot: one ballot
    /// has one proposer, so that is a protocol violation, never an operating
    /// condition.
    pub fn on_promise(&mut self, from: Id, vote: Option<(Ballot, V)>) -> DecreePromise<V> {
        if !self.acceptors.contains(from) || self.preempted.is_some() {
            return DecreePromise::Ignored;
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
        ) != PromiseFold::Answered
        {
            return DecreePromise::Ignored;
        }
        if !self.proposer.phase1_won(self.ballot) {
            return DecreePromise::Counted {
                remaining: self
                    .acceptors
                    .quorum_system()
                    .phase1_quorum_size(self.acceptors.members().len())
                    .saturating_sub(self.promised()),
            };
        }
        // Nothing is ever chosen behind a decree, so no slot is excluded and
        // no slot can be blocked: `close_phase1` opens no repair probe here.
        let outcome = self.proposer.close_phase1(|_| false);
        let value = outcome
            .recovered
            .get(&DECREE_SLOT)
            .map_or_else(|| self.proposal.clone(), |(_, v)| v.clone());
        // A decree's one slot is addressed to every acceptor, never a column.
        self.proposer
            .open_round(DECREE_SLOT, self.ballot, value.clone(), None, None);
        assert!(
            self.value() == Some(&value),
            "Phase 2 carries the selected value"
        );
        assert!(
            self.proposer.election().is_none(),
            "a won decree closes its Phase 1"
        );
        DecreePromise::Quorum(value)
    }

    /// Fold one Phase-2b accept, reporting the chosen value when it completes
    /// the quorum.
    ///
    /// # Panics
    ///
    /// If the decree is chosen at another ballot or with another value than
    /// the one it proposed: a protocol violation, never an operating
    /// condition.
    pub fn on_accepted(&mut self, from: Id) -> AcceptFold<V> {
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
        if round
            .accepted_by()
            .expect("a decree round is colocated")
            .contains(&from)
        {
            return AcceptFold::Ignored;
        }
        let vhash = round.command().fingerprint();
        if !self
            .proposer
            .fold_accepted(from, self.ballot, DECREE_SLOT, vhash)
        {
            return AcceptFold::Ignored;
        }
        if let Some((ballot, value)) = self.proposer.decided(DECREE_SLOT, &self.acceptors) {
            // A decree chooses its own Phase-2 value at its own ballot.
            assert!(ballot == self.ballot, "a decree is chosen at its ballot");
            assert!(
                self.value() == Some(&value),
                "a decree chooses the value it proposed"
            );
            return AcceptFold::Chosen(value);
        }
        let accepted = self
            .proposer
            .rounds()
            .get(&DECREE_SLOT)
            .map_or(0, |round| round.accepted_by().map_or(0, BTreeSet::len));
        // How many more accepts the decree still waits for — the one thing
        // a quorum *predicate* cannot report, so the one place a decree
        // quorum is spelled as a number, read off the caller's system.
        AcceptFold::Counted {
            remaining: self
                .acceptors
                .quorum_system()
                .phase2_quorum_size(self.acceptors.members().len())
                .saturating_sub(accepted),
        }
    }

    /// A refusal: some acceptor promised `promised` above this ballot. The
    /// proposal is preempted and the caller reopens strictly above it — above
    /// the **highest** refusal seen, so a second Nack carrying a higher
    /// promise raises the floor the reopen clears without being progress of
    /// the (already dead) proposal.
    ///
    /// # Panics
    ///
    /// If the preemption floor would fall: a programmer error.
    pub fn on_nack(&mut self, promised: Ballot) {
        if promised <= self.ballot {
            return;
        }
        let held = self.preempted;
        self.preempted = Some(self.preempted.map_or(promised, |held| held.max(promised)));
        // The floor a reopen clears only rises, and covers every refusal.
        assert!(
            self.preempted >= held,
            "a decree's preemption floor never falls"
        );
        assert!(
            self.preempted >= Some(promised),
            "a preemption covers the refusing promise"
        );
    }

    /// How many acceptors have promised.
    fn promised(&self) -> usize {
        self.proposer.election().map_or(0, |e| e.promised().len())
    }
}

#[cfg(test)]
mod tests {
    use super::{AcceptFold, Decree, DecreePromise};
    use crate::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
    use crate::membership::{
        AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, QuorumSystem,
    };
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

    /// The handover's acceptors: the set being replaced, under a majority.
    fn majority(old: &MatchmakerSet) -> AcceptorConfig<MatchmakerId> {
        AcceptorConfig::new(old.members().to_vec(), QuorumSystem::Majority)
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
        let mut d = Decree::new(ballot(1, 0), majority(&old), ids(&[3, 4, 5]));
        assert_eq!(
            d.on_promise(MatchmakerId(0), None),
            DecreePromise::Counted { remaining: 1 }
        );
        assert_eq!(
            d.on_promise(MatchmakerId(0), None),
            DecreePromise::Ignored,
            "a duplicate never counts"
        );
        assert_eq!(
            d.on_promise(MatchmakerId(7), None),
            DecreePromise::Ignored,
            "a stranger never counts"
        );
        assert_eq!(
            d.on_promise(MatchmakerId(1), None),
            DecreePromise::Quorum(ids(&[3, 4, 5]))
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
        let mut d = Decree::new(ballot(2, 2), majority(&old), ids(&[8]));
        let v0 = voters[0].prepare(ballot(2, 2)).expect("promise");
        let v1 = voters[1].prepare(ballot(2, 2)).expect("promise");
        assert_eq!(
            d.on_promise(MatchmakerId(1), v1),
            DecreePromise::Counted { remaining: 1 }
        );
        assert_eq!(
            d.on_promise(MatchmakerId(0), v0),
            DecreePromise::Quorum(ids(&[9])),
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
        let mut d = Decree::new(ballot(3, 2), majority(&old), ids(&[1]));
        d.on_nack(ballot(5, 1));
        assert_eq!(d.preempted(), Some(ballot(5, 1)));
        assert_eq!(
            d.on_promise(MatchmakerId(0), None),
            DecreePromise::Ignored,
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
        let mut d = Decree::new(ballot(2, 0), majority(&old), ids(&[8]));
        assert_eq!(
            d.on_promise(MatchmakerId(0), Some((ballot(1, 0), ids(&[1])))),
            DecreePromise::Counted { remaining: 1 }
        );
        let _ = d.on_promise(MatchmakerId(1), Some((ballot(1, 0), ids(&[2]))));
    }
}
