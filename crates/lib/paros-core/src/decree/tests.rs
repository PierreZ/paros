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
