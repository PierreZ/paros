//! **Single-decree Paxos: three machines agree on one value, and the value
//! never changes.**
//!
//! Run it: `cargo run -p paros-consensus --example single_decree`
//!
//! Start here if you want to learn the algorithm. This example needs no
//! other one, and no paper.
//!
//! It also shows a use of the crate without a log: [`Decree`] is the
//! proposer of one decision, [`Acceptor`] is its voter, and the value is a
//! type of the caller. The core chooses its next matchmaker set this way,
//! and a program above the core can choose anything that it must decide one
//! time: here, which machine is the primary.
//!
//! # The problem
//!
//! Some machines must agree on **one** value. When they agree, the answer
//! must not change, although a machine can crash at any time and a message
//! can be lost or late. "One machine decides and tells the others" fails
//! when that machine crashes after it told only some of them: the machine
//! that takes over can decide differently.
//!
//! # The parties
//!
//! - A **proposer** wants a value chosen. It sends the messages and counts
//!   the answers. Two proposers can try at the same time, and a proposer
//!   can crash.
//! - An **acceptor** votes. It keeps two things on its disk: the highest
//!   ballot that it promised, and the value that it accepted. It answers
//!   only after the write is durable.
//!
//! A **ballot** names one attempt of one proposer. A [`Ballot`] is the pair
//! `(round, node)`, printed `3.5` for round 3 of proposer 5. Two proposers
//! never share a ballot, and any two ballots compare.
//!
//! # The two phases
//!
//! **Phase 1.** The proposer sends `Prepare(ballot)`. An acceptor that did
//! not promise a higher ballot answers with a **promise**: it will accept
//! nothing at a lower ballot. The promise also reports the value that the
//! acceptor accepted before, with its ballot. An acceptor that promised a
//! higher ballot refuses, and names that ballot.
//!
//! **The rule P2c.** With promises from a majority, the proposer selects
//! its value. If a promise reported a value, the proposer **must** propose
//! the reported value of the highest ballot. It proposes its own value only
//! if no promise reported one. A reported value can be chosen already, and
//! the proposer cannot know: the acceptors that did not answer can hold it
//! too.
//!
//! **Phase 2.** The proposer sends `Accept(ballot, value)`. An acceptor
//! accepts unless it promised a higher ballot. A value is **chosen** when a
//! majority accepted it at one ballot.
//!
//! Why the chosen value never changes: a later proposer needs promises from
//! a majority, two majorities share an acceptor, that acceptor reports the
//! chosen value, and P2c makes the later proposer propose it again.
//!
//! # The caller's part
//!
//! [`Decree`] and [`Acceptor`] send nothing and store nothing. The caller:
//!
//! - opens a proposal with [`Decree::new`], sends a `Prepare` to each
//!   acceptor of [`Decree::unanswered`], and gives each answer to
//!   [`Decree::on_promise`] or [`Decree::on_nack`];
//! - on [`DecreePromise::Quorum`], sends an `Accept` with the value that the
//!   decree selected, and gives each answer to [`Decree::on_accepted`];
//! - on the acceptor, persists the [`AcceptorWrite`]s of a call before it
//!   answers.
//!
//! The value type implements [`Fingerprint`] and [`Weigh`].
//!
//! # What the three scenarios show
//!
//! 1. **No conflict.** Proposer 5 proposes "ada". No promise reports a
//!    value, and "ada" is chosen.
//! 2. **A later proposer adopts an accepted value.** Proposer 5 gets one
//!    vote for "ada" on acceptor A and crashes. Proposer 8 wants "bob". Its
//!    promises come from A and B, A reports "ada", and proposer 8 must
//!    propose "ada".
//! 3. **A value that the majority did not see is fenced.** The same start,
//!    but the promises of proposer 8 come from B and C. "bob" is chosen.
//!    Acceptor B restarts. Proposer 5 comes back, and B refuses its old
//!    ballot: the promise was on the disk. Proposer 5 tries again at a
//!    higher ballot and must propose "bob".
//!
//! Scenarios 2 and 3 start the same and choose different values. Both are
//! correct: Paxos guarantees one chosen value, not which one.
//!
//! Further reading: Lamport, *Paxos Made Simple* (2001), section 2, and
//! `specs/tla/decree/` for the model.

use std::collections::BTreeMap;

use paros_consensus::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_consensus::contract::AcceptorWrite;
use paros_consensus::decree::{AcceptFold, DECREE_SLOT, Decree, DecreePromise};
use paros_consensus::membership::{AcceptorConfig, QuorumSystem};
use paros_consensus::types::{FNV_OFFSET, Fingerprint, Weigh, fnv1a};
use paros_consensus::{Ballot, NodeId};

/// The value to agree on: the name of the primary. A decree chooses any
/// type that can say what it is ([`Fingerprint`]: a vote names the value
/// that it is for) and how large it is ([`Weigh`]).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Primary(&'static str);

impl Fingerprint for Primary {
    fn fingerprint(&self) -> u64 {
        fnv1a(FNV_OFFSET, self.0.as_bytes())
    }
}

impl Weigh for Primary {
    fn weight(&self) -> u64 {
        self.0.len() as u64
    }
}

/// The three acceptors. The proposers, 5 and 8, are other processes.
const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const C: NodeId = NodeId(3);

/// The acceptors, and their quorum rule: a majority, two of three. A decree
/// never counts votes against a number: it asks this configuration.
fn acceptors() -> AcceptorConfig {
    AcceptorConfig::new(vec![A, B, C], QuorumSystem::Majority)
}

/// What one acceptor keeps on its disk.
#[derive(Default)]
struct Disk {
    promised: Option<Ballot>,
    accepted: Option<(Ballot, Primary)>,
}

/// One acceptor machine: the [`Acceptor`] role and its disk.
struct Machine {
    id: NodeId,
    acceptor: Acceptor<Primary>,
    disk: Disk,
}

impl Machine {
    fn new(id: NodeId) -> Self {
        let mut machine = Self {
            id,
            acceptor: Acceptor::new(
                Ballot::zero(),
                BTreeMap::new(),
                DECREE_SLOT,
                BTreeMap::new(),
            ),
            disk: Disk::default(),
        };
        machine.restart();
        machine
    }

    /// A restart: a new [`Acceptor`] from what the disk holds. What the
    /// machine did not persist is gone.
    fn restart(&mut self) {
        let promised = self.disk.promised.unwrap_or_else(Ballot::zero);
        let records = self.disk.accepted.iter().cloned();
        let records = records.map(|record| (DECREE_SLOT, record)).collect();
        self.acceptor = Acceptor::new(promised, records, DECREE_SLOT, BTreeMap::new());
    }

    /// Persist the writes of one call. The machine answers only after this:
    /// a promise that left before its write would be forgotten by a crash,
    /// and the acceptor could then accept a ballot that it promised to
    /// refuse.
    fn persist(&mut self, writes: Vec<AcceptorWrite<Primary>>) {
        for write in writes {
            match write {
                AcceptorWrite::SetPromise(ballot) => self.disk.promised = Some(ballot),
                AcceptorWrite::AppendAccepted { ballot, value, .. } => {
                    self.disk.accepted = Some((ballot, value));
                }
            }
        }
    }

    /// `Prepare(ballot)` arrived. The answer is a promise with the vote of
    /// this acceptor, or a refusal with the ballot that it promised.
    fn on_prepare(&mut self, ballot: Ballot) -> Result<Option<(Ballot, Primary)>, Ballot> {
        let mut writes = Vec::new();
        let outcome = self.acceptor.prepare(ballot, DECREE_SLOT, &mut writes);
        self.persist(writes);
        match outcome {
            PrepareOutcome::Promised { .. } => Ok(self.acceptor.record(DECREE_SLOT).cloned()),
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Err(self.acceptor.promised()),
        }
    }

    /// `Accept(ballot, value)` arrived. The acceptor votes unless it
    /// promised a higher ballot. A vote at a ballot is also a promise at
    /// that ballot, and the promise is written first.
    fn on_accept(&mut self, ballot: Ballot, value: Primary) -> Result<(), Ballot> {
        match self.acceptor.admit(ballot, DECREE_SLOT) {
            AcceptOutcome::Admitted => {
                let mut writes = Vec::new();
                self.acceptor.set_promise(ballot, &mut writes);
                self.acceptor
                    .record_accepted(DECREE_SLOT, ballot, value, &mut writes);
                self.persist(writes);
                Ok(())
            }
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => Err(self.acceptor.promised()),
        }
    }
}

fn name(id: NodeId) -> &'static str {
    match id {
        A => "A",
        B => "B",
        _ => "C",
    }
}

fn machine(machines: &mut [Machine], id: NodeId) -> &mut Machine {
    machines.iter_mut().find(|m| m.id == id).expect("a machine")
}

/// One proposer runs one ballot. `phase_1` and `phase_2` name the acceptors
/// that its messages reach: this is the network of the example, and a
/// proposer that reaches nobody in a phase has crashed. Returns the decree,
/// and the value if this proposer saw it chosen.
fn propose(
    ballot: Ballot,
    value: &Primary,
    machines: &mut [Machine],
    phase_1: &[NodeId],
    phase_2: &[NodeId],
) -> (Decree<NodeId, Primary>, Option<Primary>) {
    println!(
        "   proposer {} proposes {:?} at ballot {ballot}",
        ballot.node, value.0
    );
    let mut decree = Decree::new(ballot, acceptors(), value.clone());

    // Phase 1: a `Prepare` to each acceptor, and the answers.
    for to in decree.unanswered() {
        if !phase_1.contains(&to) {
            continue;
        }
        match machine(machines, to).on_prepare(ballot) {
            Ok(vote) => {
                match &vote {
                    Some((at, value)) => {
                        println!(
                            "     {}: promise, it accepted {:?} at {at}",
                            name(to),
                            value.0
                        );
                    }
                    None => println!("     {}: promise, it accepted nothing", name(to)),
                }
                if let DecreePromise::Quorum(selected) = decree.on_promise(to, vote) {
                    let why = if decree.adopted_prior_vote() {
                        "a promise reported it"
                    } else {
                        "no promise reported a value"
                    };
                    println!("     a majority promised: propose {:?}, {why}", selected.0);
                }
            }
            Err(promised) => {
                println!("     {}: refused, it promised {promised}", name(to));
                decree.on_nack(promised);
            }
        }
    }
    // Phase 2 is open when Phase 1 selected a value.
    let Some(selected) = decree.value().cloned() else {
        println!("     no majority: the proposal stops");
        return (decree, None);
    };

    // Phase 2: an `Accept` to each acceptor, and the votes.
    let mut chosen = None;
    for to in decree.unanswered() {
        if !phase_2.contains(&to) {
            continue;
        }
        match machine(machines, to).on_accept(ballot, selected.clone()) {
            Ok(()) => {
                println!("     {}: accepted {:?}", name(to), selected.0);
                // A vote after the majority changes nothing.
                if let (AcceptFold::Chosen(value), None) = (decree.on_accepted(to), &chosen) {
                    println!("     a majority accepted: {:?} is chosen", value.0);
                    chosen = Some(value);
                }
            }
            Err(promised) => {
                println!("     {}: refused, it promised {promised}", name(to));
                decree.on_nack(promised);
            }
        }
    }
    if chosen.is_none() {
        println!("     no majority accepted: nothing is chosen by this ballot");
    }
    (decree, chosen)
}

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

/// Scenario 1: no promise reports a value, so the proposer keeps its own.
fn no_conflict() {
    println!("== 1. no conflict");
    let mut machines = [A, B, C].map(Machine::new);
    let (decree, chosen) = propose(
        ballot(1, 5),
        &Primary("ada"),
        &mut machines,
        &[A, B, C],
        &[A, B, C],
    );
    assert!(!decree.adopted_prior_vote());
    assert_eq!(chosen, Some(Primary("ada")));
}

/// Proposer 5 gets one vote, on acceptor A, and stops.
fn one_vote_for_ada(machines: &mut [Machine]) -> Decree<NodeId, Primary> {
    let (decree, chosen) = propose(ballot(3, 5), &Primary("ada"), machines, &[A, B, C], &[A]);
    assert_eq!(chosen, None, "one vote is not a majority");
    let vote = machine(machines, A).acceptor.record(DECREE_SLOT).cloned();
    assert_eq!(vote, Some((ballot(3, 5), Primary("ada"))));
    decree
}

/// Scenario 2: proposer 8 cannot know if "ada" is chosen. Acceptor C did
/// not answer, and "A and C accepted it" looks the same from here. So it
/// proposes "ada".
fn adopt_an_accepted_value() {
    println!("\n== 2. a later proposer adopts an accepted value");
    let mut machines = [A, B, C].map(Machine::new);
    one_vote_for_ada(&mut machines);
    println!("   proposer 5 crashes with one vote, on A");

    let (decree, chosen) = propose(
        ballot(7, 8),
        &Primary("bob"),
        &mut machines,
        &[A, B],
        &[A, B, C],
    );
    assert!(decree.adopted_prior_vote());
    assert_eq!(chosen, Some(Primary("ada")), "\"bob\" was never proposed");
    for machine in &machines {
        let vote = machine.acceptor.record(DECREE_SLOT);
        assert_eq!(vote, Some(&(ballot(7, 8), Primary("ada"))));
    }
}

/// Scenario 3: the promises of B and C fence ballot 3.5 for ever, so "ada"
/// can never get a second vote, and "bob" is safe to choose.
fn fence_an_unseen_value() {
    println!("\n== 3. a value that the majority did not see is fenced");
    let mut machines = [A, B, C].map(Machine::new);
    let mut old = one_vote_for_ada(&mut machines);
    println!("   proposer 5 stops with one vote, on A");

    let (decree, chosen) = propose(
        ballot(7, 8),
        &Primary("bob"),
        &mut machines,
        &[B, C],
        &[B, C],
    );
    assert!(!decree.adopted_prior_vote());
    assert_eq!(chosen, Some(Primary("bob")));

    // The promise of B is on its disk, so a restart does not lose it.
    machine(&mut machines, B).restart();
    println!("   B restarts. Proposer 5 comes back and sends its Accept at 3.5 to B");
    let refused = machine(&mut machines, B).on_accept(ballot(3, 5), Primary("ada"));
    assert_eq!(refused, Err(ballot(7, 8)));
    println!("     B: refused, it promised 7.8");

    // A refusal names the ballot to go above. The new attempt of proposer 5
    // starts with Phase 1, as each attempt does, and P2c gives it "bob".
    old.on_nack(ballot(7, 8));
    let above = old.preempted().expect("the decree is preempted");
    let (decree, chosen) = propose(
        ballot(above.round + 1, 5),
        &Primary("ada"),
        &mut machines,
        &[A, B, C],
        &[A, B, C],
    );
    assert!(decree.adopted_prior_vote());
    assert_eq!(
        chosen,
        Some(Primary("bob")),
        "the chosen value did not change"
    );
}

fn main() {
    no_conflict();
    adopt_an_accepted_value();
    fence_an_unseen_value();
    println!("\none value is chosen in each scenario, and it never changes");
}
