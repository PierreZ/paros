//! **Single-decree Paxos: safely choose one value.**
//!
//! Run it: `cargo run -p paros-core --example single_decree`
//!
//! This is the first of three executable lessons
//! (`single_decree` → `multi_paxos` → `matchmaker`). Each one drives the
//! *real* composable roles of `paros-core` — [`Proposer`] and [`Acceptor`] —
//! by hand, with direct function calls standing in for the network and a
//! `Vec` standing in for the disk. There is no second Paxos implementation
//! here: the code that decides is the library's; this file only delivers
//! messages and prints what happened.
//!
//! # Vocabulary (paper name / paros name)
//!
//! - **Ballot** (a.k.a. proposal number, round): a totally ordered tag a
//!   proposer mints for one attempt. Paros's [`Ballot`] is `(round, node)`,
//!   so two proposers can never mint the same one. A ballot is *not* a
//!   value: it is the name of an attempt.
//! - **Slot** (a.k.a. instance, decree, log index): *which* value is being
//!   chosen. Single-decree Paxos has exactly one slot; paros's [`Acceptor`]
//!   and [`Proposer`] are written over a log of slots, so this example
//!   uses a one-slot log, `Slot(0)`. Multi-Paxos (the next example) is the
//!   same machinery over many slots.
//! - **Proposed value**: what a proposer *wants* chosen when it starts.
//! - **Accepted value**: what one acceptor has durably voted for, at some
//!   ballot. An accepted value is *not* a chosen value — one acceptor's
//!   vote proves nothing on its own.
//! - **Chosen value**: a value accepted at one ballot by a **quorum**
//!   (here, a majority of the three acceptors). Once chosen, it is chosen
//!   forever, and no other value can ever be chosen for that slot — that
//!   is Paxos safety, and the last scenario below is about why.
//!
//! # The protocol, in the order this file runs it
//!
//! 1. The proposer starts a ballot.
//! 2. It sends `Prepare(ballot)` to the acceptors (Phase 1a).
//! 3. An acceptor promises — never to accept anything at a *lower* ballot —
//!    and reports the value it has already accepted, if any (Phase 1b, a
//!    `Promise`).
//! 4. The proposer waits for a Phase-1 quorum of promises.
//! 5. **P2c**, the value-selection rule: if any promise reported an
//!    accepted value, the proposer must propose the one with the highest
//!    ballot; only if none did may it propose its own value.
//! 6. It sends `Accept(ballot, value)` (Phase 2a).
//! 7. An acceptor accepts unless it has promised a higher ballot
//!    (Phase 2b, an `Accepted`).
//! 8. The proposer waits for a Phase-2 quorum of `Accepted`s.
//! 9. The value is chosen.

use std::collections::BTreeMap;

use paros_core::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_core::proposer::{Campaign, PromiseFold, Proposer};
use paros_core::{
    AcceptorConfig, AcceptorWrite, Ballot, ClientId, ClientSeq, Command, Entry, Fingerprint,
    NodeId, QuorumSystem, Slot, Value,
};

/// The one decree: single-decree Paxos is the roles over a one-slot log.
const DECREE: Slot = Slot(0);

/// The three acceptors. The proposers are separate parties (nodes 5 and 8
/// below); a proposer that is *also* an acceptor is the Multi-Paxos
/// example's business.
const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const C: NodeId = NodeId(3);

fn ballot(round: u64, proposer: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(proposer),
    }
}

/// A client value. Paros's log value type is [`Command`]; a client command
/// is an [`Entry`] whose `(client, seq)` exist for at-most-once execution
/// in Multi-Paxos and play no part here. To the acceptor a value is opaque;
/// to the proposer it has a [`Fingerprint`] and nothing more.
fn value(text: &str) -> Command {
    Command::User(Entry {
        client: ClientId(1),
        seq: ClientSeq(0),
        value: Value(text.as_bytes().to_vec()),
    })
}

fn show(command: &Command) -> String {
    match command {
        Command::User(entry) => format!("{:?}", String::from_utf8_lossy(&entry.value.0)),
        Command::Control(control) => format!("{control:?}"),
    }
}

fn show_ballot(ballot: Ballot) -> String {
    format!("{}.{}", ballot.round, ballot.node.0)
}

fn name(id: NodeId) -> &'static str {
    match id {
        A => "A",
        B => "B",
        C => "C",
        _ => "?",
    }
}

/// One acceptor process: the [`Acceptor`] role plus the disk it writes to.
///
/// The role never touches storage itself. Every durable change it makes —
/// a raised promise, an accepted record — is pushed into the `Vec` the
/// caller hands it as an [`AcceptorWrite`]. A real driver fsyncs that batch
/// *before* the reply leaves the node (persist-before-send: a promise that
/// is not durable could be forgotten by a crash and broken by the reboot).
/// Here memory is the disk, and the ordering is the same: write, then reply.
struct AcceptorNode {
    id: NodeId,
    role: Acceptor<Command>,
    disk: Vec<AcceptorWrite<Command>>,
}

impl AcceptorNode {
    fn new(id: NodeId) -> Self {
        Self {
            id,
            // Nothing promised, nothing accepted, no compaction floor, no
            // corrupted records: an acceptor is born empty.
            role: Acceptor::new(Ballot::zero(), BTreeMap::new(), DECREE, BTreeMap::new()),
            disk: Vec::new(),
        }
    }

    /// Phase 1b. `Prepare(ballot)` arrived. Either promise — raising the
    /// durable promise and reporting the accepted record, if any — or refuse
    /// with the higher ballot already promised (paros's `Nack`).
    fn on_prepare(&mut self, ballot: Ballot) -> Result<Option<(Ballot, Command)>, Ballot> {
        match self.role.prepare(ballot, DECREE, &mut self.disk) {
            // The promise is what makes P2c sound: from now on this
            // acceptor refuses every lower ballot, so whatever it reports
            // is the *last word* it will ever say about ballots below this
            // one.
            PrepareOutcome::Promised { .. } => Ok(self.role.record(DECREE).cloned()),
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Err(self.role.promised()),
        }
    }

    /// Phase 2b. `Accept(ballot, value)` arrived. Accept unless a higher
    /// ballot was promised.
    fn on_accept(&mut self, ballot: Ballot, value: Command) -> Result<(), Ballot> {
        match self.role.admit(ballot, DECREE) {
            AcceptOutcome::Admitted => {
                // Accepting at a ballot is also promising it: an acceptor
                // that votes at ballot 7 must refuse ballot 5 afterwards,
                // exactly as if it had promised 7. The role keeps the two
                // scalars separate and the caller raises the promise first,
                // so the durable batch always carries the promise ahead of
                // the record it covers.
                self.role.set_promise(ballot, &mut self.disk);
                self.role
                    .record_accepted(DECREE, ballot, value, &mut self.disk);
                Ok(())
            }
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => Err(self.role.promised()),
        }
    }
}

fn fresh_acceptors() -> Vec<AcceptorNode> {
    vec![
        AcceptorNode::new(A),
        AcceptorNode::new(B),
        AcceptorNode::new(C),
    ]
}

fn acceptor(acceptors: &mut [AcceptorNode], id: NodeId) -> &mut AcceptorNode {
    acceptors
        .iter_mut()
        .find(|a| a.id == id)
        .expect("a known acceptor")
}

/// What one proposer's attempt produced: the value it ended up proposing
/// (its own, or one P2c made it adopt) and the value chosen, if any.
struct Attempt {
    proposed: Command,
    chosen: Option<Command>,
}

/// One proposer runs one ballot from start to finish. `phase1_reach` and
/// `phase2_reach` say which acceptors each phase's messages reach — the
/// only "network" this example has, and how a scenario makes a proposer
/// crash half-way (its `Accept` reaches one acceptor and then nobody).
fn run_proposer(
    ballot: Ballot,
    my_value: &Command,
    acceptors: &mut [AcceptorNode],
    phase1_reach: &[NodeId],
    phase2_reach: &[NodeId],
) -> Attempt {
    // The membership and its quorum rule. Every quorum question in
    // paros-core crosses this one boundary: the proposer never counts
    // promises against a number, it asks the configuration whether the
    // set of voters is a quorum.
    let config = AcceptorConfig::new(vec![A, B, C], QuorumSystem::Majority);
    let mut proposer: Proposer<NodeId, Command> = Proposer::new();
    let Some(candidate) = phase1(
        &mut proposer,
        ballot,
        my_value,
        &config,
        acceptors,
        phase1_reach,
    ) else {
        return Attempt {
            proposed: my_value.clone(),
            chosen: None,
        };
    };
    let chosen = phase2(
        &mut proposer,
        ballot,
        &candidate,
        &config,
        acceptors,
        phase2_reach,
    );
    Attempt {
        proposed: candidate,
        chosen,
    }
}

/// Steps 1 to 5: start the ballot, send `Prepare`, collect promises until
/// a quorum holds, then run P2c. Returns the value Phase 2 must propose, or
/// `None` when the ballot was refused.
fn phase1(
    proposer: &mut Proposer<NodeId, Command>,
    ballot: Ballot,
    my_value: &Command,
    config: &AcceptorConfig,
    acceptors: &mut [AcceptorNode],
    reach: &[NodeId],
) -> Option<Command> {
    // ---- Steps 1 and 2: start the ballot, send Prepare -------------------
    //
    // `me: None`: this proposer is not itself an acceptor, so it casts no
    // vote of its own and its Prepare goes to all three. `prior` is the list
    // of configurations whose quorum Phase 1 needs: with a fixed membership
    // that is the one configuration (the matchmaker example is where it
    // grows). `from_slot` is the first slot the campaign recovers — the only
    // slot, here.
    let targets = proposer.open_phase1(
        Campaign {
            me: None,
            ballot,
            config: config.clone(),
            prior: vec![config.clone()],
            from_slot: DECREE,
        },
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert_eq!(targets, vec![A, B, C], "Phase 1 addresses every acceptor");
    println!(
        "ballot {}: prepare (my value {})",
        show_ballot(ballot),
        show(my_value)
    );

    // ---- Steps 3 and 4: collect promises; note when a quorum holds -------
    let mut quorum = false;
    for id in targets {
        if !reach.contains(&id) {
            println!("  {} -> (unreachable)", name(id));
            continue;
        }
        match acceptor(acceptors, id).on_prepare(ballot) {
            Ok(vote) => {
                match &vote {
                    Some((b, v)) => println!(
                        "  {} -> promise, had accepted {} at ballot {}",
                        name(id),
                        show(v),
                        show_ballot(*b)
                    ),
                    None => println!("  {} -> promise, nothing accepted", name(id)),
                }
                // The promise is folded into the tally as a one-slot
                // "page": the accepted record (if any) keyed by its slot,
                // no faulty records, and no continuation cursor because a
                // one-slot log always fits in one page.
                let accepted = vote
                    .map(|record| BTreeMap::from([(DECREE, record)]))
                    .unwrap_or_default();
                let fold =
                    proposer.fold_promise(id, ballot, DECREE, accepted, BTreeMap::new(), None);
                assert_eq!(
                    fold,
                    PromiseFold::Answered,
                    "a promise page is counted once"
                );
            }
            Err(promised) => {
                // A refusal means some acceptor already promised a higher
                // ballot. This proposer gives up at once — paros's node does
                // the same on a `Nack`: it steps down and re-campaigns above
                // the refuser after a randomized timeout. Safety does not
                // demand that: the remaining acceptors might still form a
                // Phase-1 quorum for this ballot ({B, C} is a majority even
                // if A refused). Aborting is a liveness policy: work at this
                // ballot is likely to be preempted by the higher one, and
                // retrying above it is what ends a duel between proposers.
                println!(
                    "  {} -> nack, already promised ballot {}: giving up",
                    name(id),
                    show_ballot(promised)
                );
                return None;
            }
        }
        // `phase1_won` takes the proposer's own promise so a candidate that
        // is also an acceptor cannot win below a promise it made meanwhile;
        // a proposer that is not an acceptor has only its own ballot.
        //
        // A quorum means Phase 1 *may* close: there are enough answers to
        // run P2c safely. It does not mean later promises are meaningless.
        // The Prepare went to everyone at once, and a promise that arrives
        // after the quorum can report an accepted value at a *higher*
        // ballot than any seen so far; folded before P2c runs, it changes
        // the value P2c selects (P2c takes the highest ballot reported).
        // This proposer keeps folding every promise that reaches it and
        // runs P2c only after the loop; paros's `ColocatedNode` instead
        // closes Phase 1 on the very fold that completes the quorum. Both
        // are safe — a value that is actually chosen is reported by some
        // member of *every* quorum — and once Phase 1 is closed, further
        // promises are irrelevant to this campaign.
        if !quorum && proposer.phase1_won(ballot) {
            println!("  phase 1 quorum reached");
            quorum = true;
        }
    }
    if !proposer.phase1_won(ballot) {
        println!("  no phase 1 quorum: giving up");
        return None;
    }

    // ---- Step 5: P2c, the value-selection rule ---------------------------
    //
    // Closing Phase 1 hands back the highest-ballot accepted value reported
    // per slot. The predicate answers "is this slot already known chosen
    // here?" — nothing is, in a fresh proposer. If the quorum reported a
    // value, the proposer's own value is set aside: some earlier ballot may
    // already have *chosen* that value (a quorum of accepts at that ballot
    // would intersect this promise quorum, so at least one promise would
    // report it), and proposing anything else could choose a second value.
    let outcome = proposer.close_phase1(|_| false);
    if let Some((reported_at, reported)) = outcome.recovered.get(&DECREE) {
        println!(
            "  P2c selected {} (accepted at ballot {}); my own {} is set aside",
            show(reported),
            show_ballot(*reported_at),
            show(my_value)
        );
        Some(reported.clone())
    } else {
        println!(
            "  P2c: nothing accepted below ballot {}, free to propose {}",
            show_ballot(ballot),
            show(my_value)
        );
        Some(my_value.clone())
    }
}

/// Steps 6 to 9: send `Accept`, collect `Accepted` until a quorum holds,
/// and report the chosen value.
fn phase2(
    proposer: &mut Proposer<NodeId, Command>,
    ballot: Ballot,
    candidate: &Command,
    config: &AcceptorConfig,
    acceptors: &mut [AcceptorNode],
    reach: &[NodeId],
) -> Option<Command> {
    // One round per slot per ballot. `own_vote: None`, again because this
    // proposer is no acceptor. The `Accepted` replies carry the value's
    // fingerprint so a vote for a different value at the same ballot could
    // never be miscounted.
    proposer.open_round(DECREE, ballot, candidate.clone(), None);
    println!("ballot {}: accept {}", show_ballot(ballot), show(candidate));
    let mut chosen = None;
    for id in config.phase2_addressees().to_vec() {
        if !reach.contains(&id) {
            println!("  {} -> (unreachable)", name(id));
            continue;
        }
        match acceptor(acceptors, id).on_accept(ballot, candidate.clone()) {
            Ok(()) => {
                println!("  {} -> accepted", name(id));
                let counted = proposer.fold_accepted(id, ballot, DECREE, candidate.fingerprint());
                assert!(counted, "an accept at the round's ballot and value counts");
            }
            Err(promised) => println!(
                "  {} -> nack, already promised ballot {}",
                name(id),
                show_ballot(promised)
            ),
        }
        // ---- Step 9: chosen --------------------------------------------
        //
        // The decision fires the instant a Phase-2 quorum holds; the
        // `Accept` already went to every acceptor, so the remaining
        // replies arrive afterwards and are simply counted.
        if chosen.is_none()
            && let Some((at, decided)) = proposer.decided(DECREE, config)
        {
            assert_eq!(
                at, ballot,
                "a decision is counted at the round's own ballot"
            );
            println!("chosen {} at ballot {}", show(&decided), show_ballot(at));
            chosen = Some(decided);
        }
    }
    if chosen.is_some() {
        proposer.close_round(DECREE);
    } else {
        println!(
            "  no phase 2 quorum: {} is accepted somewhere but chosen nowhere",
            show(candidate)
        );
    }
    chosen
}

/// The agreement invariant, read off the acceptors' durable state: once
/// `chosen` is chosen at `at`, every record at a ballot at or above `at`
/// carries `chosen`. (A record *below* the choosing ballot may still hold
/// some other value — the last scenario shows one — and that is fine: it
/// can never gain a quorum, because a quorum of acceptors promised past it.)
fn assert_agreement(acceptors: &[AcceptorNode], at: Ballot, chosen: &Command) {
    for acceptor in acceptors {
        if let Some((recorded_at, recorded)) = acceptor.role.record(DECREE)
            && *recorded_at >= at
        {
            assert_eq!(
                recorded,
                chosen,
                "{} holds a different value at ballot {}",
                name(acceptor.id),
                show_ballot(*recorded_at)
            );
        }
    }
}

/// The happy path: three empty acceptors, one proposer, one value.
fn scenario_empty_state() {
    println!("== 1. the empty-state happy path ==");
    let mut acceptors = fresh_acceptors();
    let attempt = run_proposer(
        ballot(1, 5),
        &value("alpha"),
        &mut acceptors,
        &[A, B, C],
        &[A, B, C],
    );
    assert_eq!(attempt.proposed, value("alpha"));
    assert_eq!(attempt.chosen, Some(value("alpha")));
    assert_agreement(&acceptors, ballot(1, 5), &value("alpha"));
    println!();
}

/// The crucial scenario. A proposer at ballot 3 got as far as one accept
/// (at A) and died. A later proposer at ballot 7 arrives with a *different*
/// client value, and its Phase-1 quorum includes A. It must adopt
/// `"old-value"`: it cannot know whether ballot 3 also reached B or C
/// before dying (it did not, here — but a Promise from A alone cannot tell
/// the difference between "accepted at A only" and "chosen by A and C"),
/// so proposing `"new-value"` could choose a second value.
fn scenario_adopt_prior_accept() {
    println!("== 2. a higher ballot must adopt a value it finds accepted ==");
    let mut acceptors = fresh_acceptors();
    let first = run_proposer(
        ballot(3, 5),
        &value("old-value"),
        &mut acceptors,
        &[A, B, C],
        &[A],
    );
    assert!(first.chosen.is_none(), "one accept is not a choice");
    assert_eq!(
        acceptor(&mut acceptors, A).role.record(DECREE),
        Some(&(ballot(3, 5), value("old-value"))),
        "A holds the old proposer's accept"
    );
    println!("  (proposer 5 crashes here)");

    let second = run_proposer(
        ballot(7, 8),
        &value("new-value"),
        &mut acceptors,
        &[A, B],
        &[A, B, C],
    );
    assert_eq!(
        second.proposed,
        value("old-value"),
        "P2c: the new proposer adopted the accepted value"
    );
    assert_eq!(second.chosen, Some(value("old-value")));
    assert_ne!(
        second.chosen,
        Some(value("new-value")),
        "the new client value was never proposed"
    );
    assert_agreement(&acceptors, ballot(7, 8), &value("old-value"));
    for acceptor in &acceptors {
        assert_eq!(
            acceptor.role.record(DECREE),
            Some(&(ballot(7, 8), value("old-value"))),
            "every acceptor re-accepted the adopted value at the new ballot"
        );
    }
    println!();
}

/// The same start, but ballot 7's Phase-1 quorum is {B, C}, which never
/// saw ballot 3's accept. Now P2c finds nothing and `"new-value"` is
/// proposed and chosen. That is still safe, and the reason is the promise:
/// B and C promised 7, so ballot 3 can never gain a second vote — as the
/// old proposer discovers when it wakes up and retries its `Accept`.
///
/// Both scenarios end with exactly one value chosen. Which one depends on
/// what the Phase-1 quorum happened to see, and Paxos is fine with that: it
/// promises that a value once chosen stays chosen, not that any particular
/// proposer wins.
fn scenario_prior_accept_never_chosen() {
    println!("== 3. an accepted value the quorum never saw is safely fenced out ==");
    let mut acceptors = fresh_acceptors();
    let first = run_proposer(
        ballot(3, 5),
        &value("old-value"),
        &mut acceptors,
        &[A, B, C],
        &[A],
    );
    assert!(first.chosen.is_none());
    println!("  (proposer 5 pauses here)");

    let second = run_proposer(
        ballot(7, 8),
        &value("new-value"),
        &mut acceptors,
        &[B, C],
        &[B, C],
    );
    assert_eq!(second.proposed, value("new-value"));
    assert_eq!(second.chosen, Some(value("new-value")));
    assert_agreement(&acceptors, ballot(7, 8), &value("new-value"));

    println!("  (proposer 5 wakes up and retries its Accept at ballot 3)");
    let late = acceptor(&mut acceptors, B).on_accept(ballot(3, 5), value("old-value"));
    println!(
        "  B -> nack, already promised ballot {}",
        show_ballot(late.expect_err("B promised ballot 7"))
    );
    assert_eq!(late, Err(ballot(7, 8)));
    // "old-value" is still accepted at A — accepted is not chosen — and it
    // can never be chosen: it would need a second vote, and both other
    // acceptors are fenced above it.
    assert_eq!(
        acceptor(&mut acceptors, A).role.record(DECREE),
        Some(&(ballot(3, 5), value("old-value")))
    );
    println!();
}

fn main() {
    scenario_empty_state();
    scenario_adopt_prior_accept();
    scenario_prior_accept_never_chosen();
    println!("all assertions held");
}
