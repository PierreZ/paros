//! **Single-decree Paxos: three machines agree on one value, and never
//! change their minds.**
//!
//! Run it: `cargo run -p paros-core --example single_decree`
//!
//! This is the first of four executable lessons
//! (`single_decree` → `multi_paxos` → `matchmaker` → `flexible_quorums`).
//! Each one drives the *real* composable roles of `paros-core` — here
//! [`Proposer`] and [`Acceptor`] — by hand, with direct function calls
//! standing in for the network and a `Vec` standing in for the disk. There
//! is no second Paxos implementation here: the code that decides is the
//! library's; this file only delivers messages and prints what happened.
//! No prior reading is assumed: every term is defined the first time it is
//! used, and the paper is only a pointer at the end.
//!
//! # The problem
//!
//! Several machines must agree on **one** value — say, "which of us is the
//! primary" — and once they agree, the answer must never change, even
//! though any machine may crash at any moment, any message may be lost or
//! arrive late, and no clock can be trusted. The obvious design, "one
//! machine decides and tells the others", breaks the moment that machine
//! crashes half-way through telling them: some know the answer, some do
//! not, and whoever takes over may decide differently. Paxos is the
//! protocol that makes agreement survive all of that. The version in this
//! file decides a single value once — hence *single-decree*; the next
//! lesson repeats it slot by slot to build a log.
//!
//! # The parties
//!
//! - A **proposer** wants a value chosen. It starts the protocol, sends the
//!   messages and counts the replies. Any machine may be a proposer, several
//!   may try at the same time, and a proposer may crash at any point. Two
//!   appear in this file, numbered 5 and 8.
//! - An **acceptor** votes. It keeps two things on disk: the highest ballot
//!   it has *promised* and the value it has *accepted* (both defined
//!   below). Acceptors are the memory of the protocol — whatever must
//!   survive a crash lives there. There are three, `A`, `B` and `C`, so a
//!   **majority** is any two of them.
//! - A **learner** is whoever needs to know the outcome. Here the proposer
//!   is also the learner: it counts the votes and announces "chosen".
//!
//! In a real cluster one machine usually plays every role at once (the
//! next lesson does that); keeping them apart here makes each step easier
//! to see.
//!
//! # Ballots: naming an attempt so attempts can be compared
//!
//! Every attempt a proposer makes is tagged with a **ballot** (the papers
//! say *proposal number* or *round*). A ballot is not the value being
//! proposed: it is the *name* of one attempt to propose. Two properties
//! carry the whole protocol:
//!
//! - **Unique.** No two attempts, by the same proposer or by different
//!   ones, may share a ballot. Paros's [`Ballot`] is the pair
//!   `(round, node)`: the ballot printed as `3.5` is round 3 minted by
//!   proposer 5, and `7.8` is round 7 minted by proposer 8. Two proposers
//!   can never collide, because the second half always differs. Without
//!   uniqueness an acceptor could not tell two attempts apart, and a vote
//!   cast for one could be counted for the other.
//! - **Totally ordered.** Any two ballots compare: `7.8` is higher than
//!   `3.5` (rounds compare first, the proposer id breaks ties). Every rule
//!   below says "higher" or "lower" ballot, so this ordering is what lets
//!   an acceptor say "I ignore anything older than this" and a proposer
//!   say "this is the newest thing anyone told me".
//!
//! # Phase 1: ask permission, and learn the past
//!
//! The proposer sends `Prepare(ballot)` to every acceptor. An acceptor
//! that has not already promised a higher ballot answers with a
//! **promise**: a durable commitment never to accept a value at any
//! *lower* ballot from now on. The promise is what fences out older
//! attempts — once `B` has promised `7.8`, the crashed-and-revived
//! proposer 5 can no longer get `B`'s vote at `3.5` (scenario 3 shows that
//! very refusal).
//!
//! The promise also carries a report: **the value this acceptor has
//! already accepted, and at which ballot**, if any. It has to, because a
//! proposer cannot see the whole cluster. A value may already have been
//! chosen (defined below) under an earlier ballot without this proposer
//! ever hearing of it — the earlier proposer may have crashed after the
//! votes were cast and before it told anyone. Asking the acceptors what
//! they voted for is the only way to find out.
//!
//! An acceptor that already promised a higher ballot refuses (a `Nack`)
//! and says which ballot it promised, so the refused proposer can retry
//! above it. The proposer needs promises from a **majority** — two of
//! three — before it may go on; why a majority is the right number is the
//! subject of the last section.
//!
//! # P2c: the rule that makes it safe
//!
//! With a majority of promises in hand the proposer picks the value it will
//! propose, and this is the one rule everything rests on (the paper calls
//! it *P2c*):
//!
//! - If **any** promise reported an accepted value, the proposer **must**
//!   propose the reported value with the **highest ballot**; its own value
//!   is set aside.
//! - Only if **no** promise reported anything may it propose its own value.
//!
//! What breaks without it, in the numbers of scenario 2: proposer 5 at
//! ballot `3.5` proposed `"old-value"`, `A` accepted it, and proposer 5
//! crashed. Proposer 8 at ballot `7.8` wants `"new-value"` and gets
//! promises from `A` and `B`; `A` reports `"old-value"` at `3.5`. From
//! where proposer 8 stands, "`A` accepted it and nobody else did" looks
//! *identical* to "`A` and `C` both accepted it" — `C` did not answer, and
//! in the second world `"old-value"` is already chosen. If proposer 8 went
//! ahead with `"new-value"`, `A` and `B` would accept it at `7.8` and the
//! cluster would have chosen two different values. Adopting `"old-value"`
//! is safe in both worlds: if it was chosen, re-proposing it changes
//! nothing; if it was not, choosing it now is as good as any value. The
//! "highest ballot" part matters when two promises report *different*
//! values: the higher-ballot one is the more recent attempt, which by the
//! same argument is the only one that could have been chosen.
//!
//! # Phase 2: cast the votes
//!
//! The proposer sends `Accept(ballot, value)` to every acceptor. An
//! acceptor **accepts** — writes the value and the ballot to disk and
//! replies `Accepted` — unless it has meanwhile promised a *higher*
//! ballot, in which case it refuses. Accepting is a vote, and one vote
//! proves nothing: in scenario 2, `A` holds an accepted value that was
//! never chosen.
//!
//! # Chosen: the point of no return
//!
//! A value is **chosen** the moment a majority of acceptors have accepted
//! it *at the same ballot*. Nobody may know yet — the replies may still be
//! in flight, the proposer may have crashed — but the fact is now written
//! on a majority of disks, and that is what makes it permanent:
//!
//! - Any later proposer needs a majority of promises before it proposes.
//! - Any two majorities of three share at least one acceptor (pick any two
//!   of `A`, `B`, `C`, then any other two: they always overlap).
//! - So at least one acceptor in the later proposer's promise majority was
//!   in the accepting majority, and its promise reports the chosen value.
//! - P2c then forces the later proposer to re-propose that value.
//!
//! That chain is the whole safety argument, and the majority is what holds
//! it together: a proposer that closed Phase 1 on one promise, or declared
//! a value chosen on one vote, could miss the acceptors that know. Once
//! chosen, a value cannot change, because every future ballot is bound to
//! re-propose it. (A majority is only *one* way to get the overlap; the
//! `flexible_quorums` lesson shows that the overlap, not the count, is
//! what matters.)
//!
//! # What the three scenarios show
//!
//! 1. **The happy path.** Three fresh acceptors; proposer 5 at ballot
//!    `1.5` wants `"alpha"`. No promise reports anything, P2c lets it
//!    propose its own value, all three accept, `"alpha"` is chosen.
//! 2. **A higher ballot must adopt a value it finds accepted.** Proposer 5
//!    at `3.5` gets one accept (`A`) and dies. Proposer 8 at `7.8` wants
//!    `"new-value"`, hears from `A` and `B`, and is made to propose
//!    `"old-value"` instead. Exactly one value ends up chosen.
//! 3. **A value the majority never saw is fenced out.** Same start, but
//!    proposer 8's promises come from `B` and `C`, neither of which ever
//!    saw `"old-value"`. P2c finds nothing and `"new-value"` is chosen —
//!    and when proposer 5 wakes up and retries its `Accept` at `3.5`, `B`
//!    refuses: it promised `7.8`. `"old-value"` stays accepted at `A`
//!    alone, forever one vote short.
//!
//! Scenarios 2 and 3 start identically and end with different values, and
//! both are correct: Paxos promises that a value once chosen stays chosen,
//! never that any particular proposer wins.
//!
//! Further reading: Lamport, *Paxos Made Simple* (2001) — the two phases,
//! the promise and the P2c rule, stated in a few pages.

use std::collections::BTreeMap;

use paros_core::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_core::proposer::{Campaign, PromiseFold, Proposer};
use paros_core::{
    AcceptorConfig, AcceptorWrite, Ballot, ClientId, ClientSeq, Command, Entry, Fingerprint,
    NodeId, QuorumSystem, Slot, Value,
};

/// The one decision. Paros's roles are written over a *log* of numbered
/// slots (the next lesson fills many); single-decree Paxos is the same
/// roles over a log with exactly one slot, number 0.
const DECREE: Slot = Slot(0);

/// The three acceptors. The proposers are separate parties (numbered 5 and
/// 8 below) that hold no vote of their own; a machine that is *both* a
/// proposer and an acceptor is the Multi-Paxos lesson's business.
const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const C: NodeId = NodeId(3);

fn ballot(round: u64, proposer: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(proposer),
    }
}

/// A client value: the thing the cluster is agreeing on. Paros's log value
/// type is [`Command`]; a client's command is an [`Entry`] whose
/// `(client, seq)` fields exist so Multi-Paxos can execute each request at
/// most once, and play no part here. An acceptor never looks inside a
/// value; a proposer only ever compares values by their [`Fingerprint`].
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

/// A ballot prints as `round.proposer`, so `7.8` is round 7 by proposer 8.
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

/// One acceptor machine: the [`Acceptor`] role plus the disk it writes to.
///
/// The role never touches storage itself. Every durable change it makes —
/// a raised promise, an accepted value — is pushed into the `Vec` the
/// caller hands it, as an [`AcceptorWrite`]. A real node flushes that batch
/// to disk *before* the reply leaves the machine. The order is essential:
/// a promise that was sent but not written could be forgotten by a crash,
/// and the rebooted acceptor could then accept a lower ballot it had sworn
/// to refuse — breaking the fence P2c relies on. Here memory stands in for
/// the disk, and the order is the same: write, then reply.
struct AcceptorNode {
    id: NodeId,
    role: Acceptor<Command>,
    disk: Vec<AcceptorWrite<Command>>,
}

impl AcceptorNode {
    fn new(id: NodeId) -> Self {
        Self {
            id,
            // An acceptor is born empty: nothing promised (ballot zero is
            // below every real ballot), nothing accepted, and none of the
            // later lessons' baggage (no compaction floor, no damaged
            // records).
            role: Acceptor::new(Ballot::zero(), BTreeMap::new(), DECREE, BTreeMap::new()),
            disk: Vec::new(),
        }
    }

    /// Phase 1, acceptor side: `Prepare(ballot)` arrived. Either promise —
    /// raise the durable promise to this ballot and report the value this
    /// acceptor has already accepted, if any — or refuse, telling the
    /// proposer which higher ballot is already promised (paros calls the
    /// refusal a `Nack`).
    fn on_prepare(&mut self, ballot: Ballot) -> Result<Option<(Ballot, Command)>, Ballot> {
        match self.role.prepare(ballot, DECREE, &mut self.disk) {
            // The promise is what makes P2c trustworthy: from now on this
            // acceptor refuses every lower ballot, so the value it reports
            // here is the *last word* it will ever say about ballots below
            // this one. Nothing older can sneak in after the report.
            PrepareOutcome::Promised { .. } => Ok(self.role.record(DECREE).cloned()),
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Err(self.role.promised()),
        }
    }

    /// Phase 2, acceptor side: `Accept(ballot, value)` arrived. Vote for it
    /// unless a higher ballot has been promised in the meantime.
    fn on_accept(&mut self, ballot: Ballot, value: Command) -> Result<(), Ballot> {
        match self.role.admit(ballot, DECREE) {
            AcceptOutcome::Admitted => {
                // Accepting at a ballot is also promising it: an acceptor
                // that votes at ballot 7 must refuse ballot 5 afterwards,
                // exactly as if it had promised 7 — otherwise a slow
                // proposer 5 could still collect votes behind ballot 7's
                // back. The role keeps the two scalars separate and the
                // caller raises the promise first, so the durable batch
                // always carries the promise ahead of the vote it covers.
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
/// (its own, or the one P2c made it adopt) and the value chosen, if any.
struct Attempt {
    proposed: Command,
    chosen: Option<Command>,
}

/// One proposer runs one ballot from start to finish. `phase1_reach` and
/// `phase2_reach` list which acceptors each phase's messages reach — the
/// only "network" this example has, and how a scenario makes a proposer
/// crash half-way (its `Accept` reaches one acceptor and then nobody).
fn run_proposer(
    ballot: Ballot,
    my_value: &Command,
    acceptors: &mut [AcceptorNode],
    phase1_reach: &[NodeId],
    phase2_reach: &[NodeId],
) -> Attempt {
    // The membership and its quorum rule: three acceptors, and "a quorum"
    // means a majority of them, two. Every quorum question in paros-core
    // crosses this one boundary: the proposer never compares a count
    // against a number, it asks the configuration whether the set of
    // acceptors that answered is a quorum.
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

/// Phase 1 plus P2c: start the ballot, send `Prepare`, collect promises
/// until a majority holds, then pick the value. Returns the value Phase 2
/// must propose, or `None` when the attempt had to give up.
fn phase1(
    proposer: &mut Proposer<NodeId, Command>,
    ballot: Ballot,
    my_value: &Command,
    config: &AcceptorConfig,
    acceptors: &mut [AcceptorNode],
    reach: &[NodeId],
) -> Option<Command> {
    // ---- Start the ballot and send Prepare -------------------------------
    //
    // `me: None`: this proposer is not itself an acceptor, so it casts no
    // vote of its own and its Prepare goes to all three. `prior` is the list
    // of configurations whose quorum Phase 1 needs: with a fixed membership
    // that is the one configuration (the matchmaker lesson is where it
    // grows). `from_slot` is the first slot the attempt asks about — the
    // only slot, here. The two empty maps are what this proposer already
    // knows about its own log: nothing, since it is not an acceptor.
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
        "ballot {}: Prepare goes to every acceptor; the proposer hopes to choose {}",
        show_ballot(ballot),
        show(my_value)
    );

    // ---- Collect promises; note the moment a majority holds --------------
    let mut quorum = false;
    for id in targets {
        if !reach.contains(&id) {
            println!("  {} -> (unreachable: the Prepare never arrives)", name(id));
            continue;
        }
        match acceptor(acceptors, id).on_prepare(ballot) {
            Ok(vote) => {
                match &vote {
                    Some((b, v)) => println!(
                        "  {} -> promise; reports it already accepted {} at ballot {}",
                        name(id),
                        show(v),
                        show_ballot(*b)
                    ),
                    None => println!("  {} -> promise; it has accepted nothing yet", name(id)),
                }
                // The promise is folded into the tally as a one-slot
                // "page": the accepted value (if any) keyed by its slot, no
                // damaged records, and no continuation cursor, because a
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
                // the same on a `Nack`: it steps down and retries above the
                // refuser's ballot after a randomized pause. Safety does not
                // demand giving up: the remaining acceptors might still form
                // a majority for this ballot ({B, C} is a majority even if A
                // refused). Giving up is a liveness policy: work at this
                // ballot is likely to be overtaken by the higher one, and
                // retrying above it is what ends a duel between proposers.
                println!(
                    "  {} -> nack: it already promised the higher ballot {}; this attempt gives up",
                    name(id),
                    show_ballot(promised)
                );
                return None;
            }
        }
        // `phase1_won` takes the proposer's own promise so a proposer that
        // is also an acceptor cannot win below a promise it made meanwhile;
        // a proposer that is not an acceptor has only its own ballot.
        //
        // A majority means Phase 1 *may* close: there are enough answers to
        // run P2c safely. It does not make later promises meaningless. The
        // Prepare went to everyone at once, and a promise that arrives after
        // the majority can report an accepted value at a *higher* ballot
        // than any seen so far; folded before P2c runs, it changes the value
        // P2c selects (P2c takes the highest ballot reported). This proposer
        // keeps folding every promise that reaches it and runs P2c only
        // after the loop; paros's `ColocatedNode` instead closes Phase 1 on
        // the very fold that completes the majority. Both are safe — a value
        // that is actually chosen is reported by some member of *every*
        // majority — and once Phase 1 is closed, further promises are
        // irrelevant to this attempt.
        if !quorum && proposer.phase1_won(ballot) {
            println!("  two promises: a majority of the three acceptors, Phase 1 may close");
            quorum = true;
        }
    }
    if !proposer.phase1_won(ballot) {
        println!("  fewer than two promises: no majority, this attempt gives up");
        return None;
    }

    // ---- P2c: pick the value -----------------------------------------------
    //
    // Closing Phase 1 hands back the highest-ballot accepted value reported
    // per slot. The predicate answers "is this slot already known chosen
    // here?" — nothing is, in a fresh proposer. If the majority reported a
    // value, the proposer's own value is set aside: some earlier ballot may
    // already have *chosen* that value (a majority of accepts at that ballot
    // would share an acceptor with this promise majority, so at least one
    // promise would report it), and proposing anything else could choose a
    // second value.
    let outcome = proposer.close_phase1(|_| false);
    if let Some((reported_at, reported)) = outcome.recovered.get(&DECREE) {
        println!(
            "  P2c: a promise reported {} accepted at ballot {}, so that is what must be proposed; {} is set aside",
            show(reported),
            show_ballot(*reported_at),
            show(my_value)
        );
        Some(reported.clone())
    } else {
        println!(
            "  P2c: no promise reported a value accepted below ballot {}, so the proposer may propose its own {}",
            show_ballot(ballot),
            show(my_value)
        );
        Some(my_value.clone())
    }
}

/// Phase 2: send `Accept`, collect votes until a majority holds, and report
/// the chosen value — or `None` when the votes never reached a majority.
fn phase2(
    proposer: &mut Proposer<NodeId, Command>,
    ballot: Ballot,
    candidate: &Command,
    config: &AcceptorConfig,
    acceptors: &mut [AcceptorNode],
    reach: &[NodeId],
) -> Option<Command> {
    // One voting round per slot per ballot. `own_vote: None`, again because
    // this proposer is no acceptor. Each `Accepted` reply carries the
    // value's fingerprint, so a vote for a different value at the same
    // ballot could never be miscounted as a vote for this one.
    proposer.open_round(DECREE, ballot, candidate.clone(), None, None);
    println!(
        "ballot {}: Accept goes to every acceptor, asking for a vote on {}",
        show_ballot(ballot),
        show(candidate)
    );
    let mut chosen = None;
    for id in config.phase2_addressees(None) {
        if !reach.contains(&id) {
            println!("  {} -> (unreachable: the Accept never arrives)", name(id));
            continue;
        }
        match acceptor(acceptors, id).on_accept(ballot, candidate.clone()) {
            Ok(()) => {
                println!(
                    "  {} -> accepted (one vote; not chosen until a majority has voted)",
                    name(id)
                );
                let counted = proposer.fold_accepted(id, ballot, DECREE, candidate.fingerprint());
                assert!(counted, "an accept at the round's ballot and value counts");
            }
            Err(promised) => println!(
                "  {} -> nack: it promised the higher ballot {} and refuses this vote",
                name(id),
                show_ballot(promised)
            ),
        }
        // ---- Chosen ----------------------------------------------------
        //
        // The decision fires the instant a majority has voted; the `Accept`
        // already went to every acceptor, so the remaining replies arrive
        // afterwards and are simply counted.
        if chosen.is_none()
            && let Some((at, decided)) = proposer.decided(DECREE, config)
        {
            assert_eq!(
                at, ballot,
                "a decision is counted at the round's own ballot"
            );
            println!(
                "  chosen: {} at ballot {} -- a majority voted for it, and it can never change now",
                show(&decided),
                show_ballot(at)
            );
            chosen = Some(decided);
        }
    }
    if chosen.is_some() {
        proposer.close_round(DECREE);
    } else {
        println!(
            "  fewer than two votes: {} is accepted somewhere, but chosen nowhere",
            show(candidate)
        );
    }
    chosen
}

/// The agreement property, read off the acceptors' disks: once `chosen` is
/// chosen at ballot `at`, every acceptor that holds a value at a ballot at
/// or above `at` holds `chosen`. (An acceptor may still hold some *other*
/// value at a ballot *below* the choosing one — scenario 3 ends that way —
/// and that is fine: the older value can never gain a majority, because a
/// majority of acceptors promised past its ballot.)
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

/// The happy path: three empty acceptors, one proposer, one value. Nothing
/// has been accepted anywhere, so P2c lets the proposer keep its own value,
/// and all three acceptors vote for it.
fn scenario_empty_state() {
    println!("== 1. the happy path: fresh acceptors, one proposer, one value ==");
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

/// The crucial scenario. Proposer 5 at ballot `3.5` got as far as one vote
/// (at `A`) and died. Proposer 8 at ballot `7.8` arrives with a *different*
/// client value, and its promise majority includes `A`. It must adopt
/// `"old-value"`: it cannot know whether ballot `3.5`'s `Accept` also
/// reached `B` or `C` before the crash (it did not, here — but a promise
/// from `A` alone reads the same in "accepted at `A` only" and in "chosen
/// by `A` and `C`"), so proposing `"new-value"` could choose a second
/// value.
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
    println!(
        "  (proposer 5 crashes here: A holds one vote for \"old-value\", and nobody else knows)"
    );

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

/// The same start, but ballot `7.8`'s promise majority is `{B, C}`, which
/// never saw ballot `3.5`'s vote. Now P2c finds nothing, and `"new-value"`
/// is proposed and chosen. That is still safe, and the reason is the
/// promise: `B` and `C` promised `7.8`, so ballot `3.5` can never gain a
/// second vote — as proposer 5 discovers when it wakes up and retries its
/// `Accept`.
///
/// Both scenarios end with exactly one value chosen. Which one depends on
/// what the promise majority happened to see, and Paxos is fine with that:
/// it promises that a value once chosen stays chosen, not that any
/// particular proposer wins.
fn scenario_prior_accept_never_chosen() {
    println!("== 3. an accepted value the majority never saw is safely fenced out ==");
    let mut acceptors = fresh_acceptors();
    let first = run_proposer(
        ballot(3, 5),
        &value("old-value"),
        &mut acceptors,
        &[A, B, C],
        &[A],
    );
    assert!(first.chosen.is_none());
    println!("  (proposer 5 pauses here: one vote at A, no majority)");

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

    println!("  (proposer 5 wakes up and retries its Accept at ballot 3.5)");
    let late = acceptor(&mut acceptors, B).on_accept(ballot(3, 5), value("old-value"));
    println!(
        "  B -> nack: it promised ballot {}, so ballot 3.5 can never get its vote",
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
