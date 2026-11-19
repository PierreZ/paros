//! Model-based trace checking (#1198 (trace checking)): TLC checks that
//! each walk is a behaviour of its flavor's TLA+ model, and refuses the
//! walks of a planted bug at the first step that the model forbids.
//!
//! A trace check costs one TLC run, about a second, so a flavor checks a
//! few short walks, not the sixty seeds of `tests/flavors.rs`. The knobs of
//! a longer run: `TRACE_SEEDS` (seeds per flavor, 4), `TRACE_FROM` (the
//! first seed, 1), `TRACE_CHAOS` (chaos steps of a cell walk, 40), and
//! `TRACE_DUMP` (print every event of each cell walk).

use consensus_sim::cell::{self, Cell, CellEvent, Flavor, Mutant};
use consensus_sim::decree::{self, DecreeEvent, DecreeModel, DecreeMutant};
use consensus_sim::trace::{Verdict, spec, tlc_or_skip};
use moonpool_walk::Walk;
use paros_consensus::decree::Phase1Rule;

/// The value of the knob `name`, or `default`.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// Seeds per flavor that TLC checks.
fn seeds() -> u64 {
    knob("TRACE_SEEDS", 4)
}

/// Short decree walks with many crashes: a trace of a few dozen events.
fn decree_walk(rule: Phase1Rule, mutant: DecreeMutant) -> Walk<DecreeModel> {
    Walk::<DecreeModel>::new()
        .chaos_steps(150)
        .crash_weights(12, 12)
        .env_prefix(None)
        .model(move |_| DecreeModel::build(rule, mutant))
}

#[test]
fn tlc_accepts_the_decree_walks() {
    let check = spec("decree", "DecreeTrace");
    if !tlc_or_skip(&check) {
        return;
    }
    for rule in [Phase1Rule::Quorum, Phase1Rule::EveryAcceptor] {
        for seed in 1..=seeds() {
            let run = decree_walk(rule, DecreeMutant::None)
                .replay(seed)
                .unwrap_or_else(|failure| panic!("{failure}"));
            let trace = decree::trace(rule, run.events());
            assert!(trace.len() >= 10, "seed {seed}: {} events", trace.len());
            eprintln!("{rule:?} seed {seed}: {} events", trace.len());
            let verdict = check.check(&trace).expect("TLC gives a verdict");
            assert_eq!(verdict, Verdict::Accepted, "{rule:?} seed {seed}");
        }
    }
}

/// The first seed in `1..=seeds` whose walk the oracles pass and TLC
/// refuses, with the event TLC names.
fn first_refused(mutant: DecreeMutant, seeds: u64) -> Option<(u64, DecreeEvent, String)> {
    let check = spec("decree", "DecreeTrace");
    (1..=seeds).find_map(|seed| {
        let run = decree_walk(Phase1Rule::Quorum, mutant).replay(seed).ok()?;
        let events: Vec<DecreeEvent> = run.events().copied().collect();
        let trace = decree::trace(Phase1Rule::Quorum, &events);
        match check.check(&trace).expect("TLC gives a verdict") {
            Verdict::Rejected(rejection) => Some((seed, events[rejection.index], rejection.state)),
            _ => None,
        }
    })
}

/// The driver drops the vote from a promise. The oracle sees a bug only
/// when a second value is chosen; TLC refuses the promise itself.
#[test]
fn tlc_refuses_a_promise_that_hides_its_vote() {
    if !tlc_or_skip(&spec("decree", "DecreeTrace")) {
        return;
    }
    let (seed, event, state) =
        first_refused(DecreeMutant::HideVote, 20).expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    let DecreeEvent::Promise { vote: None, .. } = event else {
        panic!("seed {seed}: TLC refused {event:?}, not a promise that hid a vote\n{state}");
    };
}

/// A crash loses a promise. The oracle catches it about once in a thousand
/// seeds; TLC refuses the first step below the lost promise.
#[test]
fn tlc_refuses_an_acceptor_that_forgot_its_promise() {
    if !tlc_or_skip(&spec("decree", "DecreeTrace")) {
        return;
    }
    let (seed, event, state) =
        first_refused(DecreeMutant::ForgetPromise, 40).expect("TLC refuses a seed of the mutant");
    assert!(
        matches!(
            event,
            DecreeEvent::Promise { .. } | DecreeEvent::Accept { .. }
        ),
        "seed {seed}: TLC refused {event:?}, not an acceptor step\n{state}"
    );
}

/// Short cell walks of `flavor`: a trace of a few hundred events.
fn cell_walk(flavor: Flavor) -> Walk<Cell> {
    Walk::<Cell>::new()
        .chaos_steps(knob("TRACE_CHAOS", 40))
        .quiet_steps(10)
        .env_prefix(None)
        .model(move |cx| Cell::build(flavor.clone(), cx))
}

/// The trace of one cell walk of `flavor`.
fn cell_trace(flavor: &Flavor, events: &[CellEvent]) -> consensus_sim::trace::Trace {
    cell::trace::trace(flavor.acceptors, flavor.quorum, events)
}

/// TLC accepts every checked walk of `flavor` against `MultiPaxosTrace`.
fn accepts_multi_paxos(flavor: &Flavor) {
    let check = spec("multi-paxos", "MultiPaxosTrace");
    if !tlc_or_skip(&check) {
        return;
    }
    let first = knob("TRACE_FROM", 1);
    for seed in first..=seeds() {
        let run = cell_walk(flavor.clone())
            .replay(seed)
            .unwrap_or_else(|failure| panic!("{failure}"));
        let events: Vec<CellEvent> = run.events().cloned().collect();
        let trace = cell_trace(flavor, &events);
        eprintln!("{} seed {seed}: {} events", flavor.name, trace.len());
        if std::env::var_os("TRACE_DUMP").is_some() {
            for (i, event) in events.iter().enumerate() {
                eprintln!("  {i}: {event:?}");
            }
        }
        match check.check(&trace).expect("TLC gives a verdict") {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "{} seed {seed}: TLC refused event {} {:?}\n{}",
                flavor.name, rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => {
                panic!("{} seed {seed}: {other:?}", flavor.name)
            }
        }
    }
}

#[test]
fn tlc_accepts_the_multi_paxos_walks() {
    accepts_multi_paxos(&Flavor::multi_paxos());
}

#[test]
fn tlc_accepts_the_multi_paxos_walks_on_five_with_truncation() {
    accepts_multi_paxos(&Flavor::multi_paxos_five());
}

#[test]
fn tlc_accepts_the_grid_walks() {
    accepts_multi_paxos(&Flavor::grid(2, 2));
}

#[test]
fn tlc_accepts_the_partition_walks() {
    accepts_multi_paxos(&Flavor::partitions());
}

// The flavors below have a model of their own in `specs/tla/` (the
// handoff, the proxy leader, the compartments, the reads). Their walks are
// checked against the Multi-Paxos model here: every one of them is plain
// Multi-Paxos for the log, the rules of their own models are not traced
// yet.

#[test]
fn tlc_accepts_the_handoff_walks() {
    accepts_multi_paxos(&Flavor::handoff());
}

#[test]
fn tlc_accepts_the_proxy_leader_walks() {
    accepts_multi_paxos(&Flavor::proxies());
    accepts_multi_paxos(&Flavor::proxies_on_grid());
}

#[test]
fn tlc_accepts_the_replica_walks() {
    accepts_multi_paxos(&Flavor::replicas());
}

#[test]
fn tlc_accepts_the_leaderless_read_walks() {
    accepts_multi_paxos(&Flavor::reads());
}

#[test]
fn tlc_accepts_the_compartment_walks() {
    accepts_multi_paxos(&Flavor::compartments());
}

/// The first seed in `1..=seeds` of `flavor` with `mutant` whose walk the
/// oracles pass and TLC refuses, with the event TLC names.
fn first_refused_cell(
    flavor: &Flavor,
    mutant: Mutant,
    seeds: u64,
) -> Option<(u64, CellEvent, String)> {
    let check = spec("multi-paxos", "MultiPaxosTrace");
    let flavor = flavor.clone().with(mutant);
    (1..=seeds).find_map(|seed| {
        let run = cell_walk(flavor.clone()).replay(seed).ok()?;
        let events: Vec<CellEvent> = run.events().cloned().collect();
        match check
            .check(&cell_trace(&flavor, &events))
            .expect("TLC gives a verdict")
        {
            Verdict::Rejected(rejection) => {
                Some((seed, events[rejection.index].clone(), rejection.state))
            }
            _ => None,
        }
    })
}

/// A disk loses its votes above the chosen prefix in a crash. The oracle
/// needs hundreds of seeds to see two values chosen; TLC refuses the first
/// promise that under-reports, or the vote that the lost one hid.
#[test]
fn tlc_refuses_a_disk_that_forgets_its_votes() {
    if !tlc_or_skip(&spec("multi-paxos", "MultiPaxosTrace")) {
        return;
    }
    let (seed, event, state) = first_refused_cell(&Flavor::multi_paxos(), Mutant::ForgetVotes, 30)
        .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, CellEvent::Promise { .. } | CellEvent::Accept { .. }),
        "seed {seed}: TLC refused {event:?}, not an acceptor step"
    );
}

/// The driver drops the votes from every promise. TLC refuses the first
/// promise whose report is not the acceptor's votes.
#[test]
fn tlc_refuses_a_promise_that_hides_its_votes() {
    if !tlc_or_skip(&spec("multi-paxos", "MultiPaxosTrace")) {
        return;
    }
    let (seed, event, state) = first_refused_cell(&Flavor::multi_paxos(), Mutant::HideVotes, 30)
        .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, CellEvent::Promise { ref votes, .. } if votes.is_empty()),
        "seed {seed}: TLC refused {event:?}, not a promise that hid its votes"
    );
}
