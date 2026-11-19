//! Model-based trace checking (#1198 (trace checking)): TLC checks that
//! each walk is a behaviour of its flavor's TLA+ model, and refuses the
//! walks of a planted bug at the first step that the model forbids.
//!
//! A trace check costs one TLC run, about a second, so a flavor checks a
//! few short walks, not the sixty seeds of `tests/flavors.rs`. The knobs of
//! a longer run: `TRACE_SEEDS` (seeds per flavor, 4), `TRACE_FROM` (the
//! first seed, 1), `TRACE_CHAOS` (chaos steps of a cell walk, 40), and
//! `TRACE_DUMP` (print every event of each cell walk).

use consensus_sim::cell::{Cell, CellEvent, Flavor, Mutant, Spec};
use consensus_sim::decree::{self, DecreeEvent, DecreeModel, DecreeMutant};
use consensus_sim::trace::{Verdict, require_tlc, spec};
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
    require_tlc(&check);
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
    require_tlc(&spec("decree", "DecreeTrace"));
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
    require_tlc(&spec("decree", "DecreeTrace"));
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
    cell_walk_of(flavor, knob("TRACE_CHAOS", 40))
}

/// A cell walk of `chaos` chaos steps.
fn cell_walk_of(flavor: Flavor, chaos: u64) -> Walk<Cell> {
    Walk::<Cell>::new()
        .chaos_steps(chaos)
        .quiet_steps(10)
        .env_prefix(None)
        .model(move |cx| Cell::build(flavor.clone(), cx))
}

/// The events of one walk that `spec` keeps.
fn kept(spec: Spec, run: &moonpool_walk::Run<Cell>) -> Vec<CellEvent> {
    run.events().filter(|e| spec.keeps(e)).cloned().collect()
}

/// TLC accepts every checked walk of `flavor` against `spec`.
fn accepts(spec: Spec, flavor: &Flavor) {
    let check = spec.check();
    require_tlc(&check);
    let first = knob("TRACE_FROM", 1);
    for seed in first..first + seeds() {
        let run = cell_walk(flavor.clone())
            .replay(seed)
            .unwrap_or_else(|failure| panic!("{failure}"));
        let events = kept(spec, &run);
        let trace = spec.trace(flavor, &events);
        eprintln!(
            "{spec:?} {} seed {seed}: {} events",
            flavor.name,
            trace.len()
        );
        if std::env::var_os("TRACE_DUMP").is_some() {
            for (i, event) in events.iter().enumerate() {
                eprintln!("  {i}: {event:?}");
            }
        }
        match check.check(&trace).expect("TLC gives a verdict") {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "{spec:?} {} seed {seed}: TLC refused event {} {:?}\n{}",
                flavor.name, rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => {
                panic!("{spec:?} {} seed {seed}: {other:?}", flavor.name)
            }
        }
    }
}

#[test]
fn tlc_accepts_the_multi_paxos_walks() {
    accepts(Spec::MultiPaxos, &Flavor::multi_paxos());
}

#[test]
fn tlc_accepts_the_multi_paxos_walks_on_five_with_truncation() {
    accepts(Spec::MultiPaxos, &Flavor::multi_paxos_five());
}

#[test]
fn tlc_accepts_the_grid_walks() {
    accepts(Spec::MultiPaxos, &Flavor::grid(2, 2));
}

#[test]
fn tlc_accepts_the_partition_walks() {
    accepts(Spec::MultiPaxos, &Flavor::partitions());
}

// Each flavor below is plain Multi-Paxos for the log, so its walks are
// checked against `multi-paxos/`, and against the model of its own rules.

#[test]
fn tlc_accepts_the_handoff_walks() {
    accepts(Spec::MultiPaxos, &Flavor::handoff());
    accepts(Spec::LeaderHandoff, &Flavor::handoff());
}

#[test]
fn tlc_accepts_the_proxy_leader_walks() {
    for flavor in [Flavor::proxies(), Flavor::proxies_on_grid()] {
        accepts(Spec::MultiPaxos, &flavor);
        accepts(Spec::ProxyLeader, &flavor);
    }
}

#[test]
fn tlc_accepts_the_replica_walks() {
    accepts(Spec::MultiPaxos, &Flavor::replicas());
}

#[test]
fn tlc_accepts_the_leaderless_read_walks() {
    accepts(Spec::MultiPaxos, &Flavor::reads());
}

#[test]
fn tlc_accepts_the_compartment_walks() {
    accepts(Spec::MultiPaxos, &Flavor::compartments());
    accepts(Spec::ProxyLeader, &Flavor::compartments());
}

#[test]
fn tlc_accepts_the_matchmaker_walks() {
    accepts(Spec::Matchmaker, &Flavor::matchmakers());
}

/// The first seed in `seeds` of `flavor` with `mutant` whose walk of
/// `chaos` chaos steps the oracles pass and `spec` refuses, with the event
/// TLC names.
fn first_refused_cell(
    spec: Spec,
    flavor: &Flavor,
    mutant: Mutant,
    seeds: std::ops::RangeInclusive<u64>,
    chaos: u64,
) -> Option<(u64, CellEvent, String)> {
    let check = spec.check();
    let flavor = flavor.clone().with(mutant);
    seeds.into_iter().find_map(|seed| {
        let run = cell_walk_of(flavor.clone(), chaos).replay(seed).ok()?;
        let events = kept(spec, &run);
        match check
            .check(&spec.trace(&flavor, &events))
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
    require_tlc(&Spec::MultiPaxos.check());
    let (seed, event, state) = first_refused_cell(
        Spec::MultiPaxos,
        &Flavor::multi_paxos(),
        Mutant::ForgetVotes,
        30..=40,
        40,
    )
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
    require_tlc(&Spec::MultiPaxos.check());
    let (seed, event, state) = first_refused_cell(
        Spec::MultiPaxos,
        &Flavor::multi_paxos(),
        Mutant::HideVotes,
        1..=30,
        40,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, CellEvent::Promise { ref votes, .. } if votes.is_empty()),
        "seed {seed}: TLC refused {event:?}, not a promise that hid its votes"
    );
}

/// A proxy leader crashes, and the driver keeps it running with its
/// rounds. TLC refuses the `Commit` of a round the crash lost. The refusal
/// needs a crash between a fan-out and its acks, and a commit before the
/// proxy sends the round again: long walks, and a few seeds.
#[test]
fn tlc_refuses_a_proxy_that_keeps_its_rounds_across_a_crash() {
    require_tlc(&Spec::ProxyLeader.check());
    let (seed, event, state) = first_refused_cell(
        Spec::ProxyLeader,
        &Flavor::proxies(),
        Mutant::KeepProxyRounds,
        10..=20,
        200,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, CellEvent::Commit { .. }),
        "seed {seed}: TLC refused {event:?}, not a commit"
    );
}

/// A leader hands on its ballot with an empty tail. TLC refuses the
/// payload: it must be the leader's frontier, decided and pending slots
/// (`Relinquish`).
#[test]
fn tlc_refuses_a_handoff_that_drops_its_tail() {
    require_tlc(&Spec::LeaderHandoff.check());
    let (seed, event, state) = first_refused_cell(
        Spec::LeaderHandoff,
        &Flavor::handoff(),
        Mutant::RewindHandoff,
        1..=30,
        40,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, CellEvent::Relinquish { .. }),
        "seed {seed}: TLC refused {event:?}, not a handoff"
    );
}

/// A leader asks for GC as soon as it leads. TLC refuses the request of a
/// leader whose configuration has not learned the slots below its
/// frontier (`node/gc.rs`).
#[test]
fn tlc_refuses_an_early_gc() {
    require_tlc(&Spec::Matchmaker.check());
    let (seed, event, state) = first_refused_cell(
        Spec::Matchmaker,
        &Flavor::matchmakers(),
        Mutant::EarlyGc,
        1..=30,
        40,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, CellEvent::GcAsk { .. }),
        "seed {seed}: TLC refused {event:?}, not a GC request"
    );
}
