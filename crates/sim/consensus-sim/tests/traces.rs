//! Model-based trace checking (#1198 (trace checking)): TLC checks that
//! each walk is a behaviour of its flavor's TLA+ model, and refuses the
//! walks of a planted bug at the first step that the model forbids.
//!
//! A trace check costs one TLC run, about a second, so a flavor checks a
//! few short walks with many faults, not the sixty seeds of
//! `tests/flavors.rs`.

use consensus_sim::decree::{self, DecreeEvent, DecreeModel, DecreeMutant};
use consensus_sim::trace::{Verdict, spec, tlc_or_skip};
use moonpool_walk::Walk;
use paros_consensus::decree::Phase1Rule;

/// Seeds per flavor that TLC checks.
const SEEDS: u64 = 4;

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
        for seed in 1..=SEEDS {
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
