//! Model-based trace checking (#1198 (trace checking)): TLC checks that
//! each walk is a behaviour of its flavor's TLA+ model, and refuses the
//! walks of a planted bug at the first step that the model forbids.
//!
//! A trace check costs one TLC run, about a second, so a flavor checks a
//! few walks, not the sixty seeds of `tests/flavors.rs`: the first seeds,
//! and the seeds that meet the rules of the spec no checked walk met before
//! ([`Spec::rules`]). The knobs: `TRACE_SEEDS` (the first seeds that TLC
//! always checks, 1), `TRACE_FROM` (the first seed, 1), `TRACE_SEARCH`
//! (the seeds a flavor may search for its rules, 300), `TRACE_CHAOS`
//! (chaos steps of a cluster walk, 150), `TRACE_MAX_EVENTS` (the longest
//! trace TLC checks, 200), and `TRACE_DUMP` (print every event of each
//! checked cluster walk).

use consensus_sim::cluster::{Cluster, ClusterEvent, Flavor, Mutant, Spec};
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

/// The first seeds of a cluster flavor that TLC always checks.
fn seeds() -> u64 {
    knob("TRACE_SEEDS", 1)
}

/// The decree walks TLC checks for each Phase-1 rule.
const DECREE_SEEDS: u64 = 4;

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
        let (mut chosen, mut reported) = (false, false);
        for seed in 1..=DECREE_SEEDS {
            let run = decree_walk(rule, DecreeMutant::None)
                .replay(seed)
                .unwrap_or_else(|failure| panic!("{failure}"));
            for event in run.events() {
                chosen |= matches!(event, DecreeEvent::Chosen { .. });
                reported |= matches!(event, DecreeEvent::Promise { vote: Some(_), .. });
            }
            let trace = decree::trace(rule, run.events());
            assert!(trace.len() >= 10, "seed {seed}: {} events", trace.len());
            eprintln!("{rule:?} seed {seed}: {} events", trace.len());
            let verdict = check.check(&trace).expect("TLC gives a verdict");
            assert_eq!(verdict, Verdict::Accepted, "{rule:?} seed {seed}");
        }
        assert!(chosen, "{rule:?}: no checked walk chooses a value");
        assert!(reported, "{rule:?}: no checked promise reports a vote");
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

/// A cluster walk of `chaos` chaos steps.
fn cluster_walk_of(flavor: Flavor, chaos: u64) -> Walk<Cluster> {
    Walk::<Cluster>::new()
        .chaos_steps(chaos)
        .quiet_steps(10)
        .env_prefix(None)
        .model(move |cx| Cluster::build(flavor.clone(), cx))
}

/// The events of one walk that `spec` keeps.
fn kept(spec: Spec, run: &moonpool_walk::Run<Cluster>) -> Vec<ClusterEvent> {
    run.events().filter(|e| spec.keeps(e)).cloned().collect()
}

/// TLC accepts the checked walks of `flavor` against `spec`, and the
/// checked walks meet every rule of `spec` ([`Spec::rules`]).
///
/// The walk searches the seeds from `TRACE_FROM` on, at most
/// `TRACE_SEARCH` of them. A search walk costs milliseconds and its oracles
/// judge it. TLC costs a second for a hundred events and grows faster
/// than the trace, so it checks only the first `TRACE_SEEDS` seeds and
/// each seed that meets a rule no checked seed met before, and it skips a
/// walk of more than `TRACE_MAX_EVENTS` events. The test fails when the
/// search ends with a rule unmet: the spec would accept the walks with
/// that rule never judged.
fn accepts(spec: Spec, flavor: &Flavor) {
    let check = spec.check();
    require_tlc(&check);
    let rules = spec.rules();
    let mut unmet: Vec<_> = rules.iter().collect();
    let first = knob("TRACE_FROM", 1);
    let chaos = knob("TRACE_CHAOS", 150);
    let max_events = usize::try_from(knob("TRACE_MAX_EVENTS", 200)).expect("a size");
    let mut checked = 0;
    for seed in first..first + knob("TRACE_SEARCH", 300) {
        if unmet.is_empty() && checked >= seeds() {
            break;
        }
        let run = cluster_walk_of(flavor.clone(), chaos)
            .replay(seed)
            .unwrap_or_else(|failure| panic!("{failure}"));
        let events = kept(spec, &run);
        if events.len() > max_events {
            continue;
        }
        let before = unmet.len();
        unmet.retain(|rule| !rule.met(&events));
        if unmet.len() == before && checked >= seeds() {
            continue;
        }
        checked += 1;
        let trace = spec.trace(flavor, &events);
        eprintln!(
            "{spec:?} {} seed {seed}: {} events, {} rules met",
            flavor.name,
            trace.len(),
            rules.len() - unmet.len()
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
    let names: Vec<&str> = unmet.iter().map(|rule| rule.name).collect();
    assert!(
        names.is_empty(),
        "{spec:?} {}: no walk meets {names:?}",
        flavor.name
    );
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
    accepts(Spec::MultiPaxos, &Flavor::proxies());
    accepts(Spec::ProxyLeader, &Flavor::proxies());
}

#[test]
fn tlc_accepts_the_proxy_leader_walks_on_a_grid() {
    accepts(Spec::MultiPaxos, &Flavor::proxies_on_grid());
    accepts(Spec::ProxyLeader, &Flavor::proxies_on_grid());
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
fn first_refused_cluster(
    spec: Spec,
    flavor: &Flavor,
    mutant: Mutant,
    seeds: std::ops::RangeInclusive<u64>,
    chaos: u64,
) -> Option<(u64, ClusterEvent, String)> {
    let check = spec.check();
    let flavor = flavor.clone().with(mutant);
    seeds.into_iter().find_map(|seed| {
        let run = cluster_walk_of(flavor.clone(), chaos).replay(seed).ok()?;
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
/// promise that under-reports, the vote that the lost one hid, or the win
/// of the node that lost them: the model's `Win` always counts the owner's
/// own promise, which reports the votes the code's disk forgot, so the
/// code's frontier falls below the model's.
#[test]
fn tlc_refuses_a_disk_that_forgets_its_votes() {
    require_tlc(&Spec::MultiPaxos.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::MultiPaxos,
        &Flavor::multi_paxos(),
        Mutant::ForgetVotes,
        30..=40,
        40,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(
            event,
            ClusterEvent::Promise { .. } | ClusterEvent::Accept { .. } | ClusterEvent::Win { .. }
        ),
        "seed {seed}: TLC refused {event:?}, not a step that reads the acceptor's votes"
    );
}

/// The driver drops the votes from every promise. TLC refuses the first
/// promise whose report is not the acceptor's votes.
#[test]
fn tlc_refuses_a_promise_that_hides_its_votes() {
    require_tlc(&Spec::MultiPaxos.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::MultiPaxos,
        &Flavor::multi_paxos(),
        Mutant::HideVotes,
        1..=30,
        40,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::Promise { ref votes, .. } if votes.is_empty()),
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
    let (seed, event, state) = first_refused_cluster(
        Spec::ProxyLeader,
        &Flavor::proxies(),
        Mutant::KeepProxyRounds,
        50..=60,
        200,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::Commit { .. }),
        "seed {seed}: TLC refused {event:?}, not a commit"
    );
}

/// A leader hands on its ballot with an empty tail. TLC refuses the
/// payload: it must be the leader's frontier, decided and pending slots
/// (`Relinquish`).
#[test]
fn tlc_refuses_a_handoff_that_drops_its_tail() {
    require_tlc(&Spec::LeaderHandoff.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::LeaderHandoff,
        &Flavor::handoff(),
        Mutant::RewindHandoff,
        30..=45,
        40,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::Relinquish { .. }),
        "seed {seed}: TLC refused {event:?}, not a handoff"
    );
}

/// A leader asks for GC as soon as it leads. TLC refuses the request of a
/// leader whose configuration has not learned the slots below its
/// frontier (`node/gc.rs`).
#[test]
fn tlc_refuses_an_early_gc() {
    require_tlc(&Spec::Matchmaker.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::Matchmaker,
        &Flavor::matchmakers(),
        Mutant::EarlyGc,
        1..=30,
        40,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::GcAsk { .. }),
        "seed {seed}: TLC refused {event:?}, not a GC request"
    );
}
