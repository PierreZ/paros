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
//! (chaos steps of a cluster walk, 150), `TRACE_QUIET` (quiet steps of a
//! cluster walk, 10; 250 for a retiring or a reconfiguring walk), `TRACE_MAX_EVENTS` (the longest
//! trace TLC checks, 200), and `TRACE_DUMP` (print every event of each
//! checked cluster walk).

use consensus_sim::batcher::{self, BatchEvent, BatchModel, BatchMutant};
use consensus_sim::cluster::{Cluster, ClusterEvent, Flavor, Mutant, Spec, Steer};
use consensus_sim::decree::{self, DecreeEvent, DecreeModel, DecreeMutant};
use consensus_sim::trace::{Trace, TraceCheck, Verdict, mutate, require_tlc, spec};
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

/// A cluster walk of `chaos` chaos steps, then its quiet steps: 10, or 250
/// for a [`Steer::Retiring`] or a [`Steer::Reconfiguring`] walk. A
/// retirement and a proxy round under a new configuration each follow an
/// operator move on a calm cluster: a retirement needs a leader that
/// stands while its GC floor becomes effective and every matchmaker checks
/// the node, and a delegated round needs a leadership of the new
/// configuration that settles. In the chaos steps the leaders of a cluster
/// with matchmakers seldom stand that long.
fn cluster_walk_of(flavor: Flavor, chaos: u64) -> Walk<Cluster> {
    let quiet = if flavor.steer == Steer::Retiring || flavor.steer == Steer::Reconfiguring {
        250
    } else {
        10
    };
    Walk::<Cluster>::new()
        .chaos_steps(chaos)
        .quiet_steps(knob("TRACE_QUIET", quiet))
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

#[test]
fn tlc_accepts_the_reconfigured_compartment_walks() {
    accepts(Spec::MatchmakerGrid, &Flavor::reconfiguring());
}

#[test]
fn tlc_accepts_the_lagging_read_walks() {
    accepts(Spec::Reads, &Flavor::lagging_reads());
}

#[test]
fn tlc_accepts_the_truncation_walks() {
    accepts(Spec::Truncation, &Flavor::compaction());
}

#[test]
fn tlc_accepts_the_read_replica_walks() {
    accepts(Spec::Compartments, &Flavor::compartments_compacting());
}

#[test]
fn tlc_accepts_the_disk_fault_walks() {
    accepts(Spec::Faults, &Flavor::disk_faults());
    accepts(Spec::Faults, &Flavor::disk_faults_three());
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
        1..=40,
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
        1..=60,
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

/// The control of `MatchmakerGridTrace`: the early GC request on grids. A
/// Phase-2 quorum of a grid is one column, so the request must wait for a
/// column of `C_b` to learn the prefix; TLC refuses the request.
#[test]
fn tlc_refuses_an_early_gc_on_a_grid() {
    require_tlc(&Spec::MatchmakerGrid.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::MatchmakerGrid,
        &Flavor::reconfigured_compartments(),
        Mutant::EarlyGc,
        1..=40,
        150,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::GcAsk { .. }),
        "seed {seed}: TLC refused {event:?}, not a GC request"
    );
}

/// An acceptor answers a read with its belief ballot as its read fence,
/// not the higher of its promise and its belief. The oracle sees a stale
/// read only when a lagging acceptor promised a campaign that chose a
/// write; TLC refuses the answer whose fence is below its promise. The
/// promise passes the belief only on an acceptor whose pool lags: rare,
/// so the search runs over 70 seeds.
#[test]
fn tlc_refuses_a_read_fence_below_the_promise() {
    require_tlc(&Spec::Reads.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::Reads,
        &Flavor::lagging_reads(),
        Mutant::BeliefFence,
        1..=70,
        150,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::ReadAck { .. }),
        "seed {seed}: TLC refused {event:?}, not a read answer"
    );
}

/// An acceptor answers a read as if it never voted. The oracle sees a
/// stale read only when the hidden watermarks held a completed write above
/// the fence; TLC refuses the first answer that hides a vote.
#[test]
fn tlc_refuses_a_read_answer_that_hides_its_watermark() {
    require_tlc(&Spec::Reads.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::Reads,
        &Flavor::lagging_reads(),
        Mutant::HideWatermark,
        1..=40,
        150,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(
            event,
            ClusterEvent::ReadAck {
                watermark: None,
                ..
            }
        ),
        "seed {seed}: TLC refused {event:?}, not an answer that hid its watermark"
    );
}

/// The disk compacts each truncation to the end of the chosen prefix,
/// past the records at or above `first_seq`. The oracle sees a bug only
/// when a reader asks that disk for a lost record; TLC refuses the
/// compaction itself, on the walk steered toward compactions.
#[test]
fn tlc_refuses_a_compaction_past_the_fold() {
    require_tlc(&Spec::Truncation.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::Truncation,
        &Flavor::compaction(),
        Mutant::CompactPastFold,
        1..=20,
        150,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::Compact { .. }),
        "seed {seed}: TLC refused {event:?}, not a compaction"
    );
}

/// A read replica serves a read from its own prefix, with no quorum of
/// answers. The oracle sees a stale read only when a write completed
/// beyond that prefix; TLC refuses the serve itself.
#[test]
fn tlc_refuses_a_replica_read_with_no_quorum() {
    require_tlc(&Spec::Compartments.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::Compartments,
        &Flavor::compartments(),
        Mutant::LocalRead,
        1..=30,
        150,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::ReplicaServe { .. }),
        "seed {seed}: TLC refused {event:?}, not a served read"
    );
}

/// The driver drops the faulty records from each promise: a lost value
/// reads as no vote. The oracle sees a bug only when a candidate fills a
/// chosen slot with a `Noop` over the hidden record; TLC refuses the
/// promise that hid it.
#[test]
fn tlc_refuses_a_promise_that_hides_a_faulty_record() {
    require_tlc(&Spec::Faults.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::Faults,
        &Flavor::disk_faults_three(),
        Mutant::HideFaulty,
        1..=40,
        150,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::Page { .. }),
        "seed {seed}: TLC refused {event:?}, not a promise"
    );
}

#[test]
fn tlc_accepts_the_retirement_walks() {
    accepts(Spec::Retire, &Flavor::retiring());
}

/// The control of `RetireTrace`: an operator that retires a node with no
/// evidence. TLC refuses the retirement.
#[test]
fn tlc_refuses_a_retirement_with_no_evidence() {
    require_tlc(&Spec::Retire.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::Retire,
        &Flavor::retiring(),
        Mutant::RetireUnchecked,
        1..=40,
        150,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::Retire { .. }),
        "seed {seed}: TLC refused {event:?}, not a retirement"
    );
}

#[test]
fn tlc_accepts_the_handover_walks() {
    accepts(Spec::Handover, &Flavor::handover());
}

/// The control of `HandoverTrace`: a network that drops a record from a
/// bootstrap page. TLC refuses the bootstrap.
#[test]
fn tlc_refuses_a_bootstrap_that_lost_a_registration() {
    require_tlc(&Spec::Handover.check());
    let (seed, event, state) = first_refused_cluster(
        Spec::Handover,
        &Flavor::handover(),
        Mutant::ShrinkBootstrap,
        1..=40,
        150,
    )
    .expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}\n{state}");
    assert!(
        matches!(event, ClusterEvent::MmBoot { .. }),
        "seed {seed}: TLC refused {event:?}, not a bootstrap"
    );
}

/// The batcher walks TLC always checks: a trace of a batcher walk costs
/// about a second.
const BATCHER_SEEDS: u64 = 20;

/// A batcher walk with `mutant` planted.
fn batcher_walk(mutant: BatchMutant) -> Walk<BatchModel> {
    Walk::<BatchModel>::new()
        .chaos_steps(60)
        .env_prefix(None)
        .model(move |_| BatchModel::build(mutant))
}

/// A rule of `BatcherTrace`: its name, and whether a walk meets it.
type BatchRule = (&'static str, fn(&[BatchEvent]) -> bool);

/// The rules of `BatcherTrace`: the steps some checked batcher walk must
/// take.
const BATCHER_RULES: &[BatchRule] = &[
    ("a merged batch answers a write after another", |events| {
        events.iter().any(|e| {
            let BatchEvent::Answer { token, seq, .. } = e else {
                return false;
            };
            // The write is not the first of its batch: its position is
            // above the position of a landing of a batch that holds it.
            events.iter().any(|f| matches!(f, BatchEvent::Flush { writes, .. } if writes.len() > 1 && writes[1..].contains(token)))
                && *seq > 0
        })
    }),
    ("a write goes alone", |events| {
        events.iter().enumerate().any(|(i, e)| {
            matches!(e, BatchEvent::Flush { writes, .. } if writes.len() == 1
                && !events[..i].iter().any(|p| matches!(p, BatchEvent::Push { token } if *token == writes[0])))
        })
    }),
    ("a push flushes its lane first", |events| {
        events.windows(2).any(|w| {
            matches!((&w[0], &w[1]), (BatchEvent::Flush { writes, .. }, BatchEvent::Push { token }) if !writes.contains(token))
        })
    }),
    ("a batch lands twice", |events| {
        let mut ids = std::collections::BTreeSet::new();
        events
            .iter()
            .any(|e| matches!(e, BatchEvent::Land { id, .. } if !ids.insert(*id)))
    }),
    ("the batcher crashes", |events| {
        events.iter().any(|e| matches!(e, BatchEvent::Crash))
    }),
    ("a writer pushes a write again", |events| {
        let mut pushed = std::collections::BTreeSet::new();
        events
            .iter()
            .any(|e| matches!(e, BatchEvent::Push { token } if !pushed.insert(*token)))
    }),
];

/// TLC accepts the batcher walks, and the checked walks meet every rule
/// of `BatcherTrace`.
#[test]
fn tlc_accepts_the_batcher_walks() {
    let check = spec("compartments", "BatcherTrace");
    require_tlc(&check);
    let max_events = usize::try_from(knob("TRACE_MAX_EVENTS", 200)).expect("a size");
    let mut unmet: Vec<_> = BATCHER_RULES.iter().collect();
    let mut checked = 0;
    let first = knob("TRACE_FROM", 1);
    for seed in first..first + knob("TRACE_SEARCH", 300) {
        if unmet.is_empty() && checked >= BATCHER_SEEDS {
            break;
        }
        let run = batcher_walk(BatchMutant::None)
            .replay(seed)
            .unwrap_or_else(|failure| panic!("{failure}"));
        let events: Vec<BatchEvent> = run.events().cloned().collect();
        if events.len() > max_events {
            continue;
        }
        let before = unmet.len();
        unmet.retain(|(_, met)| !met(&events));
        if unmet.len() == before && checked >= BATCHER_SEEDS {
            continue;
        }
        checked += 1;
        eprintln!(
            "batcher seed {seed}: {} events, {} rules met",
            events.len(),
            BATCHER_RULES.len() - unmet.len()
        );
        match check
            .check(&batcher::trace(&events))
            .expect("TLC gives a verdict")
        {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "batcher seed {seed}: TLC refused event {} {:?}\n{}",
                rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => panic!("batcher seed {seed}: {other:?}"),
        }
    }
    let names: Vec<&str> = unmet.iter().map(|(name, _)| *name).collect();
    assert!(names.is_empty(), "batcher: no walk meets {names:?}");
}

/// The batcher sends a merged lane in the reverse of push order, each
/// write with its own records. The `IN_ORDER` and `SCATTER` oracles pass;
/// TLC refuses the flush.
#[test]
fn tlc_refuses_a_lane_sent_out_of_push_order() {
    let check = spec("compartments", "BatcherTrace");
    require_tlc(&check);
    let refused = (1..=40).find_map(|seed| {
        let run = batcher_walk(BatchMutant::ReverseLane).replay(seed).ok()?;
        let events: Vec<BatchEvent> = run.events().cloned().collect();
        match check
            .check(&batcher::trace(&events))
            .expect("TLC gives a verdict")
        {
            Verdict::Rejected(rejection) => Some((seed, events[rejection.index].clone())),
            _ => None,
        }
    });
    let (seed, event) = refused.expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}");
    assert!(
        matches!(event, BatchEvent::Flush { .. }),
        "seed {seed}: TLC refused {event:?}, not a flush"
    );
}

/// The recorded traces of `spec` that the edits work on: the walks of
/// `flavor` that [`accepts`] would check, the first seed and each seed
/// that meets a rule of the spec no trace before met.
fn recorded(spec: Spec, flavor: &Flavor) -> Vec<Trace> {
    let rules = spec.rules();
    let mut unmet: Vec<_> = rules.iter().collect();
    let mut traces = Vec::new();
    for seed in 1..=300 {
        if unmet.is_empty() {
            break;
        }
        let run = cluster_walk_of(flavor.clone(), 150)
            .replay(seed)
            .unwrap_or_else(|failure| panic!("{failure}"));
        let events = kept(spec, &run);
        if events.len() > 200 {
            continue;
        }
        let before = unmet.len();
        unmet.retain(|rule| !rule.met(&events));
        if unmet.len() == before && !traces.is_empty() {
            continue;
        }
        traces.push(spec.trace(flavor, &events));
    }
    traces
}

/// The decree and batcher walks the edits may work on. A walk costs
/// milliseconds; TLC checks only the walks that an edit works on.
const EDITED_SEEDS: u64 = 40;

/// The least number of edits that find a place in the recorded traces of
/// each trace spec.
const MIN_EDITS: usize = 20;

/// TLC refuses every edit of the recorded traces at or after the edited
/// event (#1308 (mutate recorded traces)). Each edit runs on the first
/// recorded trace that has a place for it ([`mutate::Edit::apply`]). An
/// edit that TLC accepts is a binding that accepts too much: fix the trace
/// spec, never the edit's place to dodge it.
fn refuses_the_edits(name: &str, check: &TraceCheck, recorded: &[Trace]) {
    require_tlc(check);
    assert!(!recorded.is_empty(), "{name}: a recorded trace");
    let mut placed = 0;
    let mut wrong = Vec::new();
    let mut accepted = vec![false; recorded.len()];
    for edit in mutate::edits() {
        let Some((index, mutant)) = recorded
            .iter()
            .enumerate()
            .find_map(|(index, trace)| Some((index, edit.apply(trace)?)))
        else {
            continue;
        };
        if !accepted[index] {
            // The events before the edited one are recorded ones: TLC
            // accepts the whole recorded trace.
            let verdict = check.check(&recorded[index]).expect("TLC gives a verdict");
            assert_eq!(verdict, Verdict::Accepted, "{name}: recorded trace {index}");
            accepted[index] = true;
        }
        placed += 1;
        let verdict = check.check(&mutant.trace).expect("TLC gives a verdict");
        // The first event that TLC does not take: the event no behaviour
        // matches, or the event after which an invariant of the trace
        // breaks (`matched` events are matched then).
        let refused = match &verdict {
            Verdict::Rejected(rejection) => Some(rejection.index),
            Verdict::InvariantViolated { matched, .. } => matched.checked_sub(1),
            Verdict::Accepted => None,
        };
        eprintln!(
            "{name}: {} at {}: refused at {refused:?}",
            edit.name, mutant.at
        );
        match refused {
            Some(index) if index >= mutant.at => {}
            Some(index) => wrong.push(format!(
                "{}: refused at {index}, before the edited event {}: {verdict:?}",
                edit.name, mutant.at
            )),
            None => wrong.push(format!(
                "{}: accepted (edited event {})",
                edit.name, mutant.at
            )),
        }
    }
    assert!(
        wrong.is_empty(),
        "{name}: TLC must refuse every edit:\n{}",
        wrong.join("\n")
    );
    assert!(
        placed >= MIN_EDITS,
        "{name}: only {placed} edits find a place in the recorded traces, at least {MIN_EDITS} must"
    );
}

/// The edits of the recorded walks of `flavor`, judged by `spec`.
fn refuses_the_cluster_edits(spec: Spec, flavor: &Flavor) {
    refuses_the_edits(&format!("{spec:?}"), &spec.check(), &recorded(spec, flavor));
}

#[test]
fn tlc_refuses_the_edited_decree_traces() {
    let recorded: Vec<Trace> = (1..=EDITED_SEEDS)
        .map(|seed| {
            let run = decree_walk(Phase1Rule::Quorum, DecreeMutant::None)
                .replay(seed)
                .unwrap_or_else(|failure| panic!("{failure}"));
            decree::trace(Phase1Rule::Quorum, run.events())
        })
        .collect();
    refuses_the_edits("DecreeTrace", &spec("decree", "DecreeTrace"), &recorded);
}

#[test]
fn tlc_refuses_the_edited_batcher_traces() {
    let recorded: Vec<Trace> = (1..=EDITED_SEEDS)
        .filter_map(|seed| {
            let run = batcher_walk(BatchMutant::None).replay(seed).ok()?;
            let events: Vec<BatchEvent> = run.events().cloned().collect();
            (events.len() <= 200).then(|| batcher::trace(&events))
        })
        .collect();
    refuses_the_edits(
        "BatcherTrace",
        &spec("compartments", "BatcherTrace"),
        &recorded,
    );
}

#[test]
fn tlc_refuses_the_edited_multi_paxos_traces() {
    refuses_the_cluster_edits(Spec::MultiPaxos, &Flavor::multi_paxos_five());
}

#[test]
fn tlc_refuses_the_edited_proxy_leader_traces() {
    refuses_the_cluster_edits(Spec::ProxyLeader, &Flavor::proxies());
}

#[test]
fn tlc_refuses_the_edited_leader_handoff_traces() {
    refuses_the_cluster_edits(Spec::LeaderHandoff, &Flavor::handoff());
}

#[test]
fn tlc_refuses_the_edited_matchmaker_traces() {
    refuses_the_cluster_edits(Spec::Matchmaker, &Flavor::matchmakers());
}

#[test]
fn tlc_refuses_the_edited_matchmaker_grid_traces() {
    refuses_the_cluster_edits(Spec::MatchmakerGrid, &Flavor::reconfiguring());
}

#[test]
fn tlc_refuses_the_edited_read_traces() {
    refuses_the_cluster_edits(Spec::Reads, &Flavor::lagging_reads());
}

#[test]
fn tlc_refuses_the_edited_truncation_traces() {
    refuses_the_cluster_edits(Spec::Truncation, &Flavor::compaction());
}

#[test]
fn tlc_refuses_the_edited_compartment_traces() {
    refuses_the_cluster_edits(Spec::Compartments, &Flavor::compartments_compacting());
}

#[test]
fn tlc_refuses_the_edited_fault_traces() {
    refuses_the_cluster_edits(Spec::Faults, &Flavor::disk_faults());
}

#[test]
fn tlc_refuses_the_edited_retirement_traces() {
    refuses_the_cluster_edits(Spec::Retire, &Flavor::retiring());
}

#[test]
fn tlc_refuses_the_edited_handover_traces() {
    refuses_the_cluster_edits(Spec::Handover, &Flavor::handover());
}

/// The control of `TraceLearnRecordsHeld`: a trace of `spec` with the
/// record of a learn dropped from its batch. TLC reports the invariant at
/// the learn or after it.
fn learn_records_held(spec: Spec, flavor: &Flavor) {
    let check = spec.check();
    require_tlc(&check);
    let dropped = recorded(spec, flavor).iter().find_map(|trace| {
        let events = trace.events();
        let field = |e: &moonpool_tla::Value, f: &str| match e {
            moonpool_tla::Value::Record(r) => r.get(f).cloned(),
            _ => None,
        };
        let is = |e: &moonpool_tla::Value, name: &str| field(e, "event") == Some(name.into());
        // A record whose learn is earlier in its batch: every event between
        // them is of the same node.
        let (learn, k) = (1..events.len()).find_map(|k| {
            let r = &events[k];
            if !is(r, "record") {
                return None;
            }
            (0..k)
                .rev()
                .take_while(|&i| !is(&events[i], "restart"))
                .find(|&i| {
                    is(&events[i], "learn")
                        && field(&events[i], "node") == field(r, "node")
                        && field(&events[i], "slot") == field(r, "slot")
                        && events[i + 1..k].iter().all(|b| {
                            ["node", "acc", "by", "owner"]
                                .iter()
                                .find_map(|f| {
                                    field(b, f).filter(|v| matches!(v, moonpool_tla::Value::Str(_)))
                                })
                                .is_none_or(|n| Some(n) == field(r, "node"))
                        })
                })
                .map(|i| (i, k))
        })?;
        let mut edited = trace.prefix(0);
        for (i, event) in events.iter().enumerate() {
            if i == k {
                continue;
            }
            let moonpool_tla::Value::Record(mut fields) = event.clone() else {
                unreachable!("an event is a record")
            };
            let Some(moonpool_tla::Value::Str(name)) = fields.remove("event") else {
                unreachable!("an event has a name")
            };
            edited.event(&name, fields);
        }
        consensus_sim::trace::whole(&mut edited);
        Some((learn, edited))
    });
    let (learn, edited) = dropped.expect("a recorded trace has the record of a learn");
    match check.check(&edited).expect("TLC gives a verdict") {
        Verdict::InvariantViolated {
            invariant, matched, ..
        } => {
            assert_eq!(invariant, "TraceLearnRecordsHeld");
            // The invariant reads ahead to the end of the batch: it breaks
            // at the learn whose record is gone.
            assert!(
                matched > learn,
                "{spec:?}: the learn at {learn} whose record is gone breaks it, not {matched}"
            );
        }
        other => panic!("{spec:?}: TLC does not see a learn with no record: {other:?}"),
    }
}

#[test]
fn tlc_sees_a_learn_with_no_record_on_multi_paxos() {
    learn_records_held(Spec::MultiPaxos, &Flavor::multi_paxos_five());
}

#[test]
fn tlc_sees_a_learn_with_no_record_on_the_handoff() {
    learn_records_held(Spec::LeaderHandoff, &Flavor::handoff());
}
