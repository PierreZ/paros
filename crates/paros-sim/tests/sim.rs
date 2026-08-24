//! Stage-3 simulation tests: seed-replay determinism, a chaos-aware well-formed
//! single-seed run, multi-slot log progress under a stable leader, and the
//! safety-and-progress sweep under arbitrary network faults (prefix agreement, no
//! gaps, monotonic leadership, and progress under eventual synchrony).

use std::collections::BTreeMap;

use paros_sim::{
    CHAIN_REGRESSION_SEEDS, REGRESSION_SEEDS, SMOKE_ITERATIONS, chain_smoke, run_chain_seed,
    run_seed, run_seed_json,
};

#[test]
fn protobuf_exploration_recipe_recovers() {
    let report = paros_sim::replay_chain(
        6_871_908_205_527_803_561,
        vec![
            (554, 1_355_743_804_900_694_044),
            (3_045, 4_290_232_578_606_980_615),
        ],
    );
    assert_eq!(report.failed_runs, 0);
    assert!(
        report.assertion_violations.is_empty(),
        "protobuf recovery recipe: {:?}",
        report.assertion_violations
    );
}

#[test]
fn chain_single_seed_converges() {
    let report = run_chain_seed(42);
    report.eprint();
    assert_eq!(report.failed_runs, 0, "chain workload completes");
    assert!(
        report.assertion_violations.is_empty(),
        "chain safety holds: {:?}",
        report.assertion_violations
    );
}

#[test]
fn chain_regression_seeds_replay_clean() {
    for seed in CHAIN_REGRESSION_SEEDS {
        let report = run_chain_seed(*seed);
        assert_eq!(report.failed_runs, 0, "Chain regression seed {seed}");
        assert!(
            report.assertion_violations.is_empty(),
            "Chain regression seed {seed}: {:?}",
            report.assertion_violations
        );
    }
}

/// The pinned-seed regression corpus: replay every recorded durability seed and
/// assert it stays clean. `run_seed` panics on any `always`-assertion violation,
/// so a regression in the storage/recovery/seam-crash path fails this test
/// directly, without waiting for the adaptive sweep to rediscover it.
#[test]
fn pinned_regression_seeds_replay_clean() {
    for &seed in REGRESSION_SEEDS {
        let r = run_seed(seed);
        assert_eq!(r.seed, seed, "replayed the pinned seed");
    }
}

/// The determinism proof: the same seed produces a bit-identical timeline across
/// two independent runs. Network chaos is on, but it is seeded, so replay still
/// holds.
#[test]
fn same_seed_replays_identically() {
    for seed in [1_u64, 42, 12_345] {
        assert_eq!(
            run_seed_json(seed),
            run_seed_json(seed),
            "seed {seed} must replay bit-identically"
        );
    }
}

/// Distinct seeds each produce a well-formed run. (Two timelines may still
/// coincide, so we don't assert inequality, only validity.)
#[test]
fn distinct_seeds_are_independent() {
    let a = run_seed(7);
    let b = run_seed(99);
    assert_eq!(a.seed, 7);
    assert_eq!(b.seed, 99);
}

/// A single seeded run is well-formed even under network chaos: every proposal is
/// accounted for (delivered or dropped), the cluster advanced its clock, and no
/// message arrives before it departs. We do *not* assert all-delivered: chaos may
/// drop legs, and dueling proposers may livelock (observable, unfixed in Stage 2).
#[test]
fn chaotic_run_is_well_formed() {
    let r = run_seed(42);

    assert_eq!(r.requests, 12, "every proposal is observed");
    assert_eq!(
        r.delivered + r.dropped,
        r.requests,
        "each proposal is either delivered or dropped"
    );
    assert!(r.ticks > 0, "the cluster advanced its logical clock");

    // Prefix agreement spot-check at the data level: any slot two nodes both
    // chose carries the same value hash (a readable failure complementing the
    // oracle).
    let mut by_slot: BTreeMap<u64, u64> = BTreeMap::new();
    for c in &r.chosen {
        if let Some(prev) = by_slot.insert(c.slot, c.vhash) {
            assert_eq!(
                prev, c.vhash,
                "nodes disagree on the value chosen at slot {}",
                c.slot
            );
        }
    }

    for shot in &r.shots {
        assert!(
            shot.arrive_ms >= shot.depart_ms,
            "a message arrived before it left"
        );
    }

    // The protocol timeline (the wasm demo's star) is populated and well-formed:
    // proposals drive a real inter-node Paxos exchange, every leg's arrival is
    // after its departure, and senders/receivers are cluster node ids.
    assert!(
        !r.protocol.is_empty(),
        "the inter-node Paxos exchange was recorded"
    );
    for shot in &r.protocol {
        assert!(
            shot.arrive_ms >= shot.depart_ms,
            "a protocol message arrived before it left"
        );
        assert!(
            (shot.from as usize) < 3 && (shot.to as usize) < 3,
            "protocol legs are between cluster nodes"
        );
    }
    assert!(
        !r.node_states.is_empty(),
        "per-node durable state was recorded"
    );
}

/// A stable leader streams a multi-slot log: across a handful of seeds the chosen
/// log grows past slot 0 (Stage 3's stable-leader Phase-2 streaming). A concrete,
/// cheap complement to the sweep's `ProgressOracle` reachability gate.
///
/// The seed list was re-picked after the moonpool deterministic-executor bump
/// (rev `f7a6d52`, #65): that change replaced tokio's FIFO task scheduling with
/// seeded-random scheduling, so a seed's *meaning* (the exact task interleaving
/// it drives) shifted, and the previous list (`[1, 7, 42, 99, 12_345]`) no longer
/// grew any seed's log past slot 0. These five (the first hits scanning 1..=100)
/// are historical to *this* pin: each individually clears slot 2 today, but a
/// future scheduler change can shift seed meaning again.
#[test]
fn log_grows_under_a_stable_leader() {
    let mut max_slot = 0;
    for seed in [2_u64, 3, 6, 9, 10] {
        let r = run_seed(seed);
        max_slot = max_slot.max(r.chosen.iter().map(|c| c.slot).max().unwrap_or(0));
    }
    assert!(
        max_slot >= 2,
        "a stable leader streamed a multi-slot log (highest chosen slot was {max_slot})"
    );
}

/// Fast safety smoke: drive a handful ([`SMOKE_ITERATIONS`]) of random seeds under
/// swarm network chaos and assert the safety `always`-assertions all held on every
/// one: at-most-one-value-chosen (prefix agreement), no gaps in the applied prefix,
/// per-node monotonic leadership, monotonic promised ballots, and
/// never-accept-below-promised. This is a *smoke* test, not the coverage sweep:
/// it does **not** assert saturation (`convergence_timeout`). The heavy,
/// coverage-guided sweep that must saturate `AssertionCoverage`/`CodeCoverage`
/// lives in `cargo xtask sim` (sancov-instrumented) so code coverage guides seed
/// selection; keeping it out of nextest keeps the test suite quick.
#[test]
fn safety_holds_under_chaos_smoke() {
    let report = chain_smoke(SMOKE_ITERATIONS);
    report.eprint();

    assert!(
        report.assertion_violations.is_empty(),
        "safety violated under chaos: {:?}; failing seeds (replay with run_seed): {:?}",
        report.assertion_violations,
        report.seeds_failing.iter().take(10).collect::<Vec<_>>(),
    );
    assert_eq!(report.failed_runs, 0, "no run failed");
}
