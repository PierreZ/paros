//! Stage-3 simulation tests: seed-replay determinism, a chaos-aware well-formed
//! single-seed run, multi-slot log progress under a stable leader, and the
//! safety-and-progress smoke under the combined swarm campaign — network
//! turbulence, attrition and BUGGIFY on one axis (prefix agreement, no gaps,
//! monotonic leadership, and progress under eventual synchrony).

use std::collections::BTreeMap;

use paros_sim::{
    CHAIN_REGRESSION_SEEDS, REGRESSION_SEEDS, SMOKE_ITERATIONS, chain_smoke, run_chain_seed,
    run_seed, run_seed_json,
};

#[test]
#[cfg(feature = "native")]
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

/// The amnesia **red demo** stays red (issue #19 item D): a node wiped of its
/// disk rejoins naively, and the cross-restart promise audit — the only oracle
/// the wipe cannot evade, since `set_promise`'s in-core assert lives in the
/// storage record the wipe deletes — must catch the reneged promise. This test
/// asserts the *violation fires*: if the demo ever comes back green, either
/// the wipe stopped happening or the oracle went blind, and both are bugs.
/// (CTRL's `MarkNonVoting` takedown is the citation for why a wiped node must
/// never rejoin as itself; `prob_wipe` stays 0 on every real campaign.)
#[test]
fn amnesia_demo_stays_red() {
    let report = paros_sim::run_amnesia_demo_seed(paros_sim::AMNESIA_DEMO_SEED);
    assert!(
        report
            .assertion_violations
            .iter()
            .any(|v| format!("{v:?}").contains("promised ballot never decreases")),
        "the naive-wipe demo must surface the reneged promise; got: {:?}",
        report.assertion_violations
    );
}

/// The truncate-on-mismatch **red demo** stays red (issue #20 item F, CTRL
/// Figure 2): a node that truncates its log on a detected mismatch instead of
/// crashing silently drops possibly-chosen records, and the audit's
/// recovered-vs-persisted divergence leg — the only oracle a *silent* local
/// truncation cannot evade — must catch the unexplained hole. This test
/// asserts the *violation fires*: if the demo ever comes back green, either
/// the injection stopped landing or the divergence leg went blind, and both
/// are bugs. (Stage 7's baseline is detect ⇒ crash; a crash-truncatable tail
/// is the only thing a scan may ever discard.)
#[test]
fn truncate_on_mismatch_demo_stays_red() {
    let report = paros_sim::run_truncate_demo_seed(paros_sim::TRUNCATE_DEMO_SEED);
    assert!(
        report
            .assertion_violations
            .iter()
            .any(|v| format!("{v:?}").contains("omits a persisted record")),
        "the truncate-on-mismatch demo must surface the silent record loss; got: {:?}",
        report.assertion_violations
    );
}

/// Item F (issue #21): the world-backed sim storage passes the same behavioral
/// contract suite `MemStorage` passes as a `paros` unit test, so the fake can
/// never drift from the `NodeStorage` trait contract.
#[test]
fn sim_storage_passes_the_contract_suite() {
    let report = paros_sim::run_storage_contract_suite();
    assert_eq!(report.failed_runs, 0, "the contract suite completed");
    assert!(
        report.assertion_violations.is_empty(),
        "the sim storage honors the NodeStorage contract: {:?}",
        report.assertion_violations
    );
}

/// A budget-off smoke (issue #21, the WAITED leg): a handful of seeds with the
/// per-record corruption budget lifted. Safety must hold on every one — losing
/// every copy of a committed item may stall the cluster (that is the correct
/// wait), but must never lose or fabricate data.
#[test]
fn budget_off_smoke_stays_safe() {
    let report = paros_sim::budget_off_hunt(10);
    assert!(
        report.assertion_violations.is_empty(),
        "budget-off safety violated: {:?}; failing seeds: {:?}",
        report.assertion_violations,
        report.seeds_failing.iter().take(10).collect::<Vec<_>>(),
    );
}

/// The faulty-as-none **red demo** stays red (issue #21, CTRL §5.1.1's first
/// known-fatal mutation): a boot scan that withholds a rotted record from the
/// Promise tri-state lets the acceptor answer "nothing accepted here", and a
/// promise quorum excluding the surviving clean copy then treats the slot as
/// free — two values chosen for one slot. This test asserts the *violation
/// fires*: if the demo ever comes back green, either the misreport stopped
/// landing or the agreement oracles went blind, and both are bugs. (The real
/// scan reports `faulty(slot, ballot)` instead — the rule this demo proves
/// load-bearing.)
#[test]
fn faulty_as_none_demo_stays_red() {
    let report = paros_sim::run_faulty_none_demo_seed(paros_sim::FAULTY_NONE_DEMO_SEED);
    assert!(
        report
            .assertion_violations
            .iter()
            .any(|v| format!("{v:?}").contains("at most one value is ever chosen")),
        "the faulty-as-none demo must surface the double-choose; got: {:?}",
        report.assertion_violations
    );
}

/// The faulty-as-none mutation's *other* fate, pinned as the boot-side pair of
/// [`faulty_as_none_demo_stays_red`]: when the withheld record's hole lands
/// *below* the chosen prefix, the boot read-back's completeness assert ("every
/// retained slot below the chosen prefix has a durable record") refuses the
/// node before the misreport can reach a Promise — crash beats corruption. The
/// pinned seed deterministically reaches that refusal twice on the current
/// stream (visible as the node.rs completeness panic on stderr; moonpool
/// tolerates the death like attrition) and the survivors run clean. (The
/// original pin, `12343285557404141340`, was the demo's pre-assert
/// double-choose witness; the randomness-expansion stream shift moved its rot
/// off the refusing shape, so this seed — re-scanned from the shifted stream —
/// carries the pin now.) If this seed ever goes red, the boot completeness
/// assert stopped refusing the below-prefix misreport — the double-choose it
/// guards against is back.
#[test]
fn faulty_as_none_below_prefix_dies_at_boot() {
    let report = paros_sim::run_faulty_none_demo_seed(3);
    assert!(
        report.assertion_violations.is_empty(),
        "a boot-refused misreport must never reach the agreement oracles; got: {:?}",
        report.assertion_violations
    );
}

/// The witnesses farmed on the former network-swarm axis, replayed on the
/// campaign that absorbed it. That axis existed only because Moonpool's network
/// faults outlived `chaos_duration`; `43304d8` fixed the lifecycle, so swarm
/// network turbulence is now part of the main combined campaign and these seeds
/// replay through the same builder as every other Chain seed.
#[test]
fn network_regression_seeds_replay_clean() {
    for &seed in paros_sim::NETWORK_REGRESSION_SEEDS {
        let report = run_chain_seed(seed);
        assert_eq!(report.failed_runs, 0, "network seed {seed}");
        assert!(
            report.assertion_violations.is_empty(),
            "network seed {seed}: {:?}",
            report.assertion_violations
        );
    }
}

#[test]
fn snapshot_recovery_choreography_replays_clean() {
    let seed = 42;
    let report = paros_sim::run_snapshot_recovery_seed(seed);
    assert_eq!(report.failed_runs, 0, "snapshot choreography seed {seed}");
    assert!(
        report.assertion_violations.is_empty(),
        "snapshot choreography seed {seed}: {:?}",
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

    assert!(
        r.requests >= 4,
        "every client's proposals are observed (client count and request count are per-seed draws), got {}",
        r.requests
    );
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
            (shot.from as usize) < r.nodes && (shot.to as usize) < r.nodes,
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
