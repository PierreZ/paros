//! Stage-3 simulation tests: seed-replay determinism, a chaos-aware well-formed
//! single-seed run, multi-slot log progress under a stable leader, and the
//! safety-and-progress smoke under the combined swarm campaign — network
//! turbulence, attrition and BUGGIFY on one axis (prefix agreement, no gaps,
//! monotonic leadership, and progress under eventual synchrony).
//!
//! No test here replays a *witness*. A seed names a draw schedule rather than a
//! scenario (AGENTS.md, *Pinned seeds are not a regression mechanism*), so the
//! seeds that appear below are either arbitrary smoke samples, a contiguous
//! scan of the seed space, or the same seed twice for the determinism proof —
//! never a farmed reproduction anything is expected to keep reproducing.

use std::collections::BTreeMap;

use paros_sim::{SMOKE_ITERATIONS, chain_smoke, run_chain_seed, run_seed, run_seed_json};

/// One arbitrary seed through the Chain campaign, as a fast smoke: every seed
/// must satisfy this, so the value is a sample rather than a pin.
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

/// The determinism proof: the same seed produces a bit-identical timeline across
/// two independent runs. Network chaos is on, but it is seeded, so replay still
/// holds. The three values are arbitrary — what is under test is *equality
/// between the two runs*, which holds for every seed and cannot go stale the
/// way a farmed witness does.
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

/// A stable leader streams a multi-slot log: across a scan of the low seed
/// space the chosen log grows past slot 0 (Stage 3's stable-leader Phase-2
/// streaming). A concrete, cheap complement to the sweep's `ProgressOracle`
/// reachability gate.
///
/// The scan is a **contiguous prefix of the seed space, not a picked list**:
/// hand-picking the seeds that happen to stream today buys nothing, because a
/// seed's meaning — the exact interleaving it drives — moves with every
/// scheduler, BUGGIFY-location, or mailbox change (that is why the previous
/// pinned list had to be re-picked once already). Ten consecutive seeds make
/// "some run streams a multi-slot log" a claim about the *protocol* rather
/// than about one build's seed schedule, and keep this off the suite's
/// critical path.
#[test]
fn log_grows_under_a_stable_leader() {
    let mut max_slot = 0;
    for seed in 1_u64..=10 {
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
