//! The nextest smoke: seed-replay determinism, one seed through the main
//! campaign, the storage contract suite, and a handful of random seeds through
//! the safety checks under the combined swarm campaign.
//!
//! No test here replays a *witness*. A seed names a draw schedule rather than a
//! scenario (AGENTS.md, *Pinned seeds are not a regression mechanism*), so the
//! seeds below are either arbitrary smoke samples or the same seed twice for the
//! determinism proof — never a farmed reproduction anything is expected to keep
//! reproducing.

use paros_sim::{SMOKE_ITERATIONS, chain_seed_digest, chain_smoke, run_chain_seed};

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

/// The world-backed sim storage passes the same behavioral contract suite
/// `MemStorage` passes as a `paros` unit test, so the fake can never drift from
/// the `NodeStorage` trait contract.
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

/// The determinism proof: the same seed produces a bit-identical audit digest
/// across two independent runs. Every chaos surface is on, but all of it is
/// seeded, so replay still holds. The values are arbitrary — what is under
/// test is *equality between the two runs*, which holds for every seed and
/// cannot go stale the way a farmed witness does.
#[test]
fn same_seed_replays_identically() {
    for seed in [1_u64, 42, 12_345] {
        assert_eq!(
            chain_seed_digest(seed),
            chain_seed_digest(seed),
            "seed {seed} must replay bit-identically"
        );
    }
}

/// Fast safety smoke: drive a handful ([`SMOKE_ITERATIONS`]) of random seeds
/// under the combined swarm campaign and assert every `always` held on each:
/// at-most-one-value-chosen, no gaps in the applied prefix, monotonic
/// leadership and promises, never-accept-below-promised, and the client's own
/// linearizability history. A *smoke*, not the coverage sweep: it does **not**
/// assert saturation. The heavy, coverage-guided sweep lives in `cargo xtask
/// sim` (sancov-instrumented) so code coverage guides seed selection.
#[test]
fn safety_holds_under_chaos_smoke() {
    let report = chain_smoke(SMOKE_ITERATIONS);
    report.eprint();

    assert!(
        report.assertion_violations.is_empty(),
        "safety violated under chaos: {:?}; failing seeds (replay with sim-paros-hunt replay-main): {:?}",
        report.assertion_violations,
        report.seeds_failing.iter().take(10).collect::<Vec<_>>(),
    );
    assert_eq!(report.failed_runs, 0, "no run failed");
}
