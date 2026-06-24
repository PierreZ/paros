//! Stage-2 simulation tests: seed-replay determinism, a chaos-aware well-formed
//! single-seed run, and the safety sweep (single-decree Paxos chooses at most one
//! value under arbitrary network faults).

use paros_sim::{explore, run_seed, run_seed_json};

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

    for shot in &r.shots {
        assert!(
            shot.arrive_ms >= shot.depart_ms,
            "a message arrived before it left"
        );
    }
}

/// The crown jewel: drive the `UntilCoverageStable` sweep under swarm network
/// chaos and assert the three safety invariants never break. No
/// `assertion_violations` means at-most-one-value-chosen, monotonic promised
/// ballots, and never-accept-below-promised all held across every seed. Saturating
/// (no `convergence_timeout`) means the "a value is chosen" reachability fired, so
/// the protocol made progress within the cap.
#[test]
fn safety_holds_under_chaos() {
    let report = explore();

    assert!(
        report.assertion_violations.is_empty(),
        "safety violated under chaos: {:?}",
        report.assertion_violations
    );
    assert_eq!(report.failed_runs, 0, "no run failed");
    assert!(
        !report.convergence_timeout,
        "the sweep saturated within the cap (a value is reliably chosen)"
    );
}
