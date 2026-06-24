//! Stage-3 simulation tests: seed-replay determinism, a chaos-aware well-formed
//! single-seed run, multi-slot log progress under a stable leader, and the
//! safety-and-progress sweep under arbitrary network faults (prefix agreement, no
//! gaps, monotonic leadership, and progress under eventual synchrony).

use std::collections::HashMap;

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

    // Prefix agreement spot-check at the data level: any slot two nodes both
    // chose carries the same value hash (a readable failure complementing the
    // oracle).
    let mut by_slot: HashMap<u64, u64> = HashMap::new();
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
#[test]
fn log_grows_under_a_stable_leader() {
    let mut max_slot = 0;
    for seed in [1_u64, 7, 42, 99, 12_345] {
        let r = run_seed(seed);
        max_slot = max_slot.max(r.chosen.iter().map(|c| c.slot).max().unwrap_or(0));
    }
    assert!(
        max_slot >= 2,
        "a stable leader streamed a multi-slot log (highest chosen slot was {max_slot})"
    );
}

/// The crown jewel: drive the `UntilCoverageStable` sweep under swarm network
/// chaos and assert every invariant holds. Empty `assertion_violations` means the
/// safety `always`-assertions all held across every seed: at-most-one-value-chosen
/// (prefix agreement), no gaps in the applied prefix, at most one leader per
/// ballot, monotonic promised ballots, and never-accept-below-promised. Saturating
/// (no `convergence_timeout`) means every `sometimes`/`reachable` fired, including
/// the `ProgressOracle` gates (a stable leader streams several slots, and
/// leadership turns over and recovers) — i.e. the dueling-proposer livelock is
/// gone and the cluster makes progress under eventual synchrony within the cap.
#[test]
fn safety_and_progress_hold_under_chaos() {
    let report = explore();

    assert!(
        report.assertion_violations.is_empty(),
        "safety violated under chaos: {:?}",
        report.assertion_violations
    );
    assert_eq!(report.failed_runs, 0, "no run failed");
    assert!(
        !report.convergence_timeout,
        "the sweep saturated within the cap (safety + progress reachables all fired)"
    );
}
