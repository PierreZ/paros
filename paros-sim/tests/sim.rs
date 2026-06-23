//! Stage-1 simulation harness tests: seed-replay determinism and the exit
//! criteria (an empty cluster advances logical time and acknowledges proposals).

use paros_sim::{Outcome, run_seed, run_seed_json};

/// The determinism proof: the same seed produces a bit-identical timeline across
/// two independent runs.
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

/// Distinct seeds each produce a well-formed run. (Without chaos, two timelines
/// may coincide, so we don't assert inequality — only validity.)
#[test]
fn distinct_seeds_are_independent() {
    let a = run_seed(7);
    let b = run_seed(99);
    assert_eq!(a.seed, 7);
    assert_eq!(b.seed, 99);
}

/// The Stage-1 exit criterion: the empty cluster advances logical time and every
/// client proposal is acknowledged over the (calm, no-chaos) network.
#[test]
fn empty_cluster_advances_and_acknowledges() {
    let r = run_seed(42);

    assert_eq!(r.requests, 12, "every proposal is observed");
    assert_eq!(
        r.delivered + r.dropped,
        r.requests,
        "each proposal is either delivered or dropped"
    );
    assert_eq!(
        r.delivered, r.requests,
        "no chaos in Stage 1 — all delivered"
    );
    assert_eq!(r.dropped, 0, "no chaos in Stage 1 — nothing dropped");
    assert!(r.ticks > 0, "the cluster advanced its logical clock");
    assert!(!r.shots.is_empty(), "a seeded run exchanges messages");

    for shot in &r.shots {
        assert!(
            shot.arrive_ms >= shot.depart_ms,
            "a message arrived before it left"
        );
        assert_eq!(shot.outcome, Outcome::Delivered);
    }
}
