//! The #113 CTRL evaluation corpus: enumerated per-slot × per-node corruption
//! masks with analytically derived expected outcomes, the bare-quorum
//! lost-slot case, and the §5.1.2 snapshot-lifecycle compound. Deterministic
//! and bounded — this is enumerated evidence *beside* the coverage-guided
//! sweep, never a replacement for it.

use paros_sim::{
    chunk_corpus_canonical_masks, corpus_canonical_masks, run_bare_quorum_case,
    run_chunk_corpus_seed, run_chunk_mask, run_corpus_mask, run_corpus_seed,
    run_snapshot_lifecycle_case,
};

fn assert_mask_green(mask: u16) {
    let report = run_corpus_mask(mask);
    assert_eq!(report.failed_runs, 0, "corpus mask {mask:#011b} completed");
    assert!(
        report.assertion_violations.is_empty(),
        "corpus mask {mask:#011b} asserted its analytic outcome: {:?}",
        report.assertion_violations
    );
}

/// Split the canonical case list into quarters so nextest runs them in
/// parallel; together the four tests enumerate the exhaustive 2-slot × 3-node
/// sub-grid plus the full-grid corner cases.
fn canonical_quarter(quarter: usize) {
    for (i, mask) in corpus_canonical_masks().into_iter().enumerate() {
        if i % 4 == quarter {
            assert_mask_green(mask);
        }
    }
}

#[test]
fn e1_canonical_masks_quarter_0() {
    canonical_quarter(0);
}

#[test]
fn e1_canonical_masks_quarter_1() {
    canonical_quarter(1);
}

#[test]
fn e1_canonical_masks_quarter_2() {
    canonical_quarter(2);
}

#[test]
fn e1_canonical_masks_quarter_3() {
    canonical_quarter(3);
}

/// The bare-quorum lost slot: decided by two of three, then both copies (and
/// both holders' snapshots) rotted. The Phase-1 tally is `faulty, faulty,
/// none` — the cluster must WAIT at the lost slot, never no-op fill it (CTRL
/// §5.1.1 mutation (b)'s target: weakening the full-Q1 threshold to a sub-Q1
/// `none` count fabricates history and turns exactly this case red).
#[test]
fn bare_quorum_lost_slot_waits() {
    let report = run_bare_quorum_case(0);
    assert_eq!(report.failed_runs, 0, "bare-quorum case completed");
    assert!(
        report.assertion_violations.is_empty(),
        "bare-quorum case waited without fabricating: {:?}",
        report.assertion_violations
    );
}

/// The §5.1.2 snapshot-lifecycle compound: one scripted run reaching local
/// snapshot re-replay at floor 0, whole-blob `InstallSnapshot` under a
/// truncated log, the below-floor `Prepare` refusal, and the
/// truncated-past-everyone WAIT.
#[test]
fn snapshot_lifecycle_compound_reaches_all_paths() {
    let report = run_snapshot_lifecycle_case(0);
    assert_eq!(report.failed_runs, 0, "lifecycle compound completed");
    assert!(
        report.assertion_violations.is_empty(),
        "lifecycle compound asserted all four recovery paths: {:?}",
        report.assertion_violations
    );
}

/// The #101 per-chunk mask corpus: every canonical mask over the decided
/// snapshot point's chunks asserts its analytic outcome — an assemblable
/// chunk (≥ 1 clean copy anywhere) is repaired from a peer on every holder
/// (chunk repair is the only heal in these cases: the live states stay
/// healthy, so the repair-cost metric is ~chunk, never the blob), and an
/// unassemblable chunk stays faulty everywhere, never fabricated, while the
/// cluster stays fully available.
#[test]
fn chunk_canonical_masks_assert_their_analytic_outcomes() {
    for mask in chunk_corpus_canonical_masks() {
        let report = run_chunk_mask(mask, false);
        assert_eq!(report.failed_runs, 0, "chunk mask {mask:#017b} completed");
        assert!(
            report.assertion_violations.is_empty(),
            "chunk mask {mask:#017b} asserted its analytic outcome: {:?}",
            report.assertion_violations
        );
    }
}

/// One chunk mask compounded with a lost live snapshot on node 0: the node's
/// below-floor recovery races the point restore against the whole-blob
/// install, and either way converges without fabricating.
#[test]
fn chunk_mask_with_lost_live_snapshot_converges() {
    let report = run_chunk_mask(0b10, true);
    assert_eq!(report.failed_runs, 0, "chunk+live case completed");
    assert!(
        report.assertion_violations.is_empty(),
        "chunk+live case converged: {:?}",
        report.assertion_violations
    );
}

/// Hunt-found corpus witnesses (2026-08-29), pinned. All were red on rigid
/// pre-fix expectations and prove robustness fixes, not protocol bugs:
/// - E1 seed 13939994950726385685: a still-in-flight resent `Accept`
///   re-persisted a masked record 2ms after injection, legitimately clearing
///   the fault mark — the run is judged vacuous (mask superseded) instead of
///   producing a false "fabrication" verdict.
/// - chunk seeds 18124219549777579368 / 792510575699257791 (a leadership blip
///   re-seeded a second `Snap` marker, point at slot 7) and
///   3675004985962188751 (the accepted compact was only a proposal — its
///   leader died before the `Truncate`'s accepts left it): the control-tail
///   identifier and the floor re-ask absorb every legitimate coupling
///   outcome.
#[test]
fn corpus_hunt_witnesses_replay_clean() {
    let report = run_corpus_seed(13_939_994_950_726_385_685);
    assert!(
        report.assertion_violations.is_empty(),
        "E1 vacuous-run witness: {:?}",
        report.assertion_violations
    );
    for seed in [
        18_124_219_549_777_579_368_u64,
        792_510_575_699_257_791,
        3_675_004_985_962_188_751,
    ] {
        let report = run_chunk_corpus_seed(seed);
        assert!(
            report.assertion_violations.is_empty(),
            "chunk coupling witness {seed}: {:?}",
            report.assertion_violations
        );
    }
}
