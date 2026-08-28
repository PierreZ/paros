//! The #113 CTRL evaluation corpus: enumerated per-slot × per-node corruption
//! masks with analytically derived expected outcomes, the bare-quorum
//! lost-slot case, and the §5.1.2 snapshot-lifecycle compound. Deterministic
//! and bounded — this is enumerated evidence *beside* the coverage-guided
//! sweep, never a replacement for it.

use paros_sim::{
    corpus_canonical_masks, run_bare_quorum_case, run_corpus_mask, run_snapshot_lifecycle_case,
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
