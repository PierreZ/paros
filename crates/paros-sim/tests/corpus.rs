//! The #113 CTRL evaluation corpus: enumerated per-slot × per-node corruption
//! masks with analytically derived expected outcomes, the bare-quorum
//! lost-slot case, and the §5.1.2 snapshot-lifecycle compound. Deterministic
//! and bounded — this is enumerated evidence *beside* the coverage-guided
//! sweep, never a replacement for it.

use paros_sim::{
    chunk_corpus_canonical_masks, corpus_canonical_masks, departed_straggler_case,
    run_bare_quorum_case, run_chunk_mask, run_corpus_mask, run_snapshot_lifecycle_case,
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

/// The departed straggler (#124): the only clean copy of a decided slot lives
/// on the acceptor the last reconfiguration removed, and it is down. CTRL
/// Case 3 across a configuration boundary — the members in force must WAIT
/// (the leader resigning under `REPAIR_TIMEOUT_ELECTIONS`) and recover the
/// slot through the prior configuration once the straggler returns.
/// The case races GC and a late write, and loses often enough that a single
/// seed can pass having observed nothing at all: every seed must be green,
/// and at least one of them must have reached the injection. A scripted
/// corpus seed **is** its input (AGENTS.md, *Pinned seeds are not a
/// regression mechanism*), so enumerating a few is a corpus, not a pin.
#[test]
fn departed_straggler_waits_then_recovers() {
    let mut non_vacuous = 0;
    for seed in 0..4 {
        let (report, reached) = departed_straggler_case(seed);
        assert_eq!(
            report.failed_runs, 0,
            "departed-straggler case completed (seed {seed})"
        );
        assert!(
            report.assertion_violations.is_empty(),
            "departed-straggler case waited, then recovered (seed {seed}): {:?}",
            report.assertion_violations
        );
        non_vacuous += usize::from(reached);
    }
    assert!(
        non_vacuous > 0,
        "at least one departed-straggler seed reached its injection"
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
