//! The journal stores on a **real filesystem** (#206): the two contract
//! suites and a process-crash loop over `TokioStorageProvider`, the provider
//! `parosd` runs on.
//!
//! The simulated disk (`tests`) is where the stores meet every crash physics
//! — unsynced sectors resolved independently, rot, shorn writes. A real disk
//! cannot be told to tear a sector, so what this module proves is the other
//! half of "the same code runs in production": the stores' file layout,
//! directory handling, positioned I/O and syncs work against the kernel's
//! filesystem, and a **process crash** — the writer killed at an arbitrary
//! await point, its in-flight I/O finished or never started, the page cache
//! intact — loses nothing a sync acknowledged. That is the paper's fault
//! model judged by the same `judge`, minus the torn sectors only the
//! simulator can produce.

use std::path::Path;
use std::time::Duration;

use moonpool_core::TokioStorageProvider;

use super::tests::{
    Model, Rng, Shared, Tally, judge, node_suite, open_node, registry_suite, seeds, small, write,
};

/// A runtime per incarnation: dropping it waits for the blocking pool, so a
/// killed writer's in-flight file operations land (or never start) before
/// the next incarnation opens the store — exactly what a process exit does.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime")
}

/// `dir` as the provider's path prefix, with a trailing separator.
fn root(dir: &Path) -> String {
    format!("{}/", dir.to_str().expect("a UTF-8 temporary directory"))
}

#[test]
fn journal_storage_passes_the_contract_suite_on_a_real_filesystem() {
    for checkpoint_after in [1, 4, 1_000] {
        let dir = tempfile::tempdir().expect("a temporary directory");
        runtime().block_on(node_suite(
            TokioStorageProvider::new(),
            root(dir.path()),
            checkpoint_after,
        ));
    }
}

#[test]
fn journal_matchmaker_storage_passes_the_contract_suite_on_a_real_filesystem() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    runtime().block_on(registry_suite(
        TokioStorageProvider::new(),
        root(dir.path()),
    ));
}

/// Kill a writer repeatedly at an arbitrary await point and judge every
/// reboot against the ledger under the paper's model: nothing a sync
/// acknowledged is lost, nothing served was never written, and every boot
/// succeeds. `PAROS_JOURNAL_CRASH_SEEDS` / `PAROS_JOURNAL_CRASH_SEED` widen
/// or pin the loop as on the simulated disk; the kill point is real time,
/// so a seed names the plan, not the interleaving.
#[test]
fn a_process_crash_on_a_real_filesystem_loses_nothing_acknowledged() {
    let mut tally = Tally::default();
    for seed in seeds(12) {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let wal = format!("{}wal", root(dir.path()));
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let ledger = Shared::default();
        for round in 0..8 {
            let at = format!("real filesystem seed {seed} round {round}");
            let judged = runtime().block_on(async {
                let node = open_node(TokioStorageProvider::new(), &wal, small(24)).await;
                judge(node, &ledger, Model::Paper, &at)
            });
            tally.boots += 1;
            let (_, past_genesis) = judged.expect("the paper's model always boots");
            tally.past_genesis += usize::from(past_genesis);
            // The first incarnation of a seed writes past a checkpoint and
            // runs to completion, so a fold from one is reached however slow
            // the disk; every later one is killed at a real-time budget.
            let (ops, budget) = if round == 0 {
                (60, None)
            } else {
                (
                    1 + rng.below(30),
                    Some(Duration::from_millis(rng.below(60))),
                )
            };
            let plan: Vec<u64> = (0..ops).map(|_| rng.next()).collect();
            let incarnation = runtime();
            incarnation.block_on(async {
                let mut handle = tokio::spawn(write(
                    TokioStorageProvider::new(),
                    wal.clone(),
                    plan,
                    ledger.clone(),
                ));
                let Some(budget) = budget else {
                    let _ = handle.await;
                    return;
                };
                // The kill lands at whatever await the writer reached.
                if tokio::time::timeout(budget, &mut handle).await.is_err() {
                    handle.abort();
                    let _ = handle.await;
                }
            });
            // The process exits: its blocking I/O drains with the runtime.
            drop(incarnation);
        }
    }
    eprintln!("real filesystem: {tally:?}");
    assert!(
        tally.past_genesis > 0,
        "no boot ever folded from a checkpoint"
    );
}
