//! Argument parsing and report printing shared by both runner binaries.

use std::collections::BTreeMap;

use paros_sim::excuses::{Excuse, RUNS_SLOT};
use paros_sim::{AssertionStats, SimulationReport};

/// The `n`th command-line argument, parsed; `None` when absent or malformed.
pub fn arg<T: std::str::FromStr>(n: usize) -> Option<T> {
    std::env::args().nth(n).and_then(|s| s.parse().ok())
}

/// No assertion violation and no failed run: the report's safety verdict.
pub fn is_clean(report: &SimulationReport) -> bool {
    report.assertion_violations.is_empty() && report.failed_runs == 0
}

/// `N seeds: N ok, N failed`, followed by `suffix`.
pub fn print_seed_counts(report: &SimulationReport, suffix: &str) {
    println!(
        "{} seeds: {} ok, {} failed{suffix}",
        report.iterations, report.successful_runs, report.failed_runs,
    );
}

/// Name every failed run with its seed and error: a process that panicked
/// is a failed run with no assertion violation, so the violation list
/// alone never shows it.
pub fn print_failed_runs(report: &SimulationReport) {
    for (seed, run) in report.seeds_used.iter().zip(&report.individual_metrics) {
        if let Err(error) = run {
            println!("failed run: seed {seed}: {error}");
        }
    }
}

/// Name the `sometimes`/`reachable` gates that never fired, if any, each line
/// prefixed by `indent`.
pub fn print_never_fired(report: &SimulationReport, indent: &str) {
    if report.coverage_violations.is_empty() {
        return;
    }
    println!("{indent}coverage gates that never fired:");
    for gate in &report.coverage_violations {
        println!("{indent}  - {gate}");
    }
}

/// The excuse table (#738 (count excuses, the baseline)): for each oracle
/// excuse and silent skip, the runs where it fired and the journals it
/// excused, read from `slots` (`paros_sim::excuses`). A run is a seed or an
/// explored timeline. A skip judges no journal: its journal column is `-`.
pub fn print_excuses(slots: &BTreeMap<String, AssertionStats>) {
    let count = |slot: &str| slots.get(slot).map_or(0, |stats| stats.successes);
    let runs = count(RUNS_SLOT);
    println!("excuses and skips over {runs} judged runs:");
    println!(
        "  {:<34} {:>8} {:>7} {:>9}",
        "excuse", "runs", "share", "journals"
    );
    for excuse in Excuse::ALL {
        let fired = count(excuse.runs_slot());
        let share = if runs == 0 {
            0.0
        } else {
            #[expect(clippy::cast_precision_loss, reason = "a share for a report")]
            let share = fired as f64 * 100.0 / runs as f64;
            share
        };
        let journals = excuse
            .journals_slot()
            .map_or_else(|| "-".to_string(), |slot| count(slot).to_string());
        println!(
            "  {:<34} {fired:>8} {share:>6.1}% {journals:>9}",
            excuse.name()
        );
    }
}
