//! Argument parsing and report printing shared by both runner binaries.

use paros_sim::SimulationReport;

/// The `n`th command-line argument, parsed; `None` when absent or malformed.
pub fn arg<T: std::str::FromStr>(n: usize) -> Option<T> {
    std::env::args().nth(n).and_then(|s| s.parse().ok())
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
