//! Argument parsing and report printing shared by both runner binaries.

use paros_sim::SimulationReport;

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
