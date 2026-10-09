//! The mutation hunt (#269), cargo-mutants' test command: a fixed range of
//! main-campaign seeds (`paros_sim::chain_mutants`), the same for every
//! mutant. It exits non-zero on any assertion violation or failed run (a
//! paros-core `assert!` panicking in a process), which is how cargo-mutants
//! counts a mutant as caught.
//!
//! Run it through `cargo xtask mutants`, which sets `PAROS_MUTANT_SEEDS`;
//! by hand: `cargo test --release -p paros-sim-runner --features mutants
//! --test mutants`. Built only with the `mutants` feature, so the plain
//! nextest run never pays for it.

use paros_sim::{MUTANT_SEEDS, chain_mutants};

fn main() {
    let seeds = std::env::var("PAROS_MUTANT_SEEDS")
        .ok()
        .map_or(MUTANT_SEEDS, |s| {
            s.parse().expect("PAROS_MUTANT_SEEDS is a seed count")
        });
    println!("--- mutation hunt: seeds 1..={seeds} ---");
    let report = chain_mutants(seeds);
    println!(
        "{} seeds: {} ok, {} failed",
        report.iterations, report.successful_runs, report.failed_runs,
    );
    if report.assertion_violations.is_empty() && report.failed_runs == 0 {
        println!("no violations: the hunt came back clean");
        return;
    }
    for (seed, run) in report.seeds_used.iter().zip(&report.individual_metrics) {
        if let Err(error) = run {
            println!("failed run: seed {seed}: {error}");
        }
    }
    println!("VIOLATIONS: {:#?}", report.assertion_violations);
    println!("FAILING SEEDS: {:?}", report.seeds_failing);
    std::process::exit(1);
}
