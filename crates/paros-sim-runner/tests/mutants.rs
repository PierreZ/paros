//! The mutation hunt (#269), cargo-mutants' test command: a fixed range of
//! main-campaign seeds (`paros_sim::chain_mutants`), the same for every
//! mutant, run in batches of `MUTANT_BATCH`. It exits non-zero at the first
//! batch with an assertion violation or a failed run (a paros-core `assert!`
//! panicking in a process), which is how cargo-mutants counts a mutant as
//! caught; the batches after it do not run.
//!
//! Run it through `cargo xtask mutants`, which sets `PAROS_MUTANT_SEEDS`;
//! by hand: `cargo test --release -p paros-sim-runner --features mutants
//! --test mutants`. Built only with the `mutants` feature, so the plain
//! nextest run never pays for it.

use paros_sim::{MUTANT_BATCH, MUTANT_SEEDS, chain_mutants};

fn main() {
    let seeds = std::env::var("PAROS_MUTANT_SEEDS")
        .ok()
        .map_or(MUTANT_SEEDS, |s| {
            s.parse().expect("PAROS_MUTANT_SEEDS is a seed count")
        });
    assert!(seeds > 0, "the mutation hunt runs at least one seed");
    println!("--- mutation hunt: seeds 1..={seeds} ---");
    let mut ok = 0;
    let mut first = 1;
    while first <= seeds {
        let last = (first + MUTANT_BATCH - 1).min(seeds);
        let report = chain_mutants(first..=last);
        ok += report.successful_runs;
        if !report.assertion_violations.is_empty() || report.failed_runs > 0 {
            for (seed, run) in report.seeds_used.iter().zip(&report.individual_metrics) {
                if let Err(error) = run {
                    println!("failed run: seed {seed}: {error}");
                }
            }
            println!("VIOLATIONS: {:#?}", report.assertion_violations);
            println!("FAILING SEEDS: {:?}", report.seeds_failing);
            println!("caught in seeds {first}..={last}, after {ok} clean runs");
            std::process::exit(1);
        }
        first = last + 1;
    }
    println!("{seeds} seeds: {ok} ok, no violations: the hunt came back clean");
}
