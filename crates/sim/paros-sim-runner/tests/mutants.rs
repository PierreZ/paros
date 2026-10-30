//! The mutation hunt (#269), cargo-mutants' test command: a fixed range of
//! main-campaign seeds (`paros_sim::chain_mutants`), the same for every
//! mutant, run in batches of `MUTANT_BATCH`. It exits non-zero at the first
//! batch with an assertion violation or a failed run (a paros-core `assert!`
//! panicking in a process), which is how cargo-mutants counts a mutant as
//! caught; the batches after it do not run.
//!
//! The negative hunt (#742 (mutant per excuse), `cargo xtask negative`) sets
//! `PAROS_MUTANT_EXCUSE` to the name of an oracle excuse
//! (`paros_sim::excuses::Excuse::name`). Then a red batch is not enough: each
//! failing seed runs again alone, and the mutant is caught only on a seed
//! where that excuse fired. Without one, the hunt goes on to the next batch.
//! It exits 0 when clean and non-zero when red, and its last line says why
//! (cargo passes no other exit code on): [`CAUGHT`] on a seed where the
//! excuse fired, or [`RED_ELSEWHERE`] only on seeds where it did not.
//!
//! Run it through `cargo xtask mutants` or `cargo xtask negative`, which set
//! `PAROS_MUTANT_SEEDS`; by hand: `cargo test --release -p paros-sim-runner
//! --features mutants --test mutants`. Built only with the `mutants` feature,
//! so the plain nextest run never pays for it.

use std::collections::BTreeMap;

use paros_sim::excuses::{Excuse, RUNS_SLOT};
use paros_sim::{MUTANT_BATCH, MUTANT_SEEDS, SimulationReport, chain_mutants};

/// The last line of a negative hunt caught on a seed where its excuse fired.
const CAUGHT: &str = "NEGATIVE: caught";

/// The last line of a negative hunt red only on seeds where its excuse did
/// not fire.
const RED_ELSEWHERE: &str = "NEGATIVE: red elsewhere";

/// The failing seeds of a negative hunt that run again alone, per batch: a
/// mutant red on many seeds proves itself on the first few.
const RERUNS_PER_BATCH: usize = 4;

fn main() {
    let seeds = std::env::var("PAROS_MUTANT_SEEDS")
        .ok()
        .map_or(MUTANT_SEEDS, |s| {
            s.parse().expect("PAROS_MUTANT_SEEDS is a seed count")
        });
    assert!(seeds > 0, "the mutation hunt runs at least one seed");
    let excuse = std::env::var("PAROS_MUTANT_EXCUSE").ok().map(|name| {
        Excuse::ALL
            .into_iter()
            .find(|excuse| excuse.name() == name)
            .unwrap_or_else(|| panic!("PAROS_MUTANT_EXCUSE names no excuse: {name:?}"))
    });
    match excuse {
        Some(excuse) => println!(
            "--- negative hunt: seeds 1..={seeds}, red where {:?} fires ---",
            excuse.name()
        ),
        None => println!("--- mutation hunt: seeds 1..={seeds} ---"),
    }
    let mut ok = 0;
    let mut red_elsewhere = Vec::new();
    let mut tally = BTreeMap::new();
    let mut first = 1;
    while first <= seeds {
        let last = (first + MUTANT_BATCH - 1).min(seeds);
        let report = chain_mutants(first..=last);
        ok += report.successful_runs;
        add_tally(&mut tally, &report);
        if !report.assertion_violations.is_empty() || report.failed_runs > 0 {
            print_red(&report);
            let Some(excuse) = excuse else {
                println!("caught in seeds {first}..={last}, after {ok} clean runs");
                std::process::exit(1);
            };
            for seed in report.seeds_failing.iter().take(RERUNS_PER_BATCH) {
                if fired_alone(*seed, excuse) {
                    println!("caught on seed {seed}, where {:?} fired", excuse.name());
                    println!("{CAUGHT}");
                    std::process::exit(1);
                }
                println!(
                    "seed {seed} is red, but {:?} did not fire on it",
                    excuse.name()
                );
                red_elsewhere.push(*seed);
            }
        }
        first = last + 1;
    }
    // How often each excuse fired over the whole hunt: a survivor whose
    // excuse fired far more than on main hides behind it.
    if excuse.is_some() {
        println!("excuse tally over the hunt: {tally:?}");
    }
    if !red_elsewhere.is_empty() {
        println!(
            "{seeds} seeds: red on {red_elsewhere:?}, never where {:?} fired",
            excuse.map_or("", Excuse::name)
        );
        println!("{RED_ELSEWHERE}");
        std::process::exit(1);
    }
    println!("{seeds} seeds: {ok} ok, no violations: the hunt came back clean");
}

/// Add the runs where each excuse fired in `report`, and the runs judged,
/// to `tally`.
fn add_tally(tally: &mut BTreeMap<&'static str, usize>, report: &SimulationReport) {
    let count = |slot: &str| {
        report
            .assertion_results
            .get(slot)
            .map_or(0, |stats| stats.successes)
    };
    *tally.entry("runs judged").or_default() += count(RUNS_SLOT);
    for excuse in Excuse::ALL {
        let fired = count(excuse.runs_slot());
        if fired > 0 {
            *tally.entry(excuse.name()).or_default() += fired;
        }
    }
}

/// Name every failed run and every violation of a red batch.
fn print_red(report: &SimulationReport) {
    for (seed, run) in report.seeds_used.iter().zip(&report.individual_metrics) {
        if let Err(error) = run {
            println!("failed run: seed {seed}: {error}");
        }
    }
    println!("VIOLATIONS: {:#?}", report.assertion_violations);
    println!("FAILING SEEDS: {:?}", report.seeds_failing);
}

/// Run `seed` alone again and tell whether `excuse` fired on it: the
/// excuse tally (#738 (count excuses, the baseline)) counts one per run.
fn fired_alone(seed: u64, excuse: Excuse) -> bool {
    let report = chain_mutants(seed..=seed);
    let fired: Vec<&str> = Excuse::ALL
        .into_iter()
        .filter(|e| {
            report
                .assertion_results
                .get(e.runs_slot())
                .is_some_and(|stats| stats.successes > 0)
        })
        .map(Excuse::name)
        .collect();
    println!("excuses on seed {seed}: {fired:?}");
    fired.contains(&excuse.name())
}
