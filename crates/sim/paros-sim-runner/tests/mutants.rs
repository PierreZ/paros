//! The mutation hunt (#269), cargo-mutants' test command, and the negative
//! hunt's (`cargo xtask negative`): the fixed seeds `1..=N` of the main
//! campaign (`paros_sim::chain_mutants`), the same for every mutant, run one
//! seed at a time. A mutant is caught at the first seed with a red that the
//! unmutated hunt does not have on that seed (`paros_sim::mutant_hunt`), and
//! the hunt exits 1 there; the seeds after it do not run. It exits 0 when
//! no seed has such a red: the mutant survived.
//!
//! The environment sets the hunt:
//!
//! - `PAROS_MUTANT_SEEDS`: the seed count `N` (default `MUTANT_SEEDS`).
//! - `PAROS_MUTANT_RECORD=<file>`: the baseline run. Every seed runs, each
//!   red is written to the file as `<seed>\t<red>`, and the hunt exits 0.
//! - `PAROS_MUTANT_BASELINE=<file>`: the reds of the baseline run. A red the
//!   baseline has on the same seed catches nothing. With no baseline every
//!   red catches the mutant.
//! - `PAROS_MUTANT_CAUSES`: the causes that catch the mutant, one per line.
//!   With none, every red the baseline does not have catches it.
//!
//! Run it through `cargo xtask mutants` or `cargo xtask negative`, which set
//! these; by hand: `cargo test --release -p paros-sim-runner --features
//! mutants --test mutants`. Built only with the `mutants` feature, so the
//! plain nextest run never pays for it.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use paros_sim::mutant_hunt::{baseline, line, of_a_cause, reds};
use paros_sim::{MUTANT_SEEDS, chain_mutants};

fn main() {
    let seeds = std::env::var("PAROS_MUTANT_SEEDS")
        .ok()
        .map_or(MUTANT_SEEDS, |s| {
            s.parse().expect("PAROS_MUTANT_SEEDS is a seed count")
        });
    assert!(seeds > 0, "the mutation hunt runs at least one seed");
    let record = std::env::var("PAROS_MUTANT_RECORD").ok();
    let known: BTreeSet<(u64, String)> =
        std::env::var("PAROS_MUTANT_BASELINE")
            .ok()
            .map_or_else(BTreeSet::new, |path| {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("PAROS_MUTANT_BASELINE {path}: {e}"));
                baseline(&text)
            });
    let causes: Vec<String> = std::env::var("PAROS_MUTANT_CAUSES")
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(str::to_string)
        .collect();
    assert!(
        record.is_none() || (known.is_empty() && causes.is_empty()),
        "a baseline run judges nothing"
    );
    println!(
        "--- mutation hunt: seeds 1..={seeds}, {} baseline reds, {} causes ---",
        known.len(),
        causes.len()
    );
    let mut recorded = String::new();
    let mut green = 0_u64;
    for seed in 1..=seeds {
        let report = chain_mutants(seed..=seed);
        let seed_reds = reds(&report);
        if seed_reds.is_empty() {
            green += 1;
        }
        if record.is_some() {
            for red in &seed_reds {
                let _ = writeln!(recorded, "{}", line(seed, red));
            }
            continue;
        }
        let new: Vec<&String> = seed_reds
            .iter()
            .filter(|red| !known.contains(&(seed, (*red).clone())))
            .collect();
        let catching: Vec<&&String> = new.iter().filter(|r| of_a_cause(r, &causes)).collect();
        for red in &new {
            println!("new red: seed {seed}: {red}");
        }
        if !catching.is_empty() {
            for (s, cause) in &report.failure_causes {
                println!("seed {s} failed: {cause}");
            }
            println!("caught at seed {seed}, after {green} green seeds");
            std::process::exit(1);
        }
    }
    if let Some(path) = record {
        std::fs::write(&path, &recorded).unwrap_or_else(|e| panic!("{path}: {e}"));
        println!("{seeds} seeds recorded: {green} green, the reds in {path}");
        return;
    }
    println!("{seeds} seeds: {green} green, no red of the mutant: the hunt came back clean");
}
