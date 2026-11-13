//! Red-seed hunt driver: raw seed volume through one campaign axis, reporting
//! every assertion violation and its seed. Unlike `sim-paros-chain` (the CI
//! saturation gate), a hunt never stops at a coverage plateau and treats
//! coverage gates as irrelevant — its only deliverable is failing seeds.
//!
//! Usage: `sim-paros-hunt [main|canary] [iterations]`
//!        `sim-paros-hunt replay-main <seed>` — deterministic single-seed
//!        replay on the main campaign (the red→green witness command).
//!        `sim-paros-hunt canary [iterations]` — the main campaign under
//!        moonpool's determinism canary: every seed runs twice and the replay
//!        must reproduce the first run's draw fingerprints, all of them. Its
//!        deliverable is an entropy leak, named by the first diverging draw.
//!        `sim-paros-hunt explore-main <seed>` — root + explored continuation
//!        timelines, for failures that live only on explorer branches.
//!        `sim-paros-hunt replay-recipe <seed> <count>:<reseed>,..` — one
//!        explored timeline, from a bug recipe the sweep printed.
//!        `sim-paros-hunt replay-main <seed> --dump <dir>` — the replay, and
//!        what it shows written to `<dir>` (#921 (seed tools)): its
//!        fingerprint, violations, per-seed draws and the gates it fired.
//!        `sim-paros-hunt gate-diff <seed> [greens]` — the gates the seed
//!        fired that none of `greens` (default 20) fresh seeds fired.

mod common;
mod gates;
#[cfg(test)]
mod scan;

use common::{arg, is_clean, print_excuses, print_failed_runs, print_seed_counts};
use paros_sim::{
    AssertionStats, EXPLORATION_TIMELINES_PER_SEED, SimulationReport, chain_canary_hunt,
    chain_seed_canary, chain_seed_dump, chain_smoke, explore_chain_seed, replay_chain_timeline,
    run_chain_seed,
};
use std::collections::BTreeMap;
use std::path::Path;

/// The single-seed replay a `replay-*` / `explore-main` axis names, if any.
fn replay_for(axis: &str) -> Option<fn(u64) -> SimulationReport> {
    Some(match axis {
        "replay-main" => run_chain_seed,
        "replay-canary" => chain_seed_canary,
        "explore-main" => |seed| explore_chain_seed(seed, EXPLORATION_TIMELINES_PER_SEED),
        _ => return None,
    })
}

fn main() {
    let axis = std::env::args().nth(1).unwrap_or_else(|| "main".into());

    if axis == "replay-recipe" {
        let seed: u64 = arg(2).expect("replay-recipe needs a seed");
        let recipe: Vec<(u64, u64)> = std::env::args()
            .nth(3)
            .expect("replay-recipe needs a recipe: count:reseed,..")
            .split(',')
            .map(|step| {
                let (count, reseed) = step.split_once(':').expect("count:reseed");
                (
                    count.parse().expect("a count"),
                    reseed.parse().expect("a reseed"),
                )
            })
            .collect();
        println!("--- replay: recipe {recipe:?} on seed {seed} ---");
        let report = replay_chain_timeline(seed, recipe);
        if is_clean(&report) {
            println!("seed {seed}: GREEN");
            return;
        }
        println!("seed {seed}: RED");
        print_failed_runs(&report);
        println!("VIOLATIONS: {:#?}", report.assertion_violations);
        std::process::exit(1);
    }

    if axis == "gate-diff" {
        let seed: u64 = arg(2).expect("gate-diff needs a seed");
        gate_diff(seed, arg(3).unwrap_or(GATE_DIFF_GREENS));
        return;
    }

    if axis == "replay-main" && std::env::args().nth(3).as_deref() == Some("--dump") {
        let seed: u64 = arg(2).expect("replay needs a seed");
        let dir = std::env::args().nth(4).expect("--dump needs a directory");
        dump(seed, Path::new(&dir));
        return;
    }

    if let Some(replay) = replay_for(&axis) {
        let seed: u64 = arg(2).expect("replay needs a seed");
        println!("--- replay: {axis} seed {seed} ---");
        let report = replay(seed);
        if is_clean(&report) {
            println!("seed {seed}: GREEN");
            return;
        }
        println!("seed {seed}: RED");
        print_failed_runs(&report);
        println!("VIOLATIONS: {:#?}", report.assertion_violations);
        std::process::exit(1);
    }

    // AGENTS.md, *Raw hunt budget*: 2,000-3,000 ordinary seeds is the normal
    // evidence target for this binary, so that is what it does with no
    // argument. A larger hunt is an explicit request.
    let iterations = arg(2).unwrap_or(2000);

    println!("--- hunt: {axis} axis, {iterations} seeds ---");
    let report = match axis.as_str() {
        "main" => chain_smoke(iterations),
        "canary" => chain_canary_hunt(iterations),
        other => {
            eprintln!("unknown axis: {other} (expected 'main' or 'canary')");
            std::process::exit(2);
        }
    };

    print_seed_counts(&report, "");
    // The canary's seeds are fresh on every run (#460 (the canary covers two
    // seeds)): name them all, so a clean CI run says what it proved.
    if axis == "canary" {
        println!("canary seeds: {:?}", report.seeds_used);
    }
    // The assertion-slot budget (AGENTS.md, *Simulation rules*): 8192
    // slots per campaign process (moonpool's `MAX_ASSERTION_SLOTS`), shared
    // with moonpool's own internals.
    // Printed on every hunt so "count before adding" has a number to read,
    // and an overflow — evaluations dropped for want of a slot — is never
    // silent.
    gates::print_slot_headroom(&report.assertion_results, "");
    println!(
        "{} evaluations dropped for want of a slot",
        report.dropped_assertion_allocations,
    );
    // A hunt's deliverable is failing seeds, so coverage never decides its exit
    // status — but a gate that never fired across the whole hunt is exactly what
    // a starved `sometimes` looks like in the CI sweep, and finding it here is
    // far cheaper than re-running the full coverage campaign to see it.
    print_never_fired(&report, "");
    // The declared gates the hunt never ran, and the marginal ones (#446 (a
    // gate that never fires cannot fail the sweep)).
    gates::print(&gates::judge(&report.assertion_results), "");
    gates::print_rarest(&report.assertion_results, RAREST);
    print_excuses(&report.assertion_results);
    if let Some(filter) = arg::<String>(3) {
        print_gates(&report, &filter);
    }
    if is_clean(&report) {
        println!("no violations — the hunt came back empty");
        return;
    }
    print_failed_runs(&report);
    println!("VIOLATIONS: {:#?}", report.assertion_violations);
    println!("FAILING SEEDS: {:?}", report.seeds_failing);
    std::process::exit(1);
}

/// How many of the rarest declared gates a hunt prints.
const RAREST: usize = 25;

/// Every gate whose name contains `filter`, with its successes and checks: a
/// gate's rate, measured before and after a change (AGENTS.md, *Amplify the
/// worst states*).
fn print_gates(report: &SimulationReport, filter: &str) {
    println!("gates matching {filter:?}:");
    for (gate, stats) in &report.assertion_results {
        if gate.contains(filter) {
            println!(
                "  - {gate}: {} of {} checks",
                stats.successes, stats.total_checks
            );
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

/// Replay `seed` and write what it shows to `dir` (#921 (seed tools)):
/// `fingerprint.txt` (the digest, the simulated time, the event count),
/// `violations.txt` (the violations and failed runs), `draws.txt` (every
/// per-seed draw: the quorum system, the pools, the journal plan, each
/// scenario coin, `run_end`, the machine layout and each node's shape) and
/// `gates.txt` (each gate the seed fired, with its successes and checks).
/// Exits 1 when the seed is red, as `replay-main` does.
fn dump(seed: u64, dir: &Path) {
    println!(
        "--- replay: replay-main seed {seed}, dump to {} ---",
        dir.display()
    );
    let dump = chain_seed_dump(seed);
    let write = |name: &str, body: String| {
        let path = dir.join(name);
        std::fs::write(&path, body)
            .unwrap_or_else(|error| panic!("cannot write {}: {error}", path.display()));
    };
    std::fs::create_dir_all(dir)
        .unwrap_or_else(|error| panic!("cannot create {}: {error}", dir.display()));
    write("fingerprint.txt", format!("{}\n", dump.fingerprint));
    let mut violations: Vec<String> = dump.report.assertion_violations.clone();
    violations.extend(dump.report.individual_metrics.iter().filter_map(|run| {
        run.as_ref()
            .err()
            .map(|error| format!("failed run: {error}"))
    }));
    write("violations.txt", lines(&violations));
    write(
        "draws.txt",
        dump.draws
            .unwrap_or_else(|| "no client reached check(): no draws recorded\n".to_owned()),
    );
    write("gates.txt", lines(&fired(&dump.report.assertion_results)));
    println!("{}", dump.fingerprint);
    if is_clean(&dump.report) {
        println!("seed {seed}: GREEN");
        return;
    }
    println!("seed {seed}: RED");
    print_failed_runs(&dump.report);
    println!("VIOLATIONS: {:#?}", dump.report.assertion_violations);
    std::process::exit(1);
}

/// One line per entry, each ended by a newline.
fn lines(entries: &[String]) -> String {
    entries.iter().fold(String::new(), |mut out, entry| {
        out.push_str(entry);
        out.push('\n');
        out
    })
}

/// Each slot that fired, as `<successes>/<checks> <name>`, by name.
fn fired(slots: &BTreeMap<String, AssertionStats>) -> Vec<String> {
    slots
        .iter()
        .filter(|(_, stats)| stats.successes > 0)
        .map(|(name, stats)| format!("{}/{} {name}", stats.successes, stats.total_checks))
        .collect()
}

/// How many fresh seeds `gate-diff` compares a seed with, by default.
const GATE_DIFF_GREENS: usize = 20;

/// Run `seed`, then `greens` fresh seeds of the same build, and print the
/// gates the seed fired that no other seed fired (#921 (seed tools)): the
/// rare paths the red seed took, where its diagnosis starts. The fresh
/// seeds are not checked green: a red one only narrows the list.
fn gate_diff(seed: u64, greens: usize) {
    println!("--- gate diff: seed {seed} against {greens} fresh seeds ---");
    let red = run_chain_seed(seed);
    println!(
        "seed {seed}: {}",
        if is_clean(&red) { "GREEN" } else { "RED" }
    );
    let others = chain_smoke(greens);
    print_seed_counts(&others, " (the fresh seeds)");
    let only = only_fired(&red.assertion_results, &others.assertion_results);
    println!(
        "gates seed {seed} fired that no fresh seed fired: {}",
        only.len()
    );
    for gate in only {
        println!("  - {gate}");
    }
}

/// The gates of `seed` that fired there and never in `others`, as `fired`
/// prints them.
fn only_fired(
    seed: &BTreeMap<String, AssertionStats>,
    others: &BTreeMap<String, AssertionStats>,
) -> Vec<String> {
    let alone: BTreeMap<String, AssertionStats> = seed
        .iter()
        .filter(|(name, _)| others.get(*name).is_none_or(|stats| stats.successes == 0))
        .map(|(name, stats)| (name.clone(), stats.clone()))
        .collect();
    fired(&alone)
}

#[cfg(test)]
mod dump_tests {
    use std::collections::BTreeMap;

    use paros_sim::AssertionStats;

    use super::{fired, only_fired};

    fn stats(total_checks: usize, successes: usize) -> AssertionStats {
        AssertionStats {
            total_checks,
            successes,
        }
    }

    #[test]
    fn fired_lists_only_the_gates_with_a_success() {
        let slots = BTreeMap::from([("a".to_owned(), stats(3, 2)), ("b".to_owned(), stats(4, 0))]);
        assert_eq!(fired(&slots), vec!["2/3 a"]);
    }

    #[test]
    fn a_gate_diff_keeps_what_only_the_seed_fired() {
        let seed = BTreeMap::from([
            ("both".to_owned(), stats(1, 1)),
            ("alone".to_owned(), stats(2, 1)),
            ("idle".to_owned(), stats(2, 0)),
            ("missed".to_owned(), stats(1, 1)),
        ]);
        let others = BTreeMap::from([
            ("both".to_owned(), stats(9, 4)),
            ("missed".to_owned(), stats(9, 0)),
        ]);
        assert_eq!(only_fired(&seed, &others), vec!["1/2 alone", "1/1 missed"]);
    }
}
