//! Native campaign runner. `cargo xtask sim run paros-chain` drives the
//! coverage-guided + frontier-exploration gate on the one campaign (#263:
//! no scripted side axis).

mod common;
mod gates;
#[cfg(test)]
mod scan;

use common::{arg, is_clean, print_excuses, print_failed_runs, print_seed_counts};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use paros_sim::{AssertKind, AssertionStats, SWEEP_BATCH, SimulationReport, explore};

fn main() {
    // Optional first arg: the sweep's time budget in minutes, which
    // `PAROS_SIM_SWEEP_MINUTES` sets too (CI). The sweep runs batches until
    // the budget is spent (#920 (sweep budget)).
    let budget = sweep_budget();
    println!(
        "--- Chain-of-Blocks campaign (coverage + exploration), {} min budget, batches of {SWEEP_BATCH} seeds ---",
        budget.as_secs() / 60
    );
    let sweep = Sweep::run(budget);
    println!(
        "{} seeds in {} batches, {} timelines, {:.1} min{}",
        sweep.seeds,
        sweep.batches,
        sweep.timelines,
        sweep.elapsed.as_secs_f64() / 60.0,
        if sweep.red.is_some() {
            " — stopped at the first red batch"
        } else {
            " — budget spent"
        },
    );
    if let Some(edges) = &sweep.edges {
        println!("  {edges}");
    }
    if let Some(last) = &sweep.last {
        print_guidance(last);
    }
    let never_fired = sweep.never_fired();
    print_never_fired(&never_fired, "  ");
    // A declared gate that never ran has no slot, so moonpool never judges it
    // (#446 (a gate that never fires cannot fail the sweep)).
    let declared = gates::judge(&sweep.slots);
    gates::print(&declared, "  ");
    gates::print_rarest(&sweep.slots, RAREST);
    print_excuses(&sweep.slots);
    if let Some(red) = &sweep.red {
        if let Some(path) = std::env::var_os(RED_OUT) {
            write_red(std::path::Path::new(&path), red);
        }
        print_failed_runs(red);
        if let Some(exploration) = &red.exploration {
            for recipe in &exploration.bug_recipes {
                println!("  bug recipe seed={} {:?}", recipe.seed, recipe.recipe);
            }
        }
        println!("  SAFETY VIOLATIONS: {:?}", red.assertion_violations);
        println!(
            "  FAILING SEEDS (replay with sim-paros-hunt replay-main): {:?}",
            red.seeds_failing
        );
        println!("  red batch base seed: {}", red.run_id);
        // This runner is the coverage-guided sweep gate (`cargo xtask sim`):
        // any correctness defect fails CI.
        std::process::exit(1);
    }
    // A CI shard hands its gate defects (a declared gate that never ran, a
    // gate that never fired) to the job that judges every shard's together
    // (`.github/workflows/rust.yml`): such a defect fails CI only when every
    // shard has it. Judged per shard, a rare gate would miss far more often
    // than with all the shards together.
    if let Some(path) = std::env::var_os(MISSING_GATES_OUT) {
        write_gate_defects(std::path::Path::new(&path), &declared.missing, &never_fired);
        println!(
            "  Chain safety and recovery gates are green; the gates are judged over every shard"
        );
    } else if never_fired.is_empty() && declared.missing.is_empty() {
        println!("  Chain safety, recovery and coverage gates are green");
    } else {
        println!("  GATES THAT NEVER FIRED: {}", never_fired.len());
        println!(
            "  DECLARED GATES THAT NEVER RAN: {}",
            declared.missing.len()
        );
        std::process::exit(1);
    }
}

/// The sweep's time budget when neither the first argument nor
/// [`SWEEP_MINUTES_ENV`] gives one, in minutes (#920 (sweep budget)). The
/// old plateau stop made a green shard last 27 to 121 minutes (115 to 943
/// seeds); a fixed budget gives each shard the same sweep time, and the CI
/// job waits for its slowest shard.
const SWEEP_MINUTES: u64 = 45;

/// The environment variable that sets the sweep's budget, in minutes.
const SWEEP_MINUTES_ENV: &str = "PAROS_SIM_SWEEP_MINUTES";

/// The sweep's time budget: the first argument, else [`SWEEP_MINUTES_ENV`],
/// else [`SWEEP_MINUTES`], in minutes.
fn sweep_budget() -> Duration {
    let minutes = arg::<u64>(1)
        .or_else(|| {
            std::env::var(SWEEP_MINUTES_ENV)
                .ok()
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(SWEEP_MINUTES);
    Duration::from_secs(minutes.saturating_mul(60))
}

/// The sweep's batches, merged.
struct Sweep {
    /// The batches that ran.
    batches: usize,
    /// The root seeds that ran.
    seeds: usize,
    /// The timelines explored, root seeds included.
    timelines: u64,
    /// The wall time the sweep took.
    elapsed: Duration,
    /// Every batch's assertion slots, added together.
    slots: BTreeMap<String, AssertionStats>,
    /// Every gate a batch reported as never fired; [`Sweep::never_fired`]
    /// keeps those no batch fired.
    unfired: BTreeSet<String>,
    /// The first red batch: the sweep stops there.
    red: Option<SimulationReport>,
    /// The last batch, for its guidance.
    last: Option<SimulationReport>,
    /// The last batch's sancov edges, when the build is instrumented.
    edges: Option<String>,
}

impl Sweep {
    /// Run batches of [`SWEEP_BATCH`] seeds until `budget` is spent or a
    /// batch is red. Every batch draws its own base seed from the wall clock,
    /// printed so that a shard's seeds can be named.
    fn run(budget: Duration) -> Self {
        let started = Instant::now();
        let mut sweep = Self {
            batches: 0,
            seeds: 0,
            timelines: 0,
            elapsed: Duration::ZERO,
            slots: BTreeMap::new(),
            unfired: BTreeSet::new(),
            red: None,
            last: None,
            edges: None,
        };
        while started.elapsed() < budget {
            let batch = explore(SWEEP_BATCH);
            sweep.batches += 1;
            print!("batch {} (base seed {}): ", sweep.batches, batch.run_id);
            print_seed_counts(&batch, "");
            sweep.add(batch);
            if sweep.red.is_some() {
                break;
            }
        }
        sweep.elapsed = started.elapsed();
        sweep
    }

    /// Add one batch's report.
    fn add(&mut self, batch: SimulationReport) {
        self.seeds += batch.iterations;
        self.timelines += batch.exploration.as_ref().map_or_else(
            || u64::try_from(batch.iterations).unwrap_or(u64::MAX),
            |exploration| exploration.total_timelines,
        );
        merge_slots(&mut self.slots, &batch.assertion_results);
        self.unfired.extend(
            batch
                .coverage_violations
                .iter()
                .map(|v| violated_gate(v).to_owned()),
        );
        if let Some(s) = &batch.saturation {
            self.edges = Some(format!(
                "signal {:?}: {}/{} reachability fired, {}/{} edges",
                s.signal, s.sometimes_hit, s.sometimes_total, s.edges_covered, s.edges_total,
            ));
        }
        if is_clean(&batch) {
            self.last = Some(batch);
        } else {
            self.red = Some(batch);
        }
    }

    /// The gates some batch reported as never fired and no batch fired.
    fn never_fired(&self) -> Vec<String> {
        never_fired(&self.unfired, &self.slots)
    }
}

/// The gates of `unfired` whose slot in `slots` never succeeded.
fn never_fired(
    unfired: &BTreeSet<String>,
    slots: &BTreeMap<String, AssertionStats>,
) -> Vec<String> {
    unfired
        .iter()
        .filter(|gate| slots.get(*gate).is_none_or(|stats| stats.successes == 0))
        .cloned()
        .collect()
}

/// Name the `sometimes`/`reachable` gates that never fired, if any, each line
/// prefixed by `indent`.
fn print_never_fired(gates: &[String], indent: &str) {
    if gates.is_empty() {
        return;
    }
    println!("{indent}coverage gates that never fired:");
    for gate in gates {
        println!("{indent}  - {gate}");
    }
}

/// The environment variable that makes this run a CI shard: the path where
/// it writes its gate defects, one per line, and no longer fails on them. A
/// safety violation or a failed run still fails the shard.
const MISSING_GATES_OUT: &str = "PAROS_SIM_MISSING_GATES";

/// The environment variable that names where a red shard writes what failed
/// (#920 (sweep budget)): one line per violated assertion or failed run,
/// `<message>\t<failing seeds>`. CI's `main-reds` job adds the lines of a
/// red run on `main` to one table (`scripts/main-red-record.sh`).
const RED_OUT: &str = "PAROS_SIM_RED";

/// Write the red batch's failures to `path`, one per line: each violated
/// assertion by its slot name, and each failed run by the first line of its
/// error, with the batch's failing seeds.
fn write_red(path: &std::path::Path, red: &SimulationReport) {
    let seeds = red
        .seeds_failing
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    let lines: Vec<String> = red_messages(red)
        .into_iter()
        .map(|msg| format!("{}\t{seeds}\n", msg.replace(['\t', '\n'], " ")))
        .collect();
    if let Err(error) = std::fs::write(path, lines.concat()) {
        println!(
            "  cannot write the red lines to {}: {error}",
            path.display()
        );
    }
}

/// What failed in a red batch: each violated assertion by its slot name,
/// and each failed run by the first line of its error, sorted and unique.
fn red_messages(red: &SimulationReport) -> Vec<String> {
    let mut messages: Vec<String> = red
        .assertion_violations
        .iter()
        .map(|v| violated_gate(v).to_owned())
        .chain(red.individual_metrics.iter().filter_map(|run| {
            run.as_ref().err().map(|error| {
                let error = error.to_string();
                let first = error.lines().next().unwrap_or_default();
                format!("failed run: {first}")
            })
        }))
        .collect();
    messages.sort();
    messages.dedup();
    messages
}

/// Write the shard's gate defects, sorted, one per line: each declared gate
/// that never ran and each gate no batch fired, by its slot name (the
/// `Debug` form, so a line holds exactly one message). Two shards name one
/// gate alike, so the judging job can intersect their files.
fn write_gate_defects(path: &std::path::Path, missing: &[(&str, &str)], never_fired: &[String]) {
    let mut lines: Vec<String> = missing
        .iter()
        .map(|&(msg, _)| gates::slot_name(msg).to_owned())
        .chain(never_fired.iter().cloned())
        .map(|msg| format!("{msg:?}\n"))
        .collect();
    lines.sort();
    lines.dedup();
    if let Err(error) = std::fs::write(path, lines.concat()) {
        println!(
            "  cannot write the gate defects to {}: {error}",
            path.display()
        );
        std::process::exit(1);
    }
}

/// The gate a moonpool coverage or always violation names: every one quotes
/// its slot name as `('<name>')`. A violation in another shape is kept whole.
fn violated_gate(violation: &str) -> &str {
    violation
        .split_once("('")
        .and_then(|(_, rest)| rest.rsplit_once("')"))
        .map_or(violation, |(name, _)| name)
}

/// Add one batch's assertion slots to the sweep's.
fn merge_slots(
    slots: &mut BTreeMap<String, AssertionStats>,
    batch: &BTreeMap<String, AssertionStats>,
) {
    for (msg, stats) in batch {
        let slot = slots.entry(msg.clone()).or_default();
        slot.total_checks += stats.total_checks;
        slot.successes += stats.successes;
    }
}

/// How many of the rarest declared gates the sweep prints: its marginal
/// gates, one plateau away from never firing.
const RAREST: usize = 10;

fn print_guidance(report: &SimulationReport) {
    for detail in &report.assertion_details {
        match detail.kind {
            AssertKind::NumericSometimes | AssertKind::NumericAlways => println!(
                "  guidance {:?}: best watermark {}",
                detail.msg, detail.watermark,
            ),
            AssertKind::BooleanSometimesAll => println!(
                "  guidance {:?}: frontier {}/{}, {} combinations",
                detail.msg, detail.frontier, detail.frontier_target, detail.combinations_seen,
            ),
            _ => {}
        }
    }
    for bucket in &report.bucket_summaries {
        println!(
            "  buckets {:?}: {} discovered, {} hits",
            bucket.msg, bucket.buckets_discovered, bucket.total_hits,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use paros_sim::AssertionStats;

    use super::{merge_slots, never_fired, violated_gate};

    fn stats(total_checks: usize, successes: usize) -> AssertionStats {
        AssertionStats {
            total_checks,
            successes,
        }
    }

    #[test]
    fn a_batch_adds_its_checks_to_the_sweeps_slots() {
        let mut slots =
            BTreeMap::from([("a".to_owned(), stats(4, 1)), ("b".to_owned(), stats(2, 0))]);
        merge_slots(&mut slots, &BTreeMap::new());
        assert_eq!(slots.len(), 2);
        let batch = BTreeMap::from([("b".to_owned(), stats(3, 2)), ("c".to_owned(), stats(1, 1))]);
        merge_slots(&mut slots, &batch);
        let expected = BTreeMap::from([
            ("a".to_owned(), stats(4, 1)),
            ("b".to_owned(), stats(5, 2)),
            ("c".to_owned(), stats(1, 1)),
        ]);
        assert_eq!(slots, expected);
    }

    #[test]
    fn a_gate_another_batch_fired_is_not_reported() {
        let unfired = BTreeSet::from(["a".to_owned(), "b".to_owned(), "c".to_owned()]);
        let slots = BTreeMap::from([("a".to_owned(), stats(5, 1)), ("b".to_owned(), stats(5, 0))]);
        assert_eq!(never_fired(&unfired, &slots), vec!["b", "c"]);
        assert!(never_fired(&BTreeSet::new(), &slots).is_empty());
    }

    #[test]
    fn a_violation_names_its_gate() {
        assert_eq!(
            violated_gate(
                "assert_sometimes!('journal read: a wake') has 0% success rate (4 checks)"
            ),
            "journal read: a wake"
        );
        assert_eq!(
            violated_gate("assert_reachable!('a crash's site') was never reached"),
            "a crash's site"
        );
        assert_eq!(
            violated_gate("numeric assert_sometimes ('a gap') has 0% success rate (1 checks)"),
            "a gap"
        );
        assert_eq!(
            violated_gate("assert_always!('chain: one verdict') failed 2 times out of 9"),
            "chain: one verdict"
        );
        assert_eq!(violated_gate("something else"), "something else");
    }
}
