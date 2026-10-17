//! Native campaign runner. `cargo xtask sim run paros-chain` drives the
//! coverage-guided + frontier-exploration gate on the one campaign (#263:
//! no scripted side axis).

mod common;
mod gates;
#[cfg(test)]
mod scan;

use common::{arg, is_clean, print_failed_runs, print_never_fired, print_seed_counts};
use std::collections::BTreeMap;

use paros_sim::{
    AssertKind, AssertionStats, COVERAGE_ITERATIONS, SimulationReport, explore, explore_more,
};

fn main() {
    // Optional first arg: exploration iteration budget (defaults to the sancov
    // coverage cap). Lets a hunt drive the code-coverage-guided sweep harder.
    let iterations = arg(1).unwrap_or(COVERAGE_ITERATIONS);

    println!("--- Chain-of-Blocks campaign (coverage + exploration) ---");
    let report = explore(iterations);
    let stop = if report.convergence_timeout {
        "hit the iteration cap (did NOT saturate)"
    } else {
        "saturated (coverage plateaued, all reachability fired)"
    };
    print_seed_counts(&report, &format!(" — {stop}"));
    if let Some(s) = &report.saturation {
        println!(
            "  signal {:?}: {}/{} reachability fired, {}/{} edges, plateau {}",
            s.signal,
            s.sometimes_hit,
            s.sometimes_total,
            s.edges_covered,
            s.edges_total,
            s.plateau_seeds,
        );
    }
    if let Some(exploration) = &report.exploration {
        println!(
            "  exploration: {} timelines, {} expansions, {} discoveries, {} bugs",
            exploration.total_timelines,
            exploration.expansions,
            exploration.discoveries,
            exploration.bugs_found,
        );
        for recipe in &exploration.bug_recipes {
            println!("  bug recipe seed={} {:?}", recipe.seed, recipe.recipe);
        }
    }
    print_guidance(&report);
    // Name the `sometimes`/`reachable` gates that never fired. Saturation is the
    // sweep's real exit criterion, and "did not saturate" is useless without
    // knowing *which* gate is starving — that is the knob to tune.
    print_never_fired(&report, "  ");
    let top_up = top_up(&report);
    let slots = merged_slots(&report, top_up.as_ref());
    // A declared gate that never ran has no slot, so moonpool never judges it
    // (#446 (a gate that never fires cannot fail the sweep)).
    let declared = gates::judge(&slots);
    gates::print(&declared, "  ");
    gates::print_rarest(&slots, RAREST);
    // A CI shard hands its gate defects (a declared gate that never ran, a
    // gate that never fired) to the job that judges every shard's together
    // (`.github/workflows/rust.yml`): such a defect fails CI only when every
    // shard has it. Judged per shard, a rare gate would miss twice as often as
    // with one sweep.
    let shard_out = std::env::var_os(MISSING_GATES_OUT);
    if let Some(path) = &shard_out {
        write_gate_defects(std::path::Path::new(path), &declared.missing, &report);
    }
    let shard = shard_out.is_some();
    // A gate that never fired keeps the sweep from saturating, so a shard
    // with a gate defect also hits the cap: that timeout is the gate's, and
    // the judging job owns it. A shard whose every gate fired and that still
    // hit the cap fails on its own.
    let gate_defect = !report.coverage_violations.is_empty() || !declared.missing.is_empty();
    if let Some(more) = top_up.as_ref().filter(|more| !is_clean(more)) {
        print_failed_runs(more);
        println!(
            "  TOP-UP SAFETY VIOLATIONS: {:?}",
            more.assertion_violations
        );
        println!(
            "  TOP-UP FAILING SEEDS (replay with sim-paros-hunt replay-main): {:?}",
            more.seeds_failing
        );
        std::process::exit(1);
    }
    if is_clean(&report)
        && (shard || report.coverage_violations.is_empty())
        && (shard || declared.missing.is_empty())
        && (!report.convergence_timeout || (shard && gate_defect))
    {
        println!("  Chain safety, recovery, coverage, and saturation gates are green");
    } else {
        print_failed_runs(&report);
        println!("  SAFETY VIOLATIONS: {:?}", report.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", report.coverage_violations);
        println!(
            "  DECLARED GATES THAT NEVER RAN: {}",
            declared.missing.len()
        );
        println!("  CONVERGENCE TIMEOUT: {}", report.convergence_timeout);
        println!(
            "  FAILING SEEDS (replay with sim-paros-hunt replay-main): {:?}",
            report.seeds_failing
        );
        // This runner is the coverage-guided sweep gate (`cargo xtask sim`):
        // any correctness, coverage, or saturation defect must fail CI.
        std::process::exit(1);
    }
}

/// The environment variable that makes this run a CI shard: the path where
/// it writes its gate defects, one per line, and no longer fails on them. A
/// safety violation, a failed run or a convergence timeout with every gate
/// fired still fails the shard.
const MISSING_GATES_OUT: &str = "PAROS_SIM_MISSING_GATES";

/// Write the shard's gate defects, sorted, one per line: each declared gate
/// that never ran and each gate moonpool reports as never fired, by its slot
/// name (the `Debug` form, so a line holds exactly one message). Two shards
/// name one gate alike, so the judging job can intersect their files.
fn write_gate_defects(path: &std::path::Path, missing: &[(&str, &str)], report: &SimulationReport) {
    let never_fired = report.coverage_violations.iter().map(|v| violated_gate(v));
    let mut lines: Vec<String> = missing
        .iter()
        .map(|&(msg, _)| gates::slot_name(msg))
        .chain(never_fired)
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

/// The gate a moonpool coverage violation names: every one quotes its slot
/// name as `('<name>')`. A violation in another shape is kept whole.
fn violated_gate(violation: &str) -> &str {
    violation
        .split_once("('")
        .and_then(|(_, rest)| rest.rsplit_once("')"))
        .map_or(violation, |(name, _)| name)
}

/// The seeds the declared gates are judged over, at least. The coverage
/// plateau stops the sweep after 170 to 700 seeds, whatever the gates' rates:
/// at 170 seeds a gate that fires on 1.5 % of seeds misses 8 % of sweeps,
/// and with hundreds of declared gates some PR's sweep goes red by chance
/// (PR #551's and main's sweeps of 2026-10-15, #446). At 400 seeds such a gate
/// misses 0.2 % of sweeps, which is the bound [`gates::RARE`] is written for.
const GATE_FLOOR_SEEDS: usize = 400;

/// When the plateau stopped the sweep under [`GATE_FLOOR_SEEDS`], run the
/// seeds it lacks: their gates count toward the declared-gate judgement and
/// their safety oracles judge as the sweep's do. Coverage saturation is
/// still the sweep's own.
fn top_up(report: &SimulationReport) -> Option<SimulationReport> {
    let lacking = GATE_FLOOR_SEEDS.checked_sub(report.iterations)?;
    if lacking == 0 {
        return None;
    }
    println!("--- top-up: {lacking} seeds to reach {GATE_FLOOR_SEEDS} for the declared gates ---");
    let more = explore_more(lacking);
    print_seed_counts(&more, "");
    if let Some(exploration) = &more.exploration {
        for recipe in &exploration.bug_recipes {
            println!("  bug recipe seed={} {:?}", recipe.seed, recipe.recipe);
        }
    }
    Some(more)
}

/// The sweep's assertion slots, with the top-up's added.
fn merged_slots(
    report: &SimulationReport,
    top_up: Option<&SimulationReport>,
) -> BTreeMap<String, AssertionStats> {
    let mut slots = report.assertion_results.clone();
    for (msg, stats) in top_up.iter().flat_map(|more| &more.assertion_results) {
        let slot = slots.entry(msg.clone()).or_default();
        slot.total_checks += stats.total_checks;
        slot.successes += stats.successes;
    }
    slots
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
    use super::violated_gate;

    #[test]
    fn a_coverage_violation_names_its_gate() {
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
        assert_eq!(violated_gate("something else"), "something else");
    }
}
