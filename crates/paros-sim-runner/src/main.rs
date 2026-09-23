//! Native campaign runner. `cargo xtask sim run paros-chain` drives the
//! coverage-guided + frontier-exploration gate on the main campaign, then the
//! two scripted corpus axes.

mod common;

use common::{arg, is_clean, print_never_fired, print_seed_counts};
use paros_sim::{
    AssertKind, CHUNK_CORPUS_CI_ITERATIONS, CORPUS_CI_ITERATIONS, COVERAGE_ITERATIONS,
    SimulationReport, chunk_corpus_hunt, corpus_hunt, explore,
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
    if is_clean(&report) && report.coverage_violations.is_empty() && !report.convergence_timeout {
        println!("  Chain safety, recovery, coverage, and saturation gates are green");
    } else {
        println!("  SAFETY VIOLATIONS: {:?}", report.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", report.coverage_violations);
        println!("  CONVERGENCE TIMEOUT: {}", report.convergence_timeout);
        println!(
            "  FAILING SEEDS (replay with sim-paros-hunt replay-main): {:?}",
            report.seeds_failing
        );
        // This runner is the coverage-guided sweep gate (`cargo xtask sim`):
        // any correctness, coverage, or saturation defect must fail CI.
        std::process::exit(1);
    }

    run_corpus_axes();
}

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

/// One corpus axis: fail CI on any violation or a gate that never fired.
fn gate_corpus(name: &str, report: &SimulationReport) {
    println!("\n--- {name} ---");
    print_seed_counts(report, "");
    print_never_fired(report, "  ");
    if !is_clean(report) || !report.coverage_violations.is_empty() {
        println!("  SAFETY VIOLATIONS: {:?}", report.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", report.coverage_violations);
        println!("  FAILING SEEDS: {:?}", report.seeds_failing);
        std::process::exit(1);
    }
    println!("  green");
}

/// The two scripted evaluation corpora (E1 masks, per-chunk masks): seeded-mask
/// sampling with the recovery gates armed. Fast per seed — every fault is a
/// targeted injection, not swarm chaos.
fn run_corpus_axes() {
    gate_corpus(
        "CTRL E1 mask corpus axis",
        &corpus_hunt(CORPUS_CI_ITERATIONS),
    );
    gate_corpus(
        "CTRL chunk mask corpus axis",
        &chunk_corpus_hunt(CHUNK_CORPUS_CI_ITERATIONS),
    );
}
