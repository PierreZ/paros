//! Native Chain-of-Blocks campaign runner. `cargo xtask sim run paros-chain`
//! drives the coverage-guided + frontier-exploration gate, then prints one legacy
//! visualization timeline for the browser contract.

use paros_sim::{
    AssertKind, BUDGET_OFF_COVERAGE_ITERATIONS, CHUNK_CORPUS_CI_ITERATIONS, CORPUS_CI_ITERATIONS,
    COVERAGE_ITERATIONS, Outcome, PROTOCOL_BOUNDS_COVERAGE_ITERATIONS,
    SNAPSHOT_RECOVERY_COVERAGE_ITERATIONS, SimulationReport, chunk_corpus_hunt, corpus_hunt,
    explore, explore_budget_off, explore_protocol_bounds, explore_snapshot_recovery, run_seed,
    run_seed_json,
};

fn main() {
    // The illustrative timeline's seed (step 3 below). A *display* default, not
    // a pinned witness: every seed must satisfy what this run asserts, so the
    // value only decides which run gets printed for eyeballing.
    let seed = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(42);
    // Optional second arg: exploration iteration budget (defaults to the sancov
    // coverage cap). Lets a hunt drive the code-coverage-guided sweep harder.
    let iterations = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(COVERAGE_ITERATIONS);

    // 1. The DST bug-finding sweep: many seeds of combined swarm chaos
    //    (network + attrition + BUGGIFY), asserting safety and recovery.
    println!("--- Chain-of-Blocks campaign (coverage + exploration) ---");
    let report = explore(iterations);
    let stop = if report.convergence_timeout {
        "hit the iteration cap (did NOT saturate)"
    } else {
        "saturated (coverage plateaued, all reachability fired)"
    };
    println!(
        "{} seeds: {} ok, {} failed — {}",
        report.iterations, report.successful_runs, report.failed_runs, stop,
    );
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
    if !report.coverage_violations.is_empty() {
        println!("  coverage gates that never fired:");
        for gate in &report.coverage_violations {
            println!("    - {gate}");
        }
    }
    if report.assertion_violations.is_empty()
        && report.failed_runs == 0
        && report.coverage_violations.is_empty()
        && !report.convergence_timeout
    {
        println!("  Chain safety, recovery, coverage, and saturation gates are green");
    } else {
        println!("  SAFETY VIOLATIONS: {:?}", report.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", report.coverage_violations);
        println!("  CONVERGENCE TIMEOUT: {}", report.convergence_timeout);
        println!(
            "  FAILING SEEDS (replay with run_seed): {:?}",
            report.seeds_failing
        );
        // This runner is the coverage-guided sweep gate (`cargo xtask sim`): a
        // Any correctness, coverage, or saturation defect must fail CI.
        std::process::exit(1);
    }

    run_protocol_bounds_axis();
    run_snapshot_recovery_axis();
    run_budget_off_axis();
    run_corpus_axes();

    // 3. A single seed, with its full message timeline for eyeballing.
    println!("\n--- single seed timeline ---");
    let result = run_seed(seed);

    println!(
        "seed {} — {} proposals over the simulated network: {} delivered, {} dropped, \
         {} logical ticks, slowest RTT {} ms, {} ms simulated\n",
        result.seed,
        result.requests,
        result.delivered,
        result.dropped,
        result.ticks,
        result.longest_rtt_ms,
        result.sim_duration_ms,
    );

    for shot in &result.shots {
        let arrow = if shot.from == 0 { "A → B" } else { "B → A" };
        let mark = match shot.outcome {
            Outcome::Delivered => "delivered",
            Outcome::Dropped => "✗ DROPPED",
        };
        println!(
            "  req {:>2}  {}  {:>4}ms  (t={:>5}ms)  {}",
            shot.seq, arrow, shot.latency_ms, shot.arrive_ms, mark,
        );
    }

    // Print the JSON the browser would receive, so the wire format is eyeballable.
    println!("\n--- JSON (what runSeed returns to the browser) ---");
    println!("{}", run_seed_json(seed));
}

/// One deterministic long-suffix sequence proving the protocol resource bounds.
fn run_protocol_bounds_axis() {
    println!("\n--- Protocol bounds choreography axis ---");
    let bounds = explore_protocol_bounds(PROTOCOL_BOUNDS_COVERAGE_ITERATIONS);
    println!(
        "{} seeds: {} ok, {} failed; convergence_timeout={}",
        bounds.iterations, bounds.successful_runs, bounds.failed_runs, bounds.convergence_timeout,
    );
    if !bounds.assertion_violations.is_empty()
        || bounds.failed_runs > 0
        || !bounds.coverage_violations.is_empty()
        || bounds.convergence_timeout
    {
        println!("  ASSERTION VIOLATIONS: {:?}", bounds.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", bounds.coverage_violations);
        println!("  FAILING SEEDS: {:?}", bounds.seeds_failing);
        std::process::exit(1);
    }
    println!("  Promise, Ready, Accepted, and Nack bounds are green");
}

/// One fixed three-node lifecycle sequence, kept separate from the main
/// campaign's broad operation chaos.
fn run_snapshot_recovery_axis() {
    println!("\n--- Chain graceful snapshot-recovery choreography axis ---");
    let recovery = explore_snapshot_recovery(SNAPSHOT_RECOVERY_COVERAGE_ITERATIONS);
    println!(
        "{} seeds: {} ok, {} failed; convergence_timeout={}",
        recovery.iterations,
        recovery.successful_runs,
        recovery.failed_runs,
        recovery.convergence_timeout,
    );
    if let Some(s) = &recovery.saturation {
        println!(
            "  signal {:?}: {}/{} reachability fired, plateau {}",
            s.signal, s.sometimes_hit, s.sometimes_total, s.plateau_seeds,
        );
    }
    if !recovery.coverage_violations.is_empty() {
        println!("  coverage gates that never fired:");
        for gate in &recovery.coverage_violations {
            println!("    - {gate}");
        }
    }
    if !recovery.assertion_violations.is_empty()
        || recovery.failed_runs > 0
        || !recovery.coverage_violations.is_empty()
        || recovery.convergence_timeout
    {
        println!("  SAFETY VIOLATIONS: {:?}", recovery.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", recovery.coverage_violations);
        println!("  FAILING SEEDS: {:?}", recovery.seeds_failing);
        std::process::exit(1);
    }
    println!("  Graceful kill, truncation, restart, and snapshot recovery are green");
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

/// The budget-off (WAITED-leg) exploration axis: the main campaign's chaos with
/// the per-record corruption budget lifted, so every copy of a committed item
/// can rot in one run. Its `GateScope::BudgetOff` coverage pair — repair from a
/// surviving clean copy AND a correct WAIT — saturates only here, so the axis
/// must run in the registered campaign, not just the manual hunt binary.
fn run_budget_off_axis() {
    println!("\n--- Chain budget-off (WAITED-leg) axis ---");
    let budget = explore_budget_off(BUDGET_OFF_COVERAGE_ITERATIONS);
    println!(
        "{} seeds: {} ok, {} failed; convergence_timeout={}",
        budget.iterations, budget.successful_runs, budget.failed_runs, budget.convergence_timeout,
    );
    if let Some(s) = &budget.saturation {
        println!(
            "  signal {:?}: {}/{} reachability fired, plateau {}",
            s.signal, s.sometimes_hit, s.sometimes_total, s.plateau_seeds,
        );
    }
    if !budget.coverage_violations.is_empty() {
        println!("  coverage gates that never fired:");
        for gate in &budget.coverage_violations {
            println!("    - {gate}");
        }
    }
    if !budget.assertion_violations.is_empty()
        || budget.failed_runs > 0
        || !budget.coverage_violations.is_empty()
        || budget.convergence_timeout
    {
        println!("  SAFETY VIOLATIONS: {:?}", budget.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", budget.coverage_violations);
        println!("  FAILING SEEDS: {:?}", budget.seeds_failing);
        std::process::exit(1);
    }
    println!("  Budget-off safety, WAITED, and repaired gates are green");
}

/// The two scripted evaluation corpora (#113 E1 masks, #101 chunk masks):
/// seeded-mask sampling with the Stage-8 gates armed. Fast per seed — every
/// fault is a targeted injection, not swarm chaos.
fn run_corpus_axes() {
    println!("\n--- CTRL E1 mask corpus axis ---");
    let corpus = corpus_hunt(CORPUS_CI_ITERATIONS);
    println!(
        "{} seeds: {} ok, {} failed",
        corpus.iterations, corpus.successful_runs, corpus.failed_runs,
    );
    if !corpus.coverage_violations.is_empty() {
        println!("  coverage gates that never fired:");
        for gate in &corpus.coverage_violations {
            println!("    - {gate}");
        }
    }
    if !corpus.assertion_violations.is_empty()
        || corpus.failed_runs > 0
        || !corpus.coverage_violations.is_empty()
    {
        println!("  SAFETY VIOLATIONS: {:?}", corpus.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", corpus.coverage_violations);
        println!("  FAILING SEEDS: {:?}", corpus.seeds_failing);
        std::process::exit(1);
    }
    println!("  E1 mask corpus gate is green");

    println!("\n--- CTRL chunk mask corpus axis ---");
    let chunks = chunk_corpus_hunt(CHUNK_CORPUS_CI_ITERATIONS);
    println!(
        "{} seeds: {} ok, {} failed",
        chunks.iterations, chunks.successful_runs, chunks.failed_runs,
    );
    if !chunks.coverage_violations.is_empty() {
        println!("  coverage gates that never fired:");
        for gate in &chunks.coverage_violations {
            println!("    - {gate}");
        }
    }
    if !chunks.assertion_violations.is_empty()
        || chunks.failed_runs > 0
        || !chunks.coverage_violations.is_empty()
    {
        println!("  SAFETY VIOLATIONS: {:?}", chunks.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", chunks.coverage_violations);
        println!("  FAILING SEEDS: {:?}", chunks.seeds_failing);
        std::process::exit(1);
    }
    println!("  Chunk mask corpus gate is green");
}
