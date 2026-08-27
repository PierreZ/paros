//! Native Chain-of-Blocks campaign runner. `cargo xtask sim run paros-chain`
//! drives the coverage-guided + frontier-exploration gate, then prints one legacy
//! visualization timeline for the browser contract.

use paros_sim::{
    AssertKind, CORRUPTION_COVERAGE_ITERATIONS, COVERAGE_ITERATIONS, NETWORK_COVERAGE_ITERATIONS,
    Outcome, PROTOCOL_BOUNDS_COVERAGE_ITERATIONS, SNAPSHOT_RECOVERY_COVERAGE_ITERATIONS,
    SimulationReport, explore, explore_corruption, explore_network_safety, explore_protocol_bounds,
    explore_snapshot_recovery, run_seed, run_seed_json,
};

fn main() {
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

    // 1. The DST bug-finding sweep: many seeds of swarm chaos, asserting safety.
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
    run_network_axis();
    run_corruption_axis();

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

/// One fixed three-node lifecycle sequence, kept separate from both broad
/// operation chaos and the network-only safety axis.
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

/// Stage-7 corruption detection (issue #20): detect ⇒ crash permanently downs
/// a corrupted node, so this axis asserts safety + detection totality only —
/// unavailable = pass, unsafe = fail — plus saturation of the per-family and
/// per-verdict gates.
fn run_corruption_axis() {
    println!("\n--- Chain corruption-detection safety axis ---");
    let corruption = explore_corruption(CORRUPTION_COVERAGE_ITERATIONS);
    println!(
        "{} seeds: {} ok, {} failed; convergence_timeout={}",
        corruption.iterations,
        corruption.successful_runs,
        corruption.failed_runs,
        corruption.convergence_timeout,
    );
    if let Some(s) = &corruption.saturation {
        println!(
            "  signal {:?}: {}/{} reachability fired, plateau {}",
            s.signal, s.sometimes_hit, s.sometimes_total, s.plateau_seeds,
        );
    }
    if !corruption.coverage_violations.is_empty() {
        println!("  coverage gates that never fired:");
        for gate in &corruption.coverage_violations {
            println!("    - {gate}");
        }
    }
    if !corruption.assertion_violations.is_empty()
        || corruption.failed_runs > 0
        || !corruption.coverage_violations.is_empty()
        || corruption.convergence_timeout
    {
        println!("  SAFETY VIOLATIONS: {:?}", corruption.assertion_violations);
        println!(
            "  COVERAGE VIOLATIONS: {:?}",
            corruption.coverage_violations
        );
        println!("  FAILING SEEDS: {:?}", corruption.seeds_failing);
        std::process::exit(1);
    }
    println!("  Corruption detection, classification, and fail-stop gates are green");
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

/// Network faults persist past Moonpool's cutoff in the pinned revision, so
/// this is a safety axis rather than a false quiet-tail liveness claim.
fn run_network_axis() {
    println!("\n--- Chain network-swarm safety axis ---");
    let network = explore_network_safety(NETWORK_COVERAGE_ITERATIONS);
    println!(
        "{} seeds: {} ok, {} failed; convergence_timeout={}",
        network.iterations,
        network.successful_runs,
        network.failed_runs,
        network.convergence_timeout,
    );
    if let Some(s) = &network.saturation {
        println!(
            "  signal {:?}: {}/{} reachability fired, plateau {}",
            s.signal, s.sometimes_hit, s.sometimes_total, s.plateau_seeds,
        );
    }
    if !network.coverage_violations.is_empty() {
        println!("  coverage gates that never fired:");
        for gate in &network.coverage_violations {
            println!("    - {gate}");
        }
    }
    if !network.assertion_violations.is_empty()
        || network.failed_runs > 0
        || !network.coverage_violations.is_empty()
        || network.convergence_timeout
    {
        println!("  SAFETY VIOLATIONS: {:?}", network.assertion_violations);
        println!("  COVERAGE VIOLATIONS: {:?}", network.coverage_violations);
        println!("  FAILING SEEDS: {:?}", network.seeds_failing);
        std::process::exit(1);
    }
    println!("  Network-swarm Chain safety gate is green");
}
