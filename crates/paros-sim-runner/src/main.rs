//! Native smoke runner: `cargo run -p paros-sim-runner [seed]` first drives the
//! `UntilCoverageStable` safety sweep (swarm network chaos + the three Paxos
//! safety invariants), then runs one seed and prints its message timeline. The
//! browser calls the exact same [`paros_sim::run_seed`] under the hood.

use paros_sim::{COVERAGE_ITERATIONS, Outcome, explore, run_seed, run_seed_json};

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
    println!("--- safety sweep (UntilCoverageStable, swarm network chaos) ---");
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
    // Name the `sometimes`/`reachable` gates that never fired. Saturation is the
    // sweep's real exit criterion, and "did not saturate" is useless without
    // knowing *which* gate is starving — that is the knob to tune.
    if !report.coverage_violations.is_empty() {
        println!("  coverage gates that never fired:");
        for gate in &report.coverage_violations {
            println!("    - {gate}");
        }
    }
    if report.assertion_violations.is_empty() && report.failed_runs == 0 {
        println!("  no safety violations — single-decree Paxos chose at most one value");
    } else {
        println!("  SAFETY VIOLATIONS: {:?}", report.assertion_violations);
        println!(
            "  FAILING SEEDS (replay with run_seed): {:?}",
            report.seeds_failing
        );
        // This runner is the coverage-guided sweep gate (`cargo xtask sim`): a
        // safety violation or a failed run must fail the process so CI catches it.
        std::process::exit(1);
    }

    // 2. A single seed, with its full message timeline for eyeballing.
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
