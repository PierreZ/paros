//! Red-seed hunt driver: raw seed volume through one campaign axis, reporting
//! every assertion violation and its seed. Unlike `sim-paros-chain` (the CI
//! saturation gate), a hunt never stops at a coverage plateau and treats
//! coverage gates as irrelevant — its only deliverable is failing seeds.
//!
//! Usage: `sim-paros-hunt [network|main] [iterations]`
//!        `sim-paros-hunt replay-network <seed>` — deterministic single-seed
//!        replay on the network axis (the red→green witness command)
//!        `sim-paros-hunt replay-main <seed>` — same on the main campaign

use paros_sim::{chain_smoke, network_hunt, run_chain_seed, run_network_seed};

fn main() {
    let axis = std::env::args().nth(1).unwrap_or_else(|| "network".into());

    if let "replay-network" | "replay-main" = axis.as_str() {
        let seed = std::env::args()
            .nth(2)
            .and_then(|s| s.parse::<u64>().ok())
            .expect("replay needs a seed");
        println!("--- replay: {axis} seed {seed} ---");
        let report = if axis == "replay-network" {
            run_network_seed(seed)
        } else {
            run_chain_seed(seed)
        };
        if report.assertion_violations.is_empty() && report.failed_runs == 0 {
            println!("seed {seed}: GREEN");
            return;
        }
        println!("seed {seed}: RED");
        println!("VIOLATIONS: {:#?}", report.assertion_violations);
        std::process::exit(1);
    }

    let iterations = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    println!("--- hunt: {axis} axis, {iterations} seeds ---");
    let report = match axis.as_str() {
        "network" => network_hunt(iterations),
        "main" => chain_smoke(iterations),
        other => {
            eprintln!("unknown axis: {other} (expected 'network' or 'main')");
            std::process::exit(2);
        }
    };

    println!(
        "{} seeds: {} ok, {} failed",
        report.iterations, report.successful_runs, report.failed_runs,
    );
    if report.assertion_violations.is_empty() && report.failed_runs == 0 {
        println!("no violations — the hunt came back empty");
        return;
    }
    println!("VIOLATIONS: {:#?}", report.assertion_violations);
    println!("FAILING SEEDS: {:?}", report.seeds_failing);
    std::process::exit(1);
}
