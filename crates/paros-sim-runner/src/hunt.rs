//! Red-seed hunt driver: raw seed volume through one campaign axis, reporting
//! every assertion violation and its seed. Unlike `sim-paros-chain` (the CI
//! saturation gate), a hunt never stops at a coverage plateau and treats
//! coverage gates as irrelevant — its only deliverable is failing seeds.
//!
//! Usage: `sim-paros-hunt [network|main|snapshot|bounds|amnesia|budget-off] [iterations]`
//!        `sim-paros-hunt replay-network <seed>` — deterministic single-seed
//!        replay on the network axis (the red→green witness command)
//!        `sim-paros-hunt replay-main <seed>` — same on the main campaign
//!        `sim-paros-hunt replay-snapshot <seed>` — lifecycle choreography
//!        `sim-paros-hunt replay-bounds <seed>` — protocol-bounds choreography
//!        `sim-paros-hunt replay-amnesia <seed>` — the naive-wipe **red demo**
//!        (issue #19 D): RED is the expected, correct result — the cross-restart
//!        promise audit catching a wiped node's reneged promise.
//!        `sim-paros-hunt replay-truncate <seed>` — the truncate-on-mismatch
//!        **red demo** (issue #20 F): RED is the expected, correct result — the
//!        recovered-vs-persisted divergence leg catching the silent record loss
//!        of a node that truncated on a corruption verdict instead of crashing.
//!        `sim-paros-hunt budget-off [iterations]` — the WAITED-leg axis
//!        (issue #21): corruption may take every copy of a committed item, and
//!        safety must still hold while the cluster correctly waits.
//!        `sim-paros-hunt replay-budget-off <seed>` — deterministic replay.
//!        `sim-paros-hunt faulty-none [iterations]` — the faulty-as-none **red
//!        demo** (issue #21, CTRL §5.1.1 mutation a): RED is the expected
//!        result — a rotted copy misreported as "nothing accepted here" lets a
//!        unanimous-looking quorum no-op fill a chosen slot.
//!        `sim-paros-hunt replay-faulty-none <seed>` — deterministic replay.

use paros_sim::{
    amnesia_demo_hunt, budget_off_hunt, chain_smoke, explore_chain_seed, explore_snapshot_recovery,
    explore_snapshot_recovery_seed, faulty_none_demo_hunt, network_hunt, protocol_bounds_hunt,
    run_amnesia_demo_seed, run_budget_off_seed, run_chain_seed, run_faulty_none_demo_seed,
    run_network_seed, run_protocol_bounds_seed, run_snapshot_recovery_seed, run_truncate_demo_seed,
    truncate_demo_hunt,
};

fn main() {
    let axis = std::env::args().nth(1).unwrap_or_else(|| "network".into());

    if let "replay-network" | "replay-main" | "replay-snapshot" | "replay-bounds"
    | "replay-amnesia" | "replay-truncate" | "replay-budget-off" | "replay-faulty-none"
    | "explore-snapshot" | "explore-main" = axis.as_str()
    {
        let seed = std::env::args()
            .nth(2)
            .and_then(|s| s.parse::<u64>().ok())
            .expect("replay needs a seed");
        println!("--- replay: {axis} seed {seed} ---");
        let report = match axis.as_str() {
            "replay-network" => run_network_seed(seed),
            "replay-snapshot" => run_snapshot_recovery_seed(seed),
            // Root + explored continuation timelines: the replay command for
            // choreography failures that live only on explorer branches.
            "explore-snapshot" => explore_snapshot_recovery_seed(seed, 8),
            // Root + explored continuation timelines on the main campaign —
            // the replay command for failures found on explorer branches.
            "explore-main" => explore_chain_seed(seed, 8),
            "replay-bounds" => run_protocol_bounds_seed(seed),
            "replay-amnesia" => run_amnesia_demo_seed(seed),
            "replay-truncate" => run_truncate_demo_seed(seed),
            "replay-budget-off" => run_budget_off_seed(seed),
            "replay-faulty-none" => run_faulty_none_demo_seed(seed),
            _ => run_chain_seed(seed),
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
        "snapshot" => explore_snapshot_recovery(iterations),
        "bounds" => protocol_bounds_hunt(iterations),
        "budget-off" => budget_off_hunt(iterations),
        // Red demo axes: violations here are the deliverable, not a defect.
        "amnesia" => amnesia_demo_hunt(iterations),
        "truncate" => truncate_demo_hunt(iterations),
        "faulty-none" => faulty_none_demo_hunt(iterations),
        other => {
            eprintln!(
                "unknown axis: {other} (expected 'network', 'main', 'snapshot', 'bounds', 'budget-off', 'amnesia', 'truncate', or 'faulty-none')"
            );
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
