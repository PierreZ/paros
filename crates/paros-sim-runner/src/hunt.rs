//! Red-seed hunt driver: raw seed volume through one campaign axis, reporting
//! every assertion violation and its seed. Unlike `sim-paros-chain` (the CI
//! saturation gate), a hunt never stops at a coverage plateau and treats
//! coverage gates as irrelevant — its only deliverable is failing seeds.
//!
//! Usage: `sim-paros-hunt [main|corpus|corpus-chunks] [iterations]`
//!        `sim-paros-hunt replay-main <seed>` — deterministic single-seed
//!        replay on the main campaign (the red→green witness command).
//!        `sim-paros-hunt explore-main <seed>` — root + explored continuation
//!        timelines, for failures that live only on explorer branches.
//!        `sim-paros-hunt corpus [iterations]` — the E1 mask corpus: each seed
//!        draws a per-slot × per-node corruption mask and asserts its
//!        analytically derived outcome (Correct vs `CorrectlyUnavailable`).
//!        `sim-paros-hunt replay-corpus <seed>` — deterministic replay.
//!        `sim-paros-hunt replay-corpus-mask <mask>` — one explicit mask.
//!        `sim-paros-hunt corpus-chunks [iterations]` — the per-chunk mask
//!        corpus over the decided snapshot point; `replay-chunk-mask <mask>` /
//!        `replay-chunk-seed <seed>` replay one case.
//!        `sim-paros-hunt replay-bare-quorum <seed>` / `replay-lifecycle
//!        <seed>` — the bare-quorum lost-slot case and the §5.1.2
//!        snapshot-lifecycle compound; `replay-departed <seed>` — the
//!        departed-straggler case (#124).

use paros_sim::{
    chain_smoke, chunk_corpus_hunt, corpus_hunt, explore_chain_seed, run_bare_quorum_case,
    run_chain_seed, run_chunk_corpus_seed, run_chunk_mask, run_corpus_mask, run_corpus_seed,
    run_departed_straggler_case, run_snapshot_lifecycle_case,
};

fn main() {
    let axis = std::env::args().nth(1).unwrap_or_else(|| "main".into());

    if let "replay-main" | "explore-main" | "replay-corpus" | "replay-corpus-mask"
    | "replay-bare-quorum" | "replay-lifecycle" | "replay-departed" | "replay-chunk-mask"
    | "replay-chunk-seed" = axis.as_str()
    {
        let seed = std::env::args()
            .nth(2)
            .and_then(|s| s.parse::<u64>().ok())
            .expect("replay needs a seed");
        println!("--- replay: {axis} seed {seed} ---");
        let report = match axis.as_str() {
            "explore-main" => explore_chain_seed(seed, 8),
            "replay-corpus" => run_corpus_seed(seed),
            "replay-corpus-mask" => run_corpus_mask(u16::try_from(seed % 512).unwrap_or_default()),
            "replay-bare-quorum" => run_bare_quorum_case(seed),
            "replay-lifecycle" => run_snapshot_lifecycle_case(seed),
            "replay-departed" => run_departed_straggler_case(seed),
            "replay-chunk-mask" => {
                run_chunk_mask(u32::try_from(seed & 0x7FFF).unwrap_or_default(), false)
            }
            "replay-chunk-seed" => run_chunk_corpus_seed(seed),
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
        "main" => chain_smoke(iterations),
        "corpus" => corpus_hunt(iterations),
        "corpus-chunks" => chunk_corpus_hunt(iterations),
        other => {
            eprintln!("unknown axis: {other} (expected 'main', 'corpus', or 'corpus-chunks')");
            std::process::exit(2);
        }
    };

    println!(
        "{} seeds: {} ok, {} failed",
        report.iterations, report.successful_runs, report.failed_runs,
    );
    // A hunt's deliverable is failing seeds, so coverage never decides its exit
    // status — but a gate that never fired across the whole hunt is exactly what
    // a starved `sometimes` looks like in the CI sweep, and finding it here is
    // far cheaper than re-running the full coverage campaign to see it.
    if !report.coverage_violations.is_empty() {
        println!("coverage gates that never fired:");
        for gate in &report.coverage_violations {
            println!("  - {gate}");
        }
    }
    if report.assertion_violations.is_empty() && report.failed_runs == 0 {
        println!("no violations — the hunt came back empty");
        return;
    }
    println!("VIOLATIONS: {:#?}", report.assertion_violations);
    println!("FAILING SEEDS: {:?}", report.seeds_failing);
    std::process::exit(1);
}
