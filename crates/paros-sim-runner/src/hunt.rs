//! Red-seed hunt driver: raw seed volume through one campaign axis, reporting
//! every assertion violation and its seed. Unlike `sim-paros-chain` (the CI
//! saturation gate), a hunt never stops at a coverage plateau and treats
//! coverage gates as irrelevant — its only deliverable is failing seeds.
//!
//! Usage: `sim-paros-hunt [main|canary|corpus|corpus-chunks] [iterations]`
//!        `sim-paros-hunt replay-main <seed>` — deterministic single-seed
//!        replay on the main campaign (the red→green witness command).
//!        `sim-paros-hunt canary [iterations]` — the main campaign under
//!        moonpool's determinism canary: every seed runs twice and the replay
//!        must reproduce the first run's draw fingerprints, all of them. Its
//!        deliverable is an entropy leak, named by the first diverging draw.
//!        `sim-paros-hunt explore-main <seed>` — root + explored continuation
//!        timelines, for failures that live only on explorer branches.
//!        `sim-paros-hunt corpus [iterations]` — the E1 mask corpus: each seed
//!        draws a per-slot × per-node corruption mask and asserts its
//!        analytically derived outcome (Correct vs `CorrectlyUnavailable`).
//!        `sim-paros-hunt replay-corpus <seed>` — deterministic replay.
//!        `sim-paros-hunt replay-corpus-mask <mask>` — one explicit mask.
//!        `sim-paros-hunt corpus-chunks [iterations]` — the per-chunk mask
//!        corpus over the decided snapshot point; `replay-chunk-mask <mask>` /
//!        `replay-chunk-seed <seed>` replay one case, and
//!        `replay-chunk-restore-crash <mask>` the mask with node 0's live
//!        snapshot lost and a scripted crash after its point restore (#146).
//!        `sim-paros-hunt replay-bare-quorum <seed>` / `replay-lifecycle
//!        <seed>` — the bare-quorum lost-slot case and the §5.1.2
//!        snapshot-lifecycle compound; `replay-departed <seed>` — the
//!        departed-straggler case (#124).

mod common;

use common::{arg, is_clean, print_never_fired, print_seed_counts};
use paros_sim::{
    ChunkLiveCase, EXPLORATION_TIMELINES_PER_SEED, SimulationReport, chain_canary_hunt,
    chain_seed_canary, chain_smoke, chunk_corpus_hunt, corpus_hunt, explore_chain_seed,
    run_bare_quorum_case, run_chain_seed, run_chunk_corpus_seed, run_chunk_mask, run_corpus_mask,
    run_corpus_seed, run_departed_straggler_case, run_snapshot_lifecycle_case,
};

/// The chunk-corpus mask a replay argument names (its low 15 bits).
fn chunk_mask(seed: u64) -> u32 {
    u32::try_from(seed & 0x7FFF).unwrap_or_default()
}

/// The single-seed replay a `replay-*` / `explore-main` axis names, if any.
fn replay_for(axis: &str) -> Option<fn(u64) -> SimulationReport> {
    Some(match axis {
        "replay-main" => run_chain_seed,
        "replay-canary" => chain_seed_canary,
        "explore-main" => |seed| explore_chain_seed(seed, EXPLORATION_TIMELINES_PER_SEED),
        "replay-corpus" => run_corpus_seed,
        "replay-corpus-mask" => {
            |seed| run_corpus_mask(u16::try_from(seed % 512).unwrap_or_default())
        }
        "replay-bare-quorum" => run_bare_quorum_case,
        "replay-lifecycle" => run_snapshot_lifecycle_case,
        "replay-departed" => run_departed_straggler_case,
        "replay-chunk-mask" => |seed| run_chunk_mask(chunk_mask(seed), ChunkLiveCase::Intact),
        "replay-chunk-restore-crash" => {
            |seed| run_chunk_mask(chunk_mask(seed), ChunkLiveCase::LostThenRestoreCrash)
        }
        "replay-chunk-seed" => run_chunk_corpus_seed,
        _ => return None,
    })
}

fn main() {
    let axis = std::env::args().nth(1).unwrap_or_else(|| "main".into());

    if let Some(replay) = replay_for(&axis) {
        let seed: u64 = arg(2).expect("replay needs a seed");
        println!("--- replay: {axis} seed {seed} ---");
        let report = replay(seed);
        if is_clean(&report) {
            println!("seed {seed}: GREEN");
            return;
        }
        println!("seed {seed}: RED");
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
        "corpus" => corpus_hunt(iterations),
        "corpus-chunks" => chunk_corpus_hunt(iterations),
        other => {
            eprintln!(
                "unknown axis: {other} (expected 'main', 'canary', 'corpus', or 'corpus-chunks')"
            );
            std::process::exit(2);
        }
    };

    print_seed_counts(&report, "");
    // The assertion-slot budget (AGENTS.md, *Assertion doctrine*): 512
    // slots per campaign process, shared with moonpool's own internals.
    // Printed on every hunt so "count before adding" has a number to read,
    // and an overflow — evaluations dropped for want of a slot — is never
    // silent.
    println!(
        "assertion slots: {} used, {} evaluations dropped for want of a slot",
        report.assertion_results.len(),
        report.dropped_assertion_allocations,
    );
    // A hunt's deliverable is failing seeds, so coverage never decides its exit
    // status — but a gate that never fired across the whole hunt is exactly what
    // a starved `sometimes` looks like in the CI sweep, and finding it here is
    // far cheaper than re-running the full coverage campaign to see it.
    print_never_fired(&report, "");
    if is_clean(&report) {
        println!("no violations — the hunt came back empty");
        return;
    }
    println!("VIOLATIONS: {:#?}", report.assertion_violations);
    println!("FAILING SEEDS: {:?}", report.seeds_failing);
    std::process::exit(1);
}
