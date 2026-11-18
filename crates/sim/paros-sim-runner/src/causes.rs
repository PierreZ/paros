//! The heal scorecard of a run (rule 8, step 1.4 of the rework, #1093 (heal
//! scorecard and open causes)): what [`paros_sim::open_causes::judge`] found,
//! printed and written for the red record (`scripts/sim-red-record.sh`).

use std::collections::BTreeSet;
use std::fmt::Write as _;

use paros_sim::open_causes::{OpenCause, Verdict};

/// Print the heal scorecard of `verdict` over `seeds` root seeds: each open
/// cause with its phase and the seeds whose own run had it, then each cause
/// that blocks. Each line is prefixed by `indent`.
pub fn print_scorecard(verdict: &Verdict, causes: &[OpenCause], seeds: usize, indent: &str) {
    println!(
        "{indent}heal scorecard: {} open causes fired over {seeds} seeds, {} blocking",
        verdict.open.len(),
        verdict.blocking.len()
    );
    let mut open: Vec<(&String, &BTreeSet<u64>)> = verdict.open.iter().collect();
    open.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(b.0)));
    for (cause, hit) in open {
        let phase = causes
            .iter()
            .find(|c| &c.message == cause)
            .map_or(0, |c| c.phase);
        println!(
            "{indent}  open (phase {phase}) {} seeds: {cause}",
            hit.len()
        );
    }
    for (cause, hit) in &verdict.blocking {
        let shown: Vec<String> = hit.iter().take(3).map(u64::to_string).collect();
        println!(
            "{indent}  BLOCKING {} seeds: {cause} (seeds {})",
            hit.len(),
            shown.join(" ")
        );
    }
}

/// Write `verdict` to `path` for the red record (`scripts/sim-red-record.sh`):
/// one line per cause, `<cause>\t<seeds>`, the open causes first, then a
/// line `seeds run\t<count>`.
pub fn write_record(path: &std::path::Path, verdict: &Verdict, seeds: usize) {
    let mut out = String::new();
    for (cause, hit) in verdict.open.iter().chain(&verdict.blocking) {
        let shown: Vec<String> = hit.iter().map(u64::to_string).collect();
        writeln!(
            out,
            "{}\t{}",
            cause.replace(['\t', '\n'], " "),
            shown.join(" ")
        )
        .expect("writing to a String");
    }
    writeln!(out, "{SEEDS_RUN}\t{seeds}").expect("writing to a String");
    if let Err(error) = std::fs::write(path, out) {
        println!("  cannot write the record to {}: {error}", path.display());
    }
}

/// The environment variable that names where a run writes its record
/// ([`write_record`]).
pub const RECORD_ENV: &str = "PAROS_SIM_RED";

/// The key of the record's line that counts the seeds a run ran.
pub const SEEDS_RUN: &str = "seeds run";

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;

    use paros_sim::open_causes::{FIRST_PHASE, LAST_PHASE, open_causes};

    use crate::gates::SCANNED;
    use crate::scan;

    /// Every `assert_always!` and `assert_always_or_unreachable!` message of
    /// the scanned crates: an always-assertion a seed can violate.
    fn always_messages() -> BTreeSet<String> {
        let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        SCANNED
            .iter()
            .flat_map(|name| {
                scan::scan_root_for(
                    &crates.join(name).join("src"),
                    name,
                    &["assert_always", "assert_always_or_unreachable"],
                )
            })
            .map(|gate| gate.msg)
            .collect()
    }

    #[test]
    fn the_list_parses_and_names_declared_always_assertions() {
        let always = always_messages();
        assert!(!always.is_empty(), "the scan reads the oracles");
        for cause in open_causes() {
            assert!(
                always.contains(&cause.message),
                "{:?} is not an always-assertion of the simulation",
                cause.message
            );
            assert!((FIRST_PHASE..=LAST_PHASE).contains(&cause.phase));
        }
    }
}
