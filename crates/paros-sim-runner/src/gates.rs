//! The declared-gate check (#446 (a gate that never fires cannot fail the
//! sweep)).
//!
//! moonpool judges only the gates that have an assertion slot, and a gate
//! gets its slot the first time it runs. A change that makes a path
//! unreachable removes its gates from the judgement: the sweep then reports
//! "saturated" over a smaller set. [`DECLARED`] is every gate the source
//! declares (the build script's scan, `src/scan.rs`); [`judge`] names each one
//! the sweep never ran. A gate the sweep is known to miss is listed in
//! [`RARE`] with its reason, and does not fail the sweep.

use std::collections::BTreeMap;

use paros_sim::AssertionStats;

include!(concat!(env!("OUT_DIR"), "/declared_gates.rs"));

/// moonpool keeps a slot's message in a 64-byte buffer, NUL-terminated
/// (`moonpool-assertions`' `SLOT_MSG_LEN`): the report names a slot by its
/// message's first 63 bytes, cut on a character boundary.
const SLOT_MSG_BYTES: usize = 63;

/// The name the report gives the slot of `msg`.
#[must_use]
pub fn slot_name(msg: &str) -> &str {
    &msg[..msg.floor_char_boundary(SLOT_MSG_BYTES)]
}

/// A declared gate the sweep is known to miss, and why. Each entry is a
/// debt: the fix is a BUGGIFY or a per-seed scenario that makes the gate
/// fire in the sweep (root AGENTS.md, *Amplify the worst states*).
pub struct Rare {
    /// The gate's message, exactly as declared.
    pub msg: &'static str,
    /// Why the sweep misses it, with its issue.
    pub reason: &'static str,
}

/// The declared gates the sweep is known to miss.
pub const RARE: &[Rare] = &[];

/// What the sweep ran of the declared gates.
#[derive(Debug, Default)]
pub struct Verdict {
    /// Declared gates with no slot, not listed rare: `(message, site)`. Each
    /// fails the sweep.
    pub missing: Vec<(&'static str, &'static str)>,
    /// Rare gates with no slot, as expected: `(message, reason)`.
    pub rare_missing: Vec<(&'static str, &'static str)>,
    /// Rare gates that fired anyway: candidates to delist.
    pub rare_fired: Vec<&'static str>,
}

/// Judge the sweep's slots against [`DECLARED`] and [`RARE`].
#[must_use]
pub fn judge(slots: &BTreeMap<String, AssertionStats>) -> Verdict {
    let rare: BTreeMap<&str, &str> = RARE.iter().map(|r| (r.msg, r.reason)).collect();
    let mut verdict = Verdict::default();
    for &(msg, site) in DECLARED {
        let slot = slots.get(slot_name(msg));
        match (slot, rare.get(msg)) {
            (None, None) => verdict.missing.push((msg, site)),
            (None, Some(&reason)) => verdict.rare_missing.push((msg, reason)),
            (Some(stats), Some(_)) if stats.successes > 0 => verdict.rare_fired.push(msg),
            (Some(_), _) => {}
        }
    }
    verdict
}

/// Print the verdict, each line prefixed by `indent`.
pub fn print(verdict: &Verdict, indent: &str) {
    println!(
        "{indent}declared gates: {} in the source, {} never ran, {} known rare and missed",
        DECLARED.len(),
        verdict.missing.len(),
        verdict.rare_missing.len(),
    );
    for (msg, reason) in &verdict.rare_missing {
        println!("{indent}known rare, missed: {msg:?}: {reason}");
    }
    if !verdict.missing.is_empty() {
        println!("{indent}declared gates that never ran (no slot):");
        for (msg, site) in &verdict.missing {
            println!("{indent}  - {msg:?} ({site})");
        }
    }
    for msg in &verdict.rare_fired {
        println!("{indent}known-rare gate fired, delist it from gates::RARE: {msg:?}");
    }
}

/// The `n` declared gates that fired least often, rarest first: the
/// marginal gates, the next candidates for a BUGGIFY or a scenario.
pub fn print_rarest(slots: &BTreeMap<String, AssertionStats>, n: usize) {
    let mut fired: Vec<(&str, &AssertionStats)> = DECLARED
        .iter()
        .filter_map(|&(msg, _)| slots.get(slot_name(msg)).map(|stats| (msg, stats)))
        .filter(|(_, stats)| stats.successes > 0)
        .collect();
    fired.sort_by_key(|(msg, stats)| (stats.successes, *msg));
    println!("rarest declared gates that fired:");
    for (msg, stats) in fired.into_iter().take(n) {
        println!(
            "  - {msg}: {} of {} checks",
            stats.successes, stats.total_checks
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{DECLARED, RARE};
    use std::collections::BTreeSet;

    #[test]
    fn the_scan_finds_the_gates() {
        // The campaign declares hundreds of gates; a scan that finds a
        // handful is broken, not a quiet source.
        assert!(
            DECLARED.len() > 500,
            "only {} gates declared",
            DECLARED.len()
        );
        let msgs: BTreeSet<&str> = DECLARED.iter().map(|(msg, _)| *msg).collect();
        // One of each form: a core probe, a library reachable, an audit
        // `reach_once!` and a workload sometimes.
        for gate in [
            "grid: a leader tears one slot's column and resigns",
            "the driver voluntarily resigns leadership",
        ] {
            assert!(msgs.contains(gate), "{gate:?} not declared");
        }
    }

    #[test]
    fn the_build_scan_matches_a_fresh_one() {
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut fresh = BTreeSet::new();
        for name in ["paros-core", "paros", "paros-sim"] {
            let src = crates.join(name).join("src");
            for gate in crate::scan::scan_root(&src, name) {
                assert!(gate.site.starts_with(name), "{} at {}", gate.msg, gate.site);
                fresh.insert(gate.msg);
            }
        }
        let built: BTreeSet<String> = DECLARED.iter().map(|(m, _)| (*m).to_owned()).collect();
        assert_eq!(built, fresh);
    }

    #[test]
    fn no_two_gates_share_a_slot_name() {
        // The report keys its slots by their first 63 bytes: two gates that
        // share them are counted as one there, and the sweep cannot tell
        // which of them fired. Keep the distinguishing words early.
        let mut names = std::collections::BTreeMap::new();
        for &(msg, site) in DECLARED {
            if let Some(other) = names.insert(super::slot_name(msg), site) {
                panic!("{msg:?} ({site}) and the gate at {other} share a slot name");
            }
        }
    }

    #[test]
    fn every_rare_gate_is_declared_once_with_a_reason() {
        let msgs: BTreeSet<&str> = DECLARED.iter().map(|(msg, _)| *msg).collect();
        let mut seen = BTreeSet::new();
        for rare in RARE {
            assert!(
                msgs.contains(rare.msg),
                "rare gate {:?} is not declared",
                rare.msg
            );
            assert!(
                seen.insert(rare.msg),
                "rare gate {:?} listed twice",
                rare.msg
            );
            assert!(
                !rare.reason.is_empty(),
                "rare gate {:?} has no reason",
                rare.msg
            );
        }
    }
}
