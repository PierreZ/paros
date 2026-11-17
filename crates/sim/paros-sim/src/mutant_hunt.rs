//! How the mutation hunt and the negative hunt judge a mutant (#269, #742
//! (mutant per excuse), step 1.5 of the rework, #1094 (injected liveness
//! bugs)).
//!
//! A mutant is a known bug that the hunt turns on. The hunt runs the fixed
//! seeds `1..=N` of the main campaign one at a time, and each seed gives a
//! set of *reds*: the message of each always-assertion it violated, or
//! [`FAILED_RUN`] when the run failed with no violation (a panic of a
//! `paros-core` `assert!`, a stall). A red is the mutant's when the
//! unmutated hunt (the *baseline*) did not have the same red on the same
//! seed. Main is red on a share of the seeds while the causes of phase 1
//! are open (`docs/architecture/06-verification.md`, rule 8), so a red that
//! the baseline also has says nothing about the mutant.
//!
//! A mutant may also name its *causes*: the oracles that must catch it.
//! Then only a red of a named cause catches it, so a mutant that only moves
//! the draws onto another open cause is not counted as caught by it. This
//! is how the negative hunt checks that an oracle catches the bug it is
//! there for, as `FoundationDB`'s `tests/negative` do and as Paxos Made
//! Live measured its tests "by leaving known bugs in and confirming
//! detection" (`docs/references/papers/paxos-made-live`).

use std::collections::BTreeSet;

use moonpool_sim::SimulationReport;

/// The red of a seed whose run failed with no always-assertion violated.
pub const FAILED_RUN: &str = "failed run";

/// The reds of a report of one seed: the message of each violated
/// always-assertion, and [`FAILED_RUN`] when the run failed with none.
///
/// # Panics
///
/// Panics if the report is not of exactly one seed.
#[must_use]
pub fn reds(report: &SimulationReport) -> BTreeSet<String> {
    assert_eq!(report.seeds_used.len(), 1, "a report of one seed");
    let mut reds: BTreeSet<String> = report
        .assertion_violations
        .iter()
        .map(|violation| message(violation).to_string())
        .collect();
    if reds.is_empty() && (report.failed_runs > 0 || !report.seeds_failing.is_empty()) {
        reds.insert(FAILED_RUN.to_string());
    }
    assert!(
        !reds.is_empty() || (report.failed_runs == 0 && report.seeds_failing.is_empty()),
        "a failed seed has a red"
    );
    reds
}

/// The message of one violation line of a report,
/// `assert_always!('<message>') failed n times out of m`: the text between
/// the first `('` and the last `')`, or the whole line in another format.
fn message(violation: &str) -> &str {
    match (violation.find("('"), violation.rfind("')")) {
        (Some(start), Some(end)) if start + 2 <= end => &violation[start + 2..end],
        _ => violation,
    }
}

/// One line of a baseline file: `<seed>\t<red>`.
///
/// # Panics
///
/// Panics if `red` holds a line break.
#[must_use]
pub fn line(seed: u64, red: &str) -> String {
    assert!(!red.contains('\n'), "a red is one line");
    format!("{seed}\t{red}")
}

/// The reds of a baseline file, by seed (the lines of [`line`]). A line
/// that is not a seed and a red is ignored.
#[must_use]
pub fn baseline(text: &str) -> BTreeSet<(u64, String)> {
    text.lines()
        .filter_map(|l| {
            let (seed, red) = l.split_once('\t')?;
            Some((seed.parse().ok()?, red.to_string()))
        })
        .collect()
}

/// Whether `red` is of one of `causes`; every red is when `causes` is
/// empty. A report cuts a long message and ends it with `…`, so a cause
/// matches each red that is a cut of it, and each red that it is a cut of.
#[must_use]
pub fn of_a_cause(red: &str, causes: &[String]) -> bool {
    if causes.is_empty() {
        return true;
    }
    let red = red.trim_end_matches('…');
    causes.iter().any(|cause| {
        let cause = cause.trim_end_matches('…');
        !cause.is_empty() && (cause.starts_with(red) || red.starts_with(cause))
    })
}

#[cfg(test)]
mod tests {
    use super::{FAILED_RUN, baseline, line, message, of_a_cause};

    #[test]
    fn a_violation_line_gives_its_message() {
        assert_eq!(
            message("assert_always!('fleet: a sent init forms a cell') failed 1 times out of 9"),
            "fleet: a sent init forms a cell"
        );
        assert_eq!(
            message("assert_always!('the cell's journal (a)') failed 1 times out of 9"),
            "the cell's journal (a)"
        );
        assert_eq!(message("something else"), "something else");
    }

    #[test]
    fn a_baseline_reads_back_its_lines() {
        let text = [line(3, "a red"), line(12, FAILED_RUN), "junk".to_string()].join("\n");
        let reds = baseline(&text);
        assert_eq!(reds.len(), 2);
        assert!(reds.contains(&(3, "a red".to_string())));
        assert!(reds.contains(&(12, FAILED_RUN.to_string())));
        assert!(!reds.contains(&(12, "a red".to_string())));
    }

    #[test]
    fn a_cause_matches_its_cut_and_nothing_else() {
        let causes = vec!["heal: the cell answers an admin view within the bound".to_string()];
        assert!(of_a_cause(
            "heal: the cell answers an admin view within the bound",
            &causes
        ));
        assert!(of_a_cause("heal: the cell answers an admin vi…", &causes));
        assert!(!of_a_cause("fleet: a sent init forms a cell", &causes));
        assert!(!of_a_cause(FAILED_RUN, &causes));
        assert!(of_a_cause("anything", &[]));
    }
}
