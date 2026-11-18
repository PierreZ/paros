//! The list of open causes and the judge of a report against it (rule 8,
//! `docs/architecture/06-verification.md`, step 1.4 of the rework, #1093
//! (heal scorecard and open causes)).
//!
//! A red seed has one or more *causes*: the message of each always-assertion
//! its run violated, or the failure of the run itself (a panic, a stall, a
//! determinism canary that diverged). A liveness cause is *open* until the
//! gate of the phase that owns it: [`OPEN_CAUSES`] (the file
//! `crates/sim/paros-sim/open-causes.txt`) names each one with that phase. An open cause is
//! counted on the heal scorecard and does not fail a run. Every other cause
//! *blocks*: a safety cause (never on the list), a closed cause (deleted from
//! the list at its phase's gate) and a cause that the list does not name.
//! The list only gets shorter; phase 7 ends when it is empty and deleted.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::SimulationReport;

/// The list of open causes (the file `open-causes.txt` of this crate), one
/// per line: `<phase>\t<message>`. A line that
/// starts with `#` and a blank line are comments.
pub const OPEN_CAUSES: &str = include_str!("../open-causes.txt");

/// The first phase that may own an open cause. Phase 1 is the oracle phase:
/// its gate is this list, so no cause stays open for it.
pub const FIRST_PHASE: u8 = 2;

/// The last phase of the rework: its gate deletes the list.
pub const LAST_PHASE: u8 = 7;

const _: () = assert!(FIRST_PHASE <= LAST_PHASE);

/// How moonpool names a seed's always-violations in its failure cause
/// (`moonpool_sim`'s report, `failure_causes`): every other text of a cause
/// is a failed run.
const VIOLATED: &str = "an always assertion was violated";

/// How moonpool names a seed whose root run passed and one of whose explored
/// timelines failed: the timeline's violations are in the report's
/// `assertion_violations` only, and the judge reads them there.
const EXPLORED: &str = "exploration found a failing timeline";

/// One open cause: the message of a liveness assertion, and the phase whose
/// gate closes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCause {
    /// The assertion's message, exactly as declared.
    pub message: String,
    /// The phase whose gate closes the cause.
    pub phase: u8,
}

/// The open causes of `text`, in file order.
///
/// # Errors
///
/// The first line that is not `<phase>\t<message>` with a phase from
/// [`FIRST_PHASE`] to [`LAST_PHASE`], or a message listed twice.
pub fn parse(text: &str) -> Result<Vec<OpenCause>, String> {
    let mut causes: Vec<OpenCause> = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (phase, message) = line
            .split_once('\t')
            .ok_or_else(|| format!("line {}: not `<phase>\\t<message>`", number + 1))?;
        let phase: u8 = phase
            .trim()
            .parse()
            .map_err(|_| format!("line {}: the phase {phase:?} is not a number", number + 1))?;
        if !(FIRST_PHASE..=LAST_PHASE).contains(&phase) {
            return Err(format!(
                "line {}: phase {phase} is outside {FIRST_PHASE}..={LAST_PHASE}",
                number + 1
            ));
        }
        let message = message.trim();
        if message.is_empty() {
            return Err(format!("line {}: no message", number + 1));
        }
        if causes.iter().any(|cause| cause.message == message) {
            return Err(format!("line {}: {message:?} is listed twice", number + 1));
        }
        causes.push(OpenCause {
            message: message.to_owned(),
            phase,
        });
    }
    Ok(causes)
}

/// The open causes of [`OPEN_CAUSES`].
///
/// # Panics
///
/// Panics if the file does not parse; a unit test proves that it does.
#[must_use]
pub fn open_causes() -> Vec<OpenCause> {
    parse(OPEN_CAUSES).unwrap_or_else(|error| panic!("open-causes.txt: {error}"))
}

/// What a report shows against the list: each cause with the seeds whose
/// own run had it. A violation that only an exploration timeline had names
/// no root seed, so its set of seeds is empty.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Verdict {
    /// Each open cause that fired.
    pub open: BTreeMap<String, BTreeSet<u64>>,
    /// Each cause that blocks: a violated assertion not on the list, or a
    /// failed run (`failed run: <its first line>`).
    pub blocking: BTreeMap<String, BTreeSet<u64>>,
}

impl Verdict {
    /// Whether nothing blocks.
    #[must_use]
    pub fn passes(&self) -> bool {
        self.blocking.is_empty()
    }

    /// Add the red `message`, of `seed` when a root seed had it, to the
    /// open causes when `causes` names it and to the blocking ones when not.
    fn add(&mut self, message: &str, seed: Option<u64>, causes: &[OpenCause]) {
        let (key, map) = match open_cause(message, causes) {
            Some(cause) => (cause.message.clone(), &mut self.open),
            None => (message.to_owned(), &mut self.blocking),
        };
        // A report names a slot by its cut message: a cut of a cause a seed
        // already named is that cause.
        let key = match key.strip_suffix('…') {
            Some(prefix) if !prefix.is_empty() => map
                .keys()
                .find(|known| known.starts_with(prefix) && !known.ends_with('…'))
                .cloned()
                .unwrap_or(key),
            _ => key,
        };
        map.entry(key).or_default().extend(seed);
    }

    /// Add `other`'s causes and seeds to this verdict.
    pub fn merge(&mut self, other: Verdict) {
        for (cause, seeds) in other.open {
            self.open.entry(cause).or_default().extend(seeds);
        }
        for (cause, seeds) in other.blocking {
            self.blocking.entry(cause).or_default().extend(seeds);
        }
    }
}

/// The open cause that `message` names, if any. A report may name a slot by
/// its cut message, ended by `…`: a cut matches the cause it is a cut of.
fn open_cause<'a>(message: &str, causes: &'a [OpenCause]) -> Option<&'a OpenCause> {
    let cut = message.strip_suffix('…');
    causes.iter().find(|cause| match cut {
        Some(prefix) => !prefix.is_empty() && cause.message.starts_with(prefix),
        None => cause.message == message,
    })
}

/// Judge `report` against `causes`: each failing seed by the violations of
/// its own run and the failure of its run, and each violation of the report
/// (an exploration timeline's too) by the list.
#[must_use]
pub fn judge(report: &SimulationReport, causes: &[OpenCause]) -> Verdict {
    let mut verdict = Verdict::default();
    let failing: BTreeSet<u64> = report.seeds_failing.iter().copied().collect();
    for &seed in &failing {
        let violations = report.seed_violations.get(&seed);
        for message in violations.into_iter().flatten() {
            verdict.add(message, Some(seed), causes);
        }
        let cause = report
            .failure_causes
            .get(&seed)
            .map_or("no cause recorded", String::as_str);
        let explored = cause == EXPLORED && !report.assertion_violations.is_empty();
        let violated = cause.starts_with(VIOLATED) && violations.is_some_and(|v| !v.is_empty());
        if !explored && !violated {
            let first = cause.lines().next().unwrap_or_default();
            verdict
                .blocking
                .entry(format!("failed run: {first}"))
                .or_default()
                .insert(seed);
        }
    }
    for violation in &report.assertion_violations {
        verdict.add(violated_message(violation), None, causes);
    }
    if report.failed_runs > report.seeds_failing.len() {
        verdict
            .blocking
            .entry("failed run: a run failed with no failing seed".to_owned())
            .or_default();
    }
    verdict
}

/// The message a moonpool coverage or always violation names: every one
/// quotes its slot name as `('<message>')`. A line in another shape is kept
/// whole.
#[must_use]
pub fn violated_message(violation: &str) -> &str {
    violation
        .split_once("('")
        .and_then(|(_, rest)| rest.rsplit_once("')"))
        .map_or(violation, |(name, _)| name)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use moonpool_sim::SimulationReport;

    use super::{OpenCause, Verdict, judge, open_cause, open_causes, parse, violated_message};

    fn causes() -> Vec<OpenCause> {
        vec![OpenCause {
            message: "heal: the cell answers an admin view within the bound".to_owned(),
            phase: 5,
        }]
    }

    fn report(
        failing: &[(u64, &str, &[&str])],
        violations: &[&str],
        failed_runs: usize,
    ) -> SimulationReport {
        let mut report = SimulationReport {
            failed_runs,
            ..SimulationReport::default()
        };
        for &(seed, cause, messages) in failing {
            report.seeds_failing.push(seed);
            report.failure_causes.insert(seed, cause.to_owned());
            if !messages.is_empty() {
                report
                    .seed_violations
                    .insert(seed, messages.iter().map(|m| (*m).to_owned()).collect());
            }
        }
        report.assertion_violations = violations
            .iter()
            .map(|m| format!("assert_always!('{m}') failed 1 times out of 2"))
            .collect();
        report
    }

    #[test]
    fn the_list_parses() {
        let causes = open_causes();
        assert!(causes.iter().all(|cause| !cause.message.is_empty()));
    }

    #[test]
    fn a_bad_line_does_not_parse() {
        assert!(parse("# a comment\n\n3\ta cause\n").is_ok());
        assert!(parse("a cause\n").is_err(), "no phase");
        assert!(parse("x\ta cause\n").is_err(), "a phase that is no number");
        assert!(parse("1\ta cause\n").is_err(), "phase 1 owns no cause");
        assert!(parse("8\ta cause\n").is_err(), "no phase 8");
        assert!(parse("3\t\n").is_err(), "no message");
        assert!(parse("3\ta\n4\ta\n").is_err(), "a cause listed twice");
    }

    #[test]
    fn a_cut_message_matches_the_cause_it_cuts() {
        let causes = causes();
        assert!(
            open_cause(
                "heal: the cell answers an admin view within the bound",
                &causes
            )
            .is_some()
        );
        assert!(open_cause("heal: the cell answers an admin vi…", &causes).is_some());
        assert!(
            open_cause("heal: the cell answers", &causes).is_none(),
            "an uncut prefix is another message"
        );
        assert!(open_cause("…", &causes).is_none());
    }

    #[test]
    fn an_open_cause_alone_passes_and_is_counted() {
        let message = "heal: the cell answers an admin view within the bound";
        let cause = format!("an always assertion was violated: '{message}'");
        let verdict = judge(
            &report(&[(7, &cause, &[message])], &[message], 1),
            &causes(),
        );
        assert!(verdict.passes());
        assert_eq!(
            verdict.open,
            BTreeMap::from([(message.to_owned(), BTreeSet::from([7]))])
        );
    }

    #[test]
    fn a_cause_not_on_the_list_blocks() {
        let open = "heal: the cell answers an admin view within the bound";
        let safety = "chain: one value per slot";
        let cause = format!("an always assertion was violated: '{open}', '{safety}'");
        let verdict = judge(
            &report(&[(7, &cause, &[open, safety])], &[open, safety], 1),
            &causes(),
        );
        assert!(!verdict.passes());
        assert_eq!(verdict.blocking.get(safety), Some(&BTreeSet::from([7])));
        assert_eq!(verdict.open.get(open), Some(&BTreeSet::from([7])));
    }

    #[test]
    fn a_cut_violation_is_the_cause_a_seed_named() {
        let long = "fleet: a started init leaves the cell READY and joined after chaos";
        let cut = "fleet: a started init leaves the cell READY and joined after ch…";
        let cause = format!("an always assertion was violated: '{long}'");
        let verdict = judge(&report(&[(7, &cause, &[long])], &[cut], 1), &causes());
        assert_eq!(
            verdict.blocking,
            BTreeMap::from([(long.to_owned(), BTreeSet::from([7]))])
        );
    }

    #[test]
    fn a_violation_of_an_explored_timeline_is_judged_too() {
        let verdict = judge(&report(&[], &["chain: one value per slot"], 0), &causes());
        assert_eq!(
            verdict.blocking.get("chain: one value per slot"),
            Some(&BTreeSet::new())
        );
    }

    #[test]
    fn a_failed_run_blocks_even_beside_an_open_cause() {
        let message = "heal: the cell answers an admin view within the bound";
        let cause =
            format!("a stall guard stopped the run; an always assertion was violated: '{message}'");
        let verdict = judge(
            &report(&[(3, &cause, &[message])], &[message], 1),
            &causes(),
        );
        assert!(!verdict.passes());
        assert_eq!(
            verdict.blocking.get("failed run: a stall guard stopped the run; an always assertion was violated: 'heal: the cell answers an admin view within the bound'"),
            Some(&BTreeSet::from([3]))
        );
        let explored = judge(
            &report(
                &[(5, "exploration found a failing timeline", &[])],
                &[message],
                1,
            ),
            &causes(),
        );
        assert!(
            explored.passes(),
            "an explored timeline is judged by its violations"
        );
        assert_eq!(explored.open.get(message), Some(&BTreeSet::new()));
        let explored_bare = judge(
            &report(&[(5, "exploration found a failing timeline", &[])], &[], 1),
            &causes(),
        );
        assert!(
            !explored_bare.passes(),
            "an explored timeline that failed with no violation blocks"
        );
        let panicked = judge(
            &report(&[(4, "a workload task was lost", &[])], &[], 1),
            &causes(),
        );
        assert!(!panicked.passes());
    }

    #[test]
    fn verdicts_merge_their_seeds() {
        let mut one = Verdict::default();
        one.open.insert("a".to_owned(), BTreeSet::from([1]));
        let mut two = Verdict::default();
        two.open.insert("a".to_owned(), BTreeSet::from([2]));
        two.blocking.insert("b".to_owned(), BTreeSet::from([3]));
        one.merge(two);
        assert_eq!(one.open["a"], BTreeSet::from([1, 2]));
        assert!(!one.passes());
    }

    #[test]
    fn a_violation_names_its_message() {
        assert_eq!(
            violated_message(
                "assert_sometimes!('journal read: a wake') has 0% success rate (4 checks)"
            ),
            "journal read: a wake"
        );
        assert_eq!(
            violated_message("assert_reachable!('a crash's site') was never reached"),
            "a crash's site"
        );
        assert_eq!(
            violated_message("numeric assert_sometimes ('a gap') has 0% success rate (1 checks)"),
            "a gap"
        );
        assert_eq!(
            violated_message("assert_always!('chain: one verdict') failed 2 times out of 9"),
            "chain: one verdict"
        );
        assert_eq!(violated_message("something else"), "something else");
    }
}
