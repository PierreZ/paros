//! The recovery tail's verdicts (#913 (liveness checks with no verdict)).
//!
//! After chaos, `check()` must prove recovery. Two rules hold here:
//!
//! - **A stall is a verdict.** No code of this crate cancels the shutdown
//!   token. Under `chaos_until_run_ends`, moonpool cancels it only when a
//!   stall guard trips (the run-phase time budget or the no-progress
//!   detector), and a stall is a liveness failure. So a tail check that
//!   sees the token cancelled fails ([`breached`]); it never returns with
//!   no verdict.
//! - **The time to settle is measured.** Each final check reports how long
//!   it took from its start, and the whole tail reports how long it took
//!   from the end of chaos, which moonpool records one time
//!   (`moonpool_sim::chaos_ended_at`). The times go to a table of this
//!   process, not to assertion slots, which the campaign has few of: the
//!   slowest run and the run count per step ([`settle_table`]).
//!   `sim-paros-hunt` runs every seed in its own process and prints it; the
//!   sweep's forked workers each keep their own, which nobody prints. The
//!   table only observes: it draws nothing and decides nothing. A bound on
//!   the times waits for a measurement over many seeds.

use std::time::Duration;

use std::sync::{Mutex, PoisonError};

use moonpool_sim::{SimContext, TimeProvider, assert_always_or_unreachable};

/// One step of the tail whose time to settle is measured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// `FleetOps::final_fleet`: the directory and the tenants.
    Fleet,
    /// `election::election_settles`.
    Election,
    /// `FleetOps::renamed_registered`.
    Renamed,
    /// `placement::placements_booked`.
    Placements,
    /// `sets::sets_booked`.
    Sets,
    /// `controls::controls_booked`.
    Controls,
    /// `FleetOps::journals_heal`.
    Heal,
    /// `FleetOps::journals_lead`.
    Leads,
    /// `FleetOps::machines_live`.
    Live,
    /// `FleetOps::resolve_answers`.
    Resolve,
    /// `converge::worlds_converge`.
    Converge,
    /// `FleetOps::final_registry`.
    Registry,
    /// The chain client's convergence on its journal.
    Chain,
    /// The whole tail, from the end of chaos to the end of client 0's
    /// final checks.
    Whole,
}

impl Step {
    /// Every step, in the order of the report's table.
    const ALL: [Step; 14] = [
        Step::Fleet,
        Step::Election,
        Step::Renamed,
        Step::Placements,
        Step::Sets,
        Step::Controls,
        Step::Heal,
        Step::Leads,
        Step::Live,
        Step::Resolve,
        Step::Converge,
        Step::Registry,
        Step::Chain,
        Step::Whole,
    ];

    /// The step's name in the report.
    fn name(self) -> &'static str {
        match self {
            Step::Fleet => "fleet",
            Step::Election => "election",
            Step::Renamed => "renamed",
            Step::Placements => "placements",
            Step::Sets => "sets",
            Step::Controls => "controls",
            Step::Heal => "heal",
            Step::Leads => "leads",
            Step::Live => "live",
            Step::Resolve => "resolve",
            Step::Converge => "converge",
            Step::Registry => "registry",
            Step::Chain => "chain",
            Step::Whole => "whole tail",
        }
    }
}

/// Per step, in [`Step::ALL`] order: the runs that measured it and the
/// slowest time, in milliseconds.
static TABLE: Mutex<[(u64, u64); 14]> = Mutex::new([(0, 0); 14]);

/// Whether a stall breach cut the tail short (the module doc): the
/// shutdown token is cancelled. A breach is a violation, named by the
/// check it cut (`check`, a stable name).
pub(crate) fn breached(ctx: &SimContext, check: &'static str) -> bool {
    let cancelled = ctx.shutdown().is_cancelled();
    if cancelled {
        assert_always_or_unreachable!(
            false,
            "tail: a stall breach cuts a liveness check short",
            { "check" => check }
        );
    }
    cancelled
}

/// Record the time `step` took to settle, from `started` to now.
pub(crate) fn settled(ctx: &SimContext, step: Step, started: Duration) {
    let took = ctx.time().now().saturating_sub(started);
    record(step, took);
}

/// Record the whole tail's time to settle, from the end of chaos. A run
/// whose chaos never ended records nothing.
pub(crate) fn whole_settled(ctx: &SimContext) {
    if let Some(ended) = moonpool_sim::chaos_ended_at() {
        record(Step::Whole, ctx.time().now().saturating_sub(ended));
    }
}

fn record(step: Step, took: Duration) {
    let took_ms = u64::try_from(took.as_millis()).unwrap_or(u64::MAX);
    let index = Step::ALL.iter().position(|s| *s == step).unwrap_or(0);
    let mut table = TABLE.lock().unwrap_or_else(PoisonError::into_inner);
    let (runs, slowest) = &mut table[index];
    *runs += 1;
    *slowest = (*slowest).max(took_ms);
}

/// The time to settle of each step this process measured: its name, the
/// runs that measured it and the slowest, in milliseconds.
#[must_use]
pub fn settle_table() -> Vec<(&'static str, u64, u64)> {
    let table = *TABLE.lock().unwrap_or_else(PoisonError::into_inner);
    Step::ALL
        .iter()
        .zip(table)
        .map(|(step, (runs, slowest))| (step.name(), runs, slowest))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Step, record, settle_table};

    #[test]
    fn every_step_has_its_own_name() {
        let mut names: Vec<_> = Step::ALL.iter().map(|s| s.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), Step::ALL.len());
    }

    #[test]
    fn the_table_keeps_the_slowest_run_and_counts_runs() {
        let before = settle_table();
        let heal =
            |t: &[(&'static str, u64, u64)]| t.iter().find(|r| r.0 == "heal").copied().unwrap();
        let (_, runs, _) = heal(&before);
        record(Step::Heal, Duration::from_millis(7));
        record(Step::Heal, Duration::from_hours(1));
        record(Step::Heal, Duration::from_millis(3));
        let (_, after_runs, slowest) = heal(&settle_table());
        assert_eq!(after_runs, runs + 3);
        assert_eq!(slowest, 3_600_000);
    }
}
