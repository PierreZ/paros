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
//!   (`moonpool_sim::chaos_ended_at`). Each time is a numeric assertion
//!   whose watermark is the slowest run ([`settled`]), and `sim-paros-hunt`
//!   prints the table (`SETTLE_PREFIX`). The bound of each is the most time
//!   any check in the tail may take ([`SETTLE_CEILING`]); a tighter bound
//!   waits for a measurement over many seeds.

use std::time::Duration;

use moonpool_sim::{
    SimContext, TimeProvider, assert_always_less_than_or_equal_to, assert_always_or_unreachable,
};

/// The prefix of every time-to-settle slot: the runner reads the slots back
/// by it.
pub const SETTLE_PREFIX: &str = "tail settle: ";

/// The most one step of the tail may take: the longest window a check
/// gives itself (`HANDOVER_CEILING` past a check's own deadline, or the
/// controls' window, plus a drain's move). An oracle threshold, **never
/// buggified**.
pub(crate) const SETTLE_CEILING: Duration = Duration::from_mins(3);

/// The most the whole tail may take from the end of chaos. An oracle
/// threshold, **never buggified**: every step of the tail has its own
/// deadline, so this is the sum a stuck control plane would still pass.
pub(crate) const TAIL_CEILING: Duration = Duration::from_mins(15);

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
    /// The slot of the step's time to settle. Never reworded: the slot is
    /// the message.
    fn slot(self) -> &'static str {
        match self {
            Step::Fleet => "tail settle: fleet",
            Step::Election => "tail settle: election",
            Step::Renamed => "tail settle: renamed",
            Step::Placements => "tail settle: placements",
            Step::Sets => "tail settle: sets",
            Step::Controls => "tail settle: controls",
            Step::Heal => "tail settle: heal",
            Step::Leads => "tail settle: leads",
            Step::Live => "tail settle: live",
            Step::Resolve => "tail settle: resolve",
            Step::Converge => "tail settle: converge",
            Step::Registry => "tail settle: registry",
            Step::Chain => "tail settle: chain",
            Step::Whole => "tail settle: whole tail",
        }
    }

    /// The step's bound.
    fn ceiling(self) -> Duration {
        match self {
            Step::Whole => TAIL_CEILING,
            _ => SETTLE_CEILING,
        }
    }
}

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
    let ceiling_ms = u64::try_from(step.ceiling().as_millis()).unwrap_or(u64::MAX);
    assert_always_less_than_or_equal_to!(took_ms, ceiling_ms, step.slot());
}

/// Every step, in the order of the report's table.
pub const STEPS: [&str; 14] = [
    "tail settle: fleet",
    "tail settle: election",
    "tail settle: renamed",
    "tail settle: placements",
    "tail settle: sets",
    "tail settle: controls",
    "tail settle: heal",
    "tail settle: leads",
    "tail settle: live",
    "tail settle: resolve",
    "tail settle: converge",
    "tail settle: registry",
    "tail settle: chain",
    "tail settle: whole tail",
];

#[cfg(test)]
mod tests {
    use super::{SETTLE_PREFIX, STEPS, Step};

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

    #[test]
    fn every_step_has_its_own_slot_in_the_table() {
        for (step, slot) in ALL.iter().zip(STEPS) {
            assert_eq!(step.slot(), slot);
            assert!(slot.starts_with(SETTLE_PREFIX));
        }
        let mut sorted = STEPS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), STEPS.len());
    }

    #[test]
    fn the_whole_tail_bound_covers_every_step() {
        for step in ALL {
            assert!(step.ceiling() <= Step::Whole.ceiling());
        }
    }
}
