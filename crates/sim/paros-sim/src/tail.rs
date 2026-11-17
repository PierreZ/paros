//! The recovery tail of rule 8, one quiescence oracle
//! (`docs/architecture/06-verification.md`): chaos and operator actions
//! stop at one cutoff, and one predicate must hold within one bound.
//!
//! The **tail clock** ([`Clock`]) starts at moonpool's chaos cutoff
//! (`moonpool_sim::chaos_ended_at`), recorded once for the run when the
//! last client's `run()` returns. A check on the clock counts its deadline
//! and its time to heal from the cutoff, never from the moment the check
//! starts, so a check that runs late gets no new window.
//!
//! The **cell healthy predicate** ([`predicate`]) is a pure judgement over
//! what one pass reads: the ground truth of the simulator and one admin
//! view of the cell. The cell heals when [`PASSES`] passes in a row judge
//! it healthy, [`PASS_GAP`] apart, as `FoundationDB`'s quiet database waits
//! for three successful checks two seconds apart in simulation and starts
//! the count again after a failed one (`fdbserver/QuietDatabase.actor.cpp`,
//! `waitForQuietDatabase`, lines 925 to 979 of release 7.3.43).
//!
//! The bound is the workload's drawn `recovery_budget_ms` plus the longest
//! `machine_replace_after` a node drew (`Clock::of`): a cell waits that
//! long before it re-places a member of a machine that does not answer, so
//! a shorter bound judges a re-placement the cell promises not to start.
//! **Open**: the gate of phase 4 replaces it with one fixed recovery bound
//! (`docs/architecture/07-changes-milestones-done.md`, phase 4).
//!
//! No tail check skips (decisions 16 and 33). A check that sees the run's
//! token cancelled is a violation ([`cut_short`]): moonpool cancels it in
//! the tail only when its stall guard ends a run that stopped making
//! progress, never when a check ends. A cell or a matchmaker set that
//! wipes left with no quorum is the one verdict from ground truth
//! ([`lost`], [`judge_lost`]): a red of its own, and the checks of what it
//! lost do not judge again what the verdict already judged.

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{SimContext, StateHandle, assert_always};
use paros::core::journal::TenantId;
use paros::core::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};

pub(crate) mod predicate;

/// How many passes in a row must judge the cell healthy: `FoundationDB`'s
/// three. An oracle threshold, never buggified.
pub(crate) const PASSES: u32 = 3;

/// The pause between two healthy passes: `FoundationDB`'s two seconds in
/// simulation. An oracle threshold, never buggified.
pub(crate) const PASS_GAP: Duration = Duration::from_secs(2);

/// The pause after a pass that judged the cell unhealthy: `FoundationDB`'s
/// one second. An oracle threshold, never buggified.
pub(crate) const RETRY_GAP: Duration = Duration::from_secs(1);

/// The one tail clock: the chaos cutoff and the bound counted from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Clock {
    /// The instant chaos ended, as moonpool recorded it.
    cutoff: Duration,
    /// How long after the cutoff the predicate must hold.
    bound: Duration,
}

impl Clock {
    /// The clock of this run, with `bound` counted from the cutoff.
    /// `now` stands in for the cutoff when moonpool recorded none, which
    /// happens only when a check runs before every client's `run()`
    /// returned: never in the tail.
    pub(crate) fn new(cutoff: Option<Duration>, now: Duration, bound: Duration) -> Self {
        let cutoff = cutoff.unwrap_or(now);
        assert!(cutoff <= now, "the cutoff is not in the future");
        Self { cutoff, bound }
    }

    /// The clock of this run: the cutoff moonpool recorded, and a bound of
    /// `recovery_budget` plus the longest `machine_replace_after` a node of
    /// the run drew.
    pub(crate) fn of(
        state: &moonpool_sim::StateHandle,
        now: Duration,
        recovery_budget: Duration,
    ) -> Self {
        let replace_after = crate::shape::longest_replace_after(state);
        Self::new(
            moonpool_sim::chaos_ended_at(),
            now,
            recovery_budget.saturating_add(replace_after),
        )
    }

    /// The instant the predicate must hold by.
    pub(crate) fn deadline(self) -> Duration {
        self.cutoff.saturating_add(self.bound)
    }

    /// How long after the cutoff `now` is.
    pub(crate) fn since_cutoff(self, now: Duration) -> Duration {
        now.saturating_sub(self.cutoff)
    }
}

/// Whether the run's token is cancelled while a tail check runs, with
/// the violation asserted: a tail check is never cut short (rule 8).
/// `check` names the check in the detail map.
pub(crate) fn cut_short(ctx: &SimContext, check: &'static str) -> bool {
    let cut = ctx.shutdown().is_cancelled();
    assert_always!(
        !cut,
        "tail: no tail check is cut short",
        { "check" => check }
    );
    cut
}

/// What wipes took beyond its quorum, from the simulator's ground truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lost {
    /// A vote named a plan that then lost a majority of its members to
    /// wipes (`crate::machine::cell_lost`): no `cell init` can choose it,
    /// and the cell's majority journals cannot serve.
    Cell,
    /// A tenant's matchmaker set in force lost its majority to wipes: no
    /// journal of the tenant elects a leader again, because every campaign
    /// registers at a matchmaker quorum and a wiped matchmaker never
    /// rejoins.
    Set,
}

impl Lost {
    /// The label of the detail map.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cell => "cell",
            Self::Set => "set",
        }
    }
}

/// Whether the cell is lost to wipes. Sticky: a later wipe leaves the
/// cell no less lost.
pub(crate) fn cell_lost(state: &StateHandle) -> bool {
    crate::machine::cell_lost(state)
}

/// The tenants of `tenants` whose matchmaker set in force lost its
/// majority to the run's wipes.
pub(crate) fn lost_sets(state: &StateHandle, tenants: &BTreeSet<TenantId>) -> BTreeSet<TenantId> {
    let wiped = crate::machine::wiped_ids(state);
    if wiped.is_empty() {
        return BTreeSet::new();
    }
    let lost: BTreeSet<TenantId> = tenants
        .iter()
        .copied()
        .filter(|tenant| set_lost(state, *tenant, &wiped))
        .collect();
    assert!(
        lost.is_subset(tenants),
        "a lost set is one of the tenants asked"
    );
    lost
}

/// Whether `tenant`'s matchmaker set in force, as the audit bound it,
/// holds no majority outside `wiped`. `false` on plain Multi-Paxos (no
/// set), or before the audit bound one.
fn set_lost(state: &StateHandle, tenant: TenantId, wiped: &BTreeSet<u64>) -> bool {
    let Some(members) = crate::audit::set_in_force(state, tenant) else {
        return false;
    };
    // Every quorum question crosses `membership/`: the set judges its own
    // majority. The generation is not what is judged here.
    let set = MatchmakerSet::new(
        MatchmakerGeneration(0),
        members.iter().copied().map(MatchmakerId).collect(),
    );
    let live: BTreeSet<MatchmakerId> = members
        .iter()
        .filter(|m| !wiped.contains(m))
        .copied()
        .map(MatchmakerId)
        .collect();
    !set.has_quorum(&live)
}

/// What the run lost to wipes beyond its quorum, the cell first: the one
/// verdict from ground truth. `tenants` are the tenants whose sets count.
pub(crate) fn lost(state: &StateHandle, tenants: &BTreeSet<TenantId>) -> Option<Lost> {
    if cell_lost(state) {
        return Some(Lost::Cell);
    }
    (!lost_sets(state, tenants).is_empty()).then_some(Lost::Set)
}

/// The one verdict, asserted once per run at the end of the tail: no plan
/// and no matchmaker set is lost to wipes. **Open** until step 1.9: the
/// simulator then refuses a wipe that would take a quorum, and step 7.2
/// deletes the verdict.
pub(crate) fn judge_lost(state: &StateHandle, tenants: &BTreeSet<TenantId>) {
    let lost = lost(state, tenants);
    assert_always!(
        lost.is_none(),
        "verdict: no plan and no matchmaker set is lost to wipes",
        { "lost" => lost.map_or("none", Lost::as_str) }
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Clock, Lost, PASS_GAP, PASSES, RETRY_GAP};

    fn s(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    #[test]
    fn the_deadline_counts_from_the_cutoff_not_from_now() {
        let clock = Clock::new(Some(s(100)), s(130), s(60));
        assert_eq!(clock.deadline(), s(160));
        assert_eq!(clock.since_cutoff(s(130)), s(30));
        assert_eq!(clock.since_cutoff(s(90)), Duration::ZERO);
    }

    #[test]
    fn no_recorded_cutoff_counts_from_now() {
        let clock = Clock::new(None, s(42), s(10));
        assert_eq!(clock.deadline(), s(52));
        assert_eq!(clock.since_cutoff(s(42)), Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "the cutoff is not in the future")]
    fn a_cutoff_after_now_is_refused() {
        let _ = Clock::new(Some(s(10)), s(5), s(1));
    }

    #[test]
    fn the_passes_follow_foundationdb() {
        assert_eq!(PASSES, 3);
        assert_eq!(PASS_GAP, s(2));
        assert_eq!(RETRY_GAP, s(1));
    }

    #[test]
    fn the_lost_labels_are_distinct() {
        assert_ne!(Lost::Cell.as_str(), Lost::Set.as_str());
    }
}
