//! The named BUGGIFY locations a simulation harness may force per seed
//! (#318 E).
//!
//! Every driver choice is an inline BUGGIFY site at the line that makes it
//! (#294, #318). Most sites are keyed by their `file:line`, so moonpool draws
//! their activation alone. The locations here are named instead
//! (`moonpool_buggify::buggify_named!`): each is one ingredient of a per-seed
//! scenario, and the harness turns it on together with the scenario's other
//! ingredients with `moonpool_buggify::set_activation(label, active)`, so the
//! scenario does not hang on the product of independent coins. A harness
//! that does not decide a location leaves it to its own activation coin.
//!
//! Each location is a disruptive site: inert in production, and silent in
//! the simulation's recovery tail, so the run converges after its chaos
//! window. Never reword a label: the harness names the location by it.

/// The node withholds every garbage-collection request (#123) it would
/// send: the first send of a request and its re-sends alike. Always safe:
/// GC is optional work, and a floor that never becomes effective costs only
/// the retirements it would have licensed. The simulation couples it to the
/// departed-straggler scenario, which needs a prior configuration to stay
/// answerable (#124, #263). Recovery gate: "storage: a departed straggler's
/// slot is recovered through the prior configuration".
pub const WITHHOLD_GC: &str = "paros: a node withholds its garbage-collection requests";

/// The node holds one of its journals (#188): the journal's beat is skipped
/// and the peer messages that arrive for it are dropped, so the journal is a
/// slow, partitioned journal, and its siblings on the same node must not
/// notice (the non-interference claim). The held journal is the highest
/// user journal of a node that boots serving several. Always safe: a slow
/// node and a lossy network are both within the model. Recovery gate: "journal:
/// a journal keeps committing while a sibling on its nodes is held".
pub const HOLD_JOURNAL: &str = "paros: a node holds one of its journals";

/// The node drops a write's verdict (#204) at its own rate, on top of the
/// reply seam's per-kind drop. A dropped verdict makes the client's retry
/// meet the write already in the log: the idempotent `Duplicate` path. The
/// simulation couples it to the workload's lost-verdict scenario, which
/// re-sends an ambiguous write at once. Always safe: a client-facing RPC
/// response can be lost in production at any time. Recovery gate: the
/// client's retry takes the dedup path.
pub const LOSE_VERDICTS: &str = "paros: a node loses write verdicts";

/// The node opens its control-journal follow reads late (#189): on most
/// ticks it opens none, so its registry fold lags the registry's commits.
/// A node registered at runtime then speaks to members whose fold has not
/// admitted it yet, and they refuse it until the fold catches up. Always
/// safe: a slow follower is within the model, and a refused peer message is
/// re-sent. The simulation couples it to the lagging-fold scenario.
/// Recovery gate: "system: a message from a not-yet-folded node is refused
/// and later accepted".
pub const LAG_FOLLOW: &str = "paros: a node follows its control journals late";

/// The node admits the registry's pool late (#387): a node that draws it
/// at boot folds the registry as usual, but its journals admit a node the
/// registry registered (`ColocatedNode::extend_pool`) only
/// [`ADMIT_LATE_FOR`] after the fold owed it. A leader that admitted a joiner then reconfigures onto
/// it while this node's pool still lacks it, so a `Prepare`, a beat or a
/// matchmaker's answer names a configuration outside its pool, which every
/// learning path must ignore whole. Always safe: an admission is a fold
/// applied late, and a slow follower is within the model. The simulation
/// couples it to the lagging-fold scenario. Recovery gates: "matchmaking: a
/// membership probe meets a configuration outside the pool", "learning: a
/// configuration outside the pool is ignored".
pub const ADMIT_LATE: &str = "paros: a node admits the registry's pool late";

/// The proxy leader stalls (#341): it drops the `Accepted`s and `Nack`s it
/// hears, so none of its rounds closes. Its leader then takes each delegated round back
/// on its own budget, the proxy evicts each round on its retention budget,
/// and a leadership change meets the rounds the proxy still holds. Always
/// safe: a proxy decides nothing, and a lost vote is within the model. The
/// simulation couples it to the stalled-proxy scenario, which also runs an
/// acceptor grid where the pool tiles one. Recovery gates: "proxy: a leader
/// takes a delegated round back on a grid column", "proxy: a proxy evicts a
/// round nobody answers".
pub const STALL_PROXY: &str = "paros: a proxy leader stalls";

/// The leader resigns while it holds delegated rounds (#341), at its own
/// rate per beat, and campaigns again at once. Its next leadership's first
/// delegation then meets the rounds a proxy still holds for the old one,
/// which the proxy must close. Always safe: `step_down` is, and any node
/// may campaign at any time, at any fresh round. The simulation couples it to the stalled-proxy
/// scenario, whose proxies hold every round open. Recovery gate: "proxy: a
/// higher-ballot delegation meets a superseded leadership's rounds".
pub const RESIGN_DELEGATING: &str = "paros: a leader resigns while it holds delegated rounds";

/// A transferable leadership hands itself off on about a third of its beats
/// in the chaos window (#484 (silent driver fault sites)), on top of the
/// driver's own per-shape rates (0.2 to 3 %). A handoff then runs while the
/// network, attrition and the other sites still strike: a handoff is lost at
/// the send seam, and leadership moves several times in one run. Always
/// safe: a failed handoff costs availability, never safety. The simulation
/// couples it to the handoff-storm scenario. Recovery gate: "leadership is
/// handed over more than once in a run".
pub const HAND_OFF: &str = "paros: a leader hands off in a handoff storm";

/// The cell coordinator's term resigns once in the middle of a move, so the
/// next term finds the move where this one left it and finishes it (#215
/// (zones), #498 (moves in the sweep)). Always safe: a resignation costs
/// only the time of an election, and a move is resumed from the cell
/// control journal. The simulation couples it to the drained-machine
/// scenario, whose moves are few: off it, the term's own coin decides.
/// Recovery gate: "coordinator: a successor term finished a move".
pub const QUIT_MID_MOVE: &str = "paros: a coordinator term resigns mid-move";

/// The node's batch that re-accepts a slot at or below its chosen index (a
/// gap fill's re-accept or a repair at the decided ballot) meets its
/// `"batch staged, not synced"` hint at a high rate (#788 (torn-repair
/// scenario)), so the seed's attrition regime may tear the re-accept and
/// then the repair above the clean copies left. Always safe: a crash
/// between staging and syncing is a power loss, which the store and the
/// core survive. The simulation couples it to the torn-repair scenario.
/// Recovery gate: "storage: a repair at the decided ballot is torn after a
/// torn re-accept above it".
pub const TEAR_REPAIRS: &str = "paros: a node's repair batch meets its staged hint often";

/// The `"batch staged, not synced"` hint of a batch that re-accepts a slot
/// at or below the node's chosen index ([`TEAR_REPAIRS`]). Off the
/// scenario, the location's own activation coin decides; the driver keeps
/// its ordinary hint beside it.
#[must_use]
pub fn repair_staged() -> moonpool_buggify::hint::Hinted {
    moonpool_buggify::hint::at("batch staged, not synced", 0.5, TEAR_REPAIRS)
}

/// Whether this transferable leadership hands itself off on this beat
/// ([`HAND_OFF`]).
#[must_use]
pub fn hand_off() -> bool {
    let handed = moonpool_buggify::buggify_named!(HAND_OFF, 0.30);
    if handed {
        moonpool_assertions::reachable!("handoff: a handoff-storm seed hands leadership on");
    }
    handed
}

/// Whether this coordinator term resigns once in the middle of a move
/// ([`QUIT_MID_MOVE`]).
#[must_use]
pub fn quit_mid_move() -> bool {
    moonpool_buggify::buggify_named!(QUIT_MID_MOVE, 1.0)
}

/// Whether this leader, which holds delegated rounds, resigns on this beat
/// ([`RESIGN_DELEGATING`]).
#[must_use]
pub fn resign_delegating() -> bool {
    let resigned = moonpool_buggify::buggify_named!(RESIGN_DELEGATING, 0.05);
    if resigned {
        moonpool_assertions::reachable!("proxy: a leader resigns while it holds delegated rounds");
    }
    resigned
}

/// Whether this proxy drops the acceptor's answer it just heard
/// ([`STALL_PROXY`]).
#[must_use]
pub fn stall_proxy() -> bool {
    let stalled = moonpool_buggify::buggify_named!(STALL_PROXY, 1.0);
    if stalled {
        moonpool_assertions::reachable!("proxy: a stalled proxy drops an acceptor's answer");
    }
    stalled
}

/// Whether this node skips opening its follow reads on this tick
/// ([`LAG_FOLLOW`]).
#[must_use]
pub fn lag_follow() -> bool {
    let lagging = moonpool_buggify::buggify_named!(LAG_FOLLOW, 0.9);
    if lagging {
        moonpool_assertions::reachable!("system: a node follows its control journals late");
    }
    lagging
}

/// Whether this node, at boot, is one that admits the registry's pool
/// late ([`ADMIT_LATE`]): half the nodes of a scenario seed, so a leader
/// that admitted a joiner meets members that did not.
#[must_use]
pub fn late_admitter() -> bool {
    moonpool_buggify::buggify_named!(ADMIT_LATE, 0.5)
}

/// How long a late admitter ([`late_admitter`]) holds an admission it owes
/// ([`ADMIT_LATE`]): longer than a reconfiguration request, its matchmaking
/// and its first `Prepare`. A bound in time, not in ticks (a node whose
/// ticks stretch would hold far longer), and not a window: the hold also
/// runs in the recovery tail, so a joiner registered late is still met by a
/// member that lags, and every node admits at last.
pub(crate) const ADMIT_LATE_FOR: std::time::Duration = std::time::Duration::from_secs(2);

/// Whether a late admitter holds, at `now`, the admission it has owed since
/// `owed_since` ([`ADMIT_LATE`]).
///
/// # Panics
///
/// If an assertion on its own invariants fails: a programmer error, never
/// an operating condition.
#[must_use]
pub fn admit_held(late: bool, owed_since: std::time::Duration, now: std::time::Duration) -> bool {
    assert!(owed_since <= now, "an admission is owed from the past");
    let held = late && now.saturating_sub(owed_since) < ADMIT_LATE_FOR;
    if held {
        moonpool_assertions::reachable!("system: a node admits the registry's pool late");
    }
    held
}

/// Whether this node withholds its garbage-collection requests now
/// ([`WITHHOLD_GC`]).
#[must_use]
pub fn withhold_gc() -> bool {
    let withheld = moonpool_buggify::buggify_named!(WITHHOLD_GC, 1.0);
    if withheld {
        moonpool_assertions::reachable!(
            "gc: a seed withholds its GC requests for the chaos window"
        );
    }
    withheld
}

/// The journal this node may hold ([`HOLD_JOURNAL`]): the highest of `user`,
/// the user journals it boots serving, when there are several. A node that
/// serves one user journal has no sibling to keep committing, so it holds
/// none.
///
/// # Panics
///
/// If an assertion on its own invariants fails: a programmer error, never
/// an operating condition.
pub fn hold_candidate<'a>(
    user: impl IntoIterator<Item = &'a paros_core::journal::JournalIdentifier>,
) -> Option<paros_core::journal::JournalIdentifier> {
    let mut user = user.into_iter();
    let first = *user.next()?;
    let mut count = 1_usize;
    let mut highest = first;
    for journal in user {
        count += 1;
        highest = highest.max(*journal);
    }
    assert!(
        highest >= first,
        "the candidate is the highest user journal"
    );
    (count > 1).then_some(highest)
}

/// Whether this node holds `journal` now ([`HOLD_JOURNAL`]): `candidate` is
/// [`hold_candidate`]'s answer at boot.
#[must_use]
pub fn hold_journal(
    candidate: Option<paros_core::journal::JournalIdentifier>,
    journal: paros_core::journal::JournalIdentifier,
) -> bool {
    if candidate != Some(journal) {
        return false;
    }
    let held = moonpool_buggify::buggify_named!(HOLD_JOURNAL, 1.0);
    if held {
        moonpool_assertions::reachable!(
            "journal: one journal is held on every node for the chaos window"
        );
    }
    held
}

/// Whether this node loses the verdict of a write it is about to answer
/// ([`LOSE_VERDICTS`]).
#[must_use]
pub fn lose_verdict() -> bool {
    let lost = moonpool_buggify::buggify_named!(LOSE_VERDICTS, 0.10);
    if lost {
        moonpool_assertions::reachable!("client: a write's verdict is lost on a lost-verdict seed");
    }
    lost
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
    use std::time::Duration;

    fn id(journal: u64) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(1), JournalId(journal))
    }

    #[test]
    fn a_node_with_one_user_journal_holds_none() {
        assert_eq!(hold_candidate(&[]), None);
        assert_eq!(hold_candidate(&[id(3)]), None);
    }

    #[test]
    fn the_candidate_is_the_highest_user_journal() {
        assert_eq!(hold_candidate(&[id(3), id(9), id(5)]), Some(id(9)));
    }

    #[test]
    fn the_locations_are_inert_outside_a_simulation() {
        assert!(!withhold_gc());
        assert!(!hold_journal(Some(id(9)), id(9)));
        assert!(!lose_verdict());
        assert!(!stall_proxy());
        assert!(!resign_delegating());
        assert!(!late_admitter());
        assert!(!hand_off());
        assert!(!lag_follow());
        assert!(!quit_mid_move());
        assert_eq!(
            repair_staged().strike(),
            moonpool_buggify::hint::Strike::None
        );
    }

    /// With its location on, only the candidate journal is held: another
    /// journal, or a node with no candidate, is never held.
    #[test]
    fn an_active_hold_holds_only_the_candidate_journal() {
        moonpool_buggify::set_random_source(|| 0.0);
        moonpool_buggify::buggify_init(0.0);
        moonpool_buggify::set_activation(HOLD_JOURNAL, true);
        let held = hold_journal(Some(id(9)), id(9));
        let other = hold_journal(Some(id(9)), id(8));
        let none = hold_journal(None, id(9));
        moonpool_buggify::buggify_reset();
        moonpool_buggify::clear_random_source();
        assert!(held, "the candidate is held");
        assert!(!other, "another journal is not held");
        assert!(!none, "no candidate holds nothing");
    }

    #[test]
    fn a_late_admitter_holds_an_owed_admission_until_its_bound() {
        let owed = Duration::from_secs(10);
        assert!(admit_held(true, owed, owed));
        assert!(admit_held(
            true,
            owed,
            owed + ADMIT_LATE_FOR.saturating_sub(Duration::from_millis(1))
        ));
        assert!(!admit_held(true, owed, owed + ADMIT_LATE_FOR));
        assert!(!admit_held(true, owed, owed + ADMIT_LATE_FOR * 3));
    }

    #[test]
    fn a_node_that_is_not_late_never_holds() {
        let owed = Duration::from_secs(10);
        assert!(!admit_held(false, owed, owed));
        assert!(!admit_held(false, owed, owed + Duration::from_millis(1)));
    }

    #[test]
    #[should_panic(expected = "an admission is owed from the past")]
    fn an_admission_owed_from_the_future_is_a_programmer_error() {
        let _ = admit_held(true, Duration::from_secs(2), Duration::from_secs(1));
    }
}
