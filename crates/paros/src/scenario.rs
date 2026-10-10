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

/// Whether this leader, which holds delegated rounds, resigns on this beat
/// ([`RESIGN_DELEGATING`]).
pub(crate) fn resign_delegating() -> bool {
    let resigned = moonpool_buggify::buggify_named!(RESIGN_DELEGATING, 0.05);
    if resigned {
        moonpool_assertions::reachable!("proxy: a leader resigns while it holds delegated rounds");
    }
    resigned
}

/// Whether this proxy drops the acceptor's answer it just heard
/// ([`STALL_PROXY`]).
pub(crate) fn stall_proxy() -> bool {
    let stalled = moonpool_buggify::buggify_named!(STALL_PROXY, 1.0);
    if stalled {
        moonpool_assertions::reachable!("proxy: a stalled proxy drops an acceptor's answer");
    }
    stalled
}

/// Whether this node skips opening its follow reads on this tick
/// ([`LAG_FOLLOW`]).
pub(crate) fn lag_follow() -> bool {
    let lagging = moonpool_buggify::buggify_named!(LAG_FOLLOW, 0.9);
    if lagging {
        moonpool_assertions::reachable!("system: a node follows its control journals late");
    }
    lagging
}

/// Whether this node withholds its garbage-collection requests now
/// ([`WITHHOLD_GC`]).
pub(crate) fn withhold_gc() -> bool {
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
pub(crate) fn hold_candidate<'a>(
    user: impl IntoIterator<Item = &'a paros_core::JournalIdentifier>,
) -> Option<paros_core::JournalIdentifier> {
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
pub(crate) fn hold_journal(
    candidate: Option<paros_core::JournalIdentifier>,
    journal: paros_core::JournalIdentifier,
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
pub(crate) fn lose_verdict() -> bool {
    let lost = moonpool_buggify::buggify_named!(LOSE_VERDICTS, 0.10);
    if lost {
        moonpool_assertions::reachable!("client: a write's verdict is lost on a lost-verdict seed");
    }
    lost
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::{JournalId, JournalIdentifier, TenantId};

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
    }
}
