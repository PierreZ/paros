//! The kill policy (step 1.9 of the rework, #1098 (`can_kill` policy),
//! decisions 5 and 16): moonpool asks it before each kill and wipe it runs
//! (`moonpool_sim::KillPolicy`, `FoundationDB`'s `canKillProcesses`,
//! `sim2.actor.cpp`, release 7.3.43), and it refuses first each fault the
//! cell does not promise to survive.
//!
//! A machine lost for good is one more failure a set must survive: a wipe
//! (a wiped machine never rejoins), and a crash held down with no
//! scheduled restart (rule 8: a machine down at the cutoff stays down). A
//! set of `2F+1` members survives `F` such losses. The policy refuses the
//! loss of a machine that would leave a set with fewer members never lost
//! than its quorum:
//!
//! - a plan a vote names: a majority of its members never wiped and never
//!   renamed (`crate::machine::wipe_loses_a_plan`);
//! - a plan journal, or a special tenant's matchmaker set, in force or
//!   still in some leader's `H_b` (`crate::machine::plan_quorum_holders_beside`);
//! - any tenant's matchmaker set in force or proposed: a majority;
//! - any journal the audit knows: a Phase-1 quorum of some configuration
//!   it may still run under (`crate::audit::wipe_strands_a_journal`).
//!
//! A refused wipe keeps its disk (decision 5: a wipe that would take a
//! whole set is a reboot, at every size), and a refused hold-down restarts
//! the machine in place. A crash with a scheduled restart is a transient
//! fault: the policy lets it run, and the cell must heal after it.
//!
//! This is a reading of decision 16 and of `docs/architecture/05` §5: a
//! transient kill is not a loss the survive number counts. A fault on a
//! process that is no machine of the run's cell runs as drawn: the
//! harness's acceptors have their own wipe budgets (`crate::world`).

use std::collections::BTreeSet;

use moonpool_sim::{KillAsk, KillPolicy, KillVerdict, StateHandle, assert_reachable};
use paros::core::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};

/// Publish the policy in the run's state. Idempotent: every process start
/// publishes the same policy again.
pub(crate) fn publish(state: &StateHandle) {
    KillPolicy::new(judge).publish(state);
}

/// The policy's answer to `ask` (see the module doc). Draws nothing.
fn judge(state: &StateHandle, ask: &KillAsk) -> KillVerdict {
    if !ask.wipes() && !ask.held {
        return KillVerdict::Allow;
    }
    let Some(victim) = crate::machine::minted_at(state, ask.victim) else {
        return KillVerdict::Allow;
    };
    // The machines lost for good: every wipe the board recognized, and the
    // machine at each wiped address the simulator has not seen heal (its
    // boot may not have run yet).
    let mut lost = crate::machine::wiped_ids(state);
    lost.extend(
        ask.wiped
            .iter()
            .filter_map(|ip| crate::machine::minted_at(state, *ip)),
    );
    if !takes_a_quorum(state, victim, &lost) {
        return KillVerdict::Allow;
    }
    if ask.wipes() {
        assert_reachable!("kill policy: a wipe that takes a quorum keeps its disk");
        KillVerdict::KeepDisk
    } else {
        assert_reachable!("kill policy: a hold-down that takes a quorum restarts in place");
        KillVerdict::Restart
    }
}

/// Whether losing `victim` for good, beside the machines `lost`, leaves a
/// plan, a plan journal, a matchmaker set or a journal with no quorum of
/// members never lost, where `lost` alone did not.
pub(crate) fn takes_a_quorum(state: &StateHandle, victim: u64, lost: &BTreeSet<u64>) -> bool {
    if lost.contains(&victim) {
        return false;
    }
    let mut with = lost.clone();
    with.insert(victim);
    assert!(with.len() == lost.len() + 1, "the victim is one more loss");
    // The audit's worlds are read before the board is locked.
    crate::audit::wipe_strands_a_journal(state, &with, lost)
        || sets_lose_a_majority(state, &with, lost)
        || crate::machine::plan_quorum_holders_beside(state, lost).contains(&victim)
        || crate::machine::wipe_loses_a_plan(state, &with, lost)
}

/// Whether some tenant's matchmaker set in force, or one a handover
/// proposes, keeps a majority outside `without` and not outside `with`.
fn sets_lose_a_majority(
    state: &StateHandle,
    with: &BTreeSet<u64>,
    without: &BTreeSet<u64>,
) -> bool {
    crate::audit::audited_tenants(state)
        .into_iter()
        .flat_map(|tenant| {
            crate::audit::set_in_force(state, tenant)
                .into_iter()
                .chain(crate::audit::sets_proposed(state, tenant))
        })
        .any(|members| !majority_outside(&members, with) && majority_outside(&members, without))
}

/// Whether the set of `members` holds a majority outside `lost`. Every
/// quorum question crosses `membership/`: the set judges its own majority.
fn majority_outside(members: &[u64], lost: &BTreeSet<u64>) -> bool {
    if members.is_empty() {
        return true;
    }
    let set = MatchmakerSet::new(
        MatchmakerGeneration(0),
        members.iter().copied().map(MatchmakerId).collect(),
    );
    let live: BTreeSet<MatchmakerId> = members
        .iter()
        .filter(|member| !lost.contains(member))
        .copied()
        .map(MatchmakerId)
        .collect();
    set.has_quorum(&live)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::majority_outside;

    #[test]
    fn a_set_of_three_survives_one_loss_and_not_two() {
        let set = [1, 2, 3];
        assert!(majority_outside(&set, &BTreeSet::new()));
        assert!(majority_outside(&set, &BTreeSet::from([2])));
        assert!(!majority_outside(&set, &BTreeSet::from([2, 3])));
    }

    #[test]
    fn a_set_of_five_survives_two_losses_and_not_three() {
        let set = [1, 2, 3, 4, 5];
        assert!(majority_outside(&set, &BTreeSet::from([1, 5])));
        assert!(!majority_outside(&set, &BTreeSet::from([1, 3, 5])));
    }

    #[test]
    fn a_loss_outside_the_set_changes_nothing() {
        assert!(majority_outside(&[1], &BTreeSet::from([2, 3])));
        assert!(!majority_outside(&[1], &BTreeSet::from([1])));
    }
}
