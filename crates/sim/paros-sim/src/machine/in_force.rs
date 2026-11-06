//! The members in force of the cell's plan journals (#685 (no founders)):
//! what the scenarios that strike a member aim at, and what a wiped
//! machine's heal waits on. After `init`, no rule of paros asks whether a
//! machine is an initial member, so neither do these: a member in force is
//! a machine that the configuration in force of a plan journal (the cell
//! control, election and fleet journals) names, or that the matchmaker set
//! in force of a plan tenant names.
//!
//! Read from the audit's worlds: call these before the board is locked.

use std::collections::BTreeSet;

use moonpool_sim::StateHandle;
use paros::core::journal::{JournalIdentifier, TenantId};

use super::{lock, machine_board};

/// The plan journals of every plan a vote on a disk names, and the members
/// and matchmakers of those plans: the first configuration.
fn plans(state: &StateHandle) -> (Vec<JournalIdentifier>, BTreeSet<u64>) {
    let board = machine_board(state);
    let board = lock(&board);
    let mut journals = Vec::new();
    let mut first = BTreeSet::new();
    for plan in board.voters.values() {
        let control = plan.control_journals();
        journals.extend(
            std::iter::once(control.cell)
                .chain(control.election)
                .chain(control.fleet),
        );
        first.extend(plan.members.iter().map(|(id, _)| id.0));
    }
    journals.sort_unstable();
    journals.dedup();
    (journals, first)
}

/// The machines (minted ids) the configuration in force of a plan journal
/// names. `None` before a vote names a plan, or while a plan journal has no
/// configuration the audit knows: a caller that aims at a member waits.
pub(crate) fn plan_members(state: &StateHandle) -> Option<BTreeSet<u64>> {
    let (journals, _) = plans(state);
    if journals.is_empty() {
        return None;
    }
    let mut members = BTreeSet::new();
    for journal in journals {
        let config = crate::audit::config_in_force(state, journal)?;
        members.extend(config.members().iter().map(|m| m.0));
    }
    assert!(!members.is_empty(), "a plan journal has members in force");
    Some(members)
}

/// The machines (minted ids) the cell still counts on outside its
/// bookings: every member in force of a plan journal and of a plan
/// tenant's matchmaker set. A plan journal or a set the audit does not
/// know yet counts the plan's first configuration, so a heal never runs
/// ahead of what the cell knows.
pub(crate) fn held(state: &StateHandle) -> BTreeSet<u64> {
    let (journals, first) = plans(state);
    let mut held = BTreeSet::new();
    let tenants: BTreeSet<TenantId> = journals.iter().map(|j| j.tenant).collect();
    for journal in journals {
        match crate::audit::config_in_force(state, journal) {
            Some(config) => held.extend(config.members().iter().map(|m| m.0)),
            None => held.extend(first.iter().copied()),
        }
    }
    for tenant in tenants {
        match crate::audit::set_in_force(state, tenant) {
            Some(set) => held.extend(set),
            None => held.extend(first.iter().copied()),
        }
    }
    assert!(
        first.is_empty() || !held.is_empty(),
        "a cell with a plan counts on some machine"
    );
    held
}

/// Where the board knows each of `members` now: its advertised address,
/// in address order, and only once it formed or the cell admitted it.
/// `None` while one of them has no such address (a wiped old id, a machine
/// that is not in the cell yet).
pub(crate) fn joined_addresses(
    board: &super::MachineBoard,
    members: &BTreeSet<u64>,
) -> Option<Vec<paros::model::address::Address>> {
    let addrs: Vec<paros::model::address::Address> = board
        .nodes
        .iter()
        .filter(|(_, node)| members.contains(node))
        .filter(|(_, node)| board.formed.contains(node) || board.admitted.contains_key(node))
        .map(|(addr, _)| addr.clone())
        .collect();
    assert!(
        addrs.len() <= members.len(),
        "a member stands at one address"
    );
    (addrs.len() == members.len()).then_some(addrs)
}
