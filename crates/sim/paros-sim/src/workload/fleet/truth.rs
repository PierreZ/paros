//! The heal checks' excuses from the sim's ground truth (#739 (heal from
//! ground truth)), never from a reply of the system under test: a registry
//! bug that marks a live machine down, or a machine full, must not excuse
//! the journal the bug left unhealthy.
//!
//! The facts are the run's own: the machine board (each machine's process
//! starts and stops, its disk's minted id, the wipes, the drains, removals
//! and maintenance windows an operator sent, each machine's zone), the
//! audit's acceptor configuration in force on each journal, the desired
//! mode its tenant's create or configure recorded, the audit's matchmaker
//! generations, the layout's class and capacity of each machine
//! and the audit's booking model, folded from the registry's decided events
//! in position order.
//!
//! An operator's entry whose answer was lost may still land, so it counts
//! as landed: a drain, a removal, and a window, which then holds for the
//! rest of the run. A window the registry took holds no longer than its
//! length from the answer, or than the next entry over its scope the
//! registry took. In the settle tail the last operator ends every window
//! (#741 (end windows)): once the registry holds none, no window sent
//! before holds.

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{StateHandle, assert_reachable};
use paros::core::NodeId;
use paros::core::journal::{JournalIdentifier, TenantId};
use paros::core::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use paros::model::registry::MaintenanceScope;
use paros::model::status::Member;

use super::cell::Cell;
use crate::excuses::Excuse;
use crate::machine::{Landed, MachineBoard, lock, machine_board};

/// How machine `node` (a minted id) stands now: gone when an operator
/// asked to remove it, when no disk holds its id (a wipe took it), or when
/// its process is not running; leaving when an operator asked to drain it.
fn standing(board: &MachineBoard, node: u64) -> Member {
    if board.removed.contains(&node) || board.remove_asked.contains(&node) {
        return Member::Gone;
    }
    let Some(addr) = board
        .nodes
        .iter()
        .find(|(_, id)| **id == node)
        .map(|(addr, _)| addr)
    else {
        return Member::Gone;
    };
    let up = board
        .started
        .get(addr)
        .is_some_and(|started| board.ended.get(addr).is_none_or(|ended| ended < started));
    if !up {
        Member::Gone
    } else if board.drain_asked.contains(&node) {
        Member::Leaving
    } else {
        Member::Serving
    }
}

/// Whether `scope` covers machine `node`.
fn covers(board: &MachineBoard, scope: &MaintenanceScope, node: u64) -> bool {
    match scope {
        MaintenanceScope::Machine(id) => id.0 == node,
        MaintenanceScope::Zone(zone) => board
            .nodes
            .iter()
            .find(|(_, id)| **id == node)
            .and_then(|(addr, _)| board.zones.get(addr))
            .is_some_and(|held| held == zone),
    }
}

/// Whether a maintenance window may cover machine `node` at `now`.
fn under_window(board: &MachineBoard, node: u64, now: Duration) -> bool {
    board.windows.iter().enumerate().any(|(index, window)| {
        if window.length.is_zero() || window.sent > now || !covers(board, &window.scope, node) {
            return false;
        }
        if board
            .windows_ended
            .is_some_and(|ended| window.sent < ended && now >= ended)
        {
            return false;
        }
        match window.landed {
            Landed::Refused => false,
            Landed::Unknown => true,
            Landed::By(at) => {
                let cut = board.windows[index + 1..]
                    .iter()
                    .filter(|later| later.scope == window.scope)
                    .find_map(|later| match later.landed {
                        Landed::By(cut) => Some(cut),
                        Landed::Unknown | Landed::Refused => None,
                    });
                let end = at.saturating_add(window.length);
                now < cut.map_or(end, |cut| end.min(cut))
            }
        }
    })
}

/// Every scope a window may still hold over at `now`, as the operators'
/// entries say (#741 (end windows)): the last entry over the scope the
/// registry did not refuse opens a window that has not run out, or is an
/// end whose answer was lost. An end the registry refused names a machine
/// out of the pool, which holds no window.
pub(super) fn held_scopes(state: &StateHandle, now: Duration) -> Vec<MaintenanceScope> {
    let board = machine_board(state);
    let board = lock(&board);
    let mut held: Vec<MaintenanceScope> = Vec::new();
    for window in board.windows.iter().filter(|w| w.sent <= now) {
        if held.contains(&window.scope) {
            continue;
        }
        let last = board
            .windows
            .iter()
            .rev()
            .filter(|w| w.sent <= now && w.scope == window.scope)
            .find(|w| w.length.is_zero() || w.landed != Landed::Refused);
        let holds = last.is_some_and(|last| match last.landed {
            Landed::Unknown => true,
            Landed::By(at) => !last.length.is_zero() && now < at.saturating_add(last.length),
            Landed::Refused => false,
        });
        if holds {
            held.push(window.scope.clone());
        }
    }
    held
}

/// Whether the cell cannot hold `journal`'s shape at `now`, with the excuse
/// that holds: the members that serve hold no Phase-1 or no Phase-2 quorum
/// (no leader can re-place a member), a maintenance window may cover a
/// member, or fewer machines of the cell can take a slot than members fail
/// to serve. `None` when nothing excuses it, or when the audit knows no
/// configuration of it: it is judged.
pub(super) fn excuse(
    state: &StateHandle,
    cell: &Cell,
    journal: JournalIdentifier,
    now: Duration,
) -> Option<Excuse> {
    let (created, desired) = {
        let board = crate::audit::tenants::tenant_board(state);
        let board = crate::audit::tenants::lock(&board);
        (
            board.live_config(journal).cloned(),
            board.desired_acceptors(journal),
        )
    };
    let config = crate::audit::config_in_force(state, journal).or(created)?;
    let members: BTreeSet<u64> = config.members().iter().map(|m| m.0).collect();
    let (failing, candidates) = {
        let board = machine_board(state);
        let board = lock(&board);
        let alive: BTreeSet<NodeId> = members
            .iter()
            .copied()
            .filter(|id| standing(&board, *id) != Member::Gone)
            .map(NodeId)
            .collect();
        if !config.has_phase1_quorum(&alive) || !config.has_phase2_quorum(&alive) {
            return Some(Excuse::HealCannotHold);
        }
        if members.iter().any(|id| under_window(&board, *id, now)) {
            assert_reachable!("status: a maintenance window excuses a journal");
            return Some(Excuse::HealMaintenance);
        }
        let failing = members
            .iter()
            .filter(|id| standing(&board, **id) != Member::Serving)
            .count();
        // The cell's machines: its founding members still on their disks,
        // and the machines admitted into it.
        let ours = |id: u64| {
            cell.members.contains(&id)
                || board
                    .admitted
                    .get(&id)
                    .is_some_and(|joined| joined.cell_id == cell.journals.cell_id)
        };
        let candidates: BTreeSet<u64> = board
            .nodes
            .values()
            .copied()
            .filter(|id| {
                ours(*id)
                    && !members.contains(id)
                    && standing(&board, *id) == Member::Serving
                    && !under_window(&board, *id, now)
            })
            .collect();
        (failing, candidates)
    };
    let short = desired.unwrap_or(0).saturating_sub(members.len());
    if failing + short == 0 {
        return None;
    }
    let system = crate::audit::system::system_board(state);
    let system = crate::audit::system::lock(&system);
    let free = candidates
        .iter()
        .filter(|id| {
            system
                .free_slots(cell.journals.cell, **id)
                .is_some_and(|free| free > 0)
        })
        .count();
    (free < failing + short).then_some(Excuse::HealCannotHold)
}

/// Whether [`excuse`] holds.
pub(super) fn excused(
    state: &StateHandle,
    cell: &Cell,
    journal: JournalIdentifier,
    now: Duration,
) -> bool {
    excuse(state, cell, journal, now).is_some()
}

/// Whether `tenant`'s matchmaker set in force, as the audit bound it,
/// holds no majority outside `wiped`. Every campaign registers at a
/// matchmaker quorum before its first `Prepare`, and a wiped matchmaker
/// never rejoins (the library refuses an amnesiac boot), so no journal of
/// the tenant can elect the leader a re-placement needs. `false` on plain
/// Multi-Paxos (no set), or before the audit bound one: the journal is
/// judged.
pub(super) fn set_lost(state: &StateHandle, tenant: TenantId, wiped: &BTreeSet<u64>) -> bool {
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
