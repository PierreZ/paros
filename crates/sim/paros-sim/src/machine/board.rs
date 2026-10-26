//! The run's facts about its machines ([`MachineBoard`]): what the operators did, what the scenarios picked, and what the cell formed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{StateHandle, assert_always};
use paros::core::{Ballot, NodeId};
use paros::machine::{CachedRegistry, CellPlan, ControlJournals};
use paros::model::address::Address;

use crate::machine::names::{
    MACHINE_PORT, all_founders_formed, machine_host, may_rename, renamed_outside_registered,
    renamed_registered,
};
use crate::machine::{detector, heal};

/// The run's facts about its machines (`crate::state::published`).
#[derive(Default)]
pub(crate) struct MachineBoard {
    /// An operator sent `init` to a machine.
    pub(crate) init_sent: bool,
    /// The founding members the run's `cell init` lists (the layout's), by
    /// advertised address (#257).
    pub(crate) founders: BTreeSet<Address>,
    /// Each machine's process IP, by advertised address: what a fault
    /// strikes.
    pub(crate) ips: BTreeMap<Address, String>,
    /// Each machine's failure domain, by advertised address (#215 (zones)).
    pub(crate) zones: BTreeMap<Address, String>,
    /// The zone-outage scenario held a zone down from the first instant to
    /// the second (#215 (zones)).
    pub(crate) zone_down: Option<(Duration, Duration)>,
    /// A client created a single-writer journal through the coordinator
    /// (#215 (zones)): the zone outage waits for one while chaos lasts.
    pub(crate) journal_created: bool,
    /// Every durable vote of a machine still on its disk, by minted id: all
    /// of them name one cell. A wiped machine's vote is gone with its disk.
    pub(crate) voters: BTreeMap<u64, CellPlan>,
    /// The machines that formed, by minted id.
    pub(crate) formed: BTreeSet<u64>,
    /// Each machine's minted id, by address: the record on its disk now.
    pub(crate) nodes: BTreeMap<Address, u64>,
    /// The machines a wipe replaced: the address and the id it held.
    pub(crate) wiped: BTreeSet<(Address, u64)>,
    /// The live bookings of the registries the machines fold: a wiped
    /// machine heals once none is on its old id (moonpool #344 (rolling
    /// wipes), [`heal`]).
    pub(crate) bookings: heal::CellBookings,
    /// Each machine's last recorded promise in the cell decree, by minted id.
    pub(crate) promises: BTreeMap<u64, Ballot>,
    /// The first machine to record a promise: `cell init`'s receiver, which
    /// promises to itself before its fan-out reaches the others.
    pub(crate) first_promiser: Option<u64>,
    /// The ballots each plan was accepted at, by cell id.
    pub(crate) votes: BTreeMap<u64, BTreeSet<Ballot>>,
    /// The machines `cell add-machine` admitted (#216), still on their
    /// disks, by minted id: the cell each recorded.
    pub(crate) admitted: BTreeMap<u64, ControlJournals>,
    /// The cells a vote named with a majority of its members wiped, by cell
    /// id: lost for the rest of the run, even once a later wipe takes the
    /// last such vote.
    pub(crate) lost: BTreeSet<u64>,
    /// Every election journal a formatting plan named (#240): multi-writer
    /// journals, which the audit models as such.
    pub(crate) elections: BTreeSet<paros::core::journal::JournalIdentifier>,
    /// The cell control journal each election journal's terms install on
    /// (#653 (election term to installed leader)), by election journal.
    pub(crate) governs:
        BTreeMap<paros::core::journal::JournalIdentifier, paros::core::journal::JournalIdentifier>,
    /// The address an operator founded another cell on (#216): a machine
    /// that replaced a wiped member of the run's cell, alone in its own.
    pub(crate) other_founder: Option<Address>,
    /// The votes for that other cell still on a disk, by minted id.
    pub(crate) others: BTreeMap<u64, CellPlan>,
    /// How many of those votes a wipe took ([`founder_in_other_cell`]).
    pub(crate) others_wiped: u64,
    /// The other cell is the run's second cell (#232 (multiple cells)): an
    /// idle cell an operator founded on a machine `init` does not list, to
    /// join it to the universe ([`second_cell_target`]).
    pub(crate) second: bool,
    /// The IP each machine binds now, by rank (#257): its process IP, or the
    /// address it moved to.
    pub(crate) listens: BTreeMap<usize, std::net::IpAddr>,
    /// How many times each machine moved, by rank.
    pub(crate) moves: BTreeMap<usize, u8>,
    /// The name each renamed machine advertises now, by rank (#349): a
    /// machine absent here advertises its rank's address.
    pub(crate) renamed: BTreeMap<usize, Address>,
    /// How many times each machine was renamed, by rank.
    pub(crate) renames: BTreeMap<usize, u8>,
    /// Each machine's last durable cached registry fold, by minted id
    /// (#211).
    pub(crate) cached: BTreeMap<u64, CachedRegistry>,
    /// Every cached registry fold each machine tried to write, by minted id
    /// and position: what its disk may hold.
    pub(crate) cache_writes: BTreeMap<u64, BTreeMap<u64, CachedRegistry>>,
    /// The rank the moved-founder scenario crashed (#211): its next reboot
    /// comes back under a new name.
    pub(crate) rename_next: Option<usize>,
    /// When the downed-founder scenario held a founding member down (#213
    /// (three founding members)): from its crash to its restart, in
    /// simulated time.
    pub(crate) downed: Option<(Duration, Duration)>,
    /// The machine the gone-machine scenario takes down (#475 (re-place
    /// after the Down bound)), by minted id, once an operator picked it, and
    /// whether the injector struck it.
    pub(crate) gone: Option<(u64, bool)>,
    /// Every maintenance window an operator opened (#359).
    pub(crate) maintenance: Vec<MaintenanceNote>,
    /// The machines an operator removed (#359), by minted id.
    pub(crate) removed: BTreeSet<u64>,
    /// The machines an operator asked to remove (#359), by minted id: a
    /// removal whose answer was lost may still have landed.
    pub(crate) remove_asked: BTreeSet<u64>,
    /// The audit's fold of the calls every machine metered (#366
    /// (metrics)): what the machines' counters must add up to.
    pub(crate) metered: paros::observe::metrics::Totals,
    /// Each machine's metrics by its process address (#366 (metrics)): a
    /// reboot gets the metrics it had, as a real `/metrics` keeps counting
    /// on the same host, and the oracle reads every one.
    pub(crate) metrics: BTreeMap<String, paros::observe::metrics::Metrics>,
    /// When each machine's process last started `run_machine`, by
    /// advertised address (#654 (live machine marked down)).
    pub(crate) started: BTreeMap<Address, Duration>,
    /// When each machine's process last stopped, by advertised address.
    pub(crate) ended: BTreeMap<Address, Duration>,
    /// The latest instant the slow-machine scenario may slow a machine to.
    pub(crate) slowed: Option<Duration>,
    /// The `MachineDown`s sent in the tail and not judged yet
    /// (`detector::judge_sent`).
    pub(crate) downs: Vec<detector::SentDown>,
}

/// A maintenance window an operator opened over a machine (#359), for the
/// final oracle: no slot booked on it when the window opened is released
/// while the window is open.
#[derive(Clone, Debug)]
pub(crate) struct MaintenanceNote {
    /// The machine.
    pub(crate) node: u64,
    /// The window's scope: the machine, or its zone (#661 (maintenance
    /// windows)).
    pub(crate) scope: paros::model::registry::MaintenanceScope,
    /// When the operator sent the entry, in simulated time: the window
    /// opens no earlier.
    pub(crate) sent: Duration,
    /// The window's length.
    pub(crate) length: Duration,
    /// The bookings held on the machine when the operator sent it: each id
    /// and its journal or set.
    pub(crate) slots: Vec<(u64, paros::model::registry::BookingTarget)>,
}

impl MachineBoard {
    /// Remember as lost every cell a vote still on a disk names with a
    /// majority of its members wiped. Called after each wipe and after each vote lands.
    pub(crate) fn note_lost(&mut self) {
        let lost: Vec<u64> = self
            .voters
            .values()
            .filter(|plan| {
                let n = plan.members.len();
                n.saturating_sub(wiped_members(self, plan) + renamed_members(self, plan))
                    < n / 2 + 1
            })
            .map(|plan| plan.cell_id)
            .collect();
        self.lost.extend(lost);
    }
}

pub(crate) const MACHINE_BOARD_KEY: &str = "paros-machine-board";

/// The run's [`MachineBoard`].
pub(crate) fn machine_board(state: &StateHandle) -> Arc<Mutex<MachineBoard>> {
    crate::state::published(state, MACHINE_BOARD_KEY, MachineBoard::default)
}

pub(crate) fn lock(board: &Mutex<MachineBoard>) -> std::sync::MutexGuard<'_, MachineBoard> {
    board.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An operator is about to send `init` (the workload, before the call).
pub(crate) fn note_init_sent(state: &StateHandle) {
    lock(&machine_board(state)).init_sent = true;
}

/// Whether any operator sent `init` this run.
pub(crate) fn init_sent(state: &StateHandle) -> bool {
    lock(&machine_board(state)).init_sent
}

/// The cell the machines formed, once one did: the cell every durable vote
/// still on a disk names.
pub(crate) fn formed_cell(state: &StateHandle) -> Option<ControlJournals> {
    lock(&machine_board(state))
        .voters
        .values()
        .next()
        .map(CellPlan::control_journals)
}

/// Whether `journal` is a cell's election journal (#240): multi-writer.
pub(crate) fn is_election(
    state: &StateHandle,
    journal: paros::core::journal::JournalIdentifier,
) -> bool {
    lock(&machine_board(state)).elections.contains(&journal)
}

/// Every election journal a formatting plan named, with the cell control
/// journal its terms install on (#653 (election term to installed leader)).
pub(crate) fn governed(
    state: &StateHandle,
) -> Vec<(
    paros::core::journal::JournalIdentifier,
    paros::core::journal::JournalIdentifier,
)> {
    lock(&machine_board(state))
        .governs
        .iter()
        .map(|(election, cell)| (*election, *cell))
        .collect()
}

/// How many of `plan`'s members came back under a new name (#349): a
/// renamed member its peers cannot reach until the registry holds its new
/// name. Counted with the wiped ones when a later wipe could leave the
/// others no majority to write that name with.
pub(crate) fn renamed_members(board: &MachineBoard, plan: &CellPlan) -> usize {
    if board.wiped.is_empty() {
        return 0;
    }
    board
        .renamed
        .keys()
        .filter_map(|rank| {
            let addr = Address::parse(&format!("{}:{MACHINE_PORT}", machine_host(*rank))).ok()?;
            board.nodes.get(&addr).copied()
        })
        .filter(|node| plan.members.iter().any(|(id, _)| id.0 == *node))
        .count()
}

/// How many of `plan`'s members the board saw wiped.
pub(crate) fn wiped_members(board: &MachineBoard, plan: &CellPlan) -> usize {
    plan.members
        .iter()
        .filter(|(id, _)| board.wiped.iter().any(|(_, old)| *old == id.0))
        .count()
}

/// The machines admitted into cell `cell_id` (#216) whose admission is still
/// on their disks, by minted id.
pub(crate) fn admitted_into(state: &StateHandle, cell_id: u64) -> Vec<u64> {
    lock(&machine_board(state))
        .admitted
        .iter()
        .filter(|(_, cell)| cell.cell_id == cell_id)
        .map(|(node, _)| *node)
        .collect()
}

/// Every node id a machine minted this run (#474 (journals on admitted
/// machines)): the record on each disk now, and every wiped machine's old
/// id. The cell may place a journal on any it registered, an admission in
/// flight included.
pub(crate) fn minted_ids(state: &StateHandle) -> BTreeSet<u64> {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .nodes
        .values()
        .copied()
        .chain(board.wiped.iter().map(|(_, old)| *old))
        .collect()
}

/// Whether the machine at `addr` holds an admission on its disk now (#216).
pub(crate) fn is_admitted(state: &StateHandle, addr: &Address) -> bool {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .nodes
        .get(addr)
        .is_some_and(|node| board.admitted.contains_key(node))
}

/// Whether a wipe replaced the machine at `addr` this run: an answer it sent
/// before the wipe may name a disk that is gone.
pub(crate) fn was_wiped(state: &StateHandle, addr: &Address) -> bool {
    lock(&machine_board(state))
        .wiped
        .iter()
        .any(|(wiped, _)| wiped == addr)
}

/// The node id every wiped machine had before its wipe (#447 (model checks
/// for every journal world)): gone for good, so no journal it hosted waits
/// on it.
pub(crate) fn wiped_ids(state: &StateHandle) -> BTreeSet<u64> {
    lock(&machine_board(state))
        .wiped
        .iter()
        .map(|(_, old)| *old)
        .collect()
}

/// The machines `cell add-machine` admitted into their cell (#216): with the
/// founding members, the only ones that ever serve its journals.
pub(crate) fn admitted_ids(state: &StateHandle) -> BTreeSet<u64> {
    lock(&machine_board(state))
        .admitted
        .keys()
        .copied()
        .collect()
}

/// The minted id of every machine renamed this run (#349): a peer that
/// opened a lane to it before its rename must follow its new name.
pub(crate) fn renamed_ids(state: &StateHandle) -> BTreeSet<u64> {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .renamed
        .keys()
        .filter_map(|rank| {
            let addr = Address::parse(&format!("{}:{MACHINE_PORT}", machine_host(*rank))).ok()?;
            board.nodes.get(&addr).copied()
        })
        .collect()
}

/// The gone-machine scenario's operator picked machine `node` to take down
/// (#475): `false` when one was picked already.
pub(crate) fn request_gone(state: &StateHandle, node: u64) -> bool {
    let board = machine_board(state);
    let mut board = lock(&board);
    if board.gone.is_some() {
        return false;
    }
    board.gone = Some((node, false));
    true
}

/// Whether the gone-machine scenario's operator picked its machine.
pub(crate) fn gone_requested(state: &StateHandle) -> bool {
    lock(&machine_board(state)).gone.is_some()
}

/// The process IP of the machine the gone-machine scenario picked, once,
/// while it is alive: the injector strikes it then (#475).
pub(crate) fn gone_target(state: &StateHandle, dead: impl Fn(&str) -> bool) -> Option<String> {
    let board = machine_board(state);
    let mut board = lock(&board);
    let (node, false) = board.gone? else {
        return None;
    };
    let addr = board
        .nodes
        .iter()
        .find(|(_, id)| **id == node)
        .map(|(addr, _)| addr.clone())?;
    let ip = board.ips.get(&addr).filter(|ip| !dead(ip)).cloned()?;
    board.gone = Some((node, true));
    Some(ip)
}

/// An operator opened a maintenance window (#359).
pub(crate) fn note_maintenance(state: &StateHandle, note: MaintenanceNote) {
    lock(&machine_board(state)).maintenance.push(note);
}

/// An operator sends a new window over `scope`, or ends it, at `at`
/// (#661 (maintenance windows)): every window noted over `scope` holds no
/// longer than `at`.
pub(crate) fn note_window_cut(
    state: &StateHandle,
    scope: &paros::model::registry::MaintenanceScope,
    at: Duration,
) {
    let board = machine_board(state);
    let mut board = lock(&board);
    for note in board.maintenance.iter_mut().filter(|n| n.scope == *scope) {
        note.length = note.length.min(at.saturating_sub(note.sent));
    }
}

/// The machine the gone-machine scenario struck (#475), by minted id,
/// while it is down.
pub(crate) fn gone_down(state: &StateHandle) -> Option<u64> {
    let board = machine_board(state);
    let board = lock(&board);
    let Some((node, true)) = board.gone else {
        return None;
    };
    let addr = board.nodes.iter().find(|(_, id)| **id == node)?.0;
    let started = board.started.get(addr)?;
    board
        .ended
        .get(addr)
        .is_some_and(|ended| ended >= started)
        .then_some(node)
}

/// An operator removed machine `node` from the cell (#359).
pub(crate) fn note_removed(state: &StateHandle, node: u64) {
    lock(&machine_board(state)).removed.insert(node);
}

/// An operator asks to remove machine `node` from the cell (#359).
pub(crate) fn note_remove_asked(state: &StateHandle, node: u64) {
    lock(&machine_board(state)).remove_asked.insert(node);
}

/// Whether an operator asked to remove machine `node` (#359).
pub(crate) fn remove_asked(state: &StateHandle, node: u64) -> bool {
    lock(&machine_board(state)).remove_asked.contains(&node)
}

/// The machines an operator removed (#359).
pub(crate) fn removed_ids(state: &StateHandle) -> BTreeSet<u64> {
    lock(&machine_board(state)).removed.clone()
}

/// Every maintenance window an operator opened (#359).
pub(crate) fn maintenance_notes(state: &StateHandle) -> Vec<MaintenanceNote> {
    lock(&machine_board(state)).maintenance.clone()
}

/// Whether the machine at `addr` is a founding member `init` lists.
pub(crate) fn is_founder(state: &StateHandle, addr: &Address) -> bool {
    lock(&machine_board(state)).founders.contains(addr)
}

/// Whether any machine was wiped this run: the one way a machine can hold
/// another cell than the operator's.
pub(crate) fn founder_wiped(state: &StateHandle) -> bool {
    !lock(&machine_board(state)).wiped.is_empty()
}

/// The machine an operator may found another cell on (#216), if the run
/// has one: once per run (again until it forms), a machine at the address
/// of a wiped member of the run's cell, whose old id a vote still names (so
/// the cell's members keep sending it their peer traffic), and which holds
/// no vote and no admission of its own: it is idle.
pub(crate) fn other_cell_target(state: &StateHandle) -> Option<Address> {
    let board = machine_board(state);
    let board = lock(&board);
    if board.second {
        return None;
    }
    if let Some(addr) = &board.other_founder {
        return board.others.is_empty().then(|| addr.clone());
    }
    let plan = board.voters.values().next()?;
    plan.members
        .iter()
        .filter(|(id, addr)| board.wiped.contains(&(addr.clone(), id.0)))
        .map(|(_, addr)| addr.clone())
        .find(|addr| {
            board.nodes.get(addr).is_some_and(|node| {
                !board.voters.contains_key(node) && !board.admitted.contains_key(node)
            })
        })
}

/// An operator is about to found another cell on the machine at `addr`
/// (#216), alone.
pub(crate) fn note_other_cell(state: &StateHandle, addr: &Address) {
    let board = machine_board(state);
    let mut board = lock(&board);
    assert_always!(
        board.other_founder.as_ref().is_none_or(|held| held == addr),
        "machine: an operator founds at most one other cell"
    );
    board.other_founder = Some(addr.clone());
}

/// The machine an operator may found the run's second cell on (#232
/// (multiple cells)), among `unlisted` (the machines `init` does not list):
/// one that holds no vote and no admission, while no other cell was
/// founded, or again at the address the second cell was founded on until
/// it forms (a wipe or an admission may have taken the machine there).
pub(crate) fn second_cell_target(state: &StateHandle, unlisted: &[Address]) -> Option<Address> {
    let board = machine_board(state);
    let board = lock(&board);
    let idle = |addr: &Address| {
        board.nodes.get(addr).is_some_and(|node| {
            !board.voters.contains_key(node)
                && !board.admitted.contains_key(node)
                && !board.others.contains_key(node)
        })
    };
    match &board.other_founder {
        Some(addr) if board.second => (board.others.is_empty() && idle(addr)).then(|| addr.clone()),
        Some(_) => None,
        None => unlisted.iter().find(|addr| idle(addr)).cloned(),
    }
}

/// An operator is about to found the run's second cell on the machine at
/// `addr` (#232 (multiple cells)), alone.
pub(crate) fn note_second_cell(state: &StateHandle, addr: &Address) {
    let board = machine_board(state);
    let mut board = lock(&board);
    assert_always!(
        board.other_founder.is_none() || board.second && board.other_founder.as_ref() == Some(addr),
        "machine: an operator founds at most one other cell"
    );
    board.other_founder = Some(addr.clone());
    board.second = true;
}

/// Whether the machine at `addr` is where an operator founded another cell
/// (#216, #232 (multiple cells)): it may answer for that cell.
pub(crate) fn is_other_founder(state: &StateHandle, addr: &Address) -> bool {
    lock(&machine_board(state)).other_founder.as_ref() == Some(addr)
}

/// Whether an operator founded the run's second cell (#232 (multiple
/// cells)), or set out to: a machine of the run may answer for another cell.
pub(crate) fn second_cell_founded(state: &StateHandle) -> bool {
    lock(&machine_board(state)).second
}

/// Whether the vote of the second cell (#232 (multiple cells)) is still on
/// its machine's disk.
pub(crate) fn second_cell_alive(state: &StateHandle) -> bool {
    let board = machine_board(state);
    let board = lock(&board);
    board.second && !board.others.is_empty()
}

/// Whether `plan` is the other cell an operator founded at `addr` (#216): a
/// plan over that address alone, when it is not the run's founding list.
pub(crate) fn is_other_cell(board: &MachineBoard, addr: &Address, plan: &CellPlan) -> bool {
    board.other_founder.as_ref() == Some(addr)
        && plan.addrs().iter().eq(std::iter::once(addr))
        && plan.addrs() != board.founders
}

/// Whether another cell an operator founded holds the address of one of
/// this run's founders (#216 (another cell)): a re-run `init` over the
/// founders is then refused `other_cell_init`, because no vote of the run's
/// own plan survives to adopt.
///
/// `since` is [`other_cells_wiped`] when the `init` started: a vote of the
/// other cell wiped since then held the founder while the `init` ran, so
/// its refusal stands though the founder is idle again, or formed the run's
/// cell, by the time it is judged (hunt seed 16513366675242790746: the other
/// cell's machine was wiped and its successor formed the run's cell before
/// the refused `cell init` returned).
pub(crate) fn founder_in_other_cell(state: &StateHandle, since: u64) -> bool {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .other_founder
        .as_ref()
        .is_some_and(|addr| board.founders.contains(addr))
        && (!board.others.is_empty() || board.others_wiped > since)
}

/// How many votes of the other cell (#216) a wipe took so far.
pub(crate) fn other_cells_wiped(state: &StateHandle) -> u64 {
    lock(&machine_board(state)).others_wiped
}

/// Whether the run's cell is lost (#246): a vote named a plan that lost a
/// majority of its members to wipes, so no `cell init` can choose it and
/// every later one refuses `cell_lost`, and the cell's majority journals
/// cannot serve. The Paxos limit: the control plane's liveness is excused.
/// A plan that kept a majority heals: its members choose it around the
/// wiped ones. The fact is sticky: a later wipe that takes the last such
/// vote leaves the cell no less lost.
pub(crate) fn cell_lost(state: &StateHandle) -> bool {
    !lock(&machine_board(state)).lost.is_empty()
}

/// Whether the plan of `cell_id` was voted and then lost to wipes (#536
/// (cell init oracle reads a stale board)): a `cell init` reply that named
/// it was true when it came, though the machines have since formed another.
pub(crate) fn plan_lost(state: &StateHandle, cell_id: u64) -> bool {
    lock(&machine_board(state)).lost.contains(&cell_id)
}

/// Whether `ip` is a machine's process IP the board knows.
pub(crate) fn is_machine_ip(state: &StateHandle, ip: &str) -> bool {
    let board = machine_board(state);
    lock(&board).ips.values().any(|known| known == ip)
}

/// The process IP of the machine that advertises `addr` (#257): a named
/// address, or one a moved machine left, is not the IP a kill strikes.
pub(crate) fn process_ip(state: &StateHandle, addr: &Address) -> Option<String> {
    let board = machine_board(state);
    lock(&board).ips.get(addr).cloned()
}

/// The founding member the wiped-founder scenario wipes now
/// (`crate::world::scenarios::wiped_founder`), if the run is at its moment: an operator
/// sent `init`, and either every founder promised and none voted
/// (`after_vote` false: a founder other than `cell init`'s receiver), or a
/// founder voted and another did not (`after_vote` true: that one). Only a
/// live founder whose minted id the board knows. The answer is the founder's
/// process IP, what the wipe strikes.
pub(crate) fn wipe_target(
    state: &StateHandle,
    after_vote: bool,
    dead: impl Fn(&str) -> bool,
) -> Option<String> {
    let board = machine_board(state);
    let board = lock(&board);
    if !board.init_sent || !board.wiped.is_empty() {
        return None;
    }
    let mut live = board.founders.iter().filter_map(|addr| {
        let node = *board.nodes.get(addr)?;
        let ip = board.ips.get(addr)?;
        (!dead(ip)).then(|| (ip.clone(), node))
    });
    if after_vote {
        if board.voters.is_empty() {
            return None;
        }
        live.find(|(_, node)| !board.voters.contains_key(node))
            .map(|(ip, _)| ip)
    } else {
        if !board.voters.is_empty() {
            return None;
        }
        // Once every founder promised, a founder other than the receiver:
        // the receiver's decree goes on and forms the others, so the old
        // machine's vote outlives it (on a one-founder cell, the founder).
        let promised: Vec<(String, u64)> = live
            .filter(|(_, node)| board.promises.contains_key(node))
            .collect();
        if promised.len() < board.founders.len() {
            return None;
        }
        promised
            .iter()
            .rev()
            .find(|(_, node)| board.founders.len() == 1 || board.first_promiser != Some(*node))
            .map(|(ip, _)| ip.clone())
    }
}

/// The founding member the replaced-founder scenario wipes now
/// (`crate::world::scenarios::replaced_founder`, #423), if the run is at its moment:
/// every founder of a cell of three or more formed and up, every renamed
/// founder registered at its new name, and no machine was wiped yet. The highest-ranked founder, so the cell keeps a majority
/// of its members. The answer is the founder's process IP, what the wipe
/// strikes.
pub(crate) fn replace_target(state: &StateHandle, dead: impl Fn(&str) -> bool) -> Option<String> {
    let board = machine_board(state);
    let board = lock(&board);
    if board.founders.len() < 3 || !board.wiped.is_empty() {
        return None;
    }
    let formed = board.founders.iter().all(|addr| {
        board
            .nodes
            .get(addr)
            .is_some_and(|node| board.formed.contains(node))
    });
    if !formed {
        return None;
    }
    // Every founder up: a wipe while another founder is down (the silent
    // machine's crash) leaves a cell of three no majority.
    let ips: Vec<&String> = board
        .founders
        .iter()
        .filter_map(|addr| board.ips.get(addr))
        .collect();
    if ips.len() < board.founders.len() || ips.iter().any(|ip| dead(ip)) {
        return None;
    }
    // Every renamed founder registered at its new name: until then its peers
    // reach it only at its dead old name, so it is as lost as a down one,
    // and a wipe beside it takes the cell's majority for good. The mirror of
    // `may_rename`, which renames no founder once one is wiped. Witness:
    // recipe `17967350196043573522 331026:3916672135343956209` on 965794d,
    // a founder of four renamed at 63.771 s and another wiped 1 ms later.
    if !board
        .renamed
        .iter()
        .filter(|(rank, _)| **rank < board.founders.len())
        .all(|(rank, name)| renamed_registered(&board, *rank, name))
    {
        tracing::debug!("replaced_founder_waits_for_rename");
        return None;
    }
    ips.last().map(|ip| (*ip).clone())
}

/// Where the replaced-founder scenario stands (#423): the machine that
/// replaced a wiped founder, at the founder's address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Replacement {
    /// The machine at the address is idle: an operator may admit it.
    Idle(Address),
    /// The machine at the address holds an admission: a re-run `init` meets
    /// it among the founders.
    Admitted,
}

/// The machine that replaced a wiped founder (#423), when it is idle or
/// admitted. `None` before a wipe, and once the machine there holds a vote
/// or is wiped in turn before its boot is known.
pub(crate) fn replacement(state: &StateHandle) -> Option<Replacement> {
    let board = machine_board(state);
    let board = lock(&board);
    let (addr, node) = board
        .wiped
        .iter()
        .filter(|(addr, _)| board.founders.contains(addr))
        .find_map(|(addr, old)| {
            let node = *board.nodes.get(addr)?;
            (node != *old).then(|| (addr.clone(), node))
        })?;
    if board.admitted.contains_key(&node) {
        return Some(Replacement::Admitted);
    }
    (!board.voters.contains_key(&node)).then_some(Replacement::Idle(addr))
}

/// The founding member the moved-founder scenario crashes first
/// (`crate::world::scenarios::moved_founder`, #211), once every founding member formed:
/// a live one `may_rename` lets come back under a new name, once another
/// machine of the cell cached the registry. Its next reboot is a rename.
/// `None` before that, or when none may.
pub(crate) fn founder_to_move(state: &StateHandle, dead: impl Fn(&str) -> bool) -> Option<String> {
    let board = machine_board(state);
    let mut board = lock(&board);
    if board.founders.is_empty() || !all_founders_formed(&board) {
        return None;
    }
    // The cell formed, so the machines drew the layout already.
    let layout = crate::shape::machine_layout(state, 0);
    // Another machine of the cell cached the registry: once the founder
    // registers its new name, that machine's cache can name it there.
    let (rank, ip) = (0..layout.founders)
        .filter(|rank| may_rename(&board, &layout, *rank))
        .filter(|_| renamed_outside_registered(&board, &layout))
        .find_map(|rank| {
            let addr = Address::parse(&format!("{}:{MACHINE_PORT}", machine_host(rank))).ok()?;
            let node = board.nodes.get(&addr)?;
            let cached_elsewhere = board.cached.keys().any(|other| other != node);
            cached_elsewhere
                .then(|| board.ips.get(&addr).filter(|ip| !dead(ip)).cloned())
                .flatten()
                .map(|ip| (rank, ip))
        })?;
    // Its next boot comes back under a new name.
    board.rename_next = Some(rank);
    Some(ip)
}

/// The machine the moved-founder scenario crashes second (#211): a live
/// machine of the cell, other than a renamed founding member, whose durable
/// cached registry fold names that founder at its new name. Its next boot
/// dials the founder where only the cache knows it is. `None` before that.
pub(crate) fn cached_mover(state: &StateHandle, dead: impl Fn(&str) -> bool) -> Option<String> {
    let board = machine_board(state);
    let board = lock(&board);
    if board.renamed.is_empty() {
        return None;
    }
    // A machine renamed, so the machines drew the layout already.
    let layout = crate::shape::machine_layout(state, 0);
    let moved: Vec<(u64, &Address)> = board
        .renamed
        .iter()
        .filter(|(rank, _)| **rank < layout.founders)
        .filter_map(|(rank, name)| {
            let addr = Address::parse(&format!("{}:{MACHINE_PORT}", machine_host(*rank))).ok()?;
            board.nodes.get(&addr).map(|node| (*node, name))
        })
        .collect();
    board.nodes.iter().find_map(|(addr, node)| {
        let cache = board.cached.get(node)?;
        let follows = moved.iter().any(|(founder, name)| {
            founder != node && cache.address(NodeId(*founder)) == Some(*name)
        });
        follows
            .then(|| board.ips.get(addr).filter(|ip| !dead(ip)).cloned())
            .flatten()
    })
}

/// The machine the silent-machine scenario crashes now
/// (`crate::world::scenarios::silent_machine`), once the cell formed: a live machine
/// the cell admitted, else, on a cell of at least three founding members
/// that all formed and are up, a founder (the others keep a majority and
/// one of them leads), unless the downed-founder scenario takes one down (#213 (three
/// founding members)). `None` before that.
pub(crate) fn silent_target(state: &StateHandle, dead: impl Fn(&str) -> bool) -> Option<String> {
    // Drawn before the board's lock: the downed-founder scenario takes a
    // founder down itself, and two founders down leave a cell of three no
    // majority.
    let downed = crate::shape::downed_founder(state) || crate::shape::zone_outage(state);
    let board = machine_board(state);
    let board = lock(&board);
    let ip = |addr: &Address| board.ips.get(addr).filter(|ip| !dead(ip)).cloned();
    let admitted = board
        .nodes
        .iter()
        .filter(|(_, node)| board.admitted.contains_key(node))
        .find_map(|(addr, _)| ip(addr));
    if admitted.is_some() {
        return admitted;
    }
    let formed: Vec<&Address> = board
        .founders
        .iter()
        .filter(|addr| {
            board
                .nodes
                .get(*addr)
                .is_some_and(|n| board.formed.contains(n))
        })
        .collect();
    if downed || board.founders.len() < 3 || formed.len() < board.founders.len() {
        return None;
    }
    // Every founder up: a founder down already (a wipe not rebooted yet, a
    // crash) and a second one silent leave a cell of three no majority.
    if formed.iter().any(|addr| ip(addr).is_none()) {
        return None;
    }
    formed.into_iter().rev().find_map(ip)
}

/// The founding member the downed-founder scenario holds down now
/// (`crate::world::scenarios::downed_founder`, #213 (three founding members)): on a
/// cell of at least three founders, none wiped and every one alive, the
/// highest-ranked founder, once an operator sent `init`. With `formed`, only
/// once every founder formed; else only while the cell is not formed yet.
/// `None` before that.
pub(crate) fn downed_target(
    state: &StateHandle,
    formed: bool,
    dead: impl Fn(&str) -> bool,
) -> Option<String> {
    let board = machine_board(state);
    let board = lock(&board);
    if !board.init_sent || board.founders.len() < 3 || !board.wiped.is_empty() {
        return None;
    }
    let ips: Vec<&String> = board
        .founders
        .iter()
        .filter_map(|addr| board.ips.get(addr))
        .collect();
    if ips.len() < board.founders.len() || ips.iter().any(|ip| dead(ip)) {
        return None;
    }
    let all_formed = board.founders.iter().all(|addr| {
        board
            .nodes
            .get(addr)
            .is_some_and(|n| board.formed.contains(n))
    });
    if all_formed != formed {
        return None;
    }
    ips.last().map(|ip| (*ip).clone())
}

/// The downed-founder scenario holds a founding member down from `from` to
/// `until` (#213 (three founding members)).
pub(crate) fn note_downed(state: &StateHandle, from: Duration, until: Duration) {
    assert_always!(
        from < until,
        "machine: a downed founder comes back after it went down"
    );
    lock(&machine_board(state)).downed = Some((from, until));
}

/// Whether the downed-founder scenario held a founding member down for the
/// whole of `from..=until` (#213 (three founding members)): every control
/// write an operation chose inside it was chosen without that member.
pub(crate) fn founder_down_through(state: &StateHandle, from: Duration, until: Duration) -> bool {
    lock(&machine_board(state))
        .downed
        .is_some_and(|(down, up)| down <= from && until < up)
}

/// The machines the zone-outage scenario takes down now
/// (`crate::world::scenarios::zone_outage`, #215 (zones)): every live machine in
/// `zone`, once an operator sent `init`, on a cell of three founding
/// members or more, none wiped, every one alive and formed. The founders
/// stand one per zone on a scenario seed, so the others keep a majority.
/// `None` before that.
pub(crate) fn zone_targets(
    state: &StateHandle,
    zone: &str,
    dead: impl Fn(&str) -> bool,
) -> Option<Vec<String>> {
    let board = machine_board(state);
    let board = lock(&board);
    if !board.init_sent || board.founders.len() < 3 || !board.wiped.is_empty() {
        return None;
    }
    let ready = board.founders.iter().all(|addr| {
        board.ips.get(addr).is_some_and(|ip| !dead(ip))
            && board
                .nodes
                .get(addr)
                .is_some_and(|n| board.formed.contains(n))
    });
    if !ready {
        return None;
    }
    let in_zone: Vec<&Address> = board
        .zones
        .iter()
        .filter(|(_, z)| z.as_str() == zone)
        .map(|(addr, _)| addr)
        .collect();
    let founders_in = in_zone
        .iter()
        .filter(|addr| board.founders.contains(**addr))
        .count();
    assert_always!(
        founders_in * 2 < board.founders.len(),
        "zones: a zone outage leaves the founding members a majority",
        { "founders" => board.founders.len(), "in_zone" => founders_in }
    );
    let ips: Vec<String> = in_zone
        .into_iter()
        .filter_map(|addr| board.ips.get(addr).filter(|ip| !dead(ip)).cloned())
        .collect();
    (!ips.is_empty()).then_some(ips)
}

/// A client created a single-writer journal through the coordinator (#215
/// (zones)).
pub(crate) fn note_journal_created(state: &StateHandle) {
    lock(&machine_board(state)).journal_created = true;
}

/// Whether a client created a single-writer journal yet (#215 (zones)).
pub(crate) fn journal_created(state: &StateHandle) -> bool {
    lock(&machine_board(state)).journal_created
}

/// The zone-outage scenario holds a zone down from `from` to `until` (#215
/// (zones)).
pub(crate) fn note_zone_down(state: &StateHandle, from: Duration, until: Duration) {
    assert_always!(
        from < until,
        "zones: a downed zone comes back after it went down"
    );
    lock(&machine_board(state)).zone_down = Some((from, until));
}

/// Whether the zone-outage scenario held a zone down for the whole of
/// `from..=until` (#215 (zones)): every write chosen inside it was chosen
/// without that zone's machines.
pub(crate) fn zone_down_through(state: &StateHandle, from: Duration, until: Duration) -> bool {
    lock(&machine_board(state))
        .zone_down
        .is_some_and(|(down, up)| down <= from && until < up)
}

/// The machine the slow-machine scenario slows now
/// (`crate::world::scenarios::slow_machine`, #424 (busyness metrics)), once the cell
/// formed: the first live founding member that formed. It hosts the cell's
/// journals, so it has real disk and CPU work to be slow at. `None` before
/// that.
pub(crate) fn slow_target(state: &StateHandle, dead: impl Fn(&str) -> bool) -> Option<String> {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .founders
        .iter()
        .filter(|addr| {
            board
                .nodes
                .get(*addr)
                .is_some_and(|n| board.formed.contains(n))
        })
        .find_map(|addr| board.ips.get(addr).filter(|ip| !dead(ip)).cloned())
}
