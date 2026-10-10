//! The machines in the simulation (#246): every process of the
//! [`MACHINE_GROUP`] is a `parosd`, running the shipped
//! [`paros::machine::run_machine`] from an empty simulated disk — format (a
//! minted `node_id`), the wait, then the cell's journals — under the
//! driver's BUGGIFY sites and every fault the campaign draws. All of paros runs in the
//! simulation, setup included (decided on 2026-10-09): no cell is handed to
//! a machine; the workload's operators form it with `init`
//! (`paros::client::initialize`, the code `parosctl init` prints) against
//! the founding members the seed drew ([`crate::shape::machine_layout`]),
//! with `cell init`'s decree (#277).
//!
//! The disk is the library's [`ProviderDisk`] over moonpool's simulated
//! disk, handed to `run_machine` bare: no wrapper sits between the shipped
//! lifecycle and the disk (#294). Its faults are the lifecycle's own
//! `hint!`s and `buggify_*!` sites, struck by moonpool's attrition under the
//! machine group's regime, and its facts reach the oracles through the audit
//! port ([`NodeAudit::on_machines`]). The stores are `JournalStorage`, every
//! one an existing member's once formed, as `parosd`'s are. They store
//! ordered, outside the ledgered injector (the acceptors' fault model): a
//! one-member cell has no second copy to repair an ambiguous batch from,
//! and an ordered commit cut by a hint is torn or whole, never ambiguous.
//!
//! The [`MachineBoard`] holds the run's facts about its machines that the
//! oracles judge: whether an operator sent `init` (no cell forms without
//! one, §3.1), and the one cell every formation names — over the founding
//! members `init` listed, and only on them (an idle machine no `cell init`
//! lists stays idle).
//!
//! **A wiped machine** (#246): the machine group's attrition draws
//! moonpool's own `CrashAndWipe` now and then (`crate::chaos_surfaces`), at
//! a timed reboot or at a lifecycle `hint!`, so a founding member may lose
//! its whole disk during `init` (or after it). The machine at that address
//! is then a new one, with a new `node_id`, which never rejoins as the old
//! one; the board recognizes the wipe when it boots on an empty disk where
//! a record was. Before any vote names the old machine, `init` forms the
//! cell over the new one. After one does, `cell init` adopts that plan and
//! the members that kept their disks choose it, a majority of them being
//! enough: the cell forms with the old id as a dead member. Only when a
//! majority of the plan's members are wiped can no plan be chosen: `cell
//! init` refuses `cell_lost`, the Paxos limit, and the run's control-plane
//! liveness is excused ([`cell_lost`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use moonpool_sim::{
    Process, ScriptedResolver, SimContext, SimTimeProvider, SimulationError, SimulationResult,
    StateHandle, assert_always, assert_reachable, assert_sometimes,
};
use paros::machine::{
    AuditScope, CachedRegistry, CellPlan, ControlJournals, MachineAddresses, MachineError,
    MachineRecord, MachineSettings, ProviderDisk,
};
use paros::{Address, Ballot, NodeId, RunError};

use crate::audit::NodeAudit;
use crate::audit::journals::journal_board;
use crate::process::{RoleRig, arm_role, dispatch};
use crate::roles::{Deployment, MACHINE_GROUP, Role};

/// Where a machine's disk lives on its simulated disk.
const ROOT: &str = "paros/machine";

/// The run's facts about its machines (`crate::state::published`).
#[derive(Default)]
pub(crate) struct MachineBoard {
    /// An operator sent `init` to a machine.
    init_sent: bool,
    /// The founding members the run's `cell init` lists (the layout's), by
    /// advertised address (#257).
    founders: BTreeSet<Address>,
    /// Each machine's process IP, by advertised address: what a fault
    /// strikes.
    ips: BTreeMap<Address, String>,
    /// Every durable vote of a machine still on its disk, by minted id: all
    /// of them name one cell. A wiped machine's vote is gone with its disk.
    voters: BTreeMap<u64, CellPlan>,
    /// The machines that formed, by minted id.
    formed: BTreeSet<u64>,
    /// Each machine's minted id, by address: the record on its disk now.
    nodes: BTreeMap<Address, u64>,
    /// The machines a wipe replaced: the address and the id it held.
    wiped: BTreeSet<(Address, u64)>,
    /// Each machine's last recorded promise in the cell decree, by minted id.
    promises: BTreeMap<u64, Ballot>,
    /// The first machine to record a promise: `cell init`'s receiver, which
    /// promises to itself before its fan-out reaches the others.
    first_promiser: Option<u64>,
    /// The ballots each plan was accepted at, by cell id.
    votes: BTreeMap<u64, BTreeSet<Ballot>>,
    /// The machines `cell add-machine` admitted (#216), still on their
    /// disks, by minted id: the cell each recorded.
    admitted: BTreeMap<u64, ControlJournals>,
    /// The cells a vote named with a majority of its members wiped, by cell
    /// id: lost for the rest of the run, even once a later wipe takes the
    /// last such vote.
    lost: BTreeSet<u64>,
    /// Every election journal a formatting plan named (#240): multi-writer
    /// journals, which the audit models as such.
    elections: BTreeSet<paros::JournalIdentifier>,
    /// The address an operator founded another cell on (#216): a machine
    /// that replaced a wiped member of the run's cell, alone in its own.
    other_founder: Option<Address>,
    /// The votes for that other cell still on a disk, by minted id.
    others: BTreeMap<u64, CellPlan>,
    /// The IP each machine binds now, by rank (#257): its process IP, or the
    /// address it moved to.
    listens: BTreeMap<usize, std::net::IpAddr>,
    /// How many times each machine moved, by rank.
    moves: BTreeMap<usize, u8>,
    /// The name each renamed machine advertises now, by rank (#349): a
    /// machine absent here advertises its rank's address.
    renamed: BTreeMap<usize, Address>,
    /// How many times each machine was renamed, by rank.
    renames: BTreeMap<usize, u8>,
    /// Each machine's last durable cached registry fold, by minted id
    /// (#211).
    cached: BTreeMap<u64, CachedRegistry>,
    /// Every cached registry fold each machine tried to write, by minted id
    /// and position: what its disk may hold.
    cache_writes: BTreeMap<u64, BTreeMap<u64, CachedRegistry>>,
    /// The rank the moved-founder scenario crashed (#211): its next reboot
    /// comes back under a new name.
    rename_next: Option<usize>,
}

impl MachineBoard {
    /// Remember as lost every cell a vote still on a disk names with a
    /// majority of its members wiped. Called after each wipe and after each vote lands.
    fn note_lost(&mut self) {
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

const MACHINE_BOARD_KEY: &str = "paros-machine-board";

/// The run's [`MachineBoard`].
pub(crate) fn machine_board(state: &StateHandle) -> Arc<Mutex<MachineBoard>> {
    crate::state::published(state, MACHINE_BOARD_KEY, MachineBoard::default)
}

fn lock(board: &Mutex<MachineBoard>) -> std::sync::MutexGuard<'_, MachineBoard> {
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
pub(crate) fn is_election(state: &StateHandle, journal: paros::JournalIdentifier) -> bool {
    lock(&machine_board(state)).elections.contains(&journal)
}

/// How many of `plan`'s members came back under a new name (#349): a
/// renamed member its peers cannot reach until the registry holds its new
/// name. Counted with the wiped ones when a later wipe could leave the
/// others no majority to write that name with.
fn renamed_members(board: &MachineBoard, plan: &CellPlan) -> usize {
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
fn wiped_members(board: &MachineBoard, plan: &CellPlan) -> usize {
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

/// Whether `plan` is the other cell an operator founded at `addr` (#216): a
/// plan over that address alone, when it is not the run's founding list.
fn is_other_cell(board: &MachineBoard, addr: &Address, plan: &CellPlan) -> bool {
    board.other_founder.as_ref() == Some(addr)
        && plan.addrs().iter().eq(std::iter::once(addr))
        && plan.addrs() != board.founders
}

/// Whether another cell an operator founded holds the address of one of
/// this run's founders (#216 (another cell)): a re-run `init` over the
/// founders is then refused `other_cell_init`, because no vote of the run's
/// own plan survives to adopt.
pub(crate) fn founder_in_other_cell(state: &StateHandle) -> bool {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .other_founder
        .as_ref()
        .is_some_and(|addr| board.founders.contains(addr))
        && !board.others.is_empty()
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

/// The process IP of the machine that advertises `addr` (#257): a named
/// address, or one a moved machine left, is not the IP a kill strikes.
pub(crate) fn process_ip(state: &StateHandle, addr: &Address) -> Option<String> {
    let board = machine_board(state);
    lock(&board).ips.get(addr).cloned()
}

/// The founding member the wiped-founder scenario wipes now
/// (`crate::world::wiped_founder`), if the run is at its moment: an operator
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

/// The founding member the moved-founder scenario crashes first
/// (`crate::world::moved_founder`, #211), once every founding member formed:
/// a live one `may_rename` lets come back under a new name, once another
/// machine of the cell cached the registry. Its next reboot is a rename.
/// `None` before that, or when none may.
pub(crate) fn founder_to_move(state: &StateHandle, dead: impl Fn(&str) -> bool) -> Option<String> {
    let board = machine_board(state);
    let mut board = lock(&board);
    let formed = board.founders.iter().all(|addr| {
        board
            .nodes
            .get(addr)
            .is_some_and(|n| board.formed.contains(n))
    });
    if board.founders.is_empty() || !formed {
        return None;
    }
    // The cell formed, so the machines drew the layout already.
    let layout = crate::shape::machine_layout(state, 0);
    // Another machine of the cell cached the registry: once the founder
    // registers its new name, that machine's cache can name it there.
    let (rank, ip) = (0..layout.founders)
        .filter(|rank| may_rename(&board, &layout, *rank))
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
/// (`crate::world::silent_machine`), once the cell formed: a live machine
/// the cell admitted, else, on a cell of at least three founding members, a
/// live founder that formed (the others keep a majority and one of them
/// leads). `None` before that.
pub(crate) fn silent_target(state: &StateHandle, dead: impl Fn(&str) -> bool) -> Option<String> {
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
    if board.founders.len() < 3 || formed.len() < board.founders.len() {
        return None;
    }
    formed.into_iter().rev().find_map(ip)
}

/// A machine in the simulation.
pub(crate) struct MachineProcess;

impl MachineProcess {
    pub(crate) fn chaotic() -> Self {
        Self
    }
}

#[async_trait]
impl Process for MachineProcess {
    fn name(&self) -> &'static str {
        MACHINE_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        dispatch(
            ctx,
            "every machine process is mapped to the machine role",
            "a machine",
            |role| match role {
                Role::Machine(rank) => Some(rank),
                _ => None,
            },
            |deployment, rank, my_ip| async move {
                Box::pin(run_machine_role(ctx, &deployment, rank, &my_ip)).await
            },
        )
        .await
    }
}

/// The port every simulated machine binds and advertises.
const MACHINE_PORT: u16 = 4500;

const NAMES_KEY: &str = "paros-machine-names";

const OPERATOR_NAMES_KEY: &str = "paros-operator-names";

/// The machines' name table (#257): moonpool's `ScriptedResolver`, which
/// the machines resolve each other's advertised names through, and which a
/// machine that moves repoints. A name a renamed machine left keeps the IP
/// it had, where nobody listens now (#349): its peers reach it only at the
/// name the registry holds.
fn name_table(state: &StateHandle) -> ScriptedResolver {
    crate::state::published_arc(state, NAMES_KEY, ScriptedResolver::new)
        .as_ref()
        .clone()
}

/// The operators' name table (#349): the entry endpoints the workload's
/// clients dial. A rank's name follows its machine wherever it is, renamed
/// or not, as an operator who renames a machine updates its own entry.
fn operator_table(state: &StateHandle) -> ScriptedResolver {
    crate::state::published_arc(state, OPERATOR_NAMES_KEY, ScriptedResolver::new)
        .as_ref()
        .clone()
}

/// How the workload's clients resolve the machines' addresses (#257).
pub(crate) fn names(state: &StateHandle) -> paros::Names {
    paros::Names::new(operator_table(state))
}

/// How the machines resolve each other's addresses (#257, #349).
fn machine_names(state: &StateHandle) -> paros::Names {
    paros::Names::new(name_table(state))
}

/// Point `host` at `ip` in both tables.
fn point(state: &StateHandle, host: &str, ip: std::net::IpAddr) {
    name_table(state).set(host, vec![ip]);
    operator_table(state).set(host, vec![ip]);
}

/// The host a machine of rank `rank` advertises on a seed whose machines
/// advertise names (#257).
fn machine_host(rank: usize) -> String {
    format!("machine-{rank}.paros")
}

/// The host the machine of rank `rank` advertises after its `count`-th
/// rename (#349).
fn renamed_host(rank: usize, count: u8) -> String {
    format!("machine-{rank}-{count}.paros")
}

/// The address the machine of rank `rank` at `ip` advertises: its name on
/// a seed whose machines advertise names, else its literal address.
fn advertised(
    layout: &crate::shape::MachineLayout,
    rank: usize,
    ip: &str,
) -> SimulationResult<Address> {
    let text = if layout.named {
        format!("{}:{MACHINE_PORT}", machine_host(rank))
    } else {
        format!("{ip}:{MACHINE_PORT}")
    };
    Address::parse(&text).map_err(SimulationError::InvalidState)
}

/// The machines' advertised addresses (#257), in rank order: what an
/// operator lists and dials.
pub(crate) fn machine_addrs(
    state: &StateHandle,
    deployment: &Deployment,
) -> SimulationResult<Vec<Address>> {
    let layout = crate::shape::machine_layout(state, deployment.machines().len());
    if layout.named {
        // A name resolves from the start, to the machine's process IP until
        // it boots elsewhere.
        let board = machine_board(state);
        let board = lock(&board);
        let table = name_table(state);
        for (rank, ip) in deployment.machines().iter().enumerate() {
            if !board.listens.contains_key(&rank)
                && let Ok(ip) = ip.parse::<std::net::IpAddr>()
            {
                table.set(&machine_host(rank), vec![ip]);
                operator_table(state).set(&machine_host(rank), vec![ip]);
            }
        }
    }
    deployment
        .machines()
        .iter()
        .enumerate()
        .map(|(rank, ip)| advertised(&layout, rank, ip))
        .collect()
}

/// What a machine's disk holds at its boot, as the harness reads it before
/// the machine starts (#349): it decides whether the machine may come back
/// under a new name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OnDisk {
    /// No record: a new machine (a first boot, or a wipe took the old one).
    Empty,
    /// A record of a machine in a cell, formed or admitted.
    InCell,
    /// A record of an idle machine, or a read that failed.
    Other,
}

/// What the harness reads on the machine's disk before it boots, on a seed
/// whose machines advertise names (`named`); nothing is read on another.
async fn on_disk<S: moonpool_sim::StorageProvider + Clone>(
    disk: &ProviderDisk<S>,
    named: bool,
) -> OnDisk {
    if !named {
        return OnDisk::Other;
    }
    match disk.read_record().await {
        Ok(None) => OnDisk::Empty,
        Ok(Some(text)) => match MachineRecord::parse(&text) {
            Ok(record) if record.formed().is_some() || record.admitted.is_some() => OnDisk::InCell,
            _ => OnDisk::Other,
        },
        Err(_) => OnDisk::Other,
    }
}

/// Whether the machine of rank `rank` may come back under a new name
/// (#349). Until the registry holds the new name, nobody can send to it, so
/// a founding member renamed is a member its peers cannot reach, and the
/// registry's write needs a majority of the others. So: no founding member
/// was wiped, no other one was renamed, and the cell is not of two (each
/// member is the other's majority). A machine outside the founders is in
/// no quorum; it is renamed only while every founding member keeps its
/// name, so the founders its admission names still answer.
fn may_rename(board: &MachineBoard, layout: &crate::shape::MachineLayout, rank: usize) -> bool {
    let founder_renamed = board.renamed.keys().any(|r| *r < layout.founders);
    if rank >= layout.founders {
        return !founder_renamed;
    }
    layout.founders != 2
        && board.wiped.is_empty()
        && board
            .renamed
            .keys()
            .all(|r| *r == rank || *r >= layout.founders)
}

/// The founding members renamed this run and still on their disks (#349):
/// each one's id and the name it advertises now.
pub(crate) fn renamed_founders(state: &StateHandle, founders: usize) -> Vec<(u64, Address)> {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .renamed
        .iter()
        .filter(|(rank, _)| **rank < founders)
        .filter_map(|(rank, name)| {
            let rank_addr =
                Address::parse(&format!("{}:{MACHINE_PORT}", machine_host(*rank))).ok()?;
            let node = *board.nodes.get(&rank_addr)?;
            board
                .voters
                .contains_key(&node)
                .then(|| (node, name.clone()))
        })
        .collect()
}

/// Where the machine of rank `rank` binds at this boot (#257), and the
/// address it advertises (#349): its process IP at its first boot; on a
/// seed whose machines advertise names, a reboot may land at a new address
/// (the run's `move_pct`), as a container that Docker restarts gets a new
/// IP. The name follows the machine: the table is repointed before the
/// machine binds, and its peers resolve the name again after a failed dial.
/// Moonpool routes a connection by the address a listener bound, so a moved
/// machine binds an address of its own outside the topology
/// (`10.250.<rank>.<move>`); the network faults that strike by process IP
/// pass it by while it is there.
///
/// A machine in a cell may also come back under a new name (the run's
/// `rename_pct`, #349): `machine-<rank>-<n>.paros`, at a new address. Its
/// old name keeps the old address, where nobody listens, so its peers reach
/// it only once the cell's registry holds the new name. A machine whose
/// disk is empty is a new one: it advertises its rank's name again.
fn boot_listen(
    state: &StateHandle,
    layout: &crate::shape::MachineLayout,
    rank: usize,
    my_ip: &str,
    disk: OnDisk,
) -> SimulationResult<(std::net::SocketAddr, Address)> {
    let own: std::net::IpAddr = my_ip
        .parse()
        .map_err(|e| SimulationError::InvalidState(format!("bad machine ip {my_ip}: {e}")))?;
    let original = advertised(layout, rank, my_ip)?;
    if !layout.named {
        return Ok((std::net::SocketAddr::new(own, MACHINE_PORT), original));
    }
    let board = machine_board(state);
    let mut board = lock(&board);
    let rebooted = board.listens.contains_key(&rank);
    if disk == OnDisk::Empty && board.renamed.remove(&rank).is_some() {
        assert_reachable!("machine: a wiped renamed machine advertises its rank's name again");
    }
    let forced = board.rename_next == Some(rank);
    if forced {
        board.rename_next = None;
    }
    let renames = rebooted
        && disk == OnDisk::InCell
        && may_rename(&board, layout, rank)
        && (forced || moonpool_sim::sim_random_range(0_u32..100_u32) < layout.rename_pct);
    let moves =
        renames || (rebooted && moonpool_sim::sim_random_range(0_u32..100_u32) < layout.move_pct);
    let ip = if moves {
        let count = board.moves.entry(rank).or_default();
        *count = count.wrapping_add(1).max(1);
        let rank_octet = u8::try_from(rank).unwrap_or(u8::MAX);
        let new_ip = std::net::IpAddr::from([10, 250, rank_octet, *count]);
        assert_reachable!("machine: a machine comes back at a new address");
        tracing::info!(rank, from = %board.listens[&rank], to = %new_ip, "machine_moved");
        new_ip
    } else {
        board.listens.get(&rank).copied().unwrap_or(own)
    };
    board.listens.insert(rank, ip);
    if renames {
        let count = board.renames.entry(rank).or_default();
        *count = count.wrapping_add(1).max(1);
        let host = renamed_host(rank, *count);
        let new_name = Address::parse(&format!("{host}:{MACHINE_PORT}"))
            .map_err(SimulationError::InvalidState)?;
        assert_reachable!("machine: a machine comes back under a new name");
        tracing::info!(rank, %new_name, "machine_renamed");
        board.renamed.insert(rank, new_name);
        point(state, &host, ip);
    } else if board.renamed.contains_key(&rank) {
        let count = board.renames.get(&rank).copied().unwrap_or(1);
        point(state, &renamed_host(rank, count), ip);
    } else {
        name_table(state).set(&machine_host(rank), vec![ip]);
    }
    // The operator's entry for the rank follows the machine (#349).
    operator_table(state).set(&machine_host(rank), vec![ip]);
    let advertise = board.renamed.get(&rank).cloned().unwrap_or(original);
    Ok((std::net::SocketAddr::new(ip, MACHINE_PORT), advertise))
}

/// An operator's slip now and then (#257): a wildcard listen address and no
/// advertised one, which the start refuses. The operator then sets both; a
/// simulated machine binds its own address, never a wildcard (moonpool
/// routes a connection by the address a listener bound).
fn operator_slip() {
    if moonpool_sim::buggify_with_prob!(0.05) {
        let wildcard = std::net::SocketAddr::from(([0, 0, 0, 0], MACHINE_PORT));
        let refused = MachineAddresses::new(wildcard, None);
        assert_always!(
            refused.is_err(),
            "machine: a wildcard listen address with no advertised one is refused"
        );
    }
}

/// The first client id a machine's own client logs under in the control
/// journals' histories (#240): far above the workload's clients.
const MACHINE_CLIENT_BASE: u64 = 1 << 32;

/// One machine: the shipped lifecycle on this process's simulated disk. A
/// process kill aborts it; the next incarnation reads its record back. The
/// one loop here is `parosd`'s supervisor: a run that ended on a storage
/// failure (`parosd` exits 75) is started again on the same disk (moonpool
/// has no restart policy for a process that returns yet, #294).
#[tracing::instrument(level = "debug", skip_all, fields(rank = rank))]
async fn run_machine_role(
    ctx: &SimContext,
    deployment: &Deployment,
    rank: usize,
    my_ip: &str,
) -> SimulationResult<()> {
    let layout = crate::shape::machine_layout(ctx.state(), deployment.machines().len());
    let Some(draw) = layout.machines.get(rank).cloned() else {
        return Err(SimulationError::InvalidState(format!(
            "machine {rank} is outside the layout"
        )));
    };
    let addrs = machine_addrs(ctx.state(), deployment)?;
    let board = machine_board(ctx.state());
    {
        let mut board = lock(&board);
        board.founders = addrs[..layout.founders].iter().cloned().collect();
        board.ips.insert(addrs[rank].clone(), my_ip.to_string());
    }
    let settings = MachineSettings {
        class: draw.class,
        capacity: draw.capacity,
        failure_domain: draw.failure_domain,
        name: format!("machine-{rank}"),
    };
    // Ordered: a crash never leaves a batch ambiguous, which a one-member
    // cell could never repair.
    let store_layout = paros::JournalStoreConfig {
        durability: paros::journal::Durability::Ordered,
        ..crate::shape::journal_layout(ctx.state())
    };
    let disk = || ProviderDisk::new(ctx.storage().clone(), ROOT, store_layout);
    let held = on_disk(&disk(), layout.named).await;
    let (listen, advertise) = boot_listen(ctx.state(), &layout, rank, my_ip, held)?;
    let RoleRig { incarnation, .. } = arm_role(ctx, my_ip);
    let tunables = incarnation.shape.tunables;
    let time = ctx.time().clone();
    let state = ctx.state().clone();
    let audit_addr = addrs[rank].clone();
    let audits = move |scope: AuditScope| -> NodeAudit<SimTimeProvider> {
        match scope {
            AuditScope::Machine => NodeAudit::new(time.clone(), crate::audit::audit_world(&state))
                .on_machines(machine_board(&state), audit_addr.clone()),
            // The coordinator's calls join the control journals' histories
            // (#240), as client `MACHINE_CLIENT_BASE + rank`.
            AuditScope::Node(home) => {
                NodeAudit::new(time.clone(), crate::audit::audit_world_for(&state, home))
                    .on_machines(machine_board(&state), audit_addr.clone())
                    .with_tenants(crate::audit::tenants::tenant_board(&state))
                    .with_calls(Arc::new(
                        crate::chain_workload::system::Announce::of_machine(
                            &state,
                            time.clone(),
                            MACHINE_CLIENT_BASE + rank as u64,
                        ),
                    ))
            }
            AuditScope::Journal(journal) => {
                NodeAudit::new(time.clone(), crate::audit::audit_world_for(&state, journal))
                    .in_journal(journal, journal_board(&state))
                    .with_tenants(crate::audit::tenants::tenant_board(&state))
            }
        }
    };
    operator_slip();
    let addresses =
        MachineAddresses::new(listen, Some(advertise)).map_err(SimulationError::InvalidState)?;
    let names = machine_names(ctx.state());
    loop {
        let ran = Box::pin(paros::machine::run_machine(
            ctx.providers().clone(),
            disk(),
            &audits,
            &settings,
            addresses.clone(),
            names.clone(),
            tunables,
            ctx.shutdown().clone(),
        ))
        .await;
        match ran {
            Ok(()) => return Ok(()),
            Err(MachineError::Storage(_) | MachineError::Run(RunError::Storage(_))) => {
                // The record's write or read, or a store, failed under a
                // storage fault: the machine stops, as `parosd` exits 75,
                // and its supervisor starts it again.
                assert_reachable!("machine: a failed record write stops a machine, which restarts");
            }
            Err(MachineError::Run(RunError::Infra(e))) => return Err(e),
            Err(MachineError::Run(RunError::Refused(refusal))) => {
                assert_always!(
                    false,
                    "machine: a formed machine's store is never refused",
                    { "rank" => rank, "refusal" => format!("{refusal:?}") }
                );
                return Err(SimulationError::InvalidState(format!(
                    "machine {rank} refused a boot: {refusal:?}"
                )));
            }
            Err(error @ (MachineError::Invalid(_) | MachineError::Refused(_))) => {
                assert_always!(
                    false,
                    "machine: a machine's configuration and disk are never refused",
                    { "rank" => rank, "error" => error.to_string() }
                );
                return Err(SimulationError::InvalidState(format!(
                    "machine {rank} refused: {error}"
                )));
            }
        }
    }
}

/// A machine read its record at boot ([`paros::Audit::machine_booted`]):
/// the boot facts that need the run's layout. An empty disk where a record
/// was is a wipe (moonpool's `CrashAndWipe`): the machine there is a new
/// one, and the old one's vote is gone with its disk.
pub(crate) fn booted(board: &Mutex<MachineBoard>, addr: &Address, record: Option<&MachineRecord>) {
    let mut board = lock(board);
    let Some(record) = record else {
        let Some(old) = board.nodes.remove(addr) else {
            return;
        };
        let founder = board.founders.contains(addr);
        let unformed = board.voters.len() < board.founders.len();
        board.wiped.insert((addr.clone(), old));
        // Judge the loss while the wiped machine's vote still names its
        // plan: a wipe of a plan's last voter loses its cell too.
        board.note_lost();
        if board.others.remove(&old).is_some() {
            assert_reachable!("machine: a wipe takes the other cell's vote with its disk");
        }
        if board.voters.remove(&old).is_some() {
            assert_reachable!("machine: a wipe takes a machine's vote with its disk");
        }
        if board.admitted.remove(&old).is_some() {
            assert_reachable!("machine: a wipe takes a machine's admission with its disk");
        }
        if founder && board.init_sent && unformed {
            assert_reachable!("machine: a founding member is wiped during init");
        }
        tracing::info!(%addr, node = old, "machine_wiped");
        return;
    };
    assert_always!(
        board.nodes.get(addr).is_none_or(|node| *node == record.node_id.0),
        "machine: a machine boots with the id it minted",
        { "node" => record.node_id.0 }
    );
    if record.formed().is_none() && record.admitted.is_none() && !board.founders.contains(addr) {
        assert_reachable!("machine: a machine no cell init lists restarts and waits");
    }
}

/// Machine `node` tried to write its cached registry fold (#211,
/// [`paros::Audit::registry_cached`]), `durable` when the write landed: a
/// durable cache only moves forward.
pub(crate) fn registry_cached(
    board: &Mutex<MachineBoard>,
    node: NodeId,
    cache: &CachedRegistry,
    durable: bool,
) {
    let mut board = lock(board);
    assert_always!(
        cache.node == node && cache.check().is_ok(),
        "machine: a machine caches its own checked registry fold",
        { "node" => node.0 }
    );
    board
        .cache_writes
        .entry(node.0)
        .or_default()
        .insert(cache.position, cache.clone());
    if !durable {
        return;
    }
    let before = board.cached.insert(node.0, cache.clone());
    assert_always!(
        before.as_ref().is_none_or(|b| b.position < cache.position),
        "machine: a cached registry fold only moves forward",
        { "node" => node.0, "position" => cache.position }
    );
    let moved = before.is_some_and(|b| {
        b.machines
            .iter()
            .any(|(id, addr)| cache.address(*id).is_some_and(|now| now != addr))
    });
    if moved {
        assert_reachable!("machine: a cached registry fold follows a moved machine");
    }
}

/// Machine `node` booted into its cell with `cache` (#211,
/// [`paros::Audit::registry_cache_read`]): a cache the machine wrote, never
/// another's or one it never offered.
pub(crate) fn registry_cache_read(
    board: &Mutex<MachineBoard>,
    node: NodeId,
    cache: Option<&CachedRegistry>,
) {
    let board = lock(board);
    if let Some(cache) = cache {
        let wrote = board
            .cache_writes
            .get(&node.0)
            .and_then(|writes| writes.get(&cache.position));
        assert_always!(
            wrote == Some(cache),
            "machine: a boot reads a cached registry fold the machine wrote",
            { "node" => node.0, "position" => cache.position }
        );
    }
    let Some(cache) = cache else { return };
    assert_sometimes!(
        board.cached.get(&node.0) == Some(cache),
        "machine: a boot reads the last cached registry fold"
    );
    // The static-stability case: the cache dials a founding member away
    // from the address its plan names, where only the cache knows it is.
    let moved = board.voters.values().any(|plan| {
        plan.members
            .iter()
            .any(|(id, planned)| cache.address(*id).is_some_and(|a| a != planned))
    });
    assert_sometimes!(
        moved,
        "machine: a cached registry fold serves a boot with a moved machine"
    );
}

/// A machine rewrote its record durably
/// ([`paros::Audit::machine_recorded`]), as a reachable each: two proposers'
/// ballots met at one machine (a promise raised over another machine's),
/// and a plan was accepted at a second ballot — a later `cell init`
/// finished what an earlier one proposed (P2c).
pub(crate) fn recorded(board: &Mutex<MachineBoard>, addr: &Address, record: &MachineRecord) {
    let mut board = lock(board);
    let node = record.node_id.0;
    board.nodes.insert(addr.clone(), node);
    if record.promised != Ballot::default() {
        board.first_promiser.get_or_insert(node);
        let before = board.promises.insert(node, record.promised);
        if let Some(before) = before {
            assert_always!(
                before <= record.promised,
                "machine: a recorded decree promise never falls",
                { "node" => node }
            );
            if before.node != record.promised.node {
                assert_reachable!("machine: two cell init ballots meet at one machine");
            }
        }
    }
    if let Some(admission) = &record.admitted {
        // An admission names a cell some `cell init` formed (#216): the
        // operator learned it from `init` or through `Inspect`, never
        // from the harness. A wiped one-founder cell's successor may form
        // since, so every cell a vote ever named counts.
        assert_always!(
            board.votes.contains_key(&admission.cell.cell_id),
            "machine: an admission names a cell init formed",
            { "node" => node, "cell" => admission.cell.cell_id }
        );
        assert_always!(
            record.plan.is_none() && record.promised == Ballot::default(),
            "machine: an admitted machine holds no vote and no promise",
            { "node" => node }
        );
        if board.admitted.insert(node, admission.cell).is_none() {
            assert_reachable!("machine: a machine is admitted into the cell");
        }
    }
    if let Some((ballot, plan)) = &record.plan
        && is_other_cell(&board, addr, plan)
    {
        // The other cell an operator founded (#216): never the run's.
        let cell = board.voters.values().next().map(|plan| plan.cell_id);
        assert_always!(
            cell != Some(plan.cell_id),
            "machine: the other cell is never the run's cell",
            { "node" => node, "cell" => plan.cell_id }
        );
        if board.others.insert(node, plan.clone()).is_none() {
            assert_reachable!(
                "machine: an operator founds another cell on a wiped member's address"
            );
        }
        board.votes.entry(plan.cell_id).or_default().insert(*ballot);
        return;
    }
    if let Some((ballot, plan)) = &record.plan {
        // The vote is the commit point, never the format before it: a
        // machine that crashed between the two never accepted that plan, and
        // a later `cell init` may draw another (#277). Every durable vote
        // names the one plan: a formed machine never votes again, and every
        // member answers Phase 1, so every later ballot adopts it. A wiped
        // machine's vote went with its disk, so only the votes still on a
        // disk are compared.
        let cell = board
            .voters
            .values()
            .next()
            .map_or(plan.control_journals(), CellPlan::control_journals);
        assert_always!(
            cell == plan.control_journals(),
            "machine: every machine forms the one cell init drew",
            { "node" => node, "cell" => plan.cell_id, "first" => cell.cell_id }
        );
        if plan.members.iter().any(|(id, addr)| {
            board
                .wiped
                .iter()
                .any(|(at, old)| at == addr && *old != id.0)
        }) {
            assert_reachable!(
                "machine: a cell forms over the machine that replaced a wiped founder"
            );
        }
        if wiped_members(&board, plan) > 0 {
            assert_reachable!("machine: a cell forms around a wiped founder's old id");
        }
        board.voters.insert(node, plan.clone());
        board.note_lost();
        if board.formed.insert(node) && board.formed.len() > 1 {
            assert_reachable!("machine: a cell forms over several seeds");
        }
        let ballots = board.votes.entry(plan.cell_id).or_default();
        if ballots.insert(*ballot) && ballots.len() > 1 {
            assert_reachable!("machine: a later ballot finishes the plan an earlier one proposed");
        }
    }
}

/// A machine is about to format `plan`'s stores
/// ([`paros::Audit::cell_formatting`]): only after an operator's `init`,
/// only on a founding member, and over exactly the founders.
pub(crate) fn formatting(
    board: &Mutex<MachineBoard>,
    addr: &Address,
    node: NodeId,
    plan: &CellPlan,
) {
    let mut board = lock(board);
    board.elections.insert(plan.election);
    if is_other_cell(&board, addr, plan) {
        return;
    }
    assert_always!(
        board.init_sent,
        "machine: no cell forms without init",
        { "node" => node.0, "cell" => plan.cell_id }
    );
    assert_always!(
        board.founders.contains(addr),
        "machine: only a founding member forms",
        { "node" => node.0, "cell" => plan.cell_id }
    );
    assert_always!(
        plan.addrs() == board.founders,
        "machine: a cell forms over the founding members init listed",
        { "node" => node.0, "members" => plan.members.len(), "founders" => board.founders.len() }
    );
}
