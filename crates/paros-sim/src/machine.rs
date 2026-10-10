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
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimTimeProvider, SimulationError, SimulationResult, StateHandle,
    assert_always, assert_reachable,
};
use paros::machine::{
    AuditScope, CellPlan, ControlJournals, MachineError, MachineRecord, MachineSettings,
    ProviderDisk,
};
use paros::{Ballot, NodeId, RunError};

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
    /// The founding members the run's `cell init` lists (the layout's).
    founders: BTreeSet<SocketAddr>,
    /// Every durable vote of a machine still on its disk, by minted id: all
    /// of them name one cell. A wiped machine's vote is gone with its disk.
    voters: BTreeMap<u64, CellPlan>,
    /// The machines that formed, by minted id.
    formed: BTreeSet<u64>,
    /// Each machine's minted id, by address: the record on its disk now.
    nodes: BTreeMap<SocketAddr, u64>,
    /// The machines a wipe replaced: the address and the id it held.
    wiped: BTreeSet<(SocketAddr, u64)>,
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
                n - wiped_members(self, plan) < n / 2 + 1
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

/// How many of `plan`'s members the board saw wiped.
fn wiped_members(board: &MachineBoard, plan: &CellPlan) -> usize {
    plan.members
        .iter()
        .filter(|(id, addr)| board.wiped.contains(&(*addr, id.0)))
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
pub(crate) fn is_admitted(state: &StateHandle, addr: SocketAddr) -> bool {
    let board = machine_board(state);
    let board = lock(&board);
    board
        .nodes
        .get(&addr)
        .is_some_and(|node| board.admitted.contains_key(node))
}

/// Whether a wipe replaced the machine at `addr` this run: an answer it sent
/// before the wipe may name a disk that is gone.
pub(crate) fn was_wiped(state: &StateHandle, addr: SocketAddr) -> bool {
    lock(&machine_board(state))
        .wiped
        .iter()
        .any(|(wiped, _)| *wiped == addr)
}

/// Whether the machine at `addr` is a founding member `init` lists.
pub(crate) fn is_founder(state: &StateHandle, addr: SocketAddr) -> bool {
    lock(&machine_board(state)).founders.contains(&addr)
}

/// Whether any machine was wiped this run: the one way a machine can hold
/// another cell than the operator's.
pub(crate) fn founder_wiped(state: &StateHandle) -> bool {
    !lock(&machine_board(state)).wiped.is_empty()
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

/// The founding member the wiped-founder scenario wipes now
/// (`crate::world::wiped_founder`), if the run is at its moment: an operator
/// sent `init`, and either every founder promised and none voted
/// (`after_vote` false: a founder other than `cell init`'s receiver), or a
/// founder voted and another did not (`after_vote` true: that one). Only a
/// live founder whose minted id the board knows.
pub(crate) fn wipe_target(
    state: &StateHandle,
    after_vote: bool,
    dead: impl Fn(&str) -> bool,
) -> Option<SocketAddr> {
    let board = machine_board(state);
    let board = lock(&board);
    if !board.init_sent || !board.wiped.is_empty() {
        return None;
    }
    let mut live = board.founders.iter().filter_map(|addr| {
        let node = *board.nodes.get(addr)?;
        (!dead(&addr.ip().to_string())).then_some((*addr, node))
    });
    if after_vote {
        if board.voters.is_empty() {
            return None;
        }
        live.find(|(_, node)| !board.voters.contains_key(node))
            .map(|(addr, _)| addr)
    } else {
        if !board.voters.is_empty() {
            return None;
        }
        // Once every founder promised, a founder other than the receiver:
        // the receiver's decree goes on and forms the others, so the old
        // machine's vote outlives it (on a one-founder cell, the founder).
        let promised: Vec<(SocketAddr, u64)> = live
            .filter(|(_, node)| board.promises.contains_key(node))
            .collect();
        if promised.len() < board.founders.len() {
            return None;
        }
        promised
            .iter()
            .rev()
            .find(|(_, node)| board.founders.len() == 1 || board.first_promiser != Some(*node))
            .map(|(addr, _)| *addr)
    }
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

/// Parse `ip` (the topology's, port-less) into the address a machine serves.
fn machine_addr(ip: &str) -> SimulationResult<SocketAddr> {
    paros::parse_addr(ip)?
        .parse()
        .map_err(|e| SimulationError::InvalidState(format!("bad address: {e}")))
}

/// The machines' addresses, in rank order.
pub(crate) fn machine_addrs(deployment: &Deployment) -> SimulationResult<Vec<SocketAddr>> {
    deployment
        .machines()
        .iter()
        .map(|ip| machine_addr(ip))
        .collect()
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
    let addrs = machine_addrs(deployment)?;
    let board = machine_board(ctx.state());
    lock(&board).founders = addrs[..layout.founders].iter().copied().collect();
    let settings = MachineSettings {
        class: draw.class,
        capacity: draw.capacity,
        failure_domain: draw.failure_domain,
    };
    let addr = machine_addr(my_ip)?;
    // Ordered: a crash never leaves a batch ambiguous, which a one-member
    // cell could never repair.
    let store_layout = paros::JournalStoreConfig {
        durability: paros::journal::Durability::Ordered,
        ..crate::shape::journal_layout(ctx.state())
    };
    let disk = || ProviderDisk::new(ctx.storage().clone(), ROOT, store_layout);
    let RoleRig { incarnation, .. } = arm_role(ctx, my_ip);
    let tunables = incarnation.shape.tunables;
    let time = ctx.time().clone();
    let state = ctx.state().clone();
    let audits = move |scope: AuditScope| -> NodeAudit<SimTimeProvider> {
        match scope {
            AuditScope::Machine => NodeAudit::new(time.clone(), crate::audit::audit_world(&state))
                .on_machines(machine_board(&state), addr),
            // The coordinator's calls join the control journals' histories
            // (#240), as client `MACHINE_CLIENT_BASE + rank`.
            AuditScope::Node(home) => {
                NodeAudit::new(time.clone(), crate::audit::audit_world_for(&state, home))
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
            }
        }
    };
    loop {
        let ran = Box::pin(paros::machine::run_machine(
            ctx.providers().clone(),
            disk(),
            &audits,
            &settings,
            addr,
            layout.assignment,
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
pub(crate) fn booted(
    board: &Mutex<MachineBoard>,
    addr: SocketAddr,
    record: Option<&MachineRecord>,
) {
    let mut board = lock(board);
    let Some(record) = record else {
        let Some(old) = board.nodes.remove(&addr) else {
            return;
        };
        let founder = board.founders.contains(&addr);
        let unformed = board.voters.len() < board.founders.len();
        board.wiped.insert((addr, old));
        if board.voters.remove(&old).is_some() {
            assert_reachable!("machine: a wipe takes a machine's vote with its disk");
        }
        board.note_lost();
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
        board.nodes.get(&addr).is_none_or(|node| *node == record.node_id.0),
        "machine: a machine boots with the id it minted",
        { "node" => record.node_id.0 }
    );
    if record.formed().is_none() && record.admitted.is_none() && !board.founders.contains(&addr) {
        assert_reachable!("machine: a machine no cell init lists restarts and waits");
    }
}

/// A machine rewrote its record durably
/// ([`paros::Audit::machine_recorded`]), as a reachable each: two proposers'
/// ballots met at one machine (a promise raised over another machine's),
/// and a plan was accepted at a second ballot — a later `cell init`
/// finished what an earlier one proposed (P2c).
pub(crate) fn recorded(board: &Mutex<MachineBoard>, addr: SocketAddr, record: &MachineRecord) {
    let mut board = lock(board);
    let node = record.node_id.0;
    board.nodes.insert(addr, node);
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
    addr: SocketAddr,
    node: NodeId,
    plan: &CellPlan,
) {
    let mut board = lock(board);
    board.elections.insert(plan.election);
    assert_always!(
        board.init_sent,
        "machine: no cell forms without init",
        { "node" => node.0, "cell" => plan.cell_id }
    );
    assert_always!(
        board.founders.contains(&addr),
        "machine: only a founding member forms",
        { "node" => node.0, "cell" => plan.cell_id }
    );
    assert_always!(
        plan.addrs() == board.founders,
        "machine: a cell forms over the founding members init listed",
        { "node" => node.0, "members" => plan.members.len(), "founders" => board.founders.len() }
    );
}
