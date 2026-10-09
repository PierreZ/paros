//! The machines in the simulation (#246): every process of the
//! [`MACHINE_GROUP`] is a `parosd`, running the shipped
//! [`paros::machine::run_machine`] from an empty simulated disk — format (a
//! minted `node_id`), the wait, then the cell's journals — under the run's
//! driver hooks and every fault the campaign draws. All of paros runs in the
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
    /// The cell the first durable vote named: every later one names it too.
    cell: Option<ControlJournals>,
    /// The machines that formed, by minted id.
    formed: BTreeSet<u64>,
    /// Each machine's last recorded promise in the cell decree, by minted id.
    promises: BTreeMap<u64, Ballot>,
    /// The ballots each plan was accepted at, by cell id.
    votes: BTreeMap<u64, BTreeSet<Ballot>>,
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

/// The cell the machines formed, once one did.
pub(crate) fn formed_cell(state: &StateHandle) -> Option<ControlJournals> {
    lock(&machine_board(state)).cell
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
    let RoleRig {
        incarnation, hooks, ..
    } = arm_role(ctx, my_ip);
    let tunables = incarnation.shape.tunables;
    // Ordered: a crash never leaves a batch ambiguous, which a one-member
    // cell could never repair.
    let store_layout = paros::JournalStoreConfig {
        durability: paros::journal::Durability::Ordered,
        ..crate::shape::journal_layout(ctx.state())
    };
    let time = ctx.time().clone();
    let state = ctx.state().clone();
    let audits = move |scope: AuditScope| -> NodeAudit<SimTimeProvider> {
        match scope {
            AuditScope::Machine => NodeAudit::new(time.clone(), crate::audit::audit_world(&state))
                .on_machines(machine_board(&state)),
            AuditScope::Node(home) => {
                NodeAudit::new(time.clone(), crate::audit::audit_world_for(&state, home))
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
            ProviderDisk::new(ctx.storage().clone(), ROOT, store_layout),
            &audits,
            &settings,
            addr,
            layout.assignment,
            tunables,
            ctx.shutdown().clone(),
            &hooks,
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
/// the one boot fact that needs the run's layout.
pub(crate) fn booted(
    board: &Mutex<MachineBoard>,
    addr: SocketAddr,
    record: Option<&MachineRecord>,
) {
    let Some(record) = record else {
        return;
    };
    if record.formed().is_none() && !lock(board).founders.contains(&addr) {
        assert_reachable!("machine: a machine no cell init lists restarts and waits");
    }
}

/// A machine rewrote its record durably
/// ([`paros::Audit::machine_recorded`]), as a reachable each: two proposers'
/// ballots met at one machine (a promise raised over another machine's),
/// and a plan was accepted at a second ballot — a later `cell init`
/// finished what an earlier one proposed (P2c).
pub(crate) fn recorded(board: &Mutex<MachineBoard>, record: &MachineRecord) {
    let mut board = lock(board);
    let node = record.node_id.0;
    if record.promised != Ballot::default() {
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
    if let Some((ballot, plan)) = &record.plan {
        // The vote is the commit point, never the format before it: a
        // machine that crashed between the two never accepted that plan, and
        // a later `cell init` may draw another (#277). Every durable vote
        // names the one plan: a formed machine never votes again, and both
        // quorums are every member.
        let cell = *board.cell.get_or_insert(plan.control_journals());
        assert_always!(
            cell == plan.control_journals(),
            "machine: every machine forms the one cell init drew",
            { "node" => node, "cell" => plan.cell_id, "first" => cell.cell_id }
        );
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
    let board = lock(board);
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
