//! The machines in the simulation (#246): every process of the
//! [`MACHINE_GROUP`] is a `parosd`, running the shipped
//! [`paros::machine::run_machine`] from an empty simulated disk — format (a
//! minted `node_id`), the wait, then the cell's journals — under the run's
//! driver hooks and every fault the campaign draws. All of paros runs in the
//! simulation, setup included (decided on 2026-10-09): no cell is handed to
//! a machine; the workload's operators form it with `init`
//! (`paros::client::initialize`, the code `parosctl init` prints) against
//! the layout the seed drew ([`crate::shape::machine_layout`]).
//!
//! The disk is the library's [`ProviderDisk`] over moonpool's simulated
//! disk — the record's write protocol `parosd` ships — and the stores are
//! `JournalStorage`, every one an existing member's once formed, as
//! `parosd`'s are. They store ordered, outside the ledgered injector and the
//! power cut (the acceptors' fault model): a one-seed cell has no second
//! copy to repair a torn batch from.
//!
//! The [`MachineBoard`] holds the run's facts about its machines that the
//! oracles judge: whether an operator sent `init` (no cell forms without
//! one, §3.1), and the one cell every formation names.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimStorageProvider, SimTimeProvider, SimulationError, SimulationResult,
    StateHandle, assert_always, assert_reachable,
};
use paros::machine::{
    CellPlan, ControlJournals, MachineDisk, MachineError, MachineRecord, MachineSettings,
    ProviderDisk,
};
use paros::{BootKind, Config, JournalIdentifier, JournalStorage, JournalStores, NodeId, RunError};

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
    /// The cell the first formation named: every later one names it too.
    cell: Option<ControlJournals>,
    /// The machines that formed, by minted id.
    formed: BTreeSet<u64>,
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

/// One machine: the shipped lifecycle on this process's simulated disk, in
/// the seam-crash recovery loop every role with a disk runs. A process kill
/// aborts it; the next incarnation reads its record back.
#[tracing::instrument(level = "debug", skip_all, fields(rank))]
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
    let rendezvous = addrs[..layout.seeds]
        .iter()
        .map(SocketAddr::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let settings = MachineSettings {
        class: draw.class,
        capacity: draw.capacity,
        failure_domain: draw.failure_domain,
        rendezvous: Some(rendezvous),
    };
    let addr = machine_addr(my_ip)?;
    let RoleRig {
        incarnation, hooks, ..
    } = arm_role(ctx, my_ip);
    let tunables = incarnation.shape.tunables;
    // Ordered: a crash never leaves a batch ambiguous, which a one-seed
    // cell could never repair.
    let store_layout = paros::JournalStoreConfig {
        durability: paros::journal::Durability::Ordered,
        ..crate::shape::journal_layout(ctx.state())
    };
    loop {
        let disk = SimDisk {
            ctx,
            disk: ProviderDisk::new(ctx.storage().clone(), ROOT, store_layout),
        };
        note_boot(&disk.disk).await;
        let ran = Box::pin(paros::machine::run_machine(
            ctx.providers().clone(),
            disk,
            &settings,
            addr,
            resolve,
            layout.assignment,
            tunables,
            ctx.shutdown().clone(),
            &hooks,
        ))
        .await;
        match ran {
            Ok(()) => return Ok(()),
            Err(MachineError::Storage(_)) => {
                // The record's write or read failed under a storage fault:
                // the machine stops, as `parosd` exits 75, and restarts.
                assert_reachable!("machine: a failed record write stops a machine, which restarts");
                crate::process::restart_delay!(
                    ctx,
                    "a machine whose record write failed restarts after a buggified delay"
                );
            }
            Err(MachineError::Run(RunError::SeamCrash(_) | RunError::Storage(_))) => {
                crate::process::restart_delay!(
                    ctx,
                    "a seam-crashed machine restarts after a buggified delay"
                );
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

/// The rendezvous join list, resolved: the sim's are addresses already.
fn resolve(text: &str) -> Result<Vec<SocketAddr>, String> {
    text.split(',')
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.parse().map_err(|e| format!("{entry}: {e}")))
        .collect()
}

/// What this incarnation boots from, as a reachable each: an empty disk, a
/// record still waiting for its cell, or a formed one about to serve it.
async fn note_boot(disk: &ProviderDisk<SimStorageProvider>) {
    match disk.read_record().await {
        Ok(None) => assert_reachable!("machine: a machine formats an empty disk"),
        Ok(Some(text)) => match MachineRecord::parse(&text).map(|record| record.formed().is_some())
        {
            Ok(true) => assert_reachable!("machine: a formed machine restarts and serves its cell"),
            Ok(false) => assert_reachable!("machine: a formatted machine restarts and waits"),
            Err(_) => {}
        },
        Err(_) => {}
    }
}

/// A machine's [`MachineDisk`] on its simulated disk: the library's
/// [`ProviderDisk`], and stores every one an existing member's.
struct SimDisk<'a> {
    ctx: &'a SimContext,
    disk: ProviderDisk<SimStorageProvider>,
}

impl<'a> MachineDisk for SimDisk<'a> {
    type Stores = SimMachineStores<'a>;

    async fn read_record(&mut self) -> Result<Option<String>, String> {
        self.disk.read_record().await
    }

    async fn write_record(&mut self, text: &str) -> Result<(), String> {
        self.disk.write_record(text).await
    }

    async fn holds_stores(&mut self) -> bool {
        self.disk.holds_journals().await
    }

    async fn provision(&mut self, node_id: NodeId, plan: &CellPlan) -> Result<(), String> {
        let state = self.ctx.state();
        {
            let board = machine_board(state);
            let mut board = lock(&board);
            assert_always!(
                board.init_sent,
                "machine: no cell forms without init",
                { "node" => node_id.0, "cell" => plan.cell_id }
            );
            let cell = *board.cell.get_or_insert(plan.control_journals());
            assert_always!(
                cell == plan.control_journals(),
                "machine: every machine forms the one cell init drew",
                { "node" => node_id.0, "cell" => plan.cell_id, "first" => cell.cell_id }
            );
            if board.formed.insert(node_id.0) && board.formed.len() > 1 {
                assert_reachable!("machine: a cell forms over several seeds");
            }
        }
        self.disk.format(node_id, plan).await?;
        assert_reachable!("machine: a seed formats its cell's journals");
        Ok(())
    }

    async fn stores(
        &mut self,
        node_id: NodeId,
        genesis: BTreeMap<JournalIdentifier, Config>,
    ) -> Result<SimMachineStores<'a>, String> {
        assert!(!genesis.is_empty(), "a formed machine serves journals");
        Ok(SimMachineStores {
            ctx: self.ctx,
            disk: self.disk.clone(),
            node: node_id,
            genesis,
        })
    }
}

/// A formed machine's stores: one `JournalStorage` per journal of its plan,
/// each an existing member's (the formation formatted them), as `parosd`'s.
pub(crate) struct SimMachineStores<'a> {
    ctx: &'a SimContext,
    disk: ProviderDisk<SimStorageProvider>,
    node: NodeId,
    genesis: BTreeMap<JournalIdentifier, Config>,
}

impl JournalStores for SimMachineStores<'_> {
    type Store = JournalStorage<SimStorageProvider>;
    type Audit = NodeAudit<SimTimeProvider>;

    fn journals(&self) -> Vec<JournalIdentifier> {
        self.genesis.keys().copied().collect()
    }

    async fn open(&mut self, journal: JournalIdentifier) -> Option<(Self::Store, BootKind)> {
        let config = self.genesis.get(&journal)?.clone();
        assert_eq!(config.id, self.node, "a store is opened as its machine");
        Some((
            JournalStorage::new(
                self.disk.provider().clone(),
                self.disk.journal_dir(journal),
                config,
                self.disk.layout(),
            ),
            BootKind::ExistingMember,
        ))
    }

    fn audit(&self, journal: JournalIdentifier) -> Self::Audit {
        NodeAudit::new(
            self.ctx.time().clone(),
            crate::audit::audit_world_for(self.ctx.state(), journal),
        )
        .in_journal(journal, journal_board(self.ctx.state()))
    }

    /// The node's own facts report to its first journal's audit world (the
    /// plan's lowest identifier), on no journal's board.
    fn node_audit(&self) -> Self::Audit {
        let first = self
            .genesis
            .keys()
            .next()
            .copied()
            .expect("a formed machine serves journals");
        NodeAudit::new(
            self.ctx.time().clone(),
            crate::audit::audit_world_for(self.ctx.state(), first),
        )
    }
}
