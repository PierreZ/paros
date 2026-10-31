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

mod board;
mod detector;
mod heal;
mod names;
mod reports;

pub(crate) use board::{
    Landed, MachineBoard, MaintenanceNote, Replacement, admitted_ids, admitted_into, cached_mover,
    cell_lost, downed_target, formed_cell, founder_down_through, founder_in_other_cell,
    founder_to_move, founder_wiped, gone_down, gone_requested, gone_target, governed, init_sent,
    is_admitted, is_election, is_founder, is_other_founder, journal_created, lock, machine_board,
    maintenance_notes, minted_ids, note_downed, note_drain_asked, note_init_sent,
    note_journal_created, note_maintenance, note_other_cell, note_remove_asked, note_removed,
    note_second_cell, note_window_answered, note_window_cut, note_window_sent, note_windows_ended,
    note_zone_down, other_cell_target, other_cells_wiped, plan_lost, process_ip, remove_asked,
    removed_ids, renamed_ids, replace_target, replacement, request_gone, second_cell_alive,
    second_cell_founded, second_cell_target, silent_target, slow_target, was_wiped, wipe_target,
    wiped_ids, zone_down_through, zone_of, zone_targets,
};
pub(crate) use detector::{judge_pending, judge_sent, live_since, note_slowed, slowed_after};
pub(crate) use heal::{heal_wiped, registry_folded, registry_restored};
pub(crate) use names::{
    boot_listen, machine_addrs, machine_names, names, operator_slip, renamed_admitted,
    renamed_founders,
};
pub(crate) use reports::{
    Ended, booted, check_metrics, formatting, machine_audits, metered_answer, metered_call,
    recorded, registry_cache_read, registry_cached,
};

use crate::process::{RoleRig, arm_role, dispatch};
use crate::roles::{Deployment, MACHINE_GROUP, Role};
use async_trait::async_trait;
use moonpool_sim::TimeProvider as _;
use moonpool_sim::{
    Process, SimContext, SimulationError, SimulationResult, TaskProvider, assert_always,
    assert_reachable,
};
use paros::machine::{
    MachineAddresses, MachineError, MachineRecord, MachineSettings, ProviderDisk,
};
use paros::roles::node::RunError;

/// Where a machine's disk lives on its simulated disk.
const ROOT: &str = "paros/machine";

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
        ctx.run_local(self.name(), |ctx| async move {
            let ctx = &ctx;
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
        })
        .await
    }
}

/// The metrics machine `ctx` records on (#366 (metrics)): the ones the
/// board keeps for its address, unless the seed runs its machines without
/// metrics ([`crate::shape::unmetered`]).
fn machine_metrics(ctx: &SimContext) -> paros::observe::metrics::Metrics {
    if crate::shape::unmetered(ctx.state()) {
        assert_reachable!("metrics: the machines run without metrics");
        return paros::observe::metrics::Metrics::off();
    }
    let board = machine_board(ctx.state());
    lock(&board)
        .metrics
        .entry(ctx.my_ip().to_string())
        .or_insert_with(paros::observe::metrics::Metrics::on)
        .clone()
}

/// What a machine's disk holds at its boot, as the harness reads it before
/// the machine starts (#349): it decides whether the machine may come back
/// under a new name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OnDisk {
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
        board
            .zones
            .insert(addrs[rank].clone(), draw.locality.zone.clone());
    }
    let settings = MachineSettings {
        class: draw.class,
        capacity: draw.capacity,
        locality: draw.locality.clone(),
        name: format!("machine-{rank}"),
        pin: crate::frontend::machine_pin(ctx.state()),
        metrics: machine_metrics(ctx),
        frontend: Some(crate::frontend::machine_frontend(ctx)?),
    };
    // Ordered: a crash never leaves a batch ambiguous, which a one-member
    // cell could never repair. A journal a tenant created stores under the
    // mode its create recorded instead (#599 (durability mode per journal)).
    let store_layout = paros::storage::journal::JournalStoreConfig {
        durability: paros::storage::journal::Durability::Ordered,
        ..crate::shape::journal_layout(ctx.state())
    };
    let disk = || ProviderDisk::new(ctx.storage().clone(), ROOT, store_layout);
    let held = on_disk(&disk(), layout.named).await;
    let (listen, advertise) = boot_listen(ctx.state(), &layout, rank, my_ip, held)?;
    let RoleRig { incarnation, .. } = arm_role(ctx, my_ip);
    let tunables = incarnation.shape.tunables;
    let audits = machine_audits(ctx, (addrs[rank].clone(), my_ip), (rank, &draw));
    operator_slip();
    let addresses =
        MachineAddresses::new(listen, Some(advertise)).map_err(SimulationError::InvalidState)?;
    let names = machine_names(ctx.state());
    if crate::shape::cold_rot(ctx.state()) {
        // The cold-rot scenario (#362 (background scrubbing)): the disk
        // rots a cold record at rest beside the running machine.
        ctx.task()
            .spawn_task(
                "paros-cold-rot",
                crate::world::scenarios::cold_rot::run(
                    ctx.storage().clone(),
                    ctx.time().clone(),
                    ctx.state().clone(),
                    (ROOT, store_layout, rank),
                    ctx.shutdown().clone(),
                ),
            )
            .detach();
    }
    // The process's lifecycle, for the failure detector's oracles (#654
    // (live machine marked down)): a kill drops this task, and the guard
    // with it.
    let _ended = Ended::new(ctx, &addrs[rank]);
    loop {
        detector::note_started(ctx.state(), &addrs[rank], ctx.time().now());
        let ran = Box::pin(paros::machine::run_machine(
            ctx.providers().clone(),
            disk(),
            audits.clone(),
            &settings,
            addresses.clone(),
            names.clone(),
            tunables,
            crate::tls::serving(ctx, paros::net::tls::Role::Machine, ctx.my_ip())?,
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
