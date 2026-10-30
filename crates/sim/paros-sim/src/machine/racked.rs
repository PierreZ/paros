//! **Machines racked in the middle of a run** (#490 (second cell mid-run),
//! moonpool#327 (deployment changes mid-run)): an operator starts a new
//! process group of machines with `ctx.deployment().spawn_group`, then
//! forms them into a cell with `cell init`, under the run's chaos. Each is
//! a `parosd` running the shipped [`paros::machine::run_machine`] from an
//! empty disk, as a layout machine is ([`super::run_machine_role`]).
//!
//! A racked machine is outside the seed's layout: it has no rank in
//! [`crate::shape::MachineLayout`], it listens and advertises at its own
//! IP, and the machine board's oracles do not model it (its audit is not
//! `on_machines`). The cell's own oracles (the registry folds, the journal
//! histories) judge it as any other machine.

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationError, SimulationResult, assert_always, assert_reachable,
};
use paros::machine::{MachineAddresses, MachineError, MachineSettings, ProviderDisk};
use paros::model::address::Address;
use paros::roles::node::RunError;

use super::names::MACHINE_PORT;
use super::{ROOT, machine_audits, machine_metrics, machine_names};

/// The process group of the second cell's racked machines (#490 (second
/// cell mid-run)).
pub(crate) const CELL_TWO_GROUP: &str = "paros-cell-2";

/// How many machines the second cell racks: 1 to 3, a draw per run.
pub(crate) const CELL_TWO_RANGE: std::ops::RangeInclusive<usize> = 1..=3;

/// Where a racked machine's rank starts, for its audit's client ids: far
/// above the layout's ranks, so its coordinator's calls never alias a
/// layout machine's in the control journals' histories.
const RACKED_RANK_BASE: usize = 1 << 16;

/// The address a racked machine at `ip` listens and advertises at.
///
/// # Errors
///
/// An `ip` that is not an address.
pub(crate) fn racked_address(ip: &str) -> SimulationResult<Address> {
    Address::parse(&format!("{ip}:{MACHINE_PORT}"))
        .map_err(|e| SimulationError::InvalidState(format!("bad racked address {ip}: {e}")))
}

/// A machine racked in the middle of a run, a storage machine. Its rank
/// among the racked machines is its host number in its group's `/24`, so a
/// seed replays the same alone or inside a sweep.
pub(crate) struct RackedMachineProcess;

impl RackedMachineProcess {
    pub(crate) fn racked() -> Self {
        Self
    }
}

#[async_trait]
impl Process for RackedMachineProcess {
    fn name(&self) -> &'static str {
        CELL_TWO_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        ctx.run_local(self.name(), |ctx| async move {
            Box::pin(run_racked(&ctx)).await
        })
        .await
    }
}

/// One racked machine: the shipped lifecycle on this process's simulated
/// disk, started again after a storage failure, as `parosd`'s supervisor
/// does.
async fn run_racked(ctx: &SimContext) -> SimulationResult<()> {
    let my_ip = ctx.my_ip().to_string();
    let advertise = racked_address(&my_ip)?;
    let own: std::net::IpAddr = my_ip
        .parse()
        .map_err(|e| SimulationError::InvalidState(format!("bad machine ip {my_ip}: {e}")))?;
    let listen = std::net::SocketAddr::new(own, MACHINE_PORT);
    let rank = match own {
        std::net::IpAddr::V4(v4) => usize::from(v4.octets()[3]),
        std::net::IpAddr::V6(_) => 0,
    };
    let draw = crate::shape::MachineDraw {
        class: paros::machine::Class::Storage,
        capacity: 4,
        locality: paros::machine::Locality::zone(format!("zone-{}", rank % 3)),
    };
    let settings = MachineSettings {
        class: draw.class,
        capacity: draw.capacity,
        locality: draw.locality.clone(),
        name: format!("racked-{rank}"),
        pin: crate::frontend::machine_pin(ctx.state()),
        metrics: machine_metrics(ctx),
        frontend: Some(crate::frontend::machine_frontend(ctx)?),
    };
    let store_layout = paros::storage::journal::JournalStoreConfig {
        durability: paros::storage::journal::Durability::Ordered,
        ..crate::shape::journal_layout(ctx.state())
    };
    let disk = || ProviderDisk::new(ctx.storage().clone(), ROOT, store_layout);
    let incarnation = crate::shape::boot(ctx.state(), &my_ip);
    let tunables = incarnation.shape.tunables;
    let audits = machine_audits(
        ctx,
        (advertise.clone(), &my_ip, false),
        (RACKED_RANK_BASE + rank, &draw),
    );
    let addresses = MachineAddresses::new(listen, Some(advertise.clone()))
        .map_err(SimulationError::InvalidState)?;
    assert_reachable!("machine: a machine racked in the middle of a run boots");
    loop {
        let ran = Box::pin(paros::machine::run_machine(
            ctx.providers().clone(),
            disk(),
            audits.clone(),
            &settings,
            addresses.clone(),
            machine_names(ctx.state()),
            tunables,
            crate::tls::serving(ctx, paros::net::tls::Role::Machine, ctx.my_ip())?,
            ctx.shutdown().clone(),
        ))
        .await;
        match ran {
            Ok(()) => return Ok(()),
            Err(MachineError::Storage(_) | MachineError::Run(RunError::Storage(_))) => {}
            Err(MachineError::Run(RunError::Infra(e))) => return Err(e),
            Err(error) => {
                assert_always!(
                    false,
                    "machine: a racked machine's configuration and disk are never refused",
                    { "rank" => rank, "error" => error.to_string() }
                );
                return Err(SimulationError::InvalidState(format!(
                    "racked machine {rank} refused: {error}"
                )));
            }
        }
    }
}
