//! The machines: each process of the [`MACHINE_GROUP`] is a `parosd`, the
//! shipped [`paros::machine::run_machine`] on moonpool's simulated disk,
//! network and clock, from an empty disk. Nothing here wraps the shipped
//! code: the machine is the one `parosd` runs.

use async_trait::async_trait;
use moonpool_sim::{Process, SimContext, SimulationError, SimulationResult};
use paros::machine::{Class, MachineAddresses, MachineError, MachineSettings, ProviderDisk};
use paros::{Address, DriverTunables, Names, NoAudit, RunError, Transport};

/// The process group of the machines, `10.0.1.{1..}` in index order.
pub const MACHINE_GROUP: &str = "paros-machine";

/// The port every machine binds and advertises.
pub const MACHINE_PORT: u16 = 4500;

/// Where a machine's disk lives on its simulated disk.
const ROOT: &str = "paros/machine";

/// The IP of machine `index`.
#[must_use]
pub fn machine_ip(index: usize) -> String {
    format!("10.0.1.{}", index + 1)
}

/// The advertised address of machine `index`.
///
/// # Panics
///
/// Never: the address is a literal socket address.
#[must_use]
pub fn machine_addr(index: usize) -> Address {
    Address::parse(&format!("{}:{MACHINE_PORT}", machine_ip(index)))
        .expect("a literal socket address parses")
}

/// The zone (failure domain) of machine `index`: three zones, round robin.
#[must_use]
pub fn zone_of(index: usize) -> String {
    format!("zone-{}", index % 3)
}

/// The index of the machine at `ip`, when it is one.
#[must_use]
pub fn machine_index(ip: &str) -> Option<usize> {
    let last = ip.strip_prefix("10.0.1.")?.parse::<usize>().ok()?;
    last.checked_sub(1)
}

/// One machine, started fresh on every boot.
pub(crate) struct MachineProcess;

#[async_trait]
impl Process for MachineProcess {
    fn name(&self) -> &'static str {
        MACHINE_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // No fault injection: the shipped BUGGIFY sites and crash hints are
        // inert, as in production.
        moonpool_sim::buggify_reset();
        let index = machine_index(ctx.my_ip()).ok_or_else(|| {
            SimulationError::InvalidState(format!("{} is no machine IP", ctx.my_ip()))
        })?;
        let settings = MachineSettings {
            class: Class::Storage,
            capacity: 8,
            failure_domain: zone_of(index),
            name: format!("machine-{index}"),
        };
        let listen = machine_addr(index)
            .literal()
            .ok_or_else(|| SimulationError::InvalidState("a literal address".into()))?;
        let addresses = MachineAddresses::new(listen, Some(machine_addr(index)))
            .map_err(SimulationError::InvalidState)?;
        let layout = paros::JournalStoreConfig {
            durability: paros::journal::Durability::Ordered,
            ..paros::JournalStoreConfig::small()
        };
        // parosd's supervisor: a run that ended on a storage failure starts
        // again on the same disk.
        loop {
            let ran = Box::pin(paros::machine::run_machine(
                ctx.providers().clone(),
                ProviderDisk::new(ctx.storage().clone(), ROOT, layout),
                |_| NoAudit,
                &settings,
                addresses.clone(),
                Names::literal(),
                DriverTunables::default(),
                Transport::Plaintext,
                ctx.shutdown().clone(),
            ))
            .await;
            match ran {
                Ok(()) => return Ok(()),
                Err(MachineError::Storage(_) | MachineError::Run(RunError::Storage(_))) => {}
                Err(MachineError::Run(RunError::Infra(e))) => return Err(e),
                Err(error) => {
                    return Err(SimulationError::InvalidState(format!(
                        "machine {index} stopped: {error}"
                    )));
                }
            }
        }
    }
}
