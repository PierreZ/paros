//! The machine lifecycle (#246): what every machine does from its first
//! start, written once for `parosd` and the simulation alike.
//!
//! 1. **Format, once.** On an empty disk the machine mints its `node_id` at
//!    random (#225) and records it ([`MachineRecord`]). A disk that holds
//!    stores but no record lost its identity, and is refused (amnesia); a
//!    record of another class is refused (a class is fixed at format).
//! 2. **Wait.** Until it belongs to a cell, it serves the machine contract
//!    ([`super::wait_for_cell`]): an acceptor of any `cell init` that lists
//!    it, and the proposer of one sent to it (#277). It never forms a cell
//!    on its own (#216).
//! 3. **Serve.** A formed machine runs [`crate::run_journals`] over its
//!    cell's plan, every journal plain Multi-Paxos over the founding members
//!    ([`journal_config`]), and keeps answering the decree from its record
//!    ([`super::FormedCell`]). Every start after formation is an existing
//!    member's, so a lost store is refused as amnesia by the driver.
//!
//! The disk is the caller's ([`MachineDisk`]): `parosd` keeps the record and
//! the stores in a data directory, the simulation on moonpool's simulated
//! disk. Names are the caller's too: the library sees socket addresses only.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use moonpool_core::{Providers, RandomProvider, TimeProvider};
use paros_core::{Config, JournalIdentifier, NodeId};
use tokio_util::sync::CancellationToken;

use paros_core::Ballot;

use super::record::{MachineRecord, journal_config};
use super::{CellLedger, CellPlan, Class, FormedCell, MachineFacts};
use crate::{DriverHooks, DriverTunables, JournalStores, RunError};

/// What a machine is configured with: the operator's half of its record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineSettings {
    /// Its class, fixed at format: a later start under another is refused.
    pub class: Class,
    /// Its capacity, in the placement's units; may change across starts.
    pub capacity: u64,
    /// Its failure domain; may change across starts.
    pub failure_domain: String,
}

/// How a machine's run ended before or instead of a clean shutdown.
#[derive(Debug)]
pub enum MachineError {
    /// The configuration is invalid: fix it (`parosd` exits 2).
    Invalid(String),
    /// The disk disagrees with the configuration: an operator must act, a
    /// restart cannot help (`parosd` exits 78).
    Refused(String),
    /// The disk failed under the machine record (a read or a write did not
    /// complete): nothing is refused, and a restart recovers from what the
    /// disk holds (`parosd` exits 75). Found in simulation (#246): a failed
    /// sync of the record's first write ended a machine as refused, for
    /// good, and the cell never formed.
    Storage(String),
    /// The drivers ended in error ([`RunError`]).
    Run(RunError),
}

impl core::fmt::Display for MachineError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MachineError::Invalid(error)
            | MachineError::Refused(error)
            | MachineError::Storage(error) => f.write_str(error),
            MachineError::Run(error) => write!(f, "{error}"),
        }
    }
}

/// A machine's disk: its record and its journal stores. Every write returns
/// only once what it wrote survives a crash.
pub trait MachineDisk {
    /// The journal stores a formed machine serves.
    type Stores: JournalStores;

    /// The machine record's text, or `None` when there is none.
    ///
    /// # Errors
    ///
    /// The record exists and cannot be read.
    fn read_record(&mut self) -> impl Future<Output = Result<Option<String>, String>>;

    /// Replace the machine record with `text`, whole and durably: a crash
    /// leaves the old record or the new one.
    ///
    /// # Errors
    ///
    /// The record could not be made durable.
    fn write_record(&mut self, text: &str) -> impl Future<Output = Result<(), String>>;

    /// Whether the disk holds anything of a journal store: with no record,
    /// that is a machine that lost its identity.
    fn holds_stores(&mut self) -> impl Future<Output = bool>;

    /// Format the store of every journal `plan` names for member `node_id`
    /// ([`crate::provision_store`]) and remember them as provisioned. An
    /// interrupted run resumes: a store already formatted is left as it is.
    ///
    /// # Errors
    ///
    /// A store could not be formatted durably.
    fn provision(
        &mut self,
        node_id: NodeId,
        plan: &CellPlan,
    ) -> impl Future<Output = Result<(), String>>;

    /// The stores of member `node_id` serving `genesis`, every one an
    /// existing member's.
    ///
    /// # Errors
    ///
    /// The disk disagrees with the plan (a store of another machine).
    fn stores(
        &mut self,
        node_id: NodeId,
        genesis: BTreeMap<JournalIdentifier, Config>,
    ) -> impl Future<Output = Result<Self::Stores, String>>;
}

/// The [`CellLedger`] of a machine's disk: the record, rewritten whole at
/// every step.
struct DiskLedger<'a, D> {
    disk: &'a mut D,
    record: MachineRecord,
}

impl<D: MachineDisk> DiskLedger<'_, D> {
    async fn commit(&mut self, record: MachineRecord) -> Result<(), String> {
        self.disk.write_record(&record.render()).await?;
        self.record = record;
        Ok(())
    }
}

impl<D: MachineDisk> CellLedger for DiskLedger<'_, D> {
    fn promised(&self) -> Ballot {
        self.record.promised
    }

    fn vote(&self) -> Option<(Ballot, CellPlan)> {
        self.record.plan.clone()
    }

    async fn promise(&mut self, ballot: Ballot) -> Result<(), String> {
        assert!(
            ballot > self.record.promised,
            "a promise is only ever raised"
        );
        assert!(
            self.record.plan.is_none(),
            "a formed machine's vote is final"
        );
        let mut record = self.record.clone();
        record.promised = ballot;
        self.commit(record).await
    }

    async fn format(&mut self, plan: &CellPlan) -> Result<(), String> {
        assert!(
            self.record.plan.is_none(),
            "a formed machine formats no other cell"
        );
        self.disk.provision(self.record.node_id, plan).await
    }

    async fn form(&mut self, ballot: Ballot, plan: &CellPlan) -> Result<(), String> {
        assert!(
            ballot >= self.record.promised,
            "a vote is never under the promise"
        );
        assert!(
            self.record.plan.is_none(),
            "a formed machine forms no other cell"
        );
        let mut record = self.record.clone();
        record.promised = ballot;
        record.plan = Some((ballot, plan.clone()));
        self.commit(record).await?;
        assert_eq!(
            self.record.formed(),
            Some(plan),
            "the commit point is the record"
        );
        Ok(())
    }
}

/// Run a machine on `disk` until `shutdown`: format it on its first start,
/// wait for its cell while it has none, then serve the cell's journals.
/// `addr` is the address this machine serves at. `assignment` is how many
/// user journals a cell this machine draws at `cell init` serves beside its
/// control journals (the static assignment, until #212).
///
/// # Errors
///
/// See [`MachineError`].
///
/// # Panics
///
/// When the disk breaks its contract: a formed record that is not the plan
/// the wait formed, or a plan this machine is not a member of.
#[tracing::instrument(level = "debug", skip_all, fields(addr = %addr))]
#[allow(clippy::too_many_arguments)]
pub async fn run_machine<P, D, H>(
    providers: P,
    mut disk: D,
    settings: &MachineSettings,
    addr: SocketAddr,
    assignment: usize,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
) -> Result<(), MachineError>
where
    P: Providers,
    D: MachineDisk,
    H: DriverHooks,
{
    if let Some(delay) = hooks.delay_boot() {
        tracing::info!(delay_ms = delay.as_millis(), "machine_boot_delayed");
        if providers.time().sleep(delay).await.is_err() {
            return Ok(());
        }
    }
    let record = identity(&providers, &mut disk, settings).await?;
    assert_eq!(record.class, settings.class, "the class is fixed at format");
    let facts = MachineFacts {
        node_id: record.node_id,
        class: record.class,
        capacity: record.capacity,
        failure_domain: record.failure_domain.clone(),
        addr,
    };
    tracing::info!(
        node = facts.node_id.0,
        %addr,
        class = facts.class.as_str(),
        "machine_starting"
    );
    let cell = if let Some((ballot, plan)) = &record.plan {
        FormedCell {
            facts,
            plan: plan.clone(),
            ballot: *ballot,
        }
    } else {
        let mut ledger = DiskLedger {
            disk: &mut disk,
            record,
        };
        let waited = super::wait_for_cell(
            providers.clone(),
            &facts,
            assignment,
            &mut ledger,
            &tunables,
            shutdown.clone(),
            hooks,
        )
        .await
        .map_err(MachineError::Run)?;
        let Some(cell) = waited else {
            return Ok(());
        };
        assert_eq!(
            ledger.record.formed(),
            Some(&cell.plan),
            "a wait ends formed"
        );
        cell
    };
    serve(providers, disk, cell, tunables, shutdown, hooks).await
}

/// The machine's record: read, or minted on an empty disk; the
/// configuration's mutable fields recorded.
async fn identity<P: Providers, D: MachineDisk>(
    providers: &P,
    disk: &mut D,
    settings: &MachineSettings,
) -> Result<MachineRecord, MachineError> {
    let refused = |error: String| MachineError::Refused(format!("machine record: {error}"));
    let failed = |error: String| MachineError::Storage(format!("machine record: {error}"));
    let read = disk.read_record().await.map_err(failed)?;
    let Some(text) = read else {
        // A disk with stores and no identity lost it: never a new machine
        // on top of an old one's stores.
        if disk.holds_stores().await {
            return Err(MachineError::Refused(
                "stores without a machine record — this machine lost its identity \
                 (amnesia); wipe its disk to start a new machine, which never rejoins \
                 as the old one"
                    .into(),
            ));
        }
        let node_id = loop {
            let id: u64 = providers.random().random();
            if id != 0 {
                break NodeId(id);
            }
        };
        let record = MachineRecord {
            node_id,
            class: settings.class,
            capacity: settings.capacity,
            failure_domain: settings.failure_domain.clone(),
            promised: Ballot::default(),
            plan: None,
        };
        disk.write_record(&record.render()).await.map_err(failed)?;
        tracing::info!(node = node_id.0, "machine_formatted");
        return Ok(record);
    };
    let mut record = MachineRecord::parse(&text).map_err(refused)?;
    assert_ne!(record.node_id, NodeId(0), "a minted identity is set");
    if record.class != settings.class {
        return Err(MachineError::Refused(format!(
            "this machine was formatted as {}, not {}: a class is fixed at format",
            record.class.as_str(),
            settings.class.as_str()
        )));
    }
    let changed =
        record.capacity != settings.capacity || record.failure_domain != settings.failure_domain;
    record.capacity = settings.capacity;
    record.failure_domain.clone_from(&settings.failure_domain);
    if changed {
        disk.write_record(&record.render()).await.map_err(failed)?;
    }
    Ok(record)
}

/// Serve the cell's journals until shutdown.
async fn serve<P, D, H>(
    providers: P,
    mut disk: D,
    cell: FormedCell,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
) -> Result<(), MachineError>
where
    P: Providers,
    D: MachineDisk,
    H: DriverHooks,
{
    let node_id = cell.facts.node_id;
    let plan = &cell.plan;
    assert!(
        plan.members.iter().any(|(id, _)| *id == node_id),
        "a formed machine is a member of its plan"
    );
    let genesis: BTreeMap<JournalIdentifier, Config> = plan
        .journals
        .iter()
        .map(|&journal| (journal, journal_config(plan, node_id, journal)))
        .collect();
    let stores = disk
        .stores(node_id, genesis)
        .await
        .map_err(MachineError::Refused)?;
    let addr = plan
        .members
        .iter()
        .find(|(id, _)| *id == node_id)
        .map_or(cell.facts.addr, |(_, addr)| *addr);
    let book: Vec<(NodeId, String)> = plan
        .members
        .iter()
        .map(|(id, addr)| (*id, addr.to_string()))
        .collect();
    tracing::info!(node = node_id.0, cell = plan.cell_id, %addr, "machine_serving");
    crate::run_journals(
        providers,
        stores,
        node_id,
        addr.to_string(),
        book,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
        Some(cell.clone()),
        tunables,
        shutdown,
        hooks,
    )
    .await
    .map_err(MachineError::Run)
}
