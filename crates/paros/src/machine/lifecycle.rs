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
//! 3. **Serve.** A machine that `cell add-machine` admitted (#216) serves
//!    the machine contract and a node-only `Inspect` with its cell
//!    ([`super::AdmittedMachine`]), and no journal until placement (#212).
//!    A formed machine runs [`crate::run_journals`] over its
//!    cell's plan, every journal plain Multi-Paxos over the founding members
//!    ([`journal_config`]), and keeps answering the decree from its record
//!    ([`super::FormedCell`]). Every start after formation is an existing
//!    member's, so a lost store is refused as amnesia by the driver.
//!
//! The disk is a [`ProviderDisk`] over the caller's storage provider:
//! `parosd` passes Tokio's filesystem, the simulation moonpool's simulated
//! disk, and both run this code and nothing else (#294: no sim wrapper).
//! What the run observes goes to the caller's audit port ([`AuditScope`]).
//! The machine binds its listen address and advertises another one
//! ([`MachineAddresses`], #257): the advertised address is what the cell
//! plan, an admission and the registry hold, and every dialer resolves it at
//! dial time through the caller's [`Names`].
//!
//! Two moments of a machine's life are worth a fault, and the code names
//! them (`hint!`, inert outside a simulation): a late boot, and the two
//! steps of the decree in [`super::wait_for_cell`].

use std::collections::BTreeMap;
use std::net::SocketAddr;

use std::time::Duration;

use moonpool_core::{Providers, RandomProvider, StorageProvider, TimeProvider};
use paros_core::{Ballot, Config, JournalIdentifier, NodeId};
use tokio_util::sync::CancellationToken;

use super::record::{MachineRecord, journal_config};
use super::stores::{AuditScope, MachineStores};
use super::{
    Admission, AdmittedMachine, CachedRegistry, CellLedger, CellPlan, Class, FormedCell, Joined,
    MachineFacts, ProviderDisk,
};
use crate::ControlPlan;
use crate::{Address, Audit, DriverTunables, Names, RunError};

/// What a machine is configured with: the operator's half of its record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineSettings {
    /// Its class, fixed at format: a later start under another is refused.
    pub class: Class,
    /// Its capacity, in the placement's units; may change across starts.
    pub capacity: u64,
    /// Its failure domain; may change across starts.
    pub failure_domain: String,
    /// Its name, for people (#399): a label, never its identity; may change
    /// across starts. `parosd` defaults it to the host of its advertised
    /// address.
    pub name: String,
}

/// A machine's two addresses (#257, `docs/architecture.md` §3.2): what it
/// binds, and what its peers and clients dial.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineAddresses {
    /// What it binds (`PAROS_LISTEN`), resolved once at start; may be a
    /// wildcard.
    pub listen: SocketAddr,
    /// What it advertises (`PAROS_ADVERTISE`): a literal or a name, kept as
    /// a name and resolved by whoever dials it.
    pub advertise: Address,
}

impl MachineAddresses {
    /// The addresses of a machine that binds `listen` and advertises
    /// `advertise`, which defaults to `listen` when that is not a wildcard.
    ///
    /// # Errors
    ///
    /// `listen` is a wildcard and `advertise` is unset, or `advertise` is a
    /// wildcard: nobody can dial a wildcard, so the start is refused.
    pub fn new(listen: SocketAddr, advertise: Option<Address>) -> Result<Self, String> {
        let advertise = match advertise {
            Some(advertise) => advertise,
            None if listen.ip().is_unspecified() => {
                moonpool_assertions::reachable!(
                    "machine: a wildcard listen address with no advertised address is refused"
                );
                return Err(format!(
                    "PAROS_LISTEN {listen} is a wildcard: set PAROS_ADVERTISE to the HOST:PORT \
                     peers and clients dial"
                ));
            }
            None => Address::from(listen),
        };
        if advertise.literal().is_some_and(|a| a.ip().is_unspecified()) {
            return Err(format!(
                "PAROS_ADVERTISE {advertise} is a wildcard: set the HOST:PORT peers and \
                 clients dial"
            ));
        }
        Ok(Self { listen, advertise })
    }
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

/// The [`CellLedger`] of a machine's disk: the record, rewritten whole at
/// every step, each durable rewrite reported to the machine's audit.
struct DiskLedger<'a, S, A> {
    disk: &'a ProviderDisk<S>,
    audit: &'a A,
    addr: Address,
    record: MachineRecord,
}

impl<S: StorageProvider + Clone, A: Audit> DiskLedger<'_, S, A> {
    async fn commit(&mut self, record: MachineRecord) -> Result<(), String> {
        self.disk.write_record(&record.render()).await?;
        self.audit.machine_recorded(&record);
        self.record = record;
        Ok(())
    }
}

impl<S: StorageProvider + Clone, A: Audit> CellLedger for DiskLedger<'_, S, A> {
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
        let leftovers = self.disk.holds_journals().await;
        if leftovers {
            moonpool_assertions::reachable!(
                "machine: a machine formats over journals an unvoted attempt left"
            );
        }
        self.audit
            .cell_formatting(&self.addr, self.record.node_id, plan, leftovers);
        self.disk.format(self.record.node_id, plan).await?;
        moonpool_assertions::reachable!("machine: a seed formats its cell's journals");
        Ok(())
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

    async fn admit(&mut self, admission: &Admission) -> Result<(), String> {
        assert!(
            self.record.plan.is_none(),
            "a formed machine is admitted nowhere"
        );
        assert!(
            self.record.admitted.is_none(),
            "an admitted machine waits no more"
        );
        assert!(
            self.record.promised == Ballot::default(),
            "a machine promised in cell init is not admitted"
        );
        let mut record = self.record.clone();
        record.admitted = Some(admission.clone());
        self.commit(record).await?;
        assert_eq!(
            self.record.admitted.as_ref(),
            Some(admission),
            "the admission is the record"
        );
        Ok(())
    }
}

/// The longest a machine's boot is held back in a simulation: long enough
/// for a `cell init` to meet a founding member that is not up yet.
const LATE_BOOT_MS: u64 = 2_500;

/// Run a machine on `disk` until `shutdown`: format it on its first start,
/// wait for its cell while it has none, then serve the cell's journals.
/// `audits` names the audit port of each [`AuditScope`]. `addresses` are
/// what this machine binds and what it advertises; `names` resolves every
/// address it dials.
///
/// # Errors
///
/// See [`MachineError`].
///
/// # Panics
///
/// When the record breaks its contract: a formed record that is not the
/// plan the wait formed, or a plan this machine is not a member of.
#[tracing::instrument(level = "debug", skip_all, fields(addr = %addresses.advertise))]
#[allow(clippy::too_many_arguments)]
pub async fn run_machine<P, S, A, F>(
    providers: P,
    disk: ProviderDisk<S>,
    audits: F,
    settings: &MachineSettings,
    addresses: MachineAddresses,
    names: Names,
    tunables: DriverTunables,
    shutdown: CancellationToken,
) -> Result<(), MachineError>
where
    P: Providers,
    S: StorageProvider + Clone + 'static,
    A: Audit + Clone + Send + Sync + 'static,
    F: Fn(AuditScope) -> A,
{
    // A machine that starts late (#246), so a `cell init` meets a founding
    // member that is not up yet. Always safe: a late machine is a slow one.
    if let Some(delay_ms) = moonpool_buggify::buggify_range!(0.1, 250..LATE_BOOT_MS + 1) {
        moonpool_assertions::reachable!("machine: a machine starts late");
        tracing::info!(delay_ms, "machine_boot_delayed");
        if providers
            .time()
            .sleep(Duration::from_millis(delay_ms))
            .await
            .is_err()
        {
            return Ok(());
        }
    }
    let audit = audits(AuditScope::Machine);
    let MachineAddresses { listen, advertise } = addresses;
    let record = identity(&providers, &disk, &audit, &advertise, settings).await?;
    assert_eq!(record.class, settings.class, "the class is fixed at format");
    // The machine samples its own busyness from its first start on (#424):
    // every phase answers `Load` from this board.
    let load = super::load::LoadBoard::default();
    super::load::spawn_monitor(
        &providers,
        disk.root().to_string(),
        tunables.load_interval,
        load.clone(),
        moonpool_buggify::buggify_with_prob!(0.2),
        shutdown.clone(),
    );
    let facts = facts_of(&record, (advertise.clone(), listen), names, load);
    tracing::info!(
        node = facts.node_id.0,
        addr = %advertise,
        %listen,
        class = facts.class.as_str(),
        "machine_starting"
    );
    let joined = if let Some((ballot, plan)) = &record.plan {
        let cached = read_cache(&disk, &audit, record.node_id).await;
        Joined::Founded(FormedCell {
            facts,
            plan: plan.clone(),
            ballot: *ballot,
            cached,
        })
    } else if let Some(admission) = &record.admitted {
        let cached = read_cache(&disk, &audit, record.node_id).await;
        Joined::Admitted(AdmittedMachine {
            facts,
            admission: admission.clone(),
            cached,
        })
    } else {
        let mut ledger = DiskLedger {
            disk: &disk,
            audit: &audit,
            addr: advertise,
            record,
        };
        let waited = super::wait_for_cell(
            providers.clone(),
            &facts,
            &mut ledger,
            &tunables,
            shutdown.clone(),
        )
        .await
        .map_err(MachineError::Run)?;
        let Some(joined) = waited else {
            return Ok(());
        };
        match &joined {
            Joined::Founded(cell) => assert_eq!(
                ledger.record.formed(),
                Some(&cell.plan),
                "a wait ends formed"
            ),
            Joined::Admitted(admitted) => assert_eq!(
                ledger.record.admitted.as_ref(),
                Some(&admitted.admission),
                "a wait ends admitted"
            ),
        }
        joined
    };
    match joined {
        Joined::Founded(cell) => serve(providers, disk, audits, cell, &tunables, shutdown).await,
        Joined::Admitted(admitted) => {
            let cache = super::spawn_writer(
                &providers,
                disk,
                audit.clone(),
                admitted.facts.node_id,
                admitted.cached.clone(),
                shutdown.clone(),
            );
            admitted
                .serve(&providers, &tunables, cache, shutdown)
                .await
                .map_err(MachineError::Run)
        }
    }
}

/// The cached registry fold on `disk` (#211), if it holds one that machine
/// `node` wrote. A cache that cannot be read or parsed is no cache: it only
/// says where to dial, so the machine starts from its plan or admission.
async fn read_cache<S: StorageProvider + Clone, A: Audit>(
    disk: &ProviderDisk<S>,
    audit: &A,
    node: NodeId,
) -> Option<CachedRegistry> {
    let cache = match disk.read_cache().await {
        Ok(Some(text)) => match CachedRegistry::parse(&text) {
            Ok(cache) if cache.node == node => Some(cache),
            Ok(_) => {
                tracing::warn!(node = node.0, "registry_cache_foreign");
                None
            }
            Err(error) => {
                tracing::warn!(node = node.0, %error, "registry_cache_unreadable");
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            tracing::warn!(node = node.0, %error, "registry_cache_unread");
            None
        }
    };
    if cache.is_some() {
        moonpool_assertions::reachable!(
            "machine: a machine restarts with its cached registry fold"
        );
    }
    assert!(
        cache
            .as_ref()
            .is_none_or(|c| c.node == node && c.check().is_ok()),
        "a machine starts from its own checked cache"
    );
    audit.registry_cache_read(node, cache.as_ref());
    cache
}

/// The machine's record: read, or minted on an empty disk; the
/// configuration's mutable fields recorded.
async fn identity<P: Providers, S: StorageProvider + Clone, A: Audit>(
    providers: &P,
    disk: &ProviderDisk<S>,
    audit: &A,
    addr: &Address,
    settings: &MachineSettings,
) -> Result<MachineRecord, MachineError> {
    let refused = |error: String| MachineError::Refused(format!("machine record: {error}"));
    let failed = |error: String| MachineError::Storage(format!("machine record: {error}"));
    let read = disk.read_record().await.map_err(failed)?;
    let Some(text) = read else {
        // A disk with stores and no identity lost it: never a new machine
        // on top of an old one's stores.
        if disk.holds_journals().await {
            return Err(MachineError::Refused(
                "stores without a machine record — this machine lost its identity \
                 (amnesia); wipe its disk to start a new machine, which never rejoins \
                 as the old one"
                    .into(),
            ));
        }
        audit.machine_booted(addr, None);
        moonpool_assertions::reachable!("machine: a machine formats an empty disk");
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
            name: settings.name.clone(),
            promised: Ballot::default(),
            plan: None,
            admitted: None,
            created: Vec::new(),
        };
        disk.write_record(&record.render()).await.map_err(failed)?;
        audit.machine_recorded(&record);
        tracing::info!(node = node_id.0, "machine_formatted");
        return Ok(record);
    };
    let mut record = MachineRecord::parse(&text).map_err(refused)?;
    assert_ne!(record.node_id, NodeId(0), "a minted identity is set");
    audit.machine_booted(addr, Some(&record));
    if record.formed().is_some() {
        moonpool_assertions::reachable!("machine: a formed machine restarts and serves its cell");
    } else if record.admitted.is_some() {
        moonpool_assertions::reachable!("machine: an admitted machine restarts into its cell");
    } else {
        moonpool_assertions::reachable!("machine: a formatted machine restarts and waits");
        if record.promised != Ballot::default() {
            moonpool_assertions::reachable!(
                "machine: a machine restarts promised in cell init and unvoted"
            );
        }
    }
    if record.class != settings.class {
        return Err(MachineError::Refused(format!(
            "this machine was formatted as {}, not {}: a class is fixed at format",
            record.class.as_str(),
            settings.class.as_str()
        )));
    }
    let changed = record.capacity != settings.capacity
        || record.failure_domain != settings.failure_domain
        || record.name != settings.name;
    record.capacity = settings.capacity;
    record.failure_domain.clone_from(&settings.failure_domain);
    record.name.clone_from(&settings.name);
    if changed {
        disk.write_record(&record.render()).await.map_err(failed)?;
        audit.machine_recorded(&record);
    }
    Ok(record)
}

/// The facts of the machine `record` names, advertising and binding
/// `addresses`: no runtime serves it yet.
fn facts_of(
    record: &MachineRecord,
    (addr, listen): (Address, SocketAddr),
    names: Names,
    load: super::load::LoadBoard,
) -> MachineFacts {
    MachineFacts {
        node_id: record.node_id,
        class: record.class,
        capacity: record.capacity,
        failure_domain: record.failure_domain.clone(),
        name: record.name.clone(),
        addr,
        listen,
        names,
        incarnation: 0,
        load,
    }
}

/// Serve the cell's journals until shutdown.
async fn serve<P, S, A, F>(
    providers: P,
    disk: ProviderDisk<S>,
    audits: F,
    cell: FormedCell,
    tunables: &DriverTunables,
    shutdown: CancellationToken,
) -> Result<(), MachineError>
where
    P: Providers,
    S: StorageProvider + Clone + 'static,
    A: Audit + Clone + Send + Sync + 'static,
    F: Fn(AuditScope) -> A,
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
    // The record as the formation left it: the stores rewrite it when they
    // provision a created journal (#210).
    let record = disk
        .read_record()
        .await
        .map_err(|e| MachineError::Storage(format!("machine record: {e}")))?
        .ok_or_else(|| MachineError::Storage("machine record: gone after formation".into()))
        .and_then(|text| {
            MachineRecord::parse(&text)
                .map_err(|e| MachineError::Refused(format!("machine record: {e}")))
        })?;
    assert_eq!(
        record.node_id, node_id,
        "the record names the serving machine"
    );
    // The founding members where the cached registry fold dials them, else
    // at the plan's addresses (#211).
    let founders = cell.founders();
    let control = ControlPlan {
        self_id: node_id,
        class: cell.facts.class,
        seeds: founders
            .iter()
            .map(|(id, addr)| (*id, addr.to_string()))
            .collect(),
        cell: plan.control,
        founders: plan.members.iter().map(|(id, _)| *id).collect(),
        spares: Vec::new(),
    };
    let cache = super::spawn_writer(
        &providers,
        disk.clone(),
        audits(AuditScope::Machine),
        node_id,
        cell.cached.clone(),
        shutdown.clone(),
    );
    let stores = MachineStores::new(disk, record, genesis, audits).with_cache(cache);
    // Bind where the machine is (#257), never at the address the plan
    // froze: a machine whose address changed across a restart still serves.
    let listen = cell.facts.listen;
    let book: Vec<(NodeId, String)> = founders
        .iter()
        .map(|(id, addr)| (*id, addr.to_string()))
        .collect();
    tracing::info!(node = node_id.0, cell = plan.cell_id, addr = %cell.facts.addr, %listen, "machine_serving");
    crate::run_journals(
        providers,
        stores,
        node_id,
        listen.to_string(),
        book,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Some(control),
        Some(cell.clone()),
        *tunables,
        shutdown,
    )
    .await
    .map_err(MachineError::Run)
}
