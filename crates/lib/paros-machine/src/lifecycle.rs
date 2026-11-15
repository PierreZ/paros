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
//!    ([`super::AdmittedMachine`]), and runs [`paros_roles::node::run_journals`] over
//!    no plan journal: it serves the journals placed on it (#474).
//!    A formed machine runs [`paros_roles::node::run_journals`] over its
//!    cell's plan, every journal plain Multi-Paxos over the initial members
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

use moonpool_core::{LocalStorageProvider, LocalTimeProvider};
use paros_core::journal::JournalIdentifier;
use paros_core::matchmaker::MatchmakerConfig;
use paros_core::membership::MatchmakerId;
use paros_core::{Ballot, Config, NodeId};
use paros_net::runtime::ParosProviders;
use tokio_util::sync::CancellationToken;

use super::stores::{AuditScope, MachineStores};
use super::{
    AdminPin, Admission, AdmittedMachine, CachedRegistry, CellLedger, CellMachine, CellPlan, Class,
    FormedCell, Joined, Locality, MachineFacts, ProviderDisk,
};
use paros_model::address::Address;
use paros_model::address::Names;
use paros_model::machine::{Disk, MachineRecord, journal_config};
use paros_net::transport::Transport;
use paros_observe::audit::Audit;
use paros_observe::metrics::{Metrics, Phase};
use paros_roles::node::ControlPlan;
use paros_storage::boot::RunError;
use paros_tunables::DriverTunables;

/// What a machine is configured with: the operator's half of its record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineSettings {
    /// Its class, fixed at format: a later start under another is refused.
    /// `None` keeps the class of its record, and formats an empty disk as
    /// [`Class::All`] (every role).
    pub class: Option<Class>,
    /// Its zone and rack (#575: its reserved tags `paros/zone` and
    /// `paros/rack`); may change across starts. A label no tag value may
    /// hold is refused at start.
    pub locality: Locality,
    /// Its name, for people (#399): a label, never its identity; may change
    /// across starts. `parosd` defaults it to the host of its advertised
    /// address.
    pub name: String,
    /// What it checks the token of a call that puts it in a cell against
    /// while it is idle (#245 (Biscuit tokens)): `parosd --root-public-key`,
    /// or nothing.
    pub pin: AdminPin,
    /// Where it records its metrics and phase (#366 (metrics)):
    /// [`Metrics::off`](paros_observe::metrics::Metrics::off) records nothing, and
    /// the machine serves the same either way. `parosd` serves it on
    /// `/metrics`, `/healthz` and `/readyz`.
    pub metrics: paros_observe::metrics::Metrics,
    /// The frontend it serves once it is in a cell (#723 (frontend as a
    /// parosd role)): the `Authz` it checks every call through
    /// (`BiscuitAuthz` in `parosd`), or `None` to serve no frontend. The
    /// tenant coordinator books a tenant's frontends on the `stateless`
    /// machines, and `Resolve` names them.
    pub frontend: Option<paros_roles::frontend::FrontendRole>,
    /// The bytes of its disk that paros may fill (#985 (placement counts
    /// store disk)): `PAROS_DISK_BYTES`, or `None` for the size of the file
    /// system under its data directory. `Some(0)` reports no size, so the
    /// registry counts no store on it. The registry counts every store
    /// booked on it against this; it may change across starts.
    pub disk_bytes: Option<u64>,
}

/// A machine's two addresses (#257, `docs/architecture/03-2-machines.md` §3.2): what it
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

impl<S: LocalStorageProvider + Clone, A: Audit> DiskLedger<'_, S, A> {
    async fn commit(&mut self, record: MachineRecord) -> Result<(), String> {
        self.disk.write_record(&record.render()).await?;
        self.audit.machine_recorded(&record);
        self.record = record;
        Ok(())
    }
}

impl<S: LocalStorageProvider + Clone, A: Audit> CellLedger for DiskLedger<'_, S, A> {
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
/// for a `cell init` to meet an initial member that is not up yet.
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
    transport: Transport,
    shutdown: CancellationToken,
) -> Result<(), MachineError>
where
    P: ParosProviders,
    S: LocalStorageProvider + Clone + 'static,
    A: Audit + Clone + Send + Sync + 'static,
    F: Fn(AuditScope) -> A + Send + Sync + 'static,
{
    // A machine that starts late (#246), so a `cell init` meets an initial
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
    assert!(settings.class.is_none_or(|c| c == record.class), "roles");
    let mut facts = facts_of(
        &record,
        (advertise.clone(), listen),
        (names, disk_of(&providers, &disk, settings)),
        monitor(&providers, disk.root(), settings, &tunables, &shutdown),
    );
    facts.frontend.clone_from(&settings.frontend);
    tracing::info!(
        machine = %facts.name,
        disk_bytes = facts.disk.total_bytes,
        store_bytes = facts.disk.store_bytes,
        addr = %advertise,
        %listen,
        class = facts.class.as_str(),
        "machine_starting"
    );
    let joined = if let Some((ballot, plan)) = &record.plan {
        let cached = read_cache(&disk, &audit, record.node_id).await;
        Joined::Formed(FormedCell {
            facts,
            plan: plan.clone(),
            ballot: *ballot,
            cached,
            // Never written down (#245): a restarted member teaches nothing.
            token: None,
        })
    } else if let Some(admission) = &record.admitted {
        let cached = read_cache(&disk, &audit, record.node_id).await;
        Joined::Admitted(AdmittedMachine {
            facts,
            admission: admission.clone(),
            cached,
        })
    } else {
        settings.metrics.set_phase(Phase::Idle);
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
            &transport,
            &settings.pin,
            shutdown.clone(),
        )
        .await
        .map_err(MachineError::Run)?;
        let Some(joined) = waited else {
            return Ok(());
        };
        match &joined {
            Joined::Formed(cell) => assert_eq!(
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
        Joined::Formed(cell) => {
            serve(
                providers, disk, audits, cell, &tunables, transport, shutdown,
            )
            .await
        }
        Joined::Admitted(admitted) => {
            serve_admitted(
                providers, disk, audits, admitted, &tunables, transport, shutdown,
            )
            .await
        }
    }
}

/// The cached registry fold on `disk` (#211), if it holds one that machine
/// `node` wrote. A cache that cannot be read or parsed is no cache: it only
/// says where to dial, so the machine starts from its plan or admission.
async fn read_cache<S: LocalStorageProvider + Clone, A: Audit>(
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
async fn identity<P: ParosProviders, S: LocalStorageProvider + Clone, A: Audit>(
    providers: &P,
    disk: &ProviderDisk<S>,
    audit: &A,
    addr: &Address,
    settings: &MachineSettings,
) -> Result<MachineRecord, MachineError> {
    let refused = |error: String| MachineError::Refused(format!("machine record: {error}"));
    let failed = |error: String| MachineError::Storage(format!("machine record: {error}"));
    if !settings.locality.is_valid() {
        return Err(MachineError::Refused(
            "a zone or a rack is at most 255 bytes, with no control character and no ','".into(),
        ));
    }
    let read = disk.read_record().await.map_err(failed)?;
    let Some(text) = read else {
        return format_empty(providers, disk, audit, addr, settings).await;
    };
    let mut record = MachineRecord::parse(&text).map_err(refused)?;
    assert_ne!(record.node_id, NodeId(0), "a minted identity is set");
    audit.machine_booted(addr, Some(&record));
    restarted(&record);
    if reconfigure(disk, &mut record, settings).await? {
        disk.write_record(&record.render()).await.map_err(failed)?;
        audit.machine_recorded(&record);
    }
    Ok(record)
}

/// A new machine on an empty disk: a minted `node_id`, recorded. A disk
/// with stores and no identity lost it: never a new machine on top of an
/// old one's stores.
async fn format_empty<P: ParosProviders, S: LocalStorageProvider + Clone, A: Audit>(
    providers: &P,
    disk: &ProviderDisk<S>,
    audit: &A,
    addr: &Address,
    settings: &MachineSettings,
) -> Result<MachineRecord, MachineError> {
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
    let node_id = NodeId(super::cell_init::draw_nonzero(providers));
    let record = MachineRecord {
        node_id,
        class: settings.class.unwrap_or(Class::All),
        locality: settings.locality.clone(),
        name: settings.name.clone(),
        promised: Ballot::default(),
        plan: None,
        admitted: None,
        created: Vec::new(),
        deleted: Vec::new(),
    };
    disk.write_record(&record.render())
        .await
        .map_err(|error| MachineError::Storage(format!("machine record: {error}")))?;
    audit.machine_recorded(&record);
    tracing::info!(machine = %record.name, "machine_formatted");
    Ok(record)
}

/// Which restart this is: formed, admitted, or still waiting.
fn restarted(record: &MachineRecord) {
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
}

/// Take the configuration's class, locality and name into `record`, and
/// say whether it changed. A machine may take more roles at a restart
/// (`all`), never fewer: the cell may book a role on it that a narrower
/// class drops. A machine that never joined a cell holds no role yet.
async fn reconfigure<S: LocalStorageProvider + Clone>(
    disk: &ProviderDisk<S>,
    record: &mut MachineRecord,
    settings: &MachineSettings,
) -> Result<bool, MachineError> {
    let mut changed = false;
    if let Some(class) = settings.class.filter(|class| *class != record.class) {
        let joined = record.formed().is_some()
            || record.admitted.is_some()
            || record.promised != Ballot::default()
            || disk.holds_stores().await;
        if class != Class::All && joined {
            return Err(MachineError::Refused(format!(
                "this machine takes {} roles: a restart may set PAROS_ROLES=all, never \
                 {}; to take fewer roles, remove it from its cell and wipe its disk",
                record.class.as_str(),
                class.as_str()
            )));
        }
        moonpool_assertions::reachable!("machine: a machine restarts taking other roles");
        tracing::info!(machine = %record.name, from = record.class.as_str(), to = class.as_str(), "machine_roles_changed");
        record.class = class;
        changed = true;
    }
    changed |= record.locality != settings.locality || record.name != settings.name;
    record.locality.clone_from(&settings.locality);
    record.name.clone_from(&settings.name);
    Ok(changed)
}

/// The facts of the machine `record` names, advertising and binding
/// `addresses`: no runtime serves it yet.
/// The machine samples its own busyness from its first start on (#424):
/// every phase answers `Load` from the board this returns, beside the
/// metrics each window also goes to (#366 (metrics)).
fn monitor<P: ParosProviders>(
    providers: &P,
    root: &str,
    settings: &MachineSettings,
    tunables: &DriverTunables,
    shutdown: &CancellationToken,
) -> (super::load::LoadBoard, Metrics) {
    let metrics = &settings.metrics;
    // A start begins again: metrics a machine kept from its last run (the
    // simulation's reboot) show it starting, not serving.
    metrics.set_phase(Phase::Starting);
    let load = super::load::LoadBoard::default();
    super::load::spawn_monitor(
        providers,
        root.to_string(),
        tunables.load_interval,
        (load.clone(), metrics.clone()),
        moonpool_buggify::buggify_with_prob!(0.2),
        shutdown.clone(),
    );
    (load, metrics.clone())
}

/// The disk machine reports (#985 (placement counts store disk)): the bytes
/// `settings` let paros fill, else the size of the file system under its
/// data directory (`0` when it cannot be read: no limit), and what one store
/// of its layout allocates.
fn disk_of<P: ParosProviders, S: LocalStorageProvider + Clone + 'static>(
    providers: &P,
    disk: &ProviderDisk<S>,
    settings: &MachineSettings,
) -> Disk {
    let total_bytes = settings.disk_bytes.unwrap_or_else(|| {
        moonpool_core::SystemProvider::sample(providers.system(), disk.root())
            .ok()
            .and_then(|sample| sample.space)
            .map_or(0, |space| space.total_bytes)
    });
    let reported = Disk::new(total_bytes, disk.layout().store_bytes());
    assert!(reported.store_bytes > 0, "a store allocates its segment");
    reported
}

fn facts_of(
    record: &MachineRecord,
    (addr, listen): (Address, SocketAddr),
    (names, disk): (Names, Disk),
    (load, metrics): (super::load::LoadBoard, Metrics),
) -> MachineFacts {
    MachineFacts {
        node_id: record.node_id,
        class: record.class,
        locality: record.locality.clone(),
        name: record.name.clone(),
        addr,
        listen,
        names,
        incarnation: 0,
        machine_interface: Vec::new(),
        disk,
        load,
        metrics,
        frontend: None,
    }
}

/// The matchmakers machine `me` links: one per machine of `book`, where
/// the book dials it, and this machine's own at the address it advertises
/// now (a renamed machine's old name points nowhere, #349). One book, every
/// machine alike (#685 (no founders)): a machine may host a set whatever
/// it was formed or admitted with.
fn links(book: &[(NodeId, Address)], me: (NodeId, &str)) -> Vec<(MatchmakerId, String)> {
    let links: Vec<(MatchmakerId, String)> = book
        .iter()
        .map(|(id, addr)| {
            let addr = if *id == me.0 {
                me.1.to_string()
            } else {
                addr.to_string()
            };
            (MatchmakerId(id.0), addr)
        })
        .collect();
    assert_eq!(links.len(), book.len(), "one matchmaker per machine");
    let ids: std::collections::BTreeSet<u64> = links.iter().map(|(id, _)| id.0).collect();
    assert_eq!(ids.len(), links.len(), "a book links each machine once");
    links
}

/// Serve the cell's journals until shutdown.
async fn serve<P, S, A, F>(
    providers: P,
    disk: ProviderDisk<S>,
    audits: F,
    cell: FormedCell,
    tunables: &DriverTunables,
    transport: Transport,
    shutdown: CancellationToken,
) -> Result<(), MachineError>
where
    P: ParosProviders,
    S: LocalStorageProvider + Clone + 'static,
    A: Audit + Clone + Send + Sync + 'static,
    F: Fn(AuditScope) -> A + Send + Sync + 'static,
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
    let record = read_record(&disk, node_id).await?;
    // Every machine this one starts from (#211, #685 (no founders)): the
    // plan's members where the cached registry fold dials them, else at
    // the plan's addresses, then every other machine the cache names. One
    // book: each machine's matchmaker is linked from boot.
    let start = paros_model::machine::starting_book(&plan.members, cell.cached.as_ref());
    // The plan's members, the first configuration of the plan journals:
    // their peers' lanes and the follow's seeds.
    let initial_members = cell.initial_members();
    // The machines beyond the plan: in every journal's matchmaker pool from
    // boot (`ControlPlan::cached`, #685 (special tenants are tenants)).
    let cached: Vec<(NodeId, Address)> = paros_model::machine::others(&start, node_id)
        .into_iter()
        .filter(|(id, _)| !plan.members.iter().any(|(m, _)| m == id))
        .collect();
    let control = ControlPlan {
        self_id: node_id,
        class: cell.facts.class,
        seeds: initial_members
            .iter()
            .map(|(id, addr)| (*id, addr.to_string()))
            .collect(),
        cell: plan.control,
        initial_members: plan.members.iter().map(|(id, _)| *id).collect(),
        // Every journal is born with its tenant's set on the initial
        // members (#190), until placement (#212).
        matchmakers: plan.matchmakers(),
        spares: Vec::new(),
        cached: cached.iter().map(|(id, _)| *id).collect(),
        planned: Vec::new(), // An initial member serves them from its plan.
    };
    let (cache, writer) = super::spawn_writer(
        &providers,
        disk.clone(),
        audits(AuditScope::Machine),
        node_id,
        cell.cached.clone(),
        &shutdown,
    );
    // An initial member hosts every tenant's matchmaker set (#190).
    let matchmaker = MatchmakerConfig {
        id: MatchmakerId(node_id.0),
        bootstrap: plan.matchmakers(),
    };
    let stores = MachineStores::new(disk, record, genesis, audits)
        .with_cache(cache.clone())
        .with_matchmakers(matchmaker);
    // Bind where the machine is (#257), never at the address the plan
    // froze: a machine whose address changed across a restart still serves.
    let listen = cell.facts.listen;
    let book: Vec<(NodeId, String)> = initial_members
        .iter()
        .map(|(id, addr)| (*id, addr.to_string()))
        .collect();
    let matchmakers = links(&start, (node_id, &cell.facts.addr.to_string()));
    assert_eq!(
        matchmakers.len(),
        plan.members.len() + cached.len(),
        "one matchmaker per machine of the book"
    );
    tracing::info!(machine = %cell.facts.name, addr = %cell.facts.addr, %listen, "machine_serving");
    cell.facts.metrics.set_phase(Phase::Serving);
    let ran = paros_roles::node::run_journals(
        providers,
        stores,
        node_id,
        listen.to_string(),
        book,
        matchmakers,
        Vec::new(),
        Vec::new(),
        Some(control),
        Some(CellMachine::Formed {
            cell: cell.clone(),
            cache,
        }),
        *tunables,
        transport,
        shutdown,
    )
    .await;
    writer.stop().await;
    ran.map_err(MachineError::Run)
}

/// The spare templates of the cell plan's journals on the admitted machine
/// `me`: a booking grows each onto it (#685 (special tenants are tenants)
/// step 4).
fn planned_spares(admission: &Admission, me: NodeId) -> Vec<Config> {
    let cell = admission.cell;
    let planned: Vec<Config> = std::iter::once(cell.cell)
        .chain(cell.fleet)
        .chain(cell.election)
        .filter_map(|journal| {
            paros_model::machine::spare_journal_config(
                &cell,
                &admission.initial_members,
                me,
                journal,
            )
        })
        .collect();
    assert!(
        !planned.is_empty(),
        "the cell control journal has a spare template"
    );
    planned
}

/// Serve the journals placed on the admitted machine `admitted` until
/// shutdown (#474): the node loop of every machine of a cell, with no plan
/// journal, following the control journals from the initial members its
/// admission names. Its journals are born with their tenant's matchmaker
/// set on the initial members, as every journal of the cell is (#190); a
/// storage machine hosts the later generations of the sets booked on it
/// (#501 (matchmaker sets off the founders)).
async fn serve_admitted<P, S, A, F>(
    providers: P,
    disk: ProviderDisk<S>,
    audits: F,
    admitted: AdmittedMachine,
    tunables: &DriverTunables,
    transport: Transport,
    shutdown: CancellationToken,
) -> Result<(), MachineError>
where
    P: ParosProviders,
    S: LocalStorageProvider + Clone + 'static,
    A: Audit + Clone + Send + Sync + 'static,
    F: Fn(AuditScope) -> A + Send + Sync + 'static,
{
    let node_id = admitted.facts.node_id;
    let admission = admitted.admission.clone();
    assert!(admission.check().is_ok(), "an admission is checked");
    assert!(
        !admission.initial_members.contains(&node_id),
        "an admitted machine is no initial member"
    );
    let record = read_record(&disk, node_id).await?;
    // The cell's machines where the cached registry fold dials them, else
    // where the admission does (#211). The follow's first seeds are the
    // plan journals' first configuration among them (the initial members
    // the admission names). This rule must stay: it is about the first
    // configuration itself. At boot the machine knows no configuration in
    // force, because the cache holds addresses, not slots, and no fold is
    // read yet. The follow's first fold then grows the seeds to the
    // machines that hold the cell control journal's slots
    // (`ControlFollower::adopt`, #685 (no founders)).
    let book = admitted.book();
    let mut initial_members: Vec<(NodeId, String)> = book
        .iter()
        .filter(|(id, _)| admission.initial_members.contains(id))
        .map(|(id, addr)| (*id, addr.to_string()))
        .collect();
    initial_members.sort_unstable();
    // A first-configuration member at no known address is dialed nowhere:
    // the seeds fall back on every machine the admission knows.
    let seeds: Vec<(NodeId, String)> = if initial_members.is_empty() {
        book.iter()
            .filter(|(id, _)| *id != node_id)
            .map(|(id, addr)| (*id, addr.to_string()))
            .collect()
    } else {
        initial_members.clone()
    };
    // The plan's journals, which a booking here grows onto this machine
    // (#685 (special tenants are tenants) step 4): their spare templates.
    let planned = planned_spares(&admission, node_id);
    let control = ControlPlan {
        self_id: node_id,
        class: admitted.facts.class,
        seeds,
        cell: admission.cell.cell,
        initial_members: admission.initial_members.clone(),
        matchmakers: admission
            .initial_members
            .iter()
            .map(|id| MatchmakerId(id.0))
            .collect(),
        spares: Vec::new(),
        // The other machines its book names, the cached registry fold's
        // (#685 (special tenants are tenants)), beyond the initial
        // members `initial_members` links above (one link per machine).
        cached: paros_model::machine::others(&book, node_id)
            .into_iter()
            .filter_map(|(id, _)| (!admission.initial_members.contains(&id)).then_some(id))
            .collect(),
        planned,
    };
    let (cache, writer) = super::spawn_writer(
        &providers,
        disk.clone(),
        audits(AuditScope::Machine),
        node_id,
        admitted.cached.clone(),
        &shutdown,
    );
    let mut stores = MachineStores::admitted(disk, record, admission.cell.cell, audits)
        .with_planned(&control.planned);
    // A storage machine hosts the matchmaker sets the registry books on it
    // (#501 (matchmaker sets off the founders)): never a member of a
    // bootstrap generation, it opens only those.
    if admitted.facts.class.stores() {
        stores = stores.with_matchmakers(MatchmakerConfig {
            id: MatchmakerId(node_id.0),
            bootstrap: control.matchmakers.clone(),
        });
    }
    // Every machine of its book is linked, as every machine links its book
    // (#685 (no founders)); its own journals register with the sets it
    // hosts like with any member, at the address it advertises: a storage
    // machine links itself too.
    let mut linked = paros_model::machine::others(&book, node_id);
    if admitted.facts.class.stores() {
        linked.push((node_id, admitted.facts.addr.clone()));
    }
    let matchmakers = links(&linked, (node_id, &admitted.facts.addr.to_string()));
    assert!(
        matchmakers
            .iter()
            .filter(|(id, _)| id.0 == node_id.0)
            .count()
            == usize::from(admitted.facts.class.stores()),
        "an admitted machine links itself only when it stores"
    );
    let listen = admitted.facts.listen;
    tracing::info!(machine = %admitted.facts.name, addr = %admitted.facts.addr, %listen, "machine_serving_admitted_journals");
    admitted.facts.metrics.set_phase(Phase::Serving);
    let ran = paros_roles::node::run_journals(
        providers,
        stores,
        node_id,
        listen.to_string(),
        initial_members,
        matchmakers,
        Vec::new(),
        Vec::new(),
        Some(control),
        Some(CellMachine::Admitted {
            machine: admitted,
            cache,
        }),
        *tunables,
        transport,
        shutdown,
    )
    .await;
    writer.stop().await;
    ran.map_err(MachineError::Run)
}

/// The record on `disk` of machine `node`, as it stands now.
async fn read_record<S: LocalStorageProvider + Clone>(
    disk: &ProviderDisk<S>,
    node: NodeId,
) -> Result<MachineRecord, MachineError> {
    let record = disk
        .read_record()
        .await
        .map_err(|e| MachineError::Storage(format!("machine record: {e}")))?
        .ok_or_else(|| MachineError::Storage("machine record: gone after its first write".into()))
        .and_then(|text| {
            MachineRecord::parse(&text)
                .map_err(|e| MachineError::Refused(format!("machine record: {e}")))
        })?;
    assert_eq!(record.node_id, node, "the record names the serving machine");
    Ok(record)
}

#[cfg(test)]
mod tests;
