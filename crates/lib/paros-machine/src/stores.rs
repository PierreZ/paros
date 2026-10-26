//! A machine's journal stores (#246, #294): one [`JournalStorage`] per
//! journal of a founding member's plan on its [`ProviderDisk`], every one an
//! existing member's (the formation formatted them). `parosd` and the
//! simulation open the same stores; only the provider and the audit port
//! differ. An admitted machine has no plan journal (#474).
//!
//! Every machine of a cell also serves the journals its cell's control
//! journals place on it at run time (#210, #474): a founding member every
//! hosted tenant's control journal, and any storage machine every journal
//! a tenant created and the registry booked on it. Such a store is
//! **provisioned** at its first open, as the operator's own act (#208):
//! formatted durably, then recorded in the machine record (`created`), and
//! only then booted, as an existing member's. A restart opens it as an
//! existing member's again, so a store the record names and the disk lost
//! is refused as amnesia.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};

use moonpool_buggify::hint::Strike;
use moonpool_core::{LocalStorageProvider, SimulationResult};
use moonpool_rpc::RpcHandle;
use paros_core::{Config, JournalIdentifier, MatchmakerConfig, NodeId};
use paros_net::runtime::ParosProviders;
use tokio_util::sync::CancellationToken;

use super::matchmakers::SetBookings;
use super::{CacheSink, CachedRegistry, ProviderDisk};
use paros_model::machine::MachineRecord;
use paros_observe::audit::Audit;
use paros_roles::node::JournalStores;
use paros_storage::boot::BootKind;
use paros_storage::boot::RunError;
use paros_storage::journal::Durability;
use paros_storage::journal::JournalStorage;
use paros_storage::journal::JournalStoreConfig;
use paros_storage::{StorageError, StorageRecord, WriteOutcome};
use paros_tunables::DriverTunables;

/// Which audit port a machine's facts report to, named by the caller of
/// [`super::run_machine`] (`parosd` passes `NoAudit` for all three).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditScope {
    /// The lifecycle's own facts, before the machine serves a cell: its
    /// boot, its record's rewrites, its formation.
    Machine,
    /// The serving node's own facts (what no single journal owns), reported
    /// beside its plan's first journal.
    Node(JournalIdentifier),
    /// One journal's facts.
    Journal(JournalIdentifier),
}

/// A formed machine's stores.
pub(crate) struct MachineStores<S: LocalStorageProvider, F> {
    disk: ProviderDisk<S>,
    node: NodeId,
    genesis: BTreeMap<JournalIdentifier, Config>,
    /// Journals created at run time, with their configuration and the
    /// commit protocol their create recorded (#599 (durability mode per
    /// journal)).
    created: BTreeMap<JournalIdentifier, (Config, Durability)>,
    /// The machine record, rewritten whole when a store is provisioned.
    record: MachineRecord,
    audits: Arc<F>,
    /// Where the node's registry fold offers each new book (#211).
    cache: Option<CacheSink>,
    /// The matchmaker this machine is, when it hosts its cell's sets: a
    /// founding member's (#190), or a storage machine's that the registry
    /// books a set on (#501 (matchmaker sets off the founders)).
    matchmaker: Option<MatchmakerConfig>,
    /// The tenants whose set the registry books here, shared with the
    /// matchmaker host.
    sets: SetBookings,
    /// The journal the node's own facts are reported beside: the plan's
    /// first journal on a founding member, the cell control journal on an
    /// admitted machine (#474), which serves no journal at its first start.
    home: JournalIdentifier,
    /// The storage fault the last provisioning of a created journal took,
    /// handed to the driver once so it quarantines the journal.
    open_faults: BTreeMap<JournalIdentifier, RunError>,
}

impl<S: LocalStorageProvider, F> MachineStores<S, F> {
    /// The stores of member `node` serving `genesis` on `disk`.
    ///
    /// # Panics
    ///
    /// When `genesis` is empty, or names a configuration of another node.
    pub(crate) fn new(
        disk: ProviderDisk<S>,
        record: MachineRecord,
        genesis: BTreeMap<JournalIdentifier, Config>,
        audits: F,
    ) -> Self {
        let node = record.node_id;
        assert!(!genesis.is_empty(), "a formed machine serves journals");
        let home = *genesis
            .keys()
            .next()
            .expect("a formed machine serves journals");
        assert!(
            genesis.values().all(|config| config.id == node),
            "a machine's stores are opened as that machine"
        );
        assert!(
            record.created.iter().all(|j| !genesis.contains_key(j)),
            "a plan journal is never provisioned at run time"
        );
        Self {
            disk,
            node,
            genesis,
            created: BTreeMap::new(),
            record,
            audits: Arc::new(audits),
            cache: None,
            matchmaker: None,
            sets: SetBookings::default(),
            home,
            open_faults: BTreeMap::new(),
        }
    }

    /// The stores of the admitted machine `record` names (#474): no journal
    /// of a plan, only the journals placed on it, each provisioned at its
    /// first open. Its own facts are reported beside the cell control
    /// journal `cell`.
    ///
    /// # Panics
    ///
    /// When `record` holds no admission, or `cell` is unset.
    pub(crate) fn admitted(
        disk: ProviderDisk<S>,
        record: MachineRecord,
        cell: JournalIdentifier,
        audits: F,
    ) -> Self {
        assert!(record.admitted.is_some(), "an admitted machine's record");
        assert!(cell.is_set(), "a cell names its control journal");
        Self {
            disk,
            node: record.node_id,
            genesis: BTreeMap::new(),
            created: BTreeMap::new(),
            record,
            audits: Arc::new(audits),
            cache: None,
            matchmaker: None,
            sets: SetBookings::default(),
            home: cell,
            open_faults: BTreeMap::new(),
        }
    }

    /// These stores, hosting the matchmaker sets of `config` on the node's
    /// edge (#190): every set on a founding member, and the sets booked
    /// here on any other machine (#501 (matchmaker sets off the founders)).
    #[must_use]
    pub(crate) fn with_matchmakers(mut self, config: MatchmakerConfig) -> Self {
        assert!(self.matchmaker.is_none(), "a machine is one matchmaker");
        assert_eq!(
            config.id.0, self.node.0,
            "a machine's matchmaker is named by its node id"
        );
        self.matchmaker = Some(config);
        self
    }

    /// These stores, offering each book the registry fold reaches to
    /// `sink`, the machine's cached registry fold (#211).
    #[must_use]
    pub(crate) fn with_cache(mut self, sink: CacheSink) -> Self {
        assert!(self.cache.is_none(), "a machine has one registry cache");
        self.cache = Some(sink);
        self
    }
}

impl<S: LocalStorageProvider + Clone + 'static, F> MachineStores<S, F> {
    /// `journal`'s store under `config`, committing under `durability`: a
    /// plan journal's is the disk's (ordered), a created journal's the one
    /// its create recorded (#599 (durability mode per journal)). The
    /// journal records the protocol per batch, so a store opens under
    /// either whatever its earlier batches used.
    fn store(
        &self,
        journal: JournalIdentifier,
        config: Config,
        durability: Durability,
    ) -> JournalStorage<S> {
        let layout = JournalStoreConfig {
            durability,
            ..self.disk.layout()
        };
        assert!(
            layout.geometry == self.disk.layout().geometry,
            "a journal's mode changes its commits, never its geometry"
        );
        JournalStorage::new(
            self.disk.provider().clone(),
            self.disk.journal_dir(journal),
            config,
            layout,
        )
    }

    /// Provision created journal `journal` under `config`: format its store
    /// (an interrupted run resumes from the disk), then record it. `false`
    /// when it did not: on a storage fault the driver quarantines the
    /// journal and provisions it again at its re-open, else the journal is
    /// down here until a restart.
    async fn provision(
        &mut self,
        journal: JournalIdentifier,
        (config, durability): (Config, Durability),
    ) -> bool {
        let mut store = self.store(journal, config, durability);
        match paros_storage::provision::provision_store(&mut store).await {
            Ok(provisioned) => {
                if provisioned == paros_storage::provision::Provisioned::Resumed {
                    moonpool_assertions::reachable!(
                        "machine: a created journal's provisioning resumed from the disk"
                    );
                    // The marker may be one a failed format sync left staged
                    // in this process's file image (#348): make it durable
                    // before the record names the journal, or a power loss
                    // after the record would boot it as amnesia.
                    let dir = self.disk.journal_dir(journal);
                    if let Err(error) =
                        paros_storage::journal::settle(self.disk.provider(), &dir).await
                    {
                        tracing::warn!(journal = %journal, %error, "created_journal_unsettled");
                        self.open_faults.insert(journal, disk_fault());
                        return false;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(journal = %journal, %error, "created_journal_unprovisioned");
                // A storage fault is the disk's, not the journal's: the
                // driver quarantines the journal and provisions it again at
                // its re-open (#475 (re-place after the Down bound): a
                // one-acceptor journal parked for good here had no leader
                // anywhere, witness 16441966175508397884 on 757fa48).
                if matches!(error, RunError::Storage(_)) {
                    self.open_faults.insert(journal, error);
                }
                return false;
            }
        }
        // Hint: formatted, not recorded. A machine killed here formats the
        // journal again at its next request, and resumes from the marker
        // (#446 (a gate that never fires cannot fail the sweep): without
        // this moment the resume above never ran in the sweep).
        let hinted = moonpool_buggify::hint!("created journal formatted, not recorded", 0.25);
        if hinted.strike() == Strike::Killed {
            moonpool_assertions::reachable!(
                "machine: a machine dies between a journal's format and record"
            );
        }
        hinted.await;
        // The record is the commit point: the line is written after the
        // format, before the first boot.
        let mut record = self.record.clone();
        record.created.push(journal);
        if let Err(error) = self.disk.write_record(&record.render()).await {
            tracing::warn!(journal = %journal, %error, "created_journal_unrecorded");
            // The record's write is the disk's fault too: a journal parked
            // here for good left a move's new configuration with no member
            // to elect a leader on (recipe 11674261066061092807
            // 944091:3485147030935198268 on 6d6f7f0: a directory sync failed).
            self.open_faults.insert(journal, disk_fault());
            return false;
        }
        self.record = record;
        true
    }
}

/// The fault a provisioning that failed past the format takes: the disk's,
/// at the store's granularity, its effect unknown.
fn disk_fault() -> RunError {
    RunError::Storage(StorageError::Io {
        record: StorageRecord::Store,
        outcome: WriteOutcome::Unknown,
    })
}

impl<S, A, F> JournalStores for MachineStores<S, F>
where
    S: LocalStorageProvider + Clone + 'static,
    A: Audit + Clone + Send + Sync + 'static,
    F: Fn(AuditScope) -> A + Send + Sync + 'static,
{
    type Store = JournalStorage<S>;
    type Audit = A;

    fn journals(&self) -> Vec<JournalIdentifier> {
        self.genesis.keys().copied().collect()
    }

    fn provisioned(&self) -> Vec<JournalIdentifier> {
        self.record.created.clone()
    }

    async fn open(&mut self, journal: JournalIdentifier) -> Option<(Self::Store, BootKind)> {
        if let Some(config) = self.genesis.get(&journal).cloned() {
            assert!(config.id == self.node, "a store is opened as its machine");
            let durability = self.disk.layout().durability;
            return Some((
                self.store(journal, config, durability),
                BootKind::ExistingMember,
            ));
        }
        let (config, durability) = self.created.get(&journal)?.clone();
        assert!(config.id == self.node, "a store is opened as its machine");
        if !self.record.created.contains(&journal)
            && !self.provision(journal, (config.clone(), durability)).await
        {
            return None;
        }
        assert!(
            self.record.created.contains(&journal),
            "a created journal is recorded before its first boot"
        );
        Some((
            self.store(journal, config, durability),
            BootKind::ExistingMember,
        ))
    }

    fn open_fault(&mut self, journal: JournalIdentifier) -> Option<RunError> {
        let fault = self.open_faults.remove(&journal);
        assert!(
            fault
                .as_ref()
                .is_none_or(|f| matches!(f, RunError::Storage(_))),
            "only a storage fault quarantines an unopened journal"
        );
        fault
    }

    fn audit(&self, journal: JournalIdentifier) -> A {
        assert!(
            self.genesis.contains_key(&journal) || self.created.contains_key(&journal),
            "a machine audits only the journals it serves"
        );
        (self.audits)(AuditScope::Journal(journal))
    }

    /// A journal a control journal created naming this machine (#210),
    /// under `config`, committing under `durability` (#599 (durability mode
    /// per journal)). Idempotent: a restart re-folds and asks again.
    fn create(
        &mut self,
        journal: JournalIdentifier,
        config: Config,
        durability: Durability,
    ) -> bool {
        assert!(
            config.id == self.node,
            "a created journal names its machine"
        );
        if self.genesis.contains_key(&journal) {
            return false;
        }
        let held = self.created.entry(journal).or_insert((config, durability));
        // A journal's mode is fixed at its create: every ask names the same.
        assert!(
            held.1 == durability,
            "a created journal keeps the mode its create recorded"
        );
        true
    }

    fn host_on_edge<P: ParosProviders>(
        &mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        machine: Option<&moonpool_rpc::ServiceGroup<paros_wire::methods::MachineInterface>>,
        (tunables, cell_id): (&DriverTunables, u64),
        shutdown: CancellationToken,
    ) -> SimulationResult<Option<paros_roles::node::HostEnded>> {
        assert!(cell_id != 0, "a machine hosts its cell's sets");
        let Some(config) = self.matchmaker.clone() else {
            return Ok(None);
        };
        let audits = Arc::clone(&self.audits);
        super::matchmakers::spawn(
            providers,
            rpc,
            machine,
            self.disk.clone(),
            (config, Arc::clone(&self.sets), cell_id),
            tunables,
            Arc::new(move |journal| audits(AuditScope::Journal(journal))),
            shutdown,
        )
        .map(Some)
    }

    fn book_sets(&mut self, tenants: std::collections::BTreeSet<paros_core::TenantId>) {
        let mut sets = self.sets.lock().unwrap_or_else(PoisonError::into_inner);
        if *sets != tenants {
            tracing::info!(
                node = self.node.0,
                sets = tenants.len(),
                "matchmaker_sets_booked"
            );
            *sets = tenants;
        }
    }

    fn cache_registry(&self, cache: CachedRegistry) {
        assert_eq!(cache.node, self.node, "a machine caches its own fold");
        if let Some(sink) = &self.cache {
            sink.offer(cache);
        }
    }

    fn node_audit(&self) -> A {
        (self.audits)(AuditScope::Node(self.home))
    }
}
