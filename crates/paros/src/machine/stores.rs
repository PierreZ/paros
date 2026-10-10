//! A formed machine's journal stores (#246, #294): one [`JournalStorage`]
//! per journal of its plan on its [`ProviderDisk`], every one an existing
//! member's (the formation formatted them). `parosd` and the simulation
//! open the same stores; only the provider and the audit port differ.
//!
//! A founding member also serves the journals its cell's control journals
//! create at run time (#210): every hosted tenant's control journal, and
//! every journal a tenant creates naming it. Such a store is
//! **provisioned** at its first open, as the operator's own act (#208):
//! formatted durably, then recorded in the machine record (`created`), and
//! only then booted, as an existing member's. A restart opens it as an
//! existing member's again, so a store the record names and the disk lost
//! is refused as amnesia.

use std::collections::BTreeMap;

use moonpool_core::StorageProvider;
use paros_core::{Config, JournalIdentifier, NodeId};

use super::record::MachineRecord;
use super::{CacheSink, CachedRegistry, ProviderDisk};
use crate::{Audit, BootKind, JournalStorage, JournalStores};

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
pub(crate) struct MachineStores<S, F> {
    disk: ProviderDisk<S>,
    node: NodeId,
    genesis: BTreeMap<JournalIdentifier, Config>,
    /// Journals created at run time, with their configuration.
    created: BTreeMap<JournalIdentifier, Config>,
    /// The machine record, rewritten whole when a store is provisioned.
    record: MachineRecord,
    audits: F,
    /// Where the node's registry fold offers each new book (#211).
    cache: Option<CacheSink>,
}

impl<S, F> MachineStores<S, F> {
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
            audits,
            cache: None,
        }
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

impl<S: StorageProvider + Clone + 'static, F> MachineStores<S, F> {
    fn store(&self, journal: JournalIdentifier, config: Config) -> JournalStorage<S> {
        JournalStorage::new(
            self.disk.provider().clone(),
            self.disk.journal_dir(journal),
            config,
            self.disk.layout(),
        )
    }

    /// Provision created journal `journal` under `config`: format its store
    /// (an interrupted run resumes from the disk), then record it. `false`
    /// when the disk failed: the journal is down here until a restart.
    async fn provision(&mut self, journal: JournalIdentifier, config: Config) -> bool {
        let mut store = self.store(journal, config);
        match crate::provision_store(&mut store).await {
            Ok(provisioned) => {
                if provisioned == crate::Provisioned::Resumed {
                    moonpool_assertions::reachable!(
                        "machine: a created journal's provisioning resumed from the disk"
                    );
                    // The marker may be one a failed format sync left staged
                    // in this process's file image (#348): make it durable
                    // before the record names the journal, or a power loss
                    // after the record would boot it as amnesia.
                    let dir = self.disk.journal_dir(journal);
                    if let Err(error) = crate::journal::settle(self.disk.provider(), &dir).await {
                        tracing::warn!(journal = %journal, %error, "created_journal_unsettled");
                        return false;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(journal = %journal, %error, "created_journal_unprovisioned");
                return false;
            }
        }
        // The record is the commit point: the line is written after the
        // format, before the first boot.
        let mut record = self.record.clone();
        record.created.push(journal);
        if let Err(error) = self.disk.write_record(&record.render()).await {
            tracing::warn!(journal = %journal, %error, "created_journal_unrecorded");
            return false;
        }
        self.record = record;
        true
    }
}

impl<S, A, F> JournalStores for MachineStores<S, F>
where
    S: StorageProvider + Clone + 'static,
    A: Audit + Clone + Send + Sync + 'static,
    F: Fn(AuditScope) -> A,
{
    type Store = JournalStorage<S>;
    type Audit = A;

    fn journals(&self) -> Vec<JournalIdentifier> {
        self.genesis.keys().copied().collect()
    }

    async fn open(&mut self, journal: JournalIdentifier) -> Option<(Self::Store, BootKind)> {
        if let Some(config) = self.genesis.get(&journal).cloned() {
            assert!(config.id == self.node, "a store is opened as its machine");
            return Some((self.store(journal, config), BootKind::ExistingMember));
        }
        let config = self.created.get(&journal)?.clone();
        assert!(config.id == self.node, "a store is opened as its machine");
        if !self.record.created.contains(&journal) && !self.provision(journal, config.clone()).await
        {
            return None;
        }
        assert!(
            self.record.created.contains(&journal),
            "a created journal is recorded before its first boot"
        );
        Some((self.store(journal, config), BootKind::ExistingMember))
    }

    fn audit(&self, journal: JournalIdentifier) -> A {
        assert!(
            self.genesis.contains_key(&journal) || self.created.contains_key(&journal),
            "a machine audits only the journals it serves"
        );
        (self.audits)(AuditScope::Journal(journal))
    }

    /// A journal a control journal created naming this machine (#210),
    /// under `config`. Idempotent: a restart re-folds and asks again.
    fn create(&mut self, journal: JournalIdentifier, config: Config) -> bool {
        assert!(
            config.id == self.node,
            "a created journal names its machine"
        );
        if self.genesis.contains_key(&journal) {
            return false;
        }
        self.created.entry(journal).or_insert(config);
        true
    }

    fn cache_registry(&self, cache: CachedRegistry) {
        assert_eq!(cache.node, self.node, "a machine caches its own fold");
        if let Some(sink) = &self.cache {
            sink.offer(cache);
        }
    }

    fn node_audit(&self) -> A {
        let first = *self
            .genesis
            .keys()
            .next()
            .expect("a formed machine serves journals");
        (self.audits)(AuditScope::Node(first))
    }
}
