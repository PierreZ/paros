//! A formed machine's journal stores (#246, #294): one [`JournalStorage`]
//! per journal of its plan on its [`ProviderDisk`], every one an existing
//! member's (the formation formatted them). `parosd` and the simulation
//! open the same stores; only the provider and the audit port differ.
//!
//! A machine serves no system journal yet (`run_machine` passes no
//! `SystemPlan`), so no journal is created at run time and every store is a
//! genesis one: [`JournalStores::create`] keeps its refusing default.

use std::collections::BTreeMap;

use moonpool_core::StorageProvider;
use paros_core::{Config, JournalIdentifier, NodeId};

use super::ProviderDisk;
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
    audits: F,
}

impl<S, F> MachineStores<S, F> {
    /// The stores of member `node` serving `genesis` on `disk`.
    ///
    /// # Panics
    ///
    /// When `genesis` is empty, or names a configuration of another node.
    pub(crate) fn new(
        disk: ProviderDisk<S>,
        node: NodeId,
        genesis: BTreeMap<JournalIdentifier, Config>,
        audits: F,
    ) -> Self {
        assert!(!genesis.is_empty(), "a formed machine serves journals");
        assert!(
            genesis.values().all(|config| config.id == node),
            "a machine's stores are opened as that machine"
        );
        Self {
            disk,
            node,
            genesis,
            audits,
        }
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
        let config = self.genesis.get(&journal)?.clone();
        assert!(config.id == self.node, "a store is opened as its machine");
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

    fn audit(&self, journal: JournalIdentifier) -> A {
        assert!(
            self.genesis.contains_key(&journal),
            "a machine audits only the journals it serves"
        );
        (self.audits)(AuditScope::Journal(journal))
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
