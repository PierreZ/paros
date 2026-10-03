//! A matchmaker's store (#176): the library's shipped
//! [`JournalMatchmakerStorage`] on the simulated disk, with the one fault
//! the harness draws at its seam.
//!
//! There is deliberately **no matchmaker-specific fault story** (#119): torn
//! writes, checksums and rot are generic storage concerns already modelled
//! on the nodes' journals, the registry's crash seams live in the driver,
//! and a matchmaker whose state is lost for good is *replaced* through a
//! matchmaker-set reconfiguration (#125), never repaired in place. What the
//! registry does draw is the **whole-batch fsync failure** of the node's own
//! write path ([`StorageFaults::fsync_fail`], the same seed-drawn rate): the
//! matchmaker driver's fail-stop arm and the replacement path that follows
//! it were otherwise reachable only through a seam crash, which is a
//! *clean* loss. Its floor is the world's budget — at most `quorum - 1` of
//! the bootstrap set may ever fail a sync — so a matchmaking quorum always
//! survives. On the failing leg nothing reaches the disk: the stage dies
//! with the store the driver drops.
//!
//! The world keeps a shadow of the registry (refreshed from the store at
//! every boot and every sync that returned) for the corpus's probes, and
//! the operator's provisioning ledger in two steps, like a node's
//! ([`super::node_store`]).

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError, Weak};

use moonpool_sim::{
    SimStorageProvider, SimTimeProvider, assert_always, assert_reachable, buggify_with_prob,
};
use paros::{
    Ballot, JournalMatchmakerStorage, JournalStoreConfig, MatchmakerConfig, MatchmakerHardState,
    MatchmakerStorage, Registration, RegistryStorage, StorageError, StorageRecord, WriteOutcome,
};

use super::StorageWorld;
use super::faults::StorageFaults;

/// The directory a matchmaker's registry lives in on its disk.
pub(crate) const REGISTRY_DIR: &str = "matchmaker";

/// A matchmaker's store (see the module doc).
pub(crate) struct SimRegistry {
    inner: JournalMatchmakerStorage<SimStorageProvider>,
    world: Weak<Mutex<StorageWorld>>,
    key: String,
    faults: StorageFaults<SimTimeProvider>,
    bootstrap: usize,
    /// A write was staged since the last sync.
    dirty: bool,
    /// A format was staged and its sync has not returned yet.
    format_pending: bool,
}

impl SimRegistry {
    /// The registry of matchmaker `key` on `provider`, failing its fsync
    /// at `faults`' rate within the budget of a `bootstrap`-sized set.
    pub(crate) fn new(
        provider: SimStorageProvider,
        layout: JournalStoreConfig,
        world: Weak<Mutex<StorageWorld>>,
        key: String,
        faults: StorageFaults<SimTimeProvider>,
        bootstrap: usize,
    ) -> Self {
        Self {
            inner: JournalMatchmakerStorage::new(provider, REGISTRY_DIR, layout),
            world,
            key,
            faults,
            bootstrap,
            dirty: false,
            format_pending: false,
        }
    }

    fn with_world<R>(&self, f: impl FnOnce(&mut StorageWorld) -> R) -> Option<R> {
        self.world
            .upgrade()
            .map(|world| f(&mut world.lock().unwrap_or_else(PoisonError::into_inner)))
    }

    /// Note a write the store took. A fault the simulated disk handed back
    /// is not counted in the nodes' one-crash-per-fault ledger: a
    /// matchmaker's crash decision is the matchmaker audit's, and the
    /// driver fail-stops on it either way.
    fn ledger<T>(&mut self, result: Result<T, StorageError>) -> Result<T, StorageError> {
        self.dirty |= result.is_ok();
        result
    }

    /// The shadow of the registry, from the store.
    fn refresh(&self) {
        let registry: BTreeMap<Ballot, Registration> = self
            .inner
            .registered_ballots()
            .into_iter()
            .filter_map(|ballot| {
                self.inner
                    .registration(ballot)
                    .map(|registration| (ballot, registration))
            })
            .collect();
        let key = self.key.clone();
        self.with_world(|world| {
            world.matchmakers.insert(key, registry);
        });
    }
}

impl RegistryStorage for SimRegistry {
    fn initial_state(&self) -> MatchmakerHardState {
        self.inner.initial_state()
    }

    fn registration(&self, ballot: Ballot) -> Option<Registration> {
        self.inner.registration(ballot)
    }

    fn registered_ballots(&self) -> Vec<Ballot> {
        self.inner.registered_ballots()
    }
}

impl MatchmakerStorage for SimRegistry {
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.dirty = false;
        self.format_pending = false;
        let scanned = self.inner.boot_scan().await;
        let scanned = self.ledger(scanned);
        self.dirty = false;
        scanned?;
        self.refresh();
        Ok(())
    }

    fn formatted_config(&self) -> Option<MatchmakerConfig> {
        self.inner.formatted_config()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn format(&mut self, config: &MatchmakerConfig) -> Result<(), StorageError> {
        let key = self.key.clone();
        self.with_world(|world| world.note_provisioning(&key));
        let formatted = self.inner.format(config).await;
        self.ledger(formatted)?;
        self.format_pending = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    async fn register(
        &mut self,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        // Write-once, seen from the disk: a re-write of a durably registered
        // ballot carries the same bytes (the core never re-registers).
        let key = self.key.clone();
        let durable = self
            .with_world(|world| {
                world
                    .matchmakers
                    .get(&key)
                    .and_then(|registry| registry.get(&ballot).cloned())
            })
            .flatten();
        if let Some(previous) = durable {
            assert_always!(
                previous == *registration,
                "matchmaker: a durable registration is never overwritten with different bytes",
                { "round" => ballot.round, "bnode" => ballot.node.0 }
            );
        }
        let result = self.inner.register(ballot, registration).await;
        self.ledger(result)
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    async fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        let result = self.inner.set_gc_watermark(watermark).await;
        self.ledger(result)
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        let result = self.inner.set_scalars(scalars).await;
        self.ledger(result)
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &BTreeMap<Ballot, Registration>,
    ) -> Result<(), StorageError> {
        let result = self.inner.install_registry(scalars, registrations).await;
        self.ledger(result)
    }

    /// The fsync: on the failing leg (budgeted by the world, so a quorum of
    /// the bootstrap set is never sick at once) nothing reaches the disk and
    /// the driver fail-stops on the error.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn sync(&mut self) -> Result<(), StorageError> {
        if !std::mem::take(&mut self.dirty) {
            return self.inner.sync().await;
        }
        if self.faults.active() && buggify_with_prob!(self.faults.fsync_fail()) {
            let key = self.key.clone();
            let bootstrap = self.bootstrap;
            let permitted = self
                .with_world(|w| w.permit_matchmaker_sync_failure(&key, bootstrap))
                .unwrap_or(false);
            if permitted {
                // BUGGIFY pairing: the registry's fsync genuinely fails.
                assert_reachable!("matchmaker: a registry fsync fails");
                return Err(StorageError::FsyncFailed {
                    record: StorageRecord::Batch,
                    outcome: WriteOutcome::Lost,
                });
            }
        }
        let synced = self.inner.sync().await;
        self.ledger(synced)?;
        self.dirty = false;
        if std::mem::take(&mut self.format_pending) {
            // The operator's provisioning ledger (#147, #183) records the
            // matchmaker exactly when its marker lands durably.
            let key = self.key.clone();
            self.with_world(|world| world.note_provisioned(&key));
        }
        self.refresh();
        Ok(())
    }
}
