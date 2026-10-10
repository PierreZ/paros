//! A matchmaker's store (#176, #261): the library's
//! [`JournalMatchmakerStorage`] over the simulated disk, the registry a
//! `parosd` matchmaker ships, under protocol chaos.
//!
//! Like [`super::node_store::LedgeredJournal`] for an acceptor, what the
//! world still owns here is the operator's **provisioning ledger** (#147,
//! #183): the format marker lands only with the sync after the format, so
//! the ledger records the provisioning in two steps (begun at the format,
//! landed when that sync returns), and a process killed in between leaves the
//! operator honestly unsure, which the next boot resolves by reading the disk
//! ([`resolve_registry_provisioning`]). A commit may lose power partway
//! through, at the store's own `hint!`s, budgeted by [`super::cut`].
//! moonpool's storage chaos fails syncs on the simulated disk itself.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use moonpool_sim::{SimStorageProvider, StateHandle, assert_reachable, assert_sometimes};
use paros::{
    Ballot, JournalIdentifier, JournalMatchmakerStorage, MatchmakerConfig, MatchmakerHardState,
    MatchmakerStorage, Registration, RegistryStorage, StorageError,
};

use super::StorageWorld;
use super::cut::{Budget, InFlight, Owner};
use crate::audit::{AuditWorld, RegistryOp};

/// The directory a matchmaker's registry lives in on its simulated disk.
pub(crate) const REGISTRY_DIR: &str = "paros/matchmaker";

/// The journal registry, keeping the world's provisioning ledger in step
/// with its format marker (see the module doc).
pub(crate) struct LedgeredRegistry {
    inner: JournalMatchmakerStorage<SimStorageProvider>,
    world: Weak<Mutex<StorageWorld>>,
    ip: String,
    /// A format was staged and its sync has not returned yet.
    format_pending: bool,
    /// The run's state: where a commit in flight registers for the cut's
    /// budget ([`super::cut`]).
    state: StateHandle,
    /// The shared checker, told what each commit has in flight.
    checker: Arc<AuditWorld>,
    /// This matchmaker's id, the checker's key.
    matchmaker: u64,
    /// The writes staged since the last sync, in order: what the next
    /// commit may land without a report.
    staged: Vec<RegistryOp>,
    /// On a `Batched` registry, the bootstrap set's size: a cut may leave an
    /// ambiguous registration, a crash verdict, so it is the run's one
    /// matchmaker loss ([`StorageWorld::permit_matchmaker_power_cut`]).
    /// `None` on an `Ordered` registry, never ambiguous.
    cut_budget: Option<usize>,
}

impl LedgeredRegistry {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        provider: SimStorageProvider,
        id: JournalIdentifier,
        layout: paros::JournalStoreConfig,
        world: Weak<Mutex<StorageWorld>>,
        ip: String,
        state: StateHandle,
        checker: Arc<AuditWorld>,
        matchmaker: u64,
        cut_budget: Option<usize>,
    ) -> Self {
        Self {
            inner: JournalMatchmakerStorage::new(provider, REGISTRY_DIR, id, layout),
            world,
            ip,
            format_pending: false,
            state,
            checker,
            matchmaker,
            staged: Vec::new(),
            cut_budget,
        }
    }

    fn with_world<R>(&self, f: impl FnOnce(&mut StorageWorld) -> R) -> Option<R> {
        self.world
            .upgrade()
            .map(|world| f(&mut world.lock().unwrap_or_else(PoisonError::into_inner)))
    }
}

/// Resolve an interrupted provisioning of the matchmaker at `ip` before a
/// boot, the registry's twin of the acceptor's: a store that carries the
/// marker was provisioned, one that does not was not.
#[tracing::instrument(level = "debug", skip_all, fields(ip = %ip))]
pub(crate) async fn resolve_registry_provisioning(
    provider: &SimStorageProvider,
    world: &Mutex<StorageWorld>,
    id: JournalIdentifier,
    ip: &str,
) {
    let ambiguous = world
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .provisioning_ambiguous(ip);
    if !ambiguous {
        return;
    }
    // Durable as it stands first: a readable marker may be staged (#348).
    if !super::settle::settle_store(provider, REGISTRY_DIR).await {
        return;
    }
    let formatted = JournalMatchmakerStorage::peek_formatted(provider, REGISTRY_DIR, id)
        .await
        .unwrap_or(false);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    if formatted {
        guard.note_provisioned(ip);
    } else {
        guard.abandon_provisioning(ip);
    }
    assert_reachable!(
        "journal store: an interrupted matchmaker provisioning is resolved from the disk"
    );
}

impl RegistryStorage for LedgeredRegistry {
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

impl MatchmakerStorage for LedgeredRegistry {
    #[tracing::instrument(level = "debug", skip_all, fields(ip = %self.ip))]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.inner.boot_scan().await?;
        // What the last sync had in flight and this boot shows landed is
        // folded before the driver reports the boot.
        let registry: BTreeMap<Ballot, Registration> = self
            .inner
            .registered_ballots()
            .into_iter()
            .filter_map(|ballot| self.inner.registration(ballot).map(|r| (ballot, r)))
            .collect();
        self.checker.note_registry_recovered(
            paros::MatchmakerId(self.matchmaker),
            &self.inner.initial_state(),
            &registry,
        );
        // The outcome a cut registry must reach: what it held before the
        // cut is still there after it.
        let ip = self.ip.clone();
        if self.with_world(|w| w.take_registry_cut(&ip)) == Some(true) {
            assert_sometimes!(
                !self.inner.registered_ballots().is_empty(),
                "journal store: a matchmaker registration survives a power cut"
            );
        }
        Ok(())
    }

    fn formatted_config(&self) -> Option<MatchmakerConfig> {
        self.inner.formatted_config()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn format(&mut self, config: &MatchmakerConfig) -> Result<(), StorageError> {
        let ip = self.ip.clone();
        self.with_world(|w| w.note_provisioning(&ip));
        self.inner.format(config).await?;
        self.format_pending = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    async fn register(
        &mut self,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        self.staged
            .push(RegistryOp::Register(ballot, registration.clone()));
        self.inner.register(ballot, registration).await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    async fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        self.inner.set_gc_watermark(watermark).await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        self.staged.push(RegistryOp::Scalars(scalars.clone()));
        self.inner.set_scalars(scalars).await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &BTreeMap<Ballot, Registration>,
    ) -> Result<(), StorageError> {
        self.staged
            .push(RegistryOp::Install(scalars.clone(), registrations.clone()));
        self.inner.install_registry(scalars, registrations).await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn sync(&mut self) -> Result<(), StorageError> {
        let writes = self.inner.has_staged();
        let held = !self.inner.registered_ballots().is_empty();
        // What a crash from here to the driver's report may land unreported.
        let ops = std::mem::take(&mut self.staged);
        if !ops.is_empty() {
            self.checker
                .note_registry_in_flight(self.matchmaker, Some(ops));
        }
        // A commit that writes is in flight until the sync returns: a hint
        // that kills the process now spends the matchmaker loss budget.
        let in_flight = writes.then(|| {
            let budget = Budget::Matchmaker {
                bootstrap: self.cut_budget,
                held,
            };
            InFlight::open(
                &self.state,
                &self.ip,
                Owner::Matchmaker,
                budget,
                self.world.clone(),
            )
        });
        let synced = self.inner.sync().await;
        drop(in_flight);
        synced?;
        // Synced: the driver reports the commit next, with nothing between.
        self.checker.note_registry_in_flight(self.matchmaker, None);
        if std::mem::take(&mut self.format_pending) {
            // The marker is durable: the provisioning landed.
            let ip = self.ip.clone();
            self.with_world(|w| w.note_provisioned(&ip));
        }
        Ok(())
    }
}
