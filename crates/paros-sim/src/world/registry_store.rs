//! A matchmaker's store on a journal-store seed (#176): the library's
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
//! through ([`PowerCut`]); a seam crash is a power loss (`crate::process`).
//! The world-store registry's budgeted fsync failure has no counterpart:
//! moonpool's storage chaos fails syncs on the simulated disk itself.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use moonpool_sim::{SimStorageProvider, assert_reachable, assert_sometimes};
use paros::{
    Ballot, JournalIdentifier, JournalMatchmakerStorage, MatchmakerConfig, MatchmakerHardState,
    MatchmakerStorage, Registration, RegistryStorage, StorageError,
};

use super::StorageWorld;
use super::power::PowerCut;
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
    /// How this matchmaker can lose power in the middle of a commit.
    power: PowerCut,
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
        power: PowerCut,
        checker: Arc<AuditWorld>,
        matchmaker: u64,
        cut_budget: Option<usize>,
    ) -> Self {
        Self {
            inner: JournalMatchmakerStorage::new(provider, REGISTRY_DIR, id, layout),
            world,
            ip,
            format_pending: false,
            power,
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
        let (budget_world, budget_ip, budget) =
            (self.world.clone(), self.ip.clone(), self.cut_budget);
        let permit = move || {
            budget.is_none_or(|bootstrap| {
                budget_world.upgrade().is_some_and(|world| {
                    world
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .permit_matchmaker_power_cut(&budget_ip, bootstrap)
                })
            })
        };
        let world = self.world.clone();
        let ip = self.ip.clone();
        let on_cut = move || {
            if held && let Some(world) = world.upgrade() {
                world
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .note_registry_cut(&ip);
            }
        };
        // What a crash from here to the driver's report may land unreported.
        let ops = std::mem::take(&mut self.staged);
        if !ops.is_empty() {
            self.checker
                .note_registry_in_flight(self.matchmaker, Some(ops));
        }
        self.power
            .around(writes, permit, on_cut, self.inner.sync())
            .await?;
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

/// A matchmaker's store, whichever the seed drew: the world-backed registry
/// or the journal one (see the module doc).
pub(crate) enum RegistryStore {
    /// The world-backed registry: its budgeted fsync failure, its ledger.
    World(Box<super::matchmaker::DurableMatchmakerStorage<moonpool_sim::SimTimeProvider>>),
    /// The shipped journal registry on the simulated disk (#176).
    Journal(Box<LedgeredRegistry>),
}

impl RegistryStorage for RegistryStore {
    fn initial_state(&self) -> MatchmakerHardState {
        match self {
            Self::World(s) => s.initial_state(),
            Self::Journal(s) => s.initial_state(),
        }
    }

    fn registration(&self, ballot: Ballot) -> Option<Registration> {
        match self {
            Self::World(s) => s.registration(ballot),
            Self::Journal(s) => s.registration(ballot),
        }
    }

    fn registered_ballots(&self) -> Vec<Ballot> {
        match self {
            Self::World(s) => s.registered_ballots(),
            Self::Journal(s) => s.registered_ballots(),
        }
    }
}

impl MatchmakerStorage for RegistryStore {
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.boot_scan().await,
            Self::Journal(s) => s.boot_scan().await,
        }
    }

    fn formatted_config(&self) -> Option<MatchmakerConfig> {
        match self {
            Self::World(s) => s.formatted_config(),
            Self::Journal(s) => s.formatted_config(),
        }
    }

    async fn format(&mut self, config: &MatchmakerConfig) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.format(config).await,
            Self::Journal(s) => s.format(config).await,
        }
    }

    async fn register(
        &mut self,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.register(ballot, registration).await,
            Self::Journal(s) => s.register(ballot, registration).await,
        }
    }

    async fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.set_gc_watermark(watermark).await,
            Self::Journal(s) => s.set_gc_watermark(watermark).await,
        }
    }

    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.set_scalars(scalars).await,
            Self::Journal(s) => s.set_scalars(scalars).await,
        }
    }

    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &BTreeMap<Ballot, Registration>,
    ) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.install_registry(scalars, registrations).await,
            Self::Journal(s) => s.install_registry(scalars, registrations).await,
        }
    }

    async fn sync(&mut self) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.sync().await,
            Self::Journal(s) => s.sync().await,
        }
    }
}
