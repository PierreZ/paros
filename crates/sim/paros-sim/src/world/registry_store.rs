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

use std::sync::{Mutex, PoisonError, Weak};

use moonpool_sim::{SimStorageProvider, StateHandle, assert_reachable, assert_sometimes};
use paros::core::Ballot;
use paros::core::journal::JournalId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;
use paros::core::matchmaker::MatchmakerConfig;
use paros::core::matchmaker::MatchmakerHardState;
use paros::core::matchmaker::Registration;
use paros::core::matchmaker::RegistryStorage;
use paros::roles::matchmaker::MatchmakerStorage;
use paros::roles::matchmaker::Registrations;
use paros::storage::StorageError;
use paros::storage::journal::JournalMatchmakerStorage;

use super::StorageWorld;
use super::cut::{Budget, InFlight, Owner, release_parked};

/// The directory a matchmaker's registries live in on its simulated disk.
pub(crate) const REGISTRY_DIR: &str = "paros/matchmaker";

/// The directory of `tenant`'s set's registry (#190): one store per set.
pub(crate) fn registry_dir(tenant: TenantId) -> String {
    format!("{REGISTRY_DIR}/{:016x}", tenant.0)
}

/// The identity `tenant`'s set's store is stamped with: the tenant and the
/// unset journal (a set's store is no journal's).
pub(crate) fn registry_id(tenant: TenantId) -> JournalIdentifier {
    JournalIdentifier::new(tenant, JournalId::UNSET)
}

/// The provisioning ledger's key of `tenant`'s set at `ip` (#190): each set
/// is formatted on its own, so each is provisioned on its own.
pub(crate) fn set_key(ip: &str, tenant: TenantId) -> String {
    format!("{ip}#{:016x}", tenant.0)
}

/// The journal registry, keeping the world's provisioning ledger in step
/// with its format marker (see the module doc).
pub(crate) struct LedgeredRegistry {
    inner: JournalMatchmakerStorage<SimStorageProvider>,
    world: Weak<Mutex<StorageWorld>>,
    ip: String,
    /// The provisioning ledger's key of this set ([`set_key`]).
    key: String,
    /// A format was staged and its sync has not returned yet.
    format_pending: bool,
    /// The run's state: where a commit in flight registers for the cut's
    /// budget ([`super::cut`]).
    state: StateHandle,
    /// On a `Batched` registry, the bootstrap set's size: a cut may leave an
    /// ambiguous registration, a crash verdict, so it is the run's one
    /// matchmaker loss ([`StorageWorld::permit_matchmaker_power_cut`]).
    /// `None` on an `Ordered` registry, never ambiguous.
    cut_budget: Option<usize>,
    /// The run's thin matchmaker sets and this matchmaker's id: a cut never
    /// takes a member of a set that cannot spare it (#496).
    thin: (super::scenarios::thin_sets::ThinSets, u64),
}

impl LedgeredRegistry {
    /// The registry of `tenant`'s set at `ip`. What a commit has in flight
    /// is the driver's to report (`Audit::matchmaker_in_flight`, #190).
    // The store's own arguments, then the budget's (#496: the matchmaker's
    // id, which the thin sets name).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        provider: SimStorageProvider,
        tenant: TenantId,
        layout: paros::storage::journal::JournalStoreConfig,
        world: Weak<Mutex<StorageWorld>>,
        ip: String,
        state: StateHandle,
        cut_budget: Option<usize>,
        matchmaker: u64,
    ) -> Self {
        let thin = (super::scenarios::thin_sets::thin_sets(&state), matchmaker);
        Self {
            inner: JournalMatchmakerStorage::new(
                provider,
                registry_dir(tenant),
                registry_id(tenant),
                layout,
            ),
            world,
            key: set_key(&ip, tenant),
            ip,
            format_pending: false,
            state,
            cut_budget,
            thin,
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
    tenant: TenantId,
    ip: &str,
) {
    let key = set_key(ip, tenant);
    let dir = registry_dir(tenant);
    let ambiguous = world
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .provisioning_ambiguous(&key);
    if !ambiguous {
        return;
    }
    // Durable as it stands first: a readable marker may be staged (#348).
    if !super::settle::settle_store(provider, &dir).await {
        return;
    }
    let formatted = JournalMatchmakerStorage::peek_formatted(provider, &dir, registry_id(tenant))
        .await
        .unwrap_or(false);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    if formatted {
        guard.note_provisioned(&key);
    } else {
        guard.abandon_provisioning(&key);
    }
    assert_reachable!(
        "journal store: an interrupted matchmaker provisioning is resolved from the disk"
    );
}

impl RegistryStorage for LedgeredRegistry {
    fn initial_state(&self) -> MatchmakerHardState {
        self.inner.initial_state()
    }

    fn registration(&self, journal: JournalId, ballot: Ballot) -> Option<Registration> {
        self.inner.registration(journal, ballot)
    }

    fn registered(&self) -> Vec<(JournalId, Ballot)> {
        self.inner.registered()
    }
}

impl MatchmakerStorage for LedgeredRegistry {
    #[tracing::instrument(level = "debug", skip_all, fields(ip = %self.ip))]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.inner.boot_scan().await?;
        // The open synced what it keeps: a batch a failed sync left in the
        // file image is durable or gone.
        release_parked(&self.state, &self.key);
        // The outcome a cut registry must reach: what it held before the
        // cut is still there after it.
        let ip = self.ip.clone();
        if self.with_world(|w| w.take_registry_cut(&ip)) == Some(true) {
            assert_sometimes!(
                !self.inner.registered().is_empty(),
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
        let key = self.key.clone();
        self.with_world(|w| w.note_provisioning(&key));
        self.inner.format(config).await?;
        self.format_pending = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    async fn register(
        &mut self,
        journal: JournalId,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        self.inner.register(journal, ballot, registration).await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    async fn set_gc_watermark(
        &mut self,
        journal: JournalId,
        watermark: Ballot,
    ) -> Result<(), StorageError> {
        self.inner.set_gc_watermark(journal, watermark).await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        self.inner.set_scalars(scalars).await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &Registrations,
    ) -> Result<(), StorageError> {
        self.inner.install_registry(scalars, registrations).await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn sync(&mut self) -> Result<(), StorageError> {
        let writes = self.inner.has_staged();
        let held = !self.inner.registered().is_empty();
        // A commit that writes is in flight until the sync returns: a hint
        // that kills the process now spends the matchmaker loss budget.
        let in_flight = writes.then(|| {
            let budget = Budget::Matchmaker {
                bootstrap: self.cut_budget,
                held,
                thin: self.thin.clone(),
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
        match (&synced, in_flight) {
            // A failed sync leaves its batch volatile in the file image: a
            // kill may still tear it, so the commit stays in flight.
            (Err(_), Some(in_flight)) => in_flight.park(&self.key),
            (_, in_flight) => {
                drop(in_flight);
                if synced.is_ok() {
                    release_parked(&self.state, &self.key);
                }
            }
        }
        synced?;
        if std::mem::take(&mut self.format_pending) {
            // The marker is durable: the provisioning landed.
            let key = self.key.clone();
            self.with_world(|w| w.note_provisioned(&key));
        }
        Ok(())
    }
}
