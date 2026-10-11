//! The matchmaker's durable seam: the [`MatchmakerStorage`] write extension the
//! driver persists the registry through, over the core's read-only
//! [`RegistryStorage`] recovery port, and the default in-memory
//! [`MemMatchmakerStorage`] implementing both.
//!
//! The split is the node's, mirrored on purpose: [`RegistryStorage`] is to the
//! matchmaker what [`paros_core::Storage`] is to the node — the core reads its
//! durable state back through it once, at construction, record by record — and
//! [`MatchmakerStorage`] is the [`LogStorage`](crate::LogStorage) twin: the
//! driver owns every write, applies each
//! [`MatchmakerWriteOp`](paros_core::MatchmakerWriteOp) through the matching
//! method here, then [`sync`](MatchmakerStorage::sync)s the batch **before** its
//! reply leaves (persist-before-reply).
//!
//! # Why the registry gets a real storage interface
//!
//! Because it is durable state that will rot, and the recovery story built for
//! the accepted log (CTRL, Stages 7–8) is only available to state that crosses a
//! seam like this one. Every registration is one checksummed record whose
//! identity — the ballot — sits inside the checksummed region, so
//! [`boot_scan`](MatchmakerStorage::boot_scan) can verify and classify each
//! record before any byte reaches the core: a torn tail is discardable (never
//! acknowledged: the reply only leaves after the fsync), a record whose bytes
//! are lost but whose ballot survived is *recoverable* (the other matchmakers
//! hold the same bytes), and only a record whose identity is also lost is a
//! crash. That tri-state — report the ballot as faulty, never as "no
//! configuration here" — is the registry's version of CTRL's central rule.
//! Unlike the log it is not repaired in place: there is no
//! `faulty_registrations()` on [`RegistryStorage`] and no per-record repair
//! read. A matchmaker whose durable state is unusable is **replaced** through a
//! matchmaker-set reconfiguration, reconstructed from the surviving quorum
//! (see [`RegistryStorage`]). A registry booted from one blob could offer none of
//! this: one checksum, one verdict, and a matchmaker that either boots blind or
//! not at all. Every write returns [`Result`] for the same reason: the faults
//! are injectable from the start, through the existing durable-record contract
//! (`docs/analysis/storage/clstore-record-contract.md`) — there is no
//! matchmaker-specific disk-fault story, only the generic one applied to one
//! more record family.

use paros_core::{
    AcceptorConfig, Ballot, JournalId, MatchmakerConfig, MatchmakerGeneration, MatchmakerHardState,
    MatchmakerId, MatchmakerPhase, MatchmakerSet, MatchmakerWriteOp, MemRegistry, NodeId,
    QuorumSystem, Registration, RegistryStorage,
};
use std::collections::BTreeMap;
use std::future::Future;

use crate::storage::StorageError;

/// The write side of matchmaker storage: **semantic per-record ops**, the
/// [`LogStorage`](crate::LogStorage) twin over the [`RegistryStorage`] port.
///
/// The seam is **async** exactly as the node's is (see *Async seam* on
/// [`LogStorage`](crate::LogStorage)): every method here may touch the
/// device, so every one returns a `Send` future the driver awaits in
/// persist-before-reply order; an implementation writes plain `async fn`s.
/// The [`RegistryStorage`] read port stays synchronous, answered from what
/// [`boot_scan`](MatchmakerStorage::boot_scan) loaded.
pub trait MatchmakerStorage: RegistryStorage {
    /// Boot-time integrity scan, run once per incarnation **before** the core
    /// reads the store (the [`LogStorage::boot_scan`](crate::LogStorage::boot_scan)
    /// twin): verify every registration record and the watermark scalar,
    /// classify every mismatch, discard only a crash-truncatable tail (a
    /// registration is acknowledged only after its fsync, so an un-synced
    /// tail was never promised to anyone), and surface the first crash
    /// verdict. **Never truncate on a corruption verdict.** The default
    /// reports a clean store, for storage that cannot rot.
    ///
    /// # Errors
    /// Returns the first classified [`StorageError`] whose verdict requires a
    /// crash.
    fn boot_scan(&mut self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async { Ok(()) }
    }

    /// The configuration this registry was **formatted** with (#183,
    /// #207), or `None` on a registry that carries no format marker — the
    /// matchmaker twin of
    /// [`LogStorage::formatted_config`](crate::LogStorage::formatted_config).
    /// The marker is the durable proof that the matchmaker this registry
    /// belongs to has been provisioned — written once by
    /// [`format`](MatchmakerStorage::format) on its first boot, before any
    /// registry state, and never removed. The driver judges the operator's
    /// [`BootKind`](crate::BootKind) claim against it: an existing
    /// matchmaker whose store has no marker has lost its disk — every
    /// registration, the GC watermark and the generation scalars with it —
    /// and is refused rather than rejoined, because an empty registry
    /// answering a matchmaking quorum would hand a candidate a history that
    /// omits a configuration it once registered; and one formatted under
    /// another [`MatchmakerConfig`] than the operator hands it now (another
    /// identity, another bootstrap set) is refused too. Synchronous,
    /// answered from what the boot scan loaded.
    fn formatted_config(&self) -> Option<MatchmakerConfig>;

    /// Whether this store carries the format marker (#183):
    /// [`formatted_config`](MatchmakerStorage::formatted_config) is `Some`.
    fn is_formatted(&self) -> bool {
        self.formatted_config().is_some()
    }

    /// Write the format marker (#183) and the configuration the registry is
    /// provisioned under (#207). Staged like every other write and durable
    /// at the next [`sync`](MatchmakerStorage::sync); the driver syncs it
    /// alone, on a first boot, before the core reads the store, so the
    /// marker is on disk no later than the first registration. Nothing but
    /// this method writes it, and nothing removes or edits it.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn format(
        &mut self,
        config: &MatchmakerConfig,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Persist `registration` under `ballot` in `journal`'s registry as one
    /// record (#190: one store holds every journal of its tenant's set).
    /// Append-only: the core only ever registers strictly above the highest
    /// ballot the journal holds, so this is never an overwrite.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn register(
        &mut self,
        journal: JournalId,
        ballot: Ballot,
        registration: &Registration,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Persist `journal`'s raised GC watermark and drop every one of its
    /// registration records below it. Monotone: a store never lowers a
    /// watermark, and a raise never touches another journal.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn set_gc_watermark(
        &mut self,
        journal: JournalId,
        watermark: Ballot,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Persist the durable scalars whole (#125: the generation state, the
    /// freeze, the successor link, the decree record, the pending
    /// bootstraps). No journal's watermark inside sits below its durable one.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn set_scalars(
        &mut self,
        scalars: &MatchmakerHardState,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Replace the registry whole — a successor generation's activation:
    /// every record dropped, each journal's `registrations` written, and
    /// `scalars` (whose watermarks are the reconstructed ones) persisted in
    /// the same batch.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &Registrations,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Flush this batch's writes to stable storage. Every matchmaker write is
    /// safety-critical, so this is always an fsync: the batch must be durable
    /// on return, because the reply that follows claims it is.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the flush fails.
    fn sync(&mut self) -> impl Future<Output = Result<(), StorageError>> + Send;
}

/// Every journal's registration records, per journal in ballot order.
pub type Registrations = BTreeMap<JournalId, BTreeMap<Ballot, Registration>>;

/// The library's default in-memory matchmaker storage: the core's reference
/// [`MemRegistry`] (the scalars and the per-ballot records of every journal,
/// stored separately, never a single blob) behind the fallible write
/// extension, plus the format marker.
#[derive(Clone, Debug, Default)]
pub struct MemMatchmakerStorage {
    registry: MemRegistry,
    /// The format marker (#183) and the configuration it was written under
    /// (#207): set by [`MatchmakerStorage::format`], never cleared.
    formatted: Option<MatchmakerConfig>,
}

impl MemMatchmakerStorage {
    /// A fresh, empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl RegistryStorage for MemMatchmakerStorage {
    fn initial_state(&self) -> MatchmakerHardState {
        self.registry.initial_state()
    }

    fn registration(&self, journal: JournalId, ballot: Ballot) -> Option<Registration> {
        self.registry.registration(journal, ballot)
    }

    fn registered(&self) -> Vec<(JournalId, Ballot)> {
        self.registry.registered()
    }
}

impl MatchmakerStorage for MemMatchmakerStorage {
    fn formatted_config(&self) -> Option<MatchmakerConfig> {
        self.formatted.clone()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn format(&mut self, config: &MatchmakerConfig) -> Result<(), StorageError> {
        self.formatted = Some(config.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    async fn register(
        &mut self,
        journal: JournalId,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        self.registry.apply(&MatchmakerWriteOp::Register {
            journal,
            ballot,
            registration: registration.clone(),
        });
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    async fn set_gc_watermark(
        &mut self,
        journal: JournalId,
        watermark: Ballot,
    ) -> Result<(), StorageError> {
        self.registry
            .apply(&MatchmakerWriteOp::SetGcWatermark { journal, watermark });
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        self.registry
            .apply(&MatchmakerWriteOp::SetScalars(scalars.clone()));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &Registrations,
    ) -> Result<(), StorageError> {
        self.registry.apply(&MatchmakerWriteOp::InstallRegistry {
            scalars: scalars.clone(),
            registrations: registrations.clone(),
        });
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn sync(&mut self) -> Result<(), StorageError> {
        // In-memory: writes are already visible; nothing to flush.
        Ok(())
    }
}

/// The suite's journal.
const SUITE_J: JournalId = JournalId(1);

/// A sibling journal of the same tenant's set (#190).
const SUITE_SIBLING: JournalId = JournalId(2);

/// The suite's ballot at `round` (one proposer, node 1).
fn suite_ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(1),
    }
}

/// The suite's configuration of `n` acceptors.
fn suite_config(n: u64) -> AcceptorConfig {
    AcceptorConfig::new(
        (0..n).map(NodeId).collect::<Vec<_>>(),
        QuorumSystem::Majority,
    )
}

/// The suite's belief registration of `n` acceptors.
fn suite_belief(n: u64) -> Registration {
    Registration::belief(suite_config(n))
}

/// The behavioral **contract suite** every [`MatchmakerStorage`] implementation
/// must pass, run against [`MemMatchmakerStorage`] here and against the
/// journal store ([`crate::journal`]) in its tests and in `paros-sim`, so no
/// implementation drifts from the trait contract. `fresh` returns (a future of) an empty store;
/// `reopen` simulates a clean reboot of the same store (asynchronously: a
/// disk-backed store opens and scans on the way up) — every read-back goes through it and
/// through the [`RegistryStorage`] port, because that is how the core reads
/// durable state: once, at construction, record by record.
///
/// # Panics
///
/// Panics on any contract violation.
#[doc(hidden)]
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn matchmaker_storage_contract_suite<S, Fresh, Reopened>(
    mut fresh: impl FnMut() -> Fresh,
    mut reopen: impl FnMut(S) -> Reopened,
) where
    S: MatchmakerStorage,
    Fresh: Future<Output = S>,
    Reopened: Future<Output = S>,
{
    use paros_core::Matchmaker;
    // What every reopen must satisfy: the durable records and the durable
    // scalars are mutually consistent (no record below the watermark, every
    // walked ballot readable), and — the recovery contract itself — a
    // matchmaker booted from the port holds exactly what the port serves.
    let consistent = |s: &S| {
        let state = s.initial_state();
        let registered = s.registered();
        assert!(
            registered.windows(2).all(|w| w[0] < w[1]),
            "registered ballots are strictly ascending"
        );
        assert!(
            registered
                .iter()
                .all(|(journal, b)| *b >= state.gc_watermark(*journal)),
            "no registration survives below the durable watermark"
        );
        let booted = Matchmaker::new(
            &MatchmakerConfig {
                id: MatchmakerId(0),
                bootstrap: vec![MatchmakerId(0)],
            },
            s,
        );
        assert_eq!(
            *booted.hard_state(),
            state,
            "a boot adopts the durable scalars"
        );
        let walked: Vec<(JournalId, Ballot)> = booted
            .journals()
            .flat_map(|journal| {
                booted
                    .registry(journal)
                    .keys()
                    .map(move |ballot| (journal, *ballot))
            })
            .collect();
        assert_eq!(
            walked, registered,
            "a boot walks back every durable registration"
        );
        for (journal, ballot) in &registered {
            assert_eq!(
                booted.registry(*journal).get(ballot).cloned(),
                s.registration(*journal, *ballot),
                "a boot reads each registration back byte for byte"
            );
        }
    };
    // One journal's registered ballots.
    let ballots = |s: &S, journal: JournalId| -> Vec<Ballot> {
        s.registered()
            .into_iter()
            .filter(|(j, _)| *j == journal)
            .map(|(_, b)| b)
            .collect()
    };

    // The format marker (#183): absent on a fresh store, present once
    // `format` is flushed and reopened, and written by nothing else — a
    // store that took registry writes without ever being formatted stays
    // unformatted (the wiped-disk shape the driver refuses).
    let s = fresh().await;
    assert!(!s.is_formatted(), "a fresh store carries no format marker");
    let mut s = fresh().await;
    s.register(SUITE_J, suite_ballot(1), &suite_belief(3))
        .await
        .expect("register before format");
    s.sync().await.expect("sync register");
    let s = reopen(s).await;
    assert!(
        !s.is_formatted(),
        "registry writes never format a store on their own"
    );
    // #207: the marker records the configuration it was written under.
    let provisioned = MatchmakerConfig {
        id: MatchmakerId(1),
        bootstrap: vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    };
    let mut s = fresh().await;
    assert_eq!(
        s.formatted_config(),
        None,
        "a fresh store records no configuration"
    );
    s.format(&provisioned).await.expect("format");
    s.sync().await.expect("sync format");
    let mut s = reopen(s).await;
    assert!(s.is_formatted(), "the format marker survives a reopen");
    assert_eq!(
        s.formatted_config().as_ref(),
        Some(&provisioned),
        "the format marker records the configuration it was written under"
    );
    s.register(SUITE_J, suite_ballot(2), &suite_belief(3))
        .await
        .expect("register after format");
    s.set_gc_watermark(SUITE_J, suite_ballot(2))
        .await
        .expect("raise after format");
    s.sync().await.expect("sync after format");
    let s = reopen(s).await;
    assert!(s.is_formatted(), "the format marker is never removed");
    assert_eq!(
        s.formatted_config().as_ref(),
        Some(&provisioned),
        "later writes never edit the recorded configuration"
    );
    consistent(&s);

    // A fresh store is empty, and registrations round-trip through a sync as
    // individually readable records.
    let s = fresh().await;
    assert_eq!(
        s.initial_state(),
        MatchmakerHardState::default(),
        "a fresh store holds the zero watermark"
    );
    assert!(
        s.registered().is_empty(),
        "a fresh store holds no registration"
    );
    consistent(&s);
    let mut s = reopen(s).await;
    s.register(SUITE_J, suite_ballot(1), &suite_belief(3))
        .await
        .expect("register 1");
    s.register(SUITE_J, suite_ballot(2), &suite_belief(4))
        .await
        .expect("register 2");
    s.sync().await.expect("sync");
    let mut s = reopen(s).await;
    consistent(&s);
    assert_eq!(
        ballots(&s, SUITE_J),
        vec![suite_ballot(1), suite_ballot(2)],
        "registered ballots read back in ballot order"
    );
    assert_eq!(
        s.registration(SUITE_J, suite_ballot(1)),
        Some(suite_belief(3))
    );
    assert_eq!(
        s.registration(SUITE_J, suite_ballot(2)),
        Some(suite_belief(4))
    );
    assert_eq!(
        s.registration(SUITE_J, suite_ballot(3)),
        None,
        "an unregistered ballot has no record"
    );
    assert_eq!(s.initial_state().gc_watermark(SUITE_J), Ballot::zero());

    // A raised watermark is durable and drops the collected records.
    s.register(SUITE_J, suite_ballot(3), &suite_belief(5))
        .await
        .expect("register 3");
    s.set_gc_watermark(SUITE_J, suite_ballot(2))
        .await
        .expect("raise");
    s.sync().await.expect("sync raise");
    let mut s = reopen(s).await;
    consistent(&s);
    assert_eq!(
        s.initial_state().gc_watermark(SUITE_J),
        suite_ballot(2),
        "the watermark round-trips"
    );
    assert_eq!(
        ballots(&s, SUITE_J),
        vec![suite_ballot(2), suite_ballot(3)],
        "registrations below the watermark are dropped"
    );
    assert_eq!(
        s.registration(SUITE_J, suite_ballot(1)),
        None,
        "a collected record is unreadable"
    );

    // The watermark never lowers.
    s.set_gc_watermark(SUITE_J, suite_ballot(1))
        .await
        .expect("re-raise lower");
    s.sync().await.expect("sync no-op");
    let mut s = reopen(s).await;
    consistent(&s);
    assert_eq!(
        s.initial_state().gc_watermark(SUITE_J),
        suite_ballot(2),
        "the watermark is monotone"
    );

    // The generation scalars persist whole (#125): a freeze and a successor
    // link read back, and the watermark inside never lowers the durable one.
    let mut scalars = s.initial_state();
    scalars.phase = MatchmakerPhase::Stopped;
    scalars.members = vec![MatchmakerId(0)];
    scalars.successor = Some(MatchmakerSet::new(
        MatchmakerGeneration(1),
        vec![MatchmakerId(0), MatchmakerId(1)],
    ));
    scalars.journal_mut(SUITE_J).gc_watermark = suite_ballot(1);
    s.set_scalars(&scalars).await.expect("scalars");
    s.sync().await.expect("sync scalars");
    let mut s = reopen(s).await;
    consistent(&s);
    let read_back = s.initial_state();
    assert_eq!(
        read_back.phase,
        MatchmakerPhase::Stopped,
        "the freeze is durable"
    );
    assert_eq!(
        read_back.successor, scalars.successor,
        "the successor link is durable"
    );
    assert_eq!(
        read_back.gc_watermark(SUITE_J),
        suite_ballot(2),
        "scalars never lower the durable watermark"
    );

    // An activation replaces the registry whole at the reconstructed
    // watermark, and drops what sits below it.
    let mut activated = read_back;
    activated.generation = MatchmakerGeneration(1);
    activated.members = vec![MatchmakerId(0), MatchmakerId(1)];
    activated.phase = MatchmakerPhase::Active;
    activated.successor = None;
    activated.journal_mut(SUITE_J).gc_watermark = suite_ballot(4);
    let mut reconstructed = Registrations::new();
    let ledger = reconstructed.entry(SUITE_J).or_default();
    ledger.insert(suite_ballot(3), suite_belief(3));
    ledger.insert(suite_ballot(4), suite_belief(6));
    ledger.insert(suite_ballot(7), suite_belief(7));
    s.install_registry(&activated, &reconstructed)
        .await
        .expect("install");
    s.sync().await.expect("sync install");
    let s = reopen(s).await;
    consistent(&s);
    assert_eq!(
        s.initial_state(),
        activated,
        "the activation's scalars read back"
    );
    assert_eq!(
        ballots(&s, SUITE_J),
        vec![suite_ballot(4), suite_ballot(7)],
        "the reconstructed registry replaced the old one, above its watermark"
    );
    assert_eq!(
        s.registration(SUITE_J, suite_ballot(2)),
        None,
        "the replaced generation's records are gone"
    );

    // #190: one store holds every journal of its tenant's set. The same
    // ballot lives in two journals at once, a raise collects in its own
    // journal only, and an activation installs every journal's registry.
    let mut s = fresh().await;
    s.register(SUITE_J, suite_ballot(1), &suite_belief(3))
        .await
        .expect("register in the journal");
    s.register(SUITE_SIBLING, suite_ballot(1), &suite_belief(4))
        .await
        .expect("register in the sibling");
    s.register(SUITE_SIBLING, suite_ballot(2), &suite_belief(5))
        .await
        .expect("register again in the sibling");
    s.set_gc_watermark(SUITE_J, suite_ballot(2))
        .await
        .expect("raise in the journal");
    s.sync().await.expect("sync siblings");
    let mut s = reopen(s).await;
    consistent(&s);
    assert!(
        ballots(&s, SUITE_J).is_empty(),
        "a raise collects its own journal"
    );
    assert_eq!(
        ballots(&s, SUITE_SIBLING),
        vec![suite_ballot(1), suite_ballot(2)],
        "a raise never collects a sibling journal"
    );
    assert_eq!(
        s.registration(SUITE_SIBLING, suite_ballot(1)),
        Some(suite_belief(4)),
        "a ballot is read back in its own journal"
    );
    assert_eq!(
        s.initial_state().gc_watermark(SUITE_SIBLING),
        Ballot::zero(),
        "a raise never moves a sibling's watermark"
    );
    let mut activated = s.initial_state();
    activated.generation = MatchmakerGeneration(1);
    activated.members = vec![MatchmakerId(0), MatchmakerId(1)];
    activated.phase = MatchmakerPhase::Active;
    activated.journal_mut(SUITE_SIBLING).gc_watermark = suite_ballot(2);
    let mut reconstructed = Registrations::new();
    reconstructed
        .entry(SUITE_J)
        .or_default()
        .insert(suite_ballot(3), suite_belief(3));
    let sibling = reconstructed.entry(SUITE_SIBLING).or_default();
    sibling.insert(suite_ballot(1), suite_belief(4));
    sibling.insert(suite_ballot(3), suite_belief(6));
    s.install_registry(&activated, &reconstructed)
        .await
        .expect("install siblings");
    s.sync().await.expect("sync install siblings");
    let s = reopen(s).await;
    consistent(&s);
    assert_eq!(
        ballots(&s, SUITE_J),
        vec![suite_ballot(3)],
        "an activation installs each journal's registry"
    );
    assert_eq!(
        ballots(&s, SUITE_SIBLING),
        vec![suite_ballot(3)],
        "an activation drops what sits below each journal's watermark"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_matchmaker_storage_passes_the_contract_suite() {
        // In-memory writes are immediately visible: a reboot is the same
        // handle.
        futures::executor::block_on(matchmaker_storage_contract_suite(
            || std::future::ready(MemMatchmakerStorage::new()),
            std::future::ready,
        ));
    }
}
