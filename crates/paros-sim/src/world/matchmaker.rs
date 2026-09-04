//! A matchmaker's handle onto the [`StorageWorld`]: the [`MatchmakerStorage`]
//! implementation the matchmaker driver runs on, with the core's
//! [`RegistryStorage`] read port served from a boot-time view.
//!
//! The registry rides the world's durable-record contract exactly like a
//! node's disk: the scalars and the per-ballot registration records are stored
//! separately (never a blob — the shape the CTRL per-record detection and
//! repair need, see `paros::MatchmakerStorage`), writes stage locally **in
//! order** and reach the durable world only on a `sync`, so a crash before
//! the fsync loses the whole un-synced batch — a faithful clean crash — and a
//! restart reads back, record by record, exactly what the last fsync left.
//! There is deliberately **no matchmaker-specific fault story** (#119): torn
//! writes, checksums and rot are generic storage concerns already modelled on
//! the node's records, the registry's crash seams live in the driver, and a
//! matchmaker whose state is lost for good is *replaced* through a
//! matchmaker-set reconfiguration (#125), never repaired in place. What the
//! registry does draw is the **whole-batch fsync failure** of the node's own
//! write path ([`StorageFaults::fsync_fail`], the same seed-drawn rate): the
//! matchmaker driver's fail-stop arm and the replacement path that follows
//! it were otherwise reachable only through a seam crash, which is a
//! *clean* loss. Its floor is the world's budget — at most `quorum - 1` of
//! the bootstrap set may ever fail a sync — so a matchmaking quorum always
//! survives.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError, Weak};

use moonpool_sim::{TimeProvider, assert_always, assert_reachable, buggify_with_prob};
use paros::{
    Ballot, MatchmakerHardState, MatchmakerStorage, Registration, RegistryStorage, StorageError,
    StorageRecord, WriteOutcome,
};

use super::StorageWorld;
use super::storage::StorageFaults;

/// One matchmaker's durable records, owned by the world (keyed by IP): the
/// scalars and the per-ballot registration records, stored separately.
#[derive(Default)]
pub(super) struct MatchmakerDisk {
    pub(super) hard_state: MatchmakerHardState,
    pub(super) registry: BTreeMap<Ballot, Registration>,
}

impl MatchmakerDisk {
    /// Apply one flushed write, in batch order.
    fn apply(&mut self, op: Staged) {
        match op {
            Staged::Register(ballot, registration) => {
                // Write-once, seen from the disk: a re-write of a registered
                // ballot carries the same bytes (the core never re-registers,
                // and a boot replays nothing).
                if let Some(previous) = self.registry.insert(ballot, registration.clone()) {
                    assert_always!(
                        previous == registration,
                        "matchmaker: a durable registration is never overwritten with different bytes",
                        { "round" => ballot.round, "bnode" => ballot.node.0 }
                    );
                }
            }
            Staged::Watermark(watermark) => self.raise(watermark),
            Staged::Scalars(scalars) => {
                // The durable watermark never lowers: a scalar write carries
                // the core's copy, which can lag a floor already flushed.
                let durable = self.hard_state.gc_watermark;
                self.hard_state = scalars;
                self.hard_state.gc_watermark = self.hard_state.gc_watermark.max(durable);
                let floor = self.hard_state.gc_watermark;
                self.registry = self.registry.split_off(&floor);
            }
            Staged::Install(scalars, registrations) => {
                self.registry = registrations
                    .into_iter()
                    .filter(|(b, _)| *b >= scalars.gc_watermark)
                    .collect();
                self.hard_state = scalars;
            }
        }
    }

    fn raise(&mut self, watermark: Ballot) {
        if watermark > self.hard_state.gc_watermark {
            self.hard_state.gc_watermark = watermark;
            self.registry = self.registry.split_off(&watermark);
        }
    }
}

/// One staged write, replayed in order at the fsync.
enum Staged {
    Register(Ballot, Registration),
    Watermark(Ballot),
    Scalars(MatchmakerHardState),
    Install(MatchmakerHardState, BTreeMap<Ballot, Registration>),
}

/// A [`MatchmakerStorage`] onto one matchmaker's slice of the shared world.
pub(crate) struct DurableMatchmakerStorage<T> {
    /// Read view: the durable scalars and records as of this boot (the core
    /// reads the port once, at construction).
    boot_hard_state: MatchmakerHardState,
    boot_registry: BTreeMap<Ballot, Registration>,
    world: Weak<Mutex<StorageWorld>>,
    /// This matchmaker's IP — its key into the world.
    key: String,
    /// Writes staged since the last flush, in order (lost if the incarnation
    /// is dropped before a sync).
    staged: Vec<Staged>,
    /// The seed's write-path fault profile and its chaos window, shared with
    /// the node disks.
    faults: StorageFaults<T>,
    /// The deployment's bootstrap matchmaker count — the world's budget for
    /// how many registries may be failing at once.
    bootstrap: usize,
}

impl<T: TimeProvider> DurableMatchmakerStorage<T> {
    /// Build storage for the matchmaker at `key`, seeding the read view from
    /// any durable records a prior boot of the same IP left in the world.
    #[tracing::instrument(level = "debug", skip_all, fields(key = %key))]
    pub(crate) fn restore(
        world: Weak<Mutex<StorageWorld>>,
        key: String,
        faults: StorageFaults<T>,
        bootstrap: usize,
    ) -> Self {
        let (boot_hard_state, boot_registry) = world
            .upgrade()
            .and_then(|strong| {
                let guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
                guard
                    .matchmakers
                    .get(&key)
                    .map(|disk| (disk.hard_state.clone(), disk.registry.clone()))
            })
            .unwrap_or_default();
        Self {
            boot_hard_state,
            boot_registry,
            world,
            key,
            staged: Vec::new(),
            faults,
            bootstrap,
        }
    }

    /// This matchmaker's key into the world (its IP).
    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    fn with_world<R>(&self, f: impl FnOnce(&mut StorageWorld) -> R) -> Result<R, StorageError> {
        let strong = self.world.upgrade().ok_or(StorageError::Io {
            record: StorageRecord::Batch,
            outcome: WriteOutcome::Lost,
        })?;
        let mut guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(f(&mut guard))
    }
}

impl<T: TimeProvider> RegistryStorage for DurableMatchmakerStorage<T> {
    fn initial_state(&self) -> MatchmakerHardState {
        self.boot_hard_state.clone()
    }

    fn registration(&self, ballot: Ballot) -> Option<Registration> {
        self.boot_registry.get(&ballot).cloned()
    }

    fn registered_ballots(&self) -> Vec<Ballot> {
        self.boot_registry.keys().copied().collect()
    }
}

impl<T: TimeProvider> MatchmakerStorage for DurableMatchmakerStorage<T> {
    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    async fn register(
        &mut self,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        self.staged
            .push(Staged::Register(ballot, registration.clone()));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    async fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        self.staged.push(Staged::Watermark(watermark));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        self.staged.push(Staged::Scalars(scalars.clone()));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &BTreeMap<Ballot, Registration>,
    ) -> Result<(), StorageError> {
        self.staged
            .push(Staged::Install(scalars.clone(), registrations.clone()));
        Ok(())
    }

    /// The fsync: the whole stage reaches the durable world in the order the
    /// core wrote it, so a flushed floor is applied over the records it
    /// prunes exactly as the core applied them.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn sync(&mut self) -> Result<(), StorageError> {
        let staged = std::mem::take(&mut self.staged);
        if staged.is_empty() {
            return Ok(());
        }
        // The fsync fails: the stage dies with the incarnation (a clean
        // whole-batch loss — the registry has no torn-write story of its
        // own) and the driver fail-stops on the error. Budgeted by the
        // world, so a quorum of the bootstrap set is never sick at once.
        if self.faults.active() && buggify_with_prob!(self.faults.fsync_fail()) {
            let key = self.key.clone();
            let bootstrap = self.bootstrap;
            let permitted =
                self.with_world(|w| w.permit_matchmaker_sync_failure(&key, bootstrap))?;
            if permitted {
                // BUGGIFY pairing: the registry's fsync genuinely fails.
                assert_reachable!("matchmaker: a registry fsync fails");
                return Err(StorageError::FsyncFailed {
                    record: StorageRecord::Batch,
                    outcome: WriteOutcome::Lost,
                });
            }
        }
        let key = self.key.clone();
        self.with_world(|w| {
            let disk = w.matchmakers.entry(key).or_default();
            for op in staged {
                disk.apply(op);
            }
        })
    }
}
