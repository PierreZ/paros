//! A matchmaker's handle onto the [`StorageWorld`]: the [`MatchmakerStorage`]
//! implementation the matchmaker driver runs on, with the core's
//! [`RegistryStorage`] read port served from a boot-time view.
//!
//! The registry rides the world's durable-record contract exactly like a
//! node's disk: the watermark scalar and the per-ballot registration records
//! are stored separately (never a blob — the shape the CTRL per-record
//! detection and repair need, see `paros::MatchmakerStorage`), writes stage
//! locally and reach the durable world only on a `sync`, so a crash before the
//! fsync loses the whole un-synced batch — a faithful clean crash — and a
//! restart reads back, record by record, exactly what the last fsync left.
//! There is deliberately **no matchmaker-specific fault story** (#119): torn
//! writes, checksums and rot are generic storage concerns already modelled on
//! the node's records, and the registry's crash seams live in the driver.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError, Weak};

use moonpool_sim::assert_always;
use paros::{
    AcceptorConfig, Ballot, MatchmakerHardState, MatchmakerStorage, RegistryStorage, StorageError,
    StorageRecord, WriteOutcome,
};

use super::StorageWorld;

/// One matchmaker's durable records, owned by the world (keyed by IP): the
/// scalars and the per-ballot registration records, stored separately.
#[derive(Default)]
pub(super) struct MatchmakerDisk {
    pub(super) hard_state: MatchmakerHardState,
    pub(super) registry: BTreeMap<Ballot, AcceptorConfig>,
}

/// A [`MatchmakerStorage`] onto one matchmaker's slice of the shared world.
pub(crate) struct DurableMatchmakerStorage {
    /// Read view: the durable scalars and records as of this boot (the core
    /// reads the port once, at construction).
    boot_hard_state: MatchmakerHardState,
    boot_registry: BTreeMap<Ballot, AcceptorConfig>,
    world: Weak<Mutex<StorageWorld>>,
    /// This matchmaker's IP — its key into the world.
    key: String,
    /// Writes staged since the last flush (lost if the incarnation is dropped
    /// before a sync).
    staged_registrations: BTreeMap<Ballot, AcceptorConfig>,
    staged_watermark: Option<Ballot>,
}

impl DurableMatchmakerStorage {
    /// Build storage for the matchmaker at `key`, seeding the read view from
    /// any durable records a prior boot of the same IP left in the world.
    #[tracing::instrument(level = "debug", skip_all, fields(key = %key))]
    pub(crate) fn restore(world: Weak<Mutex<StorageWorld>>, key: String) -> Self {
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
            staged_registrations: BTreeMap::new(),
            staged_watermark: None,
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

impl RegistryStorage for DurableMatchmakerStorage {
    fn initial_state(&self) -> MatchmakerHardState {
        self.boot_hard_state.clone()
    }

    fn registration(&self, ballot: Ballot) -> Option<AcceptorConfig> {
        self.boot_registry.get(&ballot).cloned()
    }

    fn registered_ballots(&self) -> Vec<Ballot> {
        self.boot_registry.keys().copied().collect()
    }
}

impl MatchmakerStorage for DurableMatchmakerStorage {
    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    fn register(&mut self, ballot: Ballot, config: &AcceptorConfig) -> Result<(), StorageError> {
        self.staged_registrations.insert(ballot, config.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        self.staged_watermark = Some(
            self.staged_watermark
                .map_or(watermark, |w| w.max(watermark)),
        );
        Ok(())
    }

    /// The fsync: the whole stage reaches the durable world, registrations
    /// first and the watermark last (so a flushed floor is applied over the
    /// records it prunes, exactly as the core applied them).
    #[tracing::instrument(level = "trace", skip_all)]
    fn sync(&mut self) -> Result<(), StorageError> {
        let registrations = std::mem::take(&mut self.staged_registrations);
        let watermark = self.staged_watermark.take();
        if registrations.is_empty() && watermark.is_none() {
            return Ok(());
        }
        let key = self.key.clone();
        self.with_world(|w| {
            let disk = w.matchmakers.entry(key).or_default();
            for (ballot, config) in registrations {
                // Write-once, seen from the disk: a re-write of a registered
                // ballot carries the same bytes (the core never re-registers,
                // and a boot replays nothing).
                if let Some(previous) = disk.registry.insert(ballot, config.clone()) {
                    assert_always!(
                        previous == config,
                        "matchmaker: a durable registration is never overwritten with different bytes",
                        { "round" => ballot.round, "bnode" => ballot.node.0 }
                    );
                }
            }
            if let Some(watermark) = watermark
                && watermark > disk.hard_state.gc_watermark
            {
                disk.hard_state.gc_watermark = watermark;
                disk.registry = disk.registry.split_off(&watermark);
            }
        })
    }
}
