//! [`JournalMatchmakerStorage`]: the matchmaker's
//! [`MatchmakerStorage`](crate::MatchmakerStorage) on `moonpool-journal`.
//!
//! Each registration is an entry at a registration number of its own, its
//! ballot in the entry's identity; the scalars (generation, freeze, decree,
//! watermark) and the format marker are the journal's metainfo, so a
//! registration and the scalars it moves with land in one commit. A raised
//! watermark clears the registrations below it; an install clears them all
//! and writes the successor's, in one commit.
//!
//! **A damaged live registration is a crash verdict**: a registry is never
//! repaired in place, a matchmaker whose durable state is unusable is
//! replaced through a matchmaker-set reconfiguration (#125), so detection is
//! the whole job. One the watermark already collected is dropped. Damage
//! the journal reports ambiguous (the last batch of a one-sync commit, where
//! a crash and rot look alike) is
//! [`Undecidable`](crate::CorruptionVerdict::Undecidable); anywhere else
//! [`Corrupted`](crate::CorruptionVerdict::Corrupted).

use std::collections::BTreeMap;

use moonpool_core::StorageProvider;
use moonpool_journal::{Batch, Journal, ReadError, State};
use paros_core::{
    Ballot, JournalIdentifier, MatchmakerConfig, MatchmakerHardState, Registration, RegistryStorage,
};
use serde::{Deserialize, Serialize};

use super::{
    JournalStoreConfig, ballot_id, commit_error, decode, encode, id_ballot, open_error, store_id,
    undecodable,
};
use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::matchmaker::MatchmakerStorage;
use crate::storage::{StorageError, StorageRecord, WriteOutcome};

/// The kind byte of a registration's identity.
const REGISTRATION: u8 = 2;

/// The matchmaker's metainfo: the format marker (#183, #207) and the
/// durable scalars.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct MatchMeta {
    /// Set once by `format`, never cleared or edited.
    formatted: Option<MatchmakerConfig>,
    scalars: MatchmakerHardState,
}

/// The matchmaker's durable store on `moonpool-journal`, over any moonpool
/// [`StorageProvider`]. Like [`JournalStorage`](super::JournalStorage) it
/// opens and loads in [`boot_scan`](MatchmakerStorage::boot_scan) and
/// creates its journal at its first sync.
pub struct JournalMatchmakerStorage<P: StorageProvider> {
    provider: P,
    dir: String,
    id: JournalIdentifier,
    store: JournalStoreConfig,
    journal: Option<Journal<P>>,
    meta: MatchMeta,
    meta_dirty: bool,
    registry: BTreeMap<Ballot, Registration>,
    /// Where each registration lives.
    positions: BTreeMap<Ballot, u64>,
    next_position: u64,
    staged: Batch,
}

impl<P: StorageProvider> std::fmt::Debug for JournalMatchmakerStorage<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalMatchmakerStorage")
            .field("dir", &self.dir)
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}

impl<P: StorageProvider> JournalMatchmakerStorage<P> {
    /// A store kept under `dir` on `provider`, for the matchmaker plane of
    /// `id` (the identity its journal is stamped with). Nothing is read
    /// until the boot scan.
    #[must_use]
    pub fn new(
        provider: P,
        dir: impl Into<String>,
        id: JournalIdentifier,
        store: JournalStoreConfig,
    ) -> Self {
        Self {
            provider,
            dir: dir.into(),
            id,
            store,
            journal: None,
            meta: MatchMeta::default(),
            meta_dirty: false,
            registry: BTreeMap::new(),
            positions: BTreeMap::new(),
            next_position: 0,
            staged: Batch::new(),
        }
    }

    /// The directory the journal lives in.
    #[must_use]
    pub fn dir(&self) -> &str {
        &self.dir
    }

    #[tracing::instrument(level = "debug", skip_all, fields(dir = %self.dir))]
    async fn load(&mut self) -> Result<(), StorageError> {
        let opened = Journal::open(
            self.provider.clone(),
            &self.dir,
            store_id(self.id),
            self.store.journal(),
        )
        .await
        .map_err(|e| open_error(&e, StorageRecord::MatchmakerScalars))?;
        *self = Self::new(self.provider.clone(), self.dir.clone(), self.id, self.store);
        let Some((journal, _recovery)) = opened else {
            return Ok(());
        };
        self.meta = decode(journal.meta()).ok_or(undecodable(StorageRecord::MatchmakerScalars))?;
        let watermark = self.meta.scalars.gc_watermark;
        let replay = journal.replay(..).await.map_err(|_| StorageError::Io {
            record: StorageRecord::Store,
            outcome: WriteOutcome::Unknown,
        })?;
        for (position, read) in replay {
            self.next_position = self.next_position.max(position + 1);
            match read {
                Ok(entry) => {
                    let (ballot, _) = id_ballot(&entry.id);
                    let registration: Registration = decode(&entry.payload)
                        .ok_or(undecodable(StorageRecord::Registration(ballot)))?;
                    self.registry.insert(ballot, registration);
                    self.positions.insert(ballot, position);
                }
                Err(ReadError::Damaged { id, .. }) => {
                    let (ballot, _) = id_ballot(&id);
                    // Collected anyway: nothing it said still matters.
                    if ballot < watermark {
                        self.staged.clear(position..position + 1);
                        continue;
                    }
                    let verdict = if matches!(journal.state(position), State::Ambiguous { .. }) {
                        CorruptionVerdict::Undecidable
                    } else {
                        CorruptionVerdict::Corrupted
                    };
                    return Err(StorageError::Corruption {
                        record: StorageRecord::Registration(ballot),
                        fault: IntegrityFault::ChecksumMismatch,
                        verdict,
                    });
                }
                Err(ReadError::Empty { .. } | ReadError::Io(_)) => {
                    return Err(StorageError::Io {
                        record: StorageRecord::Store,
                        outcome: WriteOutcome::Unknown,
                    });
                }
            }
        }
        // Boot side of the watermark pair: nothing live below it.
        assert!(
            self.registry.keys().next().is_none_or(|b| *b >= watermark),
            "no registration survives below the watermark"
        );
        self.journal = Some(journal);
        Ok(())
    }

    /// Stage `registration` under `ballot` at its own position.
    fn stage_registration(&mut self, ballot: Ballot, registration: &Registration) {
        let position = *self.positions.entry(ballot).or_insert_with(|| {
            let position = self.next_position;
            self.next_position += 1;
            position
        });
        self.staged.put(
            position,
            ballot_id(ballot, super::matchmaker::REGISTRATION),
            encode(registration),
        );
        self.registry.insert(ballot, registration.clone());
    }

    /// Drop every registration below the durable watermark.
    fn collect(&mut self) {
        let watermark = self.meta.scalars.gc_watermark;
        let kept = self.registry.split_off(&watermark);
        for ballot in std::mem::replace(&mut self.registry, kept).into_keys() {
            if let Some(position) = self.positions.remove(&ballot) {
                self.staged.clear(position..position + 1);
            }
        }
        assert!(
            self.registry.keys().next().is_none_or(|b| *b >= watermark),
            "no registration survives below the watermark"
        );
    }
}

impl<P: StorageProvider> RegistryStorage for JournalMatchmakerStorage<P> {
    fn initial_state(&self) -> MatchmakerHardState {
        self.meta.scalars.clone()
    }

    fn registration(&self, ballot: Ballot) -> Option<Registration> {
        self.registry.get(&ballot).cloned()
    }

    fn registered_ballots(&self) -> Vec<Ballot> {
        self.registry.keys().copied().collect()
    }
}

impl<P: StorageProvider> MatchmakerStorage for JournalMatchmakerStorage<P> {
    #[tracing::instrument(level = "debug", skip_all, fields(dir = %self.dir))]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.load().await
    }

    fn formatted_config(&self) -> Option<MatchmakerConfig> {
        self.meta.formatted.clone()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(dir = %self.dir))]
    async fn format(&mut self, config: &MatchmakerConfig) -> Result<(), StorageError> {
        assert!(self.meta.formatted.is_none(), "a store is formatted once");
        self.meta.formatted = Some(config.clone());
        self.meta_dirty = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    async fn register(
        &mut self,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        self.stage_registration(ballot, registration);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    async fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        let before = self.meta.scalars.gc_watermark;
        if watermark > before {
            self.meta.scalars.gc_watermark = watermark;
            self.meta_dirty = true;
            self.collect();
        }
        // The durable watermark only rises, and reaches what was asked.
        assert!(
            self.meta.scalars.gc_watermark >= before,
            "a watermark never falls"
        );
        assert!(
            self.meta.scalars.gc_watermark >= watermark,
            "a raised watermark is held"
        );
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        let watermark = scalars.gc_watermark.max(self.meta.scalars.gc_watermark);
        self.meta.scalars = scalars.clone();
        self.meta.scalars.gc_watermark = watermark;
        self.meta_dirty = true;
        self.collect();
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &BTreeMap<Ballot, Registration>,
    ) -> Result<(), StorageError> {
        // Replaced whole, in the same commit: every registration goes, the
        // successor's arrive at fresh positions.
        if self.next_position > 0 {
            self.staged.clear(0..self.next_position);
        }
        self.registry.clear();
        self.positions.clear();
        self.meta.scalars = scalars.clone();
        self.meta_dirty = true;
        for (ballot, registration) in registrations.range(scalars.gc_watermark..) {
            self.stage_registration(*ballot, registration);
        }
        assert!(
            self.meta.scalars.generation == scalars.generation,
            "an installed registry holds the successor generation"
        );
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(dir = %self.dir))]
    async fn sync(&mut self) -> Result<(), StorageError> {
        let mut staged = std::mem::take(&mut self.staged);
        if !self.meta_dirty && staged.is_empty() {
            return Ok(());
        }
        if self.meta_dirty {
            staged.set_meta(encode(&self.meta));
        }
        if self.journal.is_none() {
            let created = Journal::create(
                self.provider.clone(),
                &self.dir,
                store_id(self.id),
                self.store.journal(),
                &encode(&self.meta),
            )
            .await
            .map_err(|e| open_error(&e, StorageRecord::MatchmakerScalars))?;
            self.journal = Some(created);
        }
        let journal = self.journal.as_mut().expect("created above");
        journal.commit(staged).await.map_err(|e| commit_error(&e))?;
        self.meta_dirty = false;
        Ok(())
    }
}
