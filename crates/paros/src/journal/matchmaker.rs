//! [`JournalMatchmakerStorage`]: the matchmaker's
//! [`MatchmakerStorage`](crate::MatchmakerStorage) on `moonpool-journal`.
//!
//! Each registration is an entry at a registration number of its own, its
//! ballot and its generation in the entry's identity, its journal in the
//! payload (#190: one store holds every journal of its tenant's set); the
//! scalars (generation, freeze, decree, each journal's watermark) and the
//! format marker are the journal's metainfo. A raised watermark clears its
//! journal's registrations below it; an install clears them all and writes
//! the successor's.
//!
//! **A sync is up to two commits, in an order every crash point survives**
//! (#176): the new registrations with the metainfo, then the clears. The
//! journal writes a batch's metainfo only once the batch is durable
//! (moonpool#309), so a durable metainfo vouches for its registrations; the
//! reverse does not hold, so a boot keeps only the registrations the durable
//! metainfo vouches for: at or above its watermark and of its generation. A
//! crash after the puts leaves
//! registrations the old metainfo does not count yet (a register's, kept:
//! the driver never acknowledged it, and it stands like any other; an
//! install's, of the successor generation, dropped); a crash after the
//! metainfo leaves the clears undone, and the boot drops what they would
//! have cleared. The one scalar that must cover a registration, the
//! effective configuration (at or above every reconfiguration the registry
//! keeps), is rebuilt by the boot from the registrations it keeps, as the
//! cut metainfo commit would have raised it.
//!
//! **A damaged live registration is a crash verdict**: a registry is never
//! repaired in place, a matchmaker whose durable state is unusable is
//! replaced through a matchmaker-set reconfiguration (#125), so detection is
//! the whole job. One the watermark already collected is dropped: a damaged
//! entry's journal is unknown, so it is dropped only below every journal the
//! metainfo names (the store names a journal there with its first
//! registration, so an acknowledged registration's journal is always named;
//! one whose journal is not named was never acknowledged). Damage
//! the journal reports ambiguous (the last batch of a one-sync commit, where
//! a crash and rot look alike) is
//! [`Undecidable`](crate::CorruptionVerdict::Undecidable); anywhere else
//! [`Corrupted`](crate::CorruptionVerdict::Corrupted).

use std::collections::{BTreeMap, BTreeSet};

use moonpool_core::StorageProvider;
use std::ops::Range;

use moonpool_buggify::hint::Strike;
use moonpool_journal::{Batch, ID_SIZE, Id, Journal, ReadError, State};
use paros_core::{
    Ballot, JournalId, JournalIdentifier, MatchmakerConfig, MatchmakerGeneration,
    MatchmakerHardState, Registration, RegistryStorage,
};
use serde::{Deserialize, Serialize};

use super::{
    JournalStoreConfig, ballot_id, commit_error, decode, encode, id_ballot, open_error, store_id,
    undecodable,
};
use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::matchmaker::{MatchmakerStorage, Registrations};
use crate::storage::{StorageError, StorageRecord, WriteOutcome};

/// The kind byte of a registration's identity.
const REGISTRATION: u8 = 2;

/// Where a registration's generation sits in its identity: after the ballot
/// and the kind byte, in the identity's last bytes.
const GENERATION_AT: usize = 17;

// The generation fits the identity's bytes after the ballot and kind.
const _: () = assert!(ID_SIZE - GENERATION_AT >= 7);

/// A registration's identity: its ballot, the registration kind byte and the
/// generation it belongs to (the low 7 bytes: a generation is a handover
/// count, far below 2^56).
fn registration_id(ballot: Ballot, generation: MatchmakerGeneration) -> Id {
    assert!(
        generation.0 < 1 << 56,
        "a generation fits a registration's identity"
    );
    let mut id = ballot_id(ballot, REGISTRATION);
    id[GENERATION_AT..].copy_from_slice(&generation.0.to_le_bytes()[..ID_SIZE - GENERATION_AT]);
    // Pair of `id_generation`.
    assert!(
        id_generation(&id) == generation,
        "a registration's identity reads back as its generation"
    );
    id
}

/// The generation a registration's identity names.
fn id_generation(id: &Id) -> MatchmakerGeneration {
    let mut word = [0; 8];
    word[..ID_SIZE - GENERATION_AT].copy_from_slice(&id[GENERATION_AT..]);
    MatchmakerGeneration(u64::from_le_bytes(word))
}

/// The matchmaker's metainfo: the format marker (#183, #207) and the
/// durable scalars.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct MatchMeta {
    /// Set once by `format`, never cleared or edited.
    formatted: Option<MatchmakerConfig>,
    scalars: MatchmakerHardState,
    /// Every journal a registration of this store was staged in (#190),
    /// named with the metainfo that vouches for its first registration: the
    /// floor a damaged entry, whose journal is unknown, is judged against.
    #[serde(default)]
    journals: BTreeSet<JournalId>,
}

impl MatchMeta {
    /// The lowest watermark of every journal this store names: a damaged
    /// entry below it is collected whatever journal it belongs to.
    fn lowest_watermark(&self) -> Ballot {
        self.journals
            .iter()
            .map(|journal| self.scalars.gc_watermark(*journal))
            .min()
            .unwrap_or_else(Ballot::zero)
    }
}

/// A registration's payload: its journal and the registration itself.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Stored {
    journal: JournalId,
    registration: Registration,
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
    /// The metainfo as of the last commit that wrote it (or the boot that
    /// read it).
    durable_meta: MatchMeta,
    meta_dirty: bool,
    registry: BTreeMap<(JournalId, Ballot), Registration>,
    /// Where each registration lives.
    positions: BTreeMap<(JournalId, Ballot), u64>,
    next_position: u64,
    /// Registrations staged since the last sync, by position: the first
    /// commit.
    puts: BTreeMap<u64, (Id, Vec<u8>)>,
    /// Positions staged for clearing since the last sync: the last commit.
    clears: Vec<Range<u64>>,
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
            durable_meta: MatchMeta::default(),
            meta_dirty: false,
            registry: BTreeMap::new(),
            positions: BTreeMap::new(),
            next_position: 0,
            puts: BTreeMap::new(),
            clears: Vec::new(),
        }
    }

    /// The directory the journal lives in.
    #[must_use]
    pub fn dir(&self) -> &str {
        &self.dir
    }

    /// Whether the next sync writes anything (observation: a harness aims
    /// a power cut at a commit that writes).
    #[must_use]
    pub fn has_staged(&self) -> bool {
        self.meta_dirty || !self.puts.is_empty() || !self.clears.is_empty()
    }

    /// Whether the store of the matchmaker plane `id` under `dir` carries
    /// its format marker, read from the journal's metainfo alone
    /// (`Journal::peek_meta`), like
    /// [`JournalStorage::peek_formatted`](super::JournalStorage::peek_formatted):
    /// nothing recovered, repaired or created. `false` where no journal
    /// exists.
    ///
    /// # Errors
    ///
    /// A [`StorageError::Corruption`] when no metainfo copy is valid or it
    /// does not decode, and the I/O verdict when the namespace cannot be
    /// read.
    pub async fn peek_formatted(
        provider: &P,
        dir: &str,
        id: JournalIdentifier,
    ) -> Result<bool, StorageError> {
        let Some(bytes) = Journal::peek_meta(provider, dir, store_id(id))
            .await
            .map_err(|e| open_error(&e, StorageRecord::MatchmakerScalars))?
        else {
            return Ok(false);
        };
        let meta: MatchMeta =
            decode(&bytes).ok_or(undecodable(StorageRecord::MatchmakerScalars))?;
        Ok(meta.formatted.is_some())
    }

    /// Each journal's effective configuration covers every
    /// reconfiguration this registry keeps: a sync commits a
    /// reconfiguration's registration before the metainfo that raises the
    /// scalar over it, so a crash between the two leaves the scalar
    /// behind, and the boot raises it as that commit would have
    /// (persisted with the next metainfo write).
    fn raise_effective(&mut self) {
        assert!(
            self.positions.len() == self.registry.len(),
            "every live registration has its position"
        );
        let mut newest: BTreeMap<JournalId, (Ballot, &Registration)> = BTreeMap::new();
        for ((journal, ballot), registration) in &self.registry {
            if registration.kind.is_reconfiguration() {
                newest.insert(*journal, (*ballot, registration));
            }
        }
        for (journal, (ballot, registration)) in newest {
            if self
                .meta
                .scalars
                .effective(journal)
                .is_none_or(|(held, _)| *held < ballot)
            {
                self.meta.scalars.journal_mut(journal).effective =
                    Some((ballot, registration.config.clone()));
                self.meta_dirty = true;
            }
        }
        assert!(
            self.registry
                .iter()
                .all(|((journal, ballot), registration)| {
                    !registration.kind.is_reconfiguration()
                        || self
                            .meta
                            .scalars
                            .effective(*journal)
                            .is_some_and(|(held, _)| held >= ballot)
                }),
            "every kept reconfiguration is covered by its journal's effective configuration"
        );
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
        self.durable_meta = self.meta.clone();
        let lowest = self.meta.lowest_watermark();
        let generation = self.meta.scalars.generation;
        let replay = journal.replay(..).await.map_err(|_| StorageError::Io {
            record: StorageRecord::Store,
            outcome: WriteOutcome::Unknown,
        })?;
        for (position, read) in replay {
            self.next_position = self.next_position.max(position + 1);
            // What the durable metainfo does not vouch for: of another
            // generation (an install whose metainfo never landed, or whose
            // clears did not), or below its journal's watermark. Dropped, and
            // its clear staged, damaged or not: nothing it said still
            // matters.
            let id = match &read {
                Ok(entry) => entry.id,
                Err(ReadError::Damaged { id, .. }) => *id,
                Err(ReadError::Empty { .. } | ReadError::Io(_)) => {
                    return Err(StorageError::Io {
                        record: StorageRecord::Store,
                        outcome: WriteOutcome::Unknown,
                    });
                }
            };
            let (ballot, _) = id_ballot(&id);
            if ballot < lowest || id_generation(&id) != generation {
                self.clears.push(position..position + 1);
                continue;
            }
            let Ok(entry) = read else {
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
            };
            let stored: Stored =
                decode(&entry.payload).ok_or(undecodable(StorageRecord::Registration(ballot)))?;
            if ballot < self.meta.scalars.gc_watermark(stored.journal) {
                self.clears.push(position..position + 1);
                continue;
            }
            // A journal the metainfo does not name yet: a register whose
            // metainfo never landed, kept like any other and named with the
            // next metainfo write.
            if self.meta.journals.insert(stored.journal) {
                self.meta_dirty = true;
            }
            self.registry
                .insert((stored.journal, ballot), stored.registration);
            self.positions.insert((stored.journal, ballot), position);
        }
        self.raise_effective();
        // Boot side of the watermark pair: nothing live below its journal's.
        assert!(
            self.registry
                .keys()
                .all(|(journal, b)| *b >= self.meta.scalars.gc_watermark(*journal)),
            "no registration survives below the watermark"
        );
        assert!(
            self.positions.len() == self.registry.len(),
            "every live registration has its position"
        );
        assert!(
            self.registry
                .keys()
                .all(|(journal, _)| self.meta.journals.contains(journal)),
            "the metainfo names every live journal"
        );
        assert!(
            self.registry
                .iter()
                .filter(|(_, registration)| registration.kind.is_reconfiguration())
                .all(|((journal, ballot), _)| self
                    .meta
                    .scalars
                    .effective(*journal)
                    .is_some_and(|(held, _)| held >= ballot)),
            "the effective configuration covers every kept reconfiguration"
        );
        self.journal = Some(journal);
        Ok(())
    }

    /// Stage `registration` under `ballot` in `journal` at its own
    /// position, naming the journal in the metainfo on its first one.
    fn stage_registration(
        &mut self,
        journal: JournalId,
        ballot: Ballot,
        registration: &Registration,
    ) {
        if self.meta.journals.insert(journal) {
            self.meta_dirty = true;
        }
        let position = *self.positions.entry((journal, ballot)).or_insert_with(|| {
            let position = self.next_position;
            self.next_position += 1;
            position
        });
        let stored = Stored {
            journal,
            registration: registration.clone(),
        };
        self.puts.insert(
            position,
            (
                registration_id(ballot, self.meta.scalars.generation),
                encode(&stored),
            ),
        );
        self.registry
            .insert((journal, ballot), registration.clone());
        assert!(
            self.meta.journals.contains(&journal),
            "a staged registration's journal is named"
        );
    }

    /// Drop every registration below its journal's durable watermark.
    fn collect(&mut self) {
        let collected: Vec<(JournalId, Ballot)> = self
            .registry
            .keys()
            .filter(|(journal, b)| *b < self.meta.scalars.gc_watermark(*journal))
            .copied()
            .collect();
        for key in collected {
            self.registry.remove(&key);
            if let Some(position) = self.positions.remove(&key) {
                self.puts.remove(&position);
                self.clears.push(position..position + 1);
            }
        }
        assert!(
            self.registry
                .keys()
                .all(|(journal, b)| *b >= self.meta.scalars.gc_watermark(*journal)),
            "no registration survives below the watermark"
        );
    }
}

impl<P: StorageProvider> RegistryStorage for JournalMatchmakerStorage<P> {
    fn initial_state(&self) -> MatchmakerHardState {
        self.meta.scalars.clone()
    }

    fn registration(&self, journal: JournalId, ballot: Ballot) -> Option<Registration> {
        self.registry.get(&(journal, ballot)).cloned()
    }

    fn registered(&self) -> Vec<(JournalId, Ballot)> {
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
        journal: JournalId,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        self.stage_registration(journal, ballot, registration);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    async fn set_gc_watermark(
        &mut self,
        journal: JournalId,
        watermark: Ballot,
    ) -> Result<(), StorageError> {
        let before = self.meta.scalars.gc_watermark(journal);
        if watermark > before {
            self.meta.scalars.journal_mut(journal).gc_watermark = watermark;
            self.meta_dirty = true;
            self.collect();
        }
        // The durable watermark only rises, and reaches what was asked.
        assert!(
            self.meta.scalars.gc_watermark(journal) >= before,
            "a watermark never falls"
        );
        assert!(
            self.meta.scalars.gc_watermark(journal) >= watermark,
            "a raised watermark is held"
        );
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        let held = std::mem::replace(&mut self.meta.scalars, scalars.clone());
        // Every journal keeps the higher of the two watermarks.
        for (journal, kept) in held.journals {
            let watermark = self
                .meta
                .scalars
                .gc_watermark(journal)
                .max(kept.gc_watermark);
            self.meta.scalars.journal_mut(journal).gc_watermark = watermark;
        }
        self.meta_dirty = true;
        self.collect();
        assert!(
            self.meta.scalars.generation == scalars.generation,
            "the scalars hold the written generation"
        );
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &Registrations,
    ) -> Result<(), StorageError> {
        // Replaced whole, in the same commit: every registration goes, the
        // successor's arrive at fresh positions.
        if self.next_position > 0 {
            self.puts.clear();
            self.clears.push(0..self.next_position);
        }
        self.registry.clear();
        self.positions.clear();
        self.meta.scalars = scalars.clone();
        self.meta.journals.clear();
        self.meta_dirty = true;
        for (journal, ledger) in registrations {
            for (ballot, registration) in ledger.range(scalars.gc_watermark(*journal)..) {
                self.stage_registration(*journal, *ballot, registration);
            }
        }
        assert!(
            self.meta.scalars.generation == scalars.generation,
            "an installed registry holds the successor generation"
        );
        Ok(())
    }

    /// Up to two commits, in the order the [module docs](self) argue: the
    /// new registrations with the metainfo, then the clears. Each is durable
    /// before the next starts.
    #[tracing::instrument(level = "trace", skip_all, fields(dir = %self.dir))]
    async fn sync(&mut self) -> Result<(), StorageError> {
        if !self.has_staged() {
            return Ok(());
        }
        let puts = std::mem::take(&mut self.puts);
        let clears = std::mem::take(&mut self.clears);
        // The registrations, packed into batches that each fit one segment.
        let geometry = self.store.geometry;
        let mut batch = Batch::new();
        for (position, (id, payload)) in puts {
            if !batch.is_empty() && !batch.fits_another(geometry, payload.len()) {
                self.commit(std::mem::take(&mut batch)).await?;
            }
            batch.put(position, id, payload);
        }
        // The metainfo rides the last registrations batch: the journal
        // writes it only once that batch is durable (moonpool#309), and the
        // earlier batches were synced before it.
        if self.meta_dirty {
            batch.set_meta(encode(&self.meta));
        }
        if !batch.is_empty() || self.meta_dirty {
            self.commit(batch).await?;
        }
        if self.meta_dirty {
            self.durable_meta = self.meta.clone();
            self.meta_dirty = false;
        }
        // The clears, last: until they land a boot drops what they clear.
        if !clears.is_empty() {
            // The registrations are durable and the clears are not: a boot
            // must drop what the clears would have cleared.
            let hinted = moonpool_buggify::hint!("registrations durable, clears staged");
            if hinted.strike() == Strike::Killed {
                moonpool_assertions::reachable!(
                    "a matchmaker crashes between its registrations and its clears"
                );
            }
            hinted.await;
            let mut batch = Batch::new();
            for range in clears {
                batch.clear(range);
            }
            self.commit(batch).await?;
        }
        assert!(
            self.durable_meta == self.meta,
            "a synced registry's metainfo is durable"
        );
        Ok(())
    }
}

impl<P: StorageProvider> JournalMatchmakerStorage<P> {
    /// Commit `batch`, creating the journal first if none is on disk yet,
    /// with the last durable metainfo and the format marker: a creation
    /// lands before the batch it opens for.
    async fn commit(&mut self, batch: Batch) -> Result<(), StorageError> {
        if self.journal.is_none() {
            let early = MatchMeta {
                formatted: self.meta.formatted.clone(),
                ..self.durable_meta.clone()
            };
            let created = Journal::create(
                self.provider.clone(),
                &self.dir,
                store_id(self.id),
                self.store.journal(),
                &encode(&early),
            )
            .await
            .map_err(|e| open_error(&e, StorageRecord::MatchmakerScalars))?;
            self.journal = Some(created);
            self.durable_meta = early;
        }
        let journal = self.journal.as_mut().expect("created above");
        journal.commit(batch).await.map_err(|e| commit_error(&e))
    }
}
