//! [`JournalMatchmakerStorage`]: the matchmaker's
//! [`MatchmakerStorage`](crate::MatchmakerStorage) on `moonpool-journal`.
//!
//! Each registration is an entry at a registration number of its own, its
//! ballot and its generation in the entry's identity, its journal in the
//! payload (#190: one store holds every journal of its tenant's set); the
//! scalars (generation, freeze, decree, each journal's watermark) are
//! entries too, and the journal's metainfo holds the format marker and
//! names them. A raised watermark clears its journal's registrations below
//! it; an install clears them all and writes the successor's.
//!
//! **The scalars are paged** (#439): the metainfo is one block (about 4 KB),
//! and a tenant's scalars grow with its journals and its pending
//! bootstraps. So the scalars are encoded whole and cut into chunks of
//! [`JournalStoreConfig::meta_chunk`] bytes, each an entry whose identity
//! names its epoch and its index. The metainfo is a header: the format
//! marker, the epoch, and the positions of the chunks. Every write of the
//! scalars takes a fresh epoch (above every epoch on the disk) and fresh
//! positions, in the commit that carries the metainfo, so a durable header
//! vouches for its chunks (moonpool#309). The chunks of earlier epochs go
//! with the clears; a boot drops every chunk the header does not name, and a
//! damaged or missing chunk it names is a crash verdict.
//!
//! **A sync is up to three commits, in an order every crash point survives**
//! (#176): the boot's own clears, the new registrations with the metainfo,
//! then the clears. The
//! journal writes a batch's metainfo only once the batch is durable
//! (moonpool#309), so a durable metainfo vouches for its registrations; the
//! reverse does not hold, so a boot keeps only the registrations the durable
//! metainfo vouches for: at or above its watermark and of its generation. A
//! crash after the puts leaves
//! registrations the old metainfo does not count yet (a register's, kept:
//! the driver never acknowledged it, and it stands like any other; an
//! install's, of the successor generation, dropped); a crash after the
//! metainfo leaves the clears undone, and the boot drops what they would
//! have cleared. The registrations may land in a commit of their own before
//! the metainfo's (an inline BUGGIFY decision): that is the crash point
//! between them, made likely. The one scalar that must cover a registration, the
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
//! one whose journal is not named was never acknowledged). An undamaged
//! entry is judged by its own journal's watermark alone, never by that
//! cross-journal floor (#488 (matchmaker boot drop)).
//!
//! **What a boot drops stays dropped**: the floor falls when a later
//! metainfo names a new journal (its watermark is zero), so the boot's
//! clears commit first, alone, before any later metainfo. Before #488 a
//! boot dropped an unacknowledged register of an unnamed journal below
//! another journal's watermark and only staged its clear; a later sync
//! named a new journal, a crash came before the clear, and the next boot
//! brought the registration back (witness 4532891387820985116 on the #212
//! (placement) branch, PR #478: "a restart never invents or alters a
//! registration"). Damage
//! the journal reports ambiguous (the last batch of a one-sync commit, where
//! a crash and rot look alike) is
//! [`Undecidable`](crate::CorruptionVerdict::Undecidable); anywhere else
//! [`Corrupted`](crate::CorruptionVerdict::Corrupted).

use std::collections::{BTreeMap, BTreeSet};

use moonpool_core::LocalStorageProvider;
use std::ops::Range;

use moonpool_buggify::hint::Strike;
use moonpool_journal::{Batch, Entry, ID_SIZE, Id, Journal, ReadError, State};
use paros_core::{
    Ballot, JournalId, JournalIdentifier, MatchmakerConfig, MatchmakerGeneration,
    MatchmakerHardState, Registration, RegistryStorage,
};
use serde::{Deserialize, Serialize};

use super::{
    JournalStoreConfig, ballot_id, commit_error, decode, encode, id_ballot, open_error, store_id,
    undecodable,
};
use crate::matchmaker::{MatchmakerStorage, Registrations};
use crate::model::corruption::{CorruptionVerdict, IntegrityFault};
use crate::storage::{StorageError, StorageRecord, WriteOutcome};

/// The kind byte of a registration's identity.
const REGISTRATION: u8 = 2;

/// The kind byte of a scalar chunk's identity (#439).
const SCALARS: u8 = 3;

/// A scalar chunk's identity: its epoch, its index, and the kind byte where
/// a registration's sits.
fn chunk_id(epoch: u64, index: u64) -> Id {
    let mut id = [0; ID_SIZE];
    id[..8].copy_from_slice(&epoch.to_le_bytes());
    id[8..16].copy_from_slice(&index.to_le_bytes());
    id[16] = SCALARS;
    // Pair of `id_chunk`.
    assert!(
        id_chunk(&id) == Some((epoch, index)),
        "a chunk's identity reads back as its epoch and index"
    );
    id
}

/// The epoch and index a scalar chunk's identity names; `None` for any
/// other entry.
fn id_chunk(id: &Id) -> Option<(u64, u64)> {
    let word = |at: usize| u64::from_le_bytes(id[at..at + 8].try_into().expect("8 bytes"));
    (id[16] == SCALARS).then(|| (word(0), word(8)))
}

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

/// The matchmaker's logical metainfo: the format marker (#183, #207) and
/// the durable scalars. The disk holds it as a [`Header`] in the journal's
/// metainfo and the [`Body`] in chunks (#439).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MatchMeta {
    /// Set once by `format`, never cleared or edited.
    formatted: Option<MatchmakerConfig>,
    scalars: MatchmakerHardState,
    /// Every journal a registration of this store was staged in (#190),
    /// named with the metainfo that vouches for its first registration: the
    /// floor a damaged entry, whose journal is unknown, is judged against.
    journals: BTreeSet<JournalId>,
}

/// The journal's metainfo (#439): the format marker and where the scalars'
/// chunks are. It stays a few bytes whatever the tenant holds.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Header {
    formatted: Option<MatchmakerConfig>,
    /// The epoch of the chunks this header names; `0` names none.
    epoch: u64,
    /// The chunks' positions: chunk `i` is at `chunks.start + i`.
    chunks: Range<u64>,
}

/// A replayed entry: its position and what the read found.
type Replayed = (u64, Result<Entry, ReadError>);

/// A staged entry: its position, identity and payload.
type Staged = (u64, (Id, Vec<u8>));

/// What the chunks carry, encoded whole and then cut.
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Body {
    scalars: MatchmakerHardState,
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
pub struct JournalMatchmakerStorage<P: LocalStorageProvider> {
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
    /// Positions the boot dropped: the first commit of the next sync, so no
    /// later metainfo (whose floor may be lower) meets them again.
    boot_clears: Vec<Range<u64>>,
    /// The highest chunk epoch on the disk (#439): the next write of the
    /// scalars takes the one above it.
    epoch: u64,
    /// The chunks the durable header names; cleared once a later header is
    /// durable.
    chunks: Range<u64>,
}

impl<P: LocalStorageProvider> std::fmt::Debug for JournalMatchmakerStorage<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalMatchmakerStorage")
            .field("dir", &self.dir)
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}

impl<P: LocalStorageProvider> JournalMatchmakerStorage<P> {
    /// A store kept under `dir` on `provider`, for the matchmaker plane of
    /// `id` (the identity its journal is stamped with). Nothing is read
    /// until the boot scan.
    ///
    /// # Panics
    ///
    /// When `store` names a scalar chunk of zero bytes: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn new(
        provider: P,
        dir: impl Into<String>,
        id: JournalIdentifier,
        store: JournalStoreConfig,
    ) -> Self {
        assert!(store.meta_chunk > 0, "a scalar chunk carries a byte");
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
            boot_clears: Vec::new(),
            epoch: 0,
            chunks: 0..0,
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
        self.meta_dirty
            || !self.puts.is_empty()
            || !self.clears.is_empty()
            || !self.boot_clears.is_empty()
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
        let header: Header = decode(&bytes).ok_or(undecodable(StorageRecord::MatchmakerScalars))?;
        Ok(header.formatted.is_some())
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
        super::drop_caches_for_open(&self.provider, &self.dir).await?;
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
        let header: Header =
            decode(journal.meta()).ok_or(undecodable(StorageRecord::MatchmakerScalars))?;
        let replay = journal.replay(..).await.map_err(|_| StorageError::Io {
            record: StorageRecord::Store,
            outcome: WriteOutcome::Unknown,
        })?;
        let replay = self.read_scalars(&journal, &header, replay)?;
        let lowest = self.meta.lowest_watermark();
        let generation = self.meta.scalars.generation;
        for (position, read) in replay {
            self.next_position = self.next_position.max(position + 1);
            // What the durable metainfo does not vouch for: of another
            // generation (an install whose metainfo never landed, or whose
            // clears did not), or below its journal's watermark. Dropped, and
            // its clear staged for the next sync's first commit, damaged or
            // not: nothing it said still matters. A damaged entry's journal
            // is unknown, so it is judged by the lowest watermark; an
            // undamaged one only by its own journal's, below (#488).
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
            if (read.is_err() && ballot < lowest) || id_generation(&id) != generation {
                self.boot_clears.push(position..position + 1);
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
                self.boot_clears.push(position..position + 1);
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
        self.check_boot_drops();
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

    /// The scalars the durable `header` names, read from their chunks
    /// (#439); every other chunk is dropped. Returns the rest of `replay`.
    ///
    /// A chunk the header names that is missing, damaged or of another
    /// epoch is a crash verdict: the header is written only once its
    /// chunks are durable, so the scalars are lost.
    fn read_scalars(
        &mut self,
        journal: &Journal<P>,
        header: &Header,
        replay: Vec<Replayed>,
    ) -> Result<Vec<Replayed>, StorageError> {
        assert!(
            header.epoch > 0 || header.chunks.is_empty(),
            "a header names chunks only at an epoch"
        );
        let lost = |verdict| StorageError::Corruption {
            record: StorageRecord::MatchmakerScalars,
            fault: IntegrityFault::LostWrite,
            verdict,
        };
        let mut bytes = Vec::new();
        let mut rest = Vec::with_capacity(replay.len());
        let mut next = header.chunks.start;
        self.epoch = header.epoch;
        for (position, read) in replay {
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
            let Some((epoch, index)) = id_chunk(&id) else {
                rest.push((position, read));
                continue;
            };
            self.next_position = self.next_position.max(position + 1);
            self.epoch = self.epoch.max(epoch);
            if !header.chunks.contains(&position) {
                self.boot_clears.push(position..position + 1);
                continue;
            }
            // The header's chunks are read in order, each where it says.
            let Ok(entry) = read else {
                let verdict = if matches!(journal.state(position), State::Ambiguous { .. }) {
                    CorruptionVerdict::Undecidable
                } else {
                    CorruptionVerdict::Corrupted
                };
                return Err(StorageError::Corruption {
                    record: StorageRecord::MatchmakerScalars,
                    fault: IntegrityFault::ChecksumMismatch,
                    verdict,
                });
            };
            // A chunk skipped is a chunk lost; one that names another epoch
            // or index where the header names this one is misdirected.
            if position != next {
                return Err(lost(CorruptionVerdict::Corrupted));
            }
            if epoch != header.epoch || index != position - header.chunks.start {
                return Err(undecodable(StorageRecord::MatchmakerScalars));
            }
            bytes.extend_from_slice(&entry.payload);
            next += 1;
        }
        if next != header.chunks.end {
            return Err(lost(CorruptionVerdict::Corrupted));
        }
        let body: Body = if header.chunks.is_empty() {
            Body::default()
        } else {
            decode(&bytes).ok_or(undecodable(StorageRecord::MatchmakerScalars))?
        };
        self.meta = MatchMeta {
            formatted: header.formatted.clone(),
            scalars: body.scalars,
            journals: body.journals,
        };
        self.durable_meta = self.meta.clone();
        self.chunks = header.chunks.clone();
        // Boot side of the chunk pair: the header's chunks are the ones
        // kept, and the next epoch is above every one on the disk.
        assert!(
            self.epoch >= header.epoch,
            "the epoch floor covers the header's"
        );
        assert!(
            self.boot_clears
                .iter()
                .all(|cleared| !header.chunks.contains(&cleared.start)),
            "a boot never drops a chunk its header names"
        );
        Ok(rest)
    }

    /// The scalars cut into chunks at a fresh epoch and fresh positions,
    /// and the header that names them (#439).
    fn cut_scalars(&mut self) -> (Header, Vec<Staged>) {
        let body = encode(&Body {
            scalars: self.meta.scalars.clone(),
            journals: self.meta.journals.clone(),
        });
        self.epoch += 1;
        let start = self.next_position;
        let chunks: Vec<Staged> = body
            .chunks(self.store.meta_chunk)
            .zip(0_u64..)
            .map(|(bytes, index)| (start + index, (chunk_id(self.epoch, index), bytes.to_vec())))
            .collect();
        let count = u64::try_from(chunks.len()).expect("a chunk count fits a u64");
        self.next_position = start + count;
        if count > 1 {
            moonpool_assertions::reachable!("matchmaker: its scalars span several chunks");
        }
        let header = Header {
            formatted: self.meta.formatted.clone(),
            epoch: self.epoch,
            chunks: start..start + count,
        };
        // Write side of the chunk pair: the chunks are the whole encoding.
        assert!(count > 0, "the scalars take at least one chunk");
        assert!(
            chunks
                .iter()
                .map(|(_, (_, bytes))| bytes.len())
                .sum::<usize>()
                == body.len(),
            "the chunks carry the whole encoding"
        );
        (header, chunks)
    }

    /// Boot side of the boot-drop pair (#488 (matchmaker boot drop)): a
    /// boot stages only its own clears, never over a registration it keeps;
    /// `sync` commits them first.
    fn check_boot_drops(&self) {
        assert!(
            self.clears.is_empty() && self.puts.is_empty(),
            "a boot stages only its own clears"
        );
        assert!(
            self.boot_clears
                .iter()
                .all(|cleared| self.positions.values().all(|p| !cleared.contains(p))),
            "a boot never clears a registration it keeps"
        );
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

impl<P: LocalStorageProvider> RegistryStorage for JournalMatchmakerStorage<P> {
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

impl<P: LocalStorageProvider> MatchmakerStorage for JournalMatchmakerStorage<P> {
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

    /// Up to three commits, in the order the [module docs](self) argue: the
    /// boot's own clears, the new registrations with the metainfo, then the
    /// clears. Each is durable before the next starts.
    #[tracing::instrument(level = "trace", skip_all, fields(dir = %self.dir))]
    async fn sync(&mut self) -> Result<(), StorageError> {
        if !self.has_staged() {
            return Ok(());
        }
        let boot_clears = std::mem::take(&mut self.boot_clears);
        let puts = std::mem::take(&mut self.puts);
        let clears = std::mem::take(&mut self.clears);
        // The boot's drops, first and alone: they hold under the durable
        // metainfo's floor, and the metainfo below may name a new journal
        // and lower it, so a drop left staged past it could come back
        // (#488 (matchmaker boot drop)).
        if !boot_clears.is_empty() {
            let mut batch = Batch::new();
            for range in boot_clears {
                batch.clear(range);
            }
            self.commit(batch).await?;
            // The boot's drops are durable and nothing else of the sync is.
            let hinted = moonpool_buggify::hint!("boot drops durable, registrations staged", 0.5);
            if hinted.strike() == Strike::Killed {
                moonpool_assertions::reachable!(
                    "a matchmaker crashes between its boot drops and its registrations"
                );
            }
            hinted.await;
        }
        // On a BUGGIFY decision the registrations land in a commit of their
        // own, ahead of the metainfo: the crash point between them, a
        // register the metainfo does not name yet, made likely (#488).
        let split =
            !puts.is_empty() && self.meta_dirty && moonpool_buggify::buggify_fault_with_prob!(0.5);
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
        if split {
            moonpool_assertions::reachable!(
                "a matchmaker commits its registrations before its metainfo"
            );
            self.commit(std::mem::take(&mut batch)).await?;
            // The registrations are durable and the metainfo is not: a boot
            // keeps a register's and drops an install's.
            let hinted = moonpool_buggify::hint!("registrations durable, metainfo staged", 1.0);
            if hinted.strike() == Strike::Killed {
                moonpool_assertions::reachable!(
                    "a matchmaker crashes between its registrations and its metainfo"
                );
            }
            hinted.await;
        }
        let mut clears = clears;
        let mut written = None;
        if self.meta_dirty {
            // The scalars at a fresh epoch and fresh positions, ahead of the
            // header that names them (#439).
            let (header, chunks) = self.cut_scalars();
            for (position, (id, payload)) in chunks {
                if !batch.is_empty() && !batch.fits_another(geometry, payload.len()) {
                    self.commit(std::mem::take(&mut batch)).await?;
                }
                batch.put(position, id, payload);
            }
            batch.set_meta(encode(&header));
            written = Some(header);
        }
        if !batch.is_empty() || self.meta_dirty {
            self.commit(batch).await?;
        }
        if let Some(header) = written {
            self.durable_meta = self.meta.clone();
            self.meta_dirty = false;
            // The chunks the header replaced go with the clears.
            let replaced = std::mem::replace(&mut self.chunks, header.chunks);
            if !replaced.is_empty() {
                clears.push(replaced);
            }
        }
        // The clears, last: until they land a boot drops what they clear.
        if !clears.is_empty() {
            // The registrations are durable and the clears are not: a boot
            // must drop what the clears would have cleared. Clears are rare
            // (a raised GC floor, an install), and a crash here is the last
            // step of #488 (matchmaker boot drop): the rate is above the
            // other matchmaker hints (#446).
            let hinted = moonpool_buggify::hint!("registrations durable, clears staged", 0.25);
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

impl<P: LocalStorageProvider> JournalMatchmakerStorage<P> {
    /// Commit `batch`, creating the journal first if none is on disk yet,
    /// with the last durable metainfo and the format marker: a creation
    /// lands before the batch it opens for.
    async fn commit(&mut self, batch: Batch) -> Result<(), StorageError> {
        if self.journal.is_none() {
            // A store never committed holds no scalars yet: its header names
            // no chunk.
            assert!(
                self.durable_meta == MatchMeta::default() && self.chunks.is_empty(),
                "a store is created before any scalars are durable"
            );
            let early = MatchMeta {
                formatted: self.meta.formatted.clone(),
                ..self.durable_meta.clone()
            };
            let header = Header {
                formatted: early.formatted.clone(),
                epoch: 0,
                chunks: 0..0,
            };
            let created = Journal::create(
                self.provider.clone(),
                &self.dir,
                store_id(self.id),
                self.store.journal(),
                &encode(&header),
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
