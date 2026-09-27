//! [`JournalMatchmakerStorage`]: the matchmaker's
//! [`MatchmakerStorage`](crate::MatchmakerStorage) on `moonpool-journal`.
//!
//! The registry rides the same log-of-writes shape as the node store: a
//! `Register` entry per registration (its ballot in the tag), a `Scalars`
//! entry carrying the whole durable image after every scalar write (so a
//! watermark raise is one too), an `Install .. End` bracket per successor
//! activation, and `Begin .. End` checkpoints.
//!
//! **A damaged record is a crash verdict**, unless nothing it said still
//! matters: a registration below the durable watermark (collected anyway),
//! or any record a later intact `Scalars`, `Install` or checkpoint wholly
//! replaced. There is no repair in place for a registry — a matchmaker
//! whose durable state is unusable is *replaced* through a matchmaker-set
//! reconfiguration (#125) — so detection is the whole job. A damaged record
//! in the **last** append is reported as
//! [`Undecidable`](crate::CorruptionVerdict::Undecidable): a crash between
//! the identifier write and the sync leaves exactly that shape, and so does
//! rot, and no local algorithm tells them apart (CTRL Theorem A.1); anywhere
//! earlier it is [`Corrupted`](crate::CorruptionVerdict::Corrupted), because
//! a later append proves the sync returned.

use std::collections::BTreeMap;

use moonpool_core::StorageProvider;
use moonpool_journal::{EntryId, Journal, Record, Tag};
use paros_core::{Ballot, MatchmakerHardState, NodeId, Registration, RegistryStorage};
use serde::{Deserialize, Serialize};

use super::frame::{Framed, Kind, Scanned, batch_of, encode, epoch, tag, words};
use super::plan::plan;
use super::{GENESIS, JournalStoreConfig, append_error, open_error};
use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::matchmaker::MatchmakerStorage;
use crate::storage::{StorageError, StorageRecord};

/// One durable write of the matchmaker store, as one journal entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum MatchRecord {
    /// `registration` under `ballot`.
    Register {
        ballot: Ballot,
        registration: Registration,
    },
    /// The durable scalars, whole.
    Scalars(MatchmakerHardState),
    /// A registry install opens with the activated scalars.
    Install(MatchmakerHardState),
    /// A checkpoint opens with the scalars.
    Begin(MatchmakerHardState),
    /// A bracket closes.
    End,
}

fn ballot_tag(ballot: Ballot) -> Tag {
    tag([ballot.round, ballot.node.0, 0])
}

fn ballot_of(tag: &Tag) -> Ballot {
    let [round, node, _] = words(tag);
    Ballot {
        round,
        node: NodeId(node),
    }
}

impl Framed for MatchRecord {
    fn kind(&self) -> Kind {
        match self {
            MatchRecord::Register { .. } => Kind::Register,
            MatchRecord::Scalars(_) => Kind::Scalars,
            MatchRecord::Install(_) => Kind::Install,
            MatchRecord::Begin(_) => Kind::Begin,
            MatchRecord::End => Kind::End,
        }
    }

    fn tag(&self) -> Tag {
        match self {
            MatchRecord::Register { ballot, .. } => ballot_tag(*ballot),
            _ => tag([0; 3]),
        }
    }
}

/// The registry's durable state, as the records fold it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MatchImage {
    hard_state: MatchmakerHardState,
    registry: BTreeMap<Ballot, Registration>,
}

impl MatchImage {
    /// Fold one intact record — the live write and the replay alike, with
    /// [`MemMatchmakerStorage`](crate::MemMatchmakerStorage)'s semantics.
    fn apply(&mut self, record: &MatchRecord) {
        match record {
            MatchRecord::Register {
                ballot,
                registration,
            } => {
                self.registry.insert(*ballot, registration.clone());
            }
            MatchRecord::Scalars(scalars) => {
                let watermark = scalars.gc_watermark.max(self.hard_state.gc_watermark);
                self.hard_state = scalars.clone();
                self.hard_state.gc_watermark = watermark;
                self.registry = self.registry.split_off(&watermark);
            }
            MatchRecord::Install(scalars) | MatchRecord::Begin(scalars) => {
                self.hard_state = scalars.clone();
                self.registry.clear();
            }
            MatchRecord::End => {}
        }
    }

    fn checkpoint(&self) -> Vec<MatchRecord> {
        let mut records = vec![MatchRecord::Begin(self.hard_state.clone())];
        records.extend(
            self.registry
                .iter()
                .map(|(ballot, registration)| MatchRecord::Register {
                    ballot: *ballot,
                    registration: registration.clone(),
                }),
        );
        records.push(MatchRecord::End);
        records
    }
}

/// The matchmaker's durable store on `moonpool-journal`, over any moonpool
/// [`StorageProvider`]. The [`journal` docs](super) hold the shared shape.
///
/// **A damaged record is a crash verdict**, unless nothing it said still
/// matters: a registration below the durable watermark, or a record a later
/// intact scalar write, install or checkpoint wholly replaced. A registry is
/// never repaired in place — a matchmaker whose durable state is unusable is
/// replaced (#125) — so detection is the whole job; damage in the last
/// append is [`Undecidable`](crate::CorruptionVerdict::Undecidable) (a torn
/// write and rot look alike there), anywhere earlier
/// [`Corrupted`](crate::CorruptionVerdict::Corrupted).
///
/// Like [`JournalStorage`](super::JournalStorage) it opens and loads in
/// [`boot_scan`](MatchmakerStorage::boot_scan); a write before it runs it.
pub struct JournalMatchmakerStorage<P: StorageProvider> {
    provider: P,
    dir: String,
    store: JournalStoreConfig,
    journal: Option<Journal<P>>,
    image: MatchImage,
    staged: Vec<MatchRecord>,
    batch: u64,
}

impl<P: StorageProvider> std::fmt::Debug for JournalMatchmakerStorage<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalMatchmakerStorage")
            .field("dir", &self.dir)
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}

/// A damaged record the fold has not yet seen replaced.
struct Pending {
    id: EntryId,
    kind: Option<Kind>,
    record: StorageRecord,
}

impl<P: StorageProvider> JournalMatchmakerStorage<P> {
    /// A store kept under `dir` on `provider`. Nothing is read until the
    /// boot scan.
    #[must_use]
    pub fn new(provider: P, dir: impl Into<String>, store: JournalStoreConfig) -> Self {
        Self {
            provider,
            dir: dir.into(),
            store,
            journal: None,
            image: MatchImage::default(),
            staged: Vec::new(),
            batch: 0,
        }
    }

    /// The directory the journal lives in.
    #[must_use]
    pub fn dir(&self) -> &str {
        &self.dir
    }

    fn stage(&mut self, record: MatchRecord) {
        self.image.apply(&record);
        self.staged.push(record);
    }

    async fn opened(&mut self) -> Result<(), StorageError> {
        if self.journal.is_none() {
            self.load().await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(dir = %self.dir))]
    async fn load(&mut self) -> Result<(), StorageError> {
        let (mut journal, recovery) =
            Journal::open(self.provider.clone(), &self.dir, self.store.journal())
                .await
                .map_err(|e| open_error(&e))?;
        if recovery.torn_tail {
            tracing::info!(dir = %self.dir, "journal_torn_tail_discarded");
        }
        let entries = journal
            .read_range(journal.start_index()..journal.next_index())
            .await
            .map_err(|e| open_error(&e))?;
        let mut scanned: Vec<Scanned<MatchRecord>> =
            entries.into_iter().map(Scanned::new).collect();
        let plan = plan(&scanned, journal.start_index() == GENESIS);
        if plan.lost {
            return Err(StorageError::Corruption {
                record: StorageRecord::MatchmakerScalars,
                fault: IntegrityFault::LostWrite,
                verdict: CorruptionVerdict::Corrupted,
            });
        }
        if let Some(cut) = plan.cut_at {
            let from = scanned[cut].id.index;
            journal
                .truncate_suffix(from)
                .await
                .map_err(|e| append_error(&e))?;
            scanned.truncate(cut);
        }
        let last_batch = scanned.iter().map(|entry| batch_of(entry.id.epoch)).max();
        let mut image = MatchImage::default();
        let mut pending: Vec<Pending> = Vec::new();
        let mut skip = plan.skip.iter().peekable();
        let mut at = plan.start;
        while at < scanned.len() {
            if let Some(range) = skip.peek()
                && range.start == at
            {
                at = range.end;
                skip.next();
                continue;
            }
            let entry = &scanned[at];
            at += 1;
            if let Some(record) = &entry.record {
                // A whole-image record replaces what every damaged record
                // before it could have said; a scalar write replaces the
                // scalars.
                match record {
                    MatchRecord::Install(_) | MatchRecord::Begin(_) => pending.clear(),
                    MatchRecord::Scalars(_) => pending.retain(|p| p.kind != Some(Kind::Scalars)),
                    MatchRecord::Register { .. } | MatchRecord::End => {}
                }
                image.apply(record);
                continue;
            }
            let record = match entry.kind {
                Some(Kind::End) => continue,
                Some(Kind::Register) => StorageRecord::Registration(ballot_of(&entry.id.tag)),
                Some(Kind::Scalars | Kind::Install | Kind::Begin) => {
                    StorageRecord::MatchmakerScalars
                }
                _ => StorageRecord::Store,
            };
            pending.push(Pending {
                id: entry.id,
                kind: entry.kind,
                record,
            });
        }
        // A damaged registration below the durable watermark was collected
        // anyway.
        let watermark = image.hard_state.gc_watermark;
        pending.retain(|p| !matches!(p.record, StorageRecord::Registration(b) if b < watermark));
        if let Some(first) = pending.first() {
            let verdict = if Some(batch_of(first.id.epoch)) == last_batch {
                CorruptionVerdict::Undecidable
            } else {
                CorruptionVerdict::Corrupted
            };
            tracing::warn!(dir = %self.dir, index = first.id.index, record = %first.record, "matchmaker_record_corrupt");
            return Err(StorageError::Corruption {
                record: first.record,
                fault: IntegrityFault::ChecksumMismatch,
                verdict,
            });
        }
        self.batch = plan.last_batch + 1;
        self.image = image;
        self.staged.clear();
        self.journal = Some(journal);
        Ok(())
    }

    async fn append(&mut self, records: &[MatchRecord]) -> Result<(), StorageError> {
        let batch = self.batch;
        let journal = self.journal.as_mut().expect("opened before appending");
        let payloads: Vec<Vec<u8>> = records.iter().map(encode).collect();
        let framed: Vec<Record<'_>> = records
            .iter()
            .zip(&payloads)
            .map(|(record, payload)| {
                Record::new(epoch(record.kind(), batch), payload).with_tag(record.tag())
            })
            .collect();
        journal
            .append(&framed)
            .await
            .map_err(|e| append_error(&e))?;
        self.batch += 1;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(dir = %self.dir))]
    async fn maybe_checkpoint(&mut self) -> Result<(), StorageError> {
        let journal = self.journal.as_ref().expect("opened before a checkpoint");
        if journal.next_index() - journal.start_index() < self.store.checkpoint_after.max(1) {
            return Ok(());
        }
        let begin = journal.next_index();
        let records = self.image.checkpoint();
        self.append(&records).await?;
        let journal = self.journal.as_mut().expect("opened before a checkpoint");
        journal
            .truncate_prefix(begin)
            .await
            .map_err(|e| append_error(&e))?;
        tracing::debug!(dir = %self.dir, begin, "journal_checkpointed");
        Ok(())
    }
}

impl<P: StorageProvider> RegistryStorage for JournalMatchmakerStorage<P> {
    fn initial_state(&self) -> MatchmakerHardState {
        self.image.hard_state.clone()
    }

    fn registration(&self, ballot: Ballot) -> Option<Registration> {
        self.image.registry.get(&ballot).cloned()
    }

    fn registered_ballots(&self) -> Vec<Ballot> {
        self.image.registry.keys().copied().collect()
    }
}

impl<P: StorageProvider> MatchmakerStorage for JournalMatchmakerStorage<P> {
    #[tracing::instrument(level = "debug", skip_all, fields(dir = %self.dir))]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.journal = None;
        self.load().await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    async fn register(
        &mut self,
        ballot: Ballot,
        registration: &Registration,
    ) -> Result<(), StorageError> {
        self.opened().await?;
        self.stage(MatchRecord::Register {
            ballot,
            registration: registration.clone(),
        });
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    async fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        self.opened().await?;
        if watermark > self.image.hard_state.gc_watermark {
            let mut scalars = self.image.hard_state.clone();
            scalars.gc_watermark = watermark;
            self.stage(MatchRecord::Scalars(scalars));
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn set_scalars(&mut self, scalars: &MatchmakerHardState) -> Result<(), StorageError> {
        self.opened().await?;
        // The record carries the image's watermark (the max with the
        // durable one), so a later scalar write alone restates it whole.
        let mut scalars = scalars.clone();
        scalars.gc_watermark = scalars.gc_watermark.max(self.image.hard_state.gc_watermark);
        self.stage(MatchRecord::Scalars(scalars));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(generation = scalars.generation.0))]
    async fn install_registry(
        &mut self,
        scalars: &MatchmakerHardState,
        registrations: &BTreeMap<Ballot, Registration>,
    ) -> Result<(), StorageError> {
        self.opened().await?;
        self.stage(MatchRecord::Install(scalars.clone()));
        for (ballot, registration) in registrations.range(scalars.gc_watermark..) {
            self.stage(MatchRecord::Register {
                ballot: *ballot,
                registration: registration.clone(),
            });
        }
        self.stage(MatchRecord::End);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(dir = %self.dir))]
    async fn sync(&mut self) -> Result<(), StorageError> {
        self.opened().await?;
        if !self.staged.is_empty() {
            let records = std::mem::take(&mut self.staged);
            self.append(&records).await?;
            self.maybe_checkpoint().await?;
        }
        Ok(())
    }
}
