//! [`JournalStorage`]: the node's [`NodeStorage`] on `moonpool-journal`.

use moonpool_core::StorageProvider;
use moonpool_journal::{Journal, Record, Recovery};
use paros_core::{Ballot, Command, Config, HardState, MustSync, SessionEntry, Slot, Storage};
use serde::{Deserialize, Serialize};

use super::frame::{Framed, Scanned, encode, epoch};
use super::node_image::{NodeImage, NodeRecord, SnapPoint};
use super::plan::plan;
use super::{GENESIS, JournalStoreConfig, append_error, meta_error, open_error};
use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::storage::{NodeStorage, StorageError, StorageRecord};

/// The scalars the journal's two-copy metadata holds: the ones whose loss
/// no peer can repair.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct NodeMeta {
    /// The format marker (#147): set once, never cleared.
    formatted: bool,
    /// The promised ballot.
    promise: Ballot,
}

/// Version byte in front of the metadata's encoding.
const META_VERSION: u8 = 1;

impl NodeMeta {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![META_VERSION];
        bytes.extend(postcard::to_stdvec(self).expect("in-memory encoding of the metadata"));
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let (&version, body) = bytes.split_first()?;
        (version == META_VERSION)
            .then(|| postcard::from_bytes(body).ok())
            .flatten()
    }
}

/// The node's durable store on `moonpool-journal`, over any moonpool
/// [`StorageProvider`] — Tokio's filesystem in production, the simulator's
/// disk under test. See the [module docs](super) for the mapping of every
/// durable fact onto the journal and the corruption table.
///
/// The store opens its journal in [`boot_scan`](NodeStorage::boot_scan),
/// which is also where it loads: until then every accessor answers for an
/// empty store (the driver reads only the configuration first). A write
/// before the boot scan runs it.
///
/// Like [`MemStorage`](crate::MemStorage) it carries **no application**: its
/// [`apply`](NodeStorage::apply) is a no-op and its
/// [`snapshot`](NodeStorage::snapshot) is the chosen prefix's marker. An
/// application persists its own state; this store persists paros's.
pub struct JournalStorage<P: StorageProvider> {
    provider: P,
    dir: String,
    store: JournalStoreConfig,
    config: Config,
    pub(super) journal: Option<Journal<P>>,
    meta: NodeMeta,
    meta_dirty: bool,
    pub(super) image: NodeImage,
    /// Records applied to the image and not yet appended.
    staged: Vec<NodeRecord>,
}

impl<P: StorageProvider> std::fmt::Debug for JournalStorage<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalStorage")
            .field("dir", &self.dir)
            .field("node", &self.config.id.0)
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}

impl<P: StorageProvider> JournalStorage<P> {
    /// A store for the node `config` names, kept under `dir` on `provider`.
    /// Nothing is read until the boot scan.
    #[must_use]
    pub fn new(
        provider: P,
        dir: impl Into<String>,
        config: Config,
        store: JournalStoreConfig,
    ) -> Self {
        Self {
            provider,
            dir: dir.into(),
            store,
            config,
            journal: None,
            meta: NodeMeta::default(),
            meta_dirty: false,
            image: NodeImage::default(),
            staged: Vec::new(),
        }
    }

    /// The directory the journal lives in.
    #[must_use]
    pub fn dir(&self) -> &str {
        &self.dir
    }

    /// Stage one write: fold it into the image now, append it at the next
    /// sync.
    fn stage(&mut self, record: NodeRecord) {
        self.image.apply(&record);
        self.staged.push(record);
    }

    /// Open and load the journal if the boot scan has not yet.
    async fn opened(&mut self) -> Result<(), StorageError> {
        if self.journal.is_none() {
            self.load().await?;
        }
        Ok(())
    }

    /// Open the journal, replay it into the image, and settle what the
    /// replay found. The body of [`NodeStorage::boot_scan`].
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn load(&mut self) -> Result<(), StorageError> {
        let (mut journal, recovery) =
            Journal::open(self.provider.clone(), &self.dir, self.store.journal())
                .await
                .map_err(|e| open_error(&e))?;
        report(self.config.id.0, &recovery);
        self.meta = match journal.meta() {
            None => NodeMeta::default(),
            Some(bytes) => NodeMeta::decode(bytes).ok_or(StorageError::Corruption {
                record: StorageRecord::Promise,
                fault: IntegrityFault::Misdirected,
                verdict: CorruptionVerdict::Corrupted,
            })?,
        };
        let entries = journal
            .read_range(journal.start_index()..journal.next_index())
            .await
            .map_err(|e| open_error(&e))?;
        let mut scanned: Vec<Scanned<NodeRecord>> = entries.into_iter().map(Scanned::new).collect();
        let plan = plan(&scanned, journal.start_index() == GENESIS);
        if plan.lost {
            return Err(StorageError::Corruption {
                record: StorageRecord::Truncation,
                fault: IntegrityFault::LostWrite,
                verdict: CorruptionVerdict::Corrupted,
            });
        }
        if let Some(cut) = plan.cut_at {
            let from = scanned[cut].id.index;
            tracing::info!(node = self.config.id.0, from, "journal_open_checkpoint_cut");
            journal
                .truncate_suffix(from)
                .await
                .map_err(|e| append_error(&e))?;
            scanned.truncate(cut);
        }
        let mut image = NodeImage::default();
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
            if let Some(record) = &entry.record {
                image.apply(record);
            } else {
                let strict = plan
                    .strict
                    .as_ref()
                    .is_some_and(|range| range.contains(&at));
                image.apply_damaged(&entry.id, entry.kind, strict)?;
            }
            at += 1;
        }
        image.finish();
        if !image.faulty.is_empty() {
            tracing::warn!(
                node = self.config.id.0,
                slots = image.faulty.len() as u64,
                "faulty_entry_reported"
            );
        }
        self.image = image;
        self.staged.clear();
        self.meta_dirty = false;
        self.journal = Some(journal);
        Ok(())
    }

    /// Append the staged records as one batch.
    async fn append_staged(&mut self) -> Result<(), StorageError> {
        let records = std::mem::take(&mut self.staged);
        self.append(&records).await
    }

    async fn append(&mut self, records: &[NodeRecord]) -> Result<(), StorageError> {
        let journal = self.journal.as_mut().expect("opened before appending");
        let payloads: Vec<Vec<u8>> = records.iter().map(encode).collect();
        let framed: Vec<Record<'_>> = records
            .iter()
            .zip(&payloads)
            .map(|(record, payload)| {
                Record::new(epoch(record.kind()), payload).with_tag(record.tag())
            })
            .collect();
        journal
            .append(&framed)
            .await
            .map_err(|e| append_error(&e))?;
        Ok(())
    }

    /// Checkpoint once the live log is long enough: the image as one
    /// bracketed batch, then the whole segments before it dropped.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn maybe_checkpoint(&mut self) -> Result<(), StorageError> {
        let journal = self.journal.as_ref().expect("opened before a checkpoint");
        let live = journal.next_index() - journal.start_index();
        if live < self.store.checkpoint_after.max(1) || !self.staged.is_empty() {
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
        tracing::debug!(
            node = self.config.id.0,
            begin,
            start = journal.start_index(),
            "journal_checkpointed"
        );
        Ok(())
    }
}

/// Trace what opening the journal repaired on its own.
fn report(node: u64, recovery: &Recovery) {
    if recovery.torn_tail {
        tracing::info!(node, "journal_torn_tail_discarded");
    }
    if !recovery.ambiguous_batch.is_empty() {
        tracing::warn!(
            node,
            entries = recovery.ambiguous_batch.len() as u64,
            "journal_ambiguous_batch_kept"
        );
    }
    if recovery.slots_rewritten + recovery.headers_repaired > 0 || recovery.meta_repaired {
        tracing::info!(
            node,
            slots = recovery.slots_rewritten as u64,
            headers = recovery.headers_repaired as u64,
            meta = recovery.meta_repaired,
            "journal_identifiers_repaired"
        );
    }
}

impl<P: StorageProvider> Storage for JournalStorage<P> {
    fn initial_state(&self) -> (HardState, Config) {
        let mut hard_state = HardState::default();
        hard_state.max_promised_ballot = self.meta.promise;
        hard_state.chosen_index = self.image.chosen_index;
        (hard_state, self.config.clone())
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.image.accepted.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        self.image.first
    }

    fn last_slot(&self) -> Slot {
        self.image
            .accepted
            .keys()
            .next_back()
            .copied()
            .unwrap_or(Slot(0))
    }

    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        self.image
            .sealed
            .iter()
            .map(|(&(client, seq), &slot)| (client, seq, slot))
            .collect()
    }

    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.image
            .faulty
            .iter()
            .map(|(slot, ballot)| (*slot, *ballot))
            .collect()
    }
}

impl<P: StorageProvider> NodeStorage for JournalStorage<P> {
    /// Open the journal and fold it into the image (see the [module
    /// docs](super) for the per-kind corruption table). A clean store, a
    /// store with faulty entries or chunks (reported through the read
    /// ports), and a store whose tail a crash tore all boot; a store the
    /// journal cannot open, a trusted checkpoint that lost its header or
    /// ledger are crash verdicts.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.journal = None;
        self.load().await
    }

    fn is_formatted(&self) -> bool {
        self.meta.formatted
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn format(&mut self) -> Result<(), StorageError> {
        self.opened().await?;
        self.meta.formatted = true;
        self.meta_dirty = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, round = ballot.round))]
    async fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.opened().await?;
        self.meta.promise = ballot;
        self.meta_dirty = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0))]
    async fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        self.opened().await?;
        self.stage(NodeRecord::Accepted {
            slot,
            ballot,
            command,
        });
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0))]
    async fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.opened().await?;
        self.stage(NodeRecord::ChosenIndex(slot));
        Ok(())
    }

    /// The metadata first, then the log: the promise is durable before any
    /// record it covers. A [`MustSync::Relaxed`] batch holding nothing but
    /// chosen-index records is deferred to the next flush — the relaxed
    /// contract lets a crash lose it.
    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0))]
    async fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        self.opened().await?;
        if self.meta_dirty {
            let bytes = self.meta.encode();
            let journal = self.journal.as_mut().expect("opened");
            journal
                .save_meta(&bytes)
                .await
                .map_err(|e| meta_error(&e))?;
            self.meta_dirty = false;
        }
        let relaxed_only = must_sync == MustSync::Relaxed
            && self
                .staged
                .iter()
                .all(|record| matches!(record, NodeRecord::ChosenIndex(_)));
        if !self.staged.is_empty() && !relaxed_only {
            self.append_staged().await?;
            self.maybe_checkpoint().await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, first = first.0, sealed = sealed.len()))]
    async fn truncate(&mut self, first: Slot, sealed: &[SessionEntry]) -> Result<(), StorageError> {
        self.opened().await?;
        self.stage(NodeRecord::Truncate {
            first,
            sealed: sealed.to_vec(),
        });
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0))]
    async fn snapshot(&self) -> Vec<u8> {
        self.image
            .chosen_index
            .map_or_else(Vec::new, |chosen| chosen.0.to_le_bytes().to_vec())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, at = at.0))]
    async fn record_snapshot(&mut self, at: Slot) -> Result<(), StorageError> {
        self.opened().await?;
        let point = SnapPoint::of(at, &self.snapshot().await);
        self.stage(NodeRecord::SnapPoint {
            at,
            len: point.len,
            crcs: point.crcs.clone(),
        });
        for (index, bytes) in point.chunks.iter().enumerate() {
            let chunk = u32::try_from(index).expect("chunk counts fit 32 bits");
            let bytes = bytes.clone().expect("a fresh point holds every chunk");
            self.stage(NodeRecord::SnapChunk { at, chunk, bytes });
        }
        Ok(())
    }

    fn latest_snap_point(&self) -> Option<Slot> {
        self.image.point.as_ref().map(|point| point.at)
    }

    fn snap_chunk_count(&self, at: Slot) -> Option<u32> {
        self.image
            .point
            .as_ref()
            .filter(|point| point.at == at)
            .map(|point| u32::try_from(point.chunks.len()).unwrap_or(u32::MAX))
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, at = at.0, chunk))]
    async fn read_snap_chunk(&self, at: Slot, chunk: u32) -> Option<Vec<u8>> {
        let point = self.image.point.as_ref().filter(|point| point.at == at)?;
        point.chunks.get(usize::try_from(chunk).ok()?)?.clone()
    }

    /// A repaired chunk is verified against the checksum the point's header
    /// recorded before it is staged: a chunk that does not match is refused
    /// (`Ok(false)`), never persisted.
    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, at = at.0, chunk))]
    async fn write_snap_chunk(
        &mut self,
        at: Slot,
        chunk: u32,
        bytes: &[u8],
    ) -> Result<bool, StorageError> {
        self.opened().await?;
        let Some(point) = self.image.point.as_ref().filter(|point| point.at == at) else {
            return Ok(false);
        };
        if !point.verifies(chunk, bytes) {
            return Ok(false);
        }
        self.stage(NodeRecord::SnapChunk {
            at,
            chunk,
            bytes: bytes.to_vec(),
        });
        Ok(self
            .image
            .point
            .as_ref()
            .is_some_and(|point| point.lost().next().is_none()))
    }

    fn faulty_snap_chunks(&self) -> Vec<(Slot, u32)> {
        self.image
            .point
            .as_ref()
            .map(|point| point.lost().map(|chunk| (point.at, chunk)).collect())
            .unwrap_or_default()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, chosen = chosen_index.0))]
    async fn install_snapshot(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        _snapshot: Vec<u8>,
        sessions: &[SessionEntry],
    ) -> Result<(), StorageError> {
        self.opened().await?;
        if ballot > self.meta.promise {
            self.meta.promise = ballot;
            self.meta_dirty = true;
        }
        self.stage(NodeRecord::InstallSnapshot {
            chosen_index,
            sessions: sessions.to_vec(),
        });
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0))]
    async fn apply(
        &mut self,
        _chosen_index: Slot,
        _slot: Slot,
        _command: &Command,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn applied_slot(&self) -> Option<Slot> {
        self.image.chosen_index
    }
}
