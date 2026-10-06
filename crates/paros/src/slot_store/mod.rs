//! [`SlotStorage`]: the acceptor's durable store — payload separation plus a
//! slot-indexed metadata array, owned by paros (decided on 2026-10-06,
//! `docs/architecture.md` §5).
//!
//! An acceptor's state is a sparse map keyed by slot: votes arrive out of
//! order (pipelining) and are overwritten at higher ballots, and the
//! Paxos-decided `Truncate` floor drops a prefix. A dense append-only log
//! cannot write at a chosen index, so the store before this one logged
//! *operations*, folded them at boot, and compacted by copying the whole
//! image — tenants' bytes included — forward. This store holds the state
//! itself, and takes no checkpoint: space is freed only behind the floor.
//!
//! # On disk
//!
//! | File | Holds |
//! |---|---|
//! | `meta.0`, `meta.1` | the **metainfo**: the format marker and its [`Config`], the promise, the floor, the sealed journal state, a trim jump's chosen index, the layout; two copies ([`dual`]) |
//! | `slots-<k>.dat` | the **slot records** of slots `k·C .. (k+1)·C` (`C` = [`SlotStoreConfig::chunk_slots`]): two copies per slot, each in a block of its own, copy `c` of slot `k·C + i` at block `c·C + i`; created whole with a reserved record in every copy, then renamed into place |
//! | `payload-<n>.dat` | the **payloads**, in arrival order: each sync appends one batch, a head then one frame per vote, starting on a fresh block |
//!
//! A slot record is `(slot, generation, ballot, batch, segment, offset,
//! len, crc)` plus its own CRC: CLSTORE's identifier, physically apart from
//! the item it names (CTRL §3.3.4), and its persist record (§3.3.3). The
//! frame in front of each payload repeats `(slot, generation, ballot,
//! batch)` — the redundant identifier — so a vote whose slot record was lost
//! is still named. Every record carries its own position (slot and copy, or
//! segment and offset) inside its CRC, so a misdirected read or write
//! decodes as damage ([`layout`]).
//!
//! # The write, and why a torn one never destroys a vote
//!
//! A sync writes, in this order: the metainfo if it changed (temp, sync,
//! rename, directory sync — the promise is durable before any vote it
//! covers); the batch to the active payload segment, then its sync; then
//! each slot's record into **the copy that does not hold the slot's newest
//! generation**, then the chunks' syncs. Each write spends a fresh
//! generation, one above any the slot ever showed on disk, before it is
//! written (moonpool#304's lesson): two valid copies of one generation that
//! disagree are damage, never a choice. So the newest record of a slot is
//! never rewritten in place: a crash tears at most the older copy — the one
//! being replaced — and the previous accepted record survives. A payload is
//! never rewritten either: segments only grow, and every boot appends into a
//! fresh segment.
//!
//! # Detection: CLSTORE's rule, per slot
//!
//! For each slot at or above the floor, the boot takes the newest
//! generation any intact evidence names — a slot record copy or a frame
//! header — and reads its payload ([`boot::fold_slot`]):
//!
//! | Evidence at the newest generation | Verdict |
//! |---|---|
//! | payload checks out | the vote |
//! | a slot record, payload lost | **corruption**: the record is written only after the payload's sync, so the payload was durable — `faulty(slot, ballot)` from the record |
//! | a frame alone, payload lost, its batch older than the newest batch on disk | **corruption**: a later batch proves this one was synced whole, so its record was lost too — `faulty(slot, ballot)` from the frame |
//! | a frame alone, payload lost, in the newest batch | **a crash during the write** (CTRL §3.3.3: no persist record): never acknowledged, discarded, and the generation before it decides |
//! | a frame alone, payload intact, in the newest batch | the CTRL undecidable row (Thm A.1): the payload landed, the record did not. The vote is **kept** — an acceptor may hold an accept it never acknowledged |
//! | two copies, or a copy and a frame, of one generation that disagree | crash verdict: only damage produces it |
//!
//! A faulty slot answers "unknown", never "empty": it is reported through
//! [`Storage::faulty_entries`] and the acceptor abstains in Phase 1 for it
//! until it is repaired from peers (`docs/analysis/storage/ctrl-multipaxos-restatement.md`).
//! It stays faulty across boots, because the disk keeps saying so, until a
//! new accept writes a newer generation. A slot record of generation zero is
//! the reserved record: positively nothing accepted. All zeros is damage.
//!
//! **The ambiguous region** after a crash is the batch in flight: its
//! frames without their records. The store has no bound of its own on it;
//! the core admits every write into a fresh slot, and nothing in paros
//! bounds the pipeline (an open question on the store's issue).
//!
//! # Space
//!
//! The floor rises only with a Paxos-decided `Truncate` or a trim jump —
//! CTRL's agreed snapshot index, without the snapshot (§3.5.1). Behind it a
//! sync deletes every chunk wholly below the floor, and every payload
//! segment (but the active one) none of whose frames is live: each frame is
//! below the floor or superseded by a later generation of its slot. A live
//! vote pins its segment only until it is re-accepted or truncated, so no
//! compaction is ever needed. A tenant that never truncates keeps its disk.
//!
//! # The scalars
//!
//! The promise, the floor, the sealed journal state, a trim jump's chosen
//! index, the format marker with its [`Config`] and the layout live in the
//! metainfo. The chosen index of every sync rides its batch head; a boot
//! takes the newest, never below a trim jump's or the floor's.

mod boot;
mod dual;
mod layout;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use moonpool_core::{BlockFile, DirectIo, OpenOptions, StorageFile, StorageProvider};
use paros_core::{Ballot, Command, Config, HardState, JournalState, MustSync, Slot, Storage};
use serde::{Deserialize, Serialize};

use self::boot::{Outcome, Seen, fold_slot, scan_segment};
use self::dual::{Dual, DualError};
use self::layout::{
    Copy, FRAME_LEN, Frame, Head, Loc, SlotRecord, decode_copy, encode_frame, encode_head,
    encode_record, encode_reserved,
};
use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::storage::{LogStorage, MetadataFault, StorageError, StorageRecord, WriteOutcome};

/// The smallest block the store accepts: the sector.
const MIN_BLOCK: usize = 512;

/// Version byte in front of the metainfo's encoding.
const META_VERSION: u8 = 1;
/// Version byte in front of every payload's encoding.
const PAYLOAD_VERSION: u8 = 1;

/// How a [`SlotStorage`] lays out its files.
///
/// `chunk_slots` and `block` are the addresses of every slot record, so
/// they are recorded in the metainfo and must match on every open (a
/// mismatch is a [`MetadataFault::WrongSize`]); `segment_size` and
/// `direct_io` may change across restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotStoreConfig {
    /// Slots per chunk file: the window a chunk covers, aligned on
    /// multiples of itself. Floor 1.
    pub chunk_slots: u32,
    /// The write unit: one slot record copy per block, and every payload
    /// batch starts on a block boundary. It must be the device's atomic
    /// write unit or a multiple of it, or a torn write could reach both
    /// copies of a slot. A power of two, at least 512.
    pub block: usize,
    /// A payload segment rolls over once a batch would grow it past this
    /// many bytes (a batch larger than that gets a segment of its own).
    /// Floor: one block.
    pub segment_size: u64,
    /// Direct-I/O policy for the chunk and payload files.
    pub direct_io: DirectIo,
}

impl Default for SlotStoreConfig {
    /// The production layout: 4 KiB blocks (the direct-I/O and atomic-write
    /// unit), 1,024-slot chunks (8 MiB each, two copies per slot) and 64 MiB
    /// payload segments.
    fn default() -> Self {
        let config = Self {
            chunk_slots: 1024,
            block: 4096,
            segment_size: 64 * 1024 * 1024,
            direct_io: DirectIo::Optional,
        };
        config.assert_valid();
        config
    }
}

impl SlotStoreConfig {
    /// A small layout for simulation and tests: the simulator's 512-byte
    /// sector as the block, 64-slot chunks and 64 KiB segments, so chunk
    /// drops and segment rollover happen within a short run.
    #[must_use]
    pub fn small() -> Self {
        let config = Self {
            chunk_slots: 64,
            block: MIN_BLOCK,
            segment_size: 64 * 1024,
            direct_io: DirectIo::Disabled,
        };
        config.assert_valid();
        config
    }

    fn assert_valid(self) {
        assert!(self.chunk_slots >= 1, "a chunk holds a slot");
        assert!(self.block >= MIN_BLOCK, "a block is at least a sector");
        assert!(self.block.is_power_of_two(), "a block is a power of two");
        assert!(
            self.segment_size >= self.block as u64,
            "a segment holds a block"
        );
    }

    fn chunk_of(self, slot: Slot) -> u64 {
        slot.0 / u64::from(self.chunk_slots)
    }

    /// The block holding `copy` of `slot` in its chunk.
    fn block_of(self, slot: Slot, copy: u8) -> u64 {
        let chunk = u64::from(self.chunk_slots);
        let index = u64::from(copy) * chunk + slot.0 % chunk;
        assert!(index < 2 * chunk, "a copy lies inside its chunk");
        index
    }
}

/// The metainfo's content (see the module doc).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Meta {
    chunk_slots: u32,
    block: u64,
    /// The format marker (#147) and its configuration (#207): set once,
    /// never cleared or edited.
    formatted: Option<Config>,
    promise: Ballot,
    floor: Slot,
    sealed: JournalState,
    /// The chosen index a trim jump implied (#186).
    trimmed_chosen: Option<Slot>,
}

impl Meta {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![META_VERSION];
        bytes.extend(postcard::to_stdvec(self).expect("in-memory encoding of the metainfo"));
        // Pair of `decode`: the metainfo a sync saves is what a boot reads.
        assert!(
            Self::decode(&bytes).as_ref() == Some(self),
            "saved metainfo decodes back to itself"
        );
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let (&version, body) = bytes.split_first()?;
        (version == META_VERSION)
            .then(|| postcard::from_bytes(body).ok())
            .flatten()
    }
}

fn encode_payload(command: &Command) -> Vec<u8> {
    let mut bytes = vec![PAYLOAD_VERSION];
    bytes.extend(postcard::to_stdvec(command).expect("in-memory encoding of a command"));
    assert!(
        decode_payload(&bytes).as_ref() == Some(command),
        "a payload decodes back to its command"
    );
    bytes
}

fn decode_payload(bytes: &[u8]) -> Option<Command> {
    let (&version, body) = bytes.split_first()?;
    (version == PAYLOAD_VERSION)
        .then(|| postcard::from_bytes(body).ok())
        .flatten()
}

/// What the store knows of one slot's place on disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SlotMeta {
    /// The generation each copy holds (`Some(0)`: reserved; `None`: bad or
    /// never formatted).
    copies: [Option<u64>; 2],
    /// The highest generation spent or seen for the slot.
    generation: u64,
    /// The payload the slot's vote is served from, if any: it pins its
    /// segment.
    live: Option<Loc>,
}

impl SlotMeta {
    /// The copy the next write goes to: the one not holding the newest
    /// generation.
    fn target(&self) -> u8 {
        let rank = |copy: Option<u64>| copy.map_or(0, |g| g + 1);
        let target = u8::from(rank(self.copies[1]) < rank(self.copies[0]));
        // Never the copy holding the newest record.
        let newest = self.copies.iter().flatten().max().copied();
        assert!(
            newest.is_none()
                || newest == Some(0)
                || self.copies[usize::from(target)] != newest,
            "a write never replaces the newest record"
        );
        target
    }
}

/// What a [`SlotStorage`]'s last boot found — observation for a harness's
/// reach gates, never a decision: the store boots the same way whatever
/// these say.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlotBootFacts {
    /// The store booted with a raised floor: a truncated prefix.
    pub truncated_prefix: bool,
    /// Votes whose payload landed without their slot record (a crash after
    /// the payload sync), kept.
    pub ambiguous_kept: usize,
    /// Torn writes a crash cut before they were persisted, discarded.
    pub torn_discarded: usize,
    /// Slots reported faulty.
    pub faulty: usize,
}

/// The segment being appended to.
struct Active<F> {
    segment: u64,
    file: BlockFile<F>,
    /// Where the next batch starts: a block boundary.
    offset: u64,
}

/// The acceptor's durable store (see the [module docs](self)), over any
/// moonpool [`StorageProvider`] — Tokio's filesystem in production, the
/// simulator's disk under test.
///
/// The store opens in [`boot_scan`](LogStorage::boot_scan), which is also
/// where it loads: until then every accessor answers for an empty store (the
/// driver reads only the configuration first). A write before the boot scan
/// runs it.
pub struct SlotStorage<P: StorageProvider> {
    provider: P,
    dir: String,
    store: SlotStoreConfig,
    config: Config,
    opened: bool,
    /// A sync failed part-way: the disk holds an unknown mix, and only a
    /// boot may read it again.
    poisoned: bool,
    dual: Option<Dual>,
    meta: Meta,
    meta_dirty: bool,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    faulty: BTreeMap<Slot, Ballot>,
    chosen_index: Option<Slot>,
    chosen_dirty: bool,
    slots: BTreeMap<Slot, SlotMeta>,
    /// Live frames per payload segment on disk.
    live: BTreeMap<u64, usize>,
    chunks: BTreeMap<u64, Option<BlockFile<P::File>>>,
    active: Option<Active<P::File>>,
    next_segment: u64,
    next_batch: u64,
    /// Votes staged since the last sync, the newest per slot.
    staged: BTreeMap<Slot, (Ballot, Command)>,
    boot_facts: SlotBootFacts,
}

impl<P: StorageProvider> std::fmt::Debug for SlotStorage<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotStorage")
            .field("dir", &self.dir)
            .field("node", &self.config.id.0)
            .field("floor", &self.meta.floor)
            .field("slots", &self.slots.len())
            .finish_non_exhaustive()
    }
}

fn read_fault(record: StorageRecord) -> StorageError {
    StorageError::Corruption {
        record,
        fault: IntegrityFault::ReadError,
        verdict: CorruptionVerdict::Corrupted,
    }
}

fn write_fault() -> StorageError {
    StorageError::FsyncFailed {
        record: StorageRecord::Batch,
        outcome: WriteOutcome::Unknown,
    }
}

fn chunk_path(dir: &str, chunk: u64) -> String {
    format!("{dir}/slots-{chunk:020}.dat")
}

fn segment_path(dir: &str, segment: u64) -> String {
    format!("{dir}/payload-{segment:020}.dat")
}

/// The number a file name carries, for a `prefix-<n>.dat` name.
fn numbered(name: &str, prefix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?
        .strip_suffix(".dat")?
        .parse()
        .ok()
}

/// Every directory whose entries name a component of `dir`, from the root
/// down: syncing them makes the chain of names reaching `dir` durable.
fn ancestors_of(dir: &str) -> Vec<String> {
    let absolute = dir.starts_with('/');
    let mut current = if absolute { "/" } else { "." }.to_string();
    let mut parents = Vec::new();
    for component in dir.split('/').filter(|p| !p.is_empty() && *p != ".") {
        parents.push(current.clone());
        current = match current.as_str() {
            "." => component.to_string(),
            "/" => format!("/{component}"),
            _ => format!("{current}/{component}"),
        };
    }
    parents
}

impl<P: StorageProvider> SlotStorage<P> {
    /// A store for the node `config` names, kept under `dir` on `provider`.
    /// Nothing is read until the boot scan.
    #[must_use]
    pub fn new(provider: P, dir: impl Into<String>, config: Config, store: SlotStoreConfig) -> Self {
        store.assert_valid();
        Self {
            provider,
            dir: dir.into(),
            store,
            config,
            opened: false,
            poisoned: false,
            dual: None,
            meta: Meta::default(),
            meta_dirty: false,
            accepted: BTreeMap::new(),
            faulty: BTreeMap::new(),
            chosen_index: None,
            chosen_dirty: false,
            slots: BTreeMap::new(),
            live: BTreeMap::new(),
            chunks: BTreeMap::new(),
            active: None,
            next_segment: 1,
            next_batch: 1,
            staged: BTreeMap::new(),
            boot_facts: SlotBootFacts::default(),
        }
    }

    /// Whether the store under `dir` carries its format marker, read from
    /// the metainfo alone: no repair, nothing created, so a probe never
    /// changes what the next boot finds. `false` where no store exists.
    ///
    /// # Errors
    ///
    /// A [`StorageError::Corruption`] when no metainfo copy is valid or the
    /// newest does not decode, and the read verdict when the namespace
    /// cannot be read.
    pub async fn peek_formatted(provider: &P, dir: &str) -> Result<bool, StorageError> {
        if !provider
            .exists(dir)
            .await
            .map_err(|_| read_fault(StorageRecord::Store))?
        {
            return Ok(false);
        }
        let bytes = Dual::peek(provider, dir).await.map_err(|e| meta_load_error(&e))?;
        let Some(bytes) = bytes else {
            return Ok(false);
        };
        let meta = Meta::decode(&bytes).ok_or(misdirected_meta())?;
        Ok(meta.formatted.is_some())
    }

    /// What the last boot scan found ([`SlotBootFacts`]).
    #[must_use]
    pub fn boot_facts(&self) -> SlotBootFacts {
        self.boot_facts
    }

    /// The directory the store lives in.
    #[must_use]
    pub fn dir(&self) -> &str {
        &self.dir
    }

    /// The layout the store runs with.
    #[must_use]
    pub fn layout(&self) -> SlotStoreConfig {
        self.store
    }

    fn reset(&mut self) {
        *self = Self::new(
            self.provider.clone(),
            std::mem::take(&mut self.dir),
            self.config.clone(),
            self.store,
        );
    }

    /// Open and load the store if the boot scan has not yet.
    async fn opened(&mut self) -> Result<(), StorageError> {
        if !self.opened {
            self.load().await?;
        }
        if self.poisoned {
            return Err(write_fault());
        }
        Ok(())
    }

    /// Make the directory and every name leading to it durable.
    async fn durable_dir(&self) -> std::io::Result<()> {
        self.provider.create_dir_all(&self.dir).await?;
        for parent in ancestors_of(&self.dir) {
            self.provider.sync_dir(&parent).await?;
        }
        self.provider.sync_dir(&self.dir).await
    }

    /// Read a whole file as blocks (a trailing partial block is never ours:
    /// every write is whole blocks).
    async fn read_file(&self, path: &str) -> std::io::Result<Vec<u8>> {
        let file = self
            .provider
            .open(path, OpenOptions::read_only().direct_io(self.store.direct_io))
            .await?;
        let block = self.store.block as u64;
        let size = file.size().await?;
        let count = usize::try_from(size / block)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "file too large"))?;
        if count == 0 {
            return Ok(Vec::new());
        }
        let blocks = BlockFile::new(file, self.store.block)?;
        let mut buf = blocks.buffer(count)?;
        blocks.read_blocks(0, buf.as_mut_slice()).await?;
        Ok(buf.as_slice().to_vec())
    }

    /// The body of [`LogStorage::boot_scan`] (the module doc's table).
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn load(&mut self) -> Result<(), StorageError> {
        self.reset();
        self.durable_dir()
            .await
            .map_err(|_| read_fault(StorageRecord::Store))?;
        let (dual, bytes, repaired) = Dual::load(&self.provider, &self.dir)
            .await
            .map_err(|e| meta_load_error(&e))?;
        if repaired {
            tracing::warn!(node = self.config.id.0, "slot_store_meta_repaired");
        }
        let meta = match bytes {
            None => Meta {
                chunk_slots: self.store.chunk_slots,
                block: self.store.block as u64,
                ..Meta::default()
            },
            Some(bytes) => Meta::decode(&bytes).ok_or(misdirected_meta())?,
        };
        if meta.chunk_slots != self.store.chunk_slots || meta.block != self.store.block as u64 {
            return Err(StorageError::Metadata {
                fault: MetadataFault::WrongSize,
            });
        }
        let names = self
            .provider
            .list_dir(&self.dir)
            .await
            .map_err(|_| read_fault(StorageRecord::Store))?;
        let chunk_names: BTreeSet<u64> = names.iter().filter_map(|n| numbered(n, "slots-")).collect();
        let segment_names: BTreeSet<u64> =
            names.iter().filter_map(|n| numbered(n, "payload-")).collect();

        // The payloads, and every head and frame they carry.
        let mut segments = BTreeMap::new();
        let mut heads: Vec<Head> = Vec::new();
        let mut frames: BTreeMap<Slot, Vec<Seen>> = BTreeMap::new();
        for &segment in &segment_names {
            let bytes = self
                .read_file(&segment_path(&self.dir, segment))
                .await
                .map_err(|_| read_fault(StorageRecord::Store))?;
            let scan = scan_segment(segment, &bytes, self.store.block);
            heads.extend(scan.heads);
            for seen in scan.frames {
                if seen.frame.slot >= meta.floor {
                    frames.entry(seen.frame.slot).or_default().push(seen);
                }
            }
            segments.insert(segment, bytes);
        }

        // The slot records of every chunk that reaches the floor.
        let mut copies: BTreeMap<Slot, [Copy; 2]> = BTreeMap::new();
        let chunk_len = 2 * u64::from(self.store.chunk_slots) * self.store.block as u64;
        for &chunk in &chunk_names {
            // Known whether or not it reaches the floor: a chunk wholly below
            // it is deleted at the next sync.
            self.chunks.insert(chunk, None);
            let first = chunk * u64::from(self.store.chunk_slots);
            if first + u64::from(self.store.chunk_slots) <= meta.floor.0 {
                continue;
            }
            let bytes = self
                .read_file(&chunk_path(&self.dir, chunk))
                .await
                .map_err(|_| read_fault(StorageRecord::Store))?;
            if bytes.len() as u64 != chunk_len {
                return Err(StorageError::Metadata {
                    fault: MetadataFault::WrongSize,
                });
            }
            for i in 0..u64::from(self.store.chunk_slots) {
                let slot = Slot(first + i);
                if slot < meta.floor {
                    continue;
                }
                let read = |copy: u8| {
                    let at = usize::try_from(self.store.block_of(slot, copy)).expect("in memory")
                        * self.store.block;
                    decode_copy(&bytes[at..at + self.store.block], slot, copy)
                };
                copies.insert(slot, [read(0), read(1)]);
            }
        }

        let max_batch = heads
            .iter()
            .map(|h| h.batch)
            .chain(frames.values().flatten().map(|s| s.frame.batch))
            .chain(copies.values().flatten().filter_map(|c| match c {
                Copy::Persisted(r) => Some(r.batch),
                _ => None,
            }))
            .max()
            .unwrap_or(0);

        // Every slot any evidence names.
        let named: BTreeSet<Slot> = copies.keys().chain(frames.keys()).copied().collect();
        let mut facts = SlotBootFacts {
            truncated_prefix: meta.floor > Slot(0),
            ..SlotBootFacts::default()
        };
        for slot in named {
            let pair = copies.get(&slot).copied().unwrap_or([Copy::Bad, Copy::Bad]);
            let seen = frames.get(&slot).map_or(&[][..], Vec::as_slice);
            let folded = fold_slot(slot, pair, seen, &segments, max_batch, decode_payload)?;
            facts.torn_discarded += folded.torn;
            let mut slot_meta = SlotMeta {
                copies: if copies.contains_key(&slot) {
                    folded.copies
                } else {
                    [None, None]
                },
                generation: folded.generation,
                live: None,
            };
            match folded.outcome {
                Outcome::Accepted {
                    ballot,
                    command,
                    loc,
                    unrecorded,
                } => {
                    if unrecorded {
                        facts.ambiguous_kept += 1;
                    }
                    slot_meta.live = Some(loc);
                    *self.live.entry(loc.segment).or_default() += 1;
                    self.accepted.insert(slot, (ballot, command));
                }
                Outcome::Faulty { ballot } => {
                    facts.faulty += 1;
                    tracing::warn!(slot = slot.0, round = ballot.round, "slot_store_faulty");
                    self.faulty.insert(slot, ballot);
                }
                Outcome::Empty => {}
            }
            self.slots.insert(slot, slot_meta);
        }
        for &segment in &segment_names {
            self.live.entry(segment).or_default();
        }

        // The chosen index: the newest batch's, never below a trim jump's or
        // the floor's.
        let below_floor = meta.floor.0.checked_sub(1).map(Slot);
        self.chosen_index = heads
            .iter()
            .filter_map(|h| h.chosen)
            .chain(meta.trimmed_chosen)
            .chain(below_floor)
            .max();
        self.next_batch = max_batch + 1;
        self.next_segment = segment_names.last().map_or(1, |n| n + 1);
        self.meta = meta;
        self.dual = Some(dual);
        self.boot_facts = facts;
        self.opened = true;

        // Boot side of the write pairs: nothing below the floor, a slot is
        // accepted or faulty never both, and the chosen index covers the
        // truncated prefix.
        self.assert_invariants();
        if let Some(below) = below_floor {
            assert!(
                self.chosen_index >= Some(below),
                "the chosen index covers the truncated prefix"
            );
        }
        tracing::debug!(
            node = self.config.id.0,
            slots = self.slots.len() as u64,
            faulty = facts.faulty as u64,
            ambiguous = facts.ambiguous_kept as u64,
            torn = facts.torn_discarded as u64,
            "slot_store_booted"
        );
        Ok(())
    }

    fn assert_invariants(&self) {
        let floor = self.meta.floor;
        assert!(
            self.accepted.keys().next().is_none_or(|s| *s >= floor),
            "no accepted vote below the floor"
        );
        assert!(
            self.faulty.keys().next().is_none_or(|s| *s >= floor),
            "no faulty slot below the floor"
        );
        assert!(
            self.faulty.keys().all(|s| !self.accepted.contains_key(s)),
            "a slot is accepted or faulty, never both"
        );
        assert!(
            self.slots.keys().next().is_none_or(|s| *s >= floor),
            "no slot record kept below the floor"
        );
    }

    /// Raise the floor to `first` in memory: drop every vote, faulty slot,
    /// staged write and live frame below it.
    fn raise_floor(&mut self, first: Slot) {
        if first <= self.meta.floor {
            return;
        }
        self.meta.floor = first;
        self.accepted = self.accepted.split_off(&first);
        self.faulty = self.faulty.split_off(&first);
        self.staged = self.staged.split_off(&first);
        let kept = self.slots.split_off(&first);
        for meta in std::mem::replace(&mut self.slots, kept).into_values() {
            if let Some(loc) = meta.live {
                self.unpin(loc);
            }
        }
        self.meta_dirty = true;
        self.assert_invariants();
    }

    fn unpin(&mut self, loc: Loc) {
        let count = self
            .live
            .get_mut(&loc.segment)
            .expect("a live frame's segment is counted");
        assert!(*count > 0, "a live frame is counted once");
        *count -= 1;
    }

    /// Seal `state` with a floor raised to `first`: a floor that does not
    /// rise keeps the state sealed with the higher one.
    fn seal(&mut self, first: Slot, state: JournalState) {
        if first >= self.meta.floor {
            self.meta.sealed = state;
            self.meta_dirty = true;
        }
    }

    /// Write the metainfo (the module doc's first step).
    async fn save_meta(&mut self) -> Result<(), StorageError> {
        let bytes = self.meta.encode();
        let dual = self.dual.as_mut().expect("opened before a sync");
        dual.store(&self.provider, &bytes)
            .await
            .map_err(|_| StorageError::Io {
                record: StorageRecord::Promise,
                outcome: WriteOutcome::Unknown,
            })?;
        self.meta_dirty = false;
        Ok(())
    }

    /// Open a fresh payload segment and make its name durable before
    /// anything in it is acknowledged.
    async fn roll(&mut self) -> std::io::Result<()> {
        let segment = self.next_segment;
        self.next_segment += 1;
        let file = self
            .provider
            .open(
                &segment_path(&self.dir, segment),
                OpenOptions::create_new_write()
                    .read(true)
                    .direct_io(self.store.direct_io),
            )
            .await?;
        let file = BlockFile::new(file, self.store.block)?;
        file.sync().await?;
        self.provider.sync_dir(&self.dir).await?;
        self.live.entry(segment).or_default();
        self.active = Some(Active {
            segment,
            file,
            offset: 0,
        });
        Ok(())
    }

    /// The open chunk file for `chunk`, created whole — every copy a reserved
    /// record — and renamed into place first if it does not exist yet.
    async fn chunk(&mut self, chunk: u64) -> std::io::Result<&BlockFile<P::File>> {
        let path = chunk_path(&self.dir, chunk);
        if !self.chunks.contains_key(&chunk) {
            let temporary = format!("{path}.tmp");
            if self.provider.exists(&temporary).await? {
                self.provider.delete(&temporary).await?;
            }
            let file = self
                .provider
                .open(&temporary, OpenOptions::create_new_write().read(true))
                .await?;
            let file = BlockFile::new(file, self.store.block)?;
            let slots = u64::from(self.store.chunk_slots);
            let count = usize::try_from(2 * slots).expect("a chunk fits in memory");
            let mut buf = file.buffer(count)?;
            for i in 0..slots {
                let slot = Slot(chunk * slots + i);
                for copy in 0..2 {
                    let at = usize::try_from(self.store.block_of(slot, copy)).expect("in memory")
                        * self.store.block;
                    encode_reserved(slot, copy, &mut buf.as_mut_slice()[at..at + self.store.block]);
                }
            }
            file.write_blocks(0, buf.as_slice()).await?;
            file.sync().await?;
            drop(file);
            self.provider.rename(&temporary, &path).await?;
            self.provider.sync_dir(&self.dir).await?;
            self.chunks.insert(chunk, None);
            // A fresh chunk's copies are all reserved.
            for i in 0..slots {
                let slot = Slot(chunk * slots + i);
                if let Some(meta) = self.slots.get_mut(&slot) {
                    meta.copies = [Some(0), Some(0)];
                }
            }
        }
        let entry = self.chunks.get_mut(&chunk).expect("inserted above");
        if entry.is_none() {
            let file = self
                .provider
                .open(&path, OpenOptions::read_write().direct_io(self.store.direct_io))
                .await?;
            *entry = Some(BlockFile::new(file, self.store.block)?);
        }
        Ok(entry.as_ref().expect("opened above"))
    }

    /// Append the staged votes and the chosen index as one batch, then
    /// write their slot records (the module doc's second and third steps).
    async fn write_batch(&mut self) -> std::io::Result<()> {
        let staged = std::mem::take(&mut self.staged);
        let batch = self.next_batch;
        self.next_batch += 1;
        let block = self.store.block;
        // The batch's bytes: its head, then a frame and payload per vote.
        let payloads: Vec<(Slot, Ballot, Vec<u8>)> = staged
            .into_iter()
            .map(|(slot, (ballot, command))| (slot, ballot, encode_payload(&command)))
            .collect();
        let len: usize = FRAME_LEN + payloads.iter().map(|(_, _, p)| FRAME_LEN + p.len()).sum::<usize>();
        let padded = len.next_multiple_of(block);
        let fits = self.active.as_ref().is_some_and(|a| {
            a.offset == 0 || a.offset + padded as u64 <= self.store.segment_size
        });
        if !fits {
            self.roll().await?;
        }
        let active = self.active.as_ref().expect("rolled above");
        let (segment, start) = (active.segment, active.offset);
        assert!(start.is_multiple_of(block as u64), "a batch starts on a block");
        let mut buf = active.file.buffer(padded / block)?;
        let bytes = buf.as_mut_slice();
        let mut written = Vec::with_capacity(payloads.len());
        let mut at = FRAME_LEN;
        for (slot, ballot, payload) in &payloads {
            let meta = self.slots.entry(*slot).or_default();
            // Spent before anything is written: no generation is ever
            // written twice.
            meta.generation += 1;
            let loc = Loc {
                segment,
                offset: start + at as u64,
                len: u32::try_from(payload.len()).expect("a command fits a frame"),
                crc: crc32c::crc32c(payload),
            };
            let frame = Frame {
                slot: *slot,
                generation: meta.generation,
                ballot: *ballot,
                batch,
                loc,
            };
            encode_frame(&frame, &mut bytes[at..at + FRAME_LEN]);
            at += FRAME_LEN;
            bytes[at..at + payload.len()].copy_from_slice(payload);
            at += payload.len();
            written.push(frame);
        }
        assert!(at == len, "the batch is exactly its frames");
        let head = Head {
            segment,
            offset: start,
            batch,
            frames: written.len() as u64,
            chosen: self.chosen_index,
            bytes: len as u64,
        };
        encode_head(&head, &mut bytes[..FRAME_LEN]);
        active.file.write_blocks(start / block as u64, buf.as_slice()).await?;
        active.file.sync().await?;
        self.active.as_mut().expect("active").offset = start + padded as u64;
        self.chosen_dirty = false;

        // The slot records, each into the copy not holding the newest one.
        let mut touched = BTreeSet::new();
        let mut record = vec![0_u8; block];
        for frame in &written {
            let chunk = self.store.chunk_of(frame.slot);
            self.chunk(chunk).await?;
            let meta = self.slots.get_mut(&frame.slot).expect("staged above");
            let copy = meta.target();
            meta.copies[usize::from(copy)] = Some(frame.generation);
            let previous = meta.live.replace(frame.loc);
            encode_record(
                &SlotRecord {
                    slot: frame.slot,
                    generation: frame.generation,
                    ballot: frame.ballot,
                    batch,
                    loc: frame.loc,
                },
                copy,
                &mut record,
            );
            let index = self.store.block_of(frame.slot, copy);
            let file = self.chunk(chunk).await?;
            let mut aligned = file.buffer(1)?;
            aligned.as_mut_slice().copy_from_slice(&record);
            file.write_blocks(index, aligned.as_slice()).await?;
            touched.insert(chunk);
            *self.live.entry(segment).or_default() += 1;
            if let Some(previous) = previous {
                self.unpin(previous);
            }
        }
        for chunk in touched {
            self.chunk(chunk).await?.sync().await?;
        }
        Ok(())
    }

    /// Delete what the floor and the newer generations left dead (the
    /// module doc's *Space*).
    async fn reclaim(&mut self) -> std::io::Result<()> {
        let floor = self.meta.floor;
        let slots = u64::from(self.store.chunk_slots);
        let dead_chunks: Vec<u64> = self
            .chunks
            .keys()
            .copied()
            .filter(|k| (k + 1) * slots <= floor.0)
            .collect();
        // The active segment, and the newest on disk — whose last head holds
        // the newest chosen index until a batch of this incarnation lands —
        // are never reclaimed.
        let active = self.active.as_ref().map(|a| a.segment);
        let newest = self.live.keys().next_back().copied();
        let dead_segments: Vec<u64> = self
            .live
            .iter()
            .filter(|(segment, count)| {
                **count == 0 && Some(**segment) != active && Some(**segment) != newest
            })
            .map(|(segment, _)| *segment)
            .collect();
        for &chunk in &dead_chunks {
            self.chunks.remove(&chunk);
            self.provider.delete(&chunk_path(&self.dir, chunk)).await?;
        }
        for &segment in &dead_segments {
            self.live.remove(&segment);
            self.provider.delete(&segment_path(&self.dir, segment)).await?;
        }
        if !dead_chunks.is_empty() || !dead_segments.is_empty() {
            self.provider.sync_dir(&self.dir).await?;
            tracing::debug!(
                node = self.config.id.0,
                chunks = dead_chunks.len() as u64,
                segments = dead_segments.len() as u64,
                "slot_store_reclaimed"
            );
        }
        // No live frame sits in a deleted segment.
        assert!(
            self.slots
                .values()
                .filter_map(|m| m.live)
                .all(|loc| self.live.contains_key(&loc.segment)),
            "a live vote's segment is never reclaimed"
        );
        Ok(())
    }

    async fn flush(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        if self.meta_dirty || !self.dual.as_ref().is_some_and(Dual::is_stored) {
            self.save_meta().await?;
        }
        let relaxed_only = must_sync == MustSync::Relaxed && self.staged.is_empty();
        if !relaxed_only && (!self.staged.is_empty() || self.chosen_dirty) {
            self.write_batch().await.map_err(|_| write_fault())?;
        }
        self.reclaim().await.map_err(|_| write_fault())?;
        self.assert_invariants();
        Ok(())
    }
}

fn misdirected_meta() -> StorageError {
    StorageError::Corruption {
        record: StorageRecord::Promise,
        fault: IntegrityFault::Misdirected,
        verdict: CorruptionVerdict::Corrupted,
    }
}

fn meta_load_error(error: &DualError) -> StorageError {
    match error {
        DualError::Corrupt => StorageError::Corruption {
            record: StorageRecord::Promise,
            fault: IntegrityFault::ChecksumMismatch,
            verdict: CorruptionVerdict::Corrupted,
        },
        DualError::Io(_) => read_fault(StorageRecord::Store),
    }
}

impl<P: StorageProvider> Storage for SlotStorage<P> {
    fn initial_state(&self) -> (HardState, Config) {
        let mut hard_state = HardState::default();
        hard_state.max_promised_ballot = self.meta.promise;
        hard_state.chosen_index = self.chosen_index;
        (hard_state, self.config.clone())
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.accepted.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        self.meta.floor
    }

    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }

    fn sealed_state(&self) -> JournalState {
        self.meta.sealed
    }

    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty.iter().map(|(s, b)| (*s, *b)).collect()
    }
}

impl<P: StorageProvider> LogStorage for SlotStorage<P> {
    /// Open the store and fold every slot (the module doc's table). A clean
    /// store, a store with faulty slots (reported through the read ports),
    /// and a store a crash cut mid-batch all boot; a store whose metainfo is
    /// lost, whose layout changed, or whose evidence contradicts itself is a
    /// crash verdict.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.load().await
    }

    fn formatted_config(&self) -> Option<Config> {
        self.meta.formatted.clone()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0))]
    async fn format(&mut self, config: &Config) -> Result<(), StorageError> {
        self.opened().await?;
        // The marker is set once, never edited (the driver refuses a
        // formatted store before it gets here).
        assert!(self.meta.formatted.is_none(), "a store is formatted once");
        self.meta.formatted = Some(config.clone());
        self.meta_dirty = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, round = ballot.round))]
    async fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.opened().await?;
        // Write half of the promise pair: the core only ever raises it.
        assert!(ballot >= self.meta.promise, "a persisted promise never falls");
        if ballot != self.meta.promise {
            self.meta.promise = ballot;
            self.meta_dirty = true;
        }
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
        if slot < self.meta.floor {
            // Compacted: nothing below the floor is kept.
            return Ok(());
        }
        self.faulty.remove(&slot);
        self.accepted.insert(slot, (ballot, command.clone()));
        self.staged.insert(slot, (ballot, command));
        // Write half of the vote pair: what is staged is what the read port
        // serves now, and what a boot serves after the sync.
        assert!(
            self.accepted.get(&slot) == self.staged.get(&slot),
            "a staged vote is the served vote"
        );
        self.assert_invariants();
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0))]
    async fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.opened().await?;
        if self.chosen_index != Some(slot) {
            self.chosen_index = Some(slot);
            self.chosen_dirty = true;
        }
        Ok(())
    }

    /// The metainfo first, then the payloads, then the slot records — see
    /// the module doc. A [`MustSync::Relaxed`] batch holding nothing but a
    /// chosen index is deferred to the next flush: the relaxed contract lets
    /// a crash lose it.
    #[tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0))]
    async fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        self.opened().await?;
        let flushed = self.flush(must_sync).await;
        if flushed.is_err() {
            // The disk holds an unknown mix of this batch: only a boot reads
            // it again.
            self.poisoned = true;
        }
        flushed
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, first = first.0))]
    async fn truncate(&mut self, first: Slot, sealed: JournalState) -> Result<(), StorageError> {
        self.opened().await?;
        let floor = self.meta.floor;
        self.seal(first, sealed);
        self.raise_floor(first);
        assert!(self.meta.floor >= floor, "a floor never moves backward");
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, point = point.0))]
    async fn trimmed_to(&mut self, point: Slot, state: JournalState) -> Result<(), StorageError> {
        self.opened().await?;
        self.seal(point, state);
        if let Some(boundary) = point.0.checked_sub(1).map(Slot) {
            if self.chosen_index.is_none_or(|c| c < boundary) {
                self.chosen_index = Some(boundary);
                self.chosen_dirty = true;
            }
            if self.meta.trimmed_chosen.is_none_or(|c| c < boundary) {
                self.meta.trimmed_chosen = Some(boundary);
                self.meta_dirty = true;
            }
        }
        self.raise_floor(point);
        assert!(self.meta.floor >= point, "a trim jump lands the floor on its point");
        Ok(())
    }
}
