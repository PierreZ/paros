//! The **durable stores**: [`JournalStorage`] ([`NodeStorage`](crate::NodeStorage)) and
//! [`JournalMatchmakerStorage`] ([`MatchmakerStorage`](crate::MatchmakerStorage)), both on
//! `moonpool-journal` — the CLSTORE write-ahead journal over moonpool's
//! `BlockFile`, generic over the provider's `StorageProvider`, so the same
//! stores run over Tokio's filesystem in production and over the
//! simulator's disk in tests.
//!
//! # The shape: a log of write operations, folded at boot
//!
//! The journal is a dense, append-only log with a far identifier per entry;
//! paros's durable state is not (an acceptor re-accepts a slot at a higher
//! ballot while later slots stay, a slot may have no record at all, the
//! floor drops a prefix). So the journal holds the **operations**, not the
//! state: every staged write becomes one entry, a `sync` appends the batch
//! (one write, one `fdatasync`), and the boot scan reads the log back and
//! folds it into the in-memory image the synchronous read ports answer from
//! — the same fold the live writes go through, so the image a boot rebuilds
//! is the image the writes left. A periodic **checkpoint** re-emits the
//! image as one bracketed batch and lets the journal drop the whole
//! segments before it, so the log stays proportional to the state.
//!
//! | Durable state | Where it lives | Entry identity (epoch kind, tag) |
//! |---|---|---|
//! | promise ([`HardState::max_promised_ballot`](paros_core::HardState)) + format marker (#147) | journal **metadata** (two copies, temp + fsync + rename) | — |
//! | accepted / learned `(slot, ballot, command)` | `Accepted` entry | `(slot, ballot.round, ballot.node)` |
//! | a faulty slot carried by a checkpoint | `Faulty` entry | `(slot, ballot.round, ballot.node)` |
//! | chosen index | `ChosenIndex` entry (a `Relaxed` batch is deferred to the next `Sync`) | — |
//! | truncation floor + sealed ledger | `Truncate` entry | — |
//! | snapshot install (index, floor, sessions) | `InstallSnapshot` entry (its ballot goes to the metadata) | — |
//! | decided snapshot point (#101) | `SnapPoint` header (length, per-chunk CRC) + one `SnapChunk` per chunk | `(at)` / `(at, chunk)` |
//! | matchmaker registration | `Register` entry | `(ballot.round, ballot.node)` |
//! | matchmaker scalars (generation, freeze, decree, watermark) | `Scalars` entry, the whole image | — |
//!
//! Every entry's epoch is `kind << 56 | batch`, where `batch` counts the
//! appends: the far identifier alone says what an entry *was*, and which
//! batch — the last one is the only one a crash can have left unsynced.
//!
//! # Corruption: what the journal reports, what paros does with it
//!
//! The journal tells a torn tail (no identifier: never acknowledged, cut)
//! from a damaged entry whose identifier survived, and reports the latter
//! with its identity. The stores run the journal with
//! [`AmbiguousTail::Keep`]: a damaged
//! last entry stays in the log, marked corrupt, so its identity survives the
//! next crash too — the CTRL undecidable row is the replication layer's to
//! resolve, never a local truncation. What a damaged entry means is decided
//! by its kind (`node_image` and `matchmaker` hold the per-kind table):
//!
//! - a damaged **accepted** record is reported as
//!   [`faulty(slot, ballot)`](paros_core::Storage::faulty_entries) from its
//!   tag — CTRL's recoverable class, repaired from peers, never counted as
//!   "nothing accepted here";
//! - a damaged **snapshot chunk** is a [faulty chunk](crate::NodeStorage::faulty_snap_chunks),
//!   repaired chunk by chunk;
//! - a damaged chosen index, truncation or snapshot install is **forgotten**:
//!   each is a local, re-derivable fact (the relaxed commit index, a lazy
//!   compaction whose records the log still holds, an install the node can
//!   be sent again), and forgetting one leaves the store in the state it had
//!   before it — a node behind, never a node wrong;
//! - a damaged checkpoint header or sealed ledger whose prefix the journal
//!   already dropped, a damaged live matchmaker record, and anything the
//!   journal cannot open are **crash** verdicts
//!   ([`StorageError::Corruption`] / [`StorageError::Metadata`]).
//!
//! The in-memory image is loaded once, by the boot scan, and every
//! synchronous accessor answers from it — the contract on
//! [`NodeStorage::boot_scan`](crate::NodeStorage::boot_scan).

mod frame;
mod matchmaker;
mod node;
mod node_image;
mod plan;

#[cfg(test)]
mod tests;

use moonpool_core::DirectIo;
use moonpool_journal::{AmbiguousTail, Geometry, JournalConfig, JournalError};

use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::storage::{MetadataFault, StorageError, StorageRecord, WriteOutcome};

pub use matchmaker::JournalMatchmakerStorage;
pub use node::JournalStorage;

/// The index the first entry of every journal gets: a store whose log still
/// starts here has never dropped a prefix, so its whole history is on disk.
const GENESIS: u64 = 1;

/// How a journal store lays out and maintains its journal.
///
/// A plain data struct, like [`DriverTunables`](crate::DriverTunables): a
/// production deployment keeps the CLSTORE defaults, a simulation or a test
/// shrinks the geometry so rollover and prefix truncation are cheap to
/// reach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalStoreConfig {
    /// The journal's segment shape. Must match what an existing store was
    /// created with: the journal refuses a segment whose header or size
    /// disagrees (reported as a [`MetadataFault`]).
    pub geometry: Geometry,
    /// Direct-I/O policy for the segment files.
    pub direct_io: DirectIo,
    /// Checkpoint once the live log holds at least this many entries: the
    /// image is re-emitted as one bracketed batch and the whole segments
    /// before it are dropped. Floor 1 (a checkpoint after every append is
    /// valid, only slow); the log never needs more than one checkpoint's
    /// worth plus this many entries of history.
    pub checkpoint_after: u64,
}

impl Default for JournalStoreConfig {
    /// The CLSTORE layout (64 MiB segments) and a checkpoint every 32,768
    /// entries — one segment's slot table.
    fn default() -> Self {
        Self {
            geometry: Geometry::default(),
            direct_io: DirectIo::Optional,
            checkpoint_after: 32_768,
        }
    }
}

impl JournalStoreConfig {
    /// A small layout for simulation and tests: 256-slot, 256 KiB segments
    /// and a checkpoint every 256 entries, so rollover, checkpoints and
    /// prefix truncation happen within a short run.
    #[must_use]
    pub fn small() -> Self {
        Self {
            geometry: Geometry {
                slot_count: 256,
                // Two header blocks, then 256 × 64-byte slots.
                data_start: 8 * 1024 + 16 * 1024,
                segment_size: 256 * 1024,
            },
            direct_io: DirectIo::Optional,
            checkpoint_after: 256,
        }
    }

    fn journal(self) -> JournalConfig {
        JournalConfig {
            geometry: self.geometry,
            direct_io: self.direct_io,
            first_index: GENESIS,
            ambiguous_tail: AmbiguousTail::Keep,
        }
    }
}

/// What a journal that cannot be opened means to the driver: the store is
/// unusable at file granularity, or a record is unreadable in a way no
/// replay can classify. Every arm is a crash verdict.
fn open_error(error: &JournalError) -> StorageError {
    match error {
        JournalError::DoubleFault { .. } => StorageError::Corruption {
            record: StorageRecord::Store,
            fault: IntegrityFault::ChecksumMismatch,
            verdict: CorruptionVerdict::Corrupted,
        },
        JournalError::MetadataCorrupt { .. } => StorageError::Corruption {
            record: StorageRecord::Promise,
            fault: IntegrityFault::ChecksumMismatch,
            verdict: CorruptionVerdict::Corrupted,
        },
        JournalError::SegmentSize { .. } => StorageError::Metadata {
            fault: MetadataFault::WrongSize,
        },
        JournalError::Io(_) => StorageError::Corruption {
            record: StorageRecord::Store,
            fault: IntegrityFault::ReadError,
            verdict: CorruptionVerdict::Corrupted,
        },
        _ => StorageError::Metadata {
            fault: MetadataFault::Missing,
        },
    }
}

/// What a failed append means: part of the batch may be on disk (the
/// journal poisons itself for exactly that reason), so the outcome is
/// unknown — the fsyncgate ambiguity the driver resolves by crashing.
fn append_error(error: &JournalError) -> StorageError {
    match error {
        JournalError::EntryTooLarge { .. } => StorageError::Io {
            record: StorageRecord::Batch,
            outcome: WriteOutcome::Lost,
        },
        _ => StorageError::FsyncFailed {
            record: StorageRecord::Batch,
            outcome: WriteOutcome::Unknown,
        },
    }
}

/// What a failed metadata write means: the journal keeps at least one copy
/// intact, holding the old value or the new one.
fn meta_error(_error: &JournalError) -> StorageError {
    StorageError::Io {
        record: StorageRecord::Promise,
        outcome: WriteOutcome::Unknown,
    }
}
