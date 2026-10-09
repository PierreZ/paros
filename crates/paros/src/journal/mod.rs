//! The **durable stores**: [`JournalStorage`] ([`LogStorage`](crate::LogStorage)) and
//! [`JournalMatchmakerStorage`] ([`MatchmakerStorage`](crate::MatchmakerStorage)), both on
//! `moonpool-journal`, the CLSTORE journal over moonpool's storage providers,
//! so the same stores run over Tokio's filesystem in production and over the
//! simulator's disk in tests.
//!
//! # The shape: state, not a log of operations
//!
//! The journal keys an entry by a `u64` **position** and names it with an
//! opaque identity, in any order, with overwrites: exactly a Paxos
//! acceptor's state. So the stores keep the state itself, and a boot reads
//! it back: no fold, no checkpoint.
//!
//! | Durable state | Where it lives |
//! |---|---|
//! | accepted / learned `(slot, ballot, command)` | the entry at position `slot`, the ballot in its identity |
//! | promise, format marker and its `Config` (#147, #207), chosen index, floor, sealed journal state | the journal's **metainfo** (two local copies) |
//! | truncation floor, trim-point jump (#186) | the journal's floor, raised in the same commit as the sealed state |
//! | matchmaker registration | the entry at a registration number, the ballot and the generation in its identity |
//! | matchmaker scalars and format marker (#183) | the metainfo |
//!
//! The chosen index is relaxed (re-derivable after a crash): it rides the
//! next commit that writes the metainfo for another reason, a promise, a
//! truncation or a format, and is never a reason to write it on its own.
//!
//! # Ordering: a commit is atomic one way only
//!
//! The journal writes a commit's metainfo only once its batch is durable
//! (moonpool#309): a durable metainfo vouches for its batch, but a crash
//! between the two can land the batch without its metainfo. So a store puts
//! in one commit a batch and the metainfo that depends on it, never a batch
//! that depends on the metainfo; that order goes across commits, each
//! durable before the next starts (#264, #176). [`JournalStorage`] commits a
//! raised promise alone (before any entry it covers: an entry durable above
//! its promise would be an accept the node never promised), then the
//! entries with the floor and the metainfo (a chosen index never ahead of
//! the entry that makes its slot chosen); [`JournalMatchmakerStorage`]
//! commits its new registrations with the metainfo, then its clears, and its
//! boot keeps only what the durable metainfo vouches for.
//!
//! # Corruption: what the journal reports, what paros does with it
//!
//! The journal tells a torn write (discarded: never acknowledged) from a
//! damaged entry whose persist record survived, and reports the latter with
//! its identity: [`Corrupt`](moonpool_journal::State::Corrupt), or
//! [`Ambiguous`](moonpool_journal::State::Ambiguous) in the last batch of a
//! one-sync commit, where a crash and rot look alike (CTRL Theorem A.1).
//! Both mean the same to paros:
//!
//! - a damaged **accepted** entry is reported as
//!   [`faulty(slot, ballot)`](paros_core::Storage::faulty_entries) from its
//!   identity: CTRL's recoverable class, repaired from peers, never counted
//!   as "nothing accepted here";
//! - a damaged live **registration** is a crash verdict (a registry is
//!   replaced, never repaired, #125), unless the GC watermark already
//!   collected it;
//! - anything the journal refuses to open (both metainfo copies lost, a
//!   double fault, a lost batch, a damaged segment file) is a crash verdict
//!   ([`StorageError::Corruption`] / [`StorageError::Metadata`]).
//!
//! The in-memory image is loaded once, by the boot scan, and every
//! synchronous accessor answers from it: the contract on
//! [`LogStorage::boot_scan`](crate::LogStorage::boot_scan).

mod matchmaker;
mod node;

#[cfg(test)]
mod tests;

use moonpool_core::DirectIo;
use moonpool_journal::{CommitError, Durable, ID_SIZE, Id, JournalConfig, OpenError};
use paros_core::{Ballot, JournalIdentifier, NodeId};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::storage::{MetadataFault, StorageError, StorageRecord, WriteOutcome};

pub use matchmaker::JournalMatchmakerStorage;
/// The journal's commit protocol and segment shape, re-exported so a store's
/// caller configures it without depending on `moonpool-journal`.
pub use moonpool_core::LayoutRegion;
pub use moonpool_journal::{BLOCK, Durability, Geometry, Layout};
pub use moonpool_journal::{CommitHooks, CommitPoint, NoCommitHooks};
pub use node::{JournalBootFacts, JournalStorage};

/// How a journal store lays out and writes its journal.
///
/// A plain data struct, like [`DriverTunables`](crate::DriverTunables): a
/// production deployment keeps the defaults, a simulation shrinks the
/// geometry so rollover and prefix truncation are cheap to reach and draws
/// the durability per seed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalStoreConfig {
    /// The segment shape. Must match what an existing store was created
    /// with: the journal refuses another one.
    pub geometry: Geometry,
    /// Direct-I/O policy for the segment files.
    pub direct_io: DirectIo,
    /// One sync per commit (CLSTORE's, the last batch ambiguous) or two
    /// (always decided). Either is safe for paros: an ambiguous entry is
    /// reported faulty like a corrupt one. Recorded per batch, so it may
    /// change between restarts.
    pub durability: Durability,
}

impl Default for JournalStoreConfig {
    /// 64 MiB segments, two syncs per commit.
    fn default() -> Self {
        let config = Self {
            geometry: Geometry::default(),
            direct_io: DirectIo::Optional,
            durability: Durability::Ordered,
        };
        config.assert_layout();
        config
    }
}

impl JournalStoreConfig {
    /// A small layout for simulation and tests: 512 records and 64 KiB of
    /// entries per segment, so rollover and segment deletion happen within
    /// a short run.
    #[must_use]
    pub fn small() -> Self {
        let config = Self {
            geometry: Geometry::small(),
            ..Self::default()
        };
        config.assert_layout();
        config
    }

    /// A shipped layout is one the journal opens.
    fn assert_layout(self) {
        assert!(self.geometry.is_valid(), "a shipped geometry is valid");
    }

    fn journal(self) -> JournalConfig {
        JournalConfig {
            durability: self.durability,
            geometry: self.geometry,
            direct_io: self.direct_io,
        }
    }
}

/// The journal's own identity for a paros journal's store: the tenant in
/// the high half, the journal in the low half, so a store of another
/// journal is refused (`WrongJournal`) rather than read.
fn store_id(journal: JournalIdentifier) -> moonpool_journal::JournalId {
    let id = (u128::from(journal.tenant.0) << 64) | u128::from(journal.journal.0);
    // Pair of the halves: the identity reads back as the journal.
    assert!(
        u64::try_from(id >> 64) == Ok(journal.tenant.0)
            && u64::try_from(id & u128::from(u64::MAX)) == Ok(journal.journal.0),
        "a store identity reads back as its journal"
    );
    moonpool_journal::JournalId(id)
}

// A ballot and a kind byte fit an entry's identity.
const _: () = assert!(ID_SIZE > 8 + 8);

/// An entry's identity: its ballot, and a kind byte saying what it is.
fn ballot_id(ballot: Ballot, kind: u8) -> Id {
    let mut id = [0; ID_SIZE];
    id[..8].copy_from_slice(&ballot.round.to_le_bytes());
    id[8..16].copy_from_slice(&ballot.node.0.to_le_bytes());
    id[16] = kind;
    // Pair of `id_ballot`: a damaged entry still names its ballot and kind.
    assert!(
        id_ballot(&id) == (ballot, kind),
        "an identity reads back as its ballot"
    );
    id
}

/// The ballot and kind an identity names.
fn id_ballot(id: &Id) -> (Ballot, u8) {
    let word = |at: usize| u64::from_le_bytes(id[at..at + 8].try_into().expect("8 bytes"));
    (
        Ballot {
            round: word(0),
            node: NodeId(word(8)),
        },
        id[16],
    )
}

/// Bumped when an encoding changes; bytes of another version do not decode.
const FORMAT_VERSION: u8 = 3;

/// A version byte and the value's `postcard` encoding.
fn encode<T: Serialize + DeserializeOwned + PartialEq>(value: &T) -> Vec<u8> {
    let mut bytes = vec![FORMAT_VERSION];
    bytes.extend(postcard::to_stdvec(value).expect("in-memory encoding"));
    // Pair of `decode`: what a write stores is what a boot reads.
    assert!(
        decode::<T>(&bytes).as_ref() == Some(value),
        "an encoding decodes back to itself"
    );
    bytes
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    let (&version, body) = bytes.split_first()?;
    (version == FORMAT_VERSION)
        .then(|| postcard::from_bytes(body).ok())
        .flatten()
}

/// Bytes that passed the journal's checks and still do not decode: written
/// by something else, or for something else. A crash verdict.
fn undecodable(record: StorageRecord) -> StorageError {
    StorageError::Corruption {
        record,
        fault: IntegrityFault::Misdirected,
        verdict: CorruptionVerdict::Corrupted,
    }
}

/// What a journal that cannot be opened means to the driver. An I/O error
/// is the provider failing (reopen); every other arm is a crash verdict.
fn open_error(error: &OpenError, scalars: StorageRecord) -> StorageError {
    let corrupted = |record, fault| StorageError::Corruption {
        record,
        fault,
        verdict: CorruptionVerdict::Corrupted,
    };
    match error {
        OpenError::Io(_) => StorageError::Io {
            record: StorageRecord::Store,
            outcome: WriteOutcome::Unknown,
        },
        // An entry and its persist record both damaged before the last
        // batch: nothing identifies what was there.
        OpenError::DoubleFault { .. } => {
            corrupted(StorageRecord::Store, IntegrityFault::ChecksumMismatch)
        }
        // A batch the log must hold is gone: acknowledged, and nothing on
        // disk stands in for it.
        OpenError::LostBatch { .. } => corrupted(StorageRecord::Store, IntegrityFault::LostWrite),
        // The node-unique state: no peer can tell a node what it promised.
        OpenError::MetaLost => corrupted(scalars, IntegrityFault::ChecksumMismatch),
        OpenError::WrongJournal { .. } => {
            corrupted(StorageRecord::Store, IntegrityFault::Misdirected)
        }
        OpenError::BadSegment { .. } => StorageError::Metadata {
            fault: MetadataFault::WrongSize,
        },
        OpenError::InvalidConfig(_) | OpenError::AlreadyExists => StorageError::Metadata {
            fault: MetadataFault::Missing,
        },
    }
}

/// What a failed commit means: before any write the batch is known absent;
/// after, part of it may be on disk (the journal poisons itself for exactly
/// that reason), the fsyncgate ambiguity the driver resolves by crashing.
fn commit_error(error: &CommitError) -> StorageError {
    match error {
        CommitError::Io {
            durable: Durable::Unknown,
            ..
        }
        | CommitError::Poisoned => StorageError::FsyncFailed {
            record: StorageRecord::Batch,
            outcome: WriteOutcome::Unknown,
        },
        // Refused or failed before anything was written: known absent.
        CommitError::Io {
            durable: Durable::No,
            ..
        }
        | CommitError::BelowFloor { .. }
        | CommitError::MetaTooLarge { .. }
        | CommitError::BatchTooLarge => StorageError::Io {
            record: StorageRecord::Batch,
            outcome: WriteOutcome::Lost,
        },
    }
}
