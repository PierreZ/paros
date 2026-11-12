//! What a journal failure means to the driver: each `OpenError` and
//! `CommitError` of `moonpool-journal` maps to one typed `StorageError`
//! (a reopen, a read-only store, or a crash verdict).

use std::io::{Error, ErrorKind};

use moonpool_journal::{CommitError, Durable, OpenError};
use paros_model::corruption::{CorruptionVerdict, IntegrityFault};

use super::super::{commit_error, open_error};
use crate::{MetadataFault, StorageError, StorageRecord, WriteOutcome};

fn corrupted(record: StorageRecord, fault: IntegrityFault) -> StorageError {
    StorageError::Corruption {
        record,
        fault,
        verdict: CorruptionVerdict::Corrupted,
    }
}

/// An I/O error is the provider failing (unknown outcome), a full disk
/// makes the store read-only, and damage is a crash verdict; only a lost
/// metainfo names the store's scalars.
#[test]
fn each_open_error_has_its_verdict() {
    let scalars = StorageRecord::Promise;
    let store = StorageRecord::Store;
    let cases = [
        (
            OpenError::Io(Error::from(ErrorKind::StorageFull)),
            StorageError::Full { record: store },
        ),
        (
            OpenError::Io(Error::from(ErrorKind::PermissionDenied)),
            StorageError::Io {
                record: store,
                outcome: WriteOutcome::Unknown,
            },
        ),
        (
            OpenError::DoubleFault { batch: 1, index: 2 },
            corrupted(store, IntegrityFault::ChecksumMismatch),
        ),
        (
            OpenError::LostBatch { batch: 3 },
            corrupted(store, IntegrityFault::LostWrite),
        ),
        (
            OpenError::MetaLost,
            corrupted(scalars, IntegrityFault::ChecksumMismatch),
        ),
        (
            OpenError::WrongJournal { found: 9 },
            corrupted(store, IntegrityFault::Misdirected),
        ),
        (
            OpenError::BadSegment {
                name: "0001.wal".into(),
                reason: "short",
            },
            StorageError::Metadata {
                fault: MetadataFault::WrongSize,
            },
        ),
        (
            OpenError::InvalidConfig("bad"),
            StorageError::Metadata {
                fault: MetadataFault::Missing,
            },
        ),
        (
            OpenError::AlreadyExists,
            StorageError::Metadata {
                fault: MetadataFault::Missing,
            },
        ),
    ];
    for (error, verdict) in cases {
        assert_eq!(open_error(&error, scalars), verdict, "{error:?}");
    }
    assert_eq!(
        open_error(&OpenError::MetaLost, StorageRecord::MatchmakerScalars).record(),
        StorageRecord::MatchmakerScalars
    );
}

/// A commit that may have written part of its batch is an fsync failure of
/// unknown outcome; one refused before any write is known lost, and a full
/// disk before any write is `Full`.
#[test]
fn each_commit_error_has_its_verdict() {
    let batch = StorageRecord::Batch;
    let io = |kind, durable| CommitError::Io {
        error: Error::from(kind),
        durable,
    };
    let unknown = StorageError::FsyncFailed {
        record: batch,
        outcome: WriteOutcome::Unknown,
    };
    let lost = StorageError::Io {
        record: batch,
        outcome: WriteOutcome::Lost,
    };
    let cases = [
        (io(ErrorKind::Other, Durable::Unknown), unknown.clone()),
        // A full disk after a partial write stays ambiguous.
        (
            io(ErrorKind::StorageFull, Durable::Unknown),
            unknown.clone(),
        ),
        (CommitError::Poisoned, unknown),
        (
            io(ErrorKind::StorageFull, Durable::No),
            StorageError::Full { record: batch },
        ),
        (io(ErrorKind::Other, Durable::No), lost.clone()),
        (
            CommitError::BelowFloor {
                position: 1,
                floor: 2,
            },
            lost.clone(),
        ),
        (CommitError::MetaTooLarge { len: 1 << 20 }, lost.clone()),
        (CommitError::BatchTooLarge, lost),
    ];
    for (error, verdict) in cases {
        assert_eq!(commit_error(&error), verdict, "{error:?}");
    }
}

/// A full disk is `Full` by its kind or by its `ENOSPC` code, the one
/// moonpool's simulated disk gives. `wasm32-unknown-unknown` decodes no OS
/// code into a kind, so the code alone keeps a seed the same run there
/// (#307 (paros-sim in wasm)).
#[test]
fn a_full_disk_is_known_by_its_kind_or_its_code() {
    let batch = StorageRecord::Batch;
    let store = StorageRecord::Store;
    let by_code = || Error::from_raw_os_error(28);
    let by_kind = || Error::from(ErrorKind::StorageFull);
    for full in [by_code(), by_kind()] {
        assert_eq!(
            commit_error(&CommitError::Io {
                error: full,
                durable: Durable::No,
            }),
            StorageError::Full { record: batch }
        );
    }
    for full in [by_code(), by_kind()] {
        assert_eq!(
            open_error(&OpenError::Io(full), StorageRecord::Promise),
            StorageError::Full { record: store }
        );
    }
    assert_eq!(
        open_error(
            &OpenError::Io(Error::from_raw_os_error(5)),
            StorageRecord::Promise
        ),
        StorageError::Io {
            record: store,
            outcome: WriteOutcome::Unknown,
        },
        "EIO is no full disk"
    );
}
