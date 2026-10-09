//! The typed **corruption verdict** a store surfaces (Stage 7, CTRL-shaped):
//! the fault family a failed integrity check caught ([`IntegrityFault`]) and
//! what it means to the driver ([`CorruptionVerdict`]), carried as data on
//! [`StorageError::Corruption`](crate::StorageError::Corruption). Nothing
//! downstream parses strings or rescans traces.
//!
//! The classification itself is the journal's (`moonpool-journal`, CLSTORE:
//! a torn last batch discarded, a damaged entry whose persist record
//! survived reported with its identity, an ambiguous last batch of a
//! one-sync commit); `paros::journal` maps its states onto these verdicts.
//! The durable-record contract is
//! `docs/analysis/storage/clstore-record-contract.md`.

use std::fmt;

/// The fault family a failed integrity check surfaced — *how* the detector
/// caught the record, carried as data on
/// [`StorageError::Corruption`](crate::StorageError::Corruption) so the
/// simulation can correlate injected fault ↔ surfaced error without string
/// parsing, and so Stage 8 can weigh families differently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntegrityFault {
    /// The record's bytes failed their checksum (bit flip, latent sector
    /// error, torn write).
    ChecksumMismatch,
    /// The record is absent where its identifier (or the reserved-record
    /// contract) says it must exist — a lost write.
    LostWrite,
    /// The checksum passed but the identity inside the checksummed region
    /// names a different record — a misdirected read or write.
    Misdirected,
    /// The read returned an I/O error (`EIO`). Collapsed into the corruption
    /// channel (CTRL §4.1): an unreadable record is treated exactly as a
    /// checksum mismatch ("zero-fill then mismatch" semantics).
    ReadError,
}

impl fmt::Display for IntegrityFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntegrityFault::ChecksumMismatch => write!(f, "checksum mismatch"),
            IntegrityFault::LostWrite => write!(f, "lost write"),
            IntegrityFault::Misdirected => write!(f, "misdirected record"),
            IntegrityFault::ReadError => write!(f, "read error (EIO)"),
        }
    }
}

/// The crash-vs-corruption **disentanglement verdict** for a detected
/// mismatch: what the local evidence proves about the record's history. This
/// is the value Stage 8's crash-relevance logic consumes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorruptionVerdict {
    /// A torn write at the tail: the record's persist witness never reached
    /// disk, so the write was never acknowledged to anyone and is safe to
    /// discard locally. This is the *only* verdict that may drop data.
    CrashTail,
    /// A mismatch on a previously persisted (witnessed) record — possibly
    /// chosen, so it must NOT be discarded. Stage 7 reaction: crash. Stage 8:
    /// recover from peers.
    Corrupted,
    /// The evidence cannot distinguish crash from corruption (the last-entry
    /// case, proven fundamental by CTRL Thm A.1, and the hardening's
    /// abandoned-window cases). Treated as corruption: crash; Stage 8's
    /// distributed commitment determination decides.
    Undecidable,
}

impl fmt::Display for CorruptionVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorruptionVerdict::CrashTail => write!(f, "crash-truncatable tail"),
            CorruptionVerdict::Corrupted => write!(f, "corruption"),
            CorruptionVerdict::Undecidable => write!(f, "undecidable"),
        }
    }
}
