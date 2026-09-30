//! Log storage — the read-only [`Storage`] recovery port (from `paros-core`)
//! plus the [`LogStorage`] write extension the driver persists through, and the
//! default in-memory [`MemStorage`] implementing both.
//!
//! A store keeps an ordered, trimmable log and the acceptor's scalars, and
//! nothing else: since #186 paros runs no application and holds no
//! snapshot — a journal's client folds what it reads.

use std::fmt;
use std::future::Future;

use paros_core::{Ballot, Command, JournalState, MustSync, Slot, Storage};

use crate::corruption::{CorruptionVerdict, IntegrityFault};

/// The durable record a storage operation (and therefore a storage fault) hit.
///
/// Carried as **data** on every [`StorageError`] so Stage 7's detect-and-classify
/// and Stage 8's crash-relevance decisions can *match* on the record identity,
/// and so the simulation can correlate injected fault ↔ surfaced error ↔ node
/// reaction without string parsing. New identities slot in as plain variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRecord {
    /// The promised-ballot scalar (the `HardState` promise).
    Promise,
    /// The accepted `(ballot, command)` entry at this slot.
    Accepted(Slot),
    /// The chosen-index (commit index) scalar.
    ChosenIndex,
    /// The truncation record (the durable compaction floor + sealed journal state)
    /// — a decided `Truncate` or a jump below a peer's trim point.
    Truncation,
    /// The whole staged batch: an fsync flushes every record staged since the
    /// last flush, so a failed fsync has no single-record identity.
    Batch,
    /// A matchmaker's registration record under this ballot.
    Registration(Ballot),
    /// A matchmaker's durable scalars (generation, freeze, successor,
    /// decree record, watermark).
    MatchmakerScalars,
    /// The record store itself, at file granularity (FS metadata): the
    /// identity a [`StorageError::Metadata`] fault names, since a missing or
    /// unopenable store has no single-record identity either.
    Store,
}

impl fmt::Display for StorageRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageRecord::Promise => write!(f, "promise"),
            StorageRecord::Accepted(slot) => write!(f, "accepted[{}]", slot.0),
            StorageRecord::ChosenIndex => write!(f, "chosen-index"),
            StorageRecord::Truncation => write!(f, "truncation"),
            StorageRecord::Batch => write!(f, "batch"),
            StorageRecord::Registration(ballot) => {
                write!(f, "registration[{}.{}]", ballot.round, ballot.node.0)
            }
            StorageRecord::MatchmakerScalars => write!(f, "matchmaker-scalars"),
            StorageRecord::Store => write!(f, "store"),
        }
    }
}

/// A file-granularity FS-metadata fault on the record store itself (CTRL's
/// user-data vs FS-metadata split). The verdict for every member is **reliably
/// crash** — recovery is never attempted on metadata, in Stage 8 either: a
/// store the node cannot even open holds nothing to classify, and its durable
/// promise may be gone with it (the amnesia case a naive rejoin must never
/// take). The oracle judging these is asymmetric: unavailable = pass, unsafe =
/// fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataFault {
    /// The record store is missing or unopenable.
    Missing,
    /// The store has the wrong size (checkable: the log is fixed-size
    /// preallocated).
    WrongSize,
    /// The store mounted read-only: no write can ever succeed.
    ReadOnly,
}

impl fmt::Display for MetadataFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataFault::Missing => write!(f, "store missing"),
            MetadataFault::WrongSize => write!(f, "store has wrong size"),
            MetadataFault::ReadOnly => write!(f, "store is read-only"),
        }
    }
}

/// Whether a failed write's effect reached stable storage.
///
/// This is the type-level hook for **ambiguity** (fsyncgate): an error report
/// does not imply the data is absent, and a caller may resolve the ambiguity
/// only by crashing and booting from whatever the disk *actually* holds — the
/// recovery path must be correct for **both** outcomes of every
/// [`Unknown`](WriteOutcome::Unknown) write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The effect is known absent (the write never reached the device).
    Lost,
    /// Undecidable from here: the error was reported but the effect may be
    /// durable anyway, or was reported clean elsewhere yet lost. Neither
    /// "assume it landed" nor "assume it didn't" is safe.
    Unknown,
}

impl fmt::Display for WriteOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteOutcome::Lost => write!(f, "lost"),
            WriteOutcome::Unknown => write!(f, "outcome unknown"),
        }
    }
}

/// A durable-storage failure, typed: the *fault kind* is the variant, the
/// *record identity* and (for writes) the *durability outcome* are data.
///
/// The read-side [`paros_core::Storage`] recovery port stays infallible, but
/// every *write* — and the Stage-7 [`boot_scan`](LogStorage::boot_scan) — is
/// fallible so the storage-fault stages can inject `EIO` / fsync / corruption
/// faults through these signatures. `Display` stays human-readable; the
/// Stage-7 [`Corruption`](StorageError::Corruption) verdict is typed data
/// Stage 8's crash-relevance logic pattern-matches on — nothing downstream may
/// need to parse strings or rescan traces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// A write returned an I/O error (`EIO`). Per `outcome`, the caller may
    /// not assume the data is absent. (An `EIO` on a *read* is not this
    /// variant: it collapses into the corruption channel — CTRL §4.1,
    /// [`IntegrityFault::ReadError`] — one detection path, one classification
    /// path.)
    Io {
        /// The record the failed write was for.
        record: StorageRecord,
        /// Whether the write's effect is known lost or undecidable.
        outcome: WriteOutcome,
    },
    /// An fsync failed. Per `outcome`, the staged batch may be durable anyway
    /// (fsyncgate) or genuinely lost.
    FsyncFailed {
        /// The record identity the flush covered (usually
        /// [`StorageRecord::Batch`]).
        record: StorageRecord,
        /// Whether the staged batch's durability is known lost or undecidable.
        outcome: WriteOutcome,
    },
    /// A durable record failed its integrity check: the classified verdict of
    /// the Stage-7 detection layer. Which record, which fault family surfaced
    /// it, and the crash-vs-corruption disentanglement verdict all travel as
    /// data. Stage 7's only reaction is crash; Stage 8 pattern-matches on
    /// exactly this to recover.
    Corruption {
        /// The record that failed its integrity check.
        record: StorageRecord,
        /// The fault family the detector caught.
        fault: IntegrityFault,
        /// The crash-vs-corruption disentanglement verdict.
        verdict: CorruptionVerdict,
    },
    /// The record store itself is unusable at file granularity (FS metadata).
    /// Reliably crash — never attempt recovery on metadata, in Stage 8 either.
    Metadata {
        /// The file-granularity fault.
        fault: MetadataFault,
    },
}

impl StorageError {
    /// The record identity the fault hit.
    #[must_use]
    pub fn record(&self) -> StorageRecord {
        match self {
            StorageError::Io { record, .. }
            | StorageError::FsyncFailed { record, .. }
            | StorageError::Corruption { record, .. } => *record,
            StorageError::Metadata { .. } => StorageRecord::Store,
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Io { record, outcome } => {
                write!(f, "storage io error on {record} ({outcome})")
            }
            StorageError::FsyncFailed { record, outcome } => {
                write!(f, "storage fsync failed on {record} ({outcome})")
            }
            StorageError::Corruption {
                record,
                fault,
                verdict,
            } => {
                write!(f, "storage corruption on {record}: {fault} ({verdict})")
            }
            StorageError::Metadata { fault } => write!(f, "storage metadata fault: {fault}"),
        }
    }
}

impl std::error::Error for StorageError {}

/// The write side of node storage: **semantic per-record ops**, not a whole-blob
/// rewrite.
///
/// [`paros_core::Storage`] is the read-only recovery port — the core only ever
/// *reads back* durable state (at construction). The driver, which owns all
/// writes, applies each [`paros_core::WriteOp`] a [`paros_core::Ready`] surfaces
/// through the matching method here, then [`sync`](LogStorage::sync)s the batch
/// **before** sending its messages (the persist-before-send rule). Every write
/// returns [`Result`] so faults are injectable from the start.
///
/// # Async seam
///
/// Every method that may touch the device is **async**: the writes, the
/// flush, the boot scan. The driver awaits each one in the order the persist-before-send
/// pipeline dictates, so a disk-backed implementation blocks nothing but its
/// own node loop while the device works — the same provider-generic loop
/// runs over a simulated disk and a real one. The futures are `Send`
/// (declared as `impl Future + Send`, the moonpool provider convention), so
/// the loop that awaits them can be spawned on any executor; an
/// implementation writes plain `async fn`s.
///
/// The one accessor that reports what the store already **knows about
/// itself** — [`is_formatted`](LogStorage::is_formatted) — stays
/// synchronous, exactly like the core's [`Storage`] recovery port it sits
/// beside: it is answered from the index the boot scan built, never by a
/// device read. That is the contract a disk-backed store meets in
/// [`boot_scan`](LogStorage::boot_scan): it is the one place a store
/// loads and verifies its records, and everything the synchronous ports
/// answer afterwards is served from memory.
pub trait LogStorage: Storage {
    /// Boot-time integrity scan (Stage 7): verify every durable record and
    /// classify every mismatch **before** any byte reaches
    /// [`paros_core::ColocatedNode`]. The driver calls this once per incarnation,
    /// before constructing the core, so no corrupted bytes ever cross into
    /// protocol logic — the caller sees the typed outcome, never the bytes.
    /// It is also where a disk-backed store *loads*: the synchronous read
    /// ports ([`Storage`] and the accessors below) are answered from what
    /// this scan brought into memory.
    ///
    /// The durable-record contract this scan assumes (the CLStore-equivalent
    /// design; see `docs/analysis/storage/clstore-record-contract.md`):
    ///
    /// - **Every persisted record is checksummed**: each accepted entry, the
    ///   `HardState` scalars (promise + chosen index +
    ///   truncation floor), and the sealed journal state.
    /// - **Each log entry has an identifier physically separate from the
    ///   entry** — `⟨slot, accepted_ballot, offset, cksum⟩`, atomically
    ///   writable, itself checksummed. The identifier doubles as the entry's
    ///   persist witness (update protocol: `write(e_i); write(id_i);
    ///   fsync()`), and carries `offset` so one corrupt entry never ends the
    ///   ability to parse subsequent entries.
    /// - **Identity lives inside the checksummed region and is re-derived on
    ///   every read**: a record with a valid checksum but the wrong
    ///   slot/cluster is a *misdirected* read/write, its own detected outcome.
    ///   Validate the checksum before touching any other field.
    /// - **Absence is detectable**: every slot is formatted with a real,
    ///   checksummed reserved record carrying its own slot identity, so
    ///   all-zeros is always faulty, never "empty" — a lost write is never
    ///   indistinguishable from a never-written slot.
    /// - **Sanity backstop**: slot indices in the log are in order and
    ///   monotonically increasing — on `slot` only, never on the accepted
    ///   ballot (ballots are legitimately non-monotonic across slots in
    ///   Multi-Paxos).
    /// - **`HardState` keeps two local checksummed copies**: one copy bad ⇒
    ///   use the other and repair it; both bad ⇒ crash — the node cannot know
    ///   what it promised, and no peer can tell it.
    ///
    /// A scan may resolve a **crash-truncatable tail**
    /// ([`CorruptionVerdict::CrashTail`]) by discarding it locally — those
    /// records were never acknowledged to anyone — and may repair a single bad
    /// `HardState` copy from its twin. Everything else is detection only:
    /// return the classified [`StorageError::Corruption`] (or
    /// [`StorageError::Metadata`]) and let the driver take its crash decision.
    /// **Never truncate on a corruption verdict** (CTRL Figure 2: the
    /// truncate-on-mismatch bug silently erases committed data cluster-wide).
    ///
    /// The default implementation reports a clean store, for in-memory
    /// storage that cannot rot.
    ///
    /// # Errors
    /// Returns the first classified [`StorageError`] whose verdict requires a
    /// crash.
    fn boot_scan(&mut self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async { Ok(()) }
    }

    /// Whether this store carries the **format marker** (#147): the durable
    /// proof that the identity this store belongs to has been provisioned —
    /// written once by [`format`](LogStorage::format) on the identity's
    /// first boot, before any protocol state, and never removed. The driver
    /// judges the operator's [`BootKind`](crate::BootKind) claim against it:
    /// an existing member whose store has no marker has lost its disk, and
    /// its durable promise with it, and is refused rather than rejoined.
    /// Synchronous, answered from what the boot scan loaded, like every
    /// accessor that reports what a store knows about itself.
    fn is_formatted(&self) -> bool;

    /// Write the format marker (#147). Staged like every other write and
    /// durable at the next [`sync`](LogStorage::sync); the driver syncs it
    /// alone, on a first boot, before the core reads the store, so the
    /// marker is on disk no later than the first promise. Nothing but this
    /// method writes it, and nothing removes it.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn format(&mut self) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Persist a raised promised ballot (Phase 1).
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn persist_ballot(
        &mut self,
        ballot: Ballot,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Persist the `(ballot, command)` accepted for `slot` (Phase 2). An
    /// upsert-by-slot (a chosen value overwrites a stale accept).
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Advance the durable chosen index (commit index) to `slot`.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn set_chosen_index(
        &mut self,
        slot: Slot,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Flush this batch's writes to stable storage. A [`MustSync::Sync`] batch
    /// (promise-raise / accepted-append) must be fsync-durable on return; a
    /// [`MustSync::Relaxed`] batch (chosen-index-only) may skip the fsync — its
    /// effect is safely re-derivable after a crash.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the flush fails.
    fn sync(
        &mut self,
        must_sync: MustSync,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Truncate the log below `first`, discarding the compacted prefix, and
    /// record `first` as the durable compaction floor (returned by
    /// [`Storage::first_slot`] after a restart). A decided `Truncate` drives
    /// this via [`paros_core::ColocatedNode::compact`], which only ever names slots within the
    /// chosen prefix, so nothing undecided is dropped. `sealed` is the journal
    /// state the dropped slots folded to (#204); persist it durably with the
    /// floor so [`Storage::sealed_state`] returns it after a restart — losing
    /// it would let a restarted node fold the retained log from the wrong
    /// writer and the wrong next position.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn truncate(
        &mut self,
        first: Slot,
        sealed: JournalState,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Jump below a peer's trim point (#186, [`paros_core::WriteOp::TrimmedTo`]):
    /// record `point` as the durable compaction floor, raise the durable
    /// chosen index to at least `point - 1` (everything below a trim point is
    /// chosen), drop every record below `point`, and persist `state` as the
    /// sealed journal state, exactly like [`LogStorage::truncate`]'s
    /// `sealed`. The promise does not move.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn trimmed_to(
        &mut self,
        point: Slot,
        state: JournalState,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;
}

mod contract;
mod mem;

pub use contract::storage_contract_suite;
pub use mem::MemStorage;
