//! Node storage — the read-only [`Storage`] recovery port (from `paros-core`)
//! plus the [`NodeStorage`] write extension the driver persists through, and the
//! default in-memory [`MemStorage`] implementing both.

use std::collections::BTreeMap;
use std::fmt;

use paros_core::{
    Ballot, Command, Config, ConfigId, HardState, MustSync, SessionEntry, Slot, Storage,
};

use crate::corruption::{CorruptionKind, CorruptionVerdict, MetadataFault, RecoveryCase};

/// The durable record a storage operation (and therefore a storage fault) hit.
///
/// Carried as **data** on every [`StorageError`] so Stage 7's detect-and-classify
/// and Stage 8's crash-relevance decisions can *match* on the record identity,
/// and so the simulation can correlate injected fault ↔ surfaced error ↔ node
/// reaction without string parsing. New identities slot in as plain variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRecord {
    /// The durable cluster-configuration identity scalar.
    ConfigId,
    /// The promised-ballot scalar (the `HardState` promise).
    Promise,
    /// The accepted `(ballot, command)` entry at this slot.
    Accepted(Slot),
    /// The chosen-index (commit index) scalar.
    ChosenIndex,
    /// The truncation record (the durable compaction floor + sealed sessions).
    Truncation,
    /// The installed opaque application snapshot.
    Snapshot,
    /// The staged application transition at this slot (the apply seam).
    Application(Slot),
    /// The whole staged batch: an fsync flushes every record staged since the
    /// last flush, so a failed fsync has no single-record identity.
    Batch,
    /// One of the two checksummed metainfo (`HardState`) copies, by copy index
    /// (0 or 1). Only the Stage-7 detection layer names a copy — writes update
    /// the metainfo through the scalar records above; reads verify both copies
    /// and repair a single bad one from its twin (see
    /// [`crate::corruption::decide_metainfo`]).
    Metainfo(u8),
}

impl fmt::Display for StorageRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageRecord::ConfigId => write!(f, "config-id"),
            StorageRecord::Promise => write!(f, "promise"),
            StorageRecord::Accepted(slot) => write!(f, "accepted[{}]", slot.0),
            StorageRecord::ChosenIndex => write!(f, "chosen-index"),
            StorageRecord::Truncation => write!(f, "truncation"),
            StorageRecord::Snapshot => write!(f, "snapshot"),
            StorageRecord::Application(slot) => write!(f, "application[{}]", slot.0),
            StorageRecord::Batch => write!(f, "batch"),
            StorageRecord::Metainfo(copy) => write!(f, "metainfo[{copy}]"),
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

/// A durable-write failure, typed: the *fault kind* is the variant, the
/// *record identity* and (for writes) the *durability outcome* are data.
///
/// The read-side [`paros_core::Storage`] recovery port stays infallible, but
/// every *write* is fallible so the storage-fault stages can inject `EIO` /
/// fsync / corruption faults through these signatures. `Display` stays
/// human-readable; later stages add variants (Stage 7 grows sub-structure
/// under [`Corruption`](StorageError::Corruption)) without reshaping these.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// A write returned an I/O error (`EIO`). Per `outcome`, the caller may
    /// not assume the data is absent.
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
    /// A durable record failed its integrity check — the **classified
    /// verdict** the Stage-7 detection layer produces (issue #20 item B):
    /// which record, which fault family surfaced it, and the crash-vs-
    /// corruption disentanglement verdict. Stage 8's crash-relevance logic
    /// pattern-matches on exactly this; nothing downstream parses strings or
    /// rescans traces. The named [`RecoveryCase`] travels on the tracing
    /// event beside this error.
    Corruption {
        /// The record that failed its integrity check.
        record: StorageRecord,
        /// The fault family the evidence points at.
        kind: CorruptionKind,
        /// The disentanglement verdict. Never
        /// [`CorruptionVerdict::CrashTail`]: a crash-truncatable tail is
        /// *handled* (discarded) by the boot scan, not surfaced as an error.
        verdict: CorruptionVerdict,
    },
    /// A filesystem-metadata fault at file granularity (issue #20 item E):
    /// store missing/unopenable, wrong size, or read-only. No per-record
    /// witness exists to disentangle, so the reaction is **reliably crash**
    /// — never attempt recovery on metadata, in Stage 8 either.
    Metadata {
        /// The file-level fault observed.
        fault: MetadataFault,
    },
}

impl StorageError {
    /// The record identity the fault hit. A metadata fault is at file
    /// granularity — the whole store — so it reports [`StorageRecord::Batch`].
    #[must_use]
    pub fn record(&self) -> StorageRecord {
        match self {
            StorageError::Io { record, .. }
            | StorageError::FsyncFailed { record, .. }
            | StorageError::Corruption { record, .. } => *record,
            StorageError::Metadata { .. } => StorageRecord::Batch,
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
                kind,
                verdict,
            } => {
                write!(f, "storage corruption on {record}: {kind} ({verdict})")
            }
            StorageError::Metadata { fault } => {
                write!(f, "storage metadata fault: {fault}")
            }
        }
    }
}

impl std::error::Error for StorageError {}

/// What the boot-time integrity scan did (issue #20 item B): the benign,
/// *handled* outcomes the driver reports through the audit. A fatal outcome is
/// never in here — it surfaces as the scan's [`StorageError`] instead.
///
/// The scan is the write-side flush ordering's read-back pair (the assertion
/// doctrine's two-path rule): every durable record is verified on recovery,
/// the crash-truncatable tail is discarded per the disentanglement rule, a
/// single bad metainfo copy is repaired from its twin — and in Stage 7 any
/// remaining faulty record crashes the node before a byte of it can reach
/// [`paros_core`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BootReport {
    /// Crash-truncatable tail records the scan discarded, in slot order, each
    /// with its named recovery case. Discard is legal **only** under the
    /// [`CorruptionVerdict::CrashTail`](crate::corruption::CorruptionVerdict)
    /// verdict: the identifier's absence proves the update's fsync never
    /// completed, so the record was never acknowledged to anyone.
    pub tail_discarded: Vec<(Slot, RecoveryCase)>,
    /// The durable chosen index the discard was checked against (the provably
    /// certain head): every discarded slot is strictly above it. Reported so
    /// the audit can re-assert the bound independently of the classifier.
    pub certain_head: Option<Slot>,
    /// A single bad metainfo (`HardState`) copy was rewritten from its valid
    /// twin (CTRL metainfo doctrine); carries the repaired copy's index.
    pub metainfo_repaired: Option<u8>,
}

impl BootReport {
    /// Whether the scan found nothing to handle.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.tail_discarded.is_empty() && self.metainfo_repaired.is_none()
    }
}

/// The write side of node storage: **semantic per-record ops**, not a whole-blob
/// rewrite.
///
/// [`paros_core::Storage`] is the read-only recovery port — the core only ever
/// *reads back* durable state (at construction). The driver, which owns all
/// writes, applies each [`paros_core::WriteOp`] a [`paros_core::Ready`] surfaces
/// through the matching method here, then [`sync`](NodeStorage::sync)s the batch
/// **before** sending its messages (the persist-before-send rule). Every write
/// returns [`Result`] so faults are injectable from the start.
///
/// # Durable-record contract (Stage 7, issue #20 item A)
///
/// A durable implementation must uphold the CLStore-equivalent record contract
/// (`docs/analysis/storage/record-contract.md` is the full spec; the
/// [`crate::corruption`] module is the classifier over its read-back
/// evidence):
///
/// - **Every persisted record is checksummed**: each accepted entry, the
///   snapshot, the metainfo (`HardState`: promise + chosen index + truncation
///   floor + configuration identity), and the sealed-sessions ledger.
/// - **Each log entry carries a physically separate identifier**
///   `⟨slot, accepted_ballot, offset, cksum⟩` — atomically writable, itself
///   checksummed — written *after* the entry, with **one** fsync covering
///   both. The identifier is the entry's persist witness for
///   crash-vs-corruption disentanglement, and its `offset` keeps one corrupt
///   entry from ending the ability to parse subsequent entries.
/// - **Identity lives inside the checksummed region and is re-derived on
///   every read** (validate the checksum before touching any other field): a
///   valid-checksum record answering for the wrong slot is a *misdirected*
///   read/write, its own detected outcome.
/// - **Absence is detectable**: every slot is formatted with a real,
///   checksummed reserved record carrying its own slot identity, so all-zeros
///   is always invalid — faulty, never "empty" — and a lost write can never
///   masquerade as a never-written slot.
/// - **Slot indices in the physical log are strictly increasing** — a parse-
///   time backstop against block-aligned misdirects, on `slot` only, never on
///   the accepted ballot (ballots are legitimately non-monotonic across slots
///   in Multi-Paxos).
/// - **The metainfo keeps two local checksummed copies**: one bad ⇒ read the
///   other and repair; both bad ⇒ crash.
///
/// [`boot_scan`](NodeStorage::boot_scan) is where the contract is enforced on
/// recovery; no bytes that fail verification may ever cross this trait into
/// the core or the driver's protocol logic.
pub trait NodeStorage: Storage {
    /// Verify every durable record on recovery and disentangle crash artifacts
    /// from corruption (issue #20 items B/C). Called by the driver **before**
    /// the core reads a single byte of this storage:
    ///
    /// - a crash-truncatable tail (per [`crate::corruption::classify_log`]) is
    ///   discarded and reported in the [`BootReport`];
    /// - a single bad metainfo copy is repaired from its twin and reported;
    /// - any remaining faulty record is fatal: the scan returns the classified
    ///   [`StorageError::Corruption`] (or [`StorageError::Metadata`]) and the
    ///   driver crashes the node — in Stage 7, detect ⇒ crash, never truncate.
    ///
    /// An unreadable record (`EIO` on read) is degraded to zero-fill-then-
    /// mismatch and classified through the same channel, stamped
    /// [`crate::corruption::CorruptionKind::ReadIo`].
    ///
    /// The default implementation reports a clean scan: correct for storages
    /// with no durable medium to corrupt (the in-memory [`MemStorage`]); a
    /// durable engine MUST override it.
    ///
    /// # Errors
    /// Returns the classified [`StorageError`] for the first fault that is
    /// neither a discardable crash artifact nor a repairable metainfo copy.
    fn boot_scan(&mut self) -> Result<BootReport, StorageError> {
        Ok(BootReport::default())
    }
    /// Persist the durable cluster configuration identity.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn persist_config_id(&mut self, config_id: ConfigId) -> Result<(), StorageError>;

    /// Persist a raised promised ballot (Phase 1).
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError>;

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
    ) -> Result<(), StorageError>;

    /// Advance the durable chosen index (commit index) to `slot`.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError>;

    /// Flush this batch's writes to stable storage. A [`MustSync::Sync`] batch
    /// (promise-raise / accepted-append) must be fsync-durable on return; a
    /// [`MustSync::Relaxed`] batch (chosen-index-only) may skip the fsync — its
    /// effect is safely re-derivable after a crash.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the flush fails.
    fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError>;

    /// Truncate the log below `first`, discarding the compacted prefix, and
    /// record `first` as the durable compaction floor (returned by
    /// [`Storage::first_slot`] after a restart). The application drives this via
    /// [`paros_core::RawNode::compact`], which only ever names slots within the
    /// chosen prefix, so nothing undecided is dropped. `sealed` carries the
    /// at-most-once ledger records whose slots this truncation drops; persist
    /// them durably (upsert by `(client, seq)`) so [`Storage::sealed_sessions`]
    /// returns them after a restart — losing them would let a restarted node
    /// re-execute a truncated identity every peer suppresses (#94).
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn truncate(&mut self, first: Slot, sealed: &[SessionEntry]) -> Result<(), StorageError>;

    /// The opaque application snapshot at this node's chosen prefix, for serving a
    /// below-floor peer. The **application** owns its meaning; paros only transfers
    /// the bytes. The core never calls this — only the driver, when it fills a
    /// [`paros_core::Ready::snapshot_offers`] offer with bytes before sending an
    /// [`paros_core::Message::InstallSnapshot`].
    ///
    /// Fallible because it is a durable-record **read**: the snapshot is a
    /// first-class corruption target with its own checksum (#71), verified on
    /// every read, and a mismatch surfaces here as the classified
    /// [`StorageError::Corruption`] — corrupt snapshot bytes must never be
    /// served to a peer.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the stored snapshot fails its integrity
    /// check or cannot be read.
    fn snapshot(&self) -> Result<Vec<u8>, StorageError>;

    /// Install an opaque application snapshot at `chosen_index`: set the durable
    /// commit index, raise the promise to at least `ballot`, record
    /// `chosen_index + 1` as the compaction floor, and persist `snapshot` (so a
    /// restart boots from it and the node can serve it onward). `sessions` is
    /// the serving peer's at-most-once ledger for the folded prefix; persist it
    /// as sealed records (upsert), exactly like [`NodeStorage::truncate`]'s
    /// `sealed`. Mirrors [`paros_core::WriteOp::InstallSnapshot`].
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn install_snapshot(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        snapshot: Vec<u8>,
        sessions: &[SessionEntry],
    ) -> Result<(), StorageError>;

    /// Stage one newly chosen command for durable application. Implementations
    /// must be idempotent by `slot`: a reboot may replay retained chosen records
    /// after the consensus chosen index reached disk but application effects did
    /// not. The driver flushes the staged application batch before acknowledging
    /// clients.
    ///
    /// `chosen_index` is the core's contiguous chosen prefix for the batch and is
    /// supplied so an application adapter can assert it never applies ahead of
    /// consensus.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the application transition cannot be staged.
    fn apply(
        &mut self,
        chosen_index: Slot,
        slot: Slot,
        command: &Command,
    ) -> Result<(), StorageError>;

    /// Highest slot durably reflected in the application snapshot, if the
    /// application tracks one. Used only to make reboot replay idempotent.
    fn applied_slot(&self) -> Option<Slot>;
}

/// The library's default in-memory storage: enough to *construct* a
/// [`paros_core::RawNode`] and to receive the semantic writes the driver makes
/// while draining a [`paros_core::Ready`]. The durable scalars and the per-slot
/// accepted log are stored separately (never a single blob).
///
/// A crash-testable faulty fake (fail-stop, corruption, protocol-aware recovery)
/// arrives with the storage-fault milestone (Stage 6); the driver is generic over
/// [`NodeStorage`], so it swaps in without touching the loop.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    config: Config,
    /// The compaction floor: the first slot still retained. Everything below it
    /// has been truncated away.
    first: Slot,
    /// Sealed at-most-once ledger records for truncated slots, keyed by
    /// `(client, seq)` (see [`NodeStorage::truncate`]).
    sealed: BTreeMap<(paros_core::ClientId, paros_core::ClientSeq), Slot>,
}

impl MemStorage {
    /// A fresh, empty storage for a node with the given identity/membership.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            hard_state: HardState::default(),
            accepted: BTreeMap::new(),
            config,
            first: Slot(0),
            sealed: BTreeMap::new(),
        }
    }

    fn seal(&mut self, sealed: &[SessionEntry]) {
        for &(client, seq, slot) in sealed {
            self.sealed.entry((client, seq)).or_insert(slot);
        }
    }
}

impl NodeStorage for MemStorage {
    fn persist_config_id(&mut self, config_id: ConfigId) -> Result<(), StorageError> {
        self.hard_state.config_id = config_id;
        Ok(())
    }

    fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.hard_state.max_promised_ballot = ballot;
        Ok(())
    }

    fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        self.accepted.insert(slot, (ballot, command));
        Ok(())
    }

    fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.hard_state.chosen_index = Some(slot);
        Ok(())
    }

    fn sync(&mut self, _must_sync: MustSync) -> Result<(), StorageError> {
        // In-memory: writes are already visible; nothing to flush.
        Ok(())
    }

    fn truncate(&mut self, first: Slot, sealed: &[SessionEntry]) -> Result<(), StorageError> {
        self.seal(sealed);
        self.first = self.first.max(first);
        self.accepted.retain(|slot, _| *slot >= self.first);
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, StorageError> {
        // The default in-memory storage has no application state machine, so its
        // opaque "snapshot" is a deterministic marker of the chosen prefix. A real
        // application supplies a NodeStorage whose snapshot() folds its own state.
        Ok(self
            .hard_state
            .chosen_index
            .map_or_else(Vec::new, |ci| ci.0.to_le_bytes().to_vec()))
    }

    fn install_snapshot(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        _snapshot: Vec<u8>,
        sessions: &[SessionEntry],
    ) -> Result<(), StorageError> {
        self.seal(sessions);
        self.hard_state.chosen_index = Some(chosen_index);
        self.hard_state.max_promised_ballot = self.hard_state.max_promised_ballot.max(ballot);
        let first = Slot(chosen_index.0 + 1);
        self.first = self.first.max(first);
        self.accepted.retain(|slot, _| *slot >= self.first);
        Ok(())
    }

    fn apply(
        &mut self,
        _chosen_index: Slot,
        _slot: Slot,
        _command: &Command,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn applied_slot(&self) -> Option<Slot> {
        self.hard_state.chosen_index
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state.clone(), self.config.clone())
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.accepted.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        self.first
    }

    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }

    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        self.sealed
            .iter()
            .map(|(&(client, seq), &slot)| (client, seq, slot))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::{ConfigId, NodeId, QuorumSystem};

    #[test]
    fn config_id_round_trips_through_storage() {
        let mut storage = MemStorage::new(Config {
            id: NodeId(1),
            peers: vec![NodeId(1)],
            quorum_system: QuorumSystem::Majority,
        });

        storage
            .persist_config_id(ConfigId(17))
            .expect("persist configuration identity");
        storage
            .sync(MustSync::Sync)
            .expect("sync configuration identity");

        assert_eq!(storage.initial_state().0.config_id, ConfigId(17));
    }
}
