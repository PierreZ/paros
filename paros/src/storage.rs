//! Node storage — the read-only [`Storage`] recovery port (from `paros-core`)
//! plus the [`NodeStorage`] write extension the driver persists through, and the
//! default in-memory [`MemStorage`] implementing both.

use std::collections::BTreeMap;
use std::fmt;

use paros_core::{Ballot, Config, Entry, HardState, MustSync, Slot, Storage};

/// A durable-write failure. The read-side [`paros_core::Storage`] recovery port
/// stays infallible, but every *write* is fallible **from day one** so a later
/// storage-fault stage can inject `EIO` / torn-write / fsync failures through
/// these signatures without a second trait redesign.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// An I/O error (a lost/failed write, a failed fsync).
    Io(String),
    /// A durable record failed its integrity check (Stage 7 checksums).
    Corruption(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Io(m) => write!(f, "storage io error: {m}"),
            StorageError::Corruption(m) => write!(f, "storage corruption: {m}"),
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
/// through the matching method here, then [`sync`](NodeStorage::sync)s the batch
/// **before** sending its messages (the persist-before-send rule). Every write
/// returns [`Result`] so faults are injectable from the start.
pub trait NodeStorage: Storage {
    /// Persist a raised promised ballot (Phase 1).
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError>;

    /// Persist the `(ballot, entry)` accepted for `slot` (Phase 2). An
    /// upsert-by-slot (a chosen value overwrites a stale accept).
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        entry: Entry,
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
    /// chosen prefix, so nothing undecided is dropped.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn truncate(&mut self, first: Slot) -> Result<(), StorageError>;
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
    accepted: BTreeMap<Slot, (Ballot, Entry)>,
    config: Config,
    /// The compaction floor: the first slot still retained. Everything below it
    /// has been truncated away.
    first: Slot,
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
        }
    }
}

impl NodeStorage for MemStorage {
    fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.hard_state.max_promised_ballot = ballot;
        Ok(())
    }

    fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        entry: Entry,
    ) -> Result<(), StorageError> {
        self.accepted.insert(slot, (ballot, entry));
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

    fn truncate(&mut self, first: Slot) -> Result<(), StorageError> {
        self.first = self.first.max(first);
        self.accepted.retain(|slot, _| *slot >= self.first);
        Ok(())
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state.clone(), self.config.clone())
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Entry)> {
        self.accepted.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        self.first
    }

    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }
}
