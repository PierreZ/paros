//! Node storage — the read-only [`Storage`] recovery port (from `paros-core`)
//! plus the [`NodeStorage`] write extension the driver persists through, and the
//! default in-memory [`MemStorage`] implementing both.

use std::collections::BTreeMap;
use std::fmt;

use paros_core::{
    Ballot, Command, Config, ConfigId, HardState, MustSync, SessionEntry, Slot, Storage,
};

use crate::corruption::{CorruptionVerdict, IntegrityFault};

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
    /// The record store itself, at file granularity (FS metadata): the
    /// identity a [`StorageError::Metadata`] fault names, since a missing or
    /// unopenable store has no single-record identity either.
    Store,
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
    /// preallocated and the snapshot's size is stored separately).
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
/// every *write* — and the Stage-7 [`boot_scan`](NodeStorage::boot_scan) — is
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
/// through the matching method here, then [`sync`](NodeStorage::sync)s the batch
/// **before** sending its messages (the persist-before-send rule). Every write
/// returns [`Result`] so faults are injectable from the start.
pub trait NodeStorage: Storage {
    /// Boot-time integrity scan (Stage 7): verify every durable record and
    /// classify every mismatch **before** any byte reaches
    /// [`paros_core::RawNode`]. The driver calls this once per incarnation,
    /// before constructing the core, so no corrupted bytes ever cross into
    /// protocol logic — the caller sees the typed outcome, never the bytes.
    ///
    /// The durable-record contract this scan assumes (the CLStore-equivalent
    /// design; see `docs/analysis/storage/clstore-record-contract.md`):
    ///
    /// - **Every persisted record is checksummed**: each accepted entry, the
    ///   snapshot, the `HardState` scalars (promise + chosen index +
    ///   truncation floor), and the sealed-sessions ledger.
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
    fn boot_scan(&mut self) -> Result<(), StorageError> {
        Ok(())
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
    fn snapshot(&self) -> Vec<u8>;

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

    fn snapshot(&self) -> Vec<u8> {
        // The default in-memory storage has no application state machine, so its
        // opaque "snapshot" is a deterministic marker of the chosen prefix. A real
        // application supplies a NodeStorage whose snapshot() folds its own state.
        self.hard_state
            .chosen_index
            .map_or_else(Vec::new, |ci| ci.0.to_le_bytes().to_vec())
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

/// The behavioral **contract suite** every [`NodeStorage`] implementation must
/// pass (issue #21 item F): one suite, run against both [`MemStorage`] (here)
/// and the simulation's world-backed storage (in `paros-sim`), so a fake can
/// never drift from the trait contract. The Stage-6/7 fault-budget logic stays
/// *outside* this contract (#70): from the trait's point of view both
/// implementations behave identically on the clean path this suite drives.
///
/// `fresh` must return an empty storage for the same single-node membership on
/// every call. `reopen` simulates a clean reboot of the same store: the reads
/// the suite asserts are the *recovery port's* (the core reads durable state
/// once, at construction), so every read-back goes through a reopen — an
/// in-memory implementation may return the same handle, a world-backed one
/// re-restores from its durable records.
///
/// # Panics
///
/// Panics on any contract violation.
#[doc(hidden)]
// One linear behavioral walk; splitting it would scatter the contract.
#[allow(clippy::too_many_lines)]
pub fn storage_contract_suite<S: NodeStorage>(
    mut fresh: impl FnMut() -> S,
    mut reopen: impl FnMut(S) -> S,
) {
    use paros_core::{ClientId, ClientSeq, Entry, Value};
    let ballot = |round: u64| Ballot {
        round,
        node: paros_core::NodeId(0),
    };
    let user = |seq: u64, byte: u8| {
        Command::User(Entry {
            client: ClientId(7),
            seq: ClientSeq(seq),
            value: Value(vec![byte]),
        })
    };

    // Scalars + per-slot records round-trip through a Sync flush.
    let mut s = fresh();
    s.persist_config_id(ConfigId(3)).expect("config id");
    s.persist_ballot(ballot(4)).expect("ballot");
    s.append_accepted(Slot(0), ballot(4), user(1, 0xa))
        .expect("append 0");
    s.append_accepted(Slot(1), ballot(4), user(2, 0xb))
        .expect("append 1");
    s.set_chosen_index(Slot(1)).expect("chosen index");
    s.sync(MustSync::Sync).expect("sync");
    let mut s = reopen(s);
    let (hs, _config) = s.initial_state();
    assert_eq!(hs.config_id, ConfigId(3), "config id round-trips");
    assert_eq!(hs.max_promised_ballot, ballot(4), "promise round-trips");
    assert_eq!(hs.chosen_index, Some(Slot(1)), "chosen index round-trips");
    assert_eq!(s.first_slot(), Slot(0), "floor starts at zero");
    assert_eq!(s.last_slot(), Slot(1), "last slot reflects the appends");
    assert_eq!(
        s.accepted(Slot(0)).map(|(b, _)| b),
        Some(ballot(4)),
        "an accepted record reads back"
    );
    assert!(
        s.faulty_entries().is_empty(),
        "a clean store reports no rot"
    );

    // An append is an upsert-by-slot: the newer record replaces the older.
    s.append_accepted(Slot(1), ballot(5), user(3, 0xc))
        .expect("re-append 1");
    s.sync(MustSync::Sync).expect("sync upsert");
    let mut s = reopen(s);
    assert_eq!(
        s.accepted(Slot(1)).map(|(b, _)| b),
        Some(ballot(5)),
        "append is an upsert by slot"
    );

    // Truncation raises the floor, drops the prefix, and seals the ledger.
    s.truncate(Slot(1), &[(ClientId(7), ClientSeq(1), Slot(0))])
        .expect("truncate");
    s.sync(MustSync::Sync).expect("sync truncate");
    let mut s = reopen(s);
    assert_eq!(s.first_slot(), Slot(1), "the floor rose");
    assert!(
        s.accepted(Slot(0)).is_none(),
        "a truncated record is unreadable"
    );
    assert_eq!(
        s.sealed_sessions(),
        vec![(ClientId(7), ClientSeq(1), Slot(0))],
        "the sealed ledger survives the truncation"
    );
    // A floor never moves backward.
    s.truncate(Slot(0), &[]).expect("re-truncate lower");
    s.sync(MustSync::Sync).expect("sync no-op truncate");
    let s = reopen(s);
    assert_eq!(s.first_slot(), Slot(1), "the floor is monotone");

    // A snapshot install: chosen index jumps, promise takes the max (never
    // regresses), floor lands one past the boundary, sessions seal. The blob
    // comes from a *source* storage whose applied prefix genuinely covers the
    // boundary, so an application-typed implementation's boundary checks hold.
    let mut source = fresh();
    for s in 0..=4u64 {
        source
            .append_accepted(Slot(s), ballot(1), user(10 + s, 0x40))
            .expect("source append");
    }
    source.set_chosen_index(Slot(4)).expect("source index");
    for s in 0..=4u64 {
        source
            .apply(Slot(4), Slot(s), &user(10 + s, 0x40))
            .expect("source apply");
    }
    source.sync(MustSync::Sync).expect("source sync");
    let blob = source.snapshot();

    let mut s = fresh();
    s.persist_ballot(ballot(9)).expect("high promise");
    s.sync(MustSync::Sync).expect("sync promise");
    s.install_snapshot(
        Slot(4),
        ballot(2),
        blob,
        &[(ClientId(7), ClientSeq(2), Slot(3))],
    )
    .expect("install");
    s.sync(MustSync::Sync).expect("sync install");
    let s = reopen(s);
    let (hs, _config) = s.initial_state();
    assert_eq!(hs.chosen_index, Some(Slot(4)), "the install set the index");
    assert_eq!(
        hs.max_promised_ballot,
        ballot(9),
        "an install never lowers the promise"
    );
    assert_eq!(
        s.first_slot(),
        Slot(5),
        "the floor is one past the boundary"
    );
    assert_eq!(
        s.sealed_sessions(),
        vec![(ClientId(7), ClientSeq(2), Slot(3))],
        "the install sealed the peer's ledger"
    );
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

    /// The shared behavioral contract, against the library's default storage.
    /// The simulation runs the same suite against its world-backed storage.
    #[test]
    fn mem_storage_passes_the_contract_suite() {
        storage_contract_suite(
            || {
                MemStorage::new(Config {
                    id: NodeId(0),
                    peers: vec![NodeId(0)],
                    quorum_system: QuorumSystem::Majority,
                })
            },
            // In-memory writes are immediately visible: a reboot is the same
            // handle.
            |s| s,
        );
    }
}
