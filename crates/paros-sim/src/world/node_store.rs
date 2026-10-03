//! An acceptor's store, whichever the seed drew (#187): the world-backed
//! [`DurableStorage`] every seed ran on until now, or the library's shipped
//! [`JournalStorage`] over the simulated disk (`SimStorageProvider`).
//!
//! A journal seed runs the store a real deployment runs, under every fault,
//! disk corruption included: moonpool's storage chaos on the simulated
//! disk, spread over the acceptors by a replicated fault pattern
//! (`crate::chain_builder`) that keeps every record's copies damaged in at
//! most one node at once — the world's copy budget, enforced by the
//! simulator. [`LedgeredJournal`] is the store's boundary with the world:
//!
//! - the operator's **provisioning ledger** (#147): the journal's format
//!   marker lands only with the sync after the format, so the ledger
//!   records the provisioning in two steps — begun at the format, landed
//!   when that sync returns. A kill, or a failed sync that quarantines the
//!   journal, in between leaves the operator honestly unsure; the next open
//!   (a reboot or a quarantine's re-open alike) claims an existing member
//!   and the boot scan settles it from the disk: a store that carries the
//!   marker was provisioned, and one that does not gets the interrupted
//!   provisioning finished — the format and sync a first boot would run;
//! - the **fault ledger**: every I/O error and every corruption verdict the
//!   store surfaced is counted, so "exactly one typed crash decision" still
//!   binds; an integrity verdict is persistent (the boot scan would find
//!   the same damage again), so it parks the node, as the world's own
//!   detect ⇒ crash does, while an I/O error the open reported as
//!   corruption only crashes it;
//! - the **recovery declaration**: a node holding a faulty vote has lost a
//!   record its disk may no longer show (a checkpoint rewrites the marker
//!   onto clean sectors), so it tells moonpool it is still recovering, and
//!   the rolling pattern's turn waits for the cluster to give the record
//!   back before damaging another node. A parked journal declares it for
//!   good (`hold_turn_for_good`).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use moonpool_sim::{SimStorageProvider, SimTimeProvider, assert_reachable};
use paros::{
    Ballot, Command, Config, HardState, IntegrityFault, JournalState, JournalStorage, LogStorage,
    MustSync, RunError, Slot, Storage, StorageError,
};

use super::StorageWorld;
use super::storage::DurableStorage;

/// The journals of one node incarnation still holding a faulty vote: the
/// node is recovering while any is (see the module doc).
pub(crate) type Recovering = Arc<Mutex<BTreeSet<paros::JournalKey>>>;

/// A parked journal never recovers: its node holds the rolling turn for
/// the rest of the run, whatever its disk shows (a verdict can come from a
/// transient fault, a misdirected read, that left the disk clean).
pub(crate) fn hold_turn_for_good(
    disk: &SimStorageProvider,
    recovering: &Recovering,
    key: paros::JournalKey,
) {
    recovering
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(key);
    // Only a shut-down simulation refuses, and then nothing is left to
    // damage.
    let _ = disk.set_recovering(true);
}

/// The journal store at its boundary with the world (see the module doc).
pub(crate) struct LedgeredJournal {
    inner: JournalStorage<SimStorageProvider>,
    world: Weak<Mutex<StorageWorld>>,
    ip: String,
    /// The node's rank, as the world's parking ledger keys it.
    rank: u64,
    /// The node's disk, which the recovery declaration goes to.
    disk: SimStorageProvider,
    key: paros::JournalKey,
    recovering: Recovering,
    /// A format was staged and its sync has not returned yet.
    format_pending: bool,
}

impl LedgeredJournal {
    pub(crate) fn new(
        inner: JournalStorage<SimStorageProvider>,
        world: Weak<Mutex<StorageWorld>>,
        ip: String,
        rank: u64,
        disk: SimStorageProvider,
        (key, recovering): (paros::JournalKey, Recovering),
    ) -> Self {
        Self {
            inner,
            world,
            ip,
            rank,
            disk,
            key,
            recovering,
            format_pending: false,
        }
    }

    /// Tell moonpool whether this node still holds a faulty vote in any of
    /// its journals.
    fn declare_recovery(&self) {
        let faulty = !self.inner.faulty_entries().is_empty();
        let mut journals = self
            .recovering
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let changed = if faulty {
            journals.insert(self.key)
        } else {
            journals.remove(&self.key)
        };
        if changed {
            if faulty {
                assert_reachable!("journal store: a node declares a faulty vote still recovering");
            }
            // Only a shut-down simulation refuses, and then nothing is
            // left to damage.
            let _ = self.disk.set_recovering(!journals.is_empty());
        }
    }

    fn with_world(&self, f: impl FnOnce(&mut StorageWorld)) {
        if let Some(world) = self.world.upgrade() {
            f(&mut world.lock().unwrap_or_else(PoisonError::into_inner));
        }
    }

    /// Whether the operator's provisioning of this node was interrupted.
    fn provisioning_ambiguous(&self) -> bool {
        let mut ambiguous = false;
        self.with_world(|w| ambiguous = w.provisioning_ambiguous(&self.ip));
        ambiguous
    }

    /// Count a fault the simulated disk handed back, the journal seed's
    /// fault ledger: the driver must surface each one as exactly one crash
    /// decision. An I/O fault is transient; a corruption verdict is the
    /// damage the boot scan would find again, so it parks the node.
    fn ledger<T>(&self, result: Result<T, StorageError>) -> Result<T, StorageError> {
        match &result {
            Err(StorageError::Io { .. } | StorageError::FsyncFailed { .. }) => {
                self.with_world(StorageWorld::note_disk_fault);
            }
            // An I/O error while the journal opened (a read EIO, a read
            // a crash cut short) is reported as corruption, but a reboot can
            // read past it: one crash decision, no park.
            Err(StorageError::Corruption {
                fault: IntegrityFault::ReadError,
                ..
            }) => self.with_world(|w| w.note_disk_corruption(None)),
            Err(StorageError::Corruption { .. } | StorageError::Metadata { .. }) => {
                let (ip, rank) = (self.ip.clone(), self.rank);
                self.with_world(|w| w.note_disk_corruption(Some((&ip, rank))));
                hold_turn_for_good(&self.disk, &self.recovering, self.key);
            }
            _ => {}
        }
        result
    }
}

/// An acceptor's store (see the module doc).
pub(crate) enum NodeStore {
    /// The world-backed store: every fault coin, the budget, the ledger.
    World(DurableStorage<SimTimeProvider>),
    /// The shipped journal store on the simulated disk (#187).
    Journal(LedgeredJournal),
}

impl Storage for NodeStore {
    fn initial_state(&self) -> (HardState, Config) {
        match self {
            Self::World(s) => s.initial_state(),
            Self::Journal(s) => s.inner.initial_state(),
        }
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        match self {
            Self::World(s) => s.accepted(slot),
            Self::Journal(s) => s.inner.accepted(slot),
        }
    }

    fn first_slot(&self) -> Slot {
        match self {
            Self::World(s) => s.first_slot(),
            Self::Journal(s) => s.inner.first_slot(),
        }
    }

    fn last_slot(&self) -> Slot {
        match self {
            Self::World(s) => s.last_slot(),
            Self::Journal(s) => s.inner.last_slot(),
        }
    }

    fn sealed_state(&self) -> JournalState {
        match self {
            Self::World(s) => s.sealed_state(),
            Self::Journal(s) => s.inner.sealed_state(),
        }
    }

    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        match self {
            Self::World(s) => s.faulty_entries(),
            Self::Journal(s) => s.inner.faulty_entries(),
        }
    }
}

impl LogStorage for NodeStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.boot_scan().await,
            Self::Journal(s) => {
                if s.provisioning_ambiguous() {
                    // Settled from the disk by the operator's own command
                    // (see the module doc): a scan, then the format and
                    // sync only if the marker never landed.
                    assert_reachable!(
                        "journal store: an interrupted provisioning is resolved from the disk"
                    );
                    match paros::provision_store(&mut s.inner).await {
                        Ok(_) => {
                            let ip = s.ip.clone();
                            s.with_world(|w| w.note_provisioned(&ip));
                        }
                        Err(RunError::Storage(fault)) => return s.ledger(Err(fault)),
                        // A marker under another configuration: the driver
                        // refuses it at boot, as a mismatch.
                        Err(_) => {}
                    }
                } else {
                    let scanned = s.inner.boot_scan().await;
                    s.ledger(scanned)?;
                }
                s.declare_recovery();
                let facts = s.inner.boot_facts();
                if facts.checkpoint_truncated {
                    // A cause the geometry makes likely (a small layout, a
                    // knobbed checkpoint cadence), paired as a reachable.
                    assert_reachable!(
                        "journal store: a node boots from a checkpoint-truncated prefix"
                    );
                }
                if facts.ambiguous_kept > 0 {
                    assert_reachable!(
                        "journal store: a crash leaves an ambiguous last batch the journal keeps"
                    );
                }
                if facts.corrupt_reported > 0 {
                    assert_reachable!("journal store: mid-log rot is reported as faulty votes");
                }
                if facts.meta_repaired {
                    assert_reachable!("journal store: a damaged metadata copy is repaired");
                }
                Ok(())
            }
        }
    }

    fn formatted_config(&self) -> Option<Config> {
        match self {
            Self::World(s) => s.formatted_config(),
            Self::Journal(s) => s.inner.formatted_config(),
        }
    }

    async fn format(&mut self, config: &Config) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.format(config).await,
            Self::Journal(s) => {
                let ip = s.ip.clone();
                s.with_world(|w| w.note_provisioning(&ip));
                let formatted = s.inner.format(config).await;
                s.ledger(formatted)?;
                s.format_pending = true;
                Ok(())
            }
        }
    }

    async fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.persist_ballot(ballot).await,
            Self::Journal(s) => {
                let result = s.inner.persist_ballot(ballot).await;
                s.ledger(result)
            }
        }
    }

    async fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.append_accepted(slot, ballot, command).await,
            Self::Journal(s) => {
                let result = s.inner.append_accepted(slot, ballot, command).await;
                s.ledger(result)
            }
        }
    }

    async fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.set_chosen_index(slot).await,
            Self::Journal(s) => {
                let result = s.inner.set_chosen_index(slot).await;
                s.ledger(result)
            }
        }
    }

    async fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.sync(must_sync).await,
            Self::Journal(s) => {
                let synced = s.inner.sync(must_sync).await;
                s.ledger(synced)?;
                s.declare_recovery();
                if std::mem::take(&mut s.format_pending) {
                    // The marker is durable: the provisioning landed.
                    let ip = s.ip.clone();
                    s.with_world(|w| w.note_provisioned(&ip));
                }
                Ok(())
            }
        }
    }

    async fn truncate(&mut self, first: Slot, sealed: JournalState) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.truncate(first, sealed).await,
            Self::Journal(s) => {
                let result = s.inner.truncate(first, sealed).await;
                s.ledger(result)?;
                s.declare_recovery();
                Ok(())
            }
        }
    }

    async fn trimmed_to(&mut self, point: Slot, state: JournalState) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.trimmed_to(point, state).await,
            Self::Journal(s) => {
                let result = s.inner.trimmed_to(point, state).await;
                s.ledger(result)?;
                s.declare_recovery();
                Ok(())
            }
        }
    }
}
