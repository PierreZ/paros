//! An acceptor's store, whichever the seed drew (#187): the world-backed
//! [`DurableStorage`] every seed ran on until now, or the library's shipped
//! [`JournalStorage`] over the simulated disk (`SimStorageProvider`).
//!
//! A journal seed runs the store a real deployment runs, under every fault
//! but injected disk corruption: the world's copy budget and fault ledger
//! exist only because the world injects corruption, so a seed that injects
//! none needs neither, and moonpool's own crash model on the simulated disk
//! (unsynced writes resolved at a crash) is its storage fault. What the
//! world still owns on a journal seed is the operator's **provisioning
//! ledger** (#147), which [`LedgeredJournal`] keeps: the journal's format
//! marker lands only with the sync after the format, so the ledger records
//! the provisioning in two steps — begun at the format, landed when that
//! sync returns — and a process killed in between leaves the operator
//! honestly unsure, which the next boot resolves by reading the disk
//! (`crate::process`).

use std::sync::{Mutex, PoisonError, Weak};

use moonpool_sim::{SimStorageProvider, SimTimeProvider, assert_reachable};
use paros::{
    Ballot, Command, Config, HardState, JournalStorage, LogStorage, MustSync, SessionEntry, Slot,
    Storage, StorageError,
};

use super::StorageWorld;
use super::storage::DurableStorage;

/// The journal store, keeping the world's provisioning ledger in step with
/// its format marker (see the module doc).
pub(crate) struct LedgeredJournal {
    inner: JournalStorage<SimStorageProvider>,
    world: Weak<Mutex<StorageWorld>>,
    ip: String,
    /// A format was staged and its sync has not returned yet.
    format_pending: bool,
}

impl LedgeredJournal {
    pub(crate) fn new(
        inner: JournalStorage<SimStorageProvider>,
        world: Weak<Mutex<StorageWorld>>,
        ip: String,
    ) -> Self {
        Self {
            inner,
            world,
            ip,
            format_pending: false,
        }
    }

    fn with_world(&self, f: impl FnOnce(&mut StorageWorld)) {
        if let Some(world) = self.world.upgrade() {
            f(&mut world.lock().unwrap_or_else(PoisonError::into_inner));
        }
    }

    /// Count an I/O fault the simulated disk handed back, the journal
    /// seed's fault ledger (the world injected nothing): the driver must
    /// surface each one as exactly one crash decision. A corruption verdict
    /// is not counted here — it has its own excuse path in the audit.
    fn ledger<T>(&self, result: Result<T, StorageError>) -> Result<T, StorageError> {
        if let Err(StorageError::Io { .. } | StorageError::FsyncFailed { .. }) = &result {
            self.with_world(StorageWorld::note_disk_fault);
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

    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        match self {
            Self::World(s) => s.sealed_sessions(),
            Self::Journal(s) => s.inner.sealed_sessions(),
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
                let scanned = s.inner.boot_scan().await;
                s.ledger(scanned)?;
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
                Ok(())
            }
        }
    }

    fn is_formatted(&self) -> bool {
        match self {
            Self::World(s) => s.is_formatted(),
            Self::Journal(s) => s.inner.is_formatted(),
        }
    }

    async fn format(&mut self) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.format().await,
            Self::Journal(s) => {
                let ip = s.ip.clone();
                s.with_world(|w| w.note_provisioning(&ip));
                let formatted = s.inner.format().await;
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
                if std::mem::take(&mut s.format_pending) {
                    // The marker is durable: the provisioning landed.
                    let ip = s.ip.clone();
                    s.with_world(|w| w.note_provisioned(&ip));
                }
                Ok(())
            }
        }
    }

    async fn truncate(&mut self, first: Slot, sealed: &[SessionEntry]) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.truncate(first, sealed).await,
            Self::Journal(s) => {
                let result = s.inner.truncate(first, sealed).await;
                s.ledger(result)
            }
        }
    }

    async fn trimmed_to(
        &mut self,
        point: Slot,
        sessions: &[SessionEntry],
    ) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.trimmed_to(point, sessions).await,
            Self::Journal(s) => {
                let result = s.inner.trimmed_to(point, sessions).await;
                s.ledger(result)
            }
        }
    }
}
