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
use std::time::Duration;

use futures::future::{Either, select};
use moonpool_sim::{
    RebootKind, SelfCrash, SimStorageProvider, SimTimeProvider, TimeProvider, assert_reachable,
    buggify_with_prob,
};
use paros::{
    Ballot, Command, Config, HardState, JournalState, JournalStorage, LogStorage, MustSync, Slot,
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
    /// How this node can lose power in the middle of a commit.
    power: PowerCut,
}

/// A power loss **inside** a journal commit (#176). A crash from attrition
/// lands at a random instant, and a commit's unsynced window (entries
/// written, records or the sync not yet done) is microseconds wide, so the
/// journal's crash recovery (torn records, records rebuilt from their
/// entries, an ambiguous last batch) would almost never run. This BUGGIFY
/// site makes it likely: a buggified commit races a random timer spanning a
/// commit's duration, and when the timer wins the process cuts its own power through
/// moonpool's [`SelfCrash`]: the kill lands before the commit's next storage
/// completion, every unsynced sector resolves by the disk's crash physics,
/// and the node restarts after a delay. Only inside the chaos window, so the
/// tail is a genuine recovery.
pub(crate) struct PowerCut {
    crash: SelfCrash,
    time: SimTimeProvider,
    cutoff: Duration,
    enabled: bool,
    /// How long this node's last whole commit took: the span a cut is
    /// drawn in, so it lands between a write and the sync that would have
    /// covered it.
    last_commit: Duration,
}

impl PowerCut {
    pub(crate) fn new(
        crash: SelfCrash,
        time: SimTimeProvider,
        cutoff: Duration,
        enabled: bool,
    ) -> Self {
        Self {
            crash,
            time,
            cutoff,
            enabled,
            last_commit: Duration::from_millis(1),
        }
    }

    /// The delay after which this commit loses power, if it is one that
    /// does: uniform over the node's last commit's duration.
    fn draw(&self, entries: usize) -> Option<Duration> {
        // A commit without entries (a format, a promise) has no persist
        // record to tear: the metainfo's own copies cover it.
        if !self.enabled
            || entries == 0
            || self.time.now() >= self.cutoff
            || !buggify_with_prob!(0.25)
        {
            return None;
        }
        let span = u64::try_from(self.last_commit.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        Some(Duration::from_micros(moonpool_sim::sim_random_range(
            0..span,
        )))
    }
}

impl LedgeredJournal {
    pub(crate) fn new(
        inner: JournalStorage<SimStorageProvider>,
        world: Weak<Mutex<StorageWorld>>,
        ip: String,
        power: PowerCut,
    ) -> Self {
        Self {
            inner,
            world,
            ip,
            format_pending: false,
            power,
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
    World(Box<DurableStorage<SimTimeProvider>>),
    /// The shipped journal store on the simulated disk (#187).
    Journal(Box<LedgeredJournal>),
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
                let scanned = s.inner.boot_scan().await;
                s.ledger(scanned)?;
                let facts = s.inner.boot_facts();
                // Causes the crash physics and the small geometry make
                // likely, each paired as a reachable.
                if facts.truncated {
                    assert_reachable!("journal store: a node boots above a truncated prefix");
                }
                if facts.recovery.torn > 0 {
                    assert_reachable!("journal store: a crash tears a batch the journal discards");
                }
                if facts.recovery.rebuilt > 0 {
                    assert_reachable!("journal store: a persist record is rebuilt from its entry");
                }
                if !facts.recovery.ambiguous.is_empty() {
                    assert_reachable!(
                        "journal store: a crash leaves an ambiguous last batch the journal keeps"
                    );
                }
                if facts.recovery.meta_repaired {
                    assert_reachable!("journal store: a metainfo copy is repaired from its twin");
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
                let synced = match s.power.draw(s.inner.staged_entries()) {
                    None => {
                        let start = s.power.time.now();
                        let synced = s.inner.sync(must_sync).await;
                        s.power.last_commit = s.power.time.now().saturating_sub(start);
                        synced
                    }
                    Some(after) => {
                        let commit = Box::pin(s.inner.sync(must_sync));
                        let timer = Box::pin(s.power.time.sleep(after));
                        match select(commit, timer).await {
                            Either::Left((synced, _)) => synced,
                            Either::Right((_, commit)) => {
                                // Paired with the recovery gates the journal
                                // reports at the next boot.
                                assert_reachable!("journal store: a node loses power mid-commit");
                                let restart = Duration::from_millis(
                                    moonpool_sim::sim_random_range(500..2500),
                                );
                                // The kill lands before the commit's next
                                // storage completion: keep awaiting it.
                                let _ = s.power.crash.crash(RebootKind::Crash, Some(restart));
                                commit.await
                            }
                        }
                    }
                };
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

    async fn truncate(&mut self, first: Slot, sealed: JournalState) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.truncate(first, sealed).await,
            Self::Journal(s) => {
                let result = s.inner.truncate(first, sealed).await;
                s.ledger(result)
            }
        }
    }

    async fn trimmed_to(&mut self, point: Slot, state: JournalState) -> Result<(), StorageError> {
        match self {
            Self::World(s) => s.trimmed_to(point, state).await,
            Self::Journal(s) => {
                let result = s.inner.trimmed_to(point, state).await;
                s.ledger(result)
            }
        }
    }
}
