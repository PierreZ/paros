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

use std::sync::{Arc, Mutex, PoisonError, Weak};

use moonpool_sim::{SimStorageProvider, SimTimeProvider, assert_reachable};
use paros::{
    Ballot, Command, Config, HardState, JournalState, JournalStorage, LogStorage, MustSync, Slot,
    Storage, StorageError,
};

use super::StorageWorld;
use super::power::PowerCut;
use super::storage::DurableStorage;
use crate::audit::AuditWorld;

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
    /// The shared checker, told what each commit has in flight (#264).
    checker: Arc<AuditWorld>,
    /// The node's id, the checker's key.
    node: u64,
    /// Whether this store takes the ledgered injector's damage (#261): a
    /// journal with a copy budget, never a quiet one.
    inject: bool,
    /// The simulated disk the journal lives on: the injector damages it.
    provider: SimStorageProvider,
    /// Lost copies a power cut may leave (a `Batched` commit's ambiguous
    /// last batch) budgeted by the world: `None` on an `Ordered` store,
    /// whose cut commit is torn or whole, never ambiguous; else the
    /// distinct acceptors the journal's quorum system tolerates losing.
    cut_budget: Option<usize>,
}

/// What damage a [`LedgeredJournal`] takes: its power cuts' copy budget and
/// whether the ledgered injector aims at it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DamagePolicy {
    /// See [`LedgeredJournal`]'s `cut_budget`.
    pub(crate) cut_budget: Option<usize>,
    /// See [`LedgeredJournal`]'s `inject`.
    pub(crate) inject: bool,
}

impl LedgeredJournal {
    pub(crate) fn new(
        inner: JournalStorage<SimStorageProvider>,
        world: Weak<Mutex<StorageWorld>>,
        ip: String,
        power: PowerCut,
        checker: Arc<AuditWorld>,
        damage: DamagePolicy,
        provider: SimStorageProvider,
    ) -> Self {
        let node = inner.initial_state().1.id.0;
        Self {
            node,
            cut_budget: damage.cut_budget,
            inject: damage.inject,
            provider,
            inner,
            world,
            ip,
            format_pending: false,
            power,
            checker,
        }
    }

    /// Tell the checker what the next commit may land (see
    /// [`AuditWorld::note_in_flight`]): a crash anywhere from here to the
    /// driver's report leaves these accepts durable or not, unreported.
    fn note_in_flight(&self) {
        let accepted: Vec<(u64, u64)> = self
            .inner
            .staged_slots()
            .filter_map(|slot| {
                self.inner
                    .accepted(slot)
                    .map(|(_, command)| (slot.0, paros::command_hash(&command)))
            })
            .collect();
        if !accepted.is_empty() {
            self.checker.note_in_flight(self.node, &accepted);
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
                // The ledgered injector (#261): at most one family's damage,
                // aimed by the custody ledger, applied before the journal
                // opens and judged against what it reports.
                let injection = if s.inject && s.power.in_chaos() {
                    let (ip, node) = (s.ip.clone(), s.node);
                    s.world.upgrade().and_then(|w| {
                        w.lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .plan_boot_damage(&ip, node)
                    })
                } else {
                    None
                };
                let confirmed = match &injection {
                    Some(injection) => super::injector::apply(&s.provider, injection).await,
                    None => false,
                };
                let mut scanned = s.inner.boot_scan().await;
                // A transient fault in the open's own repair (a failed sync of
                // the rewritten copy) says nothing about the damage: a boot
                // that carried an injection re-opens, as the driver's
                // quarantine would, until the journal gives its verdict.
                let mut retries = 0;
                while confirmed
                    && retries < super::injector::REOPEN_ATTEMPTS
                    && matches!(
                        scanned,
                        Err(StorageError::Io { .. } | StorageError::FsyncFailed { .. })
                    )
                {
                    retries += 1;
                    scanned = s.inner.boot_scan().await;
                }
                if let Some(injection) = injection.as_ref().filter(|_| confirmed) {
                    let faulty: Vec<u64> = s
                        .inner
                        .faulty_entries()
                        .iter()
                        .map(|(slot, _)| slot.0)
                        .collect();
                    let crashed = super::injector::judge(
                        injection,
                        &scanned,
                        s.inner.boot_facts(),
                        &faulty,
                        s.node,
                        retries > 0,
                    );
                    if crashed {
                        s.with_world(StorageWorld::note_injected_crash);
                    }
                }
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
                s.note_in_flight();
                let writes = s.inner.staged_entries() > 0;
                let slots: Vec<Slot> = s.inner.staged_slots().collect();
                let ip = s.ip.clone();
                s.with_world(|w| w.note_sync_started(&ip));
                let (world, ip, budget) = (s.world.clone(), s.ip.clone(), s.cut_budget);
                let permit = move || {
                    budget.is_none_or(|tolerated| {
                        world.upgrade().is_some_and(|world| {
                            world
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .permit_power_cut(&ip, tolerated)
                        })
                    })
                };
                let synced = s
                    .power
                    .around(writes, permit, || {}, s.inner.sync(must_sync))
                    .await;
                s.ledger(synced)?;
                // The custody ledger (#261): where what this sync wrote now
                // lives, the floor, the metainfo and header copies.
                let written: Vec<_> = slots
                    .into_iter()
                    .filter_map(|slot| s.inner.layout(slot).map(|at| (slot.0, at)))
                    .collect();
                let first = s.inner.first_slot().0;
                let regions = s.inner.regions();
                let ip = s.ip.clone();
                s.with_world(|w| w.note_synced(&ip, written, first, &regions));
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
