//! An acceptor's store (#187, #261): the library's shipped
//! [`JournalStorage`] over the simulated disk (`SimStorageProvider`), the
//! store a real deployment runs, under moonpool's crash physics, the power
//! cuts ([`super::power`]) and the ledgered injector ([`super::injector`]).
//!
//! What the world owns beside it is the operator's **provisioning ledger**
//! (#147), which [`LedgeredJournal`] keeps: the journal's format
//! marker lands only with the sync after the format, so the ledger records
//! the provisioning in two steps — begun at the format, landed when that
//! sync returns — and a process killed in between leaves the operator
//! honestly unsure, which the next boot resolves by reading the disk
//! (`crate::process`).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use moonpool_sim::{SimStorageProvider, assert_reachable};
use paros::{
    Ballot, Command, Config, HardState, JournalState, JournalStorage, LogStorage, MustSync, Slot,
    Storage, StorageError,
};

use super::StorageWorld;
use super::power::PowerCut;
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
    /// The accepts staged for the next sync, `slot -> (ballot, vhash)`:
    /// the ones its own floor drops are reported apart (see
    /// [`AuditWorld::note_dropped_in_flight`]).
    staged_accepts: BTreeMap<u64, (Ballot, u64)>,
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
            staged_accepts: BTreeMap::new(),
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
        // An accept the flush's own floor drops (the core staged a slot and
        // a truncation past it in one flush) is never written, yet the
        // landed metainfo makes it count: the floor and chosen index it
        // carries say the slot was decided, and on a quorum of one that
        // accept was the decision.
        let dropped: Vec<(u64, Ballot, u64)> = self
            .staged_accepts
            .iter()
            .filter(|(slot, _)| self.inner.accepted(Slot(**slot)).is_none())
            .map(|(slot, (ballot, vhash))| (*slot, *ballot, *vhash))
            .collect();
        if !dropped.is_empty() {
            self.checker.note_dropped_in_flight(self.node, &dropped);
        }
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

impl Storage for LedgeredJournal {
    fn initial_state(&self) -> (HardState, Config) {
        self.inner.initial_state()
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.inner.accepted(slot)
    }

    fn first_slot(&self) -> Slot {
        self.inner.first_slot()
    }

    fn last_slot(&self) -> Slot {
        self.inner.last_slot()
    }

    fn sealed_state(&self) -> JournalState {
        self.inner.sealed_state()
    }

    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.inner.faulty_entries()
    }
}

impl LogStorage for LedgeredJournal {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        // The ledgered injector (#261): at most one family's damage,
        // aimed by the custody ledger, applied before the journal
        // opens and judged against what it reports.
        let injection = if self.inject {
            let (ip, node, in_chaos) = (self.ip.clone(), self.node, self.power.in_chaos());
            self.world.upgrade().and_then(|w| {
                w.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .plan_boot_damage(&ip, node, in_chaos)
            })
        } else {
            None
        };
        let confirmed = match &injection {
            Some(injection) => super::injector::apply(&self.provider, injection).await,
            None => false,
        };
        let mut scanned = self.inner.boot_scan().await;
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
            scanned = self.inner.boot_scan().await;
        }
        if let Some(injection) = injection.as_ref().filter(|_| confirmed) {
            self.with_world(StorageWorld::note_injected);
            let faulty: Vec<u64> = self
                .inner
                .faulty_entries()
                .iter()
                .map(|(slot, _)| slot.0)
                .collect();
            let crashed = super::injector::judge(
                injection,
                &scanned,
                self.inner.boot_facts(),
                &faulty,
                self.node,
                retries > 0,
            );
            if crashed {
                self.with_world(StorageWorld::note_injected_crash);
            }
            // An outage's planned loss landed: the journal's own verdict is
            // the audit's ground truth that this copy is gone (#263).
            if let Some(slot) = injection.outage_loss()
                && faulty.contains(&slot)
            {
                self.checker.note_copy_lost(self.node, slot);
            }
        }
        self.ledger(scanned)?;
        // The custody ledger learns what the open found, a cut commit's
        // landing included (#263).
        let regions = self.inner.regions();
        let layouts: Vec<_> = regions
            .iter()
            .filter(|region| region.kind == paros::journal::Layout::ENTRY)
            .filter_map(|region| region.stripe)
            .filter_map(|slot| self.inner.layout(Slot(slot)).map(|at| (slot, at)))
            .collect();
        let faulty: Vec<u64> = self
            .inner
            .faulty_entries()
            .iter()
            .map(|(slot, _)| slot.0)
            .collect();
        self.checker.note_faulty_copies(self.node, &faulty);
        let (ip, node, first) = (self.ip.clone(), self.node, self.inner.first_slot().0);
        self.with_world(|w| w.note_opened(&ip, node, layouts, first, &regions, &faulty));
        let facts = self.inner.boot_facts();
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

    fn formatted_config(&self) -> Option<Config> {
        self.inner.formatted_config()
    }

    async fn format(&mut self, config: &Config) -> Result<(), StorageError> {
        let ip = self.ip.clone();
        self.with_world(|w| w.note_provisioning(&ip));
        let formatted = self.inner.format(config).await;
        self.ledger(formatted)?;
        self.format_pending = true;
        Ok(())
    }

    async fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        let result = self.inner.persist_ballot(ballot).await;
        self.ledger(result)
    }

    async fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        let vhash = paros::command_hash(&command);
        let result = self.inner.append_accepted(slot, ballot, command).await;
        if result.is_ok() {
            self.staged_accepts.insert(slot.0, (ballot, vhash));
        }
        self.ledger(result)
    }

    async fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        let result = self.inner.set_chosen_index(slot).await;
        self.ledger(result)
    }

    async fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        self.note_in_flight();
        let writes = self.inner.staged_entries() > 0;
        let slots: Vec<Slot> = self.inner.staged_slots().collect();
        let ip = self.ip.clone();
        self.with_world(|w| w.note_sync_started(&ip));
        let (world, ip, budget) = (self.world.clone(), self.ip.clone(), self.cut_budget);
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
        let synced = self
            .power
            .around(writes, permit, || {}, self.inner.sync(must_sync))
            .await;
        // The sync consumed what was staged, landed or not.
        self.staged_accepts.clear();
        self.ledger(synced)?;
        // The custody ledger (#261): where what this sync wrote now
        // lives, the floor, the metainfo and header copies.
        let written: Vec<_> = slots
            .into_iter()
            .filter_map(|slot| self.inner.layout(slot).map(|at| (slot.0, at)))
            .collect();
        let first = self.inner.first_slot().0;
        let regions = self.inner.regions();
        let ip = self.ip.clone();
        let node = self.node;
        self.with_world(|w| w.note_synced(&ip, node, written, first, &regions));
        if std::mem::take(&mut self.format_pending) {
            // The marker is durable: the provisioning landed.
            let ip = self.ip.clone();
            self.with_world(|w| w.note_provisioned(&ip));
        }
        Ok(())
    }

    async fn truncate(&mut self, first: Slot, sealed: JournalState) -> Result<(), StorageError> {
        let result = self.inner.truncate(first, sealed).await;
        self.ledger(result)
    }

    async fn trimmed_to(&mut self, point: Slot, state: JournalState) -> Result<(), StorageError> {
        let result = self.inner.trimmed_to(point, state).await;
        self.ledger(result)
    }
}
