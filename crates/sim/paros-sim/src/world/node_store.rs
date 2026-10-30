//! An acceptor's store (#187, #261): the library's shipped
//! [`JournalStorage`] over the simulated disk (`SimStorageProvider`), the
//! store a real deployment runs, under moonpool's crash physics, the power
//! cuts its own `hint!`s ask for (budgeted by [`super::cut`]) and the
//! ledgered injector ([`super::injector`]).
//!
//! What the world owns beside it is the operator's **provisioning ledger**
//! (#147), which [`LedgeredJournal`] keeps: the journal's format
//! marker lands only with the sync after the format, so the ledger records
//! the provisioning in two steps — begun at the format, landed when that
//! sync returns — and a process killed in between leaves the operator
//! honestly unsure, which the next boot resolves by reading the disk
//! (`crate::process`).

use std::sync::{Arc, Mutex, PoisonError, Weak};

use moonpool_sim::{SimStorageProvider, StateHandle, assert_reachable};
use paros::core::Ballot;
use paros::core::Config;
use paros::core::HardState;
use paros::core::Slot;
use paros::core::Storage;
use paros::core::contract::MustSync;
use paros::core::journal::Command;
use paros::core::journal::JournalState;
use paros::storage::LogStorage;
use paros::storage::StorageError;
use paros::storage::journal::JournalStorage;

use super::StorageWorld;
use super::cut::{Budget, InFlight, Owner};
use crate::audit::AuditWorld;

/// The journal store, keeping the world's provisioning ledger in step with
/// its format marker (see the module doc).
pub(crate) struct LedgeredJournal {
    inner: JournalStorage<SimStorageProvider>,
    world: Weak<Mutex<StorageWorld>>,
    ip: String,
    /// A format was staged and its sync has not returned yet.
    format_pending: bool,
    /// The run's state: where a commit in flight registers for the cut's
    /// budget ([`super::cut`]).
    state: StateHandle,
    /// Whether the injector's damage may land on this store while the
    /// run's chaos lasts (a quiet store never takes it).
    chaotic: bool,
    /// The shared checker, told the copies a boot finds lost or faulty.
    checker: Arc<AuditWorld>,
    /// The node's id, the checker's key.
    node: u64,
    /// Whether this store takes the ledgered injector's damage (#261): a
    /// journal with a copy budget, never a quiet one.
    inject: bool,
    /// The simulated disk the journal lives on: the injector damages it.
    provider: SimStorageProvider,
    /// Lost copies a power cut may leave (a `Batched` commit's ambiguous
    /// last batch) budgeted by the world ([`super::cut`]): `None` on an `Ordered` store,
    /// whose cut commit is torn or whole, never ambiguous; else the
    /// distinct acceptors the journal's quorum system tolerates losing.
    cut_budget: Option<usize>,
}

/// How a boot's injection is judged against the journal's verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Judging {
    /// No injection, or one that never confirmed and holds no park.
    Skip,
    /// Every write and sync confirmed: the family's verdict is exact.
    Landed,
    /// A double fault that never confirmed (#351): it may have landed
    /// whole, in part or not at all, so the verdict decides its park.
    Uncertain,
}

/// What damage a [`LedgeredJournal`] takes: its power cuts' copy budget,
/// whether the ledgered injector aims at it, and until when.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DamagePolicy {
    /// Whether the injector may damage the store while the run's chaos
    /// lasts: false for a quiet store.
    pub(crate) chaotic: bool,
    /// See [`LedgeredJournal`]'s `cut_budget`.
    pub(crate) cut_budget: Option<usize>,
    /// See [`LedgeredJournal`]'s `inject`.
    pub(crate) inject: bool,
}

impl LedgeredJournal {
    /// The ledgered injector (#261) at one boot: at most one family's
    /// damage, aimed by the custody ledger, applied before the journal
    /// opens and judged against what it reports. Returns the injection and
    /// how its verdict is judged.
    async fn damage(&mut self) -> (Option<super::injector::Injection>, Judging) {
        // A double fault a boot planned and its journal never judged is
        // judged now (#351): a second apply would flip its bytes back.
        let pending = self.world.upgrade().and_then(|w| {
            w.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .unjudged(&self.ip)
        });
        if let Some(reserved) = pending {
            let judging = if reserved.landed {
                Judging::Landed
            } else {
                Judging::Uncertain
            };
            return (Some(reserved.injection), judging);
        }
        let injection = if self.inject {
            let (ip, node, in_chaos) = (
                self.ip.clone(),
                self.node,
                self.chaotic && crate::in_chaos(),
            );
            self.world.upgrade().and_then(|w| {
                w.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .plan_boot_damage(&ip, node, in_chaos)
            })
        } else {
            None
        };
        let Some(injection) = injection else {
            return (None, Judging::Skip);
        };
        if super::injector::apply(&self.provider, &injection).await {
            let ip = self.ip.clone();
            self.with_world(|w| {
                w.note_applied(&ip);
                w.note_flip_synced(&ip);
            });
            (Some(injection), Judging::Landed)
        } else if injection.double_fault().is_some() {
            // The reserved park waits for the journal's verdict.
            (Some(injection), Judging::Uncertain)
        } else {
            // Any other family that never confirmed may or may not have
            // landed; the open may repair it either way. Not judged.
            (Some(injection), Judging::Skip)
        }
    }

    pub(crate) fn new(
        inner: JournalStorage<SimStorageProvider>,
        world: Weak<Mutex<StorageWorld>>,
        ip: String,
        state: StateHandle,
        checker: Arc<AuditWorld>,
        damage: DamagePolicy,
        provider: SimStorageProvider,
    ) -> Self {
        let node = inner.initial_state().1.id.0;
        if !damage.inject
            && let Some(world) = world.upgrade()
        {
            world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .note_quiet(&ip);
        }
        Self {
            node,
            state,
            chaotic: damage.chaotic,
            cut_budget: damage.cut_budget,
            inject: damage.inject,
            provider,
            inner,
            world,
            ip,
            format_pending: false,
            checker,
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
        let (injection, judging) = self.damage().await;
        // An open writes too (moonpool-journal's `begin_generation` rewrites
        // both metainfo copies; a header or record repair rewrites its
        // region), so a kill inside it may tear a copy the last sync left
        // whole: until `note_opened`, the ledger cannot vouch for the layout
        // and aims no family at it (#270: a bare-quorum seed's two outages
        // killed a reboot mid-open, then a metainfo rot flipped the one
        // valid copy).
        let ip = self.ip.clone();
        self.with_world(|w| w.note_sync_started(&ip));
        let mut scanned = self.inner.boot_scan().await;
        // A transient fault in the open's own repair (a failed sync of
        // the rewritten copy) says nothing about the damage: a boot
        // that carried an injection re-opens, as the driver's
        // quarantine would, until the journal gives its verdict.
        let mut retries = 0;
        while judging != Judging::Skip
            && retries < super::injector::REOPEN_ATTEMPTS
            && matches!(
                scanned,
                Err(StorageError::Io { .. } | StorageError::FsyncFailed { .. })
            )
        {
            retries += 1;
            scanned = self.inner.boot_scan().await;
        }
        let refused = matches!(scanned, Err(StorageError::Corruption { .. }));
        let faulty: Vec<u64> = self
            .inner
            .faulty_entries()
            .iter()
            .map(|(slot, _)| slot.0)
            .collect();
        if let Some(slot) = injection
            .as_ref()
            .and_then(super::injector::Injection::double_fault)
            && judging == Judging::Uncertain
            && !matches!(
                scanned,
                Err(StorageError::Io { .. } | StorageError::FsyncFailed { .. })
            )
        {
            // A double fault that may have landed in part (#351): a refusal
            // is the damage landed whole, a crash decision like any other;
            // an open is the reserved park released.
            if refused {
                self.with_world(StorageWorld::note_injected);
                self.with_world(StorageWorld::note_injected_crash);
                assert_reachable!("journal store: an unconfirmed double fault refuses the journal");
            }
            let (ip, lost) = (self.ip.clone(), faulty.contains(&slot));
            self.with_world(|w| w.judged(&ip, refused, slot, lost));
        }
        if let Some(injection) = injection.as_ref().filter(|_| judging == Judging::Landed) {
            self.with_world(StorageWorld::note_injected);
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
            if let Some(slot) = injection.double_fault() {
                let (ip, lost) = (self.ip.clone(), faulty.contains(&slot));
                self.with_world(|w| w.judged(&ip, refused, slot, lost));
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
            .filter(|region| region.kind == paros::storage::journal::Layout::ENTRY)
            .filter_map(|region| region.stripe)
            .filter_map(|slot| self.inner.layout(Slot(slot)).map(|at| (slot, at)))
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
        let result = self.inner.append_accepted(slot, ballot, command).await;
        self.ledger(result)
    }

    async fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        let result = self.inner.set_chosen_index(slot).await;
        self.ledger(result)
    }

    async fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        let writes = self.inner.staged_entries() > 0;
        let slots: Vec<Slot> = self.inner.staged_slots().collect();
        let ip = self.ip.clone();
        self.with_world(|w| w.note_sync_started(&ip));
        // A commit that writes entries is in flight until the sync returns:
        // a hint that kills the process now spends its copy budget.
        let in_flight = writes.then(|| {
            let budget = self
                .cut_budget
                .map_or(Budget::Free, |tolerated| Budget::Node {
                    tolerated,
                    slots: slots.iter().map(|slot| slot.0).collect(),
                });
            InFlight::open(
                &self.state,
                &self.ip,
                Owner::Node,
                budget,
                self.world.clone(),
            )
        });
        let synced = self.inner.sync(must_sync).await;
        drop(in_flight);
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
