//! A node's store (#176): the library's shipped [`JournalStorage`] on the
//! simulated disk, with the harness's storage fault sites at its seam.
//!
//! Every acceptor, replica, joiner and system-journal seat runs the store a
//! real deployment runs; what the [`StorageWorld`] adds sits around it:
//!
//! - **The write path's ambiguity.** Two independent BUGGIFY sites (a
//!   per-record write `EIO`, a failed batch fsync) report the fsyncgate
//!   error while the world decides, seeded and recorded, whether the effect
//!   persisted anyway: on the persisted leg the store really flushes before
//!   the error is reported, on the lost leg nothing is flushed and the stage
//!   dies with the store the driver drops. A **forced torn tail** (its own
//!   location) lands the batch and then tears it the way a crash before its
//!   sync would: from a chosen entry on, identifiers zeroed and bytes
//!   garbled — the first one possibly with its identifier kept, which the
//!   journal keeps as an ambiguous last batch.
//! - **Latent faults** at every boot inside the chaos window
//!   ([`super::latent`]), landed on the journal's files before the scan
//!   reads them back, and the ledger resolved against what the scan
//!   reported ([`StorageWorld::resolve_boot`]).
//! - **The world's shadow** ([`super::NodeDisk`]): what the store durably
//!   holds, refreshed from the store's own image at every boot and every
//!   flush that returned — the copy budget's and the corpus probes' view.
//! - **The operator's provisioning ledger** (#147): the format marker lands
//!   only with the sync after the format, so the ledger records the
//!   provisioning in two steps — begun at the format, landed when that
//!   sync returns — and a process killed in between leaves the operator
//!   honestly unsure, which the next boot resolves by reading the disk
//!   (`crate::process`).
//!
//! Every fault draw stays on the seed and runs on the node's own loop (the
//! store is called from the driver's loop, never from a spawned task).

use std::sync::{Arc, Mutex, PoisonError, Weak};

use moonpool_sim::{
    SimStorageProvider, SimTimeProvider, TimeProvider, assert_always, assert_reachable,
    assert_sometimes, buggify_with_prob, sim::sim_random,
};
use paros::{
    Ballot, Command, Config, CorruptionVerdict, HardState, IntegrityFault, JournalState,
    JournalStorage, JournalStoreConfig, LogStorage, MustSync, Slot, Storage, StorageError,
    StorageRecord, WriteOutcome, command_hash,
};

use super::faults::StorageFaults;
use super::journal_files::{self, Damage};
use super::latent::{self, Target};
use super::{
    BootRead, CorruptionInjection, CorruptionKind, InjectedFault, InjectedFaultKind, StorageWorld,
};
use crate::audit::AuditWorld;

/// The epoch of an accepted record (`paros::journal`'s frame kind 1).
const ACCEPTED_EPOCH: u64 = 1;

/// One write the store took, as the world's shadow folds it once flushed.
#[derive(Default)]
struct Staged {
    /// A promise or a format marker is staged (the metadata).
    meta: bool,
    /// Accepted records: `(slot, ballot, command hash)`.
    accepted: Vec<(Slot, Ballot, u64)>,
    chosen: Option<Slot>,
    floor: Option<Slot>,
    /// A trim-point jump's landing (`point - 1`).
    landing: Option<Slot>,
    /// Journal entries the next append writes (every staged record but the
    /// metadata).
    entries: u64,
}

impl Staged {
    fn is_empty(&self) -> bool {
        !self.meta && self.entries == 0
    }

    /// Only chosen-index records: a relaxed sync defers them.
    fn chosen_only(&self) -> bool {
        !self.meta && self.accepted.is_empty() && self.floor.is_none() && self.landing.is_none()
    }

    fn slots(&self) -> Vec<u64> {
        self.accepted.iter().map(|(slot, _, _)| slot.0).collect()
    }
}

/// One store write, routed through the write-`EIO` site.
enum Op {
    Ballot(Ballot),
    Accepted(Slot, Ballot, Command),
    Chosen(Slot),
    Truncate(Slot, JournalState),
    TrimmedTo(Slot, JournalState),
}

impl Op {
    fn record(&self) -> StorageRecord {
        match self {
            Op::Ballot(_) => StorageRecord::Promise,
            Op::Accepted(slot, ..) => StorageRecord::Accepted(*slot),
            Op::Chosen(_) => StorageRecord::ChosenIndex,
            Op::Truncate(..) | Op::TrimmedTo(..) => StorageRecord::Truncation,
        }
    }
}

/// A node's store: [`JournalStorage`] on the simulated disk with the
/// harness's fault sites (see the module doc).
pub(crate) struct SimJournal {
    inner: JournalStorage<SimStorageProvider>,
    provider: SimStorageProvider,
    layout: JournalStoreConfig,
    world: Weak<Mutex<StorageWorld>>,
    key: String,
    node: u64,
    faults: StorageFaults<SimTimeProvider>,
    checker: Arc<AuditWorld>,
    /// A format was staged and its sync has not returned yet.
    format_pending: bool,
    staged: Staged,
}

impl SimJournal {
    /// The store of node `node` (world key `key`) for `config`, in `dir`
    /// on `provider`. `faults` is the node's write-path switchboard (a quiet
    /// seat's never fires).
    #[allow(clippy::too_many_arguments)] // one store, its disk, its world, its ledgers
    pub(crate) fn new(
        provider: SimStorageProvider,
        dir: String,
        config: Config,
        layout: JournalStoreConfig,
        world: Weak<Mutex<StorageWorld>>,
        key: String,
        node: u64,
        faults: StorageFaults<SimTimeProvider>,
        checker: Arc<AuditWorld>,
    ) -> Self {
        Self {
            inner: JournalStorage::new(provider.clone(), dir, config, layout),
            provider,
            layout,
            world,
            key,
            node,
            faults,
            checker,
            format_pending: false,
            staged: Staged::default(),
        }
    }

    fn with_world<R>(&self, f: impl FnOnce(&mut StorageWorld) -> R) -> Option<R> {
        self.world
            .upgrade()
            .map(|world| f(&mut world.lock().unwrap_or_else(PoisonError::into_inner)))
    }

    /// Count an I/O fault the simulated disk handed back (the world
    /// injected none of these): the driver must surface each one as exactly
    /// one crash decision, so it joins the injected faults in that count. A
    /// corruption verdict is not counted here: the boot resolves it.
    fn ledger<T>(&self, result: Result<T, StorageError>) -> Result<T, StorageError> {
        if let Err(StorageError::Io { .. } | StorageError::FsyncFailed { .. }) = &result {
            self.with_world(StorageWorld::note_disk_fault);
        }
        result
    }

    /// Record `fault` under the budget, the accepted records it would cost
    /// being `slots`. `in_window` is whether the chaos window was open when
    /// the site decided to fire — a persisted leg's flush awaits the disk
    /// first, and the window may close meanwhile. Returns whether it is
    /// permitted.
    fn permit(&self, fault: InjectedFault, slots: &[u64], in_window: bool) -> bool {
        self.with_world(|world| {
            // Suppression is explicit: the world records no new fault
            // outside the chaos window (and never heals old ones).
            assert_always!(
                in_window,
                "storage: no new fault is injected after the chaos window"
            );
            world.permit_and_record(&self.key, fault, slots)
        })
        .unwrap_or(false)
    }

    /// Hand one write to the store and stage it for the shadow.
    async fn stage(&mut self, op: Op) -> Result<(), StorageError> {
        match op {
            Op::Ballot(ballot) => {
                let result = self.inner.persist_ballot(ballot).await;
                self.ledger(result)?;
                self.staged.meta = true;
            }
            Op::Accepted(slot, ballot, command) => {
                let hash = command_hash(&command);
                let result = self.inner.append_accepted(slot, ballot, command).await;
                self.ledger(result)?;
                self.staged.accepted.retain(|(s, _, _)| *s != slot);
                self.staged.accepted.push((slot, ballot, hash));
                self.staged.entries += 1;
            }
            Op::Chosen(slot) => {
                let result = self.inner.set_chosen_index(slot).await;
                self.ledger(result)?;
                self.staged.chosen = Some(slot);
                self.staged.entries += 1;
            }
            Op::Truncate(first, sealed) => {
                let result = self.inner.truncate(first, sealed).await;
                self.ledger(result)?;
                self.staged.floor = Some(self.staged.floor.map_or(first, |f| f.max(first)));
                self.staged.entries += 1;
            }
            Op::TrimmedTo(point, state) => {
                let result = self.inner.trimmed_to(point, state).await;
                self.ledger(result)?;
                let landing = Slot(point.0.saturating_sub(1));
                self.staged.floor = Some(self.staged.floor.map_or(point, |f| f.max(point)));
                self.staged.landing = Some(self.staged.landing.map_or(landing, |l| l.max(landing)));
                self.staged.entries += 1;
            }
        }
        Ok(())
    }

    /// One store write through the write-`EIO` BUGGIFY site: on the
    /// persisted leg the write is staged *and* the store flushes before the
    /// error is reported (the effect is durable anyway); on the lost leg
    /// nothing is staged. The error is identical either way — that is the
    /// ambiguity.
    async fn write(&mut self, op: Op) -> Result<(), StorageError> {
        if !self.faults.active() || !buggify_with_prob!(self.faults.rates.write_eio) {
            return self.stage(op).await;
        }
        let in_window = self.faults.active();
        let record = op.record();
        let slots: Vec<u64> = match record {
            StorageRecord::Accepted(slot) => vec![slot.0],
            _ => Vec::new(),
        };
        let persisted = sim_random::<f64>() < self.faults.rates.eio_persisted;
        let fault = InjectedFault {
            node: self.node,
            record,
            kind: InjectedFaultKind::WriteEio,
            persisted,
        };
        if persisted {
            self.stage(op).await?;
            self.flush(MustSync::Sync).await?;
            if !self.permit(fault, &slots, in_window) {
                // Outside the budget: an early flush, and no fault.
                return Ok(());
            }
        } else if !self.permit(fault, &slots, in_window) {
            return self.stage(op).await;
        }
        Err(StorageError::Io {
            record,
            outcome: WriteOutcome::Unknown,
        })
    }

    /// Sync the store, and fold what landed into the shadow.
    async fn flush(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        let synced = self.inner.sync(must_sync).await;
        self.ledger(synced)?;
        if must_sync == MustSync::Relaxed && self.staged.chosen_only() {
            // The store defers a relaxed chosen-index batch to the next
            // flush; so does the shadow.
            return Ok(());
        }
        self.flushed();
        Ok(())
    }

    /// The stage reached the disk: the shadow, the audit's flushed ground
    /// truth (an ambiguous leg flushes without the driver ever surfacing
    /// the writes), and the provisioning that landed with it.
    fn flushed(&mut self) {
        let staged = std::mem::take(&mut self.staged);
        let accepted: Vec<(Slot, Ballot)> = staged
            .accepted
            .iter()
            .map(|(slot, ballot, _)| (*slot, *ballot))
            .collect();
        let format = std::mem::take(&mut self.format_pending);
        let key = self.key.clone();
        let node = self.node;
        self.with_world(|world| {
            world.note_flushed(
                &key,
                node,
                &accepted,
                staged.chosen,
                staged.floor,
                staged.landing,
            );
            if format {
                world.note_provisioned(&key);
            }
        });
        let hashes: Vec<(u64, u64)> = staged
            .accepted
            .iter()
            .map(|(slot, _, hash)| (slot.0, *hash))
            .collect();
        let now_ms = u64::try_from(self.faults.time.now().as_millis()).unwrap_or(u64::MAX);
        self.checker.note_flushed_ground_truth(
            self.node,
            now_ms,
            &hashes,
            staged.floor.map(|slot| slot.0),
            staged.landing.map(|slot| slot.0),
        );
    }

    /// The boot's latent faults (inside the chaos window, and a corpus
    /// mask's targeted ones always), landed before the scan reads them back.
    /// A transient read `EIO` armed for this boot surfaces here instead of
    /// the scan: it collapses into the corruption channel (zero-fill ⇒
    /// mismatch), same detection, same crash, and the retry — the next boot
    /// — reads clean.
    async fn inject_latent(&mut self) -> Result<(), StorageError> {
        let Some(world) = self.world.upgrade() else {
            return Ok(());
        };
        let target = Target {
            provider: &self.provider,
            dir: self.inner.dir(),
            slot_count: u64::from(self.layout.geometry.slot_count),
            key: &self.key,
            node: self.node,
        };
        latent::apply_pending(&world, &target).await;
        if self.faults.active() {
            latent::roll(&world, &target).await;
        }
        let eio = world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_read_eio(&self.key, self.node);
        match eio {
            Some(record) => Err(StorageError::Corruption {
                record,
                fault: IntegrityFault::ReadError,
                verdict: CorruptionVerdict::Corrupted,
            }),
            None => Ok(()),
        }
    }

    /// The torn leg of a lost fsync: the batch lands, then is torn the way
    /// a crash before its sync returned leaves it — from one of its
    /// accepted entries on, the identifiers zeroed and the bytes garbled,
    /// the first possibly with its identifier kept (an ambiguous last
    /// batch). Never acknowledged: the driver crashes on the error this
    /// returns into. Only a batch the sync appended alone can tear — a
    /// checkpoint behind it would make it not the last batch.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.node))]
    async fn tear(&mut self) {
        let before = self.inner.log_range();
        let expected = self.staged.entries;
        if self.inner.sync(MustSync::Sync).await.is_err() {
            // The disk failed the sync on its own: the batch's fate is the
            // disk's, and the injected error already stands for it.
            return;
        }
        let after = self.inner.log_range();
        let (Some(before), Some(after)) = (before, after) else {
            return;
        };
        if after.start != before.start || after.end - before.end != expected {
            return;
        }
        let Ok(entries) = journal_files::scan(
            &self.provider,
            self.inner.dir(),
            u64::from(self.layout.geometry.slot_count),
        )
        .await
        else {
            return;
        };
        let batch: Vec<_> = entries
            .into_iter()
            .filter(|entry| entry.index >= before.end)
            .collect();
        let accepted: Vec<usize> = batch
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.epoch == ACCEPTED_EPOCH)
            .map(|(at, _)| at)
            .collect();
        if accepted.is_empty() {
            return;
        }
        let from = accepted[usize::try_from(sim_random::<u64>()).unwrap_or(0) % accepted.len()];
        let ambiguous = sim_random::<f64>() < self.faults.rates.torn_entry_faulty;
        let torn = &batch[from..];
        let slots: Vec<Slot> = torn
            .iter()
            .filter(|entry| entry.epoch == ACCEPTED_EPOCH)
            .map(|entry| Slot(entry.tag[0]))
            .collect();
        let node = self.node;
        self.with_world(|world| {
            for slot in &slots {
                world.note_corruption(CorruptionInjection::dormant(
                    node,
                    StorageRecord::Accepted(*slot),
                    CorruptionKind::TornTail,
                ));
            }
        });
        let mut landed = true;
        for (at, entry) in torn.iter().enumerate() {
            let damage = if at == 0 && ambiguous {
                Damage::RotEntry
            } else {
                Damage::Tear
            };
            landed &= journal_files::damage(&self.provider, entry, damage, None)
                .await
                .is_ok();
        }
        if landed {
            self.with_world(|world| {
                for slot in &slots {
                    world.note_landed(
                        node,
                        StorageRecord::Accepted(*slot),
                        CorruptionKind::TornTail,
                    );
                }
            });
        }
    }
}

impl Storage for SimJournal {
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

impl LogStorage for SimJournal {
    /// The latent faults first (inside the chaos window, and a corpus
    /// mask's targeted ones always), then the store's own scan, and the
    /// ledger resolved against what it read back.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.node))]
    async fn boot_scan(&mut self) -> Result<(), StorageError> {
        self.staged = Staged::default();
        self.format_pending = false;
        self.inject_latent().await?;
        let scanned = self.inner.boot_scan().await;
        let key = self.key.clone();
        let node = self.node;
        match scanned {
            Err(error @ (StorageError::Corruption { .. } | StorageError::Metadata { .. })) => {
                // The journal refuses to start on an entry whose identifier
                // is damaged too (`DoubleFault`, reported against the store).
                let double_fault = matches!(
                    error,
                    StorageError::Corruption {
                        record: StorageRecord::Store,
                        fault: IntegrityFault::ChecksumMismatch,
                        ..
                    }
                );
                self.with_world(|world| {
                    world.resolve_boot(&key, node, &BootRead::Refused { double_fault });
                });
                return Err(error);
            }
            Err(error) => return self.ledger(Err(error)),
            Ok(()) => {}
        }
        let facts = self.inner.boot_facts();
        if facts.checkpoint_truncated {
            // A cause the geometry makes likely (a small layout).
            assert_reachable!("journal store: a node boots from a checkpoint-truncated prefix");
        }
        if facts.ambiguous_kept > 0 {
            assert_reachable!(
                "journal store: a crash leaves an ambiguous last batch the journal keeps"
            );
        }
        let (hard_state, _) = self.inner.initial_state();
        let first = self.inner.first_slot();
        assert_sometimes!(
            self.inner.formatted_config().is_some(),
            "a node boots from a prior incarnation's durable records"
        );
        // Read-back pair of the flush ordering: a floor that reached the
        // disk never outruns the chosen index that reached it.
        assert_always!(
            first.0 == 0
                || hard_state
                    .chosen_index
                    .is_some_and(|ci| first.0 <= ci.0 + 1),
            "a restored floor never outruns the restored chosen index",
            {
                "floor" => first.0,
                "chosen" => hard_state.chosen_index.map_or(0, |c| c.0)
            }
        );
        if first.0 > 0 {
            assert_reachable!("a node reboots above a non-zero compaction floor");
        }
        let faulty = self.inner.faulty_entries();
        let accepted = (first.0..=self.inner.last_slot().0)
            .filter_map(|slot| {
                self.inner
                    .accepted(Slot(slot))
                    .map(|(ballot, _)| (Slot(slot), ballot))
            })
            .collect();
        let faulty_slots = faulty.iter().map(|(slot, _)| slot.0).collect();
        self.with_world(|world| {
            world.note_booted(&key, accepted, &faulty, first, hard_state.chosen_index);
            world.resolve_boot(
                &key,
                node,
                &BootRead::Booted {
                    faulty: &faulty_slots,
                    torn_tail: facts.torn_tail,
                    below_tail: facts.corrupt > 0,
                    ambiguous: facts.ambiguous_kept > 0,
                    meta_repaired: facts.meta_repaired,
                },
            );
        });
        Ok(())
    }

    fn formatted_config(&self) -> Option<Config> {
        self.inner.formatted_config()
    }

    async fn format(&mut self, config: &Config) -> Result<(), StorageError> {
        let key = self.key.clone();
        self.with_world(|world| world.note_provisioning(&key));
        let formatted = self.inner.format(config).await;
        self.ledger(formatted)?;
        self.format_pending = true;
        self.staged.meta = true;
        Ok(())
    }

    async fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.write(Op::Ballot(ballot)).await
    }

    async fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        self.write(Op::Accepted(slot, ballot, command)).await
    }

    async fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.write(Op::Chosen(slot)).await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = self.node, must_sync = ?must_sync))]
    async fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        if must_sync != MustSync::Sync {
            // The classification contract, checked from the other side: a
            // batch allowed to skip the fsync holds no safety-critical write
            // (every promise-raise or accept classifies as `MustSync::Sync`).
            assert_always!(
                !self.staged.meta || self.format_pending,
                "a relaxed flush holds no staged promise"
            );
            assert_always!(
                self.staged.accepted.is_empty(),
                "a relaxed flush holds no staged accept"
            );
            return self.flush(must_sync).await;
        }
        let slots = self.staged.slots();
        // The forced torn tail (its own BUGGIFY location): consulted only
        // where it can have an effect, a stage holding accepted records.
        let force_torn = self.faults.active()
            && !slots.is_empty()
            && buggify_with_prob!(self.faults.rates.force_torn_tail);
        // BUGGIFY site 2: the fsync fails — only when the stage holds
        // something (an empty flush has nothing at stake).
        let fails = !self.staged.is_empty()
            && (force_torn
                || (self.faults.active() && buggify_with_prob!(self.faults.rates.fsync_fail)));
        if !fails {
            return self.flush(MustSync::Sync).await;
        }
        let in_window = self.faults.active();
        let persisted = !force_torn && sim_random::<f64>() < self.faults.rates.fsync_persisted;
        let fault = InjectedFault {
            node: self.node,
            record: StorageRecord::Batch,
            kind: InjectedFaultKind::FsyncFailed,
            persisted,
        };
        let failed = Err(StorageError::FsyncFailed {
            record: StorageRecord::Batch,
            outcome: WriteOutcome::Unknown,
        });
        if persisted {
            // fsyncgate: the batch is durable, the error is reported anyway.
            self.flush(MustSync::Sync).await?;
            return if self.permit(fault, &slots, in_window) {
                failed
            } else {
                Ok(())
            };
        }
        if !self.permit(fault, &slots, in_window) {
            return self.flush(MustSync::Sync).await;
        }
        let torn =
            !slots.is_empty() && (force_torn || sim_random::<f64>() < self.faults.rates.torn_tail);
        if torn {
            if force_torn {
                // BUGGIFY pairing: the forcing site genuinely fired.
                assert_reachable!("storage: a torn tail is forced by its BUGGIFY site");
            }
            self.tear().await;
        }
        // The lost leg: the stage dies with the store the driver drops on
        // this error, never acknowledged.
        failed
    }

    async fn truncate(&mut self, first: Slot, sealed: JournalState) -> Result<(), StorageError> {
        self.write(Op::Truncate(first, sealed)).await
    }

    async fn trimmed_to(&mut self, point: Slot, state: JournalState) -> Result<(), StorageError> {
        self.write(Op::TrimmedTo(point, state)).await
    }
}
