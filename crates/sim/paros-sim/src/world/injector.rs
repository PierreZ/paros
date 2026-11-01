//! The **ledgered, journal-aware injector** (#261): boot-time corruption
//! chaos for the shipped `JournalStorage` on the simulated disk, aimed by what the journal itself
//! says it wrote and judged by what it reports when it opens.
//!
//! **The ledger.** Every sync a journal store completes tells the world where
//! the slots it wrote now live (`JournalStorage::layout`, a persist record
//! and an entry, striped by slot), where its metainfo copies and segment
//! headers are, its floor, and which slots its last batch holds
//! ([`StorageWorld::note_synced`]); a sync that starts unsettles it
//! ([`StorageWorld::note_sync_started`]), so a boot after a cut commit, whose
//! layout the ledger cannot know, injects nothing. That is the custody shadow
//! per `(journal, node)` the copy budget is counted over.
//!
//! **The injection.** At a boot inside the chaos window, before the journal
//! opens, at most one family fires (each its own BUGGIFY location, rolled in
//! a fixed order, the first that fires and passes its budget wins), and its
//! bytes are flipped through the node's own storage provider and synced:
//!
//! | Family | Damage | Budget | The journal's verdict |
//! |---|---|---|---|
//! | entry rot | a slot's entry | a lost copy: moonpool's shard gate | the slot reported faulty (CTRL's recoverable class) |
//! | record rot | a slot's persist record | moonpool's shard gate | the record rebuilt from its entry |
//! | double fault | both, outside the last batch | a lost node: [`StorageWorld::may_park`] and the gate | the journal refuses to open (`DoubleFault`), the node parked |
//! | metainfo rot | one metainfo copy | moonpool's shard gate | the copy repaired |
//! | header rot | one segment header copy | moonpool's shard gate | the header repaired |
//!
//! **Moonpool places the damage** (#760 (moonpool owns fault placement)):
//! the flip goes through `SimStorageProvider::corrupt_bytes`, which the
//! shard gate refuses when the journal's configurations in force would keep
//! too few clean copies of the slot. The simulator then sees the damage, so
//! its own crashes and rot never take the copies it leaves. A double fault
//! gives the whole node up, so it asks `SimStorageProvider::lose_replica`
//! first. A refused flip is no injection.
//!
//! **The ground truth** is the injection set against the journal's own
//! verdicts at open ([`judge`]): every injected damage resolves to exactly the
//! verdict its family names. Metainfo and header damage wait for a settled
//! store (both copies written by a completed commit), and a double fault
//! never touches the last batch, where the journal reads a doubly damaged
//! slot as a torn, unacknowledged write and would drop an acknowledged vote.
//! A planned injection unsettles its custody until the journal opens again
//! (#441 (metainfo twin)): an open that fails leaves the damage unread, and
//! no family aims at that layout again until one succeeds.
//!
//! **A double fault's park waits for its verdict** (#351): the node boots
//! again, and the journal's refusal makes the park terminal
//! ([`StorageWorld::judged`]).

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{
    OpenOptions, SimStorageProvider, StorageFile, StorageProvider, assert_always, assert_reachable,
    buggify_with_prob,
};
use paros::storage::StorageError;
use paros::storage::journal::JournalBootFacts;
use paros::storage::journal::{Layout, LayoutRegion};

use super::StorageWorld;
use super::outage::{LossShape, PlannedLoss};

/// Per-boot firing probabilities of the injector's families, each its own
/// BUGGIFY location (per-seed activation x per-boot firing).
const P_ENTRY_ROT: f64 = 0.25;
const P_RECORD_ROT: f64 = 0.2;
const P_DOUBLE_FAULT: f64 = 0.1;
const P_META_ROT: f64 = 0.15;
const P_HEADER_ROT: f64 = 0.15;

/// How many bytes one injection flips, and where in its region: inside the
/// checksummed head of every region the journal lays out (a 64-byte persist
/// record is the smallest).
const FLIPPED: u64 = 8;
const FLIPPED_AT: u64 = 8;

/// How many times a flush before the damage is retried: moonpool's storage
/// chaos fails a sync now and then, never for long. A flush that never
/// confirms leaves the store undamaged.
const WRITE_ATTEMPTS: usize = 64;

/// How many times a boot that carried an injection re-opens its journal
/// after a transient I/O fault, before the verdict is judged: the same hang
/// guard as [`WRITE_ATTEMPTS`].
pub(crate) const REOPEN_ATTEMPTS: usize = 64;

/// One journal store's custody, as of its last completed sync (see the
/// module doc).
#[derive(Clone, Debug, Default)]
pub(crate) struct Custody {
    /// The node's id (the audit's key).
    node: u64,
    /// Whether the last sync that started also completed: only then is the
    /// layout below what the disk holds.
    settled: bool,
    /// Whether an injection was planned since the journal last opened: a
    /// boot that finds it still set follows one whose open failed (#441
    /// (metainfo twin)).
    injected: bool,
    /// The journal's floor: nothing below it is targeted.
    first: u64,
    /// Where each held slot's persist record and entry live.
    records: BTreeMap<u64, Layout>,
    /// The slots of the last batch that wrote entries.
    last_batch: BTreeSet<u64>,
    /// The two metainfo copies.
    meta: Vec<LayoutRegion>,
    /// Every segment's two header copies.
    headers: Vec<LayoutRegion>,
}

impl Custody {
    /// The slot whose copies are held from `first` up: every slot this
    /// custody can lose.
    pub(super) fn holds(&self) -> impl Iterator<Item = u64> + '_ {
        self.records.keys().copied()
    }

    /// The custody's floor.
    pub(super) fn first(&self) -> u64 {
        self.first
    }
}

/// Which family an injection is (see the module doc's table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Family {
    /// A slot's entry: reported faulty.
    EntryRot(u64),
    /// A slot's persist record: rebuilt from the entry.
    RecordRot(u64),
    /// Both, outside the last batch: the journal refuses to open.
    DoubleFault(u64),
    /// One metainfo copy: repaired from its twin.
    MetaRot,
    /// One segment header copy: repaired from its twin.
    HeaderRot,
}

/// One planned injection: its family and the regions it damages.
#[derive(Clone, Debug)]
pub(crate) struct Injection {
    family: Family,
    regions: Vec<LayoutRegion>,
    /// A copy a correlated outage's plan lost (#263,
    /// [`StorageWorld::plan_outage_loss`]), not a boot's own draw.
    outage: bool,
}

impl Injection {
    /// The slot a double fault damages, if this is one.
    pub(crate) fn double_fault(&self) -> Option<u64> {
        match self.family {
            Family::DoubleFault(slot) => Some(slot),
            _ => None,
        }
    }

    /// The slot whose copy an outage's plan lost, if this is one.
    pub(crate) fn outage_loss(&self) -> Option<u64> {
        match self.family {
            Family::EntryRot(slot) if self.outage => Some(slot),
            _ => None,
        }
    }
}

impl StorageWorld {
    /// A journal store at `key` is about to commit (see the module doc):
    /// until it completes, its ledgered layout may be stale.
    pub(crate) fn note_sync_started(&mut self, key: &str) {
        self.custody.entry(key.to_string()).or_default().settled = false;
    }

    /// A journal store at `key` completed a sync: `written` are the slots
    /// it wrote, with where they now live; `first` its floor; `regions` the
    /// journal's every region (for the metainfo and header copies). A
    /// written slot is durably real again: its fault mark clears.
    pub(crate) fn note_synced(
        &mut self,
        key: &str,
        node: u64,
        written: Vec<(u64, Layout)>,
        first: u64,
        regions: &[LayoutRegion],
    ) {
        self.ledger_custody(key, node, written, first, regions, true);
    }

    /// The custody ledger's update (see [`Self::note_synced`]): `rewritten`
    /// when the slots were written by this commit, which alone clears their
    /// marks; an open only reports what the disk holds, damage included.
    fn ledger_custody(
        &mut self,
        key: &str,
        node: u64,
        written: Vec<(u64, Layout)>,
        first: u64,
        regions: &[LayoutRegion],
        rewritten: bool,
    ) {
        let custody = self.custody.entry(key.to_string()).or_default();
        custody.node = node;
        if !written.is_empty() {
            custody.last_batch = written.iter().map(|(slot, _)| *slot).collect();
        }
        let slots: Vec<u64> = written.iter().map(|(slot, _)| *slot).collect();
        custody.records.extend(written);
        custody.first = custody.first.max(first);
        custody.records = custody.records.split_off(&custody.first);
        custody.meta = regions
            .iter()
            .filter(|r| r.kind == Layout::META)
            .cloned()
            .collect();
        custody.headers = regions
            .iter()
            .filter(|r| r.kind == Layout::HEADER)
            .cloned()
            .collect();
        custody.settled = true;
        custody.injected = false;
        // Pair of the budget: nothing the ledger holds sits below its floor.
        assert_always!(
            custody
                .records
                .keys()
                .next()
                .is_none_or(|s| *s >= custody.first),
            "journal store: the custody ledger holds nothing below its floor"
        );
        let floor = custody.first;
        for marks in [self.marks.get_mut(key), self.rotted.get_mut(key)]
            .into_iter()
            .flatten()
        {
            if rewritten {
                for slot in &slots {
                    marks.remove(slot);
                }
            }
            marks.retain(|slot| *slot >= floor);
        }
    }

    /// A journal store at `key` opened: its custody is what the journal now
    /// holds (`layouts`, every slot it reports), the floor `first` and its
    /// `regions`. A commit whose sync was cut may have landed, so a boot is
    /// where the ledger learns it. `faulty` are the slots the open reported
    /// faulty: their entry is damaged on disk (a cut commit's ambiguous last
    /// batch keeps its records and loses its entries), a lost copy until a
    /// rewrite, and no family aims at them (see `rotted`): a record rot
    /// there is a double fault no budget permitted.
    pub(crate) fn note_opened(
        &mut self,
        key: &str,
        node: u64,
        layouts: Vec<(u64, Layout)>,
        first: u64,
        regions: &[LayoutRegion],
        faulty: &[u64],
    ) {
        let custody = self.custody.entry(key.to_string()).or_default();
        let mut last_batch = std::mem::take(&mut custody.last_batch);
        // A slot the open found where the ledger never saw it came from a
        // cut commit: a new slot, or a held one re-accepted at a new place
        // (#529 (sets oracles after PR #515): witness 12728932213442680004,
        // a double fault aimed at a moved slot the journal then voided as
        // its torn last batch). It may be the journal's last batch, which a
        // double fault never touches.
        last_batch.extend(
            layouts
                .iter()
                .filter(|(slot, at)| custody.records.get(slot) != Some(at))
                .map(|(slot, _)| *slot),
        );
        custody.records.clear();
        self.ledger_custody(key, node, layouts, first, regions, false);
        if let Some(custody) = self.custody.get_mut(key) {
            custody.last_batch = last_batch;
        }
        let damaged: Vec<u64> = faulty.iter().copied().filter(|s| *s >= first).collect();
        // A faulty entry is a lost copy (a crash tore the last batch, or
        // rot took it), until a rewrite: the wipe budget counts it.
        self.marks
            .entry(key.to_string())
            .or_default()
            .extend(damaged.iter().copied());
        if !damaged.is_empty() {
            self.rotted
                .entry(key.to_string())
                .or_default()
                .extend(damaged);
        }
    }

    /// Plan this boot's injection for the journal store at `key` (node
    /// `node`): the copy an outage's plan lost, which every boot after it
    /// applies, else — only `in_chaos` — a family that fires (see the
    /// module doc). What it costs is recorded once it lands
    /// ([`Self::note_landed`]).
    ///
    /// A planned injection unsettles the custody (#441 (metainfo twin)):
    /// its apply changes bytes the ledger vouches for. Until the journal's
    /// next open reports what the disk holds ([`Self::note_opened`]), no
    /// family aims at that layout again.
    /// Witness: a boot died while it flipped one metainfo copy, and the
    /// next boot flipped the other, so the journal refused to open
    /// (seed 16594478988965922038 on #434 (matchmakers everywhere)). With a
    /// metainfo rot forced on every boot: 2 red seeds in 1,800 before, none
    /// in 1,800 after, the guarded boot met 85 times.
    pub(crate) fn plan_boot_damage(
        &mut self,
        key: &str,
        node: u64,
        in_chaos: bool,
        gate_off: bool,
    ) -> Option<Injection> {
        let injection = self.plan_injection(key, node, in_chaos, gate_off)?;
        let custody = self.custody.get_mut(key);
        // Pair of `plan_one`'s and the outage plans' guard: every family
        // aims only at a settled custody.
        assert_always!(
            custody.as_ref().is_some_and(|c| c.settled),
            "journal store: an injection is planned on a settled custody",
            { "node" => node }
        );
        if let Some(custody) = custody {
            custody.settled = false;
            custody.injected = true;
        }
        if !injection.outage && injection.double_fault().is_some() {
            self.reserve_park(key, node);
        }
        Some(injection)
    }

    /// `key`'s planned `injection` landed: an entry rot is a lost copy, a
    /// record rot a rotted record, and a double fault the park its plan
    /// reserved, which the journal's verdict makes terminal (#351).
    pub(crate) fn note_landed(&mut self, key: &str, injection: &Injection) {
        self.injections += 1;
        if injection.outage {
            return;
        }
        match injection.family {
            Family::EntryRot(slot) => {
                self.marks.entry(key.to_string()).or_default().insert(slot);
            }
            Family::RecordRot(slot) => {
                self.rotted.entry(key.to_string()).or_default().insert(slot);
            }
            Family::DoubleFault(_) => self.park_landed(key, injection.clone()),
            Family::MetaRot | Family::HeaderRot => {}
        }
    }

    /// [`Self::plan_boot_damage`]'s choice, before it unsettles the custody.
    fn plan_injection(
        &mut self,
        key: &str,
        node: u64,
        in_chaos: bool,
        gate_off: bool,
    ) -> Option<Injection> {
        if let Some(slot) = self.pending.remove(key) {
            // The plan's own claim: it was made on a settled custody, and
            // nothing since (the node was down) moved it.
            let custody = self.custody.get(key);
            assert_always!(
                custody.is_some_and(|c| c.settled),
                "journal store: an outage's planned loss finds its custody settled",
                { "node" => node, "slot" => slot }
            );
            if let Some(layout) = custody.and_then(|c| c.records.get(&slot)) {
                return Some(Injection {
                    family: Family::EntryRot(slot),
                    regions: vec![layout.entry.clone()],
                    outage: true,
                });
            }
        }
        if !in_chaos {
            return None;
        }
        if self.custody.get(key).is_some_and(|c| c.injected) {
            // The boot the unsettled custody guards: the bytes an earlier
            // apply flipped are unread until the journal opens.
            assert_reachable!(
                "journal store: a boot after an interrupted injection aims at nothing"
            );
            return None;
        }
        self.plan_one(key, gate_off)
    }

    /// Plan a correlated outage's loss in this journal (#263, see
    /// [`super::outage`]): while every node is down, one slot the custody
    /// ledger holds loses its copy on some holders, applied at each one's
    /// next boot, aimed at a slot the audit saw `decided` (already acked:
    /// rot after the fact is latent damage). Under the usual budget a quorum of clean copies stays;
    /// under the loss budget's extreme only `loss.keep` clean copies do.
    /// `None` on a journal with no budget (a quiet one) or nothing to aim
    /// at.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn plan_outage_loss(
        &mut self,
        loss: LossShape,
        decided: &BTreeMap<u64, Vec<u64>>,
        keeps: &dyn Fn(u64, &[String]) -> bool,
    ) -> Option<PlannedLoss> {
        if self.cluster_size == 0 {
            return None;
        }
        let live: Vec<(&String, &Custody)> = self
            .custody
            .iter()
            .filter(|(key, _)| !self.replicas.contains(*key) && !self.parked.contains_key(*key))
            .collect();
        let installed = self.last_installed();
        let slot = Self::aim_outage_loss(&live, loss, decided, installed.as_deref())?;
        let holders: Vec<(String, u64, bool)> = live
            .iter()
            .filter(|(_, c)| c.records.contains_key(&slot))
            .map(|(key, c)| ((*key).clone(), c.node, c.settled))
            .collect();
        // The holders the plan may damage: settled ones (an unsettled
        // custody's layout is not known), not quiet (#471), not already
        // lost. The departed
        // ones go last, so a kept copy lands on them: a member of the
        // configuration that decided the slot, outside the one the operator
        // last installed. A spare no configuration named holds a copy no
        // Phase 1 asks (witness 1980540850679778313), so it is no straggler.
        let deciders = decided.get(&slot);
        let departed = |node: &u64| {
            installed
                .as_ref()
                .is_some_and(|members| !members.contains(node))
                && deciders.is_some_and(|members| members.contains(node))
        };
        let mut damageable: Vec<(String, u64)> = holders
            .iter()
            .filter(|(key, _, settled)| {
                *settled
                    && !self.quiet.contains(key)
                    && !self.marks.get(key).is_some_and(|m| m.contains(&slot))
                    && !self.rotted.get(key).is_some_and(|m| m.contains(&slot))
            })
            .map(|(key, node, _)| (key.clone(), *node))
            .collect();
        // A spare (`LossShape::spare_kept`): a holder neither the last
        // installed configuration nor the deciding one names, kept clean
        // after the straggler (#375).
        let spare = |node: &u64| {
            loss.spare_kept
                && installed
                    .as_ref()
                    .is_some_and(|members| !members.contains(node))
                && deciders.is_some_and(|members| !members.contains(node))
        };
        if loss.prefer_removed {
            damageable.sort_by_key(|(_, node)| (spare(node), departed(node)));
        }
        // The departed straggler (`LossShape::prefer_removed`): a removed
        // holder's copy is the one left clean, and the only member's — kept
        // beside a quorum of clean members it would be no shape at all.
        let straggler = loss.prefer_removed && damageable.iter().any(|(_, node)| departed(node));
        let spares = damageable.iter().filter(|(_, node)| spare(node)).count();
        if straggler && spares > 0 {
            assert_reachable!("storage: an outage keeps a spare's copy beside the straggler");
        }
        let keep = if straggler {
            Some(1 + spares)
        } else {
            loss.keep
        };
        let count = match keep {
            Some(keep) if self.loss_permitted(slot, loss.loss_budget()) => if straggler {
                damageable.len()
            } else {
                holders.len()
            }
            .saturating_sub(keep),
            _ => self.usual_loss(slot, &damageable, keeps),
        }
        .min(damageable.len());
        if count == 0 {
            return None;
        }
        let damaged: Vec<(String, u64)> = damageable.drain(..count).collect();
        for (key, _) in &damaged {
            assert_always!(
                !self.quiet.contains(key),
                "storage: a planned loss marks only a store whose boot applies it",
                { "slot" => slot }
            );
            self.marks.entry(key.clone()).or_default().insert(slot);
            self.pending.insert(key.clone(), slot);
        }
        if self.clean_copies(slot) < self.quorum() || !keeps(slot, &self.lost_with(slot, &[])) {
            self.lossy.insert(slot);
            assert_reachable!("storage: an outage spends the loss budget");
        }
        assert_always!(
            self.lossy.len() <= loss.loss_budget(),
            "storage: an outage never loses more slots than the loss budget",
            { "slot" => slot, "lost" => u64::try_from(self.lossy.len()).unwrap_or(u64::MAX) }
        );
        if straggler {
            assert_reachable!("storage: an outage leaves a clean copy on a removed node");
        }
        Some(PlannedLoss {
            slot,
            holders: holders.iter().map(|(_, node, _)| *node).collect(),
            damaged: damaged.iter().map(|(_, node)| *node).collect(),
        })
    }

    /// How many of `damageable`'s copies of `slot` an outage outside the
    /// loss budget loses: down to a quorum of clean copies, and never below
    /// the copies the journal's shard needs (`keeps`, #760 (moonpool owns
    /// fault placement)). The damage lands past the gate, and the
    /// configuration floor's quorum is less than a grown pool's: witness
    /// 15555488400013153756 on #774 (moonpool places the harness faults),
    /// where a fourth member made the need 3 and the plan left 2.
    fn usual_loss(
        &self,
        slot: u64,
        damageable: &[(String, u64)],
        keeps: &dyn Fn(u64, &[String]) -> bool,
    ) -> usize {
        let mut count = self
            .clean_copies(slot)
            .saturating_sub(self.quorum())
            .min(damageable.len());
        while count > 0 && !keeps(slot, &self.lost_with(slot, &damageable[..count])) {
            count -= 1;
        }
        count
    }

    /// The stores whose copy of `slot` is lost, with `more`: what a shard's
    /// need is judged against.
    fn lost_with(&self, slot: u64, more: &[(String, u64)]) -> Vec<String> {
        self.marks
            .iter()
            .filter(|(_, marks)| marks.contains(&slot))
            .map(|(key, _)| key.clone())
            .chain(more.iter().map(|(key, _)| key.clone()))
            .collect()
    }

    /// Whether the custody ledger holds a decided slot a loss could leave
    /// bare: a member of its deciding configuration never held it and every
    /// copy is settled (see [`Self::short_of_a_member`]). The bare-quorum
    /// scenario strikes on it (`super::scenarios::bare_outage`).
    pub(crate) fn holds_short_slot(&self, decided: &BTreeMap<u64, Vec<u64>>) -> bool {
        let live: Vec<(&String, &Custody)> = self
            .custody
            .iter()
            .filter(|(key, _)| !self.replicas.contains(*key) && !self.parked.contains_key(*key))
            .collect();
        let Some(floor) = live.iter().map(|(_, c)| c.first).max() else {
            return false;
        };
        decided
            .range(floor..)
            .any(|(slot, _)| Self::short_of_a_member(&live, *slot, decided))
    }

    /// The split floor (#409, `super::scenarios::split_floor`), if one shows: the
    /// acceptors whose floor is the highest (the ahead ones), and the
    /// lowest decided slot below it that an acceptor behind them still
    /// holds, at or above every behind acceptor's floor. Settled or not.
    pub(crate) fn split_floor(&self, decided: &BTreeMap<u64, Vec<u64>>) -> Option<(Vec<u64>, u64)> {
        let live: Vec<&Custody> = self
            .custody
            .iter()
            .filter(|(key, _)| !self.replicas.contains(*key) && !self.parked.contains_key(*key))
            .map(|(_, c)| c)
            .collect();
        let ahead_floor = live.iter().map(|c| c.first).max()?;
        let behind: Vec<&&Custody> = live.iter().filter(|c| c.first < ahead_floor).collect();
        let behind_floor = behind.iter().map(|c| c.first).max()?;
        let slot = decided
            .range(behind_floor..ahead_floor)
            .map(|(slot, _)| *slot)
            .find(|slot| behind.iter().any(|c| c.records.contains_key(slot)))?;
        let ahead = live
            .iter()
            .filter(|c| c.first == ahead_floor)
            .map(|c| c.node)
            .collect();
        Some((ahead, slot))
    }

    /// Plan the split-floor outage's loss (#409, `super::scenarios::split_floor`):
    /// at the split floor ([`Self::split_floor`]), lose every copy of the
    /// slot the acceptors behind it hold. The slot is chosen and truncated
    /// on the ahead acceptors, so nothing is lost: a leader blocked on it
    /// (CTRL Case 3) jumps past it to an ahead acceptor's trim point.
    /// `None` while no split shows, or while a holder of the slot is
    /// unsettled (its layout is not known, so its copy would stay clean and
    /// decide the slot). A quiet holder's copy is kept (#471: its boot
    /// applies no loss).
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn plan_split_floor_loss(
        &mut self,
        decided: &BTreeMap<u64, Vec<u64>>,
    ) -> Option<PlannedLoss> {
        if self.cluster_size == 0 {
            return None;
        }
        let (ahead, slot) = self.split_floor(decided)?;
        let holders: Vec<(String, u64, bool)> = self
            .custody
            .iter()
            .filter(|(key, c)| {
                !self.replicas.contains(*key)
                    && !self.parked.contains_key(*key)
                    && c.records.contains_key(&slot)
            })
            .map(|(key, c)| (key.clone(), c.node, c.settled))
            .collect();
        if holders.iter().any(|(_, _, settled)| !settled) {
            return None;
        }
        // A quiet holder (#471, a joiner's seat) keeps its copy: its boot
        // applies no loss, so a mark there would never clear. Waiting for it
        // instead, as for an unsettled holder, waits forever: it cost the
        // scenario 17 of 39 strikes in 2,000 hunt seeds. Its clean copy
        // decides the slot only where a Phase 1 asks it.
        if holders.iter().any(|(key, _, _)| self.quiet.contains(key)) {
            assert_reachable!("storage: a split-floor loss keeps a quiet holder's copy");
        }
        let damaged: Vec<(String, u64)> = holders
            .iter()
            .filter(|(key, _, _)| {
                !self.quiet.contains(key)
                    && !self.marks.get(key).is_some_and(|m| m.contains(&slot))
                    && !self.rotted.get(key).is_some_and(|m| m.contains(&slot))
            })
            .map(|(key, node, _)| (key.clone(), *node))
            .collect();
        if damaged.is_empty() {
            return None;
        }
        // An ahead acceptor truncated the slot: it holds no copy to lose.
        assert_always!(
            damaged.iter().all(|(_, node)| !ahead.contains(node)),
            "storage: a split-floor loss never damages the ahead acceptor",
            { "slot" => slot }
        );
        for (key, _) in &damaged {
            assert_always!(
                !self.quiet.contains(key),
                "storage: a planned loss marks only a store whose boot applies it",
                { "slot" => slot }
            );
            self.marks.entry(key.clone()).or_default().insert(slot);
            self.pending.insert(key.clone(), slot);
        }
        assert_reachable!("storage: a split-floor outage loses a slot below a peer's floor");
        Some(PlannedLoss {
            slot,
            holders: holders.iter().map(|(_, node, _)| *node).collect(),
            damaged: damaged.iter().map(|(_, node)| *node).collect(),
        })
    }

    /// The slot an outage's loss aims at (see [`Self::plan_outage_loss`]):
    /// a decided slot above every holder's floor, the most recent or a
    /// uniform one.
    fn aim_outage_loss(
        live: &[(&String, &Custody)],
        loss: LossShape,
        decided: &BTreeMap<u64, Vec<u64>>,
        installed: Option<&[u64]>,
    ) -> Option<u64> {
        // Above every floor, so no holder answers with a trim point instead.
        // A last batch is fair game: its persist records survive, so a
        // damaged entry there is reported faulty like any other.
        let floor = live.iter().map(|(_, c)| c.first).max()?;
        let mut candidates: BTreeSet<u64> = live
            .iter()
            .filter(|(_, c)| c.settled)
            .flat_map(|(_, c)| c.records.range(floor..).map(|(slot, _)| *slot))
            .filter(|slot| decided.contains_key(slot))
            .collect();
        // The departed straggler aims only where it can land: a slot a
        // settled, departed holder keeps (see `plan_outage_loss`), when one
        // does. A slot decided under the successor has no such holder.
        if loss.prefer_removed {
            let departed_slots: BTreeSet<u64> = candidates
                .iter()
                .copied()
                .filter(|slot| {
                    live.iter().any(|(_, c)| {
                        c.settled
                            && c.records.contains_key(slot)
                            && installed.is_some_and(|m| !m.contains(&c.node))
                            && decided.get(slot).is_some_and(|m| m.contains(&c.node))
                    })
                })
                .collect();
            if !departed_slots.is_empty() {
                assert_reachable!("storage: an outage aims at a slot a departed member holds");
                candidates = departed_slots;
            }
            // Among those, a slot a majority of the successor never held:
            // only there does the newest configuration alone hold a quorum
            // of `none` answers, so only there is the cross-configuration
            // Phase 1 the one thing standing between the slot and a no-op
            // fill (#267). An unsettled custody may hold anything, so it
            // counts as a holder.
            // With the spares kept clean (`LossShape::spare_kept`), a slot a
            // settled spare holds instead: a successor that holds it too
            // lost its copies above the straggler's ballot, so only the
            // spare's copy, which no Phase 1 asks, would qualify them (#375).
            let spare_held: BTreeSet<u64> = candidates
                .iter()
                .copied()
                .filter(|slot| {
                    loss.spare_kept
                        && live.iter().any(|(_, c)| {
                            c.settled
                                && c.records.contains_key(slot)
                                && installed.is_some_and(|m| !m.contains(&c.node))
                                && decided.get(slot).is_some_and(|m| !m.contains(&c.node))
                        })
                })
                .collect();
            if !spare_held.is_empty() {
                assert_reachable!("storage: an outage aims at a departed slot a spare holds");
                candidates = spare_held;
            }
            let never_held: BTreeSet<u64> = candidates
                .iter()
                .copied()
                .filter(|slot| {
                    !loss.spare_kept && Self::successor_never_held(live, *slot, installed)
                })
                .collect();
            if !never_held.is_empty() {
                assert_reachable!(
                    "storage: an outage aims at a departed slot most of the successor never held"
                );
                candidates = never_held;
            }
        }
        // The bare quorum aims where its shape can exist: a slot decided on
        // a quorum short of a member, every copy settled so each can be
        // lost. The most recent slot alone is usually held by every member
        // (the gate fired on 0 of 1,024 CI seeds, #270).
        if loss.short {
            let short: BTreeSet<u64> = candidates
                .iter()
                .copied()
                .filter(|slot| Self::short_of_a_member(live, *slot, decided))
                .collect();
            if !short.is_empty() {
                assert_reachable!("storage: an outage aims at a slot a deciding member never held");
                candidates = short;
            }
        }
        if loss.recent {
            assert_reachable!("storage: an outage aims at the most recent slot it holds");
            candidates.last().copied()
        } else {
            let all: Vec<u64> = candidates.iter().copied().collect();
            let at = moonpool_sim::sim_random_range(0..all.len().max(1) as u64);
            all.get(usize::try_from(at).unwrap_or(0)).copied()
        }
    }

    /// Whether a strict majority of `installed` never held `slot`: no
    /// custody of theirs that is settled holds it, and none is unsettled.
    fn successor_never_held(
        live: &[(&String, &Custody)],
        slot: u64,
        installed: Option<&[u64]>,
    ) -> bool {
        let Some(installed) = installed else {
            return false;
        };
        let never = installed
            .iter()
            .filter(|member| {
                live.iter()
                    .filter(|(_, c)| c.node == **member)
                    .all(|(_, c)| c.settled && !c.records.contains_key(&slot))
            })
            .count();
        never > installed.len() / 2
    }

    /// Whether some member of the configuration that decided `slot` never
    /// held it (every custody of theirs settled without it), while every
    /// custody holding it is settled, so a loss can take each copy.
    fn short_of_a_member(
        live: &[(&String, &Custody)],
        slot: u64,
        decided: &BTreeMap<u64, Vec<u64>>,
    ) -> bool {
        let Some(members) = decided.get(&slot) else {
            return false;
        };
        let holders_settled = live
            .iter()
            .filter(|(_, c)| c.records.contains_key(&slot))
            .all(|(_, c)| c.settled);
        let never_held = members.iter().any(|member| {
            let mut custodies = live.iter().filter(|(_, c)| c.node == *member).peekable();
            custodies.peek().is_some()
                && custodies.all(|(_, c)| c.settled && !c.records.contains_key(&slot))
        });
        holders_settled && never_held
    }

    /// Whether `slot` may lose its clean quorum: it already did, or the loss
    /// budget has room (see [`LossShape::loss_budget`]).
    fn loss_permitted(&self, slot: u64, budget: usize) -> bool {
        self.lossy.contains(&slot) || self.lossy.len() < budget
    }

    /// [`Self::plan_boot_damage`]'s draw. `gate_off`: moonpool's shard gate
    /// is off for this journal, so it judges no injection, and this draw
    /// keeps the copy budget itself (defense in depth).
    fn plan_one(&mut self, key: &str, gate_off: bool) -> Option<Injection> {
        let custody = self.custody.get(key)?.clone();
        if !custody.settled || self.parked.contains_key(key) {
            return None;
        }
        // An outage's planned loss on another store has not landed yet, so
        // the gate cannot see it (#760 (moonpool owns fault placement)): no
        // family aims until it lands. Witness 15555488400013153756 on #774
        // (moonpool places the harness faults): an entry rot took a copy the
        // gate judged spare, then the planned loss took a second one.
        if self.pending.keys().any(|other| other != key) {
            assert_reachable!("journal store: an injection waits for an outage's planned loss");
            return None;
        }
        let pick = |slots: &[u64]| -> Option<u64> {
            if slots.is_empty() {
                return None;
            }
            let at = moonpool_sim::sim_random_range(0..slots.len() as u64);
            slots.get(usize::try_from(at).unwrap_or(0)).copied()
        };
        let held: Vec<u64> = custody.records.keys().copied().collect();
        // Every family aims only where the entry and the record are whole.
        // On a slot whose entry an earlier rot (or an outage's planned loss)
        // took and no rewrite has cleared, a record rot would be a double
        // fault the budget never permitted, and a second flip of the entry
        // (an entry rot, a double fault) would put the same bytes back. A
        // rotted record stays damaged on disk (see `rotted`): an entry rot
        // there is the same unbudgeted double fault, a second record rot
        // the same restoring flip.
        let marked = self.marks.get(key);
        let rotted = self.rotted.get(key);
        let whole: Vec<u64> = held
            .iter()
            .copied()
            .filter(|slot| !marked.is_some_and(|marks| marks.contains(slot)))
            .filter(|slot| !rotted.is_some_and(|rotted| rotted.contains(slot)))
            // With the gate off (a scenario's `switch_off_shards_of`, or the
            // run's shard faults), nothing judges the copy budget: a family
            // aims only where the slot keeps a clean quorum without this
            // copy. Witness 5284606952228265407: with the gate off, an entry
            // rot took the last clean copy of slot 0, and no slot was ever
            // chosen.
            .filter(|slot| !gate_off || self.clean_copies(*slot) > self.quorum())
            .collect();
        // Aimed by what the ledger knows (#263): on some boots the most
        // recent slot this node holds, the one a lagging peer is likeliest
        // to still need; on others the oldest, the one likeliest to lie
        // under the durable chosen index, where the lost copy stops the
        // journal fold at a hole the node must pull back (#343); else a
        // uniform one.
        let aim = |held: &[u64]| {
            if buggify_with_prob!(0.5) {
                assert_reachable!("journal store: an entry rot aims at the most recent slot held");
                held.last().copied()
            } else if buggify_with_prob!(0.5) {
                assert_reachable!("journal store: an entry rot aims at the oldest slot held");
                held.first().copied()
            } else {
                pick(held)
            }
        };
        // The double fault rolled first (#351), its own BUGGIFY location:
        // last in the order, it fired on 1 of 2,000 hunt seeds, too rare
        // to reach a reserved park whose apply never landed whole.
        if buggify_with_prob!(0.5)
            && let Some(injection) = self.plan_double_fault(key, &custody, &whole)
        {
            assert_reachable!("journal store: a boot rolls the double fault first");
            return Some(injection);
        }
        if buggify_with_prob!(P_ENTRY_ROT)
            && let Some(slot) = aim(&whole)
        {
            return Some(Injection {
                family: Family::EntryRot(slot),
                regions: vec![custody.records[&slot].entry.clone()],
                outage: false,
            });
        }
        if buggify_with_prob!(P_RECORD_ROT)
            && let Some(slot) = pick(&whole)
        {
            return Some(Injection {
                family: Family::RecordRot(slot),
                regions: vec![custody.records[&slot].record.clone()],
                outage: false,
            });
        }
        if buggify_with_prob!(P_DOUBLE_FAULT)
            && let Some(injection) = self.plan_double_fault(key, &custody, &whole)
        {
            return Some(injection);
        }
        if buggify_with_prob!(P_META_ROT) && custody.meta.len() == 2 {
            let copy = usize::from(moonpool_sim::sim_random_bool(0.5));
            return Some(Injection {
                family: Family::MetaRot,
                regions: vec![custody.meta[copy].clone()],
                outage: false,
            });
        }
        if buggify_with_prob!(P_HEADER_ROT) && !custody.headers.is_empty() {
            let at = moonpool_sim::sim_random_range(0..custody.headers.len() as u64);
            let header = custody.headers[usize::try_from(at).unwrap_or(0)].clone();
            return Some(Injection {
                family: Family::HeaderRot,
                regions: vec![header],
                outage: false,
            });
        }
        None
    }
}

impl StorageWorld {
    /// A double fault on a `whole` slot of `custody` outside its last
    /// batch, when the dead-node budget permits a park.
    fn plan_double_fault(&self, key: &str, custody: &Custody, whole: &[u64]) -> Option<Injection> {
        let settled: Vec<u64> = whole
            .iter()
            .copied()
            .filter(|slot| !custody.last_batch.contains(slot))
            .collect();
        if settled.is_empty() || !self.may_park(key) {
            return None;
        }
        let at = moonpool_sim::sim_random_range(0..settled.len() as u64);
        let slot = settled[usize::try_from(at).unwrap_or(0)];
        let layout = &custody.records[&slot];
        Some(Injection {
            family: Family::DoubleFault(slot),
            regions: vec![layout.record.clone(), layout.entry.clone()],
            outage: false,
        })
    }
}

/// Flip [`FLIPPED`] bytes near the start of every region of `injection`
/// through moonpool's shard gate (see the module doc), after a flush of
/// every file it touches, so no cached write covers the flip. Whether the
/// whole injection landed: a refused or failed one changed nothing, and is
/// no injection.
#[tracing::instrument(level = "debug", skip_all, fields(family = ?injection.family))]
pub(crate) async fn apply(provider: &SimStorageProvider, injection: &Injection) -> bool {
    let Some(spans) = injection
        .regions
        .iter()
        .map(flipped)
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let paths: BTreeSet<&str> = injection.regions.iter().map(|r| r.path.as_str()).collect();
    for path in paths {
        if !flush(provider, path).await {
            return false;
        }
    }
    // A double fault gives the whole node up: the gate judges that first.
    if injection.double_fault().is_some() && !provider.lose_replica().unwrap_or(false) {
        assert_reachable!("journal store: the shard gate refuses a double fault");
        return false;
    }
    for (at, (region, span)) in injection.regions.iter().zip(spans).enumerate() {
        let landed = if injection.outage {
            // A planned loss may go past the journal's need on purpose
            // (`super::outage::LossShape`): the gate sees it, never judges it.
            provider.corrupt_bytes_past_gate(&region.path, span).is_ok()
        } else {
            let landed = provider.corrupt_bytes(&region.path, span).unwrap_or(false);
            if !landed {
                assert_always!(
                    at == 0,
                    "journal store: a double fault's gate takes both its regions"
                );
                assert_reachable!("journal store: the shard gate refuses an injection");
            }
            landed
        };
        if !landed {
            return false;
        }
    }
    true
}

/// The bytes of `region` a flip changes: [`FLIPPED`] bytes near its start,
/// where a header or metainfo copy's checksum covers its encoded fields.
fn flipped(region: &LayoutRegion) -> Option<std::ops::Range<u64>> {
    let len = region.bytes.end.saturating_sub(region.bytes.start);
    let at = region.bytes.start + FLIPPED_AT;
    (len >= FLIPPED_AT + FLIPPED).then_some(at..at + FLIPPED)
}

/// Flip one region (see [`apply`]) through the shard gate, after a flush:
/// whether it landed.
pub(crate) async fn flip(provider: &SimStorageProvider, region: &LayoutRegion) -> bool {
    let Some(span) = flipped(region) else {
        return false;
    };
    flush(provider, &region.path).await
        && provider.corrupt_bytes(&region.path, span).unwrap_or(false)
}

/// Make every write to `path` durable, so a flip of its durable bytes is
/// what the next read finds.
async fn flush(provider: &SimStorageProvider, path: &str) -> bool {
    let Ok(file) = provider.open(path, OpenOptions::read_write()).await else {
        return false;
    };
    for _ in 0..WRITE_ATTEMPTS {
        if file.sync_all().await.is_ok() {
            return true;
        }
    }
    false
}

/// Judge what the journal reported at open against what was injected (see
/// the module doc): every injected damage resolves to exactly the verdict
/// its family names, each paired with its reachable. `opened` is the boot
/// scan's result, `faulty` the slots it reported faulty, `reopened` whether
/// the boot re-opened after a transient fault: a re-open finds a copy the
/// failed attempt already repaired (a record, a metainfo copy, a header)
/// whole, so those families' verdicts are then inconclusive and not judged.
/// Whether the verdict was a crash decision (a refused journal), for the
/// world's exercised-detected count.
pub(crate) fn judge(
    injection: &Injection,
    opened: &Result<(), StorageError>,
    facts: &JournalBootFacts,
    faulty: &[u64],
    node: u64,
    reopened: bool,
) -> bool {
    let repaired_locally = matches!(
        injection.family,
        Family::RecordRot(_) | Family::MetaRot | Family::HeaderRot
    );
    if reopened && repaired_locally {
        return false;
    }
    match injection.family {
        Family::EntryRot(slot) => {
            assert_always!(
                opened.is_ok() && faulty.contains(&slot),
                "journal store: an injected entry rot is reported faulty",
                { "node" => node, "slot" => slot }
            );
            assert_reachable!("journal store: a boot finds an injected entry rot");
        }
        Family::RecordRot(slot) => {
            assert_always!(
                opened.is_ok() && facts.recovery.rebuilt > 0 && !faulty.contains(&slot),
                "journal store: an injected record rot is rebuilt from its entry",
                { "node" => node, "slot" => slot }
            );
            assert_reachable!("journal store: a boot rebuilds an injected record rot");
        }
        Family::DoubleFault(slot) => {
            assert_always!(
                matches!(opened, Err(StorageError::Corruption { .. })),
                "journal store: an injected double fault refuses the journal",
                { "node" => node, "slot" => slot }
            );
            assert_reachable!("journal store: a boot refuses an injected double fault");
            return opened.is_err();
        }
        Family::MetaRot => {
            assert_always!(
                opened.is_ok() && facts.recovery.meta_repaired,
                "journal store: an injected metainfo rot is repaired from its twin",
                { "node" => node }
            );
            assert_reachable!("journal store: a boot repairs an injected metainfo rot");
        }
        Family::HeaderRot => {
            assert_always!(
                opened.is_ok() && facts.recovery.headers_repaired > 0,
                "journal store: an injected header rot is repaired from its twin",
                { "node" => node }
            );
            assert_reachable!("journal store: a boot repairs an injected header rot");
        }
    }
    false
}
