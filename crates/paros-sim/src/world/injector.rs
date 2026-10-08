//! The **ledgered, journal-aware injector** (#261): the corruption chaos the
//! world store's boot-rot sites give its fake disk, given to the shipped
//! `JournalStorage` on the simulated disk, aimed by what the journal itself
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
//! | entry rot | a slot's entry | a lost copy: [`StorageWorld::may_corrupt_record`] | the slot reported faulty (CTRL's recoverable class) |
//! | record rot | a slot's persist record | none: repaired locally | the record rebuilt from its entry |
//! | double fault | both, outside the last batch | a lost node: [`StorageWorld::may_park`] | the journal refuses to open (`DoubleFault`), the node parked |
//! | metainfo rot | one metainfo copy | none: repaired from its twin | the copy repaired |
//! | header rot | one segment header copy | none: repaired from its twin | the header repaired |
//!
//! **The ground truth** is the injection set against the journal's own
//! verdicts at open ([`judge`]): every injected damage resolves to exactly the
//! verdict its family names. Metainfo and header damage wait for a settled
//! store (both copies written by a completed commit), and a double fault
//! never touches the last batch, where the journal reads a doubly damaged
//! slot as a torn, unacknowledged write and would drop an acknowledged vote.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{
    OpenOptions, SimStorageProvider, StorageFile, StorageProvider, assert_always, assert_reachable,
    buggify_with_prob,
};
use paros::journal::{Layout, LayoutRegion};
use paros::{JournalBootFacts, StorageError};

use super::{ParkReason, StorageWorld};

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

/// How many times the damage's own write and sync are retried: moonpool's
/// storage chaos fails a sync or shortens a transfer now and then, never for
/// long. An injection that never confirms is uncertain and not judged.
const WRITE_ATTEMPTS: usize = 64;

/// How many times a boot that carried an injection re-opens its journal
/// after a transient I/O fault, before the verdict is judged: the same hang
/// guard as [`WRITE_ATTEMPTS`].
pub(crate) const REOPEN_ATTEMPTS: usize = 64;

/// One journal store's custody, as of its last completed sync (see the
/// module doc).
#[derive(Clone, Debug, Default)]
pub(crate) struct Custody {
    /// Whether the last sync that started also completed: only then is the
    /// layout below what the disk holds.
    settled: bool,
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
    /// written slot is durably real again: its fault mark clears, as the
    /// world store's flush clears it.
    pub(crate) fn note_synced(
        &mut self,
        key: &str,
        written: Vec<(u64, Layout)>,
        first: u64,
        regions: &[LayoutRegion],
    ) {
        let custody = self.custody.entry(key.to_string()).or_default();
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
        if let Some(marks) = self.marks.get_mut(key) {
            for slot in slots {
                marks.remove(&slot);
            }
            marks.retain(|slot| *slot >= floor);
        }
    }

    /// Plan this boot's injection for the journal store at `key` (node
    /// `node`), if a family fires and its budget allows (see the module
    /// doc). A permitted entry rot marks the copy lost; a permitted double
    /// fault parks the node, before the journal ever opens.
    pub(crate) fn plan_boot_damage(&mut self, key: &str, node: u64) -> Option<Injection> {
        let custody = self.custody.get(key)?.clone();
        if !custody.settled || self.parked.contains_key(key) {
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
        if buggify_with_prob!(P_ENTRY_ROT)
            && let Some(slot) = pick(&held)
            && self.may_corrupt_record(key, slot)
        {
            self.marks.entry(key.to_string()).or_default().insert(slot);
            // The budget's own claim, at injection: the copy left behind is
            // within it.
            assert_always!(
                self.clean_copies(slot) >= self.quorum(),
                "journal store: an injected entry rot keeps a clean quorum of copies",
                { "node" => node, "slot" => slot }
            );
            return Some(Injection {
                family: Family::EntryRot(slot),
                regions: vec![custody.records[&slot].entry.clone()],
            });
        }
        if buggify_with_prob!(P_RECORD_ROT)
            && let Some(slot) = pick(&held)
        {
            return Some(Injection {
                family: Family::RecordRot(slot),
                regions: vec![custody.records[&slot].record.clone()],
            });
        }
        let settled: Vec<u64> = held
            .iter()
            .copied()
            .filter(|slot| !custody.last_batch.contains(slot))
            .collect();
        if buggify_with_prob!(P_DOUBLE_FAULT)
            && let Some(slot) = pick(&settled)
            && self.may_park(key)
        {
            self.park_as(key, node, ParkReason::Corruption);
            let layout = &custody.records[&slot];
            return Some(Injection {
                family: Family::DoubleFault(slot),
                regions: vec![layout.record.clone(), layout.entry.clone()],
            });
        }
        if buggify_with_prob!(P_META_ROT) && custody.meta.len() == 2 {
            let copy = usize::from(moonpool_sim::sim_random_bool(0.5));
            return Some(Injection {
                family: Family::MetaRot,
                regions: vec![custody.meta[copy].clone()],
            });
        }
        if buggify_with_prob!(P_HEADER_ROT) && !custody.headers.is_empty() {
            let at = moonpool_sim::sim_random_range(0..custody.headers.len() as u64);
            let header = custody.headers[usize::try_from(at).unwrap_or(0)].clone();
            return Some(Injection {
                family: Family::HeaderRot,
                regions: vec![header],
            });
        }
        None
    }
}

/// Flip [`FLIPPED`] bytes near the start of every region of `injection`,
/// through the node's own provider, and sync them. Whether every write and
/// sync confirmed: an unconfirmed injection may or may not have landed.
#[tracing::instrument(level = "debug", skip_all, fields(family = ?injection.family))]
pub(crate) async fn apply(provider: &SimStorageProvider, injection: &Injection) -> bool {
    for region in &injection.regions {
        if !flip(provider, region).await {
            return false;
        }
    }
    true
}

/// Flip the bytes of one region (see [`apply`]).
async fn flip(provider: &SimStorageProvider, region: &LayoutRegion) -> bool {
    let len = region.bytes.end.saturating_sub(region.bytes.start);
    if len < FLIPPED_AT + FLIPPED {
        return false;
    }
    // Near the region's start: a header or metainfo copy's checksum covers
    // its encoded fields, not the rest of its block.
    let at = region.bytes.start + FLIPPED_AT;
    let Ok(file) = provider.open(&region.path, OpenOptions::read_write()).await else {
        return false;
    };
    let mut bytes = vec![0_u8; usize::try_from(FLIPPED).unwrap_or(8)];
    let mut read = 0;
    for _ in 0..WRITE_ATTEMPTS {
        if read == bytes.len() {
            break;
        }
        match file.read_at(at + read as u64, &mut bytes[read..]).await {
            Ok(0) | Err(_) => return false,
            Ok(n) => read += n,
        }
    }
    if read != bytes.len() {
        return false;
    }
    for byte in &mut bytes {
        *byte ^= 0xA5;
    }
    let mut written = 0;
    for _ in 0..WRITE_ATTEMPTS {
        if written == bytes.len() {
            break;
        }
        match file.write_at(at + written as u64, &bytes[written..]).await {
            Ok(0) | Err(_) => {}
            Ok(n) => written += n,
        }
    }
    if written != bytes.len() {
        return false;
    }
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
