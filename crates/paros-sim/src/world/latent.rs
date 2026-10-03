//! The Stage-7 latent faults (#176): damage that surfaced while a node was
//! down, injected into its journal's files at the boot that immediately
//! reads them back. One independent BUGGIFY location per fault family,
//! every one of them budgeted by the [`StorageWorld`].
//!
//! Each family lands on the bytes the journal's own recovery classifies
//! (`moonpool-journal`'s table, the store's per-kind reaction on top):
//!
//! | Family | Damage | What the store's scan does |
//! |---|---|---|
//! | entry rot (+ block) | every live copy of the slot's record fails its CRC, its identifier intact | reports the slot `faulty(slot, ballot)` and keeps serving |
//! | identifier lost with its entry | the same, and the newest copy's identifier garbled | refuses to start (a double fault): crash, parked |
//! | lost write | every live copy zeroed, its identifier intact | reports the slot faulty |
//! | misdirected write | every live copy's header replaced by another entry's | reports the slot faulty |
//! | one promise copy | one of `meta.0` / `meta.1` garbled | repairs it from its twin |
//! | both promise copies | both garbled | refuses to start: crash, parked |
//! | fs metadata | the newest segment grown past its geometry | refuses to open: crash, parked |
//! | read `EIO` | none on disk: the scan's read fails once | crash, and the retry reads clean |
//!
//! Every copy of a slot's record is damaged, not only the newest: a
//! checkpoint repeats records the log still holds before it, and the fold
//! reads whichever copy its plan trusts. Damaging them all makes the outcome
//! the table's whatever the plan.
//!
//! The decision and its ledger entry are made under the world lock in one
//! step (the budget is cluster-wide and every node boots concurrently); the
//! damage lands after, through the node's own disk, and is marked
//! [`landed`](super::CorruptionInjection::landed) once synced; a crash
//! family's park is reserved under the dead-node budget at the decision and
//! made only once its damage lands. A kill in between leaves the injection
//! unconfirmed, and the next boot's scan says whether it happened
//! (`StorageWorld::resolve_boot`).

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use moonpool_sim::{
    SimStorageProvider, assert_always, assert_reachable, buggify_knob, buggify_with_prob,
    sim::sim_random,
};
use paros::{Slot, StorageRecord};

use super::journal_files::{self, Damage, EntryLoc};
use super::{CorruptionInjection, CorruptionKind, StorageWorld};

/// Per-boot firing probabilities of the rot BUGGIFY sites — each fault
/// family its own independent location (per-seed activation × per-boot
/// firing).
const P_ENTRY_ROT: f64 = 0.06;
const P_LOST_WRITE: f64 = 0.04;
const P_MISDIRECT: f64 = 0.04;
const P_PROMISE_ROT: f64 = 0.04;
const P_META_FAULT: f64 = 0.03;
const P_READ_EIO: f64 = 0.05;

/// The epoch of an accepted record and of a checkpoint's faulty copy
/// (`paros::journal`'s frame kinds 1 and 2): the entries that carry a slot.
const SLOT_EPOCHS: [u64; 2] = [1, 2];

/// Per-boot rot firing rates, one **independent knob location per fault
/// family** (AGENTS.md prong 2). The defaults are this module's documented
/// `P_*` constants; an activated seed multiplies one family's rate toward its
/// extreme.
///
/// **The floor is the cap plus the budget.** Each rate is clamped to 0.5, so a
/// boot can never rot *every* candidate record, and every family still passes
/// through [`StorageWorld::may_corrupt_record`]'s per-record clean-quorum
/// budget (or [`StorageWorld::may_park`]'s dead-node budget for the families
/// that crash), which is what keeps a live quorum readable. Density buys a
/// denser fault *window*, never a longer one: the sites are rolled only while
/// [`StorageFaults::active`](super::faults::StorageFaults::active) holds.
#[derive(Clone, Copy)]
struct RotRates {
    entry: f64,
    lost_write: f64,
    misdirect: f64,
    promise: f64,
    meta: f64,
    read_eio: f64,
}

impl RotRates {
    fn for_boot() -> Self {
        #[allow(clippy::cast_precision_loss)]
        let dense = |base: f64, multiplier: u64| (base * multiplier as f64).min(0.5);
        Self {
            entry: dense(P_ENTRY_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
            lost_write: dense(P_LOST_WRITE, buggify_knob!(1_u64, 2_u64..6_u64)),
            misdirect: dense(P_MISDIRECT, buggify_knob!(1_u64, 2_u64..6_u64)),
            promise: dense(P_PROMISE_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
            meta: dense(P_META_FAULT, buggify_knob!(1_u64, 2_u64..6_u64)),
            read_eio: dense(P_READ_EIO, buggify_knob!(1_u64, 2_u64..6_u64)),
        }
    }

    /// Whether any family drew above its default — the BUGGIFY pairing's
    /// condition.
    fn any_dense(self) -> bool {
        self.entry > P_ENTRY_ROT
            || self.lost_write > P_LOST_WRITE
            || self.misdirect > P_MISDIRECT
            || self.promise > P_PROMISE_ROT
            || self.meta > P_META_FAULT
            || self.read_eio > P_READ_EIO
    }
}

/// Where one node's journal lives, for the sites that aim at it.
pub(crate) struct Target<'a> {
    /// The node's disk.
    pub(crate) provider: &'a SimStorageProvider,
    /// The journal's directory.
    pub(crate) dir: &'a str,
    /// The geometry's slot count per segment.
    pub(crate) slot_count: u64,
    /// The node's key in the world (its IP).
    pub(crate) key: &'a str,
    /// The node's id.
    pub(crate) node: u64,
}

/// One planned write of damage, carried out after the world lock is gone.
enum Planned {
    /// Damage the entries of `slot` (every live copy, the newest one with
    /// `newest`'s damage instead); `parks` once landed when the damage is a
    /// crash verdict.
    Slot {
        slot: Slot,
        kind: CorruptionKind,
        damage: Damage,
        newest: Damage,
        parks: bool,
    },
    /// Garble metadata copy `copy`; `parks` once landed (both copies).
    Meta { copy: u64, parks: bool },
    /// Grow the newest segment, parking the node once landed.
    Resize,
}

/// The live copies of every slot's records, oldest first: the entries whose
/// epoch carries a slot, grouped by the slot their tag names.
fn copies_by_slot(entries: &[EntryLoc]) -> BTreeMap<u64, Vec<EntryLoc>> {
    let mut by_slot: BTreeMap<u64, Vec<EntryLoc>> = BTreeMap::new();
    for entry in entries {
        if SLOT_EPOCHS.contains(&entry.epoch) {
            by_slot.entry(entry.tag[0]).or_default().push(entry.clone());
        }
    }
    by_slot
}

/// Pick one rot target from `slots`: half the time the *last* one, so the
/// newest records — the last append batch's included — are visited, not
/// just the interior.
fn pick(slots: &[Slot]) -> Slot {
    if sim_random::<f64>() < 0.5 {
        slots[slots.len() - 1]
    } else {
        slots[usize::try_from(sim_random::<u64>()).unwrap_or(0) % slots.len()]
    }
}

/// Roll the latent-fault sites for one booting node and land what fired
/// on its journal's files (see the module doc). Call it before the store's
/// boot scan, only inside the chaos window.
#[allow(clippy::too_many_lines)] // one flat block per independent BUGGIFY location
#[tracing::instrument(level = "debug", skip_all, fields(key = %target.key, node = target.node))]
pub(crate) async fn roll(world: &Mutex<StorageWorld>, target: &Target<'_>) {
    // Rot density is workload-buggified config (prong 2), one knob per
    // family: a seed whose boots lose writes hard but flip no bits is a
    // different disk from one that does the reverse.
    let rates = RotRates::for_boot();
    if rates.any_dense() {
        // BUGGIFY pairing: a boot genuinely rolled at the dense extreme.
        assert_reachable!("storage: a boot rolls rot at buggified density");
    }
    let entry = buggify_with_prob!(rates.entry);
    let lost_write = buggify_with_prob!(rates.lost_write);
    let misdirect = buggify_with_prob!(rates.misdirect);
    let promise = buggify_with_prob!(rates.promise);
    let meta = buggify_with_prob!(rates.meta);
    let read_eio = buggify_with_prob!(rates.read_eio);
    if !(entry || lost_write || misdirect || promise || meta || read_eio) {
        return;
    }
    // Where the slot records live: only scanned when a family aims at one.
    let copies = if entry || lost_write || misdirect {
        match journal_files::scan(target.provider, target.dir, target.slot_count).await {
            Ok(entries) => copies_by_slot(&entries),
            Err(_) => BTreeMap::new(),
        }
    } else {
        BTreeMap::new()
    };
    let key = target.key;
    let node = target.node;
    let mut planned: Vec<Planned> = Vec::new();
    {
        let mut world = world.lock().unwrap_or_else(PoisonError::into_inner);
        let world = &mut *world;
        // A slot a family may aim at: a clean copy the shadow says this node
        // holds, whose records the scan found, inside the per-record budget.
        let candidates = |world: &StorageWorld| -> Vec<Slot> {
            world
                .clean_slots(key)
                .into_iter()
                .filter(|slot| {
                    copies.get(&slot.0).is_some_and(|c| {
                        c.last()
                            .is_some_and(|newest| SLOT_EPOCHS[0] == newest.epoch)
                    })
                })
                .filter(|slot| world.may_corrupt_record(key, slot.0))
                .collect()
        };
        let mark = |world: &mut StorageWorld, slot: Slot, kind, block| {
            world
                .marks
                .entry(key.to_string())
                .or_default()
                .insert(slot.0);
            world.note_corruption(CorruptionInjection {
                block,
                ..CorruptionInjection::dormant(node, StorageRecord::Accepted(slot), kind)
            });
            world.note_if_unrecoverable(slot.0);
        };
        // Bit flip / latent sector error, with sub-rolls for a multi-record
        // *block* fault (CTRL injects per FS block: a contiguous run
        // mismatches at once) and for the identifier rotting with its entry.
        // A record whose identity survives is recoverable — reported, the
        // node keeps running — so the gate is the per-record budget; only
        // the identifier-lost sub-case (a double fault: the store refuses to
        // start) parks, so it also needs the dead-node budget.
        if entry {
            let permitted = candidates(world);
            if !permitted.is_empty() {
                let primary = pick(&permitted);
                // Generous coin: the identifier-lost row has its own gate,
                // and entry-rot events are budget-capped per run.
                let id_faulty = sim_random::<f64>() < 0.5 && world.may_park(key);
                // The block sub-roll needs a contiguous clean run at the
                // primary; its width is a knob (floor: every member still
                // passed the per-record budget, `permitted` was filtered).
                let block = sim_random::<f64>() < 0.4;
                let width = buggify_knob!(3_u64, 2_u64..9_u64);
                let members: Vec<Slot> = if block {
                    permitted
                        .iter()
                        .copied()
                        .filter(|s| s.0 >= primary.0.saturating_sub(width - 1) && *s <= primary)
                        .collect()
                } else {
                    vec![primary]
                };
                let is_block = members.len() > 1;
                for slot in members {
                    mark(world, slot, CorruptionKind::BitFlip, is_block);
                    planned.push(Planned::Slot {
                        slot,
                        kind: CorruptionKind::BitFlip,
                        damage: Damage::RotEntry,
                        newest: if slot == primary && id_faulty {
                            Damage::RotSlot
                        } else {
                            Damage::RotEntry
                        },
                        parks: slot == primary && id_faulty,
                    });
                }
                if id_faulty {
                    // Unidentifiable record: the scan can only crash, for
                    // good. The park is reserved now, under the budget, and
                    // made once the damage lands.
                    world.reserve_park(key);
                }
            }
        }
        // A lost write: the bytes never reached the medium while the
        // identifier did. Identity known ⇒ recoverable ⇒ per-record budget.
        if lost_write {
            let permitted = candidates(world);
            if !permitted.is_empty() {
                let slot = pick(&permitted);
                mark(world, slot, CorruptionKind::LostWrite, false);
                planned.push(Planned::Slot {
                    slot,
                    kind: CorruptionKind::LostWrite,
                    damage: Damage::LoseEntry,
                    newest: Damage::LoseEntry,
                    parks: false,
                });
            }
        }
        // A misdirected write: a checksummed record of another index where
        // this one should be — the identity check catches it. Recoverable.
        // The misdirected bytes come from another slot's entry: a log
        // holding only one slot's records has none to misdirect.
        if misdirect && copies.len() > 1 {
            let permitted = candidates(world);
            if !permitted.is_empty() {
                let slot = pick(&permitted);
                mark(world, slot, CorruptionKind::Misdirected, false);
                planned.push(Planned::Slot {
                    slot,
                    kind: CorruptionKind::Misdirected,
                    damage: Damage::Misdirect,
                    newest: Damage::Misdirect,
                    parks: false,
                });
            }
        }
        // Promise-copy rot (CTRL metainfo doctrine): usually one copy —
        // repaired from its twin, no availability cost — and rarely both,
        // the one unrecoverable scalar loss (the node cannot know what it
        // promised, and no peer can tell it).
        if promise && world.has_disk(key) {
            let both = sim_random::<f64>() < 0.25;
            if both {
                if world.may_park(key) {
                    world.reserve_park(key);
                    for copy in 0..2 {
                        world.note_corruption(CorruptionInjection::dormant(
                            node,
                            StorageRecord::Promise,
                            CorruptionKind::PromiseCopy,
                        ));
                        world.meta_rotted.insert((key.to_string(), copy));
                        planned.push(Planned::Meta { copy, parks: true });
                    }
                }
            } else {
                let copy = u64::from(sim_random::<f64>() < 0.5);
                // The single-copy leg stays recoverable: never rot a copy
                // whose twin an earlier, unrepaired rot already took (that
                // shape belongs to the park-guarded branch above).
                if !world.meta_rotted.contains(&(key.to_string(), 1 - copy)) {
                    world.note_corruption(CorruptionInjection::dormant(
                        node,
                        StorageRecord::Promise,
                        CorruptionKind::PromiseCopy,
                    ));
                    world.meta_rotted.insert((key.to_string(), copy));
                    planned.push(Planned::Meta { copy, parks: false });
                }
            }
        }
        // A file-granularity metadata fault: reliably crash, never recover —
        // the whole store is the record.
        if meta && world.has_disk(key) && world.may_park(key) {
            world.reserve_park(key);
            world.note_corruption(CorruptionInjection::dormant(
                node,
                StorageRecord::Store,
                CorruptionKind::Metadata,
            ));
            planned.push(Planned::Resize);
        }
        // A transient read EIO: collapses into the corruption channel, crashes
        // the node once, and the retry — the next boot — reads clean. The
        // only family with no availability cost and nothing on the disk.
        // One armed at a time: a boot a kill cut before its scan still
        // carries the earlier one.
        if read_eio && world.has_disk(key) && !world.read_eio.contains_key(key) {
            let slots = world.clean_slots(key);
            let record = match sim_random::<u64>() % 4 {
                0 => StorageRecord::ChosenIndex,
                1 => StorageRecord::Truncation,
                _ if !slots.is_empty() => StorageRecord::Accepted(
                    slots[usize::try_from(sim_random::<u64>()).unwrap_or(0) % slots.len()],
                ),
                _ => StorageRecord::Promise,
            };
            world.read_eio.insert(key.to_string(), record);
            world.note_corruption(CorruptionInjection {
                landed: true,
                ..CorruptionInjection::dormant(node, record, CorruptionKind::ReadEio)
            });
        }
    }
    land(world, target, &copies, planned).await;
}

/// Carry out `planned` on the node's files, confirming each injection once
/// its damage is synced.
async fn land(
    world: &Mutex<StorageWorld>,
    target: &Target<'_>,
    copies: &BTreeMap<u64, Vec<EntryLoc>>,
    planned: Vec<Planned>,
) {
    for plan in planned {
        let (record, kind, landed, parks) = match plan {
            Planned::Slot {
                slot,
                kind,
                damage,
                newest,
                parks,
            } => {
                let live = copies.get(&slot.0).map_or(&[][..], Vec::as_slice);
                let mut landed = !live.is_empty();
                for (at, entry) in live.iter().enumerate() {
                    let is_newest = at + 1 == live.len();
                    // A misdirected write's source: an entry of another
                    // slot (its header names another index).
                    let from = copies
                        .iter()
                        .filter(|(other, _)| **other != slot.0)
                        .flat_map(|(_, entries)| entries)
                        .next();
                    let applied = journal_files::damage(
                        target.provider,
                        entry,
                        if is_newest { newest } else { damage },
                        from,
                    )
                    .await;
                    landed &= applied.is_ok();
                }
                (StorageRecord::Accepted(slot), kind, landed, parks)
            }
            Planned::Meta { copy, parks } => {
                let landed = journal_files::rot_meta(target.provider, target.dir, copy)
                    .await
                    .unwrap_or(false);
                (
                    StorageRecord::Promise,
                    CorruptionKind::PromiseCopy,
                    landed,
                    parks,
                )
            }
            Planned::Resize => {
                let landed = journal_files::resize_segment(target.provider, target.dir)
                    .await
                    .unwrap_or(false);
                (StorageRecord::Store, CorruptionKind::Metadata, landed, true)
            }
        };
        if landed {
            let mut world = world.lock().unwrap_or_else(PoisonError::into_inner);
            world.note_landed(target.node, record, kind);
            if parks {
                world.park_landed(target.key, target.node);
            }
        }
    }
}

/// Land the targeted latent faults a corpus mask placed on this node
/// (`super::corpus_corrupt_entry`): every live copy of each slot rots, its
/// identifier intact. Not gated on the chaos window: the corpus injects
/// with every swarm fault off.
#[tracing::instrument(level = "debug", skip_all, fields(key = %target.key, node = target.node))]
pub(crate) async fn apply_pending(world: &Mutex<StorageWorld>, target: &Target<'_>) {
    let pending: Vec<u64> = world
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .pending_rot
        .remove(target.key)
        .map(|slots| slots.into_iter().collect())
        .unwrap_or_default();
    if pending.is_empty() {
        return;
    }
    let copies = match journal_files::scan(target.provider, target.dir, target.slot_count).await {
        Ok(entries) => copies_by_slot(&entries),
        Err(_) => BTreeMap::new(),
    };
    let mut planned = Vec::new();
    for slot in pending {
        assert_always!(
            copies.contains_key(&slot),
            "storage: a targeted latent fault finds its journal entry",
            { "node" => target.node, "slot" => slot }
        );
        planned.push(Planned::Slot {
            slot: Slot(slot),
            kind: CorruptionKind::BitFlip,
            damage: Damage::RotEntry,
            newest: Damage::RotEntry,
            parks: false,
        });
    }
    land(world, target, &copies, planned).await;
}
