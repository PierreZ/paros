//! The Stage-7 boot-rot BUGGIFY sites: latent faults that surfaced while a node
//! was down, injected at the boot that immediately reads them back. One
//! independent location per fault family, every one of them budgeted.

use moonpool_sim::{assert_reachable, buggify_knob, buggify_with_prob, sim::sim_random};

use super::{
    CorruptionInjection, CorruptionKind, CorruptionOutcome, RecordHealth, SlotHealth, StorageWorld,
};
use paros::{MetadataFault, Slot, StorageRecord, WitnessStatus, snap_chunk_count};

/// Per-boot firing probabilities of the Stage-7 rot BUGGIFY sites — each fault
/// family its own independent location (per-seed activation × per-boot
/// firing), modelling latent faults that surfaced while the node was down and
/// are read back by the boot scan that immediately follows.
const P_ENTRY_ROT: f64 = 0.06;
const P_LOST_WRITE: f64 = 0.04;
const P_MISDIRECT: f64 = 0.04;
const P_SNAPSHOT_ROT: f64 = 0.05;
const P_PROMISE_ROT: f64 = 0.04;
const P_META_FAULT: f64 = 0.03;
const P_READ_EIO: f64 = 0.05;
/// Per-boot chunk rot on the retained decided snapshot point (#101): one
/// chunk's bytes fail their checksum while the point's identity survives —
/// the recoverable class the driver's chunk-repair layer pulls from peers.
const P_SNAP_CHUNK_ROT: f64 = 0.05;
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
/// [`StorageFaults::active`] holds.
#[derive(Clone, Copy)]
struct RotRates {
    entry: f64,
    lost_write: f64,
    misdirect: f64,
    snapshot: f64,
    promise: f64,
    meta: f64,
    read_eio: f64,
    snap_chunk: f64,
}

impl RotRates {
    fn for_boot() -> Self {
        #[allow(clippy::cast_precision_loss)]
        let dense = |base: f64, multiplier: u64| (base * multiplier as f64).min(0.5);
        Self {
            entry: dense(P_ENTRY_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
            lost_write: dense(P_LOST_WRITE, buggify_knob!(1_u64, 2_u64..6_u64)),
            misdirect: dense(P_MISDIRECT, buggify_knob!(1_u64, 2_u64..6_u64)),
            snapshot: dense(P_SNAPSHOT_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
            promise: dense(P_PROMISE_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
            meta: dense(P_META_FAULT, buggify_knob!(1_u64, 2_u64..6_u64)),
            read_eio: dense(P_READ_EIO, buggify_knob!(1_u64, 2_u64..6_u64)),
            snap_chunk: dense(P_SNAP_CHUNK_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
        }
    }

    /// Whether any family drew above its default — the BUGGIFY pairing's
    /// condition.
    fn any_dense(self) -> bool {
        self.entry > P_ENTRY_ROT
            || self.lost_write > P_LOST_WRITE
            || self.misdirect > P_MISDIRECT
            || self.snapshot > P_SNAPSHOT_ROT
            || self.promise > P_PROMISE_ROT
            || self.meta > P_META_FAULT
            || self.read_eio > P_READ_EIO
            || self.snap_chunk > P_SNAP_CHUNK_ROT
    }
}

/// Roll the Stage-7 rot sites for one booting node: latent faults that
/// surfaced while it was down, injected at the boot that will immediately read
/// them back (the boot scan runs before anything else in `run_node`, with no
/// await in between, so injection → detection is atomic per boot). Each fault
/// family is its own independent BUGGIFY location; every *persistent* family
/// terminally parks the node (detect ⇒ crash, and restarting cannot help), so
/// each is gated on [`StorageWorld::may_park`]'s dead-node budget.
#[allow(clippy::too_many_lines)] // one flat block per independent BUGGIFY location
pub(super) fn roll_boot_rot(world: &mut StorageWorld, key: &str, node: u64) {
    // Rot density is workload-buggified config (prong 2), and **one knob per
    // family**: each multiplies its own family's *firing* probability toward
    // the extreme, capped so a probability stays a probability. Per family
    // rather than one shared multiplier because per-seed activation has to
    // compose — a seed whose boots rot lost writes hard but flip no bits is a
    // different disk from one that does the reverse, and a single location can
    // only ever select "all families at once". Only the firing rates scale:
    // the per-record clean-quorum budget and the budget-off axis semantics are
    // untouched.
    let rates = RotRates::for_boot();
    if rates.any_dense() {
        // BUGGIFY pairing: a boot genuinely rolled at the dense extreme.
        assert_reachable!("storage: a boot rolls rot at buggified density");
    }
    let clean_slots = |world: &StorageWorld| -> Vec<Slot> {
        world.disks.get(key).map_or_else(Vec::new, |disk| {
            disk.accepted
                .keys()
                .filter(|slot| disk.slot_health(**slot).clean())
                .copied()
                .collect()
        })
    };
    // Pick a rot target: half the time the *last* retained slot, so the
    // proven-undecidable last-entry row of the disentanglement table is
    // genuinely visited, not just the interior corruption row.
    let pick = |slots: &[Slot]| -> Slot {
        if sim_random::<f64>() < 0.5 {
            slots[slots.len() - 1]
        } else {
            slots[usize::try_from(sim_random::<u64>()).unwrap_or(0) % slots.len()]
        }
    };
    let mark_entry = |world: &mut StorageWorld, slot: Slot, health: SlotHealth, kind, block| {
        if let Some(disk) = world.disks.get_mut(key) {
            disk.entry_health.insert(slot, health);
        }
        world
            .marks
            .entry(key.to_string())
            .or_default()
            .insert(slot.0);
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Accepted(slot),
            kind,
            block,
            outcome: CorruptionOutcome::Dormant,
        });
    };

    // Bit flip / latent sector error on one persisted entry — with sub-rolls
    // for a multi-record *block* fault (CTRL injects per FS block: a
    // contiguous run mismatches at once, and recovery must not assume faults
    // are singletons) and for the identifier rotting with its entry. Stage 8:
    // a record whose identity survives is **recoverable** — the node reports
    // it faulty and keeps running — so the gate is the per-record budget, not
    // the dead-node budget. Only the identifier-lost sub-case (unidentifiable
    // ⇒ crash) still needs to park, so it also needs the dead-node budget.
    if buggify_with_prob!(rates.entry) {
        let slots = clean_slots(world);
        let permitted: Vec<Slot> = slots
            .iter()
            .copied()
            .filter(|slot| world.may_corrupt_record(key, slot.0))
            .collect();
        if !permitted.is_empty() {
            let primary = pick(&permitted);
            // Generous coin: the identifier-lost row has its own per-verdict
            // sometimes-gate, and the entry-rot events that draw this coin
            // are budget-capped per run, so the sweep needs a fat coin to be
            // certain of the composition within a bounded seed schedule.
            let id_faulty = sim_random::<f64>() < 0.5 && world.may_park(key);
            // The block sub-roll needs a contiguous clean run at the primary,
            // which short (frequently truncated) logs often lack, so it rolls
            // generously to stay reachable across a bounded sweep. The block's
            // width is a knob: CTRL injects per FS block, and a block spans
            // several records. Floor: every member still passes the
            // per-record budget (`permitted` was filtered first).
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
                let id = if slot == primary && id_faulty {
                    WitnessStatus::Faulty
                } else {
                    WitnessStatus::Present
                };
                mark_entry(
                    world,
                    slot,
                    SlotHealth {
                        entry: RecordHealth::Faulty,
                        id,
                    },
                    CorruptionKind::BitFlip,
                    is_block,
                );
                world.note_if_unrecoverable(slot.0);
            }
            if id_faulty {
                // Unidentifiable record: the scan can only crash, terminally.
                world.park(key, node);
            }
        }
    }
    // A lost write: the entry reads back as its reserved record where the
    // identifier exists (absence made detectable by the reserved-record
    // contract). Identity known ⇒ recoverable ⇒ per-record budget, no park.
    if buggify_with_prob!(rates.lost_write) {
        let slots = clean_slots(world);
        if let Some(slot) = pick_permitted(world, key, &slots) {
            mark_entry(
                world,
                slot,
                SlotHealth {
                    entry: RecordHealth::Lost,
                    id: WitnessStatus::Present,
                },
                CorruptionKind::LostWrite,
                false,
            );
            world.note_if_unrecoverable(slot.0);
        }
    }
    // A misdirected write: valid checksum, wrong identity — the identity
    // check inside the checksummed region catches it. Recoverable likewise.
    if buggify_with_prob!(rates.misdirect) {
        let slots = clean_slots(world);
        if let Some(slot) = pick_permitted(world, key, &slots) {
            mark_entry(
                world,
                slot,
                SlotHealth {
                    entry: RecordHealth::Misdirected,
                    id: WitnessStatus::Present,
                },
                CorruptionKind::Misdirected,
                false,
            );
            world.note_if_unrecoverable(slot.0);
        }
    }
    // Snapshot corruption is its own kind and its own gate (#71) — a
    // first-class target, not a byproduct of log-entry coverage. Stage 8
    // recovers it (local log replay at floor 0, a peer's InstallSnapshot
    // otherwise), so no park; a singleton under a truncated log has no peer
    // to recover from, so budget-on skips that one unrecoverable shape.
    if buggify_with_prob!(rates.snapshot)
        && world
            .disks
            .get(key)
            .is_some_and(|d| d.chain.applied_count > 0)
        && (world.unbudgeted
            || world.cluster_size > 1
            || world.disks.get(key).is_some_and(|d| d.first_slot.0 == 0))
    {
        if let Some(disk) = world.disks.get_mut(key) {
            disk.snapshot_health = RecordHealth::Faulty;
        }
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Snapshot,
            kind: CorruptionKind::BitFlip,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
        // Slots this node truncated past lose their local custody: re-derive
        // the unrecoverable ground truth over the folded prefix (mirrors
        // `corpus_corrupt_snapshot`; unbudgeted only — a budgeted run never
        // permits the shape).
        let floor = world.disks.get(key).map_or(0, |d| d.first_slot.0);
        for slot in 0..floor {
            world.note_if_unrecoverable(slot);
        }
    }
    // HardState copy rot (CTRL metainfo doctrine): usually one copy — used and
    // repaired from its twin, no availability cost — and rarely both, which is
    // the one unrecoverable scalar loss (the node cannot know what it
    // promised, and no peer can tell it).
    if buggify_with_prob!(rates.promise) && world.disks.contains_key(key) {
        let both = sim_random::<f64>() < 0.25;
        if both {
            if world.may_park(key) {
                if let Some(disk) = world.disks.get_mut(key) {
                    disk.promise_health = [RecordHealth::Faulty, RecordHealth::Faulty];
                }
                world.park(key, node);
                for _copy in 0..2 {
                    world.note_corruption(CorruptionInjection {
                        node,
                        record: StorageRecord::Promise,
                        kind: CorruptionKind::PromiseCopy,
                        block: false,
                        outcome: CorruptionOutcome::Dormant,
                    });
                }
            }
        } else {
            let copy = usize::from(sim_random::<f64>() < 0.5);
            // The single-copy leg must stay recoverable: if the twin is
            // already faulty (an earlier single-copy rot that no boot healed
            // yet), rotting this copy would assemble the terminal both-lost
            // shape *outside* the park-guarded branch above — the node would
            // then crash on every boot forever, never parking, inflating the
            // detection count past the ledger. That shape belongs solely to
            // the deliberate `both` branch.
            let twin_clean = world
                .disks
                .get(key)
                .is_some_and(|d| d.promise_health[1 - copy] == RecordHealth::Clean);
            if twin_clean {
                if let Some(disk) = world.disks.get_mut(key) {
                    disk.promise_health[copy] = RecordHealth::Faulty;
                }
                world.note_corruption(CorruptionInjection {
                    node,
                    record: StorageRecord::Promise,
                    kind: CorruptionKind::PromiseCopy,
                    block: false,
                    outcome: CorruptionOutcome::Dormant,
                });
            }
        }
    }
    // A file-granularity FS-metadata fault: reliably crash, never recover
    // (item E) — the whole store is the record.
    if buggify_with_prob!(rates.meta) && world.disks.contains_key(key) && world.may_park(key) {
        let fault = match sim_random::<u64>() % 3 {
            0 => MetadataFault::Missing,
            1 => MetadataFault::WrongSize,
            _ => MetadataFault::ReadOnly,
        };
        if let Some(disk) = world.disks.get_mut(key) {
            disk.meta_fault = Some(fault);
        }
        world.park(key, node);
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Store,
            kind: CorruptionKind::Metadata,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
    }
    // #101: chunk rot on the retained decided snapshot point — the value of
    // one fixed-size chunk is lost while the point's identity (and every
    // other chunk) survives. Recoverable by construction: the point is
    // byte-identical cluster-wide, so any peer can serve the chunk back. The
    // budget keeps a clean quorum of each chunk across the holders of the
    // same point (budget-off lifts it, like every other family).
    if buggify_with_prob!(rates.snap_chunk)
        && let Some((at, state)) = world.disks.get(key).and_then(|d| d.snap_point)
    {
        let chunks = snap_chunk_count(state.encode().len());
        // How many chunks of the point rot at once: one by default, a knob
        // toward all of them — each still passes the per-chunk clean-quorum
        // check below, which is the floor.
        let rotting = buggify_knob!(1_u32, 1_u32..17_u32).min(chunks);
        let first = u32::try_from(sim_random::<u64>()).unwrap_or(0) % chunks.max(1);
        for offset in 0..rotting {
            let chunk = (first + offset) % chunks.max(1);
            let clean_copies = world
                .disks
                .iter()
                .filter(|(peer, d)| {
                    !world.parked.contains(*peer)
                        && d.snap_point.is_some_and(|(peer_at, _)| peer_at == at)
                        && d.snap_chunk_health
                            .get(usize::try_from(chunk).unwrap_or(0))
                            .is_none_or(|h| *h == RecordHealth::Clean)
                })
                .count();
            let quorum = world.quorum();
            if (world.unbudgeted || clean_copies.saturating_sub(1) >= quorum)
                && let Some(disk) = world.disks.get_mut(key)
            {
                let index = usize::try_from(chunk).unwrap_or(0);
                if disk.snap_chunk_health.len() <= index {
                    disk.snap_chunk_health
                        .resize(usize::try_from(chunks).unwrap_or(0), RecordHealth::Clean);
                }
                if disk.snap_chunk_health[index] == RecordHealth::Clean {
                    disk.snap_chunk_health[index] = RecordHealth::Faulty;
                    world.note_corruption(CorruptionInjection {
                        node,
                        record: StorageRecord::SnapChunk(Slot(at), chunk),
                        kind: CorruptionKind::BitFlip,
                        block: false,
                        outcome: CorruptionOutcome::Dormant,
                    });
                    // The point is custody for the folded prefix: losing its
                    // last clean copy of a chunk can strand every slot below
                    // the floor. Re-derive the unrecoverable ground truth
                    // (mirrors `corpus_corrupt_snap_chunk`; unbudgeted only).
                    let floor = world.disks.get(key).map_or(0, |d| d.first_slot.0);
                    for slot in 0..floor {
                        world.note_if_unrecoverable(slot);
                    }
                }
            }
        }
    }
    // A transient EIO on the read path: collapses into the corruption channel
    // (one detection path), crashes the node once, and the retry — the next
    // boot — reads clean. The only Stage-7 family with no availability cost.
    // The target record kind is drawn: any retained accepted record, or one
    // of the scalars — every kind takes the same detection path.
    if buggify_with_prob!(rates.read_eio) && world.disks.contains_key(key) {
        let slots: Vec<Slot> = world
            .disks
            .get(key)
            .map(|d| d.accepted.keys().copied().collect())
            .unwrap_or_default();
        let record = match sim_random::<u64>() % 5 {
            0 => StorageRecord::ChosenIndex,
            1 => StorageRecord::Truncation,
            2 => StorageRecord::Snapshot,
            _ if !slots.is_empty() => StorageRecord::Accepted(
                slots[usize::try_from(sim_random::<u64>()).unwrap_or(0) % slots.len()],
            ),
            _ => StorageRecord::Promise,
        };
        if let Some(disk) = world.disks.get_mut(key) {
            disk.read_eio = Some(record);
        }
        world.note_corruption(CorruptionInjection {
            node,
            record,
            kind: CorruptionKind::ReadEio,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
    }
}

/// Pick one budget-permitted rot target from `slots` (see
/// [`roll_boot_rot`]'s `pick` bias: half the time the last retained slot).
fn pick_permitted(world: &StorageWorld, key: &str, slots: &[Slot]) -> Option<Slot> {
    let permitted: Vec<Slot> = slots
        .iter()
        .copied()
        .filter(|slot| world.may_corrupt_record(key, slot.0))
        .collect();
    if permitted.is_empty() {
        return None;
    }
    Some(if sim_random::<f64>() < 0.5 {
        permitted[permitted.len() - 1]
    } else {
        permitted[usize::try_from(sim_random::<u64>()).unwrap_or(0) % permitted.len()]
    })
}
