//! The cell's layout a seed draws: the identifiers, the journals, the founders and the machines.

use std::sync::PoisonError;

use moonpool_sim::{StateHandle, assert_always, assert_reachable, buggify_knob};
use paros::core::{JournalId, JournalIdentifier, TenantId, WriterMode};
use paros::storage::journal::JournalStoreConfig;

use crate::shape::registry;
use crate::shape::scenarios::{
    batched_journals, batched_journals_among, batchers, changed_mode, cold_rot, downed_founder,
    drained_machine, lagging_fold, moved_founder, paged_handover, replaced_founder, second_cell,
    slow_link, zone_outage,
};

/// What a joiner registers as (#211): its class and its capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JoinerMachine {
    /// Its class.
    pub(crate) class: paros::model::registry::Class,
    /// The role slots of its class it advertises.
    pub(crate) capacity: u64,
}

/// The run's journals (#188): the static list every node serves, in id
/// order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JournalPlan {
    pub(crate) ids: Vec<JournalIdentifier>,
    /// The journals that run the multi-writer mode (#241): never the main
    /// one, whose owners claim, reconfigure and drive the scenarios; each
    /// other journal on a coin of its own.
    pub(crate) multi: Vec<JournalIdentifier>,
    /// The deployment's journal ([`Identifiers::main`]): the one with the seed's
    /// matchmakers, proxies, replicas and bootstrap.
    pub(crate) main: JournalIdentifier,
}

/// Every identifier the run names (`docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8: no identifier is
/// fixed), drawn once per seed: the deployment's journal and the joiners'
/// registry. Each a random tenant and a random journal, both set; the two
/// never share a tenant. The cell's id, its control journal, the fleet
/// tenant's and every tenant's control journal are not the harness's:
/// `init` and tenant creation draw them on a machine (#246, #210), and every
/// process learns them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Identifiers {
    /// The deployment's journal.
    pub(crate) main: JournalIdentifier,
    /// The node registry the joiners register in (#189): a harness journal
    /// on the acceptors until #211 makes it the cell control journal the
    /// machines serve.
    pub(crate) registry: JournalIdentifier,
}

/// A random set id.
pub(crate) fn draw_id() -> u64 {
    moonpool_sim::sim_random_range(1..u64::MAX)
}

/// The run's identifiers (see [`Identifiers`]), drawn once per seed by whoever asks
/// first.
pub(crate) fn identifiers(state: &StateHandle) -> Identifiers {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.identifiers.get_or_insert_with(|| {
        let users = TenantId(draw_id());
        let main = JournalIdentifier::new(users, JournalId(draw_id()));
        let mut tenant = TenantId(draw_id());
        while tenant == users {
            tenant = TenantId(draw_id());
        }
        let registry = JournalIdentifier::new(tenant, JournalId(draw_id()));
        Identifiers { main, registry }
    })
}

impl JournalPlan {
    /// The journal client `client` appends to: clients are spread over the
    /// journals round-robin.
    pub(crate) fn for_client(&self, client: usize) -> JournalIdentifier {
        self.ids
            .get(client % self.ids.len().max(1))
            .copied()
            .unwrap_or(self.main)
    }

    /// Whether the run serves more than one journal.
    pub(crate) fn is_multi(&self) -> bool {
        self.ids.len() > 1
    }

    /// The writer mode `journal` was created with (#241).
    pub(crate) fn mode(&self, journal: JournalIdentifier) -> WriterMode {
        if self.multi.contains(&journal) {
            WriterMode::Multi
        } else {
            WriterMode::Single
        }
    }
}

/// The layout of the run's journal stores (`JournalStorage`,
/// `JournalMatchmakerStorage`, over the simulated disk; every role stores
/// on them since the world stores went, #261): drawn once per seed by
/// whoever asks first. The commit protocol is a knob whose default is
/// production's, two syncs (always decided); the extreme is CLSTORE's one
/// sync, whose last batch a crash can leave ambiguous. Both are valid
/// stores. The segment geometry is [`journal_geometry`].
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn journal_layout(state: &StateHandle) -> JournalStoreConfig {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.journal_store.get_or_insert_with(|| {
        let batched = buggify_knob!(0_u64, 1_u64..2_u64) == 1;
        if batched {
            assert_reachable!("journal store: a seed commits with one sync per batch");
        }
        // A matchmaker store's scalar chunk (#439): an extreme of 32 to
        // 256 bytes cuts the scalars of a set with a pending bootstrap into
        // several chunks. Floor 1.
        let meta_chunk = buggify_knob!(paros::storage::journal::META_CHUNK, 32_usize..257_usize);
        JournalStoreConfig {
            durability: if batched {
                paros::storage::journal::Durability::Batched
            } else {
                paros::storage::journal::Durability::Ordered
            },
            geometry: journal_geometry(),
            meta_chunk,
            ..JournalStoreConfig::default()
        }
    })
}

/// The fewest blocks a segment's entry log may have (floor of
/// [`journal_geometry`]): it must hold the largest single entry a node
/// stores, the workload's largest write (`ChainConfig`'s
/// `MAX_BATCH_RECORDS` records of `MAX_LARGE_COMMAND_BYTES` each) plus its
/// encoding. A smaller one refuses that entry (`BatchTooLarge`) at every
/// sync, forever: a stalled node, not a knob.
pub(crate) const ENTRY_BLOCKS_FLOOR: u32 = 17;

/// The fewest blocks a segment's persist log may have (floor of
/// [`journal_geometry`]): 128 records, so a segment outlives a few commits
/// and rollover never dominates every one. Below it, with moonpool's slow
/// disk knobs on top, the old store held every node's sync past the end of
/// the run (witness 2281271371631374953, on the checkpointing store #256
/// replaced).
pub(crate) const PERSIST_BLOCKS_FLOOR: u32 = 2;

// The entry log holds the largest write the workload can make, with a
// block of slack for the encoding and the entry header.
const _: () = assert!(
    (ENTRY_BLOCKS_FLOOR as u64 - 1) * paros::storage::journal::BLOCK as u64
        >= crate::workload::MAX_BATCH_RECORDS * crate::workload::MAX_LARGE_COMMAND_BYTES as u64
);
const _: () = assert!(PERSIST_BLOCKS_FLOOR >= 1);

/// A segment of a few blocks: 512 records and 64 KiB of entries, the base
/// of the simulation's geometry draws and the matchmakers' fixed shape.
pub(crate) const COMPACT_GEOMETRY: paros::storage::journal::Geometry =
    paros::storage::journal::Geometry {
        persist_blocks: 8,
        gap_blocks: 0,
        entry_blocks: 16,
    };

/// The journal stores' segment geometry (#176, folded from #202): a
/// `buggify_knob!` per region, so segment rollover, prefix deletion and the
/// batch-split path run under varied shapes. The default is
/// [`COMPACT_GEOMETRY`]'s persist log (512 records) with the entry log at
/// its floor; the extremes shrink the persist log to [`PERSIST_BLOCKS_FLOOR`]
/// or grow either region. The gap stays empty. A deployment always runs
/// the library's default geometry (#600 (drop the small layout)); only the
/// simulation shrinks it, so a run crosses rollover often.
pub(crate) fn journal_geometry() -> paros::storage::journal::Geometry {
    let geometry = paros::storage::journal::Geometry {
        persist_blocks: buggify_knob!(
            COMPACT_GEOMETRY.persist_blocks,
            PERSIST_BLOCKS_FLOOR..33_u32
        ),
        gap_blocks: 0,
        entry_blocks: buggify_knob!(ENTRY_BLOCKS_FLOOR, ENTRY_BLOCKS_FLOOR..65_u32),
    };
    // The floors, checked where the draw lands.
    assert_always!(
        geometry.persist_blocks >= PERSIST_BLOCKS_FLOOR,
        "journal store: a drawn persist log is at least its floor"
    );
    assert_always!(
        geometry.entry_blocks >= ENTRY_BLOCKS_FLOOR,
        "journal store: a drawn entry log is at least its floor"
    );
    geometry
}

/// How many genesis nodes host the system journals (#189, #213 (three
/// founding members)) — the registry's founding members, the lowest ranks
/// of the pool: one or three, drawn once per run (decided on 2026-10-09:
/// the founding member count is a draw, 1 or 3). One keeps the one-member
/// plain journal, which sends nothing to anyone and whose only liveness
/// cost is its one member's downtime; three is the production shape, a
/// majority journal that keeps choosing through one member's loss. Three
/// were once forced to one: two more journals' beats on every link
/// livelocked a matchmaker deployment's two-round-trip campaigns, and the
/// two survivors of a plain 3-node pool duelled without choosing. The
/// driver's election backoff (`DriverTunables::election_backoff_doublings`,
/// #201) ended both duels: a round that loses doubles its timeout base, so
/// one candidate outlasts the other. Not a knob: the draw is a deployment
/// shape, never an extreme.
pub(crate) const FOUNDER_COUNTS: [usize; 2] = [1, 3];

/// The run's registry founding member count (#213 (three founding
/// members)): one of [`FOUNDER_COUNTS`], drawn once per run by whoever asks
/// first.
#[tracing::instrument(level = "debug", skip(state))]
pub(crate) fn founder_count(state: &StateHandle) -> usize {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.founder_count.get_or_insert_with(|| {
        let count = if moonpool_sim::sim_random_bool(0.5) {
            FOUNDER_COUNTS[1]
        } else {
            FOUNDER_COUNTS[0]
        };
        assert_always!(
            FOUNDER_COUNTS.contains(&count),
            "system: a run's founding member count is one of the drawn counts",
            { "count" => count }
        );
        count
    })
}

/// The registry's founding members in a pool of `pool` nodes (#189): its
/// [`founder_count`] lowest ranks.
pub(crate) fn founder_ranks(state: &StateHandle, pool: usize) -> Vec<u64> {
    (0..pool.min(founder_count(state)) as u64).collect()
}

/// Whether the run runs the **system journals** (#189) — the node registry
/// on the seeds, every node following them, and the
/// joiners joining the pool through the registry — drawn once per seed: a
/// seeded coin. Deployment shape: half the seeds keep #188's static
/// deployment. On a
/// seed with matchmakers a joiner the registry admits joins the default
/// journal as a spare a reconfiguration may pull in.
///
/// The fleet operations (#229) ride this draw — the fleet tenant's and the
/// cell's control journals are system journals — and stay on it (#247 item 9, decided from
/// the sweep's coverage on 2026-10-05): with every control-plane oracle of
/// #247 in place, `cargo xtask sim run paros-chain` saturated in 624 seeds,
/// every fleet gate fired, so the 50% draw costs the control plane no reach,
/// while the other half keeps the static deployment and the refusal parity
/// of a system append on a seed without system journals. The rarest fleet
/// gate ("a tenant removal resumed after a crash") is bounded by the
/// `TENANT` weight, the name alphabet and the removal's own crash location
/// in `workload/fleet.rs`, not by this draw.
#[tracing::instrument(level = "debug", skip(state))]
pub(crate) fn system_journals(state: &StateHandle) -> bool {
    let lagging = lagging_fold(state);
    let slow = slow_link(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.system.get_or_insert_with(|| {
        if !lagging && !slow && !moonpool_sim::sim_random_bool(0.5) {
            return false;
        }
        // BUGGIFY pairing: a seed genuinely runs the system journals (a
        // cause; the outcomes are the system board's gates).
        assert_reachable!("system: a seed runs the system journals");
        true
    })
}

/// The run's joiners' machines (#211), drawn once per seed by whoever asks
/// first, for `count` joiners in rank order. The role map's class draw: the
/// first joiner is a `storage` machine — a spare a reconfiguration may name
/// (the system board's joiner gates need one) — and every other one is
/// `storage` or `stateless` on a coin. The capacity is one `buggify_knob!`
/// for the run (default 2, extreme 1..=4; floor 1: a machine with no slot is
/// one the cell coordinator can never book, a registration that means
/// nothing).
#[tracing::instrument(level = "debug", skip(state))]
pub(crate) fn joiner_machines(state: &StateHandle, count: usize) -> Vec<JoinerMachine> {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .machines
        .get_or_insert_with(|| {
            let capacity = buggify_knob!(2_u64, 1_u64..5_u64);
            (0..count)
                .map(|rank| {
                    let class = if rank > 0 && moonpool_sim::sim_random_bool(0.5) {
                        assert_reachable!("registry: a joiner is a stateless machine");
                        paros::model::registry::Class::Stateless
                    } else {
                        paros::model::registry::Class::Storage
                    };
                    JoinerMachine { class, capacity }
                })
                .collect()
        })
        .clone()
}

/// One machine's half of its record (#246): what its operator configures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MachineDraw {
    /// Its class, fixed at format.
    pub(crate) class: paros::machine::Class,
    /// Its capacity.
    pub(crate) capacity: u64,
    /// Its zone and rack (`paros/zone`, `paros/rack`, #575 (key:value
    /// tags)): a zone always, a rack on half the machines.
    pub(crate) locality: paros::machine::Locality,
}

/// The run's machines (#246): the layout an operator gives a fleet before
/// `init`, drawn per seed so every run forms another cell. Forming it is the
/// run's own business (decided on 2026-10-09): nothing here is a cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MachineLayout {
    /// How many machines, the lowest ranks, are the founding members:
    /// `cell init` lists them, and forms the cell over them (#277).
    pub(crate) founders: usize,
    /// How many machines, the lowest ranks, `init` lists: the founders,
    /// and on a coin more when the founders are `2F+1` (one or three). The
    /// listed machines past the founders are admitted by `init` itself, so
    /// the control journals keep `2F+1` acceptors on a cell with more
    /// machines (decided on 2026-10-15).
    pub(crate) listed: usize,
    /// Each machine's settings, in rank order.
    pub(crate) machines: Vec<MachineDraw>,
    /// The machines advertise names, not literal addresses (#257): every
    /// dialer resolves them through the run's name table.
    pub(crate) named: bool,
    /// On a seed whose machines advertise names, the percent of reboots that
    /// land at a new address (#257).
    pub(crate) move_pct: u32,
    /// On a seed whose machines advertise names, the percent of a cell
    /// machine's reboots that come back under a new name at a new address
    /// (#349): the advertised string changes, and the cell's registry must
    /// learn it.
    pub(crate) rename_pct: u32,
    /// The founding members keep their names (#504 (red drain seeds)): on
    /// a drained-machine seed that renames, only admitted machines are
    /// renamed, so the lone founder never strands them (#505 (stranded
    /// admitted machine)).
    pub(crate) founders_keep_names: bool,
    /// On a second-cell seed (#232 (multiple cells)), the last machine is
    /// kept for the second cell: `init` never lists it and `ADMIT` never
    /// takes it.
    pub(crate) reserved: bool,
}

/// The capacity of one machine: the floor on the full founder, else the
/// drawn capacity.
pub(crate) fn founder_capacity(full: bool, floor: u64, capacity: u64) -> u64 {
    if full {
        assert_reachable!("machine: a founder holds only the floor");
        floor
    } else {
        capacity
    }
}

/// The run's machine capacity floor and capacity, a `buggify_knob!` over
/// the floor (#212 (placement)). Every live journal holds one slot on each
/// of its members, so the default fits the run's few tenants of three
/// journal names each, and the extreme fills the cell. On top, every
/// tenant's birth books its footprint (#476): the floor holds the cell
/// tenant's, the universe tenant's and one tenant's on any one founding
/// member, so `init` always fits and the extreme refuses a second tenant
/// on a cell of one. On a `batchers` seed (#145 (batchers)) the one
/// founder holds every tenant's footprint and journals, so it keeps room
/// for twice the births and the default room besides.
pub(crate) fn machine_capacity(batchers: bool) -> (u64, u64) {
    let floor = 3 * paros::model::footprint::Footprint::birth().storage();
    let capacity = floor + buggify_knob!(8_u64, 1_u64..4_u64);
    let capacity = if batchers {
        capacity.max(2 * floor + 8)
    } else {
        capacity
    };
    assert_always!(
        capacity > floor,
        "machine: a drawn capacity holds more than the births' floor",
        { "floor" => floor, "capacity" => capacity }
    );
    (floor, capacity)
}

/// Machine `rank`'s locality: a zone always, a rack on half the machines.
/// The zone-outage scenario (`zoned`) stands machine `r` in `zone-<r % 3>`
/// (#215 (zones)).
pub(crate) fn machine_locality(zoned: bool, rank: usize) -> paros::machine::Locality {
    paros::machine::Locality {
        zone: if zoned {
            format!("zone-{}", rank % 3)
        } else {
            format!("zone-{}", moonpool_sim::sim_random_range(0_u32..3_u32))
        },
        rack: if moonpool_sim::sim_random_bool(0.5) {
            format!("rack-{}", moonpool_sim::sim_random_range(0_u32..4_u32))
        } else {
            String::new()
        },
    }
}

/// Whether the batched-journals scenario shapes a layout of `count`
/// machines (#599 (durability mode per journal)): the run's own draw when
/// the count is not known yet, else the draw among `count` machines.
pub(crate) fn batched_layout(state: &StateHandle, count: usize) -> bool {
    if count == 0 {
        batched_journals(state)
    } else {
        batched_journals_among(state, count)
    }
}

/// The run's machine layout (#246), drawn once per seed by whoever asks
/// first, for `count` machines in rank order. The founder count is uniform
/// over `1..=count` (`3..=count` on a replaced-founder, moved-founder or
/// downed-founder seed with three machines or more, [`replaced_founder`],
/// [`moved_founder`], [`downed_founder`]; one on a drained-machine or
/// batchers seed, [`drained_machine`], [`batchers`], where the next two
/// machines are `stateless`); every founder is a `storage` machine (`cell init`
/// refuses a `stateless` member) and every other machine is `storage` or
/// `stateless` on a coin, idle, waiting until journals move onto admitted
/// machines. The capacity is one `buggify_knob!` for the run (default 8,
/// extreme 1..=3, where creates are refused for want of a slot, #212
/// (placement); floor 1: one journal's acceptor). A formed cell serves its control journals and
/// whatever its tenants create (#210). The machines advertise names on a
/// coin (#257); on such a seed a reboot lands at a new address with the
/// `move_pct` knob (default 0; extreme 20..=80, floor 0: a machine that
/// never moves), and a cell machine's reboot comes back under a new name
/// with the `rename_pct` knob (#349, default 0; extreme 20..=60, floor 0: a
/// machine that keeps its name). On a BUGGIFY coin the first founder holds
/// only the floor, so the births fill it.
#[tracing::instrument(level = "debug", skip(state))]
pub(crate) fn machine_layout(state: &StateHandle, count: usize) -> MachineLayout {
    // Drawn before the lock: the scenario takes the registry's lock too.
    let moved = moved_founder(state);
    let replaced = replaced_founder(state);
    let downed = downed_founder(state);
    let drained = drained_machine(state);
    let zoned = zone_outage(state);
    let changed = changed_mode(state);
    let batched = batched_layout(state, count);
    let rotted = cold_rot(state);
    let second = second_cell(state);
    let batchers = batchers(state) && count >= 3;
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .machine_layout
        .get_or_insert_with(|| {
            // The replaced-founder scenario needs a cell that keeps a
            // majority through one wipe: three founders or more. So does
            // the moved-founder scenario: a rename never takes a cell of
            // two, and another founder must cache the move. One founder or
            // two moved a founder on 25 of 900 hunt seeds, three or more on
            // 58. The downed-founder scenario (#213 (three founding
            // members)) holds one founder down: the others keep a majority
            // only in a cell of three or more.
            // So does the changed-mode scenario (#214 (tenant modes)): on a
            // cell of one, every mode places the same one slot.
            // So do the cold-rot scenario (#362 (background scrubbing)), which
            // rots only a journal of three members or more, and the batched-
            // journals one (#599 (durability mode per journal)), which cuts
            // only a batched commit that may lose a copy.
            let three = (replaced || moved || downed || changed || rotted || batched) && count >= 3;
            let least = if three { 3 } else { 1 };
            // The zone-outage scenario (#215 (zones)): three founding
            // members, one per zone, so a zone's loss leaves a majority.
            let zoned = zoned && count >= 3;
            // The second-cell scenario keeps the last machine out of `init`,
            // for the second cell, when the founders it needs leave one.
            // A drain needs two admitted machines besides it, and so does
            // the batchers scenario (#145 (batchers)): two `stateless`
            // machines beside its founder.
            let reserve = second && count > if drained || batchers { 3 } else { least };
            let founders = if count == 0 {
                0
            } else if zoned {
                3
            } else if drained || batchers {
                // The drained-machine scenario: one founding member, and
                // every other machine admitted. The batchers scenario: one
                // founding member, and room for two `stateless` machines.
                1
            } else if reserve {
                moonpool_sim::sim_random_range(least..count)
            } else {
                moonpool_sim::sim_random_range(least..count + 1)
            };
            // The machines `init` may list: all but the second cell's.
            let room = if reserve { count - 1 } else { count };
            // `init` lists more machines than found the cell only when the
            // founders are `2F+1`: it forms the cell on the first `2F+1`.
            // On half the batchers seeds, `init` admits every machine it
            // may list, so the `stateless` ones serve before `ADMIT` runs.
            let listed = if batchers && moonpool_sim::sim_random_bool(0.5) {
                room
            } else if founders % 2 == 1 && founders < room && !drained {
                if moonpool_sim::sim_random_bool(0.5) {
                    moonpool_sim::sim_random_range(founders + 1..room + 1)
                } else {
                    founders
                }
            } else {
                founders
            };
            let (floor, capacity) = machine_capacity(batchers);
            // On half the seeds where BUGGIFY strikes, the first founder
            // holds only the floor, so the births fill it and a tenant
            // coordinator sizes a set past a full machine (recipe
            // 122746:10910618044440912506 on 8668601583785772229). Never on
            // a zone-outage seed: its tenants need every zone's room.
            // Nor on a batchers seed, whose one founder holds every tenant.
            let full_founder =
                founders > 0 && !zoned && !batchers && moonpool_sim::buggify_with_prob!(0.5);
            // On half the drained-machine seeds, the machines advertise
            // names and a reboot renames one at the knob's extreme, so a
            // drain meets a machine renamed since it took its slots (#504
            // (red drain seeds), witness 14650518500742172559 on e61b30e):
            // the default never renames.
            let renamed_drain = drained && moonpool_sim::sim_random_bool(0.5);
            let named = moonpool_sim::sim_random_bool(0.5) || moved || renamed_drain;
            let move_pct = buggify_knob!(0_u32, 20_u32..81_u32);
            let rename_pct_knob = buggify_knob!(0_u32, 20_u32..61_u32);
            let rename_pct = renamed_drain.then(|| moonpool_sim::sim_random_range(20_u32..61_u32));
            let rename_pct = rename_pct.map_or(rename_pct_knob, |drawn| rename_pct_knob.max(drawn));
            let machines = (0..count)
                .map(|rank| {
                    // The founders, and the second cell's machine, store.
                    let stores = rank < founders || rank >= room || drained;
                    // The batchers scenario: the two machines past the
                    // founder are `stateless`.
                    let stateless = batchers && rank < founders + 2;
                    let class = if !stores && (stateless || moonpool_sim::sim_random_bool(0.3)) {
                        assert_reachable!("machine: a machine outside the seeds is stateless");
                        paros::machine::Class::Stateless
                    } else {
                        paros::machine::Class::Storage
                    };
                    MachineDraw {
                        class,
                        capacity: founder_capacity(full_founder && rank == 0, floor, capacity),
                        locality: machine_locality(zoned, rank),
                    }
                })
                .collect();
            assert_always!(
                count == 0 || (founders >= 1 && founders <= count),
                "machine: a layout's founding members are machines of it",
                { "count" => count, "founders" => founders }
            );
            assert_always!(
                !batchers || founders + 2 <= room,
                "machine: a batchers layout keeps room for two stateless machines",
                { "count" => count, "founders" => founders, "room" => room }
            );
            assert_always!(
                founders <= listed && listed <= count,
                "machine: init lists the founding members and machines past them",
                { "count" => count, "founders" => founders, "listed" => listed }
            );
            MachineLayout {
                founders,
                listed,
                machines,
                named,
                move_pct,
                rename_pct,
                founders_keep_names: renamed_drain,
                reserved: reserve,
            }
        })
        .clone()
}

/// The run's journals (#188), drawn once per seed by whoever asks first — a
/// node or a client. The count is a `buggify_knob!` (default 1, extreme
/// 2..=3; floor 1, the one-journal campaign). A seed with matchmakers draws
/// the count too (#201): PR #199 restricted it to one journal after the
/// first multi-journal hunt livelocked a matchmaker campaign under the
/// tripled traffic (witness 7568743934611962292); the driver's election
/// backoff (`DriverTunables::election_backoff_doublings`) landed after it,
/// and the 2,000-seed hunt that lifted the restriction was clean. The
/// **default** journal is the seed's deployment — its matchmakers, proxies,
/// replicas and bootstrap; every other journal is a plain Multi-Paxos
/// journal over the whole pool, or names the matchmakers on a matchmaker
/// seed, and the proxy leaders and the replica tier serve every journal
/// (`crate::process::journal_config`, #193). On a
/// multi-journal seed a second location draws whether the driver's named
/// location holds one journal on every node for the chaos window
/// (`paros::observe::scenario::HOLD_JOURNAL`): its siblings must keep committing.
/// Each node holds the highest user journal it boots serving: the same one
/// on every acceptor, possibly a lower one on a joiner that serves fewer.
/// The driver reports each hold (`Audit::journal_held`), so the journal
/// board knows every held journal. A single-journal seed holds none.
#[tracing::instrument(level = "debug", skip(state))]
pub(crate) fn journals(state: &StateHandle) -> JournalPlan {
    let main = identifiers(state).main;
    let slow = slow_link(state);
    let paged = paged_handover(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .journals
        .get_or_insert_with(|| {
            let count = buggify_knob!(1_u64, 2_u64..4_u64);
            // A slow-link seed loads every link with the most journals, and
            // a paged-handover seed hands over a set that holds them all.
            let count = if slow || paged { 3 } else { count };
            // The first journal is the deployment's ([`Identifiers::main`]);
            // every other one's identifier is drawn (#235): a random journal id,
            // in the main journal's tenant or a random one — and, in another
            // tenant, sometimes the very journal id of the first, so the
            // demux is proven to key on both halves of the identifier. No identifier is
            // fixed (§3.8).
            let mut ids = vec![main];
            while ids.len() < usize::try_from(count).unwrap_or(1) {
                let tenant = if paged || moonpool_sim::sim_random_bool(0.5) {
                    main.tenant
                } else {
                    TenantId(draw_id())
                };
                let journal = if tenant != main.tenant && moonpool_sim::sim_random_bool(0.5) {
                    assert_reachable!("journal: two tenants serve the same journal id");
                    main.journal
                } else {
                    JournalId(draw_id())
                };
                let identifier = JournalIdentifier::new(tenant, journal);
                if !ids.contains(&identifier) {
                    ids.push(identifier);
                }
            }
            ids.sort_unstable();
            if ids.len() < 2 {
                moonpool_sim::set_activation(paros::observe::scenario::HOLD_JOURNAL, false);
                return JournalPlan {
                    ids,
                    multi: Vec::new(),
                    main,
                };
            }
            // BUGGIFY pairing: a seed genuinely runs several journals (a
            // cause; the outcomes are the non-interference gates).
            assert_reachable!("journal: a seed runs more than one journal");
            // Its fired gate sits where the hold has an effect (the driver's
            // named location), not at the draw.
            moonpool_sim::set_activation(
                paros::observe::scenario::HOLD_JOURNAL,
                moonpool_sim::buggify_with_prob!(0.5),
            );
            // The writer mode is fixed when a journal is created (#241): each
            // journal beside the main one draws it once per seed.
            let multi: Vec<JournalIdentifier> = ids
                .iter()
                .copied()
                .filter(|id| *id != main && moonpool_sim::buggify_with_prob!(0.5))
                .collect();
            if !multi.is_empty() {
                // BUGGIFY pairing: a multi-writer journal runs on some seed.
                assert_reachable!("journal: a seed runs a multi-writer journal");
            }
            JournalPlan { ids, multi, main }
        })
        .clone()
}
