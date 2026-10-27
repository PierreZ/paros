//! The per-logical-node **shape**: every knob the swarm draws *for a node* —
//! the driver's transport tunables, the disk's write-path fault rates — fixed at that node's first boot of a seed and reused
//! by every later incarnation of the same node.
//!
//! Why this is its own registry and not a local in `NodeProcess::run`: a
//! moonpool attrition restart builds a **fresh** `NodeProcess` from the factory
//! and re-enters `run()`, so anything drawn there is drawn again, and a node that
//! booted with a 4-slot peer queue could come back with the production default
//! (or a different extreme). That silently breaks the FDB knob model — a knob is
//! a *configuration* of the process for the run, not a per-boot coin — and it
//! makes "this seed ran node 2 at the capacity extreme" false for half of node
//! 2's lifetime. A crash at one of the driver's `hint!`s is a restart through
//! the same factory, so the registry covers it too.
//!
//! What is deliberately **not** here: durable Paxos state (that is the
//! journal stores' concern — the shape says how a
//! node is perturbed, never what it has promised or accepted), and the
//! per-*event* draws that describe one crash rather than one node — a restart
//! delay is the attrition regime's, drawn at the crash it delays, because two
//! crashes of the same node should not be forced to look alike. Run-level shape (the application's
//! digest-lane count, fixed by whichever node boots first) also lives here so
//! that the draw happens exactly once instead of once per boot with the extra
//! draws discarded.
//!
//! The registry is published on the per-iteration `StateHandle`, like the
//! storage world and the audit: fresh per seed, shared by every node, and
//! surviving every restart.

mod layout;
mod node;
mod quorum;
mod scenarios;
mod timing;
pub(crate) use layout::{
    COMPACT_GEOMETRY, Identifiers, JoinerMachine, JournalPlan, MachineDraw, MachineLayout,
    founder_ranks, identifiers, joiner_machines, journal_layout, journals, machine_layout,
    system_journals,
};
pub(crate) use node::{Incarnation, NodeShape, REQUEST_FLOOR_MS};
pub(crate) use quorum::{
    MATCHMAKER_LOSS_FLOOR, QuorumPolicy, acceptor_config, acceptor_zones, bootstrap_ranks,
    config_floor, matchmaker_bootstrap_ranks, matchmaker_floor, peer_domains, quorum_policy,
    wide_grid,
};
pub(crate) use scenarios::{
    admitted_slot, bare_quorum, batched_journals, batched_journals_among, batched_journals_drawn,
    batchers, batchers_among, changed_mode, cold_rot, departed_straggler, disk_full,
    downed_founder, drained_machine, early_handover, entry_capacity, gone_machine, handoff_storm,
    lagging_acceptor, lagging_fold, lagging_replica, lost_verdict, matchmaker_registry_page,
    moved_founder, paged_handover, racing_floor, replaced_founder, replica_apply_page, reused_name,
    run_end, second_cell, silent_machine, slow_link, slow_machine, split_floor, stalled_proxy,
    unmetered, untagged, wiped_founder, withhold_gc, zone_outage,
};
pub(crate) use timing::{
    CONNECT_FLOOR_MS, ELECTION_RENEW_FLOOR, RESOLVE_ROUND_TRIP, longest_down_after,
    longest_election_lease, longest_internal_timeout, longest_replace_after, lowest_slow_busy,
    renew_at_floor, resolve_answer_ceiling,
};

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{StateHandle, assert_always, assert_reachable};

use paros::storage::journal::JournalStoreConfig;

/// Well-known [`StateHandle`] key of the per-iteration registry.
const SHAPE_KEY: &str = "paros-node-shapes";

#[derive(Default)]
struct Registry {
    /// Run-level: the quorum-system policy (see [`quorum_policy`]), fixed by
    /// the first caller — a node or a client.
    quorum: Option<QuorumPolicy>,
    /// Run-level: the bootstrap acceptor ranks (see [`bootstrap_ranks`]),
    /// fixed by the first caller — a node or a client.
    bootstrap: Option<Vec<u64>>,
    /// Run-level: the bootstrap matchmaker ranks (see
    /// [`matchmaker_bootstrap_ranks`]), fixed by the first caller.
    matchmaker_bootstrap: Option<Vec<u64>>,
    /// Run-level: the journals every node serves (see [`journals`]), fixed
    /// by the first caller.
    journals: Option<JournalPlan>,
    /// Run-level: the journal stores' layout (see [`journal_layout`]),
    /// fixed by the first caller.
    journal_store: Option<JournalStoreConfig>,
    /// Run-level: whether the nodes withhold their GC requests for the
    /// chaos window (see [`withhold_gc`]), fixed by the first caller.
    withhold_gc: Option<bool>,
    /// Run-level: whether the run draws the departed-straggler scenario
    /// (see [`departed_straggler`]), fixed by the first caller.
    departed_straggler: Option<bool>,
    /// Run-level: whether the run draws the bare-quorum scenario (see
    /// [`bare_quorum`]), fixed by the first caller.
    bare_quorum: Option<bool>,
    /// Run-level: whether the run draws the reused-name scenario (see
    /// [`reused_name`]), fixed by the first caller.
    reused_name: Option<bool>,
    /// Run-level: whether the run draws the lost-verdict scenario (see
    /// [`lost_verdict`]), fixed by the first caller.
    lost_verdict: Option<bool>,
    /// Run-level: whether the run draws the lagging-fold scenario (see
    /// [`lagging_fold`]), fixed by the first caller.
    lagging_fold: Option<bool>,
    /// Run-level: whether the run draws the wiped-founder scenario (see
    /// [`wiped_founder`]), fixed by the first caller.
    wiped_founder: Option<bool>,
    /// Run-level: whether the run draws the silent-machine scenario (see
    /// [`silent_machine`]), fixed by the first caller.
    silent_machine: Option<bool>,
    /// Run-level: whether the run draws the slow-machine scenario (see
    /// [`slow_machine`]), fixed by the first caller.
    slow_machine: Option<bool>,
    /// Run-level: whether the run draws the disk-full scenario (see
    /// [`disk_full`]), fixed by the first caller.
    disk_full: Option<bool>,
    /// Run-level: whether the run draws the racing-floor scenario (see
    /// [`racing_floor`]), fixed by the first caller.
    racing_floor: Option<bool>,
    /// Run-level: whether the machines run without metrics (see
    /// [`unmetered`]), fixed by the first caller.
    unmetered: Option<bool>,
    /// Whether the run sets no user tag (#575 (key:value tags), [`untagged`]),
    /// fixed by the first caller.
    untagged: Option<bool>,
    /// Whether the run's creates draw batched stores (#599 (durability mode
    /// per journal), [`batched_journals`]), fixed by the first caller.
    batched_journals: Option<bool>,
    /// Run-level: when the clients' operations end (see [`run_end`]), fixed
    /// by the first caller.
    run_end: Option<Duration>,
    /// Run-level: whether the run draws the early-handover scenario (see
    /// [`early_handover`]), fixed by the first caller.
    early_handover: Option<bool>,
    /// Run-level: whether the run draws the paged-handover scenario (see
    /// [`paged_handover`]), fixed by the first caller.
    paged_handover: Option<bool>,
    /// Run-level: whether the run draws the stalled-proxy scenario (see
    /// [`stalled_proxy`]), fixed by the first caller.
    stalled_proxy: Option<bool>,
    /// Run-level: whether the run draws the handoff-storm scenario (see
    /// [`handoff_storm`]), fixed by the first caller.
    handoff_storm: Option<bool>,
    /// Run-level: whether the run draws the wide-grid scenario (see
    /// [`wide_grid`]), fixed by the first caller.
    wide_grid: Option<bool>,
    /// Run-level: how many zones label the acceptor pool, 0 for none (see
    /// [`acceptor_zones`]), fixed by the first caller.
    acceptor_zones: Option<u64>,
    /// Run-level: whether the run draws the zone-outage scenario (see
    /// [`zone_outage`]), fixed by the first caller.
    zone_outage: Option<bool>,
    /// Run-level: whether the run draws the moved-founder scenario (see
    /// [`moved_founder`]), fixed by the first caller.
    moved_founder: Option<bool>,
    /// Run-level: whether the run draws the replaced-founder scenario (see
    /// [`replaced_founder`]), fixed by the first caller.
    replaced_founder: Option<bool>,
    /// Run-level: whether the run draws the downed-founder scenario (see
    /// [`downed_founder`]), fixed by the first caller.
    downed_founder: Option<bool>,
    /// Run-level: whether the run draws the drained-machine scenario (see
    /// [`drained_machine`]), fixed by the first caller.
    drained_machine: Option<bool>,
    /// Run-level: whether a drained-machine run takes its machine down for
    /// good instead (see [`gone_machine`]), fixed by the first caller.
    gone_machine: Option<bool>,
    /// Run-level: whether the run draws the changed-mode scenario (see
    /// [`changed_mode`]), fixed by the first caller.
    changed_mode: Option<bool>,
    /// Run-level: whether the run draws the admitted-slot scenario (see
    /// [`admitted_slot`]), fixed by the first caller.
    admitted_slot: Option<bool>,
    /// Run-level: whether the run draws the cold-rot scenario (see
    /// [`cold_rot`]), fixed by the first caller.
    cold_rot: Option<bool>,
    /// Run-level: whether the run draws the batchers scenario (see
    /// [`batchers`]), fixed by the first caller.
    batchers: Option<bool>,
    /// Run-level: whether the run draws the second-cell scenario (see
    /// [`second_cell`]), fixed by the first caller.
    second_cell: Option<bool>,
    /// Run-level: whether the run draws the slow-link scenario (see
    /// [`slow_link`]), fixed by the first caller.
    slow_link: Option<bool>,
    /// Run-level: whether the run draws the lagging-acceptor scenario (see
    /// [`lagging_acceptor`]), fixed by the first caller.
    lagging_acceptor: Option<bool>,
    /// Run-level: whether the run draws the split-floor scenario (see
    /// [`split_floor`]), fixed by the first caller.
    split_floor: Option<bool>,
    /// Run-level: whether the run draws the lagging-replica scenario (see
    /// [`lagging_replica`]), fixed by the first caller.
    lagging_replica: Option<bool>,
    /// Run-level: whether the run runs the system journals (see
    /// [`system_journals`]), fixed by the first caller.
    system: Option<bool>,
    /// Run-level: how many genesis ranks host the system journals (see
    /// [`founder_count`]), fixed by the first caller.
    founder_count: Option<usize>,
    /// Run-level: each joiner's class and capacity (see
    /// [`joiner_machines`]), fixed by the first caller.
    machines: Option<Vec<JoinerMachine>>,
    /// Run-level: the machines' layout (see [`machine_layout`]), fixed by
    /// the first caller.
    machine_layout: Option<MachineLayout>,
    /// Run-level: every identifier the run names (see [`identifiers`]), fixed by the
    /// first caller.
    identifiers: Option<Identifiers>,
    nodes: BTreeMap<String, Entry>,
}

struct Entry {
    /// Drawn once, on the node's first boot: the registry entry is created
    /// exactly once per `ip` (`or_insert_with`), so a restart can only ever
    /// reuse the shape, never redraw it.
    shape: NodeShape,
    incarnations: u64,
}

fn registry(state: &StateHandle) -> Arc<Mutex<Registry>> {
    crate::state::published(state, SHAPE_KEY, Registry::default)
}

/// Boot `ip` once more: hand back the shape its first incarnation drew, drawing
/// it now if this *is* the first incarnation.
#[tracing::instrument(level = "debug", skip(state), fields(ip = %ip))]
pub(crate) fn boot(state: &StateHandle, ip: &str) -> Incarnation {
    let slow = slow_link(state);
    // Drawn before the lock: the scenarios take the registry's lock too.
    let slow_machine = slow_machine(state);
    let moved_founder = moved_founder(state);
    let wide_grid = wide_grid(state);
    let racing_floor = racing_floor(state);
    let entry_bytes = entry_capacity(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    let entry = guard.nodes.entry(ip.to_string()).or_insert_with(|| {
        let mut shape = NodeShape::draw(slow);
        // The edge admits no batch the stores cannot hold (#663 (batches at
        // the edge limits)): the default 1 MiB limit sits far above the
        // simulation's small entry logs.
        shape.tunables.max_batch_bytes = shape.tunables.max_batch_bytes.min(entry_bytes);
        assert_always!(
            shape.tunables.max_batch_bytes >= 16_384,
            "a node's byte limit admits the workload's largest record"
        );
        if slow_machine {
            // The slow-machine scenario: a full window every second, so
            // one falls inside the slow time.
            shape.tunables.load_interval = paros::roles::node::LOAD_INTERVAL_FLOOR;
        }
        if moved_founder {
            // The moved-founder scenario: a renamed founder registers its
            // new name, and an admitted machine follows the registry, once
            // per renewal period. At the floor the second crash finds a
            // cache that follows the move inside its window: at the drawn
            // period it struck on 1 of 22 moves in 1,000 hunt seeds.
            renew_at_floor(&mut shape.tunables);
        }
        if racing_floor {
            // The racing-floor scenario: the floor moves at every renewal,
            // once per 100 ms.
            assert_reachable!("election: the scenario moves the floor at every renewal");
            renew_at_floor(&mut shape.tunables);
            shape.tunables.election_compact_after = 1;
            // A tail wait as long as a renewal period (see `NodeShape::draw`).
            let renew_ms = u64::try_from(ELECTION_RENEW_FLOOR.as_millis()).unwrap_or(u64::MAX);
            shape.tunables.max_wait_ms = shape.tunables.max_wait_ms.max(renew_ms);
        }
        if wide_grid {
            // The wide-grid scenario: a stuck round moves after 1 to 3
            // re-sends, the knob's extreme.
            shape.tunables.partition_move_resends = moonpool_sim::sim_random_range(1_u64..4);
        }
        Entry {
            shape,
            incarnations: 0,
        }
    });
    entry.incarnations += 1;
    let incarnation = Incarnation {
        shape: entry.shape,
        number: entry.incarnations,
    };
    if incarnation.is_restart() {
        // The reuse actually happens on some seed: a node that was killed
        // and revived booted again under the knobs its first boot drew.
        assert_reachable!("a restarted node boots under its first incarnation's shape");
    }
    incarnation
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shape::quorum::grid_layouts;
    use paros::core::membership::QuorumSystem;

    /// The mechanism, not a seed: a second boot of the same node returns the
    /// first boot's shape without drawing again, while another node gets its
    /// own entry.
    #[test]
    fn a_restart_reuses_the_first_incarnations_shape() {
        let state = StateHandle::new();
        let first = boot(&state, "10.0.1.1");
        assert_eq!(first.number, 1);
        assert!(!first.is_restart());
        let second = boot(&state, "10.0.1.1");
        assert_eq!(second.number, 2);
        assert!(second.is_restart());
        assert_eq!(second.shape, first.shape);
        let other = boot(&state, "10.0.1.2");
        assert_eq!(other.number, 1);

        let registry = registry(&state);
        let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = &guard.nodes["10.0.1.1"];
        assert_eq!(entry.incarnations, 2);
        assert_eq!(guard.nodes["10.0.1.2"].incarnations, 1);
    }

    /// The policy is run-level: the first caller fixes it, and a majority
    /// policy is the plain system at every size.
    #[test]
    fn the_quorum_policy_is_fixed_by_the_first_caller() {
        let state = StateHandle::new();
        let first = quorum_policy(&state, 5);
        assert_eq!(first, QuorumPolicy::Majority);
        assert_eq!(quorum_policy(&state, 5), first);
        for n in 1..=6 {
            assert_eq!(first.system(n), QuorumSystem::Majority);
            assert_eq!(first.clean_copies(n, n), n / 2 + 1);
            assert_eq!(first.clean_copies(n, 6), n / 2 + 1);
        }
    }

    /// The grid policy's floor and its budget, spelled out: only layouts
    /// with `rows >= 2` and `cols >= 2` exist, a size no layout tiles runs
    /// a majority, the drawn layout is kept where it fits and the nearest
    /// row count elsewhere, and the copy budget is the whole floor — a grid
    /// tolerates no permanent loss, and on a matchmaker seed the minimum
    /// over the sizes in force is what the budget keeps.
    #[test]
    fn a_grid_policy_tiles_or_switches_and_tolerates_no_loss() {
        assert!(grid_layouts(3).is_empty());
        assert_eq!(grid_layouts(4), vec![(2, 2)]);
        assert!(grid_layouts(5).is_empty());
        assert_eq!(grid_layouts(6), vec![(2, 3), (3, 2)]);
        assert_eq!(grid_layouts(12), vec![(2, 6), (3, 4), (4, 3), (6, 2)]);
        let policy = QuorumPolicy::Grid {
            rows: 3,
            cols: 2,
            slack: 0,
        };
        assert_eq!(
            policy.system(6),
            QuorumSystem::grid(3, 2),
            "the drawn layout where it tiles"
        );
        assert_eq!(
            policy.system(4),
            QuorumSystem::grid(2, 2),
            "the first layout of a size the drawn row count does not tile"
        );
        assert_eq!(
            QuorumPolicy::Grid {
                rows: 2,
                cols: 3,
                slack: 0
            }
            .system(12),
            QuorumSystem::grid(2, 6),
            "the layout with the drawn row count where one exists"
        );
        assert_eq!(
            policy.system(3),
            QuorumSystem::Majority,
            "no layout: a majority"
        );
        assert_eq!(policy.system(5), QuorumSystem::Majority);
        for n in 1..=8 {
            assert!(policy.system(n).admits(n));
        }
        let partitions = QuorumPolicy::Grid {
            rows: 3,
            cols: 2,
            slack: 1,
        };
        assert_eq!(partitions.system(6), QuorumSystem::partitions(1, 2));
        assert_eq!(
            partitions.system(4),
            QuorumSystem::Grid {
                rows: 2,
                cols: 2,
                acks: 1
            },
            "a slack clamps to one accept on a shorter column"
        );
        for n in 1..=8 {
            assert!(partitions.system(n).admits(n));
        }
        assert_eq!(policy.tolerated_loss(6), 0);
        assert_eq!(policy.tolerated_loss(4), 0);
        assert_eq!(policy.tolerated_loss(3), 1, "a majority of three");
        assert_eq!(policy.tolerated_loss(5), 2);
        // A plain grid seed: the floor is the pool, the budget the whole
        // of it.
        assert_eq!(policy.clean_copies(6, 6), 6);
        // A matchmaker seed with a three-node floor on a six-node pool: the
        // sizes in force tolerate 1, 0, 2, 0 losses — the minimum is zero.
        assert_eq!(policy.clean_copies(3, 6), 3);
        // A matchmaker seed on a five-node pool: sizes 3, 4, 5 tolerate 1,
        // 0, 2 — still zero, because the four-member successor is a grid.
        assert_eq!(policy.clean_copies(3, 5), 3);
        // The majority and the split are monotone, so the floor's own
        // requirement is the budget whatever the pool.
        assert_eq!(QuorumPolicy::Majority.clean_copies(3, 6), 2);
        assert_eq!(QuorumPolicy::Flexible { q2: 1 }.clean_copies(3, 6), 3);
        assert_eq!(QuorumPolicy::Flexible { q2: 2 }.clean_copies(4, 6), 3);
    }

    /// The split's floor, spelled out: `q1 + q2 = n + 1` at every size, `q2`
    /// clamped to `1..=n/2`, and the clean-copy requirement is the larger
    /// quorum — `q1` — so the tolerated loss is `q2 - 1`.
    #[test]
    fn a_flexible_policy_cross_intersects_at_every_size() {
        for drawn in 1..=3 {
            let policy = QuorumPolicy::Flexible { q2: drawn };
            for n in 1..=6 {
                let QuorumSystem::Flexible { q1, q2 } = policy.system(n) else {
                    panic!("a flexible policy runs a flexible split");
                };
                assert!(q2 >= 1);
                assert!(q2 <= (n / 2).max(1));
                assert_eq!(q1 + q2, n + 1);
                assert!(policy.system(n).admits(n));
                assert_eq!(policy.clean_copies(n, n), q1);
                assert_eq!(n - policy.clean_copies(n, n), q2 - 1);
                assert_eq!(policy.tolerated_loss(n), q2 - 1);
            }
        }
    }
}
