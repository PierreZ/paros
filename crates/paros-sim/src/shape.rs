//! The per-logical-node **shape**: every knob the swarm draws *for a node* —
//! the driver's transport tunables, the write-window crash bias, the disk's
//! write-path fault rates — fixed at that node's first boot of a seed and reused
//! by every later incarnation of the same node.
//!
//! Why this is its own registry and not a local in `NodeProcess::run`: a
//! moonpool attrition restart builds a **fresh** `NodeProcess` from the factory
//! and re-enters `run()`, so anything drawn there is drawn again, and a node that
//! booted with a 4-slot peer queue could come back with the production default
//! (or a different extreme). That silently breaks the FDB knob model — a knob is
//! a *configuration* of the process for the run, not a per-boot coin — and it
//! makes "this seed ran node 2 at the capacity extreme" false for half of node
//! 2's lifetime. A seam crash (`RunError::SeamCrash`, the recovery loop inside
//! `run()`) never had this problem because it never leaves the invocation; the
//! registry gives the attrition path the same guarantee.
//!
//! What is deliberately **not** here: durable Paxos state (that is the
//! [`StorageWorld`](crate::world::StorageWorld)'s concern — the shape says how a
//! node is perturbed, never what it has promised or accepted), and the
//! per-*event* draws that describe one crash rather than one node — a restart
//! delay is drawn at the crash it delays, because two crashes of the same node
//! should not be forced to look alike. Run-level shape (the application's
//! digest-lane count, fixed by whichever node boots first) also lives here so
//! that the draw happens exactly once instead of once per boot with the extra
//! draws discarded.
//!
//! The registry is published on the per-iteration `StateHandle`, like the
//! storage world and the audit: fresh per seed, shared by every node, and
//! surviving every restart. Only a perturbing (main-campaign) node draws; the
//! scripted corpus takes the production defaults without spending randomness.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{StateHandle, assert_always, assert_reachable, buggify_knob};

use crate::world::storage::WritePathRates;
use paros::DriverTunables;

/// Well-known [`StateHandle`] key of the per-iteration registry.
const SHAPE_KEY: &str = "paros-node-shapes";

/// The wall-clock floor of every driver timeout that races the network: one
/// Phase-1 round trip over moonpool's default cross-datacenter link plus one
/// delivery batch, i.e. production's `5 ticks × 50 ms`. See [`NodeShape::draw`]
/// for why it is a floor and not a tunable.
const ROUND_TRIP_FLOOR_MS: u64 = 250;

/// Everything the swarm fixes about one logical node for one seed.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct NodeShape {
    /// The driver's transport and timing tunables.
    pub(crate) tunables: DriverTunables,
    /// Write-window crash bias (issue #19 B, the `TigerBeetle` "×10 while
    /// writes are in flight" pressure): a multiplier on the durability-seam
    /// crash probability. The seams are only ever consulted with a batch in
    /// flight, so biasing them *is* biasing crashes into the write window.
    pub(crate) seam_crash_bias: f64,
    /// The node's disk: its write-path fault rates.
    pub(crate) write_rates: WritePathRates,
}

impl NodeShape {
    /// The production shape: every knob at its default, nothing drawn.
    fn production() -> Self {
        Self {
            tunables: DriverTunables::default(),
            seam_crash_bias: 1.0,
            write_rates: WritePathRates::default(),
        }
    }

    /// Draw one perturbing node's shape — born workload-buggified (AGENTS.md
    /// prong 2): every default is production's constant, and an activated
    /// seed draws an extreme. One `buggify_knob!` location per knob, so a
    /// seed can be extreme in one dimension and ordinary in the next.
    fn draw() -> Self {
        let defaults = DriverTunables::default();
        // A handful-sized peer queue makes mailbox overflow (the
        // `dropped_at_mailbox` audit path) likely — a leader recovery page
        // bursts up to 64 Accepts into it at once — while the extreme's floor
        // stays at 4 so one tick's steady-state traffic (heartbeat ack +
        // catch-up request + snap ack + an accepted) still fits: a queue that
        // cannot hold one tick's worth deterministically starves whichever
        // class is enqueued last *every* tick, which defeats eventual
        // synchrony outright (witness seed 8560136109856440322: a capacity-1
        // queue held each beat's heartbeat ack, so every catch-up request of
        // a 62-second tail was dropped and the node wedged below a chosen
        // gap).
        let peer_queue_capacity = buggify_knob!(defaults.peer_queue_capacity, 4_usize..17_usize);
        // The batch extreme's floor keeps the per-peer throughput ceiling
        // (~batch / delivery round trip, and an in-sim round trip can
        // approach a whole tick under load) above the protocol's
        // steady-state per-peer rate: a one-message batch capped delivery
        // near 20 msg/s for the entire run — below what a leader's beat +
        // accepts + commits need — which is a permanent partition in
        // disguise, and 7/500 seeds wedged without ever converging
        // (witness seed 4877033065878342564: an n=2 cluster that never
        // chose a single slot in 67 s). Eight-to-32 still shrinks frames
        // 2-8x against the default 64 without making the run unwinnable.
        let delivery_batch = buggify_knob!(defaults.delivery_batch, 8_usize..33_usize);
        if peer_queue_capacity != defaults.peer_queue_capacity {
            // BUGGIFY pairing: the capacity extreme genuinely runs.
            assert_reachable!("a node runs with an extreme peer-queue capacity");
        }
        if delivery_batch != defaults.delivery_batch {
            // BUGGIFY pairing: the delivery-batch extreme genuinely runs.
            assert_reachable!("a node runs with an extreme delivery batch");
        }
        // Every duration that races the network has the same structural
        // floor, `ROUND_TRIP_FLOOR_MS`: moonpool's default cross-datacenter
        // link is 20-80 ms one way, so a Phase-1 round trip plus one delivery
        // batch is ~250 ms — production's own `5 ticks × 50 ms`. An election
        // timeout below it makes a candidate abandon its own round before its
        // promises return (witness: base 3 × 50 ms on a 4-node cluster
        // campaigned 1,185 times in 80 s and never once collected a quorum);
        // a keep-alive, connect or delivery timeout below it kills every
        // stream on every round trip. Both are a permanent partition wearing
        // a knob's clothes, not a configuration. The tick itself may go fast
        // (a 10 ms tick is 25 heartbeats per round trip); the tick-counted
        // timeouts are then raised to keep their wall-clock floor. The ranges
        // still cross the client's knobbed deadline (350 ms..3 s) in both
        // directions: a node slower than the client's patience is a valid,
        // ambiguous outcome, never a wrong one.
        let ms = Duration::from_millis;
        let tick_ms = buggify_knob!(50_u64, 10_u64..201_u64);
        let floor_ticks = ROUND_TRIP_FLOOR_MS.div_ceil(tick_ms);
        let tunables = DriverTunables {
            tick_interval: ms(tick_ms),
            election_timeout_base: buggify_knob!(5_u64, 2_u64..13_u64).max(floor_ticks),
            keep_alive_interval: ms(buggify_knob!(2000_u64, ROUND_TRIP_FLOOR_MS..5001_u64)),
            keep_alive_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
            connection_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
            delivery_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
            read_retry_ticks: buggify_knob!(10_u64, 1_u64..41_u64).max(floor_ticks),
            snapshot_queue_capacity: buggify_knob!(4_usize, 1_usize..9_usize),
            client_inbox_capacity: buggify_knob!(256_usize, 1_usize..17_usize),
            peer_inbox_capacity: buggify_knob!(1024_usize, 1_usize..65_usize),
            peer_queue_capacity,
            delivery_batch,
            // The matchmaking re-send cadence: from every tick (floor 1) to a
            // handful of election-timeout bases. A slow cadence stretches
            // every campaign that lost a reply; the election timeout still
            // bounds it.
            match_resend_ticks: buggify_knob!(5_u64, 1_u64..41_u64),
        };
        // The crash bias is a plain multiplier with no floor to defend: at
        // its extreme the seams crash on one batch in three inside the
        // window, and the window still closes long before the tail does.
        #[allow(clippy::cast_precision_loss)]
        let seam_crash_bias = buggify_knob!(1_u64, 4_u64..11_u64) as f64;
        Self {
            tunables,
            seam_crash_bias,
            write_rates: WritePathRates::draw(),
        }
    }
}

/// One node's view of the registry at boot: its shape, and which incarnation
/// this boot is (1 for the first boot of the seed).
pub(crate) struct Incarnation {
    pub(crate) shape: NodeShape,
    pub(crate) number: u64,
}

impl Incarnation {
    /// Whether this boot follows a process-level kill of the same node — a
    /// moonpool attrition restart on the main campaign, a scripted restart on
    /// the corpus. Seam-crash restarts never leave `run()` and so never come
    /// through here.
    pub(crate) fn is_restart(&self) -> bool {
        self.number > 1
    }
}

struct Entry {
    shape: NodeShape,
    /// How many times this node's shape was *drawn*. The registry can only
    /// ever draw once per node by construction; the count exists so the
    /// end-of-run gate asserts the construction rather than trusting it.
    draws: u64,
    incarnations: u64,
}

#[derive(Default)]
struct Registry {
    /// Run-level: the application's digest-lane count, fixed by the first
    /// node to boot.
    lanes: Option<u8>,
    /// Run-level: the bootstrap acceptor ranks (see [`bootstrap_ranks`]),
    /// fixed by the first caller — a node or a client.
    bootstrap: Option<Vec<u64>>,
    nodes: BTreeMap<String, Entry>,
}

fn registry(state: &StateHandle) -> Arc<Mutex<Registry>> {
    if let Some(registry) = state.get::<Arc<Mutex<Registry>>>(SHAPE_KEY) {
        return registry;
    }
    let registry = Arc::new(Mutex::new(Registry::default()));
    state.publish(SHAPE_KEY, registry.clone());
    registry
}

/// Boot `ip` once more: hand back the shape its first incarnation drew, drawing
/// it now if this *is* the first incarnation. `perturb` selects the drawn
/// (main-campaign) shape over the production one; it is a property of the
/// campaign, so every incarnation of a node passes the same value.
#[tracing::instrument(level = "debug", skip(state), fields(ip = %ip, perturb))]
pub(crate) fn boot(state: &StateHandle, ip: &str, perturb: bool) -> Incarnation {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    let entry = guard.nodes.entry(ip.to_string()).or_insert_with(|| Entry {
        shape: if perturb {
            NodeShape::draw()
        } else {
            NodeShape::production()
        },
        draws: 1,
        incarnations: 0,
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

/// The run's digest-lane count, drawn once by the first caller (a perturbing
/// node draws a knob; the corpus pins the default). 1 to 128 lanes is a blob
/// of 1 to 17 chunks, so the chunk-repair plane sees the single-chunk and the
/// many-chunk shapes instead of always five. Floor 1: one lane is a complete,
/// valid application.
pub(crate) fn lane_count(state: &StateHandle, perturb: bool) -> u8 {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.lanes.get_or_insert_with(|| {
        if perturb {
            buggify_knob!(crate::chain::DEFAULT_LANES, 1_u8..129_u8)
        } else {
            crate::chain::DEFAULT_LANES
        }
    })
}

/// The smallest acceptor configuration a matchmaker seed ever puts in force
/// — the bootstrap never draws below it and no reconfiguration shrinks below
/// it. Not a tunable: it is the size the storage world's copy budget is
/// computed over on such a seed (see [`config_floor`]). Three is the smallest
/// set where one loss keeps a quorum (the dead-node budget's own floor).
pub(crate) const MIN_BOOTSTRAP: usize = 3;

/// The **configuration floor** of a run, and the size the storage world's
/// copy budget (`StorageWorld::set_cluster_size`) is computed over. On a
/// plain deployment the membership is fixed at the whole pool, so the budget
/// is sized by it, exactly as before matchmakers existed. On a matchmaker
/// deployment the acceptor set may shrink as far as [`MIN_BOOTSTRAP`], and the
/// budget is sized by *that*: a budget that keeps a clean quorum of the
/// smallest configuration keeps one of every larger configuration too (the
/// tolerated loss `⌊(n-1)/2⌋` only grows with `n`), so it stays conservative
/// through every reconfiguration at the cost of fewer storage-fault
/// injections on the larger matchmaker seeds.
pub(crate) fn config_floor(pool: usize, has_matchmakers: bool) -> usize {
    if has_matchmakers {
        MIN_BOOTSTRAP.min(pool)
    } else {
        pool
    }
}

/// The run's **bootstrap acceptor ranks** — membership as protocol data,
/// drawn once per seed by whichever process or workload asks first and
/// handed back unchanged to every later caller (an attrition restart must
/// boot the same node into the same bootstrap configuration).
///
/// The default is the whole pool: every node an acceptor, the plain
/// Multi-Paxos deployment and the shape of every existing axis. A perturbing
/// seed that deploys matchmakers may instead draw a **subset** (a run with
/// `has_matchmakers == false` never draws — a plain deployment's membership
/// must include every node, per `paros::Config::peers`), leaving the other
/// nodes as *spares*: addressable pool members outside every configuration
/// until a `Reconfigure` pulls them in. Two knob locations, each its own
/// per-seed activation: the subset *size* (floor [`MIN_BOOTSTRAP`], ceiling
/// the pool) and the *rotation* that decides which ranks are the spares, so
/// a seed can bootstrap on `{2, 3, 4}` of a five-node pool and leave
/// `{0, 1}` — the lowest ranks, the ones every "first node" heuristic would
/// pick — outside.
#[tracing::instrument(level = "debug", skip(state), fields(pool, has_matchmakers, perturb))]
pub(crate) fn bootstrap_ranks(
    state: &StateHandle,
    pool: usize,
    has_matchmakers: bool,
    perturb: bool,
) -> Vec<u64> {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .bootstrap
        .get_or_insert_with(|| {
            let all: Vec<u64> = (0..pool)
                .map(|i| u64::try_from(i).unwrap_or(u64::MAX))
                .collect();
            if !(perturb && has_matchmakers && pool > MIN_BOOTSTRAP) {
                return all;
            }
            let size = buggify_knob!(pool, MIN_BOOTSTRAP..pool);
            if size == pool {
                return all;
            }
            // BUGGIFY pairing: a seed genuinely bootstraps on a subset.
            assert_reachable!("a run bootstraps on a subset of the node pool, leaving spares");
            let rotation = buggify_knob!(0_usize, 1_usize..pool);
            if rotation != 0 {
                // BUGGIFY pairing: the spares are not simply the highest ranks.
                assert_reachable!("a run's spares are drawn from the low ranks");
            }
            let mut ranks: Vec<u64> = (0..size)
                .map(|i| u64::try_from((rotation + i) % pool).unwrap_or(u64::MAX))
                .collect();
            ranks.sort_unstable();
            ranks
        })
        .clone()
}

/// The shape gate, evaluated once per run from the workload's `check()`: every
/// node that booted drew its shape exactly once, however many incarnations it
/// went through. A second draw is what an attrition restart used to do
/// silently; this keeps it a violation instead of a regression waiting to be
/// noticed.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn check_shape_gates(state: &StateHandle) {
    let registry = registry(state);
    let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    for (ip, entry) in &guard.nodes {
        assert_always!(
            entry.draws == 1 && entry.incarnations >= 1,
            "a node's shape is drawn exactly once per seed, whatever its incarnation count",
            { "ip" => ip.as_str(), "draws" => entry.draws, "incarnations" => entry.incarnations }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The mechanism, not a seed: a second boot of the same node returns the
    /// first boot's shape without drawing again, while another node gets its
    /// own entry.
    #[test]
    fn a_restart_reuses_the_first_incarnations_shape() {
        let state = StateHandle::new();
        let first = boot(&state, "10.0.1.1", true);
        assert_eq!(first.number, 1);
        assert!(!first.is_restart());
        let second = boot(&state, "10.0.1.1", true);
        assert_eq!(second.number, 2);
        assert!(second.is_restart());
        assert_eq!(second.shape, first.shape);
        let other = boot(&state, "10.0.1.2", true);
        assert_eq!(other.number, 1);

        let registry = registry(&state);
        let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = &guard.nodes["10.0.1.1"];
        assert_eq!(entry.draws, 1);
        assert_eq!(entry.incarnations, 2);
        assert_eq!(guard.nodes["10.0.1.2"].incarnations, 1);
    }

    /// The lane count is a run-level shape: the first caller fixes it.
    #[test]
    fn the_lane_count_is_fixed_by_the_first_caller() {
        let state = StateHandle::new();
        let first = lane_count(&state, true);
        assert_eq!(lane_count(&state, true), first);
        assert_eq!(lane_count(&state, false), first);
    }

    /// A scripted node takes the production shape and never draws.
    #[test]
    fn a_scripted_node_runs_the_production_shape() {
        let state = StateHandle::new();
        let shape = boot(&state, "10.0.1.1", false).shape;
        assert_eq!(shape, NodeShape::production());
    }
}
