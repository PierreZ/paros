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

use moonpool_sim::{StateHandle, assert_reachable, buggify_knob};

use crate::world::storage::WritePathRates;
use paros::{DriverTunables, QuorumSystem};

/// Well-known [`StateHandle`] key of the per-iteration registry.
const SHAPE_KEY: &str = "paros-node-shapes";

/// The wall-clock floor of every driver timeout that races the network: one
/// Phase-1 round trip over moonpool's default cross-datacenter link plus one
/// delivery batch, i.e. production's `5 ticks × 50 ms`. See [`NodeShape::draw`]
/// for why it is a floor and not a tunable.
const ROUND_TRIP_FLOOR_MS: u64 = 250;

/// The default per-restart chance of a terminal storage loss — a wiped node
/// disk (#124) or an unusable matchmaker registry (#125).
const DEFAULT_LOSS_PCT: u32 = 35;
/// The floor of both loss knobs: rare enough that a seed still has restarts
/// that come back.
const MIN_LOSS_PCT: u32 = 5;
/// The ceiling of both loss knobs: the dead-node and matchmaker-loss budgets
/// bound the damage, so the extreme stays a valid deployment.
const MAX_LOSS_PCT: u32 = 75;

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
    /// Percent chance that a chaotic restart of this node comes back on an
    /// empty disk (#124, `crate::process`). Floor 5: the coin must stay rare
    /// enough that a run is a run and not an all-amnesia cluster (a node that
    /// wipes on its first restart never contributes a second incarnation to
    /// anything). Ceiling 75: the world's dead-node budget bounds the damage
    /// whatever the rate, so the extreme is a valid, very forgetful disk.
    pub(crate) wipe_pct: u32,
    /// Percent chance that a chaotic restart of this *matchmaker* finds its
    /// registry unusable (#125). Same floor and ceiling, and the same
    /// argument: the matchmaker-loss budget (one per run, and only where the
    /// bootstrap set can spare it) bounds it.
    pub(crate) matchmaker_loss_pct: u32,
}

impl NodeShape {
    /// The production shape: every knob at its default, nothing drawn.
    fn production() -> Self {
        Self {
            tunables: DriverTunables::default(),
            seam_crash_bias: 1.0,
            write_rates: WritePathRates::default(),
            wipe_pct: DEFAULT_LOSS_PCT,
            matchmaker_loss_pct: DEFAULT_LOSS_PCT,
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
            // Floor 1: the snapshot lane is a keep-newest `PeerMailbox` that
            // carries one class (`InstallSnapshot`), so a one-slot lane only
            // ever evicts an older offer to the same peer in favour of the
            // newer one, and the requester re-asks every beat — a slower
            // transfer, never a starved class.
            snapshot_queue_capacity: buggify_knob!(4_usize, 1_usize..9_usize),
            // Floor 1: the client inboxes are bounded mpsc queues whose tonic
            // handlers `send().await` into them, so a full inbox is
            // backpressure (the h2 request waits for the loop to drain one
            // request), never a dropped request; a one-slot inbox serialises
            // clients, and a client that waits past its own deadline times
            // out ambiguously, never wrongly.
            client_inbox_capacity: buggify_knob!(256_usize, 1_usize..17_usize),
            // Floor 1, same contract: the peer-delivery handler and the
            // matchmaker reply sinks `send().await`, so a full inbox stalls
            // the delivering peer's RPC until the loop takes one message
            // (one message per loop iteration is throttling, not a drop);
            // a batch that stalls past `delivery_timeout` is written off and
            // repaired by the next re-send, the mailbox's own contract. Only
            // the duplicate-reply hook uses `try_send`, and a duplicate that
            // finds no room is simply not injected.
            peer_inbox_capacity: buggify_knob!(1024_usize, 1_usize..65_usize),
            peer_queue_capacity,
            delivery_batch,
            // The matchmaking re-send cadence: from every tick (floor 1) to a
            // handful of election-timeout bases. A slow cadence stretches
            // every campaign that lost a reply; the election timeout still
            // bounds it.
            match_resend_ticks: buggify_knob!(5_u64, 1_u64..41_u64),
            // The GC re-send cadence, its own location: the two round trips
            // are unrelated, and a seed should be able to be extreme in one
            // and ordinary in the next. Floor 1; the ceiling is unbounded and
            // still winnable — a watermark that is never raised costs the
            // matchmakers their retained histories, never safety.
            gc_resend_ticks: buggify_knob!(5_u64, 1_u64..41_u64),
            // The handover re-send cadence. Floor 1; bounded above by the
            // stall budget drawn just below — a cadence past
            // `election_timeout * reconfigure_timeout_elections` would let
            // the phase be abandoned before it is ever re-sent, which is no
            // retry rather than a slow one, so the ceiling stays under the
            // smallest budget the next knob can draw (1 timeout × the
            // election base's own floor).
            reconfigurer_resend_ticks: buggify_knob!(5_u64, 1_u64..11_u64),
            // The handover stall budget. Floor 1 election timeout: one
            // re-sent request has time to be answered, and a phase nobody
            // answers is abandoned within a timeout instead of holding the
            // `Busy` refusal for the tail.
            reconfigure_timeout_elections: buggify_knob!(4_u64, 1_u64..17_u64),
            // The preempted decree's backoff ceiling, drawn independently of
            // the election clock (it used to be `2 × election_timeout_base`,
            // which correlated the two). Floor 1: a one-tick draw is no
            // jitter at all, so dueling reconfigurers may keep preempting
            // each other — a liveness cost the stall budget ends, never a
            // safety one.
            reconfigure_backoff_max_ticks: buggify_knob!(10_u64, 1_u64..41_u64),
        };
        if tunables.gc_resend_ticks != 5 {
            // BUGGIFY pairing: the GC cadence extreme genuinely runs.
            assert_reachable!("a node runs with an extreme GC re-send cadence");
        }
        if tunables.reconfigurer_resend_ticks != 5 {
            // BUGGIFY pairing: the handover cadence extreme genuinely runs.
            assert_reachable!("a node runs with an extreme handover re-send cadence");
        }
        if tunables.reconfigure_timeout_elections != 4 {
            // BUGGIFY pairing: the handover stall budget extreme genuinely runs.
            assert_reachable!("a node runs with an extreme handover stall budget");
        }
        if tunables.reconfigure_backoff_max_ticks != 10 {
            // BUGGIFY pairing: the decree backoff extreme genuinely runs.
            assert_reachable!("a node runs with an extreme decree backoff ceiling");
        }
        // The crash bias is a plain multiplier with no floor to defend: at
        // its extreme the seams crash on one batch in three inside the
        // window, and the window still closes long before the tail does.
        #[allow(clippy::cast_precision_loss)]
        let seam_crash_bias = buggify_knob!(1_u64, 4_u64..11_u64) as f64;
        Self {
            tunables,
            seam_crash_bias,
            write_rates: WritePathRates::draw(),
            wipe_pct: buggify_knob!(DEFAULT_LOSS_PCT, MIN_LOSS_PCT..MAX_LOSS_PCT + 1),
            matchmaker_loss_pct: buggify_knob!(DEFAULT_LOSS_PCT, MIN_LOSS_PCT..MAX_LOSS_PCT + 1),
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
    /// Drawn once, on the node's first boot: the registry entry is created
    /// exactly once per `ip` (`or_insert_with`), so a restart can only ever
    /// reuse the shape, never redraw it.
    shape: NodeShape,
    incarnations: u64,
}

/// The run's **quorum-system policy** (#140, #141): how every configuration
/// the run puts in force is judged, drawn once per seed and applied to each
/// configuration's own size. Protocol data like the bootstrap ranks — a
/// majority is the plain deployment's system and the default; a flexible
/// split and an acceptor grid are the opt-ins the swarm turns on per seed,
/// so one campaign proves the library under all three.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuorumPolicy {
    /// A majority in both phases.
    Majority,
    /// Flexible Paxos's simple quorums with `q2` accepts deciding and
    /// `q1 = n - q2 + 1` promises electing — the tightest split that still
    /// cross-intersects. `q2` is clamped to `1..=n/2` per configuration, so a
    /// successor of a different size runs the same *kind* of split at its
    /// own dimensions.
    Flexible {
        /// The Phase-2 quorum size drawn at the pool, before clamping.
        q2: usize,
    },
    /// Compartmentalized Paxos's acceptor grid (#141): rows elect, columns
    /// decide, and every slot's `Accept` goes to one column. The layout is
    /// drawn at the pool; a configuration of another size runs the grid
    /// layout closest to it that tiles it ([`grid_layouts`]), or a majority
    /// when no layout does — a successor on a grid seed is grid-shaped or
    /// switches system, never malformed.
    Grid {
        /// The rows drawn at the pool.
        rows: usize,
        /// The columns drawn at the pool.
        cols: usize,
    },
}

/// Every grid layout of `n` acceptors the harness may run: `rows × cols ==
/// n` with **both `rows >= 2` and `cols >= 2`** — the knob's floor. A
/// `1 × n` grid is Flexible Paxos's `|Q1| = 1, |Q2| = n` thought experiment
/// and an `n × 1` grid its mirror: valid configurations on paper and
/// permanent partitions under attrition (one dead acceptor stops Phase 2,
/// or Phase 1, for the rest of the run), which is the defeat of eventual
/// synchrony the knob doctrine forbids. In row-major order of `rows`, so the
/// list is a pure function of `n` every process derives alike. Empty when
/// `n` is prime or below four.
pub(crate) fn grid_layouts(n: usize) -> Vec<(usize, usize)> {
    (2..n)
        .filter(|rows| n.is_multiple_of(*rows) && n / rows >= 2)
        .map(|rows| (rows, n / rows))
        .collect()
}

impl QuorumPolicy {
    /// The quorum system a configuration of `n` acceptors runs under this
    /// policy. **Floor:** `q1 + q2 = n + 1 > n` by construction and
    /// `q2 >= 1`; the extreme `q2 = 1` is Flexible Paxos's "any single
    /// acceptor learns a value in one hop", a valid configuration because
    /// the fault window closes before the recovery tail does and the copy
    /// budget ([`StorageWorld::set_budget`]) is sized over the split's
    /// Phase-1 quorum. `n = 1` and `n = 2` admit only `q2 = 1`. A grid
    /// policy lays the drawn layout over `n` when it tiles it, else the
    /// layout of `n` with the same row count, else the first that tiles it,
    /// else a majority (see [`grid_layouts`] for the floor).
    ///
    /// [`StorageWorld::set_budget`]: crate::world::StorageWorld::set_budget
    pub(crate) fn system(self, n: usize) -> QuorumSystem {
        match self {
            QuorumPolicy::Majority => QuorumSystem::Majority,
            QuorumPolicy::Flexible { q2 } => {
                let q2 = q2.clamp(1, (n / 2).max(1));
                QuorumSystem::Flexible {
                    q1: n.saturating_sub(q2).saturating_add(1),
                    q2,
                }
            }
            QuorumPolicy::Grid { rows, cols } => {
                let layouts = grid_layouts(n);
                layouts
                    .iter()
                    .find(|(r, c)| *r == rows && *c == cols)
                    .or_else(|| layouts.iter().find(|(r, _)| *r == rows))
                    .or_else(|| layouts.first())
                    .map_or(QuorumSystem::Majority, |(rows, cols)| QuorumSystem::Grid {
                        rows: *rows,
                        cols: *cols,
                    })
            }
        }
    }

    /// The copies of a record a configuration of `n` may lose under this
    /// policy and stay winnable: `n` minus the larger of the two phase
    /// quorums, read off the membership boundary, never re-derived from a
    /// count here — `⌊(n-1)/2⌋` under a majority, `q2 - 1` under a flexible
    /// split (`n - q1`: a faulty slot is decidable only once a full Phase-1
    /// quorum of clean answers holds, CTRL R2/R3, and a value is choosable
    /// only while a Phase-2 quorum is live, so both quorums must keep clean
    /// members). Under a grid it is **zero**, whatever the layout: the
    /// record-recoverability bound would be `⌊(m-1)/2⌋` over `m = min(rows,
    /// cols)` (that many losses break strictly fewer rows and columns than
    /// the grid has), but the same budget bounds the *permanent* losses —
    /// a corruption park, a wipe — and every slot's Phase 2 is addressed to
    /// one column, so one acceptor dead for good freezes its column's
    /// slots for the rest of the run: an unwinnable run, which no budget
    /// may permit. For the grids the pool range admits (`2 × 2`, `2 × 3`,
    /// `3 × 2`) the two bounds coincide anyway.
    pub(crate) fn tolerated_loss(self, n: usize) -> usize {
        match self.system(n) {
            QuorumSystem::Grid { .. } => 0,
            system => n.saturating_sub(
                system
                    .phase1_quorum_size(n)
                    .max(system.phase2_quorum_size(n)),
            ),
        }
    }

    /// The clean live copies a record must keep at the run's configuration
    /// floor `floor` so that **every** configuration the run may put in
    /// force — every size in `floor..=pool` — stays winnable: the floor
    /// minus the smallest loss any of those sizes tolerates
    /// ([`QuorumPolicy::tolerated_loss`]). Under a majority or a flexible
    /// split the tolerated loss only grows with `n` (the split fixes `q2`),
    /// so this is the floor configuration's own `max(q1, q2)` — `⌊n/2⌋ + 1`
    /// under a majority, `q1` under the split. Under a grid it is not
    /// monotone (a three-member successor is a majority tolerating one
    /// loss, a four-member one a `2 × 2` grid tolerating none), so the
    /// minimum over the range is what the budget keeps; on every grid seed
    /// that is the whole floor — no storage-fault injection and no park,
    /// the cost the grid pays for its `1 / cols`.
    pub(crate) fn clean_copies(self, floor: usize, pool: usize) -> usize {
        let loss = (floor..=pool.max(floor))
            .map(|n| self.tolerated_loss(n))
            .min()
            .unwrap_or(0);
        floor.saturating_sub(loss)
    }
}

#[derive(Default)]
struct Registry {
    /// Run-level: the application's digest-lane count, fixed by the first
    /// node to boot.
    lanes: Option<u8>,
    /// Run-level: the quorum-system policy (see [`quorum_policy`]), fixed by
    /// the first caller — a node or a client.
    quorum: Option<QuorumPolicy>,
    /// Run-level: the bootstrap acceptor ranks (see [`bootstrap_ranks`]),
    /// fixed by the first caller — a node or a client.
    bootstrap: Option<Vec<u64>>,
    /// Run-level: the bootstrap matchmaker ranks (see
    /// [`matchmaker_bootstrap_ranks`]), fixed by the first caller.
    matchmaker_bootstrap: Option<Vec<u64>>,
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

/// The run's **quorum-system policy** (#140, #141), drawn once per seed by
/// whichever process or workload asks first and handed back unchanged to
/// every later caller (an attrition restart must boot the same node under
/// the same system, and every client must compose successors under it).
///
/// The default is the majority, the plain deployment's system. A perturbing
/// seed may instead draw a **flexible split**: one `buggify_knob!` location
/// for `q2` over the pool (default the majority, extreme `1..=pool/2`), with
/// `q1 = n - q2 + 1` derived per configuration ([`QuorumPolicy::system`]);
/// or, on a pool whose size tiles a grid, an **acceptor grid**: its own
/// `buggify_knob!` location over the layouts of the pool ([`grid_layouts`],
/// floor `rows >= 2` and `cols >= 2`; default no grid), so a seed can be
/// extreme in one system and never the other. Both are opt-in configuration
/// data on the core side, so a majority seed is byte-identical to a run
/// before the policy existed; a corpus run (`perturb == false`) never draws.
#[tracing::instrument(level = "debug", skip(state), fields(pool, perturb))]
pub(crate) fn quorum_policy(state: &StateHandle, pool: usize, perturb: bool) -> QuorumPolicy {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.quorum.get_or_insert_with(|| {
        let majority = pool / 2 + 1;
        if !perturb || pool < 2 {
            return QuorumPolicy::Majority;
        }
        let q2 = buggify_knob!(majority, 1_usize..(pool / 2 + 1));
        if q2 != majority {
            // BUGGIFY pairing: a seed genuinely runs a flexible split (a
            // cause, never a `sometimes`; the outcomes — a slot decided by
            // fewer accepts than a majority, an election completed under the
            // split — are the audit's gates).
            assert_reachable!("a run draws a flexible quorum system");
            return QuorumPolicy::Flexible { q2 };
        }
        // The grid knob, its own location: index 0 is "no grid", `k` the
        // `k`-th layout of the pool. A pool that tiles no grid (three or
        // five nodes) draws nothing here and stays a majority.
        let layouts = grid_layouts(pool);
        if layouts.is_empty() {
            return QuorumPolicy::Majority;
        }
        let pick = buggify_knob!(0_usize, 1_usize..layouts.len() + 1);
        let Some((rows, cols)) = pick.checked_sub(1).and_then(|k| layouts.get(k).copied()) else {
            return QuorumPolicy::Majority;
        };
        // BUGGIFY pairing: a seed genuinely runs an acceptor grid (a cause;
        // the outcomes — a slot decided on a column, an election covered by
        // a row — are the audit's gates).
        assert_reachable!("a run draws an acceptor grid");
        QuorumPolicy::Grid { rows, cols }
    })
}

/// The smallest acceptor configuration a matchmaker seed ever puts in force
/// — the bootstrap never draws below it and no reconfiguration shrinks below
/// it. Not a tunable: it is the size the storage world's copy budget is
/// computed over on such a seed (see [`config_floor`]). Three is the smallest
/// set where one loss keeps a quorum (the dead-node budget's own floor).
pub(crate) const MIN_BOOTSTRAP: usize = 3;

/// The **configuration floor** of a run, and the size the storage world's
/// copy budget (`StorageWorld::set_budget`) is computed over. On a plain
/// deployment the membership is fixed at the whole pool, so the budget is
/// sized by it, exactly as before matchmakers existed. On a matchmaker
/// deployment the acceptor set may shrink as far as [`MIN_BOOTSTRAP`], and the
/// budget is sized by *that*: a budget that keeps the clean copies the
/// policy demands over every size the run may put in force
/// ([`QuorumPolicy::clean_copies`] takes the floor *and* the pool: the
/// tolerated loss `n - max(q1, q2)` only grows with `n` under a policy that
/// fixes `q2`, but a grid's `⌊(min(rows, cols) - 1)/2⌋` does not, so the
/// smallest loss over the range is what the budget keeps), so it stays
/// conservative through every reconfiguration at the cost of fewer
/// storage-fault injections on the larger matchmaker seeds — and, under a
/// flexible split at a three-node floor (`q2 = 1`, `q1 = 3`) or on any
/// grid seed, of none at all.
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

/// A scripted case's **fixed bootstrap ranks**: `0..n`, installed by whoever
/// asks first (every scripted node asks with the same `n`) so the corpus can
/// stage a spare outside the bootstrap configuration without a draw.
pub(crate) fn fixed_bootstrap_ranks(state: &StateHandle, n: usize) -> Vec<u64> {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .bootstrap
        .get_or_insert_with(|| {
            (0..n)
                .map(|i| u64::try_from(i).unwrap_or(u64::MAX))
                .collect()
        })
        .clone()
}

/// The run's **bootstrap matchmaker ranks** (#125): generation 0's set, drawn
/// once per seed by whichever process or workload asks first. The default is
/// the whole matchmaker pool; a perturbing seed with two or more matchmakers
/// may draw a **subset** (any size from one up — a one- or two-member set is
/// a valid registry that tolerates no loss), leaving the rest as matchmaker
/// *spares* a `ReconfigureMatchmakers` pulls in. Two knob locations, as for
/// the acceptors: the subset size and the rotation that picks the spares.
#[tracing::instrument(level = "debug", skip(state), fields(pool, perturb))]
pub(crate) fn matchmaker_bootstrap_ranks(
    state: &StateHandle,
    pool: usize,
    perturb: bool,
) -> Vec<u64> {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .matchmaker_bootstrap
        .get_or_insert_with(|| {
            let all: Vec<u64> = (0..pool)
                .map(|i| u64::try_from(i).unwrap_or(u64::MAX))
                .collect();
            if !(perturb && pool >= 2) {
                return all;
            }
            let size = buggify_knob!(pool, 1_usize..pool);
            if size == pool {
                return all;
            }
            // BUGGIFY pairing: a seed genuinely bootstraps its matchmakers on
            // a subset, leaving matchmaker spares.
            assert_reachable!(
                "a run bootstraps on a subset of the matchmaker pool, leaving spares"
            );
            let rotation = buggify_knob!(0_usize, 1_usize..pool);
            if rotation != 0 {
                // BUGGIFY pairing: the matchmaker spares are not simply the
                // highest ranks.
                assert_reachable!("a run's matchmaker spares are drawn from the low ranks");
            }
            let mut ranks: Vec<u64> = (0..size)
                .map(|i| u64::try_from((rotation + i) % pool).unwrap_or(u64::MAX))
                .collect();
            ranks.sort_unstable();
            ranks
        })
        .clone()
}

/// The smallest bootstrap matchmaker set that can lose one member and keep a
/// quorum — and therefore the smallest set the world will ever take a
/// matchmaker from ([`StorageWorld::park_matchmaker`]) and the ceiling of
/// [`matchmaker_floor`]. Not a tunable: it is what the matchmaker-loss budget
/// is computed over.
///
/// [`StorageWorld::park_matchmaker`]: crate::world::StorageWorld::park_matchmaker
pub(crate) const MATCHMAKER_LOSS_FLOOR: usize = 3;

/// The smallest matchmaker set a run may put in force (#125):
/// [`MATCHMAKER_LOSS_FLOOR`] where the bootstrap set has that many members or
/// more, else the bootstrap size itself.
pub(crate) fn matchmaker_floor(bootstrap: usize) -> usize {
    bootstrap.min(MATCHMAKER_LOSS_FLOOR)
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

    /// The policy is run-level: the first caller fixes it, a corpus run never
    /// draws, and a majority policy is the plain system at every size.
    #[test]
    fn the_quorum_policy_is_fixed_by_the_first_caller() {
        let state = StateHandle::new();
        let first = quorum_policy(&state, 5, false);
        assert_eq!(first, QuorumPolicy::Majority);
        assert_eq!(quorum_policy(&state, 5, true), first);
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
        let policy = QuorumPolicy::Grid { rows: 3, cols: 2 };
        assert_eq!(
            policy.system(6),
            QuorumSystem::Grid { rows: 3, cols: 2 },
            "the drawn layout where it tiles"
        );
        assert_eq!(
            policy.system(4),
            QuorumSystem::Grid { rows: 2, cols: 2 },
            "the first layout of a size the drawn row count does not tile"
        );
        assert_eq!(
            QuorumPolicy::Grid { rows: 2, cols: 3 }.system(12),
            QuorumSystem::Grid { rows: 2, cols: 6 },
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
