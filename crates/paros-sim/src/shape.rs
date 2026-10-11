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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{StateHandle, assert_always, assert_reachable, buggify_knob};

use paros::{
    DriverTunables, JournalId, JournalIdentifier, JournalStoreConfig, QuorumSystem, TenantId,
    WriterMode,
};

/// Well-known [`StateHandle`] key of the per-iteration registry.
const SHAPE_KEY: &str = "paros-node-shapes";

/// The longest a drawn quarantine holds a journal down, in milliseconds:
/// production's 80 ticks of 100 ms (`DriverTunables::production`).
const QUARANTINE_CEILING_MS: u64 = 8_000;

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

/// The default per-restart chance that the operator restarts a node under an
/// edited configuration (#207, [`NodeShape::config_edit_pct`]).
const DEFAULT_CONFIG_EDIT_PCT: u32 = 10;

/// Everything the swarm fixes about one logical node for one seed.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct NodeShape {
    /// The driver's transport and timing tunables.
    pub(crate) tunables: DriverTunables,
    /// Percent chance that a chaotic restart of this node comes back on an
    /// empty disk (#124, `crate::process`). Floor 5: the coin must stay rare
    /// enough that a run is a run and not an all-amnesia cluster (a node that
    /// wipes on its first restart never contributes a second incarnation to
    /// anything). Ceiling 75: the world's dead-node budget bounds the damage
    /// whatever the rate, so the extreme is a valid, very forgetful disk.
    pub(crate) wipe_pct: u32,
    /// Percent chance that a chaotic restart of this *matchmaker* comes back
    /// on a wiped registry (#125, #183), which the library then refuses to
    /// boot. Same floor and ceiling, and the same
    /// argument: the matchmaker-loss budget (one per run, and only where the
    /// bootstrap set can spare it) bounds it.
    pub(crate) matchmaker_loss_pct: u32,
    /// Percent chance that a chaotic restart of this node is attempted under
    /// an **edited configuration** (#207): the operator changed the
    /// bootstrap membership in the configuration file, the library refuses
    /// the store formatted under the old one, and the operator restores it
    /// and restarts. No floor to defend and the ceiling is structural: a
    /// refused incarnation writes nothing, and the corrected restart is an
    /// ordinary restart, so even an operator who edits on every restart
    /// costs the cluster one restart delay per edit.
    pub(crate) config_edit_pct: u32,
}

impl NodeShape {
    /// Draw one node's shape — born workload-buggified (AGENTS.md
    /// prong 2): every default is production's constant, and an activated
    /// seed draws an extreme. One `buggify_knob!` location per knob, so a
    /// seed can be extreme in one dimension and ordinary in the next.
    ///
    /// On a slow-link seed ([`slow_link`]) the node runs a fast tick and
    /// the floor batch, never the production profile.
    fn draw(slow_link: bool) -> Self {
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
        // chose a single slot in 67 s). The rate is **per journal**, and a
        // link carries every journal its two ends serve — the genesis ones
        // and every one a client created at runtime (#189) — so a floor of
        // eight, sized for one journal, left a link carrying three with a
        // backlog that never drained: each message waited ~7 round trips
        // behind the others' beats, and under a `q1 = n` split every quorum
        // read waited on that link and expired, so no claim ever wrote
        // (witness seed 9499531859745476743, #205's hunt: 56 messages queued
        // node 2 → 0 for the whole 130 s tail; 1–3 at a floor of 24).
        // Twenty-four to 32 still shrinks frames 2-2.7x against the
        // default 64.
        let delivery_batch = buggify_knob!(defaults.delivery_batch, 24_usize..33_usize);
        let delivery_batch = if slow_link { 24 } else { delivery_batch };
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
        // still cross the client's knobbed deadline (1 s..3 s) in both
        // directions: a node slower than the client's patience is a valid,
        // ambiguous outcome, never a wrong one.
        // A read's confirmation window is the one exception to a single
        // round trip: a quorum read waits on the *slowest* member of its
        // quorum, through delivery batching on a link that carries every
        // journal's beats, so on a loaded link it lands just past one round
        // trip on every read and the driver answers each one unserved before
        // its acks arrive — no claim (#204: claims start with a read) for a
        // whole run (witness seed 3336991135298497961, #205's 10k hunt:
        // confirmations at ~9 ticks against a 5-tick window, 172 s without a
        // write). Its floor is two round trips.
        let ms = Duration::from_millis;
        let tick_ms = buggify_knob!(50_u64, 10_u64..201_u64);
        // A slow-link seed beats fast: 10 to 17 ms (#386's witness ran 17).
        let tick_ms = if slow_link {
            tick_ms.clamp(10, 17)
        } else {
            tick_ms
        };
        let floor_ticks = ROUND_TRIP_FLOOR_MS.div_ceil(tick_ms);
        let election_renew_ms = buggify_knob!(500_u64, 100_u64..1001_u64);
        let drawn = DriverTunables {
            tick_interval: ms(tick_ms),
            election_timeout_base: buggify_knob!(5_u64, 2_u64..13_u64).max(floor_ticks),
            keep_alive_interval: ms(buggify_knob!(2000_u64, ROUND_TRIP_FLOOR_MS..5001_u64)),
            keep_alive_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
            connection_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
            delivery_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
            read_retry_ticks: buggify_knob!(10_u64, 1_u64..41_u64).max(2 * floor_ticks),
            // Floor 0: a zero wait answers every journal read at the end at
            // once, empty, and the client re-asks; the ceiling crosses the
            // client's deadline, where a tail wait the client stops waiting
            // for is an ambiguous read, never a wrong one (#185, #241).
            max_wait_ms: buggify_knob!(400_u64, 0_u64..2001_u64),
            // Floor 0: no minimum. The extreme raises a client's short wait
            // well past it, still inside the client's read deadline (its
            // floor is 1 s), and a crossed maximum caps it (#241).
            min_wait_ms: buggify_knob!(0_u64, 0_u64..301_u64),
            // Floor 1: a one-record page still moves every reader. The
            // ceiling is the default, the cap the linearizability model
            // knows (`MAX_READ_RECORDS`), so a shorter page than the client
            // asked is the server's limit, never a lost record (#241).
            max_read_records: buggify_knob!(
                paros::MAX_READ_RECORDS,
                1_u64..paros::MAX_READ_RECORDS + 1
            ),
            // Floor 1: a page that can hold a record always holds one, so a
            // tiny budget serves one record per page (#241).
            max_read_bytes: buggify_knob!(64 * 1024_u64, 1_u64..65_537_u64),
            // Floor 1: the client inboxes are the RPC runtime's per-endpoint
            // queues, which refuse a request beyond capacity as `Overloaded`
            // (never admitted, so never a lost *executed* request); the loop
            // takes each request as soon as it runs, so a one-slot queue
            // serialises clients and a refused client retries on its own
            // cadence — slower, ambiguous at worst, never wrong.
            client_inbox_capacity: buggify_knob!(256_usize, 1_usize..17_usize),
            // Floor 1: the peer-delivery edge and the matchmaker reply sinks
            // `send().await` into this inbox, so a full inbox stalls the
            // delivering peer's RPC until the loop takes one message (one
            // message per loop iteration is throttling, not a drop); a batch
            // that stalls past `delivery_timeout` is written off and
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
            // The recovery page size (#330). Floor 1: a one-slot page still
            // drains the recovery, one `Ready` per slot. The ceiling is the
            // core's constant, the default. A small page makes a leader's
            // recovery span several pages, the window `advance_recovery`
            // paces; the campaign rarely opens 64 rounds at once.
            recovery_page: buggify_knob!(
                paros::LEADER_RECOVERY_BATCH,
                1_usize..paros::LEADER_RECOVERY_BATCH + 1
            ),
            // The page sizes the core's 64-entry ceilings bound (#338). The
            // campaign rarely fills a 64-entry page, so each extreme is a
            // small page (floor 1), which makes the paging paths run: a
            // Phase 1 over several promise pages, a re-send cursor that
            // wraps, an apply walk that yields mid-burst, a matchmaker
            // history over several pages. Floor 1 for each: a one-entry page
            // still makes progress, one round trip or one `Ready` per entry.
            // A one-entry promise page makes a long suffix take more round
            // trips than an election timeout; it stays winnable because a
            // paging candidate re-sends its `Prepare` to the peers that
            // answered on every tick, which keeps their election clocks
            // quiet (#428 (paging livelock)).
            promise_page: buggify_knob!(paros::PROMISE_BATCH, 1_usize..5_usize),
            resend_page: buggify_knob!(paros::RESEND_BATCH, 1_usize..5_usize),
            apply_page: buggify_knob!(paros::APPLY_BATCH, 1_usize..5_usize),
            registry_page: buggify_knob!(paros::REGISTRY_PAGE, 1_usize..5_usize),
            // The delegated round's take-back budget (#142), in
            // re-delegations (one per beat). Floor 1: a round taken back
            // after a single re-delegation runs colocated while its proxy
            // may still decide it, and the two verdicts must agree
            // (P2b-idempotent fan-outs) — the whole point of pushing it.
            // The ceiling stretches a dead proxy's cost per slot; the
            // recovery tail still outlasts it.
            proxy_take_back_resends: buggify_knob!(10_u64, 1_u64..41_u64),
            // The proxy's retention budget (#142), in unanswered re-fan-outs
            // (one per beat), drawn independently of the take-back so a
            // seed can evict before the leader takes back or long after.
            // Floor 1: a round evicted after one unanswered beat decides
            // nothing and is reopened by the leader's next re-delegation,
            // so the cost is traffic, never safety or liveness (the
            // take-back is the liveness). The ceiling stretches how long a
            // round for a compacted slot is re-fanned-out; the tail
            // outlasts it.
            proxy_round_resends: buggify_knob!(20_u64, 1_u64..81_u64),
            // A quarantined journal's re-open delay (#188), in ticks. Floor
            // 1: a journal re-opened the next beat is a restart loop that
            // still leaves the node's other journals their beats; the
            // ceiling holds one journal down on one node for at most
            // `QUARANTINE_CEILING_MS` (production's 8 s), which the
            // recovery tail outlasts. The ceiling is wall-clock: at a slow
            // tick, 160 ticks held a one-copy registry down past
            // `FLEET_SETTLE` (witness: seed 15408472114085943299, a 32 s
            // quarantine).
            quarantine_ticks: buggify_knob!(40_u64, 1_u64..161_u64)
                .min(QUARANTINE_CEILING_MS.div_ceil(tick_ms)),
            // The election backoff's ceiling (doublings of the base across
            // consecutive failed campaigns). Floor 2: below it a sole
            // candidate over a degraded link can still abandon every round
            // before its slowest promise returns; the ceiling only slows a
            // leaderless cluster's re-election.
            election_backoff_doublings: buggify_knob!(3_u32, 2_u32..7_u32),
            // The batch limits refused at the edge (#241, §2.7). Floor 1
            // record: a one-record write always fits, so the workload's
            // single-record writes and every system, fleet and checkpoint
            // write (one record each) still make progress; the extreme
            // refuses every multi-record batch.
            max_batch_records: buggify_knob!(1024_u64, 1_u64..9_u64),
            // Floor 16 KiB: one record of the workload's largest command
            // (`MAX_LARGE_COMMAND_BYTES`) and every inline checkpoint the
            // runs build still fit; the extreme refuses a batch of several
            // large commands.
            max_batch_bytes: buggify_knob!(1_u64 << 20, 16_384_u64..131_073_u64),
            // The cell election (#240). The renewal period's floor is 100 ms
            // (a renewal a few round trips apart); the lease outlasts it by
            // at least two round trips (a renewal written, then read), its
            // documented floor, so a live coordinator keeps the lease in the
            // recovery tail. Shorter is a partition, not a knob.
            election_renew: ms(election_renew_ms),
            election_lease: ms(
                election_renew_ms + buggify_knob!(1500_u64, 2 * ROUND_TRIP_FLOOR_MS..4001_u64)
            ),
            // Floor 1: the coordinator truncates at every renewal.
            election_compact_after: buggify_knob!(16_u64, 1_u64..65_u64),
            // The cell coordinator's failure detector (#211). Floor: one
            // renewal period and a round trip past it, so one missed probe
            // never marks a machine down. The range crosses a machine's
            // reboot in both directions: a short one marks a rebooting
            // machine down, then up again.
            machine_down_after: ms(
                election_renew_ms + buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)
            ),
            // A machine's busyness window (#424). Floor 1 s (the shipped
            // floor): shorter measures the sampler, not the machine. The
            // short end gives a run many windows, so `LOAD` meets a full
            // one early and often.
            load_interval: ms(buggify_knob!(5000_u64, 1000_u64..5001_u64)),
        };
        // The production profile `parosd` ships (#209), whole: every field
        // at once, which the per-field locations above would draw together
        // only by chance. Each of its values lies inside its knob's range
        // (a 100 ms tick, a ten-tick election base, two-second reads), so
        // this is a point of the swept space, not a new extreme, and it
        // clears the round-trip floor above in wall-clock terms.
        // The per-field pairings below judge `drawn`, and only when it is
        // what runs: under the production profile no drawn extreme does.
        let production = !slow_link && buggify_knob!(0_u8, 1_u8..2_u8) == 1;
        let tunables = if production {
            assert_reachable!("a node runs the production driver tunables");
            DriverTunables::production()
        } else {
            drawn
        };
        if !production {
            pair_extremes(&drawn, &defaults);
        }
        Self {
            tunables,
            wipe_pct: buggify_knob!(DEFAULT_LOSS_PCT, MIN_LOSS_PCT..MAX_LOSS_PCT + 1),
            matchmaker_loss_pct: buggify_knob!(DEFAULT_LOSS_PCT, MIN_LOSS_PCT..MAX_LOSS_PCT + 1),
            config_edit_pct: buggify_knob!(DEFAULT_CONFIG_EDIT_PCT, 25..MAX_LOSS_PCT + 1),
        }
    }
}

/// The BUGGIFY pairings of [`NodeShape::draw`]'s knobs: each extreme the
/// node genuinely runs (never under the production profile, which runs none
/// of them) reaches its gate.
fn pair_extremes(drawn: &DriverTunables, defaults: &DriverTunables) {
    if drawn.peer_queue_capacity != defaults.peer_queue_capacity {
        // BUGGIFY pairing: the capacity extreme genuinely runs.
        assert_reachable!("a node runs with an extreme peer-queue capacity");
    }
    if drawn.delivery_batch != defaults.delivery_batch {
        // BUGGIFY pairing: the delivery-batch extreme genuinely runs.
        assert_reachable!("a node runs with an extreme delivery batch");
    }
    if drawn.election_backoff_doublings != 3 {
        // BUGGIFY pairing: the election backoff extreme genuinely runs.
        assert_reachable!("a node runs with an extreme election backoff ceiling");
    }
    if drawn.gc_resend_ticks != 5 {
        // BUGGIFY pairing: the GC cadence extreme genuinely runs.
        assert_reachable!("a node runs with an extreme GC re-send cadence");
    }
    if drawn.reconfigurer_resend_ticks != 5 {
        // BUGGIFY pairing: the handover cadence extreme genuinely runs.
        assert_reachable!("a node runs with an extreme handover re-send cadence");
    }
    if drawn.reconfigure_timeout_elections != 4 {
        // BUGGIFY pairing: the handover stall budget extreme genuinely runs.
        assert_reachable!("a node runs with an extreme handover stall budget");
    }
    if drawn.recovery_page != paros::LEADER_RECOVERY_BATCH {
        // BUGGIFY pairing: the recovery page extreme genuinely runs.
        assert_reachable!("a node runs with a small recovery page");
    }
    if drawn.promise_page != paros::PROMISE_BATCH {
        // BUGGIFY pairing: the promise page extreme genuinely runs.
        assert_reachable!("a node runs with a small promise page");
    }
    if drawn.resend_page != paros::RESEND_BATCH {
        // BUGGIFY pairing: the re-send page extreme genuinely runs.
        assert_reachable!("a node runs with a small re-send page");
    }
    if drawn.apply_page != paros::APPLY_BATCH {
        // BUGGIFY pairing: the apply page extreme genuinely runs.
        assert_reachable!("a node runs with a small apply page");
    }
    if drawn.registry_page != paros::REGISTRY_PAGE {
        // BUGGIFY pairing: the registry page extreme genuinely runs.
        assert_reachable!("a node runs with a small registry page");
    }
    if drawn.reconfigure_backoff_max_ticks != 10 {
        // BUGGIFY pairing: the decree backoff extreme genuinely runs.
        assert_reachable!("a node runs with an extreme decree backoff ceiling");
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
    /// moonpool attrition restart, or the chain client's scripted restart.
    /// Seam-crash restarts never leave `run()` and so never come
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
    /// Run-level: whether the run draws the stalled-proxy scenario (see
    /// [`stalled_proxy`]), fixed by the first caller.
    stalled_proxy: Option<bool>,
    /// Run-level: whether the run draws the moved-founder scenario (see
    /// [`moved_founder`]), fixed by the first caller.
    moved_founder: Option<bool>,
    /// Run-level: whether the run draws the replaced-founder scenario (see
    /// [`replaced_founder`]), fixed by the first caller.
    replaced_founder: Option<bool>,
    /// Run-level: whether the run draws the slow-link scenario (see
    /// [`slow_link`]), fixed by the first caller.
    slow_link: Option<bool>,
    /// Run-level: whether the run draws the lagging-acceptor scenario (see
    /// [`lagging_acceptor`]), fixed by the first caller.
    lagging_acceptor: Option<bool>,
    /// Run-level: whether the run draws the split-floor scenario (see
    /// [`split_floor`]), fixed by the first caller.
    split_floor: Option<bool>,
    /// Run-level: whether the run runs the system journals (see
    /// [`system_journals`]), fixed by the first caller.
    system: Option<bool>,
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

/// What a joiner registers as (#211): its class and its capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JoinerMachine {
    /// Its class.
    pub(crate) class: paros::system::Class,
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

/// Every identifier the run names (`docs/architecture.md` §3.8: no identifier is
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
fn draw_id() -> u64 {
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
        JournalStoreConfig {
            durability: if batched {
                paros::journal::Durability::Batched
            } else {
                paros::journal::Durability::Ordered
            },
            geometry: journal_geometry(),
            ..JournalStoreConfig::small()
        }
    })
}

/// Whether the run's nodes withhold every GC request for the chaos window
/// (`paros::scenario::WITHHOLD_GC`, #263): drawn once per seed, its own
/// BUGGIFY location, which decides the driver's named location. A leader that collects nothing keeps every prior
/// configuration answerable, which is what makes a straggler a
/// reconfiguration removed still worth waiting for: the departed-straggler
/// shape. Rare-but-valid: GC is liveness of space,
/// never of the log, and it resumes in the recovery tail.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn withhold_gc(state: &StateHandle) -> bool {
    let scenario = departed_straggler(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.withhold_gc.get_or_insert_with(|| {
        // Its fired gate sits where the answer has an effect (the driver's
        // named location), not at the draw.
        let withhold = scenario || moonpool_sim::buggify_with_prob!(0.5);
        moonpool_sim::set_activation(paros::scenario::WITHHOLD_GC, withhold);
        withhold
    })
}

/// Whether the run draws the **departed-straggler scenario** (#263): drawn
/// once per seed, its own BUGGIFY location, it turns on together every
/// ingredient of the rarest storage shape, so the sweep reaches it by
/// design instead of by the product of independent coins. The nodes
/// withhold GC ([`withhold_gc`]), the acceptors bootstrap at the floor on
/// a matchmaker seed, leaving every other node a spare
/// ([`bootstrap_ranks`]), the main journal's owner rotates the set onto
/// them right after its claim (`ChainConfig::reconfigure_after_claim`),
/// and a correlated outage that lands loses the most recent slot most of
/// the successor never held down to one clean copy on a member the
/// rotation superseded, that member back last (`crate::world::outage`,
/// `crate::world::late_outage`): the newest configuration alone would
/// then decide the slot with a no-op, so only the cross-configuration
/// Phase 1 keeps it (#267). Each ingredient keeps its own coin on the
/// other seeds. Its fired gate sits where the outage's loss takes the
/// shape. Rare-but-valid: each ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn departed_straggler(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .departed_straggler
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **reused-name scenario** (#239, #192 (the
/// frontend)): drawn once per seed, its own BUGGIFY location. A name is
/// reused only after a create, a completed delete and a second create all
/// name it, three steps that each draw one of three names: the gate fired
/// on 9 of 155 checks (1,000 hunt seeds) and missed CI's sweep once. On a
/// scenario seed, every journal create and delete names the same journal.
/// Each step keeps its own name draw on the other seeds. Rare-but-valid:
/// a tenant may delete a name and create it again.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn reused_name(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .reused_name
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **bare-quorum scenario** (#270): drawn once
/// per seed, its own BUGGIFY location, never on a departed-straggler seed.
/// The bare quorum needs a slot decided on a quorum short of a member, an
/// outage landing while that member still lacks it, and every copy lost:
/// three coins whose product fired the gate on 3 of 2,094 checks (1,000
/// hunt seeds; 0 on `main`); with the scenario, 42 of 2,898 (1,400). On
/// a scenario seed, `crate::world::bare_outage` strikes the moment the
/// custody ledger holds such a slot and plans the bare loss on it, so the
/// Phase-1 tally reads `faulty, faulty, none` and the no-op fill must be
/// refused. Each ingredient keeps its own coin on the other seeds.
/// Rare-but-valid: each ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn bare_quorum(state: &StateHandle) -> bool {
    let straggler = departed_straggler(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .bare_quorum
        .get_or_insert_with(|| !straggler && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **lost-verdict scenario** (#270): drawn once
/// per seed, its own BUGGIFY location. A write's verdict is lost and its
/// retry answered from the log (`Duplicate`, #204) only when a write reply
/// is dropped at the reply seam *and* the owner re-sends the identical
/// write before any read-back, two locations whose product fired the gate
/// on 9 of 2,929 checks (1,400 hunt seeds on `main`). On a scenario seed
/// every node drops write replies at the location's rate
/// (`paros::scenario::LOSE_VERDICTS`) and every ambiguous write is re-sent
/// at once (`ChainWorkload`). Each ingredient keeps its own coin on the
/// other seeds. Rare-but-valid: each ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn lost_verdict(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.lost_verdict.get_or_insert_with(|| {
        let lose = moonpool_sim::buggify_with_prob!(1.0);
        moonpool_sim::set_activation(paros::scenario::LOSE_VERDICTS, lose);
        lose
    })
}

/// Whether the run draws the **lagging-fold scenario** (#189): drawn once
/// per seed, its own BUGGIFY location. A node registered at runtime is
/// refused by a member whose registry fold has not admitted it yet only
/// when it speaks before that fold catches up: a joiner reconfigured into
/// the default journal and a member's lagging follow read. Without the
/// scenario the gate fired on about 0.1% of seeds (2,000 hunt seeds, after
/// #210 (tenant control journal) removed the directory's create that named
/// an unregistered joiner). On a scenario seed every node opens its follow
/// reads late (`paros::scenario::LAG_FOLLOW`), the seed runs the system
/// journals ([`system_journals`]), and a client that registers a joiner
/// reconfigures onto it next whatever the swarm mask (`ChainWorkload`):
/// the gate fired on 0 of 417 checks (600 hunt seeds) before these
/// ingredients came together and 7 of 637 (600) after. Those were checks:
/// the gate fired on 3 to 4 seeds in 1,000, too few for the sweep's 1,024
/// to saturate. A refusal needs a member whose fold is behind a joiner
/// that speaks to it, and the joiner speaks once a configuration takes it
/// in. So on a scenario seed a client also registers at the ceiling weight
/// whatever the swarm mask, asks again a reconfiguration onto the joiner
/// that no leader took (`NotLeader`, `UnknownMember`), and reboots every
/// member of one that started, so each member's fold restarts behind the
/// joiner (`ChainWorkload`). The gate then fired on 35 of 3,000 hunt seeds
/// (51 of 2,126 checks on 1,800), from 5 of 1,600 (12 of 1,859) before.
/// Most seeds stay out of reach: a joiner joins the default journal only
/// with matchmakers, no proxy and no replica. Rare-but-valid: a slow
/// follower is, and each ingredient keeps its own coin on the other
/// seeds.
///
/// The scenario also turns on `paros::scenario::ADMIT_LATE` (#387
/// (unpooled joiner in a configuration)): half the nodes, drawn at boot,
/// admit the registry's pool 2 s after their fold owes it. A leader that
/// admitted a joiner then sends a configuration naming it to members whose
/// pool lacks it, and they must ignore it. The gate "learning: a
/// configuration outside the pool is ignored" fired 6 times in 1,000 hunt
/// seeds before and 30 times in 2,000 after.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn lagging_fold(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.lagging_fold.get_or_insert_with(|| {
        let lag = moonpool_sim::buggify_with_prob!(1.0);
        moonpool_sim::set_activation(paros::scenario::LAG_FOLLOW, lag);
        moonpool_sim::set_activation(paros::scenario::ADMIT_LATE, lag);
        lag
    })
}

/// Whether the run draws the **wiped-founder scenario** (#246): drawn once
/// per seed, its own BUGGIFY location. A founding member wiped during
/// `init` needs `init` inside the chaos window, the machines' attrition
/// on, its wipe weight on, and a reboot that lands on a founder between
/// two steps of the cell decree. `init` mostly starts in the recovery tail,
/// so the product almost never lines up: 2 wipes during `init` in 500 hunt
/// seeds without the scenario; 37 with it.
/// On a scenario
/// seed, client 0 runs `init` first (`crate::chain_workload`), and
/// `crate::world::wiped_founder` wipes a founder through moonpool's
/// `CrashAndWipe` at one of two moments: once every founder promised and
/// none voted, or once a founder voted and another did not. The founders
/// that kept their disks choose the plan while they are a majority; else
/// `cell init` refuses `cell_lost`. Each
/// ingredient keeps its own coin on the other seeds. Rare-but-valid: each
/// ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn wiped_founder(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .wiped_founder
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **moved-founder scenario** (#211): drawn once
/// per seed, its own BUGGIFY location. A machine boots from its durable
/// cached registry fold only after another machine of its cell moved, the
/// cache followed the move, and the machine restarted: the machines'
/// attrition and the rename knob lined that up 0 times in 300 hunt seeds.
/// On a scenario seed the machines advertise names, and
/// `crate::world::moved_founder` crashes a founding member that comes back
/// under a new name, then a machine
/// whose cache names the founder's new name. Rare-but-valid: a machine that
/// comes back under a new name is (#349).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn moved_founder(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .moved_founder
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **replaced-founder scenario** (#423): drawn
/// once per seed, its own BUGGIFY location, never on a wiped-founder or a
/// moved-founder seed. A re-run `init` meets a machine `cell add-machine`
/// admitted at a founder's address only when a founder of a cell of three
/// or more is wiped after formation, an operator admits the machine that
/// replaced it, and an operator that knows the cell runs `init` again: a
/// 2,000-seed hunt never lined the three up. On a scenario seed the layout
/// lists three founders or more where the machines allow it
/// ([`machine_layout`]), client 0 runs `init` first (`crate::chain_workload`),
/// `crate::world::replaced_founder` wipes a founder once every founder
/// formed, and client 0 admits the replacement, then runs `init` again
/// (`crate::chain_workload::fleet`). Each ingredient keeps its own coin on
/// the other seeds. Rare-but-valid: each ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn replaced_founder(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenarios take the registry's lock too.
    let other = wiped_founder(state) || moved_founder(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .replaced_founder
        .get_or_insert_with(|| !other && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **silent-machine scenario** (#211): drawn once
/// per seed, its own BUGGIFY location. The cell coordinator marks a machine
/// down only when it is silent past `machine_down_after`, while a
/// coordinator's term is served, then up when it answers again. That needs
/// a machine of a formed cell to die during the run and stay down longer
/// than a usual reboot, which the machines' attrition almost never lines up
/// (1 machine marked down, and none back up, in 1,000 hunt seeds without
/// the scenario). On a scenario seed `crate::world::silent_machine` crashes
/// one machine of the cell once it formed, and holds it down past every
/// `machine_down_after` the knob draws. Rare-but-valid: a machine down for
/// a few seconds is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn silent_machine(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .silent_machine
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **stalled-proxy scenario** (#341): drawn once
/// per seed, its own BUGGIFY location. Three mutation survivors of #269
/// need a proxy whose rounds stay open: a leader that takes a delegated
/// round back on a grid (the re-send must stay on the round's column), a
/// proxy that evicts a round nobody answers, and a new leadership whose
/// delegation meets the rounds a proxy still holds. A proxy's round closes
/// within a few beats on almost every seed, and a grid is two knobs deep,
/// so no seed in `1..=300` lined them up. On a scenario seed every proxy
/// drops the `Accepted`s and `Nack`s it hears for the chaos window
/// (`paros::scenario::STALL_PROXY`), a leader that holds delegated rounds
/// resigns now and then (`paros::scenario::RESIGN_DELEGATING`), and the run
/// runs an acceptor grid
/// where the pool tiles one ([`quorum_policy`]). Each ingredient keeps its
/// own coin on the other seeds. Rare-but-valid: a lost vote is, and so is a
/// grid.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn stalled_proxy(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.stalled_proxy.get_or_insert_with(|| {
        let stall = moonpool_sim::buggify_with_prob!(1.0);
        moonpool_sim::set_activation(paros::scenario::STALL_PROXY, stall);
        moonpool_sim::set_activation(paros::scenario::RESIGN_DELEGATING, stall);
        stall
    })
}

/// Whether the run draws the **slow-link scenario** (#386): drawn once per
/// seed, its own BUGGIFY location. A peer link starves quorum reads only
/// when its beat load fills its delivery batches: a fast tick, the floor
/// batch, several journals on the link, and a read quorum that waits on
/// the slowest link. Four knobs deep, the shape fired on 1 of 11,000 hunt
/// seeds. On a scenario seed every node runs a 10 to 17 ms tick and a
/// 24-message batch, never the production profile ([`NodeShape::draw`]),
/// the run serves three journals ([`journals`]) and the system journals
/// ([`system_journals`]), and a flexible split reads from every acceptor
/// (`q2 = 1`, `q1 = n`, [`quorum_policy`]). Each ingredient keeps its own
/// coin on the other seeds. Rare-but-valid: each ingredient is a knob
/// extreme.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn slow_link(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.slow_link.get_or_insert_with(|| {
        let slow = moonpool_sim::buggify_with_prob!(0.3);
        if slow {
            assert_reachable!("a run draws the slow-link scenario");
        }
        slow
    })
}

/// Whether the run draws the **lagging-acceptor scenario** (#340): drawn
/// once per seed, its own BUGGIFY location, never on a departed-straggler
/// or bare-quorum seed (their outages need every acceptor up when they
/// strike). A trim-point jump (`Message::TrimmedTo`) needs a node behind
/// its peers' floors: a node down while the others truncate past what it
/// holds. The journal truncates little in the chaos window and the
/// attrition brings a node back fast: the mutation hunt's 300 seeds met 4
/// jumps. On a scenario seed `crate::world::lagging_acceptor` holds one
/// acceptor down until a peer's floor passes its chosen prefix, and every
/// client compacts at every truncation step (`ChainWorkload`): 7 jumps in
/// the same 300 seeds (35 scenario seeds). The node comes back below the
/// floor and jumps, its allocator frontier far below the point. Each
/// ingredient keeps its own coin on the other seeds. Rare-but-valid: a
/// slow node is, and so is a client that compacts often.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn lagging_acceptor(state: &StateHandle) -> bool {
    let straggler = departed_straggler(state);
    let bare = bare_quorum(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .lagging_acceptor
        .get_or_insert_with(|| !straggler && !bare && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **split-floor scenario** (#409 (repair probe
/// blocked below a trim point)): drawn once per seed, its own BUGGIFY
/// location, never on a departed-straggler, bare-quorum or
/// lagging-acceptor seed (each of those owns the run's outage or its held
/// acceptor). A leader jumps with its repair probe open only when one
/// acceptor's floor passed a slot its peers still hold, every peer's copy
/// of that slot is lost, and the ahead acceptor is out of the winning
/// quorum: 0 such jumps in 900 hunt seeds without it. On a scenario seed
/// `crate::world::split_floor` strikes the outage at that split and holds
/// the ahead acceptor down last, and every client compacts at every
/// truncation step (`ChainWorkload`). Each ingredient keeps its own coin on
/// the other seeds. Rare-but-valid: a correlated outage is, and so is a
/// client that compacts often.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn split_floor(state: &StateHandle) -> bool {
    let straggler = departed_straggler(state);
    let bare = bare_quorum(state);
    let lagging = lagging_acceptor(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.split_floor.get_or_insert_with(|| {
        let split = !straggler && !bare && !lagging && moonpool_sim::buggify_with_prob!(1.0);
        if split {
            assert_reachable!("a run draws the split-floor scenario");
        }
        split
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
    (ENTRY_BLOCKS_FLOOR as u64 - 1) * paros::journal::BLOCK as u64
        >= crate::chain_workload::MAX_BATCH_RECORDS
            * crate::chain_workload::MAX_LARGE_COMMAND_BYTES as u64
);
const _: () = assert!(PERSIST_BLOCKS_FLOOR >= 1);

/// The journal stores' segment geometry (#176, folded from #202): a
/// `buggify_knob!` per region, so segment rollover, prefix deletion and the
/// batch-split path run under varied shapes. The default is the library's
/// `Geometry::small()` persist log (512 records) with the entry log at its
/// floor; the extremes shrink the persist log to [`PERSIST_BLOCKS_FLOOR`]
/// or grow either region. The gap stays empty.
fn journal_geometry() -> paros::journal::Geometry {
    let small = paros::journal::Geometry::small();
    let geometry = paros::journal::Geometry {
        persist_blocks: buggify_knob!(small.persist_blocks, PERSIST_BLOCKS_FLOOR..33_u32),
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

/// How many genesis nodes host the system journals (#189) — the seeds,
/// the lowest ranks of the pool: one. A one-member plain journal sends
/// nothing to anyone, and the system journals' beats among three seeds are
/// load a small cluster cannot always carry: two more journals on every link
/// livelocked a matchmaker deployment's two-round-trip campaigns (witness
/// 17972338006788767545 on this branch: 3 nodes, 240 campaigns) and, with
/// one node parked, the two survivors of a plain 3-node pool (witness
/// 16999966542771974935: 267 dueling rounds, nothing chosen) — the load
/// [`journals`] already keeps off those seeds. Not a tunable: its extreme is
/// a run that cannot win. The driver's `SystemPlan` takes any seed list; a
/// one-member journal's only liveness cost is its one seed's downtime.
pub(crate) const SEED_COUNT: usize = 1;

/// The seeds of a pool of `pool` nodes (#189): its [`SEED_COUNT`] lowest
/// ranks.
pub(crate) fn seed_ranks(pool: usize) -> Vec<u64> {
    (0..pool.min(SEED_COUNT) as u64).collect()
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
/// in `chain_workload/fleet.rs`, not by this draw.
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
                        paros::system::Class::Stateless
                    } else {
                        paros::system::Class::Storage
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
    /// Its failure domain.
    pub(crate) failure_domain: String,
}

/// The run's machines (#246): the layout an operator gives a fleet before
/// `init`, drawn per seed so every run forms another cell. Forming it is the
/// run's own business (decided on 2026-10-09): nothing here is a cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MachineLayout {
    /// How many machines, the lowest ranks, are the founding members:
    /// `cell init` lists them, and forms the cell over them (#277).
    pub(crate) founders: usize,
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
}

/// The run's machine layout (#246), drawn once per seed by whoever asks
/// first, for `count` machines in rank order. The founder count is uniform
/// over `1..=count` (`3..=count` on a replaced-founder seed with three
/// machines or more, [`replaced_founder`]); every founder is a `storage` machine (`cell init`
/// refuses a `stateless` member) and every other machine is `storage` or
/// `stateless` on a coin, idle, waiting for a placement that is #212's. The
/// capacity is one `buggify_knob!` for the run (default 2, extreme 1..=4;
/// floor 1, as a joiner's). A formed cell serves its control journals and
/// whatever its tenants create (#210). The machines advertise names on a
/// coin (#257); on such a seed a reboot lands at a new address with the
/// `move_pct` knob (default 0; extreme 20..=80, floor 0: a machine that
/// never moves), and a cell machine's reboot comes back under a new name
/// with the `rename_pct` knob (#349, default 0; extreme 20..=60, floor 0: a
/// machine that keeps its name).
#[tracing::instrument(level = "debug", skip(state))]
pub(crate) fn machine_layout(state: &StateHandle, count: usize) -> MachineLayout {
    // Drawn before the lock: the scenario takes the registry's lock too.
    let moved = moved_founder(state);
    let replaced = replaced_founder(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .machine_layout
        .get_or_insert_with(|| {
            // The replaced-founder scenario needs a cell that keeps a
            // majority through one wipe: three founders or more.
            let least = if replaced && count >= 3 { 3 } else { 1 };
            let founders = if count == 0 {
                0
            } else {
                moonpool_sim::sim_random_range(least..count + 1)
            };
            let capacity = buggify_knob!(2_u64, 1_u64..5_u64);
            let named = moonpool_sim::sim_random_bool(0.5) || moved;
            let move_pct = buggify_knob!(0_u32, 20_u32..81_u32);
            let rename_pct = buggify_knob!(0_u32, 20_u32..61_u32);
            let machines = (0..count)
                .map(|rank| {
                    let class = if rank >= founders && moonpool_sim::sim_random_bool(0.5) {
                        assert_reachable!("machine: a machine outside the seeds is stateless");
                        paros::machine::Class::Stateless
                    } else {
                        paros::machine::Class::Storage
                    };
                    MachineDraw {
                        class,
                        capacity,
                        failure_domain: format!(
                            "zone-{}",
                            moonpool_sim::sim_random_range(0_u32..3_u32)
                        ),
                    }
                })
                .collect();
            assert_always!(
                count == 0 || (founders >= 1 && founders <= count),
                "machine: a layout's founding members are machines of it",
                { "count" => count, "founders" => founders }
            );
            MachineLayout {
                founders,
                machines,
                named,
                move_pct,
                rename_pct,
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
/// journal over the whole pool (`crate::process`: the matchmaker plane, the
/// proxy leaders and the replica tier serve one journal each). On a
/// multi-journal seed a second location draws whether the driver's named
/// location holds one journal on every node for the chaos window
/// (`paros::scenario::HOLD_JOURNAL`): its siblings must keep committing.
/// Each node holds the highest user journal it boots serving: the same one
/// on every acceptor, possibly a lower one on a joiner that serves fewer.
/// The driver reports each hold (`Audit::journal_held`), so the journal
/// board knows every held journal. A single-journal seed holds none.
#[tracing::instrument(level = "debug", skip(state))]
pub(crate) fn journals(state: &StateHandle) -> JournalPlan {
    let main = identifiers(state).main;
    let slow = slow_link(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .journals
        .get_or_insert_with(|| {
            let count = buggify_knob!(1_u64, 2_u64..4_u64);
            // A slow-link seed loads every link with the most journals.
            let count = if slow { 3 } else { count };
            // The first journal is the deployment's ([`Identifiers::main`]);
            // every other one's identifier is drawn (#235): a random journal id,
            // in the main journal's tenant or a random one — and, in another
            // tenant, sometimes the very journal id of the first, so the
            // demux is proven to key on both halves of the identifier. No identifier is
            // fixed (§3.8).
            let mut ids = vec![main];
            while ids.len() < usize::try_from(count).unwrap_or(1) {
                let tenant = if moonpool_sim::sim_random_bool(0.5) {
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
                moonpool_sim::set_activation(paros::scenario::HOLD_JOURNAL, false);
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
                paros::scenario::HOLD_JOURNAL,
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

fn registry(state: &StateHandle) -> Arc<Mutex<Registry>> {
    crate::state::published(state, SHAPE_KEY, Registry::default)
}

/// Boot `ip` once more: hand back the shape its first incarnation drew, drawing
/// it now if this *is* the first incarnation.
#[tracing::instrument(level = "debug", skip(state), fields(ip = %ip))]
pub(crate) fn boot(state: &StateHandle, ip: &str) -> Incarnation {
    let slow = slow_link(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    let entry = guard.nodes.entry(ip.to_string()).or_insert_with(|| Entry {
        shape: NodeShape::draw(slow),
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

/// The run's **quorum-system policy** (#140, #141), drawn once per seed by
/// whichever process or workload asks first and handed back unchanged to
/// every later caller (an attrition restart must boot the same node under
/// the same system, and every client must compose successors under it).
///
/// The default is the majority, the plain deployment's system. A seed
/// may instead draw a **flexible split**: one `buggify_knob!` location
/// for `q2` over the pool (default the majority, extreme `1..=pool/2`), with
/// `q1 = n - q2 + 1` derived per configuration ([`QuorumPolicy::system`]);
/// or, on a pool whose size tiles a grid, an **acceptor grid**: its own
/// `buggify_knob!` location over the layouts of the pool ([`grid_layouts`],
/// floor `rows >= 2` and `cols >= 2`; default no grid), so a seed can be
/// extreme in one system and never the other. Both are opt-in configuration
/// data on the core side, so a majority seed is byte-identical to a run
/// before the policy existed.
#[tracing::instrument(level = "debug", skip(state), fields(pool))]
pub(crate) fn quorum_policy(state: &StateHandle, pool: usize) -> QuorumPolicy {
    let stalled = stalled_proxy(state);
    let slow = slow_link(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.quorum.get_or_insert_with(|| {
        let majority = pool / 2 + 1;
        if pool < 2 {
            return QuorumPolicy::Majority;
        }
        // The stalled-proxy scenario (#341) runs a grid wherever the pool
        // tiles one, so its take-back runs on a column.
        let layouts = grid_layouts(pool);
        if stalled && !layouts.is_empty() {
            let pick = moonpool_sim::sim_random_range(0..layouts.len());
            let (rows, cols) = layouts[pick];
            assert_reachable!("a run draws an acceptor grid");
            return QuorumPolicy::Grid { rows, cols };
        }
        let q2 = buggify_knob!(majority, 1_usize..(pool / 2 + 1));
        // A slow-link seed reads from every acceptor: `q1 = n` waits on the
        // slowest link (#386).
        let q2 = if slow { 1 } else { q2 };
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
/// Multi-Paxos deployment and the shape of every existing axis. A seed that
/// deploys matchmakers may instead draw a **subset** (a run with
/// `has_matchmakers == false` never draws — a plain deployment's membership
/// must include every node, per `paros::Config::peers`), leaving the other
/// nodes as *spares*: addressable pool members outside every configuration
/// until a `Reconfigure` pulls them in. Two knob locations, each its own
/// per-seed activation: the subset *size* (floor [`MIN_BOOTSTRAP`], ceiling
/// the pool) and the *rotation* that decides which ranks are the spares, so
/// a seed can bootstrap on `{2, 3, 4}` of a five-node pool and leave
/// `{0, 1}` — the lowest ranks, the ones every "first node" heuristic would
/// pick — outside.
#[tracing::instrument(level = "debug", skip(state), fields(pool, has_matchmakers))]
pub(crate) fn bootstrap_ranks(state: &StateHandle, pool: usize, has_matchmakers: bool) -> Vec<u64> {
    let scenario = departed_straggler(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .bootstrap
        .get_or_insert_with(|| {
            let all: Vec<u64> = (0..pool)
                .map(|i| u64::try_from(i).unwrap_or(u64::MAX))
                .collect();
            if !(has_matchmakers && pool > MIN_BOOTSTRAP) {
                return all;
            }
            // The departed-straggler scenario bootstraps at the floor,
            // leaving every other node a spare: the owner's removal right
            // after its claim is a rotation onto them, as whole as they
            // allow, so the successor is mostly members that never held
            // a departed member's slot (#267).
            let size = if scenario {
                MIN_BOOTSTRAP
            } else {
                buggify_knob!(pool, MIN_BOOTSTRAP..pool)
            };
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

/// The run's **bootstrap matchmaker ranks** (#125): generation 0's set, drawn
/// once per seed by whichever process or workload asks first. The default is
/// the whole matchmaker pool; a seed with two or more matchmakers
/// may draw a **subset** (any size from one up — a one- or two-member set is
/// a valid registry that tolerates no loss), leaving the rest as matchmaker
/// *spares* a `ReconfigureMatchmakers` pulls in. Two knob locations, as for
/// the acceptors: the subset size and the rotation that picks the spares.
#[tracing::instrument(level = "debug", skip(state), fields(pool))]
pub(crate) fn matchmaker_bootstrap_ranks(state: &StateHandle, pool: usize) -> Vec<u64> {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard
        .matchmaker_bootstrap
        .get_or_insert_with(|| {
            let all: Vec<u64> = (0..pool)
                .map(|i| u64::try_from(i).unwrap_or(u64::MAX))
                .collect();
            if pool < 2 {
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
/// matchmaker from ([`StorageWorld::wipe_matchmaker`]) and the ceiling of
/// [`matchmaker_floor`]. Not a tunable: it is what the matchmaker-loss budget
/// is computed over.
///
/// [`StorageWorld::wipe_matchmaker`]: crate::world::StorageWorld::wipe_matchmaker
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
