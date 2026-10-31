//! The per-node draw: [`NodeShape`], the bounds of its knobs and the production profile.

use std::time::Duration;

use moonpool_sim::{assert_always, assert_reachable, buggify_knob};
use paros::roles::node::DriverTunables;

use crate::shape::timing::{
    CONNECT_FLOOR_MS, INTERNAL_CALL_FLOOR_MS, SLOWEST_ROUND_TRIP_MS, cap_backoff,
    resolve_answer_within, scrub_pace,
};

/// The longest a drawn quarantine holds a journal down, in milliseconds:
/// production's 80 ticks of 100 ms (`DriverTunables::production`).
pub(crate) const QUARANTINE_CEILING_MS: u64 = 8_000;

/// The longest capped base election timeout a drawn backoff reaches, in
/// milliseconds: production's `8 × T` (`DriverTunables::production`). A
/// node waits up to twice it, and one that learns a new matchmaker set
/// waits a second timeout to ask it: both fit the shortest recovery budget
/// (45 s, `ChainConfig::recovery_budget_ms`), even at the floor's 9.6 s.
pub(crate) const BACKOFF_CEILING_MS: u64 = 8_000;
const _: () = assert!(BACKOFF_CEILING_MS <= (12 * SLOWEST_TICK_MS) << 2);
const _: () = assert!(4 * ((12 * SLOWEST_TICK_MS) << 2) < 45_000);

/// The wall-clock floor of every driver timeout that races the network: one
/// Phase-1 round trip over moonpool's default cross-datacenter link plus one
/// delivery batch, i.e. production's `5 ticks × 50 ms`. See [`NodeShape::draw`]
/// for why it is a floor and not a tunable.
pub(crate) const ROUND_TRIP_FLOOR_MS: u64 = 250;

/// The floor of a drawn `resolve_answer_within`: two sequential tail reads,
/// each allowed two round trips.
pub(crate) const RESOLVE_FLOOR_MS: u64 = 4 * ROUND_TRIP_FLOOR_MS;
const _: () = assert!(RESOLVE_FLOOR_MS < 2000, "the floor sits under the default");

/// The slowest beat a node draws (`tick_interval`), in milliseconds.
pub(crate) const SLOWEST_TICK_MS: u64 = 200;

/// The floor of a client's drawn request deadline
/// (`ChainConfig::request_timeout_ms`), in milliseconds.
pub(crate) const REQUEST_FLOOR_MS: u64 = 1_000;

/// The longest a node's take-back budget (`proxy_take_back_resends` beats)
/// may hold a delegated round, in milliseconds: a write whose round the
/// leader must take back is answered after that budget plus two round
/// trips (the client's call and the colocated Phase 2), and that answer
/// must come inside the shortest deadline a client draws. Above it, a
/// proxy whose `Accept` a needed acceptor drops makes every write and every
/// retry of it outlast its deadline, so no write is acknowledged for the
/// whole tail although the cluster keeps deciding the retries. Witness
/// (cite only): sweep recipe 4157026387069203832 `[(9989,
/// 3535784000140791486)]` on 2365d15 (and e0e605a), a single proxy that
/// node 0 does not count, node 1 down for good: 27 beats of 50 ms per slot,
/// 1.5 s an answer against a 1.5 s deadline, 91 slots decided in the tail
/// and not one recovery write acknowledged. Production takes back after
/// 2 s, inside the library client's 5 s deadline.
pub(crate) const TAKE_BACK_CEILING_MS: u64 = REQUEST_FLOOR_MS - 2 * ROUND_TRIP_FLOOR_MS;
const _: () = assert!(TAKE_BACK_CEILING_MS + 2 * ROUND_TRIP_FLOOR_MS <= REQUEST_FLOOR_MS);
const _: () = assert!(
    TAKE_BACK_CEILING_MS >= SLOWEST_TICK_MS,
    "the slowest beat still takes a round back after one re-delegation"
);

/// The default per-restart chance of a terminal storage loss — a wiped node
/// disk (#124) or an unusable matchmaker registry (#125).
pub(crate) const DEFAULT_LOSS_PCT: u32 = 35;
/// The floor of both loss knobs: rare enough that a seed still has restarts
/// that come back.
pub(crate) const MIN_LOSS_PCT: u32 = 5;
/// The ceiling of both loss knobs: the dead-node and matchmaker-loss budgets
/// bound the damage, so the extreme stays a valid deployment.
pub(crate) const MAX_LOSS_PCT: u32 = 75;

/// The default per-restart chance that the operator restarts a node under an
/// edited configuration (#207, [`NodeShape::config_edit_pct`]).
pub(crate) const DEFAULT_CONFIG_EDIT_PCT: u32 = 10;

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
    #[allow(clippy::too_many_lines)]
    pub(crate) fn draw(slow_link: bool) -> Self {
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
        // write). Its floor is two round trips of the *slowest* link the run
        // keeps into the recovery tail ([`SLOWEST_ROUND_TRIP_MS`]), not of
        // the default link: a read's row must include that link whenever one
        // acceptor is away. Witness (cite only): replay-recipe
        // 11132861827014470837 1327731:15717124982357204208 on 5833bb3, a
        // founder renamed at 20 s on a slow-link seed; a 366 ms pair stayed
        // in every read quorum, each read expired at the old 510 ms floor
        // (answers 2 of 4), every coordinator install lost its cold election
        // read, and the election settled 50 s after the tail's term, past
        // the settle oracle (red on "election: the election settles on one
        // renewing leader after chaos" and three consequences).
        let ms = Duration::from_millis;
        let tick_ms = buggify_knob!(50_u64, 10_u64..SLOWEST_TICK_MS + 1);
        // A slow-link seed beats fast: 10 to 17 ms (#386's witness ran 17).
        let tick_ms = if slow_link {
            tick_ms.clamp(10, 17)
        } else {
            tick_ms
        };
        let floor_ticks = ROUND_TRIP_FLOOR_MS.div_ceil(tick_ms);
        let election_renew_ms = buggify_knob!(500_u64, 100_u64..1001_u64);
        // The failure detector's timeout past a renewal period (#211); the
        // re-placement bound adds to it (#475).
        let down_after_ms = buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64);
        let (scrub_rate, scrub_pass_within, scrub_page) = scrub_pace();
        let mut drawn = DriverTunables {
            tick_interval: ms(tick_ms),
            election_timeout_base: buggify_knob!(5_u64, 2_u64..13_u64).max(floor_ticks),
            keep_alive_interval: ms(buggify_knob!(2000_u64, ROUND_TRIP_FLOOR_MS..5001_u64)),
            keep_alive_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
            connection_timeout: ms(buggify_knob!(1000_u64, CONNECT_FLOOR_MS..3001_u64)),
            delivery_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
            read_retry_ticks: buggify_knob!(10_u64, 1_u64..41_u64)
                .max(2 * SLOWEST_ROUND_TRIP_MS.div_ceil(tick_ms)),
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
                paros::roles::node::MAX_READ_RECORDS,
                1_u64..paros::roles::node::MAX_READ_RECORDS + 1
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
            // stall budget drawn just below — a cadence at or past
            // `election_timeout * reconfigure_timeout_elections` would let
            // the phase be abandoned before it is ever re-sent, which is no
            // retry rather than a slow one, and a freeze that never closes
            // (#548 (set handover never lands)). The draw is lowered under
            // the shortest budget once both are drawn, below.
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
                paros::core::contract::limits::RECOVERY_BATCH,
                1_usize..paros::core::contract::limits::RECOVERY_BATCH + 1
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
            promise_page: buggify_knob!(
                paros::core::contract::limits::PROMISE_BATCH,
                1_usize..5_usize
            ),
            resend_page: buggify_knob!(
                paros::core::contract::limits::RESEND_BATCH,
                1_usize..5_usize
            ),
            apply_page: buggify_knob!(paros::core::contract::limits::APPLY_BATCH, 1_usize..5_usize),
            registry_page: buggify_knob!(
                paros::core::contract::limits::REGISTRY_PAGE,
                1_usize..5_usize
            ),
            // The delegated round's take-back budget (#142), in
            // re-delegations (one per beat). Floor 1: a round taken back
            // after a single re-delegation runs colocated while its proxy
            // may still decide it, and the two verdicts must agree
            // (P2b-idempotent fan-outs) — the whole point of pushing it.
            // The ceiling stretches a dead or ignored proxy's cost per
            // slot, and is lowered to `TAKE_BACK_CEILING_MS` of this
            // node's beats: a write taken back must still be answered
            // inside the shortest client deadline (the default, 10 beats
            // of 50 ms, sits exactly on it).
            proxy_take_back_resends: buggify_knob!(10_u64, 1_u64..41_u64)
                .min(TAKE_BACK_CEILING_MS / tick_ms),
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
            // The grid round's move budget (#533 (move a stuck round)), in
            // re-sends (one per beat). Floor 1: a round moved after a single
            // re-send runs in a second column while the first may still
            // decide it, and the two verdicts must agree (one ballot, one
            // value) — the state worth reaching. The floor stays winnable
            // because each move doubles the budget (`move_budget`): a round
            // that flips between columns faster than one round trip gets a
            // longer window after a few moves. The ceiling leaves a stuck
            // column's slots to the next leadership; the tail outlasts it.
            partition_move_resends: buggify_knob!(10_u64, 1_u64..4_u64),
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
            machine_down_after: ms(election_renew_ms + down_after_ms),
            // The re-placement bound (#475 (re-place after the Down bound)).
            // Floor: a round trip past `machine_down_after`, so a machine is
            // re-placed only once it is down. The range crosses a machine's
            // reboot and the silent machine's 4.5–6.5 s outage both ways: a
            // short bound re-places a machine that only rebooted, a long one
            // lets it rejoin in place.
            machine_replace_after: ms(election_renew_ms
                + down_after_ms
                + buggify_knob!(3000_u64, ROUND_TRIP_FLOOR_MS..8001_u64)),
            // A machine's busyness window (#424). Floor 1 s (the shipped
            // floor): shorter measures the sampler, not the machine. The
            // short end gives a run many windows, so `LOAD` meets a full
            // one early and often.
            load_interval: ms(buggify_knob!(5000_u64, 1000_u64..5001_u64)),
            // The slow-machine judgement (#361 (slow machines)): a round trip
            // bound above the slowest link's round trip, a busy bound from
            // 70 % to all, one to four rounds of hysteresis.
            slow_rtt: ms(buggify_knob!(1000_u64, 2 * ROUND_TRIP_FLOOR_MS..3001_u64)),
            slow_busy_permille: u32::try_from(buggify_knob!(900_u64, 700_u64..1001_u64))
                .unwrap_or(900),
            slow_rounds: u32::try_from(buggify_knob!(3_u64, 1_u64..5_u64)).unwrap_or(3),
            // How long a new status word must hold before a member shows it
            // (#191 (status)). Floor 0 (no hold). The default is below the
            // chaos window, so a word changes inside a run; the extremes
            // show every word at once or hold past a machine's reboot.
            status_hold: ms(buggify_knob!(1000_u64, 0_u64..4001_u64)),
            // A machine's own clients (#457 (hard-coded tunables)): the
            // coordinator's duties, the registry follow, the registration
            // and the `Resolve` folds. Floor `INTERNAL_CALL_FLOOR_MS`, a
            // fresh dial and a peer-served read: shorter, a call that must
            // dial first is ambiguous every time (#565 (renamed founder's
            // registration read times out)). The range crosses the lease
            // both ways on purpose: above `election_lease - election_renew`,
            // one slow duty under chaos outlasts the lease and the members
            // take over, again and again (the metastable plan's coordinator
            // loop); the tail must still settle the election.
            internal_request_timeout: ms(buggify_knob!(5000_u64, INTERNAL_CALL_FLOOR_MS..8001_u64)),
            // Floor 1: one re-send per resolution, so an ambiguous internal
            // write is more often left unresolved.
            internal_retry_budget: buggify_knob!(4_u32, 1_u32..9_u32),
            // Floor 0: re-sends back to back, a retry storm on the internal
            // journals; the ceiling spreads them over a second.
            internal_retry_backoff: ms(buggify_knob!(100_u64, 0_u64..1001_u64)),
            resolve_answer_within: resolve_answer_within(slow_link),
            scrub_rate,
            scrub_pass_within,
            scrub_page,
            // A clean stop's bound (#560 (clean Ctrl-C shutdown)). Floor 0:
            // the stop returns at once, the same as a crash. The range
            // crosses the machines' grace period (`MACHINE_GRACE_MS`) both
            // ways: a short bound returns before the handoff lands, a long
            // one is cut by the force kill, and both are crashes the
            // oracles already judge.
            shutdown_within: ms(buggify_knob!(1500_u64, 0_u64..4_001_u64)),
            // How long a store stays read-only after a full disk (#360
            // (disk full)), in ticks. Floor 1: a store that tries a write
            // every beat costs one refused segment per beat. The time counts
            // from the fault, so a full disk holds a journal unwritable for
            // the longer of it and the quarantine; the ceiling is the
            // quarantine's wall-clock one, which the tail outlasts (witness
            // 8180419182573274071: counted from the re-open, 8 s of
            // quarantine then 8 s read-only outlasted the settle tail).
            read_only_ticks: buggify_knob!(20_u64, 1_u64..161_u64)
                .min(QUARANTINE_CEILING_MS.div_ceil(tick_ms)),
            // The lag threshold of `Write` admission (#360 (disk full)).
            // Floor 1: one undecided round still admits the next write once
            // it is decided; the extreme answers most bursts `busy`.
            admission_rounds: buggify_knob!(1024_u64, 1_u64..5_u64),
            // Placement's free-bytes floor (#360 (disk full)). Floor 0: no
            // machine is ever low on space; the extreme passes over every
            // machine the disk-full scenario filled.
            free_bytes_floor: buggify_knob!(65_536_u64, 0_u64..2_097_153_u64),
            // A batcher's tick (#145). Floor 1 ms; the extreme holds writes
            // long enough for many writers' bursts to share one slot.
            batch_tick: ms(buggify_knob!(2_u64, 1_u64..51_u64)),
            // A batcher's flush bound (#145). Floor 1: every write goes out
            // alone; the extreme flushes a lane at two or three records.
            batch_flush_records: buggify_knob!(1024_u64, 1_u64..4_u64),
        };
        cap_backoff(&mut drawn, tick_ms);
        // The handover re-send cadence stays under the shortest stall
        // budget, one base election timeout per election (`check_floors`):
        // the freeze closes only on a re-send beat, so a budget that ends on
        // or before it abandons every handover its stop quorum answered
        // (#548 (set handover never lands): witness 4294352317195364374, a
        // five-tick cadence against a five-tick budget, and a set that never
        // left its founding member). A lowered draw runs at the edge, one
        // tick under the budget.
        drawn.reconfigurer_resend_ticks = drawn.reconfigurer_resend_ticks.min(
            (drawn.election_timeout_base * drawn.reconfigure_timeout_elections).saturating_sub(1),
        );
        // A coordinator that truncates at every renewal moves the floor once
        // per renewal period: a reader the floor outran waits at the tail
        // for the next renewal, which needs a tail wait as long as a
        // renewal period (`FLOOR_RACE_WAIT_MS` in `paros::client::election`).
        // A shorter wait answers empty at once and the reader never anchors.
        if drawn.election_compact_after == 1 {
            drawn.max_wait_ms = drawn.max_wait_ms.max(election_renew_ms);
        }
        // The production profile `parosd` ships (#209), whole: every field
        // at once, which the per-field locations above would draw together
        // only by chance. Each of its values lies inside its knob's range
        // (a 100 ms tick, a ten-tick election base, two-second reads), and
        // it clears the round-trip floor above in wall-clock terms. One
        // field is lowered: its two-second take-back fits production's
        // five-second client deadline, not the sim's `REQUEST_FLOOR_MS`, so
        // it takes the drawn knob's `TAKE_BACK_CEILING_MS` (witness
        // 16141873764476836178: every recovery write timed out behind a
        // proxy a needed acceptor did not count).
        // The per-field pairings below judge `drawn`, and only when it is
        // what runs: under the production profile no drawn extreme does.
        let production = !slow_link && buggify_knob!(0_u8, 1_u8..2_u8) == 1;
        let tunables = if production {
            assert_reachable!("a node runs the production driver tunables");
            production_profile()
        } else {
            drawn
        };
        check_take_back(&tunables);
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

/// `parosd`'s profile (#209), its take-back lowered to the sim's
/// `TAKE_BACK_CEILING_MS`: production pairs two seconds with a five-second
/// client deadline, the sim's clients draw theirs from `REQUEST_FLOOR_MS`.
/// Its read-only time (#360 (disk full)) is lowered to the quarantine's
/// `QUARANTINE_CEILING_MS`: production's 30 s waits for an operator to free
/// space, and the sim's recovery tail is shorter.
pub(crate) fn production_profile() -> DriverTunables {
    let mut profile = DriverTunables::production();
    let tick = u64::try_from(profile.tick_interval.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    profile.proxy_take_back_resends = profile
        .proxy_take_back_resends
        .min(TAKE_BACK_CEILING_MS / tick);
    profile.read_only_ticks = profile
        .read_only_ticks
        .min(QUARANTINE_CEILING_MS.div_ceil(tick));
    profile
}

/// Whichever profile runs, a write taken back is answered inside the
/// shortest client deadline.
pub(crate) fn check_take_back(tunables: &DriverTunables) {
    let tick = u64::try_from(tunables.tick_interval.as_millis()).unwrap_or(u64::MAX);
    assert_always!(
        tunables.proxy_take_back_resends.saturating_mul(tick) <= TAKE_BACK_CEILING_MS,
        "shape: a node's take-back fits the client's deadline floor",
        { "resends" => tunables.proxy_take_back_resends, "tick_ms" => tick }
    );
}

/// The BUGGIFY pairings of [`NodeShape::draw`]'s knobs: each extreme the
/// node genuinely runs (never under the production profile, which runs none
/// of them) reaches its gate.
pub(crate) fn pair_extremes(drawn: &DriverTunables, defaults: &DriverTunables) {
    if drawn.internal_request_timeout + drawn.election_renew > drawn.election_lease {
        // BUGGIFY pairing: a machine's internal call outlasts what is left
        // of its coordinator's lease after a renewal (#457).
        assert_reachable!("a node runs internal calls longer than its lease allows");
    }
    if drawn.internal_retry_budget != defaults.internal_retry_budget
        || drawn.internal_retry_backoff != defaults.internal_retry_backoff
    {
        // BUGGIFY pairing: the internal re-send extreme genuinely runs.
        assert_reachable!("a node runs an extreme internal re-send policy");
    }
    if drawn.resolve_answer_within < defaults.resolve_answer_within {
        // BUGGIFY pairing: a short `Resolve` answer bound genuinely runs.
        assert_reachable!("a node runs a short resolve answer bound");
    }
    if drawn.scrub_rate < defaults.scrub_rate / 16 {
        // BUGGIFY pairing: a slow scrubber genuinely runs.
        assert_reachable!("a node runs a slow scrubber");
    }
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
    if drawn.recovery_page != paros::core::contract::limits::RECOVERY_BATCH {
        // BUGGIFY pairing: the recovery page extreme genuinely runs.
        assert_reachable!("a node runs with a small recovery page");
    }
    if drawn.promise_page != paros::core::contract::limits::PROMISE_BATCH {
        // BUGGIFY pairing: the promise page extreme genuinely runs.
        assert_reachable!("a node runs with a small promise page");
    }
    if drawn.resend_page != paros::core::contract::limits::RESEND_BATCH {
        // BUGGIFY pairing: the re-send page extreme genuinely runs.
        assert_reachable!("a node runs with a small re-send page");
    }
    if drawn.apply_page != paros::core::contract::limits::APPLY_BATCH {
        // BUGGIFY pairing: the apply page extreme genuinely runs.
        assert_reachable!("a node runs with a small apply page");
    }
    if drawn.registry_page != paros::core::contract::limits::REGISTRY_PAGE {
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
