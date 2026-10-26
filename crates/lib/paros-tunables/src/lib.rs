//! The tunables layer: every role's per-node [`DriverTunables`], the
//! transport constants they default to, their floors ([`BelowFloor`]), the
//! client's [`ClientTunables`] and the RPC frame limit. Plain data, no I/O.

mod client;
mod floors;
pub mod scrub;

pub use client::{CheckpointPolicy, ClientTunables};
pub use floors::BelowFloor;

use std::time::Duration;

/// Largest frame a paros runtime accepts or produces: a full delivery batch
/// (the driver's 3 MiB batch cap) plus envelope headroom. A client runtime
/// takes the same limit, or a large `Read` page would fail `ReplyTooLarge`
/// on its side.
pub const MAX_FRAME_BYTES: u32 = 4 << 20;

/// A busy ratio leans fast again only this far below `slow_busy`, in
/// thousandths (`paros_coordinator::cell::slow`).
pub const BUSY_MARGIN: u32 = 200;

/// How often a node advances its logical clock.
const TICK_INTERVAL: Duration = Duration::from_millis(50);

/// RPC liveness pings on peer connections. Both values use provider time, so a
/// half-open connection is failed deterministically during the settle tail.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(2);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(1);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(1);
/// Per-peer in-memory handoff capacity. Like etcd's stream mailbox, this is
/// deliberately bounded and lossy: the consensus driver never waits for
/// network I/O, and current heartbeats/resends repair anything dropped here.
/// Overflow evicts the *oldest* undelivered message (see [`PeerMailbox`]).
const PEER_QUEUE_CAPACITY: usize = 4096;
/// Leave headroom below the RPC runtime's 4 MiB frame limit
/// (`rpc::inbound`'s `MAX_FRAME_BYTES`) for the protobuf and RPC envelopes.
/// An earlier transport capped a complete payload at 1 MiB; this preserves
/// that per-message envelope while allowing compact batches.
pub const DELIVERY_BATCH_BYTES: usize = 3 * 1024 * 1024;
/// Maximum Paxos messages packed into one `Deliver` request. This keeps a
/// chatty heartbeat/catch-up round from creating one RPC frame per message.
pub const DELIVERY_BATCH: usize = 64;
/// Bounded inboxes between the RPC edge and the node loop: overload is
/// visible (a refused call, or backpressure on the peer lane), with ample
/// room for one tick's peer fanout.
const CLIENT_INBOX_CAPACITY: usize = 256;
const PEER_INBOX_CAPACITY: usize = 1024;

// A delivery batch must fit one RPC frame with room for its envelope, a
// mailbox must hold at least one whole batch, and a keep-alive must time out
// before the next one is due.
const _: () = assert!(DELIVERY_BATCH_BYTES < MAX_FRAME_BYTES as usize);
const _: () = assert!(DELIVERY_BATCH > 0);
const _: () = assert!(PEER_QUEUE_CAPACITY >= DELIVERY_BATCH);
const _: () = assert!(KEEP_ALIVE_TIMEOUT.as_millis() < KEEP_ALIVE_INTERVAL.as_millis());
const _: () = assert!(CLIENT_INBOX_CAPACITY > 0);
const _: () = assert!(PEER_INBOX_CAPACITY > 0);

/// Per-node driver tunables — **born workload-buggified config** (AGENTS.md
/// prong 2): plain data the harness layer randomizes per seed, FDB knob style,
/// around [`DriverTunables::default()`], the simulation's baseline built from
/// the constants above. A real deployment runs
/// [`DriverTunables::production()`] (#209), and an operator's override is
/// judged by [`DriverTunables::check_floors`]. Every field documents its floor: a capacity must be
/// at least 1 (a zero-capacity mpsc channel panics at construction), a
/// duration at least non-zero, and the election base at least
/// `2 * HEARTBEAT_TICKS` so a live leader always beats before a follower's
/// election clock fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DriverTunables {
    /// How often the node advances its logical clock. Pacing, not a protocol
    /// bound: every timeout the core owns is counted in ticks, so a slower
    /// tick is a slower node, which the cluster already tolerates. Floor: any
    /// non-zero duration.
    pub tick_interval: Duration,
    /// Base election timeout `T`, in ticks; the actual timeout is drawn from
    /// `[T, 2T)` to break dueling proposers. Two floors: `2 * HEARTBEAT_TICKS`
    /// (see `paros_core`), below which a live leader's beat can lose the race
    /// against its followers' election clocks every round; and, in wall-clock
    /// terms, `T × tick_interval` must exceed a Phase-1 round trip, or a
    /// candidate abandons its own round before its promises return and no
    /// leader is ever elected. One round trip is the floor, not the whole
    /// Phase 1: a long suffix is paged (`promise_page` entries per round
    /// trip), and that can take longer than `T`. Each page resets the
    /// candidate's clock, and on every tick the candidate re-sends its
    /// `Prepare` to the peers that answered, which resets theirs (#428
    /// (paging livelock)).
    pub election_timeout_base: u64,
    /// Liveness ping interval on the RPC runtime's connections
    /// (`PeerPolicy::ping_interval`, provider time, so a half-open
    /// connection is failed deterministically). Floor: non-zero.
    pub keep_alive_interval: Duration,
    /// How long a connection may stay silent after a ping before it is
    /// failed (`PeerPolicy::ping_timeout`). Floor: non-zero.
    pub keep_alive_timeout: Duration,
    /// How long a connect (and its session handshake) may take before the
    /// attempt fails (`RpcConfig::connect_timeout`); the runtime re-dials with
    /// backoff for as long as calls need the peer. Floor: non-zero.
    pub connection_timeout: Duration,
    /// How long one peer-delivery RPC may take to get its batch *into the
    /// peer's inbox* before the batch is written off as lost (the mailbox is
    /// lossy by contract; resends repair it). The peer acks on enqueue, not
    /// after processing, so this races the connection and the peer's
    /// `peer_inbox_capacity`, never its loop. Floor: non-zero.
    pub delivery_timeout: Duration,
    /// Ticks a parked read may wait for its quorum read to be served (the
    /// row answered whole and the fold covers the maximum) before the driver
    /// answers a retry redirect. Floor: the row answers in one `PreRead`
    /// round trip, so `read_retry_ticks × tick_interval` must exceed it or no
    /// read is ever served. A client whose deadline is shorter
    /// than the wait simply times out (ambiguous, never wrong).
    pub read_retry_ticks: u64,
    /// The longest `wait_ms` a journal `Read` gets (#241,
    /// `docs/architecture/02-data-plane.md` §2.7): a larger one is capped to this. A read
    /// at or past the serving node's end waits up to the capped value (the
    /// tail wait, #185) for something to be chosen before the driver
    /// answers an empty page. Floor 0: a zero cap answers every such read
    /// at once, empty, and the client simply re-asks — a busier client,
    /// never a wrong one. A client whose deadline is shorter than the wait
    /// times out (ambiguous, never wrong). The cap wins over
    /// `min_wait_ms`.
    pub max_wait_ms: u64,
    /// The shortest wait a journal `Read` that asks to wait gets (#241,
    /// §2.7): a non-zero `wait_ms` below it is raised to it, so a client
    /// cannot turn the tail wait into a busy poll. A `wait_ms` of 0 still
    /// answers at once. Floor 0 (no minimum); a minimum above the client's
    /// deadline times the client out (ambiguous, never wrong).
    pub min_wait_ms: u64,
    /// The most records one `Read` page carries (#241, §2.7): a `limit` of
    /// 0 or above it is cut to it. Floor 1: a one-record page always moves
    /// a reader forward.
    pub max_read_records: u64,
    /// The record bytes one `Read` page may carry (#241, §2.7). A page that
    /// can hold a record always holds one, whatever its size, so a record
    /// above the budget is a one-record page. Floor 1; keep it below the
    /// RPC frame (`MAX_FRAME_BYTES`), or a full page is a reply the
    /// transport refuses (the client's read is ambiguous, never wrong).
    pub max_read_bytes: u64,
    /// Ticks a journal quarantined by a storage fault (#188) stays down on
    /// this node before the driver re-opens it from its store — the
    /// per-journal twin of a crashed process's restart delay. Floor 1: a
    /// re-open the same tick is a restart loop with no room for the rest of
    /// the node; a long quarantine is a node that is slow to heal one
    /// journal, never a wrong one.
    pub quarantine_ticks: u64,
    /// How many times a node's election timeout base may double across
    /// consecutive failed rounds (an election clock reset with no leader
    /// known: a failed campaign, or a leadership a rival deposed): the
    /// `k`-th consecutive one draws from `[T·2^j, 2·T·2^j)` with
    /// `j = min(k - 1, election_backoff_doublings)`, and hearing another
    /// node lead resets it (leading does not, except for the only
    /// acceptor of its configuration, which has no rival). A fixed timeout below a Phase-1 round trip
    /// livelocks for good: a sole candidate whose slowest promise always
    /// lands one round late abandons every round it opens (witness
    /// 2881076808784637484: `q1 = n` over a degraded link, 180 rounds in
    /// 63 s and never a leader). Floor 2: `T × 4` outruns any round trip the
    /// base's own floor is sized against; a larger ceiling is a slower
    /// recovery after a leader dies behind a partition, never a wrong one.
    pub election_backoff_doublings: u32,
    /// Capacity of each client-facing endpoint queue (propose, read, compact,
    /// inspect, …) between the RPC runtime and the node loop
    /// (`RpcConfig::endpoint_queue_capacity`). Floor 1: overload is visible as
    /// a refused call (`Overloaded`, never admitted), never as a lost request.
    pub client_inbox_capacity: usize,
    /// Capacity of the peer-message inbox the `Deliver` lane feeds. Floor 1:
    /// a full inbox backpressures the lane, never loses a message.
    pub peer_inbox_capacity: usize,
    /// Per-peer in-memory handoff capacity. Like etcd's stream mailbox, this
    /// is deliberately bounded and lossy: the consensus driver never waits for
    /// network I/O, and current heartbeats/resends repair anything dropped
    /// here (overflow evicts the oldest message, keep-newest). The extreme (a
    /// handful of slots) makes mailbox overflow —
    /// [`Audit::dropped_at_mailbox`](paros_observe::audit::Audit::dropped_at_mailbox)
    /// — a likely event instead of a rare one.
    pub peer_queue_capacity: usize,
    /// Maximum Paxos messages packed into one `Deliver` request. A small
    /// batch raises RPC framing pressure and the batcher's keep-the-newest
    /// overflow shedding; its floor is a throughput floor — a link carries
    /// every journal its two ends serve, so a batch too small for their
    /// combined rate is a permanent partition (the harness keeps it at 24
    /// or more, `paros_sim::shape`).
    pub delivery_batch: usize,
    /// Ticks between re-sends of an open matchmaking request
    /// (`ColocatedNode::resend_matchmaking`), on a deployment with matchmakers.
    /// Floor 1: a re-send per tick is a request per tick per matchmaker, which
    /// the registry answers idempotently. The default is one election-timeout
    /// base, so a lost reply costs about one round trip before the retry.
    pub match_resend_ticks: u64,
    /// Ticks between re-sends of an open GC request (`ColocatedNode::resend_gc`),
    /// on a deployment with matchmakers. Its own cadence, not matchmaking's:
    /// the two pace unrelated round trips and a seed should be able to be
    /// extreme in one and ordinary in the other. Floor 1 (a request per tick,
    /// answered idempotently); the ceiling is unbounded and still safe — a
    /// watermark that is never raised costs the matchmakers their retained
    /// histories, never safety.
    pub gc_resend_ticks: u64,
    /// Ticks between re-sends of the running matchmaker-set handover's step
    /// (`HandoverDriver::resend_due`). Floor 1; bounded above by the stall
    /// budget below — a cadence longer than
    /// `election_timeout * reconfigure_timeout_elections` would let the phase
    /// be abandoned before it is ever re-sent, which is not a slower retry
    /// but no retry at all.
    pub reconfigurer_resend_ticks: u64,
    /// How many election timeouts a matchmaker-set handover may make no
    /// progress before the driver abandons it
    /// (`MatchmakerReconfigurer::abandon`). Driver policy, never a constant
    /// inside the state machine: the core only reports the stall
    /// (`stalled_for`). Floor 1 election timeout — long enough for a slow
    /// matchmaker to answer one re-sent request; below that a healthy
    /// handover could not finish, which is not an extreme configuration but
    /// a broken one. The shortest budget, `election_timeout_base` times this,
    /// also stays above `reconfigurer_resend_ticks`: the freeze closes only
    /// on a re-send beat, and a budget that ends at or before that beat
    /// abandons every handover on the tick its stop quorum would have closed
    /// (#548 (set handover never lands): witness 4294352317195364374, a
    /// five-tick cadence against a one-election budget of five ticks, so a
    /// set's handover onto an admitted machine restarted for the whole tail).
    pub reconfigure_timeout_elections: u64,
    /// Upper bound on the jittered backoff a preempted successor decree waits
    /// before it reopens at a higher ballot — the symmetry break between
    /// dueling reconfigurers, drawn from `1..=reconfigure_backoff_max_ticks`.
    /// Floor 1 (draw exactly one tick: no jitter, so two reconfigurers may
    /// duel for a while — liveness, and the stall budget ends it). Its own
    /// knob rather than a multiple of `election_timeout_base`, so a seed can
    /// push the election clock and the decree's symmetry break independently.
    pub reconfigure_backoff_max_ticks: u64,
    /// How many times a delegated round may be re-delegated (once per beat,
    /// by `resend_pending`) without the proxy's `Commit` arriving before the
    /// leader **takes it back** and runs it colocated
    /// (`ColocatedNode::take_back_delegated`, #142). Driver policy, never a
    /// constant of the state machine: liveness under a dead proxy is the
    /// leader's, and this is its whole budget. Floor 1 — taking a round back
    /// after a single re-delegation is always safe (two fan-outs of one
    /// `(slot, ballot, command)` are P2b-idempotent), it merely runs more of
    /// the log colocated; the ceiling is unbounded and still winnable, a
    /// round a dead proxy holds forever being recovered by the next
    /// leadership's Phase 1. A client is answered only once its round is
    /// decided, so its deadline must outlast this budget's wall clock plus
    /// two round trips: below that, every write whose proxy a needed
    /// acceptor drops ends ambiguous. Meaningless on a deployment without
    /// proxies.
    pub proxy_take_back_resends: u64,
    /// How many times a proxy leader may re-fan-out an open round (once per
    /// beat, by `ProxyLeader::resend_pending`) without an answer before it
    /// **evicts** it (`ProxyLeader::expire_stale`, #142) — the proxy's
    /// bounded retention. A round nobody answers is a real state, not a
    /// slow one: an `Accept` for a slot every acceptor compacted past is
    /// ignored without `Accepted` or `Nack`, so a delegation delayed until
    /// after the leader took the round back and the cluster truncated the
    /// slot would otherwise be re-fanned-out for the rest of the process's
    /// life. Eviction is never a decision — the leader's re-delegation
    /// reopens a round it still needs and its take-back
    /// (`proxy_take_back_resends`) stays the liveness — so the floor is 1
    /// and the ceiling is unbounded and still winnable. The default is twice
    /// the take-back budget, so in the default configuration the leader has
    /// taken a stalled round back before its proxy evicts it and the
    /// eviction reclaims only rounds the leader is done with. Meaningless
    /// on a proxy-less deployment, and read only by `run_proxy`.
    pub proxy_round_resends: u64,
    /// How many times a colocated round of a grid (acceptor partitions) may
    /// be re-sent (once per beat, by `resend_pending`) without a decision
    /// before the leader **moves** it to another column
    /// (`ColocatedNode::move_round`, #533 (move a stuck round)): a column
    /// that cannot reach its quorum otherwise holds its slots, and the apply
    /// walk stalls behind the first one. This is the budget of a round that
    /// never moved: each move doubles it, up to 64 times this value
    /// (`move_budget` in `driver/mod.rs`), so the new column of a moved round
    /// has time to answer. Driver policy, never a constant of the state
    /// machine. Floor 1 — a move after a single re-send is always safe (one
    /// ballot, one value; every Phase-1 quorum meets every column), it merely
    /// spreads a slot over more acceptors, and the doubling keeps it winnable
    /// when a round trip is longer than one beat; the ceiling is unbounded
    /// and still winnable, a stuck round being recovered by the next
    /// leadership's Phase 1. Meaningless under a majority or a flexible
    /// split.
    pub partition_move_resends: u64,
    /// The most records one `Write` may carry (#241, `docs/architecture/02-data-plane.md`
    /// §2.7). A larger batch is refused at the edge (`TooLarge`) before it
    /// reaches consensus. Floor 1: a one-record batch always fits, so a
    /// writer that splits its batches always makes progress.
    pub max_batch_records: u64,
    /// The most record bytes one `Write` may carry (#241, §2.7), summed over
    /// its records; refused at the edge like `max_batch_records`. Floor 1;
    /// a record larger than this can never be written, which is the
    /// contract, never a wrong answer. The default leaves room under the RPC
    /// frame (`MAX_FRAME_BYTES`) for the request's other fields.
    pub max_batch_bytes: u64,
    /// The cell election's lease (#240): how long a founding member, having
    /// seen no renewal of the cell coordinator, waits before it campaigns.
    /// A liveness hint on the member's own clock, never a fence. Floor:
    /// above `election_renew` (refused at or below it). It must also
    /// outlast one renewal's round trip, a wall-clock floor the operator
    /// keeps: below it members take over from a live coordinator, which
    /// costs availability, never safety. It covers one renewal's round
    /// trip, never a term's install or duties: the coordinator renews
    /// while those run (#542 (coordinator starves its lease while it
    /// installs)).
    pub election_lease: Duration,
    /// How often the cell coordinator renews its leadership in the election
    /// journal (#240). Floor 1 ms; it stays under `election_lease`.
    pub election_renew: Duration,
    /// The records the cell coordinator lets lie below its latest renewal
    /// before it truncates the election journal to it (#240): the bound on
    /// the journal's log. Floor 1: a truncation per renewal.
    pub election_compact_after: u64,
    /// How long the cell coordinator hears nothing from a machine before it
    /// writes `MachineDown` for it (#211, D6): the timeout of its failure
    /// detector, an `Identify` per machine at every renewal period. Floor:
    /// above `election_renew` (refused at or below it), so one missed
    /// probe never marks a machine down. It should also outlast a reboot;
    /// below that every reboot is a `Down` then an `Up`, which is noise,
    /// never a wrong answer.
    pub machine_down_after: Duration,
    /// How long the cell coordinator hears nothing from a machine it marked
    /// down before it re-places the machine's roles (#475 (re-place after
    /// the Down bound), §3.2): each journal and matchmaker set with a slot
    /// on it moves to another machine, as off a draining one. A machine
    /// that answers again inside the bound (a reboot: same `node_id`, new
    /// incarnation) keeps its roles. A machine in a maintenance window is
    /// never re-placed inside it (#359). Floor: above `machine_down_after`
    /// (refused at or below it), so a machine is re-placed only once it is
    /// down. A short bound re-places a machine that only rebooted: a move
    /// that costs work, never a wrong answer.
    pub machine_replace_after: Duration,
    /// The most rounds one page of a leader's recovery visits (#330,
    /// `ColocatedNode::set_recovery_page`) before the driver advances the
    /// batch. A per-node size, never on the wire. Floor 1: a one-slot page
    /// still drains the recovery, one `Ready` per slot. The ceiling is
    /// [`paros_core::LEADER_RECOVERY_BATCH`], the default; a larger value
    /// runs the ceiling.
    pub recovery_page: usize,
    /// The most accepted records and faulty entries one `Promise` page
    /// carries before its continuation cursor (#338,
    /// `ColocatedNode::set_promise_page`). A per-node size: a receiver
    /// checks a page against the core's ceiling, never against its own
    /// size. Floor 1: a one-entry page still walks the suffix, one round
    /// trip per entry. The ceiling is [`paros_core::PROMISE_BATCH`], the
    /// default; a larger value runs the ceiling.
    pub promise_page: usize,
    /// The most open rounds one re-send page carries (#338,
    /// `ColocatedNode::set_resend_page`). A per-node size, never on the
    /// wire. Floor 1: the cursor still visits every open round, one per
    /// beat. The ceiling is [`paros_core::RESEND_BATCH`], the default.
    pub resend_page: usize,
    /// The most chosen slots one apply walk folds before it yields (#338,
    /// `ColocatedNode::set_apply_page`, `ReplicaNode::set_apply_page`). A
    /// per-node size, never on the wire. Floor 1: the walk still folds the
    /// whole prefix, one slot per `Ready`. The ceiling is
    /// [`paros_core::APPLY_BATCH`], the default.
    pub apply_page: usize,
    /// The most registrations one matchmaker answer carries before its
    /// continuation cursor (#338, `Matchmaker::set_registry_page`). A
    /// per-matchmaker size: a candidate checks a page against the core's
    /// ceiling. Floor 1: a one-registration page still delivers the whole
    /// history, one round trip per registration. The ceiling is
    /// [`paros_core::REGISTRY_PAGE`], the default.
    pub registry_page: usize,
    /// How often a machine samples its busyness counters (#424): the length
    /// of the window `Load` answers with. FDB's `WORKER_LOGGING_INTERVAL`
    /// is 5 s. Floor 1 s: a shorter window measures the sampler's own
    /// jitter, not the machine.
    pub load_interval: Duration,
    /// The `Identify` round trip at or above which the cell coordinator
    /// leans a machine slow (#361 (slow machines), `paros_coordinator::cell::slow`),
    /// when it is also four times the round's median. Floor 1 ms; it should
    /// stay well above an ordinary round trip, or every machine leans slow
    /// and only the cap on marks holds the cell together.
    pub slow_rtt: Duration,
    /// The disk or CPU busy ratio, in thousandths, at or above which the
    /// cell coordinator leans a machine slow (#361 (slow machines)). Floor
    /// 201 (one above the fast margin), ceiling 1000.
    pub slow_busy_permille: u32,
    /// How many watch rounds in a row must lean the other way before the
    /// cell coordinator marks a machine slow, or fast again (#361 (slow
    /// machines)): the hysteresis. Floor 1.
    pub slow_rounds: u32,
    /// How long a journal's new status word must hold before a founding
    /// member shows it (#191 (status), `paros_model::status::Hysteresis`).
    /// Floor 0: every answer shows its own word. A hold shorter than the
    /// cell coordinator's failure detector (`machine_down_after`) hides
    /// little, since the registry's liveness moves no faster than that.
    pub status_hold: Duration,
    /// The request and read timeout of a machine's own clients (#457
    /// (hard-coded tunables)): the cell coordinator's, the registry follow,
    /// the registration, and the `Resolve` folds. Floor 1 ms; it should
    /// outlast a connect to a peer and a read that peer confirms with a
    /// quorum, a wall-clock floor the operator keeps: below it, a call that
    /// dials first is ambiguous every time (#565 (renamed founder's
    /// registration read times out): a moved founding member never
    /// registers), a liveness cost, never a wrong answer. Above `election_lease` minus
    /// `election_renew`, one slow call of a coordinator's duties outlasts
    /// its lease and the members take over: the production profile stays
    /// under that bound; the simulation also draws above it, so the control
    /// plane meets the takeovers it causes.
    pub internal_request_timeout: Duration,
    /// How often a machine's own client re-sends an ambiguous write while it
    /// resolves it (#457). Floor 1: one re-send.
    pub internal_retry_budget: u32,
    /// The pause between those re-sends (#457). Floor 0: back to back.
    pub internal_retry_backoff: Duration,
    /// How long a machine takes to answer one `Resolve` (#216) before it
    /// answers `unavailable` (#457). The folds keep what they read, so a
    /// later request goes on from there. Floor 1 ms; it should outlast two
    /// read round trips in sequence (the registry tail, then the directory
    /// tail), a wall-clock floor the operator keeps: below it, no answer ever
    /// comes back.
    pub resolve_answer_within: Duration,
    /// How many bytes per second the tenant coordinator's scrubber reads from
    /// each journal's acceptors (#362 (background scrubbing)): a token
    /// bucket per tenant, refilled on the term's own clock. Floor 1 KiB/s
    /// ([`SCRUB_RATE_FLOOR`]): one small record per second still moves a
    /// pass forward; below it a pass never ends.
    pub scrub_rate: u64,
    /// The full-pass bound of the scrubber (#362): a pass starts no sooner
    /// than this after the previous pass started, and a pass still running
    /// when it runs out is late (shown in `parosctl status` and the
    /// metrics). Floor 1 s ([`SCRUB_PASS_WITHIN_FLOOR`]): a shorter bound
    /// only re-reads the same records back to back.
    pub scrub_pass_within: Duration,
    /// How many records one scrub request asks of one acceptor (#362).
    /// Floor 1; the acceptor cuts a page at `paros::driver` `SCRUB_PAGE`
    /// records and 1 MiB whatever the scrubber asks.
    pub scrub_page: usize,
    /// How long a clean stop may take (#560 (clean Ctrl-C shutdown)): the
    /// node loop refuses new calls, answers the ones it holds, hands its
    /// leaderships on and lets its flushes land, then returns; once this
    /// bound runs out it returns anyway and logs the step that did not
    /// complete. Floor 0: a stop at once, the same as a crash, which
    /// recovery already handles.
    pub shutdown_within: Duration,
    /// Ticks a journal stays read-only after a full disk quarantined it
    /// (#360 (disk full)): it votes in Phase 1, serves reads and refuses
    /// Phase-2 votes, then becomes writable again; a write that still finds
    /// the disk full makes it read-only again. Floor 1: a store retried
    /// every beat costs one refused write per beat, never a wrong answer; a
    /// long hold keeps a store that has space again out of Phase 2.
    pub read_only_ticks: u64,
    /// The lag threshold of `Write` admission (#360 (disk full)): a leader
    /// that holds this many undecided Phase-2 rounds or more answers a new
    /// `Write` `busy` (its acceptors fall behind), and the client retries
    /// it. Floor 1: a leader with one round in flight still takes the next
    /// write once that round is decided; a high threshold admits every
    /// write, as before admission control.
    pub admission_rounds: u64,
    /// The free-bytes floor of placement (#360 (disk full)): a machine
    /// whose disk has less free space than this is marked low on space in
    /// the registry, and placement books it last, as a down one. Floor 0:
    /// no machine is ever low on space.
    pub free_bytes_floor: u64,
    /// The longest a write waits in a batcher's lane before the lane
    /// flushes (#145): the batcher's tick. Floor 1 ms: a lane flushes on
    /// the next tick after its first write.
    pub batch_tick: Duration,
    /// How many records a batcher's lane holds before it flushes on its own,
    /// before a tick (#145). Floor 1: every write goes out at once, alone.
    /// Capped at `max_batch_records` when the batcher starts.
    pub batch_flush_records: u64,
}

impl Default for DriverTunables {
    fn default() -> Self {
        let tunables = Self {
            tick_interval: TICK_INTERVAL,
            election_timeout_base: ELECTION_TIMEOUT_BASE,
            keep_alive_interval: KEEP_ALIVE_INTERVAL,
            keep_alive_timeout: KEEP_ALIVE_TIMEOUT,
            connection_timeout: DELIVERY_TIMEOUT,
            delivery_timeout: DELIVERY_TIMEOUT,
            read_retry_ticks: READ_RETRY_TICKS,
            max_wait_ms: MAX_WAIT_MS,
            min_wait_ms: 0,
            max_read_records: MAX_READ_RECORDS,
            max_read_bytes: MAX_READ_BYTES,
            quarantine_ticks: QUARANTINE_TICKS,
            election_backoff_doublings: ELECTION_BACKOFF_DOUBLINGS,
            client_inbox_capacity: CLIENT_INBOX_CAPACITY,
            peer_inbox_capacity: PEER_INBOX_CAPACITY,
            peer_queue_capacity: PEER_QUEUE_CAPACITY,
            delivery_batch: DELIVERY_BATCH,
            match_resend_ticks: ELECTION_TIMEOUT_BASE,
            gc_resend_ticks: ELECTION_TIMEOUT_BASE,
            reconfigurer_resend_ticks: ELECTION_TIMEOUT_BASE,
            reconfigure_timeout_elections: RECONFIGURE_TIMEOUT_ELECTIONS,
            reconfigure_backoff_max_ticks: ELECTION_TIMEOUT_BASE * 2,
            proxy_take_back_resends: PROXY_TAKE_BACK_RESENDS,
            proxy_round_resends: PROXY_ROUND_RESENDS,
            partition_move_resends: PARTITION_MOVE_RESENDS,
            max_batch_records: MAX_BATCH_RECORDS,
            max_batch_bytes: MAX_BATCH_BYTES,
            election_lease: ELECTION_LEASE,
            election_renew: ELECTION_RENEW,
            election_compact_after: ELECTION_COMPACT_AFTER,
            machine_down_after: MACHINE_DOWN_AFTER,
            machine_replace_after: MACHINE_REPLACE_AFTER,
            recovery_page: paros_core::LEADER_RECOVERY_BATCH,
            promise_page: paros_core::PROMISE_BATCH,
            resend_page: paros_core::RESEND_BATCH,
            apply_page: paros_core::APPLY_BATCH,
            registry_page: paros_core::REGISTRY_PAGE,
            load_interval: LOAD_INTERVAL,
            slow_rtt: SLOW_RTT,
            slow_busy_permille: SLOW_BUSY_PERMILLE,
            slow_rounds: SLOW_ROUNDS,
            status_hold: STATUS_HOLD,
            internal_request_timeout: INTERNAL_REQUEST_TIMEOUT,
            internal_retry_budget: INTERNAL_RETRY_BUDGET,
            internal_retry_backoff: INTERNAL_RETRY_BACKOFF,
            resolve_answer_within: RESOLVE_ANSWER_WITHIN,
            scrub_rate: SCRUB_RATE,
            scrub_pass_within: SCRUB_PASS_WITHIN,
            scrub_page: SCRUB_PAGE,
            shutdown_within: SHUTDOWN_WITHIN,
            read_only_ticks: READ_ONLY_TICKS,
            admission_rounds: ADMISSION_ROUNDS,
            free_bytes_floor: FREE_BYTES_FLOOR,
            batch_tick: BATCH_TICK,
            batch_flush_records: BATCH_FLUSH_RECORDS,
        };
        // The simulation's baseline is itself a winnable profile.
        assert!(
            tunables.check_floors().is_ok(),
            "the default profile clears every floor"
        );
        assert!(
            tunables.keep_alive_timeout < tunables.keep_alive_interval,
            "a keep-alive times out before the next is due"
        );
        tunables
    }
}

/// The most records one `Write` may carry: well above any batch the
/// simulation's writers build, and a 1 KiB record each fits the byte limit.
const MAX_BATCH_RECORDS: u64 = 1024;

/// The most record bytes one `Write` may carry: 1 MiB, a quarter of the RPC
/// frame.
const MAX_BATCH_BYTES: u64 = 1 << 20;

// The byte limit leaves the frame room for the request's other fields and
// the records' framing (#241).
const _: () = assert!(MAX_BATCH_BYTES <= (MAX_FRAME_BYTES as u64) / 2);
const _: () = assert!(MAX_BATCH_RECORDS >= 1);

/// Ticks a parked read reply may wait for its quorum read to be served before
/// the driver answers a retry redirect (500 ms — well inside the sim client's
/// 1000 ms deadline, and inside the core's own read TTL, so a late core
/// answer just finds the ctx gone and is ignored).
const READ_RETRY_TICKS: u64 = 10;

/// The longest tail wait a journal `Read` gets (#185, #241): 400 ms, eight
/// ticks at the default tick, inside the sim client's 1000 ms deadline.
const MAX_WAIT_MS: u64 = 400;

/// The records one `Read` page carries at most (#241): the page a client
/// that names no limit gets.
pub const MAX_READ_RECORDS: u64 = 256;

/// The record bytes one `Read` page carries at most (#241): 64 KiB, far
/// below the RPC frame.
const MAX_READ_BYTES: u64 = 64 * 1024;

// A page carries at least one record and always fits one RPC frame (a lone
// record above the budget is the page's one exception, bounded by the write
// that carried it).
const _: () = assert!(MAX_READ_RECORDS >= 1);
const _: () = assert!(MAX_READ_BYTES >= 1);
const _: () = assert!(MAX_READ_BYTES < MAX_FRAME_BYTES as u64);
const _: () = assert!(MAX_BATCH_BYTES < MAX_FRAME_BYTES as u64);
/// Default [`DriverTunables::election_lease`]: four renewals, and well past
/// the simulated client's one-second deadline.
const ELECTION_LEASE: Duration = Duration::from_secs(2);

/// Default [`DriverTunables::election_renew`].
const ELECTION_RENEW: Duration = Duration::from_millis(500);

/// Default [`DriverTunables::machine_down_after`]: three renewal periods.
const MACHINE_DOWN_AFTER: Duration = Duration::from_millis(1500);

const _: () = assert!(MACHINE_DOWN_AFTER.as_millis() > ELECTION_RENEW.as_millis());

/// Default [`DriverTunables::machine_replace_after`]: two failure-detector
/// periods past the first.
const MACHINE_REPLACE_AFTER: Duration = Duration::from_millis(4500);

const _: () = assert!(MACHINE_REPLACE_AFTER.as_millis() > MACHINE_DOWN_AFTER.as_millis());

/// Default [`DriverTunables::load_interval`]: FDB's 5 s.
const LOAD_INTERVAL: Duration = Duration::from_secs(5);

/// The floor of [`DriverTunables::load_interval`].
pub const LOAD_INTERVAL_FLOOR: Duration = Duration::from_secs(1);

const _: () = assert!(LOAD_INTERVAL.as_millis() >= LOAD_INTERVAL_FLOOR.as_millis());

/// Default [`DriverTunables::slow_rtt`]: a second, far above a healthy
/// `Identify` on any link the simulation draws.
const SLOW_RTT: Duration = Duration::from_secs(1);

/// Default [`DriverTunables::slow_busy_permille`]: 90 % busy.
const SLOW_BUSY_PERMILLE: u32 = 900;

/// Default [`DriverTunables::slow_rounds`]: three watch rounds.
const SLOW_ROUNDS: u32 = 3;

const _: () = assert!(SLOW_BUSY_PERMILLE > BUSY_MARGIN);
const _: () = assert!(SLOW_BUSY_PERMILLE <= 1000);

/// Default [`DriverTunables::status_hold`]: two failure-detector periods.
const STATUS_HOLD: Duration = Duration::from_millis(3000);

const _: () = assert!(STATUS_HOLD.as_millis() >= MACHINE_DOWN_AFTER.as_millis());

/// Default [`DriverTunables::internal_request_timeout`]: the library
/// client's own default.
const INTERNAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Default [`DriverTunables::internal_retry_budget`]: the library client's.
const INTERNAL_RETRY_BUDGET: u32 = 4;

/// Default [`DriverTunables::internal_retry_backoff`]: the library client's.
const INTERNAL_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Default [`DriverTunables::resolve_answer_within`].
const RESOLVE_ANSWER_WITHIN: Duration = Duration::from_secs(2);

const _: () = assert!(INTERNAL_REQUEST_TIMEOUT.as_millis() > 0);
const _: () = assert!(INTERNAL_RETRY_BUDGET > 0);
const _: () = assert!(RESOLVE_ANSWER_WITHIN.as_millis() > 0);

/// Default [`DriverTunables::scrub_rate`]: 256 KiB/s, so the simulation's
/// small journals see several passes in one run.
const SCRUB_RATE: u64 = 256 * 1024;

/// The floor of [`DriverTunables::scrub_rate`].
pub const SCRUB_RATE_FLOOR: u64 = 1024;

/// Default [`DriverTunables::scrub_pass_within`]: 10 s, a few passes in one
/// simulated run.
const SCRUB_PASS_WITHIN: Duration = Duration::from_secs(10);

/// The floor of [`DriverTunables::scrub_pass_within`].
pub const SCRUB_PASS_WITHIN_FLOOR: Duration = Duration::from_secs(1);

/// Default [`DriverTunables::scrub_page`].
const SCRUB_PAGE: usize = 64;

/// Default [`DriverTunables::shutdown_within`]: 5 s, room for a handoff's
/// round trips and an election renewal on the simulator's links.
const SHUTDOWN_WITHIN: Duration = Duration::from_secs(5);

/// Default [`DriverTunables::read_only_ticks`]: four election timeouts, so
/// a store tries a write again well inside a recovery tail.
const READ_ONLY_TICKS: u64 = 4 * ELECTION_TIMEOUT_BASE;

/// Default [`DriverTunables::admission_rounds`]: far above the workload's
/// writes in flight, so admission control refuses only on a seed that draws
/// it low.
const ADMISSION_ROUNDS: u64 = 1024;

/// Default [`DriverTunables::free_bytes_floor`]: 64 KiB, below one segment
/// of the simulation's compact geometry.
const FREE_BYTES_FLOOR: u64 = 64 * 1024;

/// Default [`DriverTunables::batch_tick`]: 2 ms, a small cost in latency
/// for many writers' writes in one slot.
const BATCH_TICK: Duration = Duration::from_millis(2);

/// Default [`DriverTunables::batch_flush_records`]: the default record limit
/// of one write, so a lane flushes at the limit or at the tick.
const BATCH_FLUSH_RECORDS: u64 = 1024;

const _: () = assert!(BATCH_TICK.as_millis() >= 1);
const _: () = assert!(BATCH_FLUSH_RECORDS >= 1);
const _: () = assert!(READ_ONLY_TICKS >= 1);
const _: () = assert!(ADMISSION_ROUNDS >= 1);

const _: () = assert!(SCRUB_RATE >= SCRUB_RATE_FLOOR);
const _: () = assert!(SCRUB_PASS_WITHIN.as_millis() >= SCRUB_PASS_WITHIN_FLOOR.as_millis());
const _: () = assert!(SCRUB_PAGE >= 1);
const _: () = assert!(SCRUB_PAGE <= scrub::SCRUB_PAGE);

impl DriverTunables {
    /// The library client tunables of a machine's own clients (#457): the
    /// client's defaults, with the request and read timeouts and the
    /// re-send budget and pause these tunables name.
    ///
    /// # Panics
    ///
    /// If an assertion on its own postconditions fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn internal_client(&self) -> ClientTunables {
        let client = ClientTunables {
            request_timeout: self.internal_request_timeout,
            read_timeout: self.internal_request_timeout,
            retry_budget: self.internal_retry_budget,
            retry_backoff: self.internal_retry_backoff,
            ..ClientTunables::default()
        };
        assert_eq!(
            client.request_timeout, client.read_timeout,
            "an internal client waits as long for a read as for a write"
        );
        assert!(
            client.retry_budget >= 1,
            "an internal client re-sends an ambiguous write at least once"
        );
        client
    }
}

/// Default [`DriverTunables::election_compact_after`].
const ELECTION_COMPACT_AFTER: u64 = 16;

const _: () = assert!(ELECTION_RENEW.as_millis() > 0);
const _: () = assert!(ELECTION_LEASE.as_millis() > ELECTION_RENEW.as_millis());

/// Default [`DriverTunables::quarantine_ticks`]: eight election timeouts —
/// long enough that a journal's re-open is not a restart loop against a
/// still-faulty device, short enough that the node rejoins the journal well
/// inside a recovery tail.
const QUARANTINE_TICKS: u64 = 8 * ELECTION_TIMEOUT_BASE;

/// Base election timeout, in ticks. Each node's actual timeout is drawn
/// uniformly from `[T, 2T)` (jitter from the [`RandomProvider`], in the driver,
/// never the zero-dep core) to break the dueling-proposer livelock. `T`
/// dominates the core's heartbeat interval, so a live leader always beats before
/// a follower's election clock fires.
const ELECTION_TIMEOUT_BASE: u64 = 5;

/// Default [`DriverTunables::election_backoff_doublings`]: up to `8 × T`
/// (two to four seconds at the default tick) after three failed campaigns.
const ELECTION_BACKOFF_DOUBLINGS: u32 = 3;

/// Default take-back budget for a delegated round (#142), in re-delegations
/// — one per beat, so two election-timeout bases of ticks: long enough for a
/// proxy's fan-out, fold and `Commit` to complete over a slow link, short
/// enough that a dead proxy costs a slot a fraction of a second rather than
/// an election. Driver policy (the core only counts), and a
/// [`DriverTunables`] field so the harness can push it to its floor.
const PROXY_TAKE_BACK_RESENDS: u64 = ELECTION_TIMEOUT_BASE * 2;

/// Default retention budget of a proxy leader's open round (#142), in
/// unanswered re-fan-outs — one per beat: twice the take-back budget, so the
/// leader reclaims a stalled round first and the proxy's eviction is the
/// backstop for rounds nobody will ever answer (a compacted slot, a round
/// the leader already decided colocated). Driver policy (the core only
/// counts), and a [`DriverTunables`] field so the harness can push it to
/// its floor.
const PROXY_ROUND_RESENDS: u64 = PROXY_TAKE_BACK_RESENDS * 2;

/// Default move budget of a grid round (#533 (move a stuck round)), in
/// re-sends — one per beat: the proxy take-back's budget, so a column that
/// cannot decide costs a slot a fraction of a second rather than an
/// election. Driver policy (the core only counts), and a [`DriverTunables`]
/// field so the harness can push it to its floor.
const PARTITION_MOVE_RESENDS: u64 = PROXY_TAKE_BACK_RESENDS;

/// Default stall budget for a matchmaker-set handover, in election timeouts:
/// long enough for a slow matchmaker to answer a re-sent request, short enough
/// that a dead one does not hold the `Busy` refusal for the rest of a run.
/// Driver policy, not a protocol bound — the core reports the stall
/// (`MatchmakerReconfigurer::stalled_for`), the driver decides — and a
/// [`DriverTunables`] field rather than a constant the harness cannot move.
const RECONFIGURE_TIMEOUT_ELECTIONS: u64 = 4;

// An election timeout spans at least two beats, so a live leader always
// beats before a follower's clock fires; a quarantine outlasts an election;
// a proxy evicts a round only after its leader would have taken it back.
const _: () = assert!(ELECTION_TIMEOUT_BASE >= 2 * paros_core::HEARTBEAT_TICKS);
const _: () = assert!(QUARANTINE_TICKS > ELECTION_TIMEOUT_BASE);
const _: () = assert!(PROXY_ROUND_RESENDS > PROXY_TAKE_BACK_RESENDS);
const _: () = assert!(PARTITION_MOVE_RESENDS > 0);
const _: () = assert!(ELECTION_BACKOFF_DOUBLINGS >= 2);
const _: () = assert!(READ_RETRY_TICKS > 0);
const _: () = assert!(RECONFIGURE_TIMEOUT_ELECTIONS > 0);
