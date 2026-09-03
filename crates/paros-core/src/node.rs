//! The [`RawNode`] handle: the sans-IO Multi-Paxos state machine and the
//! `step`/`tick`/`ready`/`advance` contract.

mod acceptor;
mod catch_up_snapshot;
mod decide_apply;
mod election;
mod gc;
mod handoff;
mod helpers;
mod matchmaking;
mod reads;
mod reconfigure;
mod replication;

use std::collections::{BTreeMap, BTreeSet};

use self::gc::GcState;
pub use self::gc::GcStep;
pub use self::handoff::{
    HANDOFF_BATCH, HANDOFF_FENCE_ELECTIONS, Handoff, HandoffCounters, LeadershipOrigin,
};
pub use self::matchmaking::MatchStep;
use self::matchmaking::Matchmaking;
use self::reads::{READ_ROUND_TTL_TICKS, ReadRound};
pub use self::reconfigure::{ReconfigureRefusal, ReconfigureResult};
use self::replication::HEARTBEAT_TICKS;
use crate::acceptor::Acceptor;
use crate::matchmaker::{
    AcceptorConfig, GcRequest, MatchRefusal, MatchReply, MatchRequest, MatchmakerGeneration,
    MatchmakerId, MatchmakerSet,
};
use crate::message::Message;
use crate::proposer::Proposer;
use crate::ready::Ready;
use crate::replica::Replica;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{
    Ballot, ClientId, ClientSeq, Command, ConfigId, Control, Entry, NodeId, SessionEntry, Slot,
    Value, command_fingerprint,
};
use crate::write::WriteOp;

/// Maximum accepted records carried by one [`Message::Promise`] page.
pub const PROMISE_BATCH: usize = 64;

/// Maximum recovered or gap-fill Phase-2 rounds started in one recovery pump.
pub const LEADER_RECOVERY_BATCH: usize = 64;

/// Election timeouts a leader's blocked repair probe may stay open before the
/// leader resigns (CTRL §4.2): a leader that cannot finish recovery — e.g.
/// partitioned from the only holder of a faulty slot's value — steps down so
/// another node can try. Multiplies the driver-supplied randomized election
/// timeout, so the effective window inherits its per-seed jitter.
pub const REPAIR_TIMEOUT_ELECTIONS: u64 = 3;

/// This node's role in the cluster. A read-only view for drivers / oracles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NodeRole {
    /// Following a (believed) leader; resets its election clock on leader traffic.
    #[default]
    Follower,
    /// Ran out of election timeout, bumped its ballot, gathering a Phase-1 quorum.
    Candidate,
    /// Holds a Phase-1 quorum for its ballot; streams Phase-2 `Accept`s per slot.
    Leader,
}

/// The outcome of [`RawNode::propose`], telling the driver how to answer the
/// client. The driver acks on commit: it holds the reply for `Accepted`/
/// `Duplicate` until that slot commits, redirects on `NotLeader`, and acks
/// immediately on `Chosen`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposeResult {
    /// This node is not the leader; the client should retry the hinted node
    /// (`None` if leadership is currently unknown).
    NotLeader(Option<NodeId>),
    /// Newly admitted at this slot; ack when the slot commits.
    Accepted(Slot),
    /// A retry already in flight at this slot; ack when the slot commits.
    Duplicate(Slot),
    /// Already **applied** (inside this node's contiguous chosen prefix); the
    /// driver acks immediately (idempotent), reporting this slot.
    ///
    /// The slot is the one this client's *highest applied* command landed at —
    /// this command's own slot for the ordinary retry (a sequential client
    /// cannot be past the seq it is still retrying), and a slot at or above it
    /// otherwise. Either way it is inside the applied prefix, which is exactly
    /// what the immediate ack claims: the write is in the register the project
    /// defines. An ack naming a slot the node has not applied would be a
    /// linearizability violation, so this carries a slot rather than nothing —
    /// it makes the fast path checkable by the simulation's oracles instead of
    /// exempt from them.
    Chosen(Slot),
}

/// The outcome of [`RawNode::read_index`], telling the driver how to answer the
/// reading client: redirect on `NotLeader`, or park the reply and wait for the
/// matching [`ReadState`] to surface via [`Ready::read_states`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadIndexResult {
    /// This node is not the leader; the client should retry the hinted node
    /// (`None` if leadership is currently unknown).
    NotLeader(Option<NodeId>),
    /// The round is running; a [`ReadState`] with this call's `ctx` surfaces via
    /// [`Ready::read_states`] once confirmed (possibly in the very next batch).
    /// A round that cannot confirm (leadership lost, acks lost) surfaces
    /// nothing — the driver owns the client-facing timeout.
    Pending,
}

/// A confirmed read-index round, surfaced via [`Ready::read_states`]: at the
/// moment the round began this node was leader (a heartbeat-ack quorum at its
/// ballot proved it afterwards) and `index` was covered by the applied prefix
/// by confirmation time — the linearization point a read at `ctx` observes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadState {
    /// The driver-supplied correlation token from [`RawNode::read_index`].
    pub ctx: u64,
    /// The read index: the applied watermark the read observes (`None` = empty
    /// prefix). The node's chosen index is at or past it on confirmation.
    pub index: Option<Slot>,
}

/// The pure, synchronous, single-threaded Multi-Paxos state machine.
///
/// No I/O, no clock, no randomness. Inputs arrive via [`RawNode::step`] (peer
/// messages and tick-injected self-events), [`RawNode::tick`] (logical time),
/// and [`RawNode::propose`] (a client value). Output is drained via
/// [`RawNode::ready`] and acknowledged via [`Ready::advance`].
///
/// Stage 3 is **Multi-Paxos**: a per-slot replicated log with a stable leader.
/// A node times out (randomized election timeout supplied by the driver),
/// becomes a Candidate, runs **one** Phase 1 for its ballot over the whole log
/// suffix, and on a promise quorum becomes Leader: it re-proposes recovered
/// in-flight slots (gap fill) and then streams Phase-2 `Accept`s for fresh
/// client values. Heartbeats hold leadership; a `Nack` or a higher ballot makes
/// a node step down (the dueling-proposer livelock fix). Client requests are
/// deduplicated by `(ClientId, ClientSeq)` for at-most-once execution.
pub struct RawNode {
    /// This node's static identity, bootstrap membership, pool and matchmaker
    /// set.
    config: Config,
    /// The acceptor configuration bound to the highest ballot this node has
    /// seen registered — on a leader, the configuration its own ballot was
    /// registered with (what Phase 2 quorums are counted over); on a
    /// follower, its belief about the latest configuration (what its next
    /// campaign registers). Learned from `Prepare`/`Heartbeat` on a
    /// deployment with matchmakers; on plain Multi-Paxos it is the bootstrap
    /// configuration for the node's whole life. Never edited underneath a
    /// live ballot: a configuration is bound to a ballot, and a change is a
    /// round change ([`RawNode::reconfigure`]).
    acceptors: AcceptorConfig,
    /// The ballot `acceptors` was registered under (`Ballot::zero()` for the
    /// bootstrap configuration).
    acceptors_since: Ballot,
    /// Durable identity of the cluster configuration this node belongs to.
    config_id: ConfigId,
    /// The **acceptor** component ([`crate::acceptor::Acceptor`]): the
    /// durable promise, the per-slot accepted log, the compaction floor and
    /// the CTRL tri-state's faulty entries. Rebuilt on boot from the durable
    /// log (see [`RawNode::new`]); persisted one delta at a time through the
    /// [`WriteOp`]s it emits into this node's batch.
    acceptor: Acceptor,
    /// The **replica** component ([`crate::replica::Replica`]): the chosen
    /// log, the durable chosen index, the contiguous apply walk, the
    /// at-most-once ledger and the application repair cursor.
    replica: Replica,

    // ---- pending output buckets: filled by the protocol logic, drained by
    // ---- `ready`, cleared by `advance`.
    /// Semantic durable write deltas produced this batch, in apply order.
    pending_writes: Vec<WriteOp>,
    pending_messages: Vec<(NodeId, Message)>,
    /// Snapshot offers to serve this batch:
    /// `(to, chosen_index, ballot, config_id)`. The core decides *who* needs a
    /// snapshot and *up to where* (a below-floor catch-up request), but holds no
    /// application state, so the driver attaches the opaque snapshot bytes (from
    /// storage) and sends the [`Message::InstallSnapshot`].
    pending_snapshot_offers: Vec<(NodeId, Slot, Ballot, ConfigId)>,
    /// Read-index rounds confirmed this batch, drained via
    /// [`Ready::read_states`] after the batch's committed entries are applied.
    pending_read_states: Vec<ReadState>,
    /// `(started, gap_fills, remaining)` for this Ready's recovery chunk.
    pending_recovery_batch: Option<(usize, usize, usize)>,

    /// Logical clock, advanced by [`RawNode::tick`].
    tick_count: u64,

    // ---- leadership / election (all volatile) ----
    /// Current role.
    role: NodeRole,
    /// The node we currently believe is leader (`None` = unknown / electing).
    leader: Option<NodeId>,
    /// The ballot this node operates under as Candidate/Leader (and the highest
    /// leader ballot it has adopted as a Follower).
    ballot: Ballot,
    /// Ticks since the last leader contact (reset on `Prepare`/`Accept`/
    /// `Heartbeat`/`Commit` at a ballot `>=` ours, and on becoming Leader).
    election_elapsed: u64,
    /// Driver-supplied randomized election timeout, in ticks. `0` disables the
    /// election clock (the sentinel until the driver seeds one).
    election_timeout: u64,
    /// Set when the election clock resets (fired or stepped down); the driver
    /// reads it to feed a fresh randomized `election_timeout`. Jitter is drawn in
    /// the driver, never here (the core stays zero-dep).
    needs_election_timeout: bool,
    /// Ticks since the leader last beat. Leader-only.
    heartbeat_elapsed: u64,
    /// Fixed heartbeat interval in ticks (not randomized).
    heartbeat_timeout: u64,
    /// Monotone per-ballot beat sequence, bumped at each broadcast
    /// ([`RawNode::broadcast_heartbeat`]); reset on winning an election. Acks
    /// echo it, so a read round knows which beats prove leadership *after* it
    /// began.
    heartbeat_seq: u64,
    /// `CheckQuorum` (#95): ticks since the leader last proved it can reach an
    /// ack quorum. Leader-only, reset when the window closes with a quorum.
    quorum_elapsed: u64,
    /// `CheckQuorum`: the distinct peers (incl. self) whose ballot-matching
    /// `HeartbeatAck` or `Accepted` arrived inside the current window.
    quorum_acked_by: BTreeSet<NodeId>,
    /// Monotone count of `CheckQuorum` step-downs this incarnation, for the
    /// driver's audit report (mirrors `duplicates_suppressed`).
    quorum_lost_step_downs: u64,

    // ---- linearizable reads (all volatile: a read round carries no durable
    // ---- obligation — losing one merely fails an RPC whose reply promise dies
    // ---- with the process, and the client retries) ----
    /// The fresh-leader read fence: the highest slot the winning prepare quorum
    /// reported (`next_slot - 1` at election). Everything a previous leader may
    /// have acked sits at or below it (quorum intersection + the Prepare floor
    /// guard), so no read round confirms until the chosen prefix covers it —
    /// Raft's "no-op at term start" problem, solved by waiting instead.
    read_floor: Option<Slot>,
    /// In-flight read-index rounds, in creation order (leader only).
    read_rounds: Vec<ReadRound>,

    // ---- proposer (multi-decree) ----
    /// The proposer component: the open Phase 1, the CTRL repair probe, the
    /// in-flight Phase-2 rounds and the bounded recovery of the current
    /// leadership ([`crate::proposer`]). Volatile; dies whole with the
    /// leadership.
    proposer: Proposer,
    /// The **matchmaking phase** while a Candidate registers its ballot's
    /// configuration with the matchmakers (#120) — the campaign state that
    /// precedes `election`, and never coexists with it. `None` on a plain
    /// deployment, always.
    matchmaking: Option<Matchmaking>,
    /// Matchmaking requests to send this batch, drained via
    /// [`Ready::match_requests`]. A separate wire from `pending_messages`:
    /// the matchmaker contract is its own RPC service, spoken only by a
    /// deployment that names matchmakers.
    pending_match_requests: Vec<(MatchmakerId, MatchRequest)>,
    /// Garbage-collection requests to send this batch (#123), drained via
    /// [`Ready::gc_requests`] over the same matchmaker wire.
    pending_gc_requests: Vec<(MatchmakerId, GcRequest)>,
    /// The matchmaker set this node believes authoritative (#125): the
    /// bootstrap set at generation 0 on boot, moved forward by a refusal
    /// naming a successor, by a matchmaker-set reconfiguration this node
    /// drove, or by a reply from a later generation. Volatile: a fresh
    /// incarnation walks the successor chain from the bootstrap set again.
    /// Empty members on plain Multi-Paxos, always.
    matchmakers: MatchmakerSet,
    /// The leader's open garbage-collection campaign (#123, `node/gc.rs`).
    /// Leader-only, volatile, `None` on plain Multi-Paxos.
    gc: Option<GcState>,
    /// Monotone campaign-phase counters this incarnation, for the driver's
    /// audit report: campaigns this node declined to open because it is not
    /// a member of the configuration it would register, and leaderships it
    /// resigned once its own reconfiguration removed it from the acceptor set.
    non_member_campaigns_skipped: u64,
    non_member_step_downs: u64,
    /// The round every later campaign opens strictly above, raised by a
    /// `Stale` matchmaking refusal to the refuser's highest registered round.
    /// Volatile: a restart starts from the durable promise again. Without it
    /// a candidate refused at round `r` re-registers at `r + 1`, is refused
    /// again by the same higher registration, and leapfrogs one round per
    /// election timeout behind a rival that never has to move — the
    /// matchmaking cousin of the dueling-proposer livelock, seen in the
    /// hunt as two hundred registrations for three completed campaigns.
    round_floor: u64,
    /// Election timeouts that found a matchmaking phase still open and
    /// re-sent its requests instead of abandoning the campaign (see
    /// [`RawNode::tick`]). Observability only.
    matchmaking_timeouts: u64,
    /// Ticks the proposer's current repair probe has been open. Reset when the probe
    /// closes; drives the recovery-timeout resignation.
    repair_elapsed: u64,
    /// Monotone count of recovery-timeout step-downs this incarnation (a leader
    /// resigning because it could not finish repairing its blocked slots).
    repair_step_downs: u64,
    /// Monotone count of blocked slots resolved as Case 1 (re-proposed from a
    /// straggler's `have`) after the election closed.
    repair_case1: u64,
    /// Monotone count of blocked slots resolved as Case 2 (a full Q1 of `none`
    /// assembled from stragglers; decided `Noop`).
    repair_case2: u64,
    /// How this node came to hold its current leadership (see
    /// [`LeadershipOrigin`]). `Elected` on every non-leader.
    leadership_origin: LeadershipOrigin,
    /// Ticks a handoff-installed leadership has held an **uncovered inherited
    /// fence** (its chosen prefix still below `read_floor`). Drives the
    /// resignation that hands an unrecoverable inherited log back to an
    /// ordinary Phase 1; reset whenever the fence is covered.
    handoff_fence_elapsed: u64,
    /// Monotone cooperative-handoff counters this incarnation, for the
    /// driver's audit report.
    handoff: HandoffCounters,
    /// Next slot the leader allocates to a fresh client proposal.
    next_slot: Slot,
    /// How many undecided holes this node filled with a [`Control::Noop`] when it
    /// won its *current* leadership (0 until it wins one, and re-set at each
    /// election). Purely observational: the driver reads it on the transition to
    /// Leader and surfaces it, so the simulation can prove the gap-fill path is
    /// genuinely reached rather than merely present.
    election_gap_fills: u64,
}

impl RawNode {
    /// Construct from a read-only [`Storage`] by reading durable state back in.
    /// Bootstrap and restart share this path. The volatile dedup tables
    /// (`applied_seq`, `inflight`) and the `chosen` map are rebuilt from the
    /// durable `accepted` log and `chosen_index`.
    ///
    /// # Panics
    ///
    /// If the configuration is malformed (membership not sorted/deduplicated,
    /// or missing this node's own id) or the durable state violates the write
    /// ordering contract (a floor past the chosen prefix). A broken invariant
    /// here means corrupted storage or a broken storage implementation;
    /// crashing beats running on it.
    #[allow(clippy::too_many_lines)]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all))]
    pub fn new<S: Storage>(storage: &S) -> Self {
        let (hard_state, config) = storage.initial_state();
        // Config shape: quorum arithmetic and broadcast both assume a strictly
        // sorted, deduplicated membership. A duplicated peer silently inflates
        // the quorum; a missing self silently deflates it.
        assert!(
            !config.peers.is_empty(),
            "the bootstrap membership names at least one acceptor"
        );
        assert!(
            config.peers.windows(2).all(|w| w[0] < w[1]),
            "membership is sorted and deduplicated"
        );
        assert!(
            config.nodes.windows(2).all(|w| w[0] < w[1]),
            "the node pool is sorted and deduplicated"
        );
        assert!(
            config.matchmakers.windows(2).all(|w| w[0] < w[1]),
            "the matchmaker set is sorted and deduplicated"
        );
        // The bootstrap membership is drawn from the pool, and this node is
        // in the pool. On a plain deployment (no `nodes`, no matchmakers) the
        // pool *is* the membership, so this is today's "membership includes
        // this node's own id": a node outside the acceptor set exists only
        // where a reconfiguration could add or remove it.
        assert!(
            config
                .peers
                .iter()
                .all(|p| config.pool().binary_search(p).is_ok()),
            "the bootstrap membership is drawn from the node pool"
        );
        assert!(
            config.pool().binary_search(&config.id).is_ok(),
            "the node pool includes this node's own id"
        );
        if !config.has_matchmakers() {
            assert!(
                config.peers.binary_search(&config.id).is_ok(),
                "a plain deployment's membership includes this node's own id"
            );
        }
        let acceptors = AcceptorConfig::new(config.peers.clone(), config.quorum_system);
        let matchmakers = MatchmakerSet::new(MatchmakerGeneration(0), config.matchmakers.clone());
        assert!(
            config
                .matchmakers
                .iter()
                .all(|m| config.matchmaker_pool().binary_search(m).is_ok()),
            "the bootstrap matchmaker set is drawn from the matchmaker pool"
        );
        let ballot = hard_state.max_promised_ballot;

        // Rebuild the working accepted log by scanning the durable per-slot log
        // (first_slot..=last_slot); gaps read back as `None` and are skipped.
        let mut accepted: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let first_slot = storage.first_slot();
        let (first, last) = (first_slot.0, storage.last_slot().0);
        for s in first..=last {
            if let Some(record) = storage.accepted(Slot(s)) {
                // Boot-side pair of the write-side ordering (`on_accept` /
                // `start_accept_round` raise the promise in the same batch as
                // the append): no durable accept ever outranks the durable
                // promise. This scan is already O(N), so the per-record check
                // stays a hard assert — crash beats corruption.
                assert!(
                    record.0 <= hard_state.max_promised_ballot,
                    "the durable promise dominates every accepted record"
                );
                accepted.insert(Slot(s), record);
            }
        }

        let replica = Replica::from_boot(
            hard_state.chosen_index,
            storage.sealed_sessions(),
            &accepted,
        );

        // The boot scan's recoverable faulty entries (Stage 8): value lost,
        // identity known. They are *records this node accepted*, so they bound
        // `next_slot` exactly like readable records; they are simply unreadable
        // and reported as `faulty` instead of `have`. Nothing below the floor is
        // retained, and a slot never appears in both maps.
        let mut faulty: BTreeMap<Slot, Ballot> = BTreeMap::new();
        for (slot, ballot) in storage.faulty_entries() {
            if slot < first_slot {
                continue;
            }
            assert!(
                !accepted.contains_key(&slot),
                "a faulty entry is never also a readable accepted record"
            );
            // Same boot-side promise domination as the readable scan above: a
            // faulty entry's identity ballot was a real accepted ballot once,
            // so the durable promise flushed with it still covers it.
            assert!(
                ballot <= hard_state.max_promised_ballot,
                "the durable promise dominates every faulty record"
            );
            faulty.insert(slot, ballot);
        }

        // Next free slot: one past the highest accepted entry (readable or
        // faulty), or (when the log is empty, e.g. fully truncated) one past
        // the durable chosen index.
        let first_unchosen = hard_state.chosen_index.map_or(Slot(0), |ci| Slot(ci.0 + 1));
        let next_slot = accepted
            .keys()
            .chain(faulty.keys())
            .max()
            .map_or(first_unchosen, |s| Slot(s.0 + 1))
            .max(first_unchosen);
        // Trust-boundary re-assertion of the durable write ordering: a flushed
        // floor never outruns the flushed chosen index (only chosen slots are
        // truncated), and a durable chosen index implies its accept was flushed
        // in the same or an earlier sync, so the rebuilt `next_slot` sits at or
        // past the first unchosen slot.
        assert!(
            first_slot <= first_unchosen,
            "the durable floor never outruns the durable chosen index"
        );
        assert!(
            next_slot >= first_unchosen,
            "the rebuilt next slot never falls inside the chosen prefix"
        );
        // Completeness of the retained chosen prefix: every slot between the
        // floor and the first unchosen slot must read back as *some* durable
        // record — readable, or faulty-with-identity. A silent hole (record
        // fully lost, identity too) boots into a permanent wedge: catch-up
        // replay stops at the hole it cannot attribute, and campaigns start at
        // `min(first_faulty, first_unchosen)`, which never covers a slot no
        // record names. The boot scan is already O(N), so this stays a hard
        // per-slot assert — crash beats corruption.
        for s in first_slot.0..first_unchosen.0 {
            assert!(
                accepted.contains_key(&Slot(s)) || faulty.contains_key(&Slot(s)),
                "every retained slot below the chosen prefix has a durable record"
            );
        }

        let node = Self {
            config,
            acceptors,
            acceptors_since: Ballot::zero(),
            config_id: hard_state.config_id,
            acceptor: Acceptor::new(hard_state.max_promised_ballot, accepted, first_slot, faulty),
            replica,
            pending_writes: Vec::new(),
            pending_messages: Vec::new(),
            pending_snapshot_offers: Vec::new(),
            pending_read_states: Vec::new(),
            pending_recovery_batch: None,
            tick_count: 0,
            role: NodeRole::Follower,
            leader: None,
            ballot,
            election_elapsed: 0,
            election_timeout: 0,
            needs_election_timeout: true,
            heartbeat_elapsed: 0,
            heartbeat_timeout: HEARTBEAT_TICKS,
            heartbeat_seq: 0,
            quorum_elapsed: 0,
            quorum_acked_by: BTreeSet::new(),
            quorum_lost_step_downs: 0,
            read_floor: None,
            read_rounds: Vec::new(),
            proposer: Proposer::new(),
            matchmaking: None,
            pending_match_requests: Vec::new(),
            pending_gc_requests: Vec::new(),
            matchmakers,
            gc: None,
            non_member_campaigns_skipped: 0,
            non_member_step_downs: 0,
            round_floor: 0,
            matchmaking_timeouts: 0,
            repair_elapsed: 0,
            repair_step_downs: 0,
            repair_case1: 0,
            repair_case2: 0,
            leadership_origin: LeadershipOrigin::Elected,
            handoff_fence_elapsed: 0,
            handoff: HandoffCounters::default(),
            next_slot,
            election_gap_fills: 0,
        };
        node.assert_invariants();
        node
    }

    /// Assert every cross-field invariant of the node's volatile state. All
    /// checks are O(1) or O(log n) (min-key probes), so this runs
    /// unconditionally — TigerBeetle-style — at boot and at the exit of every
    /// public mutating entry point.
    #[allow(clippy::too_many_lines)]
    fn assert_invariants(&self) {
        // Ordering chain: only chosen slots are ever dropped, so the compaction
        // floor never passes the first unchosen slot.
        assert!(
            self.acceptor.first_slot() <= self.first_unchosen(),
            "the compaction floor never outruns the chosen prefix"
        );
        // The replica's and the proposer's own maps against the acceptor's
        // floor.
        self.replica.assert_invariants(self.acceptor.first_slot());
        self.proposer.assert_invariants();
        // The cross-role couplings against the acceptor's floor: a compaction
        // and a snapshot install both retain the rounds above the floor they
        // raise, so an in-flight Phase-2 round below it would address a slot
        // whose record is gone.
        assert!(
            self.proposer
                .rounds()
                .keys()
                .next()
                .is_none_or(|s| *s >= self.acceptor.first_slot()),
            "no in-flight round survives below the compaction floor"
        );
        // Role couplings. Note "leader ballot >= own promise" is deliberately
        // NOT a global invariant: a still-Leader node can learn a higher-ballot
        // `Commit` (raising its promise via `mark_chosen`) before any deposing
        // message arrives — `start_accept_round`'s self-accept guard is the
        // designed defense. It holds only for a *fresh* leader
        // (see `try_become_leader`).
        // The deployment couplings: plain Multi-Paxos never matchmakes and
        // never leaves its bootstrap configuration; a matchmaker deployment
        // runs Phase 2 under a configuration drawn from the pool.
        if self.config.has_matchmakers() {
            assert!(
                !self.matchmakers.members.is_empty(),
                "a matchmaker deployment always believes in a matchmaker set"
            );
        } else {
            assert!(
                self.matchmaking.is_none(),
                "a plain deployment never opens a matchmaking phase"
            );
            assert!(
                self.acceptors.members == self.config.peers,
                "a plain deployment keeps its bootstrap configuration"
            );
            assert!(
                self.acceptors_since == Ballot::zero(),
                "a plain deployment's configuration is bound to no ballot"
            );
            assert!(
                self.matchmakers.members.is_empty(),
                "a plain deployment names no matchmaker set"
            );
            assert!(
                self.gc.is_none(),
                "a plain deployment never opens a GC campaign"
            );
        }
        if self.gc.is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds an open GC campaign"
            );
        }
        assert!(
            self.acceptors.members.iter().all(|m| self.in_pool(*m)),
            "the active configuration is drawn from the node pool"
        );
        match self.role {
            NodeRole::Leader => {
                assert!(
                    self.proposer.election().is_none(),
                    "a leader has no open campaign"
                );
                assert!(
                    self.matchmaking.is_none(),
                    "a leader has no open matchmaking phase"
                );
                assert!(
                    self.leader == Some(self.config.id),
                    "a leader knows itself as leader"
                );
                // Whose node id the operating ballot names is exactly what
                // separates the two leadership origins — an elected leader owns
                // its ballot, a handoff leader is exercising a predecessor's.
                match self.leadership_origin {
                    LeadershipOrigin::Elected => assert!(
                        self.ballot.node == self.config.id,
                        "an elected leader's ballot names its own node"
                    ),
                    LeadershipOrigin::Handoff { from } => {
                        // The ballot names whoever *minted* it, which after a
                        // chain of handoffs is neither this node nor its
                        // immediate predecessor — so only the predecessor is
                        // pinned here, and it is always someone else.
                        assert!(
                            from != self.config.id,
                            "a handoff leader inherited its authority from another node"
                        );
                        assert!(
                            self.in_pool(from),
                            "a handoff leader inherited from a pooled node"
                        );
                    }
                }
                // The #67/#88 allocator bound, gated exactly like the note
                // above: a still-Leader that learned a higher-ballot `Commit`
                // (or replayed catch-up decided past it) can see the chosen
                // prefix pass its allocator before any deposing message
                // arrives — but while its ballot still covers its own promise,
                // quorum intersection guarantees the winning Phase 1 reported
                // everything decided, so the allocator sits at or past the
                // prefix.
                if self.ballot >= self.acceptor.promised() {
                    assert!(
                        self.next_slot >= self.first_unchosen(),
                        "a leader's next slot never falls inside the chosen prefix"
                    );
                }
                assert!(
                    self.proposer
                        .rounds()
                        .keys()
                        .next_back()
                        .is_none_or(|s| *s < self.next_slot),
                    "a leader never allocates at or below an in-flight round"
                );
                // Every in-flight round runs at the leadership ballot: rounds
                // are opened only by this leader, and every promise-raising
                // path that could strand one demotes (clearing `proposer`)
                // first. O(N) structural, always-on by choice.
                assert!(
                    self.proposer
                        .rounds()
                        .values()
                        .all(|p| p.ballot() == self.ballot),
                    "a leader's in-flight rounds all run at its own ballot"
                );
            }
            NodeRole::Candidate => {
                // Exactly one campaign phase is open: matchmaking (the
                // registration round trip, #120) or Phase 1 — never both,
                // never neither. The boundary between them is
                // `start_phase1`.
                assert!(
                    self.proposer.election().is_some() != self.matchmaking.is_some(),
                    "a candidate holds exactly one open campaign phase"
                );
                assert!(
                    self.proposer
                        .election()
                        .is_none_or(|e| e.ballot() == self.ballot),
                    "a candidate's campaign runs at its own operating ballot"
                );
                assert!(
                    self.matchmaking
                        .as_ref()
                        .is_none_or(|m| m.ballot == self.ballot),
                    "a candidate's matchmaking runs at its own operating ballot"
                );
                assert!(
                    self.ballot.node == self.config.id,
                    "an operating ballot names its own node"
                );
            }
            NodeRole::Follower => {
                assert!(
                    self.proposer.election().is_none(),
                    "a follower has no open campaign"
                );
                assert!(
                    self.matchmaking.is_none(),
                    "a follower has no open matchmaking phase"
                );
            }
        }
        // A leadership origin is leadership state: it is cleared with the rest
        // of it (`become_follower`), so only a leader ever carries a handoff.
        if self.role != NodeRole::Leader {
            assert!(
                self.leadership_origin == LeadershipOrigin::Elected,
                "only a leader carries an inherited leadership origin"
            );
        }
        // Volatile leadership state exists only on a leader.
        if !self.proposer.rounds().is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds in-flight accept rounds"
            );
        }
        if !self.read_rounds.is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds pending read rounds"
            );
        }
        if self.proposer.recovery().is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds deferred recovery work"
            );
        }
        if self.proposer.probe().is_some() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds an open repair probe"
            );
        }
    }

    /// The single input entry point: every stimulus is a [`Message`], routed by
    /// variant and role. Tick-injected self-events (`CheckLeader`/`Heartbeat`)
    /// enter here too. A ballot-bearing message naming a configuration other
    /// than this node's durable configuration is ignored whole.
    ///
    /// # Panics
    ///
    /// Panics if processing exposes a broken internal invariant (a programmer
    /// error, never an operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn step(&mut self, msg: Message) {
        // Wire guard, not an assert: a foreign configuration id is an operating
        // condition (a stale peer, a misconfigured cluster, a message from a
        // past reconfiguration), never a local invariant. Quorum arithmetic is
        // meaningless across configurations, so ignore the message wholesale —
        // no reply, no state change; the sender's own configuration machinery
        // owns healing the mismatch.
        if msg
            .config_id()
            .is_some_and(|config_id| config_id != self.config_id)
        {
            return;
        }
        match msg {
            Message::Prepare {
                from,
                ballot,
                from_slot,
                config,
                ..
            } => self.on_prepare(from, ballot, from_slot, config),
            Message::Promise {
                from,
                ballot,
                from_slot,
                accepted,
                faulty,
                next_from_slot,
                ..
            } => self.on_promise(from, ballot, from_slot, accepted, faulty, next_from_slot),
            Message::Accept {
                from,
                ballot,
                slot,
                command,
                ..
            } => self.on_accept(from, ballot, slot, command),
            Message::Accepted {
                from,
                ballot,
                slot,
                vhash,
                ..
            } => self.on_accepted(from, ballot, slot, vhash),
            Message::Nack {
                from,
                ballot,
                promised,
                slot,
                ..
            } => self.on_nack(from, ballot, promised, slot),
            Message::Commit {
                ballot,
                slot,
                command,
                ..
            } => self.on_commit(ballot, slot, &command),
            Message::CatchUpRequest { from, from_slot } => self.on_catchup_request(from, from_slot),
            Message::CatchUpResponse { entries, .. } => self.on_catchup_response(entries),
            Message::InstallSnapshot {
                ballot,
                chosen_index,
                snapshot,
                sessions,
                ..
            } => self.on_install_snapshot(ballot, chosen_index, snapshot, sessions),
            Message::Relinquish {
                from,
                to,
                ballot,
                from_slot,
                next_slot,
                decided,
                pending,
                config,
                ..
            } => {
                self.on_relinquish(
                    from, to, ballot, from_slot, next_slot, decided, pending, config,
                );
            }
            Message::CheckLeader { .. } => self.on_check_leader(),
            Message::Heartbeat {
                from,
                ballot,
                commit,
                seq,
                config,
                ..
            } => self.on_heartbeat(from, ballot, commit, seq, config),
            Message::HeartbeatAck {
                from,
                ballot,
                seq,
                chosen,
                ..
            } => {
                self.on_heartbeat_ack(from, ballot, seq, chosen);
            }
            // Driver-terminal snapshot-repair traffic (CTRL §3.5): the
            // driver's repair layer owns these end to end and normally
            // intercepts them before `step`. Consensus state never depends on
            // snapshot custody, so a message that does reach the core is
            // deliberately ignored rather than an error.
            Message::SnapAck { .. }
            | Message::SnapChunkRequest { .. }
            | Message::SnapChunkResponse { .. } => {}
        }
        self.assert_invariants();
    }

    /// Client entry point: try to get `value` chosen, deduplicated by
    /// `(client, seq)`. Only the leader admits proposals; a non-leader returns
    /// [`ProposeResult::NotLeader`] with a redirect hint.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, client = client.0, seq = seq.0)))]
    pub fn propose(&mut self, client: ClientId, seq: ClientSeq, value: Value) -> ProposeResult {
        if self.role != NodeRole::Leader {
            return ProposeResult::NotLeader(self.leader);
        }
        // The applied ledger outranks the in-flight table: once an identity has
        // applied, the honest answer is `Chosen` at its first slot, even while a
        // #94 duplicate of it sits chosen-but-unapplied at a later slot (that
        // slot will suppress to a no-op at apply, so a reply parked on it would
        // hang to the client's deadline). While an application repair is open,
        // the ledger's slots are chosen but not yet re-applied here, so the
        // immediate `Chosen` ack would name a slot outside the applied prefix —
        // fall through to the honest slower paths instead.
        if self.replica.app_repair().is_none()
            && let Some(at) = self.replica.applied_at(client, seq)
        {
            // What an immediate ack claims, restated at the reply: the slot is
            // inside this node's applied prefix (the ledger is written only by
            // the contiguous walk, the boot rebuild, and sealed records below
            // the floor), and if its record is still retained, it is this very
            // identity's command — never a `Noop` or another client's write.
            assert!(
                at < self.first_unchosen(),
                "an immediate Chosen names a slot inside the applied prefix"
            );
            assert!(
                match self.replica.chosen_at(at) {
                    None => true,
                    Some(Command::User(applied)) => applied.client == client && applied.seq == seq,
                    Some(Command::Control(_)) => false,
                },
                "the applied ledger points at the identity's own chosen command"
            );
            return ProposeResult::Chosen(at);
        }
        if let Some(slot) = self.replica.inflight_at(client, seq) {
            return ProposeResult::Duplicate(slot);
        }
        let slot = self.next_slot;
        self.next_slot = Slot(slot.0 + 1);
        let entry = Entry { client, seq, value };
        self.replica.track_inflight(client, seq, slot);
        self.start_accept_round(slot, Command::User(entry));
        self.assert_invariants();
        ProposeResult::Accepted(slot)
    }

    /// Leader entry point for a **control command**: get `control` chosen into the
    /// next log slot by ordinary Paxos. Only the leader admits it; a non-leader
    /// returns [`ProposeResult::NotLeader`] with a redirect hint.
    ///
    /// A control command carries no `(client, seq)` and so is never deduplicated;
    /// each proposal takes a fresh slot. It is a normal Phase-2 round from the
    /// acceptors' point of view (they store it opaquely, exactly like a client
    /// entry). Its *effect* — for [`Control::Truncate`], dropping the log prefix —
    /// is applied lazily by every node when the slot enters its contiguous chosen
    /// prefix (see [`RawNode::advance_chosen_index`]).
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, control = ?control)))]
    pub fn propose_control(&mut self, control: Control) -> ProposeResult {
        if self.role != NodeRole::Leader {
            return ProposeResult::NotLeader(self.leader);
        }
        let slot = self.next_slot;
        self.next_slot = Slot(slot.0 + 1);
        self.start_accept_round(slot, Command::Control(control));
        self.assert_invariants();
        ProposeResult::Accepted(slot)
    }

    /// Leader entry point for a **decided snapshot point** (#101, CTRL §3.5):
    /// propose a [`Control::Snap`] marker into the next slot, with `at_index`
    /// bound to exactly that slot **by construction** — Paxos never moves an
    /// accepted command between slots, so a decided marker always describes
    /// its own position. A non-leader returns [`ProposeResult::NotLeader`].
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn propose_snap_marker(&mut self) -> ProposeResult {
        if self.role != NodeRole::Leader {
            return ProposeResult::NotLeader(self.leader);
        }
        let slot = self.next_slot;
        self.next_slot = Slot(slot.0 + 1);
        self.start_accept_round(slot, Command::Control(Control::Snap { at_index: slot }));
        self.assert_invariants();
        ProposeResult::Accepted(slot)
    }

    /// Leader entry point for a **linearizable read**: capture the current
    /// applied watermark as the read index and start a heartbeat-ack quorum
    /// round to confirm this node is still leader — no log write. The confirmed
    /// round surfaces as a [`ReadState`] carrying `ctx` via
    /// [`Ready::read_states`], once a quorum has acked a beat broadcast at or
    /// after this call **and** the chosen prefix covers the captured index
    /// (the fresh-leader fence, see the `read_floor` field). A non-leader
    /// returns [`ReadIndexResult::NotLeader`] with a redirect hint.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition): read rounds must confirm in creation order.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, ctx)))]
    pub fn read_index(&mut self, ctx: u64) -> ReadIndexResult {
        if self.role != NodeRole::Leader {
            return ReadIndexResult::NotLeader(self.leader);
        }
        // The fence dominates: a fresh leader must not serve below the highest
        // slot its prepare quorum reported, even while its own chosen prefix
        // still lags the recovered suffix.
        let index = self.replica.chosen_index().max(self.read_floor);
        // Beat immediately (rather than waiting for the next tick) so the
        // round's confirmation costs one network round trip, not a tick.
        self.broadcast_heartbeat();
        let mut acked_by = BTreeSet::new();
        // `QuorumSystem::Majority` is the load-bearing reason the leader may
        // count its own real acceptor vote unconditionally: a stale leader can
        // collect at most `q - 1` peer acks once an intersecting majority has
        // promised higher. A future asymmetric quorum system must replace this
        // cardinality check and explicit own vote with read-quorum membership.
        // A leader outside its own configuration has no acceptor vote to cast.
        if self.is_acceptor() {
            acked_by.insert(self.config.id);
        }
        self.read_rounds.push(ReadRound {
            ctx,
            index,
            required_seq: self.heartbeat_seq,
            acked_by,
            created_tick: self.tick_count,
        });
        // `try_confirm_reads` front-scans on the premise that creation order is
        // monotone in both the index and the required beat; pin it at the only
        // place a round is created (O(1): the last two entries).
        if let [.., prev, last] = self.read_rounds.as_slice() {
            assert!(
                prev.index <= last.index,
                "read rounds are created with monotone indexes"
            );
            assert!(
                prev.required_seq <= last.required_seq,
                "read rounds are created with monotone required beats"
            );
        }
        // A single-node cluster is its own quorum: confirm in this same batch.
        self.try_confirm_reads();
        self.assert_invariants();
        ReadIndexResult::Pending
    }

    /// Application-driven log compaction: drop every retained slot at or below
    /// `up_to`, raising the truncation floor. Returns the new floor (the first
    /// slot still retained).
    ///
    /// `up_to` is the last slot the application permits dropping (inclusive); the
    /// floor stored in the log is the first slot *retained*. The request is
    /// clamped to the contiguous chosen prefix (`up_to.min(chosen_index)`): a slot
    /// that is not yet chosen is never dropped, so nothing undecided is lost.
    /// Clamping makes the call safe to over-request ("drop as much as you may, up
    /// to N") and idempotent (a floor never moves backward). With nothing chosen,
    /// or when the floor would not rise, it is a no-op that emits no
    /// [`WriteOp::Truncate`].
    ///
    /// Truncation does **not** shrink the at-most-once dedup window: the ledger
    /// records whose slots this call drops are *sealed* — emitted on the
    /// [`WriteOp::Truncate`] for durable persistence and read back through
    /// [`Storage::sealed_sessions`] on the next boot — so a restart recognizes a
    /// truncated `(client, seq)` exactly like a node that never restarted, and
    /// the #94 duplicate-suppression decision stays cluster-consistent. (The
    /// sealed ledger grows with distinct client identities for the lifetime of
    /// the cluster; bounding it needs a client-session expiry policy, which is
    /// out of scope here.)
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition): the floor must rise monotonically and stay
    /// clamped inside the chosen prefix.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, up_to = up_to.0)))]
    pub fn compact(&mut self, up_to: Slot) -> Slot {
        let Some(ci) = self.replica.chosen_index() else {
            return self.acceptor.first_slot();
        };
        let mut highest_drop = up_to.min(ci);
        // An open application repair pins the floor: truncating at or past the
        // repair cursor would drop the very records the catch-up heal is about
        // to re-emit, converting a one-slot repair into a snapshot transfer (or
        // an unrecoverable wait). The floor resumes rising once the repair
        // closes; a decided `Truncate` is idempotent over-asking by design.
        if let Some(pending) = self.replica.app_repair() {
            let Some(cap) = pending.0.checked_sub(1) else {
                return self.acceptor.first_slot();
            };
            highest_drop = highest_drop.min(Slot(cap));
        }
        let old_floor = self.acceptor.first_slot();
        let first = Slot(highest_drop.0 + 1).max(old_floor);
        if first <= old_floor {
            return old_floor;
        }
        // Seal from the *ledger*, not from the dropped `chosen` range (see
        // `Replica::seal`). Only the delta is sealed — records below the old
        // floor were sealed by the truncation (or install) that dropped them.
        let sealed: Vec<SessionEntry> = self.replica.seal(old_floor, first);
        // A faulty entry below the floor is superseded by the compacted state
        // (only chosen slots are dropped, and truncation is decided over the
        // applied prefix): custodianship moved into the application snapshot.
        self.acceptor
            .truncate(first, sealed, &mut self.pending_writes);
        self.replica.truncate(first);
        self.proposer.retain_rounds_from(first);
        // Postconditions: the floor strictly rose (the no-op path returned
        // above) and stayed clamped inside the chosen prefix.
        assert!(
            self.acceptor.first_slot() > old_floor,
            "compaction raised the floor"
        );
        assert!(
            self.acceptor.first_slot() <= self.first_unchosen(),
            "compaction never drops an undecided slot"
        );
        // The cap above, restated as what it protects: an open application
        // repair still needs every decided record from its cursor up, so the
        // floor stops below the cursor (the truncate-before-heal bug class).
        assert!(
            self.replica
                .app_repair()
                .is_none_or(|cursor| self.acceptor.first_slot() <= cursor),
            "compaction never drops a slot an open application repair still needs"
        );
        self.assert_invariants();
        self.acceptor.first_slot()
    }

    /// Advance logical time by one tick, synthesizing `CheckLeader`/`Heartbeat`
    /// self-events when the election / heartbeat counters cross their thresholds.
    ///
    /// Re-sending a leader's still-pending `Accept`s is deliberately *not* part of
    /// this: it is a separate decision on the same cadence, so the driver can skip
    /// it (see [`RawNode::resend_pending`]).
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn tick(&mut self) {
        self.tick_count += 1;
        let me = self.config.id;
        if self.role == NodeRole::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.heartbeat_timeout {
                self.heartbeat_elapsed = 0;
                self.step(Message::Heartbeat {
                    config_id: self.config_id,
                    from: me,
                    ballot: self.ballot,
                    commit: self.replica.chosen_index(),
                    seq: 0,
                    config: None,
                });
            }
            // GC read rounds that outlived their TTL (lost acks, an unreachable
            // quorum). No re-broadcast logic is needed for the live ones: every
            // leader tick already broadcasts a fresh, higher-seq beat whose acks
            // confirm all older pending rounds.
            let now = self.tick_count;
            self.read_rounds
                .retain(|r| now.saturating_sub(r.created_tick) <= READ_ROUND_TTL_TICKS);
            // CheckQuorum (#95): a leader must re-prove, once per election
            // timeout, that an ack quorum can still reach it. Without this, an
            // idle leader cut off from its quorum stays Leader forever — its
            // election clock is frozen (the branch below runs only for
            // non-leaders), below-promise beats are ignored unacked rather than
            // Nacked, and an idle leader emits no `Accept`s whose Nack could
            // demote it — while it keeps admitting proposals into a stale
            // suffix for the whole partition, feeding #94's double-apply. The
            // window is the same length as the election timeout (etcd-raft's
            // CheckQuorum), so a demoted leader's peers are already eligible to
            // campaign by the time it steps down. Every beat is acked by every
            // reachable follower each tick, so a healthy leader trivially
            // refills the window.
            if self.election_timeout != 0 {
                self.quorum_elapsed += 1;
                if self.quorum_elapsed >= self.election_timeout {
                    if self.acceptors.has_quorum(&self.quorum_acked_by) {
                        self.quorum_elapsed = 0;
                        self.quorum_acked_by.clear();
                        if self.is_acceptor() {
                            self.quorum_acked_by.insert(me);
                        }
                    } else {
                        self.quorum_lost_step_downs += 1;
                        self.become_follower(None);
                    }
                }
            }
            // A leader its own reconfiguration removed from the acceptor set
            // (#122): it drives the change to completion — its inherited
            // rounds decided, its recovery and repair closed — and then
            // resigns, so an ordinary election lands leadership inside the
            // new configuration (a node campaigns only as a member).
            if self.role == NodeRole::Leader
                && !self.is_acceptor()
                && self.proposer.recovery().is_none()
                && self.proposer.probe().is_none()
                && self.proposer.rounds().is_empty()
            {
                self.non_member_step_downs = self.non_member_step_downs.saturating_add(1);
                self.become_follower(None);
            }
        } else {
            self.election_elapsed += 1;
            if self.election_timeout != 0 && self.election_elapsed >= self.election_timeout {
                self.election_elapsed = 0;
                self.needs_election_timeout = true;
                if self.matchmaking.is_some() {
                    // A campaign still waiting on its matchmakers is not
                    // abandoned by the clock: its ballot is promised and
                    // registered, and only a refusal or a higher ballot on
                    // the wire retires it. The timeout is
                    // the retry cadence — re-ask every matchmaker that has
                    // not answered — never a new round. Abandoning here made
                    // a matchmaker link slower than one election timeout an
                    // unwinnable deployment: every campaign re-registered a
                    // round higher and none ever reached Phase 1 (the hunt
                    // saw 203 registrations at one matchmaker for 3
                    // completed campaigns and no leader in a 50 s tail).
                    let ballot = self.ballot;
                    self.matchmaking_timeouts = self.matchmaking_timeouts.saturating_add(1);
                    self.resend_matchmaking();
                    // Postconditions: the clock moved nothing — same ballot,
                    // same open phase, still a candidate — and only re-asked.
                    assert!(
                        self.ballot == ballot,
                        "an election timeout never moves a pending matchmaking's ballot"
                    );
                    assert!(
                        self.role == NodeRole::Candidate && self.matchmaking.is_some(),
                        "an election timeout keeps a pending matchmaking open"
                    );
                    assert!(
                        self.proposer.election().is_none(),
                        "a re-asked matchmaking opens no Phase 1"
                    );
                } else {
                    self.step(Message::CheckLeader { from: me });
                }
            }
        }
        self.tick_handoff_fence();
        self.tick_repair();
        // The GC preconditions can become true without a message (the last
        // inherited round decided on this tick's re-send): re-check per tick.
        self.try_gc();
        self.assert_invariants();
    }

    /// Per-tick repair upkeep (Stage 8): drive the leader's open repair probe
    /// (straggler re-query + the CTRL §4.2 recovery-timeout resignation) and
    /// pull the application repair range from peers.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    fn tick_repair(&mut self) {
        // The leader's blocked-slot probe: re-send `Prepare` at our ballot to
        // every peer that has not yet answered its full suffix, once per tick
        // (the heartbeat cadence — a straggler that was down or partitioned
        // when the campaign's Prepare went out only ever answers a re-send). A
        // probe that stays blocked for a full recovery timeout resigns: another
        // node — possibly one holding the missing copy — gets to try.
        if self.role == NodeRole::Leader && self.proposer.probe().is_some() {
            self.repair_elapsed += 1;
            let timeout = self
                .election_timeout
                .saturating_mul(REPAIR_TIMEOUT_ELECTIONS);
            if self.election_timeout != 0 && self.repair_elapsed >= timeout {
                self.repair_step_downs += 1;
                self.become_follower(None);
            } else {
                let (ballot, from_slot, unanswered) = {
                    let probe = self.proposer.probe().expect("checked above");
                    // The stragglers are the members of the prior
                    // configurations the election covered — the Phase-1
                    // addressee union — that have not answered their full
                    // suffix.
                    (
                        probe.ballot(),
                        probe.from_slot(),
                        probe.stragglers(self.config.id),
                    )
                };
                let config = self.phase1_wire_config();
                for to in unanswered {
                    self.pending_messages.push((
                        to,
                        Message::Prepare {
                            config_id: self.config_id,
                            from: self.config.id,
                            ballot,
                            from_slot,
                            config: config.clone(),
                        },
                    ));
                }
            }
        }
        // The application repair pull: ask every peer for the decided range
        // from the cursor. A peer that still holds the slots serves a
        // catch-up replay; one that truncated past them offers a snapshot.
        // Once per tick — the same cadence heartbeat-driven catch-up uses.
        if let Some(from_slot) = self.replica.app_repair() {
            self.broadcast(&Message::CatchUpRequest {
                from: self.config.id,
                from_slot,
            });
        } else if let Some(first_faulty) = self
            .acceptor
            .faulty()
            .keys()
            .next()
            .copied()
            .filter(|slot| *slot < self.first_unchosen())
        {
            // A faulty **chosen** record whose effect the application already
            // holds still leaves a hole in the servable log (catch-up replay
            // stops at it — per-slot attribution). Pull the decided range from
            // peers so the record itself heals; a peer that has it chosen
            // serves it, and this node's own next election covers it either
            // way (the campaign range starts at the first faulty slot).
            self.broadcast(&Message::CatchUpRequest {
                from: self.config.id,
                from_slot: first_faulty,
            });
        }
    }

    /// Re-broadcast a fair bounded page of this leader's in-flight `Accept`
    /// rounds. A no-op on a node that is not the leader, and on a leader with
    /// nothing pending.
    ///
    /// **The driver is expected to call this on each heartbeat beat**, right after
    /// [`RawNode::tick`] — that is what lets a peer that lost the original
    /// `Accept` (or was down when it went out) catch up without waiting for an
    /// election.
    ///
    /// **Skipping a call is always safe.** Re-sending is pure optimization:
    /// nothing in Paxos safety depends on it, because the round's first broadcast
    /// already went out and a round that never gathers a quorum is simply
    /// *undecided*, which is a state the protocol is built to survive. A skipped
    /// round stalls until a later call re-sends it, or until an election recovers
    /// it. That is precisely why this is a *method* rather than something `tick`
    /// does implicitly: the decision to skip is the one whose rare omission makes
    /// the #54 election hole reachable — an undecided slot sitting *below* a
    /// decided one, which no environmental fault can produce on its own (a
    /// partition takes slots away in contiguous runs, never one here and one
    /// there) and which the `Control::Noop` gap fill exists to close. The
    /// deterministic simulation drives exactly that by skipping calls; production
    /// never skips.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn resend_pending(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        let me = self.config.id;
        let pending = self.proposer.resend_page();
        for (slot, ballot, command) in pending {
            self.broadcast_acceptors(&Message::Accept {
                config_id: self.config_id,
                from: me,
                ballot,
                slot,
                command,
            });
        }
        self.assert_invariants();
    }

    /// Re-queue the open matchmaking request toward every matchmaker that has
    /// not answered yet. A no-op on a node with no open matchmaking phase.
    ///
    /// **The driver is expected to call this on a steady cadence** while
    /// [`RawNode::matchmaking_pending`] reports an open phase, so a request
    /// or reply the transport lost does not stall the campaign until the
    /// election timeout abandons it.
    ///
    /// **Skipping a call is always safe.** Re-sending is pure optimization,
    /// exactly like [`RawNode::resend_pending`]: the matchmaker answers a
    /// repeated request idempotently from its retained history (it registers
    /// nothing twice), and a campaign that never completes its matchmaking is
    /// simply abandoned at the next election timeout and retried at a higher
    /// round. The deterministic simulation skips calls to reach exactly those
    /// abandoned campaigns; production never skips.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn resend_matchmaking(&mut self) {
        let Some(m) = self.matchmaking.as_ref() else {
            return;
        };
        let generation = self.matchmakers.generation;
        let request = if m.reconfiguration {
            MatchRequest::reconfigure(self.config.id, m.ballot, m.config.clone(), generation)
        } else {
            MatchRequest::new(self.config.id, m.ballot, m.config.clone(), generation)
        };
        let unanswered: Vec<MatchmakerId> = self
            .matchmakers
            .members
            .iter()
            .copied()
            .filter(|mm| !m.registered_by.contains(mm))
            .collect();
        for matchmaker in unanswered {
            self.pending_match_requests
                .push((matchmaker, request.clone()));
        }
        self.assert_invariants();
    }

    /// Whether a matchmaking phase is open — the driver's cue to pace
    /// [`RawNode::resend_matchmaking`], consulted only where a re-send can
    /// have an effect.
    #[must_use]
    pub fn matchmaking_pending(&self) -> bool {
        self.matchmaking.is_some()
    }

    /// The open matchmaking phase, if any: its ballot, the configuration it
    /// registers, and whether it was opened by a reconfiguration. A read view
    /// for the driver's audit report.
    #[must_use]
    pub fn matchmaking(&self) -> Option<(Ballot, &AcceptorConfig, bool)> {
        self.matchmaking
            .as_ref()
            .map(|m| (m.ballot, &m.config, m.reconfiguration))
    }

    /// Fold one matchmaker's answer into the open matchmaking phase — the
    /// leader-side half of the matchmaker contract (#120). A reply for another
    /// ballot, another node, another generation, or a matchmaker that already
    /// answered (or is outside the believed set) is ignored whole (wire
    /// input, never asserted). Returns what the reply did, so the driver can
    /// report the transition it caused.
    ///
    /// - `Registered`: the history is unioned and the watermark maxed; once a
    ///   **quorum of matchmakers** has registered the ballot, `H_b` is
    ///   computed (the union, filtered by the maximum watermark) and handed to
    ///   Phase 1 through [`RawNode::start_phase1`] — no `Prepare` ever leaves
    ///   before that instant (invariant 1). An ordinary campaign whose
    ///   histories name a **reconfiguration** to a configuration other than
    ///   the one it registered abandons the campaign and adopts that
    ///   configuration instead — `StaleConfiguration`, the rule that keeps a
    ///   superseded configuration from being reinstated by a candidate that
    ///   missed the change (see [`crate::Registration`]).
    /// - `Refused`: the campaign is abandoned and this node steps back to
    ///   follower; a refusal's ballot is diagnostic only (a `Stale` or
    ///   `BelowWatermark` refusal raises the round floor the next campaign
    ///   opens above). A refusal naming a **chosen successor set** (#125:
    ///   `Stopped { successor }`, or `Generation` from a later generation) is
    ///   adopted through [`RawNode::learn_matchmakers`] and reported as
    ///   `Superseded`; the next campaign asks the new set. A refused
    ///   registration never becomes a leadership (invariant 4).
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[allow(clippy::too_many_lines)]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, matchmaker = reply.matchmaker.0, round = reply.ballot.round)))]
    pub fn on_match_reply(&mut self, reply: MatchReply) -> MatchStep {
        let generation = reply.generation;
        let (matchmaker, to, ballot, answer) = matchmaking::split_reply(reply);
        let me = self.config.id;
        if to != me || !self.matchmakers.contains(matchmaker) {
            return MatchStep::Ignored;
        }
        let quorum = self.matchmakers.quorum_size();
        let step = {
            let Some(m) = self.matchmaking.as_mut() else {
                return MatchStep::Ignored;
            };
            if m.ballot != ballot || generation != self.matchmakers.generation {
                return MatchStep::Ignored;
            }
            match answer {
                Ok((history, watermark)) => {
                    if !m.fold(matchmaker, history, watermark) {
                        return MatchStep::Ignored;
                    }
                    let registered = m.registered_by.len();
                    if registered < quorum {
                        MatchStep::Registered {
                            remaining: quorum - registered,
                        }
                    } else if let Some((newest, config)) = m.stale_belief() {
                        // Stale belief: the quorum's histories name a
                        // reconfiguration to a configuration other than the
                        // one this ordinary campaign registered. Adopt the
                        // effective configuration and abandon the campaign;
                        // the next one registers it. Only *reconfiguration*
                        // registrations count as facts here: the ledger also
                        // records every candidate's belief, and "adopt the
                        // newest registration" made two candidates re-adopt
                        // each other's abandoned beliefs and flip-flop one
                        // round per election timeout (seed
                        // 7519660681720567139: 182 aborts, no leader for a
                        // 50 s tail). A reconfiguration request is monotone
                        // by ballot and never manufactured by a campaign, so
                        // adopting the highest one cannot flip-flop — and
                        // without it a candidate that missed a completed
                        // reconfiguration could be elected under the
                        // superseded configuration, rolling the cluster back
                        // without anyone asking (review of #132).
                        self.acceptors = config;
                        self.acceptors_since = newest;
                        self.become_follower(None);
                        MatchStep::StaleConfiguration { newest }
                    } else {
                        // The history is `H_b`, the prior set Phase 1 must
                        // cover. A belief that matches the effective
                        // configuration (or predates any reconfiguration)
                        // runs the leadership under what it registered.
                        let prior = m.prior();
                        let watermark = m.watermark;
                        let config = m.config.clone();
                        // The matchmaking → Phase 1 boundary. The registered
                        // quorum is restated here, at the one place Phase 1
                        // can open on a matchmaker deployment.
                        assert!(
                            registered >= quorum,
                            "Phase 1 opens only once a matchmaker quorum registered the ballot"
                        );
                        assert!(
                            prior
                                .iter()
                                .all(|c| c.members.iter().all(|n| self.in_pool(*n))),
                            "every prior configuration is drawn from the node pool"
                        );
                        self.matchmaking = None;
                        self.start_phase1(config, prior.clone());
                        MatchStep::Completed {
                            prior,
                            watermark,
                            registered_by: registered,
                        }
                    }
                }
                Err(refusal) => {
                    match &refusal {
                        MatchRefusal::Stale { highest } => {
                            // The next campaign opens above the round that
                            // refused this one (see `round_floor`).
                            self.round_floor = self.round_floor.max(highest.round);
                        }
                        MatchRefusal::BelowWatermark { watermark } => {
                            // A collected round is never campaigned again: the
                            // next one opens above the floor (#123 — a
                            // partitioned leader that outlived a GC recovers by
                            // campaigning higher).
                            self.round_floor = self.round_floor.max(watermark.round);
                        }
                        MatchRefusal::Stopped { .. }
                        | MatchRefusal::Generation { .. }
                        | MatchRefusal::Inactive => {}
                    }
                    let successor = match &refusal {
                        MatchRefusal::Stopped {
                            successor: Some(set),
                        } => Some(set.clone()),
                        MatchRefusal::Generation { current }
                            if current.generation > self.matchmakers.generation =>
                        {
                            Some(current.clone())
                        }
                        _ => None,
                    };
                    self.become_follower(None);
                    match successor {
                        Some(set) if self.learn_matchmakers(&set) => MatchStep::Superseded { set },
                        _ => MatchStep::Refused(refusal),
                    }
                }
            }
        };
        // Post-step restatements of invariants 1 and 4: a refused campaign
        // left nothing Phase-1-shaped behind, and a completed one
        // closed the matchmaking phase before opening Phase 1.
        match &step {
            MatchStep::Refused(_)
            | MatchStep::StaleConfiguration { .. }
            | MatchStep::Superseded { .. } => {
                assert!(
                    self.role == NodeRole::Follower,
                    "a refused registration never becomes a leadership"
                );
                assert!(
                    self.proposer.election().is_none() && self.matchmaking.is_none(),
                    "an abandoned campaign leaves no phase open"
                );
            }
            MatchStep::Completed { .. } => {
                assert!(
                    self.matchmaking.is_none(),
                    "a completed matchmaking phase is closed"
                );
            }
            MatchStep::Registered { .. } | MatchStep::Ignored => {}
        }
        self.assert_invariants();
        step
    }

    /// Advance the next bounded page of deferred chosen-prefix application or
    /// inherited leader-recovery rounds after the caller has fully processed
    /// the previous [`Ready`] batch. A no-op when neither continuation exists.
    ///
    /// Drivers call this after persistence, sends, and application complete;
    /// keeping it separate from [`Ready::advance`](crate::Ready::advance)
    /// prevents a single-node recovery from advancing the in-memory chosen
    /// prefix ahead of the batch the driver is still persisting.
    ///
    /// # Panics
    ///
    /// If an internal role/recovery invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn advance_recovery(&mut self) {
        let writes_before = self.pending_writes.len();
        self.advance_chosen_index();
        if self.pending_writes.len() != writes_before {
            self.assert_invariants();
            return;
        }
        self.pump_leader_recovery();
        self.assert_invariants();
    }

    /// Whether this leader has Phase-2 rounds whose `Accept`s can be re-sent.
    /// Drivers use this to avoid consulting optional policy hooks when skipping
    /// a re-send would have no observable effect.
    #[must_use]
    pub fn has_pending_accepts(&self) -> bool {
        self.role == NodeRole::Leader && !self.proposer.rounds().is_empty()
    }

    /// Voluntarily resign the leadership: Leader → Follower, keeping every
    /// durable commitment (the promised ballot and the accepted log are
    /// untouched) and dropping only the volatile leadership state — the in-flight
    /// Phase-2 `proposer` map and any unconfirmed read-index rounds. A no-op on a
    /// node that is not the leader.
    ///
    /// A legitimate operational primitive: a node may want to hand leadership on
    /// before a planned restart or a rebalance (etcd-raft exposes the same idea as
    /// leadership transfer), and stepping down is always sound — Paxos never
    /// requires a leader to *stay* one. The slots the resigning leader was still
    /// re-proposing are simply undecided; the next leader recovers them from its
    /// promise quorum, or fills the ones the quorum never saw with a
    /// [`Control::Noop`].
    ///
    /// In the deterministic simulation this is the decision that makes an
    /// undecided slot **permanent**: the hole a skipped
    /// [`resend_pending`](RawNode::resend_pending) leaves behind heals for as long
    /// as its holder keeps re-proposing it, and stops healing the moment that node
    /// stops being leader (#54) — and the leadership churn it creates is what
    /// #67's arc needs.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn step_down(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        self.become_follower(None);
        self.assert_invariants();
    }

    /// Open an **application repair** (Stage 8): the driver's boot replay could
    /// not walk the whole chosen prefix — a faulty chosen record blocked it, or
    /// the application snapshot was lost below the compaction floor — and the
    /// application's durable prefix stops just below `from`. The core re-emits
    /// every decided command from `from` onward, in slot order, through the
    /// ordinary [`Ready::committed`](crate::Ready::committed) seam as the
    /// missing values arrive (commit-replay catch-up, or a snapshot install
    /// when `from` sits below the floor), and pulls them from peers each tick.
    ///
    /// A no-op when nothing is chosen or `from` already covers the prefix.
    ///
    /// # Panics
    ///
    /// If `from` lies past the contiguous chosen prefix — the driver may only
    /// name a slot the durable chosen index already covers.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, from = from.0)))]
    pub fn open_app_repair(&mut self, from: Slot) {
        self.replica.open_app_repair(from);
        self.pump_app_repair();
        self.assert_invariants();
    }

    /// Re-emit the next run of decided commands the open application repair can
    /// serve: from the cursor, while each slot's value is present (readable in
    /// `chosen`), bounded per batch. Stops at the floor (only a snapshot can
    /// heal below it) or at the first still-missing value (catch-up will bring
    /// it). Closes the repair when the cursor reaches the contiguous prefix
    /// walk's frontier.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(crate) fn pump_app_repair(&mut self) {
        self.replica.pump_app_repair(self.acceptor.first_slot());
    }

    /// The driver supplies a randomized election timeout (in ticks, jitter drawn
    /// from its `RandomProvider`). Clears the [`RawNode::needs_election_timeout`]
    /// flag.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, ticks)))]
    pub fn set_election_timeout(&mut self, ticks: u64) {
        self.election_timeout = ticks;
        self.needs_election_timeout = false;
    }

    /// The election timeout in force (in ticks; zero until the driver set
    /// one) — the unit the driver paces its other timeouts in.
    #[must_use]
    pub fn election_timeout(&self) -> u64 {
        self.election_timeout
    }

    /// Borrow the node to drain one batch of work. The returned [`Ready`] holds
    /// the unique `&mut` borrow, so a second `ready()` before [`Ready::advance`]
    /// is a **compile error**.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub fn ready(&mut self) -> Ready<'_> {
        Ready::new(self)
    }

    // ---- accessors --------------------------------------------------------

    /// This node's static configuration (identity, bootstrap membership, pool
    /// and matchmaker set).
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The acceptor configuration in force for the highest ballot this node
    /// has seen registered: on a leader, the one its Phase 2 quorums are
    /// counted over; on a follower, its belief about the latest one. The
    /// bootstrap configuration on plain Multi-Paxos, always.
    #[must_use]
    pub fn acceptors(&self) -> &AcceptorConfig {
        &self.acceptors
    }

    /// The ballot [`RawNode::acceptors`] was registered under
    /// (`Ballot::zero()` for the bootstrap configuration).
    #[must_use]
    pub fn acceptors_since(&self) -> Ballot {
        self.acceptors_since
    }

    /// Whether this node is a member of its active configuration
    /// ([`RawNode::acceptors`]) — a real acceptor whose own vote counts.
    #[must_use]
    pub fn is_acceptor(&self) -> bool {
        self.acceptors.contains(self.config.id)
    }

    /// Monotone campaign-phase counters this incarnation, for the driver's
    /// audit report: `(non-member campaigns skipped, non-member step-downs)`.
    #[must_use]
    pub fn membership_counters(&self) -> (u64, u64) {
        (
            self.non_member_campaigns_skipped,
            self.non_member_step_downs,
        )
    }

    /// Election timeouts this incarnation that re-sent an open matchmaking
    /// phase's requests instead of abandoning the campaign. Observability
    /// only (see [`RawNode::tick`]).
    #[must_use]
    pub fn matchmaking_timeouts(&self) -> u64 {
        self.matchmaking_timeouts
    }

    /// How many matchmaker disagreements (two configurations reported at one
    /// ballot) the open matchmaking phase has unioned so far — 0 when none is
    /// open. Observability only: the union keeps both, so safety never
    /// depends on the count.
    #[must_use]
    pub fn matchmaking_disagreements(&self) -> u64 {
        self.matchmaking.as_ref().map_or(0, |m| m.disagreements)
    }

    /// The matchmaker set this node believes authoritative (#125): the
    /// bootstrap set at generation 0 until a later one is learned. Empty on
    /// plain Multi-Paxos.
    #[must_use]
    pub fn matchmaker_set(&self) -> &MatchmakerSet {
        &self.matchmakers
    }

    /// Adopt `set` as the authoritative matchmaker set if it is a strictly
    /// later generation than the one believed (#125): a refusal naming a
    /// successor, a reconfiguration this node's driver completed, or a reply
    /// from a later generation. An open matchmaking against the superseded
    /// set is abandoned (the next election timeout re-campaigns against the
    /// new one), a pending GC tally starts over, and a plain deployment never
    /// moves (it has no generation to move). Returns whether the belief
    /// moved.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, generation = set.generation.0)))]
    pub fn learn_matchmakers(&mut self, set: &MatchmakerSet) -> bool {
        if !self.config.has_matchmakers()
            || set.generation <= self.matchmakers.generation
            || set.members.is_empty()
        {
            return false;
        }
        // Wire hygiene: a set naming a matchmaker outside the pool is not one
        // this deployment can reach; ignore it whole.
        if !set
            .members
            .iter()
            .all(|m| self.config.matchmaker_pool().binary_search(m).is_ok())
        {
            return false;
        }
        self.matchmakers = MatchmakerSet::new(set.generation, set.members.clone());
        if self.matchmaking.is_some() {
            // The registrations collected so far were for a replaced
            // generation; a stopped quorum will never complete them.
            self.become_follower(None);
        }
        self.reset_gc_for_generation();
        self.assert_invariants();
        true
    }

    /// The current durable scalars (configuration id, promised ballot, chosen
    /// index), composed from the components that own them.
    #[must_use]
    pub fn hard_state(&self) -> HardState {
        HardState {
            config_id: self.config_id,
            max_promised_ballot: self.acceptor.promised(),
            chosen_index: self.replica.chosen_index(),
        }
    }

    /// The working per-slot accepted log (rebuilt from durable storage on boot).
    /// A read view for drivers/oracles; writes go through [`Ready`] deltas.
    #[must_use]
    pub fn accepted(&self) -> &BTreeMap<Slot, (Ballot, Command)> {
        self.acceptor.records()
    }

    /// The compaction floor: the first slot still retained. Slots below it have
    /// been truncated away (see [`RawNode::compact`]).
    #[must_use]
    pub fn first_slot(&self) -> Slot {
        self.acceptor.first_slot()
    }

    /// The number of ticks observed so far.
    #[must_use]
    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }

    /// This node's current role.
    #[must_use]
    pub fn role(&self) -> NodeRole {
        self.role
    }

    /// The node this one believes is leader, if any.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// Whether this node is currently the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader
    }

    /// This node's current operating ballot.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// Whether the driver should feed a fresh randomized election timeout (the
    /// election clock just reset).
    #[must_use]
    pub fn needs_election_timeout(&self) -> bool {
        self.needs_election_timeout
    }

    /// How many undecided holes this node filled with a [`Control::Noop`] when it
    /// won its current leadership — 0 on a node that has never led, and re-set at
    /// each election it wins. A read-only observability counter: the driver reads
    /// it on the transition to Leader so a simulation can prove the gap-fill path
    /// is genuinely reached.
    #[must_use]
    pub fn election_gap_fills(&self) -> u64 {
        self.election_gap_fills
    }

    /// The full at-most-once session ledger: every `(client, seq) -> slot`
    /// record in this node's applied prefix, flattened. The driver attaches it
    /// to each [`Message::InstallSnapshot`] it serves (paros-owned metadata
    /// beside the opaque application bytes), so a snapshot-recovered peer makes
    /// the same #94 duplicate-suppression decisions as everyone else.
    #[must_use]
    pub fn session_ledger(&self) -> Vec<SessionEntry> {
        self.replica
            .session_ledger()
            .iter()
            .flat_map(|(client, seqs)| seqs.iter().map(|(&seq, &slot)| (*client, seq, slot)))
            .collect()
    }

    /// Chosen slots the apply seam executes as a no-op because their
    /// `Command::User` identity already applied at a lower slot (see the field
    /// doc). The driver's boot replay consults this so a restart re-applies the
    /// retained prefix with the identical suppression decisions.
    #[must_use]
    pub fn duplicate_slots(&self) -> &BTreeSet<Slot> {
        self.replica.duplicate_slots()
    }

    /// Monotone count of #94 duplicate suppressions the contiguous walk
    /// performed this incarnation. The driver reads the delta per batch and
    /// reports it through its audit port.
    #[must_use]
    pub fn duplicates_suppressed(&self) -> u64 {
        self.replica.duplicates_suppressed()
    }

    /// Monotone count of `CheckQuorum` step-downs (#95) this incarnation: the
    /// times this node, as Leader, spent a full election-timeout window without
    /// hearing an ack quorum and demoted itself. The driver reads the delta per
    /// batch and reports it through its audit port.
    #[must_use]
    pub fn quorum_lost_step_downs(&self) -> u64 {
        self.quorum_lost_step_downs
    }

    /// The recoverable faulty entries this node still holds (Stage 8): value
    /// lost, identity known, reported in the Promise tri-state and repaired in
    /// place. A read view for drivers/oracles.
    #[must_use]
    pub fn faulty_entries(&self) -> &BTreeMap<Slot, Ballot> {
        self.acceptor.faulty()
    }

    /// How this node came to hold its current leadership: won by ordinary
    /// Phase 1, or installed from a predecessor's cooperative handoff.
    /// [`LeadershipOrigin::Elected`] on any non-leader.
    #[must_use]
    pub fn leadership_origin(&self) -> LeadershipOrigin {
        self.leadership_origin
    }

    /// The next slot this leader would allocate to a fresh proposal — the
    /// **allocator frontier** a cooperative handoff transfers. A read view for
    /// drivers/oracles.
    #[must_use]
    pub fn next_slot(&self) -> Slot {
        self.next_slot
    }

    /// Monotone cooperative-handoff counters this incarnation (see
    /// [`HandoffCounters`]). The driver reports the delta through its audit
    /// port, so a simulation can prove each handoff and refusal path is
    /// genuinely reached.
    #[must_use]
    pub fn handoff_counters(&self) -> HandoffCounters {
        self.handoff
    }

    /// The open application repair cursor, if any (see
    /// [`RawNode::open_app_repair`]).
    #[must_use]
    pub fn app_repair(&self) -> Option<Slot> {
        self.replica.app_repair()
    }

    /// How many blocked slots the leader's open repair probe still holds (0
    /// when no probe is open): faulty slots the promise quorum resolved neither
    /// as Case 1 (`have`) nor Case 2 (a full Q1 of `none`), still waiting on
    /// stragglers.
    #[must_use]
    pub fn blocked_repairs(&self) -> usize {
        self.proposer.probe().map_or(0, |p| p.blocked().len())
    }

    /// Monotone repair counters this incarnation, for the driver's audit
    /// report: `(faulty records repaired in place, Case-1 straggler
    /// re-proposals, Case-2 straggler no-op fills, recovery-timeout
    /// step-downs, repair payload bytes)`.
    #[must_use]
    pub fn repair_counters(&self) -> (u64, u64, u64, u64, u64) {
        let (faulty_repaired, repair_bytes) = self.acceptor.repair_counters();
        (
            faulty_repaired,
            self.repair_case1,
            self.repair_case2,
            self.repair_step_downs,
            repair_bytes,
        )
    }

    /// The **chosen gap**, if this node holds one: `(hole, highest)` where `hole`
    /// is the first slot missing from the contiguous chosen prefix and `highest`
    /// is the highest slot above it this node already knows is chosen. `None` when
    /// nothing is chosen past the prefix — the healthy steady state.
    ///
    /// A read-only observability accessor: the core cannot trace, and the gap is
    /// invisible from outside because [`Ready::committed`](crate::Ready::committed)
    /// only ever surfaces the *contiguous* prefix. A gap is a normal transient
    /// (pipelining, a follower that missed one `Commit`); a gap that **survives
    /// quiescence** is the wedge this exists to make observable — the chosen index
    /// frozen at `hole - 1` cluster-wide while higher slots keep being chosen.
    #[must_use]
    pub fn chosen_gap(&self) -> Option<(Slot, Slot)> {
        self.replica.chosen_gap()
    }

    // ---- crate-internal accessors used by `Ready` (not public API) ----

    pub(crate) fn pending_writes(&self) -> &[WriteOp] {
        &self.pending_writes
    }

    pub(crate) fn pending_messages(&self) -> &[(NodeId, Message)] {
        &self.pending_messages
    }

    pub(crate) fn pending_committed(&self) -> &[(Slot, Command)] {
        self.replica.committed()
    }

    pub(crate) fn pending_snapshot_offers(&self) -> &[(NodeId, Slot, Ballot, ConfigId)] {
        &self.pending_snapshot_offers
    }

    pub(crate) fn pending_read_states(&self) -> &[ReadState] {
        &self.pending_read_states
    }

    pub(crate) fn pending_match_requests(&self) -> &[(MatchmakerId, MatchRequest)] {
        &self.pending_match_requests
    }

    pub(crate) fn pending_gc_requests(&self) -> &[(MatchmakerId, GcRequest)] {
        &self.pending_gc_requests
    }

    pub(crate) fn pending_recovery_batch(&self) -> Option<(usize, usize, usize)> {
        self.pending_recovery_batch
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_writes.clear();
        self.pending_messages.clear();
        self.replica.clear_committed();
        self.pending_snapshot_offers.clear();
        self.pending_read_states.clear();
        self.pending_match_requests.clear();
        self.pending_gc_requests.clear();
        self.pending_recovery_batch = None;
    }
}

#[cfg(test)]
mod tests;
