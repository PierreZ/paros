//! The [`ColocatedNode`] handle: the sans-IO Multi-Paxos state machine and the
//! `step`/`tick`/`ready`/`advance` contract.

mod acceptor;
mod boot;
mod catch_up_snapshot;
mod decide_apply;
mod election;
mod gc;
mod handoff;
mod helpers;
mod invariants;
mod matchmaking;
mod reads;
mod reconfigure;
mod replication;

use std::collections::{BTreeMap, BTreeSet};

pub use self::handoff::{
    HANDOFF_BATCH, HANDOFF_FENCE_ELECTIONS, Handoff, HandoffCounters, LeadershipOrigin,
};
pub use self::matchmaking::MatchStep;
use self::reads::READ_ROUND_TTL_TICKS;
pub use self::reconfigure::{ReconfigureRefusal, ReconfigureResult};
pub use self::replication::HEARTBEAT_TICKS;
use crate::acceptor::Acceptor;
use crate::collector::Collector;
pub use crate::collector::GcStep;
use crate::matchmaker::{GcRequest, MatchRequest};
use crate::matchmaking::Matchmaking;
use crate::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet};
use crate::message::{Audience, Message};
use crate::proposer::Proposer;
use crate::ready::Ready;
use crate::replica::Replica;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{
    Ballot, ClientId, ClientSeq, Command, Control, Entry, NodeId, SessionEntry, Slot, Value,
    command_fingerprint,
};
use crate::write::WriteOp;

/// Maximum accepted records carried by one [`Message::Promise`] page — the
/// acceptor's own bound, re-exported for the driver.
pub use crate::acceptor::PROMISE_BATCH;
/// Maximum recovered or gap-fill Phase-2 rounds started in one recovery pump —
/// the proposer's own bound, re-exported for the driver.
pub use crate::proposer::RECOVERY_BATCH as LEADER_RECOVERY_BATCH;

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

/// The outcome of [`ColocatedNode::propose`], telling the driver how to answer the
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

/// The outcome of [`ColocatedNode::read_index`], telling the driver how to answer the
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
    /// The driver-supplied correlation token from [`ColocatedNode::read_index`].
    pub ctx: u64,
    /// The read index: the applied watermark the read observes (`None` = empty
    /// prefix). The node's chosen index is at or past it on confirmation.
    pub index: Option<Slot>,
}

/// **The deployment that colocates all three Paxos roles on one node**, and
/// the wiring between them.
///
/// `paros-core` is not one state machine but a small set of roles — the
/// [`Acceptor`](crate::acceptor::Acceptor) (the durable promise, the accepted
/// log, the compaction floor, the CTRL tri-state), the
/// [`Proposer`](crate::proposer::Proposer) (the Phase-1 election, its repair
/// probe, the Phase-2 rounds, the bounded recovery, the allocator and the
/// leadership's standing authority) and the
/// [`Replica`](crate::replica::Replica) (the chosen prefix, the apply walk,
/// the at-most-once ledger). Each is its own type and decides its own
/// questions. This is the deployment Multi-Paxos names: all three on every
/// node, plus what only a colocation can own —
///
/// - the **role transitions** (Follower / Candidate / Leader) the roles
///   themselves know nothing about,
/// - the **timers**: the election clock, the heartbeat, the repair and
///   handoff-fence deadlines,
/// - the **message construction**: every role hands back a decision, and this
///   is where it becomes a [`Message`] addressed to somebody,
/// - the **persist-before-send batch**: one ordered [`WriteOp`] sequence per
///   [`Ready`], the roles' writes and the node's retention ops in one place,
/// - and the **cross-role invariants** no single role can state
///   (`ColocatedNode::assert_invariants`).
///
/// It holds **no protocol tally of its own**: every quorum question goes to a
/// role, and every role's answer comes back as data. A second deployment —
/// a compartmentalized proxy leader, a bare acceptor, a read-only replica —
/// is a different wiring over the same three roles, not a different core.
///
/// Pure, synchronous and single-threaded: no I/O, no clock, no randomness.
/// Inputs arrive via [`ColocatedNode::step`] (peer messages and
/// tick-injected self-events), [`ColocatedNode::tick`] (logical time), and
/// [`ColocatedNode::propose`] (a client value). Output is drained via
/// [`ColocatedNode::ready`] and acknowledged via [`Ready::advance`]. The name
/// mirrors etcd-raft's `RawNode`/`Node` split, whose driver half is
/// `paros::run_node`.
pub struct ColocatedNode {
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
    /// round change ([`ColocatedNode::reconfigure`]).
    acceptors: AcceptorConfig,
    /// The ballot `acceptors` was registered under (`Ballot::zero()` for the
    /// bootstrap configuration).
    acceptors_since: Ballot,
    /// The highest ballot at which a configuration this node **belonged to**
    /// was in force here: `acceptors_since` restricted to the assignments
    /// that left this node inside `acceptors` (boot, `learn_config`,
    /// `try_become_leader`, a handoff install, an adopted effective
    /// configuration). Monotone, volatile, and the whole of what
    /// [`ColocatedNode::may_retire`] needs: a GC watermark strictly above it means
    /// no surviving configuration can ask this node for a Phase-1 promise,
    /// because every configuration it was ever a member of is forgotten.
    last_member_ballot: Ballot,
    /// The **acceptor** component ([`crate::acceptor::Acceptor`]): the
    /// durable promise, the per-slot accepted log, the compaction floor and
    /// the CTRL tri-state's faulty entries. Rebuilt on boot from the durable
    /// log (see [`ColocatedNode::new`]); persisted one delta at a time through the
    /// [`WriteOp`]s it emits into this node's batch.
    acceptor: Acceptor<Command>,
    /// The **replica** component ([`crate::replica::Replica`]): the chosen
    /// log, the durable chosen index, the contiguous apply walk, the
    /// at-most-once ledger and the application repair cursor.
    replica: Replica,

    // ---- pending output buckets: filled by the protocol logic, drained by
    // ---- `ready`, cleared by `advance`.
    /// Semantic durable write deltas produced this batch, in apply order.
    pending_writes: Vec<WriteOp>,
    pending_messages: Vec<(Audience, Message)>,
    /// Snapshot offers to serve this batch:
    /// `(to, chosen_index, ballot)`. The core decides *who* needs a
    /// snapshot and *up to where* (a below-floor catch-up request), but holds no
    /// application state, so the driver attaches the opaque snapshot bytes (from
    /// storage) and sends the [`Message::InstallSnapshot`].
    pending_snapshot_offers: Vec<(NodeId, Slot, Ballot)>,
    /// Read-index rounds confirmed this batch, drained via
    /// [`Ready::read_states`] after the batch's committed entries are applied.
    pending_read_states: Vec<ReadState>,
    /// `(started, gap_fills, remaining)` for this Ready's recovery chunk.
    /// Reported through [`Ready::recovery_batch`] and, while set, the pacing
    /// gate: `pump_leader_recovery` starts no further page until
    /// [`Ready::advance`] clears it and [`ColocatedNode::advance_recovery`]
    /// schedules the next.
    pending_recovery_batch: Option<(usize, usize, usize)>,

    /// Logical clock, advanced by [`ColocatedNode::tick`].
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
    /// Monotone per-ballot beat sequence, bumped at each broadcast
    /// ([`ColocatedNode::broadcast_heartbeat`]); reset on winning an election. Acks
    /// echo it, so a read round knows which beats prove leadership *after* it
    /// began.
    heartbeat_seq: u64,
    /// Monotone count of `CheckQuorum` step-downs this incarnation, for the
    /// driver's audit report (mirrors `duplicates_suppressed`).
    quorum_lost_step_downs: u64,

    // ---- proposer (multi-decree) ----
    /// The proposer component: the open Phase 1, the CTRL repair probe, the
    /// in-flight Phase-2 rounds, the bounded recovery, the allocator frontier
    /// and the leadership's standing authority — its read fence, its pending
    /// read-index rounds and its `CheckQuorum` window
    /// ([`crate::proposer`]). Volatile; dies whole with the
    /// leadership.
    proposer: Proposer<NodeId, Command>,
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
    /// `None` on plain Multi-Paxos, always — the static-membership case is
    /// the `None` arm of the same state machine, never an empty set.
    matchmakers: Option<MatchmakerSet>,
    /// The leader's open garbage-collection campaign (#123, `node/gc.rs`).
    /// Leader-only, volatile, `None` on plain Multi-Paxos.
    gc: Option<Collector>,
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
    /// [`ColocatedNode::tick`]). Observability only.
    matchmaking_timeouts: u64,
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
    /// How many undecided holes this node filled with a [`Control::Noop`] when it
    /// won its *current* leadership (0 until it wins one, and re-set at each
    /// election). Purely observational: the driver reads it on the transition to
    /// Leader and surfaces it, so the simulation can prove the gap-fill path is
    /// genuinely reached rather than merely present.
    election_gap_fills: u64,
}

impl ColocatedNode {
    /// The single wire entry point: every peer message is a [`Message`],
    /// routed by variant and role. The clock is a separate input
    /// ([`ColocatedNode::tick`]).
    ///
    /// # Panics
    ///
    /// Panics if processing exposes a broken internal invariant (a programmer
    /// error, never an operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn step(&mut self, msg: Message) {
        match msg {
            Message::Prepare {
                reply_to,
                ballot,
                from_slot,
                config,
                ..
            } => self.on_prepare(reply_to, ballot, from_slot, config),
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
                reply_to,
                leader,
                ballot,
                slot,
                command,
                ..
            } => self.on_accept(reply_to, leader, ballot, slot, command),
            Message::Accepted {
                from,
                ballot,
                slot,
                vhash,
                ..
            } => self.on_accepted(from, ballot, slot, vhash),
            Message::Nack {
                from, ballot, slot, ..
            } => self.on_nack(from, ballot, slot),
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
        let slot = self.proposer.allocate();
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
    /// prefix (see `ColocatedNode::advance_chosen_index`).
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
        let slot = self.proposer.allocate();
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
        let slot = self.proposer.allocate();
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
        let index = self.replica.chosen_index().max(self.proposer.read_floor());
        // Beat immediately (rather than waiting for the next tick) so the
        // round's confirmation costs one network round trip, not a tick.
        self.broadcast_heartbeat();
        // A leader outside its own configuration has no acceptor vote to
        // cast; a member's own vote is one ack like any other, and the round
        // confirms only when the membership boundary
        // (`AcceptorConfig::has_phase2_quorum`) says the acks form a Phase-2
        // quorum — never a count against a threshold here.
        let own_vote = self.is_acceptor().then_some(self.config.id);
        self.proposer
            .open_read(ctx, index, self.heartbeat_seq, self.tick_count, own_vote);
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

    /// Advance logical time by one tick: a leader beats and a non-leader
    /// checks on its leader when the heartbeat / election counters cross their
    /// thresholds.
    ///
    /// Re-sending a leader's still-pending `Accept`s is deliberately *not* part of
    /// this: it is a separate decision on the same cadence, so the driver can skip
    /// it (see [`ColocatedNode::resend_pending`]).
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
            // A leader beats on every tick ([`HEARTBEAT_TICKS`] is the
            // cadence the audit's oracle assumes, not a tunable). Re-sending
            // the un-acked `Accept`s is a *separate* decision the driver
            // makes on the same cadence — see [`ColocatedNode::resend_pending`].
            self.broadcast_heartbeat();
            self.assert_invariants();
            // GC read rounds that outlived their TTL (lost acks, an unreachable
            // quorum). No re-broadcast logic is needed for the live ones: every
            // leader tick already broadcasts a fresh, higher-seq beat whose acks
            // confirm all older pending rounds.
            let now = self.tick_count;
            self.proposer.expire_reads(now, READ_ROUND_TTL_TICKS);
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
            // A **Phase-2** quorum, for the reason spelled out at the read
            // fence (`node/reads.rs`): a leader's authority is the claim that
            // no later ballot has decided behind it, which every future
            // Phase-1 quorum's intersection with this ack set rules out.
            if self.election_timeout != 0 && self.proposer.tick_authority() >= self.election_timeout
            {
                if self.proposer.authority_holds(&self.acceptors) {
                    self.proposer
                        .renew_authority(self.is_acceptor().then_some(me));
                } else {
                    self.quorum_lost_step_downs += 1;
                    self.become_follower(None);
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
                    self.on_check_leader();
                    self.assert_invariants();
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
        if self.role == NodeRole::Leader
            && let Some(elapsed) = self.proposer.tick_probe()
        {
            let timeout = self
                .election_timeout
                .saturating_mul(REPAIR_TIMEOUT_ELECTIONS);
            if self.election_timeout != 0 && elapsed >= timeout {
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
                        probe.suffix_start(),
                        probe.stragglers(self.config.id),
                    )
                };
                let config = self.phase1_wire_config();
                for to in unanswered {
                    self.pending_messages.push((
                        Audience::Node(to),
                        Message::Prepare {
                            reply_to: self.config.id,
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
            .first_faulty()
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
    /// [`ColocatedNode::tick`] — that is what lets a peer that lost the original
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
                reply_to: me,
                leader: me,
                ballot,
                slot,
                command,
            });
        }
        self.assert_invariants();
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
    /// [`resend_pending`](ColocatedNode::resend_pending) leaves behind heals for as long
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
    /// from its `RandomProvider`). Clears the [`ColocatedNode::needs_election_timeout`]
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

    /// The ballot [`ColocatedNode::acceptors`] was registered under
    /// (`Ballot::zero()` for the bootstrap configuration).
    #[must_use]
    pub fn acceptors_since(&self) -> Ballot {
        self.acceptors_since
    }

    /// Whether this node is a member of its active configuration
    /// ([`ColocatedNode::acceptors`]) — a real acceptor whose own vote counts.
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
    /// only (see [`ColocatedNode::tick`]).
    #[must_use]
    pub fn matchmaking_timeouts(&self) -> u64 {
        self.matchmaking_timeouts
    }

    /// The current durable scalars (promised ballot, chosen
    /// index), composed from the components that own them.
    #[must_use]
    pub fn hard_state(&self) -> HardState {
        HardState {
            max_promised_ballot: self.acceptor.promised(),
            chosen_index: self.replica.chosen_index(),
        }
    }

    /// The node's **acceptor** role: the durable promise, the per-slot
    /// accepted log, the compaction floor and the CTRL tri-state. A read
    /// view for drivers and oracles; every write goes through the [`Ready`]
    /// batch this node emits.
    #[must_use]
    pub fn acceptor(&self) -> &Acceptor<Command> {
        &self.acceptor
    }

    /// The node's **replica** role: the chosen log, the contiguous apply
    /// walk, the at-most-once ledger and the application repair cursor. A
    /// read view, like [`ColocatedNode::acceptor`].
    #[must_use]
    pub fn replica(&self) -> &Replica {
        &self.replica
    }

    /// The node's **proposer** role: the open Phase 1, the CTRL repair
    /// probe, the in-flight Phase-2 rounds, the allocator frontier and the
    /// leadership's standing authority. A read view, like
    /// [`ColocatedNode::acceptor`].
    #[must_use]
    pub fn proposer(&self) -> &Proposer<NodeId, Command> {
        &self.proposer
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

    /// Monotone count of `CheckQuorum` step-downs (#95) this incarnation: the
    /// times this node, as Leader, spent a full election-timeout window without
    /// hearing an ack quorum and demoted itself. The driver reads the delta per
    /// batch and reports it through its audit port.
    #[must_use]
    pub fn quorum_lost_step_downs(&self) -> u64 {
        self.quorum_lost_step_downs
    }

    /// How this node came to hold its current leadership: won by ordinary
    /// Phase 1, or installed from a predecessor's cooperative handoff.
    /// [`LeadershipOrigin::Elected`] on any non-leader.
    #[must_use]
    pub fn leadership_origin(&self) -> LeadershipOrigin {
        self.leadership_origin
    }

    /// Monotone cooperative-handoff counters this incarnation (see
    /// [`HandoffCounters`]). The driver reports the delta through its audit
    /// port, so a simulation can prove each handoff and refusal path is
    /// genuinely reached.
    #[must_use]
    pub fn handoff_counters(&self) -> HandoffCounters {
        self.handoff
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
    /// step-downs)`.
    #[must_use]
    pub fn repair_counters(&self) -> (u64, u64, u64, u64) {
        (
            self.acceptor.faulty_repaired(),
            self.repair_case1,
            self.repair_case2,
            self.repair_step_downs,
        )
    }

    // ---- crate-internal accessors used by `Ready` (not public API) ----

    pub(crate) fn pending_writes(&self) -> &[WriteOp] {
        &self.pending_writes
    }

    pub(crate) fn pending_messages(&self) -> &[(Audience, Message)] {
        &self.pending_messages
    }

    pub(crate) fn pending_committed(&self) -> &[(Slot, Command)] {
        self.replica.committed()
    }

    pub(crate) fn pending_snapshot_offers(&self) -> &[(NodeId, Slot, Ballot)] {
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
