//! The [`ColocatedNode`] handle: the sans-IO Multi-Paxos state machine and the
//! `step`/`tick`/`ready`/`advance` contract.

mod acceptor;
mod authority;
mod boot;
mod catch_up;
mod election;
mod gc;
mod handoff;
mod helpers;
mod invariants;
mod learn;
mod matchmaking;
mod phase2;
mod quorum_reads;
mod reconfigure;
mod replication;

pub(crate) use self::quorum_reads::READ_TTL_TICKS;
use std::collections::{BTreeMap, BTreeSet};

pub use self::handoff::{
    HANDOFF_BATCH, HANDOFF_FENCE_ELECTIONS, Handoff, HandoffCounters, LeadershipOrigin,
};
pub use self::matchmaking::MatchStep;
pub use self::reconfigure::{ReconfigureRefusal, ReconfigureResult};
pub use self::replication::HEARTBEAT_TICKS;
use crate::acceptor::Acceptor;
use crate::collector::Collector;
pub use crate::collector::GcStep;
use crate::matchmaker::{GcRequest, MatchRequest};
use crate::matchmaking::{Matchmaking, MembershipProbe};
use crate::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet, ProxyId};
use crate::message::{Audience, Message, Party};
use crate::proposer::{Proposer, Round};
use crate::quorum_read::{QuorumReads, ReadBasis};
use crate::ready::Ready;
use crate::replica::Replica;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{Ballot, Command, Control, Entry, NodeId, Seq, Slot, command_fingerprint};
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

// A zero budget would resign every leader that opens a probe on its first tick.
const _: () = assert!(REPAIR_TIMEOUT_ELECTIONS > 0);

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
/// client: redirect on `NotLeader`, or hold the reply for `Accepted` until the
/// slot is applied and answer from its [`crate::Outcome`] (#204) — a proposal
/// is never judged here, only at apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposeResult {
    /// This node is not the leader; the client should retry the hinted node
    /// (`None` if leadership is currently unknown).
    NotLeader(Option<NodeId>),
    /// Admitted at this slot; answer from the slot's outcome once applied.
    Accepted(Slot),
}

/// **Where a proposal's Phase 2 runs** (#142): on the leader itself, or
/// handed to a proxy leader that fans the `Accept` out, folds the
/// `Accepted`s and emits the `Commit`. The driver names it at the
/// delegation call ([`ColocatedNode::propose_in`],
/// [`ColocatedNode::propose_control_in`]); production passes
/// [`Delegation::Auto`] and the core's pure function decides, the
/// deterministic simulation overrides from the node loop to reach the proxy
/// mixes the modulus alone never would. Every choice is **always safe**: a
/// proxy contributes nothing to the decision, and two fan-outs of one
/// `(slot, ballot, command)` are P2b-idempotent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Delegation {
    /// The core's own rule: `ProxyId(slot % proxy_count)`
    /// ([`ProxyId::of`]), colocated on a deployment without proxies.
    #[default]
    Auto,
    /// Run this round on the leader whatever the deployment's proxy count
    /// — the driver's "skip the delegation" choice.
    Colocated,
    /// Hand this round to this proxy.
    To(ProxyId),
}

/// A served quorum read, surfaced via [`Ready::read_states`]: a whole row
/// answered the read's `PreRead` with vote watermarks whose maximum is
/// `index`, and this node's chosen prefix covered `index` by the time it
/// surfaced — the linearization point a read at `ctx` observes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadState {
    /// The driver-supplied correlation token from [`ColocatedNode::quorum_read`].
    pub ctx: u64,
    /// The read index: the maximum vote watermark the read's row reported
    /// (`None` = empty prefix). The node's chosen index is at or past it
    /// when it surfaces.
    pub index: Option<Slot>,
}

/// Where a node's belief about the acceptor configuration in force came
/// from (#173).
///
/// A belief is volatile: every incarnation boots believing the bootstrap
/// configuration, whatever it believed before. That default is right on a
/// cluster that never reconfigured and wrong after any reconfiguration, and
/// a node cannot tell which from the default alone. It can tell whether it
/// has *heard* anything since it booted, and that is the whole distinction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BeliefSource {
    /// The bootstrap configuration this incarnation booted with; nothing
    /// heard since. A node in this state probes the matchmakers before it
    /// acts on the default at all: neither "I am outside" nor "I am inside,
    /// and this is the set to register" is a fact it heard.
    Bootstrap,
    /// Heard since boot: a configuration adopted through
    /// `ColocatedNode::adopt_configuration`, or the bootstrap confirmed in
    /// force by a probe that found no reconfiguration.
    Heard,
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
/// the journal fold). Each is its own type and decides its own
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
/// role, and every role's answer comes back as data. A second deployment is
/// a different wiring over the same roles, not a different core — and the
/// first one exists: the compartmentalized **proxy leader**
/// ([`crate::proxy_leader::ProxyLeader`], #142) is the proposer's Phase-2
/// tally ([`crate::proposer::Rounds`]) plus routing, on a process that is
/// neither an acceptor nor a replica; the replica tier
/// ([`crate::replica_node::ReplicaNode`], #144) is the next.
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
    /// The **read basis** (#260, [`ReadBasis`]): the configuration this
    /// node's quorum reads are judged over, learned only from a leadership
    /// that won its ballot — this node's own election or handoff install, or
    /// a leader's beat — with that leadership's fence. Never a `Prepare`'s
    /// configuration: a campaign may never finish, and the configurations it
    /// must cover hold slots its own quorums never voted. Volatile (`None`
    /// at boot, until the first beat), and only ever set on a matchmaker
    /// deployment: plain Multi-Paxos reads over its static configuration.
    read_basis: Option<ReadBasis<NodeId>>,
    /// Where `acceptors` came from: the bootstrap default every incarnation
    /// boots with, or something this incarnation **heard** — a leader's
    /// wire, its own election or handoff, an adopted effective configuration,
    /// or a membership probe's answer. Volatile, like the belief itself. A
    /// node whose belief is only the default probes the matchmakers before
    /// its first campaign or skip (#173); see [`BeliefSource`].
    belief_source: BeliefSource,
    /// The ballot of the **reconfiguration fact** the belief is known to
    /// match (#278): the effective configuration a membership probe or a
    /// `StaleConfiguration` adopted, kept while the wire confirms the same
    /// configuration, and `Ballot::zero()` for the bootstrap or a
    /// *different* configuration learned off the wire, which carries no
    /// fact. A probe compares against this and the read basis
    /// (`probe_floor`), never `acceptors_since`, which may be a *campaign*
    /// ballot (`learn_config` binds a `Prepare`'s `C_b` to its ballot): a
    /// campaign that never won and left this node outside must not outrank
    /// the older reconfiguration that names it. At most `acceptors_since`;
    /// volatile.
    belief_fact: Ballot,
    /// The tag of the last membership probe that **closed with this node
    /// outside** its belief (#278), until a campaign, a probe or a new belief
    /// replaces it: a matchmaker's answer at that tag arriving after the
    /// quorum is still folded ([`ColocatedNode::on_match_reply`]), because a
    /// quorum is a minimum, and the one answer a fixed latency always orders
    /// last may be the only one that names this node. The first late answer
    /// that moves the belief retires the tag, as any new belief does: a
    /// later one waits for the next re-probe, a liveness cost only.
    /// Volatile.
    closed_probe: Option<Ballot>,
    /// The highest ballot at which a configuration this node **belonged to**
    /// was in force here: `acceptors_since` restricted to the assignments
    /// that left this node inside `acceptors` (boot, `learn_config`,
    /// `try_become_leader`, a handoff install, an adopted effective
    /// configuration). Monotone, volatile, and the whole of what
    /// [`ColocatedNode::may_retire`] needs: a GC watermark strictly above it means
    /// no surviving configuration can ask this node for a Phase-1 promise,
    /// because every configuration it was ever a member of is forgotten.
    last_member_ballot: Ballot,
    /// The **addressable pool** in force (#189): every node this one follows,
    /// counts and answers — `Config::pool` at boot, grown at runtime by
    /// [`ColocatedNode::extend_pool`] as the deployment's node registry
    /// admits nodes. Grow-only, sorted and deduplicated, a superset of every
    /// configuration this node has adopted. Volatile: a restart boots from
    /// `Config::pool` again and the driver extends it anew.
    pool: Vec<NodeId>,
    /// The **acceptor** component ([`crate::acceptor::Acceptor`]): the
    /// durable promise, the per-slot accepted log, the compaction floor and
    /// the CTRL tri-state's faulty entries. Rebuilt on boot from the durable
    /// log (see [`ColocatedNode::new`]); persisted one delta at a time through the
    /// [`WriteOp`]s it emits into this node's batch.
    acceptor: Acceptor<Command>,
    /// The **replica** component ([`crate::replica::Replica`]): the chosen
    /// log, the durable chosen index, the contiguous apply walk and the
    /// journal state it folds.
    replica: Replica,

    // ---- pending output buckets: filled by the protocol logic, drained by
    // ---- `ready`, cleared by `advance`.
    /// Semantic durable write deltas produced this batch, in apply order.
    pending_writes: Vec<WriteOp>,
    pending_messages: Vec<(Audience, Message)>,
    /// Read-index rounds and quorum reads confirmed this batch, drained via
    /// [`Ready::read_states`] after the batch's committed entries are applied.
    pending_read_states: Vec<ReadState>,
    /// The **quorum reads** this node has open (#143,
    /// [`crate::quorum_read`]): leaderless, on any role, bound to the
    /// configuration each was opened against and dropped by TTL. Volatile
    /// and independent of the leadership — a role change abandons nothing
    /// here; a configuration change abandons every read opened before it.
    quorum_reads: QuorumReads<NodeId>,
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
    /// The monotone observability counters of this incarnation
    /// ([`Counters`]): what the driver's audit report reads, never a
    /// decision.
    counters: Counters,

    // ---- proposer (multi-decree) ----
    /// The proposer component: the open Phase 1, the CTRL repair probe, the
    /// in-flight Phase-2 rounds, the bounded recovery, the allocator frontier
    /// and the leadership's standing authority — its fence and its
    /// `CheckQuorum` window
    /// ([`crate::proposer`]). Volatile; dies whole with the
    /// leadership.
    proposer: Proposer<NodeId, Command>,
    /// The **matchmaking phase** while a Candidate registers its ballot's
    /// configuration with the matchmakers (#120) — the campaign state that
    /// precedes `election`, and never coexists with it. `None` on a plain
    /// deployment, always.
    matchmaking: Option<Matchmaking>,
    /// The open **membership probe** (#173, [`MembershipProbe`]): a
    /// non-member whose belief is only the bootstrap default asking the
    /// matchmakers which configuration is in force. Never coexists with a
    /// campaign; `None` on a plain deployment, always. Boxed: it is rare and
    /// the node is held inside the driver's future.
    probe: Option<Box<MembershipProbe>>,
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
    /// The round every later campaign opens strictly above, raised by a
    /// `Stale` matchmaking refusal to the refuser's highest registered round.
    /// Volatile: a restart starts from the durable promise again. Without it
    /// a candidate refused at round `r` re-registers at `r + 1`, is refused
    /// again by the same higher registration, and leapfrogs one round per
    /// election timeout behind a rival that never has to move — the
    /// matchmaking cousin of the dueling-proposer livelock, seen in the
    /// hunt as two hundred registrations for three completed campaigns.
    round_floor: u64,
    /// How this node came to hold its current leadership (see
    /// [`LeadershipOrigin`]). `Elected` on every non-leader.
    leadership_origin: LeadershipOrigin,
    /// Ticks a handoff-installed leadership has held an **uncovered inherited
    /// fence** (its chosen prefix still below the proposer's `fence`). Drives the
    /// resignation that hands an unrecoverable inherited log back to an
    /// ordinary Phase 1; reset whenever the fence is covered.
    handoff_fence_elapsed: u64,
    /// Monotone cooperative-handoff counters this incarnation, for the
    /// driver's audit report.
    handoff: HandoffCounters,
}

/// The node's **observability counters**: monotone per incarnation, read by
/// the driver's audit report and by the examples, and never a decision —
/// deleting every one of them leaves the state machine unchanged. Each
/// names a path the simulation proves reached rather than merely present.
/// The handoff's own live in [`HandoffCounters`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counters {
    /// `CheckQuorum` step-downs: full election-timeout windows without an
    /// ack quorum.
    quorum_lost_step_downs: u64,
    /// Campaigns this node declined to open because it is not a member of
    /// the configuration it would register.
    non_member_campaigns_skipped: u64,
    /// Leaderships resigned once this node's own reconfiguration removed it
    /// from the acceptor set.
    non_member_step_downs: u64,
    /// Election timeouts that found a matchmaking phase still open and
    /// re-sent its requests instead of abandoning the campaign (see
    /// [`ColocatedNode::tick`]).
    matchmaking_timeouts: u64,
    /// Recovery-timeout step-downs: a leader resigning because it could not
    /// finish repairing its blocked slots.
    repair_step_downs: u64,
    /// Blocked slots resolved as Case 1 (re-proposed from a straggler's
    /// `have`) after the election closed.
    repair_case1: u64,
    /// Blocked slots resolved as Case 2 (a full Q1 of `none` assembled from
    /// stragglers; decided `Noop`).
    repair_case2: u64,
    /// Undecided holes filled with a [`Control::Noop`] when this node won its
    /// *current* leadership (0 until it wins one, re-set at each election).
    election_gap_fills: u64,
    /// Slots a settled leader filled with a [`Control::Noop`] up to a vote
    /// watermark a pre-read reported past its frontier (monotone).
    watermark_fills: u64,
    /// Quorum reads not opened for want of a read basis (#260).
    quorum_reads_without_basis: u64,
    /// `PreRead`s left unanswered by a node whose belief was still the
    /// bootstrap default (#260).
    pre_reads_refused_unheard: u64,
}

/// The repair counters ([`ColocatedNode::repair_counters`]): monotone per
/// incarnation, observability only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepairCounters {
    /// Faulty records repaired in place.
    pub repaired: u64,
    /// Blocked slots resolved as Case 1 (re-proposed from a straggler's
    /// `have`) after the election closed.
    pub case1: u64,
    /// Blocked slots resolved as Case 2 (a full Q1 of `none` assembled from
    /// stragglers; decided `Noop`).
    pub case2: u64,
    /// Recovery-timeout step-downs: a leader resigning because it could not
    /// finish repairing its blocked slots.
    pub step_downs: u64,
}

/// The campaign-membership counters ([`ColocatedNode::membership_counters`],
/// #122): monotone per incarnation, observability only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MembershipCounters {
    /// Campaigns this node declined to open because it is not a member of
    /// the configuration it would register.
    pub campaigns_skipped: u64,
    /// Leaderships resigned once this node's own reconfiguration removed it
    /// from the acceptor set.
    pub step_downs: u64,
    /// Quorum reads not opened for want of a read basis (#260): no won
    /// leadership heard this incarnation, or the node's belief bound above
    /// its basis.
    pub reads_without_basis: u64,
    /// `PreRead`s left unanswered while this node's belief was still the
    /// bootstrap default (#260).
    pub pre_reads_refused_unheard: u64,
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
        let marks = self.durable_marks();
        let ticks = self.tick_count;
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
                config,
                ..
            } => self.on_accept(reply_to, leader, ballot, slot, command, config),
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
            Message::TrimmedTo { point, state, .. } => self.on_trimmed_to(point, state),
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
                config,
                fence,
            } => self.on_heartbeat(from, ballot, commit, config, fence),
            Message::HeartbeatAck {
                from,
                ballot,
                chosen,
                ..
            } => {
                self.on_heartbeat_ack(from, ballot, chosen);
            }
            Message::PreRead { reply_to, ctx } => self.on_pre_read(reply_to, ctx),
            Message::PreReadAck {
                from,
                ctx,
                watermark,
                config_since,
            } => self.on_pre_read_ack(from, ctx, watermark, config_since),
        }
        // A message moves no clock, and lowers no durable watermark.
        assert!(
            self.tick_count == ticks,
            "a message never advances logical time"
        );
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// Client entry point (#204, `Write`): get `entry` decided into the next
    /// slot. Only the leader admits it; a non-leader returns
    /// [`ProposeResult::NotLeader`] with a redirect hint. Nothing about the
    /// write is judged here — not its writer, not its position, not whether
    /// it retries an earlier one: every rule is the journal state machine's,
    /// at apply, in slot order ([`crate::journal_state`]). A retry takes a
    /// fresh slot and is answered from the log itself.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, leader = %entry.leader, seq = entry.seq.0)))]
    pub fn propose(&mut self, entry: Entry) -> ProposeResult {
        let result = self.propose_in(entry, None, Delegation::Auto);
        if let ProposeResult::Accepted(slot) = result {
            assert!(
                slot < self.proposer.next_slot(),
                "an admitted write holds an allocated slot"
            );
            assert!(
                self.role == NodeRole::Leader,
                "only a leader admits a write"
            );
        }
        result
    }

    /// [`ColocatedNode::propose`] with the driver naming the **column** the
    /// proposal's Phase 2 is addressed to (#141) and the **proxy** it is
    /// delegated to (#142): `Some(c)` overrides the core's own `slot % cols`
    /// ([`AcceptorConfig::column_of`]) for this one round, and `delegation`
    /// overrides its `slot % proxy_count` ([`ProxyId::of`]); `None` and
    /// [`Delegation::Auto`] are exactly [`ColocatedNode::propose`].
    ///
    /// **Always safe, which is why these are methods and not faults.** Every
    /// full column of a grid is a Phase-2 quorum of it, and every Phase-1
    /// quorum (a row) meets every column, so a value chosen through any
    /// column is learned by every later election; which column a slot uses
    /// is a load-spreading choice, never a safety one. The round records
    /// the column it was opened against, so its re-sends and its decision
    /// stay on that column; a handoff successor or a restarted leader that
    /// re-proposes the slot derives `slot % cols` afresh, and two fan-outs
    /// of one `(slot, ballot, command)` to two columns are P2b-idempotent.
    /// A proxy contributes nothing to the decision, so which proxy runs a
    /// round — or whether the leader runs it itself — is the same kind of
    /// choice. Production never overrides; the deterministic simulation
    /// does, from the node loop, to reach the column and proxy mixes the
    /// modulus alone never would.
    ///
    /// A round is delegated only on a **settled** leadership — no election
    /// recovery and no repair probe open — whatever `delegation` says;
    /// before that it runs colocated, so a fresh leadership's recovery
    /// depends on no proxy.
    ///
    /// # Panics
    ///
    /// If `column` names a column the active configuration does not have —
    /// one at or past its `cols`, or any column at all under a majority or
    /// a flexible split, which name none — or `delegation` names a proxy at
    /// or past the deployment's `proxy_count`. The driver derives both
    /// overrides from [`ColocatedNode::acceptors`] and
    /// [`ColocatedNode::config`], so this is a programmer error, never an
    /// operating condition. Also if an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, leader = %entry.leader, seq = entry.seq.0, column = ?column, delegation = ?delegation)))]
    pub fn propose_in(
        &mut self,
        entry: Entry,
        column: Option<usize>,
        delegation: Delegation,
    ) -> ProposeResult {
        if self.role != NodeRole::Leader {
            assert!(
                self.proposer.rounds().is_empty(),
                "a refusing non-leader holds no round"
            );
            return ProposeResult::NotLeader(self.leader);
        }
        let marks = self.durable_marks();
        let result = self.open_proposal(column, delegation, Command::Write(entry));
        self.assert_marks_monotone(marks);
        result
    }

    /// The tail every proposal entry point shares: allocate the next slot,
    /// open `command`'s Phase-2 round in `column` (the configuration's own
    /// for the slot when `None`) under `delegation` narrowed to a settled
    /// leadership, and answer `Accepted`. The caller has already checked the
    /// role.
    fn open_proposal(
        &mut self,
        column: Option<usize>,
        delegation: Delegation,
        command: Command,
    ) -> ProposeResult {
        assert!(
            self.role == NodeRole::Leader,
            "only a leader opens a proposal"
        );
        let marks = self.durable_marks();
        let frontier = self.proposer.next_slot();
        let slot = self.proposer.allocate();
        // The allocator hands out exactly its frontier, which no chosen or
        // compacted slot ever reaches below.
        assert!(slot == frontier, "a proposal takes the allocator frontier");
        assert!(
            slot >= self.acceptor.first_slot(),
            "a proposal never lands below the floor"
        );
        let column = column.or_else(|| self.acceptors.column_of(slot));
        let delegation = self.settled_delegation(delegation);
        self.start_accept_round_in(slot, command, column, delegation);
        assert!(
            self.proposer.next_slot() > slot,
            "the frontier moved past the proposal"
        );
        self.assert_marks_monotone(marks);
        self.assert_invariants();
        ProposeResult::Accepted(slot)
    }

    /// `delegation` on a settled leadership, colocated otherwise: only the
    /// rounds a settled leadership opens are ever delegated
    /// (`node/phase2.rs`).
    fn settled_delegation(&self, delegation: Delegation) -> Delegation {
        let settled = if self.may_delegate() {
            delegation
        } else {
            Delegation::Colocated
        };
        // A fresh leadership's recovery depends on no proxy.
        if !self.leadership_settled() {
            assert!(
                settled == Delegation::Colocated,
                "an unsettled leadership never delegates"
            );
        }
        settled
    }

    /// Leader entry point for a **control command**: get `control` chosen into the
    /// next log slot by ordinary Paxos. Only the leader admits it; a non-leader
    /// returns [`ProposeResult::NotLeader`] with a redirect hint.
    ///
    /// Each proposal takes a fresh slot. It is a normal Phase-2 round from the
    /// acceptors' point of view (they store it opaquely, exactly like a client
    /// write). Its *effect* — a [`Control::SetLeader`]'s compare-and-swap, a
    /// [`Control::Truncate`]'s new first position and the log prefix it drops —
    /// is judged and applied by every node when the slot enters its contiguous
    /// chosen prefix (see `ColocatedNode::advance_chosen_index`).
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, control = ?control)))]
    pub fn propose_control(&mut self, control: Control) -> ProposeResult {
        let result = self.propose_control_in(control, Delegation::Auto);
        if let ProposeResult::Accepted(slot) = result {
            assert!(
                slot < self.proposer.next_slot(),
                "an admitted control holds an allocated slot"
            );
            assert!(
                self.role == NodeRole::Leader,
                "only a leader admits a control command"
            );
        }
        result
    }

    /// [`ColocatedNode::propose_control`] with the driver naming the proxy
    /// the round is delegated to (#142), under exactly the rules of
    /// [`ColocatedNode::propose_in`]: always safe, delegated only on a
    /// settled leadership, [`Delegation::Auto`] is the core's own rule.
    ///
    /// # Panics
    ///
    /// If `delegation` names a proxy the deployment does not have, or an
    /// internal invariant is broken (a programmer error, never an operating
    /// condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, control = ?control, delegation = ?delegation)))]
    pub fn propose_control_in(
        &mut self,
        control: Control,
        delegation: Delegation,
    ) -> ProposeResult {
        if self.role != NodeRole::Leader {
            assert!(
                self.proposer.rounds().is_empty(),
                "a refusing non-leader holds no round"
            );
            return ProposeResult::NotLeader(self.leader);
        }
        let marks = self.durable_marks();
        let result = self.open_proposal(None, delegation, Command::Control(control));
        self.assert_marks_monotone(marks);
        result
    }

    /// Decided log compaction (a journal `Truncate`, #204): drop every
    /// retained slot at or below `up_to`, raising the truncation floor.
    /// Returns the new floor (the first slot still retained).
    ///
    /// `up_to` is the last slot the caller permits dropping (inclusive); the
    /// floor stored in the log is the first slot *retained*. The request is
    /// clamped to what the journal fold lets go
    /// ([`Replica::compaction_target`]: every slot below the one holding the
    /// journal's first retained record, never an undecided or unfolded one),
    /// which makes the call safe to over-request and idempotent (a floor never
    /// moves backward). When the floor would not rise it is a no-op that emits
    /// no [`WriteOp::Truncate`].
    ///
    /// The journal state the dropped slots folded to is **sealed** on the
    /// [`WriteOp::Truncate`] and read back through [`Storage::sealed_state`]
    /// on the next boot, so a restart folds the retained log from the same
    /// state a node that never restarted holds.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition): the floor must rise monotonically and stay
    /// clamped inside the chosen prefix.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, up_to = up_to.0)))]
    pub fn compact(&mut self, up_to: Slot) -> Slot {
        let Some(target) = self.replica.compaction_target() else {
            return self.acceptor.first_slot();
        };
        let highest_drop = up_to.min(target);
        let old_floor = self.acceptor.first_slot();
        let first = Slot(highest_drop.0 + 1).max(old_floor);
        if first <= old_floor {
            return old_floor;
        }
        // A faulty entry below the floor is dropped with the prefix: only
        // chosen slots are dropped, and a trim is decided, so no peer will
        // ever need this node's copy of a slot below it.
        let sealed = self.replica.truncate(first);
        self.acceptor
            .truncate(first, sealed, &mut self.pending_writes);
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
        let marks = self.durable_marks();
        let ticks = self.tick_count;
        self.tick_count += 1;
        if self.role == NodeRole::Leader {
            // A leader beats on every tick ([`HEARTBEAT_TICKS`] is the
            // cadence the audit's oracle assumes, not a tunable). Re-sending
            // the un-acked `Accept`s is a *separate* decision the driver
            // makes on the same cadence — see [`ColocatedNode::resend_pending`].
            self.broadcast_heartbeat();
            self.assert_invariants();
            self.tick_check_quorum();
            // A leader its own reconfiguration removed from the acceptor set
            // (#122): it drives the change to completion — its inherited
            // rounds decided, its recovery and repair closed — and then
            // resigns, so an ordinary election lands leadership inside the
            // new configuration (a node campaigns only as a member).
            if self.role == NodeRole::Leader
                && !self.is_acceptor()
                && !self.phase1_work_open()
                && self.proposer.rounds().is_empty()
            {
                probe!(
                    reachable,
                    "reconfiguration: a leader its own reconfiguration removed resigns"
                );
                self.counters.non_member_step_downs =
                    self.counters.non_member_step_downs.saturating_add(1);
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
                    self.counters.matchmaking_timeouts =
                        self.counters.matchmaking_timeouts.saturating_add(1);
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
        // Quorum reads: expire what outlived the window, serve the rest.
        self.tick_quorum_reads();
        // The GC preconditions can become true without a message (the last
        // inherited round decided on this tick's re-send): re-check per tick.
        self.try_gc();
        assert!(
            self.tick_count == ticks + 1,
            "a tick advances logical time by one"
        );
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// Per-tick repair upkeep (Stage 8): drive the leader's open repair probe
    /// (straggler re-query + the CTRL §4.2 recovery-timeout resignation) and
    /// pull a faulty chosen record's range from peers.
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
                self.counters.repair_step_downs += 1;
                self.become_follower(None);
                // Resigning takes the probe, and its clock, with it.
                assert!(
                    self.proposer.probe().is_none(),
                    "a resigned leader holds no probe"
                );
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
                // The probe runs the leadership's own Phase 1, never another.
                assert!(
                    ballot == self.ballot,
                    "a repair probe queries at the leader's ballot"
                );
                let config = self.phase1_wire_config();
                self.send_prepare(unanswered, ballot, from_slot, config);
            }
        }
        let hole = self
            .acceptor
            .first_faulty()
            .filter(|slot| *slot < self.first_unchosen())
            .into_iter()
            .chain(self.replica.fold_hole())
            .min();
        if let Some(first_faulty) = hole {
            // The hole is inside the retained, chosen-or-faulty prefix.
            assert!(
                first_faulty >= self.acceptor.first_slot(),
                "a repair hole is retained"
            );
            // A faulty **chosen** record leaves a hole in the servable log
            // (catch-up replay stops at it — per-slot attribution — and the
            // journal fold stops at it, holding the walk; the fold's hole
            // outlives the faulty mark when an election's re-proposal repaired
            // the record without this node learning it chosen). Pull the decided range from peers so the record
            // itself heals; a peer that has it chosen serves it, one that
            // trimmed past it answers `TrimmedTo`, and this node's own next
            // election covers it either way (the campaign range starts at the
            // first faulty slot). Once per tick — the same cadence
            // heartbeat-driven catch-up uses.
            self.broadcast(self.catch_up_request(first_faulty));
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
    /// **A delegated round is re-delegated** (#142): its re-send goes to
    /// the proxy it was handed to, whose re-fan-out is P2b-idempotent, and
    /// the round counts the re-delegation so a proxy that never answers
    /// becomes visible ([`ColocatedNode::take_back_delegated`]).
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
        let marks = self.durable_marks();
        let pending = self.proposer.resend_page();
        // A re-send carries the leadership's own rounds, nothing else.
        assert!(
            pending.iter().all(|accept| accept.ballot == self.ballot),
            "a re-sent Accept runs at the leader's ballot"
        );
        for accept in pending {
            // The same column the round was opened against: a re-send never
            // widens a grid round to another column — and the same proxy.
            self.send_accept(
                accept.slot,
                accept.ballot,
                accept.command,
                accept.column,
                accept.proxy,
            );
        }
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// **Take back** every delegated round re-delegated at least
    /// `after_resends` times without the proxy's `Commit` arriving, and run
    /// each colocated from here on (#142). A no-op on a node that is not
    /// the leader, on a deployment without proxies, and while every
    /// delegated round is young.
    ///
    /// **The driver is expected to call this each beat**, right after
    /// [`ColocatedNode::resend_pending`], with its own budget: how many
    /// re-delegations a proxy may swallow is driver policy (a tunable, born
    /// buggified), not a constant of the state machine, exactly as the
    /// handoff-fence and repair budgets are. Liveness under a dead proxy is
    /// the leader's, and this is the whole of it: the fallback is always
    /// today's colocated Phase 2, as a failed handoff's fallback is an
    /// election. **Skipping a call is always safe** — a delegated round
    /// nobody takes back is simply undecided until its proxy answers or the
    /// next leadership recovers it — and so is taking a round back early:
    /// the proxy may still decide it behind this call, and two fan-outs of
    /// one `(slot, ballot, command)` are P2b-idempotent, so the two
    /// verdicts can only agree.
    ///
    /// Returns the rounds it took back, each with the proxy it was
    /// delegated to — what a driver reports and an oracle counts; empty
    /// whenever the call was a no-op.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, after_resends)))]
    pub fn take_back_delegated(&mut self, after_resends: u64) -> Vec<(Slot, ProxyId)> {
        if self.role != NodeRole::Leader {
            return Vec::new();
        }
        let mut taken = Vec::new();
        for slot in self.proposer.stalled_delegations(after_resends) {
            if let Some(proxy) = self.proposer.rounds().get(&slot).and_then(Round::proxy) {
                self.take_back(slot);
                taken.push((slot, proxy));
            }
        }
        // A round taken back is colocated (or already decided and closed).
        assert!(
            taken.iter().all(|(slot, _)| self
                .proposer
                .rounds()
                .get(slot)
                .is_none_or(|r| r.proxy().is_none())),
            "a round taken back runs colocated"
        );
        assert!(
            taken
                .iter()
                .all(|(_, proxy)| proxy.is_in(self.config.proxy_count)),
            "a round was delegated only to a deployed proxy"
        );
        // Read from the rounds, apart from `stalled_delegations` (#269): no
        // delegation past the budget stays with its proxy.
        assert!(
            self.proposer
                .rounds()
                .values()
                .all(|r| r.proxy().is_none() || r.resends() < after_resends),
            "no delegation past the take-back budget stays delegated"
        );
        self.assert_invariants();
        taken
    }

    /// The slots this leader currently holds **delegated** to a proxy, with
    /// the proxy each went to — for drivers / oracles (a node that is not the
    /// leader holds none).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn delegated_rounds(&self) -> Vec<(Slot, ProxyId)> {
        let delegated: Vec<(Slot, ProxyId)> = self
            .proposer
            .rounds()
            .iter()
            .filter_map(|(slot, round)| round.proxy().map(|proxy| (*slot, proxy)))
            .collect();
        // Delegation is the opt-in of a leader on a deployment with proxies.
        if !delegated.is_empty() {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds delegated rounds"
            );
            assert!(
                self.config.proxy_count > 0,
                "only a proxy deployment delegates"
            );
        }
        delegated
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
        let marks = self.durable_marks();
        let writes_before = self.pending_writes.len();
        self.advance_chosen_index();
        if self.pending_writes.len() != writes_before {
            self.assert_marks_monotone(marks);
            self.assert_invariants();
            return;
        }
        self.pump_leader_recovery();
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// Whether this leader has Phase-2 rounds whose `Accept`s can be re-sent.
    /// Drivers use this to avoid consulting optional policy hooks when skipping
    /// a re-send would have no observable effect.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn has_pending_accepts(&self) -> bool {
        let pending = self.role == NodeRole::Leader && !self.proposer.rounds().is_empty();
        // Rounds outlive no leadership: a non-leader holds none to re-send.
        if self.role != NodeRole::Leader {
            assert!(
                self.proposer.rounds().is_empty(),
                "a non-leader holds no round"
            );
        }
        pending
    }

    /// Voluntarily resign the leadership: Leader → Follower, keeping every
    /// durable commitment (the promised ballot and the accepted log are
    /// untouched) and dropping only the volatile leadership state — the in-flight
    /// Phase-2 `proposer` map and the standing authority. A no-op on a
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
        let marks = self.durable_marks();
        self.become_follower(None);
        // Stepping down keeps every durable commitment and drops every
        // volatile leadership tally.
        assert!(
            self.proposer.rounds().is_empty(),
            "a resigned leader holds no round"
        );
        assert!(
            self.proposer.fence().is_none(),
            "a resigned leader holds no fence"
        );
        self.assert_marks_monotone(marks);
        self.assert_invariants();
    }

    /// The driver supplies a randomized election timeout (in ticks, jitter drawn
    /// from its `RandomProvider`). Clears the [`ColocatedNode::needs_election_timeout`]
    /// flag.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, ticks)))]
    pub fn set_election_timeout(&mut self, ticks: u64) {
        self.election_timeout = ticks;
        self.needs_election_timeout = false;
        assert!(
            self.election_timeout == ticks,
            "the driver's timeout is the one in force"
        );
        assert!(
            !self.needs_election_timeout,
            "a fed timeout clears the request"
        );
    }

    /// The election timeout in force (in ticks; zero until the driver set
    /// one) — the unit the driver paces its other timeouts in.
    #[must_use]
    pub fn election_timeout(&self) -> u64 {
        self.election_timeout
    }

    /// Ticks since the election clock last reset — how far toward
    /// [`ColocatedNode::election_timeout`] the timer has run. Read-only: the
    /// clock resets wherever the core hears from a live leadership or starts
    /// a campaign, and a caller that wants to draw it reads it here rather
    /// than re-deriving those reset sites.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn election_elapsed(&self) -> u64 {
        // A leader's election clock is stopped at zero: only a follower or a
        // candidate counts toward a timeout.
        if self.role == NodeRole::Leader {
            assert!(
                self.election_elapsed == 0,
                "a leader's election clock is stopped"
            );
        }
        self.election_elapsed
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn config(&self) -> &Config {
        assert!(
            self.config.pool().binary_search(&self.config.id).is_ok(),
            "a node's boot pool names the node itself"
        );
        assert!(
            self.config.has_matchmakers() == self.matchmakers.is_some(),
            "a matchmaker set is believed exactly on a matchmaker deployment"
        );
        &self.config
    }

    /// The addressable pool in force (#189): `Config::pool`, plus every node
    /// [`ColocatedNode::extend_pool`] admitted since this incarnation booted.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn pool(&self) -> &[NodeId] {
        assert!(
            self.pool.windows(2).all(|w| w[0] < w[1]),
            "the pool is sorted and deduplicated"
        );
        assert!(
            self.pool.len() >= self.config.pool().len(),
            "the pool only grows from boot"
        );
        &self.pool
    }

    /// Admit `nodes` to the addressable pool (#189): a node registered with
    /// the deployment at runtime becomes one this node follows, counts and
    /// answers — a configuration may name it, a reconfiguration may pull it
    /// in. **Grow-only**: a node leaves no pool here (a retired node is kept
    /// out by the caller, who stops talking to it; a configuration that
    /// still names it must stay one this node can run). Pure hygiene: it
    /// emits nothing. Refused — `false`, nothing moves — on a deployment
    /// without matchmakers: plain Multi-Paxos never reconfigures, so a node
    /// admitted to its pool could never be named by a configuration, and its
    /// learner traffic stays exactly today's. Returns whether the pool grew.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, nodes = nodes.len())))]
    pub fn extend_pool(&mut self, nodes: &[NodeId]) -> bool {
        if !self.config.has_matchmakers() {
            return false;
        }
        let before = self.pool.len();
        self.pool.extend_from_slice(nodes);
        self.pool.sort_unstable();
        self.pool.dedup();
        let grew = self.pool.len() > before;
        // Postconditions: the pool only grew, and still holds the boot pool.
        assert!(self.pool.len() >= before, "the pool never shrinks");
        assert!(
            self.config
                .pool()
                .iter()
                .all(|n| self.pool.binary_search(n).is_ok()),
            "the pool always holds the boot pool"
        );
        self.assert_invariants();
        grew
    }

    /// The acceptor configuration in force for the highest ballot this node
    /// has seen registered: on a leader, the one its Phase 2 quorums are
    /// counted over; on a follower, its belief about the latest one. The
    /// bootstrap configuration on plain Multi-Paxos, always.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn acceptors(&self) -> &AcceptorConfig {
        assert!(
            self.acceptors.is_drawn_from(&self.pool),
            "the configuration in force is drawn from the pool"
        );
        if !self.config.has_matchmakers() {
            assert!(
                self.acceptors.members() == self.config.peers,
                "a plain deployment runs its bootstrap membership"
            );
        }
        &self.acceptors
    }

    /// The ballot [`ColocatedNode::acceptors`] was registered under
    /// (`Ballot::zero()` for the bootstrap configuration).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn acceptors_since(&self) -> Ballot {
        if self.acceptors_since != Ballot::zero() {
            assert!(
                self.belief_source == BeliefSource::Heard,
                "a configuration bound to a ballot is a heard belief"
            );
        }
        self.acceptors_since
    }

    /// The basis this node's quorum reads are judged over (#260,
    /// [`ReadBasis`]), or `None` when it may open none: on a matchmaker
    /// deployment, before any won leadership was heard this incarnation, or
    /// while its own belief is bound above the basis — it promised a newer
    /// campaign, which may have chosen under a configuration the basis's
    /// quorums need not meet. On plain Multi-Paxos, always the static
    /// configuration with no fence.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn read_basis(&self) -> Option<ReadBasis<NodeId>> {
        if !self.config.has_matchmakers() {
            assert!(
                self.read_basis.is_none(),
                "a plain node stores no read basis"
            );
            return Some(ReadBasis {
                config: self.acceptors.clone(),
                since: self.acceptors_since,
                fence: None,
            });
        }
        let basis = self
            .read_basis
            .clone()
            .filter(|basis| basis.since >= self.acceptors_since);
        if let Some(basis) = &basis {
            assert!(
                basis.config.is_drawn_from(&self.pool),
                "a read basis is drawn from the pool"
            );
        }
        basis
    }

    /// Where [`ColocatedNode::acceptors`] came from: the bootstrap default,
    /// or something this incarnation heard (#173, [`BeliefSource`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn belief_source(&self) -> BeliefSource {
        if self.belief_source == BeliefSource::Bootstrap {
            assert!(
                self.acceptors_since == Ballot::zero(),
                "the bootstrap belief is bound to no ballot"
            );
        }
        self.belief_source
    }

    /// The open membership probe, if any (#173) — read-only, like
    /// [`ColocatedNode::matchmaking_role`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn membership_probe(&self) -> Option<&MembershipProbe> {
        if self.probe.is_some() {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment probes"
            );
            assert!(
                self.matchmaking.is_none(),
                "a probe never overlaps a campaign"
            );
        }
        self.probe.as_deref()
    }

    /// The membership fence [`ColocatedNode::may_retire`] reads: the highest
    /// ballot a configuration naming this node was bound to, as far as this
    /// incarnation heard (volatile; `Ballot::zero()` at boot).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn last_member_ballot(&self) -> Ballot {
        if self.is_acceptor() {
            assert!(
                self.last_member_ballot >= self.acceptors_since,
                "a member's fence covers its configuration's ballot"
            );
        }
        self.last_member_ballot
    }

    /// Whether this node is a member of its active configuration
    /// ([`ColocatedNode::acceptors`]) — a real acceptor whose own vote counts.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_acceptor(&self) -> bool {
        let member = self.acceptors.contains(self.config.id);
        if member {
            assert!(
                self.in_pool(self.config.id),
                "a member of the configuration is pooled"
            );
        }
        if !self.config.has_matchmakers() {
            assert!(member, "a plain deployment's node is always a member");
        }
        member
    }

    /// Monotone campaign-membership counters this incarnation, for the
    /// driver's audit report.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn membership_counters(&self) -> MembershipCounters {
        let counters = MembershipCounters {
            campaigns_skipped: self.counters.non_member_campaigns_skipped,
            step_downs: self.counters.non_member_step_downs,
            reads_without_basis: self.counters.quorum_reads_without_basis,
            pre_reads_refused_unheard: self.counters.pre_reads_refused_unheard,
        };
        // Only a matchmaker deployment moves its configuration, so only one
        // ever lacks a read basis or refuses a pre-read for an unheard belief.
        if counters.reads_without_basis > 0 || counters.pre_reads_refused_unheard > 0 {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment reads without a basis"
            );
        }
        // A node is outside its configuration only on a deployment that
        // reconfigures: plain Multi-Paxos skips and resigns nothing.
        if counters.campaigns_skipped > 0 {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment skips a campaign"
            );
        }
        if counters.step_downs > 0 {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment resigns a non-member"
            );
        }
        counters
    }

    /// Election timeouts this incarnation that re-sent an open matchmaking
    /// phase's requests instead of abandoning the campaign. Observability
    /// only (see [`ColocatedNode::tick`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn matchmaking_timeouts(&self) -> u64 {
        if self.counters.matchmaking_timeouts > 0 {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment matchmakes"
            );
        }
        self.counters.matchmaking_timeouts
    }

    /// The current durable scalars (promised ballot, chosen
    /// index), composed from the components that own them.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn hard_state(&self) -> HardState {
        let hard_state = HardState {
            max_promised_ballot: self.acceptor.promised(),
            chosen_index: self.replica.chosen_index(),
        };
        // The two scalars as the boot read-back will see them: a chosen index
        // never lags the compaction floor, and every record sits under the
        // promise (`Acceptor::assert_invariants` is the other half).
        assert!(
            self.acceptor.first_slot() <= self.replica.first_unchosen(),
            "the durable floor never outruns the durable chosen index"
        );
        assert!(
            self.acceptor
                .records()
                .values()
                .next_back()
                .is_none_or(|(b, _)| *b <= hard_state.max_promised_ballot),
            "the durable promise dominates the last record"
        );
        hard_state
    }

    /// The node's **acceptor** role: the durable promise, the per-slot
    /// accepted log, the compaction floor and the CTRL tri-state. A read
    /// view for drivers and oracles; every write goes through the [`Ready`]
    /// batch this node emits.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn acceptor(&self) -> &Acceptor<Command> {
        // The cross-role coupling a reader of the acceptor relies on.
        assert!(
            self.acceptor.first_slot() <= self.first_unchosen(),
            "the compaction floor never outruns the chosen prefix"
        );
        assert!(
            self.acceptor
                .faulty()
                .keys()
                .all(|s| *s >= self.acceptor.first_slot()),
            "no faulty record survives below the floor"
        );
        &self.acceptor
    }

    /// The node's **replica** role: the chosen log, the contiguous apply
    /// walk and the journal state it folds. A
    /// read view, like [`ColocatedNode::acceptor`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn replica(&self) -> &Replica {
        assert!(
            self.replica.folded() >= self.acceptor.first_slot(),
            "the journal fold starts at the compaction floor"
        );
        assert!(
            self.replica.folded() <= self.replica.first_unchosen(),
            "the journal fold never passes the chosen prefix"
        );
        &self.replica
    }

    /// A **journal read** (#204) from this node's journal fold:
    /// [`Replica::read`]. A pure read — nothing in the node moves.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn read_log(&self, from: Seq, limit: usize, max_bytes: usize) -> crate::LogRead {
        let read = self.replica.read(from, limit, max_bytes);
        // A read is served from the fold's head, as it stands.
        if let crate::LogRead::Page(page) = &read {
            assert!(
                page.state == self.replica.journal().view(),
                "a page names the fold's head"
            );
            assert!(page.from == from, "a page starts where the read asked");
        }
        read
    }

    /// The node's **proposer** role: the open Phase 1, the CTRL repair
    /// probe, the in-flight Phase-2 rounds, the allocator frontier and the
    /// leadership's standing authority. A read view, like
    /// [`ColocatedNode::acceptor`].
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn proposer(&self) -> &Proposer<NodeId, Command> {
        if self.role != NodeRole::Leader {
            assert!(
                self.proposer.rounds().is_empty(),
                "a non-leader holds no round"
            );
            assert!(
                self.proposer.recovery().is_none(),
                "a non-leader holds no recovery"
            );
        }
        &self.proposer
    }

    /// The quorum reads this node has open (#143, [`crate::quorum_read`]),
    /// for drivers / oracles.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn quorum_reads(&self) -> &QuorumReads<NodeId> {
        assert!(
            self.quorum_reads
                .pending()
                .iter()
                .all(|r| r.config().is_drawn_from(&self.pool)),
            "an open read asks a configuration drawn from the pool"
        );
        &self.quorum_reads
    }

    /// This node's current role.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn role(&self) -> NodeRole {
        match self.role {
            NodeRole::Leader => {
                assert!(
                    self.leader == Some(self.config.id),
                    "a leader knows itself as leader"
                );
            }
            NodeRole::Candidate => {
                assert!(self.leader.is_none(), "a candidate follows no leader");
                assert!(
                    self.ballot.node == self.config.id,
                    "a candidate runs its own ballot"
                );
            }
            NodeRole::Follower => {}
        }
        self.role
    }

    /// The node this one believes is leader, if any.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        if self.role == NodeRole::Leader {
            assert!(
                self.leader == Some(self.config.id),
                "a leader knows itself as leader"
            );
        }
        if self.role == NodeRole::Candidate {
            assert!(self.leader.is_none(), "a candidate follows no leader");
        }
        self.leader
    }

    /// Whether this node is currently the leader.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        let leader = self.role == NodeRole::Leader;
        if leader {
            assert!(
                self.proposer.election().is_none(),
                "a leader has no open campaign"
            );
            assert!(
                self.matchmaking.is_none(),
                "a leader has no open matchmaking"
            );
        }
        leader
    }

    /// This node's current operating ballot.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        if self.role == NodeRole::Candidate {
            assert!(
                self.ballot.node == self.config.id,
                "a candidate runs its own ballot"
            );
        }
        if self.role == NodeRole::Leader {
            assert!(
                self.ballot > Ballot::zero(),
                "a leader runs a minted ballot"
            );
        }
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn election_gap_fills(&self) -> u64 {
        // Every fill took a slot below the frontier.
        assert!(
            self.counters.election_gap_fills <= self.proposer.next_slot().0,
            "gap fills never outnumber the allocated slots"
        );
        self.counters.election_gap_fills
    }

    /// Monotone count of slots this node, as a settled leader, filled with a
    /// [`Control::Noop`] because a pre-read reported a vote watermark at or
    /// past its allocator frontier (`node/quorum_reads.rs`). The driver
    /// reports the delta so a simulation can prove the path is reached.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn watermark_fills(&self) -> u64 {
        assert!(
            self.counters.watermark_fills <= self.proposer.next_slot().0,
            "watermark fills never outnumber the allocated slots"
        );
        self.counters.watermark_fills
    }

    /// Monotone count of `CheckQuorum` step-downs (#95) this incarnation: the
    /// times this node, as Leader, spent a full election-timeout window without
    /// hearing an ack quorum and demoted itself. The driver reads the delta per
    /// batch and reports it through its audit port.
    #[must_use]
    pub fn quorum_lost_step_downs(&self) -> u64 {
        self.counters.quorum_lost_step_downs
    }

    /// How this node came to hold its current leadership: won by ordinary
    /// Phase 1, or installed from a predecessor's cooperative handoff.
    /// [`LeadershipOrigin::Elected`] on any non-leader.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn leadership_origin(&self) -> LeadershipOrigin {
        if let LeadershipOrigin::Handoff { from } = self.leadership_origin {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader carries a handoff origin"
            );
            assert!(from != self.config.id, "a handoff comes from another node");
        }
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn blocked_repairs(&self) -> usize {
        let blocked = self.proposer.probe().map_or(0, |p| p.blocked().len());
        if blocked > 0 {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds a repair probe"
            );
        }
        blocked
    }

    /// Monotone repair counters this incarnation, for the driver's audit
    /// report.
    #[must_use]
    pub fn repair_counters(&self) -> RepairCounters {
        RepairCounters {
            repaired: self.acceptor.faulty_repaired(),
            case1: self.counters.repair_case1,
            case2: self.counters.repair_case2,
            step_downs: self.counters.repair_step_downs,
        }
    }

    // ---- crate-internal accessors used by `Ready` (not public API) ----

    pub(crate) fn pending_writes(&self) -> &[WriteOp] {
        // Negative space: a colocated node never writes a replica's learned
        // record; its chosen values land as authoritative accepted records.
        assert!(
            !self
                .pending_writes
                .iter()
                .any(|w| matches!(w, WriteOp::Learned { .. })),
            "a colocated node persists no learned record"
        );
        &self.pending_writes
    }

    pub(crate) fn pending_messages(&self) -> &[(Audience, Message)] {
        let me = self.config.id;
        // A node speaks only in its own name.
        assert!(
            self.pending_messages.iter().all(|(_, m)| match m {
                Message::Prepare { reply_to, .. } | Message::PreRead { reply_to, .. } => {
                    *reply_to == me
                }
                Message::Promise { from, .. }
                | Message::Accepted { from, .. }
                | Message::Nack { from, .. }
                | Message::CatchUpRequest { from, .. }
                | Message::CatchUpResponse { from, .. }
                | Message::TrimmedTo { from, .. }
                | Message::Relinquish { from, .. }
                | Message::Heartbeat { from, .. }
                | Message::HeartbeatAck { from, .. }
                | Message::PreReadAck { from, .. } => *from == me,
                Message::Commit { from, .. } => *from == Party::Node(me),
                Message::Accept { leader, .. } => *leader == me,
            }),
            "a node sends only in its own name"
        );
        &self.pending_messages
    }

    pub(crate) fn pending_committed(&self) -> &[(Slot, Command, crate::Outcome)] {
        let committed = self.replica.committed();
        // Persist-before-apply: every surfaced entry is inside the chosen
        // index this same batch makes durable.
        assert!(
            committed.last().is_none_or(|(slot, _, _)| self
                .replica
                .chosen_index()
                .is_some_and(|ci| *slot <= ci)),
            "an applied entry lies inside the chosen index"
        );
        committed
    }

    pub(crate) fn pending_read_states(&self) -> &[ReadState] {
        // The apply condition, read back as the batch surfaces: the fold
        // covers every read it answers.
        assert!(
            self.pending_read_states
                .iter()
                .all(|r| self.replica.covers(r.index)),
            "a surfaced read is covered by the fold"
        );
        &self.pending_read_states
    }

    pub(crate) fn pending_match_requests(&self) -> &[(MatchmakerId, MatchRequest)] {
        if !self.pending_match_requests.is_empty() {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment registers"
            );
        }
        assert!(
            self.pending_match_requests
                .iter()
                .all(|(_, r)| r.from == self.config.id),
            "a registration leaves in this node's name"
        );
        &self.pending_match_requests
    }

    pub(crate) fn pending_gc_requests(&self) -> &[(MatchmakerId, GcRequest)] {
        if !self.pending_gc_requests.is_empty() {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment collects"
            );
        }
        assert!(
            self.pending_gc_requests
                .iter()
                .all(|(_, r)| r.from == self.config.id),
            "a GC request leaves in this node's name"
        );
        &self.pending_gc_requests
    }

    pub(crate) fn pending_recovery_batch(&self) -> Option<(usize, usize, usize)> {
        if let Some((started, gap_fills, _)) = self.pending_recovery_batch {
            assert!(
                gap_fills <= started,
                "a recovery page fills only rounds it started"
            );
            assert!(
                started <= LEADER_RECOVERY_BATCH,
                "a recovery page is bounded"
            );
        }
        self.pending_recovery_batch
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_writes.clear();
        self.pending_messages.clear();
        self.replica.clear_committed();
        self.pending_read_states.clear();
        self.pending_match_requests.clear();
        self.pending_gc_requests.clear();
        self.pending_recovery_batch = None;
        // An acknowledged batch leaves nothing to persist, send or apply.
        assert!(
            self.pending_writes.is_empty(),
            "an advanced batch has no write left"
        );
        assert!(
            self.pending_messages.is_empty(),
            "an advanced batch has no message left"
        );
        assert!(
            self.replica.committed().is_empty(),
            "an advanced batch has nothing to apply"
        );
    }
}

#[cfg(test)]
mod tests;
