//! The [`ColocatedNode`] handle: the sans-IO Multi-Paxos state machine and the
//! `step`/`tick`/`ready`/`advance` contract.
//!
//! This file holds the protocol entry points (`new`, `step`, `tick`,
//! `propose*`, `compact`, `repair_record`, `ready`). The driver-policy
//! methods are in `policy.rs`, the read-only accessors in `view.rs` and the
//! observability counters in `counters.rs`.

mod acceptor;
mod authority;
mod boot;
mod catch_up;
mod counters;
mod election;
mod gc;
mod handoff;
mod helpers;
mod invariants;
mod learn;
mod matchmaking;
mod phase2;
mod policy;
mod quorum_reads;
pub mod ready;
mod reconfigure;
mod replication;
mod view;

pub(crate) use self::quorum_reads::READ_TTL_TICKS;
use std::collections::{BTreeMap, BTreeSet};

use self::counters::Counters;
pub use self::counters::{MembershipCounters, NodeCounters, RepairCounters};
pub use self::handoff::{
    HANDOFF_BATCH, HANDOFF_FENCE_ELECTIONS, Handoff, HandoffCounters, LeadershipOrigin,
};
pub use self::matchmaking::MatchStep;
pub use self::ready::Ready;
pub use self::reconfigure::{ReconfigureRefusal, ReconfigureResult};
pub use self::replication::HEARTBEAT_TICKS;
use crate::acceptor::Acceptor;
use crate::collector::Collector;
use crate::contract::config::Config;
use crate::contract::hard_state::HardState;
use crate::contract::limits::Limits;
use crate::contract::message::{Audience, Message, Party};
use crate::contract::storage::Storage;
use crate::contract::write::WriteOp;
use crate::journal::{Command, Control, Entry, Seq, command_fingerprint};
use crate::matchmaker::{GcRequest, MatchRequest};
use crate::matchmaking::{Matchmaking, MembershipProbe};
use crate::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet, ProxyId, QuorumSystem};
use crate::proposer::{Proposer, Round};
use crate::quorum_read::{QuorumReads, ReadBasis, ReadState};
use crate::replica::Replica;
use crate::types::{Ballot, NodeId, Slot};

/// Election timeouts a leader's blocked repair probe may stay open before the
/// leader resigns (CTRL §4.2): a leader that cannot finish recovery — e.g.
/// partitioned from the only holder of a faulty slot's value — steps down so
/// another node can try. Multiplies the driver-supplied randomized election
/// timeout, so the effective window inherits its per-seed jitter.
pub const REPAIR_TIMEOUT_ELECTIONS: u64 = 3;

// A zero budget would resign every leader that opens a probe on its first tick.
const _: () = assert!(REPAIR_TIMEOUT_ELECTIONS > 0);

/// The fewest beats a follower pulls its decided-prefix gap from the beat's
/// sender alone before it asks the whole pool; the driver's election timeout
/// sets the count when it is longer. A pull only the leader can answer fails
/// for good when the follower's lane to the leader is dead (a renamed machine
/// at an address the follower never learned, #349) while the leader's beats
/// still arrive: the election clock never fires its own broadcast probe, so
/// a pull stalled for one election timeout asks the pool as that probe does.
/// A slow pull that crosses the count costs a pool-wide page, never safety.
const PULL_FALLBACK_BEATS: u64 = 3;

// A zero fallback would broadcast every pull, a fan-out per beat.
const _: () = assert!(PULL_FALLBACK_BEATS > 0);

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
/// slot is applied and answer from its [`crate::journal::Outcome`] (#204) — a proposal
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
/// [`Acceptor`] (the durable promise, the accepted
/// log, the compaction floor, the CTRL tri-state), the
/// [`Proposer`] (the Phase-1 election, its repair
/// probe, the Phase-2 rounds, the bounded recovery, the allocator and the
/// leadership's standing authority) and the
/// [`Replica`] (the chosen prefix, the apply walk,
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
    /// The page sizes and the read window this node runs under (#330,
    /// #338, #386), fixed at boot ([`ColocatedNode::new`]). The read window
    /// is kept at least `READ_TTL_TICKS` (20 ticks) and at least the
    /// driver's own, so the core never drops a read its driver still waits
    /// on.
    limits: Limits,

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
    /// The chosen index of this node's last beat-triggered pull, and how
    /// many beats in a row pulled from it with no progress
    /// ([`PULL_FALLBACK_BEATS`]). `0` beats is a fresh pull.
    pull_index: Option<Slot>,
    pull_beats: u64,
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
    /// The **matchmaker pool** in force (#501 (matchmaker sets off the
    /// founders)): `Config::matchmaker_pool` at boot, grown at runtime by
    /// [`ColocatedNode::extend_matchmaker_pool`] as the deployment's node
    /// registry admits machines that may host a matchmaker. Grow-only,
    /// sorted and deduplicated, a superset of every set this node learned.
    /// Volatile: a restart boots from `Config::matchmaker_pool` again and
    /// the driver extends it anew.
    matchmaker_pool: Vec<MatchmakerId>,
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
    /// at apply, in slot order ([`crate::journal::state`]). A retry takes a
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
        if self.ballot < self.acceptor.promised() {
            // A leadership a higher ballot passed (its promise moved on a
            // learned `Commit`, `mark_chosen`) proposes nothing: its
            // allocator may sit inside the chosen prefix, and below the
            // floor a decided `Truncate` raised since. The deposing beat or
            // nack demotes it; until then it answers as no leader.
            probe!(
                reachable,
                "propose: a leadership a higher ballot passed refuses a proposal"
            );
            return ProposeResult::NotLeader(None);
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
        // Every entry point refused a leadership a higher ballot passed, so
        // the allocator invariant (`assert_leadership_state_invariants`)
        // holds unconditionally here: the frontier sits at or past the
        // chosen prefix, hence at or past the floor.
        assert!(
            self.ballot >= self.acceptor.promised(),
            "a proposal opens under a leadership no higher ballot passed"
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
        if self.ballot < self.acceptor.promised() {
            // As `propose_in`: a passed leadership proposes nothing.
            return ProposeResult::NotLeader(None);
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

    /// A scrub found this node's durable copy of `slot` damaged (#362
    /// (background scrubbing)): its store's record failed its checksum on a
    /// read that no protocol path asked for. The acceptor writes the record
    /// it holds again ([`crate::acceptor::Acceptor::rewrite`]), so the next
    /// sync replaces the damaged copy and a later boot finds it whole. The
    /// write rides the next [`ColocatedNode::ready`] batch, like every
    /// durable change.
    ///
    /// Returns the ballot rewritten, or `None` when the node holds no
    /// readable record at `slot` (nothing to repair from here: a slot the
    /// boot already reported faulty heals by CTRL's catch-up or election).
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, slot = slot.0)))]
    pub fn repair_record(&mut self, slot: Slot) -> Option<Ballot> {
        let marks = self.durable_marks();
        let writes_before = self.pending_writes.len();
        let chosen = self.replica.chosen_index();
        let rewritten = self.acceptor.rewrite(slot, &mut self.pending_writes);
        probe!(
            sometimes,
            rewritten.is_some(),
            "scrub: a damaged record is written again from the node's own copy"
        );
        assert!(
            self.pending_writes.len() == writes_before + usize::from(rewritten.is_some()),
            "a repair writes the record once, or nothing"
        );
        assert!(
            self.replica.chosen_index() == chosen,
            "a repair never moves the chosen prefix"
        );
        self.assert_marks_monotone(marks);
        self.assert_invariants();
        rewritten
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
            // A campaign that pages a long `Promise` suffix keeps the peers
            // that already answered from campaigning over it (#428 (paging
            // livelock)), on the leader's beat cadence.
            self.keep_paging_promisers();
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
                let query = {
                    let probe = self.proposer.probe().expect("checked above");
                    let query = probe.requery(self.config.id);
                    // The caller's restatement of the straggler set (#343):
                    // every member of the prior configurations the election
                    // covered — the Phase-1 addressee union — that has not
                    // answered its full suffix is asked again, and nobody
                    // else. A probe that stopped re-asking would sit blocked
                    // until the repair timeout resigns the leadership.
                    let me = self.config.id;
                    let prior = probe.prior();
                    assert!(
                        !prior.is_empty(),
                        "a repair probe covers some prior configuration"
                    );
                    for member in prior.iter().flat_map(|c| c.members().iter()) {
                        if *member != me && !probe.answered().contains(member) {
                            assert!(
                                query.to.contains(member),
                                "a repair probe re-asks every unanswered prior member"
                            );
                        }
                    }
                    for to in &query.to {
                        assert!(
                            !probe.answered().contains(to),
                            "a repair probe never re-asks a member that answered"
                        );
                        assert!(
                            prior.iter().any(|c| c.contains(*to)),
                            "a repair probe asks only members of a prior configuration"
                        );
                    }
                    query
                };
                // The probe runs the leadership's own Phase 1, never another.
                assert!(
                    query.ballot == self.ballot,
                    "a repair probe queries at the leader's ballot"
                );
                let config = self.phase1_wire_config();
                self.send_prepare(query.to, query.ballot, query.from_slot, config);
            }
        }
        // The fold's hole covers every faulty slot under the chosen prefix
        // (`assert_invariants`, #343): a faulty chosen record is a slot the
        // fold does not hold, so the fold stops at or below it.
        if let Some(first_faulty) = self.replica.fold_hole() {
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

    /// Borrow the node to drain one batch of work. The returned [`Ready`] holds
    /// the unique `&mut` borrow, so a second `ready()` before [`Ready::advance`]
    /// is a **compile error**.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub fn ready(&mut self) -> Ready<'_> {
        Ready::new(self)
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

    pub(crate) fn pending_committed(&self) -> &[(Slot, Ballot, Command, crate::journal::Outcome)] {
        let committed = self.replica.committed();
        // Persist-before-apply: every surfaced entry is inside the chosen
        // index this same batch makes durable.
        assert!(
            committed.last().is_none_or(|(slot, _, _, _)| self
                .replica
                .chosen_index()
                .is_some_and(|ci| *slot <= ci)),
            "an applied entry lies inside the chosen index"
        );
        // The walk's ballot, read back: a record still retained only rose
        // since the walk read it (a compacted one is gone).
        assert!(
            committed.iter().all(|(slot, ballot, _, _)| self
                .acceptor
                .record(*slot)
                .is_none_or(|(recorded, _)| recorded >= ballot)),
            "an applied entry's record ballot never falls after the walk"
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
                started <= self.limits.recovery_page,
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
