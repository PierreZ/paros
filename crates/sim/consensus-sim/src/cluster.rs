//! The cluster: `ColocatedNode`s, proxy leaders and replicas of one journal,
//! under a [`moonpool_walk`] random walk.
//!
//! The walk owns the network: every message in flight is an [`Envelope`],
//! delivered in any order, dropped, or duplicated. The cluster owns the
//! parties, their disks
//! ([`MemStorage`](paros_consensus::contract::mem_storage::MemStorage)) and
//! the driver loop that a real deployment runs around each state machine:
//!
//! 1. take the `Ready` batch and release it with `advance`;
//! 2. persist its writes to the disk, in order;
//! 3. send its messages, after the writes are durable;
//! 4. ack what it applied, and answer the reads it served;
//! 5. release the next bounded continuation (`advance_recovery`).
//!
//! A node may crash before step 2 or between steps 2 and 3, and restarts
//! from exactly what its disk holds. A proxy has no disk and restarts
//! empty. A replica restarts from its disk.
//!
//! The oracles ([`crate::oracle`]) are fed at the seams: each write that
//! reaches a disk, each message that leaves a party, each slot a party
//! applies, each read it answers. They share no code with the core.

mod control;
mod faults;
mod flavor;
mod handover;
mod retire;
mod sites;
pub mod trace;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use paros_consensus::types::fnv1a;

use moonpool_walk::{Cx, Model, Phase, sometimes};
use paros_consensus::Limits;
use paros_consensus::contract::message::Audience;
use paros_consensus::contract::write::{AcceptorWrite, MustSync};
use paros_consensus::journal::{
    Command, Control, Entry, LeaderUuid, Outcome, ReplicaSet, Seq, command_fingerprint,
};
use paros_consensus::matchmaker::{
    GcAck, GcRequest, MatchReply, MatchRequest, ReconfigureReply, ReconfigureRequest, SegmentAck,
    SegmentPublish,
};
use paros_consensus::membership::{AcceptorConfig, MatchmakerId, ProxyId, QuorumSystem};
use paros_consensus::node::{
    BeliefSource, Delegation, ProposeResult, ReconfigureResult, RollResult,
};
use paros_consensus::quorum_read::ReadOpen;
use paros_consensus::{
    Ballot, ColocatedNode, Config, MatchmakerReconfigurer, Message, NodeId, ProxyLeader,
    ReplicaNode, Slot, Value, WriteOp,
};

use crate::cluster::sites::{MatchmakerSite, NodeSite, ProxySite, ReplicaSite};
use crate::oracle::ledger::Ledger;
use crate::oracle::quorum::Phase2Rule;
use crate::oracle::reads::Reads;

pub use flavor::{Flavor, Mutant, Steer};
pub use trace::{ClusterEvent, Cmd, OwnAnswer, Rule, Spec};

/// The coverage sites of the cluster. A walk names the ones its flavor must
/// reach with [`moonpool_walk::Walk::expect_sometimes`].
pub mod reach {
    /// The fresh controller ordered a reconfiguration.
    pub const CONTROL_RECONFIGURE: &str = "cluster: the fresh controller ordered a reconfiguration";
    /// The fresh controller ordered a retirement.
    pub const CONTROL_RETIRE: &str = "cluster: the fresh controller ordered a retirement";
    /// The stale controller gave an order.
    pub const CONTROL_STALE_ORDER: &str = "cluster: the stale controller gave an order";
    /// A leader started a reconfiguration that a controller ordered.
    pub const CONTROL_STARTED: &str = "cluster: a leader started a controller's reconfiguration";
    /// A node retired on a controller's order.
    pub const CONTROL_RETIRED: &str = "cluster: a node retired on a controller's order";
    /// A node retired at its driver once the matchmakers checked it, with no
    /// further order of a controller.
    pub const CONTROL_RETIRED_CHECKED: &str = "cluster: a checked node retired at its own driver";
    /// A party applied a valid `Roll`: the log entered a new segment.
    pub const ROLLED: &str = "cluster: a party applied a valid Roll";
    /// A leader admitted a `Roll` while an earlier one was still open: no
    /// pause (R4a dropped).
    pub const ROLL_PIPELINED: &str = "cluster: a leader rolled above an open Roll";
    /// A node jumped to a trim point past a `Roll`: the jump carried the
    /// tail segment.
    pub const ROLL_JUMP: &str = "cluster: a node jumped to a trim point past a Roll";
    /// A leader admitted a `Roll` while its bounded recovery still held
    /// values to propose again (`Recovery::pending`), which its tail reads
    /// (R4c).
    pub const ROLL_IN_RECOVERY: &str = "cluster: a leader rolled during a paged recovery";
    /// A node dropped the records of a held segment that no `Truncate`
    /// released: its replicas hold it (R7).
    pub const HELD_DROP: &str = "cluster: a node dropped a held segment no Truncate released";
    /// A replica that lagged found its whole segment released at a trim
    /// jump, and stopped (R6, R7).
    pub const REPLICA_TRIMMED: &str = "cluster: a lagging replica found its segment released";
    /// A majority of the matchmakers holds a node's publish of the end of a
    /// segment (R5): the chain grows.
    pub const CHAIN_PUBLISHED: &str = "cluster: a majority holds a published segment end";
    /// A durable quorum chose a slot.
    pub const CHOSEN: &str = "cluster: a durable quorum chose a slot";
    /// A second leadership was elected after the first.
    pub const NEW_LEADER: &str = "cluster: a node won an election above an earlier leader";
    /// A node crashed with a batch it had not persisted.
    pub const CRASH_BEFORE_PERSIST: &str = "cluster: a node crashed before its batch was durable";
    /// A node crashed after its batch was durable, before it sent it.
    pub const CRASH_BEFORE_SEND: &str = "cluster: a node crashed between persist and send";
    /// A node restarted from a disk that held a promise.
    pub const REBOOT: &str = "cluster: a node restarted from a disk with a promise";
    /// A party applied a write that the journal accepted.
    pub const WRITE_APPLIED: &str = "cluster: a party applied an accepted write";
    /// A journal changed its leader uuid.
    pub const SET_LEADER: &str = "cluster: a SetLeader was applied";
    /// A node compacted its log below a decided `Truncate`.
    pub const COMPACTED: &str = "cluster: a node's disk dropped a truncated prefix";
    /// A node jumped to a peer's trim point.
    pub const TRIM_JUMP: &str = "cluster: a node jumped to a trim point";
    /// A leader handed its authority to a peer with rounds still open.
    pub const HANDOFF: &str = "cluster: a leader handed off with rounds in flight";
    /// A proxy leader committed a slot.
    pub const PROXY_COMMIT: &str = "cluster: a proxy leader committed a slot";
    /// A leader took back a round it delegated to a proxy.
    pub const TAKE_BACK: &str = "cluster: a leader took back a delegated round";
    /// A proxy crashed with rounds open.
    pub const PROXY_CRASH: &str = "cluster: a proxy crashed with rounds open";
    /// A replica applied a slot.
    pub const REPLICA_APPLIED: &str = "cluster: a replica applied a slot";
    /// A leaderless read was answered at an acceptor.
    pub const NODE_READ: &str = "cluster: a node answered a leaderless read";
    /// A leaderless read was answered at a replica.
    pub const REPLICA_READ: &str = "cluster: a replica answered a leaderless read";
    /// The core's probe: a reader whose promise rose above its read basis
    /// opened no read (#1228 (stale read by a lagging reader)).
    pub const LAGGING_READER_REFUSED: &str = "quorum read: a promise above the basis opens no read";
    /// A leader started a reconfiguration.
    pub const RECONFIGURED: &str = "cluster: a leader started a reconfiguration";
    /// A slot was chosen under a configuration a reconfiguration named.
    pub const CHOSEN_RECONFIGURED: &str = "cluster: a slot was chosen after a reconfiguration";
    /// A matchmaker applied a GC request.
    pub const GC_ACKED: &str = "cluster: a matchmaker raised its GC watermark";
    /// A round ran on a grid column the driver named.
    pub const NAMED_COLUMN: &str = "cluster: a round ran on a column the driver named";
    /// A write was accepted after a write of the same leader uuid was
    /// refused at the wrong position.
    pub const WRITE_AFTER_GAP: &str =
        "cluster: a write was accepted after a write at a wrong position";
    /// A retried write was acknowledged from the log.
    pub const DUPLICATE: &str = "cluster: a retried write was acknowledged as a duplicate";
    /// A write under a superseded leader uuid was refused.
    pub const FENCED: &str = "cluster: a superseded writer's write was refused";
    /// A `Truncate` raised `first_seq`.
    pub const TRUNCATE_MOVED: &str = "cluster: a truncate raised first_seq";
    /// A crash lost a write that no fsync covered. A flavor reaches it
    /// only with a chosen-index write in a batch of its own; no seed of
    /// the flavors with matchmakers, or of proxies on a grid, reached it in
    /// 60 seeds.
    pub const LOST_UNSYNCED: &str = "cluster: a crash lost a write no fsync covered";
    /// A node campaigned while a leader stood.
    pub const USURPED: &str = "cluster: a node campaigned while a leader stood";
    /// A leader missed a message that would depose it.
    pub const DEAF_LEADER: &str = "cluster: a leader missed a message that would depose it";
    /// A leader move met a leadership that a higher ballot passed.
    pub const PASSED_LEADER: &str = "cluster: a move met a leader that a higher ballot passed";
    /// A leader moved a stuck round to another column.
    pub const MOVED_ROUND: &str = "cluster: a leader moved a stuck round to another column";
    /// A client sent a write again at a position it wrote before.
    pub const RETRY: &str = "cluster: a client retried a write";
    /// A disk lost the value of a chosen record.
    pub const ROT_CHOSEN: &str = "cluster: a disk rotted a chosen record";
    /// A disk lost the value of a record that a peer already trimmed and
    /// that a third node lacks.
    pub const ROT_SPLIT_FLOOR: &str = "cluster: a disk rotted a record below a peer's floor";
    /// A scrub found a rotted record and wrote it again.
    pub const SCRUBBED: &str = "cluster: a scrub wrote a rotted record again";
    /// The operator retired a node.
    pub const RETIRED: &str = "cluster: the operator retired a node";
    /// The operator booked a retired node again.
    pub const REBOOKED: &str = "cluster: the operator booked a retired node again";
    /// A node admitted a late node to its pool.
    pub const ADMITTED: &str = "cluster: a node admitted a late node to its pool";
    /// A replica admitted a late node to its pool.
    pub const REPLICA_ADMITTED: &str = "cluster: a replica admitted a late node to its pool";
    /// A proxy leader admitted a late node to its pool.
    pub const PROXY_ADMITTED: &str = "cluster: a proxy admitted a late node to its pool";
    /// A handover chose a new matchmaker set.
    pub const HANDED_OVER: &str = "cluster: a handover chose a new matchmaker set";
}

/// How long a node waits for its leader, in ticks: drawn per election.
const TIMEOUT_MIN: u64 = 3;
const TIMEOUT_SPREAD: u64 = 6;
/// The driver's resend budgets for the proxies of a seed.
const BUDGET_SPREAD: u64 = 4;
/// One crash in this many drains, at each of the two crash points.
const CRASH_ODDS: u64 = 150;
/// A party drains at most this many batches in a row.
const DRAIN_LIMIT: usize = 64;
/// The first replica's id; replicas are never acceptors.
const REPLICA_BASE: u64 = 100;
/// The client keeps this many of its last writes, to retry one.
const RETRY_WINDOW: usize = 8;

/// A party of the cluster: what the walk crashes and restarts, and what a
/// message is addressed to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Party {
    /// An acceptor and its colocated proposer and replica.
    Node(NodeId),
    /// A proxy leader.
    Proxy(ProxyId),
    /// A replica, by its node id.
    Replica(NodeId),
    /// A matchmaker.
    Matchmaker(MatchmakerId),
}

/// A message in flight.
#[derive(Clone, Debug)]
pub struct Envelope {
    /// Where it goes.
    pub to: Party,
    /// What it says.
    pub msg: Wire,
}

/// What a message says: a Paxos message, or one of the matchmaking
/// exchanges a node has with a matchmaker.
#[derive(Clone, Debug)]
pub enum Wire {
    /// A message between nodes, proxies and replicas.
    Paxos(Message),
    /// A candidate's registration (or a probe), to a matchmaker.
    Match(MatchRequest),
    /// A matchmaker's answer, to the node that asked.
    MatchReply(MatchReply),
    /// A leader's garbage-collection request, to a matchmaker.
    Gc(GcRequest),
    /// A matchmaker's answer to it.
    GcAck(GcAck),
    /// A node's publish of the chain of segments, to a matchmaker.
    Publish(SegmentPublish),
    /// A matchmaker's answer to it.
    PublishAck(SegmentAck),
    /// A handover's request, to a matchmaker.
    Reconfigure(ReconfigureRequest),
    /// A matchmaker's answer to it, to the node whose reconfigurer asked.
    ReconfigureReply(ReconfigureReply),
}

/// The cluster's own moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Every party's clock moves one tick: beats, elections, re-sends.
    Tick,
    /// A client writes at a leader.
    Propose,
    /// A client claims the journal (`SetLeader`).
    Claim,
    /// A client truncates the journal.
    Truncate,
    /// A client starts a leaderless read.
    Read,
    /// A leader steps down.
    StepDown,
    /// A leader hands its authority to a peer.
    Handoff,
    /// A leader moves the journal to another acceptor configuration.
    Reconfigure,
    /// A leader moves a stuck round to another column of its grid.
    MoveRound,
    /// A disk loses the value of one record and keeps its identity; a
    /// scrub may find it while the node runs.
    Rot,
    /// The operator retires a node the effective GC floor released.
    Retire,
    /// The operator books a retired node again.
    Rejoin,
    /// A node admits a late node to its pool.
    Admit,
    /// A node replaces the matchmaker set (`MatchmakerReconfigurer`).
    Handover,
    /// A node that does not lead campaigns while a leader stands, as if its
    /// election clock ran out early (`ColocatedNode::campaign`).
    Usurp,
}

const ACTIONS: &[Action] = &[
    Action::Reconfigure,
    Action::Tick,
    Action::Propose,
    Action::Claim,
    Action::Truncate,
    Action::Read,
    Action::StepDown,
    Action::Handoff,
    Action::MoveRound,
    Action::Rot,
    Action::Retire,
    Action::Rejoin,
    Action::Admit,
    Action::Handover,
    Action::Usurp,
];

/// The cluster under walk. See the module documentation.
pub struct Cluster {
    flavor: Flavor,
    config: AcceptorConfig,
    pool: Vec<NodeId>,
    nodes: Vec<NodeSite>,
    proxies: Vec<ProxySite>,
    replicas: Vec<ReplicaSite>,
    matchmakers: Vec<MatchmakerSite>,
    ledger: Ledger,
    reads: Reads,
    /// The next read token.
    next_ctx: u64,
    /// A compacting walk stepped a leader down after it compacted
    /// ([`Steer::Compacting`]).
    stepped_after_compaction: bool,
    /// The nodes whose recovery pages the slow driver of
    /// [`Steer::Rolling`] holds back until their next roll.
    held_recovery: BTreeSet<NodeId>,
    /// The writing client of the journal.
    client: Client,
    /// The faults stopped (`Model::calm`): no steer drops a message.
    calmed: bool,
    /// The highest leader uuid a client used.
    next_uuid: u128,
    /// The rounds a proxy may swallow before its leader takes them back.
    take_back_after: u64,
    /// The re-sends a proxy gives a round before it evicts it.
    expire_after: u64,
    /// The highest ballot a leader was seen at.
    leader_ballot: Option<paros_consensus::Ballot>,
    /// The ballots [`Mutant::EarlyGc`] asked GC at.
    early_gc: BTreeSet<paros_consensus::Ballot>,
    /// The steps recorded for the trace check so far.
    tracer: trace::Tracer,
    /// Each node's handover driver, by node id.
    reconfigurers: Vec<MatchmakerReconfigurer>,
    /// Each node's decree round on disk, by node id: it survives a crash.
    decree_rounds: Vec<u64>,
    /// The nodes the operator retired: down until it books them again.
    retired: BTreeSet<NodeId>,
    /// The reconfigurations the operator asked for: each ballot and the
    /// members it names.
    asked: Vec<(Ballot, Vec<NodeId>)>,
    /// The node a [`Steer::Retiring`] operator asked the matchmakers to
    /// check, and retires on its next retire moves while it may still.
    checking: Option<NodeId>,
    /// The late nodes each node and each replica admitted to its pool. The deployment's
    /// registry is durable: a node that boots admits them again.
    admitted: BTreeMap<NodeId, BTreeSet<NodeId>>,
    /// The late nodes each proxy leader admitted to its pool, read again
    /// when the proxy boots.
    proxy_admitted: BTreeMap<ProxyId, BTreeSet<NodeId>>,
    /// The FNV-1a fold of every batch a party of the core handed this
    /// driver ([`Cluster::ready_digest`]).
    ready_digest: u64,
    /// The journal controllers of a flavor with
    /// [`controllers`](Flavor::controllers).
    control: control::Controllers,
}

impl Model for Cluster {
    type Msg = Envelope;
    type Node = Party;
    type Action = Action;
    type Event = ClusterEvent;

    fn new(cx: &mut Cx<'_, Self>) -> Self {
        Self::build(Flavor::multi_paxos(), cx)
    }

    fn actions() -> &'static [Action] {
        ACTIONS
    }

    fn weight(action: Action, phase: Phase) -> u32 {
        // Columns: chaos, quiet, settle. In the settle phase a weight is a
        // switch: only the clock runs.
        let [chaos, quiet, settle] = match action {
            Action::Tick => [18, 12, 1],
            Action::Propose => [17, 4, 0],
            Action::Read => [4, 3, 0],
            Action::Handoff => [3, 0, 0],
            Action::Claim
            | Action::Truncate
            | Action::Reconfigure
            | Action::MoveRound
            | Action::Retire
            | Action::Rejoin
            | Action::Handover => [2, 1, 0],
            Action::StepDown | Action::Rot | Action::Usurp => [2, 0, 0],
            // A late node stays outside most pools for a while: a pool
            // catches up slowly, and every pool admits it once the faults
            // stop (`Cluster::calm`).
            Action::Admit => [1, 0, 0],
        };
        match phase {
            Phase::Chaos => chaos,
            Phase::Quiet => quiet,
            Phase::Settle => settle,
        }
    }

    fn act(&mut self, action: Action, cx: &mut Cx<'_, Self>) {
        if !self.flavor.allows(action) {
            return;
        }
        match action {
            Action::Tick => self.tick_all(cx),
            Action::Propose if self.flavor.steer == Steer::Compacting => {
                self.write_and_truncate(cx);
            }
            Action::Propose if self.flavor.steer == Steer::Retiring && cx.rng().chance(1, 2) => {
                self.operate(cx);
            }
            Action::Propose
                if self.flavor.steer == Steer::Reconfiguring && cx.rng().chance(1, 4) =>
            {
                self.reconfigure(cx);
            }
            Action::Propose
                if self.flavor.steer == Steer::Rolling
                    && (self.held_leader().is_some() || cx.rng().chance(1, 4)) =>
            {
                self.roll(cx);
            }
            Action::Propose => self.propose(cx),
            Action::Claim => self.claim(cx),
            Action::StepDown => self.step_down(cx),
            Action::Usurp => self.usurp(cx),
            Action::Truncate => self.truncate(cx),
            Action::Read => self.read(cx),
            Action::Handoff => self.handoff(cx),
            Action::Reconfigure => self.reconfigure(cx),
            Action::MoveRound => self.move_round(cx),
            Action::Rot => self.rot(cx),
            Action::Retire => self.retire(cx),
            Action::Rejoin => self.rejoin(cx),
            Action::Admit => self.admit(cx),
            Action::Handover => self.handover(cx),
        }
    }

    fn deliver(&mut self, envelope: Envelope, cx: &mut Cx<'_, Self>) {
        match (envelope.to, envelope.msg) {
            (Party::Node(id), Wire::ReconfigureReply(reply)) => {
                self.reconfigure_reply(id, reply, cx);
            }
            (Party::Node(id), wire) => {
                let mutant = self.flavor.mutant;
                if self.node(id).live.is_none() {
                    return;
                }
                if let Wire::MatchReply(reply) = &wire {
                    self.discover(id, reply, cx);
                }
                if let Wire::Paxos(msg) = &wire
                    && self.deafens(id, msg)
                {
                    return;
                }
                if let Wire::Paxos(msg) = &wire
                    && let Some(event) = self.read_fold(id, msg)
                {
                    cx.record(event);
                }
                let node = self.node_mut(id).live.as_mut().expect("a live node");
                match wire {
                    Wire::Paxos(mut msg) => {
                        tamper(mutant, &mut msg, false);
                        node.step(msg);
                    }
                    Wire::MatchReply(reply) => {
                        let _ = node.on_match_reply(reply);
                    }
                    Wire::GcAck(ack) => {
                        let _ = node.on_gc_ack(&ack);
                    }
                    Wire::PublishAck(ack) => {
                        let completed = node.on_segment_ack(&ack);
                        sometimes!(completed.is_some(), reach::CHAIN_PUBLISHED);
                    }
                    Wire::Match(_)
                    | Wire::Gc(_)
                    | Wire::Publish(_)
                    | Wire::Reconfigure(_)
                    | Wire::ReconfigureReply(_) => {
                        unreachable!("a node gets no matchmaker request")
                    }
                }
                self.drain_node(id, cx);
            }
            (Party::Proxy(id), Wire::Paxos(msg)) => {
                let Some(proxy) = self.proxy_mut(id).live.as_mut() else {
                    return;
                };
                proxy.step(msg);
                self.drain_proxy(id, cx);
            }
            (Party::Replica(id), Wire::Paxos(mut msg)) => {
                tamper(self.flavor.mutant, &mut msg, true);
                let Some(replica) = self.replica_mut(id).live.as_mut() else {
                    return;
                };
                replica.step(msg);
                self.drain_replica(id, cx);
            }
            (Party::Matchmaker(id), Wire::Match(request)) => self.match_request(id, request, cx),
            (Party::Matchmaker(id), Wire::Gc(request)) => self.gc_request(id, &request, cx),
            (Party::Matchmaker(id), Wire::Publish(request)) => {
                self.publish_request(id, &request, cx);
            }
            (Party::Matchmaker(id), Wire::Reconfigure(request)) => {
                self.reconfigure_request(id, request, cx);
            }
            (to, wire) => unreachable!("no party sends {wire:?} to {to:?}"),
        }
    }

    fn nodes(&self) -> Vec<Party> {
        let mut parties: Vec<Party> = self.nodes.iter().map(|s| Party::Node(s.id)).collect();
        parties.extend(self.proxies.iter().map(|s| Party::Proxy(s.id)));
        parties.extend(self.replicas.iter().map(|s| Party::Replica(s.id)));
        parties.extend(self.matchmakers.iter().map(|s| Party::Matchmaker(s.id)));
        parties
    }

    /// A steer: a node whose disk holds writes that no fsync covers yet is
    /// three times as likely to crash as another party, so a crash that
    /// loses them ([`reach::LOST_UNSYNCED`]) does not hang on the schedule.
    /// Such a window is short (the next synced batch closes it), and a
    /// crash in it is a legal fault: the contract lets a relaxed batch wait
    /// for the next fsync.
    fn node_weight(&self, party: &Party) -> u32 {
        match *party {
            Party::Node(id) if self.node(id).live.is_some() && self.node(id).unsynced() > 0 => 3,
            _ => 1,
        }
    }

    fn is_up(&self, party: &Party) -> bool {
        match *party {
            Party::Node(id) => self.node(id).live.is_some(),
            Party::Proxy(id) => self.proxy(id).live.is_some(),
            Party::Replica(id) => self.replica(id).live.is_some(),
            Party::Matchmaker(id) => self.matchmaker(id).live.is_some(),
        }
    }

    fn crash(&mut self, party: &Party, cx: &mut Cx<'_, Self>) {
        match *party {
            Party::Node(id) => self.crash_node(id, cx),
            Party::Proxy(id) => {
                let keep = self.flavor.mutant == Some(Mutant::KeepProxyRounds);
                let site = self.proxy_mut(id);
                sometimes!(
                    site.live.as_ref().is_some_and(|p| !p.rounds().is_empty()),
                    reach::PROXY_CRASH
                );
                if !keep {
                    site.live = None;
                }
                let event = self.tracer.proxy_crash(id);
                cx.record(event);
            }
            Party::Replica(id) => self.replica_mut(id).live = None,
            Party::Matchmaker(id) => self.matchmaker_mut(id).live = None,
        }
    }

    fn restart(&mut self, party: &Party, cx: &mut Cx<'_, Self>) {
        match *party {
            Party::Node(id) => {
                // A retired node stays down until the operator books it,
                // and a node that stopped for good stays down.
                if self.node(id).live.is_none()
                    && !self.retired.contains(&id)
                    && self.control.dead != Some(id)
                {
                    self.boot_node(id, true, cx);
                }
            }
            Party::Proxy(id) => {
                let config = self.proxy_config(id);
                let limits = self.proxy_limits();
                let site = self.proxy_mut(id);
                if site.live.is_none() && !site.dead_for_good {
                    site.live = Some(ProxyLeader::new(id, &config, limits));
                }
            }
            Party::Replica(id) => {
                if self.replica(id).live.is_none() {
                    let limits = self.flavor.limits;
                    let admitted: Vec<NodeId> = self.admitted(id).into_iter().collect();
                    let site = self.replica_mut(id);
                    let mut replica = ReplicaNode::new(&site.disk, limits);
                    // The registry is durable: a replica that boots admits
                    // again the late nodes it admitted.
                    let _ = replica.extend_pool(&admitted);
                    let from = replica.replica().folded();
                    site.live = Some(replica);
                    cx.record(ClusterEvent::ReplicaBoot { replica: id, from });
                    self.drain_replica(id, cx);
                }
            }
            Party::Matchmaker(id) => self.matchmaker_mut(id).boot(),
        }
    }

    fn calm(&mut self, cx: &mut Cx<'_, Self>) {
        self.calmed = true;
        // A pool that lags the deployment's registry is a fault of the
        // walk. The contract of the core: the driver admits to the pool
        // (`extend_pool`) every node that its registry, a replicated log,
        // registers, so each fold reaches every registration once the
        // network is calm. Until then a party refuses a late node's
        // messages: that costs liveness, never safety, and the walk judges
        // safety through the whole lag. The node and replica pools catch up
        // here, with the other faults. The pools of the proxy leaders lag
        // through the quiet phase, where leaders stand and reconfigure onto
        // a late node, so a delegation meets a proxy that ignores it until a
        // take-back or a move runs the round (`ProxyLeader::admits`). A
        // lagging-reader walk keeps every pool lagging through the quiet
        // phase. Every pool catches up when the settle rounds start
        // (`Cluster::tick_all`).
        if self.flavor.steer != Steer::LaggingReader {
            self.admit_nodes_and_replicas();
        }
        self.stop_for_good(cx);
        // Some proxies stay dead for the tail: a leader's take-back is the
        // only way their rounds decide.
        for site in &mut self.proxies {
            if cx.rng().chance(1, 3) {
                site.dead_for_good = true;
                if site.live.take().is_some() {
                    cx.record(self.tracer.proxy_crash(site.id));
                }
            }
        }
    }

    fn check(&mut self) {
        for site in &self.nodes {
            let Some(node) = &site.live else { continue };
            for (slot, command) in node.chosen_commands() {
                self.ledger
                    .learned(&format!("node {}", site.id.0), slot, command);
            }
            crate::oracle::memory::retained(
                &format!("node {}", site.id.0),
                node.retained_bytes(),
                node.acceptor()
                    .records()
                    .values()
                    .map(|(_, command)| command),
            );
        }
        for site in &self.replicas {
            let Some(replica) = &site.live else { continue };
            for (slot, command) in replica.chosen_commands() {
                self.ledger
                    .learned(&format!("replica {}", site.id.0), slot, command);
            }
            crate::oracle::memory::retained(
                &format!("replica {}", site.id.0),
                replica.retained_bytes(),
                replica.chosen_commands().map(|(_, command)| command),
            );
        }
    }

    fn converged(&self) -> Result<(), String> {
        let leaders = self.leaders();
        let [leader] = leaders.as_slice() else {
            return Err(format!("{} leaders", leaders.len()));
        };
        let node = self.node(*leader).live.as_ref().expect("a leader is up");
        let frontier = node.proposer().next_slot();
        if !node.proposer().rounds().is_empty() || !node.delegated_rounds().is_empty() {
            return Err(format!(
                "leader {} holds open rounds {:?}",
                leader.0,
                node.proposer().rounds().keys().collect::<Vec<_>>()
            ));
        }
        for site in &self.proxies {
            if site.live.as_ref().is_some_and(|p| !p.rounds().is_empty()) {
                return Err(format!("proxy {} holds open rounds", site.id.0));
            }
        }
        for site in self
            .nodes
            .iter()
            .filter(|s| !self.retired.contains(&s.id) && self.control.dead != Some(s.id))
        {
            let reached = site.live.as_ref().map(|n| n.replica().first_unchosen());
            if reached.is_none_or(|r| r < frontier) {
                return Err(format!(
                    "node {} reached {reached:?}, the frontier is {}",
                    site.id.0, frontier.0
                ));
            }
        }
        // A replica keeps one segment (R6): a sealed one learned all it
        // keeps once it reached the end of its segment, and one that found
        // its segment released while it lagged keeps nothing more.
        for site in self
            .replicas
            .iter()
            .filter(|s| !s.live.as_ref().is_some_and(ReplicaNode::is_trimmed))
        {
            let reached = site.live.as_ref().map(|r| r.replica().first_unchosen());
            let end = site
                .live
                .as_ref()
                .and_then(|r| r.replica().sealed_end())
                .map_or(frontier, |end| end.min(frontier));
            if reached.is_none_or(|r| r < end) {
                return Err(format!(
                    "replica {} reached {reached:?}, the frontier is {}",
                    site.id.0, frontier.0
                ));
            }
        }
        self.healed()
    }

    fn describe(&self) -> String {
        let mut out = format!("flavor {}", self.flavor.name);
        for site in &self.nodes {
            let _ = write!(
                out,
                "\n  node {}: up={} disk promise {:?} chosen {:?} floor {:?}",
                site.id.0,
                site.live.is_some(),
                site.disk.hard_state().max_promised_ballot,
                site.disk.hard_state().chosen_index,
                paros_consensus::Storage::first_slot(&site.disk),
            );
            if let Some(node) = &site.live {
                let _ = write!(
                    out,
                    " role {:?} ballot {:?} next {} rounds {:?} delegated {:?}",
                    node.role(),
                    node.ballot(),
                    node.proposer().next_slot().0,
                    node.proposer().rounds().keys().collect::<Vec<_>>(),
                    node.delegated_rounds(),
                );
            }
        }
        for site in &self.proxies {
            let _ = write!(
                out,
                "\n  proxy {}: up={} dead for good={} rounds {:?}",
                site.id.0,
                site.live.is_some(),
                site.dead_for_good,
                site.live.as_ref().map(|p| p
                    .rounds()
                    .by_slot()
                    .keys()
                    .copied()
                    .collect::<Vec<_>>()),
            );
        }
        for site in &self.replicas {
            let _ = write!(
                out,
                "\n  replica {}: up={} reached {:?}",
                site.id.0,
                site.live.is_some(),
                site.live.as_ref().map(|r| r.replica().first_unchosen()),
            );
        }
        let _ = write!(out, "\n  chosen up to {:?}", self.ledger.highest_chosen());
        out
    }
}

impl Cluster {
    /// The cluster of `flavor`, every party up.
    ///
    /// # Panics
    ///
    /// If the flavor does not lay out a well-formed configuration.
    pub fn build(flavor: Flavor, cx: &mut Cx<'_, Self>) -> Self {
        let members: Vec<NodeId> = (0..flavor.acceptors).map(NodeId).collect();
        let pool: Vec<NodeId> = (0..flavor.acceptors + flavor.spares).map(NodeId).collect();
        let config = AcceptorConfig::new(members.clone(), flavor.quorum);
        let rule = Phase2Rule::new(&members, flavor.quorum);
        let take_back_after = 1 + cx.rng().below(3);
        let expire_after = 1 + cx.rng().below(BUDGET_SPREAD);
        let mut cluster = Self {
            config,
            nodes: Vec::new(),
            proxies: Vec::new(),
            replicas: Vec::new(),
            matchmakers: Vec::new(),
            ledger: Ledger::new(rule),
            reads: Reads::default(),
            next_ctx: 1,
            stepped_after_compaction: false,
            held_recovery: BTreeSet::new(),
            client: Client::default(),
            calmed: false,
            next_uuid: 0,
            take_back_after,
            expire_after,
            leader_ballot: None,
            early_gc: BTreeSet::new(),
            tracer: trace::Tracer::default(),
            reconfigurers: Vec::new(),
            decree_rounds: Vec::new(),
            retired: BTreeSet::new(),
            asked: Vec::new(),
            checking: None,
            admitted: BTreeMap::new(),
            proxy_admitted: BTreeMap::new(),
            ready_digest: paros_consensus::types::FNV_OFFSET,
            control: control::Controllers::default(),
            pool,
            flavor,
        };
        cluster.decree_rounds = vec![0; cluster.pool.len()];
        cluster.reconfigurers = cluster
            .pool
            .iter()
            .map(|id| cluster.reconfigurer(*id))
            .collect();
        cluster.nodes = cluster
            .pool
            .iter()
            .map(|id| NodeSite::new(*id, cluster.party_config(*id)))
            .collect();
        cluster.replicas = (0..cluster.flavor.replicas)
            .map(|i| {
                let id = NodeId(REPLICA_BASE + i);
                ReplicaSite::new(id, cluster.party_config(id), cluster.flavor.limits)
            })
            .collect();
        let bootstrap = cluster.bootstrap_matchmakers();
        cluster.matchmakers = cluster
            .matchmaker_ids()
            .iter()
            .map(|id| MatchmakerSite::new(*id, bootstrap.clone(), cluster.flavor.limits))
            .collect();
        cluster.proxies = (0..cluster.flavor.proxies)
            .map(|i| {
                let id = ProxyId(i);
                ProxySite::new(id, &cluster.proxy_config(id), cluster.proxy_limits())
            })
            .collect();
        for id in cluster.pool.clone() {
            cluster.boot_node(id, false, cx);
        }
        for id in cluster.replica_ids() {
            cluster.drain_replica(id, cx);
        }
        cluster
    }

    /// The flavor of this cluster.
    #[must_use]
    pub fn flavor(&self) -> &Flavor {
        &self.flavor
    }

    /// The safety ledger.
    #[must_use]
    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    /// The read oracle.
    #[must_use]
    pub fn reads(&self) -> &Reads {
        &self.reads
    }

    /// The matchmaker pool: the bootstrap set and the spares.
    fn matchmaker_ids(&self) -> Vec<MatchmakerId> {
        (0..self.flavor.matchmakers + self.flavor.matchmaker_spares)
            .map(MatchmakerId)
            .collect()
    }

    /// The matchmaker set of generation 0.
    fn bootstrap_matchmakers(&self) -> Vec<MatchmakerId> {
        (0..self.flavor.matchmakers).map(MatchmakerId).collect()
    }

    /// The nodes that join the pool late: the last spares.
    fn late_ids(&self) -> Vec<NodeId> {
        let all = self.flavor.acceptors + self.flavor.spares;
        (all - self.flavor.late..all).map(NodeId).collect()
    }

    /// Whether a lagging-reader walk skips the tick of node `id` before
    /// the settle rounds. Node 2's clock stands still once its membership
    /// probe heard what is in force, so it never campaigns and its read
    /// fence stays on the old configuration. The clock of any other node
    /// whose pool lags runs at a quarter of the rate, so it campaigns seldom
    /// and a leader that moved onto a late node keeps its leadership long
    /// enough to choose writes.
    fn skips_tick(&self, id: NodeId, cx: &mut Cx<'_, Self>) -> bool {
        if self.flavor.steer != Steer::LaggingReader || cx.phase() == Phase::Settle {
            return false;
        }
        let heard = self
            .node(id)
            .live
            .as_ref()
            .is_some_and(|n| n.belief_source() != BeliefSource::Bootstrap);
        (id == NodeId(2) && heard) || (self.lags(id) && !cx.rng().chance(1, 4))
    }

    /// Whether a lagging-reader walk loses `envelope`, sent by `from`, until
    /// the settle rounds (#1228 (stale read by a lagging reader)). Node 2
    /// is cut from every node that moved (a late node, or one that admitted
    /// it): it hears no campaign and no beat of a leadership that moved. The
    /// other lagging nodes get no `Commit` and no catch-up from a node that
    /// moved, and lose a quarter of their `Prepare`s and of the read answers
    /// of a node that moved. A lagging reader can then promise a newer
    /// leadership, hold no vote of its writes, and still find, in node 2, an
    /// answer on its old basis.
    fn lost_to_a_lagging_pool(
        &self,
        from: NodeId,
        envelope: &Envelope,
        cx: &mut Cx<'_, Self>,
    ) -> bool {
        if self.flavor.steer != Steer::LaggingReader || cx.phase() == Phase::Settle {
            return false;
        }
        let Party::Node(to) = envelope.to else {
            return false;
        };
        let late = self.late_ids();
        let moved = late.contains(&from) || late.iter().any(|l| self.admitted(from).contains(l));
        if to == NodeId(2) {
            return moved;
        }
        if !self.lags(to) {
            return false;
        }
        match envelope.msg {
            // The log of a leadership that moved reaches a lagging pool
            // only once the pools catch up: its own votes then stay below
            // the writes that leadership chose.
            Wire::Paxos(Message::Commit { .. } | Message::CatchUpResponse { .. }) => moved,
            Wire::Paxos(Message::Prepare { .. }) => cx.rng().chance(1, 4),
            Wire::Paxos(Message::PreReadAck { .. }) => moved && cx.rng().chance(1, 4),
            _ => false,
        }
    }

    /// Whether the pool of node `id` lags: it is not a late node and has
    /// not admitted every late node.
    fn lags(&self, id: NodeId) -> bool {
        let late = self.late_ids();
        !late.contains(&id) && late.iter().any(|l| !self.admitted(id).contains(l))
    }

    /// Send what node `id` released, less what a lagging-reader walk
    /// loses on the way ([`Cluster::lost_to_a_lagging_pool`]).
    fn send_from(&self, id: NodeId, out: Vec<Envelope>, cx: &mut Cx<'_, Self>) {
        for envelope in out {
            if !self.lost_to_a_lagging_pool(id, &envelope, cx) {
                cx.send(envelope);
            }
        }
    }

    /// The late nodes node `id` admitted to its pool.
    fn admitted(&self, id: NodeId) -> BTreeSet<NodeId> {
        self.admitted.get(&id).cloned().unwrap_or_default()
    }

    fn party_config(&self, id: NodeId) -> Config {
        let matchmakers = self.bootstrap_matchmakers();
        let late = self.late_ids();
        Config {
            peers: self.config.members().to_vec(),
            quorum_system: self.flavor.quorum,
            nodes: if matchmakers.is_empty() {
                Vec::new()
            } else {
                // A late node knows the whole pool; no other node boots
                // knowing a late node.
                self.pool
                    .iter()
                    .copied()
                    .filter(|n| *n == id || !late.contains(n))
                    .collect()
            },
            matchmaker_pool: self.matchmaker_ids(),
            matchmakers,
            proxy_count: usize::try_from(self.flavor.proxies).expect("a few proxies"),
            replica_count: usize::try_from(self.flavor.replicas).expect("a few replicas"),
            segment_replicas: self.flavor.segment_replicas.then(|| {
                ReplicaSet::new((0..self.flavor.replicas).map(|i| NodeId(REPLICA_BASE + i)))
            }),
            ..Config::new(id, paros_consensus::journal::JournalIdentifier::UNSET)
        }
    }

    /// The deployment a proxy leader reads: the bootstrap configuration
    /// with no counts (a proxy never names a reply owner), and the whole
    /// pool, late nodes too. A proxy has no registry fold of its own here:
    /// it runs any configuration a leader of the deployment registered.
    fn proxy_config(&self, id: ProxyId) -> Config {
        let mut config = self.party_config(NodeId(0));
        config.proxy_count = 0;
        config.replica_count = 0;
        if !config.matchmakers.is_empty() {
            // A proxy boots knowing no late node, as a node does, and
            // admits again the late nodes it admitted before.
            config
                .nodes
                .extend(self.proxy_admitted.get(&id).into_iter().flatten());
            config.nodes.sort_unstable();
            config.nodes.dedup();
        }
        config
    }

    fn replica_ids(&self) -> Vec<NodeId> {
        self.replicas.iter().map(|s| s.id).collect()
    }

    fn index(id: u64) -> usize {
        usize::try_from(id).expect("a small id")
    }

    fn node(&self, id: NodeId) -> &NodeSite {
        &self.nodes[Self::index(id.0)]
    }

    fn node_mut(&mut self, id: NodeId) -> &mut NodeSite {
        &mut self.nodes[Self::index(id.0)]
    }

    fn proxy(&self, id: ProxyId) -> &ProxySite {
        &self.proxies[Self::index(id.0)]
    }

    fn proxy_mut(&mut self, id: ProxyId) -> &mut ProxySite {
        &mut self.proxies[Self::index(id.0)]
    }

    fn replica(&self, id: NodeId) -> &ReplicaSite {
        &self.replicas[Self::index(id.0 - REPLICA_BASE)]
    }

    fn replica_mut(&mut self, id: NodeId) -> &mut ReplicaSite {
        &mut self.replicas[Self::index(id.0 - REPLICA_BASE)]
    }

    fn matchmaker(&self, id: MatchmakerId) -> &MatchmakerSite {
        &self.matchmakers[Self::index(id.0)]
    }

    fn matchmaker_mut(&mut self, id: MatchmakerId) -> &mut MatchmakerSite {
        &mut self.matchmakers[Self::index(id.0)]
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|s| s.live.as_ref().is_some_and(ColocatedNode::is_leader))
            .map(|s| s.id)
            .collect()
    }

    // ---- lifecycle --------------------------------------------------------

    /// Crash node `id`. When writes wait for an fsync, half the crashes
    /// lose a suffix of them: the contract lets a relaxed batch
    /// (`MustSync::Relaxed`) wait for the next fsync.
    fn crash_node(&mut self, id: NodeId, cx: &mut Cx<'_, Self>) {
        let mutant = self.flavor.mutant;
        let unsynced = self.node(id).unsynced();
        let keep = (unsynced > 0 && cx.rng().chance(1, 2))
            .then(|| Self::index(cx.rng().below(unsynced as u64)));
        sometimes!(keep.is_some_and(|k| k < unsynced), reach::LOST_UNSYNCED);
        self.node_mut(id).crash(mutant, keep);
        // The crash loses the reconfigurer; it starts again from the decree
        // round on disk (the driver syncs it before the batch leaves).
        self.reconfigurers[Self::index(id.0)] = self.reconfigurer(id);
        // What the crash left on the disk is what the restart reads.
        let index = self.node(id).disk.hard_state().chosen_index;
        self.ledger.crashed(&format!("node {}", id.0), index);
    }

    /// Boot node `id` from its disk; `again` when it ran before.
    fn boot_node(&mut self, id: NodeId, again: bool, cx: &mut Cx<'_, Self>) {
        if again {
            let upto = self
                .node(id)
                .disk
                .hard_state()
                .chosen_index
                .map_or(Slot(0), |ci| Slot(ci.0 + 1));
            let faulty = paros_consensus::Storage::faulty_entries(&self.node(id).disk);
            for event in self.tracer.restart(id, upto, &faulty) {
                cx.record(event);
            }
        }
        let limits = self.flavor.limits;
        let site = self.node_mut(id);
        sometimes!(
            site.disk.hard_state().max_promised_ballot != paros_consensus::Ballot::zero(),
            reach::REBOOT
        );
        let mut node = ColocatedNode::new(&site.disk, limits);
        node.set_election_timeout(TIMEOUT_MIN + cx.rng().below(TIMEOUT_SPREAD));
        let admitted: Vec<NodeId> = self.admitted(id).into_iter().collect();
        if !admitted.is_empty() {
            let _ = node.extend_pool(&admitted);
        }
        let site = self.node_mut(id);
        // A party never boots below its disk: the disk is the floor.
        let disk = site.disk.hard_state().chosen_index;
        assert!(
            node.replica().chosen_index() >= disk,
            "{}: node {} booted at {:?} below its disk at {disk:?}",
            crate::oracle::ledger::PREFIX_SHRINKS,
            id.0,
            node.replica().chosen_index(),
        );
        let hole = node.replica().fold_hole();
        site.live = Some(node);
        if let Some(event) = self.tracer.hole(id, hole) {
            cx.record(event);
        }
        let name = format!("node {}", id.0);
        self.ledger.disk_chosen(&name, disk);
        self.drain_node(id, cx);
    }

    // ---- draining ---------------------------------------------------------

    /// The FNV-1a fold of every batch that a node, a proxy leader or a
    /// replica of the core handed this driver, in order: what each party
    /// sent, wrote, learned and served. Two builds of the core that give
    /// the same digest on the same seed behave the same on it, which is
    /// how `tests/differential.rs` checks a change that says it changes no
    /// behaviour.
    #[must_use]
    pub fn ready_digest(&self) -> u64 {
        self.ready_digest
    }

    /// Fold the digest of one batch into [`Cluster::ready_digest`].
    fn fold_digest(&mut self, batch: u64) {
        self.ready_digest = fnv1a(self.ready_digest, &batch.to_le_bytes());
    }

    /// Judge each `Commit` that node `id` routed to the replicas of a
    /// segment in `batch` ([`ROUTE_TRUE`](crate::oracle::ledger::ROUTE_TRUE)).
    fn judge_routes(
        &mut self,
        id: NodeId,
        batch: &[(Audience, Message)],
        first: Option<&ReplicaSet>,
    ) {
        for (audience, message) in batch {
            if let (Audience::ReplicasOf { segment, replicas }, Message::Commit { slot, .. }) =
                (audience, message)
            {
                self.ledger.route_holds(
                    &format!("node {}", id.0),
                    *slot,
                    *segment,
                    replicas,
                    first,
                );
            }
        }
    }

    /// Run the driver loop of node `id` until its batches are empty.
    fn drain_node(&mut self, id: NodeId, cx: &mut Cx<'_, Self>) {
        let replicas = self.replica_ids();
        for _ in 0..DRAIN_LIMIT {
            let chaos = cx.phase() == Phase::Chaos;
            let crash_before = chaos && cx.rng().chance(1, CRASH_ODDS);
            let crash_between = chaos && cx.rng().chance(1, CRASH_ODDS);
            let mutant = self.flavor.mutant;
            let site = self.node_mut(id);
            let Some(node) = site.live.as_mut() else {
                return;
            };
            // A node sends to the pool it knows, which a late node may not
            // be in yet.
            let pool = node.pool().to_vec();
            let seen = Seen::of(node);
            let known = Known::of(node);
            let ready = node.ready();
            let digest = node_digest(id, &ready);
            let writes = ready.writes().to_vec();
            let mut batch: Vec<(Audience, Message)> = ready.messages().to_vec();
            plant(mutant, &mut batch);
            plant_driver(mutant, &known, &mut batch);
            let mut out = route(&batch, &pool, Some(id), &replicas);
            let committed = ready.committed().to_vec();
            let read_states = ready.read_states().to_vec();
            let more = ready.recovery_batch().is_some();
            let matchmaking = requests_of(ready.match_requests());
            let mut gc: Vec<(MatchmakerId, GcRequest)> = ready.gc_requests().to_vec();
            out.extend(to_matchmakers(ready.match_requests(), &gc));
            out.extend(publishes_of(ready.segment_publishes()));
            // The batch flies until its persist below (`Cluster::land`).
            let flight = ready.release();
            self.fold_digest(digest);
            if mutant == Some(Mutant::EarlyGc) {
                self.early_gc(id, &seen, &mut gc, &mut out);
            }
            let gc: Vec<GcRequest> = gc.into_iter().map(|(_, request)| request).collect();
            if writes.is_empty()
                && out.is_empty()
                && committed.is_empty()
                && read_states.is_empty()
                && !more
            {
                self.land_empty(id, flight, seen, cx);
                return;
            }
            if mutant == Some(Mutant::EarlyCommit) {
                out.extend(early_commits(&out, id, &pool, &replicas));
            }
            if crash_before && !writes.is_empty() {
                sometimes!(true, reach::CRASH_BEFORE_PERSIST);
                self.crash_node(id, cx);
                cx.crashed(&Party::Node(id));
                return;
            }
            let writes = self.fold_batch(id, writes, &committed, cx);
            let before = self.node(id).disk.hard_state().chosen_index;
            if mutant != Some(Mutant::SkipPersist) {
                self.persist_node(id, &writes);
            }
            self.land(id, flight);
            let traced = Traced {
                seen,
                writes: &writes,
                messages: &batch,
                matchmaking: &matchmaking,
                gc: &gc,
                committed: &committed,
            };
            self.trace_batch(id, traced, before, cx);
            if !self.rot_at_compaction(id, &writes, cx) {
                // A fault stopped it between the persist and the send.
                return;
            }
            if crash_between {
                sometimes!(true, reach::CRASH_BEFORE_SEND);
                self.crash_node(id, cx);
                cx.crashed(&Party::Node(id));
                return;
            }
            for envelope in &out {
                self.judge_sent(&format!("node {}", id.0), envelope);
            }
            self.judge_routes(id, &batch, known.first.as_ref());
            self.send_from(id, out, cx);
            for (slot, _, command, outcome) in &committed {
                self.applied(&format!("node {}", id.0), *slot, command, outcome, cx);
            }
            for state in &read_states {
                sometimes!(true, reach::NODE_READ);
                cx.record(ClusterEvent::ReadServe {
                    node: id,
                    ctx: state.ctx,
                    index: state.index,
                });
                self.reads
                    .answered(&format!("node {}", id.0), state.ctx, state.index);
            }
            let hold = chaos && self.holds_recovery(id, more, cx);
            if !self.after_batch(id, hold) {
                return;
            }
        }
    }

    /// Give a flushed batch back to its node: its writes are durable. The
    /// driver persists after it lets go of the buckets, so the core holds
    /// the rule of one batch in flight (`Ready::release`). A crash drops the
    /// flight with the node.
    fn land(&mut self, id: NodeId, flight: paros_consensus::node::Flight) {
        let node = self
            .node_mut(id)
            .live
            .as_mut()
            .expect("a batch in flight has a live node");
        node.durable(flight);
        assert!(!node.in_flight(), "a landed batch is out of flight");
    }

    /// Land an empty batch. It can still follow a change of leadership (a
    /// win with nothing to recover): the tracer records it.
    fn land_empty(
        &mut self,
        id: NodeId,
        flight: paros_consensus::node::Flight,
        seen: Seen,
        cx: &mut Cx<'_, Self>,
    ) {
        self.land(id, flight);
        let before = self.node(id).disk.hard_state().chosen_index;
        self.trace_batch(id, Traced::empty(seen), before, cx);
    }

    /// [`Mutant::EarlyGc`]: node `id` asks every matchmaker for GC at the
    /// ballot it leads, once, whatever its configuration learned.
    fn early_gc(
        &mut self,
        id: NodeId,
        seen: &Seen,
        gc: &mut Vec<(MatchmakerId, GcRequest)>,
        out: &mut Vec<Envelope>,
    ) {
        let (Some((ballot, _, _)), Some(set)) = (seen.leading, seen.set.as_ref()) else {
            return;
        };
        if !self.early_gc.insert(ballot) {
            return;
        }
        let request = GcRequest {
            from: id,
            generation: set.generation,
            watermark: ballot,
        };
        for m in set.members() {
            gc.push((*m, request));
            out.push(Envelope {
                to: Party::Matchmaker(*m),
                msg: Wire::Gc(request),
            });
        }
    }

    /// Whether the slow driver of [`Steer::Rolling`] holds back the next
    /// recovery page of node `id`, in the chaos phase: from a batch that
    /// carried a `page`, for half the recoveries, it holds every page until
    /// the next roll at that node, so
    /// the leader rolls while its recovery still holds values to propose
    /// again. The calm phase releases every hold.
    fn holds_recovery(&mut self, id: NodeId, page: bool, cx: &mut Cx<'_, Self>) -> bool {
        self.flavor.steer == Steer::Rolling
            && (self.held_recovery.contains(&id)
                || (page
                    && self.recovers_values(id)
                    && cx.rng().chance(1, 2)
                    && self.held_recovery.insert(id)))
    }

    /// Whether node `id` leads with a recovery that still holds values to
    /// propose again.
    fn recovers_values(&self, id: NodeId) -> bool {
        self.node(id).live.as_ref().is_some_and(|node| {
            node.is_leader()
                && node
                    .proposer()
                    .recovery()
                    .is_some_and(|recovery| recovery.pending().next().is_some())
        })
    }

    /// After a batch of node `id`: advance its recovery (unless the driver
    /// `hold`s it back, which a later batch releases) and note the ballot of
    /// a leader. Returns whether the node still runs.
    fn after_batch(&mut self, id: NodeId, hold: bool) -> bool {
        let Some(node) = self.node_mut(id).live.as_mut() else {
            return false;
        };
        if !hold {
            node.advance_recovery();
        }
        if node.is_leader() {
            let ballot = node.ballot();
            sometimes!(
                self.leader_ballot.is_some_and(|b| b < ballot),
                reach::NEW_LEADER
            );
            self.leader_ballot = self.leader_ballot.max(Some(ballot));
        }
        true
    }

    /// Before node `id` persists `writes`: record the fold of the slots the
    /// batch applies, which comes before the floor it writes, and plant
    /// [`Mutant::CompactPastFold`].
    fn fold_batch(
        &mut self,
        id: NodeId,
        mut writes: Vec<WriteOp>,
        committed: &[(Slot, Ballot, Command, paros_consensus::journal::Outcome)],
        cx: &mut Cx<'_, Self>,
    ) -> Vec<WriteOp> {
        for (slot, _, _, outcome) in committed {
            if let Some(event) = self.tracer.fold(*slot, outcome) {
                cx.record(event);
            }
        }
        if self.flavor.mutant == Some(Mutant::CompactPastFold) {
            compact_past_fold(self.node(id).disk.hard_state().chosen_index, &mut writes);
        }
        writes
    }

    /// Record the steps of a batch of node `id` that reached its disk
    /// ([`trace`]); `before` is the disk's chosen index before the batch
    /// (`None`: nothing chosen).
    fn trace_batch(
        &mut self,
        id: NodeId,
        traced: Traced<'_>,
        before: Option<Slot>,
        cx: &mut Cx<'_, Self>,
    ) {
        let site = self.node(id);
        let after = site.disk.hard_state().chosen_index;
        let node = site.live.as_ref();
        let first = before.map_or(0, |s| s.0 + 1);
        let last = after.map_or(0, |s| s.0 + 1);
        let learned = (first..last)
            .filter_map(|s| {
                let command = node?.chosen_at(Slot(s)).or_else(|| {
                    traced
                        .committed
                        .iter()
                        .find(|(slot, ..)| slot.0 == s)
                        .map(|(_, _, command, _)| command)
                })?;
                Some((Slot(s), Cmd::of(command)))
            })
            .collect();
        // The basis of a plain node is its static configuration, bound to
        // no won ballot: only a deployment with matchmakers learns one.
        let basis = site
            .live
            .as_ref()
            .filter(|_| self.flavor.matchmakers > 0)
            .and_then(ColocatedNode::read_basis)
            .map(|basis| (basis.since, basis.fence));
        // A campaign of the batch starts at the hole of the fold.
        let hole = site
            .live
            .as_ref()
            .and_then(|node| node.replica().fold_hole());
        if let Some(event) = self.tracer.hole(id, hole) {
            cx.record(event);
        }
        let events = self.tracer.batch(&trace::Batch {
            id,
            leading: traced.seen.leading,
            writes: traced.writes,
            messages: traced.messages,
            learned,
            known: traced.seen.known,
            prior: traced.seen.prior,
            matchmaking: traced.matchmaking,
            gc: traced.gc,
            basis,
        });
        for event in events {
            cx.record(event);
        }
    }

    /// Persist `writes` on node `id`'s disk and feed the ledger.
    fn persist_node(&mut self, id: NodeId, writes: &[WriteOp]) {
        let name = format!("node {}", id.0);
        for op in writes {
            self.node_mut(id).persist(op);
            match op {
                WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                    slot,
                    ballot,
                    value,
                }) => {
                    self.ledger.voted(id, *slot, *ballot, value);
                    let chosen = self.ledger.chosen().get(slot).copied();
                    sometimes!(chosen.is_some(), reach::CHOSEN);
                    if self.flavor.allows(Action::Reconfigure) {
                        let moved = chosen.is_some_and(|(at, _)| {
                            self.ledger.rule(at).members() != self.config.members()
                        });
                        sometimes!(moved, reach::CHOSEN_RECONFIGURED);
                    }
                }
                WriteOp::Truncate { .. } => sometimes!(true, reach::COMPACTED),
                WriteOp::TrimmedTo { state, .. } => {
                    sometimes!(true, reach::TRIM_JUMP);
                    sometimes!(state.segment.id.0 > 0, reach::ROLL_JUMP);
                }
                _ => {}
            }
        }
        if let Some(node) = self.node(id).live.as_ref() {
            let journal = node.replica().journal();
            sometimes!(
                journal
                    .held
                    .is_some_and(|held| held.next_seq > journal.first_seq
                        && node.acceptor().first_slot() >= held.end),
                reach::HELD_DROP
            );
        }
        let synced = MustSync::for_batch(writes) == MustSync::Sync;
        if synced {
            self.node_mut(id).sync();
        }
        let hard = self.node(id).disk.hard_state();
        self.ledger.promised(id, hard.max_promised_ballot);
        self.ledger.disk_chosen(&name, hard.chosen_index);
        if synced {
            self.ledger.synced(&name, hard.chosen_index);
        }
    }

    /// The oracles on a message that leaves `who`.
    fn judge_sent(&mut self, who: &str, envelope: &Envelope) {
        let Wire::Paxos(msg) = &envelope.msg else {
            return;
        };
        match msg {
            Message::Promise {
                from,
                ballot,
                accepted,
                ..
            } => self.ledger.promise_sent(*from, *ballot, accepted),
            Message::Accepted {
                from,
                ballot,
                slot,
                vhash,
            } => self.ledger.accepted_sent(*from, *slot, *ballot, *vhash),
            Message::Commit {
                ballot,
                slot,
                command,
                ..
            } => self.ledger.commit_sent(who, *slot, *ballot, command),
            Message::Accept {
                ballot,
                slot,
                command,
                ..
            } => self.ledger.proposed(who, *slot, *ballot, command),
            _ => {}
        }
    }

    /// The fold of an answer that node `id` is about to step: a
    /// `PreReadAck` from a member of the row of a read it holds open, the
    /// first from that member (`QuorumReads::fold` ignores the others).
    fn read_fold(&self, id: NodeId, msg: &Message) -> Option<ClusterEvent> {
        let Message::PreReadAck {
            from,
            ctx,
            config_since,
            ..
        } = msg
        else {
            return None;
        };
        let node = self.node(id).live.as_ref()?;
        let read = node
            .quorum_reads()
            .pending()
            .iter()
            .find(|r| r.ctx() == *ctx)?;
        // An answer above the basis ends the read whatever its stage; any
        // other counts only while the read tallies, once per member.
        let supersedes = config_since
            .zip(self.tracer.read_basis(*ctx))
            .is_some_and(|(fence, basis)| fence > basis);
        let counts = supersedes
            || (read.confirmed_index().is_none() && !read.watermarks().contains_key(from));
        let folds = node.pool().contains(from)
            && read.config().phase1_addressees(read.row()).contains(from)
            && counts;
        folds.then_some(ClusterEvent::ReadFold {
            node: id,
            acceptor: *from,
            ctx: *ctx,
        })
    }

    fn applied(
        &mut self,
        who: &str,
        slot: Slot,
        command: &Command,
        outcome: &Outcome,
        cx: &mut Cx<'_, Self>,
    ) {
        let before = self.ledger.applied(who, slot, command, outcome);
        self.reads.applied(slot);
        if let Some(event) = self.tracer.fold(slot, outcome) {
            cx.record(event);
        }
        if let Some(event) = self.tracer.applied(slot) {
            cx.record(event);
        }
        if let Command::Write(entry) = command {
            self.client.heard(slot, entry, outcome);
            sometimes!(
                matches!(outcome, Outcome::Accepted { .. }),
                reach::WRITE_APPLIED
            );
            sometimes!(
                self.client.gap_refused && matches!(outcome, Outcome::Accepted { .. }),
                reach::WRITE_AFTER_GAP
            );
            sometimes!(
                matches!(outcome, Outcome::Duplicate { .. }),
                reach::DUPLICATE
            );
            sometimes!(
                matches!(outcome, Outcome::Refused(view)
                    if view.leader.is_some_and(|l| l != entry.leader)),
                reach::FENCED
            );
        }
        sometimes!(
            matches!(outcome, Outcome::Trimmed(after) if after.first_seq > before.first_seq),
            reach::TRUNCATE_MOVED
        );
        sometimes!(matches!(outcome, Outcome::Leader(_)), reach::SET_LEADER);
        sometimes!(matches!(outcome, Outcome::Rolled(_)), reach::ROLLED);
    }

    fn drain_proxy(&mut self, id: ProxyId, cx: &mut Cx<'_, Self>) {
        let pool = self.pool.clone();
        let replicas = self.replica_ids();
        let Some(proxy) = self.proxy_mut(id).live.as_mut() else {
            return;
        };
        let ready = proxy.ready();
        let messages = ready.messages().to_vec();
        ready.advance();
        self.fold_digest(digest_of(&(id, &messages)));
        let out = route(&messages, &pool, None, &replicas);
        let events = self.tracer.proxy_batch(id, &messages);
        for event in events {
            cx.record(event);
        }
        let who = format!("proxy {}", id.0);
        for envelope in &out {
            if matches!(envelope.msg, Wire::Paxos(Message::Commit { .. })) {
                sometimes!(true, reach::PROXY_COMMIT);
            }
            self.judge_sent(&who, envelope);
        }
        for envelope in out {
            cx.send(envelope);
        }
    }

    fn drain_replica(&mut self, id: NodeId, cx: &mut Cx<'_, Self>) {
        let pool = self.pool.clone();
        let who = format!("replica {}", id.0);
        for _ in 0..DRAIN_LIMIT {
            let site = self.replica_mut(id);
            let Some(replica) = site.live.as_mut() else {
                return;
            };
            let ready = replica.ready();
            let digest = digest_of(&(
                id,
                ready.writes(),
                ready.messages(),
                ready.committed(),
                ready.read_states(),
            ));
            let writes = ready.writes().to_vec();
            let out = route(ready.messages(), &pool, Some(id), &[]);
            let committed = ready.committed().to_vec();
            let read_states = ready.read_states().to_vec();
            ready.advance();
            sometimes!(replica.is_trimmed(), reach::REPLICA_TRIMMED);
            if writes.is_empty() && out.is_empty() && committed.is_empty() && read_states.is_empty()
            {
                self.fold_digest(digest);
                return;
            }
            for op in &writes {
                assert!(
                    !matches!(op, WriteOp::Acceptor(_)),
                    "a replica never writes a vote"
                );
                site.disk.apply(std::slice::from_ref(op));
            }
            for event in trace::replica_writes(id, &writes, true) {
                cx.record(event);
            }
            let index = site.disk.hard_state().chosen_index;
            self.fold_digest(digest);
            self.ledger.disk_chosen(&who, index);
            for envelope in out {
                cx.send(envelope);
            }
            for (slot, _, command, outcome) in &committed {
                sometimes!(true, reach::REPLICA_APPLIED);
                cx.record(ClusterEvent::ReplicaApply {
                    replica: id,
                    slot: *slot,
                });
                self.applied(&who, *slot, command, outcome, cx);
            }
            for event in trace::replica_writes(id, &writes, false) {
                cx.record(event);
            }
            for state in &read_states {
                sometimes!(true, reach::REPLICA_READ);
                cx.record(ClusterEvent::ReplicaServe {
                    replica: id,
                    ctx: state.ctx,
                    index: state.index,
                });
                self.reads.answered(&who, state.ctx, state.index);
            }
            if let Some(replica) = self.replica_mut(id).live.as_mut() {
                replica.advance_recovery();
            }
        }
    }

    // ---- the clock and the clients ------------------------------------

    /// The limits of a proxy leader: the flavor's, with this seed's
    /// retention budget.
    fn proxy_limits(&self) -> Limits {
        Limits {
            proxy_retention: self.expire_after,
            ..self.flavor.limits
        }
    }

    fn tick_all(&mut self, cx: &mut Cx<'_, Self>) {
        if cx.phase() == Phase::Settle {
            self.admit_all();
        }
        self.step_down_after_compaction(cx);
        for i in 0..self.proxies.len() {
            let id = ProxyId(i as u64);
            if let Some(proxy) = self.proxy_mut(id).live.as_mut() {
                let evicted = proxy.tick();
                for slot in evicted {
                    if let Some(event) = self.tracer.proxy_expired(id, slot) {
                        cx.record(event);
                    }
                }
                self.drain_proxy(id, cx);
            }
        }
        let resend = self.flavor.mutant != Some(Mutant::NoResend);
        let matchmaking = self.flavor.matchmakers > 0;
        let after = self.take_back_after;
        for id in self.pool.clone() {
            if self.skips_tick(id, cx) {
                continue;
            }
            let Some(node) = self.node_mut(id).live.as_mut() else {
                continue;
            };
            node.tick();
            if node.needs_election_timeout() {
                node.set_election_timeout(TIMEOUT_MIN + cx.rng().below(TIMEOUT_SPREAD));
            }
            if resend {
                node.resend_pending();
                if matchmaking {
                    node.resend_matchmaking();
                    node.resend_gc();
                    node.resend_segment_publishes();
                }
            }
            let taken = node.take_back_delegated(after);
            sometimes!(!taken.is_empty(), reach::TAKE_BACK);
            self.drain_node(id, cx);
            self.tick_reconfigurer(id, cx);
        }
        for id in self.replica_ids() {
            if let Some(replica) = self.replica_mut(id).live.as_mut() {
                replica.tick();
                self.drain_replica(id, cx);
            }
        }
        self.control(cx);
    }

    /// A leader the client reaches, and the journal as that leader folds it.
    /// Whether the network loses `msg` on its way to node `id`, a steer of
    /// the chaos: a leader misses every message that would depose it
    /// (a `Prepare`, an `Accept`, a `Heartbeat`, a `Nack` or a `Relinquish`
    /// above its ballot), but hears every `Commit`. So a leader learns a
    /// slot chosen above its ballot while it still leads, the window that
    /// [`Cluster::pick_leader`] meets. A lost message is a fault the contract
    /// allows; once the faults stop, nothing is lost.
    fn deafens(&self, id: NodeId, msg: &Message) -> bool {
        if self.calmed {
            return false;
        }
        let Some(node) = self.node(id).live.as_ref() else {
            return false;
        };
        if !node.is_leader() {
            return false;
        }
        let ballot = match msg {
            Message::Prepare { ballot, .. }
            | Message::Accept { ballot, .. }
            | Message::Heartbeat { ballot, .. }
            | Message::Nack { ballot, .. }
            | Message::Relinquish { ballot, .. } => *ballot,
            _ => return false,
        };
        let deaf = ballot > node.ballot();
        sometimes!(deaf, reach::DEAF_LEADER);
        deaf
    }

    /// The leader a leader move acts on. A leadership that a higher ballot
    /// passed (it learned a `Commit` above its ballot, and no beat or nack
    /// demoted it yet) is a rare window that a move almost never meets by
    /// chance, so the move takes such a leader first: the core must refuse
    /// its proposals and record none of its rounds.
    fn pick_leader(&self, cx: &mut Cx<'_, Self>) -> Option<NodeId> {
        let leaders = self.leaders();
        let passed: Vec<NodeId> = leaders
            .iter()
            .copied()
            .filter(|&id| {
                self.node(id)
                    .live
                    .as_ref()
                    .is_some_and(|node| node.ballot() < node.hard_state().max_promised_ballot)
            })
            .collect();
        sometimes!(!passed.is_empty(), reach::PASSED_LEADER);
        let pool = if passed.is_empty() { &leaders } else { &passed };
        cx.rng().pick(pool).copied()
    }

    /// The client of [`Steer::Compacting`]: it claims a journal that has
    /// no leader uuid, else it writes, and truncates after one write in
    /// two.
    fn write_and_truncate(&mut self, cx: &mut Cx<'_, Self>) {
        let unclaimed = self.leaders().iter().any(|id| {
            self.node(*id)
                .live
                .as_ref()
                .is_some_and(|node| node.replica().journal().leader.is_none())
        });
        if unclaimed {
            self.claim(cx);
            return;
        }
        self.propose(cx);
        if cx.rng().chance(1, 2) {
            self.truncate(cx);
        }
    }

    /// The first leader that compacted in a [`Steer::Compacting`] walk
    /// steps down, once: the next campaign asks the acceptors that
    /// compacted, over their floors. Compactions come late in a walk, when
    /// the client's moves have stopped, so the clock makes this move.
    fn step_down_after_compaction(&mut self, cx: &mut Cx<'_, Self>) {
        if self.flavor.steer != Steer::Compacting || self.stepped_after_compaction {
            return;
        }
        let compacted = self.leaders().into_iter().find(|id| {
            self.node(*id)
                .live
                .as_ref()
                .is_some_and(|node| node.acceptor().first_slot() > Slot(0))
        });
        if let Some(leader) = compacted {
            self.stepped_after_compaction = true;
            self.node_mut(leader)
                .live
                .as_mut()
                .expect("a leader is up")
                .step_down();
            self.drain_node(leader, cx);
        }
    }

    fn propose(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(leader) = self.pick_leader(cx) else {
            return;
        };
        let journal = self
            .node(leader)
            .live
            .as_ref()
            .expect("a leader is up")
            .replica()
            .journal();
        let entry = self.client.write(&journal, cx);
        let delegation = match cx.rng().below(10) {
            0..=5 => Delegation::Auto,
            6..=7 if self.flavor.proxies > 0 => {
                Delegation::To(ProxyId(cx.rng().below(self.flavor.proxies)))
            }
            _ => Delegation::Colocated,
        };
        let column = match self.flavor.quorum {
            QuorumSystem::Grid { cols, .. } if cx.rng().chance(1, 3) => {
                Some(Self::index(cx.rng().below(cols as u64)))
            }
            _ => None,
        };
        let node = self.node_mut(leader).live.as_mut().expect("a leader is up");
        let result = node.propose_in(entry, column, delegation);
        sometimes!(
            column.is_some() && matches!(result, ProposeResult::Accepted(_)),
            reach::NAMED_COLUMN
        );
        self.drain_node(leader, cx);
    }

    fn claim(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(leader) = self.pick_leader(cx) else {
            return;
        };
        let old = self
            .node(leader)
            .live
            .as_ref()
            .expect("a leader is up")
            .replica()
            .journal()
            .leader;
        self.next_uuid = self.next_uuid.max(old.map_or(0, |u| u.0)) + 1;
        let new = LeaderUuid(self.next_uuid);
        let node = self.node_mut(leader).live.as_mut().expect("a leader is up");
        let _ = node.propose_control(Control::SetLeader { new, old });
        self.drain_node(leader, cx);
    }

    /// The operator cuts the log at a leader (`ColocatedNode::roll`): half
    /// the rolls keep the replicas (`None`), half name two nodes of the pool.
    /// A leader whose recovery the slow driver of [`Steer::Rolling`] holds
    /// back, if one stands.
    fn held_leader(&self) -> Option<NodeId> {
        self.leaders()
            .into_iter()
            .find(|id| self.held_recovery.contains(id))
    }

    fn roll(&mut self, cx: &mut Cx<'_, Self>) {
        // The operator rolls first at a leader whose recovery the slow
        // driver holds back.
        let Some(leader) = self.held_leader().or_else(|| self.pick_leader(cx)) else {
            return;
        };
        let replicas = if cx.rng().chance(1, 2) {
            let mut pool = self.pool.clone();
            let first = pool.remove(Self::index(cx.rng().below(pool.len() as u64)));
            let second = pool[Self::index(cx.rng().below(pool.len() as u64))];
            Some(ReplicaSet::new([first, second]))
        } else {
            None
        };
        let node = self.node_mut(leader).live.as_mut().expect("a leader is up");
        let open = node
            .proposer()
            .rounds()
            .values()
            .any(|round| matches!(round.command(), Command::Roll(_)));
        let recovering = node
            .proposer()
            .recovery()
            .is_some_and(|recovery| recovery.pending().next().is_some());
        let boundary_base = node.replica().journal();
        let boundary_replicas = boundary_base.replicas.clone();
        let tail = boundary_base.segment;
        let set = node.matchmaker_set().cloned();
        let result = node.roll(replicas.clone());
        let accepted = matches!(result, RollResult::Accepted { .. });
        if self.flavor.mutant == Some(Mutant::EarlyPublish)
            && let (RollResult::Accepted { slot, next }, Some(set)) = (&result, set)
            && next.0 == tail.id.0 + 1
        {
            let publish = SegmentPublish {
                from: leader,
                generation: set.generation,
                sealed: tail.id,
                start: tail.start,
                end: Slot(slot.0 + 1),
                opening: paros_consensus::journal::Opening {
                    // The mutant guesses the boundary from the leader's fold
                    // at the proposal, before the Roll is chosen.
                    boundary: paros_consensus::journal::JournalState {
                        segment: paros_consensus::journal::Segment {
                            id: *next,
                            start: Slot(slot.0 + 1),
                        },
                        replicas: replicas.clone().or(boundary_replicas),
                        ..boundary_base
                    },
                    replicas,
                },
            };
            for to in set.members() {
                cx.send(Envelope {
                    to: Party::Matchmaker(*to),
                    msg: Wire::Publish(publish.clone()),
                });
            }
        }
        sometimes!(open && accepted, reach::ROLL_PIPELINED);
        sometimes!(recovering && accepted, reach::ROLL_IN_RECOVERY);
        self.held_recovery.remove(&leader);
        self.drain_node(leader, cx);
        // Half the time the leader steps down as its `Roll` leaves: the next
        // leader's Phase 1 recovers what it left open, and its recovery
        // (which the slow driver may hold back) feeds its tail.
        if accepted && cx.rng().chance(1, 2) {
            if let Some(node) = self.node_mut(leader).live.as_mut() {
                node.step_down();
            }
            self.drain_node(leader, cx);
        }
    }

    fn truncate(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(leader) = self.pick_leader(cx) else {
            return;
        };
        let node = self.node(leader).live.as_ref().expect("a leader is up");
        let journal = node.replica().journal();
        let Some(uuid) = journal.leader else {
            return;
        };
        let span = journal.next_seq.0.saturating_sub(journal.first_seq.0);
        let up_to = journal.first_seq.0 + cx.rng().below(span + 1);
        let node = self.node_mut(leader).live.as_mut().expect("a leader is up");
        let _ = node.propose_control(Control::Truncate {
            leader: uuid,
            up_to: Seq(up_to),
        });
        self.drain_node(leader, cx);
    }

    fn read(&mut self, cx: &mut Cx<'_, Self>) {
        let mut parties: Vec<Party> = self.pool.iter().map(|id| Party::Node(*id)).collect();
        parties.extend(self.replica_ids().into_iter().map(Party::Replica));
        if self.flavor.steer == Steer::LaggingReader {
            let lagging: Vec<Party> = self
                .pool
                .iter()
                .filter(|id| self.lags(**id))
                .map(|id| Party::Node(*id))
                .collect();
            // Of those, a node that promised above its belief: the reader
            // of #1228.
            let promised: Vec<Party> = lagging
                .iter()
                .copied()
                .filter(|party| {
                    let Party::Node(id) = party else { return false };
                    self.node(*id)
                        .live
                        .as_ref()
                        .is_some_and(|n| n.acceptor().promised() > n.acceptors_since())
                })
                .collect();
            let lagging = if promised.is_empty() {
                lagging
            } else {
                promised
            };
            if !lagging.is_empty() {
                parties = lagging;
            }
        }
        let Some(&party) = cx.rng().pick(&parties) else {
            return;
        };
        let ctx = self.next_ctx;
        self.next_ctx += 1;
        let local = self.flavor.mutant == Some(Mutant::LocalRead);
        match party {
            Party::Node(id) => {
                let Some(node) = self.nodes[Self::index(id.0)].live.as_mut() else {
                    return;
                };
                self.reads.started(ctx);
                if local {
                    let index = node.replica().chosen_index();
                    self.reads.answered(&format!("node {}", id.0), ctx, index);
                    return;
                }
                let basis = node.read_basis().map(|basis| (basis.since, basis.fence));
                let opened = node.quorum_read(ctx);
                assert_ne!(
                    opened,
                    ReadOpen::TokenInUse,
                    "the walk draws a fresh token for each read"
                );
                if let Some(basis) = basis
                    && let Some(read) = node
                        .quorum_reads()
                        .pending()
                        .iter()
                        .find(|r| r.ctx() == ctx)
                {
                    let own = read
                        .watermarks()
                        .get(&id)
                        .map(|&watermark| OwnAnswer { watermark });
                    cx.record(self.tracer.read_start(id, ctx, basis, own));
                }
                self.drain_node(id, cx);
            }
            Party::Replica(id) => {
                let Some(replica) = self.replicas[Self::index(id.0 - REPLICA_BASE)]
                    .live
                    .as_mut()
                else {
                    return;
                };
                self.reads.started(ctx);
                cx.record(ClusterEvent::ReplicaRead { replica: id, ctx });
                if local {
                    let index = replica.replica().chosen_index();
                    cx.record(ClusterEvent::ReplicaServe {
                        replica: id,
                        ctx,
                        index,
                    });
                    self.reads
                        .answered(&format!("replica {}", id.0), ctx, index);
                    return;
                }
                let opened = replica.quorum_read_in(ctx, None);
                assert_ne!(
                    opened,
                    ReadOpen::TokenInUse,
                    "the walk draws a fresh token for each read"
                );
                self.drain_replica(id, cx);
            }
            Party::Proxy(_) | Party::Matchmaker(_) => {}
        }
    }

    fn step_down(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(leader) = self.pick_leader(cx) else {
            return;
        };
        self.node_mut(leader)
            .live
            .as_mut()
            .expect("a leader is up")
            .step_down();
        self.drain_node(leader, cx);
    }

    /// A node that does not lead campaigns while a leader stands. With
    /// [`Cluster::deafens`], the leader it deposes may go on leading for a
    /// while and learn the new leader's decisions.
    fn usurp(&mut self, cx: &mut Cx<'_, Self>) {
        if self.leaders().is_empty() {
            return;
        }
        let others: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|s| s.live.as_ref().is_some_and(|node| !node.is_leader()))
            .map(|s| s.id)
            .collect();
        let Some(&id) = cx.rng().pick(&others) else {
            return;
        };
        sometimes!(true, reach::USURPED);
        self.node_mut(id)
            .live
            .as_mut()
            .expect("a live node")
            .campaign();
        self.drain_node(id, cx);
    }

    /// A leader moves the journal to a configuration drawn from the
    /// pool: a round change at a new ballot (Matchmaker Paxos §4).
    fn reconfigure(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(leader) = self.pick_leader(cx) else {
            return;
        };
        let size = Self::index(self.flavor.acceptors);
        // The leader draws from the pool it knows.
        let node = self.node(leader).live.as_ref().expect("a leader is up");
        // A lagging-reader walk moves onto a late node once, and stays: the
        // lagging members keep reading on the configuration it left.
        let late_ids = self.late_ids();
        if self.flavor.steer == Steer::LaggingReader
            && node
                .acceptors()
                .members()
                .iter()
                .any(|m| late_ids.contains(m))
        {
            return;
        }
        let mut members = node.pool().to_vec();
        // A late node the leader admitted is a new machine: the operator
        // moves the journal onto it half of the time.
        let late: Vec<NodeId> = self
            .late_ids()
            .into_iter()
            .filter(|l| members.contains(l))
            .collect();
        let onto = cx
            .rng()
            .pick(&late)
            .copied()
            .filter(|_| self.flavor.steer == Steer::LaggingReader || cx.rng().chance(1, 2));
        // A lagging-reader walk moves away from the members whose pools lag:
        // a quorum of the new configuration then chooses without them, while
        // they still read on the old one.
        let lagging: Vec<NodeId> = if self.flavor.steer == Steer::LaggingReader {
            node.acceptors()
                .members()
                .iter()
                .copied()
                .filter(|id| self.lags(*id))
                .collect()
        } else {
            Vec::new()
        };
        while members.len() > size {
            if let Some(i) = members.iter().position(|m| lagging.contains(m)) {
                members.remove(i);
                continue;
            }
            let out = Self::index(cx.rng().below(members.len() as u64));
            if Some(members[out]) != onto {
                members.remove(out);
            }
        }
        let config = AcceptorConfig::new(members, self.flavor.quorum).with_counts(
            Self::index(self.flavor.proxies),
            Self::index(self.flavor.replicas),
        );
        let node = self.node_mut(leader).live.as_mut().expect("a leader is up");
        let result = node.reconfigure(&config);
        sometimes!(
            matches!(result, ReconfigureResult::Started(_)),
            reach::RECONFIGURED
        );
        if let ReconfigureResult::Started(ballot) = result {
            self.asked.push((ballot, config.members().to_vec()));
        }
        self.drain_node(leader, cx);
    }

    /// A matchmaker answers a registration: step, persist, reply.
    fn match_request(&mut self, id: MatchmakerId, request: MatchRequest, cx: &mut Cx<'_, Self>) {
        let site = self.matchmaker_mut(id);
        let Some(matchmaker) = site.live.as_mut() else {
            return;
        };
        let ballot = request.ballot;
        let held = matchmaker.registry().contains_key(&ballot);
        matchmaker.step(request);
        let holds = matchmaker.registry().contains_key(&ballot);
        site.persist();
        if holds && !held {
            let event = self.tracer.register(id, ballot);
            cx.record(event.expect("a new registration is a step"));
        }
        let site = self.matchmaker_mut(id);
        let replies = site.take_replies();
        self.bind_registrations(id);
        for reply in replies {
            cx.send(Envelope {
                to: Party::Node(reply.to),
                msg: Wire::MatchReply(reply),
            });
        }
    }

    /// A matchmaker raises its GC watermark: persist, then ack.
    fn gc_request(&mut self, id: MatchmakerId, request: &GcRequest, cx: &mut Cx<'_, Self>) {
        let site = self.matchmaker_mut(id);
        let Some(ack) = site.gc(request) else {
            return;
        };
        sometimes!(ack.applied, reach::GC_ACKED);
        if ack.applied
            && let Some(event) = self.tracer.gc_ack(id, request.watermark)
        {
            cx.record(event);
        }
        cx.send(Envelope {
            to: Party::Node(request.from),
            msg: Wire::GcAck(ack),
        });
    }

    /// A matchmaker merges a publish of the chain: persist, judge the chain
    /// it now holds, then ack.
    fn publish_request(
        &mut self,
        id: MatchmakerId,
        request: &SegmentPublish,
        cx: &mut Cx<'_, Self>,
    ) {
        let site = self.matchmaker_mut(id);
        let Some(ack) = site.publish(request) else {
            return;
        };
        self.judge_chain(id);
        cx.send(Envelope {
            to: Party::Node(request.from),
            msg: Wire::PublishAck(ack),
        });
    }

    /// [`CHAIN_TRUE`](crate::oracle::ledger::CHAIN_TRUE): every record of
    /// the chain that matchmaker `id` holds is the one the chosen log gives.
    pub(super) fn judge_chain(&mut self, id: MatchmakerId) {
        let Some(chain) = self
            .matchmaker(id)
            .live
            .as_ref()
            .and_then(|matchmaker| matchmaker.chain().cloned())
        else {
            return;
        };
        self.ledger
            .chain_holds(&format!("matchmaker {}", id.0), &chain);
    }

    /// Every configuration a matchmaker holds is bound to its ballot in
    /// the ledger, before any vote at that ballot is judged.
    fn bind_registrations(&mut self, id: MatchmakerId) {
        let Some(matchmaker) = self.matchmaker(id).live.as_ref() else {
            return;
        };
        let bound: Vec<(paros_consensus::Ballot, Phase2Rule)> = matchmaker
            .registry()
            .iter()
            .map(|(ballot, registration)| {
                let config = &registration.config;
                (
                    *ballot,
                    Phase2Rule::new(config.members(), config.quorum_system()),
                )
            })
            .collect();
        for (ballot, rule) in bound {
            self.ledger.bind(ballot, &rule);
        }
    }

    /// A leader moves one of its stuck rounds (open for at least one
    /// re-send, not delegated) to another column of its grid.
    fn move_round(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(leader) = self.pick_leader(cx) else {
            return;
        };
        let node = self.node(leader).live.as_ref().expect("a leader is up");
        let QuorumSystem::Grid { cols, .. } = node.acceptors().quorum_system() else {
            return;
        };
        let stuck: Vec<(Slot, Option<usize>)> = node
            .proposer()
            .rounds()
            .iter()
            .filter(|(_, round)| round.proxy().is_none() && round.resends() > 0)
            .map(|(slot, round)| (*slot, round.column()))
            .collect();
        let Some(&(slot, from)) = cx.rng().pick(&stuck) else {
            return;
        };
        let column = Self::index(cx.rng().below(cols as u64));
        if Some(column) == from {
            return;
        }
        let node = self.node_mut(leader).live.as_mut().expect("a leader is up");
        let moved = node.move_round(slot, column);
        sometimes!(moved, reach::MOVED_ROUND);
        self.drain_node(leader, cx);
    }

    fn handoff(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(leader) = self.pick_leader(cx) else {
            return;
        };
        let node = self.node(leader).live.as_ref().expect("a leader is up");
        let candidates = node.handoff_candidates();
        let Some(&target) = cx.rng().pick(&candidates) else {
            return;
        };
        let node = self.node_mut(leader).live.as_mut().expect("a leader is up");
        let receipt = node.relinquish_to(target);
        sometimes!(receipt.is_some_and(|r| r.pending > 0), reach::HANDOFF);
        self.drain_node(leader, cx);
    }
}

/// The client that writes the journal, as a pipelined writer does
/// (`docs/architecture/02-data-plane.md` §2.2): it writes under the uuid it
/// believes leads, at the next position it believes free, without waiting
/// for the answer of the write in front. A wrong belief is refused at
/// apply. A refusal at a wrong position names the journal's next position,
/// and the client continues from there. It also retries one of its last
/// writes unchanged, and writes under a superseded uuid on purpose.
#[derive(Debug, Default)]
struct Client {
    /// The next position the client believes free.
    next_seq: u64,
    /// The highest slot whose refusal the client learned from.
    heard: Option<Slot>,
    /// The client's last writes, oldest first.
    sent: Vec<Entry>,
    /// A write of the current uuid was refused at a wrong position.
    gap_refused: bool,
}

impl Client {
    /// The next write, against the `journal` that the leader folds.
    fn write(
        &mut self,
        journal: &paros_consensus::journal::state::JournalState,
        cx: &mut Cx<'_, Cluster>,
    ) -> Entry {
        let draw = cx.rng().below(10);
        if draw < 2
            && let Some(retry) = cx.rng().pick(&self.sent)
        {
            sometimes!(true, reach::RETRY);
            return retry.clone();
        }
        if draw == 2
            && let Some(current) = journal.leader.filter(|u| u.0 > 1)
        {
            // A writer that a later `SetLeader` superseded.
            return Self::entry(LeaderUuid(current.0 - 1), journal.next_seq.0);
        }
        self.next_seq = self.next_seq.max(journal.next_seq.0);
        let entry = Self::entry(journal.leader.unwrap_or(LeaderUuid(1)), self.next_seq);
        self.next_seq += 1;
        if self.sent.len() == RETRY_WINDOW {
            self.sent.remove(0);
        }
        self.sent.push(entry.clone());
        entry
    }

    /// One record whose bytes name its position: a retry carries the same.
    fn entry(leader: LeaderUuid, seq: u64) -> Entry {
        Entry {
            leader,
            seq: Seq(seq),
            records: vec![Value::from(seq.to_le_bytes().to_vec())],
        }
    }

    /// A party applied `entry` at `slot` with `outcome`. A refusal at a
    /// wrong position under the uuid in force names where the journal
    /// stands: the client continues from there, once per slot, so a late
    /// party's old refusal never moves it back.
    fn heard(&mut self, slot: Slot, entry: &Entry, outcome: &Outcome) {
        let Outcome::Refused(view) = outcome else {
            return;
        };
        if view.leader != Some(entry.leader) || entry.seq == view.next_seq {
            return;
        }
        self.gap_refused = true;
        if self.heard.is_none_or(|h| slot > h) {
            self.heard = Some(slot);
            self.next_seq = view.next_seq.0;
        }
    }
}

/// The bugs of `mutant` that the network plants in a message it delivers
/// (to a replica when `to_replica`).
fn tamper(mutant: Option<Mutant>, msg: &mut Message, to_replica: bool) {
    match (mutant, msg) {
        (Some(Mutant::CorruptCommit), Message::Commit { command, .. }) if to_replica => {
            *command = Command::Write(Entry {
                leader: LeaderUuid(u128::MAX),
                seq: Seq(0),
                records: vec![Value::from(b"corrupt".to_vec())],
            });
        }
        (Some(Mutant::CorruptTrimJump), Message::TrimmedTo { state, .. }) => {
            state.next_seq = Seq(state.next_seq.0 + 1);
        }
        // A tail one segment ahead, shaped as a real one: segment `k + 1`
        // opened at the slot after the start of `k`, still below the point.
        (Some(Mutant::CorruptTrimSegment), Message::TrimmedTo { point, state, .. })
            if state.segment.start < *point =>
        {
            state.segment = state.segment.rolled_at(state.segment.start);
        }
        _ => {}
    }
}

/// What the driver reads of a node before its batch: the tracer's view of
/// its leadership and campaign, and where a GC request goes.
struct Seen {
    /// The ballot it leads, its frontier, and whether a handoff installed
    /// it. A new leader's frontier is one past the fence it opened its
    /// authority at (`try_become_leader`), before any allocation.
    leading: Option<(paros_consensus::Ballot, Slot, bool)>,
    /// The slots a leader knows chosen below its frontier: its recovery
    /// skips them.
    known: Vec<(Slot, Cmd)>,
    /// The configurations of an open campaign (`H_b`), that a `Prepare`
    /// of the batch opens Phase 1 over.
    prior: Vec<Vec<NodeId>>,
    set: Option<paros_consensus::membership::MatchmakerSet>,
}

impl Seen {
    fn of(node: &ColocatedNode) -> Self {
        let leading = node.is_leader().then(|| {
            let frontier = node.proposer().fence().map_or(0, |s| s.0 + 1);
            let handoff = matches!(
                node.leadership_origin(),
                paros_consensus::node::LeadershipOrigin::Handoff { .. }
            );
            (node.ballot(), Slot(frontier), handoff)
        });
        let known = leading.map_or_else(Vec::new, |(_, frontier, _)| {
            node.chosen_commands()
                .take_while(|(s, _)| *s < frontier)
                .map(|(s, command)| (s, Cmd::of(command)))
                .collect()
        });
        let prior = node.proposer().election().map_or_else(Vec::new, |e| {
            e.prior().iter().map(|c| c.members().to_vec()).collect()
        });
        Self {
            leading,
            known,
            prior,
            set: node.matchmaker_set().cloned(),
        }
    }
}

/// What a persisted batch of a node hands the tracer, beside its learns.
struct Traced<'a> {
    seen: Seen,
    writes: &'a [WriteOp],
    messages: &'a [(Audience, Message)],
    matchmaking: &'a [MatchRequest],
    gc: &'a [GcRequest],
    /// The slots the batch applied: a compaction in the same batch drops
    /// them from the chosen map before the tracer reads it.
    committed: &'a [(Slot, Ballot, Command, paros_consensus::journal::Outcome)],
}

impl Traced<'_> {
    /// A batch that writes and sends nothing.
    fn empty(seen: Seen) -> Self {
        Self {
            seen,
            writes: &[],
            messages: &[],
            matchmaking: &[],
            gc: &[],
            committed: &[],
        }
    }
}

/// The bugs of `mutant` that rewrite the messages of a node's batch.
fn plant(mutant: Option<Mutant>, batch: &mut [(Audience, Message)]) {
    match mutant {
        Some(Mutant::HideVotes) => hide_votes(batch),
        Some(Mutant::RewindHandoff) => rewind_handoffs(batch),
        Some(Mutant::HideWatermark) => hide_watermarks(batch),
        Some(Mutant::HideFaulty) => hide_faulty(batch),
        _ => {}
    }
}

/// What a node's driver reads off the node before its batch, for the bugs
/// it plants and the routes it judges.
struct Known {
    /// The ballot of the node's belief.
    belief: Ballot,
    /// The journal state at the head of the node's fold.
    tail: paros_consensus::journal::state::JournalState,
    /// The replicas of the first segment that the node's configuration names.
    first: Option<ReplicaSet>,
    /// The first slot the node's fold has not applied.
    folded: Slot,
    /// The lowest slot at or past `folded` where the node holds a `Roll`
    /// record: the last slot of the tail segment as far as the node knows.
    tail_end: Option<Slot>,
}

impl Known {
    fn of(node: &paros_consensus::ColocatedNode) -> Self {
        let folded = node.replica().folded();
        let tail_end = node
            .acceptor()
            .records()
            .range(folded..)
            .find(|(_, (_, command))| matches!(command, Command::Roll(_)))
            .map(|(slot, _)| *slot);
        Self {
            belief: node.acceptors_since(),
            tail: node.replica().journal(),
            first: node.config().segment_replicas.clone(),
            folded,
            tail_end,
        }
    }
}

/// The bugs a node's driver plants on a batch from what the node knows:
/// [`Mutant::BeliefFence`] from its belief, and [`Mutant::RouteFromTail`]
/// and [`Mutant::RouteOnDecision`] from its tail segment and the replicas
/// of its first segment.
fn plant_driver(mutant: Option<Mutant>, known: &Known, batch: &mut Vec<(Audience, Message)>) {
    match mutant {
        Some(Mutant::BeliefFence) => belief_fences(known.belief, batch),
        Some(Mutant::RouteFromTail) => route_from_tail(&known.tail, known.first.as_ref(), batch),
        Some(Mutant::RouteOnDecision) => {
            route_on_decision(known, batch);
        }
        _ => {}
    }
}

/// The [`Mutant::RouteOnDecision`] bug: a `Commit` a leader sends to the
/// learners for a slot its fold has not reached also goes to the replicas
/// of its tail segment, at the decision, over the hole below it. It routes
/// no slot past a `Roll` the leader holds above its fold: that slot is of
/// a later segment, a second bug that `ROUTE_TRUE` judges, and the control
/// plants one bug.
fn route_on_decision(known: &Known, batch: &mut Vec<(Audience, Message)>) {
    let tail = &known.tail;
    let Some(replicas) = tail.replicas.clone().or_else(|| known.first.clone()) else {
        return;
    };
    let decided: Vec<(Audience, Message)> = batch
        .iter()
        .filter(|(audience, message)| {
            !matches!(audience, Audience::ReplicasOf { .. })
                && matches!(message, Message::Commit { slot, .. }
                    if *slot > known.folded && known.tail_end.is_none_or(|end| *slot <= end))
        })
        .map(|(_, message)| {
            (
                Audience::ReplicasOf {
                    segment: tail.segment.id,
                    replicas: replicas.clone(),
                },
                message.clone(),
            )
        })
        .collect();
    batch.extend(decided);
}

/// The [`Mutant::RouteFromTail`] bug: every `Commit` to the replicas of a
/// segment names the tail segment of the leader's fold and its replicas.
fn route_from_tail(
    tail: &paros_consensus::journal::state::JournalState,
    first: Option<&ReplicaSet>,
    batch: &mut [(Audience, Message)],
) {
    let Some(replicas) = tail.replicas.clone().or_else(|| first.cloned()) else {
        return;
    };
    for (audience, _) in batch {
        if let Audience::ReplicasOf { .. } = audience {
            *audience = Audience::ReplicasOf {
                segment: tail.segment.id,
                replicas: replicas.clone(),
            };
        }
    }
}

/// The envelopes of a node's requests to the matchmakers.
/// The envelopes of a batch's publishes of the chain of segments.
fn publishes_of(
    publishes: &[(MatchmakerId, SegmentPublish)],
) -> impl Iterator<Item = Envelope> + '_ {
    publishes.iter().map(|(to, request)| Envelope {
        to: Party::Matchmaker(*to),
        msg: Wire::Publish(request.clone()),
    })
}

fn to_matchmakers<'a>(
    matches: &'a [(MatchmakerId, MatchRequest)],
    gc: &'a [(MatchmakerId, GcRequest)],
) -> impl Iterator<Item = Envelope> + 'a {
    let matches = matches.iter().map(|(to, request)| Envelope {
        to: Party::Matchmaker(*to),
        msg: Wire::Match(request.clone()),
    });
    let gc = gc.iter().map(|(to, request)| Envelope {
        to: Party::Matchmaker(*to),
        msg: Wire::Gc(*request),
    });
    matches.chain(gc)
}

/// The envelopes of one batch: each audience resolved to parties. A
/// `Learners` audience reaches the replicas too, as a deployment map
/// routes it.
/// The FNV-1a digest of the `Debug` text of `batch`: every field of every
/// value the core put in it, in order.
fn digest_of(batch: &impl std::fmt::Debug) -> u64 {
    // A journal state with no replica set and no held prefix digests as it
    // did before the fields existed, so the digests of the flavors with no
    // replicas of a segment and no `Hold` stay those of the code before them.
    let text = format!("{batch:?}")
        .replace(", held: None }", " }")
        .replace(", replicas: None }", " }");
    fnv1a(paros_consensus::types::FNV_OFFSET, text.as_bytes())
}

/// The match requests of a batch, without their addressees.
fn requests_of(requests: &[(MatchmakerId, MatchRequest)]) -> Vec<MatchRequest> {
    requests
        .iter()
        .map(|(_, request)| request.clone())
        .collect()
}

/// The digest of one batch of node `id` ([`digest_of`]). A batch with no
/// publish of the chain digests as it did before the chain existed, so the
/// recorded digests of every walk that rolls nothing prove that it behaves
/// as before.
fn node_digest(id: NodeId, ready: &paros_consensus::Ready<'_>) -> u64 {
    let batch = (
        id,
        ready.writes(),
        ready.messages(),
        ready.committed(),
        ready.read_states(),
        ready.recovery_batch(),
        ready.match_requests(),
        ready.gc_requests(),
    );
    if ready.segment_publishes().is_empty() {
        digest_of(&batch)
    } else {
        digest_of(&(batch, ready.segment_publishes()))
    }
}

fn route(
    messages: &[(Audience, Message)],
    pool: &[NodeId],
    me: Option<NodeId>,
    replicas: &[NodeId],
) -> Vec<Envelope> {
    let mut out = Vec::new();
    for (audience, msg) in messages {
        if let Some(proxy) = audience.proxy() {
            out.push(Envelope {
                to: Party::Proxy(proxy),
                msg: Wire::Paxos(msg.clone()),
            });
            continue;
        }
        // The replicas of a segment are replica sites, or nodes of the pool
        // when a `Roll` named nodes.
        if let Audience::ReplicasOf {
            replicas: named, ..
        } = audience
        {
            for to in named.members().iter().filter(|to| Some(**to) != me) {
                let to = if replicas.contains(to) {
                    Party::Replica(*to)
                } else {
                    Party::Node(*to)
                };
                out.push(Envelope {
                    to,
                    msg: Wire::Paxos(msg.clone()),
                });
            }
            continue;
        }
        if let Audience::Node(to) = audience
            && replicas.contains(to)
        {
            out.push(Envelope {
                to: Party::Replica(*to),
                msg: Wire::Paxos(msg.clone()),
            });
            continue;
        }
        let resolved = match me {
            Some(me) if pool.contains(&me) => audience.resolve(pool, me),
            _ => audience.resolve_from_proxy(pool),
        };
        out.extend(resolved.into_iter().map(|to| Envelope {
            to: Party::Node(to),
            msg: Wire::Paxos(msg.clone()),
        }));
        if *audience == Audience::Learners {
            out.extend(
                replicas
                    .iter()
                    .filter(|r| Some(**r) != me)
                    .map(|r| Envelope {
                        to: Party::Replica(*r),
                        msg: Wire::Paxos(msg.clone()),
                    }),
            );
        }
    }
    out
}

/// The [`Mutant::HideVotes`] bug: every `Promise` leaves with no vote.
fn hide_votes(batch: &mut [(Audience, Message)]) {
    for (_, message) in batch {
        if let Message::Promise { accepted, .. } = message {
            accepted.clear();
        }
    }
}

/// The [`Mutant::HideFaulty`] bug: every `Promise` leaves with no faulty
/// record.
fn hide_faulty(batch: &mut [(Audience, Message)]) {
    for (_, message) in batch {
        if let Message::Promise { faulty, .. } = message {
            faulty.clear();
        }
    }
}

/// The [`Mutant::HideWatermark`] bug: every `PreReadAck` says that its
/// sender never voted.
fn hide_watermarks(batch: &mut [(Audience, Message)]) {
    for (_, message) in batch {
        if let Message::PreReadAck { watermark, .. } = message {
            *watermark = None;
        }
    }
}

/// The [`Mutant::CompactPastFold`] bug: every compaction drops the whole
/// chosen prefix, the slots that still hold records above `first_seq`
/// too. `chosen` is the disk's chosen index before the batch.
fn compact_past_fold(chosen: Option<Slot>, writes: &mut [WriteOp]) {
    let top = writes
        .iter()
        .filter_map(|op| match op {
            WriteOp::SetChosenIndex(slot) => Some(*slot),
            _ => None,
        })
        .chain(chosen)
        .max();
    for op in writes {
        if let (WriteOp::Truncate { first, .. }, Some(top)) = (op, top) {
            *first = (*first).max(Slot(top.0 + 1));
        }
    }
}

/// The [`Mutant::BeliefFence`] bug: every `PreReadAck` carries the
/// ballot of the sender's belief, `belief`, as its read fence, and not the
/// higher of its promise and its belief.
fn belief_fences(belief: Ballot, batch: &mut [(Audience, Message)]) {
    for (_, message) in batch {
        if let Message::PreReadAck {
            config_since: Some(fence),
            ..
        } = message
        {
            *fence = belief;
        }
    }
}

/// The [`Mutant::RewindHandoff`] bug: every `Relinquish` hands on an
/// empty tail, its frontier at its first slot.
fn rewind_handoffs(batch: &mut [(Audience, Message)]) {
    for (_, message) in batch {
        if let Message::Relinquish {
            from_slot,
            next_slot,
            decided,
            pending,
            ..
        } = message
        {
            *next_slot = *from_slot;
            decided.clear();
            pending.clear();
        }
    }
}

/// The [`Mutant::EarlyCommit`] bug: a `Commit` for each `Accept` a node
/// sends, before any vote is counted.
fn early_commits(
    out: &[Envelope],
    from: NodeId,
    pool: &[NodeId],
    replicas: &[NodeId],
) -> Vec<Envelope> {
    let mut seen = BTreeSet::new();
    let mut forged = Vec::new();
    for envelope in out {
        if let Wire::Paxos(Message::Accept {
            ballot,
            slot,
            command,
            ..
        }) = &envelope.msg
            && seen.insert((*slot, command_fingerprint(command)))
        {
            let commit = Message::Commit {
                from: paros_consensus::contract::message::Party::Node(from),
                ballot: *ballot,
                slot: *slot,
                command: command.clone(),
            };
            forged.extend(route(
                &[(Audience::Learners, commit)],
                pool,
                Some(from),
                replicas,
            ));
        }
    }
    forged
}
