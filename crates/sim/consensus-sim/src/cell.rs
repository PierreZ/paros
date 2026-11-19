//! The cell: `ColocatedNode`s, proxy leaders and replicas of one journal,
//! under a [`moonpool_walk`] random walk.
//!
//! The walk owns the network: every message in flight is an [`Envelope`],
//! delivered in any order, dropped, or duplicated. The cell owns the
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

mod faults;
mod flavor;
mod handover;
mod retire;
mod sites;
pub mod trace;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use moonpool_walk::{Cx, Model, Phase, sometimes};
use paros_consensus::contract::message::Audience;
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::journal::{Command, Control, Entry, LeaderUuid, Seq, command_fingerprint};
use paros_consensus::matchmaker::{
    GcAck, GcRequest, MatchReply, MatchRequest, ReconfigureReply, ReconfigureRequest,
};
use paros_consensus::membership::{AcceptorConfig, MatchmakerId, ProxyId, QuorumSystem};
use paros_consensus::node::{BeliefSource, Delegation, ProposeResult, ReconfigureResult};
use paros_consensus::{
    Ballot, ColocatedNode, Config, MatchmakerReconfigurer, Message, NodeId, ProxyLeader,
    ReplicaNode, Slot, Value, WriteOp,
};

use crate::cell::sites::{MatchmakerSite, NodeSite, ProxySite, ReplicaSite};
use crate::oracle::ledger::Ledger;
use crate::oracle::quorum::Phase2Rule;
use crate::oracle::reads::Reads;

pub use flavor::{Flavor, Mutant};
pub use trace::{CellEvent, Cmd, Rule, Spec};

/// The coverage sites of the cell. A walk names the ones its flavor must
/// reach with [`moonpool_walk::Walk::expect_sometimes`].
pub mod reach {
    /// A durable quorum chose a slot.
    pub const CHOSEN: &str = "cell: a durable quorum chose a slot";
    /// A second leadership was elected after the first.
    pub const NEW_LEADER: &str = "cell: a node won an election above an earlier leader";
    /// A node crashed with a batch it had not persisted.
    pub const CRASH_BEFORE_PERSIST: &str = "cell: a node crashed before its batch was durable";
    /// A node crashed after its batch was durable, before it sent it.
    pub const CRASH_BEFORE_SEND: &str = "cell: a node crashed between persist and send";
    /// A node restarted from a disk that held a promise.
    pub const REBOOT: &str = "cell: a node restarted from a disk with a promise";
    /// A party applied a write that the journal accepted.
    pub const WRITE_APPLIED: &str = "cell: a party applied an accepted write";
    /// A journal changed its leader uuid.
    pub const SET_LEADER: &str = "cell: a SetLeader was applied";
    /// A node compacted its log below a decided `Truncate`.
    pub const COMPACTED: &str = "cell: a node's disk dropped a truncated prefix";
    /// A node jumped to a peer's trim point.
    pub const TRIM_JUMP: &str = "cell: a node jumped to a trim point";
    /// A leader handed its authority to a peer with rounds still open.
    pub const HANDOFF: &str = "cell: a leader handed off with rounds in flight";
    /// A proxy leader committed a slot.
    pub const PROXY_COMMIT: &str = "cell: a proxy leader committed a slot";
    /// A leader took back a round it delegated to a proxy.
    pub const TAKE_BACK: &str = "cell: a leader took back a delegated round";
    /// A proxy crashed with rounds open.
    pub const PROXY_CRASH: &str = "cell: a proxy crashed with rounds open";
    /// A replica applied a slot.
    pub const REPLICA_APPLIED: &str = "cell: a replica applied a slot";
    /// A leaderless read was answered at an acceptor.
    pub const NODE_READ: &str = "cell: a node answered a leaderless read";
    /// A leaderless read was answered at a replica.
    pub const REPLICA_READ: &str = "cell: a replica answered a leaderless read";
    /// The core's probe: a reader whose promise rose above its read basis
    /// opened no read (#1228 (stale read by a lagging reader)).
    pub const LAGGING_READER_REFUSED: &str = "quorum read: a promise above the basis opens no read";
    /// A leader started a reconfiguration.
    pub const RECONFIGURED: &str = "cell: a leader started a reconfiguration";
    /// A slot was chosen under a configuration a reconfiguration named.
    pub const CHOSEN_RECONFIGURED: &str = "cell: a slot was chosen after a reconfiguration";
    /// A matchmaker applied a GC request.
    pub const GC_ACKED: &str = "cell: a matchmaker raised its GC watermark";
    /// A round ran on a grid column the driver named.
    pub const NAMED_COLUMN: &str = "cell: a round ran on a column the driver named";
    /// A leader moved a stuck round to another column.
    pub const MOVED_ROUND: &str = "cell: a leader moved a stuck round to another column";
    /// A client sent a write again at a position it wrote before.
    pub const RETRY: &str = "cell: a client retried a write";
    /// A disk lost the value of a chosen record.
    pub const ROT_CHOSEN: &str = "cell: a disk rotted a chosen record";
    /// A disk lost the value of a record that a peer already trimmed and
    /// that a third node lacks.
    pub const ROT_SPLIT_FLOOR: &str = "cell: a disk rotted a record below a peer's floor";
    /// A scrub found a rotted record and wrote it again.
    pub const SCRUBBED: &str = "cell: a scrub wrote a rotted record again";
    /// The operator retired a node.
    pub const RETIRED: &str = "cell: the operator retired a node";
    /// The operator booked a retired node again.
    pub const REBOOKED: &str = "cell: the operator booked a retired node again";
    /// A node admitted a late node to its pool.
    pub const ADMITTED: &str = "cell: a node admitted a late node to its pool";
    /// A handover chose a new matchmaker set.
    pub const HANDED_OVER: &str = "cell: a handover chose a new matchmaker set";
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

/// A party of the cell: what the walk crashes and restarts, and what a
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
    /// A handover's request, to a matchmaker.
    Reconfigure(ReconfigureRequest),
    /// A matchmaker's answer to it, to the node whose reconfigurer asked.
    ReconfigureReply(ReconfigureReply),
}

/// The cell's own moves.
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
];

/// The cell under walk. See the module documentation.
pub struct Cell {
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
    /// The next client sequence, from the client's view of the journal.
    next_seq: u64,
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
    /// The nodes the operator retired: down until it books them again.
    retired: BTreeSet<NodeId>,
    /// The reconfigurations the operator asked for: each ballot and the
    /// members it names.
    asked: Vec<(Ballot, Vec<NodeId>)>,
    /// The late nodes each node admitted to its pool. The deployment's
    /// registry is durable: a node that boots admits them again.
    admitted: BTreeMap<NodeId, BTreeSet<NodeId>>,
}

impl Model for Cell {
    type Msg = Envelope;
    type Node = Party;
    type Action = Action;
    type Event = CellEvent;

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
            Action::StepDown | Action::Rot => [2, 0, 0],
            // A late node stays outside most pools for a while: a pool
            // catches up slowly, and every pool admits it once the faults
            // stop (`Cell::calm`).
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
            Action::Propose => self.propose(cx),
            Action::Claim => self.claim(cx),
            Action::StepDown => self.step_down(cx),
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
                if self.node(id).live.is_none() {
                    return;
                }
                if let Wire::MatchReply(reply) = &wire {
                    self.discover(id, reply, cx);
                }
                let node = self.node_mut(id).live.as_mut().expect("a live node");
                match wire {
                    Wire::Paxos(msg) => node.step(msg),
                    Wire::MatchReply(reply) => {
                        let _ = node.on_match_reply(reply);
                    }
                    Wire::GcAck(ack) => {
                        let _ = node.on_gc_ack(&ack);
                    }
                    Wire::Match(_)
                    | Wire::Gc(_)
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
            (Party::Replica(id), Wire::Paxos(msg)) => {
                let Some(replica) = self.replica_mut(id).live.as_mut() else {
                    return;
                };
                replica.step(msg);
                self.drain_replica(id, cx);
            }
            (Party::Matchmaker(id), Wire::Match(request)) => self.match_request(id, request, cx),
            (Party::Matchmaker(id), Wire::Gc(request)) => self.gc_request(id, &request, cx),
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
            Party::Node(id) => self.crash_node(id),
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
                // A retired node stays down until the operator books it.
                if self.node(id).live.is_none() && !self.retired.contains(&id) {
                    self.boot_node(id, true, cx);
                }
            }
            Party::Proxy(id) => {
                let config = self.config.clone();
                let site = self.proxy_mut(id);
                if site.live.is_none() && !site.dead_for_good {
                    site.live = Some(ProxyLeader::new(id, config));
                }
            }
            Party::Replica(id) => {
                if self.replica(id).live.is_none() {
                    let limits = self.flavor.limits;
                    let site = self.replica_mut(id);
                    site.live = Some(ReplicaNode::new(&site.disk, limits));
                    self.drain_replica(id, cx);
                }
            }
            Party::Matchmaker(id) => self.matchmaker_mut(id).boot(),
        }
    }

    fn calm(&mut self, cx: &mut Cx<'_, Self>) {
        // The deployment's registry reaches every node: each pool admits
        // the late nodes, as #189 needs for liveness. Until then a late
        // node hears no beat from a node that does not know it. A
        // lagging-reader walk keeps the pools lagging through the quiet
        // phase, where leaders stand and reconfigure, and admits them when
        // the settle rounds start (`Cell::tick_all`).
        if !self.flavor.lagging_reader {
            self.admit_all();
        }
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
            for (slot, command) in node.replica().chosen() {
                self.ledger
                    .learned(&format!("node {}", site.id.0), *slot, command);
            }
        }
        for site in &self.replicas {
            let Some(replica) = &site.live else { continue };
            for (slot, command) in replica.replica().chosen() {
                self.ledger
                    .learned(&format!("replica {}", site.id.0), *slot, command);
            }
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
        for site in self.nodes.iter().filter(|s| !self.retired.contains(&s.id)) {
            let reached = site.live.as_ref().map(|n| n.replica().first_unchosen());
            if reached.is_none_or(|r| r < frontier) {
                return Err(format!(
                    "node {} reached {reached:?}, the frontier is {}",
                    site.id.0, frontier.0
                ));
            }
        }
        for site in &self.replicas {
            let reached = site.live.as_ref().map(|r| r.replica().first_unchosen());
            if reached.is_none_or(|r| r < frontier) {
                return Err(format!(
                    "replica {} reached {reached:?}, the frontier is {}",
                    site.id.0, frontier.0
                ));
            }
        }
        Ok(())
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

impl Cell {
    /// The cell of `flavor`, every party up.
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
        let mut cell = Self {
            config,
            nodes: Vec::new(),
            proxies: Vec::new(),
            replicas: Vec::new(),
            matchmakers: Vec::new(),
            ledger: Ledger::new(rule),
            reads: Reads::default(),
            next_ctx: 1,
            next_seq: 0,
            next_uuid: 0,
            take_back_after,
            expire_after,
            leader_ballot: None,
            early_gc: BTreeSet::new(),
            tracer: trace::Tracer::default(),
            reconfigurers: Vec::new(),
            retired: BTreeSet::new(),
            asked: Vec::new(),
            admitted: BTreeMap::new(),
            pool,
            flavor,
        };
        cell.reconfigurers = cell.pool.iter().map(|id| cell.reconfigurer(*id)).collect();
        cell.nodes = cell
            .pool
            .iter()
            .map(|id| NodeSite::new(*id, cell.party_config(*id)))
            .collect();
        cell.replicas = (0..cell.flavor.replicas)
            .map(|i| {
                let id = NodeId(REPLICA_BASE + i);
                ReplicaSite::new(id, cell.party_config(id), cell.flavor.limits)
            })
            .collect();
        let bootstrap = cell.bootstrap_matchmakers();
        cell.matchmakers = cell
            .matchmaker_ids()
            .iter()
            .map(|id| MatchmakerSite::new(*id, bootstrap.clone(), cell.flavor.limits))
            .collect();
        if cell.flavor.matchmaker_spares > 0 {
            cell.register_sibling();
        }
        cell.proxies = (0..cell.flavor.proxies)
            .map(|i| ProxySite::new(ProxyId(i), cell.config.clone()))
            .collect();
        for id in cell.pool.clone() {
            cell.boot_node(id, false, cx);
        }
        for id in cell.replica_ids() {
            cell.drain_replica(id, cx);
        }
        cell
    }

    /// The flavor of this cell.
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
        if !self.flavor.lagging_reader || cx.phase() == Phase::Settle {
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
        if !self.flavor.lagging_reader || cx.phase() == Phase::Settle {
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
    /// loses on the way ([`Cell::lost_to_a_lagging_pool`]).
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

    /// A sibling journal of the same matchmaker set (#190): one belief
    /// registered at every bootstrap matchmaker, so a handover's freeze and
    /// bootstrap carry two journals, and page them when a page holds one.
    fn register_sibling(&mut self) {
        let journal = paros_consensus::journal::JournalId(
            paros_consensus::journal::JournalIdentifier::UNSET.journal.0 + 1,
        );
        let request = MatchRequest::new(
            journal,
            NodeId(0),
            Ballot {
                round: 1,
                node: NodeId(0),
            },
            self.config.clone(),
            paros_consensus::membership::MatchmakerGeneration(0),
        );
        for id in self.bootstrap_matchmakers() {
            let site = self.matchmaker_mut(id);
            site.live
                .as_mut()
                .expect("a matchmaker boots up")
                .step(request.clone());
            site.persist();
            let _ = site.take_replies();
        }
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
            ..Config::new(id, paros_consensus::journal::JournalIdentifier::UNSET)
        }
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

    fn crash_node(&mut self, id: NodeId) {
        let mutant = self.flavor.mutant;
        self.node_mut(id).crash(mutant);
        // The reconfigurer holds no durable state: the crash loses it.
        self.reconfigurers[Self::index(id.0)] = self.reconfigurer(id);
        // What the crash left on the disk is what the restart reads.
        let index = self.node(id).disk.hard_state().chosen_index;
        self.ledger.disk_chosen(&format!("node {}", id.0), index);
    }

    /// Boot node `id` from its disk; `again` when it ran before.
    fn boot_node(&mut self, id: NodeId, again: bool, cx: &mut Cx<'_, Self>) {
        if again {
            cx.record(self.tracer.restart(id));
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
        site.live = Some(node);
        let name = format!("node {}", id.0);
        self.ledger.disk_chosen(&name, disk);
        self.drain_node(id, cx);
    }

    // ---- draining ---------------------------------------------------------

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
            let ready = node.ready();
            let writes = ready.writes().to_vec();
            let mut batch: Vec<(Audience, Message)> = ready.messages().to_vec();
            plant(mutant, &mut batch);
            let mut out = route(&batch, &pool, Some(id), &replicas);
            let committed = ready.committed().to_vec();
            let read_states = ready.read_states().to_vec();
            let more = ready.recovery_batch().is_some();
            let matchmaking: Vec<MatchRequest> = ready
                .match_requests()
                .iter()
                .map(|(_, request)| request.clone())
                .collect();
            let mut gc: Vec<(MatchmakerId, GcRequest)> = ready.gc_requests().to_vec();
            out.extend(to_matchmakers(ready.match_requests(), &gc));
            ready.advance();
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
                // An empty batch can still follow a change of leadership
                // (a win with nothing to recover): the tracer records it.
                let before = self.node(id).disk.hard_state().chosen_index;
                let traced = Traced::empty(seen);
                self.trace_batch(id, traced, before, cx);
                return;
            }
            if mutant == Some(Mutant::EarlyCommit) {
                out.extend(early_commits(&out, id, &pool, &replicas));
            }
            if crash_before && !writes.is_empty() {
                sometimes!(true, reach::CRASH_BEFORE_PERSIST);
                self.crash_node(id);
                cx.crashed(&Party::Node(id));
                return;
            }
            let before = self.node(id).disk.hard_state().chosen_index;
            if mutant != Some(Mutant::SkipPersist) {
                self.persist_node(id, &writes);
            }
            let traced = Traced {
                seen,
                writes: &writes,
                messages: &batch,
                matchmaking: &matchmaking,
                gc: &gc,
            };
            self.trace_batch(id, traced, before, cx);
            if !self.rot_at_compaction(id, &writes, cx) {
                // A fault stopped it between the persist and the send.
                return;
            }
            if crash_between {
                sometimes!(true, reach::CRASH_BEFORE_SEND);
                self.crash_node(id);
                cx.crashed(&Party::Node(id));
                return;
            }
            for envelope in &out {
                self.judge_sent(&format!("node {}", id.0), envelope);
            }
            self.send_from(id, out, cx);
            for (slot, _, command, outcome) in &committed {
                self.applied(&format!("node {}", id.0), *slot, command, outcome);
            }
            for state in &read_states {
                sometimes!(true, reach::NODE_READ);
                self.reads
                    .answered(&format!("node {}", id.0), state.ctx, state.index);
            }
            if !self.after_batch(id) {
                return;
            }
        }
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
            journal: seen.journal,
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

    /// After a batch of node `id`: advance its recovery and note the
    /// ballot of a leader. Returns whether the node still runs.
    fn after_batch(&mut self, id: NodeId) -> bool {
        let Some(node) = self.node_mut(id).live.as_mut() else {
            return false;
        };
        node.advance_recovery();
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
        let chosen = site.live.as_ref().map(|node| node.replica().chosen());
        let first = before.map_or(0, |s| s.0 + 1);
        let last = after.map_or(0, |s| s.0 + 1);
        let learned = (first..last)
            .filter_map(|s| {
                let command = chosen?.get(&Slot(s))?;
                Some((Slot(s), Cmd::of(command)))
            })
            .collect();
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
                WriteOp::TrimmedTo { .. } => sometimes!(true, reach::TRIM_JUMP),
                _ => {}
            }
        }
        let hard = self.node(id).disk.hard_state();
        self.ledger.promised(id, hard.max_promised_ballot);
        self.ledger.disk_chosen(&name, hard.chosen_index);
    }

    /// The oracles on a message that leaves `who`.
    fn judge_sent(&self, who: &str, envelope: &Envelope) {
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
            _ => {}
        }
    }

    fn applied(
        &mut self,
        who: &str,
        slot: Slot,
        command: &Command,
        outcome: &paros_consensus::journal::Outcome,
    ) {
        self.ledger.applied(who, slot, command, outcome);
        self.reads.applied(slot);
        sometimes!(
            matches!(command, Command::Write(_))
                && matches!(outcome, paros_consensus::journal::Outcome::Accepted { .. }),
            reach::WRITE_APPLIED
        );
        sometimes!(
            matches!(outcome, paros_consensus::journal::Outcome::Leader(_)),
            reach::SET_LEADER
        );
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
            let writes = ready.writes().to_vec();
            let out = route(ready.messages(), &pool, Some(id), &[]);
            let committed = ready.committed().to_vec();
            let read_states = ready.read_states().to_vec();
            ready.advance();
            if writes.is_empty() && out.is_empty() && committed.is_empty() && read_states.is_empty()
            {
                return;
            }
            for op in &writes {
                assert!(
                    !matches!(op, WriteOp::Acceptor(_)),
                    "a replica never writes a vote"
                );
                site.disk.apply(std::slice::from_ref(op));
            }
            let index = site.disk.hard_state().chosen_index;
            self.ledger.disk_chosen(&who, index);
            for envelope in out {
                cx.send(envelope);
            }
            for (slot, _, command, outcome) in &committed {
                sometimes!(true, reach::REPLICA_APPLIED);
                self.applied(&who, *slot, command, outcome);
            }
            for state in &read_states {
                sometimes!(true, reach::REPLICA_READ);
                self.reads.answered(&who, state.ctx, state.index);
            }
            if let Some(replica) = self.replica_mut(id).live.as_mut() {
                replica.advance_recovery();
            }
        }
    }

    // ---- the clock and the clients ------------------------------------

    fn tick_all(&mut self, cx: &mut Cx<'_, Self>) {
        if self.flavor.lagging_reader && cx.phase() == Phase::Settle {
            self.admit_all();
        }
        let expire_after = self.expire_after;
        for i in 0..self.proxies.len() {
            let id = ProxyId(i as u64);
            if let Some(proxy) = self.proxy_mut(id).live.as_mut() {
                proxy.expire_stale(expire_after);
                proxy.resend_pending();
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
    }

    /// A leader the client reaches, and the journal as that leader folds it.
    fn pick_leader(&self, cx: &mut Cx<'_, Self>) -> Option<NodeId> {
        let leaders = self.leaders();
        cx.rng().pick(&leaders).copied()
    }

    fn propose(&mut self, cx: &mut Cx<'_, Self>) {
        let Some(leader) = self.pick_leader(cx) else {
            return;
        };
        let node = self.node(leader).live.as_ref().expect("a leader is up");
        let journal = node.replica().journal();
        // The writes the leader still has in flight: each may take the
        // next position before this one applies.
        let in_flight = (node.proposer().rounds().len() + node.delegated_rounds().len()) as u64;
        // A client writes under the uuid it believes leads, at the next
        // position it believes free; a wrong belief is refused at apply. A
        // refusal names the next position, and the client learns it: a
        // belief ahead of the view by more than the writes in flight is
        // one the refusals corrected.
        if self.next_seq > journal.next_seq.0 + in_flight {
            self.next_seq = journal.next_seq.0;
        }
        self.next_seq = self.next_seq.max(journal.next_seq.0);
        // A client that heard no answer sends a write again, with the same
        // records at the same position.
        let retry = self.next_seq > 0 && cx.rng().chance(1, 8);
        sometimes!(retry, reach::RETRY);
        let seq = if retry {
            self.next_seq - 1 - cx.rng().below(self.next_seq.min(3))
        } else {
            self.next_seq += 1;
            self.next_seq - 1
        };
        let uuid = journal.leader.unwrap_or(LeaderUuid(1));
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
        let result = node.propose_in(
            Entry {
                leader: uuid,
                seq: Seq(seq),
                records: vec![Value(seq.to_le_bytes().to_vec())],
            },
            column,
            delegation,
        );
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
        if self.flavor.lagging_reader {
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
                node.quorum_read(ctx);
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
                if local {
                    let index = replica.replica().chosen_index();
                    self.reads
                        .answered(&format!("replica {}", id.0), ctx, index);
                    return;
                }
                replica.quorum_read_in(ctx, None);
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
        if self.flavor.lagging_reader
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
            .filter(|_| self.flavor.lagging_reader || cx.rng().chance(1, 2));
        // A lagging-reader walk moves away from the members whose pools lag:
        // a quorum of the new configuration then chooses without them, while
        // they still read on the old one.
        let lagging: Vec<NodeId> = if self.flavor.lagging_reader {
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
        let (journal, ballot) = (request.journal, request.ballot);
        let held = matchmaker.registry(journal).contains_key(&ballot);
        matchmaker.step(request);
        let holds = matchmaker.registry(journal).contains_key(&ballot);
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

    /// Every configuration a matchmaker holds is bound to its ballot in
    /// the ledger, before any vote at that ballot is judged.
    fn bind_registrations(&mut self, id: MatchmakerId) {
        let Some(matchmaker) = self.matchmaker(id).live.as_ref() else {
            return;
        };
        // Only this journal's ballots: a sibling journal's are its own.
        let journal = paros_consensus::journal::JournalIdentifier::UNSET.journal;
        let bound: Vec<(paros_consensus::Ballot, Phase2Rule)> = matchmaker
            .registry(journal)
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
    journal: paros_consensus::journal::JournalId,
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
            node.replica()
                .chosen()
                .range(..frontier)
                .map(|(s, command)| (*s, Cmd::of(command)))
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
            journal: node.config().journal.journal,
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
        }
    }
}

/// The bugs of `mutant` that rewrite the messages of a node's batch.
fn plant(mutant: Option<Mutant>, batch: &mut [(Audience, Message)]) {
    match mutant {
        Some(Mutant::HideVotes) => hide_votes(batch),
        Some(Mutant::RewindHandoff) => rewind_handoffs(batch),
        _ => {}
    }
}

/// The envelopes of a node's requests to the matchmakers.
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
