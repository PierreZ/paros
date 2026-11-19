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

mod flavor;
mod sites;

use std::collections::BTreeSet;
use std::fmt::Write as _;

use moonpool_walk::{Cx, Model, Phase, sometimes};
use paros_consensus::contract::message::Audience;
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::journal::{Command, Control, Entry, LeaderUuid, Seq, command_fingerprint};
use paros_consensus::matchmaker::{GcAck, GcRequest, MatchReply, MatchRequest};
use paros_consensus::membership::{AcceptorConfig, MatchmakerId, ProxyId, QuorumSystem};
use paros_consensus::node::{Delegation, ProposeResult, ReconfigureResult};
use paros_consensus::{
    ColocatedNode, Config, Limits, Message, NodeId, ProxyLeader, ReplicaNode, Slot, Value, WriteOp,
};

use crate::cell::sites::{MatchmakerSite, NodeSite, ProxySite, ReplicaSite};
use crate::oracle::ledger::Ledger;
use crate::oracle::quorum::Phase2Rule;
use crate::oracle::reads::Reads;

pub use flavor::{Flavor, Mutant};

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
    /// A leader started a reconfiguration.
    pub const RECONFIGURED: &str = "cell: a leader started a reconfiguration";
    /// A slot was chosen under a configuration a reconfiguration named.
    pub const CHOSEN_RECONFIGURED: &str = "cell: a slot was chosen after a reconfiguration";
    /// A matchmaker applied a GC request.
    pub const GC_ACKED: &str = "cell: a matchmaker raised its GC watermark";
    /// A round ran on a grid column the driver named.
    pub const NAMED_COLUMN: &str = "cell: a round ran on a column the driver named";
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
}

impl Model for Cell {
    type Msg = Envelope;
    type Node = Party;
    type Action = Action;
    type Event = ();

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
            Action::Claim | Action::Truncate | Action::Reconfigure => [2, 1, 0],
            Action::StepDown => [2, 0, 0],
        };
        match phase {
            Phase::Chaos => chaos,
            Phase::Quiet => quiet,
            Phase::Settle => settle,
        }
    }

    fn act(&mut self, action: Action, cx: &mut Cx<'_, Self>) {
        match action {
            Action::Tick => self.tick_all(cx),
            Action::Propose => self.propose(cx),
            Action::Claim => self.claim(cx),
            Action::Truncate if self.flavor.allows(Action::Truncate) => self.truncate(cx),
            Action::Read if self.flavor.allows(Action::Read) => self.read(cx),
            Action::StepDown => self.step_down(cx),
            Action::Handoff if self.flavor.allows(Action::Handoff) => self.handoff(cx),
            Action::Reconfigure if self.flavor.allows(Action::Reconfigure) => self.reconfigure(cx),
            Action::Truncate | Action::Read | Action::Handoff | Action::Reconfigure => {}
        }
    }

    fn deliver(&mut self, envelope: Envelope, cx: &mut Cx<'_, Self>) {
        match (envelope.to, envelope.msg) {
            (Party::Node(id), wire) => {
                let Some(node) = self.node_mut(id).live.as_mut() else {
                    return;
                };
                match wire {
                    Wire::Paxos(msg) => node.step(msg),
                    Wire::MatchReply(reply) => {
                        let _ = node.on_match_reply(reply);
                    }
                    Wire::GcAck(ack) => {
                        let _ = node.on_gc_ack(&ack);
                    }
                    Wire::Match(_) | Wire::Gc(_) => {
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

    fn crash(&mut self, party: &Party, _cx: &mut Cx<'_, Self>) {
        match *party {
            Party::Node(id) => self.crash_node(id),
            Party::Proxy(id) => {
                let site = self.proxy_mut(id);
                sometimes!(
                    site.live.as_ref().is_some_and(|p| !p.rounds().is_empty()),
                    reach::PROXY_CRASH
                );
                site.live = None;
            }
            Party::Replica(id) => self.replica_mut(id).live = None,
            Party::Matchmaker(id) => self.matchmaker_mut(id).live = None,
        }
    }

    fn restart(&mut self, party: &Party, cx: &mut Cx<'_, Self>) {
        match *party {
            Party::Node(id) => {
                if self.node(id).live.is_none() {
                    self.boot_node(id, cx);
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
                    let site = self.replica_mut(id);
                    site.live = Some(ReplicaNode::new(&site.disk, Limits::DEFAULT));
                    self.drain_replica(id, cx);
                }
            }
            Party::Matchmaker(id) => self.matchmaker_mut(id).boot(),
        }
    }

    fn calm(&mut self, cx: &mut Cx<'_, Self>) {
        // Some proxies stay dead for the tail: a leader's take-back is the
        // only way their rounds decide.
        for site in &mut self.proxies {
            if cx.rng().chance(1, 3) {
                site.dead_for_good = true;
                site.live = None;
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
        for site in &self.nodes {
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
            pool,
            flavor,
        };
        cell.nodes = cell
            .pool
            .iter()
            .map(|id| NodeSite::new(*id, cell.party_config(*id)))
            .collect();
        cell.replicas = (0..cell.flavor.replicas)
            .map(|i| {
                let id = NodeId(REPLICA_BASE + i);
                ReplicaSite::new(id, cell.party_config(id))
            })
            .collect();
        let bootstrap = cell.matchmaker_ids();
        cell.matchmakers = bootstrap
            .iter()
            .map(|id| MatchmakerSite::new(*id, bootstrap.clone()))
            .collect();
        cell.proxies = (0..cell.flavor.proxies)
            .map(|i| ProxySite::new(ProxyId(i), cell.config.clone()))
            .collect();
        for id in cell.pool.clone() {
            cell.boot_node(id, cx);
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

    fn matchmaker_ids(&self) -> Vec<MatchmakerId> {
        (0..self.flavor.matchmakers).map(MatchmakerId).collect()
    }

    fn party_config(&self, id: NodeId) -> Config {
        let matchmakers = self.matchmaker_ids();
        Config {
            peers: self.config.members().to_vec(),
            quorum_system: self.flavor.quorum,
            nodes: if matchmakers.is_empty() {
                Vec::new()
            } else {
                self.pool.clone()
            },
            matchmaker_pool: matchmakers.clone(),
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
        // What the crash left on the disk is what the restart reads.
        let index = self.node(id).disk.hard_state().chosen_index;
        self.ledger.disk_chosen(&format!("node {}", id.0), index);
    }

    fn boot_node(&mut self, id: NodeId, cx: &mut Cx<'_, Self>) {
        let site = self.node_mut(id);
        sometimes!(
            site.disk.hard_state().max_promised_ballot != paros_consensus::Ballot::zero(),
            reach::REBOOT
        );
        let mut node = ColocatedNode::new(&site.disk, Limits::DEFAULT);
        node.set_election_timeout(TIMEOUT_MIN + cx.rng().below(TIMEOUT_SPREAD));
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
            let pool = self.pool.clone();
            let mutant = self.flavor.mutant;
            let site = self.node_mut(id);
            let Some(node) = site.live.as_mut() else {
                return;
            };
            let ready = node.ready();
            let writes = ready.writes().to_vec();
            let mut out = route(ready.messages(), &pool, Some(id), &replicas);
            let committed = ready.committed().to_vec();
            let read_states = ready.read_states().to_vec();
            let more = ready.recovery_batch().is_some();
            out.extend(ready.match_requests().iter().map(|(to, request)| Envelope {
                to: Party::Matchmaker(*to),
                msg: Wire::Match(request.clone()),
            }));
            out.extend(ready.gc_requests().iter().map(|(to, request)| Envelope {
                to: Party::Matchmaker(*to),
                msg: Wire::Gc(*request),
            }));
            ready.advance();
            if writes.is_empty()
                && out.is_empty()
                && committed.is_empty()
                && read_states.is_empty()
                && !more
            {
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
            if mutant != Some(Mutant::SkipPersist) {
                self.persist_node(id, &writes);
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
            for envelope in out {
                cx.send(envelope);
            }
            for (slot, _, command, outcome) in &committed {
                self.applied(&format!("node {}", id.0), *slot, command, outcome);
            }
            for state in &read_states {
                sometimes!(true, reach::NODE_READ);
                self.reads
                    .answered(&format!("node {}", id.0), state.ctx, state.index);
            }
            let site = self.node_mut(id);
            let Some(node) = site.live.as_mut() else {
                return;
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
        let out = route(ready.messages(), &pool, None, &replicas);
        ready.advance();
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
        let journal = self
            .node(leader)
            .live
            .as_ref()
            .expect("a leader is up")
            .replica()
            .journal();
        // A client writes under the uuid it believes leads, at the next
        // position it believes free; a wrong belief is refused at apply.
        self.next_seq = self.next_seq.max(journal.next_seq.0);
        let seq = self.next_seq;
        self.next_seq += 1;
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
        let mut members = self.pool.clone();
        while members.len() > size {
            let out = Self::index(cx.rng().below(members.len() as u64));
            members.remove(out);
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
        self.drain_node(leader, cx);
    }

    /// A matchmaker answers a registration: step, persist, reply.
    fn match_request(&mut self, id: MatchmakerId, request: MatchRequest, cx: &mut Cx<'_, Self>) {
        let site = self.matchmaker_mut(id);
        let Some(matchmaker) = site.live.as_mut() else {
            return;
        };
        matchmaker.step(request);
        site.persist();
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
        let bound: Vec<(paros_consensus::Ballot, Phase2Rule)> = matchmaker
            .journals()
            .flat_map(|journal| matchmaker.registry(journal).iter())
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
