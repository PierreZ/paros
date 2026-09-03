//! Unit tests for the Multi-Paxos state machine. Tests are a child module of
//! `node`, so they may read `ColocatedNode`'s private fields directly.

use std::collections::BTreeMap;

use super::{
    ColocatedNode, HANDOFF_BATCH, HANDOFF_FENCE_ELECTIONS, LEADER_RECOVERY_BATCH, LeadershipOrigin,
    MatchStep, NodeRole, PROMISE_BATCH, ProposeResult, READ_ROUND_TTL_TICKS, ReadIndexResult,
    ReadState, ReconfigureRefusal, ReconfigureResult,
};
use crate::matchmaker::{
    MatchOutcome, MatchRefusal, MatchReply, MatchRequest, Matchmaker, MatchmakerConfig,
    Registration,
};
use crate::membership::{AcceptorConfig, MatchmakerGeneration, MatchmakerId};
use crate::message::Message;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{
    Ballot, ClientId, ClientSeq, Command, ConfigId, Control, Entry, NodeId, Slot, Value,
    command_fingerprint,
};

/// In-memory [`Storage`] seeded with an explicit initial state (for restart
/// tests): the durable scalars plus a per-slot accepted log.
struct TestStorage {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    config: Config,
    first_slot: Slot,
    /// Recoverable faulty entries the simulated boot scan classified (Stage 8).
    faulty: Vec<(Slot, Ballot)>,
}

impl TestStorage {
    fn new(id: u64, members: &[u64]) -> Self {
        Self {
            hard_state: HardState::default(),
            accepted: BTreeMap::new(),
            config: Config {
                id: NodeId(id),
                peers: members.iter().copied().map(NodeId).collect(),
                quorum_system: crate::membership::QuorumSystem::Majority,
                nodes: Vec::new(),
                matchmakers: Vec::new(),
                matchmaker_pool: Vec::new(),
            },
            first_slot: Slot(0),
            faulty: Vec::new(),
        }
    }

    /// Snapshot a live node's durable state (scalars + accepted log + compaction
    /// floor) into a fresh storage, the way a real driver's persisted disk would
    /// look, for building the "restart from durable storage" path in tests.
    fn from_node(n: &ColocatedNode) -> Self {
        Self {
            hard_state: n.hard_state(),
            accepted: n.acceptor().records().clone(),
            config: n.config().clone(),
            first_slot: n.acceptor().first_slot(),
            faulty: Vec::new(),
        }
    }

    /// Rot the accepted record at `slot`: the value is lost, the identity
    /// `(slot, ballot)` survives — the boot scan's recoverable classification.
    fn rot(&mut self, slot: Slot) {
        let (ballot, _) = self
            .accepted
            .remove(&slot)
            .expect("rot targets a persisted record");
        self.faulty.push((slot, ballot));
    }
}

impl Storage for TestStorage {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state, self.config.clone())
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.accepted.get(&slot).cloned()
    }
    fn first_slot(&self) -> Slot {
        self.first_slot
    }
    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }
    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty.clone()
    }
}

fn node(id: u64, members: &[u64]) -> ColocatedNode {
    ColocatedNode::new(&TestStorage::new(id, members))
}

/// A node of a **matchmaker deployment**: bootstrap membership `members`,
/// addressable pool `pool` (a superset holding the spares), and `matchmakers`
/// matchmakers.
fn deployed_node(id: u64, members: &[u64], pool: &[u64], matchmakers: u64) -> ColocatedNode {
    let mut storage = TestStorage::new(id, members);
    storage.config.nodes = pool.iter().copied().map(NodeId).collect();
    storage.config.matchmakers = (0..matchmakers).map(MatchmakerId).collect();
    ColocatedNode::new(&storage)
}

/// Drain a node's pending matchmaking requests and clear the batch.
fn drain_match_requests(n: &mut ColocatedNode) -> Vec<(MatchmakerId, MatchRequest)> {
    let ready = n.ready();
    let requests = ready.match_requests().to_vec();
    ready.advance();
    requests
}

/// Answer `requests` from the given registries, returning every reply.
fn matchmake(
    matchmakers: &mut [Matchmaker],
    requests: Vec<(MatchmakerId, MatchRequest)>,
) -> Vec<MatchReply> {
    let mut replies = Vec::new();
    for (id, request) in requests {
        let mm = &mut matchmakers[usize::try_from(id.0).expect("matchmaker index")];
        mm.step(request);
        let ready = mm.ready();
        replies.extend(ready.replies().to_vec());
        ready.advance();
    }
    replies
}

/// Fresh in-memory registries, `MatchmakerId(0..n)`.
fn registries(n: u64) -> Vec<Matchmaker> {
    (0..n)
        .map(|i| {
            Matchmaker::new(
                &MatchmakerConfig {
                    id: MatchmakerId(i),
                    bootstrap: (0..n).map(MatchmakerId).collect(),
                },
                &MemRegistry,
            )
        })
        .collect()
}

/// An empty registry port for [`registries`].
#[derive(Default)]
struct MemRegistry;

impl crate::matchmaker::RegistryStorage for MemRegistry {
    fn initial_state(&self) -> crate::matchmaker::MatchmakerHardState {
        crate::matchmaker::MatchmakerHardState::default()
    }
    fn registration(&self, _ballot: Ballot) -> Option<crate::matchmaker::Registration> {
        None
    }
    fn registered_ballots(&self) -> Vec<Ballot> {
        Vec::new()
    }
}

/// `AcceptorConfig` over `members` under the majority system.
fn cfg(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().copied().map(NodeId).collect(),
        crate::membership::QuorumSystem::Majority,
    )
}

fn val(b: u8) -> Value {
    Value(vec![b])
}

fn entry(client: u64, seq: u64, b: u8) -> Entry {
    Entry {
        client: ClientId(client),
        seq: ClientSeq(seq),
        value: val(b),
    }
}

/// A client [`Command`] wrapping [`entry`], the common per-slot value in tests.
fn ucmd(client: u64, seq: u64, b: u8) -> Command {
    Command::User(entry(client, seq, b))
}

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

/// Drain a node's pending messages and clear the batch, resolving each
/// audience against this node's own pool exactly as the driver does.
fn drain(n: &mut ColocatedNode) -> Vec<(NodeId, Message)> {
    let pool: Vec<NodeId> = n.config().pool().to_vec();
    let me = n.config().id;
    let ready = n.ready();
    let msgs: Vec<(NodeId, Message)> = ready
        .messages()
        .iter()
        .flat_map(|(audience, msg)| {
            audience
                .resolve(&pool, me)
                .into_iter()
                .map(move |to| (to, msg.clone()))
        })
        .collect();
    ready.advance();
    msgs
}

/// The chosen client value at `slot` on this node, if any (a control command has
/// no client value and reads back as `None`).
fn chosen_at(n: &ColocatedNode, slot: u64) -> Option<Value> {
    n.replica
        .chosen()
        .get(&Slot(slot))
        .and_then(Command::user)
        .map(|e| e.value.clone())
}

/// Deliver `queue` to addressed recipients, dropping any `(to, msg)` for which
/// `keep` is false, enqueueing each delivery's resulting messages. Runs to
/// quiescence (a reliable network with a caller-controlled partition).
fn deliver_filtered(
    nodes: &mut [ColocatedNode],
    mut queue: Vec<(NodeId, Message)>,
    keep: impl Fn(NodeId, &Message) -> bool,
) {
    while let Some((to, msg)) = queue.pop() {
        if !keep(to, &msg) {
            continue;
        }
        let idx = nodes
            .iter()
            .position(|n| n.config().id == to)
            .expect("message addressed to a cluster member");
        nodes[idx].step(msg);
        queue.extend(drain(&mut nodes[idx]));
    }
}

fn deliver_all(nodes: &mut [ColocatedNode], queue: Vec<(NodeId, Message)>) {
    deliver_filtered(nodes, queue, |_, _| true);
}

/// A `CheckQuorum` window far past any unit test's tick horizon. Unit tests
/// step messages by hand rather than pumping ack traffic every tick, so a
/// realistic short timeout would demote their leaders mid-test; tests that
/// *target* `CheckQuorum` set a short window explicitly instead.
const NO_CHECK_QUORUM: u64 = 1_000_000;

/// Drive `nodes[idx]` to leadership in a healthy cluster, then beat once so the
/// followers learn who the leader is (a follower only adopts a leader on
/// `Accept`/`Heartbeat`, never on Phase 1). Leaves the leader with an
/// effectively infinite `CheckQuorum` window (see [`NO_CHECK_QUORUM`]).
fn make_leader(nodes: &mut [ColocatedNode], idx: usize) {
    nodes[idx].set_election_timeout(1);
    nodes[idx].tick(); // fires CheckLeader -> Candidate, broadcasts Prepare
    let q = drain(&mut nodes[idx]);
    deliver_all(nodes, q);
    assert!(
        nodes[idx].is_leader(),
        "node {idx} should have won the election"
    );
    nodes[idx].set_election_timeout(NO_CHECK_QUORUM);
    nodes[idx].tick(); // fires Heartbeat -> followers adopt the leader
    let q = drain(&mut nodes[idx]);
    deliver_all(nodes, q);
}

/// Drive a fresh 3-node cluster with node 0 as leader and get slots 0..=2 chosen
/// everywhere, then return the cluster (`chosen_index` is `Some(Slot(2))`).
fn cluster_with_three_chosen() -> [ColocatedNode; 3] {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    for (seq, b) in [(1u64, 10u8), (2, 20), (3, 30)] {
        let _ = nodes[0].propose(ClientId(1), ClientSeq(seq), val(b));
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }
    nodes
}

// ---- linearizable reads (read-index) ---------------------------------------

/// Step `msg` into whichever node it is addressed to, without draining it (so
/// its pending buckets stay observable), returning nothing. Panics if `to` is
/// not a cluster member.
fn step_at(nodes: &mut [ColocatedNode], to: NodeId, msg: Message) {
    let idx = nodes
        .iter()
        .position(|n| n.config().id == to)
        .expect("message addressed to a cluster member");
    nodes[idx].step(msg);
}

// ---- #67/#88: the stale-ballot election --------------------------------------
//
// Two **guard** tests (plus the acceptor-side containment below). #67 asked
// whether a Candidate that learns a higher ballot mid-campaign can go on to win
// at its now-stale ballot; the answer used to be yes, through the two
// promise-raising paths that deliberately leave the campaign open: `mark_chosen`
// (a learned `Commit`/`CatchUpResponse`) and `on_install_snapshot` (a snapshot
// whose serving peer minted its promise with no quorum behind it — the #88
// route, uncontained by quorum intersection at n >= 5). `try_become_leader` now
// refuses any win whose election ballot sits below the node's own
// `max_promised_ballot`, restoring "a leader's ballot >= its own promise".
// These tests pin the refusal, the self-heal (the next campaign ratchets past
// the learned promise), and the healthy re-propose the stale win used to break.

mod acceptor;
mod bounds;
mod catch_up_snapshot;
mod decide_apply;
mod election;
mod handoff;
mod invariants;
mod matchmaking;
mod reads;
mod reconfigure;
mod recovery;
mod replication;
