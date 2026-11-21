//! **A whole journal in one process, with storage written by hand.**
//!
//! Run it: `cargo run -p paros-journal --example in_memory`
//!
//! The journal roles of `paros_journal::roles` are pure state machines: a
//! message or a tick goes in, a list of effects comes out. They do no I/O,
//! read no clock and spawn nothing. The program that runs them, the
//! **driver**, brings the rest. This example is such a driver, written in
//! plain Rust with no runtime crate and no storage crate:
//!
//! - **Storage.** [`KvStore`] is the storage engine that a user brings: here
//!   a map of keys to bytes in memory, with an atomic batch write. The two
//!   adapters [`AcceptorStore`] and [`ReplicaStore`] map the roles' writes
//!   onto it with the codec of the crate (`paros_journal::codec`), and read
//!   it back at a restart. Swap the map for a file, `RocksDB` or an object
//!   store, and nothing else in the example changes.
//! - **Transport.** A queue of messages, delivered in order.
//! - **Clock.** A call to `tick` when the queue is empty.
//! - **The journal's register.** One [`RegisterValue`] that the driver
//!   changes by compare-and-set. A real deployment runs it on the journal's
//!   matchmakers (`paros_journal::register::RegisterCall`); one process
//!   needs no copies.
//!
//! The driver keeps the **flight rule** (`docs/journal/04-roles.md` §J4.2):
//! each persist batch has an id, each send names the batch that it waits
//! on, and the driver sends it only once that batch is durable.
//!
//! # What the run shows
//!
//! 1. The controller opens segment 1: a grid of three acceptors (one column,
//!    `F = 1`) and three replicas. Leader L1 runs Phase 1 and opens.
//! 2. Writer A takes the journal with `SetLeader` and writes two batches.
//! 3. Acceptor 2 crashes. A write is chosen at acceptors 1 and 3, `F + 1`
//!    of the column. Acceptor 2 starts again from what its store holds.
//! 4. The controller opens segment 2 with a new replica set. Leader L2 runs
//!    Phase 1 over both segments, seals segment 1 at one above the highest
//!    vote it saw, and opens segment 2 there. The replicas of segment 1
//!    write its boundary state, and the replicas of segment 2 fold from it.
//! 5. Writer A writes again, in segment 2, at the next position.
//!
//! Each step asserts its result, so CI runs this example as a test.
//!
//! The storage traits of the journal (`paros-journal-io`, #1329) do not
//! exist yet: the two store adapters here are what such a trait asks for,
//! and this example moves onto that trait when it lands.

mod common;

use std::collections::{BTreeMap, VecDeque};

use paros_journal::codec::{decode_boundary, decode_command, encode_boundary, encode_command};
use paros_journal::register::{Ballot, Change, RegisterValue};
use paros_journal::roles::{
    AcceptorDisk, AcceptorEffect, AcceptorOp, Campaign, JournalAcceptor, JournalLeader,
    LeaderEffect, LeaderPhase, Message, PersistId, Proposed, ReplicaDisk, ReplicaEffect, ReplicaOp,
    SegmentReplica,
};
use paros_journal::{
    Answer, Command, Grid, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, Reply, SegmentId,
    SegmentRecord, SegmentState, Seq, Slot,
};

use common::{fenced, show, written};

const A: LeaderUuid = LeaderUuid(0xA);

/// The acceptors of the grid.
const ACCEPTORS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];
/// The node that runs leader L1, then the node that runs leader L2.
const L1: NodeId = NodeId(100);
const L2: NodeId = NodeId(200);
/// The entries of a promise page, and the rounds of a re-send page.
const PAGE: usize = 16;

// ---------------------------------------------------------------------------
// Storage: the part a user writes.
// ---------------------------------------------------------------------------

/// The storage engine: keys to bytes, in memory. Its one write is an atomic
/// batch, as §J4.2 asks of a disk (rule 4: a batch is durable whole or not
/// at all).
#[derive(Default)]
struct KvStore {
    map: BTreeMap<String, Vec<u8>>,
}

impl KvStore {
    fn write_batch(&mut self, batch: Vec<(String, Vec<u8>)>) {
        self.map.extend(batch);
    }

    fn get(&self, key: &str) -> Option<&[u8]> {
        self.map.get(key).map(Vec::as_slice)
    }

    fn scan<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = (&'a str, &'a [u8])> + 'a {
        self.map
            .range(prefix.to_owned()..)
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.as_str(), value.as_slice()))
    }
}

fn ballot_bytes(ballot: Ballot) -> Vec<u8> {
    let mut bytes = ballot.round.to_be_bytes().to_vec();
    bytes.extend(ballot.node.0.to_be_bytes());
    bytes
}

fn ballot_of(bytes: &[u8]) -> Ballot {
    let (round, node) = bytes.split_at(8);
    Ballot {
        round: u64::from_be_bytes(round.try_into().expect("8 bytes")),
        node: NodeId(u64::from_be_bytes(node.try_into().expect("8 bytes"))),
    }
}

fn u64_of(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("8 bytes"))
}

/// A slot key sorts in slot order.
fn slot_key(prefix: &str, slot: Slot) -> String {
    format!("{prefix}{:020}", slot.0)
}

fn slot_of(key: &str, prefix: &str) -> Slot {
    Slot(key[prefix.len()..].parse().expect("a slot key"))
}

/// What the driver asks of a role's store. The journal's IO crate
/// (`paros-journal-io`, #1329) will hold the real trait; this one has the
/// same three calls.
trait Store: Default {
    /// One write of the role's persist batch.
    type Op;
    /// What a restart reads.
    type Disk;
    /// Make batch `id` durable, whole (§J4.2, rule 4).
    fn persist(&mut self, id: PersistId, ops: &[Self::Op]);
    fn durable(&self) -> Option<PersistId>;
    /// What the store holds, as the role's `recover` takes it.
    fn load(&self) -> Self::Disk;
}

/// The adapter of an acceptor: its promise, its votes and the id of its
/// last batch, as keys of the store.
#[derive(Default)]
struct AcceptorStore {
    kv: KvStore,
}

impl Store for AcceptorStore {
    type Op = AcceptorOp;
    type Disk = AcceptorDisk;

    fn persist(&mut self, id: PersistId, ops: &[AcceptorOp]) {
        let mut batch = Vec::new();
        for op in ops {
            match op {
                AcceptorOp::SetPromise(ballot) => {
                    batch.push(("promise".to_owned(), ballot_bytes(*ballot)));
                }
                AcceptorOp::AppendAccepted {
                    slot,
                    ballot,
                    value,
                } => {
                    let mut bytes = ballot_bytes(*ballot);
                    bytes.extend(encode_command(value));
                    batch.push((slot_key("vote/", *slot), bytes));
                }
            }
        }
        batch.push(("durable".to_owned(), id.0.to_be_bytes().to_vec()));
        self.kv.write_batch(batch);
    }

    fn durable(&self) -> Option<PersistId> {
        self.kv.get("durable").map(|b| PersistId(u64_of(b)))
    }

    fn load(&self) -> AcceptorDisk {
        let mut disk = AcceptorDisk {
            promised: self.kv.get("promise").map_or(Ballot::zero(), ballot_of),
            durable: self.durable(),
            ..AcceptorDisk::default()
        };
        for (key, bytes) in self.kv.scan("vote/") {
            let (ballot, command) = bytes.split_at(16);
            let command = decode_command(command).expect("the store keeps what it wrote");
            disk.votes
                .insert(slot_of(key, "vote/"), (ballot_of(ballot), command));
        }
        disk
    }
}

/// The adapter of a segment replica: the base of its fold and the chosen
/// slots it holds.
#[derive(Default)]
struct ReplicaStore {
    kv: KvStore,
}

impl Store for ReplicaStore {
    type Op = ReplicaOp;
    type Disk = ReplicaDisk;

    fn persist(&mut self, id: PersistId, ops: &[ReplicaOp]) {
        let mut batch = Vec::new();
        for op in ops {
            match op {
                ReplicaOp::Base { boundary, start } => {
                    batch.push(("base".to_owned(), encode_boundary(boundary)));
                    batch.push(("start".to_owned(), start.0.to_be_bytes().to_vec()));
                }
                ReplicaOp::Chosen { slot, command } => {
                    batch.push((slot_key("chosen/", *slot), encode_command(command)));
                }
            }
        }
        batch.push(("durable".to_owned(), id.0.to_be_bytes().to_vec()));
        self.kv.write_batch(batch);
    }

    fn durable(&self) -> Option<PersistId> {
        self.kv.get("durable").map(|b| PersistId(u64_of(b)))
    }

    fn load(&self) -> ReplicaDisk {
        let base = self.kv.get("base").map(|bytes| {
            let boundary = decode_boundary(bytes).expect("the store keeps what it wrote");
            let start = Slot(u64_of(
                self.kv.get("start").expect("a start beside the base"),
            ));
            (boundary, start)
        });
        ReplicaDisk {
            base,
            chosen: self
                .kv
                .scan("chosen/")
                .map(|(key, bytes)| {
                    let command = decode_command(bytes).expect("the store keeps what it wrote");
                    (slot_of(key, "chosen/"), command)
                })
                .collect(),
            durable: self.durable(),
        }
    }
}

// ---------------------------------------------------------------------------
// The driver: transport, flight rule, register, clock.
// ---------------------------------------------------------------------------

/// A role that has a disk: the role (`None` while its node is down), its
/// store, the batches not yet durable and the sends that wait on them.
struct Node<R, S: Store> {
    role: Option<R>,
    store: S,
    batches: Vec<(PersistId, Vec<S::Op>)>,
    waiting: Vec<(PersistId, NodeId, Message)>,
}

impl<R, S: Store> Node<R, S> {
    fn new(role: R) -> Self {
        Self {
            role: Some(role),
            store: S::default(),
            batches: Vec::new(),
            waiting: Vec::new(),
        }
    }

    /// The flight rule: a send that waits on a batch not yet durable is
    /// held; any other send is returned, to go now.
    fn send(
        &mut self,
        to: NodeId,
        message: Message,
        after: Option<PersistId>,
    ) -> Option<(NodeId, Message)> {
        match after {
            Some(id) if self.store.durable() < Some(id) => {
                self.waiting.push((id, to, message));
                None
            }
            _ => Some((to, message)),
        }
    }

    /// Make every batch durable, then return the sends that waited on
    /// them. A real driver groups many batches into one write here (group
    /// commit).
    fn flush(&mut self) -> Vec<(NodeId, Message)> {
        for (id, ops) in self.batches.drain(..) {
            self.store.persist(id, &ops);
        }
        let durable = self.store.durable();
        let (ready, waiting): (Vec<_>, Vec<_>) = self
            .waiting
            .drain(..)
            .partition(|(id, ..)| Some(*id) <= durable);
        self.waiting = waiting;
        ready
            .into_iter()
            .map(|(_, to, message)| (to, message))
            .collect()
    }

    /// A crash: what was not durable is lost, and so are the sends that
    /// waited on it. The store keeps every durable batch.
    fn crash(&mut self) {
        self.role = None;
        self.batches.clear();
        self.waiting.clear();
    }
}

type AcceptorNode = Node<JournalAcceptor, AcceptorStore>;
type ReplicaNode = Node<SegmentReplica, ReplicaStore>;

/// Where a register change came from: a leader is told of a refusal.
enum Origin {
    Leader(NodeId),
    Replica,
}

struct Driver {
    register: RegisterValue,
    acceptors: BTreeMap<NodeId, AcceptorNode>,
    replicas: BTreeMap<NodeId, ReplicaNode>,
    leaders: BTreeMap<NodeId, JournalLeader<u64>>,
    network: VecDeque<(NodeId, NodeId, Message)>,
    changes: VecDeque<(Origin, Change)>,
    replies: BTreeMap<u64, Reply>,
    tokens: u64,
}

impl Driver {
    fn new() -> Self {
        let register = RegisterValue::new()
            .apply(&Change::Elect { holder: NodeId(0) })
            .expect("the first election of the controller");
        let acceptors = ACCEPTORS
            .iter()
            .map(|id| {
                let role = JournalAcceptor::recover(*id, AcceptorDisk::default(), PAGE);
                (*id, Node::new(role))
            })
            .collect();
        Self {
            register,
            acceptors,
            replicas: BTreeMap::new(),
            leaders: BTreeMap::new(),
            network: VecDeque::new(),
            changes: VecDeque::new(),
            replies: BTreeMap::new(),
            tokens: 0,
        }
    }

    /// The controller's reshape: the next segment, on the same acceptors,
    /// with the replica set `10 * id + 1 ..= 10 * id + 3`.
    fn open_segment(&mut self) -> SegmentId {
        let newest = self.register.newest().map(SegmentRecord::id);
        let id = SegmentId(newest.map_or(1, |SegmentId(n)| n + 1));
        let replicas: Vec<NodeId> = (1..=3).map(|i| NodeId(10 * id.0 + i)).collect();
        let change = Change::Reshape {
            opening: OpeningCall {
                term: self.register.term().number,
                expected: newest,
                grid: Grid::new(1, vec![ACCEPTORS.to_vec()]).expect("a v1 grid"),
                replicas: replicas.clone(),
            },
            release: RELEASE,
        };
        self.register = self.register.apply(&change).expect("the reshape");
        for node in &replicas {
            let role = SegmentReplica::recover(
                *node,
                id,
                replicas.clone(),
                Leader::Unset,
                ReplicaDisk::default(),
            );
            self.replicas.insert(*node, Node::new(role));
        }
        self.observe();
        id
    }

    /// A new leader on `node`, bound to the newest segment.
    fn campaign(&mut self, node: NodeId, round: u64) {
        let value = self.register.clone();
        let segment = value.newest().expect("a segment").id();
        // The first slot the leader reads: the applied prefix of the oldest
        // segment it reads, as its replicas report it.
        let oldest = value
            .segments()
            .values()
            .find(|r| r.state() < SegmentState::Retired)
            .expect("a segment is read");
        let from = oldest
            .replicas()
            .iter()
            .filter_map(|r| self.replicas[r].role.as_ref()?.front())
            .max()
            .unwrap_or(Slot(0));
        let campaign = Campaign {
            ballot: Ballot { round, node },
            segment,
            from,
        };
        let (leader, effects) =
            JournalLeader::campaign(campaign, &value).expect("the newest segment takes a leader");
        self.leaders.insert(node, leader);
        self.leader_effects(node, effects);
    }

    /// The writer's call, sent to the leader on `node`. Returns the token
    /// of its reply.
    fn propose(&mut self, node: NodeId, command: Command) -> u64 {
        self.tokens += 1;
        let token = self.tokens;
        let leader = self.leaders.get_mut(&node).expect("a leader");
        let (proposed, effects) = leader.propose(token, command);
        assert!(
            matches!(proposed, Proposed::Slot(_)),
            "an open leader takes the call"
        );
        self.leader_effects(node, effects);
        token
    }

    fn leader_effects(&mut self, node: NodeId, effects: Vec<LeaderEffect<u64>>) {
        for effect in effects {
            match effect {
                LeaderEffect::Send { to, message } => self.network.push_back((node, to, message)),
                LeaderEffect::Register(change) => {
                    self.changes.push_back((Origin::Leader(node), change));
                }
                LeaderEffect::Reply { token, reply } => {
                    self.replies.insert(token, reply);
                }
            }
        }
    }

    fn acceptor_effects(&mut self, node: NodeId, effects: Vec<AcceptorEffect>) {
        let acceptor = self.acceptors.get_mut(&node).expect("an acceptor");
        for effect in effects {
            match effect {
                AcceptorEffect::Persist { id, ops } => acceptor.batches.push((id, ops)),
                AcceptorEffect::Send { to, message, after } => {
                    if let Some((to, message)) = acceptor.send(to, message, after) {
                        self.network.push_back((node, to, message));
                    }
                }
            }
        }
    }

    fn replica_effects(&mut self, node: NodeId, effects: Vec<ReplicaEffect>) {
        let replica = self.replicas.get_mut(&node).expect("a replica");
        for effect in effects {
            match effect {
                ReplicaEffect::Persist { id, ops } => replica.batches.push((id, ops)),
                ReplicaEffect::Send { to, message, after } => {
                    if let Some((to, message)) = replica.send(to, message, after) {
                        self.network.push_back((node, to, message));
                    }
                }
                ReplicaEffect::Register(change) => {
                    self.changes.push_back((Origin::Replica, change));
                }
            }
        }
    }

    /// Make every batch durable, then release the sends that waited on it.
    fn flush(&mut self) {
        for (node, acceptor) in &mut self.acceptors {
            for (to, message) in acceptor.flush() {
                self.network.push_back((*node, to, message));
            }
        }
        for (node, replica) in &mut self.replicas {
            for (to, message) in replica.flush() {
                self.network.push_back((*node, to, message));
            }
        }
    }

    /// Deliver one message. A message to a node that is down is lost.
    fn deliver(&mut self, from: NodeId, to: NodeId, message: Message) {
        if let Some(leader) = self.leaders.get_mut(&to) {
            let effects = leader.step(from, message);
            self.leader_effects(to, effects);
        } else if let Some(acceptor) = self.acceptors.get_mut(&to) {
            if let Some(role) = acceptor.role.as_mut() {
                let effects = role.step(from, message);
                self.acceptor_effects(to, effects);
            }
        } else if let Some(replica) = self.replicas.get_mut(&to)
            && let Some(role) = replica.role.as_mut()
        {
            let effects = role.step(from, message);
            self.replica_effects(to, effects);
        }
    }

    /// Run one register change; on success every role sees the new value.
    fn register_step(&mut self, origin: &Origin, change: &Change) {
        match self.register.apply(change) {
            Ok(value) => {
                self.register = value;
                self.observe();
            }
            Err(_) => {
                if let Origin::Leader(node) = *origin {
                    let leader = self.leaders.get_mut(&node).expect("a leader");
                    let effects = leader.refused();
                    self.leader_effects(node, effects);
                }
            }
        }
    }

    /// Every role reads the register value that a done write gave.
    fn observe(&mut self) {
        let value = self.register.clone();
        let leaders: Vec<NodeId> = self.leaders.keys().copied().collect();
        for node in leaders {
            let effects = self
                .leaders
                .get_mut(&node)
                .expect("a leader")
                .observe(&value);
            self.leader_effects(node, effects);
        }
        let replicas: Vec<NodeId> = self.replicas.keys().copied().collect();
        for node in replicas {
            if let Some(role) = self
                .replicas
                .get_mut(&node)
                .expect("a replica")
                .role
                .as_mut()
            {
                let effects = role.observe(&value);
                self.replica_effects(node, effects);
            }
        }
    }

    /// The clock: every role's periodic step.
    fn tick(&mut self) {
        let leaders: Vec<NodeId> = self.leaders.keys().copied().collect();
        for node in leaders {
            let effects = self.leaders.get_mut(&node).expect("a leader").tick(PAGE);
            self.leader_effects(node, effects);
        }
        let replicas: Vec<NodeId> = self.replicas.keys().copied().collect();
        for node in replicas {
            if let Some(role) = self.replicas[&node].role.as_ref() {
                let effects = role.tick();
                self.replica_effects(node, effects);
            }
        }
    }

    /// Run the loop until `done` holds: deliver, make durable, run the
    /// register's changes, and tick when nothing else is left.
    fn run_until(&mut self, what: &str, done: impl Fn(&Self) -> bool) {
        for _ in 0..64 {
            loop {
                self.flush();
                if let Some((from, to, message)) = self.network.pop_front() {
                    self.deliver(from, to, message);
                } else if let Some((origin, change)) = self.changes.pop_front() {
                    self.register_step(&origin, &change);
                } else {
                    break;
                }
            }
            if done(self) {
                return;
            }
            self.tick();
        }
        panic!("{what}: not reached in 64 ticks");
    }

    /// The answer of a writer's call, once the leader gave it.
    fn answer(&self, token: u64) -> &Answer {
        match self.replies.get(&token) {
            Some(Reply::Answer(answer)) => answer,
            other => panic!("token {token}: no answer yet ({other:?})"),
        }
    }

    fn crash_acceptor(&mut self, node: NodeId) {
        self.acceptors.get_mut(&node).expect("an acceptor").crash();
    }

    fn restart_acceptor(&mut self, node: NodeId) -> AcceptorDisk {
        let acceptor = self.acceptors.get_mut(&node).expect("an acceptor");
        let disk = acceptor.store.load();
        acceptor.role = Some(JournalAcceptor::recover(node, disk.clone(), PAGE));
        disk
    }

    fn replica(&self, node: NodeId) -> &SegmentReplica {
        self.replicas[&node].role.as_ref().expect("a live replica")
    }

    fn leader(&self, node: NodeId) -> &JournalLeader<u64> {
        &self.leaders[&node]
    }

    fn replicas_of(&self, segment: SegmentId) -> Vec<NodeId> {
        self.register
            .segment(segment)
            .expect("a segment")
            .replicas()
            .to_vec()
    }
}

fn main() {
    let mut driver = Driver::new();
    let first = open_first(&mut driver);
    write_first(&mut driver);
    crash_and_restart(&mut driver);
    let second = seal(&mut driver, first);
    write_second(&mut driver, first, second);
    println!("ok: six records, two segments, one acceptor restarted from its own store.");
}

/// Step 1: segment 1 opens, and leader L1 runs Phase 1.
fn open_first(driver: &mut Driver) -> SegmentId {
    println!("1. Segment 1 opens; leader L1 runs Phase 1.");
    let first = driver.open_segment();
    driver.campaign(L1, 1);
    driver.run_until("L1 opens", |d| {
        matches!(d.leader(L1).phase(), LeaderPhase::Open { .. })
    });
    show("L1 phase", driver.leader(L1).phase());
    assert_eq!(
        *driver.leader(L1).phase(),
        LeaderPhase::Open { start: Slot(0) }
    );
    first
}

/// Step 2: writer A takes the journal and writes two batches.
fn write_first(driver: &mut Driver) {
    println!("2. Writer A takes the journal and writes.");
    let set = driver.propose(
        L1,
        Command::SetLeader {
            new: Some(A),
            old: Leader::Unset,
        },
    );
    let w0 = driver.propose(L1, Command::Write(fenced(A, 0, &["a0", "a1"])));
    let w2 = driver.propose(L1, Command::Write(fenced(A, 2, &["a2"])));
    driver.run_until("the three calls answer", |d| {
        [set, w0, w2].iter().all(|t| d.replies.contains_key(t))
    });
    show("SetLeader(A, Unset)", driver.answer(set));
    show("Write(A, 0, [a0, a1])", driver.answer(w0));
    show("Write(A, 2, [a2])", driver.answer(w2));
    assert!(matches!(driver.answer(set), Answer::LeaderSet(_)));
    assert_eq!(*driver.answer(w0), written(0, 2));
    assert_eq!(*driver.answer(w2), written(2, 1));
}

/// Step 3: acceptor 2 crashes, a write is chosen without it, and it starts
/// again from its store.
fn crash_and_restart(driver: &mut Driver) {
    println!("3. Acceptor 2 crashes; F + 1 of the column still choose.");
    driver.crash_acceptor(NodeId(2));
    let w3 = driver.propose(L1, Command::Write(fenced(A, 3, &["a3"])));
    driver.run_until("the write answers", |d| d.replies.contains_key(&w3));
    show("Write(A, 3, [a3]) with acceptor 2 down", driver.answer(w3));
    assert_eq!(*driver.answer(w3), written(3, 1));
    let disk = driver.restart_acceptor(NodeId(2));
    show(
        "acceptor 2 reads its store again",
        &(disk.promised, disk.votes.keys().collect::<Vec<_>>()),
    );
    assert_eq!(disk.promised, Ballot { round: 1, node: L1 });
    assert_eq!(
        disk.votes.keys().copied().collect::<Vec<_>>(),
        [Slot(0), Slot(1), Slot(2)],
        "slot 3 was chosen while acceptor 2 was down"
    );
}

/// Step 4: segment 2 opens, and leader L2 seals segment 1.
fn seal(driver: &mut Driver, first: SegmentId) -> SegmentId {
    println!("4. Segment 2 opens; leader L2 seals segment 1.");
    let second = driver.open_segment();
    let stopped = driver.leaders.get_mut(&L1).expect("L1").stop();
    assert!(stopped.is_empty(), "L1 holds no unanswered call");
    driver.campaign(L2, 2);
    driver.run_until("L2 opens and segment 2 folds", |d| {
        matches!(d.leader(L2).phase(), LeaderPhase::Open { .. })
            && d.replicas_of(second)
                .iter()
                .all(|r| d.replica(*r).front().is_some())
    });
    let record = driver.register.segment(first).expect("segment 1");
    let end = record.end().expect("segment 1 is sealed");
    show("segment 1 sealed at", &end);
    show("boundary of segment 1", &record.boundary());
    assert_eq!(end, Slot(4), "one above the highest vote, slot 3");
    assert_eq!(*driver.leader(L2).phase(), LeaderPhase::Open { start: end });
    second
}

/// Step 5: writer A writes in segment 2, at the next position.
fn write_second(driver: &mut Driver, first: SegmentId, second: SegmentId) {
    println!("5. Writer A writes again, now in segment 2.");
    let w4 = driver.propose(L2, Command::Write(fenced(A, 4, &["a4", "a5"])));
    driver.run_until("the write answers", |d| d.replies.contains_key(&w4));
    show("Write(A, 4, [a4, a5])", driver.answer(w4));
    assert_eq!(*driver.answer(w4), written(4, 2));
    for node in driver.replicas_of(second) {
        let state = driver
            .replica(node)
            .state()
            .expect("a replica with its base");
        assert_eq!(state.next_seq(), Seq(6));
        assert_eq!(state.leader(), Leader::Uuid(A));
    }
    for node in driver.replicas_of(first) {
        assert!(
            driver.replica(node).full(),
            "each replica of segment 1 holds all of it"
        );
    }
}
