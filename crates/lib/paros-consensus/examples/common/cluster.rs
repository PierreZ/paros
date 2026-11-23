//! A toy cluster for the examples that drive [`ColocatedNode`]: the nodes,
//! one disk for each node, an optional set of matchmakers, and a wire.
//!
//! It is the smallest complete driver of the core. Read [`Cluster::pump`]
//! first: it is the loop that every driver writes, and it obeys the one
//! rule of the contract, **persist before send**. The rest is a network
//! that the example controls: [`Cluster::settle`] delivers until the cluster
//! is quiet, [`Cluster::cut`] isolates a node, and [`Cluster::crash`] and
//! [`Cluster::restart`] stop a node and boot it again from its disk.
//!
//! Nothing here is asynchronous and nothing is random, so each example
//! prints the same trace on every run.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use paros_consensus::contract::MemStorage;
use paros_consensus::journal::{Command, Control, Entry, JournalId, LeaderUuid, Outcome, Seq};
use paros_consensus::matchmaker::{
    GcAck, GcRequest, MatchReply, MatchRequest, MatchmakerConfig, MemRegistry,
};
use paros_consensus::membership::MatchmakerId;
use paros_consensus::node::ProposeResult;
use paros_consensus::quorum_read::ReadState;
use paros_consensus::replica::LogRead;
use paros_consensus::{ColocatedNode, Config, Limits, Matchmaker, Message, NodeId, Slot, Value};

/// How many batches one node may drain in one [`Cluster::pump`], and how
/// many packets one [`Cluster::settle`] may deliver. A healthy example is
/// far below both; a run that reaches one has a loop.
const BUDGET: usize = 10_000;

/// One thing in flight. A peer message travels on the peer wire. The four
/// other variants are the matchmaker service, a separate RPC that only a
/// matchmaker deployment speaks.
enum Packet {
    Peer {
        from: NodeId,
        to: NodeId,
        message: Message,
    },
    Match(MatchmakerId, MatchRequest),
    MatchReply(MatchReply),
    Gc(MatchmakerId, GcRequest),
    GcAck(NodeId, GcAck),
}

/// One matchmaker and its registry on disk.
struct Registry {
    matchmaker: Matchmaker,
    disk: MemRegistry,
}

/// The toy cluster. See the module doc.
pub struct Cluster {
    /// One disk for each node. A disk outlives a crash of its node.
    disks: BTreeMap<NodeId, MemStorage>,
    /// The running nodes. A crashed node is absent.
    nodes: BTreeMap<NodeId, ColocatedNode>,
    /// The matchmakers, `MatchmakerId(0..n)`. Empty on plain Multi-Paxos.
    registries: Vec<Registry>,
    wire: VecDeque<Packet>,
    /// The nodes that the network does not reach, in either direction.
    isolated: BTreeSet<NodeId>,
    /// What each node learned, in slot order: the `committed` bucket of
    /// each of its batches.
    learned: BTreeMap<NodeId, Vec<(Slot, Command, Outcome)>>,
    /// The quorum reads each node served: the `read_states` bucket.
    reads: BTreeMap<NodeId, Vec<ReadState>>,
    /// How many peer messages of each kind left a node since the last
    /// [`Cluster::take_sent`].
    sent: BTreeMap<&'static str, usize>,
    /// Print each delivery?
    pub trace: bool,
}

impl Cluster {
    /// A cluster with one node for each configuration, each on an empty disk.
    pub fn new(configs: impl IntoIterator<Item = Config>) -> Self {
        let mut cluster = Self {
            disks: BTreeMap::new(),
            nodes: BTreeMap::new(),
            registries: Vec::new(),
            wire: VecDeque::new(),
            isolated: BTreeSet::new(),
            learned: BTreeMap::new(),
            reads: BTreeMap::new(),
            sent: BTreeMap::new(),
            trace: false,
        };
        for config in configs {
            let id = config.id;
            cluster.disks.insert(id, MemStorage::new(config));
            cluster.restart(id);
        }
        cluster
    }

    /// The same cluster with `count` matchmakers, `MatchmakerId(0..count)`,
    /// each on an empty registry. The node configurations must name the
    /// same set in `Config::matchmakers`.
    pub fn with_matchmakers(mut self, count: u64) -> Self {
        let bootstrap: Vec<MatchmakerId> = (0..count).map(MatchmakerId).collect();
        self.registries = bootstrap
            .iter()
            .map(|id| {
                let disk = MemRegistry::default();
                let config = MatchmakerConfig {
                    id: *id,
                    bootstrap: bootstrap.clone(),
                };
                Registry {
                    matchmaker: Matchmaker::new(&config, &disk, Limits::DEFAULT),
                    disk,
                }
            })
            .collect();
        self
    }

    /// The running node `id`.
    pub fn node(&mut self, id: NodeId) -> &mut ColocatedNode {
        self.nodes.get_mut(&id).expect("the node is running")
    }

    /// The disk of node `id`.
    pub fn disk(&self, id: NodeId) -> &MemStorage {
        &self.disks[&id]
    }

    /// The matchmaker `id`.
    pub fn matchmaker(&self, id: MatchmakerId) -> &Matchmaker {
        let index = usize::try_from(id.0).expect("a small matchmaker id");
        &self.registries[index].matchmaker
    }

    /// The ids of the running nodes.
    pub fn running(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    // ---- the driver loop ----------------------------------------------------

    /// **The driver loop.** Drain node `id` until its batch is empty. Each
    /// batch is processed in the order of the contract:
    ///
    /// 1. persist `writes()` to the disk of the node;
    /// 2. only then send `messages()` (and the matchmaker requests);
    /// 3. learn `committed()`: the slots that the chosen prefix moved over;
    /// 4. answer `read_states()`;
    /// 5. `advance()`, then `advance_recovery()` for the next page of a
    ///    recovery.
    ///
    /// A real driver does the same with a disk that it fsyncs in step 1 and
    /// a socket in step 2.
    pub fn pump(&mut self, id: NodeId) {
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        let disk = self.disks.get_mut(&id).expect("each node has a disk");
        for _ in 0..BUDGET {
            let pool = node.pool().to_vec();
            let ready = node.ready();
            if ready.is_empty() {
                ready.advance();
                return;
            }
            // 1. Persist. From here the batch survives a crash.
            disk.apply(ready.writes());
            // 2. Send. The core names an audience; the driver resolves it
            //    to the nodes of the pool.
            for (audience, message) in ready.messages() {
                *self.sent.entry(message.kind()).or_default() += 1;
                for to in audience.resolve(&pool, id) {
                    self.wire.push_back(Packet::Peer {
                        from: id,
                        to,
                        message: message.clone(),
                    });
                }
            }
            for (to, request) in ready.match_requests() {
                self.wire.push_back(Packet::Match(*to, request.clone()));
            }
            for (to, request) in ready.gc_requests() {
                self.wire.push_back(Packet::Gc(*to, *request));
            }
            // 3. Learn. A driver answers here the clients that wait on
            //    these slots.
            self.learned.entry(id).or_default().extend(
                ready
                    .committed()
                    .iter()
                    .map(|(slot, _, command, outcome)| (*slot, command.clone(), outcome.clone())),
            );
            // 4. Answer the reads that this batch can serve.
            self.reads
                .entry(id)
                .or_default()
                .extend_from_slice(ready.read_states());
            // 5. Release the batch.
            ready.advance();
            node.advance_recovery();
        }
        panic!("node {id} did not become quiet");
    }

    // ---- the network --------------------------------------------------------

    /// Deliver every packet in flight, and every packet that a delivery
    /// causes, until the cluster is quiet.
    pub fn settle(&mut self) {
        self.settle_where(|_, _, _| true);
    }

    /// [`Cluster::settle`], but a peer message for which `deliver(from, to,
    /// message)` is false is lost.
    pub fn settle_where(&mut self, deliver: impl Fn(NodeId, NodeId, &Message) -> bool) {
        for id in self.running() {
            self.pump(id);
        }
        for _ in 0..BUDGET {
            let Some(packet) = self.wire.pop_front() else {
                return;
            };
            match packet {
                Packet::Peer { from, to, message } => {
                    let reachable = !self.isolated.contains(&from) && !self.isolated.contains(&to);
                    let kept = reachable && deliver(from, to, &message);
                    let running = self.nodes.contains_key(&to);
                    if self.trace {
                        let fate = match (kept, running) {
                            (false, _) => "  (lost)",
                            (true, false) => "  (the node is down)",
                            (true, true) => "",
                        };
                        println!("     {from} -> {to}  {}{fate}", message.kind());
                    }
                    if kept && running {
                        self.node(to).step(message);
                        self.pump(to);
                    }
                }
                Packet::Match(to, request) => self.matchmaker_step(to, request),
                Packet::MatchReply(reply) => {
                    let to = reply.to;
                    if self.nodes.contains_key(&to) && !self.isolated.contains(&to) {
                        let _ = self.node(to).on_match_reply(reply);
                        self.pump(to);
                    }
                }
                Packet::Gc(to, request) => self.matchmaker_collect(to, request),
                Packet::GcAck(to, ack) => {
                    if self.nodes.contains_key(&to) && !self.isolated.contains(&to) {
                        let _ = self.node(to).on_gc_ack(&ack);
                        self.pump(to);
                    }
                }
            }
        }
        panic!("the cluster did not become quiet");
    }

    /// One matchmaking request at matchmaker `to`: the same order as a
    /// node, persist the registration and then answer.
    fn matchmaker_step(&mut self, to: MatchmakerId, request: MatchRequest) {
        if self.isolated.contains(&request.from) {
            return;
        }
        let index = usize::try_from(to.0).expect("a small matchmaker id");
        let registry = &mut self.registries[index];
        if self.trace {
            // `Register(Belief)`, `Probe`, or `Retire` without its fields.
            let purpose = format!("{:?}", request.purpose);
            let purpose = purpose.split([' ', '{']).next().unwrap_or_default();
            println!(
                "     {} -> matchmaker {}  {purpose} at ballot {}",
                request.from, to.0, request.ballot
            );
        }
        registry.matchmaker.step(request);
        let ready = registry.matchmaker.ready();
        for write in ready.writes() {
            registry.disk.apply(write);
        }
        for reply in ready.replies() {
            self.wire.push_back(Packet::MatchReply(reply.clone()));
        }
        ready.advance();
    }

    /// One garbage-collection request at matchmaker `to`: raise the
    /// watermark, persist it, then acknowledge.
    fn matchmaker_collect(&mut self, to: MatchmakerId, request: GcRequest) {
        if self.isolated.contains(&request.from) {
            return;
        }
        let index = usize::try_from(to.0).expect("a small matchmaker id");
        let registry = &mut self.registries[index];
        if self.trace {
            println!(
                "     {} -> matchmaker {}  Gc up to ballot {}",
                request.from, to.0, request.watermark
            );
        }
        let outcome = registry.matchmaker.advance_gc_watermark(
            request.journal,
            request.generation,
            request.watermark,
        );
        let ready = registry.matchmaker.ready();
        for write in ready.writes() {
            registry.disk.apply(write);
        }
        ready.advance();
        let ack = registry.matchmaker.gc_ack(&request, outcome);
        self.wire.push_back(Packet::GcAck(request.from, ack));
    }

    /// One tick of logical time on every running node, then
    /// [`Cluster::settle`]. A leader beats on a tick, and a follower that
    /// is behind asks the sender of the beat for the slots it misses.
    pub fn tick(&mut self) {
        for id in self.running() {
            self.node(id).tick();
        }
        self.settle();
    }

    /// The network stops reaching `id`: each message to it or from it is
    /// lost. The node keeps running.
    pub fn cut(&mut self, id: NodeId) {
        self.isolated.insert(id);
    }

    /// The network reaches `id` again.
    pub fn heal(&mut self, id: NodeId) {
        self.isolated.remove(&id);
    }

    /// Stop node `id`. Its memory is gone: the leadership, the tallies and
    /// the batch it did not persist. Its disk stays.
    pub fn crash(&mut self, id: NodeId) {
        self.nodes.remove(&id).expect("the node was running");
    }

    /// Boot node `id` from its disk. A fresh disk and a disk that a crash
    /// left are the same path: [`ColocatedNode::new`] reads the promise, the
    /// accepted records and the chosen index, and the node starts as a
    /// follower.
    pub fn restart(&mut self, id: NodeId) {
        let node = ColocatedNode::new(&self.disks[&id], Limits::DEFAULT);
        assert!(self.nodes.insert(id, node).is_none(), "the node was down");
    }

    // ---- the calls of an example ---------------------------------------------

    /// Node `id` campaigns and wins, then beats one time so that each
    /// follower knows the leader.
    ///
    /// # Panics
    ///
    /// If the campaign does not win: the caller left no quorum reachable.
    pub fn elect(&mut self, id: NodeId) {
        self.node(id).campaign();
        self.settle();
        assert!(self.node(id).is_leader(), "node {id} wins its election");
        self.tick();
    }

    /// Propose `control` on the leader `id` and deliver until the cluster is
    /// quiet. Returns the slot.
    pub fn control(&mut self, id: NodeId, control: Control) -> Slot {
        let ProposeResult::Accepted(slot) = self.node(id).propose_control(control) else {
            panic!("node {id} is the leader");
        };
        self.settle();
        slot
    }

    /// Propose one `Write` on the leader `id`: `text` as one record, at
    /// position `seq`, by the writer `writer`. Delivers until the cluster is
    /// quiet and returns the slot.
    pub fn write(&mut self, id: NodeId, writer: LeaderUuid, seq: u64, text: &str) -> Slot {
        let entry = Entry {
            leader: writer,
            seq: Seq(seq),
            records: vec![Value::from(text.as_bytes().to_vec())],
        };
        let ProposeResult::Accepted(slot) = self.node(id).propose(entry) else {
            panic!("node {id} is the leader");
        };
        self.settle();
        slot
    }

    /// What node `id` learned at `slot`: the verdict of the journal state
    /// machine, from the `committed` bucket.
    pub fn outcome(&self, id: NodeId, slot: Slot) -> Option<&Outcome> {
        self.learned
            .get(&id)?
            .iter()
            .find(|(s, _, _)| *s == slot)
            .map(|(_, _, outcome)| outcome)
    }

    /// The journal as node `id` serves it to a reader: each record from
    /// the first position that the journal still holds, as text.
    pub fn journal(&mut self, id: NodeId) -> Vec<String> {
        let first = self.node(id).replica().journal().view().first_seq;
        match self.node(id).read_log(first, usize::MAX, usize::MAX) {
            LogRead::Page(page) => page
                .records
                .iter()
                .map(|record| String::from_utf8_lossy(record).into_owned())
                .collect(),
            other => panic!("node {id} serves its own journal: {other:?}"),
        }
    }

    /// The quorum reads that node `id` served since the last call.
    pub fn take_reads(&mut self, id: NodeId) -> Vec<ReadState> {
        self.reads.remove(&id).unwrap_or_default()
    }

    /// How many peer messages of each kind the nodes sent since the last
    /// call. One fan-out counts one time.
    pub fn take_sent(&mut self) -> BTreeMap<&'static str, usize> {
        std::mem::take(&mut self.sent)
    }

    /// The journal that the matchmakers file this cluster under.
    pub fn journal_id(&mut self, id: NodeId) -> JournalId {
        self.node(id).config().journal.journal
    }
}
