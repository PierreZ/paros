//! A toy cluster for the examples: the nodes, one disk for each node, a
//! wire, and the other processes that the [`Config`] of the deployment
//! names (matchmakers, proxy leaders, replicas).
//!
//! It is the smallest complete driver of the core. Read [`Cluster::pump`]
//! first: it is the loop that every driver writes, and it obeys the one
//! rule of the contract, **persist before send**. Each other kind of
//! process has a loop of the same shape below it: `pump_replica` for a
//! [`ReplicaNode`], `pump_proxy` for a [`ProxyLeader`] and
//! `pump_matchmaker` for a [`Matchmaker`].
//!
//! The rest is a network that the example controls: [`Cluster::settle`]
//! delivers until the cluster is quiet, [`Cluster::settle_where`] loses
//! messages, [`Cluster::settle_holding`] keeps messages in flight,
//! and [`Cluster::crash`] and
//! [`Cluster::restart`] stop a node and boot it again from its disk.
//!
//! Nothing here is asynchronous and nothing is random, so each example
//! prints the same trace on every run.

use std::collections::{BTreeMap, VecDeque};

use paros_consensus::contract::{Audience, MemStorage, Party};
use paros_consensus::journal::{Command, Control, Entry, JournalId, LeaderUuid, Outcome, Seq};
use paros_consensus::matchmaker::{
    GcAck, GcRequest, MatchReply, MatchRequest, MatchmakerConfig, MemRegistry, ReconfigureReply,
    ReconfigureRequest, SegmentAck, SegmentPublish,
};
use paros_consensus::membership::{MatchmakerId, ProxyId};
use paros_consensus::node::ProposeResult;
use paros_consensus::quorum_read::ReadState;
use paros_consensus::replica::LogRead;
use paros_consensus::{
    ColocatedNode, Config, Limits, Matchmaker, Message, NodeId, ProxyLeader, ReplicaNode, Slot,
    Value,
};

/// How many batches one process may drain in one pump, and how many packets
/// one [`Cluster::settle`] may deliver. A healthy example is far below
/// both; a run that reaches one has a loop.
const BUDGET: usize = 10_000;

/// One thing in flight. A peer message travels on the peer wire, to a node,
/// a replica or a proxy leader. The other variants are the matchmaker
/// service, a separate RPC that only a matchmaker deployment speaks.
enum Packet {
    Peer {
        from: Party,
        to: Party,
        message: Message,
    },
    Match(MatchmakerId, MatchRequest),
    MatchReply(MatchReply),
    Gc(MatchmakerId, GcRequest),
    GcAck(NodeId, GcAck),
    Publish(MatchmakerId, SegmentPublish),
    PublishAck(NodeId, SegmentAck),
}

/// What the network does with one peer message.
enum Fate {
    Deliver,
    Lose,
    Hold,
}

/// One matchmaker and its registry on disk.
struct Registry {
    config: MatchmakerConfig,
    matchmaker: Matchmaker,
    disk: MemRegistry,
}

/// The toy cluster. See the module doc.
pub struct Cluster {
    /// The deployment, as the first node holds it: the pool, the
    /// matchmakers and the number of proxy leaders.
    deployment: Config,
    /// One disk for each node and each replica. A disk outlives a crash.
    disks: BTreeMap<NodeId, MemStorage>,
    /// The running nodes. A crashed node is absent.
    nodes: BTreeMap<NodeId, ColocatedNode>,
    /// The replicas: processes that learn and never vote. Their ids are
    /// outside the pool of the nodes.
    replicas: BTreeMap<NodeId, ReplicaNode>,
    /// The running proxy leaders. A proxy leader has no disk.
    proxies: BTreeMap<ProxyId, ProxyLeader>,
    /// The matchmakers of the pool. Empty on plain Multi-Paxos.
    registries: BTreeMap<MatchmakerId, Registry>,
    wire: VecDeque<Packet>,
    /// What each node and each replica learned, in slot order: the
    /// `committed` bucket of each of its batches.
    learned: BTreeMap<NodeId, Vec<(Slot, Command, Outcome)>>,
    /// The quorum reads each node served: the `read_states` bucket.
    reads: BTreeMap<NodeId, Vec<ReadState>>,
    /// How many peer messages of each kind left a node since the last
    /// [`Cluster::take_sent`].
    sent: BTreeMap<&'static str, usize>,
    /// How many peer messages each node sent and received since the last
    /// [`Cluster::take_traffic`].
    traffic: BTreeMap<NodeId, usize>,
    /// How many peer messages of each kind each node received.
    received: BTreeMap<(NodeId, &'static str), usize>,
    /// Print each delivery?
    pub trace: bool,
}

impl Cluster {
    /// A cluster with one node for each configuration, each on an empty
    /// disk, and the processes that the first configuration names: one
    /// matchmaker for each id of `Config::matchmaker_pool`, and
    /// `Config::proxy_count` proxy leaders.
    pub fn new(configs: impl IntoIterator<Item = Config>) -> Self {
        let configs: Vec<Config> = configs.into_iter().collect();
        let deployment = configs.first().expect("a cluster has a node").clone();
        let mut cluster = Self {
            deployment: deployment.clone(),
            disks: BTreeMap::new(),
            nodes: BTreeMap::new(),
            replicas: BTreeMap::new(),
            proxies: BTreeMap::new(),
            registries: BTreeMap::new(),
            wire: VecDeque::new(),
            learned: BTreeMap::new(),
            reads: BTreeMap::new(),
            sent: BTreeMap::new(),
            traffic: BTreeMap::new(),
            received: BTreeMap::new(),
            trace: false,
        };
        for config in configs {
            let id = config.id;
            cluster.disks.insert(id, MemStorage::new(config));
            cluster.restart(id);
        }
        if deployment.has_matchmakers() {
            for id in deployment.matchmaker_pool() {
                let disk = MemRegistry::default();
                let config = MatchmakerConfig {
                    id: *id,
                    bootstrap: deployment.matchmakers.clone(),
                };
                let matchmaker = Matchmaker::new(&config, &disk, Limits::DEFAULT);
                let registry = Registry {
                    config,
                    matchmaker,
                    disk,
                };
                cluster.registries.insert(*id, registry);
            }
        }
        for id in (0..deployment.proxy_count).map(|p| ProxyId(p as u64)) {
            cluster.restart_proxy(id);
        }
        cluster
    }

    /// Start one replica for each id, each on an empty disk, at any time. A
    /// replica has the configuration of the deployment under its own id, and
    /// the id is outside the pool of the nodes. A replica that starts late
    /// learns the log from the beat of the leader.
    pub fn add_replicas(&mut self, ids: impl IntoIterator<Item = NodeId>) {
        for id in ids {
            let config = Config {
                id,
                ..self.deployment.clone()
            };
            let disk = MemStorage::new(config);
            let replica = ReplicaNode::new(&disk, Limits::DEFAULT);
            assert!(self.disks.insert(id, disk).is_none(), "a new process id");
            self.replicas.insert(id, replica);
        }
    }

    /// The running node `id`.
    pub fn node(&mut self, id: NodeId) -> &mut ColocatedNode {
        self.nodes.get_mut(&id).expect("the node is running")
    }

    /// The replica `id`.
    pub fn replica(&mut self, id: NodeId) -> &mut ReplicaNode {
        self.replicas
            .get_mut(&id)
            .expect("a replica of the cluster")
    }

    /// The running proxy leader `id`.
    pub fn proxy(&mut self, id: ProxyId) -> &mut ProxyLeader {
        self.proxies.get_mut(&id).expect("the proxy is running")
    }

    /// The disk of node `id`.
    pub fn disk(&self, id: NodeId) -> &MemStorage {
        &self.disks[&id]
    }

    /// The matchmaker `id`.
    pub fn matchmaker(&self, id: MatchmakerId) -> &Matchmaker {
        &self.registries[&id].matchmaker
    }

    /// The ids of the running nodes.
    pub fn running(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    // ---- the driver loops ---------------------------------------------------

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
            // 2. Send. The core names an audience, and the driver resolves
            //    it to processes: this is the deployment map.
            for (audience, message) in ready.messages() {
                *self.sent.entry(message.kind()).or_default() += 1;
                let mut to: Vec<Party> = audience
                    .resolve(&pool, id)
                    .into_iter()
                    .map(Party::Node)
                    .collect();
                // A proxy leader has an id of its own, outside the pool.
                to.extend(audience.proxy().map(Party::Proxy));
                // A replica is a learner too, outside the pool.
                if *audience == Audience::Learners {
                    to.extend(self.replicas.keys().copied().map(Party::Node));
                }
                for to in to {
                    *self.traffic.entry(id).or_default() += 1;
                    self.wire.push_back(Packet::Peer {
                        from: Party::Node(id),
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
            for (to, request) in ready.segment_publishes() {
                self.wire.push_back(Packet::Publish(*to, request.clone()));
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

    /// The driver loop of a [`ReplicaNode`]: the same order as a node. A
    /// replica persists what it learned, then sends (it only asks a node
    /// for the slots it misses), then reports what its walk moved over.
    fn pump_replica(&mut self, id: NodeId) {
        let replica = self.replicas.get_mut(&id).expect("a replica");
        let disk = self.disks.get_mut(&id).expect("each replica has a disk");
        for _ in 0..BUDGET {
            let pool = replica.config().pool().to_vec();
            let ready = replica.ready();
            if ready.is_empty() {
                ready.advance();
                return;
            }
            disk.apply(ready.writes());
            for (audience, message) in ready.messages() {
                for to in audience.resolve(&pool, id) {
                    self.wire.push_back(Packet::Peer {
                        from: Party::Node(id),
                        to: Party::Node(to),
                        message: message.clone(),
                    });
                }
            }
            self.learned.entry(id).or_default().extend(
                ready
                    .committed()
                    .iter()
                    .map(|(slot, _, command, outcome)| (*slot, command.clone(), outcome.clone())),
            );
            ready.advance();
            replica.advance_recovery();
        }
        panic!("replica {id} did not become quiet");
    }

    /// The driver loop of a [`ProxyLeader`]. A proxy leader holds nothing
    /// durable, so its batch has no writes: it only sends. It is the peer
    /// of no node, so its fan-out leaves no node out.
    fn pump_proxy(&mut self, id: ProxyId) {
        let Some(proxy) = self.proxies.get_mut(&id) else {
            return;
        };
        for _ in 0..BUDGET {
            let pool = proxy.pool().to_vec();
            let ready = proxy.ready();
            if ready.is_empty() {
                ready.advance();
                return;
            }
            for (audience, message) in ready.messages() {
                let mut to = audience.resolve_from_proxy(&pool);
                // A replica is a learner too, outside the pool.
                if *audience == Audience::Learners {
                    to.extend(self.replicas.keys().copied());
                }
                for to in to {
                    self.wire.push_back(Packet::Peer {
                        from: Party::Proxy(id),
                        to: Party::Node(to),
                        message: message.clone(),
                    });
                }
            }
            ready.advance();
        }
        panic!("proxy {} did not become quiet", id.0);
    }

    /// The driver loop of a [`Matchmaker`], after one request: the same
    /// order as a node. Persist the writes of the batch, and only then let
    /// its answers leave.
    fn pump_matchmaker(&mut self, id: MatchmakerId) -> Vec<ReconfigureReply> {
        let registry = self.registries.get_mut(&id).expect("a matchmaker");
        let ready = registry.matchmaker.ready();
        for write in ready.writes() {
            registry.disk.apply(write);
        }
        for reply in ready.replies() {
            self.wire.push_back(Packet::MatchReply(reply.clone()));
        }
        let handover = ready.reconfigure_replies().to_vec();
        ready.advance();
        handover
    }

    // ---- the network --------------------------------------------------------

    /// Deliver every packet in flight, and every packet that a delivery
    /// causes, until the cluster is quiet.
    pub fn settle(&mut self) {
        self.run(|_, _| Fate::Deliver);
    }

    /// [`Cluster::settle`], but a peer message for which `deliver(to,
    /// message)` is false is lost.
    pub fn settle_where(&mut self, deliver: impl Fn(Party, &Message) -> bool) {
        self.run(|to, message| {
            if deliver(to, message) {
                Fate::Deliver
            } else {
                Fate::Lose
            }
        });
    }

    /// [`Cluster::settle`], but a peer message for which `hold(to,
    /// message)` is true stays in flight. A later `settle` delivers it.
    pub fn settle_holding(&mut self, hold: impl Fn(Party, &Message) -> bool) {
        self.run(|to, message| {
            if hold(to, message) {
                Fate::Hold
            } else {
                Fate::Deliver
            }
        });
    }

    fn run(&mut self, fate: impl Fn(Party, &Message) -> Fate) {
        for id in self.running() {
            self.pump(id);
        }
        for id in self.replicas.keys().copied().collect::<Vec<_>>() {
            self.pump_replica(id);
        }
        for id in self.proxies.keys().copied().collect::<Vec<_>>() {
            self.pump_proxy(id);
        }
        for _ in 0..BUDGET {
            let next = self.wire.iter().position(|packet| match packet {
                Packet::Peer { to, message, .. } => !matches!(fate(*to, message), Fate::Hold),
                _ => true,
            });
            let Some(packet) = next.and_then(|position| self.wire.remove(position)) else {
                return;
            };
            match packet {
                Packet::Peer { from, to, message } => {
                    let lost = matches!(fate(to, &message), Fate::Lose);
                    self.deliver(from, to, message, lost);
                }
                Packet::Match(to, request) => {
                    if self.trace {
                        // `Register(Belief)`, `Probe`, or `Retire` without its fields.
                        let purpose = format!("{:?}", request.purpose);
                        let purpose = purpose.split([' ', '{']).next().unwrap_or_default();
                        println!(
                            "     {} -> matchmaker {}  {purpose} at ballot {}",
                            request.from, to.0, request.ballot
                        );
                    }
                    let registry = self.registries.get_mut(&to).expect("a matchmaker");
                    registry.matchmaker.step(request);
                    self.pump_matchmaker(to);
                }
                Packet::MatchReply(reply) => {
                    let to = reply.to;
                    if self.nodes.contains_key(&to) {
                        let _ = self.node(to).on_match_reply(reply);
                        self.pump(to);
                    }
                }
                Packet::Gc(to, request) => self.matchmaker_collect(to, request),
                Packet::GcAck(to, ack) => {
                    if self.nodes.contains_key(&to) {
                        let _ = self.node(to).on_gc_ack(&ack);
                        self.pump(to);
                    }
                }
                Packet::Publish(to, request) => self.matchmaker_publish(to, &request),
                Packet::PublishAck(to, ack) => {
                    if self.nodes.contains_key(&to) {
                        let _ = self.node(to).on_segment_ack(&ack);
                        self.pump(to);
                    }
                }
            }
        }
        panic!("the cluster did not become quiet");
    }

    /// One peer message at its destination: `step`, then the drain loop of
    /// the process.
    fn deliver(&mut self, from: Party, to: Party, message: Message, lost: bool) {
        let kept = !lost;
        let running = match to {
            Party::Node(id) => self.nodes.contains_key(&id) || self.replicas.contains_key(&id),
            Party::Proxy(id) => self.proxies.contains_key(&id),
        };
        if self.trace {
            let fate = match (kept, running) {
                (false, _) => "  (lost)",
                (true, false) => "  (it is down)",
                (true, true) => "",
            };
            let (from, to) = (name(from), name(to));
            println!("     {from} -> {to}  {}{fate}", message.kind());
        }
        if !kept || !running {
            return;
        }
        match to {
            Party::Node(id) if self.nodes.contains_key(&id) => {
                *self.traffic.entry(id).or_default() += 1;
                *self.received.entry((id, message.kind())).or_default() += 1;
                self.node(id).step(message);
                self.pump(id);
            }
            Party::Node(id) => {
                self.replica(id).step(message);
                self.pump_replica(id);
            }
            Party::Proxy(id) => {
                self.proxy(id).step(message);
                self.pump_proxy(id);
            }
        }
    }

    /// One garbage-collection request at matchmaker `to`: raise the
    /// watermark, persist it, then acknowledge.
    fn matchmaker_collect(&mut self, to: MatchmakerId, request: GcRequest) {
        if self.trace {
            println!(
                "     {} -> matchmaker {}  Gc up to ballot {}",
                request.from, to.0, request.watermark
            );
        }
        let registry = self.registries.get_mut(&to).expect("a matchmaker");
        let outcome = registry.matchmaker.advance_gc_watermark(
            request.journal,
            request.generation,
            request.watermark,
        );
        self.pump_matchmaker(to);
        let ack = self.registries[&to].matchmaker.gc_ack(&request, outcome);
        self.wire.push_back(Packet::GcAck(request.from, ack));
    }

    /// One publish of the chain of segments at matchmaker `to`: merge it,
    /// persist, then ack.
    fn matchmaker_publish(&mut self, to: MatchmakerId, request: &SegmentPublish) {
        let registry = self.registries.get_mut(&to).expect("a matchmaker");
        let outcome = registry.matchmaker.publish_segment(request);
        self.pump_matchmaker(to);
        let ack = self.registries[&to]
            .matchmaker
            .segment_ack(request, outcome);
        self.wire.push_back(Packet::PublishAck(request.from, ack));
    }

    /// One message of a handover of the matchmaker set at matchmaker `to`,
    /// from a `MatchmakerReconfigurer`: step, persist, and return the
    /// answers. The caller is the driver of the reconfigurer, so the
    /// answers go back to it and not on the wire.
    pub fn matchmaker_reconfigure(
        &mut self,
        to: MatchmakerId,
        request: ReconfigureRequest,
    ) -> Vec<ReconfigureReply> {
        let registry = self.registries.get_mut(&to).expect("a matchmaker");
        registry.matchmaker.step_reconfigure(request);
        self.pump_matchmaker(to)
    }

    /// Stop matchmaker `id` and boot it again from its registry on disk.
    pub fn restart_matchmaker(&mut self, id: MatchmakerId) {
        let registry = self.registries.get_mut(&id).expect("a matchmaker");
        registry.matchmaker = Matchmaker::new(&registry.config, &registry.disk, Limits::DEFAULT);
    }

    /// One tick of logical time on every running process, then
    /// [`Cluster::settle`]. A leader beats on a tick, and a follower or a
    /// replica that is behind asks the sender of the beat for the slots it
    /// misses.
    pub fn tick(&mut self) {
        for node in self.nodes.values_mut() {
            node.tick();
        }
        for replica in self.replicas.values_mut() {
            replica.tick();
        }
        for proxy in self.proxies.values_mut() {
            proxy.tick();
        }
        self.settle();
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

    /// Stop replica `id` and boot it again from its disk: the same path as
    /// a node, [`ReplicaNode::new`] on the storage that the stop left.
    pub fn restart_replica(&mut self, id: NodeId) {
        let replica = ReplicaNode::new(&self.disks[&id], Limits::DEFAULT);
        assert!(self.replicas.insert(id, replica).is_some(), "a replica");
    }

    /// Stop proxy leader `id`. It has no disk: each round it held is gone.
    pub fn crash_proxy(&mut self, id: ProxyId) {
        self.proxies.remove(&id).expect("the proxy was running");
    }

    /// Start proxy leader `id`, empty. A proxy leader is built from the
    /// description of the deployment, the same `Config` that a node has.
    pub fn restart_proxy(&mut self, id: ProxyId) {
        let proxy = ProxyLeader::new(id, &self.deployment, Limits::DEFAULT);
        assert!(
            self.proxies.insert(id, proxy).is_none(),
            "the proxy was down"
        );
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

    /// Propose one `Write` on the leader `id`, and do not deliver it:
    /// `text` as one record, at position `seq`, by the writer `writer`.
    /// Returns the slot. The caller chooses what the network does next.
    pub fn propose(&mut self, id: NodeId, writer: LeaderUuid, seq: u64, text: &str) -> Slot {
        let entry = Entry {
            leader: writer,
            seq: Seq(seq),
            records: vec![Value::from(text.as_bytes().to_vec())],
        };
        let ProposeResult::Accepted(slot) = self.node(id).propose(entry) else {
            panic!("node {id} is the leader");
        };
        slot
    }

    /// [`Cluster::propose`], then deliver until the cluster is quiet.
    pub fn write(&mut self, id: NodeId, writer: LeaderUuid, seq: u64, text: &str) -> Slot {
        let slot = self.propose(id, writer, seq, text);
        self.settle();
        slot
    }

    /// What node or replica `id` learned at `slot`: the verdict of the
    /// journal state machine, from the `committed` bucket.
    pub fn outcome(&self, id: NodeId, slot: Slot) -> Option<&Outcome> {
        self.learned
            .get(&id)?
            .iter()
            .find(|(s, _, _)| *s == slot)
            .map(|(_, _, outcome)| outcome)
    }

    /// The slots that node or replica `id` learned, in the order of its
    /// `committed` buckets.
    pub fn learned(&self, id: NodeId) -> Vec<Slot> {
        let learned = self.learned.get(&id).map_or_default(Vec::as_slice);
        learned.iter().map(|(slot, _, _)| *slot).collect()
    }

    /// The journal as node `id` serves it to a reader: each record from
    /// the first position that the journal still holds, as text.
    pub fn journal(&mut self, id: NodeId) -> Vec<String> {
        let first = self.node(id).replica().journal().view().first_seq;
        texts(id, self.node(id).read_log(first, usize::MAX, usize::MAX))
    }

    /// The journal as replica `id` serves it to a reader.
    pub fn replica_journal(&mut self, id: NodeId) -> Vec<String> {
        let first = self.replica(id).replica().journal().view().first_seq;
        texts(id, self.replica(id).read_log(first, usize::MAX, usize::MAX))
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

    /// How many peer messages node `id` sent and received since the last
    /// call. A fan-out to three processes counts three times.
    pub fn take_traffic(&mut self, id: NodeId) -> usize {
        self.traffic.remove(&id).unwrap_or_default()
    }

    /// How many peer messages of `kind` node `id` received since the
    /// cluster started.
    pub fn received(&self, id: NodeId, kind: &'static str) -> usize {
        self.received.get(&(id, kind)).copied().unwrap_or_default()
    }

    /// The journal that the matchmakers file this cluster under.
    pub fn journal_id(&mut self, id: NodeId) -> JournalId {
        self.node(id).config().journal.journal
    }
}

/// A party as the trace prints it: `3` for a node or a replica, `proxy 1`
/// for a proxy leader.
fn name(party: Party) -> String {
    match party {
        Party::Node(id) => id.to_string(),
        Party::Proxy(id) => format!("proxy {}", id.0),
    }
}

/// The records of a page, as text.
fn texts(id: NodeId, read: LogRead) -> Vec<String> {
    match read {
        LogRead::Page(page) => page
            .records
            .iter()
            .map(|record| String::from_utf8_lossy(record).into_owned())
            .collect(),
        other => panic!("{id} serves its own journal: {other:?}"),
    }
}
