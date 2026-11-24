//! Three nodes of plain Multi-Paxos through the facade, with `paros::` paths
//! only.
//!
//! A program depends on `paros` alone. This example is the smallest driver
//! that such a program writes: three `ColocatedNode`s, one in-memory disk
//! for each, and a wire that the example controls. The loop obeys the one
//! rule of the contract, **persist before send**: each batch of `ready()` is
//! written to the disk before its messages leave.
//!
//! Run it with `cargo run -p paros --example consensus`. It asserts what it
//! shows, so CI runs it as a test.
//!
//! 1. Node 1 campaigns and wins: Phase 1, one time for the whole log.
//! 2. A client becomes the writer of the journal with `SetLeader`.
//! 3. The leader appends two records; each node serves them.
//! 4. Node 3 crashes, restarts from its disk, and catches up.

use std::collections::{BTreeMap, VecDeque};

use paros::consensus::contract::MemStorage;
use paros::consensus::journal::{Control, Entry, JournalIdentifier, LeaderUuid, Outcome, Seq};
use paros::consensus::node::ProposeResult;
use paros::consensus::replica::LogRead;
use paros::consensus::{ColocatedNode, Config, Limits, Message, NodeId, Slot, Value};

const NODES: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];
const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// The nodes, their disks, the wire and what each node learned.
struct Cluster {
    disks: BTreeMap<NodeId, MemStorage>,
    nodes: BTreeMap<NodeId, ColocatedNode>,
    wire: VecDeque<(NodeId, Message)>,
    learned: BTreeMap<NodeId, BTreeMap<Slot, Outcome>>,
}

impl Cluster {
    fn new() -> Self {
        let mut cluster = Self {
            disks: BTreeMap::new(),
            nodes: BTreeMap::new(),
            wire: VecDeque::new(),
            learned: BTreeMap::new(),
        };
        for id in NODES {
            let config = Config::new(id, JournalIdentifier::UNSET).with_peers(NODES);
            cluster.disks.insert(id, MemStorage::new(config));
            cluster.restart(id);
        }
        cluster
    }

    /// Boot node `id` from what its disk holds.
    fn restart(&mut self, id: NodeId) {
        let node = ColocatedNode::new(&self.disks[&id], Limits::DEFAULT);
        self.nodes.insert(id, node);
    }

    fn node(&mut self, id: NodeId) -> &mut ColocatedNode {
        self.nodes.get_mut(&id).expect("the node runs")
    }

    /// The driver loop of one node: persist, then send, then learn, then
    /// advance, until the node has nothing more to say.
    fn pump(&mut self, id: NodeId) {
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        let disk = self.disks.get_mut(&id).expect("each node has a disk");
        loop {
            let pool = node.pool().to_vec();
            let ready = node.ready();
            if ready.is_empty() {
                ready.advance();
                return;
            }
            disk.apply(ready.writes());
            for (audience, message) in ready.messages() {
                for to in audience.resolve(&pool, id) {
                    self.wire.push_back((to, message.clone()));
                }
            }
            let learned = self.learned.entry(id).or_default();
            for (slot, _, _, outcome) in ready.committed() {
                learned.insert(*slot, outcome.clone());
            }
            ready.advance();
            node.advance_recovery();
        }
    }

    /// Deliver until the wire is empty. A message to a node that is down is
    /// lost.
    fn settle(&mut self) {
        for id in NODES {
            self.pump(id);
        }
        while let Some((to, message)) = self.wire.pop_front() {
            if let Some(node) = self.nodes.get_mut(&to) {
                node.step(message);
                self.pump(to);
            }
        }
    }

    /// One beat of the clock on every running node.
    fn tick(&mut self) {
        for id in NODES {
            if let Some(node) = self.nodes.get_mut(&id) {
                node.tick();
            }
        }
        self.settle();
    }

    fn propose(&mut self, result: ProposeResult) -> Slot {
        let ProposeResult::Accepted(slot) = result else {
            panic!("the leader admits the proposal: {result:?}");
        };
        self.settle();
        slot
    }

    /// The records that node `id` serves, as text.
    fn journal(&mut self, id: NodeId) -> Vec<String> {
        let first = self.node(id).replica().journal().view().first_seq;
        match self.node(id).read_log(first, usize::MAX, usize::MAX) {
            LogRead::Page(page) => page
                .records
                .iter()
                .map(|record| String::from_utf8_lossy(record).into_owned())
                .collect(),
            other => panic!("node {id} serves its journal: {other:?}"),
        }
    }
}

fn main() {
    let [n1, n2, n3] = NODES;
    let mut cluster = Cluster::new();

    println!("== 1. node 1 campaigns");
    cluster.node(n1).campaign();
    cluster.settle();
    assert!(cluster.node(n1).is_leader());
    cluster.tick();
    assert_eq!(cluster.node(n2).leader(), Some(n1));
    println!("   node 1 leads at ballot {}", cluster.node(n1).ballot());

    println!("== 2. a client becomes the writer");
    let result = cluster.node(n1).propose_control(Control::SetLeader {
        new: WRITER,
        old: None,
    });
    let slot = cluster.propose(result);
    assert!(matches!(
        cluster.learned[&n1].get(&slot),
        Some(Outcome::Leader(view)) if view.leader == Some(WRITER)
    ));

    println!("== 3. two records");
    for (seq, text) in [(0, "alpha"), (1, "bravo")] {
        let entry = Entry {
            leader: WRITER,
            seq: Seq(seq),
            records: vec![Value::from(text.as_bytes().to_vec())],
        };
        let result = cluster.node(n1).propose(entry);
        let slot = cluster.propose(result);
        for id in NODES {
            assert_eq!(
                cluster.learned[&id].get(&slot),
                Some(&Outcome::Accepted {
                    seq: Seq(seq),
                    count: 1
                }),
                "node {id} judged slot {slot} as the leader did"
            );
        }
    }
    for id in NODES {
        assert_eq!(cluster.journal(id), ["alpha", "bravo"]);
    }
    println!("   each node serves [\"alpha\", \"bravo\"]");

    println!("== 4. node 3 crashes and comes back");
    cluster.nodes.remove(&n3);
    let entry = Entry {
        leader: WRITER,
        seq: Seq(2),
        records: vec![Value::from(b"charlie".to_vec())],
    };
    let result = cluster.node(n1).propose(entry);
    cluster.propose(result);
    cluster.restart(n3);
    assert_eq!(cluster.journal(n3), ["alpha", "bravo"]);
    cluster.tick();
    assert_eq!(cluster.journal(n3), ["alpha", "bravo", "charlie"]);
    println!("   node 3 caught up from the beat of the leader");
}
