//! **Leaderless reads: a row of acceptors answers, one replica serves, and
//! neither the leader nor a clock is involved.**
//!
//! Run it: `cargo run -p paros-core --example quorum_read`
//!
//! The lesson after `acceptor_grid`. Same six nodes under
//! [`QuorumSystem::Grid`] `{ rows: 2, cols: 3 }`, and this time the wiring
//! is not done by hand: each node is a [`ColocatedNode`], the deployment
//! that colocates the three roles, and the example only delivers its
//! messages. What is new is a **read** that never goes near the leader.
//!
//! # Where `acceptor_grid` left off
//!
//! Every slot's Phase 2 goes to one column, so an acceptor sees a third of
//! the writes. Reads did not scale the same way: a linearizable read went
//! to the leader, which confirmed it was still leader with a beat-ack
//! quorum (the read-index path) and served it. Every read loaded the
//! leader, and the leader was the bottleneck the grid had just taken the
//! writes off.
//!
//! # Paxos Quorum Reads (Compartmentalized Paxos §3.4)
//!
//! Ask a **row** of the grid — a Phase-1 quorum — one question: *what is
//! the highest slot you have voted in?* (the acceptor's **vote watermark**,
//! [`paros_core::acceptor::Acceptor::vote_watermark`]). Take the maximum,
//! `i`. Then have **any one replica** serve the read once it has applied
//! slot `i`. No leader, no beat, no ack, and — the point of this rung — no
//! clock: the paper's read leases assume clock synchrony, and paros refuses
//! exactly that.
//!
//! Why it is linearizable, in one sentence: a write acked before the read
//! began was chosen by a full **column**, every row meets every column in
//! one cell, so some member of the row voted that slot, and the maximum
//! watermark is at least it. The replica then waits until it has applied
//! that far. (§3.5 does the case analysis; the same argument, written on
//! [`paros_core::quorum_read`], is the module's contract.)
//!
//! # What the trace below shows
//!
//! ```text
//!          col 0   col 1   col 2
//! row 0  [  0   |   1   |   2  ]
//! row 1  [  3   |   4   |   5  ]
//! ```
//!
//! 1. Node 1 leads and streams three commands; every node applies slots
//!    0, 1 and 2.
//! 2. Node 1 proposes a fourth command. Slot 3 goes to column 0 =
//!    `{0, 3}`; node 3's copy lands, node 0's copy is **in flight**. The
//!    column is half full: nothing is chosen, nothing is acked.
//! 3. Node 4 (a follower in row 1) opens a quorum read. Its row `{3, 4, 5}`
//!    answers `3, 2, 2` — node 3 has voted the in-flight slot — so the read
//!    settles on `i = 3` and **waits**: node 4's replica has applied up to
//!    slot 2. This is the safety trade: a watermark raised by a vote that
//!    has not decided costs the reader a wait, never a stale answer.
//! 4. Node 2 (row 0) opens a quorum read at the same moment. Its row
//!    `{0, 1, 2}` answers `2, 2, 2` — nobody there has voted slot 3 — so
//!    that read is served **now**, at slot 2. Both reads are linearizable:
//!    slot 3 is neither chosen nor acked, so a read that does not see it
//!    is as correct as one that will.
//! 5. Node 0's copy of the `Accept` arrives; column 0 is whole; slot 3 is
//!    chosen and committed everywhere; node 4's replica applies it and the
//!    waiting read is served at slot 3 — showing the fourth value.
//!
//! Throughout, the leader broadcast no beat for either read, opened no
//! read-index round, and could have been dead for step 3 onward.
//!
//! Further reading: Whittaker et al., *Scaling Replicated State Machines
//! with Compartmentalization* (2021), §3.4–3.5.

use std::collections::BTreeMap;

use paros_core::{
    Ballot, ClientId, ClientSeq, ColocatedNode, Command, Config, HardState, Message, NodeId,
    QuorumSystem, ReadState, Slot, Storage, Value,
};

/// Two rows of three: rows `{0, 1, 2}` and `{3, 4, 5}`, columns `{0, 3}`,
/// `{1, 4}` and `{2, 5}`.
const GRID: QuorumSystem = QuorumSystem::Grid { rows: 2, cols: 3 };

/// An empty store: every node boots fresh, as a first boot does.
struct FreshStore {
    config: Config,
}

impl Storage for FreshStore {
    fn initial_state(&self) -> (HardState, Config) {
        (HardState::default(), self.config.clone())
    }

    fn accepted(&self, _slot: Slot) -> Option<(Ballot, Command)> {
        None
    }

    fn first_slot(&self) -> Slot {
        Slot(0)
    }

    fn last_slot(&self) -> Slot {
        Slot(0)
    }
}

fn fresh(id: u64) -> ColocatedNode {
    let config = Config {
        id: NodeId(id),
        peers: (0..6).map(NodeId).collect(),
        quorum_system: GRID,
        ..Config::default()
    };
    ColocatedNode::new(&FreshStore { config })
}

/// A large `CheckQuorum` window: this example steps messages by hand and
/// never pumps ack traffic every tick.
const NO_CHECK_QUORUM: u64 = 1_000_000;

fn command(text: &str, seq: u64) -> Value {
    let _ = seq;
    Value(text.as_bytes().to_vec())
}

fn show(command: &Command) -> String {
    match command {
        Command::User(entry) => format!("{:?}", String::from_utf8_lossy(&entry.value.0)),
        Command::Control(control) => format!("{control:?}"),
    }
}

fn kind(m: &Message) -> &'static str {
    match m {
        Message::Prepare { .. } => "Prepare",
        Message::Promise { .. } => "Promise",
        Message::Accept { .. } => "Accept",
        Message::Accepted { .. } => "Accepted",
        Message::Commit { .. } => "Commit",
        Message::Heartbeat { .. } => "Heartbeat",
        Message::HeartbeatAck { .. } => "HeartbeatAck",
        Message::PreRead { .. } => "PreRead",
        Message::PreReadAck { .. } => "PreReadAck",
        _ => "other",
    }
}

/// The cluster: six nodes and the messages in flight between them.
struct Cluster {
    nodes: Vec<ColocatedNode>,
    /// Messages a node queued, addressed and not yet delivered.
    wire: Vec<(NodeId, Message)>,
    /// Read states each node surfaced, by node.
    served: BTreeMap<NodeId, Vec<ReadState>>,
    /// How many messages of each kind the leader sent — the trace's tally.
    sent_by_leader: BTreeMap<&'static str, usize>,
    leader: NodeId,
}

impl Cluster {
    fn node(&mut self, id: NodeId) -> &mut ColocatedNode {
        let i = usize::try_from(id.0).expect("small id");
        &mut self.nodes[i]
    }

    /// Drain `id`'s batch onto the wire, honouring the `Ready` contract:
    /// its writes are "persisted" (this example has no disk), its messages
    /// are resolved to addresses, its read states are collected, and the
    /// batch is acknowledged.
    fn drain(&mut self, id: NodeId) {
        let pool: Vec<NodeId> = (0..6).map(NodeId).collect();
        let leader = self.leader;
        let node = self.node(id);
        let ready = node.ready();
        let mut out = Vec::new();
        for (audience, m) in ready.messages() {
            for to in audience.resolve(&pool, id) {
                out.push((to, m.clone()));
            }
        }
        let states = ready.read_states().to_vec();
        ready.advance();
        node.advance_recovery();
        if id == leader {
            for (_, m) in &out {
                *self.sent_by_leader.entry(kind(m)).or_default() += 1;
            }
        }
        self.wire.extend(out);
        self.served.entry(id).or_default().extend(states);
    }

    /// Deliver everything in flight for which `keep` holds, to quiescence;
    /// what `keep` refuses stays in flight.
    fn deliver(&mut self, keep: impl Fn(NodeId, &Message) -> bool) {
        loop {
            let Some(position) = self.wire.iter().position(|(to, m)| keep(*to, m)) else {
                return;
            };
            let (to, m) = self.wire.remove(position);
            self.node(to).step(m);
            self.drain(to);
        }
    }

    fn deliver_all(&mut self) {
        self.deliver(|_, _| true);
    }

    fn take_served(&mut self, id: NodeId) -> Vec<ReadState> {
        self.served.remove(&id).unwrap_or_default()
    }

    fn watermarks(&self, row: &[u64]) -> String {
        let each: Vec<String> = row
            .iter()
            .map(|id| {
                let i = usize::try_from(*id).expect("small id");
                let w = self.nodes[i].acceptor().vote_watermark();
                format!(
                    "node {id} → {}",
                    w.map_or("nothing voted".to_string(), |s| format!("slot {}", s.0))
                )
            })
            .collect();
        each.join(", ")
    }

    fn applied(&self, id: u64) -> String {
        let i = usize::try_from(id).expect("small id");
        self.nodes[i]
            .replica()
            .chosen_index()
            .map_or("nothing".to_string(), |s| format!("slot {}", s.0))
    }
}

fn main() {
    println!("== quorum reads: a row answers, one replica serves, no leader, no clock ==\n");
    let mut cluster = Cluster {
        nodes: (0..6).map(fresh).collect(),
        wire: Vec::new(),
        served: BTreeMap::new(),
        sent_by_leader: BTreeMap::new(),
        leader: NodeId(1),
    };

    let leader = cluster.leader;
    elect_and_stream(&mut cluster, leader);
    half_a_column(&mut cluster, leader);
    a_read_that_waits(&mut cluster);
    a_read_served_now(&mut cluster);
    the_column_completes(&mut cluster);
    the_leaders_part(&mut cluster, leader);
}

/// Step: see the module doc.
fn elect_and_stream(cluster: &mut Cluster, leader: NodeId) {
    // ---- 1. node 1 leads and streams three commands ------------------------
    println!("-- 1. node 1 campaigns and streams three commands");
    cluster.node(leader).set_election_timeout(1);
    cluster.node(leader).tick();
    cluster.drain(leader);
    cluster.deliver_all();
    assert!(cluster.node(leader).is_leader(), "node 1 wins its election");
    cluster.node(leader).set_election_timeout(NO_CHECK_QUORUM);
    cluster.node(leader).tick(); // one beat, so every follower adopts the leader
    cluster.drain(leader);
    cluster.deliver_all();
    for (seq, text) in [(1, "alpha"), (2, "bravo"), (3, "charlie")] {
        let _ = cluster
            .node(leader)
            .propose(ClientId(7), ClientSeq(seq), command(text, seq));
        cluster.drain(leader);
        cluster.deliver_all();
    }
    for id in 0..6 {
        assert_eq!(
            cluster.applied(id),
            "slot 2",
            "node {id} applied slots 0..=2"
        );
    }
    println!("   every node has applied slots 0, 1 and 2");
    cluster.sent_by_leader.clear();
}

/// Step: see the module doc.
fn half_a_column(cluster: &mut Cluster, leader: NodeId) {
    // ---- 2. a fourth command, half-way through its column -------------------
    println!("\n-- 2. node 1 proposes \"delta\" for slot 3 → column 0 = {{0, 3}}");
    let _ = cluster
        .node(leader)
        .propose(ClientId(7), ClientSeq(4), command("delta", 4));
    cluster.drain(leader);
    // Node 3's copy of the Accept lands; node 0's stays in flight.
    cluster.deliver(|to, m| !(to == NodeId(0) && matches!(m, Message::Accept { .. })));
    assert!(
        cluster.node(leader).replica().chosen_at(Slot(3)).is_none(),
        "half a column decides nothing"
    );
    println!("   node 3 accepted slot 3; node 0's copy is in flight; nothing is chosen");
    println!("   row 1 watermarks: {}", cluster.watermarks(&[3, 4, 5]));
    println!("   row 0 watermarks: {}", cluster.watermarks(&[0, 1, 2]));
}

/// Step: see the module doc.
fn a_read_that_waits(cluster: &mut Cluster) {
    // ---- 3. a read from row 1 settles past the prefix and waits ------------
    println!("\n-- 3. node 4 opens a quorum read (ctx 1 → row 1 = {{3, 4, 5}})");
    cluster.node(NodeId(4)).quorum_read(1);
    cluster.drain(NodeId(4));
    cluster.deliver(|to, m| !(to == NodeId(0) && matches!(m, Message::Accept { .. })));
    let pending = cluster.node(NodeId(4)).quorum_reads().pending().to_vec();
    assert_eq!(pending.len(), 1, "the read is still open");
    assert_eq!(
        pending[0].confirmed_index(),
        Some(Some(Slot(3))),
        "the row answered whole and settled on node 3's watermark"
    );
    assert!(
        cluster.take_served(NodeId(4)).is_empty(),
        "node 4 has applied only up to slot 2: the read waits"
    );
    println!(
        "   the row answered; max watermark = slot 3; node 4 has applied {} → the read WAITS",
        cluster.applied(4)
    );
}

/// Step: see the module doc.
fn a_read_served_now(cluster: &mut Cluster) {
    // ---- 4. a read from row 0 is served now -------------------------------
    println!("\n-- 4. node 2 opens a quorum read (ctx 2 → row 0 = {{0, 1, 2}})");
    cluster.node(NodeId(2)).quorum_read(2);
    cluster.drain(NodeId(2));
    cluster.deliver(|to, m| !(to == NodeId(0) && matches!(m, Message::Accept { .. })));
    let served = cluster.take_served(NodeId(2));
    assert_eq!(
        served,
        vec![ReadState {
            ctx: 2,
            index: Some(Slot(2)),
        }],
        "row 0 has not voted slot 3: served at slot 2, now"
    );
    println!("   the row answered; max watermark = slot 2; served NOW at slot 2");
    println!("   (both reads are linearizable: slot 3 is neither chosen nor acked)");
}

/// Step: see the module doc.
fn the_column_completes(cluster: &mut Cluster) {
    // ---- 5. the column completes; the waiting read is served -------------
    println!("\n-- 5. node 0's copy of the Accept arrives: column 0 is whole");
    cluster.deliver_all();
    for id in 0..6 {
        assert_eq!(cluster.applied(id), "slot 3", "node {id} applied slot 3");
    }
    let served = cluster.take_served(NodeId(4));
    assert_eq!(
        served,
        vec![ReadState {
            ctx: 1,
            index: Some(Slot(3)),
        }],
        "the waiting read is served once the replica covers slot 3"
    );
    let value = cluster
        .node(NodeId(4))
        .replica()
        .chosen_at(Slot(3))
        .map(show)
        .expect("slot 3 is chosen at node 4");
    println!(
        "   slot 3 = {value} chosen and applied everywhere; node 4's read is served at slot 3"
    );
}

/// Step: see the module doc.
fn the_leaders_part(cluster: &mut Cluster, leader: NodeId) {
    // ---- the leader's part in the two reads: none ---------------------------
    let beats = cluster
        .sent_by_leader
        .get("Heartbeat")
        .copied()
        .unwrap_or(0);
    assert_eq!(
        beats, 0,
        "no beat was broadcast for either read (the election's beat came before)"
    );
    assert!(
        cluster.node(leader).proposer().read_rounds().is_empty(),
        "no read-index round was opened"
    );
    println!(
        "\n   the leader sent {} messages after step 1, none of them a beat or a read round: {:?}",
        cluster.sent_by_leader.values().sum::<usize>(),
        cluster.sent_by_leader
    );
    println!("\n== ok ==");
}
