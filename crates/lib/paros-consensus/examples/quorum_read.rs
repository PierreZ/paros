//! **Leaderless reads: a row of acceptors answers and the reader serves. The
//! leader does no work as a leader, and no clock takes part.**
//!
//! Run it: `cargo run -p paros-consensus --example quorum_read`
//!
//! This example drives [`ColocatedNode`] with the toy cluster of
//! `examples/common/cluster.rs`. Read `quickstart` first, and `acceptor_grid`
//! for the grid.
//!
//! # The problem
//!
//! A read is linearizable when it shows each write that was acknowledged
//! before the read began. The usual way is to ask the leader, and the leader
//! must first prove that it still leads. That puts each read on the one
//! process that already orders each write. A lease removes the proof, but a
//! lease trusts the clocks.
//!
//! # The technique (Compartmentalized Paxos, section 3.4)
//!
//! Any node can serve a read. The node asks a **row** of the grid one
//! question: *what is the highest slot that you voted in?* The answer is
//! the **vote watermark** of the acceptor. The reader takes the maximum,
//! `i`, and serves the read when it has learned each slot up to `i`.
//!
//! Why it is correct: a write that was acknowledged was accepted by a full
//! **column**. Each row has one acceptor of each column. Thus one member of
//! the row voted in the slot of that write, and the maximum is at or past
//! it.
//!
//! # The driver's part
//!
//! - `node.quorum_read(ctx)` opens a read, on any node. `ctx` is a token of
//!   the driver. `quorum_read_in(ctx, Some(row))` names the row.
//! - The node sends `PreRead` and gets `PreReadAck` through the usual
//!   `ready.messages()` and `step`.
//! - `ready.read_states()` gives a [`ReadState`] when the read can be
//!   served: the token, and the slot `index`. The driver then reads the
//!   journal of this node (`read_log`), which holds each slot up to `index`.
//!
//! A read that cannot complete gives nothing, and the core drops it after a
//! number of ticks. The driver owns the timeout of its client.
//!
//! # What the trace shows
//!
//! ```text
//!          col 0   col 1   col 2
//! row 0  [  0   |   1   |   2  ]
//! row 1  [  3   |   4   |   5  ]
//! ```
//!
//! 1. Node 5 leads, and three records are written.
//! 2. A fourth record goes to column 1. Node 4 accepts it. The `Accept` to
//!    node 1 is still in flight, so the record is not chosen.
//! 3. Node 3 reads through row 1. Node 4 voted in the new slot, so the read
//!    waits: a vote that did not decide yet costs the reader a wait, never
//!    a stale answer.
//! 4. Node 2 reads through row 0 at the same time. No member of row 0 voted
//!    in the new slot, so the read is served at once, with three records.
//!    That is correct too: the fourth write was not acknowledged.
//! 5. The `Accept` arrives. The slot is chosen, and the read of node 3 is
//!    served with four records.
//! 6. The leader crashes. Node 0 still serves a read through row 0.
//!
//! Further reading: Whittaker et al., *Scaling Replicated State Machines
//! with Compartmentalization* (2021), sections 3.4 and 3.5, and
//! `specs/tla/reads/` for the model.

mod common;

use paros_consensus::contract::Party;
use paros_consensus::journal::{Control, JournalIdentifier, LeaderUuid};
use paros_consensus::membership::QuorumSystem;
use paros_consensus::quorum_read::{ReadOpen, ReadState};
use paros_consensus::{Config, Message, NodeId, Slot};

use common::cluster::Cluster;

const N0: NodeId = NodeId(0);
const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);
const N5: NodeId = NodeId(5);

const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// The grid is data of the configuration: two rows of three.
fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers((0..6).map(NodeId));
    config.quorum_system = QuorumSystem::grid(2, 3);
    config
}

/// The `Accept` that node 1 has not received yet.
fn in_flight(to: Party, message: &Message) -> bool {
    to == Party::Node(N1) && matches!(message, Message::Accept { .. })
}

fn main() {
    let mut cluster = Cluster::new((0..6).map(NodeId).map(config));

    println!("== 1. node 5 leads, and three records are written");
    cluster.elect(N5);
    cluster.control(
        N5,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );
    for (seq, text) in [(0, "alpha"), (1, "bravo"), (2, "charlie")] {
        cluster.write(N5, WRITER, seq, text);
    }
    assert_eq!(cluster.journal(N3), ["alpha", "bravo", "charlie"]);
    cluster.take_sent();

    let slot = half_a_column(&mut cluster);
    a_read_that_waits(&mut cluster, slot);
    a_read_served_now(&mut cluster, slot);
    the_column_completes(&mut cluster, slot);
    without_the_leader(&mut cluster, slot);
}

/// Part 2: a write that only one acceptor of its column holds.
fn half_a_column(cluster: &mut Cluster) -> Slot {
    println!("\n== 2. \"delta\" goes to column 1, and reaches node 4 only");
    let slot = cluster.propose(N5, WRITER, 3, "delta");
    cluster.settle_holding(in_flight);
    assert!(cluster.node(N5).chosen_at(slot).is_none());
    assert!(cluster.node(N4).acceptor().record(slot).is_some());
    assert!(cluster.node(N1).acceptor().record(slot).is_none());
    println!("   slot {slot}: node 4 accepted, node 1 did not yet. Nothing is chosen");
    slot
}

/// Part 3: a row that saw the vote makes the reader wait.
fn a_read_that_waits(cluster: &mut Cluster, slot: Slot) {
    println!("\n== 3. node 3 reads through row 1 = {{3, 4, 5}}");
    assert_eq!(
        cluster.node(N3).quorum_read_in(1, Some(1)),
        ReadOpen::Opened
    );
    cluster.trace = true;
    cluster.settle_holding(in_flight);
    cluster.trace = false;

    let pending = cluster.node(N3).quorum_reads().pending();
    assert_eq!(pending.len(), 1, "the read is open");
    assert_eq!(
        pending[0].confirmed_index(),
        Some(Some(slot)),
        "the row answered, and its maximum is the vote of node 4"
    );
    assert_eq!(cluster.take_reads(N3), []);
    println!("   the maximum watermark is slot {slot}. Node 3 did not learn it: the read waits");
}

/// Part 4: a row that did not see the vote serves at once.
fn a_read_served_now(cluster: &mut Cluster, slot: Slot) {
    println!("\n== 4. node 2 reads through row 0 = {{0, 1, 2}}");
    assert_eq!(
        cluster.node(N2).quorum_read_in(2, Some(0)),
        ReadOpen::Opened
    );
    cluster.settle_holding(in_flight);

    let served = cluster.take_reads(N2);
    let [ReadState { ctx: 2, index }] = served[..] else {
        panic!("the read of node 2 is served: {served:?}");
    };
    assert!(index < Some(slot), "no member of row 0 voted in the slot");
    // The driver now serves the read from the journal of node 2.
    assert_eq!(cluster.journal(N2), ["alpha", "bravo", "charlie"]);
    println!("   no member voted in slot {slot}: served now, with three records");
    println!("   \"delta\" was not acknowledged, so a read without it is correct");
}

/// Part 5: the waiting read is served when its node learns the slot.
fn the_column_completes(cluster: &mut Cluster, slot: Slot) {
    println!("\n== 5. the Accept reaches node 1: column 1 is complete");
    cluster.settle();
    assert_eq!(
        cluster.take_reads(N3),
        [ReadState {
            ctx: 1,
            index: Some(slot),
        }]
    );
    assert_eq!(cluster.journal(N3), ["alpha", "bravo", "charlie", "delta"]);
    println!("   slot {slot} is chosen. The read of node 3 is served, with four records");

    // The leader opened no read and no round for the two reads: it answered
    // one `PreRead` as an acceptor of row 1, as node 4 did.
    let sent = cluster.take_sent();
    assert_eq!(
        sent.get("PreRead"),
        Some(&4),
        "two reads, each to the two other members of its row"
    );
    assert!(cluster.node(N5).quorum_reads().is_empty());
    println!("   messages since part 1: {sent:?}");
}

/// Part 6: a read needs a row, not the leader.
fn without_the_leader(cluster: &mut Cluster, slot: Slot) {
    println!("\n== 6. the leader crashes, and node 0 reads through row 0");
    cluster.crash(N5);
    assert_eq!(
        cluster.node(N0).quorum_read_in(3, Some(0)),
        ReadOpen::Opened
    );
    cluster.settle();
    assert_eq!(
        cluster.take_reads(N0),
        [ReadState {
            ctx: 3,
            index: Some(slot),
        }]
    );
    assert_eq!(cluster.journal(N0), ["alpha", "bravo", "charlie", "delta"]);
    println!("   served at slot {slot}, with four records and no leader");
}
