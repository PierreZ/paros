//! **Leader handoff: move the leadership without an election.**
//!
//! Run it: `cargo run -p paros-consensus --example leader_handoff`
//!
//! This example drives [`ColocatedNode`] with the toy cluster of
//! `examples/common/cluster.rs`. Read `quickstart` first.
//!
//! # The problem
//!
//! An election replaces a leader that is gone. It costs a Phase 1: a higher
//! ballot, a `Prepare` to each acceptor, a quorum of `Promise`s, and the
//! recovery of each slot that the promises report. When the leader is alive
//! and only wants to *move* (before a planned restart, or to balance load),
//! that work finds nothing new. The leader already knows all of it.
//!
//! # The technique (`DPaxos`, "Leader Handoff")
//!
//! The leader gives its authority to one successor in one message,
//! `Relinquish`. The authority is the ballot, the next free slot, and the
//! slots that are still open below it. The successor continues **at the same
//! ballot**: no `Prepare`, no `Promise`, no recovery.
//!
//! Three rules keep one proposer for each ballot:
//!
//! - **The leader stops first.** [`ColocatedNode::relinquish_to`] demotes the
//!   node in the same call that queues the message. The message cannot
//!   exist while its sender still leads.
//! - **The successor is named in the message.** A copy that reaches another
//!   node is refused.
//! - **One hop.** Only the node that won the ballot in an election may hand
//!   it on. A successor that wants to move the leadership again resigns, and
//!   an ordinary election follows.
//!
//! A handoff that fails costs availability, never safety. If the message is
//! lost, the cluster has no leader until an election, and an election is always
//! possible.
//!
//! # What the trace shows
//!
//! 1. Node 1 wins an election and writes two records.
//! 2. Node 1 hands the leadership to node 2. No `Prepare` is sent; node 2
//!    leads at the ballot that node 1 won, and writes.
//! 3. Node 2 may not hand the same ballot on: one hop.
//! 4. Node 2 wins a ballot of its own and hands it to node 3, but the
//!    message is lost. The cluster has no leader. Node 1 runs an ordinary
//!    election, and nothing that was written is lost.
//!
//! Further reading: Nawab, Agrawal & El Abbadi, *`DPaxos`: Managing Data
//! Closer to Users for Low-Latency and Mobile Applications* (2018), and
//! `specs/tla/leader-handoff/` for the model.

mod common;

use paros_consensus::journal::{Control, JournalIdentifier, LeaderUuid};
use paros_consensus::node::LeadershipOrigin;
use paros_consensus::{Config, Message, NodeId};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

fn main() {
    let mut cluster = Cluster::new(
        [N1, N2, N3].map(|id| Config::new(id, JournalIdentifier::UNSET).with_peers([N1, N2, N3])),
    );

    println!("== 1. node 1 wins an election and writes");
    cluster.elect(N1);
    let election = cluster.take_sent();
    println!(
        "   the election cost {} Prepare and {} Promise",
        election["Prepare"], election["Promise"]
    );
    cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );
    cluster.write(N1, WRITER, 0, "alpha");
    cluster.write(N1, WRITER, 1, "bravo");
    let ballot = cluster.node(N1).ballot();
    println!("   node 1 leads at ballot {ballot}");

    handoff(&mut cluster);
    one_hop(&mut cluster);
    a_lost_handoff(&mut cluster);
}

/// Part 2: the handoff that works.
fn handoff(cluster: &mut Cluster) {
    println!("\n== 2. node 1 hands the leadership to node 2");
    let ballot = cluster.node(N1).ballot();
    assert!(cluster.node(N1).can_relinquish());
    cluster.take_sent();

    let receipt = cluster
        .node(N1)
        .relinquish_to(N2)
        .expect("a settled, elected leader may relinquish");
    // The same call demoted node 1: the message and the leadership never
    // exist together.
    assert!(!cluster.node(N1).is_leader());
    println!(
        "   relinquish_to(2): ballot {}, next free slot {}",
        receipt.ballot, receipt.next_slot
    );

    cluster.trace = true;
    cluster.settle();
    cluster.trace = false;

    let successor = cluster.node(N2);
    assert!(successor.is_leader());
    assert_eq!(
        successor.ballot(),
        ballot,
        "the same ballot, not a higher one"
    );
    assert_eq!(
        successor.leadership_origin(),
        LeadershipOrigin::Handoff { from: N1 }
    );
    let sent = cluster.take_sent();
    assert!(!sent.contains_key("Prepare"), "a handoff runs no Phase 1");
    println!("   node 2 leads at ballot {ballot}: 1 Relinquish, 0 Prepare");

    cluster.tick();
    cluster.write(N2, WRITER, 2, "charlie");
    for id in [N1, N2, N3] {
        assert_eq!(cluster.journal(id), ["alpha", "bravo", "charlie"]);
    }
    println!("   node 2 writes \"charlie\" at the ballot that node 1 won");
}

/// Part 3: an inherited ballot stays where it is.
fn one_hop(cluster: &mut Cluster) {
    println!("\n== 3. node 2 may not hand the same ballot on");
    assert!(!cluster.node(N2).can_relinquish());
    assert!(cluster.node(N2).relinquish_to(N3).is_none());
    assert!(cluster.node(N2).is_leader(), "a refusal changes nothing");
    println!("   relinquish_to(3) is refused: only the node that won a ballot hands it on");
}

/// Part 4: a lost handoff costs availability, and an election repairs it.
fn a_lost_handoff(cluster: &mut Cluster) {
    println!("\n== 4. a handoff whose message is lost");
    // Node 2 resigns its inherited ballot and wins one of its own.
    cluster.node(N2).step_down();
    cluster.elect(N2);
    let ballot = cluster.node(N2).ballot();
    cluster.write(N2, WRITER, 3, "delta");
    println!("   node 2 wins ballot {ballot} and writes \"delta\"");

    let receipt = cluster.node(N2).relinquish_to(N3);
    assert!(receipt.is_some());
    cluster.settle_where(|_, message| !matches!(message, Message::Relinquish { .. }));
    for id in [N1, N2, N3] {
        assert!(!cluster.node(id).is_leader());
    }
    println!("   the Relinquish is lost: node 2 stepped down, node 3 heard nothing");
    println!("   no node leads. Nothing is lost, and nothing can be written");

    // Phase 1 is always the way back.
    cluster.elect(N1);
    let recovered = cluster.node(N1).ballot();
    assert!(recovered > ballot);
    cluster.write(N1, WRITER, 4, "echo");
    cluster.tick();
    for id in [N1, N2, N3] {
        assert_eq!(
            cluster.journal(id),
            ["alpha", "bravo", "charlie", "delta", "echo"]
        );
    }
    println!("   node 1 wins an election at ballot {recovered}, and the journal goes on");
}
