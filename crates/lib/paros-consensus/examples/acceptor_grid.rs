//! **An acceptor grid: one column decides, one acceptor of each column
//! elects, and each acceptor gets a part of the commands.**
//!
//! Run it: `cargo run -p paros-consensus --example acceptor_grid`
//!
//! This example drives [`ColocatedNode`] with the toy cluster of
//! `examples/common/cluster.rs`. Read `quickstart` and `multi_paxos` first.
//!
//! # The one rule on quorums
//!
//! Paxos is safe when each Phase-1 quorum shares an acceptor with each
//! Phase-2 quorum: the promises of the next leader then include an acceptor
//! of each decision. A majority gives that by a count. A grid gives it by a
//! shape. Put six acceptors in two rows of three:
//!
//! ```text
//!          col 0   col 1   col 2
//! row 0  [  1   |   2   |   3  ]
//! row 1  [  4   |   5   |   6  ]
//! ```
//!
//! A **full column** is a Phase-2 quorum. A set with **one acceptor of each
//! column** is a Phase-1 quorum, and a row is such a set. The two always
//! share an acceptor.
//!
//! # What the grid gives, and what it costs
//!
//! The leader sends the `Accept` of a slot to one column only, the column
//! `slot % 3`. Each acceptor thus gets one command out of three, and more
//! columns mean more throughput.
//!
//! The cost is that the count of failures no longer tells you what
//! survives. One acceptor down stops the slots of its column. A whole
//! column down stops each election, with four nodes of six up.
//!
//! # The driver's part
//!
//! The grid is data: `Config::quorum_system = QuorumSystem::grid(2, 3)`,
//! and nothing else changes in the driver loop. The core names the column
//! in the audience of each `Accept`, and `Audience::resolve` gives its
//! nodes.
//!
//! One policy is new. A round whose column cannot answer stays open, and
//! each later slot waits behind it. The driver moves the round to a
//! column that answers with `ColocatedNode::move_round(slot, column)`.
//!
//! # What the trace shows
//!
//! 1. The quorum questions, asked to the configuration directly.
//! 2. Node 1 leads and six records are written. Each acceptor receives two
//!    `Accept`s, not six.
//! 3. Node 5 crashes. The slot of column 1 stays open and the next slot
//!    waits behind it. The driver moves the round to column 0.
//! 4. Node 2 crashes too: column 1 is down. Node 4 gets a promise from each
//!    running node and does not win. Node 5 restarts, and node 4 wins.
//!
//! The journal of this example is a multi-writer journal
//! ([`WriterMode::Multi`]), so each slot holds one record of the example.
//!
//! Further reading: Whittaker et al., *Scaling Replicated State Machines
//! with Compartmentalization* (2021), section 3.2; Howard, Malkhi &
//! Spiegelman, *Flexible Paxos* (2016), section 4; and `specs/tla/grid/`
//! for the model.

mod common;

use std::collections::BTreeSet;

use paros_consensus::journal::{JournalIdentifier, LeaderUuid, WriterMode};
use paros_consensus::membership::{AcceptorConfig, QuorumSystem};
use paros_consensus::{Config, NodeId};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);
const N5: NodeId = NodeId(5);
const N6: NodeId = NodeId(6);
const ALL: [NodeId; 6] = [N1, N2, N3, N4, N5, N6];

/// Two rows of three: rows `{1, 2, 3}` and `{4, 5, 6}`, columns `{1, 4}`,
/// `{2, 5}` and `{3, 6}`.
const GRID: QuorumSystem = QuorumSystem::grid(2, 3);

fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers(ALL);
    config.quorum_system = GRID;
    config.writer_mode = WriterMode::Multi;
    config
}

fn main() {
    quorums();
    let mut cluster = Cluster::new(ALL.map(config));
    one_column_for_each_slot(&mut cluster);
    one_acceptor_down(&mut cluster);
    one_column_down(&mut cluster);
}

/// Part 1: a quorum is a shape. Each quorum question of the core is one of
/// these two calls on an [`AcceptorConfig`].
fn quorums() {
    println!("== 1. which sets are quorums");
    let grid = AcceptorConfig::new(ALL.to_vec(), GRID);
    let set = |ids: &[NodeId]| ids.iter().copied().collect::<BTreeSet<_>>();

    // Phase 2: a full column, and nothing less.
    assert!(grid.has_phase2_quorum(&set(&[N1, N4])));
    assert!(!grid.has_phase2_quorum(&set(&[N1, N2, N3])));
    assert!(!grid.has_phase2_quorum(&set(&[N1, N5])));
    println!("   Phase 2: {{1, 4}} decides. {{1, 2, 3}} and {{1, 5}} do not");

    // Phase 1: one acceptor of each column. Four of six is a majority, and
    // it is not a quorum when it misses a column.
    assert!(grid.has_phase1_quorum(&set(&[N1, N2, N3])));
    assert!(grid.has_phase1_quorum(&set(&[N4, N2, N6])));
    assert!(!grid.has_phase1_quorum(&set(&[N1, N2, N4, N5])));
    println!("   Phase 1: {{1, 2, 3}} and {{4, 2, 6}} elect. {{1, 2, 4, 5}} does not");

    // The same six acceptors under a majority: any four, and only a count.
    let majority = AcceptorConfig::new(ALL.to_vec(), QuorumSystem::Majority);
    assert!(majority.has_phase2_quorum(&set(&[N1, N2, N4, N5])));
    assert!(!majority.has_phase2_quorum(&set(&[N1, N4])));
}

/// Part 2: each `Accept` goes to one column.
fn one_column_for_each_slot(cluster: &mut Cluster) {
    println!("\n== 2. six records, each to one column");
    cluster.elect(N1);
    for (number, text) in ["a", "b", "c", "d", "e", "f"].into_iter().enumerate() {
        cluster.trace = number == 1;
        let slot = cluster.write(N1, LeaderUuid::UNSET, 0, text);
        cluster.trace = false;
        let column = cluster.node(N1).acceptors().column_of(slot);
        assert_eq!(column, Some(number % 3));
        println!(
            "   slot {slot}: {text:?} is chosen by column {}",
            number % 3
        );
    }
    // The leader is an acceptor of column 0 and votes there with no
    // message. Each other acceptor received the `Accept`s of its column.
    for id in [N2, N3, N4, N5, N6] {
        assert_eq!(cluster.received(id, "Accept"), 2);
        assert_eq!(
            cluster.received(id, "Commit"),
            6,
            "each node learns each slot"
        );
        assert_eq!(cluster.journal(id).len(), 6);
    }
    println!("   each acceptor received 2 Accepts for 6 records, and learned all 6");
}

/// Part 3: one acceptor down stops the slots of its column.
fn one_acceptor_down(cluster: &mut Cluster) {
    println!("\n== 3. node 5 crashes: column 1 = {{2, 5}} cannot decide");
    cluster.crash(N5);
    let column_0 = cluster.write(N1, LeaderUuid::UNSET, 0, "g");
    let column_1 = cluster.write(N1, LeaderUuid::UNSET, 0, "h");
    let column_2 = cluster.write(N1, LeaderUuid::UNSET, 0, "i");
    assert!(cluster.node(N1).chosen_at(column_0).is_some());
    assert!(cluster.node(N1).chosen_at(column_1).is_none());
    assert!(cluster.node(N1).chosen_at(column_2).is_some());
    // A journal is applied in slot order: "i" is chosen, and it waits.
    assert_eq!(cluster.journal(N1).len(), 7);
    println!("   slot {column_1} stays open, and slot {column_2} is chosen but waits behind it");

    // The policy of the driver: the same slot, ballot and command, in a
    // column that answers.
    assert!(cluster.node(N1).move_round(column_1, 0));
    cluster.settle();
    for id in [N1, N2, N3, N4, N6] {
        assert_eq!(cluster.journal(id).len(), 9);
    }
    println!("   move_round({column_1}, column 0): the slot is chosen, and 9 records are served");
}

/// Part 4: a whole column down stops each election.
fn one_column_down(cluster: &mut Cluster) {
    println!("\n== 4. node 2 crashes too: column 1 is down, 4 nodes of 6 are up");
    cluster.crash(N2);
    cluster.node(N4).campaign();
    cluster.settle();
    // Nodes 1, 3 and 6 promised, and node 4 promised to itself: four
    // promises, and none from column 1.
    for id in [N1, N3, N4, N6] {
        let promised = cluster.disk(id).hard_state().max_promised_ballot;
        assert_eq!(promised.node, N4);
    }
    assert!(!cluster.node(N4).is_leader());
    assert!(
        !cluster.node(N1).is_leader(),
        "node 1 promised a higher ballot"
    );
    println!("   node 4 has four promises and does not win: no acceptor of column 1 answered");

    cluster.restart(N5);
    cluster.elect(N4);
    let slot = cluster.write(N4, LeaderUuid::UNSET, 0, "j");
    assert_eq!(cluster.node(N4).acceptors().column_of(slot), Some(0));
    assert_eq!(cluster.journal(N5).len(), 10);
    println!("   node 5 restarts: column 1 has an acceptor again, and node 4 wins");
    println!("   slot {slot}: \"j\" is chosen by column 0. Node 5 serves 10 records");
}
