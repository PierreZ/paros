//! **The replica tier: acceptors vote, replicas serve reads.**
//!
//! Run it: `cargo run -p paros-consensus --example replica_tier`
//!
//! This example drives [`ColocatedNode`] and [`ReplicaNode`] with the toy
//! cluster of `examples/common/cluster.rs`. Read `quickstart` first.
//!
//! # The problem
//!
//! A [`ColocatedNode`] is an acceptor and a replica: it votes on the log,
//! and it learns the log and serves reads from it. To serve more reads you
//! would add nodes. But one more acceptor makes each quorum larger, so each
//! write becomes slower.
//!
//! # The technique (Compartmentalized Paxos, section 3.3)
//!
//! Scale the two roles apart. A [`ReplicaNode`] learns the chosen log from
//! the `Commit`s of the leader and serves journal reads from it. It never
//! votes, so the quorums do not change when you add one.
//!
//! The acceptors stay learners too. They keep the chosen records, because
//! Paxos needs them: a new leader skips what is chosen, and a replica that
//! is behind gets the slots it misses from an acceptor.
//!
//! # The driver's part
//!
//! - `Config::replica_count` names how many replicas the deployment has. A
//!   replica has the same `Config` under its own id, and its id is outside
//!   the pool of the nodes.
//! - A replica is driven like a node: `step`, `tick`, `ready()`, persist
//!   `writes()`, send `messages()`, `advance()`. A restart is
//!   [`ReplicaNode::new`] on its disk.
//! - The core addresses a `Commit` and a beat to `Audience::Learners`. The
//!   driver sends these to each node of the pool **and to each replica**:
//!   which processes are learners is the deployment map of the driver. It
//!   is one line in `Cluster::pump`.
//! - `ReplicaNode::reply_owner(slot)` names the one replica that answers
//!   the client for a slot, so the replicas share that work.
//!
//! # What the trace shows
//!
//! 1. Node 0 leads three acceptors. Five records are written, and the two
//!    replicas learn each slot without a vote.
//! 2. One `Commit` to replica 11 is lost. Replica 11 learns the slots after
//!    the hole but cannot serve them. The next beat of the leader shows
//!    that it is behind, and it asks for the slot it misses.
//! 3. Replica 11 restarts from its disk and serves the same journal.
//! 4. Two acceptors crash. Nothing can be written, and the replicas still
//!    serve each read.
//! 5. Each slot has one reply owner.
//!
//! Further reading: Whittaker et al., *Scaling Replicated State Machines
//! with Compartmentalization* (2021), sections 2.3 and 3.3, and
//! `specs/tla/compartments/` for the model.

mod common;

use paros_consensus::contract::Party;
use paros_consensus::journal::{Control, JournalIdentifier, LeaderUuid, Outcome};
use paros_consensus::membership::ReplicaId;
use paros_consensus::{ColocatedNode, Config, Message, NodeId, ReplicaNode, Slot};

use common::cluster::Cluster;

const N0: NodeId = NodeId(0);
const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const R10: NodeId = NodeId(10);
const R11: NodeId = NodeId(11);

const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// Three acceptors, and two replicas beside them.
fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers([N0, N1, N2]);
    config.replica_count = 2;
    config
}

fn main() {
    let mut cluster = Cluster::new([N0, N1, N2].map(config)).with_replicas([R10, R11]);

    println!("== 1. three acceptors decide, two replicas learn");
    cluster.elect(N0);
    cluster.control(
        N0,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );
    cluster.write(N0, WRITER, 0, "alpha");
    cluster.trace = true;
    let slot = cluster.write(N0, WRITER, 1, "bravo");
    cluster.trace = false;
    for id in [R10, R11] {
        let replica: &ReplicaNode = cluster.replica(id);
        assert!(replica.chosen_at(slot).is_some());
        // A replica judges each slot as a node does, alone.
        assert!(matches!(
            cluster.outcome(id, slot),
            Some(Outcome::Accepted { .. })
        ));
        assert_eq!(cluster.replica_journal(id), ["alpha", "bravo"]);
    }
    println!("   slot {slot}: nodes 1 and 2 voted, replicas 10 and 11 only got the Commit");

    a_lost_commit(&mut cluster);
    restart(&mut cluster);
    without_a_quorum(&mut cluster);
    reply_owners(&mut cluster);
}

/// Part 2: a replica that misses a slot asks an acceptor for it.
fn a_lost_commit(cluster: &mut Cluster) {
    println!("\n== 2. one Commit to replica 11 is lost");
    let hole = cluster.propose(N0, WRITER, 2, "charlie");
    cluster.settle_where(|to, message| {
        !(to == Party::Node(R11) && matches!(message, Message::Commit { .. }))
    });
    let last = cluster.write(N0, WRITER, 3, "delta");
    cluster.write(N0, WRITER, 4, "echo");

    // Replica 11 knows the later slots are chosen, but a journal is read in
    // order: it serves nothing past the hole.
    let gap = cluster.replica(R11).replica().chosen_gap();
    assert!(matches!(gap, Some((from, _)) if from == hole));
    assert!(cluster.replica(R11).chosen_at(last).is_some());
    assert_eq!(cluster.replica_journal(R11), ["alpha", "bravo"]);
    assert_eq!(cluster.replica_journal(R10).len(), 5);
    println!("   replica 11 misses slot {hole} and serves [\"alpha\", \"bravo\"]");

    // The beat carries the chosen index of the leader. Replica 11 sees the
    // hole and asks the leader for it.
    cluster.trace = true;
    cluster.tick();
    cluster.trace = false;
    assert_eq!(cluster.replica(R11).replica().chosen_gap(), None);
    assert_eq!(
        cluster.replica_journal(R11),
        ["alpha", "bravo", "charlie", "delta", "echo"]
    );
    assert_eq!(
        cluster.learned(R10),
        cluster.learned(R11),
        "each replica learned the same slots in the same order"
    );
    println!("   replica 11 caught up and serves five records");
}

/// Part 3: a replica persists what it learns.
fn restart(cluster: &mut Cluster) {
    println!("\n== 3. replica 11 restarts from its disk");
    cluster.restart_replica(R11);
    assert_eq!(cluster.replica_journal(R11).len(), 5);
    println!("   it serves five records before it hears from any node");
}

/// Part 4: reads do not need the acceptors.
fn without_a_quorum(cluster: &mut Cluster) {
    println!("\n== 4. two of the three acceptors crash");
    cluster.crash(N1);
    cluster.crash(N2);
    let slot = cluster.write(N0, WRITER, 5, "foxtrot");
    let leader: &ColocatedNode = cluster.node(N0);
    assert!(leader.chosen_at(slot).is_none(), "one vote is not a quorum");
    // The replicas are two more processes, and they do not help: a replica
    // is never a member of a quorum.
    for id in [R10, R11] {
        assert_eq!(cluster.replica_journal(id).len(), 5);
    }
    println!("   \"foxtrot\" is not chosen: the replicas do not vote");
    println!("   replicas 10 and 11 still serve the five records");
}

/// Part 5: the replicas share the answers to the clients.
fn reply_owners(cluster: &mut Cluster) {
    println!("\n== 5. who answers the client of each slot");
    for slot in (0..4).map(Slot) {
        let owner = cluster
            .replica(R10)
            .reply_owner(slot)
            .expect("a deployment with replicas has an owner for each slot");
        assert_eq!(owner, ReplicaId(slot.0 % 2));
        println!("   slot {slot}: replica rank {}", owner.0);
    }
}
