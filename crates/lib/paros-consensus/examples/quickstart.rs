//! **Quick start: three nodes, one journal, one driver loop.**
//!
//! Run it: `cargo run -p paros-consensus --example quickstart`
//!
//! Start here if you want to *use* the crate. The lessons `single_decree`,
//! `multi_paxos` and `matchmaker` teach the algorithm with the roles driven
//! by hand. This example drives [`ColocatedNode`], the deployment that wires
//! the roles for you, and it shows the whole contract between the core and
//! its driver.
//!
//! # The core computes; the driver does the I/O
//!
//! [`ColocatedNode`] has no socket, no disk and no clock. The driver gives
//! it three inputs:
//!
//! - a peer message: `node.step(message)`;
//! - one unit of logical time: `node.tick()`;
//! - a client call: `node.propose(entry)` or `node.propose_control(control)`.
//!
//! After each input the driver calls `node.ready()` and gets one batch of
//! work. It must process the batch in this order:
//!
//! 1. persist `ready.writes()`, and fsync them;
//! 2. only then send `ready.messages()`;
//! 3. learn `ready.committed()`, the slots that are now chosen and durable;
//! 4. call `ready.advance()`.
//!
//! Step 1 before step 2 is the rule that makes Paxos safe: a `Promise` or an
//! `Accepted` that leaves before its write is durable is a vote that a crash
//! can take back. The loop is [`Cluster::pump`] in `examples/common/cluster.rs`,
//! thirty lines. The network of this example is a queue of messages, and
//! each disk is a [`MemStorage`].
//!
//! # A journal, not a key-value store
//!
//! The log of paros is a **journal**: an ordered list of records that a
//! writer appends and a reader reads from a position. Two kinds of command
//! go through consensus:
//!
//! - `Control::SetLeader { new, old }` names the **writer** of the journal
//!   with a compare-and-set. The writer is a client, identified by a
//!   [`LeaderUuid`]. It is not the Paxos leader: the Paxos leader is the
//!   *node* that runs Phase 2, and the writer is the *client* that may
//!   append.
//! - A `Write` ([`Entry`]) carries the uuid of its writer and the position
//!   it asks for. The journal accepts it only from the current writer and
//!   only at the next position.
//!
//! The core decides nothing about a command when you propose it. Each node
//! judges it when the slot is applied, in slot order, and the verdict is the
//! [`Outcome`] that `ready.committed()` carries. Because each node applies
//! the same slots in the same order, each node reaches the same verdict.
//!
//! # What the trace shows
//!
//! 1. Node 1 campaigns and wins: `Prepare` out, `Promise` back.
//! 2. A client becomes the writer, then appends two records. A write under
//!    another uuid is chosen by Paxos and refused by the journal.
//! 3. A follower refuses a proposal and names the leader.
//! 4. Node 3 crashes, misses a record, restarts from its disk and catches up.
//! 5. The leader crashes. Node 2 campaigns at a higher ballot, and the same
//!    writer continues at the next position.

mod common;

use paros_consensus::journal::{Control, Entry, JournalIdentifier, LeaderUuid, Outcome, Seq};
use paros_consensus::node::ProposeResult;
use paros_consensus::{ColocatedNode, Config, NodeId, Slot, Value};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

/// The client that writes the journal. A real client draws a random uuid for
/// each term as the writer.
const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// The configuration of node `id`: who it is, and the three acceptors.
/// Plain Multi-Paxos needs nothing else.
fn config(id: NodeId) -> Config {
    Config::new(id, JournalIdentifier::UNSET).with_peers([N1, N2, N3])
}

fn main() {
    let mut cluster = Cluster::new([N1, N2, N3].map(config));

    elect(&mut cluster);
    write(&mut cluster);
    redirect(&mut cluster);
    crash_a_follower(&mut cluster);
    crash_the_leader(&mut cluster);

    println!("\nall three nodes serve the same journal");
}

/// Part 1: an election is Phase 1 of Paxos, one time for the whole log.
fn elect(cluster: &mut Cluster) {
    println!("== 1. node 1 campaigns");
    cluster.trace = true;
    // A driver starts an election when its election timeout runs out
    // (`set_election_timeout`, `tick`). An example does it directly.
    cluster.node(N1).campaign();
    cluster.settle();
    cluster.trace = false;

    let leader: &ColocatedNode = cluster.node(N1);
    assert!(leader.is_leader());
    println!("   node 1 leads at ballot {}", leader.ballot());

    // The promise is on each disk before the `Promise` left the node.
    for id in [N1, N2, N3] {
        assert_eq!(
            cluster.disk(id).hard_state().max_promised_ballot,
            cluster.node(N1).ballot(),
            "node {id} persisted its promise"
        );
    }
    // A follower learns who leads from the first beat.
    cluster.tick();
    assert_eq!(cluster.node(N2).leader(), Some(N1));
}

/// Part 2: the writer fence, then two records.
fn write(cluster: &mut Cluster) {
    println!("\n== 2. a client becomes the writer and appends");

    // A write before the journal has a writer is chosen, then refused.
    let slot = cluster.write(N1, WRITER, 0, "too early");
    let outcome = cluster.outcome(N1, slot).expect("the slot is applied");
    assert!(matches!(outcome, Outcome::Refused(view) if view.leader.is_none()));
    println!("   slot {slot}: Write \"too early\" -> refused, the journal has no writer");

    // `SetLeader` is a compare-and-set: `old` is the writer that the caller
    // believes current, `None` on a new journal.
    let slot = cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );
    let outcome = cluster.outcome(N1, slot).expect("the slot is applied");
    assert!(matches!(outcome, Outcome::Leader(view) if view.leader == Some(WRITER)));
    println!("   slot {slot}: SetLeader -> the client is the writer");

    for (seq, text) in [(0, "alpha"), (1, "bravo")] {
        let slot = cluster.write(N1, WRITER, seq, text);
        assert_eq!(
            cluster.outcome(N1, slot),
            Some(&Outcome::Accepted {
                seq: Seq(seq),
                count: 1
            })
        );
        println!("   slot {slot}: Write {text:?} -> accepted at position {seq}");
    }

    // Another client that never won a `SetLeader` is fenced. Paxos chooses
    // its command like any other; the journal refuses it at apply.
    let slot = cluster.write(N1, LeaderUuid(0xB0B), 2, "intruder");
    assert!(matches!(
        cluster.outcome(N1, slot),
        Some(Outcome::Refused(_))
    ));
    println!("   slot {slot}: Write \"intruder\" by another uuid -> refused");

    // Each node reached each verdict on its own, from the same slots.
    for id in [N2, N3] {
        assert_eq!(cluster.outcome(id, slot), cluster.outcome(N1, slot));
        assert_eq!(cluster.journal(id), ["alpha", "bravo"]);
    }
    println!("   each node serves [\"alpha\", \"bravo\"]");
}

/// Part 3: only the leader admits a proposal.
fn redirect(cluster: &mut Cluster) {
    println!("\n== 3. a follower refuses a proposal");
    let entry = Entry {
        leader: WRITER,
        seq: Seq(2),
        records: vec![Value::from(b"charlie".to_vec())],
    };
    let result = cluster.node(N2).propose(entry);
    assert_eq!(result, ProposeResult::NotLeader(Some(N1)));
    println!("   node 2 answers NotLeader, try node 1");
}

/// Part 4: a restart is `ColocatedNode::new` on the disk the crash left.
fn crash_a_follower(cluster: &mut Cluster) {
    println!("\n== 4. node 3 crashes, misses a record, and comes back");
    cluster.crash(N3);
    // Two of three is a majority: the journal stays writable.
    let slot = cluster.write(N1, WRITER, 2, "charlie");
    assert!(matches!(
        cluster.outcome(N1, slot),
        Some(Outcome::Accepted { .. })
    ));
    println!("   slot {slot}: Write \"charlie\" -> accepted by nodes 1 and 2");

    cluster.restart(N3);
    // Nothing that node 3 persisted is lost: it boots with its promise and
    // its chosen prefix, and it serves what it had.
    assert_eq!(cluster.journal(N3), ["alpha", "bravo"]);
    println!("   node 3 boots from its disk with [\"alpha\", \"bravo\"]");

    // The beat of the leader carries its chosen index. Node 3 sees the gap
    // and asks for the slots it misses.
    cluster.trace = true;
    cluster.tick();
    cluster.trace = false;
    assert_eq!(cluster.journal(N3), ["alpha", "bravo", "charlie"]);
    println!("   node 3 caught up");
}

/// Part 5: a new leader, the same journal, the same writer.
fn crash_the_leader(cluster: &mut Cluster) {
    println!("\n== 5. the leader crashes and node 2 takes over");
    let old = cluster.node(N1).ballot();
    cluster.crash(N1);
    cluster.elect(N2);
    let new = cluster.node(N2).ballot();
    assert!(new > old, "a new leadership is a higher ballot");
    println!("   node 2 leads at ballot {new} (it was {old})");

    // The writer did not change: the Paxos leader and the writer of the
    // journal are two separate things.
    let slot = cluster.write(N2, WRITER, 3, "delta");
    assert_eq!(
        cluster.outcome(N2, slot),
        Some(&Outcome::Accepted {
            seq: Seq(3),
            count: 1
        })
    );
    println!("   slot {slot}: Write \"delta\" -> accepted at position 3");

    // Node 1 restarts as a follower, whatever it was before.
    cluster.restart(N1);
    assert!(!cluster.node(N1).is_leader());
    cluster.tick();
    cluster.tick();
    for id in [N1, N2, N3] {
        assert_eq!(cluster.journal(id), ["alpha", "bravo", "charlie", "delta"]);
    }
    assert!(cluster.node(N1).replica().chosen_at(Slot(0)).is_some());
}
