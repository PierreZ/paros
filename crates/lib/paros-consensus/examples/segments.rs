//! **Segments: cut the log with a `Roll`, and read the chain at the
//! matchmakers.**
//!
//! Run it: `cargo run -p paros-consensus --example segments`
//!
//! This example drives [`ColocatedNode::roll`] on three nodes and three
//! [`paros_consensus::Matchmaker`]s, with the toy cluster of
//! `examples/common/cluster.rs`. Read `quickstart` and `reconfiguration`
//! first. The design is `docs/architecture/04-7-segments.md` (§4.7), and the
//! models are `specs/tla/segment-roll/` and `specs/tla/handover-chain/`.
//!
//! # A segment is a range of slots
//!
//! The log of a journal is cut into **segments**. A segment is a range of
//! slots, and the **tail** segment is the one that takes new slots. A
//! `Roll` is a command of the log, beside `Write` and `Control`. When it is
//! chosen at slot `s`, it ends the tail at `s + 1` and opens the next
//! segment there.
//!
//! - **A roll is a call.** Nothing rolls on its own. The driver calls
//!   `roll(None)` to keep the replicas of the segment it ends, or
//!   `roll(Some(replicas))` to name new ones (D4).
//! - **A roll needs matchmakers** (D5). A plain deployment refuses it, so a
//!   plain log stays in its first segment.
//! - **The fold judges it.** Like a `Write`, a `Roll` is judged when its
//!   slot is applied: it is valid only when it names the tail plus one. Each
//!   node applies the same slots in the same order, so each node cuts at the
//!   same slot.
//! - **No pause.** The leader goes on proposing above a `Roll` that is not
//!   yet chosen. A second roll builds on the first.
//! - **A roll takes no position.** Like a `Noop`, it uses a slot. The
//!   positions of the journal do not see it.
//!
//! # The chain at the matchmakers
//!
//! After its own fold applied a `Roll`, each node publishes the end of the
//! segment to the matchmakers (`Ready::segment_publishes()`; each ack comes
//! back through `ColocatedNode::on_segment_ack`). The matchmakers keep the
//! **chain**: an add-only map from a segment id to its start, how it opened
//! and its end. Each field is a function of the chosen log, so each writer
//! writes the same value, and two chains merge with no compare-and-set.
//! `Matchmaker::chain` reads it on one matchmaker.
//!
//! # What the trace shows
//!
//! 1. A plain deployment refuses a roll. A follower names the leader.
//! 2. The leader writes, then rolls and keeps the replicas. The next write takes the
//!    next position.
//! 3. The leader rolls twice with no wait, the second time onto new
//!    replicas. Each node folds both cuts at the same slots.
//! 4. Each matchmaker holds the same chain.
//! 5. The leader crashes. The new leader rolls from the tail it knows.

mod common;

use paros_consensus::journal::{
    Chain, Control, JournalIdentifier, LeaderUuid, Outcome, ReplicaSet, Segment, SegmentId, Seq,
};
use paros_consensus::membership::MatchmakerId;
use paros_consensus::node::{RollRefusal, RollResult};
use paros_consensus::{Config, NodeId, Slot};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// The configuration of node `id`: three acceptors and three matchmakers.
fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers([N1, N2, N3]);
    config.matchmakers = (0..3).map(MatchmakerId).collect();
    config
}

fn main() {
    refusals();

    println!("\n== 2. node 1 leads, writes, and rolls with the same replicas");
    let mut cluster = Cluster::new([N1, N2, N3].map(config));
    cluster.elect(N1);
    cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );
    write(&mut cluster, N1, 0, "alpha");
    write(&mut cluster, N1, 1, "bravo");

    let first = roll_and_keep(&mut cluster);
    let (second, third) = roll_twice(&mut cluster);
    read_the_chain(&mut cluster, [first, second, third]);
    roll_after_a_new_leader(&mut cluster);

    println!("\neach node and each matchmaker agree on every cut");
}

/// Part 1: the refusals are results, never panics.
fn refusals() {
    println!("== 1. who may roll");
    let plain = Config::new(N1, JournalIdentifier::UNSET).with_peers([N1]);
    let mut alone = Cluster::new([plain]);
    alone.elect(N1);
    assert_eq!(
        alone.node(N1).roll(None),
        RollResult::Refused(RollRefusal::NoMatchmakers)
    );
    println!("   plain deployment, leader roll(None)  -> refused, no matchmakers");

    let mut cluster = Cluster::new([N1, N2, N3].map(config));
    cluster.elect(N1);
    assert_eq!(cluster.node(N2).roll(None), RollResult::NotLeader(Some(N1)));
    println!("   matchmaker deployment, node 2 roll   -> not the leader, try node 1");
}

/// Part 2: one roll that keeps the replicas.
fn roll_and_keep(cluster: &mut Cluster) -> Slot {
    let RollResult::Accepted { slot, next } = cluster.node(N1).roll(None) else {
        panic!("the leader of a matchmaker deployment admits a roll");
    };
    assert_eq!(next, SegmentId(1));
    cluster.settle();
    let cut = Segment {
        id: SegmentId(1),
        start: Slot(slot.0 + 1),
    };
    assert_eq!(cluster.outcome(N1, slot), Some(&Outcome::Rolled(cut)));
    println!(
        "   slot {slot}: Roll -> segment 1 opens at slot {}",
        cut.start
    );

    // The roll used a slot and no position: the next write is position 2.
    let slot = write(cluster, N1, 2, "charlie");
    assert_eq!(slot, cut.start, "the first slot of segment 1");
    assert_eq!(cluster.journal(N1), ["alpha", "bravo", "charlie"]);
    slot
}

/// Part 3: two rolls in a row, with no wait for the first.
fn roll_twice(cluster: &mut Cluster) -> (Slot, Slot) {
    println!("\n== 3. node 1 rolls twice with no wait, then onto {{2, 3}}");
    let RollResult::Accepted { slot: a, next } = cluster.node(N1).roll(None) else {
        panic!("the leader admits a roll");
    };
    assert_eq!(next, SegmentId(2));
    // The first roll is not chosen yet: the leader builds on its open round.
    let replicas = ReplicaSet::new([N2, N3]);
    let RollResult::Accepted { slot: b, next } = cluster.node(N1).roll(Some(replicas)) else {
        panic!("the leader admits a second roll before the first is chosen");
    };
    assert_eq!(
        next,
        SegmentId(3),
        "the second roll names the tail plus one"
    );
    assert!(b > a);
    cluster.settle();

    let tail = Segment {
        id: SegmentId(3),
        start: Slot(b.0 + 1),
    };
    for id in [N1, N2, N3] {
        assert!(matches!(cluster.outcome(id, a), Some(Outcome::Rolled(_))));
        assert_eq!(cluster.outcome(id, b), Some(&Outcome::Rolled(tail)));
        assert_eq!(cluster.node(id).replica().journal().segment, tail);
    }
    println!("   slot {a}: Roll -> segment 2 opens at slot {}", a.0 + 1);
    println!(
        "   slot {b}: Roll -> segment 3 opens at slot {}",
        tail.start
    );
    println!(
        "   nodes 1, 2 and 3 all fold the tail to segment 3 at slot {}",
        tail.start
    );
    (Slot(a.0 + 1), tail.start)
}

/// Part 4: the chain at the matchmakers.
fn read_the_chain(cluster: &mut Cluster, starts: [Slot; 3]) {
    println!("\n== 4. the chain at the matchmakers");
    // A publish leaves after the fold; the cluster settled it already.
    cluster.tick();
    let journal = cluster.journal_id(N1);
    let chain: Chain = cluster
        .matchmaker(MatchmakerId(0))
        .chain(journal)
        .expect("the matchmakers hold the chain")
        .clone();
    for id in (1..3).map(MatchmakerId) {
        assert_eq!(cluster.matchmaker(id).chain(journal), Some(&chain));
    }

    let [one, two, three] = starts;
    let expected = [
        (SegmentId(0), Slot(0), Some(one), None),
        (SegmentId(1), one, Some(two), Some(None)),
        (SegmentId(2), two, Some(three), Some(None)),
        (
            SegmentId(3),
            three,
            None,
            Some(Some(ReplicaSet::new([N2, N3]))),
        ),
    ];
    assert_eq!(chain.len(), expected.len());
    for (id, start, end, opening) in expected {
        let record = &chain[&id];
        assert_eq!(record.start, start);
        assert_eq!(record.end, end);
        assert_eq!(record.opening.as_ref().map(|o| o.replicas.clone()), opening);
        let end = end.map_or_else(|| "open".to_string(), |slot| format!("ends at {slot}"));
        let opened = match &record.opening {
            None => "at the bootstrap".to_string(),
            Some(o) => match &o.replicas {
                None => "with the same replicas".to_string(),
                Some(set) => format!(
                    "on nodes {:?}",
                    set.members().iter().map(|n| n.0).collect::<Vec<_>>()
                ),
            },
        };
        println!(
            "   segment {}: starts at {start}, {end}, opened {opened}",
            id.0
        );
    }
    println!("   matchmakers 0, 1 and 2 hold the same chain");
}

/// Part 5: a new leader rolls from the tail that its Phase 1 learned.
fn roll_after_a_new_leader(cluster: &mut Cluster) {
    println!("\n== 5. node 1 crashes, node 2 leads and rolls");
    cluster.crash(N1);
    cluster.elect(N2);
    let RollResult::Accepted { slot, next } = cluster.node(N2).roll(None) else {
        panic!("the new leader admits a roll");
    };
    assert_eq!(next, SegmentId(4), "leadership is not durable, the log is");
    cluster.settle();
    assert!(matches!(
        cluster.outcome(N2, slot),
        Some(Outcome::Rolled(_))
    ));
    println!("   slot {slot}: Roll -> segment 4, after the 3 of node 1");

    write(cluster, N2, 3, "delta");
    cluster.restart(N1);
    cluster.tick();
    cluster.tick();
    for id in [N1, N2, N3] {
        assert_eq!(cluster.journal(id), ["alpha", "bravo", "charlie", "delta"]);
        assert_eq!(
            cluster.node(id).replica().journal().segment.id,
            SegmentId(4)
        );
    }
    println!("   node 1 restarts and folds to segment 4 too");
}

/// Append `text` at position `seq` through `leader`, and check that the
/// journal took it.
fn write(cluster: &mut Cluster, leader: NodeId, seq: u64, text: &str) -> Slot {
    let slot = cluster.write(leader, WRITER, seq, text);
    assert_eq!(
        cluster.outcome(leader, slot),
        Some(&Outcome::Accepted {
            seq: Seq(seq),
            count: 1
        })
    );
    println!("   slot {slot}: Write {text:?} -> accepted at position {seq}");
    slot
}
