//! **Multi-Paxos: one election for many slots, and what a new leader owes
//! the log.**
//!
//! Run it: `cargo run -p paros-consensus --example multi_paxos`
//!
//! This example drives [`ColocatedNode`] with the toy cluster of
//! `examples/common/cluster.rs`. Read `single_decree` for the two phases of
//! Paxos, and `quickstart` for the driver loop.
//!
//! # A slot is a position, a ballot is an attempt
//!
//! Single-decree Paxos chooses one value. A log needs one chosen value for
//! each position, and a position is a **slot**. A **ballot** names one
//! leadership. The two are separate: one ballot chooses many slots, and one
//! slot can be accepted under more than one ballot, always with the value
//! that the rule P2c carries from the earlier ballot to the later one.
//!
//! # One Phase 1 for each future slot
//!
//! The `Prepare` of a candidate asks for a promise on *each slot from the
//! first one that the candidate does not know is chosen*. When a quorum
//! promised, the candidate leads. Each command then costs only Phase 2: one
//! `Accept`, a quorum of `Accepted`, one `Commit`.
//!
//! # What a new leader owes the log
//!
//! A leader can crash between "a quorum accepted" and "each node was told".
//! The promises that the next leader collects report what each acceptor
//! holds, slot by slot, and the new leader does two things with them:
//!
//! 1. **It proposes each reported value again** (P2c, for each slot). A
//!    value that one acceptor reports can be chosen already, so no other
//!    value is safe in that slot.
//! 2. **It fills each slot that nobody reported with a `Noop`.** Slots are
//!    given in order, so a leader can leave an empty slot below a chosen
//!    one. Each node applies in slot order, so the log would stop at that
//!    hole for ever. A `Noop` closes it.
//!
//! # The driver's part
//!
//! None of this is new work for a driver. [`ColocatedNode`] runs the
//! recovery when its campaign wins, and the `Accept`s of the recovery are
//! in `ready.messages()` like any other. A long recovery is paged: the
//! driver calls `advance_recovery()` after each `advance()`, and
//! `ready.recovery_batch()` shows the progress.
//!
//! The journal of this example is a multi-writer journal
//! ([`WriterMode::Multi`]): a write names no writer and takes the next
//! position, so the slots of the trace hold only what the example writes.
//!
//! # What the trace shows
//!
//! 1. Node 1 wins an election. Three records then cost Phase 2 only.
//! 2. Node 1 proposes D in slot 3 and E in slot 4. D reaches no other node.
//!    E reaches node 2, so E is chosen, and only node 1 knows it. Node 1
//!    crashes.
//! 3. Node 2 wins an election. Its recovery fills slot 3 with a `Noop` and
//!    proposes E again in slot 4, at the new ballot.
//! 4. Node 1 restarts as a follower. Its disk still holds D in slot 3. It
//!    learns the `Noop`, and its record of the slot is replaced.
//!
//! Further reading: Lamport, *Paxos Made Simple* (2001), section 3, and
//! `specs/tla/multi-paxos/` for the model.

mod common;

use paros_consensus::contract::Party;
use paros_consensus::journal::{Command, Control, JournalIdentifier, LeaderUuid, WriterMode};
use paros_consensus::{Ballot, Config, Message, NodeId, Slot, Storage};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers([N1, N2, N3]);
    config.writer_mode = WriterMode::Multi;
    config
}

/// The text of the write that node `id` holds as chosen at `slot`.
fn chosen(cluster: &mut Cluster, id: NodeId, slot: Slot) -> Option<String> {
    match cluster.node(id).chosen_at(slot)? {
        Command::Write(entry) => Some(String::from_utf8_lossy(&entry.records[0]).into_owned()),
        other => Some(format!("{other:?}")),
    }
}

fn main() {
    let mut cluster = Cluster::new([N1, N2, N3].map(config));

    let first = one_phase_1(&mut cluster);
    let (hole, kept) = the_leader_crashes(&mut cluster);
    let second = recovery(&mut cluster, first, hole, kept);
    the_old_leader_returns(&mut cluster, second, hole);
}

/// Part 1: Phase 1 one time, then Phase 2 for each command.
fn one_phase_1(cluster: &mut Cluster) -> Ballot {
    println!("== 1. one election, then three records");
    cluster.trace = true;
    cluster.node(N1).campaign();
    cluster.settle();
    assert!(cluster.node(N1).is_leader());
    let ballot = cluster.node(N1).ballot();
    println!("   node 1 leads at ballot {ballot}");
    cluster.take_sent();

    for text in ["A", "B", "C"] {
        let slot = cluster.write(N1, LeaderUuid::UNSET, 0, text);
        cluster.trace = false;
        println!("   slot {slot}: {text:?} is chosen");
    }
    let sent = cluster.take_sent();
    assert!(
        !sent.contains_key("Prepare"),
        "no Phase 1 after the election"
    );
    assert_eq!(sent["Accept"], 3, "one Accept for each record");
    for id in [N1, N2, N3] {
        assert_eq!(cluster.journal(id), ["A", "B", "C"]);
    }
    println!("   three records cost {sent:?}");
    ballot
}

/// Part 2: a leader that crashes with two slots open.
fn the_leader_crashes(cluster: &mut Cluster) -> (Slot, Slot) {
    println!("\n== 2. node 1 proposes D and E, and crashes");
    let hole = cluster.propose(N1, LeaderUuid::UNSET, 0, "D");
    let kept = cluster.propose(N1, LeaderUuid::UNSET, 0, "E");
    // The network delivers one `Accept`: the one of E to node 2. No
    // `Commit` leaves node 1.
    cluster.settle_where(|to, message| match message {
        Message::Accept { slot, .. } => *slot == kept && to == Party::Node(N2),
        Message::Commit { .. } => false,
        _ => true,
    });

    // Nodes 1 and 2 accepted E: it is chosen. Only node 1 counted the
    // votes, and it cannot apply E, because slot 3 is not chosen.
    assert_eq!(chosen(cluster, N1, kept).as_deref(), Some("E"));
    assert_eq!(cluster.journal(N1), ["A", "B", "C"]);
    assert!(cluster.node(N2).acceptor().record(kept).is_some());
    assert!(cluster.node(N2).chosen_at(kept).is_none());
    assert!(cluster.node(N3).acceptor().record(kept).is_none());
    assert!(cluster.node(N2).acceptor().record(hole).is_none());
    assert!(cluster.node(N3).acceptor().record(hole).is_none());
    cluster.crash(N1);
    println!("   slot {hole}: D is on node 1 only");
    println!("   slot {kept}: E is on nodes 1 and 2. It is chosen, and no running node knows it");
    (hole, kept)
}

/// Part 3: the new leader proposes what was reported, and fills the rest.
fn recovery(cluster: &mut Cluster, first: Ballot, hole: Slot, kept: Slot) -> Ballot {
    println!("\n== 3. node 2 wins an election and recovers the log");
    cluster.trace = true;
    cluster.node(N2).campaign();
    cluster.settle();
    cluster.trace = false;
    assert!(cluster.node(N2).is_leader());
    let second = cluster.node(N2).ballot();
    assert!(second > first);

    for id in [N2, N3] {
        // No promise reported slot 3, so a `Noop` is chosen there.
        let noop = Command::Control(Control::Noop);
        assert_eq!(cluster.node(id).chosen_at(hole), Some(&noop));
        // The promise of node 2 reported E, so E is chosen again in slot 4.
        assert_eq!(chosen(cluster, id, kept).as_deref(), Some("E"));
        assert_eq!(cluster.journal(id), ["A", "B", "C", "E"]);
    }
    // The value belongs to the slot, and the ballot records which
    // leadership put it there: E is now accepted at the ballot of node 2.
    let record = cluster.node(N2).acceptor().record(kept);
    assert!(matches!(record, Some((ballot, _)) if *ballot == second));
    // A slot that node 2 knew was chosen is outside the `Prepare`, and its
    // record keeps the first ballot.
    let record = cluster.node(N2).acceptor().record(Slot(0));
    assert!(matches!(record, Some((ballot, _)) if *ballot == first));
    println!("   node 2 leads at ballot {second}");
    println!("   slot {hole}: nobody reported a value, so it is filled with a Noop");
    println!("   slot {kept}: node 2 reported E, so E is proposed again and chosen");

    let slot = cluster.write(N2, LeaderUuid::UNSET, 0, "F");
    assert!(slot > kept, "a new command goes above the recovered slots");
    println!(
        "   slot {slot}: \"F\" is chosen. D was never chosen: a client that wants D writes it again"
    );
    second
}

/// Part 4: a restart is a follower, and catch-up repairs its records.
fn the_old_leader_returns(cluster: &mut Cluster, second: Ballot, hole: Slot) {
    println!("\n== 4. node 1 restarts and catches up");
    // The disk of node 1 holds what it accepted: D in slot 3.
    let (_, command) = cluster.disk(N1).accepted(hole).expect("a record");
    assert!(matches!(command, Command::Write(entry) if *entry.records[0] == *b"D"));
    cluster.restart(N1);
    assert!(!cluster.node(N1).is_leader(), "leadership is never durable");

    cluster.trace = true;
    cluster.tick();
    cluster.trace = false;
    // Safety never needed this: a later Phase 1 hears the `Noop` at the
    // higher ballot from a quorum, and it outranks D. The catch-up makes
    // the record of node 1 agree with the log that it serves.
    let (ballot, command) = cluster.disk(N1).accepted(hole).expect("a record");
    assert_eq!(command, Command::Control(Control::Noop));
    assert_eq!(ballot, second);
    for id in [N1, N2, N3] {
        assert_eq!(cluster.journal(id), ["A", "B", "C", "E", "F"]);
    }
    println!("   slot {hole} on node 1: D is replaced by the Noop");
    println!("   each node serves [\"A\", \"B\", \"C\", \"E\", \"F\"]");
}
