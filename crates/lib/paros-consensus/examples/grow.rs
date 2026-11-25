//! **Grow one journal from three nodes to the whole deployment.**
//!
//! Run it: `cargo run -p paros-consensus --example grow`
//!
//! This is the one tour of the deployments of `paros-consensus`. It starts
//! with three nodes and plain majorities, and it grows the same running
//! journal, one reconfiguration at a time, until it has five acceptors, then
//! a grid of six, proxy leaders and replicas. On the way it hands over the
//! matchmaker set and the leadership, cuts the log into segments, and
//! survives two leader crashes. Each stage writes a record, and each node
//! serves the same journal at the end of each stage. Every claim of the trace
//! is an assertion.
//!
//! The example `single_decree` shows single-decree Paxos, which has no
//! deployment. Read it first if Paxos is new to you.
//!
//! # The core computes; the driver does the I/O
//!
//! [`ColocatedNode`] has no socket, no disk and no clock. The driver gives
//! it a peer message (`step`), one unit of logical time (`tick`) or a client
//! call (`propose`, `propose_control`, `reconfigure`, `roll`). Then it drains
//! `ready()`, in this order:
//!
//! 1. persist `ready.writes()`, and fsync them;
//! 2. only then send `ready.messages()`, and the requests to the matchmakers
//!    (`match_requests`, `gc_requests`, `segment_publishes`);
//! 3. learn `ready.committed()`, the slots that are now chosen;
//! 4. call `ready.advance()`.
//!
//! Step 1 before step 2 is the rule that makes Paxos safe: a vote that leaves
//! before its write is durable is a vote that a crash takes back. The loop is
//! `Cluster::pump` in `examples/common/cluster.rs`. The network of this
//! example is a queue of messages, each disk is a [`MemStorage`], and the
//! wire can lose or hold a message.
//!
//! # A configuration belongs to a ballot
//!
//! The set of acceptors, their quorum system and the counts of proxy leaders
//! and replicas are one [`AcceptorConfig`]. It is never edited in place. A
//! reconfiguration is a new ballot: the leader registers the new
//! configuration at a majority of the matchmakers, runs Phase 1 over each
//! configuration that the matchmakers report, and then runs Phase 2 under
//! the new one only. A deployment with no matchmakers is plain Multi-Paxos:
//! it refuses `reconfigure` and `roll`.
//!
//! # The stages
//!
//! 1. Three nodes elect a leader. A client becomes the writer and appends.
//! 2. The leader crashes with a slot open. The new leader recovers the log:
//!    P2c for a reported value, a `Noop` for a slot nobody reported.
//! 3. The journal grows to five acceptors.
//! 4. The journal grows to a grid of six: one column decides a slot, and a
//!    row serves a read with no leader.
//! 5. Two proxy leaders run Phase 2 for the leader, and two replicas join.
//! 6. The matchmaker set hands over to a new set.
//! 7. The leader hands its ballot to another node, with no election.
//! 8. The leader cuts the log into a new segment.
//! 9. The leader crashes in the full deployment. A new leader takes over,
//!    and the matchmakers forget the configurations that no leader needs.
//!
//! Further reading: Lamport, *Paxos Made Simple* (2001); Whittaker et al.,
//! *Matchmaker Paxos* (2020) and *Scaling Replicated State Machines with
//! Compartmentalization* (2021); and `specs/tla/` for the model of each
//! flavor.

mod common;

use paros_consensus::contract::{MemStorage, Party};
use paros_consensus::journal::{
    Command, Control, JournalIdentifier, LeaderUuid, Outcome, ReplicaSet, Segment, SegmentId, Seq,
};
use paros_consensus::matchmaker::{MatchmakerPhase, ReconfigurerStep};
use paros_consensus::membership::{
    AcceptorConfig, MatchmakerId, MatchmakerSet, ProxyId, QuorumSystem,
};
use paros_consensus::node::{ProposeResult, ReconfigureRefusal, ReconfigureResult, RollResult};
use paros_consensus::quorum_read::{ReadOpen, ReadState};
use paros_consensus::{
    Ballot, ColocatedNode, Config, Limits, MatchmakerReconfigurer, Message, NodeId, Slot, Storage,
    Value,
};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);
const N5: NodeId = NodeId(5);
const N6: NodeId = NodeId(6);
/// Every node that can ever be an acceptor: the pool.
const NODES: [NodeId; 6] = [N1, N2, N3, N4, N5, N6];

/// The two replicas. A replica is a process outside the pool: it learns and
/// never votes.
const R10: NodeId = NodeId(10);
const R11: NodeId = NodeId(11);

const M0: MatchmakerId = MatchmakerId(0);
const M1: MatchmakerId = MatchmakerId(1);
const M2: MatchmakerId = MatchmakerId(2);
const M3: MatchmakerId = MatchmakerId(3);

/// The client that writes the journal. A real client draws a random uuid.
const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// Two rows of three: rows `{1, 2, 3}` and `{4, 5, 6}`, columns `{1, 4}`,
/// `{2, 5}` and `{3, 6}`.
const GRID: QuorumSystem = QuorumSystem::grid(2, 3);

/// The configuration of node `id` at the bootstrap. `peers` is the first
/// configuration, `nodes` the pool that later ones draw from. The
/// matchmakers turn the deployment into Matchmaker Paxos; `m3` is a spare.
fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers([N1, N2, N3]);
    config.nodes = NODES.to_vec();
    config.matchmakers = vec![M0, M1, M2];
    config.matchmaker_pool = vec![M0, M1, M2, M3];
    config
}

/// The records that the journal holds after each stage.
const RECORDS: [&str; 10] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
];

fn main() {
    let mut cluster = Cluster::new(NODES.map(config));

    start_small(&mut cluster);
    let leader = survive_a_crash(&mut cluster);
    grow_to_five(&mut cluster, leader);
    grow_to_a_grid(&mut cluster, leader);
    add_proxies_and_replicas(&mut cluster, leader);
    hand_over_the_matchmakers(&mut cluster, leader);
    let leader = hand_over_the_leadership(&mut cluster, leader);
    roll_a_segment(&mut cluster, leader);
    crash_in_the_full_deployment(&mut cluster, leader);

    println!("\neach node and each replica serves the same ten records");
}

// ---- 1. three nodes ---------------------------------------------------------

/// Stage 1: an election is Phase 1, one time for the whole log. The journal
/// judges each command when its slot is applied.
fn start_small(cluster: &mut Cluster) {
    println!("== 1. three nodes, a majority, three matchmakers");
    // A driver campaigns when its election timeout runs out
    // (`set_election_timeout`, `tick`). An example calls `campaign`.
    cluster.trace = true;
    cluster.node(N1).campaign();
    cluster.settle();
    cluster.trace = false;
    assert!(cluster.node(N1).is_leader());
    assert_eq!(cluster.node(N1).acceptors(), &majority(&[N1, N2, N3]));
    println!(
        "   node 1 registered its ballot at the matchmakers, then leads at {}",
        cluster.node(N1).ballot()
    );
    // The promise was on each disk before the `Promise` left the node.
    for id in [N1, N2, N3] {
        assert_eq!(
            cluster.disk(id).hard_state().max_promised_ballot,
            cluster.node(N1).ballot()
        );
    }
    cluster.tick();
    assert_eq!(cluster.node(N2).leader(), Some(N1));

    // A write before the journal has a writer is chosen, then refused.
    let slot = cluster.write(N1, WRITER, 0, "too early");
    assert!(matches!(
        cluster.outcome(N1, slot),
        Some(Outcome::Refused(view)) if view.leader.is_none()
    ));
    println!("   slot {slot}: Write \"too early\" -> refused, the journal has no writer");

    // `SetLeader` is a compare-and-set on the writer of the journal. The
    // writer is a client; the Paxos leader is a node.
    let slot = cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );
    assert!(matches!(
        cluster.outcome(N1, slot),
        Some(Outcome::Leader(view)) if view.leader == Some(WRITER)
    ));
    println!("   slot {slot}: SetLeader -> the client is the writer");

    write(cluster, N1, 0);
    write(cluster, N1, 1);

    // A retry of an accepted write is acked and moves nothing. A write must
    // ask for the next position. Another uuid is fenced.
    let slot = cluster.write(N1, WRITER, 1, RECORDS[1]);
    assert_eq!(
        cluster.outcome(N1, slot),
        Some(&Outcome::Duplicate {
            seq: Seq(1),
            count: 1
        })
    );
    println!("   slot {slot}: the same write again -> duplicate, nothing moved");
    let slot = cluster.write(N1, WRITER, 5, "too far");
    assert!(matches!(
        cluster.outcome(N1, slot),
        Some(Outcome::Refused(view)) if view.next_seq == Seq(2)
    ));
    println!("   slot {slot}: a write at position 5 -> refused, the next position is 2");
    let slot = cluster.write(N1, LeaderUuid(0xB0B), 2, "intruder");
    assert!(matches!(
        cluster.outcome(N1, slot),
        Some(Outcome::Refused(_))
    ));
    println!("   slot {slot}: a write by another uuid -> refused");

    // Only the leader admits a proposal.
    let entry = paros_consensus::journal::Entry {
        leader: WRITER,
        seq: Seq(2),
        records: vec![Value::from(b"charlie".to_vec())],
    };
    assert_eq!(
        cluster.node(N2).propose(entry),
        ProposeResult::NotLeader(Some(N1))
    );
    println!("   node 2 refuses a proposal: NotLeader, try node 1");
    check(cluster, &[N1, N2, N3], 2);
}

// ---- 2. a crash on three nodes ----------------------------------------------

/// Stage 2: the leader crashes with a slot open. The new leader proposes
/// again each value that a promise reported (P2c), and fills each slot that
/// nobody reported with a `Noop`.
fn survive_a_crash(cluster: &mut Cluster) -> NodeId {
    println!("\n== 2. node 1 crashes with a slot open; node 2 recovers the log");
    // The client sends "charlie", hears nothing, and sends it again. The
    // first `Accept` is lost. The second one reaches node 2 only.
    let lost = cluster.propose(N1, WRITER, 2, RECORDS[2]);
    let retry = cluster.propose(N1, WRITER, 2, RECORDS[2]);
    cluster.settle_where(|to, message| match message {
        Message::Accept { slot, .. } => *slot == retry && to == Party::Node(N2),
        Message::Commit { .. } => false,
        _ => true,
    });
    assert!(cluster.node(N2).acceptor().record(retry).is_some());
    assert!(cluster.node(N2).acceptor().record(lost).is_none());
    assert!(cluster.node(N3).acceptor().record(lost).is_none());
    let first = cluster.node(N1).ballot();
    cluster.crash(N1);
    println!(
        "   slot {lost}: on node 1 only. slot {retry}: on nodes 1 and 2, and nobody knows it is chosen"
    );

    cluster.elect(N2);
    let second = cluster.node(N2).ballot();
    assert!(second > first, "a new leadership is a higher ballot");
    for id in [N2, N3] {
        assert_eq!(
            cluster.node(id).chosen_at(lost),
            Some(&Command::Control(Control::Noop))
        );
        assert_eq!(
            cluster.outcome(id, retry),
            Some(&Outcome::Accepted {
                seq: Seq(2),
                count: 1
            })
        );
    }
    println!("   node 2 leads at {second}");
    println!("   slot {lost}: nobody reported a value -> a Noop");
    println!(
        "   slot {retry}: node 2 reported \"charlie\" -> proposed again, accepted at position 2"
    );

    // Node 1 boots as a follower: leadership is never durable. Its disk
    // still holds "charlie" at the slot of the `Noop`. The catch-up makes
    // its record agree with the log it serves.
    cluster.restart(N1);
    assert!(!cluster.node(N1).is_leader());
    cluster.tick();
    let (_, command) = cluster.disk(N1).accepted(lost).expect("a record");
    assert_eq!(command, Command::Control(Control::Noop));
    println!("   node 1 restarts as a follower; its record of slot {lost} becomes the Noop");
    check(cluster, &[N1, N2, N3], 3);
    N2
}

// ---- 3 and 4. more acceptors, then a grid ------------------------------------

/// Stage 3: a reconfiguration is a change of round.
fn grow_to_five(cluster: &mut Cluster, leader: NodeId) {
    println!("\n== 3. the journal grows to five acceptors");
    // A plain deployment refuses: it has no matchmakers to register at.
    let plain = Config::new(N1, JournalIdentifier::UNSET).with_peers([N1]);
    let mut alone = ColocatedNode::new(&MemStorage::new(plain), Limits::DEFAULT);
    alone.campaign();
    assert_eq!(
        alone.reconfigure(&majority(&[N1, N2])),
        ReconfigureResult::Refused(ReconfigureRefusal::NoMatchmakers)
    );
    println!("   plain Multi-Paxos: reconfigure -> refused, no matchmakers");

    let five = majority(&[N1, N2, N3, N4, N5]);
    let before = [N3, N4, N5].map(|id| cluster.received(id, "Prepare"));
    let ballot = reconfigure(cluster, leader, &five);
    // Phase 1 asked the old configuration and the new one: a value chosen
    // under {1, 2, 3} is seen by the new ballot.
    for (id, before) in [N3, N4, N5].into_iter().zip(before) {
        assert!(
            cluster.received(id, "Prepare") > before,
            "node {id} got a Prepare of the new ballot"
        );
    }
    assert_eq!(cluster.received(N6, "Prepare"), 0, "node 6 is in neither");
    println!(
        "   leads at {ballot} under {{1, 2, 3, 4, 5}}; Phase 1 reached the old and the new acceptors"
    );
    write(cluster, leader, 3);
    check(cluster, &[N1, N2, N3, N4, N5], 4);
}

/// Stage 4: a grid. A full column decides a slot, one acceptor of each
/// column elects, and a full row reads.
fn grow_to_a_grid(cluster: &mut Cluster, leader: NodeId) {
    println!("\n== 4. the journal grows to a grid of six");
    let grid = AcceptorConfig::new(NODES.to_vec(), GRID);
    let ballot = reconfigure(cluster, leader, &grid);
    println!("   leads at {ballot} under the grid {{1, 2, 3}} / {{4, 5, 6}}");

    cluster.take_sent();
    let slot = write(cluster, leader, 4);
    let sent = cluster.take_sent();
    // A column has two acceptors: one `Accept` to the other member of the
    // leader's column, or two to another column. Never one to each acceptor.
    assert!(
        sent["Accept"] <= 2,
        "one column votes, not the six acceptors"
    );
    println!(
        "   one column voted: the leader sent {} Accept, not 5",
        sent["Accept"]
    );

    // A leaderless read: a row answers with its vote watermarks, and the
    // reader waits until the maximum is chosen. A row meets every column, so
    // it sees each chosen slot.
    cluster.tick();
    assert_eq!(
        cluster.node(N3).quorum_read_in(1, Some(0)),
        ReadOpen::Opened
    );
    cluster.settle();
    let reads = cluster.take_reads(N3);
    assert_eq!(
        reads,
        [ReadState {
            ctx: 1,
            index: Some(slot)
        }]
    );
    println!("   node 3 reads through row {{1, 2, 3}}: the log is chosen up to slot {slot}");
    check(cluster, &NODES, 5);
}

// ---- 5. proxy leaders and replicas -------------------------------------------

/// Stage 5: the counts of proxy leaders and replicas are part of the
/// configuration, so adding them is a reconfiguration too.
fn add_proxies_and_replicas(cluster: &mut Cluster, leader: NodeId) {
    println!("\n== 5. two proxy leaders and two replicas join");
    // The driver starts the processes first. A proxy leader has no disk; a
    // replica starts with an empty one.
    cluster.restart_proxy(ProxyId(0));
    cluster.restart_proxy(ProxyId(1));
    cluster.add_replicas([R10, R11]);
    let counted = AcceptorConfig::new(NODES.to_vec(), GRID).with_counts(2, 2);
    let ballot = reconfigure(cluster, leader, &counted);
    println!("   leads at {ballot}, with 2 proxy leaders and 2 replicas");

    // The leader gives the slot to a proxy leader, and the proxy leader
    // sends the `Accept`s, counts the votes and sends the `Commit`.
    cluster.take_sent();
    let slot = write(cluster, leader, 5);
    let proxy = cluster.proxy(ProxyId::of(slot, 2).expect("two proxies"));
    assert!(
        proxy.counters().decided > 0,
        "the proxy leader decided the slot"
    );
    let sent = cluster.take_sent();
    assert_eq!(
        sent.get("Accept"),
        Some(&1),
        "the leader sends one Accept, to the proxy leader"
    );
    assert_eq!(
        cluster.node(leader).delegated_rounds(),
        [],
        "the round came back"
    );
    println!("   slot {slot}: the leader sent one Accept, to a proxy leader, which ran Phase 2");

    // A replica that joins late learns the whole log from the beat.
    cluster.tick();
    for id in [R10, R11] {
        assert_eq!(cluster.replica_journal(id), RECORDS[..6]);
    }
    println!("   replicas 10 and 11 caught up from the beat and serve the journal");
    check(cluster, &NODES, 6);
}

// ---- 6. the matchmakers ------------------------------------------------------

/// Stage 6: the matchmaker set is a generation. The next one is chosen by
/// single-decree Paxos among the current members: stop, bootstrap, decide,
/// publish.
fn hand_over_the_matchmakers(cluster: &mut Cluster, leader: NodeId) {
    println!("\n== 6. the matchmakers {{m0, m1, m2}} hand over to {{m1, m2, m3}}");
    let current = cluster
        .node(leader)
        .matchmaker_set()
        .expect("a set")
        .clone();
    // The reconfigurer runs on any machine. `0` is the decree round on its
    // disk: none yet.
    let mut reconfigurer = MatchmakerReconfigurer::new(leader, 0, Limits::DEFAULT);
    let mut round_on_disk = 0;
    reconfigurer
        .start(&current, vec![M1, M2, M3])
        .expect("no handover runs");
    let mut successor: Option<MatchmakerSet> = None;
    while reconfigurer.is_busy() {
        let ready = reconfigurer.ready();
        // Persist before send: the decree round is what a reconfigurer keeps.
        if let Some(round) = ready.round() {
            round_on_disk = round;
        }
        let requests = ready.requests().to_vec();
        ready.advance();
        for (to, request) in requests {
            for reply in cluster.matchmaker_reconfigure(to, request) {
                if let ReconfigurerStep::Done { successor: chosen } = reconfigurer.on_reply(reply) {
                    successor = Some(chosen);
                }
            }
        }
        // The driver closes the stop when a majority answered.
        let _ = reconfigurer.close_stop();
    }
    let successor = successor.expect("the handover is done");
    assert!(round_on_disk > 0);
    assert_eq!(successor.members(), [M1, M2, M3]);
    for id in [M1, M2, M3] {
        assert_eq!(cluster.matchmaker(id).phase(), MatchmakerPhase::Active);
    }
    assert_eq!(cluster.matchmaker(M0).phase(), MatchmakerPhase::Stopped);
    assert_eq!(cluster.matchmaker(M0).successor(), Some(&successor));
    println!("   m1, m2 and m3 serve the new generation; m0 stopped and names its successor");

    // The driver of the handover tells the leader. A node that still
    // believes the old set learns the new one from the refusal of m0.
    assert!(cluster.node(leader).learn_matchmakers(&successor));
    write(cluster, leader, 6);
    check(cluster, &NODES, 7);
}

// ---- 7. the leadership -------------------------------------------------------

/// Stage 7: a cooperative handoff. The leader stops first and names its
/// successor; the successor leads at the same ballot, with no Phase 1.
fn hand_over_the_leadership(cluster: &mut Cluster, leader: NodeId) -> NodeId {
    let next = N4;
    println!("\n== 7. node {leader} hands the leadership to node {next}");
    let ballot = cluster.node(leader).ballot();
    cluster.take_sent();
    let handoff = cluster
        .node(leader)
        .relinquish_to(next)
        .expect("a settled leader hands off");
    assert_eq!(handoff.ballot, ballot);
    cluster.settle();
    assert!(cluster.node(next).is_leader());
    assert!(!cluster.node(leader).is_leader());
    assert_eq!(cluster.node(next).ballot(), ballot, "the same ballot");
    assert!(!cluster.take_sent().contains_key("Prepare"), "no Phase 1");
    println!("   node {next} leads at {ballot}, the same ballot, and no Prepare was sent");

    // One hop: only the node that won a ballot in an election may hand it on.
    assert!(cluster.node(next).relinquish_to(N5).is_none());
    println!("   node {next} may not hand the same ballot on (one hop)");
    cluster.tick();
    write(cluster, next, 7);
    check(cluster, &NODES, 8);
    next
}

// ---- 8. segments -------------------------------------------------------------

/// Stage 8: a `Roll` is a command of the log. When it is chosen, it ends the
/// tail segment and opens the next one at the next slot.
fn roll_a_segment(cluster: &mut Cluster, leader: NodeId) {
    println!("\n== 8. node {leader} cuts the log into a new segment");
    let replicas = ReplicaSet::new([N4, N5, N6]);
    let RollResult::Accepted { slot, next } = cluster.node(leader).roll(Some(replicas.clone()))
    else {
        panic!("the leader of a matchmaker deployment admits a roll");
    };
    assert_eq!(next, SegmentId(1));
    cluster.settle();
    let opened = Segment {
        id: SegmentId(1),
        start: Slot(slot.0 + 1),
    };
    for id in NODES {
        assert_eq!(cluster.outcome(id, slot), Some(&Outcome::Rolled(opened)));
        assert_eq!(cluster.node(id).replica().journal().segment, opened);
    }
    println!(
        "   slot {slot}: Roll -> segment 1 opens at slot {} on {{4, 5, 6}}",
        opened.start
    );

    // Each node publishes the end of a segment after its own fold applied
    // the `Roll`. The matchmakers keep the chain, an add-only map.
    let journal = cluster.journal_id(leader);
    let chain = cluster
        .matchmaker(M1)
        .chain(journal)
        .expect("the matchmakers hold the chain")
        .clone();
    for id in [M2, M3] {
        assert_eq!(cluster.matchmaker(id).chain(journal), Some(&chain));
    }
    assert_eq!(chain[&SegmentId(0)].end, Some(opened.start));
    assert_eq!(
        chain[&SegmentId(1)]
            .opening
            .as_ref()
            .map(|o| o.replicas.clone()),
        Some(Some(replicas))
    );
    println!(
        "   m1, m2 and m3 hold the chain: segment 0 ends at {}, segment 1 is open",
        opened.start
    );
    // The roll took a slot and no position: the next write is position 8.
    write(cluster, leader, 8);
    check(cluster, &NODES, 9);
}

// ---- 9. a crash in the full deployment ----------------------------------------

/// Stage 9: the leader crashes. A new leader is elected through the grid,
/// and garbage collection lets the matchmakers forget the configurations
/// that no future leader needs.
fn crash_in_the_full_deployment(cluster: &mut Cluster, leader: NodeId) {
    println!("\n== 9. node {leader} crashes; node 1 takes over");
    let old = cluster.node(leader).ballot();
    cluster.crash(leader);
    cluster.elect(N1);
    let new = cluster.node(N1).ballot();
    assert!(new > old);
    assert_eq!(
        cluster.node(N1).replica().journal().segment.id,
        SegmentId(1)
    );
    println!("   node 1 leads at {new}, in segment 1");

    // The beats bring back the chosen index of each acceptor. When a
    // Phase-2 quorum of the configuration in force holds the chosen prefix,
    // the leader raises the watermark at the matchmakers.
    cluster.tick();
    let (floor, _) = cluster
        .node(N1)
        .gc_effective()
        .expect("an effective watermark");
    assert_eq!(floor, new);
    let journal = cluster.journal_id(N1);
    for id in [M1, M2, M3] {
        let registry = cluster.matchmaker(id).registry(journal);
        assert!(registry.keys().all(|ballot: &Ballot| *ballot >= floor));
    }
    println!("   the matchmakers forget every configuration below {floor}");

    cluster.restart(leader);
    cluster.tick();
    let last = write_and_check(cluster, N1);
    println!("   node {leader} restarts as a follower; slot {last}: the last record");
}

// ---- helpers -----------------------------------------------------------------

/// A majority configuration of `members`.
fn majority(members: &[NodeId]) -> AcceptorConfig {
    AcceptorConfig::new(members.to_vec(), QuorumSystem::Majority)
}

/// Move the journal to `config`: a new ballot, registered at the matchmakers,
/// with Phase 1 over every configuration they report. Returns the ballot.
fn reconfigure(cluster: &mut Cluster, leader: NodeId, config: &AcceptorConfig) -> Ballot {
    let old = cluster.node(leader).ballot();
    let ReconfigureResult::Started(ballot) = cluster.node(leader).reconfigure(config) else {
        panic!("a settled leader with matchmakers starts a reconfiguration");
    };
    assert!(ballot > old, "a new configuration is a new ballot");
    // The leader is a candidate until it leads at the new ballot.
    assert!(!cluster.node(leader).is_leader());
    cluster.settle();
    assert!(cluster.node(leader).is_leader());
    assert_eq!(cluster.node(leader).ballot(), ballot);
    assert_eq!(cluster.node(leader).acceptors(), config);
    cluster.tick();
    ballot
}

/// Append `RECORDS[seq]` at position `seq` through `leader`, and check that
/// the journal took it.
fn write(cluster: &mut Cluster, leader: NodeId, seq: usize) -> Slot {
    let position = u64::try_from(seq).expect("a small position");
    let slot = cluster.write(leader, WRITER, position, RECORDS[seq]);
    assert_eq!(
        cluster.outcome(leader, slot),
        Some(&Outcome::Accepted {
            seq: Seq(position),
            count: 1
        })
    );
    println!(
        "   slot {slot}: Write {:?} -> accepted at position {seq}",
        RECORDS[seq]
    );
    slot
}

/// The last write, then the check of every node and replica.
fn write_and_check(cluster: &mut Cluster, leader: NodeId) -> Slot {
    let slot = write(cluster, leader, RECORDS.len() - 1);
    cluster.tick();
    check(cluster, &NODES, RECORDS.len());
    for id in [R10, R11] {
        assert_eq!(cluster.replica_journal(id), RECORDS);
    }
    slot
}

/// Each node of `ids` serves the first `count` records.
fn check(cluster: &mut Cluster, ids: &[NodeId], count: usize) {
    cluster.tick();
    for id in ids {
        assert_eq!(cluster.journal(*id), RECORDS[..count], "node {id}");
    }
}
