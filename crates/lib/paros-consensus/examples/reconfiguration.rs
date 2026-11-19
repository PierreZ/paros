//! **Reconfiguration: replace two acceptors of a running journal.**
//!
//! Run it: `cargo run -p paros-consensus --example reconfiguration`
//!
//! The lesson `matchmaker` teaches Matchmaker Paxos with the roles driven by
//! hand. This example runs the same protocol the way a driver does: five
//! [`ColocatedNode`]s, three [`paros_consensus::Matchmaker`]s, and the toy
//! cell of `examples/common/cluster.rs`. Read `quickstart` first.
//!
//! # What a matchmaker deployment adds to the driver
//!
//! A node with `Config::matchmakers` set speaks a second, separate service:
//!
//! - `Ready::match_requests()` are the registrations and probes to send to
//!   the matchmakers, after the writes of the batch are durable. Each answer
//!   comes back through `ColocatedNode::on_match_reply`.
//! - `Ready::gc_requests()` ask the matchmakers to forget old
//!   configurations. Each answer comes back through
//!   `ColocatedNode::on_gc_ack`.
//!
//! A matchmaker is driven like a node: `step(request)`, then `ready()`,
//! persist `writes()`, send `replies()`, `advance()`. Both loops are in
//! `examples/common/cluster.rs`.
//!
//! Plain Multi-Paxos has none of this: with `Config::matchmakers` empty, the
//! two buckets are always empty and `reconfigure` is refused.
//!
//! # The three ideas
//!
//! - **A configuration belongs to a ballot.** The set of acceptors is never
//!   edited in place. A reconfiguration is a new ballot that is registered
//!   with a new configuration.
//! - **Register, then Phase 1.** Before its `Prepare`, a candidate registers
//!   `(ballot, configuration)` at a quorum of matchmakers. The answers
//!   contain each configuration that an earlier ballot registered. Phase 1
//!   must then get a quorum of promises from **each** of them, because a
//!   value may be chosen in any of them. Phase 2 uses the new configuration
//!   only.
//! - **Removed is not shut down.** An acceptor that left the configuration
//!   must keep its records until no future leader can need them. The leader
//!   proves that with garbage collection: once a Phase-2 quorum of the new
//!   configuration holds the chosen prefix, it raises a watermark at the
//!   matchmakers, and the configurations below the watermark are forgotten.
//!   Only then may an operator retire the old acceptors.
//!
//! # What the trace shows
//!
//! 1. Node 1 becomes the leader of `{1, 2, 3}`. Its first campaign asks the
//!    matchmakers which configuration is in force, registers its ballot,
//!    and only then runs Phase 1.
//! 2. Plain Multi-Paxos refuses a reconfiguration. This deployment starts
//!    one: `{1, 2, 3}` becomes `{1, 4, 5}`. Phase 1 reaches the old and the
//!    new acceptors.
//! 3. A write is accepted by `{1, 4, 5}` only. Nodes 2 and 3 still learn it.
//! 4. The leader collects: the matchmakers forget the old configuration.
//! 5. Node 2 checks with each matchmaker that no registration names it, and
//!    may retire.
//!
//! Further reading: Whittaker et al., *Matchmaker Paxos: A Reconfigurable
//! Consensus Protocol* (2020), and `specs/tla/matchmaker/` for the model.

mod common;

use paros_consensus::journal::{Control, JournalIdentifier, LeaderUuid};
use paros_consensus::membership::{AcceptorConfig, MatchmakerId, QuorumSystem};
use paros_consensus::node::{ReconfigureRefusal, ReconfigureResult};
use paros_consensus::{Ballot, Config, NodeId};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);
const N5: NodeId = NodeId(5);

const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// The configuration of node `id`. `peers` is the bootstrap configuration,
/// `nodes` is each node that may ever be an acceptor, and `matchmakers`
/// turns the deployment into Matchmaker Paxos.
fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers([N1, N2, N3]);
    config.nodes = vec![N1, N2, N3, N4, N5];
    config.matchmakers = (0..3).map(MatchmakerId).collect();
    config
}

fn acceptors(members: [NodeId; 3]) -> AcceptorConfig {
    AcceptorConfig::new(members.to_vec(), QuorumSystem::Majority)
}

fn main() {
    let mut cluster = Cluster::new([N1, N2, N3, N4, N5].map(config)).with_matchmakers(3);

    first_leader(&mut cluster);
    let ballot = reconfigure(&mut cluster);
    write_under_the_new_configuration(&mut cluster);
    collect(&mut cluster, ballot);
    retire(&mut cluster, ballot);
}

/// Part 1: matchmaking comes before Phase 1.
fn first_leader(cluster: &mut Cluster) {
    println!("== 1. node 1 becomes the leader of {{1, 2, 3}}");
    cluster.trace = true;
    cluster.node(N1).campaign();
    cluster.settle();
    cluster.trace = false;
    assert!(cluster.node(N1).is_leader());
    assert_eq!(
        cluster.node(N1).acceptors(),
        &acceptors([N1, N2, N3]),
        "the bootstrap configuration is in force"
    );
    println!("   node 1 leads at ballot {}", cluster.node(N1).ballot());
    cluster.tick();

    cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );
    cluster.write(N1, WRITER, 0, "alpha");
    println!("   \"alpha\" is written under {{1, 2, 3}}");
}

/// Part 2: a reconfiguration is a change of round.
fn reconfigure(cluster: &mut Cluster) -> Ballot {
    println!("\n== 2. the leader moves the journal to {{1, 4, 5}}");

    // The refusals are results, never panics.
    let plain = Config::new(N1, JournalIdentifier::UNSET).with_peers([N1]);
    let mut alone = Cluster::new([plain]);
    alone.elect(N1);
    assert_eq!(
        alone.node(N1).reconfigure(&acceptors([N1, N4, N5])),
        ReconfigureResult::Refused(ReconfigureRefusal::NoMatchmakers)
    );
    println!("   plain Multi-Paxos answers Refused(NoMatchmakers)");
    assert_eq!(
        cluster.node(N2).reconfigure(&acceptors([N1, N4, N5])),
        ReconfigureResult::NotLeader(Some(N1))
    );
    println!("   a follower answers NotLeader, try node 1");

    let old = cluster.node(N1).ballot();
    let ReconfigureResult::Started(ballot) = cluster.node(N1).reconfigure(&acceptors([N1, N4, N5]))
    else {
        panic!("a settled leader with matchmakers starts the reconfiguration");
    };
    assert!(ballot > old, "a new configuration is a new ballot");
    // The leader is a candidate again until it leads at the new ballot.
    assert!(!cluster.node(N1).is_leader());
    println!("   reconfigure -> Started at ballot {ballot}");

    cluster.trace = true;
    cluster.settle();
    cluster.trace = false;
    assert!(cluster.node(N1).is_leader());
    assert_eq!(cluster.node(N1).ballot(), ballot);
    assert_eq!(cluster.node(N1).acceptors(), &acceptors([N1, N4, N5]));
    println!("   node 1 leads at ballot {ballot} under {{1, 4, 5}}");
    ballot
}

/// Part 3: Phase 2 uses the configuration of the ballot, and no other.
fn write_under_the_new_configuration(cluster: &mut Cluster) {
    println!("\n== 3. a write under {{1, 4, 5}}");
    cluster.trace = true;
    let entry_slot = cluster.write(N1, WRITER, 1, "bravo");
    cluster.trace = false;
    for id in [N1, N2, N3, N4, N5] {
        assert!(
            cluster.node(id).replica().is_chosen(entry_slot),
            "node {id} learned the slot"
        );
    }
    for id in [N2, N3] {
        assert!(
            cluster.node(id).acceptor().record(entry_slot).is_some(),
            "node {id} keeps the chosen record that the Commit carried"
        );
        assert!(!cluster.node(id).is_acceptor());
    }
    assert_eq!(cluster.journal(N3), ["alpha", "bravo"]);
    println!("   nodes 4 and 5 voted; nodes 2 and 3 only learned the Commit");
}

/// Part 4: the leader proves that the old configuration is not needed.
fn collect(cluster: &mut Cluster, ballot: Ballot) {
    println!("\n== 4. the matchmakers forget {{1, 2, 3}}");
    let journal = cluster.journal_id(N1);
    // Each beat brings back the chosen index of each acceptor. When a
    // Phase-2 quorum of {1, 4, 5} holds the chosen prefix, the leader asks
    // the matchmakers to raise their watermark to its ballot.
    assert!(cluster.node(N1).gc_effective().is_none());
    cluster.trace = true;
    cluster.tick();
    cluster.trace = false;

    let (floor, retired) = cluster
        .node(N1)
        .gc_effective()
        .expect("a quorum of matchmakers acknowledged the watermark");
    assert_eq!(floor, ballot);
    assert_eq!(retired, [N2, N3]);
    for id in (0..3).map(MatchmakerId) {
        let matchmaker = cluster.matchmaker(id);
        assert_eq!(matchmaker.gc_watermark(journal), ballot);
        assert!(
            matchmaker.registry(journal).keys().all(|b| *b >= ballot),
            "no registration below the watermark is kept"
        );
    }
    println!("   the watermark is {floor}: nodes 2 and 3 may be retired");
}

/// Part 5: a removed node leaves only with the evidence.
fn retire(cluster: &mut Cluster, watermark: Ballot) {
    println!("\n== 5. node 2 retires");
    // The watermark is the evidence that an operator brings. It is not
    // enough alone: a registration above it can name the node again.
    assert!(!cluster.node(N2).may_retire(watermark));
    assert!(cluster.node(N2).retirement_unchecked(watermark));
    println!("   may_retire is false: no matchmaker confirmed it yet");

    // The node asks each matchmaker. Each one that agrees records the node
    // as retired before it answers, and refuses a later registration that
    // names it.
    assert!(cluster.node(N2).check_retirement(watermark));
    cluster.trace = true;
    cluster.settle();
    cluster.trace = false;
    assert!(cluster.node(N2).may_retire(watermark));
    println!("   may_retire is true: the driver may shut node 2 down");

    // A member of the configuration in force never retires.
    assert!(!cluster.node(N4).may_retire(watermark));
    cluster.crash(N2);
    cluster.write(N1, WRITER, 2, "charlie");
    assert_eq!(cluster.journal(N5), ["alpha", "bravo", "charlie"]);
    println!("   the journal goes on without node 2");
}
