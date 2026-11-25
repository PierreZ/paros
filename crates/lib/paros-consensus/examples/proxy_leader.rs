//! **Proxy leaders: the leader orders the commands, and another process
//! sends them.**
//!
//! Run it: `cargo run -p paros-consensus --example proxy_leader`
//!
//! This example drives [`ColocatedNode`] and [`ProxyLeader`] with the toy
//! cluster of `examples/common/cluster.rs`. Read `quickstart` first, and
//! `acceptor_grid` for the grid.
//!
//! # The problem
//!
//! A Multi-Paxos leader does two jobs for each command. It gives the
//! command a slot, which is cheap. Then it sends an `Accept` to each
//! acceptor of the quorum, collects each `Accepted`, and sends a `Commit`
//! to each learner. The second job is most of the messages, and it grows
//! with the cluster. Only the first job must be done by the leader.
//!
//! # The technique (Compartmentalized Paxos, section 3.1)
//!
//! The leader gives the slot and then hands the `Accept` to a **proxy
//! leader**. The proxy leader sends it to the acceptors, counts the votes
//! and sends the `Commit`. A proxy leader decides nothing: it does not
//! vote, it does not order, and it holds nothing durable. To lose one costs
//! time, never safety.
//!
//! # The driver's part
//!
//! - `Config::proxy_count` names how many proxy leaders the deployment
//!   has. Zero is the plain deployment. [`ProxyLeader::new`] takes the same
//!   `Config` as a node.
//! - A proxy leader is driven like a node, with no disk: `step`, `tick`,
//!   `ready()`, send `messages()`, `advance()`.
//! - The core addresses the delegated `Accept` to `Audience::Proxy(id)`,
//!   and the driver sends it to the process that has this id. A proxy
//!   leader sends with `Audience::resolve_from_proxy`.
//! - `propose` delegates each slot to `ProxyId(slot % proxy_count)`.
//!   `propose_in` lets the driver choose: [`Delegation::Colocated`] keeps
//!   the round on the leader, and [`Delegation::To`] names a proxy leader.
//! - Liveness with a proxy leader that is down is a policy of the driver:
//!   on a beat it calls `resend_pending`, and `take_back_delegated(n)` to
//!   run on the leader each round that a proxy leader did not finish after
//!   `n` resends.
//!
//! # What the trace shows
//!
//! Six nodes in a grid of two rows and three columns, and two proxy
//! leaders. Node 1 leads.
//!
//! 1. Three records with the rounds on the leader: the cost of a plain
//!    leader.
//! 2. Three records with the rounds delegated. The leader sends one
//!    `Accept` and receives one `Commit`. It handles two more messages
//!    when it is an acceptor of the column of the slot.
//! 3. A proxy leader crashes with a round. The leader delegates the round
//!    again on each beat, then takes it back and runs it.
//! 4. The leader hands the leadership to node 4 while a delegated round is
//!    open. Node 4 delegates the round again, and the same proxy leader
//!    finishes it at the same ballot.
//!
//! Further reading: Whittaker et al., *Scaling Replicated State Machines
//! with Compartmentalization* (2021), section 3.1, and
//! `specs/tla/proxy-leader/` for the model.

mod common;

use paros_consensus::contract::Party;
use paros_consensus::journal::{Control, Entry, JournalIdentifier, LeaderUuid, Seq};
use paros_consensus::membership::{ProxyId, QuorumSystem};
use paros_consensus::node::{Delegation, ProposeResult};
use paros_consensus::{Config, Message, NodeId, ProxyLeader, Slot, Value};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N4: NodeId = NodeId(4);

const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// How many resends the driver gives a proxy leader before the leader
/// takes a round back.
const TAKE_BACK_AFTER: u64 = 2;

/// Two rows of three, and two proxy leaders. The columns are `{0, 3}`,
/// `{1, 4}` and `{2, 5}`: the leader, node 1, is an acceptor of column 1.
fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers((0..6).map(NodeId));
    config.quorum_system = QuorumSystem::grid(2, 3);
    config.proxy_count = 2;
    config
}

fn entry(seq: u64, text: &str) -> Entry {
    Entry {
        leader: WRITER,
        seq: Seq(seq),
        records: vec![Value::from(text.as_bytes().to_vec())],
    }
}

/// Propose one record on the leader `id` with the delegation that the
/// driver chose. Nothing is delivered.
fn propose(cluster: &mut Cluster, id: NodeId, seq: u64, text: &str, how: Delegation) -> Slot {
    let result = cluster.node(id).propose_in(entry(seq, text), None, how);
    let ProposeResult::Accepted(slot) = result else {
        panic!("node {id} is the leader: {result:?}");
    };
    slot
}

fn main() {
    let mut cluster = Cluster::new((0..6).map(NodeId).map(config));
    cluster.elect(N1);
    cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );

    let colocated = colocated(&mut cluster);
    let delegated = delegated(&mut cluster);
    assert!(delegated < colocated);
    println!("   the leader handled {delegated} messages, and {colocated} before");

    a_dead_proxy(&mut cluster);
    a_handoff(&mut cluster);
    let journal = cluster.journal(N4);
    assert_eq!(journal.len(), 8);
    for id in cluster.running() {
        assert_eq!(cluster.journal(id), journal);
    }
    println!("\neach node serves the same {} records", journal.len());
}

/// Part 1: what the leader sends and receives when it runs each round.
fn colocated(cluster: &mut Cluster) -> usize {
    println!("== 1. three records, each round on the leader");
    let mut total = 0;
    for (seq, text) in [(0, "alpha"), (1, "bravo"), (2, "charlie")] {
        cluster.take_traffic(N1);
        let slot = propose(cluster, N1, seq, text, Delegation::Colocated);
        cluster.settle();
        let cost = cluster.take_traffic(N1);
        println!("   slot {slot}: the leader handled {cost} messages");
        total += cost;
    }
    assert_eq!(cluster.node(N1).delegated_rounds(), []);
    total
}

/// Part 2: the same work with the proxy leaders.
fn delegated(cluster: &mut Cluster) -> usize {
    println!("\n== 2. three records, each round delegated");
    let mut total = 0;
    for (seq, text) in [(3, "delta"), (4, "echo"), (5, "foxtrot")] {
        cluster.take_traffic(N1);
        cluster.trace = seq == 3;
        let slot = propose(cluster, N1, seq, text, Delegation::Auto);
        cluster.settle();
        cluster.trace = false;
        let cost = cluster.take_traffic(N1);
        let proxy = ProxyId::of(slot, 2).expect("the deployment has proxy leaders");
        let acceptors = cluster.node(N1).acceptors().clone();
        if acceptors.column_of(slot) == Some(1) {
            // The leader is also an acceptor of this column: it gets the
            // `Accept` of the proxy leader and answers it, as node 4 does.
            assert_eq!(cost, 4);
        } else {
            assert_eq!(cost, 2, "one Accept out, one Commit in");
        }
        println!(
            "   slot {slot} by proxy {}: the leader handled {cost} messages",
            proxy.0
        );
        total += cost;
    }
    let proxy: &ProxyLeader = cluster.proxy(ProxyId(0));
    assert!(proxy.counters().decided > 0);
    total
}

/// Part 3: the leader does not wait for ever on a proxy leader.
fn a_dead_proxy(cluster: &mut Cluster) {
    println!("\n== 3. proxy 1 crashes with a round");
    let slot = propose(cluster, N1, 6, "golf", Delegation::To(ProxyId(1)));
    cluster.crash_proxy(ProxyId(1));
    cluster.settle();
    assert_eq!(cluster.node(N1).delegated_rounds(), [(slot, ProxyId(1))]);
    assert!(cluster.node(N1).chosen_at(slot).is_none());
    println!("   slot {slot} is delegated to proxy 1, and nothing is chosen");

    // The beat of a driver: time, then its two policies.
    let mut beats = 0;
    cluster.take_sent();
    while !cluster.node(N1).delegated_rounds().is_empty() {
        cluster.node(N1).tick();
        cluster.node(N1).resend_pending();
        cluster.node(N1).take_back_delegated(TAKE_BACK_AFTER);
        cluster.settle();
        beats += 1;
        let sent = cluster.take_sent();
        if !cluster.node(N1).delegated_rounds().is_empty() {
            assert_eq!(sent.get("Accept"), Some(&1), "the round is delegated again");
        }
    }
    assert_eq!(beats, TAKE_BACK_AFTER);
    assert!(cluster.node(N4).chosen_at(slot).is_some());
    println!("   after {beats} beats the leader took slot {slot} back, and it is chosen");

    // A proxy leader that starts again is empty, and that is enough.
    cluster.restart_proxy(ProxyId(1));
}

/// Part 4: a delegated round crosses a leader handoff.
fn a_handoff(cluster: &mut Cluster) {
    println!("\n== 4. node 1 hands the leadership to node 4 with a round open");
    let slot = propose(cluster, N1, 7, "hotel", Delegation::Auto);
    let proxy = ProxyId::of(slot, 2).expect("the deployment has proxy leaders");
    // The proxy leader gets the round. Its `Accept`s to the column are
    // lost, so only a new delegation can finish the round.
    cluster.settle_where(|to, _| !matches!(to, Party::Node(_)));
    assert_eq!(cluster.proxy(proxy).delegator(slot), Some(N1));
    println!("   proxy {} holds slot {slot} for node 1", proxy.0);

    let ballot = cluster.node(N1).ballot();
    let receipt = cluster
        .node(N1)
        .relinquish_to(N4)
        .expect("a settled, elected leader may relinquish");
    assert_eq!(
        receipt.pending, 1,
        "the open round goes with the leadership"
    );
    cluster.settle_holding(|_, message| !matches!(message, Message::Relinquish { .. }));
    assert!(cluster.node(N4).is_leader());
    assert_eq!(
        cluster.node(N4).delegated_rounds(),
        [(slot, proxy)],
        "node 4 delegated the round again, to the proxy leader of the slot"
    );

    cluster.settle();
    assert_eq!(cluster.node(N4).ballot(), ballot, "the same ballot");
    assert_eq!(cluster.proxy(proxy).delegator(slot), None);
    for id in (0..6).map(NodeId) {
        assert!(cluster.node(id).chosen_at(slot).is_some());
    }
    println!(
        "   node 4 leads at ballot {ballot}, and proxy {} finished slot {slot}",
        proxy.0
    );
}
