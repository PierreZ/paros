//! **The matchmakers: the registry of the configurations, and the handover
//! of the matchmaker set.**
//!
//! Run it: `cargo run -p paros-consensus --example matchmaker`
//!
//! This example drives [`Matchmaker`] and [`MatchmakerReconfigurer`] with
//! the toy cluster of `examples/common/cluster.rs`. Read `quickstart` first.
//! The example `reconfiguration` changes the acceptors of a journal. This
//! example looks at the service that makes that change safe, and then
//! replaces a member of that service.
//!
//! # A matchmaker is a registry, not an acceptor
//!
//! When the set of acceptors can change, a new leader must ask a Phase-1
//! quorum of **each** configuration that can hold a chosen value. So it
//! must know which configurations there were. A **matchmaker** keeps that
//! list: a durable map from a ballot to the configuration of its acceptors.
//! It holds no log and it votes on no slot.
//!
//! A candidate **registers** its ballot and its configuration at a majority
//! of the matchmakers before it sends a `Prepare`. Each matchmaker writes
//! the pair to its disk and then answers with the pairs of the lower
//! ballots that it holds. Two majorities share a matchmaker, so the
//! candidate hears of each earlier configuration.
//!
//! # Who changes the matchmakers?
//!
//! The matchmakers are a small fixed set, and a machine of that set can
//! fail for good. A second level of matchmakers for the matchmakers would
//! only move the problem. The answer of the paper: the matchmaker set has a
//! **generation**, and the next generation is chosen by single-decree Paxos
//! whose acceptors are the matchmakers of the current one. It is the
//! [`paros_consensus::decree::Decree`] of the example `single_decree`.
//!
//! A **handover** has four steps, in this order:
//!
//! 1. **Stop.** A majority of the old set stops, durably: it registers
//!    nothing more, and it answers with its registry.
//! 2. **Bootstrap.** Each member of the new set stores the union of those
//!    registries, not yet in service.
//! 3. **Decide.** The decree over the old set chooses the new set. From
//!    here there is one successor.
//! 4. **Publish.** The old members record the successor, so that they can
//!    name it to a node that is late. The new members start to serve.
//!
//! # The driver's part
//!
//! - `Config::matchmakers` is the first set, and `Config::matchmaker_pool`
//!   is each matchmaker that can ever be in a set.
//! - A matchmaker is driven like a node: `step(request)`, `ready()`,
//!   persist `writes()`, send `replies()`, `advance()`. A restart is
//!   [`Matchmaker::new`] on its registry.
//! - A handover is driven by one [`MatchmakerReconfigurer`], on any
//!   machine. It is the same loop: `ready()`, persist `round()` when there
//!   is one, send `requests()`, `advance()`, and give each answer to
//!   `on_reply`. When a majority stopped, the driver calls `close_stop`.
//!   The function `handover` below is this loop.
//! - A node learns the new set from the refusal of a matchmaker, with no
//!   call of the driver. The driver that ran the handover can also tell
//!   its node: `ColocatedNode::learn_matchmakers`.
//!
//! # What the trace shows
//!
//! 1. Node 1 becomes the leader. Its registration is on the disk of a
//!    majority of `{m0, m1, m2}`, and a matchmaker that restarts still
//!    holds it.
//! 2. A handover replaces `m2` by `m3`. `m3` then holds the registration
//!    of node 1, although no node ever asked it.
//! 3. Node 1 crashes. Node 2 still believes the first set. A matchmaker
//!    refuses it and names the new set, and node 2 wins its election there.
//!
//! Further reading: Whittaker et al., *Matchmaker Paxos: A Reconfigurable
//! Consensus Protocol* (2020), section 6, and `specs/tla/handover/` for the
//! model.

mod common;

use paros_consensus::journal::{Control, JournalIdentifier, LeaderUuid};
use paros_consensus::matchmaker::{MatchmakerPhase, ReconfigurerStep};
use paros_consensus::membership::{MatchmakerId, MatchmakerSet};
use paros_consensus::{Ballot, Config, Limits, Matchmaker, MatchmakerReconfigurer, NodeId};

use common::cluster::Cluster;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

const M0: MatchmakerId = MatchmakerId(0);
const M1: MatchmakerId = MatchmakerId(1);
const M2: MatchmakerId = MatchmakerId(2);
const M3: MatchmakerId = MatchmakerId(3);

const WRITER: LeaderUuid = LeaderUuid(0xA11CE);

/// Three acceptors, the matchmakers `{m0, m1, m2}`, and one spare, `m3`.
fn config(id: NodeId) -> Config {
    let mut config = Config::new(id, JournalIdentifier::UNSET).with_peers([N1, N2, N3]);
    config.matchmakers = vec![M0, M1, M2];
    config.matchmaker_pool = vec![M0, M1, M2, M3];
    config
}

fn main() {
    let mut cluster = Cluster::new([N1, N2, N3].map(config));

    let first = the_registry(&mut cluster);
    let successor = handover(&mut cluster, first);
    a_late_node(&mut cluster, first, &successor);
}

/// Part 1: what a matchmaker holds.
fn the_registry(cluster: &mut Cluster) -> Ballot {
    println!("== 1. node 1 registers its ballot, then runs Phase 1");
    cluster.trace = true;
    cluster.node(N1).campaign();
    cluster.settle();
    cluster.trace = false;
    assert!(cluster.node(N1).is_leader());
    let ballot = cluster.node(N1).ballot();
    cluster.tick();
    cluster.control(
        N1,
        Control::SetLeader {
            new: WRITER,
            old: None,
        },
    );
    cluster.write(N1, WRITER, 0, "alpha");

    // The registry: the ballot of node 1, with the acceptors that its
    // Phase 2 uses. A majority of the matchmakers holds it.
    let journal = cluster.journal_id(N1);
    let acceptors = cluster.node(N1).acceptors().clone();
    let holders: Vec<MatchmakerId> = [M0, M1, M2]
        .into_iter()
        .filter(|id| {
            cluster
                .matchmaker(*id)
                .registry(journal)
                .contains_key(&ballot)
        })
        .collect();
    assert!(holders.len() >= 2);
    let registration = &cluster.matchmaker(holders[0]).registry(journal)[&ballot];
    assert_eq!(registration.config, acceptors);
    println!("   ballot {ballot} -> {{1, 2, 3}} is on the disk of a majority of the matchmakers");

    // The registration was durable before the answer left, so a restart
    // keeps it.
    cluster.restart_matchmaker(holders[0]);
    let matchmaker: &Matchmaker = cluster.matchmaker(holders[0]);
    assert!(matchmaker.registry(journal).contains_key(&ballot));
    println!("   a matchmaker restarts from its disk and still holds it");
    ballot
}

/// Part 2: the loop of the driver of a handover.
fn handover(cluster: &mut Cluster, first: Ballot) -> MatchmakerSet {
    println!("\n== 2. a handover: {{m0, m1, m2}} becomes {{m0, m1, m3}}");
    let current = cluster.node(N3).matchmaker_set().expect("a set").clone();
    // The reconfigurer runs on the machine of node 3. `0` is the decree
    // round that the disk of this machine holds: none yet.
    let mut reconfigurer = MatchmakerReconfigurer::new(N3, 0, Limits::DEFAULT);
    let mut decree_round_on_disk = 0;
    reconfigurer
        .start(&current, vec![M0, M1, M3])
        .expect("no handover runs");

    let mut successor = None;
    while reconfigurer.is_busy() {
        let ready = reconfigurer.ready();
        // Persist before send: the decree round is the one thing that a
        // reconfigurer keeps, so that a restart never uses a round again.
        if let Some(round) = ready.round() {
            decree_round_on_disk = round;
        }
        let requests = ready.requests().to_vec();
        ready.advance();
        assert!(!requests.is_empty(), "a handover that runs has requests");

        for (to, request) in requests {
            for reply in cluster.matchmaker_reconfigure(to, request) {
                match reconfigurer.on_reply(reply) {
                    ReconfigurerStep::Deciding { ballot } => {
                        println!("   each new member holds the registries");
                        println!("   the decree opens at ballot {ballot}");
                    }
                    ReconfigurerStep::Chosen { successor } => {
                        let members: Vec<u64> = successor.members().iter().map(|m| m.0).collect();
                        println!("   a majority of the old set voted: {members:?} is chosen");
                    }
                    ReconfigurerStep::Done { successor: chosen } => successor = Some(chosen),
                    _ => {}
                }
            }
        }
        // The driver closes the stop when a majority answered. An answer
        // that comes later is not needed.
        if reconfigurer.close_stop().is_some() {
            println!("   a majority of the old set stopped and gave its registry");
        }
    }
    let successor = successor.expect("the handover is done");
    assert!(decree_round_on_disk > 0);
    assert_eq!(successor.members(), [M0, M1, M3]);
    assert!(successor.generation > current.generation);

    for id in [M0, M1, M3] {
        assert_eq!(cluster.matchmaker(id).phase(), MatchmakerPhase::Active);
        assert_eq!(cluster.matchmaker(id).set(), &successor);
    }
    // The member that left stays up, only to name its successor.
    assert_eq!(cluster.matchmaker(M2).phase(), MatchmakerPhase::Stopped);
    assert_eq!(cluster.matchmaker(M2).successor(), Some(&successor));
    println!("   m0, m1 and m3 serve the new generation. m2 stopped and names its successor");

    // No node ever asked m3. It holds the registration of node 1 from the
    // bootstrap, so the next candidate still hears of that configuration.
    let journal = cluster.journal_id(N3);
    assert!(
        cluster
            .matchmaker(M3)
            .registry(journal)
            .contains_key(&first)
    );
    println!("   m3 holds ballot {first} -> {{1, 2, 3}}, from the bootstrap");

    // The driver of the handover tells its own node.
    assert!(cluster.node(N3).learn_matchmakers(&successor));
    successor
}

/// Part 3: a node that still believes the old set loses nothing.
fn a_late_node(cluster: &mut Cluster, first: Ballot, successor: &MatchmakerSet) {
    println!("\n== 3. node 1 crashes, and node 2 campaigns with the old set");
    cluster.crash(N1);
    assert_ne!(cluster.node(N2).matchmaker_set(), Some(successor));

    // The old generation refuses the registration and names the new set.
    // The node adopts it and stops this campaign.
    cluster.trace = true;
    cluster.node(N2).campaign();
    cluster.settle();
    cluster.trace = false;
    assert!(!cluster.node(N2).is_leader());
    assert_eq!(cluster.node(N2).matchmaker_set(), Some(successor));
    println!("   a matchmaker refused node 2 and named the new set. Node 2 adopted it");

    // A driver campaigns again when the election timeout runs out.
    cluster.elect(N2);
    let second = cluster.node(N2).ballot();
    assert!(second > first);

    let journal = cluster.journal_id(N2);
    for id in [M0, M1, M3] {
        assert!(
            cluster
                .matchmaker(id)
                .registry(journal)
                .contains_key(&second)
        );
    }
    cluster.write(N2, WRITER, 1, "bravo");
    assert_eq!(cluster.journal(N3), ["alpha", "bravo"]);
    println!("   node 2 leads at ballot {second}, registered at m0, m1 and m3");
    println!("   the journal goes on: {:?}", cluster.journal(N3));
}
