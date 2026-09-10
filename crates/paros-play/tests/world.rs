//! The log world, driven directly — the engine Act II's levels are written
//! against.
//!
//! Every test here is a scenario an Act II level needs: an election run by
//! hand, a command that reaches the application, a hole that heals, a restart
//! that remembers, the two durability seams, a read that round-trips through
//! heartbeat acks, and each of the prompt kinds the act teaches.

use std::collections::BTreeSet;

use paros_core::{Ballot, Command, Config, Message, NodeId, NodeRole, QuorumSystem, Slot};
use paros_play::action::Seam;
use paros_play::prompt::{PromptKind, Verdict};
use paros_play::world::{NO_CHECK_QUORUM, World, WorldPolicy};

const CLIENT: u64 = 7;

fn policy(manual: &[PromptKind]) -> WorldPolicy {
    WorldPolicy {
        manual: manual.iter().copied().collect::<BTreeSet<_>>(),
        auto_resend: false,
        hold_leadership: true,
    }
}

fn cluster(size: u64) -> World {
    let peers: Vec<NodeId> = (0..size).map(NodeId).collect();
    let configs = peers
        .iter()
        .map(|id| Config {
            id: *id,
            peers: peers.clone(),
            quorum_system: QuorumSystem::Majority,
            ..Config::default()
        })
        .collect();
    let mut world = World::new(configs, &[CLIENT], 10);
    world.set_policy(policy(&[]));
    world
}

/// Deliver everything in flight for which `keep` holds, lowest id first, to
/// quiescence. Stops if a prompt opens.
fn deliver_where(world: &mut World, keep: impl Fn(&Message) -> bool) {
    for _ in 0..2000 {
        if world.prompt().is_some() {
            return;
        }
        let Some(id) = world
            .wire()
            .iter()
            .filter(|entry| keep(&entry.message))
            .map(|entry| entry.id)
            .min()
        else {
            return;
        };
        world.deliver(id).expect("a message that is in flight");
    }
    panic!("delivery reached quiescence");
}

fn deliver_all(world: &mut World) {
    deliver_where(world, |_| true);
}

/// Drop everything in flight for which `hit` holds — a partition the player
/// never heals.
fn drop_where(world: &mut World, hit: impl Fn(&Message) -> bool) {
    while let Some(id) = world
        .wire()
        .iter()
        .filter(|entry| hit(&entry.message))
        .map(|entry| entry.id)
        .min()
    {
        world.drop_message(id).expect("a message that is in flight");
    }
}

/// Deliver everything not addressed to `isolated`, dropping what is — the
/// player cutting one node off entirely.
fn isolate(world: &mut World, isolated: NodeId) {
    for _ in 0..2000 {
        if world.prompt().is_some() {
            return;
        }
        while let Some(id) = world
            .wire()
            .iter()
            .filter(|entry| entry.to == isolated)
            .map(|entry| entry.id)
            .min()
        {
            world.drop_message(id).expect("in flight");
        }
        let Some(id) = world.wire().iter().map(|entry| entry.id).min() else {
            return;
        };
        world.deliver(id).expect("in flight");
    }
    panic!("delivery reached quiescence");
}

fn elect(world: &mut World, leader: u64) {
    world
        .start_election(NodeId(leader))
        .expect("a live node campaigns");
    deliver_all(world);
    assert!(
        world
            .node(NodeId(leader))
            .is_some_and(paros_core::ColocatedNode::is_leader),
        "node {leader} won its election"
    );
}

fn applied_text(world: &World, node: u64) -> Vec<String> {
    world
        .disk(NodeId(node))
        .expect("a node of this world")
        .applied()
        .iter()
        .map(|(slot, command)| match command {
            Command::User(entry) => {
                format!("{}:{}", slot.0, String::from_utf8_lossy(&entry.value.0))
            }
            Command::Control(control) => format!("{}:{control:?}", slot.0),
        })
        .collect()
}

// ---- elections --------------------------------------------------------------

#[test]
fn a_three_node_election_by_hand() {
    let mut world = cluster(3);
    world
        .start_election(NodeId(0))
        .expect("a live node campaigns");
    let prepares = world
        .wire()
        .iter()
        .filter(|entry| matches!(entry.message, Message::Prepare { .. }))
        .count();
    assert_eq!(prepares, 2, "one Prepare per peer");
    assert_eq!(
        world.node(NodeId(0)).map(paros_core::ColocatedNode::role),
        Some(NodeRole::Candidate)
    );
    deliver_all(&mut world);
    let leader = world.node(NodeId(0)).expect("node 0 is running");
    assert!(leader.is_leader(), "a promise majority made node 0 leader");
    assert_eq!(world.leader(), Some(NodeId(0)));
    // A hand-stepped leader is parked at the no-check-quorum sentinel, so it is
    // not demoted between the player's moves.
    assert_eq!(leader.election_timeout(), NO_CHECK_QUORUM);
    // And it is restored the moment the leadership goes.
    world.step_down(NodeId(0)).expect("a leader may resign");
    assert_eq!(
        world
            .node(NodeId(0))
            .map(paros_core::ColocatedNode::election_timeout),
        Some(10)
    );
}

// ---- the log ----------------------------------------------------------------

#[test]
fn a_proposal_reaches_the_application_log() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha")
        .expect("the leader admits a proposal");
    deliver_all(&mut world);
    assert_eq!(applied_text(&world, 0), vec!["0:alpha".to_string()]);
    for node in 1..3 {
        assert_eq!(
            applied_text(&world, node),
            vec!["0:alpha".to_string()],
            "node {node} applied the same command at the same slot"
        );
    }
    assert!(
        world.all_writes_acked(),
        "the client's write is acknowledged"
    );
}

#[test]
fn a_proposal_to_a_follower_is_refused_with_a_redirect() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    // A won election broadcasts no beat: a follower learns who leads from the
    // leader's next beat or Accept. One tick is that beat.
    world.tick(NodeId(0)).expect("the leader beats");
    deliver_all(&mut world);
    let err = world
        .propose(NodeId(1), CLIENT, "alpha")
        .expect_err("a follower does not admit proposals");
    assert_eq!(err.code, paros_play::ActionErrorCode::NotLeader);
    assert!(err.message.contains("node 0"), "it names the leader: {err}");
}

#[test]
fn an_out_of_order_delivery_leaves_a_hole_then_heals() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world.propose(NodeId(0), CLIENT, "alpha").expect("admitted");
    world.propose(NodeId(0), CLIENT, "bravo").expect("admitted");
    // Everything except slot 0's Phase 2, so slot 1 decides first.
    let slot0 = |message: &Message| {
        matches!(
            message,
            Message::Accept { slot: Slot(0), .. }
                | Message::Accepted { slot: Slot(0), .. }
                | Message::Commit { slot: Slot(0), .. }
        )
    };
    deliver_where(&mut world, |message| !slot0(message));
    let leader = world.node(NodeId(0)).expect("running");
    assert!(
        leader.replica().is_chosen(Slot(1)),
        "slot 1 is chosen out of order"
    );
    assert_eq!(
        leader.replica().chosen_gap(),
        Some((Slot(0), Slot(1))),
        "the contiguous prefix stops below the hole"
    );
    assert!(
        applied_text(&world, 0).is_empty(),
        "nothing is applied over a hole"
    );
    // Now let slot 0 through.
    deliver_all(&mut world);
    let leader = world.node(NodeId(0)).expect("running");
    assert_eq!(leader.replica().chosen_gap(), None, "the hole healed");
    assert_eq!(
        applied_text(&world, 0),
        vec!["0:alpha".to_string(), "1:bravo".to_string()],
        "the application saw both commands, in slot order"
    );
}

// ---- crash and restart ------------------------------------------------------

#[test]
fn a_restart_keeps_the_hard_state() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world.propose(NodeId(0), CLIENT, "alpha").expect("admitted");
    deliver_all(&mut world);
    let before = world.node(NodeId(1)).expect("running").hard_state();
    let records = world.disk(NodeId(1)).expect("a disk").records().clone();
    world.crash(NodeId(1)).expect("a live node may crash");
    assert!(world.node(NodeId(1)).is_none(), "the node is gone");
    assert_eq!(
        world.disk(NodeId(1)).expect("a disk").records(),
        &records,
        "a crash never touches the disk"
    );
    world.restart(NodeId(1)).expect("a crashed node restarts");
    let after = world.node(NodeId(1)).expect("running").hard_state();
    assert_eq!(
        after.max_promised_ballot, before.max_promised_ballot,
        "a restarted node's promise never regresses"
    );
    assert_eq!(after.chosen_index, before.chosen_index);
    assert_eq!(
        world.node(NodeId(1)).map(paros_core::ColocatedNode::role),
        Some(NodeRole::Follower),
        "leadership is volatile: every boot is a follower"
    );
}

#[test]
fn restarting_a_running_node_is_refused() {
    let mut world = cluster(3);
    let err = world
        .restart(NodeId(1))
        .expect_err("a running node does not restart");
    assert_eq!(err.code, paros_play::ActionErrorCode::NodeAlive);
}

#[test]
fn delivering_to_a_crashed_node_discards_the_message() {
    let mut world = cluster(3);
    world.start_election(NodeId(0)).expect("campaigns");
    world.crash(NodeId(1)).expect("crashes");
    let to_one = world
        .wire()
        .iter()
        .find(|entry| entry.to == NodeId(1))
        .map(|entry| entry.id)
        .expect("a Prepare for node 1");
    world
        .deliver(to_one)
        .expect("delivery to a dead node is legal");
    assert!(
        world.wire().iter().all(|entry| entry.from != NodeId(1)),
        "a dead node answers nothing"
    );
}

// ---- the durability seams ---------------------------------------------------

#[test]
fn the_before_sync_seam_loses_the_whole_batch() {
    let mut world = cluster(3);
    world.start_election(NodeId(0)).expect("campaigns");
    world
        .crash_at(NodeId(1), Seam::BeforeSync)
        .expect("a live node arms a seam");
    let before = world.disk(NodeId(1)).expect("a disk").hard_state();
    let to_one = world
        .wire()
        .iter()
        .find(|entry| entry.to == NodeId(1))
        .map(|entry| entry.id)
        .expect("a Prepare for node 1");
    world.deliver(to_one).expect("delivered");
    assert!(world.node(NodeId(1)).is_none(), "the seam crashed the node");
    assert_eq!(
        world.disk(NodeId(1)).expect("a disk").hard_state(),
        before,
        "nothing became durable"
    );
    assert!(
        world.wire().iter().all(|entry| entry.from != NodeId(1)),
        "nothing was sent"
    );
}

#[test]
fn the_after_sync_seam_keeps_the_writes_and_loses_the_messages() {
    let mut world = cluster(3);
    world.start_election(NodeId(0)).expect("campaigns");
    world
        .crash_at(NodeId(1), Seam::AfterSyncBeforeSend)
        .expect("a live node arms a seam");
    let to_one = world
        .wire()
        .iter()
        .find(|entry| entry.to == NodeId(1))
        .map(|entry| entry.id)
        .expect("a Prepare for node 1");
    world.deliver(to_one).expect("delivered");
    assert!(world.node(NodeId(1)).is_none(), "the seam crashed the node");
    assert!(
        world
            .disk(NodeId(1))
            .expect("a disk")
            .hard_state()
            .max_promised_ballot
            > Ballot::zero(),
        "the promise is durable, and nobody ever heard about it"
    );
    assert!(
        world.wire().iter().all(|entry| entry.from != NodeId(1)),
        "the batch's messages never left"
    );
    // And the promise survives the reboot, which is the whole point.
    world.restart(NodeId(1)).expect("restarts");
    assert!(
        world
            .node(NodeId(1))
            .expect("running")
            .acceptor()
            .promised()
            > Ballot::zero()
    );
}

// ---- reads ------------------------------------------------------------------

#[test]
fn a_read_index_round_trips_through_heartbeat_acks() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world.propose(NodeId(0), CLIENT, "alpha").expect("admitted");
    deliver_all(&mut world);
    world
        .read_index(NodeId(0), CLIENT)
        .expect("the leader opens a read round");
    assert_eq!(world.unserved_reads(), 1, "the read is pending");
    assert!(
        world
            .wire()
            .iter()
            .any(|entry| matches!(entry.message, Message::Heartbeat { .. })),
        "opening a read beats immediately"
    );
    deliver_all(&mut world);
    let served = world.served_reads();
    assert_eq!(served.len(), 1, "the ack quorum confirmed the read");
    assert_eq!(served[0].1, Some(Slot(0)), "it observes the applied prefix");
}

#[test]
fn a_follower_refuses_a_read() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    let err = world
        .read_index(NodeId(1), CLIENT)
        .expect_err("a follower serves no linearizable read");
    assert_eq!(err.code, paros_play::ActionErrorCode::NotLeader);
}

// ---- the prompts ------------------------------------------------------------

#[test]
fn the_acceptor_prompts_judge_both_rules() {
    let mut world = cluster(3);
    world.set_policy(policy(&[
        PromptKind::AcceptorPrepare,
        PromptKind::AcceptorAccept,
    ]));
    world.start_election(NodeId(0)).expect("campaigns");
    let to_one = world
        .wire()
        .iter()
        .find(|entry| entry.to == NodeId(1))
        .map(|entry| entry.id)
        .expect("a Prepare for node 1");
    world.deliver(to_one).expect("delivered");
    let prompt = world.prompt().expect("a manual acceptor is asked");
    assert_eq!(prompt.kind, PromptKind::AcceptorPrepare);
    assert_eq!(prompt.expected(), "promise");
    let id = prompt.id;
    assert_eq!(
        world.answer(id, "nack").expect("a legal choice"),
        Verdict::Wrong
    );
    assert!(world.prompt().is_some(), "the prompt stays open");
    assert_eq!(
        world.answer(id, "promise").expect("a legal choice"),
        Verdict::Right
    );
    assert!(world.prompt().is_none(), "the right answer closes it");
    deliver_where(&mut world, |message| {
        matches!(message, Message::Promise { .. } | Message::Prepare { .. })
    });
    // Answer node 2's Prepare too, so the election completes.
    while let Some(prompt) = world.prompt() {
        let (id, expected) = (prompt.id, prompt.expected().to_string());
        world.answer(id, &expected).expect("the core's own answer");
        deliver_where(&mut world, |message| {
            matches!(message, Message::Promise { .. } | Message::Prepare { .. })
        });
    }
    assert!(
        world
            .node(NodeId(0))
            .is_some_and(paros_core::ColocatedNode::is_leader),
        "the hand-answered promises elected node 0"
    );
    world.propose(NodeId(0), CLIENT, "alpha").expect("admitted");
    let accept = world
        .wire()
        .iter()
        .find(|entry| matches!(entry.message, Message::Accept { .. }))
        .map(|entry| entry.id)
        .expect("an Accept is in flight");
    world.deliver(accept).expect("delivered");
    let prompt = world.prompt().expect("a manual acceptor is asked");
    assert_eq!(prompt.kind, PromptKind::AcceptorAccept);
    assert_eq!(prompt.expected(), "accept", "the promise is at the ballot");
}

#[test]
fn the_persist_order_prompt_holds_the_batch_back() {
    let mut world = cluster(3);
    world.set_policy(policy(&[PromptKind::PersistOrder]));
    world.start_election(NodeId(0)).expect("campaigns");
    // The candidate's own batch raises its promise and carries the Prepares, so
    // it is the first batch the question is asked about.
    let own = world.prompt().expect("the candidate's own batch asks").id;
    assert_eq!(world.answer(own, "sync_first"), Ok(Verdict::Right));
    let to_one = world
        .wire()
        .iter()
        .find(|entry| entry.to == NodeId(1))
        .map(|entry| entry.id)
        .expect("a Prepare for node 1");
    let before = world.disk(NodeId(1)).expect("a disk").hard_state();
    world.deliver(to_one).expect("delivered");
    let prompt = world
        .prompt()
        .expect("a batch with writes and messages asks");
    assert_eq!(prompt.kind, PromptKind::PersistOrder);
    assert_eq!(prompt.expected(), "sync_first");
    assert_eq!(
        world.disk(NodeId(1)).expect("a disk").hard_state(),
        before,
        "the batch is held whole until the question is answered"
    );
    let id = prompt.id;
    assert_eq!(world.answer(id, "send_first"), Ok(Verdict::Wrong));
    assert_eq!(
        world.disk(NodeId(1)).expect("a disk").hard_state(),
        before,
        "a wrong answer moves nothing"
    );
    assert_eq!(world.answer(id, "sync_first"), Ok(Verdict::Right));
    assert!(
        world
            .disk(NodeId(1))
            .expect("a disk")
            .hard_state()
            .max_promised_ballot
            > before.max_promised_ballot,
        "the promise is durable"
    );
    assert!(
        world.wire().iter().any(|entry| entry.from == NodeId(1)),
        "and only then did the Promise leave"
    );
}

#[test]
fn the_replica_apply_prompt_refuses_to_skip_a_hole() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world.propose(NodeId(0), CLIENT, "alpha").expect("admitted");
    world.propose(NodeId(0), CLIENT, "bravo").expect("admitted");
    world.set_policy(policy(&[PromptKind::ReplicaApply]));
    let slot0 = |message: &Message| {
        matches!(
            message,
            Message::Accept { slot: Slot(0), .. }
                | Message::Accepted { slot: Slot(0), .. }
                | Message::Commit { slot: Slot(0), .. }
        )
    };
    deliver_where(&mut world, |message| !slot0(message));
    let prompt = world
        .prompt()
        .expect("the slot that decides out of order asks");
    assert_eq!(prompt.kind, PromptKind::ReplicaApply);
    assert_eq!(
        prompt.expected(),
        "hold",
        "slot 1 does not extend a prefix that stops at slot 0"
    );
    let id = prompt.id;
    assert_eq!(world.answer(id, "apply"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, "hold"), Ok(Verdict::Right));
}

#[test]
fn the_leader_recovery_prompt_fills_the_permanent_gap() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world.propose(NodeId(0), CLIENT, "alpha").expect("admitted");
    world.propose(NodeId(0), CLIENT, "bravo").expect("admitted");
    // Slot 0's Accepts are lost entirely; slot 1 is chosen. Then the leader
    // crashes with its volatile proposer map, and the gap is nobody's.
    let slot0_accept = |message: &Message| matches!(message, Message::Accept { slot: Slot(0), .. });
    deliver_where(&mut world, |message| !slot0_accept(message));
    // Lost for good, not merely late: a copy still in flight would land at the
    // survivors before the new ballot fences it out, and then the quorum would
    // report slot 0 after all.
    drop_where(&mut world, slot0_accept);
    world.crash(NodeId(0)).expect("the leader crashes");
    world.set_policy(policy(&[PromptKind::LeaderRecovery]));
    world
        .start_election(NodeId(1))
        .expect("a survivor campaigns");
    deliver_all(&mut world);
    let prompt = world
        .prompt()
        .expect("a fresh leadership is asked about its recovered suffix");
    assert_eq!(prompt.kind, PromptKind::LeaderRecovery);
    assert_eq!(
        prompt.expected(),
        "fill_noop",
        "the quorum reported nothing for slot 0, so it is genuinely free"
    );
    let id = prompt.id;
    assert_eq!(world.answer(id, "skip"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, "repropose"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, "fill_noop"), Ok(Verdict::Right));
    world.set_policy(policy(&[]));
    deliver_all(&mut world);
    let leader = world.node(NodeId(1)).expect("running");
    assert_eq!(leader.replica().chosen_gap(), None, "the gap is closed");
    assert!(
        leader.election_gap_fills() >= 1,
        "and it was closed with a Noop"
    );
}

#[test]
fn the_commit_overwrite_prompt_replaces_a_stale_record() {
    // Five nodes, so a promise quorum can miss the one acceptor that voted.
    let mut world = cluster(5);
    elect(&mut world, 0);
    world.propose(NodeId(0), CLIENT, "alpha").expect("admitted");
    // Only node 1 hears the Accept; two of five is not a decision.
    let stray = |message: &Message| matches!(message, Message::Accept { slot: Slot(0), .. });
    let to_one = world
        .wire()
        .iter()
        .find(|entry| stray(&entry.message) && entry.to == NodeId(1))
        .map(|entry| entry.id)
        .expect("an Accept for node 1");
    world.deliver(to_one).expect("delivered");
    drop_where(&mut world, stray);
    deliver_all(&mut world);
    assert!(
        world
            .node(NodeId(1))
            .expect("running")
            .acceptor()
            .record(Slot(0))
            .is_some(),
        "node 1 holds the only other copy"
    );
    world.crash(NodeId(0)).expect("the leader crashes");

    // Node 2 campaigns with a quorum that misses node 1 entirely, so its
    // promise quorum reports nothing for slot 0 and it gap-fills a Noop.
    world.start_election(NodeId(2)).expect("campaigns");
    isolate(&mut world, NodeId(1));
    assert!(
        world
            .node(NodeId(2))
            .is_some_and(paros_core::ColocatedNode::is_leader),
        "node 2 won without node 1"
    );

    // Its promise quorum reported nothing for slot 0, so slot 0 is free as far
    // as it knows, and its client's next command lands there. Node 1 misses the
    // Accept and hears about the decision only from the Commit — which
    // contradicts, at a higher ballot, the record it has been holding.
    world.set_policy(policy(&[PromptKind::CommitOverwrite]));
    world.propose(NodeId(2), CLIENT, "bravo").expect("admitted");
    while let Some(id) = world
        .wire()
        .iter()
        .find(|entry| entry.to == NodeId(1) && matches!(entry.message, Message::Accept { .. }))
        .map(|entry| entry.id)
    {
        world.drop_message(id).expect("in flight");
    }
    deliver_all(&mut world);
    let prompt = world.prompt().expect("the contradicted acceptor is asked");
    assert_eq!(prompt.kind, PromptKind::CommitOverwrite);
    assert_eq!(prompt.node, 1);
    assert_eq!(prompt.expected(), "take", "the choosing ballot wins");
    let id = prompt.id;
    assert_eq!(world.answer(id, "keep"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, "take"), Ok(Verdict::Right));
}

#[test]
fn the_read_serve_prompt_waits_without_an_ack_quorum() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world.propose(NodeId(0), CLIENT, "alpha").expect("admitted");
    deliver_all(&mut world);
    world.set_policy(policy(&[PromptKind::ReadServe]));
    world.read_index(NodeId(0), CLIENT).expect("a read opens");
    // Deliver the beats, then exactly one ack: one ack plus the leader's own
    // vote is a majority of three, so the first ack confirms. Check the
    // question is asked, and that its answer is the core's.
    deliver_where(&mut world, |message| {
        matches!(message, Message::Heartbeat { .. })
    });
    let ack = world
        .wire()
        .iter()
        .find(|entry| matches!(entry.message, Message::HeartbeatAck { .. }))
        .map(|entry| entry.id)
        .expect("an ack is in flight");
    world.deliver(ack).expect("delivered");
    let prompt = world.prompt().expect("the leader is asked");
    assert_eq!(prompt.kind, PromptKind::ReadServe);
    let (id, expected) = (prompt.id, prompt.expected().to_string());
    assert_eq!(
        expected, "serve",
        "the leader's own vote plus one ack is two of three"
    );
    assert_eq!(world.answer(id, "wait"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, &expected), Ok(Verdict::Right));
    deliver_all(&mut world);
    assert_eq!(world.served_reads().len(), 1);
}

#[test]
fn a_prompt_blocks_every_other_move() {
    let mut world = cluster(3);
    world.set_policy(policy(&[PromptKind::AcceptorPrepare]));
    world.start_election(NodeId(0)).expect("campaigns");
    let to_one = world
        .wire()
        .iter()
        .find(|entry| entry.to == NodeId(1))
        .map(|entry| entry.id)
        .expect("a Prepare for node 1");
    world.deliver(to_one).expect("delivered");
    assert!(world.prompt().is_some());
    let err = world.tick_all().expect_err("the world waits on the answer");
    assert_eq!(err.code, paros_play::ActionErrorCode::PromptOpen);
}
