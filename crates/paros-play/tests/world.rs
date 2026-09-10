//! The log world, driven directly — the engine Act II's levels are written
//! against.
//!
//! Every test here is a scenario an Act II level needs: an election run by
//! hand, a command that reaches the application, a hole that heals, a restart
//! that remembers, the two durability seams, a read that round-trips through
//! heartbeat acks, and each of the prompt kinds the act teaches.

use std::collections::BTreeSet;

use paros_core::{Ballot, Command, Config, Control, Message, NodeId, NodeRole, QuorumSystem, Slot};
use paros_play::action::Seam;
use paros_play::prompt::{PromptKind, Verdict};
use paros_play::world::{Disk, NO_CHECK_QUORUM, World, WorldPolicy};

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
        .propose(NodeId(0), CLIENT, "alpha", None)
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
        .propose(NodeId(1), CLIENT, "alpha", None)
        .expect_err("a follower does not admit proposals");
    assert_eq!(err.code, paros_play::ActionErrorCode::NotLeader);
    assert!(err.message.contains("node 0"), "it names the leader: {err}");
}

#[test]
fn an_out_of_order_delivery_leaves_a_hole_then_heals() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    world
        .propose(NodeId(0), CLIENT, "bravo", None)
        .expect("admitted");
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
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
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
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
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
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
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
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    world
        .propose(NodeId(0), CLIENT, "bravo", None)
        .expect("admitted");
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
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    world
        .propose(NodeId(0), CLIENT, "bravo", None)
        .expect("admitted");
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
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
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
    world
        .propose(NodeId(2), CLIENT, "bravo", None)
        .expect("admitted");
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
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
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

// ---- truncation and snapshots (Act III) --------------------------------------

/// Drive `world` to a state where a `Truncate` has been decided: seed a
/// snapshot point (the leader refuses the first request and proposes a `Snap`
/// marker), then ask again.
fn truncate_through(world: &mut World, leader: u64, up_to: u64) {
    world
        .compact(NodeId(leader), up_to)
        .expect("the leader answers a compaction request");
    deliver_all(world);
    world
        .compact(NodeId(leader), up_to)
        .expect("the leader answers a compaction request");
    deliver_all(world);
}

#[test]
fn a_truncate_is_refused_until_a_quorum_holds_a_snapshot_point() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    world
        .propose(NodeId(0), CLIENT, "bravo", None)
        .expect("admitted");
    deliver_all(&mut world);
    // Nothing has been snapshotted, so the coupling rule refuses: past the
    // floor the entries are gone everywhere, and a snapshot nobody holds
    // rescues nobody.
    world.compact(NodeId(0), 8).expect("the leader answers");
    let first = *world.compacts().first().expect("one request answered");
    assert!(!first.accepted, "the first request is refused");
    assert_eq!(first.covered, None, "no quorum holds a point yet");
    assert!(first.seeded_marker, "and the refusal seeds one");
    assert_eq!(
        world.disk(NodeId(0)).expect("a disk").floor(),
        Slot(0),
        "nothing was truncated"
    );
    deliver_all(&mut world);
    // Now a quorum holds a decided snapshot point, and the retry goes through.
    assert!(
        world
            .snapshot_points()
            .iter()
            .filter(|(_, point)| point.is_some())
            .count()
            >= 2,
        "a quorum recorded the decided snapshot point"
    );
    world.compact(NodeId(0), 8).expect("the leader answers");
    let second = *world.compacts().last().expect("two requests answered");
    assert!(second.accepted, "the retry is admitted");
    assert!(second.covered.is_some(), "and it names the covered point");
}

#[test]
fn a_truncate_is_applied_lazily_by_every_node() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    deliver_all(&mut world);
    world.compact(NodeId(0), 8).expect("answers");
    deliver_all(&mut world);
    world.compact(NodeId(0), 8).expect("answers");
    // The decision exists but nobody outside the leader has applied it yet, so
    // hold back everything and watch the floors move one node at a time.
    let floor_before: Vec<Slot> = world.floors().iter().map(|(_, first)| *first).collect();
    deliver_where(&mut world, |message| {
        matches!(message, Message::Accept { .. } | Message::Accepted { .. })
    });
    let after_decision: Vec<Slot> = world.floors().iter().map(|(_, first)| *first).collect();
    assert!(
        after_decision[0] > floor_before[0],
        "the leader applied the decided Truncate and its floor rose"
    );
    assert_eq!(
        after_decision[1], floor_before[1],
        "a follower that has not applied that slot has not truncated"
    );
    deliver_all(&mut world);
    let floors: Vec<Slot> = world.floors().iter().map(|(_, first)| *first).collect();
    assert!(
        floors.iter().all(|first| *first == floors[0]),
        "one cluster-wide floor, forwarded by ordinary replication: {floors:?}"
    );
    assert!(floors[0] > Slot(0), "and it really moved");
}

#[test]
fn a_below_floor_catch_up_is_answered_with_a_snapshot() {
    let mut world = cluster(3);
    world.crash(NodeId(2)).expect("a live node may crash");
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    world
        .propose(NodeId(0), CLIENT, "bravo", None)
        .expect("admitted");
    deliver_all(&mut world);
    truncate_through(&mut world, 0, 8);
    let floor = world.disk(NodeId(0)).expect("a disk").floor();
    assert!(floor > Slot(0), "the survivors truncated past node 2");

    world.restart(NodeId(2)).expect("a crashed node restarts");
    assert!(
        !world.stranded().is_empty(),
        "node 2 needs slots that no longer exist anywhere"
    );
    // A candidate broadcasts a catch-up request: it has heard from no leader,
    // which is exactly the condition under which it may be silently behind.
    for _ in 0..10 {
        world.tick(NodeId(2)).expect("a live node ticks");
    }
    drop_where(&mut world, |message| {
        matches!(message, Message::Prepare { .. })
    });
    deliver_where(&mut world, |message| {
        matches!(message, Message::CatchUpRequest { .. })
    });
    assert!(
        world
            .wire()
            .iter()
            .all(|entry| !matches!(entry.message, Message::CatchUpResponse { .. })),
        "no peer replays a range it has truncated: those entries are gone"
    );
    assert!(
        world
            .wire()
            .iter()
            .any(|entry| matches!(entry.message, Message::InstallSnapshot { .. })),
        "the peer offers the application's state instead"
    );
    deliver_all(&mut world);
    assert_eq!(
        applied_text(&world, 2),
        applied_text(&world, 0),
        "node 2 was restored from bytes paros never read"
    );
}

#[test]
fn a_snapshot_install_never_lowers_the_promise() {
    let mut world = cluster(3);
    world.crash(NodeId(2)).expect("crashes");
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    deliver_all(&mut world);
    truncate_through(&mut world, 0, 8);
    world.restart(NodeId(2)).expect("restarts");
    // Node 2 campaigns before it is healed, so its own promise ends up *above*
    // the ballot the snapshot's prefix was decided under. That is the case the
    // rule exists for.
    for _ in 0..10 {
        world.tick(NodeId(2)).expect("ticks");
    }
    drop_where(&mut world, |message| {
        matches!(message, Message::Prepare { .. })
    });
    let promise_before = world
        .node(NodeId(2))
        .expect("running")
        .acceptor()
        .promised();
    let offered = {
        deliver_where(&mut world, |message| {
            matches!(message, Message::CatchUpRequest { .. })
        });
        world
            .wire()
            .iter()
            .find_map(|entry| match entry.message {
                Message::InstallSnapshot { ballot, .. } => Some(ballot),
                _ => None,
            })
            .expect("a snapshot is offered")
    };
    assert!(
        offered < promise_before,
        "the snapshot's ballot ({offered:?}) is below node 2's own promise ({promise_before:?})"
    );
    world.set_policy(policy(&[PromptKind::SnapshotPromise]));
    let id = world
        .wire()
        .iter()
        .find(|entry| matches!(entry.message, Message::InstallSnapshot { .. }))
        .map(|entry| entry.id)
        .expect("in flight");
    world.deliver(id).expect("delivered");
    let prompt = world.prompt().expect("the installing node is asked");
    assert_eq!(prompt.kind, PromptKind::SnapshotPromise);
    let (prompt_id, expected) = (prompt.id, prompt.expected().to_string());
    assert_eq!(expected, "higher", "it keeps the higher of the two");
    let wrong = prompt
        .choices
        .iter()
        .map(|choice| choice.id.clone())
        .find(|choice| *choice != expected)
        .expect("the other ballot is offered too");
    assert_eq!(world.answer(prompt_id, &wrong), Ok(Verdict::Wrong));
    assert_eq!(world.answer(prompt_id, &expected), Ok(Verdict::Right));
    deliver_all(&mut world);
    assert!(
        world
            .node(NodeId(2))
            .expect("running")
            .acceptor()
            .promised()
            >= promise_before,
        "a snapshot restores the log, never a promise"
    );
    assert_eq!(world.promise_regressed(), None);
}

#[test]
fn the_after_sync_seam_loses_the_truncate_with_the_batch() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    deliver_all(&mut world);
    world.compact(NodeId(0), 8).expect("answers");
    deliver_all(&mut world);
    world.compact(NodeId(0), 8).expect("answers");
    // Let the Truncate decide at the leader, then hand the decision to node 1
    // with the seam armed: the batch that would truncate is cut after its
    // flush and before its send.
    deliver_where(&mut world, |message| {
        !matches!(message, Message::Commit { .. })
    });
    let floor_before = world.disk(NodeId(1)).expect("a disk").floor();
    world
        .crash_at(NodeId(1), Seam::AfterSyncBeforeSend)
        .expect("a live node arms a seam");
    let commit = world
        .wire()
        .iter()
        .find(|entry| entry.to == NodeId(1) && matches!(entry.message, Message::Commit { .. }))
        .map(|entry| entry.id)
        .expect("a Commit for node 1");
    world.deliver(commit).expect("delivered");
    assert!(world.node(NodeId(1)).is_none(), "the seam crashed the node");
    assert_eq!(
        world.disk(NodeId(1)).expect("a disk").floor(),
        floor_before,
        "the truncate went with the half of the batch that was lost: a durable floor must never \
         outrun the durable application state covering the slots it drops"
    );
    // And it is safe: the floor is pure space reclamation, re-raised the next
    // time this node applies a decided Truncate.
    world.restart(NodeId(1)).expect("restarts");
    deliver_all(&mut world);
    assert_eq!(world.promise_regressed(), None);
}

// ---- the recovery judge -------------------------------------------------------

#[test]
fn a_recovered_noop_is_re_proposed_not_re_filled() {
    // A predecessor gap-filled slot 0 with a `Noop` and got it accepted at one
    // node only. That node's Promise reports it, so the fresh leadership's own
    // recovery says `Recovered(Noop)` — re-propose — and *not* `Fill`. The two
    // are indistinguishable from the batch's `Accept`, which is why the judge
    // reads the answer off the core's recovery instead of guessing.
    let carried = ballot(1, 2);
    let mut seeded = std::collections::BTreeMap::new();
    seeded.insert(Slot(0), (carried, Command::Control(Control::Noop)));
    let peers: Vec<NodeId> = (0..3).map(NodeId).collect();
    let disks: Vec<Disk> = peers
        .iter()
        .map(|id| {
            let config = Config {
                id: *id,
                peers: peers.clone(),
                quorum_system: QuorumSystem::Majority,
                ..Config::default()
            };
            if id.0 == 1 {
                Disk::seeded(config, carried, seeded.clone(), None)
            } else {
                Disk::seeded(config, carried, std::collections::BTreeMap::new(), None)
            }
        })
        .collect();
    let mut world = World::from_disks(disks, &[CLIENT], 10);
    world.set_policy(policy(&[PromptKind::LeaderRecovery]));
    world.start_election(NodeId(0)).expect("campaigns");
    // The promise quorum must contain node 1, the only node that knows
    // anything.
    deliver_where(&mut world, |message| {
        matches!(
            message,
            Message::Prepare { .. } | Message::Promise { .. } | Message::Nack { .. }
        )
    });
    let prompt = world
        .prompt()
        .expect("a fresh leadership is asked about its recovered suffix");
    assert_eq!(prompt.kind, PromptKind::LeaderRecovery);
    assert!(
        prompt
            .state_summary
            .iter()
            .any(|line| line.contains("reported Noop for slot 0")),
        "the prompt says what the quorum actually reported, not what the command looks like: \
         {:?}",
        prompt.state_summary
    );
    assert_eq!(
        prompt.expected(),
        "repropose",
        "a Noop a Promise reported is a value like any other: re-propose it"
    );
    let id = prompt.id;
    assert_eq!(world.answer(id, "fill_noop"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, "skip"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, "repropose"), Ok(Verdict::Right));
}

/// The ballot an earlier leadership ran at: `round`, minted by node `node`.
fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

// ---- the client's two dedup tables --------------------------------------------

#[test]
fn a_retry_in_the_chosen_but_unapplied_window_is_held_not_acked() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    world
        .propose(NodeId(0), CLIENT, "bravo", None)
        .expect("admitted");
    // Slot 1 decides while slot 0 is still open: chosen above a hole.
    let slot0 = |message: &Message| {
        matches!(
            message,
            Message::Accept { slot: Slot(0), .. }
                | Message::Accepted { slot: Slot(0), .. }
                | Message::Commit { slot: Slot(0), .. }
        )
    };
    deliver_where(&mut world, |message| !slot0(message));
    world.set_policy(policy(&[PromptKind::AckWrite]));
    world.retry(NodeId(0), CLIENT, 2).expect("a client retries");
    let prompt = world.prompt().expect("the leader is asked");
    assert_eq!(prompt.kind, PromptKind::AckWrite);
    assert_eq!(
        prompt.expected(),
        "inflight",
        "chosen is not applied: the reply parks on the slot it is in flight at"
    );
    let id = prompt.id;
    assert_eq!(world.answer(id, "acked"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, "fresh"), Ok(Verdict::Wrong));
    assert_eq!(world.answer(id, "inflight"), Ok(Verdict::Right));

    // Close the hole and ask again: now it really has been executed here.
    world.set_policy(policy(&[]));
    deliver_all(&mut world);
    world.set_policy(policy(&[PromptKind::AckWrite]));
    world.retry(NodeId(0), CLIENT, 2).expect("a client retries");
    let prompt = world.prompt().expect("the leader is asked again");
    assert_eq!(prompt.expected(), "acked");
    let id = prompt.id;
    assert_eq!(world.answer(id, "acked"), Ok(Verdict::Right));
    assert_eq!(
        applied_text(&world, 0)
            .iter()
            .filter(|entry| entry.ends_with("bravo"))
            .count(),
        1,
        "the command was executed exactly once"
    );
}

// ---- the client history ------------------------------------------------------

#[test]
fn a_read_across_a_leader_change_is_linearizable() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    deliver_all(&mut world);
    // Node 1 takes over with node 2; node 0 hears none of it.
    world.start_election(NodeId(1)).expect("campaigns");
    isolate(&mut world, NodeId(0));
    assert!(
        world
            .node(NodeId(1))
            .is_some_and(paros_core::ColocatedNode::is_leader)
    );
    world
        .read_index(NodeId(1), CLIENT)
        .expect("the leader opens a read round");
    isolate(&mut world, NodeId(0));
    assert_eq!(world.served_reads().len(), 1, "the read is served");
    assert_eq!(
        world.linearizable(),
        Ok(()),
        "a read at the new leader sees the write the old one acknowledged"
    );
    assert!(
        world
            .history()
            .iter()
            .any(|op| !op.write && op.completed.is_some()),
        "the history records the read's completion"
    );
}

// ---- Act IV: the grid -------------------------------------------------------

/// A cluster of `size` nodes under `system`, with one client.
fn cluster_with(size: u64, system: QuorumSystem) -> World {
    let peers: Vec<NodeId> = (0..size).map(NodeId).collect();
    let configs = peers
        .iter()
        .map(|id| Config {
            id: *id,
            peers: peers.clone(),
            quorum_system: system,
            ..Config::default()
        })
        .collect();
    let mut world = World::new(configs, &[CLIENT], 10);
    world.set_policy(policy(&[]));
    world
}

const GRID: QuorumSystem = QuorumSystem::Grid { rows: 2, cols: 3 };

#[test]
fn a_grid_addresses_each_slot_to_its_own_column() {
    let mut world = cluster_with(6, GRID);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    // Slot 0 goes to column 0 = {0, 3}; the leader is in it, so exactly one
    // Accept leaves.
    let accepts: Vec<u64> = world
        .wire()
        .iter()
        .filter(|entry| matches!(entry.message, Message::Accept { slot: Slot(0), .. }))
        .map(|entry| entry.to.0)
        .collect();
    assert_eq!(accepts, vec![3], "slot 0 is addressed to column 0 alone");
    deliver_all(&mut world);
    world
        .propose(NodeId(0), CLIENT, "bravo", None)
        .expect("admitted");
    let accepts: Vec<u64> = world
        .wire()
        .iter()
        .filter(|entry| matches!(entry.message, Message::Accept { slot: Slot(1), .. }))
        .map(|entry| entry.to.0)
        .collect();
    assert_eq!(accepts, vec![1, 4], "slot 1 is addressed to column 1");
    deliver_all(&mut world);
    assert_eq!(
        applied_text(&world, 5),
        vec!["0:alpha".to_string(), "1:bravo".to_string()],
        "every node applies both slots, whichever column decided them"
    );
    // And the view says which column an Accept was addressed to.
    world
        .propose(NodeId(0), CLIENT, "charlie", None)
        .expect("admitted");
    let column = world
        .view()
        .wire
        .into_iter()
        .find(|message| message.kind == "Accept" && message.slot == Some(2))
        .and_then(|message| message.column);
    assert_eq!(column, Some(2), "slot 2 is column 2");
}

#[test]
fn a_vote_from_outside_the_column_does_not_count() {
    let mut world = cluster_with(6, GRID);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    // Slot 0 belongs to column 0 = {0, 3}. Misroute a copy of its Accept to
    // node 4, which is in row 1 and column 1: a member of the configuration,
    // and not one of this slot's acceptors.
    let accept = world
        .wire()
        .iter()
        .find(|entry| matches!(entry.message, Message::Accept { slot: Slot(0), .. }))
        .map(|entry| entry.id)
        .expect("slot 0's Accept");
    world
        .duplicate(accept, Some(4))
        .expect("a misrouted copy is legal");
    let stray = world
        .wire()
        .iter()
        .find(|entry| {
            entry.to == NodeId(4) && matches!(entry.message, Message::Accept { slot: Slot(0), .. })
        })
        .map(|entry| entry.id)
        .expect("the misrouted copy");
    world.deliver(stray).expect("in flight");
    // Node 4 votes: it is an acceptor, and the ballot is not below its
    // promise. Its Accepted goes back to the leader.
    let vote = world
        .wire()
        .iter()
        .find(|entry| {
            matches!(
                entry.message,
                Message::Accepted {
                    from: NodeId(4),
                    slot: Slot(0),
                    ..
                }
            )
        })
        .map(|entry| entry.id)
        .expect("node 4 answered the copy it was handed");
    world.deliver(vote).expect("in flight");
    assert!(
        world
            .node(NodeId(0))
            .expect("running")
            .replica()
            .chosen_at(Slot(0))
            .is_none(),
        "a vote from outside the column decides nothing, whoever cast it"
    );
    assert!(
        world
            .narration()
            .iter()
            .any(|event| event.text.contains("does not count that vote")),
        "and the game says why"
    );
    // Node 3 — the other half of column 0 — is what the slot waits for.
    deliver_all(&mut world);
    assert!(
        world
            .node(NodeId(0))
            .expect("running")
            .replica()
            .chosen_at(Slot(0))
            .is_some(),
        "the column completes and the slot is chosen"
    );
}

#[test]
fn a_column_a_grid_does_not_have_is_refused() {
    let mut world = cluster_with(6, GRID);
    elect(&mut world, 0);
    let err = world
        .propose(NodeId(0), CLIENT, "alpha", Some(3))
        .expect_err("this grid has three columns, numbered 0 to 2");
    assert_eq!(err.code, paros_play::ActionErrorCode::BadColumn);
    let mut plain = cluster(3);
    elect(&mut plain, 0);
    let err = plain
        .propose(NodeId(0), CLIENT, "alpha", Some(0))
        .expect_err("a majority names no columns");
    assert_eq!(err.code, paros_play::ActionErrorCode::BadColumn);
}

// ---- Act IV: quorum reads ---------------------------------------------------

#[test]
fn a_follower_serves_a_quorum_read_with_no_leader_involved() {
    let mut world = cluster_with(6, GRID);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    deliver_all(&mut world);
    let beats_before = world.clock();
    // Node 4 is a follower, and node 2 is in another row entirely.
    world
        .quorum_read(NodeId(4), CLIENT)
        .expect("any node may open a quorum read");
    assert!(
        world
            .wire()
            .iter()
            .all(|entry| !matches!(entry.message, Message::Heartbeat { .. })),
        "a quorum read broadcasts no beat"
    );
    assert!(
        world
            .wire()
            .iter()
            .any(|entry| matches!(entry.message, Message::PreRead { .. })),
        "it asks its row for their vote watermarks"
    );
    deliver_all(&mut world);
    let served = world.served_reads();
    assert_eq!(served.len(), 1, "the row answered and the read was served");
    assert_eq!(served[0].1, Some(Slot(0)), "at the highest slot voted");
    assert_eq!(world.clock(), beats_before, "no tick was needed");
    assert!(
        world
            .node(NodeId(0))
            .expect("running")
            .proposer()
            .read_rounds()
            .is_empty(),
        "the leader opened no read round"
    );
    world.linearizable().expect("the history is linearizable");
}

#[test]
fn a_quorum_read_waits_until_the_replica_covers_the_watermark() {
    let mut world = cluster_with(6, GRID);
    world.set_policy(policy(&[PromptKind::QuorumReadServe]));
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    // Node 3 votes for slot 0 and the decision never comes back, so node 3's
    // watermark is above everybody's applied prefix.
    deliver_where(&mut world, |message| {
        matches!(message, Message::Accept { .. })
    });
    drop_where(&mut world, |message| {
        matches!(message, Message::Accepted { .. })
    });
    // A read from row 1 = {3, 4, 5}: node 3 reports slot 0, nobody has applied
    // anything.
    world
        .quorum_read(NodeId(4), CLIENT)
        .expect("a follower opens a read");
    deliver_all(&mut world);
    let prompt = world.prompt().expect("the row answered: serve or wait?");
    assert_eq!(prompt.kind, PromptKind::QuorumReadServe);
    assert_eq!(prompt.expected(), "wait", "the prefix is behind the row");
    let id = prompt.id;
    assert_eq!(
        world.answer(id, "serve").expect("a legal move"),
        Verdict::Wrong
    );
    assert_eq!(
        world.answer(id, "wait").expect("a legal move"),
        Verdict::Right
    );
    assert_eq!(world.unserved_reads(), 1, "the read is still waiting");
    // Let the slot decide, and the read fires on its own.
    world.resend_pending(NodeId(0)).expect("a leader re-sends");
    deliver_all(&mut world);
    assert_eq!(world.served_reads().len(), 1, "covered, and served");
}

// ---- Act IV: the cooperative handoff ---------------------------------------

#[test]
fn a_handoff_moves_the_authority_without_a_second_phase_one() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    deliver_all(&mut world);
    let ballot = world.node(NodeId(0)).expect("running").ballot();
    world
        .relinquish(NodeId(0), NodeId(1))
        .expect("a settled leader may hand its authority on");
    // The abdication is synchronous with the decision.
    assert!(
        !world.node(NodeId(0)).expect("running").is_leader(),
        "the outgoing leader stopped leading in the same call"
    );
    deliver_all(&mut world);
    let successor = world.node(NodeId(1)).expect("running");
    assert!(
        successor.is_leader(),
        "the successor installed the authority"
    );
    assert_eq!(
        successor.ballot(),
        ballot,
        "under the same ballot: no second Phase 1"
    );
    assert!(
        world
            .wire()
            .iter()
            .chain(std::iter::empty())
            .all(|entry| !matches!(entry.message, Message::Prepare { .. })),
        "no Prepare was ever sent"
    );
    // And it can lead: a command decided under the inherited ballot.
    world
        .propose(NodeId(1), CLIENT, "bravo", None)
        .expect("the successor admits a proposal");
    deliver_all(&mut world);
    assert_eq!(
        applied_text(&world, 1),
        vec!["0:alpha".to_string(), "1:bravo".to_string()]
    );
}

#[test]
fn an_authority_is_handed_on_only_once() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .relinquish(NodeId(0), NodeId(1))
        .expect("the minter may hand its ballot on");
    deliver_all(&mut world);
    let reason = world
        .handoff_refusal(NodeId(1))
        .expect("a successor may not hand an inherited authority on");
    assert!(
        reason.contains("moves") || reason.contains("once"),
        "the reason is the one-hop rule: {reason}"
    );
    let err = world
        .relinquish(NodeId(1), NodeId(2))
        .expect_err("one hop only");
    assert_eq!(err.code, paros_play::ActionErrorCode::HandoffRefused);
    // A follower is refused too, with its own reason.
    let err = world
        .relinquish(NodeId(2), NodeId(0))
        .expect_err("a follower has no authority to give");
    assert_eq!(err.code, paros_play::ActionErrorCode::HandoffRefused);
}

// ---- Act IV: a faulty record -----------------------------------------------

#[test]
fn a_rotted_record_is_reported_faulty_and_repaired_in_place() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    // Node 2 alone votes, and nothing comes back: accepted at one node, chosen
    // nowhere. Node 1 never hears of the slot at all, which is what leaves the
    // election with nothing to go on later.
    drop_where(&mut world, |message| {
        matches!(message, Message::Accept { .. })
    });
    world
        .resend_pending(NodeId(0))
        .expect("a leader re-sends its pending accepts");
    let to_two = world
        .wire()
        .iter()
        .find(|entry| {
            entry.to == NodeId(2) && matches!(entry.message, Message::Accept { slot: Slot(0), .. })
        })
        .map(|entry| entry.id)
        .expect("slot 0's Accept for node 2");
    world.deliver(to_two).expect("in flight");
    drop_where(&mut world, |message| {
        matches!(
            message,
            Message::Accept { .. } | Message::Accepted { .. } | Message::Commit { .. }
        )
    });
    world.crash(NodeId(0)).expect("crashes");
    world.crash(NodeId(1)).expect("crashes");
    world.crash(NodeId(2)).expect("crashes");
    world
        .corrupt(NodeId(2), Slot(0))
        .expect("node 2 holds a record for slot 0");
    world.restart(NodeId(2)).expect("restarts");
    world.restart(NodeId(1)).expect("restarts");
    assert_eq!(
        world.faulty_records(NodeId(2)).len(),
        1,
        "the boot scan classified the rotted record"
    );
    // Node 1 campaigns with node 2. Node 2 reports the slot faulty, so the
    // election cannot settle it: the probe opens.
    world.start_election(NodeId(1)).expect("campaigns");
    deliver_all(&mut world);
    assert_eq!(
        world.blocked_repairs(NodeId(1)),
        1,
        "the damaged slot went to the repair probe"
    );
    // Node 0 comes back holding the value; the probe re-queries it on the next
    // beat and re-proposes what it reports.
    world.restart(NodeId(0)).expect("restarts");
    world.tick(NodeId(1)).expect("the leader beats");
    deliver_all(&mut world);
    assert_eq!(world.blocked_repairs(NodeId(1)), 0, "the probe closed");
    assert_eq!(
        applied_text(&world, 2),
        vec!["0:alpha".to_string()],
        "the slot re-decided as the value that was accepted there"
    );
    assert!(
        world.faulty_records(NodeId(2)).is_empty(),
        "and the damaged record was repaired in place"
    );
}

// ---- Act IV: the wiped node -------------------------------------------------

#[test]
fn a_wiped_node_may_never_rejoin() {
    let mut world = cluster(3);
    elect(&mut world, 0);
    world
        .propose(NodeId(0), CLIENT, "alpha", None)
        .expect("admitted");
    deliver_all(&mut world);
    let promised = world.promise_watermark(NodeId(2)).expect("a node");
    assert!(promised > Ballot::zero(), "node 2 has promised something");
    world.wipe(NodeId(2)).expect("a disk may be erased");
    assert_eq!(
        world
            .disk(NodeId(2))
            .expect("the disk is still there")
            .hard_state()
            .max_promised_ballot,
        Ballot::zero(),
        "the promise is gone from the only place it was written"
    );
    let err = world
        .restart(NodeId(2))
        .expect_err("a wiped member may not boot");
    assert_eq!(err.code, paros_play::ActionErrorCode::Amnesia);
    assert!(world.node(NodeId(2)).is_none(), "it stays out");
    assert_eq!(world.refused_boots().len(), 1);
    // The survivors keep going, and nothing regressed.
    world
        .propose(NodeId(0), CLIENT, "bravo", None)
        .expect("two of three is still a quorum");
    deliver_all(&mut world);
    assert_eq!(
        applied_text(&world, 0),
        vec!["0:alpha".to_string(), "1:bravo".to_string()]
    );
    assert_eq!(
        world.promise_regressed(),
        None,
        "no node's durable promise came back lower than one it had made"
    );
}
