//! Unit tests for the Multi-Paxos state machine. Tests are a child module of
//! `node`, so they may read `RawNode`'s private fields directly.

use std::collections::BTreeMap;

use super::{NodeRole, ProposeResult, RawNode};
use crate::message::Message;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{Ballot, ClientId, ClientSeq, Command, Control, Entry, NodeId, Slot, Value};

/// In-memory [`Storage`] seeded with an explicit initial state (for restart
/// tests): the durable scalars plus a per-slot accepted log.
struct TestStorage {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    config: Config,
    first_slot: Slot,
}

impl TestStorage {
    fn new(id: u64, members: &[u64]) -> Self {
        Self {
            hard_state: HardState::default(),
            accepted: BTreeMap::new(),
            config: Config {
                id: NodeId(id),
                peers: members.iter().copied().map(NodeId).collect(),
                quorum_system: crate::state::QuorumSystem::Majority,
            },
            first_slot: Slot(0),
        }
    }

    /// Snapshot a live node's durable state (scalars + accepted log + compaction
    /// floor) into a fresh storage, the way a real driver's persisted disk would
    /// look, for building the "restart from durable storage" path in tests.
    fn from_node(n: &RawNode) -> Self {
        Self {
            hard_state: n.hard_state().clone(),
            accepted: n.accepted().clone(),
            config: n.config().clone(),
            first_slot: n.first_slot(),
        }
    }
}

impl Storage for TestStorage {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state.clone(), self.config.clone())
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.accepted.get(&slot).cloned()
    }
    fn first_slot(&self) -> Slot {
        self.first_slot
    }
    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }
}

fn node(id: u64, members: &[u64]) -> RawNode {
    RawNode::new(&TestStorage::new(id, members))
}

fn val(b: u8) -> Value {
    Value(vec![b])
}

fn entry(client: u64, seq: u64, b: u8) -> Entry {
    Entry {
        client: ClientId(client),
        seq: ClientSeq(seq),
        value: val(b),
    }
}

/// A client [`Command`] wrapping [`entry`], the common per-slot value in tests.
fn ucmd(client: u64, seq: u64, b: u8) -> Command {
    Command::User(entry(client, seq, b))
}

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

/// Drain a node's pending messages and clear the batch.
fn drain(n: &mut RawNode) -> Vec<(NodeId, Message)> {
    let ready = n.ready();
    let msgs = ready.messages().to_vec();
    ready.advance();
    msgs
}

/// The chosen client value at `slot` on this node, if any (a control command has
/// no client value and reads back as `None`).
fn chosen_at(n: &RawNode, slot: u64) -> Option<Value> {
    n.chosen
        .get(&Slot(slot))
        .and_then(Command::user)
        .map(|e| e.value.clone())
}

/// Deliver `queue` to addressed recipients, dropping any `(to, msg)` for which
/// `keep` is false, enqueueing each delivery's resulting messages. Runs to
/// quiescence (a reliable network with a caller-controlled partition).
fn deliver_filtered(
    nodes: &mut [RawNode],
    mut queue: Vec<(NodeId, Message)>,
    keep: impl Fn(NodeId, &Message) -> bool,
) {
    while let Some((to, msg)) = queue.pop() {
        if !keep(to, &msg) {
            continue;
        }
        let idx = nodes
            .iter()
            .position(|n| n.config().id == to)
            .expect("message addressed to a cluster member");
        nodes[idx].step(msg);
        queue.extend(drain(&mut nodes[idx]));
    }
}

fn deliver_all(nodes: &mut [RawNode], queue: Vec<(NodeId, Message)>) {
    deliver_filtered(nodes, queue, |_, _| true);
}

/// Drive `nodes[idx]` to leadership in a healthy cluster, then beat once so the
/// followers learn who the leader is (a follower only adopts a leader on
/// `Accept`/`Heartbeat`, never on Phase 1).
fn make_leader(nodes: &mut [RawNode], idx: usize) {
    nodes[idx].set_election_timeout(1);
    nodes[idx].tick(); // fires CheckLeader -> Candidate, broadcasts Prepare
    let q = drain(&mut nodes[idx]);
    deliver_all(nodes, q);
    assert!(
        nodes[idx].is_leader(),
        "node {idx} should have won the election"
    );
    nodes[idx].tick(); // fires Heartbeat -> followers adopt the leader
    let q = drain(&mut nodes[idx]);
    deliver_all(nodes, q);
}

#[test]
fn election_fires_after_timeout_and_becomes_candidate() {
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(3);
    n.tick();
    n.tick();
    assert_eq!(n.role(), NodeRole::Follower, "not yet timed out");
    n.tick();
    assert_eq!(
        n.role(),
        NodeRole::Candidate,
        "election fired on the 3rd tick"
    );
    assert!(n.needs_election_timeout(), "driver must reseed the timeout");
    let out = drain(&mut n);
    // On the election timeout the candidate broadcasts a Prepare *and* a proactive
    // catch-up probe to each of the two peers (the probe heals a silently-behind
    // node that is not hearing a fresh leader commit): 4 messages total.
    let prepares: Vec<_> = out
        .iter()
        .filter(|(_, m)| matches!(m, Message::Prepare { from_slot, .. } if *from_slot == Slot(0)))
        .collect();
    let probes: Vec<_> = out
        .iter()
        .filter(|(_, m)| matches!(m, Message::CatchUpRequest { from_slot, .. } if *from_slot == Slot(0)))
        .collect();
    assert_eq!(prepares.len(), 2, "Prepare broadcast to the two peers");
    assert_eq!(probes.len(), 2, "catch-up probe broadcast to the two peers");
    assert_eq!(out.len(), 4, "only Prepare + catch-up probe are broadcast");
}

#[test]
fn promise_quorum_makes_leader() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    assert_eq!(nodes[0].role(), NodeRole::Leader);
    assert_eq!(nodes[0].leader(), Some(NodeId(0)));
}

#[test]
fn leader_streams_multiple_slots_and_all_nodes_agree() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    for (seq, b) in [(1u64, 10u8), (2, 20), (3, 30)] {
        let r = nodes[0].propose(ClientId(1), ClientSeq(seq), val(b));
        assert!(
            matches!(r, ProposeResult::Accepted(_)),
            "leader admits proposal"
        );
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }

    for n in &nodes {
        assert_eq!(chosen_at(n, 0), Some(val(10)));
        assert_eq!(chosen_at(n, 1), Some(val(20)));
        assert_eq!(chosen_at(n, 2), Some(val(30)));
        assert_eq!(
            n.hard_state().chosen_index,
            Some(Slot(2)),
            "the contiguous prefix reached slot 2"
        );
    }
}

#[test]
fn follower_fills_a_hole_via_commit_replay_catch_up() {
    // Pins the #18 bug: a follower that missed both the `Accept` and the `Commit`
    // for a decided slot keeps a permanent hole (the leader re-sends `Accept`s only
    // for still-pending slots, never a `Commit`), until commit-replay catch-up
    // heals it. The `ConvergenceOracle` catches this in simulation; this is the
    // deterministic unit pin.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slot 0: healthy — every node learns it.
    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Slot 1: drop *every* message addressed to node 2. Node 0 still decides with
    // the {0,1} quorum, so slot 1 is chosen — but node 2 misses both the `Accept`
    // and the `Commit`, opening a permanent hole.
    nodes[0].propose(ClientId(1), ClientSeq(2), val(20));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(2));
    assert_eq!(
        chosen_at(&nodes[0], 1),
        Some(val(20)),
        "leader chose slot 1"
    );
    assert_eq!(
        chosen_at(&nodes[2], 1),
        None,
        "follower 2 has a hole at slot 1"
    );

    // Slot 2: healthy again — node 2 receives it, but its *contiguous* prefix is
    // stuck behind the slot-1 hole.
    nodes[0].propose(ClientId(1), ClientSeq(3), val(30));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(
        nodes[2].hard_state().chosen_index,
        Some(Slot(0)),
        "follower 2's prefix stalls at the hole (slot 2 chosen out of order)"
    );

    // A heartbeat now advertises the leader's commit = slot 2. Node 2 sees it is
    // behind, requests the decided range, the leader replays slots 1..=2, and node
    // 2 fills the hole and converges to the cluster prefix.
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(
        chosen_at(&nodes[2], 1),
        Some(val(20)),
        "the hole is filled via commit-replay catch-up"
    );
    assert_eq!(
        nodes[2].hard_state().chosen_index,
        Some(Slot(2)),
        "follower 2 converged to the cluster's chosen prefix"
    );
}

#[test]
fn promise_and_accept_batches_require_fsync() {
    use crate::write::MustSync;

    // An acceptor promoting its promise on a higher Prepare must fsync before it
    // replies Promise.
    let mut n = node(0, &[0, 1, 2]);
    n.step(Message::Prepare {
        from: NodeId(1),
        ballot: ballot(3, 1),
        from_slot: Slot(0),
    });
    {
        let r = n.ready();
        assert_eq!(
            r.must_sync(),
            MustSync::Sync,
            "a promise-raise must fsync before Promise is sent"
        );
        r.advance();
    }

    // An acceptor accepting a value must fsync before it replies Accepted.
    n.step(Message::Accept {
        from: NodeId(1),
        ballot: ballot(3, 1),
        slot: Slot(0),
        command: ucmd(1, 1, 9),
    });
    {
        let r = n.ready();
        assert_eq!(
            r.must_sync(),
            MustSync::Sync,
            "an accepted-append must fsync before Accepted is sent"
        );
        r.advance();
    }
}

#[test]
fn non_leader_propose_redirects() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    // node 1 learned the leader via the election traffic.
    let r = nodes[1].propose(ClientId(1), ClientSeq(1), val(7));
    assert_eq!(r, ProposeResult::NotLeader(Some(NodeId(0))));
    assert!(
        drain(&mut nodes[1]).is_empty(),
        "a follower proposes nothing"
    );
}

#[test]
fn follower_resets_election_clock_on_leader_traffic() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    // Arm node 1 with a timeout of 3 ticks. It ages two ticks (still Follower),
    // then leader contact (a streamed Accept) resets its clock, so two more
    // ticks still leave it a Follower. Without the reset it would have hit 3.
    nodes[1].set_election_timeout(3);
    nodes[1].tick();
    nodes[1].tick();
    assert_eq!(
        nodes[1].role(),
        NodeRole::Follower,
        "two ticks is under the timeout"
    );
    let r = nodes[0].propose(ClientId(1), ClientSeq(1), val(9));
    assert!(matches!(r, ProposeResult::Accepted(_)));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    nodes[1].tick();
    nodes[1].tick();
    assert_eq!(
        nodes[1].role(),
        NodeRole::Follower,
        "leader contact reset the clock, so it still has not timed out"
    );
}

#[test]
fn dedup_returns_duplicate_for_inflight_and_chosen_for_applied() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    let r1 = nodes[0].propose(ClientId(7), ClientSeq(1), val(1));
    let ProposeResult::Accepted(slot) = r1 else {
        panic!("expected Accepted, got {r1:?}");
    };
    // A retry while still in flight maps to the same slot (no new allocation).
    let r2 = nodes[0].propose(ClientId(7), ClientSeq(1), val(1));
    assert_eq!(
        r2,
        ProposeResult::Duplicate(slot),
        "retry dedups to the same slot"
    );

    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Once chosen+applied, a retry is reported as already chosen (idempotent).
    let r3 = nodes[0].propose(ClientId(7), ClientSeq(1), val(1));
    assert_eq!(r3, ProposeResult::Chosen);
    // And no second slot was ever allocated for it.
    assert_eq!(nodes[0].next_slot, Slot(1), "exactly one slot consumed");
}

#[test]
fn new_leader_recovers_inflight_entry_under_its_ballot() {
    // 3-node cluster. Node 1 has accepted slot 0 at an old ballot but it was
    // never chosen. Node 2 wins a new election; its recovery must re-propose that
    // entry under node 2's higher ballot (gap fill / takeover).
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    let old = ballot(1, 0);
    let recovered = ucmd(5, 1, 99);
    nodes[1].step(Message::Accept {
        from: NodeId(0),
        ballot: old,
        slot: Slot(0),
        command: recovered.clone(),
    });
    let _ = drain(&mut nodes[1]); // Accepted reply, dropped (node 0 is gone)
    assert!(nodes[1].accepted().contains_key(&Slot(0)));

    // Node 2 campaigns. Deliver only to node 1 (node 0 is partitioned).
    nodes[2].set_election_timeout(1);
    nodes[2].tick();
    let q = drain(&mut nodes[2]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));

    assert!(nodes[2].is_leader(), "node 2 won with node 1's promise");
    // Node 2 re-proposed the recovered entry for slot 0 under its own ballot.
    let (b, e) = nodes[2]
        .accepted()
        .get(&Slot(0))
        .expect("slot 0 re-accepted");
    assert_eq!(
        e, &recovered,
        "recovered value re-proposed, not overwritten"
    );
    assert!(*b > old, "re-proposed under the new, higher ballot");
    assert_eq!(
        nodes[2].next_slot,
        Slot(1),
        "next_slot is past the recovered slot"
    );
}

#[test]
fn recovery_picks_highest_ballot_value_per_slot() {
    // 5-node cluster (quorum 3): node 4 self + 2 promises. Two promises report
    // different values for slot 0 at different ballots; the higher-ballot value
    // must win the recovery merge.
    let mut n = node(4, &[0, 1, 2, 3, 4]);
    n.set_election_timeout(1);
    n.tick(); // Candidate at ballot {1,4}, Prepare from_slot 0
    let _ = drain(&mut n);
    let camp = n.ballot();

    let low = (ballot(1, 0), ucmd(1, 1, 1));
    let high = (ballot(1, 3), ucmd(1, 1, 2));
    let mut acc_low = BTreeMap::new();
    acc_low.insert(Slot(0), low);
    let mut acc_high = BTreeMap::new();
    acc_high.insert(Slot(0), high.clone());
    n.step(Message::Promise {
        from: NodeId(0),
        ballot: camp,
        from_slot: Slot(0),
        accepted: acc_low,
    });
    assert!(!n.is_leader(), "one promise short of quorum");
    n.step(Message::Promise {
        from: NodeId(3),
        ballot: camp,
        from_slot: Slot(0),
        accepted: acc_high,
    });
    assert!(n.is_leader(), "quorum reached");
    let (_, e) = n.accepted().get(&Slot(0)).expect("slot 0 re-accepted");
    assert_eq!(
        e, &high.1,
        "the highest-ballot accepted value is re-proposed"
    );
}

#[test]
fn chosen_index_advances_only_over_contiguous_prefix() {
    // Learn slots 0 and 2 (gap at 1): the applied prefix stops at 0. Filling
    // slot 1 then jumps it to 2.
    let mut n = node(1, &[0, 1, 2]);
    let b = ballot(3, 0);
    n.step(Message::Commit {
        from: NodeId(0),
        ballot: b,
        slot: Slot(0),
        command: ucmd(1, 1, 10),
    });
    n.step(Message::Commit {
        from: NodeId(0),
        ballot: b,
        slot: Slot(2),
        command: ucmd(1, 3, 30),
    });
    assert_eq!(
        n.hard_state().chosen_index,
        Some(Slot(0)),
        "gap at slot 1 holds the prefix at slot 0"
    );
    n.step(Message::Commit {
        from: NodeId(0),
        ballot: b,
        slot: Slot(1),
        command: ucmd(1, 2, 20),
    });
    assert_eq!(
        n.hard_state().chosen_index,
        Some(Slot(2)),
        "filling the gap advances the prefix to slot 2"
    );
}

#[test]
fn nack_steps_a_candidate_down_instead_of_stalling() {
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(1);
    n.tick(); // Candidate
    let _ = drain(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate);
    let camp = n.ballot();
    n.step(Message::Nack {
        from: NodeId(1),
        ballot: camp,
        slot: Slot(0),
    });
    assert_eq!(
        n.role(),
        NodeRole::Follower,
        "a Nack of our campaign steps us down (livelock fix)"
    );
    assert!(
        n.needs_election_timeout(),
        "and asks for a fresh randomized timeout"
    );
}

#[test]
fn leader_never_lowers_its_promise_on_self_accept() {
    // A leader streams, but a competing higher Prepare raises its promise; the
    // next self-accept must not pull the promise back down.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    let higher = ballot(99, 2);
    nodes[0].step(Message::Prepare {
        from: NodeId(2),
        ballot: higher,
        from_slot: Slot(0),
    });
    let _ = drain(&mut nodes[0]);
    assert_eq!(nodes[0].hard_state().max_promised_ballot, higher);
    // The leader (now superseded) tries to stream; self-accept must be skipped.
    let _ = nodes[0].propose(ClientId(1), ClientSeq(1), val(1));
    assert_eq!(
        nodes[0].hard_state().max_promised_ballot,
        higher,
        "self-accept never lowers the promise"
    );
}

#[test]
fn restart_rebuilds_state_from_hard_state() {
    // A node that had chosen slots 0..=1 and accepted (uncommitted) slot 2
    // recovers ballot, next_slot, and dedup tables on construction.
    let mut accepted = BTreeMap::new();
    accepted.insert(Slot(0), (ballot(2, 0), ucmd(1, 1, 10)));
    accepted.insert(Slot(1), (ballot(2, 0), ucmd(1, 2, 20)));
    accepted.insert(Slot(2), (ballot(2, 0), ucmd(1, 3, 30)));
    let hard_state = HardState {
        max_promised_ballot: ballot(2, 0),
        chosen_index: Some(Slot(1)),
    };
    let storage = TestStorage {
        hard_state,
        accepted,
        config: Config {
            id: NodeId(1),
            peers: vec![NodeId(0), NodeId(1), NodeId(2)],
            quorum_system: crate::state::QuorumSystem::Majority,
        },
        first_slot: Slot(0),
    };
    let n = RawNode::new(&storage);
    assert_eq!(n.ballot(), ballot(2, 0), "resumes the promised ballot");
    assert_eq!(
        n.next_slot,
        Slot(3),
        "next_slot is past the highest accepted slot"
    );
    assert_eq!(n.role(), NodeRole::Follower);
    // Dedup: applied seqs for the chosen prefix; slot 2 still in flight.
    assert_eq!(n.applied_seq.get(&ClientId(1)), Some(&ClientSeq(2)));
    assert_eq!(n.inflight.get(&(ClientId(1), ClientSeq(3))), Some(&Slot(2)));
}

#[test]
fn single_node_cluster_elects_and_chooses_immediately() {
    let mut n = node(0, &[0]);
    n.set_election_timeout(1);
    n.tick();
    assert!(n.is_leader(), "a single node wins its own election");
    let r = n.propose(ClientId(1), ClientSeq(1), val(42));
    assert_eq!(r, ProposeResult::Accepted(Slot(0)));
    assert_eq!(
        chosen_at(&n, 0),
        Some(val(42)),
        "chosen immediately (quorum of one)"
    );
}

#[test]
fn acceptor_rejects_below_promised_ballot() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(Message::Prepare {
        from: NodeId(1),
        ballot: ballot(5, 1),
        from_slot: Slot(0),
    });
    let _ = drain(&mut n);
    n.step(Message::Accept {
        from: NodeId(2),
        ballot: ballot(3, 2),
        slot: Slot(0),
        command: ucmd(1, 1, 9),
    });
    assert!(
        !n.accepted().contains_key(&Slot(0)),
        "must not accept below the promised ballot"
    );
    let out = drain(&mut n);
    assert!(matches!(out.as_slice(), [(_, Message::Nack { .. })]));
}

/// SDD regression for the "stale chosen value resurrected on restart" bug, first
/// proven as a DST safety-oracle violation under crash/restart chaos. A node holds
/// a stale lower-ballot accept from a failed earlier ballot, then learns via
/// `Commit` that a *different* value was chosen for that slot. `mark_chosen` must
/// record the chosen value as the authoritative accepted entry, because a restart
/// rebuilds `chosen` from `accepted`; keeping the stale entry would resurrect a
/// value the cluster never chose for the slot.
#[test]
fn chosen_value_survives_restart_over_a_stale_accept() {
    let mut n = node(0, &[0, 1, 2]);

    // Accept a value at a low ballot that is never chosen (its proposer died
    // before reaching a quorum); this node was the only acceptor.
    n.step(Message::Accept {
        from: NodeId(1),
        ballot: ballot(1, 1),
        slot: Slot(0),
        command: ucmd(9, 9, 1),
    });

    // Learn a DIFFERENT value was chosen for slot 0 at a higher ballot (this node
    // was not in the choosing quorum, so it never accepted that value).
    n.step(Message::Commit {
        from: NodeId(2),
        ballot: ballot(2, 2),
        slot: Slot(0),
        command: ucmd(7, 7, 2),
    });
    assert_eq!(
        chosen_at(&n, 0),
        Some(val(2)),
        "learns the chosen value live"
    );

    // Restart: rebuild from the durable state (scalars + accepted log).
    let storage = TestStorage::from_node(&n);
    let restarted = RawNode::new(&storage);

    assert_eq!(
        chosen_at(&restarted, 0),
        Some(val(2)),
        "restart must rebuild the chosen value, not the stale never-chosen accept"
    );
}

/// Drive a fresh 3-node cluster with node 0 as leader and get slots 0..=2 chosen
/// everywhere, then return the cluster (`chosen_index` is `Some(Slot(2))`).
fn cluster_with_three_chosen() -> [RawNode; 3] {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    for (seq, b) in [(1u64, 10u8), (2, 20), (3, 30)] {
        let _ = nodes[0].propose(ClientId(1), ClientSeq(seq), val(b));
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }
    nodes
}

#[test]
fn compact_clamps_to_chosen_index_and_prunes_both_maps() {
    let mut nodes = cluster_with_three_chosen();
    let leader = &mut nodes[0];
    assert_eq!(leader.hard_state().chosen_index, Some(Slot(2)));

    // A partial compaction drops slots 0..=1 and keeps slot 2.
    let floor = leader.compact(Slot(1));
    assert_eq!(floor, Slot(2), "floor is one past the last dropped slot");
    assert_eq!(leader.first_slot(), Slot(2));
    assert_eq!(
        leader.accepted().keys().copied().collect::<Vec<_>>(),
        vec![Slot(2)],
        "only slot 2 is retained in the accepted log"
    );
    assert_eq!(
        leader.chosen.keys().copied().collect::<Vec<_>>(),
        vec![Slot(2)],
        "only slot 2 is retained in the chosen map"
    );

    // Over-requesting past the chosen index clamps to it: everything drops.
    let floor = leader.compact(Slot(100));
    assert_eq!(floor, Slot(3), "clamped to chosen_index + 1");
    assert_eq!(leader.first_slot(), Slot(3));
    assert!(leader.accepted().is_empty());
    assert!(leader.chosen.is_empty());
}

#[test]
fn truncate_control_command_raises_the_floor_cluster_wide_on_apply() {
    // Leader-driven, Paxos-decided truncation: the leader decides a
    // `Truncate` control command into the log; every node truncates lazily when it
    // applies that slot (the fused-node analogue of a cluster-wide floor).
    let mut nodes = cluster_with_three_chosen();
    for n in &nodes {
        assert_eq!(n.first_slot(), Slot(0), "no truncation yet");
        assert_eq!(n.hard_state().chosen_index, Some(Slot(2)));
    }

    // The leader admits the truncate as a control command at the next slot (3).
    let r = nodes[0].propose_control(Control::Truncate { up_to: Slot(1) });
    assert!(
        matches!(r, ProposeResult::Accepted(Slot(3))),
        "control command takes the next free slot"
    );
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Once every node applied slot 3, it lazily compacted up to the decided
    // watermark (slot 1): the floor rose to 2 everywhere, and the control slot
    // itself is chosen.
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.first_slot(),
            Slot(2),
            "node {i} truncated to the decided floor"
        );
        assert_eq!(
            n.hard_state().chosen_index,
            Some(Slot(3)),
            "node {i} chose the control slot"
        );
        assert!(
            !n.accepted().contains_key(&Slot(0)),
            "node {i} dropped slot 0"
        );
        assert!(
            !n.accepted().contains_key(&Slot(1)),
            "node {i} dropped slot 1"
        );
        assert!(n.accepted().contains_key(&Slot(2)), "node {i} keeps slot 2");
    }
}

#[test]
fn propose_control_is_leader_only() {
    let mut nodes = cluster_with_three_chosen();
    // A follower refuses to admit a control command and redirects to the leader.
    let r = nodes[1].propose_control(Control::Truncate { up_to: Slot(1) });
    assert!(
        matches!(r, ProposeResult::NotLeader(Some(NodeId(0)))),
        "a non-leader redirects the truncate to the leader"
    );
    assert_eq!(
        nodes[1].first_slot(),
        Slot(0),
        "no truncation on a redirect"
    );
}

#[test]
fn compact_below_the_current_floor_is_a_no_op() {
    use crate::write::WriteOp;

    let mut nodes = cluster_with_three_chosen();
    let leader = &mut nodes[0];
    leader.compact(Slot(1)); // floor -> 2
    let _ = drain(leader); // clear the pending Truncate write

    let floor = leader.compact(Slot(0)); // below the current floor
    assert_eq!(floor, Slot(2), "the floor does not move backward");
    let r = leader.ready();
    assert!(
        !r.writes()
            .iter()
            .any(|w| matches!(w, WriteOp::Truncate { .. })),
        "a below-floor compaction emits no Truncate write"
    );
    r.advance();
}

#[test]
fn a_compact_batch_requires_fsync() {
    use crate::write::{MustSync, WriteOp};

    let mut nodes = cluster_with_three_chosen();
    let leader = &mut nodes[0];
    let _ = drain(leader); // clear any pending writes from the proposal round

    let floor = leader.compact(Slot(2));
    assert_eq!(floor, Slot(3));
    let r = leader.ready();
    assert!(
        r.writes()
            .iter()
            .any(|w| matches!(w, WriteOp::Truncate { first } if *first == Slot(3))),
        "the batch carries the Truncate delta"
    );
    assert_eq!(
        r.must_sync(),
        MustSync::Sync,
        "a truncate must fsync before the batch's messages are sent"
    );
    r.advance();
}

#[test]
fn restart_from_truncated_storage_rebuilds_floor_and_next_slot() {
    let mut nodes = cluster_with_three_chosen();
    let leader = &mut nodes[0];
    leader.compact(Slot(2)); // full compaction: floor -> 3, accepted empty
    assert!(leader.accepted().is_empty());

    let storage = TestStorage::from_node(leader);
    let restarted = RawNode::new(&storage);
    assert_eq!(restarted.first_slot(), Slot(3), "floor survives a restart");
    assert!(
        restarted.accepted().is_empty(),
        "the truncated log rebuilds empty"
    );
    assert_eq!(
        restarted.hard_state().chosen_index,
        Some(Slot(2)),
        "the chosen index is unchanged by truncation"
    );
    assert_eq!(
        restarted.next_slot,
        Slot(3),
        "next_slot falls back to first-unchosen when the log is empty"
    );
}

#[test]
fn serve_catchup_sends_nothing_below_the_floor() {
    let mut nodes = cluster_with_three_chosen();
    let leader = &mut nodes[0];
    leader.compact(Slot(1)); // floor -> 2 (slot 2 retained)
    let _ = drain(leader);

    // A request below the floor cannot be served: the decided entries are gone.
    leader.step(Message::CatchUpRequest {
        from: NodeId(1),
        from_slot: Slot(0),
    });
    let out = drain(leader);
    assert!(
        !out.iter()
            .any(|(_, m)| matches!(m, Message::CatchUpResponse { .. })),
        "a below-floor catch-up request is answered with nothing"
    );

    // A request at the floor is served normally (positive control).
    leader.step(Message::CatchUpRequest {
        from: NodeId(1),
        from_slot: Slot(2),
    });
    let out = drain(leader);
    assert!(
        out.iter().any(|(_, m)| matches!(
            m,
            Message::CatchUpResponse { entries, .. } if entries.contains_key(&Slot(2))
        )),
        "a request at the floor still gets the retained decided entry"
    );
}

#[test]
fn prepare_below_floor_is_nacked_not_promised() {
    let mut nodes = cluster_with_three_chosen();
    let n = &mut nodes[0];
    n.compact(Slot(1)); // floor -> 2
    let _ = drain(n);
    let promise_before = n.hard_state().max_promised_ballot;

    // A higher ballot that would normally win a promise, but its from_slot is
    // below our floor: those slots are chosen and we truncated them.
    n.step(Message::Prepare {
        from: NodeId(1),
        ballot: ballot(9, 1),
        from_slot: Slot(0),
    });
    let out = drain(n);
    assert!(
        matches!(out.as_slice(), [(_, Message::Nack { slot, .. })] if *slot == Slot(0)),
        "a below-floor prepare is nacked, not promised"
    );
    assert_eq!(
        n.hard_state().max_promised_ballot,
        promise_before,
        "a below-floor prepare must not raise the promise (else a blind laggard deposes the leader)"
    );
}

#[test]
fn accept_below_floor_is_ignored() {
    let mut nodes = cluster_with_three_chosen();
    let n = &mut nodes[0];
    n.compact(Slot(2)); // floor -> 3, whole prefix truncated
    let _ = drain(n);

    n.step(Message::Accept {
        from: NodeId(1),
        ballot: ballot(9, 1),
        slot: Slot(1),
        command: ucmd(1, 1, 99),
    });
    let out = drain(n);
    assert!(
        out.is_empty(),
        "a below-floor accept is ignored: no Accepted and no Nack"
    );
    assert!(
        !n.accepted().contains_key(&Slot(1)),
        "a below-floor accept records nothing"
    );
}

#[test]
fn commit_below_floor_is_not_relearned() {
    let mut nodes = cluster_with_three_chosen();
    let n = &mut nodes[0];
    n.compact(Slot(2)); // floor -> 3
    let _ = drain(n);

    n.step(Message::Commit {
        from: NodeId(1),
        ballot: ballot(1, 0),
        slot: Slot(1),
        command: ucmd(7, 7, 88),
    });
    assert!(
        chosen_at(n, 1).is_none(),
        "a below-floor commit is not relearned"
    );
    assert!(
        !n.accepted().contains_key(&Slot(1)),
        "a below-floor commit records nothing below the floor"
    );
}

/// The safety crux of Stage 5, as a directed core reproduction: a quorum that
/// truncated a chosen slot refuses a candidate blind to that slot. Without the
/// floor guard this candidate would win with an empty-looking Promise and
/// re-propose a different value into the already-chosen slot (two values chosen
/// for one slot, the DST-found `18153519926117387038` violation).
#[test]
fn truncated_quorum_refuses_a_blind_candidate() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slots 0 and 1 chosen everywhere.
    for (seq, b) in [(1u64, 10u8), (2, 20)] {
        let _ = nodes[0].propose(ClientId(1), ClientSeq(seq), val(b));
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }
    // Slot 2 chosen on the quorum {0, 1} only: drop everything addressed to node 2.
    let _ = nodes[0].propose(ClientId(1), ClientSeq(3), val(30));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(2));
    assert_eq!(nodes[0].hard_state().chosen_index, Some(Slot(2)));
    assert_eq!(nodes[1].hard_state().chosen_index, Some(Slot(2)));
    assert_eq!(
        nodes[2].hard_state().chosen_index,
        Some(Slot(1)),
        "node 2 missed slot 2"
    );

    // The caught-up quorum {0, 1} compacts past slot 2, dropping the record.
    nodes[0].compact(Slot(2));
    nodes[1].compact(Slot(2));
    let _ = drain(&mut nodes[0]);
    let _ = drain(&mut nodes[1]);
    assert_eq!(nodes[0].first_slot(), Slot(3));
    assert_eq!(nodes[1].first_slot(), Slot(3));

    // Node 2, blind to slot 2, campaigns: its Prepare's from_slot is 2, below the
    // quorum's floor (3).
    nodes[2].set_election_timeout(1);
    nodes[2].tick();
    assert_eq!(nodes[2].role(), NodeRole::Candidate);
    let q = drain(&mut nodes[2]);
    assert!(
        q.iter().any(|(_, m)| matches!(
            m,
            Message::Prepare { from_slot, .. } if *from_slot == Slot(2)
        )),
        "node 2 prepares from its hole at slot 2"
    );
    deliver_all(&mut nodes, q);

    // The quorum Nacked (below floor), so the blind candidate is deposed, never
    // becomes leader, and never learns a (possibly different) value for slot 2.
    assert_eq!(
        nodes[2].role(),
        NodeRole::Follower,
        "a blind candidate is deposed by the below-floor Nacks, never leads"
    );
    assert!(
        chosen_at(&nodes[2], 2).is_none(),
        "slot 2 is not re-chosen with a new value on the blind node"
    );
}
