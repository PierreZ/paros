//! Unit tests for the Multi-Paxos state machine. Tests are a child module of
//! `node`, so they may read `RawNode`'s private fields directly.

use std::collections::BTreeMap;

use super::{NodeRole, ProposeResult, RawNode};
use crate::message::Message;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{Ballot, ClientId, ClientSeq, Entry, NodeId, Slot, Value};

/// In-memory [`Storage`] seeded with an explicit initial state (for restart tests).
struct TestStorage {
    hard_state: HardState,
    config: Config,
}

impl TestStorage {
    fn new(id: u64, members: &[u64]) -> Self {
        Self {
            hard_state: HardState::default(),
            config: Config {
                id: NodeId(id),
                peers: members.iter().copied().map(NodeId).collect(),
            },
        }
    }
}

impl Storage for TestStorage {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state.clone(), self.config.clone())
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Entry)> {
        self.hard_state.accepted.get(&slot).cloned()
    }
    fn first_slot(&self) -> Slot {
        Slot(0)
    }
    fn last_slot(&self) -> Slot {
        self.hard_state
            .accepted
            .keys()
            .next_back()
            .copied()
            .unwrap_or(Slot(0))
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        None
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

/// The chosen value at `slot` on this node, if any.
fn chosen_at(n: &RawNode, slot: u64) -> Option<Value> {
    n.chosen.get(&Slot(slot)).map(|e| e.value.clone())
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
    assert_eq!(out.len(), 2, "Prepare broadcast to the two peers");
    assert!(
        out.iter()
            .all(|(_, m)| matches!(m, Message::Prepare { from_slot, .. } if *from_slot == Slot(0))),
        "Prepare covers the whole log from slot 0"
    );
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
    let recovered = entry(5, 1, 99);
    nodes[1].step(Message::Accept {
        from: NodeId(0),
        ballot: old,
        slot: Slot(0),
        entry: recovered.clone(),
    });
    let _ = drain(&mut nodes[1]); // Accepted reply, dropped (node 0 is gone)
    assert!(nodes[1].hard_state().accepted.contains_key(&Slot(0)));

    // Node 2 campaigns. Deliver only to node 1 (node 0 is partitioned).
    nodes[2].set_election_timeout(1);
    nodes[2].tick();
    let q = drain(&mut nodes[2]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));

    assert!(nodes[2].is_leader(), "node 2 won with node 1's promise");
    // Node 2 re-proposed the recovered entry for slot 0 under its own ballot.
    let (b, e) = nodes[2]
        .hard_state()
        .accepted
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

    let low = (ballot(1, 0), entry(1, 1, 1));
    let high = (ballot(1, 3), entry(1, 1, 2));
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
    let (_, e) = n
        .hard_state()
        .accepted
        .get(&Slot(0))
        .expect("slot 0 re-accepted");
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
        entry: entry(1, 1, 10),
    });
    n.step(Message::Commit {
        from: NodeId(0),
        ballot: b,
        slot: Slot(2),
        entry: entry(1, 3, 30),
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
        entry: entry(1, 2, 20),
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
    accepted.insert(Slot(0), (ballot(2, 0), entry(1, 1, 10)));
    accepted.insert(Slot(1), (ballot(2, 0), entry(1, 2, 20)));
    accepted.insert(Slot(2), (ballot(2, 0), entry(1, 3, 30)));
    let hard_state = HardState {
        max_promised_ballot: ballot(2, 0),
        accepted,
        chosen_index: Some(Slot(1)),
    };
    let storage = TestStorage {
        hard_state,
        config: Config {
            id: NodeId(1),
            peers: vec![NodeId(0), NodeId(1), NodeId(2)],
        },
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
        entry: entry(1, 1, 9),
    });
    assert!(
        !n.hard_state().accepted.contains_key(&Slot(0)),
        "must not accept below the promised ballot"
    );
    let out = drain(&mut n);
    assert!(matches!(out.as_slice(), [(_, Message::Nack { .. })]));
}
