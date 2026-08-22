//! Unit tests for the Multi-Paxos state machine. Tests are a child module of
//! `node`, so they may read `RawNode`'s private fields directly.

use std::collections::BTreeMap;

use super::{NodeRole, ProposeResult, READ_ROUND_TTL_TICKS, RawNode, ReadIndexResult, ReadState};
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

/// Build the #54 wedge: slot 1 reaches the leader alone, slot 2 reaches a
/// follower and is chosen, then the leader dies and the survivors elect. Returns
/// the three nodes with node 1 freshly elected. The hole at slot 1 is what the
/// promise quorum {1,2} never saw — and what `next_slot` (3, from the recovered
/// slot 2) jumped clean over.
fn wedge_after_election() -> [RawNode; 3] {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slot 0: healthy, so every node's chosen prefix starts at slot 0.
    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Slot 1: every `Accept` is lost, so only the leader holds it. Undecided.
    nodes[0].propose(ClientId(1), ClientSeq(2), val(20));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, msg| {
        !matches!(msg, Message::Accept { .. })
    });

    // Slot 2: the `Accept` reaches node 1 only — enough for the {0,1} quorum, so
    // slot 2 *is* chosen and both followers learn it from the `Commit`.
    nodes[0].propose(ClientId(1), ClientSeq(3), val(30));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, msg| {
        !(matches!(msg, Message::Accept { .. }) && to == NodeId(2))
    });
    assert_eq!(
        chosen_at(&nodes[1], 2),
        Some(val(30)),
        "slot 2 is chosen even though slot 1 never left the leader"
    );

    // Node 0 dies. Node 1 campaigns; the promise quorum is {1,2}, neither of which
    // ever saw slot 1, so `Election::recovered` holds only slot 2.
    nodes[1].set_election_timeout(1);
    nodes[1].tick();
    let q = drain(&mut nodes[1]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));
    assert!(nodes[1].is_leader(), "the survivors elected node 1");
    nodes
}

#[test]
fn election_fills_a_hole_the_promise_quorum_never_reported() {
    // Pins the #54 bug: `try_become_leader` used to re-propose only the slots in
    // `Election::recovered`, so a slot that reached the old leader alone — below a
    // later slot that *did* reach the promise quorum — was neither recovered nor
    // re-allocated (`next_slot` jumped over it). Nothing would ever propose it
    // again, freezing the contiguous chosen prefix one below it forever. The new
    // leader now fills it with a `Control::Noop`. The `GapFillOracle` catches this
    // in simulation (seed 53); this is the deterministic unit pin.
    let mut nodes = wedge_after_election();

    assert_eq!(
        nodes[1].election_gap_fills(),
        1,
        "the election found exactly one hole (slot 1) and filled it"
    );
    assert!(
        matches!(
            nodes[1].accepted.get(&Slot(1)),
            Some((_, Command::Control(Control::Noop)))
        ),
        "the hole was filled with a no-op, not a client value"
    );

    // The fill is an ordinary Phase-2 round: node 2 accepts it, the quorum decides,
    // and the frozen prefix finally walks past the hole.
    let q = drain(&mut nodes[1]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));
    assert_eq!(
        nodes[1].hard_state().chosen_index,
        Some(Slot(2)),
        "the chosen prefix advances over the filled hole to the recovered suffix"
    );
    assert_eq!(
        nodes[1].chosen_gap(),
        None,
        "no chosen slot is stranded above the applied prefix any more"
    );

    // And the leader can serve fresh proposals past it, which is what the wedge
    // used to make impossible.
    nodes[1].propose(ClientId(1), ClientSeq(4), val(40));
    let q = drain(&mut nodes[1]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));
    assert_eq!(
        nodes[1].hard_state().chosen_index,
        Some(Slot(3)),
        "the log keeps growing after the fill"
    );
}

#[test]
fn a_slot_filled_with_a_noop_frees_its_inflight_client_request() {
    // The dedup half of #54. `RawNode::new` rebuilds `inflight` from every
    // accepted-but-unchosen entry, so the restarted old leader boots holding
    // `(client 1, seq 2) -> slot 1`. When slot 1 decides as a `Noop`, that mapping
    // must go: keeping it would answer the client's retry with `Duplicate(slot 1)`,
    // a reply parked on a slot whose commit never acks a proposer (the driver skips
    // waiters for control commands), so it would hang forever. Cleared by slot, the
    // retry takes a fresh slot and commits.
    let mut storage = TestStorage::new(0, &[0, 1, 2]);
    storage.hard_state.chosen_index = Some(Slot(0));
    storage.hard_state.max_promised_ballot = ballot(1, 0);
    storage
        .accepted
        .insert(Slot(0), (ballot(1, 0), ucmd(1, 1, 10)));
    storage
        .accepted
        .insert(Slot(1), (ballot(1, 0), ucmd(1, 2, 20)));
    let mut n = RawNode::new(&storage);
    assert_eq!(
        n.inflight.get(&(ClientId(1), ClientSeq(2))),
        Some(&Slot(1)),
        "the boot rebuilt the in-flight mapping from the unchosen accepted entry"
    );

    // The cluster decided a no-op at slot 1 under a later ballot; we learn it.
    n.step(Message::Commit {
        from: NodeId(1),
        ballot: ballot(2, 1),
        slot: Slot(1),
        command: Command::Control(Control::Noop),
    });
    assert_eq!(
        n.inflight.get(&(ClientId(1), ClientSeq(2))),
        None,
        "the decision frees the slot's in-flight client request, whatever was decided"
    );

    // As leader, the client's retry is admitted at a fresh slot rather than being
    // parked on the no-op's slot forever.
    let mut nodes = [n, node(1, &[0, 1, 2]), node(2, &[0, 1, 2])];
    make_leader(&mut nodes, 0);
    assert_eq!(
        nodes[0].propose(ClientId(1), ClientSeq(2), val(20)),
        ProposeResult::Accepted(Slot(2)),
        "the retry is re-proposed at a fresh slot, not deduped onto the no-op's"
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
    assert_eq!(
        r3,
        ProposeResult::Chosen(slot),
        "the idempotent ack names the slot the command applied at"
    );
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
        promised: camp,
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
fn nack_ratchets_the_next_campaign_past_the_acceptors_promised_round() {
    // A candidate nacked by an acceptor already promised at a much higher round
    // must not climb one round per election timeout: it should jump straight
    // past the reported promise on its next campaign.
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(1);
    n.tick(); // Candidate at round 1
    let _ = drain(&mut n);
    let camp = n.ballot();
    n.step(Message::Nack {
        from: NodeId(1),
        ballot: camp,
        promised: ballot(50, 1),
        slot: Slot(0),
    });
    assert_eq!(n.role(), NodeRole::Follower, "the Nack steps us down");
    let _ = drain(&mut n);

    n.tick(); // election timeout fires again -> next campaign
    assert_eq!(
        n.role(),
        NodeRole::Candidate,
        "the fresh election timeout starts a new campaign"
    );
    assert_eq!(
        n.ballot(),
        ballot(51, 0),
        "the next campaign starts past the acceptor's promised round, not one above our own"
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
    assert_eq!(
        n.applied_seq.get(&ClientId(1)),
        Some(&(ClientSeq(2), Slot(1)))
    );
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

#[test]
fn below_floor_catchup_request_offers_a_snapshot() {
    // A node truncated its chosen prefix; a peer that missed it asks for a slot
    // below the floor. We cannot replay the truncated entries, so we offer a
    // snapshot at our chosen prefix instead of serving nothing.
    let mut nodes = cluster_with_three_chosen();
    let n = &mut nodes[0];
    n.compact(Slot(2)); // floor -> 3, chosen_index 2
    let _ = drain(n);

    n.step(Message::CatchUpRequest {
        from: NodeId(1),
        from_slot: Slot(0), // below our floor
    });
    assert_eq!(n.pending_snapshot_offers.len(), 1, "a snapshot was offered");
    let (to, chosen_index, _ballot) = n.pending_snapshot_offers[0];
    assert_eq!(to, NodeId(1), "offered to the requester");
    assert_eq!(
        chosen_index,
        Slot(2),
        "snapshot brings it up to our chosen prefix"
    );
    // The offer carries no bytes (the driver attaches them); no CatchUpResponse.
    let out = drain(n);
    assert!(
        !out.iter()
            .any(|(_, m)| matches!(m, Message::CatchUpResponse { .. })),
        "a below-floor request is answered by a snapshot offer, not a replay"
    );
}

#[test]
fn install_snapshot_jumps_a_below_floor_node_and_never_lowers_the_promise() {
    use crate::write::{MustSync, WriteOp};

    // A node that missed a truncated prefix installs a snapshot at chosen_index 5
    // under ballot {3,0}: it jumps its chosen prefix, fully compacts to floor 6,
    // and adopts the ballot as its promise.
    let mut n = node(1, &[0, 1, 2]);
    assert_eq!(n.hard_state().chosen_index, None);

    n.step(Message::InstallSnapshot {
        from: NodeId(0),
        ballot: ballot(3, 0),
        chosen_index: Slot(5),
        snapshot: Value(vec![1, 2, 3]),
    });
    assert_eq!(
        n.hard_state().chosen_index,
        Some(Slot(5)),
        "jumped to the snapshot's chosen prefix"
    );
    assert_eq!(
        n.first_slot(),
        Slot(6),
        "fully compacted up to the snapshot"
    );
    assert_eq!(
        n.hard_state().max_promised_ballot,
        ballot(3, 0),
        "adopted the choosing ballot as the promise"
    );

    let r = n.ready();
    assert_eq!(
        r.must_sync(),
        MustSync::Sync,
        "an install is fsync'd before send"
    );
    assert!(
        r.writes().iter().any(|w| matches!(
            w,
            WriteOp::InstallSnapshot { chosen_index, .. } if *chosen_index == Slot(5)
        )),
        "the install surfaced a durable WriteOp::InstallSnapshot"
    );
    assert!(
        r.committed().is_empty(),
        "snapshot-xor-entries: no committed user entries for the folded prefix"
    );
    r.advance();

    // A stale snapshot at or below our prefix is ignored (no going backward), even
    // though it carries a higher ballot.
    n.step(Message::InstallSnapshot {
        from: NodeId(0),
        ballot: ballot(9, 0),
        chosen_index: Slot(4),
        snapshot: Value(vec![]),
    });
    assert_eq!(
        n.hard_state().chosen_index,
        Some(Slot(5)),
        "a stale snapshot does not move the chosen prefix backward"
    );
    assert_eq!(
        n.hard_state().max_promised_ballot,
        ballot(3, 0),
        "an ignored stale snapshot changes nothing"
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

// ---- linearizable reads (read-index) ---------------------------------------

/// Step `msg` into whichever node it is addressed to, without draining it (so
/// its pending buckets stay observable), returning nothing. Panics if `to` is
/// not a cluster member.
fn step_at(nodes: &mut [RawNode], to: NodeId, msg: Message) {
    let idx = nodes
        .iter()
        .position(|n| n.config().id == to)
        .expect("message addressed to a cluster member");
    nodes[idx].step(msg);
}

#[test]
fn read_on_a_follower_redirects_to_the_leader() {
    let mut nodes = cluster_with_three_chosen();
    assert_eq!(
        nodes[1].read_index(1),
        ReadIndexResult::NotLeader(Some(NodeId(0))),
        "a follower redirects a read to the leader it follows"
    );
}

#[test]
fn read_confirms_on_a_heartbeat_ack_quorum_and_clears_on_advance() {
    let mut nodes = cluster_with_three_chosen();
    assert_eq!(nodes[0].read_index(7), ReadIndexResult::Pending);
    assert!(
        nodes[0].pending_read_states.is_empty(),
        "only the self ack so far: no quorum, nothing to serve"
    );

    // Deliver the round's beat to node 1 only; route its ack back by hand so
    // node 0 is never drained in between (its buckets stay observable).
    let beats = drain(&mut nodes[0]);
    for (to, m) in beats {
        if to == NodeId(1) {
            nodes[1].step(m);
        }
    }
    for (to, m) in drain(&mut nodes[1]) {
        if to == NodeId(0) && matches!(m, Message::HeartbeatAck { .. }) {
            step_at(&mut nodes, to, m);
        }
    }

    let ready = nodes[0].ready();
    assert_eq!(
        ready.read_states(),
        &[ReadState {
            ctx: 7,
            index: Some(Slot(2)),
        }],
        "self + one follower ack is a quorum of three; the captured index is the applied prefix"
    );
    ready.advance();
    assert!(
        nodes[0].pending_read_states.is_empty(),
        "read states are consume-once: cleared by advance"
    );
}

#[test]
fn fresh_leader_read_waits_for_the_read_floor() {
    // The trap: writes acked by the old leader can sit above a fresh leader's
    // chosen prefix until its election-recovered slots re-decide. A read
    // confirmed by quorum acks alone would serve a lagging watermark.
    let mut nodes = cluster_with_three_chosen();

    // Slot 3 is accepted at node 1 only; its Accepted/Commit never land.
    let _ = nodes[0].propose(ClientId(1), ClientSeq(4), val(40));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, m| {
        to == NodeId(1) && matches!(m, Message::Accept { .. })
    });

    // Elect node 1 delivering Phase-1 traffic only: it recovers its accepted
    // slot 3 (read_floor) but the re-proposal Accepts stay undelivered, so its
    // chosen prefix still lags the floor.
    nodes[1].set_election_timeout(1);
    nodes[1].tick();
    let q = drain(&mut nodes[1]);
    deliver_filtered(&mut nodes, q, |_, m| {
        matches!(
            m,
            Message::Prepare { .. }
                | Message::Promise { .. }
                | Message::CatchUpRequest { .. }
                | Message::CatchUpResponse { .. }
        )
    });
    assert!(nodes[1].is_leader(), "node 1 wins the election");
    assert_eq!(nodes[1].read_floor, Some(Slot(3)));
    assert_eq!(
        nodes[1].hard_state().chosen_index,
        Some(Slot(2)),
        "the recovered slot is re-proposed but not yet re-decided"
    );

    // A full ack quorum arrives — and must NOT confirm the read: the fence
    // holds until the chosen prefix covers the floor.
    assert_eq!(nodes[1].read_index(9), ReadIndexResult::Pending);
    let beats = drain(&mut nodes[1]);
    let mut acks = Vec::new();
    for (to, m) in beats {
        let idx = nodes
            .iter()
            .position(|n| n.config().id == to)
            .expect("beat addressed to a member");
        nodes[idx].step(m);
        acks.extend(drain(&mut nodes[idx]));
    }
    for (to, m) in acks {
        if to == NodeId(1) && matches!(m, Message::HeartbeatAck { .. }) {
            step_at(&mut nodes, to, m);
        }
    }
    assert!(
        nodes[1].pending_read_states.is_empty(),
        "quorum acked, but the recovered slot has not re-decided: the read must wait"
    );

    // The next beat re-sends the pending Accept; one Accepted re-decides slot 3
    // and the waiting read resolves at (or past) the floor.
    nodes[1].tick();
    let q = drain(&mut nodes[1]);
    let mut replies = Vec::new();
    for (to, m) in q {
        let idx = nodes
            .iter()
            .position(|n| n.config().id == to)
            .expect("message addressed to a member");
        nodes[idx].step(m);
        replies.extend(drain(&mut nodes[idx]));
    }
    for (to, m) in replies {
        if to == NodeId(1) {
            step_at(&mut nodes, to, m);
        }
    }
    assert_eq!(nodes[1].hard_state().chosen_index, Some(Slot(3)));
    assert_eq!(
        nodes[1].pending_read_states,
        vec![ReadState {
            ctx: 9,
            index: Some(Slot(3)),
        }],
        "the read confirms only once the applied prefix covers the read floor"
    );
}

#[test]
fn single_node_read_confirms_in_the_same_batch() {
    let mut n = node(0, &[0]);
    n.set_election_timeout(1);
    n.tick();
    assert!(n.is_leader(), "a single node is its own quorum");
    let _ = n.propose(ClientId(1), ClientSeq(1), val(1));
    assert_eq!(n.hard_state().chosen_index, Some(Slot(0)));

    assert_eq!(n.read_index(3), ReadIndexResult::Pending);
    assert_eq!(
        n.pending_read_states,
        vec![ReadState {
            ctx: 3,
            index: Some(Slot(0)),
        }],
        "the self ack is the whole quorum: confirmed without any tick or message"
    );
}

#[test]
fn stale_ballot_heartbeat_ack_never_credits_a_round() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].read_index(1);
    let _ = drain(&mut nodes[0]);
    let stale = ballot(nodes[0].ballot().round - 1, 0);
    let seq = nodes[0].heartbeat_seq;
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(1),
        ballot: stale,
        seq,
    });
    assert!(
        nodes[0].pending_read_states.is_empty(),
        "an ack echoing another ballot proves nothing about this leadership"
    );
}

#[test]
fn stale_seq_ack_is_ignored_and_a_later_beat_confirms() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].read_index(1);
    let _ = drain(&mut nodes[0]);
    let b = nodes[0].ballot();
    let required = nodes[0].read_rounds[0].required_seq;

    // An ack to a beat broadcast *before* the round began proves nothing: the
    // follower may have answered before a higher ballot promised elsewhere.
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(1),
        ballot: b,
        seq: required - 1,
    });
    assert!(nodes[0].pending_read_states.is_empty());

    // An ack to a *later* beat counts for every older pending round.
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(2),
        ballot: b,
        seq: required + 1,
    });
    assert_eq!(
        nodes[0].pending_read_states,
        vec![ReadState {
            ctx: 1,
            index: Some(Slot(2)),
        }]
    );
}

#[test]
fn duplicate_acks_from_one_peer_are_not_a_quorum() {
    let mut nodes = [
        node(0, &[0, 1, 2, 3, 4]),
        node(1, &[0, 1, 2, 3, 4]),
        node(2, &[0, 1, 2, 3, 4]),
        node(3, &[0, 1, 2, 3, 4]),
        node(4, &[0, 1, 2, 3, 4]),
    ];
    make_leader(&mut nodes, 0);
    let _ = nodes[0].read_index(5);
    let _ = drain(&mut nodes[0]);
    let b = nodes[0].ballot();
    let seq = nodes[0].read_rounds[0].required_seq;

    // The same peer acking three times is still one voice (quorum of 5 is 3).
    for _ in 0..3 {
        nodes[0].step(Message::HeartbeatAck {
            from: NodeId(1),
            ballot: b,
            seq,
        });
    }
    assert!(nodes[0].pending_read_states.is_empty());

    // A second distinct peer completes the quorum (self + 1 + 2).
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(2),
        ballot: b,
        seq,
    });
    assert_eq!(
        nodes[0].pending_read_states,
        vec![ReadState {
            ctx: 5,
            index: None,
        }],
        "nothing chosen yet: the confirmed watermark is the empty prefix"
    );
}

#[test]
fn step_down_drops_pending_read_rounds() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].read_index(1);
    let _ = drain(&mut nodes[0]);
    let b = nodes[0].ballot();
    let seq = nodes[0].read_rounds[0].required_seq;

    // A higher-ballot Prepare deposes the leader mid-round.
    nodes[0].step(Message::Prepare {
        from: NodeId(2),
        ballot: ballot(b.round + 1, 2),
        from_slot: Slot(3),
    });
    assert!(!nodes[0].is_leader());
    assert!(
        nodes[0].read_rounds.is_empty(),
        "unconfirmed rounds die with the leadership"
    );

    // Late acks for the dead round are ignored (role + ballot guard).
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(1),
        ballot: b,
        seq,
    });
    assert!(nodes[0].pending_read_states.is_empty());
}

#[test]
fn read_round_expires_after_its_ttl() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].read_index(1);
    assert_eq!(nodes[0].read_rounds.len(), 1);

    // No ack ever arrives; the leader garbage-collects the round silently (the
    // driver owns the client-facing retry).
    for _ in 0..=READ_ROUND_TTL_TICKS {
        nodes[0].tick();
    }
    assert!(nodes[0].read_rounds.is_empty());
    assert!(nodes[0].pending_read_states.is_empty());
}

#[test]
fn read_after_compaction_confirms_normally() {
    let mut nodes = cluster_with_three_chosen();
    nodes[0].compact(Slot(1));
    assert_eq!(nodes[0].first_slot(), Slot(2));
    let _ = drain(&mut nodes[0]);

    assert_eq!(nodes[0].read_index(4), ReadIndexResult::Pending);
    let beats = drain(&mut nodes[0]);
    for (to, m) in beats {
        if to == NodeId(1) {
            nodes[1].step(m);
        }
    }
    for (to, m) in drain(&mut nodes[1]) {
        if to == NodeId(0) && matches!(m, Message::HeartbeatAck { .. }) {
            step_at(&mut nodes, to, m);
        }
    }
    assert_eq!(
        nodes[0].pending_read_states,
        vec![ReadState {
            ctx: 4,
            index: Some(Slot(2)),
        }],
        "the compaction floor is irrelevant to the confirm condition"
    );
}
