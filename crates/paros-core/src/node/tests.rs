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

/// A CheckQuorum window far past any unit test's tick horizon. Unit tests
/// step messages by hand rather than pumping ack traffic every tick, so a
/// realistic short timeout would demote their leaders mid-test; tests that
/// *target* CheckQuorum set a short window explicitly instead.
const NO_CHECK_QUORUM: u64 = 1_000_000;

/// Drive `nodes[idx]` to leadership in a healthy cluster, then beat once so the
/// followers learn who the leader is (a follower only adopts a leader on
/// `Accept`/`Heartbeat`, never on Phase 1). Leaves the leader with an
/// effectively infinite CheckQuorum window (see [`NO_CHECK_QUORUM`]).
fn make_leader(nodes: &mut [RawNode], idx: usize) {
    nodes[idx].set_election_timeout(1);
    nodes[idx].tick(); // fires CheckLeader -> Candidate, broadcasts Prepare
    let q = drain(&mut nodes[idx]);
    deliver_all(nodes, q);
    assert!(
        nodes[idx].is_leader(),
        "node {idx} should have won the election"
    );
    nodes[idx].set_election_timeout(NO_CHECK_QUORUM);
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
fn follower_missing_only_slot_zero_catches_up_on_an_idle_beat() {
    // Pins the #56 bug: the heartbeat used to encode "nothing chosen" as
    // `Slot(0)`, so a leader that had genuinely chosen slot 0 was
    // indistinguishable on the wire from a leader that had chosen nothing. A
    // follower with an empty prefix read the beat as "no lag" and never pulled,
    // and every other healing path is closed here — the leader re-sends
    // `Accept`s only for slots still in `proposer` (slot 0 left it at
    // `try_decide`), the reverse push needs the sender to be strictly behind,
    // and the healthy beats keep resetting `election_elapsed` so the follower
    // never campaigns. With `commit: Option<Slot>` the beat says `Some(Slot(0))`,
    // which is strictly above the follower's `None`, and the ordinary catch-up
    // pull heals it.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slot 0 — the *only* slot this cluster ever decides. Node 2 receives the
    // `Accept` but not the `Commit`, so it accepted the value without ever
    // learning it was chosen: an empty contiguous prefix (`chosen_index: None`).
    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, m| {
        !(to == NodeId(2) && matches!(m, Message::Commit { .. }))
    });
    assert_eq!(
        nodes[0].hard_state().chosen_index,
        Some(Slot(0)),
        "the leader chose slot 0"
    );
    assert_eq!(
        nodes[2].hard_state().chosen_index,
        None,
        "follower 2 never learned slot 0 was chosen"
    );

    // The cluster now idles: nothing but heartbeats. Several beats, so a single
    // dropped round trip could not explain a failure to converge.
    for _ in 0..3 {
        nodes[0].tick();
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }

    assert_eq!(
        chosen_at(&nodes[2], 0),
        Some(val(10)),
        "an idle beat advertising `Some(Slot(0))` reveals the lag"
    );
    assert_eq!(
        nodes[2].hard_state().chosen_index,
        Some(Slot(0)),
        "follower 2 converged to the cluster's one-slot chosen prefix"
    );
}

#[test]
fn a_leader_that_lost_its_chosen_index_is_pushed_the_first_slot_back() {
    // The other half of #56, on the reverse-push guard: a leader whose relaxed
    // (non-fsync'd) chosen index did not survive a crash beats `None`, and a
    // follower that *does* know slot 0 is decided must push it back. Under the
    // old encoding both sides said `Slot(0)` — the leader meaning "nothing", the
    // follower meaning "slot 0" — so `commit < ci` was false, nobody pushed, and
    // the leader kept advertising an empty prefix that no follower could correct.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // The leader forgets: it beats an empty watermark at its current ballot.
    let b = nodes[0].ballot();
    nodes[1].step(Message::Heartbeat {
        from: NodeId(0),
        ballot: b,
        commit: None,
        seq: 1,
    });
    let replayed = drain(&mut nodes[1]).into_iter().any(|(to, m)| {
        to == NodeId(0)
            && matches!(m, Message::CatchUpResponse { ref entries, .. } if entries.contains_key(&Slot(0)))
    });
    assert!(
        replayed,
        "the follower pushes the decided slot back to the leader that forgot it"
    );
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
    assert!(
        nodes[0].has_pending_accepts(),
        "the driver can see that a re-send would do useful work"
    );

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
fn a_slot_chosen_above_a_hole_is_deduped_in_flight_not_acked_as_applied() {
    // Pins #55 on the `try_decide` call site: the leader streams slots
    // concurrently, so a later slot's accept quorum routinely completes while an
    // earlier slot is still open. Until the earlier slot fills, the later one is
    // *chosen* but not *applied*, and `propose` must not answer the client's
    // retry with `Chosen` — that is an immediate `committed: true` for a write
    // outside the applied prefix, which a read at the same node would not see.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slot 0 is proposed but its round never leaves the leader (the batch is
    // dropped on the floor), so nothing is chosen there.
    assert_eq!(
        nodes[0].propose(ClientId(7), ClientSeq(1), val(10)),
        ProposeResult::Accepted(Slot(0))
    );
    drop(drain(&mut nodes[0]));

    // Slot 1 goes out and is chosen — above the hole at slot 0.
    assert_eq!(
        nodes[0].propose(ClientId(7), ClientSeq(2), val(20)),
        ProposeResult::Accepted(Slot(1))
    );
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(chosen_at(&nodes[0], 1), Some(val(20)), "slot 1 is chosen");
    assert_eq!(
        nodes[0].hard_state().chosen_index,
        None,
        "but nothing is applied: the prefix is still stuck below the slot-0 hole"
    );

    // The retry lands in exactly that window. It must dedup onto the slot the
    // command is already chosen at, so the driver parks the reply there and acks
    // it when the slot applies — not report it applied, and not (the worse
    // failure) miss both tables and allocate a second slot for it.
    assert_eq!(
        nodes[0].propose(ClientId(7), ClientSeq(2), val(20)),
        ProposeResult::Duplicate(Slot(1)),
        "chosen-but-unapplied dedups to its slot, it is not acked as applied"
    );
    assert_eq!(
        nodes[0].next_slot,
        Slot(2),
        "and no second slot was allocated for a command already chosen"
    );

    // Fill the hole: the leader's beat re-sends the still-pending slot-0
    // `Accept`, slot 0 is chosen, and the walk applies slots 0 and 1 together.
    let b = nodes[0].ballot();
    nodes[0].step(Message::Heartbeat {
        from: NodeId(0),
        ballot: b,
        commit: None,
        seq: 0,
    });
    nodes[0].resend_pending();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(
        nodes[0].hard_state().chosen_index,
        Some(Slot(1)),
        "the prefix caught up over both slots"
    );
    assert_eq!(
        nodes[0].propose(ClientId(7), ClientSeq(2), val(20)),
        ProposeResult::Chosen(Slot(1)),
        "only now is the retry answered as applied, naming the slot it applied at"
    );
}

#[test]
fn a_commit_above_the_hole_holds_the_entry_in_flight_until_it_applies() {
    // The same #55 property on the `on_commit` call site, where the slot is
    // whatever the network delivers: a follower learning a decided slot above
    // its own hole records it as *in flight at that slot*, never as applied. The
    // hand-off to `applied_seq` happens in the contiguous walk, so the two
    // tables are checked on both sides of it.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Slot 1: node 2 misses both the `Accept` and the `Commit` — a hole.
    nodes[0].propose(ClientId(1), ClientSeq(2), val(20));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(2));

    // Slot 2 reaches node 2 normally, so it is chosen there, above the hole.
    nodes[0].propose(ClientId(7), ClientSeq(5), val(30));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    assert_eq!(
        nodes[2].hard_state().chosen_index,
        Some(Slot(0)),
        "node 2's applied prefix stalls at the hole"
    );
    assert_eq!(
        nodes[2].applied_seq.get(&ClientId(7)),
        None,
        "a slot chosen above the hole is not applied, so it is not in `applied_seq`"
    );
    assert_eq!(
        nodes[2].inflight.get(&(ClientId(7), ClientSeq(5))),
        Some(&Slot(2)),
        "it is in flight at its chosen slot instead — node 2 never proposed it, so \
         only `mark_chosen` could have put it there"
    );

    // Commit-replay catch-up fills the hole; the walk applies slots 1 and 2 and
    // hands the entry from one table to the other.
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[2].hard_state().chosen_index, Some(Slot(2)));
    assert_eq!(
        nodes[2]
            .applied_seq
            .get(&ClientId(7))
            .and_then(|m| m.get(&ClientSeq(5))),
        Some(&Slot(2)),
        "applied now, naming the slot it applied at"
    );
    assert_eq!(
        nodes[2].inflight.get(&(ClientId(7), ClientSeq(5))),
        None,
        "and no longer in flight"
    );
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
        n.applied_seq
            .get(&ClientId(1))
            .and_then(|m| m.get(&ClientSeq(2))),
        Some(&Slot(1))
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
            .any(|w| matches!(w, WriteOp::Truncate { first, .. } if *first == Slot(3))),
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
        sessions: vec![],
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
        sessions: vec![],
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
    nodes[1].resend_pending();
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
fn a_leader_without_an_ack_quorum_steps_down_after_its_window() {
    // Pins the #95 CheckQuorum contract after its sim red→green (23 zombie
    // seeds, e.g. 901969623722906706): an isolated leader must not stay
    // Leader past an ack-quorum-less election-timeout window.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    nodes[0].set_election_timeout(3);
    // Tick without ever delivering the beats (a fully partitioned leader):
    // the first window may have been pre-credited by `make_leader`'s
    // delivered beat, so demotion lands within two windows at the latest.
    let mut demoted_at = None;
    for i in 0..10 {
        nodes[0].tick();
        let _ = drain(&mut nodes[0]);
        if !nodes[0].is_leader() {
            demoted_at = Some(i);
            break;
        }
    }
    let at = demoted_at.expect("an isolated leader demotes itself (CheckQuorum)");
    assert!(at <= 6, "within two ack windows, demoted at tick {at}");
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert_eq!(nodes[0].quorum_lost_step_downs(), 1);
    assert!(
        nodes[0].needs_election_timeout(),
        "the demoted leader re-enters the ordinary election path"
    );
}

#[test]
fn a_leader_hearing_acks_keeps_leadership_across_windows() {
    // The healthy half of CheckQuorum: every delivered beat is acked by both
    // followers, so the window refills each time it closes and leadership is
    // never disturbed.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    nodes[0].set_election_timeout(2);
    for _ in 0..8 {
        nodes[0].tick();
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q); // beats out, acks back, window credited
        assert!(nodes[0].is_leader(), "a reachable leader never demotes");
    }
    assert_eq!(nodes[0].quorum_lost_step_downs(), 0);
}

#[test]
fn voluntary_step_down_resigns_and_drops_the_volatile_leadership_state() {
    // The public [`RawNode::step_down`] half of the same property: a leader that
    // resigns of its own accord (no deposing Prepare, no crash) keeps every
    // durable commitment — the promised ballot and the accepted log — and drops
    // exactly the volatile leadership state: the in-flight Phase-2 rounds and any
    // unconfirmed read-index round. This is the primitive the simulation drives to
    // make a never-re-sent hole permanent (#54) and to create election churn.
    let mut nodes = cluster_with_three_chosen();
    let promise_before = nodes[0].hard_state().max_promised_ballot;
    let ballot_before = nodes[0].ballot();

    nodes[0].propose(ClientId(9), ClientSeq(1), val(90));
    let _ = drain(&mut nodes[0]);
    let _ = nodes[0].read_index(1);
    let _ = drain(&mut nodes[0]);
    // Snapshot the log *after* the proposal self-accepted into slot 3: that
    // accept is durable and must survive the resignation too.
    let log_before = nodes[0].accepted.clone();
    assert!(
        !nodes[0].proposer.is_empty(),
        "a Phase-2 round is in flight"
    );
    assert_eq!(nodes[0].read_rounds.len(), 1, "a read round is pending");

    nodes[0].step_down();

    assert!(!nodes[0].is_leader(), "the leader resigned");
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert_eq!(
        nodes[0].leader(),
        None,
        "it resigned rather than handing over, so it knows no leader"
    );
    assert!(
        nodes[0].proposer.is_empty(),
        "the volatile in-flight rounds go with the leadership — this is what makes \
         a hole below a decided slot permanent"
    );
    assert!(
        nodes[0].read_rounds.is_empty(),
        "unconfirmed read rounds die with the leadership"
    );
    assert!(
        nodes[0].needs_election_timeout(),
        "it asks the driver for a fresh randomized election timeout"
    );
    assert_eq!(
        nodes[0].hard_state().max_promised_ballot,
        promise_before,
        "the durable promise never regresses on a step-down"
    );
    assert_eq!(
        nodes[0].ballot(),
        ballot_before,
        "and its operating ballot is unchanged"
    );
    assert_eq!(
        nodes[0].accepted, log_before,
        "the accepted log is durable state, untouched"
    );

    // Idempotent, and a no-op on a node that never led.
    nodes[0].step_down();
    nodes[1].step_down();
    assert!(!nodes[1].is_leader());
    assert!(
        drain(&mut nodes[1]).is_empty(),
        "a non-leader resigns silently"
    );
}

#[test]
fn a_round_the_driver_never_re_sends_stalls_until_one_call_heals_it() {
    // The [`RawNode::resend_pending`] contract, both halves. A driver that beats
    // but never calls it leaves a round whose first `Accept` was lost pending
    // forever — the cluster is *safe* (the slot is simply undecided) but the
    // contiguous chosen prefix is frozen below it. A single call decides it, which
    // is what proves the stall was the skipped re-send and nothing else.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slot 0: healthy.
    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Slot 1: every `Accept` is lost, so the round is pending on the leader alone.
    nodes[0].propose(ClientId(1), ClientSeq(2), val(20));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, msg| {
        !matches!(msg, Message::Accept { .. })
    });

    // Beat for a while, and never re-send. The beats keep the followers from
    // campaigning, so nothing else can heal the slot either.
    for _ in 0..5 {
        nodes[0].tick();
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }
    assert_eq!(
        chosen_at(&nodes[1], 1),
        None,
        "no re-send, so the follower never even saw slot 1"
    );
    assert_eq!(
        nodes[0].hard_state().chosen_index,
        Some(Slot(0)),
        "and the leader's own prefix is frozen one below the pending slot"
    );

    // One call, and the round completes on the very next exchange.
    nodes[0].resend_pending();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(
        chosen_at(&nodes[1], 1),
        Some(val(20)),
        "a single re-send is enough: skipping was the only thing holding it"
    );
    assert_eq!(
        nodes[0].hard_state().chosen_index,
        Some(Slot(1)),
        "the prefix walks past it"
    );
    assert!(
        !nodes[0].has_pending_accepts(),
        "the hook is no longer consulted once the round decides"
    );
}

#[test]
fn a_step_down_makes_a_never_re_sent_hole_permanent_until_the_noop_fill() {
    // The #54 arc built entirely from the two public decisions the simulation
    // perturbs — no crash, no packet-loss emulation at the protocol layer. The
    // driver skips the re-send of slot 1 (safe, pure optimization loss) while slot
    // 2 decides normally, and then the leader resigns (also safe). The volatile
    // `proposer` map goes with the leadership, so nothing will ever re-propose
    // slot 1 — and a promise quorum that never saw it steps clean over it. The
    // `Control::Noop` gap fill is the only thing that closes it.
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Slot 0: healthy.
    nodes[0].propose(ClientId(1), ClientSeq(1), val(10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Slot 1: its `Accept`s are lost and the driver never re-sends it.
    nodes[0].propose(ClientId(1), ClientSeq(2), val(20));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, msg| {
        !matches!(msg, Message::Accept { .. })
    });

    // Slot 2: reaches node 1, so the {0,1} quorum decides it — an *undecided* slot
    // now sits below a *decided* one.
    nodes[0].propose(ClientId(1), ClientSeq(3), val(30));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, msg| {
        !(matches!(msg, Message::Accept { .. }) && to == NodeId(2))
    });
    assert_eq!(chosen_at(&nodes[1], 2), Some(val(30)));

    // The leader resigns. Slot 1 lived only in its volatile `proposer` map.
    nodes[0].step_down();
    assert!(
        nodes[0].proposer.is_empty(),
        "the only record that slot 1 was still being proposed is gone"
    );

    // Nodes 1 and 2 elect; neither ever saw slot 1, so `Election::recovered` holds
    // slot 2 alone and `next_slot` jumps over the hole.
    nodes[1].set_election_timeout(1);
    nodes[1].tick();
    let q = drain(&mut nodes[1]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));
    assert!(nodes[1].is_leader());
    assert_eq!(
        nodes[1].election_gap_fills(),
        1,
        "the new leader found the hole the quorum never reported and filled it"
    );

    let q = drain(&mut nodes[1]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0));
    assert_eq!(
        nodes[1].hard_state().chosen_index,
        Some(Slot(2)),
        "the frozen prefix walks past the filled hole"
    );
    assert_eq!(nodes[1].chosen_gap(), None, "nothing is stranded any more");
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

// ---- #67/#88: the stale-ballot election --------------------------------------
//
// Two **guard** tests (plus the acceptor-side containment below). #67 asked
// whether a Candidate that learns a higher ballot mid-campaign can go on to win
// at its now-stale ballot; the answer used to be yes, through the two
// promise-raising paths that deliberately leave the campaign open: `mark_chosen`
// (a learned `Commit`/`CatchUpResponse`) and `on_install_snapshot` (a snapshot
// whose serving peer minted its promise with no quorum behind it — the #88
// route, uncontained by quorum intersection at n >= 5). `try_become_leader` now
// refuses any win whose election ballot sits below the node's own
// `max_promised_ballot`, restoring "a leader's ballot >= its own promise".
// These tests pin the refusal, the self-heal (the next campaign ratchets past
// the learned promise), and the healthy re-propose the stale win used to break.

/// #67/#88, route 1 (`mark_chosen`, via `on_commit` / `on_catchup_response`):
/// a candidate that learns a higher-ballot commit mid-campaign refuses the
/// stale win, and the next campaign recovers the reported slot properly.
///
/// Pre-guard, the win went through and the damage was downstream:
/// `start_accept_round`'s "never lower our promise" guard skipped the
/// self-accept for every recovered slot, so those never entered `accepted`;
/// `next_slot` — derived from `accepted` — landed *below* an in-flight slot,
/// and the next `propose` re-broadcast a second command for one
/// `(ballot, slot)` (`SafetyOracle`'s "one ballot proposes at most one command
/// for a slot" reads that off the wire).
#[test]
fn a_candidate_that_learns_a_higher_ballot_commit_refuses_the_stale_win() {
    let mut x = node(0, &[0, 1, 2]);
    let b = ballot(1, 0);
    let b_prime = ballot(1, 2);
    assert!(b_prime > b, "same round, higher node id");

    // X campaigns at `b` and promises it to itself.
    x.step(Message::CheckLeader { from: NodeId(0) });
    assert_eq!(x.role, NodeRole::Candidate);
    assert_eq!(x.ballot, b);
    let _ = drain(&mut x);

    // A `Commit` at `b'` arrives: some other proposer won `b'` with a quorum X
    // was not part of, and decided slot 0. X learns it as a *learner*; the
    // campaign stays open, but the promise is now above the campaign's ballot.
    x.step(Message::Commit {
        from: NodeId(2),
        ballot: b_prime,
        slot: Slot(0),
        command: Command::Control(Control::Noop),
    });
    let _ = drain(&mut x);
    assert_eq!(x.role, NodeRole::Candidate, "the campaign is untouched");
    assert_eq!(
        x.hard_state.max_promised_ballot, b_prime,
        "`mark_chosen` raised the promise to the choosing ballot"
    );

    // The delayed `Promise(b)` lands: a perfectly well-formed promise for a
    // ballot X has since promised away. The quorum is there — the win is not.
    let mut reported = BTreeMap::new();
    reported.insert(Slot(3), (ballot(0, 1), ucmd(9, 9, 0xA0)));
    x.step(Message::Promise {
        from: NodeId(1),
        ballot: b,
        from_slot: Slot(0),
        accepted: reported,
    });
    assert_eq!(x.role, NodeRole::Candidate, "the stale win is refused");
    assert!(x.election.is_some(), "the refused campaign stays open");
    assert!(
        x.proposer.is_empty(),
        "nothing is proposed at the stale ballot"
    );
    let _ = drain(&mut x);

    // Self-heal: the next campaign ratchets past the promise that refused the
    // win, and the same reported slot is recovered *properly* this time.
    x.step(Message::CheckLeader { from: NodeId(0) });
    let b2 = x.ballot;
    assert!(
        b2 > b_prime,
        "the fresh campaign sits above the learned promise"
    );
    let _ = drain(&mut x);
    let mut reported = BTreeMap::new();
    reported.insert(Slot(3), (ballot(0, 1), ucmd(9, 9, 0xA0)));
    x.step(Message::Promise {
        from: NodeId(1),
        ballot: b2,
        // The fresh campaign solicits from `first_unchosen()` — slot 0 was
        // chosen by the learned commit, so the probe starts at slot 1.
        from_slot: Slot(1),
        accepted: reported,
    });
    assert_eq!(x.role, NodeRole::Leader, "the healthy win goes through");
    assert!(
        x.ballot >= x.hard_state.max_promised_ballot,
        "a leader's ballot covers its own promise"
    );
    assert!(
        x.proposer.contains_key(&Slot(3)),
        "slot 3 is re-proposed (P2c)"
    );
    assert!(
        x.accepted.contains_key(&Slot(3)),
        "and its self-accept was recorded"
    );
    assert_eq!(
        x.next_slot,
        Slot(4),
        "the allocator sits above the recovered slot"
    );
}

/// #67, route 1: the containment — why the anomaly above is not a safety
/// violation.
///
/// The stale leader's promise was raised by a value **chosen** at `b'`. Chosen
/// means a Phase-1 quorum promised `b'` and a Phase-2 quorum accepted at `b'`,
/// both strictly *before* the commit reached the candidate, which is before it
/// won. Promises never decrease, so every member of that Phase-1 quorum is
/// pinned at `b'` from then on, and both gates that could give the stale leader a
/// quorum reject it: [`RawNode::on_accept`] and [`RawNode::on_heartbeat`] each
/// require `ballot >= max_promised_ballot`. The stale leader cannot even count
/// itself towards an accept quorum — `start_accept_round` skips its own
/// self-accept for the same reason.
///
/// So the acceptors available to ballot `b` are at most the complement of a
/// Phase-1 quorum, `n - q1`, and `n - q1 < q2` holds for every quorum system
/// Paxos admits (Phase-1 and Phase-2 quorums must intersect). Nothing is decided
/// at `b`, no read confirms at `b`, and the first `Nack` sends the stale leader
/// back to Follower. The duplicate `Accept`s die on the wire.
#[test]
fn an_acceptor_pinned_at_the_higher_ballot_gives_the_stale_leader_nothing() {
    let b = ballot(1, 0);
    let b_prime = ballot(1, 2);

    // A peer that promised `b'` — i.e. a member of the Phase-1 quorum that any
    // decision at `b'` had to have.
    let mut p = node(1, &[0, 1, 2]);
    p.step(Message::Prepare {
        from: NodeId(2),
        ballot: b_prime,
        from_slot: Slot(0),
    });
    let _ = drain(&mut p);
    assert_eq!(p.hard_state.max_promised_ballot, b_prime);

    // It rejects the stale leader's `Accept` …
    p.step(Message::Accept {
        from: NodeId(0),
        ballot: b,
        slot: Slot(3),
        command: ucmd(1, 2, 3),
    });
    let out = drain(&mut p);
    assert!(
        out.iter().all(|(_, m)| matches!(m, Message::Nack { .. })),
        "a Nack, never an Accepted: {out:?}"
    );

    // … and stays silent on its beat, so no read round of the stale leader's can
    // reach a confirmation quorum either.
    p.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: b,
        commit: None,
        seq: 1,
    });
    let out = drain(&mut p);
    assert!(
        !out.iter()
            .any(|(_, m)| matches!(m, Message::HeartbeatAck { .. })),
        "a below-promise beat is not acked: {out:?}"
    );
}

/// #88, route 2 (`on_install_snapshot`): a snapshot-minted promise blocks the
/// stale election win outright.
///
/// The route quorum intersection does **not** contain: a snapshot offer
/// carries the **serving node's own promised ballot** (`serve_catchup` pushes
/// `(to, ci, self.hard_state.max_promised_ballot)`), and a promise needs no
/// quorum — one campaigning node mints it. Pre-guard, X won at `b` with
/// `max_promised = m > b`: the self-accept skip left the recovered suffix out
/// of `accepted`, `next_slot` and the fresh-leader read fence dropped below
/// the suffix, and a committed read confirmed under a slot the promise quorum
/// had reported accepted. The guard refuses the win; the next campaign covers
/// `m`, and the fence lands on the recovered suffix where it belongs.
///
/// The configuration is an ordinary dueling-candidate race: node 0 and node 2
/// both time out at round 5 (node 2's ballot wins the node-id tiebreak),
/// node 1 is still at round 4, and node 0 — far enough behind that its
/// `from_slot` is under node 2's compaction floor — gets a snapshot instead of
/// a `Promise`.
#[test]
fn a_snapshot_raised_promise_blocks_the_stale_election_win() {
    // Node 0 boots at round 4 with an empty chosen prefix: far behind, which
    // is what makes a snapshot the only way it can be healed.
    let mut storage = TestStorage::new(0, &[0, 1, 2]);
    storage.hard_state.max_promised_ballot = ballot(4, 1);
    let mut x = RawNode::new(&storage);

    let b = ballot(5, 0);
    let m = ballot(5, 2);
    assert!(m > b, "node 2 wins the same-round tiebreak");

    x.step(Message::CheckLeader { from: NodeId(0) });
    assert_eq!(x.ballot, b, "one round past what it had promised");
    let _ = drain(&mut x);

    // Node 2 answers the campaign's below-floor `CatchUpRequest` with a
    // snapshot offer. The ballot on it is node 2's *promise* — minted by
    // campaigning at round 5 itself, with no quorum behind it.
    x.step(Message::InstallSnapshot {
        from: NodeId(2),
        ballot: m,
        chosen_index: Slot(5),
        snapshot: val(0xEE),
        sessions: vec![],
    });
    let _ = drain(&mut x);
    assert_eq!(x.hard_state.max_promised_ballot, m, "promise raised to m");
    assert_eq!(x.hard_state.chosen_index, Some(Slot(5)));
    assert_eq!(x.first_slot, Slot(6), "the log below the snapshot is gone");
    assert_eq!(x.role, NodeRole::Candidate, "the campaign is untouched");

    // Node 1, still at round 4, promises `b` and reports slot 8 accepted: a
    // quorum for `b` — but `b < m`, and the win is refused.
    let mut reported = BTreeMap::new();
    reported.insert(Slot(8), (ballot(4, 1), ucmd(9, 9, 0xA0)));
    x.step(Message::Promise {
        from: NodeId(1),
        ballot: b,
        from_slot: Slot(0),
        accepted: reported,
    });
    assert_eq!(
        x.role,
        NodeRole::Candidate,
        "no stale win below the minted promise"
    );
    assert!(x.proposer.is_empty(), "no accept round opens at `b`");
    assert_eq!(x.read_floor, None, "no fresh-leader read fence is set");
    let _ = drain(&mut x);

    // The next campaign covers `m`. Winning there records a real self-accept
    // for the recovered slot, so the allocator and the read fence both sit on
    // the recovered suffix.
    x.step(Message::CheckLeader { from: NodeId(0) });
    let b2 = x.ballot;
    assert!(b2 > m, "the fresh campaign sits above the minted promise");
    let _ = drain(&mut x);
    let mut reported = BTreeMap::new();
    reported.insert(Slot(8), (ballot(4, 1), ucmd(9, 9, 0xA0)));
    x.step(Message::Promise {
        from: NodeId(1),
        ballot: b2,
        from_slot: x.first_slot,
        accepted: reported,
    });
    assert_eq!(x.role, NodeRole::Leader, "the healthy win goes through");
    assert!(
        x.ballot >= x.hard_state.max_promised_ballot,
        "a leader's ballot covers its own promise"
    );
    assert!(
        x.accepted.contains_key(&Slot(8)),
        "the recovered slot reached the log"
    );
    assert_eq!(x.next_slot, Slot(9), "the allocator covers the suffix");
    assert_eq!(
        x.read_floor,
        Some(Slot(8)),
        "the read fence sits on the recovered suffix"
    );
}

/// The dedup ledger acks `Chosen` only for a seq that **actually executed** —
/// never inferred from "a later seq applied". A client's seqs do not execute in
/// order: an early seq can die without entering the log (a `NotLeader` window,
/// a round lost and paved over by the gap fill) while a later seq applies, and
/// the old `seq <= applied` shortcut then acked the dead command as committed,
/// at another command's slot (network-axis seeds 2791878389799639169 /
/// 8872503201755490526). The honest miss falls through and executes the retry
/// for real.
#[test]
fn a_retry_of_a_never_executed_seq_is_not_acked_as_chosen() {
    let mut nodes = vec![
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);

    // Seq 4 executes (seqs 0..=3 never reached this cluster: they died in a
    // NotLeader window elsewhere).
    assert_eq!(
        nodes[0].propose(ClientId(7), ClientSeq(4), val(0x44)),
        ProposeResult::Accepted(Slot(0))
    );
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[0].hard_state().chosen_index, Some(Slot(0)));

    // An exact-seq retry is honestly deduplicated to its real slot…
    assert_eq!(
        nodes[0].propose(ClientId(7), ClientSeq(4), val(0x44)),
        ProposeResult::Chosen(Slot(0))
    );
    // …but a never-executed earlier seq is NOT lied about: it executes now.
    assert_eq!(
        nodes[0].propose(ClientId(7), ClientSeq(2), val(0x22)),
        ProposeResult::Accepted(Slot(1)),
        "a dead seq below the latest applied one is re-proposed, never falsely acked"
    );
}

/// A snapshot install must re-drive the contiguous walk: a `Commit` learned
/// out of order can already sit in `chosen` just above the boundary, and
/// without the walk the node freezes at `boundary` forever — catch-up loops
/// (`mark_chosen`'s already-chosen early return never re-drives either), and
/// if the node later leads, its read fence sits above its prefix so no read
/// ever confirms. Red before the fix: `chosen_index` stuck at 9 with
/// `chosen[10]` in hand.
#[test]
fn a_snapshot_install_advances_over_an_out_of_order_chosen_slot() {
    let mut x = node(0, &[0, 1, 2]);

    // Slot 10 arrives out of order (reordered/duplicated `Commit`): chosen,
    // but far above the (empty) contiguous prefix.
    x.step(Message::Commit {
        from: NodeId(2),
        ballot: ballot(3, 2),
        slot: Slot(10),
        command: ucmd(1, 1, 0xAA),
    });
    let _ = drain(&mut x);
    assert_eq!(x.hard_state.chosen_index, None, "nothing contiguous yet");
    assert!(x.chosen.contains_key(&Slot(10)));

    // A peer answers the below-floor catch-up with a snapshot at boundary 9.
    x.step(Message::InstallSnapshot {
        from: NodeId(2),
        ballot: ballot(3, 2),
        chosen_index: Slot(9),
        snapshot: val(0xEE),
        sessions: vec![],
    });
    let _ = drain(&mut x);

    assert_eq!(
        x.hard_state.chosen_index,
        Some(Slot(10)),
        "the walk resumed over the out-of-order chosen slot at the boundary"
    );
    assert_eq!(
        x.chosen_gap(),
        None,
        "no stranded chosen slot survives the install"
    );
}

/// The catch-up half of the same freeze: a `Commit` for a slot already in
/// `chosen` must still re-drive the contiguous walk. Pre-fix, `mark_chosen`'s
/// early return skipped it, so a node stuck one below an already-known slot
/// looped `CatchUpRequest` forever while holding the very commit it needed.
#[test]
fn a_replayed_commit_for_a_known_slot_still_advances_the_prefix() {
    let mut x = node(0, &[0, 1, 2]);
    x.step(Message::Commit {
        from: NodeId(2),
        ballot: ballot(3, 2),
        slot: Slot(1),
        command: ucmd(1, 1, 0xBB),
    });
    let _ = drain(&mut x);
    assert_eq!(
        x.hard_state.chosen_index, None,
        "slot 1 is above the hole at 0"
    );

    // Slot 0 arrives; the prefix advances through both.
    x.step(Message::Commit {
        from: NodeId(2),
        ballot: ballot(3, 2),
        slot: Slot(0),
        command: ucmd(1, 0, 0xCC),
    });
    let _ = drain(&mut x);
    assert_eq!(x.hard_state.chosen_index, Some(Slot(1)));

    // A duplicated / catch-up-replayed commit for a known slot is a no-op for
    // state but must never wedge: the early return still re-drives the walk.
    x.step(Message::Commit {
        from: NodeId(2),
        ballot: ballot(3, 2),
        slot: Slot(1),
        command: ucmd(1, 1, 0xBB),
    });
    let _ = drain(&mut x);
    assert_eq!(x.hard_state.chosen_index, Some(Slot(1)));
}
