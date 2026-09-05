//! Stage 8 (CTRL) protocol-aware recovery: the Promise tri-state, the
//! blocked-slot wait/resolve machinery, in-place repair, the application
//! repair pump, and the §5.1.1 mixed-epoch named regression.

use super::*;

/// Keep every message except the ones that would let slot 3's round complete
/// beyond the intended holders: the proposer's `Accepted` acks, every
/// `Commit`, and (when `isolate_node2`) the `Accept` to node 2.
fn keep_slot3_undecided(to: NodeId, m: &Message, isolate_node2: bool) -> bool {
    match m {
        Message::Commit { .. } => false,
        Message::Accepted { .. } => to != NodeId(0),
        Message::Accept { .. } => !(isolate_node2 && to == NodeId(2)),
        _ => true,
    }
}

/// Build a 3-node cluster with slots 0..=2 chosen everywhere, then reboot
/// `victim` with the accepted records in `rotted` classified faulty (value
/// lost, identity kept).
fn rebooted_with_rot(rotted: &[u64]) -> ColocatedNode {
    let nodes = cluster_with_three_chosen();
    let mut storage = TestStorage::from_node(&nodes[1]);
    for &slot in rotted {
        storage.rot(Slot(slot));
    }
    ColocatedNode::new(&storage)
}

#[test]
fn boot_loads_faulty_entries_disjoint_from_the_log() {
    let n = rebooted_with_rot(&[1]);
    assert_eq!(n.acceptor().faulty().len(), 1);
    let (&slot, _ballot) = n
        .acceptor()
        .faulty()
        .iter()
        .next()
        .expect("one faulty entry");
    assert_eq!(slot, Slot(1));
    assert!(!n.acceptor().records().contains_key(&Slot(1)));
    // The rotted slot was chosen: the durable prefix is untouched.
    assert_eq!(n.hard_state().chosen_index, Some(Slot(2)));
}

/// R2: a rotted copy is reported as `faulty(ballot)` in the Promise — never as
/// "nothing accepted here", and never as a readable record.
#[test]
fn promise_reports_faulty_tristate_never_none() {
    let mut n = rebooted_with_rot(&[1]);
    // An unchosen accepted slot above the prefix, rotted, must be reported.
    // Rot slot 1 (chosen) is below a fresh candidate's from_slot; craft a
    // prepare from slot 0 to cover the whole log instead.
    n.step(Message::Prepare {
        reply_to: NodeId(2),
        leader: NodeId(2),
        ballot: ballot(9, 2),
        from_slot: Slot(0),
        config: None,
    });
    let msgs = drain(&mut n);
    let promise = msgs
        .iter()
        .find_map(|(to, m)| match m {
            Message::Promise {
                accepted, faulty, ..
            } if *to == NodeId(2) => Some((accepted.clone(), faulty.clone())),
            _ => None,
        })
        .expect("a promise was sent");
    let (accepted, faulty) = promise;
    assert!(!accepted.contains_key(&Slot(1)), "faulty is never `have`");
    assert!(
        faulty.contains_key(&Slot(1)),
        "faulty is reported, not `none`"
    );
    assert!(accepted.contains_key(&Slot(0)));
    assert!(accepted.contains_key(&Slot(2)));
}

/// A leader's `Accept` over a faulty slot repairs it in place (fill, never
/// delete), and the repair is counted for the audit.
#[test]
fn accept_repairs_a_faulty_slot_in_place() {
    // Rot an *unchosen* accepted slot: reboot a follower that accepted slot 3
    // but never learned it chosen.
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(4), val(40));
    let q = drain(&mut nodes[0]);
    // Deliver the Accept to node 1 but drop its ack and the commit, so node 1
    // holds an accepted-but-unchosen record at slot 3.
    deliver_filtered(&mut nodes, q, |to, m| keep_slot3_undecided(to, m, false));
    assert!(nodes[1].acceptor().records().contains_key(&Slot(3)));
    let mut storage = TestStorage::from_node(&nodes[1]);
    storage.rot(Slot(3));
    let mut n = ColocatedNode::new(&storage);
    assert_eq!(n.acceptor().faulty().len(), 1);

    // The leader re-sends its pending Accept: the fresh record replaces the
    // lost one.
    let accept = Message::Accept {
        reply_to: NodeId(0),
        leader: NodeId(0),
        ballot: nodes[0].ballot(),
        slot: Slot(3),
        command: ucmd(1, 4, 40),
    };
    n.step(accept);
    assert!(
        n.acceptor().faulty().is_empty(),
        "the faulty entry was repaired"
    );
    assert!(n.acceptor().records().contains_key(&Slot(3)));
    let (repaired, _c1, _c2, _sd) = n.repair_counters();
    assert_eq!(repaired, 1);
}

/// R3 Case 2 at the election itself: with the faulty reporter excluded, a full
/// Q1 of `none` still fills the slot with a `Noop` (uncommitted faulty items
/// are decided as early as possible).
#[test]
fn full_none_quorum_noop_fills_over_an_excluded_faulty_reporter() {
    // Node 1 holds a faulty record at slot 3 (accepted from a deposed leader,
    // never chosen anywhere); nodes 0 and 2 have nothing at slot 3.
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(4), val(40));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, m| keep_slot3_undecided(to, m, true));
    let mut storage = TestStorage::from_node(&nodes[1]);
    storage.rot(Slot(3));
    nodes[1] = ColocatedNode::new(&storage);
    // Node 0 must forget its own copy of slot 3 (it proposed it), so the
    // quorum genuinely reports none: rebuild node 0 from a log without it.
    let mut s0 = TestStorage::from_node(&nodes[0]);
    s0.accepted.remove(&Slot(3));
    nodes[0] = ColocatedNode::new(&s0);

    // Node 2 campaigns: Q1 = {0, 2} reports none at slot 3; node 1 reports
    // faulty. {0, 2} is a full quorum of none — the slot is decided Noop even
    // before node 1's report could block anything.
    make_leader(&mut nodes, 2);
    // Let the accept/commit rounds finish.
    let q = drain(&mut nodes[2]);
    deliver_all(&mut nodes, q);
    assert_eq!(
        nodes[2].replica.chosen_at(Slot(3)),
        Some(&Command::Control(Control::Noop)),
        "the faulty-but-uncommitted slot was decided as a no-op"
    );
    // The repaired follower's copy was overwritten by the decided Noop.
    assert!(nodes[1].acceptor().faulty().is_empty());
}

/// R3 Case 3 → Case 1: a slot whose only clean copy sits on a node outside the
/// winning quorum stays **blocked** (no no-op fill!) until the straggler's late
/// Promise resolves it with the real value.
#[test]
fn blocked_slot_waits_then_resolves_case1_from_a_straggler() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(4), val(40));
    let q = drain(&mut nodes[0]);
    // Only node 1 accepts slot 3; the round never completes.
    deliver_filtered(&mut nodes, q, |to, m| keep_slot3_undecided(to, m, true));
    // Node 0 reboots with its own copy of slot 3 rotted (it was the proposer's
    // self-accept), so the identity survives but the value is lost.
    let mut s0 = TestStorage::from_node(&nodes[0]);
    s0.rot(Slot(3));
    nodes[0] = ColocatedNode::new(&s0);

    // Node 0 campaigns; node 2 answers (none at slot 3), node 1's promise is
    // withheld. Tally at slot 3: self faulty(b), node 2 none — one qualifying
    // answer short of a quorum that excludes the faulty reporter.
    nodes[0].set_election_timeout(1);
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _m| to != NodeId(1));
    assert!(nodes[0].is_leader(), "the campaign still wins");
    assert_eq!(nodes[0].blocked_repairs(), 1, "slot 3 is blocked: wait");
    assert!(
        !nodes[0].replica.is_chosen(Slot(3)),
        "a blocked slot is never no-op filled"
    );

    // The straggler answers the re-queried Prepare with have(b, 40): Case 1.
    nodes[0].set_election_timeout(NO_CHECK_QUORUM);
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[0].blocked_repairs(), 0, "the probe closed");
    assert_eq!(
        chosen_at(&nodes[0], 3),
        Some(val(40)),
        "the straggler's clean copy was recovered, not overwritten"
    );
    let (_r, case1, case2, _sd) = nodes[0].repair_counters();
    assert_eq!(case1, 1);
    assert_eq!(case2, 0);
}

/// CTRL §4.2: a leader that cannot finish recovery (the only holder of a
/// blocked slot's value stays unreachable) resigns after the recovery timeout
/// so another node can try.
#[test]
fn recovery_timeout_steps_the_leader_down() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(4), val(40));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, m| keep_slot3_undecided(to, m, true));
    let mut s0 = TestStorage::from_node(&nodes[0]);
    s0.rot(Slot(3));
    nodes[0] = ColocatedNode::new(&s0);
    nodes[0].set_election_timeout(1);
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _m| to != NodeId(1));
    assert!(nodes[0].is_leader());
    assert_eq!(nodes[0].blocked_repairs(), 1);

    // Nothing from node 1 ever arrives; node 2 keeps acking beats (so
    // `CheckQuorum` stays satisfied — the leader can reach a quorum, it just
    // cannot finish recovery), and the probe times out. A multi-tick election
    // timeout gives the ack round trips room inside each CheckQuorum window.
    nodes[0].set_election_timeout(5);
    for _ in 0..=5 * super::super::REPAIR_TIMEOUT_ELECTIONS {
        nodes[0].tick();
        let q = drain(&mut nodes[0]);
        deliver_filtered(&mut nodes, q, |to, _m| to != NodeId(1));
        if nodes[0].repair_counters().3 > 0 {
            break;
        }
    }
    let (_r, _c1, _c2, step_downs) = nodes[0].repair_counters();
    assert_eq!(step_downs, 1, "the recovery timeout fired");
    assert!(!nodes[0].is_leader(), "the blocked leader resigned");
}

/// A faulty record inside the *chosen* prefix stalls the application exactly
/// like an unchosen gap (routed through `chosen_gap`) and heals through
/// ordinary commit-replay catch-up, re-emitting the decided commands in order.
#[test]
fn faulty_chosen_slot_heals_via_catchup_and_the_repair_pump() {
    let nodes = cluster_with_three_chosen();
    let mut storage = TestStorage::from_node(&nodes[1]);
    storage.rot(Slot(1));
    let mut n = ColocatedNode::new(&storage);
    // The driver's boot replay stops at the unreadable slot 1 and opens the
    // repair there.
    n.open_app_repair(Slot(1));
    assert_eq!(n.replica().app_repair(), Some(Slot(1)));
    assert_eq!(
        n.replica().chosen_gap(),
        Some((Slot(1), Slot(2))),
        "the stall is visible through the chosen-gap seam"
    );

    // The tick pull asks peers for the decided range from the cursor.
    n.set_election_timeout(NO_CHECK_QUORUM);
    n.tick();
    let msgs = drain(&mut n);
    assert!(
        msgs.iter().any(|(_, m)| matches!(
            m,
            Message::CatchUpRequest {
                from_slot: Slot(1),
                ..
            }
        )),
        "the repair pulls from its cursor"
    );

    // A peer replays the decided range; the pump re-emits slots 1..=2 in
    // order and closes the repair.
    let mut entries = BTreeMap::new();
    for s in 1..=2u64 {
        let (b, c) = nodes[0]
            .acceptor()
            .records()
            .get(&Slot(s))
            .cloned()
            .expect("chosen");
        entries.insert(Slot(s), (b, c));
    }
    n.step(Message::CatchUpResponse {
        from: NodeId(0),
        entries,
    });
    assert_eq!(n.replica().app_repair(), None, "the repair closed");
    assert!(
        n.acceptor().faulty().is_empty(),
        "the faulty record was healed"
    );
    let ready = n.ready();
    let committed: Vec<Slot> = ready.committed().iter().map(|(s, _)| *s).collect();
    ready.advance();
    assert_eq!(
        committed,
        vec![Slot(1), Slot(2)],
        "the pump re-emitted the healed range in slot order"
    );
}

/// Per-slot attribution on the serving side: a peer never serves catch-up past
/// its own faulty slot — silence, not a silently gapped replay.
#[test]
fn serve_catchup_stops_at_the_servers_own_faulty_hole() {
    let nodes = cluster_with_three_chosen();
    let mut storage = TestStorage::from_node(&nodes[1]);
    storage.rot(Slot(1));
    let mut n = ColocatedNode::new(&storage);
    n.step(Message::CatchUpRequest {
        from: NodeId(2),
        from_slot: Slot(0),
    });
    let msgs = drain(&mut n);
    let served: Vec<Slot> = msgs
        .iter()
        .find_map(|(_, m)| match m {
            Message::CatchUpResponse { entries, .. } => {
                Some(entries.keys().copied().collect::<Vec<_>>())
            }
            _ => None,
        })
        .expect("slot 0 is served");
    assert_eq!(served, vec![Slot(0)], "the replay stops at the faulty hole");
}

/// The below-floor application repair: a node whose snapshot state was lost
/// under a truncated log installs a peer snapshot at an *equal* chosen index —
/// the one legal equality install — and closes the repair.
#[test]
fn install_snapshot_at_equal_index_closes_a_below_floor_repair() {
    let nodes = cluster_with_three_chosen();
    // Simulate: node 1 truncated through slot 1 and then lost its snapshot.
    let mut storage = TestStorage::from_node(&nodes[1]);
    storage.first_slot = Slot(2);
    storage.accepted.retain(|s, _| *s >= Slot(2));
    let mut n = ColocatedNode::new(&storage);
    n.open_app_repair(Slot(0));
    assert_eq!(n.replica().app_repair(), Some(Slot(0)));

    // A snapshot at chosen_index == our own chosen index is normally a no-op;
    // with the repair open it is the heal.
    n.step(Message::InstallSnapshot {
        from: NodeId(0),
        ballot: nodes[0].ballot(),
        chosen_index: Slot(2),
        snapshot: val(9),
        sessions: Vec::new(),
    });
    assert_eq!(
        n.replica().app_repair(),
        None,
        "the install closed the repair"
    );
    assert_eq!(n.acceptor().first_slot(), Slot(3));
}

/// The CTRL §5.1.1 mixed-epoch state, restated for Multi-Paxos: three
/// different recovery decisions in one election. `S1:[a¹,·] S2:[b²,c³]
/// S3:[b²,·]` with `a` rotted on S1 and `b`,`c` rotted on S2. The new leader
/// (S2, faulty itself) must recover `b` from S3's clean copy (Case 1),
/// no-op-decide its own uncommitted `c` (Case 2), and overwrite S1's rotted
/// `a` with the decided `b` (leader-instructed discard through consensus).
#[test]
fn ctrl_5_1_1_mixed_epoch_three_decisions_in_one_election() {
    let a = ucmd(1, 1, 0xa);
    let b = ucmd(2, 1, 0xb);
    let c = ucmd(3, 1, 0xc);

    // A real disk's promise always dominates its accepted ballots (the accept
    // path raises the promise in the same flushed batch), and the boot
    // read-back re-asserts it — so each crafted storage carries a promise at
    // its highest accepted ballot.
    let mut s1 = TestStorage::new(0, &[0, 1, 2]);
    s1.hard_state.max_promised_ballot = ballot(1, 0);
    s1.accepted.insert(Slot(0), (ballot(1, 0), a));
    s1.rot(Slot(0));

    let mut s2 = TestStorage::new(1, &[0, 1, 2]);
    s2.hard_state.max_promised_ballot = ballot(3, 1);
    s2.accepted.insert(Slot(0), (ballot(2, 1), b.clone()));
    s2.accepted.insert(Slot(1), (ballot(3, 1), c));
    s2.rot(Slot(0));
    s2.rot(Slot(1));

    let mut s3 = TestStorage::new(2, &[0, 1, 2]);
    s3.hard_state.max_promised_ballot = ballot(2, 1);
    s3.accepted.insert(Slot(0), (ballot(2, 1), b.clone()));

    let mut nodes = [
        ColocatedNode::new(&s1),
        ColocatedNode::new(&s2),
        ColocatedNode::new(&s3),
    ];
    // S2 — a leader with faulty entries may be elected.
    make_leader(&mut nodes, 1);
    let q = drain(&mut nodes[1]);
    deliver_all(&mut nodes, q);
    for _ in 0..3 {
        for n in &mut nodes {
            n.tick();
        }
        let q: Vec<_> = nodes.iter_mut().flat_map(drain).collect();
        deliver_all(&mut nodes, q);
    }

    // Decision 1: slot 0 recovered as `b` (S3's clean copy wins; the P2c
    // threshold rules out anything hidden above ballot 2).
    // Decision 3: S1's rotted `a` was overwritten by the decided `b`.
    for n in &nodes {
        assert_eq!(chosen_at(n, 0), Some(val(0xb)), "slot 0 decided b");
        assert!(n.acceptor().faulty().is_empty(), "every rotted copy healed");
    }
    // Decision 2: S2's own uncommitted `c` was discarded by deciding Noop —
    // S1 and S3 form a full Q1 of none at slot 1.
    for n in &nodes {
        assert_eq!(
            n.replica.chosen_at(Slot(1)),
            Some(&Command::Control(Control::Noop)),
            "slot 1 decided as a no-op"
        );
    }
    assert_eq!(nodes[1].blocked_repairs(), 0);
}
