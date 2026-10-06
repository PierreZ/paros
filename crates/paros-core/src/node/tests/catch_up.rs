#[allow(clippy::wildcard_imports)]
use super::*;

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
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);

    // Slot 0 — the *only* slot this cluster ever decides. Node 2 receives the
    // `Accept` but not the `Commit`, so it accepted the value without ever
    // learning it was chosen: an empty contiguous prefix (`chosen_index: None`).
    nodes[0].propose(entry(1, 1, 10));
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
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    nodes[0].propose(entry(1, 1, 10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // The leader forgets: it beats an empty watermark at its current ballot.
    let b = nodes[0].ballot();
    nodes[1].step(Message::Heartbeat {
        from: NodeId(0),
        ballot: b,
        commit: None,
        config: None,
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
    // heals it. `paros_sim::audit` catches this in simulation; this is the
    // deterministic unit pin.
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);

    // Slot 0: healthy — every node learns it.
    nodes[0].propose(entry(1, 1, 10));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Slot 1: drop *every* message addressed to node 2. Node 0 still decides with
    // the {0,1} quorum, so slot 1 is chosen — but node 2 misses both the `Accept`
    // and the `Commit`, opening a permanent hole.
    nodes[0].propose(entry(1, 2, 20));
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
    nodes[0].propose(entry(1, 3, 30));
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
fn compact_clamps_to_chosen_index_and_prunes_both_maps() {
    let mut nodes = cluster_with_three_chosen();
    let leader = &mut nodes[0];
    assert_eq!(leader.hard_state().chosen_index, Some(Slot(2)));

    // A partial compaction drops slots 0..=1 and keeps slot 2.
    let floor = leader.compact(Slot(1));
    assert_eq!(floor, Slot(2), "floor is one past the last dropped slot");
    assert_eq!(leader.acceptor().first_slot(), Slot(2));
    assert_eq!(
        leader
            .acceptor()
            .records()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![Slot(2)],
        "only slot 2 is retained in the accepted log"
    );
    assert_eq!(
        leader.replica.chosen().keys().copied().collect::<Vec<_>>(),
        vec![Slot(2)],
        "only slot 2 is retained in the chosen map"
    );

    // Over-requesting past the chosen index clamps to it: everything drops.
    let floor = leader.compact(Slot(100));
    assert_eq!(floor, Slot(3), "clamped to chosen_index + 1");
    assert_eq!(leader.acceptor().first_slot(), Slot(3));
    assert!(leader.acceptor().records().is_empty());
    assert!(leader.replica.chosen().is_empty());
}

#[test]
fn truncate_control_command_raises_the_floor_cluster_wide_on_apply() {
    // Leader-driven, Paxos-decided truncation: the leader decides a
    // `Truncate` control command into the log; every node truncates lazily
    // when it applies that slot (the fused-node analogue of a cluster-wide
    // floor), dropping every slot whose records all lie below the new first
    // position (#204).
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    let _ = nodes[0].propose_control(Control::SetLeader {
        expected: Generation(0),
        owner: ClientId(1),
    });
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    // Slots 1..=3 hold positions 0..=2.
    for seq in 0..3u64 {
        let _ = nodes[0].propose(Entry {
            generation: Generation(1),
            owner: ClientId(1),
            seq: Seq(seq),
            records: vec![val(u8::try_from(seq).expect("small"))],
        });
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }
    for n in &nodes {
        assert_eq!(n.acceptor().first_slot(), Slot(0), "no truncation yet");
        assert_eq!(n.hard_state().chosen_index, Some(Slot(3)));
    }

    // The leader admits the truncate as a control command at the next slot (4).
    let r = nodes[0].propose_control(Control::Truncate {
        generation: Generation(1),
        owner: ClientId(1),
        up_to: Seq(2),
    });
    assert!(
        matches!(r, ProposeResult::Accepted(Slot(4))),
        "control command takes the next free slot"
    );
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Once every node applied slot 4, it lazily compacted below the slot
    // holding position 2 (slot 3): the floor rose to 3 everywhere, and the
    // control slot itself is chosen.
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.acceptor().first_slot(),
            Slot(3),
            "node {i} truncated to the decided floor"
        );
        assert_eq!(
            n.hard_state().chosen_index,
            Some(Slot(4)),
            "node {i} chose the control slot"
        );
        assert!(
            !n.acceptor().records().contains_key(&Slot(2)),
            "node {i} dropped slot 2"
        );
        assert!(
            n.acceptor().records().contains_key(&Slot(3)),
            "node {i} keeps slot 3"
        );
        assert_eq!(n.replica().journal().first_seq, Seq(2));
    }
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
    use crate::write::WriteOp;

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
    assert!(
        r.writes().iter().any(WriteOp::needs_sync),
        "a truncate must fsync before the batch's messages are sent"
    );
    r.advance();
}

#[test]
fn restart_from_truncated_storage_rebuilds_floor_and_next_slot() {
    let mut nodes = cluster_with_three_chosen();
    let leader = &mut nodes[0];
    leader.compact(Slot(2)); // full compaction: floor -> 3, accepted empty
    assert!(leader.acceptor().records().is_empty());

    let storage = TestStorage::from_node(leader);
    let restarted = ColocatedNode::new(&storage);
    assert_eq!(
        restarted.acceptor().first_slot(),
        Slot(3),
        "floor survives a restart"
    );
    assert!(
        restarted.acceptor().records().is_empty(),
        "the truncated log rebuilds empty"
    );
    assert_eq!(
        restarted.hard_state().chosen_index,
        Some(Slot(2)),
        "the chosen index is unchanged by truncation"
    );
    assert_eq!(
        restarted.proposer().next_slot(),
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
fn below_floor_catchup_request_is_answered_with_the_trim_point() {
    // A node truncated its chosen prefix; a peer that missed it asks for a slot
    // below the floor. We cannot replay the trimmed entries, so we tell it
    // where the retained log starts — no bytes, no ballot (#186).
    let mut nodes = cluster_with_three_chosen();
    let n = &mut nodes[0];
    n.compact(Slot(1)); // floor -> 2, chosen_index 2
    let _ = drain(n);

    n.step(Message::CatchUpRequest {
        from: NodeId(1),
        from_slot: Slot(0), // below our floor
    });
    let out = drain(n);
    assert!(
        out.iter().any(|(to, m)| *to == NodeId(1)
            && matches!(m, Message::TrimmedTo { point, .. } if *point == Slot(2))),
        "the requester is told the trim point"
    );
    assert!(
        !out.iter()
            .any(|(_, m)| matches!(m, Message::CatchUpResponse { .. })),
        "a below-floor request is answered by the trim point, not a replay"
    );
}

#[test]
fn a_trim_point_jump_moves_the_prefix_and_never_the_promise() {
    use crate::write::WriteOp;

    // A node that missed a trimmed prefix learns the trim point 6: it jumps
    // its chosen prefix to 5, raises its floor to 6, and keeps its promise
    // (#180's rule).
    let mut n = node(1, &[0, 1, 2]);
    assert_eq!(n.hard_state().chosen_index, None);
    let promised = n.hard_state().max_promised_ballot;

    n.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(6),
        state: crate::JournalState {
            owner: Some(ClientId(7)),
            generation: Generation(1),
            next_seq: Seq(4),
            first_seq: Seq(2),
        },
    });
    assert_eq!(
        n.hard_state().chosen_index,
        Some(Slot(5)),
        "jumped to one below the trim point"
    );
    assert_eq!(n.acceptor().first_slot(), Slot(6), "the floor is the point");
    assert_eq!(
        n.hard_state().max_promised_ballot,
        promised,
        "a trim-point jump never moves the promise"
    );
    assert_eq!(
        n.replica().journal().next_seq,
        Seq(4),
        "the journal state below the point is adopted"
    );

    let r = n.ready();
    assert!(
        r.writes().iter().any(|w| matches!(
            w,
            WriteOp::TrimmedTo { point, .. } if *point == Slot(6)
        )),
        "the jump surfaced a durable WriteOp::TrimmedTo"
    );
    assert!(
        r.committed().is_empty(),
        "a jump walks nothing: the slots below the point are gone"
    );
    r.advance();

    // A stale point at or below our floor is ignored.
    n.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(4),
        state: crate::JournalState::default(),
    });
    assert_eq!(n.hard_state().chosen_index, Some(Slot(5)));
    assert_eq!(n.acceptor().first_slot(), Slot(6));
}

/// A trim-point jump must re-drive the contiguous walk: a `Commit` learned
/// out of order can already sit in `chosen` just above the point, and
/// without the walk the node freezes at `point - 1` forever — catch-up loops
/// (`mark_chosen`'s already-chosen early return never re-drives either), and
/// if the node later leads, its read fence sits above its prefix so no read
/// ever confirms.
#[test]
fn a_trim_point_jump_advances_over_an_out_of_order_chosen_slot() {
    let mut x = node(0, &[0, 1, 2]);

    // Slot 10 arrives out of order (reordered/duplicated `Commit`): chosen,
    // but far above the (empty) contiguous prefix.
    x.step(Message::Commit {
        from: Party::Node(NodeId(2)),
        ballot: ballot(3, 2),
        slot: Slot(10),
        command: ucmd(1, 1, 0xAA),
    });
    let _ = drain(&mut x);
    assert_eq!(x.hard_state().chosen_index, None, "nothing contiguous yet");
    assert!(x.replica.is_chosen(Slot(10)));

    // A peer answers the below-floor catch-up with its trim point 10.
    x.step(Message::TrimmedTo {
        from: NodeId(2),
        point: Slot(10),
        state: crate::JournalState::default(),
    });
    let _ = drain(&mut x);

    assert_eq!(
        x.hard_state().chosen_index,
        Some(Slot(10)),
        "the walk resumed over the out-of-order chosen slot at the point"
    );
    assert_eq!(
        x.replica().chosen_gap(),
        None,
        "no stranded chosen slot survives the jump"
    );
}
