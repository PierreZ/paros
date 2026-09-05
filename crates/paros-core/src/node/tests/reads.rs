#[allow(clippy::wildcard_imports)]
use super::*;

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
    assert_eq!(nodes[1].proposer().read_floor(), Some(Slot(3)));
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
        chosen: None,
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
    let required = nodes[0].proposer().read_rounds()[0].required_seq();

    // An ack to a beat broadcast *before* the round began proves nothing: the
    // follower may have answered before a higher ballot promised elsewhere.
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(1),
        ballot: b,
        seq: required - 1,
        chosen: None,
    });
    assert!(nodes[0].pending_read_states.is_empty());

    // An ack to a *later* beat counts for every older pending round.
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(2),
        ballot: b,
        seq: required + 1,
        chosen: None,
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
    let seq = nodes[0].proposer().read_rounds()[0].required_seq();

    // The same peer acking three times is still one voice (quorum of 5 is 3).
    for _ in 0..3 {
        nodes[0].step(Message::HeartbeatAck {
            from: NodeId(1),
            ballot: b,
            seq,
            chosen: None,
        });
    }
    assert!(nodes[0].pending_read_states.is_empty());

    // A second distinct peer completes the quorum (self + 1 + 2).
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(2),
        ballot: b,
        seq,
        chosen: None,
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
    let seq = nodes[0].proposer().read_rounds()[0].required_seq();

    // A higher-ballot Prepare deposes the leader mid-round.
    nodes[0].step(Message::Prepare {
        reply_to: NodeId(2),
        ballot: ballot(b.round + 1, 2),
        from_slot: Slot(3),
        config: None,
    });
    assert!(!nodes[0].is_leader());
    assert!(
        nodes[0].proposer().read_rounds().is_empty(),
        "unconfirmed rounds die with the leadership"
    );

    // Late acks for the dead round are ignored (role + ballot guard).
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(1),
        ballot: b,
        seq,
        chosen: None,
    });
    assert!(nodes[0].pending_read_states.is_empty());
}

#[test]
fn read_round_expires_after_its_ttl() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].read_index(1);
    assert_eq!(nodes[0].proposer().read_rounds().len(), 1);

    // No ack ever arrives; the leader garbage-collects the round silently (the
    // driver owns the client-facing retry).
    for _ in 0..=READ_ROUND_TTL_TICKS {
        nodes[0].tick();
    }
    assert!(nodes[0].proposer().read_rounds().is_empty());
    assert!(nodes[0].pending_read_states.is_empty());
}

#[test]
fn read_after_compaction_confirms_normally() {
    let mut nodes = cluster_with_three_chosen();
    nodes[0].compact(Slot(1));
    assert_eq!(nodes[0].acceptor().first_slot(), Slot(2));
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
