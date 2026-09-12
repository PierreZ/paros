//! Quorum reads (#143): the leaderless read path, pinned at the node.

#[allow(clippy::wildcard_imports)]
use super::*;

/// A six-node `2 × 3` grid over `0..6`: rows `{0, 1, 2}` and `{3, 4, 5}`,
/// columns `{0, 3}`, `{1, 4}` and `{2, 5}`.
fn grid_node(id: u64) -> ColocatedNode {
    let mut storage = TestStorage::new(id, &[0, 1, 2, 3, 4, 5]);
    storage.config.quorum_system = crate::membership::QuorumSystem::Grid { rows: 2, cols: 3 };
    ColocatedNode::new(&storage)
}

/// A grid cluster led by node 0 with slots `0..=2` chosen everywhere.
fn grid_with_three_chosen() -> Vec<ColocatedNode> {
    let mut nodes: Vec<ColocatedNode> = (0..6).map(grid_node).collect();
    make_leader(&mut nodes, 0);
    for (seq, b) in [(1u64, 10u8), (2, 20), (3, 30)] {
        let _ = nodes[0].propose(ClientId(1), ClientSeq(seq), val(b));
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }
    for n in &nodes {
        assert_eq!(n.hard_state().chosen_index, Some(Slot(2)));
    }
    nodes
}

fn pre_read_targets(queue: &[(NodeId, Message)]) -> Vec<NodeId> {
    queue
        .iter()
        .filter_map(|(to, m)| matches!(m, Message::PreRead { .. }).then_some(*to))
        .collect()
}

/// The read half of the grid: a follower in row 1 asks *its row* — three
/// acceptors that are not a column — and the read completes on that row
/// alone, with no leader, no beat and no ack in the exchange. The
/// read-index path is untouched: the leader opened no read round, and not
/// one `Heartbeat` / `HeartbeatAck` travelled.
#[test]
fn a_quorum_read_completes_on_a_row_that_is_not_a_column() {
    let mut nodes = grid_with_three_chosen();
    // ctx 1 -> row 1 = {3, 4, 5}; node 4 is the reader and its own first
    // answer, so it asks only the other two.
    nodes[4].quorum_read(1);
    assert!(
        nodes[4].pending_read_states.is_empty(),
        "one watermark is no row"
    );
    let asks = drain(&mut nodes[4]);
    assert_eq!(pre_read_targets(&asks), vec![NodeId(3), NodeId(5)]);
    assert!(
        asks.iter()
            .all(|(_, m)| matches!(m, Message::PreRead { .. })),
        "a quorum read sends PreRead and nothing else"
    );
    let mut answers = Vec::new();
    for (to, m) in asks {
        step_at(&mut nodes, to, m);
        let idx = usize::try_from(to.0).expect("small id");
        answers.extend(drain(&mut nodes[idx]));
    }
    assert!(
        answers.iter().all(|(to, m)| *to == NodeId(4)
            && matches!(
                m,
                Message::PreReadAck {
                    watermark: Some(Slot(2)),
                    config_since: None,
                    ..
                }
            )),
        "every acceptor answers the reader with its watermark; a plain deployment names no configuration ballot"
    );
    for (to, m) in answers {
        step_at(&mut nodes, to, m);
    }
    let ready = nodes[4].ready();
    assert_eq!(
        ready.read_states(),
        &[ReadState {
            ctx: 1,
            index: Some(Slot(2)),
        }],
        "the row {{3, 4, 5}} answered whole: served at the maximum watermark"
    );
    ready.advance();
    assert!(nodes[4].quorum_reads().is_empty());
    assert!(
        nodes[0].proposer().read_rounds().is_empty(),
        "the leader took no part: no read-index round was opened"
    );
    assert_eq!(
        nodes[0].heartbeat_seq, 1,
        "no beat was broadcast for the read (the one beat is make_leader's)"
    );
}

/// The safety trade, pinned: an accept that reached a row member but never
/// decided raises that member's watermark, so the read settles past the
/// chosen prefix and *waits* — served only once the slot is learned here.
#[test]
fn a_quorum_read_waits_for_the_replica_to_cover_the_watermark() {
    let mut nodes = grid_with_three_chosen();
    // Slot 3 -> column 0 = {0, 3}. The leader self-votes; node 3 accepts but
    // its `Accepted` is lost, so the column is not whole at the leader.
    assert!(matches!(
        nodes[0].propose(ClientId(1), ClientSeq(4), val(40)),
        ProposeResult::Accepted(Slot(3))
    ));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, m| !matches!(m, Message::Accepted { .. }));
    assert_eq!(chosen_at(&nodes[0], 3), None);
    assert_eq!(nodes[3].acceptor().vote_watermark(), Some(Slot(3)));
    assert_eq!(nodes[4].acceptor().vote_watermark(), Some(Slot(2)));

    // ctx 3 -> row 1 = {3, 4, 5}: node 3's watermark points past the prefix.
    nodes[4].quorum_read(3);
    let asks = drain(&mut nodes[4]);
    deliver_all(&mut nodes, asks);
    assert_eq!(
        nodes[4].quorum_reads().pending()[0].confirmed_index(),
        Some(Some(Slot(3))),
        "the row answered whole and settled on node 3's watermark"
    );
    assert!(
        nodes[4].pending_read_states.is_empty(),
        "slot 3 is not chosen here yet: the read waits"
    );
    // The leader's re-send brings node 3's vote back; the decision reaches
    // node 4 as a `Commit`, its prefix covers slot 3, and the read is served.
    // Node 4's inbound traffic is held and stepped by hand so its buckets
    // stay observable (a drain would consume the served read state).
    nodes[0].resend_pending();
    let q = drain(&mut nodes[0]);
    let held = std::cell::RefCell::new(Vec::new());
    deliver_filtered(&mut nodes, q, |to, m| {
        if to == NodeId(4) {
            held.borrow_mut().push((to, m.clone()));
            return false;
        }
        true
    });
    assert!(nodes[4].pending_read_states.is_empty());
    for (to, m) in held.into_inner() {
        step_at(&mut nodes, to, m);
    }
    assert_eq!(chosen_at(&nodes[4], 3), Some(val(40)));
    let ready = nodes[4].ready();
    assert_eq!(
        ready.read_states(),
        &[ReadState {
            ctx: 3,
            index: Some(Slot(3)),
        }]
    );
    ready.advance();
}

/// The row guard: an answer from a configured acceptor outside the read's
/// row, or from outside the pool, is never folded — the mirror of the
/// column guard on `Accepted`.
#[test]
fn a_pre_read_answer_from_outside_the_row_never_counts() {
    let mut nodes = grid_with_three_chosen();
    nodes[4].quorum_read(1); // row 1 = {3, 4, 5}
    let _ = drain(&mut nodes[4]);
    for from in [NodeId(1), NodeId(9)] {
        nodes[4].step(Message::PreReadAck {
            from,
            ctx: 1,
            watermark: Some(Slot(2)),
            config_since: None,
        });
    }
    assert_eq!(
        nodes[4].quorum_reads().pending()[0].watermarks().len(),
        1,
        "only the reader's own watermark is counted"
    );
    // An answer for a token nobody opened is ignored too.
    nodes[4].step(Message::PreReadAck {
        from: NodeId(3),
        ctx: 77,
        watermark: Some(Slot(2)),
        config_since: None,
    });
    assert_eq!(nodes[4].quorum_reads().pending().len(), 1);
    for from in [NodeId(3), NodeId(5)] {
        nodes[4].step(Message::PreReadAck {
            from,
            ctx: 1,
            watermark: Some(Slot(2)),
            config_since: None,
        });
    }
    assert_eq!(
        nodes[4].pending_read_states,
        vec![ReadState {
            ctx: 1,
            index: Some(Slot(2)),
        }]
    );
}

/// A read is independent of the leadership — a role change abandons
/// nothing — and bounded by its TTL: a row that never answers whole is
/// dropped silently, exactly like a read-index round.
#[test]
fn a_quorum_read_survives_a_role_change_and_expires_by_ttl() {
    let mut nodes = grid_with_three_chosen();
    nodes[0].quorum_read(0); // the leader reads too: row 0 = {0, 1, 2}
    let _ = drain(&mut nodes[0]);
    nodes[0].step_down();
    assert_eq!(
        nodes[0].quorum_reads().pending().len(),
        1,
        "stepping down abandons no quorum read"
    );
    for _ in 0..READ_TTL_TICKS {
        nodes[0].tick();
    }
    assert_eq!(nodes[0].quorum_reads().pending().len(), 1);
    nodes[0].tick();
    assert!(
        nodes[0].quorum_reads().is_empty(),
        "one tick past the window the read is gone"
    );
    assert!(nodes[0].pending_read_states.is_empty());
}

/// Under a majority the "row" is the whole membership and any majority of
/// answers completes the read — served from a follower, at the prefix it
/// holds, on the plain deployment whose other messages are unchanged.
#[test]
fn a_quorum_read_on_a_plain_cluster_completes_on_a_majority() {
    let mut nodes = cluster_with_three_chosen();
    nodes[1].quorum_read(5);
    let asks = drain(&mut nodes[1]);
    assert_eq!(pre_read_targets(&asks), vec![NodeId(0), NodeId(2)]);
    // Only node 2 answers: with the reader's own watermark that is two of
    // three — a majority. Stepped by hand so the reader's buckets stay
    // observable.
    for (to, m) in asks {
        if to == NodeId(2) {
            step_at(&mut nodes, to, m);
        }
    }
    for (to, m) in drain(&mut nodes[2]) {
        step_at(&mut nodes, to, m);
    }
    assert_eq!(
        nodes[1].pending_read_states,
        vec![ReadState {
            ctx: 5,
            index: Some(Slot(2)),
        }]
    );
}

/// A configuration that moves underneath a read abandons it (#143's one
/// configuration at a time): the row asked need not intersect the
/// successor's columns.
#[test]
fn learning_a_newer_configuration_abandons_open_quorum_reads() {
    let mut nodes = cluster_with_three_chosen();
    nodes[1].quorum_read(5);
    let _ = drain(&mut nodes[1]);
    assert_eq!(nodes[1].quorum_reads().pending().len(), 1);
    // An answer naming a newer configuration ballot supersedes the read.
    nodes[1].step(Message::PreReadAck {
        from: NodeId(2),
        ctx: 5,
        watermark: Some(Slot(2)),
        config_since: Some(Ballot {
            round: 9,
            node: NodeId(0),
        }),
    });
    assert!(
        nodes[1].quorum_reads().is_empty(),
        "a row member that knows a successor configuration abandons the read"
    );
    assert!(nodes[1].pending_read_states.is_empty());
}
