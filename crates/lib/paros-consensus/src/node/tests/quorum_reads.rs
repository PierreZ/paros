//! Quorum reads (#143): the leaderless read path, pinned at the node.

use super::*;
use crate::contract::limits::Limits;

/// A six-node `2 × 3` grid over `0..6`: rows `{0, 1, 2}` and `{3, 4, 5}`,
/// columns `{0, 3}`, `{1, 4}` and `{2, 5}`.
fn grid_node(id: u64) -> ColocatedNode {
    let mut storage = MemStorage::for_node(id, &[0, 1, 2, 3, 4, 5]);
    storage.config.quorum_system = crate::membership::QuorumSystem::grid(2, 3);
    ColocatedNode::new(&storage, Limits::DEFAULT)
}

/// A grid cluster led by node 0 with slots `0..=2` chosen everywhere.
fn grid_with_three_chosen() -> Vec<ColocatedNode> {
    let mut nodes: Vec<ColocatedNode> = (0..6).map(grid_node).collect();
    make_leader(&mut nodes, 0);
    for (seq, b) in [(1u64, 10u8), (2, 20), (3, 30)] {
        let _ = nodes[0].propose(entry(1, seq, b));
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
        !nodes[0]
            .pending_messages
            .iter()
            .any(|(_, m)| matches!(m, Message::Heartbeat { .. })),
        "the leader took no part: no beat was broadcast for the read"
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
        nodes[0].propose(entry(1, 4, 40)),
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
    assert!(
        nodes[4].pending_read_states.is_empty(),
        "node 4 serves no read before its held traffic arrives"
    );
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
    assert!(
        nodes[0].pending_read_states.is_empty(),
        "an expired read leaves no read state"
    );
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
    assert!(
        nodes[1].pending_read_states.is_empty(),
        "an abandoned read leaves no read state"
    );
}

/// The driver's named row (`quorum_read_in`): a read whose token defaults
/// to row 1 asks row 0 when the driver names it, and completes on row 0's
/// answers; a row the grid does not have falls back to `ctx % rows`.
#[test]
fn a_named_row_is_asked_instead_of_the_default() {
    let mut nodes = grid_with_three_chosen();
    nodes[4].quorum_read_in(1, Some(0)); // ctx 1 defaults to row 1; row 0 = {0, 1, 2}
    assert_eq!(nodes[4].quorum_reads().pending()[0].row(), Some(0));
    assert!(
        nodes[4].quorum_reads().pending()[0].watermarks().is_empty(),
        "the reader is not in row 0, so it seeds no watermark of its own"
    );
    let _ = drain(&mut nodes[4]);
    for from in [NodeId(0), NodeId(1), NodeId(2)] {
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
    nodes[4].quorum_read_in(3, Some(7)); // no row 7: back to 3 % 2 = row 1
    assert_eq!(nodes[4].quorum_reads().pending()[0].row(), Some(1));
}

/// A vote watermark past a settled leader's frontier — a stale vote its
/// Phase-1 quorum never saw — is filled with `Noop`s by the leader, so a
/// quorum read that met that acceptor is not left waiting on a slot nobody
/// will ever propose (#204). A follower hearing the same watermark fills
/// nothing: only the leader proposes.
#[test]
fn a_leader_fills_its_frontier_to_a_reported_vote_watermark() {
    let mut nodes = cluster_with_three_chosen();
    for reader in [1_usize, 0] {
        nodes[reader].quorum_read(9);
        let _ = drain(&mut nodes[reader]);
        nodes[reader].step(Message::PreReadAck {
            from: NodeId(2),
            ctx: 9,
            watermark: Some(Slot(5)),
            config_since: None,
        });
    }
    assert_eq!(nodes[1].watermark_fills(), 0, "a follower never proposes");
    assert_eq!(nodes[0].watermark_fills(), 3, "slots 3, 4 and 5 are filled");
    let q = drain(&mut nodes[0]);
    let filled: Vec<Slot> = q
        .iter()
        .filter_map(|(_, m)| match m {
            Message::Accept { slot, command, .. }
                if *command == Command::Control(Control::Noop) =>
            {
                Some(*slot)
            }
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(filled, vec![Slot(3), Slot(4), Slot(5)]);
    deliver_all(&mut nodes, q);
    for n in &nodes {
        assert_eq!(n.hard_state().chosen_index, Some(Slot(5)));
    }
    // A watermark the frontier already covers fills nothing more.
    nodes[0].quorum_read(10);
    let _ = drain(&mut nodes[0]);
    nodes[0].step(Message::PreReadAck {
        from: NodeId(2),
        ctx: 10,
        watermark: Some(Slot(5)),
        config_since: None,
    });
    assert_eq!(nodes[0].watermark_fills(), 3);
}

/// The #260 mechanism, pinned (the sweep proved it; this pins the rule): a
/// read is judged over a **won** leadership's basis, never over a
/// campaign's configuration. Node 3 hears the leader of `C_6 = {0, 1, 3}`
/// (a `1 × 3` grid: one acceptor decides, so Phase 1 needs all three) beat at ballot 6, then promises a
/// campaign at 7 for `C_7 = {0, 2, 3}`, a majority whose quorum `{2, 3}`
/// need not hold what `C_6`'s `{0}` chose. Under the campaign it opens no
/// read; once the leadership of 7 beats with its fence, a read over `C_7`
/// waits for the fence even when the row's watermarks sit below it.
#[test]
fn a_campaign_configuration_is_no_read_basis() {
    let c6 = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(3)],
        crate::membership::QuorumSystem::grid(1, 3),
    );
    let c7 = cfg(&[0, 2, 3]);
    let mut n = deployed_node(3, &[0, 1, 2, 3], &[0, 1, 2, 3], 1);

    // No won leadership heard this incarnation: no basis, no read.
    n.quorum_read(1);
    assert!(n.quorum_reads().is_empty());
    assert_eq!(n.membership_counters().reads_without_basis, 1);

    // The leader of 6 beats: a read opens over `C_6`, asking 0 and 1.
    n.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: ballot(6, 0),
        commit: None,
        config: Some(c6.clone()),
        fence: Some(Slot(2)),
        gc_floor: None,
    });
    let _ = drain(&mut n);
    assert_eq!(
        n.read_basis().map(|b| (b.config, b.since, b.fence)),
        Some((c6, ballot(6, 0), Some(Slot(2))))
    );
    n.quorum_read(2);
    assert_eq!(pre_read_targets(&drain(&mut n)), vec![NodeId(0), NodeId(1)]);

    // A campaign at 7 for `C_7`: promising it moves the belief above the
    // basis, which abandons the open read and opens no new one.
    n.step(Message::Prepare {
        reply_to: NodeId(0),
        ballot: ballot(7, 0),
        from_slot: Slot(0),
        config: Some(c7.clone()),
    });
    let _ = drain(&mut n);
    assert_eq!(
        *n.acceptors(),
        c7,
        "the campaign's configuration is believed"
    );
    assert!(
        n.quorum_reads().is_empty(),
        "the read over C_6 is abandoned"
    );
    assert!(n.read_basis().is_none(), "a campaign is no read basis");
    n.quorum_read(3);
    assert!(n.quorum_reads().is_empty());
    assert_eq!(n.membership_counters().reads_without_basis, 2);

    // The leadership of 7 won and beats with its fence, slot 3: a read over
    // `C_7` completes on `{2, 3}` at watermark 1 but is served only once the
    // fold covers the fence.
    n.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: ballot(7, 0),
        commit: None,
        config: Some(c7),
        fence: Some(Slot(3)),
        gc_floor: None,
    });
    let _ = drain(&mut n);
    n.quorum_read(4);
    assert_eq!(pre_read_targets(&drain(&mut n)), vec![NodeId(0), NodeId(2)]);
    n.step(Message::PreReadAck {
        from: NodeId(2),
        ctx: 4,
        watermark: Some(Slot(1)),
        config_since: Some(ballot(7, 0)),
    });
    let _ = drain(&mut n);
    assert!(
        n.pending_read_states.is_empty(),
        "a row below the fence is no answer while the fold is short of it"
    );
    assert_eq!(
        n.quorum_reads().pending().len(),
        1,
        "the read waits on the fence"
    );
}

/// A rebooted acceptor whose belief is still the bootstrap default answers
/// no `PreRead` on a matchmaker deployment (#260): its configuration ballot
/// would name nothing it promised before the crash.
#[test]
fn an_unheard_acceptor_answers_no_pre_read() {
    let mut n = deployed_node(1, &[0, 1, 2], &[0, 1, 2], 1);
    n.step(Message::PreRead {
        reply_to: NodeId(0),
        ctx: 7,
    });
    assert!(
        drain(&mut n)
            .iter()
            .all(|(_, m)| !matches!(m, Message::PreReadAck { .. })),
        "an unheard node stays silent"
    );
    assert_eq!(n.membership_counters().pre_reads_refused_unheard, 1);
    n.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: ballot(2, 0),
        commit: None,
        config: Some(cfg(&[0, 1, 2])),
        fence: None,
        gc_floor: None,
    });
    let _ = drain(&mut n);
    n.step(Message::PreRead {
        reply_to: NodeId(0),
        ctx: 8,
    });
    assert!(
        drain(&mut n)
            .iter()
            .any(|(_, m)| matches!(m, Message::PreReadAck { ctx: 8, .. })),
        "a node that heard a leader answers"
    );
}

/// Node 0 wins `b_old` under `C_old = {0, 1, 2}`, beats, and has one write
/// chosen everywhere, so nodes 1 and 2 read on `(C_old, b_old)`.
fn settle_under_c_old(nodes: &mut [ColocatedNode], mms: &mut [Matchmaker]) -> Ballot {
    // Node 0 wins `b_old` under `C_old` and beats: every node hears it.
    campaign(&mut nodes[0]);
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(mms, requests) {
        nodes[0].on_match_reply(reply);
    }
    let q = drain(&mut nodes[0]);
    deliver_all(nodes, q);
    assert!(nodes[0].is_leader());
    nodes[0].set_election_timeout(NO_CHECK_QUORUM);
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_all(nodes, q);
    let b_old = nodes[0].ballot();

    // One write under `C_old`, chosen everywhere: the leadership settles.
    let _ = nodes[0].propose(entry(1, 1, 1));
    let q = drain(&mut nodes[0]);
    deliver_all(nodes, q);
    assert_eq!(nodes[0].hard_state().chosen_index, Some(Slot(0)));
    for n in &nodes[1..3] {
        assert_eq!(
            n.read_basis().map(|b| (b.config, b.since)),
            Some((cfg(&[0, 1, 2]), b_old)),
            "node {} reads on (C_old, b_old)",
            n.config().id.0
        );
    }
    b_old
}

/// Finding 1 of the paros-consensus review of 2026-11-16 (a `PreReadAck` fences
/// a read with the belief ballot, not the promise). The reads model fences
/// on the promise (`specs/tla/reads/Reads.tla`, `Fence`; the control
/// `fence_belief_lag` finds this trace): an answer from an acceptor that
/// promised a ballot above the read's basis ends the read. This test drives
/// the full trace on real nodes.
///
/// - `C_old = {0, 1, 2}`, led by node 0 at `b_old`. Every node heard its
///   beat, so each one has the read basis `(C_old, b_old)`.
/// - Nodes 1 and 2 have a lagging pool `{0, 1, 2}`: they did not admit
///   nodes 3 and 4 yet. Nodes 0, 3 and 4 have the pool `{0, 1, 2, 3, 4}`.
/// - Node 0 reconfigures to `C_new = {0, 3, 4}` at `b_new`. Node 1 gets the
///   `Prepare`: node 0 is in its pool, so it promises `b_new`; `C_new` is
///   not drawn from its pool, so `acceptors_since` stays `b_old`. The
///   `Prepare` to node 2 is late. `{0, 1}` is a Phase-1 quorum of `C_old`,
///   so node 0 leads at `b_new` under `C_new`.
/// - Node 0 writes `W`. Nodes 0 and 3 accept it: a Phase-2 quorum of
///   `C_new`, so `W` is chosen. Nothing more reaches nodes 1 and 2.
/// - Node 2 opens a quorum read on `(C_old, b_old)`. Node 1 answers with
///   `config_since = b_old`. Nodes 1 and 2 are a Phase-1 quorum of `C_old`.
///
/// Expected (the model): the answer of node 1 ends the read, because its
/// promise `b_new` is above the basis `b_old`. The read is never served
/// below `W`. Red on 25d81d5, where the answer carried the belief ballot
/// `b_old`; green once it carries the read fence (the promise).
#[test]
fn a_promise_above_the_read_basis_ends_the_read_even_when_the_configuration_was_ignored() {
    let wide = [0, 1, 2, 3, 4];
    let lagging = [0, 1, 2];
    let mut nodes = [
        deployed_node(0, &[0, 1, 2], &wide, 1),
        deployed_node(1, &[0, 1, 2], &lagging, 1),
        deployed_node(2, &[0, 1, 2], &lagging, 1),
        deployed_node(3, &[0, 1, 2], &wide, 1),
        deployed_node(4, &[0, 1, 2], &wide, 1),
    ];
    let mut mms = registries(1);

    let b_old = settle_under_c_old(&mut nodes, &mut mms);

    // Node 0 reconfigures to `C_new = {0, 3, 4}`.
    let c_new = cfg(&[0, 3, 4]);
    let ReconfigureResult::Started(b_new) = nodes[0].reconfigure(&c_new) else {
        panic!("the reconfiguration starts");
    };
    assert!(b_new > b_old);
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(&mut mms, requests) {
        let _ = nodes[0].on_match_reply(reply);
    }
    // Phase 1: nothing reaches node 2 (late). Node 1 promises.
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(2));
    assert!(
        nodes[0].is_leader(),
        "{{0, 1}} is a Phase-1 quorum of C_old"
    );
    assert_eq!(nodes[0].ballot(), b_new);
    assert_eq!(*nodes[0].acceptors(), c_new);

    // The state under test, at node 1: the promise moved to `b_new`, the
    // belief stayed at `b_old` (the configuration names nodes outside its
    // pool, so `learn_config` ignored it).
    assert_eq!(nodes[1].acceptor().promised(), b_new);
    assert_eq!(nodes[1].acceptors_since(), b_old);

    // Node 0 writes `W` under `C_new`. Only nodes 0 and 3 vote. Nothing
    // more reaches nodes 1 and 2.
    let _ = nodes[0].propose(entry(1, 2, 2));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _| to == NodeId(0) || to == NodeId(3));
    let w_slot = nodes[0]
        .hard_state()
        .chosen_index
        .expect("W is chosen at the leader");
    assert!(w_slot > Slot(0));
    assert_eq!(chosen_at(&nodes[0], w_slot.0), Some(val(2)), "W is chosen");

    // Node 2 opens a read on its basis `(C_old, b_old)`. Only node 1
    // answers (the ask to node 0 is late).
    assert_eq!(
        nodes[2].read_basis().map(|b| b.since),
        Some(b_old),
        "node 2 still reads on b_old"
    );
    nodes[2].quorum_read(9);
    let asks = drain(&mut nodes[2]);
    assert!(pre_read_targets(&asks).contains(&NodeId(1)));
    let mut answers = Vec::new();
    for (to, m) in asks {
        if to == NodeId(1) {
            nodes[1].step(m);
            answers.extend(drain(&mut nodes[1]));
        }
    }
    let ack = answers
        .iter()
        .find(|(to, m)| *to == NodeId(2) && matches!(m, Message::PreReadAck { ctx: 9, .. }))
        .cloned()
        .expect("node 1 answers the read");
    // What node 1 sends: its promise, above its belief ballot.
    let Message::PreReadAck { config_since, .. } = &ack.1 else {
        unreachable!()
    };
    assert_eq!(
        *config_since,
        Some(b_new),
        "node 1 answers with its promise as the read fence"
    );
    nodes[2].step(ack.1);
    let ready = nodes[2].ready();
    let served = ready.read_states().to_vec();
    ready.advance();

    // The model: the answer of a node that promised above the basis ends
    // the read. No read is served, and none waits.
    assert!(
        served.is_empty(),
        "stale read: served {served:?} below W at {w_slot:?}, on an answer from a node that promised {b_new:?} above the basis {b_old:?}"
    );
    assert!(
        nodes[2].quorum_reads().is_empty(),
        "the read is superseded, not open"
    );
}

/// A lagging reader that promised above its read basis opens no read, and a
/// read it opened before the promise rose is abandoned, not served (#1228
/// (stale read by a lagging reader); `specs/tla/reads/`, check
/// `reads_lag_reader`).
///
/// Five nodes, the setup of the test above, with the roles of nodes 1 and 2
/// swapped: node 1 is the reader, the lagging node that promised `b_new`
/// while its belief stayed at `b_old`, and node 2 answers with its promise
/// `b_old`. `{1, 2}` is a Phase-1 quorum of `C_old`, and `W` is chosen at a
/// Phase-2 quorum `{0, 3}` of `C_new`. A read over `(C_old, b_old)` counts
/// the reader's own watermark with no fence, so only the gate of the reader
/// on its own promise keeps the read from a stale index.
#[test]
fn a_lagging_reader_that_promised_above_its_basis_serves_no_read() {
    let wide = [0, 1, 2, 3, 4];
    let lagging = [0, 1, 2];
    let mut nodes = [
        deployed_node(0, &[0, 1, 2], &wide, 1),
        deployed_node(1, &[0, 1, 2], &lagging, 1),
        deployed_node(2, &[0, 1, 2], &lagging, 1),
        deployed_node(3, &[0, 1, 2], &wide, 1),
        deployed_node(4, &[0, 1, 2], &wide, 1),
    ];
    let mut mms = registries(1);
    let b_old = settle_under_c_old(&mut nodes, &mut mms);

    // Node 1 opens a read on `(C_old, b_old)` before the reconfiguration:
    // it is open while its promise rises.
    assert_eq!(nodes[1].read_basis().map(|b| b.since), Some(b_old));
    nodes[1].quorum_read(8);
    let early_asks = drain(&mut nodes[1]);
    assert_eq!(nodes[1].quorum_reads().pending().len(), 1);

    let c_new = cfg(&[0, 3, 4]);
    let ReconfigureResult::Started(b_new) = nodes[0].reconfigure(&c_new) else {
        panic!("the reconfiguration starts");
    };
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(&mut mms, requests) {
        let _ = nodes[0].on_match_reply(reply);
    }
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(2));
    assert!(nodes[0].is_leader());
    // The state under test, at the reader: the promise moved to `b_new`,
    // the belief stayed at `b_old`.
    assert_eq!(nodes[1].acceptor().promised(), b_new);
    assert_eq!(nodes[1].acceptors_since(), b_old);
    assert!(
        nodes[1].quorum_reads().is_empty(),
        "the promise above the basis abandons the read opened before it"
    );

    let _ = nodes[0].propose(entry(1, 2, 2));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, _| to == NodeId(0) || to == NodeId(3));
    let w_slot = nodes[0]
        .hard_state()
        .chosen_index
        .expect("W is chosen at the leader");
    assert!(w_slot > Slot(0));
    assert_eq!(chosen_at(&nodes[0], w_slot.0), Some(val(2)), "W is chosen");

    // The late answer of node 2 to the read opened before the promise
    // finds no open read.
    for (to, m) in early_asks {
        if to == NodeId(2) {
            nodes[2].step(m);
            for (to, ack) in drain(&mut nodes[2]) {
                if to == NodeId(1) {
                    nodes[1].step(ack);
                }
            }
        }
    }
    let ready = nodes[1].ready();
    let served = ready.read_states().to_vec();
    ready.advance();
    assert!(
        served.is_empty(),
        "stale read: served {served:?} below W at {w_slot:?}; the reader promised {b_new:?} above its basis {b_old:?}"
    );

    // A new read: the reader has no basis while its promise is above it.
    assert!(
        nodes[1].read_basis().is_none(),
        "a promise above the basis leaves the reader no basis"
    );
    let without = nodes[1].membership_counters().reads_without_basis;
    nodes[1].quorum_read(9);
    assert!(
        drain(&mut nodes[1]).is_empty(),
        "a read with no basis asks nobody"
    );
    assert!(nodes[1].quorum_reads().is_empty());
    assert_eq!(
        nodes[1].membership_counters().reads_without_basis,
        without + 1
    );
}

// ---- #890 (learner above the leader), option (b) ----------------------------

/// Node 3 of `{1, 2, 3}` (pool `0..4`) campaigns on its belief, and the one
/// matchmaker answers that node 2 reconfigured to `members` at a ballot
/// below the campaign. The campaign is abandoned before any `Prepare`: the
/// node keeps its promise of its own ballot, above the leader. Returns the
/// node, its campaign ballot and the leader's ballot.
fn stranded_learner(members: &[u64]) -> (ColocatedNode, Ballot, Ballot) {
    let mut n = deployed_node(3, &[1, 2, 3], &[0, 1, 2, 3], 1);
    campaign(&mut n);
    let own = n.ballot();
    let lead = ballot(own.round, 2);
    assert!(lead < own, "the leader sits below the campaign");
    let _ = drain_match_requests(&mut n);
    let history = BTreeMap::from([super::matchmaking::reconfigured(lead, members)]);
    assert_eq!(
        n.on_match_reply(super::matchmaking::registered_with(
            0,
            own,
            history,
            Ballot::zero()
        )),
        MatchStep::StaleConfiguration { newest: lead }
    );
    let sent = drain(&mut n);
    assert!(
        !sent
            .iter()
            .any(|(_, m)| matches!(m, Message::Prepare { .. })),
        "the abandoned campaign sent no Prepare"
    );
    assert_eq!(n.role(), NodeRole::Follower);
    assert_eq!(n.hard_state().max_promised_ballot, own);
    (n, own, lead)
}

/// The leader's beat: ballot `lead`, configuration `members`, no fence.
fn beat(lead: Ballot, members: &[u64]) -> Message {
    Message::Heartbeat {
        from: NodeId(2),
        ballot: lead,
        commit: None,
        config: Some(cfg(members)),
        fence: None,
        gc_floor: None,
    }
}

/// The case of #890: a node whose only promise above the leader is its own
/// campaign that the matchmakers refused takes the leader's beat as its
/// read basis when the leader's configuration leaves it out, and serves a
/// read on it. The beat is still not followed and not acked. Before option
/// (b), the node had no basis until a later leader change.
#[test]
fn a_learner_above_the_leader_by_its_own_unprepared_campaign_reads() {
    let (mut n, own, lead) = stranded_learner(&[0, 1, 2]);
    n.step(beat(lead, &[0, 1, 2]));
    let sent = drain(&mut n);
    assert!(
        !sent
            .iter()
            .any(|(_, m)| matches!(m, Message::HeartbeatAck { .. })),
        "a beat below the promise is not acked"
    );
    assert_eq!(n.leader(), None, "a beat below the promise is not followed");
    assert_eq!(
        n.hard_state().max_promised_ballot,
        own,
        "the promise stands"
    );
    assert_eq!(
        n.read_basis().map(|b| (b.config, b.since)),
        Some((cfg(&[0, 1, 2]), lead))
    );

    n.quorum_read(1);
    let targets = pre_read_targets(&drain(&mut n));
    assert!(
        !targets.contains(&NodeId(3)),
        "the learner does not answer its own read"
    );
    for to in targets {
        n.step(Message::PreReadAck {
            from: to,
            ctx: 1,
            watermark: None,
            config_since: Some(lead),
        });
    }
    assert_eq!(
        n.pending_read_states
            .iter()
            .map(|r| r.ctx)
            .collect::<Vec<_>>(),
        vec![1],
        "the read is served"
    );
}

/// The read stays judged: a `Prepare` of another campaign above the leader
/// raises the promise, which is no longer the node's own, so the open read
/// is abandoned and the basis no longer counts.
#[test]
fn a_foreign_promise_ends_the_learner_exception() {
    let (mut n, own, lead) = stranded_learner(&[0, 1, 2]);
    n.step(beat(lead, &[0, 1, 2]));
    let _ = drain(&mut n);
    n.quorum_read(1);
    let _ = drain(&mut n);
    assert_eq!(n.quorum_reads().pending().len(), 1);
    n.step(Message::Prepare {
        reply_to: NodeId(1),
        ballot: ballot(own.round + 1, 1),
        from_slot: Slot(0),
        config: Some(cfg(&[0, 1, 2])),
    });
    let _ = drain(&mut n);
    assert!(
        n.quorum_reads().is_empty(),
        "a foreign promise above the basis ends the read"
    );
    assert!(n.read_basis().is_none());
    n.step(beat(lead, &[0, 1, 2]));
    let _ = drain(&mut n);
    assert!(
        n.read_basis().is_none(),
        "a beat below a foreign promise is no basis"
    );
}

/// The exception needs the node outside the leader's configuration: a
/// member answers its own read with no fence, so its promise gates it.
#[test]
fn a_member_above_the_leader_takes_no_basis() {
    let (mut n, _, lead) = stranded_learner(&[2, 3]);
    n.step(beat(lead, &[2, 3]));
    let _ = drain(&mut n);
    assert!(n.read_basis.is_none(), "the beat is not stored as a basis");
    assert!(n.read_basis().is_none());
    n.quorum_read(1);
    assert!(n.quorum_reads().is_empty());
}

/// The exception needs a campaign that sent no `Prepare`: once its Phase 1
/// opened, an acceptor may hold its promise, so a beat below it is no basis.
#[test]
fn a_prepared_campaign_keeps_the_strict_rule() {
    let mut n = deployed_node(3, &[1, 2, 3], &[0, 1, 2, 3], 1);
    campaign(&mut n);
    let own = n.ballot();
    let _ = drain_match_requests(&mut n);
    let _ = n.on_match_reply(super::matchmaking::registered(
        0,
        own,
        &[(own, cfg(&[1, 2, 3]))],
        Ballot::zero(),
    ));
    let sent = drain(&mut n);
    assert!(
        sent.iter()
            .any(|(_, m)| matches!(m, Message::Prepare { .. })),
        "the campaign opened its Phase 1"
    );
    let lead = ballot(own.round, 2);
    n.step(beat(lead, &[0, 1, 2]));
    let _ = drain(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate, "Phase 1 is open");
    assert!(n.read_basis().is_none());
}

/// The exception needs a campaign that is over: while its matchmaking is
/// open, it may still send a `Prepare`.
#[test]
fn an_open_campaign_takes_no_basis() {
    let mut n = deployed_node(3, &[1, 2, 3], &[0, 1, 2, 3], 1);
    campaign(&mut n);
    let own = n.ballot();
    assert!(n.matchmaking_role().is_some(), "the campaign is matching");
    n.step(beat(ballot(own.round, 2), &[0, 1, 2]));
    let _ = drain(&mut n);
    assert!(n.read_basis().is_none());
}

/// The exception needs the promise to be the node's own: a `Prepare` of
/// another campaign whose configuration the node's pool makes it ignore
/// raises the promise and leaves the belief ballot below it. The basis of
/// the stranded learner stops counting, though the node is still outside
/// the basis's configuration.
#[test]
fn a_foreign_promise_under_an_ignored_configuration_ends_the_exception() {
    let (mut n, own, lead) = stranded_learner(&[0, 1, 2]);
    n.step(beat(lead, &[0, 1, 2]));
    let _ = drain(&mut n);
    assert!(n.read_basis().is_some());
    let foreign = ballot(own.round + 1, 1);
    n.step(Message::Prepare {
        reply_to: NodeId(1),
        ballot: foreign,
        from_slot: Slot(0),
        config: Some(cfg(&[0, 1, 4])),
    });
    let _ = drain(&mut n);
    assert_eq!(n.hard_state().max_promised_ballot, foreign);
    assert_eq!(
        n.acceptors_since(),
        lead,
        "a configuration outside the pool is not adopted"
    );
    assert!(
        n.read_basis().is_none(),
        "a foreign promise gates the basis"
    );
}

/// The exception needs the node outside the configuration of each open
/// read too: a read that a member opened before its campaign ends once
/// the matchmakers refuse that campaign, because the member answered that
/// read itself with no fence.
#[test]
fn a_member_read_ends_under_its_own_unprepared_campaign() {
    let mut n = deployed_node(3, &[1, 2, 3], &[0, 1, 2, 3], 1);
    campaign(&mut n);
    let first = n.ballot();
    let _ = drain_match_requests(&mut n);
    // A leader of `{1, 2, 3}` above this campaign: the node follows it
    // and reads over it as a member.
    let lead = ballot(first.round + 1, 2);
    n.step(beat(lead, &[1, 2, 3]));
    let _ = drain(&mut n);
    n.quorum_read(1);
    let _ = drain(&mut n);
    assert_eq!(n.quorum_reads().pending().len(), 1);
    // The node campaigns again with no tick (so no read is fenced while
    // the campaign matches), and the matchmaker refuses the campaign.
    n.campaign();
    let own = n.ballot();
    assert!(own > lead);
    let _ = drain_match_requests(&mut n);
    let _ = n.on_match_reply(super::matchmaking::refused(
        0,
        own,
        MatchRefusal::Stale {
            highest: ballot(own.round + 1, 1),
        },
    ));
    assert_eq!(n.role(), NodeRole::Follower);
    assert_eq!(n.hard_state().max_promised_ballot, own);
    n.serve_quorum_reads();
    assert!(
        n.quorum_reads().is_empty(),
        "a member's read ends under its own promise"
    );
}

/// A node of the full `2 × 2` grid `G = {0, 1, 2, 3}` (rows `{0, 1}` and
/// `{2, 3}`) with node 4 in its pool and one matchmaker.
fn deployed_grid_node(id: u64) -> ColocatedNode {
    let mut storage = MemStorage::for_node(id, &[0, 1, 2, 3]);
    storage.config.nodes = (0..5).map(NodeId).collect();
    storage.config.matchmakers = vec![MatchmakerId(0)];
    storage.config.quorum_system = crate::membership::QuorumSystem::grid(2, 2);
    ColocatedNode::new(&storage, Limits::DEFAULT)
}

/// Decision 31 (#1209): a Phase 1 seals each row of each full grid of
/// `H_b` other than `C_b`. The trace of `specs/tla/reads/`, check
/// `grid_full`, on real nodes:
///
/// - Node 2 leads `b1` under the full `2 × 2` grid `G = {0, 1, 2, 3}` (rows
///   `{0, 1}` and `{2, 3}`), and one write is chosen everywhere: node 0 reads
///   on `(G, b1)`.
/// - Node 2 reconfigures to `H = {1, 2, 3, 4}` (rows `{1, 2}` and `{3, 4}`)
///   at `b2`. Only nodes 3 and 4 get the `Prepare`. The row `{2, 3}` of `G`
///   promised: a Phase-1 quorum of `G` and of `H`.
/// - Expected: node 2 does not lead yet, because no acceptor of the row
///   `{0, 1}` of `G` promised `b2`. Without the seal it leads, a column of
///   `H` chooses `W`, and node 0 reads over the row `{0, 1}`, where no one
///   heard of `b2`: the read misses `W`.
/// - Once node 1 promises, node 2 leads, `W` is chosen without nodes 0 and
///   1, and the answer of node 1 to the read of node 0 names `b2` and ends
///   the read. No read is served below `W`.
#[test]
fn a_phase1_seals_each_row_of_an_old_full_grid() {
    let mut nodes: Vec<ColocatedNode> = (0..5).map(deployed_grid_node).collect();
    let mut mms = registries(1);
    let g = AcceptorConfig::new(
        (0..4).map(NodeId).collect(),
        crate::membership::QuorumSystem::grid(2, 2),
    );

    // Node 2 wins `b1` under `G`, beats, and has one write chosen everywhere.
    campaign(&mut nodes[2]);
    let requests = drain_match_requests(&mut nodes[2]);
    for reply in matchmake(&mut mms, requests) {
        nodes[2].on_match_reply(reply);
    }
    let q = drain(&mut nodes[2]);
    deliver_all(&mut nodes, q);
    assert!(nodes[2].is_leader());
    nodes[2].set_election_timeout(NO_CHECK_QUORUM);
    nodes[2].tick();
    let q = drain(&mut nodes[2]);
    deliver_all(&mut nodes, q);
    let b1 = nodes[2].ballot();
    let _ = nodes[2].propose(entry(1, 1, 1));
    let q = drain(&mut nodes[2]);
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[2].hard_state().chosen_index, Some(Slot(0)));
    assert_eq!(
        nodes[0].read_basis().map(|b| (b.config, b.since)),
        Some((g, b1)),
        "node 0 reads on (G, b1)"
    );

    // Node 2 reconfigures to `H`. Only nodes 3 and 4 get the `Prepare`.
    let h = AcceptorConfig::new(
        (1..5).map(NodeId).collect(),
        crate::membership::QuorumSystem::grid(2, 2),
    );
    let ReconfigureResult::Started(b2) = nodes[2].reconfigure(&h) else {
        panic!("the reconfiguration starts");
    };
    assert!(b2 > b1);
    let requests = drain_match_requests(&mut nodes[2]);
    for reply in matchmake(&mut mms, requests) {
        let _ = nodes[2].on_match_reply(reply);
    }
    let prepares = drain(&mut nodes[2]);
    let to_node_1: Vec<(NodeId, Message)> = prepares
        .iter()
        .filter(|(to, m)| *to == NodeId(1) && matches!(m, Message::Prepare { .. }))
        .cloned()
        .collect();
    assert_eq!(to_node_1.len(), 1, "the Prepare of b2 reaches for node 1");
    deliver_filtered(&mut nodes, prepares, |to, _| {
        to == NodeId(2) || to == NodeId(3) || to == NodeId(4)
    });
    assert_eq!(nodes[3].acceptor().promised(), b2);
    assert!(
        !nodes[2].is_leader(),
        "the row {{0, 1}} of G heard nothing of b2: the Phase 1 of b2 is not sealed"
    );

    // Node 1 promises: every row of `G` holds a promise, and node 2 leads.
    deliver_filtered(&mut nodes, to_node_1, |to, _| to != NodeId(0));
    assert!(nodes[2].is_leader(), "a sealed Phase 1 wins");
    assert_eq!(nodes[2].ballot(), b2);
    assert_eq!(*nodes[2].acceptors(), h);

    // `W` is chosen under `H` without nodes 0 and 1.
    let _ = nodes[2].propose(entry(1, 2, 2));
    let q = drain(&mut nodes[2]);
    deliver_filtered(&mut nodes, q, |to, _| to != NodeId(0) && to != NodeId(1));
    let w_slot = nodes[2]
        .hard_state()
        .chosen_index
        .expect("W is chosen at the leader");
    assert!(w_slot > Slot(0));
    assert_eq!(chosen_at(&nodes[2], w_slot.0), Some(val(2)), "W is chosen");

    // Node 0 still reads on `(G, b1)` and asks its row `{0, 1}` (ctx 0).
    assert_eq!(nodes[0].read_basis().map(|b| b.since), Some(b1));
    nodes[0].quorum_read(0);
    let asks = drain(&mut nodes[0]);
    assert_eq!(pre_read_targets(&asks), vec![NodeId(1)]);
    for (to, m) in asks {
        nodes[usize::try_from(to.0).expect("node index")].step(m);
        for (to, ack) in drain(&mut nodes[1]) {
            if to == NodeId(0) {
                nodes[0].step(ack);
            }
        }
    }
    let ready = nodes[0].ready();
    let served = ready.read_states().to_vec();
    ready.advance();
    assert!(
        served.is_empty(),
        "stale read: served {served:?} below W at {w_slot:?} over the row {{0, 1}} of G"
    );
    assert!(
        nodes[0].quorum_reads().is_empty(),
        "the answer of node 1 names b2 and ends the read"
    );
}
