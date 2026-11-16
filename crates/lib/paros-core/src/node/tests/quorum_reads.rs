//! Quorum reads (#143): the leaderless read path, pinned at the node.

use super::*;
#[allow(clippy::wildcard_imports)]
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
/// (`q2 = 1`, so `q1` is all three) beat at ballot 6, then promises a
/// campaign at 7 for `C_7 = {0, 2, 3}`, a majority whose quorum `{2, 3}`
/// need not hold what `C_6`'s `{0}` chose. Under the campaign it opens no
/// read; once the leadership of 7 beats with its fence, a read over `C_7`
/// waits for the fence even when the row's watermarks sit below it.
#[test]
fn a_campaign_configuration_is_no_read_basis() {
    let c6 = AcceptorConfig::new(
        vec![NodeId(0), NodeId(1), NodeId(3)],
        crate::membership::QuorumSystem::Flexible { q1: 3, q2: 1 },
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

/// Finding 1 of the paros-core review of 2026-11-16 (a `PreReadAck` fences
/// a read with the belief ballot, not the promise). The reads model fences
/// on the promise (`specs/tla/reads/Reads.tla:264`, `AckBallot`): an answer
/// from an acceptor that promised a ballot above the read's basis ends the
/// read. This test drives the full trace on real nodes.
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
/// below `W`.
#[test]
#[allow(clippy::too_many_lines)]
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

    // Node 0 wins `b_old` under `C_old` and beats: every node hears it.
    campaign(&mut nodes[0]);
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(&mut mms, requests) {
        nodes[0].on_match_reply(reply);
    }
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    nodes[0].set_election_timeout(NO_CHECK_QUORUM);
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    let b_old = nodes[0].ballot();

    // One write under `C_old`, chosen everywhere: the leadership settles.
    let _ = nodes[0].propose(entry(1, 1, 1));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[0].hard_state().chosen_index, Some(Slot(0)));
    for n in &nodes[1..3] {
        assert_eq!(
            n.read_basis().map(|b| (b.config, b.since)),
            Some((cfg(&[0, 1, 2]), b_old)),
            "node {} reads on (C_old, b_old)",
            n.config().id.0
        );
    }

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
    // What node 1 sends: the belief ballot, not its promise.
    let Message::PreReadAck { config_since, .. } = &ack.1 else {
        unreachable!()
    };
    assert_eq!(
        *config_since,
        Some(b_old),
        "node 1 answers with its belief ballot, below its promise"
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
