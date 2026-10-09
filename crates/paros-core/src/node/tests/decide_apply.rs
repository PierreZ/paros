#[allow(clippy::wildcard_imports)]
use super::*;

#[test]
fn leader_streams_multiple_slots_and_all_nodes_agree() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);

    for (seq, b) in [(1u64, 10u8), (2, 20), (3, 30)] {
        let r = nodes[0].propose(entry(1, seq, b));
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
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    // node 1 learned the leader via the election traffic.
    let r = nodes[1].propose(entry(1, 1, 7));
    assert_eq!(r, ProposeResult::NotLeader(Some(NodeId(0))));
    assert!(
        drain(&mut nodes[1]).is_empty(),
        "a follower proposes nothing"
    );
}

#[test]
fn chosen_index_advances_only_over_contiguous_prefix() {
    // Learn slots 0 and 2 (gap at 1): the applied prefix stops at 0. Filling
    // slot 1 then jumps it to 2.
    let mut n = node(1, &[0, 1, 2]);
    let b = ballot(3, 0);
    n.step(Message::Commit {
        from: Party::Node(NodeId(0)),
        ballot: b,
        slot: Slot(0),
        command: ucmd(1, 1, 10),
    });
    n.step(Message::Commit {
        from: Party::Node(NodeId(0)),
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
        from: Party::Node(NodeId(0)),
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

/// One command per (slot, ballot) (P2b, #317): an `Accepted` at the open
/// round's own ballot that names another command is a broken invariant, not a
/// vote to ignore.
#[test]
#[should_panic(expected = "an Accepted at an open round's ballot names the round's command")]
fn accepted_fingerprint_must_match_the_inflight_command() {
    let mut n = node(0, &[0, 1, 2]);
    campaign(&mut n);
    let _ = drain(&mut n);
    let camp = n.ballot();
    n.step(terminal_promise(NodeId(1), camp, BTreeMap::new()));
    let _ = drain(&mut n);

    let ProposeResult::Accepted(slot) = n.propose(entry(4, 5, 6)) else {
        panic!("leader must admit the proposal");
    };
    let expected = command_fingerprint(n.proposer.rounds()[&slot].command());
    n.step(Message::Accepted {
        from: NodeId(1),
        ballot: camp,
        slot,
        vhash: expected ^ 1,
    });
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
        ..TestStorage::new(1, &[0, 1, 2])
    };
    let n = ColocatedNode::new(&storage);
    assert_eq!(n.ballot(), ballot(2, 0), "resumes the promised ballot");
    assert_eq!(
        n.proposer().next_slot(),
        Slot(3),
        "next_slot is past the highest accepted slot"
    );
    assert_eq!(n.role(), NodeRole::Follower);
    // The journal fold: slot 0 and 1 are folded (refused writes, since
    // nobody owns the journal), slot 2 is not.
    assert_eq!(n.replica().folded(), Slot(2));
    assert!(matches!(
        n.replica().outcome_at(Slot(1)),
        Some(crate::Outcome::Refused(_))
    ));
    assert_eq!(n.replica().outcome_at(Slot(2)), None);
}

#[test]
fn propose_control_is_leader_only() {
    let mut nodes = cluster_with_three_chosen();
    // A follower refuses to admit a control command and redirects to the leader.
    let r = nodes[1].propose_control(Control::Truncate {
        leader: LeaderUuid(7),
        up_to: Seq(1),
    });
    assert!(
        matches!(r, ProposeResult::NotLeader(Some(NodeId(0)))),
        "a non-leader redirects the truncate to the leader"
    );
    assert_eq!(
        nodes[1].acceptor().first_slot(),
        Slot(0),
        "no truncation on a redirect"
    );
}

#[test]
fn commit_below_floor_is_not_relearned() {
    let mut nodes = cluster_with_three_chosen();
    let n = &mut nodes[0];
    n.compact(Slot(2)); // floor -> 3
    let _ = drain(n);

    n.step(Message::Commit {
        from: Party::Node(NodeId(1)),
        ballot: ballot(1, 0),
        slot: Slot(1),
        command: ucmd(7, 7, 88),
    });
    assert!(
        chosen_at(n, 1).is_none(),
        "a below-floor commit is not relearned"
    );
    assert!(
        !n.acceptor().records().contains_key(&Slot(1)),
        "a below-floor commit records nothing below the floor"
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
        from: Party::Node(NodeId(2)),
        ballot: ballot(3, 2),
        slot: Slot(1),
        command: ucmd(1, 1, 0xBB),
    });
    let _ = drain(&mut x);
    assert_eq!(
        x.hard_state().chosen_index,
        None,
        "slot 1 is above the hole at 0"
    );

    // Slot 0 arrives; the prefix advances through both.
    x.step(Message::Commit {
        from: Party::Node(NodeId(2)),
        ballot: ballot(3, 2),
        slot: Slot(0),
        command: ucmd(1, 0, 0xCC),
    });
    let _ = drain(&mut x);
    assert_eq!(x.hard_state().chosen_index, Some(Slot(1)));

    // A duplicated / catch-up-replayed commit for a known slot is a no-op for
    // state but must never wedge: the early return still re-drives the walk.
    x.step(Message::Commit {
        from: Party::Node(NodeId(2)),
        ballot: ballot(3, 2),
        slot: Slot(1),
        command: ucmd(1, 1, 0xBB),
    });
    let _ = drain(&mut x);
    assert_eq!(x.hard_state().chosen_index, Some(Slot(1)));
}

/// A six-node `2 × 3` grid over `0..6`: rows `{0, 1, 2}` and `{3, 4, 5}`,
/// columns `{0, 3}`, `{1, 4}` and `{2, 5}`.
fn grid_node(id: u64) -> ColocatedNode {
    let mut storage = TestStorage::new(id, &[0, 1, 2, 3, 4, 5]);
    storage.config.quorum_system = crate::membership::QuorumSystem::Grid { rows: 2, cols: 3 };
    ColocatedNode::new(&storage)
}

fn accept_targets(queue: &[(NodeId, Message)]) -> Vec<NodeId> {
    queue
        .iter()
        .filter(|(_, m)| matches!(m, Message::Accepted { .. } | Message::Accept { .. }))
        .filter_map(|(to, m)| matches!(m, Message::Accept { .. }).then_some(*to))
        .collect()
}

/// The driver-named column (#141's sim half): `propose_in` addresses the
/// round to the column the driver named instead of `slot % cols`, the
/// re-send stays on it, and the decision is judged by it — while a stray
/// column is a programmer error.
#[test]
fn a_driver_named_column_is_the_round_s_column() {
    let mut nodes: Vec<ColocatedNode> = (0..6).map(grid_node).collect();
    make_leader(&mut nodes, 0);
    // Slot 0 would go to column 0 = {0, 3}; the driver names column 2 =
    // {2, 5}, of which the leader is not a member.
    assert!(matches!(
        nodes[0].propose_in(entry(1, 1, 10), Some(2), Delegation::Auto),
        ProposeResult::Accepted(Slot(0))
    ));
    let first = drain(&mut nodes[0]);
    assert_eq!(accept_targets(&first), vec![NodeId(2), NodeId(5)]);
    assert_eq!(nodes[0].proposer.round_column(Slot(0)), Some(Some(2)));
    assert!(
        nodes[0].proposer.rounds()[&Slot(0)]
            .accepted_by()
            .expect("colocated")
            .is_empty(),
        "the leader is outside the named column and casts no vote"
    );
    nodes[0].resend_pending();
    assert_eq!(
        accept_targets(&drain(&mut nodes[0])),
        vec![NodeId(2), NodeId(5)]
    );
    // The slot's own column {0, 3} decides nothing for this round.
    let ballot = nodes[0].ballot();
    nodes[0].step(Message::Accepted {
        from: NodeId(3),
        ballot,
        slot: Slot(0),
        vhash: command_fingerprint(&ucmd(1, 1, 10)),
    });
    assert_eq!(chosen_at(&nodes[0], 0), None);
    deliver_all(&mut nodes, first);
    for n in &nodes {
        assert_eq!(chosen_at(n, 0), Some(val(10)));
    }
    // `None` is exactly `propose`: slot 1 -> column 1 = {1, 4}.
    assert!(matches!(
        nodes[0].propose_in(entry(1, 2, 20), None, Delegation::Auto),
        ProposeResult::Accepted(Slot(1))
    ));
    assert_eq!(
        accept_targets(&drain(&mut nodes[0])),
        vec![NodeId(1), NodeId(4)]
    );
}

#[test]
#[should_panic(expected = "an accept round's column is a column of the active configuration")]
fn a_column_the_grid_does_not_have_is_a_programmer_error() {
    let mut nodes: Vec<ColocatedNode> = (0..6).map(grid_node).collect();
    make_leader(&mut nodes, 0);
    let _ = nodes[0].propose_in(entry(1, 1, 10), Some(3), Delegation::Auto);
}

#[test]
#[should_panic(expected = "an accept round's column is a column of the active configuration")]
fn a_column_under_a_majority_is_a_programmer_error() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].propose_in(entry(1, 9, 10), Some(0), Delegation::Auto);
}

/// The mechanism behind #141's column addressing, pinned at the node: a
/// slot's `Accept` goes to its column (`slot % cols`) and nowhere else, the
/// re-send goes to the same column, a configured acceptor outside the
/// column never counts toward the decision, and the column alone decides.
#[test]
fn a_grid_round_is_addressed_and_judged_by_its_column() {
    let mut nodes: Vec<ColocatedNode> = (0..6).map(grid_node).collect();
    make_leader(&mut nodes, 0);
    let ballot = nodes[0].ballot();

    // Slot 0 -> column 0 = {0, 3}. The leader sits in it: its own vote is
    // cast, and the only other addressee is node 3.
    assert!(matches!(
        nodes[0].propose(entry(1, 1, 10)),
        ProposeResult::Accepted(_)
    ));
    let first = drain(&mut nodes[0]);
    assert_eq!(accept_targets(&first), vec![NodeId(3)]);
    assert_eq!(nodes[0].proposer.round_column(Slot(0)), Some(Some(0)));
    assert!(
        nodes[0].proposer.rounds()[&Slot(0)]
            .accepted_by()
            .expect("colocated")
            .contains(&NodeId(0))
    );

    // The re-send addresses exactly the column the round was opened
    // against — the column is a function of the slot, never re-drawn.
    nodes[0].resend_pending();
    let resent = drain(&mut nodes[0]);
    assert_eq!(accept_targets(&resent), vec![NodeId(3)]);

    // A configured acceptor outside the column (node 1, column 1) that
    // accepted a stray copy answers `Accepted`: the vote does not count,
    // and the round is not decided by it.
    nodes[0].step(Message::Accepted {
        from: NodeId(1),
        ballot,
        slot: Slot(0),
        vhash: command_fingerprint(&ucmd(1, 1, 10)),
    });
    assert!(
        !nodes[0].proposer.rounds()[&Slot(0)]
            .accepted_by()
            .expect("colocated")
            .contains(&NodeId(1)),
        "an out-of-column vote is never folded"
    );
    assert_eq!(chosen_at(&nodes[0], 0), None);

    // The column's other member decides it.
    deliver_all(&mut nodes, first);
    for n in &nodes {
        assert_eq!(chosen_at(n, 0), Some(val(10)));
    }

    // Slot 1 -> column 1 = {1, 4}: the leader is not an addressee and casts
    // no vote — both members must accept. It still records the round in its
    // own log, a stray copy the column does not count: the allocator is
    // durable by construction (`record_own_round`), so a reboot rederives
    // the frontier this proposal moved.
    assert!(matches!(
        nodes[0].propose(entry(1, 2, 20)),
        ProposeResult::Accepted(_)
    ));
    let second = drain(&mut nodes[0]);
    assert_eq!(accept_targets(&second), vec![NodeId(1), NodeId(4)]);
    assert_eq!(nodes[0].proposer.round_column(Slot(1)), Some(Some(1)));
    assert!(
        nodes[0].proposer.rounds()[&Slot(1)]
            .accepted_by()
            .expect("colocated")
            .is_empty(),
        "the leader's own record outside the column is not a vote"
    );
    assert_eq!(
        nodes[0].acceptor().record(Slot(1)),
        Some(&(ballot, ucmd(1, 2, 20))),
        "the leader records every round it opens, whichever column it went to"
    );
    // Only node 1 answers: half a column decides nothing.
    let only_node_1: Vec<(NodeId, Message)> = second
        .iter()
        .filter(|(to, _)| *to == NodeId(1))
        .cloned()
        .collect();
    deliver_all(&mut nodes, only_node_1);
    assert_eq!(chosen_at(&nodes[0], 1), None);
    let only_node_4: Vec<(NodeId, Message)> = second
        .into_iter()
        .filter(|(to, _)| *to == NodeId(4))
        .collect();
    deliver_all(&mut nodes, only_node_4);
    for n in &nodes {
        assert_eq!(chosen_at(n, 1), Some(val(20)));
    }
}

/// Run the leader's pending work to quiescence over a reliable network.
fn settle(nodes: &mut [ColocatedNode]) {
    let q = drain(&mut nodes[0]);
    deliver_all(nodes, q);
}

/// A `Write` by leader uuid `leader` at `seq`.
fn write(leader: u128, seq: u64, b: u8) -> Entry {
    Entry {
        leader: LeaderUuid(leader),
        seq: Seq(seq),
        records: vec![val(b)],
    }
}

/// The journal state machine (#204) is judged at apply, on every node alike:
/// a `SetLeader` claims the journal, the owner's writes take dense
/// positions, a retry is answered from the log, and a superseded owner is
/// refused in place — every outcome the same on every node.
#[test]
fn a_write_is_judged_at_apply_on_every_node() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    let ProposeResult::Accepted(claim) = nodes[0].propose_control(Control::SetLeader {
        new: LeaderUuid(7),
        old: None,
    }) else {
        panic!("the leader admits a SetLeader");
    };
    settle(&mut nodes);
    let mut slots = Vec::new();
    for e in [
        write(7, 0, 1),
        write(7, 1, 2),
        write(7, 0, 1), // a retry of position 0
        write(7, 0, 9), // position 0 with other bytes
        write(8, 2, 3), // a foreign writer
    ] {
        let ProposeResult::Accepted(slot) = nodes[0].propose(e) else {
            panic!("the leader admits a write");
        };
        slots.push(slot);
        settle(&mut nodes);
    }
    for n in &nodes {
        assert!(matches!(
            n.replica().outcome_at(claim),
            Some(crate::Outcome::Leader(s)) if s.leader == Some(LeaderUuid(7))
        ));
        assert_eq!(
            n.replica().outcome_at(slots[0]),
            Some(&crate::Outcome::Accepted {
                seq: Seq(0),
                count: 1
            })
        );
        assert_eq!(
            n.replica().outcome_at(slots[2]),
            Some(&crate::Outcome::Duplicate {
                seq: Seq(0),
                count: 1
            })
        );
        assert!(matches!(
            n.replica().outcome_at(slots[3]),
            Some(crate::Outcome::Refused(_))
        ));
        assert!(matches!(
            n.replica().outcome_at(slots[4]),
            Some(crate::Outcome::Refused(_))
        ));
        assert_eq!(n.replica().journal().next_seq, Seq(2));
    }
}

/// A decided `Truncate` keeps the slot holding the journal's first retained
/// record, seals the state the dropped slots folded to, and a restart from
/// the store folds the retained log back to the same state.
#[test]
fn a_truncation_seals_the_journal_state_a_restart_folds_from() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    let _ = nodes[0].propose_control(Control::SetLeader {
        new: LeaderUuid(7),
        old: None,
    });
    settle(&mut nodes);
    for seq in 0..3 {
        let _ = nodes[0].propose(write(7, seq, 10 + u8::try_from(seq).expect("small")));
        settle(&mut nodes);
    }
    let _ = nodes[0].propose_control(Control::Truncate {
        leader: LeaderUuid(7),
        up_to: Seq(2),
    });
    settle(&mut nodes);
    // Slot 0 is the claim, slots 1..=3 hold positions 0..=2: position 2's
    // slot (3) is the first retained one.
    for n in &nodes {
        assert_eq!(n.acceptor().first_slot(), Slot(3));
        assert_eq!(n.replica().journal().first_seq, Seq(2));
        assert_eq!(n.replica().journal_base().next_seq, Seq(2));
    }
    let storage = TestStorage::from_node(&nodes[1]);
    let rebooted = ColocatedNode::new(&storage);
    assert_eq!(rebooted.replica().journal(), nodes[1].replica().journal());
}
