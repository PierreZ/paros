#[allow(clippy::wildcard_imports)]
use super::*;

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
        config_id: ConfigId::default(),
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
        config_id: ConfigId::default(),
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
fn chosen_index_advances_only_over_contiguous_prefix() {
    // Learn slots 0 and 2 (gap at 1): the applied prefix stops at 0. Filling
    // slot 1 then jumps it to 2.
    let mut n = node(1, &[0, 1, 2]);
    let b = ballot(3, 0);
    n.step(Message::Commit {
        config_id: ConfigId::default(),
        from: NodeId(0),
        ballot: b,
        slot: Slot(0),
        command: ucmd(1, 1, 10),
    });
    n.step(Message::Commit {
        config_id: ConfigId::default(),
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
        config_id: ConfigId::default(),
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
fn accepted_fingerprint_must_match_the_inflight_command() {
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(1);
    n.tick();
    let _ = drain(&mut n);
    let camp = n.ballot();
    n.step(Message::Promise {
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: camp,
        from_slot: Slot(0),
        accepted: BTreeMap::new(),
        next_from_slot: None,
    });
    let _ = drain(&mut n);

    let ProposeResult::Accepted(slot) = n.propose(ClientId(4), ClientSeq(5), val(6)) else {
        panic!("leader must admit the proposal");
    };
    let expected = command_fingerprint(&n.proposer[&slot].command);
    n.step(Message::Accepted {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: camp,
        slot,
        vhash: expected ^ 1,
    });
    assert_eq!(n.hard_state().chosen_index, None);

    n.step(Message::Accepted {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: camp,
        slot,
        vhash: expected,
    });
    assert_eq!(n.hard_state().chosen_index, Some(slot));
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
        config_id: ConfigId::default(),
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
        faulty: Vec::new(),
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
fn commit_below_floor_is_not_relearned() {
    let mut nodes = cluster_with_three_chosen();
    let n = &mut nodes[0];
    n.compact(Slot(2)); // floor -> 3
    let _ = drain(n);

    n.step(Message::Commit {
        config_id: ConfigId::default(),
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

/// The catch-up half of the same freeze: a `Commit` for a slot already in
/// `chosen` must still re-drive the contiguous walk. Pre-fix, `mark_chosen`'s
/// early return skipped it, so a node stuck one below an already-known slot
/// looped `CatchUpRequest` forever while holding the very commit it needed.
#[test]
fn a_replayed_commit_for_a_known_slot_still_advances_the_prefix() {
    let mut x = node(0, &[0, 1, 2]);
    x.step(Message::Commit {
        config_id: ConfigId::default(),
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
        config_id: ConfigId::default(),
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
        config_id: ConfigId::default(),
        from: NodeId(2),
        ballot: ballot(3, 2),
        slot: Slot(1),
        command: ucmd(1, 1, 0xBB),
    });
    let _ = drain(&mut x);
    assert_eq!(x.hard_state.chosen_index, Some(Slot(1)));
}
