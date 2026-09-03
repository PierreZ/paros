#[allow(clippy::wildcard_imports)]
use super::*;

#[test]
fn matching_configuration_message_is_processed_and_reply_is_tagged() {
    let mut storage = TestStorage::new(0, &[0, 1]);
    storage.hard_state.config_id = ConfigId(7);
    let mut n = RawNode::new(&storage);

    n.step(Message::Prepare {
        config_id: ConfigId(7),
        reply_to: NodeId(1),
        leader: NodeId(1),
        ballot: ballot(1, 1),
        from_slot: Slot(0),
        config: None,
    });

    let out = drain(&mut n);
    assert!(matches!(
        out.as_slice(),
        [(
            NodeId(1),
            Message::Promise {
                config_id: ConfigId(7),
                ..
            }
        )]
    ));
}

#[test]
fn mismatching_configuration_message_is_ignored_before_dispatch() {
    let mut storage = TestStorage::new(0, &[0, 1]);
    storage.hard_state.config_id = ConfigId(7);
    let mut n = RawNode::new(&storage);
    let promise_before = n.hard_state().max_promised_ballot;

    // A foreign configuration id is an operating condition (a stale peer, a
    // misconfiguration), never a local invariant: the message is ignored
    // whole — no reply, no promise movement.
    n.step(Message::Prepare {
        config_id: ConfigId(8),
        reply_to: NodeId(1),
        leader: NodeId(1),
        ballot: ballot(1, 1),
        from_slot: Slot(0),
        config: None,
    });

    let out = drain(&mut n);
    assert!(
        out.is_empty(),
        "a cross-configuration prepare draws no reply"
    );
    assert_eq!(
        n.hard_state().max_promised_ballot,
        promise_before,
        "a cross-configuration prepare must not move the promise"
    );
}

#[test]
fn promise_and_accept_batches_require_fsync() {
    use crate::write::MustSync;

    // An acceptor promoting its promise on a higher Prepare must fsync before it
    // replies Promise.
    let mut n = node(0, &[0, 1, 2]);
    n.step(Message::Prepare {
        config_id: ConfigId::default(),
        reply_to: NodeId(1),
        leader: NodeId(1),
        ballot: ballot(3, 1),
        from_slot: Slot(0),
        config: None,
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
        config_id: ConfigId::default(),
        reply_to: NodeId(1),
        leader: NodeId(1),
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
fn acceptor_rejects_below_promised_ballot() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(Message::Prepare {
        config_id: ConfigId::default(),
        reply_to: NodeId(1),
        leader: NodeId(1),
        ballot: ballot(5, 1),
        from_slot: Slot(0),
        config: None,
    });
    let _ = drain(&mut n);
    n.step(Message::Accept {
        config_id: ConfigId::default(),
        reply_to: NodeId(2),
        leader: NodeId(2),
        ballot: ballot(3, 2),
        slot: Slot(0),
        command: ucmd(1, 1, 9),
    });
    assert!(
        !n.acceptor().records().contains_key(&Slot(0)),
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
        config_id: ConfigId::default(),
        reply_to: NodeId(1),
        leader: NodeId(1),
        ballot: ballot(1, 1),
        slot: Slot(0),
        command: ucmd(9, 9, 1),
    });

    // Learn a DIFFERENT value was chosen for slot 0 at a higher ballot (this node
    // was not in the choosing quorum, so it never accepted that value).
    n.step(Message::Commit {
        config_id: ConfigId::default(),
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
        config_id: ConfigId::default(),
        reply_to: NodeId(1),
        leader: NodeId(1),
        ballot: ballot(9, 1),
        from_slot: Slot(0),
        config: None,
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
        config_id: ConfigId::default(),
        reply_to: NodeId(1),
        leader: NodeId(1),
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
        !n.acceptor().records().contains_key(&Slot(1)),
        "a below-floor accept records nothing"
    );
}
