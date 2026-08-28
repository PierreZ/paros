#[allow(clippy::wildcard_imports)]
use super::*;

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
        config_id: ConfigId::default(),
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
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(0),
        ballot: camp,
        from_slot: Slot(0),
        accepted: acc_low,
        next_from_slot: None,
    });
    assert!(!n.is_leader(), "one promise short of quorum");
    n.step(Message::Promise {
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(3),
        ballot: camp,
        from_slot: Slot(0),
        accepted: acc_high,
        next_from_slot: None,
    });
    assert!(n.is_leader(), "quorum reached");
    let (_, e) = n.accepted().get(&Slot(0)).expect("slot 0 re-accepted");
    assert_eq!(
        e, &high.1,
        "the highest-ballot accepted value is re-proposed"
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
        config_id: ConfigId::default(),
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
fn nack_does_not_retain_an_untrusted_promised_round() {
    // A valid member's Nack still deposes the stale campaign, but its reported
    // promise is an untrusted wire value and must not pin future round selection.
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(1);
    n.tick(); // Candidate at round 1
    let _ = drain(&mut n);
    let camp = n.ballot();
    n.step(Message::Nack {
        config_id: ConfigId::default(),
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
        ballot(2, 0),
        "the next campaign advances from durable local state, not the Nack wire hint"
    );
}

#[test]
fn a_non_member_nack_cannot_depose_a_campaign() {
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(1);
    n.tick();
    let _ = drain(&mut n);
    let camp = n.ballot();

    n.step(Message::Nack {
        config_id: ConfigId::default(),
        from: NodeId(99),
        ballot: camp,
        promised: ballot(u64::MAX, 99),
        slot: Slot(0),
    });

    assert_eq!(n.role(), NodeRole::Candidate);
    assert_eq!(n.ballot(), camp);
}

#[test]
fn promise_suffix_is_served_in_bounded_pages() {
    let mut storage = TestStorage::new(0, &[0, 1, 2]);
    for slot in 0..130 {
        storage.accepted.insert(
            Slot(slot),
            (
                ballot(0, 0),
                ucmd(7, slot, u8::try_from(slot).expect("test slot fits u8")),
            ),
        );
    }
    let mut n = RawNode::new(&storage);
    let prepared = ballot(1, 1);
    let mut cursor = Slot(0);
    let mut pages = Vec::new();

    loop {
        n.step(Message::Prepare {
            config_id: ConfigId::default(),
            from: NodeId(1),
            ballot: prepared,
            from_slot: cursor,
        });
        let out = drain(&mut n);
        let [
            (
                NodeId(1),
                Message::Promise {
                    accepted,
                    next_from_slot,
                    ..
                },
            ),
        ] = out.as_slice()
        else {
            panic!("expected one Promise page, got {out:?}");
        };
        assert!(accepted.len() <= PROMISE_BATCH);
        pages.push(accepted.len());
        let Some(next) = next_from_slot else {
            break;
        };
        cursor = *next;
    }

    assert_eq!(pages, vec![PROMISE_BATCH, PROMISE_BATCH, 2]);
}

#[test]
fn a_partial_promise_page_does_not_count_toward_the_quorum() {
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(1);
    n.tick();
    let _ = drain(&mut n);
    let camp = n.ballot();
    let accepted = (0..PROMISE_BATCH as u64)
        .map(|slot| {
            (
                Slot(slot),
                (
                    ballot(0, 1),
                    ucmd(8, slot, u8::try_from(slot).expect("test slot fits u8")),
                ),
            )
        })
        .collect();

    n.step(Message::Promise {
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: camp,
        from_slot: Slot(0),
        accepted,
        next_from_slot: Some(Slot(PROMISE_BATCH as u64)),
    });
    assert_eq!(n.role(), NodeRole::Candidate);
    let _ = drain(&mut n);

    n.step(Message::Promise {
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: camp,
        from_slot: Slot(PROMISE_BATCH as u64),
        accepted: BTreeMap::new(),
        next_from_slot: None,
    });
    assert_eq!(n.role(), NodeRole::Leader);
}

#[test]
fn a_same_ballot_continuation_closes_a_different_stale_campaign() {
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(1);
    n.tick();
    let stale_campaign = n.ballot();
    let _ = drain(&mut n);
    let learned = ballot(stale_campaign.round + 1, 1);
    n.step(Message::Commit {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: learned,
        slot: Slot(0),
        command: ucmd(5, 1, 9),
    });
    let _ = drain(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate);
    assert_eq!(n.hard_state().max_promised_ballot, learned);

    n.step(Message::Prepare {
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: learned,
        from_slot: Slot(1),
    });

    assert_eq!(n.role(), NodeRole::Follower);
    assert!(n.election.is_none());
    let out = drain(&mut n);
    assert!(matches!(out.as_slice(), [(_, Message::Promise { .. })]));
}

#[test]
fn leader_recovery_is_split_across_ready_batches() {
    let mut n = node(0, &[0, 1, 2]);
    n.set_election_timeout(1);
    n.tick();
    let _ = drain(&mut n);
    let camp = n.ballot();

    for (from, len, next) in [
        (0_u64, PROMISE_BATCH, Some(PROMISE_BATCH as u64)),
        (
            PROMISE_BATCH as u64,
            PROMISE_BATCH,
            Some((2 * PROMISE_BATCH) as u64),
        ),
        ((2 * PROMISE_BATCH) as u64, 2, None),
    ] {
        let accepted = (from..from + len as u64)
            .map(|slot| {
                (
                    Slot(slot),
                    (
                        ballot(0, 1),
                        ucmd(9, slot, u8::try_from(slot).expect("test slot fits u8")),
                    ),
                )
            })
            .collect();
        n.step(Message::Promise {
        faulty: BTreeMap::new(),
            config_id: ConfigId::default(),
            from: NodeId(1),
            ballot: camp,
            from_slot: Slot(from),
            accepted,
            next_from_slot: next.map(Slot),
        });
        if next.is_some() {
            let _ = drain(&mut n);
        }
    }

    assert_eq!(n.role(), NodeRole::Leader);
    let ready = n.ready();
    assert_eq!(ready.recovery_batch(), Some((LEADER_RECOVERY_BATCH, 0, 66)));
    ready.advance();
    n.advance_recovery();
    let ready = n.ready();
    assert_eq!(ready.recovery_batch(), Some((LEADER_RECOVERY_BATCH, 0, 2)));
    ready.advance();
    n.advance_recovery();
    let ready = n.ready();
    assert_eq!(ready.recovery_batch(), Some((2, 0, 0)));
    ready.advance();
    assert!(n.leader_recovery.is_none());
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
        config_id: ConfigId::default(),
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
        config_id: ConfigId::default(),
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
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: b,
        from_slot: Slot(0),
        accepted: reported,
        next_from_slot: None,
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
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: b2,
        // The fresh campaign solicits from `first_unchosen()` — slot 0 was
        // chosen by the learned commit, so the probe starts at slot 1.
        from_slot: Slot(1),
        accepted: reported,
        next_from_slot: None,
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
        config_id: ConfigId::default(),
        from: NodeId(2),
        ballot: b_prime,
        from_slot: Slot(0),
    });
    let _ = drain(&mut p);
    assert_eq!(p.hard_state.max_promised_ballot, b_prime);

    // It rejects the stale leader's `Accept` …
    p.step(Message::Accept {
        config_id: ConfigId::default(),
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
        config_id: ConfigId::default(),
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
        config_id: ConfigId::default(),
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
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: b,
        from_slot: Slot(0),
        accepted: reported,
        next_from_slot: None,
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
        faulty: BTreeMap::new(),
        config_id: ConfigId::default(),
        from: NodeId(1),
        ballot: b2,
        from_slot: x.first_slot,
        accepted: reported,
        next_from_slot: None,
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
