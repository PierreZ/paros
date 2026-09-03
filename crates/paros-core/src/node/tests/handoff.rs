//! Cooperative leader handoff (the `DPaxos` "Leader Handoff" technique).
//!
//! The simulation owns correctness; these tests pin the *shape* of the
//! transferred authority and every refusal path, so a regression names the rule
//! it broke instead of surfacing as a rare seed.

use super::{
    ClientId, ClientSeq, ColocatedNode, Command, ConfigId, Control, HANDOFF_BATCH,
    HANDOFF_FENCE_ELECTIONS, LeadershipOrigin, Message, NO_CHECK_QUORUM, NodeId, NodeRole,
    ProposeResult, Slot, TestStorage, ballot, chosen_at, cluster_with_three_chosen, deliver_all,
    deliver_filtered, drain, make_leader, node, ucmd, val,
};
use crate::proposer::RecoveryPolicy;
use std::collections::BTreeSet;

/// Pull the single `Relinquish` a leader queued, panicking if there is not
/// exactly one.
fn take_relinquish(queue: &[(NodeId, Message)]) -> (NodeId, Message) {
    let mut found: Vec<(NodeId, Message)> = queue
        .iter()
        .filter(|(_, m)| matches!(m, Message::Relinquish { .. }))
        .cloned()
        .collect();
    assert_eq!(found.len(), 1, "exactly one Relinquish is queued");
    found.pop().expect("checked above")
}

/// Rebuild a `Relinquish` with one field replaced, for the wire-guard tests.
fn tamper(msg: &Message, edit: impl FnOnce(&mut Message)) -> Message {
    let mut msg = msg.clone();
    edit(&mut msg);
    msg
}

// ---- the happy path ---------------------------------------------------------

#[test]
fn a_handoff_moves_the_same_ballot_to_another_node_without_a_second_phase_1() {
    let mut nodes = cluster_with_three_chosen();
    let ballot_before = nodes[0].ballot();
    assert_eq!(nodes[0].leadership_origin(), LeadershipOrigin::Elected);
    let frontier = nodes[0].proposer().next_slot();

    let receipt = nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    assert_eq!(receipt.ballot, ballot_before);
    assert_eq!(receipt.next_slot, frontier);
    // The abdication is synchronous: it already happened, before any I/O.
    assert_eq!(nodes[0].role(), NodeRole::Follower);

    let queue = drain(&mut nodes[0]);
    // No Phase 1 anywhere in the handoff: the successor never prepares.
    assert!(
        !queue
            .iter()
            .any(|(_, m)| matches!(m, Message::Prepare { .. })),
        "a cooperative handoff runs no Phase 1"
    );
    deliver_all(&mut nodes, queue);

    assert!(nodes[1].is_leader(), "the successor holds the authority");
    assert_eq!(
        nodes[1].ballot(),
        ballot_before,
        "the successor continues under the *same* ballot"
    );
    assert_eq!(
        nodes[1].leadership_origin(),
        LeadershipOrigin::Handoff { from: NodeId(0) }
    );
    assert_eq!(
        nodes[1].proposer().next_slot(),
        frontier,
        "the allocator frontier moves with the authority"
    );
    assert!(!nodes[0].is_leader(), "the predecessor stays demoted");
}

#[test]
fn a_successor_streams_fresh_proposals_under_the_inherited_ballot() {
    let mut nodes = cluster_with_three_chosen();
    let inherited = nodes[0].ballot();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    let ProposeResult::Accepted(slot) = nodes[1].propose(ClientId(9), ClientSeq(1), val(77)) else {
        panic!("the successor admits proposals");
    };
    let q = drain(&mut nodes[1]);
    // Every `Accept` the successor emits carries the *inherited* ballot.
    for (_, msg) in &q {
        if let Message::Accept { ballot, .. } = msg {
            assert_eq!(*ballot, inherited);
        }
    }
    deliver_all(&mut nodes, q);
    assert_eq!(chosen_at(&nodes[1], slot.0), Some(val(77)));
    assert_eq!(chosen_at(&nodes[0], slot.0), Some(val(77)));
}

#[test]
fn the_predecessor_redirects_clients_to_its_successor() {
    let mut nodes = cluster_with_three_chosen();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    assert_eq!(
        nodes[0].propose(ClientId(1), ClientSeq(9), val(1)),
        ProposeResult::NotLeader(Some(NodeId(1))),
        "the boundary is the relinquish call: no proposal is admitted after it"
    );
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    // And a follower that accepts under the inherited ballot names the node
    // actually exercising it, not the ballot's original owner.
    let _ = nodes[1].propose(ClientId(1), ClientSeq(9), val(1));
    let q = drain(&mut nodes[1]);
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[2].leader(), Some(NodeId(1)));
    assert_eq!(
        nodes[0].leader(),
        Some(NodeId(1)),
        "the predecessor never names itself leader under the authority it gave away"
    );
}

#[test]
fn an_accepted_but_unchosen_slot_survives_the_handoff() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    // Open a round and let *nobody* answer: the slot is in flight, unchosen.
    let ProposeResult::Accepted(stranded) = nodes[0].propose(ClientId(1), ClientSeq(1), val(42))
    else {
        panic!("leader admits the proposal");
    };
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, m| !matches!(m, Message::Accept { .. }));
    assert!(
        !nodes[0].replica.is_chosen(stranded),
        "the slot is accepted-but-unchosen at the leader"
    );

    let receipt = nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    assert_eq!(receipt.pending, 1, "the open round is transferred");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // The successor re-proposed it verbatim under the same ballot and it is now
    // chosen — no election, and no `Noop` paved over the client's command.
    assert_eq!(chosen_at(&nodes[1], stranded.0), Some(val(42)));
    assert_eq!(chosen_at(&nodes[2], stranded.0), Some(val(42)));
}

#[test]
fn an_installed_authority_is_never_handed_on_again() {
    // One hop only. A successor holds a ballot it did not mint, and a replayed
    // copy of the *original* payload could otherwise re-install that ballot at a
    // node that had already handed it on — while its own successor is still
    // exercising it. Refusing the second hop keeps uniqueness structural with no
    // durable relinquishment record; see `ColocatedNode::can_relinquish`.
    let mut nodes = cluster_with_three_chosen();
    let authority = nodes[0].ballot();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[1].is_leader());
    assert_eq!(nodes[1].ballot(), authority);
    assert!(
        !nodes[1].can_relinquish(),
        "an inherited authority is not handed on again"
    );
    assert!(nodes[1].relinquish_to(NodeId(2)).is_none());
    assert!(nodes[1].is_leader(), "a refused handoff changes nothing");

    // Handing leadership on *again* is still possible — it just costs the
    // ordinary election that mints a fresh ballot for the new holder to own.
    make_leader(&mut nodes, 2);
    assert!(nodes[2].can_relinquish());
    nodes[2].relinquish_to(NodeId(0)).expect("handoff admitted");
    let q = drain(&mut nodes[2]);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    assert_eq!(nodes[0].ballot(), nodes[2].ballot());
    assert!(nodes[0].ballot() > authority);
}

// ---- the safety rule: an authority is exercised by at most one node ----------

#[test]
fn a_relinquished_authority_is_never_exercised_again_by_its_owner() {
    let mut nodes = cluster_with_three_chosen();
    let authority = nodes[0].ballot();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");

    // Nothing the predecessor can be asked to do re-opens a Phase-2 round.
    assert!(
        nodes[0].relinquish_to(NodeId(2)).is_none(),
        "an authority is relinquished at most once"
    );
    assert!(matches!(
        nodes[0].propose(ClientId(1), ClientSeq(50), val(1)),
        ProposeResult::NotLeader(_)
    ));
    assert!(matches!(
        nodes[0].propose_control(Control::Noop),
        ProposeResult::NotLeader(_)
    ));
    nodes[0].resend_pending();
    nodes[0].advance_recovery();
    let after = drain(&mut nodes[0]);
    assert!(
        !after.iter().any(|(_, m)| matches!(
            m,
            Message::Accept { ballot, .. } if *ballot == authority
        )),
        "the predecessor emits no further Accept at the relinquished ballot"
    );
}

#[test]
fn a_restart_cannot_resurrect_a_relinquished_authority() {
    let mut nodes = cluster_with_three_chosen();
    let authority = nodes[0].ballot();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    // Reboot the predecessor from its own durable state — the exact "A crashes
    // and restarts believing it still owns B" scenario. Leadership is volatile,
    // so the reboot is itself the fence: a Follower whose only route back to
    // leadership campaigns at a strictly higher round.
    let rebooted = ColocatedNode::new(&TestStorage::from_node(&nodes[0]));
    assert_eq!(rebooted.role(), NodeRole::Follower);
    assert_eq!(rebooted.leadership_origin(), LeadershipOrigin::Elected);
    nodes[0] = rebooted;
    nodes[0].set_election_timeout(1);
    nodes[0].tick();
    assert!(
        nodes[0].ballot() > authority,
        "a restarted node can only ever campaign strictly above the ballot it gave away"
    );
}

#[test]
fn a_second_node_cannot_install_a_relinquish_addressed_elsewhere() {
    let mut nodes = cluster_with_three_chosen();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let queue = drain(&mut nodes[0]);
    let (_, msg) = take_relinquish(&queue);

    // A duplicate, a misroute, or a replay toward a *second* successor: the
    // intended target lives inside the payload, so the transport can never
    // fabricate a second holder.
    nodes[2].step(msg.clone());
    assert!(!nodes[2].is_leader(), "an unaddressed successor refuses");
    assert_eq!(nodes[2].handoff_counters().rejected_target, 1);

    // The addressed successor still installs it, and a *re-delivery* to it is a
    // no-op rather than an allocator rewind.
    nodes[1].step(msg.clone());
    assert!(nodes[1].is_leader());
    let frontier = nodes[1].proposer().next_slot();
    let _ = nodes[1].propose(ClientId(3), ClientSeq(1), val(5));
    nodes[1].step(msg);
    assert_eq!(nodes[1].handoff_counters().installed, 1, "installed once");
    assert!(
        nodes[1].proposer().next_slot() > frontier,
        "the allocator never rewinds"
    );
}

#[test]
fn a_stale_relinquish_is_refused_once_the_cluster_has_moved_on() {
    let mut nodes = cluster_with_three_chosen();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let queue = drain(&mut nodes[0]);
    let (_, stale) = take_relinquish(&queue);
    // The payload never arrives; an ordinary election recovers instead.
    make_leader(&mut nodes, 2);
    assert!(nodes[2].is_leader());

    // The delayed relinquishment finally lands at its target, which has by now
    // promised a strictly higher ballot. Resurrecting dead authority here would
    // put two leaders in the cluster.
    nodes[1].step(stale);
    assert!(!nodes[1].is_leader(), "a stale authority is refused");
    assert_eq!(nodes[1].handoff_counters().rejected_stale, 1);
}

#[test]
fn a_malformed_tail_is_refused_whole() {
    let mut nodes = cluster_with_three_chosen();
    // Strand one slot so the tail is non-empty and tamperable.
    let _ = nodes[0].propose(ClientId(1), ClientSeq(7), val(7));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, m| !matches!(m, Message::Accept { .. }));
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let queue = drain(&mut nodes[0]);
    let (_, good) = take_relinquish(&queue);

    // A tail that no longer tiles `[from_slot, next_slot)` leaves the successor
    // guessing at a slot nobody described — refuse rather than invent a `Noop`.
    let holed = tamper(&good, |m| {
        if let Message::Relinquish { pending, .. } = m {
            pending.clear();
        }
    });
    nodes[1].step(holed);
    assert!(!nodes[1].is_leader());
    assert_eq!(nodes[1].handoff_counters().rejected_shape, 1);

    // An authority minted by a node outside the configured membership is not an
    // authority at all — the same membership boundary `on_accept` draws.
    let forged = tamper(&good, |m| {
        if let Message::Relinquish { ballot: b, .. } = m {
            *b = ballot(9, 7);
        }
    });
    nodes[1].step(forged);
    assert!(!nodes[1].is_leader());
    assert_eq!(nodes[1].handoff_counters().rejected_target, 1);

    // The untampered payload still installs, so the guards above rejected the
    // damage and not the mechanism.
    nodes[1].step(good);
    assert!(nodes[1].is_leader());
}

#[test]
fn a_successor_that_crashes_after_installing_leaves_an_ordinary_election_behind() {
    let mut nodes = cluster_with_three_chosen();
    let authority = nodes[0].ballot();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[1].is_leader());

    // The successor dies with the authority. Leadership is volatile, so its
    // reboot is a Follower — nobody holds the ballot, and the ordinary election
    // is the only way back.
    nodes[1] = ColocatedNode::new(&TestStorage::from_node(&nodes[1]));
    assert_eq!(nodes[1].role(), NodeRole::Follower);
    nodes[1].set_election_timeout(NO_CHECK_QUORUM);
    assert!(nodes.iter().all(|n| !n.is_leader()));
    make_leader(&mut nodes, 2);
    assert!(nodes[2].ballot() > authority);
    let ProposeResult::Accepted(slot) = nodes[2].propose(ClientId(6), ClientSeq(1), val(6)) else {
        panic!("the elected leader admits proposals");
    };
    let q = drain(&mut nodes[2]);
    deliver_all(&mut nodes, q);
    assert_eq!(chosen_at(&nodes[2], slot.0), Some(val(6)));
}

#[test]
fn a_successor_that_observes_a_higher_ballot_steps_down_by_the_ordinary_rules() {
    let mut nodes = cluster_with_three_chosen();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[1].is_leader());

    // Nothing about an inherited authority exempts its holder from Paxos: a
    // competing Phase 1 deposes it exactly like an elected leadership.
    make_leader(&mut nodes, 2);
    assert!(nodes[2].is_leader());
    assert!(
        !nodes[1].is_leader(),
        "the inherited leadership was deposed"
    );
    assert_eq!(
        nodes[1].leadership_origin(),
        LeadershipOrigin::Elected,
        "a demoted node carries no leadership origin"
    );
}

// ---- eligibility ------------------------------------------------------------

#[test]
fn only_a_settled_leader_may_relinquish() {
    let mut nodes = cluster_with_three_chosen();
    assert!(nodes[0].can_relinquish());
    // A follower has nothing to give.
    assert!(!nodes[1].can_relinquish());
    assert!(nodes[1].relinquish_to(NodeId(0)).is_none());
    // Neither self nor a non-member is a valid successor.
    assert!(nodes[0].relinquish_to(NodeId(0)).is_none());
    assert!(nodes[0].relinquish_to(NodeId(7)).is_none());
    assert!(nodes[0].is_leader(), "a refused handoff changes nothing");

    // A singleton has nobody to hand to.
    let mut solo = node(0, &[0]);
    solo.set_election_timeout(1);
    solo.tick();
    assert!(solo.is_leader());
    assert!(!solo.can_relinquish());
}

#[test]
fn an_open_repair_or_recovery_blocks_the_handoff() {
    // A leader a higher ballot has already passed holds nothing worth
    // transferring: its own promise outranks the authority it would hand over.
    let mut nodes = cluster_with_three_chosen();
    assert!(nodes[0].can_relinquish());
    let superseding = ballot(nodes[0].ballot().round + 5, 2);
    nodes[0].step(Message::Commit {
        config_id: ConfigId::default(),
        from: NodeId(2),
        ballot: superseding,
        slot: nodes[0].proposer().next_slot(),
        command: ucmd(8, 1, 3),
    });
    assert!(nodes[0].is_leader(), "the still-Leader window is the point");
    assert!(
        !nodes[0].can_relinquish(),
        "a superseded leadership is not transferable"
    );
    assert!(nodes[0].relinquish_to(NodeId(1)).is_none());
    let _ = drain(&mut nodes[0]);

    // A locally faulty record is Phase-1-shaped work too: its value is repaired
    // from a promise quorum's reports, which a handoff never gathers.
    let mut with_rot = cluster_with_three_chosen();
    let mut disk = TestStorage::from_node(&with_rot[0]);
    disk.rot(Slot(1));
    with_rot[0] = ColocatedNode::new(&disk);
    with_rot[0].set_election_timeout(NO_CHECK_QUORUM);
    assert!(!with_rot[0].acceptor().faulty().is_empty());
    assert!(!with_rot[0].can_relinquish());
}

#[test]
fn a_leader_trailing_its_own_frontier_past_the_bound_is_refused() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    // Open more in-flight rounds than one payload may carry, answering none.
    for seq in 1..=(HANDOFF_BATCH as u64 + 1) {
        let _ = nodes[0].propose(ClientId(1), ClientSeq(seq), val(1));
        let q = drain(&mut nodes[0]);
        deliver_filtered(&mut nodes, q, |_, m| !matches!(m, Message::Accept { .. }));
    }
    assert!(
        !nodes[0].can_relinquish(),
        "the transferred tail stays bounded"
    );
}

#[test]
fn a_successor_that_needs_phase_1_repair_refuses_the_handoff() {
    // A node holding a faulty record heals only from a promise quorum's
    // reports, and an installed authority runs no Phase 1. Taking the
    // leadership would strand its own repair until a fence timeout resigned it.
    let mut nodes = cluster_with_three_chosen();
    let mut disk = TestStorage::from_node(&nodes[1]);
    disk.rot(Slot(1));
    nodes[1] = ColocatedNode::new(&disk);
    nodes[1].set_election_timeout(NO_CHECK_QUORUM);
    assert!(!nodes[1].acceptor().faulty().is_empty());

    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);

    assert!(!nodes[1].is_leader(), "an unfit successor refuses");
    assert_eq!(nodes[1].handoff_counters().rejected_unfit, 1);
    // The cost is one ordinary election — which is exactly the machinery the
    // faulty record needs to be repaired from.
    make_leader(&mut nodes, 1);
    assert!(nodes[1].is_leader());
}

// ---- the fallback: ordinary Phase 1 -----------------------------------------

#[test]
fn a_dropped_relinquish_costs_availability_and_an_election_heals_it() {
    let mut nodes = cluster_with_three_chosen();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let queue = drain(&mut nodes[0]);
    // The one message vanishes. Nobody leads: the predecessor stopped and the
    // successor never started — exactly the intended trade.
    deliver_filtered(&mut nodes, queue, |_, m| {
        !matches!(m, Message::Relinquish { .. })
    });
    assert!(nodes.iter().all(|n| !n.is_leader()), "no leader remains");

    make_leader(&mut nodes, 2);
    assert!(
        nodes[2].is_leader(),
        "ordinary Phase 1 recovers the cluster"
    );
    let ProposeResult::Accepted(slot) = nodes[2].propose(ClientId(4), ClientSeq(1), val(88)) else {
        panic!("the elected leader admits proposals");
    };
    let q = drain(&mut nodes[2]);
    deliver_all(&mut nodes, q);
    assert_eq!(chosen_at(&nodes[2], slot.0), Some(val(88)));
}

#[test]
fn an_uncovered_inherited_fence_resigns_back_to_an_ordinary_election() {
    let mut nodes = cluster_with_three_chosen();
    // Hand over a tail the successor cannot complete: one in-flight slot whose
    // `Accept` never reaches a quorum, so the successor's chosen prefix stays
    // below the inherited fence forever.
    let _ = nodes[0].propose(ClientId(1), ClientSeq(9), val(9));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, m| !matches!(m, Message::Accept { .. }));
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, m| !matches!(m, Message::Accept { .. }));
    assert!(nodes[1].is_leader());

    // Beats and their acks keep flowing (so `CheckQuorum` stays satisfied and
    // the fence deadline is the *only* thing that can demote this leader);
    // only the inherited slot's `Accept` keeps vanishing.
    let timeout = 4;
    nodes[1].set_election_timeout(timeout);
    for _ in 0..(timeout * HANDOFF_FENCE_ELECTIONS) {
        if !nodes[1].is_leader() {
            break;
        }
        nodes[1].tick();
        let q = drain(&mut nodes[1]);
        deliver_filtered(&mut nodes, q, |_, m| {
            !matches!(m, Message::Accept { .. } | Message::CatchUpResponse { .. })
        });
    }
    assert!(
        !nodes[1].is_leader(),
        "a handoff leader that cannot cover its inherited fence resigns"
    );
    assert_eq!(nodes[1].handoff_counters().fence_step_downs, 1);
}

#[test]
fn a_handoff_never_no_op_fills_a_slot_nobody_described() {
    // The successor ran no Phase 1, so it must re-propose only what it was
    // handed. Give it a payload whose range starts *below* its own chosen
    // prefix and confirm no `Noop` is proposed under the inherited ballot.
    let mut nodes = cluster_with_three_chosen();
    nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    let after = drain(&mut nodes[1]);
    assert!(
        !after.iter().any(|(_, m)| matches!(
            m,
            Message::Accept {
                command: Command::Control(Control::Noop),
                ..
            }
        )),
        "an installed authority fills no gaps of its own"
    );
    assert!(
        nodes[1]
            .proposer
            .recovery()
            .is_none_or(|r| r.policy() == RecoveryPolicy::Inherited),
        "handoff recovery never gap-fills"
    );
}

#[test]
fn the_transferred_tail_names_every_slot_below_the_frontier() {
    let mut nodes = cluster_with_three_chosen();
    // Two chosen-but-unapplied-elsewhere slots and one in-flight round.
    let _ = nodes[0].propose(ClientId(2), ClientSeq(1), val(1));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    let _ = nodes[0].propose(ClientId(2), ClientSeq(2), val(2));
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |_, m| !matches!(m, Message::Accept { .. }));

    let receipt = nodes[0].relinquish_to(NodeId(1)).expect("handoff admitted");
    let queue = drain(&mut nodes[0]);
    let (_, msg) = take_relinquish(&queue);
    let Message::Relinquish {
        from_slot,
        next_slot,
        decided,
        pending,
        ballot: authority,
        ..
    } = msg
    else {
        panic!("a Relinquish")
    };
    assert_eq!(
        receipt.decided + receipt.pending,
        decided.len() + pending.len()
    );
    let described: BTreeSet<u64> = decided.keys().chain(pending.keys()).map(|s| s.0).collect();
    for slot in from_slot.0..next_slot.0 {
        assert!(described.contains(&slot), "slot {slot} is described");
    }
    assert!(
        decided.values().all(|(b, _)| *b <= authority),
        "no transferred decision outranks the transferred authority"
    );
    assert!(
        pending.values().all(|c| matches!(c, Command::User(_))),
        "the open round carries its client command verbatim"
    );
    let _ = ucmd(0, 0, 0);
}
