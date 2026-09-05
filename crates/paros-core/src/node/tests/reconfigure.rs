//! Online reconfiguration (#122), pinned at the mechanism: the refusals, the
//! round change, the cross-configuration Phase 1 that follows, Phase 2 under
//! the new configuration only, and the removed leader's resignation.

use super::*;

fn accept_targets(msgs: &[(NodeId, Message)]) -> Vec<NodeId> {
    let mut targets: Vec<NodeId> = msgs
        .iter()
        .filter(|(_, m)| matches!(m, Message::Accept { .. }))
        .map(|(to, _)| *to)
        .collect();
    targets.sort_unstable();
    targets.dedup();
    targets
}

/// A three-node matchmaker deployment over a five-node pool with one
/// registry, node 0 elected leader through a real matchmaking round.
fn deployed_cluster() -> ([ColocatedNode; 5], Vec<Matchmaker>) {
    let pool = [0, 1, 2, 3, 4];
    let mut nodes = [
        deployed_node(0, &[0, 1, 2], &pool, 1),
        deployed_node(1, &[0, 1, 2], &pool, 1),
        deployed_node(2, &[0, 1, 2], &pool, 1),
        deployed_node(3, &[0, 1, 2], &pool, 1),
        deployed_node(4, &[0, 1, 2], &pool, 1),
    ];
    let mut mms = registries(1);
    nodes[0].set_election_timeout(1);
    nodes[0].tick();
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
    (nodes, mms)
}

/// Plain Multi-Paxos refuses every reconfiguration; a matchmaker deployment
/// refuses the no-op, the unknown member, and the non-leader.
#[test]
fn reconfiguration_is_refused_where_it_cannot_run() {
    let mut nodes = [
        node(0, &[0, 1, 2]),
        node(1, &[0, 1, 2]),
        node(2, &[0, 1, 2]),
    ];
    make_leader(&mut nodes, 0);
    assert_eq!(
        nodes[0].reconfigure(&cfg(&[0, 1])),
        ReconfigureResult::Refused(ReconfigureRefusal::NoMatchmakers)
    );
    assert!(nodes[0].is_leader(), "a refusal changes nothing");

    let (mut nodes, _) = deployed_cluster();
    assert_eq!(
        nodes[1].reconfigure(&cfg(&[0, 1])),
        ReconfigureResult::NotLeader(Some(NodeId(0)))
    );
    assert_eq!(
        nodes[0].reconfigure(&cfg(&[0, 1, 2])),
        ReconfigureResult::Refused(ReconfigureRefusal::Unchanged)
    );
    assert_eq!(
        nodes[0].reconfigure(&cfg(&[0, 1, 9])),
        ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember)
    );
    assert!(nodes[0].is_leader());
}

/// The full flow: the leader registers `(b', C_new)`, its Phase 1 covers
/// `C_old` (which is in `H_b'`), and it leads at `b'` under `C_new` — with
/// accepts fanning out to `C_new` only, and a joining node voting.
#[test]
fn a_reconfiguration_moves_the_leadership_to_a_fresh_ballot_and_configuration() {
    let (mut nodes, mut mms) = deployed_cluster();
    let old_ballot = nodes[0].ballot();
    let _ = nodes[0].propose(ClientId(1), ClientSeq(1), val(1));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[0].hard_state().chosen_index, Some(Slot(0)));

    // Grow: {0,1,2} -> {0,1,2,3,4}.
    let new = cfg(&[0, 1, 2, 3, 4]);
    let started = nodes[0].reconfigure(&new);
    let ReconfigureResult::Started(b) = started else {
        panic!("{started:?}");
    };
    assert!(b > old_ballot);
    assert_eq!(nodes[0].role(), NodeRole::Candidate);
    assert!(nodes[0].matchmaking_pending());
    // Command issuance stalls: the stall window of #122.
    assert!(matches!(
        nodes[0].propose(ClientId(1), ClientSeq(2), val(2)),
        ProposeResult::NotLeader(None)
    ));
    let requests = drain_match_requests(&mut nodes[0]);
    let replies = matchmake(&mut mms, requests);
    let step = nodes[0].on_match_reply(replies[0].clone());
    let MatchStep::Completed { prior, .. } = step else {
        panic!("{step:?}");
    };
    assert_eq!(
        prior,
        vec![cfg(&[0, 1, 2])],
        "H_b' is the old configuration"
    );
    // Phase 1 fans out to C_old ∪ C_new; the old members' promises complete
    // the only prior configuration.
    let q = drain(&mut nodes[0]);
    deliver_filtered(&mut nodes, q, |to, m| {
        !(matches!(m, Message::Prepare { .. }) && (to == NodeId(3) || to == NodeId(4)))
    });
    assert!(
        nodes[0].is_leader(),
        "C_old's quorum alone completes Phase 1"
    );
    assert_eq!(*nodes[0].acceptors(), new);
    assert_eq!(nodes[0].acceptors_since(), b);
    // Phase 2 under C_new: the accept reaches every new member, including
    // the joining nodes, which vote from the first round.
    let _ = nodes[0].propose(ClientId(1), ClientSeq(2), val(2));
    let q = drain(&mut nodes[0]);
    assert_eq!(
        accept_targets(&q),
        vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
    );
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[0].hard_state().chosen_index, Some(Slot(1)));
    assert!(
        nodes[3].acceptor().records().contains_key(&Slot(1)),
        "a joining node accepted"
    );
    // A leader mid-change is refused a second change; once done, the no-op
    // is refused as unchanged.
    assert_eq!(
        nodes[0].reconfigure(&new),
        ReconfigureResult::Refused(ReconfigureRefusal::Unchanged)
    );
}

/// Shrink away from the leader: it drives the change, casts no vote of its
/// own under `C_new`, contacts only `C_new` for accepts, and resigns once its
/// inherited work is decided — then only members campaign.
#[test]
fn a_leader_removed_by_its_own_reconfiguration_resigns_once_settled() {
    let (mut nodes, mut mms) = deployed_cluster();
    let new = cfg(&[1, 2, 3]);
    assert!(matches!(
        nodes[0].reconfigure(&new),
        ReconfigureResult::Started(_)
    ));
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(&mut mms, requests) {
        nodes[0].on_match_reply(reply);
    }
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    assert!(!nodes[0].is_acceptor());
    let _ = nodes[0].propose(ClientId(1), ClientSeq(1), val(1));
    let q = drain(&mut nodes[0]);
    assert_eq!(accept_targets(&q), vec![NodeId(1), NodeId(2), NodeId(3)]);
    // The removed leader recorded nothing of its own for the slot: it is a
    // proposer and learner, not an acceptor.
    assert!(!nodes[0].acceptor().records().contains_key(&Slot(0)));
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[0].hard_state().chosen_index, Some(Slot(0)));
    // Settled: the next tick resigns.
    nodes[0].tick();
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert_eq!(nodes[0].membership_counters().1, 1);
    // ...and it never campaigns again as a non-member, while a member does.
    nodes[0].set_election_timeout(1);
    nodes[0].tick();
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert_eq!(nodes[0].membership_counters().0, 1);
    assert_eq!(
        *nodes[1].acceptors(),
        new,
        "members learned C_new from the Prepare"
    );
    nodes[1].set_election_timeout(1);
    nodes[1].tick();
    assert_eq!(nodes[1].role(), NodeRole::Candidate);
    assert!(
        drain_match_requests(&mut nodes[1])
            .iter()
            .all(|(_, r)| r.config == new)
    );
}

/// A reconfiguration is refused while Phase-1-shaped work is open.
#[test]
fn a_reconfiguration_waits_for_a_settled_leadership() {
    let (mut nodes, mut mms) = deployed_cluster();
    // Reconfigure once, and ask again mid-matchmaking: not a leader now.
    assert!(matches!(
        nodes[0].reconfigure(&cfg(&[0, 1, 2, 3])),
        ReconfigureResult::Started(_)
    ));
    assert_eq!(
        nodes[0].reconfigure(&cfg(&[0, 1, 3])),
        ReconfigureResult::NotLeader(None)
    );
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(&mut mms, requests) {
        nodes[0].on_match_reply(reply);
    }
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    assert_eq!(*nodes[0].acceptors(), cfg(&[0, 1, 2, 3]));
}

/// A configuration `new` produced is always well formed: `AcceptorConfig`'s
/// fields are private and `new` — which asserts it — is the only constructor,
/// so a malformed one cannot be *built* and `reconfigure` needs no refusal
/// for it.
#[test]
fn a_constructed_configuration_is_well_formed() {
    assert!(cfg(&[0, 1, 3]).is_well_formed());
    assert!(cfg(&[0, 1]).is_well_formed());
    assert!(cfg(&[2, 0, 1, 1]).is_well_formed());
}
