//! Matchmaker garbage collection (#123), end to end at the leader: the
//! request goes out once the forgettability condition holds, the real
//! matchmakers answer it, and the floor becomes effective only at a
//! matchmaker quorum, naming the acceptors the shrink removed.

use super::*;
use crate::collector::GcStep;
use crate::matchmaker::{GcAck, GcRequest};

/// Three nodes and three matchmakers; node 0 leads, then shrinks the
/// configuration from `{0, 1, 2}` to `{0, 1}` and decides one write under
/// it, so its GC campaign is covered and node 2 is retirable.
fn shrunk_cluster() -> ([ColocatedNode; 3], Vec<Matchmaker>) {
    let mut nodes = [0, 1, 2].map(|i| deployed_node(i, &[0, 1, 2], &[0, 1, 2], 3));
    let mut mms = registries(3);
    campaign(&mut nodes[0]);
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(&mut mms, requests) {
        nodes[0].on_match_reply(reply);
    }
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    nodes[0].set_election_timeout(NO_CHECK_QUORUM);
    assert!(matches!(
        nodes[0].reconfigure(&cfg(&[0, 1])),
        ReconfigureResult::Started(_)
    ));
    let requests = drain_match_requests(&mut nodes[0]);
    for reply in matchmake(&mut mms, requests) {
        nodes[0].on_match_reply(reply);
    }
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    assert_eq!(*nodes[0].acceptors(), cfg(&[0, 1]));
    let _ = nodes[0].propose(entry(1, 1, 1));
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    // One beat carries the leader's chosen index; the acks report each
    // follower's, which is what covers the campaign.
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    (nodes, mms)
}

/// The GC requests the leader has out now (a delivery above may have drained
/// earlier copies: the driver's re-send cadence queues them again).
fn gc_requests(n: &mut ColocatedNode) -> Vec<(MatchmakerId, GcRequest)> {
    n.resend_gc();
    drain_with(n, |ready| ready.gc_requests().to_vec()).1
}

/// Each addressed matchmaker answers its request as a driver does.
fn answer(mms: &mut [Matchmaker], requests: &[(MatchmakerId, GcRequest)]) -> Vec<GcAck> {
    requests
        .iter()
        .map(|(id, request)| {
            let mm = &mut mms[usize::try_from(id.0).expect("matchmaker index")];
            let outcome =
                mm.advance_gc_watermark(request.journal, request.generation, request.watermark);
            mm.ready().advance();
            mm.gc_ack(request, outcome)
        })
        .collect()
}

/// The floor is the leader's ballot, every matchmaker of the set is asked,
/// one ack is not enough, and the second makes the floor effective and
/// retires the removed acceptor. Later acks change nothing.
#[test]
fn a_matchmaker_quorum_makes_the_floor_effective_and_retires_the_removed_acceptor() {
    let (mut nodes, mut mms) = shrunk_cluster();
    assert!(
        nodes[0].gc_pending(),
        "a covered campaign has a request out"
    );
    let requests = gc_requests(&mut nodes[0]);
    let mut asked: Vec<MatchmakerId> = requests.iter().map(|(m, _)| *m).collect();
    asked.sort_unstable();
    assert_eq!(
        asked,
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)]
    );
    let ballot = nodes[0].ballot();
    assert!(requests.iter().all(|(_, r)| r.watermark == ballot));

    let acks = answer(&mut mms, &requests);
    assert_eq!(nodes[0].on_gc_ack(&acks[0]), GcStep::Acked { remaining: 1 });
    assert_eq!(
        nodes[0].on_gc_ack(&acks[0]),
        GcStep::Ignored,
        "a duplicate ack never counts twice"
    );
    assert!(nodes[0].gc_pending());
    assert_eq!(
        nodes[0].on_gc_ack(&acks[1]),
        GcStep::Effective {
            watermark: ballot,
            retired: vec![NodeId(2)],
        }
    );
    assert!(
        !nodes[0].gc_pending(),
        "an effective floor needs no request"
    );
    assert_eq!(nodes[0].on_gc_ack(&acks[2]), GcStep::Ignored);
    assert!(
        gc_requests(&mut nodes[0]).is_empty(),
        "nothing is re-sent once effective"
    );
}

/// An ack the open campaign did not ask for is ignored whole: one from a
/// matchmaker outside the set, one for another journal, one at a lower
/// floor, and any ack at a follower.
#[test]
fn an_ack_the_campaign_did_not_ask_for_is_ignored() {
    let (mut nodes, mut mms) = shrunk_cluster();
    let requests = gc_requests(&mut nodes[0]);
    let good = answer(&mut mms, &requests[..1]).remove(0);
    let stranger = GcAck {
        matchmaker: MatchmakerId(9),
        ..good.clone()
    };
    let other_journal = GcAck {
        journal: crate::journal::JournalId(J.0 + 1),
        ..good.clone()
    };
    let lower = GcAck {
        watermark: Ballot::zero(),
        ..good.clone()
    };
    for ack in [&stranger, &other_journal, &lower] {
        assert_eq!(nodes[0].on_gc_ack(ack), GcStep::Ignored, "{ack:?}");
    }
    assert_eq!(
        nodes[1].on_gc_ack(&good),
        GcStep::Ignored,
        "a follower collects nothing"
    );
    assert_eq!(
        nodes[0].on_gc_ack(&good),
        GcStep::Acked { remaining: 1 },
        "the ignored acks counted for nothing"
    );
}

/// The request addressed to matchmaker 0.
fn to_zero(requests: &[(MatchmakerId, GcRequest)]) -> Vec<(MatchmakerId, GcRequest)> {
    requests
        .iter()
        .filter(|(m, _)| *m == MatchmakerId(0))
        .copied()
        .collect()
}

/// Stop generation 0 at matchmaker `mm`, and tell it the successor if any.
fn stop(mm: &mut Matchmaker, successor: Option<&crate::membership::MatchmakerSet>) {
    mm.step_reconfigure(crate::matchmaker::ReconfigureRequest::Stop {
        from: NodeId(5),
        generation: MatchmakerGeneration(0),
        after: None,
    });
    mm.ready().advance();
    if let Some(successor) = successor {
        mm.step_reconfigure(crate::matchmaker::ReconfigureRequest::Chosen {
            from: NodeId(5),
            generation: MatchmakerGeneration(0),
            successor: successor.clone(),
        });
        mm.ready().advance();
    }
}

/// A stable leader learns a handover only from a GC refusal (#475): the
/// refusal names the successor, the leader believes it, and its next
/// requests address the new set.
#[test]
fn a_refused_ack_teaches_the_successor_set() {
    let (mut nodes, mut mms) = shrunk_cluster();
    let requests = gc_requests(&mut nodes[0]);
    let successor = crate::membership::MatchmakerSet::new(
        MatchmakerGeneration(1),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    );
    stop(&mut mms[0], Some(&successor));
    let refusal = answer(&mut mms, &to_zero(&requests)).remove(0);
    assert!(!refusal.applied);
    assert_eq!(nodes[0].on_gc_ack(&refusal), GcStep::Ignored);
    assert_eq!(nodes[0].matchmaker_set(), Some(&successor));
    let again = gc_requests(&mut nodes[0]);
    assert!(!again.is_empty());
    assert!(
        again
            .iter()
            .all(|(_, r)| r.generation == MatchmakerGeneration(1)),
        "the tally starts over at the new generation"
    );
}

/// A generation frozen with no successor refuses for ever: the leader
/// reports it so the driver can finish the handover.
#[test]
fn a_frozen_generation_is_reported_to_the_driver() {
    let (mut nodes, mut mms) = shrunk_cluster();
    let requests = gc_requests(&mut nodes[0]);
    stop(&mut mms[0], None);
    let refusal = answer(&mut mms, &to_zero(&requests)).remove(0);
    assert!(refusal.stopped);
    assert_eq!(
        nodes[0].on_gc_ack(&refusal),
        GcStep::Frozen {
            generation: MatchmakerGeneration(0)
        }
    );
    assert!(nodes[0].gc_pending(), "the floor still waits");
}

/// The floor a leadership made effective rides its beats, and a follower
/// keeps the highest one a beat it *followed* carried: a beat below its
/// promise teaches nothing, a plain deployment keeps none, and the floor is
/// what `may_retire_heard` judges a released slot's retirement on.
#[test]
fn a_followed_beat_s_floor_is_kept_and_a_refused_one_is_not() {
    // Node 3, a spare of the bootstrap `{0, 1, 2}`.
    let mut n = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3], 1);
    assert!(n.gc_floor_heard().is_none());
    assert!(!n.may_retire_heard());
    let w = ballot(9, 1);
    n.step(Message::Heartbeat {
        from: NodeId(1),
        ballot: w,
        commit: None,
        config: Some(cfg(&[1, 2])),
        fence: None,
        gc_floor: Some(w),
    });
    n.ready().advance();
    assert_eq!(n.gc_floor_heard(), Some(w));
    assert!(!n.is_acceptor());
    // The matchmakers have not been asked which registrations name it.
    assert!(!n.may_retire(w));
    assert!(n.retirement_unchecked(w));
    let _ = check_registrations(&mut n, w, None);
    assert!(n.may_retire(w));
    assert!(n.may_retire_heard());
    // A beat below the promise is refused whole: its floor is not heard.
    let old = ballot(8, 1);
    n.step(Message::Heartbeat {
        from: NodeId(1),
        ballot: old,
        commit: None,
        config: Some(cfg(&[1, 2, 3])),
        fence: None,
        gc_floor: Some(old),
    });
    n.ready().advance();
    assert_eq!(n.gc_floor_heard(), Some(w), "a heard floor never falls");
    assert!(!n.is_acceptor());
    // A later leadership that names this node again: its floor is heard,
    // and the node is a member, so it does not retire.
    let newer = ballot(10, 1);
    n.step(Message::Heartbeat {
        from: NodeId(1),
        ballot: newer,
        commit: None,
        config: Some(cfg(&[1, 2, 3])),
        fence: None,
        gc_floor: Some(newer),
    });
    n.ready().advance();
    assert_eq!(n.gc_floor_heard(), Some(newer));
    assert!(n.is_acceptor());
    assert!(!n.may_retire_heard());
    // Plain Multi-Paxos hears no floor.
    let mut plain = node(1, &[0, 1, 2]);
    plain.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: ballot(2, 0),
        commit: None,
        config: None,
        fence: None,
        gc_floor: Some(ballot(2, 0)),
    });
    plain.ready().advance();
    assert!(plain.gc_floor_heard().is_none());
    assert!(!plain.may_retire_heard());
}
