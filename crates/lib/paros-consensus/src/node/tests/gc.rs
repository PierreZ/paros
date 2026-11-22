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
    shrunk_cluster_after(0)
}

/// [`shrunk_cluster`], with `proposed` writes chosen before the shrink.
fn shrunk_cluster_after(proposed: u64) -> ([ColocatedNode; 3], Vec<Matchmaker>) {
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
    for seq in 0..proposed {
        let _ = nodes[0].propose(entry(1, seq + 10, 1));
        let q = drain(&mut nodes[0]);
        deliver_all(&mut nodes, q);
    }
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

/// The campaign judges Region 1 by its election fence: the highest slot
/// proposed below the shrink's ballot (the two writes at slots 0 and 1).
#[test]
fn the_gc_campaign_carries_its_election_fence() {
    let (nodes, _) = shrunk_cluster_after(2);
    assert_eq!(nodes[0].gc_fence(), Some(Slot(1)));
    assert_eq!(nodes[1].gc_fence(), None, "a follower collects nothing");
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
    assert!(
        !again.is_empty(),
        "the leader asks the successor set for its GC acks"
    );
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

/// While the floor is not effective the leader still needs every
/// configuration of `H_b`; once it is, only the current one.
#[test]
fn configurations_in_force_drop_the_prior_ones_once_the_floor_is_effective() {
    let (mut nodes, mut mms) = shrunk_cluster();
    let (prior, current) = nodes[0].configs_in_force().expect("node 0 leads");
    assert_eq!(*current, cfg(&[0, 1]));
    assert_eq!(prior, vec![&cfg(&[0, 1, 2])]);
    let requests = gc_requests(&mut nodes[0]);
    let acks = answer(&mut mms, &requests);
    let _ = nodes[0].on_gc_ack(&acks[0]);
    assert!(matches!(
        nodes[0].on_gc_ack(&acks[1]),
        GcStep::Effective { .. }
    ));
    let (prior, current) = nodes[0].configs_in_force().expect("node 0 leads");
    assert_eq!(*current, cfg(&[0, 1]));
    assert!(prior.is_empty(), "an effective floor forgot {prior:?}");
}

/// A refusal teaches only a successor later than the generation it
/// answered; one at that same generation teaches nothing.
#[test]
fn a_refusal_naming_its_own_generation_teaches_nothing() {
    let (mut nodes, mut mms) = shrunk_cluster();
    let requests = gc_requests(&mut nodes[0]);
    let good = answer(&mut mms, &to_zero(&requests)).remove(0);
    let set = crate::membership::MatchmakerSet::new(
        MatchmakerGeneration(1),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    );
    let odd = GcAck {
        applied: false,
        generation: MatchmakerGeneration(1),
        newer: Some(set),
        ..good
    };
    assert_eq!(nodes[0].on_gc_ack(&odd), GcStep::Ignored);
    assert_eq!(
        nodes[0].matchmaker_set().map(|m| m.generation),
        Some(MatchmakerGeneration(0)),
        "the leader still believes generation 0"
    );
}

/// After a refusal taught the successor, the tally starts over: a quorum
/// of the new generation's acks makes the floor effective.
#[test]
fn the_successor_s_acks_make_the_floor_effective() {
    let (mut nodes, mut mms) = shrunk_cluster();
    let requests = gc_requests(&mut nodes[0]);
    let first = answer(&mut mms, &requests[1..2]).remove(0);
    assert_eq!(nodes[0].on_gc_ack(&first), GcStep::Acked { remaining: 1 });
    let successor = crate::membership::MatchmakerSet::new(
        MatchmakerGeneration(1),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    );
    stop(&mut mms[0], Some(&successor));
    let refusal = answer(&mut mms, &to_zero(&requests)).remove(0);
    assert_eq!(nodes[0].on_gc_ack(&refusal), GcStep::Ignored);
    let ballot = nodes[0].ballot();
    let ack = |m: u64| GcAck {
        matchmaker: MatchmakerId(m),
        journal: J,
        generation: MatchmakerGeneration(1),
        applied: true,
        watermark: ballot,
        newer: None,
        stopped: false,
    };
    assert_eq!(
        nodes[0].on_gc_ack(&ack(1)),
        GcStep::Acked { remaining: 1 },
        "the ack of generation 0 does not count at generation 1"
    );
    assert!(matches!(
        nodes[0].on_gc_ack(&ack(2)),
        GcStep::Effective { .. }
    ));
}

/// A beat's floor sits at or below its ballot: one below is kept, and one
/// above the ballot is ignored.
#[test]
fn a_beat_s_floor_above_its_ballot_is_ignored() {
    let mut n = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3], 1);
    let beat = |floor: Ballot| Message::Heartbeat {
        from: NodeId(1),
        ballot: ballot(9, 1),
        commit: None,
        config: Some(cfg(&[1, 2])),
        fence: None,
        gc_floor: Some(floor),
    };
    n.step(beat(ballot(12, 1)));
    n.ready().advance();
    assert_eq!(n.gc_floor_heard(), None);
    n.step(beat(ballot(7, 1)));
    n.ready().advance();
    assert_eq!(n.gc_floor_heard(), Some(ballot(7, 1)));
}

/// Leg 4: a registration at the watermark itself names the node, so a floor
/// at that watermark does not retire it, checked or not.
#[test]
fn a_registration_at_the_watermark_keeps_the_node() {
    let mut n = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3], 1);
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
    assert!(n.retirement_unchecked(w));
    let _ = check_registrations(&mut n, w, Some(w));
    assert_eq!(n.last_member_ballot(), w);
    assert!(!n.retirement_unchecked(w));
    assert!(!n.may_retire(w));
}

/// A retirement check needs every matchmaker, its probe only a quorum: once
/// the probe closed, the check is still open, a second call opens no new
/// probe, and the node still reports a check in flight.
#[test]
fn a_check_outlives_its_probe() {
    let mut n = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3], 3);
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
    assert!(n.check_retirement(w));
    let tag = n
        .membership_probe()
        .map(crate::matchmaking::MembershipProbe::ballot)
        .expect("a probe");
    let _ = drain_match_requests(&mut n);
    for m in [0, 1] {
        let _ = n.on_match_reply(MatchReply {
            journal: J,
            matchmaker: MatchmakerId(m),
            to: NodeId(3),
            ballot: tag,
            generation: MatchmakerGeneration(0),
            outcome: MatchOutcome::Probed {
                effective: None,
                named: None,
            },
        });
    }
    n.ready().advance();
    assert!(
        n.membership_probe().is_none(),
        "the probe closed at a quorum"
    );
    assert!(!n.may_retire(w), "matchmaker 2 has not agreed");
    assert!(n.check_retirement(w), "the check is still in flight");
    assert!(n.membership_probe().is_none(), "no second probe");
    assert_eq!(drain_match_requests(&mut n), Vec::new());
    // Under a followed leader, the check asks the missing matchmaker again
    // once an election timeout passed.
    n.set_election_timeout(3);
    let mut asked = Vec::new();
    for _ in 0..3 {
        n.tick();
        n.step(Message::Heartbeat {
            from: NodeId(1),
            ballot: w,
            commit: None,
            config: Some(cfg(&[1, 2])),
            fence: None,
            gc_floor: Some(w),
        });
        asked.extend(drain_match_requests(&mut n).into_iter().map(|(m, _)| m));
    }
    assert_eq!(asked, vec![MatchmakerId(2)], "only the missing one");
}

/// A check whose last agreement lands after the node heard a later
/// belief does not let the node retire on the old floor: leg 5 judges the
/// belief at the close.
#[test]
fn a_check_that_closes_under_a_later_belief_lets_nothing_retire() {
    let mut n = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3], 3);
    let w = ballot(9, 1);
    let beat = |b: Ballot| Message::Heartbeat {
        from: NodeId(1),
        ballot: b,
        commit: None,
        config: Some(cfg(&[1, 2])),
        fence: None,
        gc_floor: Some(w),
    };
    n.step(beat(w));
    n.ready().advance();
    assert!(n.check_retirement(w));
    let tag = n
        .membership_probe()
        .map(crate::matchmaking::MembershipProbe::ballot)
        .expect("a probe");
    let _ = drain_match_requests(&mut n);
    let agree = |m: u64| MatchReply {
        journal: J,
        matchmaker: MatchmakerId(m),
        to: NodeId(3),
        ballot: tag,
        generation: MatchmakerGeneration(0),
        outcome: MatchOutcome::Probed {
            effective: None,
            named: None,
        },
    };
    for m in [0, 1] {
        let _ = n.on_match_reply(agree(m));
    }
    n.ready().advance();
    n.step(beat(ballot(11, 1)));
    n.ready().advance();
    let _ = n.on_match_reply(agree(2));
    n.ready().advance();
    assert!(
        !n.retire_mark().checked,
        "no checked mark under a later belief"
    );
    assert!(!n.may_retire(w));
}
