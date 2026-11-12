//! The retire fence (#1002 (retire fence gaps)): a node retires only when no
//! registration above the floor names it. #893 (retire fence) fenced the
//! registrant's own campaign, in memory. Two paths stayed open: a removed
//! member that a stale belief of another node names (gap A), and the
//! registrant after a restart (gap B). Either one leaves a registration
//! above the floor whose Phase-1 quorum needs a node that is gone, and no
//! later campaign wins.

use super::*;
use crate::contract::write::WriteOp;
use crate::membership::QuorumSystem;

/// `members` under a flexible system with `q1 = n` and `q2 = 1`: every
/// Phase 1 needs every member.
fn all_of(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().copied().map(NodeId).collect(),
        QuorumSystem::Flexible {
            q1: members.len(),
            q2: 1,
        },
    )
}

/// Persist `n`'s ready batch into `storage`, as a driver does, and return
/// the matchmaking requests it sends.
fn persist(n: &mut ColocatedNode, storage: &mut MemStorage) -> Vec<(MatchmakerId, MatchRequest)> {
    let ready = n.ready();
    storage.apply(ready.writes());
    let requests = ready.match_requests().to_vec();
    ready.advance();
    requests
}

/// Register `config` at `ballot` from `from` at every matchmaker of `mms`.
fn register(mms: &mut [Matchmaker], request: &MatchRequest) {
    for mm in mms.iter_mut() {
        mm.step(request.clone());
        mm.ready().advance();
    }
}

/// Gap A. Configuration `{0,1,2,3}` with `q1 = n`. A reconfiguration to
/// `{0,1,2}` wins at 12 and its floor is effective. Node 2 missed it, and
/// its campaign registers the stale belief `(13, {0,1,2,3})`. Node 3 did
/// not campaign: its fence is its last membership, below 12. If it retired
/// on the floor 12, every later campaign would need its promise and no
/// leader would win again. The node must not retire on the floor alone.
#[test]
fn a_stale_belief_of_another_node_holds_the_removed_member() {
    let mut mms = registries(3);
    let mut n3 = deployed_node(3, &[0, 1, 2, 3], &[0, 1, 2, 3], 3);
    let generation = n3
        .matchmaker_set()
        .expect("a matchmaker deployment")
        .generation;
    let old = all_of(&[0, 1, 2, 3]);
    let new = all_of(&[0, 1, 2]);
    // Node 0 led at 3 under `{0,1,2,3}`: node 3 was a member at 3.
    let member_at = ballot(3, 0);
    register(
        &mut mms,
        &MatchRequest::reconfigure(J, NodeId(0), member_at, old.clone(), generation),
    );
    n3.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: member_at,
        commit: None,
        config: Some(old.clone()),
        fence: None,
        gc_floor: None,
    });
    n3.ready().advance();
    assert!(n3.is_acceptor());
    assert_eq!(n3.last_member_ballot(), member_at);
    // The reconfiguration to `{0,1,2}` wins at 12 and its floor is
    // effective at every matchmaker.
    let w = ballot(12, 0);
    register(
        &mut mms,
        &MatchRequest::reconfigure(J, NodeId(0), w, new.clone(), generation),
    );
    for mm in &mut mms {
        let _ = mm.advance_gc_watermark(J, generation, w);
        mm.ready().advance();
    }
    // Node 2 missed it: its campaign registers the stale belief at 13. The
    // matchmakers answer with the reconfiguration, node 2 stops, and the
    // registration stays.
    let stale = ballot(13, 2);
    register(
        &mut mms,
        &MatchRequest::new(J, NodeId(2), stale, old.clone(), generation),
    );
    // Node 3 hears the leader at 12: it is outside, its belief is bound to
    // the floor, and the beat carries the floor.
    n3.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: w,
        commit: None,
        config: Some(new.clone()),
        fence: None,
        gc_floor: Some(w),
    });
    n3.ready().advance();
    assert!(!n3.is_acceptor());
    assert_eq!(n3.acceptors_since(), w);
    assert_eq!(n3.gc_floor_heard(), Some(w));
    // Why it matters: the next campaign's `H_b` holds the stale belief,
    // and `{0,1,2}` is no Phase-1 quorum of it.
    let next = MatchRequest::new(J, NodeId(0), ballot(14, 0), new.clone(), generation);
    let mut prior = Vec::new();
    for mm in &mut mms {
        mm.step(next.clone());
        let ready = mm.ready();
        for reply in ready.replies() {
            if let MatchOutcome::Registered { history, .. } = &reply.outcome {
                prior.extend(history.values().map(|r| r.config.clone()));
            }
        }
        ready.advance();
    }
    assert!(prior.contains(&old), "the stale belief is in `H_b`");
    let survivors = [0, 1, 2].map(NodeId).into_iter().collect();
    assert!(!old.has_phase1_quorum(&survivors));
    // So node 3 may not retire on the floor 12, by `Retire` or on a beat.
    assert!(
        !n3.may_retire(w),
        "a registration above the floor names this node"
    );
    assert!(!n3.may_retire_heard());
    // The check asks the matchmakers. Each one names the stale belief at
    // 13, and the fence rises to it, durably.
    assert!(n3.retirement_unchecked(w));
    assert!(n3.check_retirement_heard(), "the check opens a probe");
    let requests = drain_match_requests(&mut n3);
    assert!(requests.iter().all(|(_, r)| r.purpose.is_probe()));
    let mut writes = Vec::new();
    for reply in matchmake(&mut mms, requests) {
        let _ = n3.on_match_reply(reply);
        let ready = n3.ready();
        writes.extend(ready.writes().to_vec());
        ready.advance();
    }
    assert!(n3.membership_probe().is_none(), "a quorum closed the probe");
    assert!(!n3.is_acceptor());
    assert_eq!(n3.registration_fence(), stale);
    assert!(writes.contains(&WriteOp::RegistrationFence(stale)));
    assert!(!n3.retirement_unchecked(w), "the check is done");
    assert!(!n3.may_retire(w));
    assert!(!n3.may_retire_heard());
    // A later leader (node 3 still answers, so it can win) makes its floor
    // effective above the stale belief: the matchmakers collect it.
    let later = ballot(15, 0);
    for mm in &mut mms {
        let _ = mm.advance_gc_watermark(J, generation, later);
        mm.ready().advance();
    }
    n3.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: later,
        commit: None,
        config: Some(new),
        fence: None,
        gc_floor: Some(later),
    });
    n3.ready().advance();
    assert!(!n3.may_retire_heard(), "a new belief is checked again");
    assert!(n3.check_retirement_heard());
    let requests = drain_match_requests(&mut n3);
    for reply in matchmake(&mut mms, requests) {
        let _ = n3.on_match_reply(reply);
    }
    assert_eq!(n3.registration_fence(), stale, "nothing names it any more");
    assert!(n3.may_retire(later));
    assert!(n3.may_retire_heard());
}

/// Gap B. Node 1 registers its belief at 13, which names it, and meets the
/// reconfiguration at 12 that left it out. Its fence is 13. It restarts,
/// and its membership probe adopts the configuration at 12. The fence must
/// still be 13: the registration is still in the matchmakers' histories.
#[test]
fn a_restarted_registrant_keeps_the_fence_of_its_registration() {
    let mut storage = MemStorage::for_node(1, &[0, 1, 2]);
    storage.config.nodes = [0, 1, 2].map(NodeId).to_vec();
    storage.config.matchmakers = vec![MatchmakerId(0)];
    let mut n = ColocatedNode::new(&storage, Limits::DEFAULT);
    // A leader at 3 taught node 1 `{0, 1, 2}`.
    let learned = ballot(3, 0);
    n.step(Message::Prepare {
        reply_to: NodeId(0),
        ballot: learned,
        from_slot: Slot(0),
        config: Some(cfg(&[0, 1, 2])),
    });
    let _ = persist(&mut n, &mut storage);
    // Its campaign registers that belief: the fence follows.
    campaign(&mut n);
    let b = n.ballot();
    let _ = persist(&mut n, &mut storage);
    assert_eq!(n.last_member_ballot(), b);
    // The reconfiguration to `{0}` at `w`, below `b`: the campaign stops.
    let w = ballot(4, 0);
    assert!(w < b);
    let effective = Some((w, cfg(&[0])));
    let step = n.on_match_reply(MatchReply {
        journal: J,
        matchmaker: MatchmakerId(0),
        to: NodeId(1),
        ballot: b,
        generation: n.matchmaker_set().expect("a set").generation,
        outcome: MatchOutcome::Registered {
            from_ballot: Ballot::zero(),
            history: BTreeMap::new(),
            next_from_ballot: None,
            gc_watermark: Ballot::zero(),
            effective: effective.clone(),
        },
    });
    assert_eq!(step, MatchStep::StaleConfiguration { newest: w });
    let _ = persist(&mut n, &mut storage);
    // The restart: the probe adopts `{0}` at `w`, and node 1 is outside.
    let mut r = ColocatedNode::new(&storage, Limits::DEFAULT);
    fire_election(&mut r);
    let tag = r.membership_probe().expect("a boot probes").ballot();
    let generation = r.matchmaker_set().expect("a set").generation;
    let _ = persist(&mut r, &mut storage);
    let step = r.on_match_reply(MatchReply {
        journal: J,
        matchmaker: MatchmakerId(0),
        to: NodeId(1),
        ballot: tag,
        generation,
        outcome: MatchOutcome::Probed {
            effective,
            named: None,
        },
    });
    assert!(
        matches!(step, MatchStep::ProbeClosed { member: false, .. }),
        "{step:?}"
    );
    assert_eq!(r.acceptors_since(), w);
    assert!(
        r.last_member_ballot() >= b,
        "the fence of the registration survives the restart"
    );
    assert!(
        !r.may_retire(w),
        "a registration above the floor names this node"
    );
    // A beat of a later leadership carries a floor above the registration.
    let later = ballot(6, 0);
    r.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: later,
        commit: None,
        config: Some(cfg(&[0])),
        fence: None,
        gc_floor: Some(later),
    });
    let _ = persist(&mut r, &mut storage);
    assert!(later > b);
    let _ = check_registrations(&mut r, later, None);
    assert!(r.may_retire(later));
    // Plain Multi-Paxos writes no fence and checks nothing.
    let mut plain = node(1, &[0, 1, 2]);
    campaign(&mut plain);
    assert!(
        plain
            .ready()
            .writes()
            .iter()
            .all(|w| !matches!(w, WriteOp::RegistrationFence(_)))
    );
    assert!(!plain.check_retirement(later));
    assert_eq!(plain.registration_fence(), Ballot::zero());
}

/// A retirement check opens while the node follows a live leader, whose
/// beats hold the election clock. Its answers are lost: the probe asks
/// again on its own clock, once an election timeout passed.
#[test]
fn a_retirement_check_under_a_live_leader_is_asked_again() {
    let mut n = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3], 3);
    let w = ballot(9, 1);
    let beat = Message::Heartbeat {
        from: NodeId(1),
        ballot: w,
        commit: None,
        config: Some(cfg(&[1, 2])),
        fence: None,
        gc_floor: Some(w),
    };
    n.step(beat.clone());
    n.ready().advance();
    assert!(n.check_retirement_heard());
    // The answers are lost.
    assert_eq!(drain_match_requests(&mut n).len(), 3);
    n.set_election_timeout(3);
    for _ in 0..2 {
        n.step(beat.clone());
        n.tick();
        assert!(drain_match_requests(&mut n).is_empty(), "not yet");
    }
    n.step(beat.clone());
    n.tick();
    let again = drain_match_requests(&mut n);
    assert!(!again.is_empty(), "the probe is asked again");
    assert!(again.iter().all(|(_, r)| r.purpose.is_probe()));
    assert_eq!(n.leader(), Some(NodeId(1)), "the leader is still followed");
    // A second call while the probe is open opens nothing new.
    let tag = n.membership_probe().expect("open").ballot();
    assert!(n.check_retirement_heard());
    assert_eq!(n.membership_probe().expect("open").ballot(), tag);
}
