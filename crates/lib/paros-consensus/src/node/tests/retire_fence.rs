//! The retire fence (#1002 (retire fence gaps)): a node retires only when no
//! registration above the floor names it. #893 (retire fence) fenced the
//! registrant's own campaign, in memory. Two paths stayed open: a removed
//! member that a stale belief of another node names (gap A), and the
//! registrant after a restart (gap B). Either one leaves a registration
//! above the floor whose Phase-1 quorum needs a node that is gone, and no
//! later campaign wins.

use super::*;
use crate::contract::write::{RetireMark, WriteOp};
use crate::matchmaker::{MatchPurpose, MatchRefusal};
use crate::membership::QuorumSystem;

/// `members` as a `1 × n` grid: the one row is the only Phase-1 quorum,
/// so every Phase 1 needs every member, and one acceptor decides.
fn all_of(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().copied().map(NodeId).collect(),
        QuorumSystem::grid(1, members.len()),
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

/// `node` hears a beat of node 0, the leader at `ballot`, with
/// `config` in force and `gc_floor` effective; it persists what it wrote.
fn hear_leader(
    node: &mut ColocatedNode,
    ballot: Ballot,
    config: AcceptorConfig,
    gc_floor: Option<Ballot>,
) {
    node.step(Message::Heartbeat {
        from: NodeId(0),
        ballot,
        commit: None,
        config: Some(config),
        fence: None,
        gc_floor,
    });
    node.ready().advance();
}

/// Register `config` at `ballot` from `from` at every matchmaker of `mms`.
fn register(mms: &mut [Matchmaker], request: &MatchRequest) {
    for mm in mms.iter_mut() {
        mm.step(request.clone());
        mm.ready().advance();
    }
}

/// Gap A. Configuration `{0,1,2,3}` as a `1 × n` grid. A reconfiguration to
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
        &MatchRequest::reconfigure(NodeId(0), member_at, old.clone(), generation),
    );
    hear_leader(&mut n3, member_at, old.clone(), None);
    assert!(n3.is_acceptor());
    assert_eq!(n3.last_member_ballot(), member_at);
    // The reconfiguration to `{0,1,2}` wins at 12 and its floor is
    // effective at every matchmaker.
    let w = ballot(12, 0);
    register(
        &mut mms,
        &MatchRequest::reconfigure(NodeId(0), w, new.clone(), generation),
    );
    for mm in &mut mms {
        let _ = mm.advance_gc_watermark(generation, w);
        mm.ready().advance();
    }
    // Node 2 missed it: its campaign registers the stale belief at 13. The
    // matchmakers answer with the reconfiguration, node 2 stops, and the
    // registration stays.
    let stale = ballot(13, 2);
    register(
        &mut mms,
        &MatchRequest::new(NodeId(2), stale, old.clone(), generation),
    );
    // Node 3 hears the leader at 12: it is outside, its belief is bound to
    // the floor, and the beat carries the floor.
    hear_leader(&mut n3, w, new.clone(), Some(w));
    assert!(!n3.is_acceptor());
    assert_eq!(n3.acceptors_since(), w);
    assert_eq!(n3.gc_floor_heard(), Some(w));
    // Why it matters: the next campaign's `H_b` holds the stale belief,
    // and `{0,1,2}` is no Phase-1 quorum of it.
    let next = MatchRequest::new(NodeId(0), ballot(14, 0), new.clone(), generation);
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
        let _ = mm.advance_gc_watermark(generation, later);
        mm.ready().advance();
    }
    hear_leader(&mut n3, later, new, Some(later));
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

/// The TLA+ trace of #1002 (retire fence gaps), found by the model of
/// PR #1021: a slow registration reaches a matchmaker after that
/// matchmaker answered the retire check. Configuration `{0,1,2}` with
/// a `1 × n` grid. A reconfiguration to `{0,1}` wins at 12 at matchmakers 0 and
/// 1, and its floor is effective there. Node 1's slow campaign registers
/// the stale belief `(13, {0,1,2})` at matchmaker 0 first. Node 2 asks
/// every matchmaker; matchmaker 1 answers first, and only then does the
/// slow registration reach it; matchmaker 2 answers next. A quorum (1 and 2)
/// said that nothing names node 2, but matchmakers 0 and 1 hold the stale
/// belief, so every matchmaker quorum holds it in `H_b`, and every later
/// Phase 1 needs node 2's promise.
///
/// The rule Pierre chose on 2026-11-12: every matchmaker of the set must
/// answer, and a matchmaker that answered refuses each later registration
/// that names the node.
#[test]
fn a_registration_after_the_check_does_not_strand_a_retired_member() {
    let mut mms = registries(3);
    let mut n2 = deployed_node(2, &[0, 1, 2], &[0, 1, 2], 3);
    let generation = n2
        .matchmaker_set()
        .expect("a matchmaker deployment")
        .generation;
    let old = all_of(&[0, 1, 2]);
    let new = all_of(&[0, 1]);
    // Node 0 led at 3 under `{0,1,2}`: node 2 was a member at 3.
    let member_at = ballot(3, 0);
    register(
        &mut mms,
        &MatchRequest::reconfigure(NodeId(0), member_at, old.clone(), generation),
    );
    hear_leader(&mut n2, member_at, old.clone(), None);
    // The reconfiguration to `{0,1}` wins at 12 at matchmakers 0 and 1, and
    // its floor is effective there.
    let w = ballot(12, 0);
    let reconfigure = MatchRequest::reconfigure(NodeId(0), w, new.clone(), generation);
    register(&mut mms[..2], &reconfigure);
    for mm in &mut mms[..2] {
        let _ = mm.advance_gc_watermark(generation, w);
        mm.ready().advance();
    }
    // Node 1's slow campaign registers the stale belief at matchmaker 0.
    let stale = MatchRequest::new(NodeId(1), ballot(13, 1), old.clone(), generation);
    register(&mut mms[..1], &stale);
    // Node 2 hears the leader at 12 with the floor.
    hear_leader(&mut n2, w, new.clone(), Some(w));
    assert!(!n2.is_acceptor());
    assert_eq!(n2.gc_floor_heard(), Some(w));
    assert!(n2.check_retirement_heard(), "the check opens a probe");
    let requests = drain_match_requests(&mut n2);
    let ask = |id: u64| {
        requests
            .iter()
            .filter(|(to, _)| *to == MatchmakerId(id))
            .cloned()
            .collect::<Vec<_>>()
    };
    // Matchmaker 1 answers first.
    for reply in matchmake(&mut mms, ask(1)) {
        let _ = n2.on_match_reply(reply);
        n2.ready().advance();
    }
    // The slow registration reaches matchmaker 1 only now: it must not
    // register a configuration that names a node it said may retire.
    mms[1].step(stale.clone());
    let ready = mms[1].ready();
    let outcomes: Vec<MatchOutcome> = ready.replies().iter().map(|r| r.outcome.clone()).collect();
    ready.advance();
    assert!(
        outcomes
            .iter()
            .all(|o| !matches!(o, MatchOutcome::Registered { .. })),
        "a matchmaker that answered the check refuses a belief naming the node"
    );
    // Matchmaker 2 answers next: a quorum has answered.
    for reply in matchmake(&mut mms, ask(2)) {
        let _ = n2.on_match_reply(reply);
        n2.ready().advance();
    }
    assert!(
        !n2.may_retire(w),
        "a quorum is not every matchmaker: matchmaker 0 holds the stale belief"
    );
    // Matchmaker 0 answers last and names the stale belief.
    for reply in matchmake(&mut mms, ask(0)) {
        let _ = n2.on_match_reply(reply);
        n2.ready().advance();
    }
    assert!(!n2.may_retire(w), "a registration above the floor names it");
    assert!(!n2.may_retire_heard());
}

/// Node 2 of `{0,1,2}` retires from the journal: node 0 removes it at 12,
/// the floor 12 is effective at every matchmaker, and every matchmaker
/// agrees to node 2's retirement check. The checked mark is durable in
/// `storage`. Returns the floor, the old and the new configurations and the
/// matchmaker generation.
fn retire_node_2(
    mms: &mut [Matchmaker],
    storage: &mut MemStorage,
) -> (Ballot, AcceptorConfig, AcceptorConfig, MatchmakerGeneration) {
    storage.config.nodes = [0, 1, 2].map(NodeId).to_vec();
    storage.config.matchmakers = (0..3).map(MatchmakerId).collect();
    let mut n2 = ColocatedNode::new(storage, Limits::DEFAULT);
    let generation = n2.matchmaker_set().expect("a set").generation;
    let old = all_of(&[0, 1, 2]);
    let new = all_of(&[0, 1]);
    // Node 0 led at 3 under `{0,1,2}`, then removed node 2 at 12; the floor
    // 12 is effective at every matchmaker.
    let member_at = ballot(3, 0);
    register(
        mms,
        &MatchRequest::reconfigure(NodeId(0), member_at, old.clone(), generation),
    );
    n2.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: member_at,
        commit: None,
        config: Some(old.clone()),
        fence: None,
        gc_floor: None,
    });
    let _ = persist(&mut n2, storage);
    let w = ballot(12, 0);
    register(
        mms,
        &MatchRequest::reconfigure(NodeId(0), w, new.clone(), generation),
    );
    for mm in mms.iter_mut() {
        let _ = mm.advance_gc_watermark(generation, w);
        mm.ready().advance();
    }
    n2.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: w,
        commit: None,
        config: Some(new.clone()),
        fence: None,
        gc_floor: Some(w),
    });
    let _ = persist(&mut n2, storage);
    // The check: every matchmaker agrees, and the mark is checked, durably.
    assert!(n2.check_retirement_heard());
    let requests = persist(&mut n2, storage);
    for reply in matchmake(mms, requests) {
        let _ = n2.on_match_reply(reply);
        let _ = persist(&mut n2, storage);
    }
    assert!(n2.may_retire(w));
    let checked = RetireMark {
        incarnation: 0,
        checked: true,
    };
    assert_eq!(n2.retire_mark(), checked);
    assert_eq!(storage.retire_mark, checked, "the check is durable");
    (w, old, new, generation)
}

/// The comeback of #1037 (rejoin) (Rejoin, Pierre's choice on 2026-11-12): node 2
/// retires once every matchmaker agreed, and each one records it retired
/// and refuses a registration that names it. Placement books it back
/// later, and the journal restarts from the kept store. The boot reads the
/// checked mark, raises the incarnation durably before it sends, and asks
/// every matchmaker to drop the record. A registration that names the node
/// registers again, and a retirement check of the earlier incarnation is
/// refused. Checked with TLA+: `specs/tla/retire-fence`, `rejoin_tagged`.
#[test]
fn a_rejoined_node_is_named_again() {
    let mut mms = registries(3);
    let mut storage = MemStorage::for_node(2, &[0, 1, 2]);
    let (w, old, new, generation) = retire_node_2(&mut mms, &mut storage);
    // Every matchmaker now refuses a registration naming node 2.
    let back = MatchRequest::reconfigure(NodeId(0), ballot(19, 0), old.clone(), generation);
    let refused = matchmake(
        &mut mms,
        (0..3).map(|m| (MatchmakerId(m), back.clone())).collect(),
    );
    assert!(refused.iter().all(|r| matches!(
        r.outcome,
        MatchOutcome::Refused(MatchRefusal::Retired { node: NodeId(2) })
    )));
    // Placement books node 2 back: the journal restarts from its store. The
    // boot probe carries the rejoin at incarnation 1, and the raised mark
    // is in the same batch, ahead of the send.
    let mut r = ColocatedNode::new(&storage, Limits::DEFAULT);
    fire_election(&mut r);
    let ready = r.ready();
    let writes = ready.writes().to_vec();
    storage.apply(&writes);
    let requests = ready.match_requests().to_vec();
    ready.advance();
    let rejoined = RetireMark {
        incarnation: 1,
        checked: false,
    };
    assert!(writes.contains(&WriteOp::RetireMark(rejoined)));
    assert_eq!(requests.len(), 3, "every matchmaker is asked");
    assert!(
        requests
            .iter()
            .all(|(_, q)| q.purpose == MatchPurpose::Rejoin { incarnation: 1 })
    );
    // Matchmakers 0 and 1 take the rejoin; matchmaker 2 is slow. A quorum
    // is enough to name node 2 again: matchmaker 2 still refuses, and a
    // candidate skips that refusal (Pierre, 2026-11-12).
    let (late, quorum): (Vec<_>, Vec<_>) = requests
        .into_iter()
        .partition(|(m, _)| *m == MatchmakerId(2));
    for reply in matchmake(&mut mms, quorum) {
        let _ = r.on_match_reply(reply);
        let _ = persist(&mut r, &mut storage);
    }
    let named = MatchRequest::reconfigure(NodeId(0), ballot(20, 0), old.clone(), generation);
    let replies = matchmake(
        &mut mms,
        (0..3).map(|m| (MatchmakerId(m), named.clone())).collect(),
    );
    let registered = replies
        .iter()
        .filter(|r| matches!(r.outcome, MatchOutcome::Registered { .. }))
        .count();
    assert_eq!(
        registered, 2,
        "a quorum that took the rejoin registers: {replies:?}"
    );
    assert!(matches!(
        replies[2].outcome,
        MatchOutcome::Refused(MatchRefusal::Retired { node: NodeId(2) })
    ));
    // The rejoin is still open for matchmaker 2, and blocks nothing: a
    // retirement check at the new incarnation takes its place.
    r.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: w,
        commit: None,
        config: Some(new.clone()),
        fence: None,
        gc_floor: Some(w),
    });
    let _ = persist(&mut r, &mut storage);
    assert!(r.check_retirement_heard(), "the check opens");
    let asks = persist(&mut r, &mut storage);
    assert!(
        !asks.is_empty(),
        "the retirement check asks the matchmakers"
    );
    assert!(asks.iter().all(|(_, q)| q.purpose
        == MatchPurpose::Retire {
            watermark: w,
            incarnation: 1
        }));
    // Matchmaker 2 takes the late rejoin; the node no longer waits on it.
    for reply in matchmake(&mut mms, late) {
        let _ = r.on_match_reply(reply);
        let _ = persist(&mut r, &mut storage);
    }
    assert_eq!(r.retire_mark(), rejoined);
    assert_eq!(storage.retire_mark, rejoined);
    // A registration that names node 2 registers again.
    let again = MatchRequest::reconfigure(NodeId(0), ballot(21, 0), old.clone(), generation);
    let replies = matchmake(
        &mut mms,
        (0..3).map(|m| (MatchmakerId(m), again.clone())).collect(),
    );
    assert!(
        replies
            .iter()
            .all(|r| matches!(r.outcome, MatchOutcome::Registered { .. })),
        "{replies:?}"
    );
    // A late retirement check of incarnation 0 is refused: the node came
    // back at 1.
    let late = MatchRequest::probe(NodeId(2), ballot(22, 2), new, generation).retiring(w, 0);
    let replies = matchmake(&mut mms[..1], vec![(MatchmakerId(0), late)]);
    assert!(matches!(
        replies[0].outcome,
        MatchOutcome::Refused(MatchRefusal::Rejoined { incarnation: 1 })
    ));
    // Plain Multi-Paxos holds no mark and asks for no rejoin.
    let plain = node(2, &[0, 1, 2]);
    assert_eq!(plain.retire_mark(), RetireMark::default());
}

/// A rejoin staged durably but never sent is sent again at the next boot
/// (#1037 (rejoin)). Node 2 retired, and placement booked it back. The
/// boot stages `RetireMark { 1, unchecked }`, and the batch lands, but the
/// journal stops before a `Rejoin` request leaves (a failed sync with an
/// unknown outcome quarantines it). The next boot reads an unchecked mark
/// at incarnation 1: it is no retirement, but every matchmaker still holds
/// node 2 retired at 0. The boot asks again at the staged incarnation, with
/// no new write, and a registration naming node 2 registers again.
/// Witness: `sim-paros-hunt replay-main 3069338247747008104` on 689e9ae (a
/// move back to a node every matchmaker held retired, refused `Retired`
/// until the deadline).
#[test]
fn a_staged_rejoin_that_never_left_is_sent_again_at_boot() {
    let mut mms = registries(3);
    let mut storage = MemStorage::for_node(2, &[0, 1, 2]);
    let (_, old, _, generation) = retire_node_2(&mut mms, &mut storage);
    // The first comeback stages the next incarnation durably, then stops
    // before its requests leave.
    let mut first = ColocatedNode::new(&storage, Limits::DEFAULT);
    fire_election(&mut first);
    let ready = first.ready();
    storage.apply(ready.writes());
    assert!(
        !ready.match_requests().is_empty(),
        "the rejoin was due to leave"
    );
    drop(first);
    let staged = RetireMark {
        incarnation: 1,
        checked: false,
    };
    assert_eq!(storage.retire_mark, staged, "the staged mark is durable");
    // The next boot sends the rejoin again, at the staged incarnation.
    let mut again = ColocatedNode::new(&storage, Limits::DEFAULT);
    assert_eq!(again.retire_mark(), staged);
    fire_election(&mut again);
    let ready = again.ready();
    let writes = ready.writes().to_vec();
    storage.apply(&writes);
    let requests = ready.match_requests().to_vec();
    ready.advance();
    assert!(
        !writes.iter().any(|w| matches!(w, WriteOp::RetireMark(_))),
        "the incarnation is durable already: {writes:?}"
    );
    assert_eq!(requests.len(), 3, "every matchmaker is asked");
    assert!(
        requests
            .iter()
            .all(|(_, q)| q.purpose == MatchPurpose::Rejoin { incarnation: 1 }),
        "{requests:?}"
    );
    for reply in matchmake(&mut mms, requests) {
        let _ = again.on_match_reply(reply);
        let _ = persist(&mut again, &mut storage);
    }
    assert_eq!(again.retire_mark(), staged);
    let named = MatchRequest::reconfigure(NodeId(0), ballot(20, 0), old, generation);
    let replies = matchmake(
        &mut mms,
        (0..3).map(|m| (MatchmakerId(m), named.clone())).collect(),
    );
    assert!(
        replies
            .iter()
            .all(|r| matches!(r.outcome, MatchOutcome::Registered { .. })),
        "{replies:?}"
    );
    // A node that never retired holds an unchecked mark at 0, and its boot
    // asks for no rejoin.
    let mut fresh_storage = MemStorage::for_node(1, &[0, 1, 2]);
    fresh_storage.config.matchmakers = (0..3).map(MatchmakerId).collect();
    let mut fresh = ColocatedNode::new(&fresh_storage, Limits::DEFAULT);
    fire_election(&mut fresh);
    let ready = fresh.ready();
    assert!(
        ready
            .match_requests()
            .iter()
            .all(|(_, q)| !matches!(q.purpose, MatchPurpose::Rejoin { .. })),
        "a node at incarnation 0 never rejoins"
    );
    ready.advance();
}

/// A rejoin a matchmaker quorum took, with no leader to follow, asks the
/// rest again on its own clock, ahead of an election clock that a
/// campaign's `Prepare` restarted.
#[test]
fn a_rejoin_with_no_leader_asks_the_rest_again() {
    let mut mms = registries(3);
    let mut storage = MemStorage::for_node(2, &[0, 1, 2]);
    let _ = retire_node_2(&mut mms, &mut storage);
    let mut n = ColocatedNode::new(&storage, Limits::DEFAULT);
    fire_election(&mut n);
    let requests = persist(&mut n, &mut storage);
    assert_eq!(requests.len(), 3, "every matchmaker is asked");
    let mut replies = matchmake(&mut mms, requests);
    let _ = replies.pop();
    for reply in replies {
        let _ = n.on_match_reply(reply);
        let _ = persist(&mut n, &mut storage);
    }
    assert!(n.rejoin.is_some(), "a quorum leaves the rejoin open");
    assert!(n.leader().is_none());
    n.set_election_timeout(3);
    n.tick();
    n.tick();
    // A campaign's `Prepare` restarts the election clock, not the rejoin's.
    n.step(Message::Prepare {
        reply_to: NodeId(0),
        ballot: ballot(40, 0),
        from_slot: Slot(0),
        config: None,
    });
    let _ = persist(&mut n, &mut storage);
    assert!(n.leader().is_none());
    n.tick();
    let asked: Vec<(MatchmakerId, MatchPurpose)> = persist(&mut n, &mut storage)
        .into_iter()
        .map(|(m, q)| (m, q.purpose))
        .collect();
    assert_eq!(
        asked,
        vec![(MatchmakerId(2), MatchPurpose::Rejoin { incarnation: 1 })],
        "only the matchmaker that did not take the rejoin is asked again"
    );
    assert!(n.membership_probe().is_none(), "no probe carried it");
}

/// Close `n2`'s open rejoin. Late answers at the closed check's tag
/// `check_tag` are not the rejoin's: every matchmaker answering there
/// leaves the rejoin open. A quorum of the rejoin's own answers to
/// `requests` leaves it open too; the last one closes it.
fn close_rejoin(
    n2: &mut ColocatedNode,
    storage: &mut MemStorage,
    mms: &mut [Matchmaker],
    requests: Vec<(MatchmakerId, MatchRequest)>,
    check_tag: Ballot,
) {
    for m in 0..3 {
        let _ = n2.on_match_reply(MatchReply {
            matchmaker: MatchmakerId(m),
            to: NodeId(2),
            ballot: check_tag,
            generation: MatchmakerGeneration(0),
            outcome: MatchOutcome::Probed {
                effective: None,
                named: None,
            },
        });
        let _ = persist(n2, storage);
    }
    assert!(
        n2.rejoin.is_some(),
        "an answer at another tag is not the rejoin's"
    );
    let mut replies = matchmake(mms, requests);
    let last = replies.pop().expect("three answers");
    for reply in replies {
        let _ = n2.on_match_reply(reply);
        let _ = persist(n2, storage);
    }
    assert!(n2.rejoin.is_some(), "a quorum leaves the rejoin open");
    let _ = n2.on_match_reply(last);
    let _ = persist(n2, storage);
    assert!(n2.rejoin.is_none(), "every matchmaker took the rejoin");
}

/// A journal booked again while it still runs on a node that retired from
/// it rejoins with no reboot (#1037 (rejoin)): `rejoin` stages the next
/// incarnation and asks every matchmaker to drop its record, and a
/// registration naming the node is taken again. Witness of the stall it
/// fixes: replay-recipe `2684408044022300164 1194217:12375250690498203620`
/// (a reconfiguration onto a lingering node refused `Retired` for good).
#[test]
fn a_running_journal_booked_again_rejoins() {
    let mut mms = registries(3);
    let mut storage = MemStorage::for_node(2, &[0, 1, 2]);
    storage.config.nodes = [0, 1, 2].map(NodeId).to_vec();
    storage.config.matchmakers = (0..3).map(MatchmakerId).collect();
    let mut n2 = ColocatedNode::new(&storage, Limits::DEFAULT);
    let generation = n2.matchmaker_set().expect("a set").generation;
    let old = all_of(&[0, 1, 2]);
    let new = all_of(&[0, 1]);
    let member_at = ballot(3, 0);
    register(
        &mut mms,
        &MatchRequest::reconfigure(NodeId(0), member_at, old.clone(), generation),
    );
    let w = ballot(12, 0);
    register(
        &mut mms,
        &MatchRequest::reconfigure(NodeId(0), w, new.clone(), generation),
    );
    for mm in &mut mms {
        let _ = mm.advance_gc_watermark(generation, w);
        mm.ready().advance();
    }
    n2.step(Message::Heartbeat {
        from: NodeId(0),
        ballot: w,
        commit: None,
        config: Some(new),
        fence: None,
        gc_floor: Some(w),
    });
    let _ = persist(&mut n2, &mut storage);
    assert!(n2.check_retirement_heard());
    let requests = persist(&mut n2, &mut storage);
    let check_tag = requests[0].1.ballot;
    for reply in matchmake(&mut mms, requests) {
        let _ = n2.on_match_reply(reply);
        let _ = persist(&mut n2, &mut storage);
    }
    assert!(
        n2.retire_mark().checked,
        "every matchmaker let node 2 retire"
    );
    assert_eq!(n2.closed_probe, Some(check_tag), "the check's probe closed");
    // Booked again before it stopped: the running node rejoins.
    assert!(n2.rejoin());
    let ready = n2.ready();
    let writes = ready.writes().to_vec();
    storage.apply(&writes);
    let requests = ready.match_requests().to_vec();
    ready.advance();
    let rejoined = RetireMark {
        incarnation: 1,
        checked: false,
    };
    assert!(
        writes.contains(&WriteOp::RetireMark(rejoined)),
        "the incarnation is durable before the rejoin leaves"
    );
    assert_eq!(n2.retire_mark(), rejoined);
    assert!(!n2.may_retire(w), "a rejoined node does not retire");
    assert_eq!(requests.len(), 3, "every matchmaker is asked");
    assert!(
        requests
            .iter()
            .all(|(_, q)| q.purpose == MatchPurpose::Rejoin { incarnation: 1 })
    );
    // A second call while the rejoin is open asks nothing new.
    assert!(n2.rejoin());
    assert!(
        persist(&mut n2, &mut storage).is_empty(),
        "a second rejoin asks nothing new"
    );
    close_rejoin(&mut n2, &mut storage, &mut mms, requests, check_tag);
    // A registration naming node 2 is taken again.
    let back = MatchRequest::reconfigure(NodeId(0), ballot(19, 0), old, generation);
    let answers = matchmake(
        &mut mms,
        (0..3).map(|m| (MatchmakerId(m), back.clone())).collect(),
    );
    assert!(
        answers
            .iter()
            .all(|r| matches!(r.outcome, MatchOutcome::Registered { .. }))
    );
}

/// Plain Multi-Paxos never rejoins (#1037 (rejoin)).
#[test]
fn a_plain_journal_never_rejoins() {
    let storage = MemStorage::for_node(0, &[0, 1, 2]);
    let mut n0 = ColocatedNode::new(&storage, Limits::DEFAULT);
    assert!(!n0.rejoin());
    let ready = n0.ready();
    assert!(ready.writes().is_empty() && ready.match_requests().is_empty());
    ready.advance();
}

/// The gone-node gap (2026-11-14 review; `specs/tla/retire-fence`, the
/// `crash_all_tomb` check). The fence protects a node that *retires*. A node
/// that crashes for good never runs the retire check, so no matchmaker
/// records it and nothing refuses a registration that names it.
///
/// Five acceptors under majorities. A reconfiguration from `{0,1,2,3,4}` to
/// `{0,1,2,3,5}` wins at 12 and its floor is effective. Node 4 left and
/// crashed, as the paper allows. Node 3 missed the change and its campaign
/// registers the stale belief `(13, {0,1,2,3,4})` above the floor. Then nodes
/// 0 and 1 crash: two failures of five, inside what `{0,1,2,3,5}` is sized
/// for. Node 2's campaign gets the stale belief in `H_b`, and that
/// configuration has no live Phase-1 quorum: no campaign wins again, and only
/// a leader can raise the floor past the registration.
///
/// The registry still hands the stale belief to every campaign: the gap is
/// closed one layer up. Node 3 is alive and testifies that it never opened
/// Phase 1 at 13, so Phase 1 drops `{0,1,2,3,4}` from `H_b`
/// (`node/tests/matchmaking.rs`,
/// `an_abandoned_registration_testified_by_its_owner_needs_no_quorum`).
#[test]
fn a_crashed_member_named_by_a_stale_belief_stays_in_the_history() {
    let mut mms = registries(3);
    let n = deployed_node(2, &[0, 1, 2, 3, 4], &[0, 1, 2, 3, 4], 3);
    let generation = n
        .matchmaker_set()
        .expect("a matchmaker deployment")
        .generation;
    let old = cfg(&[0, 1, 2, 3, 4]);
    let new = cfg(&[0, 1, 2, 3, 5]);
    register(
        &mut mms,
        &MatchRequest::reconfigure(NodeId(0), ballot(3, 0), old.clone(), generation),
    );
    let w = ballot(12, 0);
    register(
        &mut mms,
        &MatchRequest::reconfigure(NodeId(0), w, new.clone(), generation),
    );
    for mm in &mut mms {
        let _ = mm.advance_gc_watermark(generation, w);
        mm.ready().advance();
    }
    // Node 3's stale belief, above the floor. Node 4 crashed, so no
    // matchmaker recorded it and nothing refuses the registration.
    register(
        &mut mms,
        &MatchRequest::new(NodeId(3), ballot(13, 3), old.clone(), generation),
    );
    // Nodes 0 and 1 crash. Node 2 campaigns under the configuration in force.
    let next = MatchRequest::new(NodeId(2), ballot(14, 2), new.clone(), generation);
    let mut prior = Vec::new();
    let mut registered = 0;
    for mm in &mut mms {
        mm.step(next.clone());
        let ready = mm.ready();
        for reply in ready.replies() {
            if let MatchOutcome::Registered { history, .. } = &reply.outcome {
                registered += 1;
                prior.extend(history.values().map(|r| r.config.clone()));
            }
        }
        ready.advance();
    }
    assert_eq!(registered, mms.len(), "every matchmaker registers node 2");
    let survivors: std::collections::BTreeSet<NodeId> = [2, 3, 5].map(NodeId).into_iter().collect();
    assert!(
        new.has_phase1_quorum(&survivors),
        "the configuration in force is live"
    );
    assert!(
        new.has_phase2_quorum(&survivors),
        "the configuration in force is live"
    );
    assert!(prior.contains(&old), "the stale belief is in H_b");
    assert!(
        !old.has_phase1_quorum(&survivors),
        "without the owner testimony, no campaign could win"
    );
}
