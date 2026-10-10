//! The membership probe (#173), pinned at the mechanism: a node whose belief
//! is only the bootstrap default asks the matchmakers which configuration is
//! in force before it campaigns or skips on that default, the probe
//! registers nothing anywhere, and its answer either brings the node into a
//! campaign on a belief it heard or settles it as a non-member for this
//! incarnation.
//!
//! The scenario is the issue's: a rotation from `{0, 1, 2}` to `{3, 4, 5}`
//! whose every new member rebooted. The red→green evidence is the
//! simulation's (the commit that landed the probe); these tests pin what a
//! single node does with the answers.

use super::*;

/// Registries that hold `{3, 4, 5}` as the effective configuration: a
/// leader at ballot `(2, 2)` registered the rotation with each of them.
fn rotated_registries() -> Vec<Matchmaker> {
    let mut mms = registries(3);
    let rotation = (0..3)
        .map(|m| {
            (
                MatchmakerId(m),
                MatchRequest::reconfigure(NodeId(2), ballot(2, 2), cfg(&[3, 4, 5]), G0),
            )
        })
        .collect();
    matchmake(&mut mms, rotation);
    mms
}

const G0: MatchmakerGeneration = MatchmakerGeneration(0);

/// A fresh incarnation of node 3 on the six-node pool whose bootstrap is
/// `{0, 1, 2}`: what every member of the rotated set boots as.
fn rebooted_member() -> ColocatedNode {
    deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3, 4, 5], 3)
}

#[test]
fn a_node_outside_its_bootstrap_belief_probes_instead_of_skipping() {
    let mut n = rebooted_member();
    assert_eq!(n.belief_source(), BeliefSource::Bootstrap);
    let promised = n.hard_state().max_promised_ballot;
    fire_election(&mut n);
    assert_eq!(n.role(), NodeRole::Follower, "a probe is not a campaign");
    assert_eq!(
        n.membership_counters().campaigns_skipped,
        0,
        "nothing was skipped"
    );
    let (msgs, (requests, writes)) = drain_with(&mut n, |ready| {
        (ready.match_requests().to_vec(), ready.writes().to_vec())
    });
    assert!(
        msgs.iter()
            .all(|(_, m)| !matches!(m, Message::Prepare { .. }))
    );
    assert!(writes.is_empty(), "a probe promises nothing");
    assert_eq!(n.hard_state().max_promised_ballot, promised);
    assert_eq!(requests.len(), 3, "every matchmaker is asked");
    assert!(requests.iter().all(|(_, r)| r.purpose.is_probe()));
}

#[test]
fn a_probe_registers_nothing_at_the_matchmakers() {
    let mut mms = rotated_registries();
    let mut n = rebooted_member();
    fire_election(&mut n);
    let requests = drain_match_requests(&mut n);
    for (id, request) in requests {
        let mm = &mut mms[usize::try_from(id.0).expect("matchmaker index")];
        let before = mm.hard_state().clone();
        mm.step(request.clone());
        let ready = mm.ready();
        assert!(ready.writes().is_empty(), "a probe stages no write");
        assert_eq!(
            ready.replies()[0].outcome,
            MatchOutcome::Probed {
                effective: Some((ballot(2, 2), cfg(&[3, 4, 5])))
            }
        );
        ready.advance();
        assert_eq!(*mm.hard_state(), before);
        // The next campaign's history does not name the probe: a fresh
        // registration above it sees only the rotation.
        mm.step(MatchRequest::new(
            NodeId(4),
            ballot(90, 4),
            cfg(&[3, 4, 5]),
            G0,
        ));
        let ready = mm.ready();
        let MatchOutcome::Registered { history, .. } = &ready.replies()[0].outcome else {
            panic!("expected a registration");
        };
        assert_eq!(history.keys().copied().collect::<Vec<_>>(), [ballot(2, 2)]);
        ready.advance();
    }
}

/// The #173 wedge, closed: a rebooted member of the rotated set learns the
/// rotation from a matchmaker quorum and campaigns under it at once.
#[test]
fn a_probe_that_finds_its_node_inside_opens_a_campaign() {
    let mut mms = rotated_registries();
    let mut n = rebooted_member();
    fire_election(&mut n);
    let replies = matchmake(&mut mms, drain_match_requests(&mut n));
    let steps: Vec<MatchStep> = replies.into_iter().map(|r| n.on_match_reply(r)).collect();
    assert!(steps.contains(&MatchStep::ProbeAnswered));
    assert!(steps.contains(&MatchStep::ProbeClosed {
        effective: Some(ballot(2, 2)),
        member: true,
        answered_by: 2,
    }));
    assert_eq!(*n.acceptors(), cfg(&[3, 4, 5]));
    assert_eq!(n.acceptors_since(), ballot(2, 2));
    assert_eq!(n.belief_source(), BeliefSource::Heard);
    assert_eq!(n.role(), NodeRole::Candidate);
    let requests = drain_match_requests(&mut n);
    assert!(
        requests
            .iter()
            .all(|(_, r)| !r.purpose.is_probe() && r.config == cfg(&[3, 4, 5])),
        "the campaign registers the configuration the probe learned"
    );
}

/// A spare on a cluster that never reconfigured: the quorum names no
/// reconfiguration, the bootstrap stands as a heard belief, and the node
/// skips its campaigns from then on, re-probing on each election timeout so
/// a reconfiguration naming it is heard even if no campaign registers it
/// (#270).
#[test]
fn a_probe_that_finds_no_reconfiguration_settles_a_spare() {
    let mut mms = registries(3);
    let mut n = rebooted_member();
    fire_election(&mut n);
    let replies = matchmake(&mut mms, drain_match_requests(&mut n));
    let steps: Vec<MatchStep> = replies.into_iter().map(|r| n.on_match_reply(r)).collect();
    assert!(steps.contains(&MatchStep::ProbeClosed {
        effective: None,
        member: false,
        answered_by: 2,
    }));
    assert_eq!(n.belief_source(), BeliefSource::Heard);
    assert_eq!(n.role(), NodeRole::Follower);
    fire_election(&mut n);
    assert_eq!(
        n.membership_counters().campaigns_skipped,
        1,
        "the settled spare skips"
    );
    assert_eq!(n.role(), NodeRole::Follower, "and never campaigns");
    assert!(
        n.membership_probe().is_some(),
        "but re-probes from outside its belief"
    );
    assert!(!drain_match_requests(&mut n).is_empty());
}

/// A node outside a heard reconfiguration re-probes, and a quorum that
/// misses the one matchmaker holding it never moves its belief backwards
/// (#270).
#[test]
fn a_re_probe_never_moves_a_belief_backwards() {
    let mut n = rebooted_member();
    n.step(Message::Heartbeat {
        from: NodeId(4),
        ballot: ballot(3, 4),
        commit: None,
        config: Some(cfg(&[0, 1, 4])),
        fence: None,
    });
    assert_eq!(n.belief_source(), BeliefSource::Heard);
    let mut mms = registries(3);
    fire_election(&mut n);
    let replies = matchmake(&mut mms, drain_match_requests(&mut n));
    let steps: Vec<MatchStep> = replies.into_iter().map(|r| n.on_match_reply(r)).collect();
    assert!(steps.contains(&MatchStep::ProbeClosed {
        effective: Some(ballot(3, 4)),
        member: false,
        answered_by: 2,
    }));
    assert_eq!(n.acceptors(), &cfg(&[0, 1, 4]), "the newer belief stands");
}

/// A belief heard on the wire answers the probe itself: the node adopts it
/// and the open probe closes without a reply.
#[test]
fn a_heard_belief_closes_an_open_probe() {
    let mut n = rebooted_member();
    fire_election(&mut n);
    assert!(n.membership_probe().is_some());
    let _ = drain_match_requests(&mut n);
    n.step(Message::Heartbeat {
        from: NodeId(4),
        ballot: ballot(3, 4),
        commit: None,
        config: Some(cfg(&[3, 4, 5])),
        fence: None,
    });
    assert!(n.membership_probe().is_none());
    assert_eq!(n.belief_source(), BeliefSource::Heard);
    assert_eq!(*n.acceptors(), cfg(&[3, 4, 5]));
}

/// A campaign opens strictly above every probe tag, so a late answer to a
/// probe can never be read as the campaign's.
#[test]
fn a_campaign_opens_above_the_probe_tag() {
    let mut mms = rotated_registries();
    let mut n = rebooted_member();
    fire_election(&mut n);
    let tag = n
        .membership_probe()
        .map(crate::matchmaking::MembershipProbe::ballot)
        .expect("a probe");
    let replies = matchmake(&mut mms, drain_match_requests(&mut n));
    for reply in replies {
        n.on_match_reply(reply);
    }
    assert_eq!(n.role(), NodeRole::Candidate);
    assert!(n.ballot() > tag);
}

/// A member of the bootstrap default probes too, and never registers the
/// default it did not hear: a rebooted member of the five-node bootstrap,
/// after the cluster moved to `{0, 2, 4}`, learns that set and campaigns on
/// it — where campaigning on the default registered `{0..4}`, a record every
/// later `H_b` had to cover even with node 1 retired.
#[test]
fn a_member_of_its_default_probes_and_registers_only_what_it_heard() {
    let mut mms = registries(3);
    let shrink = (0..3)
        .map(|m| {
            (
                MatchmakerId(m),
                MatchRequest::reconfigure(NodeId(3), ballot(16, 3), cfg(&[0, 2, 4]), G0),
            )
        })
        .collect();
    matchmake(&mut mms, shrink);
    let mut n = deployed_node(0, &[0, 1, 2, 3, 4], &[0, 1, 2, 3, 4], 3);
    fire_election(&mut n);
    assert_eq!(
        n.role(),
        NodeRole::Follower,
        "a member probes first as well"
    );
    let requests = drain_match_requests(&mut n);
    assert!(requests.iter().all(|(_, r)| r.purpose.is_probe()));
    let replies = matchmake(&mut mms, requests);
    for reply in replies {
        n.on_match_reply(reply);
    }
    assert_eq!(*n.acceptors(), cfg(&[0, 2, 4]));
    assert_eq!(n.role(), NodeRole::Candidate);
    let requests = drain_match_requests(&mut n);
    assert!(
        requests
            .iter()
            .all(|(_, r)| !r.purpose.is_probe() && r.config == cfg(&[0, 2, 4])),
        "the only registration is the set the probe heard"
    );
}

/// The #278 wedge, shape A: a rotation that reached one matchmaker of three,
/// whose answer arrives after the other two closed the probe. The late
/// answer is folded as the probe would have folded it, and the node it
/// names campaigns.
#[test]
fn a_late_probe_answer_moves_an_outside_node() {
    let mut mms = registries(3);
    matchmake(
        &mut mms,
        vec![(
            MatchmakerId(2),
            MatchRequest::reconfigure(NodeId(2), ballot(2, 2), cfg(&[3, 4, 5]), G0),
        )],
    );
    let mut n = rebooted_member();
    fire_election(&mut n);
    let (late, early): (Vec<_>, Vec<_>) = drain_match_requests(&mut n)
        .into_iter()
        .partition(|(id, _)| *id == MatchmakerId(2));
    let steps: Vec<MatchStep> = matchmake(&mut mms, early)
        .into_iter()
        .map(|r| n.on_match_reply(r))
        .collect();
    assert!(steps.contains(&MatchStep::ProbeClosed {
        effective: None,
        member: false,
        answered_by: 2,
    }));
    assert_eq!(n.role(), NodeRole::Follower);
    let steps: Vec<MatchStep> = matchmake(&mut mms, late)
        .into_iter()
        .map(|r| n.on_match_reply(r))
        .collect();
    assert_eq!(
        steps,
        [MatchStep::ProbeLate {
            effective: Some(ballot(2, 2)),
            member: true,
        }]
    );
    assert_eq!(*n.acceptors(), cfg(&[3, 4, 5]));
    assert_eq!(n.role(), NodeRole::Candidate);
}

/// A late answer that names no newer fact, or lands after a campaign
/// opened, moves nothing.
#[test]
fn a_late_probe_answer_after_a_campaign_is_ignored() {
    let mut mms = rotated_registries();
    let mut n = rebooted_member();
    fire_election(&mut n);
    let (late, early): (Vec<_>, Vec<_>) = drain_match_requests(&mut n)
        .into_iter()
        .partition(|(id, _)| *id == MatchmakerId(2));
    for reply in matchmake(&mut mms, early) {
        n.on_match_reply(reply);
    }
    assert_eq!(n.role(), NodeRole::Candidate, "the quorum named it");
    let steps: Vec<MatchStep> = matchmake(&mut mms, late)
        .into_iter()
        .map(|r| n.on_match_reply(r))
        .collect();
    assert_eq!(steps, [MatchStep::Ignored]);
    assert_eq!(n.role(), NodeRole::Candidate);
}

/// The #278 wedge, shape B: a node that promised an ordinary campaign at
/// round 4 learned its `C_b`, which leaves it outside. The reconfiguration
/// at round 2 that names it is an older ballot but a fact, and the campaign
/// was only promised, never known to win: the probe adopts the fact, and
/// keeps the belief bound at round 4 so no `Prepare` at or below it flips
/// it back.
#[test]
fn a_probe_prefers_a_fact_to_a_campaign_heard_belief() {
    let mut mms = rotated_registries();
    let mut n = rebooted_member();
    n.step(Message::Prepare {
        reply_to: NodeId(1),
        ballot: ballot(4, 1),
        from_slot: Slot(0),
        config: Some(cfg(&[0, 1, 2])),
    });
    let _ = drain(&mut n);
    assert_eq!(n.belief_source(), BeliefSource::Heard);
    assert_eq!(n.acceptors_since(), ballot(4, 1));
    fire_election(&mut n);
    let steps: Vec<MatchStep> = matchmake(&mut mms, drain_match_requests(&mut n))
        .into_iter()
        .map(|r| n.on_match_reply(r))
        .collect();
    assert!(steps.contains(&MatchStep::ProbeClosed {
        effective: Some(ballot(4, 1)),
        member: true,
        answered_by: 2,
    }));
    assert_eq!(*n.acceptors(), cfg(&[3, 4, 5]));
    assert_eq!(n.acceptors_since(), ballot(4, 1));
    assert_eq!(n.role(), NodeRole::Candidate);
}

/// A leadership this node knows won outranks an older reconfiguration a
/// matchmaker still holds (#278): the rotation at round 2 was never in
/// effect if a leader won round 4 under `{0, 1, 2}`, and a spare that
/// adopted it would finish a rotation nobody was told of.
#[test]
fn a_won_leadership_outranks_an_older_reconfiguration() {
    let mut mms = rotated_registries();
    let mut n = rebooted_member();
    n.step(Message::Heartbeat {
        from: NodeId(1),
        ballot: ballot(4, 1),
        commit: None,
        config: Some(cfg(&[0, 1, 2])),
        fence: None,
    });
    fire_election(&mut n);
    let steps: Vec<MatchStep> = matchmake(&mut mms, drain_match_requests(&mut n))
        .into_iter()
        .map(|r| n.on_match_reply(r))
        .collect();
    assert!(steps.contains(&MatchStep::ProbeClosed {
        effective: Some(ballot(4, 1)),
        member: false,
        answered_by: 2,
    }));
    assert_eq!(
        *n.acceptors(),
        cfg(&[0, 1, 2]),
        "the won configuration stands"
    );
    assert_eq!(n.role(), NodeRole::Follower);
}
