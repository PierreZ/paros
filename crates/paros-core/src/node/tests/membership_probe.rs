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
/// skips its campaigns from then on without probing again.
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
    }));
    assert_eq!(n.belief_source(), BeliefSource::Heard);
    assert_eq!(n.role(), NodeRole::Follower);
    fire_election(&mut n);
    assert_eq!(
        n.membership_counters().campaigns_skipped,
        1,
        "the settled spare skips"
    );
    assert!(
        drain_match_requests(&mut n).is_empty(),
        "and probes no more"
    );
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
