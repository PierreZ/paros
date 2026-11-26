//! Online reconfiguration (#122), pinned at the mechanism: the refusals, the
//! round change, the cross-configuration Phase 1 that follows, Phase 2 under
//! the new configuration only, and the removed leader's resignation.

use super::*;
use crate::matchmaker::{MatchPurpose, RegistrationKind};

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
pub(super) fn deployed_cluster() -> ([ColocatedNode; 5], Vec<Matchmaker>) {
    deployed_cluster_with(1)
}

/// [`deployed_cluster`] with `matchmakers` matchmakers in the set.
pub(super) fn deployed_cluster_with(matchmakers: u64) -> ([ColocatedNode; 5], Vec<Matchmaker>) {
    let pool = [0, 1, 2, 3, 4];
    let mut nodes = [
        deployed_node(0, &[0, 1, 2], &pool, matchmakers),
        deployed_node(1, &[0, 1, 2], &pool, matchmakers),
        deployed_node(2, &[0, 1, 2], &pool, matchmakers),
        deployed_node(3, &[0, 1, 2], &pool, matchmakers),
        deployed_node(4, &[0, 1, 2], &pool, matchmakers),
    ];
    let mut mms = registries(matchmakers);
    campaign(&mut nodes[0]);
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
    let mut nodes = cluster::<3>();
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
    let _ = nodes[0].propose(entry(1, 1, 1));
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
        nodes[0].propose(entry(1, 2, 2)),
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
    let _ = nodes[0].propose(entry(1, 2, 2));
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

/// While the GC of a reconfiguration is not effective, the old
/// configuration is still in force: the next campaign needs a Phase-1 seal
/// of it too. Only a member of the current configuration credits a window,
/// so the members that both configurations share prove the old one. A
/// window that only a new member acked proves nothing for it, and a second
/// reconfiguration is refused until a shared member acks.
#[test]
fn a_second_reconfiguration_needs_a_proof_for_the_configuration_still_in_force() {
    let (mut nodes, mut mms) = deployed_cluster();
    close_window(&mut nodes, 0);
    let new = cfg(&[0, 1, 3]);
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
    assert_eq!(*nodes[0].acceptors(), new);
    let (prior, _) = nodes[0].configs_in_force().expect("a leader");
    assert_eq!(prior, vec![&cfg(&[0, 1, 2])], "the GC is not effective");
    // A window that the new member 3 acks: {0, 3} is a majority of
    // {0, 1, 3}, not of {0, 1, 2}.
    close_window_alone(&mut nodes[0], &[3]);
    let next = cfg(&[0, 1, 4]);
    assert_eq!(
        nodes[0].reconfigure(&next),
        ReconfigureResult::Refused(ReconfigureRefusal::Unproven)
    );
    // The shared member 1 acks the open window: {0, 1} is a majority of
    // both.
    nodes[0].step(Message::HeartbeatAck {
        from: NodeId(1),
        ballot: nodes[0].ballot(),
        chosen: None,
    });
    drain(&mut nodes[0]);
    let result = nodes[0].reconfigure(&next);
    assert!(
        matches!(result, ReconfigureResult::Started(_)),
        "{result:?}"
    );
}

/// A reconfiguration that matchmakers refuse as naming a retired node
/// (#1002 (retire fence gaps)) skips a minority of such refusals, because a
/// quorum that took the node's rejoin is enough (#1037 (rejoin)). Once the
/// others cannot make a quorum, it is abandoned: the leader steps down with
/// the belief it led under, and its next campaign registers that belief,
/// not the refused configuration. Before the fix every such refusal was
/// ignored and the campaign was re-asked until the node rejoined (witness
/// `replay-recipe 1516240296740112277 121760:12518854452560519938` on
/// b90808f).
#[test]
fn a_reconfiguration_refused_by_a_quorum_as_naming_a_retired_node_is_abandoned() {
    let (mut nodes, _) = deployed_cluster_with(3);
    let old = cfg(&[0, 1, 2]);
    let new = cfg(&[0, 1, 2, 3]);
    let ReconfigureResult::Started(b) = nodes[0].reconfigure(&new) else {
        panic!("the reconfiguration starts");
    };
    let requests = drain_match_requests(&mut nodes[0]);
    assert!(
        !requests.is_empty(),
        "the reconfiguration sends match requests"
    );
    let set = nodes[0].matchmaker_set().cloned().expect("a set");
    let refuse = |node: &mut ColocatedNode, mm: u64| {
        node.on_match_reply(MatchReply {
            matchmaker: MatchmakerId(mm),
            to: NodeId(0),
            ballot: b,
            generation: set.generation,
            outcome: MatchOutcome::Refused(MatchRefusal::Retired { node: NodeId(3) }),
        })
    };
    // One refusal of three: the other two can still make a quorum.
    let step = refuse(&mut nodes[0], 0);
    assert!(matches!(step, MatchStep::Ignored), "{step:?}");
    assert!(nodes[0].matchmaking_pending(), "a minority does not end it");
    // The same matchmaker again changes nothing.
    let step = refuse(&mut nodes[0], 0);
    assert!(matches!(step, MatchStep::Ignored), "{step:?}");
    assert!(nodes[0].matchmaking_pending());
    // A second: no quorum is left, so the campaign is abandoned.
    let step = refuse(&mut nodes[0], 1);
    assert!(
        matches!(step, MatchStep::Refused(MatchRefusal::Retired { .. })),
        "{step:?}"
    );
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert!(!nodes[0].matchmaking_pending());
    assert_eq!(*nodes[0].acceptors(), old, "the belief it led under stays");
    // The next campaign registers the belief, as a belief.
    fire_election(&mut nodes[0]);
    let requests = drain_match_requests(&mut nodes[0]);
    assert!(!requests.is_empty(), "the node campaigns again");
    assert!(
        requests.iter().all(|(_, q)| q.config == old
            && q.purpose == MatchPurpose::Register(RegistrationKind::Belief))
    );
}

/// A node whose own campaign a matchmaker refuses as naming this very node
/// retired rejoins (#1037 (rejoin)): the matchmaker took its retirement
/// check, but the node runs on and campaigns. The incarnation is durable
/// before the rejoin leaves, and the campaign stays open (one refusal of
/// three). Witness `replay-main 13727873074255889094` on b90808f.
#[test]
fn a_node_refused_as_retired_itself_rejoins() {
    let (mut nodes, _) = deployed_cluster_with(3);
    let new = cfg(&[0, 1, 2, 3]);
    let ReconfigureResult::Started(b) = nodes[0].reconfigure(&new) else {
        panic!("the reconfiguration starts");
    };
    let _ = drain_match_requests(&mut nodes[0]);
    let set = nodes[0].matchmaker_set().cloned().expect("a set");
    assert_eq!(nodes[0].retire_mark().incarnation, 0);
    let step = nodes[0].on_match_reply(MatchReply {
        matchmaker: MatchmakerId(0),
        to: NodeId(0),
        ballot: b,
        generation: set.generation,
        outcome: MatchOutcome::Refused(MatchRefusal::Retired { node: NodeId(0) }),
    });
    assert!(matches!(step, MatchStep::Ignored), "{step:?}");
    assert!(nodes[0].matchmaking_pending(), "the campaign stays open");
    assert_eq!(nodes[0].retire_mark().incarnation, 1, "the node rejoins");
    let ready = nodes[0].ready();
    let rejoins = ready
        .match_requests()
        .iter()
        .filter(|(_, q)| q.purpose == MatchPurpose::Rejoin { incarnation: 1 })
        .count();
    ready.advance();
    assert_eq!(rejoins, 3, "every matchmaker is asked to drop its record");
}

/// A matchmaker that refused a reconfiguration as naming a retired node
/// and registers it later (it took the node's rejoin since) no longer
/// counts as refusing. Before the fix a node panicked on its own
/// invariant here.
#[test]
fn a_refusal_ends_when_the_matchmaker_registers() {
    use crate::matchmaking::{MatchFold, Matchmaking, RegisteredPage};
    use crate::membership::MatchmakerSet;
    let set = MatchmakerSet::new(
        MatchmakerGeneration(0),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    );
    let mut m = Matchmaking::new(
        ballot(9, 0),
        cfg(&[0, 1, 2, 3]),
        RegistrationKind::Reconfiguration,
    );
    m.refuse_retired(MatchmakerId(0));
    m.refuse_retired(MatchmakerId(1));
    assert!(
        !m.quorum_reachable(&set),
        "two refusals of three leave no quorum"
    );
    let page = RegisteredPage {
        from_ballot: Ballot::zero(),
        history: std::collections::BTreeMap::new(),
        next_from_ballot: None,
        gc_watermark: Ballot::zero(),
        effective: None,
    };
    assert_eq!(m.fold(MatchmakerId(1), page), MatchFold::Registered);
    m.assert_invariants();
    assert!(m.quorum_reachable(&set), "the registration ended a refusal");
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
    let _ = nodes[0].propose(entry(1, 1, 1));
    let q = drain(&mut nodes[0]);
    assert_eq!(accept_targets(&q), vec![NodeId(1), NodeId(2), NodeId(3)]);
    // The removed leader recorded the round in its own log — the allocator
    // is durable by construction — but the record is a stray copy under
    // `C_new`, not a vote: it is a proposer and learner, not an acceptor.
    assert!(nodes[0].acceptor().records().contains_key(&Slot(0)));
    assert!(
        nodes[0].proposer().rounds()[&Slot(0)]
            .accepted_by()
            .expect("colocated")
            .is_empty(),
        "a removed leader's own record is not a vote under C_new"
    );
    deliver_all(&mut nodes, q);
    assert_eq!(nodes[0].hard_state().chosen_index, Some(Slot(0)));
    // Settled: the next tick resigns.
    nodes[0].tick();
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert_eq!(nodes[0].membership_counters().step_downs, 1);
    // ...and it never campaigns again as a non-member, while a member does.
    campaign(&mut nodes[0]);
    assert_eq!(nodes[0].role(), NodeRole::Follower);
    assert_eq!(nodes[0].membership_counters().campaigns_skipped, 1);
    assert_eq!(
        *nodes[1].acceptors(),
        new,
        "members learned C_new from the Prepare"
    );
    campaign(&mut nodes[1]);
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

/// A node booked again boots on a checked retire mark with its rejoin
/// unstaged: the first probe stages it (#1037 (rejoin)). A node that heard
/// its belief from a leader's beat before that probe campaigns with no
/// probe at all, so its rejoin stays unstaged while it campaigns. A
/// retirement check asked of it then must not panic, and a matchmaker that
/// refuses its campaign as naming it retired stages the rejoin: the
/// incarnation is durable and every matchmaker is asked to drop its
/// record. Before the fix the check asserted that the node followed, and
/// the refusal left the rejoin unstaged, so every matchmaker refused the
/// node for good. Witness: consensus-sim, flavor `retirement`, seed 68.
#[test]
fn a_campaign_with_an_unstaged_rejoin_stages_it_on_a_retired_refusal() {
    use crate::contract::write::{RetireMark, WriteOp};
    let (mut nodes, _) = deployed_cluster_with(3);
    // Node 1 restarts on a disk whose retirement every matchmaker took.
    let mut disk = MemStorage::from_node(&nodes[1]);
    disk.retire_mark = RetireMark {
        incarnation: 0,
        checked: true,
    };
    let mut n = ColocatedNode::new(&disk, Limits::DEFAULT);
    assert_eq!(n.belief_source(), BeliefSource::Bootstrap);
    // The leader's beat teaches the belief before any probe runs.
    nodes[0].tick();
    let beats = drain(&mut nodes[0]);
    for (to, msg) in beats {
        if to == NodeId(1) {
            n.step(msg);
        }
    }
    let _ = drain(&mut n);
    assert_eq!(n.belief_source(), BeliefSource::Heard);
    // The leader is gone: node 1 campaigns, with no probe.
    fire_election(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate);
    assert!(n.membership_probe().is_none(), "a campaign, not a probe");
    let requests = drain_match_requests(&mut n);
    let b = requests.first().expect("the campaign registers").1.ballot;
    assert_eq!(n.retire_mark().incarnation, 0, "the rejoin is unstaged");
    // An operator's retirement check on the candidate is no panic.
    let _ = n.check_retirement(ballot(1, 0));
    // A matchmaker refuses the campaign: this node is retired there.
    let set = n.matchmaker_set().cloned().expect("a set");
    let _ = n.on_match_reply(MatchReply {
        matchmaker: MatchmakerId(0),
        to: NodeId(1),
        ballot: b,
        generation: set.generation,
        outcome: MatchOutcome::Refused(MatchRefusal::Retired { node: NodeId(1) }),
    });
    assert_eq!(n.retire_mark().incarnation, 1, "the rejoin is staged");
    let ready = n.ready();
    let staged = ready
        .writes()
        .iter()
        .any(|w| matches!(w, WriteOp::RetireMark(m) if m.incarnation == 1 && !m.checked));
    let rejoins = ready
        .match_requests()
        .iter()
        .filter(|(_, q)| q.purpose == MatchPurpose::Rejoin { incarnation: 1 })
        .count();
    ready.advance();
    assert!(
        staged,
        "the incarnation is durable before the rejoin leaves"
    );
    assert_eq!(rejoins, 3, "every matchmaker is asked to drop its record");
}

/// Node 0 leads `deployed_cluster`, then learns slot 40 chosen at a ballot
/// above its own through a `Commit`: it still leads, with its promise past
/// its ballot, until a deposing message arrives.
fn passed_leader() -> ([ColocatedNode; 5], Ballot) {
    let (mut nodes, _) = deployed_cluster();
    let mine = nodes[0].ballot();
    nodes[0].step(Message::Commit {
        from: Party::Node(NodeId(1)),
        ballot: ballot(mine.round + 5, 1),
        slot: Slot(40),
        command: ucmd(1, 1, 9),
    });
    nodes[0].ready().advance();
    assert!(nodes[0].is_leader(), "a Commit does not depose the leader");
    assert!(nodes[0].hard_state().max_promised_ballot > mine);
    assert_eq!(nodes[0].ballot(), mine);
    (nodes, mine)
}

/// A leadership whose promise a later ballot passed holds nothing worth
/// moving: a reconfiguration is refused as unsettled.
#[test]
fn a_leader_whose_promise_was_passed_refuses_a_reconfiguration() {
    let (mut nodes, _) = passed_leader();
    assert_eq!(
        nodes[0].reconfigure(&cfg(&[0, 1])),
        ReconfigureResult::Refused(ReconfigureRefusal::Unsettled)
    );
}

/// On a full grid of 2 x 2 a `CheckQuorum` window holds on one column, a
/// Phase-2 quorum, but a Phase-1 quorum needs a member of each column, and
/// a seal of the grid, which the next campaign asks of it, also needs a
/// member of each row. A leader whose proof is one column, or one row,
/// refuses a reconfiguration.
#[test]
fn a_window_held_by_one_column_or_one_row_of_a_grid_proves_nothing() {
    let pool: Vec<NodeId> = (0..5).map(NodeId).collect();
    let mut storage = MemStorage::for_node(0, &[0, 1, 2, 3]);
    storage.config.nodes.clone_from(&pool);
    storage.config.matchmakers = vec![MatchmakerId(0)];
    storage.config.quorum_system = crate::membership::QuorumSystem::grid(2, 2);
    let mut n = ColocatedNode::new(&storage, Limits::DEFAULT);
    let mut mms = registries(1);
    campaign(&mut n);
    let requests = drain_match_requests(&mut n);
    for reply in matchmake(&mut mms, requests) {
        n.on_match_reply(reply);
    }
    drain(&mut n);
    assert!(n.is_leader(), "the first campaign bypasses Phase 1");
    let grid = |members: &[u64]| {
        AcceptorConfig::new(
            members.iter().copied().map(NodeId).collect(),
            crate::membership::QuorumSystem::grid(2, 2),
        )
    };
    let next = grid(&[0, 1, 2, 4]);
    assert_eq!(
        n.reconfigure(&next),
        ReconfigureResult::Refused(ReconfigureRefusal::Unproven),
        "a bypassed election proves nothing"
    );
    // Column 0 is {0, 2}: its acks hold the window, a Phase-2 quorum.
    close_window_alone(&mut n, &[2]);
    assert_eq!(
        n.reconfigure(&next),
        ReconfigureResult::Refused(ReconfigureRefusal::Unproven),
        "one column is no Phase-1 quorum of the grid"
    );
    // Row 0 is {0, 1}: one member of each column, a Phase-1 quorum, but no
    // seal. The next campaign asks the grid of today for a seal, since it is
    // a prior configuration there, so these acks of the open window prove
    // nothing.
    n.step(Message::HeartbeatAck {
        from: NodeId(1),
        ballot: n.ballot(),
        chosen: None,
    });
    drain(&mut n);
    assert_eq!(
        n.reconfigure(&next),
        ReconfigureResult::Refused(ReconfigureRefusal::Unproven),
        "one row is no seal of a full-column grid"
    );
    close_window_alone(&mut n, &[1, 2]);
    assert!(matches!(
        n.reconfigure(&next),
        ReconfigureResult::Started(_)
    ));
}
