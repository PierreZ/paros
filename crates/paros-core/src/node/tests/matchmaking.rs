//! The leader-side matchmaking phase (#120) and the cross-configuration
//! Phase 1 (#121), pinned at the mechanism: what a campaign sends before and
//! after its matchmaker quorum, how histories union, and — the
//! safety-critical rule — that Phase 1 completes only with a promise quorum
//! of **every** prior configuration, never of their union.

use super::*;

/// Reply to `n`'s open matchmaking from `mms`, folding every answer.
fn run_matchmaking(n: &mut RawNode, mms: &mut [Matchmaker]) -> Vec<MatchStep> {
    let requests = drain_match_requests(n);
    let replies = matchmake(mms, requests);
    replies
        .into_iter()
        .map(|reply| n.on_match_reply(reply))
        .collect()
}

/// Fire the election clock on `n`.
fn campaign(n: &mut RawNode) {
    n.set_election_timeout(1);
    n.tick();
}

fn prepares(msgs: &[(NodeId, Message)]) -> Vec<NodeId> {
    msgs.iter()
        .filter(|(_, m)| matches!(m, Message::Prepare { .. }))
        .map(|(to, _)| *to)
        .collect()
}

/// A registered reply from `mm` for `ballot`, hand-built (the wire shape,
/// independent of any registry instance).
fn registered(
    mm: u64,
    ballot: Ballot,
    history: &[(Ballot, AcceptorConfig)],
    wm: Ballot,
) -> MatchReply {
    MatchReply {
        matchmaker: MatchmakerId(mm),
        to: ballot.node,
        ballot,
        outcome: MatchOutcome::Registered {
            history: history.iter().cloned().collect(),
            gc_watermark: wm,
        },
    }
}

fn promise(from: u64, ballot: Ballot) -> Message {
    Message::Promise {
        config_id: ConfigId::default(),
        from: NodeId(from),
        ballot,
        from_slot: Slot(0),
        accepted: BTreeMap::new(),
        faulty: BTreeMap::new(),
        next_from_slot: None,
    }
}

/// Invariant 1 (#120): on a matchmaker deployment a campaign sends its
/// registration first and no `Prepare` until a matchmaker quorum has
/// answered; the plain path sends `Prepare` at once and never a request.
#[test]
fn a_campaign_registers_before_it_prepares() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    campaign(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate);
    assert!(n.matchmaking_pending());
    let msgs = drain(&mut n);
    assert!(prepares(&msgs).is_empty(), "no Prepare before the quorum");
    let requests = {
        let ready = n.ready();
        let r = ready.match_requests().to_vec();
        ready.advance();
        r
    };
    // The batch was drained above; re-run to see the requests were queued
    // with the campaign (one per matchmaker, all at the campaign ballot).
    assert!(requests.is_empty(), "drained with the batch");
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    campaign(&mut n);
    let requests = drain_match_requests(&mut n);
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|(_, r)| r.ballot == n.ballot()));
    assert!(requests.iter().all(|(_, r)| r.config == cfg(&[0, 1, 2])));

    // Plain Multi-Paxos: straight to Prepare, no request ever.
    let mut plain = node(0, &[0, 1, 2]);
    campaign(&mut plain);
    assert!(!plain.matchmaking_pending());
    let msgs = drain(&mut plain);
    assert_eq!(prepares(&msgs).len(), 2);
    assert!(drain_match_requests(&mut plain).is_empty());
    assert!(msgs.iter().all(|(_, m)| !matches!(
        m,
        Message::Prepare {
            config: Some(_),
            ..
        }
    )));
}

/// A minority of matchmakers answering is not enough; the quorum's last
/// reply opens Phase 1 with a `Prepare` to every member, carrying the
/// registered configuration.
#[test]
fn a_matchmaker_quorum_opens_phase_one() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    let mut mms = registries(3);
    campaign(&mut n);
    let requests = drain_match_requests(&mut n);
    let replies = matchmake(&mut mms, requests);
    let step = n.on_match_reply(replies[0].clone());
    assert_eq!(step, MatchStep::Registered { remaining: 1 });
    assert!(n.matchmaking_pending());
    assert!(prepares(&drain(&mut n)).is_empty());
    let step = n.on_match_reply(replies[1].clone());
    assert!(matches!(
        step,
        MatchStep::Completed {
            registered_by: 2,
            ..
        }
    ));
    assert!(!n.matchmaking_pending());
    let msgs = drain(&mut n);
    let mut targets = prepares(&msgs);
    targets.sort_unstable();
    assert_eq!(targets, vec![NodeId(1), NodeId(2)]);
    assert!(msgs.iter().any(|(_, m)| matches!(
        m,
        Message::Prepare { config: Some(c), .. } if *c == cfg(&[0, 1, 2])
    )));
    // The third, late reply changes nothing.
    assert_eq!(n.on_match_reply(replies[2].clone()), MatchStep::Ignored);
}

/// Invariant 4: a refusal abandons the campaign — follower again, no
/// `Prepare`, and the next campaign runs at a strictly higher round.
#[test]
fn a_refused_registration_never_becomes_a_leadership() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    let mut mms = registries(3);
    // Another proposer is already registered above us everywhere.
    let above = MatchRequest::new(NodeId(1), ballot(7, 1), cfg(&[0, 1, 2]));
    for mm in &mut mms {
        mm.step(above.clone());
        mm.ready().advance();
    }
    campaign(&mut n);
    let first_round = n.ballot().round;
    let steps = run_matchmaking(&mut n, &mut mms);
    assert!(matches!(
        steps[0],
        MatchStep::Refused(MatchRefusal::Stale { .. })
    ));
    assert_eq!(n.role(), NodeRole::Follower);
    assert!(!n.matchmaking_pending());
    assert!(prepares(&drain(&mut n)).is_empty());
    // The refusal's `highest` lifts the next campaign strictly above the
    // round that refused this one — never one round up from our own, which
    // the same registration would refuse again (the leapfrog livelock).
    campaign(&mut n);
    assert!(n.ballot().round > first_round);
    assert_eq!(n.ballot().round, 8, "one above the refuser's highest (7)");
}

/// Invariants 2 and 3: `H_b` is the union of every replying matchmaker's
/// history — a configuration reported by only one of them is in — filtered
/// by the **maximum** reported watermark, and a duplicate reply is folded
/// once.
#[test]
fn the_prior_set_is_the_union_above_the_maximum_watermark() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4], 3);
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    let c_a = cfg(&[0, 1, 2]);
    let c_b = cfg(&[2, 3, 4]);
    let c_old = cfg(&[1, 2, 3]);
    // Matchmaker 0 saw the old config at round 1 and A at round 2; matchmaker
    // 1 saw A at round 2 and B at round 3 and has GC'd below round 2.
    let r0 = registered(
        0,
        b,
        &[(ballot(1, 1), c_old.clone()), (ballot(2, 1), c_a.clone())],
        Ballot::zero(),
    );
    let r1 = registered(
        1,
        b,
        &[(ballot(2, 1), c_a.clone()), (ballot(3, 2), c_b.clone())],
        ballot(2, 1),
    );
    assert_eq!(
        n.on_match_reply(r0.clone()),
        MatchStep::Registered { remaining: 1 }
    );
    assert_eq!(
        n.on_match_reply(r0),
        MatchStep::Ignored,
        "a duplicate reply folds once"
    );
    let step = n.on_match_reply(r1);
    let MatchStep::Completed {
        prior, watermark, ..
    } = step
    else {
        panic!("quorum closes the phase: {step:?}");
    };
    assert_eq!(
        watermark,
        ballot(2, 1),
        "the maximum watermark, not the minimum"
    );
    assert_eq!(
        prior,
        vec![c_a.clone(), c_b.clone()],
        "the union above the watermark: A (shared), B (only matchmaker 1); the old config is collected"
    );
    // The Prepare fans out to the union of the prior configurations and C_b.
    let msgs = drain(&mut n);
    let mut targets = prepares(&msgs);
    targets.sort_unstable();
    assert_eq!(targets, vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);
}

/// A history naming a configuration other than the registered one is `H_b`
/// to cover, never a reason to abandon the campaign or change the node's
/// belief (the ledger records every registration, aborted ones included —
/// adopting "the newest" flip-flopped a candidate between two beliefs
/// forever). The campaign completes, Phase 1 covers both, and the
/// leadership runs under the configuration it registered.
#[test]
fn a_history_naming_another_configuration_is_covered_not_adopted() {
    let mut n = deployed_node(2, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    let other = cfg(&[2, 3, 4]);
    let step = n.on_match_reply(registered(
        0,
        b,
        &[
            (ballot(1, 0), cfg(&[0, 1, 2])),
            (ballot(4, 1), other.clone()),
        ],
        Ballot::zero(),
    ));
    let MatchStep::Completed { prior, .. } = step else {
        panic!("the campaign completes: {step:?}");
    };
    assert_eq!(prior, vec![cfg(&[0, 1, 2]), other]);
    assert_eq!(n.role(), NodeRole::Candidate);
    assert_eq!(*n.acceptors(), cfg(&[0, 1, 2]), "the belief is untouched");
    let mut targets = prepares(&drain(&mut n));
    targets.sort_unstable();
    assert_eq!(targets, vec![NodeId(0), NodeId(1), NodeId(3), NodeId(4)]);
}

/// Re-sending targets only the matchmakers that have not answered, and is a
/// no-op once the phase closed.
#[test]
fn resend_targets_only_the_unanswered_matchmakers() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    n.on_match_reply(registered(1, b, &[], Ballot::zero()));
    n.resend_matchmaking();
    let mut targets: Vec<u64> = drain_match_requests(&mut n)
        .into_iter()
        .map(|(mm, _)| mm.0)
        .collect();
    targets.sort_unstable();
    assert_eq!(targets, vec![0, 2]);
    n.on_match_reply(registered(2, b, &[], Ballot::zero()));
    assert!(!n.matchmaking_pending());
    n.resend_matchmaking();
    assert!(drain_match_requests(&mut n).is_empty());
}

/// The election timeout does **not** abandon a campaign stuck in
/// matchmaking (lost replies, a slow matchmaker link): the ballot is promised
/// and registered, so the clock re-sends the requests to every unanswered
/// matchmaker and the campaign stays at its ballot. A late reply still
/// completes it. (Abandoning made a matchmaker link slower than one election
/// timeout an unwinnable deployment: every campaign re-registered one round
/// higher, forever.)
#[test]
fn the_election_clock_re_sends_a_stuck_matchmaking() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    campaign(&mut n);
    let first = n.ballot();
    drain_match_requests(&mut n);
    n.on_match_reply(registered(1, first, &[], Ballot::zero()));
    n.set_election_timeout(2);
    n.tick();
    n.tick();
    assert_eq!(n.role(), NodeRole::Candidate);
    assert_eq!(n.ballot(), first, "the clock never moves the ballot");
    assert!(n.matchmaking_pending());
    assert_eq!(n.matchmaking_timeouts(), 1);
    let mut targets: Vec<u64> = drain_match_requests(&mut n)
        .into_iter()
        .map(|(mm, _)| mm.0)
        .collect();
    targets.sort_unstable();
    assert_eq!(
        targets,
        vec![0, 2],
        "only the unanswered matchmakers are re-asked"
    );
    // A late reply completes the same campaign.
    assert!(matches!(
        n.on_match_reply(registered(2, first, &[], Ballot::zero())),
        MatchStep::Completed { .. }
    ));
    assert_eq!(n.ballot(), first);
}

// ---- cross-configuration Phase 1 (#121) --------------------------------------

/// Drive `n` through matchmaking with an explicit history and return the
/// Phase-1 `Prepare` targets.
fn open_phase1(n: &mut RawNode, prior: &[AcceptorConfig]) -> Vec<NodeId> {
    campaign(n);
    let b = n.ballot();
    drain_match_requests(n);
    let history: Vec<(Ballot, AcceptorConfig)> = prior
        .iter()
        .enumerate()
        .map(|(i, c)| (ballot(u64::try_from(i).unwrap() + 1, 9), c.clone()))
        .collect();
    let step = n.on_match_reply(registered(0, b, &history, Ballot::zero()));
    assert!(matches!(step, MatchStep::Completed { .. }), "{step:?}");
    let mut targets = prepares(&drain(n));
    targets.sort_unstable();
    targets
}

/// Identical consecutive configurations: one promise quorum, as today.
#[test]
fn identical_configurations_need_one_quorum() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 1);
    open_phase1(&mut n, &[cfg(&[0, 1, 2]), cfg(&[0, 1, 2])]);
    n.step(promise(1, n.ballot()));
    assert!(n.is_leader(), "self + one peer is a majority of {{0,1,2}}");
}

/// Overlapping configurations: one promise counts toward every
/// configuration containing its sender, and the shared majority wins both.
#[test]
fn one_promise_counts_toward_every_configuration_containing_it() {
    let mut n = deployed_node(2, &[0, 1, 2], &[0, 1, 2, 3], 1);
    // {0,1,2} and {1,2,3}: node 2 (self) and node 1 are in both.
    open_phase1(&mut n, &[cfg(&[0, 1, 2]), cfg(&[1, 2, 3])]);
    assert!(!n.is_leader(), "self alone is one of two in each");
    n.step(promise(1, n.ballot()));
    assert!(
        n.is_leader(),
        "node 1's single promise completes both configurations at once"
    );
}

/// Disjoint configurations complete only when both independently answer.
#[test]
fn disjoint_configurations_each_need_their_own_quorum() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4, 5], 1);
    open_phase1(&mut n, &[cfg(&[0, 1, 2]), cfg(&[3, 4, 5])]);
    n.step(promise(1, n.ballot()));
    n.step(promise(2, n.ballot()));
    assert!(!n.is_leader(), "all of {{0,1,2}} is no quorum of {{3,4,5}}");
    n.step(promise(3, n.ballot()));
    assert!(!n.is_leader());
    n.step(promise(4, n.ballot()));
    assert!(n.is_leader());
}

/// **The headline negative case**: a promise set larger than any single
/// quorum — a majority of the *union* — that still fails one old
/// configuration must not complete Phase 1. `quorum(union)` would have
/// elected here; `∀C: quorum(C)` refuses.
#[test]
fn a_majority_of_the_union_that_fails_one_configuration_does_not_win() {
    let mut n = deployed_node(0, &[0, 1, 2, 3], &[0, 1, 2, 3, 4, 5, 6], 1);
    // union = {0..=6}: 7 nodes, majority 4. {4,5,6} needs 2.
    open_phase1(&mut n, &[cfg(&[0, 1, 2, 3]), cfg(&[4, 5, 6])]);
    for from in [1, 2, 3, 4] {
        n.step(promise(from, n.ballot()));
    }
    // Five promises (self + 4) out of seven: a majority of the union, all of
    // {0,1,2,3}, but only one of {4,5,6}.
    assert!(
        !n.is_leader(),
        "a union majority that fails a prior configuration never elects"
    );
    assert_eq!(n.role(), NodeRole::Candidate);
    n.step(promise(5, n.ballot()));
    assert!(n.is_leader(), "the second {{4,5,6}} promise completes it");
}

/// Several historic configurations (3+ in `H_b`), none complete until all are.
#[test]
fn every_one_of_several_historic_configurations_must_be_covered() {
    let mut n = deployed_node(0, &[0, 1], &[0, 1, 2, 3, 4, 5], 1);
    open_phase1(&mut n, &[cfg(&[0, 1]), cfg(&[2, 3]), cfg(&[4, 5])]);
    n.step(promise(1, n.ballot()));
    n.step(promise(2, n.ballot()));
    n.step(promise(3, n.ballot()));
    n.step(promise(4, n.ballot()));
    assert!(!n.is_leader(), "{{4,5}} needs both");
    n.step(promise(5, n.ballot()));
    assert!(n.is_leader());
}

/// Empty `H_b` (nothing below the watermark): Phase 1 is trivially complete
/// — explicitly, at the boundary — and the leader starts from its own prefix.
#[test]
fn an_empty_history_completes_phase_one_at_once() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 1);
    let targets = open_phase1(&mut n, &[]);
    assert!(n.is_leader(), "nothing to intersect with");
    assert_eq!(targets, vec![NodeId(1), NodeId(2)], "C_b is still prepared");
    assert_eq!(n.next_slot(), Slot(0));
}

/// Safe-value selection sees every response: a value held only by an old
/// configuration's member is re-proposed, never no-op-filled.
#[test]
fn a_value_held_only_by_an_old_configuration_is_re_proposed() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    open_phase1(&mut n, &[cfg(&[0, 1, 2]), cfg(&[3, 4])]);
    n.step(promise(1, n.ballot()));
    let old = ballot(2, 9);
    n.step(Message::Promise {
        config_id: ConfigId::default(),
        from: NodeId(3),
        ballot: n.ballot(),
        from_slot: Slot(0),
        accepted: BTreeMap::from([(Slot(0), (old, ucmd(7, 1, 42)))]),
        faulty: BTreeMap::new(),
        next_from_slot: None,
    });
    assert!(!n.is_leader(), "{{3,4}} needs both");
    n.step(promise(4, n.ballot()));
    assert!(n.is_leader());
    let msgs = drain(&mut n);
    let re_proposed = msgs.iter().any(|(_, m)| {
        matches!(m, Message::Accept { slot, command, .. } if *slot == Slot(0) && *command == ucmd(7, 1, 42))
    });
    assert!(re_proposed, "the old configuration's only copy wins P2c");
}

/// Gap fill under several configurations: a slot nobody reported is
/// no-op-filled only once every prior configuration's quorum is in — the
/// fill is licensed by the same predicate as the win.
#[test]
fn gap_fill_waits_for_every_configuration() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    open_phase1(&mut n, &[cfg(&[0, 1, 2]), cfg(&[3, 4])]);
    n.step(promise(1, n.ballot()));
    // A record at slot 1 and nothing at slot 0: slot 0 is the hole.
    n.step(Message::Promise {
        config_id: ConfigId::default(),
        from: NodeId(3),
        ballot: n.ballot(),
        from_slot: Slot(0),
        accepted: BTreeMap::from([(Slot(1), (ballot(2, 9), ucmd(7, 2, 43)))]),
        faulty: BTreeMap::new(),
        next_from_slot: None,
    });
    assert!(!n.is_leader());
    assert!(
        drain(&mut n)
            .iter()
            .all(|(_, m)| !matches!(m, Message::Accept { .. }))
    );
    n.step(promise(4, n.ballot()));
    assert!(n.is_leader());
    assert_eq!(
        n.election_gap_fills(),
        1,
        "slot 0 is filled once {{3,4}} is covered"
    );
}

/// A leader outside an old configuration is still answered by its members:
/// the acceptor guard is "the ballot's owner, from the pool", never "in my
/// configuration". And a spare accepts from the moment it is prepared.
#[test]
fn a_removed_member_still_answers_phase_one_for_a_non_member_leader() {
    // Node 3 is a spare; nodes 0..=2 are the bootstrap configuration.
    let mut old = deployed_node(1, &[0, 1, 2], &[0, 1, 2, 3], 1);
    let mut spare = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3], 1);
    let leader_ballot = ballot(5, 3);
    let prepare = Message::Prepare {
        config_id: ConfigId::default(),
        from: NodeId(3),
        ballot: leader_ballot,
        from_slot: Slot(0),
        config: Some(cfg(&[3])),
    };
    old.step(prepare.clone());
    let msgs = drain(&mut old);
    assert!(
        msgs.iter()
            .any(|(to, m)| *to == NodeId(3) && matches!(m, Message::Promise { .. })),
        "the old member promises a ballot owned by a node outside its configuration"
    );
    // ...and learned the configuration the ballot was registered with.
    assert_eq!(*old.acceptors(), cfg(&[3]));
    assert_eq!(old.acceptors_since(), leader_ballot);
    spare.step(prepare);
    assert!(
        drain(&mut spare)
            .iter()
            .any(|(_, m)| matches!(m, Message::Promise { .. }))
    );
    // A plain node ignores the configuration field outright.
    let mut plain = node(1, &[0, 1, 2]);
    plain.step(Message::Prepare {
        config_id: ConfigId::default(),
        from: NodeId(0),
        ballot: ballot(5, 0),
        from_slot: Slot(0),
        config: Some(cfg(&[0])),
    });
    assert_eq!(*plain.acceptors(), cfg(&[0, 1, 2]));
    assert_eq!(plain.acceptors_since(), Ballot::zero());
}
