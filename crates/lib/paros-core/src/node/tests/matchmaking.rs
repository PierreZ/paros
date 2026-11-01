//! The leader-side matchmaking phase (#120) and the cross-configuration
//! Phase 1 (#121), pinned at the mechanism: what a campaign sends before and
//! after its matchmaker quorum, how histories union, and — the
//! safety-critical rule — that Phase 1 completes only with a promise quorum
//! of **every** prior configuration, never of their union.

use super::*;
use crate::contract::limits::Limits;

/// The journal every node of these tests serves (`Config::new` with
/// `JournalIdentifier::UNSET`).
const J: crate::journal::JournalId = crate::journal::JournalId::UNSET;

/// Reply to `n`'s open matchmaking from `mms`, folding every answer.
fn run_matchmaking(n: &mut ColocatedNode, mms: &mut [Matchmaker]) -> Vec<MatchStep> {
    let requests = drain_match_requests(n);
    let replies = matchmake(mms, requests);
    replies
        .into_iter()
        .map(|reply| n.on_match_reply(reply))
        .collect()
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
    let history = history
        .iter()
        .map(|(b, c)| (*b, Registration::belief(c.clone())))
        .collect();
    registered_with(mm, ballot, history, wm)
}

/// A registered reply carrying an explicit ledger (beliefs and
/// reconfigurations).
fn registered_with(
    mm: u64,
    ballot: Ballot,
    history: BTreeMap<Ballot, Registration>,
    wm: Ballot,
) -> MatchReply {
    registered_effective(mm, ballot, history, wm, None)
}

/// A registered reply that also reports the matchmaker's durable effective
/// configuration — what survives a GC floor raised over its record.
fn registered_effective(
    mm: u64,
    ballot: Ballot,
    history: BTreeMap<Ballot, Registration>,
    wm: Ballot,
    effective: Option<(Ballot, AcceptorConfig)>,
) -> MatchReply {
    registered_reply(
        mm,
        ballot,
        MatchOutcome::Registered {
            from_ballot: wm,
            history,
            next_from_ballot: None,
            gc_watermark: wm,
            effective,
        },
    )
}

/// One page of a paged answer: it starts at `from`, and `next` is the cursor
/// the following page begins at (`None` for the last page).
fn registered_page(
    mm: u64,
    ballot: Ballot,
    from: Ballot,
    history: BTreeMap<Ballot, Registration>,
    next: Option<Ballot>,
) -> MatchReply {
    registered_reply(
        mm,
        ballot,
        MatchOutcome::Registered {
            from_ballot: from,
            history,
            next_from_ballot: next,
            gc_watermark: Ballot::zero(),
            effective: None,
        },
    )
}

/// The generation-0 reply from `mm` to `ballot`'s candidate carrying
/// `outcome`.
fn registered_reply(mm: u64, ballot: Ballot, outcome: MatchOutcome) -> MatchReply {
    MatchReply {
        journal: J,
        matchmaker: MatchmakerId(mm),
        to: ballot.node,
        ballot,
        generation: MatchmakerGeneration(0),
        outcome,
    }
}

/// A ledger entry that was a candidate's belief.
fn belief(ballot: Ballot, members: &[u64]) -> (Ballot, Registration) {
    (ballot, Registration::belief(cfg(members)))
}

/// A ledger entry that was a reconfiguration request.
fn reconfigured(ballot: Ballot, members: &[u64]) -> (Ballot, Registration) {
    (ballot, Registration::reconfiguration(cfg(members)))
}

fn promise(from: u64, ballot: Ballot) -> Message {
    terminal_promise(NodeId(from), ballot, BTreeMap::new())
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
    let requests = drain_match_requests(&mut n);
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
    let above = MatchRequest::new(
        J,
        NodeId(1),
        ballot(7, 1),
        cfg(&[0, 1, 2]),
        MatchmakerGeneration(0),
    );
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
fn open_phase1(n: &mut ColocatedNode, prior: &[AcceptorConfig]) -> Vec<NodeId> {
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
    assert_eq!(n.proposer().next_slot(), Slot(0));
}

/// Safe-value selection sees every response: a value held only by an old
/// configuration's member is re-proposed, never no-op-filled.
#[test]
fn a_value_held_only_by_an_old_configuration_is_re_proposed() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    open_phase1(&mut n, &[cfg(&[0, 1, 2]), cfg(&[3, 4])]);
    n.step(promise(1, n.ballot()));
    let old = ballot(2, 9);
    n.step(terminal_promise(
        NodeId(3),
        n.ballot(),
        BTreeMap::from([(Slot(0), (old, ucmd(7, 1, 42)))]),
    ));
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
    n.step(terminal_promise(
        NodeId(3),
        n.ballot(),
        BTreeMap::from([(Slot(1), (ballot(2, 9), ucmd(7, 2, 43)))]),
    ));
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
        reply_to: NodeId(3),
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
        reply_to: NodeId(0),
        ballot: ballot(5, 0),
        from_slot: Slot(0),
        config: Some(cfg(&[0])),
    });
    assert_eq!(*plain.acceptors(), cfg(&[0, 1, 2]));
    assert_eq!(plain.acceptors_since(), Ballot::zero());
}

// ---- the effective configuration (#122, review of #132) --------------------

/// A stale candidate never reinstates a superseded configuration: the
/// quorum's histories name a reconfiguration to `{2, 3, 4}`, so a campaign
/// that registered the bootstrap `{0, 1, 2}` abandons, adopts it, and its
/// next campaign registers it.
#[test]
fn a_stale_candidate_adopts_the_highest_reconfiguration_and_re_campaigns() {
    let mut n = deployed_node(2, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    let history = BTreeMap::from([
        belief(ballot(1, 0), &[0, 1, 2]),
        reconfigured(ballot(4, 1), &[2, 3, 4]),
        // A later *belief* in the old set (a rival that was itself stale)
        // does not outrank the reconfiguration.
        belief(ballot(6, 0), &[0, 1, 2]),
    ]);
    let step = n.on_match_reply(registered_with(0, b, history, Ballot::zero()));
    assert_eq!(
        step,
        MatchStep::StaleConfiguration {
            newest: ballot(4, 1)
        }
    );
    assert_eq!(n.role(), NodeRole::Follower);
    assert_eq!(*n.acceptors(), cfg(&[2, 3, 4]));
    assert_eq!(n.acceptors_since(), ballot(4, 1));
    assert!(
        prepares(&drain(&mut n)).is_empty(),
        "no Prepare under the stale belief"
    );
    campaign(&mut n);
    let requests = drain_match_requests(&mut n);
    assert!(
        requests
            .iter()
            .all(|(_, r)| r.config == cfg(&[2, 3, 4]) && !r.purpose.is_reconfiguration())
    );
}

/// Beliefs are not facts: a history made only of other candidates'
/// registrations — however many, however new — never moves a belief. This is
/// the negative space of the flip-flop the hunt found.
#[test]
fn other_candidates_beliefs_never_change_a_belief() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    let history = BTreeMap::from([
        belief(ballot(3, 1), &[2, 3, 4]),
        belief(ballot(5, 2), &[1, 2, 3]),
        belief(ballot(7, 1), &[3, 4]),
    ]);
    let step = n.on_match_reply(registered_with(0, b, history, Ballot::zero()));
    assert!(matches!(step, MatchStep::Completed { .. }), "{step:?}");
    assert_eq!(*n.acceptors(), cfg(&[0, 1, 2]), "the belief stands");
    assert_eq!(n.role(), NodeRole::Candidate);
}

/// The highest reconfiguration wins, across replies: matchmaker 0 knows an
/// older change, matchmaker 1 a newer one, and the union's highest is the
/// effective configuration. A belief that already matches it completes.
#[test]
fn the_effective_configuration_is_the_highest_reconfiguration_across_the_quorum() {
    let mut stale = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4], 3);
    campaign(&mut stale);
    let b = stale.ballot();
    drain_match_requests(&mut stale);
    let older = BTreeMap::from([reconfigured(ballot(2, 1), &[1, 2, 3])]);
    let newer = BTreeMap::from([
        reconfigured(ballot(2, 1), &[1, 2, 3]),
        reconfigured(ballot(5, 2), &[2, 3, 4]),
    ]);
    assert_eq!(
        stale.on_match_reply(registered_with(0, b, older, Ballot::zero())),
        MatchStep::Registered { remaining: 1 }
    );
    assert_eq!(
        stale.on_match_reply(registered_with(1, b, newer.clone(), Ballot::zero())),
        MatchStep::StaleConfiguration {
            newest: ballot(5, 2)
        }
    );
    assert_eq!(*stale.acceptors(), cfg(&[2, 3, 4]));

    let mut current = deployed_node(2, &[2, 3, 4], &[0, 1, 2, 3, 4], 1);
    campaign(&mut current);
    let b = current.ballot();
    drain_match_requests(&mut current);
    let step = current.on_match_reply(registered_with(0, b, newer, Ballot::zero()));
    assert!(matches!(step, MatchStep::Completed { .. }), "{step:?}");
}

/// A reconfiguring leader is exempt: its own registration *is* the next
/// effective configuration, whatever the ledger held before — and the
/// request it sends is flagged as one.
#[test]
fn a_reconfiguration_campaign_is_never_stale() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3], 1);
    let mut mms = registries(1);
    campaign(&mut n);
    let requests = drain_match_requests(&mut n);
    assert!(
        requests
            .iter()
            .all(|(_, r)| !r.purpose.is_reconfiguration())
    );
    for reply in matchmake(&mut mms, requests) {
        n.on_match_reply(reply);
    }
    drain(&mut n);
    n.step(promise(1, n.ballot()));
    assert!(n.is_leader());
    let new = cfg(&[0, 1, 3]);
    assert!(matches!(n.reconfigure(&new), ReconfigureResult::Started(_)));
    let b = n.ballot();
    let requests = drain_match_requests(&mut n);
    assert!(
        requests
            .iter()
            .all(|(_, r)| r.purpose.is_reconfiguration() && r.config == new)
    );
    // The ledger names an unrelated older reconfiguration: irrelevant to a
    // reconfiguration campaign, which completes and prepares its own target.
    let history = BTreeMap::from([
        belief(ballot(1, 0), &[0, 1, 2]),
        reconfigured(ballot(2, 2), &[1, 2, 3]),
    ]);
    let step = n.on_match_reply(registered_with(0, b, history, Ballot::zero()));
    assert!(matches!(step, MatchStep::Completed { .. }), "{step:?}");
    assert_eq!(n.role(), NodeRole::Candidate);
}

/// The GC hole (review finding P1): a leader's garbage collection raises the
/// watermark above the last reconfiguration's record, so every history is
/// empty and no reply can *show* the effective configuration. The durable
/// scalar reports it anyway, and a candidate that rebooted to its bootstrap
/// belief still abandons and adopts it — without the scalar it would have
/// completed and been elected under the superseded configuration.
#[test]
fn a_stale_candidate_adopts_the_effective_configuration_after_gc() {
    let mut n = deployed_node(2, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    // The floor sits above the reconfiguration's ballot: its record is gone
    // and the history is empty, exactly as `advance_gc_watermark` leaves it.
    let floor = ballot(9, 0);
    let step = n.on_match_reply(registered_effective(
        0,
        b,
        BTreeMap::new(),
        floor,
        Some((ballot(4, 1), cfg(&[2, 3, 4]))),
    ));
    assert_eq!(
        step,
        MatchStep::StaleConfiguration {
            newest: ballot(4, 1)
        },
        "the reported scalar is the only witness left of the configuration in force"
    );
    assert_eq!(n.role(), NodeRole::Follower);
    assert_eq!(*n.acceptors(), cfg(&[2, 3, 4]));
    assert_eq!(n.acceptors_since(), ballot(4, 1));
    campaign(&mut n);
    let requests = drain_match_requests(&mut n);
    assert!(
        requests
            .iter()
            .all(|(_, r)| r.config == cfg(&[2, 3, 4]) && !r.purpose.is_reconfiguration())
    );
}

/// An honored reconfiguration may bind a node to an *older* ballot than the
/// one it holds: a reconfiguration campaign at 4 whose registration reached
/// a quorum after an ordinary leader at 9 was elected is still adopted by
/// every later campaign. The adoption rolls `acceptors_since` back to 4, and
/// the membership fence keeps the 9 it already recorded — the fence is the
/// maximum over every membership, never the current binding (seeds
/// 15760233921517076726 and 15615437002394963727: the sweep panicked on a
/// fence asserted to never run ahead of the configuration).
#[test]
fn an_honored_reconfiguration_may_bind_an_older_ballot_than_the_fence() {
    let mut n = deployed_node(2, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    // A leader at 9 taught this node the membership it holds.
    let learned = ballot(9, 1);
    n.step(Message::Prepare {
        reply_to: NodeId(1),
        ballot: learned,
        from_slot: Slot(0),
        config: Some(cfg(&[1, 2, 3])),
    });
    n.ready().advance();
    assert_eq!(n.acceptors_since(), learned);
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    // The quorum reports a reconfiguration at 4 that names this node.
    let step = n.on_match_reply(registered_effective(
        0,
        b,
        BTreeMap::new(),
        Ballot::zero(),
        Some((ballot(4, 1), cfg(&[2, 3, 4]))),
    ));
    assert_eq!(
        step,
        MatchStep::StaleConfiguration {
            newest: ballot(4, 1)
        }
    );
    assert_eq!(n.role(), NodeRole::Follower);
    assert_eq!(*n.acceptors(), cfg(&[2, 3, 4]));
    assert_eq!(n.acceptors_since(), ballot(4, 1));
    // The fence did not follow the binding down: this node is a member of
    // the adopted configuration, so it refuses to retire at any watermark.
    assert!(!n.may_retire(ballot(9, 1)));
    assert!(!n.may_retire(ballot(10, 1)));
}

/// A node's belief about its own membership comes only from what it heard,
/// so a member of `C_w` that never heard `C_w` — a spare pulled in by a
/// reconfiguration whose `Prepare` and beats never reached it, or a member
/// that rebooted to its bootstrap belief — is outside the configuration it
/// believes in force and its membership fence sits below the watermark. It
/// must still refuse a retirement at `w` (#165: the sweep shut down node 5
/// of `C_9` with its promise at round 3), until it hears the configuration
/// the floor kept and can tell whether that names it — exactly `C_w`,
/// neither an older belief nor a newer one.
#[test]
fn a_belief_older_than_the_floor_refuses_to_retire() {
    // Node 3, a spare of the bootstrap `{0, 1, 2}`. `C_9 = {1, 2, 3}` names
    // it, but nothing at 9 ever reached it.
    let mut n = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    let w = ballot(9, 1);
    assert!(!n.is_acceptor());
    assert_eq!(n.acceptors_since(), Ballot::zero());
    assert!(!n.may_retire(w), "a belief bound below the floor is stale");

    // The leader's beat at 9 carries `C_9`: now it knows it is a member.
    n.step(Message::Heartbeat {
        from: NodeId(1),
        ballot: w,
        commit: None,
        config: Some(cfg(&[1, 2, 3])),
        fence: None,
    });
    n.ready().advance();
    assert!(n.is_acceptor());
    assert!(!n.may_retire(w));

    // A spare that hears the configuration the floor kept, and is not in
    // it, may retire: its belief is now a fact about `C_w`.
    let mut spare = deployed_node(4, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    assert!(!spare.may_retire(w));
    spare.step(Message::Heartbeat {
        from: NodeId(1),
        ballot: w,
        commit: None,
        config: Some(cfg(&[1, 2, 3])),
        fence: None,
    });
    spare.ready().advance();
    assert!(!spare.is_acceptor());
    assert!(spare.may_retire(w));
    // A floor above what it heard is again beyond its knowledge.
    assert!(!spare.may_retire(ballot(10, 1)));

    // A belief bound *above* the floor is stale too: node 3, a member of
    // `C_9` that heard nothing at 9, hears a later configuration at 11 that
    // drops it. `C_9` still names it and the floor at 9 did not collect
    // `C_9`, so the old watermark proves nothing.
    let mut late = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    late.step(Message::Heartbeat {
        from: NodeId(1),
        ballot: ballot(11, 1),
        commit: None,
        config: Some(cfg(&[0, 1, 2])),
        fence: None,
    });
    late.ready().advance();
    assert!(!late.is_acceptor());
    assert!(!late.may_retire(w));
}

/// The reported scalar and the histories are folded together, maximum wins:
/// a matchmaker whose floor collected the newest record still reports it,
/// and a matchmaker still holding an older record does not outrank it.
#[test]
fn the_effective_configuration_is_the_maximum_of_the_shown_and_the_reported() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4], 3);
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    // Matchmaker 0 still holds the older change; matchmaker 1 collected
    // both records but reports the newer one as its durable scalar.
    let older = BTreeMap::from([reconfigured(ballot(2, 1), &[1, 2, 3])]);
    assert_eq!(
        n.on_match_reply(registered_effective(
            0,
            b,
            older,
            Ballot::zero(),
            Some((ballot(2, 1), cfg(&[1, 2, 3]))),
        )),
        MatchStep::Registered { remaining: 1 }
    );
    assert_eq!(
        n.on_match_reply(registered_effective(
            1,
            b,
            BTreeMap::new(),
            ballot(8, 0),
            Some((ballot(5, 2), cfg(&[2, 3, 4]))),
        )),
        MatchStep::StaleConfiguration {
            newest: ballot(5, 2)
        }
    );
    assert_eq!(*n.acceptors(), cfg(&[2, 3, 4]));
}

/// Beliefs are not facts: a candidate never reports an effective
/// configuration for an ordinary registration, so a matchmaker whose
/// registry only ever saw beliefs answers `None` and nothing moves.
#[test]
fn an_ordinary_registration_never_raises_the_effective_configuration() {
    let mut mms = registries(1);
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 1);
    campaign(&mut n);
    let requests = drain_match_requests(&mut n);
    for reply in matchmake(&mut mms, requests) {
        match reply.outcome {
            MatchOutcome::Registered { effective, .. } => assert_eq!(effective, None),
            MatchOutcome::Refused(r) => panic!("expected a registration, got {r:?}"),
            MatchOutcome::Probed { .. } => panic!("expected a registration, got a probe answer"),
        }
    }
    assert_eq!(mms[0].effective(J), None);
}

/// A full page of beliefs at rounds `1..=REGISTRY_PAGE`, all naming
/// `members` — what a matchmaker's first page looks like when its registry
/// does not fit in one answer.
fn full_page(members: &[u64]) -> BTreeMap<Ballot, Registration> {
    (1..=crate::contract::limits::REGISTRY_PAGE)
        .map(|round| belief(ballot(round as u64, 1), members))
        .collect()
}

/// Review finding P9: a registry that retains more than `REGISTRY_PAGE`
/// registrations answers in pages, and a page that is not the last counts
/// for nothing — the candidate re-asks with the cursor, and the quorum
/// closes only on a complete answer.
#[test]
fn a_paged_answer_counts_only_once_its_last_page_lands() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3], 1);
    campaign(&mut n);
    let b = n.ballot();
    assert_eq!(drain_match_requests(&mut n).len(), 1);
    let cursor = ballot(200, 1);
    let step = n.on_match_reply(registered_page(
        0,
        b,
        Ballot::zero(),
        full_page(&[0, 1, 3]),
        Some(cursor),
    ));
    assert_eq!(step, MatchStep::Paged { next: cursor });
    // The candidate asked that matchmaker for the rest, from the cursor,
    // and opened no Phase 1.
    let requests = drain_match_requests(&mut n);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, MatchmakerId(0));
    assert_eq!(requests[0].1.from_ballot, Some(cursor));
    assert!(
        prepares(&drain(&mut n)).is_empty(),
        "an incomplete answer opens no Phase 1"
    );
    // A page at the wrong cursor is not the one owed: ignored whole.
    assert_eq!(
        n.on_match_reply(registered_page(0, b, Ballot::zero(), BTreeMap::new(), None)),
        MatchStep::Ignored
    );
    // A page carrying a continuation but no registration is malformed: a
    // matchmaker's page size is at least one (#338).
    assert_eq!(
        n.on_match_reply(registered_page(
            0,
            b,
            cursor,
            BTreeMap::new(),
            Some(ballot(300, 1))
        )),
        MatchStep::Ignored
    );
    // The last page closes the answer, and the union spans both pages.
    let second = BTreeMap::from([belief(ballot(200, 1), &[1, 2, 3])]);
    let step = n.on_match_reply(registered_page(0, b, cursor, second, None));
    let MatchStep::Completed { prior, .. } = step else {
        panic!("the last page closes the quorum: {step:?}");
    };
    assert_eq!(prior, vec![cfg(&[0, 1, 3]), cfg(&[1, 2, 3])]);
}

/// The matchmaker side: a registry larger than one page answers a prefix
/// and names the cursor the rest starts at, and the cursor request answers
/// the remainder without registering anything twice. Every registration
/// names its own configuration, so none is left out.
#[test]
fn a_registry_larger_than_a_page_answers_a_prefix_and_a_cursor() {
    let mut mm = registries(1);
    let mm = &mut mm[0];
    let page = crate::contract::limits::REGISTRY_PAGE;
    for round in 1..=page + 1 {
        mm.step(MatchRequest::new(
            J,
            NodeId(1),
            ballot(round as u64, 1),
            cfg(&[0, 1, 2 + round as u64]),
            MatchmakerGeneration(0),
        ));
        mm.ready().advance();
    }
    let top = ballot(page as u64 + 5, 1);
    mm.step(MatchRequest::new(
        J,
        NodeId(1),
        top,
        cfg(&[0, 1, 2]),
        MatchmakerGeneration(0),
    ));
    let ready = mm.ready();
    let first = ready.replies()[0].outcome.clone();
    ready.advance();
    let MatchOutcome::Registered {
        from_ballot,
        history,
        next_from_ballot,
        ..
    } = first
    else {
        panic!("expected a registration");
    };
    assert_eq!(from_ballot, Ballot::zero());
    assert_eq!(history.len(), page);
    assert_eq!(next_from_ballot, Some(ballot(page as u64 + 1, 1)));
    // The cursor request re-answers idempotently, from where the first page
    // stopped, and registers nothing.
    mm.step(
        MatchRequest::new(J, NodeId(1), top, cfg(&[0, 1, 2]), MatchmakerGeneration(0))
            .from_page(next_from_ballot.expect("a cursor")),
    );
    let ready = mm.ready();
    assert!(ready.writes().is_empty(), "a cursor request writes nothing");
    let second = ready.replies()[0].outcome.clone();
    ready.advance();
    let MatchOutcome::Registered {
        from_ballot,
        history,
        next_from_ballot,
        ..
    } = second
    else {
        panic!("expected a registration");
    };
    assert_eq!(from_ballot, ballot(page as u64 + 1, 1));
    assert_eq!(history.len(), 1);
    assert_eq!(next_from_ballot, None);
}

/// The matchmaker side (#570 (election duel outlasts the recovery tail)):
/// a belief a later registration repeats is left out of the history, so a
/// registry that a leaderless cluster filled with failed campaigns of one
/// configuration answers in one page, even on a one-record page. Every
/// reconfiguration is kept, and each configuration keeps its highest record,
/// so the candidate's union and effective configuration are unchanged.
/// Before, the answer took one round trip per earlier campaign: witness
/// seed 16302262455227242419 on c739108, where every campaign's
/// matchmaking outlasted its rivals' capped election timeouts and no
/// `q1 = n` Phase 1 won through the recovery tail.
#[test]
fn a_page_leaves_out_a_belief_a_later_registration_repeats() {
    let mut mm = Matchmaker::new(
        &MatchmakerConfig {
            id: MatchmakerId(0),
            bootstrap: (0..1).map(MatchmakerId).collect(),
        },
        &crate::matchmaker::MemRegistry::default(),
        Limits {
            registry_page: 1,
            ..Limits::DEFAULT
        },
    );
    let mm = &mut mm;
    // Rounds 1..=40: one belief repeated by every failed campaign, a
    // reconfiguration to the same members at 20, and a second belief at 30.
    for round in 1..=40_u64 {
        let request = match round {
            20 => MatchRequest::reconfigure(
                J,
                NodeId(1),
                ballot(round, 1),
                cfg(&[0, 1, 2]),
                MatchmakerGeneration(0),
            ),
            30 => MatchRequest::new(
                J,
                NodeId(2),
                ballot(round, 2),
                cfg(&[1, 2, 3]),
                MatchmakerGeneration(0),
            ),
            _ => MatchRequest::new(
                J,
                NodeId(1),
                ballot(round, 1),
                cfg(&[0, 1, 2]),
                MatchmakerGeneration(0),
            ),
        };
        mm.step(request);
        mm.ready().advance();
    }
    let top = ballot(50, 3);
    let ask = |from: Option<Ballot>| {
        let request =
            MatchRequest::new(J, NodeId(3), top, cfg(&[0, 1, 2]), MatchmakerGeneration(0));
        match from {
            Some(cursor) => request.from_page(cursor),
            None => request,
        }
    };
    let answer = |mm: &mut Matchmaker, from: Option<Ballot>| {
        mm.step(ask(from));
        let ready = mm.ready();
        let outcome = ready.replies()[0].outcome.clone();
        ready.advance();
        let MatchOutcome::Registered {
            history,
            next_from_ballot,
            ..
        } = outcome
        else {
            panic!("expected a registration: {outcome:?}");
        };
        (history, next_from_ballot)
    };
    let mut from = None;
    let mut pages = Vec::new();
    loop {
        let (history, next) = answer(mm, from);
        assert!(history.len() <= 1, "a one-record page: {history:?}");
        pages.extend(history);
        from = next;
        if from.is_none() {
            break;
        }
    }
    // Three records over three round trips, not forty: the reconfiguration,
    // the second belief, and the highest record of the repeated belief.
    let kept: Vec<Ballot> = pages.iter().map(|(b, _)| *b).collect();
    assert_eq!(kept, vec![ballot(20, 1), ballot(30, 2), ballot(40, 1)]);
    assert!(
        pages[0].1.kind.is_reconfiguration(),
        "a reconfiguration is kept"
    );
    assert_eq!(pages[1].1.config, cfg(&[1, 2, 3]));
    assert_eq!(pages[2].1.config, cfg(&[0, 1, 2]));
    // A page asked from a cursor inside the repeated run leaves out the same
    // records: the criterion looks only forward.
    let (history, next) = answer(mm, Some(ballot(21, 1)));
    assert_eq!(
        history.keys().copied().collect::<Vec<_>>(),
        vec![ballot(30, 2)]
    );
    assert_eq!(next, Some(ballot(40, 1)));
}

/// A GC raise between two pages of one answer collects the cursor itself:
/// the matchmaker's next page starts at its raised watermark, *above* the
/// cursor the candidate asked for, and reports that watermark beside it.
/// The candidate takes the page — everything it skipped sits below a floor
/// the fold maxes into the closing watermark — and the campaign completes
/// with the union filtered at the raised floor. Refusing it wedged the
/// campaign at that matchmaker for good: `tick` never abandons a pending
/// matchmaking, so the candidate re-asked from the collected cursor forever
/// (the handover model's claim 4 found it on seed 8 once its tail raised
/// floors between pages). A page above the cursor that does *not* start at
/// the sender's watermark is still not the one owed.
#[test]
fn a_page_that_starts_at_a_raised_watermark_above_the_cursor_is_taken() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3], 1);
    campaign(&mut n);
    let b = n.ballot();
    let cursor = ballot(200, 1);
    assert_eq!(
        n.on_match_reply(registered_page(
            0,
            b,
            Ballot::zero(),
            full_page(&[0, 1, 3]),
            Some(cursor),
        )),
        MatchStep::Paged { next: cursor }
    );
    drain_match_requests(&mut n);
    // Above the cursor but not at the reported floor: a page of some other
    // shape, ignored whole.
    let raised = ballot(300, 1);
    let second = BTreeMap::from([belief(ballot(301, 1), &[1, 2, 3])]);
    assert_eq!(
        n.on_match_reply(registered_page(0, b, raised, second.clone(), None)),
        MatchStep::Ignored
    );
    // The same page at the floor it reports: taken, and it closes the answer.
    let mut reply = registered_page(0, b, raised, second, None);
    if let MatchOutcome::Registered { gc_watermark, .. } = &mut reply.outcome {
        *gc_watermark = raised;
    }
    let step = n.on_match_reply(reply);
    let MatchStep::Completed {
        prior, watermark, ..
    } = step
    else {
        panic!("the page at the raised floor closes the quorum: {step:?}");
    };
    assert_eq!(
        watermark, raised,
        "the raised floor is the closing watermark"
    );
    assert_eq!(
        prior,
        vec![cfg(&[1, 2, 3])],
        "the first page's configurations sit below the raised floor"
    );
}

/// The same wedge end to end against a real registry: a registry larger
/// than a page answers a prefix and a cursor, its floor rises over that
/// cursor before the re-ask lands, and the re-asked page starts at the new
/// floor. The candidate's campaign completes anyway. Every registration
/// names its own configuration, so none is left out of a page.
#[test]
fn a_floor_raised_over_the_cursor_between_two_pages_still_completes_the_campaign() {
    let page = crate::contract::limits::REGISTRY_PAGE as u64;
    let mut mms = registries(1);
    for round in 1..=page + 1 {
        mms[0].step(MatchRequest::new(
            J,
            NodeId(1),
            ballot(round, 1),
            cfg(&[0, 1, 2 + round]),
            MatchmakerGeneration(0),
        ));
        mms[0].ready().advance();
    }
    // The candidate's first campaign is stale against that registry; the
    // refusal raises its round floor and the next one opens above it.
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3], 1);
    campaign(&mut n);
    assert!(matches!(
        run_matchmaking(&mut n, &mut mms).as_slice(),
        [MatchStep::Refused(MatchRefusal::Stale { .. })]
    ));
    campaign(&mut n);
    let b = n.ballot();
    assert!(b > ballot(page + 1, 1));
    let cursor = ballot(page + 1, 1);
    assert_eq!(
        run_matchmaking(&mut n, &mut mms),
        vec![MatchStep::Paged { next: cursor }]
    );
    // A leader's GC lands between the two pages, strictly above the cursor
    // and below the campaign.
    let raised = ballot(page + 1, 5);
    assert!(raised > cursor && raised < b);
    assert_eq!(
        mms[0].advance_gc_watermark(J, MatchmakerGeneration(0), raised),
        crate::matchmaker::GcOutcome::Raised
    );
    mms[0].ready().advance();
    // The re-ask from the cursor is answered from the raised floor, and the
    // candidate closes on it: nothing below the floor is owed to Phase 1.
    let steps = run_matchmaking(&mut n, &mut mms);
    let [
        MatchStep::Completed {
            prior, watermark, ..
        },
    ] = steps.as_slice()
    else {
        panic!("the campaign completes on the page at the raised floor: {steps:?}");
    };
    assert_eq!(*watermark, raised);
    assert!(
        prior.is_empty(),
        "every prior configuration sits below the raised floor"
    );
}

/// A live campaign's promise sits at its ballot (`assert_invariants`): a
/// candidate still in its matchmaking phase that learns a slot chosen at a
/// higher ballot raises its promise there, so its campaign closes, as a
/// higher `Prepare` closes it. Its matchmaker quorum answering afterwards
/// opens no Phase 1 (which would run at a ballot its own promise refuses),
/// and its next campaign opens above the learned ballot.
#[test]
fn a_slot_chosen_above_a_matchmaking_campaign_closes_it() {
    let mut n = deployed_node(1, &[0, 1, 2], &[0, 1, 2], 3);
    let mut mms = registries(3);
    campaign(&mut n);
    let b = n.ballot();
    assert_eq!(n.role(), NodeRole::Candidate);
    assert!(n.matchmaking_pending());
    let requests = drain_match_requests(&mut n);
    let replies = matchmake(&mut mms, requests);
    // Another proposer chose slot 0 at a ballot above the campaign's.
    let above = ballot(b.round + 1, 2);
    n.step(Message::Commit {
        from: Party::Node(NodeId(2)),
        ballot: above,
        slot: Slot(0),
        command: ucmd(7, 7, 2),
    });
    assert_eq!(chosen_at(&n, 0), Some(val(2)), "the slot is learned chosen");
    assert_eq!(
        n.hard_state().max_promised_ballot,
        above,
        "the promise follows it"
    );
    assert_eq!(n.role(), NodeRole::Follower, "the campaign is closed");
    assert!(!n.matchmaking_pending());
    // The matchmaker quorum's answers arrive late: they fold into nothing.
    for reply in replies {
        assert_eq!(n.on_match_reply(reply), MatchStep::Ignored);
    }
    assert!(prepares(&drain(&mut n)).is_empty(), "no Phase 1 opens");
    // The next campaign opens above the ballot the promise learned.
    campaign(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate);
    assert!(n.ballot() > above);
    assert_eq!(n.hard_state().max_promised_ballot, n.ballot());
}

/// Only a leader names the configurations in force (#760 (moonpool owns
/// fault placement)): a follower and a candidate do not know them.
#[test]
fn only_a_leader_names_its_configurations_in_force() {
    let mut n = deployed_node(2, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    assert!(n.configs_in_force().is_none());
    campaign(&mut n);
    assert!(n.configs_in_force().is_none());
}

// ---- refusals, foreign replies and the read-only role ------------------------

/// A refusal of `ballot`'s registration from `mm` at generation 0.
fn refused(mm: u64, ballot: Ballot, refusal: MatchRefusal) -> MatchReply {
    registered_reply(mm, ballot, MatchOutcome::Refused(refusal))
}

/// A three-matchmaker node of `{0, 1, 2}` with its campaign open (its
/// requests drained). Returns the node and the campaign ballot.
fn campaigning() -> (ColocatedNode, Ballot) {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    campaign(&mut n);
    drain_match_requests(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate);
    let b = n.ballot();
    (n, b)
}

/// The matchmaker set `{0, 1, 2}` at `generation`.
fn set_at(generation: u64) -> crate::membership::MatchmakerSet {
    crate::membership::MatchmakerSet::new(
        MatchmakerGeneration(generation),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    )
}

/// The tag of the membership probe open on `n`.
fn probe_tag(n: &ColocatedNode) -> Ballot {
    n.membership_probe()
        .map(crate::matchmaking::MembershipProbe::ballot)
        .expect("a bootstrap belief probes first")
}

/// A reply from matchmaker 0 to node `to` at `tag`, generation 0.
fn reply_at(to: u64, tag: Ballot, outcome: MatchOutcome) -> MatchReply {
    MatchReply {
        journal: J,
        matchmaker: MatchmakerId(0),
        to: NodeId(to),
        ballot: tag,
        generation: MatchmakerGeneration(0),
        outcome,
    }
}

/// A `BelowWatermark` refusal stops the campaign. The next campaign opens
/// above the round of the watermark, not one round above its own.
#[test]
fn a_below_watermark_refusal_lifts_the_next_campaign_above_the_watermark() {
    let (mut n, b) = campaigning();
    let refusal = MatchRefusal::BelowWatermark {
        watermark: ballot(20, 1),
    };
    let step = n.on_match_reply(refused(0, b, refusal.clone()));
    assert_eq!(step, MatchStep::Refused(refusal));
    assert_eq!(n.role(), NodeRole::Follower);
    assert!(!n.matchmaking_pending());
    assert!(prepares(&drain(&mut n)).is_empty());
    campaign(&mut n);
    assert_eq!(n.ballot().round, 21, "one above the watermark round (20)");
}

/// A refusal that names no successor set and no ballot (`Stopped` with no
/// successor, `Inactive`, or a `Generation` that is not later) stops the
/// campaign. It does not change the believed set and does not lift the
/// next round past one above the last.
#[test]
fn a_refusal_without_a_successor_keeps_the_set_and_the_round_floor() {
    for refusal in [
        MatchRefusal::Stopped { successor: None },
        MatchRefusal::Inactive,
        MatchRefusal::Generation { current: set_at(0) },
    ] {
        let (mut n, b) = campaigning();
        let step = n.on_match_reply(refused(1, b, refusal.clone()));
        assert_eq!(step, MatchStep::Refused(refusal.clone()), "{refusal:?}");
        assert_eq!(n.role(), NodeRole::Follower, "{refusal:?}");
        assert!(!n.matchmaking_pending(), "{refusal:?}");
        assert_eq!(
            n.matchmaker_set().map(|s| s.generation),
            Some(MatchmakerGeneration(0)),
            "{refusal:?}"
        );
        campaign(&mut n);
        assert_eq!(n.ballot().round, b.round + 1, "{refusal:?}");
    }
}

/// A refusal that names a later matchmaker set (`Stopped` with a
/// successor, or `Generation` from a later generation) stops the campaign
/// and makes that set the believed set. The next campaign asks the new
/// generation.
#[test]
fn a_refusal_naming_a_later_set_supersedes_the_believed_set() {
    for refusal in [
        MatchRefusal::Stopped {
            successor: Some(set_at(1)),
        },
        MatchRefusal::Generation { current: set_at(1) },
    ] {
        let (mut n, b) = campaigning();
        let step = n.on_match_reply(refused(2, b, refusal.clone()));
        assert_eq!(
            step,
            MatchStep::Superseded { set: set_at(1) },
            "{refusal:?}"
        );
        assert_eq!(n.role(), NodeRole::Follower, "{refusal:?}");
        assert_eq!(n.matchmaker_set(), Some(&set_at(1)), "{refusal:?}");
        campaign(&mut n);
        let requests = drain_match_requests(&mut n);
        assert_eq!(requests.len(), 3, "{refusal:?}");
        assert!(
            requests
                .iter()
                .all(|(_, r)| r.generation == MatchmakerGeneration(1)),
            "{refusal:?}"
        );
    }
}

/// A successor set that names a matchmaker outside the pool is not a set
/// this node can reach. The campaign stops as a plain refusal, and the
/// believed set does not change.
#[test]
fn a_successor_set_outside_the_pool_is_a_plain_refusal() {
    let (mut n, b) = campaigning();
    let outside = crate::membership::MatchmakerSet::new(
        MatchmakerGeneration(1),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(9)],
    );
    let refusal = MatchRefusal::Stopped {
        successor: Some(outside),
    };
    let step = n.on_match_reply(refused(0, b, refusal.clone()));
    assert_eq!(step, MatchStep::Refused(refusal));
    assert_eq!(n.role(), NodeRole::Follower);
    assert_eq!(
        n.matchmaker_set().map(|s| s.generation),
        Some(MatchmakerGeneration(0))
    );
}

/// A refusal of an open membership probe closes the probe. The node stays
/// a follower and opens no campaign.
#[test]
fn a_refused_membership_probe_closes_the_probe() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    fire_election(&mut n);
    let tag = probe_tag(&n);
    drain_match_requests(&mut n);
    let refusal = MatchRefusal::Stale {
        highest: ballot(4, 2),
    };
    let reply = reply_at(0, tag, MatchOutcome::Refused(refusal.clone()));
    assert_eq!(n.on_match_reply(reply), MatchStep::Refused(refusal));
    assert!(n.membership_probe().is_none());
    assert!(!n.matchmaking_pending());
    assert_eq!(n.role(), NodeRole::Follower);
    assert!(drain_match_requests(&mut n).is_empty());
}

/// A reply for another journal, for another generation, of the wrong kind,
/// or to a node with no matchmakers changes nothing. The open campaign
/// still needs the same number of registrations.
#[test]
fn a_reply_the_campaign_did_not_ask_for_is_ignored() {
    let (mut n, b) = campaigning();
    let good = registered(0, b, &[], Ballot::zero());
    let mut other_journal = good.clone();
    other_journal.journal = crate::journal::JournalId(7);
    let mut other_generation = good.clone();
    other_generation.generation = MatchmakerGeneration(1);
    let probe_answer = registered_reply(1, b, MatchOutcome::Probed { effective: None });
    for reply in [other_journal, other_generation, probe_answer] {
        assert_eq!(
            n.on_match_reply(reply.clone()),
            MatchStep::Ignored,
            "{reply:?}"
        );
        assert!(n.matchmaking_pending());
    }
    assert_eq!(
        n.on_match_reply(good.clone()),
        MatchStep::Registered { remaining: 1 },
        "the ignored replies counted for nothing"
    );

    // Plain Multi-Paxos names no matchmakers and answers no reply.
    let mut plain = node(0, &[0, 1, 2]);
    assert_eq!(plain.on_match_reply(good), MatchStep::Ignored);
    assert!(!plain.matchmaking_pending());
}

/// At an open probe's tag, only a probe answer or a refusal counts. A
/// registration page there answers nothing the node asked.
#[test]
fn a_registration_page_at_a_probe_tag_is_ignored() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    fire_election(&mut n);
    let tag = probe_tag(&n);
    let page = reply_at(
        0,
        tag,
        MatchOutcome::Registered {
            from_ballot: Ballot::zero(),
            history: BTreeMap::new(),
            next_from_ballot: None,
            gc_watermark: Ballot::zero(),
            effective: None,
        },
    );
    assert_eq!(n.on_match_reply(page), MatchStep::Ignored);
    let probe = n.membership_probe().expect("the probe stays open");
    assert_eq!(probe.answered_by(), 0, "the page counted for nothing");
}

/// After a probe closed with this node outside, its tag accepts only late
/// probe answers. A refusal or a page at that tag is ignored, and a later
/// late answer still moves the belief.
#[test]
fn a_closed_probe_tag_takes_only_late_probe_answers() {
    let mut n = deployed_node(3, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    fire_election(&mut n);
    let tag = probe_tag(&n);
    let closed = n.on_match_reply(reply_at(3, tag, MatchOutcome::Probed { effective: None }));
    assert!(
        matches!(closed, MatchStep::ProbeClosed { member: false, .. }),
        "{closed:?}"
    );
    let refusal = reply_at(3, tag, MatchOutcome::Refused(MatchRefusal::Inactive));
    assert_eq!(n.on_match_reply(refusal), MatchStep::Ignored);
    let page = reply_at(
        3,
        tag,
        MatchOutcome::Registered {
            from_ballot: Ballot::zero(),
            history: BTreeMap::new(),
            next_from_ballot: None,
            gc_watermark: Ballot::zero(),
            effective: None,
        },
    );
    assert_eq!(n.on_match_reply(page), MatchStep::Ignored);
    assert_eq!(n.role(), NodeRole::Follower);
    assert!(!n.is_acceptor());

    let late = reply_at(
        3,
        tag,
        MatchOutcome::Probed {
            effective: Some((ballot(5, 1), cfg(&[1, 2, 3]))),
        },
    );
    let step = n.on_match_reply(late);
    assert!(
        matches!(step, MatchStep::ProbeLate { member: true, .. }),
        "{step:?}"
    );
    assert_eq!(*n.acceptors(), cfg(&[1, 2, 3]));
    assert_eq!(n.role(), NodeRole::Candidate, "a member campaigns at once");
}

/// The node shows its open matchmaking phase, and only while it is open.
/// The phase runs at the node's ballot, registers the believed
/// configuration, and counts each registration.
#[test]
fn the_matchmaking_role_shows_the_open_phase_only() {
    let plain = node(0, &[0, 1, 2]);
    assert!(plain.matchmaking_role().is_none());

    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    let set = n
        .matchmaker_set()
        .cloned()
        .expect("a matchmaker deployment");
    assert!(n.matchmaking_role().is_none(), "no campaign yet");
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    let m = n.matchmaking_role().expect("the campaign matchmakes");
    assert_eq!(m.ballot(), b);
    assert_eq!(*m.config(), cfg(&[0, 1, 2]));
    assert_eq!(m.registered(), 0);
    assert_eq!(m.remaining(&set), 2);

    n.on_match_reply(registered(0, b, &[], Ballot::zero()));
    let m = n.matchmaking_role().expect("one reply is not a quorum");
    assert_eq!(m.registered(), 1);
    assert_eq!(m.remaining(&set), 1);

    n.on_match_reply(registered(1, b, &[], Ballot::zero()));
    assert!(
        n.matchmaking_role().is_none(),
        "the quorum closed the phase"
    );
}

/// A node that won a leadership under `{0, 1, 2}`, with one matchmaker.
/// Returns the node and the won ballot.
fn won_leader() -> (ColocatedNode, Ballot) {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2, 3, 4], 1);
    open_phase1(&mut n, &[]);
    assert!(n.is_leader());
    let won = n.ballot();
    (n, won)
}

/// A reconfiguration at or below a leadership this node won, to the
/// configuration that leadership ran, is in force. A later campaign that
/// registers another belief adopts it and stops.
#[test]
fn a_campaign_adopts_the_reconfiguration_its_won_leadership_ran() {
    let (mut n, won) = won_leader();
    // A higher Prepare teaches the node an older candidate's belief.
    n.step(Message::Prepare {
        reply_to: NodeId(1),
        ballot: ballot(won.round + 1, 1),
        from_slot: Slot(0),
        config: Some(cfg(&[0, 1, 3])),
    });
    n.ready().advance();
    assert_eq!(*n.acceptors(), cfg(&[0, 1, 3]));
    campaign(&mut n);
    let b = n.ballot();
    drain_match_requests(&mut n);
    let history = BTreeMap::from([reconfigured(won, &[0, 1, 2])]);
    let step = n.on_match_reply(registered_with(0, b, history, Ballot::zero()));
    assert_eq!(step, MatchStep::StaleConfiguration { newest: won });
    assert_eq!(n.role(), NodeRole::Follower);
    assert_eq!(*n.acceptors(), cfg(&[0, 1, 2]), "the honored one is back");
}

/// A reconfiguration below a leadership this node won, to a configuration
/// that leadership did not run, was missed by its quorum and is not in
/// force. The campaign keeps its belief, and Phase 1 still covers the
/// missed configuration.
#[test]
fn a_campaign_keeps_its_belief_over_a_reconfiguration_below_its_won_leadership() {
    let (mut n, won) = won_leader();
    assert!(won.round >= 2, "room for a ballot below the won one");
    n.step_down();
    assert_eq!(n.role(), NodeRole::Follower);
    campaign(&mut n);
    let b = n.ballot();
    assert!(b > won);
    drain_match_requests(&mut n);
    let missed = ballot(won.round - 1, 4);
    let history = BTreeMap::from([reconfigured(missed, &[0, 1, 3])]);
    let step = n.on_match_reply(registered_with(0, b, history, Ballot::zero()));
    let MatchStep::Completed { prior, .. } = step else {
        panic!("the belief stands: {step:?}");
    };
    assert!(prior.contains(&cfg(&[0, 1, 3])), "Phase 1 covers it");
    assert_eq!(*n.acceptors(), cfg(&[0, 1, 2]));
    let msgs = drain(&mut n);
    assert!(msgs.iter().any(|(_, m)| matches!(
        m,
        Message::Prepare { config: Some(c), .. } if *c == cfg(&[0, 1, 2])
    )));
}
