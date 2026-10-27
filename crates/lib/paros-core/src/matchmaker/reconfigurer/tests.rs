use super::*;
use crate::matchmaker::MatchmakerPhase;
use crate::matchmaker::{MatchRequest, Matchmaker, MatchmakerConfig, MemRegistry};
use crate::membership::{AcceptorConfig, MatchmakerGeneration, QuorumSystem};

const J: JournalId = JournalId(1);

fn ids(v: &[u64]) -> Vec<MatchmakerId> {
    v.iter().copied().map(MatchmakerId).collect()
}

/// A pool of matchmakers, `0..n`, bootstrapped on `bootstrap`.
fn pool(n: u64, bootstrap: &[u64]) -> Vec<Matchmaker> {
    (0..n)
        .map(|i| {
            Matchmaker::new(
                &MatchmakerConfig {
                    id: MatchmakerId(i),
                    bootstrap: ids(bootstrap),
                },
                &MemRegistry::default(),
                Limits::DEFAULT,
            )
        })
        .collect()
}

/// One driver beat: deliver every queued request, fold the replies, then
/// close a freeze whose quorum answered — the shape the driver has.
fn exchange(
    r: &mut MatchmakerReconfigurer,
    pool: &mut [Matchmaker],
    drop: &[u64],
) -> Vec<ReconfigurerStep> {
    let steps = deliver(r, pool, drop);
    r.close_stop();
    steps
}

/// Deliver every queued request of `r` to the pool and fold the replies,
/// returning the steps, *without* the driver's closing beat. `drop`
/// names matchmakers that never answer.
fn deliver(
    r: &mut MatchmakerReconfigurer,
    pool: &mut [Matchmaker],
    drop: &[u64],
) -> Vec<ReconfigurerStep> {
    route(r, pool, drop, &[])
}

fn set(g: u64, m: &[u64]) -> MatchmakerSet {
    MatchmakerSet::new(MatchmakerGeneration(g), ids(m))
}

/// Like [`deliver`], but every matchmaker *does* step its request and
/// only the replies of `mute` are lost on the way back — the shape of a
/// lost ack, as opposed to a lost request.
fn deliver_muting(
    r: &mut MatchmakerReconfigurer,
    pool: &mut [Matchmaker],
    mute: &[u64],
) -> Vec<ReconfigurerStep> {
    route(r, pool, &[], mute)
}

/// [`deliver`] and [`deliver_muting`] in one: the requests to `drop`
/// are lost on the way out, the replies of `mute` on the way back.
fn route(
    r: &mut MatchmakerReconfigurer,
    pool: &mut [Matchmaker],
    drop: &[u64],
    mute: &[u64],
) -> Vec<ReconfigurerStep> {
    let mut steps = Vec::new();
    let ready = r.ready();
    let requests = ready.requests().to_vec();
    ready.advance();
    for (to, request) in requests {
        if drop.contains(&to.0) {
            continue;
        }
        let mm = &mut pool[usize::try_from(to.0).expect("index")];
        mm.step_reconfigure(request);
        let ready = mm.ready();
        let replies = ready.reconfigure_replies().to_vec();
        ready.advance();
        if mute.contains(&to.0) {
            continue;
        }
        for reply in replies {
            steps.push(r.on_reply(reply));
        }
    }
    steps
}

/// A `Learned` lost on its way back from a member that *activated* the
/// successor must not end the handover: the re-sent `Chosen` finds that
/// member already at the successor's generation, and it answers
/// `Learned { activated: false, at: g + 1 }` again — counted toward the
/// successor's quorum, so the publication reaches `Done`. Before
/// `Matchmaker::on_chosen` had that arm the member fell through to a
/// refusal naming its new `current`, which the reconfigurer read as
/// `Superseded` and aborted: one lost ack from an activated member and
/// `Done` was unreachable.
#[test]
fn a_lost_learned_from_an_activated_member_is_recovered_by_the_re_sent_chosen() {
    let mut pool = pool(4, &[0, 1, 2]);
    let mut r = MatchmakerReconfigurer::new(NodeId(7), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
        .expect("start");
    exchange(&mut r, &mut pool, &[]);
    exchange(&mut r, &mut pool, &[]);
    exchange(&mut r, &mut pool, &[]);
    let steps = exchange(&mut r, &mut pool, &[]);
    assert!(
        steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Chosen { .. }))
    );
    assert!(matches!(r.phase(), ReconfigurerPhase::Publishing { .. }));
    // Every matchmaker learns the decision, but the acks of two successor
    // members — one shared with the old set, one spare — are lost. The
    // old set's quorum is met (1 and 2 answered); the successor's is not.
    let steps = deliver_muting(&mut r, &mut pool, &[0, 3]);
    assert_eq!(
        steps,
        vec![
            ReconfigurerStep::Published {
                old_remaining: 1,
                new_remaining: 1,
            },
            ReconfigurerStep::Published {
                old_remaining: 0,
                new_remaining: 1,
            },
        ]
    );
    for i in [0, 1, 3] {
        assert_eq!(pool[i].phase(), MatchmakerPhase::Active);
        assert_eq!(*pool[i].set(), set(1, &[0, 1, 3]));
    }
    assert!(
        r.is_busy(),
        "a publication short of the successor's quorum stays open"
    );
    // The driver's re-send reaches the two activated members again.
    r.resend();
    let steps = deliver(&mut r, &mut pool, &[]);
    assert!(
        steps.iter().any(|s| matches!(s, ReconfigurerStep::Done { successor } if *successor == set(1, &[0, 1, 3]))),
        "the re-sent Chosen is acknowledged by the activated members: {steps:?}"
    );
    assert!(
        !steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Superseded { .. })),
        "an activated member is not a superseding generation"
    );
    assert!(!r.is_busy());
}

/// The happy path: stop a quorum, bootstrap all of the successor, decide,
/// publish — every matchmaker of both generations ends where it should.
#[test]
fn a_handover_replaces_one_matchmaker_by_a_spare() {
    let mut pool = pool(4, &[0, 1, 2]);
    let mut r = MatchmakerReconfigurer::new(NodeId(7), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
        .expect("start");
    assert!(r.is_busy());
    let steps = exchange(&mut r, &mut pool, &[]);
    // Every member answers the freeze; the quorum-completing ack does
    // not close it, the beat that follows does — so the third answer is
    // still counted, not dropped on a phase that already moved on.
    assert_eq!(
        steps,
        vec![
            ReconfigurerStep::Stopped { remaining: 1 },
            ReconfigurerStep::Stopped { remaining: 0 },
            ReconfigurerStep::Stopped { remaining: 0 },
        ]
    );
    assert!(matches!(r.phase(), ReconfigurerPhase::Bootstrapping { .. }));
    let steps = exchange(&mut r, &mut pool, &[]);
    assert!(matches!(
        steps.last(),
        Some(ReconfigurerStep::Deciding { .. })
    ));
    let steps = exchange(&mut r, &mut pool, &[]);
    assert!(
        steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Proposing { adopted: false, .. }))
    );
    let steps = exchange(&mut r, &mut pool, &[]);
    assert!(steps.iter().any(
        |s| matches!(s, ReconfigurerStep::Chosen { successor } if *successor == set(1, &[0, 1, 3]))
    ));
    let steps = exchange(&mut r, &mut pool, &[]);
    assert!(
        steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Done { .. }))
    );
    assert!(!r.is_busy());
    for i in [0, 1, 3] {
        assert_eq!(pool[i].phase(), MatchmakerPhase::Active);
        assert_eq!(*pool[i].set(), set(1, &[0, 1, 3]));
    }
    assert_eq!(pool[2].phase(), MatchmakerPhase::Stopped);
    assert_eq!(pool[2].successor(), Some(&set(1, &[0, 1, 3])));
}

/// Review finding P4, at the reconfigurer: the stall clock moves on
/// progress and only on progress. A re-sent `Stop` is answered again by
/// a matchmaker that already froze — the freeze is idempotent — and that
/// duplicate must be `Ignored`, or a phase whose remaining members are
/// all dead resets its clock forever and is never abandoned. The
/// counted-but-short direction is the same fold's other half: it reports
/// how many acks are still missing.
#[test]
fn a_duplicate_ack_is_ignored_and_never_resets_the_stall_clock() {
    let mut pool = pool(5, &[0, 1, 2, 3, 4]);
    let mut r = MatchmakerReconfigurer::new(NodeId(7), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2, 3, 4]), ids(&[0, 1, 2]))
        .expect("start");
    // One freeze: counted, three short of the quorum of five.
    let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4]);
    assert_eq!(steps, vec![ReconfigurerStep::Stopped { remaining: 2 }]);
    assert_eq!(r.stalled_for(), 0);
    r.tick();
    r.tick();
    assert_eq!(r.stalled_for(), 2);
    // The same matchmaker answers a re-sent `Stop`: idempotent at the
    // registry, and nothing at all here.
    r.resend();
    let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4]);
    assert!(
        steps.is_empty(),
        "a frozen member is not re-asked: {steps:?}"
    );
    r.on_reply(ReconfigureReply::Stopped {
        matchmaker: MatchmakerId(0),
        generation: MatchmakerGeneration(0),
        after: None,
        next: None,
        registries: BTreeMap::new(),
        successor: None,
        decree_promised: Ballot::zero(),
    });
    assert_eq!(r.stalled_for(), 2, "a duplicate freeze ack is not progress");
    // A second, different member is: the clock restarts.
    r.resend();
    let steps = exchange(&mut r, &mut pool, &[0, 2, 3, 4]);
    assert_eq!(steps, vec![ReconfigurerStep::Stopped { remaining: 1 }]);
    assert_eq!(r.stalled_for(), 0);
}

/// The decree's own folds, through the reconfigurer: a promise and a
/// vote that count toward a quorum still short are `Promised` /
/// `Accepted` with what is missing, and the publication reports the
/// learners each set still needs.
#[test]
fn a_short_decree_quorum_reports_what_is_missing() {
    let mut pool = pool(6, &[0, 1, 2, 3, 4]);
    let mut r = MatchmakerReconfigurer::new(NodeId(7), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2, 3, 4]), ids(&[0, 1, 5]))
        .expect("start");
    // Freeze a quorum, bootstrap every proposed member, open the decree.
    while !matches!(r.phase(), ReconfigurerPhase::Deciding { .. }) {
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(!steps.is_empty(), "the handover stalled: {:?}", r.phase());
    }
    // Phase 1 over five acceptors: the first two promises are short.
    let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4]);
    assert_eq!(steps, vec![ReconfigurerStep::Promised { remaining: 2 }]);
    r.resend();
    let steps = exchange(&mut r, &mut pool, &[0, 2, 3, 4]);
    assert_eq!(steps, vec![ReconfigurerStep::Promised { remaining: 1 }]);
    r.resend();
    let steps = exchange(&mut r, &mut pool, &[0, 1, 3, 4]);
    assert!(matches!(
        steps.as_slice(),
        [ReconfigurerStep::Proposing { .. }]
    ));
    // Phase 2, the same shape.
    let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4]);
    assert_eq!(steps, vec![ReconfigurerStep::Accepted { remaining: 2 }]);
    r.resend();
    let steps = exchange(&mut r, &mut pool, &[0, 2, 3, 4]);
    assert_eq!(steps, vec![ReconfigurerStep::Accepted { remaining: 1 }]);
    r.resend();
    let steps = exchange(&mut r, &mut pool, &[0, 1, 3, 4]);
    assert!(matches!(
        steps.as_slice(),
        [ReconfigurerStep::Chosen { .. }]
    ));
    // Publishing: one learner of each set at a time.
    let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4, 5]);
    assert_eq!(
        steps,
        vec![ReconfigurerStep::Published {
            old_remaining: 2,
            new_remaining: 1
        }]
    );
}

/// Review finding S2c: the reconstruction counts the ballots two frozen
/// registries disagreed on. The union must pick one and the successor
/// generation then holds it durably, so a silent pick would narrow the
/// ledger forever — the count is what makes the "one registration per
/// ballot" claim checkable at the boundary that cannot keep both.
#[test]
fn a_reconstruction_counts_same_ballot_disagreements() {
    let mut disagreeing = pool(3, &[0, 1, 2]);
    // Two matchmakers register the *same* ballot with different
    // configurations — impossible with one proposer per ballot, which
    // is exactly why the audit wants to hear about it.
    let cfg = |n: u64| AcceptorConfig::new(vec![NodeId(n), NodeId(n + 1)], QuorumSystem::Majority);
    let b = Ballot {
        round: 1,
        node: NodeId(9),
    };
    for (i, members) in [0_u64, 5].into_iter().enumerate() {
        disagreeing[i].step(MatchRequest::new(
            J,
            NodeId(9),
            b,
            cfg(members),
            MatchmakerGeneration(0),
        ));
        disagreeing[i].ready().advance();
    }
    let mut r = MatchmakerReconfigurer::new(NodeId(7), Limits::DEFAULT);
    r.finish(&set(0, &[0, 1, 2])).expect("finish");
    // Freeze both disagreeing members before the beat closes the stop.
    deliver(&mut r, &mut disagreeing, &[2]);
    let reconstruction = r.close_stop().expect("the quorum closes");
    assert_eq!(reconstruction.disagreements, 1);
    assert_eq!(reconstruction.bootstrap.registries[&J].history.len(), 1);
    // A quorum that agrees counts nothing.
    let mut agreeing = pool(3, &[0, 1, 2]);
    for mm in &mut agreeing[..2] {
        mm.step(MatchRequest::new(
            J,
            NodeId(9),
            b,
            cfg(0),
            MatchmakerGeneration(0),
        ));
        mm.ready().advance();
    }
    let mut r = MatchmakerReconfigurer::new(NodeId(7), Limits::DEFAULT);
    r.finish(&set(0, &[0, 1, 2])).expect("finish");
    deliver(&mut r, &mut agreeing, &[2]);
    assert_eq!(r.close_stop().expect("closes").disagreements, 0);
}

/// Review finding P7b: a member of the *successor* that only recorded
/// the chain link is not serving the new generation, and counting it
/// let a publication finish while no quorum of the new set answered for
/// it. The generation the learner reports is what decides — which also
/// makes the count idempotent under a re-sent `Chosen`.
#[test]
fn a_learner_that_only_recorded_does_not_count_for_the_successor() {
    let mut pool = pool(4, &[0, 1, 2]);
    let mut r = MatchmakerReconfigurer::new(NodeId(7), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
        .expect("start");
    while !matches!(r.phase(), ReconfigurerPhase::Publishing { .. }) {
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(!steps.is_empty(), "the handover stalled: {:?}", r.phase());
        r.resend();
    }
    let learned = |m: u64, at: u64| ReconfigureReply::Learned {
        matchmaker: MatchmakerId(m),
        generation: MatchmakerGeneration(0),
        activated: at > 0,
        at: MatchmakerGeneration(at),
    };
    // Matchmaker 0 is in both sets but still at generation 0: it counts
    // for the generation it is leaving, never for the one it has not
    // joined.
    assert_eq!(
        r.on_reply(learned(0, 0)),
        ReconfigurerStep::Published {
            old_remaining: 1,
            new_remaining: 2
        }
    );
    assert_eq!(
        r.on_reply(learned(1, 1)),
        ReconfigurerStep::Published {
            old_remaining: 0,
            new_remaining: 1
        }
    );
    assert_eq!(
        r.on_reply(learned(1, 1)),
        ReconfigurerStep::Ignored,
        "a re-sent Chosen a member answers again changes nothing"
    );
    // Matchmaker 0 activates and answers again: now it counts.
    assert_eq!(
        r.on_reply(learned(0, 1)),
        ReconfigurerStep::Done {
            successor: set(1, &[0, 1, 3])
        }
    );
}

/// The replacement story: one old matchmaker never answers (lost for
/// good), the quorum of the other two suffices for every step.
#[test]
fn a_dead_matchmaker_is_replaced_without_its_cooperation() {
    let mut pool = pool(4, &[0, 1, 2]);
    let mut r = MatchmakerReconfigurer::new(NodeId(7), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
        .expect("start");
    for _ in 0..8 {
        exchange(&mut r, &mut pool, &[2]);
        r.resend();
    }
    assert!(!r.is_busy(), "the handover completed without matchmaker 2");
    assert_eq!(*pool[3].set(), set(1, &[0, 1, 3]));
    assert_eq!(
        pool[2].phase(),
        MatchmakerPhase::Active,
        "never told, never stopped"
    );
}

/// The headline scenario: two reconfigurers propose incompatible
/// successors; the decree serializes them and the loser adopts the
/// winner, so exactly one set becomes authoritative for generation 1.
#[test]
fn concurrent_incompatible_proposals_are_serialized_by_the_decree() {
    let mut pool = pool(6, &[0, 1, 2]);
    let mut r1 = MatchmakerReconfigurer::new(NodeId(1), Limits::DEFAULT);
    let mut r2 = MatchmakerReconfigurer::new(NodeId(2), Limits::DEFAULT);
    r1.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3])).expect("r1");
    r2.start(&set(0, &[0, 1, 2]), ids(&[1, 2, 4])).expect("r2");
    // Both freeze and bootstrap.
    exchange(&mut r1, &mut pool, &[]);
    exchange(&mut r2, &mut pool, &[]);
    exchange(&mut r1, &mut pool, &[]);
    exchange(&mut r2, &mut pool, &[]);
    assert!(matches!(r1.phase(), ReconfigurerPhase::Deciding { .. }));
    assert!(matches!(r2.phase(), ReconfigurerPhase::Deciding { .. }));
    // R1 completes Phase 1 and Phase 2 first.
    exchange(&mut r1, &mut pool, &[]);
    let steps = exchange(&mut r1, &mut pool, &[]);
    assert!(
        steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Chosen { .. }))
    );
    // R2's Phase 1 at (1, 2) is nacked by promises at (1, 1)? No: (1,2) > (1,1),
    // so R2 is promised and *learns R1's vote* — P2c makes it propose R1's set.
    let steps = exchange(&mut r2, &mut pool, &[]);
    assert!(
        steps.iter().any(|s| matches!(s, ReconfigurerStep::Proposing { adopted: true, members, .. } if *members == ids(&[0, 1, 3]))),
        "the loser adopts the winner: {steps:?}"
    );
    for _ in 0..4 {
        exchange(&mut r1, &mut pool, &[]);
        exchange(&mut r2, &mut pool, &[]);
        r1.resend();
        r2.resend();
    }
    // One authoritative set for generation 1, everywhere.
    for i in [0, 1, 3] {
        assert_eq!(*pool[i].set(), set(1, &[0, 1, 3]));
    }
    assert_eq!(
        pool[4].phase(),
        MatchmakerPhase::Inactive,
        "the losing proposal never activates"
    );
    assert_eq!(pool[2].successor(), Some(&set(1, &[0, 1, 3])));
}

/// A reconfigurer that starts after the generation was already replaced
/// learns the successor from the frozen members and aborts.
#[test]
fn a_late_reconfigurer_adopts_the_successor_it_finds() {
    let mut pool = pool(5, &[0, 1, 2]);
    let mut r1 = MatchmakerReconfigurer::new(NodeId(1), Limits::DEFAULT);
    r1.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3])).expect("r1");
    for _ in 0..8 {
        exchange(&mut r1, &mut pool, &[]);
        r1.resend();
    }
    assert!(!r1.is_busy());
    let mut r2 = MatchmakerReconfigurer::new(NodeId(2), Limits::DEFAULT);
    r2.start(&set(0, &[0, 1, 2]), ids(&[2, 4])).expect("r2");
    let steps = exchange(&mut r2, &mut pool, &[]);
    assert!(
        steps.iter().any(|s| matches!(s, ReconfigurerStep::Superseded { successor } if *successor == set(1, &[0, 1, 3]))),
        "{steps:?}"
    );
    assert!(!r2.is_busy());
}

/// A Nack reopens the decree strictly above the refusing promise, on the
/// next re-send.
#[test]
fn a_nacked_decree_reopens_above_the_refusing_promise() {
    let mut pool = pool(4, &[0, 1, 2]);
    let mut r = MatchmakerReconfigurer::new(NodeId(1), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
        .expect("start");
    // The freeze completes with no decree promise anywhere...
    exchange(&mut r, &mut pool, &[]);
    // ...then a competing proposer promises the acceptors at a high
    // ballot before this reconfigurer's decree opens.
    for mm in pool.iter_mut().take(3) {
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(9),
            generation: MatchmakerGeneration(0),
            ballot: Ballot {
                round: 5,
                node: NodeId(9),
            },
        });
        let ready = mm.ready();
        ready.advance();
    }
    exchange(&mut r, &mut pool, &[]);
    let steps = exchange(&mut r, &mut pool, &[]);
    assert!(
        steps.iter().any(
            |s| matches!(s, ReconfigurerStep::Preempted { promised, .. } if promised.round == 5)
        ),
        "{steps:?}"
    );
    // The reopen is the driver's: only a re-send moves above the promise.
    r.resend();
    assert!(matches!(
        r.phase(),
        ReconfigurerPhase::Deciding { decree, .. } if decree.ballot().round == 6
    ));
    for _ in 0..4 {
        exchange(&mut r, &mut pool, &[]);
        r.resend();
    }
    assert!(!r.is_busy());
    assert_eq!(*pool[3].set(), set(1, &[0, 1, 3]));
}

/// Claim 6 of the handover model, at the decree: a preempted decree is
/// dead until the driver's next re-send reopens it, so a *second*
/// refusal — the same Nack duplicated, or another member's — moves
/// nothing, is `Ignored`, and never resets the stall clock. Answering
/// `Preempted` again did (the model caught it on its first seed, and in
/// the two-finisher case, where every refusal a dueling finisher sent
/// kept the dead decree's abandon timer from ever firing). The higher
/// promise a later refusal carries still raises the floor the reopen
/// clears.
#[test]
fn a_second_refusal_of_a_preempted_decree_is_ignored_and_raises_the_floor() {
    let mut pool = pool(4, &[0, 1, 2]);
    let mut r = MatchmakerReconfigurer::new(NodeId(1), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
        .expect("start");
    exchange(&mut r, &mut pool, &[]);
    for mm in pool.iter_mut().take(3) {
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(9),
            generation: MatchmakerGeneration(0),
            ballot: Ballot {
                round: 5,
                node: NodeId(9),
            },
        });
        mm.ready().advance();
    }
    exchange(&mut r, &mut pool, &[]);
    // Only matchmaker 0 answers the decree's Phase 1: one Nack, one
    // preemption.
    let steps = exchange(&mut r, &mut pool, &[1, 2]);
    assert!(
        matches!(steps.as_slice(), [ReconfigurerStep::Preempted { promised, .. }] if promised.round == 5),
        "{steps:?}"
    );
    r.tick();
    r.tick();
    assert_eq!(r.stalled_for(), 2);
    let nack = |m: u64, promised: u64| ReconfigureReply::Nacked {
        matchmaker: MatchmakerId(m),
        generation: MatchmakerGeneration(0),
        ballot: Ballot {
            round: 1,
            node: NodeId(1),
        },
        promised: Ballot {
            round: promised,
            node: NodeId(9),
        },
    };
    assert_eq!(
        r.on_reply(nack(0, 5)),
        ReconfigurerStep::Ignored,
        "the same refusal again moves nothing"
    );
    assert_eq!(
        r.on_reply(nack(1, 8)),
        ReconfigurerStep::Ignored,
        "another member's refusal of a dead decree moves nothing"
    );
    assert_eq!(
        r.stalled_for(),
        2,
        "a refusal of a dead decree is not progress"
    );
    // The reopen clears the highest refusal seen, not the first.
    r.resend();
    assert!(matches!(
        r.phase(),
        ReconfigurerPhase::Deciding { decree, .. } if decree.ballot().round == 9
    ));
}

/// A decree opens strictly above the promises the stop quorum reports:
/// a fresh incarnation (round counter at zero) whose predecessor took
/// promises at round 5 opens at 6 without a single Nack — the rule that
/// keeps a rebooted node from reusing a ballot its earlier incarnation
/// may have had a value accepted at (the handover model's seed 103).
#[test]
fn a_decree_opens_above_the_stop_quorums_promises() {
    let mut pool = pool(4, &[0, 1, 2]);
    // The earlier incarnation of node 1 promised the acceptors at round 5.
    for mm in pool.iter_mut().take(3) {
        mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
            from: NodeId(1),
            generation: MatchmakerGeneration(0),
            ballot: Ballot {
                round: 5,
                node: NodeId(1),
            },
        });
        let ready = mm.ready();
        ready.advance();
    }
    // Its fresh incarnation starts from nothing.
    let mut r = MatchmakerReconfigurer::new(NodeId(1), Limits::DEFAULT);
    r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
        .expect("start");
    exchange(&mut r, &mut pool, &[]);
    let steps = exchange(&mut r, &mut pool, &[]);
    assert!(
        steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Deciding { ballot } if ballot.round == 6)),
        "the decree opens above the reported promises: {steps:?}"
    );
    let steps = exchange(&mut r, &mut pool, &[]);
    assert!(
        !steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Preempted { .. })),
        "no Nack: {steps:?}"
    );
    for _ in 0..4 {
        exchange(&mut r, &mut pool, &[]);
        r.resend();
    }
    assert!(!r.is_busy());
    assert_eq!(*pool[3].set(), set(1, &[0, 1, 3]));
}
