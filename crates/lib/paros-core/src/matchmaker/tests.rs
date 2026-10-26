use super::*;
use crate::membership::{AcceptorConfig, QuorumSystem};
use crate::types::NodeId;

/// Review 3 of #133: a member whose own GC floor sits *above* the
/// reconstructed one activates with the higher floor, and its registry is
/// exactly the reconstruction at or above that floor — before and after a
/// restart from the durable install.
#[test]
fn activation_applies_the_higher_of_the_local_and_reconstructed_floors() {
    let cfg = |n: u64| AcceptorConfig::new(vec![NodeId(n), NodeId(n + 1)], QuorumSystem::Majority);
    // Matchmaker 0, a member of M_0 = {0, 1, 2}: registers ballots 1..=6
    // and hears a GarbageA raising its floor to ballot 5.
    let mut mm = fresh(0);
    for round in 1..=6 {
        mm.step(MatchRequest::new(
            J,
            NodeId(1),
            ballot(round, 1),
            cfg(round),
            G0,
        ));
        mm.ready().advance();
    }
    assert_eq!(
        mm.advance_gc_watermark(J, G0, ballot(5, 1)),
        GcOutcome::Raised
    );
    mm.ready().advance();
    assert_eq!(
        mm.registry(J).len(),
        2,
        "ballots 5 and 6 survive the local floor"
    );
    // A reconstruction from *another* frozen quorum ({1, 2}) that never
    // heard the GarbageA: floor at ballot 2, history 2..=6.
    let history: BTreeMap<Ballot, Registration> = (2..=6)
        .map(|round| (ballot(round, 1), Registration::belief(cfg(round))))
        .collect();
    let successor = MatchmakerSet::new(
        MatchmakerGeneration(1),
        vec![MatchmakerId(0), MatchmakerId(3)],
    );
    mm.step_reconfigure(ReconfigureRequest::Bootstrap {
        from: NodeId(9),
        bootstrap: PendingBootstrap {
            set: successor.clone(),
            registries: BTreeMap::from([(
                J,
                JournalRegistry {
                    gc_watermark: ballot(2, 1),
                    history,
                    effective: None,
                },
            )]),
            complete: true,
        }
        .page(None, REGISTRY_PAGE),
    });
    mm.ready().advance();
    mm.step_reconfigure(ReconfigureRequest::Chosen {
        from: NodeId(9),
        generation: G0,
        successor: successor.clone(),
    });
    let ready = mm.ready();
    let installed = ready.writes().iter().find_map(|op| match op {
        MatchmakerWriteOp::InstallRegistry {
            scalars,
            registrations,
        } => Some((scalars.gc_watermark(J), registrations[&J].clone())),
        _ => None,
    });
    ready.advance();
    let (durable_floor, durable_registry) =
        installed.expect("the activation installs the registry");
    let expected: Vec<Ballot> = vec![ballot(5, 1), ballot(6, 1)];
    assert_eq!(*mm.set(), successor);
    assert_eq!(mm.gc_watermark(J), ballot(5, 1), "the higher floor wins");
    assert_eq!(mm.registry(J).keys().copied().collect::<Vec<_>>(), expected);
    assert_eq!(durable_floor, ballot(5, 1));
    assert_eq!(
        durable_registry.keys().copied().collect::<Vec<_>>(),
        expected
    );
    // The restart reads back exactly that.
    let rebooted = Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &image(&mm), Limits::DEFAULT);
    assert_eq!(*rebooted.set(), successor);
    assert_eq!(rebooted.gc_watermark(J), ballot(5, 1));
    assert_eq!(
        rebooted.registry(J).keys().copied().collect::<Vec<_>>(),
        expected
    );
    assert_eq!(rebooted.phase(), MatchmakerPhase::Active);
}

/// What a reboot of `mm` reads back: its durable scalars and records,
/// as a [`MemRegistry`] image (the core applies each write the instant
/// it decides it, so after `advance` the live state *is* the durable
/// state).
fn image(mm: &Matchmaker) -> MemRegistry {
    let registrations = mm
        .journals()
        .map(|journal| (journal, mm.registry(journal).clone()))
        .collect();
    MemRegistry::new(mm.hard_state().clone(), registrations)
}

const G0: MatchmakerGeneration = MatchmakerGeneration(0);
const J: JournalId = JournalId(1);

/// A one-journal set's registries: `J`'s watermark, history and
/// effective configuration.
fn journal_registries(
    gc_watermark: Ballot,
    history: BTreeMap<Ballot, Registration>,
    effective: Option<(Ballot, AcceptorConfig)>,
) -> BTreeMap<JournalId, JournalRegistry> {
    BTreeMap::from([(
        J,
        JournalRegistry {
            gc_watermark,
            history,
            effective,
        },
    )])
}

fn mmconfig(id: u64, bootstrap: &[u64]) -> MatchmakerConfig {
    MatchmakerConfig {
        id: MatchmakerId(id),
        bootstrap: bootstrap.iter().copied().map(MatchmakerId).collect(),
    }
}

fn fresh(id: u64) -> Matchmaker {
    Matchmaker::new(
        &mmconfig(id, &[0, 1, 2]),
        &MemRegistry::default(),
        Limits::DEFAULT,
    )
}

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn config(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().map(|n| NodeId(*n)).collect(),
        QuorumSystem::Majority,
    )
}

fn request(from: u64, ballot: Ballot, members: &[u64]) -> MatchRequest {
    MatchRequest::new(J, NodeId(from), ballot, config(members), G0)
}

fn drain(mm: &mut Matchmaker) -> (Vec<MatchmakerWriteOp>, Vec<MatchReply>) {
    let ready = mm.ready();
    let writes = ready.writes().to_vec();
    let replies = ready.replies().to_vec();
    ready.advance();
    (writes, replies)
}

fn drain_reconfigure(mm: &mut Matchmaker) -> (Vec<MatchmakerWriteOp>, Vec<ReconfigureReply>) {
    let ready = mm.ready();
    let writes = ready.writes().to_vec();
    let replies = ready.reconfigure_replies().to_vec();
    ready.advance();
    (writes, replies)
}

/// Tell `mm` that `successor` was chosen to replace generation 0, and
/// drain what it did.
fn chosen(
    mm: &mut Matchmaker,
    from: u64,
    successor: &MatchmakerSet,
) -> (Vec<MatchmakerWriteOp>, Vec<ReconfigureReply>) {
    mm.step_reconfigure(ReconfigureRequest::Chosen {
        from: NodeId(from),
        generation: G0,
        successor: successor.clone(),
    });
    drain_reconfigure(mm)
}

fn registered(reply: &MatchReply) -> (&BTreeMap<Ballot, Registration>, Ballot) {
    match &reply.outcome {
        MatchOutcome::Registered {
            history,
            gc_watermark,
            ..
        } => (history, *gc_watermark),
        MatchOutcome::Refused(r) => panic!("expected a registration, got {r:?}"),
        MatchOutcome::Probed { .. } => panic!("expected a registration, got a probe answer"),
    }
}

fn set(generation: u64, members: &[u64]) -> MatchmakerSet {
    MatchmakerSet::new(
        MatchmakerGeneration(generation),
        members.iter().copied().map(MatchmakerId).collect(),
    )
}

/// Review finding P7: a second `Chosen` for one generation naming a
/// *different* successor is the shape a wrong relay produces. It is
/// refused — nothing recorded, nothing activated — and the refusal names
/// the successor this matchmaker holds.
#[test]
fn a_chosen_contradicting_the_recorded_successor_is_refused() {
    let mut mm = fresh(0);
    let recorded = set(1, &[0, 1, 2]);
    chosen(&mut mm, 5, &recorded);
    assert_eq!(mm.successor(), Some(&recorded));
    // The same publication again is idempotent.
    let (writes, replies) = chosen(&mut mm, 5, &recorded);
    assert!(matches!(
        &replies[0],
        ReconfigureReply::Learned {
            activated: false,
            ..
        }
    ));
    assert!(writes.is_empty(), "a duplicate publication writes nothing");
    // A contradicting one is not.
    let (writes, replies) = chosen(&mut mm, 6, &set(1, &[0, 3, 4]));
    assert!(
        matches!(
            &replies[0],
            ReconfigureReply::Refused { successor: Some(s), .. } if *s == recorded
        ),
        "the refusal names the recorded successor, not the contradicting one"
    );
    assert!(
        writes.is_empty(),
        "a contradiction changes no durable state"
    );
    assert_eq!(mm.successor(), Some(&recorded));
    assert_eq!(*mm.set(), set(0, &[0, 1, 2]), "and activates nothing");
}

/// Review finding P6: a matchmaker bootstrapped into a proposal that
/// *lost* its decree must not carry that whole reconstructed registry in
/// its durable scalars forever. The learn path prunes it — here at a
/// spare, which is outside the chosen set and so never activates, the
/// branch where nothing else would ever drop it.
#[test]
fn a_losing_bootstrap_is_dropped_when_the_successor_is_learned() {
    // Matchmaker 3 is a spare of M_0 = {0, 1, 2}: it is a member of the
    // proposed {0, 1, 3} and of nothing else.
    let mut mm = Matchmaker::new(
        &mmconfig(3, &[0, 1, 2]),
        &MemRegistry::default(),
        Limits::DEFAULT,
    );
    assert_eq!(mm.phase(), MatchmakerPhase::Inactive);
    let losing = set(1, &[0, 1, 3]);
    let mut history = BTreeMap::new();
    history.insert(ballot(1, 1), Registration::belief(config(&[0, 1, 2])));
    mm.step_reconfigure(ReconfigureRequest::Bootstrap {
        from: NodeId(5),
        bootstrap: PendingBootstrap {
            set: losing.clone(),
            registries: journal_registries(Ballot::zero(), history.clone(), None),
            complete: true,
        }
        .page(None, REGISTRY_PAGE),
    });
    let (writes, replies) = drain_reconfigure(&mut mm);
    assert_eq!(writes.len(), 1, "the pending bootstrap is durable");
    assert!(matches!(&replies[0], ReconfigureReply::Bootstrapped { .. }));
    assert_eq!(mm.hard_state().pending.len(), 1);
    // A different set wins generation 1, and this matchmaker is not in it.
    let winner = set(1, &[0, 1, 2]);
    let (writes, replies) = chosen(&mut mm, 5, &winner);
    assert!(
        matches!(&replies[0], ReconfigureReply::Refused { .. }),
        "a spare outside the chosen set activates nothing"
    );
    assert!(
        mm.hard_state().pending.is_empty(),
        "the losing bootstrap is gone"
    );
    assert!(
        writes.iter().any(
            |w| matches!(w, MatchmakerWriteOp::SetScalars(scalars) if scalars.pending.is_empty())
        ),
        "the prune is durable"
    );
    // And the loser cannot come back: a resent bootstrap for a settled
    // generation is refused. (Here from the other side — a member of the
    // succeeded generation that recorded the successor.)
    let mut member = fresh(0);
    member.step_reconfigure(ReconfigureRequest::Chosen {
        from: NodeId(5),
        generation: G0,
        successor: set(1, &[0, 1, 2]),
    });
    drain_reconfigure(&mut member);
    member.step_reconfigure(ReconfigureRequest::Bootstrap {
        from: NodeId(5),
        bootstrap: PendingBootstrap {
            set: set(1, &[0, 1, 3]),
            registries: journal_registries(Ballot::zero(), history, None),
            complete: true,
        }
        .page(None, REGISTRY_PAGE),
    });
    let (writes, replies) = drain_reconfigure(&mut member);
    assert!(matches!(&replies[0], ReconfigureReply::Refused { .. }));
    assert!(writes.is_empty(), "a settled proposal is not stored");
    assert!(member.hard_state().pending.is_empty());
}

#[test]
fn a_fresh_request_registers_and_reports_the_history_strictly_below() {
    let mut mm = fresh(0);
    mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
    let (writes, replies) = drain(&mut mm);
    assert_eq!(
        writes,
        vec![MatchmakerWriteOp::Register {
            journal: J,
            ballot: ballot(1, 1),
            registration: Registration::belief(config(&[0, 1, 2])),
        }]
    );
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].to, NodeId(1));
    assert_eq!(replies[0].generation, G0);
    let (history, watermark) = registered(&replies[0]);
    assert!(history.is_empty(), "the first registration has no past");
    assert_eq!(watermark, Ballot::zero());

    mm.step(request(2, ballot(3, 2), &[1, 2, 3]));
    let (_, replies) = drain(&mut mm);
    let (history, _) = registered(&replies[0]);
    // Exactly the ballots strictly below: `< b`, not `<= b`.
    assert_eq!(
        history.keys().copied().collect::<Vec<_>>(),
        vec![ballot(1, 1)]
    );
    assert_eq!(history[&ballot(1, 1)].config, config(&[0, 1, 2]));
    assert_eq!(mm.highest(J), Some(ballot(3, 2)));
}

#[test]
fn a_request_at_or_below_the_highest_is_refused_with_the_highest() {
    let mut mm = fresh(0);
    mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
    drain(&mut mm);
    // Strictly below.
    mm.step(request(2, ballot(4, 9), &[0, 1, 2]));
    // Same round, lower node: below in the total order.
    mm.step(request(0, ballot(5, 0), &[0, 1, 2]));
    let (writes, replies) = drain(&mut mm);
    assert!(writes.is_empty(), "a refusal writes nothing");
    for reply in &replies {
        assert_eq!(
            reply.outcome,
            MatchOutcome::Refused(MatchRefusal::Stale {
                highest: ballot(5, 1)
            })
        );
    }
    assert_eq!(mm.registry(J).len(), 1);
}

#[test]
fn the_same_request_again_is_answered_without_a_second_registration() {
    let mut mm = fresh(0);
    mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
    mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
    let (writes, replies) = drain(&mut mm);
    assert_eq!(writes.len(), 2);
    let (first_history, _) = registered(&replies[1]);
    let first_history = first_history.clone();

    mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
    let (writes, replies) = drain(&mut mm);
    assert!(writes.is_empty(), "a duplicate never re-registers");
    let (history, _) = registered(&replies[0]);
    assert_eq!(*history, first_history, "the re-answer is the first answer");
    assert_eq!(mm.registry(J).len(), 2);
}

#[test]
fn a_registered_ballot_with_different_bytes_is_refused_write_once() {
    let mut mm = fresh(0);
    mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
    drain(&mut mm);
    mm.step(request(1, ballot(1, 1), &[0, 1]));
    let (writes, replies) = drain(&mut mm);
    assert!(writes.is_empty());
    assert_eq!(
        replies[0].outcome,
        MatchOutcome::Refused(MatchRefusal::Stale {
            highest: ballot(1, 1)
        })
    );
    assert_eq!(mm.registry(J)[&ballot(1, 1)].config, config(&[0, 1, 2]));
}

#[test]
fn a_request_below_the_watermark_is_refused_and_the_history_starts_at_it() {
    let mut mm = fresh(0);
    // Each its own configuration: a page leaves out none of them.
    for round in 1..=4 {
        mm.step(request(1, ballot(round, 1), &[0, 1, 1 + round]));
    }
    drain(&mut mm);
    assert_eq!(
        mm.advance_gc_watermark(J, G0, ballot(3, 1)),
        GcOutcome::Raised
    );
    assert_eq!(
        mm.advance_gc_watermark(J, G0, ballot(2, 1)),
        GcOutcome::Unchanged,
        "the floor never lowers"
    );
    assert_eq!(
        mm.advance_gc_watermark(J, G0, ballot(3, 1)),
        GcOutcome::Unchanged,
        "re-raising is a no-op"
    );
    assert_eq!(
        mm.advance_gc_watermark(J, MatchmakerGeneration(1), ballot(9, 1)),
        GcOutcome::Refused,
        "another generation's floor is refused"
    );
    let (writes, _) = drain(&mut mm);
    assert_eq!(
        writes,
        vec![MatchmakerWriteOp::SetGcWatermark {
            journal: J,
            watermark: ballot(3, 1)
        }]
    );
    assert_eq!(
        mm.registry(J).keys().copied().collect::<Vec<_>>(),
        vec![ballot(3, 1), ballot(4, 1)],
        "collected registrations are dropped"
    );

    mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
    mm.step(request(1, ballot(6, 1), &[0, 1, 2]));
    let (_, replies) = drain(&mut mm);
    assert_eq!(
        replies[0].outcome,
        MatchOutcome::Refused(MatchRefusal::BelowWatermark {
            watermark: ballot(3, 1)
        })
    );
    let (history, watermark) = registered(&replies[1]);
    assert_eq!(watermark, ballot(3, 1));
    assert_eq!(
        history.keys().copied().collect::<Vec<_>>(),
        vec![ballot(3, 1), ballot(4, 1)]
    );
}

/// The must-fix of #131's review: a retry after GC is answered from the
/// *retained* history — a strict subset of the first answer, with the
/// raised watermark beside it — and still registers nothing.
#[test]
fn a_retry_after_gc_is_answered_from_the_retained_history() {
    let mut mm = fresh(0);
    // Each its own configuration: a page leaves out none of them.
    for round in 1..=3 {
        mm.step(request(1, ballot(round, 1), &[0, 1, 1 + round]));
    }
    let (_, replies) = drain(&mut mm);
    let (first, watermark) = registered(&replies[2]);
    assert_eq!(
        first.keys().copied().collect::<Vec<_>>(),
        vec![ballot(1, 1), ballot(2, 1)]
    );
    assert_eq!(watermark, Ballot::zero());

    // The first reply is lost; GC moves the floor; the client retries.
    assert_eq!(
        mm.advance_gc_watermark(J, G0, ballot(2, 1)),
        GcOutcome::Raised
    );
    drain(&mut mm);
    mm.step(request(1, ballot(3, 1), &[0, 1, 4]));
    let (writes, replies) = drain(&mut mm);
    assert!(writes.is_empty(), "a retry never re-registers");
    let (retry, watermark) = registered(&replies[0]);
    assert_eq!(
        retry.keys().copied().collect::<Vec<_>>(),
        vec![ballot(2, 1)],
        "the retry sees the retained window only"
    );
    assert_eq!(watermark, ballot(2, 1), "and the raised floor beside it");
    assert!(
        retry.iter().all(|(b, c)| first.get(b) == Some(c)),
        "a re-answer is a subset of the first answer, never a superset"
    );
}

/// The paros-specific keying: two proposers at the *same round* hold two
/// distinct registry keys, ordered by node, so neither can overwrite the
/// other and the lower one is an ancestor in the higher one's history. A
/// registry keyed on the bare round would give them one key.
#[test]
fn ballots_at_one_round_from_two_proposers_are_distinct_keys() {
    let mut mm = fresh(0);
    mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
    mm.step(request(2, ballot(5, 2), &[3, 4, 5]));
    let (writes, replies) = drain(&mut mm);
    assert_eq!(writes.len(), 2, "two keys, two registrations");
    let (history, _) = registered(&replies[1]);
    assert_eq!(
        history.get(&ballot(5, 1)).map(|r| &r.config),
        Some(&config(&[0, 1, 2])),
        "the same-round lower ballot is in the higher one's history"
    );
    assert_eq!(mm.registry(J).len(), 2);
    assert_eq!(mm.registry(J)[&ballot(5, 1)].config, config(&[0, 1, 2]));
    assert_eq!(mm.registry(J)[&ballot(5, 2)].config, config(&[3, 4, 5]));

    // The reverse arrival order: the lower node's ballot is stale once
    // the higher node's is registered, and refused — never merged into it.
    let mut mm = fresh(1);
    mm.step(request(2, ballot(5, 2), &[3, 4, 5]));
    mm.step(request(1, ballot(5, 1), &[0, 1, 2]));
    let (_, replies) = drain(&mut mm);
    assert_eq!(
        replies[1].outcome,
        MatchOutcome::Refused(MatchRefusal::Stale {
            highest: ballot(5, 2)
        })
    );
    assert_eq!(mm.registry(J).len(), 1);
}

#[test]
fn a_restart_answers_exactly_as_the_original_would_have() {
    let mut mm = fresh(2);
    mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
    mm.step(request(2, ballot(2, 2), &[0, 1, 2, 3]));
    drain(&mut mm);
    // A reboot walks the durable records back through the port.
    let mut rebooted = Matchmaker::new(&mmconfig(2, &[0, 1, 2]), &image(&mm), Limits::DEFAULT);
    rebooted.step(request(1, ballot(3, 1), &[1, 2, 3]));
    mm.step(request(1, ballot(3, 1), &[1, 2, 3]));
    let (_, expected) = drain(&mut mm);
    let (_, observed) = drain(&mut rebooted);
    assert_eq!(observed, expected);
}

#[test]
#[should_panic(expected = "no registration survives below the gc watermark")]
fn a_registry_below_its_watermark_refuses_to_boot() {
    let scalars = MatchmakerHardState {
        journals: BTreeMap::from([(
            J,
            JournalScalars {
                gc_watermark: ballot(2, 0),
                effective: None,
            },
        )]),
        ..Default::default()
    };
    let store = MemRegistry::new(
        scalars,
        BTreeMap::from([(
            J,
            BTreeMap::from([(ballot(1, 1), Registration::belief(config(&[0])))]),
        )]),
    );
    let _ = Matchmaker::new(&mmconfig(0, &[0]), &store, Limits::DEFAULT);
}

#[test]
fn a_configuration_is_normalized_and_sizes_its_quorum() {
    let config = AcceptorConfig::new(
        vec![NodeId(2), NodeId(0), NodeId(2), NodeId(1)],
        QuorumSystem::Majority,
    );
    assert_eq!(config.members(), vec![NodeId(0), NodeId(1), NodeId(2)]);
    let two: std::collections::BTreeSet<NodeId> = [NodeId(0), NodeId(1)].into_iter().collect();
    let one: std::collections::BTreeSet<NodeId> = [NodeId(0)].into_iter().collect();
    assert!(config.has_phase1_quorum(&two) && config.has_phase2_quorum(&two));
    assert!(!config.has_phase1_quorum(&one) && !config.has_phase2_quorum(&one));
}

// ---- generations (#125) ---------------------------------------------

/// A fresh store resolves against the bootstrap set: a member is active
/// for generation 0, a spare is inactive and refuses everything with
/// `Inactive` — neither writes anything at boot.
#[test]
fn a_fresh_matchmaker_resolves_its_phase_from_the_bootstrap_set() {
    let member = fresh(0);
    assert_eq!(member.phase(), MatchmakerPhase::Active);
    assert_eq!(*member.set(), set(0, &[0, 1, 2]));
    let mut spare = Matchmaker::new(
        &mmconfig(7, &[0, 1, 2]),
        &MemRegistry::default(),
        Limits::DEFAULT,
    );
    assert_eq!(spare.phase(), MatchmakerPhase::Inactive);
    spare.step(request(1, ballot(1, 1), &[0, 1, 2]));
    let (writes, replies) = drain(&mut spare);
    assert!(writes.is_empty());
    assert_eq!(
        replies[0].outcome,
        MatchOutcome::Refused(MatchRefusal::Inactive)
    );
}

/// A request fenced by another generation is refused with the current
/// set, never served.
#[test]
fn a_request_for_another_generation_is_refused_with_the_current_set() {
    let mut mm = fresh(0);
    mm.step(MatchRequest::new(
        J,
        NodeId(1),
        ballot(1, 1),
        config(&[0, 1, 2]),
        MatchmakerGeneration(3),
    ));
    let (writes, replies) = drain(&mut mm);
    assert!(writes.is_empty());
    assert_eq!(
        replies[0].outcome,
        MatchOutcome::Refused(MatchRefusal::Generation {
            current: set(0, &[0, 1, 2])
        })
    );
}

/// The freeze: a stop is durable before its answer, idempotent, and a
/// frozen matchmaker registers nothing for its generation ever again —
/// across a reboot too.
#[test]
fn a_stop_freezes_durably_and_is_idempotent() {
    let mut mm = fresh(0);
    mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
    drain(&mut mm);
    mm.step_reconfigure(ReconfigureRequest::Stop {
        from: NodeId(5),
        generation: G0,
        after: None,
    });
    let (writes, replies) = drain_reconfigure(&mut mm);
    assert!(
        matches!(writes.as_slice(), [MatchmakerWriteOp::SetScalars(s)] if s.phase == MatchmakerPhase::Stopped)
    );
    let ReconfigureReply::Stopped {
        generation,
        registries,
        successor,
        ..
    } = &replies[0]
    else {
        panic!("expected Stopped, got {:?}", replies[0]);
    };
    assert_eq!(*generation, G0);
    assert_eq!(registries[&J].history.len(), 1);
    assert!(successor.is_none());
    // Idempotent: no second write.
    mm.step_reconfigure(ReconfigureRequest::Stop {
        from: NodeId(6),
        generation: G0,
        after: None,
    });
    let (writes, replies) = drain_reconfigure(&mut mm);
    assert!(writes.is_empty(), "a re-sent stop writes nothing");
    assert!(matches!(replies[0], ReconfigureReply::Stopped { .. }));
    // Frozen, and still frozen after a reboot.
    for mm in [
        &mut mm.clone(),
        &mut Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &image(&mm), Limits::DEFAULT),
    ] {
        mm.step(request(1, ballot(2, 1), &[0, 1, 2]));
        let (writes, replies) = drain(mm);
        assert!(writes.is_empty());
        assert_eq!(
            replies[0].outcome,
            MatchOutcome::Refused(MatchRefusal::Stopped { successor: None })
        );
    }
    // A stop for another generation is refused with what is known.
    mm.step_reconfigure(ReconfigureRequest::Stop {
        from: NodeId(5),
        generation: MatchmakerGeneration(4),
        after: None,
    });
    let (_, replies) = drain_reconfigure(&mut mm);
    assert!(matches!(
        &replies[0],
        ReconfigureReply::Refused { current, phase: MatchmakerPhase::Stopped, .. } if *current == set(0, &[0, 1, 2])
    ));
}

/// The handover end to end at one matchmaker in both generations: stop,
/// bootstrap (pending, refused to serve), decree votes (durable), chosen
/// (activates the pending registry whole, at the reconstructed
/// watermark), and the new generation serves from the reconstruction.
#[test]
#[allow(clippy::too_many_lines)]
fn a_chosen_successor_activates_its_pending_bootstrap() {
    let mut mm = fresh(0);
    mm.step(request(1, ballot(1, 1), &[0, 1, 2]));
    drain(&mut mm);
    mm.step_reconfigure(ReconfigureRequest::Stop {
        from: NodeId(5),
        generation: G0,
        after: None,
    });
    drain_reconfigure(&mut mm);
    let successor = set(1, &[0, 3, 4]);
    let mut history = BTreeMap::new();
    history.insert(ballot(1, 1), Registration::belief(config(&[0, 1, 2])));
    history.insert(ballot(2, 2), Registration::belief(config(&[1, 2, 3])));
    let bootstrap = PendingBootstrap {
        set: successor.clone(),
        registries: journal_registries(ballot(1, 1), history, None),
        complete: true,
    };
    // A bootstrap for a set this matchmaker is not in is refused.
    mm.step_reconfigure(ReconfigureRequest::Bootstrap {
        from: NodeId(5),
        bootstrap: PendingBootstrap {
            set: set(1, &[3, 4, 5]),
            ..bootstrap.clone()
        }
        .page(None, REGISTRY_PAGE),
    });
    let (writes, replies) = drain_reconfigure(&mut mm);
    assert!(writes.is_empty());
    assert!(matches!(replies[0], ReconfigureReply::Refused { .. }));
    mm.step_reconfigure(ReconfigureRequest::Bootstrap {
        from: NodeId(5),
        bootstrap: bootstrap.page(None, REGISTRY_PAGE),
    });
    let (writes, replies) = drain_reconfigure(&mut mm);
    assert_eq!(writes.len(), 1, "the pending bootstrap is durable");
    assert!(
        matches!(&replies[0], ReconfigureReply::Bootstrapped { set: s, .. } if *s == successor)
    );
    // Still frozen at generation 0: generation 1 is not served yet.
    mm.step(MatchRequest::new(
        J,
        NodeId(1),
        ballot(5, 1),
        config(&[0, 1, 2]),
        MatchmakerGeneration(1),
    ));
    let (_, replies) = drain(&mut mm);
    assert!(matches!(
        replies[0].outcome,
        MatchOutcome::Refused(MatchRefusal::Inactive)
    ));
    // The decree over generation 0.
    mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
        from: NodeId(5),
        generation: G0,
        ballot: ballot(1, 5),
    });
    let (writes, replies) = drain_reconfigure(&mut mm);
    assert_eq!(writes.len(), 1, "a promise is durable");
    assert!(matches!(
        &replies[0],
        ReconfigureReply::Promised { vote: None, .. }
    ));
    mm.step_reconfigure(ReconfigureRequest::DecreeAccept {
        from: NodeId(5),
        generation: G0,
        ballot: ballot(1, 5),
        members: successor.members().to_vec(),
    });
    let (writes, replies) = drain_reconfigure(&mut mm);
    assert_eq!(writes.len(), 1, "a vote is durable");
    assert!(matches!(&replies[0], ReconfigureReply::Accepted { .. }));
    // A lower decree ballot is refused with the promise.
    mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
        from: NodeId(4),
        generation: G0,
        ballot: ballot(1, 4),
    });
    let (_, replies) = drain_reconfigure(&mut mm);
    assert!(
        matches!(&replies[0], ReconfigureReply::Nacked { promised, .. } if *promised == ballot(1, 5))
    );
    // A higher one learns the vote (P2c's input).
    mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
        from: NodeId(4),
        generation: G0,
        ballot: ballot(2, 4),
    });
    let (_, replies) = drain_reconfigure(&mut mm);
    assert!(
        matches!(&replies[0], ReconfigureReply::Promised { vote: Some((b, m)), .. } if *b == ballot(1, 5) && *m == successor.members())
    );
    // Chosen: the successor is recorded for generation 0 and, being a
    // member holding the bootstrap, this matchmaker activates it.
    let (writes, replies) = chosen(&mut mm, 5, &successor);
    assert!(matches!(
        &replies[0],
        ReconfigureReply::Learned {
            activated: true,
            ..
        }
    ));
    assert!(
        matches!(writes.last(), Some(MatchmakerWriteOp::InstallRegistry { scalars, registrations }) if scalars.generation == MatchmakerGeneration(1) && registrations.get(&J).is_some_and(|r| r.len() == 2))
    );
    assert_eq!(mm.phase(), MatchmakerPhase::Active);
    assert_eq!(*mm.set(), successor);
    assert_eq!(mm.gc_watermark(J), ballot(1, 1));
    assert!(mm.successor().is_none());
    assert_eq!(mm.hard_state().decree, DecreeRecord::default());
    // Generation 1 serves from the reconstruction; generation 0 is told
    // the chain link is gone from here (this matchmaker moved on).
    mm.step(MatchRequest::new(
        J,
        NodeId(1),
        ballot(5, 1),
        config(&[0, 1, 2]),
        MatchmakerGeneration(1),
    ));
    mm.step(request(1, ballot(6, 1), &[0, 1, 2]));
    let (_, replies) = drain(&mut mm);
    let (history, wm) = registered(&replies[0]);
    assert_eq!(history.len(), 2);
    assert_eq!(wm, ballot(1, 1));
    assert_eq!(
        replies[1].outcome,
        MatchOutcome::Refused(MatchRefusal::Generation {
            current: successor.clone()
        })
    );
    // A reboot lands in generation 1 with the installed registry (plus
    // the one registration generation 1 took above).
    let rebooted = Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &image(&mm), Limits::DEFAULT);
    assert_eq!(*rebooted.set(), successor);
    assert_eq!(rebooted.registry(J).len(), 3);
}

/// A member of the succeeded generation that is *not* in the successor
/// records the chain link (freezing on the spot if it was still active)
/// and points late proposers at it forever.
#[test]
fn a_departed_matchmaker_answers_with_its_successor() {
    let mut mm = fresh(2);
    let successor = set(1, &[0, 1, 3]);
    let (writes, replies) = chosen(&mut mm, 5, &successor);
    assert!(matches!(
        &replies[0],
        ReconfigureReply::Learned {
            activated: false,
            ..
        }
    ));
    assert_eq!(writes.len(), 2, "the freeze and the link are durable");
    assert_eq!(mm.phase(), MatchmakerPhase::Stopped);
    assert_eq!(mm.successor(), Some(&successor));
    mm.step(request(1, ballot(9, 1), &[0, 1, 2]));
    let (_, replies) = drain(&mut mm);
    assert_eq!(
        replies[0].outcome,
        MatchOutcome::Refused(MatchRefusal::Stopped {
            successor: Some(successor.clone())
        })
    );
    // A second, different "chosen" for the same generation is refused
    // (the first record stands; the audit judges the conflict).
    mm.step_reconfigure(ReconfigureRequest::Chosen {
        from: NodeId(6),
        generation: G0,
        successor: set(1, &[4, 5, 6]),
    });
    let (writes, _) = drain_reconfigure(&mut mm);
    assert!(writes.is_empty());
    assert_eq!(mm.successor(), Some(&successor));
}
