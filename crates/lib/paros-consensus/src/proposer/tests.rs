use super::*;
use crate::journal::{Command, Entry, LeaderUuid, Seq, command_fingerprint};
use crate::membership::QuorumSystem;
use crate::types::{NodeId, Value};

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn cmd(seq: u64) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(seq),
        records: vec![Value(seq.to_le_bytes().to_vec())],
    })
}

fn config(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().map(|n| NodeId(*n)).collect(),
        QuorumSystem::Majority,
    )
}

/// Fold an empty terminal `Promise` page from `from` at `ballot(1, 0)`.
fn fold_empty(p: &mut Proposer<NodeId, Command>, from: u64, from_slot: Slot) -> PromiseFold {
    p.fold_promise(
        NodeId(from),
        ballot(1, 0),
        from_slot,
        BTreeMap::new(),
        BTreeMap::new(),
        None,
    )
}

/// Fold an empty terminal `Promise` page from `from` into the probe.
fn fold_empty_probe(
    p: &mut Proposer<NodeId, Command>,
    from: u64,
    at: Ballot,
    from_slot: Slot,
) -> PromiseFold {
    p.fold_probe_promise(
        NodeId(from),
        at,
        from_slot,
        &BTreeMap::new(),
        &BTreeMap::new(),
        None,
    )
}

fn opened(prior: Vec<AcceptorConfig>) -> Proposer<NodeId, Command> {
    let mut p = Proposer::new();
    let mut expected: Vec<NodeId> = prior
        .iter()
        .flat_map(|c| c.members().iter().copied())
        .chain([NodeId(1), NodeId(2)])
        .filter(|n| *n != NodeId(0))
        .collect();
    expected.sort_unstable();
    expected.dedup();
    let targets = p.open_phase1(
        Campaign {
            me: Some(NodeId(0)),
            ballot: ballot(1, 0),
            config: config(&[0, 1, 2]),
            prior,
            from_slot: Slot(0),
        },
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert_eq!(
        targets, expected,
        "Phase 1 addresses the union of the prior configurations and C_b"
    );
    p
}

/// Phase 1 completes only with a quorum of *every* prior configuration,
/// never of their union (#121).
#[test]
fn phase1_needs_a_quorum_of_every_prior_configuration() {
    let mut p = opened(vec![config(&[0, 1, 2]), config(&[2, 3, 4])]);
    assert_eq!(fold_empty(&mut p, 1, Slot(0)), PromiseFold::Answered);
    // {0, 1, 3} is a majority of the union's five members, yet the
    // second configuration holds only one promise (node 3): the union
    // rule would wrongly complete Phase 1 here.
    assert_eq!(fold_empty(&mut p, 3, Slot(0)), PromiseFold::Answered);
    assert!(!p.phase1_won(ballot(1, 0)), "quorum(union) is not the rule");
    assert_eq!(fold_empty(&mut p, 4, Slot(0)), PromiseFold::Answered);
    assert!(p.phase1_won(ballot(1, 0)));
    assert!(
        !p.phase1_won(ballot(2, 1)),
        "a campaign below the node's own promise never wins"
    );
}

/// The merge keeps the highest-ballot report per slot (P2c) and the
/// closed outcome carries it.
#[test]
fn promise_merge_is_p2c() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let mut low = BTreeMap::new();
    low.insert(Slot(0), (ballot(0, 1), cmd(1)));
    let mut high = BTreeMap::new();
    high.insert(Slot(0), (ballot(0, 2), cmd(2)));
    p.fold_promise(
        NodeId(1),
        ballot(1, 0),
        Slot(0),
        high,
        BTreeMap::new(),
        None,
    );
    p.fold_promise(NodeId(2), ballot(1, 0), Slot(0), low, BTreeMap::new(), None);
    assert!(p.phase1_won(ballot(1, 0)));
    let out = p.close_phase1(|_| false);
    assert_eq!(out.recovered.get(&Slot(0)), Some(&(ballot(0, 2), cmd(2))));
    assert_eq!(out.highest_reported, Some(Slot(0)));
    assert!(out.blocked.is_empty());
    assert!(p.probe().is_none());
    assert!(p.election().is_none());
}

/// A page at the wrong cursor, or from a sender already counted, is
/// ignored; a continuation names the next cursor.
#[test]
fn promise_pages_are_cursor_checked() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let full: BTreeMap<Slot, (Ballot, Command)> = (0..PROMISE_BATCH as u64)
        .map(|s| (Slot(s), (ballot(0, 1), cmd(s))))
        .collect();
    assert_eq!(
        p.fold_promise(
            NodeId(1),
            ballot(1, 0),
            Slot(0),
            full,
            BTreeMap::new(),
            Some(Slot(PROMISE_BATCH as u64)),
        ),
        PromiseFold::Continue(Slot(PROMISE_BATCH as u64))
    );
    assert_eq!(
        fold_empty(&mut p, 1, Slot(3)),
        PromiseFold::Ignored,
        "a page at the wrong cursor is ignored"
    );
    assert_eq!(
        fold_empty(&mut p, 1, Slot(PROMISE_BATCH as u64)),
        PromiseFold::Answered
    );
    assert_eq!(
        fold_empty(&mut p, 1, Slot(0)),
        PromiseFold::Ignored,
        "a counted sender is not merged twice"
    );
}

/// Wire hygiene of a `Promise` page: each rule of the shape check refuses
/// the one malformed page that breaks it, next to the well-formed page it
/// takes. An honest acceptor never sends these pages, so no walk reaches the
/// refusals.
#[test]
fn a_promise_page_of_the_wrong_shape_is_refused() {
    let acc = |slots: &[u64]| -> BTreeMap<Slot, (Ballot, Command)> {
        slots
            .iter()
            .map(|&s| (Slot(s), (ballot(0, 1), cmd(s))))
            .collect()
    };
    let fau = |slots: &[u64]| -> BTreeMap<Slot, Ballot> {
        slots.iter().map(|&s| (Slot(s), ballot(0, 1))).collect()
    };
    let valid = |expected: u64, accepted: &[u64], faulty: &[u64], from: u64, next: Option<u64>| {
        promise_page_shape_valid(
            Slot(expected),
            &acc(accepted),
            &fau(faulty),
            Slot(from),
            next.map(Slot),
        )
    };
    // Well formed: a terminal page, a continued page, an empty terminal page.
    assert!(valid(2, &[2, 4], &[3], 2, None));
    assert!(valid(2, &[2, 4], &[5], 2, Some(6)));
    assert!(valid(2, &[], &[], 2, None));
    // The page starts at the cursor the receiver expects.
    assert!(!valid(2, &[3], &[], 3, None), "a page off its cursor");
    // No entry sits below the page's start.
    assert!(!valid(2, &[1, 4], &[], 2, None), "a record below the start");
    assert!(
        !valid(2, &[4], &[1], 2, None),
        "a faulty entry below the start"
    );
    // The two maps are disjoint.
    assert!(
        !valid(2, &[4], &[4], 2, None),
        "a slot both readable and faulty"
    );
    // A continued page carries an entry, and its cursor passes its start and
    // every entry of both maps.
    assert!(!valid(2, &[], &[], 2, Some(3)), "an empty continued page");
    assert!(!valid(2, &[2], &[], 2, Some(2)), "a cursor at the start");
    assert!(
        !valid(2, &[3], &[], 2, Some(3)),
        "a cursor at the last record"
    );
    assert!(
        !valid(2, &[], &[3], 2, Some(3)),
        "a cursor at the last faulty entry"
    );
    assert!(
        !valid(2, &[2, 5], &[], 2, Some(4)),
        "a cursor before a record"
    );
    assert!(
        !valid(2, &[2], &[5], 2, Some(4)),
        "a cursor before a faulty entry"
    );
    // At most the ceiling, across both maps.
    let ceiling = PROMISE_BATCH as u64;
    let readable: Vec<u64> = (0..ceiling / 2).collect();
    let damaged: Vec<u64> = (ceiling / 2..ceiling).collect();
    assert!(valid(0, &readable, &damaged, 0, None));
    let over: Vec<u64> = (ceiling / 2..=ceiling).collect();
    assert!(
        !valid(0, &readable, &over, 0, None),
        "a page past the ceiling"
    );
}

/// A faulty report above the best `have` blocks the slot (Case 3) until
/// a straggler's answer lets the probe decide it (Case 1).
/// Review finding C8: the probe pages a straggler's `Promise` through the
/// same [`PromiseTally`] the election does, and only the election half was
/// tested. A page at the wrong cursor is ignored, the continuation is
/// tracked per straggler, and a straggler that answered completely is
/// never merged twice.
#[test]
fn probe_promise_pages_are_cursor_checked() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(0), ballot(0, 2));
    p.fold_promise(
        NodeId(1),
        ballot(1, 0),
        Slot(0),
        BTreeMap::new(),
        faulty,
        None,
    );
    let out = p.close_phase1(|_| false);
    assert!(out.blocked.contains(&Slot(0)));
    assert_eq!(
        p.probe()
            .expect("the probe opens for the blocked slot")
            .requery(NodeId(0))
            .to,
        vec![(NodeId(2), Slot(0))],
        "a straggler that sent no page is asked from the election's first slot"
    );
    // A full first page from the straggler leaves it mid-suffix.
    let full: BTreeMap<Slot, (Ballot, Command)> = (0..PROMISE_BATCH as u64)
        .map(|s| (Slot(s), (ballot(0, 1), cmd(s))))
        .collect();
    assert_eq!(
        p.fold_probe_promise(
            NodeId(2),
            ballot(1, 0),
            Slot(0),
            &full,
            &BTreeMap::new(),
            Some(Slot(PROMISE_BATCH as u64)),
        ),
        PromiseFold::Continue(Slot(PROMISE_BATCH as u64))
    );
    assert_eq!(
        p.probe().expect("still open").requery(NodeId(0)).to,
        vec![(NodeId(2), Slot(PROMISE_BATCH as u64))],
        "a straggler mid-suffix is still a straggler, asked for its next page"
    );
    assert_eq!(
        fold_empty_probe(&mut p, 2, ballot(1, 0), Slot(3)),
        PromiseFold::Ignored,
        "a page at the wrong cursor is ignored"
    );
    assert_eq!(
        fold_empty_probe(&mut p, 2, ballot(2, 0), Slot(PROMISE_BATCH as u64)),
        PromiseFold::Ignored,
        "a page at another ballot is ignored"
    );
    // The terminal page decides the blocked slot from the first page's
    // `have` and closes the probe.
    assert_eq!(
        fold_empty_probe(&mut p, 2, ballot(1, 0), Slot(PROMISE_BATCH as u64)),
        PromiseFold::Answered
    );
    assert_eq!(
        p.resolve_probe(),
        vec![ProbeDecision {
            slot: Slot(0),
            command: Some(cmd(0)),
        }]
    );
    assert!(p.probe().is_none());
}

#[test]
fn faulty_report_blocks_until_the_probe_decides() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(0), ballot(0, 2));
    p.fold_promise(
        NodeId(1),
        ballot(1, 0),
        Slot(0),
        BTreeMap::new(),
        faulty,
        None,
    );
    assert!(p.phase1_won(ballot(1, 0)));
    let out = p.close_phase1(|_| false);
    assert!(
        out.blocked.contains(&Slot(0)),
        "a faulty report with no have blocks"
    );
    let probe = p.probe().expect("the probe opens for the blocked slot");
    assert_eq!(probe.requery(NodeId(0)).to, vec![(NodeId(2), Slot(0))]);
    assert!(p.resolve_probe().is_empty(), "Case 3: wait");
    let mut have = BTreeMap::new();
    have.insert(Slot(0), (ballot(0, 2), cmd(7)));
    assert_eq!(
        p.fold_probe_promise(
            NodeId(2),
            ballot(1, 0),
            Slot(0),
            &have,
            &BTreeMap::new(),
            None
        ),
        PromiseFold::Answered
    );
    let decisions = p.resolve_probe();
    assert_eq!(
        decisions,
        vec![ProbeDecision {
            slot: Slot(0),
            command: Some(cmd(7)),
        }]
    );
    assert!(
        p.probe().is_none(),
        "the last blocked slot closes the probe"
    );
}

/// A round decides on a quorum of *its configuration*; a vote from a
/// non-member never counts toward it.
#[test]
fn rounds_decide_on_a_configuration_quorum() {
    let mut p = Proposer::new();
    let c = config(&[0, 1, 2]);
    p.open_round(Slot(5), ballot(1, 0), cmd(1), Some(NodeId(0)), None);
    assert!(p.decided(Slot(5), &c).is_none());
    assert!(p.fold_accepted(
        NodeId(1),
        ballot(1, 0),
        Slot(5),
        command_fingerprint(&cmd(1))
    ));
    assert_eq!(p.decided(Slot(5), &c), Some((ballot(1, 0), cmd(1))));
    assert!(p.supersedes(ballot(1, 0), Slot(5)));
    assert!(!p.supersedes(ballot(2, 0), Slot(5)));
    p.close_round(Slot(5));
    assert!(p.rounds().is_empty());
}

/// The recovery pump fills undescribed slots only under a Phase-1-backed
/// policy.
#[test]
fn recovery_policy_decides_the_fill() {
    let mut p: Proposer<NodeId, Command> = Proposer::new();
    let mut recovered = BTreeMap::new();
    recovered.insert(Slot(1), cmd(1));
    p.open_recovery(
        recovered.clone(),
        BTreeSet::new(),
        Slot(0),
        Slot(2),
        RecoveryPolicy::Phase1Backed,
    );
    assert_eq!(p.recovery_next(), Some((Slot(0), RecoveryStep::Fill)));
    assert_eq!(
        p.recovery_next(),
        Some((Slot(1), RecoveryStep::Recovered(cmd(1))))
    );
    assert_eq!(p.recovery_next(), None);
    assert_eq!(p.recovery_remaining(), 0);
    p.close_drained_recovery();
    assert!(p.recovery().is_none());

    p.open_recovery(
        recovered,
        BTreeSet::new(),
        Slot(0),
        Slot(2),
        RecoveryPolicy::Inherited,
    );
    assert_eq!(
        p.recovery_next(),
        Some((Slot(0), RecoveryStep::Undescribed))
    );
    assert_eq!(
        p.recovery_next(),
        Some((Slot(1), RecoveryStep::Recovered(cmd(1))))
    );
    p.close_drained_recovery();
    assert!(p.recovery().is_none());
}

// ---- the repair probe (CTRL Stage 8) -----------------------------------------

/// A five-member leadership at `ballot(1, 0)` whose probe is blocked on
/// slots 0 and 1. Nodes 1 and 2 report both slots faulty at `ballot(0, 3)`.
/// Node 3 holds `cmd(5)` at slot 0 at the lower `ballot(0, 1)`. Node 4 has
/// not answered. Each slot has only two qualifying answers (0 and 3): one
/// short of a majority of five.
fn blocked_on_two_slots() -> Proposer<NodeId, Command> {
    let mut p = opened(vec![config(&[0, 1, 2, 3, 4])]);
    let faulty: BTreeMap<Slot, Ballot> = [(Slot(0), ballot(0, 3)), (Slot(1), ballot(0, 3))]
        .into_iter()
        .collect();
    for from in [1, 2] {
        p.fold_promise(
            NodeId(from),
            ballot(1, 0),
            Slot(0),
            BTreeMap::new(),
            faulty.clone(),
            None,
        );
    }
    let mut have = BTreeMap::new();
    have.insert(Slot(0), (ballot(0, 1), cmd(5)));
    p.fold_promise(
        NodeId(3),
        ballot(1, 0),
        Slot(0),
        have,
        BTreeMap::new(),
        None,
    );
    assert!(p.phase1_won(ballot(1, 0)));
    let out = p.close_phase1(|_| false);
    assert_eq!(out.blocked, [Slot(0), Slot(1)].into_iter().collect());
    p
}

/// The blocked slots of the open probe, as a list.
fn blocked_slots(p: &Proposer<NodeId, Command>) -> Vec<Slot> {
    p.probe()
        .expect("the probe is open")
        .blocked()
        .iter()
        .copied()
        .collect()
}

/// The probe queries at the ballot of the leadership that opened it. The
/// re-sent `Prepare` uses the same ballot.
#[test]
fn a_repair_probe_queries_at_its_leadership_ballot() {
    let p = blocked_on_two_slots();
    let probe = p.probe().expect("the probe is open");
    assert_eq!(probe.ballot(), ballot(1, 0));
    assert_eq!(probe.requery(NodeId(0)).ballot, probe.ballot());
    assert_eq!(probe.requery(NodeId(0)).to, vec![(NodeId(4), Slot(0))]);
}

/// A straggler's faulty report counts against its slot. That slot stays
/// blocked. A slot the same answer lets the probe decide is decided at
/// once, and the probe stays open for the other slot.
#[test]
fn a_straggler_faulty_report_keeps_only_its_own_slot_blocked() {
    let mut p = blocked_on_two_slots();
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(1), ballot(0, 2));
    assert_eq!(
        p.fold_probe_promise(
            NodeId(4),
            ballot(1, 0),
            Slot(0),
            &BTreeMap::new(),
            &faulty,
            None,
        ),
        PromiseFold::Answered
    );
    assert_eq!(
        p.resolve_probe(),
        vec![ProbeDecision {
            slot: Slot(0),
            command: Some(cmd(5)),
        }],
        "slot 0 has three qualifying answers; slot 1 has two"
    );
    assert_eq!(blocked_slots(&p), vec![Slot(1)]);
    assert!(
        p.probe()
            .expect("the probe stays open")
            .requery(NodeId(0))
            .to
            .is_empty(),
        "every member answered: nobody is asked again"
    );
    assert!(p.resolve_probe().is_empty(), "Case 3: slot 1 waits");
}

/// A slot decided through another path leaves the probe. The probe never
/// decides that slot again. The probe keeps its clock while a slot stays
/// blocked, and closes with its clock when no slot stays blocked. A slot
/// the probe does not hold changes nothing. A closed probe decides nothing.
#[test]
fn a_slot_resolved_elsewhere_leaves_the_probe() {
    let mut p = blocked_on_two_slots();
    assert_eq!(p.tick_probe(), Some(1));
    p.probe_resolved_elsewhere(Slot(9));
    assert_eq!(blocked_slots(&p), vec![Slot(0), Slot(1)]);
    p.probe_resolved_elsewhere(Slot(0));
    assert_eq!(blocked_slots(&p), vec![Slot(1)]);
    assert_eq!(p.probe_elapsed(), Some(1), "the clock runs on");
    // Node 4's answer makes both slots decidable. Only slot 1 is still the
    // probe's: slot 0 is not proposed a second time.
    assert_eq!(
        fold_empty_probe(&mut p, 4, ballot(1, 0), Slot(0)),
        PromiseFold::Answered
    );
    assert_eq!(
        p.resolve_probe(),
        vec![ProbeDecision {
            slot: Slot(1),
            command: None,
        }]
    );
    assert!(p.probe().is_none());

    let mut p = blocked_on_two_slots();
    assert_eq!(p.tick_probe(), Some(1));
    p.probe_resolved_elsewhere(Slot(0));
    p.probe_resolved_elsewhere(Slot(1));
    assert!(
        p.probe().is_none(),
        "the last blocked slot closes the probe"
    );
    assert_eq!(p.probe_elapsed(), None, "the clock closes with the probe");
    assert_eq!(p.tick_probe(), None);
    assert!(p.resolve_probe().is_empty(), "no probe: no decision");
    p.probe_resolved_elsewhere(Slot(1));
    assert!(p.probe().is_none(), "no probe: nothing to resolve");
}

/// A trim-point jump drops every blocked slot below the point. The probe
/// keeps the slots at or above it, with its clock, and never decides a
/// dropped slot. A jump past every blocked slot closes the probe.
#[test]
fn a_trim_point_jump_keeps_only_the_blocked_slots_above_it() {
    let mut p = blocked_on_two_slots();
    assert_eq!(p.tick_probe(), Some(1));
    p.probe_retain_from(Slot(0));
    assert_eq!(blocked_slots(&p), vec![Slot(0), Slot(1)], "nothing below 0");
    p.probe_retain_from(Slot(1));
    assert_eq!(blocked_slots(&p), vec![Slot(1)]);
    assert_eq!(p.probe_elapsed(), Some(1), "the clock runs on");
    // Node 4 reports a value at the dropped slot 0: the probe ignores it.
    let mut have = BTreeMap::new();
    have.insert(Slot(0), (ballot(0, 2), cmd(6)));
    assert_eq!(
        p.fold_probe_promise(
            NodeId(4),
            ballot(1, 0),
            Slot(0),
            &have,
            &BTreeMap::new(),
            None,
        ),
        PromiseFold::Answered
    );
    assert_eq!(
        p.resolve_probe(),
        vec![ProbeDecision {
            slot: Slot(1),
            command: None,
        }]
    );
    assert!(p.probe().is_none());

    let mut p = blocked_on_two_slots();
    p.probe_retain_from(Slot(2));
    assert!(
        p.probe().is_none(),
        "a jump past every blocked slot closes it"
    );
    assert_eq!(p.probe_elapsed(), None);
    p.probe_retain_from(Slot(3));
    assert!(p.probe().is_none(), "no probe: the jump changes nothing");
}

/// Phase 1 is a conversation with `C_b` and `H_b` alone: a `Promise` from
/// a node in neither is never merged or paged, whatever its cursor. Sweep
/// recipe 277225755259181109 105940:12012675180235557599 on d615e2a: a
/// wiped machine answered a member's `Prepare` in its new identity, the
/// candidate paged its suffix, and the page went to a node outside the
/// ballot's configurations.
#[test]
fn a_promise_from_outside_the_configurations_is_ignored() {
    let mut p = opened(vec![config(&[0, 1, 2]), config(&[2, 3, 4])]);
    assert_eq!(
        p.fold_promise(
            NodeId(7),
            ballot(1, 0),
            Slot(0),
            BTreeMap::new(),
            BTreeMap::new(),
            Some(Slot(5)),
        ),
        PromiseFold::Ignored,
        "an outsider's continuation is not paged"
    );
    assert_eq!(fold_empty(&mut p, 7, Slot(0)), PromiseFold::Ignored);
    assert!(
        !p.phase1_won(ballot(1, 0)),
        "an outsider's answer counts nowhere"
    );
    // A member of a prior configuration only is answered, as before.
    assert_eq!(fold_empty(&mut p, 3, Slot(0)), PromiseFold::Answered);
    // The probe pages its prior configurations' stragglers alone.
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(0), ballot(0, 2));
    p.fold_promise(
        NodeId(1),
        ballot(1, 0),
        Slot(0),
        BTreeMap::new(),
        faulty,
        None,
    );
    let out = p.close_phase1(|_| false);
    assert!(out.blocked.contains(&Slot(0)));
    assert_eq!(
        fold_empty_probe(&mut p, 7, ballot(1, 0), Slot(0)),
        PromiseFold::Ignored,
        "an outsider's straggler page is ignored"
    );
    assert_eq!(
        fold_empty_probe(&mut p, 2, ballot(1, 0), Slot(0)),
        PromiseFold::Answered
    );
}

/// An election reports the first slot it asked from, and the reports it
/// merged so far, before it closes.
#[test]
fn an_open_election_reports_its_first_slot_and_its_merge() {
    let mut p: Proposer<NodeId, Command> = Proposer::new();
    let _ = p.open_phase1(
        Campaign {
            me: Some(NodeId(0)),
            ballot: ballot(1, 0),
            config: config(&[0, 1, 2]),
            prior: vec![config(&[0, 1, 2])],
            from_slot: Slot(3),
        },
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert_eq!(p.election().map(Election::from_slot), Some(Slot(3)));
    let mut report = BTreeMap::new();
    report.insert(Slot(4), (ballot(0, 1), cmd(4)));
    let _ = p.fold_promise(
        NodeId(1),
        ballot(1, 0),
        Slot(3),
        report.clone(),
        BTreeMap::new(),
        None,
    );
    assert_eq!(p.election().map(Election::recovered), Some(&report));
}

/// The tally's own invariants name a sender that is both answered and
/// owed a page.
#[test]
#[should_panic(expected = "an answered sender owes no further page")]
fn an_answered_sender_that_owes_a_page_breaks_the_check() {
    let mut tally: PromiseTally<NodeId> = PromiseTally::new(ballot(1, 0), Slot(0), BTreeSet::new());
    tally.answered.insert(NodeId(1));
    tally.promise_next.insert(NodeId(1), Slot(4));
    tally.assert_invariants();
}

/// A candidate holds no repair probe: the proposer's check names it.
#[test]
#[should_panic(expected = "a candidate holds no repair probe")]
fn a_candidate_with_a_repair_probe_breaks_the_check() {
    let mut p = opened(vec![config(&[0, 1, 2])]);
    let mut leader = blocked_on_two_slots();
    p.probe = leader.probe.take();
    assert!(p.probe.is_some(), "the leader holds a probe");
    p.assert_invariants();
}

/// A recovery's cursor is where it was opened, and moves one slot per step.
#[test]
fn a_recovery_cursor_starts_where_it_was_opened() {
    let mut p: Proposer<NodeId, Command> = Proposer::new();
    p.open_recovery(
        BTreeMap::new(),
        BTreeSet::new(),
        Slot(3),
        Slot(5),
        RecoveryPolicy::Phase1Backed,
    );
    assert_eq!(p.recovery().map(Recovery::cursor), Some(Slot(3)));
    let _ = p.recovery_next();
    assert_eq!(p.recovery().map(Recovery::cursor), Some(Slot(4)));
}

/// The standing authority the proposer reports is the one it opened.
#[test]
fn the_reported_authority_is_the_open_one() {
    let mut p: Proposer<NodeId, Command> = Proposer::new();
    p.raise_next_slot(Slot(8));
    p.open_authority(Some(Slot(6)), Some(NodeId(0)));
    assert_eq!(p.authority().fence(), Some(Slot(6)));
}

/// The probe's tally is over its blocked slots only: the check names a
/// tally over a slot that is not blocked.
#[test]
#[should_panic(expected = "the probe's have-tally is over blocked slots only")]
fn a_probe_tally_over_an_unblocked_slot_breaks_the_check() {
    let mut p = blocked_on_two_slots();
    p.probe
        .as_mut()
        .expect("a probe")
        .best_have
        .insert(Slot(9), (ballot(0, 1), cmd(9)));
    p.assert_invariants();
}

/// The recovery pump hands a blocked slot out as `Blocked`, never as a
/// re-proposal or a fill: slot 0 has a reported command and slot 1 none,
/// and both are the open repair probe's to resolve.
#[test]
fn the_recovery_pump_hands_out_a_blocked_slot_as_blocked() {
    let mut p: Proposer<NodeId, Command> = Proposer::new();
    let recovered: BTreeMap<Slot, Command> = [(Slot(0), cmd(5))].into_iter().collect();
    let blocked: BTreeSet<Slot> = [Slot(0), Slot(1)].into_iter().collect();
    p.open_recovery(
        recovered,
        blocked,
        Slot(0),
        Slot(3),
        RecoveryPolicy::Phase1Backed,
    );
    assert_eq!(p.recovery_next(), Some((Slot(0), RecoveryStep::Blocked)));
    assert_eq!(p.recovery_next(), Some((Slot(1), RecoveryStep::Blocked)));
    assert_eq!(p.recovery_next(), Some((Slot(2), RecoveryStep::Fill)));
    assert_eq!(p.recovery_next(), None);
}
