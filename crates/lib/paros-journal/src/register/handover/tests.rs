use super::*;
use crate::register::member::RegisterMember;
use crate::register::test_support::{deliver, ids, members, set};
use crate::register::value::{Change, RegisterValue};
use crate::register::{CallStep, MemberPhase, RegisterCall};

fn op(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

type Pool = BTreeMap<MatchmakerId, RegisterMember>;

/// Run `handover` to its end over the members in `up`.
fn run(handover: &mut RegisterHandover, pool: &mut Pool, up: &[u64]) -> Vec<HandoverStep> {
    let mut steps = Vec::new();
    for _ in 0..64 {
        let replies = deliver(pool, handover.requests(), up);
        if replies.is_empty() {
            return steps;
        }
        for (from, reply) in replies {
            let step = handover.on_reply(from, reply);
            if step != HandoverStep::Pending {
                steps.push(step);
            }
        }
    }
    panic!("a handover ends");
}

fn elect_at(pool: &mut Pool, first: &MatchmakerSet, round: u64, up: &[u64]) -> CallStep {
    let mut call = RegisterCall::new(
        op(round, 1),
        first.clone(),
        Change::Elect { holder: NodeId(5) },
    );
    let mut last = CallStep::Pending;
    for _ in 0..4 {
        for (from, reply) in deliver(pool, call.requests(), up) {
            let step = call.on_reply(from, reply);
            if step != CallStep::Pending {
                last = step;
            }
        }
    }
    last
}

#[test]
fn a_handover_carries_every_done_write_to_the_next_set() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3, 4]);
    // A write done at {1, 2}: member 3 never took it.
    assert!(matches!(
        elect_at(&mut pool, &first, 1, &[1, 2]),
        CallStep::Done(_)
    ));
    let mut handover = RegisterHandover::new(NodeId(9), 0);
    handover
        .start(first.clone(), ids(&[2, 3, 4]))
        .expect("started");
    // The stop majority is {2, 3}: it holds the write at member 2.
    let steps = run(&mut handover, &mut pool, &[2, 3, 4]);
    let Some(HandoverStep::Done(successor)) = steps.last() else {
        panic!("the handover ends: {steps:?}");
    };
    assert_eq!(successor.set, set(1, &[2, 3, 4]));
    assert_eq!(successor.written.op, op(1, 1));
    assert!(steps.contains(&HandoverStep::Round(1)));
    assert_eq!(handover.round(), 1);
    assert!(matches!(handover.phase(), HandoverPhase::Idle));
    // The next set serves the write: a read there sees term 1.
    let next = set(1, &[2, 3, 4]);
    let mut call = RegisterCall::new(op(5, 1), next, Change::Elect { holder: NodeId(6) });
    let mut done = None;
    for _ in 0..4 {
        for (from, reply) in deliver(&mut pool, call.requests(), &[3, 4]) {
            if let CallStep::Done(value) = call.on_reply(from, reply) {
                done = Some(value);
            }
        }
    }
    assert_eq!(done.map(|v| v.term().number), Some(2));
    // The old set is frozen and names its successor.
    assert!(matches!(
        elect_at(&mut pool, &first, 7, &[1, 2, 3]),
        CallStep::Moved { set: Some(s) } if s.generation == MatchmakerGeneration(1)
    ));
}

#[test]
fn a_handover_waits_for_every_member_of_the_next_set() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3, 4]);
    let mut handover = RegisterHandover::new(NodeId(9), 0);
    handover.start(first, ids(&[2, 3, 4])).expect("started");
    let steps = run(&mut handover, &mut pool, &[1, 2, 3]);
    assert!(matches!(steps.as_slice(), [HandoverStep::Closed(_)]));
    assert!(matches!(
        handover.phase(),
        HandoverPhase::Bootstrapping { .. }
    ));
    assert_eq!(
        handover.start(set(0, &[1]), ids(&[1])),
        Err(HandoverRefusal::Busy)
    );
    handover.abandon();
    assert_eq!(
        handover.start(set(0, &[1]), Vec::new()),
        Err(HandoverRefusal::Empty)
    );
}

#[test]
fn two_handovers_choose_one_successor() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3, 4, 5]);
    let mut a = RegisterHandover::new(NodeId(8), 0);
    let mut b = RegisterHandover::new(NodeId(9), 0);
    a.start(first.clone(), ids(&[1, 2, 4])).expect("a");
    b.start(first, ids(&[1, 2, 5])).expect("b");
    // a gets a vote at member 1 only, then b runs to its end.
    for _ in 0..3 {
        for (from, reply) in deliver(&mut pool, a.requests(), &[1, 2, 3, 4]) {
            a.on_reply(from, reply);
        }
    }
    assert!(matches!(a.phase(), HandoverPhase::Deciding { .. }));
    for (from, reply) in deliver(&mut pool, a.requests(), &[1]) {
        a.on_reply(from, reply);
    }
    let steps = run(&mut b, &mut pool, &[1, 2, 3, 4, 5]);
    // b's decree saw a's vote, and chose it (P2c).
    let Some(HandoverStep::Done(chosen)) = steps.last() else {
        panic!("b ends: {steps:?}");
    };
    assert_eq!(chosen.set, set(1, &[1, 2, 4]));
    // a learns that a later set is in force.
    let steps = run(&mut a, &mut pool, &[1, 2, 3, 4, 5]);
    assert!(
        steps
            .iter()
            .any(|s| matches!(s, HandoverStep::Superseded(_) | HandoverStep::Done(_))),
        "{steps:?}"
    );
}

#[test]
fn a_nack_opens_a_round_above_the_promise() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3, 4]);
    // An earlier handover promised round 5 at member 2.
    pool.get_mut(&MatchmakerId(2))
        .expect("2")
        .step(Request::Prepare {
            generation: first.generation,
            ballot: op(5, 7),
        });
    let mut handover = RegisterHandover::new(NodeId(9), 0);
    handover.start(first, ids(&[2, 3, 4])).expect("started");
    let steps = run(&mut handover, &mut pool, &[1, 2, 3, 4]);
    // The stop reported the promise: the decree opens above it at once.
    assert!(steps.contains(&HandoverStep::Round(6)), "{steps:?}");
    assert!(matches!(steps.last(), Some(HandoverStep::Done(_))));
    let _ = RegisterValue::new();
}

#[test]
fn a_new_incarnation_opens_above_its_durable_round() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3, 4]);
    let mut handover = RegisterHandover::new(NodeId(9), 11);
    handover.start(first, ids(&[2, 3, 4])).expect("started");
    let steps = run(&mut handover, &mut pool, &[1, 2, 3, 4]);
    assert!(steps.contains(&HandoverStep::Round(12)), "{steps:?}");
}

#[test]
fn only_a_later_set_supersedes_the_handover() {
    let first = set(0, &[1, 2, 3]);
    let mut handover = RegisterHandover::new(NodeId(9), 0);
    handover
        .start(first.clone(), ids(&[2, 3, 4]))
        .expect("started");
    let refused = |successor| Reply::Refused {
        set: first.clone(),
        phase: MemberPhase::Frozen,
        successor,
    };
    // The set it hands over, as the set or as a successor, is not later.
    assert_eq!(
        handover.on_reply(MatchmakerId(1), refused(None)),
        HandoverStep::Pending
    );
    assert_eq!(
        handover.on_reply(MatchmakerId(1), refused(Some(first.clone()))),
        HandoverStep::Pending
    );
    assert!(matches!(handover.phase(), HandoverPhase::Stopping { .. }));
    let later = set(1, &[1, 2, 4]);
    assert_eq!(
        handover.on_reply(MatchmakerId(1), refused(Some(later.clone()))),
        HandoverStep::Superseded(later)
    );
    assert!(matches!(handover.phase(), HandoverPhase::Idle));
}

#[test]
fn a_stop_report_of_another_generation_is_not_counted() {
    let first = set(0, &[1, 2, 3]);
    let mut handover = RegisterHandover::new(NodeId(9), 0);
    handover.start(first, ids(&[2, 3, 4])).expect("started");
    let stopped = |generation| Reply::Stopped {
        generation: MatchmakerGeneration(generation),
        written: Written::default(),
        read: Ballot::zero(),
        promised: Ballot::zero(),
    };
    assert_eq!(
        handover.on_reply(MatchmakerId(1), stopped(5)),
        HandoverStep::Pending
    );
    assert_eq!(
        handover.on_reply(MatchmakerId(2), stopped(0)),
        HandoverStep::Pending
    );
    assert!(matches!(handover.phase(), HandoverPhase::Stopping { .. }));
    assert!(matches!(
        handover.on_reply(MatchmakerId(3), stopped(0)),
        HandoverStep::Closed(_)
    ));
}

#[test]
fn a_bootstrap_ack_of_another_state_is_not_counted() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3, 4]);
    let mut handover = RegisterHandover::new(NodeId(9), 0);
    handover.start(first, ids(&[2, 3, 4])).expect("started");
    for (from, reply) in deliver(&mut pool, handover.requests(), &[1, 2, 3]) {
        handover.on_reply(from, reply);
    }
    assert!(matches!(
        handover.phase(),
        HandoverPhase::Bootstrapping { .. }
    ));
    for id in [2, 3, 4] {
        assert_eq!(
            handover.on_reply(MatchmakerId(id), Reply::Bootstrapped { successor: 0 }),
            HandoverStep::Pending
        );
    }
    assert!(matches!(
        handover.phase(),
        HandoverPhase::Bootstrapping { .. }
    ));
}

#[test]
fn the_round_it_opened_is_the_round_to_persist() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3, 4]);
    let mut handover = RegisterHandover::new(NodeId(9), 11);
    assert_eq!(handover.round(), 11);
    handover.start(first, ids(&[2, 3, 4])).expect("started");
    run(&mut handover, &mut pool, &[1, 2, 3, 4]);
    assert_eq!(handover.round(), 12);
}
