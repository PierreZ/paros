use super::*;
use crate::register::test_support::{deliver, members, reshape, set};
use crate::register::{MemberPhase, RegisterMember};
use paros_consensus::NodeId;

fn op(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(1),
    }
}

fn elect() -> Change {
    Change::Elect { holder: NodeId(5) }
}

/// Run `call` to its end over the members in `up`.
fn run(
    call: &mut RegisterCall,
    pool: &mut BTreeMap<MatchmakerId, RegisterMember>,
    up: &[u64],
) -> Vec<CallStep> {
    let mut steps = Vec::new();
    loop {
        let requests = call.requests();
        if requests.is_empty() {
            return steps;
        }
        let replies = deliver(pool, requests, up);
        if replies.is_empty() {
            return steps;
        }
        for (from, reply) in replies {
            let step = call.on_reply(from, reply);
            if step != CallStep::Pending {
                steps.push(step);
            }
        }
    }
}

#[test]
fn an_operation_is_done_when_a_majority_took_its_write() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3]);
    let mut call = RegisterCall::new(op(1), first.clone(), elect());
    let steps = run(&mut call, &mut pool, &[1, 3]);
    let elected = RegisterValue::new().apply(&elect()).expect("elected");
    assert_eq!(
        steps,
        vec![
            CallStep::Write {
                seen: RegisterValue::new(),
                value: elected.clone()
            },
            CallStep::Done(elected.clone())
        ]
    );
    assert_eq!(call.phase(), &CallPhase::Done(elected));
    assert_eq!(call.requests(), Vec::new());
    // Member 2 never heard it.
    assert_eq!(pool[&MatchmakerId(2)].state().written.op, Ballot::zero());
}

#[test]
fn a_minority_is_never_done() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3]);
    let mut call = RegisterCall::new(op(1), first, elect());
    let steps = run(&mut call, &mut pool, &[2]);
    assert_eq!(steps, Vec::new());
    assert!(matches!(call.phase(), CallPhase::Reading(_)));
}

#[test]
fn a_read_takes_the_highest_write_and_the_guard_refuses_on_it() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3]);
    run(
        &mut RegisterCall::new(op(1), first.clone(), elect()),
        &mut pool,
        &[1, 2],
    );
    // The reshape of term 0 is refused: the read saw term 1 at member 2.
    let mut call = RegisterCall::new(op(2), first, reshape(0, None));
    let steps = run(&mut call, &mut pool, &[2, 3]);
    assert!(matches!(
        steps.as_slice(),
        [CallStep::Refused { refusal: ChangeRefusal::Term { held: 1, asked: 0 }, seen }]
            if seen.term().number == 1
    ));
    assert_eq!(call.phase(), &CallPhase::Ended);
}

#[test]
fn a_later_read_makes_the_write_a_conflict() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3]);
    let mut early = RegisterCall::new(op(1), first.clone(), elect());
    let replies = deliver(&mut pool, early.requests(), &[1, 2, 3]);
    for (from, reply) in replies {
        early.on_reply(from, reply);
    }
    // A later operation reads at every member before the early write lands.
    let late = RegisterCall::new(op(2), first, elect());
    deliver(&mut pool, late.requests(), &[1, 2, 3]);
    let steps = run(&mut early, &mut pool, &[1, 2, 3]);
    assert_eq!(steps, vec![CallStep::Conflict { above: op(2) }]);
    assert_eq!(early.requests(), Vec::new());
}

#[test]
fn a_frozen_set_moves_the_operation_once_the_successor_is_known() {
    let first = set(0, &[1, 2, 3]);
    let mut call = RegisterCall::new(op(1), first.clone(), elect());
    let frozen = |successor| Reply::Refused {
        set: first.clone(),
        phase: MemberPhase::Frozen,
        successor,
    };
    assert_eq!(
        call.on_reply(MatchmakerId(1), frozen(None)),
        CallStep::Moved { set: None }
    );
    assert!(matches!(call.phase(), CallPhase::Reading(_)));
    let next = set(1, &[1, 2, 4]);
    assert_eq!(
        call.on_reply(MatchmakerId(2), frozen(Some(next.clone()))),
        CallStep::Moved { set: Some(next) }
    );
    assert_eq!(call.phase(), &CallPhase::Ended);
    // A reply from outside the set, or after the end, does nothing.
    assert_eq!(
        call.on_reply(MatchmakerId(9), frozen(None)),
        CallStep::Pending
    );
    assert_eq!(
        call.on_reply(MatchmakerId(3), frozen(None)),
        CallStep::Pending
    );
    assert_eq!(call.op(), op(1));
    assert_eq!(call.set(), &first);
    assert_eq!(call.change(), &elect());
}

#[test]
fn a_reply_of_another_operation_does_nothing() {
    let first = set(0, &[1, 2, 3]);
    let mut call = RegisterCall::new(op(3), first, elect());
    assert_eq!(
        call.on_reply(
            MatchmakerId(1),
            Reply::Value {
                op: op(2),
                written: Written::default()
            }
        ),
        CallStep::Pending
    );
    assert_eq!(
        call.on_reply(MatchmakerId(1), Reply::Wrote { op: op(3) }),
        CallStep::Pending
    );
    assert_eq!(
        call.on_reply(
            MatchmakerId(1),
            Reply::Conflict {
                op: op(3),
                read: op(4),
                written: op(4)
            }
        ),
        CallStep::Pending
    );
}

#[test]
fn a_conflict_of_another_operation_does_not_end_the_write() {
    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3]);
    let mut call = RegisterCall::new(op(3), first, elect());
    for (from, reply) in deliver(&mut pool, call.requests(), &[1, 2]) {
        call.on_reply(from, reply);
    }
    assert!(matches!(call.phase(), CallPhase::Writing { .. }));
    assert_eq!(
        call.on_reply(
            MatchmakerId(1),
            Reply::Conflict {
                op: op(2),
                read: op(4),
                written: op(4)
            }
        ),
        CallStep::Pending
    );
    assert!(matches!(call.phase(), CallPhase::Writing { .. }));
}

#[test]
fn a_successor_of_the_same_generation_does_not_move_the_operation() {
    let first = set(0, &[1, 2, 3]);
    let mut call = RegisterCall::new(op(1), first.clone(), elect());
    assert_eq!(
        call.on_reply(
            MatchmakerId(1),
            Reply::Refused {
                set: first.clone(),
                phase: MemberPhase::Frozen,
                successor: Some(first),
            }
        ),
        CallStep::Moved { set: None }
    );
    assert!(matches!(call.phase(), CallPhase::Reading(_)));
}

/// Run `change` as operation `round` at every member of `set`: it is done.
fn done(
    pool: &mut BTreeMap<MatchmakerId, RegisterMember>,
    set: &MatchmakerSet,
    round: u64,
    change: Change,
) -> RegisterValue {
    let mut call = RegisterCall::new(op(round), set.clone(), change);
    match run(&mut call, pool, &[1, 2, 3]).pop() {
        Some(CallStep::Done(value)) => value,
        other => panic!("the change is done: {other:?}"),
    }
}

/// A leader's seal reaches one member only, and its answer is lost. A
/// replica's boundary write reads that member, finds the half-written seal
/// and writes it on at a majority together with the boundary state. The
/// leader asks its seal again: the register holds its own end, so the seal
/// is ok again and done, never refused (the leader would stop for good).
#[test]
fn a_seal_asked_again_over_its_own_half_write_is_done() {
    use crate::command::Leader;
    use crate::fold::BoundaryState;
    use crate::segment::{SegmentId, SegmentRecord};
    use paros_consensus::Slot;
    use paros_consensus::journal::{LeaderUuid, Seq};

    let first = set(0, &[1, 2, 3]);
    let mut pool = members(&first, &[1, 2, 3]);
    done(&mut pool, &first, 1, elect());
    done(&mut pool, &first, 2, reshape(1, None));
    done(&mut pool, &first, 3, reshape(1, Some(1)));
    let seal = Change::Seal {
        segment: SegmentId(1),
        end: Slot(4),
    };
    let mut half = RegisterCall::new(op(4), first.clone(), seal.clone());
    for (from, reply) in deliver(&mut pool, half.requests(), &[1, 2, 3]) {
        half.on_reply(from, reply);
    }
    assert!(matches!(half.phase(), CallPhase::Writing { .. }));
    for (from, reply) in deliver(&mut pool, half.requests(), &[1]) {
        assert_eq!(half.on_reply(from, reply), CallStep::Pending);
    }
    let state = BoundaryState {
        next_seq: Seq(2),
        first_seq: Seq(0),
        leader: Leader::multi(LeaderUuid(7)),
    };
    let mut bound = RegisterCall::new(
        op(5),
        first.clone(),
        Change::Boundary {
            segment: SegmentId(1),
            state,
        },
    );
    match run(&mut bound, &mut pool, &[1, 2]).pop() {
        Some(CallStep::Done(value)) => assert_eq!(
            value
                .segment(SegmentId(1))
                .and_then(SegmentRecord::boundary),
            Some(state)
        ),
        other => panic!("the boundary write is done: {other:?}"),
    }
    let value = done(&mut pool, &first, 6, seal);
    assert_eq!(
        value.segment(SegmentId(1)).and_then(SegmentRecord::end),
        Some(Slot(4))
    );
    // Without the boundary write, a seal asked again over its own half
    // write is done too.
    let mut pool = members(&first, &[1, 2, 3]);
    done(&mut pool, &first, 1, elect());
    done(&mut pool, &first, 2, reshape(1, None));
    done(&mut pool, &first, 3, reshape(1, Some(1)));
    let seal = Change::Seal {
        segment: SegmentId(1),
        end: Slot(4),
    };
    let mut half = RegisterCall::new(op(4), first.clone(), seal.clone());
    for (from, reply) in deliver(&mut pool, half.requests(), &[1, 2, 3]) {
        half.on_reply(from, reply);
    }
    for (from, reply) in deliver(&mut pool, half.requests(), &[1]) {
        half.on_reply(from, reply);
    }
    let mut again = RegisterCall::new(op(5), first, seal);
    let steps = run(&mut again, &mut pool, &[1, 2]);
    assert!(
        matches!(steps.last(), Some(CallStep::Done(value)) if value.segment(SegmentId(1)).and_then(SegmentRecord::end) == Some(Slot(4))),
        "{steps:?}"
    );
}
