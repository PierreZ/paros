use super::*;
use crate::register::test_support::{ids, set};
use crate::register::value::{Change, RegisterValue};

fn op(round: u64) -> Ballot {
    Ballot {
        round,
        node: paros_consensus::NodeId(1),
    }
}

fn member(id: u64) -> RegisterMember {
    RegisterMember::new(
        MatchmakerId(id),
        MemberState::first(MatchmakerId(id), set(0, &[1, 2, 3])),
    )
}

fn value() -> RegisterValue {
    RegisterValue::new()
        .apply(&Change::Elect {
            holder: paros_consensus::NodeId(4),
        })
        .expect("elected")
}

fn read(round: u64) -> Request {
    Request::Read {
        generation: MatchmakerGeneration(0),
        op: op(round),
    }
}

fn write(round: u64) -> Request {
    Request::Write {
        generation: MatchmakerGeneration(0),
        op: op(round),
        value: value(),
    }
}

fn successor(generation: u64, members: &[u64], written_round: u64) -> Successor {
    Successor {
        set: set(generation, members),
        written: Written {
            op: op(written_round),
            value: value(),
        },
        read: op(written_round),
    }
}

#[test]
fn a_member_of_the_first_set_is_active_and_a_spare_is_not() {
    assert!(member(1).serves(MatchmakerGeneration(0)));
    let spare = RegisterMember::new(
        MatchmakerId(9),
        MemberState::first(MatchmakerId(9), set(0, &[1, 2, 3])),
    );
    assert_eq!(spare.state().phase, MemberPhase::Inactive);
    assert!(!spare.serves(MatchmakerGeneration(0)));
}

#[test]
fn a_read_raises_the_read_generation_and_answers_the_highest_write() {
    let mut m = member(1);
    let step = m.step(read(5));
    assert!(step.persist);
    assert_eq!(
        step.reply,
        Reply::Value {
            op: op(5),
            written: Written::default()
        }
    );
    // A lower read leaves the read generation where it is.
    assert!(!m.step(read(3)).persist);
    assert_eq!(m.state().read, op(5));
}

#[test]
fn a_write_lands_only_above_every_read_and_write() {
    let mut m = member(1);
    m.step(read(5));
    assert_eq!(
        m.step(write(4)).reply,
        Reply::Conflict {
            op: op(4),
            read: op(5),
            written: Ballot::zero()
        }
    );
    let step = m.step(write(5));
    assert!(step.persist);
    assert_eq!(step.reply, Reply::Wrote { op: op(5) });
    // The same write again is the same answer, and changes nothing.
    let again = m.step(write(5));
    assert_eq!(
        (again.persist, again.reply),
        (false, Reply::Wrote { op: op(5) })
    );
    // Another value at the same generation is a conflict.
    let other = m.step(Request::Write {
        generation: MatchmakerGeneration(0),
        op: op(5),
        value: RegisterValue::new(),
    });
    assert!(matches!(other.reply, Reply::Conflict { .. }));
    assert_eq!(m.state().written.op, op(5));
}

#[test]
fn a_member_refuses_another_generation() {
    let mut m = member(1);
    let reply = m
        .step(Request::Read {
            generation: MatchmakerGeneration(1),
            op: op(1),
        })
        .reply;
    assert_eq!(
        reply,
        Reply::Refused {
            set: set(0, &[1, 2, 3]),
            phase: MemberPhase::Active,
            successor: None
        }
    );
}

#[test]
fn a_frozen_member_takes_no_read_and_no_write_and_answers_the_decree() {
    let mut m = member(1);
    m.step(write(2));
    let stopped = m.step(Request::Stop {
        generation: MatchmakerGeneration(0),
    });
    assert!(stopped.persist);
    assert!(matches!(stopped.reply, Reply::Stopped { written, .. } if written.op == op(2)));
    assert!(matches!(
        m.step(read(9)).reply,
        Reply::Refused {
            phase: MemberPhase::Frozen,
            ..
        }
    ));
    assert!(matches!(m.step(write(9)).reply, Reply::Refused { .. }));
    assert_eq!(m.state().read, Ballot::zero());
    let prepared = m.step(Request::Prepare {
        generation: MatchmakerGeneration(0),
        ballot: op(3),
    });
    assert_eq!(
        prepared.reply,
        Reply::Promised {
            ballot: op(3),
            vote: None
        }
    );
    // A lower decree ballot is refused with the promise.
    assert_eq!(
        m.step(Request::Accept {
            generation: MatchmakerGeneration(0),
            ballot: op(2),
            successor: successor(1, &[1, 2, 4], 2),
        })
        .reply,
        Reply::Nacked {
            ballot: op(2),
            promised: op(3)
        }
    );
    let accepted = m.step(Request::Accept {
        generation: MatchmakerGeneration(0),
        ballot: op(3),
        successor: successor(1, &[1, 2, 4], 2),
    });
    assert_eq!(accepted.reply, Reply::Accepted { ballot: op(3) });
    // The vote is reported to a later Phase 1.
    assert!(matches!(
        m.step(Request::Prepare { generation: MatchmakerGeneration(0), ballot: op(4) }).reply,
        Reply::Promised { vote: Some((b, _)), .. } if b == op(3)
    ));
    // A decree value that is not one generation above is refused.
    assert!(matches!(
        m.step(Request::Accept {
            generation: MatchmakerGeneration(0),
            ballot: op(5),
            successor: successor(2, &[1, 2, 4], 2),
        })
        .reply,
        Reply::Refused { .. }
    ));
}

#[test]
fn a_new_member_stores_the_bootstrap_and_serves_the_chosen_state() {
    let mut spare = RegisterMember::new(
        MatchmakerId(4),
        MemberState::first(MatchmakerId(4), set(0, &[1, 2, 3])),
    );
    let next = successor(1, &[1, 2, 4], 6);
    let reply = spare
        .step(Request::Bootstrap {
            successor: next.clone(),
        })
        .reply;
    assert_eq!(
        reply,
        Reply::Bootstrapped {
            successor: next.fingerprint()
        }
    );
    // A bootstrap that does not name it is refused.
    assert!(matches!(
        spare
            .step(Request::Bootstrap {
                successor: successor(1, &[1, 2, 3], 6)
            })
            .reply,
        Reply::Refused { .. }
    ));
    let learned = spare.step(Request::Chosen {
        successor: next.clone(),
    });
    assert_eq!(
        learned.reply,
        Reply::Learned {
            generation: MatchmakerGeneration(1)
        }
    );
    assert!(spare.serves(MatchmakerGeneration(1)));
    assert_eq!(spare.state().written, next.written);
    // A late bootstrap of the generation it serves is refused.
    assert!(matches!(
        spare
            .step(Request::Bootstrap {
                successor: next.clone()
            })
            .reply,
        Reply::Refused { .. }
    ));
    // The chosen state again: learned again, nothing changes.
    let again = spare.step(Request::Chosen { successor: next });
    assert!(!again.persist);
}

#[test]
fn an_activated_member_keeps_its_own_higher_write() {
    let mut m = member(2);
    m.step(read(9));
    m.step(write(9));
    m.step(Request::Stop {
        generation: MatchmakerGeneration(0),
    });
    let next = successor(1, &[1, 2, 4], 6);
    m.step(Request::Chosen { successor: next });
    assert!(m.serves(MatchmakerGeneration(1)));
    assert_eq!((m.state().written.op, m.state().read), (op(9), op(9)));
}

#[test]
fn an_old_member_outside_the_next_set_records_the_successor() {
    let mut m = member(3);
    m.step(Request::Stop {
        generation: MatchmakerGeneration(0),
    });
    let next = successor(1, &[1, 2, 4], 6);
    assert_eq!(
        m.step(Request::Chosen {
            successor: next.clone()
        })
        .reply,
        Reply::Learned {
            generation: MatchmakerGeneration(1)
        }
    );
    assert_eq!(
        m.step(read(1)).reply,
        Reply::Refused {
            set: set(0, &[1, 2, 3]),
            phase: MemberPhase::Frozen,
            successor: Some(next.set)
        }
    );
    // A spare that never heard of the set refuses the chosen state.
    let mut spare = RegisterMember::new(
        MatchmakerId(8),
        MemberState::first(
            MatchmakerId(8),
            set(0, &ids(&[1, 2, 3]).iter().map(|m| m.0).collect::<Vec<_>>()),
        ),
    );
    assert!(matches!(
        spare
            .step(Request::Chosen {
                successor: successor(1, &[1, 2, 4], 6)
            })
            .reply,
        Reply::Refused { .. }
    ));
}

#[test]
fn a_member_keeps_its_id() {
    assert_eq!(member(2).id(), MatchmakerId(2));
}

#[test]
#[should_panic(expected = "a member of its set is active or frozen")]
fn a_member_of_its_set_that_is_a_spare_is_refused() {
    let mut state = MemberState::first(MatchmakerId(1), set(0, &[1, 2, 3]));
    state.phase = MemberPhase::Inactive;
    let _ = RegisterMember::new(MatchmakerId(1), state);
}

#[test]
fn a_late_chosen_state_does_not_wake_a_frozen_member_of_its_generation() {
    let mut spare = RegisterMember::new(
        MatchmakerId(4),
        MemberState::first(MatchmakerId(4), set(0, &[1, 2, 3])),
    );
    let next = successor(1, &[1, 2, 4], 6);
    spare.step(Request::Chosen {
        successor: next.clone(),
    });
    assert!(spare.serves(MatchmakerGeneration(1)));
    // The next handover stops generation 1, then the chosen state of
    // generation 1 arrives again.
    spare.step(Request::Stop {
        generation: MatchmakerGeneration(1),
    });
    let late = spare.step(Request::Chosen { successor: next });
    assert_eq!(
        late.reply,
        Reply::Learned {
            generation: MatchmakerGeneration(1)
        }
    );
    assert_eq!(spare.state().phase, MemberPhase::Frozen);
    assert!(!spare.serves(MatchmakerGeneration(1)));
}

#[test]
fn the_fingerprint_of_a_successor_sees_its_set_and_its_write() {
    let base = successor(1, &[1, 2, 4], 6);
    let fingerprints = [
        base.fingerprint(),
        successor(1, &[1, 2, 3], 6).fingerprint(),
        successor(2, &[1, 2, 4], 6).fingerprint(),
        successor(1, &[1, 2, 4], 7).fingerprint(),
    ];
    for (i, a) in fingerprints.iter().enumerate() {
        for b in &fingerprints[i + 1..] {
            assert_ne!(a, b);
        }
    }
    assert_eq!(base.fingerprint(), base.clone().fingerprint());
}

/// A successor weighs the bytes of its members' ids.
#[test]
fn a_successor_weighs_its_member_ids() {
    use paros_consensus::types::Weigh;
    assert_eq!(successor(1, &[1, 2, 3], 1).weight(), 24);
    assert_eq!(successor(1, &[4, 5], 1).weight(), 16);
}
