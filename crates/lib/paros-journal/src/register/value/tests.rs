use super::*;
use crate::codec::RELEASE;
use crate::command::Leader;
use crate::register::test_support::{opening, reshape};

fn elect(value: &RegisterValue) -> RegisterValue {
    value
        .apply(&Change::Elect { holder: NodeId(7) })
        .expect("an election is never refused")
}

/// Term 1, segment 1 in force at slot 0, segment 2 pending.
fn two_segments() -> RegisterValue {
    let value = elect(&RegisterValue::new());
    let value = value.apply(&reshape(1, None)).expect("the first segment");
    value
        .apply(&reshape(1, Some(1)))
        .expect("the second segment")
}

fn boundary() -> BoundaryState {
    BoundaryState {
        next_seq: Seq(4),
        first_seq: Seq(0),
        leader: Leader::multi(paros_consensus::journal::LeaderUuid(0x3A)),
    }
}

#[test]
fn an_election_raises_the_term_and_names_its_holder() {
    let value = elect(&elect(&RegisterValue::new()));
    assert_eq!(
        value.term(),
        Term {
            number: 2,
            holder: Some(NodeId(7))
        }
    );
}

#[test]
fn the_first_reshape_puts_its_segment_in_force_at_slot_zero() {
    let value = two_segments();
    let first = value.segment(SegmentId(1)).expect("segment 1");
    assert_eq!(first.state(), SegmentState::InForce);
    assert_eq!(first.start(), Some(Slot(0)));
    assert_eq!(
        value.segment(SegmentId(2)).map(SegmentRecord::state),
        Some(SegmentState::Pending)
    );
    assert_eq!(value.in_force().map(SegmentRecord::id), Some(SegmentId(1)));
    assert_eq!(value.newest().map(SegmentRecord::id), Some(SegmentId(2)));
}

#[test]
fn a_reshape_is_fenced_by_the_term_and_the_expected_segment() {
    let value = elect(&RegisterValue::new());
    assert_eq!(
        value.apply(&reshape(0, None)),
        Err(ChangeRefusal::Term { held: 1, asked: 0 })
    );
    assert_eq!(
        value.apply(&reshape(1, Some(1))),
        Err(ChangeRefusal::Expected {
            newest: None,
            asked: Some(SegmentId(1))
        })
    );
    // A deposed controller: a later election raised the term.
    let later = elect(&value.apply(&reshape(1, None)).expect("opened"));
    assert_eq!(
        later.apply(&reshape(1, Some(1))),
        Err(ChangeRefusal::Term { held: 2, asked: 1 })
    );
}

#[test]
fn a_reshape_the_register_took_is_ok_again() {
    let value = two_segments();
    assert_eq!(value.apply(&reshape(1, Some(1))), Ok(value.clone()));
    // Its term is old now: the identity still wins.
    let value = elect(&value);
    assert_eq!(value.apply(&reshape(1, Some(1))), Ok(value.clone()));
}

#[test]
fn a_reshape_needs_the_newest_in_force_and_the_older_retired() {
    let value = two_segments();
    assert_eq!(
        value.apply(&reshape(1, Some(2))),
        Err(ChangeRefusal::Unretired(SegmentId(1)))
    );
    let sealed = value
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(6),
        })
        .expect("sealed");
    assert_eq!(
        sealed.apply(&reshape(1, Some(2))),
        Err(ChangeRefusal::Unretired(SegmentId(1)))
    );
    let retired = sealed
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state: boundary(),
        })
        .and_then(|v| {
            v.apply(&Change::Retire {
                segment: SegmentId(1),
                holders: 2,
                unrepaired_damage: false,
            })
        })
        .expect("retired");
    let third = retired
        .apply(&reshape(1, Some(2)))
        .expect("a third segment");
    assert_eq!(
        third.newest().map(|r| (r.id(), r.state())),
        Some((SegmentId(3), SegmentState::Pending))
    );
}

#[test]
fn a_seal_is_written_once_and_puts_the_successor_in_force() {
    let value = two_segments();
    let seal = |end| Change::Seal {
        segment: SegmentId(1),
        end: Slot(end),
    };
    let sealed = value.apply(&seal(6)).expect("sealed");
    assert_eq!(
        sealed.segment(SegmentId(1)).map(|r| (r.state(), r.end())),
        Some((SegmentState::Sealed, Some(Slot(6))))
    );
    assert_eq!(
        sealed.in_force().map(|r| (r.id(), r.start())),
        Some((SegmentId(2), Some(Slot(6))))
    );
    // A second first leader that raced: its end is refused.
    assert_eq!(
        sealed.apply(&seal(8)),
        Err(ChangeRefusal::Record(
            SegmentId(1),
            RecordRefusal::State(SegmentState::Sealed)
        ))
    );
    // No pending successor: nothing to put in force.
    assert_eq!(
        sealed.apply(&Change::Seal {
            segment: SegmentId(2),
            end: Slot(9)
        }),
        Err(ChangeRefusal::NoSuccessor(SegmentId(2)))
    );
    assert_eq!(
        RegisterValue::new().apply(&seal(1)),
        Err(ChangeRefusal::NoSuccessor(SegmentId(1)))
    );
}

#[test]
fn a_record_refusal_names_its_segment_and_changes_nothing() {
    let value = two_segments();
    assert_eq!(
        value.apply(&Change::Retire {
            segment: SegmentId(1),
            holders: 3,
            unrepaired_damage: false
        }),
        Err(ChangeRefusal::Record(
            SegmentId(1),
            RecordRefusal::State(SegmentState::InForce)
        ))
    );
    assert_eq!(
        value.apply(&Change::Offload {
            segment: SegmentId(9)
        }),
        Err(ChangeRefusal::NoSegment(SegmentId(9)))
    );
    assert_eq!(
        value.apply(&Change::Trim {
            segment: SegmentId(9),
            first_seq: Seq(1)
        }),
        Err(ChangeRefusal::NoSegment(SegmentId(9)))
    );
}

#[test]
fn a_confirm_writes_the_value_again_only_in_its_state() {
    let value = two_segments();
    let confirm = |state| Change::Confirm {
        segment: SegmentId(1),
        state,
    };
    assert_eq!(
        value.apply(&confirm(SegmentState::InForce)),
        Ok(value.clone())
    );
    assert_eq!(
        value.apply(&confirm(SegmentState::Offloaded)),
        Err(ChangeRefusal::Confirm(SegmentState::InForce))
    );
    assert_eq!(
        value.apply(&Change::Confirm {
            segment: SegmentId(5),
            state: SegmentState::InForce
        }),
        Err(ChangeRefusal::NoSegment(SegmentId(5)))
    );
}

#[test]
fn a_sealed_segment_retires_is_offloaded_and_trimmed() {
    let value = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(6),
        })
        .expect("sealed");
    let one = SegmentId(1);
    let steps = [
        Change::Boundary {
            segment: one,
            state: boundary(),
        },
        Change::Retire {
            segment: one,
            holders: 2,
            unrepaired_damage: false,
        },
        Change::Offload { segment: one },
        Change::Trim {
            segment: one,
            first_seq: Seq(4),
        },
    ];
    let value = steps
        .iter()
        .try_fold(value, |value, step| value.apply(step))
        .expect("every step");
    assert_eq!(
        value.segment(one).map(SegmentRecord::state),
        Some(SegmentState::Trimmed)
    );
    assert_eq!(
        value.segment(one).map(SegmentRecord::release),
        Some(RELEASE)
    );
    assert_eq!(
        value.segment(one).map(SegmentRecord::opening),
        Some(&opening(1, None))
    );
}

/// Term 1, segment 1 retired with the replicas 10, 11, 12, segment 2 in
/// force.
fn retired_one() -> RegisterValue {
    let one = SegmentId(1);
    [
        Change::Seal {
            segment: one,
            end: Slot(6),
        },
        Change::Boundary {
            segment: one,
            state: boundary(),
        },
        Change::Retire {
            segment: one,
            holders: 2,
            unrepaired_damage: false,
        },
    ]
    .iter()
    .try_fold(two_segments(), |value, step| value.apply(step))
    .expect("segment 1 retires")
}

fn heal(read: &RegisterValue, term: u64, remove: u64, add: u64) -> Change {
    Change::Rereplicate {
        segment: SegmentId(1),
        read: read.segment(SegmentId(1)).expect("segment 1").clone(),
        term,
        remove: NodeId(remove),
        add: NodeId(add),
    }
}

fn replicas(value: &RegisterValue) -> Vec<NodeId> {
    value
        .segment(SegmentId(1))
        .expect("segment 1")
        .replicas()
        .to_vec()
}

#[test]
fn a_heal_swaps_a_member_of_the_record_it_read() {
    let value = retired_one();
    let healed = value.apply(&heal(&value, 1, 11, 20)).expect("a heal");
    assert_eq!(replicas(&healed), [NodeId(10), NodeId(20), NodeId(12)]);
    assert_eq!(healed.term(), value.term());
    assert_eq!(
        healed.apply(&heal(&value, 1, 11, 20)),
        Ok(healed.clone()),
        "a heal the register took is ok again"
    );
}

#[test]
fn a_delayed_heal_never_applies_on_a_set_that_came_back() {
    let value = retired_one();
    let first = heal(&value, 1, 11, 20);
    let healed = value.apply(&first).expect("11 out, 20 in");
    let back = healed
        .apply(&heal(&healed, 1, 20, 11))
        .expect("20 out, 11 in again");
    assert_eq!(replicas(&back), replicas(&value), "the same set again");
    assert_eq!(
        back.segment(SegmentId(1)).expect("segment 1").heals(),
        2,
        "the record counts both heals"
    );
    assert_eq!(
        back.apply(&first),
        Err(ChangeRefusal::Stale(SegmentId(1))),
        "a copy of the first heal, delayed in the network, finds a record it never read"
    );
}

#[test]
fn a_heal_is_fenced_by_the_term() {
    let value = retired_one();
    let elected = elect(&value);
    assert_eq!(
        elected.apply(&heal(&value, 1, 11, 20)),
        Err(ChangeRefusal::Term { held: 2, asked: 1 }),
        "a deposed controller stops"
    );
    assert!(elected.apply(&heal(&elected, 2, 11, 20)).is_ok());
}

#[test]
fn a_heal_on_an_old_read_is_refused() {
    let value = retired_one();
    let other = value.apply(&heal(&value, 1, 12, 21)).expect("a heal");
    assert_eq!(
        other.apply(&heal(&value, 1, 11, 20)),
        Err(ChangeRefusal::Stale(SegmentId(1))),
        "a heal of another plan since the read (control cas_blind)"
    );
    let trimmed = value
        .apply(&Change::Trim {
            segment: SegmentId(1),
            first_seq: Seq(4),
        })
        .expect("trimmed");
    assert_eq!(
        trimmed.apply(&heal(&value, 1, 11, 20)),
        Err(ChangeRefusal::Stale(SegmentId(1))),
        "a heal never undoes a trim (control cas_term)"
    );
    assert_eq!(
        trimmed.apply(&heal(&trimmed, 1, 11, 20)),
        Err(ChangeRefusal::Record(
            SegmentId(1),
            RecordRefusal::State(SegmentState::Trimmed)
        )),
        "a trimmed record stays trimmed"
    );
}

#[test]
fn a_heal_names_a_retired_segment_and_a_member() {
    let value = retired_one();
    let mut wrong = heal(&value, 1, 11, 20);
    if let Change::Rereplicate { segment, .. } = &mut wrong {
        *segment = SegmentId(9);
    }
    assert_eq!(
        value.apply(&wrong),
        Err(ChangeRefusal::NoSegment(SegmentId(9)))
    );
    let mut other = heal(&value, 1, 11, 20);
    if let Change::Rereplicate { segment, .. } = &mut other {
        *segment = SegmentId(2);
    }
    assert_eq!(
        value.apply(&other),
        Err(ChangeRefusal::Stale(SegmentId(2))),
        "the record read is of another segment"
    );
    assert_eq!(
        value.apply(&heal(&value, 1, 30, 20)),
        Err(ChangeRefusal::Record(
            SegmentId(1),
            RecordRefusal::NotMember(NodeId(30))
        ))
    );
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(6),
        })
        .expect("sealed");
    assert_eq!(
        sealed.apply(&heal(&sealed, 1, 11, 20)),
        Err(ChangeRefusal::Record(
            SegmentId(1),
            RecordRefusal::State(SegmentState::Sealed)
        )),
        "a sealed segment retires before a heal"
    );
}

#[test]
#[should_panic(expected = "a record is keyed by its id")]
fn a_record_under_another_id_is_refused() {
    let value = two_segments();
    let record = value.segment(SegmentId(1)).expect("segment 1").clone();
    let _ = RegisterValue::from_parts(value.term(), BTreeMap::from([(SegmentId(2), record)]));
}

#[test]
fn a_seal_needs_its_successor_pending() {
    // Segment 1 in force and segment 2 sealed: a register no reshape
    // builds, which the codec can read back.
    let mut first = SegmentRecord::open(SegmentId(1), RELEASE, opening(1, None)).expect("opened");
    first.put_in_force(Slot(0)).expect("in force");
    let mut second =
        SegmentRecord::open(SegmentId(2), RELEASE, opening(1, Some(1))).expect("opened");
    second.put_in_force(Slot(4)).expect("in force");
    second.seal(Slot(6)).expect("sealed");
    let value = RegisterValue::from_parts(
        Term {
            number: 1,
            holder: Some(NodeId(7)),
        },
        [(SegmentId(1), first), (SegmentId(2), second)].into(),
    );
    assert_eq!(
        value.apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(8)
        }),
        Err(ChangeRefusal::NoSuccessor(SegmentId(1)))
    );
}

#[test]
fn a_bind_takes_a_ballot_above_every_bound_one() {
    let ballot = |round| paros_consensus::Ballot {
        round,
        node: NodeId(7),
    };
    assert_eq!(
        RegisterValue::new().apply(&Change::Bind { ballot: ballot(2) }),
        Err(ChangeRefusal::Empty),
        "a journal with no segment has no configuration to bind"
    );
    let value = elect(&RegisterValue::new())
        .apply(&reshape(1, None))
        .expect("the first segment");
    assert_eq!(value.bound(), None);
    let bound = value
        .apply(&Change::Bind { ballot: ballot(2) })
        .expect("a first bind");
    assert_eq!(bound.binding(), Some((ballot(2), SegmentId(1))));
    assert_eq!(
        bound.apply(&Change::Bind { ballot: ballot(2) }),
        Ok(bound.clone()),
        "the bound ballot on the same newest segment is ok again and changes nothing"
    );
    assert_eq!(
        bound.apply(&Change::Bind { ballot: ballot(1) }),
        Err(ChangeRefusal::Bound {
            held: ballot(2),
            asked: ballot(1)
        }),
        "a bind below the bound ballot is refused"
    );
    let opened = bound
        .apply(&reshape(1, Some(1)))
        .expect("the second segment");
    assert_eq!(
        opened.apply(&Change::Bind { ballot: ballot(2) }),
        Err(ChangeRefusal::Bound {
            held: ballot(2),
            asked: ballot(2)
        }),
        "the bound ballot never takes a later segment"
    );
    let higher = opened
        .apply(&Change::Bind { ballot: ballot(3) })
        .expect("a higher bind");
    assert_eq!(higher.binding(), Some((ballot(3), SegmentId(2))));
    assert_eq!(
        higher.segments(),
        opened.segments(),
        "a bind changes no record"
    );
    assert_eq!(higher.term(), opened.term(), "a bind changes no term");
}
