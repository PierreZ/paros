use super::*;
use crate::command::{Fence, Record, Write};
use crate::fold::Answer;
use paros_consensus::journal::{LeaderUuid, Seq};

fn write(byte: u8) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: LeaderUuid(1),
            expected_seq: Seq(0),
        },
        records: vec![Record::new(vec![byte])],
    })
}

const WRITTEN: Answer = Answer::Written {
    seq: Seq(0),
    count: 1,
};

#[test]
fn a_slot_that_applied_the_proposal_answers_it() {
    let mut held = Held::new();
    held.hold(Slot(4), "a", write(1));
    let reply = held.applied(Slot(4), &write(1), &WRITTEN);
    assert_eq!(reply, Some(("a", Reply::Answer(WRITTEN))));
    assert!(held.is_empty());
}

#[test]
fn a_slot_that_chose_another_command_answers_unknown() {
    // The control `held_slot` of compartments/: answering from the slot's
    // outcome would ack a write that the slot never chose.
    let mut held = Held::new();
    held.hold(Slot(4), "a", write(1));
    assert_eq!(
        held.applied(Slot(4), &write(2), &WRITTEN),
        Some(("a", Reply::Unknown))
    );
    held.hold(Slot(5), "b", write(1));
    assert_eq!(
        held.applied(Slot(5), &Command::Noop, &Answer::Noop),
        Some(("b", Reply::Unknown))
    );
}

#[test]
fn a_slot_with_no_hold_answers_nobody() {
    let mut held: Held<&str> = Held::new();
    assert_eq!(held.applied(Slot(1), &write(1), &WRITTEN), None);
}

#[test]
fn a_leader_that_steps_down_answers_unknown_in_slot_order() {
    let mut held = Held::new();
    held.hold(Slot(7), "b", write(2));
    held.hold(Slot(3), "a", write(1));
    assert_eq!(held.len(), 2);
    assert_eq!(
        held.release_all(),
        vec![("a", Reply::Unknown), ("b", Reply::Unknown)]
    );
    assert!(held.is_empty());
}

#[test]
#[should_panic(expected = "one command at a slot")]
fn a_slot_holds_one_command() {
    let mut held = Held::new();
    held.hold(Slot(1), "a", write(1));
    held.hold(Slot(1), "b", write(2));
}
