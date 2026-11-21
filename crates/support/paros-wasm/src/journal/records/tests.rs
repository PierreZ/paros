use super::*;
use crate::journal::keyed::{Lab, check_exercises};

#[test]
fn every_exercise_is_solved_and_no_move_trips_the_register() {
    check_exercises(exercises(), 4);
}

fn refused(lab: &mut Lab<Records>, key: &str) -> String {
    lab.play(&crate::journal::keyed::Move::Act {
        key: key.to_string(),
    });
    lab.refusal().expect("refused").to_string()
}

#[test]
fn each_guard_names_its_refusal() {
    let mut lab = Lab::new(exercises(), "records-retire").expect("an exercise");
    assert!(refused(&mut lab, "seal:1").contains("NoSuccessor"));
    assert!(refused(&mut lab, "boundary:1").contains("State: the record is in force"));
    lab.make("reshape");
    assert!(refused(&mut lab, "reshape").contains("Unretired"));
    lab.make("seal:1");
    assert!(refused(&mut lab, "retire:1:2").contains("NoBoundary"));
    lab.make("boundary:1");
    assert!(refused(&mut lab, "boundary:1").contains("BoundaryWritten"));
    assert!(refused(&mut lab, "retire:1:1").contains("FewHolders"));
    assert!(refused(&mut lab, "trim:1").contains("FloorBelowEnd"));
    assert!(refused(&mut lab, "trim:2").contains("State: the record is in force"));
    assert_eq!(lab.view().moves, 3, "a refused move is not a move");
}

#[test]
fn the_seal_puts_the_successor_in_force_at_its_end() {
    let mut lab = Lab::new(exercises(), "records-retire").expect("an exercise");
    lab.make("reshape");
    lab.make("seal:1");
    let view = lab.view().world;
    assert_eq!(view.records[0].slots, "slots 0 – 3");
    assert_eq!(view.records[1].slots, "from slot 4");
    assert_eq!(view.records[1].state, "in force");
}
