use super::*;
use crate::journal::keyed::{Lab, check_exercises};

#[test]
fn every_exercise_is_solved_and_no_move_trips_the_register() {
    check_exercises(exercises(), 4);
}

fn lab(id: &str) -> Lab<Register> {
    Lab::new(exercises(), id).expect("an exercise")
}

#[test]
fn a_member_that_is_down_takes_no_request() {
    let mut lab = lab("register-down");
    lab.make("P:elect");
    let keys: Vec<String> = lab.view().actions.into_iter().map(|a| a.key).collect();
    assert!(keys.contains(&"P>m3:drop".to_string()));
    assert!(!keys.contains(&"P>m3:deliver".to_string()), "m3 is down");
    lab.make("m3:restart");
    lab.make("P>m3:deliver");
}

#[test]
fn a_read_at_one_member_is_not_a_majority() {
    let mut lab = lab("register-race");
    lab.make("P:elect");
    lab.make("P>m1:deliver");
    assert_eq!(
        lab.view().world.callers[0].phase,
        "reading: 1 of 3 answered"
    );
    lab.make("P>m2:deliver");
    assert_eq!(lab.view().world.callers[0].phase, "writing: 0 of 3 took it");
}

#[test]
fn a_read_that_meets_the_minority_write_raises_the_term() {
    // P's write lands at m1 only; Q reads m1 and m2, sees it and writes
    // term 2: a minority write can come back.
    let mut lab = lab("register-minority");
    for key in ["P:elect", "P>m1:deliver", "P>m2:deliver", "P>m1:lose"] {
        lab.make(key);
    }
    for key in [
        "Q:elect",
        "Q>m1:deliver",
        "Q>m2:deliver",
        "Q>m1:deliver",
        "Q>m2:deliver",
    ] {
        lab.make(key);
    }
    assert!(lab.world().done("Q", |v| v.term().number == 2));
    assert!(!lab.solved(), "Q met P's write");
}

#[test]
fn the_loser_of_a_race_reads_the_term_taken() {
    let mut lab = lab("register-race");
    for key in exercises()[1].solution {
        lab.make(key);
    }
    for key in [
        "P:elect",
        "P>m1:deliver",
        "P>m2:deliver",
        "P>m1:deliver",
        "P>m2:deliver",
    ] {
        lab.make(key);
    }
    assert!(
        lab.world().done("P", |v| v.term().number == 2),
        "P's retry opens above"
    );
}

#[test]
fn a_move_that_is_not_open_is_refused_and_changes_nothing() {
    let mut lab = lab("register-down");
    lab.play(&crate::journal::keyed::Move::Act {
        key: "P>m1:deliver".to_string(),
    });
    assert!(lab.refusal().is_some());
    assert_eq!(lab.view().moves, 0);
}
