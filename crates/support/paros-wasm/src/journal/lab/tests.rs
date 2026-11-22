use super::*;

#[test]
fn every_exercise_has_a_solution_that_reaches_its_goal() {
    for exercise in exercises() {
        let mut lab = Lab::start(exercise);
        assert!(
            !lab.solved(),
            "{}: the goal does not hold at the start",
            exercise.id
        );
        for &call in exercise.solution {
            lab.make(call);
        }
        assert!(
            lab.solved(),
            "{}: the solution reaches the goal",
            exercise.id
        );
        let view = lab.view();
        assert!(
            view.solved && view.done.is_some(),
            "{}: the view says so",
            exercise.id
        );
    }
}

#[test]
fn the_exercise_ids_differ_and_each_call_is_listed_once() {
    let ids: std::collections::BTreeSet<&str> = exercises().iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), exercises().len());
    for exercise in exercises() {
        let keys: std::collections::BTreeSet<String> =
            exercise.calls.iter().map(|c| c.key()).collect();
        assert_eq!(
            keys.len(),
            exercise.calls.len(),
            "{}: one button per call",
            exercise.id
        );
    }
}

/// Every sequence of up to five calls of every exercise: no call trips an
/// assertion of the core (an abort in wasm), and a refused call moves
/// nothing.
#[test]
fn no_sequence_of_calls_trips_the_core() {
    fn walk(lab: &Lab, depth: usize) {
        if depth == 0 {
            return;
        }
        for index in 0..lab.exercise.calls.len() {
            let mut next = Lab {
                exercise: lab.exercise,
                journal: lab.journal.clone(),
                moves: lab.moves.clone(),
                setup: lab.setup,
                refusal: None,
            };
            let before = next.journal.events().len();
            next.play(&Move::Call { index });
            let after = next.journal.events().len();
            if next.refusal.is_some() {
                assert_eq!(before, after, "a refused call moves nothing");
            } else {
                assert_eq!(before + 1, after, "a call is one event");
            }
            let _ = next.view();
            walk(&next, depth - 1);
        }
    }
    for exercise in exercises() {
        walk(&Lab::start(exercise), 5);
    }
}

#[test]
fn the_deposed_writer_resend_is_a_duplicate_not_fenced() {
    let mut lab = Lab::new("journal-fenced").expect("an exercise");
    for &call in lab.exercise.solution {
        lab.make(call);
    }
    lab.make(Call::Resend(A));
    // The resend is the new write a2 at 1, which is not written: Fenced
    // again. a1 sits at 0 and is the setup's.
    let last = &lab.view().history[0];
    assert_eq!(last.answer, "Fenced", "{last:?}");

    let mut lab = Lab::new("journal-fenced").expect("an exercise");
    lab.make(Call::Read(B));
    lab.make(Call::SetLeader(B));
    lab.make(Call::Resend(A));
    assert_eq!(
        lab.view().history[0].answer,
        "Duplicate",
        "position before fence"
    );
}

#[test]
fn an_unfenced_write_on_a_single_writer_journal_is_wrong_mode() {
    let mut lab = Lab::new("journal-multi").expect("an exercise");
    lab.make(Call::WriteOpen(B));
    assert_eq!(lab.view().history[0].answer, "WrongMode");
    assert!(!lab.solved());
}

#[test]
fn a_lost_answer_does_not_count() {
    let mut lab = Lab::new("journal-duplicate").expect("an exercise");
    lab.make(Call::WriteLost(A));
    lab.make(Call::WriteLost(A));
    let view = lab.view();
    assert!(view.history[0].lost);
    // A never heard the first answer, so its second write asks the same
    // position: a2 at 0 is Taken, and its answer is lost too.
    assert_eq!(view.history[0].answer, "Taken");
    assert!(!lab.solved());
}

#[test]
fn undo_replays_every_move_but_the_last() {
    let mut lab = Lab::new("journal-below").expect("an exercise");
    lab.make(Call::WriteLost(A));
    let after_one = lab.view().world.segments.len();
    lab.make(Call::Seal);
    assert_eq!(lab.view().world.segments.len(), after_one + 1);
    lab.play(&Move::Undo);
    assert_eq!(lab.view().moves, 1);
    assert_eq!(lab.view().world.segments.len(), after_one);
    lab.play(&Move::Reset);
    assert_eq!(lab.view().moves, 0);
    assert!(
        lab.view().history.is_empty(),
        "the setup is not in the history"
    );
}

#[test]
fn a_refused_move_changes_nothing_and_says_why() {
    let mut lab = Lab::new("journal-below").expect("an exercise");
    lab.play(&Move::Call { index: 99 });
    assert!(lab.view().refused.is_some());
    let resolve = lab
        .exercise
        .calls
        .iter()
        .position(|&c| c == Call::Resolve(A))
        .expect("listed");
    lab.play(&Move::Call { index: resolve });
    assert!(lab.view().refused.is_some(), "A has no write yet");
    assert_eq!(lab.view().moves, 0);
}
