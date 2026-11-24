//! The exercises reach their goals, and the engine keeps its promises.

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
        (exercise.solution)(&mut lab);
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
fn the_view_draws_each_log_its_hole_and_the_leader() {
    let decree = Lab::new("vote-not-chosen").expect("an exercise").view();
    assert!(decree.logs.is_empty(), "one decree has no log strip");
    let mut lab = Lab::new("gap").expect("an exercise");
    let view = lab.view();
    assert_eq!(view.logs.len(), view.lanes.len(), "one log per lane");
    assert_eq!(view.nodes.len(), view.lanes.len(), "one state per lane");
    assert!(
        view.nodes.iter().all(|node| node.holes.is_empty()),
        "no hole at the start"
    );
    let n0 = view
        .lanes
        .iter()
        .position(|lane| lane.id == "n0")
        .expect("lane n0");
    assert_eq!(
        view.nodes[n0].role.as_deref(),
        Some("leader"),
        "node 0 leads"
    );
    lab.extra("The client writes bravo");
    lab.drop_all(|m| m.kind == "Accept");
    lab.extra("The client writes charlie");
    lab.settle(|m| m.to != "n0" || m.kind == "Accepted");
    let view = lab.view();
    let n1 = view
        .lanes
        .iter()
        .position(|lane| lane.id == "n1")
        .expect("lane n1");
    assert_eq!(view.nodes[n1].holes, [1], "node 1 has no vote at slot 1");
    assert!(
        view.logs[n1].iter().any(|cell| cell.slot == 2),
        "node 1 voted at slot 2: {:?}",
        view.logs[n1]
    );
    let desk = Lab::new("lost-votes").expect("an exercise").view();
    assert_eq!(
        desk.logs.len(),
        desk.lanes.len(),
        "the compartments draw a log per lane"
    );
}

#[test]
fn the_exercise_ids_differ() {
    let ids: std::collections::BTreeSet<&str> = exercises().iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), exercises().len());
}

#[test]
fn two_values_are_chosen_only_with_the_rule_off() {
    let lab = {
        let mut lab = Lab::new("two-values").expect("an exercise");
        (lab.exercise.solution)(&mut lab);
        lab
    };
    assert!(lab.view().two_values, "the view says two values are chosen");
    assert_eq!(
        lab.view().chosen.len(),
        2,
        "two decisions: {:?}",
        lab.view().chosen
    );
    assert!(lab.moves.iter().any(|m| matches!(m, Move::Extra { index } if (lab.exercise.extras)()[*index].1 == Extra::Act(Action::Wipe { node: 2 }))),
        "the solution needs the wipe");
}

#[test]
fn the_old_set_needs_node_2() {
    let mut lab = Lab::new("old-set").expect("an exercise");
    lab.extra("Node 3 starts an election");
    lab.settle(|m| m.to != "n2");
    lab.drop_all(|_| true);
    assert!(
        !lab.solved(),
        "with no message to node 2, node 3 cannot lead: {:?}",
        lab.view().said
    );
}

#[test]
fn a_slot_of_a_dead_proxy_needs_the_leader_beats() {
    let mut lab = Lab::new("proxy-down").expect("an exercise");
    lab.extra("The client writes bravo");
    lab.settle(|_| true);
    assert!(!lab.solved(), "with no beat, slot 1 stays open");
    lab.extra("Node 0 beats");
    lab.settle(|_| true);
    assert!(
        !lab.solved(),
        "one beat sends the slot to the dead proxy again"
    );
    lab.extra("Node 0 beats");
    lab.settle(|_| true);
    assert!(
        lab.solved(),
        "the second beat takes the slot back: {:?}",
        lab.view().said
    );
}

#[test]
fn votes_delivered_in_time_do_not_reach_the_goal() {
    let mut lab = Lab::new("lost-votes").expect("an exercise");
    lab.extra("The client writes alpha");
    lab.settle(|_| true);
    assert!(
        lab.desk()
            .chosen()
            .iter()
            .any(|line| line == "slot 0 = alpha")
    );
    assert!(!lab.solved(), "no vote was dropped");
}

#[test]
fn a_replica_that_learns_in_order_never_waits() {
    let mut lab = Lab::new("replica-waits").expect("an exercise");
    lab.extra("The client writes alpha");
    lab.extra("The client writes bravo");
    lab.play(&Move::DeliverAll);
    assert_eq!(
        lab.desk().learned(0),
        Some(1),
        "replica 0 learns both slots"
    );
    assert!(
        !lab.desk().waited(0) && !lab.solved(),
        "in order, it never waits"
    );
}

#[test]
fn a_replica_holds_a_slot_above_its_prefix_and_says_so() {
    let mut lab = Lab::new("replica-waits").expect("an exercise");
    lab.extra("The client writes alpha");
    lab.extra("The client writes bravo");
    lab.settle(|m| !(m.kind == "Commit" && m.to == "r0" && m.slot == Some(0)));
    assert_eq!(lab.desk().learned(0), None, "slot 0 is not learned yet");
    assert!(lab.desk().waited(0));
    assert!(!lab.view().two_values, "two slots are not two values");
    let view = lab.view();
    let r0 = view
        .lanes
        .iter()
        .position(|lane| lane.id == "r0")
        .expect("lane r0");
    assert!(
        view.state[r0]
            .iter()
            .any(|line| line == "holds slot 1, waits"),
        "{:?}",
        view.state[r0]
    );
}

#[test]
fn a_compartments_move_to_a_dead_proxy_is_refused() {
    let mut lab = Lab::new("lost-votes").expect("an exercise");
    lab.play(&Move::Extra { index: 1 });
    assert!(lab.view().refused.is_none(), "an idle proxy may beat");
    let mut lab = Lab::new("proxy-down").expect("an exercise");
    let view = lab.view();
    assert_eq!(view.alive.len(), view.lanes.len());
    let p1 = view
        .lanes
        .iter()
        .position(|lane| lane.id == "p1")
        .expect("lane p1");
    assert!(!view.alive[p1], "proxy leader 1 is down");
    assert!(
        view.lanes.iter().all(|lane| lane.id != "b0"),
        "no batcher lane"
    );
    lab.play(&Move::Deliver { id: 9_999 });
    assert!(lab.view().refused.is_some());
}

#[test]
fn deliver_all_empties_the_wire_in_one_move() {
    let mut lab = Lab::new("majority").expect("an exercise");
    lab.play(&Move::DeliverAll);
    let view = lab.view();
    assert!(view.wire.is_empty(), "the wire is empty");
    assert_eq!(view.moves, 1, "one move");
    assert!(!view.solved, "acceptor 3 heard the ballot");
    assert!(!view.two_values, "one value is chosen");
    lab.play(&Move::Undo);
    assert_eq!(lab.view().moves, 0);
    assert!(!lab.view().wire.is_empty(), "undo puts the wire back");
}

#[test]
fn undo_replays_every_move_but_the_last() {
    let mut lab = Lab::new("vote-not-chosen").expect("an exercise");
    let first = lab.wire()[0].id;
    lab.play(&Move::Deliver { id: first });
    let after_one = lab.view();
    let second = lab.wire()[0].id;
    lab.play(&Move::Drop { id: second });
    assert_eq!(lab.view().moves, 2);
    lab.play(&Move::Undo);
    let undone = lab.view();
    assert_eq!(undone.moves, 1);
    assert_eq!(
        undone.wire.iter().map(|m| m.id).collect::<Vec<_>>(),
        after_one.wire.iter().map(|m| m.id).collect::<Vec<_>>(),
        "the replay gives the same wire"
    );
    lab.play(&Move::Reset);
    assert_eq!(lab.view().moves, 0);
}

#[test]
fn a_refused_move_changes_nothing_and_says_why() {
    let mut lab = Lab::new("vote-not-chosen").expect("an exercise");
    let before = lab.view();
    lab.play(&Move::Deliver { id: 9_999 });
    let after = lab.view();
    assert!(after.refused.is_some(), "the move is refused");
    assert_eq!(after.moves, before.moves);
    lab.play(&Move::Extra { index: 0 });
    assert!(
        lab.view().refused.is_some(),
        "this exercise has no extra move"
    );
}
