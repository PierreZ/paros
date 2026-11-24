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

/// Play every guide step of `id` on a fresh lab, as "Show me how" does.
fn demo(id: &str) -> (Lab, Vec<Played>) {
    let mut lab = Lab::new(id).expect("the exercise exists");
    let mut played = Vec::new();
    let mut n = 0;
    while let Some(step) = lab.guide_step(n) {
        played.push(step);
        n += 1;
    }
    assert_eq!(n, lab.view().guide, "every guide step plays");
    (lab, played)
}

#[test]
fn a_guide_reaches_the_goal_and_never_counts_as_solved() {
    for exercise in exercises().iter().filter(|e| !e.guide.is_empty()) {
        let (lab, played) = demo(exercise.id);
        assert!(lab.goal(), "{}: the guide reaches the goal", exercise.id);
        let view = lab.view();
        assert!(view.demo);
        assert!(!view.solved, "{}: a demo is not solved", exercise.id);
        assert!(view.done.is_none());
        assert!(
            view.parts.iter().all(|part| part.holds),
            "{}: every line holds",
            exercise.id
        );
        for step in &played {
            assert_ne!(
                step.says.trim(),
                "",
                "{}: a guide step says what it does",
                exercise.id
            );
            assert!(
                !step.frames.is_empty(),
                "{}: a guide step moves",
                exercise.id
            );
            // Each frame names the button of its move, so the page can
            // outline it before it plays.
            for frame in &step.frames {
                assert!(
                    frame.message.is_some() || frame.extra.is_some(),
                    "{}: a guide frame names its move",
                    exercise.id
                );
            }
        }
    }
}

#[test]
fn a_demo_refuses_the_readers_moves_until_it_starts_again() {
    let (mut lab, _) = demo("gap");
    let moves = lab.view().moves;
    lab.play(&Move::Undo);
    assert!(lab.view().refused.is_some());
    assert_eq!(lab.view().moves, moves);
    lab.play(&Move::Reset);
    let view = lab.view();
    assert!(!view.demo);
    assert_eq!(view.moves, 0);
    assert!(view.refused.is_none());
}

#[test]
fn the_gap_guide_chooses_charlie_at_slot_2_before_node_0_crashes() {
    let mut lab = Lab::new("gap").expect("gap exists");
    for n in 0..3 {
        lab.guide_step(n).expect("the guide has five steps");
    }
    let world = lab.view_of_world();
    assert!(chosen_at(&world, 2, "charlie"));
    assert!(!chosen_at_any(&world, 1));
}

#[test]
fn the_intro_plays_each_move_from_the_view_before_it() {
    for exercise in exercises().iter().filter(|e| !e.intro.is_empty()) {
        let lab = Lab::new(exercise.id).expect("the exercise exists");
        let mut before = lab.intro_start().expect("an intro has a start").clone();
        let mut n = 0;
        while let Some(step) = lab.intro_step(n) {
            assert!(
                !step.frames.is_empty(),
                "{}: an intro step moves",
                exercise.id
            );
            for frame in &step.frames {
                // The setup is not the reader's, and each frame says what the
                // core said about its own move.
                assert_eq!(
                    frame.view.moves, 0,
                    "{}: no intro frame counts a move",
                    exercise.id
                );
                assert!(
                    !frame.view.said.is_empty(),
                    "{}: each intro frame says something",
                    exercise.id
                );
                assert!(
                    frame.message.is_some() || frame.view.said != before.said,
                    "{}: an action's frame says what the action did",
                    exercise.id
                );
                if let Some(id) = frame.message {
                    assert!(
                        before.wire.iter().any(|m| m.id == id),
                        "{}: a frame moves a message of the wire before it",
                        exercise.id
                    );
                }
                before = frame.view.clone();
            }
            n += 1;
        }
        assert_eq!(n, exercise.intro.len());
        // The last frame is the state the reader starts from, with no move
        // of the reader's.
        let start = lab.view();
        assert_eq!(start.moves, 0);
        assert_eq!(before.wire.len(), start.wire.len());
        assert_eq!(before.logs, start.logs);
    }
}

#[test]
fn the_third_hint_alone_solves_a_guided_exercise() {
    for exercise in exercises().iter().filter(|e| !e.parts.is_empty()) {
        let mut lab = Lab::new(exercise.id).expect("the exercise exists");
        for _ in 0..MOVES {
            let view = lab.view();
            if view.solved {
                break;
            }
            assert!(
                view.stuck.is_none(),
                "{}: the hints never lead to a dead end",
                exercise.id
            );
            let hint = view.hint.expect("a goal that does not hold has a hint");
            let next = hint.next.expect("each hint names a move");
            lab.play(&next);
            assert!(
                lab.view().refused.is_none(),
                "{}: the hinted move {next:?} is refused",
                exercise.id
            );
        }
        let view = lab.view();
        assert!(view.solved, "{}: the hints reach the goal", exercise.id);
        assert!(view.hint.is_none());
        assert!(view.parts.iter().all(|part| part.holds));
    }
}

#[test]
fn a_vote_at_slot_1_is_a_dead_end_that_undo_leaves() {
    let mut lab = Lab::new("gap").expect("gap exists");
    lab.extra("The client writes bravo");
    lab.deliver(|m| m.kind == "Accept" && m.slot == Some(1) && m.to != "n0");
    let view = lab.view();
    let stuck = view
        .stuck
        .expect("slot 1 has a vote: the first line cannot hold");
    assert_eq!(stuck.part, 0);
    assert!(view.hint.is_none());
    lab.play(&Move::Undo);
    let view = lab.view();
    assert!(view.stuck.is_none());
    assert_eq!(view.hint.map(|hint| hint.part), Some(0));
}

#[test]
fn an_exercise_with_parts_has_the_goal_of_its_parts() {
    for exercise in exercises().iter().filter(|e| !e.parts.is_empty()) {
        let mut lab = Lab::start(exercise);
        assert!(!lab.goal());
        (exercise.solution)(&mut lab);
        assert_eq!(
            lab.goal(),
            exercise.parts.iter().all(|part| (part.holds)(&lab))
        );
        assert!(lab.solved());
    }
}

/// A seeded draw, the same each run.
struct Draw(u64);

impl Draw {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1))
    }

    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        usize::try_from(self.0 >> 33).expect("31 bits fit") % n
    }

    /// A move that tries to reach the goal: the hinted move, an extra move
    /// or Deliver all as often as one message, so a few moves go far. A dead
    /// end has no hint, so from one this is a random walk; a dead end that is
    /// named wrongly is left by such a walk, and the hints then reach the
    /// goal. `None` when no move is open.
    fn goal_move(&mut self, lab: &Lab) -> Option<Move> {
        let view = lab.view();
        let mut open: Vec<Move> = (0..view.extras.len())
            .map(|index| Move::Extra { index })
            .collect();
        if let Some(next) = view.hint.and_then(|hint| hint.next) {
            open.extend([next.clone(), next.clone(), next]);
        }
        if !view.wire.is_empty() {
            open.push(Move::DeliverAll);
            open.push(Move::DeliverAll);
            let m = &view.wire[self.below(view.wire.len())];
            open.push(Move::Deliver { id: m.id });
            open.push(Move::Drop { id: m.id });
        }
        self.pick(open)
    }

    /// The exercise `id` after up to 19 moves of the reader's.
    fn start(&mut self, id: &str) -> Lab {
        let mut lab = Lab::new(id).expect("the exercise exists");
        for _ in 0..self.below(20) {
            let Some(pick) = self.reader_move(&lab) else {
                break;
            };
            lab.play(&pick);
        }
        lab
    }

    /// One of `open` at random; `None` when it is empty.
    fn pick(&mut self, mut open: Vec<Move>) -> Option<Move> {
        if open.is_empty() {
            return None;
        }
        let k = self.below(open.len());
        Some(open.swap_remove(k))
    }

    /// One move of the reader's, at random: each message delivered or
    /// dropped (a delivery twice as likely), or an extra move. `None` when
    /// no move is open (an exercise with no extra move and an empty wire).
    fn reader_move(&mut self, lab: &Lab) -> Option<Move> {
        let view = lab.view();
        let mut open: Vec<Move> = Vec::new();
        for m in &view.wire {
            open.push(Move::Deliver { id: m.id });
            open.push(Move::Deliver { id: m.id });
            open.push(Move::Drop { id: m.id });
        }
        open.extend((0..view.extras.len()).map(|index| Move::Extra { index }));
        self.pick(open)
    }
}

fn replay(id: &str, moves: &[Move]) -> Lab {
    let mut lab = Lab::new(id).expect("the exercise exists");
    for again in moves {
        lab.play(again);
    }
    lab
}

/// From any state the reader can reach, a guided exercise either names a
/// dead end that is one (random moves from it never reach the goal), or its
/// third hint names a move that is not refused and the hints alone reach
/// the goal with no dead end on the way. The states come from seeded random
/// moves (a fixed list of seeds: the run is the same each time).
#[test]
fn from_any_state_the_hints_reach_the_goal_or_a_dead_end_is_named() {
    for exercise in exercises().iter().filter(|e| !e.parts.is_empty()) {
        let mut stuck = 0;
        // The dead ends judged so far, per part, and the tries from a live
        // state that reached the goal.
        let mut judged = vec![0_usize; exercise.parts.len()];
        let mut reached = 0;
        for seed in 0..1000_u64 {
            let mut draw = Draw::new(seed);
            let mut lab = draw.start(exercise.id);
            let start = lab.moves().to_vec();
            if let Some(dead) = lab.view().stuck {
                stuck += 1;
                // A named dead end is one: moves that try to reach the goal
                // never do from it (the first 10 dead ends of each part, 30
                // tries of 16 moves).
                if judged[dead.part] < 10 {
                    judged[dead.part] += 1;
                    for _ in 0..30 {
                        let mut tried = replay(exercise.id, &start);
                        for _ in 0..16 {
                            let Some(pick) = draw.goal_move(&tried) else {
                                break;
                            };
                            tried.play(&pick);
                            assert!(
                                !tried.view().solved,
                                "{} after {start:?}: the dead end {:?} is not one: {:?} reach the goal",
                                exercise.id,
                                dead.says,
                                tried.moves()
                            );
                        }
                    }
                }
                continue;
            }
            // The same tries from a live state sometimes reach the goal, so a
            // dead end that they never leave says something.
            if seed < 100 {
                for _ in 0..30 {
                    let mut tried = replay(exercise.id, &start);
                    for _ in 0..16 {
                        let Some(pick) = draw.goal_move(&tried) else {
                            break;
                        };
                        tried.play(&pick);
                    }
                    if tried.view().solved {
                        reached += 1;
                    }
                }
            }
            for _ in 0..MOVES {
                let view = lab.view();
                if view.solved {
                    break;
                }
                assert!(
                    view.stuck.is_none(),
                    "{} after {start:?}: the hints lead to a dead end: {:?}",
                    exercise.id,
                    view.stuck
                );
                let hint = view.hint.expect("a goal that does not hold has a hint");
                let next = hint.next.unwrap_or_else(|| {
                    panic!(
                        "{} after {start:?}: the hint of part {} names no move, at {:?}",
                        exercise.id,
                        hint.part,
                        lab.moves()
                    )
                });
                lab.play(&next);
                assert!(
                    lab.view().refused.is_none(),
                    "{} after {start:?}: the hinted move {next:?} is refused: {:?}",
                    exercise.id,
                    lab.view().refused
                );
            }
            assert!(
                lab.view().solved,
                "{} after {start:?}: the hints reach the goal",
                exercise.id
            );
        }
        // The walk meets the dead ends of more than one part, and the tries
        // reach the goal from live states.
        assert!(
            stuck > 0,
            "{}: some random moves reach a dead end",
            exercise.id
        );
        assert!(
            reached > 0,
            "{}: the tries reach the goal from a live state",
            exercise.id
        );
        assert!(
            judged.iter().filter(|n| **n > 0).count() >= 2,
            "{}: dead ends of two parts are judged",
            exercise.id
        );
    }
}

/// Slot 2 lost its Accepts, but a vote at slot 3 sets a new leader's
/// frontier above it: slot 2 gets a `Noop`, so this is no dead end, and the
/// hints reach the goal.
#[test]
fn a_vote_above_slot_2_keeps_slot_2_alive() {
    let mut lab = Lab::new("gap").expect("gap exists");
    lab.extra("The client writes bravo");
    lab.drop_all(|m| m.kind == "Accept" && m.slot == Some(1));
    lab.extra("The client writes charlie");
    lab.drop_all(|m| m.kind == "Accept" && m.slot == Some(2));
    lab.extra("The client writes charlie");
    let to_1 = lab
        .wire()
        .into_iter()
        .find(|m| m.kind == "Accept" && m.slot == Some(3) && m.to == "n1")
        .expect("an Accept of slot 3 to node 1");
    lab.must(&Move::Deliver { id: to_1.id });
    assert!(
        lab.view().stuck.is_none(),
        "a vote at slot 3 keeps slot 2 alive"
    );
    for _ in 0..MOVES {
        let view = lab.view();
        if view.solved {
            break;
        }
        assert!(view.stuck.is_none());
        let next = view
            .hint
            .and_then(|hint| hint.next)
            .expect("a hint names a move");
        lab.must(&next);
    }
    assert!(lab.view().solved, "the hints reach the goal");
}

/// In the grid, a write in the wrong order is a dead end that undo leaves,
/// and a lost vote of slot 1 is not one: the leader sends its open Accepts
/// again.
#[test]
fn a_grid_write_out_of_order_is_a_dead_end_and_a_lost_vote_is_not() {
    let mut lab = Lab::new("grid-column").expect("grid-column exists");
    lab.extra("The client writes bravo");
    let stuck = lab.view().stuck.expect("bravo took slot 0");
    assert_eq!(stuck.part, 0);
    lab.play(&Move::Undo);
    assert!(lab.view().stuck.is_none());

    lab.extra("The client writes alpha");
    lab.extra("The client writes bravo");
    lab.drop_all(|m| m.kind == "Accept" && m.slot == Some(1) && m.to == "n1");
    lab.settle(|m| m.slot == Some(1) && m.to != "n3");
    let view = lab.view();
    assert!(view.stuck.is_none(), "a lost Accept is no dead end");
    assert!(!view.parts[1].holds, "column 1 misses the vote of node 1");
    let hint = view.hint.expect("a hint");
    assert_eq!(hint.next, Some(Move::Extra { index: 2 }), "send again");
    lab.play(&Move::Extra { index: 2 });
    lab.settle(|m| m.slot == Some(1) && m.to != "n3");
    assert!(lab.view().solved);
}

/// In two of three, a message to acceptor 3 is a dead end of the last line,
/// and a lost promise of acceptor 1 is one of the first.
#[test]
fn two_of_three_names_each_dead_end() {
    let mut lab = Lab::new("majority").expect("majority exists");
    lab.deliver(|m| m.to == "n3");
    assert_eq!(lab.view().stuck.map(|s| s.part), Some(2));
    lab.play(&Move::Undo);
    lab.drop_all(|m| m.kind == "Prepare" && m.to == "n1");
    assert_eq!(lab.view().stuck.map(|s| s.part), Some(0));
    lab.play(&Move::Undo);
    assert!(lab.view().stuck.is_none());
}
