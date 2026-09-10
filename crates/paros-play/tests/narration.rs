//! The narration: derived from the transition, not from a script.
//!
//! These check the three things that make a narration line trustworthy — it
//! appears exactly when the thing it describes happened, it carries that
//! transition's own numbers, and it is reproduced bit-for-bit by a replay,
//! because it is a function of the world and nothing else.

use paros_play::action::Action;
use paros_play::level::{Level, levels};
use paros_play::narration::{NarrationEvent, NarrationKind};
use paros_play::prompt::PromptKind;
use paros_play::{Game, GoalStatus};

/// Play a level's reference solution.
fn play_reference(level: &Level) -> Game {
    let mut game = Game::new(level.id).expect("a registered level");
    for action in (level.reference)() {
        let label = action.label();
        game.act(action)
            .unwrap_or_else(|err| panic!("{}: reference step {label} refused: {err}", level.id));
    }
    game
}

/// Every line the whole play produced, in order.
fn all_lines(game: &Game) -> Vec<NarrationEvent> {
    game.narration_log().iter().flatten().cloned().collect()
}

fn of_kind(game: &Game, kind: NarrationKind) -> Vec<String> {
    all_lines(game)
        .into_iter()
        .filter(|event| event.kind == kind)
        .map(|event| event.text)
        .collect()
}

#[test]
fn choosing_a_value_is_narrated_once_with_its_numbers() {
    let level = paros_play::level::level("act1/choose-a-value").expect("the first level");
    let game = play_reference(level);
    let chosen = of_kind(&game, NarrationKind::Chosen);
    assert_eq!(
        chosen.len(),
        1,
        "one decision, one line about it — got {chosen:#?}"
    );
    let line = &chosen[0];
    assert!(
        line.contains("1.5"),
        "the decision names the ballot it was taken at: {line}"
    );
    assert!(
        line.contains("acceptors 1 and 2"),
        "and which acceptors voted for it: {line}"
    );
    assert!(line.contains("\"alpha\""), "and the value itself: {line}");
    assert!(
        matches!(game.goal(), GoalStatus::Reached(_)),
        "the reference reaches the goal"
    );
    assert_eq!(
        of_kind(&game, NarrationKind::Goal).len(),
        1,
        "the goal is announced once, on the action that reached it"
    );
}

#[test]
fn every_line_carries_a_sentence() {
    for level in levels() {
        let game = play_reference(level);
        for event in all_lines(&game) {
            assert!(
                event.text.len() > 20,
                "{}: a narration line is a sentence, not a shrug: {:?}",
                level.id,
                event.text
            );
        }
    }
}

#[test]
fn a_wrong_answer_narrates_the_violation() {
    let level = paros_play::level::level("act1/be-the-acceptor").expect("the acceptor level");
    let mut game = Game::new(level.id).expect("a registered level");
    for action in (level.reference)() {
        // Stop at the first Prepare the player must answer and get it wrong.
        if let Action::Answer { prompt, .. } = &action {
            let view = game.view();
            let open = view
                .prompt
                .as_ref()
                .expect("the reference answers an open prompt");
            if open.kind == PromptKind::AcceptorPrepare && open.feedback.is_none() {
                let expected = expected_choice(&game);
                let wrong = open
                    .choices
                    .iter()
                    .map(|choice| choice.id.clone())
                    .find(|id| *id != expected)
                    .expect("a prompt offers more than one choice");
                game.act(Action::Answer {
                    prompt: *prompt,
                    choice: wrong,
                })
                .expect("a wrong answer is a legal move");
                let lines = game.narration();
                assert_eq!(
                    lines.len(),
                    1,
                    "a refused answer says one thing: {lines:#?}"
                );
                assert_eq!(
                    lines[0].kind,
                    NarrationKind::Violation,
                    "and it is the rule it would have broken"
                );
                assert!(
                    lines[0].text.len() > 80,
                    "the violation is explained, not merely flagged"
                );
                assert_eq!(game.mistakes(), 1);
                return;
            }
        }
        game.act(action).expect("the reference replays");
    }
    panic!("act1/be-the-acceptor raises a Prepare prompt");
}

/// The answer `paros-core` itself gives to the open prompt.
fn expected_choice(game: &Game) -> String {
    game.world()
        .prompt()
        .expect("a prompt is open")
        .expected()
        .to_string()
}

#[test]
fn a_gated_prompt_is_not_answered_by_the_caption_above_it() {
    // The persist-order and recovery prompts are asked about a batch
    // `paros-core` has already produced, and the world is holding it. Narrating
    // that batch while the question is open would print the answer above the
    // question, so the story waits for the answer.
    for id in ["act2/persist-before-send", "act2/the-permanent-gap"] {
        let level = paros_play::level::level(id).expect("a registered level");
        let mut game = Game::new(id).expect("a registered level");
        let mut asked = 0usize;
        for action in (level.reference)() {
            game.act(action).expect("the reference replays");
            if game.view().prompt.is_none() {
                continue;
            }
            asked += 1;
            for event in game.narration() {
                assert!(
                    !matches!(
                        event.kind,
                        NarrationKind::Accept | NarrationKind::Chosen | NarrationKind::Promise
                    ),
                    "{id}: the caption gave away the open question: {:?}",
                    event.text
                );
            }
        }
        assert!(asked > 0, "{id}: the reference runs into its question");
    }
}

#[test]
fn replay_reproduces_the_narration_exactly() {
    for level in levels() {
        let a = play_reference(level);
        let b = play_reference(level);
        assert_eq!(
            all_lines(&a),
            all_lines(&b),
            "{}: two replays of one action log narrate differently",
            level.id
        );
    }
}

#[test]
fn undo_reproduces_the_narration_exactly() {
    for level in levels() {
        let mut game = Game::new(level.id).expect("a registered level");
        for action in (level.reference)() {
            let before = all_lines(&game);
            game.act(action.clone()).expect("the reference replays");
            let after = all_lines(&game);
            game.undo();
            assert_eq!(
                all_lines(&game),
                before,
                "{}: undo after {} left a different narration",
                level.id,
                action.label()
            );
            game.act(action).expect("the reference replays");
            assert_eq!(
                all_lines(&game),
                after,
                "{}: replaying the undone action narrated differently",
                level.id
            );
        }
    }
}
