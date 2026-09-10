//! Every registered level, checked the way a player would find out: the
//! reference solution reaches the goal, every wrong prompt answer is refused
//! and moves nothing, and undo puts the world back exactly.

use std::collections::BTreeSet;

use paros_play::action::{Action, ActionKind};
use paros_play::auto::AutomationFlag;
use paros_play::level::{Level, levels};
use paros_play::prompt::{ALL_PROMPTS, PromptKind};
use paros_play::{Game, GoalStatus};

/// The questions a level pins manual — the ones it exists to teach. A level may
/// pin a flag that governs no prompt (heartbeat delivery, say); that promises
/// no question.
fn taught_prompts(level: &Level) -> Vec<PromptKind> {
    ALL_PROMPTS
        .iter()
        .copied()
        .filter(|kind| level.pinned_off.contains(&kind.flag()))
        .collect()
}

/// Which flags a level pins off, for a message.
fn pinned(level: &Level) -> Vec<AutomationFlag> {
    level.pinned_off.to_vec()
}

/// The world half of the view, as JSON — what "the world did not move" means.
fn world_json(game: &Game) -> String {
    serde_json::to_string(&game.view().world).expect("the view encodes")
}

fn whole_json(game: &Game) -> String {
    serde_json::to_string(&game.view()).expect("the view encodes")
}

fn wire(game: &Game) -> String {
    game.view()
        .world
        .wire
        .iter()
        .map(|m| format!("#{} {} {}->{}", m.id, m.kind, m.from, m.to))
        .collect::<Vec<_>>()
        .join(", ")
}

fn play_reference(level: &Level) -> Game {
    let mut game = Game::new(level.id).expect("a registered level");
    for (step, action) in (level.reference)().into_iter().enumerate() {
        let label = action.label();
        game.act(action).unwrap_or_else(|err| {
            panic!(
                "{}: reference step {step} ({label}) was refused: {err}\n  in flight: {}",
                level.id,
                wire(&game)
            )
        });
    }
    game
}

#[test]
fn level_ids_are_unique() {
    let mut seen = BTreeSet::new();
    for level in levels() {
        assert!(seen.insert(level.id), "duplicate level id {}", level.id);
    }
    assert!(!seen.is_empty(), "at least one level is registered");
}

#[test]
fn every_reference_reaches_its_goal() {
    for level in levels() {
        let game = play_reference(level);
        match game.goal() {
            GoalStatus::Reached(_) => {}
            other => panic!(
                "{}: the reference solution left the goal at {other:?}\n  in flight: {}",
                level.id,
                wire(&game)
            ),
        }
        assert_eq!(
            game.mistakes(),
            0,
            "{}: the reference solution answers every prompt correctly",
            level.id
        );
    }
}

#[test]
fn allowed_actions_cover_every_reference_step() {
    for level in levels() {
        for action in (level.reference)() {
            assert!(
                level.allows(action.kind()),
                "{}: the reference uses {:?}, which the level does not offer",
                level.id,
                action.kind()
            );
        }
    }
}

#[test]
fn every_wrong_answer_is_refused_and_moves_nothing() {
    for level in levels() {
        let reference = (level.reference)();
        let mut game = Game::new(level.id).expect("a registered level");
        let mut prompts_seen = 0usize;
        for action in reference {
            if let Action::Answer { prompt, choice } = &action {
                let view = game.view();
                let open = view.prompt.as_ref().unwrap_or_else(|| {
                    panic!(
                        "{}: the reference answers a prompt that is not open",
                        level.id
                    )
                });
                assert_eq!(
                    open.id, *prompt,
                    "{}: the reference answers the open prompt",
                    level.id
                );
                prompts_seen += 1;
                let wrong: Vec<String> = open
                    .choices
                    .iter()
                    .map(|c| c.id.clone())
                    .filter(|id| id != choice)
                    .collect();
                for id in wrong {
                    let before = world_json(&game);
                    let mistakes = game.mistakes();
                    game.act(Action::Answer {
                        prompt: *prompt,
                        choice: id.clone(),
                    })
                    .expect("a wrong answer is a legal move, not an error");
                    assert_eq!(
                        game.mistakes(),
                        mistakes + 1,
                        "{}: a wrong answer costs a mistake",
                        level.id
                    );
                    assert_eq!(
                        world_json(&game),
                        before,
                        "{}: answering {id:?} moved the world",
                        level.id
                    );
                    let after = game.view();
                    let still = after.prompt.as_ref().unwrap_or_else(|| {
                        panic!("{}: a wrong answer closed the prompt", level.id)
                    });
                    assert_eq!(
                        still.id, *prompt,
                        "{}: the same prompt stays open",
                        level.id
                    );
                    let feedback = still
                        .feedback
                        .as_ref()
                        .unwrap_or_else(|| panic!("{}: a wrong answer explains itself", level.id));
                    assert!(
                        feedback.len() > 40,
                        "{}: the explanation for {id:?} is a sentence, not a shrug",
                        level.id
                    );
                }
            }
            game.act(action).expect("the reference replays");
        }
        if taught_prompts(level).is_empty() {
            continue;
        }
        assert!(
            prompts_seen > 0,
            "{}: it pins {:?} manual, so its reference must answer at least one prompt",
            level.id,
            pinned(level)
        );
    }
}

#[test]
fn every_reference_asks_the_question_its_level_teaches() {
    for level in levels() {
        let taught = taught_prompts(level);
        if taught.is_empty() {
            continue;
        }
        let mut game = Game::new(level.id).expect("a registered level");
        let mut seen: BTreeSet<PromptKind> = BTreeSet::new();
        for action in (level.reference)() {
            if let Some(prompt) = game.view().prompt {
                seen.insert(prompt.kind);
            }
            game.act(action).expect("the reference replays");
        }
        if let Some(prompt) = game.view().prompt {
            seen.insert(prompt.kind);
        }
        for kind in taught {
            assert!(
                seen.contains(&kind),
                "{}: the level makes {kind:?} manual, but its reference never runs into that \
                 question — it teaches nothing. Seen: {seen:?}",
                level.id
            );
        }
    }
}

#[test]
fn undo_reproduces_the_previous_view() {
    for level in levels() {
        let mut game = Game::new(level.id).expect("a registered level");
        for action in (level.reference)() {
            let before = whole_json(&game);
            game.act(action.clone()).expect("the reference replays");
            game.undo();
            assert_eq!(
                whole_json(&game),
                before,
                "{}: undo after {} did not restore the previous view",
                level.id,
                action.label()
            );
            game.act(action).expect("the reference replays");
        }
    }
}

#[test]
fn replay_is_bit_exact() {
    for level in levels() {
        let a = play_reference(level);
        let b = play_reference(level);
        assert_eq!(
            whole_json(&a),
            whole_json(&b),
            "{}: two replays of one action log differ",
            level.id
        );
    }
}

#[test]
fn an_action_a_level_does_not_offer_is_refused() {
    for level in levels() {
        if level.allows(ActionKind::TickAll) {
            continue;
        }
        let mut game = Game::new(level.id).expect("a registered level");
        let err = game
            .act(Action::TickAll)
            .expect_err("a level refuses an action it does not offer");
        assert_eq!(err.code, paros_play::ActionErrorCode::NotAllowed);
    }
}

#[test]
fn the_level_map_lists_every_level() {
    let summaries = Game::levels();
    assert_eq!(summaries.len(), levels().len());
}

#[test]
fn every_act_is_registered_in_play_order() {
    let acts: Vec<u8> = levels().into_iter().map(|level| level.act).collect();
    assert!(
        acts.windows(2).all(|pair| pair[0] <= pair[1]),
        "the level map is in act order: {acts:?}"
    );
    for act in 1..=3u8 {
        assert!(acts.contains(&act), "act {act} has levels registered");
    }
}

#[test]
fn every_field_guide_link_is_a_bare_book_filename() {
    // The frontend prefixes `../`: the game is served from `/play/` beside the
    // book. A path here would resolve somewhere else entirely.
    for level in levels() {
        let link = level.field_guide;
        assert!(
            std::path::Path::new(link)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("html")),
            "{}: the field guide {link:?} is not a book page",
            level.id
        );
        assert!(
            !link.contains('/') && !link.starts_with('.'),
            "{}: the field guide {link:?} must be a bare filename",
            level.id
        );
    }
}

#[test]
fn every_level_briefs_the_mechanism_before_the_task() {
    // Two or three paragraphs of Paxos (plus at most a short list), not a
    // sentence of instructions: the briefing is what the book no longer
    // carries.
    for level in levels() {
        let blocks = level.briefing.split("\n\n").count();
        assert!(
            (2..=6).contains(&blocks),
            "{}: the briefing has {blocks} block(s); a mechanism briefing is two or three \
             paragraphs, plus at most a short list",
            level.id
        );
        assert!(
            !level.symbols.is_empty(),
            "{}: the level names the symbols it exercises",
            level.id
        );
    }
}

#[test]
fn a_levels_unlocks_are_flags_it_pins_manual() {
    // A level rewards the decision it made you take by hand, and nothing else.
    for level in levels() {
        for flag in level.unlocks {
            assert!(
                level.pinned_off.contains(flag),
                "{}: it unlocks {flag:?} without ever making it manual",
                level.id
            );
        }
    }
}
