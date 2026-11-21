//! An exercise over a world whose moves are named by a key: the shell of
//! the register lab ([`super::register`]) and the record lab
//! ([`super::records`]).
//!
//! The world lists the moves the reader may make now, each with a stable
//! key (`P>m1:deliver`, `seal:1`), and plays one by its key. The shell keeps
//! the exercise: its setup, its goal, the reader's moves, undo and reset.
//! The world draws no randomness, so a list of keys is a full replay: undo
//! starts the exercise again and plays every key but the last.

use serde::{Deserialize, Serialize};

/// How many moves an exercise takes at most: far above any solution, and a
/// bound on the replay of an undo.
const MOVES: usize = 200;

/// One move the reader may make now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Action {
    /// A stable name: the move plays it, the smoke test clicks it.
    pub key: String,
    /// The button's text.
    pub label: String,
    /// Where the page puts the button: a caller, a matchmaker, a segment.
    pub group: String,
}

/// A world the reader drives by keys.
pub trait World: Sized {
    /// What the page draws of the world.
    type View: Serialize;

    /// The world at the start of every exercise.
    fn fresh() -> Self;

    /// The moves the reader may make now.
    fn actions(&self) -> Vec<Action>;

    /// Play the move `key`.
    ///
    /// # Errors
    ///
    /// Why the move is refused. A refused move changes nothing.
    fn act(&mut self, key: &str) -> Result<(), String>;

    /// The view the page draws.
    fn view(&self) -> Self::View;

    /// How many past events the world holds.
    fn events(&self) -> usize;
}

/// One exercise.
pub struct Exercise<W: 'static> {
    /// A stable id: the address and the progress use it.
    pub id: &'static str,
    /// The tab's text.
    pub title: &'static str,
    /// The goal, in one or two sentences.
    pub task: &'static str,
    /// What the reader sees when the goal holds: why it matters.
    pub done: &'static str,
    /// The moves made before the reader's first one. Undo never takes one
    /// back.
    pub setup: &'static [&'static str],
    /// Whether the goal holds.
    pub goal: fn(&W) -> bool,
    /// A way to reach the goal, played by the tests and, with the same
    /// keys, by the browser smoke test.
    pub solution: &'static [&'static str],
}

/// A move of the reader.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Move {
    /// Play the action with this key.
    Act {
        /// The key, from [`LabView::actions`].
        key: String,
    },
    /// Take the last move back.
    Undo,
    /// Start the exercise again.
    Reset,
}

/// What the page draws.
#[derive(Clone, Debug, Serialize)]
pub struct LabView<V> {
    /// The exercise.
    pub id: String,
    /// The tab's text.
    pub title: String,
    /// The goal.
    pub task: String,
    /// The world.
    pub world: V,
    /// The moves the reader may make now.
    pub actions: Vec<Action>,
    /// Why the last move was refused.
    pub refused: Option<String>,
    /// The reader's moves so far.
    pub moves: usize,
    /// How many events the setup made: the page shows the events after them.
    pub setup: usize,
    /// Whether the goal holds.
    pub solved: bool,
    /// Why it matters, once the goal holds.
    pub done: Option<String>,
}

/// One exercise in progress.
pub struct Lab<W: World + 'static> {
    exercise: &'static Exercise<W>,
    world: W,
    moves: Vec<String>,
    setup: usize,
    refusal: Option<String>,
}

impl<W: World + 'static> Lab<W> {
    /// Start exercise `id` of `exercises`.
    ///
    /// # Errors
    ///
    /// When no exercise has that id.
    pub fn new(exercises: &'static [Exercise<W>], id: &str) -> Result<Self, String> {
        let exercise = exercises
            .iter()
            .find(|exercise| exercise.id == id)
            .ok_or_else(|| format!("there is no exercise {id:?}"))?;
        Ok(Self::start(exercise))
    }

    /// Start `exercise`.
    ///
    /// # Panics
    ///
    /// When a move of the setup is refused: the setup is a run that works.
    #[must_use]
    pub fn start(exercise: &'static Exercise<W>) -> Self {
        let mut world = W::fresh();
        for key in exercise.setup {
            if let Err(why) = world.act(key) {
                panic!("{}: the setup move {key} is refused: {why}", exercise.id);
            }
        }
        Self {
            exercise,
            setup: world.events(),
            world,
            moves: Vec::new(),
            refusal: None,
        }
    }

    /// The exercise.
    #[must_use]
    pub fn exercise(&self) -> &'static Exercise<W> {
        self.exercise
    }

    /// The world.
    #[must_use]
    pub fn world(&self) -> &W {
        &self.world
    }

    /// Play one move. A refused move changes nothing and says why.
    pub fn play(&mut self, chosen: &Move) {
        match chosen {
            Move::Reset => *self = Self::start(self.exercise),
            Move::Undo => {
                let mut moves = std::mem::take(&mut self.moves);
                moves.pop();
                *self = Self::start(self.exercise);
                for key in moves {
                    self.play(&Move::Act { key });
                }
            }
            Move::Act { key } => {
                if self.moves.len() >= MOVES {
                    self.refusal = Some(format!("An exercise takes at most {MOVES} moves."));
                    return;
                }
                if !self.world.actions().iter().any(|action| action.key == *key) {
                    self.refusal = Some("This move is not open now.".to_string());
                    return;
                }
                match self.world.act(key) {
                    Ok(()) => {
                        self.moves.push(key.clone());
                        self.refusal = None;
                    }
                    Err(why) => self.refusal = Some(why),
                }
            }
        }
    }

    /// Play `key`, which must be open and must not be refused: what a
    /// solution is written in.
    ///
    /// # Panics
    ///
    /// When the move is not open now, or it is refused.
    pub fn make(&mut self, key: &str) {
        self.play(&Move::Act {
            key: key.to_string(),
        });
        assert!(
            self.refusal.is_none(),
            "{}: {key} is refused: {:?}",
            self.exercise.id,
            self.refusal
        );
    }

    /// Whether the goal holds.
    #[must_use]
    pub fn solved(&self) -> bool {
        (self.exercise.goal)(&self.world)
    }

    /// Why the last move was refused.
    #[must_use]
    pub fn refusal(&self) -> Option<&str> {
        self.refusal.as_deref()
    }

    /// The view the page draws.
    #[must_use]
    pub fn view(&self) -> LabView<W::View> {
        let solved = self.solved();
        LabView {
            id: self.exercise.id.to_string(),
            title: self.exercise.title.to_string(),
            task: self.exercise.task.to_string(),
            world: self.world.view(),
            actions: self.world.actions(),
            refused: self.refusal.clone(),
            moves: self.moves.len(),
            setup: self.setup,
            solved,
            done: solved.then(|| self.exercise.done.to_string()),
        }
    }
}

/// Every exercise's solution reaches its goal, from a start where it does
/// not hold, and every sequence of up to `depth` open moves trips no
/// assertion of `paros-journal` (an abort in wasm). The tests of each lab
/// call it.
///
/// # Panics
///
/// When one of these fails.
#[cfg(test)]
pub fn check_exercises<W: World + Clone + 'static>(
    exercises: &'static [Exercise<W>],
    depth: usize,
) {
    let ids: std::collections::BTreeSet<&str> = exercises.iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), exercises.len(), "the exercise ids differ");
    for exercise in exercises {
        let mut lab = Lab::start(exercise);
        assert!(
            !lab.solved(),
            "{}: the goal does not hold at the start",
            exercise.id
        );
        for key in exercise.solution {
            lab.make(key);
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
        lab.play(&Move::Undo);
        assert!(
            !lab.solved(),
            "{}: undo takes the last move back",
            exercise.id
        );
        walk(&Lab::start(exercise).world, depth);
    }
}

/// Every sequence of up to `depth` open moves from `world`: each move is
/// one event, and a refused move moves nothing.
#[cfg(test)]
fn walk<W: World + Clone>(world: &W, depth: usize) {
    if depth == 0 {
        return;
    }
    let actions = world.actions();
    let keys: std::collections::BTreeSet<&str> = actions.iter().map(|a| a.key.as_str()).collect();
    assert_eq!(keys.len(), actions.len(), "one button per move");
    for action in &actions {
        let mut next = world.clone();
        let before = next.events();
        match next.act(&action.key) {
            Ok(()) => assert!(next.events() > before, "{}: a move is an event", action.key),
            Err(_) => assert_eq!(next.events(), before, "a refused move moves nothing"),
        }
        let _ = next.view();
        walk(&next, depth - 1);
    }
}
