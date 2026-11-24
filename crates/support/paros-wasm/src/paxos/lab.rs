//! The exercises of the Paxos course ("your turn"): a world of
//! `paros-consensus` that the reader drives by hand, with one goal.
//!
//! The reader is the network: each message on the wire can be delivered or
//! dropped. An exercise can also give the reader a few more moves (a proposer
//! opens a ballot, an acceptor loses its disk). The core decides everything
//! else. The goal is a predicate on the world and on the messages the reader
//! delivered, so the page holds no protocol logic: it draws the [`LabView`]
//! and sends the reader's [`Move`]s.
//!
//! A move outside the exercise is refused. A rule that the exercise turns off
//! is a move of the driver that the shipped code refuses (an acceptor that
//! boots on an empty disk), never a change of the core; the exercise says so
//! in `rule_off`. Each exercise has a solution in Rust, and a test plays it:
//! an exercise that the core no longer lets a reader solve fails CI.
//!
//! The engine draws no randomness, so a list of moves is a full replay: undo
//! starts the exercise again and plays every move but the last.
//!
//! A guided exercise (`gap` is the first) also has:
//! - an intro: the setup in [`Step`]s, each a sentence and its moves, which
//!   the page plays on the stage before the job ([`Lab::intro_step`]);
//! - parts: the goal as a checklist of [`Part`]s, each a predicate with two
//!   hints (the idea, the plan), the move to press now, and a predicate that
//!   says when it cannot hold again ([`LabView::parts`], [`LabView::hint`],
//!   [`LabView::stuck`]); the goal is that every part holds;
//! - a guide: a solution in [`Step`]s, which the tests play and which "Show
//!   me how" plays on the page ([`Lab::guide_step`]). A lab that played a
//!   guide step is a demo: its goal never counts as solved.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::paxos::action::Action;
use crate::paxos::level::WorldKind;
use crate::paxos::trace::compart::desk::{Desk, Verb};
use crate::paxos::trace::{lane_logs, lane_of, lane_state, lane_states, lanes, message_label};
use crate::paxos::view::{NodeFlavour, WorldView};
use crate::paxos::{ActionError, Game};
use crate::steps::LogCell;
use crate::steps::story::NodeState;

pub use crate::paxos::trace::Lane;

mod compartmentalized;
mod matchmaker;
mod multi_paxos;
mod quorums;
mod single_decree;

/// The setup of the single-decree exercises: acceptors 1, 2 and 3,
/// proposers 5 and 8, one decree.
const DECREE: &str = "act1/the-duel";

/// The setup of the log exercises: three nodes, each an acceptor and a
/// replica.
const LOG: &str = "act2/steady-state";

/// The grid: six acceptors in 2 rows and 3 columns.
const GRID: &str = "act4/the-grid";

/// Four nodes (node 3 outside the acceptors at first) and two matchmakers.
const MATCHMAKERS: &str = "act4/reconfigure";

/// Where an exercise runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Setup {
    /// A level of the game's worlds, by its id.
    Level(&'static str),
    /// The compartmentalized deployment: node 0 leads three acceptors, with
    /// two proxy leaders and two replicas ([`Desk`]).
    Compartments,
}

/// A move of an exercise other than deliver and drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Extra {
    /// An action of the game's worlds.
    Act(Action),
    /// A verb of the compartmentalized deployment.
    Compart(Verb),
}

/// How many moves an exercise takes at most: far above any solution, and a
/// bound on the replay of an undo.
const MOVES: usize = 400;

/// One exercise.
pub struct Exercise {
    /// A stable id: the address and the progress use it.
    pub id: &'static str,
    /// The tab's text.
    pub title: &'static str,
    /// The goal, in one or two sentences.
    pub task: &'static str,
    /// The rule this build turns off, in plain words, if any.
    pub rule_off: Option<&'static str>,
    /// What the reader sees when the goal holds: why it matters.
    pub done: &'static str,
    /// Where the exercise runs.
    setup_on: Setup,
    /// The moves played before the reader's first one. They are not the
    /// reader's: undo never takes one back.
    setup: fn(&mut Lab),
    /// The moves played after `setup`, step by step, with a sentence each:
    /// the page plays them before the job. Not the reader's either.
    intro: &'static [Step],
    /// The goal as a checklist; empty when the goal is one sentence.
    parts: &'static [Part],
    /// A solution step by step, with a sentence each, for "Show me how".
    guide: &'static [Step],
    /// The moves other than deliver and drop, with their button text.
    extras: fn() -> Vec<(&'static str, Extra)>,
    /// Whether the goal holds.
    goal: fn(&Lab) -> bool,
    /// A way to reach the goal, played by the tests.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "only the tests play the solutions")
    )]
    solution: fn(&mut Lab),
}

/// One step of an intro or a guide: a sentence, then the moves it plays.
pub struct Step {
    /// What the step does and why, in one or two sentences.
    pub says: &'static str,
    /// Its moves.
    pub play: fn(&mut Lab),
}

/// One line of the goal of a guided exercise.
pub struct Part {
    /// The line, as the checklist shows it.
    pub label: &'static str,
    /// Whether it holds now.
    pub holds: fn(&Lab) -> bool,
    /// The first hint: the idea.
    pub idea: &'static str,
    /// The second hint: the plan.
    pub plan: &'static str,
    /// The third hint: the move to press now, if one of the page's buttons
    /// is it.
    pub next: fn(&Lab) -> Option<Move>,
    /// Why it cannot hold again, whatever the reader does now, and what to
    /// do (undo); none while it still can.
    pub dead: fn(&Lab) -> Option<&'static str>,
}

/// An exercise with no intro, no parts or no guide.
const NONE: &[Step] = &[];
const NO_PARTS: &[Part] = &[];

/// Every exercise, in course order.
#[must_use]
pub fn exercises() -> &'static [Exercise] {
    &EXERCISES
}

/// The exercise `id`.
#[must_use]
pub fn exercise(id: &str) -> Option<&'static Exercise> {
    EXERCISES.iter().find(|exercise| exercise.id == id)
}

static EXERCISES: [Exercise; 11] = [
    single_decree::VOTE_NOT_CHOSEN,
    single_decree::TWO_VALUES,
    single_decree::DUEL,
    quorums::MAJORITY,
    multi_paxos::GAP,
    multi_paxos::READ,
    quorums::GRID_COLUMN,
    matchmaker::OLD_SET,
    compartmentalized::PROXY_DOWN,
    compartmentalized::LOST_VOTES,
    compartmentalized::REPLICA_WAITS,
];

/// Some node holds `value` chosen at `slot`.
fn chosen_at(world: &WorldView, slot: u64, value: &str) -> bool {
    world.nodes.iter().any(|node| {
        node.accepted
            .iter()
            .any(|record| record.slot == slot && record.chosen && record.value == value)
    })
}

/// Some node holds a value chosen at `slot`.
fn chosen_at_any(world: &WorldView, slot: u64) -> bool {
    world.nodes.iter().any(|node| {
        node.accepted
            .iter()
            .any(|record| record.slot == slot && record.chosen)
    })
}

/// How many acceptors voted for `value`. A proposer's card also lists the
/// value it proposes, which is not a vote.
fn voters(world: &WorldView, value: &str) -> usize {
    world
        .nodes
        .iter()
        .filter(|node| node.flavour == NodeFlavour::Acceptor)
        .filter(|node| node.accepted.iter().any(|vote| vote.value == value))
        .count()
}

/// A ballot `round.node` as a pair that orders as ballots do.
fn ballot(text: &str) -> (u64, u64) {
    let (round, node) = text.split_once('.').unwrap_or((text, "0"));
    (round.parse().unwrap_or(0), node.parse().unwrap_or(0))
}

/// One move of the reader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Move {
    /// Deliver the message `id`.
    Deliver {
        /// The message's id on the wire.
        id: u64,
    },
    /// Lose the message `id`.
    Drop {
        /// The message's id on the wire.
        id: u64,
    },
    /// Deliver every message on the wire, and the ones they send, until the
    /// wire is empty.
    DeliverAll,
    /// The extra move at `index` in [`LabView::extras`].
    Extra {
        /// Its index.
        index: usize,
    },
    /// Take back the last move.
    Undo,
    /// Start the exercise again.
    Reset,
}

/// What the page draws.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabView {
    /// The exercise's id.
    pub id: String,
    /// Its tab text.
    pub title: String,
    /// The goal.
    pub task: String,
    /// The rule the build turns off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule_off: Option<String>,
    /// The parties, in order.
    pub lanes: Vec<Lane>,
    /// A few lines of state per lane.
    pub state: Vec<Vec<String>>,
    /// Whether each lane is up.
    pub alive: Vec<bool>,
    /// The log of each lane, in lane order, for the log strip under its
    /// node: empty in a world of one decree.
    pub logs: Vec<Vec<LogCell>>,
    /// The typed state of each lane, as the story draws it: up or down,
    /// its role (a leader gets the accent ring), the holes of its log.
    pub nodes: Vec<NodeState>,
    /// The messages on the wire.
    pub wire: Vec<WireMessage>,
    /// The other moves, by their button text.
    pub extras: Vec<String>,
    /// The chosen values, as `alpha at 1.5` (one decree) or `slot 0 = alpha`
    /// (a log), in the order they were chosen.
    pub chosen: Vec<String>,
    /// Whether two values are chosen for one decree: the goal of the
    /// exercise that turns a rule off.
    pub two_values: bool,
    /// What the core said about the last move.
    pub said: Vec<String>,
    /// The last move was refused: why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
    /// How many moves the reader made.
    pub moves: usize,
    /// Whether the goal holds. Never in a demo.
    pub solved: bool,
    /// The goal as a checklist (a guided exercise); empty otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<PartView>,
    /// The hint of the first line that does not hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<HintView>,
    /// A line that cannot hold again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stuck: Option<StuckView>,
    /// How many intro steps the exercise has.
    pub intro: usize,
    /// How many guide steps the exercise has ("Show me how").
    pub guide: usize,
    /// A guide step played: "Show me how" runs.
    pub demo: bool,
    /// Why the goal matters, once it holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub done: Option<String>,
}

/// One message on the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireMessage {
    /// Its id.
    pub id: u64,
    /// The `Message` variant's name, e.g. `Prepare`.
    pub kind: String,
    /// The sending lane.
    pub from: String,
    /// The receiving lane.
    pub to: String,
    /// The render family: `prepare`, `promise`, ….
    pub phase: String,
    /// `Prepare 1.5`.
    pub label: String,
    /// What it says, in a short sentence.
    pub summary: String,
    /// The slot it names, if it names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<u64>,
}

/// The world an exercise runs.
enum Engine {
    /// A level of the game's worlds.
    Game(Box<Game>),
    /// The compartmentalized deployment.
    Compart(Box<Desk>),
}

/// One exercise in progress.
pub struct Lab {
    exercise: &'static Exercise,
    engine: Engine,
    /// The reader's moves since the start, for undo.
    moves: Vec<Move>,
    /// The messages the reader delivered, by (variant, receiving lane).
    delivered: BTreeMap<(String, String), usize>,
    /// The messages the reader dropped, by (variant, receiving lane).
    dropped: BTreeMap<(String, String), usize>,
    said: Vec<String>,
    refusal: Option<String>,
    /// The frames of the step being played, while an intro or a guide step
    /// plays: one per move.
    record: Option<Vec<Frame>>,
    /// The view before the intro, and each intro step as it played.
    intro_start: Option<LabView>,
    intro: Vec<Played>,
    /// A guide step played: the goal no longer counts as solved.
    demo: bool,
    /// A guide step plays now: its moves are not refused as the reader's.
    guiding: bool,
}

/// One move of an intro or a guide step, as the page plays it: the message
/// that crosses (lost or not), then the view after it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Frame {
    /// The message that the move delivered or dropped; none for another
    /// move.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<u64>,
    /// The move dropped the message.
    pub lost: bool,
    /// The extra move it played, by its index in [`LabView::extras`]; none
    /// for another move.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<usize>,
    /// The view after the move.
    pub view: LabView,
}

/// One step as it played: its sentence and its frames.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Played {
    /// The step's sentence.
    pub says: String,
    /// One frame per move, in order.
    pub frames: Vec<Frame>,
}

/// One line of the checklist, as the page draws it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartView {
    /// The line.
    pub label: String,
    /// It holds now.
    pub holds: bool,
}

/// The hint of the first line that does not hold, in three levels.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HintView {
    /// The line, by its index in [`LabView::parts`].
    pub part: usize,
    /// The idea.
    pub idea: String,
    /// The plan.
    pub plan: String,
    /// The move to press now, if a button of the page is it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<Move>,
}

/// A line of the goal that cannot hold again.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StuckView {
    /// The line, by its index in [`LabView::parts`].
    pub part: usize,
    /// Why, and what to do (undo).
    pub says: String,
}

impl Lab {
    /// Start exercise `id`.
    ///
    /// # Errors
    ///
    /// When no exercise has that id.
    pub fn new(id: &str) -> Result<Self, String> {
        let exercise = exercise(id).ok_or_else(|| format!("there is no exercise {id:?}"))?;
        Ok(Self::start(exercise))
    }

    fn start(exercise: &'static Exercise) -> Self {
        let engine = match exercise.setup_on {
            Setup::Level(level) => Engine::Game(Box::new(
                Game::new(level).expect("an exercise names a registered level"),
            )),
            Setup::Compartments => Engine::Compart(Box::default()),
        };
        let mut lab = Self {
            exercise,
            engine,
            moves: Vec::new(),
            delivered: BTreeMap::new(),
            dropped: BTreeMap::new(),
            said: Vec::new(),
            refusal: None,
            record: None,
            intro_start: None,
            intro: Vec::new(),
            demo: false,
            guiding: false,
        };
        (exercise.setup)(&mut lab);
        if !exercise.intro.is_empty() {
            lab.intro_start = Some(lab.view());
            for step in exercise.intro {
                let played = lab.recorded(step);
                lab.intro.push(played);
            }
        }
        // The setup is not the reader's: no frame of the intro counts a move.
        for frame in lab.intro.iter_mut().flat_map(|step| step.frames.iter_mut()) {
            frame.view.moves = 0;
        }
        lab.moves.clear();
        lab.delivered.clear();
        lab.dropped.clear();
        lab.said = lab.texts();
        lab
    }

    /// Play `action` as part of the setup.
    ///
    /// # Panics
    ///
    /// When the core refuses it: a setup is a run that works.
    fn act(&mut self, action: &Action) {
        self.game_mut()
            .act(action)
            .expect("the setup of an exercise is a run that works");
        self.said = self.texts();
        self.frame(None, false, None);
    }

    /// Keep a frame of the move just played, while a step is recorded.
    fn frame(&mut self, message: Option<u64>, lost: bool, extra: Option<usize>) {
        if let Some(mut frames) = self.record.take() {
            frames.push(Frame {
                message,
                lost,
                extra,
                view: self.view(),
            });
            self.record = Some(frames);
        }
    }

    /// Play `step` and keep a frame of each of its moves.
    fn recorded(&mut self, step: &Step) -> Played {
        assert!(self.record.is_none(), "one step is recorded at a time");
        self.record = Some(Vec::new());
        (step.play)(self);
        let frames = self.record.take().expect("the step was recorded");
        Played {
            says: step.says.to_string(),
            frames,
        }
    }

    /// The view before the intro, when the exercise has one.
    #[must_use]
    pub fn intro_start(&self) -> Option<&LabView> {
        self.intro_start.as_ref()
    }

    /// Intro step `n` as it played at the start, if the intro has one.
    #[must_use]
    pub fn intro_step(&self, n: usize) -> Option<&Played> {
        self.intro.get(n)
    }

    /// Play guide step `n` on this lab, if the guide has one. The lab is a
    /// demo from then on: its goal never counts as solved.
    pub fn guide_step(&mut self, n: usize) -> Option<Played> {
        let step = self.exercise.guide.get(n)?;
        self.demo = true;
        self.guiding = true;
        let played = self.recorded(step);
        self.guiding = false;
        Some(played)
    }

    /// Play `verb` as part of the setup.
    ///
    /// # Panics
    ///
    /// When the deployment refuses it: a setup is a run that works.
    fn verb(&mut self, verb: &Verb) {
        self.desk_mut()
            .act(verb)
            .expect("the setup of an exercise is a run that works");
        self.said = self.texts();
        self.frame(None, false, None);
    }

    /// Play one move. A refused move changes nothing and says why.
    pub fn play(&mut self, chosen: &Move) {
        match chosen {
            Move::Reset => *self = Self::start(self.exercise),
            // A demo plays its guide alone: the reader starts again first.
            _ if self.demo && !self.guiding => {
                self.refusal = Some("Start again to play it yourself.".to_string());
            }
            Move::Undo => {
                let mut moves = std::mem::take(&mut self.moves);
                moves.pop();
                *self = Self::start(self.exercise);
                for again in &moves {
                    self.play(again);
                }
            }
            other => {
                if self.moves.len() >= MOVES {
                    self.refusal = Some(format!("An exercise takes at most {MOVES} moves."));
                    return;
                }
                match self.apply(other) {
                    Ok(()) => {
                        self.moves.push(other.clone());
                        self.refusal = None;
                        self.said = self.texts();
                        match other {
                            Move::Deliver { id } => self.frame(Some(*id), false, None),
                            Move::Drop { id } => self.frame(Some(*id), true, None),
                            Move::Extra { index } => self.frame(None, false, Some(*index)),
                            _ => self.frame(None, false, None),
                        }
                    }
                    Err(why) => self.refusal = Some(why),
                }
            }
        }
    }

    fn apply(&mut self, chosen: &Move) -> Result<(), String> {
        match chosen {
            Move::DeliverAll => {
                let mut left = MOVES;
                while let Some(message) = self.wire().into_iter().next() {
                    assert!(left > 0, "a wire empties within {MOVES} deliveries");
                    left -= 1;
                    self.apply(&Move::Deliver { id: message.id })?;
                }
                Ok(())
            }
            Move::Deliver { id } | Move::Drop { id } => {
                let deliver = matches!(chosen, Move::Deliver { .. });
                let message = self
                    .wire()
                    .into_iter()
                    .find(|m| m.id == *id)
                    .ok_or_else(|| format!("No message {id} is on the wire."))?;
                match &mut self.engine {
                    Engine::Game(game) => game
                        .act(&if deliver {
                            Action::Deliver { id: *id }
                        } else {
                            Action::Drop { id: *id }
                        })
                        .map_err(|error: ActionError| error.to_string())?,
                    Engine::Compart(desk) if deliver => desk.deliver(*id)?,
                    Engine::Compart(desk) => desk.lose(*id)?,
                }
                let count = if deliver {
                    &mut self.delivered
                } else {
                    &mut self.dropped
                };
                *count.entry((message.kind, message.to)).or_default() += 1;
                Ok(())
            }
            Move::Extra { index } => {
                let extra = (self.exercise.extras)()
                    .into_iter()
                    .nth(*index)
                    .map(|(_, extra)| extra)
                    .ok_or_else(|| "This exercise has no such move.".to_string())?;
                match (&mut self.engine, extra) {
                    (Engine::Game(game), Extra::Act(action)) => game
                        .act(&action)
                        .map_err(|error: ActionError| error.to_string()),
                    (Engine::Compart(desk), Extra::Compart(verb)) => desk.act(&verb),
                    _ => unreachable!("an exercise's extra moves fit its world"),
                }
            }
            Move::Undo | Move::Reset => unreachable!("handled in play"),
        }
    }

    /// How many `Nack`s the reader delivered to `proposer`.
    #[must_use]
    pub fn refusals(&self, proposer: u64) -> usize {
        self.delivered
            .get(&("Nack".to_string(), format!("n{proposer}")))
            .copied()
            .unwrap_or(0)
    }

    /// How many messages the reader delivered to node `node`.
    #[must_use]
    pub fn delivered_to(&self, node: u64) -> usize {
        let lane = format!("n{node}");
        self.delivered
            .iter()
            .filter(|((_, to), _)| *to == lane)
            .map(|(_, count)| count)
            .sum()
    }

    /// How many `kind` messages the reader dropped on their way to `lane`.
    #[must_use]
    pub fn dropped(&self, kind: &str, lane: &str) -> usize {
        self.dropped
            .get(&(kind.to_string(), lane.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// Whether the goal holds. A demo's goal never counts.
    #[must_use]
    pub fn solved(&self) -> bool {
        !self.demo && self.goal()
    }

    /// Whether the goal holds, demo or not.
    #[must_use]
    pub fn goal(&self) -> bool {
        (self.exercise.goal)(self)
    }

    /// The moves the reader played, oldest first.
    #[must_use]
    pub fn moves(&self) -> &[Move] {
        &self.moves
    }

    /// The checklist, the hint and the dead end of a guided exercise.
    fn guided(&self) -> (Vec<PartView>, Option<HintView>, Option<StuckView>) {
        let parts = self.exercise.parts;
        let holds: Vec<bool> = parts.iter().map(|part| (part.holds)(self)).collect();
        let stuck = parts.iter().enumerate().find_map(|(k, part)| {
            if holds[k] {
                return None;
            }
            (part.dead)(self).map(|says| StuckView {
                part: k,
                says: says.to_string(),
            })
        });
        let hint = if stuck.is_some() {
            None
        } else {
            holds.iter().position(|holds| !holds).map(|k| HintView {
                part: k,
                idea: parts[k].idea.to_string(),
                plan: parts[k].plan.to_string(),
                next: (parts[k].next)(self),
            })
        };
        let lines = parts
            .iter()
            .zip(&holds)
            .map(|(part, holds)| PartView {
                label: part.label.to_string(),
                holds: *holds,
            })
            .collect();
        (lines, hint, stuck)
    }

    fn game(&self) -> &Game {
        match &self.engine {
            Engine::Game(game) => game,
            Engine::Compart(_) => unreachable!("this exercise runs a level of the game"),
        }
    }

    fn game_mut(&mut self) -> &mut Game {
        match &mut self.engine {
            Engine::Game(game) => game,
            Engine::Compart(_) => unreachable!("this exercise runs a level of the game"),
        }
    }

    fn desk(&self) -> &Desk {
        match &self.engine {
            Engine::Compart(desk) => desk,
            Engine::Game(_) => unreachable!("this exercise runs the compartments"),
        }
    }

    fn desk_mut(&mut self) -> &mut Desk {
        match &mut self.engine {
            Engine::Compart(desk) => desk,
            Engine::Game(_) => unreachable!("this exercise runs the compartments"),
        }
    }

    fn view_of_world(&self) -> WorldView {
        self.game().world().view()
    }

    /// The messages on the wire, oldest first.
    fn wire(&self) -> Vec<WireMessage> {
        match &self.engine {
            Engine::Game(game) => {
                let log = matches!(game.world(), WorldKind::Log(_));
                game.world()
                    .view()
                    .wire
                    .iter()
                    .map(|message| WireMessage {
                        id: message.id,
                        kind: message.kind.clone(),
                        from: lane_of(message, true),
                        to: lane_of(message, false),
                        phase: message.phase.clone(),
                        label: message_label(message, log),
                        summary: message.summary.clone(),
                        slot: message.slot.filter(|_| log),
                    })
                    .collect()
            }
            Engine::Compart(desk) => desk.wire(),
        }
    }

    /// What the last move did.
    fn texts(&self) -> Vec<String> {
        match &self.engine {
            Engine::Game(game) => game
                .narration()
                .iter()
                .map(|event| event.text.clone())
                .collect(),
            Engine::Compart(desk) => desk.said().to_vec(),
        }
    }

    /// The view the page draws.
    #[must_use]
    pub fn view(&self) -> LabView {
        let (lanes, state, alive, chosen) = match &self.engine {
            Engine::Game(game) => game_parts(game),
            Engine::Compart(desk) => (Desk::lanes(), desk.state(), desk.alive(), desk.chosen()),
        };
        let (logs, nodes) = match &self.engine {
            Engine::Game(game) => {
                let world = game.world().view();
                (lane_logs(&world), lane_states(&world))
            }
            Engine::Compart(desk) => (desk.logs(), desk.nodes()),
        };
        let solved = self.solved();
        let (parts, hint, stuck) = self.guided();
        LabView {
            id: self.exercise.id.to_string(),
            title: self.exercise.title.to_string(),
            task: self.exercise.task.to_string(),
            rule_off: self.exercise.rule_off.map(str::to_string),
            lanes,
            state,
            alive,
            logs,
            nodes,
            wire: self.wire(),
            extras: (self.exercise.extras)()
                .into_iter()
                .map(|(label, _)| label.to_string())
                .collect(),
            two_values: match &self.engine {
                Engine::Game(game) => match game.world() {
                    WorldKind::Decree(decree) => decree.decisions().len() > 1,
                    WorldKind::Log(_) => false,
                },
                Engine::Compart(_) => false,
            },
            chosen,
            said: self.said.clone(),
            refused: self.refusal.clone(),
            moves: self.moves.len(),
            solved,
            parts,
            hint,
            stuck,
            intro: self.exercise.intro.len(),
            guide: self.exercise.guide.len(),
            demo: self.demo,
            done: solved.then(|| self.exercise.done.to_string()),
        }
    }

    // ---- what a solution is written in ----------------------------------------

    /// Deliver the first message on the wire that `hit` matches.
    fn deliver(&mut self, hit: impl Fn(&WireMessage) -> bool) {
        let id = self
            .wire()
            .into_iter()
            .find(|m| hit(m))
            .expect("a solution delivers a message that is on the wire")
            .id;
        self.must(&Move::Deliver { id });
    }

    /// Deliver every message that `hit` matches, the new ones too, until none
    /// is left.
    fn settle(&mut self, hit: impl Fn(&WireMessage) -> bool) {
        while let Some(message) = self.wire().into_iter().find(|m| hit(m)) {
            self.must(&Move::Deliver { id: message.id });
        }
    }

    /// Drop every message on the wire that `hit` matches.
    fn drop_all(&mut self, hit: impl Fn(&WireMessage) -> bool) {
        for message in self.wire().into_iter().filter(|m| hit(m)) {
            self.must(&Move::Drop { id: message.id });
        }
    }

    /// Play the extra move whose button says `label`.
    fn extra(&mut self, label: &str) {
        let index = (self.exercise.extras)()
            .iter()
            .position(|(text, _)| *text == label)
            .expect("a solution names an extra move of its exercise");
        self.must(&Move::Extra { index });
    }

    fn must(&mut self, chosen: &Move) {
        self.play(chosen);
        assert!(
            self.refusal.is_none(),
            "{}: the move {chosen:?} is refused: {:?}",
            self.exercise.id,
            self.refusal
        );
    }
}

/// The lanes, their state, which are up, and the chosen values of a game.
fn game_parts(game: &Game) -> (Vec<Lane>, Vec<Vec<String>>, Vec<bool>, Vec<String>) {
    let world = game.world().view();
    let lanes = lanes(&world);
    // A matchmaker lane is up or down as its matchmaker is.
    let alive = lane_states(&world).iter().map(|node| node.alive).collect();
    let chosen = match game.world() {
        WorldKind::Decree(decree) => decree
            .decisions()
            .iter()
            .map(|(ballot, command)| {
                format!(
                    "{} at {}",
                    crate::paxos::view::show_command(command),
                    crate::paxos::view::show_ballot(*ballot)
                )
            })
            .collect(),
        WorldKind::Log(_) => {
            let mut slots: BTreeMap<u64, String> = BTreeMap::new();
            for node in &world.nodes {
                for record in node.accepted.iter().filter(|record| record.chosen) {
                    slots.insert(record.slot, record.value.clone());
                }
            }
            slots
                .into_iter()
                .map(|(slot, value)| format!("slot {slot} = {value}"))
                .collect()
        }
    };
    (lanes, lane_state(&world), alive, chosen)
}

#[cfg(test)]
mod tests;
