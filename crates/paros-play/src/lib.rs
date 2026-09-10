//! **paros play** — the interactive Paxos game's engine.
//!
//! The player is the network and the clock: which message lands next, which
//! node ticks, who crashes, who proposes. And when a level makes a role
//! manual, the player *is* that role — promise or nack, which value to
//! re-propose, fill the hole or not, serve the read or wait — and the real
//! `paros-core` state machine marks the answer.
//!
//! # What this crate is, and is not
//!
//! It is a **driver**. `paros-core` is not modified, not forked, not
//! feature-gated and not perturbed: the engine calls its public API and
//! nothing else, exactly as `crates/paros/src/driver` does with a real network
//! underneath instead of a player. A wrong answer never enters the core — the
//! engine computes the core's own answer on a *clone of the role*, and the
//! world only advances when the two agree. There is no toy acceptor anywhere.
//!
//! It draws no randomness and reads no clock, so the action log is the whole
//! state: [`Game::undo`] rebuilds the world from the level's setup and replays
//! all but the last action, and the result is bit-identical.
//!
//! # The pieces
//!
//! - [`world::World`] — the log world: `ColocatedNode`s, disks, a wire, a
//!   clock. Act II onward.
//! - [`world::decree::DecreeWorld`] — the Act I world: bare `Proposer` and
//!   `Acceptor` roles over one slot.
//! - [`action::Action`] — every player verb; [`action::ActionError`] every
//!   refusal.
//! - [`prompt::Prompt`] — the questions a manual role is asked, and the judge.
//! - [`auto::Automation`] — automation as reward, and the pump it enables.
//! - [`level::Level`] — briefing, goal, hint, reference solution.
//! - [`narration::NarrationEvent`] — what the game says just happened, derived
//!   from the transition rather than scripted.
//! - [`view`] — the one contract the browser reads.

pub mod action;
pub mod auto;
pub mod level;
pub mod narration;
pub mod prompt;
pub mod view;
pub mod world;

use paros_core::NodeId;

pub use action::{Action, ActionError, ActionErrorCode};
pub use level::{GoalStatus, Level, WorldKind};
pub use view::{GameView, LevelSummary};

use auto::{ALL_FLAGS, Automation, AutomationFlag};
use narration::{NarrationEvent, NarrationKind};
use prompt::{ALL_PROMPTS, Verdict};
use view::{ActionView, AutomationFlagView, AutomationView, LevelView, PromptView};
use world::WorldPolicy;

/// One level in progress: the world, the automation flags, and the action log
/// that is also the undo stack.
pub struct Game {
    level: &'static Level,
    world: WorldKind,
    automation: Automation,
    log: Vec<Action>,
    /// What each logged action did, in Paxos — parallel to `log`, rebuilt by a
    /// replay exactly as the world is.
    narration: Vec<Vec<NarrationEvent>>,
    mistakes: u32,
}

impl Game {
    /// Start `level_id` from its setup.
    ///
    /// # Errors
    ///
    /// [`ActionErrorCode::UnknownLevel`] when no level has that id.
    pub fn new(level_id: &str) -> Result<Self, ActionError> {
        let level = level::level(level_id).ok_or_else(|| {
            ActionError::new(
                ActionErrorCode::UnknownLevel,
                format!("there is no level {level_id:?}"),
            )
        })?;
        let mut game = Self {
            level,
            world: (level.setup)(),
            automation: initial_automation(level),
            log: Vec::new(),
            narration: Vec::new(),
            mistakes: 0,
        };
        game.sync_policy();
        Ok(game)
    }

    /// Every registered level, in play order.
    #[must_use]
    pub fn levels() -> Vec<LevelSummary> {
        level::levels().into_iter().map(Level::summary).collect()
    }

    /// The level being played.
    #[must_use]
    pub fn level(&self) -> &'static Level {
        self.level
    }

    /// The world, for a goal predicate or a test.
    #[must_use]
    pub fn world(&self) -> &WorldKind {
        &self.world
    }

    /// Whether the level's goal is reached.
    ///
    /// A world that has been asked to hold **two values for one slot** fails
    /// here, ahead of the level's own predicate and whatever it was watching:
    /// that is the one thing Paxos promises can never happen, so the game says
    /// so out loud rather than letting a level report progress on top of it.
    #[must_use]
    pub fn goal(&self) -> GoalStatus {
        if let Some(detail) = self.world.violation() {
            return GoalStatus::Failed(detail);
        }
        (self.level.goal)(&self.world)
    }

    /// How many prompts the player has got wrong.
    #[must_use]
    pub fn mistakes(&self) -> u32 {
        self.mistakes
    }

    /// The action log so far.
    #[must_use]
    pub fn log(&self) -> &[Action] {
        &self.log
    }

    /// What the last action did, in Paxos. Empty before the first move.
    #[must_use]
    pub fn narration(&self) -> &[NarrationEvent] {
        self.narration.last().map_or(&[], Vec::as_slice)
    }

    /// Every action's narration, in play order — the whole stream.
    #[must_use]
    pub fn narration_log(&self) -> &[Vec<NarrationEvent>] {
        &self.narration
    }

    /// Play one move.
    ///
    /// A **wrong prompt answer is not an error**: it costs a mistake and an
    /// explanation, both of which show up in [`Game::view`], and the world does
    /// not move. An `Err` here means the move itself was not available — an
    /// action the level does not offer, a message that is not in flight, a node
    /// that is crashed, an answer to no open prompt.
    ///
    /// # Errors
    ///
    /// See [`ActionErrorCode`].
    pub fn act(&mut self, action: Action) -> Result<(), ActionError> {
        if !self.level.allows(action.kind()) {
            return Err(ActionError::new(
                ActionErrorCode::NotAllowed,
                format!("this level does not offer {}", action.label()),
            ));
        }
        let events = self.apply(&action)?;
        self.log.push(action);
        self.narration.push(events);
        Ok(())
    }

    /// Undo the last move by replaying every earlier one from the level's
    /// setup. A no-op when nothing has been played.
    ///
    /// This is exact, not approximate: the core is deterministic and the engine
    /// draws nothing, so the rebuilt world is the one that was there before.
    pub fn undo(&mut self) {
        if self.log.pop().is_none() {
            return;
        }
        self.narration.pop();
        self.rebuild();
    }

    /// Start the level again from its setup.
    pub fn reset(&mut self) {
        self.log.clear();
        self.narration.clear();
        self.rebuild();
    }

    /// Render everything the browser needs for one frame.
    #[must_use]
    pub fn view(&self) -> GameView {
        GameView {
            level: LevelView {
                id: self.level.id.to_string(),
                act: self.level.act,
                title: self.level.title.to_string(),
                briefing: self.level.briefing.to_string(),
                field_guide: self.level.field_guide.to_string(),
                symbols: self
                    .level
                    .symbols
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
                allowed_actions: self.level.allowed_actions.to_vec(),
                unlocks: self.level.unlocks.to_vec(),
                hint: (self.level.hint)(&self.world, self.mistakes),
            },
            world: self.world.view(),
            prompt: self.world.prompt().map(|prompt| PromptView {
                id: prompt.id,
                kind: prompt.kind,
                node: prompt.node,
                question: prompt.question.clone(),
                state_summary: prompt.state_summary.clone(),
                choices: prompt.choices.clone(),
                feedback: prompt.feedback.clone(),
            }),
            goal: self.goal().view(),
            log: self
                .log
                .iter()
                .enumerate()
                .map(|(index, action)| ActionView {
                    index,
                    kind: action.kind(),
                    label: action.label(),
                    narration: self
                        .narration
                        .get(index)
                        .map(|events| events.iter().map(NarrationEvent::view).collect())
                        .unwrap_or_default(),
                })
                .collect(),
            automation: AutomationView {
                flags: ALL_FLAGS
                    .iter()
                    .map(|flag| AutomationFlagView {
                        flag: *flag,
                        label: flag.label().to_string(),
                        on: self.automation.is_on(*flag),
                        unlocked: self.automation.unlocked.contains(flag),
                        pinned_off: self.automation.is_pinned_off(*flag),
                    })
                    .collect(),
            },
            mistakes: self.mistakes,
            narration: self.narration().iter().map(NarrationEvent::view).collect(),
        }
    }

    // ---- internals ---------------------------------------------------------

    fn rebuild(&mut self) {
        self.world = (self.level.setup)();
        self.automation = initial_automation(self.level);
        self.mistakes = 0;
        self.sync_policy();
        let replay = std::mem::take(&mut self.log);
        self.narration.clear();
        for action in &replay {
            // Every action in the log was accepted once, from this same start
            // state, by this same deterministic engine — narration included:
            // every line is derived from the transition, and the transitions
            // are the same ones.
            let events = self.apply(action).unwrap_or_default();
            self.narration.push(events);
        }
        self.log = replay;
    }

    /// Derive the world's policy from the flag set, run the action, and take
    /// the narration it produced.
    fn apply(&mut self, action: &Action) -> Result<Vec<NarrationEvent>, ActionError> {
        self.sync_policy();
        self.world.clear_narration();
        let goal_before = self.goal().is_reached();
        match action {
            Action::Deliver { id } => self.world.deliver(*id)?,
            Action::Drop { id } => self.world.drop_message(*id)?,
            Action::Duplicate { id, to } => self.world.duplicate(*id, *to)?,
            Action::Tick { node } => self.log_world()?.tick(NodeId(*node))?,
            Action::TickAll => self.log_world()?.tick_all()?,
            Action::Crash { node } => self.log_world()?.crash(NodeId(*node))?,
            Action::CrashAt { node, seam } => {
                self.log_world()?.crash_at(NodeId(*node), *seam)?;
            }
            Action::Restart { node } => self.log_world()?.restart(NodeId(*node))?,
            Action::Propose {
                node,
                client,
                value,
                column,
            } => self
                .log_world()?
                .propose(NodeId(*node), *client, value, *column)?,
            Action::StartElection { node } => self.log_world()?.start_election(NodeId(*node))?,
            Action::SetElectionTimeout { node, ticks } => {
                self.log_world()?
                    .set_election_timeout(NodeId(*node), *ticks)?;
            }
            Action::ReadIndex { node, client } => {
                let world = self.log_world()?;
                let client = match client {
                    Some(client) => *client,
                    None => world.clients().first().copied().ok_or_else(|| {
                        ActionError::new(
                            ActionErrorCode::UnknownParty,
                            "this level has no client to read for",
                        )
                    })?,
                };
                world.read_index(NodeId(*node), client)?;
            }
            Action::QuorumRead { node, client } => {
                let world = self.log_world()?;
                let client = match client {
                    Some(client) => *client,
                    None => world.clients().first().copied().ok_or_else(|| {
                        ActionError::new(
                            ActionErrorCode::UnknownParty,
                            "this level has no client to read for",
                        )
                    })?,
                };
                world.quorum_read(NodeId(*node), client)?;
            }
            Action::Relinquish { node, to } => {
                self.log_world()?.relinquish(NodeId(*node), NodeId(*to))?;
            }
            Action::Corrupt { node, slot } => {
                self.log_world()?
                    .corrupt(NodeId(*node), paros_core::Slot(*slot))?;
            }
            Action::Wipe { node } => self.log_world()?.wipe(NodeId(*node))?,
            Action::Retry { node, client, seq } => {
                self.log_world()?.retry(NodeId(*node), *client, *seq)?;
            }
            Action::Compact { node, up_to } => {
                self.log_world()?.compact(NodeId(*node), *up_to)?;
            }
            Action::ResendPending { node } => self.log_world()?.resend_pending(NodeId(*node))?,
            Action::StepDown { node } => self.log_world()?.step_down(NodeId(*node))?,
            Action::OpenBallot { proposer, value } => {
                self.decree_world()?.open_ballot(*proposer, value)?;
            }
            Action::SetReach { phase, nodes } => {
                self.decree_world()?.set_reach(*phase, nodes)?;
            }
            Action::Answer { prompt, choice } => {
                if self.world.answer(*prompt, choice)? == Verdict::Wrong {
                    self.mistakes += 1;
                    return Ok(self.world.take_narration());
                }
            }
            Action::SetAutomation { flag, on } => {
                self.set_automation(*flag, *on)?;
                self.sync_policy();
            }
        }
        auto::pump(&mut self.world, &self.automation);
        let mut events = self.world.take_narration();
        if !goal_before && let GoalStatus::Reached(detail) = self.goal() {
            events.push(NarrationEvent::new(NarrationKind::Goal, detail));
        }
        Ok(events)
    }

    fn set_automation(&mut self, flag: AutomationFlag, on: bool) -> Result<(), ActionError> {
        if !self.automation.unlocked.contains(&flag) {
            return Err(ActionError::new(
                ActionErrorCode::NotUnlocked,
                format!("{} is not available in this level", flag.label()),
            ));
        }
        if on && self.automation.is_pinned_off(flag) {
            return Err(ActionError::new(
                ActionErrorCode::PinnedOff,
                format!(
                    "{} stays manual here: it is what this level teaches",
                    flag.label()
                ),
            ));
        }
        if on {
            self.automation.on.insert(flag);
        } else {
            self.automation.on.remove(&flag);
        }
        Ok(())
    }

    fn sync_policy(&mut self) {
        let manual = ALL_PROMPTS
            .iter()
            .copied()
            .filter(|kind| !self.automation.is_on(kind.flag()))
            .collect();
        self.world.set_policy(WorldPolicy {
            manual,
            auto_resend: self.automation.is_on(AutomationFlag::ResendPending),
            hold_leadership: !self.automation.is_on(AutomationFlag::DeliverHeartbeats),
        });
    }

    fn log_world(&mut self) -> Result<&mut world::World, ActionError> {
        self.world.log_mut().ok_or_else(|| {
            ActionError::new(
                ActionErrorCode::WrongWorld,
                "that move belongs to the replicated-log world",
            )
        })
    }

    fn decree_world(&mut self) -> Result<&mut world::decree::DecreeWorld, ActionError> {
        self.world.decree_mut().ok_or_else(|| {
            ActionError::new(
                ActionErrorCode::WrongWorld,
                "that move belongs to the single-decree world",
            )
        })
    }
}

fn initial_automation(level: &Level) -> Automation {
    let mut on: std::collections::BTreeSet<AutomationFlag> =
        level.automation_on.iter().copied().collect();
    for flag in level.pinned_off {
        on.remove(flag);
    }
    Automation {
        unlocked: level.unlocked.iter().copied().collect(),
        on,
        pinned_off: level.pinned_off.iter().copied().collect(),
    }
}

// ---- the wasm surface -------------------------------------------------------

#[cfg(target_arch = "wasm32")]
mod wasm {
    use wasm_bindgen::prelude::*;

    use crate::action::Action;
    use crate::view::ErrorView;

    /// The browser's whole handle on the engine.
    ///
    /// Every method returns JSON: a [`crate::GameView`], or an
    /// [`ErrorView`] the UI shows. Nothing panics on a player-reachable path —
    /// every action is validated before it reaches `paros-core`, whose own
    /// `assert!`s would abort the module — and
    /// `console_error_panic_hook` is installed for whatever is left.
    #[wasm_bindgen]
    pub struct WasmGame {
        inner: crate::Game,
    }

    fn error_json(code: &str, message: &str) -> String {
        serde_json::to_string(&ErrorView {
            code: code.to_string(),
            error: message.to_string(),
        })
        .unwrap_or_else(|_| {
            r#"{"code":"internal","error":"could not encode the error"}"#.to_string()
        })
    }

    fn json<T: serde::Serialize>(value: &T) -> String {
        serde_json::to_string(value).unwrap_or_else(|err| {
            error_json("internal", &format!("could not encode the view: {err}"))
        })
    }

    #[wasm_bindgen]
    impl WasmGame {
        /// Start a level by id.
        ///
        /// # Errors
        ///
        /// A JS exception carrying the message when the id is unknown.
        #[wasm_bindgen(constructor)]
        pub fn new(level_id: &str) -> Result<WasmGame, JsError> {
            console_error_panic_hook::set_once();
            let inner = crate::Game::new(level_id).map_err(|err| JsError::new(&err.message))?;
            Ok(Self { inner })
        }

        /// Every registered level, as JSON.
        #[must_use]
        pub fn levels() -> String {
            json(&crate::Game::levels())
        }

        /// Play one move, given an [`Action`] as JSON. Returns the new view, or
        /// an error object.
        #[must_use]
        pub fn act(&mut self, action_json: &str) -> String {
            let action: Action = match serde_json::from_str(action_json) {
                Ok(action) => action,
                Err(err) => {
                    return error_json("bad_action", &format!("could not read the action: {err}"));
                }
            };
            match self.inner.act(action) {
                Ok(()) => json(&self.inner.view()),
                Err(err) => error_json(&json_code(&err), &err.message),
            }
        }

        /// Undo the last move. Returns the new view.
        #[must_use]
        pub fn undo(&mut self) -> String {
            self.inner.undo();
            json(&self.inner.view())
        }

        /// Start the level again. Returns the new view.
        #[must_use]
        pub fn reset(&mut self) -> String {
            self.inner.reset();
            json(&self.inner.view())
        }

        /// The current view.
        #[must_use]
        pub fn view(&self) -> String {
            json(&self.inner.view())
        }
    }

    fn json_code(err: &crate::ActionError) -> String {
        serde_json::to_string(&err.code)
            .unwrap_or_else(|_| "\"internal\"".to_string())
            .trim_matches('"')
            .to_string()
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::WasmGame;
