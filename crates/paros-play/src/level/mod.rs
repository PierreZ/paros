//! Levels: what the player is asked to do, and how the engine knows they did.
//!
//! A [`Level`] is data plus four function pointers — `setup`, `goal`, `hint`
//! and `reference` — and nothing else. It declares which world it runs in,
//! which automation flags start on, which it forbids, which actions it offers,
//! the briefing that replaces the book's prose for this mechanism, and the
//! reference solution the tests replay.
//!
//! Level ids are stable strings (`act1/choose-a-value`), never indices: the
//! frontend's progress store keys on them and a reordering must not lose
//! anybody's progress.

pub mod act1;
pub mod act2;
pub mod act3;
pub mod act4;
mod script;

use crate::action::{Action, ActionError, ActionKind};
use crate::auto::AutomationFlag;
use crate::narration::NarrationEvent;
use crate::prompt::{Prompt, Verdict};
use crate::view::{GoalView, LevelSummary, WorldView};
use crate::world::decree::DecreeWorld;
use crate::world::{World, WorldPolicy};

/// Which world a level runs in.
pub enum WorldKind {
    /// Act I: the bare roles over slot 0.
    Decree(Box<DecreeWorld>),
    /// Act II onward: `ColocatedNode`s, disks and a clock.
    Log(Box<World>),
}

impl WorldKind {
    /// The open prompt, if any.
    #[must_use]
    pub fn prompt(&self) -> Option<&Prompt> {
        match self {
            WorldKind::Decree(world) => world.prompt(),
            WorldKind::Log(world) => world.prompt(),
        }
    }

    /// A safety violation the world has been asked to enact — today only the
    /// single-decree world's "two values for one slot" (see
    /// [`DecreeWorld::violation`]). `None` is the invariant holding.
    #[must_use]
    pub fn violation(&self) -> Option<String> {
        match self {
            WorldKind::Decree(world) => world.violation().map(str::to_string),
            WorldKind::Log(_) => None,
        }
    }

    /// Render the world.
    #[must_use]
    pub fn view(&self) -> WorldView {
        match self {
            WorldKind::Decree(world) => world.view(),
            WorldKind::Log(world) => world.view(),
        }
    }

    /// Install the policy the automation flags imply.
    pub fn set_policy(&mut self, policy: WorldPolicy) {
        match self {
            WorldKind::Decree(world) => world.set_policy(policy),
            WorldKind::Log(world) => world.set_policy(policy),
        }
    }

    /// Start a fresh action's narration.
    pub fn clear_narration(&mut self) {
        match self {
            WorldKind::Decree(world) => world.clear_narration(),
            WorldKind::Log(world) => world.clear_narration(),
        }
    }

    /// Take the narration the action just produced.
    pub fn take_narration(&mut self) -> Vec<NarrationEvent> {
        match self {
            WorldKind::Decree(world) => world.take_narration(),
            WorldKind::Log(world) => world.take_narration(),
        }
    }

    /// Deliver the in-flight message `id`.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`crate::action::ActionErrorCode`].
    pub fn deliver(&mut self, id: u64) -> Result<(), ActionError> {
        match self {
            WorldKind::Decree(world) => world.deliver(id),
            WorldKind::Log(world) => world.deliver(id),
        }
    }

    /// Drop the in-flight message `id`.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`crate::action::ActionErrorCode`].
    pub fn drop_message(&mut self, id: u64) -> Result<(), ActionError> {
        match self {
            WorldKind::Decree(world) => world.drop_message(id),
            WorldKind::Log(world) => world.drop_message(id),
        }
    }

    /// Put a second copy of the in-flight message `id` on the wire.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`crate::action::ActionErrorCode`].
    pub fn duplicate(&mut self, id: u64, to: Option<u64>) -> Result<(), ActionError> {
        match self {
            WorldKind::Decree(world) => world.duplicate(id, to),
            WorldKind::Log(world) => world.duplicate(id, to),
        }
    }

    /// Answer the open prompt.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`crate::action::ActionErrorCode`].
    pub fn answer(&mut self, prompt: u64, choice: &str) -> Result<Verdict, ActionError> {
        match self {
            WorldKind::Decree(world) => world.answer(prompt, choice),
            WorldKind::Log(world) => world.answer(prompt, choice),
        }
    }

    /// The next message an automation pump would deliver.
    #[must_use]
    pub fn next_auto_delivery(&self, beats: bool, replies: bool, matchmaker: bool) -> Option<u64> {
        match self {
            WorldKind::Decree(world) => world.next_auto_delivery(beats, replies),
            WorldKind::Log(world) => world.next_auto_delivery(beats, replies, matchmaker),
        }
    }

    /// The log world, if this is one.
    #[must_use]
    pub fn log(&self) -> Option<&World> {
        match self {
            WorldKind::Log(world) => Some(world),
            WorldKind::Decree(_) => None,
        }
    }

    /// The log world, mutably.
    pub fn log_mut(&mut self) -> Option<&mut World> {
        match self {
            WorldKind::Log(world) => Some(world),
            WorldKind::Decree(_) => None,
        }
    }

    /// The single-decree world, if this is one.
    #[must_use]
    pub fn decree(&self) -> Option<&DecreeWorld> {
        match self {
            WorldKind::Decree(world) => Some(world),
            WorldKind::Log(_) => None,
        }
    }

    /// The single-decree world, mutably.
    pub fn decree_mut(&mut self) -> Option<&mut DecreeWorld> {
        match self {
            WorldKind::Decree(world) => Some(world),
            WorldKind::Log(_) => None,
        }
    }
}

/// Whether the level's goal is reached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoalStatus {
    /// Not yet; the string says what the player is working toward.
    Open(String),
    /// Done; the string says what was proved.
    Reached(String),
    /// Unreachable from here; the string says what went wrong.
    Failed(String),
}

impl GoalStatus {
    /// Whether the goal is reached.
    #[must_use]
    pub fn is_reached(&self) -> bool {
        matches!(self, GoalStatus::Reached(_))
    }

    /// Render it.
    #[must_use]
    pub fn view(&self) -> GoalView {
        match self {
            GoalStatus::Open(detail) => GoalView::Open {
                detail: detail.clone(),
            },
            GoalStatus::Reached(detail) => GoalView::Reached {
                detail: detail.clone(),
            },
            GoalStatus::Failed(detail) => GoalView::Failed {
                detail: detail.clone(),
            },
        }
    }
}

/// One level.
pub struct Level {
    /// The stable id, e.g. `act1/choose-a-value`.
    pub id: &'static str,
    /// Which act it belongs to.
    pub act: u8,
    /// Its title.
    pub title: &'static str,
    /// The briefing, in markdown. This is the mechanism prose the book no
    /// longer carries.
    pub briefing: &'static str,
    /// A link into the book's field guide.
    pub field_guide: &'static str,
    /// The `paros-core` symbols this level names.
    pub symbols: &'static [&'static str],
    /// The automation flags that start on.
    pub automation_on: &'static [AutomationFlag],
    /// The automation flags this level forbids turning on.
    pub pinned_off: &'static [AutomationFlag],
    /// The automation flags this level offers as toggles.
    pub unlocked: &'static [AutomationFlag],
    /// The automation flags passing this level unlocks for later ones.
    pub unlocks: &'static [AutomationFlag],
    /// The actions this level offers.
    pub allowed_actions: &'static [ActionKind],
    /// Build the world.
    pub setup: fn() -> WorldKind,
    /// Judge it.
    pub goal: fn(&WorldKind) -> GoalStatus,
    /// A nudge, once the player has earned one.
    pub hint: fn(&WorldKind, u32) -> Option<String>,
    /// The reference solution the tests replay.
    pub reference: fn() -> Vec<Action>,
}

impl Level {
    /// Whether this level offers `kind`.
    #[must_use]
    pub fn allows(&self, kind: ActionKind) -> bool {
        self.allowed_actions.contains(&kind)
    }

    /// Render it for the level map.
    #[must_use]
    pub fn summary(&self) -> LevelSummary {
        LevelSummary {
            id: self.id.to_string(),
            act: self.act,
            title: self.title.to_string(),
            unlocks: self.unlocks.to_vec(),
        }
    }
}

/// No hint at all.
#[must_use]
pub fn no_hint(_world: &WorldKind, _mistakes: u32) -> Option<String> {
    None
}

/// Every registered level, in play order.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    let mut all = act1::levels();
    all.extend(act2::levels());
    all.extend(act3::levels());
    all.extend(act4::levels());
    all
}

/// The level with `id`, if there is one.
#[must_use]
pub fn level(id: &str) -> Option<&'static Level> {
    levels().into_iter().find(|level| level.id == id)
}
