//! Levels: the setups the traces of the site's `/paxos/` pages run on.
//!
//! A [`Level`] is a world to build and the automation flags that start on
//! there. It was a level of the Paxos game (#966 (delete the game)); the
//! briefing, the goal, the hint and the reference solution are gone with the
//! game, and so is every level no trace runs on.
//!
//! Level ids are stable strings (`act1/the-duel`), never indices.

pub mod act1;
pub mod act2;
pub mod act3;
pub mod act4;
pub(crate) mod common;

use crate::paxos::action::ActionError;
use crate::paxos::auto::AutomationFlag;
use crate::paxos::narration::NarrationEvent;
use crate::paxos::prompt::{Prompt, Verdict};
use crate::paxos::view::WorldView;
use crate::paxos::world::decree::DecreeWorld;
use crate::paxos::world::{InFlight, World, WorldPolicy};

/// `$body` on whichever world `$kind` holds, bound as `$world`.
macro_rules! either {
    ($kind:expr, $world:ident => $body:expr) => {
        match $kind {
            WorldKind::Decree($world) => $body,
            WorldKind::Log($world) => $body,
        }
    };
}

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
        either!(self, world => world.prompt())
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
        either!(self, world => world.view())
    }

    /// Install the policy the automation flags imply.
    pub fn set_policy(&mut self, policy: WorldPolicy) {
        either!(self, world => world.set_policy(policy));
    }

    /// Start a fresh action's narration.
    pub fn clear_narration(&mut self) {
        either!(self, world => world.clear_narration());
    }

    /// Take the narration the action just produced.
    pub fn take_narration(&mut self) -> Vec<NarrationEvent> {
        either!(self, world => world.take_narration())
    }

    /// Deliver the in-flight message `id`.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`crate::paxos::action::ActionErrorCode`].
    pub fn deliver(&mut self, id: u64) -> Result<(), ActionError> {
        either!(self, world => world.deliver(id))
    }

    /// Drop the in-flight message `id`.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`crate::paxos::action::ActionErrorCode`].
    pub fn drop_message(&mut self, id: u64) -> Result<(), ActionError> {
        either!(self, world => world.drop_message(id))
    }

    /// Put a second copy of the in-flight message `id` on the wire.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`crate::paxos::action::ActionErrorCode`].
    pub fn duplicate(&mut self, id: u64, to: Option<u64>) -> Result<(), ActionError> {
        either!(self, world => world.duplicate(id, to))
    }

    /// Answer the open prompt.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`crate::paxos::action::ActionErrorCode`].
    pub fn answer(&mut self, prompt: u64, choice: &str) -> Result<Verdict, ActionError> {
        either!(self, world => world.answer(prompt, choice))
    }

    /// The next message an automation pump would deliver.
    #[must_use]
    pub fn next_auto_delivery(&self, beats: bool, replies: bool, matchmaker: bool) -> Option<u64> {
        match self {
            WorldKind::Decree(world) => world.next_auto_delivery(beats, replies),
            WorldKind::Log(world) => world.next_auto_delivery(beats, replies, matchmaker),
        }
    }

    /// Everything in flight, as the world holds it.
    #[must_use]
    pub fn wire(&self) -> &[InFlight] {
        either!(self, world => world.wire())
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

/// One setup a trace runs on: a world, and which decisions the engine
/// makes on its own there.
pub struct Level {
    /// The stable id, e.g. `act1/the-duel`.
    pub id: &'static str,
    /// The automation flags that start on.
    pub automation_on: &'static [AutomationFlag],
    /// The automation flags that stay off, whatever `automation_on` says.
    pub pinned_off: &'static [AutomationFlag],
    /// Build the world.
    pub setup: fn() -> WorldKind,
}

/// Every registered level.
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
