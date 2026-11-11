//! Levels: the setups the traces of the site's `/paxos/` pages run on.
//!
//! A [`Level`] is a world to build. It was a level of the Paxos game (#966
//! (delete the game)); the briefing, the goal, the hint, the reference
//! solution and the automation flags are gone with the game, and so is every
//! level no trace runs on.
//!
//! Level ids are stable strings (`act1/the-duel`), never indices.

pub mod act1;
pub mod act2;
pub mod act3;
pub mod act4;
pub(crate) mod common;

use crate::paxos::action::ActionError;
use crate::paxos::narration::NarrationEvent;
use crate::paxos::view::WorldView;
use crate::paxos::world::decree::DecreeWorld;
use crate::paxos::world::{InFlight, World};

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

/// One setup a trace runs on.
pub struct Level {
    /// The stable id, e.g. `act1/the-duel`.
    pub id: &'static str,
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
