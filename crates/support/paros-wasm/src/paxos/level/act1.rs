//! The single-decree setup: three acceptors, two proposers, one slot, over
//! the [`crate::paxos::world::decree::DecreeWorld`]. No log, no leader, no
//! clock, no disk.

use crate::paxos::level::{Level, WorldKind};
use crate::paxos::world::decree::DecreeWorld;

/// The setups of this file.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![&THE_DUEL]
}

/// The three acceptors of the single-decree setup.
const ACCEPTORS: &[u64] = &[1, 2, 3];

// ---- the duel ----------------------------------------------------------------

/// `act1/the-duel`.
pub static THE_DUEL: Level = Level {
    id: "act1/the-duel",
    setup: || WorldKind::Decree(Box::new(DecreeWorld::new(ACCEPTORS, &[5, 8]))),
};
