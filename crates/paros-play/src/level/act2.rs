//! Act II — a replicated log.
//!
//! The seven levels the plan names (`act2/persist-before-send` through
//! `act2/the-read-that-lies`) live here. They run in the **log world**
//! ([`crate::world::World`]): three `ColocatedNode`s, one client, disks that
//! survive a crash, and the clock the player ticks.
//!
//! The engine is finished for them: the world supports elections
//! ([`World::start_election`](crate::world::World::start_election)), proposals,
//! out-of-order delivery, crash and restart, the two durability seams
//! ([`crate::action::Seam`]), read-index rounds, and the five prompt kinds Act
//! II needs — `PersistOrder`, `ReplicaApply`, `LeaderRecovery`,
//! `CommitOverwrite` and `ReadServe`. `crates/paros-play/tests/world.rs` drives
//! each of those directly, so a level here is briefing, goal and reference
//! solution over an engine that already works.

use super::Level;

/// Act II's levels. Empty until they land.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    Vec::new()
}
