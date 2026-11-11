//! The truncation setup: a log world whose log shrinks by a decided
//! `Truncate`.

use paros_core::membership::QuorumSystem;

use crate::paxos::level::Level;
use crate::paxos::level::common::{CLIENT, fresh};

/// The setups of this file.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![&TRUNCATE_BY_CONSENSUS]
}

// ---- truncate by consensus ---------------------------------------------------

/// `act3/truncate-by-consensus`.
pub static TRUNCATE_BY_CONSENSUS: Level = Level {
    id: "act3/truncate-by-consensus",
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
};
