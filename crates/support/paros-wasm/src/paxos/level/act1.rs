//! The single-decree setup: three acceptors, two proposers, one slot, over
//! the [`crate::paxos::world::decree::DecreeWorld`]. No log, no leader, no
//! clock, no disk.

use crate::paxos::auto::AutomationFlag;
use crate::paxos::level::{Level, WorldKind};
use crate::paxos::world::decree::DecreeWorld;

/// The setups of this file.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![&THE_DUEL]
}

/// The three acceptors of the single-decree setup.
const ACCEPTORS: &[u64] = &[1, 2, 3];

/// Every prompt-governing flag: what a level turns on when it wants the roles
/// answered by the engine.
const ALL_ROLES_AUTOMATIC: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::QuorumReadServe,
];

// ---- the duel ----------------------------------------------------------------

/// `act1/the-duel`.
pub static THE_DUEL: Level = Level {
    id: "act1/the-duel",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    setup: || WorldKind::Decree(Box::new(DecreeWorld::new(ACCEPTORS, &[5, 8]))),
};
