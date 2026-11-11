//! The replicated-log setups: three or five `ColocatedNode`s over the log
//! world ([`crate::paxos::world::World`]), one client, disks that survive a
//! crash, and a clock the script ticks.

use paros_core::membership::QuorumSystem;

use crate::paxos::auto::AutomationFlag;
use crate::paxos::level::Level;
use crate::paxos::level::common::{CLIENT, fresh};

/// The setups of this file.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![&STEADY_STATE, &THE_PERMANENT_GAP, &THE_READ_THAT_LIES]
}

/// Every role answered by the engine. A setup removes the one its runs
/// answer by a prompt.
const ALL_ROLES_AUTOMATIC: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::QuorumReadServe,
];

// ---- steady state ------------------------------------------------------------

/// `act2/steady-state`.
pub static STEADY_STATE: Level = Level {
    id: "act2/steady-state",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[AutomationFlag::DeliverHeartbeats],
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
};

// ---- the permanent gap -------------------------------------------------------

/// `act2/the-permanent-gap`.
pub static THE_PERMANENT_GAP: Level = Level {
    id: "act2/the-permanent-gap",
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ProposerP2c,
        AutomationFlag::ReplicaApply,
        AutomationFlag::PersistOrder,
        AutomationFlag::QuorumReadServe,
    ],
    pinned_off: &[AutomationFlag::LeaderRecovery],
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
};

// ---- the read that lies ------------------------------------------------------

/// `act2/the-read-that-lies`.
pub static THE_READ_THAT_LIES: Level = Level {
    id: "act2/the-read-that-lies",
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ProposerP2c,
        AutomationFlag::ReplicaApply,
        AutomationFlag::LeaderRecovery,
        AutomationFlag::PersistOrder,
    ],
    pinned_off: &[AutomationFlag::QuorumReadServe],
    setup: || fresh(5, QuorumSystem::Majority, &[CLIENT]),
};
