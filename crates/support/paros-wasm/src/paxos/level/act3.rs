//! The truncation setup: a log world whose log shrinks by a decided
//! `Truncate`.

use paros_core::membership::QuorumSystem;

use crate::paxos::auto::AutomationFlag;
use crate::paxos::level::Level;
use crate::paxos::level::common::{CLIENT, fresh};

/// The setups of this file.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![&TRUNCATE_BY_CONSENSUS]
}

/// Every role answered by the engine. A setup pins off the ones its runs answer by a prompt
/// (`pinned_off`), which keeps them off whatever this list says.
const ALL_ROLES_AUTOMATIC: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::QuorumReadServe,
    AutomationFlag::TrimPoint,
    AutomationFlag::AckWrite,
];

// ---- truncate by consensus ---------------------------------------------------

/// `act3/truncate-by-consensus`.
pub static TRUNCATE_BY_CONSENSUS: Level = Level {
    id: "act3/truncate-by-consensus",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
};
