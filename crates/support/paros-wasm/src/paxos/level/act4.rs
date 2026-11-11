//! The setups that change who counts: a flexible split, a grid, a
//! cooperative handoff, and the matchmaker plane (a reconfiguration, the
//! garbage collection of old configurations, a matchmaker-set handover).

use paros_core::membership::{MatchmakerId, QuorumSystem};
use paros_core::{Config, NodeId};

use crate::paxos::auto::AutomationFlag;
use crate::paxos::level::common::{CLIENT, TIMEOUT, fresh};
use crate::paxos::level::{Level, WorldKind};
use crate::paxos::world::decree::DecreeWorld;
use crate::paxos::world::matchmakers::MatchmakerProcess;
use crate::paxos::world::{Disk, World};

/// The setups of this file.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![
        &FLEXIBLE_QUORUMS,
        &THE_GRID,
        &THE_HANDOFF,
        // The matchmaker plane.
        &RECONFIGURE,
        &GARBAGE_COLLECTION,
        &MATCHMAKER_GENERATIONS,
    ]
}

/// How many beats a matchmaker-set handover may make no progress for before
/// the node driving it gives it up, on the level that teaches the handover.
///
/// Its floor is structural: a phase must get more beats than one round trip
/// needs, or a handover that is merely slow is given up every time. Four is
/// three beats above the one beat a freeze or a bootstrap answer takes here.
const HANDOVER_STALL: u64 = 4;

/// The four acceptors of the flexible-quorum level, and the two proposers that
/// compete over them.
const FOUR_ACCEPTORS: &[u64] = &[1, 2, 3, 4];

/// The acceptor grid every grid level lays out: two rows of three.
const GRID: QuorumSystem = QuorumSystem::grid(2, 3);

/// Every prompt-governing flag. A level pins off the ones it teaches
/// (`pinned_off`), which keeps them off whatever this list says.
const ALL_ROLES_AUTOMATIC: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::TrimPoint,
    AutomationFlag::AckWrite,
    AutomationFlag::GridColumn,
    AutomationFlag::QuorumReadServe,
    AutomationFlag::RepairVerdict,
    AutomationFlag::WipedRejoin,
    AutomationFlag::Phase1Complete,
    AutomationFlag::StaleConfiguration,
    AutomationFlag::GenerationFence,
    AutomationFlag::MayRetire,
];

// ---- worlds -----------------------------------------------------------------

/// A cluster that **names matchmakers**: `pool` is every node that may ever be
/// an acceptor, `bootstrap` the acceptor set in force before any ballot was
/// registered, `matchmakers` the registry tier of generation 0, and `spares`
/// the matchmakers a generation handover may pull in.
///
/// A node outside `bootstrap` is a spare acceptor: it is addressable, it
/// answers Phase 1 for the ballots it takes part in, and it becomes an acceptor
/// only when a reconfiguration names it.
///
/// `stall` is how many beats a matchmaker-set handover may make no progress
/// for before the node driving it gives up. It is **driver policy**, never a
/// constant inside the state machine, so every level names its own; `0`
/// disables it, which is what a level that never wants a handover abandoned
/// asks for.
fn deployed(
    pool: &[u64],
    bootstrap: &[u64],
    matchmakers: &[u64],
    spares: &[u64],
    stall: u64,
) -> WorldKind {
    let nodes: Vec<NodeId> = pool.iter().copied().map(NodeId).collect();
    let members: Vec<NodeId> = bootstrap.iter().copied().map(NodeId).collect();
    let set: Vec<MatchmakerId> = matchmakers.iter().copied().map(MatchmakerId).collect();
    let mut all = set.clone();
    all.extend(spares.iter().copied().map(MatchmakerId));
    all.sort_unstable();
    let disks: Vec<Disk> = nodes
        .iter()
        .map(|id| {
            Disk::new(Config {
                peers: members.clone(),
                quorum_system: QuorumSystem::Majority,
                nodes: nodes.clone(),
                matchmakers: set.clone(),
                matchmaker_pool: all.clone(),
                proxy_count: 0,
                ..Config::new(*id, paros_core::journal::JournalIdentifier::UNSET)
            })
        })
        .collect();
    let processes = all
        .iter()
        .map(|id| MatchmakerProcess::new(*id, set.clone()))
        .collect();
    WorldKind::Log(Box::new(
        World::from_disks(disks, &[CLIENT], TIMEOUT)
            .with_matchmakers(processes)
            .with_reconfigure_timeout(stall),
    ))
}

// ---- flexible quorums --------------------------------------------------------

/// `act4/flexible-quorums`.
pub static FLEXIBLE_QUORUMS: Level = Level {
    id: "act4/flexible-quorums",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    setup: || {
        WorldKind::Decree(Box::new(DecreeWorld::with_system(
            FOUR_ACCEPTORS,
            &[5, 8],
            &std::collections::BTreeMap::new(),
            None,
            QuorumSystem::Flexible { q1: 3, q2: 2 },
        )))
    },
};

// ---- the grid ----------------------------------------------------------------

/// `act4/the-grid`.
pub static THE_GRID: Level = Level {
    id: "act4/the-grid",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[AutomationFlag::GridColumn],
    setup: || fresh(6, GRID, &[CLIENT]),
};

// ---- the handoff -------------------------------------------------------------

/// `act4/the-handoff`.
pub static THE_HANDOFF: Level = Level {
    id: "act4/the-handoff",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    setup: || fresh(3, QuorumSystem::Majority, &[CLIENT]),
};

// ---- reconfigure -------------------------------------------------------------

/// `act4/reconfigure`.
pub static RECONFIGURE: Level = Level {
    id: "act4/reconfigure",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[
        AutomationFlag::Phase1Complete,
        AutomationFlag::StaleConfiguration,
    ],
    setup: || deployed(&[0, 1, 2, 3], &[0, 1, 2], &[0, 1], &[], 0),
};

// ---- garbage collection ------------------------------------------------------

/// `act4/garbage-collection`.
pub static GARBAGE_COLLECTION: Level = Level {
    id: "act4/garbage-collection",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[AutomationFlag::MayRetire],
    setup: || deployed(&[0, 1, 2, 3], &[0, 1, 2, 3], &[0, 1], &[], 0),
};

// ---- matchmaker generations --------------------------------------------------

/// `act4/matchmaker-generations`.
pub static MATCHMAKER_GENERATIONS: Level = Level {
    id: "act4/matchmaker-generations",
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[AutomationFlag::GenerationFence],
    setup: || deployed(&[0, 1, 2], &[0, 1, 2], &[0, 1, 2], &[3], HANDOVER_STALL),
};
