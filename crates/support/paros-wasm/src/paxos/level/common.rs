//! What the setups share: the client and the clock they start with, the
//! worlds they build, and the action shorthands a trace script is written in.

use paros_core::membership::QuorumSystem;
use paros_core::{Config, NodeId};

use crate::paxos::action::Action;
use crate::paxos::level::WorldKind;
use crate::paxos::world::{Disk, World};

/// The client every log-world level gives the player.
pub(super) const CLIENT: u64 = 7;

/// The election timeout every log-world node starts with, in ticks. Long
/// enough that a level's own ticks are deliberate, short enough that
/// [`Action::StartElection`] is not the only way to campaign.
pub(super) const TIMEOUT: u64 = 5;

// ---- worlds -----------------------------------------------------------------

/// The ids of a `size`-node cluster.
pub(super) fn peers(size: u64) -> Vec<NodeId> {
    (0..size).map(NodeId).collect()
}

/// The configuration of node `id` in a `size`-node cluster under `system`.
pub(super) fn config(id: NodeId, size: u64, system: QuorumSystem) -> Config {
    Config {
        peers: peers(size),
        quorum_system: system,
        ..Config::new(id, paros_core::journal::JournalIdentifier::UNSET)
    }
}

/// A cluster of `size` fresh nodes under `system`, with `clients` clients.
pub(super) fn fresh(size: u64, system: QuorumSystem, clients: &[u64]) -> WorldKind {
    let disks = peers(size)
        .into_iter()
        .map(|id| Disk::new(config(id, size, system)))
        .collect();
    WorldKind::Log(Box::new(World::from_disks(disks, clients, TIMEOUT)))
}

// ---- reading the world for a goal -------------------------------------------

// ---- action shorthands ------------------------------------------------------

pub(crate) fn open(proposer: u64, value: &str) -> Action {
    Action::OpenBallot {
        proposer,
        value: value.to_string(),
    }
}

pub(crate) fn start_election(node: u64) -> Action {
    Action::StartElection { node }
}

/// [`CLIENT`] proposes `value` to `node`.
pub(crate) fn propose(node: u64, value: &str) -> Action {
    propose_as(CLIENT, node, value)
}

/// `client` proposes `value` to `node`.
pub(super) fn propose_as(client: u64, node: u64, value: &str) -> Action {
    Action::Propose {
        node,
        client,
        value: value.to_string(),
        column: None,
    }
}

pub(crate) fn crash(node: u64) -> Action {
    Action::Crash { node }
}
