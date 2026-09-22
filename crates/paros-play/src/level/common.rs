//! What every act's levels share: the client and the clock they start with,
//! the worlds they build, what a goal reads off them, and the action
//! shorthands a reference is written in.

use paros_core::{Command, Config, NodeId, QuorumSystem, Slot};

use crate::action::Action;
use crate::auto::AutomationFlag;
use crate::level::WorldKind;
use crate::view::{MessageView, show_command};
use crate::world::{Disk, World};

/// The client every log-world level gives the player.
pub(super) const CLIENT: u64 = 7;

/// The election timeout every log-world node starts with, in ticks. Long
/// enough that a level's own ticks are deliberate, short enough that
/// [`Action::StartElection`] is not the only way to campaign.
pub(super) const TIMEOUT: u64 = 5;

/// The one convenience toggle every level offers: deliver the replies for me.
pub(super) const REPLIES_ONLY: &[AutomationFlag] = &[AutomationFlag::DeliverReplies];

/// The convenience toggles the later log-world levels offer.
pub(super) const REPLIES_AND_BEATS: &[AutomationFlag] = &[
    AutomationFlag::DeliverReplies,
    AutomationFlag::DeliverHeartbeats,
];

/// `all` without `taught`, in `all`'s order: every role answered for the
/// player but the ones a level teaches. `M` is the length that leaves, so a
/// `taught` flag missing from `all` is a compile error, not a silent no-op.
pub(super) const fn all_but<const M: usize>(
    all: &[AutomationFlag],
    taught: &[AutomationFlag],
) -> [AutomationFlag; M] {
    let mut out = [AutomationFlag::AcceptorReplies; M];
    let mut kept = 0;
    let mut i = 0;
    while i < all.len() {
        let mut j = 0;
        let mut dropped = false;
        while j < taught.len() {
            dropped |= all[i] as u8 == taught[j] as u8;
            j += 1;
        }
        if !dropped {
            out[kept] = all[i];
            kept += 1;
        }
        i += 1;
    }
    assert!(kept == M, "every taught flag is one of the roles");
    out
}

// ---- worlds -----------------------------------------------------------------

/// The ids of a `size`-node cluster.
pub(super) fn peers(size: u64) -> Vec<NodeId> {
    (0..size).map(NodeId).collect()
}

/// The configuration of node `id` in a `size`-node cluster under `system`.
pub(super) fn config(id: NodeId, size: u64, system: QuorumSystem) -> Config {
    Config {
        id,
        peers: peers(size),
        quorum_system: system,
        ..Config::default()
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

/// What a node's application has executed, in order.
pub(super) fn applied(world: &WorldKind, node: u64) -> Vec<String> {
    world
        .log()
        .and_then(|world| world.disk(NodeId(node)))
        .map(|disk| {
            disk.applied()
                .iter()
                .map(|(_, command)| show_command(command))
                .collect()
        })
        .unwrap_or_default()
}

/// A watermark as the goals write it.
pub(super) fn at(slot: Option<Slot>) -> String {
    slot.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0))
}

/// The text inside a client command, for a goal that has to name a value.
pub(super) fn text(command: &Command) -> String {
    command
        .user()
        .map(|entry| String::from_utf8_lossy(&entry.value.0).into_owned())
        .unwrap_or_default()
}

/// The value the single-decree world holds, if it holds one.
pub(super) fn chosen_text(world: &WorldKind) -> Option<String> {
    world
        .decree()
        .and_then(|decree| decree.chosen().map(|(_, command)| text(command)))
}

// ---- action shorthands ------------------------------------------------------

pub(super) fn open(proposer: u64, value: &str) -> Action {
    Action::OpenBallot {
        proposer,
        value: value.to_string(),
    }
}

pub(super) fn start_election(node: u64) -> Action {
    Action::StartElection { node }
}

/// [`CLIENT`] proposes `value` to `node`.
pub(super) fn propose(node: u64, value: &str) -> Action {
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

/// `client` asks `node` for a read-index read.
pub(super) fn read_index_as(client: u64, node: u64) -> Action {
    Action::ReadIndex {
        node,
        client: Some(client),
    }
}

pub(super) fn crash(node: u64) -> Action {
    Action::Crash { node }
}

pub(super) fn restart(node: u64) -> Action {
    Action::Restart { node }
}

pub(super) fn tick(node: u64) -> Action {
    Action::Tick { node }
}

/// The Phase-2 traffic of one slot: its `Accept`s, its `Accepted`s and the
/// `Commit`s that report the decision. Deliberately not "everything naming this
/// slot": a `Heartbeat`'s slot is its commit watermark, not a proposal.
pub(super) fn slot_traffic(slot: u64) -> impl Fn(&MessageView) -> bool {
    move |message| is_phase2(message) && message.slot == Some(slot)
}

/// Whether a message is Phase-2 traffic: an `Accept`, an `Accepted` or a
/// `Commit`.
pub(super) fn is_phase2(message: &MessageView) -> bool {
    matches!(message.kind.as_str(), "Accept" | "Accepted" | "Commit")
}
