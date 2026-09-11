//! The **matchmaker plane**: the registry tier, the candidate's matchmaking
//! phase, the garbage-collection floor, and the generation handover.
//!
//! This is `crates/paros-core/examples/matchmaker.rs` with the parts the
//! example hard-codes turned into player choices. The example's
//! `MatchmakerNode` — a [`Matchmaker`](paros_core::Matchmaker) role, the
//! static [`MatchmakerConfig`](paros_core::MatchmakerConfig) it boots with, and
//! a [`MemRegistry`](paros_core::MemRegistry) disk it writes to
//! and reboots from — is [`MatchmakerProcess`] here, and its `deliver_match` /
//! `deliver_reconfigure` are the two methods below: **step, persist, take the
//! reply, advance**, in that order and never another.
//!
//! # Where each piece lives
//!
//! - A **matchmaker** is not a node. It holds a registry and no log, it votes
//!   on no slot, and it has its own identity space
//!   ([`MatchmakerId`]), which is why the wire addresses a
//!   [`Party`](crate::world::Party) rather than a node id and why crashing one is its
//!   own verb.
//! - The **matchmaking phase** belongs to the candidate. `ColocatedNode` opens
//!   it, queues the requests through `Ready::match_requests`, and folds each
//!   answer through `on_match_reply`; the world only carries the messages.
//! - The **garbage-collection floor** belongs to the leader in the same way
//!   (`Ready::gc_requests`, `on_gc_ack`).
//! - The **reconfigurer** is a *node-side driver object*, not a role of the
//!   core's node: the example holds one beside the pool, and
//!   `crates/paros/src/driver/handover.rs` holds one per driver. So the world
//!   holds one per node, and the node that drives a handover is the node the
//!   player asked.
//!
//! # What the world decides, and what it does not
//!
//! Two decisions here are the **driver's**, exactly as they are in
//! `paros::run_node`, and the game gives them to the player's clock rather than
//! to an ack:
//!
//! - **closing a freeze** ([`paros_core::MatchmakerReconfigurer::close_stop`])
//!   happens on a
//!   beat, not on the ack that first completed the quorum, so a straggler that
//!   arrives in between widens the reconstruction;
//! - **abandoning a stalled handover** happens after
//!   the handover stall timeout beats without progress.
//!
//! Everything else is the core's own answer.
mod delivery;
mod process;
mod prompts;
mod render;
mod verbs;

use paros_core::{MatchmakerId, NodeId};

pub use process::{MatchmakerProcess, RegistryDisk};
pub(crate) use render::{plane_view, why};

/// How the narration names a matchmaker. Nodes are "node 1", matchmakers are
/// "matchmaker 1": two identity spaces, two words.
#[must_use]
pub(crate) fn which(id: MatchmakerId) -> String {
    format!("matchmaker {}", id.0)
}

/// An acceptor set, as every player-facing sentence names one.
#[must_use]
pub(crate) fn show_members(members: &[NodeId]) -> String {
    let ids: Vec<String> = members.iter().map(|id| id.0.to_string()).collect();
    format!("{{{}}}", ids.join(", "))
}

/// A matchmaker set, as every player-facing sentence names one.
#[must_use]
pub(crate) fn show_set(members: &[MatchmakerId]) -> String {
    let ids: Vec<String> = members.iter().map(|id| id.0.to_string()).collect();
    format!("{{{}}}", ids.join(", "))
}
