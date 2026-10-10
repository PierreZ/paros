//! The wire contract and the bridge between moonpool-rpc and the
//! single-owner drivers.
//!
//! Every method paros speaks is a **well-known** moonpool-rpc endpoint
//! ([`methods`]): the deployment map names processes by `ip:port`, never by
//! a published reference, so a caller reaches whichever incarnation is
//! serving that address — exactly what a node's peers, its clients and its
//! matchmakers expect of a restart. The bodies are the protobuf messages
//! generated from `proto/*.proto`.

/// Types both wire contracts speak, generated from `proto/common.proto`: a
/// ballot and an acceptor configuration mean the same thing on the consensus
/// wire and on the matchmaker wire, so they are declared once (see the proto
/// for why that changes no bytes).
pub mod common {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.common.v1.rs"));
}

/// Client-facing journal contract generated from `proto/paros.proto`.
pub mod public {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.v1.rs"));
}

/// Cluster-internal consensus contract generated from `proto/internal.proto`.
pub mod internal {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.internal.v1.rs"));
}

/// The matchmaker contract generated from `proto/matchmaker.proto`: a per-ballot
/// configuration registry, spoken only by a deployment that names matchmakers.
pub mod matchmaker {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.matchmaker.v1.rs"));
}

/// The cell control journal's entries (#189), generated from
/// `proto/system.proto`: one record per slot of the node registry (the cell
/// tenant's control journal, #235), read by `paros::system`.
pub mod system {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.system.v1.rs"));
}

/// The fleet tenant's entries (#229), generated from `proto/fleet.proto`.
pub mod fleet {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.fleet.v1.rs"));
}

/// The machine contract (#196, #216), generated from `proto/machine.proto`:
/// what an uninitialized `parosd` serves while it waits for `init`.
pub mod machine {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.machine.v1.rs"));
}

/// The checkpoint record (#230), generated from `proto/checkpoint.proto`:
/// read and written by `paros::client::checkpoint`.
pub mod checkpoint {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.checkpoint.v1.rs"));
}

/// The election record (#240), generated from `proto/election.proto`: read
/// and written by `paros::client::election`.
pub mod election {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.election.v1.rs"));
}

/// A tenant's control journal entries (#210), generated from
/// `proto/tenant.proto`: read by `paros::tenant`.
pub mod tenant {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.tenant.v1.rs"));
}

/// The administrative views (#399), generated from `proto/view.proto`.
pub mod view {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.view.v1.rs"));
}

mod client;
mod codec;
mod consensus;
mod inbound;
mod inspect;
mod matchmaker_codec;
pub mod methods;
mod refusal;
#[cfg(test)]
mod tests;

pub use client::NodeClient;
pub(crate) use client::{MatchmakerClient, well_known};
pub use inbound::{EdgeRejection, MAX_FRAME_BYTES};
pub(crate) use inbound::{
    Inbound, OnReject, ReplySender, rpc_config, serve_deliveries, serve_well_known,
};
pub use inspect::{InspectRefusal, InspectTarget};
pub use internal::{InspectReply, InspectRequest, RetireAck, RetireRequest};
pub(crate) use matchmaker::{
    GarbageCollect as WireGarbageCollect, GarbageCollectAck as WireGarbageCollectAck,
    MatchReply as WireMatchReply, MatchRequest as WireMatchRequest,
    ReconfigureReply as WireReconfigureReply, ReconfigureRequest as WireReconfigureRequest,
};
pub use public::{
    Read, ReadAck, Reconfigure, ReconfigureAck, ReconfigureMatchmakers, ReconfigureMatchmakersAck,
    SetLeader, SetLeaderAck, Truncate, TruncateAck, Write, WriteAck, WriteOutcome,
};
pub use refusal::{MatchmakersRefusal, RetireRefusal};

pub use codec::{
    WireQuorumSystem, journal_state_from_proto, journal_state_to_proto, journal_view_from_proto,
    journal_view_to_proto, leader_from_proto, leader_uuid_from_proto, leader_uuid_to_proto,
    quorum_system_from_proto, quorum_system_to_proto, writer_mode_from_proto, writer_mode_to_proto,
};
pub(crate) use codec::{config_from_proto, config_to_proto};
pub(crate) use consensus::{message_from_proto, message_to_proto};
pub(crate) use matchmaker_codec::{
    garbage_collect_ack_from_wire, garbage_collect_from_wire, match_reply_from_wire,
    match_request_from_wire, reconfigure_reply_from_wire, reconfigure_request_from_wire,
    wire_garbage_collect, wire_garbage_collect_ack, wire_match_reply, wire_match_request,
    wire_reconfigure_reply, wire_reconfigure_request,
};
