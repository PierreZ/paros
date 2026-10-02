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

/// The system journals' entries (#189), generated from
/// `proto/system.proto`: one record per slot of the directory or
/// the node registry (two tenants' control journals, #235), read by `paros::system`.
pub mod system {
    #![allow(missing_docs, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/paros.system.v1.rs"));
}

mod client;
mod codec;
mod consensus;
mod inbound;
mod matchmaker_codec;
pub mod methods;
#[cfg(test)]
mod tests;

pub use client::NodeClient;
pub(crate) use client::{MatchmakerClient, well_known};
pub use inbound::{EdgeRejection, MAX_FRAME_BYTES};
pub(crate) use inbound::{
    Inbound, OnReject, ReplySender, rpc_config, serve_deliveries, serve_well_known,
};
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

pub use codec::{
    WireQuorumSystem, journal_state_from_proto, journal_state_to_proto, quorum_system_from_proto,
    quorum_system_to_proto,
};
pub(crate) use codec::{config_from_proto, config_to_proto};
pub(crate) use consensus::{message_from_proto, message_to_proto};
pub(crate) use matchmaker_codec::{
    garbage_collect_ack_from_wire, garbage_collect_from_wire, match_reply_from_wire,
    match_request_from_wire, reconfigure_reply_from_wire, reconfigure_request_from_wire,
    wire_garbage_collect, wire_garbage_collect_ack, wire_match_reply, wire_match_request,
    wire_reconfigure_reply, wire_reconfigure_request,
};
