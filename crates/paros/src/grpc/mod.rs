//! Generated gRPC contract and the bridge into the single-owner node driver.

use tokio::sync::oneshot;

/// Types both wire contracts speak, generated from `proto/common.proto`: a
/// ballot and an acceptor configuration mean the same thing on the consensus
/// wire and on the matchmaker wire, so they are declared once (see the proto
/// for why that changes no bytes).
pub mod common {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.common.v1");
}

/// Client-facing journal contract generated from `proto/paros.proto`.
pub mod public {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.v1");
}

/// Cluster-internal consensus contract generated from `proto/internal.proto`.
pub(crate) mod internal {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.internal.v1");
}

/// The matchmaker contract generated from `proto/matchmaker.proto`: a per-ballot
/// configuration registry, spoken only by a deployment that names matchmakers.
pub mod matchmaker {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.matchmaker.v1");
}

mod codec;
mod consensus;
mod matchmaker_codec;
mod matchmaker_service;
mod service;
#[cfg(test)]
mod tests;

pub use internal::paros_internal_client::ParosInternalClient;
pub(crate) use internal::paros_internal_server::ParosInternalServer;
pub use internal::{InspectReply, InspectRequest, RetireAck, RetireRequest};
pub(crate) use matchmaker::paros_matchmaker_client::ParosMatchmakerClient;
pub(crate) use matchmaker::paros_matchmaker_server::ParosMatchmakerServer;
pub(crate) use matchmaker::{
    GarbageCollect as WireGarbageCollect, GarbageCollectAck as WireGarbageCollectAck,
    MatchReply as WireMatchReply, MatchRequest as WireMatchRequest,
    ReconfigureReply as WireReconfigureReply, ReconfigureRequest as WireReconfigureRequest,
};
pub use public::paros_client::ParosClient;
pub(crate) use public::paros_server::ParosServer;
pub use public::{
    Compact, CompactAck, Propose, ProposeAck, Read, ReadAck, Reconfigure, ReconfigureAck,
    ReconfigureMatchmakers, ReconfigureMatchmakersAck,
};

pub(crate) type ReplySender<T> = oneshot::Sender<T>;
type Call<T, U> = (T, ReplySender<U>);

pub use codec::{WireQuorumSystem, quorum_system_from_proto, quorum_system_to_proto};
pub(crate) use consensus::message_to_proto;
pub(crate) use matchmaker_codec::{
    garbage_collect_ack_from_wire, match_reply_from_wire, reconfigure_reply_from_wire,
    wire_garbage_collect, wire_match_request, wire_reconfigure_request,
};
pub(crate) use matchmaker_service::{MatchmakerInbox, matchmaker_channel};
pub use service::EdgeRejection;
pub(crate) use service::{OnReject, RpcInbox, proxy_channel, rpc_channel};
