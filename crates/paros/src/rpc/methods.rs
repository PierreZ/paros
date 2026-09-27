//! The method set: one moonpool-rpc [`RpcMethod`] marker per call paros
//! speaks, each served as a **well-known endpoint** ([`WellKnownMethod`]).
//!
//! Ids are explicit and permanent, never derived from Rust names: a method's
//! [`MethodId`] and its [`WellKnownId`] are the same number, and neither is
//! ever reused. An incompatible change to a method's bodies bumps its
//! [`SchemaVersion`], which a server refuses before decoding.
//!
//! - `0x5041_0001..` — the public journal (`proto/paros.proto`), served by
//!   a node; `QuorumRead` also by a replica.
//! - `0x5041_0101..` — cluster-internal (`proto/internal.proto`), served by a
//!   node; `Deliver` also by a proxy leader and a replica, `Inspect` also by
//!   a replica.
//! - `0x5041_0201..` — the matchmaker contract (`proto/matchmaker.proto`),
//!   served by a matchmaker.
//!
//! A role that does not serve a method simply does not register it, and a
//! call to it is refused `EndpointNotFound` before any handler runs.

use moonpool_rpc::{MethodId, RpcMethod, SchemaVersion, WellKnownId};

use super::{internal, matchmaker, public};

/// A method served at a fixed [`WellKnownId`] on every process that serves
/// it: callers address it by `ip:port` alone, and it answers whichever
/// incarnation is listening there.
pub trait WellKnownMethod: RpcMethod {
    /// The endpoint's well-known id (equal to the method id).
    const ID: WellKnownId;
}

macro_rules! well_known_method {
    ($(#[$doc:meta])* $marker:ident, $id:literal, $request:ty => $reply:ty, $name:literal) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy)]
        pub struct $marker;

        impl RpcMethod for $marker {
            type Request = $request;
            type Reply = $reply;
            const METHOD: MethodId = MethodId::new($id);
            const SCHEMA: SchemaVersion = SchemaVersion::new(1);
            const NAME: &'static str = $name;
        }

        impl WellKnownMethod for $marker {
            const ID: WellKnownId = WellKnownId::new($id);
        }
    };
}

well_known_method!(
    /// Propose a client command; answered once it commits, or redirected.
    ProposeRpc, 0x5041_0001, public::Propose => public::ProposeAck, "paros.Propose"
);
well_known_method!(
    /// A read-index read, asked of the leader.
    ReadRpc, 0x5041_0002, public::Read => public::ReadAck, "paros.Read"
);
well_known_method!(
    /// A leaderless quorum read (#143), asked of any node or replica.
    QuorumReadRpc, 0x5041_0003, public::Read => public::ReadAck, "paros.QuorumRead"
);
well_known_method!(
    /// Ask the leader to truncate the log.
    CompactRpc, 0x5041_0004, public::Compact => public::CompactAck, "paros.Compact"
);
well_known_method!(
    /// Ask the leader to reconfigure the acceptor set.
    ReconfigureRpc, 0x5041_0005, public::Reconfigure => public::ReconfigureAck, "paros.Reconfigure"
);
well_known_method!(
    /// Ask any node to drive a matchmaker-set handover (#125).
    ReconfigureMatchmakersRpc, 0x5041_0006,
    public::ReconfigureMatchmakers => public::ReconfigureMatchmakersAck,
    "paros.ReconfigureMatchmakers"
);
well_known_method!(
    /// Hand a batch of consensus messages to a peer. The ack means every
    /// message entered the peer's bounded inbox — not that its loop
    /// processed them.
    DeliverRpc, 0x5041_0101, internal::Deliver => internal::DeliverAck, "paros.internal.Deliver"
);
well_known_method!(
    /// Private replica inspection (deterministic workloads, operational
    /// tooling). The application bytes stay opaque to paros.
    InspectRpc, 0x5041_0102, internal::InspectRequest => internal::InspectReply,
    "paros.internal.Inspect"
);
well_known_method!(
    /// Operator decommissioning (#123), refused unless a GC watermark proves
    /// the cluster is done with this node.
    RetireRpc, 0x5041_0103, internal::RetireRequest => internal::RetireAck,
    "paros.internal.Retire"
);
well_known_method!(
    /// Register a configuration under a ballot and learn the history below
    /// it (Matchmaker Paxos's `MatchA` / `MatchB`).
    MatchmakeRpc, 0x5041_0201, matchmaker::MatchRequest => matchmaker::MatchReply,
    "paros.matchmaker.Matchmake"
);
well_known_method!(
    /// Raise the garbage-collection watermark (`GarbageA` / `GarbageB`).
    GarbageCollectRpc, 0x5041_0202, matchmaker::GarbageCollect => matchmaker::GarbageCollectAck,
    "paros.matchmaker.GarbageCollect"
);
well_known_method!(
    /// One step of a matchmaker-set reconfiguration (#125).
    MatchmakerReconfigureRpc, 0x5041_0203,
    matchmaker::ReconfigureRequest => matchmaker::ReconfigureReply,
    "paros.matchmaker.Reconfigure"
);
