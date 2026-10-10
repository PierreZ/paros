//! The method set: one moonpool-rpc [`RpcMethod`] marker per call paros
//! speaks, each served as a **well-known endpoint** ([`WellKnownMethod`]).
//!
//! Ids are explicit and permanent, never derived from Rust names: a method's
//! [`MethodId`] and its [`WellKnownId`] are the same number, and neither is
//! ever reused. An incompatible change to a method's bodies bumps its
//! [`SchemaVersion`], which a server refuses before decoding.
//!
//! - `0x5041_0001..` — the public journal (`proto/paros.proto`), served by
//!   a node; `Read` also by a replica.
//! - `0x5041_0101..` — cluster-internal (`proto/internal.proto`), served by a
//!   node; `Deliver` also by a proxy leader and a replica, `Inspect` also by
//!   a replica.
//! - `0x5041_0201..` — the matchmaker contract (`proto/matchmaker.proto`),
//!   served by a matchmaker.
//! - `0x5041_0301..` — the machine contract (`proto/machine.proto`, #196),
//!   served by an uninitialized `parosd` while it waits for `init`.
//!
//! A role that does not serve a method simply does not register it, and a
//! call to it is refused `EndpointNotFound` before any handler runs.

use moonpool_rpc::{MethodId, RpcMethod, SchemaVersion, WellKnownId};

use super::{internal, machine, matchmaker, public};

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

// 0x5041_0001..=0x5041_0004 were `Propose`, the read-index `Read`,
// `QuorumRead` and `Compact`, superseded by the journal calls of #185;
// 0x5041_0007..=0x5041_000A were those calls — `Append`, the LSN `Read`,
// `CheckTail` and `Trim` — superseded by the four calls of #204. Retired,
// never reused.
well_known_method!(
    /// Append a batch at a position under a writer generation (#204);
    /// answered with the verdict the journal state machine gave at apply,
    /// or redirected.
    WriteRpc, 0x5041_000B, public::Write => public::WriteAck, "paros.Write"
);
well_known_method!(
    /// Read a journal's records from a position up (#204); served by any
    /// node or replica through the leaderless read, long-polling at the
    /// tail.
    ReadRpc, 0x5041_000C, public::Read => public::ReadAck, "paros.Read"
);
well_known_method!(
    /// Drop every record below a position (#204), decided by consensus.
    TruncateRpc, 0x5041_000D, public::Truncate => public::TruncateAck, "paros.Truncate"
);
well_known_method!(
    /// Compare-and-swap a journal's writer (#204).
    SetLeaderRpc, 0x5041_000E, public::SetLeader => public::SetLeaderAck, "paros.SetLeader"
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
well_known_method!(
    /// Who a waiting machine is (#196).
    IdentifyRpc, 0x5041_0301, machine::Identify => machine::IdentifyAck,
    "paros.machine.Identify"
);
well_known_method!(
    /// Phase 2 of the cell decree: accept a plan, which forms the machine
    /// (#196, #277).
    FormCellRpc, 0x5041_0302, machine::FormCell => machine::FormCellAck,
    "paros.machine.FormCell"
);
// 0x5041_0303 was `Init`, the seed-based formation, superseded by the cell
// decree of #277. Retired, never reused.
well_known_method!(
    /// `cell init`: drive the cell decree over the listed machines (#277).
    CellInitRpc, 0x5041_0304, machine::CellInit => machine::CellInitAck,
    "paros.machine.CellInit"
);
well_known_method!(
    /// Phase 1 of the cell decree: promise, and report an accepted plan (#277).
    PrepareCellRpc, 0x5041_0305, machine::PrepareCell => machine::PrepareCellAck,
    "paros.machine.PrepareCell"
);
well_known_method!(
    /// `cell add-machine`'s last step: admit an idle machine into a cell
    /// that registered it (#216).
    AdmitRpc, 0x5041_0306, machine::Admit => machine::AdmitAck,
    "paros.machine.Admit"
);
well_known_method!(
    /// A machine of a cell asks the cell coordinator to register its
    /// advertised address (#349).
    RegisterRpc, 0x5041_0307, machine::Register => machine::RegisterAck,
    "paros.machine.Register"
);
well_known_method!(
    /// A tenant's journal create or delete, sent to the tenant coordinator
    /// (#210).
    JournalRequestRpc, 0x5041_0308, machine::JournalRequest => machine::JournalRequestAck,
    "paros.machine.JournalRequest"
);
