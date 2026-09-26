//! `paros` — the Paxos node library.
//!
//! This is the user-facing entry point. It re-exports the sans-IO
//! [`paros_core`] state machine and adds the **driver** that owns it and
//! performs I/O — the etcd-raft `Node` layer to `paros_core`'s `ColocatedNode`.
//!
//! [`run_node`] is written once over moonpool's `P: Providers` abstraction, so
//! the *same* code runs in production (`TokioProviders`) and deterministic
//! simulation (`SimProviders`); the deterministic-simulation harness lives in
//! `paros-sim` and adapts a moonpool `Process` to [`run_node`]. The client API
//! and a `parosd` binary land here too, once the protocol stabilizes.
//!
//! [`run_matchmaker`] is the same shape for the **matchmaker** role (the
//! per-ballot configuration registry of Matchmaker Paxos), driven over
//! [`MatchmakerStorage`]. It is opt-in: a deployment without matchmakers never
//! runs it, and [`run_node`] does not know it exists.
//!
//! [`run_proxy`] is the same shape again for the **proxy leader** role (#142,
//! Compartmentalized Paxos §3.1): the leader's Phase-2 fan-out and fold on a
//! process of its own, with nothing to persist. Opt-in the same way: a
//! deployment whose `Config::proxy_count` is zero runs no proxy and exchanges
//! exactly the plain deployment's messages.

mod audit;
mod corruption;
mod driver;
mod grpc;
mod hooks;
mod matchmaker;
mod proxy;
mod storage;

pub use audit::{Audit, DelegationOutcome, Deployment, HistoryPage, NoAudit, StorageFaultDecision};
pub use corruption::{
    CorruptionVerdict, IntegrityFault, RecoveryCase, SlotRecord, WitnessStatus, classify_log,
};
pub use driver::{
    BootKind, BootRefusal, DriverTunables, RunError, command_hash, message_kind, parse_addr,
    registration_history_hash, run_node,
};
pub use grpc::{
    Compact, CompactAck, EdgeRejection, InspectReply, InspectRequest, ParosClient,
    ParosInternalClient, Propose, ProposeAck, Read, ReadAck, Reconfigure, ReconfigureAck,
    ReconfigureMatchmakers, ReconfigureMatchmakersAck, RetireAck, RetireRequest, WireQuorumSystem,
    quorum_system_from_proto, quorum_system_to_proto,
};
pub use hooks::{DriverHooks, HandoffContext, NoHooks, Reply, Seam};
pub use matchmaker::{
    MatchmakerStorage, MemMatchmakerStorage, matchmaker_storage_contract_suite, run_matchmaker,
};
pub use proxy::{ProxyConfig, run_proxy};
pub use storage::{
    MemStorage, MetadataFault, NodeStorage, SNAP_CHUNK_BYTES, StorageError, StorageRecord,
    WriteOutcome, snap_chunk_count, storage_contract_suite,
};

// The whole sans-IO core, re-exported: the roles (`acceptor`, `proposer`,
// `replica`, `matchmaking`, `membership`, `retained`), `ColocatedNode`, the
// matchmaker, and every message, record and scalar type they exchange. No core
// name collides with a name `paros` defines itself, so a user reaches both
// through one path.
pub use paros_core::*;
