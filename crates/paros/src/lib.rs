//! `paros` — the Paxos node library.
//!
//! This is the user-facing entry point. It re-exports the sans-IO
//! [`paros_core`] state machine and adds the **driver** that owns it and
//! performs I/O — the etcd-raft `Node` layer to `paros_core`'s `ColocatedNode`.
//!
//! [`run_node`] is written once over moonpool's `P: Providers` abstraction, so
//! the *same* code runs in production (`TokioProviders`) and deterministic
//! simulation (`SimProviders`); the deterministic-simulation harness lives in
//! `paros-sim` and adapts a moonpool `Process` to [`run_node`]; the `parosd`
//! binary (its own crate, #206) runs the same drivers over Tokio.
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
//!
//! [`run_replica`] is the fourth (#144, Compartmentalized Paxos §3.3): a
//! [`ReplicaNode`] — the chosen log and the application, no vote — over the
//! same [`LogStorage`] a node runs on. Opt-in again: a deployment whose
//! `Config::replica_count` is zero runs none, and the node driver's learner
//! traffic reaches the pool alone.

mod audit;
pub mod client;
mod corruption;
mod driver;
pub mod fleet;
mod hooks;
pub mod journal;
pub mod machine;
mod matchmaker;
mod moment;
mod provision;
mod proxy;
mod replica_tier;
mod rpc;
mod storage;
pub mod system;

pub use audit::{
    Audit, DelegationOutcome, Deployment, HistoryPage, LogReadAnswer, LogReadReport, NoAudit,
    StorageFaultDecision,
};
pub use corruption::{CorruptionVerdict, IntegrityFault};
pub use driver::{
    BelowFloor, BootKind, BootRefusal, DriverTunables, JournalStores, RunError, SystemPlan,
    command_hash, message_kind, parse_addr, registration_history_hash, run_journals, run_node,
};
pub use rpc::{
    EdgeRejection, InspectRefusal, InspectReply, InspectRequest, InspectTarget, MAX_FRAME_BYTES,
    NodeClient, Read, ReadAck, Reconfigure, ReconfigureAck, ReconfigureMatchmakers,
    ReconfigureMatchmakersAck, RetireAck, RetireRequest, SetLeader, SetLeaderAck, Truncate,
    TruncateAck, WireQuorumSystem, Write, WriteAck, journal_state_from_proto,
    journal_state_to_proto, journal_view_from_proto, journal_view_to_proto, leader_from_proto,
    leader_uuid_from_proto, leader_uuid_to_proto, quorum_system_from_proto, quorum_system_to_proto,
};
/// The wire contract: the RPC method markers ([`rpc::methods`]) and the
/// generated protobuf bodies.
pub mod wire {
    pub use crate::rpc::methods;
    pub use crate::rpc::{
        checkpoint, common, fleet, internal, machine, matchmaker, public, system,
    };
}
pub use hooks::{DriverHooks, HandoffContext, NoHooks, Reply};
pub use journal::{JournalBootFacts, JournalMatchmakerStorage, JournalStorage, JournalStoreConfig};
pub use matchmaker::{
    MatchmakerStorage, MemMatchmakerStorage, matchmaker_storage_contract_suite, run_matchmaker,
};
pub use provision::{Provisioned, provision_matchmaker_store, provision_store};
pub use proxy::{ProxyConfig, run_proxy};
pub use replica_tier::run_replica;
pub use storage::{
    LogStorage, MemStorage, MetadataFault, StorageError, StorageRecord, WriteOutcome,
    storage_contract_suite,
};

// The whole sans-IO core, re-exported: the roles (`acceptor`, `proposer`,
// `replica`, `matchmaking`, `membership`, `retained`), `ColocatedNode`, the
// matchmaker, and every message, record and scalar type they exchange. No core
// name collides with a name `paros` defines itself, so a user reaches both
// through one path.
pub use paros_core::*;
