//! `paros` — the Paxos node library.
//!
//! This is the user-facing entry point. It re-exports the sans-IO
//! [`paros_core`] state machine and adds the **driver** that owns it and
//! performs I/O — the etcd-raft `Node` layer to `paros_core`'s `ColocatedNode`.
//!
//! [`run_node`] is written once over [`ParosProviders`] (moonpool's local providers), so
//! the *same* code runs in production (`CompioProviders`) and deterministic
//! simulation (`SimProviders`); the deterministic-simulation harness lives in
//! `paros-sim` and adapts a moonpool `Process` to [`run_node`]; the `parosd`
//! binary (its own crate, #206) runs the same drivers on one compio thread.
//!
//! [`run_matchmaker`] is the same shape for the **matchmaker** role (the
//! per-ballot configuration registry of Matchmaker Paxos), driven over
//! [`MatchmakerStorage`]. It is opt-in: a deployment without matchmakers never
//! runs it, and [`run_node`] does not know it exists.
//!
//! [`run_proxy`] is the same shape again for the **proxy leader** role (#142,
//! Compartmentalized Paxos §3.1): the leader's Phase-2 fan-out and fold on a
//! process of its own, with nothing to persist, one `ProxyLeader` per journal it
//! serves (#193). Opt-in the same way: a
//! deployment whose `Config::proxy_count` is zero runs no proxy and exchanges
//! exactly the plain deployment's messages.
//!
//! [`run_replica`] is the fourth (#144, Compartmentalized Paxos §3.3): a
//! [`ReplicaNode`] — the chosen log and the application, no vote — over the
//! same [`LogStorage`] a node runs on, one per journal it serves, opened
//! through the same [`JournalStores`] seam (#193). Opt-in again: a deployment whose
//! `Config::replica_count` is zero runs none, and the node driver's learner
//! traffic reaches the pool alone.

pub mod client;
pub mod coordinator;
pub mod inspect;
pub mod machine;
pub mod model;
pub mod net;
pub mod observe;
pub mod roles;
pub mod storage;
pub mod tunables;
pub mod wire;

pub use model::address::{Address, Names};
pub use model::corruption::{CorruptionVerdict, IntegrityFault};
pub use net::runtime::ParosProviders;
pub use net::transport::Transport;
pub use net::{EdgeRejection, NodeClient, Pass};
pub use observe::audit::{
    Audit, DelegationOutcome, Deployment, HistoryPage, LogReadAnswer, LogReadReport, NoAudit,
    StorageFaultDecision,
};
pub use roles::matchmaker::{
    HostedSet, MatchmakerStorage, MemMatchmakerStorage, Registrations,
    matchmaker_storage_contract_suite, run_matchmaker,
};
pub use roles::node::reply::Reply;
pub use roles::node::{
    BelowFloor, BootKind, BootRefusal, ControlPlan, DriverTunables, JournalStores,
    LOAD_INTERVAL_FLOOR, MAX_READ_RECORDS, RunError, command_hash, message_kind, parse_addr,
    registration_history_hash, run_journals, run_node,
};
pub use roles::proxy_leader::{ProxyConfig, ProxyJournal, run_proxy};
pub use roles::replica::run_replica;
pub use storage::journal::{
    JournalBootFacts, JournalMatchmakerStorage, JournalStorage, JournalStoreConfig,
};
pub use storage::provision::{Provisioned, provision_matchmaker_store, provision_store};
pub use storage::{
    LogStorage, MemStorage, MetadataFault, ScrubPage, ScrubbedSlot, StorageError, StorageRecord,
    WriteOutcome, command_bytes, storage_contract_suite,
};
pub use tunables::MAX_FRAME_BYTES;
pub use wire::{
    InspectRefusal, InspectReply, InspectRequest, InspectTarget, Read, ReadAck, Reconfigure,
    ReconfigureAck, ReconfigureMatchmakers, ReconfigureMatchmakersAck, RetireAck, RetireRequest,
    SetLeader, SetLeaderAck, Truncate, TruncateAck, WireQuorumSystem, Write, WriteAck,
    journal_state_from_proto, journal_state_to_proto, journal_view_from_proto,
    journal_view_to_proto, leader_from_proto, leader_uuid_from_proto, leader_uuid_to_proto,
    quorum_system_from_proto, quorum_system_to_proto,
};

// The whole sans-IO core, re-exported: the roles (`acceptor`, `proposer`,
// `replica`, `matchmaking`, `membership`, `retained`), `ColocatedNode`, the
// matchmaker, and every message, record and scalar type they exchange. No core
// name collides with a name `paros` defines itself, so a user reaches both
// through one path.
pub use paros_core::*;
