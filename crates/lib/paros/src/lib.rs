//! `paros` — the Paxos node library.
//!
//! This is the user-facing entry point. It re-exports the sans-IO
//! [`core`] state machine and adds the **driver** that owns it and
//! performs I/O — the etcd-raft `Node` layer to `paros_core`'s `ColocatedNode`.
//!
//! [`run_node`] is written once over [`ParosProviders`](net::runtime::ParosProviders) (moonpool's local providers), so
//! the *same* code runs in production (`CompioProviders`) and deterministic
//! simulation (`SimProviders`); the deterministic-simulation harness lives in
//! `paros-sim` and adapts a moonpool `Process` to [`run_node`]; the `parosd`
//! binary (its own crate, #206) runs the same drivers on one compio thread.
//!
//! [`run_matchmaker`] is the same shape for the **matchmaker** role (the
//! per-ballot configuration registry of Matchmaker Paxos), driven over
//! [`MatchmakerStorage`](storage::MatchmakerStorage). It is opt-in: a deployment without matchmakers never
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
//! [`ReplicaNode`](core::ReplicaNode) — the chosen log and the application, no vote — over the
//! same [`LogStorage`](storage::LogStorage) a node runs on, one per journal it serves, opened
//! through the same [`JournalStores`](roles::node::JournalStores) seam (#193). Opt-in again: a deployment whose
//! `Config::replica_count` is zero runs none, and the node driver's learner
//! traffic reaches the pool alone.

//!
//! Each layer is a crate of its own (#722 (three products)), and this crate
//! re-exports each one as a module. A layer imports only the layers below
//! it: `core` ← `wire` ← `model` ← `tunables` ← `storage` ← `net` ←
//! `client` ← `observe` ← `roles` ← `coordinator` ← `machine`.

pub use paros_client as client;
pub use paros_coordinator as coordinator;
pub use paros_core as core;
pub use paros_machine as machine;
pub use paros_machine::inspect;
pub use paros_model as model;
pub use paros_net as net;
pub use paros_observe as observe;
pub use paros_roles as roles;
pub use paros_storage as storage;
pub use paros_tunables as tunables;
pub use paros_wire as wire;

pub use paros_machine::run_machine;
pub use paros_roles::batcher::run_batcher;
pub use paros_roles::frontend::run_frontend;
pub use paros_roles::matchmaker::run_matchmaker;
pub use paros_roles::node::{run_journals, run_node};
pub use paros_roles::proxy_leader::run_proxy;
pub use paros_roles::replica::run_replica;
