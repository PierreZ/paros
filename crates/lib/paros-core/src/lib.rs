//! `paros-core` — a sans-IO Multi-Paxos state machine.
//!
//! No I/O, no clock, no randomness, and std only — which keeps it portable to
//! wasm32 and trivially deterministic. Two optional features, neither of which
//! touches behavior:
//!
//! - `serde` (default off) adds `Serialize`/`Deserialize` derives on the public
//!   protocol types (e.g. [`Message`]) so a driver can put the same type on the
//!   wire; derives only, no runtime, and serde is itself wasm-safe.
//! - `tracing` (default **on**) adds `#[tracing::instrument]` spans on the
//!   state machine's public entry points and internal message handlers, each
//!   carrying the node id and the message's key coordinates. Spans observe and
//!   never decide; build with `default-features = false` for a dependency-free
//!   core with the identical state machine.
//!
//! The driver drives the core: feed events via [`ColocatedNode::step`] and logical time
//! via [`ColocatedNode::tick`], drain a batch of work via [`ColocatedNode::ready`], and
//! acknowledge it via [`Ready::advance`]. The core *describes* the side effects
//! to perform; the caller *performs* them.
//!
//! # The durability contract
//!
//! Each [`Ready`] batch must be processed in order: **persist [`HardState`] →
//! send [`Message`]s (only once the state is durable) → learn the committed
//! prefix → [`Ready::advance`]**. This persist-before-send edge is the heart of Paxos
//! safety; see [`Ready`] and [`HardState`] for the details.
//!
//! # The handshake is type-enforced
//!
//! [`ColocatedNode::ready`] returns a [`Ready`] that holds the node's unique mutable
//! borrow, so calling `ready()` again before [`Ready::advance`] is a *compile*
//! error — not a runtime panic.
//!
//! Beside the node lives the sans-IO **matchmaker** ([`Matchmaker`], the
//! per-ballot acceptor-configuration registry of Matchmaker Paxos), driven
//! through the same `step` → `ready` → `advance` shape. It is a separate handle:
//! a cluster deployed without matchmakers never constructs one, and [`ColocatedNode`]
//! never steps a matchmaker message.
//!
//! # Learning Paxos with paros-core
//!
//! [`ColocatedNode`] is one deployment of a few composable **roles** —
//! [`proposer::Proposer`], [`acceptor::Acceptor`], [`replica::Replica`] — and
//! the roles can be driven by hand. The interactive Paxos game on the paros
//! site does exactly that: each of its levels drives these roles, compiled to
//! WebAssembly, and teaches one property (single-decree Paxos, the log, the
//! [`Matchmaker`] and the [`matchmaking::Matchmaking`] phase, flexible and
//! grid quorums, quorum reads, proxy leaders and the replica tier). The game
//! is the crate `paros-wasm`, feature `paxos`.
//!
//! Beside them, `flexible_quorums.rs`, `acceptor_grid.rs` and
//! `quorum_read.rs` change the *data* the same roles run over, and
//! `proxy_leader.rs` runs the first **second deployment**: a
//! [`ProxyLeader`] beside a leader, the Phase-2 tally on another process.
//! `replica_tier.rs` runs the third: [`ReplicaNode`]s that learn and serve
//! reads without voting, beside acceptors that vote — none of them runs an
//! application: a journal's client folds what it reads (#186).

// First: `probe!` is a textual-scope macro every module below uses.
#[macro_use]
mod probe;

pub mod acceptor;
pub mod batcher;
pub mod collector;
pub mod contract;
pub mod decree;
pub mod journal;
pub mod matchmaker;
pub mod matchmaking;
pub mod membership;
#[cfg(test)]
mod models;
pub mod node;
pub mod proposer;
pub mod proxy_leader;
pub mod quorum_read;
pub mod replica;
pub mod replica_node;
pub mod retained;
#[cfg(test)]
mod test_support;
pub mod types;

// The root holds what a driver needs to write its loop: the deployments,
// the contract and the Paxos scalars. Every other item has one path, under
// its module.
pub use contract::{Config, HardState, Limits, Message, Storage, WriteOp};
pub use matchmaker::{Matchmaker, MatchmakerReconfigurer};
pub use node::{ColocatedNode, Ready};
pub use proxy_leader::ProxyLeader;
pub use replica_node::ReplicaNode;
pub use types::{Ballot, NodeId, Slot, Value};
