//! `paros-consensus`: the Paxos roles of paros as sans-IO state machines.
//!
//! The crate has no I/O, no clock and no randomness, and it uses std only.
//! A caller, the **driver**, owns the sockets, the disk and the timers. The
//! core computes what to persist and what to send; the driver does it. The
//! same code runs in a test, in a simulation, in a browser (wasm32) and
//! behind a real network.
//!
//! # Quick start
//!
//! Three [`ColocatedNode`]s, each with a [`contract::MemStorage`] as its
//! disk, and a queue as the network. `examples/quickstart.rs` is the same
//! program with a crash, a restart and a trace.
//!
//! ```
//! use std::collections::VecDeque;
//!
//! use paros_consensus::contract::MemStorage;
//! use paros_consensus::journal::{Control, Entry, JournalIdentifier, LeaderUuid, Outcome, Seq};
//! use paros_consensus::node::ProposeResult;
//! use paros_consensus::{ColocatedNode, Config, Limits, Message, NodeId, Value};
//!
//! let ids = [NodeId(1), NodeId(2), NodeId(3)];
//! let mut disks: Vec<MemStorage> = ids
//!     .iter()
//!     .map(|id| MemStorage::new(Config::new(*id, JournalIdentifier::UNSET).with_peers(ids)))
//!     .collect();
//! let mut nodes: Vec<ColocatedNode> = disks
//!     .iter()
//!     .map(|disk| ColocatedNode::new(disk, Limits::DEFAULT))
//!     .collect();
//! let mut outcomes = Vec::new();
//!
//! // The driver loop: drain node `i`, then deliver what it sent, until the
//! // network is quiet.
//! let mut run = |nodes: &mut Vec<ColocatedNode>, start: usize| {
//!     let mut wire: VecDeque<(NodeId, Message)> = VecDeque::new();
//!     let mut next = Some(start);
//!     while let Some(i) = next {
//!         loop {
//!             let ready = nodes[i].ready();
//!             if ready.is_empty() {
//!                 ready.advance();
//!                 break;
//!             }
//!             disks[i].apply(ready.writes()); // 1. persist
//!             for (audience, message) in ready.messages() {
//!                 for to in audience.resolve(&ids, ids[i]) {
//!                     wire.push_back((to, message.clone())); // 2. send
//!                 }
//!             }
//!             if i == 0 {
//!                 // 3. learn: the verdict of the journal on each slot
//!                 outcomes.extend(ready.committed().iter().map(|(_, _, _, o)| o.clone()));
//!             }
//!             ready.advance(); // 4. release the batch
//!             nodes[i].advance_recovery();
//!         }
//!         next = wire.pop_front().map(|(to, message)| {
//!             let i = ids.iter().position(|id| *id == to).expect("a known node");
//!             nodes[i].step(message);
//!             i
//!         });
//!     }
//! };
//!
//! // Node 1 wins an election.
//! nodes[0].campaign();
//! run(&mut nodes, 0);
//! assert!(nodes[0].is_leader());
//!
//! // A client becomes the writer of the journal, then appends one record.
//! let writer = LeaderUuid(7);
//! let set_leader = Control::SetLeader { new: writer, old: None };
//! assert!(matches!(nodes[0].propose_control(set_leader), ProposeResult::Accepted(_)));
//! run(&mut nodes, 0);
//! let entry = Entry { leader: writer, seq: Seq(0), records: vec![Value::from(b"hello".to_vec())] };
//! assert!(matches!(nodes[0].propose(entry), ProposeResult::Accepted(_)));
//! run(&mut nodes, 0);
//!
//! assert!(matches!(outcomes[0], Outcome::Leader(_)));
//! assert_eq!(outcomes[1], Outcome::Accepted { seq: Seq(0), count: 1 });
//! ```
//!
//! # The contract of a driver
//!
//! The driver gives the core its inputs: a peer message with
//! [`ColocatedNode::step`], logical time with [`ColocatedNode::tick`], and a
//! client call with [`ColocatedNode::propose`] or
//! [`ColocatedNode::propose_control`]. After each input it drains a batch
//! with [`ColocatedNode::ready`] and processes it in this order:
//!
//! 1. **persist** [`Ready::writes`] ([`WriteOp`]s, in order), and fsync;
//! 2. **send** [`Ready::messages`], only when step 1 is durable;
//! 3. **learn** [`Ready::committed`]: the slots that are chosen and durable,
//!    each with the [`journal::Outcome`] that answers its client;
//! 4. **release** the batch with [`Ready::advance`].
//!
//! Persist before send is the rule that makes Paxos safe: a `Promise` or an
//! `Accepted` that leaves before its write is durable is a vote that a crash
//! takes back. See [`Ready`] and [`HardState`] for the details. One message
//! may leave early: a batch that raises no promise may send its `Accept`s
//! before its own flush, because an `Accept` carries a proposal, not a vote
//! (Paxos Made Live, section 4.2). Between `advance()` and the end of the
//! flush, the driver steps nothing into the node: one batch is in flight.
//!
//! [`ColocatedNode::ready`] returns a [`Ready`] that holds the unique mutable
//! borrow of the node. A second `ready()` before [`Ready::advance`] is a
//! *compile* error, not a runtime panic.
//!
//! A restart is [`ColocatedNode::new`] on the storage that the crash left. A
//! node always boots as a follower: leadership is never durable.
//!
//! # The journal
//!
//! The log carries a **journal** ([`journal`]): a `Write` appends records at
//! a position, `SetLeader` names the client that may write, and `Truncate`
//! drops a prefix. The core judges none of them at `propose`. Each node
//! judges a command when its slot is applied, in slot order
//! ([`journal::JournalState::apply`]), so each node reaches the same
//! [`journal::Outcome`]. A read is [`ColocatedNode::read_log`].
//!
//! # The deployments and the roles
//!
//! [`ColocatedNode`] is one deployment of a few composable **roles**:
//! [`proposer::Proposer`], [`acceptor::Acceptor`] and [`replica::Replica`].
//! The crate has two more deployments, [`ProxyLeader`] (the Phase-2 tally on
//! another process) and [`ReplicaNode`] (a node that learns and serves reads
//! and never votes), and the [`Matchmaker`] of Matchmaker Paxos. Each one
//! has the same shape: `step`, then `ready`, then `advance`. A deployment
//! without matchmakers never constructs a [`Matchmaker`], and a
//! [`ColocatedNode`] never steps a matchmaker message.
//!
//! What a deployment runs is configuration data ([`Config`]), never a cargo
//! feature: the quorum system ([`membership::QuorumSystem`], a majority or a
//! grid), the matchmakers, the proxy leaders and the replicas.
//!
//! # Examples
//!
//! Each example is one program in `examples/`, with a printed trace and
//! assertions on the property that it shows. Run one with
//! `cargo run -p paros-consensus --example <name>`.
//!
//! The first example is single-decree Paxos on [`decree::Decree`]. Each
//! other example drives the deployments as a driver does, on the toy
//! cluster of `examples/common/cluster.rs`: one disk for each process,
//! persist before send, and a network that can lose or hold a message.
//!
//! | Example | Shows |
//! |---|---|
//! | `single_decree` | [`decree::Decree`] and [`acceptor::Acceptor`]: Phase 1, P2c, Phase 2 for one value, and a restart |
//! | `quickstart` | the driver loop, an election, writes, a crash and a restart |
//! | `multi_paxos` | one Phase 1 for many slots, and the recovery of a new leader: P2c and the `Noop` gap fill |
//! | `journal_calls` | `SetLeader`, `Write`, a retry, a read, `Truncate`, and each [`journal::Outcome`] |
//! | `leader_handoff` | [`ColocatedNode::relinquish_to`]: a new leader without an election |
//! | `acceptor_grid` | a grid of acceptors ([`membership::QuorumSystem::grid`]): one column decides, one acceptor of each column elects, [`ColocatedNode::move_round`] |
//! | `quorum_read` | a leaderless read on a grid ([`ColocatedNode::quorum_read`]) |
//! | `proxy_leader` | [`ProxyLeader`]s beside the leader |
//! | `replica_tier` | [`ReplicaNode`]s beside the acceptors |
//! | `matchmaker` | the registry of the [`Matchmaker`]s, and the handover of their set ([`MatchmakerReconfigurer`]) |
//! | `reconfiguration` | [`ColocatedNode::reconfigure`] with three [`Matchmaker`]s, garbage collection, a retirement |
//!
//! # Features
//!
//! No feature changes a decision of the core.
//!
//! - `tracing` (default on) adds `#[tracing::instrument]` spans on the entry
//!   points and the message handlers.
//! - `assertions` (default on) adds coverage probes for a simulation.
//! - `serde` (default off) derives `Serialize` and `Deserialize` on the
//!   public protocol types (for example [`Message`]), so a driver can put the
//!   same type on the wire.
//!
//! Build with `default-features = false` for a core with no dependency.

// First: `probe!` is a textual-scope macro every module below uses.
#[macro_use]
mod probe;

pub mod acceptor;
pub mod batcher;
pub mod belief;
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
pub mod publisher;
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
pub use types::{Ballot, FNV_OFFSET, NodeId, Slot, Value, fnv1a};
