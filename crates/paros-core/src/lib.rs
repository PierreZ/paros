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
//! the roles can be driven by hand. Three runnable examples in this crate's
//! `examples/` directory do exactly that, in order, each with a printed trace
//! and assertions on the property it teaches:
//!
//! 1. `single_decree.rs` — Phase 1, P2c, Phase 2: one value
//!    (`cargo run -p paros-core --example single_decree`).
//! 2. `multi_paxos.rs` — slots versus ballots, one Phase 1 amortized over many
//!    slots, leader recovery as P2c per slot.
//! 3. `matchmaker.rs` — configuration discovery through the [`Matchmaker`]
//!    and the [`matchmaking::Matchmaking`] phase, reconfiguration, and the
//!    matchmaker set itself chosen by the same single-decree Paxos over
//!    `Vec<MatchmakerId>`, persisted through a [`MemRegistry`].
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
mod collector;
pub mod decree;
pub mod journal_state;
mod matchmaker;
pub mod matchmaking;
pub mod membership;
mod message;
#[cfg(test)]
mod model_support;
mod node;
pub mod proposer;
pub mod proxy_leader;
#[cfg(test)]
mod proxy_model;
pub mod quorum_read;
mod ready;
pub mod replica;
pub mod replica_node;
pub mod retained;
mod state;
mod storage;
mod types;
mod write;

pub use decree::Decree;
pub use journal_state::{JournalState, JournalView, Outcome, WriterMode};
pub use matchmaker::{
    DecreeRecord, GcAck, GcOutcome, GcRequest, JournalRegistry, JournalScalars, MatchOutcome,
    MatchPurpose, MatchRefusal, MatchReply, MatchRequest, Matchmaker, MatchmakerConfig,
    MatchmakerHardState, MatchmakerPhase, MatchmakerReady, MatchmakerReconfigurer,
    MatchmakerWriteOp, MemRegistry, PendingBootstrap, REGISTRY_PAGE, ReconfigureReply,
    ReconfigureRequest, ReconfigurerPhase, ReconfigurerReady, ReconfigurerStep, Reconstruction,
    Registration, RegistrationKind, RegistryStorage, StartRefusal, SuccessorDecree,
};
pub use membership::{
    AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, ProxyId, QuorumSystem,
    ReplicaId,
};
pub use message::{Audience, Message, Party};
pub use node::{
    APPLY_BATCH, BeliefSource, ColocatedNode, Delegation, GcStep, HANDOFF_BATCH,
    HANDOFF_FENCE_ELECTIONS, HEARTBEAT_TICKS, Handoff, HandoffCounters, LEADER_RECOVERY_BATCH,
    LeadershipOrigin, MatchStep, MembershipCounters, NodeRole, PROMISE_BATCH, ProposeResult,
    REPAIR_TIMEOUT_ELECTIONS, RESEND_BATCH, ReadState, ReconfigureRefusal, ReconfigureResult,
    RepairCounters,
};
pub use proxy_leader::{ProxyLeader, ProxyReady};
pub use quorum_read::{PreReadFold, QuorumRead, QuorumReads, ReadBasis};
pub use ready::Ready;
pub use replica::{LogPage, LogRead};
pub use replica_node::{ReplicaCounters, ReplicaNode, ReplicaReady};
pub use retained::RetainedWindow;
pub use state::{Config, HardState};
pub use storage::Storage;
pub use types::{
    Ballot, Command, Control, Entry, Fingerprint, JournalId, JournalIdentifier, LeaderUuid, NodeId,
    Seq, Slot, TenantId, Value, command_fingerprint,
};
pub use write::{AcceptorWrite, MustSync, WriteOp};
