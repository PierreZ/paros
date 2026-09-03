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
//! The application drives the core: feed events via [`RawNode::step`] and logical time
//! via [`RawNode::tick`], drain a batch of work via [`RawNode::ready`], and
//! acknowledge it via [`Ready::advance`]. The core *describes* the side effects
//! to perform; the caller *performs* them.
//!
//! # The durability contract
//!
//! Each [`Ready`] batch must be processed in order: **persist [`HardState`] →
//! send [`Message`]s (only once the state is durable) → apply committed values →
//! [`Ready::advance`]**. This persist-before-send edge is the heart of Paxos
//! safety; see [`Ready`] and [`HardState`] for the details.
//!
//! # The handshake is type-enforced
//!
//! [`RawNode::ready`] returns a [`Ready`] that holds the node's unique mutable
//! borrow, so calling `ready()` again before [`Ready::advance`] is a *compile*
//! error — not a runtime panic.
//!
//! Stage 0 pins this contract in the type system with **zero protocol logic**.
//!
//! Beside the node lives the sans-IO **matchmaker** ([`Matchmaker`], the
//! per-ballot acceptor-configuration registry of Matchmaker Paxos), driven
//! through the same `step` → `ready` → `advance` shape. It is a separate handle:
//! a cluster deployed without matchmakers never constructs one, and [`RawNode`]
//! never steps a matchmaker message.

pub(crate) mod acceptor;
mod collector;
mod matchmaker;
pub(crate) mod membership;
mod message;
mod node;
pub(crate) mod proposer;
mod ready;
pub(crate) mod replica;
mod single_decree;
mod state;
mod storage;
mod types;
mod write;

pub use matchmaker::{
    GcAck, GcOutcome, GcRequest, MatchOutcome, MatchRefusal, MatchReply, MatchRequest, Matchmaker,
    MatchmakerConfig, MatchmakerHardState, MatchmakerPhase, MatchmakerReady,
    MatchmakerReconfigurer, MatchmakerWriteOp, PendingBootstrap, ReconfigureReply,
    ReconfigureRequest, ReconfigurerPhase, ReconfigurerStep, Registration, RegistryStorage,
    StartRefusal,
};
pub use membership::{
    AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, QuorumSystem,
};
pub use message::Message;
pub use node::{
    GcStep, HANDOFF_BATCH, HANDOFF_FENCE_ELECTIONS, HEARTBEAT_TICKS, Handoff, HandoffCounters,
    LEADER_RECOVERY_BATCH, LeadershipOrigin, MatchStep, NodeRole, PROMISE_BATCH, ProposeResult,
    REPAIR_TIMEOUT_ELECTIONS, RawNode, ReadIndexResult, ReadState, ReconfigureRefusal,
    ReconfigureResult,
};
pub use ready::Ready;
pub use single_decree::{AcceptFold, DecreeAcceptor, DecreePhase, DecreeProposer, PromiseFold};
pub use state::{Config, HardState};
pub use storage::Storage;
pub use types::{
    Ballot, ClientId, ClientSeq, Command, ConfigId, Control, Entry, NodeId, SessionEntry, Slot,
    Value, command_fingerprint,
};
pub use write::{MustSync, WriteOp};
