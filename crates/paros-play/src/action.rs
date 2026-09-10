//! Every verb the player has, and the one error type the engine answers with.
//!
//! An [`Action`] is the whole input surface of the game: the browser sends one
//! as JSON, [`crate::Game::act`] validates it against the level's
//! [`Level::allowed_actions`](crate::level::Level::allowed_actions) and the
//! world's own state, and only then calls into `paros-core`. Nothing here
//! reaches the core unvalidated — the core asserts, and an assert in wasm is an
//! abort with no stack, so every player-reachable refusal is an
//! [`ActionError`] instead.
//!
//! The log of actions *is* the save file: `undo` replays all but the last, and
//! the engine draws no randomness and reads no clock, so a replay is bit-exact.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::auto::AutomationFlag;

/// A durability seam a [`Action::CrashAt`] cuts the next drained batch at.
///
/// These are the two seams process-level attrition cannot reach: the crash
/// lands *inside* the `Ready` handshake, between the batch's writes and the
/// flush that makes them durable, or between that flush and the sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Seam {
    /// Cut before the batch is durable: nothing is written, nothing is sent.
    BeforeSync,
    /// Cut after the batch is durable but before its messages leave: the
    /// writes survive, the messages are lost.
    AfterSyncBeforeSend,
}

impl Seam {
    /// A short label for the view.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Seam::BeforeSync => "before sync",
            Seam::AfterSyncBeforeSend => "after sync, before send",
        }
    }
}

/// Which Paxos phase an [`Action::SetReach`] restricts (the Act I
/// quorum-intersection level).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Phase 1: which acceptors a `Prepare` reaches.
    One,
    /// Phase 2: which acceptors an `Accept` reaches.
    Two,
}

/// One player move.
///
/// The player is the network (`Deliver`, `Drop`, `Duplicate`), the clock
/// (`Tick`, `TickAll`, `SetElectionTimeout`, `StartElection`), the operator
/// (`Crash`, `CrashAt`, `Restart`, `StepDown`, `ResendPending`), the client
/// (`Propose`, `Retry`, `ReadIndex`, `Compact`), and — when a level makes a
/// role manual — the role itself (`Answer`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Action {
    /// Deliver the in-flight message `id` to its addressee. Delivering to a
    /// crashed node discards it.
    Deliver {
        /// The in-flight message's id.
        id: u64,
    },
    /// Drop the in-flight message `id`. This is what a partition is: the
    /// player not delivering.
    Drop {
        /// The in-flight message's id.
        id: u64,
    },
    /// Put a second copy of the in-flight message `id` on the wire.
    Duplicate {
        /// The in-flight message's id.
        id: u64,
    },
    /// Advance one node's logical clock by one tick.
    Tick {
        /// The node's id.
        node: u64,
    },
    /// Advance every live node's logical clock by one tick, in id order.
    TickAll,
    /// Drop a node's volatile state, keeping its disk.
    Crash {
        /// The node's id.
        node: u64,
    },
    /// Arm a durability seam: the node's **next** drained batch is cut there
    /// and the node crashes (see [`Seam`]).
    CrashAt {
        /// The node's id.
        node: u64,
        /// Where to cut.
        seam: Seam,
    },
    /// Rebuild a crashed node from its disk.
    Restart {
        /// The node's id.
        node: u64,
    },
    /// A client asks `node` to get `value` chosen.
    Propose {
        /// The node the client asks (must be the leader).
        node: u64,
        /// The client's id.
        client: u64,
        /// The command text; the engine turns it into opaque bytes.
        value: String,
    },
    /// Force `node`'s election timeout to fire on the spot.
    StartElection {
        /// The node's id.
        node: u64,
    },
    /// Set `node`'s election timeout, in ticks.
    SetElectionTimeout {
        /// The node's id.
        node: u64,
        /// The timeout in ticks (0 disables the clock entirely).
        ticks: u64,
    },
    /// A client asks `node` for a linearizable read.
    ReadIndex {
        /// The node the client asks (must be the leader).
        node: u64,
        /// Which client is reading. Omitted (or `null`) means the level's
        /// first client, which is what every single-client level wants and
        /// what the field meant before there were two of them.
        #[serde(default)]
        client: Option<u64>,
    },
    /// A client asks `node` again for a write it already sent — same client,
    /// same sequence number, same bytes.
    Retry {
        /// The node the client asks (must be the leader).
        node: u64,
        /// The client's id.
        client: u64,
        /// The sequence number of the write being retried.
        seq: u64,
    },
    /// A client asks `node` to drop the log prefix up to `up_to`.
    Compact {
        /// The node the client asks (must be the leader).
        node: u64,
        /// The last slot the client permits dropping, inclusive.
        up_to: u64,
    },
    /// Re-broadcast a leader's still-pending `Accept`s.
    ResendPending {
        /// The node's id.
        node: u64,
    },
    /// A leader resigns.
    StepDown {
        /// The node's id.
        node: u64,
    },
    /// Act I: a proposer opens Phase 1 at its next ballot for `value`.
    OpenBallot {
        /// The proposer's id.
        proposer: u64,
        /// The value it wants chosen.
        value: String,
    },
    /// Act I: which acceptors a phase's messages reach.
    SetReach {
        /// The phase to restrict.
        phase: Phase,
        /// The acceptors the phase reaches.
        nodes: Vec<u64>,
    },
    /// Answer the open prompt.
    Answer {
        /// The prompt's id (from [`crate::view::PromptView`]).
        prompt: u64,
        /// The chosen [`crate::prompt::Choice::id`].
        choice: String,
    },
    /// Turn an automation flag on or off.
    SetAutomation {
        /// The flag.
        flag: AutomationFlag,
        /// Whether it should be on.
        on: bool,
    },
}

/// The payload-free discriminant of an [`Action`]: what a level's
/// `allowed_actions` lists and what the action log renders as a family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    /// [`Action::Deliver`].
    Deliver,
    /// [`Action::Drop`].
    Drop,
    /// [`Action::Duplicate`].
    Duplicate,
    /// [`Action::Tick`].
    Tick,
    /// [`Action::TickAll`].
    TickAll,
    /// [`Action::Crash`].
    Crash,
    /// [`Action::CrashAt`].
    CrashAt,
    /// [`Action::Restart`].
    Restart,
    /// [`Action::Propose`].
    Propose,
    /// [`Action::StartElection`].
    StartElection,
    /// [`Action::SetElectionTimeout`].
    SetElectionTimeout,
    /// [`Action::ReadIndex`].
    ReadIndex,
    /// [`Action::Retry`].
    Retry,
    /// [`Action::Compact`].
    Compact,
    /// [`Action::ResendPending`].
    ResendPending,
    /// [`Action::StepDown`].
    StepDown,
    /// [`Action::OpenBallot`].
    OpenBallot,
    /// [`Action::SetReach`].
    SetReach,
    /// [`Action::Answer`].
    Answer,
    /// [`Action::SetAutomation`].
    SetAutomation,
}

impl Action {
    /// This action's payload-free family.
    #[must_use]
    pub fn kind(&self) -> ActionKind {
        match self {
            Action::Deliver { .. } => ActionKind::Deliver,
            Action::Drop { .. } => ActionKind::Drop,
            Action::Duplicate { .. } => ActionKind::Duplicate,
            Action::Tick { .. } => ActionKind::Tick,
            Action::TickAll => ActionKind::TickAll,
            Action::Crash { .. } => ActionKind::Crash,
            Action::CrashAt { .. } => ActionKind::CrashAt,
            Action::Restart { .. } => ActionKind::Restart,
            Action::Propose { .. } => ActionKind::Propose,
            Action::StartElection { .. } => ActionKind::StartElection,
            Action::SetElectionTimeout { .. } => ActionKind::SetElectionTimeout,
            Action::ReadIndex { .. } => ActionKind::ReadIndex,
            Action::Retry { .. } => ActionKind::Retry,
            Action::Compact { .. } => ActionKind::Compact,
            Action::ResendPending { .. } => ActionKind::ResendPending,
            Action::StepDown { .. } => ActionKind::StepDown,
            Action::OpenBallot { .. } => ActionKind::OpenBallot,
            Action::SetReach { .. } => ActionKind::SetReach,
            Action::Answer { .. } => ActionKind::Answer,
            Action::SetAutomation { .. } => ActionKind::SetAutomation,
        }
    }

    /// A one-line human label for the action log.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Action::Deliver { id } => format!("deliver #{id}"),
            Action::Drop { id } => format!("drop #{id}"),
            Action::Duplicate { id } => format!("duplicate #{id}"),
            Action::Tick { node } => format!("tick node {node}"),
            Action::TickAll => "tick every node".to_string(),
            Action::Crash { node } => format!("crash node {node}"),
            Action::CrashAt { node, seam } => {
                format!("crash node {node} {}", seam.label())
            }
            Action::Restart { node } => format!("restart node {node}"),
            Action::Propose {
                node,
                client,
                value,
            } => format!("client {client} proposes {value:?} at node {node}"),
            Action::StartElection { node } => format!("node {node} campaigns"),
            Action::SetElectionTimeout { node, ticks } => {
                format!("node {node} election timeout = {ticks}")
            }
            Action::ReadIndex { node, client } => match client {
                Some(client) => format!("client {client} reads at node {node}"),
                None => format!("read at node {node}"),
            },
            Action::Retry { node, client, seq } => {
                format!("client {client} retries write #{seq} at node {node}")
            }
            Action::Compact { node, up_to } => {
                format!("compact node {node} up to slot {up_to}")
            }
            Action::ResendPending { node } => format!("node {node} re-sends its accepts"),
            Action::StepDown { node } => format!("node {node} resigns"),
            Action::OpenBallot { proposer, value } => {
                format!("proposer {proposer} opens a ballot for {value:?}")
            }
            Action::SetReach { phase, nodes } => {
                let phase = match phase {
                    Phase::One => "phase 1",
                    Phase::Two => "phase 2",
                };
                format!("{phase} reaches {nodes:?}")
            }
            Action::Answer { choice, .. } => format!("answer {choice:?}"),
            Action::SetAutomation { flag, on } => {
                format!(
                    "{} automation {}",
                    flag.label(),
                    if *on { "on" } else { "off" }
                )
            }
        }
    }
}

/// Why an [`Action`] was refused. Stable `code`s for the UI, a sentence for the
/// player.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ActionErrorCode {
    /// No level with that id.
    UnknownLevel,
    /// The level does not offer this action.
    NotAllowed,
    /// No in-flight message with that id.
    UnknownMessage,
    /// No node with that id in this world.
    UnknownNode,
    /// The node is crashed.
    NodeCrashed,
    /// The node is already running.
    NodeAlive,
    /// The action belongs to the other world kind (log versus single decree).
    WrongWorld,
    /// A prompt is open: answer it first.
    PromptOpen,
    /// No prompt is open, or the id does not match the open one.
    NoPrompt,
    /// The prompt has no such choice.
    UnknownChoice,
    /// The node is not the leader.
    NotLeader,
    /// The level pins this automation flag off.
    PinnedOff,
    /// The automation flag is not unlocked in this level.
    NotUnlocked,
    /// No such client or proposer.
    UnknownParty,
    /// The action names an empty or out-of-pool reach set.
    BadReach,
    /// Nothing to undo.
    NothingToUndo,
}

/// An action the engine refused, with a stable code and a sentence to show.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ActionError {
    /// The stable machine-readable reason.
    pub code: ActionErrorCode,
    /// A sentence for the player.
    pub message: String,
}

impl ActionError {
    /// An error with `code` and `message`.
    pub fn new(code: ActionErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl core::fmt::Display for ActionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ActionError {}
