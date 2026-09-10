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
            Seam::BeforeSync => "before the sync",
            Seam::AfterSyncBeforeSend => "after the sync and before the send",
        }
    }
}

/// A ballot an operator types: the round, and the node that minted it.
///
/// The one place a player writes a ballot down is the evidence a `Retire`
/// carries, and that evidence has to be exact — an operator reads it from a
/// leader's report and passes it on unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct BallotSpec {
    /// The round.
    pub round: u64,
    /// The node that minted the ballot.
    pub node: u64,
}

impl BallotSpec {
    /// The core ballot it names.
    #[must_use]
    pub fn ballot(self) -> paros_core::Ballot {
        paros_core::Ballot {
            round: self.round,
            node: paros_core::NodeId(self.node),
        }
    }
}

/// The quorum system a new acceptor configuration runs.
///
/// A configuration and its quorum system are one thing: the membership must
/// admit the system, and a set that does not is refused rather than repaired.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuorumSpec {
    /// More than half the acceptors, in both phases.
    Majority,
    /// A split: `q1` acceptors answer Phase 1 and `q2` vote in Phase 2.
    Flexible {
        /// How many answer Phase 1.
        q1: u64,
        /// How many vote in Phase 2.
        q2: u64,
    },
    /// A grid: a full row answers Phase 1 and a full column votes in Phase 2.
    Grid {
        /// The rows.
        rows: u64,
        /// The columns.
        cols: u64,
    },
}

impl QuorumSpec {
    /// The core quorum system it names.
    #[must_use]
    pub fn system(self) -> paros_core::QuorumSystem {
        match self {
            QuorumSpec::Majority => paros_core::QuorumSystem::Majority,
            QuorumSpec::Flexible { q1, q2 } => paros_core::QuorumSystem::Flexible {
                q1: usize::try_from(q1).unwrap_or(usize::MAX),
                q2: usize::try_from(q2).unwrap_or(usize::MAX),
            },
            QuorumSpec::Grid { rows, cols } => paros_core::QuorumSystem::Grid {
                rows: usize::try_from(rows).unwrap_or(usize::MAX),
                cols: usize::try_from(cols).unwrap_or(usize::MAX),
            },
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
        /// Address the copy to a **different** node: a misrouted message,
        /// which is a thing networks do and which every rule in the protocol
        /// is written to survive. Omitted (or `null`) keeps the addressee.
        #[serde(default)]
        #[ts(optional = nullable)]
        to: Option<u64>,
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
        /// Which column of an acceptor grid this proposal's Accept goes to.
        /// Omitted (or `null`) lets the configuration derive it, which is
        /// what every deployment that is not a grid does.
        #[serde(default)]
        #[ts(optional = nullable)]
        column: Option<u64>,
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
        #[ts(optional = nullable)]
        client: Option<u64>,
    },
    /// A client asks `node` for a **leaderless** read: `node` asks a Phase-1
    /// quorum for their vote watermarks and serves the read once its own
    /// applied prefix covers the highest of them. Any node may answer one.
    QuorumRead {
        /// The node the client asks. It need not be the leader.
        node: u64,
        /// Which client is reading. Omitted (or `null`) means the level's
        /// first client.
        #[serde(default)]
        #[ts(optional = nullable)]
        client: Option<u64>,
    },
    /// A leader hands its Phase-2 authority to a peer, under the same ballot
    /// and with no Phase 1 of its own.
    Relinquish {
        /// The leader that gives the authority up.
        node: u64,
        /// The peer that is offered it.
        to: u64,
    },
    /// Rot one accepted record on `node`'s disk: its value is lost and its
    /// identity survives. The node reads it back at its next boot.
    Corrupt {
        /// The node whose disk is damaged.
        node: u64,
        /// The slot whose record loses its value.
        slot: u64,
    },
    /// Erase `node`'s disk. What survives is the operator's memory of having
    /// provisioned that identity, which is what makes the refusal possible.
    Wipe {
        /// The node whose disk is erased.
        node: u64,
    },
    /// Drop a matchmaker's volatile role, keeping its registry.
    ///
    /// A matchmaker is not a node — it holds no log and votes on no slot, and
    /// its ids are their own space — so it gets its own verb rather than an
    /// overloaded [`Action::Crash`].
    CrashMatchmaker {
        /// The matchmaker's id.
        matchmaker: u64,
    },
    /// Rebuild a crashed matchmaker from its registry.
    RestartMatchmaker {
        /// The matchmaker's id.
        matchmaker: u64,
    },
    /// A client asks the leader at `node` to put a new acceptor set in force.
    Reconfigure {
        /// The node the client asks (must be the leader).
        node: u64,
        /// The acceptors of the new configuration.
        members: Vec<u64>,
        /// The quorum system it runs. Omitted (or `null`) is a majority, which
        /// is what every deployment runs unless it says otherwise.
        #[serde(default)]
        #[ts(optional = nullable)]
        quorum: Option<QuorumSpec>,
    },
    /// An operator asks `target` to shut down for good, showing the effective
    /// garbage-collection watermark read from `node`'s report.
    Retire {
        /// The leader whose report the operator read the watermark from.
        node: u64,
        /// The acceptor asked to retire.
        target: u64,
        /// The evidence. Omitted (or `null`) sends none, which is refused:
        /// an installed successor is not a collected predecessor.
        #[serde(default)]
        #[ts(optional = nullable)]
        gc_watermark: Option<BallotSpec>,
    },
    /// `node` drives a handover of the matchmaker set onto `members`.
    ReconfigureMatchmakers {
        /// The node that drives the handover.
        node: u64,
        /// The matchmakers of the proposed successor.
        members: Vec<u64>,
    },
    /// Re-send a candidate's open registration to every matchmaker that has
    /// not answered.
    ResendMatchmaking {
        /// The node's id.
        node: u64,
    },
    /// Re-send a leader's open garbage-collection request.
    ResendGc {
        /// The node's id.
        node: u64,
    },
    /// Re-issue a handover's current step, and close a freeze whose quorum has
    /// answered.
    ResendReconfigurer {
        /// The node's id.
        node: u64,
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
    /// [`Action::QuorumRead`].
    QuorumRead,
    /// [`Action::Relinquish`].
    Relinquish,
    /// [`Action::Corrupt`].
    Corrupt,
    /// [`Action::Wipe`].
    Wipe,
    /// [`Action::CrashMatchmaker`].
    CrashMatchmaker,
    /// [`Action::RestartMatchmaker`].
    RestartMatchmaker,
    /// [`Action::Reconfigure`].
    Reconfigure,
    /// [`Action::Retire`].
    Retire,
    /// [`Action::ReconfigureMatchmakers`].
    ReconfigureMatchmakers,
    /// [`Action::ResendMatchmaking`].
    ResendMatchmaking,
    /// [`Action::ResendGc`].
    ResendGc,
    /// [`Action::ResendReconfigurer`].
    ResendReconfigurer,
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
            Action::QuorumRead { .. } => ActionKind::QuorumRead,
            Action::Relinquish { .. } => ActionKind::Relinquish,
            Action::Corrupt { .. } => ActionKind::Corrupt,
            Action::Wipe { .. } => ActionKind::Wipe,
            Action::CrashMatchmaker { .. } => ActionKind::CrashMatchmaker,
            Action::RestartMatchmaker { .. } => ActionKind::RestartMatchmaker,
            Action::Reconfigure { .. } => ActionKind::Reconfigure,
            Action::Retire { .. } => ActionKind::Retire,
            Action::ReconfigureMatchmakers { .. } => ActionKind::ReconfigureMatchmakers,
            Action::ResendMatchmaking { .. } => ActionKind::ResendMatchmaking,
            Action::ResendGc { .. } => ActionKind::ResendGc,
            Action::ResendReconfigurer { .. } => ActionKind::ResendReconfigurer,
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
            Action::Deliver { id } => format!("deliver message {id}"),
            Action::Drop { id } => format!("drop message {id}"),
            Action::Duplicate { id, to } => match to {
                Some(to) => format!("send a copy of message {id} to node {to}"),
                None => format!("send a copy of message {id}"),
            },
            Action::Tick { node } => format!("advance the clock of node {node}"),
            Action::TickAll => "advance the clock of every node".to_string(),
            Action::Crash { node } => format!("crash node {node}"),
            Action::CrashAt { node, seam } => {
                format!("crash node {node} {}", seam.label())
            }
            Action::Restart { node } => format!("restart node {node}"),
            Action::Propose {
                node,
                client,
                value,
                column,
            } => match column {
                Some(column) => {
                    format!(
                        "ask node {node} to choose {value} for client {client}, in column {column}"
                    )
                }
                None => format!("ask node {node} to choose {value} for client {client}"),
            },
            Action::StartElection { node } => format!("make node {node} campaign"),
            Action::SetElectionTimeout { node, ticks } => {
                format!("set the election timeout of node {node} to {ticks} ticks")
            }
            Action::ReadIndex { node, client } => match client {
                Some(client) => format!("ask node {node} to read for client {client}"),
                None => format!("ask node {node} to read"),
            },
            Action::QuorumRead { node, client } => match client {
                Some(client) => format!("ask node {node} for a quorum read for client {client}"),
                None => format!("ask node {node} for a quorum read"),
            },
            Action::Relinquish { node, to } => {
                format!("tell node {node} to hand its leadership to node {to}")
            }
            Action::Corrupt { node, slot } => {
                format!("rot the record of node {node} for slot {slot}")
            }
            Action::Wipe { node } => format!("erase the disk of node {node}"),
            Action::CrashMatchmaker { matchmaker } => format!("crash matchmaker {matchmaker}"),
            Action::RestartMatchmaker { matchmaker } => format!("restart matchmaker {matchmaker}"),
            Action::Reconfigure { node, members, .. } => {
                format!("ask node {node} to make {members:?} the acceptors")
            }
            Action::Retire {
                target,
                gc_watermark,
                ..
            } => match gc_watermark {
                Some(watermark) => format!(
                    "retire node {target} with the watermark {}.{}",
                    watermark.round, watermark.node
                ),
                None => format!("retire node {target} with no watermark"),
            },
            Action::ReconfigureMatchmakers { node, members } => {
                format!("ask node {node} to make the matchmakers {members:?}")
            }
            Action::ResendMatchmaking { node } => {
                format!("tell node {node} to ask the matchmakers again")
            }
            Action::ResendGc { node } => format!("tell node {node} to ask for the floor again"),
            Action::ResendReconfigurer { node } => {
                format!("tell node {node} to send its handover step again")
            }
            Action::Retry { node, client, seq } => {
                format!("retry write {seq} of client {client} at node {node}")
            }
            Action::Compact { node, up_to } => format!("compact node {node} up to slot {up_to}"),
            Action::ResendPending { node } => {
                format!("tell node {node} to re-send its pending Accepts")
            }
            Action::StepDown { node } => format!("make node {node} resign"),
            Action::OpenBallot { proposer, value } => {
                format!("tell proposer {proposer} to open a ballot for {value}")
            }
            Action::SetReach { phase, nodes } => {
                let phase = match phase {
                    Phase::One => "Phase 1",
                    Phase::Two => "Phase 2",
                };
                format!("let {phase} reach the acceptors {nodes:?}")
            }
            Action::Answer { choice, .. } => format!("answer {choice:?}"),
            Action::SetAutomation { flag, on } => {
                format!(
                    "turn the {:?} automation {}",
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
    /// The action names a column the configuration in force does not have.
    BadColumn,
    /// The node is not in a state a cooperative handoff may leave from, or
    /// the peer named cannot take one.
    HandoffRefused,
    /// The store was provisioned once and no longer carries its format
    /// marker: the node's promise is gone, and it may never rejoin.
    Amnesia,
    /// This deployment names no matchmakers, so it has no matchmaker set to
    /// change and no reconfiguration to honour.
    NoMatchmakers,
    /// The node is already driving a matchmaker-set handover.
    HandoverBusy,
    /// The node is driving no matchmaker-set handover.
    NoHandover,
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
