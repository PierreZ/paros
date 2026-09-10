//! The one contract the browser reads.
//!
//! Every type here derives `Serialize` and [`ts_rs::TS`], and
//! `cargo test -p paros-play` writes the generated TypeScript into
//! `web/play/src/generated/`. Nothing in the frontend reaches past these
//! structs into the engine, and nothing here is stateful: a [`GameView`] is
//! derived from the world on every read, so the renderer never has to keep
//! animation state of its own.
//!
//! Two rendering conventions, fixed here so every surface agrees: a **ballot**
//! is `round.node` ([`show_ballot`]), and a **value** is its UTF-8 text
//! ([`show_command`]) — a control command names itself instead.

use paros_core::{Ballot, Command, Control, NodeRole, Slot};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::action::{ActionKind, Seam};
use crate::auto::AutomationFlag;
use crate::prompt::{Choice, PromptKind};

/// A ballot as `round.node` — the notation the book and the levels use.
#[must_use]
pub fn show_ballot(ballot: Ballot) -> String {
    format!("{}.{}", ballot.round, ballot.node.0)
}

/// A command as the player sees it: a client value is its UTF-8 text, a
/// control command names itself.
#[must_use]
pub fn show_command(command: &Command) -> String {
    match command {
        Command::User(entry) => format!("{:?}", String::from_utf8_lossy(&entry.value.0)),
        Command::Control(Control::Noop) => "Noop".to_string(),
        Command::Control(Control::Truncate { up_to }) => format!("Truncate(up to {})", up_to.0),
        Command::Control(Control::Snap { at_index }) => format!("Snap(at {})", at_index.0),
    }
}

/// Everything the browser renders for one frame.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct GameView {
    /// The level being played.
    pub level: LevelView,
    /// The world, derived fresh.
    pub world: WorldView,
    /// The question the world is waiting on, if any.
    pub prompt: Option<PromptView>,
    /// Whether the level's goal is reached.
    pub goal: GoalView,
    /// The action log, oldest first — also the undo stack.
    pub log: Vec<ActionView>,
    /// The automation toggles.
    pub automation: AutomationView,
    /// How many prompts the player got wrong in this attempt.
    pub mistakes: u32,
}

/// The level's static text and rules.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct LevelView {
    /// The stable id, e.g. `act1/choose-a-value`.
    pub id: String,
    /// Which act it belongs to.
    pub act: u8,
    /// The level's title.
    pub title: String,
    /// The briefing, in markdown.
    pub briefing: String,
    /// A link into the book's field guide.
    pub field_guide: String,
    /// The `paros-core` symbols this level names, for the reference panel.
    pub symbols: Vec<String>,
    /// The actions this level offers.
    pub allowed_actions: Vec<ActionKind>,
    /// A hint, once the player has earned one.
    pub hint: Option<String>,
}

/// Which world a level runs in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum WorldFlavour {
    /// Act I: bare `Proposer` + `Acceptor` roles over slot 0.
    Decree,
    /// Act II onward: `ColocatedNode`s with disks and a clock.
    Log,
}

/// The world.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct WorldView {
    /// Which world kind this is.
    pub flavour: WorldFlavour,
    /// Logical time: how many ticks the player has spent.
    pub clock: u64,
    /// Every node, in id order.
    pub nodes: Vec<NodeView>,
    /// Everything in flight, oldest first.
    pub wire: Vec<MessageView>,
    /// The clients and what they are waiting for.
    pub clients: Vec<ClientView>,
    /// The matchmakers (Act IV; empty until then).
    pub matchmakers: Vec<MatchmakerView>,
    /// The single-decree world's decision, if it has one.
    pub chosen: Option<ChosenView>,
}

/// The single-decree world's one decision.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ChosenView {
    /// The chosen value's text.
    pub value: String,
    /// The ballot it was chosen at, as `round.node`.
    pub ballot: String,
}

/// What a node is: an acceptor, a proposer, or a node that colocates both.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum NodeFlavour {
    /// A bare `Acceptor` role (Act I).
    Acceptor,
    /// A bare `Proposer` role (Act I).
    Proposer,
    /// A `ColocatedNode`.
    Colocated,
}

/// One node.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct NodeView {
    /// The node's id.
    pub id: u64,
    /// Which roles it holds.
    pub flavour: NodeFlavour,
    /// False while it is crashed (its disk survives).
    pub alive: bool,
    /// `follower`, `candidate` or `leader`; `None` for a bare role.
    pub role: Option<String>,
    /// Its operating ballot, as `round.node`.
    pub ballot: Option<String>,
    /// The node it believes is leader.
    pub leader: Option<u64>,
    /// Its durable promise, as `round.node`.
    pub promised: Option<String>,
    /// Its accepted log, above the compaction floor.
    pub accepted: Vec<SlotView>,
    /// The contiguous chosen prefix's last slot.
    pub chosen_index: Option<u64>,
    /// The first slot outside the contiguous chosen prefix.
    pub first_unchosen: Option<u64>,
    /// The allocator frontier.
    pub next_slot: Option<u64>,
    /// A chosen slot stranded above a hole: `(hole, highest)`.
    pub chosen_gap: Option<GapView>,
    /// The compaction floor: the first slot still retained.
    pub floor: Option<u64>,
    /// The election clock.
    pub election: Option<ElectionView>,
    /// Slots with an in-flight Phase-2 round.
    pub open_rounds: Vec<u64>,
    /// Whether this leader has accepts it could re-send.
    pub pending_accepts: bool,
    /// Read-index rounds awaiting confirmation.
    pub read_rounds: Vec<ReadRoundView>,
    /// Slots the fresh leadership has still to recover.
    pub recovery_remaining: usize,
    /// The acceptor configuration in force here.
    pub acceptors: Vec<u64>,
    /// Its quorum system, e.g. `majority`.
    pub quorum_system: String,
    /// A one-line summary of what this node's application has applied.
    pub applied: Vec<SlotView>,
    /// An armed durability seam, if the player set one.
    pub armed_seam: Option<Seam>,
}

/// A chosen slot stranded above a hole in the contiguous prefix.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, TS)]
pub struct GapView {
    /// The first slot missing from the prefix.
    pub hole: u64,
    /// The highest slot above it already known chosen.
    pub highest: u64,
}

/// The election clock.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, TS)]
pub struct ElectionView {
    /// The timeout in ticks.
    pub timeout: u64,
    /// Whether the timeout is the no-check-quorum sentinel a hand-stepped
    /// leader holds (see [`crate::world::NO_CHECK_QUORUM`]).
    pub held: bool,
}

/// A read-index round waiting for its ack quorum.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, TS)]
pub struct ReadRoundView {
    /// The client's correlation token.
    pub ctx: u64,
    /// The captured read index.
    pub index: Option<u64>,
    /// How many acks are credited to it.
    pub acks: usize,
}

/// One slot of a node's log.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct SlotView {
    /// The slot number.
    pub slot: u64,
    /// The ballot the record was accepted at, as `round.node`.
    pub ballot: Option<String>,
    /// The command's text.
    pub value: String,
    /// Whether this node knows the slot is chosen.
    pub chosen: bool,
    /// Whether this node has applied it.
    pub applied: bool,
}

/// One message in flight.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct MessageView {
    /// The id an action names.
    pub id: u64,
    /// The `Message` variant's name, e.g. `Prepare`.
    pub kind: String,
    /// The sender.
    pub from: u64,
    /// The addressee.
    pub to: u64,
    /// The ballot it carries, as `round.node`.
    pub ballot: Option<String>,
    /// The slot it names.
    pub slot: Option<u64>,
    /// A one-line description for the wire list.
    pub summary: String,
    /// The render family: `prepare`, `promise`, `accept`, `accepted`, `nack`,
    /// `commit`, `heartbeat`, `catchup`, `snapshot`, `read`, `handoff`,
    /// `match`, `gc`, `reconfigure`.
    pub phase: String,
    /// The clock reading when it was queued.
    pub sent_at: u64,
}

/// A client and what it is waiting for.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ClientView {
    /// The client's id.
    pub id: u64,
    /// Its writes.
    pub proposals: Vec<ProposalView>,
    /// Its reads.
    pub reads: Vec<ReadView>,
}

/// One client write.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ProposalView {
    /// The client's sequence number.
    pub seq: u64,
    /// The command's text.
    pub value: String,
    /// The node it was sent to.
    pub node: u64,
    /// The slot it was admitted at.
    pub slot: Option<u64>,
    /// Whether that node has applied it (the write is acknowledged).
    pub acked: bool,
}

/// One client read.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ReadView {
    /// The correlation token.
    pub ctx: u64,
    /// The node it was sent to.
    pub node: u64,
    /// The index it was served at, once served.
    pub index: Option<u64>,
    /// Whether it has been served.
    pub served: bool,
}

/// A matchmaker. Act IV fills this in; the field exists so the view contract
/// does not change under the frontend when it lands.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct MatchmakerView {
    /// The matchmaker's id.
    pub id: u64,
}

/// The open prompt.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct PromptView {
    /// The prompt's id, which the answer must name.
    pub id: u64,
    /// Which question.
    pub kind: PromptKind,
    /// The node whose role is being played.
    pub node: u64,
    /// The question, with this prompt's own numbers in it.
    pub question: String,
    /// The state the decision rests on.
    pub state_summary: Vec<String>,
    /// The answers on offer.
    pub choices: Vec<Choice>,
    /// The explanation for the last wrong answer.
    pub feedback: Option<String>,
}

/// Whether the level's goal is reached.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GoalView {
    /// Not yet.
    Open {
        /// What the player is working toward.
        detail: String,
    },
    /// Done.
    Reached {
        /// What was proved.
        detail: String,
    },
    /// The level cannot be finished from here; reset or undo.
    Failed {
        /// What went wrong.
        detail: String,
    },
}

/// One entry of the action log.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ActionView {
    /// Its position in the log.
    pub index: usize,
    /// Its family.
    pub kind: ActionKind,
    /// A one-line description.
    pub label: String,
}

/// The automation toggles.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct AutomationView {
    /// Every flag this level knows about.
    pub flags: Vec<AutomationFlagView>,
}

/// One toggle.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct AutomationFlagView {
    /// The flag.
    pub flag: AutomationFlag,
    /// Its label.
    pub label: String,
    /// Whether it is on.
    pub on: bool,
    /// Whether this level offers it as a toggle.
    pub unlocked: bool,
    /// Whether this level forbids turning it on.
    pub pinned_off: bool,
}

/// A level, as the level map lists it.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct LevelSummary {
    /// The stable id.
    pub id: String,
    /// Which act.
    pub act: u8,
    /// The title.
    pub title: String,
    /// The automation flags passing it unlocks.
    pub unlocks: Vec<AutomationFlag>,
}

/// A refusal the wasm surface hands back instead of a view.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ErrorView {
    /// The stable machine-readable reason.
    pub code: String,
    /// A sentence for the player.
    pub error: String,
}

// ---- builders ---------------------------------------------------------------

impl SlotView {
    /// A slot of an accepted log.
    #[must_use]
    pub fn accepted(
        slot: Slot,
        ballot: Ballot,
        command: &Command,
        chosen: bool,
        applied: bool,
    ) -> Self {
        Self {
            slot: slot.0,
            ballot: Some(show_ballot(ballot)),
            value: show_command(command),
            chosen,
            applied,
        }
    }

    /// A slot of an application's applied log.
    #[must_use]
    pub fn applied_entry(slot: Slot, command: &Command) -> Self {
        Self {
            slot: slot.0,
            ballot: None,
            value: show_command(command),
            chosen: true,
            applied: true,
        }
    }
}

/// The `role` string for a [`NodeRole`].
#[must_use]
pub fn show_role(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Follower => "follower",
        NodeRole::Candidate => "candidate",
        NodeRole::Leader => "leader",
    }
}

/// Render one in-flight message.
///
/// `phase` is the render family the SVG stage colours by, and it is
/// deliberately coarser than the variant: `CatchUpRequest` and
/// `CatchUpResponse` are one family, so are the four snapshot messages.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn message_view(
    id: u64,
    from: u64,
    to: u64,
    sent_at: u64,
    message: &paros_core::Message,
) -> MessageView {
    use paros_core::Message as M;
    let (kind, phase, ballot, slot, summary) = match message {
        M::Prepare {
            ballot, from_slot, ..
        } => (
            "Prepare",
            "prepare",
            Some(*ballot),
            Some(*from_slot),
            format!(
                "Prepare at {} for slots from {}",
                show_ballot(*ballot),
                from_slot.0
            ),
        ),
        M::Promise {
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
            ..
        } => (
            "Promise",
            "promise",
            Some(*ballot),
            Some(*from_slot),
            format!(
                "Promise {} from slot {}: {} accepted, {} faulty{}",
                show_ballot(*ballot),
                from_slot.0,
                accepted.len(),
                faulty.len(),
                next_from_slot.map_or(String::new(), |s| format!(", more from {}", s.0))
            ),
        ),
        M::Accept {
            ballot,
            slot,
            command,
            ..
        } => (
            "Accept",
            "accept",
            Some(*ballot),
            Some(*slot),
            format!(
                "Accept {} at {} for slot {}",
                show_command(command),
                show_ballot(*ballot),
                slot.0
            ),
        ),
        M::Accepted { ballot, slot, .. } => (
            "Accepted",
            "accepted",
            Some(*ballot),
            Some(*slot),
            format!("Accepted slot {} at {}", slot.0, show_ballot(*ballot)),
        ),
        M::Nack { ballot, slot, .. } => (
            "Nack",
            "nack",
            Some(*ballot),
            Some(*slot),
            format!("Nack {} at slot {}", show_ballot(*ballot), slot.0),
        ),
        M::Commit {
            ballot,
            slot,
            command,
            ..
        } => (
            "Commit",
            "commit",
            Some(*ballot),
            Some(*slot),
            format!(
                "Commit slot {} = {} at {}",
                slot.0,
                show_command(command),
                show_ballot(*ballot)
            ),
        ),
        M::CatchUpRequest { from_slot, .. } => (
            "CatchUpRequest",
            "catchup",
            None,
            Some(*from_slot),
            format!("Catch me up from slot {}", from_slot.0),
        ),
        M::CatchUpResponse { entries, .. } => (
            "CatchUpResponse",
            "catchup",
            None,
            entries.keys().next().copied(),
            format!("Catch-up: {} decided slot(s)", entries.len()),
        ),
        M::InstallSnapshot {
            ballot,
            chosen_index,
            ..
        } => (
            "InstallSnapshot",
            "snapshot",
            Some(*ballot),
            Some(*chosen_index),
            format!(
                "InstallSnapshot at slot {} ({})",
                chosen_index.0,
                show_ballot(*ballot)
            ),
        ),
        M::SnapAck { at_index, .. } => (
            "SnapAck",
            "snapshot",
            None,
            Some(*at_index),
            format!("SnapAck at slot {}", at_index.0),
        ),
        M::SnapChunkRequest {
            at_index, chunks, ..
        } => (
            "SnapChunkRequest",
            "snapshot",
            None,
            Some(*at_index),
            format!(
                "{} chunk(s) of the snapshot at {}",
                chunks.len(),
                at_index.0
            ),
        ),
        M::SnapChunkResponse {
            at_index, chunks, ..
        } => (
            "SnapChunkResponse",
            "snapshot",
            None,
            Some(*at_index),
            format!(
                "{} chunk(s) for the snapshot at {}",
                chunks.len(),
                at_index.0
            ),
        ),
        M::Relinquish {
            ballot, next_slot, ..
        } => (
            "Relinquish",
            "handoff",
            Some(*ballot),
            Some(*next_slot),
            format!(
                "Relinquish {} with the frontier at {}",
                show_ballot(*ballot),
                next_slot.0
            ),
        ),
        M::Heartbeat {
            ballot,
            commit,
            seq,
            ..
        } => (
            "Heartbeat",
            "heartbeat",
            Some(*ballot),
            *commit,
            format!(
                "Heartbeat {} #{seq}, commit {}",
                show_ballot(*ballot),
                commit.map_or_else(|| "nothing".to_string(), |s| s.0.to_string())
            ),
        ),
        M::HeartbeatAck { ballot, seq, .. } => (
            "HeartbeatAck",
            "heartbeat",
            Some(*ballot),
            None,
            format!("HeartbeatAck {} #{seq}", show_ballot(*ballot)),
        ),
        M::PreRead { ctx, .. } => (
            "PreRead",
            "read",
            None,
            None,
            format!("PreRead ctx {ctx}: how high have you voted?"),
        ),
        M::PreReadAck { ctx, watermark, .. } => (
            "PreReadAck",
            "read",
            None,
            *watermark,
            format!(
                "PreReadAck ctx {ctx}: voted up to {}",
                watermark.map_or_else(|| "nothing".to_string(), |s| s.0.to_string())
            ),
        ),
        _ => ("Other", "other", None, None, "a message".to_string()),
    };
    MessageView {
        id,
        kind: kind.to_string(),
        from,
        to,
        ballot: ballot.map(show_ballot),
        slot: slot.map(|s| s.0),
        summary,
        phase: phase.to_string(),
        sent_at,
    }
}
