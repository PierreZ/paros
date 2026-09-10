//! The one contract the browser reads.
//!
//! Every type here derives `Serialize` and [`ts_rs::TS`], and
//! `cargo test -p paros-play` writes the generated TypeScript into
//! `web/play/src/generated/`. Nothing in the frontend reaches past these
//! structs into the engine, and nothing here is stateful: a [`GameView`] is
//! derived from the world on every read, so the renderer never has to keep
//! animation state of its own.
//!
//! Three rendering conventions, fixed here so every surface agrees.
//!
//! A **ballot** is `round.node` ([`show_ballot`]). A **value is plain text
//! everywhere** — in a prompt, in a narration line, in a goal, and in the view
//! ([`show_command`] and [`value_text`] agree, and neither ever prints Rust's
//! `Debug` quoting): the frontend sets a value in its monospace face, which is
//! what tells the reader "this is the client's bytes", and a caption that said
//! `Accept "alpha"` beside a slot box that said `alpha` was reading as two
//! different things. A **control command names itself in words** (`Noop`,
//! `Truncate up to 3`), and the view carries that name beside the control's own
//! discriminant ([`control_kind`]) so the renderer styles a `Truncate`
//! differently from a client's `alpha` without parsing either.

use paros_core::{Ballot, Command, Control, NodeRole, QuorumSystem, Slot};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::action::{ActionKind, Seam};
use crate::auto::AutomationFlag;
use crate::narration::NarrationKind;
use crate::prompt::{Choice, PromptKind};

/// A ballot as `round.node` — the notation the book and the levels use.
#[must_use]
pub fn show_ballot(ballot: Ballot) -> String {
    format!("{}.{}", ballot.round, ballot.node.0)
}

/// A command **in prose** — a prompt, a narration line, a goal.
///
/// The same plain text the view carries ([`value_text`]): a client value is its
/// UTF-8 bytes as written, and a control command names itself in words. There
/// is one convention, and this function exists only to say where a value is
/// being read rather than rendered.
#[must_use]
pub fn show_command(command: &Command) -> String {
    value_text(command)
}

/// A command **in the view**: plain text, never Rust's `Debug` quoting. A
/// client entry is its UTF-8 bytes as written; a control command is the label
/// the stage prints inside its slot box.
#[must_use]
pub fn value_text(command: &Command) -> String {
    match command {
        Command::User(entry) => String::from_utf8_lossy(&entry.value.0).into_owned(),
        Command::Control(Control::Noop) => "Noop".to_string(),
        Command::Control(Control::Truncate { up_to }) => format!("Truncate up to {}", up_to.0),
        Command::Control(Control::Snap { at_index }) => format!("Snap at {}", at_index.0),
    }
}

/// Which control command this is, or `None` for an opaque client entry — the
/// discriminant the renderer colours by, so it never has to read
/// [`value_text`].
#[must_use]
pub fn control_kind(command: &Command) -> Option<String> {
    match command {
        Command::User(_) => None,
        Command::Control(Control::Noop) => Some("noop".to_string()),
        Command::Control(Control::Truncate { .. }) => Some("truncate".to_string()),
        Command::Control(Control::Snap { .. }) => Some("snap".to_string()),
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
    /// What the **last** action did, derived from the transition itself. It is
    /// cleared and rebuilt on every action; the whole stream is kept per log
    /// entry in [`ActionView::narration`].
    pub narration: Vec<NarrationView>,
}

/// One narration line: the game saying what just happened, in Paxos.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct NarrationView {
    /// What it is about — the caption's colour, and the log's grouping.
    pub kind: NarrationKind,
    /// The sentence, with this transition's own numbers in it.
    pub text: String,
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
    /// The field-guide page for this level's mechanism: a **bare book
    /// filename** (`stable-leader.html`), never a path. The game is served from
    /// `/play/` beside the book, so the frontend prefixes `../` to link it.
    pub field_guide: String,
    /// The `paros-core` symbols this level names, for the reference panel.
    pub symbols: Vec<String>,
    /// The actions this level offers.
    pub allowed_actions: Vec<ActionKind>,
    /// The automation flags passing this level unlocks for later ones — what
    /// the "reward" line in the briefing panel names.
    pub unlocks: Vec<AutomationFlag>,
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
    /// The single-decree world's phase reach sets — which acceptors a
    /// `Prepare` and an `Accept` currently get to. `None` in the log world,
    /// which has no reach: a partition there is the player not delivering.
    pub reach: Option<ReachView>,
}

/// Which acceptors each phase's messages reach (the Act I world's only
/// network).
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ReachView {
    /// The acceptors a `Prepare` reaches.
    pub one: Vec<u64>,
    /// The acceptors an `Accept` reaches.
    pub two: Vec<u64>,
}

/// The single-decree world's one decision.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ChosenView {
    /// The chosen value's plain text (see [`value_text`]).
    pub value: String,
    /// Which control command it is, or `None` for a client entry.
    pub control: Option<String>,
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

/// A log node's role in the current ballot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RoleView {
    /// It follows whoever it believes leads.
    Follower,
    /// Its Phase 1 is in flight.
    Candidate,
    /// It won a promise quorum and runs Phase 2 alone.
    Leader,
}

/// Where a single-decree proposer's attempt has got to. The Act I world's
/// proposers hold no role in the log sense, so this is what the stage prints
/// under them instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum AttemptView {
    /// It has not opened a ballot yet.
    Idle,
    /// Phase 1 is in flight.
    Phase1,
    /// Phase 2 is in flight.
    Phase2,
    /// An acceptor refused it: a higher ballot is promised somewhere.
    Preempted,
    /// Its value was chosen.
    Won,
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
    /// Its role in the current ballot; `None` for a crashed node and for the
    /// Act I world's bare roles, whose [`NodeView::flavour`] already says what
    /// they are.
    pub role: Option<RoleView>,
    /// Where a single-decree proposer's attempt has got to; `None` for every
    /// other node.
    pub attempt: Option<AttemptView>,
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
    /// The same quorum system, as numbers the stage can draw.
    pub quorum: QuorumSystemView,
    /// Where this acceptor sits in the grid, when the configuration runs one.
    pub grid_cell: Option<GridCellView>,
    /// A one-line summary of what this node's application has applied.
    pub applied: Vec<SlotView>,
    /// An armed durability seam, if the player set one.
    pub armed_seam: Option<Seam>,
    /// The ballot the acceptor configuration in force here was bound to, as
    /// `round.node`. A configuration is never edited: it belongs to one
    /// ballot, and this is that ballot.
    pub acceptors_since: Option<String>,
    /// The matchmaker set this node believes authoritative. `None` on a plain
    /// deployment, which names no matchmakers at all.
    pub matchmakers: Option<MatchmakerSetView>,
    /// The open matchmaking phase, if this node is a candidate that has not
    /// finished registering.
    pub matchmaking: Option<MatchmakingView>,
    /// The garbage-collection floor this leadership made effective.
    pub gc: Option<GcView>,
    /// Which step of a matchmaker-set handover this node is driving.
    pub handover: Option<HandoverPhaseView>,
    /// Whether an operator retired this node: it answered the evidence, shut
    /// down, and it never comes back.
    pub retired: bool,
}

/// Which family of quorums a configuration counts with.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum QuorumKindView {
    /// Any group of more than half the acceptors, in both phases.
    Majority,
    /// A split: `q1` acceptors answer Phase 1 and `q2` vote in Phase 2.
    Flexible,
    /// A grid: a full row answers Phase 1 and a full column votes in Phase 2.
    Grid,
}

/// The quorum system a configuration runs, as a structure rather than a
/// sentence: the renderer draws a grid from `rows` and `cols` and prints a
/// split from `q1` and `q2`, and never parses
/// [`NodeView::quorum_system`](NodeView::quorum_system).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct QuorumSystemView {
    /// Which family this is.
    pub kind: QuorumKindView,
    /// How many acceptors answer Phase 1 (a flexible split's `q1`).
    pub q1: Option<u64>,
    /// How many acceptors vote in Phase 2 (a flexible split's `q2`).
    pub q2: Option<u64>,
    /// The grid's rows: how many Phase-1 quorums there are.
    pub rows: Option<u64>,
    /// The grid's columns: how many Phase-2 quorums there are.
    pub cols: Option<u64>,
}

/// Where one acceptor sits in a grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct GridCellView {
    /// The row this acceptor is in. Its row is a Phase-1 quorum.
    pub row: u64,
    /// The column this acceptor is in. Its column is a Phase-2 quorum.
    pub column: u64,
}

/// A count for the view. Every count here is a configuration size, so the
/// saturation is unreachable; it exists so no cast can truncate one.
fn as_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// The structured form of `system`. Every number is a fact the configuration
/// itself carries; nothing here is a threshold the game computed.
#[must_use]
pub fn quorum_view(system: QuorumSystem) -> QuorumSystemView {
    match system {
        QuorumSystem::Majority => QuorumSystemView {
            kind: QuorumKindView::Majority,
            q1: None,
            q2: None,
            rows: None,
            cols: None,
        },
        QuorumSystem::Flexible { q1, q2 } => QuorumSystemView {
            kind: QuorumKindView::Flexible,
            q1: Some(as_u64(q1)),
            q2: Some(as_u64(q2)),
            rows: None,
            cols: None,
        },
        QuorumSystem::Grid { rows, cols } => QuorumSystemView {
            kind: QuorumKindView::Grid,
            q1: None,
            q2: None,
            rows: Some(as_u64(rows)),
            cols: Some(as_u64(cols)),
        },
    }
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
    /// The command's plain text (see [`value_text`]) — no Rust quoting.
    pub value: String,
    /// Which control command it is (`noop`, `truncate`, `snap`), or `None` for
    /// an opaque client entry.
    pub control: Option<String>,
    /// Whether this node knows the slot is chosen.
    pub chosen: bool,
    /// Whether this node has applied it.
    pub applied: bool,
}

/// Which tier a message's endpoint belongs to. Node ids and matchmaker ids are
/// **different identity spaces**, so a renderer must be told which one a number
/// names before it can draw the link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum PartyView {
    /// A node of the acceptor pool.
    Node,
    /// A matchmaker of the registry tier.
    Matchmaker,
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
    /// Which tier [`MessageView::from`] names.
    pub from_party: PartyView,
    /// The addressee.
    pub to: u64,
    /// Which tier [`MessageView::to`] names.
    pub to_party: PartyView,
    /// The ballot it carries, as `round.node`.
    pub ballot: Option<String>,
    /// The slot it names.
    pub slot: Option<u64>,
    /// The column an `Accept` (or the `Accepted` that answers it) was
    /// addressed to, when the sender runs a grid. Every full column is a
    /// Phase-2 quorum, and a slot's column is `slot % cols`.
    pub column: Option<u64>,
    /// A one-line description for the wire list.
    pub summary: String,
    /// The render family: `prepare`, `promise`, `accept`, `accepted`, `nack`,
    /// `commit`, `heartbeat`, `catchup`, `snapshot`, `read`, `handoff`,
    /// `match`, `gc`, `reconfigure`.
    pub phase: String,
    /// Whether this message **answers** one (a `Promise`, an `Accepted`, a
    /// `Nack`, an ack, a catch-up or snapshot reply) rather than asking
    /// something. The stage draws the two directions differently, and this is
    /// the fact it draws from — never the variant's name.
    pub reply: bool,
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
    /// The command's text, exactly as the client wrote it — no Rust quoting.
    pub value: String,
    /// The node it was sent to.
    pub node: u64,
    /// The slot it was admitted at.
    pub slot: Option<u64>,
    /// Whether that node has applied it (the write is acknowledged).
    pub acked: bool,
    /// Whether the client may send this write again.
    ///
    /// Always true for a write, and that is a property of the protocol rather
    /// than of this write's state: a retry repeats the same
    /// `(client, sequence number, bytes)`, and the leader answers it from two
    /// ledgers in a fixed order — applied here (ack the slot it executed at),
    /// chosen or in flight at a slot (wait on **that** slot), never seen (take
    /// the next free slot). So a write that was acknowledged, one still
    /// waiting, and one whose answer was lost are all safe to ask for again,
    /// and none of them can be executed twice.
    pub retryable: bool,
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
    /// Always false: a read carries no identity the cluster remembers, so
    /// asking again is a **new** read at a new watermark, not a retry. The
    /// field is here so the frontend has one rule for every client operation.
    pub retryable: bool,
}

/// Where a matchmaker stands in the generation it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum MatchmakerPhaseView {
    /// A fresh store: nothing has ever been written here.
    Fresh,
    /// Authoritative for no generation: a spare, or a proposed member whose
    /// decree has not been decided yet.
    Inactive,
    /// Serving matchmaking for its generation.
    Active,
    /// Frozen for its generation: it registers nothing more, it still votes in
    /// the successor decree, and it points late candidates at the successor.
    Stopped,
}

/// Why a registration was made: what a candidate believed, or what an operator
/// changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationKindView {
    /// A candidate registered the configuration it believed was in force.
    Belief,
    /// A leader registered an operator's explicit change. The highest of these
    /// is the effective configuration.
    Reconfiguration,
}

/// One matchmaker set: which generation, and who is in it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct MatchmakerSetView {
    /// The generation number. Generation 0 is the deployment's bootstrap set.
    pub generation: u64,
    /// The matchmakers in it, in id order.
    pub members: Vec<u64>,
}

/// One `ballot -> configuration` record in a matchmaker's registry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RegistrationView {
    /// The ballot the configuration was registered under, as `round.node`.
    pub ballot: String,
    /// The acceptors of that configuration, in id order.
    pub members: Vec<u64>,
    /// Whether the record is a belief or a reconfiguration.
    pub kind: RegistrationKindView,
}

/// One matchmaker: a registry, never an acceptor.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct MatchmakerView {
    /// The matchmaker's id.
    pub id: u64,
    /// False while it is crashed (its registry survives).
    pub alive: bool,
    /// The generation it holds.
    pub generation: u64,
    /// Where it stands in that generation.
    pub phase: MatchmakerPhaseView,
    /// The floor below which it has forgotten its registrations for good, as
    /// `round.node`.
    pub gc_watermark: String,
    /// Its registry, in ballot order.
    pub registrations: Vec<RegistrationView>,
    /// The chosen successor of its generation, once it has learned one.
    pub successor: Option<MatchmakerSetView>,
}

/// A candidate's open matchmaking phase: what it is registering, and how many
/// matchmakers still have to answer.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct MatchmakingView {
    /// The ballot being registered, as `round.node`.
    pub ballot: String,
    /// The acceptor configuration this campaign intends to run the ballot
    /// with, in id order.
    pub config: Vec<u64>,
    /// Whether the campaign registers a belief or a reconfiguration.
    pub kind: RegistrationKindView,
    /// How many more matchmakers must answer before the quorum holds.
    pub remaining: usize,
}

/// A leader's garbage-collection floor, once a matchmaker quorum has made it
/// effective. `None` before that: nothing is retirable until the acks are in.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct GcView {
    /// The floor a matchmaker quorum durably acked, as `round.node`.
    pub effective_watermark: String,
    /// The acceptors this floor released: they answer no future Phase 1.
    pub retirable: Vec<u64>,
}

/// Which step of a matchmaker-set handover a node is driving.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum HandoverPhaseView {
    /// Freezing a quorum of the current generation.
    Stopping,
    /// Handing the reconstructed registry to every proposed member.
    Bootstrapping,
    /// Running the successor decree over the current generation.
    Deciding,
    /// Telling both generations which set was chosen.
    Publishing,
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
    /// What this action did, in Paxos — kept per entry so the log panel shows
    /// the whole stream, not only the latest caption.
    pub narration: Vec<NarrationView>,
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
            value: value_text(command),
            control: control_kind(command),
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
            value: value_text(command),
            control: control_kind(command),
            chosen: true,
            applied: true,
        }
    }
}

/// A matchmaker set, rendered.
#[must_use]
pub fn matchmaker_set_view(set: &paros_core::MatchmakerSet) -> MatchmakerSetView {
    MatchmakerSetView {
        generation: set.generation.0,
        members: set.members().iter().map(|m| m.0).collect(),
    }
}

/// The [`RegistrationKindView`] for a core registration kind.
#[must_use]
pub fn registration_kind_view(kind: paros_core::RegistrationKind) -> RegistrationKindView {
    match kind {
        paros_core::RegistrationKind::Belief => RegistrationKindView::Belief,
        paros_core::RegistrationKind::Reconfiguration => RegistrationKindView::Reconfiguration,
    }
}

/// One registry record, rendered.
#[must_use]
pub fn registration_view(
    ballot: Ballot,
    registration: &paros_core::Registration,
) -> RegistrationView {
    RegistrationView {
        ballot: show_ballot(ballot),
        members: registration.config.members().iter().map(|n| n.0).collect(),
        kind: registration_kind_view(registration.kind),
    }
}

/// The [`MatchmakerPhaseView`] for a core matchmaker phase.
#[must_use]
pub fn matchmaker_phase_view(phase: paros_core::MatchmakerPhase) -> MatchmakerPhaseView {
    match phase {
        paros_core::MatchmakerPhase::Fresh => MatchmakerPhaseView::Fresh,
        paros_core::MatchmakerPhase::Inactive => MatchmakerPhaseView::Inactive,
        paros_core::MatchmakerPhase::Active => MatchmakerPhaseView::Active,
        paros_core::MatchmakerPhase::Stopped => MatchmakerPhaseView::Stopped,
    }
}

/// The [`RoleView`] for a [`NodeRole`].
#[must_use]
pub fn show_role(role: NodeRole) -> RoleView {
    match role {
        NodeRole::Follower => RoleView::Follower,
        NodeRole::Candidate => RoleView::Candidate,
        NodeRole::Leader => RoleView::Leader,
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
    system: QuorumSystem,
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
    // The column is a Phase-2 fact, so only the two Phase-2 messages carry
    // one, and only under a grid: `QuorumSystem::column_of` answers `None`
    // for every other system, which is exactly "the whole membership".
    let column = match message {
        M::Accept { slot, .. } | M::Accepted { slot, .. } => system.column_of(*slot),
        _ => None,
    };
    MessageView {
        id,
        kind: kind.to_string(),
        from,
        from_party: PartyView::Node,
        to,
        to_party: PartyView::Node,
        ballot: ballot.map(show_ballot),
        slot: slot.map(|s| s.0),
        column: column.map(as_u64),
        summary,
        phase: phase.to_string(),
        reply: is_reply(message),
        sent_at,
    }
}

/// Whether a message answers one. Stated over the variants rather than over
/// their names, so a renderer never has to guess from a string.
fn is_reply(message: &paros_core::Message) -> bool {
    use paros_core::Message as M;
    matches!(
        message,
        M::Promise { .. }
            | M::Accepted { .. }
            | M::Nack { .. }
            | M::HeartbeatAck { .. }
            | M::CatchUpResponse { .. }
            | M::InstallSnapshot { .. }
            | M::SnapAck { .. }
            | M::SnapChunkResponse { .. }
            | M::PreReadAck { .. }
    )
}
