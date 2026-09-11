//! The log world: `ColocatedNode`s, their disks, the wire, and the clock the
//! player owns.
//!
//! This is `crates/paros-core/examples/quorum_read.rs` generalised. That example
//! is 45 lines of `drain` / `deliver` around a `Vec<ColocatedNode>`; this module
//! is the same two functions with the parts the example hard-codes turned into
//! player choices: which message lands next, which node ticks, who crashes, and
//! — when a level makes a role manual — what the role answers.
//!
//! # The drain contract
//!
//! After **any** call into a node, exactly once: `ready()`, copy the buckets
//! out, `advance()`, persist (`Truncate` held back), send, apply, flush the
//! truncates, serve the snapshot offers, answer the reads, `advance_recovery()`
//! — and drain again until the node is quiet. The `Ready` guard is never held
//! across anything else: not a disk write, not a prompt, not a player action.
//! It lives in the crate-private `drain` module, with the durability seams and
//! the two prompts that can hold a batch back.
//!
//! # Delivery and time are player choices
//!
//! Nothing leaves the wire unless the player delivers, drops or duplicates it
//! (or an automation does it for them). Delivering to a crashed node discards
//! the message. There is no latency model and no partition object: a partition
//! is the player not delivering. Likewise there is no clock — [`World::tick`]
//! advances one node, [`World::tick_all`] the whole pool.

pub mod decree;
pub mod disk;
mod drain;
mod history;
mod lifecycle;
pub mod matchmakers;
mod prompts;
mod reads;
mod render;
mod verbs;

use std::collections::{BTreeMap, BTreeSet};

use paros_core::matchmaking::Matchmaking;
use paros_core::proposer::RecoveryStep;
use paros_core::{
    AcceptorConfig, Ballot, ClientId, ColocatedNode, Command, Config, GcAck, GcRequest, MatchReply,
    MatchRequest, MatchmakerId, MatchmakerReconfigurer, Message, NodeId, QuorumSystem,
    ReconfigureReply, ReconfigureRequest, Slot,
};

use crate::action::{ActionError, ActionErrorCode, Seam};
use crate::narration;
use crate::narration::{NarrationEvent, NarrationKind, NodeSnapshot, say, who};
use crate::prompt::{Prompt, PromptKind};
use crate::view::{MessageView, message_view};
use crate::world::drain::Paused;
use crate::world::history::Client;
use crate::world::matchmakers::plane_view;

pub use disk::Disk;
pub use history::HistoryOp;
pub use matchmakers::{MatchmakerProcess, RegistryDisk};
pub use verbs::{CompactOutcome, HandoffOutcome, RetryAnswer, RetryOutcome};

/// The election timeout a hand-stepped leader holds.
///
/// `CheckQuorum` demotes a leader that spends a whole election-timeout window
/// without hearing an ack quorum — correct in production, and fatal to a game
/// where minutes pass between the player's moves. So while heartbeat delivery
/// is manual, the world parks a leader's timeout at this sentinel, exactly as
/// `paros-core`'s own `examples/quorum_read.rs` does; the moment
/// [`crate::auto::AutomationFlag::DeliverHeartbeats`] is on, the ack traffic
/// flows on every tick and the level's real timeout is restored.
pub const NO_CHECK_QUORUM: u64 = 1_000_000;

/// One endpoint of the wire.
///
/// Node ids and matchmaker ids are **different identity spaces**: node 1 and
/// matchmaker 1 are not the same party, and no guard anywhere may confuse them.
/// So the wire addresses a party rather than a number, and the operator's
/// crash verbs come in two families for the same reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Party {
    /// A node of the acceptor pool.
    Node(NodeId),
    /// A matchmaker of the registry tier.
    Matchmaker(MatchmakerId),
}

/// What one wire entry carries.
///
/// The node protocol and the matchmaker plane are two contracts, and the
/// registry tier is never stepped with a node's [`Message`]: a matchmaker holds
/// no log and votes on no slot. Keeping them apart in the type is what makes
/// that true by construction.
#[derive(Clone, Debug)]
pub enum Envelope {
    /// One node's message to another node.
    Node(Message),
    /// A candidate registers a ballot and its configuration.
    Match(MatchRequest),
    /// A matchmaker's answer to a registration.
    MatchReply(MatchReply),
    /// A leader asks a matchmaker to raise its watermark.
    Gc(GcRequest),
    /// A matchmaker's answer to a garbage-collection request.
    GcAck(GcAck),
    /// One step of a matchmaker-set handover.
    Reconfigure(ReconfigureRequest),
    /// A matchmaker's answer to a handover step.
    ReconfigureReply(ReconfigureReply),
}

/// One message in flight: what the wire is made of.
#[derive(Clone, Debug)]
pub struct InFlight {
    /// The id every player action names.
    pub id: u64,
    /// The sender.
    pub from: Party,
    /// The addressee.
    pub to: Party,
    /// What it carries — a real core message, never a game-local imitation.
    pub envelope: Envelope,
    /// The clock reading when it was queued.
    pub sent_at: u64,
}

impl InFlight {
    /// The node message this entry carries, if it carries one.
    #[must_use]
    pub fn message(&self) -> Option<&Message> {
        match &self.envelope {
            Envelope::Node(message) => Some(message),
            _ => None,
        }
    }

    /// The node this entry is addressed to, if it is addressed to one.
    #[must_use]
    pub fn to_node(&self) -> Option<NodeId> {
        self.to.node()
    }

    /// The node that sent this entry, if a node sent it.
    #[must_use]
    pub fn from_node(&self) -> Option<NodeId> {
        self.from.node()
    }

    /// Render it for the wire list and the stage, under the quorum system the
    /// **sender** runs: that is what decides which column an `Accept` was
    /// addressed to, and a message carries no column of its own.
    ///
    /// # Panics
    ///
    /// Never on a player-reachable path: every envelope is either a node
    /// message or one the matchmaker plane's renderer covers, and the match
    /// below is exhaustive over both.
    #[must_use]
    pub fn view(&self, system: QuorumSystem) -> MessageView {
        match &self.envelope {
            Envelope::Node(message) => message_view(
                self.id,
                self.from.number(),
                self.to.number(),
                self.sent_at,
                message,
                system,
            ),
            // Every other envelope is the matchmaker plane's, and none of them
            // names a slot or a column.
            _ => plane_view(self).expect("a matchmaker-plane entry renders"),
        }
    }
}

/// What the level and the automation flags make the world do for the player.
///
/// The [`crate::Game`] refreshes this from the [`crate::auto::Automation`]
/// before every action, so the world itself never reads the flag set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorldPolicy {
    /// The prompt kinds that are manual: the world parks and asks instead of
    /// stepping.
    pub manual: BTreeSet<PromptKind>,
    /// Whether a tick also re-sends a leader's pending `Accept`s.
    pub auto_resend: bool,
    /// Whether a leader's election timeout is parked at [`NO_CHECK_QUORUM`].
    pub hold_leadership: bool,
}

/// A narration the world owes the player once a prompt is answered.
///
/// The prompts that gate a **batch** — the persist order, the recovery page —
/// are asked about work `paros-core` has already done: the batch exists, and
/// the world is holding it. Narrating that batch while the question is open
/// would print the answer above the question, so the diff is deferred to the
/// point the node next reaches quiet, and taken from the state it was in
/// before the question was asked.
#[derive(Debug)]
struct Deferred {
    node: NodeId,
    before: NodeSnapshot,
    mark: usize,
}

/// The log world.
pub struct World {
    nodes: Vec<Option<ColocatedNode>>,
    disks: Vec<Disk>,
    wire: Vec<InFlight>,
    next_message_id: u64,
    clock: u64,
    pool: Vec<NodeId>,
    /// The per-node election timeout the level (or the player) asked for — what
    /// [`NO_CHECK_QUORUM`] is restored to.
    election_timeouts: Vec<u64>,
    armed_seams: Vec<Option<Seam>>,
    clients: Vec<Client>,
    policy: WorldPolicy,
    prompt: Option<Prompt>,
    paused: Option<Paused>,
    next_prompt_id: u64,
    next_read_ctx: u64,
    /// The highest promise each node has ever been seen to hold, live or on
    /// disk. A durable promise that ends up **below** its own watermark is the
    /// one thing a crash must never be able to do, and it is what the
    /// restart-safety levels are judged on.
    promise_watermarks: Vec<Ballot>,
    /// Every durability seam that actually cut a batch, in the order they
    /// fired: `(node, seam)`. A level's goal reads it to insist the player
    /// really visited the seam rather than talking about it.
    seams_fired: Vec<(NodeId, Seam)>,
    /// A story the narration is not allowed to tell yet: see [`Deferred`].
    deferred: Option<Deferred>,
    /// What the core will do with each slot of the recovery page about to be
    /// pumped — the `LeaderRecovery` prompt's oracle, read off a clone of the
    /// proposer *before* the call that pumps it. See
    /// [`World::plan_recovery`](crate::world::World::plan_recovery).
    recovery_plan: BTreeMap<Slot, RecoveryStep<Command>>,
    /// Every `Compact` the player asked for and what the leader answered.
    compacts: Vec<CompactOutcome>,
    /// Every client retry and what the leader answered it with.
    retries: Vec<RetryOutcome>,
    /// Every cooperative handoff that went through, in order.
    handoffs: Vec<HandoffOutcome>,
    /// How many `Heartbeat`s any leader has broadcast. A leaderless read is
    /// judged partly by this staying at zero, so it is counted where the beat
    /// is queued rather than looked for on a wire it has already left.
    beats_broadcast: u64,
    /// Every boot the engine refused because the node's disk was erased:
    /// `(node, the promise it last made)`.
    refused_boots: Vec<(NodeId, Ballot)>,
    /// The matchmakers this level deployed, in id order. Empty is plain
    /// Multi-Paxos, which is Act I to Act III and every Act IV level before
    /// this act's second half.
    matchmakers: Vec<MatchmakerProcess>,
    /// One handover driver per node. The reconfigurer is a **driver** object,
    /// not a role of the core's node: the node that drives a handover is the
    /// node the operator asked, and it holds no durable state of its own.
    reconfigurers: Vec<MatchmakerReconfigurer>,
    /// Per node, a second [`Matchmaking`] fed exactly the answers the node is
    /// fed — the oracle the `StaleConfiguration` prompt reads. `ColocatedNode`
    /// hands out no reference to its own, so this is the one prompt whose
    /// answer is computed on a parallel instance of the core's role rather
    /// than on a clone of the node's.
    matchmaking_shadow: Vec<Option<Matchmaking>>,
    /// Per node, the prior configurations its last completed matchmaking
    /// phase reported — `H_b`. `Election` keeps them private, so the world
    /// records what `MatchStep::Completed` handed it.
    campaign_prior: Vec<Vec<AcceptorConfig>>,
    /// Per node, whether an operator retired it for good.
    retired: Vec<bool>,
    /// Every retire request the engine refused for want of evidence:
    /// `(node, the watermark the operator showed)`.
    refused_retires: Vec<(NodeId, Ballot)>,
    /// Every retirement that went through: `(node, the watermark it retired
    /// on)`. The watermark is kept because a goal must be able to say that the
    /// shutdown rested on a floor a leadership really made effective.
    retirements: Vec<(NodeId, Ballot)>,
    /// Every handover a node gave up because it stopped making progress, in
    /// the order they were given up. Abandoning is a **driver** decision, so
    /// the world is the party that records it.
    abandoned_handovers: Vec<NodeId>,
    /// How many beats a handover phase may make no progress for before the
    /// driver gives it up. **Driver policy, never a constant inside the state
    /// machine**, so it is a level tunable. Its floor is structural: a phase
    /// must get more beats than one round trip needs, or a handover that is
    /// simply slow is abandoned every time. `0` disables it.
    reconfigure_timeout_ticks: u64,
    /// The history's own clock: a monotone counter stamped on every client
    /// operation as it is issued and again as it completes. It is **not** the
    /// tick clock — a level may never tick at all — and it never goes
    /// backwards, which is all a linearizability check needs from it.
    next_event: u64,
    /// What the action in progress has done so far, in Paxos. Cleared by the
    /// [`crate::Game`] before every action and drained after it.
    narration: Vec<NarrationEvent>,
}

impl World {
    /// A world of `configs.len()` fresh nodes, one per config, each with its
    /// own empty disk.
    ///
    /// # Panics
    ///
    /// If `configs` is empty, or two configs name the same node.
    #[must_use]
    pub fn new(configs: Vec<Config>, clients: &[u64], election_timeout: u64) -> Self {
        Self::from_disks(
            configs.into_iter().map(Disk::new).collect(),
            clients,
            election_timeout,
        )
    }

    /// A world booted from disks a level pre-seeded.
    ///
    /// # Panics
    ///
    /// If `disks` is empty, or two disks name the same node.
    #[must_use]
    pub fn from_disks(disks: Vec<Disk>, clients: &[u64], election_timeout: u64) -> Self {
        assert!(!disks.is_empty(), "a world has at least one node");
        let pool: Vec<NodeId> = disks.iter().map(|d| d.config().id).collect();
        let mut sorted = pool.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert!(sorted.len() == pool.len(), "node ids are distinct");
        let reconfigurers: Vec<MatchmakerReconfigurer> = pool
            .iter()
            .copied()
            .map(MatchmakerReconfigurer::new)
            .collect();
        let nodes: Vec<Option<ColocatedNode>> = disks
            .iter()
            .map(|disk| Some(ColocatedNode::new(disk)))
            .collect();
        let count = disks.len();
        let mut world = Self {
            nodes,
            disks,
            wire: Vec::new(),
            next_message_id: 1,
            clock: 0,
            pool,
            election_timeouts: vec![election_timeout; count],
            armed_seams: vec![None; count],
            clients: clients
                .iter()
                .map(|id| Client {
                    id: ClientId(*id),
                    next_seq: 1,
                    proposals: Vec::new(),
                    reads: Vec::new(),
                })
                .collect(),
            policy: WorldPolicy::default(),
            prompt: None,
            paused: None,
            next_prompt_id: 1,
            next_read_ctx: 1,
            promise_watermarks: vec![Ballot::zero(); count],
            seams_fired: Vec::new(),
            deferred: None,
            recovery_plan: BTreeMap::new(),
            compacts: Vec::new(),
            retries: Vec::new(),
            handoffs: Vec::new(),
            beats_broadcast: 0,
            refused_boots: Vec::new(),
            matchmakers: Vec::new(),
            reconfigurers: reconfigurers.into_iter().collect(),
            matchmaking_shadow: (0..count).map(|_| None).collect(),
            campaign_prior: vec![Vec::new(); count],
            retired: vec![false; count],
            refused_retires: Vec::new(),
            retirements: Vec::new(),
            abandoned_handovers: Vec::new(),
            reconfigure_timeout_ticks: 0,
            next_event: 1,
            narration: Vec::new(),
        };
        for index in 0..count {
            if let Some(node) = world.nodes[index].as_mut() {
                node.set_election_timeout(election_timeout);
            }
        }
        world
    }

    /// Deploy `matchmakers` beside the pool: the registry tier this level's
    /// nodes name in their configuration.
    ///
    /// A level whose disks name no matchmakers must not call this, and a level
    /// that names them must: the two halves are one deployment, and a node
    /// that names a matchmaker nobody deployed would campaign into silence.
    ///
    /// # Panics
    ///
    /// If two matchmakers share an id.
    #[must_use]
    pub fn with_matchmakers(mut self, matchmakers: Vec<MatchmakerProcess>) -> Self {
        let mut ids: Vec<MatchmakerId> = matchmakers.iter().map(MatchmakerProcess::id).collect();
        let count = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert!(ids.len() == count, "matchmaker ids are distinct");
        self.matchmakers = matchmakers;
        self
    }

    /// How many beats a handover phase may stall for before the driver gives
    /// it up (see the handover stall timeout).
    #[must_use]
    pub fn with_reconfigure_timeout(mut self, ticks: u64) -> Self {
        self.reconfigure_timeout_ticks = ticks;
        self
    }

    /// Install the policy the automation flags imply.
    pub fn set_policy(&mut self, policy: WorldPolicy) {
        self.policy = policy;
        self.settle();
    }

    /// The policy in force.
    #[must_use]
    pub fn policy(&self) -> &WorldPolicy {
        &self.policy
    }

    /// What has been said since the last [`World::clear_narration`].
    #[must_use]
    pub fn narration(&self) -> &[NarrationEvent] {
        &self.narration
    }

    /// Start a fresh action's narration.
    pub fn clear_narration(&mut self) {
        self.narration.clear();
    }

    /// Take this action's narration.
    pub fn take_narration(&mut self) -> Vec<NarrationEvent> {
        std::mem::take(&mut self.narration)
    }

    /// Say one line.
    pub(crate) fn narrate(&mut self, kind: NarrationKind, text: impl Into<String>) {
        self.narration.push(say(kind, text));
    }

    /// Say one already-built line.
    pub(crate) fn narration_push(&mut self, event: NarrationEvent) {
        self.narration.push(event);
    }

    /// Run `f` and narrate what it did to `id`, entirely from the diff of the
    /// node's own role accessors and the messages its batches sent.
    ///
    /// This is the whole derivation: no caller tells the narration what it is
    /// about to do, and a call that changes nothing says nothing.
    pub(crate) fn observe<R>(&mut self, id: NodeId, f: impl FnOnce(&mut Self) -> R) -> R {
        // A batch the core has already produced can be held back by a prompt
        // (see `drain::gate_batch`). The story of that batch *is the answer to
        // the question*, so it waits with the batch: the diff is deferred, and
        // taken from where the node stood before the question was asked.
        let (before, mark) = match self.deferred.take() {
            Some(held) if held.node == id => (held.before, held.mark),
            _ => (NodeSnapshot::capture(self.node(id)), self.wire.len()),
        };
        // How many acceptors a decision is counted against: the **configuration
        // in force at this node**, not the pool it happens to be deployed in.
        // They coincide today, and the line the narration prints ("2 of 3
        // acceptors voted") is a claim about the configuration.
        let members = self
            .node(id)
            .map_or_else(|| self.pool.len(), |node| node.acceptors().members().len());
        let system = self.system(id);
        let out = f(self);
        if self.prompt.is_some() {
            self.deferred = Some(Deferred {
                node: id,
                before,
                mark,
            });
            return out;
        }
        let after = NodeSnapshot::capture(self.node(id));
        // Only the node protocol's own messages: the matchmaker plane has its
        // own narration, because the diff of a node's role accessors says
        // nothing about a registry.
        let sent: Vec<(NodeId, Message)> = self.wire[mark..]
            .iter()
            .filter(|entry| entry.from == Party::Node(id))
            .filter_map(|entry| match (&entry.envelope, entry.to) {
                (Envelope::Node(message), Party::Node(to)) => Some((to, message.clone())),
                _ => None,
            })
            .collect();
        let events = narration::describe(id, &before, &after, &sent, system, members);
        self.narration.extend(events);
        out
    }

    /// The open prompt, if any.
    #[must_use]
    pub fn prompt(&self) -> Option<&Prompt> {
        self.prompt.as_ref()
    }

    /// Logical time.
    #[must_use]
    pub fn clock(&self) -> u64 {
        self.clock
    }

    /// Everything in flight.
    #[must_use]
    pub fn wire(&self) -> &[InFlight] {
        &self.wire
    }

    /// The addressable pool, in id order.
    #[must_use]
    pub fn pool(&self) -> &[NodeId] {
        &self.pool
    }

    /// The quorum system in force at `id`: what the node's own configuration
    /// says, or — for a node that is not running — what its disk says. Every
    /// quorum question in the world goes through a configuration, never
    /// through a count.
    #[must_use]
    pub fn system(&self, id: NodeId) -> QuorumSystem {
        let Some(index) = self.index_of(id) else {
            return QuorumSystem::Majority;
        };
        self.nodes[index].as_ref().map_or_else(
            || self.disks[index].config().quorum_system,
            |node| node.acceptors().quorum_system(),
        )
    }

    /// Render one in-flight message under its sender's quorum system. A
    /// matchmaker runs none of its own — its quorums are majorities of a
    /// matchmaker set, and nothing it sends names a column.
    #[must_use]
    pub fn render(&self, entry: &InFlight) -> MessageView {
        let system = entry
            .from
            .node()
            .map_or(QuorumSystem::Majority, |id| self.system(id));
        entry.view(system)
    }

    /// The live node with `id`, if it is running.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&ColocatedNode> {
        self.index_of(id).and_then(|i| self.nodes[i].as_ref())
    }

    /// The disk of the node with `id`.
    #[must_use]
    pub fn disk(&self, id: NodeId) -> Option<&Disk> {
        self.index_of(id).map(|i| &self.disks[i])
    }

    /// The leader, as the cluster's own nodes report it.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.pool
            .iter()
            .copied()
            .find(|id| self.node(*id).is_some_and(ColocatedNode::is_leader))
    }

    /// Re-derive everything that is not the core's business: which writes the
    /// admitting node has applied, and the leadership hold.
    fn settle(&mut self) {
        let mut stamp = self.next_event;
        for client in &mut self.clients {
            for proposal in &mut client.proposals {
                if proposal.acked {
                    continue;
                }
                let Some(index) = self.pool.iter().position(|id| *id == proposal.node) else {
                    continue;
                };
                if let Some(node) = self.nodes[index].as_ref()
                    && let Some(at) = node.replica().applied_at(client.id, proposal.seq)
                {
                    proposal.slot = Some(at);
                    proposal.acked = true;
                    proposal.acked_at = Some(stamp);
                    stamp += 1;
                }
            }
        }
        self.next_event = stamp;
        for index in 0..self.pool.len() {
            let seen = self.nodes[index]
                .as_ref()
                .map_or_else(
                    || self.disks[index].hard_state().max_promised_ballot,
                    |node| node.acceptor().promised(),
                )
                .max(self.disks[index].hard_state().max_promised_ballot);
            self.promise_watermarks[index] = self.promise_watermarks[index].max(seen);
        }
        for index in 0..self.pool.len() {
            self.sync_matchmaking_shadow(index);
        }
        let hold = self.policy.hold_leadership;
        for index in 0..self.pool.len() {
            let restore = self.election_timeouts[index];
            let Some(node) = self.nodes[index].as_mut() else {
                continue;
            };
            if hold && node.is_leader() {
                if node.election_timeout() != NO_CHECK_QUORUM {
                    node.set_election_timeout(NO_CHECK_QUORUM);
                }
            } else if node.election_timeout() == NO_CHECK_QUORUM && restore != NO_CHECK_QUORUM {
                node.set_election_timeout(restore);
            }
        }
    }

    /// The next reading of the history's monotone counter.
    fn take_event(&mut self) -> u64 {
        let event = self.next_event;
        self.next_event += 1;
        event
    }

    fn take_prompt_id(&mut self) -> u64 {
        let id = self.next_prompt_id;
        self.next_prompt_id += 1;
        id
    }

    fn index_of(&self, id: NodeId) -> Option<usize> {
        self.pool.iter().position(|node| *node == id)
    }

    fn position_of(&self, id: u64) -> Result<usize, ActionError> {
        self.wire
            .iter()
            .position(|entry| entry.id == id)
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownMessage,
                    format!("there is no message #{id} in flight"),
                )
            })
    }

    fn require_live(&self, id: NodeId) -> Result<usize, ActionError> {
        let index = self.index_of(id).ok_or_else(|| unknown_node(id))?;
        if self.nodes[index].is_none() {
            return Err(ActionError::new(
                ActionErrorCode::NodeCrashed,
                format!("node {} is crashed; restart it first", id.0),
            ));
        }
        Ok(index)
    }

    fn require_no_prompt(&self) -> Result<(), ActionError> {
        if let Some(prompt) = &self.prompt {
            return Err(ActionError::new(
                ActionErrorCode::PromptOpen,
                format!("answer the open question first: {}", prompt.question),
            ));
        }
        Ok(())
    }
}

/// How the game names one endpoint of the wire.
#[must_use]
pub fn name(party: Party) -> String {
    match party {
        Party::Node(id) => who(id),
        Party::Matchmaker(id) => matchmakers::which(id),
    }
}

fn unknown_node(id: NodeId) -> ActionError {
    ActionError::new(
        ActionErrorCode::UnknownNode,
        format!("there is no node {} in this world", id.0),
    )
}

/// The `quorum_system` string a [`crate::view::NodeView`] carries.
#[must_use]
pub fn quorum_name(system: paros_core::QuorumSystem) -> String {
    match system {
        paros_core::QuorumSystem::Majority => "majority".to_string(),
        paros_core::QuorumSystem::Flexible { q1, q2 } => format!("flexible q1={q1} q2={q2}"),
        paros_core::QuorumSystem::Grid { rows, cols } => format!("grid {rows}x{cols}"),
    }
}
