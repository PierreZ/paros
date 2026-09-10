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
mod prompts;
mod render;

use std::collections::{BTreeMap, BTreeSet};

use paros_core::proposer::RecoveryStep;
use paros_core::{
    Ballot, ClientId, ClientSeq, ColocatedNode, Command, Config, Control, HANDOFF_BATCH,
    LeadershipOrigin, Message, NodeId, ProposeResult, QuorumSystem, ReadIndexResult, ReadState,
    Slot, Value,
};

use crate::action::{ActionError, ActionErrorCode, Seam};
use crate::narration;
use crate::narration::{NarrationEvent, NarrationKind, NodeSnapshot, many, say, who};
use crate::prompt::{Prompt, PromptKind, Verdict};
use crate::view::{MessageView, message_view, show_ballot};
use crate::world::drain::Paused;

pub use disk::Disk;

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

/// One message in flight: what the wire is made of.
#[derive(Clone, Debug)]
pub struct InFlight {
    /// The id every player action names.
    pub id: u64,
    /// The sender.
    pub from: NodeId,
    /// The addressee.
    pub to: NodeId,
    /// The message itself — a real [`Message`], never a game-local imitation.
    pub message: Message,
    /// The clock reading when it was queued.
    pub sent_at: u64,
}

impl InFlight {
    /// Render it for the wire list and the stage, under the quorum system the
    /// **sender** runs: that is what decides which column an `Accept` was
    /// addressed to, and a message carries no column of its own.
    #[must_use]
    pub fn view(&self, system: QuorumSystem) -> MessageView {
        message_view(
            self.id,
            self.from.0,
            self.to.0,
            self.sent_at,
            &self.message,
            system,
        )
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

/// One client's writes.
#[derive(Clone, Debug)]
struct Proposal {
    seq: ClientSeq,
    value: String,
    node: NodeId,
    slot: Option<Slot>,
    acked: bool,
    /// When the client issued it, on the history's own monotone counter.
    issued: u64,
    /// When it was acknowledged, on the same counter.
    acked_at: Option<u64>,
}

/// One client's reads.
#[derive(Clone, Debug)]
struct PendingRead {
    ctx: u64,
    node: NodeId,
    /// The index the round captured, recorded here because
    /// `Proposer::read_rounds` exposes no accessor for it.
    index: Option<Slot>,
    /// The beat sequence an ack must carry to count, for display only.
    required_seq: u64,
    /// Who has acked a qualifying beat since the read opened — the world's own
    /// tally, for the prompt's summary. The *judgement* always comes from a
    /// clone of the real `Proposer`.
    acks: BTreeSet<NodeId>,
    /// Whether this is a **leaderless** read: a Phase-1 quorum's vote
    /// watermarks rather than a leader's beat acks. The two are served through
    /// the same `ReadState`, and only the client knows which it asked for.
    leaderless: bool,
    served: bool,
    /// When the client asked, on the history's own monotone counter.
    issued: u64,
    /// When it was answered, on the same counter.
    served_at: Option<u64>,
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

/// What a leader answered one `Compact` request with.
///
/// The refusal is the interesting one, and it is not a failure: a `Truncate`
/// may only be proposed once a **quorum holds a decided snapshot point**
/// covering it, because past that floor the log is gone and the snapshot is
/// the only thing left to recover a stranded node from. A request no point
/// covers seeds the next point instead and is answered `accepted: false`; the
/// client retries once the marker is decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactOutcome {
    /// The node the client asked.
    pub node: NodeId,
    /// The prefix the client asked to drop, inclusive.
    pub requested: Slot,
    /// Whether a `Truncate` was proposed.
    pub accepted: bool,
    /// The decided snapshot point a quorum held, if any — what the request was
    /// clamped to.
    pub covered: Option<Slot>,
    /// Whether the refusal seeded a fresh `Snap` marker.
    pub seeded_marker: bool,
}

/// What the leader answered one client **retry** with.
///
/// The three answers are the three things the leader can honestly know about a
/// `(client, seq)`: it executed it, it holds it at a slot but has not executed
/// it yet, or it has never heard of it. Which one it gives comes from the two
/// dedup tables, consulted in that order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryAnswer {
    /// Applied here: the ack names the slot it executed at.
    Applied(Slot),
    /// Chosen or in flight at a slot, not executed here yet: the client waits
    /// on that slot.
    InFlight(Slot),
    /// Never seen: it takes the next free slot.
    Fresh(Slot),
    /// The node could not answer at all (it is not the leader any more).
    Refused,
}

/// One retry and what it was answered with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryOutcome {
    /// The node the client asked.
    pub node: NodeId,
    /// The client's id.
    pub client: u64,
    /// The sequence number being retried.
    pub seq: u64,
    /// What the leader answered.
    pub answer: RetryAnswer,
}

/// One cooperative handoff that went through: what the successor was handed.
///
/// A **refused** handoff is not here. It is an [`ActionError`] with the reason
/// in it, because the refusal is about the state the leader is in, and the
/// state is what a goal reads back (see [`World::handoff_refusal`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandoffOutcome {
    /// The leader that gave the authority up.
    pub from: NodeId,
    /// The peer that was offered it.
    pub to: NodeId,
    /// The ballot that travelled — the successor runs Phase 2 under this very
    /// ballot, with no Phase 1 of its own.
    pub ballot: Ballot,
    /// The allocator frontier that travelled with it: the first slot the
    /// successor hands out, and the fence its reads sit behind until its own
    /// prefix reaches it.
    pub next_slot: Slot,
    /// Tail slots handed over as already chosen.
    pub decided: usize,
    /// Tail slots handed over as still-open Phase-2 rounds.
    pub pending: usize,
}

/// One client-visible operation, as the linearizability judge reads it.
///
/// The client is the only party that knows its own program order, so this is
/// recorded **client-side**: what it asked, when it asked, when it was
/// answered, and the one number the answer carries — the slot a write landed
/// at, or the watermark a read observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryOp {
    /// Whose operation it is.
    pub client: u64,
    /// The node it was sent to.
    pub node: NodeId,
    /// True for a write, false for a read.
    pub write: bool,
    /// A write's slot once acknowledged, or a read's watermark once served.
    /// `None` on a read means the empty prefix.
    pub at: Option<Slot>,
    /// When it was issued, on the history's monotone counter.
    pub started: u64,
    /// When it completed, or `None` while it is still outstanding. An
    /// outstanding operation constrains nothing: it may still complete later.
    pub completed: Option<u64>,
}

/// One client.
#[derive(Clone, Debug)]
struct Client {
    id: ClientId,
    next_seq: u64,
    proposals: Vec<Proposal>,
    reads: Vec<PendingRead>,
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

    // ---- policy ------------------------------------------------------------

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

    // ---- narration ---------------------------------------------------------

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

    /// Note that a durability seam actually cut a batch.
    pub(crate) fn record_seam(&mut self, id: NodeId, seam: Seam) {
        self.seams_fired.push((id, seam));
        let text = match seam {
            Seam::BeforeSync => format!(
                "{} dies before the flush. The batch is gone whole: nothing was written and \
                 nothing was sent, so its disk is exactly what it was — which is why this seam \
                 is always safe.",
                who(id)
            ),
            Seam::AfterSyncBeforeSend => format!(
                "{} dies after the flush and before the send. The writes are durable and the \
                 messages are lost: it now holds a promise (or a vote) that nobody in the \
                 cluster has ever heard about. That is the safe half of the seam — the \
                 dangerous half is the other order.",
                who(id)
            ),
        };
        self.narrate(NarrationKind::Crash, text);
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
        let sent: Vec<(NodeId, Message)> = self.wire[mark..]
            .iter()
            .filter(|entry| entry.from == id)
            .map(|entry| (entry.to, entry.message.clone()))
            .collect();
        let events = narration::describe(id, &before, &after, &sent, system, members);
        self.narration.extend(events);
        out
    }

    // ---- read views used by goals and the frontend --------------------------

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

    /// Render one in-flight message under its sender's quorum system.
    #[must_use]
    pub fn render(&self, entry: &InFlight) -> MessageView {
        entry.view(self.system(entry.from))
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

    /// Whether every client write that was admitted has been applied by the
    /// node that admitted it.
    #[must_use]
    pub fn all_writes_acked(&self) -> bool {
        self.clients
            .iter()
            .flat_map(|c| c.proposals.iter())
            .all(|p| p.acked)
    }

    /// Every read a client asked for that has been served, as
    /// `(ctx, index)`.
    #[must_use]
    pub fn served_reads(&self) -> Vec<(u64, Option<Slot>)> {
        self.clients
            .iter()
            .flat_map(|c| c.reads.iter())
            .filter(|r| r.served)
            .map(|r| (r.ctx, r.index))
            .collect()
    }

    /// The node whose durable promise sits **below** the highest promise it was
    /// ever seen to hold — the regression a crash must never cause.
    ///
    /// `None` is the invariant holding. This is the game's own bookkeeping, not
    /// the core's: the core cannot regress a promise, and the point of the
    /// restart levels is to watch that hold across a crash the player chose.
    ///
    /// An **erased** disk is not counted, and that is the whole point of the
    /// wipe: its promise really is below the one it made, which is exactly why
    /// the node may never come back. The invariant this reports on is about
    /// nodes that *do* come back, and the engine's boot refusal is what keeps
    /// a wiped one out of that set.
    #[must_use]
    pub fn promise_regressed(&self) -> Option<NodeId> {
        self.pool
            .iter()
            .copied()
            .enumerate()
            .find_map(|(index, id)| {
                let disk = &self.disks[index];
                if disk.provisioned() && !disk.is_formatted() {
                    return None;
                }
                let durable = disk.hard_state().max_promised_ballot;
                (durable < self.promise_watermarks[index]).then_some(id)
            })
    }

    /// The highest promise `id` has ever been seen to hold.
    #[must_use]
    pub fn promise_watermark(&self, id: NodeId) -> Option<Ballot> {
        self.index_of(id)
            .map(|index| self.promise_watermarks[index])
    }

    /// Every durability seam that cut a batch, in firing order.
    #[must_use]
    pub fn seams_fired(&self) -> &[(NodeId, Seam)] {
        &self.seams_fired
    }

    /// The highest slot a client write has been acknowledged at — the write a
    /// later linearizable read must not read behind.
    #[must_use]
    pub fn highest_acked_slot(&self) -> Option<Slot> {
        self.clients
            .iter()
            .flat_map(|client| client.proposals.iter())
            .filter(|proposal| proposal.acked)
            .filter_map(|proposal| proposal.slot)
            .max()
    }

    /// Every read a client asked for, as `(node, index, served)`.
    #[must_use]
    pub fn reads(&self) -> Vec<(NodeId, Option<Slot>, bool)> {
        self.clients
            .iter()
            .flat_map(|client| client.reads.iter())
            .map(|read| (read.node, read.index, read.served))
            .collect()
    }

    /// The clients this level gave the player, in id order.
    #[must_use]
    pub fn clients(&self) -> Vec<u64> {
        self.clients.iter().map(|client| client.id.0).collect()
    }

    /// Every client operation, in the order it was issued — the history the
    /// linearizability judge reads.
    #[must_use]
    pub fn history(&self) -> Vec<HistoryOp> {
        let mut ops: Vec<HistoryOp> = Vec::new();
        for client in &self.clients {
            for proposal in &client.proposals {
                ops.push(HistoryOp {
                    client: client.id.0,
                    node: proposal.node,
                    write: true,
                    at: proposal.slot.filter(|_| proposal.acked),
                    started: proposal.issued,
                    completed: proposal.acked_at,
                });
            }
            for read in &client.reads {
                ops.push(HistoryOp {
                    client: client.id.0,
                    node: read.node,
                    write: false,
                    at: read.index.filter(|_| read.served),
                    started: read.issued,
                    completed: read.served_at,
                });
            }
        }
        ops.sort_by_key(|op| (op.started, op.client, !op.write));
        ops
    }

    /// Judge the recorded history by the three conditions a totally ordered
    /// log needs — no search, because the log *is* the order:
    ///
    /// 1. a committed read observes every write acknowledged before it began;
    /// 2. watermarks never move backwards across non-overlapping reads;
    /// 3. a write issued after a committed read lands **above** that read's
    ///    watermark.
    ///
    /// Operations that never completed constrain nothing: a write whose ack
    /// never arrived may still be chosen later, and that is not a violation of
    /// anything.
    ///
    /// `Ok(())` is the history being linearizable so far.
    ///
    /// # Errors
    ///
    /// The sentence naming the condition that failed and the two operations
    /// that failed it.
    pub fn linearizable(&self) -> Result<(), String> {
        let ops = self.history();
        let reads: Vec<&HistoryOp> = ops
            .iter()
            .filter(|op| !op.write && op.completed.is_some())
            .collect();
        let writes: Vec<&HistoryOp> = ops
            .iter()
            .filter(|op| op.write && op.completed.is_some())
            .collect();
        for read in &reads {
            let began = read.started;
            for write in &writes {
                let Some(acked) = write.completed else {
                    continue;
                };
                if acked < began && write.at > read.at {
                    return Err(format!(
                        "client {}'s read at node {} observed {}, while client {}'s write at {} \
                         had already been acknowledged. A read never goes behind a write that \
                         completed before it began.",
                        read.client,
                        read.node.0,
                        at(read.at),
                        write.client,
                        at(write.at)
                    ));
                }
            }
        }
        for earlier in &reads {
            let Some(done) = earlier.completed else {
                continue;
            };
            for later in &reads {
                if later.started >= done && later.at < earlier.at {
                    return Err(format!(
                        "a read at node {} observed {} after a read at node {} had already \
                         observed {}. A watermark never moves backwards.",
                        later.node.0,
                        at(later.at),
                        earlier.node.0,
                        at(earlier.at)
                    ));
                }
            }
        }
        for read in &reads {
            let Some(done) = read.completed else { continue };
            for write in &writes {
                if write.started > done && write.at <= read.at {
                    return Err(format!(
                        "client {}'s write landed at {}, at or below the {} a read at node {} \
                         had already observed — a write issued after a read must land above it.",
                        write.client,
                        at(write.at),
                        at(read.at),
                        read.node.0
                    ));
                }
            }
        }
        Ok(())
    }

    /// Every node's compaction floor, in pool order.
    #[must_use]
    pub fn floors(&self) -> Vec<(NodeId, Slot)> {
        self.pool
            .iter()
            .copied()
            .enumerate()
            .map(|(index, id)| (id, self.disks[index].floor()))
            .collect()
    }

    /// Every node's retained decided snapshot point, in pool order.
    #[must_use]
    pub fn snapshot_points(&self) -> Vec<(NodeId, Option<Slot>)> {
        self.pool
            .iter()
            .copied()
            .enumerate()
            .map(|(index, id)| (id, self.disks[index].snapshot_point()))
            .collect()
    }

    /// Every `Compact` the player asked for and what the leader answered.
    #[must_use]
    pub fn compacts(&self) -> &[CompactOutcome] {
        &self.compacts
    }

    /// Every client retry and what the leader answered it with.
    #[must_use]
    pub fn retries(&self) -> &[RetryOutcome] {
        &self.retries
    }

    /// Every cooperative handoff that went through, in order.
    #[must_use]
    pub fn handoffs(&self) -> &[HandoffOutcome] {
        &self.handoffs
    }

    /// How many beats any leader has broadcast since the level began.
    #[must_use]
    pub fn beats_broadcast(&self) -> u64 {
        self.beats_broadcast
    }

    /// Every boot the engine refused because the node's disk was erased.
    #[must_use]
    pub fn refused_boots(&self) -> &[(NodeId, Ballot)] {
        &self.refused_boots
    }

    /// The records `id` holds whose value it has lost, with the ballot each
    /// was accepted at — what its next `Promise` reports as *faulty*, and
    /// never as "nothing accepted here".
    #[must_use]
    pub fn faulty_records(&self, id: NodeId) -> Vec<(Slot, Ballot)> {
        let Some(index) = self.index_of(id) else {
            return Vec::new();
        };
        self.nodes[index].as_ref().map_or_else(
            || {
                self.disks[index]
                    .faulty()
                    .iter()
                    .map(|(slot, ballot)| (*slot, *ballot))
                    .collect()
            },
            |node| {
                node.acceptor()
                    .faulty()
                    .iter()
                    .map(|(slot, ballot)| (*slot, *ballot))
                    .collect()
            },
        )
    }

    /// How many slots `id`'s repair probe is still blocked on.
    #[must_use]
    pub fn blocked_repairs(&self, id: NodeId) -> usize {
        self.node(id).map_or(0, ColocatedNode::blocked_repairs)
    }

    /// Why `id` may not hand its leadership on, or `None` when it may.
    ///
    /// Every reason is read off the node itself — the role, where the
    /// leadership came from, what Phase-1-shaped work is open, how long the
    /// tail is — and the last word is the core's own
    /// [`ColocatedNode::can_relinquish`]. Both the refusal a player sees and
    /// the goal that watches for one read this.
    #[must_use]
    pub fn handoff_refusal(&self, id: NodeId) -> Option<String> {
        let Some(node) = self.node(id) else {
            return Some(format!("node {} is not running", id.0));
        };
        if node.can_relinquish() {
            return None;
        }
        if !node.is_leader() {
            return Some(format!("node {} is not the leader", id.0));
        }
        if let LeadershipOrigin::Handoff { from } = node.leadership_origin() {
            return Some(format!(
                "node {} did not win ballot {} — node {} handed it over. An authority moves \
                 once: only the node that minted a ballot may pass it on, because a replayed \
                 hand-off would otherwise install it at a node that had already given it up, \
                 beside the node exercising it now. Hold an election instead.",
                id.0,
                show_ballot(node.ballot()),
                from.0
            ));
        }
        if node.proposer().recovery().is_some()
            || node.proposer().probe().is_some()
            || node.proposer().election().is_some()
            || node.replica().app_repair().is_some()
            || !node.acceptor().faulty().is_empty()
        {
            return Some(format!(
                "node {} still has work that only a promise quorum can finish: an inherited slot \
                 to settle, a damaged record to repair, or an application prefix to pull. A \
                 successor runs no Phase 1, so it could not finish any of it. An election can.",
                id.0
            ));
        }
        let tail = node
            .proposer()
            .next_slot()
            .0
            .saturating_sub(node.replica().first_unchosen().0);
        if tail > u64::try_from(HANDOFF_BATCH).unwrap_or(u64::MAX) {
            return Some(format!(
                "node {}'s tail is {tail} slots long, and a hand-off carries at most {}. The \
                 successor must be told about every slot below the frontier, or it would never \
                 propose the ones it was not told about.",
                id.0, HANDOFF_BATCH
            ));
        }
        if node.acceptors().members().len() <= 1 {
            return Some(format!(
                "node {} has nobody to hand its leadership to",
                id.0
            ));
        }
        Some(format!(
            "node {} is not in a state a hand-off may leave from",
            id.0
        ))
    }

    /// Whether some live node's chosen prefix sits **below** another node's
    /// compaction floor — a node truncation has stranded, which only a
    /// snapshot can rescue.
    #[must_use]
    pub fn stranded(&self) -> Vec<NodeId> {
        let highest_floor = self.disks.iter().map(Disk::floor).max().unwrap_or(Slot(0));
        self.pool
            .iter()
            .copied()
            .enumerate()
            .filter(|(index, _)| {
                let next = self.disks[*index]
                    .hard_state()
                    .chosen_index
                    .map_or(Slot(0), |s| Slot(s.0 + 1));
                next < highest_floor
            })
            .map(|(_, id)| id)
            .collect()
    }

    /// The reads a client asked for that have not been served.
    #[must_use]
    pub fn unserved_reads(&self) -> usize {
        self.clients
            .iter()
            .flat_map(|c| c.reads.iter())
            .filter(|r| !r.served)
            .count()
    }

    // ---- the wire ----------------------------------------------------------

    /// Deliver the in-flight message `id`.
    ///
    /// A message addressed to a crashed node is discarded: this is the whole
    /// failure model for "the message arrived at a machine that is not there".
    /// A message whose addressee has a manual role parks instead, and the
    /// world raises the prompt.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn deliver(&mut self, id: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let position = self.position_of(id)?;
        let entry = self.wire.remove(position);
        let summary = self.render(&entry).summary;
        let Some(index) = self.index_of(entry.to) else {
            return Ok(());
        };
        if self.nodes[index].is_none() {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "{summary} reaches {}, which is not running, so it is discarded. A message \
                     to a machine that is not there is simply lost — that is the whole of this \
                     failure model.",
                    who(entry.to)
                ),
            );
            return Ok(());
        }
        self.note_ack(&entry);
        if let Some(prompt) = self.prompt_for(entry.to, &entry.message) {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "{summary} stops at {}: {} You answer for it, and the real state machine \
                     marks the answer.",
                    who(entry.to),
                    prompt.question
                ),
            );
            self.prompt = Some(prompt);
            self.paused = Some(Paused::Message {
                node: entry.to,
                message: Box::new(entry.message),
            });
            return Ok(());
        }
        self.step(entry.to, entry.message);
        Ok(())
    }

    /// Drop the in-flight message `id`.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn drop_message(&mut self, id: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let position = self.position_of(id)?;
        let summary = self.render(&self.wire[position].clone()).summary;
        self.wire.remove(position);
        self.narrate(
            NarrationKind::Info,
            format!(
                "{summary} is lost. There is no partition object in this game: a partition is \
                 you not delivering, and the protocol may not assume the difference."
            ),
        );
        Ok(())
    }

    /// Put a second copy of the in-flight message `id` on the wire.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn duplicate(&mut self, id: u64, to: Option<u64>) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let position = self.position_of(id)?;
        let mut copy = self.wire[position].clone();
        copy.id = self.next_message_id;
        if let Some(to) = to {
            let to = NodeId(to);
            if self.index_of(to).is_none() {
                return Err(unknown_node(to));
            }
            copy.to = to;
        }
        let summary = self.render(&copy).summary;
        self.next_message_id += 1;
        copy.sent_at = self.clock;
        let misrouted = copy.to;
        self.wire.push(copy);
        self.narrate(
            NarrationKind::Info,
            match to {
                None => format!(
                    "A second copy of {summary} is on the wire. Every rule in the protocol is \
                     stated so that a message arriving twice changes nothing the first arrival \
                     did not."
                ),
                Some(_) => format!(
                    "A copy of {summary} is on the wire, addressed to {} instead. Networks \
                     misroute messages, and the protocol answers for it: every guard asks who a \
                     message is *from* and what the configuration says about them. No guard \
                     asks what the transport did with it.",
                    who(misrouted)
                ),
            },
        );
        Ok(())
    }

    /// The next message an automation pump would deliver: the lowest-id
    /// heartbeat (when `beats`) or reply (when `replies`) on the wire.
    #[must_use]
    pub fn next_auto_delivery(&self, beats: bool, replies: bool) -> Option<u64> {
        self.wire
            .iter()
            .filter(|entry| match &entry.message {
                Message::Heartbeat { .. } | Message::HeartbeatAck { .. } => beats,
                Message::Promise { .. } | Message::Accepted { .. } | Message::Nack { .. } => {
                    replies
                }
                _ => false,
            })
            .map(|entry| entry.id)
            .min()
    }

    // ---- time --------------------------------------------------------------

    /// Advance one node's clock by one tick (and re-send its pending accepts
    /// when [`WorldPolicy::auto_resend`] is on).
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn tick(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.clock += 1;
        let resend = self.policy.auto_resend;
        self.narrate_tick(id, index);
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.tick();
                if resend {
                    node.resend_pending();
                }
            }
            world.pump(id);
        });
        Ok(())
    }

    /// Advance every live node's clock by one tick, in id order. Stops at the
    /// first prompt a tick raises.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn tick_all(&mut self) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        self.clock += 1;
        let resend = self.policy.auto_resend;
        let pool = self.pool.clone();
        for id in pool {
            if self.prompt.is_some() {
                break;
            }
            let Some(index) = self.index_of(id) else {
                continue;
            };
            if self.nodes[index].is_none() {
                continue;
            }
            self.narrate_tick(id, index);
            self.observe(id, move |world| {
                if let Some(node) = world.nodes[index].as_mut() {
                    node.tick();
                    if resend {
                        node.resend_pending();
                    }
                }
                world.pump(id);
            });
        }
        Ok(())
    }

    /// Force `id`'s election timeout to fire now.
    ///
    /// The mechanism is the one `paros-core`'s own examples use: set the
    /// timeout to one tick, tick once, then restore the timeout the level (or
    /// the player) asked for. `tick` is the *only* way a campaign starts —
    /// there is no "campaign now" entry point on the core, and there should
    /// not be one — so this is what "start an election" means.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn start_election(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.clock += 1;
        let restore = self.election_timeouts[index];
        self.narrate(
            NarrationKind::Election,
            format!(
                "{}'s election timeout fires: it has waited long enough without hearing from a \
                 leader. Nothing about that is a safety decision — a timeout only ever costs a \
                 round.",
                who(id)
            ),
        );
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.set_election_timeout(1);
                node.tick();
                node.set_election_timeout(restore);
            }
            world.pump(id);
        });
        Ok(())
    }

    /// Set `id`'s election timeout, in ticks.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn set_election_timeout(&mut self, id: NodeId, ticks: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.election_timeouts[index] = ticks;
        if let Some(node) = self.nodes[index].as_mut() {
            node.set_election_timeout(ticks);
        }
        self.settle();
        Ok(())
    }

    // ---- the operator ------------------------------------------------------

    /// Drop `id`'s volatile state; its disk survives untouched.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn crash(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let disk = &self.disks[index];
        let text = format!(
            "{} crashes. Its disk keeps promise {} and {}; its role, its open rounds, its read \
             rounds and its election timer are gone. Leadership in paros is entirely volatile, \
             so a crash *is* an abdication and needs no durable fence — and the Phase-2 rounds \
             that die with it are the ones nobody will ever re-send.",
            who(id),
            show_ballot(disk.hard_state().max_promised_ballot),
            many(disk.records().len(), "accepted record")
        );
        self.narrate(NarrationKind::Crash, text);
        self.nodes[index] = None;
        self.armed_seams[index] = None;
        self.settle();
        Ok(())
    }

    /// Arm a durability seam on `id`'s **next** drained batch.
    ///
    /// The semantics, which are the two seams a process-level crash cannot
    /// reach:
    ///
    /// - [`Seam::BeforeSync`]: the batch is discarded whole — nothing durable,
    ///   nothing sent — and the node is dropped. The disk is exactly what it
    ///   was before the call into the node.
    /// - [`Seam::AfterSyncBeforeSend`]: the batch's writes are applied to the
    ///   disk, its messages, committed entries and read states are discarded,
    ///   and the node is dropped. This is the seam that makes a promise
    ///   durable that nobody ever heard about.
    ///
    /// Arming is idempotent and a crash or restart disarms.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn crash_at(&mut self, id: NodeId, seam: Seam) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.armed_seams[index] = Some(seam);
        let text = match seam {
            Seam::BeforeSync => format!(
                "{} is armed to die before its next batch is durable: nothing will be written and \
                 nothing will be sent, so the disk will be exactly what it is now.",
                who(id)
            ),
            Seam::AfterSyncBeforeSend => format!(
                "{} is armed to die after its next batch is durable but before it is sent: the \
                 writes will survive and the messages will not. That is how a promise nobody \
                 ever heard about ends up on a disk.",
                who(id)
            ),
        };
        self.narrate(NarrationKind::Info, text);
        Ok(())
    }

    /// Rebuild a crashed node from its disk.
    ///
    /// # Panics
    ///
    /// Never on a player-reachable path: the node is installed one line above
    /// the read-back the narration uses.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn restart(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.index_of(id).ok_or_else(|| unknown_node(id))?;
        if self.nodes[index].is_some() {
            return Err(ActionError::new(
                ActionErrorCode::NodeAlive,
                format!("node {} is already running", id.0),
            ));
        }
        // A store that was provisioned once and no longer carries its own
        // format marker is a node whose promise is gone. An empty disk and a
        // brand-new disk look exactly alike from inside, so the operator's
        // record of having provisioned this identity is the only thing that
        // tells them apart — and it is what makes the refusal possible.
        if self.disks[index].provisioned() && !self.disks[index].is_formatted() {
            if let Some(prompt) = self.wiped_rejoin_prompt(id, index) {
                self.narrate(
                    NarrationKind::Restart,
                    format!(
                        "{} asks to come back. {} You answer for the operator.",
                        who(id),
                        prompt.question
                    ),
                );
                self.prompt = Some(prompt);
                self.paused = Some(Paused::Boot { node: id });
                return Ok(());
            }
            return Err(self.amnesia(id, index));
        }
        let mut node = ColocatedNode::new(&self.disks[index]);
        node.set_election_timeout(self.election_timeouts[index]);
        self.nodes[index] = Some(node);
        self.armed_seams[index] = None;
        let booted = self.nodes[index].as_ref().expect("just installed");
        let text = format!(
            "{} restarts from its disk: promise {}, {}, applied prefix ending at {}. It boots as \
             a follower — the disk carries the promise and the log, never the leadership.",
            who(id),
            show_ballot(booted.acceptor().promised()),
            many(booted.acceptor().records().len(), "accepted record"),
            booted
                .replica()
                .chosen_index()
                .map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0))
        );
        self.narrate(NarrationKind::Restart, text);
        self.observe(id, move |world| world.pump(id));
        Ok(())
    }

    /// A leader resigns.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn step_down(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.narrate(
            NarrationKind::Election,
            format!("{} resigns its leadership.", who(id)),
        );
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.step_down();
            }
            world.pump(id);
        });
        Ok(())
    }

    /// Re-broadcast a leader's still-pending `Accept`s.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn resend_pending(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.narrate(
            NarrationKind::Info,
            format!(
                "{} re-sends the Accepts it is still waiting on. Re-sending is always safe and \
                 never necessary: an acceptor that already voted answers the same way twice.",
                who(id)
            ),
        );
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.resend_pending();
            }
            world.pump(id);
        });
        Ok(())
    }

    // ---- the client --------------------------------------------------------

    /// A client asks `id` to get `value` chosen, optionally naming the grid
    /// **column** its Phase 2 goes to.
    ///
    /// A column is validated against the configuration in force before the
    /// core is called: the core asserts on a column its configuration does not
    /// have, and an assert in wasm is an abort with no stack. A grid leader
    /// asked for no column, on a level that teaches the choice, is asked which
    /// column instead — and the world holds the proposal until it is answered.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn propose(
        &mut self,
        id: NodeId,
        client: u64,
        value: &str,
        column: Option<u64>,
    ) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let column = self.validate_column(id, column)?;
        // A player who named a column has answered the question already.
        if column.is_none()
            && let Some(prompt) = self.grid_column_prompt(id, index)
        {
            self.narrate(
                NarrationKind::Client,
                format!(
                    "Client {client} asks {} to get {value:?} chosen. {} You answer for it, and \
                     the configuration marks the answer.",
                    who(id),
                    prompt.question
                ),
            );
            self.prompt = Some(prompt);
            self.paused = Some(Paused::Propose {
                node: id,
                client,
                value: value.to_string(),
            });
            return Ok(());
        }
        self.propose_now(id, index, client, value, column)
    }

    /// Send the proposal for real, once nobody owes an answer for it.
    fn propose_now(
        &mut self,
        id: NodeId,
        index: usize,
        client: u64,
        value: &str,
        column: Option<usize>,
    ) -> Result<(), ActionError> {
        let slot = self
            .clients
            .iter()
            .position(|c| c.id == ClientId(client))
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no client {client} in this level"),
                )
            })?;
        let seq = ClientSeq(self.clients[slot].next_seq);
        let mark = self.narration.len();
        let bytes = Value(value.as_bytes().to_vec());
        let issued = self.take_event();
        let result = self.observe(id, move |world| {
            let out = world.nodes[index]
                .as_mut()
                .map(|node| node.propose_in(ClientId(client), seq, bytes, column));
            world.pump(id);
            out
        });
        let fresh = matches!(result, Some(ProposeResult::Accepted(_)));
        let admitted = match result {
            Some(ProposeResult::NotLeader(hint)) => {
                self.narration.truncate(mark);
                return Err(ActionError::new(
                    ActionErrorCode::NotLeader,
                    match hint {
                        Some(leader) => format!(
                            "node {} is not the leader; the client should ask node {}",
                            id.0, leader.0
                        ),
                        None => format!(
                            "node {} is not the leader, and it does not know who is",
                            id.0
                        ),
                    },
                ));
            }
            Some(
                ProposeResult::Accepted(slot)
                | ProposeResult::Duplicate(slot)
                | ProposeResult::Chosen(slot),
            ) => Some(slot),
            None => None,
        };
        self.clients[slot].next_seq += 1;
        self.clients[slot].proposals.push(Proposal {
            seq,
            value: value.to_string(),
            node: id,
            slot: admitted,
            acked: false,
            issued,
            acked_at: None,
        });
        let opening = say(
            NarrationKind::Client,
            format!(
                "Client {client} asks {} to get {value:?} chosen. {}",
                who(id),
                match (admitted, fresh) {
                    (None, _) => "It is not running, so nothing happens.".to_string(),
                    (Some(slot), true) => format!(
                        "The leader hands it the next free slot, {}, and goes straight to Phase \
                         2 — one round trip, because the ballot it won already covers the whole \
                         suffix.",
                        slot.0
                    ),
                    // `Duplicate` and `Chosen`: no new slot, no new round. The
                    // narration must not claim one was opened.
                    (Some(slot), false) => format!(
                        "The leader recognises this command: it is already at slot {}, so \
                         nothing new is proposed. At-most-once execution is a property of the \
                         log, not of the network.",
                        slot.0
                    ),
                }
            ),
        );
        self.narration.insert(mark, opening);
        Ok(())
    }

    /// A client **retries** a write it already sent: the same
    /// `(client, seq, bytes)`, asked again.
    ///
    /// This is the whole of at-most-once execution from the client's side. The
    /// leader has three honest answers — it applied this command already, it
    /// has it in flight at a slot, or it has never seen it — and which one it
    /// gives is the [`crate::prompt::PromptKind::AckWrite`] question.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn retry(&mut self, id: NodeId, client: u64, seq: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let position = self
            .clients
            .iter()
            .position(|c| c.id == ClientId(client))
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no client {client} in this level"),
                )
            })?;
        if !self.clients[position]
            .proposals
            .iter()
            .any(|proposal| proposal.seq == ClientSeq(seq))
        {
            return Err(ActionError::new(
                ActionErrorCode::UnknownParty,
                format!("client {client} never sent a write with sequence number {seq}"),
            ));
        }
        if let Some(prompt) = self.ack_write_prompt(id, index, ClientId(client), ClientSeq(seq)) {
            self.narrate(
                NarrationKind::Client,
                format!(
                    "Client {client} asks {} again for its write #{seq}. {} You answer for it, \
                     and the protocol marks the answer.",
                    who(id),
                    prompt.question
                ),
            );
            self.prompt = Some(prompt);
            self.paused = Some(Paused::Retry {
                node: id,
                client,
                seq,
            });
            return Ok(());
        }
        self.retry_now(id, client, seq);
        Ok(())
    }

    /// Send the retry for real, once nobody owes an answer for it.
    pub(super) fn retry_now(&mut self, id: NodeId, client: u64, seq: u64) {
        let Some(index) = self.index_of(id) else {
            return;
        };
        let Some(position) = self.clients.iter().position(|c| c.id == ClientId(client)) else {
            return;
        };
        let Some(bytes) = self.clients[position]
            .proposals
            .iter()
            .find(|proposal| proposal.seq == ClientSeq(seq))
            .map(|proposal| Value(proposal.value.as_bytes().to_vec()))
        else {
            return;
        };
        let mark = self.narration.len();
        let result = self.observe(id, move |world| {
            let out = world.nodes[index]
                .as_mut()
                .map(|node| node.propose(ClientId(client), ClientSeq(seq), bytes));
            world.pump(id);
            out
        });
        let answer = match result {
            Some(ProposeResult::Chosen(slot)) => RetryAnswer::Applied(slot),
            Some(ProposeResult::Duplicate(slot)) => RetryAnswer::InFlight(slot),
            Some(ProposeResult::Accepted(slot)) => RetryAnswer::Fresh(slot),
            Some(ProposeResult::NotLeader(_)) | None => RetryAnswer::Refused,
        };
        self.retries.push(RetryOutcome {
            node: id,
            client,
            seq,
            answer,
        });
        let text = match result {
            Some(ProposeResult::Chosen(slot)) => format!(
                "{} answers immediately: write #{seq} applied at slot {}. The ledger the \
                 contiguous walk writes is the only thing that licenses that answer — an ack \
                 names a slot this node has really executed.",
                who(id),
                slot.0
            ),
            Some(ProposeResult::Duplicate(slot)) => format!(
                "{} holds write #{seq} in flight at slot {}: chosen, perhaps, but not yet \
                 applied here. The client waits — and it waits on the *same* slot, which is why \
                 the command is never executed twice.",
                who(id),
                slot.0
            ),
            Some(ProposeResult::Accepted(slot)) => format!(
                "{} has never seen write #{seq}: it takes the next free slot, {}. Neither dedup \
                 table knew the identity, so this really is a first attempt as far as the log is \
                 concerned.",
                who(id),
                slot.0
            ),
            Some(ProposeResult::NotLeader(_)) | None => {
                format!("{} cannot answer for write #{seq}.", who(id))
            }
        };
        self.narration
            .insert(mark, say(NarrationKind::Client, text));
    }

    /// A client asks `id` to drop the log prefix up to `up_to`.
    ///
    /// The coupling rule is what makes this more than "raise a number": a
    /// `Truncate` may only be proposed once a **quorum holds a decided
    /// snapshot point** at or past what is being dropped. Below the floor the
    /// entries are gone everywhere, and the snapshot is the only thing left to
    /// rescue a node that was away. A request no point covers is therefore
    /// **refused** — and the refusal seeds the next snapshot point, so the
    /// client's retry can go further.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn compact(&mut self, id: NodeId, up_to: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let node = self.nodes[index].as_ref().ok_or_else(|| unknown_node(id))?;
        if !node.is_leader() {
            return Err(ActionError::new(
                ActionErrorCode::NotLeader,
                match node.leader() {
                    Some(leader) => format!(
                        "node {} is not the leader; a compaction request goes to node {}",
                        id.0, leader.0
                    ),
                    None => format!(
                        "node {} is not the leader, and it does not know who is",
                        id.0
                    ),
                },
            ));
        }
        let covered = self.covered_snap_point(index);
        let marker_open = node
            .proposer()
            .rounds()
            .values()
            .any(|round| matches!(round.command(), Command::Control(Control::Snap { .. })));
        let mark = self.narration.len();
        let (accepted, seeded) = if let Some(point) = covered {
            {
                let clamped = Slot(up_to.min(point.0));
                let accepted = self.observe(id, move |world| {
                    let out = world.nodes[index].as_mut().map(|node| {
                        matches!(
                            node.propose_control(Control::Truncate { up_to: clamped }),
                            ProposeResult::Accepted(_)
                        )
                    });
                    world.pump(id);
                    out.unwrap_or(false)
                });
                // The request outran the covered prefix: seed the next point so
                // a later compaction may go further.
                let seed = up_to > point.0 && !marker_open;
                if seed {
                    self.seed_snap_marker(id, index);
                }
                (accepted, seed)
            }
        } else {
            let seed = !marker_open;
            if seed {
                self.seed_snap_marker(id, index);
            }
            (false, seed)
        };
        self.compacts.push(CompactOutcome {
            node: id,
            requested: Slot(up_to),
            accepted,
            covered,
            seeded_marker: seeded,
        });
        let opening = say(
            NarrationKind::Truncate,
            match (accepted, covered) {
                (true, Some(point)) => format!(
                    "A client asks {} to drop everything up to slot {up_to}. A quorum holds a \
                     decided snapshot at slot {}, so the leader proposes a Truncate — through \
                     ordinary consensus, into the next free slot, exactly like a client value. \
                     Every node will drop its prefix when it *applies* that slot.",
                    who(id),
                    point.0
                ),
                (_, None) => format!(
                    "A client asks {} to drop everything up to slot {up_to}, and the leader \
                     refuses. No quorum holds a decided snapshot covering that prefix, and past \
                     a floor the entries are gone everywhere — the snapshot is the only thing \
                     left to rescue a node that was away. It seeds a snapshot point instead; ask \
                     again once that is decided.",
                    who(id)
                ),
                (false, Some(point)) => format!(
                    "A client asks {} to drop everything up to slot {up_to}. A quorum's snapshot \
                     covers slot {}, but the leader did not admit the proposal.",
                    who(id),
                    point.0
                ),
            },
        );
        self.narration.insert(mark, opening);
        Ok(())
    }

    /// The highest decided snapshot point a **Phase-2 quorum** of the
    /// configuration in force holds.
    ///
    /// The custody tally is read straight off the disks: a real driver learns
    /// it from the per-tick `SnapAck` advertisements, which are driver-terminal
    /// and never enter the core, so the game reads the same fact from the one
    /// place it already owns rather than modelling a message the player would
    /// have nothing to decide about. The quorum question itself goes through
    /// the membership boundary, never a count.
    fn covered_snap_point(&self, index: usize) -> Option<Slot> {
        let node = self.nodes[index].as_ref()?;
        let acceptors = node.acceptors();
        let mut points: Vec<Slot> = self
            .disks
            .iter()
            .filter_map(Disk::snapshot_point)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        points.sort_unstable();
        points.into_iter().rev().find(|point| {
            let holders: BTreeSet<NodeId> = self
                .pool
                .iter()
                .copied()
                .enumerate()
                .filter(|(index, _)| {
                    self.disks[*index]
                        .snapshot_point()
                        .is_some_and(|held| held >= *point)
                })
                .map(|(_, id)| id)
                .collect();
            acceptors.has_phase2_quorum(&holders)
        })
    }

    /// Ask the leader to decide the next snapshot point.
    fn seed_snap_marker(&mut self, id: NodeId, index: usize) {
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.propose_snap_marker();
            }
            world.pump(id);
        });
    }

    /// A client asks `id` for a linearizable read.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn read_index(&mut self, id: NodeId, client: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let slot = self
            .clients
            .iter()
            .position(|c| c.id == ClientId(client))
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no client {client} in this level"),
                )
            })?;
        let ctx = self.next_read_ctx;
        let issued = self.take_event();
        // The index a read-index round captures, recomputed here because
        // `ReadRound` exposes none of its fields: the applied watermark, or the
        // fresh-leader fence when that sits higher.
        let captured = self.nodes[index].as_ref().and_then(|node| {
            let fence = node.proposer().read_floor();
            node.replica().chosen_index().max(fence)
        });
        let mark = self.narration.len();
        let outcome = self.observe(id, move |world| {
            let out = world.nodes[index].as_mut().map(|node| node.read_index(ctx));
            world.pump(id);
            out
        });
        match outcome {
            Some(ReadIndexResult::NotLeader(hint)) => {
                self.narration.truncate(mark);
                return Err(ActionError::new(
                    ActionErrorCode::NotLeader,
                    match hint {
                        Some(leader) => format!(
                            "node {} is not the leader; a linearizable read goes to node {}",
                            id.0, leader.0
                        ),
                        None => format!(
                            "node {} is not the leader, and it does not know who is",
                            id.0
                        ),
                    },
                ));
            }
            Some(ReadIndexResult::Pending) | None => {}
        }
        self.next_read_ctx += 1;
        let required_seq = self.nodes[index]
            .as_ref()
            .and_then(|node| {
                node.proposer()
                    .read_rounds()
                    .last()
                    .map(paros_core::proposer::ReadRound::required_seq)
            })
            .unwrap_or(0);
        self.clients[slot].reads.push(PendingRead {
            ctx,
            node: id,
            index: captured,
            required_seq,
            acks: BTreeSet::new(),
            leaderless: false,
            served: false,
            issued,
            served_at: None,
        });
        let opening = say(
            NarrationKind::Read,
            format!(
                "Client {client} asks {} for a linearizable read. The read captures {} and asks \
                 for nothing to be written: what it needs is a fresh proof that {} still leads, \
                 and a beat's acks are that proof.",
                who(id),
                captured.map_or_else(
                    || "the empty prefix".to_string(),
                    |s| format!("slot {} as its watermark", s.0)
                ),
                who(id)
            ),
        );
        self.narration.insert(mark, opening);
        Ok(())
    }

    /// A client asks `id` for a **leaderless** read (Compartmentalized Paxos
    /// §3.4).
    ///
    /// `id` asks a Phase-1 quorum — one row of a grid, the whole membership
    /// under a majority — for the highest slot each of them has voted in,
    /// takes the maximum, and answers the read once its own applied prefix
    /// covers it. No leader is asked, no beat is sent, and no clock is read.
    /// Any node may serve one: a leader, a follower, a node that is not even
    /// an acceptor.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn quorum_read(&mut self, id: NodeId, client: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let position = self
            .clients
            .iter()
            .position(|c| c.id == ClientId(client))
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no client {client} in this level"),
                )
            })?;
        let ctx = self.next_read_ctx;
        self.next_read_ctx += 1;
        let issued = self.take_event();
        let row = self
            .node(id)
            .and_then(|node| node.acceptors().row_of(ctx))
            .map_or_else(|| "every acceptor".to_string(), |row| format!("row {row}"));
        self.clients[position].reads.push(PendingRead {
            ctx,
            node: id,
            // A quorum read captures nothing when it opens: the index is the
            // maximum watermark the row reports, and the row has not answered
            // yet. `serve_read` fills it in.
            index: None,
            required_seq: 0,
            acks: BTreeSet::new(),
            leaderless: true,
            served: false,
            issued,
            served_at: None,
        });
        let mark = self.narration.len();
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.quorum_read(ctx);
            }
            world.pump(id);
        });
        let opening = say(
            NarrationKind::Read,
            format!(
                "Client {client} asks {} for a read, and {} does not ask the leader. It asks \
                 {row} one question: what is the highest slot you have voted in? The largest of \
                 those answers is the index this read must reach before it may be answered.",
                who(id),
                who(id)
            ),
        );
        self.narration.insert(mark, opening);
        Ok(())
    }

    // ---- the operator's Act IV verbs ---------------------------------------

    /// A leader hands its Phase-2 authority to `to`, under the **same ballot**
    /// and with no Phase 1.
    ///
    /// The abdication happens inside the core's own call, before the message
    /// exists, so this world never holds a state where two nodes own one
    /// ballot. Every refusal is checked first ([`World::handoff_refusal`]) so
    /// the player reads a reason rather than watching nothing happen.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn relinquish(&mut self, id: NodeId, to: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        if self.index_of(to).is_none() {
            return Err(unknown_node(to));
        }
        if let Some(reason) = self.handoff_refusal(id) {
            return Err(ActionError::new(ActionErrorCode::HandoffRefused, reason));
        }
        let candidates = self
            .node(id)
            .map(ColocatedNode::handoff_candidates)
            .unwrap_or_default();
        if !candidates.contains(&to) {
            return Err(ActionError::new(
                ActionErrorCode::HandoffRefused,
                format!(
                    "node {} cannot take the authority: a hand-off goes to another member of the \
                     configuration in force.",
                    to.0
                ),
            ));
        }
        let mark = self.narration.len();
        let receipt = self.observe(id, move |world| {
            let out = world.nodes[index]
                .as_mut()
                .and_then(|node| node.relinquish_to(to));
            world.pump(id);
            out
        });
        let Some(receipt) = receipt else {
            self.narration.truncate(mark);
            return Err(ActionError::new(
                ActionErrorCode::HandoffRefused,
                format!("node {} did not hand its leadership over", id.0),
            ));
        };
        self.handoffs.push(HandoffOutcome {
            from: id,
            to,
            ballot: receipt.ballot,
            next_slot: receipt.next_slot,
            decided: receipt.decided,
            pending: receipt.pending,
        });
        let opening = say(
            NarrationKind::Election,
            format!(
                "{} hands ballot {} to {} and stops leading inside the same call, before the \
                 message exists. It sends the frontier — slot {} is the next free slot — and the \
                 tail below it: {} already chosen, {} still in flight. The two exactly cover the \
                 range. That is what lets the successor skip Phase 1 and still know it has been \
                 told about every slot.",
                who(id),
                show_ballot(receipt.ballot),
                who(to),
                receipt.next_slot.0,
                many(receipt.decided, "slot"),
                many(receipt.pending, "slot")
            ),
        );
        self.narration.insert(mark, opening);
        Ok(())
    }

    /// Rot one accepted record on `id`'s disk: the value is lost, the identity
    /// is not.
    ///
    /// A running node keeps its record in memory, so the damage shows up at
    /// the **next boot** — which is exactly how a real disk fault behaves, and
    /// why this level crashes and restarts the node it damages.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn corrupt(&mut self, id: NodeId, slot: Slot) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.index_of(id).ok_or_else(|| unknown_node(id))?;
        let ballot = self.disks[index].records().get(&slot).map(|(at, _)| *at);
        let Some(ballot) = ballot else {
            return Err(ActionError::new(
                ActionErrorCode::UnknownMessage,
                format!("node {} holds no record for slot {}", id.0, slot.0),
            ));
        };
        self.disks[index].corrupt(slot);
        self.narrate(
            NarrationKind::Crash,
            format!(
                "{}'s record for slot {} rots. The value is gone and the identity survives: the \
                 disk still knows it voted there, at ballot {}. At its next boot it reports that \
                 slot as damaged. It does not report \"nothing accepted here\", because that \
                 answer tells a candidate it may decide something else at a slot a quorum may \
                 already have decided.",
                who(id),
                slot.0,
                show_ballot(ballot)
            ),
        );
        Ok(())
    }

    /// Erase `id`'s disk, keeping only the memory that the identity was
    /// provisioned once.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn wipe(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.index_of(id).ok_or_else(|| unknown_node(id))?;
        let promised = self.disks[index].hard_state().max_promised_ballot;
        self.nodes[index] = None;
        self.armed_seams[index] = None;
        self.disks[index].wipe();
        self.narrate(
            NarrationKind::Crash,
            format!(
                "{}'s disk is erased. It had promised {}, and that promise is gone from the one \
                 place it was written down. Nothing in the cluster returns it: no peer knows what \
                 this node has promised, and a snapshot restores the log and not a promise.",
                who(id),
                show_ballot(promised)
            ),
        );
        self.settle();
        Ok(())
    }

    // ---- prompts -----------------------------------------------------------

    /// Answer the open prompt.
    ///
    /// A wrong answer is not an error: it costs a mistake and an explanation,
    /// and the world does not move. `Ok(Verdict::Wrong)` says so.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn answer(&mut self, prompt_id: u64, choice: &str) -> Result<Verdict, ActionError> {
        let Some(prompt) = self.prompt.as_mut() else {
            return Err(ActionError::new(
                ActionErrorCode::NoPrompt,
                "no prompt is open",
            ));
        };
        if prompt.id != prompt_id {
            return Err(ActionError::new(
                ActionErrorCode::NoPrompt,
                format!("prompt {prompt_id} is not the open one"),
            ));
        }
        if !prompt.offers(choice) {
            return Err(ActionError::new(
                ActionErrorCode::UnknownChoice,
                format!("this prompt has no choice {choice:?}"),
            ));
        }
        let verdict = prompt.judge(choice);
        let (kind, node) = (prompt.kind, prompt.node);
        if verdict == Verdict::Wrong {
            let feedback = prompt.feedback.clone().unwrap_or_default();
            self.narrate(NarrationKind::Violation, feedback);
            return Ok(Verdict::Wrong);
        }
        self.prompt = None;
        self.narrate(
            NarrationKind::Info,
            format!(
                "That is what the protocol does here, so node {node} really does it: {}",
                crate::prompt::confirmation(kind)
            ),
        );
        if let Some(paused) = self.paused.take() {
            self.resume(paused);
        }
        Ok(Verdict::Right)
    }

    // ---- the drain ---------------------------------------------------------
    // ---- bookkeeping -------------------------------------------------------

    /// Note a heartbeat ack against the read rounds it qualifies for — display
    /// only (see [`PendingRead::acks`]).
    fn note_ack(&mut self, entry: &InFlight) {
        let Message::HeartbeatAck { from, seq, .. } = &entry.message else {
            return;
        };
        for client in &mut self.clients {
            for read in &mut client.reads {
                if read.node == entry.to && !read.served && *seq >= read.required_seq {
                    read.acks.insert(*from);
                }
            }
        }
    }

    fn serve_read(&mut self, state: ReadState) {
        let mut served = false;
        let mut leaderless = false;
        let stamp = self.next_event;
        for client in &mut self.clients {
            for read in &mut client.reads {
                if read.ctx == state.ctx {
                    read.served = true;
                    read.index = state.index;
                    read.served_at = Some(stamp);
                    served = true;
                    leaderless = read.leaderless;
                }
            }
        }
        if !served {
            return;
        }
        self.next_event += 1;
        let at = state.index.map_or_else(
            || "the empty prefix".to_string(),
            |s| format!("slot {}", s.0),
        );
        let text = if leaderless {
            format!(
                "The read at ctx {} is served at {at}, and no leader was asked. A Phase-1 quorum \
                 reported the highest slot each of them had voted in, every write acknowledged \
                 before this read began was chosen by a Phase-2 quorum, and the two always share \
                 an acceptor — so the maximum they reported is at or above that write. This \
                 node has now applied that far.",
                state.ctx
            )
        } else {
            format!(
                "The read at ctx {} is served at {at}. A quorum acked a beat sent after the read \
                 began, so no other node could have been committing behind this one's back, and \
                 the applied prefix covers the watermark the read captured.",
                state.ctx
            )
        };
        self.narrate(NarrationKind::Read, text);
    }

    /// The line a tick gets, before anything is stepped.
    fn narrate_tick(&mut self, id: NodeId, index: usize) {
        let timeout = self.nodes[index]
            .as_ref()
            .map_or(0, ColocatedNode::election_timeout);
        let clock = self.clock;
        let text = if timeout == NO_CHECK_QUORUM {
            format!(
                "{} ticks (logical time {clock}). Its election clock is parked while you deliver \
                 heartbeats by hand, so CheckQuorum will not depose it between your moves.",
                who(id)
            )
        } else {
            format!(
                "{} ticks (logical time {clock}). Its election timeout is {timeout} tick(s) of \
                 silence from a leader.",
                who(id)
            )
        };
        self.narrate(NarrationKind::Info, text);
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

    /// Resume a proposal whose column the player has just named. The column
    /// they were checked against is the one the configuration derives, so this
    /// hands the core no override at all: it derives the same column itself.
    pub(super) fn propose_answered(&mut self, id: NodeId, client: u64, value: &str) {
        let Some(index) = self.index_of(id) else {
            return;
        };
        let _ = self.propose_now(id, index, client, value, None);
    }

    /// Resume a wiped node's boot, once the player has refused it.
    pub(super) fn boot_refused(&mut self, id: NodeId) {
        let Some(index) = self.index_of(id) else {
            return;
        };
        let _ = self.amnesia(id, index);
    }

    /// The refusal an erased disk earns, and the sentence that goes with it.
    fn amnesia(&mut self, id: NodeId, index: usize) -> ActionError {
        let promised = self.promise_watermarks[index];
        self.refused_boots.push((id, promised));
        self.narrate(
            NarrationKind::Restart,
            format!(
                "{} is refused. Its disk is empty and this identity was provisioned once, so \
                 what is missing is a promise it already made — it last promised {}. A node that \
                 booted here with an empty promise would answer a ballot below {} that it had \
                 already sworn to refuse, and a quorum built behind that older ballot could \
                 choose a second value for a slot. What heals the cluster is a change of the \
                 acceptor set, decided by the cluster: the survivors go on without this \
                 identity.",
                who(id),
                show_ballot(promised),
                show_ballot(promised)
            ),
        );
        ActionError::new(
            ActionErrorCode::Amnesia,
            format!(
                "node {} lost its disk: it may never rejoin, because a promise cannot be \
                 restored from anywhere.",
                id.0
            ),
        )
    }

    /// Turn a player's column into one the configuration in force actually
    /// has, or refuse it. The core asserts on a column its configuration does
    /// not have, so nothing unvalidated ever reaches it.
    fn validate_column(
        &self,
        id: NodeId,
        column: Option<u64>,
    ) -> Result<Option<usize>, ActionError> {
        let Some(column) = column else {
            return Ok(None);
        };
        let QuorumSystem::Grid { cols, .. } = self.system(id) else {
            return Err(ActionError::new(
                ActionErrorCode::BadColumn,
                format!(
                    "node {} runs no grid, so it names no columns: an Accept goes to the whole \
                     configuration.",
                    id.0
                ),
            ));
        };
        let index = usize::try_from(column).unwrap_or(usize::MAX);
        if index >= cols {
            return Err(ActionError::new(
                ActionErrorCode::BadColumn,
                format!("this grid has {cols} column(s), numbered 0 to {}", cols - 1),
            ));
        }
        Ok(Some(index))
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

/// "nothing", or "slot 3" — how the goals and the history judge name a
/// watermark.
fn at(slot: Option<Slot>) -> String {
    slot.map_or_else(
        || "the empty prefix".to_string(),
        |s| format!("slot {}", s.0),
    )
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
