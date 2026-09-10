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
//! out, `advance()`, persist, send, apply, answer, `advance_recovery()`, and
//! drain again until the node is quiet. The `Ready` guard is never held across
//! anything else — not a disk write, not a prompt, not a player action. It
//! lives in the crate-private `drain` module, with the durability seams and
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

use std::collections::BTreeSet;

use paros_core::{
    ClientId, ClientSeq, ColocatedNode, Config, Message, NodeId, ProposeResult, ReadIndexResult,
    ReadState, Slot, Value,
};

use crate::action::{ActionError, ActionErrorCode, Seam};
use crate::prompt::{Prompt, PromptKind, Verdict};
use crate::view::{MessageView, message_view};
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
    /// Render it for the wire list and the stage.
    #[must_use]
    pub fn view(&self) -> MessageView {
        message_view(self.id, self.from.0, self.to.0, self.sent_at, &self.message)
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
    served: bool,
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

    /// The clients this level gave the player, in id order.
    #[must_use]
    pub fn clients(&self) -> Vec<u64> {
        self.clients.iter().map(|client| client.id.0).collect()
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
        let Some(index) = self.index_of(entry.to) else {
            return Ok(());
        };
        if self.nodes[index].is_none() {
            return Ok(());
        }
        self.note_ack(&entry);
        if let Some(prompt) = self.prompt_for(entry.to, &entry.message) {
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
        self.wire.remove(position);
        Ok(())
    }

    /// Put a second copy of the in-flight message `id` on the wire.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn duplicate(&mut self, id: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let position = self.position_of(id)?;
        let mut copy = self.wire[position].clone();
        copy.id = self.next_message_id;
        self.next_message_id += 1;
        copy.sent_at = self.clock;
        self.wire.push(copy);
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
        if let Some(node) = self.nodes[index].as_mut() {
            node.tick();
            if resend {
                node.resend_pending();
            }
        }
        self.pump(id);
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
            if let Some(node) = self.nodes[index].as_mut() {
                node.tick();
                if resend {
                    node.resend_pending();
                }
            } else {
                continue;
            }
            self.pump(id);
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
        if let Some(node) = self.nodes[index].as_mut() {
            node.set_election_timeout(1);
            node.tick();
            node.set_election_timeout(restore);
        }
        self.pump(id);
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
        Ok(())
    }

    /// Rebuild a crashed node from its disk.
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
        let mut node = ColocatedNode::new(&self.disks[index]);
        node.set_election_timeout(self.election_timeouts[index]);
        self.nodes[index] = Some(node);
        self.armed_seams[index] = None;
        self.pump(id);
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
        if let Some(node) = self.nodes[index].as_mut() {
            node.step_down();
        }
        self.pump(id);
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
        if let Some(node) = self.nodes[index].as_mut() {
            node.resend_pending();
        }
        self.pump(id);
        Ok(())
    }

    // ---- the client --------------------------------------------------------

    /// A client asks `id` to get `value` chosen.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn propose(&mut self, id: NodeId, client: u64, value: &str) -> Result<(), ActionError> {
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
        let seq = ClientSeq(self.clients[slot].next_seq);
        let result = self.nodes[index]
            .as_mut()
            .map(|node| node.propose(ClientId(client), seq, Value(value.as_bytes().to_vec())));
        let admitted = match result {
            Some(ProposeResult::NotLeader(hint)) => {
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
        });
        self.pump(id);
        Ok(())
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
        // The index a read-index round captures, recomputed here because
        // `ReadRound` exposes none of its fields: the applied watermark, or the
        // fresh-leader fence when that sits higher.
        let captured = self.nodes[index].as_ref().and_then(|node| {
            let fence = node.proposer().read_floor();
            node.replica().chosen_index().max(fence)
        });
        let outcome = self.nodes[index].as_mut().map(|node| node.read_index(ctx));
        match outcome {
            Some(ReadIndexResult::NotLeader(hint)) => {
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
            served: false,
        });
        self.pump(id);
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
        if prompt.judge(choice) == Verdict::Wrong {
            return Ok(Verdict::Wrong);
        }
        self.prompt = None;
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
        for client in &mut self.clients {
            for read in &mut client.reads {
                if read.ctx == state.ctx {
                    read.served = true;
                    read.index = state.index;
                }
            }
        }
    }

    /// Re-derive everything that is not the core's business: which writes the
    /// admitting node has applied, and the leadership hold.
    fn settle(&mut self) {
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
                }
            }
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
