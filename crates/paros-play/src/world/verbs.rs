//! Every player verb the log world offers, and what each one answered with.
//!
//! The wire (deliver, drop, duplicate), the clock (tick, the election
//! timeout), the client (propose, retry, compact, the leaderless verbs of
//! Act IV), the operator's hand-off, and the answer to an open prompt. An
//! accessor lives beside the verb that fills it: `compacts()` beside
//! `compact`, `handoffs()` and `handoff_refusal()` beside `relinquish`.

use std::collections::BTreeSet;

use paros_core::{
    Ballot, ClientId, ClientSeq, ColocatedNode, Command, Control, HANDOFF_BATCH, LeadershipOrigin,
    MatchmakerId, Message, NodeId, ProposeResult, QuorumSystem, Slot, Value,
};

use crate::action::{ActionError, ActionErrorCode};
use crate::narration::{NarrationKind, many, say, who};
use crate::prompt::Verdict;
use crate::view::show_ballot;
use crate::world::disk::Disk;
use crate::world::drain::Paused;
use crate::world::history::Proposal;
use crate::world::{Envelope, NO_CHECK_QUORUM, Party, World, name, unknown_node};

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

impl World {
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
        // Three addressees, three contracts: a node's own protocol, a
        // matchmaker's registry, and the answers the registry sends back.
        if matches!(entry.to, Party::Matchmaker(_)) {
            self.deliver_to_matchmaker(entry);
            return Ok(());
        }
        if !matches!(entry.envelope, Envelope::Node(_)) {
            self.deliver_matchmaker_reply(entry);
            return Ok(());
        }
        let (Party::Node(to), Envelope::Node(message)) = (entry.to, entry.envelope) else {
            return Ok(());
        };
        let Some(index) = self.index_of(to) else {
            return Ok(());
        };
        if self.nodes[index].is_none() {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "{summary} reaches {}, which is not running, so it is discarded. A message \
                     to a machine that is not there is lost. That is the complete failure model \
                     here.",
                    who(to)
                ),
            );
            return Ok(());
        }
        self.note_ack(to, &message);
        if let Some(prompt) = self.prompt_for(to, &message) {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "{summary} stops at {}: {} You answer for it, and the real state machine \
                     marks the answer.",
                    who(to),
                    prompt.question
                ),
            );
            self.prompt = Some(prompt);
            self.paused = Some(Paused::Message {
                node: to,
                message: Box::new(message),
            });
            return Ok(());
        }
        self.step(to, message);
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
                "{summary} is lost. This game has no partition object. A partition is you not \
                 delivering a message. The protocol must treat a lost message and a partition \
                 the same way."
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
            // A copy is re-addressed inside its own tier: a node's message
            // misroutes to another node, a registration to another matchmaker.
            // Crossing the two would be a message the receiver has no contract
            // for, which is not something a network does.
            copy.to = match copy.to {
                Party::Node(_) => {
                    let to = NodeId(to);
                    if self.index_of(to).is_none() {
                        return Err(unknown_node(to));
                    }
                    Party::Node(to)
                }
                Party::Matchmaker(_) => {
                    let to = MatchmakerId(to);
                    if self.matchmaker(to).is_none() {
                        return Err(ActionError::new(
                            ActionErrorCode::UnknownParty,
                            format!("there is no matchmaker {} in this level", to.0),
                        ));
                    }
                    Party::Matchmaker(to)
                }
            };
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
                     stated so that a second copy changes nothing. The first copy already did \
                     all the work."
                ),
                Some(_) => format!(
                    "A copy of {summary} is on the wire, addressed to {} instead. Networks \
                     misroute messages, and the protocol answers for that. Every guard asks who \
                     a message is *from*, and what the configuration says about that sender. No \
                     guard asks what the transport did with the message.",
                    name(misrouted)
                ),
            },
        );
        Ok(())
    }

    /// The next message an automation pump would deliver: the lowest-id
    /// heartbeat (when `beats`), node reply (when `replies`) or
    /// matchmaker-plane message (when `matchmaker`) on the wire.
    #[must_use]
    pub fn next_auto_delivery(&self, beats: bool, replies: bool, matchmaker: bool) -> Option<u64> {
        self.wire
            .iter()
            .filter(|entry| match &entry.envelope {
                Envelope::Node(Message::Heartbeat { .. } | Message::HeartbeatAck { .. }) => beats,
                Envelope::Node(
                    Message::Promise { .. } | Message::Accepted { .. } | Message::Nack { .. },
                ) => replies,
                Envelope::Node(_) => false,
                _ => matchmaker,
            })
            .map(|entry| entry.id)
            .min()
    }

    /// Advance one node's clock by one tick (and re-send its pending accepts
    /// when [`WorldPolicy::auto_resend`](crate::world::WorldPolicy::auto_resend) is on).
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
        // A tick is the driver's beat, and the handover is driver policy: the
        // stall clock advances, a freeze whose quorum answered is closed, and
        // a phase that has stopped moving is given up.
        self.beat_reconfigurer(index);
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
            self.beat_reconfigurer(index);
        }
        Ok(())
    }

    /// The line a tick gets, before anything is stepped.
    fn narrate_tick(&mut self, id: NodeId, index: usize) {
        let timeout = self.nodes[index]
            .as_ref()
            .map_or(0, ColocatedNode::election_timeout);
        let clock = self.clock;
        let text = if timeout == NO_CHECK_QUORUM {
            format!(
                "{} ticks (logical time {clock}). Its election clock is held while you deliver \
                 heartbeats by hand. CheckQuorum therefore does not depose it between your \
                 moves.",
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
                "{}'s election timeout fires. It waited long enough without hearing from a \
                 leader. A timeout is not a safety decision. A timeout costs one round and \
                 nothing more.",
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
                "{} re-sends the Accepts it is still waiting for. A re-send is always safe, and \
                 it is never required. An acceptor that already voted answers the same way \
                 twice.",
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
                    "Client {client} asks {} to get {value} chosen. {} You answer for it, and \
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
                            "node {} is not the leader; the client must ask node {}",
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
                "Client {client} asks {} to get {value} chosen. {}",
                who(id),
                match (admitted, fresh) {
                    (None, _) => "It is not running, so nothing happens.".to_string(),
                    (Some(slot), true) => format!(
                        "The leader gives it the next free slot, {}, and goes directly to Phase \
                         2. That costs one round trip, because the ballot the leader holds \
                         already covers the whole log suffix.",
                        slot.0
                    ),
                    // `Duplicate` and `Chosen`: no new slot, no new round. The
                    // narration must not claim one was opened.
                    (Some(slot), false) => format!(
                        "The leader recognises this command. It is already at slot {}, so the \
                         leader proposes nothing new. At-most-once execution is a property of \
                         the log, and not of the network.",
                        slot.0
                    ),
                }
            ),
        );
        self.narration.insert(mark, opening);
        Ok(())
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
                format!("client {client} did not send a write with sequence number {seq}"),
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
                "{} answers at once: it applied write #{seq} at slot {}. The contiguous walk \
                 writes a ledger, and that ledger is the only thing that permits this answer. \
                 An ack names a slot this node has really executed.",
                who(id),
                slot.0
            ),
            Some(ProposeResult::Duplicate(slot)) => format!(
                "{} holds write #{seq} in flight at slot {}. That slot may be chosen already, \
                 but this node has not applied it yet. The client waits, and it waits on the \
                 *same* slot. That is why the command is not executed twice.",
                who(id),
                slot.0
            ),
            Some(ProposeResult::Accepted(slot)) => format!(
                "{} has not seen write #{seq} before, so it takes the next free slot, {}. \
                 Neither at-most-once table held this identity. For the log, this really is a \
                 first attempt.",
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
                     decided snapshot at slot {}, so the leader proposes a Truncate. The \
                     Truncate goes through ordinary consensus, into the next free slot, exactly \
                     like a client value. Every node drops its prefix when it *applies* that \
                     slot.",
                    who(id),
                    point.0
                ),
                (_, None) => format!(
                    "A client asks {} to drop everything up to slot {up_to}, and the leader \
                     refuses. No quorum holds a decided snapshot that covers that prefix. Below \
                     a floor the entries are gone on every node, and the snapshot is the only \
                     way to recover a node that was away. The leader seeds a snapshot point \
                     instead. Ask again once that point is decided.",
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
                "{} hands ballot {} to {}, and it stops leading inside the same call, before the \
                 message exists. It sends the frontier: slot {} is the next free slot. It also \
                 sends the tail below the frontier: {} already chosen, {} still in flight. The \
                 two parts cover the whole range. The successor can therefore skip Phase 1 and \
                 still know every slot below the frontier.",
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
                "node {} did not create ballot {}; node {} handed it over. An authority moves \
                 once: only the node that created a ballot may pass it on. A replayed hand-off \
                 would otherwise install that authority at a node that had already given it up, \
                 beside the node that exercises it now. Hold an election instead.",
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
                "node {} still has work that only a promise quorum can finish. It must settle \
                 an inherited slot, repair a damaged record, or complete an application prefix. \
                 A successor runs no Phase 1, so it cannot finish that work. An election can \
                 finish it.",
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
                 successor must learn every slot below the frontier. A slot it does not learn \
                 about is a slot nobody proposes again.",
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
}
