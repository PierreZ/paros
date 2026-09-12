//! The Act I world: single-decree Paxos over the bare roles.
//!
//! This is `crates/paros-core/examples/single_decree.rs` with the network
//! turned into player moves. There is no [`ColocatedNode`](paros_core::ColocatedNode)
//! here, no [`Storage`](paros_core::Storage) and no clock: an acceptor is an
//! [`Acceptor<Command>`] plus a `Vec<AcceptorWrite<Command>>` standing in for
//! its disk, a proposer is a [`Proposer<NodeId, Command>`], and both phases run
//! over one slot, [`DECREE`]. The messages on the wire are real
//! [`Message`]s, so the same renderer draws this world and the log world.
//!
//! Two things this world models that the log world hides inside
//! `ColocatedNode`: the **reach** of each phase (which acceptors a `Prepare` or
//! an `Accept` gets to, the only network this world has), and a proposer that
//! is *not* an acceptor — it holds no vote of its own, which is what makes the
//! quorum arithmetic visible.

use std::collections::BTreeMap;

use paros_core::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_core::proposer::{Campaign, Proposer};
use paros_core::{
    AcceptorConfig, AcceptorWrite, Ballot, ClientId, ClientSeq, Command, Entry, Fingerprint,
    Message, NodeId, QuorumSystem, Slot, Value,
};

use crate::action::{ActionError, ActionErrorCode, Phase};
use crate::narration::{NarrationEvent, NarrationKind, list_nodes, phase1_note, phase2_note, say};
use crate::prompt::{Prompt, PromptKind, Verdict};
use crate::view::{show_ballot, show_command};
use crate::world::{Envelope, InFlight, Party, WorldPolicy};

mod render;

/// The one decision. Single-decree Paxos is the same roles over a log with
/// exactly one slot.
pub const DECREE: Slot = Slot(0);

/// One acceptor: the role, and the `Vec` its durable writes go to.
pub(super) struct DecreeAcceptor {
    pub(super) id: NodeId,
    pub(super) role: Acceptor<Command>,
    disk: Vec<AcceptorWrite<Command>>,
}

/// Where a proposer's attempt has got to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Attempt {
    /// It has not opened a ballot yet.
    Idle,
    /// Phase 1 is in flight.
    Phase1,
    /// Phase 2 is in flight.
    Phase2,
    /// An acceptor refused it: a higher ballot is already promised somewhere.
    Preempted,
    /// Its value was chosen.
    Won,
}

/// One proposer: the role, the value it wants, and the ballot it is at.
pub(super) struct DecreeProposer {
    pub(super) id: NodeId,
    pub(super) role: Proposer<NodeId, Command>,
    pub(super) value: Option<Command>,
    pub(super) ballot: Option<Ballot>,
    last_round: u64,
    pub(super) attempt: Attempt,
    /// The value Phase 2 is actually proposing (its own, or the one P2c made
    /// it adopt).
    pub(super) proposing: Option<Command>,
}

/// What the decree world is holding back while a prompt is open.
enum Paused {
    /// A delivered message an acceptor has not answered for yet.
    Message { to: NodeId, message: Box<Message> },
    /// A won Phase 1 waiting on the value-selection answer.
    Phase2 { proposer: NodeId },
}

/// A Phase 1 that completed: which ballot, which acceptors it reached, and the
/// value it went on to propose. The quorum-intersection level's goal reads it.
#[derive(Clone, Debug)]
pub struct CompletedPhase1 {
    /// The ballot the campaign ran at.
    pub ballot: Ballot,
    /// The acceptors its `Prepare` reached.
    pub reach: Vec<NodeId>,
    /// What P2c made it propose.
    pub proposed: Command,
}

/// What a level pre-seeds an acceptor with: its durable promise, and the
/// record it already holds at [`DECREE`], if any.
pub type Seed = (Ballot, Option<(Ballot, Command)>);

/// The single-decree world.
pub struct DecreeWorld {
    pub(super) acceptors: Vec<DecreeAcceptor>,
    pub(super) proposers: Vec<DecreeProposer>,
    pub(super) wire: Vec<InFlight>,
    next_message_id: u64,
    pub(super) config: AcceptorConfig,
    phase1_reach: Vec<NodeId>,
    phase2_reach: Vec<NodeId>,
    pub(super) chosen: Option<(Ballot, Command)>,
    completed: Vec<CompletedPhase1>,
    policy: WorldPolicy,
    prompt: Option<Prompt>,
    paused: Option<Paused>,
    next_prompt_id: u64,
    /// A decision that contradicted one this world already held — two values
    /// for one slot. See [`DecreeWorld::violation`].
    violation: Option<String>,
    /// What the action in progress has done so far, in Paxos.
    narration: Vec<NarrationEvent>,
}

/// How this world names an acceptor. There is no colocation here: an acceptor
/// is only ever an acceptor, and a proposer holds no vote of its own.
fn actor(id: NodeId) -> String {
    format!("acceptor {}", id.0)
}

/// A client value, tagged with the proposer that carries it. Single-decree
/// Paxos never looks inside, and the `(client, seq)` fields exist only because
/// Multi-Paxos needs them for at-most-once execution.
#[must_use]
pub fn value(proposer: u64, round: u64, text: &str) -> Command {
    Command::User(Entry {
        client: ClientId(proposer),
        seq: ClientSeq(round),
        value: Value(text.as_bytes().to_vec()),
    })
}

impl DecreeWorld {
    /// A world of fresh acceptors and idle proposers under a majority quorum.
    ///
    /// # Panics
    ///
    /// If `acceptors` is empty.
    #[must_use]
    pub fn new(acceptors: &[u64], proposers: &[u64]) -> Self {
        Self::seeded(acceptors, proposers, &BTreeMap::new(), None)
    }

    /// A world where some acceptors already hold a promise and a record — how a
    /// level puts a history in place before the player's first move.
    ///
    /// `seeds` maps an acceptor id to `(promised, accepted)`; `chosen` says
    /// the decision was already taken, which a level uses to build the
    /// "a value is chosen, now try to change it" setup.
    ///
    /// # Panics
    ///
    /// If `acceptors` is empty, or a seed names an acceptor the world has not
    /// got.
    #[must_use]
    pub fn seeded(
        acceptors: &[u64],
        proposers: &[u64],
        seeds: &BTreeMap<u64, Seed>,
        chosen: Option<(Ballot, Command)>,
    ) -> Self {
        Self::with_system(acceptors, proposers, seeds, chosen, QuorumSystem::Majority)
    }

    /// A seeded world under a **named quorum system** — the flexible split
    /// Act IV runs, where a Phase-1 quorum and a Phase-2 quorum are different
    /// sizes and only the intersection *between* them is required.
    ///
    /// # Panics
    ///
    /// If `acceptors` is empty, if a seed names an acceptor the world has not
    /// got, or if `system` is not well formed over this many acceptors
    /// ([`QuorumSystem::admits`]) — a level's own configuration, checked once
    /// where it is written.
    #[must_use]
    pub fn with_system(
        acceptors: &[u64],
        proposers: &[u64],
        seeds: &BTreeMap<u64, Seed>,
        chosen: Option<(Ballot, Command)>,
        system: QuorumSystem,
    ) -> Self {
        assert!(!acceptors.is_empty(), "a decree world has acceptors");
        assert!(
            system.admits(acceptors.len()),
            "a level's quorum system fits its acceptors"
        );
        let members: Vec<NodeId> = acceptors.iter().copied().map(NodeId).collect();
        let config = AcceptorConfig::new(members.clone(), system);
        let roles = acceptors
            .iter()
            .map(|id| {
                let (promised, record) = seeds.get(id).cloned().unwrap_or((Ballot::zero(), None));
                let mut records = BTreeMap::new();
                if let Some(record) = record {
                    records.insert(DECREE, record);
                }
                DecreeAcceptor {
                    id: NodeId(*id),
                    role: Acceptor::new(promised, records, DECREE, BTreeMap::new()),
                    disk: Vec::new(),
                }
            })
            .collect();
        Self {
            acceptors: roles,
            proposers: proposers
                .iter()
                .map(|id| DecreeProposer {
                    id: NodeId(*id),
                    role: Proposer::new(),
                    value: None,
                    ballot: None,
                    last_round: 0,
                    attempt: Attempt::Idle,
                    proposing: None,
                })
                .collect(),
            wire: Vec::new(),
            next_message_id: 1,
            config,
            phase1_reach: members.clone(),
            phase2_reach: members,
            chosen,
            completed: Vec::new(),
            policy: WorldPolicy::default(),
            prompt: None,
            paused: None,
            next_prompt_id: 1,
            violation: None,
            narration: Vec::new(),
        }
    }

    // ---- policy and read views ---------------------------------------------

    /// Install the policy the automation flags imply.
    pub fn set_policy(&mut self, policy: WorldPolicy) {
        self.policy = policy;
    }

    /// The open prompt, if any.
    #[must_use]
    pub fn prompt(&self) -> Option<&Prompt> {
        self.prompt.as_ref()
    }

    /// What has been said since the last [`DecreeWorld::clear_narration`].
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

    fn narrate(&mut self, kind: NarrationKind, text: impl Into<String>) {
        self.narration.push(say(kind, text));
    }

    /// Everything in flight.
    #[must_use]
    pub fn wire(&self) -> &[InFlight] {
        &self.wire
    }

    /// The value chosen, if one is.
    #[must_use]
    pub fn chosen(&self) -> Option<&(Ballot, Command)> {
        self.chosen.as_ref()
    }

    /// Every Phase 1 that completed, in order.
    #[must_use]
    pub fn completed_phase1(&self) -> &[CompletedPhase1] {
        &self.completed
    }

    /// The one thing this world must never be able to do: hold two different
    /// values for [`DECREE`]. `None` is the invariant holding.
    ///
    /// The branch that sets it is a `Commit` at or below a record this
    /// acceptor already holds, carrying a *different* command. It is
    /// unreachable through the protocol — one value is chosen, and every
    /// later ballot's P2c re-proposes exactly it, so a duplicate or replayed
    /// `Commit` always carries the same command — and it used to be a silent
    /// `return`, which is the worst possible answer: the game would quietly
    /// swallow the very outcome it exists to say is impossible. It is
    /// surfaced instead (a `violation` narration and a failed goal), and the
    /// core is still never handed the contradiction, whose own agreement
    /// assert would abort the wasm module with no stack.
    #[must_use]
    pub fn violation(&self) -> Option<&str> {
        self.violation.as_deref()
    }

    /// What acceptor `id` has promised and accepted.
    #[must_use]
    pub fn acceptor_state(&self, id: u64) -> Option<(Ballot, Option<(Ballot, Command)>)> {
        let acceptor = self.acceptors.iter().find(|a| a.id == NodeId(id))?;
        Some((
            acceptor.role.promised(),
            acceptor.role.record(DECREE).cloned(),
        ))
    }

    /// The acceptors a phase's messages currently reach.
    #[must_use]
    pub fn reach(&self, phase: Phase) -> &[NodeId] {
        match phase {
            Phase::One => &self.phase1_reach,
            Phase::Two => &self.phase2_reach,
        }
    }

    /// The next message an automation pump would deliver.
    #[must_use]
    pub fn next_auto_delivery(&self, beats: bool, replies: bool) -> Option<u64> {
        // Single-decree Paxos has no leader and therefore no beat: the
        // heartbeat flag has nothing to deliver here.
        let _ = beats;
        self.wire
            .iter()
            .filter(|entry| {
                replies
                    && matches!(
                        entry.envelope,
                        Envelope::Node(
                            Message::Promise { .. }
                                | Message::Accepted { .. }
                                | Message::Nack { .. }
                        )
                    )
            })
            .map(|entry| entry.id)
            .min()
    }

    // ---- verbs -------------------------------------------------------------

    /// A proposer opens Phase 1 at its next ballot for `text`.
    ///
    /// The round is the proposer's own last round plus one — never the highest
    /// round anybody has seen. That is what a real proposer knows: a `Nack`
    /// carries the ballot it *refused*, deliberately not the promise that
    /// refused it, so an untrusted wire value can never select a future
    /// campaign's round. Escalating one round at a time is how the duel ends.
    ///
    /// The role is rebuilt for each ballot: a campaign tallies its own
    /// promises and nobody else's.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn open_ballot(&mut self, proposer: u64, text: &str) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self
            .proposers
            .iter()
            .position(|p| p.id == NodeId(proposer))
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no proposer {proposer} in this level"),
                )
            })?;
        let round = self.proposers[index].last_round + 1;
        let ballot = Ballot {
            round,
            node: NodeId(proposer),
        };
        let command = value(proposer, round, text);
        {
            let entry = &mut self.proposers[index];
            entry.role = Proposer::new();
            entry.value = Some(command.clone());
            entry.proposing = None;
            entry.ballot = Some(ballot);
            entry.last_round = round;
            entry.attempt = Attempt::Phase1;
        }
        let config = self.config.clone();
        let targets = self.proposers[index].role.open_phase1(
            Campaign {
                me: None,
                ballot,
                config: config.clone(),
                prior: vec![config],
                from_slot: DECREE,
            },
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        let reach = self.phase1_reach.clone();
        let asked: Vec<NodeId> = targets
            .iter()
            .copied()
            .filter(|to| reach.contains(to))
            .collect();
        self.narrate(
            NarrationKind::Election,
            format!(
                "Proposer {proposer} opens ballot {} for {text:?} and sends Prepare to {}. Phase \
                 1 names no value. It claims the ballot, and it asks what the acceptors have \
                 already accepted.",
                show_ballot(ballot),
                list_nodes(asked)
            ),
        );
        for to in targets {
            if !reach.contains(&to) {
                continue;
            }
            self.send(
                NodeId(proposer),
                to,
                Message::Prepare {
                    reply_to: NodeId(proposer),
                    ballot,
                    from_slot: DECREE,
                    config: None,
                },
            );
        }
        Ok(())
    }

    /// Restrict which acceptors a phase's messages reach.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn set_reach(&mut self, phase: Phase, nodes: &[u64]) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let members: Vec<NodeId> = nodes.iter().copied().map(NodeId).collect();
        if members.is_empty() || !members.iter().all(|id| self.config.contains(*id)) {
            return Err(ActionError::new(
                ActionErrorCode::BadReach,
                "a reach set is a non-empty subset of the acceptors",
            ));
        }
        let named = list_nodes(members.clone());
        match phase {
            Phase::One => self.phase1_reach = members,
            Phase::Two => self.phase2_reach = members,
        }
        self.narrate(
            NarrationKind::Info,
            format!(
                "{} now reaches {named} and nobody else.",
                match phase {
                    Phase::One => "Phase 1",
                    Phase::Two => "Phase 2",
                }
            ),
        );
        Ok(())
    }

    /// Deliver the in-flight message `id`.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn deliver(&mut self, id: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let position = self.position_of(id)?;
        let entry = self.wire.remove(position);
        let summary = entry.view(self.config.quorum_system()).summary;
        // The single-decree world runs bare roles: every entry on its wire is
        // a node message, and it has no matchmaker plane at all.
        let (Party::Node(to), Envelope::Node(message)) = (entry.to, entry.envelope) else {
            return Ok(());
        };
        if let Some(prompt) = self.prompt_for(to, &message) {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "{summary} stops at {}: {} You answer for it, and the real state machine \
                     marks the answer.",
                    actor(to),
                    prompt.question
                ),
            );
            self.prompt = Some(prompt);
            self.paused = Some(Paused::Message {
                to,
                message: Box::new(message),
            });
            return Ok(());
        }
        self.route(to, message);
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
        let summary = self.wire[position]
            .view(self.config.quorum_system())
            .summary;
        self.wire.remove(position);
        self.narrate(
            NarrationKind::Info,
            format!(
                "{summary} is lost. This game has no partition object. A partition is you not \
                 delivering a message."
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
            if !self.config.contains(to) && self.proposer_index(to).is_none() {
                return Err(ActionError::new(
                    ActionErrorCode::UnknownNode,
                    format!("there is no node {} in this world", to.0),
                ));
            }
            copy.to = Party::Node(to);
        }
        let summary = copy.view(self.config.quorum_system()).summary;
        self.next_message_id += 1;
        self.wire.push(copy);
        self.narrate(
            NarrationKind::Info,
            format!("A second copy of {summary} is on the wire."),
        );
        Ok(())
    }

    /// Answer the open prompt (see [`crate::world::World::answer`]).
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
        match self.paused.take() {
            Some(Paused::Message { to, message }) => self.route(to, *message),
            Some(Paused::Phase2 { proposer }) => self.open_phase2(proposer),
            None => {}
        }
        Ok(Verdict::Right)
    }

    // ---- routing -----------------------------------------------------------

    fn route(&mut self, to: NodeId, message: Message) {
        match message {
            Message::Prepare { ballot, .. } => self.on_prepare(to, ballot),
            Message::Accept {
                ballot,
                slot,
                command,
                ..
            } => self.on_accept(to, ballot, slot, &command),
            Message::Commit {
                ballot, command, ..
            } => self.on_commit(to, ballot, &command),
            Message::Promise {
                from,
                ballot,
                from_slot,
                accepted,
                faulty,
                next_from_slot,
            } => self.on_promise(
                to,
                from,
                ballot,
                from_slot,
                accepted,
                faulty,
                next_from_slot,
            ),
            Message::Accepted {
                from,
                ballot,
                slot,
                vhash,
            } => self.on_accepted(to, from, ballot, slot, vhash),
            Message::Nack { ballot, .. } => self.on_nack(to, ballot),
            _ => {}
        }
    }

    fn on_prepare(&mut self, to: NodeId, ballot: Ballot) {
        let Some(index) = self.acceptor_index(to) else {
            return;
        };
        let held = self.acceptors[index].role.promised();
        let reply = {
            let acceptor = &mut self.acceptors[index];
            match acceptor.role.prepare(ballot, DECREE, &mut acceptor.disk) {
                PrepareOutcome::Promised { .. } => {
                    let page = acceptor.role.promise_page(DECREE);
                    Message::Promise {
                        from: to,
                        ballot,
                        from_slot: DECREE,
                        accepted: page.accepted,
                        faulty: page.faulty,
                        next_from_slot: page.next_from_slot,
                    }
                }
                PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Message::Nack {
                    from: to,
                    ballot,
                    slot: DECREE,
                },
            }
        };
        let reported = self.acceptors[index]
            .role
            .record(DECREE)
            .map(|(at, command)| (*at, command.clone()));
        let (kind, text) = match &reply {
            Message::Nack { .. } => (
                NarrationKind::Nack,
                format!(
                    "{} receives Prepare {}. It has already promised {}, so it refuses. A \
                     promise is the only fence Paxos has. An acceptor that withdraws a promise \
                     lets two values be chosen for one slot.",
                    actor(to),
                    show_ballot(ballot),
                    show_ballot(held)
                ),
            ),
            _ => (
                NarrationKind::Promise,
                format!(
                    "{} receives Prepare {}. Its promise was {}, so it promises {} and reports \
                     what it accepted: {}.",
                    actor(to),
                    show_ballot(ballot),
                    show_ballot(held),
                    show_ballot(ballot),
                    reported.map_or_else(
                        || "nothing".to_string(),
                        |(at, command)| format!(
                            "{} at ballot {}",
                            show_command(&command),
                            show_ballot(at)
                        )
                    )
                ),
            ),
        };
        self.narrate(kind, text);
        self.send(to, ballot.node, reply);
    }

    fn on_accept(&mut self, to: NodeId, ballot: Ballot, slot: Slot, command: &Command) {
        let Some(index) = self.acceptor_index(to) else {
            return;
        };
        let held = self.acceptors[index].role.promised();
        let reply = {
            let acceptor = &mut self.acceptors[index];
            match acceptor.role.admit(ballot, slot) {
                AcceptOutcome::Admitted => {
                    // Accepting at a ballot is also promising it, and the
                    // promise write always precedes the record it covers.
                    acceptor.role.set_promise(ballot, &mut acceptor.disk);
                    acceptor.role.record_accepted(
                        slot,
                        ballot,
                        command.clone(),
                        &mut acceptor.disk,
                    );
                    Message::Accepted {
                        from: to,
                        ballot,
                        slot,
                        vhash: command.fingerprint(),
                    }
                }
                AcceptOutcome::Refused | AcceptOutcome::BelowFloor => Message::Nack {
                    from: to,
                    ballot,
                    slot,
                },
            }
        };
        let (kind, text) = match &reply {
            Message::Nack { .. } => (
                NarrationKind::Nack,
                format!(
                    "{} receives Accept {} for {}. It promised {}, which is higher, so it \
                     refuses the vote. The ballot it holds the fence for may already have chosen \
                     a value.",
                    actor(to),
                    show_ballot(ballot),
                    show_command(command),
                    show_ballot(held)
                ),
            ),
            _ => (
                NarrationKind::Accept,
                format!(
                    "{} votes for {} at ballot {}. Its promise was {}, and it refuses nothing at \
                     or above that promise. It re-affirms the promise and writes the record down \
                     before the Accepted reports it.",
                    actor(to),
                    show_command(command),
                    show_ballot(ballot),
                    show_ballot(held)
                ),
            ),
        };
        self.narrate(kind, text);
        self.send(to, ballot.node, reply);
    }

    fn on_commit(&mut self, to: NodeId, ballot: Ballot, command: &Command) {
        let Some(index) = self.acceptor_index(to) else {
            return;
        };
        // A `Commit` at or below a record this acceptor already holds, carrying
        // a *different* command, is two values for one slot. It is not handed
        // to the core — whose own agreement assert would abort the module with
        // no stack, and the game always validates before it calls the core —
        // but it is not swallowed either: it is the one outcome this whole
        // game exists to say cannot happen, so it is said out loud. See
        // `DecreeWorld::violation`.
        let contradiction = self.acceptors[index]
            .role
            .record(DECREE)
            .filter(|(held_at, held)| ballot <= *held_at && *held != *command)
            .map(|(held_at, held)| (*held_at, held.clone()));
        if let Some((held_at, held)) = contradiction {
            let detail = format!(
                "{} was told slot {} holds {} at ballot {}, and it already holds {} at ballot \
                 {}. That is two values for one slot. Paxos states that this result is \
                 impossible. Nothing in this game can produce it through the protocol. If you \
                 read this line, the game is wrong, and Paxos is not.",
                actor(to),
                DECREE.0,
                show_command(command),
                show_ballot(ballot),
                show_command(&held),
                show_ballot(held_at)
            );
            self.violation = Some(detail.clone());
            self.narrate(NarrationKind::Violation, detail);
            return;
        }
        let acceptor = &mut self.acceptors[index];
        let promise = acceptor.role.promised().max(ballot);
        acceptor.role.set_promise(promise, &mut acceptor.disk);
        acceptor
            .role
            .record_accepted(DECREE, ballot, command.clone(), &mut acceptor.disk);
        self.narrate(
            NarrationKind::Accept,
            format!(
                "{} learns the decision: it records {} at ballot {}, whether or not it ever \
                 voted for it.",
                actor(to),
                show_command(command),
                show_ballot(ballot)
            ),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn on_promise(
        &mut self,
        to: NodeId,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: BTreeMap<Slot, (Ballot, Command)>,
        faulty: BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) {
        let Some(index) = self.proposer_index(to) else {
            return;
        };
        self.proposers[index].role.fold_promise(
            from,
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
        );
        if self.proposers[index].attempt != Attempt::Phase1 {
            return;
        }
        let promised: Vec<NodeId> = self.proposers[index]
            .role
            .election()
            .map(|election| election.promised().iter().copied().collect())
            .unwrap_or_default();
        let members = self.config.members().len();
        // A bare proposer holds no promise of its own, so the win gate's
        // promise argument is the zero ballot.
        let won = self.proposers[index].role.phase1_won(Ballot::zero());
        let needed = phase1_note(self.config.quorum_system(), members);
        self.narrate(
            NarrationKind::Promise,
            format!(
                "Proposer {} holds Promises from {}. That is {} of {members}. {needed} {}",
                to.0,
                list_nodes(promised.clone()),
                promised.len(),
                if won {
                    "Phase 1 is complete."
                } else {
                    "That is not enough yet, so it may propose nothing."
                }
            ),
        );
        if !won {
            return;
        }
        if self.policy.manual.contains(&PromptKind::ProposerValue) {
            let own = self.proposers[index]
                .value
                .clone()
                .unwrap_or(Command::Control(paros_core::Control::Noop));
            let reported = self.proposers[index]
                .role
                .clone()
                .close_phase1(|_| false)
                .recovered
                .get(&DECREE)
                .cloned();
            let id = self.take_prompt_id();
            self.prompt = Some(Prompt::proposer_value(
                id,
                to,
                ballot,
                &own,
                reported.as_ref(),
            ));
            self.paused = Some(Paused::Phase2 { proposer: to });
            return;
        }
        self.open_phase2(to);
    }

    /// Close a won Phase 1 and start Phase 2 with whatever P2c selected.
    fn open_phase2(&mut self, proposer: NodeId) {
        let Some(index) = self.proposer_index(proposer) else {
            return;
        };
        let Some(ballot) = self.proposers[index].ballot else {
            return;
        };
        let outcome = self.proposers[index].role.close_phase1(|_| false);
        let own = self.proposers[index]
            .value
            .clone()
            .unwrap_or(Command::Control(paros_core::Control::Noop));
        let candidate = outcome
            .recovered
            .get(&DECREE)
            .map_or(own, |(_, command)| command.clone());
        let adopted = outcome.recovered.get(&DECREE).cloned();
        self.narrate(
            NarrationKind::Info,
            match &adopted {
                Some((at, command)) => format!(
                    "A promise reported {} accepted at ballot {}. The value-selection rule (P2c) \
                     makes proposer {} propose that value again, instead of its own. One report \
                     is exactly what an already-chosen value looks like from here.",
                    show_command(command),
                    show_ballot(*at),
                    proposer.0
                ),
                None => format!(
                    "No acceptor reported a value, so proposer {} may propose its own. Quorum \
                     intersection says that a member of this quorum would have reported a value \
                     already chosen.",
                    proposer.0
                ),
            },
        );
        self.proposers[index]
            .role
            .open_round(DECREE, ballot, candidate.clone(), None, None);
        self.proposers[index].attempt = Attempt::Phase2;
        self.proposers[index].proposing = Some(candidate.clone());
        self.completed.push(CompletedPhase1 {
            ballot,
            reach: self.phase1_reach.clone(),
            proposed: candidate.clone(),
        });
        let reach = self.phase2_reach.clone();
        let addressed: Vec<NodeId> = self
            .config
            .phase2_addressees(None)
            .into_iter()
            .filter(|to| reach.contains(to))
            .collect();
        self.narrate(
            NarrationKind::Accept,
            format!(
                "Phase 2 begins: Accept {} at ballot {} to {}.",
                show_command(&candidate),
                show_ballot(ballot),
                list_nodes(addressed)
            ),
        );
        for to in self.config.phase2_addressees(None) {
            if !reach.contains(&to) {
                continue;
            }
            self.send(
                proposer,
                to,
                Message::Accept {
                    reply_to: paros_core::Party::Node(proposer),
                    leader: proposer,
                    ballot,
                    slot: DECREE,
                    command: candidate.clone(),
                    config: None,
                },
            );
        }
    }

    fn on_accepted(&mut self, to: NodeId, from: NodeId, ballot: Ballot, slot: Slot, vhash: u64) {
        let Some(index) = self.proposer_index(to) else {
            return;
        };
        self.proposers[index]
            .role
            .fold_accepted(from, ballot, slot, vhash);
        let config = self.config.clone();
        let voters = crate::narration::votes_of(self.proposers[index].role.rounds(), DECREE);
        let members = config.members().len();
        let needed = phase2_note(config.quorum_system(), members);
        let Some((at, command)) = self.proposers[index].role.decided(DECREE, &config) else {
            let count = voters.len();
            self.narrate(
                NarrationKind::Info,
                format!(
                    "Proposer {} has {count} of {members} votes at ballot {}. {needed} Nothing is \
                     chosen yet.",
                    to.0,
                    show_ballot(ballot)
                ),
            );
            return;
        };
        self.narrate(
            NarrationKind::Chosen,
            format!(
                "Slot {} is chosen: {} voted for {} at ballot {}. That is {} of {members} \
                 acceptors. {needed} This decision is final. Every Phase-1 quorum of a higher \
                 ballot meets this set of voters, so every later proposer learns the value and \
                 must propose it again.",
                DECREE.0,
                list_nodes(voters.iter().copied()),
                show_command(&command),
                show_ballot(at),
                voters.len()
            ),
        );
        self.proposers[index].role.close_round(DECREE);
        self.proposers[index].attempt = Attempt::Won;
        if self.chosen.is_none() {
            self.chosen = Some((at, command.clone()));
        }
        for member in config.members() {
            self.send(
                to,
                *member,
                Message::Commit {
                    from: paros_core::Party::Node(to),
                    ballot: at,
                    slot: DECREE,
                    command: command.clone(),
                },
            );
        }
    }

    fn on_nack(&mut self, to: NodeId, ballot: Ballot) {
        let Some(index) = self.proposer_index(to) else {
            return;
        };
        if self.proposers[index].ballot == Some(ballot)
            && self.proposers[index].attempt != Attempt::Won
        {
            self.proposers[index].attempt = Attempt::Preempted;
            self.narrate(
                NarrationKind::Nack,
                format!(
                    "Proposer {}'s ballot {} is preempted. Some acceptor has promised a higher \
                     ballot. Notice what the Nack does *not* carry: the promise that refused it. \
                     A proposer raises its ballot one round at a time, from what it knows.",
                    to.0,
                    show_ballot(ballot)
                ),
            );
        }
    }

    // ---- prompts -----------------------------------------------------------

    fn prompt_for(&mut self, to: NodeId, message: &Message) -> Option<Prompt> {
        let manual = |kind: PromptKind| self.policy.manual.contains(&kind);
        let index = self.acceptor_index(to)?;
        match message {
            Message::Prepare {
                ballot, from_slot, ..
            } if manual(PromptKind::AcceptorPrepare) => {
                let acceptor = &self.acceptors[index];
                let promised = acceptor.role.promised();
                let floor = acceptor.role.first_slot();
                let mut clone = acceptor.role.clone();
                let mut writes: Vec<AcceptorWrite<Command>> = Vec::new();
                let outcome = clone.prepare(*ballot, *from_slot, &mut writes);
                let id = self.take_prompt_id();
                Some(Prompt::acceptor_prepare(
                    id, to, *ballot, *from_slot, promised, floor, outcome,
                ))
            }
            Message::Accept {
                ballot,
                slot,
                command,
                ..
            } if manual(PromptKind::AcceptorAccept) => {
                let acceptor = &self.acceptors[index];
                let outcome = acceptor.role.admit(*ballot, *slot);
                let promised = acceptor.role.promised();
                let id = self.take_prompt_id();
                Some(Prompt::acceptor_accept(
                    id, to, *ballot, *slot, command, promised, outcome,
                ))
            }
            Message::Commit {
                ballot, command, ..
            } if manual(PromptKind::CommitOverwrite) => {
                let acceptor = &self.acceptors[index];
                let (held_at, held) = acceptor.role.record(DECREE)?;
                if *held_at >= *ballot || held == command {
                    return None;
                }
                let (held_at, held) = (*held_at, held.clone());
                let id = self.take_prompt_id();
                Some(Prompt::commit_overwrite(
                    id, to, DECREE, *ballot, command, held_at, &held,
                ))
            }
            _ => None,
        }
    }

    // ---- helpers -----------------------------------------------------------

    fn send(&mut self, from: NodeId, to: NodeId, message: Message) {
        self.wire.push(InFlight {
            id: self.next_message_id,
            from: Party::Node(from),
            to: Party::Node(to),
            envelope: Envelope::Node(message),
            sent_at: 0,
        });
        self.next_message_id += 1;
    }

    fn acceptor_index(&self, id: NodeId) -> Option<usize> {
        self.acceptors.iter().position(|a| a.id == id)
    }

    fn proposer_index(&self, id: NodeId) -> Option<usize> {
        self.proposers.iter().position(|p| p.id == id)
    }

    fn take_prompt_id(&mut self) -> u64 {
        let id = self.next_prompt_id;
        self.next_prompt_id += 1;
        id
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
