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
use crate::prompt::{Prompt, PromptKind, Verdict};
use crate::world::{InFlight, WorldPolicy};

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
        assert!(!acceptors.is_empty(), "a decree world has acceptors");
        let members: Vec<NodeId> = acceptors.iter().copied().map(NodeId).collect();
        let config = AcceptorConfig::new(members.clone(), QuorumSystem::Majority);
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
                        entry.message,
                        Message::Promise { .. } | Message::Accepted { .. } | Message::Nack { .. }
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
        match phase {
            Phase::One => self.phase1_reach = members,
            Phase::Two => self.phase2_reach = members,
        }
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
        if let Some(prompt) = self.prompt_for(entry.to, &entry.message) {
            self.prompt = Some(prompt);
            self.paused = Some(Paused::Message {
                to: entry.to,
                message: Box::new(entry.message),
            });
            return Ok(());
        }
        self.route(entry.to, entry.message);
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
        self.wire.push(copy);
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
        if prompt.judge(choice) == Verdict::Wrong {
            return Ok(Verdict::Wrong);
        }
        self.prompt = None;
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
        self.send(to, ballot.node, reply);
    }

    fn on_accept(&mut self, to: NodeId, ballot: Ballot, slot: Slot, command: &Command) {
        let Some(index) = self.acceptor_index(to) else {
            return;
        };
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
        self.send(to, ballot.node, reply);
    }

    fn on_commit(&mut self, to: NodeId, ballot: Ballot, command: &Command) {
        let Some(index) = self.acceptor_index(to) else {
            return;
        };
        let acceptor = &mut self.acceptors[index];
        // A replayed or duplicated `Commit` below a record this acceptor
        // already holds is refused here rather than handed to the role, whose
        // own agreement assert would abort: the game validates before it calls
        // the core, always.
        if let Some((held_at, held)) = acceptor.role.record(DECREE)
            && ballot <= *held_at
            && held != command
        {
            return;
        }
        let promise = acceptor.role.promised().max(ballot);
        acceptor.role.set_promise(promise, &mut acceptor.disk);
        acceptor
            .role
            .record_accepted(DECREE, ballot, command.clone(), &mut acceptor.disk);
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
        // A bare proposer holds no promise of its own, so the win gate's
        // promise argument is the zero ballot.
        if !self.proposers[index].role.phase1_won(Ballot::zero()) {
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
        for to in self.config.phase2_addressees(None) {
            if !reach.contains(&to) {
                continue;
            }
            self.send(
                proposer,
                to,
                Message::Accept {
                    reply_to: proposer,
                    leader: proposer,
                    ballot,
                    slot: DECREE,
                    command: candidate.clone(),
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
        let Some((at, command)) = self.proposers[index].role.decided(DECREE, &config) else {
            return;
        };
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
                    from: to,
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
            from,
            to,
            message,
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
