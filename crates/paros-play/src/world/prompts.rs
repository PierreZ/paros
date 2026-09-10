//! Which question a delivery raises at the node it reaches, and the state the
//! answer rests on.
//!
//! Every answer is computed here on a **clone of the role**, never on the node
//! itself: [`Acceptor`](paros_core::acceptor::Acceptor),
//! [`Proposer`](paros_core::proposer::Proposer) and
//! [`Replica`](paros_core::replica::Replica) are all `Clone`, and
//! `ColocatedNode` hands them out read-only. So the world can ask "what would
//! `paros-core` do with this message?" without doing it — which is what lets a
//! wrong answer cost a mistake instead of a state.

use paros_core::{
    AcceptorWrite, Ballot, ClientId, ClientSeq, Command, Message, NodeId, Slot, WriteOp,
};

use crate::prompt::{Prompt, PromptKind};
use crate::world::World;

impl World {
    /// The prompt a delivery raises at `to`, if any.
    ///
    /// Every answer is computed on a **clone of the role** — the acceptor for
    /// the two vote rules, the proposer for a decision or a read — so the real
    /// node is untouched until the player is right. The three helpers below
    /// take disjoint message kinds, so their order is presentation, not
    /// precedence.
    pub(super) fn prompt_for(&mut self, to: NodeId, message: &Message) -> Option<Prompt> {
        let index = self.index_of(to)?;
        self.nodes[index].as_ref()?;
        self.vote_prompt(to, index, message)
            .or_else(|| self.learn_prompt(to, index, message))
            .or_else(|| self.read_prompt(to, index, message))
            .or_else(|| self.snapshot_prompt(to, index, message))
    }

    /// The question a peer's snapshot raises at a node stranded below the
    /// cluster's floor: what is its durable promise afterwards?
    ///
    /// Judged on a clone of the acceptor, driven exactly as
    /// `ColocatedNode::on_install_snapshot` drives the real one.
    fn snapshot_prompt(&mut self, to: NodeId, index: usize, message: &Message) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::SnapshotPromise) {
            return None;
        }
        let Message::InstallSnapshot {
            ballot,
            chosen_index,
            snapshot,
            sessions,
            ..
        } = message
        else {
            return None;
        };
        let node = self.nodes[index].as_ref()?;
        // A snapshot the core would ignore teaches nothing: it neither
        // installs nor moves the promise, so there is no decision to play.
        if node
            .replica()
            .chosen_index()
            .is_some_and(|ci| *chosen_index <= ci)
        {
            return None;
        }
        let held = node.acceptor().promised();
        let mut clone = node.acceptor().clone();
        let mut writes: Vec<WriteOp> = Vec::new();
        if *ballot > clone.promised() {
            clone.set_promise(*ballot, &mut writes);
        }
        clone.install(
            *chosen_index,
            *ballot,
            snapshot.clone(),
            sessions.clone(),
            &mut writes,
        );
        let promised = clone.promised();
        let id = self.take_prompt_id();
        Some(Prompt::snapshot_promise(
            id,
            to,
            *chosen_index,
            *ballot,
            held,
            promised,
        ))
    }

    /// The question a client's retry raises at the leader: which of the three
    /// honest answers is this one?
    ///
    /// Judged on a clone of the replica, through the two ledgers the core
    /// itself consults — and in the order it consults them.
    pub(super) fn ack_write_prompt(
        &mut self,
        to: NodeId,
        index: usize,
        client: ClientId,
        seq: ClientSeq,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::AckWrite) {
            return None;
        }
        let node = self.nodes[index].as_ref()?;
        if !node.is_leader() {
            return None;
        }
        let clone = node.replica().clone();
        let applied_at = clone.applied_at(client, seq);
        let inflight_at = clone.inflight_at(client, seq);
        let chosen_index = clone.chosen_index();
        let id = self.take_prompt_id();
        Some(Prompt::ack_write(
            id,
            to,
            client.0,
            seq.0,
            applied_at,
            inflight_at,
            chosen_index,
        ))
    }

    /// The acceptor's two questions. **One rule governs both**: refuse
    /// anything *below* the promise held, admit anything at or above it. A
    /// `Prepare` reports and fences; an `Accept` records. What differs is what
    /// the answer is *for*, not which comparison it uses.
    fn vote_prompt(&mut self, to: NodeId, index: usize, message: &Message) -> Option<Prompt> {
        let manual = |kind: PromptKind| self.policy.manual.contains(&kind);
        let node = self.nodes[index].as_ref()?;
        match message {
            Message::Prepare {
                ballot, from_slot, ..
            } if manual(PromptKind::AcceptorPrepare) => {
                let promised = node.acceptor().promised();
                let floor = node.acceptor().first_slot();
                let mut clone = node.acceptor().clone();
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
                let outcome = node.acceptor().admit(*ballot, *slot);
                let promised = node.acceptor().promised();
                let id = self.take_prompt_id();
                Some(Prompt::acceptor_accept(
                    id, to, *ballot, *slot, command, promised, outcome,
                ))
            }
            _ => None,
        }
    }

    /// The two learn paths: a `Commit`, and the catch-up replay a lagging node
    /// actually takes. Either can contradict a stale record, and either can
    /// make a slot chosen out of order.
    fn learn_prompt(&mut self, to: NodeId, index: usize, message: &Message) -> Option<Prompt> {
        let manual = |kind: PromptKind| self.policy.manual.contains(&kind);
        let node = self.nodes[index].as_ref()?;
        match message {
            Message::Commit {
                ballot,
                slot,
                command,
                ..
            } => {
                if manual(PromptKind::CommitOverwrite)
                    && let Some(prompt) = self.overwrite_prompt(to, index, *slot, *ballot, command)
                {
                    return Some(prompt);
                }
                self.replica_apply_prompt(to, index, *slot, command)
            }
            Message::CatchUpResponse { entries, .. } if manual(PromptKind::CommitOverwrite) => {
                let contradiction = entries.iter().find_map(|(slot, (ballot, command))| {
                    let (held_at, held) = node.acceptor().record(*slot)?;
                    (*held_at < *ballot && held != command)
                        .then(|| (*slot, *ballot, command.clone()))
                })?;
                let (slot, ballot, command) = contradiction;
                self.overwrite_prompt(to, index, slot, ballot, &command)
            }
            Message::Accepted {
                from,
                ballot,
                slot,
                vhash,
            } if manual(PromptKind::ReplicaApply) => {
                if node.replica().is_chosen(*slot) {
                    return None;
                }
                // Would this ack decide the slot? Ask the proposer's own tally,
                // on a clone.
                let mut clone = node.proposer().clone();
                if !clone.fold_accepted(*from, *ballot, *slot, *vhash) {
                    return None;
                }
                let (_, command) = clone.decided(*slot, node.acceptors())?;
                self.replica_apply_prompt(to, index, *slot, &command)
            }
            _ => None,
        }
    }

    /// The read-index question: does this ack complete the leadership proof?
    fn read_prompt(&mut self, to: NodeId, index: usize, message: &Message) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::ReadServe) {
            return None;
        }
        let Message::HeartbeatAck { from, seq, .. } = message else {
            return None;
        };
        let node = self.nodes[index].as_ref()?;
        if node.proposer().read_rounds().is_empty() {
            return None;
        }
        let mut clone = node.proposer().clone();
        clone.credit_read_ack(*from, *seq);
        let confirmed = !clone
            .confirm_reads(node.acceptors(), node.replica().chosen_index())
            .is_empty();
        let chosen_index = node.replica().chosen_index();
        let read = self
            .clients
            .iter()
            .flat_map(|client| client.reads.iter())
            .find(|read| read.node == to && !read.served)?;
        let (ctx, captured, acks) = (read.ctx, read.index, read.acks.len());
        let id = self.take_prompt_id();
        Some(Prompt::read_serve(
            id,
            to,
            ctx,
            captured,
            acks,
            chosen_index,
            confirmed,
        ))
    }

    /// The `CommitOverwrite` question, when what arrived contradicts a record
    /// this acceptor holds at a lower ballot.
    fn overwrite_prompt(
        &mut self,
        to: NodeId,
        index: usize,
        slot: Slot,
        ballot: Ballot,
        command: &Command,
    ) -> Option<Prompt> {
        let node = self.nodes[index].as_ref()?;
        let (held_at, held) = node.acceptor().record(slot)?;
        if *held_at >= ballot || held == command {
            return None;
        }
        let (held_at, held) = (*held_at, held.clone());
        let id = self.take_prompt_id();
        Some(Prompt::commit_overwrite(
            id, to, slot, ballot, command, held_at, &held,
        ))
    }

    /// The `ReplicaApply` question for a slot that is about to become chosen
    /// at `to`.
    ///
    /// The answer is the **replica's own**: a clone learns the slot and runs
    /// the contiguous walk, and whether the walk surfaced this slot as
    /// committed is the whole judgement. `records_agree` is always true on the
    /// clone — the coupling it asserts is the acceptor's business, and the
    /// real node has already been given (or is about to be given) the record.
    fn replica_apply_prompt(
        &mut self,
        to: NodeId,
        index: usize,
        slot: Slot,
        command: &Command,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::ReplicaApply) {
            return None;
        }
        let node = self.nodes[index].as_ref()?;
        if node.replica().is_chosen(slot) {
            return None;
        }
        let chosen_index = node.replica().chosen_index();
        let first_unchosen = node.replica().first_unchosen();
        let mut clone = node.replica().clone();
        clone.learn(slot, command);
        let mut writes: Vec<WriteOp> = Vec::new();
        clone.advance(|_, _| true, &mut writes);
        let applies_now = clone.committed().iter().any(|(at, _)| *at == slot);
        let id = self.take_prompt_id();
        Some(Prompt::replica_apply(
            id,
            to,
            slot,
            chosen_index,
            first_unchosen,
            applies_now,
        ))
    }
}
