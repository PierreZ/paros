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

use paros_core::{AcceptorWrite, Ballot, Command, Message, NodeId, Slot};

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
    }

    /// The acceptor's two rules: `>` to promise, `>=` to vote.
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
                self.replica_apply_prompt(to, index, *slot)
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
                clone.decided(*slot, node.acceptors())?;
                self.replica_apply_prompt(to, index, *slot)
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
    fn replica_apply_prompt(&mut self, to: NodeId, index: usize, slot: Slot) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::ReplicaApply) {
            return None;
        }
        let node = self.nodes[index].as_ref()?;
        if node.replica().is_chosen(slot) {
            return None;
        }
        let chosen_index = node.replica().chosen_index();
        let first_unchosen = node.replica().first_unchosen();
        let id = self.take_prompt_id();
        Some(Prompt::replica_apply(
            id,
            to,
            slot,
            chosen_index,
            first_unchosen,
        ))
    }
}
