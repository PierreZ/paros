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
    AcceptorWrite, Ballot, ClientId, ClientSeq, Command, Message, NodeId, QuorumSystem, Slot,
    WriteOp,
};

use crate::prompt::{Prompt, PromptKind, RepairCase};
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
            .or_else(|| self.quorum_read_prompt(to, index, message))
            .or_else(|| self.repair_prompt(to, index, message))
            .or_else(|| self.snapshot_prompt(to, index, message))
    }

    /// The question a grid leader is asked before it proposes: which column
    /// takes the slot it is about to allocate?
    ///
    /// Judged by the configuration's own
    /// [`column_of`](paros_core::AcceptorConfig::column_of), so the answer is
    /// the rule itself and not a modulus restated here.
    pub(super) fn grid_column_prompt(&mut self, to: NodeId, index: usize) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::GridColumn) {
            return None;
        }
        let node = self.nodes[index].as_ref()?;
        if !node.is_leader() {
            return None;
        }
        let QuorumSystem::Grid { cols, .. } = node.acceptors().quorum_system() else {
            return None;
        };
        let slot = node.proposer().next_slot();
        let expected = node.acceptors().column_of(slot)?;
        let id = self.take_prompt_id();
        Some(Prompt::grid_column(id, to, slot, cols, expected))
    }

    /// The question an erased disk raises when its node asks to come back.
    ///
    /// There is no role to clone here — the store holds no promise, which is
    /// the whole problem — so the prompt's answer is a constant and its doc
    /// comment says why.
    pub(super) fn wiped_rejoin_prompt(&mut self, to: NodeId, index: usize) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::WipedRejoin) {
            return None;
        }
        let promised = self.promise_watermarks[index];
        let id = self.take_prompt_id();
        Some(Prompt::wiped_rejoin(id, to, promised))
    }

    /// The question a quorum read's last answer raises: the row has answered,
    /// so serve the read, or wait for this node's own prefix to reach the
    /// index the row settled on?
    ///
    /// Judged on a **clone of the node's own quorum reads**, folded with this
    /// answer and served with the replica's own `covers`.
    fn quorum_read_prompt(
        &mut self,
        to: NodeId,
        index: usize,
        message: &Message,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::QuorumReadServe) {
            return None;
        }
        let Message::PreReadAck {
            from,
            ctx,
            watermark,
            config_since,
        } = message
        else {
            return None;
        };
        let node = self.nodes[index].as_ref()?;
        let mut clone = node.quorum_reads().clone();
        if clone.fold(*ctx, *from, *watermark, *config_since) != paros_core::PreReadFold::Counted {
            return None;
        }
        // What the row settled on, and whether this node may answer with it:
        // both come from the clone, driven exactly as the node drives the real
        // one.
        let replica = node.replica().clone();
        let applied = replica.chosen_index();
        let served = clone.serve(|at| replica.covers(at));
        let confirmed = clone
            .pending()
            .iter()
            .find(|read| read.ctx() == *ctx)
            .and_then(paros_core::quorum_read::QuorumRead::confirmed_index);
        let (settled, answered) = match confirmed {
            Some(index) => (
                index,
                clone
                    .pending()
                    .iter()
                    .find(|read| read.ctx() == *ctx)
                    .map_or(0, |read| read.watermarks().len()),
            ),
            // The read left the tally, so it was served: the index it was
            // served at is the one the row settled on.
            None => (
                served
                    .iter()
                    .find(|(served_ctx, _)| *served_ctx == *ctx)
                    .and_then(|(_, at)| *at),
                node.acceptors()
                    .phase1_addressees(node.acceptors().row_of(*ctx))
                    .len(),
            ),
        };
        let serve = served.iter().any(|(served_ctx, _)| *served_ctx == *ctx);
        // A row that has not answered whole yet asks nothing: there is no
        // index to serve or wait for.
        if !serve && confirmed.is_none() {
            return None;
        }
        let id = self.take_prompt_id();
        Some(Prompt::quorum_read_serve(
            id, to, *ctx, settled, applied, answered, serve,
        ))
    }

    /// The question a straggler's `Promise` raises at a leader whose repair
    /// probe is still blocked: which CTRL case does this answer put the
    /// damaged slot in?
    ///
    /// Judged on a **clone of the proposer**: the page is folded through
    /// `fold_probe_promise` and the probe resolved through `resolve_probe`,
    /// exactly as the node does it.
    fn repair_prompt(&mut self, to: NodeId, index: usize, message: &Message) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::RepairVerdict) {
            return None;
        }
        let Message::Promise {
            from,
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
        } = message
        else {
            return None;
        };
        let node = self.nodes[index].as_ref()?;
        let probe = node.proposer().probe()?;
        let slot = *probe.blocked().iter().next()?;
        let mut clone = node.proposer().clone();
        clone.fold_probe_promise(
            *from,
            *ballot,
            *from_slot,
            accepted,
            faulty,
            *next_from_slot,
        );
        let decisions = clone.resolve_probe();
        let decision = decisions.iter().find(|decision| decision.slot == slot);
        let expected = match decision {
            Some(decision) if decision.command.is_some() => RepairCase::ReproposeReported,
            Some(_) => RepairCase::FillNoop,
            None => RepairCase::Wait,
        };
        // What the reports hold for the slot, taken from the same clone: the
        // value it would re-propose, or — while it is still blocked — the
        // highest report the page just added.
        let reported = decision
            .and_then(|decision| decision.command.clone())
            .or_else(|| accepted.get(&slot).map(|(_, command)| command.clone()));
        let faulty_at = faulty
            .get(&slot)
            .copied()
            .or_else(|| node.acceptor().faulty().get(&slot).copied())
            .or_else(|| self.reported_faulty(slot));
        let id = self.take_prompt_id();
        Some(Prompt::repair_verdict(
            id,
            to,
            slot,
            reported.as_ref(),
            faulty_at,
            expected,
        ))
    }

    /// The ballot some acceptor's damaged record for `slot` was accepted at,
    /// read off the disks the world owns — what the earlier `Promise` that
    /// blocked the probe reported.
    fn reported_faulty(&self, slot: Slot) -> Option<Ballot> {
        self.disks
            .iter()
            .find_map(|disk| disk.faulty().get(&slot).copied())
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
