//! The drain contract: one `Ready` batch at a time, from the node to the disk
//! and the wire.
//!
//! This is the loop `crates/paros-core/examples/quorum_read.rs` writes in
//! fifteen lines, with the two things a game needs added: a **durability seam**
//! that can cut a batch in half, and a **prompt** that can hold one back until
//! the player has said what the core did with it.
//!
//! The order never changes, and it is the order `paros::driver::ready` uses
//! with a real disk underneath:
//!
//! 1. `ready()`, copy every bucket out, `advance()` — the guard is never held
//!    across a disk write, a prompt, or a player action.
//! 2. Persist the batch's writes, `Truncate` held back.
//! 3. Send: one wire entry per resolved addressee.
//! 4. Apply `committed` to the application log, then flush the held-back
//!    truncates.
//! 5. Serve the batch's snapshot offers — after the apply, so the bytes read
//!    back really do cover the boundary the message advertises.
//! 6. Answer the batch's read states.
//! 7. `advance_recovery()`, and drain again until the node is quiet.

use std::collections::BTreeMap;

use paros_core::proposer::RecoveryStep;
use paros_core::{
    Ballot, ColocatedNode, Command, Control, GcRequest, MatchReply, MatchRequest, MatchmakerId,
    Message, NodeId, ReadState, Slot, WriteOp,
};

use crate::action::Seam;
use crate::narration::{self, NarrationKind, NodeSnapshot, many, who};
use crate::prompt::{Prompt, PromptKind};
use crate::world::{Envelope, Party, World};

/// How many drain rounds one action may take before the engine calls it a
/// non-terminating loop.
const DRAIN_BUDGET: usize = 512;

/// One batch a node drained, copied out of the borrow guard.
#[derive(Clone, Debug, Default)]
pub(super) struct Batch {
    writes: Vec<WriteOp>,
    messages: Vec<(NodeId, Message)>,
    committed: Vec<(Slot, Command)>,
    read_states: Vec<ReadState>,
    /// `(to, chosen_index, ballot)` per peer the core decided needs a
    /// snapshot. The core holds no application state, so the *world* — which
    /// owns the disks — fills in the opaque bytes (see
    /// [`World::serve_snapshot_offers`]).
    snapshot_offers: Vec<(NodeId, Slot, Ballot)>,
    /// `(started, gap fills, remaining)` when this batch carried a
    /// leader-recovery page — the marker the `LeaderRecovery` prompt gates on.
    recovery: Option<(usize, usize, usize)>,
    /// The registrations this batch owes the matchmakers. They travel with the
    /// batch's other messages, after its writes, for the same reason: a
    /// registration is a claim about the promise the candidate has just made
    /// durable. Always empty on plain Multi-Paxos.
    match_requests: Vec<(MatchmakerId, MatchRequest)>,
    /// The garbage-collection requests this batch owes the matchmakers.
    /// Always empty on plain Multi-Paxos.
    gc_requests: Vec<(MatchmakerId, GcRequest)>,
}

impl Batch {
    fn is_empty(&self) -> bool {
        self.writes.is_empty()
            && self.messages.is_empty()
            && self.committed.is_empty()
            && self.read_states.is_empty()
            && self.snapshot_offers.is_empty()
            && self.match_requests.is_empty()
            && self.gc_requests.is_empty()
    }

    /// What this batch's recovery page did, one entry per slot in slot order.
    ///
    /// The slots come from the batch's own `Accept`s; **what each one means
    /// comes from the core**, through [`World::recovery_plan`], which was read
    /// off the proposer before the call that pumped this page. That
    /// distinction is the whole point: a `Noop` on the wire is a
    /// [`RecoveryStep::Fill`] *or* a [`RecoveryStep::Recovered`] carrying a
    /// predecessor's own gap fill, and only the recovery knows which. Guessing
    /// from the command told the player "the quorum reported nothing" about a
    /// slot a Promise had explicitly reported.
    ///
    /// The fallback is the old guess, and it is unreachable in practice: every
    /// slot the pump starts a round for was handed out by `recovery_next`, so
    /// the plan names it.
    fn recovery_steps(
        &self,
        plan: &BTreeMap<Slot, RecoveryStep<Command>>,
    ) -> Vec<(Slot, RecoveryStep<Command>)> {
        let mut by_slot: BTreeMap<Slot, Command> = BTreeMap::new();
        for (_, message) in &self.messages {
            if let Message::Accept { slot, command, .. } = message {
                by_slot.entry(*slot).or_insert_with(|| command.clone());
            }
        }
        by_slot
            .into_iter()
            .map(|(slot, command)| {
                let guess = if matches!(command, Command::Control(Control::Noop)) {
                    RecoveryStep::Fill
                } else {
                    RecoveryStep::Recovered(command)
                };
                let step = plan.get(&slot).cloned().unwrap_or(guess);
                (slot, step)
            })
            .collect()
    }
}

/// What the world is holding back while a prompt is open.
#[derive(Clone, Debug)]
pub(super) enum Paused {
    /// A delivered message the manual role has not answered for yet.
    Message { node: NodeId, message: Box<Message> },
    /// A client retry the `AckWrite` answer has not been given for yet.
    Retry { node: NodeId, client: u64, seq: u64 },
    /// A client's proposal waiting on the column its Phase 2 goes to.
    Propose {
        node: NodeId,
        client: u64,
        value: String,
    },
    /// A wiped node's boot, waiting on the operator's answer.
    Boot { node: NodeId },
    /// A registration waiting on the matchmaker's generation answer.
    Registration {
        from: Party,
        to: MatchmakerId,
        request: Box<MatchRequest>,
    },
    /// A matchmaker's answer waiting on the candidate's staleness answer.
    MatchReply {
        node: NodeId,
        reply: Box<MatchReply>,
    },
    /// An operator's retire request waiting on the target's answer.
    Retire { target: NodeId, watermark: Ballot },
    /// A drained batch waiting on the persist-order answer.
    Batch { node: NodeId, batch: Box<Batch> },
    /// A drained recovery batch, and the slots still to be quizzed on.
    Recovery {
        node: NodeId,
        batch: Box<Batch>,
        steps: Vec<(Slot, RecoveryStep<Command>)>,
    },
}

impl World {
    /// Hand one message to a node, and narrate what it did about it.
    ///
    /// The receipt line says what arrived and what the node knew; everything
    /// after it is the diff of the node's own accessors across the step (see
    /// [`World::observe`]).
    pub(super) fn step(&mut self, id: NodeId, message: Message) {
        let known = NodeSnapshot::capture(self.node(id));
        let receipt = narration::receipt(id, &message, &known);
        self.narration_push(receipt);
        self.note_stray_vote(id, &message);
        // A won Phase 1 opens *and* pumps its first recovery page inside this
        // one `step`, so the oracle for that page has to be taken now.
        self.plan_recovery(id, Some(&message));
        self.observe(id, move |world| {
            if let Some(index) = world.index_of(id)
                && let Some(node) = world.nodes[index].as_mut()
            {
                node.step(message);
            }
            world.pump(id);
        });
    }

    /// Say so when a vote arrives from an acceptor the slot's Phase-2 quorum
    /// does not contain.
    ///
    /// Under a grid a slot is decided by **one column**, and an acceptor
    /// outside it is a member of the configuration whose vote for that slot
    /// counts toward nothing. The fact is read off the configuration itself
    /// (`is_phase2_addressee` over the slot's own column), so the line is only
    /// ever printed when the tally really is about to ignore the vote.
    fn note_stray_vote(&mut self, id: NodeId, message: &Message) {
        let Message::Accepted { from, slot, .. } = message else {
            return;
        };
        let (from, slot) = (*from, *slot);
        let Some(node) = self.node(id) else {
            return;
        };
        let config = node.acceptors();
        let Some(column) = config.column_of(slot) else {
            return;
        };
        if config.is_phase2_addressee(from, Some(column)) {
            return;
        }
        self.narrate(
            NarrationKind::Info,
            format!(
                "{} does not count that vote. Slot {} belongs to column {column}, and node {} is \
                 not in that column. A Phase-2 quorum of this grid is one whole column, so the \
                 tally does not move. Node {} is a member of the configuration. It is not one of \
                 the acceptors that decide this slot.",
                who(id),
                slot.0,
                from.0,
                from.0
            ),
        );
    }

    /// Read off the proposer, **before** the call that pumps it, what the core
    /// will do with each slot of the recovery page that call produces.
    ///
    /// Two shapes, because a leadership's recovery is opened in one place and
    /// continued in another:
    ///
    /// - a page after the first is pumped by `advance_recovery`, and the
    ///   recovery is already open when this runs, so a clone's own
    ///   [`paros_core::proposer::Proposer::recovery_next`] answers — with the
    ///   per-slot guards `pump_leader_recovery` re-checks (below the floor,
    ///   already chosen, blocked on the repair probe) applied here too;
    /// - the **first** page is opened and pumped inside the `step` of the
    ///   winning `Promise`, so there is no recovery to clone yet. The campaign
    ///   answers instead: fold that Promise into a clone of the proposer,
    ///   close Phase 1 on it, and the outcome's `recovered` map is exactly the
    ///   map the real `open_recovery` is about to be handed. A slot it does
    ///   not name is a Phase-1-backed gap fill — the licence quorum
    ///   intersection buys — which is what the lookup's absent entry means.
    ///
    /// Either way the answer is the core's own, computed on a clone; nothing
    /// here re-derives a rule.
    pub(super) fn plan_recovery(&mut self, id: NodeId, arriving: Option<&Message>) {
        self.recovery_plan.clear();
        let Some(index) = self.index_of(id) else {
            return;
        };
        let Some(node) = self.nodes[index].as_ref() else {
            return;
        };
        if node.proposer().recovery().is_some() {
            let floor = node.acceptor().first_slot();
            let mut clone = node.proposer().clone();
            let mut plan: BTreeMap<Slot, RecoveryStep<Command>> = BTreeMap::new();
            while let Some((slot, step)) = clone.recovery_next() {
                if slot < floor || node.replica().is_chosen(slot) || clone.recovery_blocked(slot) {
                    continue;
                }
                plan.insert(slot, step);
            }
            self.recovery_plan = plan;
            return;
        }
        let Some(Message::Promise {
            from,
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
        }) = arriving
        else {
            return;
        };
        if node.role() != paros_core::NodeRole::Candidate {
            return;
        }
        let mut clone = node.proposer().clone();
        clone.fold_promise(
            *from,
            *ballot,
            *from_slot,
            accepted.clone(),
            faulty.clone(),
            *next_from_slot,
        );
        if !clone.phase1_won(node.acceptor().promised()) {
            return;
        }
        let outcome = clone.close_phase1(|slot| node.replica().is_chosen(slot));
        self.recovery_plan = outcome
            .recovered
            .into_iter()
            .map(|(slot, (_at, command))| (slot, RecoveryStep::Recovered(command)))
            .collect();
    }

    /// Drain `id` until it is quiet, honouring the seams and the prompts.
    ///
    /// # Panics
    ///
    /// If the drain does not reach quiescence within [`DRAIN_BUDGET`] rounds —
    /// a programmer error, never a player-reachable state.
    pub(super) fn pump(&mut self, id: NodeId) {
        let Some(index) = self.index_of(id) else {
            return;
        };
        let mut idle = 0u8;
        for _ in 0..DRAIN_BUDGET {
            if self.prompt.is_some() {
                self.settle();
                return;
            }
            let Some(batch) = self.take_batch(index) else {
                self.settle();
                return;
            };
            if batch.is_empty() {
                idle += 1;
                if idle > 1 {
                    self.settle();
                    return;
                }
            } else {
                idle = 0;
            }
            let Some(batch) = self.gate_batch(index, batch, false) else {
                self.settle();
                return;
            };
            self.commit_batch(index, batch);
            // The oracle for the page `advance_recovery` is about to pump.
            self.plan_recovery(id, None);
            if let Some(node) = self.nodes[index].as_mut() {
                node.advance_recovery();
            } else {
                self.settle();
                return;
            }
        }
        panic!("the drain loop reaches quiescence");
    }

    /// Raise the prompt this batch owes an answer for, parking it, or hand it
    /// back for committing.
    ///
    /// Both questions are about a batch the core has already produced — the
    /// player is checked against what `paros-core` did, and the batch is what
    /// waits. That is the same shape as every other prompt: the core decides,
    /// and the world holds still until the player has matched it.
    fn gate_batch(&mut self, index: usize, batch: Batch, persisted: bool) -> Option<Batch> {
        let id = self.pool[index];
        if !persisted
            && !batch.writes.is_empty()
            && !batch.messages.is_empty()
            && self.policy.manual.contains(&PromptKind::PersistOrder)
        {
            let prompt_id = self.take_prompt_id();
            self.prompt = Some(Prompt::persist_order(
                prompt_id,
                id,
                batch.writes.len(),
                batch.messages.len(),
            ));
            self.paused = Some(Paused::Batch {
                node: id,
                batch: Box::new(batch),
            });
            return None;
        }
        if batch.recovery.is_some() && self.policy.manual.contains(&PromptKind::LeaderRecovery) {
            let steps = batch.recovery_steps(&self.recovery_plan);
            if !steps.is_empty() {
                self.raise_recovery(index, Box::new(batch), steps);
                return None;
            }
        }
        Some(batch)
    }

    /// Commit a batch whose questions are all answered, and keep draining.
    fn release(&mut self, index: usize, batch: Batch) {
        let id = self.pool[index];
        self.commit_batch(index, batch);
        self.plan_recovery(id, None);
        if let Some(node) = self.nodes[index].as_mut() {
            node.advance_recovery();
        }
        self.pump(id);
    }

    /// One `ready()` / `advance()` round, plus the armed-seam cut.
    ///
    /// Returns `None` when the node is not running, or when the seam fired (the
    /// node is dropped and the caller stops).
    fn take_batch(&mut self, index: usize) -> Option<Batch> {
        let id = self.pool[index];
        let pool = self.pool.clone();
        let node = self.nodes[index].as_mut()?;
        let ready = node.ready();
        let batch = Batch {
            writes: ready.writes().to_vec(),
            messages: ready
                .messages()
                .iter()
                .flat_map(|(audience, message)| {
                    audience
                        .resolve(&pool, id)
                        .into_iter()
                        .map(move |to| (to, message.clone()))
                })
                .collect(),
            committed: ready.committed().to_vec(),
            read_states: ready.read_states().to_vec(),
            snapshot_offers: ready.snapshot_offers().to_vec(),
            recovery: ready.recovery_batch(),
            match_requests: ready.match_requests().to_vec(),
            gc_requests: ready.gc_requests().to_vec(),
        };
        ready.advance();
        if let Some(seam) = self.armed_seams[index].take() {
            if batch.is_empty() {
                // Nothing to cut: keep the seam armed for the batch that has
                // something to lose.
                self.armed_seams[index] = Some(seam);
                return Some(batch);
            }
            match seam {
                Seam::BeforeSync => {}
                Seam::AfterSyncBeforeSend => {
                    // Exactly the split `commit_batch` makes, for exactly the
                    // same reason: a durable floor must never outrun the
                    // durable application state covering the slots it drops,
                    // and this cut *skips* the application apply. So the
                    // truncates go with the half that was lost, not the half
                    // that survived — a floor is pure space reclamation, and
                    // the next decided `Truncate` raises it again.
                    for write in batch
                        .writes
                        .iter()
                        .filter(|write| !matches!(write, WriteOp::Truncate { .. }))
                    {
                        self.disks[index].apply(write);
                    }
                }
            }
            self.nodes[index] = None;
            self.record_seam(id, seam);
            return None;
        }
        Some(batch)
    }

    /// Persist, send, apply, truncate, offer, answer — in that order.
    fn commit_batch(&mut self, index: usize, batch: Batch) {
        let id = self.pool[index];
        // `Truncate` waits until after the application apply: see the module
        // doc, and `paros::driver::ready`.
        let (truncates, writes): (Vec<WriteOp>, Vec<WriteOp>) = batch
            .writes
            .into_iter()
            .partition(|write| matches!(write, WriteOp::Truncate { .. }));
        for write in &writes {
            self.disks[index].apply(write);
        }
        self.narrate_installs(id, &writes);
        for (to, message) in batch.messages {
            if matches!(message, Message::Heartbeat { .. }) {
                self.beats_broadcast = self.beats_broadcast.saturating_add(1);
            }
            self.send(Party::Node(id), Party::Node(to), Envelope::Node(message));
        }
        for (to, request) in batch.match_requests {
            self.send(
                Party::Node(id),
                Party::Matchmaker(to),
                Envelope::Match(request),
            );
        }
        for (to, request) in batch.gc_requests {
            self.send(
                Party::Node(id),
                Party::Matchmaker(to),
                Envelope::Gc(request),
            );
        }
        for (slot, command) in batch.committed {
            let marker = match &command {
                Command::Control(Control::Snap { .. }) => Some(slot),
                _ => None,
            };
            self.disks[index].apply_committed(slot, command);
            if let Some(at) = marker {
                self.narrate(
                    NarrationKind::Snapshot,
                    format!(
                        "{} retains a snapshot of its application at slot {}. A snapshot point \
                         is a *decided* slot, so every node takes it at the same place in the \
                         same order. That is why one node's copy can replace another node's \
                         log.",
                        who(id),
                        at.0
                    ),
                );
            }
        }
        for write in &truncates {
            if let WriteOp::Truncate { first, .. } = write {
                let dropped = self.disks[index]
                    .records()
                    .keys()
                    .filter(|slot| *slot < first)
                    .count();
                self.narrate(
                    NarrationKind::Truncate,
                    format!(
                        "{} truncates. It dropped {}, and its floor is now slot {}. The \
                         truncation happens when this node *applies* the decided Truncate, and \
                         not when the leader asked for it. Every node therefore reaches the same \
                         floor, and nobody broadcasts that floor.",
                        who(id),
                        many(dropped, "accepted record"),
                        first.0
                    ),
                );
            }
            self.disks[index].apply(write);
        }
        self.serve_snapshot_offers(index, &batch.snapshot_offers);
        for state in batch.read_states {
            self.serve_read(state);
        }
    }

    /// Say what an `InstallSnapshot` write did to this node's disk.
    fn narrate_installs(&mut self, id: NodeId, writes: &[WriteOp]) {
        for write in writes {
            let WriteOp::InstallSnapshot {
                chosen_index,
                ballot,
                ..
            } = write
            else {
                continue;
            };
            let promised = self
                .index_of(id)
                .map(|index| self.disks[index].hard_state().max_promised_ballot);
            self.narrate(
                NarrationKind::Snapshot,
                format!(
                    "{} installs the snapshot. Its chosen prefix moves to slot {}, and its log \
                     below that slot is gone, because the state is in the bytes now. Its durable \
                     promise is {}. The snapshot's ballot was {}, and a promise only ever rises. \
                     A snapshot restores the log, and it does not restore a promise.",
                    who(id),
                    chosen_index.0,
                    promised.map_or_else(|| "unchanged".to_string(), crate::view::show_ballot),
                    crate::view::show_ballot(*ballot)
                ),
            );
        }
    }

    /// Fill in the opaque bytes for every snapshot offer the core recorded, and
    /// put the `InstallSnapshot` on the wire like any other message.
    ///
    /// This is the driver's half of the seam (`paros::driver::ready`): the core
    /// decided *who* needs a snapshot and *up to where* and holds no
    /// application state, so the world, which owns the disks, reads the bytes.
    /// The guard is the driver's too — an offer must describe **exactly** the
    /// application prefix its message names, so an offer whose boundary the
    /// applied log does not reach is skipped rather than sent wrong. The peer
    /// re-asks on its next catch-up.
    fn serve_snapshot_offers(&mut self, index: usize, offers: &[(NodeId, Slot, Ballot)]) {
        let id = self.pool[index];
        for &(to, at, ballot) in offers {
            if self.disks[index].applied_slot() != Some(at) {
                self.narrate(
                    NarrationKind::Snapshot,
                    format!(
                        "{} does not serve a snapshot at slot {}. Its own application has not \
                         executed that far, so the bytes would not describe the boundary the \
                         message claims. The peer asks again.",
                        who(id),
                        at.0
                    ),
                );
                continue;
            }
            let sessions = self.nodes[index]
                .as_ref()
                .map(ColocatedNode::session_ledger)
                .unwrap_or_default();
            let snapshot = self.disks[index].snapshot();
            self.narrate(
                NarrationKind::Snapshot,
                format!(
                    "{} offers {} a snapshot instead of a replay. The slots it asked for are \
                     below this node's floor, and they no longer exist anywhere. The bytes are \
                     the application's own state at slot {}. paros ships those bytes and never \
                     reads them.",
                    who(id),
                    who(to),
                    at.0
                ),
            );
            self.send(
                Party::Node(id),
                Party::Node(to),
                Envelope::Node(Message::InstallSnapshot {
                    from: id,
                    ballot,
                    chosen_index: at,
                    snapshot,
                    sessions,
                }),
            );
        }
    }

    /// Ask about the front of `steps`, parking `batch` behind the answer.
    fn raise_recovery(
        &mut self,
        index: usize,
        batch: Box<Batch>,
        steps: Vec<(Slot, RecoveryStep<Command>)>,
    ) {
        let id = self.pool[index];
        let ballot = self.nodes[index]
            .as_ref()
            .map_or_else(Ballot::zero, ColocatedNode::ballot);
        let (slot, step) = steps[0].clone();
        let prompt_id = self.take_prompt_id();
        self.prompt = Some(Prompt::leader_recovery(prompt_id, id, ballot, slot, &step));
        self.paused = Some(Paused::Recovery {
            node: id,
            batch,
            steps,
        });
    }

    /// Resume whatever the answered prompt was holding back.
    pub(super) fn resume(&mut self, paused: Paused) {
        match paused {
            Paused::Message { node, message } => self.step(node, *message),
            Paused::Retry { node, client, seq } => self.retry_now(node, client, seq),
            Paused::Propose {
                node,
                client,
                value,
            } => self.propose_answered(node, client, &value),
            Paused::Boot { node } => self.boot_refused(node),
            Paused::Registration { from, to, request } => self.register_now(from, to, *request),
            Paused::MatchReply { node, reply } => {
                let Some(index) = self.index_of(node) else {
                    return;
                };
                self.fold_match_reply(node, index, *reply);
            }
            Paused::Retire { target, watermark } => self.retire_now(target, watermark),
            Paused::Batch { node, batch } => {
                let Some(index) = self.index_of(node) else {
                    return;
                };
                self.observe(node, move |world| {
                    if let Some(batch) = world.gate_batch(index, *batch, true) {
                        world.release(index, batch);
                    }
                });
            }
            Paused::Recovery {
                node,
                batch,
                mut steps,
            } => {
                let Some(index) = self.index_of(node) else {
                    return;
                };
                steps.remove(0);
                self.observe(node, move |world| {
                    if steps.is_empty() {
                        world.release(index, *batch);
                    } else {
                        world.raise_recovery(index, batch, steps);
                    }
                });
            }
        }
    }
}
