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
//! 5. Answer the batch's read states.
//! 6. `advance_recovery()`, and drain again until the node is quiet.

use std::collections::BTreeMap;

use paros_core::proposer::RecoveryStep;
use paros_core::{Ballot, ColocatedNode, Command, Message, NodeId, ReadState, Slot, WriteOp};

use crate::action::Seam;
use crate::prompt::{Prompt, PromptKind};
use crate::world::{InFlight, World};

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
    /// `(started, gap fills, remaining)` when this batch carried a
    /// leader-recovery page — the marker the `LeaderRecovery` prompt gates on.
    recovery: Option<(usize, usize, usize)>,
}

impl Batch {
    fn is_empty(&self) -> bool {
        self.writes.is_empty()
            && self.messages.is_empty()
            && self.committed.is_empty()
            && self.read_states.is_empty()
    }

    /// What this batch's recovery page did, one entry per slot in slot order.
    ///
    /// Read off the batch's own `Accept`s rather than re-derived: a
    /// [`paros_core::Control::Noop`] is the gap fill
    /// ([`RecoveryStep::Fill`]), anything else is the P2c re-proposal
    /// ([`RecoveryStep::Recovered`]). [`RecoveryStep::Undescribed`] leaves no
    /// message at all, and is unreachable here — it needs a cooperative
    /// handoff, which the game has no verb for yet.
    fn recovery_steps(&self) -> Vec<(Slot, RecoveryStep<Command>)> {
        let mut by_slot: BTreeMap<Slot, Command> = BTreeMap::new();
        for (_, message) in &self.messages {
            if let Message::Accept { slot, command, .. } = message {
                by_slot.entry(*slot).or_insert_with(|| command.clone());
            }
        }
        by_slot
            .into_iter()
            .map(|(slot, command)| {
                let step = if matches!(command, Command::Control(paros_core::Control::Noop)) {
                    RecoveryStep::Fill
                } else {
                    RecoveryStep::Recovered(command)
                };
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
    pub(super) fn step(&mut self, id: NodeId, message: Message) {
        if let Some(index) = self.index_of(id)
            && let Some(node) = self.nodes[index].as_mut()
        {
            node.step(message);
        }
        self.pump(id);
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
            let steps = batch.recovery_steps();
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
            recovery: ready.recovery_batch(),
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
                    for write in &batch.writes {
                        self.disks[index].apply(write);
                    }
                }
            }
            self.nodes[index] = None;
            return None;
        }
        Some(batch)
    }

    /// Persist, send, apply, answer — in that order.
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
        for (to, message) in batch.messages {
            self.wire.push(InFlight {
                id: self.next_message_id,
                from: id,
                to,
                message,
                sent_at: self.clock,
            });
            self.next_message_id += 1;
        }
        for (slot, command) in batch.committed {
            self.disks[index].apply_committed(slot, command);
        }
        for write in &truncates {
            self.disks[index].apply(write);
        }
        for state in batch.read_states {
            self.serve_read(state);
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
            Paused::Batch { node, batch } => {
                let Some(index) = self.index_of(node) else {
                    return;
                };
                if let Some(batch) = self.gate_batch(index, *batch, true) {
                    self.release(index, batch);
                }
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
                if steps.is_empty() {
                    self.release(index, *batch);
                } else {
                    self.raise_recovery(index, batch, steps);
                }
            }
        }
    }
}
