//! The drain contract: one `Ready` batch at a time, from the node to the disk
//! and the wire.
//!
//! This is the loop `crates/lib/paros-core/examples/quorum_read.rs` writes in
//! fifteen lines, with one thing a step-through needs added: a **durability
//! seam** that can cut a batch in half.
//!
//! The order never changes, and it is the order `paros::roles::node::ready` uses
//! with a real disk underneath:
//!
//! 1. `ready()`, copy every bucket out, `advance()` — the guard is never held
//!    across a disk write or a recorded action.
//! 2. Persist the batch's writes, `Truncate` held back.
//! 3. Send: one wire entry per resolved addressee.
//! 4. Apply `committed` to the application log, then flush the held-back
//!    truncates.
//! 5. Answer the batch's read states.
//! 6. `advance_recovery()`, and drain again until the node is quiet.

use paros_core::journal::Command;
use paros_core::matchmaker::{GcRequest, MatchRequest};
use paros_core::membership::MatchmakerId;
use paros_core::quorum_read::ReadState;
use paros_core::{Message, NodeId, Slot, WriteOp};

use crate::paxos::action::Seam;
use crate::paxos::narration::{self, NarrationKind, NodeSnapshot, many, who};
use crate::paxos::world::{Envelope, Party, World};

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
            && self.match_requests.is_empty()
            && self.gc_requests.is_empty()
    }
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

    /// Drain `id` until it is quiet, honouring the seams.
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

    /// One `ready()` / `advance()` round, plus the armed-seam cut.
    ///
    /// Returns `None` when the node is not running, or when the seam fired (the
    /// node is dropped and the caller stops).
    fn take_batch(&mut self, index: usize) -> Option<Batch> {
        let id = self.pool[index];
        let pool = &self.pool;
        let node = self.nodes[index].as_mut()?;
        let ready = node.ready();
        let batch = Batch {
            writes: ready.writes().to_vec(),
            messages: ready
                .messages()
                .iter()
                .flat_map(|(audience, message)| {
                    audience
                        .resolve(pool, id)
                        .into_iter()
                        .map(move |to| (to, message.clone()))
                })
                .collect(),
            committed: ready
                .committed()
                .iter()
                .map(|(slot, _, command, _)| (*slot, command.clone()))
                .collect(),
            read_states: ready.read_states().to_vec(),
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

    /// Persist, send, apply, truncate, answer — in that order.
    fn commit_batch(&mut self, index: usize, batch: Batch) {
        let id = self.pool[index];
        // `Truncate` waits until after the application apply: see the module
        // doc, and `paros::roles::node::ready`.
        let (truncates, writes): (Vec<WriteOp>, Vec<WriteOp>) = batch
            .writes
            .into_iter()
            .partition(|write| matches!(write, WriteOp::Truncate { .. }));
        for write in &writes {
            self.disks[index].apply(write);
        }
        self.narrate_jumps(id, &writes);
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
            self.disks[index].apply_committed(slot, command);
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
        for state in batch.read_states {
            self.serve_read(state);
        }
    }

    /// Say what a trim-point jump did to this node's disk.
    fn narrate_jumps(&mut self, id: NodeId, writes: &[WriteOp]) {
        for write in writes {
            let WriteOp::TrimmedTo { point, .. } = write else {
                continue;
            };
            let promised = self
                .index_of(id)
                .map(|index| self.disks[index].hard_state().max_promised_ballot);
            self.narrate(
                NarrationKind::TrimPoint,
                format!(
                    "{} jumps to the trim point. Its floor is now slot {}, and it counts every \
                     slot below that as chosen. It never walks those slots: they are gone on \
                     every node, and a decided Truncate put them there. Its durable promise is \
                     still {}. A trim point says where the log starts, and nothing about a \
                     ballot.",
                    who(id),
                    point.0,
                    promised
                        .map_or_else(|| "unchanged".to_string(), crate::paxos::view::show_ballot),
                ),
            );
        }
    }
}
