//! The client half of the world: what each client asked for, when it asked,
//! and the history the linearizability judge reads.
//!
//! The client is the only party that knows its own program order, so this is
//! recorded client-side and judged client-side.

use std::collections::BTreeSet;

use paros_core::{ClientId, ClientSeq, NodeId, Slot};

use crate::world::World;

/// One client's writes.
#[derive(Clone, Debug)]
pub(super) struct Proposal {
    pub(super) seq: ClientSeq,
    pub(super) value: String,
    pub(super) node: NodeId,
    pub(super) slot: Option<Slot>,
    pub(super) acked: bool,
    /// When the client issued it, on the history's own monotone counter.
    pub(super) issued: u64,
    /// When it was acknowledged, on the same counter.
    pub(super) acked_at: Option<u64>,
}

/// One client's reads.
#[derive(Clone, Debug)]
pub(super) struct PendingRead {
    pub(super) ctx: u64,
    pub(super) node: NodeId,
    /// The index the round captured, recorded here because
    /// `Proposer::read_rounds` exposes no accessor for it.
    pub(super) index: Option<Slot>,
    /// The beat sequence an ack must carry to count, for display only.
    pub(super) required_seq: u64,
    /// Who has acked a qualifying beat since the read opened — the world's own
    /// tally, for the prompt's summary. The *judgement* always comes from a
    /// clone of the real `Proposer`.
    pub(super) acks: BTreeSet<NodeId>,
    /// Whether this is a **leaderless** read: a Phase-1 quorum's vote
    /// watermarks rather than a leader's beat acks. The two are served through
    /// the same `ReadState`, and only the client knows which it asked for.
    pub(super) leaderless: bool,
    pub(super) served: bool,
    /// When the client asked, on the history's own monotone counter.
    pub(super) issued: u64,
    /// When it was answered, on the same counter.
    pub(super) served_at: Option<u64>,
}

/// One client-visible operation, as the linearizability judge reads it.
///
/// The client is the only party that knows its own program order, so this is
/// recorded **client-side**: what it asked, when it asked, when it was
/// answered, and the one number the answer carries — the slot a write landed
/// at, or the watermark a read observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryOp {
    /// Whose operation it is.
    pub client: u64,
    /// The node it was sent to.
    pub node: NodeId,
    /// True for a write, false for a read.
    pub write: bool,
    /// A write's slot once acknowledged, or a read's watermark once served.
    /// `None` on a read means the empty prefix.
    pub at: Option<Slot>,
    /// When it was issued, on the history's monotone counter.
    pub started: u64,
    /// When it completed, or `None` while it is still outstanding. An
    /// outstanding operation constrains nothing: it may still complete later.
    pub completed: Option<u64>,
}

/// One client.
#[derive(Clone, Debug)]
pub(super) struct Client {
    pub(super) id: ClientId,
    pub(super) next_seq: u64,
    pub(super) proposals: Vec<Proposal>,
    pub(super) reads: Vec<PendingRead>,
}

impl World {
    /// Whether every client write that was admitted has been applied by the
    /// node that admitted it.
    #[must_use]
    pub fn all_writes_acked(&self) -> bool {
        self.clients
            .iter()
            .flat_map(|c| c.proposals.iter())
            .all(|p| p.acked)
    }

    /// The clients this level gave the player, in id order.
    #[must_use]
    pub fn clients(&self) -> Vec<u64> {
        self.clients.iter().map(|client| client.id.0).collect()
    }

    /// Every distinct value a client asked for, in the order it was first
    /// asked.
    ///
    /// A goal that must name a value reads it here rather than writing it
    /// down. A level's own reference proposes what it likes, and a player may
    /// propose anything at all; a goal that compares an applied log against a
    /// literal is asking whether the player typed the same word as the
    /// reference, which is not the rule any level teaches.
    #[must_use]
    pub fn proposed_values(&self) -> Vec<String> {
        let mut asked: Vec<(u64, String)> = self
            .clients
            .iter()
            .flat_map(|client| client.proposals.iter())
            .map(|proposal| (proposal.issued, proposal.value.clone()))
            .collect();
        asked.sort_by_key(|(issued, _)| *issued);
        let mut values: Vec<String> = Vec::new();
        for (_, value) in asked {
            if !values.contains(&value) {
                values.push(value);
            }
        }
        values
    }

    /// Every client operation, in the order it was issued — the history the
    /// linearizability judge reads.
    #[must_use]
    pub fn history(&self) -> Vec<HistoryOp> {
        let mut ops: Vec<HistoryOp> = Vec::new();
        for client in &self.clients {
            for proposal in &client.proposals {
                ops.push(HistoryOp {
                    client: client.id.0,
                    node: proposal.node,
                    write: true,
                    at: proposal.slot.filter(|_| proposal.acked),
                    started: proposal.issued,
                    completed: proposal.acked_at,
                });
            }
            for read in &client.reads {
                ops.push(HistoryOp {
                    client: client.id.0,
                    node: read.node,
                    write: false,
                    at: read.index.filter(|_| read.served),
                    started: read.issued,
                    completed: read.served_at,
                });
            }
        }
        ops.sort_by_key(|op| (op.started, op.client, !op.write));
        ops
    }

    /// Judge the recorded history by the three conditions a totally ordered
    /// log needs — no search, because the log *is* the order:
    ///
    /// 1. a committed read observes every write acknowledged before it began;
    /// 2. watermarks never move backwards across non-overlapping reads;
    /// 3. a write issued after a committed read lands **above** that read's
    ///    watermark.
    ///
    /// Operations that never completed constrain nothing: a write whose ack
    /// never arrived may still be chosen later, and that is not a violation of
    /// anything.
    ///
    /// `Ok(())` is the history being linearizable so far.
    ///
    /// # Errors
    ///
    /// The sentence naming the condition that failed and the two operations
    /// that failed it.
    pub fn linearizable(&self) -> Result<(), String> {
        let ops = self.history();
        let reads: Vec<&HistoryOp> = ops
            .iter()
            .filter(|op| !op.write && op.completed.is_some())
            .collect();
        let writes: Vec<&HistoryOp> = ops
            .iter()
            .filter(|op| op.write && op.completed.is_some())
            .collect();
        for read in &reads {
            let began = read.started;
            for write in &writes {
                let Some(acked) = write.completed else {
                    continue;
                };
                if acked < began && write.at > read.at {
                    return Err(format!(
                        "client {}'s read at node {} observed {}. Client {}'s write at {} was \
                         already acknowledged before that read began. A read must not go behind \
                         a write that completed before the read began.",
                        read.client,
                        read.node.0,
                        at(read.at),
                        write.client,
                        at(write.at)
                    ));
                }
            }
        }
        for earlier in &reads {
            let Some(done) = earlier.completed else {
                continue;
            };
            for later in &reads {
                if later.started >= done && later.at < earlier.at {
                    return Err(format!(
                        "a read at node {} observed {}. A read at node {} had already observed \
                         {} before it. A watermark must not move backwards.",
                        later.node.0,
                        at(later.at),
                        earlier.node.0,
                        at(earlier.at)
                    ));
                }
            }
        }
        for read in &reads {
            let Some(done) = read.completed else { continue };
            for write in &writes {
                if write.started > done && write.at <= read.at {
                    return Err(format!(
                        "client {}'s write landed at {}. That is at or below {}, which a read at \
                         node {} had already observed. A write issued after a read must land \
                         above the watermark of that read.",
                        write.client,
                        at(write.at),
                        at(read.at),
                        read.node.0
                    ));
                }
            }
        }
        Ok(())
    }

    /// The highest slot a client write has been acknowledged at — the write a
    /// later linearizable read must not read behind.
    #[must_use]
    pub fn highest_acked_slot(&self) -> Option<Slot> {
        self.clients
            .iter()
            .flat_map(|client| client.proposals.iter())
            .filter(|proposal| proposal.acked)
            .filter_map(|proposal| proposal.slot)
            .max()
    }
}

/// "nothing", or "slot 3" — how the goals and the history judge name a
/// watermark.
fn at(slot: Option<Slot>) -> String {
    slot.map_or_else(
        || "the empty prefix".to_string(),
        |s| format!("slot {}", s.0),
    )
}
