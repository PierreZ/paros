//! The client half of the world: what each client asked for, when it asked,
//! and the history the linearizability judge reads.
//!
//! The client is the only party that knows its own program order, so this is
//! recorded client-side and judged client-side.

use paros_core::journal::Entry;
use paros_core::{NodeId, Slot};

use super::ClientId;

use crate::paxos::narration::prefix_at as at;
use crate::paxos::world::World;

/// One client's writes.
#[derive(Clone, Debug)]
pub(super) struct Proposal {
    /// The client's own number for the write (1, 2, ...): what a retry
    /// names.
    pub(super) seq: u64,
    /// The exact write it sent — a retry sends these bytes again.
    pub(super) entry: Entry,
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
    /// The index the read observes: the watermark the quorum read was
    /// served at. Kept here because the client's history outlives the read,
    /// which the core drops once it serves.
    pub(super) index: Option<Slot>,
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
    /// The journal position its next write asks for (#204): one past its
    /// last admitted write.
    pub(super) next_position: u64,
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
        judge(&self.history())
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

/// A completed operation and the counter reading it completed at.
type Done<'a> = (&'a HistoryOp, u64);

/// The three conditions of [`World::linearizable`] over `ops`. Outstanding
/// operations constrain nothing, so only completed ones are judged.
fn judge(ops: &[HistoryOp]) -> Result<(), String> {
    let completed = |write: bool| -> Vec<Done<'_>> {
        ops.iter()
            .filter(|op| op.write == write)
            .filter_map(|op| op.completed.map(|done| (op, done)))
            .collect()
    };
    let reads = completed(false);
    let writes = completed(true);
    reads_see_acked_writes(&reads, &writes)?;
    watermarks_never_fall(&reads)?;
    writes_land_above_reads(&reads, &writes)
}

/// 1. A read observes every write acknowledged before it began.
fn reads_see_acked_writes(reads: &[Done<'_>], writes: &[Done<'_>]) -> Result<(), String> {
    for (read, _) in reads {
        for (write, acked) in writes {
            if *acked < read.started && write.at > read.at {
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
    Ok(())
}

/// 2. A read that begins after another read completed observes no less.
fn watermarks_never_fall(reads: &[Done<'_>]) -> Result<(), String> {
    for (earlier, done) in reads {
        for (later, _) in reads {
            if later.started >= *done && later.at < earlier.at {
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
    Ok(())
}

/// 3. A write issued after a read completed lands above what it observed.
fn writes_land_above_reads(reads: &[Done<'_>], writes: &[Done<'_>]) -> Result<(), String> {
    for (read, done) in reads {
        for (write, _) in writes {
            if write.started > *done && write.at <= read.at {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn op(write: bool, at: Option<u64>, started: u64, completed: Option<u64>) -> HistoryOp {
        HistoryOp {
            client: if write { 7 } else { 8 },
            node: NodeId(u64::from(!write)),
            write,
            at: at.map(Slot),
            started,
            completed,
        }
    }

    #[test]
    fn an_empty_or_ordered_history_is_linearizable() {
        assert_eq!(judge(&[]), Ok(()));
        let ops = [
            op(true, Some(0), 0, Some(1)),
            op(false, Some(0), 2, Some(3)),
            op(false, Some(0), 4, Some(5)),
            op(true, Some(1), 6, Some(7)),
        ];
        assert_eq!(judge(&ops), Ok(()));
    }

    #[test]
    fn a_read_behind_an_acknowledged_write_is_refused() {
        let ops = [
            op(true, Some(2), 0, Some(1)),
            op(false, Some(1), 2, Some(3)),
        ];
        assert_eq!(
            judge(&ops),
            Err(
                "client 8's read at node 1 observed slot 1. Client 7's write at slot 2 was \
                 already acknowledged before that read began. A read must not go behind \
                 a write that completed before the read began."
                    .to_string()
            )
        );
        // The same write acknowledged only once the read began binds nothing.
        let overlapping = [
            op(true, Some(2), 0, Some(2)),
            op(false, Some(1), 2, Some(3)),
        ];
        assert_eq!(judge(&overlapping), Ok(()));
    }

    #[test]
    fn a_watermark_that_moves_backwards_is_refused() {
        let ops = [
            op(false, Some(3), 0, Some(1)),
            op(false, Some(2), 1, Some(2)),
        ];
        assert_eq!(
            judge(&ops),
            Err(
                "a read at node 1 observed slot 2. A read at node 1 had already observed \
                 slot 3 before it. A watermark must not move backwards."
                    .to_string()
            )
        );
        // Two overlapping reads may observe in either order.
        let overlapping = [
            op(false, Some(3), 0, Some(2)),
            op(false, Some(2), 1, Some(3)),
        ];
        assert_eq!(judge(&overlapping), Ok(()));
    }

    #[test]
    fn a_write_at_or_below_an_earlier_read_is_refused() {
        let ops = [
            op(false, Some(2), 0, Some(1)),
            op(true, Some(2), 2, Some(3)),
        ];
        assert_eq!(
            judge(&ops),
            Err(
                "client 7's write landed at slot 2. That is at or below slot 2, which a read at \
                 node 1 had already observed. A write issued after a read must land \
                 above the watermark of that read."
                    .to_string()
            )
        );
        // A write issued as the read completed is concurrent with it.
        let concurrent = [
            op(false, Some(2), 0, Some(1)),
            op(true, Some(2), 1, Some(3)),
        ];
        assert_eq!(judge(&concurrent), Ok(()));
    }

    #[test]
    fn an_outstanding_operation_constrains_nothing() {
        let ops = [
            op(true, Some(5), 0, None),
            op(false, Some(1), 2, Some(3)),
            op(false, None, 4, None),
            op(true, Some(0), 6, None),
        ];
        assert_eq!(judge(&ops), Ok(()));
    }
}
