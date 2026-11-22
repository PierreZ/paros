//! The **leaderless read** of a segment replica (§J2.5, §J3.3; Compartmentalized
//! Paxos §3.4).
//!
//! A client reads the records of a segment from a replica of that segment.
//! No read goes through the leader. The rules, from
//! `specs/tla/experiments/segments/reads/` (`ReadAsk`, `ReadServe`, checks
//! `part_wipe_belief` and `wipe_belief`):
//!
//! - **A read of an open segment asks a Phase-1 quorum of the segment's
//!   grid** (`F + 1` of each column) for their vote watermarks
//!   ([`Message::PreRead`]), and serves once the fold covers the highest
//!   watermark: every slot that a quorum of the grid could have chosen
//!   before the read started is then applied.
//! - **The fence of an answer ends the read.** Each answer carries the
//!   newest segment that the acceptor knows since it started (its promise
//!   and its belief, `Above` of the model). An answer that names a later
//!   segment ends the read with [`ReadAnswer::Superseded`]: the client reads
//!   the register again and asks a replica of the later segment. An acceptor
//!   that knows no segment since it started answers no read.
//! - **A read of a sealed segment asks nobody**: the end is a fact of a done
//!   seal, so the replica serves once its fold reaches the end.
//! - **A server answers only from what it holds.** A position below the
//!   segment is [`ReadAnswer::Below`], a position below the trim floor is
//!   [`ReadAnswer::Truncated`], and a replica with no base, a full table of
//!   reads, or a read that waits too long answers [`ReadAnswer::Unserved`]:
//!   the client asks again.
//! - **An answer waits on the replica's last batch** (§J4.2, rule 1): the
//!   fold it reads may hold slots of batches still in flight.

use std::collections::BTreeSet;

use paros_consensus::journal::Seq;
use paros_consensus::{NodeId, Slot};

use super::{Folded, ReplicaEffect, SegmentReplica};
use crate::command::Record;
pub use crate::read::ReadAnswer;
use crate::roles::leader::ReadSegment;
use crate::roles::message::Message;
use crate::segment::SegmentId;

/// The most reads one replica holds open; a read above it is
/// [`ReadAnswer::Unserved`].
pub const MAX_READS: usize = 64;

/// The ticks a read stays open with no answer before it is
/// [`ReadAnswer::Unserved`].
pub const READ_TICKS: u64 = 8;

/// The most records one page carries. A `limit` of 0 or above it is cut to
/// it (§J2.7).
pub const READ_PAGE: usize = 256;

/// One open read.
#[derive(Clone, Debug)]
pub(super) struct PendingRead {
    from: Seq,
    limit: usize,
    /// The acceptors that answered under the fence.
    answered: BTreeSet<NodeId>,
    /// The highest watermark of those answers.
    watermark: Option<Slot>,
    ticks: u64,
}

impl SegmentReplica {
    /// Start the read `read` of the records from `from`, at most `limit`
    /// of them. The answer comes as [`ReplicaEffect::Read`], in this step
    /// or a later one. A read id that is already open changes nothing.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn read(&mut self, read: u64, from: Seq, limit: usize) -> Vec<ReplicaEffect> {
        if self.reads.contains_key(&read) {
            return Vec::new();
        }
        if self.dropped || self.folded.is_none() || self.reads.len() >= MAX_READS {
            probe!(reachable, "replica: a read it cannot serve is unserved");
            return vec![self.read_answer(read, ReadAnswer::Unserved)];
        }
        let mut effects = Vec::new();
        if self.end.is_none() {
            let Some(grid) = &self.grid else {
                return vec![self.read_answer(read, ReadAnswer::Unserved)];
            };
            for acceptor in grid.columns().iter().flatten() {
                effects.push(ReplicaEffect::Send {
                    to: *acceptor,
                    message: Message::PreRead {
                        segment: self.segment,
                        read,
                    },
                    after: None,
                });
            }
        }
        self.reads.insert(
            read,
            PendingRead {
                from,
                limit: limit.clamp(1, READ_PAGE),
                answered: BTreeSet::new(),
                watermark: None,
                ticks: 0,
            },
        );
        effects.extend(self.serve_reads());
        assert!(
            self.reads.len() <= MAX_READS,
            "a replica holds at most MAX_READS reads"
        );
        self.assert_invariants();
        effects
    }

    /// The reads open now.
    #[must_use]
    pub fn reads(&self) -> usize {
        self.reads.len()
    }

    /// An acceptor's answer to the read `read`.
    pub(super) fn pre_read_ack(
        &mut self,
        from: NodeId,
        read: u64,
        watermark: Option<Slot>,
        bound: SegmentId,
    ) -> Vec<ReplicaEffect> {
        let in_grid = self
            .grid
            .as_ref()
            .is_some_and(|grid| grid.columns().iter().flatten().any(|a| *a == from));
        if !in_grid || self.end.is_some() {
            return Vec::new();
        }
        let Some(pending) = self.reads.get_mut(&read) else {
            return Vec::new();
        };
        if bound > self.segment {
            probe!(reachable, "replica: a read meets a later segment");
            self.reads.remove(&read);
            return vec![self.read_answer(read, ReadAnswer::Superseded(bound))];
        }
        pending.answered.insert(from);
        pending.watermark = pending.watermark.max(watermark);
        self.serve_reads()
    }

    /// The periodic step of the reads: a read open for [`READ_TICKS`] ticks
    /// is unserved, and the others ask again the acceptors that did not
    /// answer.
    pub(super) fn tick_reads(&mut self) -> Vec<ReplicaEffect> {
        let mut effects = self.serve_reads();
        let mut expired = Vec::new();
        for (read, pending) in &mut self.reads {
            pending.ticks += 1;
            if pending.ticks >= READ_TICKS {
                expired.push(*read);
                continue;
            }
            if self.end.is_some() {
                continue;
            }
            let Some(grid) = &self.grid else { continue };
            for acceptor in grid.columns().iter().flatten() {
                if !pending.answered.contains(acceptor) {
                    effects.push(ReplicaEffect::Send {
                        to: *acceptor,
                        message: Message::PreRead {
                            segment: self.segment,
                            read: *read,
                        },
                        after: None,
                    });
                }
            }
        }
        for read in expired {
            probe!(reachable, "replica: a read that waits too long is unserved");
            self.reads.remove(&read);
            effects.push(self.read_answer(read, ReadAnswer::Unserved));
        }
        effects
    }

    /// Unserve every open read: the replica stops (its driver stops it, or
    /// it dropped its copy), and each reader asks again elsewhere.
    pub fn unserve_reads(&mut self) -> Vec<ReplicaEffect> {
        let reads = std::mem::take(&mut self.reads);
        reads
            .into_keys()
            .map(|read| self.read_answer(read, ReadAnswer::Unserved))
            .collect()
    }

    /// Serve each open read whose answers are complete and whose fold
    /// covers what they need.
    pub(super) fn serve_reads(&mut self) -> Vec<ReplicaEffect> {
        let Some(folded) = &self.folded else {
            return Vec::new();
        };
        let columns = self.grid.as_ref().map(ReadSegment::columns_of);
        let ready: Vec<u64> = self
            .reads
            .iter()
            .filter(|(_, pending)| match self.end {
                Some(end) => folded.front >= end,
                None => {
                    columns.as_ref().is_some_and(|columns| {
                        columns
                            .iter()
                            .all(|column| column.has_phase1_quorum(&pending.answered))
                    }) && pending.watermark.is_none_or(|w| folded.front > w)
                }
            })
            .map(|(read, _)| *read)
            .collect();
        let mut effects = Vec::new();
        for read in ready {
            let pending = self.reads.remove(&read).expect("an open read");
            let answer = self.page(&pending);
            effects.push(self.read_answer(read, answer));
        }
        effects
    }

    /// The answer of a read that is ready: from the fold now.
    fn page(&self, pending: &PendingRead) -> ReadAnswer {
        let folded = self.folded.as_ref().expect("a ready read has a fold");
        let view = folded.state.view();
        if pending.from < folded.state.start_seq() {
            probe!(reachable, "replica: a read below the segment is Below");
            return ReadAnswer::Below(view);
        }
        if pending.from < folded.state.first_seq() {
            probe!(reachable, "replica: a read below the floor is Truncated");
            return ReadAnswer::Truncated(view);
        }
        let records = records(folded, pending.from, pending.limit);
        let reaches = records
            .last()
            .map_or(pending.from, |(seq, _)| Seq(seq.0 + 1))
            >= view.next_seq;
        let sealed = self.end.is_some() && reaches;
        probe!(sometimes, sealed, "replica: a read reaches the sealed end");
        probe!(
            sometimes,
            !sealed && !records.is_empty(),
            "replica: a read of the open segment serves records"
        );
        assert!(
            records.len() <= pending.limit,
            "a page holds at most the limit"
        );
        assert!(
            records.windows(2).all(|w| w[1].0.0 == w[0].0.0 + 1),
            "a page holds consecutive positions"
        );
        ReadAnswer::Page {
            records,
            view,
            sealed,
        }
    }

    fn read_answer(&self, read: u64, answer: ReadAnswer) -> ReplicaEffect {
        ReplicaEffect::Read {
            read,
            answer,
            after: self.flight.last(),
        }
    }
}

/// The records of the fold from `from`, at most `limit` of them.
fn records(folded: &Folded, from: Seq, limit: usize) -> Vec<(Seq, Record)> {
    let first = folded
        .positions
        .range(..=from)
        .next_back()
        .map_or(from, |(seq, _)| *seq);
    folded
        .positions
        .range(first..)
        .flat_map(|(seq, write)| {
            write
                .records
                .iter()
                .zip(seq.0..)
                .map(|(record, at)| (Seq(at), record.clone()))
        })
        .filter(|(at, _)| *at >= from)
        .take(limit)
        .collect()
}

#[cfg(test)]
mod tests;
