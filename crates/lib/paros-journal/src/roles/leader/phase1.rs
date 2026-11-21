//! The leader's **Phase 1** over the segments it reads (§J3.1,
//! `specs/tla/journal-state/`, `Phase1`).
//!
//! The leader asks every acceptor of every segment that is not retired, from
//! the first slot it writes; a promise comes in pages. Phase 1 is done when
//! the acceptors that answered completely hold a Phase-1 quorum of every
//! column of every grid read (§J5.1, through `membership/`). Then:
//!
//! - **each slot takes the highest vote** among the reports of the
//!   acceptors of each segment that reads the slot; a sealed segment reads
//!   only below its end (P2c);
//! - **a slot whose top report is a faulty vote is blocked**: its value is
//!   lost at that acceptor, so the leader proposes nothing there until a
//!   repair (CTRL, §J5.3);
//! - **the seal end**, when the leader's segment is pending, is one above
//!   the highest slot where an acceptor of the segment before reported a
//!   vote, a faulty one included (#1313), and at least the start of that
//!   segment and the first slot the leader reads (every slot below it is
//!   applied, so chosen in that grid).

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::{Ballot, NodeId, Slot};

use super::ReadSegment;
use crate::command::Command;
use crate::roles::message::PromisePage;

/// What one acceptor reported.
#[derive(Clone, Debug, Default)]
struct Report {
    votes: BTreeMap<Slot, (Ballot, Command)>,
    faulty: BTreeMap<Slot, Ballot>,
    /// The slot the next page starts at.
    cursor: Slot,
    complete: bool,
}

/// What a closed Phase 1 gives the leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Phase1Close {
    /// The vote to propose again at each slot.
    pub safe: BTreeMap<Slot, Command>,
    /// The slots whose top report is a faulty vote.
    pub blocked: Vec<Slot>,
    /// The seal end, when the leader's segment is pending.
    pub end: Option<Slot>,
}

/// The Phase 1 of one ballot.
#[derive(Clone, Debug)]
pub struct Phase1 {
    ballot: Ballot,
    from: Slot,
    reports: BTreeMap<NodeId, Report>,
}

impl Phase1 {
    /// The Phase 1 of `ballot` over `segments`, from slot `from`.
    ///
    /// # Panics
    ///
    /// When no segment is read.
    #[must_use]
    pub fn new(ballot: Ballot, from: Slot, segments: &[ReadSegment]) -> Self {
        assert!(!segments.is_empty(), "Phase 1 reads at least one segment");
        let acceptors: BTreeSet<NodeId> =
            segments.iter().flat_map(ReadSegment::acceptors).collect();
        let reports = acceptors
            .into_iter()
            .map(|node| {
                (
                    node,
                    Report {
                        cursor: from,
                        ..Report::default()
                    },
                )
            })
            .collect();
        Self {
            ballot,
            from,
            reports,
        }
    }

    /// The ballot.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// Each acceptor that has not answered completely, with the slot its
    /// next page starts at.
    #[must_use]
    pub fn wanted(&self) -> Vec<(NodeId, Slot)> {
        self.reports
            .iter()
            .filter(|(_, report)| !report.complete)
            .map(|(node, report)| (*node, report.cursor))
            .collect()
    }

    /// Fold one page from `from` that starts at `at`. Returns the slot of
    /// the next page when the acceptor has more. A page from an acceptor
    /// that Phase 1 does not ask, at another cursor, or after its last page
    /// is not folded.
    ///
    /// # Panics
    ///
    /// When a page carries a slot below its start, or a cursor at or below
    /// it.
    pub fn fold(&mut self, from: NodeId, at: Slot, page: PromisePage) -> Option<Slot> {
        let report = self.reports.get_mut(&from)?;
        if report.complete || report.cursor != at {
            return None;
        }
        assert!(
            page.votes
                .keys()
                .chain(page.faulty.keys())
                .all(|s| *s >= at),
            "a page starts at its cursor"
        );
        report.votes.extend(page.votes);
        report.faulty.extend(page.faulty);
        if let Some(next) = page.next {
            assert!(next > at, "a cursor moves forward");
            report.cursor = next;
            Some(next)
        } else {
            report.complete = true;
            None
        }
    }

    /// The acceptors that answered completely.
    fn complete(&self) -> BTreeSet<NodeId> {
        self.reports
            .iter()
            .filter(|(_, report)| report.complete)
            .map(|(node, _)| *node)
            .collect()
    }

    /// The outcome of Phase 1, once the complete answers hold a Phase-1
    /// quorum of every column of every segment read.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn close(&self, segments: &[ReadSegment]) -> Option<Phase1Close> {
        let complete = self.complete();
        let covered = segments.iter().all(|segment| {
            segment
                .columns
                .iter()
                .all(|c| c.has_phase1_quorum(&complete))
        });
        if !covered {
            return None;
        }
        let mut top: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let mut rotted: BTreeMap<Slot, Ballot> = BTreeMap::new();
        for node in &complete {
            let report = &self.reports[node];
            let reads = |slot: Slot| {
                segments
                    .iter()
                    .any(|segment| segment.reads(slot) && segment.acceptors().any(|a| a == *node))
            };
            // The highest ballot at each slot. Two votes at one ballot carry
            // one command (P2c), so a tie keeps either.
            for (slot, vote) in report.votes.iter().filter(|(slot, _)| reads(**slot)) {
                top.entry(*slot)
                    .and_modify(|held| {
                        *held = std::cmp::max_by_key(held.clone(), vote.clone(), |v| v.0);
                    })
                    .or_insert_with(|| vote.clone());
            }
            for (slot, ballot) in report.faulty.iter().filter(|(slot, _)| reads(**slot)) {
                rotted
                    .entry(*slot)
                    .and_modify(|held| *held = (*held).max(*ballot))
                    .or_insert(*ballot);
            }
        }
        let blocked: Vec<Slot> = rotted
            .iter()
            .filter(|(slot, ballot)| top.get(slot).is_none_or(|(b, _)| *ballot > b))
            .map(|(slot, _)| *slot)
            .collect();
        let safe: BTreeMap<Slot, Command> = top
            .into_iter()
            .filter(|(slot, _)| !blocked.contains(slot))
            .map(|(slot, (_, command))| (slot, command))
            .collect();
        let end = self.end(segments, &complete);
        assert!(
            safe.keys().chain(blocked.iter()).all(|s| *s >= self.from),
            "Phase 1 reports only from its first slot"
        );
        Some(Phase1Close { safe, blocked, end })
    }

    /// The seal end of the segment before the leader's, when the leader's
    /// segment is pending.
    fn end(&self, segments: &[ReadSegment], complete: &BTreeSet<NodeId>) -> Option<Slot> {
        let own = segments.last()?;
        let before = segments.iter().rev().nth(1)?;
        if own.start.is_some() || before.end.is_some() {
            return None;
        }
        let start = before
            .start
            .expect("the segment before a pending one is in force");
        let voted = before
            .acceptors()
            .filter(|node| complete.contains(node))
            .flat_map(|node| {
                let report = &self.reports[&node];
                report.votes.keys().chain(report.faulty.keys()).copied()
            })
            .max()
            .map(|slot| Slot(slot.0 + 1));
        let end = [Some(start), Some(self.from), voted]
            .into_iter()
            .flatten()
            .max()
            .expect("a start");
        assert!(end >= start, "the end is at or above the start");
        Some(end)
    }
}

#[cfg(test)]
mod tests;
