//! The leader's **Phase 1** over the segments it reads (§J3.1,
//! `specs/tla/journal-state/`, `Phase1`).
//!
//! The leader asks every acceptor of every segment that is not retired, from
//! the first slot it writes; a promise comes in pages. Phase 1 is done when
//! the acceptors that answered completely hold a Phase-1 quorum of every
//! column of every grid read (§J5.1, through `membership/`). Then:
//!
//! - **each slot takes the highest vote** among the reports of every
//!   acceptor of every segment read, sealed or not (P2c). The seal end
//!   hides no vote: a vote of a sealed grid at or above its end has a ballot
//!   below every ballot of the next grid, so a vote of the next grid wins
//!   where there is one, and where there is none the old value is safe to
//!   propose again (plain Matchmaker Paxos; `end_applied_safe` of
//!   `specs/tla/journal-read/`);
//! - **a slot whose top report is a faulty vote goes to the tri-state
//!   tally** (CTRL, §J5.3, [`Phase1::decide`]): it is decided when the
//!   answers but the faulty votes above the highest readable vote hold a
//!   Phase-1 quorum of every column of every segment read; else it is blocked, and
//!   the leader proposes nothing there until a late answer decides it or a
//!   replica gives its chosen command;
//! - **the seal end**, when the leader's segment is pending, is one above
//!   the highest slot where an acceptor of the segment before reported a
//!   vote, a faulty one included (#1313), and at least the start of that
//!   segment and the first slot the leader reads (every slot below it is
//!   applied, so chosen in that grid);
//! - **a slot that an acceptor dropped is skipped** (§J3.2,
//!   `specs/tla/journal-compact/`): it is chosen and applied at `F + 1`
//!   replicas of its set, so the leader proposes nothing there, and the
//!   seal end counts a dropped slot of the segment before as a vote (a
//!   compaction can drop the only votes a quorum held).

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::membership::AcceptorConfig;
use paros_consensus::proposer::{merge_report, qualifying_answers, slot_decidable};
use paros_consensus::{Ballot, NodeId, Slot};

use super::ReadSegment;
use crate::command::Command;
use crate::roles::message::PromisePage;
use crate::roles::ranges;

/// What one acceptor reported.
#[derive(Clone, Debug, Default)]
struct Report {
    votes: BTreeMap<Slot, (Ballot, Command)>,
    faulty: BTreeMap<Slot, Ballot>,
    /// The dropped ranges, `start .. below` by start.
    dropped: BTreeMap<Slot, Slot>,
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
    /// The ranges that an acceptor dropped, `start .. below` by start: no
    /// proposal there.
    pub dropped: BTreeMap<Slot, Slot>,
}

/// The Phase 1 of one ballot.
#[derive(Clone, Debug)]
pub struct Phase1 {
    ballot: Ballot,
    from: Slot,
    reports: BTreeMap<NodeId, Report>,
}

impl Phase1 {
    /// The Phase 1 of `ballot` over `segments`, from slot `from`. With no
    /// segment (a leader whose bind runs), it asks no acceptor and never
    /// closes.
    #[must_use]
    pub fn new(ballot: Ballot, from: Slot, segments: &[ReadSegment]) -> Self {
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
        for (start, below) in page.dropped {
            ranges::add(&mut report.dropped, start, below);
        }
        if let Some(next) = page.next {
            assert!(next > at, "a cursor moves forward");
            report.cursor = next;
            Some(next)
        } else {
            report.complete = true;
            None
        }
    }

    /// The acceptors that answered completely: the reports that
    /// [`Self::close`] reads.
    #[must_use]
    pub fn complete(&self) -> BTreeSet<NodeId> {
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
        if segments.is_empty() {
            return None;
        }
        let complete = self.complete();
        let covered = prior(segments)
            .iter()
            .all(|column| column.has_phase1_quorum(&complete));
        if !covered {
            return None;
        }
        let mut top: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let mut rotted: BTreeMap<Slot, Ballot> = BTreeMap::new();
        let mut dropped: BTreeMap<Slot, Slot> = BTreeMap::new();
        for (start, below) in complete.iter().flat_map(|node| &self.reports[node].dropped) {
            ranges::add(&mut dropped, *start, *below);
        }
        let skip = |slot: Slot| ranges::contains(&dropped, slot);
        for node in &complete {
            let report = &self.reports[node];
            // The highest ballot at each slot: the core's P2c merge, which
            // asserts that two votes at one ballot carry one command.
            for (slot, (ballot, command)) in &report.votes {
                merge_report(&mut top, *slot, *ballot, command.clone());
            }
            for (slot, ballot) in &report.faulty {
                rotted
                    .entry(*slot)
                    .and_modify(|held| *held = (*held).max(*ballot))
                    .or_insert(*ballot);
            }
        }
        probe!(
            sometimes,
            top.keys().chain(rotted.keys()).any(|slot| skip(*slot)),
            "leader: Phase 1 skips a vote in a dropped range"
        );
        let mut blocked: Vec<Slot> = Vec::new();
        for (slot, ballot) in rotted.iter().filter(|(slot, _)| !skip(**slot)) {
            if top.get(slot).is_some_and(|(b, _)| ballot <= b) {
                continue;
            }
            // The faulty vote is the top report: the tally decides the slot
            // only when the answers without it hold a quorum, and then its
            // value is the top readable vote, or none.
            if self.decide(segments, *slot).is_none() {
                blocked.push(*slot);
            }
        }
        let safe: BTreeMap<Slot, Command> = top
            .into_iter()
            .filter(|(slot, _)| !blocked.contains(slot) && !skip(*slot))
            .map(|(slot, (_, command))| (slot, command))
            .collect();
        let end = self.end(segments, &complete);
        assert!(
            safe.keys().chain(blocked.iter()).all(|s| *s >= self.from),
            "Phase 1 reports only from its first slot"
        );
        assert!(
            safe.keys().chain(blocked.iter()).all(|s| !skip(*s)),
            "Phase 1 proposes nothing in a dropped range"
        );
        Some(Phase1Close {
            safe,
            blocked,
            end,
            dropped,
        })
    }

    /// The acceptors whose complete answer reports a faulty vote at
    /// `slot`: the acceptors that a CTRL repair of the slot writes to.
    #[must_use]
    pub fn faulty_at(&self, slot: Slot) -> Vec<NodeId> {
        self.complete()
            .into_iter()
            .filter(|node| self.reports[node].faulty.contains_key(&slot))
            .collect()
    }

    /// The first acceptor whose complete answer dropped `slot`: the slot is
    /// then chosen and applied at `F + 1` replicas (§J3.2), and the leader
    /// proposes nothing there, as [`Self::close`] skips it.
    #[must_use]
    pub fn dropped_by(&self, slot: Slot) -> Option<NodeId> {
        self.complete()
            .into_iter()
            .find(|node| ranges::contains(&self.reports[node].dropped, slot))
    }

    /// The tri-state tally of `slot` (CTRL, §J5.3, `specs/tla/journal-ctrl/`,
    /// `ProbeDecide`): the command to propose there, once the complete
    /// answers decide it. The threshold is the highest readable vote at
    /// `slot` among the complete answers. An answer qualifies unless it is
    /// a faulty vote above the threshold ([`Self::qualifying`]). The slot is
    /// decided when the qualifying answers hold a Phase-1 quorum of every
    /// column of every segment read (the core's `slot_decidable`, each
    /// column a configuration of its own): then the readable vote at the threshold is safe,
    /// or a `Noop` when no answer holds one. `None`: the slot waits for more
    /// answers, or for a replica that applied it.
    ///
    /// # Panics
    ///
    /// When no segment is read.
    #[must_use]
    pub fn decide(&self, segments: &[ReadSegment], slot: Slot) -> Option<Command> {
        assert!(!segments.is_empty(), "Phase 1 reads at least one segment");
        let top = self.top(slot);
        let complete = self.complete();
        let reporters = self.reporters(slot);
        assert!(
            reporters.keys().all(|node| complete.contains(node)),
            "only a complete answer reports a faulty vote to the tally"
        );
        let decided = slot_decidable(
            &prior(segments),
            &complete,
            Some(&reporters),
            top.map(|(ballot, _)| *ballot),
        );
        probe!(
            sometimes,
            decided && top.is_none(),
            "leader: the tally decides a faulty slot with a Noop"
        );
        probe!(
            sometimes,
            decided && top.is_some(),
            "leader: the tally decides a faulty slot with its readable vote"
        );
        decided.then(|| top.map_or(Command::Noop, |(_, command)| command.clone()))
    }

    /// The complete answers that qualify in the tally of `slot`: each one
    /// but a faulty vote above the highest readable vote there.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn qualifying(&self, slot: Slot) -> BTreeSet<NodeId> {
        let complete = self.complete();
        let threshold = self.top(slot).map(|(ballot, _)| *ballot);
        let qualifying = qualifying_answers(&complete, Some(&self.reporters(slot)), threshold);
        assert!(
            qualifying.is_subset(&complete),
            "only a complete answer qualifies"
        );
        qualifying
    }

    /// The faulty ballot at `slot` of each complete answer that holds one:
    /// the reporters of the core's tally.
    fn reporters(&self, slot: Slot) -> BTreeMap<NodeId, Ballot> {
        self.complete()
            .into_iter()
            .filter_map(|node| Some((node, *self.reports[&node].faulty.get(&slot)?)))
            .collect()
    }

    /// The highest readable vote at `slot` among the complete answers.
    fn top(&self, slot: Slot) -> Option<&(Ballot, Command)> {
        self.complete()
            .into_iter()
            .filter_map(|node| self.reports[&node].votes.get(&slot))
            .max_by_key(|(ballot, _)| *ballot)
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
                let dropped = report.dropped.values().map(|below| Slot(below.0 - 1));
                report
                    .votes
                    .keys()
                    .chain(report.faulty.keys())
                    .copied()
                    .chain(dropped)
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

/// The configurations whose Phase-1 quorum a decision needs: each column of
/// each segment read, as its own configuration, once. A column that two
/// segments share is one configuration: its quorum is asked once.
fn prior(segments: &[ReadSegment]) -> Vec<AcceptorConfig<NodeId>> {
    let mut prior: Vec<AcceptorConfig<NodeId>> = Vec::new();
    for column in segments.iter().flat_map(|segment| &segment.columns) {
        if !prior.contains(column) {
            prior.push(column.clone());
        }
    }
    prior
}

#[cfg(test)]
mod tests;
