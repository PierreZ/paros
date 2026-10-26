//! The node loop's clean stop (#560 (clean Ctrl-C shutdown)): what a
//! machine does between the operator's `SIGINT` or `SIGTERM` and its exit,
//! the same code in `parosd` and in the simulation (moonpool's graceful
//! reboot cancels the same token `parosd`'s signal does).
//!
//! 1. **Stop accepting.** A `Write`, `SetLeader`, `Truncate` or `Read`
//!    that arrives is refused at once with `stopping`: retryable, and
//!    the call took no part in any journal. Peer traffic goes on: the
//!    handoffs need it.
//! 2. **Finish the work in flight.** Every call a journal holds is answered
//!    from the slot it waits on, and every read waits out its own bound.
//! 3. **Hand the leaderships on**, at the same time as step 2: a held call
//!    is answered by the successor's decision too. Each journal this node leads relinquishes
//!    its authority to a member (`relinquish_to`, the cooperative handoff)
//!    and waits for the successor's first beat; the cell coordinator's term
//!    is handed on by its own task (`machine::coordinator`). A journal with
//!    no other member keeps its leadership, and one that cannot hand off
//!    within an election timeout is left to an election.
//! 4. **Flush.** Every batch in flight lands.
//!
//! [`DriverTunables::shutdown_within`] bounds it all: when it runs out the
//! loop returns anyway, and the log names the step that did not complete. A
//! stop at any point is a crash, which recovery already handles, so no
//! bound is a safety rule.

use std::collections::BTreeMap;
use std::time::Duration;

use moonpool_core::RandomProvider;
use paros_core::{Ballot, ColocatedNode, Handoff, JournalIdentifier, NodeId, NodeRole};
use paros_net::runtime::ParosProviders;

use super::DriverTunables;
use super::journals::Journals;

/// A call refused because the machine is stopping: reported once per call,
/// at the edge, before any journal sees it.
pub(crate) fn refused(call: &'static str) {
    moonpool_assertions::reachable!("stop: a stopping machine refuses a call");
    tracing::debug!(call, "call_refused_stopping");
}

/// Hand `node`'s leadership on to one of its members, drawn at random, when
/// it may (`relinquish_to`, the cooperative handoff): the successor runs on
/// under the same ballot, with no election. `None` when it does not lead,
/// leads alone, or its leadership is not settled yet.
pub(crate) fn hand_on<P: ParosProviders>(
    providers: &P,
    node: &mut ColocatedNode,
) -> Option<Handoff> {
    if !node.can_relinquish() {
        return None;
    }
    let candidates = node.handoff_candidates();
    if candidates.is_empty() {
        return None;
    }
    let target = candidates[providers.random().random_range(0..candidates.len())];
    let handoff = node.relinquish_to(target)?;
    assert!(handoff.to == target, "a handoff goes to the member drawn");
    assert!(!node.is_leader(), "a relinquished leadership demotes");
    moonpool_assertions::reachable!("stop: a stopping leader hands its journal on");
    Some(handoff)
}

/// The step of a clean stop that is still open, named in the log when the
/// bound runs out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Open {
    /// A call or a read is still held.
    InFlight,
    /// A journal still leads, or waits for its successor's first beat.
    Leaderships,
    /// The cell coordinator's task did not end yet.
    Coordinator,
    /// A batch's flush still flies.
    Flush,
}

impl Open {
    /// The step's label in the log.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Open::InFlight => "in_flight",
            Open::Leaderships => "leaderships",
            Open::Coordinator => "coordinator",
            Open::Flush => "flush",
        }
    }
}

/// A leadership handed on: whom to, under which ballot, and whether a beat
/// showed it moved.
#[derive(Clone, Copy, Debug)]
struct Handed {
    to: NodeId,
    ballot: Ballot,
    taken: bool,
}

/// A clean stop in progress.
pub(crate) struct Stop {
    /// When it began, on the loop's clock.
    began: Duration,
    /// When it returns whatever is still open.
    deadline: Duration,
    /// Beats since it began.
    beats: u64,
    /// Beats a leader that cannot hand off yet is waited for.
    patience: u64,
    /// The leaderships it handed on.
    handed: BTreeMap<JournalIdentifier, Handed>,
}

impl Stop {
    /// A stop that begins at `now`, bounded by `tunables`.
    pub(crate) fn begin(now: Duration, tunables: &DriverTunables) -> Self {
        let stop = Self {
            began: now,
            deadline: now.saturating_add(tunables.shutdown_within),
            beats: 0,
            patience: tunables.election_timeout_base,
            handed: BTreeMap::new(),
        };
        assert!(stop.deadline >= stop.began, "a stop's bound lies ahead");
        assert!(stop.handed.is_empty(), "a stop begins with nothing handed");
        stop
    }

    /// How long the stop has run.
    pub(crate) fn took(&self, now: Duration) -> Duration {
        now.saturating_sub(self.began)
    }

    /// Whether the bound ran out.
    pub(crate) fn expired(&self, now: Duration) -> bool {
        assert!(self.deadline >= self.began, "a stop's bound lies ahead");
        now >= self.deadline
    }

    /// One more beat of the stop.
    pub(crate) fn beat(&mut self) {
        self.beats = self.beats.saturating_add(1);
    }

    /// Record that `journal`'s leadership went to `to` under `ballot`.
    pub(crate) fn handed(&mut self, journal: JournalIdentifier, to: NodeId, ballot: Ballot) {
        assert!(ballot > Ballot::zero(), "a handed ballot was minted");
        let earlier = self.handed.insert(
            journal,
            Handed {
                to,
                ballot,
                taken: false,
            },
        );
        // Only the minter relinquishes a ballot, once (one hop).
        assert!(
            earlier.is_none_or(|earlier| earlier.ballot < ballot),
            "a journal hands one ballot on once"
        );
    }

    /// A beat from `from` at `ballot` reached `journal`: it shows a handed
    /// leadership moved when its successor beats at the handed ballot or
    /// above, or anyone beats above it (the handoff was lost and an
    /// election moved it). Whether it was a successor taking the handoff.
    pub(crate) fn heard(
        &mut self,
        journal: JournalIdentifier,
        from: NodeId,
        ballot: Ballot,
    ) -> bool {
        let Some(handed) = self.handed.get_mut(&journal) else {
            return false;
        };
        if handed.taken {
            return false;
        }
        assert!(handed.ballot > Ballot::zero(), "a handed ballot was minted");
        let successor = from == handed.to && ballot >= handed.ballot;
        if successor || ballot > handed.ballot {
            handed.taken = true;
        }
        successor
    }

    /// The first step still open, or `None` when the stop is done.
    /// `coordinator_ended` says whether the cell coordinator's task ended.
    pub(crate) fn open<S, A>(
        &self,
        journals: &Journals<S, A>,
        coordinator_ended: bool,
    ) -> Option<Open> {
        let in_flight = journals
            .live
            .values()
            .any(|rt| !rt.waiters.pending.is_empty() || rt.waiters.reads.busy());
        for (journal, handed) in &self.handed {
            // One hop: a journal handed on leads again only through an
            // election, above the ballot it handed.
            if let Some(rt) = journals.live.get(journal)
                && rt.node.role() == NodeRole::Leader
            {
                assert!(
                    rt.node.ballot() > handed.ballot,
                    "a journal handed on leads again only above its ballot"
                );
            }
        }
        if in_flight {
            return Some(Open::InFlight);
        }
        let leading = journals.live.values().any(|rt| {
            rt.node.role() == NodeRole::Leader
                && !rt.node.handoff_candidates().is_empty()
                && self.beats < self.patience
        });
        if leading || self.handed.values().any(|handed| !handed.taken) {
            return Some(Open::Leaderships);
        }
        if !coordinator_ended {
            return Some(Open::Coordinator);
        }
        if journals.flying() {
            return Some(Open::Flush);
        }
        None
    }

    /// How many leaderships it handed on, and how many a successor took.
    pub(crate) fn handoffs(&self) -> (usize, usize) {
        let taken = self.handed.values().filter(|handed| handed.taken).count();
        (self.handed.len(), taken)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::{JournalId, TenantId};

    fn journal() -> JournalIdentifier {
        JournalIdentifier::new(TenantId(7), JournalId(9))
    }

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    #[test]
    fn a_handoff_is_taken_by_its_successor_or_an_election_above_it() {
        let mut stop = Stop::begin(Duration::ZERO, &DriverTunables::default());
        stop.handed(journal(), NodeId(2), ballot(4, 1));
        assert!(
            !stop.heard(journal(), NodeId(3), ballot(4, 1)),
            "a bystander at the ballot"
        );
        assert_eq!(stop.handoffs(), (1, 0));
        assert!(
            stop.heard(journal(), NodeId(2), ballot(4, 1)),
            "the successor"
        );
        assert_eq!(stop.handoffs(), (1, 1));
        let mut lost = Stop::begin(Duration::ZERO, &DriverTunables::default());
        lost.handed(journal(), NodeId(2), ballot(4, 1));
        assert!(
            !lost.heard(journal(), NodeId(3), ballot(5, 3)),
            "an election, not a handoff"
        );
        assert_eq!(lost.handoffs(), (1, 1));
    }

    #[test]
    fn the_bound_runs_out_after_shutdown_within() {
        let tunables = DriverTunables {
            shutdown_within: Duration::from_secs(3),
            ..DriverTunables::default()
        };
        let stop = Stop::begin(Duration::from_secs(10), &tunables);
        assert!(!stop.expired(Duration::from_secs(12)));
        assert!(stop.expired(Duration::from_secs(13)));
        assert_eq!(stop.took(Duration::from_secs(12)), Duration::from_secs(2));
    }
}
