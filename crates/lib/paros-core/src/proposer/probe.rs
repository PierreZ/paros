//! The leader's **repair probe** (CTRL Stage 8): the Phase 1 that keeps
//! running for the slots a won election could not decide.
//!
//! The state ([`RepairProbe`]) inherits the election's ballot, first slot and
//! promise quorum, and pages the stragglers' `Promise`s through the same
//! [`PromiseTally`]; the [`Proposer`] methods here fold those pages, decide
//! every slot the tally allows ([`ProbeDecision`]), and close the probe —
//! taking its resign clock with it — when nothing stays blocked.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    ProbeDecision, PromiseFold, PromiseTally, Proposer, member_union, merge_report, slot_decidable,
};
use crate::membership::AcceptorConfig;
use crate::types::{Ballot, Slot};

/// The leader's open **distributed commitment determination** (Stage 8): the
/// faulty slots its winning promise quorum resolved neither as Case 1 (some
/// `have`) nor Case 2 (a full Q1 of qualifying `none`). The leader keeps
/// re-querying the peers that have not answered (their `Promise` pages arrive
/// through the ordinary Phase-1 path, at the leader's own ballot) and decides
/// each blocked slot the moment the tally allows; a probe that stays blocked
/// for a full recovery timeout resigns the leadership (CTRL §4.2).
#[derive(Clone, Debug)]
pub struct RepairProbe<Id, V> {
    /// The paging half, shared with [`Election`]: the ballot, the first slot,
    /// the stragglers that answered completely, and their next cursors.
    pub(super) promises: PromiseTally<Id>,
    /// The prior configurations the election covered: a blocked slot is
    /// decidable only once a full Q1 of qualifying answers holds in **every**
    /// one of them (the same predicate as the election's), and the
    /// straggler re-query fans out to their union.
    pub(super) prior: Vec<AcceptorConfig<Id>>,
    /// Faulty reports per still-blocked slot: reporter → accepted ballot.
    pub(super) faulty_reports: BTreeMap<Slot, BTreeMap<Id, Ballot>>,
    /// Highest-ballot `have` seen per still-blocked slot.
    pub(super) best_have: BTreeMap<Slot, (Ballot, V)>,
    /// Slots still undecidable (Case 3: wait).
    pub(super) blocked: BTreeSet<Slot>,
    /// Driver ticks this probe has been open (the caller's resign clock).
    /// It lives here, with the probe it times, so that closing a probe —
    /// by a decision, a commit, a trim-point jump or an abandoned
    /// leadership — takes the clock with it and no caller has to remember
    /// to reset one.
    pub(super) elapsed: u64,
}

impl<Id: Copy + Ord, V> RepairProbe<Id, V> {
    /// The probe's own invariants: it always has work, and every tally it
    /// keeps is over a still-blocked slot.
    pub(super) fn assert_invariants(&self) {
        assert!(
            !self.blocked.is_empty(),
            "an open repair probe holds a blocked slot"
        );
        assert!(
            self.best_have.keys().all(|s| self.blocked.contains(s)),
            "the probe's have-tally is over blocked slots only"
        );
        assert!(
            self.faulty_reports.keys().all(|s| self.blocked.contains(s)),
            "the probe's faulty tally is over blocked slots only"
        );
        assert!(
            self.blocked
                .first()
                .is_none_or(|s| *s >= self.promises.from_slot),
            "a blocked slot lies inside the campaign range"
        );
    }

    /// The leadership ballot the probe queries at.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        assert!(
            !self.blocked.is_empty(),
            "an open repair probe holds a blocked slot"
        );
        self.promises.ballot
    }

    /// The slots still undecidable (Case 3: wait).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn blocked(&self) -> &BTreeSet<Slot> {
        assert!(
            !self.blocked.is_empty(),
            "an open repair probe holds a blocked slot"
        );
        assert!(
            self.best_have.keys().all(|s| self.blocked.contains(s)),
            "the probe's have-tally is over blocked slots only"
        );
        &self.blocked
    }

    /// The members whose complete suffix answer the probe holds: the
    /// election's promise quorum, the leader itself included, and every
    /// straggler that answered since.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn answered(&self) -> &BTreeSet<Id> {
        assert!(
            !self.promises.answered.is_empty(),
            "a repair probe inherits a promise quorum"
        );
        &self.promises.answered
    }

    /// The prior configurations the election covered: the straggler
    /// re-query fans out to their union.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn prior(&self) -> &[AcceptorConfig<Id>] {
        assert!(
            !self.prior.is_empty(),
            "a repair probe covers some prior configuration"
        );
        &self.prior
    }

    /// The `Prepare` the leader re-sends this tick (#343): at the
    /// leadership's ballot, from the first slot the original Phase 1 covered
    /// (the cursor the tally expects a first page at), to the stragglers —
    /// the members of the prior configurations the election covered (the
    /// Phase-1 addressee union) that have not answered their full suffix.
    /// `me` is never a straggler.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn requery(&self, me: Id) -> RepairQuery<Id> {
        let to: Vec<Id> = member_union(&self.prior)
            .into_iter()
            .filter(|p| *p != me && !self.promises.answered.contains(p))
            .collect();
        assert!(!to.contains(&me), "the leader is never its own straggler");
        assert!(
            self.blocked
                .first()
                .is_none_or(|s| *s >= self.promises.from_slot),
            "a blocked slot lies inside the campaign range"
        );
        RepairQuery {
            ballot: self.promises.ballot,
            from_slot: self.promises.from_slot,
            to,
        }
    }
}

/// One tick's straggler re-query of an open [`RepairProbe`]: the `Prepare`
/// the leader re-sends, and to whom.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairQuery<Id> {
    /// The leadership ballot the probe queries at.
    pub ballot: Ballot,
    /// The first slot the original Phase 1 covered: the cursor a straggler's
    /// first page must carry.
    pub from_slot: Slot,
    /// The stragglers: every member of the prior configurations but the
    /// leader that has not answered its full suffix.
    pub to: Vec<Id>,
}

impl<Id: Copy + Ord, V: Clone + PartialEq> Proposer<Id, V> {
    // ---- repair probe -------------------------------------------------------

    /// The open repair probe, if any.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn probe(&self) -> Option<&RepairProbe<Id, V>> {
        if let Some(probe) = &self.probe {
            assert!(
                self.election.is_none(),
                "a probe outlives its campaign, never overlaps it"
            );
            assert!(
                !probe.blocked.is_empty(),
                "an open repair probe holds a blocked slot"
            );
        }
        self.probe.as_ref()
    }

    /// Advance the open probe's clock by one driver tick and report its new
    /// age; `None` when no probe is open. The caller owns the *policy* (how
    /// many ticks are too many); the probe only counts.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn tick_probe(&mut self) -> Option<u64> {
        let probe = self.probe.as_mut()?;
        probe.elapsed = probe.elapsed.saturating_add(1);
        assert!(probe.elapsed > 0, "a ticked probe has aged");
        Some(probe.elapsed)
    }

    /// The open probe's age in driver ticks, `None` when none is open.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn probe_elapsed(&self) -> Option<u64> {
        let elapsed = self.probe.as_ref().map(|probe| probe.elapsed);
        assert!(
            elapsed.is_some() == self.probe.is_some(),
            "only an open probe has an age"
        );
        if elapsed.is_some() {
            assert!(
                self.election.is_none(),
                "a probe's clock runs after its campaign"
            );
        }
        elapsed
    }

    /// Fold one straggler `Promise` page into the open repair probe. Only the
    /// still-blocked slots matter: everything else was decided or
    /// re-proposed when the election closed. Same P2c/P2b rule as the
    /// election merge, over the probe's `have` tally.
    ///
    /// # Panics
    ///
    /// If two acceptors report different commands for one `(slot, ballot)`,
    /// exactly as in [`Proposer::fold_promise`]. A malformed page is refused,
    /// never asserted.
    pub fn fold_probe_promise(
        &mut self,
        from: Id,
        ballot: Ballot,
        from_slot: Slot,
        accepted: &BTreeMap<Slot, (Ballot, V)>,
        faulty: &BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) -> PromiseFold {
        let Some(probe) = self.probe.as_mut() else {
            return PromiseFold::Ignored;
        };
        if !probe
            .promises
            .accepts(from, ballot, from_slot, accepted, faulty, next_from_slot)
        {
            return PromiseFold::Ignored;
        }
        // The probe's own merge: only the slots it is still blocked on.
        for (slot, (ab, command)) in accepted {
            if probe.blocked.contains(slot) {
                merge_report(&mut probe.best_have, *slot, *ab, command.clone());
            }
        }
        for (slot, fb) in faulty {
            if probe.blocked.contains(slot) {
                probe
                    .faulty_reports
                    .entry(*slot)
                    .or_default()
                    .insert(from, *fb);
            }
        }
        let fold = probe.promises.close_page(from, next_from_slot);
        probe.assert_invariants();
        if fold == PromiseFold::Answered {
            assert!(
                probe.promises.answered.contains(&from),
                "an answered straggler is counted"
            );
        }
        assert!(
            fold != PromiseFold::Ignored,
            "a page past the guards always folds"
        );
        fold
    }

    /// Decide every blocked slot the current probe tally allows: Case 1
    /// (re-propose the best `have`) or Case 2 (a full Q1 of qualifying
    /// answers with no `have`, reported as no value for the caller to fill).
    /// Closes the probe when nothing stays blocked. Empty when no probe is
    /// open.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn resolve_probe(&mut self) -> Vec<ProbeDecision<V>> {
        let mut decisions = Vec::new();
        let Some(probe) = self.probe.as_mut() else {
            return decisions;
        };
        for slot in probe.blocked.clone() {
            let have = probe.best_have.get(&slot);
            let threshold = have.map(|(b, _)| *b);
            if !slot_decidable(
                &probe.prior,
                &probe.promises.answered,
                probe.faulty_reports.get(&slot),
                threshold,
            ) {
                continue;
            }
            let command = have.map(|(_b, command)| command.clone());
            probe.blocked.remove(&slot);
            probe.best_have.remove(&slot);
            probe.faulty_reports.remove(&slot);
            decisions.push(ProbeDecision { slot, command });
        }
        // A decided slot leaves every tally of the probe.
        assert!(
            decisions.iter().all(|d| !probe.blocked.contains(&d.slot)),
            "a decided slot is no longer blocked"
        );
        if probe.blocked.is_empty() {
            self.probe = None;
        } else {
            probe.assert_invariants();
        }
        decisions
    }

    /// A decision for `slot` arrived elsewhere (Case 1 through the commit
    /// path rather than a straggler's `Promise`): drop it from the probe,
    /// closing the probe — and with it its clock — when nothing stays
    /// blocked.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn probe_resolved_elsewhere(&mut self, slot: Slot) {
        let Some(probe) = self.probe.as_mut() else {
            return;
        };
        if !probe.blocked.remove(&slot) {
            return;
        }
        probe.best_have.remove(&slot);
        probe.faulty_reports.remove(&slot);
        assert!(
            !probe.blocked.contains(&slot),
            "a resolved slot is no longer blocked"
        );
        if probe.blocked.is_empty() {
            self.probe = None;
        } else {
            probe.assert_invariants();
        }
    }

    /// A trim-point jump dropped everything below `first`: a probe blocked
    /// below the boundary is resolved by the fold as well, and the probe
    /// closes when nothing stays blocked.
    ///
    /// **Proved by mutation** (#409): emptied, it survived the mutation
    /// hunt's 300 seeds until the split-floor scenario
    /// (`paros_sim::world::split_floor`) made a leader jump with its probe
    /// blocked below the point; it is now caught in seeds 81..=100 by "a
    /// repair probe surviving a trim-point jump keeps only retained slots".
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn probe_retain_from(&mut self, first: Slot) {
        if let Some(probe) = self.probe.as_mut() {
            probe.blocked = probe.blocked.split_off(&first);
            probe.best_have = probe.best_have.split_off(&first);
            probe.faulty_reports = probe.faulty_reports.split_off(&first);
            assert!(
                probe.blocked.first().is_none_or(|s| *s >= first),
                "no blocked slot survives below the jump"
            );
            if probe.blocked.is_empty() {
                self.probe = None;
            }
        }
        assert!(
            self.probe.as_ref().is_none_or(|p| !p.blocked.is_empty()),
            "an open repair probe holds a blocked slot"
        );
    }
}
