//! The leader's **repair probe** (CTRL Stage 8): the Phase 1 that keeps
//! running for the slots a won election could not decide.
//!
//! The state ([`RepairProbe`]) inherits the election's ballot, first slot and
//! promise quorum, and pages the stragglers' `Promise`s through the same
//! [`PromiseTally`]; the [`Proposer`] methods here fold those pages, decide
//! every slot the tally allows ([`ProbeDecision`]), and close the probe —
//! taking its resign clock with it — when nothing stays blocked.

use std::collections::{BTreeMap, BTreeSet};

use super::{ProbeDecision, PromiseFold, PromiseTally, Proposer, merge_report, slot_decidable};
use crate::membership::AcceptorConfig;
use crate::types::{Ballot, Command, Control, NodeId, Slot};

/// The leader's open **distributed commitment determination** (Stage 8): the
/// faulty slots its winning promise quorum resolved neither as Case 1 (some
/// `have`) nor Case 2 (a full Q1 of qualifying `none`). The leader keeps
/// re-querying the peers that have not answered (their `Promise` pages arrive
/// through the ordinary Phase-1 path, at the leader's own ballot) and decides
/// each blocked slot the moment the tally allows; a probe that stays blocked
/// for a full recovery timeout resigns the leadership (CTRL §4.2).
#[derive(Clone, Debug)]
pub struct RepairProbe {
    /// The paging half, shared with [`Election`]: the ballot, the first slot,
    /// the stragglers that answered completely, and their next cursors.
    pub(super) promises: PromiseTally,
    /// The prior configurations the election covered: a blocked slot is
    /// decidable only once a full Q1 of qualifying answers holds in **every**
    /// one of them (the same predicate as the election's), and the
    /// straggler re-query fans out to their union.
    pub(super) prior: Vec<AcceptorConfig>,
    /// Faulty reports per still-blocked slot: reporter → accepted ballot.
    pub(super) faulty_reports: BTreeMap<Slot, BTreeMap<NodeId, Ballot>>,
    /// Highest-ballot `have` seen per still-blocked slot.
    pub(super) best_have: BTreeMap<Slot, (Ballot, Command)>,
    /// Slots still undecidable (Case 3: wait).
    pub(super) blocked: BTreeSet<Slot>,
    /// Driver ticks this probe has been open (the caller's resign clock).
    /// It lives here, with the probe it times, so that closing a probe —
    /// by a decision, a commit, a snapshot install or an abandoned
    /// leadership — takes the clock with it and no caller has to remember
    /// to reset one.
    pub(super) elapsed: u64,
}

impl RepairProbe {
    /// The leadership ballot the probe queries at.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.promises.ballot
    }

    /// First slot the original Phase 1 covered — the cursor a re-sent
    /// `Prepare` echoes.
    #[must_use]
    pub fn suffix_start(&self) -> Slot {
        self.promises.from_slot
    }

    /// The slots still undecidable (Case 3: wait).
    #[must_use]
    pub fn blocked(&self) -> &BTreeSet<Slot> {
        &self.blocked
    }

    /// The stragglers to re-query: the members of the prior configurations
    /// the election covered — the Phase-1 addressee union — that have not
    /// answered their full suffix. `me` is never a straggler.
    #[must_use]
    pub fn stragglers(&self, me: NodeId) -> Vec<NodeId> {
        let mut unanswered: Vec<NodeId> = self
            .prior
            .iter()
            .flat_map(|c| c.members().iter().copied())
            .filter(|p| *p != me && !self.promises.answered.contains(p))
            .collect();
        unanswered.sort_unstable();
        unanswered.dedup();
        unanswered
    }
}

impl Proposer {
    // ---- repair probe -------------------------------------------------------

    /// The open repair probe, if any.
    #[must_use]
    pub fn probe(&self) -> Option<&RepairProbe> {
        self.probe.as_ref()
    }

    /// Advance the open probe's clock by one driver tick and report its new
    /// age; `None` when no probe is open. The caller owns the *policy* (how
    /// many ticks are too many); the probe only counts.
    pub fn tick_probe(&mut self) -> Option<u64> {
        let probe = self.probe.as_mut()?;
        probe.elapsed = probe.elapsed.saturating_add(1);
        Some(probe.elapsed)
    }

    /// The open probe's age in driver ticks, `None` when none is open.
    #[must_use]
    pub fn probe_elapsed(&self) -> Option<u64> {
        self.probe.as_ref().map(|probe| probe.elapsed)
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
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: &BTreeMap<Slot, (Ballot, Command)>,
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
        probe.promises.close_page(from, next_from_slot)
    }

    /// Decide every blocked slot the current probe tally allows: Case 1
    /// (re-propose the best `have`) or Case 2 (a full Q1 of qualifying
    /// answers with no `have`: decide `Noop`). Closes the probe when nothing
    /// stays blocked. Empty when no probe is open.
    pub fn resolve_probe(&mut self) -> Vec<ProbeDecision> {
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
            let (command, from_have) = have.map_or_else(
                || (Command::Control(Control::Noop), false),
                |(_b, command)| (command.clone(), true),
            );
            probe.blocked.remove(&slot);
            probe.best_have.remove(&slot);
            probe.faulty_reports.remove(&slot);
            decisions.push(ProbeDecision {
                slot,
                command,
                from_have,
            });
        }
        if probe.blocked.is_empty() {
            self.probe = None;
        }
        decisions
    }

    /// A decision for `slot` arrived elsewhere (Case 1 through the commit
    /// path rather than a straggler's `Promise`): drop it from the probe,
    /// closing the probe — and with it its clock — when nothing stays
    /// blocked.
    pub fn probe_resolved_elsewhere(&mut self, slot: Slot) {
        let Some(probe) = self.probe.as_mut() else {
            return;
        };
        if !probe.blocked.remove(&slot) {
            return;
        }
        probe.best_have.remove(&slot);
        probe.faulty_reports.remove(&slot);
        if probe.blocked.is_empty() {
            self.probe = None;
        }
    }

    /// A snapshot install folded everything below `first`: a probe blocked
    /// below the boundary is resolved by the fold as well, and the probe
    /// closes when nothing stays blocked.
    pub fn probe_retain_from(&mut self, first: Slot) {
        if let Some(probe) = self.probe.as_mut() {
            probe.blocked = probe.blocked.split_off(&first);
            probe.best_have = probe.best_have.split_off(&first);
            probe.faulty_reports = probe.faulty_reports.split_off(&first);
            if probe.blocked.is_empty() {
                self.probe = None;
            }
        }
    }
}
