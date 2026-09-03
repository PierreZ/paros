//! **Phase 1**: the campaign a candidate runs before it may propose anything.
//!
//! The state ([`Election`]) is one ballot's promise tally across every prior
//! configuration; the [`Proposer`] methods here open it, fold the paged
//! `Promise`s into it, and close it into a [`Phase1Outcome`] the wiring turns
//! into a leadership.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    Campaign, Phase1Outcome, PromiseFold, PromiseTally, Proposer, RepairProbe, merge_report,
    slot_decidable,
};
use crate::membership::AcceptorConfig;
use crate::types::{Ballot, Command, NodeId, Slot};

/// Volatile per-ballot Phase-1 state while a candidate recovers the log
/// suffix.
///
/// # Cross-configuration completion (#121)
///
/// Phase 1 is complete when **every** prior configuration in `prior` holds a
/// Phase-1 quorum of promises at `ballot` — `quorum(C1) AND quorum(C2) AND …`,
/// never `quorum(union(C1, C2, …))`. The two are not equivalent: a large
/// promise set drawn mostly from `C1` satisfies the union's quorum while
/// failing to intersect a Phase-2 quorum of `C2`, so a value `C2` already
/// chose stays invisible and the new leader proposes another — two values
/// chosen for one slot. One promise counts toward every configuration that
/// contains its sender (the normal case: consecutive configurations overlap
/// heavily), through one shared `promised_by` pool with a per-configuration
/// tally ([`Election::covered`]). Value selection runs over *all* gathered
/// promises regardless of configuration (`recovered`), exactly as before.
///
/// On a plain deployment `prior` is the one static configuration, so the
/// predicate is today's single quorum comparison.
#[derive(Clone, Debug)]
pub struct Election {
    /// The paging half: the ballot, the first slot, who answered completely,
    /// and each non-terminal answerer's next cursor.
    pub(super) promises: PromiseTally,
    /// `C_b`: the configuration this ballot runs Phase 2 with once won —
    /// registered with the matchmakers before this election opened, or the
    /// static configuration on a plain deployment.
    pub(super) config: AcceptorConfig,
    /// `H_b`: every distinct prior configuration whose Phase-1 quorum this
    /// election must independently obtain. Empty means nothing below this
    /// ballot survives the matchmakers' watermark, so Phase 1 is trivially
    /// complete (an explicit, gated case, never an accident).
    pub(super) prior: Vec<AcceptorConfig>,
    /// Highest-ballot accepted command per slot seen across the promise quorum,
    /// for slots `>= from_slot`. Drives gap-fill re-proposal once leader.
    pub(super) recovered: BTreeMap<Slot, (Ballot, Command)>,
    /// The tri-state's third answer, per slot: which acceptor reported its copy
    /// **faulty** (value lost, identity known) at what accepted ballot. A
    /// faulty report is silence toward the none-tally, never denial: it blocks
    /// the no-op gap fill at its slot until [`slot_decidable`] finds a full Q1
    /// of qualifying reports (Stage 8, CTRL restatement R2/R3).
    pub(super) faulty_reports: BTreeMap<Slot, BTreeMap<NodeId, Ballot>>,
}

impl Election {
    /// The ballot this election runs under.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.promises.ballot
    }

    /// `C_b`: the configuration a won election runs Phase 2 with.
    #[must_use]
    pub fn config(&self) -> &AcceptorConfig {
        &self.config
    }

    /// Whether every prior configuration holds a Phase-1 quorum of promises —
    /// the completion predicate, in one readable place (see the type doc).
    #[must_use]
    pub fn covered(&self) -> bool {
        self.prior
            .iter()
            .all(|config| config.has_phase1_quorum(&self.promises.answered))
    }

    /// The Phase-1 addressees: the union of every prior configuration — the
    /// addressee list *is* a union, only the completion predicate is not —
    /// plus `C_b` itself, so the incoming members promise the ballot (and
    /// learn the configuration) before Phase 2 reaches them. `me` is never
    /// addressed (the candidate is its own first acceptor).
    #[must_use]
    pub fn targets(&self, me: NodeId) -> Vec<NodeId> {
        let mut targets: Vec<NodeId> = self
            .prior
            .iter()
            .chain(std::iter::once(&self.config))
            .flat_map(|c| c.members().iter().copied())
            .filter(|p| *p != me)
            .collect();
        targets.sort_unstable();
        targets.dedup();
        targets
    }
}

impl Proposer {
    // ---- Phase 1 ------------------------------------------------------------

    /// The open Phase 1, if any.
    #[must_use]
    pub fn election(&self) -> Option<&Election> {
        self.election.as_ref()
    }

    /// Open one Phase 1 for `campaign`: at its ballot, over
    /// `[from_slot, ..)`, against every prior configuration. The candidate
    /// is its own first acceptor: its promise counts toward every prior
    /// configuration that contains it, its `own_records` seed the P2c tally
    /// and its `own_faulty` entries seed the tri-state tally (a rotted copy
    /// must block the none-tally exactly like a peer's, never silently count
    /// as "nothing accepted here"). Returns the Phase-1 addressees.
    ///
    /// # Panics
    ///
    /// If a Phase 1 is already open, or a Phase-2 round is in flight (a
    /// campaign opens on a node that holds no leadership).
    pub fn open_phase1(
        &mut self,
        campaign: Campaign,
        own_records: &BTreeMap<Slot, (Ballot, Command)>,
        own_faulty: &BTreeMap<Slot, Ballot>,
    ) -> Vec<NodeId> {
        assert!(self.election.is_none(), "one Phase 1 per ballot");
        assert!(
            self.rounds.is_empty(),
            "a campaign opens with no Phase-2 round in flight"
        );
        let Campaign {
            me,
            ballot,
            config,
            prior,
            from_slot,
        } = campaign;
        let recovered: BTreeMap<Slot, (Ballot, Command)> = own_records
            .range(from_slot..)
            .map(|(s, v)| (*s, v.clone()))
            .collect();
        let mut faulty_reports: BTreeMap<Slot, BTreeMap<NodeId, Ballot>> = BTreeMap::new();
        for (slot, ballot) in own_faulty.range(from_slot..) {
            faulty_reports.entry(*slot).or_default().insert(me, *ballot);
        }
        let mut promised_by = BTreeSet::new();
        promised_by.insert(me);
        let election = Election {
            promises: PromiseTally::new(ballot, from_slot, promised_by),
            config,
            prior,
            recovered,
            faulty_reports,
        };
        let targets = election.targets(me);
        self.election = Some(election);
        targets
    }

    /// Fold one `Promise` page from `from` into the open Phase 1: the
    /// reported accepted suffix merges by highest ballot per slot (P2c) and
    /// the faulty reports join the tri-state tally. A page is counted only
    /// at the exact cursor expected from its sender; a sender whose complete
    /// suffix is already merged is ignored.
    ///
    /// # Panics
    ///
    /// If two acceptors report different commands for one `(slot, ballot)`
    /// (the P2c merge's own rule — one ballot has one proposer, so that is a
    /// protocol violation, never an operating condition). Malformed *shape*
    /// is not asserted: a page whose cursor, bounds or ordering are wrong is
    /// refused as wire input ([`PromiseFold::Ignored`]).
    pub fn fold_promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: BTreeMap<Slot, (Ballot, Command)>,
        faulty: BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) -> PromiseFold {
        let Some(e) = self.election.as_mut() else {
            return PromiseFold::Ignored;
        };
        if !e
            .promises
            .accepts(from, ballot, from_slot, &accepted, &faulty, next_from_slot)
        {
            return PromiseFold::Ignored;
        }
        // The election's own merge: every slot the page names counts.
        for (slot, (ab, command)) in accepted {
            merge_report(&mut e.recovered, slot, ab, command);
        }
        for (slot, fb) in faulty {
            e.faulty_reports.entry(slot).or_default().insert(from, fb);
        }
        e.promises.close_page(from, next_from_slot)
    }

    /// The win gate (#121): every prior configuration covered — one
    /// predicate, in one place ([`Election::covered`]) — at a ballot the
    /// node's own `promise` has not moved past (#67/#88: a campaign whose
    /// ballot fell below its own promise is refused even with a quorum
    /// behind it; the election stays open and the next campaign ratchets
    /// past the promise).
    #[must_use]
    pub fn phase1_won(&self, promise: Ballot) -> bool {
        self.election
            .as_ref()
            .is_some_and(|e| e.covered() && e.promises.ballot >= promise)
    }

    /// Close the won Phase 1: hand its tally to the leadership and open the
    /// repair probe for every faulty-reported slot the tally could not
    /// decide (Case 3). `is_chosen` names the slots already decided here —
    /// a faulty report at one of them needs no probe: the value re-replicates
    /// through the normal commit/catch-up paths, repairing the faulty copies
    /// in place. Every Phase-2 round and the re-send cursor are reset: a
    /// fresh leadership starts with nothing in flight.
    ///
    /// # Panics
    ///
    /// If no Phase 1 is open, if it is not covered, or if a probe is
    /// already open (a candidate holds none).
    pub fn close_phase1(&mut self, is_chosen: impl Fn(Slot) -> bool) -> Phase1Outcome {
        let e = self
            .election
            .take()
            .expect("closing Phase 1 requires an open election");
        // Post-win restatement of the quorum half of the win condition: the
        // campaign that just closed really held a Phase-1 quorum of *every*
        // prior configuration.
        assert!(
            e.covered(),
            "a won election holds a promise quorum of every prior configuration"
        );
        assert!(self.probe.is_none(), "a candidate holds no repair probe");
        self.rounds.clear();
        self.resend_cursor = None;
        // ---- Faulty-slot tally (Stage 8, CTRL): a slot some quorum member
        // reported *faulty* is fair game for the pump only if the tally
        // already rules out a hidden chosen value (see `qualifying_answers`):
        // then a reported `have` re-proposes normally and an all-`none` slot
        // no-op fills normally. Anything else is **blocked** — Case 3: wait —
        // and moves to the repair probe, which keeps querying stragglers.
        let mut blocked: BTreeSet<Slot> = BTreeSet::new();
        let mut probe_have: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let mut probe_faulty: BTreeMap<Slot, BTreeMap<NodeId, Ballot>> = BTreeMap::new();
        for (slot, reporters) in &e.faulty_reports {
            if is_chosen(*slot) {
                continue;
            }
            let have = e.recovered.get(slot);
            let threshold = have.map(|(b, _)| *b);
            if slot_decidable(&e.prior, &e.promises.answered, Some(reporters), threshold) {
                continue;
            }
            blocked.insert(*slot);
            if let Some((b, command)) = have {
                probe_have.insert(*slot, (*b, command.clone()));
            }
            probe_faulty.insert(*slot, reporters.clone());
        }
        let highest_reported = e
            .recovered
            .keys()
            .chain(e.faulty_reports.keys())
            .max()
            .copied();
        if !blocked.is_empty() {
            self.probe = Some(RepairProbe {
                // The probe inherits the election's ballot, first slot and
                // promise quorum: it is the same Phase 1, still running for
                // the slots the quorum could not decide.
                promises: PromiseTally::new(
                    e.promises.ballot,
                    e.promises.from_slot,
                    e.promises.answered.clone(),
                ),
                prior: e.prior.clone(),
                faulty_reports: probe_faulty,
                elapsed: 0,
                best_have: probe_have,
                blocked: blocked.clone(),
            });
        }
        Phase1Outcome {
            ballot: e.promises.ballot,
            config: e.config,
            prior: e.prior,
            promised_by: e.promises.answered,
            recovered: e.recovered,
            blocked,
            highest_reported,
        }
    }
}
