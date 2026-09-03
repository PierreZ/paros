//! The **proposer** component: the leader-side tallies of Multi-Paxos — the
//! Phase-1 election, its CTRL repair probe, the per-slot Phase-2 rounds and
//! the bounded recovery a fresh leadership drains — judged over a
//! [`QuorumSystem`](crate::QuorumSystem), never over a raw count.
//!
//! The component holds **volatile** state only: everything here dies whole
//! when a leadership does ([`Proposer::abandon`]), and none of it is ever
//! persisted. It decides three things and nothing else:
//!
//! - **Phase 1 is complete** when *every* prior configuration holds a quorum
//!   of promises ([`Election::covered`], the #121 rule — `quorum(C1) AND
//!   quorum(C2)`, never `quorum(union)`), and the winner's recovered suffix
//!   is the highest-ballot report per slot (P2c, at the merge).
//! - **A faulty slot is decidable** once a full Phase-1 quorum of qualifying
//!   answers holds in every prior configuration (CTRL R2/R3); until then it
//!   is blocked and the repair probe keeps asking the stragglers.
//! - **A slot is chosen** once a Phase-2 quorum of the ballot's configuration
//!   accepted the round's command ([`Proposer::decided`]).
//!
//! What it deliberately does *not* know: the node's role, its timers, the
//! wire (it builds no message), the acceptor's durable state (the caller
//! hands in its own records when Phase 1 opens) and the replica's chosen
//! prefix (the caller passes a predicate where the tally needs one). A
//! component must not acquire knowledge merely because the current
//! deployment colocates it — the same [`Proposer`] runs a single decree over
//! a one-slot log and a Multi-Paxos leadership over an unbounded one.
//!
//! [`RawNode`](crate::RawNode) is the wiring: it opens the phases, feeds the
//! folds, turns the outcomes into messages and role transitions, and pumps
//! the recovery one bounded page at a time.

use std::collections::{BTreeMap, BTreeSet};

use crate::matchmaker::AcceptorConfig;
use crate::node::{LEADER_RECOVERY_BATCH, PROMISE_BATCH};
use crate::single_decree::select_highest;
use crate::types::{Ballot, Command, Control, NodeId, Slot, command_fingerprint};

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
    /// The ballot this election runs under.
    ballot: Ballot,
    /// `C_b`: the configuration this ballot runs Phase 2 with once won —
    /// registered with the matchmakers before this election opened, or the
    /// static configuration on a plain deployment.
    config: AcceptorConfig,
    /// `H_b`: every distinct prior configuration whose Phase-1 quorum this
    /// election must independently obtain. Empty means nothing below this
    /// ballot survives the matchmakers' watermark, so Phase 1 is trivially
    /// complete (an explicit, gated case, never an accident).
    prior: Vec<AcceptorConfig>,
    /// First slot this election recovers (`chosen_index + 1`, or `Slot(0)`).
    from_slot: Slot,
    /// Acceptors (incl. self) that have promised `ballot` — the one shared
    /// promise pool every configuration's tally is drawn from.
    promised_by: BTreeSet<NodeId>,
    /// Highest-ballot accepted command per slot seen across the promise quorum,
    /// for slots `>= from_slot`. Drives gap-fill re-proposal once leader.
    recovered: BTreeMap<Slot, (Ballot, Command)>,
    /// The tri-state's third answer, per slot: which acceptor reported its copy
    /// **faulty** (value lost, identity known) at what accepted ballot. A
    /// faulty report is silence toward the none-tally, never denial: it blocks
    /// the no-op gap fill at its slot until [`slot_decidable`] finds a full Q1
    /// of qualifying reports (Stage 8, CTRL restatement R2/R3).
    faulty_reports: BTreeMap<Slot, BTreeMap<NodeId, Ballot>>,
    /// Next suffix-page cursor expected from each non-terminal acceptor.
    promise_next: BTreeMap<NodeId, Slot>,
}

impl Election {
    /// The ballot this election runs under.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// `C_b`: the configuration a won election runs Phase 2 with.
    #[must_use]
    pub fn config(&self) -> &AcceptorConfig {
        &self.config
    }

    /// `H_b`: the prior configurations whose quorums this election needs.
    #[must_use]
    pub fn prior(&self) -> &[AcceptorConfig] {
        &self.prior
    }

    /// First slot this election recovers.
    #[must_use]
    pub fn from_slot(&self) -> Slot {
        self.from_slot
    }

    /// The acceptors (incl. self) whose complete promise has been merged.
    #[must_use]
    pub fn promised_by(&self) -> &BTreeSet<NodeId> {
        &self.promised_by
    }

    /// Whether every prior configuration holds a Phase-1 quorum of promises —
    /// the completion predicate, in one readable place (see the type doc).
    #[must_use]
    pub fn covered(&self) -> bool {
        self.prior
            .iter()
            .all(|config| config.has_quorum(&self.promised_by))
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
            .flat_map(|c| c.members.iter().copied())
            .filter(|p| *p != me)
            .collect();
        targets.sort_unstable();
        targets.dedup();
        targets
    }
}

/// The leader's open **distributed commitment determination** (Stage 8): the
/// faulty slots its winning promise quorum resolved neither as Case 1 (some
/// `have`) nor Case 2 (a full Q1 of qualifying `none`). The leader keeps
/// re-querying the peers that have not answered (their `Promise` pages arrive
/// through the ordinary Phase-1 path, at the leader's own ballot) and decides
/// each blocked slot the moment the tally allows; a probe that stays blocked
/// for a full recovery timeout resigns the leadership (CTRL §4.2).
#[derive(Clone, Debug)]
pub struct RepairProbe {
    /// The leadership ballot the probe queries at.
    ballot: Ballot,
    /// The prior configurations the election covered: a blocked slot is
    /// decidable only once a full Q1 of qualifying answers holds in **every**
    /// one of them (the same predicate as the election's), and the
    /// straggler re-query fans out to their union.
    prior: Vec<AcceptorConfig>,
    /// First slot the original Phase 1 covered (re-sent `Prepare`s echo it).
    from_slot: Slot,
    /// Acceptors (incl. self) whose complete suffix answer has been merged.
    answered: BTreeSet<NodeId>,
    /// Faulty reports per still-blocked slot: reporter → accepted ballot.
    faulty_reports: BTreeMap<Slot, BTreeMap<NodeId, Ballot>>,
    /// Highest-ballot `have` seen per still-blocked slot.
    best_have: BTreeMap<Slot, (Ballot, Command)>,
    /// Slots still undecidable (Case 3: wait).
    blocked: BTreeSet<Slot>,
    /// Next suffix-page cursor expected from each non-terminal straggler.
    promise_next: BTreeMap<NodeId, Slot>,
}

impl RepairProbe {
    /// The leadership ballot the probe queries at.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// First slot the original Phase 1 covered.
    #[must_use]
    pub fn from_slot(&self) -> Slot {
        self.from_slot
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
            .flat_map(|c| c.members.iter().copied())
            .filter(|p| *p != me && !self.answered.contains(p))
            .collect();
        unanswered.sort_unstable();
        unanswered.dedup();
        unanswered
    }
}

/// Volatile state of one in-flight per-slot Phase-2 (`Accept`) round.
#[derive(Clone, Debug)]
pub struct Round {
    /// The ballot this slot is being accepted under.
    ballot: Ballot,
    /// The command being accepted for this slot.
    command: Command,
    /// Acceptors (incl. self) that have accepted this slot's command at `ballot`.
    accepted_by: BTreeSet<NodeId>,
}

impl Round {
    /// The ballot this slot is being accepted under.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The command being accepted for this slot.
    #[must_use]
    pub fn command(&self) -> &Command {
        &self.command
    }

    /// Acceptors (incl. self) that accepted this round.
    #[must_use]
    pub fn accepted_by(&self) -> &BTreeSet<NodeId> {
        &self.accepted_by
    }
}

/// What licenses a fresh leadership to fill a slot its recovery does not
/// name: an explicit policy, never a flag, because the two answers rest on
/// two different safety arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryPolicy {
    /// The recovery came out of a won Phase 1. Quorum intersection guarantees
    /// a value already chosen at an unreported slot would have been reported,
    /// so an unreported slot is genuinely free and is filled with a
    /// [`Control::Noop`] (the election gap fill).
    Phase1Backed,
    /// The recovery was inherited through a cooperative handoff, which ran
    /// **no** Phase 1: there is no quorum report behind it, so a slot the
    /// predecessor did not explicitly describe is simply skipped — filling
    /// it could overwrite a value chosen under an older ballot.
    Inherited,
}

/// Bounded continuation for a leadership's recovered suffix.
#[derive(Clone, Debug)]
pub struct Recovery {
    /// Highest-ballot command reported for each retained slot.
    recovered: BTreeMap<Slot, Command>,
    /// Slots the Phase-1 tally could not decide (Case 3: wait): neither
    /// re-proposed nor no-op-filled by the pump; the open [`RepairProbe`]
    /// resolves them as stragglers answer.
    blocked: BTreeSet<Slot>,
    /// Next slot to recover or fill.
    cursor: Slot,
    /// One past the highest slot covered by the recovery.
    end: Slot,
    /// Whether an undescribed slot may be filled.
    policy: RecoveryPolicy,
}

impl Recovery {
    /// The policy this recovery runs under.
    #[must_use]
    pub fn policy(&self) -> RecoveryPolicy {
        self.policy
    }

    /// Whether `slot` is blocked on the repair probe (Case 3: wait).
    #[must_use]
    pub fn is_blocked(&self, slot: Slot) -> bool {
        self.blocked.contains(&slot)
    }

    /// How many slots the cursor has still to sweep.
    #[must_use]
    pub fn remaining(&self) -> usize {
        usize::try_from(self.end.0.saturating_sub(self.cursor.0)).unwrap_or(usize::MAX)
    }
}

/// One step of a recovery pump ([`Proposer::recovery_next`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryStep {
    /// The recovery names a command for this slot: re-propose it (P2c).
    Recovered(Command),
    /// Nobody reported the slot and the policy is [`RecoveryPolicy::Phase1Backed`]:
    /// fill it with a [`Control::Noop`].
    Fill,
    /// Nobody described the slot and the policy is [`RecoveryPolicy::Inherited`]:
    /// skip it.
    Undescribed,
}

/// How one `Promise` page folded into a tally.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromiseFold {
    /// Not merged: no matching tally, a sender already counted, or a page at
    /// the wrong cursor / of the wrong shape.
    Ignored,
    /// Merged; the sender has more pages and the next one starts here.
    Continue(Slot),
    /// Merged; the sender's complete suffix is now counted.
    Answered,
}

/// The campaign a Phase 1 opens for ([`Proposer::open_phase1`]).
#[derive(Clone, Debug)]
pub struct Campaign {
    /// The candidate: its own first acceptor.
    pub me: NodeId,
    /// The ballot the campaign runs at.
    pub ballot: Ballot,
    /// `C_b`: the configuration Phase 2 runs under once won.
    pub config: AcceptorConfig,
    /// `H_b`: the prior configurations whose quorums Phase 1 needs.
    pub prior: Vec<AcceptorConfig>,
    /// First slot the campaign recovers.
    pub from_slot: Slot,
}

/// A blocked slot the probe tally resolved ([`Proposer::resolve_probe`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeDecision {
    /// The slot.
    pub slot: Slot,
    /// The command to re-propose: the best `have` (Case 1) or a `Noop`
    /// (Case 2).
    pub command: Command,
    /// Whether the command came from a reported `have` (Case 1).
    pub from_have: bool,
}

/// What a closed, won Phase 1 hands the leadership
/// ([`Proposer::close_phase1`]).
#[derive(Clone, Debug)]
pub struct Phase1Outcome {
    /// The won ballot.
    pub ballot: Ballot,
    /// `C_b`: the configuration Phase 2 runs under.
    pub config: AcceptorConfig,
    /// `H_b`: the prior configurations the election covered.
    pub prior: Vec<AcceptorConfig>,
    /// First slot the election recovered.
    pub from_slot: Slot,
    /// Every acceptor whose promise was counted.
    pub promised_by: BTreeSet<NodeId>,
    /// The highest-ballot report per slot (P2c), for the whole campaign
    /// range — including slots below the caller's chosen prefix, which a
    /// faulty chosen record can pull the range down to.
    pub recovered: BTreeMap<Slot, (Ballot, Command)>,
    /// The faulty-reported slots the tally could not decide (Case 3): they
    /// went to the repair probe, which is open iff this is non-empty.
    pub blocked: BTreeSet<Slot>,
    /// The highest slot any report (accepted or faulty) named.
    pub highest_reported: Option<Slot>,
}

/// The qualifying answers among `answered` for a faulty-reported slot.
///
/// The unified CTRL restatement (R2/R3): let `threshold` be the highest
/// reported `have` ballot at the slot (`None` when nothing was reported). A
/// chosen value at or below `threshold` is value-identical to the best `have`
/// (the P2c chain), and a value chosen *above* it would leave a record at
/// ballot `> threshold` on some member of every Q2 — so an acceptor's answer
/// qualifies when it reported either nothing (`none`) or a record at ballot
/// `<= threshold`.
fn qualifying_answers(
    answered: &BTreeSet<NodeId>,
    reporters: Option<&BTreeMap<NodeId, Ballot>>,
    threshold: Option<Ballot>,
) -> BTreeSet<NodeId> {
    answered
        .iter()
        .filter(|node| {
            reporters
                .and_then(|reporters| reporters.get(*node))
                .is_none_or(|ballot| Some(*ballot) <= threshold)
        })
        .copied()
        .collect()
}

/// Whether a faulty-reported slot is decidable: a full Q1 of qualifying
/// answers holds in **every** prior configuration, so quorum intersection
/// rules out a hidden chosen value in each of them. Then the best `have` is
/// decided (Case 1) or, with no `have` at all, `Noop` (Case 2 — a full Q1 of
/// `none` per configuration). Anything less is Case 3: wait. With no prior
/// configuration at all nothing could have been chosen below this ballot, so
/// the slot is decidable outright.
fn slot_decidable(
    prior: &[AcceptorConfig],
    answered: &BTreeSet<NodeId>,
    reporters: Option<&BTreeMap<NodeId, Ballot>>,
    threshold: Option<Ballot>,
) -> bool {
    let qualifying = qualifying_answers(answered, reporters, threshold);
    prior.iter().all(|config| config.has_quorum(&qualifying))
}

/// A `Promise` page is useful only at the exact requested cursor, carries at
/// most the advertised bound across both tri-state maps, keeps them disjoint,
/// and advances its continuation past everything it reported.
fn promise_page_shape_valid(
    expected: Slot,
    accepted: &BTreeMap<Slot, (Ballot, Command)>,
    faulty: &BTreeMap<Slot, Ballot>,
    from_slot: Slot,
    next_from_slot: Option<Slot>,
) -> bool {
    let len = accepted.len() + faulty.len();
    from_slot == expected
        && len <= PROMISE_BATCH
        && accepted.keys().all(|slot| *slot >= from_slot)
        && faulty
            .keys()
            .all(|slot| *slot >= from_slot && !accepted.contains_key(slot))
        && next_from_slot.is_none_or(|next| {
            len == PROMISE_BATCH
                && next > from_slot
                && accepted.keys().next_back().is_none_or(|last| next > *last)
                && faulty.keys().next_back().is_none_or(|last| next > *last)
        })
}

/// Merge one reported `(ballot, command)` for `slot` into a highest-ballot
/// tally: the P2c selection rule ([`select_highest`], shared with the decree
/// kernel), at the merge. A lower report never replaces the recorded one, and
/// two reports at one ballot are the same command (one proposer per ballot,
/// P2b) — a disagreement is a protocol violation, not a tie.
fn merge_report(
    tally: &mut BTreeMap<Slot, (Ballot, Command)>,
    slot: Slot,
    ballot: Ballot,
    command: Command,
) {
    let mut best = tally.remove(&slot);
    select_highest(
        &mut best,
        (ballot, command),
        "two Phase-1 reports of one (slot, ballot) agree on the command",
    );
    tally.insert(
        slot,
        best.expect("the selection fold always holds a report"),
    );
}

/// The proposer component (see the module doc).
#[derive(Clone, Debug, Default)]
pub struct Proposer {
    /// The open Phase 1 while a candidate. `None` once leader.
    election: Option<Election>,
    /// The leader's open repair probe (Stage 8, CTRL).
    probe: Option<RepairProbe>,
    /// Per-slot in-flight Phase-2 rounds, keyed by slot. The leader streams these.
    rounds: BTreeMap<Slot, Round>,
    /// Remaining bounded recovery work for the current leadership.
    recovery: Option<Recovery>,
    /// Fair cursor for bounded pending-Accept re-sends.
    resend_cursor: Option<Slot>,
}

impl Proposer {
    /// A proposer with nothing open.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The component's own cross-field invariants. The proposer holds no
    /// durable state and no floor of its own: "no in-flight round survives
    /// below the compaction floor" couples two roles, so it is asserted by
    /// the wiring that owns both ([`crate::RawNode::assert_invariants`]).
    ///
    /// # Panics
    ///
    /// Panics when a proposer invariant is broken: a programmer error, never
    /// an operating condition.
    pub fn assert_invariants(&self) {
        // A probe is opened only by a won election and closes with its last
        // blocked slot, so an open one always has work.
        assert!(
            self.probe.as_ref().is_none_or(|p| !p.blocked.is_empty()),
            "an open repair probe holds at least one blocked slot"
        );
        // A handoff ran no Phase 1, so nothing could have been blocked by one.
        assert!(
            self.recovery
                .as_ref()
                .is_none_or(|r| r.policy == RecoveryPolicy::Phase1Backed || r.blocked.is_empty()),
            "an inherited recovery blocks no slot"
        );
    }

    /// Drop every open tally: the campaign, the probe, the rounds, the
    /// recovery and the re-send cursor. Leadership state dies whole.
    pub fn abandon(&mut self) {
        self.election = None;
        self.probe = None;
        self.rounds.clear();
        self.recovery = None;
        self.resend_cursor = None;
    }

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
            ballot,
            config,
            prior,
            from_slot,
            promised_by,
            recovered,
            faulty_reports,
            promise_next: BTreeMap::new(),
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
        if e.ballot != ballot || e.promised_by.contains(&from) {
            return PromiseFold::Ignored;
        }
        let expected = e.promise_next.get(&from).copied().unwrap_or(e.from_slot);
        if !promise_page_shape_valid(expected, &accepted, &faulty, from_slot, next_from_slot) {
            return PromiseFold::Ignored;
        }
        for (slot, (ab, command)) in accepted {
            merge_report(&mut e.recovered, slot, ab, command);
        }
        for (slot, fb) in faulty {
            e.faulty_reports.entry(slot).or_default().insert(from, fb);
        }
        if let Some(next) = next_from_slot {
            e.promise_next.insert(from, next);
            PromiseFold::Continue(next)
        } else {
            e.promise_next.remove(&from);
            e.promised_by.insert(from);
            PromiseFold::Answered
        }
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
            .is_some_and(|e| e.covered() && e.ballot >= promise)
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
            if slot_decidable(&e.prior, &e.promised_by, Some(reporters), threshold) {
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
                ballot: e.ballot,
                prior: e.prior.clone(),
                from_slot: e.from_slot,
                answered: e.promised_by.clone(),
                faulty_reports: probe_faulty,
                best_have: probe_have,
                blocked: blocked.clone(),
                promise_next: BTreeMap::new(),
            });
        }
        Phase1Outcome {
            ballot: e.ballot,
            config: e.config,
            prior: e.prior,
            from_slot: e.from_slot,
            promised_by: e.promised_by,
            recovered: e.recovered,
            blocked,
            highest_reported,
        }
    }

    // ---- repair probe -------------------------------------------------------

    /// The open repair probe, if any.
    #[must_use]
    pub fn probe(&self) -> Option<&RepairProbe> {
        self.probe.as_ref()
    }

    /// Fold one straggler `Promise` page into the open repair probe. Only the
    /// still-blocked slots matter: everything else was decided or
    /// re-proposed when the election closed. Same P2c/P2b rule as the
    /// election merge, over the probe's `have` tally.
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
        if probe.ballot != ballot || probe.answered.contains(&from) {
            return PromiseFold::Ignored;
        }
        let expected = probe
            .promise_next
            .get(&from)
            .copied()
            .unwrap_or(probe.from_slot);
        if !promise_page_shape_valid(expected, accepted, faulty, from_slot, next_from_slot) {
            return PromiseFold::Ignored;
        }
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
        if let Some(next) = next_from_slot {
            probe.promise_next.insert(from, next);
            PromiseFold::Continue(next)
        } else {
            probe.promise_next.remove(&from);
            probe.answered.insert(from);
            PromiseFold::Answered
        }
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
                &probe.answered,
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
    /// closing the probe when nothing stays blocked. Whether the slot was
    /// blocked.
    pub fn probe_resolved_elsewhere(&mut self, slot: Slot) -> bool {
        let Some(probe) = self.probe.as_mut() else {
            return false;
        };
        if !probe.blocked.remove(&slot) {
            return false;
        }
        probe.best_have.remove(&slot);
        probe.faulty_reports.remove(&slot);
        if probe.blocked.is_empty() {
            self.probe = None;
        }
        true
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

    // ---- Phase 2 ------------------------------------------------------------

    /// Every in-flight Phase-2 round, keyed by slot.
    #[must_use]
    pub fn rounds(&self) -> &BTreeMap<Slot, Round> {
        &self.rounds
    }

    /// Open the Phase-2 round for `slot` at `ballot`, with `own_vote` as its
    /// first accept when the proposer is itself an acceptor of the ballot's
    /// configuration and its promise allows the self-accept.
    ///
    /// # Panics
    ///
    /// If a round is already open at `slot`: one round per slot per
    /// leadership — the allocator only hands out fresh slots, a recovery
    /// visits each inherited slot once, and a blocked slot is opened only by
    /// the probe that resolves it. A second round would let one
    /// `(slot, ballot)` carry two commands.
    pub fn open_round(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
        own_vote: Option<NodeId>,
    ) {
        assert!(
            !self.rounds.contains_key(&slot),
            "a slot has at most one open Phase-2 round"
        );
        let mut accepted_by = BTreeSet::new();
        if let Some(me) = own_vote {
            accepted_by.insert(me);
        }
        self.rounds.insert(
            slot,
            Round {
                ballot,
                command,
                accepted_by,
            },
        );
    }

    /// Fold an `Accepted` from `from` into the round at `slot`: counted only
    /// for the round's own ballot and command fingerprint. Whether it
    /// counted. Which configurations `from` belongs to is the caller's
    /// guard; the decision ([`Proposer::decided`]) counts members only.
    pub fn fold_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot, vhash: u64) -> bool {
        let Some(round) = self.rounds.get_mut(&slot) else {
            return false;
        };
        if round.ballot != ballot || command_fingerprint(&round.command) != vhash {
            return false;
        }
        round.accepted_by.insert(from);
        true
    }

    /// Whether the round at `slot` holds a Phase-2 quorum of `config`: then
    /// its `(ballot, command)` is chosen.
    ///
    /// # Panics
    ///
    /// If a vote behind a decision came from outside `config`: the caller's
    /// guard refuses any other sender, restated here so the quorum arithmetic
    /// is never fed an id that could not have made a durable promise.
    #[must_use]
    pub fn decided(&self, slot: Slot, config: &AcceptorConfig) -> Option<(Ballot, Command)> {
        let round = self.rounds.get(&slot)?;
        if !config.has_quorum(&round.accepted_by) {
            return None;
        }
        assert!(
            round.accepted_by.iter().all(|n| config.contains(*n)),
            "every vote behind a decision comes from a configured acceptor"
        );
        Some((round.ballot, round.command.clone()))
    }

    /// Close the round at `slot` (decided, or abandoned by a decision that
    /// arrived from elsewhere).
    pub fn close_round(&mut self, slot: Slot) {
        self.rounds.remove(&slot);
    }

    /// Drop every round below `first` (a compaction or a snapshot install
    /// folded those slots: they are chosen).
    pub fn retain_rounds_from(&mut self, first: Slot) {
        self.rounds.retain(|slot, _| *slot >= first);
    }

    /// The next fair page of rounds whose `Accept`s are to be re-sent: at
    /// most [`LEADER_RECOVERY_BATCH`] rounds from the cursor up, wrapping
    /// around from the lowest round held, and the cursor advances past the
    /// page.
    pub fn resend_page(&mut self) -> Vec<(Slot, Ballot, Command)> {
        // No round survives below the compaction floor (the cross-role
        // invariant `RawNode::assert_invariants` pins), so a fresh cursor
        // starts at the bottom of the map and needs no floor handed in.
        let start = self.resend_cursor.unwrap_or(Slot(0));
        let mut pending: Vec<(Slot, Ballot, Command)> = self
            .rounds
            .range(start..)
            .take(LEADER_RECOVERY_BATCH)
            .map(|(s, r)| (*s, r.ballot, r.command.clone()))
            .collect();
        if pending.len() < LEADER_RECOVERY_BATCH {
            let remaining = LEADER_RECOVERY_BATCH - pending.len();
            pending.extend(
                self.rounds
                    .range(..start)
                    .take(remaining)
                    .map(|(s, r)| (*s, r.ballot, r.command.clone())),
            );
        }
        self.resend_cursor = pending
            .last()
            .and_then(|(slot, _, _)| slot.0.checked_add(1).map(Slot));
        pending
    }

    /// Whether a `Nack` for `ballot` at `slot` supersedes work this proposer
    /// has in flight: the open campaign at that ballot, or the open round at
    /// that slot and ballot.
    #[must_use]
    pub fn supersedes(&self, ballot: Ballot, slot: Slot) -> bool {
        self.election.as_ref().is_some_and(|e| e.ballot == ballot)
            || self.rounds.get(&slot).is_some_and(|r| r.ballot == ballot)
    }

    // ---- recovery -----------------------------------------------------------

    /// The open recovery continuation, if any.
    #[must_use]
    pub fn recovery(&self) -> Option<&Recovery> {
        self.recovery.as_ref()
    }

    /// Open the bounded recovery of `[cursor, end)`: `recovered` names the
    /// command per slot, `blocked` the slots the repair probe owns, and
    /// `policy` what an undescribed slot means.
    ///
    /// # Panics
    ///
    /// If a recovery is already open, or an inherited recovery blocks a slot
    /// (a handoff ran no Phase 1, so nothing could have been blocked by one).
    pub fn open_recovery(
        &mut self,
        recovered: BTreeMap<Slot, Command>,
        blocked: BTreeSet<Slot>,
        cursor: Slot,
        end: Slot,
        policy: RecoveryPolicy,
    ) {
        assert!(
            self.recovery.is_none(),
            "one recovery continuation per leadership"
        );
        assert!(
            policy == RecoveryPolicy::Phase1Backed || blocked.is_empty(),
            "an inherited recovery blocks no slot"
        );
        self.recovery = Some(Recovery {
            recovered,
            blocked,
            cursor,
            end,
            policy,
        });
    }

    /// Advance the recovery cursor one slot and say what the pump does with
    /// it. `None` when no recovery is open or its range is drained.
    pub fn recovery_next(&mut self) -> Option<(Slot, RecoveryStep)> {
        let recovery = self.recovery.as_mut()?;
        if recovery.cursor >= recovery.end {
            return None;
        }
        let slot = recovery.cursor;
        recovery.cursor = Slot(recovery.cursor.0.saturating_add(1));
        let step = match recovery.recovered.remove(&slot) {
            Some(command) => RecoveryStep::Recovered(command),
            // Only a Phase-1-backed recovery may invent a value for a slot
            // nobody reported (see `RecoveryPolicy`).
            None => match recovery.policy {
                RecoveryPolicy::Phase1Backed => RecoveryStep::Fill,
                RecoveryPolicy::Inherited => RecoveryStep::Undescribed,
            },
        };
        Some((slot, step))
    }

    /// Whether the open recovery holds `slot` blocked on the repair probe.
    #[must_use]
    pub fn recovery_blocked(&self, slot: Slot) -> bool {
        self.recovery
            .as_ref()
            .is_some_and(|recovery| recovery.is_blocked(slot))
    }

    /// How many slots the open recovery has still to sweep (0 when none).
    #[must_use]
    pub fn recovery_remaining(&self) -> usize {
        self.recovery.as_ref().map_or(0, Recovery::remaining)
    }

    /// Close the recovery once its cursor swept the whole range.
    ///
    /// # Panics
    ///
    /// If a drained recovery left a recovered slot at or past its cursor
    /// unvisited (consumed entries are removed as the cursor passes them;
    /// what survives is only the below-range residue the caller's prefix
    /// heal already handled).
    pub fn close_drained_recovery(&mut self) {
        if self.recovery_remaining() != 0 {
            return;
        }
        if let Some(recovery) = self.recovery.take() {
            assert!(
                recovery.cursor >= recovery.end,
                "a closed recovery drained its range"
            );
            assert!(
                recovery.recovered.range(recovery.cursor..).next().is_none(),
                "a closed recovery leaves no recovered slot unvisited"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::QuorumSystem;
    use crate::types::{ClientId, ClientSeq, Entry, Value};

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn cmd(seq: u64) -> Command {
        Command::User(Entry {
            client: ClientId(1),
            seq: ClientSeq(seq),
            value: Value(seq.to_le_bytes().to_vec()),
        })
    }

    fn config(members: &[u64]) -> AcceptorConfig {
        AcceptorConfig::new(
            members.iter().map(|n| NodeId(*n)).collect(),
            QuorumSystem::Majority,
        )
    }

    fn opened(prior: Vec<AcceptorConfig>) -> Proposer {
        let mut p = Proposer::new();
        let mut expected: Vec<NodeId> = prior
            .iter()
            .flat_map(|c| c.members.iter().copied())
            .chain([NodeId(1), NodeId(2)])
            .filter(|n| *n != NodeId(0))
            .collect();
        expected.sort_unstable();
        expected.dedup();
        let targets = p.open_phase1(
            Campaign {
                me: NodeId(0),
                ballot: ballot(1, 0),
                config: config(&[0, 1, 2]),
                prior,
                from_slot: Slot(0),
            },
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!(
            targets, expected,
            "Phase 1 addresses the union of the prior configurations and C_b"
        );
        p
    }

    /// Phase 1 completes only with a quorum of *every* prior configuration,
    /// never of their union (#121).
    #[test]
    fn phase1_needs_a_quorum_of_every_prior_configuration() {
        let mut p = opened(vec![config(&[0, 1, 2]), config(&[2, 3, 4])]);
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(0),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Answered
        );
        // {0, 1, 3} is a majority of the union's five members, yet the
        // second configuration holds only one promise (node 3): the union
        // rule would wrongly complete Phase 1 here.
        assert_eq!(
            p.fold_promise(
                NodeId(3),
                ballot(1, 0),
                Slot(0),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Answered
        );
        assert!(!p.phase1_won(ballot(1, 0)), "quorum(union) is not the rule");
        assert_eq!(
            p.fold_promise(
                NodeId(4),
                ballot(1, 0),
                Slot(0),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Answered
        );
        assert!(p.phase1_won(ballot(1, 0)));
        assert!(
            !p.phase1_won(ballot(2, 1)),
            "a campaign below the node's own promise never wins"
        );
    }

    /// The merge keeps the highest-ballot report per slot (P2c) and the
    /// closed outcome carries it.
    #[test]
    fn promise_merge_is_p2c() {
        let mut p = opened(vec![config(&[0, 1, 2])]);
        let mut low = BTreeMap::new();
        low.insert(Slot(0), (ballot(0, 1), cmd(1)));
        let mut high = BTreeMap::new();
        high.insert(Slot(0), (ballot(0, 2), cmd(2)));
        p.fold_promise(
            NodeId(1),
            ballot(1, 0),
            Slot(0),
            high,
            BTreeMap::new(),
            None,
        );
        p.fold_promise(NodeId(2), ballot(1, 0), Slot(0), low, BTreeMap::new(), None);
        assert!(p.phase1_won(ballot(1, 0)));
        let out = p.close_phase1(|_| false);
        assert_eq!(out.recovered.get(&Slot(0)), Some(&(ballot(0, 2), cmd(2))));
        assert_eq!(out.highest_reported, Some(Slot(0)));
        assert!(out.blocked.is_empty());
        assert!(p.probe().is_none());
        assert!(p.election().is_none());
    }

    /// A page at the wrong cursor, or from a sender already counted, is
    /// ignored; a continuation names the next cursor.
    #[test]
    fn promise_pages_are_cursor_checked() {
        let mut p = opened(vec![config(&[0, 1, 2])]);
        let full: BTreeMap<Slot, (Ballot, Command)> = (0..PROMISE_BATCH as u64)
            .map(|s| (Slot(s), (ballot(0, 1), cmd(s))))
            .collect();
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(0),
                full,
                BTreeMap::new(),
                Some(Slot(PROMISE_BATCH as u64)),
            ),
            PromiseFold::Continue(Slot(PROMISE_BATCH as u64))
        );
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(3),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Ignored,
            "a page at the wrong cursor is ignored"
        );
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(PROMISE_BATCH as u64),
                BTreeMap::new(),
                BTreeMap::new(),
                None,
            ),
            PromiseFold::Answered
        );
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(0),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Ignored,
            "a counted sender is not merged twice"
        );
    }

    /// A faulty report above the best `have` blocks the slot (Case 3) until
    /// a straggler's answer lets the probe decide it (Case 1).
    #[test]
    fn faulty_report_blocks_until_the_probe_decides() {
        let mut p = opened(vec![config(&[0, 1, 2])]);
        let mut faulty = BTreeMap::new();
        faulty.insert(Slot(0), ballot(0, 2));
        p.fold_promise(
            NodeId(1),
            ballot(1, 0),
            Slot(0),
            BTreeMap::new(),
            faulty,
            None,
        );
        assert!(p.phase1_won(ballot(1, 0)));
        let out = p.close_phase1(|_| false);
        assert!(
            out.blocked.contains(&Slot(0)),
            "a faulty report with no have blocks"
        );
        let probe = p.probe().expect("the probe opens for the blocked slot");
        assert_eq!(probe.stragglers(NodeId(0)), vec![NodeId(2)]);
        assert!(p.resolve_probe().is_empty(), "Case 3: wait");
        let mut have = BTreeMap::new();
        have.insert(Slot(0), (ballot(0, 2), cmd(7)));
        assert_eq!(
            p.fold_probe_promise(
                NodeId(2),
                ballot(1, 0),
                Slot(0),
                &have,
                &BTreeMap::new(),
                None
            ),
            PromiseFold::Answered
        );
        let decisions = p.resolve_probe();
        assert_eq!(
            decisions,
            vec![ProbeDecision {
                slot: Slot(0),
                command: cmd(7),
                from_have: true
            }]
        );
        assert!(
            p.probe().is_none(),
            "the last blocked slot closes the probe"
        );
    }

    /// A round decides on a quorum of *its configuration*; a vote from a
    /// non-member never counts toward it.
    #[test]
    fn rounds_decide_on_a_configuration_quorum() {
        let mut p = Proposer::new();
        let c = config(&[0, 1, 2]);
        p.open_round(Slot(5), ballot(1, 0), cmd(1), Some(NodeId(0)));
        assert!(p.decided(Slot(5), &c).is_none());
        assert!(!p.fold_accepted(NodeId(1), ballot(1, 0), Slot(5), 0));
        assert!(p.fold_accepted(
            NodeId(1),
            ballot(1, 0),
            Slot(5),
            command_fingerprint(&cmd(1))
        ));
        assert_eq!(p.decided(Slot(5), &c), Some((ballot(1, 0), cmd(1))));
        assert!(p.supersedes(ballot(1, 0), Slot(5)));
        assert!(!p.supersedes(ballot(2, 0), Slot(5)));
        p.close_round(Slot(5));
        assert!(p.rounds().is_empty());
    }

    /// The recovery pump fills undescribed slots only under a Phase-1-backed
    /// policy.
    #[test]
    fn recovery_policy_decides_the_fill() {
        let mut p = Proposer::new();
        let mut recovered = BTreeMap::new();
        recovered.insert(Slot(1), cmd(1));
        p.open_recovery(
            recovered.clone(),
            BTreeSet::new(),
            Slot(0),
            Slot(2),
            RecoveryPolicy::Phase1Backed,
        );
        assert_eq!(p.recovery_next(), Some((Slot(0), RecoveryStep::Fill)));
        assert_eq!(
            p.recovery_next(),
            Some((Slot(1), RecoveryStep::Recovered(cmd(1))))
        );
        assert_eq!(p.recovery_next(), None);
        assert_eq!(p.recovery_remaining(), 0);
        p.close_drained_recovery();
        assert!(p.recovery().is_none());

        p.open_recovery(
            recovered,
            BTreeSet::new(),
            Slot(0),
            Slot(2),
            RecoveryPolicy::Inherited,
        );
        assert_eq!(
            p.recovery_next(),
            Some((Slot(0), RecoveryStep::Undescribed))
        );
        assert_eq!(
            p.recovery_next(),
            Some((Slot(1), RecoveryStep::Recovered(cmd(1))))
        );
        p.close_drained_recovery();
        assert!(p.recovery().is_none());
    }
}
