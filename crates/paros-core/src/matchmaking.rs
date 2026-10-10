//! The **matchmaking phase**: what a candidate learns from the matchmakers
//! before it may send a single `Prepare` (Matchmaker Paxos §3.1–§3.2).
//!
//! The role holds one ballot's registration tally: which matchmakers have
//! answered completely, the union of the histories they returned, the
//! maximum GC watermark they reported, and the effective configuration
//! they hold. It decides three things and nothing else:
//!
//! - **the phase is complete** when a matchmaker quorum has answered
//!   ([`Matchmaking::quorum_held`] — asked at the membership boundary,
//!   never as a count);
//! - **`H_b`**, the prior configurations Phase 1 must obtain a quorum of
//!   *each* of ([`Matchmaking::prior`]): every distinct configuration
//!   registered at or above the maximum watermark, in ballot order. The
//!   union is filtered once, at closure, by the *maximum* watermark (§3.2)
//!   — never per reply, never by the minimum;
//! - whether an ordinary campaign's **belief is stale**
//!   ([`Matchmaking::stale_belief`]): the histories name a reconfiguration
//!   to a configuration other than the one registered, so the campaign
//!   must be abandoned and the effective configuration adopted.
//!
//! Why a quorum suffices: every earlier ballot registered with a matchmaker
//! quorum before it sent its own `Prepare` (the node's invariant 1), and any
//! two matchmaker quorums intersect, so at least one answerer holds its
//! record. Under-reporting is impossible; over-reporting (a configuration
//! that never got anywhere) only costs Phase 1 a few extra promises.
//!
//! What it deliberately does *not* know: the node's role, the wire (it
//! builds no request and reads no reply — the caller decodes a
//! [`MatchReply`] into a [`RegisteredPage`] or a refusal), the matchmaker
//! set it is asked over (handed in as data to every quorum question), and
//! what to do about a stale belief or a refusal. That is the wiring's
//! ([`crate::ColocatedNode::on_match_reply`]), exactly as
//! [`crate::proposer::Proposer`] tallies promises and the node turns the
//! outcome into a leadership. Paging follows the log's own `Promise` paging:
//! a matchmaker mid-answer is re-asked from the cursor its last page named,
//! and only a complete answer counts toward the quorum.
//!
//! # The effective configuration is a registration fact, not a chosen value
//!
//! A configuration becomes authoritative the moment a leader's
//! *reconfiguration* registration ([`RegistrationKind::Reconfiguration`])
//! has landed at a matchmaker quorum — before, and independently of, any
//! Phase 1 or Phase 2 under the new acceptor set. From then on quorum
//! intersection puts that record in every later campaign's histories, and
//! the effective configuration every ordinary campaign must register is the
//! **highest-ballot reconfiguration registration** those histories name.
//! Beliefs never count: the ledger also records every candidate's belief,
//! and "adopt the newest registration" made two candidates re-adopt each
//! other's abandoned beliefs and flip-flop forever. GC collects the flagged
//! *record* like any other, so every matchmaker also reports the effective
//! configuration as a durable scalar beside its history
//! ([`crate::MatchmakerHardState::effective`]) and the fold takes the
//! maximum of the two.
//!
//! # The membership probe
//!
//! [`MembershipProbe`] is the phase's registration-free sibling (#173): the
//! tally of a node that is *not* campaigning — one whose belief is only the
//! bootstrap default, or a heard belief that leaves it outside (#270) — and
//! asks a matchmaker quorum for nothing but
//! the effective configuration. It shares the phase's intersection argument
//! and none of its registrations.

use std::collections::{BTreeMap, BTreeSet};

use crate::matchmaker::{MatchOutcome, MatchRefusal, MatchReply, REGISTRY_PAGE, Registration};
use crate::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet};
use crate::types::Ballot;

pub use crate::matchmaker::RegistrationKind;

/// One `MatchB` page as the phase folds it: the `Registered` half of a
/// [`MatchOutcome`], decoded ([`RegisteredPage::from_outcome`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredPage {
    /// Where this page starts (echoed by the matchmaker).
    pub from_ballot: Ballot,
    /// `ballot -> registration` for the page's window, in ballot order.
    pub history: BTreeMap<Ballot, Registration>,
    /// Where the next page starts, when this one was cut short.
    pub next_from_ballot: Option<Ballot>,
    /// The matchmaker's watermark when the page was computed.
    pub gc_watermark: Ballot,
    /// The effective configuration the matchmaker durably holds.
    pub effective: Option<(Ballot, AcceptorConfig)>,
}

impl RegisteredPage {
    /// Decode one matchmaker's answer to a registration: the page it
    /// registered, or its refusal. `None` for a probe's answer
    /// ([`MatchOutcome::Probed`]), which no registration folds.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn from_outcome(outcome: MatchOutcome) -> Option<Result<Self, MatchRefusal>> {
        let probed = matches!(outcome, MatchOutcome::Probed { .. });
        let decoded = Self::decode(outcome);
        // Negative space: only a probe's answer decodes to nothing.
        assert!(
            decoded.is_none() == probed,
            "only a probe's answer is not a page"
        );
        decoded
    }

    /// [`Self::from_outcome`] before its postcondition.
    fn decode(outcome: MatchOutcome) -> Option<Result<Self, MatchRefusal>> {
        match outcome {
            MatchOutcome::Registered {
                from_ballot,
                history,
                next_from_ballot,
                gc_watermark,
                effective,
            } => Some(Ok(Self {
                from_ballot,
                history,
                next_from_ballot,
                gc_watermark,
                effective,
            })),
            MatchOutcome::Refused(refusal) => Some(Err(refusal)),
            MatchOutcome::Probed { .. } => None,
        }
    }

    /// Decode a whole reply: the answering matchmaker, and its page or
    /// refusal (`None` for a probe's answer). The caller checks the reply's
    /// addressee, ballot and generation first — those are its guards, not
    /// the phase's.
    #[must_use]
    pub fn from_reply(reply: MatchReply) -> (MatchmakerId, Option<Result<Self, MatchRefusal>>) {
        (reply.matchmaker, Self::from_outcome(reply.outcome))
    }
}

/// What one page did to the phase — the twin of the log's
/// [`PromiseFold`](crate::proposer::PromiseFold).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchFold {
    /// Not merged: a matchmaker already counted, or a page whose cursor or
    /// shape is not what that matchmaker owed next.
    Ignored,
    /// Merged; the matchmaker's answer is paged and its next page starts
    /// here. The registration does not count until the last page lands.
    Paged(Ballot),
    /// Merged; the matchmaker's complete answer is now counted.
    Registered,
}

/// Volatile per-ballot matchmaking state while a candidate registers its
/// configuration and collects the prior ones (see the module doc).
#[derive(Clone, Debug)]
pub struct Matchmaking {
    /// The ballot being registered.
    ballot: Ballot,
    /// `C_b`: the configuration this ballot will run with once registered.
    config: AcceptorConfig,
    /// What this campaign registers: an operator's deliberate change or
    /// this node's belief about the configuration in force. Only a
    /// [`RegistrationKind::Belief`] campaign is subject to the
    /// stale-configuration abort.
    kind: RegistrationKind,
    /// Matchmakers whose **complete** answer has been folded — a paged one
    /// counts only once its last page arrived.
    registered_by: BTreeSet<MatchmakerId>,
    /// Next history-page cursor expected from each matchmaker still
    /// mid-answer, exactly as the log's Phase 1 tracks its promise pages.
    page_next: BTreeMap<MatchmakerId, Ballot>,
    /// The union of every reported history so far, ballot by ballot. A ballot
    /// normally maps to one configuration (one proposer per ballot, write-once
    /// per matchmaker); two matchmakers disagreeing would be a registry bug,
    /// and rather than assert on wire input the union keeps *both* — Phase 1
    /// then needs a quorum of each, which is always safe.
    history: BTreeMap<Ballot, Vec<AcceptorConfig>>,
    /// The highest-ballot **reconfiguration** registration any reply named:
    /// the effective configuration below this ballot. `None` when no reply
    /// named one — the bootstrap configuration is then the only one ever in
    /// force.
    effective: Option<(Ballot, AcceptorConfig)>,
    /// The **maximum** reported GC watermark (§3.2's `w = max(w, ...)`):
    /// entries below it are excluded from `H_b`.
    watermark: Ballot,
    /// Distinct disagreements seen while unioning (two configurations at one
    /// ballot) — observability for the driver's audit report.
    disagreements: u64,
}

impl Matchmaking {
    /// Open the phase for `ballot` with `config` as `C_b`.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn new(ballot: Ballot, config: AcceptorConfig, kind: RegistrationKind) -> Self {
        let matchmaking = Self {
            ballot,
            config,
            kind,
            registered_by: BTreeSet::new(),
            page_next: BTreeMap::new(),
            history: BTreeMap::new(),
            effective: None,
            watermark: Ballot::zero(),
            disagreements: 0,
        };
        matchmaking.assert_invariants();
        matchmaking
    }

    /// The phase's own invariants: a matchmaker is mid-answer or done, never
    /// both; every unioned slot of the history holds distinct, non-empty
    /// configurations.
    ///
    /// # Panics
    ///
    /// If a registered matchmaker still owes a page, or the history holds an
    /// empty or duplicated entry.
    pub fn assert_invariants(&self) {
        assert!(
            self.page_next
                .keys()
                .all(|m| !self.registered_by.contains(m)),
            "a registered matchmaker owes no further page"
        );
        assert!(
            self.history.values().all(|configs| !configs.is_empty()),
            "every unioned ballot holds a configuration"
        );
        // Every extra configuration at a ballot is one disagreement, and
        // nothing else is (#269: the bound alone let a fold count every
        // fresh ballot).
        let extra: u64 = self
            .history
            .values()
            .map(|configs| u64::try_from(configs.len()).unwrap_or(u64::MAX) - 1)
            .sum();
        assert!(
            self.disagreements == extra,
            "every extra configuration at a ballot was counted as a disagreement"
        );
    }

    /// The ballot being registered.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// `C_b`: the configuration this ballot runs with once registered.
    #[must_use]
    pub fn config(&self) -> &AcceptorConfig {
        &self.config
    }

    /// What this campaign registers: a belief, or a reconfiguration.
    #[must_use]
    pub fn kind(&self) -> RegistrationKind {
        self.kind
    }

    /// The maximum GC watermark any reply reported so far.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn watermark(&self) -> Ballot {
        // A raised watermark was reported by someone.
        if self.watermark > Ballot::zero() {
            assert!(self.heard_anyone(), "a raised watermark was reported");
        }
        self.watermark
    }

    /// Whether any matchmaker's page has been folded, complete or not.
    fn heard_anyone(&self) -> bool {
        !self.registered_by.is_empty() || !self.page_next.is_empty()
    }

    /// The highest-ballot reconfiguration registration any reply named, with
    /// the ballot it was registered under.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn effective(&self) -> Option<&(Ballot, AcceptorConfig)> {
        // Nothing is effective that no answer named.
        if self.effective.is_some() {
            assert!(
                self.heard_anyone(),
                "an effective configuration was reported"
            );
        }
        self.effective.as_ref()
    }

    /// Distinct ballots two matchmakers reported with different
    /// configurations. Observability only: the union keeps both.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn disagreements(&self) -> u64 {
        // A disagreement is two configurations at one unioned ballot.
        if self.disagreements > 0 {
            assert!(!self.history.is_empty(), "a disagreement lies in the union");
        }
        self.disagreements
    }

    /// The union of every history folded so far, ballot by ballot and
    /// unfiltered — the read view a checker compares against what the
    /// matchmakers durably hold. [`Self::prior`] is the filtered, deduplicated
    /// form Phase 1 runs over; nothing in the core reads this.
    #[must_use]
    pub fn history(&self) -> &BTreeMap<Ballot, Vec<AcceptorConfig>> {
        &self.history
    }

    /// Whether this page counts at all: a matchmaker not already done, and
    /// a page whose shape and cursor are what that matchmaker owes next.
    /// Wire input, so a refusal is a `false`, never an assert — the twin of
    /// the log's own `PromiseTally::accepts`.
    fn accepts(&self, matchmaker: MatchmakerId, page: &RegisteredPage) -> bool {
        if self.registered_by.contains(&matchmaker) {
            return false;
        }
        // The first page starts wherever the matchmaker's own watermark is,
        // which the candidate cannot know; every later one must start at the
        // cursor that page named — or *above* it, at the sender's own
        // watermark, when a GC raise collected the cursor itself between two
        // pages (`Matchmaker::page` starts every page at `max(cursor,
        // watermark)`). What such a page skips sits below a floor `fold`
        // maxes into the closing watermark, so the union filtered at closure
        // loses nothing it would have kept. Refusing it instead wedged the
        // campaign at that matchmaker for good: the candidate re-asked from
        // the collected cursor on every election timeout, the matchmaker
        // answered from its floor every time, and `ColocatedNode::tick`
        // never abandons a pending matchmaking — the handover model's
        // claim 4 found the wedge on seed 8 once its tail raised floors
        // between pages.
        if let Some(expected) = self.page_next.get(&matchmaker) {
            let at_cursor = page.from_ballot == *expected;
            let cursor_collected =
                page.from_ballot > *expected && page.from_ballot == page.gc_watermark;
            if !at_cursor && !cursor_collected {
                return false;
            }
            // Either way a later page never starts below the cursor owed.
            assert!(
                page.from_ballot >= *expected,
                "a later page starts at or above its cursor"
            );
        }
        // Only the lower bound, exactly as `promise_page_shape_valid` checks
        // its page: an entry above the request's ballot would merely add a
        // configuration to `H_b`, which Phase 1 covering more than it must
        // is always safe.
        page.history.len() <= REGISTRY_PAGE
            && page.history.keys().all(|b| *b >= page.from_ballot)
            && page.next_from_ballot.is_none_or(|next| {
                page.history.len() == REGISTRY_PAGE
                    && next > page.from_ballot
                    && page
                        .history
                        .keys()
                        .next_back()
                        .is_none_or(|last| next > *last)
            })
    }

    /// Fold one `Registered` page from `matchmaker`: its history is unioned,
    /// its watermark maxed, and its effective configuration taken if newer.
    /// A page is counted only at the exact cursor expected from its sender;
    /// a matchmaker whose complete answer is already merged is ignored.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn fold(&mut self, matchmaker: MatchmakerId, page: RegisteredPage) -> MatchFold {
        if !self.accepts(matchmaker, &page) {
            return MatchFold::Ignored;
        }
        let watermark = self.watermark;
        let disagreements = self.disagreements;
        let RegisteredPage {
            history,
            next_from_ballot,
            gc_watermark,
            effective,
            ..
        } = page;
        for (ballot, registration) in history {
            let Registration { config, kind } = registration;
            if kind.is_reconfiguration() {
                self.raise_effective(ballot, &config);
            }
            let entry = self.history.entry(ballot).or_default();
            if !entry.contains(&config) {
                if !entry.is_empty() {
                    self.disagreements = self.disagreements.saturating_add(1);
                }
                entry.push(config);
            }
        }
        // The effective configuration is the maximum of what the histories
        // *show* and what the matchmakers *hold*: GC drops the record but
        // never the scalar, so a floor raised over the last reconfiguration
        // leaves the reported scalar as the only witness of the acceptor set
        // in force (see `MatchmakerHardState::effective`).
        if let Some((ballot, config)) = effective {
            self.raise_effective(ballot, &config);
        }
        // The watermark is the maximum reported, never the minimum and never
        // a per-reply filter (§3.2): the union is filtered once, at closure.
        self.watermark = self.watermark.max(gc_watermark);
        let fold = if let Some(next) = next_from_ballot {
            self.page_next.insert(matchmaker, next);
            MatchFold::Paged(next)
        } else {
            self.page_next.remove(&matchmaker);
            self.registered_by.insert(matchmaker);
            MatchFold::Registered
        };
        // The union is monotone: the watermark is the maximum reported and
        // the disagreement count only grows.
        assert!(
            self.watermark >= watermark,
            "the unioned watermark never falls"
        );
        assert!(
            self.disagreements >= disagreements,
            "disagreements are never forgotten"
        );
        self.assert_invariants();
        fold
    }

    /// Raise the effective configuration to `(ballot, config)` when it is
    /// newer than the one held (monotone in the ballot).
    fn raise_effective(&mut self, ballot: Ballot, config: &AcceptorConfig) {
        let held = self.effective.as_ref().map(|(b, _)| *b);
        crate::matchmaker::raise_effective(&mut self.effective, ballot, config);
        let now = self.effective.as_ref().map(|(b, _)| *b);
        // Monotone in the ballot, and never below what was offered.
        assert!(
            now >= held,
            "the effective configuration only moves forward"
        );
        assert!(
            now >= Some(ballot),
            "a raise lands at or above the offered ballot"
        );
    }

    /// Whether a matchmaker quorum of `matchmakers` has answered completely
    /// — the phase's own completion predicate, asked at the membership
    /// boundary and never as a count.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn quorum_held(&self, matchmakers: &MatchmakerSet) -> bool {
        let held = matchmakers.has_quorum(&self.registered_by);
        if held {
            assert!(
                !self.registered_by.is_empty(),
                "a registered quorum is someone"
            );
        }
        held
    }

    /// How many more complete answers the phase still waits for
    /// ([`MatchmakerSet::remaining`]).
    ///
    /// # Panics
    ///
    /// If `matchmakers` is not well formed.
    #[must_use]
    pub fn remaining(&self, matchmakers: &MatchmakerSet) -> usize {
        // The phase counts only the set it registers with.
        assert!(
            self.registered_by.iter().all(|m| matchmakers.contains(*m)),
            "a registration is counted only from a member"
        );
        let remaining = matchmakers.remaining(&self.registered_by);
        // Pair of `quorum_held`: nothing remains exactly when a quorum holds.
        assert!(
            (remaining == 0) == self.quorum_held(matchmakers),
            "nothing remains exactly when the quorum holds"
        );
        remaining
    }

    /// The matchmakers that have not answered completely, with the page
    /// cursor each owes next — whom a re-send addresses, and from where.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn unanswered(&self, matchmakers: &MatchmakerSet) -> Vec<(MatchmakerId, Option<Ballot>)> {
        let unanswered: Vec<(MatchmakerId, Option<Ballot>)> = matchmakers
            .unanswered(&self.registered_by)
            .map(|mm| (mm, self.page_next.get(&mm).copied()))
            .collect();
        assert!(
            unanswered
                .iter()
                .all(|(mm, _)| !self.registered_by.contains(mm)),
            "a registered matchmaker is never re-asked"
        );
        unanswered
    }

    /// How many matchmakers have answered completely.
    #[must_use]
    pub fn registered(&self) -> usize {
        self.registered_by.len()
    }

    /// `H_b`: every distinct configuration reported at a ballot at or above
    /// the maximum watermark, in ballot order.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn prior(&self) -> Vec<AcceptorConfig> {
        let mut prior: Vec<AcceptorConfig> = Vec::new();
        for configs in self.history.range(self.watermark..).map(|(_, c)| c) {
            for config in configs {
                if !prior.contains(config) {
                    prior.push(config.clone());
                }
            }
        }
        // `H_b` names each configuration once, and only what survived the
        // maximum watermark.
        assert!(
            prior
                .iter()
                .enumerate()
                .all(|(i, c)| !prior[..i].contains(c)),
            "H_b names each configuration once"
        );
        assert!(
            prior.len() <= self.history.values().map(Vec::len).sum::<usize>(),
            "H_b holds nothing the histories did not report"
        );
        prior
    }

    /// The stale-belief signal: the effective configuration the quorum's
    /// histories name, when this is an ordinary campaign that registered
    /// something else. A reconfiguration campaign is exempt — it *is* the
    /// next effective configuration. `None` when no reconfiguration was ever
    /// registered below this ballot, or the belief already matches it.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn stale_belief(&self) -> Option<(Ballot, AcceptorConfig)> {
        if self.kind.is_reconfiguration() {
            return None;
        }
        let (newest, config) = self.effective.as_ref()?;
        if *config == self.config {
            return None;
        }
        // Only an ordinary campaign can be stale, against a configuration
        // other than its own.
        assert!(
            !self.kind.is_reconfiguration(),
            "a reconfiguration is never stale"
        );
        Some((*newest, config.clone()))
    }
}

/// A **membership probe** (#173): the tally of a node that asks the
/// matchmakers which acceptor configuration is in force, without
/// campaigning and without registering anything.
///
/// Who probes: a node on a matchmaker deployment whose belief about the
/// configuration in force is only the bootstrap default — it has heard
/// nothing since it booted — at its first election timeout, before it
/// either campaigns or skips; and a node whose **heard** belief leaves it
/// outside, at every election timeout (#270). Three wedges came from acting
/// on a belief unasked:
///
/// - **Outside it**, a node never campaigns (leadership belongs inside the
///   acceptor set), so it could never learn that a reconfiguration moved it
///   *in*: a rotation whose every new member rebooted is a cluster where
///   every member believes itself outside, every non-member knows better
///   but does not lead, and nobody ever registers anything.
/// - **Inside it**, a node campaigned and *registered* the default before
///   `StaleConfiguration` corrected it — a record every later `H_b` then
///   covered (below). A flexible split with `q1 = 5` over a five-node
///   bootstrap, one member retired and the rest of the successor rebooted,
///   asked every later campaign for a promise nobody could give.
/// - **Outside a heard belief** (#270), a node once never asked again: a
///   reconfiguration that reached one matchmaker of three raised that
///   matchmaker's effective scalar alone, so the old members' probes
///   adopted it and left them outside, the new members' probes missed it
///   and left them outside the bootstrap, and nobody campaigned. So a node
///   outside its heard belief re-probes on its election clock, and a probe
///   adopts only a strictly newer configuration: a quorum that misses the
///   one holder never moves a belief backwards. The sweep went red on
///   "cluster converged after chaos" without the re-probe and green with it.
/// - **Still outside after the re-probe** (#278), two ways. A node that
///   promised an ordinary campaign learned its `C_b` bound to the
///   campaign's ballot, and "strictly newer" compared the reconfiguration
///   fact naming it against that ballot, so an older fact never won; and
///   with fixed per-pair latencies the one holder of a minority-registered
///   rotation answered after the quorum on every re-probe, and the answer
///   was dropped. So a probe compares a reconfiguration against what this
///   node knows to be in force, never a campaign it only promised: the fact
///   its belief matches (kept while the wire confirms the same
///   configuration) or a leadership it knows won, whichever is higher; and
///   a probe that closed with its node outside still folds its late
///   answers. Two hunt seeds went red on "every node
///   converges to the cluster's chosen prefix" without both and green with
///   them.
///
/// Why a quorum of effective configurations suffices: a reconfiguration is
/// honored once its registration landed at a matchmaker quorum, which raised
/// the durable effective scalar at every matchmaker of that quorum; any
/// quorum of answers intersects it, so the maximum effective configuration
/// they report is at least that one. A reconfiguration registered at a
/// minority may be missed, exactly as a campaign may miss it, or adopted
/// from its one holder; a later wire message, campaign or re-probe corrects
/// the belief then.
///
/// Why a probe and not a campaign: a campaign registers its belief, and a
/// registered belief is a configuration every later campaign's `H_b` must
/// cover with a Phase-1 quorum until GC collects it. A rebooted node's
/// belief is the bootstrap, whose members the floor may have long since
/// released and the operator retired; registering it would ask every later
/// leader for promises nobody can give. A probe leaves no record, so a node
/// only ever registers a belief it heard — on a cluster that never
/// reconfigured, the probe's empty answer is what it heard. The cost is one
/// matchmaker round trip before an incarnation's first campaign.
#[derive(Clone, Debug)]
pub struct MembershipProbe {
    /// The ballot naming this probe's requests and answers. Never promised,
    /// never registered: only a tag the wiring keeps campaigns above.
    ballot: Ballot,
    /// The configuration the prober believed when it opened.
    believed: AcceptorConfig,
    /// Matchmakers whose answer has been folded.
    answered: BTreeSet<MatchmakerId>,
    /// The highest-ballot effective configuration any answer reported.
    effective: Option<(Ballot, AcceptorConfig)>,
}

impl MembershipProbe {
    /// Open a probe tagged `ballot`, from a node that believes `believed`.
    #[must_use]
    pub fn new(ballot: Ballot, believed: AcceptorConfig) -> Self {
        Self {
            ballot,
            believed,
            answered: BTreeSet::new(),
            effective: None,
        }
    }

    /// The tag of this probe's requests.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The configuration the prober believed when it opened.
    #[must_use]
    pub fn believed(&self) -> &AcceptorConfig {
        &self.believed
    }

    /// Fold one matchmaker's answer. Returns whether it counted: a second
    /// answer from the same matchmaker is ignored whole (wire input).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn fold(
        &mut self,
        matchmaker: MatchmakerId,
        effective: Option<(Ballot, AcceptorConfig)>,
    ) -> bool {
        if !self.answered.insert(matchmaker) {
            return false;
        }
        let held = self.effective.as_ref().map(|(b, _)| *b);
        if let Some((ballot, config)) = effective {
            crate::matchmaker::raise_effective(&mut self.effective, ballot, &config);
        }
        assert!(
            self.effective.as_ref().map(|(b, _)| *b) >= held,
            "the probed effective configuration only moves forward"
        );
        true
    }

    /// Whether a matchmaker quorum has answered — asked at the membership
    /// boundary, never as a count.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn quorum_held(&self, matchmakers: &MatchmakerSet) -> bool {
        let held = matchmakers.has_quorum(&self.answered);
        if held {
            assert!(!self.answered.is_empty(), "an answered quorum is someone");
        }
        held
    }

    /// The matchmakers that have not answered — whom a re-send addresses.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn unanswered(&self, matchmakers: &MatchmakerSet) -> Vec<MatchmakerId> {
        let unanswered: Vec<MatchmakerId> = matchmakers.unanswered(&self.answered).collect();
        assert!(
            unanswered.iter().all(|m| !self.answered.contains(m)),
            "an answered matchmaker is never re-asked"
        );
        unanswered
    }

    /// The highest-ballot effective configuration the answers named, `None`
    /// when none named one (the bootstrap is then the only configuration
    /// ever in force, as far as a quorum knows).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn effective(&self) -> Option<&(Ballot, AcceptorConfig)> {
        // Only an answer names an effective configuration.
        if self.effective.is_some() {
            assert!(
                !self.answered.is_empty(),
                "a probed configuration was reported"
            );
        }
        self.effective.as_ref()
    }
}
