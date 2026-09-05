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
    /// Decode one matchmaker's answer: the page it registered, or its
    /// refusal.
    ///
    /// # Errors
    ///
    /// The refusal, when the matchmaker refused.
    pub fn from_outcome(outcome: MatchOutcome) -> Result<Self, MatchRefusal> {
        match outcome {
            MatchOutcome::Registered {
                from_ballot,
                history,
                next_from_ballot,
                gc_watermark,
                effective,
            } => Ok(Self {
                from_ballot,
                history,
                next_from_ballot,
                gc_watermark,
                effective,
            }),
            MatchOutcome::Refused(refusal) => Err(refusal),
        }
    }

    /// Decode a whole reply: the answering matchmaker, and its page or
    /// refusal. The caller checks the reply's addressee, ballot and
    /// generation first — those are its guards, not the phase's.
    ///
    /// # Errors
    ///
    /// The refusal, when the matchmaker refused.
    pub fn from_reply(reply: MatchReply) -> (MatchmakerId, Result<Self, MatchRefusal>) {
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
    #[must_use]
    pub fn new(ballot: Ballot, config: AcceptorConfig, kind: RegistrationKind) -> Self {
        Self {
            ballot,
            config,
            kind,
            registered_by: BTreeSet::new(),
            page_next: BTreeMap::new(),
            history: BTreeMap::new(),
            effective: None,
            watermark: Ballot::zero(),
            disagreements: 0,
        }
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
    #[must_use]
    pub fn watermark(&self) -> Ballot {
        self.watermark
    }

    /// The highest-ballot reconfiguration registration any reply named, with
    /// the ballot it was registered under.
    #[must_use]
    pub fn effective(&self) -> Option<&(Ballot, AcceptorConfig)> {
        self.effective.as_ref()
    }

    /// Distinct ballots two matchmakers reported with different
    /// configurations. Observability only: the union keeps both.
    #[must_use]
    pub fn disagreements(&self) -> u64 {
        self.disagreements
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
        // cursor that page named.
        if self
            .page_next
            .get(&matchmaker)
            .is_some_and(|expected| *expected != page.from_ballot)
        {
            return false;
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
    pub fn fold(&mut self, matchmaker: MatchmakerId, page: RegisteredPage) -> MatchFold {
        if !self.accepts(matchmaker, &page) {
            return MatchFold::Ignored;
        }
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
        if let Some(next) = next_from_ballot {
            self.page_next.insert(matchmaker, next);
            MatchFold::Paged(next)
        } else {
            self.page_next.remove(&matchmaker);
            self.registered_by.insert(matchmaker);
            MatchFold::Registered
        }
    }

    /// Raise the effective configuration to `(ballot, config)` when it is
    /// newer than the one held (monotone in the ballot).
    fn raise_effective(&mut self, ballot: Ballot, config: &AcceptorConfig) {
        if self
            .effective
            .as_ref()
            .is_none_or(|(newest, _)| ballot > *newest)
        {
            self.effective = Some((ballot, config.clone()));
        }
    }

    /// Whether a matchmaker quorum of `matchmakers` has answered completely
    /// — the phase's own completion predicate, asked at the membership
    /// boundary and never as a count.
    #[must_use]
    pub fn quorum_held(&self, matchmakers: &MatchmakerSet) -> bool {
        matchmakers.has_quorum(&self.registered_by)
    }

    /// How many more complete answers the phase still waits for. The one
    /// thing a predicate cannot report, and the only place a matchmaker
    /// quorum is ever spelled as a number.
    ///
    /// # Panics
    ///
    /// If `matchmakers` is not well formed.
    #[must_use]
    pub fn remaining(&self, matchmakers: &MatchmakerSet) -> usize {
        matchmakers
            .quorum_size()
            .saturating_sub(self.registered_by.len())
    }

    /// The matchmakers that have not answered completely, with the page
    /// cursor each owes next — whom a re-send addresses, and from where.
    #[must_use]
    pub fn unanswered(&self, matchmakers: &MatchmakerSet) -> Vec<(MatchmakerId, Option<Ballot>)> {
        matchmakers
            .members()
            .iter()
            .copied()
            .filter(|mm| !self.registered_by.contains(mm))
            .map(|mm| (mm, self.page_next.get(&mm).copied()))
            .collect()
    }

    /// How many matchmakers have answered completely.
    #[must_use]
    pub fn registered(&self) -> usize {
        self.registered_by.len()
    }

    /// `H_b`: every distinct configuration reported at a ballot at or above
    /// the maximum watermark, in ballot order.
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
        prior
    }

    /// The stale-belief signal: the effective configuration the quorum's
    /// histories name, when this is an ordinary campaign that registered
    /// something else. A reconfiguration campaign is exempt — it *is* the
    /// next effective configuration. `None` when no reconfiguration was ever
    /// registered below this ballot, or the belief already matches it.
    #[must_use]
    pub fn stale_belief(&self) -> Option<(Ballot, AcceptorConfig)> {
        if self.kind.is_reconfiguration() {
            return None;
        }
        let (newest, config) = self.effective.as_ref()?;
        if *config == self.config {
            return None;
        }
        Some((*newest, config.clone()))
    }
}
