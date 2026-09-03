//! The leader-side **matchmaking phase** (Matchmaker Paxos §3.1–§3.2, §4.2):
//! the round trip a campaign runs *before* Phase 1 on a deployment that names
//! matchmakers.
//!
//! A candidate's round no longer begins with `Prepare`. It picks the acceptor
//! configuration it intends to run its ballot with (`C_b`), registers
//! `(b, C_b)` with a **quorum of matchmakers**, and collects the histories they
//! return — every configuration each of them holds at a ballot below `b`. The
//! **union** of those histories, filtered by the **maximum** reported GC
//! watermark, is `H_b`: the set of prior configurations whose Phase-1 quorums
//! the candidate must each obtain before it may propose anything
//! ([`super::election`]). That is the whole safety argument of §3.3: the set
//! came from an intersecting matchmaker quorum, so no configuration that could
//! still hold a chosen-but-unlearned value is missing from it.
//!
//! The phase is deliberately its own state ([`Matchmaking`]) beside
//! [`super::election::Election`], never folded into it: a reader must be able
//! to point at the matchmaking state, the Phase-1 state, and the boundary
//! between them ([`super::RawNode::start_phase1`]). A candidate holds exactly
//! one of the two, and `RawNode::assert_invariants` says so.
//!
//! # Plain Multi-Paxos never comes here
//!
//! A deployment with no matchmakers ([`crate::Config::matchmakers`] empty)
//! skips this module entirely: `on_check_leader` goes straight from the ballot
//! bump to `Prepare` against its one static configuration, `H_b` is
//! implicitly `{C}`, and no matchmaker message or extra round trip exists
//! (AGENTS.md, *Plain Multi-Paxos is first-class*). Only a configuration that
//! names matchmakers ever constructs a [`Matchmaking`].
//!
//! # The effective configuration is a registration fact, not a chosen value
//!
//! This is the one place the protocol's notion of "the configuration in
//! force" is decided, and it is **not** a value chosen by Paxos. A
//! configuration becomes authoritative the moment a leader's
//! *reconfiguration* registration ([`crate::Registration::reconfiguration`])
//! has landed at a **matchmaker quorum** — before, and independently of,
//! any Phase 1 or Phase 2 the new acceptor set ever completes. From then on,
//! quorum intersection puts that record in every later campaign's histories,
//! and the effective configuration every ordinary campaign must register is
//! the **highest-ballot reconfiguration registration** those histories name
//! (`Matchmaking::effective`, `Matchmaking::stale_belief`). Three
//! consequences a reader must not miss:
//!
//! - **Durability is the matchmaker quorum's**, not the acceptors'. A
//!   `Reconfigure` acked `accepted: true` has *started*; it is guaranteed to
//!   be honored once its matchmaking completed at a quorum, and may be lost
//!   before that like any proposal short of a quorum.
//! - **Two reconfigurations may overlap.** With `R1 -> C1` and `R2 -> C2`
//!   both registered, `R1` may still finish its Phase 1 and lead briefly
//!   under `C1` while `C2` is already the highest registration; `R2`'s
//!   Phase 1 covers `C1` (it is in `H_b`), so nothing chosen under `C1` is
//!   lost, and every later ordinary campaign registers `C2`. Paxos safety is
//!   the acceptors' quorums; configuration monotonicity is the matchmakers'.
//! - **Beliefs never count.** An ordinary campaign's registration is what
//!   the candidate believed, possibly stale, possibly abandoned; only the
//!   flagged records decide, which is what keeps two stale candidates from
//!   re-adopting each other's beliefs forever.
//!
//! GC (#123) collects the flagged *record* like any other: the floor is a
//! leader's own ballot and rises over it as soon as an ordinary leader
//! campaigns above it. What it must never forget is the **fact**, which is
//! why every matchmaker also holds the effective configuration as a durable
//! monotone scalar the watermark does not touch
//! ([`crate::MatchmakerHardState::effective`]) and reports it beside every
//! history. `Matchmaking::fold` takes the maximum of the two, so a campaign
//! whose replies show an empty history still learns the configuration in
//! force (review finding P1: without the scalar, an ordinary leader's GC
//! erased it and a rebooted candidate was elected under the superseded
//! configuration).
//!
//! # Where the promise sits
//!
//! The candidate promises its own ballot **before** matchmaking, exactly where
//! the plain path promises it. Promising first is always safe — a promise only
//! refuses lower ballots — and it keeps the durable-write shape of a campaign
//! identical in both deployments; a refused registration then simply leaves a
//! follower with a slightly higher promise, which is the same state a lost
//! election leaves.
//!
//! # Refusals, staleness, and re-sends
//!
//! - A **refusal** from any matchmaker (`Stale`, or `BelowWatermark`) aborts
//!   the campaign: the candidate becomes a follower and re-campaigns, at a
//!   strictly higher round, when its randomized election timeout next fires —
//!   the same shape as a `Nack` on a `Prepare`, and the same dueling-proposer
//!   livelock fix. The refusal's payload (`highest`, the watermark) is a
//!   diagnostic, never adopted as a campaign hint, exactly like
//!   [`crate::Message::Nack`]'s `promised`.
//! - A **stale configuration**: an ordinary campaign registers the
//!   configuration this node believes is in force, and a node that was down
//!   or partitioned through a reconfiguration believes an old one. The ledger
//!   distinguishes a **reconfiguration** registration (a leader's explicit
//!   change, [`crate::Registration::reconfiguration`]) from a candidate's
//!   belief, and the highest-ballot reconfiguration the quorum's histories
//!   name is the **effective configuration**. When it differs from the one an
//!   ordinary campaign just registered, the candidate abandons the campaign,
//!   adopts it, and re-campaigns — so a leader change can never quietly
//!   reinstate a superseded configuration. Beliefs never trigger this:
//!   "adopt the newest *registration*" made two candidates re-adopt each
//!   other's abandoned beliefs and flip-flop forever, while reconfiguration
//!   requests are monotone by ballot and never manufactured by a campaign.
//!   Its abandoned registration stays in the registry and is honored by every
//!   later leader's Phase 1 (nothing was ever accepted at it, but the registry
//!   cannot know that); GC retires it later (#123), the flagged one
//!   included — the effective configuration outlives its record as the
//!   durable scalar every `Registered` reply reports
//!   ([`crate::MatchmakerHardState::effective`]).
//! - **Lost replies** are the driver's business: [`super::RawNode::resend_matchmaking`]
//!   re-queues the request for every matchmaker that has not answered, and
//!   skipping the call is always safe — the matchmaker answers a repeated
//!   request idempotently from its retained history, and a campaign that never
//!   completes is simply abandoned at the next election timeout.

use super::{BTreeMap, BTreeSet, Ballot, NodeId, NodeRole, RawNode};
use crate::matchmaker::{
    MatchOutcome, MatchRefusal, MatchReply, MatchRequest, REGISTRY_PAGE, Registration,
    RegistrationKind,
};
use crate::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet};

/// Volatile per-ballot matchmaking state while a Candidate registers its
/// configuration and collects the prior ones.
pub(super) struct Matchmaking {
    /// The ballot being registered.
    pub(super) ballot: Ballot,
    /// `C_b`: the configuration this ballot will run with once registered.
    pub(super) config: AcceptorConfig,
    /// What this campaign registers: an operator's deliberate change
    /// ([`super::RawNode::reconfigure`]) or this node's belief about the
    /// configuration in force (the election clock). Only a
    /// [`RegistrationKind::Belief`] campaign is subject to the
    /// stale-configuration abort.
    pub(super) kind: RegistrationKind,
    /// Matchmakers whose **complete** answer has been folded — a paged one
    /// counts only once its last page arrived.
    pub(super) registered_by: BTreeSet<MatchmakerId>,
    /// Next history-page cursor expected from each matchmaker still
    /// mid-answer, exactly as the log's Phase 1 tracks its promise pages.
    pub(super) page_next: BTreeMap<MatchmakerId, Ballot>,
    /// The union of every reported history so far, ballot by ballot. A ballot
    /// normally maps to one configuration (one proposer per ballot, write-once
    /// per matchmaker); two matchmakers disagreeing would be a registry bug,
    /// and rather than assert on wire input the union keeps *both* — Phase 1
    /// then needs a quorum of each, which is always safe.
    pub(super) history: BTreeMap<Ballot, Vec<AcceptorConfig>>,
    /// The highest-ballot **reconfiguration** registration any reply named:
    /// the effective configuration below this ballot (see
    /// [`crate::Registration`]). `None` when no reply named one — the
    /// bootstrap configuration is then the only one ever in force.
    pub(super) effective: Option<(Ballot, AcceptorConfig)>,
    /// The **maximum** reported GC watermark (§3.2's `w = max(w, ...)`):
    /// entries below it are excluded from `H_b`.
    pub(super) watermark: Ballot,
    /// Distinct disagreements seen while unioning (two configurations at one
    /// ballot) — observability for the driver's audit report.
    pub(super) disagreements: u64,
}

/// What one matchmaker reply did to an open campaign, returned by
/// [`super::RawNode::on_match_reply`] so the driver can report the transition
/// it caused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchStep {
    /// Not for this campaign (no campaign open, a different ballot, a
    /// duplicate answer, or addressed to another node): nothing changed.
    Ignored,
    /// One more matchmaker registered the ballot; `remaining` more are needed
    /// for the matchmaker quorum.
    Registered {
        /// Registrations still missing before the quorum holds.
        remaining: usize,
    },
    /// A matchmaker's answer is paged and this was not its last page: the
    /// registration does not count yet, and the candidate has queued the
    /// request for the page starting at `next`.
    Paged {
        /// The cursor the next page starts at.
        next: Ballot,
    },
    /// The matchmaker quorum holds and Phase 1 has started: `prior` is `H_b`
    /// (the distinct prior configurations, in ballot order) and `watermark`
    /// the maximum GC watermark it was filtered by.
    Completed {
        /// `H_b` — every distinct prior configuration Phase 1 must cover.
        prior: Vec<AcceptorConfig>,
        /// The maximum watermark any replying matchmaker reported.
        watermark: Ballot,
        /// How many matchmakers answered before the quorum closed.
        registered_by: usize,
    },
    /// A matchmaker refused the registration: the campaign is abandoned and
    /// this node is a follower again.
    Refused(MatchRefusal),
    /// The matchmaker set this campaign addressed has a chosen successor
    /// (#125): the campaign is abandoned, the successor adopted as this
    /// node's matchmaker set, and the next campaign asks it.
    Superseded {
        /// The adopted set.
        set: MatchmakerSet,
    },
    /// The quorum's histories named a reconfiguration to a configuration
    /// other than the one this ordinary campaign registered: the belief was
    /// stale. The campaign is abandoned, the effective configuration adopted
    /// as this node's belief, and the next campaign registers it.
    StaleConfiguration {
        /// The ballot the effective configuration was registered under.
        newest: Ballot,
    },
}

impl Matchmaking {
    /// Open the phase for `ballot` with `config` as `C_b`.
    pub(super) fn new(ballot: Ballot, config: AcceptorConfig, kind: RegistrationKind) -> Self {
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

    /// Whether this page counts at all: a matchmaker not already done, and
    /// a page whose shape and cursor are what that matchmaker owes next.
    /// Wire input, so a refusal is a `false`, never an assert — the twin of
    /// the log's own `PromiseTally::accepts`.
    fn accepts(
        &self,
        matchmaker: MatchmakerId,
        from_ballot: Ballot,
        history: &BTreeMap<Ballot, Registration>,
        next_from_ballot: Option<Ballot>,
    ) -> bool {
        if self.registered_by.contains(&matchmaker) {
            return false;
        }
        // The first page starts wherever the matchmaker's own watermark is,
        // which the candidate cannot know; every later one must start at the
        // cursor that page named.
        if self
            .page_next
            .get(&matchmaker)
            .is_some_and(|expected| *expected != from_ballot)
        {
            return false;
        }
        // Only the lower bound, exactly as `promise_page_shape_valid` checks
        // its page: an entry above the request's ballot would merely add a
        // configuration to `H_b`, which Phase 1 covering more than it must
        // is always safe.
        history.len() <= REGISTRY_PAGE
            && history.keys().all(|b| *b >= from_ballot)
            && next_from_ballot.is_none_or(|next| {
                history.len() == REGISTRY_PAGE
                    && next > from_ballot
                    && history.keys().next_back().is_none_or(|last| next > *last)
            })
    }

    /// Fold one `Registered` page's history, watermark and reported
    /// effective configuration. Returns the cursor the next page starts at
    /// when the answer is still incomplete, `Ok(None)` when this page
    /// completed it, and `Err(())` when the page does not count (a
    /// duplicate, or one whose cursor or shape is not what this matchmaker
    /// owed).
    pub(super) fn fold(
        &mut self,
        matchmaker: MatchmakerId,
        from_ballot: Ballot,
        history: BTreeMap<Ballot, Registration>,
        next_from_ballot: Option<Ballot>,
        watermark: Ballot,
        reported: Option<(Ballot, AcceptorConfig)>,
    ) -> Result<Option<Ballot>, ()> {
        if !self.accepts(matchmaker, from_ballot, &history, next_from_ballot) {
            return Err(());
        }
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
        if let Some((ballot, config)) = reported {
            self.raise_effective(ballot, &config);
        }
        // The watermark is the maximum reported, never the minimum and never
        // a per-reply filter (§3.2): the union is filtered once, at closure.
        self.watermark = self.watermark.max(watermark);
        if let Some(next) = next_from_ballot {
            self.page_next.insert(matchmaker, next);
        } else {
            self.page_next.remove(&matchmaker);
            self.registered_by.insert(matchmaker);
        }
        Ok(next_from_ballot)
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

    /// `H_b`: every distinct configuration reported at a ballot at or above
    /// the maximum watermark, in ballot order.
    pub(super) fn prior(&self) -> Vec<AcceptorConfig> {
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
    pub(super) fn stale_belief(&self) -> Option<(Ballot, AcceptorConfig)> {
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

/// What one matchmaker answered: the history, watermark and effective
/// configuration of a registration, or the refusal.
pub(super) type MatchAnswer = Result<RegisteredPage, MatchRefusal>;

/// One `MatchB` page as the campaign folds it.
pub(super) struct RegisteredPage {
    pub(super) from_ballot: Ballot,
    pub(super) history: BTreeMap<Ballot, Registration>,
    pub(super) next_from_ballot: Option<Ballot>,
    pub(super) gc_watermark: Ballot,
    pub(super) effective: Option<(Ballot, AcceptorConfig)>,
}

/// Decode one reply into the answer the campaign folds.
pub(super) fn split_reply(reply: MatchReply) -> (MatchmakerId, NodeId, Ballot, MatchAnswer) {
    let MatchReply {
        matchmaker,
        to,
        ballot,
        outcome,
        generation: _,
    } = reply;
    let answer = match outcome {
        MatchOutcome::Registered {
            from_ballot,
            history,
            next_from_ballot,
            gc_watermark,
            effective,
        } => Ok(RegisteredPage {
            from_ballot,
            history,
            next_from_ballot,
            gc_watermark,
            effective,
        }),
        MatchOutcome::Refused(refusal) => Err(refusal),
    };
    (matchmaker, to, ballot, answer)
}

impl RawNode {
    /// Re-queue the open matchmaking request toward every matchmaker that has
    /// not answered yet. A no-op on a node with no open matchmaking phase.
    ///
    /// **The driver is expected to call this on a steady cadence** while
    /// [`RawNode::matchmaking_pending`] reports an open phase, so a request
    /// or reply the transport lost does not stall the campaign until the
    /// election timeout abandons it.
    ///
    /// **Skipping a call is always safe.** Re-sending is pure optimization,
    /// exactly like [`RawNode::resend_pending`]: the matchmaker answers a
    /// repeated request idempotently from its retained history (it registers
    /// nothing twice), and a campaign that never completes its matchmaking is
    /// simply abandoned at the next election timeout and retried at a higher
    /// round. The deterministic simulation skips calls to reach exactly those
    /// abandoned campaigns; production never skips.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn resend_matchmaking(&mut self) {
        let Some(m) = self.matchmaking.as_ref() else {
            return;
        };
        let generation = self.matchmakers.generation;
        let request = match m.kind {
            RegistrationKind::Reconfiguration => {
                MatchRequest::reconfigure(self.config.id, m.ballot, m.config.clone(), generation)
            }
            RegistrationKind::Belief => {
                MatchRequest::new(self.config.id, m.ballot, m.config.clone(), generation)
            }
        };
        let unanswered: Vec<(MatchmakerId, Option<Ballot>)> = self
            .matchmakers
            .members
            .iter()
            .copied()
            .filter(|mm| !m.registered_by.contains(mm))
            .map(|mm| (mm, m.page_next.get(&mm).copied()))
            .collect();
        for (matchmaker, cursor) in unanswered {
            // A matchmaker mid-answer is re-asked from where its last page
            // stopped, never from the start: the pages already folded stay
            // folded, and an answer restarted at the watermark would be
            // refused by `Matchmaking::accepts` anyway.
            let request = match cursor {
                Some(from) => request.clone().from_page(from),
                None => request.clone(),
            };
            self.pending_match_requests.push((matchmaker, request));
        }
        self.assert_invariants();
    }

    /// Whether a matchmaking phase is open — the driver's cue to pace
    /// [`RawNode::resend_matchmaking`], consulted only where a re-send can
    /// have an effect.
    #[must_use]
    pub fn matchmaking_pending(&self) -> bool {
        self.matchmaking.is_some()
    }

    /// The open matchmaking phase, if any: its ballot, the configuration it
    /// registers, and what kind of registration that is. A read view for the
    /// driver's audit report.
    #[must_use]
    pub fn matchmaking(&self) -> Option<(Ballot, &AcceptorConfig, RegistrationKind)> {
        self.matchmaking
            .as_ref()
            .map(|m| (m.ballot, &m.config, m.kind))
    }

    /// Fold one matchmaker's answer into the open matchmaking phase — the
    /// leader-side half of the matchmaker contract (#120). A reply for another
    /// ballot, another node, another generation, or a matchmaker that already
    /// answered (or is outside the believed set) is ignored whole (wire
    /// input, never asserted). Returns what the reply did, so the driver can
    /// report the transition it caused.
    ///
    /// - `Registered`: the history is unioned and the watermark maxed; once a
    ///   **quorum of matchmakers** has registered the ballot, `H_b` is
    ///   computed (the union, filtered by the maximum watermark) and handed to
    ///   Phase 1 through [`RawNode::start_phase1`] — no `Prepare` ever leaves
    ///   before that instant (invariant 1). An ordinary campaign whose
    ///   histories name a **reconfiguration** to a configuration other than
    ///   the one it registered abandons the campaign and adopts that
    ///   configuration instead — `StaleConfiguration`, the rule that keeps a
    ///   superseded configuration from being reinstated by a candidate that
    ///   missed the change (see [`crate::Registration`]).
    /// - `Refused`: the campaign is abandoned and this node steps back to
    ///   follower; a refusal's ballot is diagnostic only (a `Stale` or
    ///   `BelowWatermark` refusal raises the round floor the next campaign
    ///   opens above). A refusal naming a **chosen successor set** (#125:
    ///   `Stopped { successor }`, or `Generation` from a later generation) is
    ///   adopted through [`RawNode::learn_matchmakers`] and reported as
    ///   `Superseded`; the next campaign asks the new set. A refused
    ///   registration never becomes a leadership (invariant 4).
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, matchmaker = reply.matchmaker.0, round = reply.ballot.round)))]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, matchmaker = reply.matchmaker.0, round = reply.ballot.round)))]
    pub fn on_match_reply(&mut self, reply: MatchReply) -> MatchStep {
        let generation = reply.generation;
        let (matchmaker, to, ballot, answer) = split_reply(reply);
        if to != self.config.id || !self.matchmakers.contains(matchmaker) {
            return MatchStep::Ignored;
        }
        if generation != self.matchmakers.generation
            || self.matchmaking.as_ref().is_none_or(|m| m.ballot != ballot)
        {
            return MatchStep::Ignored;
        }
        let step = match answer {
            Ok(page) => self.fold_registration(matchmaker, page),
            Err(refusal) => self.fold_refusal(refusal),
        };
        // Post-step restatements of invariants 1 and 4: a refused campaign
        // left nothing Phase-1-shaped behind, and a completed one
        // closed the matchmaking phase before opening Phase 1.
        match &step {
            MatchStep::Refused(_)
            | MatchStep::StaleConfiguration { .. }
            | MatchStep::Superseded { .. } => {
                assert!(
                    self.role == NodeRole::Follower,
                    "a refused registration never becomes a leadership"
                );
                assert!(
                    self.proposer.election().is_none() && self.matchmaking.is_none(),
                    "an abandoned campaign leaves no phase open"
                );
            }
            MatchStep::Completed { .. } => {
                assert!(
                    self.matchmaking.is_none(),
                    "a completed matchmaking phase is closed"
                );
            }
            MatchStep::Registered { .. } | MatchStep::Paged { .. } | MatchStep::Ignored => {}
        }
        self.assert_invariants();
        step
    }

    /// The `Registered` half of [`RawNode::on_match_reply`]: union this
    /// matchmaker's history, and once a quorum has registered the ballot,
    /// either adopt an effective configuration this campaign is stale against
    /// or hand `H_b` to Phase 1.
    ///
    /// # Panics
    ///
    /// If Phase 1 would open without the quorum, or on a prior configuration
    /// naming a node outside the pool.
    fn fold_registration(&mut self, matchmaker: MatchmakerId, page: RegisteredPage) -> MatchStep {
        let matchmakers = &self.matchmakers;
        let Some(m) = self.matchmaking.as_mut() else {
            return MatchStep::Ignored;
        };
        let RegisteredPage {
            from_ballot,
            history,
            next_from_ballot,
            gc_watermark,
            effective,
        } = page;
        let Ok(next) = m.fold(
            matchmaker,
            from_ballot,
            history,
            next_from_ballot,
            gc_watermark,
            effective,
        ) else {
            return MatchStep::Ignored;
        };
        if let Some(next) = next {
            // The answer is still paged: ask this matchmaker for the rest.
            // Nothing counts toward the quorum until its last page lands.
            let ballot = m.ballot;
            let config = m.config.clone();
            let kind = m.kind;
            let generation = self.matchmakers.generation;
            let request = match kind {
                RegistrationKind::Reconfiguration => {
                    MatchRequest::reconfigure(self.config.id, ballot, config, generation)
                }
                RegistrationKind::Belief => {
                    MatchRequest::new(self.config.id, ballot, config, generation)
                }
            };
            self.pending_match_requests
                .push((matchmaker, request.from_page(next)));
            return MatchStep::Paged { next };
        }
        let m = self.matchmaking.as_mut().expect("the phase is still open");
        let registered = m.registered_by.len();
        let quorum_held = matchmakers.has_quorum(&m.registered_by);
        if !quorum_held {
            MatchStep::Registered {
                remaining: self.matchmakers.quorum_size().saturating_sub(registered),
            }
        } else if let Some((newest, config)) = m.stale_belief() {
            // Stale belief: the quorum's histories name a
            // reconfiguration to a configuration other than the
            // one this ordinary campaign registered. Adopt the
            // effective configuration and abandon the campaign;
            // the next one registers it. Only *reconfiguration*
            // registrations count as facts here: the ledger also
            // records every candidate's belief, and "adopt the
            // newest registration" made two candidates re-adopt
            // each other's abandoned beliefs and flip-flop one
            // round per election timeout (seed
            // 7519660681720567139: 182 aborts, no leader for a
            // 50 s tail). A reconfiguration request is monotone
            // by ballot and never manufactured by a campaign, so
            // adopting the highest one cannot flip-flop — and
            // without it a candidate that missed a completed
            // reconfiguration could be elected under the
            // superseded configuration, rolling the cluster back
            // without anyone asking (review of #132).
            // The adoption binds this node to the reconfiguration's own
            // ballot, which may be *older* than the one it holds: a
            // reconfiguration campaign at 8 can complete its registration
            // after an ordinary leader at 10 was elected, and it is honored
            // by every later campaign regardless (intersection hands its
            // record to them). `acceptors_since` is therefore not monotone
            // here, unlike `learn_config`, and the membership fence keeps
            // the higher ballot it already recorded.
            self.acceptors = config;
            self.acceptors_since = newest;
            self.record_membership();
            self.become_follower(None);
            MatchStep::StaleConfiguration { newest }
        } else {
            // The history is `H_b`, the prior set Phase 1 must
            // cover. A belief that matches the effective
            // configuration (or predates any reconfiguration)
            // runs the leadership under what it registered.
            let prior = m.prior();
            let watermark = m.watermark;
            let config = m.config.clone();
            // The matchmaking → Phase 1 boundary. The registered
            // quorum is restated here, at the one place Phase 1
            // can open on a matchmaker deployment.
            assert!(
                quorum_held,
                "Phase 1 opens only once a matchmaker quorum registered the ballot"
            );
            assert!(
                prior
                    .iter()
                    .all(|c| c.members().iter().all(|n| self.in_pool(*n))),
                "every prior configuration is drawn from the node pool"
            );
            self.matchmaking = None;
            self.start_phase1(config, prior.clone());
            MatchStep::Completed {
                prior,
                watermark,
                registered_by: registered,
            }
        }
    }

    /// The refusal half of [`RawNode::on_match_reply`]: raise the round floor
    /// the next campaign opens above, adopt a chosen successor set when the
    /// refusal names one, and abandon the campaign either way.
    fn fold_refusal(&mut self, refusal: MatchRefusal) -> MatchStep {
        match &refusal {
            MatchRefusal::Stale { highest } => {
                // The next campaign opens above the round that
                // refused this one (see `round_floor`).
                self.round_floor = self.round_floor.max(highest.round);
            }
            MatchRefusal::BelowWatermark { watermark } => {
                // A collected round is never campaigned again: the
                // next one opens above the floor (#123 — a
                // partitioned leader that outlived a GC recovers by
                // campaigning higher).
                self.round_floor = self.round_floor.max(watermark.round);
            }
            MatchRefusal::Stopped { .. }
            | MatchRefusal::Generation { .. }
            | MatchRefusal::Inactive
            | MatchRefusal::Malformed => {}
        }
        let successor = match &refusal {
            MatchRefusal::Stopped {
                successor: Some(set),
            } => Some(set.clone()),
            MatchRefusal::Generation { current }
                if current.generation > self.matchmakers.generation =>
            {
                Some(current.clone())
            }
            _ => None,
        };
        self.become_follower(None);
        match successor {
            Some(set) if self.learn_matchmakers(&set) => MatchStep::Superseded { set },
            _ => MatchStep::Refused(refusal),
        }
    }
    /// How many matchmaker disagreements (two configurations reported at one
    /// ballot) the open matchmaking phase has unioned so far — 0 when none is
    /// open. Observability only: the union keeps both, so safety never
    /// depends on the count.
    #[must_use]
    pub fn matchmaking_disagreements(&self) -> u64 {
        self.matchmaking.as_ref().map_or(0, |m| m.disagreements)
    }

    /// The matchmaker set this node believes authoritative (#125): the
    /// bootstrap set at generation 0 until a later one is learned. Empty on
    /// plain Multi-Paxos.
    #[must_use]
    pub fn matchmaker_set(&self) -> &MatchmakerSet {
        &self.matchmakers
    }

    /// Adopt `set` as the authoritative matchmaker set if it is a strictly
    /// later generation than the one believed (#125): a refusal naming a
    /// successor, a reconfiguration this node's driver completed, or a reply
    /// from a later generation. An open matchmaking against the superseded
    /// set is abandoned (the next election timeout re-campaigns against the
    /// new one), a pending GC tally starts over, and a plain deployment never
    /// moves (it has no generation to move). Returns whether the belief
    /// moved.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, generation = set.generation.0)))]
    pub fn learn_matchmakers(&mut self, set: &MatchmakerSet) -> bool {
        if !self.config.has_matchmakers()
            || set.generation <= self.matchmakers.generation
            || set.members.is_empty()
        {
            return false;
        }
        // Wire hygiene: a set naming a matchmaker outside the pool is not one
        // this deployment can reach; ignore it whole.
        if !set
            .members
            .iter()
            .all(|m| self.config.matchmaker_pool().binary_search(m).is_ok())
        {
            return false;
        }
        self.matchmakers = MatchmakerSet::new(set.generation, set.members.clone());
        if self.matchmaking.is_some() {
            // The registrations collected so far were for a replaced
            // generation; a stopped quorum will never complete them.
            self.become_follower(None);
        }
        self.reset_gc_for_generation();
        self.assert_invariants();
        true
    }
}
