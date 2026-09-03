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
//!   cannot know that); GC retires it later (#123) — and must never retire
//!   the highest reconfiguration registration, the effective configuration's
//!   only durable record.
//! - **Lost replies** are the driver's business: [`super::RawNode::resend_matchmaking`]
//!   re-queues the request for every matchmaker that has not answered, and
//!   skipping the call is always safe — the matchmaker answers a repeated
//!   request idempotently from its retained history, and a campaign that never
//!   completes is simply abandoned at the next election timeout.

use super::{BTreeMap, BTreeSet, Ballot, NodeId};
use crate::matchmaker::{
    AcceptorConfig, MatchOutcome, MatchRefusal, MatchReply, MatchmakerId, Registration,
};

/// Volatile per-ballot matchmaking state while a Candidate registers its
/// configuration and collects the prior ones.
pub(super) struct Matchmaking {
    /// The ballot being registered.
    pub(super) ballot: Ballot,
    /// `C_b`: the configuration this ballot will run with once registered.
    pub(super) config: AcceptorConfig,
    /// Whether this campaign was opened by
    /// [`super::RawNode::reconfigure`] (the configuration is a deliberate
    /// change) rather than by the election clock (the configuration is this
    /// node's belief about the latest one). Only an ordinary campaign is
    /// subject to the stale-configuration abort.
    pub(super) reconfiguration: bool,
    /// Matchmakers whose `Registered` reply has been folded.
    pub(super) registered_by: BTreeSet<MatchmakerId>,
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
    pub(super) fn new(ballot: Ballot, config: AcceptorConfig, reconfiguration: bool) -> Self {
        Self {
            ballot,
            config,
            reconfiguration,
            registered_by: BTreeSet::new(),
            history: BTreeMap::new(),
            effective: None,
            watermark: Ballot::zero(),
            disagreements: 0,
        }
    }

    /// The matchmaker quorum over a set of `matchmakers`: a majority, so any
    /// two registration quorums intersect (§3.3).
    pub(super) fn quorum(matchmakers: usize) -> usize {
        matchmakers / 2 + 1
    }

    /// Fold one `Registered` reply's history and watermark. Returns `false`
    /// when `matchmaker` had already been folded (a duplicate answer).
    pub(super) fn fold(
        &mut self,
        matchmaker: MatchmakerId,
        history: BTreeMap<Ballot, Registration>,
        watermark: Ballot,
    ) -> bool {
        if !self.registered_by.insert(matchmaker) {
            return false;
        }
        for (ballot, registration) in history {
            let Registration {
                config,
                reconfiguration,
            } = registration;
            if reconfiguration
                && self
                    .effective
                    .as_ref()
                    .is_none_or(|(newest, _)| ballot > *newest)
            {
                self.effective = Some((ballot, config.clone()));
            }
            let entry = self.history.entry(ballot).or_default();
            if !entry.contains(&config) {
                if !entry.is_empty() {
                    self.disagreements = self.disagreements.saturating_add(1);
                }
                entry.push(config);
            }
        }
        // The watermark is the maximum reported, never the minimum and never
        // a per-reply filter (§3.2): the union is filtered once, at closure.
        self.watermark = self.watermark.max(watermark);
        true
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
        if self.reconfiguration {
            return None;
        }
        let (newest, config) = self.effective.as_ref()?;
        if *config == self.config {
            return None;
        }
        Some((*newest, config.clone()))
    }
}

/// What one matchmaker answered: the history and watermark of a
/// registration, or the refusal.
pub(super) type MatchAnswer = Result<(BTreeMap<Ballot, Registration>, Ballot), MatchRefusal>;

/// Decode one reply into the answer the campaign folds.
pub(super) fn split_reply(reply: MatchReply) -> (MatchmakerId, NodeId, Ballot, MatchAnswer) {
    let MatchReply {
        matchmaker,
        to,
        ballot,
        outcome,
    } = reply;
    let answer = match outcome {
        MatchOutcome::Registered {
            history,
            gc_watermark,
        } => Ok((history, gc_watermark)),
        MatchOutcome::Refused(refusal) => Err(refusal),
    };
    (matchmaker, to, ballot, answer)
}
