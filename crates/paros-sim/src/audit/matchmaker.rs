//! The matchmaker oracles: the registry side (#119) and the leader-side
//! matchmaking phase (#120), one incremental fold each.
//!
//! Every check is O(1) in the size of the run (a map probe over a registry of
//! a handful of ballots), fed by the driver's typed reports at the instant
//! each fact becomes true: a registration at its fsync, a reply as it leaves,
//! a registry as it boots, a campaign as it opens, folds a reply, closes, or
//! is refused.
//!
//! # The registry (#119), where each invariant is judged
//!
//! 1. **A successful reply never escapes a non-durable registration** — at the
//!    reply (the folded durable registry must already hold the ballot), and
//!    again at every restart (every durable registration at or above the
//!    recovered watermark reads back).
//! 2. **Write-once per ballot** — at every registration and every boot, against
//!    a ledger that is never pruned.
//! 3. **Registration is monotone** — at every registration, against the
//!    registry's highest ballot.
//! 4. **The history is complete below the request** — at the reply: the
//!    history *is* the folded registry's window `[watermark, ballot)`, no
//!    entry missing and none foreign.
//! 5. **The watermark is monotone and durable** — at every raise, every reply
//!    (which must report the durable floor) and every restart.
//!
//! # The matchmaking phase (#120), where each invariant is judged
//!
//! 1. **No `Prepare` before a matchmaker quorum** — at every `Prepare` a node
//!    sends on a deployment with matchmakers: its ballot must be one whose
//!    matchmaking this fold saw close with a quorum
//!    ([`MatchmakerAudit::phase1_licensed`]).
//! 2. **`H_b` is the union, never a sample** — at the close: the reported
//!    prior set equals the union of every history the registering
//!    matchmakers answered with, no configuration dropped because another
//!    matchmaker did not report it.
//! 3. **The watermark used is the maximum reported** — at the close: the
//!    filter applied is the max over the same replies, and every
//!    configuration at or above it survives.
//! 4. **A refused registration never becomes a leadership** — at every
//!    election on a matchmaker deployment: the won ballot's campaign closed
//!    with a quorum and was never refused.
//! 5. **Registration precedes exercise** — at every election: the
//!    configuration the leader runs Phase 2 with is the one some matchmaker
//!    durably registered under that ballot.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes};
use paros::{AcceptorConfig, Ballot, MatchRefusal, MatchmakerId, NodeId, Seam};

/// One matchmaker's folded registry.
#[derive(Default)]
struct Registry {
    /// Durable registrations, exactly as the disk holds them (pruned by GC).
    registered: BTreeMap<Ballot, AcceptorConfig>,
    /// The durable watermark.
    watermark: Ballot,
    /// Ballots a `Registered` reply escaped for, with the history each first
    /// answer carried (a re-answer may only shrink it).
    replied: BTreeMap<Ballot, Vec<(Ballot, AcceptorConfig)>>,
    /// Boots observed (1 after the first).
    boots: u64,
}

/// One candidate's matchmaking phase for one ballot, as the fold sees it.
#[derive(Default)]
struct Campaign {
    /// `C_b`, the configuration the campaign registers.
    config: Option<AcceptorConfig>,
    /// The `Registered` replies the matchmakers sent this candidate for this
    /// ballot: `matchmaker -> (history, watermark)`. What the candidate may
    /// have received.
    replies: BTreeMap<u64, (Vec<(Ballot, AcceptorConfig)>, Ballot)>,
    /// The matchmakers whose replies the candidate reported folding.
    registered_by: BTreeSet<u64>,
    /// A refusal was folded: the campaign is dead.
    refused: bool,
    /// The matchmaking closed with a quorum; Phase 1 is licensed.
    completed: bool,
}

/// The fold and the flag set (independent sticky bits per gate).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct MatchmakerAudit {
    registries: BTreeMap<u64, Registry>,
    /// Every `(matchmaker, ballot) -> configuration` ever folded, **never**
    /// pruned: the write-once ledger a GC'd-then-reused ballot would trip.
    ever: BTreeMap<(u64, Ballot), AcceptorConfig>,
    /// Every candidate's matchmaking phase, keyed by `(node, ballot)`.
    campaigns: BTreeMap<(u64, Ballot), Campaign>,
    /// The size of the deployed matchmaker set (from the boot reports).
    matchmakers: Option<usize>,
    deployed: bool,
    registered_any: bool,
    history_nonempty: bool,
    refused_stale: bool,
    refused_below_watermark: bool,
    duplicate_answered: bool,
    watermark_raised: bool,
    recovered_after_restart: bool,
    crashed_before_sync: bool,
    crashed_after_sync: bool,
    reply_dropped: bool,
    // --- the leader-side phase (#120) ---
    campaign_opened: bool,
    campaign_completed: bool,
    campaign_refused: bool,
    campaign_empty_history: bool,
    campaign_union_several: bool,
    campaign_disagreeing_histories: bool,
    resend_skipped: bool,
    request_resent: bool,
    reconfiguration_opened: bool,
}

impl MatchmakerAudit {
    /// The deployment's matchmaker count, as the boot reports declared it.
    pub(super) fn note_deployment(&mut self, matchmakers: usize) {
        let n = *self.matchmakers.get_or_insert(matchmakers);
        assert_always!(
            n == matchmakers,
            "matchmaker: every node derives the same matchmaker set",
            { "reported" => matchmakers, "folded" => n }
        );
        if matchmakers > 0 {
            self.deployed = true;
        }
    }

    /// Whether the deployment names matchmakers at all.
    pub(super) fn has_matchmakers(&self) -> bool {
        self.matchmakers.is_some_and(|n| n > 0)
    }

    fn quorum(&self) -> usize {
        self.matchmakers.unwrap_or(0) / 2 + 1
    }

    /// Whether `node` holds a matchmaker quorum for `ballot` — the licence
    /// for any `Prepare` and any leadership at that ballot.
    pub(super) fn phase1_licensed(&self, node: u64, ballot: Ballot) -> bool {
        self.campaigns
            .get(&(node, ballot))
            .is_some_and(|c| c.completed && !c.refused)
    }

    /// The configuration some matchmaker durably registered under `ballot`,
    /// if any (they never disagree; the first fold wins the lookup).
    pub(super) fn registered_config(&self, ballot: Ballot) -> Option<&AcceptorConfig> {
        self.ever
            .range((0, ballot)..=(u64::MAX, ballot))
            .find(|((_, b), _)| *b == ballot)
            .map(|(_, c)| c)
    }

    /// A matchmaker booted from its durable registry.
    pub(super) fn recovered(
        &mut self,
        matchmaker: MatchmakerId,
        registry: &[(Ballot, AcceptorConfig)],
        gc_watermark: Ballot,
    ) {
        self.deployed = true;
        let entry = self.registries.entry(matchmaker.0).or_default();
        entry.boots += 1;
        if entry.boots > 1 {
            // Invariant 5, across the restart.
            assert_always!(
                gc_watermark >= entry.watermark,
                "matchmaker: a recovered gc watermark never regresses",
                {
                    "matchmaker" => matchmaker.0,
                    "recovered_round" => gc_watermark.round,
                    "folded_round" => entry.watermark.round
                }
            );
            // Invariant 1, across the restart: every registration the driver
            // reported durable (and so every one it ever replied for) reads
            // back — unless GC legitimately dropped it below the floor.
            for (ballot, config) in &entry.registered {
                if *ballot < gc_watermark {
                    continue;
                }
                let read_back = registry.iter().find(|(b, _)| b == ballot).map(|(_, c)| c);
                assert_always!(
                    read_back == Some(config),
                    "matchmaker: a restart recovers every durable registration",
                    {
                        "matchmaker" => matchmaker.0,
                        "round" => ballot.round,
                        "bnode" => ballot.node.0,
                        "read_back" => read_back.map_or(0, |c| c.members.len())
                    }
                );
            }
            if !entry.registered.is_empty() {
                reach_once!(
                    self.recovered_after_restart,
                    "matchmaker: a matchmaker recovers its registry across a restart"
                );
            }
        }
        // Invariant 2, across the restart: a recovered registration is one
        // the driver reported durable, with the same bytes — a boot never
        // invents or alters one.
        for (ballot, config) in registry {
            let known = entry.registered.get(ballot);
            assert_always!(
                known == Some(config),
                "matchmaker: a restart never invents or alters a registration",
                {
                    "matchmaker" => matchmaker.0,
                    "round" => ballot.round,
                    "bnode" => ballot.node.0
                }
            );
        }
        entry.registered = registry.iter().cloned().collect();
        entry.watermark = gc_watermark;
    }

    /// A registration became durable.
    pub(super) fn registered(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        config: &AcceptorConfig,
    ) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 3: strictly above everything registered.
        let highest = entry.registered.keys().next_back().copied();
        assert_always!(
            highest.is_none_or(|h| ballot > h),
            "matchmaker: registrations are strictly increasing",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "bnode" => ballot.node.0,
                "highest_round" => highest.map_or(0, |h| h.round)
            }
        );
        // Invariant 5: never below the floor.
        assert_always!(
            ballot >= entry.watermark,
            "matchmaker: a registration never sits below the gc watermark",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "watermark_round" => entry.watermark.round
            }
        );
        // Invariant 2: one configuration per ballot, ever.
        let prior = self
            .ever
            .entry((matchmaker.0, ballot))
            .or_insert_with(|| config.clone());
        assert_always!(
            *prior == *config,
            "matchmaker: a ballot is registered with one configuration, ever",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        // One ballot has one proposer, so two matchmakers never register it
        // with different bytes: the paper's no-disagreement property, judged
        // across the registries.
        let elsewhere = self
            .ever
            .range((0, ballot)..=(u64::MAX, ballot))
            .find(|((m, b), c)| *b == ballot && *m != matchmaker.0 && **c != *config)
            .map(|((m, _), _)| *m);
        assert_always!(
            elsewhere.is_none(),
            "matchmaker: two matchmakers never register one ballot with different configurations",
            {
                "matchmaker" => matchmaker.0,
                "other" => elsewhere.unwrap_or(u64::MAX),
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        entry.registered.insert(ballot, config.clone());
        reach_once!(
            self.registered_any,
            "matchmaker: a configuration is registered"
        );
    }

    /// The watermark rose durably.
    pub(super) fn watermark_raised(&mut self, matchmaker: MatchmakerId, watermark: Ballot) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        assert_always!(
            watermark >= entry.watermark,
            "matchmaker: the gc watermark never regresses",
            {
                "matchmaker" => matchmaker.0,
                "round" => watermark.round,
                "folded_round" => entry.watermark.round
            }
        );
        entry.watermark = watermark;
        // Mirror the disk: registrations below the floor are gone.
        entry.registered = entry.registered.split_off(&watermark);
        reach_once!(
            self.watermark_raised,
            "matchmaker: the gc watermark is raised"
        );
    }

    /// A `Registered` reply is leaving.
    pub(super) fn replied(
        &mut self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        history: &[(Ballot, AcceptorConfig)],
        gc_watermark: Ballot,
    ) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 1: the registration is already folded durable — the driver
        // flushed and reported it before this send.
        assert_always!(
            entry.registered.contains_key(&ballot),
            "matchmaker: a registration is durable before its reply leaves",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        // Invariant 5: the reply reports the durable floor.
        assert_always!(
            gc_watermark == entry.watermark,
            "matchmaker: a reply carries the durable gc watermark",
            {
                "matchmaker" => matchmaker.0,
                "reported_round" => gc_watermark.round,
                "folded_round" => entry.watermark.round
            }
        );
        // Invariant 4: the history is the folded window `[watermark, ballot)`
        // — nothing missing (under-reporting), nothing foreign.
        let expected: Vec<(Ballot, AcceptorConfig)> = entry
            .registered
            .range(entry.watermark..ballot)
            .map(|(b, c)| (*b, c.clone()))
            .collect();
        assert_always!(
            history == expected.as_slice(),
            "matchmaker: a reply reports every registration below its ballot",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "reported" => history.len(),
                "expected" => expected.len()
            }
        );
        if !history.is_empty() {
            reach_once!(
                self.history_nonempty,
                "matchmaker: a reply carries a prior configuration"
            );
        }
        match entry.replied.get(&ballot) {
            // A re-answer of an already-answered ballot: the idempotent
            // duplicate path. It may only shrink the first answer (GC).
            Some(first) => {
                assert_always!(
                    history.iter().all(|h| first.contains(h)),
                    "matchmaker: a re-answer never adds to the first answer's history",
                    { "matchmaker" => matchmaker.0, "round" => ballot.round }
                );
                reach_once!(
                    self.duplicate_answered,
                    "matchmaker: a duplicate request is answered again idempotently"
                );
            }
            None => {
                entry.replied.insert(ballot, history.to_vec());
            }
        }
        // The leader side: what this candidate may receive for this ballot.
        self.campaigns
            .entry((to.0, ballot))
            .or_default()
            .replies
            .insert(matchmaker.0, (history.to_vec(), gc_watermark));
    }

    /// A refusal is leaving: it must name the registry's own state.
    pub(super) fn refused(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        match refusal {
            MatchRefusal::Stale { highest } => {
                let folded = entry.registered.keys().next_back().copied();
                assert_always!(
                    folded == Some(highest) && ballot <= highest,
                    "matchmaker: a stale refusal names the registry's highest ballot",
                    {
                        "matchmaker" => matchmaker.0,
                        "round" => ballot.round,
                        "highest_round" => highest.round,
                        "folded_round" => folded.map_or(0, |h| h.round)
                    }
                );
                reach_once!(self.refused_stale, "matchmaker: a stale request is refused");
            }
            MatchRefusal::BelowWatermark { watermark } => {
                assert_always!(
                    watermark == entry.watermark && ballot < watermark,
                    "matchmaker: a below-watermark refusal names the durable watermark",
                    {
                        "matchmaker" => matchmaker.0,
                        "round" => ballot.round,
                        "watermark_round" => watermark.round,
                        "folded_round" => entry.watermark.round
                    }
                );
                reach_once!(
                    self.refused_below_watermark,
                    "matchmaker: a request below the gc watermark is refused"
                );
            }
        }
    }

    /// A matchmaker crashed at one of its seams.
    pub(super) fn crashed(&mut self, seam: Seam) {
        match seam {
            Seam::MatchBeforeSync => reach_once!(
                self.crashed_before_sync,
                "matchmaker: the driver crashes before syncing a registration"
            ),
            Seam::MatchAfterSyncBeforeReply => reach_once!(
                self.crashed_after_sync,
                "matchmaker: the driver crashes after syncing and before replying"
            ),
            _ => {}
        }
    }

    /// A reply was deliberately dropped after the registration was durable.
    pub(super) fn reply_dropped(&mut self) {
        reach_once!(
            self.reply_dropped,
            "matchmaker: a reply is dropped at the reply seam"
        );
    }

    // ---- the leader-side matchmaking phase (#120) ---------------------------

    /// A candidate opened matchmaking for `ballot`, registering `config`.
    pub(super) fn campaign_started(
        &mut self,
        node: NodeId,
        ballot: Ballot,
        config: &AcceptorConfig,
        reconfiguration: bool,
    ) {
        // A campaign is opened once per ballot: the ballot is fresh.
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        assert_always!(
            campaign.config.is_none() && !campaign.completed,
            "matchmaking: a campaign opens once per ballot",
            { "node" => node.0, "round" => ballot.round }
        );
        assert_always!(
            ballot.node == node,
            "matchmaking: a campaign registers a ballot naming its own node",
            { "node" => node.0, "bnode" => ballot.node.0 }
        );
        campaign.config = Some(config.clone());
        reach_once!(
            self.campaign_opened,
            "matchmaking: a candidate opens a matchmaking phase"
        );
        if reconfiguration {
            reach_once!(
                self.reconfiguration_opened,
                "matchmaking: a leader opens a reconfiguration campaign"
            );
        }
    }

    /// A candidate sent (or re-sent) its request to `matchmaker`.
    pub(super) fn request_sent(&mut self, node: NodeId, matchmaker: MatchmakerId, ballot: Ballot) {
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        // A request only ever leaves an open, unrefused, uncompleted campaign
        // — the first send, or a re-send while the answer is outstanding.
        assert_always!(
            campaign.config.is_some() && !campaign.refused && !campaign.completed,
            "matchmaking: a request leaves only an open campaign",
            { "node" => node.0, "matchmaker" => matchmaker.0, "round" => ballot.round }
        );
        if campaign.replies.contains_key(&matchmaker.0)
            || campaign.registered_by.contains(&matchmaker.0)
        {
            // The matchmaker already answered once: this is the re-send that
            // meets the idempotent re-answer path.
            reach_once!(
                self.request_resent,
                "matchmaking: a request is re-sent to a matchmaker that already answered"
            );
        }
    }

    /// The candidate deliberately skipped a due re-send.
    pub(super) fn resend_skipped(&mut self) {
        reach_once!(
            self.resend_skipped,
            "matchmaking: the driver skips a due matchmaking re-send"
        );
    }

    /// The candidate folded `matchmaker`'s registration.
    pub(super) fn registered_by(
        &mut self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        remaining: usize,
    ) {
        let quorum = self.quorum();
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        // The reply it folded is one the matchmaker actually sent it.
        assert_always!(
            campaign.replies.contains_key(&matchmaker.0),
            "matchmaking: a folded registration was sent by that matchmaker",
            { "node" => node.0, "matchmaker" => matchmaker.0, "round" => ballot.round }
        );
        campaign.registered_by.insert(matchmaker.0);
        assert_always!(
            remaining == quorum.saturating_sub(campaign.registered_by.len()),
            "matchmaking: the remaining count is the quorum minus the registrations folded",
            {
                "node" => node.0,
                "round" => ballot.round,
                "remaining" => remaining,
                "registered" => campaign.registered_by.len(),
                "quorum" => quorum
            }
        );
    }

    /// The candidate's matchmaking closed with a quorum: Phase 1 opens.
    pub(super) fn completed(
        &mut self,
        node: NodeId,
        ballot: Ballot,
        prior: &[AcceptorConfig],
        watermark: Ballot,
        registered_by: usize,
        disagreements: u64,
    ) {
        let quorum = self.quorum();
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        // The closing fold happened on this ballot's last folded reply, so the
        // registering set is complete here; it must be a quorum.
        assert_always!(
            registered_by >= quorum && campaign.registered_by.len() >= quorum,
            "matchmaking: Phase 1 opens only on a matchmaker quorum",
            {
                "node" => node.0,
                "round" => ballot.round,
                "registered" => campaign.registered_by.len(),
                "quorum" => quorum
            }
        );
        assert_always!(
            !campaign.refused,
            "matchmaking: a refused campaign never reaches Phase 1",
            { "node" => node.0, "round" => ballot.round }
        );
        // The write-once ledger already guarantees no two matchmakers
        // disagree; the union's own count must agree.
        assert_always!(
            disagreements == 0,
            "matchmaking: no two matchmakers disagree on a ballot's configuration",
            { "node" => node.0, "round" => ballot.round, "disagreements" => disagreements }
        );
        // Invariants 2 and 3, re-derived from the replies the registering
        // matchmakers sent: the max watermark, and the union at or above it.
        let replies: Vec<&(Vec<(Ballot, AcceptorConfig)>, Ballot)> = campaign
            .registered_by
            .iter()
            .filter_map(|m| campaign.replies.get(m))
            .collect();
        let expected_watermark = replies.iter().map(|(_, w)| *w).max().unwrap_or_default();
        assert_always!(
            watermark == expected_watermark,
            "matchmaking: the watermark used is the maximum reported",
            {
                "node" => node.0,
                "round" => ballot.round,
                "used_round" => watermark.round,
                "max_round" => expected_watermark.round
            }
        );
        let mut expected: Vec<&AcceptorConfig> = Vec::new();
        let mut histories: BTreeSet<Vec<Ballot>> = BTreeSet::new();
        for (history, _) in &replies {
            histories.insert(history.iter().map(|(b, _)| *b).collect());
            for (b, config) in history {
                if *b >= expected_watermark && !expected.contains(&config) {
                    expected.push(config);
                }
            }
        }
        let union_matches = prior.len() == expected.len()
            && expected.iter().all(|c| prior.contains(c))
            && prior.iter().all(|c| expected.contains(&c));
        assert_always!(
            union_matches,
            "matchmaking: the prior set is the union of every registering reply above the watermark",
            {
                "node" => node.0,
                "round" => ballot.round,
                "reported" => prior.len(),
                "expected" => expected.len()
            }
        );
        campaign.completed = true;
        reach_once!(
            self.campaign_completed,
            "matchmaking: a campaign closes with a matchmaker quorum"
        );
        if prior.is_empty() {
            reach_once!(
                self.campaign_empty_history,
                "matchmaking: a campaign finds no prior configuration (Phase 1 trivially complete)"
            );
        }
        if prior.len() > 1 {
            reach_once!(
                self.campaign_union_several,
                "matchmaking: a campaign unions more than one prior configuration"
            );
        }
        if histories.len() > 1 {
            reach_once!(
                self.campaign_disagreeing_histories,
                "matchmaking: a campaign unions histories that differ between matchmakers"
            );
        }
    }

    /// The candidate folded a refusal and abandoned the campaign.
    pub(super) fn campaign_refused(&mut self, node: NodeId, ballot: Ballot, refusal: MatchRefusal) {
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        assert_always!(
            !campaign.completed,
            "matchmaking: a refusal never lands on a campaign that already reached Phase 1",
            { "node" => node.0, "round" => ballot.round }
        );
        campaign.refused = true;
        let _ = refusal;
        reach_once!(
            self.campaign_refused,
            "matchmaking: a campaign is refused by a matchmaker"
        );
    }

    /// The `sometimes` gates, evaluated once per run: both deployment modes are
    /// visited, and a deployed registry genuinely registers, reports a past,
    /// and licenses a leadership. Everything rarer (each refusal leg, the
    /// duplicate re-answer, a GC raise, a restart) is a
    /// `reachable` recorded at its transition — each is conditioned on the
    /// deployment draw *and* a rarer event, and a per-sweep gate on such a
    /// conjunction would starve saturation.
    pub(super) fn check_gates(&self) {
        assert_sometimes!(self.deployed, "matchmaker: a run deploys matchmakers");
        assert_sometimes!(!self.deployed, "matchmaker: a run deploys no matchmakers");
        assert_sometimes!(
            self.registered_any,
            "matchmaker: a configuration is registered"
        );
        assert_sometimes!(
            self.history_nonempty,
            "matchmaker: a reply carries a prior configuration"
        );
        assert_sometimes!(
            self.campaign_completed,
            "matchmaking: a campaign closes with a matchmaker quorum"
        );
        if self.recovered_after_restart {
            assert_reachable!("matchmaker: a matchmaker recovers its registry across a restart");
        }
    }

    /// One line for the red-path print.
    pub(super) fn diagnostics(&self) -> String {
        let summary: Vec<String> = self
            .registries
            .iter()
            .map(|(id, r)| {
                format!(
                    "mm{id}[boots={} registered={} watermark={}.{}]",
                    r.boots,
                    r.registered.len(),
                    r.watermark.round,
                    r.watermark.node.0
                )
            })
            .collect();
        let campaigns = self.campaigns.values().filter(|c| c.completed).count();
        format!("{} campaigns_completed={campaigns}", summary.join(" "))
    }
}
