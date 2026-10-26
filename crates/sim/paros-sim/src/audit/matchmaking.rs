//! The candidate's matchmaking phase (#120), garbage collection (#123) and the reconfigurer of the matchmaker set (#125): the node's side.

use std::collections::BTreeSet;
use std::ops::Bound;

use moonpool_sim::{TimeProvider, assert_always, assert_reachable, assert_sometimes};
use paros::client::{MatchmakersRefusal, RetireRefusal};
use paros::core::{
    AcceptorConfig, Ballot, GcAck, GcStep, MatchRefusal, MatchmakerId, MatchmakerSet, NodeId,
    ReconfigureReply, ReconfigureResult, ReconfigurerStep, Registration, RegistrationKind, Slot,
};
use paros::roles::node::BootRefusal;

use super::NodeAudit;
use super::matchmaker::MatchmakerAudit;
use super::state::AuditState;
use crate::audit::matchmaker::{OpenProbe, check_folded_union, folded_effective, history_digest};

impl<T: TimeProvider> NodeAudit<T> {
    // ---- the leader-side matchmaking phase (#120) and reconfiguration (#122) ----
    pub(super) fn matchmaking_started(
        &self,
        node: NodeId,
        ballot: Ballot,
        config: &AcceptorConfig,
        kind: RegistrationKind,
        generation: u64,
    ) {
        self.state()
            .matchmaker
            .campaign_started(node, ballot, config, kind, generation);
    }

    // ---- garbage collection (#123) ------------------------------------------
    pub(super) fn gc_request_sent(
        &self,
        node: NodeId,
        _matchmaker: MatchmakerId,
        _generation: u64,
        watermark: Ballot,
        fence: Option<Slot>,
    ) {
        self.state().gc_request_sent(node, watermark, fence);
    }

    pub(super) fn gc_step(
        &self,
        node: NodeId,
        _matchmaker: MatchmakerId,
        ack: &GcAck,
        step: &GcStep,
    ) {
        self.state().gc_step(node, ack, step);
    }

    // ---- the matchmaker set and its reconfiguration (#125) ------------------
    pub(super) fn matchmakers_learned(&self, node: NodeId, set: &MatchmakerSet) {
        self.state().matchmaker.set_learned(node, set);
    }

    pub(super) fn reconfigurer_started(
        &self,
        node: NodeId,
        old: &MatchmakerSet,
        target: &[MatchmakerId],
    ) {
        self.state()
            .matchmaker
            .reconfigurer_started(node, old, target);
    }

    pub(super) fn reconfigurer_aborted(&self, node: NodeId) {
        self.state().matchmaker.reconfigurer_aborted(node);
    }

    pub(super) fn reconfigurer_backoff(&self, _node: NodeId, _ticks: u64) {
        self.state().matchmaker.reconfigurer_backoff();
    }

    pub(super) fn reconfigure_matchmakers_acked(
        &self,
        _node: NodeId,
        refusal: Option<MatchmakersRefusal>,
    ) {
        self.state().reconfigure_matchmakers_acked(refusal);
    }

    pub(super) fn reconfigurer_step(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        reply: &ReconfigureReply,
        step: &ReconfigurerStep,
    ) {
        self.state()
            .matchmaker
            .reconfigurer_step(node, matchmaker, reply, step);
    }

    pub(super) fn successor_republished(
        &self,
        node: NodeId,
        _matchmaker: MatchmakerId,
        successor: &MatchmakerSet,
    ) {
        self.state()
            .matchmaker
            .successor_republished(node, successor);
    }

    pub(super) fn retire_acked(&self, _node: NodeId, refusal: Option<RetireRefusal>) {
        self.state().retire_acked(refusal);
    }

    pub(super) fn retired(&self, node: NodeId) {
        self.state().retired(node);
    }

    pub(super) fn membership_probe_opened(
        &self,
        node: NodeId,
        ballot: Ballot,
        _believed: &AcceptorConfig,
        generation: u64,
    ) {
        self.state()
            .matchmaker
            .probe_opened(node, ballot, generation);
    }

    pub(super) fn membership_probe_closed(
        &self,
        node: NodeId,
        ballot: Ballot,
        effective: Option<Ballot>,
        member: bool,
        answered_by: usize,
    ) {
        self.state()
            .matchmaker
            .probe_closed(node, ballot, effective, member, answered_by);
    }

    pub(super) fn membership_probe_late(
        &self,
        node: NodeId,
        _ballot: Ballot,
        _effective: Option<Ballot>,
        member: bool,
    ) {
        self.state().matchmaker.probe_late(node, member);
    }

    pub(super) fn match_request_sent(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
    ) {
        self.state()
            .matchmaker
            .request_sent(node, matchmaker, ballot);
    }

    pub(super) fn matchmaking_stale_configuration(
        &self,
        node: NodeId,
        ballot: Ballot,
        newest: Ballot,
    ) {
        self.state().matchmaker.campaign_stale(node, ballot, newest);
    }

    pub(super) fn matchmaking_timeout(&self, node: NodeId, ballot: Ballot, _count: u64) {
        self.state().matchmaker.clock_reasked(node, ballot);
    }

    pub(super) fn match_registered_by(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        remaining: usize,
        watermark: Ballot,
        history_hash: u64,
    ) {
        self.state().matchmaker.registered_by(
            node,
            matchmaker,
            ballot,
            remaining,
            watermark,
            history_hash,
        );
    }

    pub(super) fn match_paged(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        _next: Ballot,
        watermark: Ballot,
        history_hash: u64,
    ) {
        self.state()
            .matchmaker
            .paged(node, matchmaker, ballot, watermark, history_hash);
    }

    pub(super) fn matchmaking_completed(
        &self,
        node: NodeId,
        ballot: Ballot,
        prior: &[AcceptorConfig],
        watermark: Ballot,
        registered_by: usize,
        disagreements: u64,
    ) {
        self.state().matchmaking_completed(
            node,
            ballot,
            prior,
            watermark,
            registered_by,
            disagreements,
        );
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "the `Audit` hook's signature"
    )]
    pub(super) fn matchmaking_refused(
        &self,
        node: NodeId,
        _matchmaker: MatchmakerId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        self.state()
            .matchmaker
            .campaign_refused(node, ballot, &refusal);
    }

    pub(super) fn reconfigure_acked(
        &self,
        node: NodeId,
        _members: &[NodeId],
        result: ReconfigureResult,
    ) {
        self.state().reconfigure_acked(node, result);
    }
}

impl MatchmakerAudit {
    // ---- the leader-side matchmaking phase (#120) ---------------------------

    /// The bootstrap configuration every node boots with.
    pub(super) fn note_bootstrap(&mut self, bootstrap: &AcceptorConfig) {
        self.bootstrap.get_or_insert_with(|| bootstrap.clone());
    }

    /// `node` booted (a first boot, an attrition restart, a seam-crash
    /// recovery): its round floor is volatile and starts over from the
    /// durable promise, so a refusal folded for its previous incarnation no
    /// longer binds its next campaign (the hunt found the gate red on exactly
    /// that — a node refused at round 1, restarted, and honestly campaigned
    /// at round 2 again; one more refusal re-floors it).
    pub(super) fn node_booted(&mut self, node: NodeId) {
        self.refused_floor.remove(&node.0);
        // A fresh incarnation probes from the bootstrap: naming it is not
        // a re-probe's work.
        self.probed_outside.remove(&node.0);
    }

    /// A candidate opened matchmaking for `ballot`, registering `config`
    /// with `generation`'s matchmakers.
    pub(super) fn campaign_started(
        &mut self,
        node: NodeId,
        ballot: Ballot,
        config: &AcceptorConfig,
        kind: RegistrationKind,
        generation: u64,
    ) {
        let reconfiguration = kind.is_reconfiguration();
        // A refused candidate re-campaigns strictly above the round that
        // refused it — never one round up from its own, which the same
        // registration would refuse again (the leapfrog livelock).
        if let Some(floor) = self.refused_floor.get(&node.0).copied() {
            assert_always!(
                ballot.round > floor,
                "matchmaking: a refused candidate re-campaigns above the refuser's highest round",
                { "node" => node.0, "round" => ballot.round, "floor" => floor }
            );
            self.refused_floor.remove(&node.0);
        }
        // A belief comes from a leader's wire or the bootstrap, never from
        // the ledger: a plain campaign registers only a configuration some
        // completed campaign or started reconfiguration already put on the
        // wire (a `Prepare`, `Heartbeat` or `Relinquish` carries it), or the
        // one every node boots with.
        if !reconfiguration && self.bootstrap.is_some() {
            let known =
                self.bootstrap.as_ref() == Some(config) || self.wire_configs.contains(config);
            assert_always!(
                known,
                "matchmaking: a candidate registers only the bootstrap or a configuration a leader put on the wire",
                { "node" => node.0, "round" => ballot.round, "members" => config.members().len() }
            );
        }
        if reconfiguration {
            self.wire_configs.insert(config.clone());
        }
        // A campaign is opened once per ballot: the ballot is fresh.
        let floor_at_start = self.effective_floor;
        let effective_at_start = self.effective_at(generation, ballot);
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        campaign.floor_at_start = floor_at_start;
        campaign.effective_at_start = effective_at_start;
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
        campaign.kind = kind;
        campaign.generation = generation;
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

    /// The candidate's election clock fired on an open matchmaking and
    /// re-asked instead of abandoning: the campaign it reports is the one
    /// still open — never completed, never refused, at the same ballot.
    pub(super) fn clock_reasked(&mut self, node: NodeId, ballot: Ballot) {
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        assert_always!(
            campaign.config.is_some() && !campaign.completed && !campaign.refused,
            "matchmaking: the election clock re-asks an open campaign and moves nothing",
            { "node" => node.0, "round" => ballot.round }
        );
        reach_once!(
            self.clock_reasked,
            "matchmaking: the election clock re-asks a pending matchmaking instead of abandoning it"
        );
    }

    /// The candidate folded `matchmaker`'s registration.
    /// Record the page a candidate reported folding: it must be one this
    /// matchmaker actually sent for this ballot, named by its watermark and
    /// history digest.
    fn fold_page(
        &mut self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        watermark: Ballot,
        history_hash: u64,
    ) {
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        // The reply it folded is one the matchmaker actually sent it — and,
        // now that the candidate names *which* copy it folded, exactly one
        // of them. A matchmaker answers a re-sent request again from a
        // registry a floor may have been raised on in between, so the copies
        // differ; matching the point is both cheaper than the cartesian
        // search it replaces and strictly stronger.
        let sent = campaign.replies.get(&matchmaker.0).and_then(|copies| {
            copies
                .iter()
                .find(|(history, w, _)| *w == watermark && history_digest(history) == history_hash)
                .cloned()
        });
        assert_always!(
            sent.is_some(),
            "matchmaking: a folded registration was sent by that matchmaker",
            { "node" => node.0, "matchmaker" => matchmaker.0, "round" => ballot.round }
        );
        if let Some(copy) = sent {
            campaign.folded.entry(matchmaker.0).or_default().push(copy);
        }
    }

    /// The candidate folded a page that was not the last one (#P9): nothing
    /// counts toward the quorum until the answer is complete.
    pub(super) fn paged(
        &mut self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        watermark: Ballot,
        history_hash: u64,
    ) {
        self.fold_page(node, matchmaker, ballot, watermark, history_hash);
        let counted = self
            .campaigns
            .get(&(node.0, ballot))
            .is_some_and(|c| c.registered_by.contains(&matchmaker.0));
        assert_always!(
            !counted,
            "matchmaking: an incomplete answer never counts toward the quorum",
            { "node" => node.0, "matchmaker" => matchmaker.0, "round" => ballot.round }
        );
        reach_once!(
            self.answer_paged,
            "matchmaking: a matchmaker answers a registration in pages"
        );
    }

    pub(super) fn registered_by(
        &mut self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        remaining: usize,
        watermark: Ballot,
        history_hash: u64,
    ) {
        let generation = self
            .campaigns
            .get(&(node.0, ballot))
            .map_or(0, |c| c.generation);
        let quorum = self.quorum(generation);
        self.fold_page(node, matchmaker, ballot, watermark, history_hash);
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
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
        let (generation, floor_at_start) = self
            .campaigns
            .get(&(node.0, ballot))
            .map_or((0, Ballot::default()), |c| (c.generation, c.floor_at_start));
        let quorum = self.quorum(generation);
        // GC invariant 3: the watermark a campaign filters by is at or above
        // every floor proven effective at a matchmaker quorum *before the
        // campaign opened* — every reply it folds was sent after that, so
        // quorum intersection plus the max rule (#120's invariant 3) is
        // exactly what keeps a collected configuration out of every later
        // `H`. A floor that became effective mid-campaign may legitimately
        // be missed: the reply from the intersecting matchmaker can predate
        // its ack (a stale but safe superset of `H`).
        assert_always!(
            watermark >= floor_at_start,
            "gc: a campaign's watermark is at or above the effective floor",
            {
                "node" => node.0,
                "round" => ballot.round,
                "used_round" => watermark.round,
                "floor_round" => floor_at_start.round
            }
        );
        self.check_effective_configuration(node, ballot);
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
        let histories = check_folded_union(campaign, node, ballot, prior, watermark);
        // The effective configuration (review of #132): the highest-ballot
        // *reconfiguration* registration the folded replies name. An
        // ordinary campaign completes only if it registered exactly that —
        // a stale belief must have aborted instead (`campaign_stale`), so a
        // superseded configuration is never reinstated by an election. A
        // reconfiguration campaign is exempt: it is the next one.
        // A reconfiguration below a leadership that won under another
        // configuration is one that leadership's matchmaking quorum
        // missed, held by a minority: the candidate keeps its belief over
        // it (`probe_floor`; replay-main 9491883622406273044 on 832b116).
        // A reconfiguration registered *above* the one the quorum shows,
        // which the quorum missed (held by a minority, heard from one late
        // probe answer or an earlier quorum), is the other half of the
        // probe floor: the fact the candidate's belief matches. The
        // candidate keeps it; it is newer than what the quorum shows, never
        // superseded by it (replay-main 1859054409658602303 and
        // 1861290827981239083 on 25f93b4).
        let effective = folded_effective(campaign);
        if !campaign.kind.is_reconfiguration()
            && let Some((newest, config)) = effective
        {
            let below_a_leadership = self
                .won
                .range((Bound::Excluded(newest), Bound::Excluded(ballot)))
                .any(|(_, ran)| *ran != config);
            let newer_fact = campaign.config.as_ref().is_some_and(|mine| {
                self.reconfigurations
                    .range((Bound::Excluded(newest), Bound::Excluded(ballot)))
                    .any(|(_, (registered, _))| registered == mine)
            });
            assert_always!(
                campaign.config.as_ref() == Some(&config) || below_a_leadership || newer_fact,
                "matchmaking: a completed ordinary campaign registered the effective configuration",
                { "node" => node.0, "round" => ballot.round, "newest_round" => newest.round }
            );
            if campaign.config.as_ref() != Some(&config) && newer_fact {
                reach_once!(
                    self.campaign_kept_newer_fact,
                    "matchmaking: a campaign keeps a reconfiguration its quorum missed"
                );
            }
        }
        campaign.completed = true;
        let registered = campaign.config.clone();
        // The per-matchmaker reply copies are scratch for the fold above and
        // nothing reads them once the campaign closed; the keys stay (a
        // late re-answer still has to be one this matchmaker sent).
        for copies in campaign.replies.values_mut() {
            copies.clear();
            copies.shrink_to_fit();
        }
        campaign.folded.clear();
        self.completed_by_generation
            .entry(generation)
            .or_default()
            .insert(ballot, registered.clone());
        if let Some(config) = registered {
            // From here the configuration rides this campaign's `Prepare`s
            // (and its heartbeats, if it wins): a belief others may learn.
            self.wire_configs.insert(config);
        }
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

    /// The candidate abandoned an ordinary campaign because the quorum's
    /// histories named a reconfiguration to another configuration, and
    /// adopted the one registered at `newest`. That ballot must hold a
    /// *reconfiguration* registration in the write-once ledger: only a
    /// leader's explicit change ever moves a belief, never another
    /// candidate's belief (the flip-flop of #132's hunt).
    pub(super) fn campaign_stale(&mut self, node: NodeId, ballot: Ballot, newest: Ballot) {
        let named = self
            .ever
            .range((0, newest)..=(u64::MAX, newest))
            .find(|((_, b), _)| *b == newest)
            .map(|(_, r)| r.kind.is_reconfiguration());
        assert_always!(
            named == Some(true),
            "matchmaking: a stale-belief abort adopts a reconfiguration registration",
            {
                "node" => node.0,
                "round" => ballot.round,
                "newest_round" => newest.round,
                "registered" => named.is_some()
            }
        );
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        assert_always!(
            !campaign.completed && !campaign.kind.is_reconfiguration(),
            "matchmaking: a stale-configuration abort never follows Phase 1 and never hits a reconfiguration",
            { "node" => node.0, "round" => ballot.round }
        );
        campaign.refused = true;
        reach_once!(
            self.campaign_stale,
            "matchmaking: a candidate adopts the effective configuration and re-campaigns"
        );
    }

    /// The candidate folded a refusal and abandoned the campaign.
    /// A node opened a membership probe tagged `ballot` against
    /// `generation`'s matchmakers (#173).
    pub(super) fn probe_opened(&mut self, node: NodeId, ballot: Ballot, generation: u64) {
        let ceiling = Ballot {
            round: u64::MAX,
            node: NodeId(u64::MAX),
        };
        let held = self.effective_at(generation, ceiling);
        self.probes
            .insert((node.0, ballot), OpenProbe { held, generation });
    }

    /// A node's membership probe closed (#173). A reconfiguration a
    /// matchmaker quorum already held when the probe opened is in every
    /// quorum of answers (each raised its effective scalar durably before
    /// answering anything), so the configuration the probe adopted is at
    /// least that one — the same intersection argument that hands a
    /// campaign's matchmaking every earlier registration.
    pub(super) fn probe_closed(
        &mut self,
        node: NodeId,
        ballot: Ballot,
        effective: Option<Ballot>,
        member: bool,
        answered_by: usize,
    ) {
        let opened = self.probes.remove(&(node.0, ballot));
        // The probe's twin of the campaign's quorum check (#343): a probe
        // closes only once a quorum of the set it asked has answered. An
        // unknown probe (its opening lost to a reboot) counts against the
        // bootstrap set.
        let quorum = self.quorum(opened.as_ref().map_or(0, |p| p.generation));
        assert_always!(
            answered_by >= quorum,
            "matchmaking: a membership probe closes only on a matchmaker quorum",
            {
                "node" => node.0,
                "round" => ballot.round,
                "answered" => answered_by,
                "quorum" => quorum
            }
        );
        let held = opened.and_then(|p| p.held);
        let learned = held
            .as_ref()
            .is_none_or(|(at, _)| effective.is_some_and(|adopted| adopted >= *at));
        assert_always!(
            learned,
            "matchmaking: a membership probe learns every reconfiguration a matchmaker quorum held when it opened",
            {
                "node" => node.0,
                "held_round" => held.as_ref().map_or(0, |(at, _)| at.round),
                "adopted_round" => effective.map_or(0, |at| at.round)
            }
        );
        if member && effective.is_some() {
            reach_once!(
                self.probe_rejoined,
                "matchmaking: a membership probe finds its node inside a reconfiguration and it campaigns"
            );
        }
        if member && self.probed_outside.remove(&node.0) {
            reach_once!(
                self.reprobe_rejoined,
                "matchmaking: a node outside its heard belief re-probes, is named and campaigns"
            );
        }
        if !member && effective.is_some() {
            self.probed_outside.insert(node.0);
        }
    }

    /// A closed probe's late answer moved an outside node's belief (#278).
    pub(super) fn probe_late(&mut self, node: NodeId, member: bool) {
        reach_once!(
            self.late_answer_moved,
            "membership: a late probe answer moves an outside node's belief"
        );
        if member {
            self.probed_outside.remove(&node.0);
        }
    }

    pub(super) fn campaign_refused(
        &mut self,
        node: NodeId,
        ballot: Ballot,
        refusal: &MatchRefusal,
    ) {
        if self.probes.remove(&(node.0, ballot)).is_some() {
            // A refused probe is simply spent: the node's next election
            // timeout opens another.
            return;
        }
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        assert_always!(
            !campaign.completed,
            "matchmaking: a refusal never lands on a campaign that already reached Phase 1",
            { "node" => node.0, "round" => ballot.round }
        );
        campaign.refused = true;
        // Dead campaign: the reply copies are scratch nothing reads again.
        for copies in campaign.replies.values_mut() {
            copies.clear();
            copies.shrink_to_fit();
        }
        campaign.folded.clear();
        if let MatchRefusal::Stale { highest } = refusal {
            let floor = self.refused_floor.entry(node.0).or_default();
            *floor = (*floor).max(highest.round);
        }
        reach_once!(
            self.campaign_refused,
            "matchmaking: a campaign is refused by a matchmaker"
        );
    }

    /// The `sometimes` gates, evaluated once per run: **only** the deployment
    /// draw itself, which every seed decides one way or the other.
    ///
    /// Everything else this fold observes is a `reachable` recorded at its own
    /// transition — each is conditioned on the deployment draw *and* a rarer
    /// event, and a per-sweep gate on such a conjunction would starve
    /// saturation. That rule is also mechanical, not only stylistic: moonpool
    /// keys an assertion slot by the **hash of its message**, so a `sometimes`
    /// here sharing a string with the `reach_once!` at the transition is one
    /// slot, not two, and whichever fires first decides the kind. Five gates
    /// did exactly that (a configuration is registered, a reply carries a
    /// prior configuration, a campaign closes with a matchmaker quorum, a
    /// floor becomes effective, a handover completes) and are now recorded
    /// only where they happen.
    pub(super) fn check_gates(&self) {
        assert_sometimes!(self.deployed, "matchmaker: a run deploys matchmakers");
        assert_sometimes!(!self.deployed, "matchmaker: a run deploys no matchmakers");
        if self.recovered_after_restart {
            assert_reachable!("matchmaker: a matchmaker recovers its registry across a restart");
        }
    }

    // ---- garbage collection (#123) ------------------------------------------

    /// Whether a GC request at `(node, watermark)` still needs its licence
    /// judged. The licence is judged **once** per pair, and re-deriving it is
    /// an O(fence x members) walk the leader repeats on every re-send, so the
    /// caller asks first and only then pays for the derivation.
    pub(super) fn gc_needs_licence(&self, node: NodeId, watermark: Ballot) -> bool {
        !self.gc_judged.contains(&(node.0, watermark))
    }

    /// A leader sent a GC request; `covered` is the audit's own re-derivation
    /// of the forgettability condition (computed by the caller, which holds
    /// the durable-record fold, and only when [`Self::gc_needs_licence`] says
    /// it is still wanted), judged once per `(node, watermark)`.
    pub(super) fn gc_requested(
        &mut self,
        node: NodeId,
        watermark: Ballot,
        handed: bool,
        fence: Option<Slot>,
        covered: Option<bool>,
        uncovered: &str,
    ) {
        // A leader collects at its own ballot — the floor it raises is the
        // ballot it registered above every configuration it may forget —
        // or at the ballot its minter handed on to it (#529).
        assert_always!(
            watermark.node == node || handed,
            "gc: a leader collects at its own ballot",
            { "node" => node.0, "bnode" => watermark.node.0, "round" => watermark.round }
        );
        if self.gc_judged.insert((node.0, watermark))
            && let Some(covered) = covered
        {
            assert_always!(
                covered,
                "gc: a GC request is licensed by a quorum holding the fence",
                {
                    "node" => node.0,
                    "round" => watermark.round,
                    "fence" => crate::signed_watermark(fence.map(|s| s.0)),
                    "uncovered" => uncovered.to_string()
                }
            );
        }
        reach_once!(
            self.gc_requested,
            "gc: a leader asks the matchmakers to raise the floor"
        );
    }

    /// A node abandoned a handover phase that made no progress for the
    /// timeout: the frozen generation waits for the next finisher.
    pub(super) fn reconfigurer_aborted(&mut self, node: NodeId) {
        self.reconfigurer_trace.entry(node.0).or_default().1 += 1;
        reach_once!(
            self.reconfigurer_aborted,
            "generation: a stalled handover is abandoned after its timeout"
        );
    }

    /// A node's preempted successor decree waits a jittered backoff before
    /// reopening.
    pub(super) fn reconfigurer_backoff(&mut self) {
        reach_once!(
            self.reconfigurer_backed_off,
            "generation: a preempted successor decree backs off before reopening"
        );
    }

    /// A matchmaker answered a GC request.
    pub(super) fn gc_replied(&mut self, matchmaker: MatchmakerId, ack: &GcAck) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        // The ack reports the durable floor (the raise was folded first).
        assert_always!(
            ack.watermark == entry.watermark,
            "gc: a garbage-collection ack carries the durable watermark",
            {
                "matchmaker" => matchmaker.0,
                "reported_round" => ack.watermark.round,
                "folded_round" => entry.watermark.round
            }
        );
        if !ack.applied {
            reach_once!(
                self.gc_refused,
                "gc: a garbage-collection request for another generation is refused"
            );
        }
    }

    /// The leader folded a GC ack.
    pub(super) fn gc_step(
        &mut self,
        node: NodeId,
        ack: &GcAck,
        step: &GcStep,
        config: Option<&AcceptorConfig>,
    ) {
        self.gc_acks
            .entry((node.0, ack.watermark))
            .or_default()
            .insert(ack.matchmaker.0);
        let GcStep::Effective { watermark, retired } = step else {
            return;
        };
        // GC invariant 2 (issue invariant 5): the floor is held durably at a
        // quorum of the addressed generation's set before it is called
        // effective and before anything is retired.
        let generation = ack.generation.0;
        let holders = self
            .registries
            .iter()
            .filter(|(id, _)| self.member_of(generation, **id))
            .filter(|(_, r)| r.watermark >= *watermark)
            .count();
        let quorum = self.quorum(generation);
        assert_always!(
            holders >= quorum,
            "gc: a floor reported effective is held durably by a matchmaker quorum",
            {
                "node" => node.0,
                "round" => watermark.round,
                "holders" => holders,
                "quorum" => quorum
            }
        );
        // The same claim from the acks the leader actually folded, and only
        // from matchmakers the generation names: the core adds `ack.matchmaker`
        // to its tally after checking its *own belief* about the set, so an
        // ack from a matchmaker outside the authoritative generation is
        // exactly what this catches.
        let acked = self
            .gc_acks
            .iter()
            .filter(|((leader, floor), _)| *leader == node.0 && *floor >= *watermark)
            .flat_map(|(_, matchmakers)| matchmakers.iter().copied())
            .filter(|matchmaker| self.member_of(generation, *matchmaker))
            .collect::<BTreeSet<u64>>()
            .len();
        assert_always!(
            acked >= quorum,
            "gc: a floor is effective only once a quorum of the generation's matchmakers acked",
            {
                "node" => node.0,
                "round" => watermark.round,
                "generation" => generation,
                "acked" => acked,
                "quorum" => quorum
            }
        );
        // GC invariant 3: a retirable acceptor is `members(H_b) \ C_b`, so it
        // was a member of a configuration this floor forgets. Without this,
        // a leader naming an arbitrary spare has its word taken for it: the
        // operator parks a healthy node for the run and the convergence
        // oracle excuses it *because the leader named it*.
        if let Some(prior) = self.prior_members(*watermark) {
            let outsider = retired.iter().find(|n| !prior.contains(&n.0)).map(|n| n.0);
            assert_always!(
                outsider.is_none(),
                "gc: a retirable acceptor was a member of a prior configuration",
                {
                    "node" => node.0,
                    "round" => watermark.round,
                    "retired" => outsider.unwrap_or(u64::MAX),
                    "prior" => prior.len()
                }
            );
        }
        // GC invariant 4: nothing the configuration in force needs is retired.
        if let Some(config) = config {
            let inside = retired.iter().find(|n| config.contains(**n)).map(|n| n.0);
            assert_always!(
                inside.is_none(),
                "gc: a retired acceptor is outside the configuration in force",
                { "node" => node.0, "round" => watermark.round, "retired" => inside.unwrap_or(u64::MAX) }
            );
        }
        self.effective_floor = self.effective_floor.max(*watermark);
        for n in retired {
            self.retired_by_gc.insert(n.0);
        }
        reach_once!(
            self.gc_effective,
            "gc: a leader's floor becomes effective at a matchmaker quorum"
        );
        if !retired.is_empty() {
            reach_once!(
                self.gc_retired_any,
                "gc: an effective floor names retirable acceptors"
            );
        }
    }

    /// A node answered an operator `Retire`.
    pub(super) fn retire_acked(&mut self, accepted: bool) {
        if accepted {
            reach_once!(self.retire_accepted, "gc: a node accepts its retirement");
        } else {
            reach_once!(
                self.retire_refused,
                "gc: a node still in its configuration refuses to retire"
            );
        }
    }

    /// A node shut down for good on an operator's retirement.
    pub(super) fn retired(&mut self, node: NodeId) {
        assert_always!(
            self.retired_by_gc.contains(&node.0),
            "gc: a node retires only after an effective floor named it retirable",
            { "node" => node.0 }
        );
        reach_once!(
            self.node_retired,
            "gc: a retired acceptor is genuinely shut down"
        );
    }

    // ---- generations (#125) -------------------------------------------------

    /// A node adopted `set` as its matchmaker set.
    pub(super) fn set_learned(&mut self, node: NodeId, set: &MatchmakerSet) {
        let chosen = self.is_chosen(set);
        assert_always!(
            chosen,
            "generation: a node adopts only a chosen matchmaker set",
            { "node" => node.0, "generation" => set.generation.0 }
        );
        reach_once!(
            self.set_learned,
            "generation: a node learns a later matchmaker generation"
        );
    }

    /// A node started a handover from `old` (finishing one when the target
    /// is `old`'s own membership).
    pub(super) fn reconfigurer_started(
        &mut self,
        node: NodeId,
        old: &MatchmakerSet,
        target: &[MatchmakerId],
    ) {
        self.reconfigurer_trace.entry(node.0).or_default().0 += 1;
        let chosen = self.is_chosen(old);
        assert_always!(
            chosen,
            "generation: a handover replaces a chosen generation",
            { "node" => node.0, "generation" => old.generation.0 }
        );
        // A fresh attempt folds its own freeze acks: an earlier attempt by
        // this node (one its incarnation lost, or one abandoned) may have
        // folded a different subset, and the reconstruction is judged
        // against exactly the quorum this attempt reconstructs from.
        self.stop_acks.clear(node.0, old.generation.0);
        let members: Vec<u64> = old.members().iter().map(|m| m.0).collect();
        if target.iter().map(|m| m.0).collect::<Vec<_>>() == members {
            reach_once!(
                self.reconfigurer_finishing,
                "generation: a node finishes a handover whose reconfigurer died"
            );
        }
        reach_once!(
            self.reconfigurer_started_flag,
            "generation: a matchmaker-set handover starts"
        );
    }

    /// A node told a straggling matchmaker the chosen successor.
    pub(super) fn successor_republished(&mut self, node: NodeId, set: &MatchmakerSet) {
        let chosen = self.is_chosen(set);
        assert_always!(
            chosen,
            "generation: a republished successor is the chosen set",
            { "node" => node.0, "generation" => set.generation.0 }
        );
        reach_once!(
            self.successor_republished,
            "generation: a node republishes the chosen set to a straggling matchmaker"
        );
    }

    /// The reconfigurer folded `reply` from `matchmaker` into `step`.
    #[allow(clippy::too_many_lines)]
    pub(super) fn reconfigurer_step(
        &mut self,
        node: NodeId,
        matchmaker: MatchmakerId,
        reply: &ReconfigureReply,
        step: &ReconfigurerStep,
    ) {
        if !matches!(step, ReconfigurerStep::Ignored) {
            self.reconfigurer_trace.entry(node.0).or_default().2 =
                format!("{step:?}").chars().take(48).collect();
        }
        if let ReconfigureReply::Accepted {
            matchmaker: voter,
            generation,
            ballot,
        } = reply
        {
            self.decree_voters
                .entry((generation.0, *ballot))
                .or_default()
                .insert(voter.0);
        }
        if let ReconfigureReply::Stopped {
            generation,
            after,
            next,
            registries,
            ..
        } = reply
            && matches!(step, ReconfigurerStep::Stopped { .. })
        {
            let frozen = registries
                .iter()
                .map(|(journal, registry)| {
                    let snapshot: Vec<(Ballot, Registration)> =
                        registry.history.clone().into_iter().collect();
                    (*journal, (registry.gc_watermark, snapshot))
                })
                .collect();
            self.stop_acks.insert(
                (node.0, generation.0, matchmaker.0),
                frozen,
                after.is_none(),
                next.is_none(),
            );
        }
        match step {
            // The counted-but-short folds: progress the driver's stall
            // clock reads, with nothing new to judge (the quorum-closing
            // fold of each phase carries the claim).
            ReconfigurerStep::Ignored
            | ReconfigurerStep::Stopped { .. }
            | ReconfigurerStep::Bootstrapped { .. }
            | ReconfigurerStep::Promised { .. }
            | ReconfigurerStep::Accepted { .. }
            | ReconfigurerStep::Published { .. } => {}
            ReconfigurerStep::Deciding { .. } => {
                // Invariant 7: every proposed member holds the bootstrap.
                if let ReconfigureReply::Bootstrapped { set, .. } = reply {
                    let members: Vec<u64> = set.members().iter().map(|m| m.0).collect();
                    let holders = self
                        .bootstrapped
                        .holders(&(set.generation.0, members.clone()));
                    assert_always!(
                        holders == members.len(),
                        "generation: the decree opens only once every proposed member holds the bootstrap",
                        { "node" => node.0, "generation" => set.generation.0, "holders" => holders, "members" => members.len() }
                    );
                }
                reach_once!(
                    self.reconfigurer_deciding,
                    "generation: the successor decree opens over the old generation"
                );
            }
            ReconfigurerStep::Proposing {
                ballot,
                members,
                adopted,
            } => {
                // The generation decided over comes from the `Promised`
                // reply that closed Phase 1, never from the successor's
                // number.
                if let ReconfigureReply::Promised { generation, .. } = reply {
                    let proposed: Vec<u64> = members.iter().map(|m| m.0).collect();
                    let first = self
                        .decree_proposals
                        .entry((generation.0, *ballot))
                        .or_insert_with(|| proposed.clone());
                    assert_always!(
                        *first == proposed,
                        "generation: one value is voted per decree ballot",
                        {
                            "node" => node.0,
                            "generation" => generation.0,
                            "round" => ballot.round,
                            "first" => first.len(),
                            "proposed" => proposed.len()
                        }
                    );
                }
                if *adopted {
                    reach_once!(
                        self.reconfigurer_adopted,
                        "generation: a reconfigurer adopts a competing proposal already voted"
                    );
                }
            }
            ReconfigurerStep::Preempted { .. } => {
                reach_once!(
                    self.reconfigurer_preempted,
                    "generation: a preempted decree reopens above the refusing promise"
                );
            }
            ReconfigurerStep::Chosen { successor } => {
                let members: Vec<u64> = successor.members().iter().map(|m| m.0).collect();
                self.bind_set(successor.generation.0, &members, "chosen");
                // The decree itself, from the wire: the set published is the
                // value this node put to the vote, and a majority of the old
                // generation voted for it at that one ballot. Until now the
                // only proof of this lived in the sans-IO model checker,
                // which never runs the driver's plumbing, its crash seams or
                // its abandon path. The publishing reply names both
                // coordinates, so the decree judged is exactly the one that
                // just closed; skipped where the proposal was not observed (a
                // `finish` that adopts an already-chosen set publishes
                // without ever proposing).
                if let ReconfigureReply::Accepted {
                    generation, ballot, ..
                } = reply
                    && let Some(proposed) = self.decree_proposals.get(&(generation.0, *ballot))
                {
                    let generation = generation.0;
                    let proposed = proposed.clone();
                    let voted = self
                        .decree_voters
                        .get(&(generation, *ballot))
                        .map_or(0, BTreeSet::len);
                    let quorum = self.quorum(generation);
                    assert_always!(
                        proposed == members && voted >= quorum,
                        "generation: a chosen set was voted by a majority of its old generation at one ballot",
                        {
                            "node" => node.0,
                            "generation" => generation,
                            "round" => ballot.round,
                            "proposed" => proposed.len(),
                            "chosen" => members.len(),
                            "voted" => voted,
                            "quorum" => quorum
                        }
                    );
                    reach_once!(
                        self.decree_checked,
                        "generation: a chosen set is checked against its decree's votes"
                    );
                }
                reach_once!(
                    self.reconfigurer_chosen,
                    "generation: a successor set is chosen by the decree"
                );
            }
            ReconfigurerStep::Done { .. } => {
                reach_once!(
                    self.reconfigurer_done,
                    "generation: a matchmaker-set handover completes"
                );
            }
            ReconfigurerStep::Superseded { successor } => {
                let chosen = self.is_chosen(successor);
                assert_always!(
                    chosen,
                    "generation: a superseded reconfigurer adopts the chosen set",
                    { "node" => node.0, "generation" => successor.generation.0 }
                );
                reach_once!(
                    self.reconfigurer_superseded,
                    "generation: a late reconfigurer adopts the successor already chosen"
                );
            }
        }
    }
}

/// The bodies of the matchmaker callbacks of `NodeAudit`'s `Audit` impl
/// (#417) that read more of the shared state than [`MatchmakerAudit`]: the
/// impl delegates each one here under the state's lock.
impl AuditState {
    pub(super) fn match_registered(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        registration: &Registration,
    ) {
        self.matchmaker.registered(matchmaker, ballot, registration);
        let config = &registration.config;
        // The per-ballot configuration the quorum oracles count over: bound
        // at its durable registration, before any leader could exercise it.
        self.bind_config(ballot, config);
    }

    pub(super) fn gc_request_sent(&mut self, node: NodeId, watermark: Ballot, fence: Option<Slot>) {
        // The leader re-sends its request every beat and the licence is judged
        // once per `(node, watermark)`, so ask before deriving: everything
        // below is an O(fence x members) walk whose answer would be dropped.
        // The minter relinquished the ballot: a successor collects at it.
        let handed = self
            .authorities
            .get(&(watermark.round, watermark.node.0))
            .is_some_and(|a| a.retired.contains(&watermark.node.0));
        if !self.matchmaker.gc_needs_licence(node, watermark) {
            self.matchmaker
                .gc_requested(node, watermark, handed, fence, None, "");
            return;
        }
        // GC invariant 1, re-derived from the audit's own durable fold: a
        // Phase-2 quorum of the configuration bound to the leader's ballot
        // holds every slot up to the fence — a durable record carrying the
        // decided value where the audit knows one (any record otherwise), or
        // a compaction floor above the slot (the below-floor `Nack` is the
        // acceptor's "already chosen"). `None` when the configuration is not
        // known yet (then nothing is judged).
        let mut uncovered: Vec<String> = Vec::new();
        let covered = self.config_of(watermark).cloned().map(|config| {
            let holders: BTreeSet<NodeId> = config
                .members()
                .iter()
                .filter(|m| {
                    let m = m.0;
                    match fence {
                        None => true,
                        Some(fence) => {
                            let gap = (0..=fence.0).find(|slot| {
                                let floor = self.floor.get(&m).map_or(0, |f| f.now);
                                if floor > *slot {
                                    return false;
                                }
                                match (self.persisted.get(&(m, *slot)), self.chosen.get(slot)) {
                                    (Some(held), Some(decided)) => held != decided,
                                    (Some(_), None) => false,
                                    (None, _) => true,
                                }
                            });
                            if let Some(slot) = gap {
                                let why = match self.persisted.get(&(m, slot)) {
                                    Some(_) => "mismatch",
                                    None => "missing",
                                };
                                uncovered.push(format!("{m}@{slot}:{why}"));
                            }
                            gap.is_none()
                        }
                    }
                })
                .copied()
                .collect();
            // Judged by the configuration's own quorum system, never by a
            // count: the custody claim the leader's GC rests on is the same
            // Phase-2 quorum question `Collector::covered` asks in the core.
            config.has_phase2_quorum(&holders)
        });
        self.matchmaker.gc_requested(
            node,
            watermark,
            handed,
            fence,
            covered,
            &uncovered.join(","),
        );
    }

    pub(super) fn gc_step(&mut self, node: NodeId, ack: &GcAck, step: &GcStep) {
        let config = self.config_of(ack.watermark).cloned();
        self.matchmaker.gc_step(node, ack, step, config.as_ref());
    }

    pub(super) fn reconfigure_matchmakers_acked(&mut self, refusal: Option<MatchmakersRefusal>) {
        if refusal.is_none() {
            reach_once!(
                self.reconfigure_matchmakers_started,
                "generation: a client's matchmaker reconfiguration is started"
            );
        } else {
            reach_once!(
                self.reconfigure_matchmakers_refused,
                "generation: a client's matchmaker reconfiguration is refused"
            );
        }
    }

    pub(super) fn retire_acked(&mut self, refusal: Option<RetireRefusal>) {
        // The refusal legs the shared gate cannot tell apart. `not_collected`
        // is the one #123's rule turns from a workload discipline into a
        // protocol answer: the node is outside the configuration it believes
        // in force, and still refuses, because nothing proves the cluster is
        // done with the configurations it *was* in.
        match refusal {
            Some(RetireRefusal::NotCollected) => reach_once!(
                self.retire_not_collected,
                "gc: a retirement is refused for want of an effective floor"
            ),
            // The freshness leg (#165): outside the configuration it
            // believes in force, but that belief predates the floor, so
            // the node cannot tell whether the configuration the floor
            // kept names it.
            Some(RetireRefusal::Stale) => reach_once!(
                self.retire_stale,
                "gc: a retirement is refused on a belief older than the floor"
            ),
            Some(RetireRefusal::Leader) => reach_once!(
                self.retire_leader,
                "gc: a retirement is refused by the sitting leader"
            ),
            _ => {}
        }
        self.matchmaker.retire_acked(refusal.is_none());
    }

    pub(super) fn retired(&mut self, node: NodeId) {
        self.matchmaker.retired(node);
        self.retired.insert(node.0);
    }

    pub(super) fn matchmaking_completed(
        &mut self,
        node: NodeId,
        ballot: Ballot,
        prior: &[AcceptorConfig],
        watermark: Ballot,
        registered_by: usize,
        disagreements: u64,
    ) {
        self.matchmaker
            .completed(node, ballot, prior, watermark, registered_by, disagreements);
        self.note_prior(node.0, ballot, prior);
    }

    pub(super) fn reconfigure_acked(&mut self, node: NodeId, result: ReconfigureResult) {
        match result {
            ReconfigureResult::Started(_) => {
                assert_always!(
                    self.matchmaker.has_matchmakers(),
                    "reconfiguration: a deployment without matchmakers never starts one",
                    { "node" => node.0 }
                );
                reach_once!(
                    self.reconfigure_started,
                    "reconfiguration: a reconfiguration request is started"
                );
            }
            ReconfigureResult::Refused(_) | ReconfigureResult::NotLeader(_) => {
                reach_once!(
                    self.reconfigure_refused,
                    "reconfiguration: a reconfiguration request is refused or redirected"
                );
            }
        }
    }

    pub(super) fn matchmaker_boot_refused(
        &mut self,
        matchmaker: MatchmakerId,
        refusal: BootRefusal,
    ) {
        match refusal {
            // #183: the library, not the harness, keeps a wiped registry out.
            BootRefusal::Amnesia => self.matchmaker.boot_refused(matchmaker.0),
            BootRefusal::AlreadyFormatted => {
                assert_always!(
                    false,
                    "matchmaker: a first boot never meets a formatted registry",
                    { "matchmaker" => matchmaker.0 }
                );
            }
            // #207: an edited configuration is refused, then restored.
            BootRefusal::ConfigMismatch => {
                reach_once!(
                    self.matchmaker_config_mismatch_refused,
                    "matchmaker: the library refuses a restart under an edited configuration"
                );
            }
        }
    }
}
