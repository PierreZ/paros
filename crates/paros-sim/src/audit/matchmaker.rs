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
//!
//! # Garbage collection (#123), where each invariant is judged
//!
//! 1. **A GC request is licensed by the fence** — at the first send of each
//!    leadership's request: a Phase-2 quorum of the configuration bound to
//!    the leader's ballot durably holds every slot up to the election fence
//!    (a record, or a floor above it) — the forgettability condition the
//!    leader claims (`paros_core` `node/gc.rs`), re-derived from the audit's
//!    own fold of durable records.
//! 2. **A floor reported effective is held by a matchmaker quorum** — at the
//!    leader's `Effective` step: the durable watermarks this fold holds for
//!    the addressed generation's set reach the floor at a quorum. This is
//!    invariant 5 of the issue (retirement never runs ahead of the acks).
//! 3. **No campaign filters below the effective floor** — at every
//!    completion: the maximum watermark used is at or above the highest
//!    floor proven effective, so no collected configuration ever re-enters
//!    an `H` (invariant 6: the max reported watermark is what makes partial
//!    GC safe).
//! 4. **A retired acceptor is outside the configuration in force**, and a
//!    node that stays down retired only after an effective floor named it.
//!
//! # Generations (#125), where each invariant is judged
//!
//! 1. **One authoritative set per generation** — at every activation, chosen
//!    step, successor link and learned set: a generation's members never
//!    disagree, anywhere.
//! 2. **A frozen generation never registers again** — at every registration
//!    (the folded durable phase is not `Stopped`) and every restart (a
//!    frozen phase recovers frozen, or a later generation).
//! 3. **A reconstruction is complete** — at the reconfigurer's bootstrap:
//!    the union of the frozen quorum's durable registries above their
//!    maximum watermark, and every completed registration of the replaced
//!    generation above that watermark is in it.
//! 4. **The watermark never regresses across a generation change** — at
//!    every activation.
//! 5. **Generation fencing** — at every registration reply: served only by
//!    a matchmaker active for exactly that generation.
//! 6. **Successor metadata names the chosen set** — at every persisted
//!    successor link and every set a node adopts.
//! 7. **A chosen set was fully bootstrapped first** — at the decree's
//!    opening, every proposed member durably holds the bootstrap.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes};
use paros::{
    AcceptorConfig, Ballot, GcAck, GcStep, MatchRefusal, MatchmakerHardState, MatchmakerId,
    MatchmakerPhase, MatchmakerSet, NodeId, ReconfigureReply, ReconfigurerStep, Registration, Seam,
    Slot,
};

/// One matchmaker's folded registry.
#[derive(Default)]
struct Registry {
    /// Durable registrations, exactly as the disk holds them (pruned by GC).
    registered: BTreeMap<Ballot, Registration>,
    /// The durable watermark.
    watermark: Ballot,
    /// Ballots a `Registered` reply escaped for, with the history each first
    /// answer carried (a re-answer may only shrink it).
    replied: BTreeMap<Ballot, Vec<(Ballot, Registration)>>,
    /// Boots observed (1 after the first).
    boots: u64,
    /// The durable generation state last persisted (or recovered):
    /// `(generation, phase)`. `None` until the first fold.
    generation: Option<(u64, MatchmakerPhase)>,
}

/// One candidate's matchmaking phase for one ballot, as the fold sees it.
/// One `Registered` reply as sent: the history it names and the watermark it
/// reports.
type ReplyCopy = (Vec<(Ballot, Registration)>, Ballot);
/// A stopped matchmaker's frozen registry: `(generation, watermark, history)`.
type FrozenRegistry = (u64, Ballot, Vec<(Ballot, Registration)>);

#[derive(Default)]
struct Campaign {
    /// `C_b`, the configuration the campaign registers.
    config: Option<AcceptorConfig>,
    /// The `Registered` replies the matchmakers sent this candidate for this
    /// ballot: `matchmaker -> [(history, watermark)]`, every copy in send
    /// order (a re-sent request is answered again, possibly from a registry
    /// a floor was raised on in between). What the candidate may have
    /// received; it folds whichever copy reaches it first.
    replies: BTreeMap<u64, Vec<ReplyCopy>>,
    /// The floor proven effective at a matchmaker quorum when the campaign
    /// opened: every reply it can fold was sent after that, so quorum
    /// intersection puts the floor into the watermark it uses.
    floor_at_start: Ballot,
    /// The matchmakers whose replies the candidate reported folding.
    registered_by: BTreeSet<u64>,
    /// A refusal was folded: the campaign is dead.
    refused: bool,
    /// The matchmaking closed with a quorum; Phase 1 is licensed.
    completed: bool,
    /// Opened by `RawNode::reconfigure`: its registration *is* the next
    /// effective configuration, so the stale-belief rule does not apply.
    reconfiguration: bool,
    /// The matchmaker generation the campaign addressed.
    generation: u64,
}

/// The fold and the flag set (independent sticky bits per gate).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct MatchmakerAudit {
    registries: BTreeMap<u64, Registry>,
    /// Every `(matchmaker, ballot) -> configuration` ever folded, **never**
    /// pruned: the write-once ledger a GC'd-then-reused ballot would trip.
    ever: BTreeMap<(u64, Ballot), Registration>,
    /// Every candidate's matchmaking phase, keyed by `(node, ballot)`.
    campaigns: BTreeMap<(u64, Ballot), Campaign>,
    /// The bootstrap matchmaker set (generation 0), from the boot reports.
    bootstrap_set: Option<Vec<u64>>,
    /// The authoritative members of every generation seen (#125,
    /// invariant 1): generation 0 is the bootstrap set, each later one the
    /// value the successor decree chose.
    sets: BTreeMap<u64, Vec<u64>>,
    /// Per matchmaker: the frozen registry it answered a `Stop` with,
    /// `(generation, watermark, registry)`.
    frozen: BTreeMap<u64, FrozenRegistry>,
    /// Per `(node, generation)`: the matchmakers whose `Stopped` replies the
    /// reconfigurer folded.
    stop_acks: BTreeMap<(u64, u64), BTreeSet<u64>>,
    /// Refusals folded, by kind (diagnostics only).
    refusal_counts: BTreeMap<&'static str, u64>,
    /// Per node: handovers started, abandoned, and the last step folded
    /// (diagnostics only).
    reconfigurer_trace: BTreeMap<u64, (u64, u64, String)>,
    /// Per matchmaker: the last scalars persisted, summarized (diagnostics).
    scalars_note: BTreeMap<u64, String>,
    reconfigurer_aborted: bool,
    reconfigurer_backed_off: bool,
    /// Per proposed `(generation, members)`: the matchmakers that durably
    /// hold its bootstrap.
    bootstrapped: BTreeMap<(u64, Vec<u64>), BTreeSet<u64>>,
    /// The highest GC floor proven effective at a matchmaker quorum.
    effective_floor: Ballot,
    /// Nodes an effective floor named retirable.
    retired_by_gc: BTreeSet<u64>,
    /// `(node, watermark)` GC requests whose licence was judged (once each).
    gc_judged: BTreeSet<(u64, Ballot)>,
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
    clock_reasked: bool,
    refloored: bool,
    campaign_stale: bool,
    effective_checked: bool,
    ledger_agreement_checked: bool,
    /// Every configuration some completed campaign or started reconfiguration
    /// put on the wire — the only sources a candidate may learn a belief from
    /// (a leader's `Prepare`, `Heartbeat` or `Relinquish`), beside the
    /// bootstrap.
    wire_configs: BTreeSet<AcceptorConfig>,
    /// The bootstrap configuration, from the boot reports.
    bootstrap: Option<AcceptorConfig>,
    /// Per node: the highest round a `Stale` refusal named to it, which its
    /// next campaign must open strictly above.
    refused_floor: BTreeMap<u64, u64>,
    campaign_empty_history: bool,
    campaign_union_several: bool,
    campaign_disagreeing_histories: bool,
    resend_skipped: bool,
    request_resent: bool,
    reconfiguration_opened: bool,
    // --- garbage collection (#123) ---
    gc_requested: bool,
    gc_effective: bool,
    gc_retired_any: bool,
    gc_refused: bool,
    gc_resend_skipped: bool,
    gc_ack_dropped: bool,
    retire_accepted: bool,
    retire_refused: bool,
    node_retired: bool,
    // --- generations (#125) ---
    refused_stopped: bool,
    refused_stopped_with_successor: bool,
    refused_generation: bool,
    refused_inactive: bool,
    set_learned: bool,
    reconfigurer_started_flag: bool,
    reconfigurer_finishing: bool,
    reconfigurer_bootstrapping: bool,
    reconfigurer_deciding: bool,
    reconfigurer_adopted: bool,
    reconfigurer_preempted: bool,
    reconfigurer_chosen: bool,
    reconfigurer_done: bool,
    reconfigurer_superseded: bool,
    reconfigurer_resend_skipped: bool,
    reconfigure_reply_dropped: bool,
    matchmaker_frozen: bool,
    matchmaker_bootstrapped_flag: bool,
    matchmaker_activated_flag: bool,
    matchmaker_departed: bool,
    matchmaker_refused_step: bool,
    matchmaker_lost: bool,
    successor_republished: bool,
    reconstruction_checked: bool,
    handover_with_prior_registrations: bool,
}

/// The union of every distinct configuration the registering replies name
/// at or above `watermark` (invariant 3's expectation), and the set of
/// distinct ballot sequences the histories had (to see whether they
/// differed at all).
/// Every way to pick one reply per registering matchmaker (the cartesian
/// product of the copies each one sent): the candidate folded exactly one
/// of them.
fn reply_combinations<'a, T>(lists: &[&'a Vec<T>]) -> Vec<Vec<&'a T>> {
    let mut combos: Vec<Vec<&'a T>> = vec![Vec::new()];
    for list in lists {
        combos = combos
            .iter()
            .flat_map(|prefix| {
                list.iter().map(move |item| {
                    let mut next = prefix.clone();
                    next.push(item);
                    next
                })
            })
            .collect();
    }
    combos
}

/// Invariants 2 and 3 of a completed campaign, re-derived from the replies
/// the registering matchmakers sent. A matchmaker may have answered a
/// re-sent request a second time from a registry a floor was raised on in
/// between; the candidate folds whichever copy reached it first, so the
/// claim is existential: *some* choice of one reply per registering
/// matchmaker yields exactly the watermark and the union reported. Returns
/// that choice (empty when none matches, after the violations recorded).
fn folded_replies<'a>(
    campaign: &'a Campaign,
    node: NodeId,
    ballot: Ballot,
    prior: &[AcceptorConfig],
    watermark: Ballot,
) -> Vec<&'a ReplyCopy> {
    let per_matchmaker: Vec<&Vec<ReplyCopy>> = campaign
        .registered_by
        .iter()
        .filter_map(|m| campaign.replies.get(m))
        .collect();
    let mut max_seen = Ballot::default();
    let mut watermark_matches = false;
    let mut chosen: Option<Vec<&ReplyCopy>> = None;
    for combo in reply_combinations(&per_matchmaker) {
        let max = combo.iter().map(|(_, w)| *w).max().unwrap_or_default();
        max_seen = max_seen.max(max);
        if max != watermark {
            continue;
        }
        watermark_matches = true;
        let (expected, _) = union_above(&combo, max);
        let union_matches = prior.len() == expected.len()
            && expected.iter().all(|c| prior.contains(c))
            && prior.iter().all(|c| expected.contains(&c));
        if union_matches {
            chosen = Some(combo);
            break;
        }
    }
    assert_always!(
        watermark_matches,
        "matchmaking: the watermark used is the maximum reported",
        {
            "node" => node.0,
            "round" => ballot.round,
            "used_round" => watermark.round,
            "max_round" => max_seen.round
        }
    );
    assert_always!(
        chosen.is_some(),
        "matchmaking: the prior set is the union of every registering reply above the watermark",
        {
            "node" => node.0,
            "round" => ballot.round,
            "reported" => prior.len(),
            "registering" => per_matchmaker.len()
        }
    );
    chosen.unwrap_or_default()
}

/// Whether a matchmaker whose folded generation scalars are `folded` serves
/// `generation`: the core resolves `Fresh` scalars — no generation write yet,
/// or a scalar write (a decree vote) that left the phase untouched — by
/// bootstrap membership, so a bootstrap member is active for generation 0
/// and a spare is inactive.
fn serves_generation(
    folded: Option<(u64, MatchmakerPhase)>,
    generation: u64,
    bootstrap_member: bool,
) -> bool {
    match folded {
        Some((generation_no, MatchmakerPhase::Active)) => generation_no == generation,
        Some((0, MatchmakerPhase::Fresh)) | None => generation == 0 && bootstrap_member,
        Some(_) => false,
    }
}

/// The round of the first history entry that disagrees with what *any*
/// matchmaker ever durably registered for that ballot, if one does.
fn ledger_disagreement(
    ever: &BTreeMap<(u64, Ballot), Registration>,
    history: &[(Ballot, Registration)],
) -> Option<u64> {
    history
        .iter()
        .find(|(b, r)| {
            ever.range((0, *b)..=(u64::MAX, *b))
                .any(|((_, eb), er)| *eb == *b && er != r)
        })
        .map(|(b, _)| b.round)
}

/// A phase, for the detail maps.
fn phase_name(phase: MatchmakerPhase) -> &'static str {
    match phase {
        MatchmakerPhase::Fresh => "fresh",
        MatchmakerPhase::Inactive => "inactive",
        MatchmakerPhase::Active => "active",
        MatchmakerPhase::Stopped => "stopped",
    }
}

fn union_above<'a>(
    replies: &[&'a ReplyCopy],
    watermark: Ballot,
) -> (Vec<&'a AcceptorConfig>, BTreeSet<Vec<Ballot>>) {
    let mut expected: Vec<&AcceptorConfig> = Vec::new();
    let mut histories: BTreeSet<Vec<Ballot>> = BTreeSet::new();
    for (history, _) in replies {
        histories.insert(history.iter().map(|(b, _)| *b).collect());
        for (b, registration) in history {
            let config = &registration.config;
            if *b >= watermark && !expected.contains(&config) {
                expected.push(config);
            }
        }
    }
    (expected, histories)
}

impl MatchmakerAudit {
    /// The deployment's bootstrap matchmaker set, as the boot reports
    /// declared it.
    pub(super) fn note_deployment(&mut self, matchmakers: &[MatchmakerId]) {
        let ids: Vec<u64> = matchmakers.iter().map(|m| m.0).collect();
        let known = self
            .bootstrap_set
            .get_or_insert_with(|| ids.clone())
            .clone();
        assert_always!(
            known == ids,
            "matchmaker: every node derives the same matchmaker set",
            { "reported" => ids.len(), "folded" => known.len() }
        );
        if !ids.is_empty() {
            self.deployed = true;
            self.sets.entry(0).or_insert(ids);
        }
    }

    /// Whether the deployment names matchmakers at all.
    pub(super) fn has_matchmakers(&self) -> bool {
        self.bootstrap_set.as_ref().is_some_and(|s| !s.is_empty())
    }

    /// The matchmaker quorum of `generation`'s set (the bootstrap set until
    /// a generation is known).
    fn quorum(&self, generation: u64) -> usize {
        let members = self.sets.get(&generation).map_or_else(
            || self.bootstrap_set.clone().unwrap_or_default(),
            Clone::clone,
        );
        members.len() / 2 + 1
    }

    /// Whether `matchmaker` is a member of `generation`'s set (unknown
    /// generations count nobody).
    fn member_of(&self, generation: u64, matchmaker: u64) -> bool {
        self.sets
            .get(&generation)
            .is_some_and(|m| m.contains(&matchmaker))
    }

    /// Invariant 1 of #125: record `members` as `generation`'s authoritative
    /// set, or check it against the one already known.
    fn bind_set(&mut self, generation: u64, members: &[u64], source: &'static str) {
        let known = self
            .sets
            .entry(generation)
            .or_insert_with(|| members.to_vec());
        assert_always!(
            known.as_slice() == members,
            "generation: at most one matchmaker set is authoritative per generation",
            {
                "generation" => generation,
                "source" => source,
                "known" => known.len(),
                "reported" => members.len()
            }
        );
    }

    /// Whether `set` is the chosen (authoritative) set of its generation.
    fn is_chosen(&self, set: &MatchmakerSet) -> bool {
        let members: Vec<u64> = set.members.iter().map(|m| m.0).collect();
        self.sets.get(&set.generation.0) == Some(&members)
    }

    /// Whether an effective floor named `node` retirable.
    pub(super) fn retired_by_gc(&self, node: u64) -> bool {
        self.retired_by_gc.contains(&node)
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
            .map(|(_, r)| &r.config)
    }

    /// A matchmaker booted from its durable registry.
    pub(super) fn recovered(
        &mut self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        phase: MatchmakerPhase,
        registry: &[(Ballot, Registration)],
        gc_watermark: Ballot,
    ) {
        self.deployed = true;
        // A booted matchmaker names an authoritative set for its generation
        // (a spare names the bootstrap set it is outside of).
        if phase != MatchmakerPhase::Inactive {
            let members: Vec<u64> = set.members.iter().map(|m| m.0).collect();
            self.bind_set(set.generation.0, &members, "recovered");
        }
        let entry = self.registries.entry(matchmaker.0).or_default();
        entry.boots += 1;
        if entry.boots > 1 {
            // Invariant 2 of #125, across the restart: a frozen generation
            // recovers frozen (or a later generation, activated since); an
            // activated generation recovers itself.
            if let Some((generation_no, folded)) = entry.generation {
                let thawed = folded == MatchmakerPhase::Stopped
                    && set.generation.0 == generation_no
                    && phase != MatchmakerPhase::Stopped;
                assert_always!(
                    !thawed,
                    "generation: a frozen generation never thaws",
                    { "matchmaker" => matchmaker.0, "generation" => generation_no }
                );
                assert_always!(
                    set.generation.0 >= generation_no,
                    "generation: a restart never regresses the generation",
                    { "matchmaker" => matchmaker.0, "recovered" => set.generation.0, "folded" => generation_no }
                );
            }
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
                        "read_back" => read_back.map_or(0, |r| r.config.members.len())
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
        entry.generation = Some((set.generation.0, phase));
    }

    /// A registration became durable.
    pub(super) fn registered(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        registration: &Registration,
    ) {
        let config = registration;
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 2 of #125: a frozen matchmaker registers nothing again.
        assert_always!(
            !matches!(entry.generation, Some((_, MatchmakerPhase::Stopped))),
            "generation: a frozen matchmaker never registers again",
            { "matchmaker" => matchmaker.0, "round" => ballot.round }
        );
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
        generation: u64,
        history: &[(Ballot, Registration)],
        gc_watermark: Ballot,
    ) {
        let bootstrap_member = self
            .bootstrap_set
            .as_ref()
            .is_some_and(|s| s.contains(&matchmaker.0));
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 5 of #125: served only by the active generation. The
        // core resolves `Fresh` scalars — no generation write yet, or a
        // scalar write (a decree vote) that left the phase untouched — by
        // bootstrap membership: a bootstrap member is active for
        // generation 0, a spare is inactive. Mirror that resolution here.
        let active = serves_generation(entry.generation, generation, bootstrap_member);
        let (folded_generation, folded_phase) = entry
            .generation
            .map_or((u64::MAX, "unknown"), |(g, p)| (g, phase_name(p)));
        assert_always!(
            active,
            "generation: a registration is served only by the active generation",
            {
                "matchmaker" => matchmaker.0,
                "generation" => generation,
                "round" => ballot.round,
                "folded_generation" => folded_generation,
                "folded_phase" => folded_phase
            }
        );
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
        let expected: Vec<(Ballot, Registration)> = entry
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
        // The registry protocol itself, judged on every reply: each entry
        // the history names agrees with what *any* matchmaker durably
        // registered for that ballot. Together with the completion-time
        // `disagreements == 0`, this is the claim that two honest
        // matchmakers never disagree — not merely that a disagreeing pair
        // would still be safe (review of #132).
        let disagreeing = ledger_disagreement(&self.ever, history);
        assert_always!(
            disagreeing.is_none(),
            "matchmaker: a reply's history agrees with every matchmaker's ledger",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "disagreeing_round" => disagreeing.unwrap_or(0)
            }
        );
        if !history.is_empty() {
            reach_once!(
                self.ledger_agreement_checked,
                "matchmaker: a reply's history is checked against the other ledgers"
            );
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
            .entry(matchmaker.0)
            .or_default()
            .push((history.to_vec(), gc_watermark));
    }

    /// A refusal is leaving: it must name the registry's own state.
    pub(super) fn refused(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        let kind = match &refusal {
            MatchRefusal::Stale { .. } => "stale",
            MatchRefusal::BelowWatermark { .. } => "below_watermark",
            MatchRefusal::Stopped { .. } => "stopped",
            MatchRefusal::Generation { .. } => "generation",
            MatchRefusal::Inactive => "inactive",
        };
        *self.refusal_counts.entry(kind).or_default() += 1;
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
            MatchRefusal::Stopped { successor } => {
                // A frozen generation answers frozen — and a successor it
                // names is the chosen one (invariant 6 of #125).
                assert_always!(
                    matches!(entry.generation, Some((_, MatchmakerPhase::Stopped))),
                    "generation: a stopped refusal comes from a durably frozen matchmaker",
                    { "matchmaker" => matchmaker.0, "round" => ballot.round }
                );
                match successor {
                    Some(successor) => {
                        let chosen = self.is_chosen(&successor);
                        assert_always!(
                            chosen,
                            "generation: a successor named to a proposer is the chosen set",
                            { "matchmaker" => matchmaker.0, "generation" => successor.generation.0 }
                        );
                        reach_once!(
                            self.refused_stopped_with_successor,
                            "generation: a frozen matchmaker points a proposer at its successor"
                        );
                    }
                    None => reach_once!(
                        self.refused_stopped,
                        "generation: a frozen matchmaker refuses a proposer before a successor is chosen"
                    ),
                }
            }
            MatchRefusal::Generation { current } => {
                let chosen = self.is_chosen(&current);
                assert_always!(
                    chosen,
                    "generation: a generation refusal names the set in force at the matchmaker",
                    { "matchmaker" => matchmaker.0, "generation" => current.generation.0 }
                );
                reach_once!(
                    self.refused_generation,
                    "generation: a proposer addressing another generation is refused"
                );
            }
            MatchRefusal::Inactive => {
                reach_once!(
                    self.refused_inactive,
                    "generation: an inactive matchmaker refuses a proposer"
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

    /// A reply was deliberately dropped after its write was durable.
    pub(super) fn reply_dropped(&mut self, reply: paros::Reply) {
        match reply {
            paros::Reply::GcAck => reach_once!(
                self.gc_ack_dropped,
                "gc: a garbage-collection ack is dropped at the reply seam"
            ),
            paros::Reply::MatchmakerReconfigure => reach_once!(
                self.reconfigure_reply_dropped,
                "generation: a handover reply is dropped at the reply seam"
            ),
            _ => reach_once!(
                self.reply_dropped,
                "matchmaker: a reply is dropped at the reply seam"
            ),
        }
    }

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
    }

    /// A candidate opened matchmaking for `ballot`, registering `config`
    /// with `generation`'s matchmakers.
    pub(super) fn campaign_started(
        &mut self,
        node: NodeId,
        ballot: Ballot,
        config: &AcceptorConfig,
        reconfiguration: bool,
        generation: u64,
    ) {
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
            reach_once!(
                self.refloored,
                "matchmaking: a refused candidate re-campaigns above the refuser's round"
            );
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
                { "node" => node.0, "round" => ballot.round, "members" => config.members.len() }
            );
        }
        if reconfiguration {
            self.wire_configs.insert(config.clone());
        }
        // A campaign is opened once per ballot: the ballot is fresh.
        let floor_at_start = self.effective_floor;
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        campaign.floor_at_start = floor_at_start;
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
        campaign.reconfiguration = reconfiguration;
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
        let generation = self
            .campaigns
            .get(&(node.0, ballot))
            .map_or(0, |c| c.generation);
        let quorum = self.quorum(generation);
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
        let replies = folded_replies(campaign, node, ballot, prior, watermark);
        let (_, histories) = union_above(&replies, watermark);
        // The effective configuration (review of #132): the highest-ballot
        // *reconfiguration* registration the folded replies name. An
        // ordinary campaign completes only if it registered exactly that —
        // a stale belief must have aborted instead (`campaign_stale`), so a
        // superseded configuration is never reinstated by an election. A
        // reconfiguration campaign is exempt: it is the next one.
        let effective = replies
            .iter()
            .flat_map(|(history, _)| history.iter())
            .filter(|(_, r)| r.reconfiguration)
            .max_by_key(|(b, _)| *b)
            .map(|(b, r)| (*b, &r.config));
        if !campaign.reconfiguration
            && let Some((newest, config)) = effective
        {
            assert_always!(
                campaign.config.as_ref() == Some(config),
                "matchmaking: a completed ordinary campaign registered the effective configuration",
                { "node" => node.0, "round" => ballot.round, "newest_round" => newest.round }
            );
            reach_once!(
                self.effective_checked,
                "matchmaking: a completed campaign is checked against the effective configuration"
            );
        }
        campaign.completed = true;
        if let Some(config) = campaign.config.clone() {
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
            .map(|(_, r)| r.reconfiguration);
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
            !campaign.completed && !campaign.reconfiguration,
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
    pub(super) fn campaign_refused(
        &mut self,
        node: NodeId,
        ballot: Ballot,
        refusal: &MatchRefusal,
    ) {
        let campaign = self.campaigns.entry((node.0, ballot)).or_default();
        assert_always!(
            !campaign.completed,
            "matchmaking: a refusal never lands on a campaign that already reached Phase 1",
            { "node" => node.0, "round" => ballot.round }
        );
        campaign.refused = true;
        if let MatchRefusal::Stale { highest } = refusal {
            let floor = self.refused_floor.entry(node.0).or_default();
            *floor = (*floor).max(highest.round);
        }
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
        // #123: a leader's floor becomes effective on some seed — the outcome
        // GC exists for, and the sweep is certain to reach it (any settled
        // leadership on a matchmaker seed with a live matchmaker quorum).
        assert_sometimes!(
            self.gc_effective,
            "gc: a leader's floor becomes effective at a matchmaker quorum"
        );
        // #125: a handover completes on some seed (the workload asks for one
        // on every matchmaker seed, and a one-member pool can hand over to
        // itself).
        assert_sometimes!(
            self.reconfigurer_done,
            "generation: a matchmaker-set handover completes"
        );
    }

    // ---- garbage collection (#123) ------------------------------------------

    /// A leader sent a GC request; `covered` is the audit's own re-derivation
    /// of the forgettability condition (computed by the caller, which holds
    /// the durable-record fold), judged once per `(node, watermark)`.
    pub(super) fn gc_requested(
        &mut self,
        node: NodeId,
        watermark: Ballot,
        fence: Option<Slot>,
        covered: Option<bool>,
        uncovered: &str,
    ) {
        // A leader collects at its own ballot — the floor it raises is the
        // ballot it registered above every configuration it may forget.
        assert_always!(
            watermark.node == node,
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
                    "fence" => fence.map_or(-1_i64, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
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

    /// The leader skipped a due GC re-send.
    pub(super) fn gc_resend_skipped(&mut self) {
        reach_once!(
            self.gc_resend_skipped,
            "gc: the driver skips a due garbage-collection re-send"
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
        self.stop_acks.remove(&(node.0, old.generation.0));
        let members: Vec<u64> = old.members.iter().map(|m| m.0).collect();
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

    /// The reconfigurer skipped a due re-send.
    pub(super) fn reconfigurer_resend_skipped(&mut self) {
        reach_once!(
            self.reconfigurer_resend_skipped,
            "generation: the driver skips a due handover re-send"
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
        if let ReconfigureReply::Stopped { generation, .. } = reply
            && matches!(
                step,
                ReconfigurerStep::Stopped { .. } | ReconfigurerStep::Bootstrapping { .. }
            )
        {
            self.stop_acks
                .entry((node.0, generation.0))
                .or_default()
                .insert(matchmaker.0);
        }
        match step {
            ReconfigurerStep::Ignored
            | ReconfigurerStep::Stopped { .. }
            | ReconfigurerStep::Bootstrapped { .. } => {}
            ReconfigurerStep::Bootstrapping { bootstrap } => {
                // Invariant 3: the reconstruction is the union of the frozen
                // quorum's durable registries above their maximum watermark
                // — and every completed registration of the replaced
                // generation above that watermark is in it.
                let old = bootstrap.set.generation.0.saturating_sub(1);
                let folded = self
                    .stop_acks
                    .get(&(node.0, old))
                    .cloned()
                    .unwrap_or_default();
                let quorum = self.quorum(old);
                assert_always!(
                    folded.len() >= quorum,
                    "generation: a reconstruction rests on a frozen matchmaker quorum",
                    { "node" => node.0, "generation" => old, "folded" => folded.len(), "quorum" => quorum }
                );
                let max_watermark = folded
                    .iter()
                    .filter_map(|m| self.frozen.get(m))
                    .filter(|(g, _, _)| *g == old)
                    .map(|(_, w, _)| *w)
                    .max()
                    .unwrap_or_default();
                let mut expected: BTreeMap<Ballot, Registration> = BTreeMap::new();
                for m in &folded {
                    if let Some((g, _, registry)) = self.frozen.get(m)
                        && *g == old
                    {
                        for (b, r) in registry {
                            if *b >= max_watermark {
                                expected.entry(*b).or_insert_with(|| r.clone());
                            }
                        }
                    }
                }
                let folded_ids: Vec<String> = folded.iter().map(ToString::to_string).collect();
                assert_always!(
                    bootstrap.gc_watermark == max_watermark && bootstrap.history == expected,
                    "generation: a reconstruction is the union of the frozen quorum above its maximum watermark",
                    {
                        "node" => node.0,
                        "generation" => old,
                        "reported" => bootstrap.history.len(),
                        "expected" => expected.len(),
                        "reported_round" => bootstrap.gc_watermark.round,
                        "max_round" => max_watermark.round,
                        "folded" => folded_ids.join(",")
                    }
                );
                let missing = self
                    .campaigns
                    .iter()
                    .filter(|((_, b), c)| {
                        c.completed && c.generation == old && *b >= bootstrap.gc_watermark
                    })
                    .find(|((_, b), c)| {
                        bootstrap.history.get(b).map(|r| &r.config) != c.config.as_ref()
                    })
                    .map(|((_, b), _)| b.round);
                assert_always!(
                    missing.is_none(),
                    "generation: a reconstruction carries every completed registration above its watermark",
                    { "node" => node.0, "generation" => old, "missing_round" => missing.unwrap_or(0) }
                );
                if !bootstrap.history.is_empty() {
                    reach_once!(
                        self.handover_with_prior_registrations,
                        "generation: a handover carries prior registrations forward"
                    );
                }
                reach_once!(
                    self.reconstruction_checked,
                    "generation: a reconstruction is checked against the frozen registries"
                );
                reach_once!(
                    self.reconfigurer_bootstrapping,
                    "generation: a frozen quorum is reconstructed and bootstrapped"
                );
            }
            ReconfigurerStep::Deciding { .. } => {
                // Invariant 7: every proposed member holds the bootstrap.
                if let ReconfigureReply::Bootstrapped { set, .. } = reply {
                    let members: Vec<u64> = set.members.iter().map(|m| m.0).collect();
                    let holders = self
                        .bootstrapped
                        .get(&(set.generation.0, members.clone()))
                        .map_or(0, BTreeSet::len);
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
            ReconfigurerStep::Proposing { adopted, .. } => {
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
                let members: Vec<u64> = successor.members.iter().map(|m| m.0).collect();
                self.bind_set(successor.generation.0, &members, "chosen");
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

    /// A matchmaker durably persisted its generation scalars.
    pub(super) fn scalars_persisted(
        &mut self,
        matchmaker: MatchmakerId,
        scalars: &MatchmakerHardState,
    ) {
        let generation = scalars.generation.0;
        self.scalars_note.insert(
            matchmaker.0,
            format!(
                "g{generation}/{}/pending={:?}/succ={:?}/promised={}",
                phase_name(scalars.phase),
                scalars
                    .pending
                    .iter()
                    .map(|p| (p.set.generation.0, p.set.members.len()))
                    .collect::<Vec<_>>(),
                scalars.successor.as_ref().map(|s| s.generation.0),
                scalars.decree.promised.round
            ),
        );
        // A persisted phase names generation 0 or an activated generation
        // whose membership is the chosen one.
        if generation > 0 && scalars.phase != MatchmakerPhase::Inactive {
            let members: Vec<u64> = scalars.members.iter().map(|m| m.0).collect();
            self.bind_set(generation, &members, "scalars");
        }
        if let Some(successor) = &scalars.successor {
            let chosen = self.is_chosen(successor);
            assert_always!(
                chosen,
                "generation: a persisted successor link names the chosen set",
                { "matchmaker" => matchmaker.0, "generation" => successor.generation.0 }
            );
        }
        for pending in &scalars.pending {
            let members: Vec<u64> = pending.set.members.iter().map(|m| m.0).collect();
            self.bootstrapped
                .entry((pending.set.generation.0, members))
                .or_default()
                .insert(matchmaker.0);
        }
        let entry = self.registries.entry(matchmaker.0).or_default();
        if let Some((generation_no, MatchmakerPhase::Stopped)) = entry.generation {
            // Invariant 2: frozen stays frozen at its generation.
            assert_always!(
                generation > generation_no || scalars.phase == MatchmakerPhase::Stopped,
                "generation: a frozen generation never thaws",
                { "matchmaker" => matchmaker.0, "generation" => generation_no }
            );
        }
        if scalars.phase == MatchmakerPhase::Stopped
            && entry
                .generation
                .is_none_or(|(g, p)| g != generation || p != MatchmakerPhase::Stopped)
        {
            reach_once!(
                self.matchmaker_frozen,
                "generation: a matchmaker freezes its generation"
            );
        }
        entry.generation = Some((generation, scalars.phase));
        if !scalars.pending.is_empty() {
            reach_once!(
                self.matchmaker_bootstrapped_flag,
                "generation: a matchmaker durably holds a pending bootstrap"
            );
        }
    }

    /// A matchmaker durably activated a successor generation.
    pub(super) fn activated(
        &mut self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        gc_watermark: Ballot,
        registry: &[(Ballot, Registration)],
    ) {
        let members: Vec<u64> = set.members.iter().map(|m| m.0).collect();
        self.bind_set(set.generation.0, &members, "activated");
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 4: the watermark never regresses across a generation.
        assert_always!(
            gc_watermark >= entry.watermark,
            "generation: activation never lowers the watermark",
            {
                "matchmaker" => matchmaker.0,
                "generation" => set.generation.0,
                "activated_round" => gc_watermark.round,
                "folded_round" => entry.watermark.round
            }
        );
        assert_always!(
            entry.generation.is_none_or(|(g, _)| set.generation.0 > g),
            "generation: activation moves to a strictly later generation",
            { "matchmaker" => matchmaker.0, "generation" => set.generation.0 }
        );
        entry.registered = registry.iter().cloned().collect();
        entry.watermark = gc_watermark;
        entry.generation = Some((set.generation.0, MatchmakerPhase::Active));
        // The write-once ledger spans generations: the reconstructed
        // registry re-states registrations other matchmakers made, with the
        // same bytes.
        for (ballot, registration) in registry {
            let prior = self
                .ever
                .entry((matchmaker.0, *ballot))
                .or_insert_with(|| registration.clone());
            assert_always!(
                *prior == *registration,
                "matchmaker: a ballot is registered with one configuration, ever",
                { "matchmaker" => matchmaker.0, "round" => ballot.round, "bnode" => ballot.node.0 }
            );
        }
        reach_once!(
            self.matchmaker_activated_flag,
            "generation: a matchmaker activates a successor generation"
        );
    }

    /// A matchmaker answered a handover request.
    pub(super) fn reconfigure_replied(
        &mut self,
        matchmaker: MatchmakerId,
        reply: &ReconfigureReply,
    ) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        match reply {
            ReconfigureReply::Stopped {
                generation,
                gc_watermark,
                history,
                ..
            } => {
                // The freeze is durable before the answer, and the answer is
                // the durable registry above the durable watermark.
                assert_always!(
                    entry.generation == Some((generation.0, MatchmakerPhase::Stopped)),
                    "generation: a stop is answered only once the freeze is durable",
                    { "matchmaker" => matchmaker.0, "generation" => generation.0 }
                );
                let expected: Vec<(Ballot, Registration)> = entry
                    .registered
                    .range(entry.watermark..)
                    .map(|(b, r)| (*b, r.clone()))
                    .collect();
                let reported: Vec<(Ballot, Registration)> =
                    history.iter().map(|(b, r)| (*b, r.clone())).collect();
                assert_always!(
                    *gc_watermark == entry.watermark && reported == expected,
                    "generation: a frozen registry is answered as the durable one",
                    { "matchmaker" => matchmaker.0, "reported" => reported.len(), "expected" => expected.len() }
                );
                self.frozen
                    .insert(matchmaker.0, (generation.0, *gc_watermark, reported));
            }
            ReconfigureReply::Bootstrapped { set, .. } => {
                let members: Vec<u64> = set.members.iter().map(|m| m.0).collect();
                let held = self
                    .bootstrapped
                    .get(&(set.generation.0, members))
                    .is_some_and(|h| h.contains(&matchmaker.0));
                assert_always!(
                    held,
                    "generation: a bootstrap is acknowledged only once durably pending",
                    { "matchmaker" => matchmaker.0, "generation" => set.generation.0 }
                );
            }
            ReconfigureReply::Learned {
                activated,
                generation,
                ..
            } => {
                if *activated {
                    assert_always!(
                        entry.generation.is_some_and(|(g, p)| g > generation.0 && p == MatchmakerPhase::Active),
                        "generation: an activation is acknowledged only once durable",
                        { "matchmaker" => matchmaker.0, "generation" => generation.0 }
                    );
                } else {
                    reach_once!(
                        self.matchmaker_departed,
                        "generation: a departed matchmaker records its successor and stays frozen"
                    );
                }
            }
            ReconfigureReply::Refused { .. } => {
                reach_once!(
                    self.matchmaker_refused_step,
                    "generation: a matchmaker refuses a handover step for another generation"
                );
            }
            ReconfigureReply::Promised { .. }
            | ReconfigureReply::Accepted { .. }
            | ReconfigureReply::Nacked { .. } => {}
        }
    }

    /// A matchmaker's registry was lost for good (the harness's coin).
    pub(super) fn lost(&mut self) {
        reach_once!(
            self.matchmaker_lost,
            "generation: a matchmaker's registry is lost for good"
        );
    }

    /// One line for the red-path print.
    pub(super) fn diagnostics(&self) -> String {
        let summary: Vec<String> = self
            .registries
            .iter()
            .map(|(id, r)| {
                format!(
                    "mm{id}[boots={} registered={} watermark={}.{} gen={:?}]",
                    r.boots,
                    r.registered.len(),
                    r.watermark.round,
                    r.watermark.node.0,
                    r.generation.map(|(g, p)| (g, phase_name(p)))
                )
            })
            .collect();
        let campaigns = self.campaigns.values().filter(|c| c.completed).count();
        let opened = self.campaigns.len();
        format!(
            "{} campaigns_opened={opened} campaigns_completed={campaigns} refusals={:?} reconfigurers={:?} scalars={:?} generations={:?} floor={}.{}",
            summary.join(" "),
            self.refusal_counts,
            self.reconfigurer_trace,
            self.scalars_note,
            self.sets.keys().collect::<Vec<_>>(),
            self.effective_floor.round,
            self.effective_floor.node.0
        )
    }
}
