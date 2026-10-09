//! Post-batch upkeep and the delta trackers it reports through: the randomized
//! election-timeout draw, the leadership/handoff/membership transitions, and
//! the held-reply bookkeeping a step-down performs.

use moonpool_core::{Providers, RandomProvider};
use paros_core::{
    Ballot, ColocatedNode, HandoffCounters, LeadershipOrigin, MembershipCounters, NodeId, NodeRole,
    RepairCounters,
};

use crate::audit::Audit;
use crate::hooks::HandoffContext;

use super::ready::ClientWaiters;

/// What a handoff would transfer right now, from the core's public read views:
/// the span between this leader's contiguous chosen prefix and its allocator
/// frontier, plus whether it is itself still healing a hole.
///
/// Pure observation — it exists only so [`DriverHooks::initiate_handoff`] can be
/// biased toward the interesting shapes instead of firing uniformly.
pub(crate) fn handoff_context(node: &ColocatedNode, candidates: usize) -> HandoffContext {
    let first_unchosen = node.hard_state().chosen_index.map_or(0, |ci| ci.0 + 1);
    let tail = usize::try_from(node.proposer().next_slot().0.saturating_sub(first_unchosen))
        .unwrap_or(usize::MAX);
    HandoffContext {
        tail,
        next_slot: node.proposer().next_slot(),
        settled: tail == 0,
        healing: node.replica().chosen_gap().is_some(),
        candidates,
    }
}

/// Draw a randomized election timeout in `[T, 2T)` ticks from the provider's
/// seeded RNG. Drawn here, never in the zero-dep core, so the core stays
/// deterministic and dependency-free while a seed still replays bit-identically.
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id, base))]
pub(crate) fn draw_election_timeout<P: Providers>(providers: &P, self_id: u64, base: u64) -> u64 {
    // A base of at least two beats (`check_floors`): the draw's range is
    // never empty.
    assert!(base > 0, "an election timeout base is at least one tick");
    // The two jitter extremes are rare-but-valid choices, each its own
    // BUGGIFY location, silent in the recovery tail. The longest is drawn
    // only when the shortest stayed quiet, so the two never both apply.
    let ticks = if moonpool_buggify::buggify_fault_with_prob!(0.5) {
        moonpool_assertions::reachable!("the driver selects the shortest valid election timeout");
        tracing::info!(node = self_id, ticks = base, "election_timeout_extreme");
        base
    } else if moonpool_buggify::buggify_fault_with_prob!(0.5) {
        // The highest value the honest draw below could produce.
        moonpool_assertions::reachable!("the driver selects the longest valid election timeout");
        let ticks = base * 2 - 1;
        tracing::info!(node = self_id, ticks, "election_timeout_extreme");
        ticks
    } else {
        providers.random().random_range(base..base * 2)
    };
    // Whatever the sites chose, the timeout is one the honest draw could
    // produce: in `[base, 2 * base)`.
    assert!(ticks >= base, "an election timeout is at least its base");
    assert!(
        ticks < base * 2,
        "an election timeout stays below twice its base"
    );
    ticks
}

/// Report this batch's cooperative-handoff transitions and return whether an
/// authority was **installed** in it.
///
/// Three channels, each a different fact: the install of a predecessor's
/// authority (a leadership acquired with *no* Phase 1, so it is deliberately
/// not reported through [`Audit::elected`], whose "leadership ballots strictly
/// increase" reading is about a node's own campaigns), the per-reason refusal
/// totals the wire guards accumulated, and the inherited-fence resignations.
/// The relinquish half is reported at its own call site, at the instant the
/// authority changes hands.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn report_handoff<A: Audit>(
    node: &ColocatedNode,
    last: &mut HandoffCounters,
    self_id: u64,
    audit: &A,
) -> bool {
    let handoff = node.handoff_counters();
    // The counters are monotone per incarnation, and the delta tracker was
    // seeded from this incarnation's.
    assert!(
        handoff.installed >= last.installed,
        "the install counter never falls"
    );
    assert!(handoff.out >= last.out, "the handoff counter never falls");
    let installed_now = handoff.installed != last.installed;
    if handoff.rejected_target != last.rejected_target
        || handoff.rejected_stale != last.rejected_stale
        || handoff.rejected_shape != last.rejected_shape
        || handoff.rejected_unfit != last.rejected_unfit
    {
        audit.handoff_refused(
            NodeId(self_id),
            handoff.rejected_target,
            handoff.rejected_stale,
            handoff.rejected_shape,
            handoff.rejected_unfit,
        );
        tracing::info!(
            node = self_id,
            target = handoff.rejected_target,
            stale = handoff.rejected_stale,
            shape = handoff.rejected_shape,
            unfit = handoff.rejected_unfit,
            "handoff_refused"
        );
    }
    if handoff.fence_step_downs != last.fence_step_downs {
        audit.handoff_fence_expired(NodeId(self_id), handoff.fence_step_downs);
        tracing::info!(
            node = self_id,
            count = handoff.fence_step_downs,
            "handoff_fence_expired"
        );
    }
    // Keyed on the install counter rather than a role transition: an install
    // can also replace a leadership this node already held at a lower ballot.
    if installed_now && let LeadershipOrigin::Handoff { from } = node.leadership_origin() {
        let ballot = node.ballot();
        let next_slot = node.proposer().next_slot();
        let tail = u64::try_from(handoff_context(node, 0).tail).unwrap_or(u64::MAX);
        audit.authority_installed(NodeId(self_id), from, ballot, next_slot, tail);
        tracing::info!(
            node = self_id,
            from = from.0,
            round = ballot.round,
            bnode = ballot.node.0,
            next_slot = next_slot.0,
            tail,
            "authority_installed"
        );
    }
    *last = handoff;
    installed_now
}

/// The loop's cross-batch delta trackers, so `maintain` reports each monotone
/// core counter exactly once per change.
pub(crate) struct Deltas {
    pub(crate) role: NodeRole,
    pub(crate) quorum_lost: u64,
    pub(crate) watermark_fills: u64,
    pub(crate) repair: RepairCounters,
    pub(crate) handoff: HandoffCounters,
    pub(crate) membership: MembershipCounters,
    pub(crate) matchmaking: Option<Ballot>,
    pub(crate) matchmaking_timeouts: u64,
    pub(crate) matchmaker_generation: u64,
    /// Consecutive election-clock expiries with no leader known between
    /// them: the election backoff's streak
    /// ([`DriverTunables::election_backoff_doublings`](super::DriverTunables)).
    pub(crate) failed_campaigns: u32,
}

impl Deltas {
    /// The trackers' starting point: the counters the core recovered with.
    pub(crate) fn new(node: &ColocatedNode) -> Self {
        Self {
            role: node.role(),
            quorum_lost: node.quorum_lost_step_downs(),
            watermark_fills: node.watermark_fills(),
            repair: node.repair_counters(),
            handoff: node.handoff_counters(),
            membership: node.membership_counters(),
            matchmaking: None,
            matchmaking_timeouts: node.matchmaking_timeouts(),
            matchmaker_generation: node.matchmaker_set().map_or(0, |set| set.generation.0),
            failed_campaigns: 0,
        }
    }
}

/// A re-send clock: ticks since an open request was last (re-)sent, due every
/// `cadence` ticks (a cadence of zero is read as one).
#[derive(Default)]
pub(crate) struct Cadence {
    elapsed: u64,
}

impl Cadence {
    /// One tick while a request is open: whether its re-send is due now (the
    /// clock restarts when it is).
    pub(crate) fn tick(&mut self, cadence: u64) -> bool {
        self.elapsed += 1;
        if self.elapsed >= cadence.max(1) {
            self.elapsed = 0;
            return true;
        }
        false
    }

    /// One tick of a request that may be closed: nothing is due and the
    /// clock restarts while `open` is false.
    pub(crate) fn tick_if(&mut self, open: bool, cadence: u64) -> bool {
        if open {
            self.tick(cadence)
        } else {
            self.reset();
            false
        }
    }

    /// Restart the clock.
    pub(crate) fn reset(&mut self) {
        self.elapsed = 0;
        assert!(self.elapsed == 0, "a reset cadence starts from zero");
    }
}

/// Surface the campaign-membership transitions (#122): a campaign this node
/// declined as a non-member, and a leadership it resigned once its own
/// reconfiguration removed it.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn report_membership<A: Audit>(
    node: &ColocatedNode,
    last_membership: &mut MembershipCounters,
    self_id: u64,
    audit: &A,
) {
    let membership = node.membership_counters();
    assert!(
        membership.campaigns_skipped >= last_membership.campaigns_skipped,
        "skipped campaigns are counted monotonically"
    );
    assert!(
        membership.step_downs >= last_membership.step_downs,
        "non-member step-downs are counted monotonically"
    );
    if membership.campaigns_skipped != last_membership.campaigns_skipped {
        audit.campaign_skipped_non_member(NodeId(self_id), membership.campaigns_skipped);
        tracing::info!(
            node = self_id,
            count = membership.campaigns_skipped,
            "campaign_skipped_non_member"
        );
    }
    assert!(
        membership.reads_without_basis >= last_membership.reads_without_basis,
        "reads without a basis are counted monotonically"
    );
    assert!(
        membership.pre_reads_refused_unheard >= last_membership.pre_reads_refused_unheard,
        "unheard pre-read refusals are counted monotonically"
    );
    if membership.reads_without_basis != last_membership.reads_without_basis {
        audit.read_without_basis(NodeId(self_id), membership.reads_without_basis);
        tracing::info!(
            node = self_id,
            count = membership.reads_without_basis,
            "read_without_basis"
        );
    }
    if membership.pre_reads_refused_unheard != last_membership.pre_reads_refused_unheard {
        audit.pre_read_refused_unheard(NodeId(self_id), membership.pre_reads_refused_unheard);
        tracing::info!(
            node = self_id,
            count = membership.pre_reads_refused_unheard,
            "pre_read_refused_unheard"
        );
    }
    if membership.step_downs != last_membership.step_downs {
        audit.non_member_leader_resigned(NodeId(self_id), membership.step_downs);
        tracing::info!(
            node = self_id,
            count = membership.step_downs,
            "non_member_leader_resigned"
        );
    }
    *last_membership = membership;
}

/// Post-batch upkeep: feed the core a fresh randomized election timeout whenever
/// its election clock reset, emit `leader_elected` on the transition to Leader,
/// and drop held client replies on step-down (so clients time out and retry the
/// new leader).
// The `last_*` parameters are the loop's cross-batch delta trackers (role,
// #94 suppressions, `CheckQuorum` step-downs); bundling them into a struct
// would only rename the same nine things.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
pub(crate) fn maintain<P: Providers, A: Audit>(
    node: &mut ColocatedNode,
    providers: &P,
    last: &mut Deltas,
    waiters: &mut ClientWaiters,
    self_id: u64,
    (election_base, backoff_doublings): (u64, u32),
    audit: &A,
) {
    let Deltas {
        role: last_role,
        quorum_lost: last_quorum_lost,
        watermark_fills: last_watermark_fills,
        repair: last_repair,
        handoff: last_handoff,
        membership: last_membership,
        matchmaking: _,
        matchmaking_timeouts: last_matchmaking_timeouts,
        matchmaker_generation: last_generation,
        failed_campaigns,
    } = last;
    // The election backoff's streak ends only when this node hears another
    // node lead: a cluster with a leader other nodes follow has settled.
    // Being the leader does not end it — a leadership deposed before any
    // peer followed it is a duel, not a settlement.
    if node.leader().is_some_and(|leader| leader.0 != self_id) {
        *failed_campaigns = 0;
    }
    if node.needs_election_timeout() {
        // The election backoff: a clock that reset with no leader known —
        // a campaign that failed, or a leadership a rival's `Prepare`
        // deposed — is one more round in a streak, and each consecutive
        // one doubles the base (capped), so a round eventually outlasts the
        // slowest promise it waits on and the time a new leader's first
        // beat takes to arrive. A deposed leader that only counted
        // candidacies reset its streak on winning and re-campaigned one base
        // timeout after its rival's `Prepare`, before the rival's first beat
        // could land, and deposed it in turn: two matchmaker-deployment
        // candidates traded leadership through 183 rounds and the whole
        // quiet tail (witness seed 14889077543971178620 of the coverage
        // sweep that landed #204).
        if node.role() != NodeRole::Leader && node.leader().is_none() {
            *failed_campaigns = failed_campaigns.saturating_add(1);
        }
        let doublings = failed_campaigns.saturating_sub(1).min(backoff_doublings);
        assert!(doublings <= backoff_doublings, "the backoff is capped");
        let base = election_base.saturating_mul(1_u64 << doublings.min(16));
        if doublings > 0 {
            tracing::info!(node = self_id, doublings, base, "election_backoff");
            audit.election_backoff(NodeId(self_id), doublings);
        }
        let ticks = draw_election_timeout(providers, self_id, base);
        node.set_election_timeout(ticks);
        assert!(
            !node.needs_election_timeout(),
            "a drawn timeout clears the request"
        );
        audit.election_timeout_set(NodeId(self_id), ticks);
    }
    // Surface any repair progress (Stage 8): in-place heals, straggler
    // resolutions, and recovery-timeout resignations.
    let repair = node.repair_counters();
    if repair != *last_repair {
        *last_repair = repair;
        let RepairCounters {
            repaired,
            case1,
            case2,
            step_downs,
        } = repair;
        audit.repair_progress(NodeId(self_id), repaired, case1, case2, step_downs);
        tracing::info!(
            node = self_id,
            repaired,
            case1,
            case2,
            step_downs,
            "repair_progress"
        );
    }
    let installed_now = report_handoff(node, last_handoff, self_id, audit);
    report_membership(node, last_membership, self_id, audit);
    // Surface a matchmaker set learned through a path that reports nothing
    // itself (#125): a handover this node's reconfigurer completed, a reply
    // from a later generation.
    if let Some(set) = node.matchmaker_set()
        && set.generation.0 != *last_generation
    {
        let generation = set.generation.0;
        *last_generation = generation;
        audit.matchmakers_learned(NodeId(self_id), set);
        tracing::info!(
            node = self_id,
            generation,
            members = set.members().len() as u64,
            "matchmakers_learned"
        );
    }
    // Surface an election timeout that re-asked an open matchmaking (#120)
    // instead of abandoning it: the campaign's ballot travels with it so the
    // audit can hold the clock to "moved nothing".
    let timeouts = node.matchmaking_timeouts();
    if timeouts != *last_matchmaking_timeouts {
        *last_matchmaking_timeouts = timeouts;
        let ballot = node.ballot();
        audit.matchmaking_timeout(NodeId(self_id), ballot, timeouts);
        tracing::info!(
            node = self_id,
            round = ballot.round,
            count = timeouts,
            "matchmaking_timeout"
        );
    }
    // Surface any CheckQuorum step-down the batch's tick performed (#95).
    let quorum_lost = node.quorum_lost_step_downs();
    if quorum_lost > *last_quorum_lost {
        let count = quorum_lost - *last_quorum_lost;
        *last_quorum_lost = quorum_lost;
        audit.quorum_lost(NodeId(self_id), count);
        tracing::info!(node = self_id, count, "leader_quorum_lost");
    }
    let watermark_fills = node.watermark_fills();
    if watermark_fills > *last_watermark_fills {
        let count = watermark_fills - *last_watermark_fills;
        *last_watermark_fills = watermark_fills;
        audit.watermark_filled(NodeId(self_id), count);
        tracing::info!(node = self_id, count, "leader_watermark_filled");
    }
    let role = node.role();
    if role == NodeRole::Leader && *last_role != NodeRole::Leader && !installed_now {
        // The won ballot *and* the promise held at the instant of victory. They
        // are normally the same ballot — winning means having promised your own
        // campaign ballot and heard nothing higher — and the oracle asserts
        // exactly that: a leader never holds a promise above the ballot it just
        // won (#67). Emitting both here, on the transition, is what makes the
        // stale win visible; a tick later the leader may legitimately learn a
        // higher-ballot commit and the state is no longer distinguishable.
        let ballot = node.ballot();
        let promised = node.hard_state().max_promised_ballot;
        let gaps = node.election_gap_fills();
        audit.elected(NodeId(self_id), ballot, promised, gaps, node.acceptors());
        tracing::info!(
            node = self_id,
            round = ballot.round,
            bnode = ballot.node.0,
            pround = promised.round,
            pbnode = promised.node.0,
            members = node.acceptors().members().len() as u64,
            "leader_elected"
        );
    } else if *last_role == NodeRole::Leader && role != NodeRole::Leader {
        // Parked calls are dropped: their slots may still decide under the
        // new leader, so the clients time out — on purpose, an ambiguous
        // outcome — and a retried `Write` is answered from the log.
        let calls = waiters.pending.values().map(Vec::len).sum::<usize>();
        if calls > 0 {
            audit.waiters_cleared(NodeId(self_id), u64::try_from(calls).unwrap_or(u64::MAX));
            tracing::info!(node = self_id, calls, "waiters_cleared");
        }
        for call in std::mem::take(&mut waiters.pending).into_values().flatten() {
            drop(call);
        }
        assert!(waiters.pending.is_empty(), "a deposed leader holds no call");
    }
    *last_role = role;
}
