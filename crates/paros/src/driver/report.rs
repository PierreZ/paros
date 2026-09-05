//! Post-batch upkeep and the delta trackers it reports through: the randomized
//! election-timeout draw, the leadership/handoff/membership transitions, and
//! the held-reply bookkeeping a step-down performs.

use moonpool_core::{Providers, RandomProvider};
use paros_core::{Ballot, ColocatedNode, HandoffCounters, LeadershipOrigin, NodeId, NodeRole};

use crate::audit::Audit;
use crate::grpc::ReadAck;
use crate::hooks::{DriverHooks, HandoffContext};

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
pub(crate) fn draw_election_timeout<P: Providers, H: DriverHooks, A: Audit>(
    providers: &P,
    hooks: &H,
    audit: &A,
    self_id: u64,
    base: u64,
) -> u64 {
    if hooks.shortest_election_timeout() {
        audit.election_timeout_extreme(NodeId(self_id), base);
        tracing::info!(node = self_id, ticks = base, "election_timeout_extreme");
        base
    } else if hooks.longest_election_timeout() {
        // The other jitter extreme: the highest value the honest draw below
        // could produce. Consulted only when the shortest hook stayed quiet,
        // so the two extremes remain independent locations. Its BUGGIFY
        // pairing gate fires in the sim hook implementation (the audit's
        // `election_timeout_extreme` reach gate is the shortest extreme's).
        let ticks = base * 2 - 1;
        tracing::info!(node = self_id, ticks, "election_timeout_extreme");
        ticks
    } else {
        providers.random().random_range(base..base * 2)
    }
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
    pub(crate) duplicates: u64,
    pub(crate) quorum_lost: u64,
    pub(crate) repair: (u64, u64, u64, u64),
    pub(crate) handoff: HandoffCounters,
    pub(crate) membership: (u64, u64),
    pub(crate) matchmaking: Option<Ballot>,
    pub(crate) matchmaking_timeouts: u64,
    pub(crate) matchmaker_generation: u64,
}

/// Surface the campaign-membership transitions (#122): a campaign this node
/// declined as a non-member, and a leadership it resigned once its own
/// reconfiguration removed it.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn report_membership<A: Audit>(
    node: &ColocatedNode,
    last_membership: &mut (u64, u64),
    self_id: u64,
    audit: &A,
) {
    let membership = node.membership_counters();
    if membership.0 != last_membership.0 {
        audit.campaign_skipped_non_member(NodeId(self_id), membership.0);
        tracing::info!(
            node = self_id,
            count = membership.0,
            "campaign_skipped_non_member"
        );
    }
    if membership.1 != last_membership.1 {
        audit.non_member_leader_resigned(NodeId(self_id), membership.1);
        tracing::info!(
            node = self_id,
            count = membership.1,
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
pub(crate) fn maintain<P: Providers, H: DriverHooks, A: Audit>(
    node: &mut ColocatedNode,
    providers: &P,
    last: &mut Deltas,
    waiters: &mut ClientWaiters,
    self_id: u64,
    election_base: u64,
    hooks: &H,
    audit: &A,
) {
    let Deltas {
        role: last_role,
        duplicates: last_duplicates,
        quorum_lost: last_quorum_lost,
        repair: last_repair,
        handoff: last_handoff,
        membership: last_membership,
        matchmaking: _,
        matchmaking_timeouts: last_matchmaking_timeouts,
        matchmaker_generation: last_generation,
    } = last;
    if node.needs_election_timeout() {
        let ticks = draw_election_timeout(providers, hooks, audit, self_id, election_base);
        node.set_election_timeout(ticks);
        audit.election_timeout_set(NodeId(self_id), ticks);
    }
    // Surface any #94 duplicate suppressions the batch's contiguous walk
    // performed (the counter is monotone per incarnation).
    let duplicates = node.replica().duplicates_suppressed();
    if duplicates > *last_duplicates {
        let count = duplicates - *last_duplicates;
        *last_duplicates = duplicates;
        audit.duplicate_suppressed(NodeId(self_id), count);
        tracing::info!(node = self_id, count, "duplicate_suppressed");
    }
    // Surface any repair progress (Stage 8): in-place heals, straggler
    // resolutions, and recovery-timeout resignations.
    let repair = node.repair_counters();
    if repair != *last_repair {
        *last_repair = repair;
        let (repaired, case1, case2, step_downs) = repair;
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
    let generation = node.matchmaker_set().generation.0;
    if generation != *last_generation {
        *last_generation = generation;
        let set = node.matchmaker_set();
        audit.matchmakers_learned(NodeId(self_id), set);
        tracing::info!(
            node = self_id,
            generation,
            members = set.members.len() as u64,
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
        let writes = waiters.pending.values().map(Vec::len).sum::<usize>();
        let reads = waiters.pending_reads.len();
        if writes + reads > 0 {
            audit.waiters_cleared(
                NodeId(self_id),
                u64::try_from(writes).unwrap_or(u64::MAX),
                u64::try_from(reads).unwrap_or(u64::MAX),
            );
            tracing::info!(node = self_id, writes, reads, "waiters_cleared");
        }
        waiters.pending.clear();
        // Parked reads have no slot whose commit could ever answer them:
        // redirect explicitly so the client retries the new leader now rather
        // than burning its deadline (writes time out instead, on purpose —
        // their slot may still commit under the new leader).
        for (_, (seq, _, waiter)) in std::mem::take(&mut waiters.pending_reads) {
            let _ = waiter.send(ReadAck {
                seq,
                leader: node.leader().map(|n| n.0),
                committed: false,
                read_index: None,
            });
        }
    }
    *last_role = role;
}
