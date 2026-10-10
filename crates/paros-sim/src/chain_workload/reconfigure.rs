//! The chain client's reconfigurations: the `RECONFIGURE` operation, the
//! shape rings, the set composer and the operators' ledger.

use std::collections::BTreeSet;
use std::sync::PoisonError;
use std::time::Duration;

use futures::future::join_all;
use moonpool_sim::{
    RandomProvider, TimeProvider, assert_always, assert_reachable, buggify_with_prob,
};
use paros::client::{
    MatchmakersRefusal, ReconfigureMatchmakersOutcome, ReconfigureOutcome, RetireOutcome,
};
use paros::{
    QuorumSystem, ReconfigureRefusal, RetireRequest, WireQuorumSystem, quorum_system_from_proto,
};

use super::system::SystemOps;
use super::{ChainWorkload, Step, adopt_plane_leader, weighted_index};

/// The reconfiguration shapes, by `raw_class` draw (see [`RECONFIGURE`]).
pub(super) const RECONFIGURE_SHAPES: [&str; 5] =
    ["grow", "shrink", "replace", "remove-leader", "rotate"];

/// The `shrink` entry of [`RECONFIGURE_SHAPES`], the shape a rotation through
/// a ring no larger than the set in force actually composes.
const SHRINK_SHAPE: usize = 1;
/// The `rotate` entry of [`RECONFIGURE_SHAPES`], the departed-straggler
/// scenario's removal.
pub(super) const ROTATE_SHAPE: usize = 4;
/// The shapes that move a member out, as indices into
/// [`RECONFIGURE_SHAPES`]: every one but `grow`.
pub(super) const REMOVING_SHAPES: [usize; 4] = [1, 2, 3, 4];
/// The shapes a [`RECONFIGURE_MATCHMAKERS`] step draws from, as indices into
/// [`RECONFIGURE_SHAPES`]: a matchmaker set has no leader to remove.
pub(super) const MATCHMAKER_SHAPES: [usize; 4] = [0, 1, 2, 4];

/// Compose the set a [`RECONFIGURE`] (or [`RECONFIGURE_MATCHMAKERS`]) step
/// asks for, from the set in force (`members`) and the step's shape draw.
/// `candidates` are the ids the successor may draw from — the pool minus
/// every identity the run has lost for good (wiped, retired, or parked), so
/// a client never asks for a member that can no longer answer. `floor` is
/// the smallest configuration the run may put in force
/// (`crate::shape::config_floor` for acceptors, the size the storage world's
/// copy budget is computed over; `crate::shape::matchmaker_floor` for
/// matchmakers). A dead member (one outside `candidates`) is the first one
/// a `replace` or `shrink` moves out: that is how a wiped identity (#124)
/// leaves the configuration. `None` when the shape is impossible here (no
/// spare to grow onto, nothing above the floor to shrink); the step is then
/// a no-op.
///
/// `whole` asks a `rotate` for a **whole-set rotation**: the successor drawn
/// from the spares alone, sharing no member with the set in force, whenever
/// the candidates hold enough of them (a BUGGIFY choice at the call site:
/// an ordinary rotation's random start on the ring rarely lands there).
///
/// The index and name returned are the shape **observed in the composed set**,
/// not the one asked for: a `rotate` through a candidate ring no larger than
/// the set in force drops members instead of replacing them, which is a
/// `shrink`, and labelling it a rotation lit the whole-set-rotation gate on a
/// successor that shared every surviving member with its predecessor.
pub(super) fn compose_reconfiguration(
    shape: usize,
    members: &[u64],
    candidates: &[u64],
    floor: usize,
    leader: Option<u64>,
    draw: u64,
    whole: bool,
) -> Option<(usize, &'static str, Vec<u64>)> {
    let mut current: Vec<u64> = members.to_vec();
    current.sort_unstable();
    current.dedup();
    if current.is_empty() || candidates.is_empty() {
        return None;
    }
    let spares: Vec<u64> = candidates
        .iter()
        .copied()
        .filter(|n| !current.contains(n))
        .collect();
    let dead: Option<usize> = current.iter().position(|n| !candidates.contains(n));
    let pick = |len: usize| usize::try_from(draw % u64::try_from(len).unwrap_or(1)).unwrap_or(0);
    let mut next = current.clone();
    let mut observed = shape % RECONFIGURE_SHAPES.len();
    let name = RECONFIGURE_SHAPES[observed];
    match name {
        "grow" => {
            if spares.is_empty() {
                return None;
            }
            next.push(spares[pick(spares.len())]);
        }
        "shrink" => {
            if current.len() <= floor {
                return None;
            }
            next.remove(dead.unwrap_or_else(|| pick(current.len())));
        }
        "replace" => {
            if spares.is_empty() {
                return None;
            }
            next[dead.unwrap_or_else(|| pick(current.len()))] = spares[pick(spares.len())];
        }
        "remove-leader" => {
            let leader = leader?;
            if current.len() <= floor || !current.contains(&leader) {
                return None;
            }
            next.retain(|n| *n != leader);
        }
        _ => {
            // "rotate": the same number of members, read off the candidate
            // ring from a shifted start — a mostly or wholly disjoint
            // successor when spares allow it; wholly, off the spares alone,
            // when `whole` asks and there are enough of them.
            if whole && spares.len() >= current.len() {
                let start = pick(spares.len());
                next = (0..current.len())
                    .map(|k| spares[(start + k) % spares.len()])
                    .collect();
            } else {
                let ring = candidates.len();
                let start = 1 + pick(ring.max(2) - 1);
                next = (0..current.len().min(ring))
                    .map(|k| candidates[(start + k) % ring])
                    .collect();
            }
        }
    }
    next.sort_unstable();
    next.dedup();
    if next == current || next.len() < floor {
        return None;
    }
    if RECONFIGURE_SHAPES[observed] == "rotate" && next.len() < current.len() {
        // A ring no larger than the set in force cannot rotate it: what came
        // out is the surviving members, one short — a shrink.
        observed = SHRINK_SHAPE;
    }
    Some((observed, RECONFIGURE_SHAPES[observed], next))
}

/// File a reconfiguration asking for `members` in the operators' ledger
/// (#198) before it leaves; returns the id its answer is filed under.
/// A quorum system `n` members do not admit, judged by the workload's own
/// arithmetic, never the library's (#269), or `None` when `n` admits every
/// shape below. `draw` picks one of three: the split `{1, 1}` (two quorums
/// that need not meet once `n >= 2`), the boundary split `{1, n - 1}`
/// (`q1 + q2 == n`: still no intersection), or a 2-row grid of `n` columns
/// (it does not tile `n` members).
pub(super) fn malformed_system(n: usize, draw: u64) -> Option<QuorumSystem> {
    if n < 2 {
        return None;
    }
    let system = match draw % 3 {
        0 => QuorumSystem::Flexible { q1: 1, q2: 1 },
        1 => QuorumSystem::Flexible { q1: 1, q2: n - 1 },
        _ => QuorumSystem::Grid { rows: 2, cols: n },
    };
    Some(system)
}

pub(super) fn ledger_request(state: &moonpool_sim::StateHandle, members: &[u64]) -> u64 {
    crate::world::storage_world(state)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .note_reconfiguration_requested(members)
}

/// File the leader's answer to ledger request `id`: the round it started
/// at, or a refusal. An ambiguous answer is never filed — the request may
/// have registered anywhere, and the ledger keeps it as such.
pub(super) fn ledger_answer(
    state: &moonpool_sim::StateHandle,
    id: u64,
    outcome: &ReconfigureOutcome,
) {
    let started = match outcome {
        ReconfigureOutcome::Started { round, .. } => Some(*round),
        ReconfigureOutcome::NotLeader { .. }
        | ReconfigureOutcome::Refused { .. }
        | ReconfigureOutcome::Unrecognized { .. } => None,
        ReconfigureOutcome::Ambiguous => return,
    };
    crate::world::storage_world(state)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .note_reconfiguration_answered(id, started);
}

/// The ids (ranks into `ips`) a reconfiguration may still draw from: every
/// identity the run has not lost for good.
pub(super) fn live_candidates(
    ips: &[String],
    dead: &std::collections::BTreeSet<String>,
) -> Vec<u64> {
    ips.iter()
        .enumerate()
        .filter(|(_, ip)| !dead.contains(*ip))
        .map(|(i, _)| u64::try_from(i).unwrap_or(u64::MAX))
        .collect()
}

impl ChainWorkload {
    /// One `RECONFIGURE` step (#122): compose a successor configuration from
    /// the set in force and ask the leader for it, filed in the operators'
    /// ledger. `remove_next` is the owner's removal, cleared once it leaves.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn reconfigure_step(
        &mut self,
        step: &Step<'_>,
        remove_next: &mut bool,
        system_ops: &mut SystemOps,
    ) {
        let Step {
            after_claim,
            after_register,
            config,
            config_floor,
            ctx,
            has_matchmakers,
            journal,
            nodes,
            policy,
            raw_class,
            raw_payload,
            raw_policy,
            reconfigurer,
            server_count,
            servers,
            target,
            time,
            ..
        } = *step;
        // Read the configuration in force from the hinted leader
        // (or the step's target): every node learns it from the
        // ballot's `Prepare`, so a stale answer only makes the
        // request refused (`unchanged`, `unknown_member`) — an
        // operating condition, never a wrong state.
        let probe_target = nodes.leader().unwrap_or(target);
        let in_force = nodes.inspect(probe_target, journal).await.map(|reply| {
            let wire = WireQuorumSystem {
                quorum_system: reply.quorum_system,
                phase1_quorum: reply.phase1_quorum,
                phase2_quorum: reply.phase2_quorum,
                rows: reply.rows,
                cols: reply.cols,
            };
            (reply.members, quorum_system_from_proto(&wire).ok())
        });
        let members = in_force.as_ref().map(|(members, _)| members.clone());
        let system_in_force = in_force.and_then(|(_, system)| system);
        // The owner's first operation after its claim (see
        // `reconfigure_after_claim`) starts the shape ring at
        // one that moves a member out — never `grow`.
        // On a departed-straggler seed it is a `rotate`, whole
        // when the spares allow: the successor's fresh members
        // never held the departed members' slots, so the newest
        // configuration alone may hold a quorum of `none`
        // answers where the prior one does not, the sub-shape
        // only the cross-configuration Phase 1 decides (#267).
        let scenario_rotation = after_claim && crate::shape::departed_straggler(ctx.state());
        let drawn = if scenario_rotation {
            ROTATE_SHAPE
        } else if after_claim {
            REMOVING_SHAPES[usize::try_from(raw_class % 4).unwrap_or(0)]
        } else {
            weighted_index(&config.reconfigure_shape_weights, raw_class)
        };
        let leader_id = nodes.leader().map(|l| nodes.id_of(l));
        // The successor draws from the live pool: an identity the
        // run lost for good (wiped, retired, corruption-parked)
        // is never asked for, and is the first one moved out.
        let mut live = live_candidates(servers, &crate::world::parked_nodes(ctx.state(), journal));
        // The joiners the node registry admitted (#189): a
        // successor may pull one in. Read here, before the
        // composition and its ledger entry, which take no await
        // between them — a retirement reserved in the meantime
        // is re-checked at the ledger.
        let joinable = system_ops.joinable(ctx, nodes, raw_payload).await;
        live.extend(joinable.iter().copied());
        // The adversarial draw (R5): compose from *every* rank
        // instead, so the request may name an identity the run
        // lost for good. A well-behaved operator would not, and
        // the protocol must survive one who does. What it must
        // never be asked for is an *unwinnable* configuration —
        // one whose live members cannot form a quorum — so every
        // composition, adversarial or not, is filtered on that
        // and the shape ring falls through to one that holds.
        // This binds the ordinary path too: `grow` keeps the set
        // in force whole, so growing onto a spare from a
        // configuration that already carried a dead member left
        // one live of two (hunt seed 11169765483580423663); the
        // ring now reaches `replace`/`shrink` instead, which move
        // the dead identity out — the composer's documented job.
        let all_ranks: Vec<u64> = (0..u64::try_from(server_count).unwrap_or(0)).collect();
        let adversarial_members = buggify_with_prob!(0.10);
        // A whole-set rotation (#173): the successor off the
        // spares alone. The ring's random start lands there only
        // by luck, and it is the shape that leaves every
        // rebooted member outside the bootstrap belief.
        let whole_rotation = scenario_rotation || buggify_with_prob!(0.5);
        // The successor's quorum system (#140, #141): the seed's
        // policy at the successor's own size — or, on a flexible
        // or a grid seed, a coin that composes a *majority*
        // successor instead, so the cross-configuration Phase 1
        // asks two different systems their own predicates. Only
        // that direction: a majority successor always sits
        // inside the copy budget a flexible or a grid policy was
        // sized for (its tolerated loss is never below theirs),
        // while a split or a grid on a majority seed would not,
        // so a majority seed never composes one. A grid policy
        // switches on its own too, at every size no layout
        // tiles (`QuorumPolicy::system`).
        let switch_to_majority = matches!(
            policy,
            crate::shape::QuorumPolicy::Flexible { .. } | crate::shape::QuorumPolicy::Grid { .. }
        ) && buggify_with_prob!(0.25);
        let successor_system = |n: usize| {
            if switch_to_majority {
                QuorumSystem::Majority
            } else {
                policy.system(n)
            }
        };
        // A live quorum of *both* phases under the successor's
        // own system: Phase 1 must complete against it (it is in
        // `H_b` from then on) and Phase 2 must decide under it.
        // Asked of the membership boundary, never a count. On a
        // grid, Phase 2 is asked of **every** column: each slot
        // is decided by its own column (`column_of`), so one
        // live column decides only its own slots, and a column
        // with a member lost for good freezes the rest — the
        // leader's recovery never closes and no later
        // reconfiguration can move the dead member out (#198).
        let keeps_live_quorum = |next: &[u64]| {
            let live_members: BTreeSet<u64> =
                next.iter().filter(|m| live.contains(m)).copied().collect();
            let system = successor_system(next.len());
            let columns: BTreeSet<usize> = (0..next.len() as u64)
                .filter_map(|slot| system.column_of(paros::Slot(slot)))
                .collect();
            let phase2 = if columns.is_empty() {
                system.is_phase2_quorum(next, &live_members)
            } else {
                columns
                    .iter()
                    .all(|column| system.is_phase2_quorum_in(next, &live_members, Some(*column)))
            };
            system.is_phase1_quorum(next, &live_members) && phase2
        };
        // Most shapes need a spare, which most seeds do not have:
        // walk the shape ring from the draw so an impossible
        // shape falls through to the next one instead of making
        // the whole step a silent no-op.
        let compose_from = |candidates: &[u64]| {
            members.as_deref().and_then(|members| {
                (0..RECONFIGURE_SHAPES.len()).find_map(|k| {
                    let shape = (drawn + k) % RECONFIGURE_SHAPES.len();
                    compose_reconfiguration(
                        shape,
                        members,
                        candidates,
                        config_floor,
                        leader_id,
                        raw_payload,
                        whole_rotation,
                    )
                    .filter(|(_, _, next)| keeps_live_quorum(next))
                    .map(|(observed, name, next)| (shape, observed, name, next))
                })
            })
        };
        // A registered joiner (#189) is one spare among the
        // pool's, and growing onto it is the rare step the
        // system board's "joins a journal's configuration
        // through Reconfigure" gate waits on: on a coin of the
        // step's policy draw, a step with one joinable draws the
        // new member from the joiners alone (the members in
        // force stay candidates), falling back to the whole
        // live pool when no shape holds — always on the step
        // right after a registration. A composition policy,
        // like the shape draw — a per-seed BUGGIFY activation
        // left the CI sweep's 1,024 seeds short of the gate.
        let prefer_joiner = !joinable.is_empty() && (after_register || (raw_policy >> 11) % 2 == 0);
        let joiner_first: Vec<u64> = live
            .iter()
            .copied()
            .filter(|n| joinable.contains(n) || members.as_deref().is_some_and(|m| m.contains(n)))
            .collect();
        let composed = if adversarial_members {
            compose_from(&all_ranks).or_else(|| compose_from(&live))
        } else if prefer_joiner {
            let onto_joiner = compose_from(&joiner_first);
            if onto_joiner.is_some() {
                assert_reachable!(
                    "reconfiguration: the composer draws a successor's new member from the registered joiners"
                );
            }
            onto_joiner.or_else(|| compose_from(&live))
        } else {
            compose_from(&live)
        };
        if let Some((shape, observed, name, next)) = composed {
            assert_always!(
                keeps_live_quorum(&next),
                "reconfiguration: a requested configuration keeps a live quorum",
                {
                    "members" => next.len() as u64,
                    "live" => next.iter().filter(|m| live.contains(m)).count() as u64
                }
            );
            if next.iter().any(|m| !live.contains(m)) {
                assert_reachable!(
                    "reconfiguration: a requested configuration names an identity lost for good"
                );
            }
            if shape != drawn {
                assert_reachable!(
                    "reconfiguration: the drawn shape is impossible and the step falls through"
                );
            }
            if after_claim && REMOVING_SHAPES.contains(&shape) {
                // BUGGIFY pairing: the owner's first operation
                // after its claim moves a member out.
                assert_reachable!(
                    "reconfiguration: an owner's first operation after its claim removes a member"
                );
            }
            let disjoint = members
                .as_deref()
                .is_some_and(|in_force| in_force.iter().all(|m| !next.contains(m)));
            if disjoint {
                assert_reachable!(
                    "reconfiguration: a successor acceptor set shares no member with its predecessor"
                );
            }
            let mut system = successor_system(next.len());
            if system_in_force.is_some_and(|in_force| {
                std::mem::discriminant(&in_force) != std::mem::discriminant(&system)
            }) {
                assert_reachable!(
                    "reconfiguration: the client composes a successor under a different quorum system"
                );
            }
            // The adversarial half (R5's spirit): an operator who
            // names a quorum system the membership does not admit
            // must be refused at the wire, never crash the node.
            // The workload judges "malformed" with its own
            // arithmetic, never the library's `admits` (#269: a
            // mutant of `admits` also silenced the request).
            let malformed = buggify_with_prob!(0.05)
                .then(|| malformed_system(next.len(), ctx.random().random::<u64>()))
                .flatten();
            if let Some(bad) = malformed {
                system = bad;
            }
            let malformed = malformed.is_some();
            tracing::info!(shape = name, members = ?next, ?system, "chain_reconfigure_request");
            *remove_next = false;
            // The operators' ledger (#198): filed before the
            // request leaves, answered below; a retirement reads
            // it (`StorageWorld::retire`).
            let ledger_id = ledger_request(ctx.state(), &next);
            let outcome = reconfigurer.reconfigure(&next, system, probe_target).await;
            tracing::info!(shape = name, outcome = ?outcome, "chain_reconfigure_outcome");
            ledger_answer(ctx.state(), ledger_id, &outcome);
            match outcome {
                ReconfigureOutcome::Started { leader, .. } => {
                    // The AGENTS.md rule, client-visible: a
                    // deployment without matchmakers never honors
                    // a reconfiguration.
                    assert_always!(
                        has_matchmakers,
                        "reconfiguration: a deployment without matchmakers never accepts a reconfiguration",
                        { "shape" => name }
                    );
                    assert_always!(
                        !malformed,
                        "reconfiguration: a configuration that does not admit its quorum system is never started",
                        { "shape" => name }
                    );
                    self.adversarial.reconfigure_started[observed] = true;
                    nodes.observe_leader(leader);
                    // A rare-but-valid operator act (#173):
                    // reboot every member of the configuration
                    // just installed. Each loses its belief in
                    // force and boots to the bootstrap one, so a
                    // successor disjoint from the bootstrap is a
                    // cluster whose members all believe they are
                    // outside the configuration in force, and
                    // whose non-members know better but do not
                    // lead. A clean reboot keeps every disk. A
                    // successor sharing no member with its
                    // predecessor is the shape that leaves no
                    // rebooted member inside the default, so it
                    // is the one the location leans on.
                    if buggify_with_prob!(if disjoint { 0.9 } else { 0.25 }) {
                        let _ = time
                            .sleep(Duration::from_millis(config.reboot_successor_delay_ms))
                            .await;
                        assert_reachable!(
                            "reconfiguration: the client reboots every member of the configuration it installed"
                        );
                        for member in &next {
                            if let Some(ip) = usize::try_from(*member)
                                .ok()
                                .and_then(|rank| servers.get(rank))
                            {
                                crate::lifecycle::restart(ctx, ip).await;
                            }
                        }
                    }
                }
                ReconfigureOutcome::Refused { leader, refusal } => {
                    if refusal == ReconfigureRefusal::NoMatchmakers {
                        assert_always!(
                            !has_matchmakers,
                            "reconfiguration: only a deployment without matchmakers refuses for lack of them",
                            { "shape" => name }
                        );
                        self.adversarial.reconfigure_refused_plain = true;
                    }
                    if refusal == ReconfigureRefusal::Malformed {
                        assert_always!(
                            malformed,
                            "reconfiguration: only a configuration that does not admit its quorum system is refused as malformed",
                            { "shape" => name }
                        );
                        assert_reachable!(
                            "reconfiguration: a configuration that does not admit its quorum system is refused"
                        );
                    }
                    adopt_plane_leader(nodes, has_matchmakers, leader);
                }
                ReconfigureOutcome::NotLeader { leader }
                | ReconfigureOutcome::Unrecognized { leader } => {
                    adopt_plane_leader(nodes, has_matchmakers, leader);
                }
                ReconfigureOutcome::Ambiguous => {}
            }
        }
    }

    /// One `RECONFIGURE_MATCHMAKERS` step (#125): ask for a successor
    /// matchmaker set, composed from the set in force.
    pub(super) async fn reconfigure_matchmakers_step(&mut self, step: &Step<'_>) {
        let Step {
            config,
            ctx,
            has_matchmakers,
            journal,
            matchmaker_floor,
            matchmaker_ips,
            matchmaker_reconfigurer,
            nodes,
            raw_class,
            raw_payload,
            target,
            ..
        } = *step;
        // Any node may drive a matchmaker handover, and every
        // node learns the authoritative set: read it from the
        // step's target and ask that same node. A stale answer
        // only makes the handover superseded or refused — an
        // operating condition, never a wrong state.
        let current: Option<(u64, Vec<u64>)> = nodes
            .inspect(target, journal)
            .await
            .map(|reply| (reply.matchmaker_generation, reply.matchmakers));
        let drawn_slot = weighted_index(&config.matchmaker_shape_weights, raw_class);
        let candidates = live_candidates(
            matchmaker_ips,
            &crate::world::parked_matchmakers(ctx.state()),
        );
        let request = if has_matchmakers {
            // The same shape ring as the acceptor composer: a
            // matchmaker set at its floor admits no shrink and a
            // full bootstrap leaves no spare, so a fixed shape
            // would make the step a no-op for the whole run.
            current.as_ref().and_then(|(_, members)| {
                (0..MATCHMAKER_SHAPES.len()).find_map(|k| {
                    let slot = (drawn_slot + k) % MATCHMAKER_SHAPES.len();
                    compose_reconfiguration(
                        MATCHMAKER_SHAPES[slot],
                        members,
                        &candidates,
                        matchmaker_floor,
                        None,
                        raw_payload,
                        false,
                    )
                    .map(|(observed, name, next)| {
                        // The observed shape's own slot: a
                        // rotation that came out a shrink is
                        // gated as the shrink it is.
                        let observed_slot = MATCHMAKER_SHAPES
                            .iter()
                            .position(|s| *s == observed)
                            .unwrap_or(slot);
                        (slot, observed_slot, name, next)
                    })
                })
            })
        } else {
            // Plain Multi-Paxos: the request is sent anyway, and
            // the point is the refusal.
            current
                .is_some()
                .then_some((drawn_slot, drawn_slot, "plain", vec![0]))
        };
        if let Some((shape_slot, observed_slot, name, next)) = request {
            if shape_slot != drawn_slot {
                assert_reachable!(
                    "reconfiguration: the drawn shape is impossible and the step falls through"
                );
            }
            if current
                .as_ref()
                .is_some_and(|(_, in_force)| in_force.iter().all(|m| !next.contains(m)))
            {
                assert_reachable!(
                    "generation: a successor matchmaker set shares no member with its predecessor"
                );
            }
            tracing::info!(shape = name, members = ?next, "chain_reconfigure_matchmakers_request");
            let outcome = matchmaker_reconfigurer
                .reconfigure_matchmakers(&next, target)
                .await;
            tracing::info!(shape = name, outcome = ?outcome, "chain_reconfigure_matchmakers_outcome");
            match outcome {
                ReconfigureMatchmakersOutcome::Started { generation } => {
                    assert_always!(
                        has_matchmakers,
                        "generation: a deployment without matchmakers never accepts a matchmaker reconfiguration",
                        { "shape" => name }
                    );
                    // The node may have learned a newer generation
                    // between the read and the request (a handover
                    // completed in between): the set it starts
                    // from is its own, and the client's stale
                    // composition is what a rotate through the
                    // pool looks like — an operating condition.
                    tracing::info!(
                        shape = name,
                        generation,
                        "chain_reconfigure_matchmakers_started"
                    );
                    self.adversarial.reconfigure_matchmakers_started[observed_slot] = true;
                }
                ReconfigureMatchmakersOutcome::Refused(refusal) => {
                    assert_always!(
                        (refusal == MatchmakersRefusal::NoMatchmakers) != has_matchmakers,
                        "generation: only a deployment without matchmakers refuses for lack of them",
                        { "shape" => name, "refusal" => format!("{refusal:?}") }
                    );
                }
                ReconfigureMatchmakersOutcome::Ambiguous => {}
            }
        }
    }

    /// One `RETIRE` step (#123): ask a node to retire, carrying the effective
    /// GC watermark as its evidence.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn retire_step(&mut self, step: &Step<'_>) {
        let Step {
            audit,
            ctx,
            has_matchmakers,
            journal,
            nodes,
            policy,
            raw_payload,
            reconfigurer,
            server_count,
            servers,
            target,
            ..
        } = *step;
        // Only a leader reports what its effective floor retired;
        // a follower answers an empty list and the step is a
        // no-op.
        let probe_target = nodes.leader().unwrap_or(target);
        // The retirable list, the configuration in force and the
        // effective GC watermark come from the *same* reply: the
        // world can hold the protocol to "a retirable node is
        // outside C_b", and the node itself refuses the request
        // unless the watermark proves every configuration it was
        // a member of is forgotten (#123).
        let inspected = nodes.inspect(probe_target, journal).await;
        let (retirable, in_force, gc_watermark) = inspected
            .map(|reply| (reply.retirable, reply.members, reply.gc_watermark))
            .unwrap_or_default();
        assert_always!(
            retirable.is_empty() || has_matchmakers,
            "gc: a deployment without matchmakers never names a retirable node"
        );
        let parked = crate::world::parked_nodes(ctx.state(), journal);
        let live = |id: &u64| {
            usize::try_from(*id)
                .ok()
                .filter(|i| *i < server_count && !parked.contains(&servers[*i]))
        };
        // The stale member (#165), its own location: a member of
        // the configuration the floor kept whose *own* belief
        // does not name it — it never heard that configuration,
        // or rebooted to its bootstrap belief and has not heard a
        // beat since. The window is narrow, so a blind aim almost
        // never lands in it; this operator asks every member at
        // once what it believes (one request timeout for all) and
        // aims at the first that does not know it is one. The
        // node must still refuse (`stale`). None found: the
        // ordinary draw below, so the retirement mix is kept.
        let mut stale_member = None;
        if gc_watermark.is_some() && buggify_with_prob!(0.25) {
            assert_reachable!("gc: an operator probes the members' beliefs before a retirement");
            let candidates: Vec<usize> = in_force.iter().filter_map(live).collect();
            let beliefs = join_all(candidates.iter().map(|i| nodes.inspect(*i, journal))).await;
            stale_member = candidates.iter().zip(beliefs).find_map(|(i, reply)| {
                let own = u64::try_from(*i).unwrap_or(u64::MAX);
                reply
                    .is_some_and(|reply| !reply.members.contains(&own))
                    .then_some(*i)
            });
            if stale_member.is_some() {
                assert_reachable!(
                    "gc: a retirement is aimed at a member whose belief does not name it"
                );
            }
        }
        // The adversarial aim (R5): send the retirement to a node
        // the *same* reply names as a current member instead of a
        // retirable one. A well-behaved operator would not; the
        // node must refuse it (`member`, or `leader` when it is
        // the sitting one), so the world reservation is skipped
        // for this draw — nothing is parked, and a refusal has
        // nothing to release.
        let aim_at_member = stale_member.is_some() || buggify_with_prob!(0.10);
        let pool: &[u64] = if aim_at_member { &in_force } else { &retirable };
        let victims: Vec<usize> = pool.iter().filter_map(live).collect();
        if aim_at_member && !victims.is_empty() {
            assert_reachable!("gc: a retirement is aimed at a current member");
        }
        if !victims.is_empty() {
            let victim = stale_member.unwrap_or(
                victims[usize::try_from(raw_payload % u64::try_from(victims.len()).unwrap_or(1))
                    .unwrap_or(0)],
            );
            // The racing operator (#198), its own location: a
            // reconfiguration that puts the victim back, asked
            // for just before the retirement — the order two
            // uncoordinated clients produced (the re-add is
            // registered, and on its way to the victim, when the
            // victim accepts its retirement). The operators'
            // ledger must withhold the retirement; without it a
            // grid successor is installed with a member dead for
            // good.
            if !aim_at_member && has_matchmakers && !in_force.is_empty() && buggify_with_prob!(0.25)
            {
                let mut readd = in_force.clone();
                readd.push(u64::try_from(victim).unwrap_or(u64::MAX));
                readd.sort_unstable();
                readd.dedup();
                assert_reachable!("gc: an operator asks to re-add a node just before retiring it");
                let ledger_id = ledger_request(ctx.state(), &readd);
                let system = policy.system(readd.len());
                let outcome = reconfigurer.reconfigure(&readd, system, probe_target).await;
                ledger_answer(ctx.state(), ledger_id, &outcome);
            }
            // Park the identity first, under the dead-node budget
            // (a retirement is one more way to lose every copy a
            // node holds); a restart of a parked identity exits
            // at boot, so an ambiguous ack can never bring it
            // back. Refused by the budget: the step is a no-op.
            let reserved = if aim_at_member {
                // No reservation: the target is a member, the
                // node refuses, and parking it would remove a
                // live acceptor the protocol still names.
                true
            } else {
                let world = crate::world::storage_world(ctx.state());
                let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
                guard.retire(
                    &servers[victim],
                    u64::try_from(victim).unwrap_or(u64::MAX),
                    &in_force,
                    gc_watermark.map_or(0, |w| w.round),
                )
            };
            if reserved {
                tracing::info!(node = victim as u64, "chain_retire_request");
                let outcome = nodes.retire(victim, RetireRequest { gc_watermark }).await;
                tracing::info!(node = victim as u64, outcome = ?outcome, "chain_retire_outcome");
                match outcome {
                    RetireOutcome::Retired => self.adversarial.retired = true,
                    RetireOutcome::Refused(_) => {
                        // Refused means the node is a member of
                        // the configuration in force, is the
                        // leader, or no effective floor sits
                        // above its membership fence: it is still
                        // live, so the pre-emptive park must be
                        // undone or the harness has removed a
                        // member outside the protocol. Only ever
                        // on an explicit refusal — an ambiguous
                        // ack may have been honored.
                        self.adversarial.retire_refused = true;
                        let world = crate::world::storage_world(ctx.state());
                        let released = world
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .release_retirement(
                                &servers[victim],
                                u64::try_from(victim).unwrap_or(u64::MAX),
                            );
                        self.adversarial.retire_released |= released;
                    }
                    // Ambiguous: the park stands for good (an
                    // honored retirement must never come back),
                    // so the audit excuses the identity now
                    // rather than at a boot that may never come.
                    RetireOutcome::Ambiguous if !aim_at_member => {
                        audit.note_retired_parked(u64::try_from(victim).unwrap_or(u64::MAX));
                    }
                    RetireOutcome::Ambiguous => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape composer, pinned at the mechanism: each shape moves the set
    /// the way its name says, never below the floor, never onto a node outside
    /// the pool, and never to the set already in force.
    #[test]
    fn reconfiguration_shapes_respect_the_floor_and_the_pool() {
        let members = [1_u64, 2, 3];
        let pool5 = [0_u64, 1, 2, 3, 4];
        let grow = compose_reconfiguration(0, &members, &pool5, 3, Some(1), 7, false).unwrap();
        assert_eq!(grow.1, "grow");
        assert_eq!(grow.2.len(), 4);
        assert!(grow.2.iter().all(|n| *n < 5));
        assert!(
            compose_reconfiguration(0, &[0, 1, 2], &[0, 1, 2], 3, None, 0, false).is_none(),
            "no spare"
        );
        assert!(
            compose_reconfiguration(1, &members, &pool5, 3, None, 0, false).is_none(),
            "at the floor"
        );
        let shrink = compose_reconfiguration(1, &[0, 1, 2, 3], &pool5, 3, None, 2, false).unwrap();
        assert_eq!((shrink.1, shrink.2.len()), ("shrink", 3));
        let replace = compose_reconfiguration(2, &members, &pool5, 3, None, 1, false).unwrap();
        assert_eq!(replace.1, "replace");
        assert_eq!(replace.2.len(), 3);
        assert_ne!(replace.2, members.to_vec());
        assert!(
            compose_reconfiguration(3, &members, &pool5, 3, Some(1), 0, false).is_none(),
            "removing the leader at the floor is refused"
        );
        let removed =
            compose_reconfiguration(3, &[0, 1, 2, 3], &pool5, 3, Some(2), 0, false).unwrap();
        assert_eq!(
            (removed.1, removed.2.clone()),
            ("remove-leader", vec![0, 1, 3])
        );
        assert!(
            compose_reconfiguration(3, &[0, 1, 2, 3], &pool5, 3, None, 0, false).is_none(),
            "no leader known"
        );
        let rotate =
            compose_reconfiguration(4, &[0, 1, 2], &[0, 1, 2, 3, 4, 5], 3, None, 2, false).unwrap();
        assert_eq!((rotate.1, rotate.2.clone()), ("rotate", vec![3, 4, 5]));
        // A whole-set rotation draws the successor off the spares alone,
        // whatever the draw; with too few spares it is an ordinary one.
        for draw in 0..6 {
            let whole =
                compose_reconfiguration(4, &[1, 2, 3], &[0, 1, 2, 3, 4, 5, 6], 3, None, draw, true)
                    .unwrap();
            assert_eq!(whole.1, "rotate");
            assert!(whole.2.iter().all(|n| ![1, 2, 3].contains(n)));
        }
        let few_spares =
            compose_reconfiguration(4, &[0, 1, 2], &[0, 1, 2, 3, 4], 3, None, 2, true).unwrap();
        assert_eq!(few_spares.2.len(), 3);
        // A rotation through a ring no larger than the set in force drops a
        // member instead of replacing it: observed as the shrink it is.
        let short_ring =
            compose_reconfiguration(4, &[0, 1, 2, 3], &[0, 2, 3], 3, None, 1, false).unwrap();
        assert_eq!(
            (short_ring.1, short_ring.2.clone()),
            ("shrink", vec![0, 2, 3])
        );
        // A dead member (outside the candidates) is the first one moved out.
        let heal = compose_reconfiguration(2, &[0, 1, 2], &[0, 2, 3], 3, None, 0, false).unwrap();
        assert_eq!((heal.1, heal.2.clone()), ("replace", vec![0, 2, 3]));
        let drop_dead =
            compose_reconfiguration(1, &[0, 1, 2, 3], &[0, 2, 3], 3, None, 5, false).unwrap();
        assert_eq!(
            (drop_dead.1, drop_dead.2.clone()),
            ("shrink", vec![0, 2, 3])
        );
        assert!(
            compose_reconfiguration(0, &members, &[], 3, None, 0, false).is_none(),
            "no live candidate at all"
        );
        assert!(
            compose_reconfiguration(4, &[0, 1, 2], &[0, 1, 2], 3, None, 0, false).is_none(),
            "a rotation through a pool with no spare is the same set"
        );
    }
}
