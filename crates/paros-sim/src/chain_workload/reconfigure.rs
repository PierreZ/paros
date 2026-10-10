//! The chain client's reconfigurations: the `RECONFIGURE` operation, the
//! shape rings, the set composer and the operators' ledger.

use std::sync::PoisonError;

use paros::QuorumSystem;
use paros::client::ReconfigureOutcome;

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
