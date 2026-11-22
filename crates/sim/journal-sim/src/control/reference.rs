//! The **reference decision**: `decide` written again from
//! `docs/journal/06-controller.md` §J6.2 and the order of the rules of
//! `paros_journal_control::decide`, with no code of `paros_journal_control`
//! (§J6.2, test 2). It counts members where the crate asks `membership/`,
//! judges the zone rule by its definition, and finds a placement by trying
//! every assignment in order.

use std::collections::BTreeSet;

use paros_journal::segment::{OpeningCall, SegmentRecord, SegmentState};
use paros_journal::{Grid, NodeId};
use paros_journal_control::{Action, Alert, Decision, GridState, Liveness, Wait};

use super::{Case, ME, TUNABLES};

fn is(case: &Case, node: NodeId, liveness: Liveness) -> bool {
    case.liveness.get(&node) == Some(&liveness)
}

/// The members of a column that are up, against `F + 1`.
fn up_in(case: &Case, column: &[NodeId]) -> usize {
    column
        .iter()
        .filter(|m| is(case, **m, Liveness::Up))
        .count()
}

/// The journal state of the grid in force, from the table of §J6.2.
#[must_use]
pub fn state(case: &Case) -> GridState {
    state_of(case, &case.grid)
}

fn state_of(case: &Case, grid: &Grid) -> GridState {
    let acks = usize::from(grid.f()) + 1;
    let ups: Vec<usize> = grid.columns().iter().map(|c| up_in(case, c)).collect();
    let healthy = ups.iter().all(|u| *u > acks);
    let electable = ups.iter().all(|u| *u >= acks);
    let writable = ups.iter().any(|u| *u >= acks);
    if healthy {
        GridState::Healthy
    } else if electable && placement(case, grid).is_some() {
        GridState::AtRisk
    } else if electable {
        GridState::AtRiskUnplaceable
    } else if !writable {
        GridState::Stopped
    } else if case.leader_live {
        GridState::WritesDegraded
    } else {
        GridState::ElectionBlocked
    }
}

/// The zone rule by its definition: unlabeled when no member has a zone;
/// otherwise every member has one, and for every zone, the members outside
/// it hold a majority of every column and the members inside it a majority
/// of no column.
fn zones_admit(case: &Case, grid: &Grid) -> bool {
    let members: Vec<NodeId> = grid.columns().iter().flatten().copied().collect();
    let labeled = members
        .iter()
        .filter(|m| case.zones.contains_key(m))
        .count();
    if labeled == 0 {
        return true;
    }
    if labeled < members.len() {
        return false;
    }
    let zones: BTreeSet<_> = members.iter().map(|m| &case.zones[m]).collect();
    zones.iter().all(|zone| {
        grid.columns().iter().all(|column| {
            let inside = column.iter().filter(|m| &&case.zones[*m] == zone).count();
            let outside = column.len() - inside;
            2 * outside > column.len() && 2 * inside <= column.len()
        })
    })
}

fn usable(case: &Case, members: &[NodeId]) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    for c in &case.candidates {
        if is(case, *c, Liveness::Up) && !members.contains(c) && !out.contains(c) {
            out.push(*c);
        }
    }
    out
}

/// The first placement in order: the holes (the members that are down,
/// column by column) take candidates in the host's order, the first
/// assignment that admits the zone rule wins.
fn placement(case: &Case, grid: &Grid) -> Option<Grid> {
    let members: Vec<NodeId> = grid.columns().iter().flatten().copied().collect();
    let holes: Vec<usize> = (0..members.len())
        .filter(|i| is(case, members[*i], Liveness::Down))
        .collect();
    let pool = usable(case, &members);
    if holes.len() > pool.len() {
        return None;
    }
    // Every injective map from holes to the pool, in lexicographic order of
    // the pool indices.
    let mut pick = vec![0usize; holes.len()];
    loop {
        let distinct = pick.iter().collect::<BTreeSet<_>>().len() == pick.len();
        if distinct {
            let mut flat = members.clone();
            for (h, p) in holes.iter().zip(&pick) {
                flat[*h] = pool[*p];
            }
            let rows = grid.rows();
            let columns: Vec<Vec<NodeId>> = flat.chunks(rows).map(<[NodeId]>::to_vec).collect();
            let candidate = Grid::new(grid.f(), columns).expect("a v1 grid");
            if zones_admit(case, &candidate) {
                return Some(candidate);
            }
        }
        // The next tuple.
        let mut i = pick.len();
        loop {
            if i == 0 {
                return None;
            }
            i -= 1;
            pick[i] += 1;
            if pick[i] < pool.len() {
                for later in &mut pick[i + 1..] {
                    *later = 0;
                }
                break;
            }
        }
    }
}

fn replica_placement(case: &Case, set: &[NodeId]) -> Option<Vec<NodeId>> {
    let pool = usable(case, set);
    let mut next = pool.into_iter();
    set.iter()
        .map(|m| {
            if is(case, *m, Liveness::Down) {
                next.next()
            } else {
                Some(*m)
            }
        })
        .collect()
}

/// Whether a plan from `old` to `new` (and its replica sets) names no
/// member that is down and admits the zone rule.
#[must_use]
pub fn plan_is_safe(
    case: &Case,
    old: &Grid,
    new: &Grid,
    old_replicas: &[NodeId],
    new_replicas: &[NodeId],
) -> bool {
    let shape = old.f() == new.f()
        && old.columns().len() == new.columns().len()
        && old_replicas.len() == new_replicas.len();
    let before: BTreeSet<NodeId> = old
        .columns()
        .iter()
        .flatten()
        .chain(old_replicas)
        .copied()
        .collect();
    // A member that stays may be suspect; a member that joins is up; no
    // member is down.
    let fit = new.columns().iter().flatten().chain(new_replicas).all(|m| {
        !is(case, *m, Liveness::Down) && (before.contains(m) || is(case, *m, Liveness::Up))
    });
    shape && fit && zones_admit(case, new)
}

/// The decision of the reference.
#[must_use]
pub fn decide(case: &Case) -> Decision {
    let mut alerts = BTreeSet::new();
    let action = act(case, &mut alerts);
    Decision { action, alerts }
}

fn act(case: &Case, alerts: &mut BTreeSet<Alert>) -> Action {
    let term = case.register.term();
    if term.holder != Some(ME) || term.number != 1 {
        return Action::Stop;
    }
    if case.signals.data_lost {
        alerts.insert(Alert::DataLost);
    }
    // Five matchmakers: a majority is three.
    let live = case.matchmakers_live.len();
    let frozen = live < 3;
    if frozen {
        alerts.insert(Alert::ControlFrozen);
    } else if live == 3 {
        alerts.insert(Alert::MatchmakersAtRisk);
    }
    let Some(in_force) = case.register.in_force() else {
        return Action::Wait(Wait::NoSegment);
    };
    assert!(
        in_force.grid() == &case.grid,
        "the sweep's grid is in force"
    );
    let state = state(case);
    match state {
        GridState::Healthy | GridState::AtRisk => {}
        GridState::AtRiskUnplaceable => {
            alerts.insert(Alert::Unplaceable);
        }
        GridState::WritesDegraded => {
            alerts.insert(Alert::WritesDegraded);
        }
        GridState::ElectionBlocked => {
            alerts.insert(Alert::ElectionBlocked);
        }
        GridState::Stopped => {
            alerts.insert(Alert::Stopped);
        }
    }
    let reshaping = case.register.segments().values().any(|r| {
        r.id() != in_force.id()
            && !matches!(
                r.state(),
                SegmentState::Retired | SegmentState::Offloaded | SegmentState::Trimmed
            )
    });
    if reshaping
        && case
            .signals
            .seal_age
            .is_some_and(|a| a > TUNABLES.stall_window)
    {
        alerts.insert(Alert::ReshapeStalled);
    }
    if frozen {
        return Action::Wait(Wait::ControlFrozen);
    }
    if case.signals.signal_age > TUNABLES.stale_after {
        return Action::Wait(Wait::Stale);
    }
    if let Some(segment) = case.repairs.iter().next() {
        return Action::Repair { segment: *segment };
    }
    if let Some(action) = rereplicate(case, alerts) {
        return action;
    }
    heal(case, in_force, state, reshaping, alerts)
}

/// The re-replication of the first sealed or retired segment with a
/// replica that is down.
fn rereplicate(case: &Case, alerts: &mut BTreeSet<Alert>) -> Option<Action> {
    let copy = case.register.segments().values().find(|r| {
        matches!(r.state(), SegmentState::Sealed | SegmentState::Retired)
            && r.replicas().iter().any(|m| is(case, *m, Liveness::Down))
    });
    if let Some(record) = copy {
        let Some(to) = replica_placement(case, record.replicas()) else {
            alerts.insert(Alert::ReplicasUnplaceable);
            return Some(Action::Wait(Wait::NoReplicaPlacement));
        };
        if !case.signals.token_free {
            return Some(Action::Wait(Wait::NoToken));
        }
        return Some(Action::Rereplicate {
            segment: record.id(),
            from: record.replicas().to_vec(),
            to,
        });
    }
    None
}

/// The heal reshape of the segment in force.
fn heal(
    case: &Case,
    in_force: &SegmentRecord,
    state: GridState,
    reshaping: bool,
    alerts: &mut BTreeSet<Alert>,
) -> Action {
    let members: Vec<NodeId> = case.grid.columns().iter().flatten().copied().collect();
    let acceptors_down = members.iter().any(|m| is(case, *m, Liveness::Down));
    let replicas_down = in_force
        .replicas()
        .iter()
        .any(|m| is(case, *m, Liveness::Down));
    match state {
        GridState::Healthy if !replicas_down => return Action::Idle,
        GridState::AtRisk if !acceptors_down && !replicas_down => {
            return Action::Wait(Wait::Suspect);
        }
        GridState::Healthy | GridState::AtRisk => {}
        held => return Action::Wait(Wait::Hold(held)),
    }
    if reshaping {
        return Action::Wait(Wait::Reshaping);
    }
    if case
        .signals
        .since_last_reshape
        .is_some_and(|s| s < TUNABLES.min_reshape_interval)
    {
        return Action::Wait(Wait::Interval);
    }
    if !case.signals.token_free {
        return Action::Wait(Wait::NoToken);
    }
    let Some(grid) = placement(case, &case.grid) else {
        alerts.insert(Alert::Unplaceable);
        return Action::Wait(Wait::NoGridPlacement);
    };
    let Some(replicas) = replica_placement(case, in_force.replicas()) else {
        alerts.insert(Alert::ReplicasUnplaceable);
        return Action::Wait(Wait::NoReplicaPlacement);
    };
    Action::Reshape(OpeningCall {
        term: 1,
        expected: case.register.newest().map(SegmentRecord::id),
        grid,
        replicas,
    })
}
