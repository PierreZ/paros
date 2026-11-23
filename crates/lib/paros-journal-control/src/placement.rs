//! The **placement of a heal** (`docs/journal/06-controller.md` §J6.2,
//! "A zone loss"): which machines replace the members that are down, so
//! that the new grid meets the zone rule.
//!
//! The host offers the candidates: the machines that are up and may take a
//! role, in its order of preference (the placement trait of the host, which
//! parosd provides). This module only picks among them. A heal keeps every
//! member that is not down in its column and puts a candidate in the place
//! of each member that is down, so no survivor changes column.
//!
//! The replica set of a segment keeps the same rule as one column: the
//! replicas outside one domain hold a majority, the replicas inside one
//! domain hold none. A retired segment's replicas hold its only copy (v1 has
//! no offload), and the leader retires it at `F + 1` holders, so a set with
//! a majority in one zone loses the segment with that zone; and a heal into
//! a zone that already holds a replica can leave no free machine there for
//! a later heal of the grid (`journal-sim`'s whole walk, a machine lost for
//! good).
//!
//! The zone rule of a v1 grid is the zone rule of each of its columns: a
//! Phase-1 quorum of a v1 grid is a majority of every column and a Phase-2
//! quorum a majority of one, so "the members outside one domain hold a
//! Phase-1 and a Phase-2 quorum" is "they hold a majority of every column",
//! and "the members inside one domain hold no Phase-2 quorum" is "they hold
//! a majority of no column". Each column asks
//! [`AcceptorConfig::zone_rule`].

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::NodeId;
use paros_consensus::membership::{AcceptorConfig, FailureDomain, QuorumSystem, ZoneRule};
use paros_journal::Grid;

use crate::measure::Liveness;

/// The zone rule over a whole grid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GridZones {
    /// No member carries a failure domain: the rule is not asked (the plain
    /// arm of [`ZoneRule::Unlabeled`]).
    Unlabeled,
    /// Some members carry a domain and this one does not: the rule cannot
    /// be judged, so it does not hold.
    PartlyLabeled(NodeId),
    /// Every column holds the zone rule.
    Holds,
    /// A column breaks it.
    Breaks(ZoneRule),
}

impl GridZones {
    /// Whether a plan with this verdict may be asked for: the rule holds,
    /// or no member is labeled.
    #[must_use]
    pub fn admits(&self) -> bool {
        matches!(self, Self::Unlabeled | Self::Holds)
    }
}

/// The zone rule over `grid` with the domains of `zones`.
///
/// # Panics
///
/// When an assertion on its own postconditions fails: a programmer error,
/// never an operating condition.
#[must_use]
pub fn grid_zones(grid: &Grid, zones: &BTreeMap<NodeId, FailureDomain>) -> GridZones {
    let members: Vec<NodeId> = grid.columns().iter().flatten().copied().collect();
    let labeled = members.iter().filter(|m| zones.contains_key(m)).count();
    if labeled == 0 {
        return GridZones::Unlabeled;
    }
    if let Some(unlabeled) = members.iter().find(|m| !zones.contains_key(m)) {
        return GridZones::PartlyLabeled(*unlabeled);
    }
    for column in grid.columns() {
        let rule = column_rule(column, zones);
        if !rule.holds() {
            return GridZones::Breaks(rule);
        }
    }
    GridZones::Holds
}

/// The zone rule of one group of labeled members, asked as a majority.
fn column_rule(column: &[NodeId], zones: &BTreeMap<NodeId, FailureDomain>) -> ZoneRule {
    let config = AcceptorConfig::zoned(
        column.iter().map(|m| (*m, zones[m].clone())).collect(),
        QuorumSystem::Majority,
    );
    let rule = config.zone_rule();
    assert!(
        rule != ZoneRule::Unlabeled,
        "a column of labeled members is labeled"
    );
    rule
}

/// The zone rule over a replica set, as over one column.
#[must_use]
pub fn set_zones(set: &[NodeId], zones: &BTreeMap<NodeId, FailureDomain>) -> GridZones {
    let labeled = set.iter().filter(|m| zones.contains_key(m)).count();
    if labeled == 0 {
        return GridZones::Unlabeled;
    }
    if let Some(unlabeled) = set.iter().find(|m| !zones.contains_key(m)) {
        return GridZones::PartlyLabeled(*unlabeled);
    }
    let rule = column_rule(set, zones);
    if rule.holds() {
        GridZones::Holds
    } else {
        GridZones::Breaks(rule)
    }
}

/// The members of `members` that are down.
#[must_use]
pub fn down(members: &[NodeId], liveness: &BTreeMap<NodeId, Liveness>) -> Vec<NodeId> {
    members
        .iter()
        .copied()
        .filter(|m| liveness.get(m) == Some(&Liveness::Down))
        .collect()
}

/// The new grid of a heal: each member of `grid` that is down replaced by a
/// candidate that is up and not in the grid, such that the new grid admits
/// the zone rule ([`GridZones::admits`]). The search tries the candidates in
/// the host's order and backtracks, so it finds a placement whenever one
/// exists. `None`: no placement exists. With no member down, the grid
/// itself, when it admits the rule.
///
/// # Panics
///
/// When an assertion on its own postconditions fails: a programmer error,
/// never an operating condition.
#[must_use]
pub fn heal_grid(
    grid: &Grid,
    liveness: &BTreeMap<NodeId, Liveness>,
    candidates: &[NodeId],
    zones: &BTreeMap<NodeId, FailureDomain>,
) -> Option<Grid> {
    let members: BTreeSet<NodeId> = grid.columns().iter().flatten().copied().collect();
    let usable: Vec<NodeId> = usable(candidates, liveness, &members);
    let mut columns: Vec<Vec<NodeId>> = grid.columns().to_vec();
    let holes: Vec<(usize, usize)> = columns
        .iter()
        .enumerate()
        .flat_map(|(c, column)| {
            column
                .iter()
                .enumerate()
                .filter(|(_, m)| liveness.get(m) == Some(&Liveness::Down))
                .map(move |(r, _)| (c, r))
        })
        .collect();
    let mut taken = vec![false; usable.len()];
    let found = fill(grid.f(), &mut columns, &holes, &usable, &mut taken, zones)?;
    assert!(
        found
            .columns()
            .iter()
            .flatten()
            .all(|m| liveness.get(m) != Some(&Liveness::Down)),
        "a heal leaves no member that is down"
    );
    assert!(
        grid_zones(&found, zones).admits(),
        "a heal meets the zone rule"
    );
    Some(found)
}

/// The candidates that may join a group of `members`: up, not in it, each
/// once, in the host's order.
fn usable(
    candidates: &[NodeId],
    liveness: &BTreeMap<NodeId, Liveness>,
    members: &BTreeSet<NodeId>,
) -> Vec<NodeId> {
    let mut seen = BTreeSet::new();
    candidates
        .iter()
        .copied()
        .filter(|c| liveness.get(c) == Some(&Liveness::Up))
        .filter(|c| !members.contains(c))
        .filter(|c| seen.insert(*c))
        .collect()
}

fn fill(
    f: u8,
    columns: &mut Vec<Vec<NodeId>>,
    holes: &[(usize, usize)],
    usable: &[NodeId],
    taken: &mut [bool],
    zones: &BTreeMap<NodeId, FailureDomain>,
) -> Option<Grid> {
    let Some(((c, r), rest)) = holes.split_first() else {
        let grid = Grid::new(f, columns.clone()).expect("a heal keeps the shape of the grid");
        return grid_zones(&grid, zones).admits().then_some(grid);
    };
    let gone = columns[*c][*r];
    for (i, candidate) in usable.iter().enumerate() {
        if taken[i] {
            continue;
        }
        taken[i] = true;
        columns[*c][*r] = *candidate;
        if let Some(grid) = fill(f, columns, rest, usable, taken, zones) {
            return Some(grid);
        }
        taken[i] = false;
    }
    columns[*c][*r] = gone;
    None
}

/// The new replica set of a heal: each member of `replicas` that is down
/// replaced by a candidate that is up and not in the set, such that the new
/// set admits the zone rule of one column ([`set_zones`]). The search tries
/// the candidates in the host's order and backtracks, as [`heal_grid`].
/// A set with no member down is kept as it is: the zone rule judges a
/// placement, so a grid heal that carries the set over never waits on it.
/// `None`: no placement exists.
///
/// # Panics
///
/// When an assertion on its own postconditions fails: a programmer error,
/// never an operating condition.
#[must_use]
pub fn heal_replicas(
    replicas: &[NodeId],
    liveness: &BTreeMap<NodeId, Liveness>,
    candidates: &[NodeId],
    zones: &BTreeMap<NodeId, FailureDomain>,
) -> Option<Vec<NodeId>> {
    let members: BTreeSet<NodeId> = replicas.iter().copied().collect();
    let usable = usable(candidates, liveness, &members);
    let holes: Vec<usize> = (0..replicas.len())
        .filter(|i| liveness.get(&replicas[*i]) == Some(&Liveness::Down))
        .collect();
    if holes.is_empty() {
        return Some(replicas.to_vec());
    }
    let mut set = replicas.to_vec();
    let mut taken = vec![false; usable.len()];
    let healed = fill_set(&mut set, &holes, &usable, &mut taken, zones)?;
    assert!(
        healed.len() == replicas.len(),
        "a heal keeps the set's size"
    );
    assert!(
        healed.iter().collect::<BTreeSet<_>>().len() == healed.len(),
        "a healed set names each replica once"
    );
    assert!(
        set_zones(&healed, zones).admits(),
        "a heal of a replica set meets the zone rule"
    );
    Some(healed)
}

/// The next step of the heal of a retired segment's replica set: one
/// member of `replicas` that is down replaced by a candidate that is up and
/// not in the set, with every other member kept, such that the new set
/// admits the zone rule of one column ([`set_zones`]). The down members are
/// tried in the set's order. A re-replication writes one replacement at a
/// time (§J3.1), so each set that it writes meets the zone rule on its own:
/// a joint plan of [`heal_replicas`] whose pairs run one by one can leave a
/// set that breaks the rule and that no later replacement can fix (#1496).
/// `None`: no member is down, or no single replacement exists.
///
/// # Panics
///
/// When an assertion on its own postconditions fails: a programmer error,
/// never an operating condition.
#[must_use]
pub fn heal_one_replica(
    replicas: &[NodeId],
    liveness: &BTreeMap<NodeId, Liveness>,
    candidates: &[NodeId],
    zones: &BTreeMap<NodeId, FailureDomain>,
) -> Option<Vec<NodeId>> {
    let members: BTreeSet<NodeId> = replicas.iter().copied().collect();
    let usable = usable(candidates, liveness, &members);
    let healed = (0..replicas.len())
        .filter(|i| liveness.get(&replicas[*i]) == Some(&Liveness::Down))
        .find_map(|hole| {
            let mut set = replicas.to_vec();
            let mut taken = vec![false; usable.len()];
            fill_set(&mut set, &[hole], &usable, &mut taken, zones)
        })?;
    assert!(
        healed.len() == replicas.len(),
        "a heal keeps the set's size"
    );
    assert!(
        healed
            .iter()
            .zip(replicas)
            .filter(|(new, old)| new != old)
            .count()
            == 1,
        "a heal of a retired set replaces exactly one member"
    );
    assert!(
        set_zones(&healed, zones).admits(),
        "a heal of a replica set meets the zone rule"
    );
    Some(healed)
}

fn fill_set(
    set: &mut Vec<NodeId>,
    holes: &[usize],
    usable: &[NodeId],
    taken: &mut [bool],
    zones: &BTreeMap<NodeId, FailureDomain>,
) -> Option<Vec<NodeId>> {
    let Some((hole, rest)) = holes.split_first() else {
        return set_zones(set, zones).admits().then(|| set.clone());
    };
    let gone = set[*hole];
    for (i, candidate) in usable.iter().enumerate() {
        if taken[i] {
            continue;
        }
        taken[i] = true;
        set[*hole] = *candidate;
        if let Some(found) = fill_set(set, rest, usable, taken, zones) {
            return Some(found);
        }
        taken[i] = false;
    }
    set[*hole] = gone;
    None
}

#[cfg(test)]
mod tests;
