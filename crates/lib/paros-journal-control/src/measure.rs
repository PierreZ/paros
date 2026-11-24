//! The **protocol facts** of a grid (the P measures of
//! `docs/journal/06-controller.md` §J6.2): the spare of each column,
//! `electable`, `writable` and the journal state, as pure functions over a
//! v1 grid and the set of live acceptors.
//!
//! Every quorum question crosses `paros_consensus::membership`: a column of
//! a v1 grid is a majority of `2F + 1` (§J5.1), and each count here asks
//! [`AcceptorConfig::has_phase1_quorum`] or
//! [`AcceptorConfig::has_phase2_quorum`]. No count is compared with a
//! threshold of its own. On a v1 grid the Phase-1 count of a column
//! (`rows - acks + 1`) equals its Phase-2 count (`acks`), so the two spares
//! of a column are one number (`PartitionedOneSpare` of
//! `specs/tla/health/`, the `v1` check); [`column_spares`] computes both and
//! asserts it.

use std::collections::BTreeSet;

use paros_consensus::NodeId;
use paros_consensus::membership::AcceptorConfig;
use paros_journal::Grid;

/// The liveness of one member (the O measure `live` of §J6.2), as the host
/// judges it over its suspicion window (**Open**: the window is the host's
/// tunable, no number is set).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Liveness {
    /// The member answered inside the window.
    Up,
    /// The member missed the window, or flaps (`flap_rate`): it counts as
    /// lost for the spares, and the controller does not replace it.
    Suspect,
    /// The member is gone: it counts as lost, and a heal replaces it.
    Down,
}

/// The two spares of one column (§J6.2, "The spares of a column").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Spares {
    /// How many more members the column can lose and still choose a slot.
    pub phase2: i64,
    /// How many more members the column can lose and still give its part of
    /// a Phase-1 quorum.
    pub phase1: i64,
}

impl Spares {
    /// The lower of the two spares: the `spare` of the column.
    #[must_use]
    pub fn spare(self) -> i64 {
        self.phase1.min(self.phase2)
    }
}

/// The grid part of a journal's health on a v1 grid: one of the six states
/// of §J6.2 that a v1 grid can be in. `WritesBlocked` is not a v1 state
/// (`NoWritesBlocked` of `specs/tla/health/`): on a v1 grid an electable
/// grid can write in every column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GridState {
    /// Every column has a spare of at least 1.
    Healthy,
    /// A column has a spare of 0; electable and writable; a placement that
    /// meets the zone rule exists. The action: one heal reshape.
    AtRisk,
    /// As `AtRisk`, and no placement meets the zone rule. Hold and alert.
    AtRiskUnplaceable,
    /// Not electable; a column can write; the leader is live. v1: hold and
    /// alert.
    WritesDegraded,
    /// Not electable; a column can write; no live leader. Hold and page.
    ElectionBlocked,
    /// Not electable and no column can write. Hold and page.
    Stopped,
}

/// How far `holds` is from failing on `config`, asked through the
/// predicate alone: with the quorum on `live`, the number of live members
/// that can go (one at a time, in member order) before it fails; without
/// it, minus the number of lost members that must come back (in member
/// order) before it holds.
fn margin(
    config: &AcceptorConfig<NodeId>,
    live: &BTreeSet<NodeId>,
    holds: impl Fn(&AcceptorConfig<NodeId>, &BTreeSet<NodeId>) -> bool,
) -> i64 {
    let members = config.members();
    let mut voters: BTreeSet<NodeId> = members
        .iter()
        .copied()
        .filter(|m| live.contains(m))
        .collect();
    let mut steps: i64 = 0;
    if holds(config, &voters) {
        for member in members.iter().filter(|m| live.contains(m)) {
            voters.remove(member);
            if !holds(config, &voters) {
                return steps;
            }
            steps += 1;
        }
        unreachable!("an empty set holds no quorum of a configuration")
    }
    for member in members.iter().filter(|m| !live.contains(m)) {
        voters.insert(*member);
        steps -= 1;
        if holds(config, &voters) {
            return steps;
        }
    }
    unreachable!("the whole membership holds a quorum of its configuration")
}

/// The spares of each column of `grid` with the acceptors of `live`.
///
/// # Panics
///
/// When the two spares of a column differ: on a v1 grid they are one
/// number, so a difference is a programmer error.
#[must_use]
pub fn column_spares(grid: &Grid, live: &BTreeSet<NodeId>) -> Vec<Spares> {
    let spares: Vec<Spares> = grid
        .column_configs()
        .iter()
        .map(|column| Spares {
            phase2: margin(column, live, AcceptorConfig::has_phase2_quorum),
            phase1: margin(column, live, AcceptorConfig::has_phase1_quorum),
        })
        .collect();
    for s in &spares {
        assert!(
            s.phase1 == s.phase2,
            "the two spares of a column of a v1 grid are one number"
        );
        assert!(
            s.spare() <= i64::from(grid.f()),
            "a column spares at most F members"
        );
    }
    spares
}

/// `electable`: the live acceptors hold a Phase-1 quorum of every column.
#[must_use]
pub fn electable(grid: &Grid, live: &BTreeSet<NodeId>) -> bool {
    grid.column_configs()
        .iter()
        .all(|c| c.has_phase1_quorum(live))
}

/// `writable`: some column can choose a slot with the live acceptors.
#[must_use]
pub fn writable(grid: &Grid, live: &BTreeSet<NodeId>) -> bool {
    grid.column_configs()
        .iter()
        .any(|c| c.has_phase2_quorum(live))
}

/// The journal state of `grid` with the acceptors of `live`, a live leader
/// or not, and whether a placement that meets the zone rule exists.
///
/// # Panics
///
/// When the grid is electable and a column cannot write: not a v1 state
/// (`NoWritesBlocked`), so a programmer error.
#[must_use]
pub fn grid_state(
    grid: &Grid,
    live: &BTreeSet<NodeId>,
    leader_live: bool,
    placeable: bool,
) -> GridState {
    let spares = column_spares(grid, live);
    let electable = electable(grid, live);
    let writable = writable(grid, live);
    if electable {
        assert!(
            spares.iter().all(|s| s.phase2 >= 0),
            "an electable v1 grid can write in every column"
        );
        assert!(writable, "an electable v1 grid is writable");
    }
    let state = if spares.iter().all(|s| s.spare() >= 1) {
        GridState::Healthy
    } else if electable && placeable {
        GridState::AtRisk
    } else if electable {
        GridState::AtRiskUnplaceable
    } else if !writable {
        GridState::Stopped
    } else if leader_live {
        GridState::WritesDegraded
    } else {
        GridState::ElectionBlocked
    };
    assert!(
        (state == GridState::Healthy) == spares.iter().all(|s| s.spare() >= 1),
        "Healthy is a spare in every column"
    );
    state
}

#[cfg(test)]
mod tests;
