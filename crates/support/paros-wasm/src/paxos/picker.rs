//! The quorum picker of the quorums page (#948 (quorum shapes and picker)).
//!
//! A reader stops some acceptors of a shape, and the page says whether a
//! Phase-1 quorum and a Phase-2 quorum still exist among the others. Every
//! answer is a question to `paros-core`'s own [`AcceptorConfig`]: the page
//! counts nothing.

use std::collections::BTreeSet;

use paros_core::NodeId;
use paros_core::membership::{AcceptorConfig, QuorumSystem};
use serde::{Deserialize, Serialize};

/// A shape the picker offers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// Five acceptors, a majority in both phases.
    Majority,
    /// Five acceptors: 4 answer Phase 1, 2 vote in Phase 2.
    Flexible,
    /// Six acceptors in 2 rows of 3: one acceptor of every column answers
    /// Phase 1, and a full column votes in Phase 2.
    Grid,
}

impl Shape {
    /// The shape named `name`: `majority`, `flexible` or `grid`.
    #[must_use]
    pub fn named(name: &str) -> Option<Self> {
        match name {
            "majority" => Some(Self::Majority),
            "flexible" => Some(Self::Flexible),
            "grid" => Some(Self::Grid),
            _ => None,
        }
    }

    fn config(self) -> AcceptorConfig {
        let (count, system) = match self {
            Self::Majority => (5, QuorumSystem::Majority),
            Self::Flexible => (5, QuorumSystem::Flexible { q1: 4, q2: 2 }),
            Self::Grid => (6, QuorumSystem::grid(2, 3)),
        };
        AcceptorConfig::new((0..count).map(NodeId).collect(), system)
    }
}

/// What the core says about the acceptors that still run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Answer {
    /// The acceptors of the shape.
    pub members: Vec<u64>,
    /// The grid's rows and columns, when the shape is a grid.
    pub grid: Option<(usize, usize)>,
    /// Whether the running acceptors hold a Phase-1 quorum: an election and
    /// a quorum read can finish.
    pub phase1: bool,
    /// Whether they hold a Phase-2 quorum: a write can be chosen.
    pub phase2: bool,
    /// On a grid, the columns that are complete (each one a Phase-2 quorum
    /// for its slots).
    pub columns: Vec<bool>,
    /// How many acceptors Phase 1 and Phase 2 need, for a shape that counts.
    pub sizes: Option<(usize, usize)>,
}

/// Ask the core about `shape` with the acceptors in `down` stopped.
#[must_use]
pub fn check(shape: Shape, down: &[u64]) -> Answer {
    let config = shape.config();
    let members: Vec<u64> = config.members().iter().map(|id| id.0).collect();
    let alive: BTreeSet<NodeId> = config
        .members()
        .iter()
        .copied()
        .filter(|id| !down.contains(&id.0))
        .collect();
    let system = config.quorum_system();
    let (grid, columns, sizes) = match system {
        QuorumSystem::Grid { rows, cols, .. } => (
            Some((rows, cols)),
            (0..cols)
                .map(|column| config.has_phase2_quorum_in(&alive, Some(column)))
                .collect(),
            None,
        ),
        _ => (
            None,
            Vec::new(),
            Some((
                system.phase1_quorum_size(members.len()),
                system.phase2_quorum_size(members.len()),
            )),
        ),
    };
    let phase2 = if columns.is_empty() {
        config.has_phase2_quorum(&alive)
    } else {
        // A grid writes a slot to one column: a write can be chosen while
        // one column is complete, but only in that column's slots.
        columns.iter().any(|complete| *complete)
    };
    Answer {
        phase1: config.has_phase1_quorum(&alive),
        phase2,
        members,
        grid,
        columns,
        sizes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_majority_of_five_survives_two_stops() {
        let answer = check(Shape::Majority, &[0, 1]);
        assert!(answer.phase1 && answer.phase2);
        assert_eq!(answer.sizes, Some((3, 3)));
        let answer = check(Shape::Majority, &[0, 1, 2]);
        assert!(!answer.phase1 && !answer.phase2);
    }

    #[test]
    fn a_flexible_split_writes_with_two_and_elects_with_four() {
        let answer = check(Shape::Flexible, &[0, 1]);
        assert!(!answer.phase1, "4 of 5 for Phase 1");
        assert!(answer.phase2, "2 of 5 for Phase 2");
        assert_eq!(answer.sizes, Some((4, 2)));
    }

    #[test]
    fn a_grid_elects_with_one_acceptor_per_column_and_writes_with_a_full_column() {
        // Columns {0,3}, {1,4}, {2,5}.
        let answer = check(Shape::Grid, &[3]);
        assert_eq!(answer.columns, [false, true, true]);
        assert!(answer.phase1 && answer.phase2);
        // No full row is left, but every column keeps one acceptor.
        let answer = check(Shape::Grid, &[0, 4]);
        assert!(answer.phase1, "one acceptor of every column");
        assert_eq!(answer.columns, [false, false, true]);
        assert!(answer.phase2, "column {{2, 5}} is full");
        // A whole column down: no Phase-1 quorum.
        let answer = check(Shape::Grid, &[1, 4]);
        assert!(!answer.phase1);
        assert_eq!(answer.columns, [true, false, true]);
    }

    #[test]
    fn a_shape_has_a_name() {
        assert_eq!(Shape::named("grid"), Some(Shape::Grid));
        assert_eq!(Shape::named("ring"), None);
    }
}
