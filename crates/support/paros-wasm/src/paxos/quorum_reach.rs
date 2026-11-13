//! The quorum picker of the site's grid-quorums page (#951 (live
//! Engineering pages)): the reader stops some acceptors of a journal's
//! partitions, and `paros-core`'s own quorum boundary
//! ([`AcceptorConfig::has_phase1_quorum`],
//! [`AcceptorConfig::has_phase2_quorum_in`]) says whether an election and a
//! write in each partition can still finish. The page computes nothing.

use std::collections::BTreeSet;

use paros_core::NodeId;
use paros_core::membership::{AcceptorConfig, QuorumSystem};
use serde::{Deserialize, Serialize};

/// The largest `F` the picker takes, as a journal's durability does.
pub const MAX_F: usize = 3;

/// The most partitions the picker takes.
pub const MAX_PARTITIONS: usize = 5;

/// What the reader asks: the shape and the acceptors that are up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReachRequest {
    /// Failures each partition survives: `2F+1` acceptors per partition.
    pub f: usize,
    /// How many partitions.
    pub partitions: usize,
    /// The acceptors that are down, by index in the membership.
    #[serde(default)]
    pub down: Vec<u64>,
}

/// What `paros-core` answers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReachView {
    /// Rows of the grid: `2F+1`.
    pub rows: usize,
    /// Columns of the grid: one per partition.
    pub cols: usize,
    /// For each acceptor, by index: its row and its column.
    pub cells: Vec<(usize, usize)>,
    /// Acceptors of one partition that choose a slot (`F+1`).
    pub write_quorum: usize,
    /// Acceptors of every partition that an election needs.
    pub election_per_partition: usize,
    /// Whether the acceptors that are up hold a Phase-1 quorum: an election
    /// and a tail read can finish.
    pub phase1: bool,
    /// For each partition: whether the acceptors that are up hold its
    /// Phase-2 quorum, so the slots of that partition can be chosen.
    pub phase2: Vec<bool>,
}

/// Answer `request`.
///
/// # Errors
///
/// `F` outside 0 to [`MAX_F`], partitions outside 1 to [`MAX_PARTITIONS`],
/// or a down acceptor that is not in the membership.
pub fn reach(request: &ReachRequest) -> Result<ReachView, String> {
    if request.f > MAX_F {
        return Err(format!("F is 0 to {MAX_F}"));
    }
    if request.partitions == 0 || request.partitions > MAX_PARTITIONS {
        return Err(format!("a journal has 1 to {MAX_PARTITIONS} partitions"));
    }
    let system = QuorumSystem::partitions(request.f, request.partitions);
    let QuorumSystem::Grid { rows, cols, acks } = system else {
        unreachable!("partitions build a grid");
    };
    let members: Vec<NodeId> = (0..(rows * cols) as u64).map(NodeId).collect();
    if let Some(bad) = request.down.iter().find(|d| **d >= members.len() as u64) {
        return Err(format!("no acceptor {bad}"));
    }
    let config = AcceptorConfig::new(members.clone(), system);
    let up: BTreeSet<NodeId> = members
        .iter()
        .copied()
        .filter(|m| !request.down.contains(&m.0))
        .collect();
    Ok(ReachView {
        rows,
        cols,
        cells: (0..rows * cols).map(|i| (i / cols, i % cols)).collect(),
        write_quorum: acks,
        election_per_partition: system.phase1_per_column().unwrap_or(rows),
        phase1: config.has_phase1_quorum(&up),
        phase2: (0..cols)
            .map(|c| config.has_phase2_quorum_in(&up, Some(c)))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(f: usize, partitions: usize, down: &[u64]) -> ReachView {
        reach(&ReachRequest {
            f,
            partitions,
            down: down.to_vec(),
        })
        .expect("a valid shape")
    }

    #[test]
    fn every_quorum_holds_when_all_are_up() {
        let view = ask(1, 3, &[]);
        assert_eq!((view.rows, view.cols), (3, 3));
        assert_eq!(view.write_quorum, 2);
        assert_eq!(view.election_per_partition, 2);
        assert!(view.phase1);
        assert_eq!(view.phase2, vec![true; 3]);
    }

    #[test]
    fn two_down_in_one_partition_stop_its_writes_and_every_election() {
        // Column 0 holds acceptors 0, 3 and 6 (row-major).
        let view = ask(1, 3, &[0, 3]);
        assert!(
            !view.phase1,
            "an election needs a majority of every partition"
        );
        assert_eq!(view.phase2, vec![false, true, true]);
    }

    #[test]
    fn one_down_in_each_partition_stops_nothing() {
        let view = ask(1, 3, &[0, 4, 8]);
        assert!(view.phase1);
        assert_eq!(view.phase2, vec![true; 3]);
    }

    #[test]
    fn a_shape_out_of_its_bounds_is_refused() {
        let refuse = |f, p, down: &[u64]| {
            reach(&ReachRequest {
                f,
                partitions: p,
                down: down.to_vec(),
            })
            .unwrap_err()
        };
        assert_eq!(refuse(4, 1, &[]), "F is 0 to 3");
        assert_eq!(refuse(1, 0, &[]), "a journal has 1 to 5 partitions");
        assert_eq!(refuse(1, 1, &[3]), "no acceptor 3");
    }
}
