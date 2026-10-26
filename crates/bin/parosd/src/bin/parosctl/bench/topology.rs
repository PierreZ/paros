//! The bench's journal topologies: `--topology 3x1,3x2,3x3` compares
//! the plain majority with acceptor grids under the same load. A topology
//! `RxC` is `C` acceptor partitions of `R` acceptors each: `R = 2F+1`, so
//! it is the durability `F` per partition over `C` partitions
//! (`paros::model::tenant::Durability`). `3x1` is the plain majority of three;
//! `3x3` is nine acceptors, each slot decided by two of one column.

use std::fmt;
use std::str::FromStr;

use paros::model::tenant::{Desired, Durability, MAX_FAILURES_PER_PARTITION};

/// One journal topology: `rows` acceptors in each of `columns` partitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Topology {
    durability: Durability,
}

impl Topology {
    /// The acceptors of one partition: `2F+1`.
    pub fn rows(self) -> usize {
        self.durability.acceptors_per_partition()
    }

    /// The acceptor partitions.
    pub fn columns(self) -> u32 {
        self.durability.partitions
    }

    /// The acceptors a journal of this topology asks for: `R × C`.
    pub fn acceptors(self) -> usize {
        self.durability.acceptors()
    }

    /// The mode a journal of this topology is created in.
    pub fn desired(self) -> Desired {
        Desired {
            durability: self.durability,
            replicas: 0,
        }
    }

    /// Its durability's label: `1 per partition · 3 partitions (…) · 9 acceptors`.
    pub fn label(self) -> String {
        self.durability.label()
    }
}

impl fmt::Display for Topology {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}x{}", self.rows(), self.columns())
    }
}

impl FromStr for Topology {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let shape = || {
            format!(
                "{text:?}: a topology is ROWSxCOLUMNS, ROWS odd from 1 to {}, COLUMNS at least 1 (3x1, 3x3)",
                2 * MAX_FAILURES_PER_PARTITION + 1
            )
        };
        let (rows, columns) = text.trim().split_once(['x', 'X']).ok_or_else(shape)?;
        let rows: u32 = rows.parse().map_err(|_| shape())?;
        let columns: u32 = columns.parse().map_err(|_| shape())?;
        if rows.is_multiple_of(2) {
            return Err(shape());
        }
        let durability = Durability::new(rows / 2, columns).map_err(|_| shape())?;
        let topology = Topology { durability };
        assert_eq!(topology.rows(), rows as usize, "the rows round-trip");
        assert_eq!(topology.columns(), columns, "the columns round-trip");
        Ok(topology)
    }
}

/// The topologies of one run, in the order given, each once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Topologies(Vec<Topology>);

impl Topologies {
    /// Every topology, in the order given.
    pub fn all(&self) -> &[Topology] {
        &self.0
    }

    /// The most acceptors one of them asks for.
    pub fn widest(&self) -> usize {
        self.0.iter().map(|t| t.acceptors()).max().unwrap_or(0)
    }
}

impl FromStr for Topologies {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut out: Vec<Topology> = Vec::new();
        for part in text.split(',') {
            let topology: Topology = part.parse()?;
            if out.contains(&topology) {
                return Err(format!("{text:?}: topology {topology} is named twice"));
            }
            out.push(topology);
        }
        assert!(!out.is_empty(), "split always yields one part");
        Ok(Topologies(out))
    }
}

#[cfg(test)]
mod tests {
    use super::{Topologies, Topology};

    #[test]
    fn a_topology_parses_and_prints() {
        let majority: Topology = "3x1".parse().expect("3x1");
        assert_eq!(majority.acceptors(), 3);
        assert_eq!(majority.to_string(), "3x1");
        assert_eq!(majority.desired().durability.failures_per_partition, 1);
        let grid: Topology = "5X3".parse().expect("5x3");
        assert_eq!(grid.acceptors(), 15);
        assert_eq!(grid.to_string(), "5x3");
        assert_eq!("1x1".parse::<Topology>().expect("1x1").acceptors(), 1);
    }

    #[test]
    fn a_bad_topology_is_refused() {
        for bad in ["", "3", "4x1", "3x0", "9x1", "x3", "3x", "3x1x1", "-3x1"] {
            assert!(bad.parse::<Topology>().is_err(), "{bad:?} is refused");
        }
    }

    #[test]
    fn a_list_keeps_its_order_and_refuses_a_repeat() {
        let list: Topologies = "3x3,3x1,5x1".parse().expect("a list");
        let names: Vec<String> = list.all().iter().map(ToString::to_string).collect();
        assert_eq!(names, ["3x3", "3x1", "5x1"]);
        assert_eq!(list.widest(), 9);
        assert!("3x1,3x1".parse::<Topologies>().is_err());
    }
}
