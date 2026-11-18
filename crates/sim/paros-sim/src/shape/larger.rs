//! The **larger cell** (step 1.3 of the rework, #1092 (larger cell), #459
//! (narrow topology)): on a share of the seeds, the machine group is a cell
//! of 5 to 9 machines, drawn with the failures it promises to survive.
//!
//! A cell that survives `F` machines over `P` acceptor partitions needs
//! `2F+1` storage machines for each partition. A shape with spares has `F`
//! machines more, so each set can have every member live again after `F`
//! machines die for good. A shape with no spare cannot place a lost member,
//! and the cell healthy predicate judges its typed reason (`CannotPlace`,
//! `crate::tail::predicate`). The six shapes in 5 to 9 machines with an
//! `F` a durability allows (at most `MAX_SURVIVE_MACHINES`, 3) are
//! [`SHAPES`].
//!
//! One draw sets three surfaces, so the three always agree. The draw is a
//! moonpool seed plan ([`plan`], `SimulationBuilder::seed_plan`), made from
//! the simulation stream before the process groups resolve:
//!
//! - the machine group's count, the shape's [`LargerCell::machines`];
//! - the machines' attrition budget, `max_dead = F`;
//! - the outage of every machine (`crate::world::outage::machine_regime`),
//!   which strikes only on a larger cell: every other seed calms it.
//!
//! The plan publishes `F`, `P` and the spares in the run's `StateHandle`,
//! and [`larger_cell`] reads them back. On a larger cell the layout lists
//! `2F+1` initial members and every machine, all of them storage
//! (`crate::shape::machine_layout`), so `init` gives the cell a survive
//! number of `F`. The scenarios whose layout needs another shape give way
//! to it (batchers, drained machine, zone outage, admitted slot, second
//! cell), and most journal creates ask for `F` failures over `P`
//! partitions (`crate::workload::fleet::journals`).

use moonpool_sim::{SeedPlan, StateHandle};

use crate::roles::MACHINE_GROUP;

/// The share of the seeds that run a larger cell. A quarter is this step's
/// reading of "a share of the seeds" (docs/architecture 07, step 1.3).
/// **Open**: the gate of phase 1 decides the share; the heal scorecard
/// counts each cause over every seed and does not split it by shape yet. A
/// schedule parameter, never buggified.
pub(crate) const SHARE: f64 = 0.25;

/// The [`StateHandle`] key of the drawn survive number `F`.
const SURVIVE_KEY: &str = "paros.larger-cell.survive";
/// The [`StateHandle`] key of the drawn acceptor partitions `P`.
const PARTITIONS_KEY: &str = "paros.larger-cell.partitions";
/// The [`StateHandle`] key of the drawn spares: `1` with spares, `0`
/// without.
const SPARES_KEY: &str = "paros.larger-cell.spares";

/// One larger cell's shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LargerCell {
    /// `F`: the machines the cell promises to survive.
    pub(crate) survive: u32,
    /// `P`: the acceptor partitions of a journal.
    pub(crate) partitions: u32,
    /// Whether the cell has `F` machines more than its partitions need.
    pub(crate) spares: bool,
}

/// Every larger cell of 5 to 9 machines: `(2F+1)·P` machines, plus `F`
/// with spares. `(F, P)` = `(1, 1)` is 3 or 4 machines, which the
/// usual draw covers; `(2, 2)`, `(1, 3)` with spares and `(3, 1)` with
/// spares are 10.
pub(crate) const SHAPES: [LargerCell; 6] = [
    LargerCell {
        survive: 2,
        partitions: 1,
        spares: false,
    },
    LargerCell {
        survive: 1,
        partitions: 2,
        spares: false,
    },
    LargerCell {
        survive: 1,
        partitions: 2,
        spares: true,
    },
    LargerCell {
        survive: 2,
        partitions: 1,
        spares: true,
    },
    LargerCell {
        survive: 3,
        partitions: 1,
        spares: false,
    },
    LargerCell {
        survive: 1,
        partitions: 3,
        spares: false,
    },
];

/// The fewest and the most machines of a larger cell.
pub(crate) const MACHINES: std::ops::RangeInclusive<usize> = 5..=9;

impl LargerCell {
    /// The storage machines one acceptor partition needs: `2F+1`. They are
    /// also the cell's initial members, so `init` gives the cell a survive
    /// number of `F` (`crate::workload::fleet::cell`).
    pub(crate) fn initial_members(self) -> usize {
        2 * self.survive as usize + 1
    }

    /// The cell's machines: `2F+1` for each partition, plus `F` spares.
    pub(crate) fn machines(self) -> usize {
        let spares = if self.spares {
            self.survive as usize
        } else {
            0
        };
        self.initial_members() * self.partitions as usize + spares
    }
}

/// The seed plan of the main campaign (`SimulationBuilder::seed_plan`): on
/// [`SHARE`] of the seeds, a larger cell from [`SHAPES`]; on the others, no
/// change but a calm machine group, so the outage of every machine does not
/// strike.
pub(crate) fn plan(plan: &mut SeedPlan) {
    if !moonpool_sim::sim_random_bool(SHARE) {
        plan.calm(MACHINE_GROUP);
        return;
    }
    let shape = SHAPES[crate::draw_index(moonpool_sim::sim_random::<u64>(), SHAPES.len())];
    moonpool_sim::assert_always!(
        MACHINES.contains(&shape.machines()),
        "cell: a larger cell has 5 to 9 machines",
        { "machines" => shape.machines() }
    );
    plan.set_count(MACHINE_GROUP, shape.machines());
    plan.set_max_dead(MACHINE_GROUP, shape.survive as usize);
    plan.publish(SURVIVE_KEY, u64::from(shape.survive));
    plan.publish(PARTITIONS_KEY, u64::from(shape.partitions));
    plan.publish(SPARES_KEY, u64::from(shape.spares));
}

/// The run's larger cell, if its seed plan drew one ([`plan`]). It reads
/// the values the plan published and draws nothing.
pub(crate) fn larger_cell(state: &StateHandle) -> Option<LargerCell> {
    let survive = state.get::<u64>(SURVIVE_KEY)?;
    let partitions = state.get::<u64>(PARTITIONS_KEY)?;
    let spares = state.get::<u64>(SPARES_KEY)?;
    let shape = LargerCell {
        survive: u32::try_from(survive).ok()?,
        partitions: u32::try_from(partitions).ok()?,
        spares: spares == 1,
    };
    SHAPES.contains(&shape).then_some(shape)
}

/// Judge a larger cell's machine layout: `count` machines as the shape
/// draws, none kept out of `init` (`reserve`), and every one `listed`.
/// Then reach the shape's gate.
pub(crate) fn judge_layout(shape: LargerCell, count: usize, reserve: bool, listed: usize) {
    moonpool_sim::assert_always!(
        count == shape.machines() && !reserve && listed == count,
        "machine: a larger cell's layout lists its drawn machines",
        { "count" => count, "machines" => shape.machines(), "listed" => listed }
    );
    reach(shape);
}

/// Reach one gate for the shape of a larger cell: each shape has its own
/// message, so the sweep shows that every shape ran.
fn reach(shape: LargerCell) {
    match (shape.survive, shape.partitions, shape.spares) {
        (2, 1, false) => {
            moonpool_sim::assert_reachable!("cell: a larger cell survives 2 in 5 machines");
        }
        (1, 2, false) => {
            moonpool_sim::assert_reachable!("cell: a larger cell has 2 partitions in 6 machines");
        }
        (1, 2, true) => {
            moonpool_sim::assert_reachable!(
                "cell: a larger cell has 2 partitions and a spare in 7 machines"
            );
        }
        (2, 1, true) => {
            moonpool_sim::assert_reachable!("cell: a larger cell survives 2 with 2 spares");
        }
        (3, 1, _) => {
            moonpool_sim::assert_reachable!("cell: a larger cell survives 3 in 7 machines");
        }
        _ => {
            moonpool_sim::assert_reachable!("cell: a larger cell has 3 partitions in 9 machines");
        }
    }
}

#[cfg(test)]
mod tests {
    use paros::model::tenant::Durability;

    use super::{LargerCell, MACHINES, SHAPES};

    #[test]
    fn every_shape_is_a_larger_cell_of_its_own_size() {
        let mut sizes: Vec<(usize, u32)> = SHAPES
            .iter()
            .map(|shape| (shape.machines(), shape.survive))
            .collect();
        for shape in SHAPES {
            assert!(MACHINES.contains(&shape.machines()), "{shape:?}");
            assert_eq!(shape.initial_members() % 2, 1, "{shape:?}");
            assert!(shape.initial_members() * shape.partitions as usize <= shape.machines());
            assert!(Durability::new(shape.survive, shape.partitions).is_ok());
        }
        sizes.sort_unstable();
        sizes.dedup();
        // The machine count and F name one shape each.
        assert_eq!(sizes.len(), SHAPES.len());
    }

    #[test]
    fn spares_are_f_machines_past_the_partitions() {
        let shape = LargerCell {
            survive: 2,
            partitions: 1,
            spares: true,
        };
        assert_eq!(shape.initial_members(), 5);
        assert_eq!(shape.machines(), 7);
        let shape = LargerCell {
            survive: 1,
            partitions: 3,
            spares: false,
        };
        assert_eq!(shape.initial_members(), 3);
        assert_eq!(shape.machines(), 9);
    }

    #[test]
    fn every_shape_of_five_to_nine_machines_is_listed() {
        for survive in 1_u32..=paros::model::tenant::MAX_SURVIVE_MACHINES {
            for partitions in 1_u32..=4 {
                for spares in [false, true] {
                    let shape = LargerCell {
                        survive,
                        partitions,
                        spares,
                    };
                    assert_eq!(
                        SHAPES.contains(&shape),
                        MACHINES.contains(&shape.machines()),
                        "{shape:?}"
                    );
                }
            }
        }
    }
}
