//! The plan of `cell init`, declared with moonpool's `shard!` (#760
//! (moonpool owns fault placement), #1179 (the kill policy as shards)):
//! the cell exists while a majority of the plan's members keeps its vote
//! and is reachable under the name the plan lists. Fewer, and no `cell init`
//! can choose the plan again, and the one member left cannot write a
//! renamed member's new name into the registry. The simulator keeps every
//! wipe and every hold-down inside this need; in production no sink is
//! installed and nothing here runs past one thread-local read.
//!
//! Each machine that holds a vote declares, when it votes and at each boot
//! that reads its vote. Each member names the others by the address the
//! plan lists, as a peer dials it.

use moonpool_buggify::shard;
use moonpool_buggify::shard::{Need, Replica, Replication};
use paros_model::machine::CellPlan;

/// The prefix of a plan's shard name: `cell/<cell_id>`.
pub const PREFIX: &str = "cell/";

/// The shard name of `plan`: `cell/<cell_id>`.
fn name(plan: &CellPlan) -> String {
    format!("{PREFIX}{}", plan.cell_id)
}

/// The replication of `plan`: its members by the addresses it lists, a
/// majority of them intact and up (the decree's Phase-2 quorum).
fn replication(plan: &CellPlan) -> Replication {
    let replicas: Vec<Replica> = plan
        .members
        .iter()
        .map(|(_, addr)| Replica::new(addr.as_str()))
        .collect();
    assert!(!replicas.is_empty(), "a plan lists its initial members");
    let need = Need::AtLeast(super::cell_init::majority(replicas.len()), replicas.clone());
    Replication {
        replicas,
        progress: need.clone(),
        durable: need,
    }
}

/// Declare `plan`: this machine holds a vote for it.
pub(crate) fn declare(plan: &CellPlan) {
    if !shard::is_active() {
        return;
    }
    shard!(name(plan), current = replication(plan));
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::NodeId;
    use paros_model::address::Address;

    #[test]
    fn a_plan_of_three_needs_two_of_its_members() {
        let addr = |s: &str| Address::parse(s).expect("an address");
        let plan = crate::rig::plan(
            7,
            vec![
                (NodeId(1), addr("machine-0.paros:4500")),
                (NodeId(2), addr("machine-1.paros:4500")),
                (NodeId(3), addr("10.0.6.3:4500")),
            ],
        );
        assert_eq!(name(&plan), "cell/7");
        let replication = replication(&plan);
        assert_eq!(replication.replicas.len(), 3);
        assert_eq!(
            replication.replicas[0],
            Replica::new("machine-0.paros:4500")
        );
        let up = |names: &[&str]| {
            replication
                .durable
                .holds(&|replica| names.contains(&replica.address()))
        };
        assert!(up(&["machine-0.paros:4500", "10.0.6.3:4500"]));
        assert!(!up(&["machine-1.paros:4500"]));
        assert_eq!(replication.progress, replication.durable);
    }
}
