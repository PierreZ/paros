//! Goal 2 of `docs/architecture/01-goal.md` for the journal controller
//! (#1564 (journal controller, version 1)): after the faults stop, the
//! journal is whole again within one bound, or it reports a true reason.
//!
//! The judge takes the decision of the controller as the thing it judges,
//! and the facts of the cluster as the driver counted them: which members
//! of the configuration in force are down, and whether a node could replace
//! one. It never calls the rules of `paros_consensus::controller`.

use paros_consensus::NodeId;
use paros_consensus::controller::{Decision, HoldReason, Order};

/// Oracle 10: the journal heals, or its controller holds for a true reason.
pub const HEALED: &str = "the journal is whole again, or its controller holds for a true reason";

/// Oracle 11: once the faults stop, the fresh controller replaces no member
/// that runs. A member is replaced only on two witnesses (the leader missed
/// it, and it sent no fresh feedback), so a controller that acts on a view
/// of the cluster as it is never removes a member that runs again: a member
/// that the leader missed while it was down in the chaos sends a fresh
/// feedback once it runs.
pub const NO_FALSE_SILENCE: &str =
    "once the faults stop, a controller replaces no member that runs";

/// The facts of the cluster that goal 2 reads, counted by the driver.
#[derive(Debug)]
pub struct Facts<'a> {
    /// The members of the configuration in force.
    pub members: &'a [NodeId],
    /// The members of it that do not run.
    pub down: &'a [NodeId],
    /// Whether a node of the pool runs outside the configuration, is not
    /// retired, was never a member and is offered as a spare.
    pub spare_runs: bool,
    /// The nodes that run outside the configuration in force, that a
    /// configuration named or that leave the cell, and that did not retire
    /// although the leader made a floor effective.
    pub lingering: &'a [NodeId],
}

/// Judge the controller's `decision` once the faults stopped.
///
/// # Errors
///
/// What is still missing: the walk settles again, and fails the seed when
/// it is still missing after the last settle round.
pub fn judge(decision: &Decision, facts: &Facts<'_>) -> Result<(), String> {
    if !facts.lingering.is_empty() {
        return Err(format!(
            "{HEALED}: nodes {:?} run outside {:?} past an effective floor and did not retire",
            facts.lingering, facts.members
        ));
    }
    match decision {
        Decision::Steady if facts.down.is_empty() => Ok(()),
        Decision::Hold(HoldReason::NoSpare { node })
            if facts.down.contains(node) && !facts.spare_runs =>
        {
            Ok(())
        }
        _ => Err(format!(
            "{HEALED}: members {:?} of {:?} are down, a spare runs: {}, and the controller decides {decision:?}",
            facts.down, facts.members, facts.spare_runs
        )),
    }
}

/// Judge an order of the fresh controller once the faults stopped: a
/// reconfiguration removes only members that do not run.
///
/// # Panics
///
/// With [`NO_FALSE_SILENCE`], when the order removes a member that runs.
pub fn judge_order(order: &Order, members: &[NodeId], runs: impl Fn(NodeId) -> bool) {
    if let Order::Reconfigure { config, .. } = order {
        for &member in members {
            assert!(
                config.contains(member) || !runs(member),
                "{NO_FALSE_SILENCE}: node {} runs, and the order {order:?} removes it",
                member.0
            );
        }
    }
}
