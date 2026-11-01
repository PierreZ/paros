//! The balance view (#687 (rebalance a cell)): how the cell's balance
//! stands, judged live from the registry fold by the balancer's own pure
//! [`crate::balance::judge`], with names for people.

use paros_core::NodeId;
use paros_core::journal::JournalIdentifier;
use paros_wire::view as wire;

use crate::balance::{Bounds, Gap, Judgement, Repair, judge};
use crate::placement;
use crate::registry::balancing::{BalanceMove, BalanceOutcome};
use crate::registry::{NodeStanding, Registry};
use crate::tenant::TenantControl;

/// The cell's balance, as `registry` and the tenants' control journals
/// `tenants` (for the journals' names) stand, under a spread of `spread`
/// roles.
///
/// # Panics
///
/// If the answer shows a move while its state is another (a programmer
/// error).
#[must_use]
pub fn cell_balance(
    registry: &Registry,
    tenants: &[TenantControl],
    spread: u32,
) -> wire::CellBalance {
    let spread = spread.max(1);
    let bounds = Bounds {
        spread,
        rounds: 1,
        // A view has no busyness: every machine reads idle.
        busy_spread: placement::BUSY_STEPS,
        leader_spread: 1,
    };
    let machines = placement::machines(registry, registry.pool());
    let stays = |node: NodeId| {
        registry
            .get(node)
            .is_some_and(|n| n.standing == NodeStanding::Registered)
            && !registry.is_slow(node)
    };
    let mut reply = wire::CellBalance {
        paused: registry.balance().paused(),
        spread,
        ended: registry
            .balance()
            .ended()
            .map(|e| move_view(registry, tenants, &e.moved, Some((e.outcome, e.at))))
            .collect(),
        ..wire::CellBalance::default()
    };
    let mut shown: Vec<wire::BalanceMachine> = machines
        .iter()
        .filter(|m| m.class.stores() && m.up && stays(m.node))
        .map(|m| wire::BalanceMachine {
            node_id: m.node.0,
            name: name(registry, m.node),
            zone: m.zone.clone(),
            roles: m.booked,
        })
        .collect();
    shown.sort_by(|a, b| (&a.zone, &a.name, a.node_id).cmp(&(&b.zone, &b.name, b.node_id)));
    reply.machines = shown;
    reply.state = match judge(registry, &machines, stays, bounds) {
        Judgement::Moving(moved) => {
            reply.moving = Some(move_view(registry, tenants, &moved, None));
            "moving"
        }
        Judgement::Paused => "paused",
        Judgement::Repairing(repair) => {
            let (why, node) = match repair {
                Repair::Draining(node) => ("draining", node),
                Repair::Unwell(node) => ("unwell", node),
                Repair::Stranded(node) => ("stranded", node),
            };
            reply.repair = why.to_string();
            reply.repair_machine = node.0;
            reply.repair_machine_name = name(registry, node);
            "repairing"
        }
        Judgement::Stuck(gap) => {
            reply.gap = Some(gap_view(registry, &gap));
            "stuck"
        }
        Judgement::Unbalanced(gap) => {
            reply.gap = Some(gap_view(registry, &gap));
            "unbalanced"
        }
        Judgement::Balanced => "balanced",
    }
    .to_string();
    assert_eq!(
        reply.moving.is_some(),
        reply.state == "moving",
        "a move shows only in flight"
    );
    reply
}

/// Machine `node`'s name, empty when the registry holds none.
fn name(registry: &Registry, node: NodeId) -> String {
    registry
        .get(node)
        .map_or_else(String::new, |n| n.name.clone())
}

/// A move, with its names, and how it ended.
fn move_view(
    registry: &Registry,
    tenants: &[TenantControl],
    moved: &BalanceMove,
    end: Option<(BalanceOutcome, u64)>,
) -> wire::BalanceMoveView {
    let (tenant_name, journal_name) = names(registry, tenants, moved.journal);
    wire::BalanceMoveView {
        tenant: moved.journal.tenant.0,
        journal: moved.journal.journal.0,
        tenant_name,
        journal_name,
        from: moved.from.0,
        from_name: name(registry, moved.from),
        to: moved.to.0,
        to_name: name(registry, moved.to),
        reason: moved.reason.as_str().to_string(),
        started: moved.started,
        ended: end.map_or(0, |(_, at)| at),
        outcome: match end {
            None => String::new(),
            Some((BalanceOutcome::Done, _)) => "done".to_string(),
            Some((BalanceOutcome::Abandoned, _)) => "abandoned".to_string(),
        },
    }
}

/// A gap, with its machines' names.
fn gap_view(registry: &Registry, gap: &Gap) -> wire::BalanceGap {
    wire::BalanceGap {
        zone: gap.zone.clone(),
        reason: gap.reason.as_str().to_string(),
        from: gap.from.0.0,
        from_name: name(registry, gap.from.0),
        from_roles: gap.from.1,
        to: gap.to.0.0,
        to_name: name(registry, gap.to.0),
        to_roles: gap.to.1,
    }
}

/// The tenant's and the journal's names, as the registry and the tenant's
/// control journal hold them; empty when they hold none.
fn names(
    registry: &Registry,
    tenants: &[TenantControl],
    journal: JournalIdentifier,
) -> (Vec<u8>, Vec<u8>) {
    let tenant = registry
        .hosted_tenant(journal.tenant)
        .map_or_else(Vec::new, |h| h.name.clone());
    let name = tenants
        .iter()
        .find(|c| c.tenant() == journal.tenant)
        .and_then(|c| c.get(journal.journal))
        .map_or_else(Vec::new, |j| j.name.clone());
    (tenant, name)
}
