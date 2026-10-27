//! The tenant coordinator's role counts (#604, `docs/architecture/03-4-tenant-modes.md`
//! §3.4): a tenant's `batchers=` and `frontends=` counts, recorded by a
//! `Configure` of its default in its control journal, become that many
//! `batcher` and `frontend` slots of the tenant (`BookingTarget::Tenant`)
//! on the cell's `stateless` machines.
//!
//! One step per watch round, after the footprint step, in turn over the
//! hosted tenants, and for the tenant in turn over its roles: release one
//! slot on a machine that leaves (draining, held slow, or gone,
//! `crate::tenant::moves::stays`), else book or release the slots that make
//! the count, with `paros_model::placement` (one slot per machine, zones in
//! round robin, least-loaded first). The count is capped at the `stateless`
//! machines that can take a slot, so a cell without one places none and a
//! cell that grows places the rest later. Every step starts from the
//! registry and the tenant control journal, so a later term finishes what
//! an earlier one started.
//!
//! A slot is placement only: every machine serves the batcher endpoint
//! (`paros_roles::batcher`) and, given a frontend role, the frontend
//! (`paros_roles::frontend`, #723 (frontend as a parosd role)); `Resolve`
//! names the machines the registry books a tenant's batchers and frontends
//! on, which is where its clients send their multi-writer writes and their
//! calls. The count of proxy leaders is recorded and not placed yet (#604).

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use paros_core::NodeId;
use paros_core::journal::TenantId;
use paros_net::runtime::ParosProviders;

use crate::tenant::TenantDesk;
use crate::tenant::moves::{Leaving, stays};
use paros_client::Client;
use paros_client::cell::CellSession;
use paros_client::journals::JournalAnswer;
use paros_model::machine::Class;
use paros_model::placement::{self, Step, Want};
use paros_model::registry::{Booking, BookingTarget, NodeStanding, Registry, Role, SystemCommand};

/// What one role step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RoleStep {
    /// No hosted tenant, or the tenant in turn holds its count.
    Idle,
    /// The tenant's control journal could not be read.
    Waiting,
    /// Slots were booked.
    Booked,
    /// A slot was released.
    Released,
}

/// The role steps of one coordinator term.
#[derive(Debug, Default)]
pub(crate) struct Roles {
    /// The tenant the last step took: the next step takes the next one.
    last: Option<TenantId>,
}

/// The roles a tenant's counts place, in the order a step takes them.
const PLACED: [Role; 2] = [Role::Batcher, Role::Frontend];

/// The tenant's slots of `role`.
fn slots(registry: &Registry, tenant: TenantId, role: Role) -> Vec<(u64, Booking)> {
    assert!(PLACED.contains(&role), "a count places a placed role");
    registry
        .bookings()
        .filter(|(_, b)| b.role == role && b.target == BookingTarget::Tenant { tenant })
        .map(|(id, b)| (id, *b))
        .collect()
}

/// The `stateless` machines a batcher or frontend slot may go to:
/// registered, serving the cell, and staying.
fn stateless(registry: &Registry, leaving: &Leaving) -> Vec<NodeId> {
    let nodes: Vec<NodeId> = registry
        .nodes()
        .filter(|(id, n)| {
            n.class == Class::Stateless
                && n.standing == NodeStanding::Registered
                && registry.serves(*id)
                && stays(registry, leaving, *id)
        })
        .map(|(id, _)| id)
        .collect();
    assert!(
        nodes.iter().all(|id| registry
            .get(*id)
            .is_some_and(|n| n.class == Class::Stateless)),
        "a counted role goes only to a stateless machine"
    );
    nodes
}

impl Roles {
    /// Take one step of the next hosted tenant's role counts, through the
    /// term's session `cell` (its bookings drawn by `desk`, which holds the
    /// tenant control journals). `None` when a write did not land: the term
    /// writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn step<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        leaving: &Leaving,
    ) -> Option<RoleStep> {
        let registry = cell.registry();
        let hosted: BTreeSet<TenantId> = registry
            .hosted()
            .filter(|tenant| !registry.dropped(*tenant))
            .collect();
        let Some(tenant) = self
            .last
            .and_then(|last| hosted.range((Excluded(last), Unbounded)).next())
            .or_else(|| hosted.first())
            .copied()
        else {
            return Some(RoleStep::Idle);
        };
        self.last = Some(tenant);
        let Some(control) = registry.hosted_tenant(tenant).map(|h| h.control) else {
            return Some(RoleStep::Waiting);
        };
        for role in PLACED {
            match role_step(client, cell, desk, leaving, (tenant, control), role).await? {
                RoleStep::Idle => {}
                step => return Some(step),
            }
        }
        Some(RoleStep::Idle)
    }
}

/// Release one of the tenant's `role` slots on a machine that leaves:
/// `Some(None)` when the write did not land, `None` when no slot leaves.
async fn release_leaving<P: ParosProviders>(
    client: &Client<P>,
    cell: &mut CellSession,
    leaving: &Leaving,
    tenant: TenantId,
    role: Role,
) -> Option<Option<RoleStep>> {
    let registry = cell.registry();
    let (booking, _) = slots(registry, tenant, role)
        .into_iter()
        .find(|(_, b)| !stays(registry, leaving, b.node))?;
    assert!(
        registry.booking(booking).is_some_and(|b| b.role == role),
        "a released slot is one of the role's"
    );
    let command = SystemCommand::ReleaseCapacity { booking };
    if cell.record(client, 0, &command).await.is_err() {
        return Some(None);
    }
    match role {
        Role::Frontend => {
            moonpool_assertions::reachable!("coordinator: a frontend leaves a machine that leaves");
        }
        _ => moonpool_assertions::reachable!("coordinator: a batcher leaves a machine that leaves"),
    }
    Some(Some(RoleStep::Released))
}

/// One step of the tenant's count of `role`: `Idle` when it holds its count.
#[tracing::instrument(level = "debug", skip_all, fields(tenant = tenant.0, role = role.as_str()))]
async fn role_step<P: ParosProviders>(
    client: &Client<P>,
    cell: &mut CellSession,
    desk: &mut TenantDesk,
    leaving: &Leaving,
    (tenant, control): (TenantId, paros_core::journal::JournalId),
    role: Role,
) -> Option<RoleStep> {
    // A slot on a machine that leaves goes first: the count is then made
    // on the machines that stay.
    if let Some(step) = release_leaving(client, cell, leaving, tenant, role).await {
        return step;
    }
    let held = slots(cell.registry(), tenant, role);
    let wanted = match desk.tenant(client, tenant, control).await {
        Ok(state) => {
            let roles = state.roles();
            match role {
                Role::Frontend => roles.frontends,
                _ => roles.batchers,
            }
        }
        Err(JournalAnswer::NotCoordinator) => return None,
        Err(_) => return Some(RoleStep::Waiting),
    };
    let registry = cell.registry();
    let nodes = stateless(registry, leaving);
    let machines = placement::machines(registry, nodes.iter().copied());
    let holding: BTreeSet<NodeId> = held.iter().map(|(_, b)| b.node).collect();
    // A machine with a slot of the tenant's role, or with room.
    let eligible = machines
        .iter()
        .filter(|m| holding.contains(&m.node) || m.free() > 0)
        .count();
    let count = usize::try_from(wanted).unwrap_or(usize::MAX).min(eligible);
    if count < usize::try_from(wanted).unwrap_or(usize::MAX) {
        match role {
            Role::Frontend => moonpool_assertions::reachable!(
                "coordinator: a frontend count is capped at the machines"
            ),
            _ => moonpool_assertions::reachable!(
                "coordinator: a batcher count is capped at the machines"
            ),
        }
    }
    let want = Want {
        role,
        target: BookingTarget::Tenant { tenant },
        count,
    };
    let current: Vec<(u64, Booking)> = registry
        .bookings()
        .filter(|(_, b)| b.target == want.target)
        .map(|(id, b)| (id, *b))
        .collect();
    let Ok(steps) = placement::plan(&want, &machines, &current, tenant.0) else {
        return Some(RoleStep::Waiting);
    };
    if steps.is_empty() {
        if count > 0 {
            match role {
                Role::Frontend => {
                    moonpool_assertions::reachable!("coordinator: a tenant holds its frontends");
                }
                _ => moonpool_assertions::reachable!("coordinator: a tenant holds its batchers"),
            }
        }
        return Some(RoleStep::Idle);
    }
    if let Some(Step::Release { booking, .. }) =
        steps.iter().find(|s| matches!(s, Step::Release { .. }))
    {
        let command = SystemCommand::ReleaseCapacity { booking: *booking };
        if cell.record(client, 0, &command).await.is_err() {
            return None;
        }
        match role {
            Role::Frontend => moonpool_assertions::reachable!(
                "coordinator: a frontend slot is released to its count"
            ),
            _ => moonpool_assertions::reachable!(
                "coordinator: a batcher slot is released to its count"
            ),
        }
        return Some(RoleStep::Released);
    }
    desk.book(client, cell, &want, &steps).await.ok()?;
    match role {
        Role::Frontend => {
            moonpool_assertions::reachable!("coordinator: a tenant's frontends are booked");
        }
        _ => moonpool_assertions::reachable!("coordinator: a tenant's batchers are booked"),
    }
    tracing::info!(
        tenant = tenant.0,
        role = role.as_str(),
        count,
        "tenant_role_booked"
    );
    Some(RoleStep::Booked)
}
