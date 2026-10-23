//! The tenant coordinator's role counts (#604, `docs/architecture/03-4-tenant-modes.md`
//! §3.4): a tenant's `batchers=` count, recorded by a `Configure` of its
//! default in its control journal, becomes that many `batcher` slots of the
//! tenant (`BookingTarget::Tenant`) on the cell's `stateless` machines.
//!
//! One step per watch round, after the footprint step, in turn over the
//! hosted tenants: release one batcher slot on a machine that leaves
//! (draining, held slow, or gone, `super::moves::stays`), else book or
//! release the slots that make the count, with `crate::placement` (one
//! slot per machine, zones in round robin, least-loaded first). The count
//! is capped at the `stateless` machines that can take a slot, so a cell
//! without one places none and a cell that grows places the rest later.
//! Every step starts from the registry and the tenant control journal, so a
//! later term finishes what an earlier one started.
//!
//! A batcher slot is placement only: every machine serves the batcher
//! endpoint (`crate::batcher`), and `Resolve` names the machines the
//! registry books a tenant's batchers on, which is where its clients send
//! their multi-writer writes. The counts of frontends and proxy leaders are
//! recorded and not placed yet (#604).

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use crate::ParosProviders;
use paros_core::{NodeId, TenantId};

use super::moves::{Leaving, stays};
use super::tenants::TenantDesk;
use crate::client::Client;
use crate::client::cell::CellSession;
use crate::client::journals::JournalAnswer;
use crate::machine::Class;
use crate::placement::{self, Step, Want};
use crate::system::{Booking, BookingTarget, NodeStanding, Registry, Role, SystemCommand};

/// What one role step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RoleStep {
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
pub(super) struct Roles {
    /// The tenant the last step took: the next step takes the next one.
    last: Option<TenantId>,
}

/// The tenant's batcher slots.
fn batchers(registry: &Registry, tenant: TenantId) -> Vec<(u64, Booking)> {
    registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Batcher && b.target == BookingTarget::Tenant { tenant })
        .map(|(id, b)| (id, *b))
        .collect()
}

/// The `stateless` machines a batcher slot may go to: registered, serving
/// the cell, and staying.
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
        "a batcher goes only to a stateless machine"
    );
    nodes
}

impl Roles {
    /// Take one step of the next hosted tenant's role counts, through the
    /// term's session `cell` (its bookings drawn by `desk`, which holds the
    /// tenant control journals). `None` when a write did not land: the term
    /// writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) async fn step<P: ParosProviders>(
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
        // A batcher on a machine that leaves goes first: the count is then
        // made on the machines that stay.
        let held = batchers(registry, tenant);
        if let Some((booking, _)) = held.iter().find(|(_, b)| !stays(registry, leaving, b.node)) {
            let command = SystemCommand::ReleaseCapacity { booking: *booking };
            if cell.record(client, 0, &command).await.is_err() {
                return None;
            }
            moonpool_assertions::reachable!("coordinator: a batcher leaves a machine that leaves");
            return Some(RoleStep::Released);
        }
        let wanted = match desk.tenant(client, tenant, control).await {
            Ok(state) => state.roles().batchers,
            Err(JournalAnswer::NotCoordinator) => return None,
            Err(_) => return Some(RoleStep::Waiting),
        };
        let registry = cell.registry();
        let nodes = stateless(registry, leaving);
        let machines = placement::machines(registry, nodes.iter().copied());
        let holding: BTreeSet<NodeId> = held.iter().map(|(_, b)| b.node).collect();
        // A machine with a slot of the tenant's batchers, or with room.
        let eligible = machines
            .iter()
            .filter(|m| holding.contains(&m.node) || m.free() > 0)
            .count();
        let count = usize::try_from(wanted).unwrap_or(usize::MAX).min(eligible);
        if count < usize::try_from(wanted).unwrap_or(usize::MAX) {
            moonpool_assertions::reachable!(
                "coordinator: a batcher count is capped at the machines"
            );
        }
        let want = Want {
            role: Role::Batcher,
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
                moonpool_assertions::reachable!("coordinator: a tenant holds its batchers");
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
            moonpool_assertions::reachable!("coordinator: a batcher slot is released to its count");
            return Some(RoleStep::Released);
        }
        desk.book(client, cell, &want, &steps).await.ok()?;
        moonpool_assertions::reachable!("coordinator: a tenant's batchers are booked");
        tracing::info!(
            tenant = tenant.0,
            batchers = count,
            "tenant_batchers_booked"
        );
        Some(RoleStep::Booked)
    }
}
