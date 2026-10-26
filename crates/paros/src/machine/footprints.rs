//! The cell coordinator keeps every tenant's footprint whole (#476,
//! `crate::model::footprint`): one step per watch round, in turn over the tenants
//! whose footprint the cell granted and did not drop (the cell tenant and
//! the universe tenant too). A step places a tenant's coordinator on a
//! `stateless` machine, places a dead one again, or moves a storage slot
//! off a draining machine. Each step starts from the registry alone, so a
//! later term finishes what an earlier one started.
//!
//! The cell tenant's and the universe tenant's footprints are booked
//! here: the coordinator joins the cell to the fleet (#513 (fleet Init
//! duel)), and `init` books neither. A step books them once they fit; the
//! coordinator never records their refusal: they wait for room. A `users`
//! tenant's birth footprint is booked, or refused, as a step of its
//! creation (`super::universe::tenant_steps`, #225).

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use crate::ParosProviders;
use paros_core::TenantId;

use crate::client::Client;
use crate::client::cell::CellSession;
use crate::model::footprint::{self, Fix, Footprint};
use crate::model::registry::{GrantKey, Registry, Role, SystemCommand};

/// The footprint steps of one coordinator term.
#[derive(Debug)]
pub(super) struct Footprints {
    /// The cell tenant and, when this cell hosts it, the universe tenant:
    /// booked here, never by `init`.
    internal: Vec<TenantId>,
    /// The tenant the last step took: the next step starts after it.
    last: Option<TenantId>,
}

/// The tenants whose footprint the cell granted and still holds.
fn granted(registry: &Registry) -> BTreeSet<TenantId> {
    registry
        .grants()
        .filter_map(|(key, grant)| match key {
            GrantKey::Footprint(tenant) if grant.granted() && !registry.dropped(tenant) => {
                Some(tenant)
            }
            _ => None,
        })
        .collect()
}

impl Footprints {
    /// The steps of a term whose cell's internal tenants are `internal`.
    pub(super) fn new(internal: Vec<TenantId>) -> Self {
        assert!(
            internal.iter().all(|tenant| tenant.0 != 0),
            "an internal tenant is set"
        );
        Self {
            internal,
            last: None,
        }
    }

    /// The grant of the first internal tenant with no footprint record
    /// that fits now.
    fn internal_grant(&self, registry: &Registry) -> Option<SystemCommand> {
        self.internal.iter().find_map(|&tenant| {
            if registry.footprint(tenant).is_some() {
                return None;
            }
            let slots = footprint::plan(registry, tenant, &Footprint::birth()).ok()?;
            Some(SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(tenant),
                tenant,
                slots: Some(slots),
            })
        })
    }

    /// Take one step for the next tenant whose footprint needs one, through
    /// the term's session `cell`. `Some(true)` when it wrote, `Some(false)`
    /// when no footprint needs a step now, `None` when the write did not
    /// land: the term writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) async fn step<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
    ) -> Option<bool> {
        let registry = cell.registry();
        if let Some(grant) = self.internal_grant(registry) {
            if cell.record(client, 0, &grant).await.is_err() {
                return None;
            }
            moonpool_assertions::reachable!(
                "footprint: the coordinator books an internal tenant's footprint"
            );
            return Some(true);
        }
        let tenants = granted(registry);
        // In turn: the tenants after the last one, then from the first.
        let order: Vec<TenantId> = match self.last {
            Some(last) => tenants
                .range((Excluded(last), Unbounded))
                .chain(tenants.range(..=last))
                .copied()
                .collect(),
            None => tenants.iter().copied().collect(),
        };
        let birth = Footprint::birth();
        let Some((tenant, fix)) = order
            .into_iter()
            .find_map(|tenant| footprint::fix(registry, tenant, &birth).map(|fix| (tenant, fix)))
        else {
            return Some(false);
        };
        self.last = Some(tenant);
        let command = match fix {
            Fix::Book(slot) => SystemCommand::BookCapacity {
                booking: slot.booking,
                node: slot.node,
                role: slot.role,
                target: slot.target,
            },
            Fix::Release { booking } => SystemCommand::ReleaseCapacity { booking },
        };
        if cell.record(client, 0, &command).await.is_err() {
            return None;
        }
        if let Fix::Book(slot) = fix
            && cell.registry().booking(slot.booking).is_some()
            && slot.role == Role::Coordinator
        {
            moonpool_assertions::reachable!(
                "footprint: a tenant coordinator is placed on a stateless machine"
            );
        }
        Some(true)
    }
}
