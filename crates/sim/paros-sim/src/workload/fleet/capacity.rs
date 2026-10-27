//! Capacity requests (#476): on its own BUGGIFY location, after a journal
//! request, the operator asks the cell coordinator for one tenant-level slot
//! through `paros::client::capacity`, as a tenant coordinator does: mostly on
//! a machine that serves the cell and has room (a frontend slot on a
//! `stateless` machine, a replica slot on a `storage` one), else on any
//! machine of the run with a frontend slot, which may be refused. On another
//! location it sends the decided request again, which must read back the
//! first answer: the outcome is recorded under the request's id in the cell
//! control journal, so a retry books nothing twice, across a coordinator
//! change too.

use moonpool_sim::{
    RandomProvider, SimContext, assert_always, assert_reachable, buggify_with_prob,
};
use paros::client::capacity::{self, CapacityAnswer, CapacityRequest, Wanted};
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;
use paros::model::registry::{BookingTarget, Class, Role};

use super::FleetOps;
use super::cell::Cell;

impl FleetOps {
    /// One capacity request for a slot of `tenant`, and on a
    /// BUGGIFY location its retry.
    pub(super) async fn ask_capacity(&self, ctx: &SimContext, cell: &Cell, tenant: TenantId) {
        let Some(election) = cell.journals.election else {
            return;
        };
        let mut machines: Vec<u64> = cell.members.clone();
        machines.extend(crate::machine::minted_ids(ctx.state()));
        if machines.is_empty() {
            return;
        }
        // Mostly a machine that serves the cell and has room, with a role of
        // its class, as a tenant coordinator asks; else any machine with a
        // resolver slot, which may be refused. Never a frontend or a batcher:
        // the tenant coordinator places those to the tenant's counts (#723
        // (frontend as a parosd role)).
        let room: Vec<(u64, Role)> = if ctx.random().random_bool(0.8)
            && let Some(registry) = super::sets::registry(cell, 0).await
        {
            registry
                .nodes()
                .filter(|(node, _)| registry.serves(*node) && registry.available(*node) > 0)
                .map(|(node, n)| {
                    let role = match n.class {
                        Class::Stateless => Role::Resolver,
                        Class::Storage => Role::Replica,
                    };
                    (node.0, role)
                })
                .collect()
        } else {
            Vec::new()
        };
        let (node, role) = if room.is_empty() {
            (
                machines[ctx.random().random_range(0..machines.len())],
                Role::Resolver,
            )
        } else {
            room[ctx.random().random_range(0..room.len())]
        };
        let request = CapacityRequest {
            request: ctx.random().random_range(1..u64::MAX),
            tenant,
            slots: vec![Wanted {
                node: NodeId(node),
                role,
                target: BookingTarget::Tenant { tenant },
            }],
        };
        let answer = self.ask_capacity_once(cell, election, &request).await;
        match &answer {
            CapacityAnswer::Granted(bookings) => {
                assert_always!(
                    bookings.len() == request.slots.len(),
                    "capacity: a grant names one booking per slot",
                    { "tenant" => tenant.0, "bookings" => bookings.len() }
                );
                assert_reachable!("capacity: a tenant's slot is granted");
            }
            CapacityAnswer::Refused => {
                assert_reachable!("capacity: a request is refused");
            }
            CapacityAnswer::Malformed => {
                assert_always!(
                    false,
                    "capacity: a well-formed request is never refused malformed",
                    { "tenant" => tenant.0 }
                );
            }
            CapacityAnswer::UnknownTenant
            | CapacityAnswer::NotCoordinator
            | CapacityAnswer::Unavailable => {}
        }
        if answer.is_retryable() || !buggify_with_prob!(0.5) {
            return;
        }
        // The answer was lost: the same id again reads back the decision.
        let again = self.ask_capacity_once(cell, election, &request).await;
        if again.is_retryable() {
            return;
        }
        assert_always!(
            again == answer,
            "capacity: a retried request reads back its first answer",
            { "tenant" => tenant.0 }
        );
        assert_reachable!("capacity: a client retries a decided request");
    }

    async fn ask_capacity_once(
        &self,
        cell: &Cell,
        election: JournalIdentifier,
        request: &CapacityRequest,
    ) -> CapacityAnswer {
        capacity::request(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &cell.client,
            election,
            request,
            self.patience,
        )
        .await
    }
}
