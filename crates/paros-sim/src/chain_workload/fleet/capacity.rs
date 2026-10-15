//! Capacity requests (#476): on its own BUGGIFY location, after a journal
//! request, the operator asks the cell coordinator for one frontend slot of
//! its tenant through `paros::client::capacity`, as a tenant coordinator
//! does, on a machine of the run drawn at random: a `stateless` machine
//! that serves the cell may take it, any other is refused. On another
//! location it sends the decided request again, which must read back the
//! first answer: the outcome is recorded under the request's id in the cell
//! control journal, so a retry books nothing twice, across a coordinator
//! change too.

use moonpool_sim::{
    RandomProvider, SimContext, assert_always, assert_reachable, buggify_with_prob,
};
use paros::client::capacity::{self, CapacityAnswer, CapacityRequest, Wanted};
use paros::system::{BookingTarget, Role};
use paros::{JournalIdentifier, NodeId, TenantId};

use super::FleetOps;
use super::cell::Cell;

impl FleetOps {
    /// One capacity request for a frontend slot of `tenant`, and on a
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
        let node = NodeId(machines[ctx.random().random_range(0..machines.len())]);
        let request = CapacityRequest {
            request: ctx.random().random_range(1..u64::MAX),
            tenant,
            slots: vec![Wanted {
                node,
                role: Role::Frontend,
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
                assert_reachable!("capacity: a tenant's frontend slot is granted");
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
