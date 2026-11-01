//! The cell coordinator answers capacity requests (#476,
//! `paros_client::capacity`): through the term's cell session, the single
//! writer of the cell control journal. A request recorded already is
//! answered from its record. Otherwise the coordinator draws a booking id
//! per slot and writes one `GrantCapacity`; a grant the registry refuses at
//! apply (a slot past its machine's capacity, of the wrong class, on a
//! machine that left) books nothing, and the coordinator records the
//! refusal under the same id. So every request is decided once, and its
//! retry reads the decision back, in this term or a later one.
//!
//! The tenant coordinator's own creates ask through the same function: the
//! elected cell coordinator is still every tenant's coordinator, so its
//! request is a call into its own session.

use paros_net::runtime::ParosProviders;

use paros_client::Client;
use paros_client::capacity::{CapacityAnswer, CapacityRequest};
use paros_client::cell::CellSession;
use paros_model::registry::{Grant, GrantKey, Slot, SystemCommand};

/// How many booking ids one slot draws before the request answers
/// unavailable: a random 64-bit id is spent almost never.
const ID_DRAWS: usize = 4;

/// The answer a recorded grant gives.
fn answer_of(grant: &Grant) -> CapacityAnswer {
    match &grant.bookings {
        Some(bookings) => CapacityAnswer::Granted(bookings.clone()),
        None => CapacityAnswer::Refused,
    }
}

/// Decide `request` through the term's session `cell`, each booking id
/// drawn with `draw`.
#[tracing::instrument(level = "debug", skip_all, fields(tenant = request.tenant.0, request = request.request))]
pub(crate) async fn grant<P: ParosProviders>(
    client: &Client<P>,
    cell: &mut CellSession,
    request: &CapacityRequest,
    mut draw: impl FnMut() -> u64,
) -> CapacityAnswer {
    let (key, tenant) = (GrantKey::Request(request.request), request.tenant);
    if request.request == 0 || request.slots.is_empty() {
        return CapacityAnswer::Malformed;
    }
    if let Some(recorded) = cell.registry().grant_of(key) {
        if recorded.tenant != tenant {
            // Another tenant's id: never its answer.
            return CapacityAnswer::Malformed;
        }
        moonpool_assertions::reachable!("capacity: a request is answered from its record");
        return answer_of(recorded);
    }
    // The universe tenant, a special tenant (#685 (special tenants are
    // tenants)), takes slots too: its matchmaker set is placed like any
    // tenant's. The cell tenant's set stays on the founders until step 4.
    let journals = cell.journals();
    let special =
        tenant != journals.cell.tenant && journals.fleet.is_some_and(|f| f.tenant == tenant);
    let registry = cell.registry();
    if !special
        && !registry.hosts(tenant)
        && !registry.footprint(tenant).is_some_and(Grant::granted)
    {
        return CapacityAnswer::UnknownTenant;
    }
    if special {
        moonpool_assertions::reachable!("capacity: a special tenant is granted its slots");
    }
    // Only a machine that serves the cell takes a slot (#511 (placement
    // books machines that never served)): the registry books any
    // registered one, so the coordinator refuses the others.
    let serves = request.slots.iter().all(|slot| registry.serves(slot.node));
    let mut slots: Vec<Slot> = Vec::with_capacity(request.slots.len());
    for wanted in &request.slots {
        let fresh = |booking: u64, slots: &[Slot]| {
            booking != 0
                && !registry.spent().any(|spent| spent == booking)
                && slots.iter().all(|slot| slot.booking != booking)
        };
        let Some(booking) = (0..ID_DRAWS).map(|_| draw()).find(|b| fresh(*b, &slots)) else {
            return CapacityAnswer::Unavailable;
        };
        slots.push(Slot {
            booking,
            node: wanted.node,
            role: wanted.role,
            target: wanted.target,
        });
    }
    if serves {
        let command = SystemCommand::GrantCapacity {
            key,
            tenant,
            slots: Some(slots),
        };
        if cell.record(client, 0, &command).await.is_err() {
            return CapacityAnswer::Unavailable;
        }
        if let Some(recorded) = cell.registry().grant_of(key) {
            assert!(
                recorded.granted(),
                "a grant this session wrote first booked"
            );
            return answer_of(recorded);
        }
    }
    // A slot on a machine that does not serve, or one the registry refused:
    // record the refusal, so the id is decided.
    moonpool_assertions::reachable!("capacity: a request the cell has no room for is refused");
    let refusal = SystemCommand::GrantCapacity {
        key,
        tenant,
        slots: None,
    };
    if cell.record(client, 0, &refusal).await.is_err() {
        return CapacityAnswer::Unavailable;
    }
    cell.registry()
        .grant_of(key)
        .map_or(CapacityAnswer::Unavailable, answer_of)
}
