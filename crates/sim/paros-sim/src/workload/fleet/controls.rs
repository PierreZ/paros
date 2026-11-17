//! Tenant control journals' oracle (#685 (special tenants are tenants)),
//! over the final folds: the cell coordinator books a control journal's
//! slots before it grows the journal onto them, and releases a slot only
//! once the configuration in force left it. After chaos, every control
//! journal runs on its booked machines, as its leader answers `Inspect`,
//! on each seed (rule 8, no skip). A journal that does not has one typed
//! cause: a machine of it, or of its tenant's matchmaker set, was wiped
//! (a wiped machine never comes back under its id, so the journal must
//! leave it, and the coordinator moves a control journal only in steps
//! that wait for the tenant's set), or none, a bug. A cell lost to wipes
//! is the one verdict's (`crate::tail::judge_lost`).

use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_always_or_unreachable, assert_reachable};
use paros::core::journal::JournalIdentifier;
use paros::model::address::Address;
use paros::model::registry::{BookingTarget, Registry};

use crate::client::ChainClient;

use super::cell::Cell;
use super::sets::registry;

/// Read the cell's registry, then each hosted tenant's control journal's
/// configuration in force, until every one settled or `deadline`. Gives up
/// quietly when nothing can be read: the folds' liveness is `final_fleet`'s.
pub(super) async fn controls_booked(
    ctx: &SimContext,
    known: &Cell,
    deadline: Duration,
    connect: impl Fn(&[(u64, Address)]) -> ChainClient,
) {
    let mut attempt = 0_u64;
    let wiped = crate::machine::wiped_ids(ctx.state());
    while ctx.time().now() < deadline && !crate::tail::cut_short(ctx, "controls") {
        let first = known.first(attempt);
        attempt += 1;
        if let Some(registry) = registry(known, first).await {
            let mut stuck = None;
            // The cell plan's journals grow the same way (#685 (special
            // tenants are tenants) step 4).
            let plan: Vec<JournalIdentifier> = std::iter::once(known.journals.cell)
                .chain(known.journals.fleet)
                .chain(known.journals.election)
                .collect();
            // Every hosted tenant the registry has not dropped.
            let hosted = registry
                .hosted()
                .filter(|t| !registry.dropped(*t))
                .filter_map(|tenant| {
                    registry
                        .hosted_tenant(tenant)
                        .map(|h| JournalIdentifier::new(tenant, h.control))
                });
            for journal in plan.iter().copied().chain(hosted) {
                let tenant = journal.tenant;
                // Every member in force holds a slot (#685 (special tenants
                // are tenants)).
                let expected = paros::machine::control_members(&registry, journal);
                if expected.is_empty() {
                    continue;
                }
                // Each member where the registry holds it, else where this
                // operator reached it.
                let book: Vec<(u64, Address)> = expected
                    .iter()
                    .filter_map(|id| {
                        registry
                            .address(*id)
                            .and_then(|addr| Address::parse(addr).ok())
                            .or_else(|| {
                                known
                                    .servers
                                    .iter()
                                    .find(|(s, _)| *s == id.0)
                                    .map(|(_, addr)| addr.clone())
                            })
                            .map(|addr| (id.0, addr))
                    })
                    .collect();
                if book.is_empty() {
                    continue;
                }
                let client = connect(&book);
                let Some(members) = in_force(&client, book.len(), journal).await else {
                    continue;
                };
                let expected: Vec<u64> = expected.iter().map(|n| n.0).collect();
                if members != expected {
                    let gone = wiped_machine(&registry, journal, &members, &wiped);
                    stuck = Some((tenant, gone));
                    break;
                }
                if members.len() > 1 {
                    assert_reachable!(
                        "controls: a control journal runs on several machines after chaos"
                    );
                    if plan.contains(&journal) {
                        assert_reachable!(
                            "controls: a cell plan journal runs on several machines after chaos"
                        );
                    }
                }
            }
            let Some((tenant, gone)) = stuck else {
                return;
            };
            // A change may still be on its way: read again until the
            // deadline.
            if ctx.time().now() + Duration::from_millis(500) >= deadline {
                // A lost cell is the one verdict's (`crate::tail::judge_lost`).
                let lost = crate::tail::cell_lost(ctx.state());
                assert_always_or_unreachable!(
                    lost || gone,
                    "controls: every control journal runs on its booked machines after chaos",
                    { "tenant" => tenant.0 }
                );
                assert_always_or_unreachable!(
                    lost || !gone,
                    "controls: a control journal leaves a wiped machine after chaos",
                    { "tenant" => tenant.0 }
                );
                return;
            }
        }
        if ctx.time().sleep(Duration::from_millis(250)).await.is_err() {
            crate::tail::cut_short(ctx, "controls");
            return;
        }
    }
}

/// Whether a machine of `journal`'s configuration in force (`members`),
/// of its booked slots, or of its tenant's matchmaker set's slots was
/// wiped: the typed cause of a control journal that is not on its booked
/// machines.
fn wiped_machine(
    registry: &Registry,
    journal: JournalIdentifier,
    members: &[u64],
    wiped: &std::collections::BTreeSet<u64>,
) -> bool {
    let set = paros::machine::set_of(journal.tenant);
    let target = BookingTarget::Journal(journal);
    members.iter().any(|m| wiped.contains(m))
        || registry
            .bookings()
            .any(|(_, b)| (b.target == set || b.target == target) && wiped.contains(&b.node.0))
}

/// The members of `journal`'s configuration in force, as its leader among
/// the `count` servers of `client` answers, sorted; `None` when none leads.
async fn in_force(
    client: &ChainClient,
    count: usize,
    journal: JournalIdentifier,
) -> Option<Vec<u64>> {
    for target in 0..count {
        if let Some(reply) = client.inspect(target, journal).await
            && reply.leader
        {
            let mut members = reply.members;
            members.sort_unstable();
            return Some(members);
        }
    }
    None
}
