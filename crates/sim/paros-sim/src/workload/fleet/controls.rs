//! Tenant control journals' oracle (#685 (special tenants are tenants)),
//! over the final folds: the cell coordinator books a control journal's
//! slots before it grows the journal onto them, and releases a slot only
//! once the configuration in force left it. After chaos, every hosted
//! tenant's control journal runs on the founding members and its booked
//! machines, as its leader answers `Inspect`, unless a slot sits on a
//! machine that leaves or a member was wiped.

use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_always, assert_reachable};
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::model::address::Address;
use paros::model::registry::{BookingTarget, NodeStanding, Registry, Role};

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
    let founders: Vec<NodeId> = known.members.iter().copied().map(NodeId).collect();
    while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
        let first = known.first(attempt);
        attempt += 1;
        if let Some(registry) = registry(known, first).await {
            let mut stuck = None;
            for tenant in registry.hosted().filter(|t| !registry.dropped(*t)) {
                let Some(control) = registry.hosted_tenant(tenant).map(|h| h.control) else {
                    continue;
                };
                let journal = JournalIdentifier::new(tenant, control);
                let expected = paros::machine::control_members(&founders, &registry, journal);
                if !judged(&registry, journal, &expected, &wiped) {
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
                    stuck = Some(tenant);
                    break;
                }
                if members.len() > founders.len() {
                    assert_reachable!(
                        "controls: a control journal runs past the founders after chaos"
                    );
                }
            }
            let Some(tenant) = stuck else {
                return;
            };
            // A change may still be on its way: read again until the
            // deadline.
            if ctx.time().now() + Duration::from_millis(500) >= deadline {
                assert_always!(
                    crate::machine::cell_lost(ctx.state()),
                    "controls: every tenant control journal runs on its founders and booked machines after chaos",
                    { "tenant" => tenant.0 }
                );
                return;
            }
        }
        if ctx.time().sleep(Duration::from_millis(250)).await.is_err() {
            return;
        }
    }
}

/// Whether `journal`'s control journal can be judged: no slot on a machine
/// that leaves, and no member wiped (a wiped acceptor never rejoins).
fn judged(
    registry: &Registry,
    journal: JournalIdentifier,
    expected: &[NodeId],
    wiped: &std::collections::BTreeSet<u64>,
) -> bool {
    let leaving = registry.bookings().any(|(_, b)| {
        b.role == Role::Acceptor
            && b.target == BookingTarget::Journal(journal)
            && registry
                .get(b.node)
                .is_none_or(|n| n.standing != NodeStanding::Registered)
    });
    !leaving && !expected.iter().any(|n| wiped.contains(&n.0))
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
