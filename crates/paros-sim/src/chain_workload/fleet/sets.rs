//! Matchmaker sets' oracle (#501 (matchmaker sets off the founders)), over
//! the final folds: the cell coordinator books a set's slots before it hands
//! the set over onto them, and releases a slot only once the set in force
//! no longer names it. After chaos, every tenant's set in force is its
//! booked machines, and no set holds a slot on a draining machine while it
//! has a machine to go to for each such slot. The set in force is the
//! latest generation the founding members believe for the tenant's control
//! journal (`Inspect`), as the coordinator reads it.

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_always, assert_reachable};
use paros::client::checkpoint::LoadOutcome;
use paros::machine::Class;
use paros::system::{NodeStanding, Registry, Role};
use paros::{JournalIdentifier, NodeId, TenantId};

use super::cell::Cell;
use super::judged_cell;

/// Why a tenant's set has not settled yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unsettled {
    /// Its set in force is not its booked machines.
    Unbooked,
    /// It holds a slot on a draining machine while it has a machine to go
    /// to for each such slot.
    Unmoved,
}

/// Read the cell's registry, then each hosted tenant's set in force, until
/// every booked set settled or `deadline`. Gives up quietly when nothing can
/// be read: the folds' liveness is `final_fleet`'s.
pub(super) async fn sets_booked(ctx: &SimContext, known: &Cell, deadline: Duration) {
    let mut attempt = 0_u64;
    // A machine cut off from the control journals for good is judged like a
    // wiped one: nothing booked on it can start (§3.2's static-stability
    // limit; a lone founder renamed after admissions).
    let mut wiped = crate::machine::wiped_ids(ctx.state());
    wiped.extend(crate::machine::stranded_ids(ctx.state()));
    while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
        let first = known.first(attempt);
        attempt += 1;
        if let Some(registry) = registry(known, first).await {
            // The coordinator records only an admission that landed (#511
            // (placement books machines that never served)). A wipe takes
            // the board's admission with the disk, and the registry never
            // learns of a wipe, so a wiped id is not judged (#529 (sets
            // oracles after PR #515)).
            let admitted = crate::machine::admitted_ids(ctx.state());
            for (node, _) in registry
                .nodes()
                .filter(|(node, n)| n.admitted && !wiped.contains(&node.0))
            {
                assert_always!(
                    admitted.contains(&node.0),
                    "sets: the registry holds as admitted only a machine admitted",
                    { "node" => node.0 }
                );
            }
            let mut stuck = None;
            for tenant in registry.hosted().filter(|t| !registry.dropped(*t)) {
                let Some(control) = registry.hosted_tenant(tenant).map(|h| h.control) else {
                    continue;
                };
                let journal = JournalIdentifier::new(tenant, control);
                let Some(members) = in_force(known, journal).await else {
                    continue;
                };
                if let Some(why) = unsettled(&registry, tenant, &members, (&known.members, &wiped))
                {
                    stuck = Some((tenant, why));
                    break;
                }
            }
            let Some((tenant, why)) = stuck else {
                return;
            };
            // A set's move may still be on its way: read again until the
            // deadline.
            if ctx.time().now() + Duration::from_millis(500) >= deadline {
                let lost = crate::machine::cell_lost(ctx.state());
                match why {
                    Unsettled::Unbooked => assert_always!(
                        lost,
                        "sets: every tenant's set in force is its booked machines after chaos",
                        { "tenant" => tenant.0 }
                    ),
                    Unsettled::Unmoved => assert_always!(
                        lost,
                        "sets: a set with a machine to go to leaves a draining one after chaos",
                        { "tenant" => tenant.0 }
                    ),
                }
                return;
            }
        }
        if ctx.time().sleep(Duration::from_millis(250)).await.is_err() {
            return;
        }
    }
}

/// The cell's registry, read to its tail.
async fn registry(known: &Cell, first: usize) -> Option<Registry> {
    let mut cell = judged_cell(&known.members);
    let loaded =
        paros::client::checkpoint::load(&mut cell, known.journals.cell, &known.client, first, 0)
            .await;
    matches!(loaded, LoadOutcome::Loaded { .. }).then(|| cell.state().registry().clone())
}

/// The members of the latest generation the founding members believe for
/// `journal`, or `None` when none answers.
async fn in_force(known: &Cell, journal: JournalIdentifier) -> Option<Vec<u64>> {
    let mut best: Option<(u64, Vec<u64>)> = None;
    for target in 0..known.servers.len() {
        if let Some(reply) = known.client.inspect(target, journal).await
            && best
                .as_ref()
                .is_none_or(|(generation, _)| reply.matchmaker_generation > *generation)
        {
            best = Some((reply.matchmaker_generation, reply.matchmakers));
        }
    }
    best.map(|(_, mut members)| {
        members.sort_unstable();
        members
    })
}

/// Why `tenant`'s set, whose set in force is `members`, has not settled:
/// `None` when it did, or when it cannot (no slot booked yet, a machine of
/// it wiped).
fn unsettled(
    registry: &Registry,
    tenant: TenantId,
    members: &[u64],
    (founders, wiped): (&[u64], &BTreeSet<u64>),
) -> Option<Unsettled> {
    let target = paros::machine::set_of(tenant);
    let slots: Vec<NodeId> = registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Matchmaker && b.target == target)
        .map(|(_, b)| b.node)
        .collect();
    // A set born on the founding members holds no slot until it is placed,
    // and a cell may have no room for it.
    if slots.is_empty() || members.is_empty() {
        return None;
    }
    assert_reachable!("sets: a booked set is judged after chaos");
    // A wiped machine's set is gone for good: the handover cannot stop it.
    if slots
        .iter()
        .map(|n| n.0)
        .chain(members.iter().copied())
        .any(|n| wiped.contains(&n))
    {
        return None;
    }
    let registered = |node: NodeId| {
        registry
            .get(node)
            .is_some_and(|n| n.standing == NodeStanding::Registered)
    };
    let leaving = slots.iter().filter(|n| !registered(**n)).count();
    if leaving > 0 {
        let free = registry
            .nodes()
            .filter(|(node, n)| {
                n.standing == NodeStanding::Registered
                    && n.class == Class::Storage
                    && registry.serves(*node)
                    && registry.liveness(*node).up
                    && registry.available(*node) > 0
                    && !slots.contains(node)
            })
            .count();
        return (free >= leaving).then_some(Unsettled::Unmoved);
    }
    let mut booked: Vec<u64> = slots.iter().map(|n| n.0).collect();
    booked.sort_unstable();
    if booked != members {
        return Some(Unsettled::Unbooked);
    }
    if members.iter().any(|m| !founders.contains(m)) {
        assert_reachable!("sets: a set in force runs on an admitted machine");
    }
    None
}
