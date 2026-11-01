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
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;
use paros::model::address::Address;
use paros::model::registry::{NodeStanding, Registry, Role};

use crate::client::ChainClient;

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
pub(super) async fn sets_booked(
    ctx: &SimContext,
    known: &Cell,
    deadline: Duration,
    connect: impl Fn(&[(u64, Address)]) -> ChainClient,
) {
    let mut attempt = 0_u64;
    // A machine cut off from its founding members is judged like any other:
    // the cell coordinator hands it the cell's book again (#505 (stranded
    // admitted machine)).
    let wiped = crate::machine::wiped_ids(ctx.state());
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
            // The founding members as the coordinator dials them: at the
            // address the registry holds, every founder, not only the ones
            // this operator reached at the tail's start. Only the founder
            // that drove a handover holds its generation until the others
            // campaign again: a belief is not a fact. Seed
            // 461662224030609195 on 522c917: the operator reached two
            // founders that still believed generation 0.
            let founders: Vec<(u64, Address)> = known
                .members
                .iter()
                .filter_map(|id| {
                    registry
                        .address(NodeId(*id))
                        .and_then(|addr| Address::parse(addr).ok())
                        .map(|addr| (*id, addr))
                })
                .collect();
            // A registry that holds no founder's address names nobody to
            // dial yet: read it again. Seed 18233303712623226963 on
            // 3c8c73d: the check built a client over no server.
            if founders.is_empty() {
                if ctx.time().sleep(Duration::from_millis(250)).await.is_err() {
                    return;
                }
                continue;
            }
            let client = connect(&founders);
            let mut stuck = None;
            // The universe tenant's set moves too (#685 (special tenants are
            // tenants) step 3), judged by its own control journal; the cell
            // tenant's stays on the founding members until step 4.
            let special: Vec<JournalIdentifier> = known
                .journals
                .fleet
                .into_iter()
                .filter(|fleet| fleet.tenant != known.journals.cell.tenant)
                .collect();
            let hosted = registry
                .hosted()
                .filter(|t| !registry.dropped(*t))
                .filter_map(|tenant| {
                    registry
                        .hosted_tenant(tenant)
                        .map(|h| JournalIdentifier::new(tenant, h.control))
                });
            for journal in special.iter().copied().chain(hosted) {
                let tenant = journal.tenant;
                let Some(members) = in_force(&client, founders.len(), journal).await else {
                    continue;
                };
                if special.contains(&journal)
                    && registry.bookings().any(|(_, b)| {
                        b.role == Role::Matchmaker && b.target == paros::machine::set_of(tenant)
                    })
                {
                    assert_reachable!("sets: a special tenant's booked set is judged after chaos");
                }
                let kept: &[u64] = if special.contains(&journal) {
                    &known.members
                } else {
                    &[]
                };
                if let Some(why) =
                    unsettled(&registry, tenant, &members, (&known.members, &wiped), kept)
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
pub(super) async fn registry(known: &Cell, first: usize) -> Option<Registry> {
    let mut cell = judged_cell(&known.members);
    let loaded =
        paros::client::checkpoint::load(&mut cell, known.journals.cell, &known.client, first, 0)
            .await;
    matches!(loaded, LoadOutcome::Loaded { .. }).then(|| cell.state().registry().clone())
}

/// The members of the latest generation the founding members believe for
/// `journal`, or `None` when none answers.
pub(super) async fn in_force(
    client: &ChainClient,
    count: usize,
    journal: JournalIdentifier,
) -> Option<Vec<u64>> {
    let mut best: Option<(u64, Vec<u64>)> = None;
    for target in 0..count {
        if let Some(reply) = client.inspect(target, journal).await
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
/// it wiped). `kept` are the members a special tenant's set keeps beside its
/// slots: its founding members (#685 (special tenants are tenants)).
fn unsettled(
    registry: &Registry,
    tenant: TenantId,
    members: &[u64],
    (founders, wiped): (&[u64], &BTreeSet<u64>),
    kept: &[u64],
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
                    && n.class.stores()
                    && registry.serves(*node)
                    && registry.liveness(*node).up
                    && registry.available(*node) > 0
                    && !slots.contains(node)
            })
            .count();
        return (free >= leaving).then_some(Unsettled::Unmoved);
    }
    // A special tenant's set keeps its founding members beside its slots
    // (#685 (special tenants are tenants) step 3).
    let mut booked: Vec<u64> = slots
        .iter()
        .map(|n| n.0)
        .chain(kept.iter().copied())
        .collect();
    booked.sort_unstable();
    booked.dedup();
    if booked != members {
        return Some(Unsettled::Unbooked);
    }
    if members.iter().any(|m| !founders.contains(m)) {
        assert_reachable!("sets: a set in force runs on an admitted machine");
    }
    None
}
