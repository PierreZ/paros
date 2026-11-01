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
use paros::machine::Class;
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

/// What one read found of the cell's set moves: every matchmaker slot
/// booked, and the first unsettled set's tenant and set in force. A move on
/// its way changes it.
type Seen = (Vec<(u64, u64)>, TenantId, Vec<u64>);

/// The check's deadline after a read found `seen` at `now`: while the set
/// moves go on (`seen` differs from the last read, `before`), the check
/// gets [`super::FLEET_SETTLE`] again from now, never past `ceiling`. The
/// coordinator takes one set step per watch round, and a round in the tail
/// can take seconds, so a move of several steps may outlast a fixed window
/// while it progresses. Seed 17928495823018000955 on d23a42f: with rounds
/// about 5 s apart, the move finished 9 s after a fixed 20 s deadline.
fn extended(
    deadline: Duration,
    ceiling: Duration,
    now: Duration,
    (before, seen): (Option<&Seen>, &Seen),
) -> Duration {
    let moved = before.is_some_and(|before| before != seen);
    let next = if moved {
        deadline
            .max(now + super::FLEET_SETTLE)
            .min(ceiling.max(deadline))
    } else {
        deadline
    };
    assert!(next >= deadline, "a deadline never moves back");
    assert!(
        next <= ceiling.max(deadline),
        "a deadline never passes the ceiling"
    );
    next
}

/// The founding members at the addresses `registry` holds for them.
fn founder_book(known: &Cell, registry: &Registry) -> Vec<(u64, Address)> {
    known
        .members
        .iter()
        .filter_map(|id| {
            registry
                .address(NodeId(*id))
                .and_then(|addr| Address::parse(addr).ok())
                .map(|addr| (*id, addr))
        })
        .collect()
}

/// [`extended`] over the last read `previous`, which `seen` replaces.
fn advance(
    deadline: Duration,
    ceiling: Duration,
    now: Duration,
    (previous, seen): (&mut Option<Seen>, Seen),
) -> Duration {
    let next = extended(deadline, ceiling, now, (previous.as_ref(), &seen));
    if next > deadline {
        assert_reachable!("sets: a set move on its way extends the check");
    }
    *previous = Some(seen);
    next
}

/// Read the cell's registry, then each hosted tenant's set in force, until
/// every booked set settled or `deadline` ([`extended`] while the moves go
/// on). Gives up quietly when nothing can be read: the folds' liveness is
/// `final_fleet`'s.
pub(super) async fn sets_booked(
    ctx: &SimContext,
    known: &Cell,
    mut deadline: Duration,
    connect: impl Fn(&[(u64, Address)]) -> ChainClient,
) {
    let ceiling = ctx.time().now() + super::HANDOVER_CEILING;
    let mut previous: Option<Seen> = None;
    let mut attempt = 0_u64;
    // A machine cut off from its founding members is judged like any other:
    // the cell coordinator hands it the cell's book again (#505 (stranded
    // admitted machine)).
    let wiped = crate::machine::wiped_ids(ctx.state());
    while ctx.time().now() < ceiling.max(deadline) && !ctx.shutdown().is_cancelled() {
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
            let founders = founder_book(known, &registry);
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
                    stuck = Some((tenant, why, members));
                    break;
                }
            }
            let Some((tenant, why, members)) = stuck else {
                return;
            };
            let seen: Seen = (matchmaker_slots(&registry), tenant, members);
            deadline = advance(deadline, ceiling, ctx.time().now(), (&mut previous, seen));
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

/// Every matchmaker slot the registry books, as `(node, tenant)`, in order.
fn matchmaker_slots(registry: &Registry) -> Vec<(u64, u64)> {
    let mut slots: Vec<(u64, u64)> = registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Matchmaker)
        .map(|(_, b)| (b.node.0, b.target.tenant().0))
        .collect();
    slots.sort_unstable();
    slots
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
                    && n.class == Class::Storage
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use paros::core::journal::TenantId;

    use super::{Seen, extended};

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn a_set_move_on_its_way_gets_the_settle_window_again_up_to_the_ceiling() {
        let tenant = TenantId(9);
        let before: Seen = (vec![(1, 9), (3, 9), (4, 9)], tenant, vec![1, 2, 3, 4]);
        let booked: Seen = (
            vec![(1, 9), (3, 9), (4, 9), (5, 8)],
            tenant,
            vec![1, 2, 3, 4],
        );
        let (deadline, ceiling) = (secs(20), secs(60));
        // The first read has nothing to compare with.
        assert_eq!(
            extended(deadline, ceiling, secs(5), (None, &before)),
            deadline
        );
        // A stuck set keeps its deadline.
        assert_eq!(
            extended(
                deadline,
                ceiling,
                secs(15),
                (Some(&before), &before.clone())
            ),
            deadline
        );
        // A slot booked since the last read: the window starts again.
        assert_eq!(
            extended(deadline, ceiling, secs(15), (Some(&before), &booked)),
            secs(35)
        );
        // Never past the ceiling.
        assert_eq!(
            extended(secs(50), ceiling, secs(55), (Some(&before), &booked)),
            ceiling
        );
    }
}
