//! Matchmaker sets' oracle (#501 (matchmaker sets off the founders)), over
//! the final folds: the cell coordinator books a set's slots before it hands
//! the set over onto them, and releases a slot only once the set in force
//! no longer names it. After chaos, every tenant's set in force is its
//! booked machines, and no set holds a slot on a draining machine while it
//! has a machine to go to for each such slot. The set in force is the
//! latest generation the machines of the tenant's slots believe for the
//! tenant, read through its control journal (`Inspect` names the set the
//! tenant's journals on a node believe), as the coordinator reads it.
//!
//! The check is judged over one coordinator term's window: a served term
//! that replaces the one the check counts from gets [`super::FLEET_SETTLE`]
//! again, up to [`super::HANDOVER_CEILING`] (`election::window_for_term`),
//! as the renamed check and a pending operation are. An operator's claim on
//! the cell control journal in the tail supersedes the term's session, the
//! term resigns, and its successor re-asks every pending set's handover
//! before it releases a slot (#563 (set release races a late handover)),
//! one step per watch round. Witness replay-main 5742660267466840157 on
//! 860d6c7: the cell tenant's set, shrunk from its four founders to three,
//! was in force on its three booked machines 1 s into the tail; the term's
//! release of the fourth slot was superseded, the next term served 12 s
//! in, re-asked two sets' handovers in turn, and released the slot 26 s in,
//! 6 s after a fixed window.
//!
//! The draining claim is load-bearing: sweep recipe `11402846704955563023
//! 284084:17684519257711352965,550800:13491835932460455912` on 9ec57f4
//! (PR #883 (#685 (no founders))) found a hosted set on the one slot of a
//! draining initial member, two storage machines free and a third gone,
//! that the coordinator never moved: it sized the set at three over the
//! gone machine too, no plan could book three, and nothing stayed to hand
//! over to. The size is now capped at the machines placement can book
//! (`paros_coordinator`'s `tenant/sets.rs`, `eligible`); green after.

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

/// What one read found of the cell's set moves: every matchmaker slot
/// booked, and the first unsettled set's tenant, set in force and its
/// generation. A move on its way changes it: a slot booked or released,
/// or a handover landed. A handover onto the members in force lands too
/// (#563 (set release races a late handover): a term releases a slot only
/// once a handover it started is in force), so the generation is part of
/// what moved.
pub(super) type Seen = (Vec<(u64, u64)>, TenantId, Vec<u64>, u64);

/// The check's deadline after a read found `seen` at `now`: while the set
/// moves go on (`seen` differs from the last read, `before`), the check
/// gets [`super::FLEET_SETTLE`] again from now, never past `ceiling`. The
/// coordinator takes one set step per watch round, and a round in the tail
/// can take seconds, so a move of several steps may outlast a fixed window
/// while it progresses. Seed 17928495823018000955 on d23a42f: with rounds
/// about 5 s apart, the move finished 9 s after a fixed 20 s deadline.
pub(super) fn extended(
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

/// [`extended`] over the last read `previous`, which `seen` replaces, then
/// `election::window_for_term` over the served term `term` against the
/// term the check counts from, `counted`: a served term that replaces it
/// gets [`super::FLEET_SETTLE`] again from `now`, never past `ceiling`. A
/// successor term re-asks every pending set's handover before it releases
/// a slot (#563 (set release races a late handover)), one step per watch
/// round, so a term change is a move on its way the reads cannot see
/// (witness replay-main 5742660267466840157 on 860d6c7, the module doc).
fn advance(
    deadline: Duration,
    ceiling: Duration,
    now: Duration,
    (previous, seen): (&mut Option<Seen>, Seen),
    (counted, term): (&mut Option<u64>, Option<u64>),
) -> Duration {
    let moved = extended(deadline, ceiling, now, (previous.as_ref(), &seen));
    if moved > deadline {
        assert_reachable!("sets: a set move on its way extends the check");
    }
    *previous = Some(seen);
    let next = super::election::window_for_term(moved, ceiling, now, counted, term);
    if next > moved {
        assert_reachable!("sets: a new term restarts the sets check's window");
    }
    assert!(next >= deadline, "a deadline never moves back");
    assert!(
        next <= ceiling.max(deadline),
        "a deadline never passes the ceiling"
    );
    next
}

/// The control journals of the hosted tenants this oracle judges. A tenant
/// on the move, or born in another cell, has its set read through its birth
/// cell's initial members, never this cell's (#784 (coordinator handover)): the
/// move's own oracles judge it.
fn judged(registry: &Registry) -> impl Iterator<Item = JournalIdentifier> + '_ {
    registry
        .hosted()
        .filter(|t| !registry.dropped(*t) && !registry.moving(*t) && !registry.born_elsewhere(*t))
        .filter_map(|tenant| {
            registry
                .hosted_tenant(tenant)
                .map(|h| JournalIdentifier::new(tenant, h.control))
        })
}

/// Read the cell's registry, then each hosted tenant's set in force, until
/// every booked set settled or `deadline` ([`extended`] while the moves go
/// on, and [`advance`]d again by a served term that replaces `settled`, the
/// term the election check judged settled, else the first one read). Gives
/// up quietly when nothing can be read: the folds' liveness is
/// `final_fleet`'s.
pub(super) async fn sets_booked(
    ctx: &SimContext,
    known: &Cell,
    mut deadline: Duration,
    settled: Option<u64>,
    connect: impl Fn(&[(u64, Address)]) -> ChainClient,
) {
    let ceiling = ctx.time().now() + super::HANDOVER_CEILING;
    let mut previous: Option<Seen> = None;
    let mut counted = settled;
    let mut attempt = 0_u64;
    // A machine cut off from its initial members is judged like any other:
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
            // An initial member is admitted by the registry's seed (#685
            // (special tenants are tenants)): it took no `Admit`.
            for (node, _) in registry.nodes().filter(|(node, n)| {
                n.admitted && !wiped.contains(&node.0) && !known.members.contains(&node.0)
            }) {
                assert_always!(
                    admitted.contains(&node.0),
                    "sets: the registry holds as admitted only a machine admitted",
                    { "node" => node.0 }
                );
            }
            let initial_members = dialed_initial_members(ctx, known, &registry);
            // A registry that holds no initial member's address names nobody to
            // dial yet: read it again. Seed 18233303712623226963 on
            // 3c8c73d: the check built a client over no server.
            if initial_members.is_empty() {
                if ctx.time().sleep(Duration::from_millis(250)).await.is_err() {
                    return;
                }
                continue;
            }
            let client = connect(&initial_members);
            let mut stuck = None;
            // The special tenants' sets move too (#685 (special tenants are
            // tenants) steps 3 and 4), judged by their own control journals.
            let special: Vec<JournalIdentifier> = std::iter::once(known.journals.cell)
                .chain(known.journals.fleet)
                .collect();
            for journal in special.iter().copied().chain(judged(&registry)) {
                let tenant = journal.tenant;
                let read =
                    read_set(&registry, (&client, &initial_members), &connect, journal).await;
                let Some((generation, members)) = read else {
                    continue;
                };
                if special.contains(&journal)
                    && registry.bookings().any(|(_, b)| {
                        b.role == Role::Matchmaker && b.target == paros::machine::set_of(tenant)
                    })
                {
                    assert_reachable!("sets: a special tenant's booked set is judged after chaos");
                }
                if let Some(why) = unsettled(&registry, tenant, &members, (&known.members, &wiped))
                {
                    stuck = Some((tenant, why, members, generation));
                    break;
                }
            }
            let Some((tenant, why, members, generation)) = stuck else {
                return;
            };
            let seen: Seen = (matchmaker_slots(&registry), tenant, members, generation);
            let served = super::election::coordinator_term(
                ctx,
                known,
                usize::try_from(attempt).unwrap_or(usize::MAX),
            )
            .await
            .and_then(|(term, served)| served.then_some(term));
            deadline = advance(
                deadline,
                ceiling,
                ctx.time().now(),
                (&mut previous, seen),
                (&mut counted, served),
            );
            // A set's move may still be on its way: read again until the
            // deadline.
            if ctx.time().now() + Duration::from_millis(500) >= deadline {
                let lost = crate::machine::cell_lost(ctx.state());
                match why {
                    Unsettled::Unbooked => assert_always!(
                        lost,
                        "sets: every tenant's set in force is its booked machines after chaos",
                        {
                            "tenant" => tenant.0,
                            "in_force" => format!("{:?}", previous.as_ref().map(|p| &p.2)),
                            "slots" => format!("{:?}", previous.as_ref().map(|p| &p.0))
                        }
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

/// What a pending tenant operation's check reads of the fleet tenant's set
/// moves: every matchmaker slot booked, and the set in force of the fleet
/// tenant's control journal (empty when no initial member answers). A
/// tenant operation reads and writes that journal, and the journal elects
/// no leader while its set's handover is on its way. `None` when the
/// registry cannot be read.
pub(super) async fn fleet_set(known: &Cell, first: usize) -> Option<Seen> {
    let fleet = known.fleet;
    let registry = registry(known, first).await?;
    let (generation, members) = in_force(&known.client, known.servers.len(), fleet)
        .await
        .unwrap_or_default();
    let seen: Seen = (
        matchmaker_slots(&registry),
        fleet.tenant,
        members,
        generation,
    );
    assert!(
        seen.1 == known.fleet.tenant,
        "the fleet tenant's set is read"
    );
    Some(seen)
}

/// The set in force of `journal` and its generation, read as the coordinator reads it (#685
/// (special tenants are tenants)): on the machines of the journal's acceptor
/// slots and of its tenant's set slots, or, before any is booked, on the
/// machines the cell was formed on `formed` (through `client`). Hunt seed
/// 2510730384370761556 on bf0c783: a lone draining machine still believed
/// the generation before the handover.
async fn read_set(
    registry: &Registry,
    (client, formed): (&ChainClient, &[(u64, Address)]),
    connect: &impl Fn(&[(u64, Address)]) -> ChainClient,
    journal: JournalIdentifier,
) -> Option<(u64, Vec<u64>)> {
    let mut book: Vec<(u64, Address)> = Vec::new();
    let holders = paros::machine::control_members(registry, journal)
        .into_iter()
        .chain(set_slots(registry, journal.tenant));
    for node in holders {
        if let Some(addr) = registry.address(node).and_then(|a| Address::parse(a).ok())
            && !book.iter().any(|(id, _)| *id == node.0)
        {
            book.push((node.0, addr));
        }
    }
    if book.is_empty() {
        return in_force(client, formed.len(), journal).await;
    }
    in_force(&connect(&book), book.len(), journal).await
}

/// The machines that hold a slot of `tenant`'s matchmaker set.
fn set_slots(registry: &Registry, tenant: TenantId) -> Vec<NodeId> {
    let target = paros::machine::set_of(tenant);
    registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Matchmaker && b.target == target)
        .map(|(_, b)| b.node)
        .collect()
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

/// The initial members of `known` as the coordinator dials them: at the address
/// `registry` holds, every initial member, not only the ones this operator
/// reached at the tail's start. Only the initial member that drove a handover
/// holds its generation until the others campaign again: a belief is not a
/// fact. Seed 461662224030609195 on 522c917: the operator reached two initial
/// members that still believed generation 0.
///
/// An initial member the registry does not hold yet is dialed at the address
/// the cell's plan names, as the coordinator dials it
/// (`paros::machine::address_book`). Seed 13756221012129098124 on e6dd7f4: the
/// one initial member that drove a handover was not registered, and the check
/// read only the two initial members that still believed the generation before
/// it.
fn dialed_initial_members(
    ctx: &SimContext,
    known: &Cell,
    registry: &Registry,
) -> Vec<(u64, Address)> {
    known
        .members
        .iter()
        .filter_map(|id| {
            registry
                .address(NodeId(*id))
                .and_then(|addr| Address::parse(addr).ok())
                .or_else(|| crate::machine::planned_address(ctx.state(), known.journals, *id))
                .map(|addr| (*id, addr))
        })
        .collect()
}

/// The cell's registry, read to its tail.
pub(super) async fn registry(known: &Cell, first: usize) -> Option<Registry> {
    let mut cell = judged_cell(&known.members);
    let loaded =
        paros::client::checkpoint::load(&mut cell, known.journals.cell, &known.client, first, 0)
            .await;
    matches!(loaded, LoadOutcome::Loaded { .. }).then(|| cell.state().registry().clone())
}

/// The latest generation the servers believe for `journal`'s tenant and
/// its members, read through `journal`, or `None` when none answers.
pub(super) async fn in_force(
    client: &ChainClient,
    count: usize,
    journal: JournalIdentifier,
) -> Option<(u64, Vec<u64>)> {
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
    best.map(|(generation, mut members)| {
        members.sort_unstable();
        (generation, members)
    })
}

/// Why `tenant`'s set, whose set in force is `members`, has not settled:
/// `None` when it did, or when it cannot (no slot booked yet, a machine of
/// it wiped). `formed` are the machines the cell was formed on.
fn unsettled(
    registry: &Registry,
    tenant: TenantId,
    members: &[u64],
    (formed, wiped): (&[u64], &BTreeSet<u64>),
) -> Option<Unsettled> {
    let target = paros::machine::set_of(tenant);
    let slots: Vec<NodeId> = registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Matchmaker && b.target == target)
        .map(|(_, b)| b.node)
        .collect();
    // A set the cell was formed with holds no slot until it is placed, and
    // a cell may have no room for it.
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
        // A machine the set can go to: registered (`registrations > 0`,
        // never an initial member that has not registered yet, #685
        // (special tenants are tenants)), storing, serving, up and free.
        let free = registry
            .nodes()
            .filter(|(node, n)| {
                n.standing == NodeStanding::Registered
                    && n.registrations > 0
                    && n.class.stores()
                    && registry.serves(*node)
                    && registry.liveness(*node).up
                    && !slots.contains(node)
            })
            .count();
        return (free >= leaving).then_some(Unsettled::Unmoved);
    }
    let mut booked: Vec<u64> = slots.iter().map(|n| n.0).collect();
    booked.sort_unstable();
    booked.dedup();
    if booked != members {
        return Some(Unsettled::Unbooked);
    }
    if members.iter().any(|m| !formed.contains(m)) {
        assert_reachable!("sets: a set in force runs on an admitted machine");
    }
    None
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use std::collections::BTreeSet;

    use paros::core::NodeId;
    use paros::core::journal::TenantId;
    use paros::model::machine::{Class, Locality};
    use paros::model::registry::{Registry, RegistryEvent, Role, SystemCommand};

    use super::{Seen, Unsettled, advance, extended, unsettled};

    const TENANT: TenantId = TenantId(9);

    /// A registry of initial member 1 and admitted storage machines 2 to 5,
    /// with the tenant's set booked on `slots`.
    fn registry(slots: &[u64]) -> Registry {
        let mut registry = Registry::new([NodeId(1)]);
        let mut seq = 0;
        let mut fold = |registry: &mut Registry, command: SystemCommand| {
            let event = registry.fold(seq, &command.encode());
            seq += 1;
            assert!(!matches!(event, RegistryEvent::Refused(_)), "{event:?}");
        };
        for id in 2..=5 {
            fold(
                &mut registry,
                SystemCommand::RegisterNode {
                    id: NodeId(id),
                    addr: format!("n{id}"),
                    class: Class::Storage,
                    locality: Locality::default(),
                    name: format!("m{id}"),
                    incarnation: u128::from(id),
                    machine_interface: Vec::new(),
                },
            );
            fold(
                &mut registry,
                SystemCommand::MachineAdmitted { id: NodeId(id) },
            );
        }
        for (booking, node) in (1..).zip(slots) {
            fold(
                &mut registry,
                SystemCommand::BookCapacity {
                    booking,
                    node: NodeId(*node),
                    role: Role::Matchmaker,
                    target: paros::machine::set_of(TENANT),
                },
            );
        }
        registry
    }

    fn drain(mut registry: Registry, id: u64, seq: u64) -> Registry {
        registry.fold(seq, &SystemCommand::DrainNode { id: NodeId(id) }.encode());
        registry
    }

    fn judge(registry: &Registry, members: &[u64], wiped: &[u64]) -> Option<Unsettled> {
        let wiped: BTreeSet<u64> = wiped.iter().copied().collect();
        unsettled(registry, TENANT, members, (&[1], &wiped))
    }

    #[test]
    fn a_set_with_no_slot_or_no_members_is_not_judged() {
        assert_eq!(judge(&registry(&[]), &[1], &[]), None);
        assert_eq!(judge(&registry(&[2, 3, 4]), &[], &[]), None);
    }

    #[test]
    fn a_set_in_force_on_its_booked_machines_is_settled() {
        let registry = registry(&[2, 3, 4]);
        assert_eq!(judge(&registry, &[2, 3, 4], &[]), None);
        assert_eq!(judge(&registry, &[1, 2, 3], &[]), Some(Unsettled::Unbooked));
        // Every set lives on its slots alone (#685 (no founders)).
        assert_eq!(
            judge(&registry, &[1, 2, 3, 4], &[]),
            Some(Unsettled::Unbooked)
        );
    }

    #[test]
    fn a_set_that_names_a_wiped_machine_is_not_judged() {
        let registry = registry(&[2, 3, 4]);
        assert_eq!(judge(&registry, &[1, 2, 3], &[1]), None);
        assert_eq!(judge(&registry, &[2, 3, 5], &[4]), None);
    }

    #[test]
    fn a_slot_on_a_draining_machine_is_unmoved_while_a_machine_is_free() {
        let registry = drain(registry(&[2, 3, 4]), 4, 100);
        assert_eq!(judge(&registry, &[2, 3, 4], &[]), Some(Unsettled::Unmoved));
        // With every free machine draining too, the set cannot move.
        let registry = drain(registry, 5, 101);
        assert_eq!(judge(&registry, &[2, 3, 4], &[]), None);
    }

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn a_fleet_set_handed_over_gives_a_pending_operation_the_window_again() {
        // The fleet tenant's set, booked on 5 and then in force there: the
        // handover finishing is a move on its way, as a booking is.
        let fleet = TenantId(3);
        let booked: Seen = (vec![(1, 3), (5, 3)], fleet, vec![1], 0);
        let handed: Seen = (vec![(1, 3), (5, 3)], fleet, vec![1, 5], 1);
        let (deadline, ceiling) = (secs(86), secs(120));
        assert_eq!(
            extended(deadline, ceiling, secs(78), (Some(&booked), &handed)),
            secs(98)
        );
        // An operation whose fleet set stays put keeps its deadline.
        assert_eq!(
            extended(
                deadline,
                ceiling,
                secs(80),
                (Some(&handed), &handed.clone())
            ),
            deadline
        );
    }

    #[test]
    fn a_set_move_on_its_way_gets_the_settle_window_again_up_to_the_ceiling() {
        let tenant = TenantId(9);
        let before: Seen = (vec![(1, 9), (3, 9), (4, 9)], tenant, vec![1, 2, 3, 4], 2);
        let booked: Seen = (
            vec![(1, 9), (3, 9), (4, 9), (5, 8)],
            tenant,
            vec![1, 2, 3, 4],
            2,
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

    /// A served term that replaces the one the check counts from gets the
    /// settle window again, with no move seen: its successor re-asks every
    /// pending set's handover before it releases a slot (witness
    /// replay-main 5742660267466840157 on 860d6c7).
    #[test]
    fn a_new_term_restarts_the_window_with_no_move_seen() {
        let tenant = TenantId(9);
        let stuck: Seen = (
            vec![(1, 9), (2, 9), (3, 9), (4, 9)],
            tenant,
            vec![1, 2, 3],
            1,
        );
        let (deadline, ceiling) = (secs(20), secs(60));
        // The first read counts from the term it sees.
        let mut previous = None;
        let mut counted = None;
        assert_eq!(
            advance(
                deadline,
                ceiling,
                secs(1),
                (&mut previous, stuck.clone()),
                (&mut counted, Some(3))
            ),
            deadline
        );
        assert_eq!(counted, Some(3));
        // The same term, the same reads: the deadline holds.
        assert_eq!(
            advance(
                deadline,
                ceiling,
                secs(12),
                (&mut previous, stuck.clone()),
                (&mut counted, Some(3))
            ),
            deadline
        );
        // No served term read (an election journal read that missed): the
        // deadline holds, and the counted term stays.
        assert_eq!(
            advance(
                deadline,
                ceiling,
                secs(12),
                (&mut previous, stuck.clone()),
                (&mut counted, None)
            ),
            deadline
        );
        assert_eq!(counted, Some(3));
        // A term that replaces it: the window starts again from now.
        assert_eq!(
            advance(
                deadline,
                ceiling,
                secs(12),
                (&mut previous, stuck.clone()),
                (&mut counted, Some(4))
            ),
            secs(32)
        );
        assert_eq!(counted, Some(4));
        // The check counts from the term the election check judged
        // settled: a first read of another term restarts the window too.
        let mut counted = Some(3);
        assert_eq!(
            advance(
                deadline,
                ceiling,
                secs(5),
                (&mut None, stuck.clone()),
                (&mut counted, Some(4))
            ),
            secs(25)
        );
        // Never past the ceiling.
        assert_eq!(
            advance(
                secs(50),
                ceiling,
                secs(55),
                (&mut previous, stuck),
                (&mut counted, Some(5))
            ),
            ceiling
        );
    }
}
