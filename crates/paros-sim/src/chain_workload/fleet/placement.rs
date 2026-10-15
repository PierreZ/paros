//! Placement's oracles (#212 (placement)), over the final folds: the
//! coordinator books an acceptor slot on every member of a journal before it
//! writes the create, and releases them only after the journal's tombstone,
//! so every live journal of every tenant the cell hosts holds one live
//! acceptor booking per member in the cell's registry. The registry itself
//! judges each booking at apply (the class, the capacity, an id booked once),
//! so a placement never exceeds the capacity it was granted.
//!
//! A journal moves off a draining machine (#474 (journals on admitted
//! machines)): its new slot is booked before its old one is released, so a
//! live journal always holds at least as many slots as its create named
//! members, and only a member that left the registered machines lost its
//! slot. After chaos, every journal that has a machine to go to holds no
//! slot on a draining machine: its move finished.
//!
//! A machine down past `machine_replace_after` is gone (#475 (re-place
//! after the Down bound)): its slots move as a draining machine's do, so a
//! member the registry may have marked down is excused from holding its
//! slot. While a maintenance window an operator opened holds it, a down
//! machine keeps every slot it held (#359). A machine an operator removed
//! is retired and holds no slot (#359).

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_always, assert_reachable, assert_sometimes};
use paros::client::checkpoint::LoadOutcome;
use paros::client::journals;
use paros::machine::Class;
use paros::system::{BookingTarget, NodeStanding, Registry, Role};
use paros::tenant::TenantControl;
use paros::{JournalIdentifier, NodeId, TenantId};

use super::cell::Cell;
use super::judged_cell;
use crate::audit::liveness::JudgedRegistry;

/// Read every hosted tenant's control journal, then the cell's registry,
/// and hold each live journal's members against the registry's bookings.
/// Gives up quietly at `deadline`: the folds' liveness is
/// `final_fleet`'s. The operator learns each tenant control journal from
/// the cell's `HostTenant` (§3.8) and tells `learn` before it reads it.
pub(super) async fn placements_booked(
    ctx: &SimContext,
    known: &Cell,
    deadline: Duration,
    learn: impl Fn(JournalIdentifier),
) {
    let mut attempt = 0_u64;
    let mut judged = false;
    let wiped = crate::machine::wiped_ids(ctx.state());
    while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
        let first = known.first(attempt);
        attempt += 1;
        if let Some((tenants, registry)) = read(known, first, &learn).await {
            if !judged {
                judge(&tenants, &registry);
                maintained(ctx, &tenants, registry.registry());
                removed(ctx, registry.registry());
                judged = true;
            }
            let stuck = unmoved(&tenants, registry.registry(), &wiped);
            if stuck.is_none() {
                return;
            }
            // A move may still be on its way: read again until the deadline.
            if ctx.time().now() + Duration::from_millis(500) >= deadline {
                let (journal, node) = stuck.unwrap_or_default();
                assert_always!(
                    crate::machine::cell_lost(ctx.state()),
                    "placement: a journal with a machine to go to leaves a draining one after chaos",
                    { "tenant" => journal, "node" => node }
                );
                return;
            }
        }
        if ctx.time().sleep(Duration::from_millis(250)).await.is_err() {
            return;
        }
    }
}

/// The hosted tenants' control journals, read before the registry: a
/// journal created before the first read had its slots booked before it.
async fn read(
    known: &Cell,
    first: usize,
    learn: &impl Fn(JournalIdentifier),
) -> Option<(Vec<TenantControl>, JudgedRegistry)> {
    let mut cell = judged_cell(&known.members);
    let loaded =
        paros::client::checkpoint::load(&mut cell, known.journals.cell, &known.client, first, 0)
            .await;
    if !matches!(loaded, LoadOutcome::Loaded { .. }) {
        return None;
    }
    let hosted: Vec<(TenantId, paros::JournalId)> = cell
        .state()
        .registry()
        .hosted()
        .filter_map(|tenant| {
            cell.state()
                .registry()
                .hosted_tenant(tenant)
                .map(|h| (tenant, h.control))
        })
        .collect();
    let mut tenants = Vec::with_capacity(hosted.len());
    for (tenant, control) in hosted {
        learn(JournalIdentifier::new(tenant, control));
        tenants.push(journals::list(&known.client, tenant, control).await?);
    }
    let mut after = judged_cell(&known.members);
    let loaded =
        paros::client::checkpoint::load(&mut after, known.journals.cell, &known.client, first, 0)
            .await;
    if !matches!(loaded, LoadOutcome::Loaded { .. }) {
        return None;
    }
    Some((tenants, after.state().clone()))
}

/// The oracle itself, and the gate that a placement spread over zones.
fn judge(tenants: &[TenantControl], judged: &JudgedRegistry) {
    let registry = judged.registry();
    // Placement books only a machine that serves the cell: a founding
    // member, or one whose admission the coordinator saw (#511 (placement
    // books machines that never served)). A slot on a machine that never
    // won its `Admit` would never start.
    for (_, booking) in registry.bookings() {
        assert_always!(
            registry.serves(booking.node),
            "placement: every slot is booked on a machine that serves the cell",
            { "node" => booking.node.0 }
        );
    }
    for control in tenants {
        let tenant = control.tenant();
        if registry.dropped(tenant) {
            continue;
        }
        for (id, journal) in control.live() {
            let target = BookingTarget::Journal(JournalIdentifier::new(tenant, id));
            let members = journal.config.members();
            let slots: Vec<NodeId> = registry
                .bookings()
                .filter(|(_, b)| b.role == Role::Acceptor && b.target == target)
                .map(|(_, b)| b.node)
                .collect();
            let registered = |node: NodeId| {
                registry
                    .get(node)
                    .is_some_and(|n| n.standing == NodeStanding::Registered)
            };
            // Only a member that left the registered machines, or one down
            // past the re-placement bound (#475), moved off.
            let lost = members
                .iter()
                .filter(|member| {
                    registered(**member) && !slots.contains(member) && !judged.maybe_down(**member)
                })
                .count();
            assert_always!(
                lost == 0,
                "placement: every live journal's members hold its slots after chaos",
                { "journal" => target.tenant().0, "members" => members.len(), "lost" => lost }
            );
            assert_always!(
                slots.len() >= members.len(),
                "placement: a moved journal holds as many slots as its create named",
                { "journal" => target.tenant().0, "members" => members.len(), "slots" => slots.len() }
            );
            assert_reachable!("placement: a live journal's slots are judged after chaos");
            if members.iter().any(|member| !slots.contains(member)) {
                assert_reachable!("placement: a journal moved off a draining machine");
            }
            let zones: BTreeSet<&str> = members
                .iter()
                .filter_map(|m| registry.get(*m).map(|n| n.failure_domain.as_str()))
                .collect();
            if members.len() > 1 {
                assert_sometimes!(
                    zones.len() > 1,
                    "placement: a journal's members span two zones"
                );
            }
        }
    }
}

/// A live journal that still holds a slot on a draining machine while it
/// has a machine to go to for each such slot, as `(tenant, node)`: a
/// registered storage machine that serves the cell, never wiped, up, with a free slot, that
/// holds no slot of it,
/// while every machine it stays on was never wiped. `None` when every such
/// move finished.
fn unmoved(
    tenants: &[TenantControl],
    registry: &Registry,
    wiped: &BTreeSet<u64>,
) -> Option<(u64, u64)> {
    let registered = |node: NodeId| {
        registry
            .get(node)
            .is_some_and(|n| n.standing == NodeStanding::Registered)
    };
    for control in tenants {
        let tenant = control.tenant();
        if registry.dropped(tenant) {
            continue;
        }
        for (id, _) in control.live() {
            let target = BookingTarget::Journal(JournalIdentifier::new(tenant, id));
            let slots: Vec<NodeId> = registry
                .bookings()
                .filter(|(_, b)| b.role == Role::Acceptor && b.target == target)
                .map(|(_, b)| b.node)
                .collect();
            let leaving: Vec<NodeId> = slots
                .iter()
                .filter(|node| !registered(**node))
                .copied()
                .collect();
            let Some(first) = leaving.first() else {
                continue;
            };
            let staying_alive = slots
                .iter()
                .filter(|node| registered(**node))
                .all(|node| !wiped.contains(&node.0));
            // A move books every slot it lacks at once: a journal with two
            // slots on draining machines needs two machines to go to.
            let free = registry.nodes().filter(|(node, n)| {
                n.standing == NodeStanding::Registered
                    && n.class == Class::Storage
                    && registry.serves(*node)
                    && !wiped.contains(&node.0)
                    && registry.liveness(*node).up
                    && registry.available(*node) > 0
                    && !slots.contains(node)
            });
            if staying_alive && free.count() >= leaving.len() {
                return Some((tenant.0, first.0));
            }
        }
    }
    None
}

/// Whether the tenant's journal or set `target` is live in `tenants`.
fn live(tenants: &[TenantControl], registry: &Registry, target: BookingTarget) -> bool {
    if registry.dropped(target.tenant()) {
        return false;
    }
    match target {
        BookingTarget::Journal(journal) => tenants
            .iter()
            .filter(|c| c.tenant() == journal.tenant)
            .any(|c| c.live().any(|(id, _)| id == journal.journal)),
        BookingTarget::Set { .. } => true,
    }
}

/// While a maintenance window an operator opened still holds (#359), every
/// slot its machine held when the operator sent it is still booked, unless
/// the machine left the registered machines or the slot's journal is gone.
fn maintained(ctx: &SimContext, tenants: &[TenantControl], registry: &Registry) {
    let now = ctx.time().now();
    for note in crate::machine::maintenance_notes(ctx.state()) {
        if now >= note.sent + note.length {
            continue;
        }
        let node = NodeId(note.node);
        if registry
            .get(node)
            .is_none_or(|n| n.standing != NodeStanding::Registered)
        {
            continue;
        }
        assert_reachable!("placement: a maintenance window is judged after chaos");
        let booked: BTreeSet<u64> = registry
            .bookings()
            .filter(|(_, b)| b.node == node)
            .map(|(id, _)| id)
            .collect();
        // A slot released with its journal is the journal's, not a move's.
        let moved = note
            .slots
            .iter()
            .filter(|(id, target)| !booked.contains(id) && live(tenants, registry, *target))
            .count();
        assert_always!(
            moved == 0,
            "placement: a maintenance window keeps its machine's slots",
            { "node" => note.node, "moved" => moved }
        );
    }
}

/// Every machine an operator removed (#359) is retired and holds no slot.
fn removed(ctx: &SimContext, registry: &Registry) {
    for id in crate::machine::removed_ids(ctx.state()) {
        let node = NodeId(id);
        let retired = registry
            .get(node)
            .is_some_and(|n| n.standing == NodeStanding::Retired);
        assert_always!(
            retired && registry.booked(node) == 0,
            "placement: a removed machine is retired and holds no slot",
            { "node" => id, "booked" => registry.booked(node) }
        );
        assert_reachable!("placement: a removed machine is judged after chaos");
    }
}
