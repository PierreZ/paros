//! The tenant coordinator's role counts (#604, `docs/architecture/03-4-tenant-modes.md`
//! §3.4): a tenant's `frontends=` count, recorded by a `Configure` of its
//! default, becomes that many `frontend` slots of the tenant
//! (`BookingTarget::Tenant`) on the cell's `stateless` machines; and each
//! journal's scale counts (#753 (per-journal proxies)), recorded in its
//! desired mode, become that many slots of the journal
//! (`BookingTarget::Journal`): `proxies=` proxy leaders and `batchers=`
//! batchers (a multi-writer journal only) on `stateless` machines, and
//! `replicas=` replicas on `storage` machines that hold none of the
//! journal's acceptors.
//!
//! One step per watch round, after the footprint step, in turn over the
//! hosted tenants, and for the tenant in turn over its frontends and then
//! its journals' counts: release one slot on a machine that leaves
//! (draining, held slow, or gone, `crate::tenant::moves::stays`), else book
//! or release the slots that make the count, with `paros_model::placement`
//! (one slot per machine, zones in round robin, least-loaded first). The
//! count is capped at the machines that can take a slot, so a cell without
//! one places none and a cell that grows places the rest later. Every step
//! starts from the registry and the tenant control journal, so a later term
//! finishes what an earlier one started.
//!
//! A journal's proxy leader or batcher slot weighs nothing
//! (`Booking::weighs`): it counts for the spread, and idle journals never
//! fill the cell. A slot is placement only. Every machine serves the batcher
//! endpoint (`paros_roles::batcher`) and the frontend
//! (`paros_roles::frontend`, #723 (frontend as a parosd role)); `Resolve`
//! names the machines the registry books a tenant's frontends and its
//! journals' batchers on. The journal's leader runs the proxy and replica
//! counts of its configuration in force (`crate::tenant::modes` applies the
//! booked counts as a round change, #754 (counts off the format marker)), and
//! runs a round colocated when its machine reaches no proxy leader of that
//! rank.

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use paros_core::NodeId;
use paros_core::journal::state::WriterMode;
use paros_core::journal::{JournalIdentifier, TenantId};
use paros_net::runtime::ParosProviders;

use crate::tenant::TenantDesk;
use crate::tenant::moves::{Leaving, stays};
use paros_client::Client;
use paros_client::cell::CellSession;
use paros_client::journals::JournalAnswer;
use paros_model::machine::Class;
use paros_model::placement::{self, Step, Want};
use paros_model::registry::{Booking, BookingTarget, NodeStanding, Registry, Role, SystemCommand};
use paros_model::tenant::TenantJournal;

/// What one role step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RoleStep {
    /// No hosted tenant, or the tenant in turn holds its counts.
    Idle,
    /// The tenant's control journal could not be read.
    Waiting,
    /// Slots were booked.
    Booked,
    /// A slot was released.
    Released,
}

/// The role steps of one coordinator term.
#[derive(Debug, Default)]
pub(crate) struct Roles {
    /// The tenant the last step took: the next step takes the next one.
    last: Option<TenantId>,
}

/// The roles a journal's counts place, in the order a step takes them.
const JOURNAL_ROLES: [Role; 3] = [Role::ProxyLeader, Role::Batcher, Role::Replica];

/// The live slots of `role` for `target`.
fn slots(registry: &Registry, target: BookingTarget, role: Role) -> Vec<(u64, Booking)> {
    registry
        .bookings()
        .filter(|(_, b)| b.role == role && b.target == target)
        .map(|(id, b)| (id, *b))
        .collect()
}

/// The machines of `class` a counted slot may go to: registered, serving
/// the cell, and staying.
fn eligible(registry: &Registry, leaving: &Leaving, class: Class) -> Vec<NodeId> {
    let nodes: Vec<NodeId> = registry
        .nodes()
        .filter(|(id, n)| {
            n.class == class
                && n.standing == NodeStanding::Registered
                && registry.serves(*id)
                && stays(registry, leaving, *id)
        })
        .map(|(id, _)| id)
        .collect();
    assert!(
        nodes
            .iter()
            .all(|id| registry.get(*id).is_some_and(|n| n.class == class)),
        "a counted role goes only to a machine of its class"
    );
    nodes
}

/// How many slots of `role` a journal under its desired mode asks for: a
/// single-writer journal has no batcher (`docs/architecture/02-data-plane.md`
/// §2.4).
pub(crate) fn journal_count(journal: &TenantJournal, role: Role) -> u32 {
    assert!(
        JOURNAL_ROLES.contains(&role),
        "a journal counts its own roles"
    );
    match role {
        Role::ProxyLeader => journal.desired.proxies,
        Role::Batcher if journal.writer == WriterMode::Multi => journal.desired.batchers,
        Role::Replica => journal.desired.replicas,
        _ => 0,
    }
}

impl Roles {
    /// Take one step of the next hosted tenant's role counts, through the
    /// term's session `cell` (its bookings drawn by `desk`, which holds the
    /// tenant control journals). `None` when a write did not land: the term
    /// writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn step<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        leaving: &Leaving,
    ) -> Option<RoleStep> {
        let registry = cell.registry();
        let hosted: BTreeSet<TenantId> = registry
            .hosted()
            .filter(|tenant| !registry.dropped(*tenant))
            .collect();
        let Some(tenant) = self
            .last
            .and_then(|last| hosted.range((Excluded(last), Unbounded)).next())
            .or_else(|| hosted.first())
            .copied()
        else {
            return Some(RoleStep::Idle);
        };
        self.last = Some(tenant);
        let Some(control) = registry.hosted_tenant(tenant).map(|h| h.control) else {
            return Some(RoleStep::Waiting);
        };
        // The tenant's frontends, then its journals' counts, as the tenant
        // control journal records them now.
        let mut counts: Vec<(Role, BookingTarget, u32)> = Vec::new();
        match desk.tenant(client, tenant, control).await {
            Ok(state) => {
                counts.push((
                    Role::Frontend,
                    BookingTarget::Tenant { tenant },
                    state.roles().frontends,
                ));
                for (id, journal) in state.live() {
                    let target = BookingTarget::Journal(JournalIdentifier::new(tenant, id));
                    // A journal with no acceptor slot was never placed by
                    // this cell: it takes no other role either.
                    if !registry
                        .bookings()
                        .any(|(_, b)| b.role == Role::Acceptor && b.target == target)
                    {
                        continue;
                    }
                    for role in JOURNAL_ROLES {
                        counts.push((role, target, journal_count(journal, role)));
                    }
                }
            }
            Err(JournalAnswer::NotCoordinator) => return None,
            Err(_) => return Some(RoleStep::Waiting),
        }
        for (role, target, wanted) in counts {
            match role_step(client, cell, desk, leaving, (role, target), wanted).await? {
                RoleStep::Idle => {}
                step => return Some(step),
            }
        }
        Some(RoleStep::Idle)
    }
}

/// Release one of the `role` slots of `target` on a machine that leaves,
/// or, for a storage role, on a machine that holds another storage slot of
/// the journal (placement's rule 2: a move may place an acceptor on a
/// replica's machine): `Some(None)` when the write did not land, `None`
/// when no slot leaves.
async fn release_leaving<P: ParosProviders>(
    client: &Client<P>,
    cell: &mut CellSession,
    leaving: &Leaving,
    (role, target): (Role, BookingTarget),
) -> Option<Option<RoleStep>> {
    let registry = cell.registry();
    let shared = |node: NodeId| {
        role.class() == Class::Storage
            && registry.bookings().any(|(_, b)| {
                b.target == target
                    && b.role != role
                    && b.class() == Class::Storage
                    && b.node == node
            })
    };
    let (booking, slot) = slots(registry, target, role)
        .into_iter()
        .find(|(_, b)| !stays(registry, leaving, b.node) || shared(b.node))?;
    if stays(registry, leaving, slot.node) {
        moonpool_assertions::reachable!(
            "coordinator: a replica leaves a machine another storage slot of its journal takes"
        );
    }
    assert!(
        registry.booking(booking).is_some_and(|b| b.role == role),
        "a released slot is one of the role's"
    );
    let command = SystemCommand::ReleaseCapacity { booking };
    if cell.record(client, 0, &command).await.is_err() {
        return Some(None);
    }
    match role {
        Role::Frontend => {
            moonpool_assertions::reachable!("coordinator: a frontend leaves a machine that leaves");
        }
        Role::Replica => {
            moonpool_assertions::reachable!("coordinator: a replica leaves a machine that leaves");
        }
        _ => moonpool_assertions::reachable!(
            "coordinator: a journal's stateless role leaves a machine that leaves"
        ),
    }
    Some(Some(RoleStep::Released))
}

/// One step of the count of `role` for `target`: `Idle` when it holds its
/// count of `wanted`, capped at the machines that can take a slot.
#[tracing::instrument(level = "debug", skip_all, fields(tenant = target.tenant().0, role = role.as_str()))]
async fn role_step<P: ParosProviders>(
    client: &Client<P>,
    cell: &mut CellSession,
    desk: &mut TenantDesk,
    leaving: &Leaving,
    (role, target): (Role, BookingTarget),
    wanted: u32,
) -> Option<RoleStep> {
    // A slot on a machine that leaves goes first: the count is then made
    // on the machines that stay.
    if let Some(step) = release_leaving(client, cell, leaving, (role, target)).await {
        return step;
    }
    let registry = cell.registry();
    let held = slots(registry, target, role);
    let nodes = eligible(registry, leaving, role.class());
    let machines = placement::machines(registry, nodes.iter().copied());
    let current: Vec<(u64, Booking)> = registry
        .bookings()
        .filter(|(_, b)| b.target == target)
        .map(|(id, b)| (id, *b))
        .collect();
    let holding: BTreeSet<NodeId> = held.iter().map(|(_, b)| b.node).collect();
    // A storage slot never shares a machine with another storage instance
    // of the journal (placement's rule 2).
    let taken: BTreeSet<NodeId> = current
        .iter()
        .filter(|(_, b)| role.class() == Class::Storage && b.class() == Class::Storage)
        .filter(|(_, b)| b.role != role)
        .map(|(_, b)| b.node)
        .collect();
    let weighs = Booking::weighs_for(role, target);
    // A machine with a slot of the role, or one that can take one.
    let room = machines
        .iter()
        .filter(|m| {
            holding.contains(&m.node) || (!taken.contains(&m.node) && (!weighs || m.free() > 0))
        })
        .count();
    let wanted = usize::try_from(wanted).unwrap_or(usize::MAX);
    let count = wanted.min(room);
    if count < wanted {
        match role {
            Role::Frontend => moonpool_assertions::reachable!(
                "coordinator: a frontend count is capped at the machines"
            ),
            _ => moonpool_assertions::reachable!(
                "coordinator: a journal's role count is capped at the machines"
            ),
        }
    }
    let want = Want {
        role,
        target,
        count,
    };
    let Ok(steps) = placement::plan(&want, &machines, &current, target.tenant().0) else {
        return Some(RoleStep::Waiting);
    };
    if steps.is_empty() {
        if count > 0 {
            match role {
                Role::Frontend => {
                    moonpool_assertions::reachable!("coordinator: a tenant holds its frontends");
                }
                _ => {
                    moonpool_assertions::reachable!("coordinator: a journal holds its role counts");
                }
            }
        }
        return Some(RoleStep::Idle);
    }
    if let Some(Step::Release { booking, .. }) =
        steps.iter().find(|s| matches!(s, Step::Release { .. }))
    {
        let command = SystemCommand::ReleaseCapacity { booking: *booking };
        if cell.record(client, 0, &command).await.is_err() {
            return None;
        }
        match role {
            Role::Frontend => moonpool_assertions::reachable!(
                "coordinator: a frontend slot is released to its count"
            ),
            _ => moonpool_assertions::reachable!(
                "coordinator: a journal's role slot is released to its count"
            ),
        }
        return Some(RoleStep::Released);
    }
    desk.book(client, cell, &want, &steps).await.ok()?;
    match role {
        Role::Frontend => {
            moonpool_assertions::reachable!("coordinator: a tenant's frontends are booked");
        }
        Role::ProxyLeader => {
            moonpool_assertions::reachable!("coordinator: a journal's proxy leaders are booked");
        }
        Role::Batcher => {
            moonpool_assertions::reachable!("coordinator: a journal's batchers are booked");
        }
        _ => moonpool_assertions::reachable!("coordinator: a journal's replicas are booked"),
    }
    tracing::info!(
        tenant = target.tenant().0,
        role = role.as_str(),
        count,
        "role_booked"
    );
    Some(RoleStep::Booked)
}
