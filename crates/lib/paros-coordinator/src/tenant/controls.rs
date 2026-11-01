//! The tenant coordinator's control journals (#685 (special tenants are
//! tenants)): every hosted tenant's control journal is born on the founding
//! members, and the coordinator grows it to the tenant's own `2F+1` on the
//! cell's storage machines, `F` the largest `survive_machines` of the tenant
//! (its default and every live journal's, as its matchmaker set, #214
//! (tenant modes)), then moves it off a machine that leaves, one step per watch round. Every
//! step starts from what the registry and the journal's leader hold now, so
//! any later term finishes a change an earlier one started:
//!
//! 1. book the acceptor slots the journal lacks on registered storage
//!    machines outside the founders (`paros_model::placement::plan`, with
//!    the slots on machines that stay as the current ones); the machine
//!    starts the journal as a spare over the founders
//!    (`paros_roles::node::control`);
//! 2. ask the journal's leader to reconfigure onto the founders and the
//!    booked machines that stay (`Reconfigure`, a majority);
//! 3. once that configuration is in force and the leader's GC watermark has
//!    reached it, release every slot it left: one on a machine that leaves,
//!    or one past the size the mode wants now.
//!
//! The founding members stay members of every control journal, whatever
//! the mode: every reader of a tenant control journal (the machines'
//! follow, `parosctl`, the frontend) still dials them, and a founder is
//! never drained yet. The size is `max(founders, 2F+1)`, capped by the
//! machines that may take a slot. A user tenant follows its own mode
//! (Pierre, 2026-10-31); only the special tenants (the cell and universe
//! tenants) follow the control plane's mode
//! (`paros_model::registry::control_plane`). Only the
//! coordinator writes a control journal, and its term client reaches the
//! booked members by the leader hints their answers carry.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use paros_core::journal::JournalIdentifier;
use paros_core::membership::{AcceptorConfig, QuorumSystem};
use paros_net::runtime::ParosProviders;

use crate::tenant::TenantDesk;
use crate::tenant::moves::{Leaving, at_or_above, bookable, leader_of, stays};
use paros_client::cell::CellSession;
use paros_client::{Client, ReconfigureOutcome, Server};
use paros_model::address::{Address, Names};
use paros_model::placement::{self, Want};
use paros_model::registry::control_plane::acceptors;
use paros_model::registry::{Booking, BookingTarget, Registry, Role};
use paros_net::NodeClient;

/// What one control journal step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlStep {
    /// Every control journal is in its shape.
    Idle,
    /// A change waits: for a leader, a free machine, a configuration in
    /// force or the GC watermark.
    Waiting,
    /// The slots the journal lacks were booked.
    Booked,
    /// The leader started the reconfiguration.
    Reconfigured,
    /// The configuration in force is the target, and every slot it left was
    /// released.
    Finished,
}

impl ControlStep {
    /// Whether the step started or advanced a change that is not done.
    pub(crate) fn in_flight(self) -> bool {
        matches!(self, Self::Booked | Self::Reconfigured)
    }
}

/// The control journal changes of one coordinator term.
#[derive(Debug, Default)]
pub(crate) struct Controls {
    /// Each control journal judged settled, with the slots and the size it
    /// was settled at: it is judged again once either moves.
    settled: BTreeMap<JournalIdentifier, (Vec<u64>, usize)>,
    /// The journal the last step took: the next step takes the next one.
    last: Option<JournalIdentifier>,
}

/// The acceptor slots of `journal`.
fn slots(registry: &Registry, journal: JournalIdentifier) -> Vec<(u64, Booking)> {
    registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Acceptor && b.target == BookingTarget::Journal(journal))
        .map(|(id, b)| (id, *b))
        .collect()
}

/// The size a control journal of a tenant whose largest `F` is `failures`
/// wants, on a cell whose founding members are `founders` and whose
/// machines that may take a slot outside them are `room`: the founders, or
/// the tenant's `2F+1` when larger, capped by the machines it may run on.
fn size(failures: u32, founders: usize, room: usize) -> usize {
    let size = acceptors(failures).max(founders).min(founders + room);
    assert!(size >= founders, "a control journal keeps its founders");
    size
}

/// Every hosted tenant's control journal, in id order.
fn controls(registry: &Registry) -> BTreeSet<JournalIdentifier> {
    registry
        .hosted()
        .filter(|tenant| !registry.dropped(*tenant))
        .filter_map(|tenant| {
            registry
                .hosted_tenant(tenant)
                .map(|hosted| JournalIdentifier::new(tenant, hosted.control))
        })
        .collect()
}

/// The ids of `journal`'s acceptor slots, sorted.
fn held_ids(registry: &Registry, journal: JournalIdentifier) -> Vec<u64> {
    let mut held: Vec<u64> = slots(registry, journal).iter().map(|(id, _)| *id).collect();
    held.sort_unstable();
    held
}

/// A client of `members` of `cell`, each where the cell's address book
/// says it is. `None` when the book names none of them.
fn members_client<P: ParosProviders>(
    (rpc, names, client): (&RpcHandle<P>, &Names, &Client<P>),
    cell: &CellSession,
    members: &[NodeId],
) -> Option<(Client<P>, Vec<NodeId>)> {
    let book: BTreeMap<NodeId, Address> =
        paros_model::machine::cell_book(cell.founders(), cell.registry())
            .into_iter()
            .collect();
    let servers: Vec<(NodeId, Server<P>)> = members
        .iter()
        .filter_map(|node| {
            let addr = book.get(node)?;
            Some((
                *node,
                Server {
                    id: node.0,
                    node: NodeClient::named(rpc, names.clone(), addr.clone()),
                },
            ))
        })
        .collect();
    if servers.is_empty() {
        return None;
    }
    let ids: Vec<NodeId> = servers.iter().map(|(id, _)| *id).collect();
    let nodes = client.with_servers(servers.into_iter().map(|(_, s)| s).collect());
    assert_eq!(ids.len(), nodes.server_count(), "one id per server");
    Some((nodes, ids))
}

impl Controls {
    /// The next control journal after the last one, in id order and
    /// wrapping: one whose slots moved since it settled first, else any,
    /// since its tenant's mode may have changed since.
    fn next(&self, registry: &Registry) -> Option<JournalIdentifier> {
        let all = controls(registry);
        let pending: BTreeSet<JournalIdentifier> = all
            .iter()
            .filter(|journal| {
                self.settled
                    .get(*journal)
                    .is_none_or(|(held, _)| *held != held_ids(registry, **journal))
            })
            .copied()
            .collect();
        let turn = if pending.is_empty() { &all } else { &pending };
        self.last
            .and_then(|last| turn.range((Excluded(last), Unbounded)).next())
            .or_else(|| turn.first())
            .copied()
    }

    /// Take one step of the next control journal's change, through the
    /// term's session `cell` (its bookings drawn by `desk`). A slot on a
    /// machine `gone` (down past the re-placement bound, #475) leaves as a
    /// draining machine's does. `None` when a write did not land: the term
    /// writes nothing more.
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn step<P: ParosProviders>(
        &mut self,
        (_, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        gone: &Leaving,
    ) -> Option<ControlStep> {
        let registry = cell.registry();
        let founders: Vec<NodeId> = cell.founders().iter().map(|(id, _)| *id).collect();
        // The machines outside the founders a control journal may take a
        // slot on: storage, bookable, staying.
        let room: Vec<placement::Machine> = bookable(registry, desk, &gone.gone)
            .into_iter()
            .filter(|m| !founders.contains(&m.node) && stays(registry, gone, m.node))
            .collect();
        let Some(journal) = self.next(registry) else {
            return Some(ControlStep::Idle);
        };
        self.last = Some(journal);
        // The tenant's own mode (#685): its largest `F`.
        let failures = match desk.tenant(client, journal.tenant, journal.journal).await {
            Ok(state) => state.largest_failures(),
            Err(paros_client::journals::JournalAnswer::NotCoordinator) => return None,
            Err(_) => return Some(ControlStep::Waiting),
        };
        let registry = cell.registry();
        // The machines that hold a slot already count as room: a full
        // machine keeps the slot it holds.
        let kept_full = slots(registry, journal)
            .iter()
            .filter(|(_, b)| {
                stays(registry, gone, b.node)
                    && !founders.contains(&b.node)
                    && !room.iter().any(|m| m.node == b.node)
            })
            .count();
        let size = size(failures, founders.len(), room.len() + kept_full);
        let held = slots(registry, journal);
        let staying: Vec<(u64, Booking)> = held
            .iter()
            .filter(|(_, b)| stays(registry, gone, b.node) && !founders.contains(&b.node))
            .copied()
            .collect();
        let extras = size - founders.len();
        // 1. Book the slots the journal lacks.
        if staying.len() < extras {
            let want = Want {
                role: Role::Acceptor,
                target: BookingTarget::Journal(journal),
                count: extras,
            };
            let Ok(steps) = placement::plan(&want, &room, &staying, journal.tenant.0) else {
                moonpool_assertions::reachable!(
                    "coordinator: a control journal has no machine to grow onto"
                );
                return Some(ControlStep::Waiting);
            };
            desk.book(client, cell, &want, &steps).await.ok()?;
            moonpool_assertions::reachable!("coordinator: a control journal books a slot");
            moonpool_buggify::hint!("control journal booked, not reconfigured").await;
            return Some(ControlStep::Booked);
        }
        // The target: the founders and the first `extras` slots that stay.
        let mut kept: Vec<NodeId> = staying.iter().map(|(_, b)| b.node).collect();
        kept.sort_unstable();
        kept.truncate(extras);
        let mut target: Vec<NodeId> = founders.iter().copied().chain(kept).collect();
        target.sort_unstable();
        target.dedup();
        assert_eq!(target.len(), size, "a control journal's target is its size");
        // The leader's view: its configuration, its GC watermark. Asked of
        // every machine that serves the journal now or will.
        let mut asked: Vec<NodeId> = founders
            .iter()
            .copied()
            .chain(held.iter().map(|(_, b)| b.node))
            .collect();
        asked.sort_unstable();
        asked.dedup();
        let Some((nodes, ids)) = members_client((rpc, names, client), cell, &asked) else {
            return Some(ControlStep::Waiting);
        };
        let Some((at, reply)) = leader_of(&nodes, ids.len(), journal).await else {
            return Some(ControlStep::Waiting);
        };
        let mut members: Vec<NodeId> = reply.members.iter().copied().map(NodeId).collect();
        members.sort_unstable();
        // 2. Reconfigure onto the target.
        if members != target {
            let composed = AcceptorConfig::new(target.clone(), QuorumSystem::Majority);
            return Some(match nodes.reconfigure_to(journal, &composed, at).await {
                ReconfigureOutcome::Started { .. } => {
                    if target.len() > founders.len() {
                        moonpool_assertions::reachable!(
                            "coordinator: a control journal grows past the founders"
                        );
                    } else {
                        moonpool_assertions::reachable!(
                            "coordinator: a control journal shrinks to the founders"
                        );
                    }
                    ControlStep::Reconfigured
                }
                _ => ControlStep::Waiting,
            });
        }
        // 3. Release every slot the configuration in force left, once no
        // leader can ask its member again.
        let left: Vec<(u64, Booking)> = held
            .iter()
            .filter(|(_, b)| !target.contains(&b.node))
            .copied()
            .collect();
        if !left.is_empty() {
            if !at_or_above(reply.gc_watermark.as_ref(), reply.config_ballot.as_ref()) {
                return Some(ControlStep::Waiting);
            }
            for (booking, slot) in &left {
                crate::tenant::release(client, cell, |b| b == slot)
                    .await
                    .ok()?;
                assert!(
                    cell.registry().booking(*booking).is_none(),
                    "a control journal's old slot is released"
                );
            }
            moonpool_assertions::reachable!("coordinator: a control journal releases a slot");
        }
        let mut settled: Vec<u64> = slots(cell.registry(), journal)
            .iter()
            .map(|(id, _)| *id)
            .collect();
        settled.sort_unstable();
        assert!(
            target.iter().all(|node| founders.contains(node)
                || cell
                    .registry()
                    .bookings()
                    .any(|(_, b)| b.node == *node && b.target == BookingTarget::Journal(journal))),
            "every member a control journal settles on holds a slot or is a founder"
        );
        self.settled.insert(journal, (settled, size));
        if target.len() > founders.len() {
            moonpool_assertions::reachable!(
                "coordinator: a control journal runs past the founders"
            );
        }
        tracing::info!(journal = %journal, size, "control_journal_settled");
        Some(ControlStep::Finished)
    }
}

/// The machines outside `founders` that hold a slot of a hosted tenant's
/// control journal (#685), in id order: the term's client reaches them by
/// the leader hints their answers carry.
#[must_use]
pub(crate) fn extra_members(registry: &Registry, founders: &[NodeId]) -> BTreeSet<NodeId> {
    controls(registry)
        .into_iter()
        .flat_map(|journal| slots(registry, journal))
        .map(|(_, b)| b.node)
        .filter(|node| !founders.contains(node))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::moves::is_control;
    use paros_core::journal::{JournalId, TenantId};
    use paros_model::registry::{Class, GrantKey, HostedTenant, Slot, SystemCommand};

    const FLEET: u64 = 0xf1ee7;

    fn fold(registry: &mut Registry, command: &SystemCommand) {
        let seq = registry.next_seq();
        registry.fold(seq, &command.encode());
    }

    fn control(tenant: u64) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(tenant), JournalId(tenant + 1))
    }

    /// A cell of two founders and two more storage machines, hosting two
    /// tenants, 50 and 60, whose control journals are 51 and 61.
    fn registry() -> Registry {
        let mut registry = Registry::new([NodeId(1), NodeId(2)]);
        for id in [1, 2, 3, 4] {
            fold(
                &mut registry,
                &SystemCommand::RegisterNode {
                    id: NodeId(id),
                    addr: format!("m{id}.paros:1"),
                    class: Class::Storage,
                    locality: paros_model::machine::Locality::default(),
                    name: format!("m{id}"),
                    incarnation: 1,
                    machine_interface: Vec::new(),
                },
            );
        }
        fold(
            &mut registry,
            &SystemCommand::JoinFleet {
                fleet_id: FLEET,
                cell_id: 0xce11,
                version: paros_model::fleet::METADATA_VERSION,
                universe: None,
            },
        );
        for (tenant, name) in [(50, &b"acme"[..]), (60, b"globex")] {
            fold(
                &mut registry,
                &SystemCommand::GrantCapacity {
                    key: GrantKey::Footprint(TenantId(tenant)),
                    tenant: TenantId(tenant),
                    slots: Some(vec![Slot {
                        booking: tenant,
                        node: NodeId(3),
                        role: Role::Acceptor,
                        target: BookingTarget::Tenant {
                            tenant: TenantId(tenant),
                        },
                    }]),
                },
            );
            fold(
                &mut registry,
                &SystemCommand::HostTenant {
                    fleet_id: FLEET,
                    tenant: TenantId(tenant),
                    hosted: HostedTenant {
                        control: JournalId(tenant + 1),
                        name: name.to_vec(),
                        survives: paros_model::tenant::Survives::Az,
                    },
                },
            );
        }
        assert_eq!(controls(&registry).len(), 2, "both tenants are hosted");
        registry
    }

    fn book(registry: &mut Registry, booking: u64, node: u64, journal: JournalIdentifier) {
        fold(
            registry,
            &SystemCommand::BookCapacity {
                booking,
                node: NodeId(node),
                role: Role::Acceptor,
                target: BookingTarget::Journal(journal),
            },
        );
    }

    #[test]
    fn a_control_journal_wants_two_f_plus_one_with_its_founders_and_the_room_it_has() {
        assert_eq!(size(1, 2, 2), 3, "F = 1 grows two founders by one");
        assert_eq!(size(2, 2, 2), 4, "capped by the machines it may run on");
        assert_eq!(size(2, 2, 3), 5);
        assert_eq!(size(0, 2, 3), 2, "never below the founders");
        assert_eq!(size(1, 3, 0), 3, "three founders already give F = 1");
        assert_eq!(size(3, 1, 0), 1, "a cell of one machine");
    }

    #[test]
    fn the_control_journals_are_the_hosted_tenants_and_only_they() {
        let registry = registry();
        assert_eq!(
            controls(&registry).into_iter().collect::<Vec<_>>(),
            [control(50), control(60)]
        );
        assert!(is_control(&registry, control(50)));
        let data = JournalIdentifier::new(TenantId(50), JournalId(99));
        assert!(!is_control(&registry, data), "a data journal is not one");
        let foreign = JournalIdentifier::new(TenantId(70), JournalId(71));
        assert!(!is_control(&registry, foreign), "nor an unhosted tenant's");
    }

    #[test]
    fn the_extra_members_are_the_booked_machines_outside_the_founders() {
        let mut registry = registry();
        let founders = [NodeId(1), NodeId(2)];
        assert!(extra_members(&registry, &founders).is_empty());
        book(&mut registry, 100, 3, control(50));
        book(&mut registry, 101, 4, control(60));
        // A founder's slot, and a data journal's slot, are not extras.
        book(&mut registry, 102, 1, control(60));
        book(
            &mut registry,
            103,
            4,
            JournalIdentifier::new(TenantId(50), JournalId(99)),
        );
        assert_eq!(
            extra_members(&registry, &founders)
                .into_iter()
                .collect::<Vec<_>>(),
            [NodeId(3), NodeId(4)]
        );
        assert_eq!(held_ids(&registry, control(60)), [101, 102]);
        fold(
            &mut registry,
            &SystemCommand::ReleaseCapacity { booking: 100 },
        );
        assert_eq!(
            extra_members(&registry, &founders)
                .into_iter()
                .collect::<Vec<_>>(),
            [NodeId(4)]
        );
    }

    #[test]
    fn the_next_journal_is_one_whose_slots_moved_else_the_next_in_turn() {
        let mut registry = registry();
        let mut controls = Controls::default();
        // Nothing settled: every journal is pending, in id order.
        assert_eq!(controls.next(&registry), Some(control(50)));
        controls.last = Some(control(50));
        assert_eq!(controls.next(&registry), Some(control(60)));
        controls.last = Some(control(60));
        assert_eq!(controls.next(&registry), Some(control(50)), "it wraps");
        // Both settled: the turn takes every journal again, in id order.
        controls
            .settled
            .insert(control(50), (held_ids(&registry, control(50)), 2));
        controls
            .settled
            .insert(control(60), (held_ids(&registry, control(60)), 2));
        assert_eq!(controls.next(&registry), Some(control(50)));
        // A slot of 60 moved since it settled: 60 goes first.
        controls.last = Some(control(60));
        book(&mut registry, 100, 3, control(60));
        assert_eq!(controls.next(&registry), Some(control(60)));
    }
}
