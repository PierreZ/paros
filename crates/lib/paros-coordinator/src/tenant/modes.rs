//! The tenant coordinator's modes (#214 (tenant modes), §3.4): a journal
//! whose desired mode (`Configure`, FDB's `configure`) differs from the
//! configuration it is placed on moves to it, one step per watch round, so
//! every step starts from what the registry, the tenant control journal and
//! the journal's leader hold now, and any later term finishes a change an
//! earlier one started:
//!
//! 1. book the acceptor slots the new mode lacks on other storage machines
//!    (`paros_model::placement::plan`, with the journal's slots as the current
//!    ones); the machine starts the journal as a spare (`paros_roles::node::system`);
//! 2. ask the journal's leader to reconfigure onto the new members under the
//!    new quorum system (`Reconfigure`, named by its journal). A smaller mode
//!    keeps the slots placement does not release; the cross-configuration
//!    Phase 1 handles two quorum systems;
//! 3. once that configuration is in force, record it in the tenant control
//!    journal (`Placed`, what views show), then, once the leader's GC
//!    watermark has reached it, release the slot of each member it left.
//!    Unlike a move off a draining machine (`crate::tenant::moves`), it retires no
//!    member: the machine stays bookable, a later mode may place the
//!    journal on it again, and a retired identity never comes back to its
//!    journal. The release stops the journal there, its store kept.
//!
//! The configuration a change composes carries the journal's booked proxy
//! leaders and replicas (`crate::tenant::roles`) as its counts, so a count
//! change is the same round change on the same members (#754 (counts off
//! the format marker)): the journal is settled only once its configuration
//! in force names as many of each as the registry books.
//!
//! A journal with a slot on a draining machine, on one held slow (#361
//! (slow machines)), or on one gone (down past the re-placement bound, #475 (re-place after the Down bound)), is the
//! move's, never a mode change's: it waits until the move is done. Else the
//! change releases the slot the move booked, a slot outside the
//! configuration in force, at every round, and the move never ends. A gone
//! machine takes no new slot of a mode change either, and counts as no
//! machine to place on: the move would release it at once. A slot outside
//! the configuration in force on a journal whose machines all stay is the
//! change's alone: its last step records the configuration, then releases
//! it (`crate::tenant::moves` releases none). A desired mode the cell cannot place
//! waits: a configure judged it placeable when it was asked
//! (`crate::tenant`), and slots free up again.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use paros_core::journal::{JournalIdentifier, TenantId};
use paros_core::membership::{AcceptorConfig, QuorumSystem};
use paros_net::runtime::ParosProviders;

use crate::tenant::moves::{at_or_above, bookable, leader_of, stays};
use crate::tenant::{TenantDesk, placed_count};
use paros_client::cell::CellSession;
use paros_client::journals::JournalAnswer;
use paros_client::{Client, ReconfigureOutcome, Server};
use paros_model::address::Address;
use paros_model::address::Names;
use paros_model::placement::{self, Step, Want};
use paros_model::registry::{Booking, BookingTarget, Registry, Role};
use paros_model::tenant::Desired;
use paros_net::NodeClient;

/// What one mode step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ModeStep {
    /// No journal's placement differs from its desired mode.
    Idle,
    /// A change waits: for a leader, a free machine, a configuration in
    /// force or the GC watermark.
    Waiting,
    /// The slots the new mode lacks were booked.
    Booked,
    /// The leader started the reconfiguration to the new mode.
    Reconfigured,
    /// The new mode is in force and recorded, and the slots it left were
    /// released. `successor` when an earlier term started it.
    Finished {
        /// Whether this term started none of the change's steps.
        successor: bool,
    },
}

impl ModeStep {
    /// Whether the step started or advanced a change that is not done.
    pub(crate) fn in_flight(self) -> bool {
        matches!(self, Self::Booked | Self::Reconfigured)
    }
}

/// The mode changes of one coordinator term: the journals whose change
/// this term started a step of.
#[derive(Debug, Default)]
pub(crate) struct Modes {
    started: BTreeSet<JournalIdentifier>,
    /// The journal the last step took: the next step takes the next one,
    /// so a change that waits holds no other back.
    last: Option<JournalIdentifier>,
}

/// A journal whose placement differs from its desired mode.
struct Change {
    journal: JournalIdentifier,
    desired: Desired,
    /// The configuration in force its control journal records.
    placed: AcceptorConfig,
    /// Its acceptor slots.
    slots: Vec<(u64, Booking)>,
    /// The counts its configuration must carry: its booked proxy leaders
    /// and replicas ([`counts`]).
    counts: (usize, usize),
}

/// The proxy leaders and the replicas the registry books for `journal`
/// (#754 (counts off the format marker)): the counts its configuration
/// carries.
fn counts(registry: &Registry, journal: JournalIdentifier) -> (usize, usize) {
    let booked = |role: Role| {
        registry
            .bookings()
            .filter(|(_, b)| b.role == role && b.target == BookingTarget::Journal(journal))
            .count()
            .min(paros_core::membership::MAX_ROLE_COUNT)
    };
    (booked(Role::ProxyLeader), booked(Role::Replica))
}

/// The acceptor slots of `journal`.
fn slots(registry: &Registry, journal: JournalIdentifier) -> Vec<(u64, Booking)> {
    registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Acceptor && b.target == BookingTarget::Journal(journal))
        .map(|(id, b)| (id, *b))
        .collect()
}

/// The sorted nodes of `slots`.
fn nodes_of(slots: &[(u64, Booking)]) -> Vec<NodeId> {
    let mut nodes: Vec<NodeId> = slots.iter().map(|(_, b)| b.node).collect();
    nodes.sort_unstable();
    nodes
}

/// Whether `placed` is the shape `desired` asks for, `count` members under
/// its quorum system, on exactly the journal's slots, with its booked
/// `(proxies, replicas)` counts.
fn settled(
    placed: &AcceptorConfig,
    desired: &Desired,
    (count, counts): (usize, (usize, usize)),
    slots: &[NodeId],
) -> bool {
    placed.members().len() == count
        && placed.quorum_system() == desired.quorum_system()
        && placed.members() == slots
        && (placed.proxy_count(), placed.replica_count()) == counts
}

impl Modes {
    /// Take one step of the next journal's mode change, through the term's
    /// session `cell` (its bookings drawn by `desk`, which holds the tenant
    /// control journals), calling the journal's machines where the cell's
    /// address book says they are. A journal with a slot on a machine
    /// `gone` is the move's. `None` when a write did not land: the term
    /// writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn step<P: ParosProviders>(
        &mut self,
        (providers, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        gone: &crate::tenant::moves::Leaving,
    ) -> Option<ModeStep> {
        let Some(change) = self.next(client, cell, desk, gone).await? else {
            return Some(ModeStep::Idle);
        };
        let journal = change.journal;
        self.last = Some(journal);
        let registry = cell.registry();
        // The journal's machines, each where the cell's address book says.
        let book: BTreeMap<NodeId, Address> =
            paros_model::machine::cell_book(cell.founders(), registry)
                .into_iter()
                .collect();
        let servers: Vec<(NodeId, Server<P>)> = change
            .slots
            .iter()
            .filter_map(|(_, b)| {
                let addr = book.get(&b.node)?;
                Some((
                    b.node,
                    Server {
                        id: b.node.0,
                        node: NodeClient::named(rpc, names.clone(), addr.clone()),
                    },
                ))
            })
            .collect();
        if servers.is_empty() {
            return Some(ModeStep::Waiting);
        }
        let ids: Vec<NodeId> = servers.iter().map(|(id, _)| *id).collect();
        let nodes = Client::new(
            providers,
            servers.into_iter().map(|(_, s)| s).collect(),
            *client.tunables(),
        );
        let Some((at, reply)) = leader_of(&nodes, ids.len(), journal).await else {
            return Some(ModeStep::Waiting);
        };
        let Ok(system) = paros_wire::quorum_system_from_proto(&(&reply).into()) else {
            return Some(ModeStep::Waiting);
        };
        // A gone machine takes no new slot: a move would release it at
        // once (#475), and the change would book it again at every round.
        let machines = bookable(cell.registry(), desk, &gone.gone);
        let count = placed_count(&change.desired, machines.len());
        let want = Want {
            role: Role::Acceptor,
            target: BookingTarget::Journal(journal),
            count,
        };
        if change.slots.len() < count {
            return self
                .book(client, cell, (desk, &gone.gone), &want, &change.slots)
                .await;
        }
        let slot_nodes = nodes_of(&change.slots);
        let mut members: Vec<NodeId> = reply.members.iter().copied().map(NodeId).collect();
        members.sort_unstable();
        let wanted = change.desired.quorum_system();
        let in_force = members.len() == count
            && system == wanted
            && members.iter().all(|m| slot_nodes.contains(m))
            && (reply.proxies, reply.replicas) == (change.counts.0 as u64, change.counts.1 as u64);
        if !in_force {
            return Some(
                self.reconfigure(cell, desk, (&nodes, at), &want, &change, wanted)
                    .await,
            );
        }
        self.finish(
            client,
            cell,
            desk,
            &change,
            // Labeled and owned like a create's (#215 (zones)), with its
            // counts (#754).
            (
                placement::zoned(&members, &machines, system)
                    .with_cell(cell.journals().cell_id)
                    .with_counts(change.counts.0, change.counts.1),
                &reply,
            ),
        )
        .await
    }

    /// The next journal after the last one, in id order and wrapping, whose
    /// placement differs from its desired mode: a live journal of a tenant
    /// the cell hosts, with no slot on a draining, slow or `gone` machine.
    /// `None` when a later term fenced a tenant control journal: the term
    /// is over.
    async fn next<P: ParosProviders>(
        &self,
        client: &Client<P>,
        cell: &CellSession,
        desk: &mut TenantDesk,
        gone: &crate::tenant::moves::Leaving,
    ) -> Option<Option<Change>> {
        let registry = cell.registry();
        let hosted: Vec<(TenantId, paros_core::journal::JournalId)> = registry
            .hosted()
            .filter(|tenant| !registry.dropped(*tenant))
            .filter_map(|tenant| registry.hosted_tenant(tenant).map(|h| (tenant, h.control)))
            .collect();
        // The machines placement may book, as every step counts them.
        let storage = bookable(registry, desk, &gone.gone).len();
        let mut changes: BTreeMap<JournalIdentifier, Change> = BTreeMap::new();
        for (tenant, control) in hosted {
            let state = match desk.tenant(client, tenant, control).await {
                Ok(state) => state,
                Err(JournalAnswer::NotCoordinator) => return None,
                Err(_) => continue,
            };
            for (id, journal) in state.live() {
                let identifier = JournalIdentifier::new(tenant, id);
                let slots = slots(registry, identifier);
                // A journal on a draining, slow or gone machine is the
                // move's (`crate::tenant::moves`, the same `stays`); one with no slot
                // was never placed by this cell.
                // So is one the rebalancer moves (#687 (rebalance a cell)).
                if slots.is_empty()
                    || slots
                        .iter()
                        .any(|(id, b)| !stays(registry, gone, b.node) || gone.rebalances(*id))
                {
                    continue;
                }
                let count = placed_count(&journal.desired, storage);
                let counts = counts(registry, identifier);
                if settled(
                    &journal.placed,
                    &journal.desired,
                    (count, counts),
                    &nodes_of(&slots),
                ) {
                    continue;
                }
                changes.insert(
                    identifier,
                    Change {
                        journal: identifier,
                        desired: journal.desired,
                        placed: journal.placed.clone(),
                        slots,
                        counts,
                    },
                );
            }
        }
        let next = self
            .last
            .and_then(|last| changes.range((Excluded(last), Unbounded)).next())
            .or_else(|| changes.iter().next())
            .map(|(id, _)| *id);
        Some(next.and_then(|id| changes.remove(&id)))
    }

    /// Book the slots the new mode lacks, so the journal holds `want`'s
    /// count, on no machine `gone`. `None` when a booking did not land.
    async fn book<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        (desk, gone): (&mut TenantDesk, &BTreeSet<NodeId>),
        want: &Want,
        slots: &[(u64, Booking)],
    ) -> Option<ModeStep> {
        // A slow machine keeps no role (#361 (slow machines)): it takes no
        // new slot, as the slots it holds are left to the moves.
        let machines: Vec<placement::Machine> = bookable(cell.registry(), desk, gone)
            .into_iter()
            .filter(|m| cell.registry().keeps_roles(m.node))
            .collect();
        // Apart from the journal's other roles (placement's rule 2).
        let current = placement::with_others(cell.registry(), (want.role, want.target), slots);
        let Ok(steps) = placement::plan(want, &machines, &current, want.target.tenant().0) else {
            return Some(ModeStep::Waiting);
        };
        desk.book(client, cell, want, &steps).await.ok()?;
        moonpool_assertions::reachable!("coordinator: a mode change books its new slots");
        if let BookingTarget::Journal(journal) = want.target {
            self.started.insert(journal);
        }
        moonpool_buggify::hint!("mode booked, not reconfigured").await;
        Some(ModeStep::Booked)
    }

    /// Ask the leader at server `at` of `nodes` to reconfigure onto the new
    /// mode's members under `system`: every slot when the journal holds as
    /// many as the mode asks for, else the slots placement keeps.
    async fn reconfigure<P: ParosProviders>(
        &mut self,
        cell: &CellSession,
        desk: &TenantDesk,
        (nodes, at): (&Client<P>, usize),
        want: &Want,
        change: &Change,
        system: QuorumSystem,
    ) -> ModeStep {
        let mut target: Vec<NodeId> = if change.slots.len() == want.count {
            nodes_of(&change.slots)
        } else {
            // A smaller mode: placement picks the slots to release (a down
            // machine first, then the most loaded); the members are the rest.
            let storage = desk.storage(cell.registry());
            let machines = desk.machines(cell.registry(), storage.iter().copied());
            let Ok(steps) = placement::plan(want, &machines, &change.slots, want.target.tenant().0)
            else {
                return ModeStep::Waiting;
            };
            let released: BTreeSet<NodeId> = steps
                .iter()
                .filter_map(|step| match *step {
                    Step::Release { node, .. } => Some(node),
                    Step::Book { .. } => None,
                })
                .collect();
            moonpool_assertions::reachable!("coordinator: a mode change shrinks its journal");
            nodes_of(&change.slots)
                .into_iter()
                .filter(|node| !released.contains(node))
                .collect()
        };
        target.sort_unstable();
        if target.len() != want.count || !system.admits(target.len()) {
            return ModeStep::Waiting;
        }
        // Every member of the new configuration is up: a member that is
        // down may never return (a wipe), and a configuration with fewer
        // live members than a Phase-1 quorum elects no leader again, so no
        // move can heal it. A down member under a maintenance window stays
        // a member (#359), and the change waits for it (replay-recipe
        // 6088854892229177840 14147:11814493376964626340 on f918cf5 (#712
        // (renew between pages)): a window held a wiped member, a booked
        // spare was wiped a second before, the change reconfigured onto
        // both, and slot 8, decided under the old configuration, was never
        // applied).
        if target
            .iter()
            .any(|node| !cell.registry().liveness(*node).up)
        {
            moonpool_assertions::reachable!(
                "coordinator: a mode change waits for a member that is down"
            );
            return ModeStep::Waiting;
        }
        // The new configuration is labeled with its members' zones and
        // owned by this cell, like a create's (#215 (zones)).
        let machines = desk.machines(cell.registry(), target.iter().copied());
        let composed = placement::zoned(&target, &machines, system)
            .with_cell(cell.journals().cell_id)
            .with_counts(change.counts.0, change.counts.1);
        match nodes.reconfigure_to(change.journal, &composed, at).await {
            ReconfigureOutcome::Started { .. } => {
                moonpool_assertions::reachable!(
                    "coordinator: a mode change reconfigures its journal"
                );
                if (change.placed.proxy_count(), change.placed.replica_count()) != change.counts {
                    moonpool_assertions::reachable!(
                        "coordinator: a count change reconfigures its journal"
                    );
                }
                if change.placed.quorum_system() != system {
                    moonpool_assertions::reachable!(
                        "coordinator: a mode change moves its journal to another quorum system"
                    );
                }
                self.started.insert(change.journal);
                ModeStep::Reconfigured
            }
            _ => ModeStep::Waiting,
        }
    }

    /// The last step of a change: the new mode is in force as `in_force`.
    /// Record it, then, once the leader's floor reached it, release every
    /// slot it does not name (no member is retired: see the module doc).
    /// `None` when a write did not land.
    async fn finish<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        change: &Change,
        (in_force, reply): (AcceptorConfig, &paros_wire::InspectReply),
    ) -> Option<ModeStep> {
        let journal = change.journal;
        let leaving: Vec<(u64, Booking)> = change
            .slots
            .iter()
            .filter(|(_, b)| !in_force.members().contains(&b.node))
            .copied()
            .collect();
        // Every earlier configuration is forgotten once the leader's floor
        // reached this one's ballot: no leader asks a leaving member again.
        if !leaving.is_empty()
            && !at_or_above(reply.gc_watermark.as_ref(), reply.config_ballot.as_ref())
        {
            return Some(ModeStep::Waiting);
        }
        if change.placed != in_force {
            match desk
                .record_placed(client, journal.tenant, journal.journal, in_force.clone())
                .await
            {
                Ok(()) => {}
                Err(JournalAnswer::NotCoordinator) => return None,
                Err(_) => return Some(ModeStep::Waiting),
            }
            moonpool_assertions::reachable!("coordinator: a mode change records its placement");
        }
        for &(booking, slot) in &leaving {
            crate::tenant::release(client, cell, |b| *b == slot)
                .await
                .ok()?;
            assert!(
                cell.registry().booking(booking).is_none(),
                "a mode change's old slot is released"
            );
        }
        let successor = !self.started.remove(&journal);
        if !leaving.is_empty() {
            moonpool_assertions::reachable!(
                "coordinator: a mode change releases the slots it left"
            );
        }
        moonpool_assertions::sometimes!(true, "coordinator: a mode change finished");
        tracing::info!(
            journal = %journal,
            successor,
            members = in_force.members().len(),
            "journal_mode_applied"
        );
        Some(ModeStep::Finished { successor })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_model::tenant::Durability;

    fn desired(survive_machines: u32, partitions: u32) -> Desired {
        Desired {
            durability: Durability::new(survive_machines, partitions).expect("a durability"),
            ..Desired::DEFAULT
        }
    }

    #[test]
    fn a_journal_is_settled_only_on_its_desired_shape_and_slots() {
        let nodes = |ids: &[u64]| ids.iter().copied().map(NodeId).collect::<Vec<_>>();
        let three = AcceptorConfig::new(nodes(&[1, 2, 3]), QuorumSystem::Majority);
        let majority = desired(1, 1);
        assert!(settled(&three, &majority, (3, (0, 0)), &nodes(&[1, 2, 3])));
        // Another slot, or a missing one: not settled.
        assert!(!settled(&three, &majority, (3, (0, 0)), &nodes(&[1, 2, 4])));
        assert!(!settled(
            &three,
            &majority,
            (3, (0, 0)),
            &nodes(&[1, 2, 3, 4])
        ));
        // The same members under another quorum system: not settled.
        let partitions = desired(0, 3);
        assert!(!settled(
            &three,
            &partitions,
            (3, (0, 0)),
            &nodes(&[1, 2, 3])
        ));
        let grid = AcceptorConfig::new(nodes(&[1, 2, 3]), partitions.quorum_system());
        assert!(settled(&grid, &partitions, (3, (0, 0)), &nodes(&[1, 2, 3])));
        // The same shape with another count: not settled (#754).
        assert!(!settled(&three, &majority, (3, (1, 0)), &nodes(&[1, 2, 3])));
        let proxied = three.clone().with_counts(1, 0);
        assert!(settled(
            &proxied,
            &majority,
            (3, (1, 0)),
            &nodes(&[1, 2, 3])
        ));
        // A larger mode on the same slots: not settled.
        assert!(!settled(
            &three,
            &desired(2, 1),
            (5, (0, 0)),
            &nodes(&[1, 2, 3])
        ));
    }
}
