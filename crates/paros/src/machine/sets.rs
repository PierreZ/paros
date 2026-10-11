//! The tenant coordinator's matchmaker sets (#501 (matchmaker sets off the
//! founders), §3.4): every tenant's set is born on the founding members (the
//! bootstrap generation, `crate::machine::CellPlan::matchmakers`), and the
//! coordinator places it on the cell's storage machines, then moves it off
//! a draining machine, one step per watch round. Every step starts from what
//! the registry and the set's journals hold now, so any later term finishes
//! a move an earlier one started:
//!
//! 1. book the slots the set lacks (`crate::placement::plan` over the
//!    registered storage machines, `Role::Matchmaker`, with the slots on the
//!    machines that stay as the current ones): a set born on the founding
//!    members holds none, so its first move books every member; a machine
//!    hosts a later generation of a set booked on it
//!    (`crate::machine::matchmakers`);
//! 2. hand the set over onto the booked machines that stay
//!    (`ReconfigureMatchmakers`, named by its tenant), through a founding
//!    member, which serves every tenant's control journal;
//! 3. once the set in force (the latest generation the founding members
//!    believe for the tenant's control journal) is exactly those machines,
//!    release every slot on a draining machine.
//!
//! The set keeps its size: the bootstrap generation's. No step names a
//! draining machine in a set. A released member keeps its frozen store: it
//! answers a late proposer with its successor (`crate::machine::matchmakers`).
//! The cell tenant's own set never moves: the registry's elections need it
//! before any coordinator runs.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, NodeId, TenantId};

use super::tenants::TenantDesk;
use crate::client::cell::CellSession;
use crate::client::{Client, ReconfigureMatchmakersOutcome, Server};
use crate::placement::{self, Want};
use crate::rpc::{InspectReply, NodeClient};
use crate::system::{Booking, BookingTarget, NodeStanding, Registry, Role};
use crate::{Address, Names};

/// The id of a tenant's one matchmaker set inside its tenant (§3.4: one set
/// per tenant; a booking target's set id is never `0`).
pub const SET: u64 = 1;

/// The booking target of `tenant`'s matchmaker set.
#[must_use]
pub fn set_of(tenant: TenantId) -> BookingTarget {
    BookingTarget::Set { tenant, set: SET }
}

/// What one set step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SetStep {
    /// No set needs a step.
    Idle,
    /// A set waits: for an answer, a free machine or its handover.
    Waiting,
    /// The slots a set lacks were booked.
    Booked,
    /// A founding member started the handover onto the booked machines.
    HandedOver,
    /// The set in force is its booked machines, and every slot on a draining
    /// machine was released. `successor` when an earlier term started the
    /// move.
    Finished {
        /// Whether this term started none of the move's steps.
        successor: bool,
    },
}

/// The set moves of one coordinator term.
#[derive(Debug, Default)]
pub(super) struct Sets {
    /// The tenants whose set this term booked or handed over.
    started: BTreeSet<TenantId>,
    /// The tenants whose set this term found booked but not yet in force on
    /// its booked machines, before it took any step of its own: an earlier
    /// term's move, whose handover gets one round to finish first.
    inherited: BTreeSet<TenantId>,
    /// The tenants this term saw settled: their set in force is their
    /// booked machines, none of them draining.
    settled: BTreeSet<TenantId>,
    /// The tenant the last step took: the next step takes the next one, so
    /// a set that waits holds no other back.
    last: Option<TenantId>,
}

/// Whether the registry holds `node` registered (not draining, not retired).
fn registered(registry: &Registry, node: NodeId) -> bool {
    registry
        .get(node)
        .is_some_and(|n| n.standing == NodeStanding::Registered)
}

/// The live slots of `tenant`'s set.
fn slots(registry: &Registry, tenant: TenantId) -> Vec<(u64, Booking)> {
    let target = set_of(tenant);
    registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Matchmaker && b.target == target)
        .map(|(id, b)| (id, *b))
        .collect()
}

/// The next tenant after `last`, in id order and wrapping, whose set may
/// need a step: hosted, not dropped, and unplaced, holding a slot on a
/// draining machine, or not seen settled by this term.
fn next_set(
    registry: &Registry,
    settled: &BTreeSet<TenantId>,
    last: Option<TenantId>,
) -> Option<TenantId> {
    let pending: BTreeSet<TenantId> = registry
        .hosted()
        .filter(|tenant| !registry.dropped(*tenant))
        .filter(|tenant| {
            let slots = slots(registry, *tenant);
            slots.is_empty()
                || slots.iter().any(|(_, b)| !registered(registry, b.node))
                || !settled.contains(tenant)
        })
        .collect();
    last.and_then(|last| pending.range((Excluded(last), Unbounded)).next())
        .or_else(|| pending.first())
        .copied()
}

/// The set in force of `journal`'s tenant as the founding members of
/// `nodes` believe it for `journal`: the answer with the latest generation,
/// and which server gave it.
async fn set_in_force<P: Providers>(
    nodes: &Client<P>,
    count: usize,
    journal: JournalIdentifier,
) -> Option<(usize, InspectReply)> {
    let mut best: Option<(usize, InspectReply)> = None;
    for target in 0..count {
        if let Some(reply) = nodes.inspect(target, journal).await
            && best
                .as_ref()
                .is_none_or(|(_, b)| reply.matchmaker_generation > b.matchmaker_generation)
        {
            best = Some((target, reply));
        }
    }
    best
}

impl Sets {
    /// Take one step of the next set's move, through the term's session
    /// `cell` (its bookings drawn by `desk`), calling the founding members
    /// where the cell's address book says they are. `None` when a write did
    /// not land: the term writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) async fn step<P: Providers>(
        &mut self,
        (providers, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
        cell: &mut CellSession,
        desk: &mut TenantDesk,
    ) -> Option<SetStep> {
        let Some(tenant) = next_set(cell.registry(), &self.settled, self.last) else {
            return Some(SetStep::Idle);
        };
        self.last = Some(tenant);
        let registry = cell.registry();
        let Some(control) = registry.hosted_tenant(tenant).map(|h| h.control) else {
            return Some(SetStep::Waiting);
        };
        let journal = JournalIdentifier::new(tenant, control);
        // The founding members serve every tenant's control journal: each
        // where the cell's address book says.
        let book: BTreeMap<NodeId, Address> = super::address_book(cell.founders(), registry)
            .into_iter()
            .collect();
        let servers: Vec<Server<P>> = book
            .iter()
            .map(|(id, addr)| Server {
                id: id.0,
                node: NodeClient::named(rpc, names.clone(), addr.clone()),
            })
            .collect();
        if servers.is_empty() {
            return Some(SetStep::Waiting);
        }
        let count = servers.len();
        let nodes = Client::new(providers, servers, *client.tunables());
        let Some((at, reply)) = set_in_force(&nodes, count, journal).await else {
            return Some(SetStep::Waiting);
        };
        // A deployment without matchmakers has no set to move.
        if reply.matchmakers.is_empty() {
            self.settled.insert(tenant);
            return Some(SetStep::Waiting);
        }
        let size = reply.matchmakers.len();
        let registry = cell.registry();
        let slots = slots(registry, tenant);
        let staying: Vec<(u64, Booking)> = slots
            .iter()
            .filter(|(_, b)| registered(registry, b.node))
            .copied()
            .collect();
        if staying.len() < size {
            return self.book(client, cell, desk, tenant, &staying, size).await;
        }
        let mut target: Vec<u64> = staying.iter().map(|(_, b)| b.node.0).collect();
        target.sort_unstable();
        let mut members = reply.matchmakers.clone();
        members.sort_unstable();
        if members != target {
            // An earlier term's handover may still run: give it a round
            // before asking for another, so two handovers rarely duel.
            if !self.started.contains(&tenant) && self.inherited.insert(tenant) {
                moonpool_assertions::reachable!(
                    "coordinator: a term finds a set's move an earlier term started"
                );
                return Some(SetStep::Waiting);
            }
            return Some(
                match nodes.reconfigure_matchmakers_of(tenant, &target, at).await {
                    ReconfigureMatchmakersOutcome::Started { .. } => {
                        moonpool_assertions::reachable!(
                            "coordinator: a set hands over to its booked machines"
                        );
                        self.started.insert(tenant);
                        SetStep::HandedOver
                    }
                    _ => SetStep::Waiting,
                },
            );
        }
        self.finish(client, cell, tenant, &slots, &staying).await
    }

    /// The last step of a set's move: the set in force is its booked
    /// machines that stay, so release every slot on a draining machine.
    /// `None` when a release did not land.
    async fn finish<P: Providers>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        tenant: TenantId,
        slots: &[(u64, Booking)],
        staying: &[(u64, Booking)],
    ) -> Option<SetStep> {
        let leaving: Vec<(u64, Booking)> = slots
            .iter()
            .filter(|slot| !staying.contains(slot))
            .copied()
            .collect();
        for &(booking, slot) in &leaving {
            super::tenants::release(client, cell, |b| *b == slot)
                .await
                .ok()?;
            assert!(
                cell.registry().booking(booking).is_none(),
                "a set's old slot is released"
            );
        }
        self.settled.insert(tenant);
        let started = self.started.remove(&tenant);
        let inherited = self.inherited.remove(&tenant);
        if leaving.is_empty() && !started && !inherited {
            return Some(SetStep::Waiting);
        }
        let successor = !started;
        if !leaving.is_empty() {
            moonpool_assertions::reachable!("coordinator: a set moved off a draining machine");
        }
        moonpool_assertions::sometimes!(true, "coordinator: a set's move finished");
        moonpool_assertions::sometimes!(
            successor,
            "coordinator: a successor term finished a set's move"
        );
        tracing::info!(
            tenant = tenant.0,
            successor,
            released = leaving.len(),
            "matchmaker_set_moved"
        );
        Some(SetStep::Finished { successor })
    }

    /// Book the slots `tenant`'s set lacks on the cell's storage machines, so
    /// its machines that stay and the new ones are `size`. `None` when a
    /// booking did not land.
    async fn book<P: Providers>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        tenant: TenantId,
        staying: &[(u64, Booking)],
        size: usize,
    ) -> Option<SetStep> {
        let want = Want {
            role: Role::Matchmaker,
            target: set_of(tenant),
            count: size,
        };
        let storage = desk.storage(cell.registry());
        // A draining machine is not registered: it takes no new slot.
        let machines = placement::machines(cell.registry(), storage.iter().copied());
        let Ok(steps) = placement::plan(&want, &machines, staying, tenant.0) else {
            moonpool_assertions::reachable!("coordinator: a set has no machine to go to");
            return Some(SetStep::Waiting);
        };
        desk.book(client, cell, &want, &steps).await.ok()?;
        moonpool_assertions::reachable!("coordinator: a set's slots are booked");
        self.started.insert(tenant);
        moonpool_buggify::hint!("set booked, not handed over").await;
        Some(SetStep::Booked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_set_is_named_by_its_tenant() {
        assert_eq!(
            set_of(TenantId(7)),
            BookingTarget::Set {
                tenant: TenantId(7),
                set: SET
            }
        );
        assert_eq!(set_of(TenantId(7)).tenant(), TenantId(7));
    }
}
