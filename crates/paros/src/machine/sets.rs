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
//! A settled set is judged again in turn once no other set is pending: a
//! node that finishes a frozen generation proposes the members that
//! answered its freeze, so a set in force can leave its booked machines
//! after its move finished.
//!
//! The set has `2F+1` matchmakers, `F` the largest `failures_per_partition`
//! of its tenant (its default and every live journal, at least 1), on at
//! most every storage machine (#214 (tenant modes)): a set grows by booking
//! the slots it lacks and shrinks by handing over onto the slots placement
//! keeps, releasing the others once the smaller set is in force. No step
//! names a draining machine in a set. A released member keeps its frozen store: it
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
use crate::client::journals::JournalAnswer;
use crate::client::{Client, ReconfigureMatchmakersOutcome, Server};
use crate::placement::{self, Step, Want};
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
    /// booked machines, none of them draining. A settled set is judged
    /// again once no other set is pending, and leaves this set when its set
    /// in force moved off its booked machines.
    settled: BTreeSet<TenantId>,
    /// The tenant the last step took: the next step takes the next one, so
    /// a set that waits holds no other back.
    last: Option<TenantId>,
    /// The term's first finished move asks its handover again (a BUGGIFY
    /// decision of the candidacy, carried in).
    ask_again: bool,
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
/// draining machine, or not seen settled by this term. Once none is, the
/// next settled one, judged again: a settled set does not stay so (see
/// [`Sets::step`]).
fn next_set(
    registry: &Registry,
    settled: &BTreeSet<TenantId>,
    last: Option<TenantId>,
) -> Option<TenantId> {
    let hosted: BTreeSet<TenantId> = registry
        .hosted()
        .filter(|tenant| !registry.dropped(*tenant))
        .collect();
    let pending: BTreeSet<TenantId> = hosted
        .iter()
        .filter(|tenant| {
            let slots = slots(registry, **tenant);
            slots.is_empty()
                || slots.iter().any(|(_, b)| !registered(registry, b.node))
                || !settled.contains(*tenant)
        })
        .copied()
        .collect();
    assert!(pending.is_subset(&hosted), "a pending set is hosted");
    // With none pending, every hosted set is a settled one to judge again.
    let turn = if pending.is_empty() {
        &hosted
    } else {
        &pending
    };
    last.and_then(|last| turn.range((Excluded(last), Unbounded)).next())
        .or_else(|| turn.first())
        .copied()
}

/// The size of a tenant's matchmaker set (§3.4, #214 (tenant modes)):
/// `2F+1` matchmakers, `F` its largest `failures_per_partition` and at
/// least 1 (its control journal's), on at most `storage` machines.
fn set_size(largest_failures: u32, storage: usize) -> usize {
    let f = usize::try_from(largest_failures.max(1)).unwrap_or(usize::MAX);
    f.saturating_mul(2).saturating_add(1).min(storage).max(1)
}

/// The machines a set shrunk to `size` keeps, of its slots on machines that
/// stay `staying`: its members in force `members` when they are booked and
/// the size, else the slots placement does not release (a down machine
/// first, then the most loaded). `None` when placement finds no plan.
fn kept(
    registry: &Registry,
    storage: &BTreeSet<NodeId>,
    tenant: TenantId,
    (staying, members): (&[(u64, Booking)], &[u64]),
    size: usize,
) -> Option<BTreeSet<NodeId>> {
    if members.len() == size
        && members
            .iter()
            .all(|m| staying.iter().any(|(_, b)| b.node.0 == *m))
    {
        return Some(members.iter().copied().map(NodeId).collect());
    }
    let want = Want {
        role: Role::Matchmaker,
        target: set_of(tenant),
        count: size,
    };
    let machines = placement::machines(registry, storage.iter().copied());
    let steps = placement::plan(&want, &machines, staying, tenant.0).ok()?;
    let released: BTreeSet<NodeId> = steps
        .iter()
        .filter_map(|step| match *step {
            Step::Release { node, .. } => Some(node),
            Step::Book { .. } => None,
        })
        .collect();
    Some(
        staying
            .iter()
            .map(|(_, b)| b.node)
            .filter(|node| !released.contains(node))
            .collect(),
    )
}

/// The size `tenant`'s set should have: `2F+1`, `F` the tenant's largest
/// `failures_per_partition` (#214 (tenant modes)), at most every storage
/// machine; `in_force`, the size in force, while its control journal
/// `control` cannot be read. `None` once a later term fenced it.
async fn desired_size<P: Providers>(
    client: &Client<P>,
    cell: &CellSession,
    desk: &mut TenantDesk,
    (tenant, control): (TenantId, paros_core::JournalId),
    in_force: usize,
) -> Option<usize> {
    match desk.tenant(client, tenant, control).await {
        Ok(state) => {
            let largest = state.largest_failures();
            Some(set_size(largest, desk.storage(cell.registry()).len()))
        }
        Err(JournalAnswer::NotCoordinator) => None,
        Err(_) => Some(in_force),
    }
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
    /// The set moves of a new term; `ask_again` is the candidacy's BUGGIFY
    /// decision to ask the term's first finished move's handover again.
    pub(super) fn new(ask_again: bool) -> Self {
        let sets = Self {
            ask_again,
            ..Self::default()
        };
        assert!(
            sets.settled.is_empty(),
            "a new term has seen no set settled"
        );
        assert!(sets.started.is_empty(), "a new term has started no move");
        sets
    }

    /// Take one step of the next set's move, through the term's session
    /// `cell` (its bookings drawn by `desk`), calling the founding members
    /// where the cell's address book says they are. `None` when a write did
    /// not land: the term writes nothing more.
    #[allow(clippy::too_many_lines)]
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
        let size = desired_size(
            client,
            cell,
            desk,
            (tenant, control),
            reply.matchmakers.len(),
        )
        .await?;
        if size != reply.matchmakers.len() {
            moonpool_assertions::reachable!("coordinator: a set is resized to its tenant's modes");
        }
        let registry = cell.registry();
        let slots = slots(registry, tenant);
        let mut staying: Vec<(u64, Booking)> = slots
            .iter()
            .filter(|(_, b)| registered(registry, b.node))
            .copied()
            .collect();
        if staying.len() < size {
            return self.book(client, cell, desk, tenant, &staying, size).await;
        }
        let mut members = reply.matchmakers.clone();
        members.sort_unstable();
        if staying.len() > size {
            let storage = desk.storage(registry);
            let Some(kept) = kept(registry, &storage, tenant, (&staying, &members), size) else {
                return Some(SetStep::Waiting);
            };
            staying.retain(|(_, b)| kept.contains(&b.node));
            if staying.len() != size {
                return Some(SetStep::Waiting);
            }
        }
        let mut target: Vec<u64> = staying.iter().map(|(_, b)| b.node.0).collect();
        target.sort_unstable();
        if members != target {
            // A set this term saw settled does not stay so: any node finishes
            // a frozen generation onto the members that answered its freeze,
            // a thinner set, and a freeze may come from a stale retry of this
            // term's own handover, accepted after the move it asked for was
            // done. Judged again in turn, the set is moved back onto its
            // booked machines (witness 5529552317995697871, PR #555 (fix
            // main's red sweep, part 2): a set settled on its three booked
            // machines, a retried handover froze it and was abandoned, and
            // another node finished it onto two).
            if self.settled.remove(&tenant) {
                moonpool_assertions::reachable!(
                    "coordinator: a settled set left its booked machines"
                );
            }
            // An earlier term's handover may still run: give it a round
            // before asking for another, so two handovers rarely duel.
            if !self.started.contains(&tenant) && self.inherited.insert(tenant) {
                moonpool_assertions::reachable!(
                    "coordinator: a term finds a set's move an earlier term started"
                );
                return Some(SetStep::Waiting);
            }
            return Some(
                match nodes.reconfigure_matchmakers(tenant, &target, at).await {
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
        let step = self.finish(client, cell, tenant, &slots, &staying).await;
        // BUGGIFY (the candidacy's decision): the term's first finished move
        // asks its handover again, as a retry of it that lands after the
        // move does (a `Busy` answer retried by the client). Valid: a
        // founding member accepts a handover onto the set in force, and the
        // set may then leave its booked machines (a thinner set, finished by
        // another node), which the next judgement of it must undo.
        if matches!(step, Some(SetStep::Finished { .. })) && std::mem::take(&mut self.ask_again) {
            moonpool_assertions::reachable!(
                "coordinator: a finished set's handover is asked again"
            );
            let _ = nodes.reconfigure_matchmakers(tenant, &target, at).await;
        }
        step
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
    fn a_set_has_two_f_plus_one_members_on_the_storage_machines() {
        assert_eq!(set_size(0, 5), 3, "a control journal has F = 1");
        assert_eq!(set_size(1, 5), 3);
        assert_eq!(set_size(2, 5), 5);
        assert_eq!(set_size(3, 5), 5, "at most every storage machine");
        assert_eq!(set_size(3, 9), 7);
        assert_eq!(set_size(1, 1), 1, "a cell of one machine");
    }

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
