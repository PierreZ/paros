//! The tenant coordinator's matchmaker sets (#501 (matchmaker sets off the
//! founders), §3.4): every tenant's set is born on the founding members (the
//! bootstrap generation, `paros_model::machine::CellPlan::matchmakers`), and the
//! coordinator places it on the cell's storage machines, then moves it off
//! a draining machine, one step per watch round. Every step starts from what
//! the registry and the set's journals hold now, so any later term finishes
//! a move an earlier one started:
//!
//! 1. book the slots the set lacks (`paros_model::placement::plan` over the
//!    registered storage machines, `Role::Matchmaker`, with the slots on the
//!    machines that stay as the current ones): a set born on the founding
//!    members holds none, so its first move books every member; a machine
//!    hosts a later generation of a set booked on it
//!    (`paros_machine::matchmakers`);
//! 2. hand the set over onto the booked machines that stay
//!    (`ReconfigureMatchmakers`, named by its tenant), through a founding
//!    member, which serves every tenant's control journal;
//! 3. once the set in force (the latest generation the founding members
//!    believe for the tenant's control journal) is exactly those machines,
//!    release every slot on a draining machine.
//!
//! A machine down past the re-placement bound (gone, #475 (re-place after
//! the Down bound)) is left as a draining one is.
//!
//! A settled set is judged again in turn once no other set is pending: a
//! node that finishes a frozen generation proposes the members that
//! answered its freeze, so a set in force can leave its booked machines
//! after its move finished.
//!
//! The set has `2F+1` matchmakers, `F` the largest `survive_machines`
//! of its tenant (its default and every live journal, at least 1), on at
//! most every storage machine (#214 (tenant modes)): a set grows by booking
//! the slots it lacks and shrinks by handing over onto the slots placement
//! keeps, releasing the others once the smaller set is in force. A shrink
//! never releases the slot of a machine an open maintenance window holds
//! (#359): with as many held slots as its size or more, the set keeps them
//! all until a window closes. No step
//! names a draining machine in a set. A released member keeps its frozen store: it
//! answers a late proposer with its successor (`paros_machine::matchmakers`).
//!
//! The universe tenant's set moves too (#685 (special tenants are tenants)
//! step 3), on the cell that hosts it. Its `F` is the control plane's
//! ([`paros_model::registry::control_plane`]), not a tenant description's,
//! and it keeps the founding members beside its slots. The cell tenant's
//! set stays on the founding members until step 4 (see `system_tenants`).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use paros_core::journal::{JournalIdentifier, TenantId};
use paros_net::runtime::ParosProviders;

use crate::tenant::TenantDesk;
use crate::tenant::moves::{Leaving, stays};
use paros_client::cell::CellSession;
use paros_client::journals::JournalAnswer;
use paros_client::{Client, ReconfigureMatchmakersOutcome, Server};
use paros_model::address::Address;
use paros_model::address::Names;
use paros_model::placement::{self, Step, Want};
use paros_model::registry::{Booking, BookingTarget, NodeStanding, Registry, Role};
use paros_net::NodeClient;
use paros_wire::InspectReply;

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
pub(crate) enum SetStep {
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
pub(crate) struct Sets {
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
    /// The generation in force when this term's last handover of each
    /// tenant's set started: a release waits until a later generation is
    /// in force.
    handed: BTreeMap<TenantId, u64>,
    /// The term's first finished move asks its handover again (a BUGGIFY
    /// decision of the candidacy, carried in).
    ask_again: bool,
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
    gone: &Leaving,
    (settled, system): (&BTreeSet<TenantId>, &BTreeMap<TenantId, JournalIdentifier>),
    last: Option<TenantId>,
) -> Option<TenantId> {
    let hosted: BTreeSet<TenantId> = registry
        .hosted()
        .filter(|tenant| !registry.dropped(*tenant))
        .chain(system.keys().copied())
        .collect();
    assert!(
        system.keys().all(|tenant| hosted.contains(tenant)),
        "every special tenant's set is one to move"
    );
    let pending: BTreeSet<TenantId> = hosted
        .iter()
        .filter(|tenant| {
            let slots = slots(registry, **tenant);
            // A special tenant's set that holds no slot stays on its
            // founding members by design: once judged settled it waits for
            // its turn among the settled ones, never holding a hosted set's
            // move back (sweep seed 1795891572755981828 on a01a090: a hosted
            // set booked but never handed over in the tail).
            (slots.is_empty() && !system.contains_key(*tenant))
                || slots.iter().any(|(_, b)| !stays(registry, gone, b.node))
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
/// `2F+1` matchmakers, `F` its largest `survive_machines` and at
/// least 1 (its control journal's), on at most `storage` machines.
fn set_size(largest_failures: u32, storage: usize) -> usize {
    let f = usize::try_from(largest_failures.max(1)).unwrap_or(usize::MAX);
    f.saturating_mul(2).saturating_add(1).min(storage).max(1)
}

/// The size of a special tenant's set (#685 (special tenants are
/// tenants)): `2F+1` matchmakers, `F` the control plane's, on at most
/// `eligible` machines, and always odd.
fn special_size(failures: u32, eligible: usize) -> usize {
    let size = paros_model::registry::control_plane::acceptors(failures).min(eligible.max(1));
    let size = if size.is_multiple_of(2) {
        size - 1
    } else {
        size
    };
    assert!(size % 2 == 1, "a special tenant's set has an odd size");
    size
}

/// The size a special tenant's set wants on the cell of `cell` (#685),
/// `failures` the control plane's `F`: `2F+1` of the machines placement
/// may book, never an even size. A second member of a set of two adds a
/// machine the cell's elections wait on and no failure it survives (a
/// one-founder cell that grew its set onto one admitted machine, then lost
/// it: every journal of the cell stalled).
fn special_wanted(cell: &CellSession, desk: &TenantDesk, tenant: TenantId, failures: u32) -> usize {
    let registry = cell.registry();
    let storage = desk.storage(registry);
    let held: BTreeSet<NodeId> = slots(registry, tenant)
        .iter()
        .map(|(_, b)| b.node)
        .collect();
    let eligible = placement::machines(registry, storage.iter().copied())
        .iter()
        .filter(|m| held.contains(&m.node) || m.booked < m.capacity)
        .count();
    special_size(failures, eligible)
}

/// The special tenants of the cell of `journals` whose sets move (#685
/// (special tenants are tenants) step 3), each with the control journal its
/// set is judged by: the universe tenant's, when this cell hosts it.
///
/// The cell tenant's set stays on the founding members until step 4. Its
/// handover onto admitted machines needs them to fold their booking from
/// the registry, and the registry is the cell control journal, which needs
/// the set: a one-founder cell whose cell-tenant set grew onto two
/// admitted machines lost the founder and never elected again (sweep seed
/// 17599158648748538241 on 80c7bb1).
fn system_tenants(
    journals: paros_model::machine::ControlJournals,
) -> BTreeMap<TenantId, JournalIdentifier> {
    let system: BTreeMap<TenantId, JournalIdentifier> = journals
        .fleet
        .filter(|fleet| fleet.tenant != journals.cell.tenant)
        .map(|journal| (journal.tenant, journal))
        .into_iter()
        .collect();
    assert!(
        !system.contains_key(&journals.cell.tenant),
        "the cell tenant's set stays on the founding members"
    );
    system
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
/// `survive_machines` (#214 (tenant modes)), at most every storage
/// machine placement may book that can hold the set: one that holds a slot
/// of it, or has a free slot. While its control journal `control` cannot be
/// read: the size in force or the slots `booked`, the larger, once the set
/// holds a slot; `Some(None)`, wait, while it holds none. `None` once a
/// later term fenced it.
async fn desired_size<P: ParosProviders>(
    client: &Client<P>,
    cell: &CellSession,
    desk: &mut TenantDesk,
    (tenant, control, special): (TenantId, paros_core::journal::JournalId, bool),
    (in_force, booked): (usize, usize),
) -> Option<Option<usize>> {
    // A special tenant's `F` is the control plane's (#685), from the
    // registry this term holds: its control journal has no description.
    let read = if special {
        let registry = cell.registry();
        Ok(registry
            .control_plane_mode()
            .desired(registry.storage_machines()))
    } else {
        desk.tenant(client, tenant, control)
            .await
            .map(paros_model::tenant::TenantControl::largest_failures)
    };
    match read {
        Ok(largest) if special => Some(Some(special_wanted(cell, desk, tenant, largest))),
        Ok(largest) => {
            // Capped at the machines placement may book, never at every
            // storage machine the desk names: a founding member the registry
            // does not hold counts there, and a size placement cannot reach
            // keeps the set in its book step for good. A full machine is no
            // more reachable, unless it holds a slot of the set already
            // (sweep recipe 122746:10910618044440912506 on
            // 8668601583785772229: a set of two over a full founder and one
            // machine with room never left the founder).
            let registry = cell.registry();
            let storage = desk.storage(registry);
            let held: BTreeSet<NodeId> = slots(registry, tenant)
                .iter()
                .map(|(_, b)| b.node)
                .collect();
            let machines = placement::machines(registry, storage.iter().copied());
            let eligible = machines
                .iter()
                .filter(|m| held.contains(&m.node) || m.booked < m.capacity)
                .count();
            assert!(
                eligible <= machines.len(),
                "a set's machines are machines placement may book"
            );
            if eligible < machines.len() {
                moonpool_assertions::reachable!(
                    "coordinator: a set's size passes over a full machine"
                );
            }
            // A set that holds no slot yet runs on the founding members. It
            // is placed only once placement may book as many machines as it
            // should keep of them: a cell whose founding members are not yet
            // bookable would move it onto fewer, and then onto more one round
            // at a time (seed 17573877990299474282 on c88d808: three founders
            // not yet bookable, the set booked onto the one admitted machine,
            // that machine drained, and the set left it past the settle
            // bound).
            let keep = set_size(largest, storage.len()).min(in_force);
            if held.is_empty() && eligible < keep {
                return Some(None);
            }
            Some(Some(set_size(largest, eligible)))
        }
        Err(JournalAnswer::NotCoordinator) => None,
        // An unreadable control journal never shrinks the set: a set a
        // frozen generation thinned keeps the size it was booked for. A set
        // that holds no slot yet waits for a read: a guess below its modes'
        // size booked a slot the next read could never hand over to.
        Err(_) if booked == 0 => Some(None),
        Err(_) => Some(Some(in_force.max(booked))),
    }
}

/// The steps that book `tenant`'s set up to `size` slots beside `staying`,
/// on the cell's storage machines but the `gone` ones. A draining machine
/// is not registered: it takes no new slot, and neither does one gone
/// (#475 (re-place after the Down bound)). `None` when no machine can take
/// a slot.
fn gone_plan(
    registry: &Registry,
    desk: &TenantDesk,
    (tenant, gone, founders): (TenantId, &Leaving, &BTreeSet<NodeId>),
    staying: &[(u64, Booking)],
    size: usize,
) -> Option<Vec<Step>> {
    assert!(staying.len() < size, "a plan books a set that lacks slots");
    let want = Want {
        role: Role::Matchmaker,
        target: set_of(tenant),
        count: size,
    };
    let storage = desk.storage(registry);
    // A draining, gone (#475) or slow (#361 (slow machines)) machine takes
    // no new slot: the machines a set may go to are the ones whose slots
    // stay, so a set never books a machine it is leaving (the registry
    // refuses the duplicate booking, and the refused grant ends the term).
    let machines: Vec<placement::Machine> = placement::machines(registry, storage.iter().copied())
        .into_iter()
        .filter(|m| stays(registry, gone, m.node))
        // A special tenant's set keeps its founding members beside its
        // slots (#685): it books none on them.
        .filter(|m| !founders.contains(&m.node))
        .collect();
    let steps = placement::plan(&want, &machines, staying, tenant.0).ok()?;
    assert!(
        placement::booked(&steps)
            .iter()
            .all(|node| stays(registry, gone, *node)),
        "a set never books a machine it is leaving"
    );
    Some(steps)
}

/// The set in force of `journal`'s tenant as the founding members of
/// `nodes` believe it for `journal`: the answer with the latest generation,
/// and which server gave it.
async fn set_in_force<P: ParosProviders>(
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
    pub(crate) fn new(ask_again: bool) -> Self {
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
    pub(crate) async fn step<P: ParosProviders>(
        &mut self,
        (providers, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        gone: &Leaving,
    ) -> Option<SetStep> {
        let system = system_tenants(cell.journals());
        let Some(tenant) = next_set(cell.registry(), gone, (&self.settled, &system), self.last)
        else {
            return Some(SetStep::Idle);
        };
        self.last = Some(tenant);
        let registry = cell.registry();
        let special = system.contains_key(&tenant);
        let Some(control) = system
            .get(&tenant)
            .map(|journal| journal.journal)
            .or_else(|| registry.hosted_tenant(tenant).map(|h| h.control))
        else {
            return Some(SetStep::Waiting);
        };
        let journal = JournalIdentifier::new(tenant, control);
        if special {
            moonpool_assertions::reachable!("coordinator: a special tenant's set is judged");
            // A special set born on the founding members, which are as many
            // as it wants, stays there: judged from the registry alone, with
            // no call. A call to a founding member cut off holds the watch
            // round, and the admissions `init` waits on with it (the
            // playground probe `an_isolated_machine_is_cut_then_healed`).
            let registry = cell.registry();
            let failures = registry
                .control_plane_mode()
                .desired(registry.storage_machines());
            if slots(registry, tenant).is_empty()
                && special_wanted(cell, desk, tenant, failures) <= cell.members().len()
            {
                moonpool_assertions::reachable!(
                    "coordinator: a special tenant's set stays on its founding members"
                );
                self.settled.insert(tenant);
                return Some(SetStep::Waiting);
            }
        }
        // The founding members serve every tenant's control journal: each
        // where the cell's address book says.
        let book: BTreeMap<NodeId, Address> =
            paros_model::machine::address_book(cell.founders(), registry)
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
            (tenant, control, special),
            (
                reply.matchmakers.len(),
                slots(cell.registry(), tenant).len(),
            ),
        )
        .await?;
        let Some(size) = size else {
            return Some(SetStep::Waiting);
        };
        assert!(size > 0, "a set keeps at least one matchmaker");
        if size != reply.matchmakers.len() {
            moonpool_assertions::reachable!("coordinator: a set is resized to its tenant's modes");
        }
        // A special tenant's set keeps its founding members until step 5
        // of #685 drains them: it books and hands over only the members it
        // wants beside them, so a shrink never leaves the founders (sweep
        // recipe 12137360771219473350 97859:6436796653127290360 on
        // 8441473: a one-founder cell's set grew to three, then shrank to
        // one admitted machine, and the founder never answered again).
        // Every founding member by id: the session may not know where each
        // is, and a set never leaves one it cannot dial (a session that knew
        // no founder's address handed a special set over onto nobody).
        let founders: BTreeSet<NodeId> = if special {
            cell.members().iter().copied().collect()
        } else {
            BTreeSet::new()
        };
        let size = size.saturating_sub(founders.len());
        assert!(size > 0 || special, "a hosted set keeps a matchmaker");
        let registry = cell.registry();
        let slots = slots(registry, tenant);
        let mut staying: Vec<(u64, Booking)> = slots
            .iter()
            .filter(|(_, b)| stays(registry, gone, b.node))
            .filter(|(_, b)| !founders.contains(&b.node))
            .copied()
            .collect();
        // The set lacks slots no machine can take now: it is not booked, and
        // it is handed over onto the machines that stay, a thinner set.
        let mut short = false;
        if staying.len() < size
            && gone_plan(registry, desk, (tenant, gone, &founders), &staying, size).is_none()
        {
            // With no machine to go to, the set never waits in its book
            // step (#475 (re-place after the Down bound), witness
            // 838622054030409282 on 757fa48: a set thinned off a gone
            // machine waited there for good). A slow machine's slot stays
            // (#361 (slow machines)): else the set is never handed back
            // onto it. A gone machine's slot is left all the same, as a
            // journal's is: the set is handed over onto the machines that
            // stay, and the machine takes a slot again once it answers. A
            // gone machine may be wiped and never answer as itself, and a
            // bootstrap it can never hold freezes the generation for good:
            // every journal of the tenant then waits on a GC a frozen
            // matchmaker refuses, and no move of theirs ever finishes
            // (sweep recipe `6627242282366560563
            // 263607:11592686499006116539,219039:12004839530364989954` on
            // e7ab094: two admitted machines wiped, a set of five handed
            // back onto both, a journal degraded for the whole tail).
            // Only when nothing stays is the set handed back onto every
            // registered slot, a gone one included.
            moonpool_assertions::reachable!("coordinator: a set has no machine to go to");
            short = true;
            let slow: BTreeSet<NodeId> = slots
                .iter()
                .map(|(_, b)| b.node)
                .filter(|node| registry.is_slow(*node))
                .collect();
            let kept = Leaving {
                gone: gone.gone.clone(),
                held: slow.clone(),
            };
            staying = slots
                .iter()
                .filter(|(_, b)| stays(registry, &kept, b.node))
                .copied()
                .collect();
            if staying.is_empty() {
                let kept = Leaving {
                    gone: BTreeSet::new(),
                    held: slow,
                };
                staying = slots
                    .iter()
                    .filter(|(_, b)| stays(registry, &kept, b.node))
                    .copied()
                    .collect();
            } else if staying.len() < slots.len() {
                moonpool_assertions::reachable!(
                    "coordinator: a set thins off a gone machine with nowhere to go"
                );
            }
            assert!(
                staying.iter().all(|(_, b)| registry
                    .get(b.node)
                    .is_some_and(|n| n.standing == NodeStanding::Registered)),
                "a set with no machine to go to keeps its registered slots"
            );
        }
        if staying.len() < size && !short {
            return self
                .book(
                    client,
                    cell,
                    desk,
                    (tenant, gone, &founders),
                    &staying,
                    size,
                )
                .await;
        }
        assert!(
            staying.len() >= size || short,
            "a set books only where a machine can take the slot"
        );
        let mut members = reply.matchmakers.clone();
        members.sort_unstable();
        if staying.len() > size {
            let storage = desk.storage(registry);
            // A shrink to the tenant's modes never releases the slot of a
            // machine an open maintenance window holds (#359): the window
            // keeps every slot its machine held.
            let (held, free): (Vec<_>, Vec<_>) = staying
                .iter()
                .copied()
                .partition(|(_, b)| gone.held.contains(&b.node));
            let storage: BTreeSet<NodeId> = storage
                .into_iter()
                .filter(|node| !founders.contains(node))
                .collect();
            let kept = if size == 0 {
                // A special set back to its founding members alone keeps
                // only what a window holds.
                moonpool_assertions::reachable!(
                    "coordinator: a special tenant's set shrinks to its founding members"
                );
                Some(held.iter().map(|(_, b)| b.node).collect())
            } else if held.is_empty() {
                kept(registry, &storage, tenant, (&staying, &members), size)
            } else {
                // With as many held slots as the size or more, the set keeps
                // them all, a larger set, until a window closes (sweep recipe
                // 2284108:14858352659006779586 on 12437471876011754429,
                // d404bf5: placement chose among four held slots for a set
                // of three and released one).
                let mut keep: BTreeSet<NodeId> = held.iter().map(|(_, b)| b.node).collect();
                if size > held.len() {
                    match kept(
                        registry,
                        &storage,
                        tenant,
                        (&free, &members),
                        size - held.len(),
                    ) {
                        Some(rest) => keep.extend(rest),
                        None => return Some(SetStep::Waiting),
                    }
                }
                moonpool_assertions::reachable!(
                    "coordinator: a set's shrink keeps a held machine's slot"
                );
                Some(keep)
            };
            let Some(kept) = kept else {
                return Some(SetStep::Waiting);
            };
            assert!(
                held.iter().all(|(_, b)| kept.contains(&b.node)),
                "a set's shrink keeps every slot a maintenance window holds"
            );
            assert!(
                kept.len() >= held.len(),
                "a set's shrink keeps at least its held slots"
            );
            staying.retain(|(_, b)| kept.contains(&b.node));
            if staying.len() != size.max(held.len()) {
                return Some(SetStep::Waiting);
            }
        }
        let mut target: Vec<u64> = staying
            .iter()
            .map(|(_, b)| b.node.0)
            .chain(founders.iter().map(|n| n.0))
            .collect();
        target.sort_unstable();
        target.dedup();
        assert!(
            founders.iter().all(|f| target.contains(&f.0)),
            "a special set keeps its founding members"
        );
        // A handover an earlier term started may still land on a machine
        // this release frees: the founders' belief is no fence. Release
        // only once a handover this term started is in force, even onto
        // the same members (#563 (set release races a late handover),
        // witness 11433570171281660862 on 15d04c1).
        let fenced = self
            .handed
            .get(&tenant)
            .is_some_and(|g| reply.matchmaker_generation > *g);
        if members != target || (slots.len() > staying.len() && !fenced) {
            // A set this term saw settled does not stay so: any node finishes
            // a frozen generation onto the members that answered its freeze,
            // a thinner set, and a freeze may come from a stale retry of this
            // term's own handover, accepted after the move it asked for was
            // done. Judged again in turn, the set is moved back onto its
            // booked machines (witness 5529552317995697871, PR #555 (fix
            // main's red sweep, part 2): a set settled on its three booked
            // machines, a retried handover froze it and was abandoned, and
            // another node finished it onto two).
            if members != target && self.settled.remove(&tenant) {
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
                        self.handed.insert(tenant, reply.matchmaker_generation);
                        SetStep::HandedOver
                    }
                    _ => SetStep::Waiting,
                },
            );
        }
        let step = self
            .finish(client, cell, (tenant, gone), &slots, &staying)
            .await;
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
    async fn finish<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        (tenant, gone): (TenantId, &Leaving),
        slots: &[(u64, Booking)],
        staying: &[(u64, Booking)],
    ) -> Option<SetStep> {
        let leaving: Vec<(u64, Booking)> = slots
            .iter()
            .filter(|slot| !staying.contains(slot))
            .copied()
            .collect();
        for &(booking, slot) in &leaving {
            crate::tenant::release(client, cell, |b| *b == slot)
                .await
                .ok()?;
            assert!(
                cell.registry().booking(booking).is_none(),
                "a set's old slot is released"
            );
        }
        self.settled.insert(tenant);
        self.handed.remove(&tenant);
        let started = self.started.remove(&tenant);
        let inherited = self.inherited.remove(&tenant);
        if leaving.is_empty() && !started && !inherited {
            return Some(SetStep::Waiting);
        }
        let successor = !started;
        if !leaving.is_empty() {
            moonpool_assertions::reachable!("coordinator: a set moved off a draining machine");
        }
        if leaving.iter().any(|(_, b)| gone.gone.contains(&b.node)) {
            moonpool_assertions::reachable!(
                "coordinator: a set moved off a machine down past its bound"
            );
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
    async fn book<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        (tenant, gone, founders): (TenantId, &Leaving, &BTreeSet<NodeId>),
        staying: &[(u64, Booking)],
        size: usize,
    ) -> Option<SetStep> {
        let want = Want {
            role: Role::Matchmaker,
            target: set_of(tenant),
            count: size,
        };
        let Some(steps) = gone_plan(
            cell.registry(),
            desk,
            (tenant, gone, founders),
            staying,
            size,
        ) else {
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
    fn a_special_tenants_set_is_two_f_plus_one_and_always_odd() {
        assert_eq!(special_size(0, 5), 1, "F = 0 is one matchmaker");
        assert_eq!(special_size(1, 5), 3);
        assert_eq!(special_size(1, 2), 1, "never a set of two");
        assert_eq!(special_size(2, 4), 3, "four machines hold three");
        assert_eq!(special_size(2, 5), 5);
        assert_eq!(special_size(1, 0), 1, "a cell with no room keeps one");
    }

    #[test]
    fn the_universe_tenants_set_moves_and_the_cell_tenants_stays() {
        let journal = |t, j| JournalIdentifier::new(TenantId(t), paros_core::journal::JournalId(j));
        let only_cell = system_tenants(paros_model::machine::ControlJournals {
            cell_id: 1,
            cell: journal(10, 11),
            fleet: None,
            election: Some(journal(10, 12)),
        });
        assert!(only_cell.is_empty(), "the cell tenant's set stays");
        let both = system_tenants(paros_model::machine::ControlJournals {
            cell_id: 1,
            cell: journal(10, 11),
            fleet: Some(journal(20, 21)),
            election: None,
        });
        assert_eq!(
            both.into_iter().collect::<Vec<_>>(),
            [(TenantId(20), journal(20, 21))]
        );
    }

    /// A cell of one founder and one more storage machine hosting tenant
    /// 50, whose set holds no slot yet.
    fn hosting() -> Registry {
        use paros_model::registry::{Class, GrantKey, HostedTenant, Slot, SystemCommand};
        let mut registry = Registry::new([NodeId(1)]);
        let fold = |registry: &mut Registry, command: SystemCommand| {
            let seq = registry.next_seq();
            registry.fold(seq, &command.encode());
        };
        for id in [1, 2] {
            fold(
                &mut registry,
                SystemCommand::RegisterNode {
                    id: NodeId(id),
                    addr: format!("m{id}.paros:1"),
                    class: Class::Storage,
                    capacity: 4,
                    locality: paros_model::machine::Locality::default(),
                    name: format!("m{id}"),
                    incarnation: 1,
                    machine_interface: Vec::new(),
                },
            );
        }
        fold(
            &mut registry,
            SystemCommand::JoinFleet {
                fleet_id: 7,
                cell_id: 8,
                version: paros_model::fleet::METADATA_VERSION,
                universe: None,
            },
        );
        fold(
            &mut registry,
            SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(TenantId(50)),
                tenant: TenantId(50),
                slots: Some(vec![Slot {
                    booking: 50,
                    node: NodeId(2),
                    role: Role::Acceptor,
                    target: BookingTarget::Tenant {
                        tenant: TenantId(50),
                    },
                }]),
            },
        );
        fold(
            &mut registry,
            SystemCommand::HostTenant {
                fleet_id: 7,
                tenant: TenantId(50),
                hosted: HostedTenant {
                    control: paros_core::journal::JournalId(51),
                    name: b"acme".to_vec(),
                    survives: paros_model::tenant::Survives::Az,
                },
            },
        );
        assert!(registry.hosts(TenantId(50)), "the tenant is hosted");
        registry
    }

    #[test]
    fn a_settled_special_set_with_no_slot_holds_no_hosted_set_back() {
        let registry = hosting();
        let gone = Leaving {
            gone: BTreeSet::new(),
            held: BTreeSet::new(),
        };
        let cell = TenantId(10); // a special tenant, as the universe tenant is
        let system: BTreeMap<TenantId, JournalIdentifier> = [(
            cell,
            JournalIdentifier::new(cell, paros_core::journal::JournalId(11)),
        )]
        .into_iter()
        .collect();
        // Nothing settled: both are pending, in id order.
        let none = BTreeSet::new();
        assert_eq!(
            next_set(&registry, &gone, (&none, &system), None),
            Some(cell)
        );
        // The special set settled with no slot: the hosted set's turn comes
        // every round, never the special one's.
        let special: BTreeSet<TenantId> = [cell].into_iter().collect();
        for last in [None, Some(cell), Some(TenantId(50))] {
            assert_eq!(
                next_set(&registry, &gone, (&special, &system), last),
                Some(TenantId(50))
            );
        }
        // A hosted set with no slot is unplaced, so it stays pending even
        // once seen settled; the special one never is.
        let both: BTreeSet<TenantId> = [cell, TenantId(50)].into_iter().collect();
        assert_eq!(
            next_set(&registry, &gone, (&both, &system), Some(TenantId(50))),
            Some(TenantId(50))
        );
        // With no hosted tenant, the settled special set is judged in turn.
        let empty = Registry::new([NodeId(1)]);
        assert_eq!(
            next_set(&empty, &gone, (&special, &system), Some(cell)),
            Some(cell)
        );
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
