//! The tenant coordinator's moves (#474 (journals on admitted machines),
//! §3.4): a journal that holds an acceptor slot on a draining machine, or on
//! one the cell coordinator holds slow (#361 (slow machines)), moves off it, one step per watch round, so every step starts from what the
//! registry and the journal hold now and any later term finishes a move an
//! earlier one started:
//!
//! 1. book a slot for the journal on another storage machine
//!    (`paros_model::placement::plan` over the registered machines, with the slots
//!    on the machines that stay as the current ones); the machine starts the
//!    journal as a spare (`paros_roles::node::system`);
//! 2. ask the journal's leader to reconfigure onto the machines that stay
//!    and the new one (`Reconfigure`, named by its journal);
//! 3. once that configuration is in force and the leader's GC watermark has
//!    reached it (every earlier configuration forgotten,
//!    `ColocatedNode::may_retire`), retire the old member with that
//!    watermark as its evidence, then release its slot. The release does
//!    not wait for the old member's answer: the watermark proves that no
//!    leader can ask it again, and a draining machine that never comes back
//!    must not hold the move forever.
//!
//! No step names the draining machine in a configuration, so the operator's
//! half of the retirement rule holds: no reconfiguration the coordinator
//! asks for above the floor names the node it retires.
//!
//! A machine the coordinator marked down and has not heard from for
//! `machine_replace_after` is left the same way (#475 (re-place after the
//! Down bound)): the watch names it gone ([`crate::cell::Watch`]), and
//! its slots move as a draining machine's do. It stays registered, so it
//! takes new slots again once it answers.
//!
//! A move books only the slots the journal's desired mode keeps (#214
//! (tenant modes)): when the machines that stay already hold as many
//! members as the mode asks for, it books none and reconfigures onto them.
//! Else a move whose cell has no machine to go to waits forever, and the
//! mode change that would release the leaving slot waits for the move.
//!
//! A slot outside the configuration in force on a journal whose machines
//! all stay (a move an earlier term started off a machine that answered
//! again before the move ended) is the mode change's to release
//! (`crate::tenant::modes`), which records the configuration in force first. A move
//! that released it would also release the slot a mode change booked
//! before its reconfiguration, at every round.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use paros_core::journal::JournalIdentifier;
use paros_core::membership::AcceptorConfig;
use paros_net::runtime::ParosProviders;

use crate::tenant::{TenantDesk, placed_count};
use paros_client::cell::CellSession;
use paros_client::journals::JournalAnswer;
use paros_client::{Client, ReconfigureOutcome, RetireOutcome, Server};
use paros_model::address::Address;
use paros_model::address::Names;
use paros_model::balance::Rebalance;
use paros_model::placement::{self, Want};
use paros_model::registry::{Booking, BookingTarget, NodeStanding, Registry, Role};
use paros_model::tenant::Desired;
use paros_net::NodeClient;
use paros_wire::common::Ballot;
use paros_wire::{InspectReply, RetireRequest};

/// What one move step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MoveStep {
    /// No journal holds a slot on a draining machine.
    Idle,
    /// A move waits: for a leader, a free machine, a configuration in force
    /// or the GC watermark.
    Waiting,
    /// A slot on a new machine was booked.
    Booked,
    /// The leader started the reconfiguration onto the new members.
    Reconfigured,
    /// The old member's slot was released: the move is done. `successor`
    /// when an earlier term started it.
    Finished {
        /// Whether this term started none of the move's steps.
        successor: bool,
    },
}

impl MoveStep {
    /// Whether the step started or advanced a move that is not done.
    pub(crate) fn in_flight(self) -> bool {
        matches!(self, Self::Booked | Self::Reconfigured)
    }
}

/// The moves of one coordinator term: the journals whose move this term
/// started a step of.
#[derive(Debug, Default)]
pub(crate) struct Moves {
    started: BTreeSet<JournalIdentifier>,
    /// The journal the last step took: the next step takes the next one,
    /// so a move that waits holds no other back.
    last: Option<JournalIdentifier>,
}

/// What a watch round says of the machines a term's steps place on.
#[derive(Clone, Debug, Default)]
pub(crate) struct Leaving {
    /// Down past the re-placement bound (#475): their roles move.
    pub(crate) gone: BTreeSet<NodeId>,
    /// Covered by an open maintenance window (#359): a slow mark (#361
    /// (slow machines)) moves none of their roles, as a down mark does not.
    pub(crate) held: BTreeSet<NodeId>,
    /// The acceptor booking the rebalancer moves off its machine (#687
    /// (rebalance a cell), `paros_model::balance`): it leaves as a
    /// draining machine's slot does, and its machine stays.
    pub(crate) rebalance: Option<Rebalance>,
}

impl Leaving {
    /// Whether booking `id` leaves for the rebalancer (#687).
    pub(crate) fn rebalances(&self, id: u64) -> bool {
        self.rebalance.is_some_and(|r| r.booking == id)
    }
}

/// Whether a slot on `node` stays where it is: the machine is registered
/// (not draining, not retired), not held slow outside an open maintenance
/// window (#361 (slow machines), #359), and not `gone` (down past the
/// re-placement bound, #475).
pub(crate) fn stays(registry: &Registry, leaving: &Leaving, node: NodeId) -> bool {
    let registered = registry
        .get(node)
        .is_some_and(|n| n.standing == NodeStanding::Registered);
    let slow = registry.is_slow(node) && !leaving.held.contains(&node);
    assert!(
        !registry.keeps_roles(node) || registered,
        "a machine that keeps its roles is registered"
    );
    registered && !slow && !leaving.gone.contains(&node)
}

/// The machines a move keeps for a journal whose leader names `members`
/// (sorted) and whose registry slots that stay are `staying`: at most `size`
/// of them, the members that stay first, then the booked spares. A mode
/// change that booked its new slots and was then handed to the move by a
/// machine that left (#361 (slow machines), #475) leaves the registry more
/// slots than the configuration in force names; the slots left over stay
/// booked for the mode change. Under a grid or a flexible split the composed
/// count must be one the quorum system admits (`AcceptorConfig::build`
/// asserts it): `size` is, and so is a smaller `staying` the caller checked.
fn move_target(members: &[u64], staying: &[(u64, Booking)], size: usize) -> Vec<u64> {
    let staying_ids: BTreeSet<u64> = staying.iter().map(|(_, b)| b.node.0).collect();
    let mut target: Vec<u64> = members
        .iter()
        .copied()
        .filter(|m| staying_ids.contains(m))
        .collect();
    target.extend(
        staying_ids
            .iter()
            .copied()
            .filter(|n| !members.contains(n))
            .take(size.saturating_sub(target.len())),
    );
    target.sort_unstable();
    target.dedup();
    assert!(
        target.len() <= size,
        "a move's target never exceeds the configuration's size"
    );
    assert!(
        target.iter().all(|n| staying_ids.contains(n)),
        "a move's target is drawn from the slots that stay"
    );
    target
}

/// The next journal after `last`, in id order and wrapping, with an
/// acceptor slot on a draining machine or one `gone`, of a tenant the cell
/// still hosts, with its slots.
fn next_move(
    registry: &Registry,
    gone: &Leaving,
    last: Option<JournalIdentifier>,
) -> Option<(JournalIdentifier, Vec<(u64, Booking)>)> {
    // A draining machine, one gone (#475) and one the coordinator holds
    // slow (#361 (slow machines)) keep no role.
    let leaving = |node: NodeId| !stays(registry, gone, node);
    let moving: BTreeSet<JournalIdentifier> = registry
        .bookings()
        .filter(|(id, b)| b.role == Role::Acceptor && (leaving(b.node) || gone.rebalances(*id)))
        .filter_map(|(_, b)| match b.target {
            // A tenant's control journal moves through
            // `crate::tenant::controls` (#685 (special tenants are tenants)).
            BookingTarget::Journal(journal)
                if !registry.dropped(journal.tenant) && !is_control(registry, journal) =>
            {
                Some(journal)
            }
            _ => None,
        })
        .collect();
    let journal = last
        .and_then(|last| moving.range((Excluded(last), Unbounded)).next())
        .or_else(|| moving.first())
        .copied()?;
    let slots = registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Acceptor && b.target == BookingTarget::Journal(journal))
        .map(|(id, b)| (id, *b))
        .collect();
    Some((journal, slots))
}

/// A journal's acceptor slots: each booking id and its booking.
type Slots = Vec<(u64, Booking)>;

/// `slots` split into the ones that stay and the ones that leave: on a
/// draining, slow or gone machine, or the booking the rebalancer moves
/// (#687 (rebalance a cell)), whose machine stays.
fn split(registry: &Registry, gone: &Leaving, slots: &[(u64, Booking)]) -> (Slots, Slots) {
    let (staying, leaving): (Slots, Slots) = slots
        .iter()
        .copied()
        .partition(|(id, b)| stays(registry, gone, b.node) && !gone.rebalances(*id));
    assert!(!leaving.is_empty(), "a move leaves a draining machine");
    assert_eq!(
        staying.len() + leaving.len(),
        slots.len(),
        "every slot stays or leaves"
    );
    if leaving.iter().any(|(id, _)| gone.rebalances(*id)) {
        moonpool_assertions::reachable!("coordinator: a move takes a rebalanced acceptor");
    }
    (staying, leaving)
}

/// Whether `journal` is the control journal of a tenant the cell hosts
/// (#685 (special tenants are tenants)).
pub(crate) fn is_control(registry: &Registry, journal: JournalIdentifier) -> bool {
    registry
        .hosted_tenant(journal.tenant)
        .is_some_and(|hosted| hosted.control == journal.journal)
}

/// The first of the `count` servers of `nodes` that answers an `Inspect` of
/// `journal` as its leader, with its answer.
pub(crate) async fn leader_of<P: ParosProviders>(
    nodes: &Client<P>,
    count: usize,
    journal: JournalIdentifier,
) -> Option<(usize, InspectReply)> {
    for target in 0..count {
        if let Some(reply) = nodes.inspect(target, journal).await
            && reply.leader
        {
            return Some((target, reply));
        }
    }
    None
}

/// Retire the old member at server `index` of `nodes` from `journal`, with
/// the GC watermark of the leader's answer `reply` as its evidence. Its
/// answer changes nothing: the watermark already proves it is forgotten.
pub(crate) async fn retire<P: ParosProviders>(
    nodes: &Client<P>,
    index: Option<usize>,
    journal: JournalIdentifier,
    reply: &InspectReply,
) {
    let Some(index) = index else { return };
    let request = RetireRequest {
        gc_watermark: reply.gc_watermark,
        tenant: journal.tenant.0,
        journal: journal.journal.0,
    };
    if nodes.retire(index, request).await == RetireOutcome::Retired {
        moonpool_assertions::reachable!("coordinator: a move retires its old member");
    }
}

/// The desired mode of `journal` and the configuration in force its
/// control journal records, when its tenant holds it live, as that journal
/// reads now; `None` when it is not live.
async fn live<P: ParosProviders>(
    client: &Client<P>,
    registry: &Registry,
    journal: JournalIdentifier,
) -> Option<(Desired, AcceptorConfig)> {
    let control = registry.hosted_tenant(journal.tenant).map(|h| h.control)?;
    paros_client::journals::list(client, journal.tenant, control)
        .await?
        .get(journal.journal)
        .filter(|j| j.deleted_at.is_none())
        .map(|j| (j.desired, j.placed.clone()))
}

/// The storage machines a move or a mode change may book for a journal of
/// `registry`, as `desk` names them: never a draining one, nor one `gone`
/// (#475).
pub(crate) fn bookable(
    registry: &Registry,
    desk: &TenantDesk,
    gone: &BTreeSet<NodeId>,
) -> Vec<placement::Machine> {
    desk.machines(registry, desk.storage(registry))
        .into_iter()
        .filter(|m| !gone.contains(&m.node))
        .collect()
}

/// The proxy and replica counts in force of the leader's answer `reply`
/// (#754 (counts off the format marker)), or `None` past the cap.
fn counts_in_force(reply: &InspectReply) -> Option<(usize, usize)> {
    paros_wire::counts_from_proto(reply.proxies, reply.replicas).ok()
}

/// Record the configuration in force of the leader's answer `reply` as
/// `journal`'s placement (`Placed`, what views show) when its tenant
/// control journal holds another. `Some(true)` once it is recorded,
/// `Some(false)` when it could not be (no tenant, a write that did not
/// land), `None` when a later term fenced the tenant control journal.
async fn record_in_force<P: ParosProviders>(
    client: &Client<P>,
    registry: &Registry,
    desk: &mut TenantDesk,
    journal: JournalIdentifier,
    reply: &InspectReply,
) -> Option<bool> {
    let Some(control) = registry.hosted_tenant(journal.tenant).map(|h| h.control) else {
        return Some(false);
    };
    let Ok(system) = paros_wire::quorum_system_from_proto(&reply.into()) else {
        return Some(false);
    };
    let mut members: Vec<NodeId> = reply.members.iter().copied().map(NodeId).collect();
    members.sort_unstable();
    if members.is_empty() || !system.admits(members.len()) {
        return Some(false);
    }
    let Some((proxies, replicas)) = counts_in_force(reply) else {
        return Some(false);
    };
    let in_force = AcceptorConfig::new(members, system).with_counts(proxies, replicas);
    let placed = match desk.tenant(client, journal.tenant, control).await {
        Ok(state) => state.get(journal.journal).map(|j| j.placed.clone()),
        Err(JournalAnswer::NotCoordinator) => return None,
        Err(_) => return Some(false),
    };
    if placed.as_ref() == Some(&in_force) {
        return Some(true);
    }
    match desk
        .record_placed(client, journal.tenant, journal.journal, in_force)
        .await
    {
        Ok(()) => {
            moonpool_assertions::reachable!("coordinator: a move records its placement");
            Some(true)
        }
        Err(JournalAnswer::NotCoordinator) => None,
        Err(_) => Some(false),
    }
}

/// Record the configuration `composed` a move shrank `journal` to as its
/// placement, once the leader's floor reached it. `None` when it is
/// recorded, else the step's answer: `Some(None)` when a later term fenced
/// the tenant control journal, `Some(Some(Waiting))` when it waits.
async fn record_shrunk<P: ParosProviders>(
    client: &Client<P>,
    cell: &CellSession,
    desk: &mut TenantDesk,
    journal: JournalIdentifier,
    reply: &InspectReply,
    composed: AcceptorConfig,
) -> Option<Option<MoveStep>> {
    if !at_or_above(reply.gc_watermark.as_ref(), reply.config_ballot.as_ref()) {
        return Some(Some(MoveStep::Waiting));
    }
    let Some(control) = cell
        .registry()
        .hosted_tenant(journal.tenant)
        .map(|h| h.control)
    else {
        return Some(Some(MoveStep::Waiting));
    };
    let recorded = match desk.tenant(client, journal.tenant, control).await {
        Ok(_) => {
            desk.record_placed(client, journal.tenant, journal.journal, composed)
                .await
        }
        Err(answer) => Err(answer),
    };
    match recorded {
        Ok(()) => {
            moonpool_assertions::reachable!(
                "coordinator: a move records the configuration it shrank to"
            );
            None
        }
        Err(JournalAnswer::NotCoordinator) => Some(None),
        Err(_) => Some(Some(MoveStep::Waiting)),
    }
}

/// Whether ballot `a` is at or above ballot `b`.
pub(crate) fn at_or_above(a: Option<&Ballot>, b: Option<&Ballot>) -> bool {
    let key = |ballot: &Ballot| (ballot.round, ballot.node);
    match (a, b) {
        (Some(a), Some(b)) => key(a) >= key(b),
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// A client of the machines that hold `slots`, each where the cell's
/// address book says it is, with their ids in the client's order. `None`
/// when the book names none of them.
fn journal_nodes<P: ParosProviders>(
    (providers, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
    cell: &CellSession,
    slots: &[(u64, Booking)],
) -> Option<(Client<P>, Vec<NodeId>)> {
    let book: BTreeMap<NodeId, Address> =
        paros_model::machine::cell_book(cell.founders(), cell.registry())
            .into_iter()
            .collect();
    let servers: Vec<(NodeId, Server<P>)> = slots
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
        return None;
    }
    let ids: Vec<NodeId> = servers.iter().map(|(id, _)| *id).collect();
    let nodes = Client::new(
        providers,
        servers.into_iter().map(|(_, s)| s).collect(),
        *client.tunables(),
    );
    assert_eq!(ids.len(), nodes.server_count(), "one id per server");
    Some((nodes, ids))
}

impl Moves {
    /// Take one step of the next move, through the term's session `cell`
    /// (its bookings drawn by `desk`), calling the journal's machines where
    /// the cell's address book says they are. A slot on a machine `gone`
    /// (down past the re-placement bound, #475) moves as a draining
    /// machine's does. `None` when a write did not land: the term writes
    /// nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn step<P: ParosProviders>(
        &mut self,
        (providers, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        gone: &Leaving,
    ) -> Option<MoveStep> {
        // An operator drains through a session of its own: the term's fold
        // learns of it only by reading the journal's tail again.
        if !cell.refresh(client, 0).await {
            return Some(MoveStep::Waiting);
        }
        let Some((journal, slots)) = next_move(cell.registry(), gone, self.last) else {
            return Some(MoveStep::Idle);
        };
        self.last = Some(journal);
        // Only a journal its tenant holds live moves: a deleted one's slots
        // are released by its tenant's next create.
        let Some((desired, placed)) = live(client, cell.registry(), journal).await else {
            moonpool_assertions::reachable!("coordinator: a move passes over a journal not live");
            return Some(MoveStep::Waiting);
        };
        let registry = cell.registry();
        let (staying, leaving) = split(registry, gone, &slots);
        if leaving.iter().any(|(_, b)| gone.gone.contains(&b.node)) {
            moonpool_assertions::reachable!(
                "coordinator: a move leaves a machine down past its bound"
            );
        }
        let Some((nodes, ids)) = journal_nodes((providers, rpc, names, client), cell, &slots)
        else {
            return Some(MoveStep::Waiting);
        };
        // The leader's view: its configuration, its GC watermark.
        let leader = leader_of(&nodes, ids.len(), journal).await;
        let Some((at, reply)) = leader else {
            return Some(MoveStep::Waiting);
        };
        let size = reply.members.len();
        let Ok(system) = paros_wire::quorum_system_from_proto(&(&reply).into()) else {
            return Some(MoveStep::Waiting);
        };
        // The machines that stay hold as many members as the desired mode
        // asks for: the move books none (the mode change shrinks the rest).
        // A machine `gone` (#475) still counts: it may answer again, and a
        // configuration shrunk off it with no spare to take its place loses
        // that member for good, so the move waits for a spare instead.
        // Seed 8716878275573457412 on 492ac54: a move shrank a journal to its
        // two staying members, attrition wiped one, and no Phase-1 quorum
        // was left.
        let storage = desk.machines(cell.registry(), desk.storage(cell.registry()));
        let wanted = placed_count(&desired, storage.len());
        let kept = staying.len() >= wanted.max(1) && system.admits(staying.len());
        if staying.len() < size && !kept {
            return self
                .book(client, cell, desk, (journal, gone), &staying, size)
                .await;
        }
        if staying.len() < size {
            moonpool_assertions::reachable!(
                "coordinator: a move books no slot its desired mode would release"
            );
        }
        let mut members = reply.members.clone();
        members.sort_unstable();
        let target = move_target(&members, &staying, size);
        if staying.len() > size {
            moonpool_assertions::reachable!(
                "coordinator: a move leaves a mode change's extra slots booked"
            );
        }
        assert!(
            system.admits(target.len()),
            "a move composes a configuration its quorum system admits"
        );
        // The machines that stay, labeled with their zones as the registry
        // names them and owned by this cell (#215 (zones)): a move composes
        // a new configuration like a create.
        let staying_nodes: Vec<NodeId> = target.iter().copied().map(NodeId).collect();
        let machines = desk.machines(cell.registry(), staying_nodes.iter().copied());
        // A move keeps the proxy and replica counts in force (#754 (counts
        // off the format marker)): only a mode change moves them.
        let Some((proxies, replicas)) = counts_in_force(&reply) else {
            return Some(MoveStep::Waiting);
        };
        let composed = placement::zoned(&staying_nodes, &machines, system)
            .with_cell(cell.journals().cell_id)
            .with_counts(proxies, replicas);
        if members != target {
            return Some(match nodes.reconfigure_to(journal, &composed, at).await {
                ReconfigureOutcome::Started { .. } => {
                    moonpool_assertions::reachable!("coordinator: a move reconfigures its journal");
                    self.started.insert(journal);
                    MoveStep::Reconfigured
                }
                _ => MoveStep::Waiting,
            });
        }
        // A move that books no slot leaves fewer slots than the control
        // journal names members: it records the configuration in force
        // first, as a mode change's last step does, so the journal never
        // holds fewer slots than its recorded members.
        if placed.members().len() > staying.len()
            && let Some(waiting) =
                record_shrunk(client, cell, desk, journal, &reply, composed).await
        {
            return waiting;
        }
        self.finish(
            client,
            (cell, desk),
            (&nodes, &ids),
            journal,
            &leaving,
            &reply,
        )
        .await
    }

    /// The last step of a move: once the leader's floor reached the
    /// configuration in force, retire each leaving member it names and
    /// release every leaving slot. `None` when a release did not land.
    async fn finish<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        (cell, desk): (&mut CellSession, &mut TenantDesk),
        (nodes, ids): (&Client<P>, &[NodeId]),
        journal: JournalIdentifier,
        leaving: &[(u64, Booking)],
        reply: &InspectReply,
    ) -> Option<MoveStep> {
        // The configuration in force names the machines that stay. Every
        // earlier configuration is forgotten once the leader's floor reached
        // this one's ballot: no leader can ask a leaving member again.
        if !at_or_above(reply.gc_watermark.as_ref(), reply.config_ballot.as_ref()) {
            return Some(MoveStep::Waiting);
        }
        // The configuration in force is recorded before a slot is released
        // (#361 (slow machines)): a slow machine is registered again once
        // it is fast, so no reader may take its released slot for a lost
        // member of the placement views show.
        if !record_in_force(client, cell.registry(), desk, journal, reply).await? {
            return Some(MoveStep::Waiting);
        }
        for &(booking, slot) in leaving {
            // A retirement acts on the leader's word: only a member its floor
            // named retirable. A floor that rose past an earlier one names
            // none, and the released slot stops the journal there all the
            // same (`driver::SystemCtx::place`).
            //
            // And only a machine that leaves for good, a draining one: a
            // slow (#361) or gone (#475) machine stays registered and takes
            // slots again, so its member is released, never retired, as a
            // mode change's is (`crate::tenant::modes`). A retired identity booked
            // again is a member lost for good. Recipe 8829326179751023212
            // 4788036:12833132764577949844 on a063911: "gc: a leader never
            // runs a grid configuration naming a retired identity".
            let draining = cell
                .registry()
                .get(slot.node)
                .is_some_and(|n| n.standing == NodeStanding::Draining);
            if !draining && reply.retirable.contains(&slot.node.0) {
                moonpool_assertions::reachable!(
                    "coordinator: a move releases a member it does not retire"
                );
            }
            if draining && reply.retirable.contains(&slot.node.0) {
                let index = ids.iter().position(|id| *id == slot.node);
                retire(nodes, index, journal, reply).await;
            }
            crate::tenant::release(client, cell, |b| *b == slot)
                .await
                .ok()?;
            assert!(
                cell.registry().booking(booking).is_none(),
                "a move's old slot is released"
            );
        }
        if leaving
            .iter()
            .any(|(_, slot)| cell.registry().is_slow(slot.node))
        {
            moonpool_assertions::reachable!(
                "coordinator: a move takes a journal off a slow machine"
            );
        }
        let successor = !self.started.remove(&journal);
        moonpool_assertions::sometimes!(true, "coordinator: a move finished");
        // A reachable, not a sometimes: the gone-machine scenario's moves
        // (#475 (re-place after the Down bound)) check it often enough that
        // a sweep with no successor finish reads as a 0% outcome; `RARE`
        // covers a sweep that never reaches it.
        if successor {
            moonpool_assertions::reachable!("coordinator: a successor term finished a move");
        }
        tracing::info!(journal = %journal, successor, "journal_moved");
        Some(MoveStep::Finished { successor })
    }

    /// Book the slots `journal` lacks on other storage machines, so its
    /// machines that stay and the new ones are `size`. `None` when a
    /// booking did not land.
    async fn book<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        (journal, gone): (JournalIdentifier, &Leaving),
        staying: &[(u64, Booking)],
        size: usize,
    ) -> Option<MoveStep> {
        let want = Want {
            role: Role::Acceptor,
            target: BookingTarget::Journal(journal),
            count: size,
        };
        // A draining or gone (#475) machine is not bookable, and a slow one
        // (#361 (slow machines)) takes no new slot: a move never books the
        // machine it leaves.
        // Nor the machine a rebalanced booking leaves (#687 (rebalance a
        // cell)): the move would land where it started.
        let from = gone
            .rebalance
            .filter(|r| r.journal == journal)
            .map(|r| r.from);
        let machines: Vec<placement::Machine> = bookable(cell.registry(), desk, &gone.gone)
            .into_iter()
            .filter(|m| stays(cell.registry(), gone, m.node) && Some(m.node) != from)
            .collect();
        // Apart from the journal's other roles (placement's rule 2).
        let current = placement::with_others(cell.registry(), (want.role, want.target), staying);
        let Ok(steps) = placement::plan(&want, &machines, &current, journal.tenant.0) else {
            moonpool_assertions::reachable!("coordinator: a move has no machine to go to");
            return Some(MoveStep::Waiting);
        };
        assert!(
            placement::booked(&steps)
                .iter()
                .all(|node| stays(cell.registry(), gone, *node)),
            "a move never books a machine it is leaving"
        );
        desk.book(client, cell, &want, &steps).await.ok()?;
        moonpool_assertions::reachable!("coordinator: a move books its journal's new slot");
        self.started.insert(journal);
        moonpool_buggify::hint!("move booked, not reconfigured").await;
        Some(MoveStep::Booked)
    }
}
