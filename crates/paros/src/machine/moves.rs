//! The tenant coordinator's moves (#474 (journals on admitted machines),
//! §3.4): a journal that holds an acceptor slot on a draining machine moves
//! off it, one step per watch round, so every step starts from what the
//! registry and the journal hold now and any later term finishes a move an
//! earlier one started:
//!
//! 1. book a slot for the journal on another storage machine
//!    (`crate::placement::plan` over the registered machines, with the slots
//!    on the machines that stay as the current ones); the machine starts the
//!    journal as a spare (`crate::driver::system`);
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

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, NodeId};

use super::tenants::TenantDesk;
use crate::client::cell::CellSession;
use crate::client::{Client, ReconfigureOutcome, RetireOutcome, Server};
use crate::placement::{self, Want};
use crate::rpc::common::Ballot;
use crate::rpc::{InspectReply, NodeClient, RetireRequest};
use crate::system::{Booking, BookingTarget, NodeStanding, Registry, Role};
use crate::{Address, Names};

/// What one move step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MoveStep {
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
    pub(super) fn in_flight(self) -> bool {
        matches!(self, Self::Booked | Self::Reconfigured)
    }
}

/// The moves of one coordinator term: the journals whose move this term
/// started a step of.
#[derive(Debug, Default)]
pub(super) struct Moves {
    started: BTreeSet<JournalIdentifier>,
    /// The journal the last step took: the next step takes the next one,
    /// so a move that waits holds no other back.
    last: Option<JournalIdentifier>,
}

/// The next journal after `last`, in id order and wrapping, with an
/// acceptor slot on a draining machine, of a tenant the cell still hosts,
/// with its slots.
fn next_move(
    registry: &Registry,
    last: Option<JournalIdentifier>,
) -> Option<(JournalIdentifier, Vec<(u64, Booking)>)> {
    let leaving = |node: NodeId| {
        registry
            .get(node)
            .is_none_or(|n| n.standing != NodeStanding::Registered)
    };
    let moving: BTreeSet<JournalIdentifier> = registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Acceptor && leaving(b.node))
        .filter_map(|(_, b)| match b.target {
            BookingTarget::Journal(journal) if !registry.dropped(journal.tenant) => Some(journal),
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

/// The first of the `count` servers of `nodes` that answers an `Inspect` of
/// `journal` as its leader, with its answer.
async fn leader_of<P: Providers>(
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
async fn retire<P: Providers>(
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

/// Whether `journal`'s tenant holds it live, as its control journal reads
/// now.
async fn live<P: Providers>(
    client: &Client<P>,
    registry: &Registry,
    journal: JournalIdentifier,
) -> bool {
    let Some(control) = registry.hosted_tenant(journal.tenant).map(|h| h.control) else {
        return false;
    };
    crate::client::journals::list(client, journal.tenant, control)
        .await
        .is_some_and(|t| {
            t.get(journal.journal)
                .is_some_and(|j| j.deleted_at.is_none())
        })
}

/// Whether ballot `a` is at or above ballot `b`.
fn at_or_above(a: Option<&Ballot>, b: Option<&Ballot>) -> bool {
    let key = |ballot: &Ballot| (ballot.round, ballot.node);
    match (a, b) {
        (Some(a), Some(b)) => key(a) >= key(b),
        (Some(_), None) => true,
        (None, _) => false,
    }
}

impl Moves {
    /// Take one step of the next move, through the term's session `cell`
    /// (its bookings drawn by `desk`), calling the journal's machines where
    /// the cell's address book says they are. `None` when a write did not
    /// land: the term writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) async fn step<P: Providers>(
        &mut self,
        (providers, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
        cell: &mut CellSession,
        desk: &mut TenantDesk,
    ) -> Option<MoveStep> {
        // An operator drains through a session of its own: the term's fold
        // learns of it only by reading the journal's tail again.
        if !cell.refresh(client, 0).await {
            return Some(MoveStep::Waiting);
        }
        let Some((journal, slots)) = next_move(cell.registry(), self.last) else {
            return Some(MoveStep::Idle);
        };
        self.last = Some(journal);
        // Only a journal its tenant holds live moves: a deleted one's slots
        // are released by its tenant's next create.
        if !live(client, cell.registry(), journal).await {
            moonpool_assertions::reachable!("coordinator: a move passes over a journal not live");
            return Some(MoveStep::Waiting);
        }
        let registry = cell.registry();
        let staying: Vec<(u64, Booking)> = slots
            .iter()
            .filter(|(_, b)| {
                registry
                    .get(b.node)
                    .is_some_and(|n| n.standing == NodeStanding::Registered)
            })
            .copied()
            .collect();
        let leaving: Vec<(u64, Booking)> = slots
            .iter()
            .filter(|slot| !staying.contains(slot))
            .copied()
            .collect();
        assert!(!leaving.is_empty(), "a move leaves a draining machine");
        // The journal's machines, each where the cell's address book says.
        let book: BTreeMap<NodeId, Address> = super::cell_book(cell.founders(), registry)
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
            return Some(MoveStep::Waiting);
        }
        let ids: Vec<NodeId> = servers.iter().map(|(id, _)| *id).collect();
        let nodes = Client::new(
            providers,
            servers.into_iter().map(|(_, s)| s).collect(),
            *client.tunables(),
        );
        // The leader's view: its configuration, its GC watermark.
        let leader = leader_of(&nodes, ids.len(), journal).await;
        let Some((at, reply)) = leader else {
            return Some(MoveStep::Waiting);
        };
        let size = reply.members.len();
        if staying.len() < size {
            return self.book(client, cell, desk, journal, &staying, size).await;
        }
        let mut target: Vec<u64> = staying.iter().map(|(_, b)| b.node.0).collect();
        target.sort_unstable();
        let mut members = reply.members.clone();
        members.sort_unstable();
        if members != target {
            let Ok(system) = crate::rpc::quorum_system_from_proto(&(&reply).into()) else {
                return Some(MoveStep::Waiting);
            };
            return Some(
                match nodes
                    .reconfigure_journal(journal, &target, system, at)
                    .await
                {
                    ReconfigureOutcome::Started { .. } => {
                        moonpool_assertions::reachable!(
                            "coordinator: a move reconfigures its journal"
                        );
                        self.started.insert(journal);
                        MoveStep::Reconfigured
                    }
                    _ => MoveStep::Waiting,
                },
            );
        }
        self.finish(client, cell, (&nodes, &ids), journal, &leaving, &reply)
            .await
    }

    /// The last step of a move: once the leader's floor reached the
    /// configuration in force, retire each leaving member it names and
    /// release every leaving slot. `None` when a release did not land.
    async fn finish<P: Providers>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
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
        for &(booking, slot) in leaving {
            // A retirement acts on the leader's word: only a member its floor
            // named retirable. A floor that rose past an earlier one names
            // none, and the released slot stops the journal there all the
            // same (`driver::SystemCtx::place`).
            if reply.retirable.contains(&slot.node.0) {
                let index = ids.iter().position(|id| *id == slot.node);
                retire(nodes, index, journal, reply).await;
            }
            super::tenants::release(client, cell, |b| *b == slot)
                .await
                .ok()?;
            assert!(
                cell.registry().booking(booking).is_none(),
                "a move's old slot is released"
            );
        }
        let successor = !self.started.remove(&journal);
        moonpool_assertions::sometimes!(true, "coordinator: a move finished");
        moonpool_assertions::sometimes!(successor, "coordinator: a successor term finished a move");
        tracing::info!(journal = %journal, successor, "journal_moved");
        Some(MoveStep::Finished { successor })
    }

    /// Book the slots `journal` lacks on other storage machines, so its
    /// machines that stay and the new ones are `size`. `None` when a
    /// booking did not land.
    async fn book<P: Providers>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        journal: JournalIdentifier,
        staying: &[(u64, Booking)],
        size: usize,
    ) -> Option<MoveStep> {
        let want = Want {
            role: Role::Acceptor,
            target: BookingTarget::Journal(journal),
            count: size,
        };
        let storage = desk.storage(cell.registry());
        // A draining machine is not registered: it takes no new slot.
        let machines = placement::machines(cell.registry(), storage.iter().copied());
        let Ok(steps) = placement::plan(&want, &machines, staying, journal.tenant.0) else {
            moonpool_assertions::reachable!("coordinator: a move has no machine to go to");
            return Some(MoveStep::Waiting);
        };
        desk.book(client, cell, &want, &steps).await.ok()?;
        moonpool_assertions::reachable!("coordinator: a move books its journal's new slot");
        self.started.insert(journal);
        moonpool_buggify::hint!("move booked, not reconfigured").await;
        Some(MoveStep::Booked)
    }
}
