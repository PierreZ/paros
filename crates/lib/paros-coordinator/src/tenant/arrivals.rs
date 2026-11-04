//! The target's half of a **tenant move between cells** (#784 (coordinator
//! handover), §3.7): the tenant coordinator moves every journal of a tenant
//! this cell adopted onto its own machines, then the tenant's matchmaker
//! set, then writes `SettleTenant`. One step per watch round, as the
//! in-cell moves of #474 and #687 (rebalance a cell): every step starts
//! from what the registry and the journals hold now, so a later term
//! finishes what an earlier one started.
//!
//! For each journal, the control journal first, then each live one:
//!
//! 1. book as many acceptor slots on this cell's machines as the
//!    configuration in force has members; each machine starts the journal
//!    as a spare and learns the chosen slots;
//! 2. once they are warm (`move_warm_lag`, at most `move_warm_rounds`),
//!    ask the leader, still on the source's machines, to reconfigure onto
//!    them;
//! 3. once that configuration is in force, ask the leader to hand its
//!    leadership to a member here (best effort: a handoff is an
//!    optimization), then wait for the GC watermark to reach the
//!    configuration's ballot: no leader can ask a source machine again;
//! 4. record the configuration in force (`Placed`) in the tenant control
//!    journal.
//!
//! The first step that needs the tenant control journal claims it under
//! the term's uuid. The source wrote its release first and claims it no
//! more, so the claim is the fence between the two coordinators.
//!
//! Then the matchmaker set: book as many slots here as the set in force
//! has members, and hand the set over onto them, through a machine that
//! serves the tenant control journal. Once the set in force is those
//! machines, the move settles.
//!
//! The source's slots are the source's: it releases them once the operator
//! flipped the directory and it wrote `MovedTenant`
//! ([`crate::cell::moving`]).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use moonpool_rpc::RpcHandle;
use paros_client::cell::CellSession;
use paros_client::journals::JournalAnswer;
use paros_client::{Client, ReconfigureMatchmakersOutcome, ReconfigureOutcome, Server};
use paros_core::NodeId;
use paros_core::journal::{JournalIdentifier, TenantId};
use paros_model::address::{Address, Names};
use paros_model::placement::{self, Want};
use paros_model::registry::{BookingTarget, Registry, Role, SystemCommand};
use paros_net::NodeClient;
use paros_net::runtime::ParosProviders;
use paros_wire::InspectReply;

use crate::tenant::TenantDesk;
use crate::tenant::moves::{Moves, Warm, at_or_above, counts_in_force, leader_of, record_in_force};
use crate::tenant::sets::{set_in_force, set_of};

/// What one arrival step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArrivalStep {
    /// No tenant arrives.
    Idle,
    /// An arrival waits: for a leader, a free machine, warm members, a
    /// configuration in force or the GC watermark.
    Waiting,
    /// Slots on this cell's machines were booked.
    Booked,
    /// A journal's leader started the reconfiguration onto them.
    Reconfigured,
    /// A machine started the handover of the set onto them.
    HandedOver,
    /// Every journal and the set run here: the move settled.
    Settled,
}

impl ArrivalStep {
    /// Whether the step started or advanced an arrival that is not done.
    pub(crate) fn in_flight(self) -> bool {
        matches!(self, Self::Booked | Self::Reconfigured | Self::HandedOver)
    }
}

/// The arrivals of one coordinator term.
#[derive(Debug, Default)]
pub(crate) struct Arrivals {
    /// The tenant the last step took: the next step takes the next one.
    last: Option<TenantId>,
    /// The warm checks of the journals' new members (#687 (rebalance a
    /// cell)).
    moves: Moves,
    /// The journals whose leader this term asked to hand its leadership to
    /// a member here: once each.
    handed: BTreeSet<JournalIdentifier>,
    /// The set generation in force when this term's last handover of each
    /// tenant's set started: no second handover until a later one is.
    sets: BTreeMap<TenantId, u64>,
}

/// The next tenant after `last`, in id order and wrapping, that arrives
/// here and is not dropped.
fn next_arrival(registry: &Registry, last: Option<TenantId>) -> Option<TenantId> {
    let arriving: BTreeSet<TenantId> = registry
        .hosted()
        .filter(|t| registry.arriving(*t).is_some() && !registry.dropped(*t))
        .collect();
    last.and_then(|last| arriving.range((Excluded(last), Unbounded)).next())
        .or_else(|| arriving.first())
        .copied()
}

/// The nodes of `slots` (this cell's acceptor or matchmaker slots of one
/// target), in id order.
fn booked(registry: &Registry, role: Role, target: BookingTarget) -> Vec<NodeId> {
    let mut nodes: Vec<NodeId> = registry
        .bookings()
        .filter(|(_, b)| b.role == role && b.target == target)
        .map(|(_, b)| b.node)
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    nodes
}

/// Whether every member `members` names is one of `here`.
fn all_here(members: &[u64], here: &[NodeId]) -> bool {
    !members.is_empty() && members.iter().all(|m| here.contains(&NodeId(*m)))
}

/// A client of every machine `tenant` uses: this cell's machines booked
/// for it, where the cell's book says they are, then the other cell's that
/// the registry names (#782 (peers of another cell)). Their ids in the
/// client's order. `None` when there is none.
fn tenant_nodes<P: ParosProviders>(
    (providers, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
    cell: &CellSession,
    tenant: TenantId,
) -> Option<(Client<P>, Vec<NodeId>)> {
    let registry = cell.registry();
    let book: BTreeMap<NodeId, Address> =
        paros_model::machine::cell_book(cell.founders(), registry)
            .into_iter()
            .collect();
    let mut targets: Vec<(NodeId, Address)> = registry
        .bookings()
        .filter(|(_, b)| b.target.tenant() == tenant)
        .filter_map(|(_, b)| book.get(&b.node).map(|a| (b.node, a.clone())))
        .collect();
    targets.sort_by_key(|(id, _)| *id);
    targets.dedup_by_key(|(id, _)| *id);
    if let Some(peers) = registry.move_peers(tenant) {
        targets.extend(peers.peers.iter().filter_map(|p| {
            Address::parse(&p.addr)
                .ok()
                .filter(|_| !book.contains_key(&p.id))
                .map(|a| (p.id, a))
        }));
    }
    if targets.is_empty() {
        return None;
    }
    let ids: Vec<NodeId> = targets.iter().map(|(id, _)| *id).collect();
    let servers = targets
        .into_iter()
        .map(|(id, addr)| Server {
            id: id.0,
            node: NodeClient::named(rpc, names.clone(), addr),
        })
        .collect();
    let nodes = Client::new(providers, servers, *client.tunables());
    assert_eq!(ids.len(), nodes.server_count(), "one id per server");
    Some((nodes, ids))
}

impl Arrivals {
    /// The arrivals of a new term, whose new members warm to `warm`.
    pub(crate) fn new(warm: Warm) -> Self {
        Self {
            moves: Moves::new(warm),
            ..Self::default()
        }
    }

    /// Take one step of the next arrival, through the term's session
    /// `cell` (its bookings drawn by `desk`). `None` when a write did not
    /// land, or a later term fenced the tenant control journal: the term
    /// writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn step<P: ParosProviders>(
        &mut self,
        ctx: (&P, &RpcHandle<P>, &Names, &Client<P>),
        cell: &mut CellSession,
        desk: &mut TenantDesk,
    ) -> Option<ArrivalStep> {
        let Some(tenant) = next_arrival(cell.registry(), self.last) else {
            return Some(ArrivalStep::Idle);
        };
        self.last = Some(tenant);
        let Some(control) = cell.registry().hosted_tenant(tenant).map(|h| h.control) else {
            return Some(ArrivalStep::Waiting);
        };
        let Some((nodes, ids)) = tenant_nodes(ctx, cell, tenant) else {
            return Some(ArrivalStep::Waiting);
        };
        let control = JournalIdentifier::new(tenant, control);
        // The control journal first: it holds the list of the others.
        let step = self
            .journal(ctx.3, cell, desk, (&nodes, &ids), (control, true))
            .await?;
        if step != ArrivalStep::Settled {
            return Some(step);
        }
        let live: Vec<JournalIdentifier> = match desk.tenant(&nodes, tenant, control.journal).await
        {
            Ok(state) => state
                .live()
                .map(|(id, _)| JournalIdentifier::new(tenant, id))
                .collect(),
            Err(JournalAnswer::NotCoordinator) => return None,
            Err(_) => return Some(ArrivalStep::Waiting),
        };
        for journal in live {
            let step = self
                .journal(ctx.3, cell, desk, (&nodes, &ids), (journal, false))
                .await?;
            if step != ArrivalStep::Settled {
                return Some(step);
            }
        }
        let step = self.set(ctx.3, cell, desk, (&nodes, &ids), control).await?;
        if step != ArrivalStep::Settled {
            return Some(step);
        }
        let fleet_id = cell.registry().fleet()?.fleet_id;
        let settle = SystemCommand::SettleTenant { fleet_id, tenant };
        cell.record(ctx.3, 0, &settle).await.ok()?;
        assert!(
            cell.registry().arriving(tenant).is_none(),
            "a settled tenant arrives no more"
        );
        moonpool_assertions::reachable!("move: the target settles a tenant");
        tracing::info!(tenant = tenant.0, "tenant_arrived");
        Some(ArrivalStep::Settled)
    }

    /// One step of `journal`'s arrival, its leader reached through
    /// `nodes`. `Settled` once it runs here and its placement is recorded
    /// (the control journal records none of its own).
    async fn journal<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        (nodes, ids): (&Client<P>, &[NodeId]),
        (journal, is_control): (JournalIdentifier, bool),
    ) -> Option<ArrivalStep> {
        let target = BookingTarget::Journal(journal);
        let here = booked(cell.registry(), Role::Acceptor, target);
        let Some((at, reply)) = leader_of(nodes, ids.len(), journal).await else {
            return Some(ArrivalStep::Waiting);
        };
        let mut members = reply.members.clone();
        members.sort_unstable();
        let size = members.len();
        if all_here(&members, &here) {
            return self
                .arrived(
                    cell,
                    desk,
                    (nodes, ids),
                    (journal, is_control),
                    (at, &reply),
                    &here,
                )
                .await;
        }
        if here.len() < size {
            return self
                .book(client, cell, desk, (target, journal.tenant), size, &here)
                .await;
        }
        let Ok(system) = paros_wire::quorum_system_from_proto(&(&reply).into()) else {
            return Some(ArrivalStep::Waiting);
        };
        let Some((proxies, replicas)) = counts_in_force(&reply) else {
            return Some(ArrivalStep::Waiting);
        };
        let chosen: Vec<NodeId> = here.iter().copied().take(size).collect();
        if !system.admits(chosen.len()) {
            return Some(ArrivalStep::Waiting);
        }
        let new: Vec<u64> = chosen.iter().map(|n| n.0).collect();
        if !self
            .moves
            .warm((nodes, ids), journal, (&members, &new), reply.chosen_index)
            .await
        {
            return Some(ArrivalStep::Waiting);
        }
        let machines = desk.machines(cell.registry(), chosen.iter().copied());
        let composed = placement::zoned(&chosen, &machines, system)
            .with_cell(cell.journals().cell_id)
            .with_counts(proxies, replicas);
        Some(match nodes.reconfigure_to(journal, &composed, at).await {
            ReconfigureOutcome::Started { .. } => {
                moonpool_assertions::reachable!(
                    "move: a journal reconfigures onto the target's machines"
                );
                tracing::info!(journal = %journal, "arrival_reconfigured");
                ArrivalStep::Reconfigured
            }
            _ => ArrivalStep::Waiting,
        })
    }

    /// The last steps of a journal whose configuration in force runs here:
    /// hand the leadership here once, wait for the GC watermark, then
    /// record the placement.
    async fn arrived<P: ParosProviders>(
        &mut self,
        cell: &CellSession,
        desk: &mut TenantDesk,
        (nodes, ids): (&Client<P>, &[NodeId]),
        (journal, is_control): (JournalIdentifier, bool),
        (at, reply): (usize, &InspectReply),
        here: &[NodeId],
    ) -> Option<ArrivalStep> {
        let leader = ids[at];
        if !here.contains(&leader) && self.handed.insert(journal) {
            let to = NodeId(reply.members.iter().copied().min()?);
            assert!(here.contains(&to), "a handoff goes to a member here");
            if nodes.handoff(at, journal, to).await {
                moonpool_assertions::reachable!("move: a journal's leadership comes to the target");
            }
        }
        if !at_or_above(reply.gc_watermark.as_ref(), reply.config_ballot.as_ref()) {
            return Some(ArrivalStep::Waiting);
        }
        if !is_control && !record_in_force(nodes, cell.registry(), desk, journal, reply).await? {
            return Some(ArrivalStep::Waiting);
        }
        Some(ArrivalStep::Settled)
    }

    /// Book `size` slots of `target` on this cell's storage machines,
    /// beside the ones `here` holds. `None` when the booking did not land.
    async fn book<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        (target, tenant): (BookingTarget, TenantId),
        size: usize,
        here: &[NodeId],
    ) -> Option<ArrivalStep> {
        let role = match target {
            BookingTarget::Set { .. } => Role::Matchmaker,
            _ => Role::Acceptor,
        };
        let want = Want {
            role,
            target,
            count: size,
        };
        let registry = cell.registry();
        let machines = crate::tenant::moves::bookable(registry, desk, &BTreeSet::new());
        let current: Vec<_> = registry
            .bookings()
            .filter(|(_, b)| b.role == role && b.target == target && here.contains(&b.node))
            .map(|(id, b)| (id, *b))
            .collect();
        let current = placement::with_others(registry, (role, target), &current);
        let Ok(steps) = placement::plan(&want, &machines, &current, tenant.0) else {
            moonpool_assertions::reachable!("move: the target has no machine to take a slot");
            return Some(ArrivalStep::Waiting);
        };
        desk.book(client, cell, &want, &steps).await.ok()?;
        moonpool_assertions::reachable!("move: the target books a slot for an arriving tenant");
        moonpool_buggify::hint!("arrival booked, not reconfigured").await;
        Some(ArrivalStep::Booked)
    }

    /// One step of the tenant's matchmaker set's arrival: `Settled` once
    /// the set in force, as the machines serving `control` believe it, is
    /// this cell's booked machines.
    async fn set<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        cell: &mut CellSession,
        desk: &mut TenantDesk,
        (nodes, ids): (&Client<P>, &[NodeId]),
        control: JournalIdentifier,
    ) -> Option<ArrivalStep> {
        let tenant = control.tenant;
        let target = set_of(tenant);
        let here = booked(cell.registry(), Role::Matchmaker, target);
        let Some((at, reply)) = set_in_force(nodes, ids.len(), control).await else {
            return Some(ArrivalStep::Waiting);
        };
        // A deployment without matchmakers has no set to move.
        if reply.matchmakers.is_empty() || all_here(&reply.matchmakers, &here) {
            self.sets.remove(&tenant);
            return Some(ArrivalStep::Settled);
        }
        let size = reply.matchmakers.len();
        if here.len() < size {
            return self
                .book(client, cell, desk, (target, tenant), size, &here)
                .await;
        }
        if self
            .sets
            .get(&tenant)
            .is_some_and(|g| reply.matchmaker_generation <= *g)
        {
            return Some(ArrivalStep::Waiting);
        }
        let members: Vec<u64> = here.iter().take(size).map(|n| n.0).collect();
        Some(
            match nodes.reconfigure_matchmakers(tenant, &members, at).await {
                ReconfigureMatchmakersOutcome::Started { .. } => {
                    moonpool_assertions::reachable!(
                        "move: a set hands over onto the target's machines"
                    );
                    self.sets.insert(tenant, reply.matchmaker_generation);
                    ArrivalStep::HandedOver
                }
                _ => ArrivalStep::Waiting,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_runs_here_only_when_every_member_is_here() {
        let here = [NodeId(21), NodeId(22), NodeId(23)];
        assert!(all_here(&[21, 22, 23], &here));
        assert!(all_here(&[22], &here));
        assert!(!all_here(&[1, 21, 22], &here), "a source member is left");
        assert!(!all_here(&[], &here), "no configuration runs nowhere");
    }

    #[test]
    fn an_arrival_takes_the_tenants_in_turn() {
        use paros_model::checkpoint::Checkpointable as _;
        let mut registry = Registry::new([NodeId(1)]);
        assert_eq!(next_arrival(&registry, None), None);
        registry.apply(
            0,
            &SystemCommand::JoinFleet {
                fleet_id: 5,
                cell_id: 7,
                version: 1,
                universe: None,
            }
            .encode(),
        );
        assert_eq!(next_arrival(&registry, None), None, "nothing arrives");
        assert!(!ArrivalStep::Settled.in_flight());
        assert!(ArrivalStep::Reconfigured.in_flight());
    }
}
