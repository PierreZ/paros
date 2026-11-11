//! A drain (#474 (journals on admitted machines)): on its own BUGGIFY
//! location in `ADMIT`, the operator drains a registered admitted storage
//! machine through `paros::client::cell::CellSession::drain`, the code
//! `parosctl` would print. The cell coordinator then moves every journal off
//! it. The operator's session fences the coordinator's, so the term that
//! started a move often resigns, and a later term must finish it. A
//! journal may have no machine to move to: its move waits. On a
//! drained-machine seed (`crate::shape::drained_machine`), the operator
//! drains one machine at a time, only one that holds a journal's or a
//! matchmaker set's slot another storage machine could take (#501
//! (matchmaker sets off the founders)), at an `ADMIT` or right after a
//! journal create, and admits machines until then. On its own BUGGIFY
//! location, the operator drains a machine renamed since it joined (#504
//! (red drain seeds)).
//!
//! On a coin, the operator drains a machine that holds a tenant's batcher
//! (#145 (batchers)): the cell coordinator releases that slot. On a
//! batchers seed (`crate::shape::batchers`), the operator does so at every
//! journal step of the chaos window, once a read of the registry shows a
//! booked batcher, until one drain landed in the run.
//!
//! On a coin, the operator drains an initial member instead, at most one in
//! a run (#685 (special tenants are tenants) step 5).
//!
//! Once a drained machine holds no slot, the operator removes it on a coin
//! (`CellSession::remove`, #359): its `node_id` is tombstoned for good. An
//! initial member is removed only once no control journal holds it.
//!
//! On a gone-machine seed (`crate::shape::gone_machine`, #475 (re-place
//! after the Down bound)), the operator picks the machine the same way but
//! does not drain it: `crate::world::scenarios::gone_machine` takes it down for longer
//! than the re-placement bound, so the cell coordinator must re-place its
//! roles. On a coin it first opens a maintenance window over the machine
//! or its zone (#359): a short one that ends while the machine is down, or
//! one that outlasts the run, under which no slot it held may move.

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{RandomProvider, SimContext, TimeProvider, assert_reachable, buggify_with_prob};
use paros::client::cell::{CellSession, Removal};
use paros::client::checkpoint::CheckpointPolicy;
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::machine::Class;
use paros::model::registry::{BookingTarget, NodeStanding, Registry, Role};

use super::FleetOps;

/// How long the drained-machine scenario tries a drain after chaos
/// ([`FleetOps::drain_in_tail`]).
const TAIL_DRAIN: Duration = Duration::from_secs(5);

/// Well-known `StateHandle` key: a batchers seed's one drain of a batcher's
/// machine landed (#145 (batchers)).
const BATCHER_DRAINED_KEY: &str = "paros-batcher-drained";

/// Whether a drain of a batcher's machine landed in this run, on a
/// batchers seed (`crate::shape::batchers`): the scenario drains one.
pub(in crate::workload) fn batcher_drained(state: &moonpool_sim::StateHandle) -> bool {
    *crate::state::published(state, BATCHER_DRAINED_KEY, || false)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl FleetOps {
    /// Drain one registered admitted storage machine, when one is
    /// registered; with `loaded`, only one that holds a journal's slot
    /// another storage machine could take. Whether it ran.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn drain(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
        loaded: bool,
    ) -> bool {
        let Some(cell) = self.learn(ctx).await else {
            return false;
        };
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let initial_members: Vec<(NodeId, paros::model::address::Address)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let mut session = CellSession::new(
            cell.journals,
            &members,
            initial_members,
            self.leader_seeds.next(),
            policy,
        );
        let first = cell.first(draw);
        if session.open(&cell.client, first).await.is_err() {
            return false;
        }
        let admitted: Vec<NodeId> = session
            .registry()
            .nodes()
            .filter(|(id, n)| {
                !cell.members.contains(&id.0)
                    && n.standing == NodeStanding::Registered
                    && n.class.stores()
            })
            .map(|(id, _)| id)
            .collect();
        let registry = session.registry();
        // The scenario drains one machine at a time, so a journal keeps
        // machines to go to. A drained one, holding no slot, is removed on a
        // coin (#359).
        let drained: Vec<NodeId> = registry
            .nodes()
            .filter(|(id, n)| n.standing == NodeStanding::Draining && registry.booked(*id) == 0)
            .map(|(id, _)| id)
            .collect();
        if let Some(node) = drained.first().copied()
            && buggify_with_prob!(0.5)
        {
            return self
                .remove_machine(ctx, &mut session, (&cell.client, first), node)
                .await;
        }
        let draining = registry
            .nodes()
            .any(|(_, n)| n.standing == NodeStanding::Draining);
        if loaded && draining {
            return false;
        }
        let gone = crate::shape::gone_machine(ctx.state());
        if gone && crate::machine::gone_requested(ctx.state()) {
            return false;
        }
        // A scenario drain prefers a journal's slot (`prefer_journals`).
        let mut admitted: Vec<NodeId> = if loaded {
            let movable: Vec<NodeId> = admitted
                .into_iter()
                .filter(|node| movable(registry, &cell.members, *node, false))
                .collect();
            prefer_journals(registry, &cell.members, &movable)
        } else {
            admitted
        };
        // On its own location, a machine renamed since it took its slots
        // (#504 (red drain seeds), witness 14650518500742172559 on
        // e61b30e): its peers must follow its new name for its journals to
        // elect the leader a move asks to reconfigure (#454 (control plane
        // meets chaos), `readdress_peer`).
        let renamed = crate::machine::renamed_ids(ctx.state());
        if admitted.iter().any(|node| renamed.contains(&node.0)) && buggify_with_prob!(0.5) {
            admitted.retain(|node| renamed.contains(&node.0));
            assert_reachable!("drain: an operator drains a renamed machine");
        }
        if !gone
            && let Some(initial_members) = initial_members_to_drain(registry, &cell.members)
            && crate::machine::note_initial_member_drain(ctx.state(), cell.members.len())
        {
            admitted = initial_members;
        }
        if admitted.is_empty() {
            return false;
        }
        let node = admitted[usize::try_from(draw % admitted.len() as u64).unwrap_or(0)];
        if gone {
            // A window first, on a coin, then the machine goes down (#475).
            if buggify_with_prob!(0.75)
                && let Some(scope) = super::windows::scope_of(&session, node)
            {
                let for_ms = super::windows::window_ms(ctx);
                self.send_window(ctx, &mut session, (&cell.client, first), scope, for_ms)
                    .await;
            }
            if crate::machine::request_gone(ctx.state(), node.0) {
                assert_reachable!("drain: an operator picks a machine to take down");
                tracing::info!(node = node.0, "machine_gone_requested");
            }
            return true;
        }
        note_drain(ctx, session.registry(), node);
        if let Ok(true) = session.drain(&cell.client, first, node).await {
            assert_reachable!("drain: an operator drains an admitted machine");
            tracing::info!(node = node.0, "machine_drained");
        }
        true
    }

    /// The drained-machine scenario's drain after chaos (#498 (moves in the
    /// sweep)), on a drained-machine seed that does not take its machine
    /// down: try a scenario drain ([`FleetOps::drain`]) every half second
    /// for [`TAIL_DRAIN`], until one runs; when none ran, create journals
    /// of one acceptor (`FleetOps::create_in_tail`) and try again. Most of
    /// the scenario's journals
    /// land on an admitted machine late, after the clients' last steps: in
    /// 30 hunt seeds on 1689868 with the scenario on every seed, tenant
    /// journals were booked on admitted machines on 12, and no drain came
    /// after them on 11. A drain with a machine already draining, or no
    /// tenant journal another machine could take, does not run. Whether a
    /// drain ran.
    pub(in crate::workload) async fn drain_in_tail(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
    ) -> bool {
        if !crate::shape::drained_machine(ctx.state()) || crate::shape::gone_machine(ctx.state()) {
            return false;
        }
        if self.drain_until(ctx, policy).await {
            return true;
        }
        // No journal to drain yet: create some, then try again.
        self.create_in_tail(ctx, ctx.random().random_range(0..u64::MAX))
            .await;
        if self.drain_until(ctx, policy).await {
            assert_reachable!("drain: the scenario drains a journal created after chaos");
            return true;
        }
        false
    }

    /// Try a scenario drain every half second for [`TAIL_DRAIN`], until one
    /// runs. Whether one ran.
    async fn drain_until(&mut self, ctx: &SimContext, policy: CheckpointPolicy) -> bool {
        let deadline = ctx.time().now() + TAIL_DRAIN;
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            let draw = ctx.random().random_range(0..u64::MAX);
            if self.drain(ctx, policy, draw, true).await {
                assert_reachable!("drain: the scenario drains a machine after chaos");
                return true;
            }
            if ctx.time().sleep(Duration::from_millis(500)).await.is_err() {
                return false;
            }
        }
        false
    }

    /// The batchers scenario's drain (#145 (batchers)): read the registry
    /// without a session, and only when it books a tenant's batcher,
    /// drain that machine ([`FleetOps::drain_batcher`]). A session fences
    /// the coordinator's term, so an operator that opened one at every step
    /// would keep the coordinator from booking. Records a drain that
    /// landed ([`batcher_drained`]).
    pub(in crate::workload) async fn drain_booked_batcher(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        let Some(cell) = self.learn(ctx).await else {
            return;
        };
        let mut folded = super::judged_cell(&cell.members);
        let loaded = paros::client::checkpoint::load(
            &mut folded,
            cell.journals.cell,
            &cell.client,
            cell.first(draw),
            0,
        )
        .await;
        if !matches!(
            loaded,
            paros::client::checkpoint::LoadOutcome::Loaded { .. }
        ) || batcher_host(folded.state().registry()).is_none()
        {
            return;
        }
        if self.drain_batcher(ctx, policy, draw).await {
            *crate::state::published(ctx.state(), BATCHER_DRAINED_KEY, || false)
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        }
    }

    /// Drain a registered machine that holds a tenant's batcher (#145
    /// (batchers)), when one does and no machine is draining: the cell
    /// coordinator releases the slot and books the count on the machines
    /// that stay. Not on a drained-machine or gone-machine seed, which
    /// drain one machine of their own. Whether the drain landed.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn drain_batcher(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) -> bool {
        if crate::shape::drained_machine(ctx.state()) || crate::shape::gone_machine(ctx.state()) {
            return false;
        }
        let Some(cell) = self.learn(ctx).await else {
            return false;
        };
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let initial_members: Vec<(NodeId, paros::model::address::Address)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let mut session = CellSession::new(
            cell.journals,
            &members,
            initial_members,
            self.leader_seeds.next(),
            policy,
        );
        let first = cell.first(draw);
        if session.open(&cell.client, first).await.is_err() {
            return false;
        }
        let registry = session.registry();
        if registry
            .nodes()
            .any(|(_, n)| n.standing == NodeStanding::Draining)
        {
            return false;
        }
        let Some(node) = batcher_host(registry) else {
            return false;
        };
        crate::machine::note_drain_asked(ctx.state(), node.0);
        if let Ok(true) = session.drain(&cell.client, first, node).await {
            assert_reachable!("drain: an operator drains a batcher's machine");
            tracing::info!(node = node.0, "machine_drained");
            return true;
        }
        false
    }

    /// Remove the drained machine `node` (#359), through `session`. Whether
    /// it ran.
    async fn remove_machine(
        &mut self,
        ctx: &SimContext,
        session: &mut CellSession,
        (client, first): (&crate::client::ChainClient, usize),
        node: NodeId,
    ) -> bool {
        crate::machine::note_remove_asked(ctx.state(), node.0);
        match session.remove(client, first, node).await {
            Ok(Removal::Removed) => {
                assert_reachable!("drain: an operator removes a drained machine");
                tracing::info!(node = node.0, "machine_removed");
                crate::machine::note_removed(ctx.state(), node.0);
                // On a coin the operator then shuts the removed machine
                // down for good (#858 (boot from the cached registry)): a
                // removed machine answers nobody again, so nothing the cell
                // does next may rely on it.
                if buggify_with_prob!(0.5)
                    && let Some(ip) = crate::machine::ip_of(ctx.state(), node.0, |_| false)
                {
                    assert_reachable!("drain: an operator shuts a removed machine down");
                    crate::machine::note_shut_down(ctx.state(), node.0);
                    crate::lifecycle::crash(ctx, &ip).await;
                }
                true
            }
            Ok(Removal::Unchanged) => true,
            Ok(
                Removal::NotDrained { .. }
                | Removal::NotDraining
                | Removal::Unknown
                | Removal::StillMember { .. },
            )
            | Err(_) => false,
        }
    }
}

/// On a coin, the initial members `members` as the machines to drain (#685
/// (special tenants are tenants) step 5): the cell coordinator moves their
/// control journals and sets off the one drained, as any machine's. One initial
/// member at most in a run, so the clients that dial the initial members keep a
/// majority of them: `None` once an initial member left the registered ones,
/// and `None` when the others are no majority without the one drained. A cell
/// of one or two initial members never drains one: a drained initial member
/// may be removed and shut down for good, and then the operator's clients dial
/// no machine that serves the cell (witness `replay-main 6420703237330153959`
/// on 091ac5d: the one initial member was drained, removed and shut down, and
/// every read of the election journal after chaos went unanswered).
fn initial_members_to_drain(registry: &Registry, members: &[u64]) -> Option<Vec<NodeId>> {
    if members.len().saturating_sub(1) <= members.len() / 2 {
        return None;
    }
    let all_registered = members.iter().all(|id| {
        registry
            .get(NodeId(*id))
            .is_some_and(|n| n.standing == NodeStanding::Registered)
    });
    if !all_registered || !buggify_with_prob!(0.1) {
        return None;
    }
    assert_reachable!("drain: an operator drains an initial member");
    Some(members.iter().copied().map(NodeId).collect())
}

/// The machines of `admitted` that hold a tenant journal's slot another
/// storage machine could take, when one does (#498 (moves in the sweep)):
/// in 400 hunt seeds on 4bc5411, the scenario's drains moved 17 matchmaker
/// sets and finished no journal's move off a draining machine. When none
/// does, none: the drain waits for a create. A tenant control journal or a
/// cell plan journal is no tenant journal: the cell coordinator moves it
/// through `controls`, never a journal move. In 100 hunt seeds on 1689868
/// with the scenario on every seed, 11 of its 17 drains took a machine that
/// held only such journals, and the run drained no other machine (one at a
/// time). The set moves keep their own coins: the drain of an initial
/// member, and the drains off the scenario.
fn prefer_journals(registry: &Registry, members: &[u64], admitted: &[NodeId]) -> Vec<NodeId> {
    let journals: Vec<NodeId> = admitted
        .iter()
        .copied()
        .filter(|node| movable(registry, members, *node, true))
        .collect();
    if !journals.is_empty() {
        assert_reachable!("drain: a scenario drain takes a machine that holds a tenant journal");
        return journals;
    }
    Vec::new()
}

/// Whether `node` holds a journal's slot, or a matchmaker set's (#501
/// (matchmaker sets off the founders)) unless `journals_only`, that another
/// storage machine could take: an initial member (`members`) or a
/// registered storage machine. With `journals_only`, only a tenant
/// journal's slot counts ([`tenant_journal`]).
fn movable(registry: &Registry, members: &[u64], node: NodeId, journals_only: bool) -> bool {
    let storage: BTreeSet<NodeId> = members
        .iter()
        .map(|id| NodeId(*id))
        .chain(
            registry
                .nodes()
                .filter(|(_, n)| n.standing == NodeStanding::Registered && n.class.stores())
                .map(|(id, _)| id),
        )
        .collect();
    registry.bookings().any(|(_, b)| {
        let holders: BTreeSet<NodeId> = registry
            .bookings()
            .filter(|(_, o)| o.target == b.target && o.role.class() == Class::Storage)
            .map(|(_, o)| o.node)
            .collect();
        let moves = match b.target {
            BookingTarget::Journal(journal) => {
                b.role == Role::Acceptor && (!journals_only || tenant_journal(registry, journal))
            }
            BookingTarget::Set { .. } => !journals_only && b.role == Role::Matchmaker,
            BookingTarget::Tenant { .. } => false,
        };
        b.node == node && moves && storage.iter().any(|m| !holders.contains(m))
    })
}

/// Note that an operator asks to drain `node`: a drain whose answer is
/// lost may still land (#739 (heal from ground truth)), and the placement
/// oracle judges the moves of the tenant journals it holds now (#498
/// (moves in the sweep)).
fn note_drain(ctx: &SimContext, registry: &Registry, node: NodeId) {
    crate::machine::note_drain_asked(ctx.state(), node.0);
    crate::machine::note_drained_journals(ctx.state(), node.0, tenant_journals_on(registry, node));
}

/// The tenant journals ([`tenant_journal`]) with an acceptor slot on
/// `node`.
fn tenant_journals_on(registry: &Registry, node: NodeId) -> Vec<JournalIdentifier> {
    registry
        .bookings()
        .filter_map(|(_, b)| match b.target {
            BookingTarget::Journal(journal)
                if b.node == node
                    && b.role == Role::Acceptor
                    && tenant_journal(registry, journal) =>
            {
                Some(journal)
            }
            _ => None,
        })
        .collect()
}

/// Whether `journal` is a tenant journal the cell coordinator's journal
/// moves take (`paros::coordinator` `tenant::moves`): of a tenant the cell
/// hosts, not dropped nor moving to another cell, and not its tenant
/// control journal. A cell plan journal's tenant is not hosted.
fn tenant_journal(registry: &Registry, journal: JournalIdentifier) -> bool {
    registry
        .hosted_tenant(journal.tenant)
        .is_some_and(|hosted| hosted.control != journal.journal)
        && !registry.dropped(journal.tenant)
        && !registry.moving(journal.tenant)
}

/// A registered machine that holds a journal's batcher or proxy leader
/// slot, or a tenant's frontend slot (#145 (batchers), #753 (per-journal
/// proxies), #723 (frontend as a parosd role)), if any: a drain moves
/// them, which the tenant coordinator places to the counts.
fn batcher_host(registry: &Registry) -> Option<NodeId> {
    registry
        .bookings()
        .filter(|(_, b)| matches!(b.role, Role::Batcher | Role::ProxyLeader | Role::Frontend))
        .map(|(_, b)| b.node)
        .find(|node| {
            registry
                .get(*node)
                .is_some_and(|n| n.standing == NodeStanding::Registered)
        })
}

#[cfg(test)]
mod tests {
    use paros::core::NodeId;
    use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
    use paros::machine::Class;
    use paros::model::machine::Locality;
    use paros::model::registry::{
        BookingTarget, GrantKey, HostedTenant, Registry, Role, Slot, SystemCommand,
    };

    use super::{movable, prefer_journals, tenant_journal};

    const TENANT: u64 = 9;
    const CONTROL: u64 = 100;
    const DATA: u64 = 101;

    fn fold(registry: &mut Registry, command: &SystemCommand) {
        let seq = registry.next_seq();
        registry.fold(seq, &command.encode());
        assert!(registry.next_seq() > seq, "the fold reads the entry");
    }

    fn journal(id: u64) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(TENANT), JournalId(id))
    }

    /// Initial member 1 and admitted storage machines 2 to 4; the tenant's
    /// footprint is on 4, its control journal holds a slot on 2, a journal
    /// of its own on 3.
    fn registry() -> Registry {
        let mut registry = Registry::new([NodeId(1)]);
        for id in 2..=4 {
            fold(
                &mut registry,
                &SystemCommand::RegisterNode {
                    id: NodeId(id),
                    addr: format!("n{id}"),
                    class: Class::Storage,
                    locality: Locality::default(),
                    name: format!("m{id}"),
                    incarnation: u128::from(id),
                    machine_interface: Vec::new(),
                },
            );
            fold(
                &mut registry,
                &SystemCommand::MachineAdmitted { id: NodeId(id) },
            );
        }
        fold(
            &mut registry,
            &SystemCommand::JoinFleet {
                fleet_id: 7,
                cell_id: 8,
                version: paros::model::fleet::METADATA_VERSION,
                universe: None,
            },
        );
        fold(
            &mut registry,
            &SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(TenantId(TENANT)),
                tenant: TenantId(TENANT),
                slots: Some(vec![Slot {
                    booking: 1,
                    node: NodeId(4),
                    role: Role::Acceptor,
                    target: BookingTarget::Tenant {
                        tenant: TenantId(TENANT),
                    },
                }]),
            },
        );
        for (booking, (node, id)) in [(2, CONTROL), (3, DATA)].into_iter().enumerate() {
            fold(
                &mut registry,
                &SystemCommand::BookCapacity {
                    booking: 10 + booking as u64,
                    node: NodeId(node),
                    role: Role::Acceptor,
                    target: BookingTarget::Journal(journal(id)),
                },
            );
        }
        fold(
            &mut registry,
            &SystemCommand::HostTenant {
                fleet_id: 7,
                tenant: TenantId(TENANT),
                hosted: HostedTenant {
                    control: JournalId(CONTROL),
                    name: b"t".to_vec(),
                    survives: paros::model::tenant::Survives::Az,
                    members: vec![NodeId(2)],
                },
            },
        );
        assert!(registry.hosts(TenantId(TENANT)), "the tenant is hosted");
        registry
    }

    #[test]
    fn a_tenant_journal_is_hosted_and_not_its_control_journal() {
        let registry = registry();
        assert!(tenant_journal(&registry, journal(DATA)));
        assert!(
            !tenant_journal(&registry, journal(CONTROL)),
            "a control journal moves through the controls, not a journal move"
        );
        let unhosted = JournalIdentifier::new(TenantId(TENANT + 1), JournalId(DATA));
        assert!(
            !tenant_journal(&registry, unhosted),
            "a cell plan journal's tenant is not hosted"
        );
    }

    #[test]
    fn a_scenario_drain_takes_only_a_machine_that_holds_a_tenant_journal() {
        let registry = registry();
        let members = [1];
        // Machine 2 holds the control journal's slot alone: any journal
        // counts off the scenario, only a tenant journal on it.
        assert!(movable(&registry, &members, NodeId(2), false));
        assert!(!movable(&registry, &members, NodeId(2), true));
        assert!(movable(&registry, &members, NodeId(3), true));
        assert!(!movable(&registry, &members, NodeId(4), false), "no slot");
        assert_eq!(
            prefer_journals(&registry, &members, &[NodeId(2), NodeId(3), NodeId(4)]),
            vec![NodeId(3)]
        );
        assert!(
            prefer_journals(&registry, &members, &[NodeId(2), NodeId(4)]).is_empty(),
            "with no tenant journal to move, the drain waits for a create"
        );
    }
}
