//! What a node does with the control journals' folds (#189): admission of the pool, refused and read-only journals, and opening a journal.

use std::collections::BTreeSet;

use paros_core::contract::Party;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_core::membership::MatchmakerId;
use paros_core::{Message, NodeId};
use paros_model::registry::{NodeStanding, RegistryEvent, SystemEvent};
use paros_model::tenant::TenantEvent;
use paros_net::MatchmakerClient;
use paros_net::runtime::ParosProviders;
use paros_observe::audit::Audit;
use paros_storage::boot::RunError;
use paros_tunables::DriverTunables;

use super::journals::{JournalRt, JournalStores, Journals, OwedAsk, boot_journal};
use super::matchmaking::MatchmakerLinks;
use super::system::ControlFollower;
use super::transport::{LaneOpener, Outbound, peer_target};
use crate::node::system;

/// What applying the system journals' folds (#189) touches: the node's
/// journals and their opener, the lanes, and the node-level audit.
pub(crate) struct SystemCtx<'a, 'l, P: ParosProviders, J: JournalStores> {
    pub(crate) stores: &'a mut J,
    pub(crate) journals: &'a mut Journals<J::Store, J::Audit>,
    pub(crate) now: u64,
    pub(crate) tunables: &'a DriverTunables,
    pub(crate) out: &'a Outbound,
    pub(crate) lanes: &'a LaneOpener<'l, P, J::Audit>,
    pub(crate) links: &'a MatchmakerLinks<P>,
    pub(crate) rpc: &'a moonpool_rpc::RpcHandle<P>,
    pub(crate) audit: &'a J::Audit,
    /// The cell fence, opened for a tenant move's other cell (#783 (cross
    /// the cell fence)) as the registry fold says.
    pub(crate) fence: &'a paros_net::CellFence,
}

/// Whether a node outside a journal's bootstrap membership starts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Seat {
    /// Only a member does: a tenant control journal, served by the initial
    /// members.
    Member,
    /// Any node does, as a spare: a journal placed on it (#474).
    Spare,
}

/// An event is tagged by the journal whose fold produced it: a registry
/// event by the registry, a tenant event by a tenant control journal.
pub(crate) fn assert_event_source(
    registry: JournalIdentifier,
    journal: JournalIdentifier,
    event: &SystemEvent,
) {
    match event {
        SystemEvent::Registry(_) => {
            assert!(
                journal == registry,
                "a registry event comes from the registry"
            );
        }
        SystemEvent::Tenant(_) => {
            assert!(
                journal != registry,
                "a tenant event never comes from the registry"
            );
            assert!(
                journal.tenant != registry.tenant,
                "a tenant event never comes from the cell tenant"
            );
        }
    }
}

impl<P: ParosProviders, J: JournalStores> SystemCtx<'_, '_, P, J> {
    /// Apply what `journal`'s fold moved, in position order: start a created
    /// journal naming this node, stop a tombstoned one, open a lane to an
    /// admitted node, and stop every user journal on this node's own
    /// retirement. Each event is reported first, once. Whether the
    /// registry's pool is owed to the live journals ([`admit_owed`]), and
    /// whether the journals placed here are owed a [`SystemCtx::place`]
    /// (#474), which the caller runs once every fold of the round reported
    /// its events.
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(level = "debug", skip_all, fields(node = follower.self_id().0, journal = %journal, events = events.len()))]
    pub(crate) async fn apply(
        &mut self,
        follower: &ControlFollower<P>,
        journal: JournalIdentifier,
        events: Vec<(u64, SystemEvent)>,
        checkpoints: Vec<system::FoldedCheckpoint>,
    ) -> (bool, bool) {
        let me = follower.self_id();
        self.report_checkpoints(follower, journal, checkpoints);
        // Whether the pool moved or a journal opened: every live journal
        // then admits the registry's pool (`ColocatedNode::extend_pool`,
        // refused on a journal that cannot reconfigure).
        let mut admit = false;
        // Whether a booking or a created journal moved: the journals placed
        // here are started and stopped after the events (#474).
        let mut place = false;
        for (seq, event) in events {
            assert_event_source(follower.registry_key(), journal, &event);
            self.audit.system_folded(me, journal, seq, &event);
            tracing::info!(node = me.0, journal = %journal, seq, event = ?event, "system_folded");
            match event {
                // A created journal starts where the registry books its
                // acceptor slots (#474): placed below, once both folds hold
                // it, whichever comes first.
                SystemEvent::Tenant(TenantEvent::Created { id, .. }) => {
                    // A created journal lives in the tenant whose control
                    // journal created it (#210): never in another one.
                    let id = JournalIdentifier::new(journal.tenant, id);
                    assert!(
                        id.tenant == journal.tenant,
                        "a journal is created inside its own tenant"
                    );
                    place = true;
                }
                SystemEvent::Tenant(TenantEvent::Deleted { id, .. }) => {
                    let id = JournalIdentifier::new(journal.tenant, id);
                    self.stop(me, id);
                }
                // A checkpoint of a tenant control journal: stop every
                // deleted journal; the live ones are placed below.
                SystemEvent::Tenant(TenantEvent::Checkpoint { .. }) => {
                    let deleted: Vec<JournalId> = follower
                        .tenant(journal.tenant)
                        .map(|t| {
                            t.journals()
                                .filter(|(_, j)| j.deleted_at.is_some())
                                .map(|(id, _)| id)
                                .collect()
                        })
                        .unwrap_or_default();
                    for id in deleted {
                        self.stop(me, JournalIdentifier::new(journal.tenant, id));
                    }
                    place = true;
                }
                // A slot booked or released here (#474): placed below.
                SystemEvent::Registry(
                    RegistryEvent::Booked { node, .. } | RegistryEvent::Released { node, .. },
                ) if node == me => place = true,
                // A grant that books a slot here (#476 (tenant footprint)):
                // a set's slots come as one grant, with no other event.
                SystemEvent::Registry(RegistryEvent::Granted { ref slots, .. })
                    if slots.iter().any(|slot| slot.node == me) =>
                {
                    place = true;
                }
                // The cell hosts a tenant (#210): every initial member
                // serves its control journal (the placement until #212). A
                // tenant that arrives from another cell (#784 (coordinator
                // handover)) runs on its birth cell's bootstrap, so a machine
                // here starts it as a member only when it is one of those;
                // the others start it once a slot of it is booked here.
                SystemEvent::Registry(
                    RegistryEvent::TenantHosted { tenant, control }
                    | RegistryEvent::TenantAdopted {
                        tenant, control, ..
                    },
                ) => {
                    let id = JournalIdentifier::new(tenant, control);
                    if self.start_control(follower, id, Seat::Member).await {
                        admit = true;
                    }
                    place = true;
                }
                // The cell dropped a tenant: every journal of it stops here.
                SystemEvent::Registry(RegistryEvent::TenantDropped { tenant }) => {
                    self.stop_tenant(me, tenant);
                }
                // The machines of another cell a moving tenant uses (#782
                // (peers of another cell)): a lane and a matchmaker link to
                // each, and every journal of the tenant admits them.
                SystemEvent::Registry(RegistryEvent::MovePeersSet { tenant, .. }) => {
                    self.admit_move_peers(follower, tenant);
                    admit = true;
                }
                // A tenant that moved out (#785 (moved record)): every
                // journal of it stops here. Its frozen matchmaker sets stay
                // open for the cell it lives in now.
                SystemEvent::Registry(RegistryEvent::TenantMovedOut { tenant, .. }) => {
                    moonpool_assertions::reachable!("machine: a tenant that moved out stops here");
                    self.leave_tenant(follower, tenant);
                    place = true;
                }
                SystemEvent::Registry(
                    RegistryEvent::Registered { id, .. } | RegistryEvent::Reregistered { id, .. },
                ) if id == me => {
                    self.join_spares(follower).await;
                    admit = true;
                }
                // A registered machine registered again, maybe at another
                // address (a rename, #349): a lane opened at the old one
                // follows it, matchmaker link included (#454 (control plane
                // meets chaos): witness 17483207099981818289, an admitted
                // machine renamed in chaos whose set handover never reached
                // it). An initial member that moved before its first
                // registration landed moves with a `Registered` (#543): an
                // admitted machine opened its lane from the admission's book,
                // at the plan's address, and that lane follows it too. Seed
                // 15535649749772960988 (recipe 2208855:8827221011598423148) on
                // fba843a: the joiner's lane to a moved initial member dialed
                // its dead address for the whole run, so a `1x4` grid journal
                // never held a Phase-1 quorum again and a chosen slot stayed
                // unapplied.
                SystemEvent::Registry(
                    RegistryEvent::Reregistered { id, addr, .. }
                    | RegistryEvent::Registered { id, addr, .. },
                ) => {
                    let interface = follower
                        .registry()
                        .get(id)
                        .map(|node| node.machine_interface.clone())
                        .unwrap_or_default();
                    self.readdress_peer(me, id, &addr, &interface);
                    self.admit_peer(me, id, &addr);
                    admit = true;
                }
                SystemEvent::Registry(RegistryEvent::Retired { id }) if id == me => {
                    self.retire_self(me);
                }
                // A checkpoint (#230): the fold holds the registry it names,
                // restored or verified. Everything it says is applied again —
                // a fold restored from it never saw the entries below it.
                SystemEvent::Registry(RegistryEvent::Checkpoint { .. }) => {
                    let registry = follower.registry().clone();
                    for tenant in registry.hosted() {
                        let Some(hosted) = registry.hosted_tenant(tenant) else {
                            continue;
                        };
                        let id = JournalIdentifier::new(tenant, hosted.control);
                        self.start_control(follower, id, Seat::Member).await;
                    }
                    let served: Vec<TenantId> =
                        self.journals.live.keys().map(|j| j.tenant).collect();
                    for tenant in served {
                        if registry.dropped(tenant) {
                            self.stop_tenant(me, tenant);
                        } else if registry.moved(tenant).is_some() {
                            self.leave_tenant(follower, tenant);
                        }
                    }
                    let moving: Vec<TenantId> = registry.all_move_peers().map(|(t, _)| t).collect();
                    for tenant in moving {
                        self.admit_move_peers(follower, tenant);
                    }
                    for (id, node) in registry.nodes() {
                        match node.standing {
                            NodeStanding::Retired if id == me => self.retire_self(me),
                            NodeStanding::Retired => {}
                            // An initial member that never registered has
                            // no address to dial yet (#685 (special
                            // tenants are tenants)): it is a peer from the
                            // plan, admitted here once it registers.
                            _ if node.registrations == 0 => {}
                            // A draining node takes no new work.
                            NodeStanding::Registered if id == me => {
                                self.join_spares(follower).await;
                            }
                            NodeStanding::Draining if id == me => {}
                            _ => self.admit_peer(me, id, &node.addr),
                        }
                    }
                    admit = true;
                    place = true;
                }
                _ => {}
            }
        }
        if self.fence.follow(follower.registry()) {
            moonpool_assertions::reachable!("driver: a move opens the cell fence");
        }
        (admit, place)
    }

    /// Open a lane and a matchmaker link to each machine of another cell
    /// that a move of `tenant` uses (#782 (peers of another cell)), or move
    /// them where the registry names them now. No pool admission is
    /// reported: they are not this cell's machines.
    fn admit_move_peers(&mut self, follower: &ControlFollower<P>, tenant: TenantId) {
        let me = follower.self_id();
        let Some(peers) = follower.registry().move_peers(tenant) else {
            return;
        };
        for peer in peers.peers.clone() {
            if peer.id == me {
                continue;
            }
            if self.out.has_peer(peer.id) {
                self.readdress_peer(me, peer.id, &peer.addr, &peer.machine_interface);
                continue;
            }
            match peer_target(&peer.addr) {
                Ok(addr) => {
                    self.links.add(
                        MatchmakerId(peer.id.0),
                        MatchmakerClient::named(self.rpc, self.lanes.names.clone(), addr.clone()),
                    );
                    let lane = self.lanes.open(
                        self.rpc,
                        "paros-peer-delivery",
                        addr,
                        Party::Node(peer.id),
                    );
                    self.out.add_peer(peer.id, lane);
                    moonpool_assertions::reachable!(
                        "driver: a lane opens to a machine of another cell"
                    );
                    tracing::info!(
                        node = me.0,
                        peer = peer.id.0,
                        tenant = tenant.0,
                        "move_peer_admitted"
                    );
                }
                Err(error) => {
                    tracing::warn!(node = me.0, peer = peer.id.0, %error, "move_peer_address_unusable");
                }
            }
        }
    }
}

impl<P: ParosProviders, J: JournalStores> SystemCtx<'_, '_, P, J> {
    /// Start the tenant control journal `id` on this node: bootstrapped
    /// over the machines its tenant was born on, a majority, single-writer,
    /// ordered. Each of them serves it as a member from the tenant's birth;
    /// any other machine joins it as a spare once the coordinator books it
    /// a slot (#685 (no founders)). Whether it started.
    async fn start_control(
        &mut self,
        follower: &ControlFollower<P>,
        id: JournalIdentifier,
        seat: Seat,
    ) -> bool {
        let Some(hosted) = follower.registry().hosted_tenant(id.tenant) else {
            return false;
        };
        assert!(hosted.control == id.journal, "a tenant's control journal");
        let members = hosted.members.clone();
        assert!(!members.is_empty(), "a hosted tenant is born on machines");
        self.start(
            follower,
            id,
            (&members, &[], 0),
            (
                paros_core::membership::QuorumSystem::Majority,
                paros_core::journal::WriterMode::Single,
                paros_model::tenant::StoreDurability::Ordered,
            ),
            seat,
        )
        .await
    }

    /// Start the cell plan journal of spare template `config` on this
    /// machine, outside the initial members (#685 (special tenants are
    /// tenants) step 4): unless it is served here already, or this machine
    /// takes no storage work. Whether it started.
    async fn start_planned(
        &mut self,
        follower: &ControlFollower<P>,
        config: paros_core::Config,
    ) -> bool {
        let me = follower.self_id();
        let id = config.journal;
        assert!(config.id == me, "a spare template names this node");
        assert!(
            !follower.initial_members().contains(&me),
            "an initial member serves the plan's journals from its plan"
        );
        if self.journals.serves(id) || !follower.takes_storage_work() {
            return false;
        }
        if !self.stores.create(
            id,
            config,
            paros_model::tenant::StoreDurability::Ordered.journal(),
        ) {
            return false;
        }
        open_journal(
            self.lanes.providers,
            self.stores,
            self.journals,
            id,
            self.now,
            self.tunables,
        )
        .await;
        let started = self.journals.live.contains_key(&id);
        if started {
            self.audit.journal_started(me, id);
            tracing::info!(node = me.0, journal = %id, "plan_journal_started");
        }
        started
    }

    /// Start journal `id` on this node, bootstrapped over `members` under
    /// `quorum_system`, written in `writer` mode, its store committing under
    /// `store` (#599 (durability mode per journal)) — unless the journal is
    /// gone for good, already served here, this machine takes no storage
    /// work (a `stateless` machine never serves a journal, #211), or this
    /// node is no member and `seat` takes members only. A node outside
    /// `members` joins as a spare (#474): its own id beside the bootstrap
    /// membership, which a reconfiguration then names; only a journal with
    /// matchmakers can. The configuration depends on the arguments alone, so
    /// every start of the journal here opens its store under the same one.
    /// Whether it started.
    async fn start(
        &mut self,
        follower: &ControlFollower<P>,
        id: JournalIdentifier,
        (members, domains, cell_id): (&[NodeId], &[paros_core::membership::FailureDomain], u64),
        (quorum_system, writer, store): (
            paros_core::membership::QuorumSystem,
            paros_core::journal::WriterMode,
            paros_model::tenant::StoreDurability,
        ),
        seat: Seat,
    ) -> bool {
        assert!(
            domains.is_empty() || domains.len() == members.len(),
            "a started journal labels every member or none"
        );
        let me = follower.self_id();
        let member = members.contains(&me);
        let matchmakers = follower.matchmakers_of(id.tenant);
        let spare = !member && seat == Seat::Spare && !matchmakers.is_empty();
        if !(member || spare)
            || follower.is_tombstoned(id)
            || self.journals.serves(id)
            || !follower.takes_storage_work()
        {
            return false;
        }
        let nodes = if spare {
            moonpool_assertions::reachable!("machine: a placed journal starts as a spare");
            let mut nodes = members.to_vec();
            nodes.push(me);
            nodes.sort_unstable();
            nodes.dedup();
            nodes
        } else {
            Vec::new()
        };
        let journal_config = paros_core::Config {
            peers: members.to_vec(),
            // The placed configuration's labels (#215 (zones)), bound to
            // the journal's bootstrap like its membership.
            peer_domains: domains.to_vec(),
            cell_id,
            quorum_system,
            nodes,
            writer_mode: writer,
            // Born with its tenant's matchmaker set (#190), on the machines
            // its birth booked (#685 (no founders)).
            matchmakers,
            ..paros_core::Config::new(me, id)
        };
        assert!(
            journal_config.pool().contains(&me),
            "a started journal's pool holds this node"
        );
        if !self.stores.create(id, journal_config, store.journal()) {
            return false;
        }
        open_journal(
            self.lanes.providers,
            self.stores,
            self.journals,
            id,
            self.now,
            self.tunables,
        )
        .await;
        let started = self.journals.live.contains_key(&id);
        if started {
            self.audit.journal_started(me, id);
            tracing::info!(node = me.0, journal = %id, "journal_started");
        }
        started
    }

    /// Start and stop the journals placed on this node (#474): every live
    /// journal of a hosted tenant the registry books an acceptor slot of
    /// here runs here, and every other journal such a tenant created stops
    /// here, its store kept: a later booking starts it again from there. A
    /// tenant control journal booked here (#685) follows the same rule off
    /// the initial members ([`stops_unbooked`]).
    /// Whether a journal started.
    pub(crate) async fn place(&mut self, follower: &ControlFollower<P>) -> bool {
        let me = follower.self_id();
        let booked = booked_journals(follower);
        // A journal booked here again while it awaited its retirement (a
        // released slot that lingers, or a tenant that moved out) rejoins
        // on its next beat (#1037 (rejoin)): a matchmaker that agreed to its
        // retirement check refuses every registration naming this node.
        let awaiting = self.journals.moved_out();
        for journal in &booked {
            if !(self.journals.lingers(*journal) || awaiting.contains(journal)) {
                continue;
            }
            if let Some(rt) = self.journals.live.get_mut(journal) {
                rt.owed = Some(OwedAsk::Rejoin);
            }
        }
        // A journal of a tenant that moved out and came back, booked here
        // again before it stopped, serves again ([`Self::leave_tenant`]).
        self.journals.rebook_moved_out(&booked);
        // The matchmaker sets booked here (#501 (matchmaker sets off the
        // founders)): the host opens a set of a later generation only then.
        let sets: BTreeSet<TenantId> = follower
            .registry()
            .bookings()
            .filter(|(_, b)| b.node == me && b.role == paros_model::registry::Role::Matchmaker)
            // A footprint's matchmaker slot (#476) reserves room only.
            .filter(|(_, b)| matches!(b.target, paros_model::registry::BookingTarget::Set { .. }))
            .map(|(_, b)| b.target.tenant())
            .collect();
        self.stores.book_sets(sets, follower.hosted_bootstraps());
        // A journal its tenant created (live or deleted) that is no longer
        // booked here stops: its booking was released. A store a booking
        // released while this machine was down counts too: no journal of
        // this incarnation knows it, but the machine still holds it.
        let provisioned: Vec<JournalIdentifier> = self
            .stores
            .provisioned()
            .into_iter()
            .filter(|journal| !self.journals.released(*journal))
            .collect();
        let held: BTreeSet<JournalIdentifier> = self.journals.known().chain(provisioned).collect();
        // A tenant control journal booked here (#685 (special tenants are
        // tenants)) stops the same way once its slot is released: the
        // coordinator released it only after a reconfiguration left this
        // machine out, so no leader sends it a commit again.
        // Of the plan journals' first configuration: a spare template is
        // only for a machine outside it.
        let initial = follower.initial_members().contains(&me);
        // A journal of a tenant that moved out stops on its own path
        // ([`Self::settle_lingering`]), never on a release.
        let moved_out = self.journals.moved_out();
        let unbooked: Vec<JournalIdentifier> = held
            .into_iter()
            .filter(|journal| !moved_out.contains(journal))
            .filter(|journal| {
                let kind = self.placed(follower, *journal, initial);
                stops_unbooked(kind, booked.contains(journal))
            })
            .collect();
        self.stop_unbooked(follower, &unbooked);
        let mut started = false;
        for journal in booked {
            // A cell plan journal booked here (#685 (special tenants are
            // tenants) step 4): the coordinator grows it past the initial
            // members. It starts as a spare over the initial members.
            if let Some(config) = follower.planned(journal).cloned() {
                if self.start_planned(follower, config).await {
                    moonpool_assertions::reachable!(
                        "machine: a booked cell plan journal starts as a spare"
                    );
                    started = true;
                }
                continue;
            }
            // A tenant's control journal booked here (#685 (special tenants
            // are tenants)): a machine it was born on starts it as a
            // member, any other as a spare over those machines, the
            // bootstrap every member of it shares.
            if follower.registry().hosts_control(journal) {
                if self.start_control(follower, journal, Seat::Spare).await {
                    moonpool_assertions::reachable!(
                        "machine: a booked tenant control journal starts as a spare"
                    );
                    started = true;
                }
                continue;
            }
            let Some(created) = follower
                .tenant(journal.tenant)
                .and_then(|t| t.get(journal.journal))
                .filter(|j| j.deleted_at.is_none())
                .cloned()
            else {
                continue;
            };
            let systems = (
                created.config.quorum_system(),
                created.writer,
                created.store,
            );
            if self
                .start(
                    follower,
                    journal,
                    (
                        created.config.members(),
                        created.config.domains(),
                        created.config.cell(),
                    ),
                    systems,
                    Seat::Spare,
                )
                .await
            {
                if !follower.initial_members().contains(&me) {
                    moonpool_assertions::reachable!(
                        "machine: an admitted machine serves a journal"
                    );
                }
                started = true;
            }
        }
        started
    }

    /// Stop every journal of `unbooked`: its booking here was released, or
    /// its machine's retirement dropped it. A released journal the core may
    /// not retire from yet keeps answering instead ([`Self::keeps_answering`]),
    /// and stops once it may ([`Self::settle_lingering`]).
    fn stop_unbooked(&mut self, follower: &ControlFollower<P>, unbooked: &[JournalIdentifier]) {
        self.journals.retain_lingering(unbooked);
        for &journal in unbooked {
            if self.keeps_answering(follower, journal) {
                if !self.journals.lingers(journal) {
                    moonpool_assertions::reachable!(
                        "machine: a released journal keeps answering until its floor is effective"
                    );
                    tracing::info!(node = follower.self_id().0, journal = %journal, "journal_release_deferred");
                }
                self.journals.linger(journal);
                continue;
            }
            self.release(follower, journal);
        }
        assert!(
            unbooked
                .iter()
                .all(|journal| !self.journals.serves(*journal) || self.journals.lingers(*journal)),
            "an unbooked journal is no longer served, or lingers"
        );
    }

    /// Whether `journal`, released here, must keep answering: it is live, its
    /// deployment names matchmakers, and the core refuses to retire from it
    /// on the floor it heard (`ColocatedNode::may_retire_heard`): a
    /// registration above the floor may still name this node (its own
    /// campaign's, abandoned at the matchmaking boundary when it met the
    /// reconfiguration that left it out), and every later Phase 1 needs its
    /// promise; a node that dropped the journal there left it with no leader
    /// for good (witness `3055323747627105565 515957:2164842326828835157` on
    /// 2949e4e). A retired machine answers nothing: its peers refuse it at
    /// the edge, so its journals stop at once, as before.
    fn keeps_answering(&self, follower: &ControlFollower<P>, journal: JournalIdentifier) -> bool {
        let me = follower.self_id();
        let retired = follower
            .registry()
            .get(me)
            .is_some_and(|node| node.standing == NodeStanding::Retired);
        self.journals.live.get(&journal).is_some_and(|rt| {
            lingers(
                retired,
                rt.node.config().has_matchmakers(),
                rt.node.may_retire_heard(),
            )
        })
    }

    /// Stop the released journal `journal` here for this incarnation: its
    /// store stays, and a later booking starts it again from there.
    fn release(&mut self, follower: &ControlFollower<P>, journal: JournalIdentifier) {
        let me = follower.self_id();
        // A cell plan journal grown here (#685 step 4) stops the same way:
        // a released slot, or one its retirement dropped, ends it (seed
        // 1659659991668070206 on 6241ecc: a retired machine kept the plan
        // journal open, refused by the initial member and stuck below the
        // chosen prefix).
        if follower.planned(journal).is_some() {
            moonpool_assertions::reachable!(
                "machine: a released booking stops a cell plan journal"
            );
            self.journals.forget_planned(journal);
            self.audit.journal_stopped(me, journal);
            tracing::info!(node = me.0, journal = %journal, "plan_journal_released");
            return;
        }
        moonpool_assertions::reachable!("machine: a released booking stops its journal");
        if follower.registry().hosts_control(journal) {
            moonpool_assertions::reachable!(
                "machine: a released booking stops a tenant control journal"
            );
        }
        self.journals.forget(journal);
        self.audit.journal_stopped(me, journal);
        tracing::info!(node = me.0, journal = %journal, "journal_released");
        assert!(
            !self.journals.serves(journal),
            "a released journal is no longer served"
        );
    }

    /// Stop every lingering journal the core may now retire from: the floor
    /// a followed leader's beat carried is above every registration naming
    /// this node, and its belief is bound to that floor. A journal of a
    /// tenant that moved out stops the same way, and its store goes
    /// ([`Self::leave_tenant`]). Called every tick.
    pub(crate) fn settle_lingering(&mut self, follower: &ControlFollower<P>) {
        for journal in self.journals.lingering() {
            if self.keeps_answering(follower, journal) {
                self.ask_retirement_check(journal);
                continue;
            }
            moonpool_assertions::reachable!(
                "machine: a lingering journal stops once its floor is effective"
            );
            self.note_checked_retirement(journal);
            self.release(follower, journal);
            assert!(
                !self.journals.lingers(journal),
                "a settled journal lingers no more"
            );
        }
        // A journal of a tenant that moved out goes with its store
        // ([`Self::leave_tenant`]); one no longer live answers nothing.
        for journal in self.journals.moved_out() {
            if self.keeps_answering(follower, journal) {
                self.ask_retirement_check(journal);
                continue;
            }
            moonpool_assertions::reachable!(
                "machine: a moved-out journal stops once its floor is effective"
            );
            self.note_checked_retirement(journal);
            self.drop_moved_out(follower.self_id(), journal);
            assert!(
                !self.journals.moved_out().contains(&journal),
                "a settled moved-out journal is gone"
            );
        }
    }

    /// The gate of #1002 (retire fence gaps) where a released journal
    /// retires on its own: a journal with matchmakers that stops because
    /// `may_retire_heard` holds has passed its sixth leg, the matchmakers'
    /// check. The operator's `Retire` reaches the same gate in the
    /// workload; this path is the common one.
    fn note_checked_retirement(&self, journal: JournalIdentifier) {
        let Some(rt) = self.journals.live.get(&journal) else {
            return;
        };
        if rt.node.config().has_matchmakers() && rt.node.may_retire_heard() {
            assert!(
                rt.node.gc_floor_heard().is_some(),
                "a retirement on a heard floor heard a floor"
            );
            moonpool_assertions::reachable!("gc: a node retires once the matchmakers were asked");
        }
    }

    /// Mark `journal`, which still answers, so its next beat asks the
    /// matchmakers which registrations name this node, when the floor it
    /// heard may be all it waits for but that check was not made yet
    /// (#1002 (retire fence gaps)). A journal that is not live is skipped.
    fn ask_retirement_check(&mut self, journal: JournalIdentifier) {
        let Some(rt) = self.journals.live.get_mut(&journal) else {
            return;
        };
        // A rejoin owed wins over a check.
        if rt.owed != Some(OwedAsk::Rejoin)
            && rt
                .node
                .gc_floor_heard()
                .is_some_and(|floor| rt.node.retirement_unchecked(floor))
        {
            rt.owed = Some(OwedAsk::RetirementCheck);
        }
    }

    /// What `journal`, held here, is for [`stops_unbooked`]; `initial` when
    /// this machine is of the plan journals' first configuration (an initial
    /// member), which serves them from its plan and holds no spare template.
    fn placed(
        &self,
        follower: &ControlFollower<P>,
        journal: JournalIdentifier,
        initial: bool,
    ) -> Placed {
        if follower.planned(journal).is_some() {
            assert!(!initial, "an initial member holds no spare template");
            Placed::Planned
        } else if self.journals.is_control(journal) {
            Placed::CellControl
        } else if follower.registry().hosts_control(journal) {
            Placed::TenantControl
        } else if follower
            .tenant(journal.tenant)
            .is_some_and(|t| t.get(journal.journal).is_some())
        {
            Placed::Created
        } else {
            Placed::Unknown
        }
    }

    /// Stop journal `id` here for good, and forget its store.
    fn stop(&mut self, me: NodeId, id: JournalIdentifier) {
        if self.journals.serves(id) {
            self.journals.park(id, None);
            self.audit.journal_stopped(me, id);
            tracing::info!(node = me.0, journal = %id, "journal_stopped");
        }
        self.stores.delete(id);
    }

    /// Stop every journal of `tenant` here, its control journal included.
    fn stop_tenant(&mut self, me: NodeId, tenant: TenantId) {
        let served: Vec<JournalIdentifier> = self
            .journals
            .live
            .keys()
            .copied()
            .filter(|j| j.tenant == tenant)
            .collect();
        for id in served {
            self.stop(me, id);
        }
    }

    /// Stop every journal of `tenant`, which moved to another cell (#785
    /// (moved record)), and forget its store. Unlike a dropped tenant's, its
    /// journals are not down for good: the tenant may come back, and a
    /// later booking of a slot here starts each one again, as a new spare
    /// over an empty store ([`Self::place`]). Witness `replay-recipe
    /// 8126885539797468740 810276:7248213314656309019` on 49ccb69: a tenant
    /// moved out, back, and out again; the first moved-out record parked a
    /// journal down on a machine of the cell, the third move booked that
    /// machine for it again, it never started, and the journal's
    /// reconfiguration onto it never elected a leader.
    ///
    /// A live journal the core may not retire from yet keeps answering
    /// first ([`Self::keeps_answering`]), and stops once it may
    /// ([`Self::settle_lingering`]): the move settles once the GC
    /// watermark reaches the configuration on the target's machines, but a
    /// registration above it (a campaign's belief) may still name this
    /// machine, and every later Phase 1 needs its promise. Seed
    /// 2510727159218678827 on c567e20: the source machines deleted a moved
    /// journal while a belief at round 6 above the watermark named them; its
    /// leader rebooted, every later Phase 1 waited on them, and no client
    /// call to the journal succeeded again.
    fn leave_tenant(&mut self, follower: &ControlFollower<P>, tenant: TenantId) {
        let known: Vec<JournalIdentifier> = self
            .journals
            .known()
            .filter(|j| j.tenant == tenant)
            .collect();
        for id in known {
            if self.keeps_answering(follower, id) {
                moonpool_assertions::reachable!(
                    "machine: a moved-out journal keeps answering until its floor is effective"
                );
                self.journals.move_out(id);
                continue;
            }
            self.drop_moved_out(follower.self_id(), id);
        }
        assert!(
            self.journals
                .known()
                .filter(|j| j.tenant == tenant)
                .all(|j| self.journals.moved_out().contains(&j)),
            "a moved-out tenant's journals are no longer served, or keep answering"
        );
    }

    /// Stop `id`, a journal of a tenant that moved out, here and forget its
    /// store.
    fn drop_moved_out(&mut self, me: NodeId, id: JournalIdentifier) {
        self.journals.forget(id);
        self.stores.delete(id);
        self.audit.journal_stopped(me, id);
        tracing::info!(node = me.0, journal = %id, "journal_moved_out");
        assert!(
            !self.journals.serves(id),
            "a moved-out journal is no longer served"
        );
    }

    /// Report each checkpoint `journal`'s fold met, in position order.
    fn report_checkpoints(
        &self,
        follower: &ControlFollower<P>,
        journal: JournalIdentifier,
        checkpoints: Vec<system::FoldedCheckpoint>,
    ) {
        let me = follower.self_id();
        for (seq, verified, state) in checkpoints {
            assert!(
                journal == follower.registry_key(),
                "only the registry is checkpointed"
            );
            assert!(
                state.next_seq() <= seq + 1,
                "a checkpoint's registry holds no position past it"
            );
            self.audit
                .checkpoint_folded(me, journal, seq, verified, &state);
            tracing::info!(node = me.0, journal = %journal, seq, ?verified, "checkpoint_folded");
        }
    }

    /// Node `id` registered again at `addr`: its lane, if this node has
    /// one, and its matchmaker link dial `addr` from their next batch on.
    fn readdress_peer(&mut self, me: NodeId, id: NodeId, addr: &str, interface: &[u8]) {
        assert!(id != me, "a node never readdresses itself");
        if !self.out.has_peer(id) {
            return;
        }
        match peer_target(addr) {
            Ok(target) => {
                let moved = self.out.readdress(id, target.clone(), interface.to_vec());
                assert!(moved, "a peer this node holds has a lane");
                if let Some(link) = self.links.client(MatchmakerId(id.0)) {
                    link.readdress(target, interface);
                }
                moonpool_assertions::reachable!("driver: a registered peer is readdressed");
            }
            Err(error) => {
                tracing::warn!(node = me.0, peer = id.0, %error, "registered_address_unusable");
            }
        }
    }

    /// Node `id`, registered at `addr`, is in the pool: open its peer lane
    /// (once) and report the admission.
    fn admit_peer(&mut self, me: NodeId, id: NodeId, addr: &str) {
        // This node's own registration joins spares; it is never a peer.
        assert!(id != me, "a node never admits itself as a peer");
        if !self.out.has_peer(id) {
            match peer_target(addr) {
                Ok(addr) => {
                    // A machine the registry admits may host a matchmaker
                    // set of a later generation (#501 (matchmaker sets off
                    // the founders)): this node links to it too.
                    self.links.add(
                        MatchmakerId(id.0),
                        MatchmakerClient::named(self.rpc, self.lanes.names.clone(), addr.clone()),
                    );
                    let lane =
                        self.lanes
                            .open(self.rpc, "paros-peer-delivery", addr, Party::Node(id));
                    self.out.add_peer(id, lane);
                }
                Err(error) => {
                    tracing::warn!(node = me.0, admitted = id.0, %error, "registered_address_unusable");
                }
            }
        }
        self.audit.pool_admitted(me, id);
        tracing::info!(node = me.0, admitted = id.0, "pool_admitted");
    }

    /// This node's own retirement: every user journal it serves stops,
    /// live, quarantined or down. A journal quarantined at the retirement
    /// would re-open after its quarantine and run on a retired node: the
    /// retirement drops the node's bookings with no `Released` event, so
    /// no placement stops it later.
    fn retire_self(&mut self, me: NodeId) {
        let served: Vec<JournalIdentifier> = self
            .journals
            .known()
            .filter(|journal| !self.journals.is_control(*journal))
            .collect();
        for journal in served {
            self.journals.park(journal, None);
            self.audit.journal_stopped(me, journal);
            tracing::info!(node = me.0, journal = %journal, "journal_stopped");
        }
        // A retired node serves its control journals and nothing else.
        assert!(
            self.journals
                .live
                .keys()
                .all(|j| self.journals.is_control(*j)),
            "a retired node runs no user journal"
        );
    }

    /// This node is in the pool now (#189): it joins every journal a
    /// reconfiguration may pull it into, as a spare — its own identity, the
    /// pool the registry has admitted.
    async fn join_spares(&mut self, follower: &ControlFollower<P>) {
        let me = follower.self_id();
        if !follower.takes_storage_work() {
            return;
        }
        for template in follower.spares() {
            let journal = template.journal;
            if self.journals.serves(journal) {
                continue;
            }
            let mut nodes = follower.pool();
            nodes.push(me);
            nodes.sort_unstable();
            nodes.dedup();
            let config = paros_core::Config {
                id: me,
                nodes,
                ..template.clone()
            };
            // A spare of the static plan: ordered, like every journal no
            // tenant created.
            if !self
                .stores
                .create(journal, config, paros_storage::journal::Durability::Ordered)
            {
                continue;
            }
            open_journal(
                self.lanes.providers,
                self.stores,
                self.journals,
                journal,
                self.now,
                self.tunables,
            )
            .await;
            if self.journals.live.contains_key(&journal) {
                self.audit.journal_started(me, journal);
                tracing::info!(node = me.0, journal = %journal, "spare_joined");
            }
        }
    }
}

/// What a journal held on a machine is, for [`stops_unbooked`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Placed {
    /// One of the cell's own control journals: every machine follows it.
    CellControl,
    /// A hosted tenant's control journal (#685 (special tenants are
    /// tenants)).
    TenantControl,
    /// A cell plan journal grown onto this machine outside the initial
    /// members (#685 step 4): it has a spare template here.
    Planned,
    /// A journal its tenant created, live or deleted.
    Created,
    /// A journal no fold names yet: placement waits for the fold.
    Unknown,
}

/// The journals the registry books an acceptor slot of on this node.
fn booked_journals<P: ParosProviders>(
    follower: &ControlFollower<P>,
) -> BTreeSet<JournalIdentifier> {
    follower.registry().acceptor_journals_on(follower.self_id())
}

/// Whether a journal this machine holds stops here because no acceptor slot
/// of it is booked here (#474). Every member of a tenant's control journal
/// holds a slot, the machines it was born on too (#685 (no founders)): a
/// machine stops it once its slot is released, as it stops a created
/// journal: the coordinator releases the slot only after a
/// reconfiguration left the machine out, so no leader commits to it again
/// and it could never catch up. Seed 889740265956596316 (recipe
/// 682041:6843300581960091633) on 88fc10a: a retired machine kept a
/// released control journal open, stuck below the chosen prefix.
pub(crate) fn stops_unbooked(kind: Placed, booked: bool) -> bool {
    !booked
        && match kind {
            Placed::CellControl | Placed::Unknown => false,
            Placed::TenantControl | Placed::Planned | Placed::Created => true,
        }
}

/// Whether a released journal keeps answering here instead of stopping: its
/// machine is not retired (`retired`), its deployment names matchmakers
/// (`matchmakers`: a plain journal is never released, and has no floor to
/// wait for), and the core does not yet retire from it on the floor it heard
/// (`retirable`, `ColocatedNode::may_retire_heard`). "Removed is not shut
/// down": a registration above the floor may still name this node.
pub(crate) fn lingers(retired: bool, matchmakers: bool, retirable: bool) -> bool {
    !retired && matchmakers && !retirable
}

/// Admit the registry's pool into every live journal when an admission is
/// owed, unless this node holds it late ([`paros_observe::scenario::ADMIT_LATE`],
/// #387): then it stays owed, and a later call admits it.
pub(crate) fn admit_owed<P: ParosProviders, S, A>(
    journals: &mut Journals<S, A>,
    follower: &ControlFollower<P>,
    owed: &mut Option<std::time::Duration>,
    late: bool,
    now: std::time::Duration,
) {
    let Some(since) = *owed else { return };
    if paros_observe::scenario::admit_held(late, since, now) {
        return;
    }
    admit_pool(journals, follower);
    *owed = None;
    assert!(owed.is_none(), "an admission made is no longer owed");
}

/// Every live journal admits the registry's pool (#189): a registered node
/// becomes one it follows, counts and answers. Refused by the core on a
/// journal that cannot reconfigure; retired nodes stay in (the pool is
/// grow-only) and are kept out at the edge instead. Every machine the
/// registry holds, a retired one too, is admitted to the matchmaker pool.
pub(crate) fn admit_pool<P: ParosProviders, S, A>(
    journals: &mut Journals<S, A>,
    follower: &ControlFollower<P>,
) {
    let pool = follower.pool();
    // In a cell, a machine's matchmaker is named by its node id: every
    // registered machine may host a set of a later generation (#501
    // (matchmaker sets off the founders)). A retired machine stays a
    // matchmaker of the pool: a journal re-opened from its boot pool walks
    // its set's generations from the bootstrap one, and a generation in
    // between may name a machine retired since. Sweep recipe
    // 140393:9830954766615262418 on 10491397258943519739: an initial member's
    // control journal re-opened at generation 0 and never learned
    // generation 1, on a retired machine, so its set stayed off its booked
    // machine for good.
    let registered = follower.registry().nodes().map(|(node, _)| node);
    // The machines the cached registry fold names, too (#685 (special tenants
    // are tenants)): a cell with one initial member's control journal re-opened
    // at generation 0 is refused by the initial member with the set in force on
    // admitted machines, and only learns it once they are in its pool. The fold
    // that would admit them reads the registry, which waits for that set (sweep
    // seed 17599158648748538241 on b15d6f6: the initial member probed
    // generation 0 for the rest of the run).
    let mut matchmakers: Vec<MatchmakerId> = pool
        .iter()
        .copied()
        .chain(registered)
        .chain(follower.cached().iter().copied())
        .map(|n| MatchmakerId(n.0))
        .collect();
    matchmakers.sort_unstable();
    matchmakers.dedup();
    assert!(
        pool.iter()
            .all(|n| matchmakers.binary_search(&MatchmakerId(n.0)).is_ok()),
        "every pool member is a matchmaker of the pool"
    );
    for (id, rt) in &mut journals.live {
        rt.node.extend_pool(&pool);
        rt.node.extend_matchmaker_pool(&matchmakers);
        // The machines of another cell a move of its tenant uses (#782
        // (peers of another cell)): its leader reaches them, and they it.
        if let Some(peers) = follower.registry().move_peers(id.tenant) {
            let nodes: Vec<NodeId> = peers.peers.iter().map(|p| p.id).collect();
            let mut set: Vec<MatchmakerId> = nodes.iter().map(|n| MatchmakerId(n.0)).collect();
            set.sort_unstable();
            let mut nodes = nodes;
            nodes.sort_unstable();
            rt.node.extend_pool(&nodes);
            rt.node.extend_matchmaker_pool(&set);
        }
        // A journal that can reconfigure now admits the whole registry pool.
        if rt.node.config().has_matchmakers() {
            assert!(
                pool.iter().all(|n| rt.node.pool().binary_search(n).is_ok()),
                "a reconfigurable journal admits the registry's pool"
            );
        }
    }
}

/// Whether a call naming `journal` — not live on this node — is refused as
/// unknown (`true`: answer `unknown_journal`, reported through the audit) or
/// silently left unanswered (`false`: the node serves the journal but it is
/// quarantined or down here, and the client's deadline retries elsewhere).
pub(crate) fn refuse_unknown<S, A: Audit, N: Audit, P: ParosProviders>(
    journals: &Journals<S, A>,
    follower: Option<&ControlFollower<P>>,
    journal: JournalIdentifier,
    call: &'static str,
    self_id: u64,
    audit: &N,
) -> bool {
    // A journal its tenant tombstoned (#189, #210) is unknown from here on,
    // whatever this node still holds of it. So is a journal of a tenant
    // that moved out of this cell (#785 (moved record)): its journals are
    // parked here, not tombstoned, and a parked journal is unavailable, so
    // a frontend of this cell that resolved the name before the move got
    // no answer, call after call, and never resolved the name again to
    // learn the move. Answered unknown, it drops the stale resolution and
    // the directory sends the client to the cell the tenant lives in. The
    // node's own moved-out mark counts as well as the registry's record: a
    // journal that keeps answering after its tenant left (`move_out`) is
    // still live here, and its tenant may be back in this cell, so the
    // registry no longer names the move, while no booking here started the
    // journal again. Witness (cite only): replay-main 14794430150722133609
    // on 983c7d1 (#990 (placement counts the disk) on 52f1415), a `Read` of
    // such a journal panicked on "a live journal is never refused"; green
    // with this check.
    let tombstoned = !journals.serves_clients(journal)
        || follower.is_some_and(|f| {
            f.is_tombstoned(journal) || f.registry().moved(journal.tenant).is_some()
        });
    if journal.is_set() && journals.serves(journal) && !tombstoned {
        // Unavailable, not unknown: a journal this node serves but cannot
        // answer right now is quarantined or down, never live.
        assert!(
            !journals.live.contains_key(&journal),
            "a live journal is never refused"
        );
        tracing::info!(
            node = self_id,
            journal = %journal,
            call,
            "journal_unavailable"
        );
        return false;
    }
    audit.journal_refused(NodeId(self_id), journal, call);
    tracing::info!(node = self_id, journal = %journal, call, "journal_refused");
    true
}

/// A journal a full disk quarantined re-opens **read-only** (#360 (disk
/// full), decided on 2026-10-18: a state of the live journal, not a longer
/// quarantine, so the node keeps its Phase-1 promises and its reads): from
/// the fault's tick `since` for `DriverTunables::read_only_ticks`, so the
/// quarantine before the re-open counts (a full disk holds a journal
/// unwritable for the longer of the two, not their sum), it votes in Phase 1,
/// serves reads, refuses every Phase-2 vote ([`read_only_refuses`]) and
/// never campaigns (an election timeout of 0 never fires). A write that
/// still finds the disk full quarantines it again.
pub(crate) fn enter_read_only<S, A: Audit>(
    rt: &mut JournalRt<S, A>,
    journal: JournalIdentifier,
    since: u64,
    now: u64,
) {
    assert!(
        rt.read_only.is_none(),
        "a re-opened journal starts writable"
    );
    assert!(
        since <= now,
        "a full disk took the journal before it re-opens"
    );
    // A re-opened core is a follower with no campaign open.
    assert!(!rt.node.is_leader(), "a re-opened journal does not lead");
    rt.read_only = Some(since);
    rt.back_from_full = false;
    rt.node.set_election_timeout(0);
    assert!(
        rt.node.election_timeout() == 0,
        "a read-only journal never campaigns"
    );
    moonpool_assertions::reachable!("disk full: a store re-opens read-only");
    let node = rt.node.config().id.0;
    tracing::warn!(node, journal = %journal, reason = "disk_full", "journal_read_only");
}

/// Whether a read-only journal (#360 (disk full)) refuses `msg`: a Phase-2
/// vote (`Accept`), the records a catch-up would write, and a handoff that
/// would make it lead. A refused message is dropped, as a lost one: the
/// leader's re-send reaches the node once it is writable again, and a
/// quorum of other acceptors decides meanwhile. Phase 1 (`Prepare`) and
/// reads go on.
pub(crate) fn read_only_refuses(msg: &Message) -> bool {
    matches!(
        msg,
        Message::Accept { .. } | Message::CatchUpResponse { .. } | Message::Relinquish { .. }
    )
}

/// Open `journal`'s store and boot it into `journals` (at boot, or when its
/// quarantine is over at tick `now`): live on success, back in quarantine on
/// a storage fault (its open's too, [`JournalStores::open_fault`]), down for
/// good when the store refuses to open.
pub(crate) async fn open_journal<P: ParosProviders, J: JournalStores>(
    providers: &P,
    stores: &mut J,
    journals: &mut Journals<J::Store, J::Audit>,
    journal: JournalIdentifier,
    now: u64,
    tunables: &DriverTunables,
) {
    // A batch the journal's last runtime left flying lands first: one
    // handle writes a store at a time (#613 (settle off the node loop)).
    journals.land_orphan(journal).await;
    let audit = stores.audit(journal);
    let Some((storage, boot)) = stores.open(journal).await else {
        if let Some(fault) = stores.open_fault(journal) {
            assert!(
                matches!(fault, RunError::Storage(_)),
                "an open quarantines only on a storage fault"
            );
            moonpool_assertions::reachable!(
                "driver: a journal's open is quarantined on a storage fault"
            );
            tracing::info!(journal = %journal, "journal_open_quarantined");
            journals.requarantine(journal, now, fault);
            return;
        }
        tracing::info!(journal = %journal, "journal_down");
        journals.park(journal, None);
        return;
    };
    match boot_journal(providers, storage, boot, audit, tunables).await {
        Ok(mut rt) => {
            if let Some(since) = journals.take_full(journal) {
                enter_read_only(&mut rt, journal, since, now);
            }
            journals.insert(journal, rt);
            stores.opened(journal);
        }
        Err(fault @ RunError::Storage(_)) => journals.requarantine(journal, now, fault),
        Err(fault) => journals.park(journal, Some(fault)),
    }
    // Whatever happened, the journal now sits in exactly one state.
    assert!(
        journals.serves(journal),
        "an opened journal is live, quarantined or down"
    );
}

#[cfg(test)]
mod tests {
    use super::{Placed, lingers, stops_unbooked};

    /// A released journal lingers only while the core refuses to retire on
    /// the floor it heard, on a live machine, under matchmakers.
    #[test]
    fn a_released_journal_lingers_until_the_core_retires() {
        assert!(lingers(false, true, false));
        assert!(!lingers(false, true, true), "a retirable journal stops");
        assert!(
            !lingers(true, true, false),
            "a retired machine's journal stops"
        );
        assert!(!lingers(false, false, false), "a plain journal stops");
    }

    #[test]
    fn a_booked_journal_never_stops() {
        for kind in [
            Placed::CellControl,
            Placed::TenantControl,
            Placed::Created,
            Placed::Unknown,
        ] {
            assert!(!stops_unbooked(kind, true), "{kind:?}");
        }
    }

    #[test]
    fn an_unbooked_created_journal_stops() {
        assert!(stops_unbooked(Placed::Created, false));
    }

    #[test]
    fn an_unbooked_tenant_control_journal_stops() {
        assert!(stops_unbooked(Placed::TenantControl, false));
    }

    #[test]
    fn an_unbooked_plan_journal_stops_off_the_initial_members() {
        assert!(stops_unbooked(Placed::Planned, false));
        assert!(!stops_unbooked(Placed::Planned, true));
    }

    #[test]
    fn a_cell_control_or_unknown_journal_never_stops_here() {
        assert!(!stops_unbooked(Placed::CellControl, false));
        assert!(!stops_unbooked(Placed::Unknown, false));
    }
}
