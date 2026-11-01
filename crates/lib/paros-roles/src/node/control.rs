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

use super::journals::{JournalRt, JournalStores, Journals, boot_journal};
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
}

/// Whether a node outside a journal's bootstrap membership starts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Seat {
    /// Only a member does: a tenant control journal, served by the founding
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
                // The cell hosts a tenant (#210): every founding member
                // serves its control journal (the placement until #212).
                SystemEvent::Registry(RegistryEvent::TenantHosted { tenant, control }) => {
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
                // it). A founding member that moved before its first
                // registration landed moves with a `Registered` (#543): an
                // admitted machine opened its lane from the admission's book,
                // at the plan's address, and that lane follows it too.
                // Seed 15535649749772960988 (recipe
                // 2208855:8827221011598423148) on fba843a: the joiner's lane
                // to a moved founder dialed its dead address for the whole
                // run, so a `1x4` grid journal never held a Phase-1 quorum
                // again and a chosen slot stayed unapplied.
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
                    for tenant in served.into_iter().filter(|t| registry.dropped(*t)) {
                        self.stop_tenant(me, tenant);
                    }
                    for (id, node) in registry.nodes() {
                        match node.standing {
                            NodeStanding::Retired if id == me => self.retire_self(me),
                            NodeStanding::Retired => {}
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
        (admit, place)
    }
}

impl<P: ParosProviders, J: JournalStores> SystemCtx<'_, '_, P, J> {
    /// Start the tenant control journal `id` on this node: bootstrapped
    /// over the founding members, a majority, single-writer, ordered. A
    /// founding member serves it as a member from the tenant's birth; any
    /// other machine joins it as a spare once the coordinator books it a
    /// slot (#685 (special tenants are tenants)). Whether it started.
    async fn start_control(
        &mut self,
        follower: &ControlFollower<P>,
        id: JournalIdentifier,
        seat: Seat,
    ) -> bool {
        let founders = follower.founders().to_vec();
        assert!(!founders.is_empty(), "a cell has founding members");
        self.start(
            follower,
            id,
            (&founders, &[], 0),
            (
                paros_core::membership::QuorumSystem::Majority,
                paros_core::journal::WriterMode::Single,
                paros_model::tenant::StoreDurability::Ordered,
            ),
            seat,
        )
        .await
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
        let spare = !member && seat == Seat::Spare && !follower.matchmakers().is_empty();
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
            // Born with its tenant's matchmaker set (#190).
            matchmakers: follower.matchmakers().to_vec(),
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
    /// the founding members ([`stops_unbooked`]).
    /// Whether a journal started.
    pub(crate) async fn place(&mut self, follower: &ControlFollower<P>) -> bool {
        let me = follower.self_id();
        let booked: BTreeSet<JournalIdentifier> = follower
            .registry()
            .bookings()
            .filter(|(_, b)| b.node == me && b.role == paros_model::registry::Role::Acceptor)
            .filter_map(|(_, b)| match b.target {
                paros_model::registry::BookingTarget::Journal(journal) => Some(journal),
                paros_model::registry::BookingTarget::Set { .. }
                | paros_model::registry::BookingTarget::Tenant { .. } => None,
            })
            .collect();
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
        self.stores.book_sets(sets);
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
        // machine out, so no leader sends it a commit again. A founding
        // member serves it with no booking and never stops it here.
        let founder = follower.founders().contains(&me);
        let unbooked: Vec<JournalIdentifier> = held
            .into_iter()
            .filter(|journal| {
                let kind = self.placed(follower, *journal, founder);
                stops_unbooked(kind, booked.contains(journal))
            })
            .collect();
        for journal in unbooked {
            moonpool_assertions::reachable!("machine: a released booking stops its journal");
            if follower.registry().hosts_control(journal) {
                moonpool_assertions::reachable!(
                    "machine: a released booking stops a tenant control journal"
                );
            }
            self.journals.forget(journal);
            self.audit.journal_stopped(me, journal);
            tracing::info!(node = me.0, journal = %journal, "journal_released");
        }
        let mut started = false;
        for journal in booked {
            // A tenant's control journal booked here (#685 (special tenants
            // are tenants)): the coordinator grows it past the founding
            // members. It starts as a spare over the founders, the
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
                if !follower.founders().contains(&me) {
                    moonpool_assertions::reachable!(
                        "machine: an admitted machine serves a journal"
                    );
                }
                started = true;
            }
        }
        started
    }

    /// What `journal`, held here, is for [`stops_unbooked`]; `founder` when
    /// this machine is a founding member.
    fn placed(
        &self,
        follower: &ControlFollower<P>,
        journal: JournalIdentifier,
        founder: bool,
    ) -> Placed {
        if self.journals.is_control(journal) {
            Placed::CellControl
        } else if follower.registry().hosts_control(journal) {
            Placed::TenantControl { founder }
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
    /// tenants)); `founder` when this machine is a founding member.
    TenantControl { founder: bool },
    /// A journal its tenant created, live or deleted.
    Created,
    /// A journal no fold names yet: placement waits for the fold.
    Unknown,
}

/// Whether a journal this machine holds stops here because no acceptor slot
/// of it is booked here (#474). A founding member serves its tenants'
/// control journals with no booking; a machine the coordinator grew a
/// control journal onto stops it once its slot is released, as it stops a
/// created journal: the coordinator releases the slot only after a
/// reconfiguration left the machine out, so no leader commits to it again
/// and it could never catch up. Seed 889740265956596316 (recipe
/// 682041:6843300581960091633) on 88fc10a: a retired machine kept a
/// released control journal open, stuck below the chosen prefix.
pub(crate) fn stops_unbooked(kind: Placed, booked: bool) -> bool {
    !booked
        && match kind {
            Placed::CellControl | Placed::Unknown => false,
            Placed::TenantControl { founder } => !founder,
            Placed::Created => true,
        }
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
    // 140393:9830954766615262418 on 10491397258943519739: a founder's
    // control journal re-opened at generation 0 and never learned
    // generation 1, on a retired machine, so its set stayed off its booked
    // machine for good.
    let registered = follower.registry().nodes().map(|(node, _)| node);
    // The machines the cached registry fold names, too (#685 (special
    // tenants are tenants)): a one-founder cell's control journal re-opened
    // at generation 0 is refused by the founder with the set in force on
    // admitted machines, and only learns it once they are in its pool. The
    // fold that would admit them reads the registry, which waits for that
    // set (sweep seed 17599158648748538241 on b15d6f6: the founder probed
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
    for rt in journals.live.values_mut() {
        rt.node.extend_pool(&pool);
        rt.node.extend_matchmaker_pool(&matchmakers);
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
    // whatever this node still holds of it.
    let tombstoned = follower.is_some_and(|f| f.is_tombstoned(journal));
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
    use super::{Placed, stops_unbooked};

    #[test]
    fn a_booked_journal_never_stops() {
        for kind in [
            Placed::CellControl,
            Placed::TenantControl { founder: false },
            Placed::TenantControl { founder: true },
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
    fn an_unbooked_tenant_control_journal_stops_off_the_founders() {
        assert!(stops_unbooked(
            Placed::TenantControl { founder: false },
            false
        ));
        assert!(!stops_unbooked(
            Placed::TenantControl { founder: true },
            false
        ));
    }

    #[test]
    fn a_cell_control_or_unknown_journal_never_stops_here() {
        assert!(!stops_unbooked(Placed::CellControl, false));
        assert!(!stops_unbooked(Placed::Unknown, false));
    }
}
