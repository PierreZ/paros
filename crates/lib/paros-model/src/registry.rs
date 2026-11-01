//! The **cell control journal** (#189, #211): the node registry, the cell
//! tenant's control journal (#235). A service must add and retire machines
//! while it runs; paros already has the right tool — a replicated log — so
//! the cell's machines and their roles are a journal of their own, and every
//! node learns them by reading it. A tenant's journals live in its own
//! control journal ([`crate::tenant`], #210).
//!
//! This module is the one reading of its entries: the typed
//! [`SystemCommand`] a writer appends (one record per position, encoded by
//! [`SystemCommand::encode`]), and the pure fold [`Registry`] that every
//! node, and every client reading back its own request, runs over the
//! chosen entries in position order. A fold is a function of the log alone,
//! so every reader that has folded a prefix agrees on it.
//!
//! **This is not an application** (#186): paros still decides nothing about
//! the bytes of a user journal. The control journals are paros's own control
//! plane, like the matchmaker registry; the core keeps their entries as
//! opaque as any other, and only this module and the driver read them.
//!
//! - **Registry** (#211), keyed by `node_id` (random, minted at format). The
//!   node pool is the founding members plus every registered node not yet
//!   retired ([`Registry::pool`]). A node registers with its class
//!   (`storage` or `stateless`); a machine has no capacity in role slots
//!   (#837 (remove role slots)): the cell reports its busyness instead.
//!   A reboot registers the same id again, updating its address,
//!   never class, and records the machine's RPC incarnation (with the
//!   address, its `InterfaceRef` identity). A genesis node registers too
//!   (#349): its entry is the address its peers dial, in place of the one
//!   its deployment was booted with ([`Registry::address`]); it stays in the
//!   pool and is never drained. A registered node is drained, then retired — an
//!   id is never reused, a retired one included. **Bookings**
//!   (`BookCapacity`, written by the cell coordinator) record where each
//!   role runs: one booking holds one [`Role`] of one journal or matchmaker
//!   set ([`BookingTarget`]). They are judged at apply: a role of its
//!   class on a node of that class only, one live
//!   booking per node, target and role, and a booking id is never booked
//!   twice ([`Registry::spent`], kept across checkpoints). **Liveness** (D6)
//!   is written by the cell coordinator as changes only: `MachineDown` and
//!   `MachineUp`, each refused when it changes nothing
//!   ([`RegistryRefusal::LivenessUnchanged`]). The registry is checkpointed
//!   with `paros::client::checkpoint` (#230): its state is the latest entry
//!   per `node_id`, the live bookings, the spent booking ids, the liveness
//!   and the cell's side of the fleet.
//!
//!   The cell's side of the fleet (#229, §3.7): `JoinFleet` records, once,
//!   the fleet this cell belongs to and its own `cell_id` (`init` step 3),
//!   and `HostTenant` / `DropTenant` the tenants it hosts — the cell's
//!   tenant list, which the fleet directory ([`crate::fleet`]) must equal.
//!   `HostTenant` records the tenant's control journal, name and
//!   `survives` ([`HostedTenant`], #210). Every one names its fleet, and an
//!   entry naming another fleet than the one the cell joined is refused; a
//!   repeat folds to [`RegistryEvent::Unchanged`], so a re-run step is
//!   harmless.
//!
//!   Not yet: placement by booking and re-placement of a machine down past
//!   its bound (#212), and the durable cached registry fold on every
//!   machine.
//!
//! Every malformed entry — a record that does not decode, a configuration
//! that does not admit its quorum system — folds to a refusal, never a
//! panic: the entries are external input.

mod checkpoint;
mod command;
pub mod control_plane;
mod event;
mod records;
mod universe;

pub use crate::machine::Class;

use crate::machine::{Locality, incarnation_halves};
use crate::tags::UserTags;
use control_plane::{ControlPlane, ControlPlaneMode};
use paros_core::NodeId;
use paros_core::journal::TenantId;
use paros_wire::system as wire;
use std::collections::{BTreeMap, BTreeSet};

// The cell control journal has no fixed identifier (§3.8): a deployment draws
// it and hands it to whoever folds it (`paros_roles::node::ControlPlan`).

/// The registry's fold (#189, #211): the genesis pool the deployment was
/// booted with, every node registered at runtime keyed by its `node_id`
/// with where it stands, and every live booking.
///
/// The fold enforces the placement's rules at apply, where every reader
/// agrees on them: a booking is of the node's own class
/// ([`RegistryRefusal::WrongClass`]), and a node holds one instance of a
/// role of a target ([`RegistryRefusal::AlreadyBooked`]). It is
/// [`Checkpointable`]: its checkpoint is every runtime node and live
/// booking, never the history that made them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Registry {
    genesis: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, RegisteredNode>,
    bookings: BTreeMap<u64, Booking>,
    /// The fleet registration (#229): `(fleet_id, cell_id, version)`.
    fleet: Option<FleetRegistration>,
    /// The universe pointer its join recorded (#232 (multiple cells)):
    /// `None` for the universe's own cell.
    universe: Option<UniversePointer>,
    /// The tenants the cell hosts (#229), with what it records of each
    /// (#210).
    hosted: BTreeMap<TenantId, HostedTenant>,
    /// The tenants the cell dropped (#229): never hosted again, so a stale
    /// `HostTenant` — decided by an operator from a fleet directory fold another
    /// operator's removal has since overtaken — is refused here, where the
    /// cell's single writer judges it.
    dropped: BTreeSet<TenantId>,
    /// Every booking id ever booked (#211): never booked again, across
    /// checkpoints.
    spent: BTreeSet<u64>,
    /// What the cell coordinator last wrote of each machine's liveness.
    liveness: BTreeMap<NodeId, Liveness>,
    /// The open maintenance windows (#359).
    maintenance: BTreeMap<MaintenanceScope, MaintenanceWindow>,
    /// Every grant recorded (#476): its outcome stays, so a retry under its
    /// key reads it back.
    grants: BTreeMap<GrantKey, Grant>,
    /// Every tag request applied (#575): a retry never applies one again.
    tagged: BTreeSet<u64>,
    /// The control plane's mode (#685): `None` while the cell recorded
    /// none, which follows [`ControlPlaneMode::default`].
    control_plane: Option<ControlPlaneMode>,
    /// Every control plane request applied (#685).
    control_plane_requests: BTreeSet<u64>,
    next_seq: u64,
}

impl Registry {
    /// An empty registry over the deployment's `genesis` pool.
    #[must_use]
    pub fn new(genesis: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            genesis: genesis.into_iter().collect(),
            ..Self::default()
        }
    }

    /// The next position this fold expects.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Fold the record at position `seq` of the registry (in position
    /// order). A checkpoint record is not an entry: fold through a
    /// [`Folder`](crate::checkpoint::Folder), which restores it.
    ///
    /// # Panics
    ///
    /// If `seq` is below a position already folded (a programmer error of the caller).
    #[allow(clippy::too_many_lines)] // one arm per command
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> RegistryEvent {
        assert!(seq >= self.next_seq, "the registry folds in position order");
        self.next_seq = seq + 1;
        match SystemCommand::decode(record).ok() {
            Some(SystemCommand::RegisterNode {
                id,
                addr,
                class,
                locality,
                name,
                incarnation,
                machine_interface,
            }) => self.register(
                id,
                addr,
                class,
                (incarnation, machine_interface),
                (locality, name),
            ),
            Some(SystemCommand::SetMachineTags { request, id, tags }) => {
                self.set_tags(request, id, tags)
            }
            Some(SystemCommand::SetControlPlane { request, mode }) => {
                self.set_control_plane(request, mode)
            }
            Some(SystemCommand::DrainNode { id }) => match self.nodes.get_mut(&id) {
                // A genesis node is never drained, registered or not.
                Some(node)
                    if node.standing == NodeStanding::Registered && !self.genesis.contains(&id) =>
                {
                    node.standing = NodeStanding::Draining;
                    RegistryEvent::Draining { id }
                }
                _ => RegistryEvent::Refused(RegistryRefusal::NotRegistered { id }),
            },
            Some(SystemCommand::RetireNode { id }) => match self.nodes.get_mut(&id) {
                Some(node) if node.standing == NodeStanding::Draining => {
                    node.standing = NodeStanding::Retired;
                    self.bookings.retain(|_, booking| booking.node != id);
                    self.liveness.remove(&id);
                    self.maintenance.remove(&MaintenanceScope::Machine(id));
                    RegistryEvent::Retired { id }
                }
                _ => RegistryEvent::Refused(RegistryRefusal::NotDraining { id }),
            },
            Some(SystemCommand::BookCapacity {
                booking,
                node,
                role,
                target,
            }) => self.book(booking, node, role, target),
            Some(SystemCommand::MachineDown { id, incarnation }) => self.down(id, incarnation),
            Some(SystemCommand::MachineUp { id, incarnation }) => self.up(id, incarnation),
            Some(SystemCommand::MachineSlow {
                id,
                incarnation,
                slow,
                ..
            }) => self.slow(id, incarnation, slow),
            Some(SystemCommand::MachineSpace {
                id,
                incarnation,
                low,
                ..
            }) => self.space(id, incarnation, low),
            Some(SystemCommand::MachineAdmitted { id }) => self.admitted(id),
            Some(SystemCommand::Maintenance { scope, for_ms }) => {
                self.open_maintenance(seq, scope, for_ms)
            }
            Some(SystemCommand::ReleaseCapacity { booking }) => {
                match self.bookings.remove(&booking) {
                    Some(held) => RegistryEvent::Released {
                        booking,
                        node: held.node,
                    },
                    None => RegistryEvent::Refused(RegistryRefusal::UnknownBooking { booking }),
                }
            }
            Some(SystemCommand::JoinFleet {
                fleet_id,
                cell_id,
                version,
                universe,
            }) => self.join_fleet(
                FleetRegistration {
                    fleet_id,
                    cell_id,
                    version,
                },
                universe,
            ),
            Some(SystemCommand::HostTenant {
                fleet_id,
                tenant,
                hosted,
            }) => self.host(fleet_id, tenant, Some(hosted)),
            Some(SystemCommand::DropTenant { fleet_id, tenant }) => {
                self.host(fleet_id, tenant, None)
            }
            Some(SystemCommand::GrantCapacity { key, tenant, slots }) => {
                self.grant(key, tenant, slots)
            }
            _ => RegistryEvent::Refused(RegistryRefusal::Malformed),
        }
    }

    fn register(
        &mut self,
        id: NodeId,
        addr: String,
        class: Class,
        (incarnation, machine_interface): (u128, Vec<u8>),
        (locality, name): (Locality, String),
    ) -> RegistryEvent {
        // A genesis node (a founding member) registers too (#349): its
        // entry is the address peers dial, which may change across its
        // restarts. It stays in the pool whatever the registry says.
        match self.nodes.get_mut(&id) {
            Some(node) if node.standing == NodeStanding::Retired => {
                RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id })
            }
            // A machine may take more roles at a restart (`all`), never
            // fewer: a narrower class could drop a role the cell books on it.
            Some(node) if node.class != class && class != Class::All => {
                RegistryEvent::Refused(RegistryRefusal::ClassChanged { id })
            }
            Some(node) => {
                node.class = class;
                node.addr.clone_from(&addr);
                node.locality = locality;
                node.name = name;
                node.registrations += 1;
                node.incarnation = incarnation;
                node.machine_interface = machine_interface;
                // A registration is a machine that answered: up, as the
                // incarnation it names.
                self.liveness.insert(
                    id,
                    Liveness {
                        incarnation,
                        up: true,
                        slow: false,
                        low_space: false,
                    },
                );
                RegistryEvent::Reregistered { id, addr }
            }
            None => {
                self.nodes.insert(
                    id,
                    RegisteredNode {
                        addr: addr.clone(),
                        locality,
                        name,
                        class,
                        standing: NodeStanding::Registered,
                        registrations: 1,
                        incarnation,
                        machine_interface,
                        admitted: false,
                        tags: UserTags::none(),
                    },
                );
                self.liveness.insert(
                    id,
                    Liveness {
                        incarnation,
                        up: true,
                        slow: false,
                        low_space: false,
                    },
                );
                RegistryEvent::Registered { id, addr, class }
            }
        }
    }

    /// Open, replace or end (`for_ms == 0`) the maintenance window over
    /// `scope`, from the entry at `seq` (#359). A machine window names a
    /// machine of the pool: a genesis node, or one registered and not
    /// retired.
    fn open_maintenance(
        &mut self,
        seq: u64,
        scope: MaintenanceScope,
        for_ms: u64,
    ) -> RegistryEvent {
        if let MaintenanceScope::Machine(id) = &scope
            && !self.contains(*id)
        {
            return RegistryEvent::Refused(RegistryRefusal::NotInPool { id: *id });
        }
        if for_ms == 0 {
            return match self.maintenance.remove(&scope) {
                Some(_) => {
                    moonpool_assertions::reachable!(
                        "registry: an operator ends a maintenance window"
                    );
                    RegistryEvent::Maintenance { scope, for_ms }
                }
                None => RegistryEvent::Unchanged,
            };
        }
        self.maintenance
            .insert(scope.clone(), MaintenanceWindow { for_ms, seq });
        assert!(
            self.maintenance.get(&scope).is_some_and(|w| w.seq == seq),
            "a maintenance entry opens its own window"
        );
        RegistryEvent::Maintenance { scope, for_ms }
    }

    /// The open maintenance windows (#359), with what each covers.
    pub fn maintenance(&self) -> impl Iterator<Item = (&MaintenanceScope, &MaintenanceWindow)> {
        self.maintenance.iter()
    }

    /// The open maintenance windows that cover machine `id` (#359): its
    /// own, and its failure domain's.
    pub fn maintenance_of(
        &self,
        id: NodeId,
    ) -> impl Iterator<Item = (&MaintenanceScope, &MaintenanceWindow)> {
        let zone = self.get(id).map(|n| n.locality.zone.clone());
        self.maintenance
            .iter()
            .filter(move |(scope, _)| match scope {
                MaintenanceScope::Machine(machine) => *machine == id,
                MaintenanceScope::Zone(domain) => zone.as_deref() == Some(domain.as_str()),
            })
    }

    /// A tag request (#575): judged at apply, applied at most once, for a
    /// machine the registry holds and did not retire. The tags were judged
    /// at decode: no reserved key, within the cap.
    fn set_tags(&mut self, request: u64, id: NodeId, tags: UserTags) -> RegistryEvent {
        if request == 0 {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if self.tagged.contains(&request) {
            return RegistryEvent::TagRepeated { request };
        }
        match self.nodes.get_mut(&id) {
            Some(node) if node.standing != NodeStanding::Retired => {
                node.tags = tags;
                self.tagged.insert(request);
                RegistryEvent::Tagged { id }
            }
            _ => RegistryEvent::Refused(RegistryRefusal::NotRegistered { id }),
        }
    }

    /// A control plane request (#685): judged at apply, applied at most
    /// once. A manual `F` the cell's storage machines cannot place is
    /// refused; auto is never refused, since it wants only what fits.
    fn set_control_plane(&mut self, request: u64, mode: ControlPlaneMode) -> RegistryEvent {
        if request == 0 {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if self.control_plane_requests.contains(&request) {
            return RegistryEvent::ControlPlaneRepeated { request };
        }
        let storage = self.storage_machines();
        if let ControlPlaneMode::Manual { failures } = mode
            && control_plane::acceptors(failures) > storage
        {
            moonpool_assertions::reachable!("registry: a manual control plane too wide is refused");
            return RegistryEvent::Refused(RegistryRefusal::ControlPlaneTooWide {
                failures,
                storage,
            });
        }
        self.control_plane = Some(mode);
        self.control_plane_requests.insert(request);
        assert_eq!(
            self.control_plane_mode(),
            mode,
            "the mode applied is the mode read"
        );
        RegistryEvent::ControlPlaneSet { mode }
    }

    /// The control plane's mode (#685): the one recorded last, or the
    /// default (auto, target 1) while the cell recorded none.
    #[must_use]
    pub fn control_plane_mode(&self) -> ControlPlaneMode {
        self.control_plane.unwrap_or_default()
    }

    /// Whether the control plane request `request` was applied (#685).
    #[must_use]
    pub fn control_plane_applied(&self, request: u64) -> bool {
        self.control_plane_requests.contains(&request)
    }

    /// The control plane (#685), its journals on `members` acceptors now.
    #[must_use]
    pub fn control_plane(&self, members: usize) -> ControlPlane {
        ControlPlane::new(self.control_plane_mode(), self.storage_machines(), members)
    }

    /// The storage machines the control plane may be placed on (#685): the
    /// genesis nodes (a founding member stores) and every registered
    /// storage machine that is neither draining nor retired.
    ///
    /// # Panics
    ///
    /// If a genesis node is not counted: a programmer error.
    #[must_use]
    pub fn storage_machines(&self) -> usize {
        let mut storage: BTreeSet<NodeId> = self.genesis.clone();
        storage.extend(
            self.nodes
                .iter()
                .filter(|(_, n)| n.class.stores() && n.standing == NodeStanding::Registered)
                .map(|(id, _)| *id),
        );
        assert!(
            storage.len() >= self.genesis.len(),
            "every genesis node stores"
        );
        storage.len()
    }

    /// Whether the tag request `request` was applied (#575).
    #[must_use]
    pub fn tagged(&self, request: u64) -> bool {
        self.tagged.contains(&request)
    }

    fn join_fleet(
        &mut self,
        joined: FleetRegistration,
        universe: Option<UniversePointer>,
    ) -> RegistryEvent {
        if joined.fleet_id == 0 || joined.cell_id == 0 {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if joined.version == 0 || joined.version > crate::fleet::METADATA_VERSION {
            return RegistryEvent::Refused(RegistryRefusal::Unsupported);
        }
        match self.fleet {
            None => {
                self.fleet = Some(joined);
                self.universe = universe;
                RegistryEvent::JoinedFleet {
                    fleet_id: joined.fleet_id,
                    cell_id: joined.cell_id,
                }
            }
            Some(fleet) if fleet.fleet_id == joined.fleet_id && fleet.cell_id == joined.cell_id => {
                RegistryEvent::Unchanged
            }
            Some(_) => RegistryEvent::Refused(RegistryRefusal::OtherFleet {
                fleet_id: joined.fleet_id,
            }),
        }
    }

    /// Host (`host`) or drop `tenant` for fleet `fleet_id`: idempotent, and
    /// refused unless the cell joined that fleet. A drop is a tombstone,
    /// written whether the tenant was hosted or not: a dropped tenant is
    /// never hosted again ([`RegistryRefusal::TenantDropped`]).
    fn host(
        &mut self,
        fleet_id: u64,
        tenant: TenantId,
        host: Option<HostedTenant>,
    ) -> RegistryEvent {
        if self.fleet.is_none_or(|fleet| fleet.fleet_id != fleet_id) {
            return RegistryEvent::Refused(RegistryRefusal::OtherFleet { fleet_id });
        }
        if !tenant.is_set() {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if let Some(hosted) = host {
            if !hosted.control.is_set() {
                return RegistryEvent::Refused(RegistryRefusal::Malformed);
            }
            if self.dropped.contains(&tenant) {
                return RegistryEvent::Refused(RegistryRefusal::TenantDropped { tenant });
            }
            // The first host wins: a re-run step names the same tenant again.
            if self.hosted.contains_key(&tenant) {
                return RegistryEvent::Unchanged;
            }
            // A cell hosts a tenant only once its footprint is booked (#476).
            if !self.footprint(tenant).is_some_and(Grant::granted) {
                return RegistryEvent::Refused(RegistryRefusal::NoFootprint { tenant });
            }
            let control = hosted.control;
            self.hosted.insert(tenant, hosted);
            return RegistryEvent::TenantHosted { tenant, control };
        }
        self.hosted.remove(&tenant);
        if self.dropped.insert(tenant) {
            RegistryEvent::TenantDropped { tenant }
        } else {
            RegistryEvent::Unchanged
        }
    }

    /// Whether the cell dropped `tenant`: it never hosts it again (#229).
    #[must_use]
    pub fn dropped(&self, tenant: TenantId) -> bool {
        self.dropped.contains(&tenant)
    }

    /// Every tenant the cell dropped (#229). A fold restored from a
    /// checkpoint learns a drop only here: the `TenantDropped` entry is
    /// below the checkpoint.
    pub fn dropped_tenants(&self) -> impl Iterator<Item = TenantId> + '_ {
        self.dropped.iter().copied()
    }

    /// The cell side of the fleet's registration, once `init` wrote it
    /// (#229).
    #[must_use]
    pub fn fleet(&self) -> Option<FleetRegistration> {
        self.fleet
    }

    /// Where the universe directory is, when the cell joined a universe
    /// another cell hosts (#232 (multiple cells)).
    #[must_use]
    pub fn universe(&self) -> Option<&UniversePointer> {
        self.universe.as_ref()
    }

    /// Whether the cell hosts `tenant` (#229).
    #[must_use]
    pub fn hosts(&self, tenant: TenantId) -> bool {
        self.hosted.contains_key(&tenant)
    }

    /// Whether `journal` is the control journal of a tenant the cell hosts
    /// (#685 (special tenants are tenants)).
    #[must_use]
    pub fn hosts_control(&self, journal: paros_core::journal::JournalIdentifier) -> bool {
        self.hosted
            .get(&journal.tenant)
            .is_some_and(|hosted| hosted.control == journal.journal)
    }

    /// The tenants the cell hosts, in id order (#229).
    pub fn hosted(&self) -> impl Iterator<Item = TenantId> + '_ {
        self.hosted.keys().copied()
    }

    /// What the cell records of `tenant`, while it hosts it (#210).
    #[must_use]
    pub fn hosted_tenant(&self, tenant: TenantId) -> Option<&HostedTenant> {
        self.hosted.get(&tenant)
    }

    fn book(
        &mut self,
        booking: u64,
        id: NodeId,
        role: Role,
        target: BookingTarget,
    ) -> RegistryEvent {
        if self.spent.contains(&booking) {
            return RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking });
        }
        assert!(
            !self.bookings.contains_key(&booking),
            "a live booking's id is spent"
        );
        let class = role.class();
        let Some(node) = self
            .nodes
            .get(&id)
            .filter(|n| n.standing == NodeStanding::Registered)
        else {
            return RegistryEvent::Refused(RegistryRefusal::NotRegistered { id });
        };
        if !node.class.takes(class) {
            return RegistryEvent::Refused(RegistryRefusal::WrongClass { id });
        }
        // A tenant's slots are not role instances: they may stack (#476).
        if !matches!(target, BookingTarget::Tenant { .. })
            && self
                .bookings
                .values()
                .any(|b| b.node == id && b.role == role && b.target == target)
        {
            return RegistryEvent::Refused(RegistryRefusal::AlreadyBooked { id });
        }
        self.spent.insert(booking);
        self.bookings.insert(
            booking,
            Booking {
                node: id,
                role,
                target,
            },
        );
        RegistryEvent::Booked {
            booking,
            node: id,
            role,
            target,
        }
    }

    /// `GrantCapacity` (#476): record a refusal, or book every slot and
    /// record the grant, or book none. A slot the fold refuses undoes the
    /// slots booked before it, and nothing is recorded: the requester
    /// plans again from the newer fold. The first grant under a key wins.
    fn grant(
        &mut self,
        key: GrantKey,
        tenant: TenantId,
        slots: Option<Vec<Slot>>,
    ) -> RegistryEvent {
        let keyed = match key {
            GrantKey::Footprint(of) => of == tenant,
            GrantKey::Request(request) => request != 0,
        };
        if !tenant.is_set() || !keyed {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if self.grants.contains_key(&key) {
            return RegistryEvent::Unchanged;
        }
        let Some(slots) = slots else {
            self.grants.insert(
                key,
                Grant {
                    tenant,
                    bookings: None,
                },
            );
            return RegistryEvent::GrantRefused { key };
        };
        // A footprint may book nothing (#837 (remove role slots): no
        // stateless machine to place the coordinator on); a request books
        // at least one slot.
        let empty = slots.is_empty() && !matches!(key, GrantKey::Footprint(_));
        if empty || slots.iter().any(|slot| slot.target.tenant() != tenant) {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        let mut booked: Vec<u64> = Vec::with_capacity(slots.len());
        for slot in &slots {
            match self.book(slot.booking, slot.node, slot.role, slot.target) {
                RegistryEvent::Booked { .. } => booked.push(slot.booking),
                refused => {
                    for booking in booked {
                        assert!(
                            self.bookings.remove(&booking).is_some(),
                            "an undone slot was live"
                        );
                        assert!(
                            self.spent.remove(&booking),
                            "an undone slot's id was spent here"
                        );
                    }
                    return refused;
                }
            }
        }
        assert_eq!(booked.len(), slots.len(), "a grant books every slot");
        self.grants.insert(
            key,
            Grant {
                tenant,
                bookings: Some(booked),
            },
        );
        RegistryEvent::Granted { key, slots }
    }

    /// The grant recorded under `key` (#476).
    #[must_use]
    pub fn grant_of(&self, key: GrantKey) -> Option<&Grant> {
        self.grants.get(&key)
    }

    /// Every grant recorded, in key order (#476).
    pub fn grants(&self) -> impl Iterator<Item = (GrantKey, &Grant)> {
        self.grants.iter().map(|(key, grant)| (*key, grant))
    }

    /// The footprint grant of `tenant` (#476): granted at its birth, or
    /// refused.
    #[must_use]
    pub fn footprint(&self, tenant: TenantId) -> Option<&Grant> {
        self.grants.get(&GrantKey::Footprint(tenant))
    }

    /// The genesis pool, in id order: the founding members.
    pub fn genesis(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.genesis.iter().copied()
    }

    /// `MachineDown` (#211, D6): a change only. Refused for a machine
    /// outside the pool, for one held down, and for an incarnation other
    /// than the one held (a later one answered since).
    fn down(&mut self, id: NodeId, incarnation: u128) -> RegistryEvent {
        if !self.contains(id) {
            return RegistryEvent::Refused(RegistryRefusal::NotInPool { id });
        }
        let held = self.liveness(id);
        if !held.up {
            return RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id });
        }
        if held.incarnation != incarnation {
            return RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id });
        }
        self.liveness.insert(
            id,
            Liveness {
                incarnation,
                up: false,
                slow: held.slow,
                low_space: held.low_space,
            },
        );
        assert!(!self.liveness(id).up, "a machine marked down is held down");
        RegistryEvent::MachineDown { id }
    }

    /// `MachineAdmitted` (#511 (placement books machines that never
    /// served)): refused for a node not registered, retired, or a genesis
    /// node (in the cell from its start); a repeat changes nothing.
    fn admitted(&mut self, id: NodeId) -> RegistryEvent {
        if self.genesis.contains(&id) {
            return RegistryEvent::Refused(RegistryRefusal::NotRegistered { id });
        }
        let Some(node) = self
            .nodes
            .get_mut(&id)
            .filter(|n| n.standing != NodeStanding::Retired)
        else {
            return RegistryEvent::Refused(RegistryRefusal::NotRegistered { id });
        };
        if node.admitted {
            return RegistryEvent::Unchanged;
        }
        node.admitted = true;
        assert!(self.serves(id), "an admitted machine serves the cell");
        RegistryEvent::MachineAdmitted { id }
    }

    /// `MachineUp` (#211, D6): a change only. Refused for a machine outside
    /// the pool and for the incarnation held up.
    fn up(&mut self, id: NodeId, incarnation: u128) -> RegistryEvent {
        if !self.contains(id) {
            return RegistryEvent::Refused(RegistryRefusal::NotInPool { id });
        }
        let held = self.liveness(id);
        if held.up && held.incarnation == incarnation {
            return RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id });
        }
        if let Some(node) = self.nodes.get_mut(&id) {
            node.incarnation = incarnation;
        }
        // A slow judgement belongs to one incarnation: a new one starts fast.
        self.liveness.insert(
            id,
            Liveness {
                incarnation,
                up: true,
                slow: held.slow && held.incarnation == incarnation,
                low_space: held.low_space && held.incarnation == incarnation,
            },
        );
        RegistryEvent::MachineUp {
            id,
            incarnation,
            was_down: !held.up,
        }
    }

    /// What the cell coordinator last wrote of `id`'s liveness: a pool
    /// member it never wrote of is up, its incarnation the registration's
    /// (`0` for a genesis node).
    #[must_use]
    pub fn liveness(&self, id: NodeId) -> Liveness {
        self.liveness.get(&id).copied().unwrap_or(Liveness {
            incarnation: self.nodes.get(&id).map_or(0, |n| n.incarnation),
            up: true,
            slow: false,
            low_space: false,
        })
    }

    /// `MachineSpace` (#360 (disk full)): a change only. Refused for a
    /// machine outside the pool, for an incarnation other than the one held,
    /// and for the judgement held.
    fn space(&mut self, id: NodeId, incarnation: u128, low: bool) -> RegistryEvent {
        if !self.contains(id) {
            return RegistryEvent::Refused(RegistryRefusal::NotInPool { id });
        }
        let held = self.liveness(id);
        if held.incarnation != incarnation {
            return RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id });
        }
        if held.low_space == low {
            return RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id });
        }
        self.liveness.insert(
            id,
            Liveness {
                low_space: low,
                ..held
            },
        );
        assert!(
            self.liveness(id).low_space == low,
            "a space judgement is held"
        );
        assert!(
            self.liveness(id).up == held.up && self.liveness(id).slow == held.slow,
            "a space judgement leaves the up and slow states alone"
        );
        RegistryEvent::MachineSpace { id, low }
    }

    /// Whether the cell coordinator holds `id` low on space (#360 (disk
    /// full)).
    #[must_use]
    pub fn is_low_on_space(&self, id: NodeId) -> bool {
        self.contains(id) && self.liveness(id).low_space
    }

    /// `MachineSlow` (#361 (slow machines)): a change only. Refused for a
    /// machine outside the pool, for an incarnation other than the one held,
    /// and for the judgement held.
    fn slow(&mut self, id: NodeId, incarnation: u128, slow: bool) -> RegistryEvent {
        if !self.contains(id) {
            return RegistryEvent::Refused(RegistryRefusal::NotInPool { id });
        }
        let held = self.liveness(id);
        if held.incarnation != incarnation {
            return RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id });
        }
        if held.slow == slow {
            return RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id });
        }
        self.liveness.insert(id, Liveness { slow, ..held });
        assert!(self.liveness(id).slow == slow, "a slow judgement is held");
        assert!(
            self.liveness(id).up == held.up,
            "a slow judgement leaves the up state alone"
        );
        RegistryEvent::MachineSlow { id, slow }
    }

    /// Whether the cell coordinator holds `id` slow (#361 (slow machines)).
    #[must_use]
    pub fn is_slow(&self, id: NodeId) -> bool {
        self.contains(id) && self.liveness(id).slow
    }

    /// Whether `id` keeps the roles placed on it (#361 (slow machines)):
    /// registered, neither draining nor retired, and not held slow. A role
    /// on any other machine moves off it (`machine::moves`).
    #[must_use]
    pub fn keeps_roles(&self, id: NodeId) -> bool {
        self.nodes
            .get(&id)
            .is_some_and(|n| n.standing == NodeStanding::Registered)
            && !self.is_slow(id)
    }

    /// Whether `id` serves the cell (#511 (placement books machines that
    /// never served)): a genesis node, or a registered machine not retired
    /// that the cell coordinator saw in the cell (`MachineAdmitted`). Only
    /// such a machine is offered to placement: a registration comes before
    /// the `Admit` it may never win (a wiped founder's replacement stuck in
    /// another cell decree).
    #[must_use]
    pub fn serves(&self, id: NodeId) -> bool {
        self.genesis.contains(&id)
            || self
                .nodes
                .get(&id)
                .is_some_and(|n| n.admitted && n.standing != NodeStanding::Retired)
    }

    /// Whether `id` was seen alive: in the pool, and not marked down.
    #[must_use]
    pub fn seen_alive(&self, id: NodeId) -> bool {
        self.contains(id) && self.liveness(id).up
    }

    /// Every booking id ever booked, in id order: none is booked again.
    pub fn spent(&self) -> impl Iterator<Item = u64> + '_ {
        self.spent.iter().copied()
    }

    /// Whether `id` is in the pool: a genesis node, or registered and not
    /// retired.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.genesis.contains(&id)
            || self
                .nodes
                .get(&id)
                .is_some_and(|n| n.standing != NodeStanding::Retired)
    }

    /// The pool, sorted: the genesis nodes and every registered node not
    /// retired.
    #[must_use]
    pub fn pool(&self) -> Vec<NodeId> {
        let mut pool: BTreeSet<NodeId> = self.genesis.clone();
        pool.extend(
            self.nodes
                .iter()
                .filter(|(_, n)| n.standing != NodeStanding::Retired)
                .map(|(id, _)| *id),
        );
        pool.into_iter().collect()
    }

    /// The node registered as `id`, whatever its standing.
    #[must_use]
    pub fn get(&self, id: NodeId) -> Option<&RegisteredNode> {
        self.nodes.get(&id)
    }

    /// The address node `id` registered last (#349): what its peers dial.
    /// `None` for a node that never registered (a genesis node keeps the
    /// address its cell plan names) or a retired one.
    #[must_use]
    pub fn address(&self, id: NodeId) -> Option<&str> {
        self.nodes
            .get(&id)
            .filter(|n| n.standing != NodeStanding::Retired)
            .map(|n| n.addr.as_str())
    }

    /// Every node registered at runtime, in id order.
    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, &RegisteredNode)> {
        self.nodes.iter().map(|(id, n)| (*id, n))
    }

    /// The live bookings of `id`: the roles it holds, a weightless one
    /// ([`Booking::weighs`]) included.
    #[must_use]
    pub fn booked(&self, id: NodeId) -> u64 {
        self.bookings.values().filter(|b| b.node == id).count() as u64
    }

    /// The roles `id` hosts that count for the spread: every booking
    /// but a weightless one ([`Booking::weighs`], #753 (per-journal
    /// proxies)).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn weight(&self, id: NodeId) -> u64 {
        let weight = self
            .bookings
            .values()
            .filter(|b| b.node == id && b.weighs())
            .count() as u64;
        assert!(weight <= self.booked(id), "a weight counts bookings");
        weight
    }

    /// The live booking `booking`.
    #[must_use]
    pub fn booking(&self, booking: u64) -> Option<&Booking> {
        self.bookings.get(&booking)
    }

    /// Every live booking, in id order.
    pub fn bookings(&self) -> impl Iterator<Item = (u64, &Booking)> {
        self.bookings.iter().map(|(id, b)| (*id, b))
    }

    #[allow(clippy::too_many_lines)] // one field per part of the fold
    fn state_to_wire(&self) -> wire::RegistryState {
        wire::RegistryState {
            nodes: self
                .nodes
                .iter()
                .map(|(id, n)| wire::RegisteredNodeState {
                    id: id.0,
                    addr: n.addr.clone(),
                    tags: n.all_tags().to_wire(),
                    name: n.name.clone(),
                    standing: n.standing.to_wire(),
                    registrations: n.registrations,
                    incarnation_high: incarnation_halves(n.incarnation).0,
                    incarnation_low: incarnation_halves(n.incarnation).1,
                    machine_interface: n.machine_interface.clone(),
                    admitted: n.admitted,
                })
                .collect(),
            bookings: self
                .bookings
                .iter()
                .map(|(id, b)| {
                    let (tenant, journal, set, tenant_level) = b.target.to_wire();
                    wire::BookingState {
                        booking: *id,
                        node: b.node.0,
                        tenant,
                        journal,
                        role: b.role.as_str().into(),
                        set,
                        tenant_level,
                    }
                })
                .collect(),
            spent: self.spent.iter().copied().collect(),
            liveness: self
                .liveness
                .iter()
                .map(|(id, l)| wire::LivenessState {
                    id: id.0,
                    incarnation_high: incarnation_halves(l.incarnation).0,
                    incarnation_low: incarnation_halves(l.incarnation).1,
                    up: l.up,
                    slow: l.slow,
                    low_space: l.low_space,
                })
                .collect(),
            fleet_id: self.fleet.map_or(0, |f| f.fleet_id),
            cell_id: self.fleet.map_or(0, |f| f.cell_id),
            version: self.fleet.map_or(0, |f| f.version),
            directory_tenant: pointer_to_wire(self.universe.as_ref()).0,
            directory_journal: pointer_to_wire(self.universe.as_ref()).1,
            hosts: pointer_to_wire(self.universe.as_ref()).2,
            hosted: self
                .hosted
                .iter()
                .map(|(t, h)| wire::HostedTenantState {
                    tenant: t.0,
                    control_journal: h.control.0,
                    name: h.name.clone(),
                    survives: h.survives.to_wire(),
                })
                .collect(),
            dropped: self.dropped.iter().map(|t| t.0).collect(),
            maintenance: self
                .maintenance
                .iter()
                .map(|(scope, window)| {
                    let (node, zone) = scope.to_wire();
                    wire::MaintenanceState {
                        node,
                        zone,
                        for_ms: window.for_ms,
                        seq: window.seq,
                    }
                })
                .collect(),
            grants: self
                .grants
                .iter()
                .map(|(key, grant)| {
                    let (request, footprint) = match key {
                        GrantKey::Footprint(_) => (0, true),
                        GrantKey::Request(request) => (*request, false),
                    };
                    wire::GrantState {
                        request,
                        tenant: grant.tenant.0,
                        footprint,
                        refused: !grant.granted(),
                        bookings: grant.bookings.clone().unwrap_or_default(),
                    }
                })
                .collect(),
            tagged: self.tagged.iter().copied().collect(),
            control_plane: self.control_plane.map(|mode| wire::ControlPlaneState {
                auto: mode.is_auto(),
                failures: mode.failures(),
            }),
            control_plane_requests: self.control_plane_requests.iter().copied().collect(),
        }
    }
}

#[cfg(test)]
mod tests;
pub use command::{Grant, GrantKey, Slot, SystemCommand};
pub use event::{RegistryEvent, RegistryRefusal, SystemEvent, registry_event};
pub use records::{
    Booking, BookingTarget, Liveness, MaintenanceScope, MaintenanceWindow, NodeStanding,
    RegisteredNode, Role, SlowFacts,
};
pub(crate) use universe::pointer_to_wire;
pub use universe::{FleetRegistration, HostedTenant, UniversePointer};
