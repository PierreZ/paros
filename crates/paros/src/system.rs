//! The **cell control journal** (#189, #211): the node registry, the cell
//! tenant's control journal (#235). A service must add and retire machines
//! while it runs; paros already has the right tool — a replicated log — so
//! the cell's machines and capacity are a journal of their own, and every
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
//!   (`storage` or `stateless`) and its capacity (role slots of its class);
//!   a reboot registers the same id again, updating address and capacity,
//!   never class, and records the machine's RPC incarnation (with the
//!   address, its `InterfaceRef` identity). A genesis node registers too
//!   (#349): its entry is the address its peers dial, in place of the one
//!   its deployment was booted with ([`Registry::address`]); it stays in the
//!   pool and is never drained. A registered node is drained, then retired — an
//!   id is never reused, a retired one included. Capacity **bookings**
//!   (`BookCapacity`, written by the cell coordinator) are keyed by role:
//!   one slot holds one [`Role`] of one journal or matchmaker set
//!   ([`BookingTarget`]). They are judged at apply: a slot of the role's
//!   class on a node of that class only, never past its capacity, one live
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

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{JournalId, JournalIdentifier, NodeId, TenantId};
use prost::Message as _;

use crate::client::checkpoint::{Checkpointable, Folded};
pub use crate::machine::Class;
use crate::machine::{incarnation_from_halves, incarnation_halves};

use crate::rpc::system as wire;

// The cell control journal has no fixed identifier (§3.8): a deployment draws
// it and hands it to whoever folds it (`crate::ControlPlan`).

/// One system-journal entry, as a client appends it and a fold reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemCommand {
    /// Add node `id`, reachable at `addr`, to the pool as a machine of
    /// `class` with `capacity` role slots — or, for a node registered
    /// already, register it again (a reboot).
    RegisterNode {
        /// The node's identity (random, minted at format).
        id: NodeId,
        /// Its address.
        addr: String,
        /// Its class.
        class: Class,
        /// The role slots of its class it advertises.
        capacity: u64,
        /// Its failure domain (opaque; placement reads it).
        failure_domain: String,
        /// Its RPC incarnation (`0` unknown): with `addr`, its
        /// `InterfaceRef` identity.
        incarnation: u128,
    },
    /// Stop placing new work on node `id`.
    DrainNode {
        /// The node to drain.
        id: NodeId,
    },
    /// Remove drained node `id` from the pool for good.
    RetireNode {
        /// The node to retire.
        id: NodeId,
    },
    /// Book one slot of `node` for `role` of `target` (#211): written by
    /// the cell coordinator, the registry's single writer.
    BookCapacity {
        /// The id its writer drew; refused if it was ever booked.
        booking: u64,
        /// The node.
        node: NodeId,
        /// The role the slot holds; its class is the slot's.
        role: Role,
        /// The journal or matchmaker set the slot is for.
        target: BookingTarget,
    },
    /// The cell coordinator saw machine `id`, as `incarnation`, stop
    /// answering (#211, D6).
    MachineDown {
        /// The machine.
        id: NodeId,
        /// The incarnation the registry holds for it.
        incarnation: u128,
    },
    /// The cell coordinator saw machine `id` answer as `incarnation`
    /// (#211, D6).
    MachineUp {
        /// The machine.
        id: NodeId,
        /// The incarnation that answered.
        incarnation: u128,
    },
    /// Release a booking.
    ReleaseCapacity {
        /// The booking.
        booking: u64,
    },
    /// The cell side of the fleet's registration (#229): this cell is
    /// `cell_id` in fleet `fleet_id`, speaking metadata `version`.
    JoinFleet {
        /// The fleet.
        fleet_id: u64,
        /// This cell's id.
        cell_id: u64,
        /// The metadata version.
        version: u32,
    },
    /// Tenant creation's cell step (#229): the cell hosts `tenant`, whose
    /// control journal is `control` (#210).
    HostTenant {
        /// The fleet the writer believes the cell belongs to.
        fleet_id: u64,
        /// The tenant.
        tenant: TenantId,
        /// What the cell records of it: its control journal, name and
        /// `survives`, which its first coordinator describes it with.
        hosted: HostedTenant,
    },
    /// Tenant removal's cell step (#229): the cell forgets `tenant`.
    DropTenant {
        /// The fleet the writer believes the cell belongs to.
        fleet_id: u64,
        /// The tenant.
        tenant: TenantId,
    },
}

impl SystemCommand {
    /// The record a client writes: exactly one per position.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        use wire::system_entry::Kind;
        let kind = match self {
            SystemCommand::RegisterNode {
                id,
                addr,
                class,
                capacity,
                failure_domain,
                incarnation,
            } => Kind::RegisterNode(wire::RegisterNode {
                id: id.0,
                addr: addr.clone(),
                failure_domain: failure_domain.clone(),
                class: class.as_str().into(),
                capacity: *capacity,
                incarnation_high: incarnation_halves(*incarnation).0,
                incarnation_low: incarnation_halves(*incarnation).1,
            }),
            SystemCommand::DrainNode { id } => Kind::DrainNode(wire::DrainNode { id: id.0 }),
            SystemCommand::RetireNode { id } => Kind::RetireNode(wire::RetireNode { id: id.0 }),
            SystemCommand::BookCapacity {
                booking,
                node,
                role,
                target,
            } => {
                let (tenant, journal, set) = target.to_wire();
                Kind::BookCapacity(wire::BookCapacity {
                    booking: *booking,
                    node: node.0,
                    tenant,
                    journal,
                    role: role.as_str().into(),
                    set,
                })
            }
            SystemCommand::MachineDown { id, incarnation } => {
                let (incarnation_high, incarnation_low) = incarnation_halves(*incarnation);
                Kind::MachineDown(wire::MachineDown {
                    id: id.0,
                    incarnation_high,
                    incarnation_low,
                })
            }
            SystemCommand::MachineUp { id, incarnation } => {
                let (incarnation_high, incarnation_low) = incarnation_halves(*incarnation);
                Kind::MachineUp(wire::MachineUp {
                    id: id.0,
                    incarnation_high,
                    incarnation_low,
                })
            }
            SystemCommand::ReleaseCapacity { booking } => {
                Kind::ReleaseCapacity(wire::ReleaseCapacity { booking: *booking })
            }
            SystemCommand::JoinFleet {
                fleet_id,
                cell_id,
                version,
            } => Kind::JoinFleet(wire::JoinFleet {
                fleet_id: *fleet_id,
                cell_id: *cell_id,
                version: *version,
            }),
            SystemCommand::HostTenant {
                fleet_id,
                tenant,
                hosted,
            } => Kind::HostTenant(wire::HostTenant {
                fleet_id: *fleet_id,
                tenant: tenant.0,
                control_journal: hosted.control.0,
                name: hosted.name.clone(),
                survives: hosted.survives.to_wire(),
            }),
            SystemCommand::DropTenant { fleet_id, tenant } => Kind::DropTenant(wire::DropTenant {
                fleet_id: *fleet_id,
                tenant: tenant.0,
            }),
        };
        wire::SystemEntry { kind: Some(kind) }.encode_to_vec()
    }

    /// Read one record back.
    ///
    /// # Errors
    ///
    /// The record is not a system entry, names no kind, or carries a
    /// configuration that does not admit its quorum system.
    pub fn decode(record: &[u8]) -> Result<Self, &'static str> {
        use wire::system_entry::Kind;
        let entry = wire::SystemEntry::decode(record).map_err(|_| "not a system entry")?;
        Ok(match entry.kind.ok_or("a system entry names no kind")? {
            Kind::RegisterNode(register) => SystemCommand::RegisterNode {
                id: NodeId(register.id),
                addr: register.addr,
                class: register.class.parse()?,
                capacity: register.capacity,
                failure_domain: register.failure_domain,
                incarnation: incarnation_from_halves(
                    register.incarnation_high,
                    register.incarnation_low,
                ),
            },
            Kind::DrainNode(drain) => SystemCommand::DrainNode {
                id: NodeId(drain.id),
            },
            Kind::RetireNode(retire) => SystemCommand::RetireNode {
                id: NodeId(retire.id),
            },
            Kind::BookCapacity(book) => SystemCommand::BookCapacity {
                booking: book.booking,
                node: NodeId(book.node),
                role: book.role.parse()?,
                target: BookingTarget::from_wire(book.tenant, book.journal, book.set)?,
            },
            Kind::MachineDown(down) => SystemCommand::MachineDown {
                id: NodeId(down.id),
                incarnation: incarnation_from_halves(down.incarnation_high, down.incarnation_low),
            },
            Kind::MachineUp(up) => SystemCommand::MachineUp {
                id: NodeId(up.id),
                incarnation: incarnation_from_halves(up.incarnation_high, up.incarnation_low),
            },
            Kind::ReleaseCapacity(release) => SystemCommand::ReleaseCapacity {
                booking: release.booking,
            },
            Kind::JoinFleet(join) => SystemCommand::JoinFleet {
                fleet_id: join.fleet_id,
                cell_id: join.cell_id,
                version: join.version,
            },
            Kind::HostTenant(host) => SystemCommand::HostTenant {
                fleet_id: host.fleet_id,
                tenant: TenantId(host.tenant),
                hosted: HostedTenant {
                    control: JournalId(host.control_journal),
                    name: host.name,
                    survives: crate::tenant::Survives::from_wire(host.survives)?,
                },
            },
            Kind::DropTenant(drop) => SystemCommand::DropTenant {
                fleet_id: drop.fleet_id,
                tenant: TenantId(drop.tenant),
            },
        })
    }
}

/// A role a capacity booking holds a slot for (§3.2): its class is the
/// slot's. One slot holds one role instance of one journal or matchmaker
/// set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// A journal's acceptor.
    Acceptor,
    /// A journal's replica (a learner).
    Replica,
    /// A matchmaker of a matchmaker set.
    Matchmaker,
    /// A tenant's frontend, its entry role.
    Frontend,
    /// A tenant's resolver.
    Resolver,
    /// A journal's proxy leader.
    ProxyLeader,
    /// A journal's batcher.
    Batcher,
    /// A journal's unbatcher.
    Unbatcher,
    /// A coordinator.
    Coordinator,
}

impl Role {
    /// Every role, in a fixed order.
    pub const ALL: [Role; 9] = [
        Role::Acceptor,
        Role::Replica,
        Role::Matchmaker,
        Role::Frontend,
        Role::Resolver,
        Role::ProxyLeader,
        Role::Batcher,
        Role::Unbatcher,
        Role::Coordinator,
    ];

    /// The role's name on the wire.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Acceptor => "acceptor",
            Role::Replica => "replica",
            Role::Matchmaker => "matchmaker",
            Role::Frontend => "frontend",
            Role::Resolver => "resolver",
            Role::ProxyLeader => "proxy_leader",
            Role::Batcher => "batcher",
            Role::Unbatcher => "unbatcher",
            Role::Coordinator => "coordinator",
        }
    }

    /// The class of the machines that run it (FDB's classes, §3.2): the
    /// roles with a durable store are `storage`, every other one is
    /// `stateless`.
    #[must_use]
    pub fn class(self) -> Class {
        match self {
            Role::Acceptor | Role::Replica | Role::Matchmaker => Class::Storage,
            Role::Frontend
            | Role::Resolver
            | Role::ProxyLeader
            | Role::Batcher
            | Role::Unbatcher
            | Role::Coordinator => Class::Stateless,
        }
    }

    /// The roles of `class`, in [`Role::ALL`]'s order.
    pub fn of(class: Class) -> impl Iterator<Item = Role> {
        Role::ALL
            .into_iter()
            .filter(move |role| role.class() == class)
    }
}

impl core::str::FromStr for Role {
    type Err = &'static str;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Role::ALL
            .into_iter()
            .find(|role| role.as_str() == text)
            .ok_or("a role is one of the placement's roles")
    }
}

/// What a capacity booking is for (§3.2): a journal, or a matchmaker set of
/// a tenant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BookingTarget {
    /// A journal.
    Journal(JournalIdentifier),
    /// A matchmaker set of `tenant`.
    Set {
        /// The tenant.
        tenant: TenantId,
        /// The set's id within the tenant (never `0`).
        set: u64,
    },
}

impl BookingTarget {
    /// The target's tenant.
    #[must_use]
    pub fn tenant(self) -> TenantId {
        match self {
            BookingTarget::Journal(journal) => journal.tenant,
            BookingTarget::Set { tenant, .. } => tenant,
        }
    }

    /// The wire's `(tenant, journal, set)`: `set` is `0` for a journal and
    /// `journal` is `0` for a set.
    fn to_wire(self) -> (u64, u64, u64) {
        match self {
            BookingTarget::Journal(journal) => (journal.tenant.0, journal.journal.0, 0),
            BookingTarget::Set { tenant, set } => (tenant.0, 0, set),
        }
    }

    fn from_wire(tenant: u64, journal: u64, set: u64) -> Result<Self, &'static str> {
        match (journal, set) {
            (_, 0) => Ok(BookingTarget::Journal(JournalIdentifier::new(
                TenantId(tenant),
                JournalId(journal),
            ))),
            (0, set) => Ok(BookingTarget::Set {
                tenant: TenantId(tenant),
                set,
            }),
            _ => Err("a booking names a journal or a matchmaker set, not both"),
        }
    }
}

/// Where a registered node stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStanding {
    /// In the pool.
    Registered,
    /// In the pool, taking no new work.
    Draining,
    /// Out of the pool for good.
    Retired,
}

impl NodeStanding {
    fn to_wire(self) -> u32 {
        match self {
            NodeStanding::Registered => 0,
            NodeStanding::Draining => 1,
            NodeStanding::Retired => 2,
        }
    }

    fn from_wire(standing: u32) -> Result<Self, &'static str> {
        match standing {
            0 => Ok(NodeStanding::Registered),
            1 => Ok(NodeStanding::Draining),
            2 => Ok(NodeStanding::Retired),
            _ => Err("a node standing is registered, draining or retired"),
        }
    }
}

/// A node the registry registered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredNode {
    /// Its address (the latest registration's).
    pub addr: String,
    /// Its failure domain.
    pub failure_domain: String,
    /// Its class, fixed by its first registration.
    pub class: Class,
    /// The role slots of its class it advertises (the latest registration's).
    pub capacity: u64,
    /// Where it stands.
    pub standing: NodeStanding,
    /// How many registrations it made: 1, plus one per re-registration (a
    /// reboot).
    pub registrations: u64,
    /// Its RPC incarnation (the latest registration's; `0` unknown).
    pub incarnation: u128,
}

/// What the cell coordinator last wrote of one machine's liveness (#211,
/// D6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Liveness {
    /// The incarnation last seen (`0` unknown).
    pub incarnation: u128,
    /// Whether it was answering.
    pub up: bool,
}

/// One capacity booking: a slot of `node` held for `role` of `target`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Booking {
    /// The node whose slot it holds.
    pub node: NodeId,
    /// The role the slot holds.
    pub role: Role,
    /// The journal or matchmaker set it was booked for.
    pub target: BookingTarget,
}

impl Booking {
    /// The class of the slot: the role's, always the node's.
    #[must_use]
    pub fn class(&self) -> Class {
        self.role.class()
    }
}

/// What one registry record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryEvent {
    /// A node joined the pool.
    Registered {
        /// The node.
        id: NodeId,
        /// Its address.
        addr: String,
        /// Its class.
        class: Class,
        /// Its capacity.
        capacity: u64,
    },
    /// A registered node registered again (a reboot): its address and
    /// capacity are the new registration's.
    Reregistered {
        /// The node.
        id: NodeId,
        /// Its address now.
        addr: String,
        /// Its capacity now.
        capacity: u64,
    },
    /// A node is draining.
    Draining {
        /// The node.
        id: NodeId,
    },
    /// A node left the pool for good, its live bookings with it.
    Retired {
        /// The node.
        id: NodeId,
    },
    /// A slot of `node` was booked.
    Booked {
        /// The booking's id.
        booking: u64,
        /// The node.
        node: NodeId,
        /// The slot's role.
        role: Role,
    },
    /// The cell coordinator marked machine `id` down (#211).
    MachineDown {
        /// The machine.
        id: NodeId,
    },
    /// The cell coordinator marked machine `id` up as `incarnation`
    /// (#211): `was_down` when it was marked down before.
    MachineUp {
        /// The machine.
        id: NodeId,
        /// The incarnation that answered.
        incarnation: u128,
        /// Whether the registry held it down.
        was_down: bool,
    },
    /// A booking was released.
    Released {
        /// The booking's id.
        booking: u64,
        /// The node whose slot it freed.
        node: NodeId,
    },
    /// The cell joined a fleet (#229).
    JoinedFleet {
        /// The fleet.
        fleet_id: u64,
        /// This cell's id.
        cell_id: u64,
    },
    /// The cell hosts a tenant (#229).
    TenantHosted {
        /// The tenant.
        tenant: TenantId,
        /// Its control journal's id (#210).
        control: JournalId,
    },
    /// The cell forgot a tenant (#229).
    TenantDropped {
        /// The tenant.
        tenant: TenantId,
    },
    /// The entry asked for what the cell already records: a re-run step of
    /// a fleet operation (#229).
    Unchanged,
    /// A checkpoint (#230): every position below `covers_up_to` is in the
    /// state this fold now holds — restored from it, or verified against it
    /// (the fold reports which through its audit, not here, so every fold
    /// of the same position folds the same event).
    Checkpoint {
        /// The checkpoint's horizon, its own position.
        covers_up_to: u64,
    },
    /// The entry changed nothing.
    Refused(RegistryRefusal),
}

/// Why a registry entry changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryRefusal {
    /// Not exactly one decodable registry entry (or a checkpoint this fold
    /// cannot use).
    Malformed,
    /// The id is a retired one (ids are never reused).
    AlreadyKnown {
        /// The node named.
        id: NodeId,
    },
    /// A re-registration under another class: a machine's class is fixed by
    /// its first registration.
    ClassChanged {
        /// The node named.
        id: NodeId,
    },
    /// A drain, or a booking, of a node not registered and in the pool (a
    /// draining node takes no new work).
    NotRegistered {
        /// The node named.
        id: NodeId,
    },
    /// A retirement of a node that is not draining.
    NotDraining {
        /// The node named.
        id: NodeId,
    },
    /// A booking under an id that was ever booked (a live booking holds it,
    /// or one held it and was released): booking ids are never reused, and
    /// its writer redraws.
    BookingTaken {
        /// The id asked for.
        booking: u64,
    },
    /// A booking of a slot of the other class than the node's: a storage
    /// machine never takes stateless work, and the reverse.
    WrongClass {
        /// The node named.
        id: NodeId,
    },
    /// A booking of a node with no slot left.
    NoCapacity {
        /// The node named.
        id: NodeId,
    },
    /// A booking of a node that already holds a live booking for the same
    /// role of the same journal or set: one slot per role instance.
    AlreadyBooked {
        /// The node named.
        id: NodeId,
    },
    /// A liveness entry that changes nothing: `MachineDown` of a machine
    /// held down, or `MachineUp` of the incarnation held up (D6: changes
    /// only).
    LivenessUnchanged {
        /// The node named.
        id: NodeId,
    },
    /// A `MachineDown` naming another incarnation than the one the registry
    /// holds: a later incarnation answered since.
    StaleIncarnation {
        /// The node named.
        id: NodeId,
    },
    /// A liveness entry of a node that is not in the pool.
    NotInPool {
        /// The node named.
        id: NodeId,
    },
    /// A release of a booking that is not live.
    UnknownBooking {
        /// The id named.
        booking: u64,
    },
    /// A fleet entry naming another fleet (or cell) than the one the cell
    /// joined, or a tenant entry before the cell joined any (#229): a step
    /// of a fleet operation that talks to another fleet changes nothing.
    OtherFleet {
        /// The fleet the entry names.
        fleet_id: u64,
    },
    /// A fleet registration at a metadata version this fold does not
    /// understand (#229).
    Unsupported,
    /// A `HostTenant` of a tenant the cell dropped (#229): a dropped tenant
    /// is never hosted again.
    TenantDropped {
        /// The tenant named.
        tenant: TenantId,
    },
}

/// The registry's fold (#189, #211): the genesis pool the deployment was
/// booted with, every node registered at runtime keyed by its `node_id`
/// with where it stands, and every live capacity booking.
///
/// The fold enforces the placement's two rules at apply, where every
/// reader agrees on them: a booking takes a slot of the node's own class
/// ([`RegistryRefusal::WrongClass`]), and a node is never booked past its
/// capacity ([`RegistryRefusal::NoCapacity`]). It is
/// [`Checkpointable`]: its checkpoint is every runtime node and live
/// booking, never the history that made them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Registry {
    genesis: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, RegisteredNode>,
    bookings: BTreeMap<u64, Booking>,
    /// The fleet registration (#229): `(fleet_id, cell_id, version)`.
    fleet: Option<FleetRegistration>,
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
    next_seq: u64,
}

/// What the cell records of a tenant it hosts (#210): its control journal,
/// where every machine that serves the tenant learns its journals, and the
/// name and `survives` its first coordinator describes it with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedTenant {
    /// The tenant control journal's id, inside the tenant.
    pub control: JournalId,
    /// The tenant's name.
    pub name: Vec<u8>,
    /// What it survives (#252).
    pub survives: crate::tenant::Survives,
}

/// The cell side of the fleet's registration (#229, §3.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetRegistration {
    /// The fleet.
    pub fleet_id: u64,
    /// This cell's id.
    pub cell_id: u64,
    /// The metadata version.
    pub version: u32,
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
    /// [`Folder`](crate::client::checkpoint::Folder), which restores it.
    ///
    /// # Panics
    ///
    /// If `seq` is below a position already folded (a programmer error of the caller).
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> RegistryEvent {
        assert!(seq >= self.next_seq, "the registry folds in position order");
        self.next_seq = seq + 1;
        match SystemCommand::decode(record).ok() {
            Some(SystemCommand::RegisterNode {
                id,
                addr,
                class,
                capacity,
                failure_domain,
                incarnation,
            }) => self.register(id, addr, class, (capacity, incarnation), failure_domain),
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
            }) => self.join_fleet(FleetRegistration {
                fleet_id,
                cell_id,
                version,
            }),
            Some(SystemCommand::HostTenant {
                fleet_id,
                tenant,
                hosted,
            }) => self.host(fleet_id, tenant, Some(hosted)),
            Some(SystemCommand::DropTenant { fleet_id, tenant }) => {
                self.host(fleet_id, tenant, None)
            }
            _ => RegistryEvent::Refused(RegistryRefusal::Malformed),
        }
    }

    fn register(
        &mut self,
        id: NodeId,
        addr: String,
        class: Class,
        (capacity, incarnation): (u64, u128),
        failure_domain: String,
    ) -> RegistryEvent {
        // A genesis node (a founding member) registers too (#349): its
        // entry is the address peers dial, which may change across its
        // restarts. It stays in the pool whatever the registry says.
        match self.nodes.get_mut(&id) {
            Some(node) if node.standing == NodeStanding::Retired => {
                RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id })
            }
            Some(node) if node.class != class => {
                RegistryEvent::Refused(RegistryRefusal::ClassChanged { id })
            }
            Some(node) => {
                node.addr.clone_from(&addr);
                node.capacity = capacity;
                node.failure_domain = failure_domain;
                node.registrations += 1;
                node.incarnation = incarnation;
                // A registration is a machine that answered: up, as the
                // incarnation it names.
                self.liveness.insert(
                    id,
                    Liveness {
                        incarnation,
                        up: true,
                    },
                );
                RegistryEvent::Reregistered { id, addr, capacity }
            }
            None => {
                self.nodes.insert(
                    id,
                    RegisteredNode {
                        addr: addr.clone(),
                        failure_domain,
                        class,
                        capacity,
                        standing: NodeStanding::Registered,
                        registrations: 1,
                        incarnation,
                    },
                );
                self.liveness.insert(
                    id,
                    Liveness {
                        incarnation,
                        up: true,
                    },
                );
                RegistryEvent::Registered {
                    id,
                    addr,
                    class,
                    capacity,
                }
            }
        }
    }

    fn join_fleet(&mut self, joined: FleetRegistration) -> RegistryEvent {
        if joined.fleet_id == 0 || joined.cell_id == 0 {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if joined.version == 0 || joined.version > crate::fleet::METADATA_VERSION {
            return RegistryEvent::Refused(RegistryRefusal::Unsupported);
        }
        match self.fleet {
            None => {
                self.fleet = Some(joined);
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

    /// The cell side of the fleet's registration, once `init` wrote it
    /// (#229).
    #[must_use]
    pub fn fleet(&self) -> Option<FleetRegistration> {
        self.fleet
    }

    /// Whether the cell hosts `tenant` (#229).
    #[must_use]
    pub fn hosts(&self, tenant: TenantId) -> bool {
        self.hosted.contains_key(&tenant)
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
        if node.class != class {
            return RegistryEvent::Refused(RegistryRefusal::WrongClass { id });
        }
        if self.booked(id) >= node.capacity {
            return RegistryEvent::Refused(RegistryRefusal::NoCapacity { id });
        }
        if self
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
        assert!(
            self.booked(id) <= self.nodes.get(&id).map_or(0, |n| n.capacity),
            "a node is never booked past its capacity"
        );
        RegistryEvent::Booked {
            booking,
            node: id,
            role,
        }
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
            },
        );
        assert!(!self.liveness(id).up, "a machine marked down is held down");
        RegistryEvent::MachineDown { id }
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
        self.liveness.insert(
            id,
            Liveness {
                incarnation,
                up: true,
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
        })
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

    /// The slots of `id` its live bookings hold.
    #[must_use]
    pub fn booked(&self, id: NodeId) -> u64 {
        self.bookings.values().filter(|b| b.node == id).count() as u64
    }

    /// The slots `id` has left: its capacity less its live bookings (`0` for
    /// a node that is not registered and in the pool, or over-booked after a
    /// re-registration lowered its capacity).
    #[must_use]
    pub fn available(&self, id: NodeId) -> u64 {
        self.nodes
            .get(&id)
            .filter(|n| n.standing == NodeStanding::Registered)
            .map_or(0, |n| n.capacity.saturating_sub(self.booked(id)))
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

    fn state_to_wire(&self) -> wire::RegistryState {
        wire::RegistryState {
            nodes: self
                .nodes
                .iter()
                .map(|(id, n)| wire::RegisteredNodeState {
                    id: id.0,
                    addr: n.addr.clone(),
                    failure_domain: n.failure_domain.clone(),
                    class: n.class.as_str().into(),
                    capacity: n.capacity,
                    standing: n.standing.to_wire(),
                    registrations: n.registrations,
                    incarnation_high: incarnation_halves(n.incarnation).0,
                    incarnation_low: incarnation_halves(n.incarnation).1,
                })
                .collect(),
            bookings: self
                .bookings
                .iter()
                .map(|(id, b)| {
                    let (tenant, journal, set) = b.target.to_wire();
                    wire::BookingState {
                        booking: *id,
                        node: b.node.0,
                        tenant,
                        journal,
                        role: b.role.as_str().into(),
                        set,
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
                })
                .collect(),
            fleet_id: self.fleet.map_or(0, |f| f.fleet_id),
            cell_id: self.fleet.map_or(0, |f| f.cell_id),
            version: self.fleet.map_or(0, |f| f.version),
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
        }
    }
}

impl Checkpointable for Registry {
    type Event = RegistryEvent;

    fn apply(&mut self, seq: u64, record: &[u8]) -> RegistryEvent {
        self.fold(seq, record)
    }

    fn checkpoint(&self) -> Vec<u8> {
        self.state_to_wire().encode_to_vec()
    }

    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str> {
        let state =
            wire::RegistryState::decode(state).map_err(|_| "a registry state does not decode")?;
        let mut nodes = BTreeMap::new();
        for n in state.nodes {
            let id = NodeId(n.id);
            if self.genesis.contains(&id) {
                return Err("a registry state names a genesis node");
            }
            nodes.insert(
                id,
                RegisteredNode {
                    addr: n.addr,
                    failure_domain: n.failure_domain,
                    class: n.class.parse()?,
                    capacity: n.capacity,
                    standing: NodeStanding::from_wire(n.standing)?,
                    registrations: n.registrations,
                    incarnation: incarnation_from_halves(n.incarnation_high, n.incarnation_low),
                },
            );
        }
        let spent: BTreeSet<u64> = state.spent.into_iter().collect();
        let mut bookings = BTreeMap::new();
        for b in state.bookings {
            if !spent.contains(&b.booking) {
                return Err("a registry state holds a booking whose id is not spent");
            }
            bookings.insert(
                b.booking,
                Booking {
                    node: NodeId(b.node),
                    role: b.role.parse()?,
                    target: BookingTarget::from_wire(b.tenant, b.journal, b.set)?,
                },
            );
        }
        let mut liveness = BTreeMap::new();
        for l in state.liveness {
            liveness.insert(
                NodeId(l.id),
                Liveness {
                    incarnation: incarnation_from_halves(l.incarnation_high, l.incarnation_low),
                    up: l.up,
                },
            );
        }
        let fleet = match (state.fleet_id, state.cell_id) {
            (0, 0) => None,
            (0, _) | (_, 0) => return Err("a registry state names half a fleet registration"),
            (fleet_id, cell_id) => Some(FleetRegistration {
                fleet_id,
                cell_id,
                version: state.version,
            }),
        };
        let mut hosted = BTreeMap::new();
        for h in state.hosted {
            if h.control_journal == 0 {
                return Err("a registry state hosts a tenant with no control journal");
            }
            hosted.insert(
                TenantId(h.tenant),
                HostedTenant {
                    control: JournalId(h.control_journal),
                    name: h.name,
                    survives: crate::tenant::Survives::from_wire(h.survives)?,
                },
            );
        }
        let dropped: BTreeSet<TenantId> = state.dropped.into_iter().map(TenantId).collect();
        if hosted.keys().chain(&dropped).any(|t| !t.is_set())
            || hosted.keys().any(|t| dropped.contains(t))
            || (fleet.is_none() && !(hosted.is_empty() && dropped.is_empty()))
        {
            return Err("a registry state hosts a tenant it cannot");
        }
        self.nodes = nodes;
        self.bookings = bookings;
        self.spent = spent;
        self.liveness = liveness;
        self.fleet = fleet;
        self.hosted = hosted;
        self.dropped = dropped;
        self.next_seq = covers_up_to + 1;
        Ok(())
    }
}

/// The event a [`Folded`] registry record is reported as: an entry's own
/// event, a checkpoint's, or a refusal for a record the fold cannot use.
/// `None` for a record the fold skipped (above a gap) or is waiting on.
#[must_use]
pub fn registry_event(folded: Folded<RegistryEvent>) -> Option<RegistryEvent> {
    match folded {
        Folded::Entry(event) => Some(event),
        Folded::Checkpoint { covers_up_to, .. } => Some(RegistryEvent::Checkpoint { covers_up_to }),
        Folded::Unreadable(_) => Some(RegistryEvent::Refused(RegistryRefusal::Malformed)),
        Folded::NeedsRef(_) | Folded::Skipped => None,
    }
}

/// What one control-journal record folded to — the registry's or a
/// tenant control journal's event (#210) — as the driver reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemEvent {
    /// A cell control journal (registry) record.
    Registry(RegistryEvent),
    /// A tenant control journal record (#210).
    Tenant(crate::tenant::TenantEvent),
}

#[cfg(test)]
mod tests {
    use super::*;
    fn hosted(control: u64) -> HostedTenant {
        HostedTenant {
            control: JournalId(control),
            name: b"acme".to_vec(),
            survives: crate::tenant::Survives::Az,
        }
    }

    fn one(command: &SystemCommand) -> Vec<u8> {
        command.encode()
    }

    #[test]
    fn every_command_round_trips() {
        let commands = [
            SystemCommand::RegisterNode {
                id: NodeId(100),
                addr: "10.0.5.1:4500".into(),
                class: Class::Storage,
                capacity: 3,
                failure_domain: "rack-a".into(),
                incarnation: (7_u128 << 64) | 9,
            },
            SystemCommand::DrainNode { id: NodeId(100) },
            SystemCommand::RetireNode { id: NodeId(100) },
            SystemCommand::BookCapacity {
                booking: 7,
                node: NodeId(100),
                role: Role::Frontend,
                target: BookingTarget::Journal(JournalIdentifier::new(
                    TenantId(300),
                    JournalId(400),
                )),
            },
            SystemCommand::BookCapacity {
                booking: 8,
                node: NodeId(100),
                role: Role::Matchmaker,
                target: BookingTarget::Set {
                    tenant: TenantId(300),
                    set: 2,
                },
            },
            SystemCommand::MachineDown {
                id: NodeId(100),
                incarnation: u128::MAX,
            },
            SystemCommand::MachineUp {
                id: NodeId(100),
                incarnation: 1,
            },
            SystemCommand::ReleaseCapacity { booking: 7 },
            SystemCommand::JoinFleet {
                fleet_id: 11,
                cell_id: 12,
                version: 1,
            },
            SystemCommand::HostTenant {
                fleet_id: 11,
                tenant: TenantId(300),
                hosted: hosted(301),
            },
            SystemCommand::DropTenant {
                fleet_id: 11,
                tenant: TenantId(300),
            },
        ];
        for command in commands {
            assert_eq!(SystemCommand::decode(&command.encode()), Ok(command));
        }
        assert!(SystemCommand::decode(b"\xff\xff").is_err());
    }

    #[test]
    fn the_pool_grows_by_registration_and_shrinks_by_retirement() {
        let mut reg = Registry::new([NodeId(0), NodeId(1)]);
        let register = |id: u64| register(id, Class::Storage, 1);
        // A genesis node registers its address (#349), and stays in the
        // pool once.
        assert!(matches!(
            reg.fold(0, &register(0)),
            RegistryEvent::Registered { .. }
        ));
        assert_eq!(reg.address(NodeId(0)), Some("n0"));
        assert_eq!(reg.address(NodeId(1)), None);
        assert!(matches!(
            reg.fold(1, &register(100)),
            RegistryEvent::Registered { .. }
        ));
        assert_eq!(reg.pool(), vec![NodeId(0), NodeId(1), NodeId(100)]);
        // Retiring needs a drain first.
        assert_eq!(
            reg.fold(2, &one(&SystemCommand::RetireNode { id: NodeId(100) })),
            RegistryEvent::Refused(RegistryRefusal::NotDraining { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(3, &one(&SystemCommand::DrainNode { id: NodeId(100) })),
            RegistryEvent::Draining { id: NodeId(100) }
        );
        assert!(
            reg.contains(NodeId(100)),
            "a draining node stays in the pool"
        );
        assert_eq!(
            reg.fold(4, &one(&SystemCommand::RetireNode { id: NodeId(100) })),
            RegistryEvent::Retired { id: NodeId(100) }
        );
        assert!(!reg.contains(NodeId(100)));
        assert_eq!(reg.pool(), vec![NodeId(0), NodeId(1)]);
        // A retired id is never registered again, a re-registration
        // included.
        assert_eq!(
            reg.fold(5, &register(100)),
            RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id: NodeId(100) })
        );
        // Genesis nodes are neither drained nor retired.
        assert_eq!(
            reg.fold(6, &one(&SystemCommand::DrainNode { id: NodeId(0) })),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(0) })
        );
    }

    fn register(id: u64, class: Class, capacity: u64) -> Vec<u8> {
        one(&SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: format!("n{id}"),
            class,
            capacity,
            failure_domain: String::new(),
            incarnation: u128::from(id),
        })
    }

    fn book(booking: u64, node: u64, class: Class) -> Vec<u8> {
        let role = Role::of(class).next().expect("every class has a role");
        one(&SystemCommand::BookCapacity {
            booking,
            node: NodeId(node),
            role,
            target: BookingTarget::Journal(JournalIdentifier::new(
                TenantId(300),
                JournalId(booking),
            )),
        })
    }

    fn liveness(id: u64, incarnation: u128, up: bool) -> Vec<u8> {
        let id = NodeId(id);
        one(&if up {
            SystemCommand::MachineUp { id, incarnation }
        } else {
            SystemCommand::MachineDown { id, incarnation }
        })
    }

    #[test]
    fn every_role_names_its_class_and_parses_back() {
        for role in Role::ALL {
            assert_eq!(role.as_str().parse::<Role>(), Ok(role));
            assert!(Role::of(role.class()).any(|r| r == role));
        }
        assert_eq!(Role::of(Class::Storage).count(), 3);
        assert!("seed".parse::<Role>().is_err());
        assert!(BookingTarget::from_wire(1, 2, 3).is_err());
    }

    #[test]
    fn a_booking_id_is_never_booked_twice_even_after_its_release() {
        let mut reg = Registry::new([NodeId(0)]);
        reg.fold(0, &register(100, Class::Storage, 4));
        assert!(matches!(
            reg.fold(1, &book(1, 100, Class::Storage)),
            RegistryEvent::Booked { booking: 1, .. }
        ));
        reg.fold(2, &one(&SystemCommand::ReleaseCapacity { booking: 1 }));
        assert_eq!(
            reg.fold(3, &book(1, 100, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking: 1 })
        );
        assert_eq!(reg.spent().collect::<Vec<_>>(), vec![1]);
        // One slot per role instance: the same role of the same journal
        // twice on one node is refused, another role is not.
        let again = |booking, role| {
            one(&SystemCommand::BookCapacity {
                booking,
                node: NodeId(100),
                role,
                target: BookingTarget::Journal(JournalIdentifier::new(TenantId(300), JournalId(9))),
            })
        };
        assert!(matches!(
            reg.fold(4, &again(2, Role::Acceptor)),
            RegistryEvent::Booked { .. }
        ));
        assert_eq!(
            reg.fold(5, &again(3, Role::Acceptor)),
            RegistryEvent::Refused(RegistryRefusal::AlreadyBooked { id: NodeId(100) })
        );
        assert!(matches!(
            reg.fold(6, &again(4, Role::Replica)),
            RegistryEvent::Booked { .. }
        ));
        // The spent ids cross a checkpoint.
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(6, &reg.checkpoint())
            .expect("a registry's own checkpoint restores");
        assert_eq!(
            restored,
            Registry {
                next_seq: 7,
                ..reg.clone()
            }
        );
        assert_eq!(
            restored.fold(7, &book(1, 100, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking: 1 })
        );
    }

    #[test]
    fn liveness_entries_are_changes_only() {
        let mut reg = Registry::new([NodeId(0)]);
        reg.fold(0, &register(100, Class::Storage, 1));
        // A registration is up, as its incarnation.
        assert_eq!(
            reg.fold(1, &liveness(100, 100, true)),
            RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(2, &liveness(100, 5, false)),
            RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(3, &liveness(100, 100, false)),
            RegistryEvent::MachineDown { id: NodeId(100) }
        );
        assert!(!reg.seen_alive(NodeId(100)));
        // No `Down` after `Down`.
        assert_eq!(
            reg.fold(4, &liveness(100, 100, false)),
            RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
        );
        // Back as a new incarnation: a reboot.
        assert_eq!(
            reg.fold(5, &liveness(100, 101, true)),
            RegistryEvent::MachineUp {
                id: NodeId(100),
                incarnation: 101,
                was_down: true
            }
        );
        assert_eq!(reg.get(NodeId(100)).map(|n| n.incarnation), Some(101));
        // A genesis node is watched too; an unknown node is not.
        assert_eq!(
            reg.fold(6, &liveness(0, 9, true)),
            RegistryEvent::MachineUp {
                id: NodeId(0),
                incarnation: 9,
                was_down: false
            }
        );
        assert_eq!(
            reg.fold(7, &liveness(7, 9, true)),
            RegistryEvent::Refused(RegistryRefusal::NotInPool { id: NodeId(7) })
        );
        assert!(reg.seen_alive(NodeId(0)) && reg.seen_alive(NodeId(100)));
    }

    #[test]
    fn a_reboot_registers_again_under_the_same_class() {
        let mut reg = Registry::new([NodeId(0)]);
        assert!(matches!(
            reg.fold(0, &register(100, Class::Storage, 2)),
            RegistryEvent::Registered { .. }
        ));
        assert_eq!(
            reg.fold(1, &register(100, Class::Storage, 5)),
            RegistryEvent::Reregistered {
                id: NodeId(100),
                addr: "n100".into(),
                capacity: 5
            }
        );
        let node = reg.get(NodeId(100)).expect("registered");
        assert_eq!((node.capacity, node.registrations), (5, 2));
        assert_eq!(
            reg.fold(2, &register(100, Class::Stateless, 5)),
            RegistryEvent::Refused(RegistryRefusal::ClassChanged { id: NodeId(100) })
        );
    }

    #[test]
    fn a_booking_takes_a_slot_of_the_nodes_own_class_within_its_capacity() {
        let mut reg = Registry::new([NodeId(0)]);
        reg.fold(0, &register(100, Class::Storage, 2));
        reg.fold(1, &register(200, Class::Stateless, 1));
        // A storage machine never takes stateless work, and the reverse.
        assert_eq!(
            reg.fold(2, &book(1, 100, Class::Stateless)),
            RegistryEvent::Refused(RegistryRefusal::WrongClass { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(3, &book(1, 200, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::WrongClass { id: NodeId(200) })
        );
        // A genesis node, or one never registered, is not bookable.
        assert_eq!(
            reg.fold(4, &book(1, 0, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(0) })
        );
        assert!(matches!(
            reg.fold(5, &book(1, 100, Class::Storage)),
            RegistryEvent::Booked { booking: 1, .. }
        ));
        // A live booking id is booked at most once.
        assert_eq!(
            reg.fold(6, &book(1, 100, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking: 1 })
        );
        assert!(matches!(
            reg.fold(7, &book(2, 100, Class::Storage)),
            RegistryEvent::Booked { booking: 2, .. }
        ));
        assert_eq!(reg.available(NodeId(100)), 0);
        // Capacity is honoured.
        assert_eq!(
            reg.fold(8, &book(3, 100, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::NoCapacity { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(9, &one(&SystemCommand::ReleaseCapacity { booking: 1 })),
            RegistryEvent::Released {
                booking: 1,
                node: NodeId(100)
            }
        );
        assert_eq!(
            reg.fold(10, &one(&SystemCommand::ReleaseCapacity { booking: 1 })),
            RegistryEvent::Refused(RegistryRefusal::UnknownBooking { booking: 1 })
        );
        assert_eq!(reg.available(NodeId(100)), 1);
        // A draining node takes no new work; a retired one drops its bookings.
        reg.fold(11, &one(&SystemCommand::DrainNode { id: NodeId(100) }));
        assert_eq!(
            reg.fold(12, &book(3, 100, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(100) })
        );
        reg.fold(13, &one(&SystemCommand::RetireNode { id: NodeId(100) }));
        assert_eq!(reg.bookings().count(), 0);
    }

    #[test]
    fn a_registry_restored_from_its_checkpoint_is_the_registry_folded_whole() {
        use crate::client::checkpoint::{CheckpointRecord, Folded, Folder};
        let records = [
            register(100, Class::Storage, 2),
            register(200, Class::Stateless, 1),
            book(1, 100, Class::Storage),
            register(100, Class::Storage, 3),
            one(&SystemCommand::DrainNode { id: NodeId(200) }),
        ];
        let mut whole = Folder::new(Registry::new([NodeId(0)]));
        for (seq, record) in (0..).zip(&records) {
            assert!(matches!(whole.fold(seq, record), Some(Folded::Entry(_))));
        }
        let at = records.len() as u64;
        let checkpoint = CheckpointRecord::Inline {
            covers_up_to: at,
            chunks: vec![whole.state().checkpoint()],
        }
        .encode();
        // The fold that held every position verifies the checkpoint.
        assert_eq!(
            whole.fold(at, &checkpoint),
            Some(Folded::Checkpoint {
                covers_up_to: at,
                verified: Some(true)
            })
        );
        // A fold that jumped to the floor restores from it, and agrees.
        let mut restored = Folder::new(Registry::new([NodeId(0)]));
        restored.jump(at);
        assert!(!restored.is_whole());
        assert_eq!(
            restored.fold(at, &checkpoint),
            Some(Folded::Checkpoint {
                covers_up_to: at,
                verified: None
            })
        );
        assert!(restored.is_whole());
        assert_eq!(restored.state().checkpoint(), whole.state().checkpoint());
        let next = register(300, Class::Storage, 1);
        assert_eq!(whole.fold(at + 1, &next), restored.fold(at + 1, &next));
        assert_eq!(restored.state(), whole.state());
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn the_cell_joins_one_fleet_and_hosts_tenants_only_for_it() {
        let fleet = 0xf1ee7;
        let cell = 0xce11;
        let join = |fleet_id, cell_id, version| {
            one(&SystemCommand::JoinFleet {
                fleet_id,
                cell_id,
                version,
            })
        };
        let host = |fleet_id, tenant: u64| {
            one(&SystemCommand::HostTenant {
                fleet_id,
                tenant: TenantId(tenant),
                hosted: hosted(tenant + 1),
            })
        };
        let drop = |fleet_id, tenant| {
            one(&SystemCommand::DropTenant {
                fleet_id,
                tenant: TenantId(tenant),
            })
        };
        let mut registry = Registry::new([NodeId(0)]);
        // Nothing is hosted before the cell joins a fleet.
        assert_eq!(
            registry.fold(0, &host(fleet, 300)),
            RegistryEvent::Refused(RegistryRefusal::OtherFleet { fleet_id: fleet })
        );
        assert_eq!(
            registry.fold(1, &join(fleet, cell, crate::fleet::METADATA_VERSION + 1)),
            RegistryEvent::Refused(RegistryRefusal::Unsupported)
        );
        assert_eq!(
            registry.fold(2, &join(fleet, cell, 1)),
            RegistryEvent::JoinedFleet {
                fleet_id: fleet,
                cell_id: cell
            }
        );
        assert_eq!(
            registry.fold(3, &join(fleet, cell, 1)),
            RegistryEvent::Unchanged
        );
        assert_eq!(
            registry.fold(4, &join(fleet, cell + 1, 1)),
            RegistryEvent::Refused(RegistryRefusal::OtherFleet { fleet_id: fleet })
        );
        assert_eq!(
            registry.fold(5, &host(fleet, 300)),
            RegistryEvent::TenantHosted {
                tenant: TenantId(300),
                control: JournalId(301),
            }
        );
        assert_eq!(
            registry.fold(6, &host(fleet, 300)),
            RegistryEvent::Unchanged
        );
        assert_eq!(
            registry.fold(7, &host(fleet + 1, 301)),
            RegistryEvent::Refused(RegistryRefusal::OtherFleet {
                fleet_id: fleet + 1
            })
        );
        assert_eq!(
            registry.fold(8, &host(fleet, 0)),
            RegistryEvent::Refused(RegistryRefusal::Malformed)
        );
        assert!(registry.hosts(TenantId(300)));
        // The checkpoint carries the registration and the tenant list.
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(8, &registry.checkpoint())
            .expect("restores");
        assert_eq!(restored, registry);
        assert_eq!(
            registry.fold(9, &drop(fleet, 300)),
            RegistryEvent::TenantDropped {
                tenant: TenantId(300)
            }
        );
        assert_eq!(
            registry.fold(10, &drop(fleet, 300)),
            RegistryEvent::Unchanged
        );
        assert_eq!(registry.hosted().count(), 0);
        // A dropped tenant is never hosted again: a stale host is refused.
        assert_eq!(
            registry.fold(11, &host(fleet, 300)),
            RegistryEvent::Refused(RegistryRefusal::TenantDropped {
                tenant: TenantId(300)
            })
        );
        // A drop of a tenant never hosted still tombstones it.
        assert_eq!(
            registry.fold(12, &drop(fleet, 301)),
            RegistryEvent::TenantDropped {
                tenant: TenantId(301)
            }
        );
        assert!(registry.dropped(TenantId(301)));
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(12, &registry.checkpoint())
            .expect("restores");
        assert_eq!(restored, registry);
    }
}
