//! What the registry records of a node: its roles and bookings, its standing, its liveness and slow facts, and the maintenance windows (#359).

use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};

use crate::machine::{Class, Locality, machine_tags};
use crate::tags::{Tags, UserTags};

// The cell control journal has no fixed identifier (§3.8): a deployment draws
// it and hands it to whoever folds it (`paros_roles::node::ControlPlan`).

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

    /// The roles a machine of `class` takes, in [`Role::ALL`]'s order.
    pub fn of(class: Class) -> impl Iterator<Item = Role> {
        Role::ALL
            .into_iter()
            .filter(move |role| class.takes(role.class()))
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
    /// `tenant` as a whole (#476): its minimum footprint (`docs/architecture/03-4-tenant-modes.md`
    /// §3.4), the slots its birth booked against the cell's capacity, whose
    /// `Coordinator` slot is where the cell coordinator placed the tenant's
    /// coordinator, and its tenant-wide roles (its frontends). Unlike a
    /// journal's or a set's role instance, a tenant may hold several slots of
    /// one role on one machine.
    Tenant {
        /// The tenant.
        tenant: TenantId,
    },
}

impl BookingTarget {
    /// The target's tenant.
    #[must_use]
    pub fn tenant(self) -> TenantId {
        match self {
            BookingTarget::Journal(journal) => journal.tenant,
            BookingTarget::Set { tenant, .. } | BookingTarget::Tenant { tenant } => tenant,
        }
    }

    /// The wire's `(tenant, journal, set, tenant_level)`: `set` is `0` for a
    /// journal, `journal` is `0` for a set, and both are `0` for the tenant
    /// as a whole.
    pub(crate) fn to_wire(self) -> (u64, u64, u64, bool) {
        match self {
            BookingTarget::Journal(journal) => (journal.tenant.0, journal.journal.0, 0, false),
            BookingTarget::Set { tenant, set } => (tenant.0, 0, set, false),
            BookingTarget::Tenant { tenant } => (tenant.0, 0, 0, true),
        }
    }

    pub(crate) fn from_wire(
        tenant: u64,
        journal: u64,
        set: u64,
        tenant_level: bool,
    ) -> Result<Self, &'static str> {
        if tenant_level {
            if journal != 0 || set != 0 {
                return Err("a tenant's booking names no journal and no set");
            }
            return Ok(BookingTarget::Tenant {
                tenant: TenantId(tenant),
            });
        }
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
    pub(crate) fn to_wire(self) -> u32 {
        match self {
            NodeStanding::Registered => 0,
            NodeStanding::Draining => 1,
            NodeStanding::Retired => 2,
        }
    }

    pub(crate) fn from_wire(standing: u32) -> Result<Self, &'static str> {
        match standing {
            0 => Ok(NodeStanding::Registered),
            1 => Ok(NodeStanding::Draining),
            2 => Ok(NodeStanding::Retired),
            _ => Err("a node standing is registered, draining or retired"),
        }
    }
}

/// What a maintenance window covers (#359): one machine, or every machine
/// of one failure domain.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MaintenanceScope {
    /// One machine.
    Machine(NodeId),
    /// Every machine of a failure domain (a zone).
    Zone(String),
}

impl MaintenanceScope {
    pub(crate) fn from_wire(node: u64, zone: String) -> Result<Self, &'static str> {
        match (node, zone.is_empty()) {
            (0, false) => Ok(Self::Zone(zone)),
            (node, true) if node != 0 => Ok(Self::Machine(NodeId(node))),
            _ => Err("a maintenance window covers one machine or one zone"),
        }
    }

    pub(crate) fn to_wire(&self) -> (u64, String) {
        match self {
            Self::Machine(id) => (id.0, String::new()),
            Self::Zone(zone) => (0, zone.clone()),
        }
    }

    /// Whether the window covers machine `id` of failure domain `zone`.
    #[must_use]
    pub fn covers(&self, id: NodeId, zone: &str) -> bool {
        match self {
            Self::Machine(machine) => *machine == id,
            Self::Zone(domain) => domain == zone,
        }
    }
}

/// An open maintenance window (#359): its length and the position of the
/// entry that opened it. A coordinator measures it on its own clock from
/// when it first folds that entry: the registry holds no time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceWindow {
    /// How long it lasts, in milliseconds.
    pub for_ms: u64,
    /// The position of the entry that opened it: a later entry for the same
    /// scope opens a new window.
    pub seq: u64,
}

/// A node the registry registered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredNode {
    /// Its address (the latest registration's).
    pub addr: String,
    /// Its zone and rack (the latest registration's; #575: reserved tags).
    pub locality: Locality,
    /// Its name, for people (#399; the latest registration's): a label,
    /// never its identity.
    pub name: String,
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
    /// Its machine interface (#404 (static vs dynamic endpoints); the latest
    /// registration's, empty when it serves none): the encoded
    /// `InterfaceRef` a peer dials `Deliver` through.
    pub machine_interface: Vec<u8>,
    /// Whether the cell coordinator saw it answer as a machine of this cell
    /// (`MachineAdmitted`, #511 (placement books machines that never
    /// served)). A registration comes before the `Admit` it may never win.
    pub admitted: bool,
    /// Its user tags (#575): a re-registration keeps them.
    pub tags: UserTags,
}

impl RegisteredNode {
    /// Every tag it carries: its reserved tags and its user tags.
    #[must_use]
    pub fn all_tags(&self) -> Tags {
        self.tags
            .tags()
            .clone()
            .with(machine_tags(self.class, &self.locality))
    }
}

/// What the cell coordinator last wrote of one machine's liveness (#211,
/// D6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Liveness {
    /// The incarnation last seen (`0` unknown).
    pub incarnation: u128,
    /// Whether it was answering.
    pub up: bool,
    /// Whether it was judged slow (#361 (slow machines)): alive, but its
    /// roles move off it like a drain's.
    pub slow: bool,
    /// Whether it was judged low on space (#360 (disk full)): its disk's
    /// free bytes below the free-bytes floor, so placement books it last.
    pub low_space: bool,
}

/// The latency facts a slow judgement read (#361 (slow machines)): what the
/// cell coordinator measured of one machine in one watch round.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlowFacts {
    /// Its `Identify` round trip, in microseconds, on the coordinator's
    /// clock.
    pub rtt_us: u64,
    /// Its disk's busy ratio, in thousandths, from its last `Load` window.
    pub disk_busy_permille: u32,
    /// Its CPU's busy ratio, in thousandths, from its last `Load` window.
    pub cpu_busy_permille: u32,
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

    /// Whether the booking reserves a slot of the machine's capacity. A
    /// journal's proxy leader or batcher does not (decided on 2026-10-28,
    /// #753 (per-journal proxies)): it counts for the spread, and idle
    /// journals never fill a cell's `stateless` machines.
    #[must_use]
    pub fn weighs(&self) -> bool {
        Self::weighs_for(self.role, self.target)
    }

    /// Whether a booking of `role` for `target` reserves a slot
    /// ([`Booking::weighs`]).
    #[must_use]
    pub fn weighs_for(role: Role, target: BookingTarget) -> bool {
        !(matches!(target, BookingTarget::Journal(_))
            && matches!(role, Role::ProxyLeader | Role::Batcher))
    }
}
