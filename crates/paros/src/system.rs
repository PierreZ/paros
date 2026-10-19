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
use crate::machine::{
    Locality, incarnation_from_halves, incarnation_halves, machine_tags, parse_machine_tags,
};
use crate::tags::{Tag, Tags, UserTags};

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
        /// Its zone and rack (#575: reserved tags; placement reads the
        /// zone).
        locality: Locality,
        /// Its name, for people (#399): a label, never its identity.
        name: String,
        /// Its RPC incarnation (`0` unknown): with `addr`, its
        /// `InterfaceRef` identity.
        incarnation: u128,
        /// Its machine interface for that incarnation (#404 (static vs
        /// dynamic endpoints)): the encoded `InterfaceRef` its peers dial
        /// `Deliver` through, empty when it serves none. Opaque here.
        machine_interface: Vec<u8>,
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
    /// The cell coordinator judged machine `id`, as `incarnation`, slow or
    /// fast again (#361 (slow machines)), from `facts`.
    MachineSlow {
        /// The machine.
        id: NodeId,
        /// The incarnation the registry holds for it.
        incarnation: u128,
        /// Whether it is slow.
        slow: bool,
        /// The latency facts the judgement read.
        facts: SlowFacts,
    },
    /// The cell coordinator judged machine `id`, as `incarnation`, low on
    /// space or not any more (#360 (disk full)): its disk had `free_bytes`
    /// free, against the free-bytes floor.
    MachineSpace {
        /// The machine.
        id: NodeId,
        /// The incarnation the registry holds for it.
        incarnation: u128,
        /// Whether it is low on space.
        low: bool,
        /// The free bytes its last `Load` answer reported.
        free_bytes: u64,
    },
    /// The cell coordinator saw machine `id` answer as `incarnation`
    /// (#211, D6).
    MachineUp {
        /// The machine.
        id: NodeId,
        /// The incarnation that answered.
        incarnation: u128,
    },
    /// The cell coordinator saw registered machine `id` answer `Identify`
    /// as a machine of this cell (#511 (placement books machines that never
    /// served)): its admission landed, and placement may book it.
    MachineAdmitted {
        /// The machine.
        id: NodeId,
    },
    /// Open (or end, `for_ms == 0`) an operator's maintenance window over
    /// a machine or a failure domain (#359): the cell coordinator re-places
    /// no role of a machine it covers while the window is open.
    Maintenance {
        /// The machine or the failure domain.
        scope: MaintenanceScope,
        /// How long the window lasts, from when the coordinator first folds
        /// the entry; `0` ends it.
        for_ms: u64,
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
        /// Where the universe directory is, when this cell does not host
        /// the universe tenant (#232 (multiple cells)): `universe add-cell`.
        universe: Option<UniversePointer>,
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
    /// A capacity request's outcome (#476): every slot of `slots` is booked
    /// for `tenant`, or none is. `None` records that its writer found no
    /// room. The first grant under a key wins; a repeat changes nothing.
    GrantCapacity {
        /// What it is recorded under.
        key: GrantKey,
        /// The tenant the capacity is for: every slot's target is its.
        tenant: TenantId,
        /// The slots, or `None` for a refusal.
        slots: Option<Vec<Slot>>,
    },
    /// Request `request`: registered machine `id`'s user tags become `tags`
    /// (#575), all of them at once.
    SetMachineTags {
        /// The caller's idempotency id: applied once.
        request: u64,
        /// The machine.
        id: NodeId,
        /// Its user tags, all of them.
        tags: UserTags,
    },
}

/// One slot a grant books (#476): what one `BookCapacity` names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot {
    /// The id its writer drew; never booked before.
    pub booking: u64,
    /// The node.
    pub node: NodeId,
    /// The role the slot holds.
    pub role: Role,
    /// What the slot is for.
    pub target: BookingTarget,
}

impl Slot {
    fn to_wire(self) -> wire::BookCapacity {
        let (tenant, journal, set, tenant_level) = self.target.to_wire();
        wire::BookCapacity {
            booking: self.booking,
            node: self.node.0,
            tenant,
            journal,
            role: self.role.as_str().into(),
            set,
            tenant_level,
        }
    }

    fn from_wire(book: &wire::BookCapacity) -> Result<Self, &'static str> {
        Ok(Slot {
            booking: book.booking,
            node: NodeId(book.node),
            role: book.role.parse()?,
            target: BookingTarget::from_wire(
                book.tenant,
                book.journal,
                book.set,
                book.tenant_level,
            )?,
        })
    }
}

/// What a grant is recorded under (#476): a tenant's footprint, booked at
/// its birth, or a capacity request's idempotency id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GrantKey {
    /// The minimum footprint of the tenant (`docs/architecture/03-4-tenant-modes.md` §3.4).
    Footprint(TenantId),
    /// A capacity request, by the id its requester drew (never `0`).
    Request(u64),
}

/// A recorded grant (#476).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    /// The tenant it is for.
    pub tenant: TenantId,
    /// The booking ids it granted, or `None` when it was refused.
    pub bookings: Option<Vec<u64>>,
}

impl Grant {
    /// Whether the capacity was granted.
    #[must_use]
    pub fn granted(&self) -> bool {
        self.bookings.is_some()
    }
}

/// The record of a grant (#476 (tenant footprint)): its key, its tenant,
/// and its slots, or none when refused.
fn grant_to_wire(key: GrantKey, tenant: TenantId, slots: Option<&[Slot]>) -> wire::GrantCapacity {
    let (request, footprint) = match key {
        GrantKey::Footprint(_) => (0, true),
        GrantKey::Request(request) => (request, false),
    };
    wire::GrantCapacity {
        request,
        tenant: tenant.0,
        footprint,
        refused: slots.is_none(),
        bookings: slots
            .unwrap_or_default()
            .iter()
            .map(|slot| slot.to_wire())
            .collect(),
    }
}

impl SystemCommand {
    /// The record a client writes: exactly one per position.
    #[must_use]
    #[allow(clippy::too_many_lines)] // one arm per command
    pub fn encode(&self) -> Vec<u8> {
        use wire::system_entry::Kind;
        let kind = match self {
            SystemCommand::RegisterNode {
                id,
                addr,
                class,
                capacity,
                locality,
                name,
                incarnation,
                machine_interface,
            } => Kind::RegisterNode(wire::RegisterNode {
                id: id.0,
                addr: addr.clone(),
                name: name.clone(),
                tags: machine_tags(*class, locality)
                    .iter()
                    .map(Tag::to_wire)
                    .collect(),
                capacity: *capacity,
                incarnation_high: incarnation_halves(*incarnation).0,
                incarnation_low: incarnation_halves(*incarnation).1,
                machine_interface: machine_interface.clone(),
            }),
            SystemCommand::DrainNode { id } => Kind::DrainNode(wire::DrainNode { id: id.0 }),
            SystemCommand::RetireNode { id } => Kind::RetireNode(wire::RetireNode { id: id.0 }),
            SystemCommand::BookCapacity {
                booking,
                node,
                role,
                target,
            } => Kind::BookCapacity(
                Slot {
                    booking: *booking,
                    node: *node,
                    role: *role,
                    target: *target,
                }
                .to_wire(),
            ),
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
            SystemCommand::MachineSlow {
                id,
                incarnation,
                slow,
                facts,
            } => {
                let (incarnation_high, incarnation_low) = incarnation_halves(*incarnation);
                Kind::MachineSlow(wire::MachineSlow {
                    id: id.0,
                    incarnation_high,
                    incarnation_low,
                    slow: *slow,
                    rtt_us: facts.rtt_us,
                    disk_busy_permille: facts.disk_busy_permille,
                    cpu_busy_permille: facts.cpu_busy_permille,
                })
            }
            SystemCommand::MachineSpace {
                id,
                incarnation,
                low,
                free_bytes,
            } => {
                let (incarnation_high, incarnation_low) = incarnation_halves(*incarnation);
                Kind::MachineSpace(wire::MachineSpace {
                    id: id.0,
                    incarnation_high,
                    incarnation_low,
                    low: *low,
                    free_bytes: *free_bytes,
                })
            }
            SystemCommand::MachineAdmitted { id } => {
                Kind::MachineAdmitted(wire::MachineAdmitted { id: id.0 })
            }
            SystemCommand::Maintenance { scope, for_ms } => {
                let (node, zone) = scope.to_wire();
                Kind::Maintenance(wire::Maintenance {
                    node,
                    zone,
                    for_ms: *for_ms,
                })
            }
            SystemCommand::SetMachineTags { request, id, tags } => {
                Kind::SetMachineTags(wire::SetMachineTags {
                    request: *request,
                    id: id.0,
                    tags: tags.to_wire(),
                })
            }
            SystemCommand::ReleaseCapacity { booking } => {
                Kind::ReleaseCapacity(wire::ReleaseCapacity { booking: *booking })
            }
            SystemCommand::JoinFleet {
                fleet_id,
                cell_id,
                version,
                universe,
            } => Kind::JoinFleet(join_to_wire(
                *fleet_id,
                *cell_id,
                *version,
                universe.as_ref(),
            )),
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
            SystemCommand::GrantCapacity { key, tenant, slots } => {
                Kind::GrantCapacity(grant_to_wire(*key, *tenant, slots.as_deref()))
            }
        };
        wire::SystemEntry { kind: Some(kind) }.encode_to_vec()
    }

    /// Read one record back.
    ///
    /// # Errors
    ///
    /// The record is not a system entry, names no kind, or carries a
    /// configuration that does not admit its quorum system.
    #[allow(clippy::too_many_lines)] // one arm per command
    pub fn decode(record: &[u8]) -> Result<Self, &'static str> {
        use wire::system_entry::Kind;
        let entry = wire::SystemEntry::decode(record).map_err(|_| "not a system entry")?;
        Ok(match entry.kind.ok_or("a system entry names no kind")? {
            Kind::RegisterNode(register) => {
                let (class, locality, user) = Tags::from_wire(register.tags)
                    .and_then(parse_machine_tags)
                    .map_err(|_| "a registration's tags are malformed or unknown")?;
                if !user.is_empty() {
                    return Err("a registration carries its reserved tags only");
                }
                SystemCommand::RegisterNode {
                    id: NodeId(register.id),
                    addr: register.addr,
                    class,
                    capacity: register.capacity,
                    locality,
                    name: register.name,
                    incarnation: incarnation_from_halves(
                        register.incarnation_high,
                        register.incarnation_low,
                    ),
                    machine_interface: register.machine_interface,
                }
            }
            Kind::DrainNode(drain) => SystemCommand::DrainNode {
                id: NodeId(drain.id),
            },
            Kind::RetireNode(retire) => SystemCommand::RetireNode {
                id: NodeId(retire.id),
            },
            Kind::BookCapacity(book) => {
                let slot = Slot::from_wire(&book)?;
                SystemCommand::BookCapacity {
                    booking: slot.booking,
                    node: slot.node,
                    role: slot.role,
                    target: slot.target,
                }
            }
            Kind::MachineDown(down) => SystemCommand::MachineDown {
                id: NodeId(down.id),
                incarnation: incarnation_from_halves(down.incarnation_high, down.incarnation_low),
            },
            Kind::MachineUp(up) => SystemCommand::MachineUp {
                id: NodeId(up.id),
                incarnation: incarnation_from_halves(up.incarnation_high, up.incarnation_low),
            },
            Kind::MachineSlow(slow) => SystemCommand::MachineSlow {
                id: NodeId(slow.id),
                incarnation: incarnation_from_halves(slow.incarnation_high, slow.incarnation_low),
                slow: slow.slow,
                facts: SlowFacts {
                    rtt_us: slow.rtt_us,
                    disk_busy_permille: slow.disk_busy_permille,
                    cpu_busy_permille: slow.cpu_busy_permille,
                },
            },
            Kind::MachineSpace(space) => SystemCommand::MachineSpace {
                id: NodeId(space.id),
                incarnation: incarnation_from_halves(space.incarnation_high, space.incarnation_low),
                low: space.low,
                free_bytes: space.free_bytes,
            },
            Kind::MachineAdmitted(admitted) => SystemCommand::MachineAdmitted {
                id: NodeId(admitted.id),
            },
            Kind::Maintenance(window) => SystemCommand::Maintenance {
                scope: MaintenanceScope::from_wire(window.node, window.zone)?,
                for_ms: window.for_ms,
            },
            Kind::SetMachineTags(set) => SystemCommand::SetMachineTags {
                request: set.request,
                id: NodeId(set.id),
                tags: UserTags::from_wire(set.tags)
                    .map_err(|_| "a user tag set is malformed, reserved or over the cap")?,
            },
            Kind::ReleaseCapacity(release) => SystemCommand::ReleaseCapacity {
                booking: release.booking,
            },
            Kind::JoinFleet(join) => SystemCommand::JoinFleet {
                fleet_id: join.fleet_id,
                cell_id: join.cell_id,
                version: join.version,
                universe: pointer_from_wire(
                    join.directory_tenant,
                    join.directory_journal,
                    join.hosts,
                )?,
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
            Kind::GrantCapacity(grant) => {
                let key = if grant.footprint {
                    if grant.request != 0 {
                        return Err("a footprint grant names no request");
                    }
                    GrantKey::Footprint(TenantId(grant.tenant))
                } else {
                    GrantKey::Request(grant.request)
                };
                if grant.refused && !grant.bookings.is_empty() {
                    return Err("a refused grant books nothing");
                }
                let slots = if grant.refused {
                    None
                } else {
                    Some(
                        grant
                            .bookings
                            .iter()
                            .map(Slot::from_wire)
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                };
                SystemCommand::GrantCapacity {
                    key,
                    tenant: TenantId(grant.tenant),
                    slots,
                }
            }
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
    fn to_wire(self) -> (u64, u64, u64, bool) {
        match self {
            BookingTarget::Journal(journal) => (journal.tenant.0, journal.journal.0, 0, false),
            BookingTarget::Set { tenant, set } => (tenant.0, 0, set, false),
            BookingTarget::Tenant { tenant } => (tenant.0, 0, 0, true),
        }
    }

    fn from_wire(
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
    fn from_wire(node: u64, zone: String) -> Result<Self, &'static str> {
        match (node, zone.is_empty()) {
            (0, false) => Ok(Self::Zone(zone)),
            (node, true) if node != 0 => Ok(Self::Machine(NodeId(node))),
            _ => Err("a maintenance window covers one machine or one zone"),
        }
    }

    fn to_wire(&self) -> (u64, String) {
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
    /// The cell coordinator recorded machine `id`'s admission (#511).
    MachineAdmitted {
        /// The machine.
        id: NodeId,
    },
    /// Machine `id`'s user tags changed (#575).
    Tagged {
        /// The machine.
        id: NodeId,
    },
    /// A tag request applied already: nothing changed (#575).
    TagRepeated {
        /// The request.
        request: u64,
    },
    /// The cell coordinator marked machine `id` down (#211).
    MachineDown {
        /// The machine.
        id: NodeId,
    },
    /// The cell coordinator judged machine `id` slow, or fast again (#361
    /// (slow machines)).
    MachineSlow {
        /// The machine.
        id: NodeId,
        /// Whether it is slow now.
        slow: bool,
    },
    /// The cell coordinator judged machine `id` low on space, or not any
    /// more (#360 (disk full)).
    MachineSpace {
        /// The machine.
        id: NodeId,
        /// Whether it is low on space now.
        low: bool,
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
    /// A maintenance window was opened, or ended (`for_ms == 0`), over
    /// `scope` (#359).
    Maintenance {
        /// The machine or failure domain.
        scope: MaintenanceScope,
        /// The window's length; `0` when it ended.
        for_ms: u64,
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
    /// A grant booked every slot it names (#476).
    Granted {
        /// What it is recorded under.
        key: GrantKey,
        /// The slots booked.
        slots: Vec<Slot>,
    },
    /// A grant's writer found no room, and the refusal is recorded (#476).
    GrantRefused {
        /// What it is recorded under.
        key: GrantKey,
    },
    /// A checkpoint (#230): every position below `covers_up_to` is in the
    /// state this fold now holds — restored from it, or verified against it
    /// (the fold reports which through its audit, not here, so every fold
    /// of the same position folds the same event).
    Checkpoint {
        /// The checkpoint's horizon, the position of its run's `Begin`.
        covers_up_to: u64,
    },
    /// A checkpoint run's `Begin` or `Chunk` (#353): nothing changed until
    /// the run's `End`. Reported so every position of the registry folds to
    /// an event.
    CheckpointRun,
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
    /// A `HostTenant` of a tenant whose footprint the cell has not granted
    /// (#476): a cell hosts a tenant only once its footprint is booked.
    NoFootprint {
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

/// The **universe pointer** (#232 (multiple cells), §3.7): where a cell
/// that does not host the universe tenant reads the universe directory.
/// Recorded by its join; its readers dial `hosts`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniversePointer {
    /// The universe directory: the universe tenant's control journal.
    pub directory: JournalIdentifier,
    /// Machines of the cell that hosts it, by node id and address.
    pub hosts: Vec<(u64, crate::Address)>,
}

type PointerWire = (u64, u64, Vec<wire::UniverseHost>);

/// A `JoinFleet` on the wire, its universe pointer flattened.
fn join_to_wire(
    fleet_id: u64,
    cell_id: u64,
    version: u32,
    universe: Option<&UniversePointer>,
) -> wire::JoinFleet {
    let (directory_tenant, directory_journal, hosts) = pointer_to_wire(universe);
    wire::JoinFleet {
        fleet_id,
        cell_id,
        version,
        directory_tenant,
        directory_journal,
        hosts,
    }
}

fn pointer_to_wire(pointer: Option<&UniversePointer>) -> PointerWire {
    pointer.map_or((0, 0, Vec::new()), |p| {
        (
            p.directory.tenant.0,
            p.directory.journal.0,
            p.hosts
                .iter()
                .map(|(node_id, addr)| wire::UniverseHost {
                    node_id: *node_id,
                    addr: addr.to_string(),
                })
                .collect(),
        )
    })
}

fn pointer_from_wire(
    tenant: u64,
    journal: u64,
    hosts: Vec<wire::UniverseHost>,
) -> Result<Option<UniversePointer>, &'static str> {
    if tenant == 0 && journal == 0 && hosts.is_empty() {
        return Ok(None);
    }
    let directory = JournalIdentifier::new(TenantId(tenant), JournalId(journal));
    if !directory.is_set() || hosts.is_empty() {
        return Err("a universe pointer names no directory or no host");
    }
    let hosts = hosts
        .into_iter()
        .map(|h| {
            let addr = crate::Address::parse(&h.addr)
                .map_err(|_| "a universe host's address does not parse")?;
            if h.node_id == 0 {
                return Err("a universe host names no node");
            }
            Ok((h.node_id, addr))
        })
        .collect::<Result<_, _>>()?;
    Ok(Some(UniversePointer { directory, hosts }))
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
                locality,
                name,
                incarnation,
                machine_interface,
            }) => self.register(
                id,
                addr,
                class,
                (capacity, incarnation, machine_interface),
                (locality, name),
            ),
            Some(SystemCommand::SetMachineTags { request, id, tags }) => {
                self.set_tags(request, id, tags)
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
        (capacity, incarnation, machine_interface): (u64, u128, Vec<u8>),
        (locality, name): (Locality, String),
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
                RegistryEvent::Reregistered { id, addr, capacity }
            }
            None => {
                self.nodes.insert(
                    id,
                    RegisteredNode {
                        addr: addr.clone(),
                        locality,
                        name,
                        class,
                        capacity,
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
                RegistryEvent::Registered {
                    id,
                    addr,
                    class,
                    capacity,
                }
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
                Some(_) => RegistryEvent::Maintenance { scope, for_ms },
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
        if slots.is_empty() || slots.iter().any(|slot| slot.target.tenant() != tenant) {
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
                    tags: n.all_tags().to_wire(),
                    name: n.name.clone(),
                    capacity: n.capacity,
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
            // A genesis node registers too (#349): its entry is the address
            // peers dial, and it crosses a checkpoint like any other (#211).
            let id = NodeId(n.id);
            let (class, locality, tags) = Tags::from_wire(n.tags)
                .and_then(parse_machine_tags)
                .map_err(|_| "a registry state node's tags are malformed or unknown")?;
            nodes.insert(
                id,
                RegisteredNode {
                    addr: n.addr,
                    locality,
                    name: n.name,
                    class,
                    capacity: n.capacity,
                    standing: NodeStanding::from_wire(n.standing)?,
                    registrations: n.registrations,
                    incarnation: incarnation_from_halves(n.incarnation_high, n.incarnation_low),
                    machine_interface: n.machine_interface,
                    admitted: n.admitted,
                    tags,
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
                    target: BookingTarget::from_wire(b.tenant, b.journal, b.set, b.tenant_level)?,
                },
            );
        }
        let liveness = liveness_from_wire(state.liveness);
        let fleet = match (state.fleet_id, state.cell_id) {
            (0, 0) => None,
            (0, _) | (_, 0) => return Err("a registry state names half a fleet registration"),
            (fleet_id, cell_id) => Some(FleetRegistration {
                fleet_id,
                cell_id,
                version: state.version,
            }),
        };
        let universe =
            pointer_from_wire(state.directory_tenant, state.directory_journal, state.hosts)?;
        if universe.is_some() && fleet.is_none() {
            return Err("a registry state points at a universe it did not join");
        }
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
        let maintenance = maintenance_from_wire(state.maintenance, covers_up_to)?;
        let grants = grants_from_wire(state.grants, &spent)?;
        self.nodes = nodes;
        self.bookings = bookings;
        self.grants = grants;
        self.spent = spent;
        self.maintenance = maintenance;
        self.liveness = liveness;
        self.fleet = fleet;
        self.universe = universe;
        self.hosted = hosted;
        self.dropped = dropped;
        self.tagged = state.tagged.into_iter().collect();
        self.next_seq = covers_up_to + 1;
        Ok(())
    }
}

/// The liveness marks of a registry checkpoint: each machine's incarnation,
/// down or up, and slow or not (#361 (slow machines)).
fn liveness_from_wire(states: Vec<wire::LivenessState>) -> BTreeMap<NodeId, Liveness> {
    states
        .into_iter()
        .map(|l| {
            (
                NodeId(l.id),
                Liveness {
                    incarnation: incarnation_from_halves(l.incarnation_high, l.incarnation_low),
                    up: l.up,
                    slow: l.slow,
                    low_space: l.low_space,
                },
            )
        })
        .collect()
}

/// The open maintenance windows of a registry checkpoint (#359), each
/// opened at or below `covers_up_to`.
fn maintenance_from_wire(
    states: Vec<wire::MaintenanceState>,
    covers_up_to: u64,
) -> Result<BTreeMap<MaintenanceScope, MaintenanceWindow>, &'static str> {
    let mut maintenance = BTreeMap::new();
    for m in states {
        if m.for_ms == 0 || m.seq > covers_up_to {
            return Err("a registry state holds a maintenance window it cannot");
        }
        maintenance.insert(
            MaintenanceScope::from_wire(m.node, m.zone)?,
            MaintenanceWindow {
                for_ms: m.for_ms,
                seq: m.seq,
            },
        );
    }
    Ok(maintenance)
}

/// The grants of a registry checkpoint (#476): each under its key, its
/// booking ids spent.
fn grants_from_wire(
    states: Vec<wire::GrantState>,
    spent: &BTreeSet<u64>,
) -> Result<BTreeMap<GrantKey, Grant>, &'static str> {
    let mut grants = BTreeMap::new();
    for g in states {
        let key = match (g.footprint, g.request) {
            (true, 0) => GrantKey::Footprint(TenantId(g.tenant)),
            (false, request) if request != 0 => GrantKey::Request(request),
            _ => return Err("a registry state names a grant by no key"),
        };
        if g.refused && !g.bookings.is_empty() {
            return Err("a registry state holds a refused grant that booked");
        }
        if g.bookings.iter().any(|b| !spent.contains(b)) {
            return Err("a registry state holds a grant whose booking id is not spent");
        }
        grants.insert(
            key,
            Grant {
                tenant: TenantId(g.tenant),
                bookings: (!g.refused).then_some(g.bookings),
            },
        );
    }
    Ok(grants)
}

/// The event a [`Folded`] registry record is reported as: an entry's own
/// event, a checkpoint's, or a refusal for a record the fold cannot use.
/// `None` for a record the fold skipped (above a gap).
#[must_use]
pub fn registry_event(folded: Folded<RegistryEvent>) -> Option<RegistryEvent> {
    match folded {
        Folded::Entry(event) => Some(event),
        Folded::Checkpoint { covers_up_to, .. } => Some(RegistryEvent::Checkpoint { covers_up_to }),
        Folded::Unreadable(_) => Some(RegistryEvent::Refused(RegistryRefusal::Malformed)),
        Folded::Run => Some(RegistryEvent::CheckpointRun),
        Folded::Skipped => None,
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
                locality: crate::machine::Locality::zone("rack-a"),
                name: "storage-7".into(),
                incarnation: (7_u128 << 64) | 9,
                machine_interface: vec![1, 2, 3],
            },
            SystemCommand::DrainNode { id: NodeId(100) },
            SystemCommand::RetireNode { id: NodeId(100) },
            SystemCommand::MachineAdmitted { id: NodeId(100) },
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
            SystemCommand::MachineSlow {
                id: NodeId(100),
                incarnation: 1,
                slow: true,
                facts: SlowFacts {
                    rtt_us: 250_000,
                    disk_busy_permille: 990,
                    cpu_busy_permille: 12,
                },
            },
            SystemCommand::JoinFleet {
                fleet_id: 11,
                cell_id: 13,
                version: 4,
                universe: Some(UniversePointer {
                    directory: JournalIdentifier::new(TenantId(5), JournalId(6)),
                    hosts: vec![(7, crate::Address::parse("10.0.0.7:4500").unwrap())],
                }),
            },
            SystemCommand::ReleaseCapacity { booking: 7 },
            SystemCommand::JoinFleet {
                fleet_id: 11,
                cell_id: 12,
                version: 1,
                universe: None,
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
            SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(TenantId(300)),
                tenant: TenantId(300),
                slots: Some(vec![Slot {
                    booking: 9,
                    node: NodeId(100),
                    role: Role::Coordinator,
                    target: BookingTarget::Tenant {
                        tenant: TenantId(300),
                    },
                }]),
            },
            SystemCommand::GrantCapacity {
                key: GrantKey::Request(77),
                tenant: TenantId(300),
                slots: None,
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
            locality: crate::machine::Locality::default(),
            name: format!("m{id}"),
            incarnation: u128::from(id),
            machine_interface: Vec::new(),
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
        assert!(BookingTarget::from_wire(1, 2, 3, false).is_err());
        assert!(BookingTarget::from_wire(1, 2, 0, true).is_err());
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

    fn slow(id: u64, incarnation: u128, slow: bool) -> Vec<u8> {
        SystemCommand::MachineSlow {
            id: NodeId(id),
            incarnation,
            slow,
            facts: SlowFacts::default(),
        }
        .encode()
    }

    #[test]
    fn slow_marks_are_changes_only_and_move_roles() {
        let mut reg = Registry::new([NodeId(0)]);
        reg.fold(0, &register(100, Class::Storage, 1));
        assert!(reg.keeps_roles(NodeId(100)) && !reg.is_slow(NodeId(100)));
        // A registration starts fast.
        assert_eq!(
            reg.fold(1, &slow(100, 100, false)),
            RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(2, &slow(100, 5, true)),
            RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(3, &slow(100, 100, true)),
            RegistryEvent::MachineSlow {
                id: NodeId(100),
                slow: true
            }
        );
        // Slow is alive, and its roles move.
        assert!(reg.is_slow(NodeId(100)) && reg.seen_alive(NodeId(100)));
        assert!(!reg.keeps_roles(NodeId(100)));
        assert_eq!(
            reg.fold(4, &slow(100, 100, true)),
            RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
        );
        // Down keeps the judgement; a new incarnation starts fast.
        reg.fold(5, &liveness(100, 100, false));
        assert!(reg.is_slow(NodeId(100)));
        reg.fold(6, &liveness(100, 101, true));
        assert!(!reg.is_slow(NodeId(100)) && reg.keeps_roles(NodeId(100)));
        // A checkpoint keeps a slow mark.
        reg.fold(7, &slow(100, 101, true));
        let mut restored = Registry::new([NodeId(0)]);
        restored.restore(7, &reg.checkpoint()).expect("restores");
        assert!(restored.is_slow(NodeId(100)));
        assert_eq!(
            reg.fold(8, &slow(7, 1, true)),
            RegistryEvent::Refused(RegistryRefusal::NotInPool { id: NodeId(7) })
        );
    }

    #[test]
    fn a_machine_serves_once_its_admission_is_recorded() {
        let mut reg = Registry::new([NodeId(0)]);
        reg.fold(0, &register(100, Class::Storage, 2));
        // A genesis node serves from its start; a registration alone does
        // not (#511 (placement books machines that never served)).
        assert!(reg.serves(NodeId(0)));
        assert!(!reg.serves(NodeId(100)));
        let admitted = |id| one(&SystemCommand::MachineAdmitted { id: NodeId(id) });
        assert_eq!(
            reg.fold(1, &admitted(0)),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(0) })
        );
        assert_eq!(
            reg.fold(2, &admitted(7)),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(7) })
        );
        assert_eq!(
            reg.fold(3, &admitted(100)),
            RegistryEvent::MachineAdmitted { id: NodeId(100) }
        );
        assert!(reg.serves(NodeId(100)));
        assert_eq!(reg.fold(4, &admitted(100)), RegistryEvent::Unchanged);
        // A reboot keeps the admission; a checkpoint carries it.
        reg.fold(5, &register(100, Class::Storage, 3));
        assert!(reg.serves(NodeId(100)));
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(6, &reg.checkpoint())
            .expect("a checkpoint restores");
        assert!(restored.serves(NodeId(100)));
        // A retired machine serves no more.
        reg.fold(6, &one(&SystemCommand::DrainNode { id: NodeId(100) }));
        assert!(reg.serves(NodeId(100)));
        reg.fold(7, &one(&SystemCommand::RetireNode { id: NodeId(100) }));
        assert!(!reg.serves(NodeId(100)));
        assert_eq!(
            reg.fold(8, &admitted(100)),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(100) })
        );
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
    fn a_machine_carries_its_reserved_tags_and_its_user_tags_once_per_request() {
        let mut reg = Registry::new([NodeId(0)]);
        let placed = |zone: &str| {
            one(&SystemCommand::RegisterNode {
                id: NodeId(100),
                addr: "n100".into(),
                class: Class::Storage,
                capacity: 2,
                locality: Locality {
                    zone: zone.into(),
                    rack: "r1".into(),
                },
                name: "m100".into(),
                incarnation: 100,
                machine_interface: Vec::new(),
            })
        };
        reg.fold(0, &placed("zone-a"));
        let tags = |texts: &[&str]| UserTags::parse(texts).unwrap();
        let set = |request: u64, id: u64, texts: &[&str]| {
            one(&SystemCommand::SetMachineTags {
                request,
                id: NodeId(id),
                tags: tags(texts),
            })
        };
        assert_eq!(
            reg.fold(1, &set(1, 100, &["disk:nvme"])),
            RegistryEvent::Tagged { id: NodeId(100) }
        );
        assert_eq!(
            reg.fold(2, &set(2, 100, &["disk:ssd"])),
            RegistryEvent::Tagged { id: NodeId(100) }
        );
        // A retry of the first request never undoes the second.
        assert_eq!(
            reg.fold(3, &set(1, 100, &["disk:nvme"])),
            RegistryEvent::TagRepeated { request: 1 }
        );
        assert_eq!(
            reg.fold(4, &set(3, 999, &["disk:ssd"])),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(999) })
        );
        // A reboot in another zone moves its reserved tags and keeps its user
        // tags.
        reg.fold(5, &placed("zone-b"));
        let node = reg.get(NodeId(100)).unwrap();
        assert_eq!(
            node.all_tags().label(),
            "disk:ssd,paros/class:storage,paros/rack:r1,paros/zone:zone-b"
        );
        // A registration carrying a user tag, or no class, is malformed.
        let mut forged = wire::SystemEntry::decode(placed("z").as_slice()).unwrap();
        if let Some(wire::system_entry::Kind::RegisterNode(register)) = &mut forged.kind {
            register
                .tags
                .retain(|tag| tag.key != crate::tags::reserved::CLASS);
        }
        assert_eq!(
            reg.fold(6, &forged.encode_to_vec()),
            RegistryEvent::Refused(RegistryRefusal::Malformed)
        );
        // Both cross a checkpoint.
        let mut restored = Registry::new([NodeId(0)]);
        restored.restore(6, &reg.checkpoint()).unwrap();
        assert_eq!(restored.get(NodeId(100)), reg.get(NodeId(100)));
        assert!(restored.tagged(1) && restored.tagged(2) && !restored.tagged(3));
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
    fn a_registered_genesis_node_crosses_a_checkpoint() {
        use crate::client::checkpoint::{Folded, Folder, run};
        // A founding member registers its address (#349), then the owner
        // checkpoints: a fold that restores there keeps the address.
        let records = [register(7, Class::Storage, 1)];
        let mut whole = Folder::new(Registry::new([NodeId(7)]));
        assert!(matches!(whole.fold(0, &records[0]), Some(Folded::Entry(_))));
        assert!(whole.state().address(NodeId(7)).is_some());
        let run = run(1, &whole.state().checkpoint(), 64);
        let mut restored = Folder::new(Registry::new([NodeId(7)]));
        restored.jump(1);
        let end = run.len() as u64;
        for (seq, record) in (1..end).zip(&run) {
            assert_eq!(restored.fold(seq, record), Some(Folded::Run));
        }
        assert_eq!(
            restored.fold(end, run.last().expect("a run has an End")),
            Some(Folded::Checkpoint {
                covers_up_to: 1,
                verified: None
            })
        );
        assert!(restored.is_whole());
        assert_eq!(restored.state().checkpoint(), whole.state().checkpoint());
        assert_eq!(restored.state().address(NodeId(7)), Some("n7"));
        assert!(restored.state().contains(NodeId(7)));
    }

    #[test]
    fn a_registry_restored_from_its_checkpoint_is_the_registry_folded_whole() {
        use crate::client::checkpoint::{Folded, Folder, run};
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
        // A small chunk: the run spans many records.
        let run = run(at, &whole.state().checkpoint(), 8);
        assert!(run.len() > 3, "the registry's state takes several chunks");
        let end = at + run.len() as u64 - 1;
        let fold_run = |fold: &mut Folder<Registry>| {
            for (seq, record) in (at..end).zip(&run) {
                assert_eq!(fold.fold(seq, record), Some(Folded::Run));
            }
            fold.fold(end, run.last().unwrap())
        };
        // The fold that held every position verifies the checkpoint.
        assert_eq!(
            fold_run(&mut whole),
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
            fold_run(&mut restored),
            Some(Folded::Checkpoint {
                covers_up_to: at,
                verified: None
            })
        );
        assert!(restored.is_whole());
        assert_eq!(restored.state().checkpoint(), whole.state().checkpoint());
        let next = register(300, Class::Storage, 1);
        assert_eq!(whole.fold(end + 1, &next), restored.fold(end + 1, &next));
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
                universe: None,
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
        registry.fold(0, &register(0, Class::Storage, 1));
        let footprint = |tenant: u64, slots| {
            one(&SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(TenantId(tenant)),
                tenant: TenantId(tenant),
                slots,
            })
        };
        // A tenant whose footprint was refused is never hosted (#476).
        assert_eq!(
            registry.fold(1, &footprint(301, None)),
            RegistryEvent::GrantRefused {
                key: GrantKey::Footprint(TenantId(301))
            }
        );
        let slot = Slot {
            booking: 1,
            node: NodeId(0),
            role: Role::Acceptor,
            target: BookingTarget::Tenant {
                tenant: TenantId(300),
            },
        };
        assert!(matches!(
            registry.fold(2, &footprint(300, Some(vec![slot]))),
            RegistryEvent::Granted { .. }
        ));
        // Nothing is hosted before the cell joins a fleet.
        assert_eq!(
            registry.fold(3, &host(fleet, 300)),
            RegistryEvent::Refused(RegistryRefusal::OtherFleet { fleet_id: fleet })
        );
        assert_eq!(
            registry.fold(4, &join(fleet, cell, crate::fleet::METADATA_VERSION + 1)),
            RegistryEvent::Refused(RegistryRefusal::Unsupported)
        );
        assert_eq!(
            registry.fold(5, &join(fleet, cell, 1)),
            RegistryEvent::JoinedFleet {
                fleet_id: fleet,
                cell_id: cell
            }
        );
        assert_eq!(
            registry.fold(6, &join(fleet, cell, 1)),
            RegistryEvent::Unchanged
        );
        assert_eq!(
            registry.fold(7, &join(fleet, cell + 1, 1)),
            RegistryEvent::Refused(RegistryRefusal::OtherFleet { fleet_id: fleet })
        );
        assert_eq!(
            registry.fold(8, &host(fleet, 300)),
            RegistryEvent::TenantHosted {
                tenant: TenantId(300),
                control: JournalId(301),
            }
        );
        assert_eq!(
            registry.fold(9, &host(fleet, 300)),
            RegistryEvent::Unchanged
        );
        assert_eq!(
            registry.fold(10, &host(fleet + 1, 301)),
            RegistryEvent::Refused(RegistryRefusal::OtherFleet {
                fleet_id: fleet + 1
            })
        );
        assert_eq!(
            registry.fold(11, &host(fleet, 0)),
            RegistryEvent::Refused(RegistryRefusal::Malformed)
        );
        assert!(registry.hosts(TenantId(300)));
        assert!(!registry.hosts(TenantId(301)));
        // The checkpoint carries the registration and the tenant list.
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(11, &registry.checkpoint())
            .expect("restores");
        assert_eq!(restored, registry);
        assert_eq!(
            registry.fold(12, &drop(fleet, 300)),
            RegistryEvent::TenantDropped {
                tenant: TenantId(300)
            }
        );
        assert_eq!(
            registry.fold(13, &drop(fleet, 300)),
            RegistryEvent::Unchanged
        );
        assert_eq!(registry.hosted().count(), 0);
        // A dropped tenant is never hosted again: a stale host is refused.
        assert_eq!(
            registry.fold(14, &host(fleet, 300)),
            RegistryEvent::Refused(RegistryRefusal::TenantDropped {
                tenant: TenantId(300)
            })
        );
        // A drop of a tenant never hosted still tombstones it.
        assert_eq!(
            registry.fold(15, &drop(fleet, 301)),
            RegistryEvent::TenantDropped {
                tenant: TenantId(301)
            }
        );
        assert!(registry.dropped(TenantId(301)));
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(15, &registry.checkpoint())
            .expect("restores");
        assert_eq!(restored, registry);
    }
    #[test]
    fn a_grant_books_every_slot_or_none_and_a_host_needs_its_footprint() {
        let fleet = 0xf1ee7;
        let tenant = TenantId(300);
        let target = BookingTarget::Tenant { tenant };
        let mut registry = Registry::new([NodeId(0)]);
        registry.fold(0, &register(0, Class::Storage, 2));
        registry.fold(
            1,
            &one(&SystemCommand::JoinFleet {
                fleet_id: fleet,
                cell_id: 0xce11,
                version: 1,
                universe: None,
            }),
        );
        let host = one(&SystemCommand::HostTenant {
            fleet_id: fleet,
            tenant,
            hosted: hosted(301),
        });
        assert_eq!(
            registry.fold(2, &host),
            RegistryEvent::Refused(RegistryRefusal::NoFootprint { tenant })
        );
        let slot = |booking| Slot {
            booking,
            node: NodeId(0),
            role: Role::Acceptor,
            target,
        };
        let grant = |slots: Vec<Slot>| {
            one(&SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(tenant),
                tenant,
                slots: Some(slots),
            })
        };
        // Three slots on a machine of two: the third undoes the first two.
        assert_eq!(
            registry.fold(3, &grant(vec![slot(1), slot(2), slot(3)])),
            RegistryEvent::Refused(RegistryRefusal::NoCapacity { id: NodeId(0) })
        );
        assert_eq!(registry.booked(NodeId(0)), 0);
        assert!(
            registry.footprint(tenant).is_none(),
            "a slot refused records nothing"
        );
        // Two slots of one role stack on one machine: a footprint is slots.
        assert!(matches!(
            registry.fold(4, &grant(vec![slot(1), slot(2)])),
            RegistryEvent::Granted { .. }
        ));
        assert_eq!(registry.booked(NodeId(0)), 2);
        assert_eq!(
            registry.fold(5, &grant(vec![slot(4)])),
            RegistryEvent::Unchanged,
            "the first grant under a key wins"
        );
        // A slot for another tenant is malformed; so is a request id of 0.
        let foreign = one(&SystemCommand::GrantCapacity {
            key: GrantKey::Request(9),
            tenant,
            slots: Some(vec![Slot {
                target: BookingTarget::Tenant {
                    tenant: TenantId(7),
                },
                ..slot(5)
            }]),
        });
        assert_eq!(
            registry.fold(6, &foreign),
            RegistryEvent::Refused(RegistryRefusal::Malformed)
        );
        assert!(matches!(
            registry.fold(7, &host),
            RegistryEvent::TenantHosted { .. }
        ));
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(7, &registry.checkpoint())
            .expect("restores");
        assert_eq!(restored, registry);
    }
}
