//! One registry entry ([`SystemCommand`]) and the grants it books (#476): their types and their wire form.

use paros_core::{JournalId, NodeId, TenantId};
use paros_wire::system as wire;
use prost::Message as _;

use crate::machine::Class;
use crate::machine::{
    Locality, incarnation_from_halves, incarnation_halves, machine_tags, parse_machine_tags,
};
use crate::registry::records::{BookingTarget, MaintenanceScope, Role, SlowFacts};
use crate::registry::universe::{HostedTenant, UniversePointer, join_to_wire, pointer_from_wire};
use crate::tags::{Tag, Tags, UserTags};

// The cell control journal has no fixed identifier (§3.8): a deployment draws
// it and hands it to whoever folds it (`paros_roles::node::ControlPlan`).

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
    pub(crate) fn to_wire(self) -> wire::BookCapacity {
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

    pub(crate) fn from_wire(book: &wire::BookCapacity) -> Result<Self, &'static str> {
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
pub(crate) fn grant_to_wire(
    key: GrantKey,
    tenant: TenantId,
    slots: Option<&[Slot]>,
) -> wire::GrantCapacity {
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
