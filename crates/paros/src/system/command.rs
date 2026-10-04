//! [`SystemCommand`]: one entry of a system journal, as a client writes it
//! and every fold reads it — one record per position, framed by
//! `proto/system.proto`'s `SystemEntry`. Each fold reads only its own
//! journal's kinds and refuses the others as malformed.

use paros_core::{AcceptorConfig, JournalId, JournalKey, NodeId, TenantId};
use prost::Message as _;

use super::Class;
use crate::rpc::system as wire;
use crate::rpc::{config_from_proto, config_to_proto};

/// The fleet and the cell a fleet step believes it talks to (#229): every
/// step names them, and the journal it writes refuses the step at apply
/// when they are not the ones recorded there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FleetContext {
    /// The fleet's id, minted at `init`.
    pub fleet_id: u64,
    /// The cell's id, minted at `init`.
    pub cell_id: u64,
}

/// One system-journal entry, as a client appends it and a fold reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemCommand {
    /// Create journal `id` named `name` over the static configuration
    /// `config`.
    CreateJournal {
        /// The id its creator drew (user range); refused at apply when
        /// reserved or taken.
        id: JournalId,
        /// Opaque bytes; paros never interprets them.
        name: Vec<u8>,
        /// The journal's static acceptor configuration.
        config: AcceptorConfig,
    },
    /// Delete journal `id` (a tombstone, never a reuse).
    DeleteJournal {
        /// The journal to delete.
        id: JournalId,
    },
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
    /// Book one `class` slot of `node` for `journal` (#211): written by the
    /// cell coordinator, the registry's single writer.
    BookCapacity {
        /// The id its writer drew; refused while a live booking holds it.
        booking: u64,
        /// The node.
        node: NodeId,
        /// The slot's class.
        class: Class,
        /// The journal the slot is for.
        journal: JournalKey,
    },
    /// Release a booking.
    ReleaseCapacity {
        /// The booking.
        booking: u64,
    },
    /// Meta (#229): register cell `context.cell_id` in fleet
    /// `context.fleet_id` (the first registration names the fleet).
    RegisterCell {
        /// The fleet and the cell.
        context: FleetContext,
        /// The metadata version the writer speaks.
        metadata_version: u32,
    },
    /// Meta: the cell is ready to receive tenants.
    CellReady {
        /// The fleet and the cell.
        context: FleetContext,
    },
    /// Meta: register tenant `tenant` named `name` in `context.cell_id`.
    RegisterTenant {
        /// The fleet and the cell it is assigned to.
        context: FleetContext,
        /// The id its creator drew; refused when reserved or ever used.
        tenant: TenantId,
        /// Opaque bytes, unique among the fleet's live tenants.
        name: Vec<u8>,
    },
    /// Meta: the tenant is created in its cell.
    TenantReady {
        /// The fleet and the cell it was created in.
        context: FleetContext,
        /// The tenant.
        tenant: TenantId,
    },
    /// Meta: the tenant is being removed.
    RemoveTenant {
        /// The fleet.
        fleet_id: u64,
        /// The tenant.
        tenant: TenantId,
    },
    /// Meta: a removed tenant is gone for good.
    ForgetTenant {
        /// The fleet.
        fleet_id: u64,
        /// The tenant.
        tenant: TenantId,
    },
    /// Cell (#229): this cell's half of the fleet registration.
    RegisterFleet {
        /// The fleet and this cell.
        context: FleetContext,
        /// The metadata version the writer speaks.
        metadata_version: u32,
    },
    /// Cell: this cell hosts tenant `tenant` named `name`, its control
    /// journal over `control` (#210).
    HostTenant {
        /// The fleet and this cell.
        context: FleetContext,
        /// The tenant.
        tenant: TenantId,
        /// Its name.
        name: Vec<u8>,
        /// The static configuration of its control journal (`tenant/1`).
        control: AcceptorConfig,
    },
    /// A tenant control journal's description of its tenant (#210).
    DescribeTenant {
        /// The tenant's name.
        name: Vec<u8>,
    },
    /// Cell: this cell no longer hosts tenant `tenant`.
    UnhostTenant {
        /// The fleet and this cell.
        context: FleetContext,
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
            SystemCommand::CreateJournal { id, name, config } => {
                Kind::CreateJournal(wire::CreateJournal {
                    name: name.clone(),
                    config: Some(config_to_proto(config)),
                    id: id.0,
                })
            }
            SystemCommand::DeleteJournal { id } => {
                Kind::DeleteJournal(wire::DeleteJournal { id: id.0 })
            }
            SystemCommand::RegisterNode {
                id,
                addr,
                class,
                capacity,
                failure_domain,
            } => Kind::RegisterNode(wire::RegisterNode {
                id: id.0,
                addr: addr.clone(),
                failure_domain: failure_domain.clone(),
                class: class.as_str().into(),
                capacity: *capacity,
            }),
            SystemCommand::DrainNode { id } => Kind::DrainNode(wire::DrainNode { id: id.0 }),
            SystemCommand::RetireNode { id } => Kind::RetireNode(wire::RetireNode { id: id.0 }),
            SystemCommand::BookCapacity {
                booking,
                node,
                class,
                journal,
            } => Kind::BookCapacity(wire::BookCapacity {
                booking: *booking,
                node: node.0,
                class: class.as_str().into(),
                tenant: journal.tenant.0,
                journal: journal.journal.0,
            }),
            SystemCommand::ReleaseCapacity { booking } => {
                Kind::ReleaseCapacity(wire::ReleaseCapacity { booking: *booking })
            }
            fleet => fleet.fleet_kind(),
        };
        wire::SystemEntry { kind: Some(kind) }.encode_to_vec()
    }

    /// The wire kind of a fleet entry (#229): meta's and the cell's halves.
    fn fleet_kind(&self) -> wire::system_entry::Kind {
        use wire::system_entry::Kind;
        match self {
            SystemCommand::RegisterCell {
                context,
                metadata_version,
            } => Kind::RegisterCell(wire::RegisterCell {
                fleet_id: context.fleet_id,
                cell_id: context.cell_id,
                metadata_version: *metadata_version,
            }),
            SystemCommand::CellReady { context } => Kind::CellReady(wire::CellReady {
                fleet_id: context.fleet_id,
                cell_id: context.cell_id,
            }),
            SystemCommand::RegisterTenant {
                context,
                tenant,
                name,
            } => Kind::RegisterTenant(wire::RegisterTenant {
                fleet_id: context.fleet_id,
                cell_id: context.cell_id,
                tenant: tenant.0,
                name: name.clone(),
            }),
            SystemCommand::TenantReady { context, tenant } => {
                Kind::TenantReady(wire::TenantReady {
                    fleet_id: context.fleet_id,
                    cell_id: context.cell_id,
                    tenant: tenant.0,
                })
            }
            SystemCommand::RemoveTenant { fleet_id, tenant } => {
                Kind::RemoveTenant(wire::RemoveTenant {
                    fleet_id: *fleet_id,
                    tenant: tenant.0,
                })
            }
            SystemCommand::ForgetTenant { fleet_id, tenant } => {
                Kind::ForgetTenant(wire::ForgetTenant {
                    fleet_id: *fleet_id,
                    tenant: tenant.0,
                })
            }
            SystemCommand::RegisterFleet {
                context,
                metadata_version,
            } => Kind::RegisterFleet(wire::RegisterFleet {
                fleet_id: context.fleet_id,
                cell_id: context.cell_id,
                metadata_version: *metadata_version,
            }),
            SystemCommand::HostTenant {
                context,
                tenant,
                name,
                control,
            } => Kind::HostTenant(wire::HostTenant {
                fleet_id: context.fleet_id,
                cell_id: context.cell_id,
                tenant: tenant.0,
                name: name.clone(),
                control: Some(config_to_proto(control)),
            }),
            SystemCommand::DescribeTenant { name } => {
                Kind::DescribeTenant(wire::DescribeTenant { name: name.clone() })
            }
            SystemCommand::UnhostTenant { context, tenant } => {
                Kind::UnhostTenant(wire::UnhostTenant {
                    fleet_id: context.fleet_id,
                    cell_id: context.cell_id,
                    tenant: tenant.0,
                })
            }
            SystemCommand::CreateJournal { .. }
            | SystemCommand::DeleteJournal { .. }
            | SystemCommand::RegisterNode { .. }
            | SystemCommand::DrainNode { .. }
            | SystemCommand::RetireNode { .. }
            | SystemCommand::BookCapacity { .. }
            | SystemCommand::ReleaseCapacity { .. } => {
                unreachable!("`encode` encodes every non-fleet entry itself")
            }
        }
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
        let context = |fleet_id, cell_id| FleetContext { fleet_id, cell_id };
        Ok(match entry.kind.ok_or("a system entry names no kind")? {
            Kind::CreateJournal(create) => SystemCommand::CreateJournal {
                id: JournalId(create.id),
                name: create.name,
                config: config_from_proto(create.config)?
                    .ok_or("a created journal names no configuration")?,
            },
            Kind::DeleteJournal(delete) => SystemCommand::DeleteJournal {
                id: JournalId(delete.id),
            },
            Kind::RegisterNode(register) => SystemCommand::RegisterNode {
                id: NodeId(register.id),
                addr: register.addr,
                class: register.class.parse()?,
                capacity: register.capacity,
                failure_domain: register.failure_domain,
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
                class: book.class.parse()?,
                journal: JournalKey::new(TenantId(book.tenant), JournalId(book.journal)),
            },
            Kind::ReleaseCapacity(release) => SystemCommand::ReleaseCapacity {
                booking: release.booking,
            },
            Kind::RegisterCell(cell) => SystemCommand::RegisterCell {
                context: context(cell.fleet_id, cell.cell_id),
                metadata_version: cell.metadata_version,
            },
            Kind::CellReady(cell) => SystemCommand::CellReady {
                context: context(cell.fleet_id, cell.cell_id),
            },
            Kind::RegisterTenant(t) => SystemCommand::RegisterTenant {
                context: context(t.fleet_id, t.cell_id),
                tenant: TenantId(t.tenant),
                name: t.name,
            },
            Kind::TenantReady(t) => SystemCommand::TenantReady {
                context: context(t.fleet_id, t.cell_id),
                tenant: TenantId(t.tenant),
            },
            Kind::RemoveTenant(t) => SystemCommand::RemoveTenant {
                fleet_id: t.fleet_id,
                tenant: TenantId(t.tenant),
            },
            Kind::ForgetTenant(t) => SystemCommand::ForgetTenant {
                fleet_id: t.fleet_id,
                tenant: TenantId(t.tenant),
            },
            Kind::RegisterFleet(f) => SystemCommand::RegisterFleet {
                context: context(f.fleet_id, f.cell_id),
                metadata_version: f.metadata_version,
            },
            Kind::HostTenant(t) => SystemCommand::HostTenant {
                context: context(t.fleet_id, t.cell_id),
                tenant: TenantId(t.tenant),
                name: t.name,
                control: config_from_proto(t.control)?
                    .ok_or("a hosted tenant names no control configuration")?,
            },
            Kind::DescribeTenant(d) => SystemCommand::DescribeTenant { name: d.name },
            Kind::UnhostTenant(t) => SystemCommand::UnhostTenant {
                context: context(t.fleet_id, t.cell_id),
                tenant: TenantId(t.tenant),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::QuorumSystem;

    #[test]
    fn every_command_round_trips() {
        let context = FleetContext {
            fleet_id: 11,
            cell_id: 22,
        };
        let commands = [
            SystemCommand::CreateJournal {
                id: JournalId(0x9e37_79b9),
                name: b"orders".to_vec(),
                config: AcceptorConfig::new(
                    vec![NodeId(0), NodeId(1), NodeId(2)],
                    QuorumSystem::Majority,
                ),
            },
            SystemCommand::DeleteJournal { id: JournalId(131) },
            SystemCommand::RegisterNode {
                id: NodeId(100),
                addr: "10.0.5.1:4500".into(),
                class: Class::Storage,
                capacity: 3,
                failure_domain: "rack-a".into(),
            },
            SystemCommand::DrainNode { id: NodeId(100) },
            SystemCommand::RetireNode { id: NodeId(100) },
            SystemCommand::BookCapacity {
                booking: 7,
                node: NodeId(100),
                class: Class::Stateless,
                journal: JournalKey::new(TenantId(300), JournalId(400)),
            },
            SystemCommand::ReleaseCapacity { booking: 7 },
            SystemCommand::RegisterCell {
                context,
                metadata_version: 1,
            },
            SystemCommand::CellReady { context },
            SystemCommand::RegisterTenant {
                context,
                tenant: TenantId(0xabc),
                name: b"acme".to_vec(),
            },
            SystemCommand::TenantReady {
                context,
                tenant: TenantId(0xabc),
            },
            SystemCommand::RemoveTenant {
                fleet_id: 11,
                tenant: TenantId(0xabc),
            },
            SystemCommand::ForgetTenant {
                fleet_id: 11,
                tenant: TenantId(0xabc),
            },
            SystemCommand::RegisterFleet {
                context,
                metadata_version: 1,
            },
            SystemCommand::HostTenant {
                context,
                tenant: TenantId(0xabc),
                name: b"acme".to_vec(),
                control: AcceptorConfig::new(vec![NodeId(0), NodeId(1)], QuorumSystem::Majority),
            },
            SystemCommand::DescribeTenant {
                name: b"acme".to_vec(),
            },
            SystemCommand::UnhostTenant {
                context,
                tenant: TenantId(0xabc),
            },
        ];
        for command in commands {
            assert_eq!(SystemCommand::decode(&command.encode()), Ok(command));
        }
        assert!(SystemCommand::decode(b"\xff\xff").is_err());
    }
}
