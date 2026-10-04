//! The registry's fleet half (#229): the cell's own record of the fleet it
//! belongs to (`RegisterFleet`) and of the tenants it hosts (`HostTenant`,
//! `UnhostTenant`). Folded by the same [`Registry`] — one journal, one fold —
//! but a concern of its own beside the node pool and the bookings.

use paros_core::{AcceptorConfig, TenantId};

use super::{Registry, RegistryEvent, RegistryRefusal};
use crate::system::{FleetContext, METADATA_VERSION};

/// The cell's half of its fleet registration (#229).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetRegistration {
    /// The fleet and this cell.
    pub context: FleetContext,
    /// The metadata version it was written in.
    pub metadata_version: u32,
}

/// A tenant this cell hosts (#229).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedTenant {
    /// Its name.
    pub name: Vec<u8>,
    /// The static configuration of its control journal (`tenant/1`, #210).
    pub control: AcceptorConfig,
}

impl Registry {
    pub(super) fn register_fleet(&mut self, context: FleetContext, version: u32) -> RegistryEvent {
        if version == 0 || version > METADATA_VERSION {
            return RegistryEvent::Refused(RegistryRefusal::UnsupportedVersion { version });
        }
        if context.fleet_id == 0 || context.cell_id == 0 {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        match self.registration {
            Some(held) if held.context == context => {
                RegistryEvent::Refused(RegistryRefusal::AlreadyRegistered)
            }
            Some(_) => RegistryEvent::Refused(RegistryRefusal::OtherFleet),
            None => {
                self.registration = Some(FleetRegistration {
                    context,
                    metadata_version: version,
                });
                RegistryEvent::FleetRegistered { context }
            }
        }
    }

    /// A tenant step names the fleet and this cell, as registered.
    fn check_context(&self, context: FleetContext) -> Result<(), RegistryRefusal> {
        match self.registration {
            None => Err(RegistryRefusal::NoFleet),
            Some(held) if held.context != context => Err(RegistryRefusal::OtherFleet),
            Some(_) => Ok(()),
        }
    }

    pub(super) fn host(
        &mut self,
        context: FleetContext,
        tenant: TenantId,
        name: Vec<u8>,
        control: AcceptorConfig,
    ) -> RegistryEvent {
        if let Err(refusal) = self.check_context(context) {
            return RegistryEvent::Refused(refusal);
        }
        if !tenant.is_user() {
            return RegistryEvent::Refused(RegistryRefusal::ReservedTenant { tenant });
        }
        if self.tenants.contains_key(&tenant) {
            return RegistryEvent::Refused(RegistryRefusal::TenantHosted { tenant });
        }
        if self.unhosted.contains(&tenant) {
            return RegistryEvent::Refused(RegistryRefusal::TenantGone { tenant });
        }
        self.tenants.insert(
            tenant,
            HostedTenant {
                name: name.clone(),
                control: control.clone(),
            },
        );
        RegistryEvent::TenantHosted {
            tenant,
            name,
            control,
        }
    }

    /// The cell's half of its fleet registration, once written.
    #[must_use]
    pub fn registration(&self) -> Option<FleetRegistration> {
        self.registration
    }

    /// The tenant this cell hosts as `tenant`.
    #[must_use]
    pub fn tenant(&self, tenant: TenantId) -> Option<&HostedTenant> {
        self.tenants.get(&tenant)
    }

    /// Whether `tenant` was unhosted: never hosted here again.
    #[must_use]
    pub fn is_unhosted(&self, tenant: TenantId) -> bool {
        self.unhosted.contains(&tenant)
    }

    /// Every tenant this cell hosts, in id order.
    pub fn tenants(&self) -> impl Iterator<Item = (TenantId, &HostedTenant)> {
        self.tenants.iter().map(|(id, t)| (*id, t))
    }

    pub(super) fn unhost(&mut self, context: FleetContext, tenant: TenantId) -> RegistryEvent {
        if let Err(refusal) = self.check_context(context) {
            return RegistryEvent::Refused(refusal);
        }
        // A fence as much as a removal: an id never hosted is tombstoned
        // too, so a creator's `HostTenant` that lands after its tenant's
        // removal is refused (`TenantGone`).
        if !tenant.is_user() || !self.unhosted.insert(tenant) {
            return RegistryEvent::Refused(RegistryRefusal::UnknownTenant { tenant });
        }
        self.tenants.remove(&tenant);
        RegistryEvent::TenantUnhosted { tenant }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::checkpoint::Checkpointable;
    use crate::system::SystemCommand;
    use paros_core::NodeId;

    fn one(command: &SystemCommand) -> Vec<u8> {
        command.encode()
    }

    fn control() -> AcceptorConfig {
        AcceptorConfig::new(vec![NodeId(0)], paros_core::QuorumSystem::Majority)
    }

    #[test]
    fn a_cell_registers_once_and_hosts_tenants_only_for_its_own_fleet() {
        let here = FleetContext {
            fleet_id: 9,
            cell_id: 4,
        };
        let elsewhere = FleetContext {
            fleet_id: 10,
            cell_id: 4,
        };
        let host = |context, tenant: u64| {
            one(&SystemCommand::HostTenant {
                context,
                tenant: TenantId(tenant),
                name: format!("t{tenant}").into_bytes(),
                control: control(),
            })
        };
        let unhost = |context, tenant: u64| {
            one(&SystemCommand::UnhostTenant {
                context,
                tenant: TenantId(tenant),
            })
        };
        let register = |context| {
            one(&SystemCommand::RegisterFleet {
                context,
                metadata_version: METADATA_VERSION,
            })
        };
        let mut reg = Registry::new([NodeId(0)]);
        assert_eq!(
            reg.fold(0, &host(here, 300)),
            RegistryEvent::Refused(RegistryRefusal::NoFleet)
        );
        assert_eq!(
            reg.fold(1, &register(here)),
            RegistryEvent::FleetRegistered { context: here }
        );
        assert_eq!(
            reg.fold(2, &register(here)),
            RegistryEvent::Refused(RegistryRefusal::AlreadyRegistered)
        );
        assert_eq!(
            reg.fold(3, &register(elsewhere)),
            RegistryEvent::Refused(RegistryRefusal::OtherFleet)
        );
        assert_eq!(
            reg.fold(4, &host(elsewhere, 300)),
            RegistryEvent::Refused(RegistryRefusal::OtherFleet)
        );
        assert_eq!(
            reg.fold(5, &host(here, 2)),
            RegistryEvent::Refused(RegistryRefusal::ReservedTenant {
                tenant: TenantId(2)
            })
        );
        assert!(matches!(
            reg.fold(6, &host(here, 300)),
            RegistryEvent::TenantHosted { .. }
        ));
        assert_eq!(
            reg.fold(7, &host(here, 300)),
            RegistryEvent::Refused(RegistryRefusal::TenantHosted {
                tenant: TenantId(300)
            })
        );
        assert_eq!(
            reg.fold(8, &unhost(here, 300)),
            RegistryEvent::TenantUnhosted {
                tenant: TenantId(300)
            }
        );
        assert_eq!(
            reg.fold(9, &host(here, 300)),
            RegistryEvent::Refused(RegistryRefusal::TenantGone {
                tenant: TenantId(300)
            })
        );
        assert_eq!(
            reg.fold(10, &unhost(here, 300)),
            RegistryEvent::Refused(RegistryRefusal::UnknownTenant {
                tenant: TenantId(300)
            })
        );
    }

    #[test]
    fn an_unhosting_fences_the_id_and_the_hosting_survives_a_checkpoint() {
        let here = FleetContext {
            fleet_id: 9,
            cell_id: 4,
        };
        let host = |tenant: u64| {
            one(&SystemCommand::HostTenant {
                context: here,
                tenant: TenantId(tenant),
                name: format!("t{tenant}").into_bytes(),
                control: control(),
            })
        };
        let unhost = |tenant: u64| {
            one(&SystemCommand::UnhostTenant {
                context: here,
                tenant: TenantId(tenant),
            })
        };
        let mut reg = Registry::new([NodeId(0)]);
        reg.fold(
            10,
            &one(&SystemCommand::RegisterFleet {
                context: here,
                metadata_version: METADATA_VERSION,
            }),
        );
        // An id never hosted is fenced too: its late host is refused.
        assert_eq!(
            reg.fold(11, &unhost(302)),
            RegistryEvent::TenantUnhosted {
                tenant: TenantId(302)
            }
        );
        assert_eq!(
            reg.fold(12, &host(302)),
            RegistryEvent::Refused(RegistryRefusal::TenantGone {
                tenant: TenantId(302)
            })
        );
        // The registration and the hosted list survive a checkpoint.
        reg.fold(13, &host(301));
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(14, &reg.checkpoint())
            .expect("a checkpoint restores");
        assert_eq!(restored.registration(), reg.registration());
        assert_eq!(restored.checkpoint(), reg.checkpoint());
        assert_eq!(restored.fold(15, &host(300)), reg.fold(15, &host(300)));
    }
}
