use super::*;

const FLEET: u64 = 0xf1ee7;
const CELL: u64 = 0xce11;

fn identifier(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

/// The fleet tenant's own control journal and the cell tenant's: drawn ids, nothing fixed.
fn fleet_control() -> JournalIdentifier {
    identifier(0x3e7a, 0x51)
}

fn cell_control() -> JournalIdentifier {
    identifier(0xc0de, 0x52)
}

fn entry(command: FleetCommand) -> Vec<u8> {
    FleetEntry::new(FLEET, command).encode()
}

/// A fleet directory folded through `FormFleet`, `AddCell` and the cell marked `READY`.
fn formed() -> FleetDirectory {
    let mut directory = FleetDirectory::default();
    assert_eq!(
        directory.fold(
            0,
            &entry(FleetCommand::FormFleet {
                control: fleet_control(),
                name: Vec::new()
            })
        ),
        FleetEvent::FleetFormed { fleet_id: FLEET }
    );
    assert_eq!(
        directory.fold(
            1,
            &entry(FleetCommand::AddCell {
                cell_id: CELL,
                control: cell_control(),
                name: Vec::new()
            })
        ),
        FleetEvent::CellAdded { cell_id: CELL }
    );
    assert_eq!(
        directory.fold(
            2,
            &entry(FleetCommand::MarkCell {
                cell_id: CELL,
                state: CellState::Ready
            })
        ),
        FleetEvent::CellMarked {
            cell_id: CELL,
            state: CellState::Ready
        }
    );
    directory
}

fn register(tenant: u64, name: &[u8]) -> Vec<u8> {
    entry(FleetCommand::RegisterTenant {
        control: identifier(tenant, tenant + 1),
        name: name.to_vec(),
        cell_id: CELL,
        survives: Survives::Az,
    })
}

fn mark(tenant: u64, state: TenantState) -> Vec<u8> {
    entry(FleetCommand::MarkTenant {
        tenant: TenantId(tenant),
        state,
    })
}

fn remove(tenant: u64) -> Vec<u8> {
    entry(FleetCommand::RemoveTenant {
        tenant: TenantId(tenant),
    })
}

#[test]
fn every_entry_round_trips() {
    let commands = [
        FleetCommand::FormFleet {
            control: fleet_control(),
            name: Vec::new(),
        },
        FleetCommand::AddCell {
            cell_id: CELL,
            control: cell_control(),
            name: Vec::new(),
        },
        FleetCommand::MarkCell {
            cell_id: CELL,
            state: CellState::Restoring,
        },
        FleetCommand::RegisterTenant {
            control: identifier(0x9e37_79b9, 7),
            name: b"acme".to_vec(),
            cell_id: CELL,
            survives: Survives::Region,
        },
        FleetCommand::MarkTenant {
            tenant: TenantId(300),
            state: TenantState::UpdatingConfiguration,
        },
        FleetCommand::RemoveTenant {
            tenant: TenantId(300),
        },
    ];
    for command in commands {
        let entry = FleetEntry::new(FLEET, command);
        assert_eq!(FleetEntry::decode(&entry.encode()), Ok(entry));
    }
    assert!(FleetEntry::decode(b"\xff\xff").is_err());
}

#[test]
fn the_fleet_is_formed_once_with_the_fleet_tenant_as_its_first_internal_tenant() {
    let mut directory = FleetDirectory::default();
    let add = entry(FleetCommand::AddCell {
        cell_id: CELL,
        control: cell_control(),
        name: Vec::new(),
    });
    // Nothing before the fleet; the fleet tenant's control journal must be named.
    assert_eq!(
        directory.fold(0, &add),
        FleetEvent::Refused(FleetDirectoryRefusal::NoFleet)
    );
    assert_eq!(
        directory.fold(
            1,
            &entry(FleetCommand::FormFleet {
                control: JournalIdentifier::UNSET,
                name: Vec::new()
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::Malformed)
    );
    assert_eq!(
        directory.fold(
            2,
            &entry(FleetCommand::FormFleet {
                control: fleet_control(),
                name: Vec::new()
            })
        ),
        FleetEvent::FleetFormed { fleet_id: FLEET }
    );
    assert_eq!(directory.control(), Some(fleet_control()));
    let own = directory
        .tenant(fleet_control().tenant)
        .expect("the fleet tenant is a tenant");
    assert_eq!(
        (own.groups, own.control),
        (Groups::FLEET_TENANT, fleet_control().journal)
    );
    // A re-run of the same step changes nothing; another control journal for the fleet tenant, or
    // another fleet, is refused — for every later step too.
    assert_eq!(
        directory.fold(
            3,
            &entry(FleetCommand::FormFleet {
                control: fleet_control(),
                name: Vec::new()
            })
        ),
        FleetEvent::Unchanged
    );
    assert_eq!(
        directory.fold(
            4,
            &entry(FleetCommand::FormFleet {
                control: identifier(1, 1),
                name: Vec::new()
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::OtherFleet { fleet_id: FLEET })
    );
    let other = FleetEntry::new(
        FLEET + 1,
        FleetCommand::AddCell {
            cell_id: CELL,
            control: cell_control(),
            name: Vec::new(),
        },
    )
    .encode();
    assert_eq!(
        directory.fold(5, &other),
        FleetEvent::Refused(FleetDirectoryRefusal::OtherFleet {
            fleet_id: FLEET + 1
        })
    );
    assert!(directory.cell(CELL).is_none());
    // A version this fold does not speak, and a malformed record.
    let future = FleetEntry {
        fleet_id: FLEET,
        version: METADATA_VERSION + 1,
        command: FleetCommand::MarkCell {
            cell_id: CELL,
            state: CellState::Ready,
        },
    }
    .encode();
    assert_eq!(
        directory.fold(6, &future),
        FleetEvent::Refused(FleetDirectoryRefusal::UnknownVersion {
            version: METADATA_VERSION + 1
        })
    );
    assert_eq!(
        directory.fold(7, b"junk"),
        FleetEvent::Refused(FleetDirectoryRefusal::Malformed)
    );
    assert_eq!(directory.fleet(), Some(FLEET));
}

#[test]
fn a_cell_brings_its_cell_tenant_and_only_a_ready_cell_takes_tenants() {
    let mut directory = FleetDirectory::default();
    directory.fold(
        0,
        &entry(FleetCommand::FormFleet {
            control: fleet_control(),
            name: Vec::new(),
        }),
    );
    let add = entry(FleetCommand::AddCell {
        cell_id: CELL,
        control: cell_control(),
        name: Vec::new(),
    });
    assert_eq!(
        directory.fold(1, &add),
        FleetEvent::CellAdded { cell_id: CELL }
    );
    let cell_tenant = directory
        .tenant(cell_control().tenant)
        .expect("the cell tenant");
    assert_eq!(
        (cell_tenant.groups, cell_tenant.cell_id),
        (Groups::CELL_TENANT, CELL)
    );
    // The fleet's first cell hosts the fleet tenant.
    assert_eq!(
        directory.tenant(fleet_control().tenant).map(|t| t.cell_id),
        Some(CELL)
    );
    assert_eq!(directory.fold(2, &add), FleetEvent::Unchanged);
    assert_eq!(
        directory.fold(
            3,
            &entry(FleetCommand::AddCell {
                cell_id: CELL,
                control: identifier(0xbad, 1),
                name: Vec::new()
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::OtherCellTenant { cell_id: CELL })
    );
    // A second cell cannot take a tenant id the fleet directory holds.
    assert_eq!(
        directory.fold(
            4,
            &entry(FleetCommand::AddCell {
                cell_id: CELL + 1,
                control: fleet_control(),
                name: Vec::new()
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::TenantIdTaken {
            tenant: fleet_control().tenant
        })
    );
    assert_eq!(
        directory.fold(5, &register(300, b"acme")),
        FleetEvent::Refused(FleetDirectoryRefusal::CellNotReady { cell_id: CELL })
    );
    assert_eq!(
        directory.fold(
            6,
            &entry(FleetCommand::MarkCell {
                cell_id: CELL,
                state: CellState::Restoring
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::CellTransition {
            from: CellState::Registering,
            to: CellState::Restoring
        })
    );
    assert_eq!(
        directory.fold(
            7,
            &entry(FleetCommand::MarkCell {
                cell_id: CELL + 1,
                state: CellState::Ready
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::UnknownCell { cell_id: CELL + 1 })
    );
}

#[test]
fn a_users_tenant_id_and_name_are_checked_at_registration() {
    let mut directory = formed();
    // Any set id will do, small ones included: nothing is reserved. An id
    // the cell tenant holds is taken, and an unset one malformed.
    assert_eq!(
        directory.fold(3, &register(0, b"acme")),
        FleetEvent::Refused(FleetDirectoryRefusal::Malformed)
    );
    assert_eq!(
        directory.fold(
            4,
            &entry(FleetCommand::RegisterTenant {
                control: cell_control(),
                name: b"acme".to_vec(),
                cell_id: CELL,
                survives: Survives::Az,
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::TenantIdTaken {
            tenant: cell_control().tenant
        })
    );
    assert_eq!(
        directory.fold(5, &register(3, b"acme")),
        FleetEvent::TenantRegistered {
            tenant: TenantId(3),
            cell_id: CELL
        }
    );
    let acme = directory.tenant(TenantId(3)).expect("registered");
    assert_eq!((acme.groups, acme.control), (Groups::SERVED, JournalId(4)));
    // The same registration again is a re-run; the id under another name is
    // taken; the name under another id is taken.
    assert_eq!(
        directory.fold(6, &register(3, b"acme")),
        FleetEvent::Unchanged
    );
    assert_eq!(
        directory.fold(7, &register(3, b"other")),
        FleetEvent::Refused(FleetDirectoryRefusal::TenantIdTaken {
            tenant: TenantId(3)
        })
    );
    assert_eq!(
        directory.fold(8, &register(301, b"acme")),
        FleetEvent::Refused(FleetDirectoryRefusal::NameTaken {
            holder: TenantId(3)
        })
    );
    // The internal tenants are never marked or removed through the tenant
    // API's entries.
    for (seq, tenant) in [(9, fleet_control().tenant), (10, cell_control().tenant)] {
        assert_eq!(
            directory.fold(seq, &mark(tenant.0, TenantState::Removing)),
            FleetEvent::Refused(FleetDirectoryRefusal::Internal { tenant })
        );
    }
    assert_eq!(
        directory.fold(11, &remove(cell_control().tenant.0)),
        FleetEvent::Refused(FleetDirectoryRefusal::Internal {
            tenant: cell_control().tenant
        })
    );
}

#[test]
fn a_removed_tenant_frees_its_name_and_never_its_id() {
    let mut directory = formed();
    directory.fold(4, &register(300, b"acme"));
    // REGISTERING -> READY; a removal needs REMOVING first.
    assert_eq!(
        directory.fold(8, &mark(300, TenantState::Ready)),
        FleetEvent::TenantMarked {
            tenant: TenantId(300),
            state: TenantState::Ready
        }
    );
    assert_eq!(
        directory.fold(9, &remove(300)),
        FleetEvent::Refused(FleetDirectoryRefusal::TenantTransition {
            from: TenantState::Ready,
            to: TenantState::Removing
        })
    );
    assert_eq!(
        directory.fold(10, &mark(300, TenantState::UpdatingConfiguration)),
        FleetEvent::TenantMarked {
            tenant: TenantId(300),
            state: TenantState::UpdatingConfiguration
        }
    );
    assert_eq!(
        directory.tenant(TenantId(300)).map(|t| t.config_seq),
        Some(1)
    );
    assert_eq!(
        directory.fold(11, &mark(300, TenantState::Removing)),
        FleetEvent::TenantMarked {
            tenant: TenantId(300),
            state: TenantState::Removing
        }
    );
    // A tenant being removed takes no other state.
    assert_eq!(
        directory.fold(12, &mark(300, TenantState::Ready)),
        FleetEvent::Refused(FleetDirectoryRefusal::TenantTransition {
            from: TenantState::Removing,
            to: TenantState::Ready
        })
    );
    assert_eq!(
        directory.fold(13, &remove(300)),
        FleetEvent::TenantRemoved {
            tenant: TenantId(300)
        }
    );
    assert_eq!(directory.fold(14, &remove(300)), FleetEvent::Unchanged);
    assert!(directory.is_removed(TenantId(300)));
    assert!(directory.named(b"acme").is_none());
    // The id is never reused; the name frees up for a new id.
    assert_eq!(
        directory.fold(15, &register(300, b"acme")),
        FleetEvent::Refused(FleetDirectoryRefusal::TenantIdTaken {
            tenant: TenantId(300)
        })
    );
    assert_eq!(
        directory.fold(16, &register(302, b"acme")),
        FleetEvent::TenantRegistered {
            tenant: TenantId(302),
            cell_id: CELL
        }
    );
    assert_eq!(
        directory.fold(17, &mark(302, TenantState::Registering)),
        FleetEvent::Unchanged
    );
    assert_eq!(
        directory.fold(18, &mark(999, TenantState::Ready)),
        FleetEvent::Refused(FleetDirectoryRefusal::UnknownTenant {
            tenant: TenantId(999)
        })
    );
}

#[test]
fn the_transition_tables() {
    use TenantState as T;
    let states = [
        T::Registering,
        T::Ready,
        T::Removing,
        T::UpdatingConfiguration,
        T::Error,
    ];
    for from in states {
        assert!(from.may_become(T::Removing), "any tenant may be removed");
        assert!(!from.may_become(T::Registering));
        assert_eq!(
            from.may_become(T::UpdatingConfiguration),
            from == T::Ready,
            "only a READY tenant is reconfigured"
        );
    }
    assert!(!T::Removing.may_become(T::Ready));
    assert!(!T::Removing.may_become(T::Error));
    assert!(CellState::Restoring.may_become(CellState::Ready));
    assert!(!CellState::Removing.may_become(CellState::Ready));
}

#[test]
fn a_checkpoint_restores_the_whole_directory() {
    let mut directory = formed();
    directory.fold(3, &register(300, b"acme"));
    directory.fold(4, &register(301, b"globex"));
    directory.fold(5, &mark(300, TenantState::Ready));
    directory.fold(6, &mark(301, TenantState::Removing));
    directory.fold(7, &remove(301));
    let state = directory.checkpoint();
    let mut restored = FleetDirectory::default();
    restored
        .restore(directory.next_seq() - 1, &state)
        .expect("restores");
    assert_eq!(restored, directory);
    // Equal states encode to equal bytes.
    assert_eq!(restored.checkpoint(), state);
    // The restored fold goes on like the original.
    assert_eq!(
        restored.fold(8, &register(301, b"globex")),
        FleetEvent::Refused(FleetDirectoryRefusal::TenantIdTaken {
            tenant: TenantId(301)
        })
    );
    assert!(FleetDirectory::default().restore(0, b"\xff\xff").is_err());
}

#[test]
fn the_groups_decide_who_moves() {
    // The three sets of §3.7, and the one rule that forbids a move.
    assert!(Groups::FLEET_TENANT.contains(Group::Internal));
    assert!(Groups::FLEET_TENANT.contains(Group::Fleet));
    assert!(!Groups::FLEET_TENANT.contains(Group::Cell));
    assert!(Groups::CELL_TENANT.contains(Group::Internal));
    assert!(Groups::CELL_TENANT.contains(Group::Cell));
    assert!(!Groups::SERVED.contains(Group::Internal));
    assert!(Groups::SERVED.contains(Group::Users));
    assert!(
        Groups::FLEET_TENANT.may_move(),
        "the fleet tenant moves with its coordinator"
    );
    assert!(Groups::SERVED.may_move(), "a served tenant moves");
    assert!(!Groups::CELL_TENANT.may_move(), "a cell tenant is its cell");
    assert_eq!(Groups::FLEET_TENANT.label(), "internal,fleet");
    assert_eq!(Groups::CELL_TENANT.label(), "internal,cell");
    assert_eq!(Groups::SERVED.label(), "users");
    // The wire carries only the three sets.
    for groups in [Groups::FLEET_TENANT, Groups::CELL_TENANT, Groups::SERVED] {
        assert_eq!(Groups::from_wire(groups.to_wire()), Some(groups));
    }
    for bits in [0, 1, 2, 4, 6, 9, 15, 16] {
        assert_eq!(Groups::from_wire(bits), None, "bits {bits} name no set");
    }
}
