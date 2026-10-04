use super::*;

const FLEET: u64 = 0xf1ee7;
const CELL: u64 = 0xce11;

fn entry(command: MetaCommand) -> Vec<u8> {
    MetaEntry::new(FLEET, command).encode()
}

/// A meta folded through `FormFleet`, `AddCell` and the cell marked `READY`.
fn formed() -> Meta {
    let mut meta = Meta::default();
    assert_eq!(
        meta.fold(0, &entry(MetaCommand::FormFleet)),
        MetaEvent::FleetFormed { fleet_id: FLEET }
    );
    assert_eq!(
        meta.fold(1, &entry(MetaCommand::AddCell { cell_id: CELL })),
        MetaEvent::CellAdded { cell_id: CELL }
    );
    assert_eq!(
        meta.fold(
            2,
            &entry(MetaCommand::MarkCell {
                cell_id: CELL,
                state: CellState::Ready
            })
        ),
        MetaEvent::CellMarked {
            cell_id: CELL,
            state: CellState::Ready
        }
    );
    meta
}

fn register(tenant: u64, name: &[u8]) -> Vec<u8> {
    entry(MetaCommand::RegisterTenant {
        tenant: TenantId(tenant),
        name: name.to_vec(),
        cell_id: CELL,
    })
}

fn mark(tenant: u64, state: TenantState) -> Vec<u8> {
    entry(MetaCommand::MarkTenant {
        tenant: TenantId(tenant),
        state,
    })
}

#[test]
fn every_entry_round_trips() {
    let commands = [
        MetaCommand::FormFleet,
        MetaCommand::AddCell { cell_id: CELL },
        MetaCommand::MarkCell {
            cell_id: CELL,
            state: CellState::Restoring,
        },
        MetaCommand::RegisterTenant {
            tenant: TenantId(0x9e37_79b9),
            name: b"acme".to_vec(),
            cell_id: CELL,
        },
        MetaCommand::MarkTenant {
            tenant: TenantId(300),
            state: TenantState::UpdatingConfiguration,
        },
        MetaCommand::RemoveTenant {
            tenant: TenantId(300),
        },
    ];
    for command in commands {
        let entry = MetaEntry::new(FLEET, command);
        assert_eq!(MetaEntry::decode(&entry.encode()), Ok(entry));
    }
    assert!(MetaEntry::decode(b"\xff\xff").is_err());
}

#[test]
fn the_fleet_is_formed_once_and_every_entry_names_it() {
    let mut meta = Meta::default();
    // Nothing before the fleet.
    assert_eq!(
        meta.fold(0, &entry(MetaCommand::AddCell { cell_id: CELL })),
        MetaEvent::Refused(MetaRefusal::NoFleet)
    );
    assert_eq!(
        meta.fold(1, &entry(MetaCommand::FormFleet)),
        MetaEvent::FleetFormed { fleet_id: FLEET }
    );
    // A re-run of the same step changes nothing; another fleet is refused,
    // for every later step too.
    assert_eq!(
        meta.fold(2, &entry(MetaCommand::FormFleet)),
        MetaEvent::Unchanged
    );
    let other = MetaEntry::new(FLEET + 1, MetaCommand::FormFleet).encode();
    assert_eq!(
        meta.fold(3, &other),
        MetaEvent::Refused(MetaRefusal::OtherFleet {
            fleet_id: FLEET + 1
        })
    );
    let other = MetaEntry::new(FLEET + 1, MetaCommand::AddCell { cell_id: CELL }).encode();
    assert_eq!(
        meta.fold(4, &other),
        MetaEvent::Refused(MetaRefusal::OtherFleet {
            fleet_id: FLEET + 1
        })
    );
    assert!(meta.cell(CELL).is_none());
    // A version this fold does not speak, and a malformed record.
    let future = MetaEntry {
        fleet_id: FLEET,
        version: METADATA_VERSION + 1,
        command: MetaCommand::AddCell { cell_id: CELL },
    }
    .encode();
    assert_eq!(
        meta.fold(5, &future),
        MetaEvent::Refused(MetaRefusal::UnknownVersion {
            version: METADATA_VERSION + 1
        })
    );
    assert_eq!(
        meta.fold(6, b"junk"),
        MetaEvent::Refused(MetaRefusal::Malformed)
    );
    assert_eq!(meta.fleet(), Some(FLEET));
}

#[test]
fn only_a_ready_cell_receives_tenants_and_cells_move_by_the_table() {
    let mut meta = Meta::default();
    meta.fold(0, &entry(MetaCommand::FormFleet));
    meta.fold(1, &entry(MetaCommand::AddCell { cell_id: CELL }));
    assert_eq!(
        meta.fold(2, &register(300, b"acme")),
        MetaEvent::Refused(MetaRefusal::CellNotReady { cell_id: CELL })
    );
    assert_eq!(
        meta.fold(3, &entry(MetaCommand::AddCell { cell_id: CELL })),
        MetaEvent::Unchanged
    );
    assert_eq!(
        meta.fold(
            4,
            &entry(MetaCommand::MarkCell {
                cell_id: CELL,
                state: CellState::Registering
            })
        ),
        MetaEvent::Unchanged
    );
    assert_eq!(
        meta.fold(
            5,
            &entry(MetaCommand::MarkCell {
                cell_id: CELL,
                state: CellState::Restoring
            })
        ),
        MetaEvent::Refused(MetaRefusal::CellTransition {
            from: CellState::Registering,
            to: CellState::Restoring
        })
    );
    assert_eq!(
        meta.fold(
            6,
            &entry(MetaCommand::MarkCell {
                cell_id: CELL + 1,
                state: CellState::Ready
            })
        ),
        MetaEvent::Refused(MetaRefusal::UnknownCell { cell_id: CELL + 1 })
    );
}

#[test]
fn a_tenant_id_and_name_are_checked_at_registration() {
    let mut meta = formed();
    // Reserved ids are refused.
    assert_eq!(
        meta.fold(3, &register(2, b"acme")),
        MetaEvent::Refused(MetaRefusal::Reserved {
            tenant: TenantId(2)
        })
    );
    assert_eq!(
        meta.fold(4, &register(300, b"acme")),
        MetaEvent::TenantRegistered {
            tenant: TenantId(300),
            cell_id: CELL
        }
    );
    // The same registration again is a re-run; the id under another name is
    // taken; the name under another id is taken.
    assert_eq!(meta.fold(5, &register(300, b"acme")), MetaEvent::Unchanged);
    assert_eq!(
        meta.fold(6, &register(300, b"other")),
        MetaEvent::Refused(MetaRefusal::TenantIdTaken {
            tenant: TenantId(300)
        })
    );
    assert_eq!(
        meta.fold(7, &register(301, b"acme")),
        MetaEvent::Refused(MetaRefusal::NameTaken {
            holder: TenantId(300)
        })
    );
}

#[test]
fn a_removed_tenant_frees_its_name_and_never_its_id() {
    let mut meta = formed();
    meta.fold(4, &register(300, b"acme"));
    // REGISTERING -> READY; a removal needs REMOVING first.
    assert_eq!(
        meta.fold(8, &mark(300, TenantState::Ready)),
        MetaEvent::TenantMarked {
            tenant: TenantId(300),
            state: TenantState::Ready
        }
    );
    assert_eq!(
        meta.fold(
            9,
            &entry(MetaCommand::RemoveTenant {
                tenant: TenantId(300)
            })
        ),
        MetaEvent::Refused(MetaRefusal::TenantTransition {
            from: TenantState::Ready,
            to: TenantState::Removing
        })
    );
    assert_eq!(
        meta.fold(10, &mark(300, TenantState::UpdatingConfiguration)),
        MetaEvent::TenantMarked {
            tenant: TenantId(300),
            state: TenantState::UpdatingConfiguration
        }
    );
    assert_eq!(meta.tenant(TenantId(300)).map(|t| t.config_seq), Some(1));
    assert_eq!(
        meta.fold(11, &mark(300, TenantState::Removing)),
        MetaEvent::TenantMarked {
            tenant: TenantId(300),
            state: TenantState::Removing
        }
    );
    // A tenant being removed takes no other state.
    assert_eq!(
        meta.fold(12, &mark(300, TenantState::Ready)),
        MetaEvent::Refused(MetaRefusal::TenantTransition {
            from: TenantState::Removing,
            to: TenantState::Ready
        })
    );
    assert_eq!(
        meta.fold(
            13,
            &entry(MetaCommand::RemoveTenant {
                tenant: TenantId(300)
            })
        ),
        MetaEvent::TenantRemoved {
            tenant: TenantId(300)
        }
    );
    assert_eq!(
        meta.fold(
            14,
            &entry(MetaCommand::RemoveTenant {
                tenant: TenantId(300)
            })
        ),
        MetaEvent::Unchanged
    );
    assert!(meta.is_removed(TenantId(300)));
    assert!(meta.named(b"acme").is_none());
    // The id is never reused; the name frees up for a new id.
    assert_eq!(
        meta.fold(15, &register(300, b"acme")),
        MetaEvent::Refused(MetaRefusal::TenantIdTaken {
            tenant: TenantId(300)
        })
    );
    assert_eq!(
        meta.fold(16, &register(302, b"acme")),
        MetaEvent::TenantRegistered {
            tenant: TenantId(302),
            cell_id: CELL
        }
    );
    assert_eq!(
        meta.fold(17, &mark(302, TenantState::Registering)),
        MetaEvent::Unchanged
    );
    assert_eq!(
        meta.fold(18, &mark(999, TenantState::Ready)),
        MetaEvent::Refused(MetaRefusal::UnknownTenant {
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
        T::Renaming,
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
    let mut meta = formed();
    meta.fold(3, &register(300, b"acme"));
    meta.fold(4, &register(301, b"globex"));
    meta.fold(5, &mark(300, TenantState::Ready));
    meta.fold(6, &mark(301, TenantState::Removing));
    meta.fold(
        7,
        &entry(MetaCommand::RemoveTenant {
            tenant: TenantId(301),
        }),
    );
    let state = meta.checkpoint();
    let mut restored = Meta::default();
    restored
        .restore(meta.next_seq() - 1, &state)
        .expect("restores");
    assert_eq!(restored, meta);
    // Equal states encode to equal bytes.
    assert_eq!(restored.checkpoint(), state);
    // The restored fold goes on like the original.
    assert_eq!(
        restored.fold(8, &register(301, b"globex")),
        MetaEvent::Refused(MetaRefusal::TenantIdTaken {
            tenant: TenantId(301)
        })
    );
    assert!(Meta::default().restore(0, b"\xff\xff").is_err());
}
