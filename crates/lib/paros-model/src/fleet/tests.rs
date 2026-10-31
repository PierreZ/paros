use super::*;
use crate::tags::UserTags;

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

const OTHER: u64 = 0x0ce2;

fn other_control() -> JournalIdentifier {
    identifier(0xc1de, 0x53)
}

fn other_endpoint() -> Vec<(u64, Address)> {
    vec![
        (21, Address::parse("10.0.2.1:4500").unwrap()),
        (22, Address::parse("machine-2.paros:4500").unwrap()),
    ]
}

/// The universe of [`formed`] with a second cell, `OTHER`, added `READY`
/// with its endpoint (#232 (multiple cells)).
fn two_cells() -> FleetDirectory {
    let mut directory = formed();
    let add = entry(FleetCommand::AddCell {
        cell_id: OTHER,
        control: other_control(),
        name: b"west".to_vec(),
        kind: CellKind::default(),
        endpoint: other_endpoint(),
    });
    assert_eq!(
        directory.fold(3, &add),
        FleetEvent::CellAdded { cell_id: OTHER }
    );
    let ready = entry(FleetCommand::MarkCell {
        cell_id: OTHER,
        state: CellState::Ready,
    });
    assert!(matches!(
        directory.fold(4, &ready),
        FleetEvent::CellMarked { .. }
    ));
    directory
}

#[test]
fn a_second_cell_keeps_its_endpoint_and_the_universe_its_host() {
    let directory = two_cells();
    assert_eq!(directory.endpoint(OTHER), other_endpoint().as_slice());
    assert!(directory.endpoint(CELL).is_empty());
    assert_eq!(directory.universe_cell(), Some(CELL));
    assert_eq!(directory.cell_named(b"west"), Some(OTHER));
    // A checkpoint keeps the endpoint.
    let mut restored = FleetDirectory::default();
    restored.restore(4, &directory.checkpoint()).unwrap();
    assert_eq!(restored.endpoint(OTHER), other_endpoint().as_slice());
}

#[test]
fn a_cell_is_removed_only_empty_and_removing_and_never_comes_back() {
    let mut directory = two_cells();
    let remove_cell = entry(FleetCommand::RemoveCell { cell_id: OTHER });
    // Not REMOVING yet.
    assert!(matches!(
        directory.fold(5, &remove_cell),
        FleetEvent::Refused(FleetDirectoryRefusal::CellTransition { .. })
    ));
    // A tenant lives there: not empty.
    let tenant = entry(FleetCommand::RegisterTenant {
        control: identifier(0x700, 0x701),
        name: b"acme".to_vec(),
        cell_id: OTHER,
        survives: Survives::Az,
        tags: UserTags::none(),
        placement: buckets::Placement::override_for("named"),
    });
    assert!(matches!(
        directory.fold(6, &tenant),
        FleetEvent::TenantRegistered { .. }
    ));
    let removing = entry(FleetCommand::MarkCell {
        cell_id: OTHER,
        state: CellState::Removing,
    });
    assert!(matches!(
        directory.fold(7, &removing),
        FleetEvent::CellMarked { .. }
    ));
    assert_eq!(
        directory.fold(8, &remove_cell),
        FleetEvent::Refused(FleetDirectoryRefusal::CellNotEmpty {
            cell_id: OTHER,
            tenant: TenantId(0x700)
        })
    );
    assert_eq!(
        directory.fold(9, &mark(0x700, TenantState::Removing)),
        FleetEvent::TenantMarked {
            tenant: TenantId(0x700),
            state: TenantState::Removing
        }
    );
    assert_eq!(
        directory.fold(10, &remove(0x700)),
        FleetEvent::TenantRemoved {
            tenant: TenantId(0x700)
        }
    );
    assert_eq!(
        directory.fold(11, &remove_cell),
        FleetEvent::CellRemoved { cell_id: OTHER }
    );
    assert!(directory.cell(OTHER).is_none());
    assert!(directory.is_cell_removed(OTHER));
    assert!(directory.is_removed(other_control().tenant));
    assert!(directory.endpoint(OTHER).is_empty());
    // A re-run changes nothing; a rejoin is refused.
    assert_eq!(directory.fold(12, &remove_cell), FleetEvent::Unchanged);
    let rejoin = entry(FleetCommand::AddCell {
        cell_id: OTHER,
        control: identifier(0xc2de, 0x54),
        name: Vec::new(),
        kind: CellKind::default(),
        endpoint: Vec::new(),
    });
    assert_eq!(
        directory.fold(13, &rejoin),
        FleetEvent::Refused(FleetDirectoryRefusal::CellTombstoned { cell_id: OTHER })
    );
    // A checkpoint keeps the tombstone.
    let mut restored = FleetDirectory::default();
    restored.restore(13, &directory.checkpoint()).unwrap();
    assert!(restored.is_cell_removed(OTHER));
    assert_eq!(
        restored.fold(14, &rejoin),
        FleetEvent::Refused(FleetDirectoryRefusal::CellTombstoned { cell_id: OTHER })
    );
}

fn root_key(key_id: u32, label: &str) -> RootKeyEntry {
    RootKeyEntry {
        key_id,
        label: label.into(),
        public_key: format!("ed25519/{}", "0f".repeat(32)),
    }
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
                name: Vec::new(),
                kind: CellKind::default(),
                endpoint: Vec::new(),
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
        tags: UserTags::none(),
        placement: buckets::Placement::Bucket,
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
        FleetCommand::AssignBuckets {
            range: buckets::BucketRange::new(10, 20).unwrap(),
            cell_id: CELL,
        },
        FleetCommand::FormFleet {
            control: fleet_control(),
            name: Vec::new(),
        },
        FleetCommand::AddCell {
            cell_id: CELL,
            control: cell_control(),
            name: Vec::new(),
            kind: CellKind::default(),
            endpoint: Vec::new(),
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
            tags: UserTags::none(),
            placement: buckets::Placement::Bucket,
        },
        FleetCommand::MarkTenant {
            tenant: TenantId(300),
            state: TenantState::UpdatingConfiguration,
        },
        FleetCommand::SetTenantTags {
            request: 9,
            tenant: TenantId(300),
            tags: UserTags::parse(&["team:payments", "env:prod"]).unwrap(),
        },
        FleetCommand::SetCellTags {
            request: 10,
            cell_id: CELL,
            tags: UserTags::none(),
        },
        FleetCommand::AddCell {
            cell_id: CELL,
            control: cell_control(),
            name: b"west".to_vec(),
            kind: CellKind::MultiRegion {
                regions: vec!["w".into(), "c".into(), "n".into()],
                witness: "n".into(),
            },
            endpoint: Vec::new(),
        },
        FleetCommand::RemoveTenant {
            tenant: TenantId(300),
        },
        FleetCommand::AddRootKey(root_key(7, "current")),
        FleetCommand::RemoveRootKey {
            label: "current".into(),
        },
        FleetCommand::AddCell {
            cell_id: OTHER,
            control: other_control(),
            name: b"west".to_vec(),
            kind: CellKind::default(),
            endpoint: other_endpoint(),
        },
        FleetCommand::RemoveCell { cell_id: OTHER },
    ];
    for command in commands {
        let entry = FleetEntry::new(FLEET, command);
        assert_eq!(FleetEntry::decode(&entry.encode()), Ok(entry));
    }
    assert!(FleetEntry::decode(b"\xff\xff").is_err());
}

#[test]
#[allow(clippy::too_many_lines)]
fn the_fleet_is_formed_once_with_the_fleet_tenant_as_its_first_internal_tenant() {
    let mut directory = FleetDirectory::default();
    let add = entry(FleetCommand::AddCell {
        cell_id: CELL,
        control: cell_control(),
        name: Vec::new(),
        kind: CellKind::default(),
        endpoint: Vec::new(),
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
        (own.kind, own.movable, own.control),
        (
            TenantKind::Universe,
            Movable::Movable,
            fleet_control().journal
        )
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
            kind: CellKind::default(),
            endpoint: Vec::new(),
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
        kind: CellKind::default(),
        endpoint: Vec::new(),
    });
    assert_eq!(
        directory.fold(1, &add),
        FleetEvent::CellAdded { cell_id: CELL }
    );
    let cell_tenant = directory
        .tenant(cell_control().tenant)
        .expect("the cell tenant");
    assert_eq!(
        (cell_tenant.kind, cell_tenant.movable, cell_tenant.cell_id),
        (TenantKind::Cell, Movable::Pinned, CELL)
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
                name: Vec::new(),
                kind: CellKind::default(),
                endpoint: Vec::new(),
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
                name: Vec::new(),
                kind: CellKind::default(),
                endpoint: Vec::new(),
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
                tags: UserTags::none(),
                placement: buckets::Placement::Bucket,
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
    assert_eq!((acme.kind, acme.control), (TenantKind::Users, JournalId(4)));
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
    let added = directory.fold(8, &entry(FleetCommand::AddRootKey(root_key(1, "current"))));
    assert_eq!(added, FleetEvent::RootKeyAdded { key_id: 1 });
    directory.fold(9, &entry(FleetCommand::AddRootKey(root_key(2, "next"))));
    let removed = directory.fold(
        10,
        &entry(FleetCommand::RemoveRootKey {
            label: "current".into(),
        }),
    );
    assert_eq!(removed, FleetEvent::RootKeyRemoved { key_id: 1 });
    let state = directory.checkpoint();
    let mut restored = FleetDirectory::default();
    restored
        .restore(directory.next_seq() - 1, &state)
        .expect("restores");
    assert_eq!(restored, directory);
    // Equal states encode to equal bytes.
    assert_eq!(restored.checkpoint(), state);
    let keys: Vec<_> = restored.root_keys().map(|key| key.key_id).collect();
    assert_eq!(keys, [2]);
    // The restored fold goes on like the original.
    assert_eq!(
        restored.fold(11, &entry(FleetCommand::AddRootKey(root_key(1, "again")))),
        FleetEvent::Refused(FleetDirectoryRefusal::RootKeyTaken {
            key_id: 1,
            label: "again".into()
        })
    );
    assert_eq!(
        restored.fold(12, &register(301, b"globex")),
        FleetEvent::Refused(FleetDirectoryRefusal::TenantIdTaken {
            tenant: TenantId(301)
        })
    );
    assert!(FleetDirectory::default().restore(0, b"\xff\xff").is_err());
}

#[test]
fn the_reserved_tags_decide_who_moves() {
    // The three kinds of §3.7, and the one rule that pins a tenant.
    let mut directory = formed();
    directory.fold(3, &register(3, b"acme"));
    let label = |tenant: TenantId| directory.tenant(tenant).unwrap().all_tags().label();
    assert_eq!(
        label(fleet_control().tenant),
        "paros/kind:internal,paros/movable:true,paros/role:universe"
    );
    assert_eq!(
        label(cell_control().tenant),
        "paros/kind:internal,paros/movable:false,paros/role:cell"
    );
    assert_eq!(label(TenantId(3)), "paros/kind:users,paros/movable:true");
    assert!(directory.tenant(fleet_control().tenant).unwrap().may_move());
    assert!(directory.tenant(TenantId(3)).unwrap().may_move());
    assert!(!directory.tenant(cell_control().tenant).unwrap().may_move());
    assert!(TenantKind::Cell.is_internal() && TenantKind::Universe.is_internal());
    assert!(!TenantKind::Users.is_internal());
    // A checkpoint that unpins the cell tenant, or names a role a users
    // tenant has not, is refused.
    let mut state = wire::FleetDirectoryState::decode(directory.checkpoint().as_slice()).unwrap();
    let cell = state
        .tenants
        .iter_mut()
        .find(|t| t.tenant == cell_control().tenant.0)
        .unwrap();
    for tag in &mut cell.tags {
        if tag.key == crate::tags::reserved::MOVABLE {
            tag.value = "true".into();
        }
    }
    assert!(
        FleetDirectory::default()
            .restore(0, &state.encode_to_vec())
            .is_err()
    );
}

#[test]
fn user_tags_are_set_once_per_request_and_a_user_name_never_holds_a_colon() {
    let mut directory = formed();
    assert_eq!(
        directory.fold(3, &register(3, b"cell:prod")),
        FleetEvent::Refused(FleetDirectoryRefusal::NameRefused)
    );
    directory.fold(4, &register(3, b"acme"));
    let tags = |texts: &[&str]| UserTags::parse(texts).unwrap();
    let set = |request: u64, texts: &[&str]| {
        entry(FleetCommand::SetTenantTags {
            request,
            tenant: TenantId(3),
            tags: tags(texts),
        })
    };
    assert_eq!(
        directory.fold(5, &set(1, &["team:payments"])),
        FleetEvent::TenantTagged {
            tenant: TenantId(3)
        }
    );
    assert_eq!(
        directory.fold(6, &set(2, &["team:core", "env:prod"])),
        FleetEvent::TenantTagged {
            tenant: TenantId(3)
        }
    );
    // A retry of the first request never undoes the second.
    assert_eq!(
        directory.fold(7, &set(1, &["team:payments"])),
        FleetEvent::Unchanged
    );
    assert_eq!(
        directory.tenant(TenantId(3)).unwrap().tags,
        tags(&["team:core", "env:prod"])
    );
    assert_eq!(
        directory.fold(
            8,
            &entry(FleetCommand::SetCellTags {
                request: 3,
                cell_id: CELL,
                tags: tags(&["disk:nvme"]),
            })
        ),
        FleetEvent::CellTagged { cell_id: CELL }
    );
    assert_eq!(
        directory.cell(CELL).unwrap().all_tags().label(),
        "disk:nvme,paros/kind:regional"
    );
    assert_eq!(
        directory.fold(
            9,
            &entry(FleetCommand::SetCellTags {
                request: 4,
                cell_id: CELL + 1,
                tags: tags(&[]),
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::UnknownCell { cell_id: CELL + 1 })
    );
    // A reserved key on the wire is refused at apply, never applied.
    let mut forged = wire::FleetEntry::decode(set(5, &["a:b"]).as_slice()).unwrap();
    if let Some(wire::fleet_entry::Kind::SetTenantTags(set)) = &mut forged.kind {
        set.tags[0].key = "paros/kind".into();
    }
    assert_eq!(
        directory.fold(10, &forged.encode_to_vec()),
        FleetEvent::Refused(FleetDirectoryRefusal::Malformed)
    );
    assert!(!directory.tagged(5));
    // The tags and the applied requests cross a checkpoint.
    let mut restored = FleetDirectory::default();
    restored.restore(10, &directory.checkpoint()).unwrap();
    assert_eq!(restored.tenant(TenantId(3)), directory.tenant(TenantId(3)));
    assert_eq!(restored.cell(CELL), directory.cell(CELL));
    assert!(restored.tagged(1) && restored.tagged(2) && restored.tagged(3));
}

#[test]
fn a_root_key_needs_a_formed_fleet() {
    let mut directory = FleetDirectory::default();
    assert_eq!(
        directory.fold(0, &entry(FleetCommand::AddRootKey(root_key(1, "current")))),
        FleetEvent::Refused(FleetDirectoryRefusal::NoFleet)
    );
    let mut directory = formed();
    assert_eq!(
        directory.fold(3, &entry(FleetCommand::AddRootKey(root_key(1, "current")))),
        FleetEvent::RootKeyAdded { key_id: 1 }
    );
    assert_eq!(
        directory.fold(
            4,
            &entry(FleetCommand::RemoveRootKey {
                label: "current".into()
            })
        ),
        FleetEvent::Refused(FleetDirectoryRefusal::LastRootKey {
            label: "current".into()
        })
    );
}

fn assign(first: u16, last: u16, cell_id: u64) -> Vec<u8> {
    entry(FleetCommand::AssignBuckets {
        range: buckets::BucketRange::new(first, last).unwrap(),
        cell_id,
    })
}

#[test]
fn the_first_cell_takes_every_bucket_and_a_new_cell_none() {
    let directory = two_cells();
    assert_eq!(
        directory.buckets().count(CELL),
        usize::from(buckets::BUCKETS)
    );
    assert_eq!(directory.buckets().count(OTHER), 0);
    assert_eq!(directory.bucket_cell_of(b"acme"), Some(CELL));
}

#[test]
fn a_bucket_placed_tenant_names_its_buckets_cell() {
    let mut directory = two_cells();
    let bucket = buckets::bucket_of(b"acme");
    // To the other cell by its bucket: the bucket names this cell.
    let elsewhere = entry(FleetCommand::RegisterTenant {
        control: identifier(0x700, 0x701),
        name: b"acme".to_vec(),
        cell_id: OTHER,
        survives: Survives::Az,
        tags: UserTags::none(),
        placement: buckets::Placement::Bucket,
    });
    assert_eq!(
        directory.fold(5, &elsewhere),
        FleetEvent::Refused(FleetDirectoryRefusal::BucketElsewhere {
            bucket,
            cell_id: CELL
        })
    );
    // Move its bucket, then the same entry lands.
    assert_eq!(
        directory.fold(6, &assign(bucket, bucket, OTHER)),
        FleetEvent::BucketsAssigned {
            range: buckets::BucketRange::new(bucket, bucket).unwrap(),
            cell_id: OTHER
        }
    );
    assert_eq!(
        directory.fold(7, &assign(bucket, bucket, OTHER)),
        FleetEvent::Unchanged
    );
    assert!(matches!(
        directory.fold(8, &elsewhere),
        FleetEvent::TenantRegistered { cell_id: OTHER, .. }
    ));
    assert_eq!(directory.misplaced(), None);
    // The bucket holds a tenant now: it does not move back.
    assert_eq!(
        directory.fold(9, &assign(0, buckets::BUCKETS - 1, CELL)),
        FleetEvent::Refused(FleetDirectoryRefusal::BucketNotEmpty {
            bucket,
            tenant: TenantId(0x700)
        })
    );
    // A checkpoint keeps the table and the placement.
    let mut restored = FleetDirectory::default();
    restored.restore(9, &directory.checkpoint()).unwrap();
    assert_eq!(restored.buckets(), directory.buckets());
    assert_eq!(
        restored
            .tenant(TenantId(0x700))
            .map(|t| t.placement.clone()),
        Some(buckets::Placement::Bucket)
    );
}

#[test]
fn an_override_wins_over_the_bucket_and_never_blocks_a_move() {
    let mut directory = two_cells();
    let named = entry(FleetCommand::RegisterTenant {
        control: identifier(0x700, 0x701),
        name: b"acme".to_vec(),
        cell_id: OTHER,
        survives: Survives::Az,
        tags: UserTags::none(),
        placement: buckets::Placement::override_for("dedicated"),
    });
    assert!(matches!(
        directory.fold(5, &named),
        FleetEvent::TenantRegistered { cell_id: OTHER, .. }
    ));
    assert_eq!(
        directory
            .tenant(TenantId(0x700))
            .map(|t| t.placement.label()),
        Some("override (dedicated)".to_string())
    );
    // The universe tenant and the cell tenants are placed by their kind.
    assert!(
        directory
            .tenants()
            .all(|(_, t)| t.kind == TenantKind::Users || !t.placement.is_bucket())
    );
    // Every bucket can move to the other cell: no tenant is placed by one.
    assert!(matches!(
        directory.fold(6, &assign(0, buckets::BUCKETS - 1, OTHER)),
        FleetEvent::BucketsAssigned { .. }
    ));
    assert_eq!(directory.bucket_cell_of(b"acme"), Some(OTHER));
}

#[test]
fn buckets_go_only_to_a_ready_cell_and_hold_a_cell_in_the_universe() {
    let mut directory = two_cells();
    assert_eq!(
        directory.fold(5, &assign(0, 9, 0x5eed)),
        FleetEvent::Refused(FleetDirectoryRefusal::UnknownCell { cell_id: 0x5eed })
    );
    assert!(matches!(
        directory.fold(6, &assign(0, 9, OTHER)),
        FleetEvent::BucketsAssigned { .. }
    ));
    let removing = entry(FleetCommand::MarkCell {
        cell_id: OTHER,
        state: CellState::Removing,
    });
    assert!(matches!(
        directory.fold(7, &removing),
        FleetEvent::CellMarked { .. }
    ));
    assert_eq!(
        directory.fold(8, &assign(10, 19, OTHER)),
        FleetEvent::Refused(FleetDirectoryRefusal::CellNotReady { cell_id: OTHER })
    );
    let remove_cell = entry(FleetCommand::RemoveCell { cell_id: OTHER });
    assert_eq!(
        directory.fold(9, &remove_cell),
        FleetEvent::Refused(FleetDirectoryRefusal::CellHoldsBuckets {
            cell_id: OTHER,
            buckets: 10
        })
    );
    assert!(matches!(
        directory.fold(10, &assign(0, 9, CELL)),
        FleetEvent::BucketsAssigned { .. }
    ));
    assert_eq!(
        directory.fold(11, &remove_cell),
        FleetEvent::CellRemoved { cell_id: OTHER }
    );
}

#[test]
fn a_version_5_registration_reads_as_an_override() {
    let mut record = FleetEntry::new(
        FLEET,
        FleetCommand::RegisterTenant {
            control: identifier(0x700, 0x701),
            name: b"acme".to_vec(),
            cell_id: CELL,
            survives: Survives::Az,
            tags: UserTags::none(),
            placement: buckets::Placement::Bucket,
        },
    );
    let mut wire = paros_wire::fleet::FleetEntry::decode(record.encode().as_slice()).unwrap();
    wire.version = 5;
    if let Some(paros_wire::fleet::fleet_entry::Kind::RegisterTenant(register)) = &mut wire.kind {
        register.placement = 0;
    }
    let decoded = FleetEntry::decode(&wire.encode_to_vec()).unwrap();
    record.version = 5;
    record.command = match record.command {
        FleetCommand::RegisterTenant {
            control,
            name,
            cell_id,
            survives,
            tags,
            ..
        } => FleetCommand::RegisterTenant {
            control,
            name,
            cell_id,
            survives,
            tags,
            placement: buckets::Placement::override_for("placed before buckets"),
        },
        other => other,
    };
    assert_eq!(decoded, record);
    // At version 6, a registration names its placement.
    wire.version = 6;
    assert!(FleetEntry::decode(&wire.encode_to_vec()).is_err());
}

#[test]
fn a_selector_picks_a_ready_matching_cell_with_the_fewest_tenants() {
    let mut directory = two_cells();
    let tag = |request, cell_id, text: &str| {
        entry(FleetCommand::SetCellTags {
            request,
            cell_id,
            tags: UserTags::parse(&[text]).unwrap(),
        })
    };
    assert!(matches!(
        directory.fold(5, &tag(1, CELL, "env:prod")),
        FleetEvent::CellTagged { .. }
    ));
    assert!(matches!(
        directory.fold(6, &tag(2, OTHER, "env:prod")),
        FleetEvent::CellTagged { .. }
    ));
    let prod: crate::tags::Selector = "env:prod".parse().unwrap();
    // Both match and hold no tenant: the first by name (the cell is unnamed).
    assert_eq!(directory.select_cell(&prod, Survives::Az), Some(CELL));
    assert!(matches!(
        directory.fold(7, &register(0x700, b"acme")),
        FleetEvent::TenantRegistered { .. }
    ));
    assert_eq!(directory.select_cell(&prod, Survives::Az), Some(OTHER));
    // No regional cell carries a tenant that survives a region.
    assert_eq!(directory.select_cell(&prod, Survives::Region), None);
    let staging: crate::tags::Selector = "env:staging".parse().unwrap();
    assert_eq!(directory.select_cell(&staging, Survives::Az), None);
}
