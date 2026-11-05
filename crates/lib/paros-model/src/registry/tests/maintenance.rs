//! Maintenance windows (#359) and space marks (#360 (disk full)): what the
//! registry fold holds of them, and how they cross a checkpoint.

use super::*;
use crate::checkpoint::{Folded, Folder, run};

fn register_in(id: u64, zone: &str) -> Vec<u8> {
    one(&SystemCommand::RegisterNode {
        id: NodeId(id),
        addr: format!("n{id}"),
        class: Class::Storage,
        locality: crate::machine::Locality::zone(zone),
        name: format!("m{id}"),
        incarnation: u128::from(id),
        machine_interface: Vec::new(),
    })
}

fn window(scope: MaintenanceScope, for_ms: u64) -> Vec<u8> {
    one(&SystemCommand::Maintenance { scope, for_ms })
}

fn space(id: u64, incarnation: u128, low: bool) -> Vec<u8> {
    one(&SystemCommand::MachineSpace {
        id: NodeId(id),
        incarnation,
        low,
        free_bytes: 7,
    })
}

/// The windows over machine `id`, as `(scope, for_ms, seq)`.
fn windows_of(reg: &Registry, id: u64) -> Vec<(MaintenanceScope, u64, u64)> {
    reg.maintenance_of(NodeId(id))
        .map(|(scope, w)| (scope.clone(), w.for_ms, w.seq))
        .collect()
}

#[test]
fn a_window_opens_is_replaced_and_ends() {
    let machine = MaintenanceScope::Machine(NodeId(100));
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register_in(100, "zone-a"));
    assert_eq!(
        reg.fold(1, &window(machine.clone(), 500)),
        RegistryEvent::Maintenance {
            scope: machine.clone(),
            for_ms: 500
        }
    );
    assert_eq!(windows_of(&reg, 100), vec![(machine.clone(), 500, 1)]);
    // A later entry for the same scope opens a new window.
    reg.fold(2, &window(machine.clone(), 900));
    assert_eq!(windows_of(&reg, 100), vec![(machine.clone(), 900, 2)]);
    assert_eq!(
        reg.fold(3, &window(machine.clone(), 0)),
        RegistryEvent::Maintenance {
            scope: machine.clone(),
            for_ms: 0
        }
    );
    assert_eq!(reg.maintenance().count(), 0);
    // No window to end: the entry changes nothing.
    assert_eq!(reg.fold(4, &window(machine, 0)), RegistryEvent::Unchanged);
}

#[test]
fn a_machine_window_names_a_machine_of_the_pool() {
    let mut reg = Registry::new([NodeId(5)]);
    assert_eq!(
        reg.fold(0, &window(MaintenanceScope::Machine(NodeId(7)), 500)),
        RegistryEvent::Refused(RegistryRefusal::NotInPool { id: NodeId(7) })
    );
    // A genesis node is in the pool; a zone needs no machine.
    let genesis = MaintenanceScope::Machine(NodeId(5));
    assert_eq!(
        reg.fold(1, &window(genesis.clone(), 500)),
        RegistryEvent::Maintenance {
            scope: genesis.clone(),
            for_ms: 500
        }
    );
    assert!(matches!(
        reg.fold(2, &window(MaintenanceScope::Zone("zone-z".into()), 500)),
        RegistryEvent::Maintenance { .. }
    ));
    assert_eq!(reg.maintenance().count(), 2);
    assert_eq!(windows_of(&reg, 5), vec![(genesis, 500, 1)]);
}

#[test]
fn a_machine_is_covered_by_its_own_window_and_its_zones() {
    let zone_a = MaintenanceScope::Zone("zone-a".into());
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register_in(100, "zone-a"));
    reg.fold(1, &register_in(200, "zone-b"));
    reg.fold(2, &window(zone_a.clone(), 500));
    reg.fold(3, &window(MaintenanceScope::Machine(NodeId(200)), 600));
    assert_eq!(windows_of(&reg, 100), vec![(zone_a.clone(), 500, 2)]);
    assert_eq!(
        windows_of(&reg, 200),
        vec![(MaintenanceScope::Machine(NodeId(200)), 600, 3)]
    );
    // A machine the registry does not hold has no zone.
    assert_eq!(windows_of(&reg, 300), vec![]);
    assert!(zone_a.covers(NodeId(300), "zone-a"));
    assert!(!zone_a.covers(NodeId(100), "zone-b"));
    assert!(MaintenanceScope::Machine(NodeId(300)).covers(NodeId(300), "zone-b"));
    assert!(!MaintenanceScope::Machine(NodeId(300)).covers(NodeId(100), "zone-b"));
}

#[test]
fn a_retired_machine_takes_its_window_with_it() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register_in(100, "zone-a"));
    reg.fold(1, &window(MaintenanceScope::Machine(NodeId(100)), 500));
    reg.fold(2, &window(MaintenanceScope::Zone("zone-a".into()), 500));
    reg.fold(3, &one(&SystemCommand::DrainNode { id: NodeId(100) }));
    assert_eq!(
        reg.fold(4, &one(&SystemCommand::RetireNode { id: NodeId(100) })),
        RegistryEvent::Retired { id: NodeId(100) }
    );
    let left: Vec<_> = reg.maintenance().map(|(scope, _)| scope.clone()).collect();
    assert_eq!(left, vec![MaintenanceScope::Zone("zone-a".into())]);
}

#[test]
fn space_marks_are_changes_only() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register_in(100, "zone-a"));
    assert_eq!(
        reg.fold(1, &space(7, 7, true)),
        RegistryEvent::Refused(RegistryRefusal::NotInPool { id: NodeId(7) })
    );
    assert_eq!(
        reg.fold(2, &space(100, 5, true)),
        RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(3, &space(100, 100, false)),
        RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(4, &space(100, 100, true)),
        RegistryEvent::MachineSpace {
            id: NodeId(100),
            low: true
        }
    );
    let held = reg.liveness(NodeId(100));
    assert!(held.low_space && held.up && !held.slow);
    assert_eq!(
        reg.fold(5, &space(100, 100, true)),
        RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(6, &space(100, 100, false)),
        RegistryEvent::MachineSpace {
            id: NodeId(100),
            low: false
        }
    );
    assert!(!reg.liveness(NodeId(100)).low_space);
}

#[test]
fn windows_and_space_marks_cross_a_checkpoint() {
    let records = [
        register_in(100, "zone-a"),
        window(MaintenanceScope::Machine(NodeId(100)), 500),
        window(MaintenanceScope::Zone("zone-b".into()), 900),
        space(100, 100, true),
    ];
    let mut whole = Folder::new(Registry::new([NodeId(0)]));
    for (seq, record) in (0..).zip(&records) {
        assert!(matches!(whole.fold(seq, record), Some(Folded::Entry(_))));
    }
    let at = records.len() as u64;
    let run = run(at, &whole.state().checkpoint(), 64);
    let mut restored = Folder::new(Registry::new([NodeId(0)]));
    restored.jump(at);
    let end = at + run.len() as u64 - 1;
    for (seq, record) in (at..end).zip(&run) {
        assert_eq!(restored.fold(seq, record), Some(Folded::Run));
    }
    assert_eq!(
        restored.fold(end, run.last().expect("a run has an End")),
        Some(Folded::Checkpoint {
            covers_up_to: at,
            verified: None
        })
    );
    assert_eq!(restored.state().checkpoint(), whole.state().checkpoint());
    assert_eq!(windows_of(restored.state(), 100).len(), 1);
    assert_eq!(restored.state().maintenance().count(), 2);
    assert!(restored.state().liveness(NodeId(100)).low_space);
}

#[test]
fn a_checkpoint_with_a_window_it_cannot_hold_is_refused() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register_in(100, "zone-a"));
    reg.fold(1, &window(MaintenanceScope::Machine(NodeId(100)), 500));
    let state = wire::RegistryState::decode(reg.checkpoint().as_slice()).unwrap();
    let with = |change: fn(&mut wire::MaintenanceState)| {
        let mut state = state.clone();
        change(&mut state.maintenance[0]);
        state.encode_to_vec()
    };
    let mut restored = Registry::new([NodeId(0)]);
    assert_eq!(restored.restore(2, &with(|_| {})), Ok(()));
    assert_eq!(restored.checkpoint(), reg.checkpoint());
    let refusal = Err("a registry state holds a maintenance window it cannot");
    assert_eq!(restored.restore(2, &with(|m| m.for_ms = 0)), refusal);
    assert_eq!(restored.restore(2, &with(|m| m.seq = 3)), refusal);
    assert_eq!(
        restored.restore(2, &with(|m| m.zone = "zone-a".into())),
        Err("a maintenance window covers one machine or one zone")
    );
    assert_eq!(
        restored.restore(2, &with(|m| m.node = 0)),
        Err("a maintenance window covers one machine or one zone")
    );
}
