//! A `Register` checked where it enters, and the registry's record of the
//! machine it names.

use super::*;
use crate::fixture;
use paros_model::machine::machine_tags;
use paros_model::tags::Tag;

fn identity(id: u64, addr: &str, cell_id: u64) -> wire::IdentifyAck {
    wire::IdentifyAck {
        node_id: id,
        addr: addr.to_string(),
        tags: machine_tags(Class::Storage, &Locality::zone("z9"))
            .iter()
            .map(Tag::to_wire)
            .collect(),
        name: format!("m{id}"),
        cell_id,
        ..wire::IdentifyAck::default()
    }
}

fn register(identity: Option<wire::IdentifyAck>) -> wire::Register {
    wire::Register { identity }
}

#[test]
fn a_register_names_its_machine_address_class_and_zone() {
    let (identity, (id, addr, class, locality)) =
        registering(register(Some(identity(2, "m2.paros:1", 8))), 8).expect("well formed");
    assert_eq!(identity.node_id, 2);
    assert_eq!((id, class), (NodeId(2), Class::Storage));
    assert_eq!(addr, Address::parse("m2.paros:1").expect("an address"));
    assert_eq!(locality.zone, "z9");
}

#[test]
fn a_malformed_register_or_one_of_another_cell_is_refused() {
    let refused = |request| registering(request, 8).map(|_| ()).unwrap_err();
    assert_eq!(refused(register(None)), "malformed", "no identity");
    assert_eq!(
        refused(register(Some(identity(0, "m.paros:1", 8)))),
        "malformed",
        "no id"
    );
    assert_eq!(
        refused(register(Some(identity(2, "", 8)))),
        "malformed",
        "no address"
    );
    let mut untagged = identity(2, "m2.paros:1", 8);
    untagged.tags.clear();
    assert_eq!(refused(register(Some(untagged))), "malformed", "no roles");
    assert_eq!(
        refused(register(Some(identity(2, "m2.paros:1", 9)))),
        "other_cell"
    );
}

#[test]
fn the_registry_holds_a_machine_under_its_name_interface_and_class() {
    let mut registry = fixture::cell(&[1]);
    fixture::register(&mut registry, 2, Class::Storage);
    let same = identity(2, "m2.paros:1", 8);
    assert!(holds(&registry, &same, Class::Storage));
    assert!(!holds(&registry, &same, Class::All), "another class");
    let mut renamed = same.clone();
    renamed.name = "other".into();
    assert!(!holds(&registry, &renamed, Class::Storage), "another name");
    let mut moved = same.clone();
    moved.machine_interface = vec![1];
    assert!(
        !holds(&registry, &moved, Class::Storage),
        "another interface"
    );
    assert!(
        !holds(&registry, &identity(3, "m3.paros:1", 8), Class::Storage),
        "unknown"
    );
}
