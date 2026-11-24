use std::collections::BTreeMap;

use paros_consensus::Slot;

use super::{add, contains};

#[test]
fn a_range_holds_its_start_and_not_its_end() {
    let mut ranges = BTreeMap::new();
    add(&mut ranges, Slot(2), Slot(5));
    assert!(!contains(&ranges, Slot(1)));
    assert!(contains(&ranges, Slot(2)));
    assert!(contains(&ranges, Slot(4)));
    assert!(!contains(&ranges, Slot(5)));
}

#[test]
fn a_range_at_a_held_start_only_grows() {
    let mut ranges = BTreeMap::new();
    add(&mut ranges, Slot(2), Slot(5));
    add(&mut ranges, Slot(2), Slot(3));
    assert_eq!(ranges, BTreeMap::from([(Slot(2), Slot(5))]));
    add(&mut ranges, Slot(2), Slot(7));
    assert_eq!(ranges, BTreeMap::from([(Slot(2), Slot(7))]));
}

#[test]
fn two_segments_keep_two_ranges() {
    let mut ranges = BTreeMap::new();
    add(&mut ranges, Slot(0), Slot(2));
    add(&mut ranges, Slot(4), Slot(6));
    assert!(contains(&ranges, Slot(1)));
    assert!(!contains(&ranges, Slot(3)));
    assert!(contains(&ranges, Slot(5)));
}

#[test]
#[should_panic(expected = "a dropped range holds a slot")]
fn an_empty_range_is_a_programmer_error() {
    add(&mut BTreeMap::new(), Slot(3), Slot(3));
}
