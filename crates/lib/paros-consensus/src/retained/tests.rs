//! The four rules of a retained window, each read through every accessor:
//! the floor never moves backward, nothing below it survives, a key below it
//! is refused, and what is retained is read in key order.

use std::collections::BTreeMap;

use super::RetainedWindow;

fn window(keys: &[u64], floor: u64) -> RetainedWindow<u64, char> {
    let entries: BTreeMap<u64, char> = keys
        .iter()
        .map(|&k| (k, char::from(b'a' + u8::try_from(k).unwrap())))
        .collect();
    RetainedWindow::new(entries, floor)
}

#[test]
fn an_empty_window_has_no_first_or_last_key() {
    let w = window(&[], 3);
    assert_eq!(w.first_key(), None);
    assert_eq!(w.last_key(), None);
    assert!(!w.contains_key(3));
    assert_eq!(w.get(3), None);
    assert_eq!(w.floor(), 3);
}

#[test]
fn the_accessors_read_what_the_window_holds() {
    let w = window(&[3, 5, 8], 2);
    assert_eq!(w.first_key(), Some(3));
    assert_eq!(w.last_key(), Some(8));
    assert!(w.contains_key(5));
    assert!(!w.contains_key(4));
    assert_eq!(w.get(5), Some(&'f'));
    assert_eq!(w.get(4), None);
    let keys: Vec<u64> = w.range(4..=8).map(|(k, _)| *k).collect();
    assert_eq!(keys, vec![5, 8]);
    assert_eq!(w.entries().len(), 3);
}

#[test]
fn a_key_below_the_floor_is_refused_and_one_at_it_is_not() {
    let w = window(&[4], 4);
    assert!(w.below_floor(3));
    assert!(!w.below_floor(4));
    assert!(!w.below_floor(9));
}

#[test]
fn raising_the_floor_drops_everything_below_it_and_keeps_the_rest() {
    let mut w = window(&[1, 3, 5, 8], 0);
    w.raise_floor(4);
    assert_eq!(w.floor(), 4);
    assert_eq!(w.first_key(), Some(5));
    assert!(!w.contains_key(3));
    assert!(w.contains_key(5));
    assert!(w.contains_key(8));
    // Raising it to the floor held is a no-op.
    w.raise_floor(4);
    assert_eq!(w.first_key(), Some(5));
}

#[test]
fn insert_and_remove_move_entries_at_or_above_the_floor() {
    let mut w = window(&[5], 2);
    assert_eq!(w.insert(2, 'x'), None);
    assert_eq!(w.insert(5, 'y'), Some('f'));
    assert_eq!(w.first_key(), Some(2));
    assert_eq!(w.remove(2), Some('x'));
    assert_eq!(w.remove(2), None);
    assert_eq!(w.first_key(), Some(5));
    assert_eq!(w.get(5), Some(&'y'));
}

#[test]
#[should_panic(expected = "never takes an entry below its floor")]
fn an_insert_below_the_floor_is_a_programmer_error() {
    let mut w = window(&[], 3);
    let _ = w.insert(2, 'x');
}

#[test]
#[should_panic(expected = "floor never moves backward")]
fn lowering_the_floor_is_a_programmer_error() {
    let mut w = window(&[], 3);
    w.raise_floor(2);
}

#[test]
#[should_panic(expected = "no entry survives below")]
fn a_window_built_with_an_entry_below_its_floor_is_a_programmer_error() {
    let _ = window(&[1], 2);
}
