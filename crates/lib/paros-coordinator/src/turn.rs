//! The coordinators' one rule of turns: each step of a watch round takes
//! the next key after the one the last step took, in key order, and wraps
//! to the first. A key that stays pending is taken again only once every
//! other one had its turn.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

/// The key of `keys` whose turn follows `last`: the first above it, else
/// the first. `None` when `keys` is empty.
pub(crate) fn next<T: Ord + Copy>(keys: &BTreeSet<T>, last: Option<T>) -> Option<T> {
    let next = last
        .and_then(|last| keys.range((Excluded(last), Unbounded)).next())
        .or_else(|| keys.first())
        .copied();
    assert_eq!(next.is_some(), !keys.is_empty(), "a turn falls on a key");
    next
}

/// The entry of `map` whose turn follows `last` (see [`next`]).
pub(crate) fn next_entry<K: Ord + Copy, V>(
    map: &BTreeMap<K, V>,
    last: Option<K>,
) -> Option<(&K, &V)> {
    let next = last
        .and_then(|last| map.range((Excluded(last), Unbounded)).next())
        .or_else(|| map.iter().next());
    assert_eq!(next.is_some(), !map.is_empty(), "a turn falls on a key");
    next
}

/// Every key of `keys` in turn after `last`: the ones above it, then from
/// the first, `last` itself at the end.
pub(crate) fn order<T: Ord + Copy>(keys: &BTreeSet<T>, last: Option<T>) -> Vec<T> {
    let order: Vec<T> = match last {
        Some(last) => keys
            .range((Excluded(last), Unbounded))
            .chain(keys.range(..=last))
            .copied()
            .collect(),
        None => keys.iter().copied().collect(),
    };
    assert_eq!(order.len(), keys.len(), "every key has one turn");
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn the_next_turn_is_the_first_key_above_the_last_and_wraps() {
        let k = keys(&[2, 5, 9]);
        assert_eq!(next(&k, None), Some(2), "a first turn takes the first key");
        assert_eq!(next(&k, Some(2)), Some(5));
        assert_eq!(next(&k, Some(3)), Some(5), "the last key may be gone");
        assert_eq!(next(&k, Some(9)), Some(2), "the turn wraps");
        assert_eq!(next(&k, Some(10)), Some(2));
        assert_eq!(
            next(&keys(&[4]), Some(4)),
            Some(4),
            "one key is its own next"
        );
        assert_eq!(next(&keys(&[]), Some(4)), None);
    }

    #[test]
    fn a_map_takes_its_turns_as_its_keys_do() {
        let map: BTreeMap<u64, &str> = [(2, "a"), (5, "b")].into_iter().collect();
        assert_eq!(next_entry(&map, None), Some((&2, &"a")));
        assert_eq!(next_entry(&map, Some(2)), Some((&5, &"b")));
        assert_eq!(next_entry(&map, Some(5)), Some((&2, &"a")));
        assert_eq!(next_entry(&BTreeMap::<u64, &str>::new(), Some(5)), None);
    }

    #[test]
    fn an_order_gives_every_key_one_turn_the_last_one_at_the_end() {
        let k = keys(&[2, 5, 9]);
        assert_eq!(order(&k, None), [2, 5, 9]);
        assert_eq!(order(&k, Some(5)), [9, 2, 5]);
        assert_eq!(order(&k, Some(6)), [9, 2, 5]);
        assert_eq!(order(&k, Some(9)), [2, 5, 9]);
        assert!(order(&keys(&[]), Some(1)).is_empty());
    }
}
