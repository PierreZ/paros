//! The dropped ranges: `start .. below`, keyed by start. An acceptor drops
//! the votes of a range when its segment compacts, its promise names the
//! range, and the leader's Phase 1 skips it; the leader also adds the one
//! slot that a late answer dropped. A range is a segment fact: each segment
//! compacts alone, so a range sits in the middle of the log, and the core's
//! acceptor, with one floor, has none.

use std::collections::BTreeMap;

use paros_consensus::Slot;

/// Whether `slot` is in one of the `ranges`.
#[must_use]
pub fn contains(ranges: &BTreeMap<Slot, Slot>, slot: Slot) -> bool {
    ranges
        .range(..=slot)
        .next_back()
        .is_some_and(|(_, below)| slot < *below)
}

/// Add the range `start .. below` to `ranges`: a range at a held start
/// only grows.
///
/// # Panics
///
/// When the range is empty.
pub fn add(ranges: &mut BTreeMap<Slot, Slot>, start: Slot, below: Slot) {
    assert!(start < below, "a dropped range holds a slot");
    let held = ranges.entry(start).or_insert(below);
    *held = (*held).max(below);
}

/// The highest slot of the `ranges`, when they hold one.
///
/// # Panics
///
/// When a range is empty: [`add`] keeps every range non-empty.
#[must_use]
pub fn top(ranges: &BTreeMap<Slot, Slot>) -> Option<Slot> {
    assert!(
        ranges.iter().all(|(start, below)| start < below),
        "a dropped range holds a slot"
    );
    let top = ranges.values().max().map(|below| Slot(below.0 - 1));
    assert!(
        top.is_none_or(|slot| contains(ranges, slot)),
        "the highest dropped slot is in a range"
    );
    top
}

#[cfg(test)]
mod tests;
