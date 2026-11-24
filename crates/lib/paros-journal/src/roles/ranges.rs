//! The dropped ranges of a segment: `start .. below` by start, one range for
//! each compaction of a segment. An acceptor drops the votes of a range, its
//! promise names the range, and the leader's Phase 1 skips it. A range is
//! a segment fact: each segment compacts alone, so a range sits in the
//! middle of the log, and the core's acceptor, with one floor, has none.

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

/// Add the range `start .. below` to `ranges`. The ranges of one segment
/// start at its start, so a range that starts at a held start only grows.
///
/// # Panics
///
/// When the range is empty or the add drops a slot that `ranges` held.
pub fn add(ranges: &mut BTreeMap<Slot, Slot>, start: Slot, below: Slot) {
    assert!(start < below, "a dropped range holds a slot");
    let before = ranges.get(&start).copied();
    let held = ranges.entry(start).or_insert(below);
    *held = (*held).max(below);
    assert!(
        before.is_none_or(|before| *held >= before),
        "a dropped range only grows"
    );
    assert!(contains(ranges, start), "the range holds its start");
}

#[cfg(test)]
mod tests;
