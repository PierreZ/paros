//! **The retained window**: a map with a floor under it.
//!
//! Two places in the core keep "everything at or above a floor, and nothing
//! below it, forever": the [`Acceptor`](crate::acceptor::Acceptor)'s accepted
//! log above its compaction floor, and the
//! [`Matchmaker`](crate::Matchmaker)'s registry above its GC watermark. They
//! are the same structure over different keys, and they had the same four
//! rules written out twice — the floor never moves backward, nothing below it
//! survives, a query below it is refused rather than answered, and what is
//! retained is handed out one bounded page at a time.
//!
//! So the rules live here once, and the *decisions* stay with their owners: a
//! below-floor `Prepare` and a collected ballot are refused differently, and
//! the acceptor's tri-state promise page interleaves two windows (the
//! readable records and the faulty entries), which [`RetainedWindow::page`]
//! deliberately does not try to express.

use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::ops::RangeBounds;

/// Everything retained at or above `floor`, keyed by `K`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedWindow<K, V> {
    entries: BTreeMap<K, V>,
    floor: K,
}

impl<K: Copy + Ord, V> RetainedWindow<K, V> {
    /// A window holding `entries` above `floor`.
    ///
    /// # Panics
    ///
    /// If any entry sits below the floor — the one thing a retained window
    /// may never hold (a boot scan that read one back is a corrupt store, not
    /// an operating condition).
    #[must_use]
    pub fn new(entries: BTreeMap<K, V>, floor: K) -> Self {
        let window = Self { entries, floor };
        window.assert_invariants();
        window
    }

    /// The floor: the first key still retained.
    #[must_use]
    pub fn floor(&self) -> K {
        self.floor
    }

    /// Whether `key` sits below the floor — everything a caller needs to
    /// refuse a question about a key this window can no longer answer.
    #[must_use]
    pub fn below_floor(&self, key: K) -> bool {
        key < self.floor
    }

    /// The retained entries, in key order.
    #[must_use]
    pub fn entries(&self) -> &BTreeMap<K, V> {
        &self.entries
    }

    /// The entry at `key`, if retained.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn get(&self, key: K) -> Option<&V> {
        let value = self.entries.get(&key);
        if value.is_some() {
            assert!(
                !self.below_floor(key),
                "a retained entry sits at or above the floor"
            );
        }
        value
    }

    /// Whether `key` is retained.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn contains_key(&self, key: K) -> bool {
        let contains = self.entries.contains_key(&key);
        if contains {
            assert!(
                !self.below_floor(key),
                "a retained key sits at or above the floor"
            );
        }
        contains
    }

    /// The retained entries in `range`, in key order.
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> Range<'_, K, V> {
        self.entries.range(range)
    }

    /// The highest retained key, if any.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn last_key(&self) -> Option<K> {
        let last = self.entries.keys().next_back().copied();
        assert!(
            last.is_none_or(|k| k >= self.floor),
            "the last entry sits above the floor"
        );
        last
    }

    /// The lowest retained key, if any.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn first_key(&self) -> Option<K> {
        let first = self.entries.keys().next().copied();
        assert!(
            first.is_some() != self.entries.is_empty(),
            "a window has a first key iff it holds an entry"
        );
        first
    }

    /// Insert `value` at `key`, returning what it replaced.
    ///
    /// # Panics
    ///
    /// If `key` sits below the floor: re-inserting under the floor would
    /// resurrect exactly what raising it dropped.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        assert!(
            !self.below_floor(key),
            "a retained window never takes an entry below its floor"
        );
        let previous = self.entries.insert(key, value);
        assert!(
            self.entries.contains_key(&key),
            "an inserted entry is retained"
        );
        previous
    }

    /// Remove the entry at `key`, returning it.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn remove(&mut self, key: K) -> Option<V> {
        let removed = self.entries.remove(&key);
        assert!(!self.entries.contains_key(&key), "a removed entry is gone");
        removed
    }

    /// Raise the floor to `floor`, dropping everything below it.
    ///
    /// # Panics
    ///
    /// If `floor` sits below the floor held: a floor is monotone for the
    /// window's whole life, which is what makes "below the floor" a stable
    /// answer.
    pub fn raise_floor(&mut self, floor: K) {
        assert!(
            floor >= self.floor,
            "a retained window's floor never moves backward"
        );
        self.entries = self.entries.split_off(&floor);
        self.floor = floor;
        assert!(
            self.floor == floor,
            "the floor lands where it was raised to"
        );
        self.assert_invariants();
    }

    /// One bounded page of the retained entries: at most `limit` of them from
    /// `max(from, floor)` up to (but not including) `upper`, plus the key the
    /// next page starts at when the window did not fit. `None` there means
    /// the answer is complete.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn page(&self, from: K, upper: K, limit: usize) -> (BTreeMap<K, V>, Option<K>)
    where
        V: Clone,
    {
        let from = from.max(self.floor);
        let mut window = self.entries.range(from..upper);
        let page: BTreeMap<K, V> = window
            .by_ref()
            .take(limit)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        let cursor = window.next().map(|(k, _)| *k);
        // Postconditions: bounded, inside `[from, upper)`, and the cursor
        // strictly past everything the page carried.
        assert!(page.len() <= limit, "a page honours its limit");
        assert!(
            page.keys().next().is_none_or(|k| *k >= from),
            "a page starts at its cursor"
        );
        if let Some(cursor) = cursor {
            assert!(cursor < upper, "a continuation lies below the upper bound");
            assert!(
                page.keys().next_back().is_none_or(|k| *k < cursor),
                "the continuation lies past the page"
            );
        }
        (page, cursor)
    }

    /// The window's own invariant: nothing below the floor. A bounded
    /// structural scan, always on (the maps are small and crash beats
    /// corruption).
    ///
    /// # Panics
    ///
    /// If an entry sits below the floor.
    pub fn assert_invariants(&self) {
        assert!(
            self.first_key().is_none_or(|first| first >= self.floor),
            "no entry survives below the retained window's floor"
        );
    }
}
