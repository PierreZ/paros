//! **Batchers and unbatchers** (#145, Compartmentalized Paxos §3.5,
//! `docs/architecture/02-data-plane.md` §2.4): the two roles that put many
//! writers' writes of one **multi-writer** journal into one Paxos slot, and
//! hand each write its own `seq` range back.
//!
//! A multi-writer journal has no leader and no expected sequence: the
//! journal state machine gives an accepted write `[next_seq, next_seq + n)`
//! at apply (`JournalState`). So `k` writes of one journal merged in order
//! into one write of `n_1 + .. + n_k` records change nothing a writer can
//! observe: the merged write lands at `[seq, seq + N)` as a whole or not at
//! all, and write `i` sits at `[seq + n_1 + .. + n_(i-1), .. + n_i)`, dense
//! and disjoint with its neighbours. A slot holds one write, as for a
//! single-writer journal (whose client batches, `paros::client::Writer`);
//! a batcher never touches a single-writer journal.
//!
//! - [`Batcher`] is pure accumulation with a flush rule: one lane per
//!   journal, flushed when it holds [`BatchLimits::flush_records`] records,
//!   when the next write would pass the leader's limits, or at a
//!   driver-delivered [`Batcher::tick`] (the core has no timer). Lanes never
//!   mix journals: a [`Batch`] names one journal, so a slot never holds two
//!   journals' writes.
//! - [`Unbatcher`] is a pure scatter: from the merged write's answer to one
//!   answer per write, in the order the batch holds them.
//!
//! A write the batcher must not merge goes out alone, in its own batch: an
//! empty write (the journal refuses it, and a merged one would be accepted
//! with its neighbours) and a write over the limits on its own (the leader
//! refuses it at the edge, `TooLarge`, and that answer is the writer's).
//! Within the limits a merged multi-writer write is never refused for its
//! size. Every write of a batch is answered the same way when the merged
//! write is refused or goes unanswered: a re-sent batch may duplicate its
//! writes (at-least-once, §2.4), but never splits one.

use std::collections::BTreeMap;

use crate::JournalIdentifier;

/// The flush rule's bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchLimits {
    /// The most records one merged write may carry: the leader's
    /// `max_batch_records`. At least 1.
    pub max_records: usize,
    /// The most record bytes one merged write may carry: the leader's
    /// `max_batch_bytes`. At least 1.
    pub max_bytes: usize,
    /// A lane flushes once it holds this many records, before a tick. At
    /// least 1 (every write flushes at once), at most `max_records`.
    pub flush_records: usize,
}

impl BatchLimits {
    /// Whether the bounds are usable: every bound at least 1, and a lane
    /// that flushes within the record limit.
    #[must_use]
    pub fn valid(&self) -> bool {
        self.max_records >= 1
            && self.max_bytes >= 1
            && self.flush_records >= 1
            && self.flush_records <= self.max_records
    }
}

/// The writes of one journal a batcher sends as one merged write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch<T> {
    /// The journal every write names.
    pub journal: JournalIdentifier,
    /// Each write's token and record count, in the order its records sit
    /// in `records`.
    pub writes: Vec<(T, usize)>,
    /// The merged write's records: each write's records in turn.
    pub records: Vec<Vec<u8>>,
}

impl<T> Batch<T> {
    /// Whether the batch holds more than one write.
    #[must_use]
    pub fn merged(&self) -> bool {
        self.writes.len() > 1
    }

    /// Its record bytes.
    #[must_use]
    pub fn bytes(&self) -> usize {
        bytes_of(&self.records)
    }
}

/// The record bytes of `records`.
fn bytes_of(records: &[Vec<u8>]) -> usize {
    records.iter().map(Vec::len).fold(0, usize::saturating_add)
}

/// One journal's writes not yet flushed.
#[derive(Debug)]
struct Lane<T> {
    writes: Vec<(T, usize)>,
    records: Vec<Vec<u8>>,
    bytes: usize,
}

impl<T> Lane<T> {
    fn new() -> Self {
        Self {
            writes: Vec::new(),
            records: Vec::new(),
            bytes: 0,
        }
    }

    /// The lane as a batch of `journal`, emptied.
    fn take(&mut self, journal: JournalIdentifier) -> Batch<T> {
        let batch = Batch {
            journal,
            writes: std::mem::take(&mut self.writes),
            records: std::mem::take(&mut self.records),
        };
        assert_eq!(
            batch.writes.iter().map(|(_, n)| n).sum::<usize>(),
            batch.records.len(),
            "a batch's counts cover its records exactly"
        );
        assert_eq!(batch.bytes(), self.bytes, "a lane counts its bytes");
        self.bytes = 0;
        assert!(self.writes.is_empty() && self.records.is_empty());
        batch
    }
}

/// Pure accumulation of multi-writer writes, one lane per journal (see the
/// module doc). `T` is the caller's token for a write's held reply.
#[derive(Debug)]
pub struct Batcher<T> {
    limits: BatchLimits,
    lanes: BTreeMap<JournalIdentifier, Lane<T>>,
}

impl<T> Batcher<T> {
    /// A batcher under `limits`.
    ///
    /// # Panics
    ///
    /// If `limits` is not [`BatchLimits::valid`] (a programmer error: the
    /// caller's knobs keep their floors).
    #[must_use]
    pub fn new(limits: BatchLimits) -> Self {
        assert!(limits.valid(), "a batcher's limits keep their floors");
        let batcher = Self {
            limits,
            lanes: BTreeMap::new(),
        };
        assert_eq!(batcher.pending(), 0, "a new batcher holds nothing");
        batcher
    }

    /// Its bounds now.
    #[must_use]
    pub fn limits(&self) -> BatchLimits {
        self.limits
    }

    /// How many writes wait in its lanes.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.lanes.values().map(|lane| lane.writes.len()).sum()
    }

    /// Lower its limits to a leader's (`max_records`, `max_bytes`), learned
    /// from a merged write the leader refused as too large. A limit only
    /// goes down, and never below 1; the flush bound follows the record
    /// limit down. A lane over the new limits goes out at its next flush,
    /// whole: the leader refuses it again, and the caller sends each write
    /// alone.
    ///
    /// # Panics
    ///
    /// Never: the asserts check that the limits keep their floors.
    pub fn lower_limits(&mut self, max_records: usize, max_bytes: usize) {
        let before = self.limits;
        self.limits.max_records = self.limits.max_records.min(max_records.max(1));
        self.limits.max_bytes = self.limits.max_bytes.min(max_bytes.max(1));
        self.limits.flush_records = self.limits.flush_records.min(self.limits.max_records);
        assert!(self.limits.valid(), "lowered limits keep their floors");
        assert!(
            self.limits.max_records <= before.max_records
                && self.limits.max_bytes <= before.max_bytes,
            "a limit only goes down"
        );
    }

    /// Whether `records` alone is within the limits.
    fn fits_alone(&self, records: &[Vec<u8>]) -> bool {
        records.len() <= self.limits.max_records && bytes_of(records) <= self.limits.max_bytes
    }

    /// Queue write `token` of `records` for `journal`. Returns the batches
    /// to send now, in order: the journal's lane when this write would pass
    /// the limits beside it, a write that goes alone (empty, or over the
    /// limits by itself), or the lane once it holds
    /// [`BatchLimits::flush_records`] records.
    ///
    /// # Panics
    ///
    /// Never: the asserts check that every lane stays within the limits.
    pub fn push(
        &mut self,
        journal: JournalIdentifier,
        token: T,
        records: Vec<Vec<u8>>,
    ) -> Vec<Batch<T>> {
        let before = self.pending();
        let count = records.len();
        let mut out = Vec::new();
        if records.is_empty() || !self.fits_alone(&records) {
            // Alone, after the writes that came before it on its journal.
            if let Some(lane) = self.lanes.get_mut(&journal)
                && !lane.writes.is_empty()
            {
                out.push(lane.take(journal));
            }
            out.push(Batch {
                journal,
                writes: vec![(token, count)],
                records,
            });
            probe!(reachable, "batcher: a write goes alone");
            assert!(self.pending() <= before, "a lone write queues nothing");
            return out;
        }
        let bytes = bytes_of(&records);
        let limits = self.limits;
        let lane = self.lanes.entry(journal).or_insert_with(Lane::new);
        if lane.records.len() + count > limits.max_records || lane.bytes + bytes > limits.max_bytes
        {
            assert!(
                !lane.writes.is_empty(),
                "a write that fits alone fits an empty lane"
            );
            out.push(lane.take(journal));
        }
        lane.writes.push((token, count));
        lane.records.extend(records);
        lane.bytes += bytes;
        assert!(
            lane.records.len() <= limits.max_records && lane.bytes <= limits.max_bytes,
            "a lane stays within the limits"
        );
        if lane.records.len() >= limits.flush_records {
            out.push(lane.take(journal));
        }
        for batch in &out {
            assert_eq!(batch.journal, journal, "a push flushes only its journal");
            assert!(!batch.writes.is_empty(), "a batch holds a write");
        }
        out
    }

    /// Flush every lane that holds a write, in journal order: the
    /// driver-delivered tick of the flush rule.
    ///
    /// # Panics
    ///
    /// Never: the asserts check that every lane is empty after it.
    pub fn tick(&mut self) -> Vec<Batch<T>> {
        let out: Vec<Batch<T>> = self
            .lanes
            .iter_mut()
            .filter(|(_, lane)| !lane.writes.is_empty())
            .map(|(journal, lane)| lane.take(*journal))
            .collect();
        self.lanes.clear();
        assert_eq!(self.pending(), 0, "a tick flushes every lane");
        for batch in &out {
            assert!(!batch.writes.is_empty(), "a batch holds a write");
        }
        out
    }
}

/// The pure scatter of a merged write's answer (see the module doc).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Unbatcher;

impl Unbatcher {
    /// Each write's `(token, seq, count)` of `writes`, a batch accepted at
    /// `seq` with `count` records: in the batch's order, dense from `seq`.
    ///
    /// # Panics
    ///
    /// If `count` is not the sum of the writes' counts: the leader accepted
    /// another write than the batch (a programmer error).
    #[must_use]
    pub fn scatter<T: Copy>(writes: &[(T, usize)], seq: u64, count: u64) -> Vec<(T, u64, u64)> {
        let total: u64 = writes.iter().map(|(_, n)| *n as u64).sum();
        assert_eq!(total, count, "a merged write is accepted whole");
        let mut at = seq;
        let out: Vec<(T, u64, u64)> = writes
            .iter()
            .map(|(token, n)| {
                let n = *n as u64;
                let range = (*token, at, n);
                at += n;
                range
            })
            .collect();
        assert_eq!(at, seq + count, "the ranges cover the merged write");
        assert_eq!(out.len(), writes.len(), "one answer per write");
        if out.len() > 1 {
            probe!(reachable, "unbatcher: a merged write is scattered");
        }
        out
    }
}

#[cfg(test)]
mod tests;
