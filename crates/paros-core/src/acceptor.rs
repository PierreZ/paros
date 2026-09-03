//! The **acceptor**: one node's Paxos voting state, and nothing else.
//!
//! An acceptor knows its durable promise, the per-slot records it accepted,
//! the compaction floor below which those records are gone, and the CTRL
//! tri-state's third answer — the slots whose record it *had* but can no
//! longer read (`faulty`: identity known, value lost). It answers two
//! questions, both pure Paxos:
//!
//! - `Prepare(from_slot, ballot)`: promise, or refuse with the promise held
//!   ([`Acceptor::prepare`]), and page out the accepted suffix a promise
//!   reports ([`Acceptor::promise_page`]).
//! - `Accept(slot, ballot, value)`: admissible at this promise, or refused
//!   ([`Acceptor::admit`]); the record itself lands through
//!   [`Acceptor::record_accepted`].
//!
//! It is generic over the value its log carries — to an acceptor a value has
//! identity and nothing else — so the one-slot decree of the matchmaker-set
//! handover and the unbounded Multi-Paxos log are the same role over
//! different `V`. Its two durable ops are named by [`AcceptorWrite`]; the
//! caller's batch type only has to say where they sit (`W: From<_>`). The
//! two *retention* ops, [`WriteOp::Truncate`] and [`WriteOp::InstallSnapshot`],
//! still speak the node batch's own language: a log that compacts is the
//! multi-slot deployment's concern, and giving retention its own role is a
//! later phase.
//!
//! It knows nothing about leadership, elections, replicas, matchmakers, the
//! network, timers, randomness, or *why* a `Prepare` arrived — the
//! [`crate::ColocatedNode`] wiring owns those couplings (a `Prepare` that deposes a
//! leader, a heartbeat that adopts a sender) and builds the wire messages.
//! Every durable change it makes is emitted as a [`WriteOp`] into the batch
//! the caller hands it, so the persist-before-send ordering stays the
//! caller's structural contract — and every op in a batch that needs an
//! fsync comes from here ([`crate::WriteOp::needs_sync`]): a second
//! deployment reusing this role gets the whole durable surface with it, and
//! cannot silently lose a write by forgetting to push one beside the call.
//!
//! Hard `assert!`s throughout: a broken voting invariant is a programmer
//! error, never an operating condition (AGENTS.md, *Assertion doctrine*).

use std::collections::BTreeMap;

use crate::types::{Ballot, SessionEntry, Slot, Value};
use crate::write::{AcceptorWrite, WriteOp};

/// Maximum accepted records and faulty entries carried by one promise page —
/// the bound this role enforces in [`Acceptor::promise_page`], and the reason
/// a `Promise` carries a continuation cursor.
pub const PROMISE_BATCH: usize = 64;

/// What a `Prepare` did at this acceptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrepareOutcome {
    /// The requested range starts below the compaction floor: those slots are
    /// chosen and truncated, so no promise could report them — refused
    /// *without* moving the promise (a blind laggard never ratchets it).
    BelowFloor,
    /// The promise held already dominates the ballot: refused, promise
    /// untouched.
    Refused,
    /// Promised. `raised` says whether the promise moved (a same-ballot
    /// continuation page re-affirms it without a write).
    Promised {
        /// Whether this prepare raised the durable promise.
        raised: bool,
    },
}

/// Whether an `Accept` may land at this acceptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// The slot is below the compaction floor: already chosen, ignore.
    BelowFloor,
    /// The promise held dominates the ballot: refused.
    Refused,
    /// Admissible: the caller raises the promise and records the value.
    Admitted,
}

/// One page of a `Promise`: the readable records and the faulty entries at
/// or after the requested slot, bounded, disjoint, and the continuation
/// cursor when the suffix did not fit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromisePage<V> {
    /// Readable records, by slot.
    pub accepted: BTreeMap<Slot, (Ballot, V)>,
    /// Faulty entries (identity known, value lost), by slot.
    pub faulty: BTreeMap<Slot, Ballot>,
    /// Where the next page starts, when this one was full.
    pub next_from_slot: Option<Slot>,
}

/// The acceptor: promise, records, floor, tri-state, over the value `V` its
/// log carries. See the module doc.
#[derive(Clone, Debug)]
pub struct Acceptor<V> {
    /// The highest ballot promised. Monotone for the node's whole lifetime —
    /// the durable safety hinge.
    promised: Ballot,
    /// The working per-slot accepted log (rebuilt from durable storage on
    /// boot): the highest-ballot record per slot, or the chosen value once
    /// learned.
    records: BTreeMap<Slot, (Ballot, V)>,
    /// The compaction floor: the first slot still retained.
    first_slot: Slot,
    /// Slots this acceptor accepted but can no longer read, at the ballot
    /// the lost record carried — the tri-state's `faulty` answer.
    faulty: BTreeMap<Slot, Ballot>,
    /// Faulty entries healed in place by a fresh record, this incarnation.
    faulty_repaired: u64,
}

impl<V: Clone + PartialEq> Acceptor<V> {
    /// An acceptor over the durable state a boot scan read back.
    ///
    /// # Panics
    ///
    /// If the state breaks the acceptor's invariants: a record or faulty
    /// entry below the floor, a slot both readable and faulty, or a record
    /// above the promise (the write side always flushes the promise ahead of
    /// the record it covers).
    #[must_use]
    pub fn new(
        promised: Ballot,
        records: BTreeMap<Slot, (Ballot, V)>,
        first_slot: Slot,
        faulty: BTreeMap<Slot, Ballot>,
    ) -> Self {
        let acceptor = Self {
            promised,
            records,
            first_slot,
            faulty,
            faulty_repaired: 0,
        };
        acceptor.assert_invariants();
        acceptor
    }

    /// The acceptor's own cross-field invariants: min-key probes against the
    /// floor, and bounded structural scans over the retained log and the
    /// faulty set (always-on by choice — the maps are small and crash beats
    /// corruption).
    ///
    /// # Panics
    ///
    /// If a record or faulty entry sits below the floor, a slot is both
    /// readable and faulty, or a record or faulty entry stands above the
    /// promise.
    pub fn assert_invariants(&self) {
        assert!(
            self.records
                .keys()
                .next()
                .is_none_or(|s| *s >= self.first_slot),
            "no accepted record survives below the compaction floor"
        );
        assert!(
            self.faulty
                .keys()
                .next()
                .is_none_or(|s| *s >= self.first_slot),
            "no faulty entry survives below the compaction floor"
        );
        // The tri-state is a partition: a slot is readable, faulty, or absent —
        // never two at once.
        assert!(
            self.faulty.keys().all(|s| !self.records.contains_key(s)),
            "the faulty set stays disjoint from the accepted log"
        );
        // The write-side ordering, read back: a record is admitted only at or
        // below the promise, and the promise is flushed ahead of the record it
        // covers — so nothing this acceptor holds may stand above it. The
        // faulty half says the same about a record whose value was lost: its
        // identity survived, and so did the promise that covered it.
        assert!(
            self.records
                .values()
                .all(|(ballot, _)| *ballot <= self.promised),
            "the promise dominates every accepted record"
        );
        assert!(
            self.faulty.values().all(|ballot| *ballot <= self.promised),
            "the promise dominates every faulty record"
        );
    }

    // ---- reads --------------------------------------------------------------

    /// The highest ballot promised.
    #[must_use]
    pub fn promised(&self) -> Ballot {
        self.promised
    }

    /// The working accepted log.
    #[must_use]
    pub fn records(&self) -> &BTreeMap<Slot, (Ballot, V)> {
        &self.records
    }

    /// The record at `slot`, if readable.
    #[must_use]
    pub fn record(&self, slot: Slot) -> Option<&(Ballot, V)> {
        self.records.get(&slot)
    }

    /// The compaction floor: the first slot still retained.
    #[must_use]
    pub fn first_slot(&self) -> Slot {
        self.first_slot
    }

    /// The faulty entries: identity known, value lost.
    #[must_use]
    pub fn faulty(&self) -> &BTreeMap<Slot, Ballot> {
        &self.faulty
    }

    /// The lowest faulty slot, if any.
    #[must_use]
    pub fn first_faulty(&self) -> Option<Slot> {
        self.faulty.keys().next().copied()
    }

    /// Faulty entries repaired in place this incarnation — half of the CTRL
    /// §5.2 metric. The other half, the payload bytes those repairs shipped,
    /// is the caller's to tally: it needs the *meaning* of a value, which an
    /// acceptor deliberately does not have (see the module doc's opacity
    /// rule), and [`Acceptor::record_accepted`] reports each repair as it
    /// happens.
    #[must_use]
    pub fn faulty_repaired(&self) -> u64 {
        self.faulty_repaired
    }

    // ---- Phase 1 ------------------------------------------------------------

    /// A candidate prepares `ballot` for every slot at or after `from_slot`.
    /// Promotes the promise when `ballot` is strictly higher (emitting the
    /// [`AcceptorWrite::SetPromise`]), re-affirms it for a same-ballot page
    /// continuation, and refuses below it — or below the floor, without
    /// touching the promise.
    ///
    /// # Panics
    ///
    /// If the promise does not land exactly on the prepared ballot (a
    /// programmer error).
    pub fn prepare<W: From<AcceptorWrite<V>>>(
        &mut self,
        ballot: Ballot,
        from_slot: Slot,
        writes: &mut Vec<W>,
    ) -> PrepareOutcome {
        if from_slot < self.first_slot {
            return PrepareOutcome::BelowFloor;
        }
        if ballot < self.promised {
            return PrepareOutcome::Refused;
        }
        let raised = ballot > self.promised;
        if raised {
            self.set_promise(ballot, writes);
        }
        // Postcondition: the promise sits exactly at the prepared ballot.
        assert!(
            self.promised == ballot,
            "a promise reply carries the exact promised ballot"
        );
        PrepareOutcome::Promised { raised }
    }

    /// One bounded page over the slot-ordered union of readable records
    /// (`have`) and faulty entries (the tri-state's third answer): a rotted
    /// copy is reported as `faulty(ballot)` — silence toward the none-tally,
    /// never "nothing accepted here".
    ///
    /// # Panics
    ///
    /// Never in practice: the peeked cursors are advanced only after a
    /// successful peek.
    #[must_use]
    pub fn promise_page(&self, from_slot: Slot) -> PromisePage<V> {
        let mut readable = self.records.range(from_slot..).peekable();
        let mut rotted = self.faulty.range(from_slot..).peekable();
        let mut page = PromisePage {
            accepted: BTreeMap::new(),
            faulty: BTreeMap::new(),
            next_from_slot: None,
        };
        while page.accepted.len() + page.faulty.len() < PROMISE_BATCH {
            let take_readable = match (readable.peek(), rotted.peek()) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((ra, _)), Some((rf, _))) => ra < rf,
            };
            if take_readable {
                let (slot, record) = readable.next().expect("peeked");
                page.accepted.insert(*slot, record.clone());
            } else {
                let (slot, fb) = rotted.next().expect("peeked");
                page.faulty.insert(*slot, *fb);
            }
        }
        page.next_from_slot = match (readable.peek(), rotted.peek()) {
            (None, None) => None,
            (Some((slot, _)), None) | (None, Some((slot, _))) => Some(**slot),
            (Some((ra, _)), Some((rf, _))) => Some(*std::cmp::min(*ra, *rf)),
        };
        page
    }

    // ---- Phase 2 ------------------------------------------------------------

    /// Whether an `Accept` at `ballot` for `slot` may land here: not below the
    /// floor (already chosen — ignore, never refuse), and at or above the
    /// promise.
    #[must_use]
    pub fn admit(&self, ballot: Ballot, slot: Slot) -> AcceptOutcome {
        if slot < self.first_slot {
            return AcceptOutcome::BelowFloor;
        }
        if ballot < self.promised {
            return AcceptOutcome::Refused;
        }
        AcceptOutcome::Admitted
    }

    // ---- durable writes -----------------------------------------------------

    /// Raise (or re-affirm) the promised ballot to `ballot`, emitting a
    /// [`AcceptorWrite::SetPromise`] only when it actually changes.
    ///
    /// # Panics
    ///
    /// If `ballot` is below the promise held: a promise is never lowered,
    /// across the node's whole lifetime.
    pub fn set_promise<W: From<AcceptorWrite<V>>>(&mut self, ballot: Ballot, writes: &mut Vec<W>) {
        assert!(
            ballot >= self.promised,
            "a node's promised ballot never decreases"
        );
        if self.promised != ballot {
            self.promised = ballot;
            writes.push(AcceptorWrite::SetPromise(ballot).into());
        }
    }

    /// Record `(ballot, command)` as accepted for `slot` and emit the matching
    /// [`AcceptorWrite::AppendAccepted`]. An upsert-by-slot: a higher-ballot
    /// re-accept, or a chosen value overwriting a stale accept. A fresh record
    /// over a faulty entry is the in-place repair (fill or
    /// replace-with-proven-identical, never delete).
    ///
    /// Returns whether this record *was* such an in-place repair, so a caller
    /// that understands what a value is can attribute the repair's cost to it
    /// (the acceptor cannot: to it a value is opaque).
    ///
    /// # Panics
    ///
    /// If `slot` is below the floor, `ballot` is above the promise (the write
    /// side always raises the promise first), or an accept at or below the
    /// recorded ballot carries a different command — the acceptor-side
    /// agreement rule: a record is replaced either by a *higher* ballot, or
    /// at-or-below the recorded ballot only by the *chosen* value, which P2c
    /// makes identical to whatever was accepted here at any ballot at or
    /// above the choosing one; and one ballot has one proposer (P2b).
    pub fn record_accepted<W: From<AcceptorWrite<V>>>(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: V,
        writes: &mut Vec<W>,
    ) -> bool {
        assert!(
            slot >= self.first_slot,
            "never record an accept below the compaction floor"
        );
        assert!(
            ballot <= self.promised,
            "a record is never accepted above the promise"
        );
        let repaired = self.faulty.remove(&slot).is_some();
        if repaired {
            self.faulty_repaired += 1;
        }
        if let Some((recorded_ballot, recorded)) = self.records.get(&slot)
            && ballot <= *recorded_ballot
        {
            assert!(
                *recorded == command,
                "an accept at or below the recorded ballot carries the recorded command"
            );
        }
        self.records.insert(slot, (ballot, command.clone()));
        writes.push(
            AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value: command,
            }
            .into(),
        );
        repaired
    }

    /// Drop every record and faulty entry below `first`, raise the floor to
    /// it, and emit the durable [`WriteOp::Truncate`] carrying `sealed` (the
    /// at-most-once ledger records whose slots the drop removes). A decided
    /// truncation: the caller has already established that the prefix is
    /// chosen and applied.
    ///
    /// # Panics
    ///
    /// If `first` is below the floor held.
    pub fn truncate(&mut self, first: Slot, sealed: Vec<SessionEntry>, writes: &mut Vec<WriteOp>) {
        self.drop_prefix(first);
        writes.push(WriteOp::Truncate { first, sealed });
    }

    /// Fold the prefix an installed snapshot covers: drop every record and
    /// faulty entry at or below `chosen_index` (their decided effects live in
    /// the opaque bytes now), raise the floor one past it, and emit the
    /// durable [`WriteOp::InstallSnapshot`]. Returns the new floor.
    ///
    /// The caller adopts the snapshot's ballot through [`Self::set_promise`]
    /// *before* this call (the promise never regresses) and owns everything
    /// outside the acceptor — the replica's prefix jump, the proposer's
    /// blocked work. What the acceptor owns is the floor and the write.
    ///
    /// # Panics
    ///
    /// If the resulting floor is below the floor held, or `chosen_index` is
    /// the numeric ceiling (the caller's wire guard refuses one).
    pub fn install(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        snapshot: Value,
        sessions: Vec<SessionEntry>,
        writes: &mut Vec<WriteOp>,
    ) -> Slot {
        assert!(
            chosen_index.0 < u64::MAX,
            "a snapshot boundary has a floor one past it"
        );
        let first = Slot(chosen_index.0 + 1);
        self.drop_prefix(first);
        writes.push(WriteOp::InstallSnapshot {
            chosen_index,
            ballot,
            snapshot,
            sessions,
        });
        first
    }

    /// Drop every record and faulty entry below `first` and raise the floor
    /// to it. The floor never moves backward. The two callers differ only in
    /// the durable op they emit beside it.
    fn drop_prefix(&mut self, first: Slot) {
        assert!(
            first >= self.first_slot,
            "the compaction floor never moves backward"
        );
        self.records = self.records.split_off(&first);
        self.faulty = self.faulty.split_off(&first);
        self.first_slot = first;
        self.assert_invariants();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ClientId, ClientSeq, Command, Entry, NodeId};

    fn ballot(round: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(0),
        }
    }

    fn command(byte: u8) -> Command {
        Command::User(Entry {
            client: ClientId(1),
            seq: ClientSeq(u64::from(byte)),
            value: Value(vec![byte]),
        })
    }

    /// The role classification `write.rs` states: every durable change an
    /// acceptor makes is emitted by the acceptor itself, and every op it
    /// emits needs an fsync. `truncate` and `install` used to change the
    /// floor and emit nothing, leaving the wiring to remember the write.
    #[test]
    fn every_acceptor_mutation_emits_its_own_fsynced_write() {
        let mut acceptor = Acceptor::new(Ballot::zero(), BTreeMap::new(), Slot(0), BTreeMap::new());
        let mut writes = Vec::new();
        acceptor.set_promise(ballot(1), &mut writes);
        acceptor.record_accepted(Slot(0), ballot(1), command(0), &mut writes);
        acceptor.record_accepted(Slot(1), ballot(1), command(1), &mut writes);
        acceptor.truncate(Slot(1), Vec::new(), &mut writes);
        assert_eq!(acceptor.first_slot(), Slot(1));
        assert!(
            matches!(writes.last(), Some(WriteOp::Truncate { first, .. }) if *first == Slot(1)),
            "the truncation is durable"
        );
        let first = acceptor.install(Slot(4), ballot(2), Value(vec![9]), Vec::new(), &mut writes);
        assert_eq!(first, Slot(5));
        assert_eq!(acceptor.first_slot(), Slot(5));
        assert!(acceptor.records().is_empty(), "the folded prefix is gone");
        assert!(
            matches!(
                writes.last(),
                Some(WriteOp::InstallSnapshot { chosen_index, .. }) if *chosen_index == Slot(4)
            ),
            "the install is durable"
        );
        assert!(
            writes.iter().all(WriteOp::needs_sync),
            "every write an acceptor emits is safety-critical"
        );
    }
}
