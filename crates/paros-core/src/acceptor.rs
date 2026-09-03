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
//! It knows nothing about leadership, elections, replicas, matchmakers, the
//! network, timers, randomness, or *why* a `Prepare` arrived — the
//! [`crate::RawNode`] wiring owns those couplings (a `Prepare` that deposes a
//! leader, a heartbeat that adopts a sender) and builds the wire messages.
//! Every durable change it makes is emitted as a [`WriteOp`] into the batch
//! the caller hands it, so the persist-before-send ordering stays the
//! caller's structural contract.
//!
//! Hard `assert!`s throughout: a broken voting invariant is a programmer
//! error, never an operating condition (AGENTS.md, *Assertion doctrine*).

use std::collections::BTreeMap;

use crate::node::PROMISE_BATCH;
use crate::types::{Ballot, Command, Control, Slot};
use crate::write::WriteOp;

/// Payload bytes a repaired command shipped (the CTRL §5.2 repair-cost metric:
/// a protocol-aware repair moves one entry, not the log).
fn command_payload_bytes(command: &Command) -> u64 {
    match command {
        Command::User(entry) => entry.value.0.len() as u64,
        Command::Control(Control::Truncate { .. } | Control::Snap { .. }) => 8,
        Command::Control(Control::Noop) => 1,
    }
}

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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PromisePage {
    /// Readable records, by slot.
    pub accepted: BTreeMap<Slot, (Ballot, Command)>,
    /// Faulty entries (identity known, value lost), by slot.
    pub faulty: BTreeMap<Slot, Ballot>,
    /// Where the next page starts, when this one was full.
    pub next_from_slot: Option<Slot>,
}

/// The acceptor: promise, records, floor, tri-state. See the module doc.
#[derive(Clone, Debug)]
pub struct Acceptor {
    /// The highest ballot promised. Monotone for the node's whole lifetime —
    /// the durable safety hinge.
    promised: Ballot,
    /// The working per-slot accepted log (rebuilt from durable storage on
    /// boot): the highest-ballot record per slot, or the chosen value once
    /// learned.
    records: BTreeMap<Slot, (Ballot, Command)>,
    /// The compaction floor: the first slot still retained.
    first_slot: Slot,
    /// Slots this acceptor accepted but can no longer read, at the ballot
    /// the lost record carried — the tri-state's `faulty` answer.
    faulty: BTreeMap<Slot, Ballot>,
    /// Faulty entries healed in place by a fresh record, this incarnation.
    faulty_repaired: u64,
    /// Payload bytes those repairs shipped.
    repair_bytes: u64,
}

impl Acceptor {
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
        records: BTreeMap<Slot, (Ballot, Command)>,
        first_slot: Slot,
        faulty: BTreeMap<Slot, Ballot>,
    ) -> Self {
        let acceptor = Self {
            promised,
            records,
            first_slot,
            faulty,
            faulty_repaired: 0,
            repair_bytes: 0,
        };
        acceptor.assert_invariants();
        acceptor
    }

    /// The acceptor's own cross-field invariants (O(N) over the faulty set,
    /// always-on by choice — crash beats corruption).
    ///
    /// # Panics
    ///
    /// If a record or faulty entry sits below the floor, or a slot is both
    /// readable and faulty.
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
    }

    // ---- reads --------------------------------------------------------------

    /// The highest ballot promised.
    #[must_use]
    pub fn promised(&self) -> Ballot {
        self.promised
    }

    /// The working accepted log.
    #[must_use]
    pub fn records(&self) -> &BTreeMap<Slot, (Ballot, Command)> {
        &self.records
    }

    /// The record at `slot`, if readable.
    #[must_use]
    pub fn record(&self, slot: Slot) -> Option<&(Ballot, Command)> {
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

    /// One past the highest slot this acceptor holds a record or a faulty
    /// entry for — the allocator's lower bound.
    #[must_use]
    pub fn highest_slot(&self) -> Option<Slot> {
        self.records.keys().chain(self.faulty.keys()).max().copied()
    }

    /// `(faulty entries repaired in place, payload bytes those repairs
    /// shipped)` this incarnation — the CTRL §5.2 metric.
    #[must_use]
    pub fn repair_counters(&self) -> (u64, u64) {
        (self.faulty_repaired, self.repair_bytes)
    }

    // ---- Phase 1 ------------------------------------------------------------

    /// A candidate prepares `ballot` for every slot at or after `from_slot`.
    /// Promotes the promise when `ballot` is strictly higher (emitting the
    /// [`WriteOp::SetPromise`]), re-affirms it for a same-ballot page
    /// continuation, and refuses below it — or below the floor, without
    /// touching the promise.
    ///
    /// # Panics
    ///
    /// If the promise does not land exactly on the prepared ballot (a
    /// programmer error).
    pub fn prepare(
        &mut self,
        ballot: Ballot,
        from_slot: Slot,
        writes: &mut Vec<WriteOp>,
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
    pub fn promise_page(&self, from_slot: Slot) -> PromisePage {
        let mut readable = self.records.range(from_slot..).peekable();
        let mut rotted = self.faulty.range(from_slot..).peekable();
        let mut page = PromisePage::default();
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
    /// [`WriteOp::SetPromise`] only when it actually changes.
    ///
    /// # Panics
    ///
    /// If `ballot` is below the promise held: a promise is never lowered,
    /// across the node's whole lifetime.
    pub fn set_promise(&mut self, ballot: Ballot, writes: &mut Vec<WriteOp>) {
        assert!(
            ballot >= self.promised,
            "a node's promised ballot never decreases"
        );
        if self.promised != ballot {
            self.promised = ballot;
            writes.push(WriteOp::SetPromise(ballot));
        }
    }

    /// Record `(ballot, command)` as accepted for `slot` and emit the matching
    /// [`WriteOp::AppendAccepted`]. An upsert-by-slot: a higher-ballot
    /// re-accept, or a chosen value overwriting a stale accept. A fresh record
    /// over a faulty entry is the in-place repair (fill or
    /// replace-with-proven-identical, never delete).
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
    pub fn record_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
        writes: &mut Vec<WriteOp>,
    ) {
        assert!(
            slot >= self.first_slot,
            "never record an accept below the compaction floor"
        );
        assert!(
            ballot <= self.promised,
            "a record is never accepted above the promise"
        );
        if self.faulty.remove(&slot).is_some() {
            self.faulty_repaired += 1;
            self.repair_bytes += command_payload_bytes(&command);
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
        writes.push(WriteOp::AppendAccepted {
            slot,
            ballot,
            command,
        });
    }

    /// Drop every record and faulty entry below `first` and raise the floor
    /// to it (a decided truncation, or a snapshot install folding the prefix).
    /// The floor never moves backward.
    ///
    /// # Panics
    ///
    /// If `first` is below the floor held.
    pub fn truncate(&mut self, first: Slot) {
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
