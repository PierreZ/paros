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
//! two *retention* ops, [`crate::WriteOp::Truncate`] and [`crate::WriteOp::TrimmedTo`],
//! live in their own module (`acceptor/retention.rs`) and still speak the
//! node batch's own language: a log that compacts is the multi-slot
//! deployment's concern, and they stay methods of this role because the
//! role that moves the floor emits the write that makes it durable (see
//! that module's doc for why a separate retention *type* was not the
//! answer).
//!
//! It knows nothing about leadership, elections, replicas, matchmakers, the
//! network, timers, randomness, or *why* a `Prepare` arrived — the
//! [`crate::ColocatedNode`] wiring owns those couplings (a `Prepare` that deposes a
//! leader, a heartbeat that adopts a sender) and builds the wire messages.
//! Every durable change it makes is emitted as a [`crate::WriteOp`] into the batch
//! the caller hands it, so the persist-before-send ordering stays the
//! caller's structural contract — and every op in a batch that needs an
//! fsync comes from here ([`crate::WriteOp::needs_sync`]): a second
//! deployment reusing this role gets the whole durable surface with it, and
//! cannot silently lose a write by forgetting to push one beside the call.
//!
//! Hard `assert!`s throughout: a broken voting invariant is a programmer
//! error, never an operating condition (AGENTS.md, *Assertion doctrine*).

mod retention;

use std::collections::BTreeMap;

use crate::retained::RetainedWindow;
use crate::types::{Ballot, Slot};
use crate::write::AcceptorWrite;

/// Maximum accepted records and faulty entries carried by one promise page —
/// the bound this role enforces in [`Acceptor::promise_page`], and the reason
/// a `Promise` carries a continuation cursor.
pub const PROMISE_BATCH: usize = 64;

// A page that cannot carry one entry could never make progress through a
// faulty or accepted suffix: the continuation cursor would never advance.
const _: () = assert!(PROMISE_BATCH > 0);

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
    /// learned — retained above the compaction floor.
    records: RetainedWindow<Slot, (Ballot, V)>,
    /// Slots this acceptor accepted but can no longer read, at the ballot
    /// the lost record carried — the tri-state's `faulty` answer. Retained
    /// above the same floor, which the two windows keep in step (asserted).
    faulty: RetainedWindow<Slot, Ballot>,
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
        // The windows enforce their own floor, so these say the same thing
        // in the *acceptor's* words, at the boot seam where a corrupt store
        // is what breaks it.
        assert!(
            records.keys().next().is_none_or(|s| *s >= first_slot),
            "no accepted record survives below the compaction floor"
        );
        assert!(
            faulty.keys().next().is_none_or(|s| *s >= first_slot),
            "no faulty entry survives below the compaction floor"
        );
        let acceptor = Self {
            promised,
            records: RetainedWindow::new(records, first_slot),
            faulty: RetainedWindow::new(faulty, first_slot),
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
                .first_key()
                .is_none_or(|s| s >= self.first_slot()),
            "no accepted record survives below the compaction floor"
        );
        assert!(
            self.faulty
                .first_key()
                .is_none_or(|s| s >= self.first_slot()),
            "no faulty entry survives below the compaction floor"
        );
        assert!(
            self.records.floor() == self.faulty.floor(),
            "the tri-state's two windows share one compaction floor"
        );
        // The tri-state is a partition: a slot is readable, faulty, or absent —
        // never two at once.
        assert!(
            self.faulty
                .entries()
                .keys()
                .all(|s| !self.records.contains_key(*s)),
            "the faulty set stays disjoint from the accepted log"
        );
        // The write-side ordering, read back: a record is admitted only at or
        // below the promise, and the promise is flushed ahead of the record it
        // covers — so nothing this acceptor holds may stand above it. The
        // faulty half says the same about a record whose value was lost: its
        // identity survived, and so did the promise that covered it.
        assert!(
            self.records
                .entries()
                .values()
                .all(|(ballot, _)| *ballot <= self.promised),
            "the promise dominates every accepted record"
        );
        assert!(
            self.faulty
                .entries()
                .values()
                .all(|ballot| *ballot <= self.promised),
            "the promise dominates every faulty record"
        );
    }

    // ---- reads --------------------------------------------------------------

    /// The highest ballot promised.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn promised(&self) -> Ballot {
        // The read side of the write ordering: the last record never stands
        // above the promise that covered it.
        assert!(
            self.records
                .last_key()
                .and_then(|s| self.records.get(s))
                .is_none_or(|(b, _)| *b <= self.promised),
            "the promise dominates the last record"
        );
        self.promised
    }

    /// The working accepted log.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn records(&self) -> &BTreeMap<Slot, (Ballot, V)> {
        let records = self.records.entries();
        assert!(
            records
                .keys()
                .next()
                .is_none_or(|s| *s >= self.first_slot()),
            "no accepted record survives below the compaction floor"
        );
        records
    }

    /// The record at `slot`, if readable.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn record(&self, slot: Slot) -> Option<&(Ballot, V)> {
        let record = self.records.get(slot);
        if let Some((ballot, _)) = record {
            // Read-back of `record_accepted`'s preconditions: a readable
            // record sits at or above the floor and under the promise.
            assert!(
                !self.records.below_floor(slot),
                "a readable record sits above the floor"
            );
            assert!(
                *ballot <= self.promised,
                "the promise dominates a readable record"
            );
            assert!(
                !self.faulty.contains_key(slot),
                "a readable slot is never faulty"
            );
        }
        record
    }

    /// The compaction floor: the first slot still retained.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn first_slot(&self) -> Slot {
        let floor = self.records.floor();
        assert!(
            floor == self.faulty.floor(),
            "the tri-state's two windows share one floor"
        );
        floor
    }

    /// The faulty entries: identity known, value lost.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn faulty(&self) -> &BTreeMap<Slot, Ballot> {
        let faulty = self.faulty.entries();
        assert!(
            faulty.keys().next().is_none_or(|s| *s >= self.first_slot()),
            "no faulty entry survives below the compaction floor"
        );
        faulty
    }

    /// The lowest faulty slot, if any.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn first_faulty(&self) -> Option<Slot> {
        let first = self.faulty.first_key();
        if let Some(slot) = first {
            assert!(
                slot >= self.first_slot(),
                "a faulty entry sits above the floor"
            );
            assert!(
                !self.records.contains_key(slot),
                "a faulty slot is never readable"
            );
        }
        first
    }

    /// The **vote watermark** (#143, Compartmentalized Paxos §3.4): the
    /// highest slot this acceptor has voted in — the last retained record,
    /// the last faulty entry (identity known, value lost: it *was* voted),
    /// or `first_slot - 1` when the retained log is empty above a compaction
    /// floor (every truncated slot was chosen, hence voted). `None` only on a
    /// log that never voted anything.
    ///
    /// paros records a *chosen* value as the authoritative accepted record
    /// (`mark_chosen` → [`Acceptor::record_accepted`]), so a learner-only
    /// record raises the watermark too. That is **conservative, never
    /// unsafe**: a quorum read waits until the replica has applied the
    /// watermark, so a watermark that is too high costs latency, and one
    /// that is too low would be the bug — it cannot be, because every vote
    /// this acceptor cast is either retained, faulty, or below the floor.
    /// Monotone across [`Acceptor::record_accepted`] and
    /// [`Acceptor::truncate`] (asserted at both).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn vote_watermark(&self) -> Option<Slot> {
        let truncated = self.first_slot().0.checked_sub(1).map(Slot);
        let watermark = [self.records.last_key(), self.faulty.last_key(), truncated]
            .into_iter()
            .flatten()
            .max();
        // Postconditions: the watermark covers every vote this acceptor still
        // holds, and every vote the floor stands in for.
        assert!(
            watermark >= self.records.last_key(),
            "the vote watermark covers every retained record"
        );
        assert!(
            watermark >= self.faulty.last_key(),
            "the vote watermark covers every faulty entry"
        );
        assert!(
            watermark >= truncated,
            "the vote watermark covers every truncated slot"
        );
        watermark
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
        let writes_before = writes.len();
        if self.records.below_floor(from_slot) {
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
        // Negative space: a raise emits exactly the one durable promise, a
        // same-ballot continuation emits nothing.
        if raised {
            assert!(
                writes.len() == writes_before + 1,
                "a raised promise emits one write"
            );
        } else {
            assert!(
                writes.len() == writes_before,
                "a re-affirmed promise emits no write"
            );
        }
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
        // Postconditions: bounded, disjoint, at or after the requested slot,
        // and the cursor strictly past everything this page carried.
        assert!(
            page.accepted.len() + page.faulty.len() <= PROMISE_BATCH,
            "a promise page is bounded by PROMISE_BATCH"
        );
        assert!(
            page.faulty.keys().all(|s| !page.accepted.contains_key(s)),
            "a promise page reports a slot as readable or faulty, never both"
        );
        assert!(
            page.accepted
                .keys()
                .chain(page.faulty.keys())
                .all(|s| *s >= from_slot),
            "a promise page starts at the requested slot"
        );
        if let Some(next) = page.next_from_slot {
            assert!(
                page.accepted
                    .keys()
                    .chain(page.faulty.keys())
                    .all(|s| *s < next),
                "the continuation cursor lies past every slot the page carried"
            );
            assert!(
                page.accepted.len() + page.faulty.len() == PROMISE_BATCH,
                "only a full page carries a continuation cursor"
            );
        }
        page
    }

    // ---- Phase 2 ------------------------------------------------------------

    /// Whether an `Accept` at `ballot` for `slot` may land here: not below the
    /// floor (already chosen — ignore, never refuse), and at or above the
    /// promise.
    #[must_use]
    pub fn admit(&self, ballot: Ballot, slot: Slot) -> AcceptOutcome {
        if self.records.below_floor(slot) {
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
        let writes_before = writes.len();
        if self.promised != ballot {
            self.promised = ballot;
            writes.push(AcceptorWrite::SetPromise(ballot).into());
        }
        assert!(
            self.promised == ballot,
            "the promise lands on the requested ballot"
        );
        assert!(
            writes.len() <= writes_before + 1,
            "a promise change emits at most one write"
        );
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
            !self.records.below_floor(slot),
            "never record an accept below the compaction floor"
        );
        assert!(
            ballot <= self.promised,
            "a record is never accepted above the promise"
        );
        let watermark_before = self.vote_watermark();
        let writes_before = writes.len();
        let repaired = self.faulty.remove(slot).is_some();
        if repaired {
            self.faulty_repaired += 1;
        }
        if let Some((recorded_ballot, recorded)) = self.records.get(slot)
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
        // Postcondition: a vote only ever raises the watermark (a quorum read
        // that captured the old one stays sound).
        assert!(
            self.vote_watermark() >= watermark_before,
            "the vote watermark never decreases across a record"
        );
        // The record and its durable op are one: exactly one write, the slot
        // readable at the ballot just recorded, and no longer faulty.
        assert!(
            writes.len() == writes_before + 1,
            "a record emits exactly one write"
        );
        assert!(
            self.records.get(slot).is_some_and(|(b, _)| *b == ballot),
            "the record lands at the recorded ballot"
        );
        assert!(
            !self.faulty.contains_key(slot),
            "a recorded slot is never faulty"
        );
        repaired
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_state::JournalState;
    use crate::types::{ClientId, Command, Entry, Generation, NodeId, Seq, Value};
    use crate::write::WriteOp;

    fn ballot(round: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(0),
        }
    }

    fn command(byte: u8) -> Command {
        Command::Write(Entry {
            generation: Generation(0),
            owner: ClientId(1),
            seq: Seq(u64::from(byte)),
            records: vec![Value(vec![byte])],
        })
    }

    /// One ballot carries one value (P2b): a second value at the ballot a
    /// slot already recorded is a programmer error, not a tie. The proposer
    /// half of the same rule is
    /// `node::tests::invariants::conflicting_equal_ballot_promise_reports_trip_the_merge`;
    /// this is the half a single-decree deployment leans on, where a silent
    /// overwrite would let two successor sets be chosen for one generation.
    #[test]
    #[should_panic(expected = "an accept at or below the recorded ballot carries the recorded")]
    fn two_values_at_one_ballot_are_a_programmer_error() {
        let mut acceptor: Acceptor<Command> =
            Acceptor::new(ballot(1), BTreeMap::new(), Slot(0), BTreeMap::new());
        let mut writes: Vec<WriteOp> = Vec::new();
        acceptor.record_accepted(Slot(0), ballot(1), command(1), &mut writes);
        acceptor.record_accepted(Slot(0), ballot(1), command(2), &mut writes);
    }

    /// The vote watermark (#143) is the highest slot voted, and it survives
    /// a truncation: the truncated prefix was voted, so the floor stands in
    /// for it. A faulty entry counts (it was voted), and so does a record
    /// learned rather than accepted (conservative, never unsafe).
    #[test]
    fn the_vote_watermark_is_the_highest_slot_voted_and_survives_truncation() {
        let mut acceptor: Acceptor<Command> =
            Acceptor::new(Ballot::zero(), BTreeMap::new(), Slot(0), BTreeMap::new());
        assert_eq!(acceptor.vote_watermark(), None, "nothing voted yet");
        let mut writes: Vec<WriteOp> = Vec::new();
        acceptor.set_promise(ballot(1), &mut writes);
        acceptor.record_accepted(Slot(0), ballot(1), command(0), &mut writes);
        acceptor.record_accepted(Slot(3), ballot(1), command(3), &mut writes);
        assert_eq!(acceptor.vote_watermark(), Some(Slot(3)));
        // A vote at a lower slot never lowers it.
        acceptor.record_accepted(Slot(1), ballot(1), command(1), &mut writes);
        assert_eq!(acceptor.vote_watermark(), Some(Slot(3)));
        // Truncating past every record: the floor stands in for the votes.
        acceptor.truncate(Slot(4), JournalState::default(), &mut writes);
        assert!(acceptor.records().is_empty());
        assert_eq!(acceptor.vote_watermark(), Some(Slot(3)));
        // Truncating to a floor above the old watermark raises it: every
        // slot below the floor was chosen, hence voted.
        acceptor.truncate(Slot(6), JournalState::default(), &mut writes);
        assert_eq!(acceptor.vote_watermark(), Some(Slot(5)));
        // A faulty entry — identity known, value lost — was voted too.
        let mut faulty = BTreeMap::new();
        faulty.insert(Slot(9), ballot(1));
        let rotted: Acceptor<Command> = Acceptor::new(ballot(1), BTreeMap::new(), Slot(6), faulty);
        assert_eq!(rotted.vote_watermark(), Some(Slot(9)));
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
        acceptor.truncate(Slot(1), JournalState::default(), &mut writes);
        assert_eq!(acceptor.first_slot(), Slot(1));
        assert!(
            matches!(writes.last(), Some(WriteOp::Truncate { first, .. }) if *first == Slot(1)),
            "the truncation is durable"
        );
        acceptor.trim_to(Slot(5), JournalState::default(), &mut writes);
        assert_eq!(acceptor.first_slot(), Slot(5));
        assert!(acceptor.records().is_empty(), "the dropped prefix is gone");
        assert_eq!(
            acceptor.promised(),
            ballot(1),
            "a trim-point jump never moves the promise"
        );
        assert!(
            matches!(
                writes.last(),
                Some(WriteOp::TrimmedTo { point, .. }) if *point == Slot(5)
            ),
            "the jump is durable"
        );
        assert!(
            writes.iter().all(WriteOp::needs_sync),
            "every write an acceptor emits is safety-critical"
        );
    }
}
