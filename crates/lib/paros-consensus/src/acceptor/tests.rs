use super::*;
use crate::contract::write::WriteOp;
use crate::journal::state::JournalState;
use crate::journal::{Command, Entry, LeaderUuid, Seq};
use crate::types::{NodeId, Value};

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(0),
    }
}

fn command(byte: u8) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(u64::from(byte)),
        records: vec![Value::from(vec![byte])],
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

/// The scrub's rewrite (#362 (background scrubbing)) writes the record
/// held, at its own ballot, and nothing else; a slot with no readable
/// record (never voted, faulty, below the floor) writes nothing.
#[test]
fn a_rewrite_writes_the_record_held_and_moves_nothing() {
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(3), ballot(1));
    let mut acceptor = Acceptor::new(ballot(2), BTreeMap::new(), Slot(1), faulty);
    let mut writes: Vec<WriteOp> = Vec::new();
    acceptor.record_accepted(Slot(1), ballot(1), command(1), &mut writes);
    let (promised, watermark, repaired) = (
        acceptor.promised(),
        acceptor.vote_watermark(),
        acceptor.faulty_repaired(),
    );
    writes.clear();
    assert_eq!(acceptor.rewrite(Slot(1), &mut writes), Some(ballot(1)));
    assert!(
        matches!(
            writes.as_slice(),
            [WriteOp::Acceptor(AcceptorWrite::AppendAccepted { slot, ballot: b, value })]
                if *slot == Slot(1) && *b == ballot(1) && *value == command(1)
        ),
        "the rewrite is the record held"
    );
    assert_eq!(acceptor.promised(), promised);
    assert_eq!(acceptor.vote_watermark(), watermark);
    assert_eq!(acceptor.faulty_repaired(), repaired + 1);
    writes.clear();
    // Never voted, faulty, below the floor: nothing to write.
    for slot in [Slot(2), Slot(3), Slot(0)] {
        assert_eq!(acceptor.rewrite(slot, &mut writes), None);
    }
    assert!(writes.is_empty(), "a slot with no record writes nothing");
}

/// The tri-state is a partition: a slot both readable and faulty breaks
/// the acceptor's check.
#[test]
#[should_panic(expected = "the faulty set stays disjoint from the accepted log")]
fn a_slot_both_readable_and_faulty_breaks_the_check() {
    let mut records = BTreeMap::new();
    records.insert(Slot(1), (ballot(1), command(1)));
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(1), ballot(1));
    let _ = Acceptor::new(ballot(1), records, Slot(0), faulty);
}

/// A learn holds the chosen command and never lowers a recorded ballot: a
/// held record of the command at or above the choosing ballot stays with no
/// write, a lower or foreign record is replaced at the choosing ballot, and
/// the promise rises ahead of the record in the same batch.
#[test]
fn a_learn_keeps_a_higher_record_and_replaces_a_lower_one() {
    let mut records = BTreeMap::new();
    records.insert(Slot(1), (ballot(5), command(1)));
    records.insert(Slot(2), (ballot(1), command(9)));
    let mut acceptor = Acceptor::new(ballot(5), records, Slot(0), BTreeMap::new());
    let mut writes: Vec<WriteOp> = Vec::new();

    // Chosen at 3, held at 5: the record stays at 5, nothing is written.
    assert_eq!(
        acceptor.learn_chosen(Slot(1), ballot(3), &command(1), &mut writes),
        ballot(5)
    );
    assert!(writes.is_empty(), "a held higher record writes nothing");

    // Chosen at 4 with another command than the stale vote at 1: replaced.
    assert_eq!(
        acceptor.learn_chosen(Slot(2), ballot(4), &command(2), &mut writes),
        ballot(4)
    );
    assert_eq!(acceptor.record(Slot(2)), Some(&(ballot(4), command(2))));
    assert!(
        matches!(
            writes.as_slice(),
            [WriteOp::Acceptor(AcceptorWrite::AppendAccepted { slot, ballot: b, .. })]
                if *slot == Slot(2) && *b == ballot(4)
        ),
        "one record, no promise move under the promise"
    );
    writes.clear();

    // Chosen above the promise: the promise rises first, then the record.
    assert_eq!(
        acceptor.learn_chosen(Slot(3), ballot(7), &command(3), &mut writes),
        ballot(7)
    );
    assert_eq!(acceptor.promised(), ballot(7));
    assert!(
        matches!(
            writes.as_slice(),
            [
                WriteOp::Acceptor(AcceptorWrite::SetPromise(p)),
                WriteOp::Acceptor(AcceptorWrite::AppendAccepted { slot, .. }),
            ] if *p == ballot(7) && *slot == Slot(3)
        ),
        "the promise rides ahead of the record"
    );
}

/// A faulty entry above the choosing ballot is repaired at its own ballot:
/// the acceptor voted there, and a vote above the choosing ballot carries
/// the chosen command. Repairing it at the choosing ballot would hide the
/// vote from every later Phase 1 and CTRL threshold.
#[test]
fn a_learn_repairs_a_faulty_entry_at_its_own_ballot() {
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(1), ballot(6));
    faulty.insert(Slot(2), ballot(2));
    let mut acceptor = Acceptor::new(ballot(6), BTreeMap::new(), Slot(0), faulty);
    let mut writes: Vec<WriteOp> = Vec::new();

    assert_eq!(
        acceptor.learn_chosen(Slot(1), ballot(3), &command(1), &mut writes),
        ballot(6),
        "the lost vote's ballot is kept"
    );
    assert_eq!(acceptor.record(Slot(1)), Some(&(ballot(6), command(1))));
    // Below the choosing ballot, the faulty vote gives way to it.
    assert_eq!(
        acceptor.learn_chosen(Slot(2), ballot(4), &command(2), &mut writes),
        ballot(4)
    );
    assert!(acceptor.faulty().is_empty(), "both faulty entries healed");
    assert_eq!(acceptor.faulty_repaired(), 2);
    assert_eq!(acceptor.promised(), ballot(6), "no promise moved");
}

/// A learn below the floor is the caller's error: a truncated slot is
/// never learned again.
#[test]
#[should_panic(expected = "a learned slot lies at or above the compaction floor")]
fn a_learn_below_the_floor_is_a_programmer_error() {
    let mut acceptor: Acceptor<Command> =
        Acceptor::new(ballot(1), BTreeMap::new(), Slot(2), BTreeMap::new());
    let mut writes: Vec<WriteOp> = Vec::new();
    let _ = acceptor.learn_chosen(Slot(1), ballot(1), &command(1), &mut writes);
}

/// `prepare` at the role (#907 (core unit tests)): a higher ballot raises
/// the promise with one write, the same ballot re-affirms it with none, a
/// lower ballot is refused, and a range below the floor is refused without
/// a move of the promise, whatever the ballot.
#[test]
fn prepare_raises_reaffirms_refuses_and_guards_the_floor() {
    let mut acceptor: Acceptor<Command> =
        Acceptor::new(ballot(2), BTreeMap::new(), Slot(3), BTreeMap::new());
    let mut writes: Vec<WriteOp> = Vec::new();
    assert_eq!(
        acceptor.prepare(ballot(5), Slot(1), &mut writes),
        PrepareOutcome::BelowFloor
    );
    assert_eq!(acceptor.promised(), ballot(2), "a laggard never ratchets");
    assert_eq!(writes.len(), 0);
    assert_eq!(
        acceptor.prepare(ballot(1), Slot(3), &mut writes),
        PrepareOutcome::Refused
    );
    assert_eq!(writes.len(), 0);
    assert_eq!(
        acceptor.prepare(ballot(4), Slot(3), &mut writes),
        PrepareOutcome::Promised { raised: true }
    );
    assert_eq!(acceptor.promised(), ballot(4));
    assert_eq!(writes.len(), 1, "a raise writes the promise once");
    assert_eq!(
        acceptor.prepare(ballot(4), Slot(7), &mut writes),
        PrepareOutcome::Promised { raised: false }
    );
    assert_eq!(writes.len(), 1, "a page continuation writes nothing");
}

/// `admit` at the role: an `Accept` below the floor is ignored, one below
/// the promise is refused, one at or above the promise lands; `admit` moves
/// nothing.
#[test]
fn admit_ignores_below_the_floor_and_refuses_below_the_promise() {
    let acceptor: Acceptor<Command> =
        Acceptor::new(ballot(3), BTreeMap::new(), Slot(2), BTreeMap::new());
    assert_eq!(
        acceptor.admit(ballot(9), Slot(1)),
        AcceptOutcome::BelowFloor
    );
    assert_eq!(acceptor.admit(ballot(2), Slot(2)), AcceptOutcome::Refused);
    assert_eq!(acceptor.admit(ballot(3), Slot(2)), AcceptOutcome::Admitted);
    assert_eq!(acceptor.admit(ballot(4), Slot(9)), AcceptOutcome::Admitted);
    assert_eq!(acceptor.promised(), ballot(3), "admit only answers");
}

/// `promise_page` at the role: the union of readable and faulty slots in
/// slot order, cut at the limit with a cursor at the next slot held, and no
/// cursor once the suffix is carried.
#[test]
fn promise_pages_walk_the_readable_and_faulty_union() {
    let mut records = BTreeMap::new();
    for s in [1u64, 2, 5] {
        records.insert(Slot(s), (ballot(1), command(u8::try_from(s).unwrap())));
    }
    let mut faulty = BTreeMap::new();
    faulty.insert(Slot(3), ballot(1));
    let acceptor = Acceptor::new(ballot(1), records, Slot(1), faulty);
    let first = acceptor.promise_page(Slot(0), 2);
    assert_eq!(
        first.accepted.keys().copied().collect::<Vec<_>>(),
        [Slot(1), Slot(2)]
    );
    assert!(first.faulty.is_empty());
    assert_eq!(first.next_from_slot, Some(Slot(3)));
    let second = acceptor.promise_page(Slot(3), 2);
    assert_eq!(second.faulty.keys().copied().collect::<Vec<_>>(), [Slot(3)]);
    assert_eq!(
        second.accepted.keys().copied().collect::<Vec<_>>(),
        [Slot(5)]
    );
    assert_eq!(second.next_from_slot, None, "the suffix ends on this page");
    let tail = acceptor.promise_page(Slot(6), 2);
    assert!(tail.accepted.is_empty() && tail.faulty.is_empty());
    assert_eq!(tail.next_from_slot, None, "an empty tail has no cursor");
    let whole = acceptor.promise_page(Slot(0), 8);
    assert_eq!(whole.accepted.len() + whole.faulty.len(), 4);
    assert_eq!(whole.next_from_slot, None, "a page that fits has no cursor");
}

fn sized(byte: u8, len: usize) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(u64::from(byte)),
        records: vec![Value::from(vec![byte; len])],
    })
}

/// The retained bytes (#1153 (retained log memory)) are the sum of the
/// records' weights: a boot sums what it read back, a record adds its
/// bytes, a record that replaces another swaps their bytes, and a
/// truncation and a trim-point jump drop the bytes below the floor.
#[test]
fn the_retained_bytes_follow_every_record_written_and_dropped() {
    let mut records = BTreeMap::new();
    records.insert(Slot(0), (ballot(1), sized(0, 3)));
    records.insert(Slot(1), (ballot(1), sized(1, 5)));
    let mut acceptor: Acceptor<Command> =
        Acceptor::new(ballot(1), records, Slot(0), BTreeMap::new());
    assert_eq!(
        acceptor.retained_bytes(),
        8,
        "a boot sums the records read back"
    );
    let mut writes: Vec<WriteOp> = Vec::new();
    acceptor.set_promise(ballot(2), &mut writes);
    acceptor.record_accepted(Slot(2), ballot(2), sized(2, 7), &mut writes);
    assert_eq!(acceptor.retained_bytes(), 15, "a new record adds its bytes");
    acceptor.record_accepted(Slot(1), ballot(2), sized(9, 2), &mut writes);
    assert_eq!(
        acceptor.retained_bytes(),
        12,
        "a record that replaces another swaps their bytes"
    );
    acceptor.record_accepted(
        Slot(3),
        ballot(2),
        Command::Control(crate::journal::Control::Noop),
        &mut writes,
    );
    assert_eq!(
        acceptor.retained_bytes(),
        12,
        "a control command weighs nothing"
    );
    acceptor.truncate(Slot(1), JournalState::default(), &mut writes);
    assert_eq!(
        acceptor.retained_bytes(),
        9,
        "a truncation drops the bytes below the floor"
    );
    acceptor.trim_to(Slot(3), JournalState::default(), &mut writes);
    assert_eq!(
        acceptor.retained_bytes(),
        0,
        "a trim-point jump drops the bytes below its point"
    );
    acceptor.trim_to(Slot(4), JournalState::default(), &mut writes);
    assert!(acceptor.records().is_empty());
    assert_eq!(acceptor.retained_bytes(), 0, "no record, no byte");
}
