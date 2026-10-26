use super::*;
use crate::journal::state::JournalState;
use crate::journal::{Command, Entry, LeaderUuid, Seq};
use crate::types::{NodeId, Value};
use crate::write::WriteOp;

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
