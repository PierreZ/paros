//! How each answer is metered, and the names a journal is labelled with.

use moonpool_sim::SimProviders;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_model::machine::ControlJournals;
use paros_observe::metrics::CallOutcome;
use paros_wire::{ReadAck, SetLeaderAck, TruncateAck, WriteAck, WriteOutcome};

use super::{
    Answer, journal_names, read_answer, request_bytes, set_leader_answer, truncate_answer,
    write_answer,
};

fn judged(answer: &Answer) -> (CallOutcome, u64, u64) {
    (answer.outcome, answer.written, answer.read)
}

fn write(outcome: WriteOutcome) -> WriteAck {
    WriteAck {
        outcome: outcome.into(),
        ..WriteAck::default()
    }
}

#[test]
fn a_write_counts_its_bytes_only_when_accepted() {
    assert_eq!(
        judged(&write_answer(&write(WriteOutcome::Accepted), 9)),
        (CallOutcome::Ok, 9, 0)
    );
    let refusals = [
        (WriteOutcome::Duplicate, CallOutcome::Duplicate),
        (WriteOutcome::Refused, CallOutcome::Refused),
        (WriteOutcome::Truncated, CallOutcome::Truncated),
        (WriteOutcome::TooLarge, CallOutcome::TooLarge),
        (WriteOutcome::WrongMode, CallOutcome::WrongMode),
    ];
    for (wire, outcome) in refusals {
        assert_eq!(judged(&write_answer(&write(wire), 9)), (outcome, 0, 0));
    }
    // A busy leader is busy whatever else the answer says.
    let busy = WriteAck {
        busy: true,
        ..write(WriteOutcome::Accepted)
    };
    assert_eq!(judged(&write_answer(&busy, 9)), (CallOutcome::Busy, 0, 0));
}

#[test]
fn an_answer_with_no_verdict_is_a_redirect_an_unknown_journal_or_unserved() {
    let redirect = WriteAck {
        leader: Some(2),
        ..WriteAck::default()
    };
    assert_eq!(write_answer(&redirect, 1).outcome, CallOutcome::Redirected);
    let unknown = WriteAck {
        unknown_journal: true,
        leader: Some(2),
        ..WriteAck::default()
    };
    assert_eq!(
        write_answer(&unknown, 1).outcome,
        CallOutcome::UnknownJournal
    );
    assert_eq!(
        write_answer(&WriteAck::default(), 1).outcome,
        CallOutcome::Unserved
    );
    let unknown_label = WriteAck {
        outcome: 999,
        ..WriteAck::default()
    };
    assert_eq!(
        write_answer(&unknown_label, 1).outcome,
        CallOutcome::Unserved
    );
}

#[test]
fn a_read_counts_the_bytes_it_served() {
    let page = ReadAck {
        served: true,
        records: vec![vec![1, 2], vec![3]],
        ..ReadAck::default()
    };
    assert_eq!(judged(&read_answer(&page, 0)), (CallOutcome::Ok, 0, 3));
    let below = ReadAck {
        served: true,
        truncated: true,
        records: vec![vec![1]],
        ..ReadAck::default()
    };
    assert_eq!(
        judged(&read_answer(&below, 0)),
        (CallOutcome::Truncated, 0, 0)
    );
    assert_eq!(
        read_answer(&ReadAck::default(), 0).outcome,
        CallOutcome::Unserved
    );
    let unknown = ReadAck {
        served: true,
        unknown_journal: true,
        ..ReadAck::default()
    };
    assert_eq!(
        read_answer(&unknown, 0).outcome,
        CallOutcome::UnknownJournal
    );
}

#[test]
fn a_set_leader_and_a_truncate_are_judged_by_their_flags() {
    let set = |f: fn(&mut SetLeaderAck)| {
        let mut ack = SetLeaderAck::default();
        f(&mut ack);
        set_leader_answer(&ack, 0).outcome
    };
    assert_eq!(set(|a| a.wrong_mode = true), CallOutcome::WrongMode);
    assert_eq!(set(|a| (a.decided, a.won) = (true, true)), CallOutcome::Ok);
    assert_eq!(set(|a| a.decided = true), CallOutcome::Refused);
    assert_eq!(set(|a| a.leader = Some(1)), CallOutcome::Redirected);
    assert_eq!(set(|_| ()), CallOutcome::Unserved);
    // A wrong mode wins over a decision.
    assert_eq!(
        set(|a| (a.wrong_mode, a.decided, a.won) = (true, true, true)),
        CallOutcome::WrongMode
    );

    let truncate = |f: fn(&mut TruncateAck)| {
        let mut ack = TruncateAck::default();
        f(&mut ack);
        truncate_answer(&ack, 0).outcome
    };
    assert_eq!(truncate(|a| a.wrong_mode = true), CallOutcome::WrongMode);
    assert_eq!(
        truncate(|a| (a.refused, a.decided) = (true, true)),
        CallOutcome::Refused
    );
    assert_eq!(truncate(|a| a.decided = true), CallOutcome::Ok);
    assert_eq!(
        truncate(|a| a.unknown_journal = true),
        CallOutcome::UnknownJournal
    );
    assert_eq!(truncate(|_| ()), CallOutcome::Unserved);
}

#[test]
fn a_request_carries_the_bytes_of_its_records() {
    assert_eq!(request_bytes(&[]), 0);
    assert_eq!(request_bytes(&[vec![1, 2, 3], vec![], vec![4]]), 4);
}

#[test]
fn the_cells_own_journals_have_names_and_any_other_is_named_in_hex() {
    let id = |tenant, journal| JournalIdentifier::new(TenantId(tenant), JournalId(journal));
    let cell = ControlJournals {
        cell_id: 1,
        cell: id(16, 1),
        fleet: Some(id(17, 1)),
        election: Some(id(16, 2)),
    };
    let names = |journal| journal_names::<SimProviders>(None, Some(&cell), journal);
    assert_eq!(names(id(16, 1)), ("cell".into(), "(control)".into()));
    assert_eq!(names(id(16, 2)), ("cell".into(), "(election)".into()));
    assert_eq!(names(id(17, 1)), ("universe".into(), "(directory)".into()));
    assert_eq!(names(id(31, 255)), ("1f".into(), "ff".into()));
    assert_eq!(
        journal_names::<SimProviders>(None, None, id(16, 1)),
        ("10".into(), "1".into()),
        "off a machine, no journal is the cell's"
    );
}
