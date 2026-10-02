//! The client's pure parts, pinned: the judges read a reply the way the
//! wire contract means it, the writer's belief follows the verdicts, and
//! the reader's cursor never moves back and never skips a gap silently.
//! The policy loops are judged by the simulation, which runs them.

use moonpool_rpc::{ErrorReason, RpcError};
use paros_core::{ClientId, Generation, JournalKey, JournalState, ReconfigureRefusal, Seq, Value};

use super::outcome::{
    MatchmakersRefusal, ReadOutcome, ReconfigureMatchmakersOutcome, ReconfigureOutcome,
    RetireOutcome, RetireRefusal, SetLeaderOutcome, TruncateOutcome, WriteOutcome,
};
use super::{ClaimOutcome, Learned, Reader, ReaderOutcome, Writer};
use crate::rpc::public::WriteOutcome as Wire;
use crate::rpc::{
    ReadAck, ReconfigureAck, ReconfigureMatchmakersAck, RetireAck, SetLeaderAck, TruncateAck,
    WriteAck, journal_state_to_proto,
};

fn state(owner: Option<u64>, generation: u64, next: u64, first: u64) -> JournalState {
    JournalState {
        owner: owner.map(ClientId),
        generation: Generation(generation),
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

fn write_ack(outcome: Wire, state: Option<JournalState>) -> WriteAck {
    WriteAck {
        outcome: outcome as i32,
        seq: 4,
        count: 2,
        state: state.map(journal_state_to_proto),
        ..WriteAck::default()
    }
}

#[test]
fn a_transport_error_is_ambiguous_never_a_refusal() {
    fn lost<T>() -> Result<T, RpcError> {
        Err(RpcError::not_admitted(ErrorReason::EndpointNotFound))
    }
    assert_eq!(WriteOutcome::judge(&lost()), WriteOutcome::Ambiguous);
    assert_eq!(
        SetLeaderOutcome::judge(&lost()),
        SetLeaderOutcome::Ambiguous
    );
    assert_eq!(ReadOutcome::judge(lost()), ReadOutcome::Ambiguous);
    assert_eq!(TruncateOutcome::judge(&lost()), TruncateOutcome::Ambiguous);
    assert_eq!(
        ReconfigureOutcome::judge(lost()),
        ReconfigureOutcome::Ambiguous
    );
    assert_eq!(RetireOutcome::judge(lost()), RetireOutcome::Ambiguous);
}

#[test]
fn a_write_reply_is_judged_by_its_outcome() {
    let s = state(Some(7), 2, 6, 0);
    assert_eq!(
        WriteOutcome::judge(&Ok(write_ack(Wire::Accepted, None))),
        WriteOutcome::Written {
            seq: 4,
            count: 2,
            duplicate: false
        }
    );
    assert_eq!(
        WriteOutcome::judge(&Ok(write_ack(Wire::Duplicate, None))),
        WriteOutcome::Written {
            seq: 4,
            count: 2,
            duplicate: true
        }
    );
    assert_eq!(
        WriteOutcome::judge(&Ok(write_ack(Wire::Refused, Some(s)))),
        WriteOutcome::Refused { state: s }
    );
    assert_eq!(
        WriteOutcome::judge(&Ok(write_ack(Wire::Truncated, Some(s)))),
        WriteOutcome::Truncated { state: s }
    );
    let redirect = WriteAck {
        leader: Some(3),
        ..WriteAck::default()
    };
    assert_eq!(
        WriteOutcome::judge(&Ok(redirect)),
        WriteOutcome::Redirect { leader: Some(3) }
    );
    let unknown = WriteAck {
        unknown_journal: true,
        ..write_ack(Wire::Accepted, None)
    };
    assert_eq!(
        WriteOutcome::judge(&Ok(unknown)),
        WriteOutcome::UnknownJournal
    );
    // An owner without a generation is no journal state.
    let mut bad = journal_state_to_proto(s);
    bad.owner = None;
    let malformed = WriteAck {
        state: Some(bad),
        ..write_ack(Wire::Refused, None)
    };
    assert_eq!(WriteOutcome::judge(&Ok(malformed)), WriteOutcome::Malformed);
}

#[test]
fn a_set_leader_reply_wins_loses_or_redirects() {
    let s = state(Some(1), 3, 0, 0);
    let ack = |decided, won| SetLeaderAck {
        decided,
        won,
        leader: Some(2),
        state: Some(journal_state_to_proto(s)),
        ..SetLeaderAck::default()
    };
    assert_eq!(
        SetLeaderOutcome::judge(&Ok(ack(true, true))),
        SetLeaderOutcome::Won { state: s }
    );
    assert_eq!(
        SetLeaderOutcome::judge(&Ok(ack(true, false))),
        SetLeaderOutcome::Lost { state: s }
    );
    assert_eq!(
        SetLeaderOutcome::judge(&Ok(ack(false, false))),
        SetLeaderOutcome::Redirect { leader: Some(2) }
    );
    assert_eq!(
        ClaimOutcome::from(SetLeaderOutcome::Won { state: s }),
        ClaimOutcome::Won { state: s }
    );
}

#[test]
fn a_read_reply_is_a_page_a_truncation_or_unserved() {
    let s = state(None, 0, 9, 5);
    let ack = |served, truncated| ReadAck {
        served,
        truncated,
        from_seq: 6,
        records: if truncated {
            vec![]
        } else {
            vec![b"a".to_vec()]
        },
        state: Some(journal_state_to_proto(s)),
        ..ReadAck::default()
    };
    assert_eq!(
        ReadOutcome::judge(Ok(ack(true, false))),
        ReadOutcome::Page {
            from: 6,
            records: vec![b"a".to_vec()],
            state: s
        }
    );
    assert_eq!(
        ReadOutcome::judge(Ok(ack(true, true))),
        ReadOutcome::Truncated { state: s }
    );
    assert_eq!(
        ReadOutcome::judge(Ok(ack(false, false))),
        ReadOutcome::Unserved
    );
    assert!(!ReadOutcome::Unserved.is_served());
    assert!(ReadOutcome::Truncated { state: s }.is_served());
}

#[test]
fn a_truncation_is_applied_refused_or_redirected() {
    let s = state(None, 0, 9, 5);
    let applied = TruncateAck {
        decided: true,
        state: Some(journal_state_to_proto(s)),
        ..TruncateAck::default()
    };
    assert_eq!(
        TruncateOutcome::judge(&Ok(applied)),
        TruncateOutcome::Applied { state: s }
    );
    let redirect = TruncateAck {
        leader: Some(1),
        ..TruncateAck::default()
    };
    assert_eq!(
        TruncateOutcome::judge(&Ok(redirect)),
        TruncateOutcome::Redirect { leader: Some(1) }
    );
    let refused = TruncateAck {
        decided: true,
        refused: true,
        state: Some(journal_state_to_proto(s)),
        ..TruncateAck::default()
    };
    assert_eq!(
        TruncateOutcome::judge(&Ok(refused)),
        TruncateOutcome::Refused { state: s }
    );
}

#[test]
fn every_refusal_label_the_node_sends_is_typed() {
    let reconfigure = |refusal: &str| {
        ReconfigureOutcome::judge(Ok(ReconfigureAck {
            refusal: refusal.into(),
            ..ReconfigureAck::default()
        }))
    };
    assert_eq!(
        reconfigure("not_leader"),
        ReconfigureOutcome::NotLeader { leader: None }
    );
    for (label, refusal) in [
        ("no_matchmakers", ReconfigureRefusal::NoMatchmakers),
        ("unchanged", ReconfigureRefusal::Unchanged),
        ("unknown_member", ReconfigureRefusal::UnknownMember),
        ("malformed", ReconfigureRefusal::Malformed),
        ("unsettled", ReconfigureRefusal::Unsettled),
        ("round_exhausted", ReconfigureRefusal::RoundExhausted),
    ] {
        assert_eq!(
            reconfigure(label),
            ReconfigureOutcome::Refused {
                leader: None,
                refusal
            }
        );
    }
    assert_eq!(
        reconfigure("a newer label"),
        ReconfigureOutcome::Unrecognized { leader: None }
    );
    let started = ReconfigureOutcome::judge(Ok(ReconfigureAck {
        accepted: true,
        round: Some(9),
        leader: Some(1),
        ..ReconfigureAck::default()
    }));
    assert_eq!(
        started,
        ReconfigureOutcome::Started {
            leader: Some(1),
            round: 9
        }
    );

    for (label, refusal) in [
        ("no_matchmakers", MatchmakersRefusal::NoMatchmakers),
        ("empty", MatchmakersRefusal::Empty),
        ("unknown_matchmaker", MatchmakersRefusal::UnknownMatchmaker),
        ("busy", MatchmakersRefusal::Busy),
        ("?", MatchmakersRefusal::Unrecognized),
    ] {
        assert_eq!(
            ReconfigureMatchmakersOutcome::judge(Ok(ReconfigureMatchmakersAck {
                refusal: label.into(),
                ..ReconfigureMatchmakersAck::default()
            })),
            ReconfigureMatchmakersOutcome::Refused(refusal)
        );
    }

    for (label, refusal) in [
        ("plain", RetireRefusal::Plain),
        ("leader", RetireRefusal::Leader),
        ("member", RetireRefusal::Member),
        ("stale", RetireRefusal::Stale),
        ("not_collected", RetireRefusal::NotCollected),
        ("?", RetireRefusal::Unrecognized),
    ] {
        assert_eq!(
            RetireOutcome::judge(Ok(RetireAck {
                accepted: false,
                refusal: label.into(),
            })),
            RetireOutcome::Refused(refusal)
        );
    }
    assert_eq!(
        RetireOutcome::judge(Ok(RetireAck {
            accepted: true,
            refusal: String::new(),
        })),
        RetireOutcome::Retired
    );
}

#[test]
fn a_writer_owns_only_what_it_claimed_and_stops_when_superseded() {
    let journal = JournalKey::default();
    let mut writer = Writer::new(journal, 7);
    assert_eq!(writer.entry(vec![Value(b"x".to_vec())]), None);

    writer.won(&state(Some(7), 3, 10, 0));
    let entry = writer
        .entry(vec![Value(b"x".to_vec())])
        .expect("an owner builds a write");
    assert_eq!(
        (entry.generation, entry.owner, entry.seq),
        (Generation(3), ClientId(7), Seq(10))
    );
    let request = writer.request(&entry);
    assert_eq!(
        (request.journal, request.generation, request.seq),
        (journal.journal.0, 3, 10)
    );
    // A truncation carries the owner's own fence (#228).
    let truncate = writer.truncate_request(5).expect("an owner truncates");
    assert_eq!(
        (truncate.generation, truncate.owner, truncate.up_to),
        (3, 7, 5)
    );

    // A written batch moves the position past it, never back.
    assert_eq!(
        writer.absorb(&WriteOutcome::Written {
            seq: 10,
            count: 2,
            duplicate: false
        }),
        None
    );
    assert_eq!(writer.next_seq(), 12);
    writer.advance_to(11);
    assert_eq!(writer.next_seq(), 12);

    // A refusal naming itself corrects its position.
    assert_eq!(
        writer.absorb(&WriteOutcome::Refused {
            state: state(Some(7), 3, 14, 0)
        }),
        Some(Learned::Owner)
    );
    assert_eq!(writer.next_seq(), 14);

    // Another owner supersedes it: it owns nothing, and only the explicit
    // misbehaviour still writes under the old generation.
    assert_eq!(
        writer.absorb(&WriteOutcome::Refused {
            state: state(Some(8), 4, 14, 0)
        }),
        Some(Learned::Superseded)
    );
    assert_eq!(writer.owned(), None);
    assert_eq!(writer.entry(vec![]), None);
    assert_eq!(writer.stale_entry(vec![]).generation, Generation(3));
    assert_eq!(
        writer.truncate_request(1),
        None,
        "a superseded writer truncates nothing"
    );
    assert_eq!(writer.stale_truncate_request(1).generation, 3);
    assert_eq!(
        writer.learn(&state(Some(8), 4, 14, 0)),
        Learned::NotOwner,
        "superseded once, reported once"
    );

    // A claim that finds it already the owner adopts the state.
    assert_eq!(
        writer.claimed(&ClaimOutcome::Owned {
            state: state(Some(7), 5, 20, 0)
        }),
        Some(Learned::Owner)
    );
    assert_eq!((writer.owned(), writer.next_seq()), (Some(5), 20));
    assert_eq!(writer.claimed(&ClaimOutcome::Unread), None);
}

#[test]
fn a_reader_resumes_at_the_floor_and_reports_the_gap() {
    let journal = JournalKey::default();
    let mut reader = Reader::new(journal, 3);
    let request = reader.request(16, 50);
    assert_eq!(
        (
            request.journal,
            request.from_seq,
            request.limit,
            request.wait_ms
        ),
        (journal.journal.0, 3, 16, 50)
    );

    let floor = state(None, 0, 20, 8);
    assert_eq!(
        reader.absorb(ReadOutcome::Truncated { state: floor }),
        ReaderOutcome::Gap {
            from: 3,
            resumed_at: 8,
            state: floor
        }
    );
    assert_eq!(reader.cursor(), 8);

    let page = ReadOutcome::Page {
        from: 8,
        records: vec![b"a".to_vec(), b"b".to_vec()],
        state: floor,
    };
    assert!(matches!(
        reader.absorb(page),
        ReaderOutcome::Records { from: 8, .. }
    ));
    assert_eq!(reader.cursor(), 10);
    assert!(!reader.at_tail(&floor));

    // An unserved page moves nothing.
    assert_eq!(
        reader.absorb(ReadOutcome::Unserved),
        ReaderOutcome::Unavailable
    );
    assert_eq!(reader.cursor(), 10);
}
