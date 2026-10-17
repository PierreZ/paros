//! The client's pure parts, pinned: the judges read a reply the way the
//! wire contract means it, the writer's belief follows the verdicts, and
//! the reader's cursor never moves back and never skips a gap silently.
//! The policy loops are judged by the simulation, which runs them.

use moonpool_rpc::{ErrorReason, RpcError};
use paros_core::{JournalIdentifier, JournalView, LeaderUuid, ReconfigureRefusal, Seq, Value};

use super::outcome::{
    MatchmakersRefusal, ReadOutcome, ReconfigureMatchmakersOutcome, ReconfigureOutcome,
    RetireOutcome, RetireRefusal, SetLeaderOutcome, TruncateOutcome, WriteOutcome,
};
use super::{ClaimOutcome, Learned, Reader, ReaderOutcome, Writer, leader_uuid};
use crate::rpc::public::WriteOutcome as Wire;
use crate::rpc::{
    ReadAck, ReconfigureAck, ReconfigureMatchmakersAck, RetireAck, SetLeaderAck, TruncateAck,
    WriteAck, journal_view_to_proto, leader_uuid_from_proto,
};

fn state(leader: Option<LeaderUuid>, next: u64, first: u64) -> JournalView {
    JournalView {
        leader,
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

fn write_ack(outcome: Wire, state: Option<JournalView>) -> WriteAck {
    WriteAck {
        outcome: outcome as i32,
        seq: 4,
        count: 2,
        state: state.map(journal_view_to_proto),
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
    let s = state(Some(LeaderUuid(7)), 6, 0);
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
    // A floor past the next position is no journal view.
    let mut bad = journal_view_to_proto(s);
    bad.first_seq = bad.next_seq + 1;
    let malformed = WriteAck {
        state: Some(bad),
        ..write_ack(Wire::Refused, None)
    };
    assert_eq!(WriteOutcome::judge(&Ok(malformed)), WriteOutcome::Malformed);
}

#[test]
fn an_edge_refusal_names_the_limits_and_is_no_journal_verdict() {
    let ack = WriteAck {
        max_records: 8,
        max_bytes: 4096,
        ..write_ack(Wire::TooLarge, None)
    };
    let outcome = WriteOutcome::judge(&Ok(ack));
    assert_eq!(
        outcome,
        WriteOutcome::TooLarge {
            max_records: 8,
            max_bytes: 4096
        }
    );
    // Nothing reached the journal: no leader is learned from it.
    assert!(!outcome.is_verdict());
}

#[test]
fn a_set_leader_reply_wins_loses_or_redirects() {
    let s = state(Some(LeaderUuid(1)), 0, 0);
    let ack = |decided, won| SetLeaderAck {
        decided,
        won,
        leader: Some(2),
        state: Some(journal_view_to_proto(s)),
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
    let s = state(None, 9, 5);
    let ack = |served, truncated| ReadAck {
        served,
        truncated,
        from_seq: 6,
        records: if truncated {
            vec![]
        } else {
            vec![b"a".to_vec()]
        },
        state: Some(journal_view_to_proto(s)),
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
    let s = state(None, 9, 5);
    let applied = TruncateAck {
        decided: true,
        state: Some(journal_view_to_proto(s)),
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
        state: Some(journal_view_to_proto(s)),
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
    assert_eq!(
        reconfigure("unknown_journal"),
        ReconfigureOutcome::UnknownJournal
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
        ("unknown_tenant", MatchmakersRefusal::UnknownTenant),
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
        ("unknown_journal", RetireRefusal::UnknownJournal),
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
    let journal = JournalIdentifier::new(paros_core::TenantId(7), paros_core::JournalId(9));
    let mut writer = Writer::new(journal, 7);
    let first = leader_uuid(7, 0);
    assert_eq!(writer.uuid(), first);
    assert_eq!(writer.entry(vec![Value(b"x".to_vec())]), None);

    writer.won(&state(Some(first), 10, 0));
    let entry = writer
        .entry(vec![Value(b"x".to_vec())])
        .expect("a leader builds a write");
    assert_eq!((entry.leader, entry.seq), (first, Seq(10)));
    let request = writer.request(&entry);
    assert_eq!(
        (
            request.journal,
            leader_uuid_from_proto(request.leader),
            request.seq
        ),
        (journal.journal.0, first, 10)
    );
    // A truncation carries the leader's own fence (#228).
    let truncate = writer.truncate_request(5).expect("a leader truncates");
    assert_eq!(
        (leader_uuid_from_proto(truncate.leader), truncate.up_to),
        (first, 5)
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
            state: state(Some(first), 14, 0)
        }),
        Some(Learned::Owner)
    );
    assert_eq!(writer.next_seq(), 14);

    // Another leader supersedes it: it leads nothing, its next claim asks
    // under a new uuid, and only the explicit misbehaviour still writes
    // under the old one.
    let other = LeaderUuid(8);
    assert_eq!(
        writer.absorb(&WriteOutcome::Refused {
            state: state(Some(other), 14, 0)
        }),
        Some(Learned::Superseded)
    );
    assert_eq!(writer.owned(), None);
    assert_eq!(writer.entry(vec![]), None);
    let second = writer.uuid();
    assert_ne!(second, first, "a new term, a new uuid");
    assert_eq!(second, leader_uuid(7, 1));
    assert_eq!(writer.stale_entry(vec![]).leader, first);
    assert_eq!(
        writer.truncate_request(1),
        None,
        "a superseded writer truncates nothing"
    );
    assert_eq!(
        leader_uuid_from_proto(writer.stale_truncate_request(1).leader),
        first
    );
    assert_eq!(
        writer.learn(&state(Some(other), 14, 0)),
        Learned::NotOwner,
        "superseded once, reported once"
    );
    assert_eq!(writer.uuid(), second, "a uuid is spent once per term");

    // A claim that finds its uuid already leading adopts the state.
    assert_eq!(
        writer.claimed(&ClaimOutcome::Owned {
            state: state(Some(second), 20, 0)
        }),
        Some(Learned::Owner)
    );
    assert_eq!((writer.owned(), writer.next_seq()), (Some(second), 20));
    assert_eq!(writer.claimed(&ClaimOutcome::Unread), None);
}

#[test]
fn derived_leader_uuids_are_set_and_distinct() {
    let uuids: std::collections::BTreeSet<LeaderUuid> = (0..64)
        .flat_map(|seed| (0..64).map(move |k| leader_uuid(seed, k)))
        .collect();
    assert_eq!(
        uuids.len(),
        64 * 64,
        "no two (seed, term) pairs collide here"
    );
    assert!(uuids.iter().all(|uuid| uuid.is_set()));
}

#[test]
fn a_reader_resumes_at_the_floor_and_reports_the_gap() {
    let journal = JournalIdentifier::new(paros_core::TenantId(7), paros_core::JournalId(9));
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

    let floor = state(None, 20, 8);
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

/// A pipeline over a writer that won `journal` at position `next`.
fn leading_lanes(next: u64) -> (super::pipeline::Lanes, LeaderUuid) {
    let journal = JournalIdentifier::new(paros_core::TenantId(7), paros_core::JournalId(9));
    let mut writer = Writer::new(journal, 11);
    let uuid = writer.uuid();
    writer.won(&state(Some(uuid), next, 0));
    (super::pipeline::Lanes::new(writer), uuid)
}

fn one(byte: u8) -> Vec<Value> {
    vec![Value(vec![byte])]
}

#[test]
fn a_pipeline_gives_consecutive_positions_and_waits_out_an_early_write() {
    use super::WriterOutcome;
    use super::pipeline::{Admit, Folded, Lanes, Resumed};

    let journal = JournalIdentifier::new(paros_core::TenantId(7), paros_core::JournalId(9));
    let idle = Lanes::new(Writer::new(journal, 3));
    assert_eq!(idle.admit(), Admit::NotOwner);

    let (mut lanes, uuid) = leading_lanes(10);
    assert_eq!(lanes.admit(), Admit::At(10));
    let a = lanes.open(one(1));
    let b = lanes.open(vec![Value(vec![2]), Value(vec![3])]);
    let c = lanes.open(one(4));
    let seqs: Vec<u64> = [&a, &b, &c].iter().map(|t| t.entry().seq.0).collect();
    assert_eq!(seqs, [10, 11, 13]);
    assert!([&a, &b, &c].iter().all(|t| t.entry().leader == uuid));

    // `c` arrives first: refused at a tail below it, under its own uuid.
    let early = state(Some(uuid), 10, 0);
    assert_eq!(
        lanes.fold(2, WriterOutcome::Refused { state: early }),
        Folded::Early
    );
    assert_eq!(lanes.resume(2), Resumed::Wait);
    let written = |seq, count| WriterOutcome::Written {
        seq,
        count,
        duplicate: false,
        resolved: false,
    };
    assert_eq!(lanes.fold(0, written(10, 1)), Folded::Done(written(10, 1)));
    assert_eq!(lanes.resume(2), Resumed::Wait);
    assert_eq!(lanes.fold(1, written(11, 2)), Folded::Done(written(11, 2)));
    // Every write in front of it is written: the identical write again.
    assert_eq!(lanes.resume(2), Resumed::Send(c.entry().clone()));
    assert_eq!(lanes.fold(2, written(13, 1)), Folded::Done(written(13, 1)));
    assert_eq!(lanes.admit(), Admit::At(14));
    assert_eq!(lanes.writer.next_seq(), 14);
}

#[test]
fn a_failed_write_strands_the_writes_behind_it_until_the_pipeline_drains() {
    use super::WriterOutcome;
    use super::pipeline::{Admit, Folded, Resumed};

    let (mut lanes, uuid) = leading_lanes(0);
    let _a = lanes.open(one(1));
    let _b = lanes.open(one(2));
    let _c = lanes.open(one(3));
    // `b` arrived before `a`, then `a` is not written.
    let early = state(Some(uuid), 0, 0);
    assert_eq!(
        lanes.fold(1, WriterOutcome::Refused { state: early }),
        Folded::Early
    );
    assert_eq!(
        lanes.fold(0, WriterOutcome::Ambiguous),
        Folded::Done(WriterOutcome::Ambiguous)
    );
    // No write opens while the failure's writes are in flight.
    assert_eq!(lanes.admit(), Admit::Drain);
    // `b`'s position is never reached: its one verdict is the refusal.
    assert_eq!(
        lanes.resume(1),
        Resumed::Done(WriterOutcome::Refused { state: early })
    );
    assert_eq!(lanes.admit(), Admit::Drain);
    assert_eq!(
        lanes.fold(2, WriterOutcome::Refused { state: early }),
        Folded::Early
    );
    assert_eq!(
        lanes.resume(2),
        Resumed::Done(WriterOutcome::Refused { state: early })
    );
    // Drained: the next write opens at the tail the writer knows.
    assert_eq!(lanes.admit(), Admit::At(0));
}

#[test]
fn a_new_term_opens_at_its_view_while_the_old_one_drains() {
    use super::WriterOutcome;
    use super::pipeline::{Admit, Folded};

    let (mut lanes, old) = leading_lanes(5);
    let _a = lanes.open(one(1));
    let _b = lanes.open(one(2));
    let new = lanes.begin_claim(true);
    assert_ne!(new, old);
    assert_eq!(lanes.admit(), Admit::NotOwner);
    // The claim lands after `a`: the journal stands at 6 under `new`.
    lanes.claimed(
        new,
        &ClaimOutcome::Won {
            state: state(Some(new), 6, 0),
        },
    );
    assert_eq!(lanes.admit(), Admit::At(6));
    let c = lanes.open(one(3));
    assert_eq!((c.entry().leader, c.entry().seq.0), (new, 6));
    // The old term's writes settle without stopping the new term.
    let written = WriterOutcome::Written {
        seq: 5,
        count: 1,
        duplicate: false,
        resolved: false,
    };
    assert_eq!(lanes.fold(0, written.clone()), Folded::Done(written));
    let fenced = WriterOutcome::Superseded {
        state: state(Some(new), 7, 0),
    };
    assert_eq!(lanes.fold(1, fenced.clone()), Folded::Done(fenced));
    assert_eq!(lanes.admit(), Admit::At(7));
    assert_eq!(lanes.writer.owned(), Some(new));
}

#[test]
fn an_abandoned_write_stops_the_pipeline_until_it_drains() {
    use super::pipeline::Admit;

    let (mut lanes, _) = leading_lanes(3);
    let _a = lanes.open(one(1));
    let _b = lanes.open(one(2));
    lanes.abandon(0);
    assert_eq!(lanes.admit(), Admit::Drain);
    lanes.abandon(1);
    assert_eq!(lanes.admit(), Admit::At(3));
}
