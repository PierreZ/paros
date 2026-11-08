//! A writer over fake nodes: it claims, writes at its tail, stops when
//! superseded, settles a write whose answer was lost, and truncates under
//! its own fence.

use paros_core::Value;
use paros_core::journal::{Command, Control, JournalView, LeaderUuid, Seq, WriterMode};
use paros_wire::public::WriteOutcome as Wire;
use paros_wire::{ReadAck, WriteAck, journal_view_to_proto};

use crate::rig::Rig;
use crate::rig::cell::{Act, Cell, ids};
use crate::{ClaimOutcome, TruncateOutcome, Writer, WriterOutcome};

const U: LeaderUuid = LeaderUuid(11);
const V: LeaderUuid = LeaderUuid(22);

fn journal() -> paros_core::journal::JournalIdentifier {
    ids(1, 2)
}

fn view(leader: Option<LeaderUuid>, next: u64, first: u64) -> JournalView {
    JournalView {
        leader,
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

fn one(byte: u8) -> Vec<Value> {
    vec![Value(vec![byte])]
}

/// A cell of nodes led by node 1, its journal created and unled.
fn cell() -> Cell {
    let cell = Cell::led_by(1);
    cell.create(journal(), WriterMode::Single);
    cell
}

fn answer(outcome: Wire, state: JournalView) -> Act<WriteAck> {
    Act::Answer(WriteAck {
        outcome: outcome.into(),
        state: Some(journal_view_to_proto(state)),
        ..WriteAck::default()
    })
}

/// A read-back that finds the journal at `state`, holding nothing.
fn read_back(state: JournalView) -> Act<ReadAck> {
    Act::Answer(ReadAck {
        served: true,
        state: Some(journal_view_to_proto(state)),
        ..ReadAck::default()
    })
}

/// Claim with a writer under `U`, then write `records` once: the claim's
/// and the write's outcomes, and the writer.
fn claim_and_write(cell: &Cell, records: Vec<Value>) -> (ClaimOutcome, WriterOutcome, Writer) {
    Rig::new().run_with(cell, &[1, 2], |client| async move {
        let mut writer = Writer::with_uuid(journal(), U);
        let claimed = writer.claim(&client, 1, false).await;
        let wrote = writer.write(&client, records, 1).await;
        (claimed, wrote, writer)
    })
}

#[test]
fn a_writer_that_leads_nothing_sends_nothing() {
    let cell = cell();
    let (wrote, truncated) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut writer = Writer::with_uuid(journal(), U);
        let wrote = writer.write(&client, one(1), 0).await;
        let truncated = writer.truncate(&client, 0, 0).await;
        (wrote, truncated)
    });
    assert_eq!(wrote, WriterOutcome::NotOwner);
    assert_eq!(truncated, None);
    assert!(cell.heard().is_empty());
}

#[test]
fn a_writer_claims_and_writes_at_its_tail() {
    let cell = cell();
    let (claimed, wrote, writer) = claim_and_write(&cell, one(1));
    assert_eq!(
        claimed,
        ClaimOutcome::Won {
            state: view(Some(U), 0, 0)
        }
    );
    assert_eq!(
        wrote,
        WriterOutcome::Written {
            seq: 0,
            count: 1,
            duplicate: false,
            resolved: false
        }
    );
    assert_eq!(writer.next_seq(), 1);
    assert_eq!(writer.owned(), Some(U));
    assert_eq!(cell.records(journal()), vec![Value(vec![1])]);
}

#[test]
fn a_superseded_writer_learns_it_and_stops() {
    let cell = cell();
    let probe = cell.clone();
    let (first, second, owned) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut writer = Writer::with_uuid(journal(), U);
        writer.claim(&client, 0, false).await;
        probe.apply(
            journal(),
            &Command::Control(Control::SetLeader {
                new: V,
                old: Some(U),
            }),
        );
        let first = writer.write(&client, one(1), 0).await;
        let second = writer.write(&client, one(2), 0).await;
        (first, second, writer.owned())
    });
    assert_eq!(
        first,
        WriterOutcome::Superseded {
            state: view(Some(V), 0, 0)
        }
    );
    assert_eq!(
        second,
        WriterOutcome::NotOwner,
        "nothing sent once superseded"
    );
    assert_eq!(owned, None);
    assert!(cell.records(journal()).is_empty());
}

#[test]
fn a_lost_answer_settles_written_and_moves_the_tail() {
    let cell = cell();
    cell.script(1, |s| s.writes.push_back(Act::Lose));
    let (_, wrote, writer) = claim_and_write(&cell, one(1));
    assert_eq!(
        wrote,
        WriterOutcome::Written {
            seq: 0,
            count: 1,
            duplicate: true,
            resolved: true
        }
    );
    assert_eq!(writer.next_seq(), 1);
    assert_eq!(cell.records(journal()), vec![Value(vec![1])]);
}

#[test]
fn an_unanswered_write_fenced_by_another_leader_is_superseded() {
    let cell = cell();
    cell.script(1, |s| {
        s.writes.push_back(Act::Hold);
        // The read-back (the claim reads at node 2).
        s.reads.push_back(read_back(view(Some(V), 0, 0)));
    });
    let (_, wrote, writer) = claim_and_write(&cell, one(1));
    assert_eq!(
        wrote,
        WriterOutcome::Superseded {
            state: view(Some(V), 0, 0)
        }
    );
    assert_eq!(writer.owned(), None);
}

#[test]
fn an_unanswered_write_below_the_floor_is_truncated() {
    let cell = cell();
    cell.script(1, |s| {
        s.writes.push_back(Act::Hold);
        s.reads.push_back(read_back(view(Some(U), 9, 5)));
    });
    let (_, wrote, writer) = claim_and_write(&cell, one(1));
    assert_eq!(
        wrote,
        WriterOutcome::Truncated {
            state: view(Some(U), 9, 5)
        }
    );
    assert_eq!(writer.next_seq(), 9, "the view moves the tail up");
}

#[test]
fn a_write_no_server_settles_is_ambiguous() {
    let cell = cell();
    cell.script(1, |s| {
        s.writes.extend((0..10).map(|_| Act::Hold));
        s.reads.push_back(read_back(view(None, 0, 0)));
        s.reads.extend((0..10).map(|_| Act::Hold));
    });
    let (_, wrote, _) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut writer = Writer::with_uuid(journal(), U);
        let claimed = writer.claim(&client, 0, false).await;
        let wrote = writer.write(&client, one(1), 0).await;
        (claimed, wrote, writer)
    });
    assert_eq!(wrote, WriterOutcome::Ambiguous);
}

#[test]
fn each_verdict_reaches_the_writer() {
    let own = view(Some(U), 0, 0);
    let cases = [
        (
            answer(Wire::Refused, own),
            WriterOutcome::Refused { state: own },
        ),
        (
            answer(Wire::Truncated, own),
            WriterOutcome::Truncated { state: own },
        ),
        (
            answer(Wire::WrongMode, own),
            WriterOutcome::WrongMode { state: own },
        ),
        (
            Act::Answer(WriteAck {
                outcome: Wire::TooLarge.into(),
                max_records: 1,
                max_bytes: 2,
                ..WriteAck::default()
            }),
            WriterOutcome::TooLarge {
                max_records: 1,
                max_bytes: 2,
            },
        ),
        (
            Act::Answer(WriteAck {
                unknown_journal: true,
                ..WriteAck::default()
            }),
            WriterOutcome::UnknownJournal,
        ),
        (
            Act::Answer(WriteAck {
                outcome: Wire::Refused.into(),
                state: Some(paros_wire::common::JournalView {
                    leader: None,
                    next_seq: 0,
                    first_seq: 5,
                }),
                ..WriteAck::default()
            }),
            WriterOutcome::Unavailable { leader: None },
        ),
    ];
    for (reply, expected) in cases {
        let cell = cell();
        cell.script(1, |s| s.writes.push_back(reply));
        // Node 2 does not host the journal: an unknown journal is asked
        // there once more.
        cell.script(2, |s| {
            s.writes.push_back(Act::Answer(WriteAck {
                unknown_journal: true,
                ..WriteAck::default()
            }));
        });
        let wrote = Rig::new().run_with(&cell, &[1, 2], |client| async move {
            let mut writer = Writer::with_uuid(journal(), U);
            writer.claim(&client, 0, false).await;
            writer.write(&client, one(1), 0).await
        });
        assert_eq!(wrote, expected);
    }
}

/// A writer whose client was led, by another journal's verdict, to a
/// server that does not host this journal settles its write past a dead
/// server outside the rotation, at the rotation's first server. The
/// coordinator's case (sweep recipe `16688101968864809467
/// 776343:11864439495246821215` on a1d1506): its client rotated over the
/// two holders of the cell control journal, a draining holder answered
/// unknown, the third server was a renamed machine at a dead address, and
/// the first holder, the leader, was never asked for the whole term.
#[test]
fn a_writer_settles_past_a_dead_server_outside_its_rotation() {
    let cell = cell();
    // Node 2 stopped hosting the journal; node 3 never answers.
    cell.script(2, |s| {
        s.writes.extend(
            std::iter::repeat_with(|| {
                Act::Answer(WriteAck {
                    unknown_journal: true,
                    ..WriteAck::default()
                })
            })
            .take(32),
        );
    });
    cell.script(3, |s| {
        s.writes
            .extend(std::iter::repeat_with(|| Act::Hold).take(32));
        s.reads
            .extend(std::iter::repeat_with(|| Act::Hold).take(32));
    });
    let (wrote, leader) = Rig::new().run_with(&cell, &[1, 2, 3], |client| async move {
        let client = client.rotating_over(2);
        let mut writer = Writer::with_uuid(journal(), U);
        writer.claim(&client, 0, false).await;
        // Another journal's verdict taught the client node 2.
        client.observe_leader(Some(2));
        let wrote = writer.write(&client, one(1), 0).await;
        (wrote, client.leader())
    });
    assert!(matches!(wrote, WriterOutcome::Written { .. }), "{wrote:?}");
    assert_eq!(leader, Some(0), "the leader, node 1");
    assert_eq!(cell.records(journal()), one(1));
}

#[test]
fn a_write_with_no_leader_is_unavailable() {
    let cell = cell();
    let probe = cell.clone();
    let wrote = Rig::new().run_with(&cell, &[1, 2], |client| async move {
        let mut writer = Writer::with_uuid(journal(), U);
        writer.claim(&client, 0, false).await;
        probe.lead(None);
        writer.write(&client, one(1), 0).await
    });
    assert_eq!(wrote, WriterOutcome::Unavailable { leader: None });
}

#[test]
fn a_writer_truncates_under_its_own_fence() {
    let cell = cell();
    let probe = cell.clone();
    let (applied, refused, owned) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut writer = Writer::with_uuid(journal(), U);
        writer.claim(&client, 0, false).await;
        writer.write(&client, one(1), 0).await;
        writer.write(&client, one(2), 0).await;
        let applied = writer.truncate(&client, 1, 0).await;
        probe.apply(
            journal(),
            &Command::Control(Control::SetLeader {
                new: V,
                old: Some(U),
            }),
        );
        let refused = writer.truncate(&client, 2, 0).await;
        (applied, refused, writer.owned())
    });
    assert_eq!(
        applied,
        Some(TruncateOutcome::Applied {
            state: view(Some(U), 2, 1)
        })
    );
    assert_eq!(
        refused,
        Some(TruncateOutcome::Refused {
            state: view(Some(V), 2, 1)
        })
    );
    assert_eq!(owned, None, "a refused truncation supersedes the writer");
    assert_eq!(cell.records(journal()), vec![Value(vec![2])]);
}

#[test]
fn a_fresh_claim_supersedes_the_writer_s_own_term() {
    let cell = cell();
    let (first, second, uuid) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut writer = Writer::with_uuid(journal(), U);
        let first = writer.claim(&client, 0, false).await;
        let second = writer.claim(&client, 0, true).await;
        (first, second, writer.owned())
    });
    assert_eq!(
        first,
        ClaimOutcome::Won {
            state: view(Some(U), 0, 0)
        }
    );
    let uuid = uuid.expect("the fresh claim won");
    assert_ne!(uuid, U);
    assert_eq!(
        second,
        ClaimOutcome::Won {
            state: view(Some(uuid), 0, 0)
        }
    );
}

#[test]
fn a_claim_over_another_judged_leader_is_lost() {
    let cell = cell();
    cell.apply(
        journal(),
        &Command::Control(Control::SetLeader { new: V, old: None }),
    );
    let (lost, won) = Rig::new().run_with(&cell, &[1], |client| async move {
        let mut writer = Writer::with_uuid(journal(), U);
        let lost = writer.claim_over(&client, 0, None).await;
        let won = writer.claim_over(&client, 0, Some(V)).await;
        (lost, won)
    });
    assert_eq!(
        lost,
        ClaimOutcome::Lost {
            state: view(Some(V), 0, 0)
        }
    );
    assert!(matches!(won, ClaimOutcome::Won { .. }));
}
