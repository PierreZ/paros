//! The client's policy loops over fake nodes on a simulated network: where
//! a write goes after a redirect, a busy leader or a node that does not
//! host the journal; which server a read asks; how an ambiguous write
//! settles; and what a claim, a `SetLeader` and a truncation answer.

use std::time::Duration;

use paros_core::Value;
use paros_core::journal::{Command, Control, JournalView, LeaderUuid, Seq, WriterMode};
use paros_wire::{ReadAck, SetLeaderAck, Write, WriteAck, leader_uuid_to_proto};

use crate::rig::cell::{Act, Cell, Heard, ids};
use crate::rig::{self, Rig};
use crate::{
    ClaimOutcome, ReadOutcome, Resolution, Retarget, SetLeaderOutcome, TruncateOutcome,
    WriteOptions, WriteOutcome,
};

const U: LeaderUuid = LeaderUuid(11);
const V: LeaderUuid = LeaderUuid(22);

fn journal() -> paros_core::journal::JournalIdentifier {
    ids(1, 2)
}

/// A write of `bytes` at `seq` under `leader`.
fn write(leader: LeaderUuid, seq: u64, bytes: &[&[u8]]) -> Write {
    Write {
        journal: journal().journal.0,
        tenant: journal().tenant.0,
        leader: Some(leader_uuid_to_proto(leader)),
        seq,
        records: bytes.iter().map(|b| b.to_vec()).collect(),
    }
}

/// A cell of `count` nodes led by node `leader`, with the journal created
/// and led by `U`.
fn cell_led(leader: u64) -> Cell {
    let cell = Cell::led_by(leader);
    cell.create(journal(), WriterMode::Single);
    cell.apply(
        journal(),
        &Command::Control(Control::SetLeader { new: U, old: None }),
    );
    cell
}

fn busy() -> Act<WriteAck> {
    Act::Answer(WriteAck {
        busy: true,
        ..WriteAck::default()
    })
}

fn unknown() -> Act<WriteAck> {
    Act::Answer(WriteAck {
        unknown_journal: true,
        ..WriteAck::default()
    })
}

fn view(leader: Option<LeaderUuid>, next: u64, first: u64) -> JournalView {
    JournalView {
        leader,
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

#[test]
fn a_write_follows_a_redirect_to_the_leader() {
    let mut world = Rig::new();
    let cell = cell_led(2);
    let report = world.run_with(&cell, &[1, 2, 3], |client| async move {
        let report = client
            .write(&write(U, 0, &[b"a"]), 0, WriteOptions::default())
            .await;
        (report, client.leader())
    });
    let (report, leader) = report;
    assert_eq!(
        report.outcome,
        WriteOutcome::Written {
            seq: 0,
            count: 1,
            duplicate: false
        }
    );
    assert_eq!(report.server, 1, "the leader, node 2");
    assert_eq!(report.redirects, 1);
    assert_eq!(leader, Some(1), "the verdict teaches the leader");
    assert_eq!(cell.writes_heard(), vec![1, 2]);
    assert_eq!(cell.records(journal()), vec![Value(b"a".to_vec())]);
}

#[test]
fn a_write_stops_at_a_redirect_when_asked_to() {
    let mut world = Rig::new();
    let cell = cell_led(2);
    let (report, leader) = world.run_with(&cell, &[1, 2], |client| async move {
        let options = WriteOptions {
            stop_at_redirect: true,
            ..WriteOptions::default()
        };
        let report = client.write(&write(U, 0, &[b"a"]), 0, options).await;
        (report, client.leader())
    });
    assert_eq!(report.outcome, WriteOutcome::Redirect { leader: Some(2) });
    assert_eq!(report.server, 0);
    assert_eq!(report.redirects, 0);
    assert_eq!(leader, Some(1), "the redirect teaches the leader");
    assert!(cell.records(journal()).is_empty());
}

#[test]
fn a_write_gives_up_after_the_redirect_limit() {
    let mut world = Rig::new();
    let cell = cell_led(2);
    cell.lead(None);
    let report = world.run_with(&cell, &[1, 2], |client| async move {
        client
            .write(&write(U, 0, &[b"a"]), 0, WriteOptions::default())
            .await
    });
    let limit = rig::tunables().redirect_limit;
    assert_eq!(report.outcome, WriteOutcome::Redirect { leader: None });
    assert_eq!(report.redirects, limit);
    assert_eq!(cell.writes_heard().len(), limit as usize + 1);
    assert_eq!(
        cell.writes_heard()[..4],
        [1, 2, 1, 2],
        "a redirect naming none moves to the next server"
    );
}

#[test]
fn a_busy_leader_is_asked_again_until_it_admits_the_write() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    cell.script(1, |s| s.writes.extend([busy(), busy()]));
    let report = world.run_with(&cell, &[1, 2], |client| async move {
        client
            .write(&write(U, 0, &[b"a"]), 0, WriteOptions::default())
            .await
    });
    assert!(matches!(
        report.outcome,
        WriteOutcome::Written { seq: 0, .. }
    ));
    assert_eq!(report.redirects, 0, "busy is no redirect");
    assert_eq!(
        cell.writes_heard(),
        vec![1, 1, 1],
        "the same leader each time"
    );
}

/// A leader busy to the deadline: the loop runs out at its top, so the
/// write comes back ambiguous, not busy, and a writer settles it with
/// `resolve` (it moved nothing, so the settlement finds it not written or
/// writes it).
#[test]
fn a_leader_busy_to_the_deadline_leaves_the_write_ambiguous() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    cell.script(1, |s| s.writes.extend((0..1000).map(|_| busy())));
    let report = world.run_with(&cell, &[1], |client| async move {
        client
            .write(&write(U, 0, &[b"a"]), 0, WriteOptions::default())
            .await
    });
    assert_eq!(report.outcome, WriteOutcome::Ambiguous);
    assert!(
        cell.writes_heard().len() > 2,
        "asked again until the deadline"
    );
    assert!(cell.records(journal()).is_empty());
}

#[test]
fn a_write_asks_past_a_server_that_does_not_host_the_journal() {
    let mut world = Rig::new();
    let cell = cell_led(3);
    cell.script(1, |s| s.writes.push_back(unknown()));
    cell.script(2, |s| s.writes.push_back(unknown()));
    let report = world.run_with(&cell, &[1, 2, 3], |client| async move {
        client
            .write(&write(U, 0, &[b"a"]), 0, WriteOptions::default())
            .await
    });
    assert!(matches!(report.outcome, WriteOutcome::Written { .. }));
    assert_eq!(report.server, 2);
    assert_eq!(cell.writes_heard(), vec![1, 2, 3]);
}

#[test]
fn a_write_that_no_server_hosts_is_an_unknown_journal() {
    let mut world = Rig::new();
    let cell = Cell::led_by(1);
    let report = world.run_with(&cell, &[1, 2], |client| async move {
        client
            .write(&write(U, 0, &[b"a"]), 0, WriteOptions::default())
            .await
    });
    assert_eq!(report.outcome, WriteOutcome::UnknownJournal);
    assert_eq!(cell.writes_heard(), vec![1, 2], "each server once");
}

#[test]
fn a_silent_leader_is_forgotten() {
    let mut world = Rig::new();
    let cell = cell_led(2);
    cell.script(2, |s| s.writes.push_back(Act::Hold));
    let (report, leader) = world.run_with(&cell, &[1, 2], |client| async move {
        client.observe_leader(Some(2));
        let report = client
            .write(&write(U, 0, &[b"a"]), 1, WriteOptions::default())
            .await;
        (report, client.leader())
    });
    assert_eq!(report.outcome, WriteOutcome::Ambiguous);
    assert_eq!(report.server, 1);
    assert_eq!(leader, None, "no answer from the believed leader drops it");
}

#[test]
fn an_abandoned_first_attempt_is_ambiguous_and_may_land() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    cell.script(1, |s| s.writes.push_back(Act::Lose));
    let report = world.run_with(&cell, &[1], |client| async move {
        let options = WriteOptions {
            abandon_first_after: Some(Duration::from_millis(50)),
            ..WriteOptions::default()
        };
        client.write(&write(U, 0, &[b"a"]), 0, options).await
    });
    assert_eq!(report.outcome, WriteOutcome::Ambiguous);
    assert_eq!(cell.records(journal()), vec![Value(b"a".to_vec())]);
}

#[test]
fn a_read_moves_past_a_server_that_does_not_answer_and_starts_after_it_next() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    cell.script(1, |s| s.reads.push_back(Act::Hold));
    let (first, second, silent) = world.run_with(&cell, &[1, 2], |client| async move {
        let read = crate::state_read(journal());
        let first = client.read_any(&read, 0).await;
        let silent = client.hint().silent;
        let second = client.read_any(&read, 0).await;
        (first, second, silent)
    });
    assert!(first.outcome.is_served());
    assert_eq!((first.server, first.attempts), (1, 2));
    assert_eq!(silent, Some(0));
    assert_eq!(
        (second.server, second.attempts),
        (1, 1),
        "the silent server is asked last"
    );
    assert_eq!(second.outcome.state(), Some(view(Some(U), 0, 0)));
}

#[test]
fn a_read_names_an_unknown_journal_only_when_every_server_does() {
    let mut world = Rig::new();
    let cell = Cell::led_by(1);
    let unserved = || {
        Act::Answer(ReadAck {
            served: false,
            ..ReadAck::default()
        })
    };
    cell.script(2, |s| s.reads.push_back(unserved()));
    let (mixed, unknown) = world.run_with(&cell, &[1, 2], |client| async move {
        let read = crate::state_read(journal());
        let mixed = client.read_any(&read, 0).await;
        let unknown = client.read_any(&read, 0).await;
        (mixed, unknown)
    });
    assert_eq!(
        mixed.outcome,
        ReadOutcome::Unserved,
        "one server did not say"
    );
    assert_eq!(mixed.attempts, 2);
    assert_eq!(unknown.outcome, ReadOutcome::UnknownJournal);
    assert_eq!(unknown.attempts, 2);
}

#[test]
fn a_read_back_proves_a_lost_write_written_from_where_its_uuid_led() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    cell.script(1, |s| s.writes.push_back(Act::Lose));
    let report = world.run_with(&cell, &[1], |client| async move {
        let request = write(U, 0, &[b"a", b"b"]);
        let first = client.write(&request, 0, WriteOptions::default()).await;
        assert_eq!(first.outcome, WriteOutcome::Ambiguous);
        client
            .resolve(&request, 0, Retarget::FollowHint, Some(0))
            .await
    });
    assert_eq!(report.resolution, Resolution::Written { seq: 0, count: 2 });
    assert!(report.by_read_back);
    assert_eq!(cell.writes_heard(), vec![1], "no re-send");
}

#[test]
fn without_a_lead_position_a_lost_write_settles_by_its_re_send() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    cell.apply(
        journal(),
        &Command::Write(paros_core::journal::Entry {
            leader: U,
            seq: Seq(0),
            records: vec![Value(b"a".to_vec())],
        }),
    );
    let report = world.run_with(&cell, &[1], |client| async move {
        client
            .resolve(&write(U, 0, &[b"a"]), 0, Retarget::FollowHint, None)
            .await
    });
    assert_eq!(report.resolution, Resolution::Written { seq: 0, count: 1 });
    assert!(!report.by_read_back, "the re-send's duplicate settles it");
    assert_eq!(cell.writes_heard(), vec![1]);
}

#[test]
fn a_write_another_leader_fenced_before_its_position_is_not_written() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    cell.apply(
        journal(),
        &Command::Control(Control::SetLeader {
            new: V,
            old: Some(U),
        }),
    );
    let report = world.run_with(&cell, &[1], |client| async move {
        client
            .resolve(&write(U, 0, &[b"a"]), 0, Retarget::FollowHint, Some(0))
            .await
    });
    assert_eq!(
        report.resolution,
        Resolution::NotWritten {
            state: view(Some(V), 0, 0)
        }
    );
    assert!(report.by_read_back);
    assert!(cell.writes_heard().is_empty());
}

#[test]
fn a_write_below_the_floor_is_truncated() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    let one = |seq| {
        Command::Write(paros_core::journal::Entry {
            leader: U,
            seq: Seq(seq),
            records: vec![Value(vec![1])],
        })
    };
    cell.apply(journal(), &one(0));
    cell.apply(journal(), &one(1));
    cell.apply(
        journal(),
        &Command::Control(Control::Truncate {
            leader: U,
            up_to: Seq(1),
        }),
    );
    let report = world.run_with(&cell, &[1], |client| async move {
        client
            .resolve(&write(U, 0, &[&[1]]), 0, Retarget::FollowHint, Some(0))
            .await
    });
    assert_eq!(
        report.resolution,
        Resolution::Truncated {
            state: view(Some(U), 2, 1)
        }
    );
    assert!(report.by_read_back);
}

#[test]
fn a_write_ahead_of_the_tail_under_its_leader_stays_unresolved() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    let report = world.run_with(&cell, &[1], |client| async move {
        client
            .resolve(&write(U, 5, &[b"a"]), 0, Retarget::FollowHint, Some(0))
            .await
    });
    assert_eq!(report.resolution, Resolution::Unresolved);
    assert_eq!(
        cell.writes_heard(),
        vec![1],
        "refused once: it may still land"
    );
}

#[test]
fn a_write_whose_position_another_write_took_is_not_written() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    cell.apply(
        journal(),
        &Command::Write(paros_core::journal::Entry {
            leader: U,
            seq: Seq(0),
            records: vec![Value(b"other".to_vec())],
        }),
    );
    let report = world.run_with(&cell, &[1], |client| async move {
        client
            .resolve(&write(U, 0, &[b"mine"]), 0, Retarget::FollowHint, Some(0))
            .await
    });
    assert_eq!(
        report.resolution,
        Resolution::NotWritten {
            state: view(Some(U), 1, 0)
        }
    );
    assert!(!report.by_read_back, "the bytes differ: the re-send judges");
}

#[test]
fn a_write_no_server_answers_stays_unresolved_after_its_budget() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    let budget = rig::tunables().retry_budget as usize;
    cell.script(1, |s| {
        s.writes.extend((0..budget).map(|_| Act::Hold));
        s.reads.extend((0..=budget).map(|_| Act::Hold));
    });
    let report = world.run_with(&cell, &[1], |client| async move {
        client
            .resolve(&write(U, 0, &[b"a"]), 0, Retarget::FollowHint, Some(0))
            .await
    });
    assert_eq!(report.resolution, Resolution::Unresolved);
    assert_eq!(cell.writes_heard().len(), budget, "one re-send per budget");
}

#[test]
fn a_claim_wins_an_unled_journal_and_owns_it_after() {
    let mut world = Rig::new();
    let cell = Cell::led_by(2);
    cell.create(journal(), WriterMode::Single);
    let (won, owned, lost, over) = world.run_with(&cell, &[1, 2], |client| async move {
        let won = client.claim(journal(), U, 0).await;
        let owned = client.claim(journal(), U, 0).await;
        let lost = client.reclaim(journal(), V, 0).await;
        let over = client.claim_over(journal(), V, None, 0).await;
        (won, owned, lost, over)
    });
    let led = view(Some(U), 0, 0);
    assert_eq!(won, ClaimOutcome::Won { state: led });
    assert_eq!(owned, ClaimOutcome::Owned { state: led });
    assert_eq!(lost, ClaimOutcome::Lost { state: led });
    assert_eq!(over, ClaimOutcome::Lost { state: led });
    let set_leaders: Vec<u64> = cell
        .heard()
        .into_iter()
        .filter_map(|(node, call)| matches!(call, Heard::SetLeader(_)).then_some(node))
        .collect();
    assert_eq!(
        set_leaders,
        vec![1, 2],
        "one SetLeader, redirected once; the others send none"
    );
}

#[test]
fn a_claim_over_the_judged_leader_replaces_it() {
    let mut world = Rig::new();
    let cell = cell_led(1);
    let won = world.run_with(&cell, &[1], |client| async move {
        client.claim_over(journal(), V, Some(U), 0).await
    });
    assert_eq!(
        won,
        ClaimOutcome::Won {
            state: view(Some(V), 0, 0)
        }
    );
}

#[test]
fn a_claim_that_reads_nothing_sends_no_set_leader() {
    let mut world = Rig::new();
    let cell = Cell::led_by(1);
    cell.script(1, |s| s.reads.push_back(Act::Hold));
    let (unread, unknown) = world.run_with(&cell, &[1], |client| async move {
        let unread = client.claim(journal(), U, 0).await;
        let unknown = client.claim(journal(), U, 0).await;
        (unread, unknown)
    });
    assert_eq!(unread, ClaimOutcome::Unread);
    assert_eq!(unknown, ClaimOutcome::UnknownJournal);
    assert!(
        cell.heard()
            .iter()
            .all(|(_, call)| matches!(call, Heard::Read(_)))
    );
}

#[test]
fn a_set_leader_redirected_to_none_asks_the_next_server() {
    let mut world = Rig::new();
    let cell = Cell::led_by(2);
    cell.create(journal(), WriterMode::Single);
    cell.script(1, |s| {
        s.set_leaders
            .push_back(Act::Answer(SetLeaderAck::default()));
    });
    let (outcome, leader) = world.run_with(&cell, &[1, 2], |client| async move {
        let outcome = client.set_leader(journal(), U, None, 0).await;
        (outcome, client.leader())
    });
    assert_eq!(
        outcome,
        SetLeaderOutcome::Won {
            state: view(Some(U), 0, 0)
        }
    );
    assert_eq!(leader, Some(1));
}

#[test]
fn a_set_leader_gives_up_after_the_redirect_limit() {
    let mut world = Rig::new();
    let cell = Cell::led_by(2);
    cell.create(journal(), WriterMode::Single);
    cell.lead(None);
    let outcome = world.run_with(&cell, &[1, 2], |client| async move {
        client.set_leader(journal(), U, None, 0).await
    });
    assert_eq!(outcome, SetLeaderOutcome::Redirect { leader: None });
    let asked = cell
        .heard()
        .iter()
        .filter(|(_, call)| matches!(call, Heard::SetLeader(_)))
        .count();
    assert_eq!(asked, rig::tunables().redirect_limit as usize + 1);
}

#[test]
fn a_truncation_follows_the_redirect_and_is_fenced() {
    let mut world = Rig::new();
    let cell = cell_led(2);
    let (applied, refused) = world.run_with(&cell, &[1, 2], |client| async move {
        let truncate = |leader| paros_wire::Truncate {
            journal: journal().journal.0,
            tenant: journal().tenant.0,
            up_to: 0,
            leader: Some(leader_uuid_to_proto(leader)),
        };
        let applied = client.truncate(&truncate(U), 0).await;
        let refused = client.truncate(&truncate(V), 0).await;
        (applied, refused)
    });
    let led = view(Some(U), 0, 0);
    assert_eq!(applied, TruncateOutcome::Applied { state: led });
    assert_eq!(refused, TruncateOutcome::Refused { state: led });
}

#[test]
fn a_truncation_with_no_leader_gives_up_after_the_redirect_limit() {
    let mut world = Rig::new();
    let cell = cell_led(2);
    cell.lead(None);
    let outcome = world.run_with(&cell, &[1, 2], |client| async move {
        let request = paros_wire::Truncate {
            journal: journal().journal.0,
            tenant: journal().tenant.0,
            up_to: 0,
            leader: Some(leader_uuid_to_proto(U)),
        };
        client.truncate(&request, 0).await
    });
    assert_eq!(outcome, TruncateOutcome::Ambiguous);
    let asked = cell
        .heard()
        .iter()
        .filter(|(_, call)| matches!(call, Heard::Truncate(_)))
        .count();
    assert_eq!(asked, rig::tunables().redirect_limit as usize);
}
