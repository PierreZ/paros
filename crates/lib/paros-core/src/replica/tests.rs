use std::collections::BTreeMap;

use super::{LogRead, Replica};
use crate::journal::state::{JournalState, Outcome, WriterMode};
use crate::journal::{Command, Control, Entry, LeaderUuid, Seq};
use crate::types::{Ballot, NodeId, Slot, Value};

fn write(seq: u64, records: &[&[u8]]) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(seq),
        records: records.iter().map(|r| Value(r.to_vec())).collect(),
    })
}

fn claim() -> Command {
    Command::Control(Control::SetLeader {
        new: LeaderUuid(1),
        old: None,
    })
}

fn records(commands: &[Command]) -> BTreeMap<Slot, (Ballot, Command)> {
    let ballot = Ballot {
        round: 1,
        node: NodeId(1),
    };
    commands
        .iter()
        .enumerate()
        .map(|(i, c)| (Slot(i as u64), (ballot, c.clone())))
        .collect()
}

/// A replica whose chosen prefix is `commands` from slot 0.
fn replica(commands: &[Command]) -> Replica {
    let ci = commands.len().checked_sub(1).map(|i| Slot(i as u64));
    Replica::from_boot(
        WriterMode::Single,
        ci,
        Slot(0),
        JournalState::default(),
        &records(commands),
    )
}

fn page(read: LogRead) -> super::LogPage {
    match read {
        LogRead::Page(page) => page,
        LogRead::Truncated(_) | LogRead::NotHeld => panic!("expected a page"),
    }
}

/// A trim-point jump whose base still counts a record below the floor
/// (the truncation that let the peer's floor rise lies above it, unfolded
/// here) serves a read there as not held — never a page, never
/// `Truncated` — and serves every position at or past the floor.
#[test]
fn a_jumped_fold_behind_its_truncation_does_not_hold_the_base_records() {
    let commands = [
        claim(),
        write(0, &[b"a"]),
        write(1, &[b"b"]),
        write(2, &[b"c"]),
        Command::Control(Control::Truncate {
            leader: LeaderUuid(1),
            up_to: Seq(1),
        }),
    ];
    let mut r = replica(&commands[..1]);
    let base = JournalState {
        leader: Some(LeaderUuid(1)),
        term: 1,
        next_seq: Seq(1),
        first_seq: Seq(0),
    };
    assert_eq!(r.trim_to(Slot(2), base), base);
    r.learn(Slot(2), &commands[2]);
    r.advance(|_, _| Some(Ballot::zero()), &mut Vec::new());
    assert_eq!(r.journal().first_seq, Seq(0));
    assert_eq!(r.read(Seq(0), 8, 64), LogRead::NotHeld);
    assert_eq!(bytes(&page(r.read(Seq(1), 8, 64))), vec![b"b".to_vec()]);
    // Once the fold passes the truncation, the gap is `Truncated`.
    r.learn(Slot(3), &commands[3]);
    r.learn(Slot(4), &commands[4]);
    r.advance(|_, _| Some(Ballot::zero()), &mut Vec::new());
    assert_eq!(r.journal().first_seq, Seq(1));
    assert!(matches!(r.read(Seq(0), 8, 64), LogRead::Truncated(_)));
}

fn bytes(page: &super::LogPage) -> Vec<Vec<u8>> {
    page.records.iter().map(|v| v.0.clone()).collect()
}

/// A floor keeps every record a retained slot's verdict read, not only
/// the journal's first retained one: a node that refolds from the floor
/// judges a retry above it exactly as before.
#[test]
fn a_floor_keeps_every_record_a_retained_slot_compares_against() {
    let commands = [
        claim(),
        write(0, &[b"a"]),
        write(1, &[b"b"]),
        write(0, &[b"a"]), // a retry of position 0
        Command::Control(Control::Truncate {
            leader: LeaderUuid(1),
            up_to: Seq(1),
        }),
    ];
    let mut r = replica(&commands);
    assert!(matches!(
        r.outcome_at(Slot(3)),
        Some(Outcome::Duplicate { .. })
    ));
    // The first retained record (position 1) sits at slot 2, but slot
    // 3's verdict read position 0 at slot 1: the floor stops at slot 1.
    assert_eq!(r.compaction_target(), Some(Slot(0)));
    let first = Slot(1);
    let sealed = r.truncate(first);
    let mut recs = records(&commands);
    recs.retain(|slot, _| *slot >= first);
    let rebooted = Replica::from_boot(WriterMode::Single, Some(Slot(4)), first, sealed, &recs);
    assert!(matches!(
        rebooted.outcome_at(Slot(3)),
        Some(Outcome::Duplicate { .. })
    ));
    assert_eq!(rebooted.journal(), r.journal());
}

#[test]
fn a_truncation_below_a_jumped_floor_keeps_the_floor() {
    // A node that jumped to slot 2 holds nothing below it: positions 0
    // and 1 lie in slots it never saw. A `Truncate` to position 1 folded
    // after the jump names a record that is already gone here.
    let base = JournalState {
        leader: Some(LeaderUuid(1)),
        term: 1,
        next_seq: Seq(2),
        first_seq: Seq(0),
    };
    let commands = [
        claim(),
        write(0, &[b"a"]),
        write(2, &[b"c"]),
        Command::Control(Control::Truncate {
            leader: LeaderUuid(1),
            up_to: Seq(1),
        }),
    ];
    let mut recs = records(&commands);
    recs.retain(|slot, _| *slot >= Slot(2));
    let r = Replica::from_boot(WriterMode::Single, Some(Slot(3)), Slot(2), base, &recs);
    assert_eq!(r.journal().first_seq, Seq(1));
    assert_eq!(r.compaction_target(), Some(Slot(1)), "the floor stays");
}

#[test]
fn a_read_is_dense_by_position_and_skips_every_hole() {
    let r = replica(&[
        claim(),
        write(0, &[b"a", b"b"]),
        Command::Control(Control::Noop),
        write(0, &[b"a", b"b"]), // a retry: acked, no position
        write(9, &[b"gap"]),     // refused: no position
        write(2, &[b"c"]),
    ]);
    let p = page(r.read(Seq(0), 64, 1024));
    assert_eq!(bytes(&p), vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(p.next(), Seq(3));
    assert_eq!(p.state.next_seq, Seq(3));
    assert!(matches!(
        r.outcome_at(Slot(3)),
        Some(Outcome::Duplicate { .. })
    ));
    assert!(matches!(r.outcome_at(Slot(4)), Some(Outcome::Refused(_))));
}

#[test]
fn a_read_may_start_inside_a_batch_and_is_bounded() {
    let r = replica(&[claim(), write(0, &[b"xxxx", b"yyyy", b"z"])]);
    let p = page(r.read(Seq(1), 64, 1024));
    assert_eq!(bytes(&p), vec![b"yyyy".to_vec(), b"z".to_vec()]);
    let p = page(r.read(Seq(0), 64, 1));
    assert_eq!(p.records.len(), 1, "a page always carries one record");
    let p = page(r.read(Seq(0), 2, 1024));
    assert_eq!(p.next(), Seq(2));
}

#[test]
fn a_read_at_the_end_is_empty_and_below_first_seq_is_truncated() {
    let r = replica(&[
        claim(),
        write(0, &[b"a"]),
        write(1, &[b"b"]),
        Command::Control(Control::Truncate {
            leader: LeaderUuid(1),
            up_to: Seq(1),
        }),
    ]);
    let p = page(r.read(Seq(2), 64, 64));
    assert!(p.records.is_empty());
    let p = page(r.read(Seq(7), 64, 64));
    assert_eq!(p.next(), Seq(7));
    assert!(matches!(r.read(Seq(0), 64, 64), LogRead::Truncated(s) if s.first_seq == Seq(1)));
}

#[test]
fn truncation_keeps_the_write_holding_the_first_record_and_seals_its_base() {
    let mut r = replica(&[
        claim(),
        write(0, &[b"a"]),
        write(1, &[b"b", b"c"]),
        Command::Control(Control::Truncate {
            leader: LeaderUuid(1),
            up_to: Seq(2),
        }),
    ]);
    // Slot 2 holds position 2: everything below it may go.
    assert_eq!(r.compaction_target(), Some(Slot(1)));
    let base = r.truncate(Slot(2));
    assert_eq!(base.next_seq, Seq(1));
    assert_eq!(base.term, 1);
    let p = page(r.read(Seq(2), 64, 64));
    assert_eq!(bytes(&p), vec![b"c".to_vec()]);
    // A reboot from the sealed base folds to the same head.
    let kept: Vec<Command> = r.chosen().values().cloned().collect();
    let mut recs = BTreeMap::new();
    for (i, c) in kept.iter().enumerate() {
        recs.insert(
            Slot(2 + i as u64),
            (
                Ballot {
                    round: 1,
                    node: NodeId(1),
                },
                c.clone(),
            ),
        );
    }
    let rebooted = Replica::from_boot(WriterMode::Single, Some(Slot(3)), Slot(2), base, &recs);
    assert_eq!(rebooted.journal(), r.journal());
}

#[test]
fn a_hole_below_the_prefix_stops_the_fold_until_it_heals() {
    let mut recs = records(&[claim(), write(0, &[b"a"]), write(1, &[b"b"])]);
    let lost = recs.remove(&Slot(1)).expect("slot 1").1;
    let mut r = Replica::from_boot(
        WriterMode::Single,
        Some(Slot(2)),
        Slot(0),
        JournalState::default(),
        &recs,
    );
    assert_eq!(r.fold_hole(), Some(Slot(1)));
    assert!(!r.covers(Some(Slot(1))));
    r.learn(Slot(1), &lost);
    assert_eq!(r.fold_hole(), None);
    assert_eq!(r.journal().next_seq, Seq(2));
}
