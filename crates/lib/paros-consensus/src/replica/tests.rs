use std::collections::BTreeMap;

use super::LogRead;

mod held;
use crate::journal::state::{JournalState, Outcome, WriterMode};
use crate::journal::{Command, Control, Entry, LeaderUuid, Seq};
use crate::types::{Ballot, NodeId, Slot, Value};
use held::Held;

fn write(seq: u64, records: &[&[u8]]) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(seq),
        records: records.iter().map(|r| Value::from(r.to_vec())).collect(),
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
fn replica(commands: &[Command]) -> Held {
    let ci = commands.len().checked_sub(1).map(|i| Slot(i as u64));
    Held::boot(
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
        segment: crate::journal::Segment::default(),
    };
    assert_eq!(r.trim_to(Slot(2), base), base);
    r.learn(Slot(2), &commands[2]);
    r.advance(&mut Vec::new());
    assert_eq!(r.journal().first_seq, Seq(0));
    assert_eq!(r.read(Seq(0), 8, 64), LogRead::NotHeld);
    assert_eq!(bytes(&page(r.read(Seq(1), 8, 64))), vec![b"b".to_vec()]);
    // Once the fold passes the truncation, the gap is `Truncated`.
    r.learn(Slot(3), &commands[3]);
    r.learn(Slot(4), &commands[4]);
    r.advance(&mut Vec::new());
    assert_eq!(r.journal().first_seq, Seq(1));
    assert!(matches!(r.read(Seq(0), 8, 64), LogRead::Truncated(_)));
}

fn bytes(page: &super::LogPage) -> Vec<Vec<u8>> {
    page.records.iter().map(|v| v.to_vec()).collect()
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
    let rebooted = Held::boot(WriterMode::Single, Some(Slot(4)), first, sealed, &recs);
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
        segment: crate::journal::Segment::default(),
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
    let r = Held::boot(WriterMode::Single, Some(Slot(3)), Slot(2), base, &recs);
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
    assert!(
        p.records.is_empty(),
        "a read at the end of the log returns no record"
    );
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
    let kept: Vec<Command> = r.chosen_commands();
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
    let rebooted = Held::boot(WriterMode::Single, Some(Slot(3)), Slot(2), base, &recs);
    assert_eq!(rebooted.journal(), r.journal());
}

#[test]
fn a_hole_below_the_prefix_stops_the_fold_until_it_heals() {
    let mut recs = records(&[claim(), write(0, &[b"a"]), write(1, &[b"b"])]);
    let lost = recs.remove(&Slot(1)).expect("slot 1").1;
    let mut r = Held::boot(
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

fn truncate_to(up_to: u64, leader: u128) -> Command {
    Command::Control(Control::Truncate {
        leader: LeaderUuid(leader),
        up_to: Seq(up_to),
    })
}

/// A multi-writer `Write`: no leader, and the journal picks its position.
fn append(records: &[&[u8]]) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(0),
        seq: Seq(0),
        records: records.iter().map(|r| Value::from(r.to_vec())).collect(),
    })
}

/// An empty replica under `mode` that learns `commands` from slot 0 and
/// walks them, as a live node does. Returns the replica and the walk's
/// compaction target.
fn walked(mode: WriterMode, commands: &[Command]) -> (Held, Option<Slot>) {
    let mut r = Held::boot(
        mode,
        None,
        Slot(0),
        JournalState::default(),
        &BTreeMap::new(),
    );
    for (i, c) in commands.iter().enumerate() {
        r.learn(Slot(i as u64), c);
    }
    let target = r.advance(&mut Vec::new());
    assert_eq!(r.committed().len(), commands.len(), "the walk applied all");
    (r, target)
}

/// A retained retry above the floor keeps the record it read. Slot 3
/// retries position 0, so slot 1 (which holds position 0) stays. A caller
/// that compacts to the walk's target passes the check.
#[test]
fn a_compaction_to_the_walk_target_passes_the_check() {
    let commands = [
        claim(),
        write(0, &[b"a"]),
        write(1, &[b"b"]),
        write(0, &[b"a"]),
        truncate_to(1, 1),
    ];
    let (mut r, target) = walked(WriterMode::Single, &commands);
    assert!(matches!(
        r.outcome_at(Slot(3)),
        Some(Outcome::Duplicate { .. })
    ));
    assert_eq!(target, Some(Slot(0)), "only the claim at slot 0 may go");
    r.truncate(Slot(1));
    r.assert_compacted();
    // The check judges one walk once: a second call has nothing to judge.
    r.assert_compacted();
    assert_eq!(bytes(&page(r.read(Seq(1), 8, 64))), vec![b"b".to_vec()]);
}

/// A caller that does not compact after a walk that folded a `Truncate`
/// fails the check: the floor slot holds no record anyone needs.
#[test]
#[should_panic(expected = "a compaction leaves a needed slot at the floor")]
fn a_skipped_compaction_fails_the_check() {
    let commands = [
        claim(),
        write(0, &[b"a"]),
        write(1, &[b"b"]),
        write(0, &[b"a"]),
        truncate_to(1, 1),
    ];
    let (mut r, target) = walked(WriterMode::Single, &commands);
    assert_eq!(target, Some(Slot(0)));
    r.assert_compacted();
}

/// A walk that folded no `Truncate` is not judged, whatever the floor.
#[test]
fn a_walk_without_a_truncate_is_not_judged() {
    let (mut r, target) = walked(
        WriterMode::Single,
        &[claim(), write(0, &[b"a"]), write(1, &[b"b"])],
    );
    assert_eq!(target, None, "no truncation: nothing to compact");
    assert_eq!(r.compaction_target(), Some(Slot(0)));
    r.assert_compacted();
}

/// A multi-writer journal has no retry. A write that names a position
/// already in the journal never holds the floor down. The floor goes up to
/// the write that holds the first record. A write is found by the position
/// the journal gave it, not by the position it names.
#[test]
fn a_multi_writer_floor_follows_the_first_record_only() {
    let commands = [
        append(&[b"a"]),
        append(&[b"b"]),
        append(&[b"c"]),
        truncate_to(2, 0),
    ];
    let (mut r, target) = walked(WriterMode::Multi, &commands);
    assert_eq!(
        r.accepted_at(Seq(1)).map(|e| e.records.clone()),
        Some(vec![Value::from(b"b".to_vec())])
    );
    assert_eq!(target, Some(Slot(1)), "slots 0 and 1 hold dropped records");
    r.truncate(Slot(2));
    r.assert_compacted();
    assert_eq!(bytes(&page(r.read(Seq(2), 8, 64))), vec![b"c".to_vec()]);
    assert!(matches!(r.read(Seq(1), 8, 64), LogRead::Truncated(s) if s.first_seq == Seq(2)));
}

/// A trim-point jump below a fold that already passed the point keeps the
/// node's own journal state. The peer's sealed state is not used. The new
/// base is the node's own state at the point, and only the prefix below
/// the point goes.
#[test]
fn a_trim_point_below_the_fold_keeps_the_own_state() {
    let commands = [
        claim(),
        write(0, &[b"a"]),
        write(1, &[b"b"]),
        write(2, &[b"c"]),
    ];
    let mut r = replica(&commands);
    let head = r.journal();
    let peer = JournalState {
        leader: Some(LeaderUuid(9)),
        term: 7,
        next_seq: Seq(50),
        first_seq: Seq(40),
        segment: crate::journal::Segment::default(),
    };
    let base = r.trim_to(Slot(2), peer);
    assert_eq!(
        base,
        replica(&commands[..2]).journal(),
        "the own state at 2"
    );
    assert_eq!(r.journal_base(), base);
    assert_eq!(r.journal(), head, "the fold's head does not move");
    assert_eq!(r.folded(), Slot(4));
    assert_eq!(r.chosen_index(), Some(Slot(3)));
    assert_eq!(r.chosen().iter().next(), Some(&Slot(2)));
    assert_eq!(r.read(Seq(0), 8, 64), LogRead::NotHeld);
    assert_eq!(
        bytes(&page(r.read(Seq(1), 8, 64))),
        vec![b"b".to_vec(), b"c".to_vec()]
    );
}

/// The byte budget of a read counts across writes. The page stops before
/// the record that goes over the budget. The record limit stops a page at
/// a write boundary too.
#[test]
fn a_read_budget_spans_several_writes() {
    let r = replica(&[
        claim(),
        write(0, &[b"aa"]),
        write(1, &[b"bb"]),
        write(2, &[b"cc"]),
    ]);
    let p = page(r.read(Seq(0), 64, 4));
    assert_eq!(bytes(&p), vec![b"aa".to_vec(), b"bb".to_vec()]);
    assert_eq!(p.next(), Seq(2));
    let p = page(r.read(Seq(0), 64, 5));
    assert_eq!(p.records.len(), 2, "a third record would need 6 bytes");
    let p = page(r.read(Seq(1), 1, 64));
    assert_eq!(bytes(&p), vec![b"bb".to_vec()]);
}

/// A floor left on a slot that holds no record fails the check, even when
/// the journal's first record sits right at the base: slot 1 holds the
/// first `Truncate` and no record, and slot 2 holds position 1, the first
/// record. The floor may go up to slot 2, so slot 1 is not needed.
#[test]
#[should_panic(expected = "a compaction leaves a needed slot at the floor")]
fn a_floor_on_a_slot_without_records_fails_the_check() {
    let commands = [
        append(&[b"a"]),
        truncate_to(1, 0),
        append(&[b"b"]),
        truncate_to(1, 0),
    ];
    let (mut r, target) = walked(WriterMode::Multi, &commands);
    assert_eq!(target, Some(Slot(1)), "slot 2 holds the first record");
    r.truncate(Slot(1));
    r.assert_compacted();
}

/// A refused write is never a retained retry: a foreign writer's write at
/// the journal's next position reads no record, so it holds no floor down.
#[test]
fn a_refused_write_at_the_next_position_holds_no_floor() {
    let foreign = Command::Write(Entry {
        leader: LeaderUuid(2),
        seq: Seq(2),
        records: vec![Value::from(b"x".to_vec())],
    });
    let commands = [
        claim(),
        write(0, &[b"a"]),
        write(1, &[b"b"]),
        foreign,
        truncate_to(1, 1),
    ];
    let (r, target) = walked(WriterMode::Single, &commands);
    assert!(
        !matches!(r.outcome_at(Slot(3)), Some(Outcome::Accepted { .. })),
        "{:?}",
        r.outcome_at(Slot(3))
    );
    assert_eq!(target, Some(Slot(1)), "slot 2 holds the first record");
    assert_eq!(r.compaction_target(), target);
}

/// The fold never stops at a slot it holds chosen below the first unchosen
/// slot: `assert_invariants` names a fold left behind.
#[test]
#[should_panic(expected = "the journal fold stops only at a slot it does not hold")]
fn a_fold_left_behind_fails_the_invariants() {
    let mut r = replica(&[claim(), write(0, &[b"a"]), write(1, &[b"b"])]);
    r.folded = Slot(1);
    r.assert_invariants(Slot(0));
}

/// The replica reads a chosen command from the records its deployment
/// holds, and keeps no copy: a slot it does not know chosen reads as
/// nothing, even when the records hold a command there.
#[test]
fn a_chosen_command_is_read_from_the_records_and_only_at_a_chosen_slot() {
    let mut r = replica(&[claim(), write(0, &[b"a"])]);
    assert_eq!(r.chosen_at(Slot(1)), Some(&write(0, &[b"a"])));
    assert_eq!(r.chosen_at(Slot(2)), None, "slot 2 is not chosen");
    let mut log = records(&[claim(), write(0, &[b"a"])]);
    log.insert(Slot(2), (Ballot::zero(), write(1, &[b"b"])));
    assert_eq!(
        r.replica.chosen_at(&log, Slot(2)),
        None,
        "an accepted record that is not chosen is not read"
    );
    r.learn(Slot(2), &write(1, &[b"b"]));
    let _ = r.advance(&mut Vec::new());
    assert_eq!(r.chosen_at(Slot(2)), Some(&write(1, &[b"b"])));
    assert_eq!(
        r.accepted_at(Seq(1)).map(|e| e.records.clone()),
        Some(vec![Value::from(b"b")])
    );
    assert_eq!(r.chosen_commands().len(), 3);
}

/// A learn whose record the deployment did not write first breaks the
/// coupling of a chosen slot and its record.
#[test]
#[should_panic(expected = "a learned slot's record carries the learned command")]
fn a_learn_with_no_record_is_refused() {
    let mut r = replica(&[claim()]);
    let log = records(&[claim()]);
    r.replica.learn(&log, Slot(1), &write(0, &[b"a"]));
}

/// A learn whose record holds another command is refused the same way.
#[test]
#[should_panic(expected = "a learned slot's record carries the learned command")]
fn a_learn_over_another_record_is_refused() {
    let mut r = replica(&[claim()]);
    let log = records(&[claim(), write(0, &[b"x"])]);
    r.replica.learn(&log, Slot(1), &write(0, &[b"a"]));
}

/// A store that lost the record of a slot the replica knows chosen is a
/// broken deployment, never a missing value.
#[test]
#[should_panic(expected = "a slot known chosen holds its record in the chosen log")]
fn a_chosen_slot_with_no_record_fails_its_read() {
    let r = replica(&[claim(), write(0, &[b"a"])]);
    let log = records(&[claim()]);
    let _ = r.replica.chosen_at(&log, Slot(1));
}

/// A trim-point jump past a `Roll` carries the tail segment in its state,
/// so the jumped fold judges the next `Roll` as a node that walked the
/// whole log does: the same end for each segment.
#[test]
fn a_jump_past_a_roll_carries_the_tail_segment() {
    use crate::journal::{Outcome, Segment, SegmentId};
    let roll = |next| {
        Command::Control(Control::Roll {
            next: SegmentId(next),
            replicas: None,
        })
    };
    let commands = [claim(), roll(1), write(0, &[b"a"]), roll(2), roll(2)];
    let walked = replica(&commands);
    let at_three = replica(&commands[..3]).journal();
    assert_eq!(
        at_three.segment,
        Segment {
            id: SegmentId(1),
            start: Slot(2),
        }
    );
    let mut r = replica(&commands[..1]);
    assert_eq!(r.trim_to(Slot(3), at_three), at_three);
    r.learn(Slot(3), &commands[3]);
    r.learn(Slot(4), &commands[4]);
    r.advance(&mut Vec::new());
    let two = Segment {
        id: SegmentId(2),
        start: Slot(4),
    };
    assert_eq!(r.outcome_at(Slot(3)), Some(&Outcome::Rolled(two)));
    assert_eq!(r.outcome_at(Slot(4)), Some(&Outcome::RollIgnored(two)));
    assert_eq!(r.journal(), walked.journal());
}
