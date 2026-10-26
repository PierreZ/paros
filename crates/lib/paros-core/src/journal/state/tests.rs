use super::{JournalState, Outcome, WriterMode};
use crate::journal::{Command, Control, Entry, LeaderUuid, Seq};
use crate::types::Value;

fn write(leader: u128, seq: u64, records: &[&[u8]]) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(leader),
        seq: Seq(seq),
        records: records.iter().map(|r| Value(r.to_vec())).collect(),
    })
}

fn truncate(leader: u128, up_to: u64) -> Command {
    Command::Control(Control::Truncate {
        leader: LeaderUuid(leader),
        up_to: Seq(up_to),
    })
}

fn set_leader(new: u128, old: Option<u128>) -> Command {
    Command::Control(Control::SetLeader {
        new: LeaderUuid(new),
        old: old.map(LeaderUuid),
    })
}

/// Apply `commands` in order against a log that remembers each accepted
/// write, as the replica's positions index does.
fn fold(commands: &[Command]) -> (JournalState, Vec<Outcome>) {
    fold_in(WriterMode::Single, commands)
}

fn fold_in(mode: WriterMode, commands: &[Command]) -> (JournalState, Vec<Outcome>) {
    let mut state = JournalState::default();
    let mut accepted: Vec<Entry> = Vec::new();
    let mut outcomes = Vec::new();
    for command in commands {
        let outcome = state.apply(mode, command, |seq| accepted.iter().find(|e| e.seq == seq));
        if let (Outcome::Accepted { .. }, Command::Write(entry)) = (&outcome, command) {
            accepted.push(entry.clone());
        }
        outcomes.push(outcome);
    }
    (state, outcomes)
}

#[test]
fn nobody_writes_before_the_first_set_leader() {
    let (state, outcomes) = fold(&[write(1, 0, &[b"a"])]);
    assert!(matches!(outcomes[0], Outcome::Refused(_)));
    assert_eq!(state.next_seq, Seq(0));
}

#[test]
fn a_leader_writes_dense_positions_and_a_batch_takes_one_per_record() {
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a", b"b"]),
        write(1, 2, &[b"c"]),
    ]);
    assert_eq!(
        outcomes[1],
        Outcome::Accepted {
            seq: Seq(0),
            count: 2
        }
    );
    assert_eq!(
        outcomes[2],
        Outcome::Accepted {
            seq: Seq(2),
            count: 1
        }
    );
    assert_eq!(state.next_seq, Seq(3));
}

#[test]
fn a_gap_a_superseded_leader_and_a_foreign_uuid_are_refused() {
    let (_, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 1, &[b"gap"]),
        write(2, 0, &[b"foreign"]),
        set_leader(2, Some(1)),
        write(1, 0, &[b"stale"]),
    ]);
    assert!(matches!(outcomes[1], Outcome::Refused(_)));
    assert!(matches!(outcomes[2], Outcome::Refused(_)));
    assert!(matches!(outcomes[4], Outcome::Refused(v) if v.leader == Some(LeaderUuid(2))));
}

#[test]
fn a_retry_is_acked_only_with_the_same_bytes() {
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a"]),
        write(1, 0, &[b"a"]),
        write(1, 0, &[b"other"]),
    ]);
    assert_eq!(
        outcomes[2],
        Outcome::Duplicate {
            seq: Seq(0),
            count: 1
        }
    );
    assert!(matches!(outcomes[3], Outcome::Refused(_)));
    assert_eq!(state.next_seq, Seq(1));
}

#[test]
fn a_retry_survives_a_leadership_change_but_not_a_truncation() {
    let (_, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a"]),
        set_leader(2, Some(1)),
        write(1, 0, &[b"a"]),
        truncate(2, 1),
        write(1, 0, &[b"a"]),
    ]);
    assert!(matches!(outcomes[3], Outcome::Duplicate { .. }));
    assert!(matches!(outcomes[5], Outcome::Truncated(v) if v.first_seq == Seq(1)));
}

#[test]
fn set_leader_is_a_compare_and_set_and_raises_the_term() {
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        set_leader(2, None),
        set_leader(2, Some(1)),
    ]);
    assert!(matches!(outcomes[0], Outcome::Leader(v) if v.leader == Some(LeaderUuid(1))));
    assert!(matches!(outcomes[1], Outcome::LeaderRefused(v) if v.leader == Some(LeaderUuid(1))));
    assert!(matches!(outcomes[2], Outcome::Leader(_)));
    assert_eq!(state.leader, Some(LeaderUuid(2)));
    assert_eq!(state.term, 2);
}

#[test]
fn the_unset_uuid_and_the_current_leader_never_win() {
    let (state, outcomes) = fold(&[
        set_leader(0, None),
        set_leader(1, None),
        set_leader(1, Some(1)),
    ]);
    assert!(matches!(outcomes[0], Outcome::LeaderRefused(v) if v.leader.is_none()));
    assert!(matches!(outcomes[2], Outcome::LeaderRefused(_)));
    assert_eq!(state.term, 1);
}

#[test]
fn truncate_is_monotone_and_clamped_to_the_next_position() {
    let (state, _) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a", b"b"]),
        truncate(1, 9),
        truncate(1, 1),
    ]);
    assert_eq!(state.first_seq, Seq(2));
}

#[test]
fn truncate_is_fenced_like_a_write() {
    let (state, outcomes) = fold(&[
        truncate(1, 0),
        set_leader(1, None),
        write(1, 0, &[b"a", b"b"]),
        truncate(2, 1),
        set_leader(2, Some(1)),
        truncate(1, 2),
        truncate(2, 1),
    ]);
    assert!(matches!(outcomes[0], Outcome::TruncateRefused(v) if v.leader.is_none()));
    assert!(matches!(outcomes[3], Outcome::TruncateRefused(v) if v.first_seq == Seq(0)));
    assert!(matches!(outcomes[5], Outcome::TruncateRefused(v) if v.leader == Some(LeaderUuid(2))));
    assert!(matches!(outcomes[6], Outcome::Trimmed(v) if v.first_seq == Seq(1)));
    assert_eq!(state.first_seq, Seq(1));
}

#[test]
fn a_multi_writer_journal_takes_any_write_at_the_next_position() {
    let (state, outcomes) = fold_in(
        WriterMode::Multi,
        &[
            write(0, 7, &[b"a", b"b"]),
            write(0, 0, &[b"a", b"b"]),
            write(0, 0, &[]),
        ],
    );
    assert_eq!(
        outcomes[0],
        Outcome::Accepted {
            seq: Seq(0),
            count: 2
        }
    );
    // No deduplication: the same bytes land again.
    assert_eq!(
        outcomes[1],
        Outcome::Accepted {
            seq: Seq(2),
            count: 2
        }
    );
    assert!(matches!(outcomes[2], Outcome::Refused(_)));
    assert_eq!(state.next_seq, Seq(4));
    assert_eq!(state.leader, None);
}

#[test]
fn a_multi_writer_journal_lets_anyone_truncate_and_refuses_a_leader() {
    let (state, outcomes) = fold_in(
        WriterMode::Multi,
        &[
            write(0, 0, &[b"a", b"b", b"c"]),
            truncate(0, 2),
            truncate(0, 1),
            set_leader(1, None),
            write(1, 3, &[b"d"]),
            truncate(1, 3),
        ],
    );
    assert!(matches!(outcomes[1], Outcome::Trimmed(v) if v.first_seq == Seq(2)));
    assert!(matches!(outcomes[2], Outcome::Trimmed(v) if v.first_seq == Seq(2)));
    assert!(matches!(outcomes[3], Outcome::WrongMode(v) if v.leader.is_none()));
    assert!(matches!(outcomes[4], Outcome::WrongMode(_)));
    assert!(matches!(outcomes[5], Outcome::WrongMode(_)));
    assert_eq!(state.first_seq, Seq(2));
    assert_eq!(state.next_seq, Seq(3));
    assert_eq!(state.term, 0);
}

#[test]
fn a_single_writer_journal_refuses_a_multi_writer_call() {
    let (state, outcomes) = fold(&[set_leader(1, None), write(0, 0, &[b"a"]), truncate(0, 0)]);
    assert!(matches!(outcomes[1], Outcome::WrongMode(v) if v.leader == Some(LeaderUuid(1))));
    assert!(matches!(outcomes[2], Outcome::WrongMode(_)));
    assert_eq!(state.next_seq, Seq(0));
}
