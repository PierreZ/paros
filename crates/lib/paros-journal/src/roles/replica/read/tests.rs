use paros_consensus::journal::LeaderUuid;

use super::*;
use crate::RELEASE;
use crate::command::{Command, Fence, Leader, Write};
use crate::fold::JournalView;
use crate::register::{Change, RegisterValue};
use crate::roles::replica::ReplicaDisk;
use crate::segment::{Grid, OpeningCall, SegmentRecord};

const LEADER: NodeId = NodeId(101);
const ME: NodeId = NodeId(11);
const GRID: [u64; 3] = [1, 2, 3];

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn reshape(value: &RegisterValue) -> RegisterValue {
    let newest = value.newest().map(SegmentRecord::id);
    let id = SegmentId(newest.map_or(1, |SegmentId(n)| n + 1));
    let grid = Grid::new(1, vec![nodes(&GRID)]).expect("a grid");
    value
        .apply(&Change::Reshape {
            opening: OpeningCall {
                term: value.term().number,
                expected: newest,
                grid,
                replicas: nodes(&[10 * id.0 + 1, 10 * id.0 + 2, 10 * id.0 + 3]),
            },
            release: RELEASE,
        })
        .expect("a reshape")
}

fn one_segment() -> RegisterValue {
    reshape(
        &RegisterValue::new()
            .apply(&Change::Elect { holder: NodeId(0) })
            .expect("an election"),
    )
}

/// A replica of segment 1 that took its base and applied a claim of uuid 7
/// at slot 0 and two writes of one record each at slots 1 and 2.
fn served() -> SegmentReplica {
    let mut replica = SegmentReplica::recover(
        ME,
        SegmentId(1),
        nodes(&[11, 12, 13]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
    replica.observe(&one_segment());
    let claim = Command::SetLeader {
        new: Some(LeaderUuid(7)),
        old: Leader::Unset,
    };
    replica.step(LEADER, commit(0, claim));
    replica.step(LEADER, commit(1, write(0, 1)));
    replica.step(LEADER, commit(2, write(1, 2)));
    assert_eq!(replica.front(), Some(Slot(3)));
    replica
}

fn write(at: u64, byte: u8) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: LeaderUuid(7),
            expected_seq: Seq(at),
        },
        records: vec![Record::new(vec![byte])],
    })
}

fn commit(slot: u64, command: Command) -> Message {
    Message::Commit {
        segment: SegmentId(1),
        slot: Slot(slot),
        command,
    }
}

fn ack(read: u64, watermark: Option<u64>, bound: u64) -> Message {
    Message::PreReadAck {
        segment: SegmentId(1),
        read,
        watermark: watermark.map(Slot),
        bound: SegmentId(bound),
    }
}

/// The answers of `effects`, by read.
fn answers(effects: &[ReplicaEffect]) -> Vec<(u64, ReadAnswer)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            ReplicaEffect::Read { read, answer, .. } => Some((*read, answer.clone())),
            _ => None,
        })
        .collect()
}

/// The acceptors that `effects` ask with a `PreRead`.
fn asked(effects: &[ReplicaEffect]) -> Vec<NodeId> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            ReplicaEffect::Send {
                to,
                message: Message::PreRead { .. },
                ..
            } => Some(*to),
            _ => None,
        })
        .collect()
}

fn page(answer: &ReadAnswer) -> (Vec<u64>, JournalView, bool) {
    match answer {
        ReadAnswer::Page {
            records,
            view,
            sealed,
        } => (
            records.iter().map(|(seq, _)| seq.0).collect(),
            *view,
            *sealed,
        ),
        other => panic!("not a page: {other:?}"),
    }
}

#[test]
fn a_read_of_an_open_segment_asks_the_grid_and_serves_at_a_quorum() {
    let mut replica = served();
    let effects = replica.read(1, Seq(0), 10);
    assert_eq!(
        asked(&effects),
        nodes(&GRID),
        "the read asks every acceptor"
    );
    assert!(answers(&effects).is_empty(), "no answer before a quorum");
    let effects = replica.step(NodeId(1), ack(1, Some(2), 1));
    assert!(answers(&effects).is_empty(), "one answer is no quorum");
    let effects = replica.step(NodeId(2), ack(1, Some(2), 1));
    let answered = answers(&effects);
    assert_eq!(answered.len(), 1);
    let (seqs, view, sealed) = page(&answered[0].1);
    assert_eq!(seqs, vec![0, 1]);
    assert_eq!(view.next_seq, Seq(2));
    assert!(!sealed);
    assert_eq!(replica.reads(), 0, "a served read is closed");
}

#[test]
fn a_read_waits_until_the_fold_covers_the_highest_watermark() {
    let mut replica = served();
    replica.read(1, Seq(1), 10);
    replica.step(NodeId(1), ack(1, Some(4), 1));
    let effects = replica.step(NodeId(3), ack(1, None, 1));
    assert!(answers(&effects).is_empty(), "slot 4 may be chosen");
    let effects = replica.step(LEADER, commit(3, write(2, 3)));
    assert!(answers(&effects).is_empty(), "slot 4 is not applied yet");
    let effects = replica.step(LEADER, commit(4, Command::Noop));
    let answered = answers(&effects);
    let (seqs, view, _) = page(&answered[0].1);
    assert_eq!(seqs, vec![1, 2]);
    assert_eq!(view.next_seq, Seq(3));
}

#[test]
fn an_answer_that_knows_a_later_segment_supersedes_the_read() {
    let mut replica = served();
    replica.read(1, Seq(0), 10);
    replica.step(NodeId(1), ack(1, Some(0), 1));
    let effects = replica.step(NodeId(2), ack(1, Some(0), 2));
    assert_eq!(
        answers(&effects),
        vec![(1, ReadAnswer::Superseded(SegmentId(2)))]
    );
    assert_eq!(replica.reads(), 0);
    let late = replica.step(NodeId(3), ack(1, Some(0), 1));
    assert!(answers(&late).is_empty(), "a closed read takes no answer");
}

#[test]
fn an_answer_from_outside_the_grid_is_not_counted() {
    let mut replica = served();
    replica.read(1, Seq(0), 10);
    replica.step(NodeId(1), ack(1, Some(0), 1));
    let effects = replica.step(NodeId(9), ack(1, Some(0), 1));
    assert_eq!(answers(&effects), Vec::new());
    let effects = replica.step(NodeId(9), ack(1, Some(0), 7));
    assert!(answers(&effects).is_empty(), "nor its fence");
    assert_eq!(replica.reads(), 1);
}

#[test]
fn a_page_holds_at_most_its_limit_and_starts_at_the_position() {
    let mut replica = served();
    replica.read(1, Seq(1), 1);
    replica.step(NodeId(1), ack(1, None, 1));
    let effects = replica.step(NodeId(2), ack(1, None, 1));
    let (seqs, ..) = page(&answers(&effects)[0].1);
    assert_eq!(seqs, vec![1]);
    // A limit of 0 is cut to one record.
    replica.read(2, Seq(0), 0);
    replica.step(NodeId(1), ack(2, None, 1));
    let effects = replica.step(NodeId(2), ack(2, None, 1));
    let (seqs, ..) = page(&answers(&effects)[0].1);
    assert_eq!(seqs, vec![0]);
}

#[test]
fn a_read_past_the_tail_serves_an_empty_page() {
    let mut replica = served();
    replica.read(1, Seq(5), 10);
    replica.step(NodeId(1), ack(1, None, 1));
    let effects = replica.step(NodeId(2), ack(1, None, 1));
    let (seqs, view, _) = page(&answers(&effects)[0].1);
    assert_eq!(seqs, Vec::new());
    assert_eq!(view.next_seq, Seq(2));
}

#[test]
fn a_read_of_a_sealed_segment_asks_nobody_and_serves_at_the_end() {
    let mut replica = served();
    let value = reshape(&one_segment())
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(4),
        })
        .expect("a seal");
    replica.observe(&value);
    let effects = replica.read(1, Seq(0), 10);
    assert!(asked(&effects).is_empty(), "the end is a fact");
    assert!(answers(&effects).is_empty(), "slot 3 is not applied");
    let effects = replica.step(LEADER, commit(3, Command::Noop));
    let (seqs, view, sealed) = page(&answers(&effects)[0].1);
    assert_eq!(seqs, vec![0, 1]);
    assert_eq!(view.next_seq, Seq(2));
    assert!(sealed, "the page reaches the end of the sealed segment");
    // A page cut by its limit does not reach the end.
    let effects = replica.read(2, Seq(0), 1);
    let (_, _, sealed) = page(&answers(&effects)[0].1);
    assert!(!sealed);
}

#[test]
fn a_read_below_the_floor_is_truncated() {
    let mut replica = served();
    let truncate = Command::Truncate {
        fence: Some(LeaderUuid(7)),
        up_to: Seq(1),
    };
    replica.step(LEADER, commit(3, truncate));
    replica.read(1, Seq(0), 10);
    replica.step(NodeId(1), ack(1, Some(3), 1));
    let effects = replica.step(NodeId(2), ack(1, Some(3), 1));
    let answered = answers(&effects);
    assert!(
        matches!(answered[0].1, ReadAnswer::Truncated(view) if view.first_seq == Seq(1)),
        "{answered:?}"
    );
}

#[test]
fn a_replica_with_no_base_or_too_many_reads_is_unserved() {
    let mut empty = SegmentReplica::recover(
        ME,
        SegmentId(1),
        nodes(&[11, 12, 13]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
    assert_eq!(
        answers(&empty.read(1, Seq(0), 1)),
        vec![(1, ReadAnswer::Unserved)]
    );
    let mut replica = served();
    for read in 0..MAX_READS as u64 {
        assert_eq!(answers(&replica.read(read, Seq(0), 1)), Vec::new());
    }
    assert_eq!(replica.reads(), MAX_READS);
    let over = MAX_READS as u64;
    assert_eq!(
        answers(&replica.read(over, Seq(0), 1)),
        vec![(over, ReadAnswer::Unserved)]
    );
    assert!(
        replica.read(0, Seq(0), 1).is_empty(),
        "an open read id changes nothing"
    );
}

#[test]
fn a_read_asks_again_on_a_tick_and_is_unserved_after_its_ticks() {
    let mut replica = served();
    replica.read(1, Seq(0), 1);
    replica.step(NodeId(2), ack(1, None, 1));
    let tick = replica.tick();
    assert_eq!(asked(&tick), nodes(&[1, 3]), "only the silent acceptors");
    for _ in 1..READ_TICKS - 1 {
        assert_eq!(answers(&replica.tick()), Vec::new());
    }
    assert_eq!(answers(&replica.tick()), vec![(1, ReadAnswer::Unserved)]);
    assert_eq!(replica.reads(), 0);
}

#[test]
fn an_answer_waits_on_the_last_batch_of_the_replica() {
    let mut replica = served();
    replica.read(1, Seq(0), 1);
    replica.step(NodeId(1), ack(1, None, 1));
    let effects = replica.step(NodeId(2), ack(1, None, 1));
    let last = replica.flight.last();
    assert!(last.is_some());
    assert!(effects.iter().all(|effect| matches!(
        effect,
        ReplicaEffect::Read { after, .. } if *after == last
    )));
}

#[test]
fn a_replica_that_drops_its_copy_unserves_its_reads() {
    let mut replica = served();
    replica.read(1, Seq(0), 1);
    // A value that removes the replica from the set drops its copy.
    let mut value = one_segment();
    let record = value.segment(SegmentId(1)).expect("a record").clone();
    let removed = SegmentRecord::from_parts(
        record.id(),
        record.release(),
        record.opening().clone(),
        nodes(&[12, 13, 14]),
        record.heals(),
        record.state(),
        record.start(),
        record.end(),
        record.boundary(),
    )
    .expect("the same record with another set");
    let mut segments = value.segments().clone();
    segments.insert(SegmentId(1), removed);
    value = RegisterValue::from_parts(value.term(), segments);
    let effects = replica.observe(&value);
    assert_eq!(answers(&effects), vec![(1, ReadAnswer::Unserved)]);
    assert!(effects.contains(&ReplicaEffect::Drop));
    assert_eq!(replica.reads(), 0);
}
