use super::*;
use crate::command::{Fence, Record, Write};
use crate::segment::SegmentId;
use paros_consensus::journal::{LeaderUuid, Seq};

const ME: NodeId = NodeId(1);
const LEADER: NodeId = NodeId(101);

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: LEADER,
    }
}

fn write(byte: u8) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: LeaderUuid(1),
            expected_seq: Seq(0),
        },
        records: vec![Record::new(vec![byte])],
    })
}

fn fresh() -> JournalAcceptor {
    JournalAcceptor::recover(ME, AcceptorDisk::default(), PROMISE_BATCH)
}

fn prepare(round: u64, from: u64) -> Message {
    Message::Prepare {
        ballot: ballot(round),
        from: Slot(from),
        segment: SegmentId(1),
    }
}

fn accept(round: u64, slot: u64, command: Command) -> Message {
    Message::Accept {
        ballot: ballot(round),
        slot: Slot(slot),
        command,
    }
}

/// The persist batches, the sends and the batch each send waits on.
type Split = (
    Vec<(PersistId, Vec<AcceptorOp>)>,
    Vec<Message>,
    Vec<Option<PersistId>>,
);

fn split(effects: Vec<AcceptorEffect>) -> Split {
    let mut batches = Vec::new();
    let mut sends = Vec::new();
    let mut afters = Vec::new();
    for effect in effects {
        match effect {
            AcceptorEffect::Persist { id, ops } => batches.push((id, ops)),
            AcceptorEffect::Send { to, message, after } => {
                assert_eq!(to, LEADER, "an acceptor answers its sender");
                sends.push(message);
                afters.push(after);
            }
        }
    }
    (batches, sends, afters)
}

#[test]
fn a_prepare_that_raises_the_promise_writes_it_before_the_promise_leaves() {
    let mut acceptor = fresh();
    let (batches, sends, afters) = split(acceptor.step(LEADER, prepare(2, 0)));
    assert_eq!(
        batches,
        vec![(
            PersistId(0),
            vec![AcceptorOp::Write(AcceptorWrite::SetPromise(ballot(2)))]
        )]
    );
    assert_eq!(
        sends,
        vec![Message::Promise {
            ballot: ballot(2),
            from: Slot(0),
            page: PromisePage {
                votes: BTreeMap::new(),
                faulty: BTreeMap::new(),
                next: None,
                dropped: BTreeMap::new(),
            },
        }]
    );
    assert_eq!(afters, vec![Some(PersistId(0))]);
    assert_eq!(acceptor.promised(), ballot(2));
}

#[test]
fn a_prepare_at_the_promise_writes_nothing_and_waits_on_the_last_batch() {
    // The batch that recorded the promise may still be in flight
    // (`flight-rule/`, control `own_batch`).
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare(2, 0));
    let (batches, sends, afters) = split(acceptor.step(LEADER, prepare(2, 0)));
    assert_eq!(batches.len(), 0);
    assert!(matches!(sends[..], [Message::Promise { .. }]));
    assert_eq!(afters, vec![Some(PersistId(0))]);
}

#[test]
fn a_prepare_below_the_promise_is_refused_with_the_promise() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare(3, 0));
    let (batches, sends, afters) = split(acceptor.step(LEADER, prepare(2, 0)));
    assert_eq!(batches.len(), 0);
    assert_eq!(
        sends,
        vec![Message::Nack {
            ballot: ballot(2),
            promised: ballot(3),
        }]
    );
    assert_eq!(afters, vec![Some(PersistId(0))]);
    assert_eq!(acceptor.promised(), ballot(3));
}

#[test]
fn an_accept_that_raises_the_promise_writes_the_promise_and_the_vote_in_one_batch() {
    // #1309 (the state is the disk), control `order_vote_first`: a vote
    // durable before its promise lets a restart take a lower ballot over it.
    let mut acceptor = fresh();
    let (batches, sends, afters) = split(acceptor.step(LEADER, accept(2, 4, write(1))));
    assert_eq!(
        batches,
        vec![(
            PersistId(0),
            vec![
                AcceptorOp::Write(AcceptorWrite::SetPromise(ballot(2))),
                AcceptorOp::Write(AcceptorWrite::AppendAccepted {
                    slot: Slot(4),
                    ballot: ballot(2),
                    value: write(1),
                }),
            ]
        )]
    );
    assert_eq!(
        sends,
        vec![Message::Accepted {
            ballot: ballot(2),
            slot: Slot(4),
        }]
    );
    assert_eq!(afters, vec![Some(PersistId(0))]);
    assert_eq!(acceptor.vote(Slot(4)), Some(&(ballot(2), write(1))));
    assert_eq!(acceptor.promised(), ballot(2));
}

#[test]
fn an_accept_at_the_promise_writes_the_vote_alone() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare(2, 0));
    let (batches, _, afters) = split(acceptor.step(LEADER, accept(2, 4, write(1))));
    assert_eq!(
        batches,
        vec![(
            PersistId(1),
            vec![AcceptorOp::Write(AcceptorWrite::AppendAccepted {
                slot: Slot(4),
                ballot: ballot(2),
                value: write(1),
            })]
        )]
    );
    assert_eq!(afters, vec![Some(PersistId(1))]);
}

#[test]
fn an_accept_already_held_writes_nothing_and_waits_on_the_last_batch() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, accept(2, 4, write(1)));
    acceptor.step(LEADER, accept(2, 5, write(2)));
    let (batches, sends, afters) = split(acceptor.step(LEADER, accept(2, 4, write(1))));
    assert_eq!(batches.len(), 0);
    assert_eq!(
        sends,
        vec![Message::Accepted {
            ballot: ballot(2),
            slot: Slot(4),
        }]
    );
    assert_eq!(afters, vec![Some(PersistId(1))]);
}

#[test]
fn a_higher_ballot_votes_again_at_a_slot() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, accept(2, 4, write(1)));
    let (batches, _, _) = split(acceptor.step(LEADER, accept(3, 4, write(1))));
    assert_eq!(batches.len(), 1);
    assert_eq!(acceptor.vote(Slot(4)), Some(&(ballot(3), write(1))));
}

#[test]
fn an_accept_below_the_promise_is_refused() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare(3, 0));
    let (batches, sends, _) = split(acceptor.step(LEADER, accept(2, 4, write(1))));
    assert_eq!(batches.len(), 0);
    assert_eq!(
        sends,
        vec![Message::Nack {
            ballot: ballot(2),
            promised: ballot(3),
        }]
    );
    assert_eq!(acceptor.vote(Slot(4)), None);
}

#[test]
#[should_panic(expected = "carries the recorded command")]
fn two_commands_at_one_slot_and_ballot_break_paxos() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, accept(2, 4, write(1)));
    acceptor.step(LEADER, accept(2, 4, write(2)));
}

#[test]
fn a_promise_reports_the_votes_from_its_slot_in_pages() {
    let mut acceptor = JournalAcceptor::recover(ME, AcceptorDisk::default(), 1);
    acceptor.step(LEADER, accept(2, 3, write(1)));
    acceptor.step(LEADER, accept(2, 5, write(2)));
    acceptor.step(LEADER, accept(2, 7, write(3)));
    let (_, sends, _) = split(acceptor.step(LEADER, prepare(4, 4)));
    let [Message::Promise { from, page, .. }] = &sends[..] else {
        panic!("one promise: {sends:?}");
    };
    assert_eq!(*from, Slot(4));
    assert_eq!(
        page.votes,
        BTreeMap::from([(Slot(5), (ballot(2), write(2)))]),
        "a page starts at its slot and holds one vote"
    );
    assert_eq!(
        page.next,
        Some(Slot(7)),
        "the next page starts at the next vote"
    );
    let (_, sends, _) = split(acceptor.step(LEADER, prepare(4, 7)));
    let [Message::Promise { page, .. }] = &sends[..] else {
        panic!("one promise: {sends:?}");
    };
    assert_eq!(
        page.votes.keys().copied().collect::<Vec<_>>(),
        vec![Slot(7)]
    );
    assert_eq!(page.next, None, "the last page has no next");
}

#[test]
fn a_message_for_another_role_changes_nothing() {
    let mut acceptor = fresh();
    let effects = acceptor.step(
        LEADER,
        Message::Accepted {
            ballot: ballot(2),
            slot: Slot(0),
        },
    );
    assert_eq!(effects.len(), 0);
    assert_eq!(acceptor.promised(), Ballot::default());
}

#[test]
fn a_restart_starts_from_the_disk() {
    let mut acceptor = fresh();
    let mut disk = AcceptorDisk::default();
    for effect in acceptor.step(LEADER, accept(2, 4, write(1))) {
        if let AcceptorEffect::Persist { id, ops } = effect {
            disk.apply(id, &ops);
        }
    }
    assert_eq!(disk.promised, ballot(2));
    assert_eq!(disk.durable, Some(PersistId(0)));
    let mut restarted = JournalAcceptor::recover(ME, disk, PROMISE_BATCH);
    assert_eq!(restarted.id(), ME);
    assert_eq!(restarted.promised(), ballot(2));
    assert_eq!(restarted.vote(Slot(4)), Some(&(ballot(2), write(1))));
    // A reply with no write waits on the durable batch; the next batch is
    // above it.
    let (_, _, afters) = split(restarted.step(LEADER, prepare(2, 0)));
    assert_eq!(afters, vec![Some(PersistId(0))]);
    let (batches, _, _) = split(restarted.step(LEADER, accept(2, 5, write(2))));
    assert_eq!(batches[0].0, PersistId(1));
}

#[test]
fn a_faulty_vote_is_reported_and_a_new_vote_repairs_it() {
    let disk = AcceptorDisk {
        promised: ballot(2),
        faulty: BTreeMap::from([(Slot(4), ballot(2))]),
        ..AcceptorDisk::default()
    };
    let mut acceptor = JournalAcceptor::recover(ME, disk.clone(), PROMISE_BATCH);
    let (_, sends, _) = split(acceptor.step(LEADER, prepare(3, 0)));
    let [Message::Promise { page, .. }] = &sends[..] else {
        panic!("one promise: {sends:?}");
    };
    assert_eq!(page.faulty, BTreeMap::from([(Slot(4), ballot(2))]));
    let mut repaired = disk;
    repaired.apply(
        PersistId(0),
        &[AcceptorOp::Write(AcceptorWrite::AppendAccepted {
            slot: Slot(4),
            ballot: ballot(2),
            value: write(1),
        })],
    );
    assert!(
        repaired.faulty.is_empty(),
        "a durable vote replaces the faulty entry"
    );
    assert_eq!(repaired.votes[&Slot(4)], (ballot(2), write(1)));
}

#[test]
#[should_panic(expected = "id order")]
fn a_disk_takes_batches_in_id_order() {
    let mut disk = AcceptorDisk::default();
    disk.apply(PersistId(1), &[]);
    disk.apply(PersistId(1), &[]);
}

#[test]
#[should_panic(expected = "under its promise")]
fn a_disk_refuses_a_vote_above_its_promise() {
    let mut disk = AcceptorDisk::default();
    disk.apply(
        PersistId(0),
        &[AcceptorOp::Write(AcceptorWrite::AppendAccepted {
            slot: Slot(0),
            ballot: ballot(2),
            value: write(1),
        })],
    );
}

#[test]
#[should_panic(expected = "never falls")]
fn a_disk_refuses_a_lower_promise() {
    let mut disk = AcceptorDisk::default();
    disk.apply(
        PersistId(0),
        &[AcceptorOp::Write(AcceptorWrite::SetPromise(ballot(3)))],
    );
    disk.apply(
        PersistId(1),
        &[AcceptorOp::Write(AcceptorWrite::SetPromise(ballot(2)))],
    );
}

#[test]
#[should_panic(expected = "at least one entry")]
fn a_page_of_zero_is_refused() {
    let _ = JournalAcceptor::recover(ME, AcceptorDisk::default(), 0);
}

#[test]
#[should_panic(expected = "at most PROMISE_BATCH")]
fn a_page_above_the_batch_is_refused() {
    let _ = JournalAcceptor::recover(ME, AcceptorDisk::default(), PROMISE_BATCH + 1);
}

fn prepare_in(round: u64, segment: u64) -> Message {
    Message::Prepare {
        ballot: ballot(round),
        from: Slot(0),
        segment: SegmentId(segment),
    }
}

fn pre_read(read: u64) -> Message {
    Message::PreRead {
        segment: SegmentId(1),
        read,
    }
}

#[test]
fn an_acceptor_with_no_belief_answers_no_read() {
    let mut acceptor = fresh();
    assert_eq!(acceptor.bound(), None);
    assert_eq!(acceptor.step(LEADER, pre_read(1)), Vec::new());
}

#[test]
fn a_read_gets_the_watermark_and_the_newest_segment_once_the_last_batch_is_durable() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare_in(2, 1));
    acceptor.step(LEADER, accept(2, 4, write(1)));
    assert_eq!(acceptor.bound(), Some(SegmentId(1)));
    let (batches, sends, afters) = split(acceptor.step(LEADER, pre_read(9)));
    assert!(batches.is_empty(), "a read writes nothing");
    assert_eq!(
        sends,
        vec![Message::PreReadAck {
            segment: SegmentId(1),
            read: 9,
            watermark: Some(Slot(4)),
            bound: SegmentId(1),
        }]
    );
    assert_eq!(afters, vec![Some(PersistId(1))], "the vote's batch");
}

#[test]
fn a_prepare_of_a_later_segment_raises_the_belief_and_a_refused_one_does_not() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare_in(2, 1));
    acceptor.step(LEADER, prepare_in(3, 2));
    assert_eq!(acceptor.bound(), Some(SegmentId(2)));
    // A Prepare at the promise of an older segment keeps the newest.
    acceptor.step(LEADER, prepare_in(3, 1));
    assert_eq!(acceptor.bound(), Some(SegmentId(2)));
    // A refused Prepare teaches nothing.
    acceptor.step(LEADER, prepare_in(1, 4));
    assert_eq!(acceptor.bound(), Some(SegmentId(2)));
    let (_, sends, _) = split(acceptor.step(LEADER, pre_read(1)));
    assert!(matches!(
        sends.as_slice(),
        [Message::PreReadAck {
            bound: SegmentId(2),
            ..
        }]
    ));
}

#[test]
fn an_accept_that_raises_the_promise_clears_the_belief() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare_in(2, 1));
    // An Accept at the promise keeps the belief.
    acceptor.step(LEADER, accept(2, 0, write(1)));
    assert_eq!(acceptor.bound(), Some(SegmentId(1)));
    // An Accept above it names no segment: the acceptor answers no read
    // until a Prepare tells it one.
    acceptor.step(LEADER, accept(5, 1, write(2)));
    assert_eq!(acceptor.bound(), None);
    assert_eq!(acceptor.step(LEADER, pre_read(1)), Vec::new());
    acceptor.step(LEADER, prepare_in(6, 3));
    assert_eq!(acceptor.bound(), Some(SegmentId(3)));
}

#[test]
fn an_acceptor_that_starts_again_has_no_belief() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare_in(2, 1));
    let disk = AcceptorDisk {
        promised: ballot(2),
        durable: Some(PersistId(0)),
        ..AcceptorDisk::default()
    };
    let restarted = JournalAcceptor::recover(ME, disk, PROMISE_BATCH);
    assert_eq!(restarted.promised(), ballot(2));
    assert_eq!(restarted.bound(), None, "the belief is in memory only");
}

#[test]
fn a_compact_drops_the_votes_of_its_range_and_keeps_the_range() {
    let mut acceptor = fresh();
    let mut disk = AcceptorDisk::default();
    for slot in 0..3 {
        for (id, ops) in split(acceptor.step(LEADER, accept(1, slot, write(1)))).0 {
            disk.apply(id, &ops);
        }
    }
    let (batches, sends, _) = split(acceptor.step(
        LEADER,
        Message::Compact {
            segment: SegmentId(1),
            start: Slot(0),
            below: Slot(2),
        },
    ));
    assert_eq!(batches.len(), 1, "the drop is a batch of its own");
    assert_eq!(sends, Vec::new(), "a compaction sends nothing");
    assert_eq!(
        acceptor.dropped(),
        &BTreeMap::from([(Slot(0), Slot(2))]),
        "the acceptor keeps the range it dropped"
    );
    assert!(acceptor.vote(Slot(0)).is_none());
    assert!(acceptor.vote(Slot(1)).is_none());
    assert!(
        acceptor.vote(Slot(2)).is_some(),
        "the slot at `below` stays"
    );
    for (id, ops) in &batches {
        disk.apply(*id, ops);
    }
    assert_eq!(
        disk.votes.keys().copied().collect::<Vec<_>>(),
        vec![Slot(2)],
        "the disk drops the votes of the range and keeps the slot at `below`"
    );
    assert_eq!(disk.dropped, BTreeMap::from([(Slot(0), Slot(2))]));
}

#[test]
fn a_compact_drops_the_faulty_entries_of_its_range() {
    let disk = AcceptorDisk {
        promised: ballot(1),
        faulty: BTreeMap::from([(Slot(1), ballot(1)), (Slot(5), ballot(1))]),
        ..AcceptorDisk::default()
    };
    let mut acceptor = JournalAcceptor::recover(ME, disk.clone(), PROMISE_BATCH);
    let (batches, _, _) = split(acceptor.step(
        LEADER,
        Message::Compact {
            segment: SegmentId(1),
            start: Slot(0),
            below: Slot(3),
        },
    ));
    let mut disk = disk;
    for (id, ops) in &batches {
        disk.apply(*id, ops);
    }
    assert_eq!(
        disk.faulty.keys().copied().collect::<Vec<_>>(),
        vec![Slot(5)],
        "the disk drops the faulty entry in the range and keeps the one above"
    );
    let (_, sends, _) = split(acceptor.step(LEADER, prepare(2, 0)));
    let [Message::Promise { page, .. }] = &sends[..] else {
        panic!("a promise: {sends:?}");
    };
    assert_eq!(
        page.faulty.keys().copied().collect::<Vec<_>>(),
        vec![Slot(5)],
        "the faulty entry in the range goes, the one above stays"
    );
    assert_eq!(page.dropped, BTreeMap::from([(Slot(0), Slot(3))]));
}

#[test]
fn the_read_watermark_counts_the_last_slot_of_a_dropped_range() {
    let mut acceptor = fresh();
    acceptor.step(LEADER, prepare(1, 0));
    acceptor.step(
        LEADER,
        Message::Compact {
            segment: SegmentId(1),
            start: Slot(0),
            below: Slot(3),
        },
    );
    let (_, sends, _) = split(acceptor.step(
        LEADER,
        Message::PreRead {
            segment: SegmentId(1),
            read: 1,
        },
    ));
    assert!(
        matches!(
            sends[..],
            [Message::PreReadAck {
                watermark: Some(Slot(2)),
                ..
            }]
        ),
        "the last dropped slot is the watermark: {sends:?}"
    );
}

fn repair(round: u64, slot: u64, command: Command) -> Message {
    Message::Repair {
        ballot: ballot(round),
        slot: Slot(slot),
        command,
    }
}

#[test]
fn a_repair_writes_the_chosen_command_at_the_higher_ballot_in_one_batch() {
    let disk = AcceptorDisk {
        promised: ballot(4),
        faulty: BTreeMap::from([(Slot(1), ballot(4)), (Slot(2), ballot(1))]),
        votes: BTreeMap::from([(Slot(3), (ballot(2), write(3)))]),
        ..AcceptorDisk::default()
    };
    let mut acceptor = JournalAcceptor::recover(ME, disk.clone(), PROMISE_BATCH);
    let mut image = disk;
    // Above the choosing ballot: the faulty vote keeps its ballot, and the
    // promise does not move.
    let (batches, sends, _) = split(acceptor.step(LEADER, repair(3, 1, write(1))));
    assert!(sends.is_empty(), "a repair has no answer");
    let [(id, ops)] = &batches[..] else {
        panic!("one batch: {batches:?}");
    };
    image.apply(*id, ops);
    assert_eq!(image.votes[&Slot(1)], (ballot(4), write(1)));
    assert_eq!(image.promised, ballot(4));
    // Below the choosing ballot: the vote is written at the choosing ballot,
    // with the promise raised first in the same batch.
    let (batches, _, _) = split(acceptor.step(LEADER, repair(6, 2, write(2))));
    let [(id, ops)] = &batches[..] else {
        panic!("one batch: {batches:?}");
    };
    assert!(matches!(
        ops[..],
        [
            AcceptorOp::Write(AcceptorWrite::SetPromise(_)),
            AcceptorOp::Write(AcceptorWrite::AppendAccepted { .. })
        ]
    ));
    image.apply(*id, ops);
    assert_eq!(image.votes[&Slot(2)], (ballot(6), write(2)));
    assert_eq!(image.promised, ballot(6));
    assert!(image.faulty.is_empty(), "both faulty votes are repaired");
    assert_eq!(acceptor.vote(Slot(2)), Some(&(ballot(6), write(2))));
    // A readable vote, or a slot with no vote, takes no repair.
    assert_eq!(acceptor.step(LEADER, repair(6, 3, write(9))).len(), 0);
    assert_eq!(acceptor.step(LEADER, repair(6, 7, write(9))).len(), 0);
    assert_eq!(acceptor.vote(Slot(3)), Some(&(ballot(2), write(3))));
}
