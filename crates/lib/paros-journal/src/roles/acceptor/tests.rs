use super::*;
use crate::command::{Fence, Record, Write};
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
        vec![(PersistId(0), vec![AcceptorWrite::SetPromise(ballot(2))])]
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
                AcceptorWrite::SetPromise(ballot(2)),
                AcceptorWrite::AppendAccepted {
                    slot: Slot(4),
                    ballot: ballot(2),
                    value: write(1),
                },
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
            vec![AcceptorWrite::AppendAccepted {
                slot: Slot(4),
                ballot: ballot(2),
                value: write(1),
            }]
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
        &[AcceptorWrite::AppendAccepted {
            slot: Slot(4),
            ballot: ballot(2),
            value: write(1),
        }],
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
        &[AcceptorWrite::AppendAccepted {
            slot: Slot(0),
            ballot: ballot(2),
            value: write(1),
        }],
    );
}

#[test]
#[should_panic(expected = "never falls")]
fn a_disk_refuses_a_lower_promise() {
    let mut disk = AcceptorDisk::default();
    disk.apply(PersistId(0), &[AcceptorWrite::SetPromise(ballot(3))]);
    disk.apply(PersistId(1), &[AcceptorWrite::SetPromise(ballot(2))]);
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
