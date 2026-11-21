use super::*;
use crate::RELEASE;
use crate::command::{Fence, Record, Write};
use crate::roles::message::PromisePage;
use crate::segment::OpeningCall;
use paros_consensus::journal::{LeaderUuid, Seq};

const LEADER: NodeId = NodeId(101);

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: LEADER,
    }
}

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
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

/// The register after the reshape that opens the next segment with
/// `columns` and the replicas `10k + 1..=3`.
fn reshape(value: &RegisterValue, columns: &[&[u64]]) -> RegisterValue {
    let newest = value.newest().map(SegmentRecord::id);
    let id = SegmentId(newest.map_or(1, |SegmentId(n)| n + 1));
    let grid = Grid::new(1, columns.iter().map(|c| nodes(c)).collect()).expect("a grid");
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

/// Segment 1 in force on acceptors 1, 2, 3.
fn one_segment() -> RegisterValue {
    let value = RegisterValue::new()
        .apply(&Change::Elect { holder: NodeId(0) })
        .expect("an election");
    reshape(&value, &[&[1, 2, 3]])
}

/// Segment 1 in force on 1, 2, 3, segment 2 pending on 4, 5, 6.
fn two_segments() -> RegisterValue {
    reshape(&one_segment(), &[&[4, 5, 6]])
}

fn campaign(
    value: &RegisterValue,
    round: u64,
    segment: u64,
    from: u64,
) -> (JournalLeader<u32>, Vec<LeaderEffect<u32>>) {
    JournalLeader::campaign(
        Campaign {
            ballot: ballot(round),
            segment: SegmentId(segment),
            from: Slot(from),
        },
        value,
    )
    .expect("a campaign")
}

fn page(votes: &[(u64, u64, Command)], faulty: &[(u64, u64)]) -> PromisePage {
    PromisePage {
        votes: votes
            .iter()
            .map(|(slot, round, command)| (Slot(*slot), (ballot(*round), command.clone())))
            .collect(),
        faulty: faulty
            .iter()
            .map(|(slot, round)| (Slot(*slot), ballot(*round)))
            .collect(),
        next: None,
    }
}

fn promise(round: u64, from: u64, page: PromisePage) -> Message {
    Message::Promise {
        ballot: ballot(round),
        from: Slot(from),
        page,
    }
}

/// The messages of `effects`, by receiver.
fn sends(effects: &[LeaderEffect<u32>]) -> Vec<(NodeId, Message)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            LeaderEffect::Send { to, message } => Some((*to, message.clone())),
            _ => None,
        })
        .collect()
}

fn replies(effects: &[LeaderEffect<u32>]) -> Vec<(u32, Reply)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            LeaderEffect::Reply { token, reply } => Some((*token, reply.clone())),
            _ => None,
        })
        .collect()
}

/// The accepts of `effects`: slot and command, once each.
fn accepts(effects: &[LeaderEffect<u32>]) -> BTreeMap<Slot, Command> {
    sends(effects)
        .into_iter()
        .filter_map(|(_, message)| match message {
            Message::Accept { slot, command, .. } => Some((slot, command)),
            _ => None,
        })
        .collect()
}

/// An open leader of segment 1, from slot 0, on promises from 1 and 2.
fn open_leader() -> JournalLeader<u32> {
    let value = one_segment();
    let (mut leader, _) = campaign(&value, 2, 1, 0);
    leader.step(NodeId(1), promise(2, 0, page(&[], &[])));
    leader.step(NodeId(2), promise(2, 0, page(&[], &[])));
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(0) });
    leader
}

#[test]
fn a_campaign_asks_every_acceptor_from_its_first_slot() {
    let value = one_segment();
    let (leader, effects) = campaign(&value, 2, 1, 3);
    assert_eq!(*leader.phase(), LeaderPhase::Phase1);
    assert_eq!(leader.ballot(), ballot(2));
    assert_eq!(leader.segment(), SegmentId(1));
    assert_eq!(
        sends(&effects),
        nodes(&[1, 2, 3])
            .into_iter()
            .map(|to| (
                to,
                Message::Prepare {
                    ballot: ballot(2),
                    from: Slot(3),
                }
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn a_campaign_on_a_missing_or_sealed_segment_is_refused() {
    let value = one_segment();
    let missing = JournalLeader::<u32>::campaign(
        Campaign {
            ballot: ballot(2),
            segment: SegmentId(2),
            from: Slot(0),
        },
        &value,
    );
    assert!(missing.is_none());
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(0),
        })
        .expect("a seal");
    let old = JournalLeader::<u32>::campaign(
        Campaign {
            ballot: ballot(2),
            segment: SegmentId(1),
            from: Slot(0),
        },
        &sealed,
    );
    assert!(old.is_none(), "a sealed segment takes no new leader");
}

#[test]
fn phase_one_waits_for_a_quorum_of_complete_answers() {
    let value = one_segment();
    let (mut leader, _) = campaign(&value, 2, 1, 0);
    let effects = leader.step(NodeId(1), promise(2, 0, page(&[], &[])));
    assert_eq!(effects.len(), 0);
    assert_eq!(*leader.phase(), LeaderPhase::Phase1);
    // A promise of another ballot is not counted.
    leader.step(NodeId(2), promise(3, 0, page(&[], &[])));
    assert_eq!(*leader.phase(), LeaderPhase::Phase1);
    // A tick asks again the acceptors that did not answer.
    let again = leader.tick(1);
    assert_eq!(
        sends(&again)
            .into_iter()
            .map(|(to, _)| to)
            .collect::<Vec<_>>(),
        nodes(&[2, 3])
    );
    leader.step(NodeId(2), promise(2, 0, page(&[], &[])));
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(0) });
}

#[test]
fn a_promise_in_pages_asks_for_the_next_page() {
    let value = one_segment();
    let (mut leader, _) = campaign(&value, 2, 1, 0);
    let mut first = page(&[(0, 1, write(1))], &[]);
    first.next = Some(Slot(5));
    let effects = leader.step(NodeId(1), promise(2, 0, first));
    assert_eq!(
        sends(&effects),
        vec![(
            NodeId(1),
            Message::Prepare {
                ballot: ballot(2),
                from: Slot(5),
            }
        )]
    );
    leader.step(NodeId(2), promise(2, 0, page(&[], &[])));
    assert_eq!(
        *leader.phase(),
        LeaderPhase::Phase1,
        "acceptor 1 is not complete"
    );
    let effects = leader.step(NodeId(1), promise(2, 5, page(&[(6, 1, write(2))], &[])));
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(0) });
    assert_eq!(
        accepts(&effects),
        BTreeMap::from([
            (Slot(0), write(1)),
            (Slot(1), Command::Noop),
            (Slot(2), Command::Noop),
            (Slot(3), Command::Noop),
            (Slot(4), Command::Noop),
            (Slot(5), Command::Noop),
            (Slot(6), write(2)),
        ])
    );
}

#[test]
fn an_open_leader_proposes_again_the_highest_vote_and_fills_the_gaps() {
    let value = one_segment();
    let (mut leader, _) = campaign(&value, 3, 1, 0);
    leader.step(NodeId(1), promise(3, 0, page(&[(1, 1, write(1))], &[])));
    let effects = leader.step(
        NodeId(2),
        promise(3, 0, page(&[(1, 2, write(2)), (3, 1, write(3))], &[])),
    );
    assert_eq!(
        accepts(&effects),
        BTreeMap::from([
            (Slot(0), Command::Noop),
            (Slot(1), write(2)),
            (Slot(2), Command::Noop),
            (Slot(3), write(3)),
        ])
    );
    // Each accept goes to the three acceptors of the column, at the
    // leader's ballot.
    assert_eq!(sends(&effects).len(), 12);
    assert!(
        sends(&effects)
            .iter()
            .all(|(_, m)| matches!(m, Message::Accept { ballot: b, .. } if *b == ballot(3)))
    );
    let (proposed, effects) = leader.propose(7, write(9));
    assert_eq!(
        proposed,
        Proposed::Slot(Slot(4)),
        "a proposal takes the slot after the gap fill"
    );
    assert_eq!(accepts(&effects), BTreeMap::from([(Slot(4), write(9))]));
}

#[test]
fn a_proposal_is_answered_once_a_replica_applies_it() {
    let mut leader = open_leader();
    assert_eq!(leader.propose(7, write(1)).0, Proposed::Slot(Slot(0)));
    assert_eq!(leader.propose(8, write(2)).0, Proposed::Slot(Slot(1)));
    let accepted = |slot| Message::Accepted {
        ballot: ballot(2),
        slot: Slot(slot),
    };
    assert!(
        leader.step(NodeId(1), accepted(0)).is_empty(),
        "one vote is not a quorum"
    );
    // A vote of another ballot or another slot does not count.
    leader.step(
        NodeId(2),
        Message::Accepted {
            ballot: ballot(1),
            slot: Slot(0),
        },
    );
    leader.step(NodeId(2), accepted(5));
    let effects = leader.step(NodeId(2), accepted(0));
    let commit = Message::Commit {
        segment: SegmentId(1),
        slot: Slot(0),
        command: write(1),
    };
    assert_eq!(
        sends(&effects),
        nodes(&[11, 12, 13])
            .into_iter()
            .map(|to| (to, commit.clone()))
            .collect::<Vec<_>>(),
        "a chosen slot goes to the replica set of its segment"
    );
    // A third vote changes nothing.
    assert_eq!(leader.step(NodeId(3), accepted(0)).len(), 0);
    // A tick sends the commit again, and the accept of the open round.
    let tick = leader.tick(4);
    assert!(sends(&tick).contains(&(NodeId(11), commit.clone())));
    assert_eq!(accepts(&tick), BTreeMap::from([(Slot(1), write(2))]));
    let answer = Answer::Written {
        seq: Seq(0),
        count: 1,
    };
    let effects = leader.step(
        NodeId(11),
        Message::Applied {
            segment: SegmentId(1),
            slot: Slot(0),
            command: write(1),
            answer: answer.clone(),
        },
    );
    assert_eq!(replies(&effects), vec![(7, Reply::Answer(answer))]);
    let tick = leader.tick(4);
    assert!(
        !sends(&tick).contains(&(NodeId(11), commit)),
        "an applied slot is not sent again"
    );
}

#[test]
fn a_slot_that_applied_another_command_answers_unknown() {
    let mut leader = open_leader();
    leader.propose(7, write(1));
    let effects = leader.step(
        NodeId(11),
        Message::Applied {
            segment: SegmentId(1),
            slot: Slot(0),
            command: write(2),
            answer: Answer::Noop,
        },
    );
    assert_eq!(replies(&effects), vec![(7, Reply::Unknown)]);
}

#[test]
fn a_tick_sends_at_most_a_page() {
    let mut leader = open_leader();
    for token in 0..5 {
        leader.propose(token, write(1));
    }
    assert_eq!(accepts(&leader.tick(2)).len(), 2);
}

#[test]
#[should_panic(expected = "at least one round")]
fn a_page_of_zero_is_refused() {
    open_leader().tick(0);
}

#[test]
#[should_panic(expected = "at most RESEND_BATCH")]
fn a_page_above_the_batch_is_refused() {
    open_leader().tick(RESEND_BATCH + 1);
}

#[test]
fn a_leader_stops_on_a_higher_promise_and_answers_unknown() {
    let mut leader = open_leader();
    leader.propose(7, write(1));
    leader.propose(8, write(2));
    // A refusal of another ballot, or that names a lower promise, is not
    // for this leader.
    assert_eq!(
        leader
            .step(
                NodeId(1),
                Message::Nack {
                    ballot: ballot(1),
                    promised: ballot(5),
                }
            )
            .len(),
        0
    );
    let effects = leader.step(
        NodeId(1),
        Message::Nack {
            ballot: ballot(2),
            promised: ballot(5),
        },
    );
    assert_eq!(*leader.phase(), LeaderPhase::Stopped);
    assert_eq!(
        replies(&effects),
        vec![(7, Reply::Unknown), (8, Reply::Unknown)]
    );
    assert_eq!(
        leader.propose(9, write(3)),
        (Proposed::NotLeader, Vec::new())
    );
    assert!(leader.tick(4).is_empty(), "a stopped leader sends nothing");
    assert_eq!(
        leader.step(NodeId(2), promise(2, 0, page(&[], &[]))).len(),
        0
    );
}

#[test]
fn a_leader_in_phase_one_does_not_write() {
    let (mut leader, _) = campaign(&one_segment(), 2, 1, 0);
    assert_eq!(
        leader.propose(1, write(1)),
        (Proposed::NotLeader, Vec::new())
    );
    assert!(leader.segment_of(Slot(0)).is_none());
}

#[test]
fn a_first_leader_seals_the_segment_before_above_its_highest_vote() {
    let value = two_segments();
    let (mut leader, effects) = campaign(&value, 2, 2, 0);
    let asked: Vec<NodeId> = sends(&effects).into_iter().map(|(to, _)| to).collect();
    assert_eq!(
        asked,
        nodes(&[1, 2, 3, 4, 5, 6]),
        "Phase 1 reads both grids"
    );
    for node in [4, 5] {
        leader.step(NodeId(node), promise(2, 0, page(&[], &[])));
    }
    assert_eq!(
        *leader.phase(),
        LeaderPhase::Phase1,
        "the old grid has no quorum yet"
    );
    leader.step(NodeId(1), promise(2, 0, page(&[(2, 1, write(1))], &[])));
    let effects = leader.step(NodeId(2), promise(2, 0, page(&[], &[(4, 1)])));
    // A faulty vote counts for the end (#1313): one above slot 4.
    assert_eq!(*leader.phase(), LeaderPhase::Sealing { end: Slot(5) });
    let seal = Change::Seal {
        segment: SegmentId(1),
        end: Slot(5),
    };
    assert_eq!(effects, vec![LeaderEffect::Register(seal.clone())]);
    assert_eq!(leader.blocked(), &[Slot(4)]);
    assert_eq!(leader.propose(1, write(9)).0, Proposed::NotLeader);
    assert_eq!(leader.tick(4), vec![LeaderEffect::Register(seal.clone())]);
    // A value with no end changes nothing.
    assert_eq!(leader.observe(&value).len(), 0);
    let sealed = value.apply(&seal).expect("the seal");
    let effects = leader.observe(&sealed);
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(5) });
    // Slots 0 to 3 of the old grid are written again in the new grid; the
    // blocked slot 4 is not.
    assert_eq!(
        accepts(&effects),
        BTreeMap::from([
            (Slot(0), Command::Noop),
            (Slot(1), Command::Noop),
            (Slot(2), write(1)),
            (Slot(3), Command::Noop),
        ])
    );
    assert!(
        sends(&effects)
            .iter()
            .all(|(to, _)| [4, 5, 6].contains(&to.0)),
        "every slot is written in the leader's own grid"
    );
    assert_eq!(leader.segment_of(Slot(3)).map(|s| s.id), Some(SegmentId(1)));
    assert_eq!(leader.segment_of(Slot(5)).map(|s| s.id), Some(SegmentId(2)));
    assert_eq!(leader.segments()[0].end, Some(Slot(5)));
    // A chosen slot below the end goes to the replicas of segment 1.
    let accepted = |slot| Message::Accepted {
        ballot: ballot(2),
        slot: Slot(slot),
    };
    leader.step(NodeId(4), accepted(2));
    let effects = leader.step(NodeId(5), accepted(2));
    assert_eq!(
        sends(&effects)
            .into_iter()
            .map(|(to, _)| to)
            .collect::<Vec<_>>(),
        nodes(&[11, 12, 13])
    );
    assert_eq!(leader.propose(1, write(9)).0, Proposed::Slot(Slot(5)));
    // A later done value changes nothing for an open leader.
    assert_eq!(leader.observe(&sealed).len(), 0);
}

#[test]
fn a_first_leader_whose_end_lost_stops() {
    let value = two_segments();
    let (mut leader, _) = campaign(&value, 2, 2, 0);
    for node in [1, 2, 4, 5] {
        leader.step(NodeId(node), promise(2, 0, page(&[], &[])));
    }
    assert_eq!(*leader.phase(), LeaderPhase::Sealing { end: Slot(0) });
    let other = value
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        })
        .expect("another seal");
    leader.observe(&other);
    assert_eq!(*leader.phase(), LeaderPhase::Stopped);
}

#[test]
fn a_refused_seal_stops_the_leader() {
    let value = two_segments();
    let (mut leader, _) = campaign(&value, 2, 2, 0);
    assert_eq!(leader.refused().len(), 0);
    assert_eq!(
        *leader.phase(),
        LeaderPhase::Phase1,
        "a refusal outside the seal is not for it"
    );
    for node in [1, 2, 4, 5] {
        leader.step(NodeId(node), promise(2, 0, page(&[], &[])));
    }
    leader.refused();
    assert_eq!(*leader.phase(), LeaderPhase::Stopped);
}

#[test]
fn the_end_is_at_least_the_first_slot_and_the_start() {
    let value = two_segments();
    let (mut leader, _) = campaign(&value, 2, 2, 7);
    for node in [1, 2, 4, 5] {
        leader.step(NodeId(node), promise(2, 7, page(&[], &[])));
    }
    assert_eq!(*leader.phase(), LeaderPhase::Sealing { end: Slot(7) });
}

#[test]
fn a_leader_of_a_sealed_chain_reads_the_old_segment_below_its_end_only() {
    // Segment 1 sealed at 3; segment 2 in force from 3. A vote above the end
    // in the old grid is not a vote of segment 1.
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        })
        .expect("the seal");
    let (mut leader, _) = campaign(&sealed, 3, 2, 0);
    leader.step(NodeId(1), promise(3, 0, page(&[(5, 1, write(5))], &[])));
    leader.step(NodeId(2), promise(3, 0, page(&[(1, 1, write(1))], &[])));
    leader.step(NodeId(4), promise(3, 0, page(&[], &[])));
    let effects = leader.step(NodeId(5), promise(3, 0, page(&[(3, 2, write(3))], &[])));
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(3) });
    assert_eq!(
        accepts(&effects),
        BTreeMap::from([
            (Slot(0), Command::Noop),
            (Slot(1), write(1)),
            (Slot(2), Command::Noop),
            (Slot(3), write(3)),
        ])
    );
}

#[test]
fn a_two_column_grid_writes_each_slot_in_its_column() {
    let value = RegisterValue::new()
        .apply(&Change::Elect { holder: NodeId(0) })
        .expect("an election");
    let value = reshape(&value, &[&[1, 2, 3], &[4, 5, 6]]);
    let (mut leader, _) = campaign(&value, 2, 1, 0);
    for node in [1, 2, 4] {
        leader.step(NodeId(node), promise(2, 0, page(&[], &[])));
    }
    assert_eq!(
        *leader.phase(),
        LeaderPhase::Phase1,
        "every column needs a quorum"
    );
    leader.step(NodeId(5), promise(2, 0, page(&[], &[])));
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(0) });
    let (_, even) = leader.propose(1, write(1));
    let (_, odd) = leader.propose(2, write(2));
    assert_eq!(
        sends(&even)
            .into_iter()
            .map(|(to, _)| to)
            .collect::<Vec<_>>(),
        nodes(&[1, 2, 3])
    );
    assert_eq!(
        sends(&odd)
            .into_iter()
            .map(|(to, _)| to)
            .collect::<Vec<_>>(),
        nodes(&[4, 5, 6])
    );
    // A vote from the other column does not count.
    leader.step(
        NodeId(4),
        Message::Accepted {
            ballot: ballot(2),
            slot: Slot(0),
        },
    );
    let effects = leader.step(
        NodeId(5),
        Message::Accepted {
            ballot: ballot(2),
            slot: Slot(0),
        },
    );
    assert_eq!(effects.len(), 0);
    leader.step(
        NodeId(1),
        Message::Accepted {
            ballot: ballot(2),
            slot: Slot(0),
        },
    );
    let effects = leader.step(
        NodeId(2),
        Message::Accepted {
            ballot: ballot(2),
            slot: Slot(0),
        },
    );
    assert_eq!(sends(&effects).len(), 3);
}

#[test]
fn a_leader_stopped_by_its_driver_answers_unknown() {
    let mut leader = open_leader();
    leader.propose(4, write(1));
    let effects = leader.stop();
    assert_eq!(replies(&effects), vec![(4, Reply::Unknown)]);
    assert_eq!(*leader.phase(), LeaderPhase::Stopped);
}
