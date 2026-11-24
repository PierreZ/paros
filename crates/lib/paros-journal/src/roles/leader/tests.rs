use super::*;
use crate::RELEASE;
use crate::command::{Fence, Record, Write};
use crate::roles::message::PromisePage;
use crate::segment::{Grid, OpeningCall};
use paros_consensus::journal::{LeaderUuid, Seq};

/// The answers a leader of these tests holds at most.
const WINDOW: usize = 64;
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

/// A write as a leader proposes it at time 0.
fn write(byte: u8) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: LeaderUuid(1),
            expected_seq: Seq(0),
        },
        records: vec![Record::new(vec![byte])],
    })
    .stamped(0)
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

/// A leader of `round` whose bind on `value` is done: it reads the segments
/// of the bound value and its newest one, `segment`, is its own. Returns
/// the leader and the effects of the done bind (the `Prepare`s).
fn campaign(
    value: &RegisterValue,
    round: u64,
    segment: u64,
    from: u64,
) -> (JournalLeader<u32>, Vec<LeaderEffect<u32>>) {
    let (mut leader, effects) = JournalLeader::campaign(Campaign {
        ballot: ballot(round),
        from: Slot(from),
    });
    assert_eq!(
        effects,
        vec![LeaderEffect::Register(Change::Bind {
            ballot: ballot(round)
        })]
    );
    let bound = value
        .apply(&Change::Bind {
            ballot: ballot(round),
        })
        .expect("a bind");
    let effects = leader.observe(&bound);
    assert_eq!(leader.segment(), Some(SegmentId(segment)));
    (leader, effects)
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
        dropped: BTreeMap::new(),
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
    assert_eq!(leader.segment(), Some(SegmentId(1)));
    assert_eq!(
        sends(&effects),
        nodes(&[1, 2, 3])
            .into_iter()
            .map(|to| (
                to,
                Message::Prepare {
                    ballot: ballot(2),
                    from: Slot(3),
                    segment: SegmentId(1),
                }
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn a_campaign_binds_its_ballot_before_phase_one() {
    let (mut leader, effects) = JournalLeader::<u32>::campaign(Campaign {
        ballot: ballot(3),
        from: Slot(0),
    });
    assert_eq!(
        effects,
        vec![LeaderEffect::Register(Change::Bind { ballot: ballot(3) })]
    );
    assert_eq!(*leader.phase(), LeaderPhase::Binding);
    assert_eq!(leader.segment(), None);
    // The bind is asked again at each tick until its value comes.
    assert_eq!(
        leader.tick(1),
        vec![LeaderEffect::Register(Change::Bind { ballot: ballot(3) })]
    );
    // A promise before the bind is not folded, and a call takes no slot.
    assert!(
        leader
            .step(NodeId(1), promise(3, 0, page(&[], &[])))
            .is_empty(),
        "a promise before the bind is not folded"
    );
    assert_eq!(
        leader.propose(1, write(1), 0, WINDOW).0,
        Proposed::NotLeader
    );
    // A value of another write with a lower bound ballot changes nothing.
    let lower = one_segment()
        .apply(&Change::Bind { ballot: ballot(2) })
        .expect("a bind");
    assert!(
        leader.observe(&lower).is_empty(),
        "a lower bound ballot changes nothing"
    );
    assert_eq!(*leader.phase(), LeaderPhase::Binding);
    let bound = lower
        .apply(&Change::Bind { ballot: ballot(3) })
        .expect("a bind");
    assert_eq!(sends(&leader.observe(&bound)).len(), 3);
    assert_eq!(*leader.phase(), LeaderPhase::Phase1);
    assert_eq!(leader.segment(), Some(SegmentId(1)));
}

#[test]
fn a_refused_bind_or_a_higher_bound_ballot_stops_the_leader() {
    let (mut refused, _) = JournalLeader::<u32>::campaign(Campaign {
        ballot: ballot(2),
        from: Slot(0),
    });
    refused.refused();
    assert_eq!(*refused.phase(), LeaderPhase::Stopped);
    let (mut late, _) = JournalLeader::<u32>::campaign(Campaign {
        ballot: ballot(2),
        from: Slot(0),
    });
    let higher = one_segment()
        .apply(&Change::Bind { ballot: ballot(4) })
        .expect("a bind");
    late.observe(&higher);
    assert_eq!(*late.phase(), LeaderPhase::Stopped);
    assert_eq!(late.segment(), None);
}

#[test]
fn a_ballot_bound_after_a_seal_takes_the_next_segment() {
    // Ballot 2 sealed segment 1 at slot 0 and put segment 2 in force. A
    // driver that still holds the value before the seal starts ballot 3:
    // that stale value shows segment 1 in force, but it is no done value of
    // ballot 3's bind, so the leader does not start from it. The bind on
    // the register binds ballot 3 to segment 2, the newest, and its Phase 1
    // reads segment 1 as the sealed grid before its own (control
    // `bind_stale` of `specs/tla/journal-read/`: a ballot above the seal
    // bound to segment 1 chooses a second value in the sealed grid).
    let stale = two_segments()
        .apply(&Change::Bind { ballot: ballot(2) })
        .expect("a bind");
    let sealed = stale
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(0),
        })
        .expect("a seal");
    let (mut leader, _) = JournalLeader::<u32>::campaign(Campaign {
        ballot: ballot(3),
        from: Slot(0),
    });
    assert!(
        leader.observe(&stale).is_empty(),
        "a value without the leader's bind changes nothing"
    );
    assert_eq!(*leader.phase(), LeaderPhase::Binding);
    let bound = sealed
        .apply(&Change::Bind { ballot: ballot(3) })
        .expect("a bind");
    leader.observe(&bound);
    assert_eq!(leader.segment(), Some(SegmentId(2)));
    let read: Vec<(SegmentId, Option<Slot>)> =
        leader.segments().iter().map(|s| (s.id, s.end)).collect();
    assert_eq!(
        read,
        vec![(SegmentId(1), Some(Slot(0))), (SegmentId(2), None)]
    );
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
                segment: SegmentId(1),
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
    let (proposed, effects) = leader.propose(7, write(9), 0, WINDOW);
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
    assert_eq!(
        leader.propose(7, write(1), 0, WINDOW).0,
        Proposed::Slot(Slot(0))
    );
    assert_eq!(
        leader.propose(8, write(2), 0, WINDOW).0,
        Proposed::Slot(Slot(1))
    );
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
    leader.propose(7, write(1), 0, WINDOW);
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
        leader.propose(token, write(1), 0, WINDOW);
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
    leader.propose(7, write(1), 0, WINDOW);
    leader.propose(8, write(2), 0, WINDOW);
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
        leader.propose(9, write(3), 0, WINDOW),
        (Proposed::NotLeader, Vec::new())
    );
    assert!(leader.tick(4).is_empty(), "a stopped leader sends nothing");
    assert_eq!(
        leader.step(NodeId(2), promise(2, 0, page(&[], &[]))).len(),
        0
    );
}

#[test]
fn an_open_leader_whose_segment_a_later_leader_sealed_stops_and_answers_unknown() {
    let mut leader = open_leader();
    leader.propose(7, write(1), 0, WINDOW);
    leader.propose(8, write(2), 0, WINDOW);
    // A value with segment 1 still open changes nothing.
    assert_eq!(replies(&leader.observe(&two_segments())), vec![]);
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(0) });
    // A later leader sealed segment 1 at slot 1: the rounds above wait on
    // acceptors that may retire and never refuse them.
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(1),
        })
        .expect("a seal");
    let effects = leader.observe(&sealed);
    assert_eq!(*leader.phase(), LeaderPhase::Stopped);
    assert_eq!(
        replies(&effects),
        vec![(7, Reply::Unknown), (8, Reply::Unknown)]
    );
    assert!(leader.tick(4).is_empty(), "a stopped leader sends nothing");
}

/// A sealing leader whose blocked slot a late answer dropped opens with
/// the slot in a dropped range: the gap fill writes no `Noop` there.
#[test]
fn a_sealing_leader_skips_a_dropped_blocked_slot_when_it_opens() {
    let value = two_segments();
    let (mut leader, _) = campaign(&value, 2, 2, 0);
    for node in [4, 5] {
        leader.step(NodeId(node), promise(2, 0, page(&[], &[])));
    }
    leader.step(NodeId(1), promise(2, 0, page(&[(2, 1, write(1))], &[])));
    leader.step(NodeId(2), promise(2, 0, page(&[], &[(4, 1)])));
    assert_eq!(*leader.phase(), LeaderPhase::Sealing { end: Slot(5) });
    assert_eq!(leader.blocked(), &[Slot(4)]);
    let mut dropped = page(&[], &[]);
    dropped.dropped.insert(Slot(4), Slot(5));
    leader.step(NodeId(3), promise(2, 0, dropped));
    assert_eq!(leader.blocked().len(), 0);
    let sealed = value
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(5),
        })
        .expect("the seal");
    let effects = leader.observe(&sealed);
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(5) });
    assert_eq!(
        accepts(&effects),
        BTreeMap::from([
            (Slot(0), Command::Noop),
            (Slot(1), Command::Noop),
            (Slot(2), write(1)),
            (Slot(3), Command::Noop),
        ]),
        "the dropped slot 4 is not written"
    );
}

#[test]
fn a_leader_in_phase_one_does_not_write() {
    let (mut leader, _) = campaign(&one_segment(), 2, 1, 0);
    assert_eq!(
        leader.propose(1, write(1), 0, WINDOW),
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
    assert_eq!(
        leader.propose(1, write(9), 0, WINDOW).0,
        Proposed::NotLeader
    );
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
    assert_eq!(
        leader.propose(1, write(9), 0, WINDOW).0,
        Proposed::Slot(Slot(5))
    );
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

/// A sealing leader whose own seal a later operation made done, and whose
/// own segment a later leader then sealed, stops on that value: its
/// segment takes no more writes. Opening it would hold its callers'
/// commands on acceptors that refuse them.
#[test]
fn a_sealing_leader_whose_segment_a_later_leader_sealed_stops() {
    let value = two_segments();
    let (mut leader, _) = campaign(&value, 2, 2, 0);
    for node in [1, 2, 4, 5] {
        leader.step(NodeId(node), promise(2, 0, page(&[], &[])));
    }
    assert_eq!(*leader.phase(), LeaderPhase::Sealing { end: Slot(0) });
    let one = SegmentId(1);
    let later = value
        .apply(&Change::Seal {
            segment: one,
            end: Slot(0),
        })
        .and_then(|v| {
            v.apply(&Change::Boundary {
                segment: one,
                state: crate::fold::BoundaryState::fresh(crate::command::Leader::Unset),
            })
        })
        .and_then(|v| {
            v.apply(&Change::Retire {
                segment: one,
                holders: 2,
                unrepaired_damage: false,
            })
        })
        .expect("segment 1 sealed at the leader's end, then retired");
    let later = reshape(&later, &[&[7, 8, 9]])
        .apply(&Change::Seal {
            segment: SegmentId(2),
            end: Slot(4),
        })
        .expect("a later leader sealed segment 2");
    leader.observe(&later);
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
fn a_leader_of_a_sealed_chain_reads_every_slot_of_the_old_grid() {
    // Segment 1 sealed at 3; segment 2 in force from 3. A vote above the end
    // in the old grid is a report like any other: no vote of the new grid
    // is there, so the leader proposes it again, in its own grid. The end
    // hides nothing from Phase 1.
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
            (Slot(4), Command::Noop),
            (Slot(5), write(5)),
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
    let (_, even) = leader.propose(1, write(1), 0, WINDOW);
    let (_, odd) = leader.propose(2, write(2), 0, WINDOW);
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
    leader.propose(4, write(1), 0, WINDOW);
    let effects = leader.stop();
    assert_eq!(replies(&effects), vec![(4, Reply::Unknown)]);
    assert_eq!(*leader.phase(), LeaderPhase::Stopped);
}

#[test]
fn a_retired_segment_is_not_read() {
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(0),
        })
        .expect("the seal");
    let state = crate::fold::BoundaryState::fresh(crate::command::Leader::Unset);
    let retired = sealed
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state,
        })
        .and_then(|value| {
            value.apply(&Change::Retire {
                segment: SegmentId(1),
                holders: 2,
                unrepaired_damage: false,
            })
        })
        .expect("the retire");
    let (leader, effects) = campaign(&retired, 3, 2, 0);
    assert_eq!(
        sends(&effects)
            .into_iter()
            .map(|(to, _)| to)
            .collect::<Vec<_>>(),
        nodes(&[4, 5, 6]),
        "only the grid in force is asked"
    );
    assert_eq!(leader.segments().len(), 1);
}

#[test]
fn a_retire_between_the_campaign_and_the_bind_raises_the_first_slot() {
    // The driver read `from` = 0, the start of segment 1, before the bind.
    // Segment 1 (sealed at slot 3) retires before the bind is done, so the
    // leader reads segment 2 only. Every slot below its start is in a
    // retired segment, chosen and held by `F + 1` replicas: the leader
    // starts at slot 3 and never fills slots 0 to 2 with a `Noop`.
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        })
        .expect("the seal");
    let state = crate::fold::BoundaryState::fresh(crate::command::Leader::Unset);
    let retired = sealed
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state,
        })
        .and_then(|value| {
            value.apply(&Change::Retire {
                segment: SegmentId(1),
                holders: 2,
                unrepaired_damage: false,
            })
        })
        .expect("the retire");
    let (mut leader, _) = JournalLeader::<u32>::campaign(Campaign {
        ballot: ballot(3),
        from: Slot(0),
    });
    let bound = retired
        .apply(&Change::Bind { ballot: ballot(3) })
        .expect("a bind");
    let effects = leader.observe(&bound);
    assert_eq!(leader.from(), Slot(3), "the first slot is the start read");
    assert!(
        sends(&effects)
            .iter()
            .all(|(_, message)| matches!(message, Message::Prepare { from: Slot(3), .. })),
        "Phase 1 asks from the start of segment 2"
    );
    for node in [4, 5, 6] {
        leader.step(NodeId(node), promise(3, 3, page(&[], &[])));
    }
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(3) });
}

#[test]
fn a_refusal_at_its_own_ballot_does_not_stop_the_leader() {
    let mut leader = open_leader();
    leader.step(
        NodeId(1),
        Message::Nack {
            ballot: ballot(2),
            promised: ballot(2),
        },
    );
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(0) });
}

#[test]
#[should_panic(expected = "only an open leader chooses")]
fn a_leader_in_phase_one_has_no_chosen_slot() {
    let (mut leader, _) = campaign(&one_segment(), 2, 1, 0);
    leader.unapplied.insert(
        Slot(0),
        Unapplied {
            segment: SegmentId(1),
            command: Command::Noop,
        },
    );
    leader.assert_invariants();
}

#[test]
#[should_panic(expected = "in one write")]
fn a_pending_segment_follows_one_with_no_end() {
    let (mut leader, _) = campaign(&two_segments(), 2, 2, 0);
    leader.segments[0].end = Some(Slot(3));
    leader.assert_invariants();
}

/// An open leader of segment 2, with segment 1 sealed at 3 (its replicas
/// 11, 12, 13), and the register value it holds.
fn open_on_sealed() -> (JournalLeader<u32>, RegisterValue) {
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        })
        .expect("the seal");
    let (mut leader, _) = campaign(&sealed, 3, 2, 3);
    for node in [1, 2, 4, 5] {
        leader.step(NodeId(node), promise(3, 3, page(&[], &[])));
    }
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(3) });
    (leader, sealed)
}

fn holds(segment: u64, full: bool) -> Message {
    Message::Holds {
        segment: SegmentId(segment),
        full,
    }
}

/// The receivers of the `Holding`s of `effects`.
fn holding(effects: &[LeaderEffect<u32>]) -> Vec<NodeId> {
    sends(effects)
        .into_iter()
        .filter_map(|(to, message)| {
            (message
                == Message::Holding {
                    segment: SegmentId(1),
                })
            .then_some(to)
        })
        .collect()
}

fn retires(effects: &[LeaderEffect<u32>]) -> Vec<Change> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            LeaderEffect::Register(change @ Change::Retire { .. }) => Some(change.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn an_open_leader_retires_the_sealed_segment_at_f_plus_one_holders() {
    let (mut leader, sealed) = open_on_sealed();
    let effects = leader.tick(4);
    assert_eq!(holding(&effects), nodes(&[11, 12, 13]));
    assert!(retires(&effects).is_empty(), "no holder yet");
    assert_eq!(leader.step(NodeId(11), holds(1, true)), vec![]);
    leader.step(NodeId(12), holds(1, false));
    leader.step(NodeId(99), holds(1, true));
    leader.step(NodeId(13), holds(2, true));
    let effects = leader.tick(4);
    assert_eq!(
        holding(&effects),
        nodes(&[12, 13]),
        "only a replica of the set that holds the segment counts (control retire_f)"
    );
    assert!(retires(&effects).is_empty(), "one holder of F + 1");
    leader.step(NodeId(13), holds(1, true));
    let effects = leader.tick(4);
    assert_eq!(holding(&effects), nodes(&[12]));
    let retire = Change::Retire {
        segment: SegmentId(1),
        holders: 2,
        unrepaired_damage: false,
    };
    assert_eq!(retires(&effects), vec![retire.clone()]);
    // The register refuses the retire until the boundary state is written
    // (control retire_norecord): the leader is not stopped and asks again.
    assert!(sealed.apply(&retire).is_err());
    assert_eq!(leader.refused(), vec![]);
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(3) });
    assert!(
        leader.observe(&sealed).is_empty(),
        "a value with the segment sealed changes nothing"
    );
    assert_eq!(retires(&leader.tick(4)), vec![retire.clone()]);
    let retired = sealed
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state: crate::fold::BoundaryState::fresh(crate::command::Leader::Unset),
        })
        .and_then(|value| value.apply(&retire))
        .expect("the retire");
    assert_eq!(
        leader.observe(&retired),
        vec![LeaderEffect::Retired {
            segment: SegmentId(1)
        }],
        "the old acceptors stop only on a done retire (RetireKept)"
    );
    assert_eq!(leader.observe(&retired), vec![], "said once");
    let effects = leader.tick(4);
    assert_eq!(holding(&effects), vec![]);
    assert_eq!(retires(&effects), vec![]);
    assert_eq!(leader.step(NodeId(12), holds(1, true)), vec![]);
}

#[test]
fn a_retire_ends_the_rounds_and_the_commits_of_the_retired_segment() {
    // Segment 1 sealed at 3, with votes at slots 1 and 2 that the leader
    // proposes again in segment 1.
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        })
        .expect("the seal");
    let (mut leader, _) = campaign(&sealed, 3, 2, 1);
    let votes = page(&[(1, 2, write(1)), (2, 2, write(2))], &[]);
    for node in [1, 2] {
        leader.step(NodeId(node), promise(3, 1, votes.clone()));
    }
    for node in [4, 5] {
        leader.step(NodeId(node), promise(3, 1, page(&[], &[])));
    }
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(3) });
    // Phase 2 runs on the leader's own grid. Slot 1 is chosen and
    // committed to the replicas of segment 1; slot 2 waits on its round.
    for node in [4, 5] {
        leader.step(
            NodeId(node),
            Message::Accepted {
                ballot: ballot(3),
                slot: Slot(1),
            },
        );
    }
    let tick = leader.tick(4);
    let commit = Message::Commit {
        segment: SegmentId(1),
        slot: Slot(1),
        command: write(1),
    };
    assert!(sends(&tick).contains(&(NodeId(11), commit.clone())));
    assert_eq!(accepts(&tick), BTreeMap::from([(Slot(2), write(2))]));
    let retired = sealed
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state: crate::fold::BoundaryState::fresh(crate::command::Leader::Unset),
        })
        .and_then(|value| {
            value.apply(&Change::Retire {
                segment: SegmentId(1),
                holders: 2,
                unrepaired_damage: false,
            })
        })
        .expect("the retire");
    assert_eq!(
        leader.observe(&retired),
        vec![LeaderEffect::Retired {
            segment: SegmentId(1)
        }]
    );
    // The replicas of a retired segment answer nothing and its acceptors
    // stop: the leader sends neither the commit nor the accept again.
    let tick = leader.tick(4);
    assert!(!sends(&tick).contains(&(NodeId(11), commit)));
    assert_eq!(accepts(&tick).len(), 0);
    leader.assert_invariants();
}

#[test]
fn a_leader_with_no_sealed_segment_retires_nothing() {
    let mut leader = open_leader();
    leader.step(NodeId(11), holds(1, true));
    leader.step(NodeId(12), holds(1, true));
    let effects = leader.tick(4);
    assert_eq!(holding(&effects), vec![]);
    assert_eq!(retires(&effects), vec![]);
    let (mut stopped, _) = open_on_sealed();
    stopped.step(NodeId(11), holds(1, true));
    stopped.stop();
    stopped.step(NodeId(12), holds(1, true));
    assert!(
        stopped.tick(4).is_empty(),
        "a stopped leader retires nothing"
    );
}

#[test]
fn a_sealing_leader_counts_no_holder() {
    let value = two_segments();
    let (mut leader, _) = campaign(&value, 2, 2, 0);
    for node in [1, 2, 4, 5] {
        leader.step(NodeId(node), promise(2, 0, page(&[], &[])));
    }
    assert!(matches!(leader.phase(), LeaderPhase::Sealing { .. }));
    leader.step(NodeId(11), holds(1, true));
    leader.step(NodeId(12), holds(1, true));
    assert_eq!(retires(&leader.tick(4)), vec![]);
}

#[test]
fn the_holders_a_retire_needs_are_f_plus_one_of_the_grid() {
    let (leader, _) = open_on_sealed();
    assert_eq!(leader.segments()[0].acks(), 2);
    let wide = reshape(
        &RegisterValue::new()
            .apply(&Change::Elect { holder: NodeId(0) })
            .expect("an election"),
        &[&[1, 2, 3], &[4, 5, 6]],
    );
    let (leader, _) = campaign(&wide, 2, 1, 0);
    assert_eq!(leader.segments()[0].acks(), 2);
    let value = RegisterValue::new()
        .apply(&Change::Elect { holder: NodeId(0) })
        .expect("an election");
    let five = value
        .apply(&Change::Reshape {
            opening: OpeningCall {
                term: value.term().number,
                expected: None,
                grid: Grid::new(2, vec![nodes(&[1, 2, 3, 4, 5])]).expect("a grid"),
                replicas: nodes(&[11, 12, 13, 14, 15]),
            },
            release: RELEASE,
        })
        .expect("a reshape");
    let (leader, _) = campaign(&five, 2, 1, 0);
    assert_eq!(
        leader.segments()[0].acks(),
        3,
        "F = 2: three holders, a majority of five"
    );
}

/// An open leader of segment 1 that delegates to proxies 201 and 202.
fn delegating_leader() -> JournalLeader<u32> {
    let value = one_segment();
    let (mut leader, _) = campaign(&value, 2, 1, 0);
    leader.delegate_to(nodes(&[201, 202]));
    leader.step(NodeId(1), promise(2, 0, page(&[], &[])));
    leader.step(NodeId(2), promise(2, 0, page(&[], &[])));
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(0) });
    leader
}

#[test]
fn a_delegating_leader_gives_each_round_to_a_proxy() {
    let mut leader = delegating_leader();
    let (_, first) = leader.propose(1, write(1), 0, WINDOW);
    let (_, second) = leader.propose(2, write(2), 0, WINDOW);
    let first = sends(&first);
    assert_eq!(first.len(), 1, "one delegation, no Accept: {first:?}");
    let (to, message) = &first[0];
    assert_eq!(*to, NodeId(201), "slot 0 goes to the proxy at 0");
    let Message::Delegate {
        slot,
        column,
        segment,
        replicas,
        ..
    } = message
    else {
        panic!("a delegation: {message:?}");
    };
    assert_eq!(*slot, Slot(0));
    assert_eq!(*column, nodes(&[1, 2, 3]));
    assert_eq!(*segment, SegmentId(1));
    assert_eq!(*replicas, nodes(&[11, 12, 13]));
    assert_eq!(sends(&second)[0].0, NodeId(202), "slot 1 goes to the next");
    // An `Accepted` that reaches the leader for a delegated round is not
    // counted: the votes are the proxy's.
    for acceptor in [1, 2, 3] {
        let effects = leader.step(
            NodeId(acceptor),
            Message::Accepted {
                ballot: ballot(2),
                slot: Slot(0),
            },
        );
        assert_eq!(effects, Vec::new());
    }
}

#[test]
fn a_chosen_from_a_proxy_commits_the_slot_and_holds_the_answer() {
    let mut leader = delegating_leader();
    leader.propose(1, write(1), 0, WINDOW);
    // A `Chosen` of another ballot changes nothing.
    assert_eq!(
        leader.step(
            NodeId(201),
            Message::Chosen {
                ballot: ballot(1),
                slot: Slot(0),
            },
        ),
        Vec::new()
    );
    let effects = leader.step(
        NodeId(201),
        Message::Chosen {
            ballot: ballot(2),
            slot: Slot(0),
        },
    );
    let commits: Vec<NodeId> = sends(&effects)
        .into_iter()
        .filter(|(_, m)| matches!(m, Message::Commit { slot: Slot(0), .. }))
        .map(|(to, _)| to)
        .collect();
    assert_eq!(commits, nodes(&[11, 12, 13]));
    assert!(
        replies(&effects).is_empty(),
        "the answer waits for a replica"
    );
    // A second `Chosen` finds no open round.
    assert_eq!(
        leader.step(
            NodeId(202),
            Message::Chosen {
                ballot: ballot(2),
                slot: Slot(0),
            },
        ),
        Vec::new()
    );
}

#[test]
fn a_stalled_delegation_moves_to_the_next_proxy() {
    let mut leader = delegating_leader();
    leader.propose(1, write(1), 0, WINDOW);
    let mut proxies = Vec::new();
    for _ in 0..=MOVE_AFTER {
        let effects = leader.tick(4);
        proxies.extend(
            sends(&effects)
                .into_iter()
                .filter_map(|(to, m)| matches!(m, Message::Delegate { .. }).then_some(to)),
        );
    }
    assert_eq!(
        proxies.first(),
        Some(&NodeId(201)),
        "the first re-sends go to the same proxy"
    );
    assert_eq!(
        proxies.last(),
        Some(&NodeId(202)),
        "after MOVE_AFTER re-sends the round moves"
    );
}

#[test]
fn a_nack_that_a_proxy_relays_stops_the_leader() {
    let mut leader = delegating_leader();
    leader.propose(1, write(1), 0, WINDOW);
    let effects = leader.step(
        NodeId(201),
        Message::Nack {
            ballot: ballot(2),
            promised: ballot(3),
        },
    );
    assert_eq!(*leader.phase(), LeaderPhase::Stopped);
    assert_eq!(replies(&effects), vec![(1, Reply::Unknown)]);
}

#[test]
#[should_panic(expected = "the leader delegates before it opens a round")]
fn a_leader_delegates_only_before_its_first_round() {
    let mut leader = open_leader();
    leader.propose(1, write(1), 0, WINDOW);
    leader.delegate_to(nodes(&[201]));
}

#[test]
fn a_retire_seen_in_phase_one_is_kept_once_the_leader_opens() {
    let sealed = two_segments()
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        })
        .expect("the seal");
    let (mut leader, _) = campaign(&sealed, 3, 2, 3);
    let retired = sealed
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state: crate::fold::BoundaryState::fresh(crate::command::Leader::Unset),
        })
        .and_then(|value| {
            value.apply(&Change::Retire {
                segment: SegmentId(1),
                holders: 2,
                unrepaired_damage: false,
            })
        })
        .expect("another leader retired segment 1");
    assert_eq!(leader.observe(&retired), Vec::new());
    for node in [1, 2, 4, 5] {
        leader.step(NodeId(node), promise(3, 3, page(&[], &[])));
    }
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(3) });
    let effects = leader.tick(4);
    assert_eq!(holding(&effects), vec![], "the leader asks no holder");
    assert_eq!(retires(&effects), vec![], "and writes no retire again");
}

#[test]
fn a_proposal_carries_the_leader_time_of_its_write() {
    let mut leader = open_leader();
    let mut client = Record::new(vec![1, 2]);
    client.headers.leader_time = Some(99);
    let given = Command::Write(Write {
        fence: Fence::Leader {
            uuid: LeaderUuid(1),
            expected_seq: Seq(0),
        },
        records: vec![client, Record::new(vec![3])],
    });
    let (_, effects) = leader.propose(7, given, 42, WINDOW);
    let accepted = accepts(&effects);
    let Some(Command::Write(write)) = accepted.values().next() else {
        panic!("an Accept of the write: {accepted:?}");
    };
    assert_eq!(
        write
            .records
            .iter()
            .map(|r| r.headers.leader_time)
            .collect::<Vec<_>>(),
        vec![Some(42), Some(42)],
        "one time for the write, and the client's time is replaced"
    );
    let (_, effects) = leader.propose(8, Command::Noop, 43, WINDOW);
    assert_eq!(
        accepts(&effects).values().next(),
        Some(&Command::Noop),
        "a command with no record is not stamped"
    );
}

#[test]
fn only_the_replicas_of_a_segment_count_toward_its_compaction() {
    let mut leader = open_leader();
    let report = |segment: u64| Message::CompactedTo {
        segment: SegmentId(segment),
        compacted: Slot(2),
        answers: Slot(2),
    };
    let compacts = |effects: &[LeaderEffect<u32>]| {
        sends(effects)
            .into_iter()
            .filter(|(_, m)| matches!(m, Message::Compact { .. }))
            .count()
    };
    // Two nodes outside the set of segment 1, and a replica of segment 1
    // that reports for another segment: none counts.
    let mut effects = leader.step(NodeId(21), report(1));
    effects.extend(leader.step(NodeId(22), report(1)));
    effects.extend(leader.step(NodeId(11), report(9)));
    effects.extend(leader.tick(4));
    assert_eq!(compacts(&effects), 0, "no report of the set: no compaction");
    // Two replicas of the set are F + 1: the acceptors drop the votes.
    let mut effects = leader.step(NodeId(11), report(1));
    effects.extend(leader.step(NodeId(12), report(1)));
    effects.extend(leader.tick(4));
    assert!(compacts(&effects) > 0, "F + 1 replicas compacted");
}

#[test]
fn a_first_leader_seals_above_a_dropped_range() {
    let value = two_segments();
    let (mut leader, _) = campaign(&value, 2, 2, 0);
    for node in [4, 5] {
        leader.step(NodeId(node), promise(2, 0, page(&[], &[])));
    }
    let mut dropped = page(&[], &[]);
    dropped.dropped = BTreeMap::from([(Slot(0), Slot(3))]);
    leader.step(NodeId(1), promise(2, 0, dropped));
    leader.step(NodeId(2), promise(2, 0, page(&[], &[])));
    // A dropped range counts as voted: the end is one above its last slot.
    assert_eq!(*leader.phase(), LeaderPhase::Sealing { end: Slot(3) });
}

#[test]
fn a_blocked_slot_above_every_vote_is_never_given_to_a_new_command() {
    let value = one_segment();
    let (mut leader, _) = campaign(&value, 3, 1, 0);
    leader.step(NodeId(1), promise(3, 0, page(&[], &[(2, 2)])));
    leader.step(NodeId(2), promise(3, 0, page(&[], &[])));
    assert_eq!(leader.blocked(), &[Slot(2)]);
    // The faulty vote at slot 2 may be the chosen value: the gap fill
    // writes a `Noop` below it, and the callers write above it.
    for call in 0..3 {
        let (proposed, _) = leader.propose(call, write(9), 0, WINDOW);
        assert_eq!(proposed, Proposed::Slot(Slot(3 + u64::from(call))));
    }
}

/// A leader of segment 1 whose Phase 1 left slot 1 blocked: node 1 voted
/// `write(1)` at ballot 1 there, node 2 reports a faulty vote at ballot 2.
fn blocked_leader() -> JournalLeader<u32> {
    let value = one_segment();
    let (mut leader, _) = campaign(&value, 3, 1, 0);
    leader.step(NodeId(1), promise(3, 0, page(&[(1, 1, write(1))], &[])));
    leader.step(NodeId(2), promise(3, 0, page(&[], &[(1, 2)])));
    assert_eq!(*leader.phase(), LeaderPhase::Open { start: Slot(0) });
    assert_eq!(leader.blocked(), &[Slot(1)]);
    leader
}

fn chosen_at(leader: &mut JournalLeader<u32>, slot: u64) -> Vec<LeaderEffect<u32>> {
    let accepted = Message::Accepted {
        ballot: ballot(3),
        slot: Slot(slot),
    };
    leader.step(NodeId(1), accepted.clone());
    leader.step(NodeId(3), accepted)
}

#[test]
fn a_tick_asks_the_missing_answers_and_the_replicas_of_a_blocked_slot() {
    let mut leader = blocked_leader();
    let asks = sends(&leader.tick(RESEND_BATCH));
    assert!(asks.contains(&(
        NodeId(3),
        Message::Prepare {
            ballot: ballot(3),
            from: Slot(0),
            segment: SegmentId(1),
        }
    )));
    for replica in [11, 12, 13] {
        assert!(asks.contains(&(
            NodeId(replica),
            Message::Learn {
                segment: SegmentId(1),
                slot: Slot(1),
            }
        )));
    }
    // A leader with no blocked slot asks nothing of the kind.
    let mut open = open_leader();
    assert!(
        sends(&open.tick(RESEND_BATCH))
            .iter()
            .all(|(_, m)| !matches!(m, Message::Learn { .. } | Message::Prepare { .. }))
    );
}

/// A late answer that dropped the blocked slot proves that the slot is
/// chosen and applied at `F + 1` replicas. Its page shows no vote there,
/// and the other answers predate the compaction: the tally would read a
/// stale vote. The leader proposes nothing there and repairs nothing
/// (`specs/tla/journal-ctrl/`, `ProbeDecide`, control `ctrl_probe_drop`).
#[test]
fn a_late_answer_that_dropped_a_blocked_slot_skips_it() {
    let mut leader = blocked_leader();
    let mut dropped = page(&[], &[]);
    dropped.dropped.insert(Slot(0), Slot(2));
    let effects = leader.step(NodeId(3), promise(3, 0, dropped));
    assert_eq!(leader.blocked().len(), 0);
    assert_eq!(accepts(&effects), BTreeMap::new());
    let effects = chosen_at(&mut leader, 1);
    assert!(
        sends(&effects)
            .iter()
            .all(|(_, m)| !matches!(m, Message::Repair { .. }))
    );
    // A later tick asks nothing more for the slot.
    assert!(
        sends(&leader.tick(RESEND_BATCH))
            .iter()
            .all(|(_, m)| !matches!(m, Message::Learn { .. } | Message::Prepare { .. }))
    );
}

#[test]
fn a_promise_at_another_ballot_does_not_decide_a_blocked_slot() {
    let mut leader = blocked_leader();
    // The page would decide slot 1 at ballot 3, but it answers ballot 2.
    let effects = leader.step(NodeId(3), promise(2, 0, page(&[], &[])));
    assert_eq!(effects, Vec::new());
    assert_eq!(leader.blocked(), &[Slot(1)]);
}

#[test]
fn a_late_answer_that_decides_a_blocked_slot_proposes_it_and_repairs_the_faulty_vote() {
    let mut leader = blocked_leader();
    // Node 3 has no vote at slot 1: with node 1 it holds a quorum that
    // qualifies, and the readable vote at the threshold is safe.
    let effects = leader.step(NodeId(3), promise(3, 0, page(&[], &[])));
    assert_eq!(leader.blocked().len(), 0);
    assert_eq!(accepts(&effects), BTreeMap::from([(Slot(1), write(1))]));
    let effects = chosen_at(&mut leader, 1);
    let repair = Message::Repair {
        ballot: ballot(3),
        slot: Slot(1),
        command: write(1),
    };
    assert!(
        sends(&effects).contains(&(NodeId(2), repair)),
        "the faulty acceptor gets the chosen command"
    );
    assert_eq!(
        sends(&effects)
            .iter()
            .filter(|(_, m)| matches!(m, Message::Repair { .. }))
            .count(),
        1,
        "only the faulty acceptor is repaired"
    );
}

#[test]
fn a_replica_that_applied_a_blocked_slot_unblocks_it() {
    let mut leader = blocked_leader();
    let learned = |segment| Message::Learned {
        segment: SegmentId(segment),
        slot: Slot(1),
        command: write(7),
    };
    // An answer from a node outside the set, or of another segment, counts
    // for nothing.
    assert_eq!(leader.step(NodeId(9), learned(1)).len(), 0);
    assert_eq!(leader.step(NodeId(11), learned(2)).len(), 0);
    assert_eq!(leader.blocked(), &[Slot(1)]);
    let effects = leader.step(NodeId(11), learned(1));
    assert_eq!(leader.blocked().len(), 0);
    assert_eq!(accepts(&effects), BTreeMap::from([(Slot(1), write(7))]));
    // A second answer finds the slot proposed already.
    assert_eq!(leader.step(NodeId(12), learned(1)).len(), 0);
    let effects = chosen_at(&mut leader, 1);
    assert!(sends(&effects).contains(&(
        NodeId(2),
        Message::Repair {
            ballot: ballot(3),
            slot: Slot(1),
            command: write(7),
        }
    )));
    // A slot that was never blocked sends no repair when it is chosen.
    let (proposed, _) = leader.propose(1, write(2), 0, WINDOW);
    let Proposed::Slot(slot) = proposed else {
        panic!("a slot")
    };
    let effects = chosen_at(&mut leader, slot.0);
    assert!(
        sends(&effects)
            .iter()
            .all(|(_, m)| !matches!(m, Message::Repair { .. }))
    );
}

#[test]
fn a_late_answer_that_does_not_decide_keeps_the_slot_blocked() {
    let mut leader = blocked_leader();
    // Node 3 reports a faulty vote above the threshold too.
    let effects = leader.step(NodeId(3), promise(3, 0, page(&[], &[(1, 2)])));
    assert_eq!(effects.len(), 0);
    assert_eq!(leader.blocked(), &[Slot(1)]);
    // An answer of another ballot is not folded.
    assert_eq!(
        leader.step(NodeId(3), promise(4, 0, page(&[], &[]))).len(),
        0
    );
}

#[test]
fn a_leader_with_its_window_full_answers_busy_and_holds_no_more() {
    let mut leader = open_leader();
    // No round closes (a column below quorum): each proposal holds its
    // answer, and the window bounds what the leader holds.
    assert_eq!(leader.propose(1, write(1), 0, 2).0, Proposed::Slot(Slot(0)));
    assert_eq!(leader.propose(2, write(2), 0, 2).0, Proposed::Slot(Slot(1)));
    let (proposed, effects) = leader.propose(3, write(3), 0, 2);
    assert_eq!(proposed, Proposed::Busy, "the window is full");
    assert!(effects.is_empty(), "a busy proposal sends nothing");
    assert_eq!(
        leader.propose(4, write(4), 0, 3).0,
        Proposed::Slot(Slot(2)),
        "a larger window takes the proposal"
    );
}

#[test]
#[should_panic(expected = "a leader holds at least one answer")]
fn a_window_of_zero_is_a_programmer_error() {
    let mut leader = open_leader();
    let _ = leader.propose(1, write(1), 0, 0);
}
