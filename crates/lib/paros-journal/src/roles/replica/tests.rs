use super::*;
use crate::RELEASE;
use crate::command::Fence;
use crate::segment::{Grid, OpeningCall, SegmentRecord};

const LEADER: NodeId = NodeId(101);
const ME: NodeId = NodeId(11);

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn reshape(value: &RegisterValue) -> RegisterValue {
    let newest = value.newest().map(SegmentRecord::id);
    let id = SegmentId(newest.map_or(1, |SegmentId(n)| n + 1));
    let grid = Grid::new(1, vec![nodes(&[1, 2, 3])]).expect("a grid");
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

fn sealed(end: u64) -> RegisterValue {
    reshape(&one_segment())
        .apply(&Change::Seal {
            segment: SegmentId(1),
            end: Slot(end),
        })
        .expect("a seal")
}

fn replica() -> SegmentReplica {
    SegmentReplica::recover(
        ME,
        SegmentId(1),
        nodes(&[11, 12, 13]),
        Leader::Unset,
        ReplicaDisk::default(),
    )
}

fn claim(uuid: u128) -> Command {
    Command::SetLeader {
        new: Some(LeaderUuid(uuid)),
        old: Leader::Unset,
    }
}

fn write(uuid: u128, at: u64, byte: u8) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: LeaderUuid(uuid),
            expected_seq: Seq(at),
        },
        records: vec![Record::new(vec![byte])],
    })
}

fn commit(segment: u64, slot: u64, command: Command) -> Message {
    Message::Commit {
        segment: SegmentId(segment),
        slot: Slot(slot),
        command,
    }
}

/// The `Applied` slots of `effects`, each with the batch it waits on.
fn applied(effects: &[ReplicaEffect]) -> Vec<(Slot, Option<PersistId>)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            ReplicaEffect::Send {
                to,
                message: Message::Applied { slot, .. },
                after,
            } => {
                assert_eq!(*to, LEADER, "the answer goes to the committer");
                Some((*slot, *after))
            }
            _ => None,
        })
        .collect()
}

fn persists(effects: &[ReplicaEffect]) -> Vec<(PersistId, Vec<ReplicaOp>)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            ReplicaEffect::Persist { id, ops } => Some((*id, ops.clone())),
            _ => None,
        })
        .collect()
}

/// A replica of segment 1 that took its base.
fn based() -> SegmentReplica {
    let mut replica = replica();
    replica.observe(&one_segment());
    replica
}

#[test]
fn the_first_segment_folds_from_the_fresh_journal() {
    let mut replica = replica();
    assert_eq!(replica.id(), ME);
    assert_eq!(replica.segment(), SegmentId(1));
    assert_eq!(replica.front(), None);
    assert!(
        replica.tick().is_empty(),
        "a replica with no base asks nothing"
    );
    let effects = replica.observe(&one_segment());
    assert_eq!(
        persists(&effects),
        vec![(
            PersistId(0),
            vec![ReplicaOp::Base {
                boundary: BoundaryState::fresh(Leader::Unset),
                start: Slot(0),
            }]
        )]
    );
    assert_eq!(replica.front(), Some(Slot(0)));
    assert_eq!(replica.start(), Some(Slot(0)));
    assert_eq!(
        replica.state().map(JournalState::leader),
        Some(Leader::Unset)
    );
    assert!(
        persists(&replica.observe(&one_segment())).is_empty(),
        "the base is taken once"
    );
}

#[test]
fn a_chosen_slot_is_durable_before_its_answer_leaves() {
    let mut replica = based();
    let effects = replica.step(LEADER, commit(1, 0, claim(7)));
    assert_eq!(
        persists(&effects),
        vec![(
            PersistId(1),
            vec![ReplicaOp::Chosen {
                slot: Slot(0),
                command: claim(7),
            }]
        )]
    );
    assert_eq!(applied(&effects), vec![(Slot(0), Some(PersistId(1)))]);
    assert_eq!(replica.front(), Some(Slot(1)));
    assert_eq!(
        replica.state().map(JournalState::leader),
        Some(Leader::Uuid(LeaderUuid(7)))
    );
    assert_eq!(replica.chosen().len(), 1);
}

#[test]
fn the_fold_never_steps_over_a_hole() {
    let mut replica = based();
    let effects = replica.step(LEADER, commit(1, 1, write(7, 0, 1)));
    assert!(applied(&effects).is_empty(), "slot 1 waits for slot 0");
    assert_eq!(replica.front(), Some(Slot(0)));
    let effects = replica.step(LEADER, commit(1, 0, claim(7)));
    assert_eq!(
        applied(&effects),
        vec![(Slot(0), Some(PersistId(2))), (Slot(1), Some(PersistId(2)))]
    );
    assert_eq!(replica.front(), Some(Slot(2)));
    assert_eq!(replica.state().map(JournalState::next_seq), Some(Seq(1)));
}

#[test]
fn a_commit_again_answers_from_the_fold_with_no_write() {
    let mut replica = based();
    replica.step(LEADER, commit(1, 0, claim(7)));
    replica.step(LEADER, commit(1, 1, write(7, 0, 1)));
    let effects = replica.step(LEADER, commit(1, 0, claim(7)));
    assert_eq!(persists(&effects).len(), 0);
    assert_eq!(applied(&effects), vec![(Slot(0), Some(PersistId(2)))]);
    let [
        ReplicaEffect::Send {
            message: Message::Applied { answer, .. },
            ..
        },
    ] = &effects[..]
    else {
        panic!("one answer: {effects:?}");
    };
    assert!(matches!(answer, Answer::LeaderSet(_)));
}

#[test]
#[should_panic(expected = "Paxos agreement")]
fn two_commands_chosen_at_one_slot_break_paxos() {
    let mut replica = based();
    replica.step(LEADER, commit(1, 0, claim(7)));
    replica.step(LEADER, commit(1, 0, claim(8)));
}

#[test]
fn a_commit_of_another_segment_or_past_the_end_is_not_taken() {
    let mut replica = based();
    assert_eq!(replica.step(LEADER, commit(2, 0, claim(7))).len(), 0);
    replica.observe(&sealed(1));
    assert_eq!(replica.end(), Some(Slot(1)));
    assert_eq!(replica.step(LEADER, commit(1, 1, claim(7))).len(), 0);
    assert!(replica.chosen().is_empty());
    assert_eq!(
        replica
            .step(
                LEADER,
                Message::Accepted {
                    ballot: paros_consensus::Ballot::default(),
                    slot: Slot(0)
                }
            )
            .len(),
        0
    );
}

#[test]
fn a_full_replica_writes_the_boundary_state_until_a_done_value_shows_it() {
    let mut replica = based();
    replica.step(LEADER, commit(1, 0, claim(7)));
    assert!(!replica.full());
    let value = sealed(1);
    let effects = replica.observe(&value);
    assert!(replica.full());
    let state = replica.state().expect("a fold").boundary();
    let boundary = Change::Boundary {
        segment: SegmentId(1),
        state,
    };
    assert_eq!(effects, vec![ReplicaEffect::Register(boundary.clone())]);
    assert_eq!(
        replica.tick(),
        vec![ReplicaEffect::Register(boundary.clone())]
    );
    let done = value.apply(&boundary).expect("the boundary");
    assert_eq!(replica.observe(&done).len(), 0);
    assert!(
        replica.tick().is_empty(),
        "a full replica with its boundary written asks nothing"
    );
}

#[test]
fn a_later_segment_folds_from_the_boundary_of_the_one_before() {
    let value = sealed(1);
    let mut first = based();
    first.step(LEADER, commit(1, 0, claim(7)));
    first.observe(&value);
    let state = first.state().expect("a fold").boundary();
    let mut second = SegmentReplica::recover(
        NodeId(21),
        SegmentId(2),
        nodes(&[21, 22, 23]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
    assert!(
        second.observe(&value).is_empty(),
        "no base before the boundary is written"
    );
    let done = value
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state,
        })
        .expect("the boundary");
    let effects = second.observe(&done);
    assert_eq!(
        persists(&effects),
        vec![(
            PersistId(0),
            vec![ReplicaOp::Base {
                boundary: state,
                start: Slot(1)
            }]
        )]
    );
    assert_eq!(
        second.state().map(JournalState::leader),
        Some(Leader::Uuid(LeaderUuid(7)))
    );
    assert_eq!(second.front(), Some(Slot(1)));
}

#[test]
fn a_replica_behind_pulls_from_its_peers_and_a_peer_answers_a_page() {
    let behind = based();
    let tick = behind.tick();
    let pulls: Vec<NodeId> = tick
        .iter()
        .filter_map(|effect| match effect {
            ReplicaEffect::Send {
                to,
                message: Message::Pull { segment, from },
                after: None,
            } if *segment == SegmentId(1) && *from == Slot(0) => Some(*to),
            _ => None,
        })
        .collect();
    assert_eq!(pulls, nodes(&[12, 13]));
    let mut peer = SegmentReplica::recover(
        NodeId(12),
        SegmentId(1),
        nodes(&[11, 12, 13]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
    peer.observe(&one_segment());
    peer.step(LEADER, commit(1, 0, claim(7)));
    peer.step(LEADER, commit(1, 1, write(7, 0, 1)));
    let answer = peer.step(
        ME,
        Message::Pull {
            segment: SegmentId(1),
            from: Slot(1),
        },
    );
    assert_eq!(
        answer,
        vec![ReplicaEffect::Send {
            to: ME,
            message: commit(1, 1, write(7, 0, 1)),
            after: Some(PersistId(2)),
        }]
    );
    assert_eq!(
        peer.step(
            ME,
            Message::Pull {
                segment: SegmentId(2),
                from: Slot(0)
            }
        )
        .len(),
        0
    );
}

#[test]
fn a_restart_folds_its_disk_again() {
    let mut replica = based();
    let mut disk = ReplicaDisk::default();
    disk.apply(
        PersistId(0),
        &[ReplicaOp::Base {
            boundary: BoundaryState::fresh(Leader::Unset),
            start: Slot(0),
        }],
    );
    for effect in replica.step(LEADER, commit(1, 0, claim(7))) {
        if let ReplicaEffect::Persist { id, ops } = effect {
            disk.apply(id, &ops);
        }
    }
    let restarted =
        SegmentReplica::recover(ME, SegmentId(1), nodes(&[11, 12, 13]), Leader::Unset, disk);
    assert_eq!(restarted.front(), Some(Slot(1)));
    assert_eq!(restarted.state(), replica.state());
}

#[test]
fn a_resolve_reads_the_fold() {
    assert_eq!(
        replica().resolve(LeaderUuid(7), Seq(0), &[Record::new(vec![1])]),
        ResolveAnswer::NotCovered,
        "a replica with no base covers nothing"
    );
    let mut replica = based();
    replica.step(LEADER, commit(1, 0, claim(7)));
    replica.step(LEADER, commit(1, 1, write(7, 0, 1)));
    assert!(matches!(
        replica.resolve(LeaderUuid(7), Seq(0), &[Record::new(vec![1])]),
        ResolveAnswer::Written { .. }
    ));
    assert_eq!(
        replica.resolve(LeaderUuid(7), Seq(0), &[Record::new(vec![2])]),
        ResolveAnswer::NotWritten
    );
}

#[test]
#[should_panic(expected = "the base is written once")]
fn a_disk_takes_its_base_once() {
    let mut disk = ReplicaDisk::default();
    let base = ReplicaOp::Base {
        boundary: BoundaryState::fresh(Leader::Unset),
        start: Slot(0),
    };
    disk.apply(PersistId(0), std::slice::from_ref(&base));
    disk.apply(PersistId(1), &[base]);
}

#[test]
#[should_panic(expected = "keeps its command")]
fn a_disk_keeps_the_command_of_a_slot() {
    let mut disk = ReplicaDisk::default();
    disk.apply(
        PersistId(0),
        &[ReplicaOp::Chosen {
            slot: Slot(0),
            command: claim(1),
        }],
    );
    disk.apply(
        PersistId(1),
        &[ReplicaOp::Chosen {
            slot: Slot(0),
            command: claim(2),
        }],
    );
}

#[test]
#[should_panic(expected = "id order")]
fn a_disk_takes_batches_in_id_order() {
    let mut disk = ReplicaDisk::default();
    disk.apply(PersistId(1), &[]);
    disk.apply(PersistId(0), &[]);
}

#[test]
#[should_panic(expected = "in its own set")]
fn a_replica_is_in_its_set() {
    let _ = SegmentReplica::recover(
        NodeId(99),
        SegmentId(1),
        nodes(&[11]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
}

#[test]
#[should_panic(expected = "unset or multi-writer")]
fn a_fresh_journal_has_no_uuid() {
    let _ = SegmentReplica::recover(
        ME,
        SegmentId(1),
        nodes(&[11]),
        Leader::Uuid(LeaderUuid(1)),
        ReplicaDisk::default(),
    );
}
