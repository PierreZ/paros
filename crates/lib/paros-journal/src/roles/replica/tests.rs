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
        new: Leader::uuid(LeaderUuid(uuid)),
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
    assert_eq!(replica.start(), None);
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
    // The boundary write waits on the batch of the last slot it folds.
    let [ReplicaEffect::Register { change, after }] = effects.as_slice() else {
        panic!("one register change: {effects:?}");
    };
    assert_eq!(*change, boundary);
    assert!(after.is_some(), "the boundary write waits on a batch");
    let register = ReplicaEffect::Register {
        change: boundary.clone(),
        after: *after,
    };
    // The tick also compacts the log: slot 0 holds no record.
    let compact = ReplicaEffect::Persist {
        id: PersistId(2),
        ops: vec![ReplicaOp::Compact(Trim {
            slot: Slot(1),
            state: *replica.state().expect("a fold"),
            floor: Seq(0),
        })],
    };
    assert_eq!(replica.tick(), vec![register, compact]);
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
    assert_eq!(second.start(), Some(Slot(1)));
}

#[test]
fn a_replica_behind_pulls_from_its_peers_and_a_peer_answers_a_page() {
    let mut behind = based();
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
        replica()
            .resolve(LeaderUuid(7), Seq(0), &[Record::new(vec![1])])
            .0,
        ResolveAnswer::NotCovered,
        "a replica with no base covers nothing"
    );
    let mut replica = based();
    replica.step(LEADER, commit(1, 0, claim(7)));
    let effects = replica.step(LEADER, commit(1, 1, write(7, 0, 1)));
    let last = effects.iter().find_map(|effect| match effect {
        ReplicaEffect::Persist { id, .. } => Some(*id),
        _ => None,
    });
    let (written, after) = replica.resolve(LeaderUuid(7), Seq(0), &[Record::new(vec![1])]);
    assert!(matches!(written, ResolveAnswer::Written { .. }));
    assert_eq!(
        after, last,
        "the answer waits on the batch of the last slot it read"
    );
    assert_eq!(
        replica
            .resolve(LeaderUuid(7), Seq(0), &[Record::new(vec![2])])
            .0,
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
fn a_replica_outside_its_set_drops_its_copy_and_stops() {
    let mut replica = SegmentReplica::recover(
        NodeId(99),
        SegmentId(1),
        nodes(&[11, 12, 13]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
    assert!(!replica.dropped());
    assert_eq!(replica.observe(&one_segment()), vec![ReplicaEffect::Drop]);
    assert!(replica.dropped());
    assert_eq!(replica.front(), None, "a dropped replica takes no base");
    assert_eq!(replica.observe(&one_segment()), vec![]);
    assert_eq!(replica.step(LEADER, commit(1, 0, Command::Noop)), vec![]);
    assert_eq!(replica.step(LEADER, holding()), vec![]);
    assert_eq!(replica.tick(), vec![], "a dropped replica takes no step");
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

#[test]
fn a_later_segment_takes_no_base_without_the_record_before() {
    // Only the first segment of the journal folds from the fresh journal: a
    // segment whose record before is missing waits.
    let state = BoundaryState::fresh(Leader::Unset);
    let chain = reshape(
        &sealed(0)
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
            .expect("the boundary and the retire"),
    )
    .apply(&Change::Seal {
        segment: SegmentId(2),
        end: Slot(0),
    })
    .expect("the second seal");
    let gap: BTreeMap<SegmentId, SegmentRecord> = chain
        .segments()
        .iter()
        .filter(|(id, _)| **id != SegmentId(2))
        .map(|(id, record)| (*id, record.clone()))
        .collect();
    let value = RegisterValue::from_parts(chain.term(), gap);
    let mut replica = SegmentReplica::recover(
        NodeId(31),
        SegmentId(3),
        nodes(&[31, 32, 33]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
    assert_eq!(replica.observe(&value), Vec::new());
    assert_eq!(replica.front(), None);
    // With the record before, it takes its base.
    let mut whole = replica.clone();
    let _ = whole.observe(&chain);
    assert_eq!(whole.front(), None, "segment 2 has no boundary state yet");
}

#[test]
#[should_panic(expected = "never passes the end")]
fn the_front_never_passes_the_end() {
    let mut replica = based();
    replica.step(LEADER, commit(1, 0, claim(7)));
    replica.end = Some(Slot(0));
    replica.assert_invariants();
}

/// Segment 1 sealed at slot 2 with its boundary state, and retired; the
/// replicas 11, 12, 13.
fn retired_two() -> RegisterValue {
    sealed(2)
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state: BoundaryState::fresh(Leader::Unset),
        })
        .and_then(|value| {
            value.apply(&Change::Retire {
                segment: SegmentId(1),
                holders: 2,
                unrepaired_damage: false,
            })
        })
        .expect("segment 1 retires")
}

/// `value` with `remove` replaced by `add` in segment 1's set.
fn healed(value: &RegisterValue, remove: u64, add: u64) -> RegisterValue {
    value
        .apply(&Change::Rereplicate {
            segment: SegmentId(1),
            read: value.segment(SegmentId(1)).expect("segment 1").clone(),
            term: value.term().number,
            remove: NodeId(remove),
            add: NodeId(add),
        })
        .expect("a heal")
}

const NEW: NodeId = NodeId(20);
const COPY: CopyId = CopyId { term: 1, number: 3 };

fn joiner() -> SegmentReplica {
    let (replica, effects) = SegmentReplica::join(
        NEW,
        SegmentId(1),
        nodes(&[11, 12, 13]),
        Leader::Unset,
        COPY,
        None,
    );
    assert_eq!(
        persists(&effects),
        vec![(PersistId(0), vec![ReplicaOp::Joining(Some(COPY))])],
        "the copy is on the disk before anything else"
    );
    replica
}

fn holding() -> Message {
    Message::Holding {
        segment: SegmentId(1),
    }
}

fn holds(full: bool) -> Message {
    Message::Holds {
        segment: SegmentId(1),
        full,
    }
}

/// The receivers of the `Holding` and the `Pull`s of `effects`.
fn asks(effects: &[ReplicaEffect]) -> (Vec<NodeId>, Vec<(NodeId, Slot)>) {
    let mut holding = Vec::new();
    let mut pulls = Vec::new();
    for effect in effects {
        match effect {
            ReplicaEffect::Send {
                to,
                message: Message::Holding { segment },
                after: None,
            } if *segment == SegmentId(1) => holding.push(*to),
            ReplicaEffect::Send {
                to,
                message: Message::Pull { segment, from },
                after: None,
            } if *segment == SegmentId(1) => pulls.push((*to, *from)),
            _ => {}
        }
    }
    (holding, pulls)
}

fn copied(effects: &[ReplicaEffect]) -> Vec<(CopyId, Option<PersistId>)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            ReplicaEffect::Copied {
                segment,
                copy,
                after,
            } => {
                assert_eq!(*segment, SegmentId(1));
                Some((*copy, *after))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn a_replica_says_whether_it_holds_the_whole_sealed_segment() {
    let mut replica = based();
    let answer = |replica: &mut SegmentReplica| replica.step(LEADER, holding());
    let effects = answer(&mut replica);
    assert_eq!(
        effects,
        vec![ReplicaEffect::Send {
            to: LEADER,
            message: holds(false),
            after: Some(PersistId(0)),
        }],
        "an open segment is not held whole; the answer waits on the last batch"
    );
    replica.observe(&sealed(2));
    replica.step(LEADER, commit(1, 0, Command::Noop));
    assert!(matches!(
        &answer(&mut replica)[..],
        [ReplicaEffect::Send { message, .. }] if *message == holds(false)
    ));
    replica.step(LEADER, commit(1, 1, Command::Noop));
    assert_eq!(
        answer(&mut replica),
        vec![ReplicaEffect::Send {
            to: LEADER,
            message: holds(true),
            after: Some(PersistId(2)),
        }],
        "a full answer waits on the batch of the last slot"
    );
    assert!(
        replica
            .step(
                LEADER,
                Message::Holding {
                    segment: SegmentId(2)
                }
            )
            .is_empty(),
        "a question about another segment is not for this replica"
    );
    assert!(
        replica.step(NodeId(12), holds(true)).is_empty(),
        "a member that is no new replica looks for no source"
    );
}

#[test]
#[should_panic(expected = "not a member yet")]
fn a_new_replica_is_not_a_member() {
    let _ = SegmentReplica::join(
        ME,
        SegmentId(1),
        nodes(&[11, 12, 13]),
        Leader::Unset,
        COPY,
        None,
    );
}

#[test]
fn a_new_replica_copies_from_a_full_member_and_reports_its_copy() {
    let mut replica = joiner();
    assert_eq!(replica.joining(), Some(COPY));
    assert_eq!(replica.set(), nodes(&[11, 12, 13]));
    assert_eq!(
        asks(&replica.tick()),
        (nodes(&[11, 12, 13]), vec![]),
        "it looks for a source, and pulls nothing before its base"
    );
    let value = retired_two();
    let effects = replica.observe(&value);
    assert!(!replica.dropped(), "a plan's new replica keeps its copy");
    assert_eq!(effects.len(), 1, "the base, and no drop");
    assert_eq!(replica.front(), Some(Slot(0)));
    assert_eq!(
        replica.end(),
        Some(Slot(2)),
        "the end comes from the record"
    );
    assert_eq!(replica.step(NodeId(12), holds(false)), vec![]);
    assert!(
        replica.step(NodeId(99), holds(true)).is_empty(),
        "a node outside the set is no source (control live_outside)"
    );
    assert_eq!(replica.step(NEW, holds(true)), vec![]);
    assert!(
        replica
            .step(
                NodeId(11),
                Message::Holds {
                    segment: SegmentId(2),
                    full: true,
                }
            )
            .is_empty(),
        "an answer about another segment names no source"
    );
    assert_eq!(
        asks(&replica.step(NodeId(11), holds(true))),
        (vec![], vec![(NodeId(11), Slot(0))]),
        "a full member is a source: the copy pulls from it"
    );
    assert_eq!(
        asks(&replica.tick()),
        (nodes(&[11, 12, 13]), vec![(NodeId(11), Slot(0))]),
        "the copy pulls only from the sources (control copy_any)"
    );
    let records = [Record::new(vec![7])];
    let effects = replica.step(NodeId(11), commit(1, 0, claim(1)));
    assert!(copied(&effects).is_empty(), "the copy is not complete");
    assert_eq!(
        replica.resolve(LeaderUuid(1), Seq(0), &records).0,
        ResolveAnswer::NotCovered,
        "a partial copy answers no position past its front (control answer_partial)"
    );
    let effects = replica.step(NodeId(11), commit(1, 1, write(1, 0, 7)));
    assert_eq!(
        copied(&effects),
        vec![(COPY, Some(PersistId(3)))],
        "the report waits on the batch of the last slot"
    );
    assert!(replica.full());
    assert_eq!(copied(&replica.tick()), vec![(COPY, Some(PersistId(3)))]);
    assert_eq!(
        asks(&replica.step(NodeId(12), holds(true))),
        (vec![], vec![]),
        "a complete copy pulls from no new source"
    );
    assert_eq!(
        asks(&replica.tick()),
        (vec![], vec![]),
        "a complete copy asks nothing"
    );
    assert!(
        matches!(
            replica.resolve(LeaderUuid(1), Seq(0), &records).0,
            ResolveAnswer::Written { .. }
        ),
        "a complete copy answers"
    );
    let effects = replica.observe(&healed(&value, 13, 20));
    assert_eq!(copied(&effects), vec![]);
    assert_eq!(
        persists(&effects),
        // Batch 4 is the compaction of the full copy, at a tick.
        vec![(PersistId(5), vec![ReplicaOp::Joining(None)])],
        "a member is no new replica, on its disk too"
    );
    assert_eq!(replica.joining(), None, "the heal is done: a member");
    assert_eq!(replica.set(), nodes(&[11, 12, 20]));
    assert_eq!(copied(&replica.tick()), vec![]);
}

#[test]
fn a_source_that_stops_holding_is_no_source() {
    let mut replica = joiner();
    replica.observe(&retired_two());
    replica.step(NodeId(11), holds(true));
    assert_eq!(asks(&replica.step(NodeId(11), holds(false))).1, vec![]);
    assert_eq!(
        asks(&replica.tick()).1,
        vec![],
        "no source: the copy pulls nothing"
    );
}

#[test]
fn a_new_replica_whose_plan_ended_drops_its_copy() {
    let mut replica = joiner();
    replica.observe(&retired_two());
    assert_eq!(
        replica.abandon(
            CopyId { term: 1, number: 4 },
            &healed(&retired_two(), 13, 21)
        ),
        vec![],
        "another copy's end changes nothing while the replica runs its copy"
    );
    assert_eq!(replica.joining(), Some(COPY));
    let effects = replica.abandon(COPY, &retired_two());
    assert_eq!(
        persists(&effects),
        vec![(PersistId(2), vec![ReplicaOp::Joining(None)])]
    );
    assert!(effects.contains(&ReplicaEffect::Drop));
    assert_eq!(replica.joining(), None);
    assert!(
        replica.dropped(),
        "a value that leaves it out drops the copy"
    );
    assert_eq!(
        replica.abandon(COPY, &retired_two()),
        vec![],
        "abandoned once"
    );
}

/// `value` after `times` controller elections.
fn elected(value: &RegisterValue, times: u64) -> RegisterValue {
    (0..times).fold(value.clone(), |value, holder| {
        value
            .apply(&Change::Elect {
                holder: NodeId(30 + holder),
            })
            .expect("an election")
    })
}

#[test]
fn a_new_replica_ends_a_copy_of_a_term_that_is_over() {
    let mut replica = joiner();
    let current = retired_two();
    assert_eq!(current.term().number, COPY.term);
    replica.observe(&current);
    assert_eq!(
        replica.joining(),
        Some(COPY),
        "the copy of the term in force runs on"
    );
    let effects = replica.observe(&elected(&retired_two(), 1));
    assert_eq!(
        persists(&effects),
        vec![(PersistId(2), vec![ReplicaOp::Joining(None)])],
        "the end of the copy is on the disk"
    );
    assert!(effects.contains(&ReplicaEffect::Drop));
    assert_eq!(replica.joining(), None);
    assert!(
        replica.dropped(),
        "an orphan copy is dropped with no abandon"
    );
}

#[test]
fn a_new_replica_that_a_later_term_names_is_a_member() {
    let mut replica = joiner();
    let value = elected(&healed(&retired_two(), 11, 20), 1);
    assert!(value.term().number > COPY.term);
    let effects = replica.observe(&value);
    assert_eq!(
        persists(&effects).first(),
        Some(&(PersistId(1), vec![ReplicaOp::Joining(None)])),
        "the value that names it ends the copy: it is a member"
    );
    assert!(!replica.dropped(), "a member keeps its copy in every term");
    assert!(replica.set().contains(&NEW));
}

#[test]
fn an_abandon_after_the_commit_keeps_a_member() {
    let mut replica = joiner();
    replica.observe(&retired_two());
    // The heal committed, its "ok" was lost, and a late copy of the change
    // was refused: the controller abandons a copy that is a member now.
    let effects = replica.abandon(COPY, &healed(&retired_two(), 11, 20));
    assert_eq!(
        persists(&effects),
        vec![(PersistId(2), vec![ReplicaOp::Joining(None)])],
        "the value that names it ends the copy: it is a member"
    );
    assert!(!replica.dropped(), "a member keeps its copy");
    assert_eq!(replica.joining(), None);
    assert!(replica.set().contains(&NEW));
}

#[test]
fn a_member_that_a_heal_removed_drops_its_copy() {
    let mut replica = based();
    replica.observe(&retired_two());
    assert_eq!(
        replica.observe(&healed(&retired_two(), 11, 20)),
        vec![ReplicaEffect::Drop]
    );
}

#[test]
fn every_replica_drops_a_trimmed_segment() {
    let trimmed = retired_two()
        .apply(&Change::Trim {
            segment: SegmentId(1),
            first_seq: Seq(0),
        })
        .expect("trimmed");
    let mut member = based();
    assert_eq!(member.observe(&trimmed), vec![ReplicaEffect::Drop]);
    let mut new = joiner();
    assert_eq!(new.observe(&trimmed), vec![ReplicaEffect::Drop]);
    assert_eq!(new.joining(), None);
}

#[test]
fn a_new_replica_that_restarts_keeps_its_copy_while_its_plan_runs() {
    let keep = |effects: &[ReplicaEffect], disk: &mut ReplicaDisk| {
        for (id, ops) in persists(effects) {
            disk.apply(id, &ops);
        }
    };
    let (mut replica, effects) = SegmentReplica::join(
        NEW,
        SegmentId(1),
        nodes(&[11, 12, 13]),
        Leader::Unset,
        COPY,
        None,
    );
    let mut disk = ReplicaDisk::default();
    keep(&effects, &mut disk);
    keep(&replica.observe(&retired_two()), &mut disk);
    keep(
        &replica.step(NodeId(11), commit(1, 0, Command::Noop)),
        &mut disk,
    );
    assert_eq!(disk.joining, Some(COPY));
    let set = || nodes(&[11, 12, 13]);
    let mut again = SegmentReplica::recover(NEW, SegmentId(1), set(), Leader::Unset, disk.clone());
    assert_eq!(again.joining(), Some(COPY));
    assert_eq!(
        again.front(),
        Some(Slot(1)),
        "the copy goes on from its disk"
    );
    assert_eq!(
        again.observe(&retired_two()),
        vec![],
        "a new replica of a running plan keeps its copy"
    );
    keep(&again.abandon(COPY, &retired_two()), &mut disk);
    assert_eq!(disk.joining, None);
    let mut removed = SegmentReplica::recover(NEW, SegmentId(1), set(), Leader::Unset, disk);
    assert_eq!(
        removed.observe(&retired_two()),
        vec![ReplicaEffect::Drop],
        "once its plan ended, a restart does not bring the copy back"
    );
}

/// Segment 1 holds a claim and one record, sealed at 2 with its boundary
/// written and retired, and a replica of segment 2 with its base.
fn over_one_record() -> (RegisterValue, SegmentReplica) {
    let value = sealed(2);
    let mut first = based();
    first.step(LEADER, commit(1, 0, claim(7)));
    first.step(LEADER, commit(1, 1, write(7, 0, 1)));
    first.observe(&value);
    let state = first.state().expect("a fold").boundary();
    let done = value
        .apply(&Change::Boundary {
            segment: SegmentId(1),
            state,
        })
        .expect("the boundary");
    let mut second = SegmentReplica::recover(
        NodeId(21),
        SegmentId(2),
        nodes(&[21, 22, 23]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
    second.observe(&done);
    assert_eq!(second.start(), Some(Slot(2)));
    let effects = second.step(LEADER, commit(2, 2, truncate(1)));
    assert_eq!(
        trims(&effects),
        vec![],
        "a sealed segment that is not retired is not trimmed"
    );
    let retired = done
        .apply(&Change::Retire {
            segment: SegmentId(1),
            holders: 2,
            unrepaired_damage: false,
        })
        .expect("segment 1 retires");
    (retired, second)
}

fn trims(effects: &[ReplicaEffect]) -> Vec<(SegmentId, Seq, Option<PersistId>)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            ReplicaEffect::Register {
                change: Change::Trim { segment, first_seq },
                after,
            } => Some((*segment, *first_seq, *after)),
            _ => None,
        })
        .collect()
}

fn truncate(up_to: u64) -> Command {
    Command::Truncate {
        fence: Some(LeaderUuid(7)),
        up_to: Seq(up_to),
    }
}

#[test]
fn a_floor_over_a_retired_segment_trims_it_after_its_slot_is_durable() {
    let (value, mut second) = over_one_record();
    let effects = second.observe(&value);
    assert_eq!(
        trims(&effects),
        vec![(SegmentId(1), Seq(1), Some(PersistId(1)))],
        "the trim waits on the batch of the Truncate"
    );
    let trimmed = value
        .apply(&Change::Trim {
            segment: SegmentId(1),
            first_seq: Seq(1),
        })
        .expect("the trim");
    assert_eq!(
        trims(&second.observe(&trimmed)),
        vec![],
        "a trimmed record is not trimmed again"
    );
}

#[test]
fn a_floor_inside_a_retired_segment_does_not_trim_it() {
    let (value, _) = over_one_record();
    let mut second = SegmentReplica::recover(
        NodeId(22),
        SegmentId(2),
        nodes(&[21, 22, 23]),
        Leader::Unset,
        ReplicaDisk::default(),
    );
    second.observe(&value);
    let effects = second.step(LEADER, commit(2, 2, truncate(0)));
    assert_eq!(trims(&effects), vec![], "the floor is below the last seq");
    assert_eq!(trims(&second.observe(&value)), vec![]);
}

#[test]
fn a_laggard_jumps_into_a_blind_fold_that_answers_nothing_until_the_truncate() {
    let mut replica = based();
    replica.step(LEADER, commit(1, 0, claim(7)));
    // A peer compacted to slot 2: the fold there has the claim and one
    // write, and the floor 1 that the `Truncate` at slot 3 raises.
    let state = JournalState::resume(
        BoundaryState {
            next_seq: Seq(1),
            first_seq: Seq(0),
            leader: Leader::Uuid(LeaderUuid(7)),
        },
        Seq(0),
    )
    .expect("a fold");
    let effects = replica.step(
        NodeId(12),
        Message::TrimmedTo {
            segment: SegmentId(1),
            slot: Slot(2),
            state,
            floor: Seq(1),
        },
    );
    let trims: Vec<ReplicaOp> = persists(&effects)
        .into_iter()
        .flat_map(|(_, ops)| ops)
        .collect();
    assert!(
        matches!(trims.as_slice(), [ReplicaOp::Compact(trim)] if trim.slot == Slot(2) && trim.floor == Seq(1)),
        "the jump writes the peer's trim: {trims:?}"
    );
    assert_eq!(replica.front(), Some(Slot(2)));
    // The write at slot 2 applies blind: no answer leaves.
    let effects = replica.step(LEADER, commit(1, 2, write(7, 1, 2)));
    assert!(applied(&effects).is_empty(), "a blind fold answers nothing");
    assert_eq!(replica.front(), Some(Slot(3)));
    let report = |replica: &mut SegmentReplica| {
        replica
            .step(
                LEADER,
                Message::Compaction {
                    segment: SegmentId(1),
                },
            )
            .into_iter()
            .find_map(|effect| match effect {
                ReplicaEffect::Send {
                    message:
                        Message::CompactedTo {
                            compacted, answers, ..
                        },
                    ..
                } => Some((compacted, answers)),
                _ => None,
            })
            .expect("a report")
    };
    assert_eq!(
        report(&mut replica),
        (Slot(2), Slot(3)),
        "no answer below the slot after the blind apply"
    );
    // The `Truncate` applies blind too, and makes the fold whole again:
    // the next slot answers.
    let truncate = Command::Truncate {
        fence: Some(LeaderUuid(7)),
        up_to: Seq(1),
    };
    let effects = replica.step(LEADER, commit(1, 3, truncate));
    assert!(applied(&effects).is_empty(), "the Truncate applies blind");
    assert_eq!(report(&mut replica), (Slot(2), Slot(4)));
    let effects = replica.step(LEADER, commit(1, 4, write(7, 2, 3)));
    assert_eq!(
        applied(&effects).len(),
        1,
        "the fold reached the floor: it answers"
    );
    assert_eq!(report(&mut replica).1, Slot(4), "the answers start at 4");
}

#[test]
fn a_trim_from_outside_the_set_or_of_another_segment_is_not_taken() {
    let mut replica = based();
    let state = JournalState::resume(BoundaryState::fresh(Leader::Unset), Seq(0)).expect("a fold");
    let trimmed = |segment: u64| Message::TrimmedTo {
        segment: SegmentId(segment),
        slot: Slot(2),
        state,
        floor: Seq(0),
    };
    assert!(
        persists(&replica.step(NodeId(99), trimmed(1))).is_empty(),
        "not a peer"
    );
    assert!(
        persists(&replica.step(NodeId(12), trimmed(2))).is_empty(),
        "another segment"
    );
    assert_eq!(replica.front(), Some(Slot(0)));
    assert_eq!(
        persists(&replica.step(NodeId(12), trimmed(1))).len(),
        1,
        "a peer's trim"
    );
    assert_eq!(replica.front(), Some(Slot(2)));
}

#[test]
fn a_compaction_keeps_the_position_of_a_write_at_the_floor() {
    let mut replica = based();
    replica.step(LEADER, commit(1, 0, claim(7)));
    replica.step(LEADER, commit(1, 1, write(7, 0, 1)));
    replica.step(LEADER, commit(1, 2, write(7, 1, 2)));
    let truncate = Command::Truncate {
        fence: Some(LeaderUuid(7)),
        up_to: Seq(1),
    };
    replica.step(LEADER, commit(1, 3, truncate));
    let trims: Vec<ReplicaOp> = persists(&replica.tick())
        .into_iter()
        .flat_map(|(_, ops)| ops)
        .filter(|op| matches!(op, ReplicaOp::Compact(_)))
        .collect();
    assert!(
        matches!(trims.as_slice(), [ReplicaOp::Compact(trim)] if trim.slot == Slot(2) && trim.floor == Seq(1)),
        "the point is the write at the floor: {trims:?}"
    );
    // The resend of the write at the floor still finds its position.
    let effects = replica.step(LEADER, commit(1, 4, write(7, 1, 2)));
    let answers: Vec<Answer> = effects
        .into_iter()
        .filter_map(|effect| match effect {
            ReplicaEffect::Send {
                message: Message::Applied { answer, .. },
                ..
            } => Some(answer),
            _ => None,
        })
        .collect();
    assert!(
        matches!(answers.as_slice(), [Answer::Duplicate { .. }]),
        "a resend at the floor is a Duplicate: {answers:?}"
    );
}

#[test]
fn a_compaction_ask_of_another_segment_is_not_answered() {
    let mut replica = based();
    let asked = |replica: &mut SegmentReplica, segment: u64| {
        replica
            .step(
                LEADER,
                Message::Compaction {
                    segment: SegmentId(segment),
                },
            )
            .into_iter()
            .filter(|effect| {
                matches!(
                    effect,
                    ReplicaEffect::Send {
                        message: Message::CompactedTo { .. },
                        ..
                    }
                )
            })
            .count()
    };
    assert_eq!(asked(&mut replica, 2), 0, "another segment");
    assert_eq!(asked(&mut replica, 1), 1, "its own segment");
}
