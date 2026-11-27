//! A replica of one segment (R6 of `docs/architecture/04-7-segments.md`,
//! the model `specs/tla/compartments/`): its walk stops after the `Roll`
//! that ends the segment, it joins a later segment from the boundary state
//! of the chain, and it answers a read past its end "sealed".

use super::config;
use crate::contract::config::Config;
use crate::contract::limits::Limits;
use crate::contract::mem_storage::MemStorage;
use crate::contract::message::{Audience, Message, Party};
use crate::contract::storage::Storage;
use crate::contract::write::WriteOp;
use crate::journal::segment::{Roll, Segment, SegmentId};
use crate::journal::state::{JournalState, WriterMode};
use crate::journal::{Command, Control, Entry, LeaderUuid, Seq};
use crate::quorum_read::{ReadOpen, ReadState};
use crate::replica::LogRead;
use crate::replica_node::{JoinRefusal, ReplicaNode};
use crate::types::{Ballot, NodeId, Slot, Value};

/// A multi-writer replica: a write needs no leader, so a test commits
/// writes alone.
fn multi() -> Config {
    Config {
        writer_mode: WriterMode::Multi,
        ..config(10)
    }
}

fn write(byte: u8) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(0),
        seq: Seq(0),
        records: vec![Value::from(vec![byte])],
    })
}

fn roll(next: u64) -> Command {
    Command::Roll(Roll {
        next: SegmentId(next),
        replicas: None,
    })
}

fn commit(slot: u64, command: Command) -> Message {
    Message::Commit {
        from: Party::Node(NodeId(0)),
        ballot: Ballot {
            round: 1,
            node: NodeId(0),
        },
        slot: Slot(slot),
        command,
    }
}

/// Persist the batch on `disk` and acknowledge it.
fn drain(replica: &mut ReplicaNode, disk: &mut MemStorage) -> Vec<Slot> {
    let ready = replica.ready();
    disk.apply(ready.writes());
    let walked = ready.committed().iter().map(|(slot, ..)| *slot).collect();
    ready.advance();
    walked
}

/// The journal state at the start of segment 1, opened by a `Roll` at
/// slot 1 after one record.
fn boundary() -> JournalState {
    JournalState {
        next_seq: Seq(1),
        segment: Segment {
            id: SegmentId(1),
            start: Slot(2),
        },
        ..JournalState::default()
    }
}

/// The walk stops after the `Roll` that ends the first segment: the slot
/// past it is not this replica's, learned before or after the seal, and a
/// read at the end is answered "sealed" with the segment that follows.
#[test]
fn a_replica_stops_after_the_roll_that_ends_its_segment() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert_eq!(replica.replica().segment_bound(), Some(SegmentId(0)));
    replica.step(commit(0, write(7)));
    // Learned out of order, before the `Roll` that ends the segment.
    replica.step(commit(2, write(8)));
    assert!(replica.retained_bytes() > 1, "both writes are held");
    replica.step(commit(1, roll(1)));
    assert_eq!(drain(&mut replica, &mut disk), vec![Slot(0), Slot(1)]);
    assert!(replica.replica().is_sealed());
    assert_eq!(replica.replica().sealed_end(), Some(Slot(2)));
    assert_eq!(replica.replica().chosen_index(), Some(Slot(1)));
    assert_eq!(
        replica.chosen_at(Slot(2)),
        None,
        "slot 2 is not this replica's"
    );
    assert_eq!(
        replica.retained_bytes(),
        1,
        "only the write of the segment is held"
    );
    // Learned after the seal: refused.
    replica.step(commit(3, write(9)));
    assert_eq!(drain(&mut replica, &mut disk), Vec::<Slot>::new());
    assert_eq!(replica.counters().past_end, 1);
    let LogRead::Page(page) = replica.read_log(Seq(0), 10, usize::MAX) else {
        panic!("a read inside the segment is a page");
    };
    assert_eq!(page.records, vec![Value::from(vec![7])]);
    assert_eq!(
        replica.read_log(Seq(1), 10, usize::MAX),
        LogRead::Sealed {
            end: Seq(1),
            next: Segment {
                id: SegmentId(1),
                start: Slot(2)
            }
        }
    );
}

/// A reboot keeps the seal: the boot folds up to the `Roll` and takes no
/// record past it, even one the disk still holds.
#[test]
fn a_rebooted_replica_stays_sealed() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    replica.step(commit(2, write(8)));
    let _ = drain(&mut replica, &mut disk);
    replica.step(commit(1, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    assert!(
        disk.accepted(Slot(2)).is_some(),
        "the disk keeps the record past the end"
    );
    let mut rebooted = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert!(rebooted.replica().is_sealed());
    assert_eq!(rebooted.chosen_at(Slot(2)), None);
    assert_eq!(rebooted.retained_bytes(), 1);
    assert!(
        drain(&mut rebooted, &mut disk).is_empty(),
        "the boot walks nothing new"
    );
    assert!(matches!(
        rebooted.read_log(Seq(1), 10, usize::MAX),
        LogRead::Sealed { end: Seq(1), .. }
    ));
}

/// A fresh replica joins segment 1 from the boundary state: it holds no
/// record of segment 0, serves the records of its own, keeps segment 1
/// across a reboot, and stops after the `Roll` that ends it.
#[test]
fn a_replica_joins_a_later_segment_from_the_boundary_state() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert_eq!(replica.join_segment(Slot(2), boundary()), Ok(()));
    let ready = replica.ready();
    assert!(
        ready
            .writes()
            .iter()
            .any(|w| matches!(w, WriteOp::TrimmedTo { point: Slot(2), .. })),
        "the join is durable as a trim point"
    );
    disk.apply(ready.writes());
    ready.advance();
    assert_eq!(replica.replica().segment_bound(), Some(SegmentId(1)));
    assert_eq!(replica.first_slot(), Slot(2));
    replica.step(commit(2, write(8)));
    assert_eq!(drain(&mut replica, &mut disk), vec![Slot(2)]);
    assert_eq!(
        replica.read_log(Seq(0), 10, usize::MAX),
        LogRead::NotHeld,
        "the record of segment 0 is in an earlier segment"
    );
    let LogRead::Page(page) = replica.read_log(Seq(1), 10, usize::MAX) else {
        panic!("a read of the joined segment is a page");
    };
    assert_eq!(page.records, vec![Value::from(vec![8])]);
    let mut rebooted = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert_eq!(rebooted.replica().segment_bound(), Some(SegmentId(1)));
    rebooted.step(commit(3, roll(2)));
    rebooted.step(commit(4, write(9)));
    assert_eq!(drain(&mut rebooted, &mut disk), vec![Slot(3)]);
    assert!(rebooted.replica().is_sealed());
    assert!(matches!(
        rebooted.read_log(Seq(2), 10, usize::MAX),
        LogRead::Sealed { end: Seq(2), next } if next.id == SegmentId(2)
    ));
}

/// Only a fresh replica joins, and only a boundary that opens a later
/// segment at the start.
#[test]
fn a_join_is_refused_off_a_fresh_replica_or_a_true_boundary() {
    let disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert_eq!(
        replica.join_segment(Slot(0), boundary()),
        Err(JoinRefusal::Boundary)
    );
    assert_eq!(
        replica.join_segment(Slot(3), boundary()),
        Err(JoinRefusal::Boundary)
    );
    let first = JournalState {
        segment: Segment {
            id: SegmentId(0),
            start: Slot(2),
        },
        ..boundary()
    };
    assert_eq!(
        replica.join_segment(Slot(2), first),
        Err(JoinRefusal::Boundary)
    );
    let ahead = JournalState {
        first_seq: Seq(2),
        ..boundary()
    };
    assert_eq!(
        replica.join_segment(Slot(2), ahead),
        Err(JoinRefusal::Boundary),
        "a state whose first position passed its next one is no boundary"
    );
    replica.step(commit(0, write(7)));
    assert_eq!(
        replica.join_segment(Slot(2), boundary()),
        Err(JoinRefusal::NotFresh)
    );
    let mut learned = ReplicaNode::new(&disk, Limits::DEFAULT);
    learned.step(commit(1, write(7)));
    assert_eq!(
        learned.join_segment(Slot(2), boundary()),
        Err(JoinRefusal::NotFresh),
        "a record above a hole is a learned slot too"
    );
    assert_eq!(replica.replica().segment_bound(), Some(SegmentId(0)));
}

/// A trim point of another segment would carry the replica out of the one
/// it keeps: refused, and so is any trim point once sealed.
#[test]
fn a_trim_point_of_another_segment_is_not_taken() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(1, write(7)));
    let _ = drain(&mut replica, &mut disk);
    replica.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(3),
        state: boundary(),
        first_seq: Seq(0),
    });
    assert_eq!(replica.first_slot(), Slot(0), "the trim point is not taken");
    assert_eq!(replica.counters().past_end, 1);
    replica.step(commit(0, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    assert!(replica.replica().is_sealed());
    replica.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(3),
        state: JournalState::default(),
        first_seq: Seq(0),
    });
    assert_eq!(replica.first_slot(), Slot(0));
    assert_eq!(replica.counters().past_end, 2);
}

/// A sealed replica pulls nothing from a beat that names a later chosen
/// index; an open one does.
#[test]
fn a_sealed_replica_pulls_nothing() {
    let beat = Message::Heartbeat {
        from: NodeId(0),
        ballot: Ballot {
            round: 1,
            node: NodeId(0),
        },
        commit: Some(Slot(9)),
        config: None,
        fence: None,
        gc_floor: None,
    };
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(beat.clone());
    assert_eq!(
        replica.counters().catch_up_requests,
        1,
        "an open replica pulls"
    );
    let _ = drain(&mut replica, &mut disk);
    replica.step(commit(0, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    replica.step(beat);
    assert_eq!(
        replica.counters().catch_up_requests,
        1,
        "a sealed replica pulls nothing"
    );
}

/// A sealed replica answers a confirmed read past its end "sealed", and one
/// inside its segment with a state.
#[test]
fn a_sealed_replica_answers_a_read_past_its_end_sealed() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    replica.step(commit(1, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    let ack = |from: u64, ctx: u64, watermark: u64| Message::PreReadAck {
        from: NodeId(from),
        ctx,
        watermark: Some(Slot(watermark)),
        config_since: None,
    };
    for (ctx, watermark) in [(1, 5), (2, 1)] {
        assert_eq!(replica.quorum_read_in(ctx, None), ReadOpen::Opened);
        replica.step(ack(0, ctx, watermark));
        replica.step(ack(1, ctx, watermark));
    }
    let ready = replica.ready();
    assert_eq!(
        ready.sealed_reads(),
        &[ReadState {
            ctx: 1,
            index: Some(Slot(5))
        }]
    );
    assert_eq!(
        ready.read_states(),
        &[ReadState {
            ctx: 2,
            index: Some(Slot(1))
        }]
    );
    assert!(!ready.is_empty());
    ready.advance();
    let ready = replica.ready();
    assert_eq!(ready.sealed_reads(), [], "an advanced batch is gone");
    ready.advance();
}

/// A replica open at its segment's end holds a read past its prefix, and
/// answers nothing sealed.
#[test]
fn an_open_replica_holds_a_read_past_its_prefix() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    let _ = drain(&mut replica, &mut disk);
    assert_eq!(replica.quorum_read_in(1, None), ReadOpen::Opened);
    for from in [0, 1] {
        replica.step(Message::PreReadAck {
            from: NodeId(from),
            ctx: 1,
            watermark: Some(Slot(5)),
            config_since: None,
        });
    }
    let ready = replica.ready();
    assert_eq!(ready.sealed_reads(), []);
    assert_eq!(ready.read_states(), []);
    ready.advance();
}

/// A sealed replica compacts below the `Roll` that ends its segment, never
/// past it: its base stays in the segment it keeps.
#[test]
fn a_sealed_replica_keeps_the_roll_when_it_compacts() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    replica.step(commit(1, write(8)));
    replica.step(commit(
        2,
        Command::Control(Control::Truncate {
            leader: LeaderUuid(0),
            up_to: Seq(2),
        }),
    ));
    replica.step(commit(3, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    assert!(replica.replica().is_sealed());
    assert!(replica.first_slot() <= Slot(3), "the `Roll` is kept");
    assert!(replica.first_slot() > Slot(0), "the truncation compacts");
    assert_eq!(replica.replica().journal_base().segment.id, SegmentId(0));
    let rebooted = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert_eq!(rebooted.replica().segment_bound(), Some(SegmentId(0)));
    assert!(rebooted.replica().is_sealed());
}

/// A segment whose `Roll` sits at slot 0 holds nothing to compact below it.
#[test]
fn a_roll_at_slot_zero_is_never_compacted() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    assert!(replica.replica().is_sealed());
    assert_eq!(replica.first_slot(), Slot(0));
}

/// A `Truncate` of every record and the `Roll` after it fold in one walk
/// when the `Roll` is learned first: the compaction stops at the `Roll`
/// (its slot is the floor), never past it, and the floor it leaves passes
/// the judge of a compaction.
#[test]
fn a_full_truncation_folded_with_the_roll_keeps_the_roll() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    replica.step(commit(1, write(8)));
    replica.step(commit(3, roll(1)));
    replica.step(commit(
        2,
        Command::Control(Control::Truncate {
            leader: LeaderUuid(0),
            up_to: Seq(2),
        }),
    ));
    let _ = drain(&mut replica, &mut disk);
    assert!(replica.replica().is_sealed());
    assert_eq!(replica.first_slot(), Slot(3), "the floor is the `Roll`");
    assert_eq!(replica.replica().journal_base().segment.id, SegmentId(0));
    let rebooted = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert!(rebooted.replica().is_sealed());
    assert_eq!(rebooted.first_slot(), Slot(3));
}

/// A slot past the end that the replica learned before its seal, and that
/// rotted on its disk, is not its own: the rebooted replica, sealed again,
/// counts no faulty record there and pulls nothing for it.
#[test]
fn a_rotted_slot_past_the_end_is_not_pulled_after_a_reboot() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    replica.step(commit(2, write(8)));
    replica.step(commit(1, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    assert!(replica.replica().is_sealed());
    assert_eq!(
        disk.initial_state().0.chosen_index,
        Some(Slot(1)),
        "the durable chosen index stops at the `Roll`"
    );
    let _ = disk.rot(Slot(2));
    let mut rebooted = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert!(rebooted.replica().is_sealed());
    rebooted.tick();
    let ready = rebooted.ready();
    assert!(
        ready.messages().is_empty(),
        "a sealed replica pulls nothing"
    );
    ready.advance();
}

/// A multi-writer replica of a matchmaker deployment, where segments
/// exist (R10).
fn matchmaker_multi() -> Config {
    Config {
        nodes: vec![NodeId(0), NodeId(1), NodeId(2)],
        matchmakers: vec![crate::membership::MatchmakerId(0)],
        ..multi()
    }
}

fn beat() -> Message {
    Message::Heartbeat {
        from: NodeId(0),
        ballot: Ballot {
            round: 1,
            node: NodeId(0),
        },
        commit: Some(Slot(1)),
        config: None,
        fence: None,
        gc_floor: None,
    }
}

/// The `SegmentHeld` reports a beat gets back.
fn reports(replica: &mut ReplicaNode, disk: &mut MemStorage) -> Vec<Message> {
    replica.step(beat());
    let ready = replica.ready();
    disk.apply(ready.writes());
    let sent = ready
        .messages()
        .iter()
        .filter(|(to, m)| {
            matches!(m, Message::SegmentHeld { .. })
                && *to == crate::contract::message::Audience::Node(NodeId(0))
        })
        .map(|(_, m)| m.clone())
        .collect();
    ready.advance();
    sent
}

/// A sealed replica that holds its whole segment answers each beat that it
/// holds it, up to its end; an open replica does not.
#[test]
fn a_sealed_replica_reports_its_whole_segment_on_each_beat() {
    let mut disk = MemStorage::new(matchmaker_multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    let _ = drain(&mut replica, &mut disk);
    assert_eq!(reports(&mut replica, &mut disk), [], "an open replica");
    replica.step(commit(1, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    let held = Message::SegmentHeld {
        from: NodeId(10),
        segment: SegmentId(0),
        end: Slot(2),
    };
    assert_eq!(
        reports(&mut replica, &mut disk),
        std::slice::from_ref(&held)
    );
    assert_eq!(reports(&mut replica, &mut disk), [held], "on each beat");
}

/// A replica that joined segment 1 at its start reports segment 1 once
/// sealed.
#[test]
fn a_joined_replica_reports_the_segment_it_joined() {
    let mut disk = MemStorage::new(matchmaker_multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert_eq!(replica.join_segment(Slot(2), boundary()), Ok(()));
    let _ = drain(&mut replica, &mut disk);
    replica.step(commit(2, write(8)));
    replica.step(commit(3, roll(2)));
    let _ = drain(&mut replica, &mut disk);
    assert_eq!(
        reports(&mut replica, &mut disk),
        [Message::SegmentHeld {
            from: NodeId(10),
            segment: SegmentId(1),
            end: Slot(4),
        }]
    );
}

/// A sealed replica that compacted inside its segment no longer holds all
/// of it, and a plain replica has no segments to report.
#[test]
fn a_compacted_or_plain_replica_reports_nothing() {
    let mut disk = MemStorage::new(matchmaker_multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    replica.step(commit(1, write(8)));
    replica.step(commit(
        2,
        Command::Control(Control::Truncate {
            leader: LeaderUuid(0),
            up_to: Seq(2),
        }),
    ));
    replica.step(commit(3, roll(1)));
    let _ = drain(&mut replica, &mut disk);
    assert!(replica.replica().is_sealed());
    assert!(replica.first_slot() > Slot(0));
    assert_eq!(reports(&mut replica, &mut disk), []);
    let mut disk = MemStorage::new(multi());
    let mut plain = ReplicaNode::new(&disk, Limits::DEFAULT);
    plain.step(commit(0, roll(1)));
    let _ = drain(&mut plain, &mut disk);
    assert!(plain.replica().is_sealed());
    assert_eq!(reports(&mut plain, &mut disk), []);
}

/// A replica whose chosen slot rotted on its disk boots with no record and
/// no fold, and still knows the slot chosen: it is not fresh.
#[test]
fn a_replica_with_a_rotted_chosen_slot_does_not_join() {
    let mut disk = MemStorage::new(multi());
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(commit(0, write(7)));
    let _ = drain(&mut replica, &mut disk);
    let _ = disk.rot(Slot(0));
    let mut rebooted = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert_eq!(rebooted.replica().chosen_index(), Some(Slot(0)));
    assert_eq!(rebooted.replica().folded(), Slot(0));
    assert_eq!(
        rebooted.join_segment(Slot(2), boundary()),
        Err(JoinRefusal::NotFresh)
    );
}

/// A lagging replica of the first segment gets a trim point of the next
/// one. With a state that proves every write of its segment released, it
/// marks the segment trimmed: it pulls nothing more, serves no read and
/// ignores later trim points. A state that proves nothing leaves it open.
#[test]
fn a_lagging_replica_of_a_released_segment_stops() {
    let pull_beat = Message::Heartbeat {
        from: NodeId(0),
        ballot: Ballot {
            round: 1,
            node: NodeId(0),
        },
        commit: Some(Slot(9)),
        config: None,
        fence: None,
        gc_floor: None,
    };
    // The state at the point, the head `first_seq` the sender adds, and
    // whether they prove the segment released.
    for (first_seq, head, releases) in [(0, 0, false), (1, 0, true), (0, 1, true)] {
        let mut disk = MemStorage::new(multi());
        let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
        // Slot 1 is learned, slot 0 is not: the replica lags.
        replica.step(commit(1, write(7)));
        let _ = drain(&mut replica, &mut disk);
        let state = JournalState {
            first_seq: Seq(first_seq),
            ..boundary()
        };
        assert_eq!(state.releases(SegmentId::FIRST, Seq(head)), releases);
        replica.step(Message::TrimmedTo {
            from: NodeId(0),
            point: Slot(3),
            state,
            first_seq: Seq(head),
        });
        assert_eq!(replica.first_slot(), Slot(0), "the trim point is not taken");
        assert_eq!(replica.is_trimmed(), releases);
        let _ = drain(&mut replica, &mut disk);
        let asked = replica.counters().catch_up_requests;
        replica.step(pull_beat.clone());
        assert_eq!(
            replica.counters().catch_up_requests,
            asked + u64::from(!releases),
            "only an open replica pulls"
        );
        assert_eq!(
            matches!(replica.read_log(Seq(0), 8, 1 << 20), LogRead::NotHeld),
            releases,
            "a trimmed replica serves no read"
        );
        let ignored = replica.counters().ignored;
        replica.step(Message::TrimmedTo {
            from: NodeId(0),
            point: Slot(4),
            state: boundary(),
            first_seq: Seq(0),
        });
        assert_eq!(
            replica.counters().ignored,
            ignored + u64::from(releases),
            "a trimmed replica ignores every trim point"
        );
        assert!(!replica.replica().is_sealed());
        assert_eq!(
            replica.quorum_read_in(9, None) == ReadOpen::Trimmed,
            releases,
            "a trimmed replica opens no read"
        );
    }
}

/// A replica of a segment `id`, multi-writer, whose set is `10` and `11`.
fn peer_config(id: u64) -> Config {
    Config {
        segment_replicas: Some(crate::journal::segment::ReplicaSet::new([
            NodeId(10),
            NodeId(11),
        ])),
        ..Config {
            id: NodeId(id),
            ..multi()
        }
    }
}

/// The messages of one batch, persisted on `disk` and acknowledged.
fn sent(replica: &mut ReplicaNode, disk: &mut MemStorage) -> Vec<(Audience, Message)> {
    let ready = replica.ready();
    disk.apply(ready.writes());
    let out = ready.messages().to_vec();
    ready.advance();
    out
}

/// A lagging replica of a segment that its peer holds gets a trim point of
/// the next segment that does not prove the segment released: the acceptors dropped the
/// segment at its `Hold`. It pulls the segment from its peer, which serves
/// its records, and it seals (R8 of §4.7). A party outside the set gets
/// nothing, and a sealed replica pulls nothing more.
#[test]
fn a_lagging_replica_pulls_its_segment_from_a_peer() {
    let mut peer_disk = MemStorage::new(peer_config(11));
    let mut peer = ReplicaNode::new(&peer_disk, Limits::DEFAULT);
    peer.step(commit(0, write(7)));
    peer.step(commit(1, roll(1)));
    let _ = drain(&mut peer, &mut peer_disk);
    assert_eq!(peer.replica().sealed_end(), Some(Slot(2)));

    let mut disk = MemStorage::new(peer_config(10));
    let mut laggard = ReplicaNode::new(&disk, Limits::DEFAULT);
    assert!(!boundary().releases(SegmentId::FIRST, Seq(0)));
    laggard.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(3),
        state: boundary(),
        first_seq: Seq(0),
    });
    assert!(!laggard.is_trimmed());
    let pulls = sent(&mut laggard, &mut disk);
    let to_peer = Audience::ReplicasOf {
        segment: SegmentId::FIRST,
        replicas: crate::journal::segment::ReplicaSet::new([NodeId(11)]),
    };
    assert_eq!(
        pulls,
        vec![(
            to_peer,
            Message::CatchUpRequest {
                from: NodeId(10),
                from_slot: Slot(0),
            }
        )]
    );
    assert_eq!(laggard.counters().peer_pulls, 1);

    // A party outside the set is not served, and the peer answers its own.
    peer.step(Message::CatchUpRequest {
        from: NodeId(12),
        from_slot: Slot(0),
    });
    assert_eq!(sent(&mut peer, &mut peer_disk), Vec::new());
    peer.step(pulls[0].1.clone());
    let pages = sent(&mut peer, &mut peer_disk);
    assert_eq!(pages.len(), 1);
    assert_eq!(peer.counters().peer_pages, 1);
    let (audience, page) = pages[0].clone();
    assert_eq!(
        audience,
        Audience::ReplicasOf {
            segment: SegmentId::FIRST,
            replicas: crate::journal::segment::ReplicaSet::new([NodeId(10)]),
        }
    );
    let Message::CatchUpResponse { from, entries } = &page else {
        panic!("a peer answers with a page, got {page:?}");
    };
    assert_eq!(*from, NodeId(11));
    assert_eq!(
        entries.keys().copied().collect::<Vec<_>>(),
        vec![Slot(0), Slot(1)]
    );

    // A page from outside the set teaches nothing; the peer's page seals.
    let ignored = laggard.counters().ignored;
    laggard.step(Message::CatchUpResponse {
        from: NodeId(12),
        entries: entries.clone(),
    });
    assert_eq!(laggard.counters().ignored, ignored + 1);
    laggard.step(page);
    assert_eq!(drain(&mut laggard, &mut disk), vec![Slot(0), Slot(1)]);
    assert_eq!(laggard.replica().sealed_end(), Some(Slot(2)));
    assert_eq!(laggard.chosen_at(Slot(0)), Some(&write(7)));

    // Sealed, it pulls nothing more on the next trim point.
    laggard.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(3),
        state: boundary(),
        first_seq: Seq(0),
    });
    assert_eq!(sent(&mut laggard, &mut disk), Vec::new());
    assert_eq!(laggard.counters().peer_pulls, 1);
}

/// With no peer in its set, a lagging replica has no one to pull from.
#[test]
fn a_replica_alone_in_its_set_pulls_from_no_peer() {
    let mut disk = MemStorage::new(Config {
        segment_replicas: Some(crate::journal::segment::ReplicaSet::new([NodeId(10)])),
        ..multi()
    });
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(3),
        state: boundary(),
        first_seq: Seq(0),
    });
    assert_eq!(sent(&mut replica, &mut disk), Vec::new());
    assert_eq!(replica.counters().peer_pulls, 0);
}

/// A pooled node that the set names (here the leader, node 0) dropped the
/// segment at its `Hold` like every acceptor, and it answers with a trim
/// point. The pull skips it and turns over the replica peers, one for each
/// trim point, so the trim points of the leader never stall the pull
/// (`PullRule = "replicas"` of `specs/tla/peer-pull/`).
#[test]
fn a_lagging_replica_pulls_from_no_pooled_node_of_its_set() {
    let mut disk = MemStorage::new(Config {
        segment_replicas: Some(crate::journal::segment::ReplicaSet::new([
            NodeId(0),
            NodeId(10),
            NodeId(11),
            NodeId(12),
            NodeId(13),
        ])),
        ..Config {
            id: NodeId(10),
            ..multi()
        }
    });
    let mut laggard = ReplicaNode::new(&disk, Limits::DEFAULT);
    for (turn, peer) in [11, 12, 13, 11].into_iter().enumerate() {
        laggard.step(Message::TrimmedTo {
            from: NodeId(0),
            point: Slot(3),
            state: boundary(),
            first_seq: Seq(0),
        });
        let pulls = sent(&mut laggard, &mut disk);
        assert_eq!(
            pulls.iter().map(|(to, _)| to.clone()).collect::<Vec<_>>(),
            vec![Audience::ReplicasOf {
                segment: SegmentId::FIRST,
                replicas: crate::journal::segment::ReplicaSet::new([NodeId(peer)]),
            }]
        );
        assert_eq!(laggard.counters().peer_pulls, turn as u64 + 1);
    }
}

/// The slots of a peer's page, asked from `from_slot` by replica 10.
fn page_from(peer: &mut ReplicaNode, disk: &mut MemStorage, from_slot: u64) -> Vec<Slot> {
    peer.step(Message::CatchUpRequest {
        from: NodeId(10),
        from_slot: Slot(from_slot),
    });
    sent(peer, disk)
        .into_iter()
        .flat_map(|(_, page)| match page {
            Message::CatchUpResponse { entries, .. } => entries.into_keys().collect::<Vec<_>>(),
            other => panic!("a peer answers with a page, got {other:?}"),
        })
        .collect()
}

/// A peer pages its records contiguous from the asked slot: a hole ends
/// the page, a slot it does not hold gives no answer, and a trimmed peer
/// serves nothing.
#[test]
fn a_peer_serves_a_contiguous_page_from_the_asked_slot() {
    let mut disk = MemStorage::new(peer_config(11));
    let mut peer = ReplicaNode::new(&disk, Limits::DEFAULT);
    peer.step(commit(0, write(7)));
    peer.step(commit(1, write(8)));
    peer.step(commit(3, write(9)));
    let _ = drain(&mut peer, &mut disk);
    assert_eq!(page_from(&mut peer, &mut disk, 0), vec![Slot(0), Slot(1)]);
    assert_eq!(page_from(&mut peer, &mut disk, 1), vec![Slot(1)]);
    assert_eq!(page_from(&mut peer, &mut disk, 2), Vec::<Slot>::new());
    assert_eq!(page_from(&mut peer, &mut disk, 3), vec![Slot(3)]);
    assert_eq!(peer.counters().peer_pages, 3);

    // A trim point that proves the segment released marks the peer
    // trimmed: it serves nothing more.
    peer.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(5),
        state: JournalState {
            first_seq: Seq(1),
            ..boundary()
        },
        first_seq: Seq(0),
    });
    assert!(peer.is_trimmed());
    let _ = sent(&mut peer, &mut disk);
    assert_eq!(page_from(&mut peer, &mut disk, 3), Vec::<Slot>::new());
    assert_eq!(peer.counters().peer_pages, 3);
}

/// A page holds at most `CATCHUP_BATCH` records: the receiver drops a
/// longer one, so a longer page would heal nothing.
#[test]
fn a_peer_page_holds_at_most_one_batch() {
    use crate::contract::limits::CATCHUP_BATCH;
    let mut disk = MemStorage::new(peer_config(11));
    let mut peer = ReplicaNode::new(&disk, Limits::DEFAULT);
    let held = CATCHUP_BATCH as u64 + 2;
    for slot in 0..held {
        peer.step(commit(slot, write(7)));
    }
    let _ = drain(&mut peer, &mut disk);
    let page = page_from(&mut peer, &mut disk, 0);
    assert_eq!(page.len(), CATCHUP_BATCH);
    assert_eq!(page.first(), Some(&Slot(0)));
    assert_eq!(page.last(), Some(&Slot(CATCHUP_BATCH as u64 - 1)));
    assert_eq!(
        page_from(&mut peer, &mut disk, held - 1),
        vec![Slot(held - 1)]
    );
}

/// A pull of the first segment, queued before the replica joins the next
/// one, addresses a peer of the segment it leaves: the join drops it, and
/// the batch after the join sends no message to a replica set.
#[test]
fn a_join_drops_the_pull_of_the_segment_it_leaves() {
    let mut disk = MemStorage::new(peer_config(10));
    let mut replica = ReplicaNode::new(&disk, Limits::DEFAULT);
    replica.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(3),
        state: boundary(),
        first_seq: Seq(0),
    });
    assert_eq!(replica.counters().peer_pulls, 1);
    assert_eq!(replica.join_segment(Slot(2), boundary()), Ok(()));
    let out = sent(&mut replica, &mut disk);
    assert!(
        out.iter()
            .all(|(to, _)| !matches!(to, Audience::ReplicasOf { .. })),
        "a join keeps no pull of the segment it leaves: {out:?}"
    );
}
