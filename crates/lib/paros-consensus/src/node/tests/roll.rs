//! The `Roll` (§4.7 of `docs/architecture/04-7-segments.md`), pinned at the
//! node: the refusals of `roll`, the cut that every node folds at the same
//! slot, and a second roll pipelined above the first (no pause).

use super::reconfigure::deployed_cluster;
use super::*;
use crate::contract::limits::Limits;
use crate::journal::{Command, Outcome, ReplicaSet, Roll, Segment, SegmentId};
use crate::node::{RollRefusal, RollResult};

/// A plain deployment refuses a roll (D5), and a follower of a matchmaker
/// deployment names its leader.
#[test]
fn a_roll_is_refused_without_matchmakers_and_off_the_leader() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    assert_eq!(
        nodes[0].roll(None),
        RollResult::Refused(RollRefusal::NoMatchmakers)
    );
    assert!(
        drain(&mut nodes[0]).is_empty(),
        "a refused roll sends nothing"
    );

    let (mut nodes, _) = deployed_cluster();
    assert_eq!(nodes[1].roll(None), RollResult::NotLeader(Some(NodeId(0))));
}

/// The leader rolls twice without waiting (no pause): the second `Roll`
/// names the segment after the first, from the leader's open round (R4c).
/// Every node folds both at the same slots, so each segment has one end.
#[test]
fn every_node_folds_a_pipelined_pair_of_rolls_at_the_same_slots() {
    let (mut nodes, _) = deployed_cluster();
    let RollResult::Accepted { slot: first, next } = nodes[0].roll(None) else {
        panic!("the leader admits a roll");
    };
    assert_eq!(next, SegmentId(1));
    let RollResult::Accepted { slot: second, next } =
        nodes[0].roll(Some(ReplicaSet::new([NodeId(3), NodeId(4)])))
    else {
        panic!("the leader admits a second roll before the first is chosen");
    };
    assert_eq!(
        next,
        SegmentId(2),
        "the second roll builds on the open first"
    );
    assert!(second > first);
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    let one = Segment {
        id: SegmentId(1),
        start: Slot(first.0 + 1),
    };
    let two = Segment {
        id: SegmentId(2),
        start: Slot(second.0 + 1),
    };
    for n in &nodes[..3] {
        assert_eq!(n.replica().outcome_at(first), Some(&Outcome::Rolled(one)));
        assert_eq!(n.replica().outcome_at(second), Some(&Outcome::Rolled(two)));
        assert_eq!(n.replica().journal().segment, two);
    }
    // The next roll builds on the folded tail.
    let RollResult::Accepted { next, .. } = nodes[0].roll(None) else {
        panic!("the leader admits a third roll");
    };
    assert_eq!(next, SegmentId(3));
}

/// A leader whose bounded recovery has not yet opened the round of a
/// recovered `Roll` still counts it in the tail (R4c): the values its
/// Phase 1 recovered hold it until the pump proposes them again.
#[test]
fn the_tail_counts_a_roll_that_a_paged_recovery_has_not_opened_yet() {
    paged_recovery_holds_the_roll(false);
}

/// The same with a report below the recovery's cursor: the candidate learned
/// slot 0 chosen before it won, so its recovery starts at slot 1 and the
/// report of slot 0 stays behind in the recovered values. Asking the tail
/// then is a correct driver's call, not a broken invariant.
#[test]
fn the_tail_skips_a_report_below_the_recovery_cursor() {
    paged_recovery_holds_the_roll(true);
}

fn paged_recovery_holds_the_roll(learned_first: bool) {
    let limits = Limits {
        recovery_page: 1,
        ..Limits::DEFAULT
    };
    let mut candidate = ColocatedNode::new(&MemStorage::for_node(1, &[0, 1, 2]), limits);
    campaign(&mut candidate);
    let ballot = candidate.ballot();
    let _ = drain(&mut candidate);

    let mut storage = MemStorage::for_node(0, &[0, 1, 2]);
    let roll = Command::Roll(Roll {
        next: SegmentId(1),
        replicas: None,
    });
    let commands = [ucmd(7, 0, 1), ucmd(7, 1, 2), roll, ucmd(7, 2, 3)];
    for (slot, command) in commands.iter().enumerate() {
        let slot = Slot(u64::try_from(slot).expect("fits"));
        storage
            .accepted
            .insert(slot, (Ballot::zero(), command.clone()));
    }
    if learned_first {
        candidate.step(Message::Commit {
            from: Party::Node(NodeId(0)),
            ballot: Ballot::zero(),
            slot: Slot(0),
            command: commands[0].clone(),
        });
        let _ = drain(&mut candidate);
        assert!(!candidate.is_leader(), "the campaign is still open");
        assert!(candidate.replica.chosen().contains(&Slot(0)));
    }
    let mut acceptor = ColocatedNode::new(&storage, Limits::DEFAULT);
    acceptor.step(Message::Prepare {
        reply_to: NodeId(1),
        ballot,
        from_slot: Slot(0),
        config: None,
    });
    for (_, message) in drain(&mut acceptor) {
        if matches!(message, Message::Promise { .. }) {
            candidate.step(message);
        }
    }
    assert!(candidate.is_leader(), "one promise and its own elect it");
    let _ = drain(&mut candidate);

    let recovery = candidate
        .proposer
        .recovery()
        .expect("a paged recovery stays open");
    assert!(
        recovery.cursor() <= Slot(2),
        "the recovery has not handed out the Roll's slot"
    );
    assert!(
        recovery.pending().any(|(slot, _)| *slot == Slot(2)),
        "the Roll waits in the recovery, not in a round"
    );
    assert!(!candidate.proposer.rounds().contains_key(&Slot(2)));
    assert_eq!(candidate.tail_segment(), SegmentId(1));
}
