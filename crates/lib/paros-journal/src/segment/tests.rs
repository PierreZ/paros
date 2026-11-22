use super::*;
use crate::codec::RELEASE;
use crate::command::Leader;

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn grid() -> Grid {
    Grid::new(1, vec![nodes(&[1, 2, 3])]).expect("a v1 grid")
}

fn opening(expected: Option<u64>) -> OpeningCall {
    OpeningCall {
        term: 4,
        expected: expected.map(SegmentId),
        grid: grid(),
        replicas: nodes(&[10, 11, 12]),
    }
}

fn boundary(next: u64) -> BoundaryState {
    BoundaryState {
        next_seq: Seq(next),
        first_seq: Seq(0),
        leader: Leader::Multi,
    }
}

fn sealed() -> SegmentRecord {
    let mut record = SegmentRecord::open(SegmentId(2), RELEASE, opening(Some(1))).expect("open");
    record.put_in_force(Slot(5)).expect("in force");
    record.seal(Slot(9)).expect("sealed");
    record
}

#[test]
fn a_v1_grid_has_columns_of_2f_plus_1() {
    let grid = Grid::new(1, vec![nodes(&[1, 2, 3]), nodes(&[4, 5, 6])]).expect("two columns");
    assert_eq!((grid.rows(), grid.acks(), grid.f()), (3, 2, 1));
    assert_eq!(Grid::new(1, vec![]), Err(GridError::NoColumn));
    assert_eq!(
        Grid::new(1, vec![nodes(&[1, 2])]),
        Err(GridError::ColumnSize { column: 0, size: 2 })
    );
    assert_eq!(
        Grid::new(1, vec![nodes(&[1, 2, 3]), nodes(&[3, 4, 5])]),
        Err(GridError::Repeated(NodeId(3)))
    );
}

#[test]
fn a_segment_goes_through_its_states_in_order() {
    let mut record = sealed();
    assert_eq!(record.start(), Some(Slot(5)));
    assert_eq!(record.end(), Some(Slot(9)));
    record.write_boundary(boundary(7)).expect("boundary");
    record.retire(2, false).expect("retired");
    assert_eq!(record.state(), SegmentState::Retired);
    record.mark_offloaded().expect("offloaded");
    record.trim(Seq(7)).expect("trimmed");
    assert_eq!(record.state(), SegmentState::Trimmed);
}

#[test]
fn a_new_segment_needs_replicas_and_an_older_expected_one() {
    let mut no_replicas = opening(None);
    no_replicas.replicas.clear();
    assert_eq!(
        SegmentRecord::open(SegmentId(1), RELEASE, no_replicas),
        Err(RecordRefusal::Replicas)
    );
    let mut twice = opening(None);
    twice.replicas = nodes(&[1, 1]);
    assert_eq!(
        SegmentRecord::open(SegmentId(1), RELEASE, twice),
        Err(RecordRefusal::Replicas)
    );
    assert_eq!(
        SegmentRecord::open(SegmentId(2), RELEASE, opening(Some(2))),
        Err(RecordRefusal::Expected)
    );
}

#[test]
fn the_seal_end_is_written_once_and_not_below_the_start() {
    let mut record = SegmentRecord::open(SegmentId(2), RELEASE, opening(Some(1))).expect("open");
    assert_eq!(
        record.seal(Slot(9)),
        Err(RecordRefusal::State(SegmentState::Pending)),
        "a pending segment is not sealed"
    );
    record.put_in_force(Slot(5)).expect("in force");
    assert_eq!(
        record.seal(Slot(4)),
        Err(RecordRefusal::EndBelowStart {
            start: Slot(5),
            end: Slot(4)
        })
    );
    record.seal(Slot(5)).expect("an empty segment seals");
    assert_eq!(
        record.seal(Slot(8)),
        Err(RecordRefusal::State(SegmentState::Sealed)),
        "a second end is refused"
    );
    assert_eq!(record.end(), Some(Slot(5)));
}

#[test]
fn the_boundary_state_is_written_once_on_a_sealed_segment() {
    let mut pending = SegmentRecord::open(SegmentId(1), RELEASE, opening(None)).expect("open");
    assert_eq!(
        pending.write_boundary(boundary(1)),
        Err(RecordRefusal::State(SegmentState::Pending))
    );
    let mut record = sealed();
    let bad = BoundaryState {
        next_seq: Seq(1),
        first_seq: Seq(2),
        leader: Leader::Unset,
    };
    assert_eq!(
        record.write_boundary(bad),
        Err(RecordRefusal::BoundaryInvalid)
    );
    record.write_boundary(boundary(7)).expect("boundary");
    assert_eq!(
        record.write_boundary(boundary(8)),
        Err(RecordRefusal::BoundaryWritten)
    );
    assert_eq!(record.boundary(), Some(boundary(7)));
}

#[test]
fn retire_needs_the_boundary_f_plus_1_holders_and_no_damage() {
    // `retire_early` and `retire_norecord` of segment-replicas/, and the
    // damage margin of ctrl-segments/.
    let mut record = sealed();
    assert_eq!(record.retire(3, false), Err(RecordRefusal::NoBoundary));
    record.write_boundary(boundary(7)).expect("boundary");
    assert_eq!(
        record.retire(1, false),
        Err(RecordRefusal::FewHolders {
            holders: 1,
            needed: 2
        })
    );
    assert_eq!(record.retire(2, true), Err(RecordRefusal::Damage));
    assert_eq!(
        record.state(),
        SegmentState::Sealed,
        "a refusal changes nothing"
    );
    record.retire(2, false).expect("retired");
}

#[test]
fn trim_needs_a_sealed_segment_and_its_floor_over_it() {
    // `trim_open` and `trim_intent` of segment-truncate/.
    let mut open = SegmentRecord::open(SegmentId(1), RELEASE, opening(None)).expect("open");
    open.put_in_force(Slot(0)).expect("in force");
    assert_eq!(
        open.trim(Seq(100)),
        Err(RecordRefusal::State(SegmentState::InForce))
    );
    let mut record = sealed();
    assert_eq!(record.trim(Seq(100)), Err(RecordRefusal::NoBoundary));
    record.write_boundary(boundary(7)).expect("boundary");
    assert_eq!(
        record.trim(Seq(6)),
        Err(RecordRefusal::FloorBelowEnd {
            first_seq: Seq(6),
            last: Seq(7)
        })
    );
    record.trim(Seq(7)).expect("trimmed from sealed");
    // TrimStays: a trimmed record never becomes live again.
    assert_eq!(
        record.retire(3, false),
        Err(RecordRefusal::State(SegmentState::Trimmed))
    );
    assert_eq!(
        record.mark_offloaded(),
        Err(RecordRefusal::State(SegmentState::Trimmed))
    );
    assert_eq!(
        record.trim(Seq(9)),
        Err(RecordRefusal::State(SegmentState::Trimmed))
    );
}

#[test]
fn offload_needs_a_sealed_segment_with_its_boundary() {
    let mut record = sealed();
    assert_eq!(record.mark_offloaded(), Err(RecordRefusal::NoBoundary));
    record.write_boundary(boundary(7)).expect("boundary");
    record.mark_offloaded().expect("offloaded from sealed");
    assert_eq!(
        record.mark_offloaded(),
        Err(RecordRefusal::State(SegmentState::Offloaded))
    );
}

#[test]
fn from_parts_refuses_fields_that_disagree() {
    let record = sealed();
    let parts = |state, start, end, boundary| {
        SegmentRecord::from_parts(
            record.id(),
            record.release(),
            record.opening().clone(),
            record.replicas().to_vec(),
            state,
            start,
            end,
            boundary,
        )
    };
    assert!(parts(SegmentState::Sealed, Some(Slot(5)), Some(Slot(9)), None).is_ok());
    assert!(
        parts(SegmentState::Sealed, Some(Slot(5)), None, None).is_err(),
        "no end"
    );
    assert!(
        parts(SegmentState::Pending, Some(Slot(5)), None, None).is_err(),
        "early start"
    );
    assert!(
        parts(SegmentState::Sealed, Some(Slot(5)), Some(Slot(4)), None).is_err(),
        "end below start"
    );
    assert!(
        parts(SegmentState::Retired, Some(Slot(5)), Some(Slot(9)), None).is_err(),
        "retired with no boundary"
    );
    assert!(
        parts(
            SegmentState::InForce,
            Some(Slot(5)),
            None,
            Some(boundary(1))
        )
        .is_err(),
        "a boundary before the seal"
    );
}

#[test]
fn a_grid_of_f_2_has_columns_of_5() {
    let grid = Grid::new(2, vec![nodes(&[1, 2, 3, 4, 5])]).expect("a v1 grid");
    assert_eq!((grid.f(), grid.rows(), grid.acks()), (2, 5, 3));
}

#[test]
fn from_parts_refuses_a_replica_named_twice() {
    let record = sealed();
    let parts = |replicas: Vec<NodeId>| {
        SegmentRecord::from_parts(
            record.id(),
            record.release(),
            record.opening().clone(),
            replicas,
            SegmentState::Sealed,
            Some(Slot(5)),
            Some(Slot(9)),
            None,
        )
    };
    assert_eq!(
        parts(nodes(&[10, 10, 11])).err(),
        Some(RecordRefusal::Replicas)
    );
    assert_eq!(parts(Vec::new()).err(), Some(RecordRefusal::Replicas));
}

#[test]
fn an_empty_segment_ends_at_its_start() {
    let record = sealed();
    assert!(
        SegmentRecord::from_parts(
            record.id(),
            record.release(),
            record.opening().clone(),
            record.replicas().to_vec(),
            SegmentState::Sealed,
            Some(Slot(5)),
            Some(Slot(5)),
            None,
        )
        .is_ok()
    );
}

#[test]
#[should_panic(expected = "a segment has replicas")]
fn the_invariants_see_no_replica() {
    let mut record = sealed();
    record.replicas.clear();
    record.assert_invariants();
}

fn retired() -> SegmentRecord {
    let mut record = sealed();
    record.write_boundary(boundary(4)).expect("boundary");
    record.retire(2, false).expect("retired");
    record
}

#[test]
fn a_heal_swaps_one_member_of_a_retired_segment_in_place() {
    let mut record = retired();
    record
        .replace_replica(NodeId(11), NodeId(20))
        .expect("a heal of a retired segment");
    assert_eq!(record.replicas(), nodes(&[10, 20, 12]));
    assert_eq!(record.state(), SegmentState::Retired);
    assert_eq!(
        record.opening().replicas,
        nodes(&[10, 11, 12]),
        "the opening call keeps the set it named"
    );
}

#[test]
fn a_heal_starts_only_from_retired() {
    let mut sealed = sealed();
    sealed.write_boundary(boundary(4)).expect("boundary");
    let before = sealed.clone();
    assert_eq!(
        sealed.replace_replica(NodeId(11), NodeId(20)),
        Err(RecordRefusal::State(SegmentState::Sealed)),
        "a sealed segment still has its acceptors: it retires first"
    );
    assert_eq!(sealed, before);
    let mut trimmed = retired();
    trimmed.trim(Seq(4)).expect("trimmed");
    assert_eq!(
        trimmed.replace_replica(NodeId(11), NodeId(20)),
        Err(RecordRefusal::State(SegmentState::Trimmed)),
        "a trimmed record stays trimmed"
    );
}

#[test]
fn a_heal_removes_a_member_and_adds_a_new_node() {
    let mut record = retired();
    let before = record.clone();
    assert_eq!(
        record.replace_replica(NodeId(30), NodeId(20)),
        Err(RecordRefusal::NotMember(NodeId(30)))
    );
    assert_eq!(
        record.replace_replica(NodeId(11), NodeId(12)),
        Err(RecordRefusal::AlreadyMember(NodeId(12)))
    );
    assert_eq!(
        record.replace_replica(NodeId(11), NodeId(11)),
        Err(RecordRefusal::AlreadyMember(NodeId(11)))
    );
    assert_eq!(record, before, "a refusal changes nothing");
}
