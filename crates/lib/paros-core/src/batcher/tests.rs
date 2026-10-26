use super::*;
use crate::{JournalId, TenantId};

fn journal(n: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(1), JournalId(n))
}

fn limits(max_records: usize, max_bytes: usize, flush_records: usize) -> BatchLimits {
    BatchLimits {
        max_records,
        max_bytes,
        flush_records,
    }
}

fn records(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| vec![u8::try_from(i).unwrap(); 2]).collect()
}

#[test]
fn writes_merge_until_a_tick() {
    let mut b = Batcher::new(limits(16, 1024, 16));
    assert!(b.push(journal(1), 1, records(2)).is_empty());
    assert!(b.push(journal(1), 2, records(3)).is_empty());
    assert!(b.push(journal(2), 3, records(1)).is_empty());
    assert_eq!(b.pending(), 3);
    let out = b.tick();
    assert_eq!(out.len(), 2, "one batch per journal, never mixed");
    assert_eq!(out[0].journal, journal(1));
    assert_eq!(out[0].writes, vec![(1, 2), (2, 3)]);
    assert_eq!(out[0].records.len(), 5);
    assert!(out[0].merged());
    assert_eq!(out[1].writes, vec![(3, 1)]);
    assert_eq!(b.pending(), 0);
}

#[test]
fn a_full_lane_flushes_and_the_limits_hold() {
    let mut b = Batcher::new(limits(4, 1024, 3));
    assert!(b.push(journal(1), 1, records(2)).is_empty());
    let out = b.push(journal(1), 2, records(2));
    assert_eq!(out.len(), 1, "the flush bound is reached");
    assert_eq!(out[0].records.len(), 4);
    // Past the record limit: the lane goes first, the write starts a new one.
    let mut b = Batcher::new(limits(4, 1024, 4));
    assert!(b.push(journal(1), 1, records(3)).is_empty());
    let out = b.push(journal(1), 2, records(2));
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].writes, vec![(1, 3)]);
    assert_eq!(b.pending(), 1);
    // Past the byte limit likewise.
    let mut b = Batcher::new(limits(16, 6, 16));
    assert!(b.push(journal(1), 1, records(2)).is_empty());
    let out = b.push(journal(1), 2, records(2));
    assert_eq!(out[0].writes, vec![(1, 2)]);
}

#[test]
fn empty_and_oversized_writes_go_alone_after_their_lane() {
    let mut b = Batcher::new(limits(4, 1024, 4));
    assert!(b.push(journal(1), 1, records(1)).is_empty());
    let out = b.push(journal(1), 2, Vec::new());
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].writes, vec![(1, 1)]);
    assert_eq!(out[1].writes, vec![(2, 0)]);
    let out = b.push(journal(1), 3, records(5));
    assert_eq!(out.len(), 1, "an empty lane sends only the lone write");
    assert_eq!(out[0].writes, vec![(3, 5)]);
    assert_eq!(b.pending(), 0);
}

#[test]
fn lowered_limits_only_go_down() {
    let mut b: Batcher<u8> = Batcher::new(limits(8, 100, 6));
    b.lower_limits(4, 1000);
    assert_eq!(b.limits(), limits(4, 100, 4));
    b.lower_limits(0, 0);
    assert_eq!(b.limits(), limits(1, 1, 1));
}

#[test]
fn a_scatter_is_dense_and_in_order() {
    let out = Unbatcher::scatter(&[(7, 2), (8, 0), (9, 3)], 10, 5);
    assert_eq!(out, vec![(7, 10, 2), (8, 12, 0), (9, 12, 3)]);
}

#[test]
#[should_panic(expected = "accepted whole")]
fn a_scatter_refuses_a_partial_answer() {
    let _ = Unbatcher::scatter(&[(1, 2), (2, 2)], 0, 3);
}
