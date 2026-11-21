use super::*;
use crate::journal::{JournalId, TenantId};

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
    assert!(
        b.push(journal(1), 1, records(2)).is_empty(),
        "a write under the limits waits in its lane"
    );
    assert!(
        b.push(journal(1), 2, records(3)).is_empty(),
        "a second write under the limits joins its lane"
    );
    assert!(
        b.push(journal(2), 3, records(1)).is_empty(),
        "a write for another journal opens its own lane"
    );
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
    assert!(
        b.push(journal(1), 1, records(2)).is_empty(),
        "a lane below the flush bound waits"
    );
    let out = b.push(journal(1), 2, records(2));
    assert_eq!(out.len(), 1, "the flush bound is reached");
    assert_eq!(out[0].records.len(), 4);
    // Past the record limit: the lane goes first, the write starts a new one.
    let mut b = Batcher::new(limits(4, 1024, 4));
    assert!(
        b.push(journal(1), 1, records(3)).is_empty(),
        "a lane below the record limit waits"
    );
    let out = b.push(journal(1), 2, records(2));
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].writes, vec![(1, 3)]);
    assert_eq!(b.pending(), 1);
    // Past the byte limit likewise.
    let mut b = Batcher::new(limits(16, 6, 16));
    assert!(
        b.push(journal(1), 1, records(2)).is_empty(),
        "a lane below the byte limit waits"
    );
    let out = b.push(journal(1), 2, records(2));
    assert_eq!(out[0].writes, vec![(1, 2)]);
}

#[test]
fn empty_and_oversized_writes_go_alone_after_their_lane() {
    let mut b = Batcher::new(limits(4, 1024, 4));
    assert!(
        b.push(journal(1), 1, records(1)).is_empty(),
        "a small write waits in its lane"
    );
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
    assert_eq!(out, Some(vec![(7, 10, 2), (8, 12, 0), (9, 12, 3)]));
}

/// An answer that does not count the batch is an input, refused with
/// `None`, never a panic: a count too small, too large, or a range past
/// `u64::MAX`.
#[test]
fn a_scatter_refuses_an_answer_that_is_not_the_batch() {
    assert_eq!(Unbatcher::scatter(&[(1, 2), (2, 2)], 0, 3), None);
    assert_eq!(Unbatcher::scatter(&[(1, 2), (2, 2)], 0, 5), None);
    assert_eq!(Unbatcher::scatter(&[(1, 2)], u64::MAX - 1, 2), None);
    assert_eq!(
        Unbatcher::scatter(&[(1, 2)], u64::MAX - 2, 2),
        Some(vec![(1, u64::MAX - 2, 2)])
    );
    assert_eq!(Unbatcher::scatter::<u8>(&[], 7, 0), Some(Vec::new()));
}

/// A write that would carry its lane past the byte limit flushes the lane
/// first: one byte waiting and four more pass a limit of four.
#[test]
fn a_write_past_the_byte_limit_flushes_its_lane_first() {
    let mut b = Batcher::new(limits(16, 4, 16));
    assert_eq!(b.push(journal(1), 1, vec![vec![0]]), Vec::new());
    let out = b.push(journal(1), 2, vec![vec![0; 4]]);
    assert_eq!(out.len(), 1, "the waiting lane goes first");
    assert_eq!(out[0].writes, vec![(1, 1)]);
    assert_eq!(b.pending(), 1, "the new write starts a lane of its own");
}

/// Every bound of `BatchLimits` is checked on its own.
#[test]
fn each_batch_limit_is_checked() {
    assert!(limits(4, 8, 4).valid());
    assert!(limits(4, 8, 1).valid());
    assert!(!limits(0, 8, 1).valid(), "no record");
    assert!(!limits(4, 0, 1).valid(), "no byte");
    assert!(!limits(4, 8, 0).valid(), "no flush");
    assert!(!limits(4, 8, 5).valid(), "a flush past the record limit");
}
