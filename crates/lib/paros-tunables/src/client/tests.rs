//! The client's tunables through their public API: the checkpoint policy
//! they name and the batch rule a checkpoint run writes by.

use std::time::Duration;

use super::{CheckpointPolicy, ClientTunables};

fn records(lens: &[usize]) -> Vec<Vec<u8>> {
    lens.iter().map(|&len| vec![0; len]).collect()
}

#[test]
fn the_policy_carries_each_checkpoint_tunable() {
    let tunables = ClientTunables {
        checkpoint_factor: 3,
        checkpoint_interval: Duration::from_secs(7),
        checkpoint_chunk_bytes: 11,
        checkpoint_batch_records: 13,
        checkpoint_batch_bytes: 17,
        ..ClientTunables::default()
    };
    assert_eq!(
        tunables.checkpoint_policy(),
        CheckpointPolicy {
            factor: 3,
            interval: Duration::from_secs(7),
            chunk_bytes: 11,
            batch_records: 13,
            batch_bytes: 17,
        }
    );
}

#[test]
fn the_default_policy_is_the_default_tunables_policy() {
    let policy = ClientTunables::default().checkpoint_policy();
    assert_eq!(policy.factor, 4);
    assert_eq!(policy.interval, Duration::from_mins(1));
    assert_eq!(policy.chunk_bytes, 8 << 10);
    assert_eq!(policy.batch_records, 64);
    assert_eq!(policy.batch_bytes, 256 << 10);
}

/// A default reader asks for the server's own page size and does not wait
/// at the tail: `0` is the "not set" value of both.
#[test]
fn a_default_reader_takes_the_server_page_and_does_not_wait() {
    let tunables = ClientTunables::default();
    assert_eq!(tunables.page_size, 0);
    assert_eq!(tunables.wait_ms, 0);
}

#[test]
fn a_batch_takes_one_record_then_what_fits() {
    let three = records(&[4, 4, 4]);
    assert_eq!(CheckpointPolicy::batch_len(&three, (8, 100)), 3);
    assert_eq!(CheckpointPolicy::batch_len(&three, (2, 100)), 2);
    assert_eq!(CheckpointPolicy::batch_len(&three, (8, 1)), 1);
    assert_eq!(CheckpointPolicy::batch_len(&three, (0, 0)), 1);
}

#[test]
fn a_record_that_fills_the_byte_limit_exactly_fits() {
    let three = records(&[4, 4, 4]);
    assert_eq!(CheckpointPolicy::batch_len(&three, (8, 8)), 2);
    assert_eq!(CheckpointPolicy::batch_len(&three, (8, 7)), 1);
    assert_eq!(CheckpointPolicy::batch_len(&three, (8, 12)), 3);
}

#[test]
fn a_record_past_the_limit_stops_the_batch_even_if_a_later_one_fits() {
    let mixed = records(&[4, 10, 1]);
    assert_eq!(CheckpointPolicy::batch_len(&mixed, (8, 8)), 1);
}

#[test]
fn a_first_record_larger_than_the_byte_limit_still_goes() {
    let big = records(&[50, 1]);
    assert_eq!(CheckpointPolicy::batch_len(&big, (8, 10)), 1);
}

#[test]
#[should_panic(expected = "a batch takes a record")]
fn an_empty_run_is_a_programmer_error() {
    let _ = CheckpointPolicy::batch_len(&[], (8, 8));
}
