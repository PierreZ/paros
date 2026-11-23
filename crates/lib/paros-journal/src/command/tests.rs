use super::*;

fn record(payload: &[u8], time: Option<u64>) -> Record {
    Record {
        headers: Headers {
            leader_time: time,
            client: vec![Header {
                key: b"k".to_vec(),
                value: payload.to_vec(),
            }],
        },
        payload: payload.to_vec(),
    }
}

fn fenced(uuid: u128, seq: u64, records: Vec<Record>) -> Write {
    Write {
        fence: Fence::Leader {
            uuid: LeaderUuid(uuid),
            expected_seq: Seq(seq),
        },
        records,
    }
}

#[test]
fn the_leader_time_is_not_part_of_the_write() {
    let a = fenced(1, 0, vec![record(b"x", Some(5))]);
    let b = fenced(1, 0, vec![record(b"x", Some(9))]);
    assert!(a.same_write(&b));
    assert_ne!(a, b, "the commands still differ");
}

#[test]
fn a_write_is_its_fence_and_its_client_records() {
    let a = fenced(1, 0, vec![record(b"x", None)]);
    assert!(
        !a.same_write(&fenced(2, 0, vec![record(b"x", None)])),
        "another uuid"
    );
    assert!(
        !a.same_write(&fenced(1, 1, vec![record(b"x", None)])),
        "another position"
    );
    assert!(
        !a.same_write(&fenced(1, 0, vec![record(b"y", None)])),
        "other bytes"
    );
    assert!(
        !a.same_write(&fenced(1, 0, vec![record(b"x", None), record(b"x", None)])),
        "more records"
    );
    let mut header = record(b"x", None);
    header.headers.client[0].value = b"other".to_vec();
    assert!(
        !a.same_write(&fenced(1, 0, vec![header])),
        "another client header"
    );
}

#[test]
fn a_write_counts_its_records() {
    assert_eq!(fenced(1, 0, vec![]).count(), 0);
    assert_eq!(fenced(1, 0, vec![Record::new(vec![1]); 3]).count(), 3);
}

#[test]
fn a_new_leader_is_multi_or_a_set_uuid() {
    assert_eq!(Leader::from_new(None), Leader::Multi);
    assert_eq!(
        Leader::from_new(Some(LeaderUuid(7))),
        Leader::Uuid(LeaderUuid(7))
    );
    assert!(Leader::Unset.is_valid() && Leader::Multi.is_valid());
    assert!(!Leader::Uuid(LeaderUuid::UNSET).is_valid());
}

#[test]
#[should_panic(expected = "the unset uuid never leads")]
fn the_unset_uuid_never_leads() {
    let _ = Leader::uuid(LeaderUuid::UNSET);
}

#[test]
fn the_fingerprint_tells_two_commands_apart() {
    // The Phase-2 tally of the core counts a vote by its fingerprint: two
    // commands with one fingerprint would count as one value.
    use paros_consensus::types::Fingerprint;
    let truncate = |up_to| Command::Truncate {
        fence: None,
        up_to: Seq(up_to),
    };
    assert_eq!(truncate(1).fingerprint(), truncate(1).fingerprint());
    assert_ne!(truncate(1).fingerprint(), truncate(2).fingerprint());
    assert_ne!(Command::Noop.fingerprint(), truncate(1).fingerprint());
}

#[test]
fn a_stamp_gives_every_record_of_a_write_one_time() {
    let write = Command::Write(fenced(
        1,
        0,
        vec![record(b"a", Some(5)), record(b"b", None)],
    ));
    let Command::Write(stamped) = write.clone().stamped(9) else {
        panic!("a write stays a write");
    };
    assert!(
        stamped
            .records
            .iter()
            .all(|r| r.headers.leader_time == Some(9))
    );
    let Command::Write(given) = write else {
        unreachable!()
    };
    assert!(
        stamped.same_write(&given),
        "a stamp keeps the write's identity"
    );
    assert_eq!(Command::Noop.stamped(9), Command::Noop);
}

/// A write weighs its records' payload bytes, the measure the core's
/// retained bytes sum; every other command weighs nothing.
#[test]
fn a_write_weighs_its_payload_bytes_and_a_control_command_nothing() {
    use paros_consensus::types::Weigh;
    let write = Command::Write(fenced(
        1,
        0,
        vec![record(b"abc", None), record(b"de", Some(4))],
    ));
    assert_eq!(write.weight(), 5, "the headers do not count");
    assert_eq!(Command::Noop.weight(), 0);
    assert_eq!(Command::Write(fenced(1, 0, Vec::new())).weight(), 0);
}
