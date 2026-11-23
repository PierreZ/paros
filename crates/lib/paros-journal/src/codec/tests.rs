use super::*;

fn rich_record(byte: u8) -> Record {
    Record {
        headers: Headers {
            leader_time: Some(1_700_000_000_000),
            client: vec![
                Header {
                    key: b"trace-id".to_vec(),
                    value: vec![byte; 3],
                },
                Header {
                    key: Vec::new(),
                    value: Vec::new(),
                },
            ],
        },
        payload: vec![byte; 5],
    }
}

fn commands() -> Vec<Command> {
    vec![
        Command::Noop,
        Command::Write(Write {
            fence: Fence::Leader {
                uuid: LeaderUuid(u128::MAX),
                expected_seq: Seq(u64::MAX),
            },
            records: vec![rich_record(1), Record::new(Vec::new())],
        }),
        Command::Write(Write {
            fence: Fence::Open,
            records: Vec::new(),
        }),
        Command::Truncate {
            fence: Some(LeaderUuid(3)),
            up_to: Seq(9),
        },
        Command::Truncate {
            fence: None,
            up_to: Seq(0),
        },
        Command::SetLeader {
            new: Leader::multi(LeaderUuid(4)),
            old: Leader::uuid(LeaderUuid(5)),
        },
        Command::SetLeader {
            new: Leader::uuid(LeaderUuid(6)),
            old: Leader::Unset,
        },
        Command::SetLeader {
            new: Leader::uuid(LeaderUuid(6)),
            old: Leader::multi(LeaderUuid(u128::MAX)),
        },
        Command::Upgrade {
            release: Release(2),
        },
    ]
}

fn segment() -> SegmentRecord {
    let grid = Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]]).expect("grid");
    let mut record = SegmentRecord::open(
        SegmentId(3),
        RELEASE,
        OpeningCall {
            term: 7,
            expected: Some(SegmentId(2)),
            grid,
            replicas: vec![NodeId(9), NodeId(8)],
        },
    )
    .expect("open");
    record.put_in_force(Slot(4)).expect("in force");
    record.seal(Slot(6)).expect("seal");
    record
        .write_boundary(BoundaryState {
            next_seq: Seq(10),
            first_seq: Seq(2),
            leader: Leader::uuid(LeaderUuid(1)),
        })
        .expect("boundary");
    record
}

#[test]
fn every_command_round_trips() {
    for command in commands() {
        let bytes = encode_command(&command);
        assert_eq!(&bytes[..2], &RELEASE.0.to_le_bytes(), "the release leads");
        assert_eq!(decode_command(&bytes), Ok(command));
    }
}

#[test]
fn a_record_round_trips_with_its_headers() {
    for record in [rich_record(4), Record::new(vec![1, 2])] {
        assert_eq!(decode_record(&encode_record(&record)), Ok(record));
    }
}

#[test]
fn segment_records_and_boundaries_round_trip() {
    let mut record = segment();
    assert_eq!(decode_segment(&encode_segment(&record)), Ok(record.clone()));
    record.retire(2, false).expect("retire");
    record.trim(Seq(10)).expect("trim");
    assert_eq!(decode_segment(&encode_segment(&record)), Ok(record.clone()));
    let pending = SegmentRecord::open(SegmentId(1), RELEASE, record.opening().clone());
    assert!(
        pending.is_err(),
        "the expected segment is below the new one"
    );
    let boundary = BoundaryState::fresh(Leader::multi(LeaderUuid(8)));
    assert_eq!(decode_boundary(&encode_boundary(&boundary)), Ok(boundary));
}

#[test]
fn an_unknown_release_is_refused() {
    let mut bytes = encode_command(&Command::Noop);
    bytes[..2].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_command(&bytes),
        Err(DecodeError::UnknownRelease(Release(2)))
    );
    // The release of a segment record is checked too.
    let mut bytes = encode_segment(&segment());
    let at = HEADER_LEN + size_of::<u64>();
    bytes[at..at + 2].copy_from_slice(&9u16.to_le_bytes());
    assert_eq!(
        decode_segment(&bytes),
        Err(DecodeError::UnknownRelease(Release(9)))
    );
}

#[test]
fn another_kind_is_refused() {
    let bytes = encode_record(&Record::new(vec![1]));
    assert_eq!(
        decode_command(&bytes),
        Err(DecodeError::Kind {
            expected: COMMAND,
            found: RECORD
        })
    );
}

#[test]
fn every_cut_is_short_and_extra_bytes_are_trailing() {
    for command in commands() {
        let bytes = encode_command(&command);
        for cut in 0..bytes.len() {
            assert!(
                decode_command(&bytes[..cut]).is_err(),
                "{command:?} cut at {cut}"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(decode_command(&longer), Err(DecodeError::Trailing(1)));
    }
    let bytes = encode_segment(&segment());
    for cut in 0..bytes.len() {
        assert!(
            decode_segment(&bytes[..cut]).is_err(),
            "segment cut at {cut}"
        );
    }
}

#[test]
fn a_huge_count_is_refused_before_it_is_read() {
    // A write of u32::MAX records in a few bytes.
    let mut bytes = encode_command(&Command::Write(Write {
        fence: Fence::Open,
        records: Vec::new(),
    }));
    let at = bytes.len() - 4;
    bytes[at..].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode_command(&bytes), Err(DecodeError::Short));
}

#[test]
fn an_unset_uuid_is_refused() {
    let mut bytes = encode_command(&Command::Truncate {
        fence: Some(LeaderUuid(1)),
        up_to: Seq(0),
    });
    // header, tag 2, option 1, then the uuid.
    let at = HEADER_LEN + 2;
    bytes[at..at + 16].copy_from_slice(&0u128.to_le_bytes());
    assert!(matches!(
        decode_command(&bytes),
        Err(DecodeError::Invalid(_))
    ));
}

#[test]
fn an_unknown_paros_key_is_refused() {
    let mut bytes = encode_record(&Record {
        headers: Headers {
            leader_time: Some(3),
            client: Vec::new(),
        },
        payload: Vec::new(),
    });
    // header, the count of paros headers, then the key.
    let at = HEADER_LEN + 2;
    bytes[at..at + 2].copy_from_slice(&7u16.to_le_bytes());
    assert_eq!(decode_record(&bytes), Err(DecodeError::ParosKey(7)));
}

#[test]
fn a_bad_tag_is_refused() {
    let mut bytes = encode_command(&Command::Noop);
    bytes[HEADER_LEN] = 9;
    assert_eq!(
        decode_command(&bytes),
        Err(DecodeError::Tag {
            what: "command",
            tag: 9
        })
    );
}

#[test]
fn a_boundary_with_its_floor_above_its_tail_is_refused() {
    let mut bytes = encode_boundary(&BoundaryState::fresh(Leader::Unset));
    bytes[HEADER_LEN + 8..HEADER_LEN + 16].copy_from_slice(&5u64.to_le_bytes());
    assert!(matches!(
        decode_boundary(&bytes),
        Err(DecodeError::Invalid(_))
    ));
}
