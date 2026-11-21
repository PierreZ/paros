use super::*;
use crate::codec::{HEADER_LEN, RELEASE, Release, encode_command};
use crate::command::{Command, Fence, Header, Headers, Leader, Record, Write};
use paros_consensus::journal::LeaderUuid;

/// A small seeded generator (splitmix64): the crate has no dependency for
/// randomness, and a seed names a run.
struct Draw(u64);

impl Draw {
    fn u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.u64() % n
    }

    /// A number that is often at an edge (0, 1, the largest) and else any.
    fn edge(&mut self) -> u64 {
        match self.below(4) {
            0 => 0,
            1 => 1,
            2 => u64::MAX,
            _ => self.u64(),
        }
    }

    fn ballot(&mut self) -> Ballot {
        Ballot {
            round: self.edge(),
            node: NodeId(self.edge()),
        }
    }

    fn uuid(&mut self) -> LeaderUuid {
        LeaderUuid(u128::from(self.u64()) << 64 | u128::from(self.u64()) | 1)
    }

    fn leader(&mut self) -> Leader {
        match self.below(3) {
            0 => Leader::Unset,
            1 => Leader::Multi,
            _ => Leader::uuid(self.uuid()),
        }
    }

    fn bytes(&mut self) -> Vec<u8> {
        let len = self.below(6);
        (0..len)
            .map(|_| u8::try_from(self.below(256)).expect("a byte"))
            .collect()
    }

    fn record(&mut self) -> Record {
        let client = (0..self.below(3))
            .map(|_| Header {
                key: self.bytes(),
                value: self.bytes(),
            })
            .collect();
        let leader_time = (self.below(2) == 0).then(|| self.edge());
        Record {
            headers: Headers {
                leader_time,
                client,
            },
            payload: self.bytes(),
        }
    }

    fn command(&mut self) -> Command {
        match self.below(5) {
            0 => Command::Noop,
            1 => Command::Write(Write {
                fence: if self.below(2) == 0 {
                    Fence::Open
                } else {
                    Fence::Leader {
                        uuid: self.uuid(),
                        expected_seq: Seq(self.edge()),
                    }
                },
                records: (0..self.below(3)).map(|_| self.record()).collect(),
            }),
            2 => Command::Truncate {
                fence: (self.below(2) == 0).then(|| self.uuid()),
                up_to: Seq(self.edge()),
            },
            3 => Command::SetLeader {
                new: (self.below(2) == 0).then(|| self.uuid()),
                old: self.leader(),
            },
            _ => Command::Upgrade {
                release: Release(u16::try_from(self.below(4)).expect("small")),
            },
        }
    }

    fn view(&mut self) -> JournalView {
        let a = self.edge();
        let b = self.edge();
        JournalView {
            leader: self.leader(),
            next_seq: Seq(a.max(b)),
            first_seq: Seq(a.min(b)),
        }
    }

    fn answer(&mut self) -> Answer {
        match self.below(14) {
            0 => Answer::Written {
                seq: Seq(self.edge()),
                count: self.edge(),
            },
            1 => Answer::Duplicate {
                seq: Seq(self.edge()),
                count: self.edge(),
            },
            2 => Answer::Taken(self.view()),
            3 => Answer::Fenced(self.view()),
            4 => Answer::Early(self.view()),
            5 => Answer::Truncated(self.view()),
            6 => Answer::Below(self.view()),
            7 => Answer::WrongMode(self.view()),
            8 => Answer::Empty(self.view()),
            9 => Answer::LeaderSet(self.view()),
            10 => Answer::LeaderRefused(self.view()),
            11 => Answer::Trimmed(self.view()),
            12 => Answer::Reserved(self.view()),
            _ => Answer::Noop,
        }
    }

    fn page(&mut self) -> PromisePage {
        PromisePage {
            votes: (0..self.below(4))
                .map(|_| (Slot(self.edge()), (self.ballot(), self.command())))
                .collect(),
            faulty: (0..self.below(3))
                .map(|_| (Slot(self.edge()), self.ballot()))
                .collect(),
            next: (self.below(2) == 0).then(|| Slot(self.edge())),
        }
    }

    fn message(&mut self) -> Message {
        match self.below(8) {
            0 => Message::Prepare {
                ballot: self.ballot(),
                from: Slot(self.edge()),
            },
            1 => Message::Promise {
                ballot: self.ballot(),
                from: Slot(self.edge()),
                page: self.page(),
            },
            2 => Message::Nack {
                ballot: self.ballot(),
                promised: self.ballot(),
            },
            3 => Message::Accept {
                ballot: self.ballot(),
                slot: Slot(self.edge()),
                command: self.command(),
            },
            4 => Message::Accepted {
                ballot: self.ballot(),
                slot: Slot(self.edge()),
            },
            5 => Message::Commit {
                segment: SegmentId(self.edge()),
                slot: Slot(self.edge()),
                command: self.command(),
            },
            6 => Message::Applied {
                segment: SegmentId(self.edge()),
                slot: Slot(self.edge()),
                command: self.command(),
                answer: self.answer(),
            },
            _ => Message::Pull {
                segment: SegmentId(self.edge()),
                from: Slot(self.edge()),
            },
        }
    }
}

/// The variant of a message, as its tag.
fn tag(message: &Message) -> usize {
    match message {
        Message::Prepare { .. } => 0,
        Message::Promise { .. } => 1,
        Message::Nack { .. } => 2,
        Message::Accept { .. } => 3,
        Message::Accepted { .. } => 4,
        Message::Commit { .. } => 5,
        Message::Applied { .. } => 6,
        Message::Pull { .. } => 7,
    }
}

const RUNS: u64 = 4_000;

#[test]
fn drawn_messages_round_trip() {
    let mut draw = Draw(1);
    let mut variants = [0usize; 8];
    let mut answers = std::collections::BTreeSet::new();
    for _ in 0..RUNS {
        let message = draw.message();
        variants[tag(&message)] += 1;
        if let Message::Applied { answer, .. } = &message {
            let name = format!("{answer:?}");
            let end = name.find([' ', '(']).unwrap_or(name.len());
            answers.insert(name[..end].to_owned());
        }
        let bytes = encode_message(&message);
        assert_eq!(&bytes[..2], &RELEASE.0.to_le_bytes(), "the release leads");
        assert_eq!(bytes[2], MESSAGE, "the kind follows");
        assert_eq!(decode_message(&bytes), Ok(message));
    }
    assert!(
        variants.iter().all(|&n| n > 0),
        "every variant: {variants:?}"
    );
    assert_eq!(answers.len(), 14, "every answer");
}

#[test]
fn every_cut_is_refused_and_extra_bytes_are_trailing() {
    let mut draw = Draw(2);
    for _ in 0..RUNS / 10 {
        let bytes = encode_message(&draw.message());
        for cut in 0..bytes.len() {
            assert!(decode_message(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(decode_message(&longer), Err(DecodeError::Trailing(1)));
    }
}

#[test]
fn garbled_bytes_decode_to_a_result_never_a_panic() {
    // Each flip of one byte gives a message or an error; a message that
    // decodes encodes to the same bytes (one encoding for each message).
    let mut draw = Draw(3);
    for _ in 0..RUNS / 4 {
        let mut bytes = encode_message(&draw.message());
        let at = usize::try_from(draw.below(bytes.len() as u64)).expect("an index");
        bytes[at] ^= u8::try_from(draw.below(255) + 1).expect("a byte");
        if let Ok(message) = decode_message(&bytes) {
            assert_eq!(encode_message(&message), bytes, "one encoding");
        }
    }
    // Bytes that no encoder wrote.
    for _ in 0..RUNS {
        let len = draw.below(64);
        let mut bytes: Vec<u8> = (0..len)
            .map(|_| u8::try_from(draw.below(256)).expect("a byte"))
            .collect();
        if bytes.len() >= HEADER_LEN {
            bytes[..2].copy_from_slice(&RELEASE.0.to_le_bytes());
            bytes[2] = MESSAGE;
        }
        if let Ok(message) = decode_message(&bytes) {
            assert_eq!(encode_message(&message), bytes, "one encoding");
        }
    }
}

#[test]
fn another_kind_and_an_unknown_release_are_refused() {
    assert_eq!(
        decode_message(&encode_command(&Command::Noop)),
        Err(DecodeError::Kind {
            expected: MESSAGE,
            found: super::super::COMMAND
        })
    );
    let mut bytes = encode_message(&Message::Pull {
        segment: SegmentId(1),
        from: Slot(0),
    });
    bytes[..2].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_message(&bytes),
        Err(DecodeError::UnknownRelease(Release(2)))
    );
}

#[test]
fn bad_tags_are_refused() {
    let mut bytes = encode_message(&Message::Pull {
        segment: SegmentId(1),
        from: Slot(0),
    });
    bytes[HEADER_LEN] = 8;
    assert_eq!(
        decode_message(&bytes),
        Err(DecodeError::Tag {
            what: "message",
            tag: 8
        })
    );
    let applied = Message::Applied {
        segment: SegmentId(1),
        slot: Slot(2),
        command: Command::Noop,
        answer: Answer::Noop,
    };
    let mut bytes = encode_message(&applied);
    let last = bytes.len() - 1;
    assert_eq!(bytes[last], 13, "the answer tag ends a Noop answer");
    bytes[last] = 14;
    assert_eq!(
        decode_message(&bytes),
        Err(DecodeError::Tag {
            what: "answer",
            tag: 14
        })
    );
}

/// The bytes of a promise with two votes at `first` and `second`.
fn promise_with_votes(first: u64, second: u64) -> Vec<u8> {
    // Encode with increasing slots 1 and 2, then write the slots the test
    // asks for at their places.
    let page = PromisePage {
        votes: [
            (Slot(1), (Ballot::zero(), Command::Noop)),
            (Slot(2), (Ballot::zero(), Command::Noop)),
        ]
        .into_iter()
        .collect(),
        faulty: BTreeMap::new(),
        next: None,
    };
    let mut bytes = encode_message(&Message::Promise {
        ballot: Ballot::zero(),
        from: Slot(0),
        page,
    });
    // header, tag, ballot (16), from (8), count (4), then the first vote.
    let first_at = HEADER_LEN + 1 + 16 + 8 + 4;
    // A vote is a slot (8), a ballot (16) and a Noop (1).
    let second_at = first_at + 8 + 16 + 1;
    bytes[first_at..first_at + 8].copy_from_slice(&first.to_le_bytes());
    bytes[second_at..second_at + 8].copy_from_slice(&second.to_le_bytes());
    bytes
}

#[test]
fn a_page_whose_slots_do_not_increase_is_refused() {
    assert!(decode_message(&promise_with_votes(1, 2)).is_ok());
    for (first, second) in [(2, 2), (3, 2)] {
        assert_eq!(
            decode_message(&promise_with_votes(first, second)),
            Err(DecodeError::Invalid(
                "the slots of a promise page are not increasing"
            )),
            "slots {first} then {second}"
        );
    }
}

#[test]
fn a_view_with_its_floor_above_its_tail_is_refused() {
    let view = JournalView {
        leader: Leader::Unset,
        next_seq: Seq(4),
        first_seq: Seq(4),
    };
    let mut bytes = encode_message(&Message::Applied {
        segment: SegmentId(1),
        slot: Slot(0),
        command: Command::Noop,
        answer: Answer::Fenced(view),
    });
    assert!(
        decode_message(&bytes).is_ok(),
        "a floor at the tail is a view"
    );
    let at = bytes.len() - 8;
    bytes[at..].copy_from_slice(&5u64.to_le_bytes());
    assert_eq!(
        decode_message(&bytes),
        Err(DecodeError::Invalid("a view with first_seq above next_seq"))
    );
}

#[test]
fn a_huge_count_is_refused_before_it_is_read() {
    let mut bytes = encode_message(&Message::Promise {
        ballot: Ballot::zero(),
        from: Slot(0),
        page: PromisePage {
            votes: BTreeMap::new(),
            faulty: BTreeMap::new(),
            next: None,
        },
    });
    let at = HEADER_LEN + 1 + 16 + 8;
    bytes[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode_message(&bytes), Err(DecodeError::Short));
}
