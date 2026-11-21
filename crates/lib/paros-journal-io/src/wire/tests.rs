use super::*;
use paros_journal::roles::PromisePage;
use paros_journal::{Command, Slot};

fn envelope(message: Message) -> Envelope {
    Envelope {
        journal: JournalId(7),
        from: NodeId(1),
        to: NodeId(u64::MAX),
        message,
    }
}

fn messages() -> Vec<Message> {
    let ballot = paros_consensus::Ballot {
        round: 3,
        node: NodeId(100),
    };
    vec![
        Message::Prepare {
            ballot,
            from: Slot(0),
        },
        Message::Promise {
            ballot,
            from: Slot(0),
            page: PromisePage {
                votes: [(Slot(2), (ballot, Command::Noop))].into_iter().collect(),
                faulty: [(Slot(4), ballot)].into_iter().collect(),
                next: Some(Slot(5)),
            },
        },
        Message::Accepted {
            ballot,
            slot: Slot(9),
        },
    ]
}

#[test]
fn an_envelope_round_trips_and_the_release_leads() {
    for message in messages() {
        let envelope = envelope(message);
        let bytes = envelope.encode();
        assert_eq!(
            &bytes[..2],
            &paros_journal::RELEASE.0.to_le_bytes(),
            "the release leads"
        );
        assert_eq!(Envelope::decode(&bytes), Ok(envelope));
    }
}

#[test]
fn every_cut_and_extra_byte_is_refused() {
    for message in messages() {
        let bytes = envelope(message).encode();
        for cut in 0..bytes.len() {
            assert!(Envelope::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        // A byte more moves the route by one: never the same envelope.
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(Envelope::decode(&longer).is_err());
    }
}

#[test]
fn the_unset_journal_is_refused() {
    let mut bytes = envelope(messages().remove(0)).encode();
    let at = bytes.len() - ROUTE_LEN;
    bytes[at..at + 8].copy_from_slice(&0u64.to_le_bytes());
    assert_eq!(
        Envelope::decode(&bytes),
        Err(DecodeError::Invalid("an envelope of the unset journal"))
    );
}
