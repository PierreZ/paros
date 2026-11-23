use super::*;

const A: LeaderUuid = LeaderUuid(0xA);
const B: LeaderUuid = LeaderUuid(0xB);
/// A multi-writer mode, with its tag.
const M: Leader = Leader::Multi(LeaderUuid(0x3A));

fn records(text: &str) -> Vec<Record> {
    vec![Record::new(text.as_bytes().to_vec())]
}

fn view(leader: Leader, next: u64, first: u64) -> JournalView {
    JournalView {
        leader,
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

fn fenced(uuid: LeaderUuid, at: u64, text: &str) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid,
            expected_seq: Seq(at),
        },
        records: records(text),
    })
}

fn open(text: &str) -> Command {
    Command::Write(Write {
        fence: Fence::Open,
        records: records(text),
    })
}

fn reply(answer: Answer) -> Response {
    Response::Reply(Reply::Answer(answer))
}

/// The answer of a replica of the segment in force to a floor request: an
/// empty page at the tail, with the floor `first`.
fn floor_page(next: u64, first: u64) -> Response {
    Response::Read(ReadAnswer::Page {
        records: Vec::new(),
        view: view(M, next, first),
        sealed: false,
    })
}

/// The one request out, which must be a floor request: its id.
fn one_floor(client: &mut Client) -> RequestId {
    let requests = client.take_requests();
    match requests.as_slice() {
        [Request::Floor { id }] => *id,
        other => panic!("not one floor request: {other:?}"),
    }
}

/// The one request out, which must be a command: its id and command.
fn one_command(client: &mut Client) -> (RequestId, Command) {
    let requests = client.take_requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    match requests.into_iter().next() {
        Some(Request::Command { id, command }) => (id, command),
        other => panic!("not a command: {other:?}"),
    }
}

fn one_outcome(client: &mut Client) -> (CallId, Outcome) {
    let outcomes = client.take_outcomes();
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    outcomes.into_iter().next().expect("an outcome")
}

/// A client that holds the journal under `uuid`, its next write at `next`.
fn leader(uuid: LeaderUuid, next: u64) -> Client {
    let mut client = Client::new();
    let call = client.set_leader(Leader::uuid(uuid), Leader::Unset);
    let (id, command) = one_command(&mut client);
    assert_eq!(
        command,
        Command::SetLeader {
            new: Leader::uuid(uuid),
            old: Leader::Unset
        }
    );
    client.answer(
        id,
        reply(Answer::LeaderSet(view(Leader::Uuid(uuid), next, 0))),
    );
    assert_eq!(
        one_outcome(&mut client),
        (call, Outcome::LeaderSet(view(Leader::Uuid(uuid), next, 0)))
    );
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid,
            next: Seq(next)
        }
    );
    assert!(client.idle());
    client
}

#[test]
fn a_new_client_writes_unfenced() {
    let mut client = Client::default();
    assert_eq!(client.mode(), Mode::Open);
    assert!(client.idle());
    let call = client.write(records("x"));
    assert!(!client.idle());
    let (id, command) = one_command(&mut client);
    assert_eq!(command, open("x"));
    client.answer(
        id,
        reply(Answer::Written {
            seq: Seq(4),
            count: 1,
        }),
    );
    assert_eq!(
        one_outcome(&mut client),
        (
            call,
            Outcome::Written {
                seq: Seq(4),
                count: 1
            }
        )
    );
    assert_eq!(client.mode(), Mode::Open, "an open write moves no position");
    assert!(client.idle());
}

#[test]
fn a_leader_writes_at_its_next_position_and_moves_on() {
    let mut client = leader(A, 3);
    let first = client.write(vec![Record::new(b"a".to_vec()), Record::new(b"b".to_vec())]);
    let second = client.write(records("c"));
    let (id, command) = one_command(&mut client);
    let Command::Write(write) = &command else {
        panic!("a write")
    };
    assert_eq!(
        write.fence,
        Fence::Leader {
            uuid: A,
            expected_seq: Seq(3)
        }
    );
    assert_eq!(write.count(), 2);
    assert_eq!(client.take_requests(), [], "one call in flight");
    client.answer(
        id,
        reply(Answer::Written {
            seq: Seq(3),
            count: 2,
        }),
    );
    assert_eq!(
        one_outcome(&mut client),
        (
            first,
            Outcome::Written {
                seq: Seq(3),
                count: 2
            }
        )
    );
    let (id, command) = one_command(&mut client);
    assert_eq!(command, fenced(A, 5, "c"), "the next write after the batch");
    client.answer(
        id,
        reply(Answer::Written {
            seq: Seq(5),
            count: 1,
        }),
    );
    assert_eq!(
        one_outcome(&mut client),
        (
            second,
            Outcome::Written {
                seq: Seq(5),
                count: 1
            }
        )
    );
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(6)
        }
    );
}

#[test]
fn an_unknown_fate_sends_the_identical_write_again() {
    for lost in [
        Response::Reply(Reply::Unknown),
        Response::NotLeader,
        Response::Lost,
        reply(Answer::Early(view(Leader::Uuid(A), 0, 0))),
    ] {
        let mut client = leader(A, 0);
        let call = client.write(records("a"));
        let (first, command) = one_command(&mut client);
        client.answer(first, lost.clone());
        assert_eq!(client.take_outcomes(), [], "{lost:?} ends nothing");
        let (second, again) = one_command(&mut client);
        assert_eq!(again, command, "the same write after {lost:?}");
        assert_ne!(second, first, "a new copy has a new id");
        // The first copy's late answer is ignored.
        client.answer(
            first,
            reply(Answer::Written {
                seq: Seq(0),
                count: 1,
            }),
        );
        assert_eq!(client.take_outcomes(), []);
        assert_eq!(client.take_requests(), []);
        client.answer(
            second,
            reply(Answer::Duplicate {
                seq: Seq(0),
                count: 1,
            }),
        );
        assert_eq!(
            one_outcome(&mut client),
            (
                call,
                Outcome::Written {
                    seq: Seq(0),
                    count: 1
                }
            )
        );
        assert_eq!(
            client.mode(),
            Mode::Fenced {
                uuid: A,
                next: Seq(1)
            }
        );
    }
}

#[test]
fn an_open_write_is_sent_again_at_least_once() {
    let mut client = Client::new();
    client.write(records("x"));
    let (first, command) = one_command(&mut client);
    client.answer(first, Response::Reply(Reply::Unknown));
    let (_, again) = one_command(&mut client);
    assert_eq!(again, command);
}

#[test]
fn below_is_resolved_written_at_the_old_segment() {
    let mut client = leader(A, 7);
    let call = client.write(records("a"));
    let (id, _) = one_command(&mut client);
    client.answer(id, reply(Answer::Below(view(Leader::Uuid(A), 9, 0))));
    let requests = client.take_requests();
    let [
        Request::Resolve {
            id,
            uuid,
            expected_seq,
            records: sent,
        },
    ] = requests.as_slice()
    else {
        panic!("a Resolve: {requests:?}")
    };
    assert_eq!((*uuid, *expected_seq), (A, Seq(7)));
    assert_eq!(*sent, records("a"));
    assert!(!client.idle());
    client.answer(
        *id,
        Response::Resolved(ResolveAnswer::Written {
            seq: Seq(7),
            count: 1,
        }),
    );
    assert_eq!(
        one_outcome(&mut client),
        (
            call,
            Outcome::Written {
                seq: Seq(7),
                count: 1
            }
        )
    );
    // The write ends at 8, in the sealed segment; the tail that the `Below`
    // named is 9, and the client never goes back below it.
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(9)
        }
    );
}

#[test]
fn below_not_written_writes_again_at_the_tail() {
    let mut client = leader(A, 7);
    let call = client.write(records("a"));
    let next = client.write(records("b"));
    let (id, _) = one_command(&mut client);
    client.answer(id, reply(Answer::Below(view(Leader::Uuid(A), 9, 0))));
    let resolve = client.take_requests()[0].id();
    // A replica that does not cover the position: ask again.
    for again in [
        Response::Resolved(ResolveAnswer::NotCovered),
        Response::Lost,
        Response::NotLeader,
    ] {
        let before = client.take_requests();
        assert_eq!(before, []);
        let current = client.flight.as_ref().expect("in flight").request;
        client.answer(current, again);
        let requests = client.take_requests();
        assert!(
            matches!(
                requests.as_slice(),
                [Request::Resolve {
                    expected_seq: Seq(7),
                    ..
                }]
            ),
            "{requests:?}"
        );
        client.flight = client.flight.take().map(|f| Flight {
            request: requests[0].id(),
            ..f
        });
    }
    assert_eq!(client.take_outcomes(), []);
    assert_ne!(resolve, client.flight.as_ref().expect("in flight").request);
    let current = client.flight.as_ref().expect("in flight").request;
    client.answer(current, Response::Resolved(ResolveAnswer::NotWritten));
    let (id, command) = one_command(&mut client);
    assert_eq!(command, fenced(A, 9, "a"), "the same records at the tail");
    client.answer(
        id,
        reply(Answer::Written {
            seq: Seq(9),
            count: 1,
        }),
    );
    assert_eq!(
        one_outcome(&mut client),
        (
            call,
            Outcome::Written {
                seq: Seq(9),
                count: 1
            }
        )
    );
    let (id, command) = one_command(&mut client);
    assert_eq!(command, fenced(A, 10, "b"));
    client.answer(id, reply(Answer::Fenced(view(Leader::Uuid(B), 12, 0))));
    assert_eq!(
        one_outcome(&mut client),
        (
            next,
            Outcome::Refused(Answer::Fenced(view(Leader::Uuid(B), 12, 0)))
        )
    );
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(10)
        },
        "a view of another leader moves nothing"
    );
}

#[test]
fn a_resolve_below_the_floor_is_truncated() {
    let mut client = leader(A, 7);
    let call = client.write(records("a"));
    let (id, _) = one_command(&mut client);
    let below = view(Leader::Uuid(A), 9, 8);
    client.answer(id, reply(Answer::Below(below)));
    let resolve = client.take_requests()[0].id();
    client.answer(resolve, Response::Resolved(ResolveAnswer::Truncated));
    assert_eq!(one_outcome(&mut client), (call, Outcome::Truncated(below)));
    assert!(client.idle());
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(9)
        }
    );
}

#[test]
fn truncated_resolves_nothing_and_follows_the_tail() {
    let mut client = leader(A, 2);
    let call = client.write(records("a"));
    let (id, _) = one_command(&mut client);
    let at = view(Leader::Uuid(A), 5, 4);
    client.answer(id, reply(Answer::Truncated(at)));
    assert_eq!(one_outcome(&mut client), (call, Outcome::Truncated(at)));
    assert_eq!(client.take_requests(), []);
    // The next write is at the tail, not below the floor again.
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(5)
        }
    );
}

#[test]
fn a_final_refusal_follows_the_view_of_its_own_leader() {
    for refusal in [
        Answer::Taken as fn(JournalView) -> Answer,
        Answer::Fenced,
        Answer::WrongMode,
        Answer::Empty,
    ] {
        let mut client = leader(A, 2);
        let call = client.write(records("a"));
        let (id, _) = one_command(&mut client);
        let answer = refusal(view(Leader::Uuid(A), 6, 0));
        client.answer(id, reply(answer.clone()));
        assert_eq!(one_outcome(&mut client), (call, Outcome::Refused(answer)));
        assert_eq!(
            client.mode(),
            Mode::Fenced {
                uuid: A,
                next: Seq(6)
            }
        );
    }
}

#[test]
fn an_answer_that_does_not_fit_a_write_refuses_it() {
    let mut client = leader(A, 2);
    let call = client.write(records("a"));
    let (id, _) = one_command(&mut client);
    client.answer(id, reply(Answer::Noop));
    assert_eq!(
        one_outcome(&mut client),
        (call, Outcome::Refused(Answer::Noop))
    );
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(2)
        }
    );
}

#[test]
fn a_duplicate_of_another_uuid_moves_no_position() {
    let mut client = leader(A, 2);
    // The client claims B while a write under A is answered: the next
    // write of A is not B's position.
    client.mode = Mode::Fenced {
        uuid: B,
        next: Seq(4),
    };
    client.flight = Some(Flight {
        call: CallId(0),
        request: RequestId(0),
        stage: Stage::Sent {
            command: fenced(A, 2, "a"),
            copies: 1,
        },
    });
    client.answer(
        RequestId(0),
        reply(Answer::Written {
            seq: Seq(2),
            count: 1,
        }),
    );
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: B,
            next: Seq(4)
        }
    );
}

#[test]
fn a_truncate_is_fenced_by_the_leader_and_sent_again_when_lost() {
    let mut client = leader(A, 5);
    let call = client.truncate(Seq(3));
    let (id, command) = one_command(&mut client);
    assert_eq!(
        command,
        Command::Truncate {
            fence: Some(A),
            up_to: Seq(3)
        }
    );
    client.answer(id, Response::Lost);
    let (id, again) = one_command(&mut client);
    assert_eq!(again, command);
    let after = view(Leader::Uuid(A), 5, 3);
    client.answer(id, reply(Answer::Trimmed(after)));
    assert_eq!(one_outcome(&mut client), (call, Outcome::Trimmed(after)));

    let call = client.truncate(Seq(4));
    let (id, _) = one_command(&mut client);
    let fence = Answer::Fenced(view(Leader::Uuid(B), 5, 3));
    client.answer(id, reply(fence.clone()));
    assert_eq!(one_outcome(&mut client), (call, Outcome::Refused(fence)));
}

#[test]
fn an_open_client_truncates_unfenced() {
    let mut client = Client::new();
    client.truncate(Seq(1));
    let (_, command) = one_command(&mut client);
    assert_eq!(
        command,
        Command::Truncate {
            fence: None,
            up_to: Seq(1)
        }
    );
}

#[test]
fn set_leader_none_makes_the_client_write_open() {
    let mut client = leader(A, 2);
    let call = client.set_leader(M, Leader::Uuid(A));
    let (id, command) = one_command(&mut client);
    assert_eq!(
        command,
        Command::SetLeader {
            new: M,
            old: Leader::Uuid(A)
        }
    );
    client.answer(id, reply(Answer::LeaderSet(view(M, 2, 0))));
    assert_eq!(
        one_outcome(&mut client),
        (call, Outcome::LeaderSet(view(M, 2, 0)))
    );
    assert_eq!(client.mode(), Mode::Open);
}

#[test]
fn a_lost_set_leader_that_won_settles_itself() {
    let mut client = Client::new();
    let call = client.set_leader(Leader::uuid(A), Leader::Unset);
    let (id, command) = one_command(&mut client);
    client.answer(id, Response::Lost);
    let (id, again) = one_command(&mut client);
    assert_eq!(again, command);
    let won = view(Leader::Uuid(A), 4, 0);
    client.answer(id, reply(Answer::LeaderRefused(won)));
    assert_eq!(one_outcome(&mut client), (call, Outcome::LeaderSet(won)));
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(4)
        }
    );
}

#[test]
fn a_lost_set_leader_of_a_mode_settles_itself() {
    // The tag of the mode is the caller's own and fresh: a refused resend
    // whose view names it means that the first copy won.
    let mut client = leader(A, 2);
    let call = client.set_leader(M, Leader::Uuid(A));
    let (id, _) = one_command(&mut client);
    client.answer(id, Response::Lost);
    let (id, _) = one_command(&mut client);
    let multi = view(M, 4, 0);
    client.answer(id, reply(Answer::LeaderRefused(multi)));
    assert_eq!(one_outcome(&mut client), (call, Outcome::LeaderSet(multi)));
    assert_eq!(client.mode(), Mode::Open);
}

#[test]
fn a_lost_set_leader_refused_under_another_mode_stays_refused() {
    // Another caller made the journal multi-writer, with its own tag: the
    // resend proves nothing.
    let mut client = leader(A, 2);
    let call = client.set_leader(M, Leader::Uuid(A));
    let (id, _) = one_command(&mut client);
    client.answer(id, Response::Lost);
    let (id, _) = one_command(&mut client);
    let other = view(Leader::multi(LeaderUuid(0x3B)), 4, 0);
    client.answer(id, reply(Answer::LeaderRefused(other)));
    assert_eq!(
        one_outcome(&mut client),
        (call, Outcome::LeaderRefused(other))
    );
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(2)
        }
    );
}

#[test]
fn a_first_copy_refused_is_refused() {
    for new in [Leader::uuid(A), M] {
        let mut client = Client::new();
        let call = client.set_leader(new, Leader::Unset);
        let (id, _) = one_command(&mut client);
        // The journal names `new` on a first copy: the client claims no win
        // that no copy of its own can have made.
        let refused = view(new, 4, 0);
        client.answer(id, reply(Answer::LeaderRefused(refused)));
        assert_eq!(
            one_outcome(&mut client),
            (call, Outcome::LeaderRefused(refused))
        );
        assert_eq!(client.mode(), Mode::Open);
    }
}

#[test]
fn a_resend_refused_under_another_leader_is_refused() {
    let mut client = Client::new();
    let call = client.set_leader(Leader::uuid(A), Leader::Unset);
    let (id, _) = one_command(&mut client);
    client.answer(id, Response::Reply(Reply::Unknown));
    let (id, _) = one_command(&mut client);
    let other = view(Leader::Uuid(B), 4, 0);
    client.answer(id, reply(Answer::LeaderRefused(other)));
    assert_eq!(
        one_outcome(&mut client),
        (call, Outcome::LeaderRefused(other))
    );
    assert_eq!(client.mode(), Mode::Open);
}

#[test]
fn a_set_leader_answered_out_of_shape_is_refused() {
    let mut client = Client::new();
    let call = client.set_leader(Leader::uuid(A), Leader::Unset);
    let (id, _) = one_command(&mut client);
    client.answer(id, reply(Answer::Noop));
    assert_eq!(
        one_outcome(&mut client),
        (call, Outcome::Refused(Answer::Noop))
    );
}

#[test]
fn reads_leave_at_once_and_ask_again_when_unserved() {
    let mut client = leader(A, 2);
    let write = client.write(records("a"));
    let read = client.read(Seq(1), 10);
    let requests = client.take_requests();
    assert_eq!(requests.len(), 2, "the read does not wait: {requests:?}");
    let Request::Floor { id } = requests[1].clone() else {
        panic!("a floor request first")
    };
    client.answer(id, floor_page(2, 1));
    let Request::Read { id, from, limit } = client.take_requests()[0].clone() else {
        panic!("a read")
    };
    assert_eq!((from, limit), (Seq(1), 10));
    let mut current = id;
    for again in [
        Response::Read(ReadAnswer::Unserved),
        Response::Lost,
        Response::NotLeader,
    ] {
        client.answer(current, again);
        let requests = client.take_requests();
        let [
            Request::Read {
                id,
                from: Seq(1),
                limit: 10,
            },
        ] = requests.as_slice()
        else {
            panic!("the read again: {requests:?}")
        };
        assert_ne!(*id, current);
        current = *id;
    }
    let page = ReadAnswer::Page {
        records: records("r")
            .into_iter()
            .zip(1..)
            .map(|(record, at)| (Seq(at), record))
            .collect(),
        view: view(Leader::Uuid(A), 2, 0),
        sealed: false,
    };
    client.answer(current, Response::Read(page));
    assert_eq!(
        one_outcome(&mut client),
        (
            read,
            Outcome::Page {
                from: Seq(1),
                records: records("r"),
                view: view(Leader::Uuid(A), 2, 0),
                sealed: false,
            }
        )
    );
    assert!(!client.idle(), "the write is still in flight");
    client.answer(
        requests[0].id(),
        reply(Answer::Written {
            seq: Seq(2),
            count: 1,
        }),
    );
    assert_eq!(one_outcome(&mut client).0, write);
    assert!(client.idle());
}

#[test]
fn a_read_below_the_floor_is_truncated() {
    let mut client = Client::new();
    let read = client.read(Seq(3), 1);
    let id = one_floor(&mut client);
    client.answer(id, floor_page(9, 3));
    let Request::Read { id, .. } = client.take_requests()[0].clone() else {
        panic!("a read at the floor leaves")
    };
    let floor = view(M, 9, 4);
    client.answer(id, Response::Read(ReadAnswer::Truncated(floor)));
    assert_eq!(one_outcome(&mut client), (read, Outcome::Truncated(floor)));
    assert!(client.idle());
}

#[test]
fn a_read_below_the_floor_of_the_segment_in_force_never_leaves() {
    // The holder of position 1 is a replica of a sealed segment that still
    // has it: only the floor of the segment in force says that it is gone.
    let mut client = Client::new();
    let read = client.read(Seq(1), 4);
    let id = one_floor(&mut client);
    client.answer(id, floor_page(9, 2));
    assert_eq!(client.take_requests(), [], "no read below the floor");
    assert_eq!(
        one_outcome(&mut client),
        (read, Outcome::Truncated(view(M, 9, 2)))
    );
    assert!(client.idle());
}

#[test]
fn a_floor_from_a_sealed_segment_or_none_is_asked_again() {
    let mut client = Client::new();
    let read = client.read(Seq(1), 4);
    let mut current = one_floor(&mut client);
    for again in [
        Response::Read(ReadAnswer::Page {
            records: Vec::new(),
            view: view(M, 5, 0),
            sealed: true,
        }),
        Response::Read(ReadAnswer::Truncated(view(M, 9, 1))),
        Response::Read(ReadAnswer::Below(view(M, 5, 0))),
        Response::Read(ReadAnswer::Superseded(crate::segment::SegmentId(2))),
        Response::Read(ReadAnswer::Unserved),
        Response::NotLeader,
        Response::Lost,
    ] {
        client.answer(current, again);
        let id = one_floor(&mut client);
        assert_ne!(id, current, "a new copy of the floor request");
        current = id;
    }
    client.answer(current, floor_page(9, 1));
    let Request::Read { id, from, limit } = client.take_requests()[0].clone() else {
        panic!("the read leaves at its floor")
    };
    assert_eq!((from, limit), (Seq(1), 4));
    assert_eq!(client.reads[&id].floor, Some(Seq(1)));
    client.answer(id, Response::Read(ReadAnswer::Truncated(view(M, 9, 2))));
    assert_eq!(one_outcome(&mut client).0, read);
}

#[test]
#[should_panic(expected = "a floor is answered by a read")]
fn a_reply_for_a_floor_panics() {
    let mut client = Client::new();
    client.read(Seq(0), 1);
    let id = one_floor(&mut client);
    client.answer(id, Response::Reply(Reply::Unknown));
}

#[test]
#[should_panic(expected = "a read leaves only at or above its floor")]
fn a_read_above_its_position_breaks_an_invariant() {
    let mut client = Client::new();
    client.read(Seq(0), 1);
    let id = one_floor(&mut client);
    client.reads.get_mut(&id).expect("the read").floor = Some(Seq(1));
    client.assert_invariants();
}

#[test]
fn an_unknown_request_is_ignored() {
    let mut client = leader(A, 0);
    client.answer(RequestId(99), Response::Lost);
    assert_eq!(client.take_requests(), []);
    assert_eq!(client.take_outcomes(), []);
    assert!(client.idle());
}

#[test]
#[should_panic(expected = "at least one record")]
fn a_read_of_nothing_panics() {
    let _ = Client::new().read(Seq(0), 0);
}

#[test]
#[should_panic(expected = "answered by a reply")]
fn a_page_for_a_write_panics() {
    let mut client = Client::new();
    client.write(records("a"));
    let id = client.take_requests()[0].id();
    client.answer(id, Response::Read(ReadAnswer::Unserved));
}

#[test]
#[should_panic(expected = "answered by a replica")]
fn a_reply_for_a_resolve_panics() {
    let mut client = leader(A, 0);
    client.write(records("a"));
    let id = client.take_requests()[0].id();
    client.answer(id, reply(Answer::Below(view(Leader::Uuid(A), 1, 0))));
    let id = client.take_requests()[0].id();
    client.answer(id, Response::Reply(Reply::Unknown));
}

#[test]
#[should_panic(expected = "answered by a page")]
fn a_reply_for_a_read_panics() {
    let mut client = Client::new();
    client.read(Seq(0), 1);
    let id = one_floor(&mut client);
    client.answer(id, floor_page(1, 0));
    let id = client.take_requests()[0].id();
    client.answer(id, Response::Lost);
    let id = client.take_requests()[0].id();
    client.answer(id, Response::Resolved(ResolveAnswer::NotCovered));
}

#[test]
fn request_ids_name_every_kind() {
    let id = RequestId(3);
    for request in [
        Request::Command {
            id,
            command: Command::Noop,
        },
        Request::Resolve {
            id,
            uuid: A,
            expected_seq: Seq(0),
            records: Vec::new(),
        },
        Request::Floor { id },
        Request::Read {
            id,
            from: Seq(0),
            limit: 1,
        },
    ] {
        assert_eq!(request.id(), id);
    }
}

#[test]
#[should_panic(expected = "a call waits only behind a call in flight")]
fn a_call_waiting_with_nothing_in_flight_breaks_an_invariant() {
    let mut client = Client::new();
    client
        .waiting
        .push_back((CallId(0), Call::Truncate(Seq(0))));
    client.assert_invariants();
}

#[test]
fn an_adopted_uuid_fences_the_next_write_at_the_handed_tail() {
    let mut client = Client::new();
    assert!(client.calls_idle());
    client.adopt(B, Seq(7));
    assert_eq!(
        client.mode(),
        Mode::Fenced {
            uuid: B,
            next: Seq(7)
        }
    );
    let call = client.write(records("x"));
    assert!(!client.calls_idle(), "the write is in flight");
    let (id, command) = one_command(&mut client);
    assert_eq!(command, fenced(B, 7, "x"));
    client.answer(
        id,
        reply(Answer::Written {
            seq: Seq(7),
            count: 1,
        }),
    );
    assert_eq!(
        one_outcome(&mut client),
        (
            call,
            Outcome::Written {
                seq: Seq(7),
                count: 1
            }
        )
    );
    assert!(client.calls_idle());
}

#[test]
fn a_read_in_flight_does_not_stop_an_adoption() {
    let mut client = Client::new();
    let _ = client.read(Seq(0), 1);
    assert!(
        client.calls_idle(),
        "a read is not a call that the mode fences"
    );
    client.adopt(A, Seq(0));
    assert!(matches!(client.mode(), Mode::Fenced { uuid, .. } if uuid == A));
}

#[test]
#[should_panic(expected = "no call in flight")]
fn an_adoption_under_a_call_in_flight_panics() {
    let mut client = Client::new();
    let _ = client.write(records("x"));
    client.adopt(A, Seq(0));
}
